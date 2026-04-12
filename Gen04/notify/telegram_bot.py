"""
telegram_bot.py — Q-TRON Telegram 조회형 봇 (polling)
======================================================
Read-only snapshot 기반. Live 객체 직접 참조 금지.
커맨드: /status, /trail, /help

REST(Gen04-REST) 전환 시 snapshot_getter 인터페이스만 맞추면 동작.

Usage (standalone test):
    python -m notify.telegram_bot
"""
from __future__ import annotations

import copy
import json
import logging
import os
import threading
import time
import urllib.error
import urllib.parse
import urllib.request
from datetime import datetime
from pathlib import Path
from typing import Callable, Dict, Optional, Set

logger = logging.getLogger("gen4.telegram.bot")


class TelegramBot:
    """Polling-based Telegram bot. Reads snapshot only, never modifies state."""

    def __init__(
        self,
        token: str,
        chat_id: str,
        allowed_user_ids: Set[int],
        snapshot_getter: Callable[[], Optional[Dict]],
        state_dir: Optional[Path] = None,
    ):
        self._token = token
        self._chat_id = str(chat_id)
        self._allowed_user_ids = allowed_user_ids
        self._snapshot_getter = snapshot_getter
        self._state_dir = state_dir or Path(__file__).resolve().parent.parent / "state"
        self._offset_file = self._state_dir / "telegram_offset.json"

        self._running = False
        self._thread: Optional[threading.Thread] = None
        self._last_update_id = 0
        self._conflict_count = 0  # consecutive 409 counter
        self._polling_disabled = False  # True when 409 conflict detected

    # ── Offset Persistence ───────────────────────────────────────────────────

    def _load_offset(self) -> int:
        """Load last_update_id from file. 손상 시 0 반환."""
        try:
            if self._offset_file.exists():
                data = json.loads(self._offset_file.read_text(encoding="utf-8"))
                offset = data.get("last_update_id", 0)
                if isinstance(offset, int) and offset >= 0:
                    logger.debug(f"[TG_BOT] loaded offset={offset} from file")
                    return offset
                logger.warning(f"[TG_BOT] invalid offset in file: {offset}, resetting to 0")
        except Exception as e:
            logger.warning(f"[TG_BOT] offset file load failed: {e}, resetting to 0")
        return 0

    def _save_offset(self, offset: int) -> None:
        """Atomic save: tmp → rename. 실패 시 warning + in-memory 유지."""
        try:
            self._state_dir.mkdir(parents=True, exist_ok=True)
            tmp = self._offset_file.with_suffix(".tmp")
            tmp.write_text(
                json.dumps({"last_update_id": offset}, indent=2),
                encoding="utf-8",
            )
            # Windows: rename fails if target exists → remove first
            if self._offset_file.exists():
                self._offset_file.unlink()
            tmp.rename(self._offset_file)
        except Exception as e:
            logger.warning(f"[TG_BOT] offset save failed: {e}, in-memory offset maintained")

    def _flush_old_updates(self) -> int:
        """시작 시 과거 메시지 flush. 최신 update_id 반환."""
        try:
            url = (f"https://api.telegram.org/bot{self._token}"
                   f"/getUpdates?offset=-1&limit=1&timeout=1")
            req = urllib.request.Request(url)
            r = urllib.request.urlopen(req, timeout=5)
            data = json.loads(r.read().decode())
            if data.get("ok") and data.get("result"):
                latest_id = data["result"][-1]["update_id"]
                logger.info(f"[TG_BOT] flushed old updates, latest_id={latest_id}")
                return latest_id
        except Exception as e:
            logger.warning(f"[TG_BOT] flush failed: {e}")
        return 0

    # ── API ──────────────────────────────────────────────────────────────────

    def _get_updates(self, offset: int, timeout: int = 3) -> list:
        """getUpdates with long polling. 409 감지 시 polling 자동 비활성화."""
        try:
            url = (f"https://api.telegram.org/bot{self._token}"
                   f"/getUpdates?offset={offset}&timeout={timeout}")
            req = urllib.request.Request(url)
            r = urllib.request.urlopen(req, timeout=timeout + 5)
            data = json.loads(r.read().decode())
            if data.get("ok"):
                self._conflict_count = 0  # reset on success
                return data.get("result", [])
        except urllib.error.HTTPError as e:
            if e.code == 409:
                self._conflict_count += 1
                if self._conflict_count >= 3:
                    self._polling_disabled = True
                    logger.warning(
                        f"[TG_BOT_CONFLICT] 409 Conflict x{self._conflict_count} — "
                        f"another bot instance is polling this token. "
                        f"[TG_BOT_POLLING_DISABLED] Polling stopped. "
                        f"Send-only mode active. Engine unaffected.")
                return []
            logger.warning(f"[TG_BOT] getUpdates HTTP {e.code}: {e}")
        except Exception as e:
            logger.warning(f"[TG_BOT] getUpdates error: {e}")
        return []

    def _send_reply(self, text: str) -> bool:
        """Send reply. timeout 2초."""
        try:
            url = f"https://api.telegram.org/bot{self._token}/sendMessage"
            payload = json.dumps({
                "chat_id": self._chat_id,
                "text": text,
                "disable_web_page_preview": True,
            }, ensure_ascii=False).encode("utf-8")
            req = urllib.request.Request(
                url, data=payload,
                headers={"Content-Type": "application/json"},
            )
            r = urllib.request.urlopen(req, timeout=2)
            return json.loads(r.read().decode()).get("ok", False)
        except Exception as e:
            logger.warning(f"[TG_BOT] reply error: {e}")
            return False

    # ── Access Control ───────────────────────────────────────────────────────

    def _check_access(self, update: dict) -> bool:
        """chat_id + user_id whitelist 검증. whitelist 비어있으면 기본 차단."""
        msg = update.get("message", {})
        chat_id = str(msg.get("chat", {}).get("id", ""))
        user_id = msg.get("from", {}).get("id", 0)

        if chat_id != self._chat_id:
            logger.debug(f"[TG_BOT] ignored: chat_id mismatch ({chat_id})")
            return False

        if not self._allowed_user_ids:
            logger.error("[TG_BOT] allowed_user_ids is empty — all messages blocked (config error)")
            return False

        if user_id not in self._allowed_user_ids:
            logger.warning(f"[TG_BOT] ignored: user_id={user_id} not in whitelist")
            return False

        return True

    # ── Command Handlers ─────────────────────────────────────────────────────

    _alerts_enabled = True  # /alert on/off

    def _handle_command(self, text: str) -> Optional[str]:
        """커맨드 라우팅. 미인식 커맨드 → None."""
        parts = text.strip().split()
        cmd = parts[0].lower() if parts else ""
        arg = parts[1].lower() if len(parts) > 1 else ""
        handlers = {
            "/status": self._cmd_status,
            "/trail": self._cmd_trail,
            "/positions": self._cmd_positions,
            "/sectors": self._cmd_sectors,
            "/regime": self._cmd_regime,
            "/rebal": self._cmd_rebal,
            "/pnl": self._cmd_pnl,
            "/risk": self._cmd_risk,
            "/log": self._cmd_log,
            "/help": self._cmd_help,
        }
        if cmd == "/alert":
            return self._cmd_alert(arg)
        handler = handlers.get(cmd)
        return handler() if handler else None

    def _cmd_status(self) -> str:
        snapshot = self._snapshot_getter()
        if snapshot:
            snapshot = copy.deepcopy(snapshot)  # defense-in-depth
        if not snapshot:
            return "[Q-TRON] snapshot 없음 (시스템 초기화 중)"

        ts = snapshot.get("timestamp", "?")
        # snapshot_age_sec: 응답 시점에서 계산
        try:
            snap_dt = datetime.fromisoformat(ts)
            age_sec = (datetime.now() - snap_dt).total_seconds()
            age_str = f"{age_sec:.0f}초 전"
        except Exception:
            age_str = "?"
            age_sec = -1

        equity = snapshot.get("equity", 0)
        daily = snapshot.get("daily_pnl_pct", 0)
        n_pos = snapshot.get("n_positions", 0)
        safe = snapshot.get("safe_mode", 0)
        buy_perm = snapshot.get("buy_permission", "?")
        rebal_d = snapshot.get("next_rebalance_in_days", "?")
        is_stale = snapshot.get("is_stale", False)
        max_age = snapshot.get("max_price_age_sec", 0)

        regime = snapshot.get("regime", "")

        lines = [
            f"[Q-TRON] Status ({ts[11:19]}, {age_str})",
            f"평가액: {equity:,.0f}원",
            f"일수익: {daily:+.2%}",
            f"보유: {n_pos}/20종목",
            f"SAFE: L{safe}",
            f"BuyPerm: {buy_perm}",
            f"다음리밸: D-{rebal_d}",
        ]
        if regime:
            lines.append(f"레짐: {regime}")
        if is_stale:
            lines.append(f"[STALE] 가격 데이터 오래됨 (max age {max_age:.0f}s)")
        return "\n".join(lines)

    def _cmd_trail(self) -> str:
        snapshot = self._snapshot_getter()
        if not snapshot:
            return "[Q-TRON] snapshot 없음 (시스템 초기화 중)"
        snapshot = copy.deepcopy(snapshot)  # defense-in-depth

        positions = snapshot.get("positions", [])
        if not positions:
            return "[Q-TRON] 보유종목 없음"

        # distance_to_stop_pp 기준 (config.TRAIL_ALERT_DISTANCE_PP)
        # snapshot에 threshold 포함
        threshold = snapshot.get("trail_alert_threshold_pp", 4.0)

        breached = []  # stop 도달/초과
        near = []      # 임박

        for p in positions:
            dist = p.get("distance_to_stop_pp", 999)
            drop = p.get("current_drop_pct", 0)
            stop = p.get("trail_stop_pct", 0)
            name = p.get("name", p.get("code", "?"))

            if dist <= 0:
                breached.append(f"  {name}  현재 {drop:.1%}  stop {stop:.1%}  [도달/초과]")
            elif dist <= threshold:
                near.append(f"  {name}  현재 {drop:.1%}  stop {stop:.1%}  남은 {dist:.1f}%p")

        lines = []
        if breached:
            lines.append("[Q-TRON] Trail Stop 도달/초과")
            lines.extend(breached)
        if near:
            if lines:
                lines.append("")
            lines.append(f"[Q-TRON] Trail 임박 (stop까지 {threshold:.1f}%p 이내)")
            lines.extend(near)
        if not lines:
            lines.append(f"[Q-TRON] 임박 종목 없음 (기준: stop까지 {threshold:.1f}%p 이내)")

        ts = snapshot.get("timestamp", "?")
        lines.append(f"\n기준: {ts[11:19]}")
        return "\n".join(lines)

    def _cmd_positions(self) -> str:
        snapshot = self._snapshot_getter()
        if not snapshot:
            return "[Q-TRON] snapshot 없음"
        snapshot = copy.deepcopy(snapshot)
        positions = snapshot.get("positions", [])
        if not positions:
            return "[Q-TRON] 보유종목 없음"

        # 수익률 순 정렬
        positions.sort(key=lambda p: p.get("pnl_pct", 0), reverse=True)
        lines = [f"[Q-TRON] 보유종목 ({len(positions)})"]
        for p in positions:
            name = p.get("name", p.get("code", "?"))
            pnl = p.get("pnl_pct", 0)
            drop = p.get("current_drop_pct", 0)
            lines.append(f"  {name}  {pnl:+.1%}  고점대비 {drop:.1%}")
        ts = snapshot.get("timestamp", "?")
        lines.append(f"\n기준: {ts[11:19]}")
        return "\n".join(lines)

    def _cmd_sectors(self) -> str:
        snapshot = self._snapshot_getter()
        if not snapshot:
            return "[Q-TRON] snapshot 없음"
        snapshot = copy.deepcopy(snapshot)

        # theme_regime 데이터가 있으면 사용 (REST 실시간)
        theme_regime = snapshot.get("theme_regime", [])
        if theme_regime:
            lines = [f"[Q-TRON] Theme Regime ({len(theme_regime)}테마)"]
            for t in theme_regime:
                regime = t.get("regime", "?")
                chg = t.get("change_pct", 0)
                streak = t.get("streak", 0)
                streak_str = f" ({streak}일째)" if streak > 1 else ""
                lines.append(f"  {t['name']}  {regime}  {chg:+.1f}%{streak_str}")
            return "\n".join(lines)

        # Fallback: 보유종목 기반 섹터 집계
        positions = snapshot.get("positions", [])
        if not positions:
            return "[Q-TRON] 보유종목 없음"

        try:
            import json as _json
            sm_path = self._state_dir.parent / "data" / "sector_map.json"
            sm = _json.loads(sm_path.read_text(encoding="utf-8")) if sm_path.exists() else {}
        except Exception:
            sm = {}

        sectors = {}
        for p in positions:
            code = p.get("code", "")
            sector = sm.get(code, "기타")
            s = sectors.setdefault(sector, {"count": 0, "pnl_sum": 0.0})
            s["count"] += 1
            s["pnl_sum"] += p.get("pnl_pct", 0)

        lines = [f"[Q-TRON] 섹터별 현황 ({len(sectors)}섹터)"]
        for name, v in sorted(sectors.items(), key=lambda x: -x[1]["count"]):
            avg_pnl = v["pnl_sum"] / v["count"] if v["count"] > 0 else 0
            lines.append(f"  {name}: {v['count']}종목  평균 {avg_pnl:+.1%}")
        return "\n".join(lines)

    def _cmd_regime(self) -> str:
        snapshot = self._snapshot_getter()
        if not snapshot:
            return "[Q-TRON] snapshot 없음"
        snapshot = copy.deepcopy(snapshot)
        regime = snapshot.get("regime", "")
        score = snapshot.get("regime_score", "")

        # REST 대시보드에서 실시간 레짐 조회 (장중 intraday 포함)
        if not regime:
            try:
                import urllib.request, json as _json
                with urllib.request.urlopen("http://localhost:8080/api/regime/current", timeout=3) as resp:
                    data = _json.loads(resp.read())
                    _actual = data.get("actual") or {}
                    if _actual.get("actual_label"):
                        regime = _actual["actual_label"]
                        _scores = _actual.get("scores", {})
                        score = f"{_scores.get('total', '')}"
                        if _actual.get("kospi_change") is not None:
                            score += f" (KOSPI {_actual['kospi_change']*100:+.1f}%)"
            except Exception:
                pass

        lines = ["[Q-TRON] Market Regime"]
        if regime:
            lines.append(f"  레짐: {regime}")
        else:
            lines.append("  레짐: 미판정 (REST+batch 모두 미실행)")
        if score:
            lines.append(f"  스코어: {score}")

        # 섹터 레짐 (간이)
        positions = snapshot.get("positions", [])
        if positions:
            bull = sum(1 for p in positions if p.get("pnl_pct", 0) > 0.03)
            bear = sum(1 for p in positions if p.get("pnl_pct", 0) < -0.03)
            side = len(positions) - bull - bear
            lines.append(f"\n  종목 레짐 분포:")
            lines.append(f"  BULL {bull} | SIDEWAYS {side} | BEAR {bear}")

        return "\n".join(lines)

    def _cmd_rebal(self) -> str:
        snapshot = self._snapshot_getter()
        if not snapshot:
            return "[Q-TRON] snapshot 없음"
        snapshot = copy.deepcopy(snapshot)
        d_day = snapshot.get("next_rebalance_in_days", "?")
        n_pos = snapshot.get("n_positions", 0)
        bp = snapshot.get("buy_permission", "?")

        lines = [
            "[Q-TRON] 리밸런스 현황",
            f"  다음 리밸: D-{d_day}",
            f"  현재 포지션: {n_pos}/20",
            f"  BuyPermission: {bp}",
        ]

        # 타겟 포트폴리오 로드 시도
        try:
            import json as _json
            sig_dir = self._state_dir.parent / "data" / "signals"
            sig_files = sorted(sig_dir.glob("target_*.json"), reverse=True)
            if sig_files:
                target = _json.loads(sig_files[0].read_text(encoding="utf-8"))
                tickers = target.get("tickers", target.get("target", []))
                if tickers:
                    lines.append(f"\n  최근 타겟: {len(tickers)}종목")
                    lines.append(f"  시그널: {sig_files[0].name}")
        except Exception:
            pass

        return "\n".join(lines)

    def _cmd_pnl(self) -> str:
        snapshot = self._snapshot_getter()
        if not snapshot:
            return "[Q-TRON] snapshot 없음"
        snapshot = copy.deepcopy(snapshot)

        equity = snapshot.get("equity", 0)
        daily = snapshot.get("daily_pnl_pct", 0)
        n_pos = snapshot.get("n_positions", 0)
        positions = snapshot.get("positions", [])

        # Top/Bottom 종목
        positions.sort(key=lambda p: p.get("pnl_pct", 0), reverse=True)
        top3 = positions[:3]
        bot3 = positions[-3:] if len(positions) >= 3 else []

        lines = [
            f"[Q-TRON] 수익 현황",
            f"  평가액: {equity:,.0f}원",
            f"  일수익: {daily:+.2%}",
            f"  보유: {n_pos}종목",
        ]
        if top3:
            lines.append("\n  Best:")
            for p in top3:
                lines.append(f"    {p.get('name', '?')}  {p.get('pnl_pct', 0):+.1%}")
        if bot3:
            lines.append("  Worst:")
            for p in bot3:
                lines.append(f"    {p.get('name', '?')}  {p.get('pnl_pct', 0):+.1%}")

        return "\n".join(lines)

    def _cmd_risk(self) -> str:
        snapshot = self._snapshot_getter()
        if not snapshot:
            return "[Q-TRON] snapshot 없음"
        snapshot = copy.deepcopy(snapshot)

        sm = snapshot.get("safe_mode", 0)
        bp = snapshot.get("buy_permission", "?")
        is_stale = snapshot.get("is_stale", False)
        max_age = snapshot.get("max_price_age_sec", 0)
        regime = snapshot.get("regime", "미판정")

        sm_labels = {0: "NORMAL", 1: "ALERT (알림만)", 2: "RESTRICT (BUY 축소)", 3: "BLOCK (전면 차단)"}
        lines = [
            "[Q-TRON] 리스크 현황",
            f"  SAFE_MODE: L{sm} ({sm_labels.get(sm, '?')})",
            f"  BuyPermission: {bp}",
            f"  레짐: {regime}",
            f"  데이터: {'STALE' if is_stale else 'OK'} (max age {max_age:.0f}s)",
        ]
        return "\n".join(lines)

    def _cmd_log(self) -> str:
        """최근 로그 이벤트 10건."""
        try:
            from datetime import date as _date
            log_dir = self._state_dir.parent / "logs"
            today_str = _date.today().strftime("%Y%m%d")
            # 오늘 로그 파일 찾기
            log_files = sorted(log_dir.glob(f"*{today_str}*.log"), reverse=True)
            if not log_files:
                log_files = sorted(log_dir.glob("*.log"), reverse=True)
            if not log_files:
                return "[Q-TRON] 로그 파일 없음"

            # 마지막 10줄 중 WARNING/CRITICAL/ERROR만
            lines_raw = log_files[0].read_text(encoding="utf-8", errors="ignore").splitlines()
            important = [l for l in lines_raw
                         if any(k in l for k in ["WARNING", "CRITICAL", "ERROR", "TRAIL", "RECON", "SAFE_MODE"])]
            recent = important[-10:] if important else lines_raw[-5:]

            lines = [f"[Q-TRON] 최근 이벤트 ({log_files[0].name})"]
            for l in recent:
                # 줄 길이 제한 (텔레그램 가독성)
                short = l[:120] + "..." if len(l) > 120 else l
                lines.append(short)
            return "\n".join(lines) if len(lines) > 1 else "[Q-TRON] 주요 이벤트 없음"
        except Exception as e:
            return f"[Q-TRON] 로그 조회 실패: {e}"

    def _cmd_alert(self, arg: str) -> str:
        if arg == "on":
            TelegramBot._alerts_enabled = True
            return "[Q-TRON] 봇 응답 활성화됨\n(push 알림: 항상 발송)"
        elif arg == "off":
            TelegramBot._alerts_enabled = False
            return "[Q-TRON] 봇 응답 비활성화됨\n(push 알림: 매수/매도/trail 등은 계속 발송)"
        else:
            status = "ON" if TelegramBot._alerts_enabled else "OFF"
            return (f"[Q-TRON] 봇 응답: {status}\n"
                    f"  /alert on  — 봇 커맨드 응답 활성화\n"
                    f"  /alert off — 봇 커맨드 응답 비활성화\n"
                    f"  * push 알림(체결/trail/SAFE)은 항상 발송")

    def _cmd_help(self) -> str:
        return (
            "[Q-TRON] 사용 가능한 커맨드\n"
            "/status     - 평가액, 수익, SAFE, 리밸\n"
            "/positions  - 전 종목 상세 (수익률순)\n"
            "/trail      - trail stop 임박/도달\n"
            "/sectors    - 섹터별 현황\n"
            "/regime     - 시장/종목 레짐\n"
            "/rebal      - 리밸런스 D-day + 타겟\n"
            "/pnl        - 수익 상세 (Best/Worst)\n"
            "/risk       - DD guard + SAFE + 데이터\n"
            "/log        - 최근 주요 이벤트\n"
            "/alert on/off - 알림 토글\n"
            "/help       - 이 도움말"
        )

    # ── Polling Loop ─────────────────────────────────────────────────────────

    def _poll_loop(self):
        """Daemon thread에서 실행. _running=False 시 종료."""
        # 1) offset 초기화
        file_offset = self._load_offset()
        flush_offset = self._flush_old_updates()
        self._last_update_id = max(file_offset, flush_offset)
        logger.info(f"[TG_BOT] polling start, offset={self._last_update_id}")

        # 2) Polling
        while self._running:
            if self._polling_disabled:
                # 409 conflict → stop polling, keep thread alive for clean shutdown
                time.sleep(60)
                continue

            try:
                updates = self._get_updates(
                    offset=self._last_update_id + 1, timeout=3
                )
                for update in updates:
                    uid = update.get("update_id", 0)
                    if uid > self._last_update_id:
                        self._last_update_id = uid

                    if not self._check_access(update):
                        continue

                    text = update.get("message", {}).get("text", "")
                    reply = self._handle_command(text)
                    if reply:
                        self._send_reply(reply)

                # offset 저장 (update 처리 후)
                if updates:
                    self._save_offset(self._last_update_id)

            except Exception as e:
                logger.warning(f"[TG_BOT] poll error: {e}")
                time.sleep(3)

    # ── Start / Stop ─────────────────────────────────────────────────────────

    def start(self):
        """Start polling in daemon thread."""
        if self._running:
            return
        self._running = True
        self._thread = threading.Thread(
            target=self._poll_loop, daemon=True, name="tg-bot"
        )
        self._thread.start()
        logger.info("[TG_BOT] started")

    def stop(self):
        """Stop polling. join(timeout=5). 실패 시 warning (비차단)."""
        if not self._running:
            return
        self._running = False
        if self._thread and self._thread.is_alive():
            self._thread.join(timeout=5)
            if self._thread.is_alive():
                logger.warning("[TG_BOT] thread did not exit within 5s timeout")
            else:
                logger.info("[TG_BOT] stopped")
        else:
            logger.info("[TG_BOT] stopped (thread was not alive)")


# ── Standalone test ──────────────────────────────────────────────────────────

if __name__ == "__main__":
    logging.basicConfig(level=logging.DEBUG)
    from notify.telegram_notify import _ensure_credentials, _BOT_TOKEN, _CHAT_ID
    from notify.telegram_notify import _load_credentials

    token, chat_id = _load_credentials()
    if not token or not chat_id:
        print("TELEGRAM credentials not found")
        exit(1)

    # Mock snapshot
    mock_snapshot = {
        "equity": 5_234_000,
        "daily_pnl_pct": 0.0123,
        "n_positions": 3,
        "safe_mode": 0,
        "buy_permission": "NORMAL",
        "next_rebalance_in_days": 5,
        "is_stale": False,
        "max_price_age_sec": 12.3,
        "timestamp": datetime.now().isoformat(),
        "trail_alert_threshold_pp": 4.0,
        "positions": [
            {"code": "005930", "name": "삼성전자", "qty": 10,
             "pnl_pct": 0.032, "current_drop_pct": -0.041,
             "trail_stop_pct": -0.12, "distance_to_stop_pp": 7.9},
            {"code": "000660", "name": "SK하이닉스", "qty": 5,
             "pnl_pct": 0.078, "current_drop_pct": -0.091,
             "trail_stop_pct": -0.12, "distance_to_stop_pp": 2.9},
            {"code": "373220", "name": "LG에너지솔루션", "qty": 2,
             "pnl_pct": -0.015, "current_drop_pct": -0.125,
             "trail_stop_pct": -0.12, "distance_to_stop_pp": 0.0},
        ],
    }

    import threading
    _lock = threading.Lock()

    def get_snapshot():
        with _lock:
            return copy.deepcopy(mock_snapshot)

    # user_id = chat_id (개인 채팅)
    user_id = int(chat_id)
    bot = TelegramBot(
        token=token,
        chat_id=chat_id,
        allowed_user_ids={user_id},
        snapshot_getter=get_snapshot,
    )
    print(f"Starting bot... (chat_id={chat_id}, user_id={user_id})")
    print("Send /status, /trail, /help to the bot. Ctrl+C to stop.")
    bot.start()

    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        print("\nStopping...")
        bot.stop()
        print("Done.")
