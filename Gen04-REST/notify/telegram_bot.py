# -*- coding: utf-8 -*-
"""
telegram_bot.py — 텔레그램 알림 발송
======================================
.env: TELEGRAM_BOT_TOKEN, TELEGRAM_CHAT_ID
실패 시 로그만. blocking 금지.
"""
from __future__ import annotations

import logging
import os
from pathlib import Path
from typing import Optional

logger = logging.getLogger("gen4.notify.telegram")

_BOT_TOKEN: Optional[str] = None
_CHAT_ID: Optional[str] = None
_INITIALIZED = False

SEVERITY_EMOJI = {
    "INFO": "ℹ️",
    "WARN": "⚠️",
    "CRITICAL": "🚨",
}


def _init() -> bool:
    """Load credentials from .env. Returns True if configured."""
    global _BOT_TOKEN, _CHAT_ID, _INITIALIZED
    if _INITIALIZED:
        return bool(_BOT_TOKEN and _CHAT_ID)

    # Try dotenv
    try:
        from dotenv import load_dotenv
        env_path = Path(__file__).resolve().parent.parent / ".env"
        load_dotenv(env_path)
    except ImportError:
        pass

    _BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "")
    _CHAT_ID = os.getenv("TELEGRAM_CHAT_ID", "")
    _INITIALIZED = True

    if not _BOT_TOKEN or not _CHAT_ID:
        logger.warning("[Telegram] TELEGRAM_BOT_TOKEN or TELEGRAM_CHAT_ID not set in .env")
        return False
    return True


def send(text: str, severity: str = "INFO") -> bool:
    """
    Send message to Telegram. Non-blocking, never raises.
    Returns True if sent successfully.
    """
    try:
        if not _init():
            return False

        import requests
        emoji = SEVERITY_EMOJI.get(severity, "")
        full_text = f"{emoji} {text}" if emoji else text

        url = f"https://api.telegram.org/bot{_BOT_TOKEN}/sendMessage"
        resp = requests.post(url, json={
            "chat_id": _CHAT_ID,
            "text": full_text,
            "parse_mode": "HTML",
            "disable_web_page_preview": True,
        }, timeout=10)

        if resp.status_code == 200:
            logger.info(f"[Telegram] Sent ({severity}): {text[:50]}...")
            return True
        else:
            logger.warning(f"[Telegram] HTTP {resp.status_code}: {resp.text[:100]}")
            return False

    except Exception as e:
        logger.warning(f"[Telegram] Send failed: {e}")
        return False


# ── Formatted Messages ────────────────────────────────────────

def notify_regime_change(prev_label: str, new_label: str, score: float) -> bool:
    return send(
        f"<b>레짐 변경</b>\n{prev_label} → {new_label}\n점수: {score:+.1f}",
        "WARN"
    )


def notify_trail_near(code: str, name: str, margin_pct: float) -> bool:
    return send(
        f"<b>Trail Stop 근접</b>\n{name}({code})\n트리거까지 {margin_pct:.1f}%",
        "WARN"
    )


def notify_trail_triggered(code: str, name: str, price: float, trail: float) -> bool:
    return send(
        f"<b>Trail Stop 발동</b>\n{name}({code})\n현재가 {price:,.0f} ≤ 트리거 {trail:,.0f}",
        "CRITICAL"
    )


def notify_dd_warning(dd_type: str, dd_pct: float) -> bool:
    return send(
        f"<b>DD 경고</b>\n{dd_type}: {dd_pct*100:.1f}%",
        "CRITICAL" if dd_type == "monthly" else "WARN"
    )


def notify_stale(source: str, age_sec: float) -> bool:
    return send(
        f"<b>데이터 지연</b>\n{source}: {age_sec:.0f}초 경과",
        "WARN"
    )


def notify_recon_unsafe(reason: str = "") -> bool:
    return send(
        f"<b>RECON 비신뢰</b>\n{reason or '브로커 동기화 불안정'}",
        "CRITICAL"
    )


def notify_buy(code: str, name: str, qty: int, price: float, fee: float = 0) -> bool:
    return send(
        f"🔴 <b>매수 체결</b>\n{name}({code}) {qty}주\n"
        f"체결가: {price:,.0f}원\n수수료: {fee:,.0f}원",
        "INFO"
    )


def notify_sell(code: str, name: str, qty: int, price: float,
                pnl: float = 0, pnl_pct: float = 0, fee: float = 0, tax: float = 0) -> bool:
    net = pnl - fee - tax
    return send(
        f"🔵 <b>매도 체결</b>\n{name}({code}) {qty}주\n"
        f"체결가: {price:,.0f}원 → {pnl:+,.0f}원 ({pnl_pct:+.2f}%)\n"
        f"수수료: {fee:,.0f}원 | 세금: {tax:,.0f}원\n"
        f"순이익: {net:+,.0f}원",
        "INFO"
    )


def notify_rebal_countdown(d_day: int, rebal_date: str) -> bool:
    return send(
        f"📅 <b>리밸런싱 D-{d_day}</b>\n예정일: {rebal_date}",
        "INFO"
    )


def notify_crosscheck_critical(diffs: list) -> bool:
    diff_text = "\n".join(diffs[:5])
    return send(
        f"<b>COM↔REST 불일치</b>\n{diff_text}",
        "CRITICAL"
    )


# ── Bot Command Handler (polling) ────────────────────────────

_COMMANDS = {}
_polling_active = False


def register_command(cmd: str, handler):
    """Register a command handler. handler(chat_id, args) -> str"""
    _COMMANDS[cmd] = handler


def _api(path, timeout=5):
    """Internal API call helper."""
    import requests as _req
    return _req.get(f"http://localhost:8080{path}", timeout=timeout).json()


def _default_commands():
    """Register built-in commands (Gen04 호환 + 신규)."""

    def cmd_help(chat_id, args):
        return (
            "<b>[Q-TRON] 커맨드</b>\n\n"
            "<b>KR Market</b>\n"
            "/status     - 서버상태, 평가액, SAFE, 레짐\n"
            "/positions  - 전 보유 종목 (섹터별)\n"
            "/trail      - trail stop 근접/발동\n"
            "/sectors    - 섹터별 현황\n"
            "/regime     - 오늘/내일 레짐\n"
            "/rebal      - 리밸런스 D-day + 타겟\n"
            "/score      - 리밸 SCORE + Decision\n"
            "/pnl        - 손익 현황 (Best/Worst)\n"
            "/risk       - DD guard + BUY STATUS\n"
            "/lab        - 9전략 Forward Trading\n"
            "/db         - DB Health Check\n"
            "/alert on/off - 알림 토글\n\n"
            "<b>US Market</b>\n"
            "/us_status     - Alpaca 계정 ($)\n"
            "/us_positions  - US 보유종목\n"
            "/us_trail      - US trail stop 근접\n"
            "/us_rebal      - US 리밸 프리뷰\n\n"
            "<b>Cross-Market</b>\n"
            "/market     - KR + US 통합 요약\n"
            "/help       - 이 도움말"
        )

    def cmd_status(chat_id, args):
        try:
            h = _api("/api/health")
            p = _api("/api/portfolio")
            r = _api("/api/regime/current")
            total_eval = p.get("총평가금액", 0)
            pnl = p.get("총평가손익금액", 0)
            n_hold = len(p.get("holdings", []))
            today_r = r.get("today") or {}
            regime = today_r.get("actual_label") or r.get("prediction", {}).get("predicted_label", "?")
            return (
                f"<b>[Q-TRON KR 5.0] Status</b>\n"
                f"상태: {h.get('status','?')}\n"
                f"평가액: {total_eval:,.0f}원\n"
                f"손익: {pnl:+,.0f}원\n"
                f"종목: {n_hold}개\n"
                f"레짐: {regime}\n"
                f"시각: {h.get('timestamp','?')}"
            )
        except Exception as e:
            return f"서버 연결 실패: {e}"

    def cmd_positions(chat_id, args):
        try:
            p = _api("/api/portfolio")
            holdings = p.get("holdings", [])
            if not holdings:
                return "보유종목 없음"
            lines = []
            for h in sorted(holdings, key=lambda x: -float(x.get("pnl_rate", 0)))[:15]:
                name = h.get("name", h.get("code", "?"))
                pnl = float(h.get("pnl_rate", 0))
                sign = "🔴" if pnl >= 0 else "🔵"
                lines.append(f"{sign} {name}: {pnl:+.1f}%")
            total_pnl = p.get("총평가손익금액", 0)
            return f"<b>보유종목 ({len(holdings)}) | 총손익 {total_pnl:+,.0f}원</b>\n" + "\n".join(lines)
        except Exception as e:
            return f"조회 실패: {e}"

    def cmd_trail(chat_id, args):
        try:
            p = _api("/api/portfolio")
            holdings = p.get("holdings", [])
            if not holdings:
                return "보유종목 없음"
            lines = []
            for h in holdings:
                pnl = float(h.get("pnl_rate", 0))
                if pnl < -8:  # trail -12%에 근접 (-8% 이하)
                    name = h.get("name", h.get("code", "?"))
                    lines.append(f"⚠️ {name}: {pnl:+.1f}% (trail -12%까지 {-12-pnl:.1f}%)")
            if not lines:
                return "✅ Trail stop 근접 종목 없음 (모두 -8% 이상)"
            return f"<b>Trail Stop 근접</b>\n" + "\n".join(lines)
        except Exception as e:
            return f"조회 실패: {e}"

    def cmd_sectors(chat_id, args):
        try:
            p = _api("/api/portfolio")
            holdings = p.get("holdings", [])
            if not holdings:
                return "보유종목 없음"
            # Group by sector from sector_map
            try:
                sm = _api("/api/db/health")  # just check DB is alive
            except Exception:
                pass
            lines = []
            total = sum(h.get("eval_amt", 0) for h in holdings)
            # Simple grouping by name prefix (sector_map not in portfolio API)
            for h in sorted(holdings, key=lambda x: -x.get("eval_amt", 0))[:10]:
                name = h.get("name", "?")
                pct = h.get("eval_amt", 0) / total * 100 if total > 0 else 0
                pnl = float(h.get("pnl_rate", 0))
                lines.append(f"  {name}: {pct:.1f}% ({pnl:+.1f}%)")
            return f"<b>종목별 비중 (상위 10)</b>\n" + "\n".join(lines)
        except Exception as e:
            return f"조회 실패: {e}"

    def cmd_regime(chat_id, args):
        try:
            r = _api("/api/regime/current")
            today = r.get("today") or {}
            predict = r.get("prediction") or {}
            kospi = today.get("kospi_today") or 0
            kospi_chg = (today.get("kospi_change") or 0) * 100
            breadth = (today.get("breadth_ratio") or 0) * 100
            today_label = today.get("actual_label") or "데이터 없음"
            today_date = today.get("market_date") or "?"
            pred_label = predict.get("predicted_label") or "?"
            pred_score = predict.get("composite_score") or 0
            pred_conf = predict.get("confidence_flag") or "?"
            return (
                f"<b>오늘 레짐</b> ({today_date})\n"
                f"  {today_label}\n"
                f"  KOSPI {kospi:,.0f} ({kospi_chg:+.1f}%)\n"
                f"  breadth {breadth:.0f}%\n\n"
                f"<b>내일 예측</b>\n"
                f"  {pred_label} (점수: {pred_score:.2f})\n"
                f"  신뢰도: {pred_conf}"
            )
        except Exception as e:
            return f"조회 실패: {e}"

    def cmd_rebal(chat_id, args):
        try:
            data = _api("/api/rebalance/preview-compare", timeout=10)
            target = data.get("target_date", "?")
            d_remain = data.get("days_remaining", "?")
            new_n = len(data.get("new_entries", []))
            exit_n = len(data.get("exits", []))
            keep_n = len(data.get("unchanged", []))

            lines = [
                f"<b>리밸런스 프리뷰</b>",
                f"Target: {target} | D-{d_remain}",
                f"신규: {new_n} | 제외: {exit_n} | 유지: {keep_n}",
            ]
            # Top 3 new entries
            for e in data.get("new_entries", [])[:3]:
                lines.append(f"  ▲ {e['name']} {e['change_pct']:+.1f}%")
            for e in data.get("exits", [])[:3]:
                lines.append(f"  ▼ {e['name']} {e['change_pct']:+.1f}%")
            return "\n".join(lines)
        except Exception as e:
            return f"조회 실패: {e}"

    def cmd_score(chat_id, args):
        try:
            data = _api("/api/rebalance/preview-compare", timeout=10)
            rs = data.get("rebal_score", {})
            if not rs:
                return "리밸 스코어 데이터 없음"
            return (
                f"<b>REBALANCE SCORE: {rs.get('total', 0)}</b>\n"
                f"Decision: {rs.get('decision', '?')}\n"
                f"Drift: {rs.get('drift', 0)} | Replace: {rs.get('replacement', 0)}\n"
                f"Quality: {rs.get('quality', 0)} | Market: {rs.get('market', 0)}\n"
                f"사유: {', '.join(rs.get('reasons', []))}"
            )
        except Exception as e:
            return f"조회 실패: {e}"

    def cmd_pnl(chat_id, args):
        try:
            p = _api("/api/portfolio")
            holdings = p.get("holdings", [])
            total_pnl = p.get("총평가손익금액", 0)
            # Best / Worst
            if holdings:
                best = max(holdings, key=lambda x: float(x.get("pnl_rate", 0)))
                worst = min(holdings, key=lambda x: float(x.get("pnl_rate", 0)))
                return (
                    f"<b>손익 현황</b>\n"
                    f"총평가손익: {total_pnl:+,.0f}원\n\n"
                    f"Best: {best['name']} {float(best['pnl_rate']):+.1f}% ({best['pnl']:+,}원)\n"
                    f"Worst: {worst['name']} {float(worst['pnl_rate']):+.1f}% ({worst['pnl']:+,}원)"
                )
            return f"<b>손익 현황</b>\n총평가손익: {total_pnl:+,.0f}원"
        except Exception as e:
            return f"조회 실패: {e}"

    def cmd_risk(chat_id, args):
        try:
            s = _api("/api/rebalance/status")
            h = _api("/api/health")
            return (
                f"<b>리스크 현황</b>\n"
                f"서버: {h.get('status', '?')}\n"
                f"Phase: {s.get('phase', '?')}\n"
                f"Mode: {s.get('mode', '?')}\n"
                f"시각: {h.get('timestamp', '?')}"
            )
        except Exception as e:
            return f"조회 실패: {e}"

    def cmd_lab(chat_id, args):
        try:
            data = _api("/api/lab/live/state", timeout=10)
            lanes = data.get("lanes", [])
            if not lanes:
                return "Lab Live 미실행"
            lines = []
            for l in sorted(lanes, key=lambda x: -x.get("total_return", 0)):
                emoji = "🟢" if l["total_return"] >= 0 else "🔴"
                lines.append(f"{emoji} {l['name']}: {l['total_return']:+.1f}% ({l['n_positions']}pos)")
            return f"<b>Forward Trading ({len(lanes)}전략)</b>\n" + "\n".join(lines)
        except Exception as e:
            return f"조회 실패: {e}"

    def cmd_db(chat_id, args):
        try:
            data = _api("/api/db/health")
            if data.get("status") == "ERROR":
                return f"❌ DB 오프라인: {data.get('error','')}"
            lines = [f"<b>PostgreSQL DB ({data.get('db_size','?')})</b>"]
            for t in data.get("tables", []):
                icon = "✅" if t["status"] == "OK" else "⚠️" if t["status"] == "EMPTY" else "❌"
                lines.append(f"{icon} {t['table']}: {t['rows']:,} ({t['latest']})")
            return "\n".join(lines)
        except Exception as e:
            return f"조회 실패: {e}"

    _alerts_enabled_state = {"on": True}

    def cmd_alert(chat_id, args):
        if args and args[0] in ("on", "off"):
            _alerts_enabled_state["on"] = args[0] == "on"
            return f"알림 {'활성화' if _alerts_enabled_state['on'] else '비활성화'}됨"
        return f"알림 상태: {'ON' if _alerts_enabled_state['on'] else 'OFF'}\n/alert on 또는 /alert off"

    register_command("help", cmd_help)
    register_command("status", cmd_status)
    register_command("positions", cmd_positions)
    register_command("trail", cmd_trail)
    register_command("sectors", cmd_sectors)
    register_command("regime", cmd_regime)
    register_command("rebal", cmd_rebal)
    register_command("score", cmd_score)
    register_command("pnl", cmd_pnl)
    register_command("risk", cmd_risk)
    register_command("lab", cmd_lab)
    register_command("db", cmd_db)
    register_command("alert", cmd_alert)
    register_command("portfolio", cmd_positions)  # alias

    # ── US Market Commands ──────────────────────────────────
    # Call localhost:8081 (Gen04-US dashboard). Timeout 3s.
    # All try/except: never propagate to polling loop.

    def _api_us(path, timeout=3):
        import requests as _req
        return _req.get(f"http://localhost:8081{path}", timeout=timeout).json()

    def _fmt_us_status(health, portfolio):
        eq = portfolio.get("equity", 0)
        cash = portfolio.get("cash", 0)
        n = portfolio.get("n_holdings", 0)
        market = "Open" if health.get("is_market_open") else "Closed"
        server = health.get("server_type", "?")
        ts = health.get("next_close", "")[:16]
        return (
            f"<b>[US] Status</b>\n"
            f"Server: {server}\n"
            f"Equity: ${eq:,.2f}\n"
            f"Cash: ${cash:,.2f}\n"
            f"Positions: {n}\n"
            f"Market: {market}\n"
            f"Close: {ts}"
        )

    def _fmt_us_positions(portfolio):
        holdings = portfolio.get("holdings", [])
        if not holdings:
            return "[US] No holdings"
        lines = []
        for h in sorted(holdings, key=lambda x: -float(x.get("pnl_pct", 0)))[:15]:
            sym = h.get("code", "?")
            pnl = float(h.get("pnl_pct", 0))
            sign = "🔴" if pnl >= 0 else "🔵"
            lines.append(f"{sign} {sym}: {pnl:+.1f}%")
        total_pnl = sum(float(h.get("pnl", 0)) for h in holdings)
        return f"<b>[US] Holdings ({len(holdings)}) | P&L ${total_pnl:+,.2f}</b>\n" + "\n".join(lines)

    def cmd_us_status(chat_id, args):
        try:
            h = _api_us("/api/health")
            p = _api_us("/api/portfolio")
            return _fmt_us_status(h, p)
        except Exception as e:
            return f"[US] 서버 연결 실패: {e}"

    def cmd_us_positions(chat_id, args):
        try:
            p = _api_us("/api/portfolio")
            return _fmt_us_positions(p)
        except Exception as e:
            return f"[US] 서버 연결 실패: {e}"

    def cmd_us_trail(chat_id, args):
        try:
            p = _api_us("/api/portfolio")
            holdings = p.get("holdings", [])
            if not holdings:
                return "[US] No holdings"
            lines = []
            for h in holdings:
                pnl = float(h.get("pnl_pct", 0))
                if pnl < -8:  # near trail -12% (within 4%)
                    sym = h.get("code", "?")
                    lines.append(f"⚠️ {sym}: {pnl:+.1f}% (trail -12% until {-12-pnl:.1f}%)")
            if not lines:
                return "✅ [US] No stocks near trail stop (all above -8%)"
            return f"<b>[US] Trail Stop Near</b>\n" + "\n".join(lines)
        except Exception as e:
            return f"[US] 서버 연결 실패: {e}"

    def cmd_us_rebal(chat_id, args):
        try:
            target = _api_us("/api/target")
            portfolio = _api_us("/api/portfolio")
            if target.get("error"):
                return "[US] No target portfolio"
            target_syms = set(target.get("target_tickers", []))
            current_syms = set(h["code"] for h in portfolio.get("holdings", []))
            new_entries = sorted(target_syms - current_syms)
            exits = sorted(current_syms - target_syms)
            keeps = sorted(target_syms & current_syms)
            return (
                f"<b>[US] Rebalance Preview</b>\n"
                f"Target: {len(target_syms)} | Current: {len(current_syms)}\n"
                f"New: {len(new_entries)} | Exit: {len(exits)} | Keep: {len(keeps)}\n"
                f"▲ {', '.join(new_entries[:5])}\n"
                f"▼ {', '.join(exits[:5])}"
            )
        except Exception as e:
            return f"[US] 서버 연결 실패: {e}"

    def _fmt_market(kr_data, us_data, kr_ts, us_ts):
        lines = ["<b>Market Overview</b>"]
        if kr_data:
            total_eval = kr_data.get("총평가금액", 0)
            n_kr = len(kr_data.get("holdings", []))
            lines.append(f"\n<b>KR</b> @{kr_ts}")
            lines.append(f"  평가액: {total_eval:,.0f}원 ({n_kr}종목)")
        else:
            lines.append(f"\n<b>KR</b> — not available")
        if us_data:
            eq = us_data.get("equity", 0)
            n_us = us_data.get("n_holdings", 0)
            lines.append(f"\n<b>US</b> @{us_ts}")
            lines.append(f"  Equity: ${eq:,.2f} ({n_us} pos)")
        else:
            lines.append(f"\n<b>US</b> — not available")
        lines.append(f"\n<i>snapshots may differ</i>")
        return "\n".join(lines)

    def cmd_market(chat_id, args):
        from datetime import datetime
        kr_data, us_data = None, None
        kr_ts, us_ts = "?", "?"
        try:
            kr_data = _api("/api/portfolio")
            kr_ts = datetime.now().strftime("%H:%M:%S KST")
        except Exception:
            pass
        try:
            us_data = _api_us("/api/portfolio")
            us_ts = datetime.now().strftime("%H:%M:%S")
            # Attempt to get ET time from health
            try:
                h = _api_us("/api/health")
                us_ts += " ET" if h.get("is_market_open") else ""
            except Exception:
                pass
        except Exception:
            pass
        if not kr_data and not us_data:
            return "KR + US 모두 연결 실패"
        return _fmt_market(kr_data, us_data, kr_ts, us_ts)

    register_command("us_status", cmd_us_status)
    register_command("us_positions", cmd_us_positions)
    register_command("us_trail", cmd_us_trail)
    register_command("us_rebal", cmd_us_rebal)
    register_command("market", cmd_market)


def start_polling(interval: float = 3.0):
    """Start background polling for bot commands."""
    global _polling_active
    if _polling_active:
        return

    _default_commands()
    _polling_active = True

    import threading

    def _poll_loop():
        import requests as _req
        offset = 0
        while _polling_active:
            try:
                if not _init():
                    import time; time.sleep(30)
                    continue

                url = f"https://api.telegram.org/bot{_BOT_TOKEN}/getUpdates"
                resp = _req.get(url, params={
                    "offset": offset, "timeout": 10, "limit": 10,
                }, timeout=15)

                updates = resp.json().get("result", [])
                for u in updates:
                    offset = u["update_id"] + 1
                    msg = u.get("message", {})
                    text = msg.get("text", "").strip()
                    chat = str(msg.get("chat", {}).get("id", ""))

                    if text.startswith("/"):
                        parts = text[1:].split()
                        cmd = parts[0].lower().split("@")[0]
                        args = parts[1:]

                        handler = _COMMANDS.get(cmd)
                        if handler:
                            try:
                                reply = handler(chat, args)
                                send(reply)
                            except Exception as e:
                                send(f"명령 오류: {e}")
                        else:
                            send(f"알 수 없는 명령: /{cmd}\n/help 로 확인")

            except Exception as e:
                logger.warning(f"[Telegram] Poll error: {e}")
                import time; time.sleep(10)

    threading.Thread(target=_poll_loop, daemon=True).start()
    logger.info("[Telegram] Polling started")
