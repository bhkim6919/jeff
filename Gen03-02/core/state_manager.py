# -*- coding: utf-8 -*-
"""
StateManager
============
런타임 상태 영속화 모듈.

저장 파일 위치: state/
  portfolio_state.json  — 보유 포지션 (code, qty, avg_price, entry_date, tp, sl ...)
  runtime_state.json    — 런타임 메타 (마지막 실행 시각, 레짐, 오늘 진입 종목 목록)

PAPER/LIVE 상태 파일 분리:
  LIVE:  portfolio_state.json  / runtime_state.json  / order_state.json
  PAPER: portfolio_state_paper.json / runtime_state_paper.json / order_state_paper.json

문제 시나리오 방어:
  - 재시작 시 포지션 복구  → load_portfolio()
  - 당일 중복 진입 방지    → runtime_state.today_entries
  - 포지션 누락 감지       → 로드 후 KiwoomProvider 현재가와 대조
  - paper 상태 유출 방지   → 모드별 파일명 분리
"""

import json
import logging
import os
import shutil
from datetime import date, datetime
from pathlib import Path
from typing import Any, Dict, List, Optional

from config import Gen3Config

logger = logging.getLogger("StateManager")


class StateManager:

    def __init__(self, config: Gen3Config):
        self.state_dir = config.abs_path("state")
        self.state_dir.mkdir(parents=True, exist_ok=True)

        # PAPER 모드(mock/pykrx)와 LIVE 모드의 상태 파일을 완전 분리
        self._paper = getattr(config, "paper_trading", False)
        suffix = "_paper" if self._paper else ""

        self._portfolio_file = self.state_dir / f"portfolio_state{suffix}.json"
        self._runtime_file   = self.state_dir / f"runtime_state{suffix}.json"
        self._order_file     = self.state_dir / f"order_state{suffix}.json"

        logger.info("[StateManager] mode=%s, portfolio=%s",
                    "PAPER" if self._paper else "LIVE", self._portfolio_file.name)

    # ── Portfolio State ───────────────────────────────────────────────────────

    def save_portfolio(self, portfolio) -> None:
        """
        PortfolioManager.positions 를 JSON으로 저장.
        Position 데이터클래스의 주요 필드를 dict로 직렬화.
        """
        state: Dict[str, Any] = {
            "timestamp":         datetime.now().isoformat(),
            "cash":              portfolio.cash,
            "prev_close_equity": portfolio.prev_close_equity,
            "peak_equity":       portfolio.peak_equity,
            "peak_month":        portfolio._peak_month,
            "synced_broker_equity": getattr(portfolio, "synced_broker_equity", 0.0),
            "positions": {},
        }

        for code, pos in portfolio.positions.items():
            state["positions"][code] = {
                "code":          pos.code,
                "sector":        pos.sector,
                "quantity":      pos.quantity,
                "avg_price":     pos.avg_price,
                "current_price": pos.current_price,
                "entry_date":    pos.entry_date.isoformat(),
                "tp":            pos.tp,
                "sl":            pos.sl,
                "q_score":       pos.q_score,
                "rr_ratio":      pos.rr_ratio,
                "prev_close":         getattr(pos, "prev_close", 0.0),
                "high_watermark":     getattr(pos, "high_watermark", pos.avg_price),
                "stage":              getattr(pos, "stage", ""),
                "order_no":           getattr(pos, "order_no", ""),
                "source_signal_date": getattr(pos, "source_signal_date", ""),
                "qty_sellable":       getattr(pos, "qty_sellable", -1),
                "qty_confidence":     getattr(pos, "qty_confidence", "HIGH"),
                "restricted_reason":  getattr(pos, "restricted_reason", ""),
                "needs_reconcile":    getattr(pos, "needs_reconcile", False),
                # v7.8: 보조 수량 필드
                "requested_qty":        getattr(pos, "requested_qty", 0),
                "filled_buy_qty":       getattr(pos, "filled_buy_qty", 0),
                "filled_sell_qty":      getattr(pos, "filled_sell_qty", 0),
                "broker_confirmed_qty": getattr(pos, "broker_confirmed_qty", -1),
                # v7.9: partial exit 추적
                "pending_sell_order_no":   getattr(pos, "pending_sell_order_no", ""),
                "pending_sell_qty_orig":   getattr(pos, "pending_sell_qty_orig", 0),
                "pending_sell_qty_filled": getattr(pos, "pending_sell_qty_filled", 0),
                "pending_sell_remaining":  getattr(pos, "pending_sell_remaining", 0),
            }

        self._write(self._portfolio_file, state)
        logger.debug("[StateManager] %s 저장 (%d 포지션)",
                     self._portfolio_file.name, len(state["positions"]))

    def load_portfolio(self) -> Optional[Dict[str, Any]]:
        """
        저장된 portfolio_state.json 로드.
        없거나 파싱 오류 시 None 반환.
        """
        data = self._read(self._portfolio_file)
        if data is None:
            logger.info("[StateManager] %s 없음 → 초기 상태로 시작",
                        self._portfolio_file.name)
        return data

    def restore_portfolio(self, portfolio) -> int:
        """
        저장된 상태로 PortfolioManager 포지션 복원.
        반환: 복원된 포지션 수
        """
        data = self.load_portfolio()
        if not data or not data.get("positions"):
            return 0

        # 기본 유효성 검증 (avg_price > 0, quantity > 0, 유한값)
        import math
        for code, p in list(data["positions"].items()):
            try:
                avg = float(p.get("avg_price", 0))
                qty = int(p.get("quantity", 0))
                if avg <= 0 or qty <= 0 or not math.isfinite(avg):
                    logger.warning("[StateManager] 비정상 포지션 제거: %s (avg=%.2f, qty=%d)", code, avg, qty)
                    del data["positions"][code]
            except (ValueError, TypeError) as e:
                logger.warning("[StateManager] 포지션 파싱 실패 제거: %s (%s)", code, e)
                del data["positions"][code]

        if not data["positions"]:
            return 0

        from core.position_tracker import Position
        restored = 0
        for code, p in data["positions"].items():
            try:
                _cur = float(p.get("current_price", p["avg_price"]))
                pos = Position(
                    code          = p["code"],
                    sector        = p.get("sector", ""),
                    quantity      = int(p["quantity"]),
                    avg_price     = float(p["avg_price"]),
                    current_price = _cur,
                    entry_date    = date.fromisoformat(p["entry_date"]),
                    tp            = float(p.get("tp", 0)),
                    sl            = float(p.get("sl", 0)),
                    q_score       = float(p.get("q_score", 0)),
                    rr_ratio      = float(p.get("rr_ratio", 0)),
                    prev_close    = float(p.get("prev_close", _cur)),  # v7.4: 없으면 current_price 폴백
                    high_watermark= float(p.get("high_watermark", p["avg_price"])),  # v7.5
                    stage              = p.get("stage", ""),
                    order_no           = p.get("order_no", ""),
                    source_signal_date = p.get("source_signal_date", ""),
                    qty_sellable       = int(p.get("qty_sellable", -1)),
                    qty_confidence     = p.get("qty_confidence", "HIGH"),
                    restricted_reason  = p.get("restricted_reason", ""),
                    needs_reconcile    = bool(p.get("needs_reconcile", False)),
                    # v7.8: 보조 수량 (역호환: 없으면 quantity로 초기화)
                    requested_qty        = int(p.get("requested_qty", p.get("quantity", 0))),
                    filled_buy_qty       = int(p.get("filled_buy_qty", p.get("quantity", 0))),
                    filled_sell_qty      = int(p.get("filled_sell_qty", 0)),
                    broker_confirmed_qty = int(p.get("broker_confirmed_qty", -1)),
                    # v7.9: partial exit 추적
                    pending_sell_order_no   = p.get("pending_sell_order_no", ""),
                    pending_sell_qty_orig   = int(p.get("pending_sell_qty_orig", 0)),
                    pending_sell_qty_filled = int(p.get("pending_sell_qty_filled", 0)),
                    pending_sell_remaining  = int(p.get("pending_sell_remaining", 0)),
                )
                portfolio.positions[code] = pos
                restored += 1
            except (KeyError, ValueError, TypeError) as e:
                logger.warning("[StateManager] 포지션 복원 실패 (%s): %s", code, e)

        # 현금도 복원
        if "cash" in data:
            portfolio.cash = float(data["cash"])

        # 기준가 복원 (없으면 현재 equity로 초기화 → 일간손익 0%에서 시작)
        if "prev_close_equity" in data:
            portfolio.prev_close_equity = float(data["prev_close_equity"])
        else:
            portfolio.prev_close_equity = portfolio.get_current_equity()

        # 월간 peak_equity 복원 (없으면 현재 equity → DD 0%에서 시작)
        if "peak_equity" in data:
            portfolio.peak_equity = float(data["peak_equity"])
        if "peak_month" in data:
            portfolio._peak_month = int(data["peak_month"])

        # v7.8: synced_broker_equity 복원
        if "synced_broker_equity" in data:
            portfolio.synced_broker_equity = float(data["synced_broker_equity"])

        logger.info("[StateManager] 포지션 복원: %d개", restored)
        return restored

    # ── Runtime State ─────────────────────────────────────────────────────────

    def save_runtime(self, state: Dict[str, Any]) -> None:
        """
        런타임 메타 저장.
        state 예: {"regime": "BULL", "today_entries": ["005930", "000660"], ...}
        """
        payload = {
            "timestamp": datetime.now().isoformat(),
            **state,
        }
        self._write(self._runtime_file, payload)

    def load_runtime(self) -> Dict[str, Any]:
        """저장된 runtime_state.json 로드. 없으면 빈 dict."""
        data = self._read(self._runtime_file)
        return data or {}

    def is_already_entered_today(self, code: str) -> bool:
        """오늘 이미 진입한 종목인지 확인 (중복 진입 방지)."""
        state = self.load_runtime()
        today_str = date.today().isoformat()
        if state.get("date") != today_str:
            return False
        return code in state.get("today_entries", [])

    def mark_entered(self, codes: List[str]) -> None:
        """오늘 진입한 종목 코드를 runtime_state에 기록."""
        state = self.load_runtime()
        today_str = date.today().isoformat()
        if state.get("date") != today_str:
            state = {"date": today_str, "today_entries": [], "sl_cooldown": [], "sell_blocked": []}
        existing = set(state.get("today_entries", []))
        existing.update(codes)
        state["today_entries"] = list(existing)
        self.save_runtime(state)

    def mark_sl_cooldown(self, codes: List[str]) -> None:
        """SL/GAP_DOWN 청산된 종목을 당일 재진입 금지 목록에 추가."""
        state = self.load_runtime()
        today_str = date.today().isoformat()
        if state.get("date") != today_str:
            state = {"date": today_str, "today_entries": [], "sl_cooldown": [], "sell_blocked": []}
        existing = set(state.get("sl_cooldown", []))
        existing.update(codes)
        state["sl_cooldown"] = list(existing)
        self.save_runtime(state)
        if codes:
            logger.info("[StateManager] SL cooldown 등록: %s (총 %d개)", codes, len(existing))

    def mark_sell_blocked(self, codes: List[str]) -> None:
        """매도 거부 종목을 당일 재시도 차단 목록에 추가 (T+2 결제 등)."""
        state = self.load_runtime()
        today_str = date.today().isoformat()
        if state.get("date") != today_str:
            state = {"date": today_str, "today_entries": [], "sl_cooldown": [], "sell_blocked": []}
        existing = set(state.get("sell_blocked", []))
        existing.update(codes)
        state["sell_blocked"] = list(existing)
        self.save_runtime(state)
        if codes:
            logger.info("[StateManager] sell_blocked 등록: %s (총 %d개)", codes, len(existing))

    def get_sell_blocked(self) -> set:
        """오늘 매도 거부된 종목 코드 조회."""
        state = self.load_runtime()
        today_str = date.today().isoformat()
        if state.get("date") != today_str:
            return set()
        return set(state.get("sell_blocked", []))

    def get_sl_cooldown(self) -> set:
        """오늘 SL 당한 종목 코드 조회."""
        state = self.load_runtime()
        today_str = date.today().isoformat()
        if state.get("date") != today_str:
            return set()
        return set(state.get("sl_cooldown", []))

    # ── v7.9: closed_today (ghost fill 재오픈 방지, 영속) ────────────────────

    def mark_closed_today(self, codes: List[str]) -> None:
        """당일 청산 종목 영속 등록 (ghost fill 재오픈 방지, 세션 재시작에도 유지)."""
        if not codes:
            return
        state = self.load_runtime()
        today_str = date.today().isoformat()
        if state.get("date") != today_str:
            state = {"date": today_str, "today_entries": [], "sl_cooldown": [],
                     "sell_blocked": [], "closed_today": []}
        existing = set(state.get("closed_today", []))
        existing.update(codes)
        state["closed_today"] = list(existing)
        self.save_runtime(state)

    def get_closed_today(self) -> set:
        """당일 청산 종목 조회 (ghost fill 재오픈 방지)."""
        state = self.load_runtime()
        today_str = date.today().isoformat()
        if state.get("date") != today_str:
            return set()
        return set(state.get("closed_today", []))

    def mark_entries_done(self) -> None:
        """오늘 진입 라운드 완료 표시 (재시작 시 중복 진입 방지)."""
        state = self.load_runtime()
        today_str = date.today().isoformat()
        if state.get("date") != today_str:
            state = {"date": today_str, "today_entries": [], "sl_cooldown": [], "sell_blocked": []}
        state["entries_done"] = True
        state["entries_done_at"] = datetime.now().isoformat()
        self.save_runtime(state)
        logger.info("[StateManager] entries_done=True (%s)", today_str)

    def is_entries_done(self) -> bool:
        """오늘 진입 라운드가 이미 완료되었는지 확인."""
        state = self.load_runtime()
        today_str = date.today().isoformat()
        if state.get("date") != today_str:
            return False
        return bool(state.get("entries_done", False))

    # ── Order State ───────────────────────────────────────────────────────────

    def save_orders(self, orders: List[Dict]) -> None:
        """당일 주문 내역 저장 (중복 주문 방지용)."""
        payload = {
            "timestamp": datetime.now().isoformat(),
            "date":      date.today().isoformat(),
            "orders":    orders,
        }
        self._write(self._order_file, payload)

    def load_orders(self) -> List[Dict]:
        """저장된 주문 내역 로드. 날짜가 오늘과 다르면 빈 리스트."""
        data = self._read(self._order_file)
        if not data or data.get("date") != date.today().isoformat():
            return []
        return data.get("orders", [])

    # ── 유틸 ─────────────────────────────────────────────────────────────────

    def clear_runtime(self) -> None:
        """런타임 상태 파일 초기화 (장 종료 후 EOD 처리 시 사용).
        today_entries, sl_cooldown, entries_done 보존 — 같은 날 재실행 시 중복 진입 방지용.
        """
        existing = self._read(self._runtime_file) or {}
        today_str = date.today().isoformat()
        is_today = existing.get("date") == today_str
        today_entries  = existing.get("today_entries", []) if is_today else []
        sl_cooldown    = existing.get("sl_cooldown", [])   if is_today else []
        sell_blocked   = existing.get("sell_blocked", [])   if is_today else []
        entries_done   = existing.get("entries_done", False) if is_today else False
        entries_done_at = existing.get("entries_done_at", "") if is_today else ""
        state = {
            "date": today_str,
            "today_entries": today_entries,
            "sl_cooldown":   sl_cooldown,
            "sell_blocked":  sell_blocked,
        }
        if entries_done:
            state["entries_done"] = True
            state["entries_done_at"] = entries_done_at
        self._write(self._runtime_file, state)

    def state_summary(self) -> Dict[str, Any]:
        """현재 저장 상태 요약 반환."""
        port  = self.load_portfolio()
        rt    = self.load_runtime()
        return {
            "포지션수":     len(port.get("positions", {})) if port else 0,
            "현금(원)":     int(port.get("cash", 0)) if port else 0,
            "마지막저장":   port.get("timestamp", "없음") if port else "없음",
            "오늘진입종목": rt.get("today_entries", []),
            "레짐":         rt.get("regime", "없음"),
        }

    # ── 백업 수 ────────────────────────────────────────────────────────────
    _MAX_BACKUPS = 3

    def _write(self, path: Path, data: dict) -> None:
        """
        원자적 저장: tmp 파일에 쓴 뒤 rename.
        기존 파일은 .bak.1 ~ .bak.3 으로 3세대 백업.
        """
        tmp_path = path.with_suffix(".tmp")
        content = json.dumps(data, ensure_ascii=False, indent=2)

        try:
            tmp_path.write_text(content, encoding="utf-8")
        except OSError as e:
            logger.error("[StateManager] tmp 쓰기 실패 (%s): %s", tmp_path.name, e)
            return

        # 검증: tmp를 다시 읽어서 JSON 파싱 가능한지 확인
        try:
            json.loads(tmp_path.read_text(encoding="utf-8"))
        except (json.JSONDecodeError, OSError) as e:
            logger.error("[StateManager] tmp 검증 실패 (%s): %s — 원본 유지", tmp_path.name, e)
            tmp_path.unlink(missing_ok=True)
            return

        # 백업 로테이션: .bak.3 → 삭제, .bak.2 → .bak.3, .bak.1 → .bak.2, 원본 → .bak.1
        if path.exists():
            try:
                for i in range(self._MAX_BACKUPS, 1, -1):
                    older = path.with_suffix(f".bak.{i}")
                    newer = path.with_suffix(f".bak.{i - 1}")
                    if newer.exists():
                        shutil.move(str(newer), str(older))
                shutil.copy2(str(path), str(path.with_suffix(".bak.1")))
            except OSError as e:
                logger.warning("[StateManager] 백업 실패 (%s): %s — 계속 진행", path.name, e)

        # 원자적 rename (Windows: os.replace는 대상 존재 시 덮어쓰기)
        try:
            os.replace(str(tmp_path), str(path))
        except OSError as e:
            logger.error("[StateManager] rename 실패 (%s → %s): %s", tmp_path.name, path.name, e)

    def _read(self, path: Path) -> Optional[dict]:
        if not path.exists():
            return None
        try:
            return json.loads(path.read_text(encoding="utf-8"))
        except (json.JSONDecodeError, OSError, UnicodeDecodeError) as e:
            logger.error("[StateManager] 읽기 실패 (%s): %s — 백업에서 복구 시도", path.name, e)
            # 백업에서 복구 시도
            for i in range(1, self._MAX_BACKUPS + 1):
                bak = path.with_suffix(f".bak.{i}")
                if bak.exists():
                    try:
                        data = json.loads(bak.read_text(encoding="utf-8"))
                        logger.warning("[StateManager] 백업 .bak.%d 에서 복구 성공 (%s)", i, path.name)
                        return data
                    except (json.JSONDecodeError, OSError, UnicodeDecodeError):
                        continue
            logger.error("[StateManager] 모든 백업 복구 실패 (%s)", path.name)
            return None
