# -*- coding: utf-8 -*-
"""
StateManager
============
런타임 상태 영속화 모듈.

저장 파일 위치: state/
  portfolio_state.json  — 보유 포지션 (code, qty, avg_price, entry_date, tp, sl ...)
  runtime_state.json    — 런타임 메타 (마지막 실행 시각, 레짐, 오늘 진입 종목 목록)

문제 시나리오 방어:
  - 재시작 시 포지션 복구  → load_portfolio()
  - 당일 중복 진입 방지    → runtime_state.today_entries
  - 포지션 누락 감지       → 로드 후 KiwoomProvider 현재가와 대조
"""

import json
import logging
from datetime import date, datetime
from pathlib import Path
from typing import Any, Dict, List, Optional

from config import Gen3Config

logger = logging.getLogger("StateManager")


class StateManager:

    def __init__(self, config: Gen3Config):
        self.state_dir = config.abs_path("state")
        self.state_dir.mkdir(parents=True, exist_ok=True)

        self._portfolio_file = self.state_dir / "portfolio_state.json"
        self._runtime_file   = self.state_dir / "runtime_state.json"
        self._order_file     = self.state_dir / "order_state.json"

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
            }

        self._write(self._portfolio_file, state)
        logger.debug("[StateManager] portfolio_state.json 저장 (%d 포지션)", len(state["positions"]))

    def load_portfolio(self) -> Optional[Dict[str, Any]]:
        """
        저장된 portfolio_state.json 로드.
        없거나 파싱 오류 시 None 반환.
        """
        data = self._read(self._portfolio_file)
        if data is None:
            logger.info("[StateManager] portfolio_state.json 없음 → 초기 상태로 시작")
        return data

    def restore_portfolio(self, portfolio) -> int:
        """
        저장된 상태로 PortfolioManager 포지션 복원.
        반환: 복원된 포지션 수
        """
        data = self.load_portfolio()
        if not data or not data.get("positions"):
            return 0

        from core.position_tracker import Position
        restored = 0
        for code, p in data["positions"].items():
            try:
                pos = Position(
                    code          = p["code"],
                    sector        = p.get("sector", ""),
                    quantity      = int(p["quantity"]),
                    avg_price     = float(p["avg_price"]),
                    current_price = float(p.get("current_price", p["avg_price"])),
                    entry_date    = date.fromisoformat(p["entry_date"]),
                    tp            = float(p.get("tp", 0)),
                    sl            = float(p.get("sl", 0)),
                    q_score       = float(p.get("q_score", 0)),
                    rr_ratio      = float(p.get("rr_ratio", 0)),
                )
                portfolio.positions[code] = pos
                restored += 1
            except Exception as e:
                logger.warning("[StateManager] 포지션 복원 실패 (%s): %s", code, e)

        # 현금도 복원
        if "cash" in data:
            portfolio.cash = float(data["cash"])

        # 기준가 복원 (없으면 현재 equity로 초기화 → 일간손익 0%에서 시작)
        if "prev_close_equity" in data:
            portfolio.prev_close_equity = float(data["prev_close_equity"])
        else:
            portfolio.prev_close_equity = portfolio.get_current_equity()

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
            state = {"date": today_str, "today_entries": []}
        existing = set(state.get("today_entries", []))
        existing.update(codes)
        state["today_entries"] = list(existing)
        self.save_runtime(state)

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
        """런타임 상태 파일 초기화 (장 종료 후 EOD 처리 시 사용)."""
        self._write(self._runtime_file, {"date": date.today().isoformat(), "today_entries": []})

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

    def _write(self, path: Path, data: dict) -> None:
        try:
            path.write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8")
        except Exception as e:
            logger.error("[StateManager] 저장 실패 (%s): %s", path.name, e)

    def _read(self, path: Path) -> Optional[dict]:
        if not path.exists():
            return None
        try:
            return json.loads(path.read_text(encoding="utf-8"))
        except Exception as e:
            logger.error("[StateManager] 읽기 실패 (%s): %s", path.name, e)
            return None
