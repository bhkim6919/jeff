"""
TradeLogger (확장판)
====================
trades_extended.csv  — 체결 기록 + TP/SL/예상R:R 포함
filter_log.csv       — 스테이지별 필터 탈락 원인 통계
close_log.csv        — 청산 기록 (SL/TP 도달 여부)
"""

import csv, os
from datetime import datetime
from stage5_execution.execution_engine import TradeResult

LOG_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "logs"))

# ── 컬럼 정의 ────────────────────────────────────────────────────────────────
TRADE_COLS = [
    "timestamp", "code", "side", "quantity", "exec_price",
    "slippage_pct", "tp", "sl", "expected_rr", "q_score",
    "rejected", "reject_reason"
]
FILTER_COLS = ["date", "stage", "reason", "code"]
CLOSE_COLS  = ["timestamp", "code", "close_type",   # TP/SL/MANUAL
               "entry_price", "close_price", "realized_rr", "pnl"]


class TradeLogger:

    def __init__(self, log_dir=LOG_DIR):
        self.log_dir    = os.path.abspath(log_dir)
        self.trades_csv = os.path.join(self.log_dir, "trades.csv")
        self.filter_csv = os.path.join(self.log_dir, "filter_log.csv")
        self.close_csv  = os.path.join(self.log_dir, "close_log.csv")
        os.makedirs(self.log_dir, exist_ok=True)
        self._init_csv(self.trades_csv, TRADE_COLS)
        self._init_csv(self.filter_csv, FILTER_COLS)
        self._init_csv(self.close_csv,  CLOSE_COLS)

    # ── 체결 기록 ─────────────────────────────────────────────────────────────

    def log(self, result: TradeResult, tp=0, sl=0, expected_rr=0, q_score=0):
        self._append(self.trades_csv, TRADE_COLS, {
            "timestamp":     result.timestamp.strftime("%Y-%m-%d %H:%M:%S"),
            "code":          result.code,
            "side":          result.side,
            "quantity":      result.quantity,
            "exec_price":    round(result.exec_price, 2),
            "slippage_pct":  f"{result.slippage_pct:.6f}",
            "tp":            int(tp),
            "sl":            int(sl),
            "expected_rr":   round(expected_rr, 3),
            "q_score":       round(q_score, 2),
            "rejected":      result.rejected,
            "reject_reason": result.reject_reason,
        })

    def log_all(self, results, positioned=None):
        """
        results   : list[TradeResult]
        positioned: list[dict] from RiskManager (TP/SL/q_score 포함)
        """
        pos_map = {}
        if positioned:
            for p in positioned:
                pos_map[p["code"]] = p

        for result in results:
            p = pos_map.get(result.code, {})
            entry = float(p.get("entry_price", 0) or 0)
            tp    = float(p.get("tp", 0) or 0)
            sl    = float(p.get("sl", 0) or 0)
            exp_rr = p.get("rr_ratio", 0) or 0
            qs     = p.get("q_score", 0) or 0
            self.log(result, tp=tp, sl=sl, expected_rr=exp_rr, q_score=qs)

        accepted = [r for r in results if not r.rejected]
        rejected = [r for r in results if r.rejected]
        print(f"  [체결: {len(accepted)}건 / 거부: {len(rejected)}건]")

    # ── 필터 탈락 기록 ────────────────────────────────────────────────────────

    def log_filter(self, stage: str, reason: str, code: str):
        """
        stage  : 'Stage2_유동성' | 'Stage2_MA' | 'Stage2_모멘텀' 등
        reason : 탈락 사유 설명
        code   : 종목코드
        """
        self._append(self.filter_csv, FILTER_COLS, {
            "date":  datetime.now().strftime("%Y-%m-%d"),
            "stage": stage,
            "reason":reason,
            "code":  code,
        })

    def log_filter_batch(self, filter_results: list[dict]):
        """
        filter_results: [{"stage":..., "reason":..., "code":...}, ...]
        StockFilter에서 탈락 원인을 수집해서 일괄 전달.
        """
        for item in filter_results:
            self.log_filter(item["stage"], item["reason"], item["code"])
        if filter_results:
            print(f"  [FilterLog] {len(filter_results)}건 탈락 기록")

    # ── 청산 기록 (SL/TP 도달) ────────────────────────────────────────────────

    def log_close(self, code: str, close_type: str,
                  entry_price: float, close_price: float, pnl: float):
        """
        close_type: 'TP' | 'SL' | 'MANUAL'
        """
        entry = entry_price or 1
        rr    = (close_price - entry) / abs(entry - entry)  # 단순화
        realized_rr = round((close_price - entry) / entry * 100, 2)
        self._append(self.close_csv, CLOSE_COLS, {
            "timestamp":   datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "code":        code,
            "close_type":  close_type,
            "entry_price": int(entry_price),
            "close_price": int(close_price),
            "realized_rr": realized_rr,
            "pnl":         round(pnl, 0),
        })

    # ── 유틸 ─────────────────────────────────────────────────────────────────

    @staticmethod
    def _init_csv(path, cols):
        if not os.path.exists(path):
            with open(path, "w", newline="", encoding="utf-8-sig") as f:
                csv.DictWriter(f, fieldnames=cols).writeheader()

    @staticmethod
    def _append(path, cols, row):
        with open(path, "a", newline="", encoding="utf-8-sig") as f:
            csv.DictWriter(f, fieldnames=cols).writerow(row)
