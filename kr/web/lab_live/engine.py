"""
engine.py -- Lab Live Simulator (9-Strategy Forward Paper Trading)
===================================================================
Surge 패턴 기반. 9개 전략을 실시간 forward paper trading.
Phase 1: EOD daily run. Phase 2: live P&L. Phase 3: intraday.
"""
from __future__ import annotations
import copy
import json
import logging
import sys
import threading
import time
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional

import numpy as np
import pandas as pd

import hashlib
import os

from web.lab_live.config import LabLiveConfig
from web.lab_live.state_store import (
    save_state, load_state, save_trades, load_trades, append_equity,
    atomic_write_json, safe_read_json,
    save_state_v2, load_state_v2, archive_state_v2,
)


# ── Date Lock (file-based, stale recovery) ──────────────

def _acquire_date_lock(lock_dir: Path, eod_date: str, stale_sec: float = 1800) -> bool:
    """Acquire file lock for date. Returns False if held by active process."""
    lock_dir.mkdir(parents=True, exist_ok=True)
    lock_path = lock_dir / f"{eod_date}.lock"

    if lock_path.exists():
        try:
            lock_data = json.loads(lock_path.read_text(encoding="utf-8"))
            pid = lock_data.get("pid", 0)
            started = lock_data.get("started_at", "")

            # PID alive check
            pid_alive = True
            try:
                os.kill(pid, 0)
            except (OSError, ProcessLookupError):
                pid_alive = False

            # Stale check (30 min)
            stale = False
            if started:
                try:
                    dt = datetime.fromisoformat(started)
                    age = (datetime.now() - dt).total_seconds()
                    stale = age > stale_sec
                except Exception:
                    stale = True

            if pid_alive and not stale:
                return False  # Lock held

            logging.getLogger("lab.live").warning(
                f"[LAB_LOCK] Stale lock for {eod_date}, clearing"
            )
        except Exception:
            pass
        lock_path.unlink(missing_ok=True)

    # Acquire
    atomic_write_json(lock_path, {
        "pid": os.getpid(),
        "started_at": datetime.now().isoformat(),
        "eod_date": eod_date,
    })
    return True


def _release_date_lock(lock_dir: Path, eod_date: str):
    lock_path = lock_dir / f"{eod_date}.lock"
    lock_path.unlink(missing_ok=True)


# ── Snapshot ID (reproducibility) ───────────────────────

def _compute_data_snapshot_id(close_df: pd.DataFrame, dates: pd.Series) -> str:
    """Content-based hash of the final price matrix used by strategies.

    matrix_hash는 전략 입력에 실제 사용된 최종 가격 행렬의 short hash.
    종목 수 + 날짜 범위 + 샘플 가격값을 포함하여, 동일 입력 → 동일 hash 보장.
    종목 순서가 다르면 hash가 달라질 수 있음 (의도된 동작: 데이터 구성 변화 감지).
    """
    h = hashlib.sha256()
    h.update(str(len(close_df.columns)).encode())
    h.update(str(len(dates)).encode())
    if len(dates) > 0:
        h.update(str(dates.iloc[0]).encode())
        h.update(str(dates.iloc[-1]).encode())
    # Sample first/last column values for content verification
    for col in list(close_df.columns)[:10]:
        vals = close_df[col].dropna()
        if len(vals) > 0:
            h.update(f"{vals.iloc[0]:.2f}".encode())
            h.update(f"{vals.iloc[-1]:.2f}".encode())
    return h.hexdigest()[:16]

logger = logging.getLogger("lab_live.engine")

# Add parent for lab imports
_gen04_rest = Path(__file__).resolve().parent.parent.parent
sys.path.insert(0, str(_gen04_rest))


@dataclass
class LabLivePosition:
    code: str
    name: str
    qty: int
    entry_price: float
    entry_date: str
    high_wm: float
    buy_cost_total: float
    current_price: float = 0.0
    entry_day_idx: int = 0


@dataclass
class LabLiveLane:
    name: str
    group: str
    cash: float
    positions: Dict[str, LabLivePosition] = field(default_factory=dict)
    pending_buys: List[dict] = field(default_factory=list)
    trades: List[dict] = field(default_factory=list)
    equity_history: List[dict] = field(default_factory=list)
    last_rebal_idx: int = -999
    strategy: Any = None  # BaseStrategy instance


class LabLiveSimulator:
    """9-Strategy Forward Paper Trading Engine."""

    def __init__(self, config: LabLiveConfig = None):
        self.config = config or LabLiveConfig()
        self._lock = threading.RLock()
        self._lanes: Dict[str, LabLiveLane] = {}
        self._initialized = False
        self._running = False
        self._last_run_date = ""
        self._last_snapshot_version = ""
        self._sector_map = {}
        self._start_time = None
        self._data_snapshot_id = ""
        self._missing_data_ratio = 0.0

    # ── Initialization ───────────────────────────────────────

    def initialize(self, reset: bool = False) -> dict:
        """9개 전략 초기화. reset=True면 archive 후 1억 재시작."""
        from lab.lab_config import STRATEGY_CONFIGS
        from lab.runner import create_strategy, load_sector_map

        with self._lock:
            # Archive before reset (audit trail 보존)
            if reset:
                self._archive_state()

            # Load sector map
            self._sector_map = load_sector_map(self.config.sector_map_file)

            # Try restore from saved state (v2: per-strategy + HEAD)
            saved = None
            _corrupted = False
            if not reset:
                saved = load_state_v2(self.config)
                if saved and saved.get("status") == "CORRUPTED":
                    logger.error(f"[LAB_LIVE] State CORRUPTED: {saved.get('message')}")
                    _corrupted = True
                    saved = None  # fall through to fresh start

            strategy_names = list(STRATEGY_CONFIGS.keys())

            for sname in strategy_names:
                scfg = STRATEGY_CONFIGS[sname]
                try:
                    strategy = create_strategy(sname)
                except Exception as e:
                    logger.error(f"[LAB_LIVE] Failed to create {sname}: {e}")
                    continue

                # Restore or fresh start
                if saved and sname in saved.get("lanes", {}):
                    lane_data = saved["lanes"][sname]
                    lane = LabLiveLane(
                        name=sname,
                        group=scfg.group,
                        cash=lane_data["cash"],
                        last_rebal_idx=lane_data.get("last_rebal_idx", -999),
                        equity_history=lane_data.get("equity_history", []),
                        strategy=strategy,
                    )
                    # Restore positions
                    for code, pdata in lane_data.get("positions", {}).items():
                        lane.positions[code] = LabLivePosition(
                            code=code,
                            name=pdata.get("name", code),
                            qty=pdata["qty"],
                            entry_price=pdata["entry_price"],
                            entry_date=pdata.get("entry_date", ""),
                            high_wm=pdata.get("high_wm", pdata["entry_price"]),
                            buy_cost_total=pdata.get("buy_cost_total", 0),
                            current_price=pdata.get("current_price", pdata["entry_price"]),
                            entry_day_idx=pdata.get("entry_day_idx", 0),
                        )
                    # Restore pending buys
                    lane.pending_buys = lane_data.get("pending_buys", [])
                    logger.info(f"[LAB_LIVE] Restored {sname}: "
                                f"cash={lane.cash:,.0f}, positions={len(lane.positions)}")
                else:
                    lane = LabLiveLane(
                        name=sname,
                        group=scfg.group,
                        cash=float(self.config.initial_cash),
                        strategy=strategy,
                    )

                # Restore strategy internal state
                if strategy and hasattr(strategy, '_last_rebal_idx'):
                    strategy._last_rebal_idx = lane.last_rebal_idx

                self._lanes[sname] = lane

            if saved:
                self._last_run_date = saved.get("last_run_date", "")
                self._last_snapshot_version = saved.get("snapshot_version", "")
                # v2 includes trades; fallback to file load
                self._all_trades = saved.get("trades", [])
                if not self._all_trades:
                    self._all_trades = load_trades(self.config.trades_file)
                self._equity_rows = saved.get("equity_rows", [])
            else:
                self._all_trades = []
                self._equity_rows = []

            self._initialized = True
            self._running = True
            self._start_time = datetime.now()

            result = {
                "ok": True,
                "lanes": len(self._lanes),
                "restored": saved is not None and not reset,
                "last_run_date": self._last_run_date,
            }
            if _corrupted:
                result["warning"] = "CORRUPTED"
                result["message"] = "State was corrupted — started fresh"
            if saved and saved.get("recovered_from"):
                result["recovered_from"] = saved["recovered_from"]
            return result

    # ── Daily Run (Phase 1 핵심) ─────────────────────────────

    def run_daily(self) -> dict:
        """EOD 신호 생성 + 가상 체결. 하루 한 번 실행."""
        from lab.snapshot import build_snapshot, safe_slice
        from lab.universe import build_universe

        with self._lock:
            if not self._initialized:
                return {"error": "Not initialized"}

            t0 = time.time()
            today_str = datetime.now().strftime("%Y-%m-%d")
            lock_dir = self.config.state_dir / "locks"

            # Date lock — prevent concurrent/duplicate EOD
            if not _acquire_date_lock(lock_dir, today_str):
                return {"error": f"Date lock held for {today_str}. Another run in progress."}

            try:
                return self._run_daily_locked(t0, lock_dir, today_str)
            finally:
                _release_date_lock(lock_dir, today_str)

    def _run_daily_locked(self, t0, lock_dir, lock_date) -> dict:
        from lab.snapshot import build_snapshot, safe_slice
        from lab.universe import build_universe
        from lab.runner import load_fundamental

        with self._lock:
            logger.info("[LAB_LIVE] Daily run starting...")

            # ── Data Load: DB 우선, stale 시 CSV fallback ──
            # Truth source 원칙: raw ingestion = CSV, serving = DB
            # DB stale 시 blind trust 금지 → CSV fallback
            selected_source = None
            db_last_date = None
            csv_last_date = None

            def _load_csv():
                # 날짜축 결정: **종목 union 기준** (KOSPI 의존 분리)
                # KOSPI는 보조지표로 reindex(ffill)하여 dates에 맞춤.
                # 기존 버그: dts = idf["date"] (KOSPI 기준) → KOSPI stale 시 종목 17일도 drop
                from lab.runner import load_ohlcv, build_matrices as bm
                from web.lab_live.market_context import build_stock_dates_from_csv
                all_data = load_ohlcv(self.config.ohlcv_dir, self.config.univ_min_history)
                idf = pd.read_csv(self.config.index_file)
                date_col = "index" if "index" in idf.columns else "date"
                rename_map = {date_col: "date"}
                for s, d_ in [("Open", "open"), ("High", "high"), ("Low", "low"),
                               ("Close", "close"), ("Volume", "volume")]:
                    if s in idf.columns:
                        rename_map[s] = d_
                idf = idf.rename(columns=rename_map)
                idf["date"] = pd.to_datetime(idf["date"], errors="coerce")
                idf = idf.dropna(subset=["date"]).sort_values("date").reset_index(drop=True)
                for c in ["open", "high", "low", "close", "volume"]:
                    if c in idf.columns:
                        idf[c] = pd.to_numeric(idf[c], errors="coerce").fillna(0)
                dts = build_stock_dates_from_csv(all_data)
                if len(dts) == 0:
                    # 극단적 fallback: 종목 union이 빈 경우 기존 로직 재현
                    dts = idf["date"]
                cl, op, hi, lo, vo = bm(all_data, dts)
                ic = idf.set_index("date")["close"].reindex(dts).ffill()
                return cl, op, hi, lo, vo, dts, ic

            # CSV last date 먼저 확인 (기준점)
            try:
                _sample_csvs = sorted(self.config.ohlcv_dir.glob("*.csv"))[:5]
                for _sf in _sample_csvs:
                    _sdf = pd.read_csv(_sf, usecols=["date"], parse_dates=["date"])
                    _ld = str(_sdf["date"].max().date())
                    if csv_last_date is None or _ld > csv_last_date:
                        csv_last_date = _ld
            except Exception:
                pass

            # KOSPI 날짜 분리 저장 (market_context 결정용)
            # dates = stock 기준 (기존 dates = idx_df["date"] 버그 제거)
            kospi_dates_series = None

            try:
                from data.db_provider import DbProvider
                db = DbProvider()
                close, opn, high, low, vol, dates = db.build_matrices()
                idx_df = db.get_kospi_index()
                # KOSPI dates 별도 보관 (dates 덮어쓰기 금지)
                kospi_dates_series = idx_df["date"].copy() if not idx_df.empty else None
                # KOSPI close는 stock dates에 reindex(ffill) — stale 시 마지막 값 유지
                idx_close = idx_df.set_index("date")["close"].reindex(dates).ffill()
                db_last_date = str(dates.iloc[-1].date()) if len(dates) > 0 else None

                # Stale 검증: DB가 CSV보다 오래되면 CSV fallback
                if csv_last_date and db_last_date and db_last_date < csv_last_date:
                    logger.warning(
                        f"[DATA_STALE_DB] db_last={db_last_date} < csv_last={csv_last_date}, "
                        f"falling back to CSV"
                    )
                    close, opn, high, low, vol, dates, idx_close = _load_csv()
                    selected_source = "CSV"
                    reason = f"DB stale ({db_last_date} < {csv_last_date})"
                    # CSV fallback 시 KOSPI dates도 CSV 기준으로 재로딩
                    try:
                        _idf_csv = pd.read_csv(self.config.index_file)
                        _dc = "index" if "index" in _idf_csv.columns else "date"
                        kospi_dates_series = pd.to_datetime(_idf_csv[_dc], errors="coerce").dropna()
                    except Exception:
                        kospi_dates_series = None
                else:
                    selected_source = "DB"
                    reason = "DB current"
                logger.info(f"[LAB_LIVE] {selected_source} load: {len(close.columns)} stocks")
            except Exception as e:
                logger.warning(f"[LAB_LIVE] DB failed ({e}), CSV fallback")
                close, opn, high, low, vol, dates, idx_close = _load_csv()
                selected_source = "CSV"
                reason = f"DB error ({e})"
                try:
                    _idf_csv = pd.read_csv(self.config.index_file)
                    _dc = "index" if "index" in _idf_csv.columns else "date"
                    kospi_dates_series = pd.to_datetime(_idf_csv[_dc], errors="coerce").dropna()
                except Exception:
                    kospi_dates_series = None

            # ── OHLCV Sync 결과 소비 ──
            # PARTIAL 정책:
            #   OK   → 정상 실행 허용
            #   PARTIAL → 실행 허용, 단 run_meta에 기록 + 경고 로그
            #   FAIL → DB blind trust 금지, CSV fallback 강제
            # date freshness와 universe completeness는 다른 문제:
            #   stale 검증(위)은 freshness, 여기는 completeness
            ohlcv_sync = {}
            try:
                _sync_path = self.config.state_dir / "ohlcv_sync.json"
                if _sync_path.exists():
                    ohlcv_sync = json.loads(_sync_path.read_text(encoding="utf-8"))
            except Exception:
                pass

            if ohlcv_sync:
                _ss = ohlcv_sync.get("sync_status", "?")
                _cr = ohlcv_sync.get("completeness_ratio", 0)
                if _ss == "FAIL" and selected_source == "DB":
                    logger.warning(
                        f"[DATA_SYNC_POLICY] sync_status=FAIL, "
                        f"forcing CSV fallback (DB blind trust 금지)"
                    )
                    close, opn, high, low, vol, dates, idx_close = _load_csv()
                    selected_source = "CSV"
                    reason = f"OHLCV sync FAIL → CSV fallback"
                elif _ss == "PARTIAL":
                    logger.warning(
                        f"[DATA_SYNC_POLICY] sync_status=PARTIAL, "
                        f"completeness={_cr:.1%}, proceeding with {selected_source}"
                    )

            # Today = latest COMPLETED trading date
            # 장중(당일 데이터 불완전)이면 전일 기준으로 실행
            from datetime import datetime as _dt
            _calendar_today = _dt.now().strftime("%Y-%m-%d")
            _data_last = str(dates.iloc[-1].date()) if len(dates) > 0 else ""

            if _data_last == _calendar_today and len(dates) >= 2:
                # 당일 데이터 존재하지만 장중 → 전일 기준 실행
                # (EOD 확정 시간 이후에는 당일 사용)
                _eod_hour = 16  # 16시 이후면 당일 EOD 확정
                if _dt.now().hour < _eod_hour:
                    today_idx = len(dates) - 2
                    logger.info(
                        f"[DATA_DATE] Intraday detected: data_last={_data_last}, "
                        f"using prev={dates.iloc[today_idx].date()} (before {_eod_hour}:00)"
                    )
                else:
                    today_idx = len(dates) - 1
            else:
                today_idx = len(dates) - 1

            today_date = str(dates.iloc[today_idx].date())

            # ── Market Context (KOSPI readiness 분리) ──
            # 종목 기준 dates로 결정된 effective_trade_date에 대해 KOSPI 도달 여부 판정.
            # KOSPI stale 이어도 엔진은 정상 진행, run_mode=DEGRADED로만 표시.
            from web.lab_live.market_context import resolve_effective_trade_date, SupplyStatus
            _ctx_dates = dates.iloc[:today_idx + 1]  # today까지만 고려

            # engine 시점의 KOSPI supply_status 자동 판정 (Step 2)
            # batch 의 best_status 와 독립 — 이건 "엔진이 로드한 데이터 기준 신선도"
            _supply_status = None
            try:
                if kospi_dates_series is None or len(kospi_dates_series) == 0:
                    _supply_status = SupplyStatus.EMPTY_NOT_READY
                else:
                    _kospi_last = str(pd.to_datetime(kospi_dates_series.max()).date())
                    _engine_today = str(dates.iloc[today_idx].date())
                    _supply_status = (SupplyStatus.SUCCESS_TODAY
                                      if _kospi_last >= _engine_today
                                      else SupplyStatus.SUCCESS_STALE)
            except Exception:
                _supply_status = SupplyStatus.ERROR

            market_ctx = resolve_effective_trade_date(
                stock_dates=_ctx_dates,
                kospi_dates=kospi_dates_series,
                supply_status=_supply_status,
            )
            if market_ctx.run_mode == "DEGRADED":
                logger.warning(
                    f"[MARKET_CTX] DEGRADED: effective={market_ctx.effective_trade_date}, "
                    f"kospi_last={market_ctx.kospi_last_date}, "
                    f"reasons={market_ctx.degraded_reasons}"
                )
                try:
                    from web.data_events import emit_event, Level
                    emit_event(
                        source="MARKET_CTX",
                        level=Level.WARN,
                        code="degraded",
                        message=(
                            f"엔진 DEGRADED — "
                            f"stock={market_ctx.stock_last_date}, "
                            f"kospi={market_ctx.kospi_last_date}"
                        ),
                        details={
                            "effective_trade_date": market_ctx.effective_trade_date,
                            "degraded_reasons": market_ctx.degraded_reasons,
                        },
                        telegram=False,
                    )
                except Exception:
                    pass
            else:
                logger.info(
                    f"[MARKET_CTX] OK: trade_date={market_ctx.effective_trade_date}, "
                    f"kospi_ready=True"
                )
                # Recovery signal — DEGRADED 였던 source/code reset
                try:
                    from web.data_events import emit_event, Level
                    emit_event(
                        source="MARKET_CTX",
                        level=Level.INFO,
                        code="degraded",
                        message=f"OK — trade_date={market_ctx.effective_trade_date}",
                        telegram=False,
                    )
                except Exception:
                    pass

            logger.info(
                f"[DATA_SOURCE_SELECT] trade_date={today_date}, "
                f"db_last={db_last_date}, csv_last={csv_last_date}, "
                f"source={selected_source}, reason={reason}"
            )

            # ── Idempotency: snapshot_version 기반 skip ──
            # 구성: trade_date:source:data_last_date:universe_count:matrix_hash
            # trade_date만 같다고 멱등성이 보장되지 않음 — 입력 데이터 버전이 동일해야 함
            _snap_id_pre = _compute_data_snapshot_id(close, dates)
            data_last_date = str(dates.iloc[-1].date()) if len(dates) > 0 else "?"
            _univ_count = len(close.columns)
            snapshot_version = (
                f"{today_date}:{selected_source}:{data_last_date}"
                f":{_univ_count}:{_snap_id_pre}"
            )

            # 런 메타 저장용 (head.json + result에 포함)
            # snapshot_version 구성:
            #   trade_date:source:data_last_date:universe_count:matrix_hash
            #   matrix_hash = _compute_data_snapshot_id() 반환값 (가격행렬 short hash)
            self._run_meta = {
                "selected_source": selected_source,
                "data_last_date": data_last_date,
                "csv_last_date": csv_last_date or "?",
                "db_last_date": db_last_date or "?",
                "universe_count": _univ_count,
                "matrix_hash": _snap_id_pre,
                "snapshot_version": snapshot_version,
                # OHLCV sync 결과 (daily_runner가 저장한 shared state)
                "sync_status": ohlcv_sync.get("sync_status", "?"),
                "sync_completeness": ohlcv_sync.get("completeness_ratio", None),
                "sync_failed_count": ohlcv_sync.get("failed_count", None),
                # Market Context (KOSPI readiness 별도 플래그)
                "market_context": market_ctx.to_dict(),
            }

            # 신규 전략 감지: equity_history가 비어있는 lane이 있으면 skip 금지
            _unprocessed = [n for n, ln in self._lanes.items()
                            if not ln.equity_history]
            if today_date == self._last_run_date:
                if self._last_snapshot_version == snapshot_version and not _unprocessed:
                    logger.info(
                        f"[EOD_IDEMPOTENCY] skip: trade_date={today_date}, "
                        f"sv={snapshot_version}"
                    )
                    return {"skipped": True, "date": today_date,
                            "snapshot_version": snapshot_version}
                elif _unprocessed:
                    logger.info(
                        f"[EOD_IDEMPOTENCY] re-run: {len(_unprocessed)} unprocessed lanes "
                        f"({', '.join(_unprocessed[:5])})"
                    )
                else:
                    logger.warning(
                        f"[EOD_IDEMPOTENCY] re-run: trade_date={today_date}, "
                        f"sv={snapshot_version} != last={self._last_snapshot_version}"
                    )

            # Universe
            univ = build_universe(close, vol, today_idx,
                                  self.config.univ_min_close, self.config.univ_min_amount)

            # Missing data filter — exclude tickers with >20% NaN in lookback
            _min_hist = 252
            _max_missing = 0.20
            _filtered_univ = []
            _excluded_count = 0
            for tk in univ:
                if tk not in close.columns:
                    _excluded_count += 1
                    continue
                series = close[tk].iloc[max(0, today_idx - _min_hist):today_idx + 1]
                actual = series.notna().sum()
                expected = min(_min_hist, today_idx + 1)
                if expected > 0 and (1 - actual / expected) > _max_missing:
                    _excluded_count += 1
                    continue
                _filtered_univ.append(tk)
            if _filtered_univ:
                univ = _filtered_univ
            self._missing_data_ratio = round(
                _excluded_count / (len(univ) + _excluded_count) * 100, 1
            ) if (len(univ) + _excluded_count) > 0 else 0

            # Fundamental
            fund_df = load_fundamental(self.config.fundamental_dir, dates.iloc[today_idx])

            # Snapshot
            snapshot = build_snapshot(
                today_idx, dates, close, opn, high, low, vol,
                univ, self._sector_map, idx_close, fund_df,
            )

            # Name lookup
            name_lookup = {}
            for code, info in self._sector_map.items():
                if isinstance(info, dict):
                    name_lookup[code] = info.get("name", code)

            new_trades = []
            equity_row = {"date": today_date}

            for sname, lane in sorted(self._lanes.items()):
                try:
                    strategy = lane.strategy
                    if not strategy:
                        continue

                    # Sync strategy rebal idx
                    strategy._last_rebal_idx = lane.last_rebal_idx

                    # 1. Fill pending buys at today's open
                    filled = []
                    retained = []  # NaN 가격 등으로 체결 못한 신호 보류
                    _actual_filled_count = 0
                    for pb in lane.pending_buys:
                        tk = pb["ticker"]
                        if tk in lane.positions:
                            filled.append(pb)
                            continue
                        entry_price = float(opn[tk].iloc[today_idx]) if tk in opn.columns and not pd.isna(opn[tk].iloc[today_idx]) else 0
                        if entry_price <= 0:
                            # 가격 NaN: silent drop 금지, 보류 유지 (최대 2일)
                            pb_age = pb.get("_retry", 0) + 1
                            if pb_age >= 2:
                                logger.warning(
                                    f"[PENDING_EXPIRE] {sname}/{tk}: "
                                    f"open NaN for {pb_age} days, expiring"
                                )
                                filled.append(pb)
                            else:
                                pb["_retry"] = pb_age
                                retained.append(pb)
                                logger.info(
                                    f"[PENDING_RETAIN] {sname}/{tk}: "
                                    f"open NaN, retaining (retry={pb_age})"
                                )
                            continue

                        max_pos = strategy.config.max_positions
                        if len(lane.positions) + _actual_filled_count >= max_pos:
                            logger.info(
                                f"[PENDING_REJECT] {sname}/{tk}: "
                                f"max_positions={max_pos} reached"
                            )
                            filled.append(pb)
                            continue

                        buy_cost = entry_price * (1 + self.config.buy_cost)
                        per_pos = pb.get("per_pos", lane.cash * self.config.cash_buffer / max(1, max_pos - len(lane.positions) - _actual_filled_count))
                        available = min(per_pos, lane.cash * self.config.cash_buffer)
                        qty = int(available / buy_cost)

                        if qty <= 0 or qty * buy_cost > lane.cash:
                            logger.info(
                                f"[PENDING_REJECT] {sname}/{tk}: "
                                f"insufficient cash (qty={qty}, cost={buy_cost:.0f})"
                            )
                            filled.append(pb)
                            continue

                        # [BUG_GUARD] today_idx <-> today_date 정합성 검증
                        # 과거 버그: entry_price는 idx 기반으로 저장되지만 entry_date가
                        # 다른 캘린더로 stamp → 재발 시 즉시 감지
                        _idx_date = str(dates.iloc[today_idx].date())
                        if _idx_date != today_date:
                            logger.error(
                                f"[BUG_GUARD] today_idx/today_date mismatch: "
                                f"idx={today_idx} -> dates.iloc={_idx_date}, "
                                f"today_date={today_date}, strategy={sname}, ticker={tk}"
                            )
                            raise RuntimeError(
                                f"entry_date/entry_price alignment broken: "
                                f"idx={today_idx}({_idx_date}) vs today_date={today_date}"
                            )
                        lane.cash -= qty * buy_cost
                        lane.positions[tk] = LabLivePosition(
                            code=tk,
                            name=name_lookup.get(tk, tk),
                            qty=qty,
                            entry_price=entry_price,
                            entry_date=today_date,
                            high_wm=entry_price,
                            buy_cost_total=qty * entry_price * self.config.buy_cost,
                            current_price=float(close[tk].iloc[today_idx]) if tk in close.columns else entry_price,
                            entry_day_idx=today_idx,
                        )
                        filled.append(pb)
                        _actual_filled_count += 1

                    # 체결/만료된 것만 제거, 보류는 유지
                    lane.pending_buys = retained

                    logger.debug(
                        f"[POSITION_COUNT_RECON] {sname}: filled={_actual_filled_count}, "
                        f"positions={len(lane.positions)}, pending={len(lane.pending_buys)}, "
                        f"effective={len(lane.positions) + len(lane.pending_buys)}"
                    )

                    # 2. Exit policy — track sold tickers for same-day re-entry block
                    _exit_sold = set()
                    for tk in list(lane.positions.keys()):
                        pos = lane.positions[tk]
                        pos_dict = {
                            "ticker": tk, "qty": pos.qty,
                            "entry_price": pos.entry_price,
                            "entry_idx": pos.entry_day_idx,
                            "high_wm": pos.high_wm,
                            "buy_cost_total": pos.buy_cost_total,
                        }
                        reason = strategy.exit_policy.check_exit(snapshot, pos_dict, strategy._state)
                        pos.high_wm = pos_dict.get("high_wm", pos.high_wm)

                        if reason:
                            trade = self._close_position(lane, tk, snapshot, reason, today_date)
                            if trade:
                                new_trades.append(trade)
                                _exit_sold.add(tk)

                    # 3. Generate signals
                    pos_copy = {tk: {
                        "qty": p.qty, "entry_price": p.entry_price,
                        "entry_idx": p.entry_day_idx, "high_wm": p.high_wm,
                        "buy_cost_total": p.buy_cost_total, "ticker": tk,
                    } for tk, p in lane.positions.items()}

                    signals = strategy.generate_signals(snapshot, copy.deepcopy(pos_copy))

                    # 4. SELL signals — combine with exit-sold for same-day re-entry block
                    _sold_today = set(_exit_sold)
                    for sig in signals:
                        if sig.direction == "SELL" and sig.ticker in lane.positions:
                            trade = self._close_position(lane, sig.ticker, snapshot, sig.reason, today_date)
                            if trade:
                                new_trades.append(trade)
                                _sold_today.add(sig.ticker)

                    # 5. BUY signals → pending for T+1 (same-day re-entry 금지)
                    max_pos = strategy.config.max_positions
                    current = len(lane.positions) + len(lane.pending_buys)
                    pv = lane.cash + sum(
                        p.qty * float(close[p.code].iloc[today_idx])
                        for p in lane.positions.values()
                        if p.code in close.columns and float(close[p.code].iloc[today_idx]) > 0
                    )
                    per_pos = pv / max_pos if max_pos > 0 else 0

                    buy_sigs = sorted(
                        [s for s in signals if s.direction == "BUY"],
                        key=lambda s: -s.priority
                    )
                    for sig in buy_sigs:
                        if current >= max_pos:
                            break
                        if sig.ticker in lane.positions:
                            continue
                        if sig.ticker in _sold_today:
                            continue  # same-day re-entry 금지
                        if any(pb["ticker"] == sig.ticker for pb in lane.pending_buys):
                            continue
                        lane.pending_buys.append({
                            "ticker": sig.ticker,
                            "per_pos": per_pos,
                            "signal_date": today_date,
                        })
                        current += 1

                    # Update rebal idx
                    lane.last_rebal_idx = strategy._last_rebal_idx

                    # 6. Update current prices + record equity
                    equity = lane.cash
                    for tk, pos in lane.positions.items():
                        c = float(close[tk].iloc[today_idx]) if tk in close.columns else pos.entry_price
                        if c > 0 and not pd.isna(c):
                            pos.current_price = c
                            equity += pos.qty * c

                    # Upsert-by-date: re-runs of the same trade_date must not
                    # accumulate duplicate equity_history rows. Without this,
                    # an OHLCV CSV stuck at one last-date (2026-04-22 incident
                    # pattern) — or any code path that re-enters EOD with the
                    # same today_date — produces N rows of the same date and
                    # poisons every downstream metric (Sharpe / Sortino / CAGR
                    # all collapse on the dedup'd dict). 2026-04-26: Jeff hit
                    # exactly this — equity.json had 12 rows all 2026-04-10.
                    new_eh_row = {
                        "date": today_date, "equity": equity,
                        "n_positions": len(lane.positions),
                    }
                    if lane.equity_history and lane.equity_history[-1].get("date") == today_date:
                        lane.equity_history[-1] = new_eh_row
                    else:
                        lane.equity_history.append(new_eh_row)
                    equity_row[sname] = equity

                except Exception as e:
                    logger.error(f"[LAB_LIVE] {sname} error: {e}")
                    # Record equity even on error
                    equity = lane.cash + sum(
                        p.qty * p.current_price for p in lane.positions.values()
                    )
                    equity_row[sname] = equity

            # Accumulate trades + equity, then save all via v2 committed write
            self._last_run_date = today_date
            self._last_snapshot_version = snapshot_version
            if new_trades:
                self._all_trades.extend(new_trades)
            # Upsert by date — see lane.equity_history note above. The
            # tail-only check is sufficient because rows are recorded in
            # date order; if a stale duplicate hides earlier in the list
            # it predates this fix and is cleaned by the loader (state_store).
            if self._equity_rows and self._equity_rows[-1].get("date") == today_date:
                self._equity_rows[-1] = equity_row
            else:
                self._equity_rows.append(equity_row)
            self._save_state()

            elapsed = time.time() - t0
            logger.info(f"[LAB_LIVE] Daily run complete: {today_date} "
                        f"({len(new_trades)} trades, {elapsed:.1f}s)")

            # Data snapshot ID (already computed pre-run for idempotency)
            data_snap_id = _snap_id_pre
            self._data_snapshot_id = data_snap_id

            # ── Meta Layer Phase 0: collect & store ──
            try:
                from web.lab_live.meta_collector import collect_meta
                collect_meta(
                    today_date=today_date,
                    today_idx=today_idx,
                    close=close, high=high, vol=vol,
                    universe=univ,
                    sector_map=self._sector_map,
                    index_series=idx_close,
                    fundamental=fund_df,
                    lanes=self._lanes,
                    new_trades=new_trades,
                    data_snapshot_id=data_snap_id,
                    config=self.config,
                    run_meta=getattr(self, '_run_meta', None),
                    universe_raw_count=len(univ) + getattr(self, '_missing_data_ratio', 0),
                    universe_filtered_count=len(univ),
                    missing_data_count=int(getattr(self, '_missing_data_ratio', 0)),
                )
            except Exception as e:
                logger.warning(f"[META] Collection failed (non-fatal): {e}")

            # ── Promotion Evidence: regime history (EOD 확정 기준) ──
            # Lab Live EOD 완료 이후 ‒ intraday flip 금지. snapshot_version 을
            # idempotency key 로 써서 동일 run 재호출 시 중복 기록 방지.
            try:
                from lab.promotion.regime_history import record_regime
                from pathlib import Path as _RP
                import json as _rj

                _regime_label = "UNKNOWN"
                _regime_conf = 0.0
                _regime_src_ver = "REGIME_V1"
                _rp = self.config.BASE_DIR / "data" / "regime" / "latest.json"
                if _rp.exists():
                    try:
                        _rd = _rj.loads(_rp.read_text(encoding="utf-8"))
                        if _rd.get("feature_date") == today_date:
                            _lbl = _rd.get("predicted_label")
                            if _lbl in ("BULL", "BEAR", "SIDEWAYS", "UNKNOWN"):
                                _regime_label = _lbl
                            _regime_conf = float(_rd.get("confidence", 0.0) or 0.0)
                            _regime_src_ver = str(_rd.get("version", "REGIME_V1"))
                    except Exception as _re:
                        logger.debug(f"[PROMO_REGIME] regime file parse failed: {_re}")

                _appended = 0
                for _sname in self._lanes.keys():
                    if record_regime(
                        trade_date=today_date,
                        strategy_name=_sname,
                        regime_label=_regime_label,
                        confidence=_regime_conf,
                        regime_source_version=_regime_src_ver,
                        snapshot_version=snapshot_version,
                    ):
                        _appended += 1
                if _appended:
                    logger.info(
                        f"[PROMO_REGIME] recorded {_appended} lanes "
                        f"({_regime_label}) for {today_date}"
                    )
            except Exception as e:
                logger.warning(f"[PROMO_REGIME] regime_history record failed: {e}")

            # ── Outcome batch: 과거 T+5 후행 계산 (당일 아닌 과거분) ──
            try:
                from web.lab_live.outcome_batch import compute_pending_outcomes
                outcome_result = compute_pending_outcomes(min_delay_days=5)
                if outcome_result.get("computed", 0) > 0:
                    logger.info(f"[OUTCOME_BATCH] auto: {outcome_result}")
            except Exception as e:
                logger.debug(f"[OUTCOME_BATCH] skip: {e}")

            return {
                "ok": True,
                "date": today_date,
                "trades": len(new_trades),
                "elapsed": round(elapsed, 1),
                "data_snapshot_id": data_snap_id,
                "universe_count": _univ_count,
                "snapshot_version": snapshot_version,
                "selected_source": selected_source,
                "data_last_date": data_last_date,
                "market_context": market_ctx.to_dict(),
            }

    # ── State Access ─────────────────────────────────────────

    def get_state(self) -> dict:
        """Dashboard용 전체 스냅샷."""
        with self._lock:
            lanes = []
            init_cash = self.config.initial_cash
            for sname in sorted(self._lanes.keys()):
                lane = self._lanes[sname]
                equity = lane.cash
                positions = []
                # 1차 pass: equity 계산 (weight 분모)
                for tk, pos in lane.positions.items():
                    equity += pos.qty * pos.current_price
                # 2차 pass: position dict 구성 (weight / contribution 포함)
                for tk, pos in lane.positions.items():
                    pnl_pct = (pos.current_price / pos.entry_price - 1) * 100 if pos.entry_price > 0 else 0
                    pnl_amt = (pos.current_price - pos.entry_price) * pos.qty
                    pos_value = pos.qty * pos.current_price
                    weight_pct = (pos_value / equity * 100) if equity > 0 else 0
                    contrib_pct = (pnl_amt / init_cash * 100) if init_cash > 0 else 0
                    positions.append({
                        "code": pos.code,
                        "name": pos.name,
                        "qty": pos.qty,
                        "entry_price": pos.entry_price,
                        "current_price": pos.current_price,
                        "entry_date": pos.entry_date,
                        "pnl_pct": round(pnl_pct, 2),
                        "pnl_amount": round(pnl_amt, 0),
                        "weight_pct": round(weight_pct, 2),
                        "contrib_pct": round(contrib_pct, 2),
                    })

                # Performance
                init_eq = self.config.initial_cash
                total_return = (equity / init_eq - 1) * 100 if init_eq > 0 else 0

                # MDD + start_date + day_return from equity history
                mdd = 0
                start_date = None
                day_return = None
                if lane.equity_history:
                    # 날짜별 마지막 equity (중복 실행/리셋 노이즈 제거)
                    date_eq = {}
                    for e in lane.equity_history:
                        date_eq[e["date"]] = e["equity"]
                    sorted_dates = sorted(date_eq.keys())
                    start_date = sorted_dates[0]

                    # MDD (날짜 중복 제거 후)
                    eqs = [date_eq[d] for d in sorted_dates]
                    peak = eqs[0]
                    for eq in eqs:
                        peak = max(peak, eq)
                        dd = (eq - peak) / peak * 100
                        mdd = min(mdd, dd)

                    # 일일 수익률: 오늘(마지막) vs 전날(두 번째 마지막)
                    if len(sorted_dates) >= 2:
                        prev_eq = date_eq[sorted_dates[-2]]
                        if prev_eq > 0:
                            day_return = round((equity / prev_eq - 1) * 100, 2)

                lanes.append({
                    "name": sname,
                    "group": lane.group,
                    "cash": round(lane.cash),
                    "equity": round(equity),
                    "total_return": round(total_return, 2),
                    "day_return": day_return,
                    "mdd": round(mdd, 2),
                    "start_date": start_date,
                    "n_positions": len(lane.positions),
                    "n_trades": len([t for t in self._all_trades if t.get("strategy") == sname]),
                    "n_pending": len(lane.pending_buys),
                    "positions": positions,
                })

            # 전체 기간: 전략별 start_date 중 가장 이른 날짜
            all_start = min((l["start_date"] for l in lanes if l.get("start_date")), default=None)

            # market_context 노출 (UI/리포트가 DEGRADED 배너 표시 가능)
            _rm = getattr(self, "_run_meta", None) or {}
            _mctx = _rm.get("market_context")

            return {
                "initialized": self._initialized,
                "running": self._running,
                "last_run_date": self._last_run_date,
                "start_date": all_start,
                "start_time": self._start_time.isoformat() if self._start_time else None,
                "n_lanes": len(self._lanes),
                "lanes": lanes,
                "market_context": _mctx,
            }

    def get_trades(self, limit: int = 50) -> list:
        with self._lock:
            return self._all_trades[-limit:]

    def get_equity_history(self) -> dict:
        """전략별 equity history."""
        with self._lock:
            result = {}
            for sname, lane in self._lanes.items():
                result[sname] = lane.equity_history
            return result

    # ── Internal ─────────────────────────────────────────────

    def _close_position(self, lane: LabLiveLane, ticker: str,
                        snapshot, reason: str, today_date: str) -> Optional[dict]:
        if ticker not in lane.positions:
            return None
        pos = lane.positions[ticker]
        p = float(snapshot.close.get(ticker, 0))
        if p <= 0 or pd.isna(p):
            p = pos.entry_price

        net = pos.qty * p * (1 - self.config.sell_cost)
        invested = pos.qty * pos.entry_price + pos.buy_cost_total
        pnl = (net - invested) / invested if invested > 0 else 0

        lane.cash += net

        # hold_days 계산
        try:
            from datetime import datetime as _dt
            hd = (_dt.strptime(today_date, "%Y-%m-%d") - _dt.strptime(pos.entry_date, "%Y-%m-%d")).days
        except Exception:
            hd = 0

        trade = {
            "strategy": lane.name,
            "ticker": ticker,
            "name": pos.name,
            "entry_date": pos.entry_date,
            "exit_date": today_date,
            "entry_price": pos.entry_price,
            "exit_price": p,
            "qty": pos.qty,
            "pnl_pct": round(pnl * 100, 2),
            "pnl_amount": round(net - invested),
            "exit_reason": reason,
            "hold_days": hd,
        }
        del lane.positions[ticker]
        return trade

    def _archive_state(self):
        """Archive current committed state (head + states + trades + equity)."""
        dest = archive_state_v2(self.config)
        if dest:
            logger.info(f"[LAB_LIVE] State archived → {dest}")
        else:
            logger.warning("[LAB_LIVE] Archive failed")

    def _save_state(self):
        """현재 상태를 v2 committed version으로 저장."""
        lanes_data = {}
        for sname, lane in self._lanes.items():
            lanes_data[sname] = {
                "cash": lane.cash,
                "positions": {
                    tk: {
                        "code": p.code, "name": p.name, "qty": p.qty,
                        "entry_price": p.entry_price, "entry_date": p.entry_date,
                        "high_wm": p.high_wm, "buy_cost_total": p.buy_cost_total,
                        "current_price": p.current_price,
                        "entry_day_idx": p.entry_day_idx,
                    } for tk, p in lane.positions.items()
                },
                "pending_buys": lane.pending_buys,
                "last_rebal_idx": lane.last_rebal_idx,
                "equity_history": lane.equity_history[-60:],  # keep last 60 days
            }
        save_state_v2(lanes_data, self._all_trades, self._equity_rows, self.config,
                      snapshot_version=getattr(self, '_last_snapshot_version', ''),
                      run_meta=getattr(self, '_run_meta', None),
                      trade_date=self._last_run_date)
