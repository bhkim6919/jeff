"""
gen3_signal_builder.py
======================
Q-TRON Gen3 v7 시그널 생성기.

data/ohlcv_kospi_daily/ 의 CSV 파일을 읽어 v7 RS composite 기반 시그널을 생성.
출력: data/signals/signals_YYYYMMDD.csv
      data/signals/regime_YYYYMMDD.json

Usage:
  python gen3_signal_builder.py
  python gen3_signal_builder.py --date 20260311   # 특정 날짜 기준
  python gen3_signal_builder.py --top 100         # 상위 100개 (기본 50)
"""

from __future__ import annotations

import argparse
import json
import sys
from datetime import date, datetime
from pathlib import Path
from typing import Dict, List, Optional, Tuple

import numpy as np
import pandas as pd

BASE_DIR = Path(__file__).resolve().parent
sys.path.insert(0, str(BASE_DIR))

OHLCV_DIR     = BASE_DIR / "data" / "ohlcv_kospi_daily"
INDEX_FILE    = BASE_DIR / "data" / "kospi_index_daily_5y.csv"
UNIVERSE_FILE = BASE_DIR / "data" / "universe_kospi.csv"
SIGNALS_DIR   = BASE_DIR / "data" / "signals"
SECTOR_MAP    = BASE_DIR / "data" / "sector_map.json"

# ── v7 파라미터 — Breadth 임계값은 config.py 단일 소스 사용 (WARN-1 FIX) ───
try:
    from config import Gen3Config as _Cfg
    _cfg_defaults = _Cfg()
    BREADTH_BEAR_THRESH = _cfg_defaults.BREADTH_BEAR_THRESH
    BREADTH_BULL_THRESH = _cfg_defaults.BREADTH_BULL_THRESH
except Exception:
    BREADTH_BEAR_THRESH = 0.35
    BREADTH_BULL_THRESH = 0.55

RS_ENTRY_MIN      = 0.80   # Main 진입 최소 RS composite
RS_EXIT_THRESH    = 0.40   # 청산 임계 RS
UNIV_MIN_CLOSE    = 2_000
UNIV_MIN_AMT      = 2_000_000_000
ATR_PERIOD        = 20
# WARN-1 FIX: Breadth 임계값은 config.py의 BREADTH_BEAR_THRESH / BREADTH_BULL_THRESH를 사용.
# 이 모듈 상수를 직접 수정하지 말 것 — config.py에서 중앙 관리.
REGIME_MA         = 200
REGIME_FLIP_GATE  = 2


# ── 유틸 ─────────────────────────────────────────────────────────────────────

def _progress(i: int, total: int, label: str = "") -> None:
    filled = int(40 * i / total) if total else 0
    bar    = "#" * filled + "-" * (40 - filled)
    pct    = i / total * 100 if total else 0
    print(f"\r  [{bar}] {pct:5.1f}%  {i}/{total}  {label:<12}", end="", flush=True)


def _wilder_atr(df: pd.DataFrame, period: int = 20) -> float:
    """Wilder EMA 방식 ATR(period). 마지막 행 기준."""
    if len(df) < period + 1:
        return 0.0
    high  = df["high"].values.astype(float)
    low   = df["low"].values.astype(float)
    close = df["close"].values.astype(float)
    tr    = np.maximum(
        high[1:] - low[1:],
        np.maximum(
            np.abs(high[1:] - close[:-1]),
            np.abs(low[1:] - close[:-1])
        )
    )
    # Wilder EMA 초기화: 첫 period개의 평균
    if len(tr) < period:
        return 0.0
    atr = float(tr[:period].mean())
    k   = 1.0 / period
    for v in tr[period:]:
        atr = atr * (1 - k) + v * k
    return atr


# ── 인덱스 레짐 ───────────────────────────────────────────────────────────────

def load_index() -> pd.DataFrame:
    if not INDEX_FILE.exists():
        raise FileNotFoundError(f"지수 파일 없음: {INDEX_FILE}\n"
                                 "update_data_incremental.py 를 먼저 실행하세요.")
    df = pd.read_csv(INDEX_FILE, parse_dates=["date"])
    df = df.sort_values("date").reset_index(drop=True)
    return df


def detect_regime_from_index(idx_df: pd.DataFrame) -> Tuple[str, float, float]:
    """
    MA200 기반 기본 레짐 + Breadth(placeholder) 반환.
    Breadth는 전 종목 OHLCV 로드 후 외부에서 주입.
    반환: (regime_str, ma200_value, last_close)
    """
    if len(idx_df) < REGIME_MA:
        return "SIDEWAYS", 0.0, float(idx_df["close"].iloc[-1])

    close   = idx_df["close"].astype(float)
    ma200   = float(close.rolling(REGIME_MA).mean().iloc[-1])
    last    = float(close.iloc[-1])
    return ("BULL" if last > ma200 else "BEAR"), ma200, last


# ── OHLCV 로드 ────────────────────────────────────────────────────────────────

def load_ohlcv(ticker: str, min_rows: int = 130) -> Optional[pd.DataFrame]:
    path = OHLCV_DIR / f"{ticker}.csv"
    if not path.exists():
        return None
    try:
        df = pd.read_csv(path, parse_dates=["date"])
        df = df.sort_values("date").reset_index(drop=True)
        if len(df) < min_rows:
            return None
        for c in ["open", "high", "low", "close", "volume"]:
            if c in df.columns:
                df[c] = pd.to_numeric(df[c], errors="coerce").fillna(0)
        return df
    except Exception:
        return None


# ── 유니버스 필터 ─────────────────────────────────────────────────────────────

def filter_universe(tickers: List[str]) -> List[str]:
    """최소 종가, 최소 거래대금 필터."""
    result = []
    for ticker in tickers:
        df = load_ohlcv(ticker, min_rows=25)
        if df is None or df.empty:
            continue
        last_close  = float(df["close"].iloc[-1])
        last_volume = float(df["volume"].iloc[-1])
        avg_amt     = (df["close"] * df["volume"]).tail(20).mean()
        if last_close < UNIV_MIN_CLOSE:
            continue
        if avg_amt < UNIV_MIN_AMT:
            continue
        result.append(ticker)
    return result


# ── 섹터 맵 ───────────────────────────────────────────────────────────────────

def load_sector_map() -> Dict[str, str]:
    if not SECTOR_MAP.exists():
        return {}
    try:
        with open(SECTOR_MAP, encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return {}


# ── 지표 계산 ─────────────────────────────────────────────────────────────────

def compute_features(ticker: str, df: pd.DataFrame) -> Optional[Dict]:
    """종목별 v7 지표 계산."""
    if len(df) < 125:
        return None

    close  = df["close"].astype(float)
    high   = df["high"].astype(float)
    low    = df["low"].astype(float)
    volume = df["volume"].astype(float)

    last_close = float(close.iloc[-1])
    if last_close <= 0:
        return None

    # RS 원재료: n일 수익률 (전일 종가 대비)
    def _ret(n: int) -> float:
        if len(close) <= n:
            return float("nan")
        prev = float(close.iloc[-(n + 1)])
        if prev <= 0:
            return float("nan")
        return last_close / prev - 1.0

    rs20_raw  = _ret(20)
    rs60_raw  = _ret(60)
    rs120_raw = _ret(120)

    # MA20 상회 여부
    ma20       = float(close.rolling(20).mean().iloc[-1]) if len(close) >= 20 else float("nan")
    above_ma20 = int(last_close > ma20) if not np.isnan(ma20) else 0

    # 52주 신고가
    high_252   = float(high.tail(252).max())
    is_52w_high = int(last_close >= high_252 * 0.95)  # 5% 허용 (v7 완화)

    # ATR(20) Wilder
    atr20 = _wilder_atr(df.tail(60), ATR_PERIOD)

    # 20일 신고가 돌파 (breakout 기준)
    high_20    = float(high.tail(21).iloc[:-1].max()) if len(high) >= 21 else float("nan")
    breakout   = int(last_close >= high_20) if not np.isnan(high_20) else 0

    # 갭 계산 (전일 종가 대비 당일 시가)
    if len(df) >= 2:
        prev_close = float(close.iloc[-2])
        last_open  = float(df["open"].iloc[-1])
        gap_pct    = (last_open / prev_close - 1.0) if prev_close > 0 else 0.0
        last_vol   = float(volume.iloc[-1])
        avg_vol20  = float(volume.tail(21).iloc[:-1].mean()) if len(volume) >= 21 else 1.0
        vol_ratio  = last_vol / avg_vol20 if avg_vol20 > 0 else 1.0
    else:
        gap_pct   = 0.0
        vol_ratio = 1.0

    # 갭 필터 (gap > 8% AND volume < 1.3배 → 스킵)
    gap_blocked = int(gap_pct > 0.08 and vol_ratio < 1.3)

    return {
        "ticker":      ticker,
        "last_close":  last_close,
        "rs20_raw":    rs20_raw,
        "rs60_raw":    rs60_raw,
        "rs120_raw":   rs120_raw,
        "above_ma20":  above_ma20,
        "is_52w_high": is_52w_high,
        "atr20":       atr20,
        "breakout":    breakout,
        "gap_blocked": gap_blocked,
        "high_252":    high_252,
    }


# ── RS 순위 계산 ──────────────────────────────────────────────────────────────

def compute_rs_ranks(features: List[Dict]) -> pd.DataFrame:
    """rs20_raw, rs60_raw, rs120_raw 를 0~1 백분위 순위로 변환."""
    df = pd.DataFrame(features)
    for col, out in [("rs20_raw", "rs20_rank"), ("rs60_raw", "rs60_rank"), ("rs120_raw", "rs120_rank")]:
        valid = df[col].notna()
        df.loc[valid, out]    = df.loc[valid, col].rank(pct=True)
        df.loc[~valid, out]   = float("nan")

    # rs_composite = rs20×0.30 + rs60×0.50 + rs120×0.20
    df["rs_composite"] = (
        df["rs20_rank"].fillna(0) * 0.30 +
        df["rs60_rank"].fillna(0) * 0.50 +
        df["rs120_rank"].fillna(0) * 0.20
    )
    # NaN이 포함된 행은 신뢰도 낮음
    nan_mask = df[["rs20_rank", "rs60_rank", "rs120_rank"]].isna().any(axis=1)
    df.loc[nan_mask, "rs_composite"] = float("nan")

    # ATR 순위 (낮을수록 변동성 작음)
    valid_atr = df["atr20"] > 0
    df.loc[valid_atr, "atr_rank"] = df.loc[valid_atr, "atr20"].rank(pct=True)
    df.loc[~valid_atr, "atr_rank"] = 1.0  # 데이터 없는 건 최악 취급

    return df


# ── pb_score (눌림목 보너스) ──────────────────────────────────────────────────

def _pb_score(last_close: float, high_252: float) -> float:
    """52주 고점 대비 3~7% 하락 시 +5점."""
    if high_252 <= 0:
        return 0.0
    ratio = last_close / high_252
    if 0.93 <= ratio <= 0.97:
        return 5.0
    return 0.0


# ── 시그널 생성 ───────────────────────────────────────────────────────────────

def build_signals(
    df_features: pd.DataFrame,
    sector_map:  Dict[str, str],
    regime:      str,
    top_n:       int = 50,
) -> pd.DataFrame:
    """
    v7 조건으로 signal_entry 판단 후 상위 top_n 종목 선택.
    score = rs_composite × 100 + pb_score
    """
    df = df_features.copy()

    # rs_composite NaN 제거
    df = df.dropna(subset=["rs_composite"])

    # signal_entry: breakout=1 AND rs_composite >= 0.80 AND gap 미차단
    df["signal_entry"] = (
        (df["breakout"] == 1) &
        (df["rs_composite"] >= RS_ENTRY_MIN) &
        (df["gap_blocked"] == 0)
    ).astype(int)

    # signal_exit: rs_composite < 0.40
    df["signal_exit"] = (df["rs_composite"] < RS_EXIT_THRESH).astype(int)

    # 점수: rs_composite × 100 + pb_score
    df["pb_score"] = df.apply(
        lambda r: _pb_score(r["last_close"], r["high_252"]), axis=1
    )
    df["score"] = df["rs_composite"] * 100 + df["pb_score"]

    # 섹터 매핑
    df["sector"] = df["ticker"].map(lambda t: sector_map.get(t, "기타"))

    # stage 분류: BULL이면 (52w 고가 근접 OR 돌파+고RS) → A, 나머지 B
    if regime == "BULL":
        df["stage"] = df.apply(
            lambda r: "A" if (
                (r["is_52w_high"] == 1 and r["rs_composite"] >= 0.80)
                or (r["breakout"] == 1 and r["rs_composite"] >= 0.92)
            ) else "B",
            axis=1
        )
    else:
        df["stage"] = "B"   # BEAR에서는 Early 없음

    # signal_entry=1 인 것만 상위 N개
    candidates = df[df["signal_entry"] == 1].sort_values("score", ascending=False)
    # signal_entry=0 이어도 현재 보유 종목 청산 판단용으로 전체에서 RS 확인 가능
    # → signals.csv 에는 entry=1 상위 top_n 저장
    result = candidates.head(top_n).copy()

    return result


# ── 메인 ─────────────────────────────────────────────────────────────────────

def main() -> None:
    parser = argparse.ArgumentParser(description="Q-TRON Gen3 v7 Signal Builder")
    parser.add_argument("--date", type=str, default=None,
                        help="data_asof 날짜 YYYYMMDD (기본: 자동)")
    parser.add_argument("--trade-date", type=str, default=None,
                        help="trade_date YYYYMMDD (기본: 자동=next_trade_date)")
    parser.add_argument("--top",  type=int, default=50,
                        help="상위 N개 신호 (기본: 50)")
    args = parser.parse_args()

    # v7.6: trade_date / data_asof_date 분리
    from trade_date_utils import next_trade_date as _next_td, data_asof_date as _asof

    now = datetime.now()

    # data_asof_date: 시그널 계산에 사용한 가격 데이터 기준일
    if args.date:
        target_date = datetime.strptime(args.date, "%Y%m%d").date()
    else:
        target_date = _asof(now)

    # trade_date: 이 시그널이 사용될 거래 세션 날짜 (파일명 기준)
    if args.trade_date:
        trade_dt = datetime.strptime(args.trade_date, "%Y%m%d").date()
    else:
        trade_dt = _next_td(now)

    date_str = trade_dt.strftime("%Y%m%d")
    asof_str = target_date.strftime("%Y%m%d")

    print(f"=== Gen3 v7 Signal Builder ===")
    print(f"  trade_date (session):  {trade_dt}")
    print(f"  data_asof_date:        {target_date}")
    print(f"  generated_at:          {now.strftime('%Y-%m-%d %H:%M:%S')}")
    print()

    # ── 1. 지수 로드 + 레짐 판단 ─────────────────────────────────────────
    print("[1/5] KOSPI 지수 로드...")
    try:
        idx_df = load_index()
    except FileNotFoundError as e:
        print(f"ERROR: {e}")
        sys.exit(1)

    regime_base, ma200, last_close = detect_regime_from_index(idx_df)
    print(f"  MA200 레짐: {regime_base}  (종가={last_close:.0f}, MA200={ma200:.0f})")

    # 전일 지수 수익률 (RAL용 — 저장만 해둠)
    idx_ret = 0.0
    if len(idx_df) >= 2:
        prev_c = float(idx_df["close"].iloc[-2])
        curr_c = float(idx_df["close"].iloc[-1])
        idx_ret = (curr_c / prev_c - 1.0) if prev_c > 0 else 0.0

    # ── 2. 유니버스 로드 ─────────────────────────────────────────────────
    print("[2/5] 유니버스 로드...")
    if not UNIVERSE_FILE.exists():
        print(f"  ERROR: {UNIVERSE_FILE} 없음. update_data_incremental.py 를 먼저 실행하세요.")
        sys.exit(1)
    all_tickers = pd.read_csv(UNIVERSE_FILE)["ticker"].astype(str).tolist()
    print(f"  전체 유니버스: {len(all_tickers)}개")

    # ── 3. 지표 계산 ─────────────────────────────────────────────────────
    print("[3/5] 지표 계산...")
    features_list = []
    breadth_above = 0
    breadth_total = 0

    # 적응형 Breadth: 과거 125거래일 히스토리 수집 (v7.2)
    BREADTH_HISTORY_DAYS = 125
    breadth_history_data = []   # [(date_series, above_ma20_series), ...]

    for i, ticker in enumerate(all_tickers, 1):
        _progress(i, len(all_tickers), ticker)
        df = load_ohlcv(ticker, min_rows=25)
        if df is None or df.empty:
            continue

        # 유니버스 필터 (최소 종가, 최소 거래대금)
        last_close_t = float(df["close"].iloc[-1])
        avg_amt = (df["close"] * df["volume"]).tail(20).mean()
        if last_close_t < UNIV_MIN_CLOSE or avg_amt < UNIV_MIN_AMT:
            continue

        feat = compute_features(ticker, df)
        if feat is None:
            continue

        # Breadth 계산용
        breadth_total += 1
        if feat["above_ma20"] == 1:
            breadth_above += 1

        # 적응형 Breadth 히스토리 수집: 최근 125일간 close > MA20
        if len(df) >= 40:
            close_s = df["close"].astype(float)
            ma20_s  = close_s.rolling(20).mean()
            above   = (close_s > ma20_s).astype(int)
            tail    = above.tail(BREADTH_HISTORY_DAYS)
            dates   = df["date"].tail(BREADTH_HISTORY_DAYS)
            breadth_history_data.append(
                pd.Series(tail.values, index=dates.values)
            )

        features_list.append(feat)

    print(f"\n  지표 계산 완료: {len(features_list)}개")

    # ── 4. Breadth 레짐 보완 ─────────────────────────────────────────────
    breadth = breadth_above / breadth_total if breadth_total > 0 else 0.5
    print(f"[4/5] Breadth: {breadth:.1%}  (MA20 상회 {breadth_above}/{breadth_total})")

    # 적응형 Breadth 임계값 계산 (v7.2)
    adaptive_thresh = BREADTH_BEAR_THRESH   # 폴백: 고정값
    if breadth_history_data:
        hist_df     = pd.DataFrame(breadth_history_data).T   # rows=dates, cols=stocks
        daily_breadth = hist_df.mean(axis=1).dropna()         # 일별 평균 (= 비율)
        if len(daily_breadth) >= 30:
            bm = float(daily_breadth.mean())
            bs = float(daily_breadth.std())
            adaptive_thresh = round(max(0.25, min(0.45, bm - bs)), 4)
            print(f"  적응형 Breadth 임계값: {adaptive_thresh:.1%} "
                  f"(mean={bm:.1%}, std={bs:.1%}, 고정={BREADTH_BEAR_THRESH:.0%})")
        else:
            print(f"  Breadth 히스토리 부족 ({len(daily_breadth)}일) → 고정 임계값 사용")

    # is_bull_eff: MA200 BULL AND breadth >= adaptive threshold
    is_bull_eff = (regime_base == "BULL") and (breadth >= adaptive_thresh)
    regime      = "BULL" if is_bull_eff else "BEAR"
    if regime_base == "BULL" and not is_bull_eff:
        print(f"  Breadth {breadth:.1%} < {adaptive_thresh:.1%} → BULL → BEAR 강제 전환")

    print(f"  최종 레짐: {regime}")

    # RS 순위 계산
    df_feat = compute_rs_ranks(features_list)

    # ── 5. 시그널 생성 ────────────────────────────────────────────────────
    print(f"[5/5] 시그널 생성 (상위 {args.top}개)...")
    sector_map = load_sector_map()
    df_signals = build_signals(df_feat, sector_map, regime, top_n=args.top)

    if df_signals.empty:
        print("  WARNING: signal_entry=1 종목 없음")

    # ── 저장 ─────────────────────────────────────────────────────────────
    SIGNALS_DIR.mkdir(parents=True, exist_ok=True)

    # signals_YYYYMMDD.csv (런타임 호환)
    out_csv = SIGNALS_DIR / f"signals_{date_str}.csv"
    tmp_csv = SIGNALS_DIR / f"signals_{date_str}.tmp.csv"   # v7.6: atomic write
    rows = []
    for _, row in df_signals.iterrows():
        last_close_v = row["last_close"]
        atr20_v      = row["atr20"]
        rs_c         = row["rs_composite"]

        # TP/SL 계산 (v7 레짐별 ATR 배수)
        if regime == "BULL":
            sl_mult = 2.5  # v7.3: 4.0→2.5 (SL 과도 이격 방지)
        else:
            sl_mult = 1.0

        if atr20_v > 0:
            sl = int(last_close_v - atr20_v * sl_mult)
            tp = int(last_close_v + (last_close_v - sl) * 2.0)
        else:
            sl, tp = 0, 0

        # SL 최소 거리 1% 필터
        if sl > 0 and (last_close_v - sl) / last_close_v < 0.01:
            continue

        rows.append({
            "date":         date_str,
            "ticker":       row["ticker"],
            "qscore":       round(rs_c, 4),
            "entry":        int(last_close_v),
            "tp":           tp,
            "sl":           sl,
            "sector":       row["sector"],
            "stage":        row["stage"],
            "rs_composite": round(rs_c, 4),
            "signal_entry": int(row["signal_entry"]),
            "is_52w_high":  int(row["is_52w_high"]),
            "above_ma20":   int(row["above_ma20"]),
            "rs20_rank":    round(float(row.get("rs20_rank", 0) or 0), 4),
            "rs60_rank":    round(float(row.get("rs60_rank", 0) or 0), 4),
            "rs120_rank":   round(float(row.get("rs120_rank", 0) or 0), 4),
            "atr20":        round(atr20_v, 1),
        })

    out_df = pd.DataFrame(rows)

    # v7.6: atomic write — .tmp.csv 먼저 쓰고 rename
    out_df.to_csv(tmp_csv, index=False, encoding="utf-8")
    if out_csv.exists():
        out_csv.unlink()
    tmp_csv.rename(out_csv)

    cnt_a = (out_df["stage"] == "A").sum()
    cnt_b = (out_df["stage"] == "B").sum()
    print(f"  저장: {out_csv.name}  ({len(out_df)}개 / Stage A:{cnt_a} B:{cnt_b})")

    # v7.6: signals_YYYYMMDD.meta.json — 런타임 유효성 검증용
    meta_path = SIGNALS_DIR / f"signals_{date_str}.meta.json"
    strategy_ver = "7.5"
    try:
        strategy_ver = _cfg_defaults.STRATEGY_VERSION
    except Exception:
        pass
    meta = {
        "trade_date":       date_str,
        "data_asof_date":   asof_str,
        "generated_at":     now.strftime("%Y-%m-%dT%H:%M:%S+09:00"),
        "status":           "SUCCESS",
        "engine_version":   "gen3_signal_builder",
        "strategy_version": strategy_ver,
        "signal_count":     len(out_df),
        "regime":           regime,
        "breadth":          round(breadth, 4),
    }
    with open(meta_path, "w", encoding="utf-8") as f:
        json.dump(meta, f, ensure_ascii=False, indent=2)
    print(f"  저장: {meta_path.name}  (status={meta['status']}, count={meta['signal_count']})")

    # regime_YYYYMMDD.json
    regime_out = SIGNALS_DIR / f"regime_{date_str}.json"
    regime_info = {
        "date":              date_str,
        "regime":            regime,
        "regime_base":       regime_base,
        "breadth":           round(breadth, 4),
        "breadth_thresh":    adaptive_thresh,
        "ma200":             round(ma200, 2),
        "kospi_close":       round(last_close, 2),
        "idx_ret":           round(idx_ret, 6),
    }
    with open(regime_out, "w", encoding="utf-8") as f:
        json.dump(regime_info, f, ensure_ascii=False, indent=2)
    print(f"  저장: {regime_out.name}  ({regime_info})")

    print(f"\n=== 완료: {len(out_df)}개 신호 / 레짐={regime} / Breadth={breadth:.1%} ===")


if __name__ == "__main__":
    main()
