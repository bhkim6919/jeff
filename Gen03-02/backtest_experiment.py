# -*- coding: utf-8 -*-
"""
backtest_experiment.py
======================
Score Top10 전략 개선 실험 — 6개 전략 비교 백테스트.

전략:
  BASELINE:  기존 Score Top10 (SL/TP 없음, 5일 보유)
  EXP_A:     + SL -8% / TP +12% (일봉 기준)
  EXP_B:     + 레짐 분리 (BULL/NEUTRAL/BEAR 포지션 조절)
  EXP_C:     + 수급 가중 강화 (volume proxy)
  EXP_D:     + 집중도 제한 (섹터 2종목, 연속 3회 제한)
  EXP_E:     A+B+C+D 전부 결합

Usage:
  python backtest_experiment.py
  python backtest_experiment.py --start 20240101
"""
import argparse, csv, json, sys, time as _time
from collections import defaultdict
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional, Tuple
import numpy as np, pandas as pd

sys.stdout.reconfigure(encoding='utf-8')
BASE_DIR = Path(__file__).resolve().parent
OHLCV_DIR = BASE_DIR / "data" / "ohlcv_kospi_daily"
INDEX_FILE = BASE_DIR / "data" / "kospi_index_daily_5y.csv"
REPORT_DIR = BASE_DIR / "data" / "top20" / "reports"
sys.path.insert(0, str(BASE_DIR))
import re
_PREF = re.compile(r"^\d{5}[5-9]$")

# ── 비용 ─────────────────────────────────────────────────────────────────────
BUY_COST  = 0.0015 + 0.003   # fee 0.15% + slippage 0.30%
SELL_COST = 0.0035 + 0.003   # fee 0.35% + slippage 0.30%

# ── 전략 설정 ────────────────────────────────────────────────────────────────
@dataclass
class StrategyConfig:
    name: str
    sl_pct: float = 0.0        # 0=비활성, -0.08 = -8%
    tp_pct: float = 0.0        # 0=비활성, +0.12 = +12%
    regime_adaptive: bool = False
    flow_weight: float = 0.0   # 수급 가중 추가 점수 배율
    sector_cap: int = 99       # 섹터당 최대 종목
    repeat_cap: int = 99       # 동일 종목 연속 선정 제한
    base_top_n: int = 10

STRATEGIES = {
    "BASELINE": StrategyConfig("BASELINE"),
    "EXP_A":    StrategyConfig("EXP_A", sl_pct=-0.08, tp_pct=0.12),
    "EXP_B":    StrategyConfig("EXP_B", regime_adaptive=True),
    "EXP_C":    StrategyConfig("EXP_C", flow_weight=1.0),
    "EXP_D":    StrategyConfig("EXP_D", sector_cap=2, repeat_cap=3),
    "EXP_E":    StrategyConfig("EXP_E", sl_pct=-0.08, tp_pct=0.12,
                                regime_adaptive=True, flow_weight=1.0,
                                sector_cap=2, repeat_cap=3),
}

# ── 데이터 로드 ──────────────────────────────────────────────────────────────
_cache: Dict[str, pd.DataFrame] = {}

def preload() -> Dict[str, pd.DataFrame]:
    print("[Preload] OHLCV...", end="", flush=True)
    t0 = _time.monotonic()
    for p in OHLCV_DIR.glob("*.csv"):
        tk = p.stem
        if not (tk.isdigit() and len(tk)==6) or _PREF.match(tk): continue
        try:
            df = pd.read_csv(p, dtype={"date":str})
            df["date"] = df["date"].str[:10].str.replace("-","")
            for c in ["open","high","low","close","volume"]:
                df[c] = pd.to_numeric(df[c], errors="coerce").fillna(0)
            df = df.sort_values("date").reset_index(drop=True)
            if len(df) >= 120:
                _cache[tk] = df
        except: continue
    print(f" {len(_cache)}종목 ({_time.monotonic()-t0:.1f}s)")
    return _cache

def load_index() -> Optional[pd.DataFrame]:
    if not INDEX_FILE.exists(): return None
    df = pd.read_csv(INDEX_FILE, dtype={"date":str})
    df["date"] = df["date"].str[:10].str.replace("-","")
    df["close"] = pd.to_numeric(df["close"], errors="coerce").fillna(0)
    return df.sort_values("date").reset_index(drop=True)

# ── 섹터 맵 ─────────────────────────────────────────────────────────────────
_sector_map: Dict[str, str] = {}
def _load_sector_map():
    global _sector_map
    sp = BASE_DIR / "data" / "sector_map.json"
    if sp.exists():
        try:
            with open(sp, encoding="utf-8") as f:
                _sector_map = json.load(f)
        except: pass

# ── Breadth 계산 ─────────────────────────────────────────────────────────────
def calc_breadth(all_data: Dict[str, pd.DataFrame], as_of: str) -> float:
    above = total = 0
    for tk, df in all_data.items():
        dc = df[df["date"] <= as_of]
        if len(dc) < 20: continue
        c = dc["close"]
        total += 1
        if float(c.iloc[-1]) > float(c.tail(20).mean()):
            above += 1
    return above / total * 100 if total > 0 else 50

# ── 수급 프록시 (volume direction) ───────────────────────────────────────────
def calc_flow_proxy(df_cut: pd.DataFrame) -> float:
    """최근 5일 volume*direction zscore. 양수=매집, 음수=이탈."""
    if len(df_cut) < 25: return 0.0
    c = df_cut["close"].astype(float)
    v = df_cut["volume"].astype(float)
    direction = c.diff().apply(lambda x: 1 if x > 0 else -1 if x < 0 else 0)
    flow = (v * direction).tail(20)
    if flow.std() == 0: return 0.0
    recent = float(flow.tail(5).mean())
    return (recent - float(flow.mean())) / float(flow.std())

# ── 스코어링 ─────────────────────────────────────────────────────────────────
def score_universe(all_data, as_of, cfg: StrategyConfig,
                   prev_picks: List[str] = None,
                   pick_counts: Dict[str, int] = None,
                   breadth: float = 50) -> List[Dict]:
    from top20_report import compute_full_analysis, _score_investment

    # 유동성 필터
    cands = []
    for tk, df in all_data.items():
        dc = df[df["date"] <= as_of]
        if len(dc) < 120: continue
        cl = dc["close"]; vol = dc["volume"]
        lc = float(cl.iloc[-1])
        if lc < 5000: continue
        amt = float((cl*vol).tail(20).mean()) / 1e8
        if amt < 500: continue
        cands.append((tk, dc, amt))
    cands.sort(key=lambda x: -x[2])
    cands = cands[:200]

    scored = []
    for tk, dc, _ in cands:
        r = compute_full_analysis(tk, dc)
        if not r: continue
        inv = _score_investment(r)
        score = inv["score"]

        # 수급 프록시 가중
        if cfg.flow_weight > 0:
            fp = calc_flow_proxy(dc)
            score += fp * 5.0 * cfg.flow_weight  # zscore * 5점 * weight
            if fp > 1.0:
                inv["reasons"].append(f"수급 강세 (proxy={fp:.1f}σ)")

        # 레짐 적응
        if cfg.regime_adaptive:
            if breadth > 80:
                # BULL: 모멘텀 보너스
                if r["ret_5d"] > 3: score += 5
            elif breadth < 50:
                # BEAR: RSI 과매도 보너스, 모멘텀 벌점
                if r["rsi"] < 40: score += 5
                if r["ret_5d"] > 10: score -= 5

        inv["score"] = round(score, 1)
        r["sector"] = _sector_map.get(tk, "기타")
        scored.append({**r, **inv})

    scored.sort(key=lambda x: -x["score"])

    # 포지션 수 결정 (레짐)
    if cfg.regime_adaptive:
        if breadth > 80: top_n = 10
        elif breadth >= 50: top_n = 7
        else: top_n = 5
    else:
        top_n = cfg.base_top_n

    # 집중도 제한 + 연속 제한
    filtered = []
    sector_cnt = defaultdict(int)
    for s in scored:
        tk = s["ticker"]
        sec = s.get("sector", "기타")
        # 섹터 제한
        if sector_cnt[sec] >= cfg.sector_cap: continue
        # 연속 선정 제한
        if pick_counts and pick_counts.get(tk, 0) >= cfg.repeat_cap: continue
        filtered.append(s)
        sector_cnt[sec] += 1
        if len(filtered) >= top_n: break

    return filtered

# ── 시뮬레이션 (SL/TP 지원) ──────────────────────────────────────────────────
def simulate(all_data, picks, entry_date, hold_days, cfg: StrategyConfig,
             trading_dates: List[str]) -> List[Dict]:
    trades = []
    di = trading_dates.index(entry_date) if entry_date in trading_dates else -1
    if di < 0: return trades
    entry_di = di + 1
    if entry_di >= len(trading_dates): return trades

    for p in picks:
        tk = p["ticker"]
        df = all_data.get(tk)
        if df is None: continue
        idx = df.index[df["date"] == trading_dates[entry_di]].tolist()
        if not idx: continue
        ei = idx[0]

        entry_price = float(df["open"].iloc[ei])
        if entry_price <= 0: entry_price = float(df["close"].iloc[ei])
        if entry_price <= 0: continue

        # 비용 반영 진입가
        cost_entry = entry_price * (1 + BUY_COST)

        # SL/TP 시뮬 (일봉 기준)
        exit_price = None
        exit_reason = "HOLD"
        actual_hold = 0

        for d in range(hold_days):
            ci = ei + d
            if ci >= len(df): break
            actual_hold = d + 1
            day_low = float(df["low"].iloc[ci])
            day_high = float(df["high"].iloc[ci])
            day_close = float(df["close"].iloc[ci])

            if cfg.sl_pct != 0 and cfg.tp_pct != 0:
                sl_price = entry_price * (1 + cfg.sl_pct)
                tp_price = entry_price * (1 + cfg.tp_pct)

                hit_sl = day_low <= sl_price
                hit_tp = day_high >= tp_price

                if hit_sl and hit_tp:
                    # 둘 다 충족 → 보수적으로 불리한 쪽 (SL)
                    exit_price = sl_price
                    exit_reason = "SL"
                    break
                elif hit_sl:
                    exit_price = sl_price
                    exit_reason = "SL"
                    break
                elif hit_tp:
                    exit_price = tp_price
                    exit_reason = "TP"
                    break

        # SL/TP 미히트 → 보유기간 종료 시 종가 청산
        if exit_price is None:
            final_ci = min(ei + hold_days - 1, len(df) - 1)
            exit_price = float(df["close"].iloc[final_ci])
            exit_reason = "HOLD"
            actual_hold = final_ci - ei + 1

        # 비용 반영 청산가
        cost_exit = exit_price * (1 - SELL_COST)

        ret_gross = (exit_price / entry_price - 1) * 100
        ret_net = (cost_exit / cost_entry - 1) * 100

        trades.append({
            "ticker": tk, "sector": p.get("sector", "기타"),
            "score": p.get("score", 0),
            "entry_date": trading_dates[entry_di],
            "entry_price": entry_price, "exit_price": exit_price,
            "ret_gross": round(ret_gross, 2), "ret_net": round(ret_net, 2),
            "exit_reason": exit_reason, "hold_days": actual_hold,
        })
    return trades

# ── 메트릭스 ─────────────────────────────────────────────────────────────────
def calc_metrics(results: List[Dict], idx_df, trading_dates, hold_days,
                 label: str) -> Dict:
    all_trades = [t for r in results for t in r["trades"]]
    if not all_trades:
        return {"label": label, "n_trades": 0}

    gross = [t["ret_gross"] for t in all_trades]
    net = [t["ret_net"] for t in all_trades]
    wins_g = sum(1 for r in gross if r > 0)
    wins_n = sum(1 for r in net if r > 0)
    gains_g = [r for r in gross if r > 0]
    losses_g = [r for r in gross if r < 0]
    gains_n = [r for r in net if r > 0]
    losses_n = [r for r in net if r < 0]

    # 누적 수익 (복리)
    eq = [100.0]
    for r in results:
        if r["trades"]:
            avg = np.mean([t["ret_net"] for t in r["trades"]])
            eq.append(eq[-1] * (1 + avg / 100))
    cum_ret = eq[-1] - 100

    # MDD
    peak = eq[0]
    mdd = 0
    dd_start = 0
    max_dd_duration = 0
    current_dd_start = 0
    for i, v in enumerate(eq):
        if v > peak:
            peak = v
            if current_dd_start > 0:
                max_dd_duration = max(max_dd_duration, i - current_dd_start)
            current_dd_start = 0
        dd = (v - peak) / peak * 100
        if dd < mdd:
            mdd = dd
        if dd < 0 and current_dd_start == 0:
            current_dd_start = i

    # CAGR
    n_years = len(results) * hold_days / 252 if results else 1
    cagr = ((eq[-1] / 100) ** (1 / max(n_years, 0.1)) - 1) * 100 if eq[-1] > 0 else 0

    # Sharpe (주간 수익률 기준)
    weekly_rets = [r["avg_ret_net"] for r in results if "avg_ret_net" in r]
    sharpe = (np.mean(weekly_rets) / np.std(weekly_rets) * np.sqrt(52/hold_days)
              if weekly_rets and np.std(weekly_rets) > 0 else 0)

    # Calmar
    calmar = cagr / abs(mdd) if mdd != 0 else 0

    # PF
    pf_g = abs(sum(gains_g) / sum(losses_g)) if losses_g else float('inf')
    pf_n = abs(sum(gains_n) / sum(losses_n)) if losses_n else float('inf')

    # 연속 손실
    max_consec_loss = 0
    cur = 0
    for r in net:
        if r < 0: cur += 1; max_consec_loss = max(max_consec_loss, cur)
        else: cur = 0

    # 평균 보유기간
    avg_hold = np.mean([t["hold_days"] for t in all_trades])

    # 시장 상관/Beta/Alpha
    strat_rets = []
    idx_rets = []
    for r in results:
        if not r["trades"]: continue
        d = r["date"]
        di = trading_dates.index(d) if d in trading_dates else -1
        if di < 0: continue
        edi = min(di+1, len(trading_dates)-1)
        xdi = min(di+hold_days, len(trading_dates)-1)
        ie = idx_df[idx_df["date"]==trading_dates[edi]]
        ix = idx_df[idx_df["date"]==trading_dates[xdi]]
        if len(ie)>0 and len(ix)>0:
            iev = float(ie["close"].iloc[0])
            ixv = float(ix["close"].iloc[0])
            if iev > 0:
                strat_rets.append(r.get("avg_ret_net", 0))
                idx_rets.append((ixv/iev-1)*100)

    corr = np.corrcoef(strat_rets, idx_rets)[0,1] if len(strat_rets) > 5 else 0
    beta = (np.cov(strat_rets, idx_rets)[0,1] / np.var(idx_rets)
            if len(strat_rets) > 5 and np.var(idx_rets) > 0 else 0)
    alpha_w = np.mean(strat_rets) - beta * np.mean(idx_rets) if strat_rets else 0
    alpha_a = alpha_w * 52 / hold_days

    # 캡처율
    up_s = [(s,i) for s,i in zip(strat_rets, idx_rets) if i > 0]
    dn_s = [(s,i) for s,i in zip(strat_rets, idx_rets) if i < 0]
    up_cap = np.mean([s for s,_ in up_s]) / np.mean([i for _,i in up_s]) if up_s else 0
    dn_cap = np.mean([s for s,_ in dn_s]) / np.mean([i for _,i in dn_s]) if dn_s else 0

    # Top3 종목 기여
    tk_contrib = defaultdict(float)
    for t in all_trades: tk_contrib[t["ticker"]] += t["ret_net"]
    top3 = sorted(tk_contrib.values(), reverse=True)[:3]
    total_c = sum(tk_contrib.values())
    top3_pct = sum(top3) / total_c * 100 if total_c != 0 else 0

    # Exit reason 분포
    exit_dist = defaultdict(int)
    for t in all_trades: exit_dist[t.get("exit_reason","HOLD")] += 1

    return {
        "label": label,
        "n_trades": len(all_trades), "n_rounds": len(results),
        "cum_ret_net": round(cum_ret, 2), "cagr": round(cagr, 2),
        "mdd": round(mdd, 2), "calmar": round(calmar, 3),
        "sharpe": round(sharpe, 3),
        "pf_gross": round(pf_g, 2), "pf_net": round(pf_n, 2),
        "avg_ret_gross": round(np.mean(gross), 2),
        "avg_ret_net": round(np.mean(net), 2),
        "median_net": round(np.median(net), 2),
        "win_rate_gross": round(wins_g/len(gross)*100, 1),
        "win_rate_net": round(wins_n/len(net)*100, 1),
        "avg_gain": round(np.mean(gains_n), 2) if gains_n else 0,
        "avg_loss": round(np.mean(losses_n), 2) if losses_n else 0,
        "max_gain": round(max(net), 2), "max_loss": round(min(net), 2),
        "max_consec_loss": max_consec_loss,
        "dd_duration": max_dd_duration,
        "avg_hold": round(avg_hold, 1),
        "corr": round(corr, 3), "beta": round(beta, 2),
        "alpha_annual": round(alpha_a, 2),
        "up_capture": round(up_cap, 2), "dn_capture": round(dn_cap, 2),
        "top3_pct": round(top3_pct, 1),
        "exit_dist": dict(exit_dist),
        "equity": eq,
    }

# ── 구간별 분석 ──────────────────────────────────────────────────────────────
def segment_analysis(results, idx_df, trading_dates, label):
    all_trades = [t for r in results for t in r["trades"]]
    if not all_trades: return

    # 연도별
    yearly = defaultdict(list)
    for t in all_trades: yearly[t["entry_date"][:4]].append(t["ret_net"])
    print(f"\n  [{label}] 연도별:")
    for y in sorted(yearly):
        r = yearly[y]; w = sum(1 for x in r if x > 0)
        print(f"    {y}: avg={np.mean(r):+.2f}% med={np.median(r):+.2f}% "
              f"wr={w/len(r)*100:.0f}% n={len(r)}")

    # Breadth 구간별
    breadth_buckets = {"BULL(>80%)": [], "NEUTRAL": [], "BEAR(<50%)": []}
    for r in results:
        b = r.get("breadth", 50)
        rets = [t["ret_net"] for t in r["trades"]]
        if b > 80: breadth_buckets["BULL(>80%)"].extend(rets)
        elif b >= 50: breadth_buckets["NEUTRAL"].extend(rets)
        else: breadth_buckets["BEAR(<50%)"].extend(rets)

    print(f"  [{label}] Breadth별:")
    for k, v in breadth_buckets.items():
        if not v: print(f"    {k}: N/A"); continue
        w = sum(1 for x in v if x > 0)
        print(f"    {k}: avg={np.mean(v):+.2f}% wr={w/len(v)*100:.0f}% n={len(v)}")

    # 하락일
    down_rets = []
    for r in results:
        d = r["date"]
        if d not in trading_dates: continue
        di = trading_dates.index(d)
        if di < 1: continue
        ie = idx_df[idx_df["date"]==trading_dates[di]]
        ip = idx_df[idx_df["date"]==trading_dates[di-1]]
        if len(ie)>0 and len(ip)>0:
            ir = (float(ie["close"].iloc[0])/float(ip["close"].iloc[0])-1)*100
            if ir < -1:
                down_rets.extend([t["ret_net"] for t in r["trades"]])
    if down_rets:
        w = sum(1 for x in down_rets if x > 0)
        print(f"  [{label}] 하락일(<-1%): avg={np.mean(down_rets):+.2f}% "
              f"wr={w/len(down_rets)*100:.0f}% n={len(down_rets)}")

# ── 메인 ─────────────────────────────────────────────────────────────────────
def run_experiment(start_date="20230601", hold_days=5):
    print("=" * 95)
    print(f"  Score Top10 전략 개선 실험  |  {start_date}~  |  보유: {hold_days}일")
    print("=" * 95)

    all_data = preload()
    idx_df = load_index()
    _load_sector_map()

    ref = all_data.get("005930")
    if ref is None: print("005930 없음"); return
    td = ref[ref["date"] >= start_date]["date"].tolist()
    rebal = td[::hold_days]
    rebal = [d for d in rebal if td.index(d) + hold_days < len(td)]
    print(f"거래일: {len(td)} | 리밸런싱: {len(rebal)}회")

    all_metrics = {}

    for sname, cfg in STRATEGIES.items():
        print(f"\n{'━'*95}")
        print(f"  전략: {sname} ({cfg})")
        print(f"{'━'*95}")

        results = []
        pick_counts = defaultdict(int)
        prev_picks = []
        t0 = _time.monotonic()

        for i, d in enumerate(rebal):
            if (i+1) % 10 == 0 or i == 0:
                eta = (_time.monotonic()-t0)/(i+1)*(len(rebal)-i-1) if i>0 else 0
                print(f"\r  [{i+1}/{len(rebal)}] {d} ETA {eta:.0f}s  ", end="", flush=True)

            breadth = calc_breadth(all_data, d)
            picks = score_universe(all_data, d, cfg, prev_picks, pick_counts, breadth)
            trades = simulate(all_data, picks, d, hold_days, cfg, td)

            avg_g = np.mean([t["ret_gross"] for t in trades]) if trades else 0
            avg_n = np.mean([t["ret_net"] for t in trades]) if trades else 0

            results.append({
                "date": d, "breadth": breadth,
                "trades": trades, "n": len(trades),
                "avg_ret_gross": round(avg_g, 2),
                "avg_ret_net": round(avg_n, 2),
            })

            # 연속 선정 카운트 업데이트
            current = {t["ticker"] for t in trades}
            for tk in current: pick_counts[tk] = pick_counts.get(tk, 0) + 1
            for tk in list(pick_counts):
                if tk not in current: pick_counts[tk] = 0
            prev_picks = list(current)

        elapsed = _time.monotonic() - t0
        print(f"\r  {sname} 완료: {elapsed:.0f}초 ({len(rebal)}회)          ")

        m = calc_metrics(results, idx_df, td, hold_days, sname)
        m["results"] = results
        all_metrics[sname] = m

        # 구간별
        segment_analysis(results, idx_df, td, sname)

    # ── 비교표 ────────────────────────────────────────────────────────
    print(f"\n{'='*95}")
    print(f"  전략 비교 (비용 반영, {start_date}~)")
    print(f"{'='*95}")

    headers = ["지표"] + list(STRATEGIES.keys())
    rows_data = [
        ("누적수익(net)", "cum_ret_net", "%"),
        ("CAGR", "cagr", "%"),
        ("MDD", "mdd", "%"),
        ("Calmar", "calmar", ""),
        ("Sharpe", "sharpe", ""),
        ("PF(gross)", "pf_gross", ""),
        ("PF(net)", "pf_net", ""),
        ("평균수익(net)", "avg_ret_net", "%"),
        ("중앙값(net)", "median_net", "%"),
        ("승률(net)", "win_rate_net", "%"),
        ("평균이익", "avg_gain", "%"),
        ("평균손실", "avg_loss", "%"),
        ("최대수익", "max_gain", "%"),
        ("최대손실", "max_loss", "%"),
        ("거래수", "n_trades", ""),
        ("연속손실", "max_consec_loss", ""),
        ("DD기간", "dd_duration", ""),
        ("평균보유", "avg_hold", "일"),
        ("상관계수", "corr", ""),
        ("Beta", "beta", ""),
        ("Alpha(연)", "alpha_annual", "%"),
        ("상승캡처", "up_capture", "x"),
        ("하락캡처", "dn_capture", "x"),
        ("Top3집중", "top3_pct", "%"),
    ]

    # 헤더
    print(f"\n  {'지표':<14}", end="")
    for s in STRATEGIES: print(f" {s:>12}", end="")
    print()
    print(f"  {'─'*14}", end="")
    for _ in STRATEGIES: print(f" {'─'*12}", end="")
    print()

    for display, key, unit in rows_data:
        print(f"  {display:<14}", end="")
        for sname in STRATEGIES:
            m = all_metrics[sname]
            val = m.get(key, "N/A")
            if isinstance(val, float):
                print(f" {val:>+11.2f}{unit[0] if unit else ''}", end="")
            elif isinstance(val, int):
                print(f" {val:>12}", end="")
            else:
                print(f" {'N/A':>12}", end="")
        print()

    # Exit reason 분포
    print(f"\n  청산 사유:")
    for sname in STRATEGIES:
        m = all_metrics[sname]
        ed = m.get("exit_dist", {})
        parts = [f"{k}={v}" for k,v in sorted(ed.items())]
        print(f"    {sname:10}: {', '.join(parts)}")

    # ── 핵심 검증 ─────────────────────────────────────────────────────
    print(f"\n{'='*95}")
    print(f"  핵심 검증 포인트")
    print(f"{'='*95}")

    base = all_metrics["BASELINE"]
    for sname, m in all_metrics.items():
        if sname == "BASELINE": continue
        checks = []
        # MDD 개선?
        mdd_imp = m["mdd"] > base["mdd"]  # mdd는 음수, 더 큰(0에 가까운) = 개선
        checks.append(f"MDD {'개선' if mdd_imp else '악화'} ({base['mdd']:.1f}→{m['mdd']:.1f})")
        # 중앙값 개선?
        med_imp = m["median_net"] > base["median_net"]
        checks.append(f"중앙값 {'개선' if med_imp else '악화'} ({base['median_net']:.2f}→{m['median_net']:.2f})")
        # 승률 개선?
        wr_imp = m["win_rate_net"] > base["win_rate_net"]
        checks.append(f"승률 {'개선' if wr_imp else '악화'} ({base['win_rate_net']:.1f}→{m['win_rate_net']:.1f})")
        # Alpha 개선?
        a_imp = m["alpha_annual"] > base["alpha_annual"]
        checks.append(f"Alpha {'개선' if a_imp else '악화'} ({base['alpha_annual']:.1f}→{m['alpha_annual']:.1f})")
        # PF > 1?
        pf_ok = m["pf_net"] > 1.0
        # 실패 판정
        failed = (not mdd_imp and not a_imp and m["median_net"] < 0 and not pf_ok)
        status = "FAILED" if failed else "PASS"
        print(f"\n  [{sname}] → {status}")
        for c in checks: print(f"    {c}")
        if not pf_ok: print(f"    PF(net)={m['pf_net']:.2f} <= 1.0 → 비용 반영 후 수익 불가")

    # ── 최종 결론 ─────────────────────────────────────────────────────
    print(f"\n{'='*95}")
    print(f"  최종 결론")
    print(f"{'='*95}")

    valid = {k: v for k, v in all_metrics.items() if v["n_trades"] > 0}

    best_overall = max(valid, key=lambda k: valid[k].get("cum_ret_net", -999))
    best_defense = max(valid, key=lambda k: valid[k].get("mdd", -999))
    best_bull = max(valid, key=lambda k: valid[k].get("up_capture", -999))
    worst_risk = min(valid, key=lambda k: valid[k].get("mdd", 0))

    m_bo = valid[best_overall]
    m_bd = valid[best_defense]
    m_bb = valid[best_bull]
    m_wr = valid[worst_risk]

    print(f"\n  Best overall:    {best_overall} (누적 {m_bo['cum_ret_net']:+.1f}%, "
          f"Sharpe {m_bo['sharpe']:.2f})")
    print(f"  Best defensive:  {best_defense} (MDD {m_bd['mdd']:.1f}%, "
          f"Calmar {m_bd['calmar']:.3f})")
    print(f"  Best bull-mkt:   {best_bull} (상승캡처 {m_bb['up_capture']:.2f}x)")
    print(f"  Worst risk:      {worst_risk} (MDD {m_wr['mdd']:.1f}%)")

    # EXP_E vs BASELINE
    if "EXP_E" in valid:
        e = valid["EXP_E"]
        b = valid["BASELINE"]
        improved = []
        if e["mdd"] > b["mdd"]: improved.append("MDD")
        if e["median_net"] > b["median_net"]: improved.append("중앙값")
        if e["win_rate_net"] > b["win_rate_net"]: improved.append("승률")
        if e["alpha_annual"] > b["alpha_annual"]: improved.append("Alpha")
        print(f"\n  EXP_E vs BASELINE 개선: {', '.join(improved) if improved else '없음'}")

    print(f"\n  ※ 주의: 수급 데이터는 volume proxy 사용 (실제 외인/기관 데이터 아님)")
    print(f"  ※ 생존 편향: 현재 상장 종목만 분석 (상장폐지 종목 제외)")

    print(f"\n  Recommended next step:")
    if "EXP_E" in valid and valid["EXP_E"]["pf_net"] > 1.0:
        print(f"    → EXP_E 전략을 Gen3 실전 전략에 통합 검토")
        print(f"    → 실제 수급 데이터로 EXP_C 재검증 필요")
    else:
        print(f"    → 스코어링 축 재설계 필요 (현재 PF < 1.0)")
        print(f"    → SL/TP 파라미터 최적화 실험 추가")

    # CSV 저장
    csv_path = REPORT_DIR / f"experiment_{datetime.now().strftime('%Y%m%d_%H%M')}.csv"
    REPORT_DIR.mkdir(parents=True, exist_ok=True)
    with open(csv_path, "w", newline="", encoding="utf-8-sig") as f:
        w = csv.writer(f)
        w.writerow(["지표"] + list(STRATEGIES.keys()))
        for display, key, unit in rows_data:
            row = [display]
            for sname in STRATEGIES:
                row.append(all_metrics[sname].get(key, "N/A"))
            w.writerow(row)
    print(f"\n  CSV: {csv_path}")

    return all_metrics


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--start", default="20230601")
    parser.add_argument("--hold", type=int, default=5)
    args = parser.parse_args()
    run_experiment(start_date=args.start, hold_days=args.hold)
