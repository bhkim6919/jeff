"""
backtest_gen4_core.py
=====================
Gen4 Core: Low Volatility + Momentum 전략 백테스트.

변형 4개 + Gen3 비교:
  V1: LowVol30% + Mom6m, 월간, Trail -10%
  V2: LowVol30% + Mom12-1m, 월간, Trail -12%
  V3: LowVol30% + Mom6m+12m blend, 분기, Trail -10%
  V4: V2 + Regime(MA200→현금70%), 월간, Trail -10%

공통:
  - 기간: 2023-06-01 ~ 2026-03-20
  - 비용: buy 0.65%, sell 0.83%
  - 초기자본: 5억원
"""
from __future__ import annotations
import json, sys, warnings
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Tuple

import numpy as np
import pandas as pd

warnings.filterwarnings("ignore")

BASE_DIR = Path(__file__).resolve().parent
OHLCV_DIR = BASE_DIR / "data" / "ohlcv_kospi_daily"
INDEX_FILE = BASE_DIR / "data" / "kospi_index_daily_5y.csv"
REPORT_DIR = BASE_DIR / "data" / "top20" / "reports"

BUY_COST  = 0.0065
SELL_COST = 0.0083
INITIAL   = 500_000_000


# ── Data ──────────────────────────────────────────────────────────────────────
def load_all():
    data = {}
    for f in sorted(OHLCV_DIR.glob("*.csv")):
        try:
            df = pd.read_csv(f, parse_dates=["date"]).sort_values("date").reset_index(drop=True)
            for c in ("open","high","low","close","volume"):
                df[c] = pd.to_numeric(df[c], errors="coerce").fillna(0)
            if len(df) >= 260:
                data[f.stem] = df
        except Exception:
            pass
    return data


def build_matrices(all_data, dates):
    d = {tk: df.set_index("date") for tk, df in all_data.items()}
    close = pd.DataFrame({tk: v["close"] for tk, v in d.items()}, index=dates).ffill().fillna(0)
    opn   = pd.DataFrame({tk: v["open"]  for tk, v in d.items()}, index=dates).ffill().fillna(0)
    high  = pd.DataFrame({tk: v["high"]  for tk, v in d.items()}, index=dates).ffill().fillna(0)
    low   = pd.DataFrame({tk: v["low"]   for tk, v in d.items()}, index=dates).ffill().fillna(0)
    vol   = pd.DataFrame({tk: v["volume"]for tk, v in d.items()}, index=dates).ffill().fillna(0)
    return close, opn, high, low, vol


# ── Universe ──────────────────────────────────────────────────────────────────
def get_universe(close, vol, i):
    if i < 20: return []
    c = close.iloc[i]
    amt = (close.iloc[max(0,i-19):i+1] * vol.iloc[max(0,i-19):i+1]).mean()
    ok = (c >= 2000) & (amt >= 2e9) & (c > 0)
    return ok[ok].index.tolist()


# ── Metrics ───────────────────────────────────────────────────────────────────
def metrics(eq: pd.Series, trades: list, idx_rets: pd.Series) -> dict:
    if len(eq) < 2: return {}
    r = eq.pct_change().dropna()
    tot = eq.iloc[-1]/eq.iloc[0]-1
    ny = len(eq)/252
    cagr = (eq.iloc[-1]/eq.iloc[0])**(1/ny)-1 if ny>0 else 0
    pk = eq.expanding().max(); dd=(eq-pk)/pk; mdd=float(dd.min())
    sharpe = float(r.mean()/r.std()*np.sqrt(252)) if r.std()>0 else 0
    calmar = abs(cagr/mdd) if mdd!=0 else 0

    pnls = [t["pnl_pct"] for t in trades] if trades else []
    wins = [p for p in pnls if p>0]; losses = [p for p in pnls if p<=0]
    wr = len(wins)/len(pnls) if pnls else 0
    pf = abs(sum(wins)/sum(losses)) if losses and sum(losses)!=0 else 999
    avg = np.mean(pnls) if pnls else 0
    med = np.median(pnls) if pnls else 0
    hd = [t.get("hold_days",0) for t in trades]
    avg_hold = np.mean(hd) if hd else 0

    # turnover: total traded / avg equity
    if trades and len(eq)>0:
        total_traded = sum(abs(t.get("pnl_amount",0))/max(abs(t["pnl_pct"]),0.001)
                          for t in trades if t["pnl_pct"]!=0)
        avg_eq = eq.mean()
        turnover = total_traded / avg_eq / ny if avg_eq>0 and ny>0 else 0
    else:
        turnover = 0

    al = pd.DataFrame({"s":r,"m":idx_rets}).dropna()
    if len(al)>10:
        beta = al["s"].cov(al["m"])/al["m"].var() if al["m"].var()>0 else 1
        mkt_ann = float(al["m"].mean()*252)
        alpha = cagr - (0.03 + beta*(mkt_ann-0.03))
        up=al[al["m"]>0]; dn=al[al["m"]<0]
        up_cap = float(up["s"].mean()/up["m"].mean()) if len(up)>0 and up["m"].mean()!=0 else 0
        dn_cap = float(dn["s"].mean()/dn["m"].mean()) if len(dn)>0 and dn["m"].mean()!=0 else 0
    else:
        beta=alpha=up_cap=dn_cap=0

    return dict(total_return=tot, cagr=cagr, mdd=mdd, calmar=calmar, sharpe=sharpe,
                profit_factor=pf, win_rate=wr, avg_return=avg, median_return=med,
                n_trades=len(trades), avg_hold_days=avg_hold, turnover_annual=turnover,
                alpha=alpha, beta=beta, up_capture=up_cap, down_capture=dn_cap,
                avg_win=np.mean(wins) if wins else 0,
                avg_loss=np.mean(losses) if losses else 0,
                max_win=max(pnls) if pnls else 0,
                max_loss=min(pnls) if pnls else 0)


# ── Core Engine ───────────────────────────────────────────────────────────────
def run_lowvol_mom(close, opn, high, low, vol, idx_close, dates,
                   start_i, end_i, *,
                   vol_pct=0.30,        # 변동성 하위 N%
                   mom_mode="6m",       # "6m", "12-1m", "blend"
                   n_stocks=20,
                   rebal_days=21,       # 21=월간, 63=분기
                   trail_pct=0.10,      # trailing SL %
                   regime_filter=False, # MA200 레짐
                   bear_cash_pct=0.70,  # BEAR시 현금 비중
                   label="") -> Tuple[pd.Series, list]:

    print(f"\n  [{label}] vol<{vol_pct:.0%} | mom={mom_mode} | "
          f"rebal={rebal_days}d | trail={trail_pct:.0%} | regime={regime_filter}")

    cash = INITIAL
    positions = {}   # tk -> {qty, entry_price, entry_idx, high_wm}
    trades = []
    equity_hist = {}

    for i in range(start_i, end_i+1):
        dt = dates[i]

        # ── Rebalance ────────────────────────────────────────────────────
        if (i-start_i) % rebal_days == 0 and i >= start_i + 252:

            # Regime check
            is_bear = False
            if regime_filter:
                ma200 = float(idx_close.iloc[max(0,i-199):i+1].mean())
                is_bear = float(idx_close.iloc[i]) < ma200

            universe = get_universe(close, vol, i)

            # ── 1) Volatility: 12m daily std ────────────────────────────
            vol_scores = {}
            mom_scores = {}
            for tk in universe:
                rets = close[tk].iloc[max(0,i-251):i+1].pct_change().dropna()
                if len(rets) < 200: continue
                vol_12m = float(rets.std() * np.sqrt(252))
                if vol_12m <= 0: continue
                vol_scores[tk] = vol_12m

                # ── 2) Momentum ─────────────────────────────────────────
                c_now = float(close[tk].iloc[i])
                if c_now <= 0: continue

                if mom_mode == "6m":
                    c_ref = float(close[tk].iloc[max(0,i-126)])
                    mom_scores[tk] = (c_now/c_ref - 1) if c_ref > 0 else 0
                elif mom_mode == "12-1m":
                    c_skip = float(close[tk].iloc[max(0,i-21)])   # skip 1m
                    c_12m  = float(close[tk].iloc[max(0,i-252)])
                    mom_scores[tk] = (c_skip/c_12m - 1) if c_12m > 0 else 0
                elif mom_mode == "blend":
                    c_6m  = float(close[tk].iloc[max(0,i-126)])
                    c_skip = float(close[tk].iloc[max(0,i-21)])
                    c_12m  = float(close[tk].iloc[max(0,i-252)])
                    m6  = (c_now/c_6m - 1)  if c_6m > 0  else 0
                    m12 = (c_skip/c_12m - 1) if c_12m > 0 else 0
                    mom_scores[tk] = m6 * 0.5 + m12 * 0.5

            if not vol_scores:
                equity_hist[dt] = cash + sum(
                    p["qty"]*float(close[tk].iloc[i])
                    for tk,p in positions.items() if float(close[tk].iloc[i])>0)
                continue

            # Low-vol filter
            vs = pd.Series(vol_scores)
            thresh = vs.quantile(vol_pct)
            low_vol = set(vs[vs <= thresh].index)

            # Positive momentum within low-vol
            cands = [(tk, mom_scores.get(tk,0)) for tk in low_vol
                     if tk in mom_scores and mom_scores[tk] > 0]
            cands.sort(key=lambda x: x[1], reverse=True)

            # Regime: reduce positions in BEAR
            max_n = n_stocks
            if is_bear:
                max_n = max(2, int(n_stocks * (1 - bear_cash_pct)))

            selected = [tk for tk,_ in cands[:max_n]]
            new_set = set(selected)

            # ── Close positions not selected ────────────────────────────
            for tk in list(positions.keys()):
                if tk not in new_set:
                    pos = positions[tk]
                    ep = float(close[tk].iloc[i])
                    if ep > 0:
                        net = ep*(1-SELL_COST)
                        pnl = net/(pos["entry_price"]*(1+BUY_COST))-1
                        cash += pos["qty"]*net
                        trades.append(dict(
                            ticker=tk, entry_date=str(dates[pos["entry_idx"]].date()),
                            exit_date=str(dt.date()), entry_price=pos["entry_price"],
                            exit_price=ep, pnl_pct=pnl, pnl_amount=pos["qty"]*(net-pos["entry_price"]*(1+BUY_COST)),
                            hold_days=i-pos["entry_idx"], exit_reason="REBALANCE"))
                    del positions[tk]

            # ── Open new positions ──────────────────────────────────────
            total_eq = cash + sum(
                p["qty"]*float(close[tk].iloc[i])
                for tk,p in positions.items() if float(close[tk].iloc[i])>0)

            # Equal weight among all selected (including held)
            target_alloc = total_eq / max(len(selected),1) if selected else 0
            if is_bear:
                target_alloc = total_eq * (1-bear_cash_pct) / max(len(selected),1)

            for tk in selected:
                if tk in positions: continue
                if i+1 > end_i: continue
                ep = float(opn[tk].iloc[i+1])
                if ep <= 0: continue
                alloc = min(target_alloc, cash*0.95)
                qty = int(alloc / (ep*(1+BUY_COST)))
                if qty <= 0: continue
                cost = qty*ep*(1+BUY_COST)
                if cost > cash: continue
                cash -= cost
                positions[tk] = dict(qty=qty, entry_price=ep, entry_idx=i+1, high_wm=ep)

        # ── Daily: Trailing SL ───────────────────────────────────────────
        for tk in list(positions.keys()):
            pos = positions[tk]
            h = float(high[tk].iloc[i])
            l = float(low[tk].iloc[i])
            if h > pos["high_wm"]:
                pos["high_wm"] = h
            trail = pos["high_wm"] * (1 - trail_pct)
            if l > 0 and l <= trail:
                ep = trail
                net = ep*(1-SELL_COST)
                pnl = net/(pos["entry_price"]*(1+BUY_COST))-1
                cash += pos["qty"]*net
                trades.append(dict(
                    ticker=tk, entry_date=str(dates[pos["entry_idx"]].date()),
                    exit_date=str(dt.date()), entry_price=pos["entry_price"],
                    exit_price=ep, pnl_pct=pnl, pnl_amount=pos["qty"]*(net-pos["entry_price"]*(1+BUY_COST)),
                    hold_days=i-pos["entry_idx"], exit_reason="TRAIL_SL"))
                del positions[tk]

        # Equity
        pv = cash
        for tk,p in positions.items():
            c = float(close[tk].iloc[i])
            if c > 0: pv += p["qty"]*c
        equity_hist[dt] = pv

    # Close remaining
    for tk,pos in list(positions.items()):
        c = float(close[tk].iloc[end_i])
        if c > 0:
            net=c*(1-SELL_COST); pnl=net/(pos["entry_price"]*(1+BUY_COST))-1
            trades.append(dict(
                ticker=tk, entry_date=str(dates[pos["entry_idx"]].date()),
                exit_date=str(dates[end_i].date()), entry_price=pos["entry_price"],
                exit_price=c, pnl_pct=pnl, pnl_amount=pos["qty"]*(net-pos["entry_price"]*(1+BUY_COST)),
                hold_days=end_i-pos["entry_idx"], exit_reason="END_OF_TEST"))

    return pd.Series(equity_hist).sort_index(), trades


# ── HTML Report ───────────────────────────────────────────────────────────────
def html_report(results: dict, kospi_ret: float, path: Path):
    def fp(v): return f"{v*100:+.1f}%" if isinstance(v,(int,float)) else str(v)
    def fn(v): return f"{v:.2f}" if isinstance(v,(int,float)) else str(v)

    names = list(results.keys())
    rows_data = [
        ("Total Return","total_return",fp),("CAGR","cagr",fp),("MDD","mdd",fp),
        ("Calmar","calmar",fn),("Sharpe","sharpe",fn),("PF(net)","profit_factor",fn),
        ("Win Rate","win_rate",fp),("Avg Return","avg_return",fp),
        ("Median Return","median_return",fp),
        ("Avg Win","avg_win",fp),("Avg Loss","avg_loss",fp),
        ("Max Win","max_win",fp),("Max Loss","max_loss",fp),
        ("Trades","n_trades",lambda v:str(int(v))),
        ("Avg Hold Days","avg_hold_days",fn),
        ("Turnover/Year","turnover_annual",fn),
        ("Alpha","alpha",fp),("Beta","beta",fn),
        ("Up Capture","up_capture",fn),("Down Capture","down_capture",fn),
    ]

    tbl = "<table><tr><th>Metric</th>"
    for n in names: tbl += f"<th>{n}</th>"
    tbl += "</tr>\n"
    for label, key, fmt in rows_data:
        tbl += f"<tr><td style='text-align:left'><b>{label}</b></td>"
        vals = [results[n]["m"].get(key,0) for n in names]
        for v in vals:
            color = ""
            if key=="total_return" and v>0: color=" style='color:#00ff88'"
            elif key=="total_return" and v<0: color=" style='color:#ff4444'"
            tbl += f"<td{color}>{fmt(v)}</td>"
        tbl += "</tr>\n"
    tbl += "</table>"

    configs = {
        "V1.Mom6m": "LowVol30% + 6개월모멘텀, 월간리밸, Trail-10%, 20종목",
        "V2.Mom12-1": "LowVol30% + 12-1개월모멘텀, 월간리밸, Trail-12%, 20종목",
        "V3.Blend.Q": "LowVol30% + 6m+12m블렌드, 분기리밸, Trail-10%, 20종목",
        "V4.Regime": "V2 + MA200레짐(BEAR→현금70%), 월간리밸, Trail-10%, 20종목",
        "Gen3.Current": "RS Composite + 20일돌파 + ATR SL + MAX_LOSS_CAP -8%",
    }

    desc = ""
    for n in names:
        d = configs.get(n, "")
        desc += f"<div class='card'><b>{n}</b>: {d}</div>\n"

    html = f"""<!DOCTYPE html><html><head><meta charset="utf-8">
<title>Gen4 Core Strategy Backtest</title>
<style>
body{{font-family:'Segoe UI',Arial;background:#0a0a0a;color:#e0e0e0;padding:20px}}
h1{{color:#00d4ff;border-bottom:2px solid #00d4ff;padding-bottom:10px}}
h2{{color:#ffa500;margin-top:30px}}
table{{border-collapse:collapse;margin:10px 0 20px;width:100%}}
th{{background:#1a1a2e;color:#00d4ff;padding:8px 12px;text-align:center;border:1px solid #333}}
td{{padding:6px 12px;border:1px solid #333;text-align:center}}
tr:nth-child(even){{background:#111}}
tr:hover{{background:#1a1a2e}}
.card{{background:#111;border:1px solid #333;border-radius:8px;padding:12px;margin:8px 0}}
</style></head><body>
<h1>Gen4 Core: Low Volatility + Momentum Backtest</h1>
<p>Period: 2023-06-01 ~ 2026-03-20 | KOSPI Buy&Hold: {kospi_ret*100:+.1f}% | Generated: {datetime.now().strftime('%Y-%m-%d %H:%M')}</p>
<h2>Strategy Comparison</h2>
{tbl}
<h2>Strategy Configs</h2>
{desc}
<footer style="margin-top:40px;color:#666;border-top:1px solid #333;padding-top:10px">
Gen4 Core Backtest v1.0</footer></body></html>"""
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(html, encoding="utf-8")
    print(f"\n  Report: {path}")


# ── Main ──────────────────────────────────────────────────────────────────────
def main():
    print("="*70)
    print("  Gen4 Core: LowVol + Momentum Backtest")
    print("="*70)

    print("\n[1/3] Loading...")
    all_data = load_all()
    idx_df = pd.read_csv(INDEX_FILE, parse_dates=["date"]).sort_values("date").reset_index(drop=True)
    dates = idx_df["date"]
    print(f"  {len(all_data)} stocks, {len(dates)} dates")

    close, opn, high, low, vol = build_matrices(all_data, dates)
    idx_close = idx_df.set_index("date")["close"].reindex(dates).ffill()
    idx_rets = idx_close.pct_change().fillna(0)

    sd = pd.Timestamp("2023-06-01"); ed = pd.Timestamp("2026-03-20")
    si = int((dates>=sd).values.argmax())
    ei = int(len(dates) - 1 - (dates<=ed).values[::-1].argmax())

    print(f"\n[2/3] Running variants...")

    configs = [
        ("V1.Mom6m",    dict(mom_mode="6m",   rebal_days=21, trail_pct=0.10, regime_filter=False)),
        ("V2.Mom12-1",  dict(mom_mode="12-1m",rebal_days=21, trail_pct=0.12, regime_filter=False)),
        ("V3.Blend.Q",  dict(mom_mode="blend", rebal_days=63, trail_pct=0.10, regime_filter=False)),
        ("V4.Regime",   dict(mom_mode="12-1m",rebal_days=21, trail_pct=0.10, regime_filter=True, bear_cash_pct=0.70)),
    ]

    results = {}
    for name, kwargs in configs:
        eq, tr = run_lowvol_mom(close, opn, high, low, vol, idx_close, dates,
                                si, ei, label=name, **kwargs)
        m = metrics(eq, tr, idx_rets)
        results[name] = {"eq":eq, "tr":tr, "m":m}
        print(f"    Return={m['total_return']*100:+.1f}%  MDD={m['mdd']*100:.1f}%  "
              f"PF={m['profit_factor']:.2f}  Alpha={m['alpha']*100:+.1f}%  "
              f"Trades={m['n_trades']}  AvgHold={m['avg_hold_days']:.0f}d")

    # Gen3 comparison
    g3eq = REPORT_DIR/"backtest_gen3_equity.csv"
    g3tr = REPORT_DIR/"backtest_gen3_trades.csv"
    if g3eq.exists() and g3tr.exists():
        eq3 = pd.read_csv(g3eq, index_col=0, parse_dates=True)["equity"]
        tr3 = pd.read_csv(g3tr).to_dict("records")
        m3 = metrics(eq3, tr3, idx_rets)
        results["Gen3.Current"] = {"eq":eq3, "tr":tr3, "m":m3}
        print(f"\n  Gen3.Current: Return={m3['total_return']*100:+.1f}%  "
              f"MDD={m3['mdd']*100:.1f}%  Alpha={m3['alpha']*100:+.1f}%")

    kospi_ret = float(idx_close.iloc[ei]/idx_close.iloc[si]-1)
    print(f"\n  KOSPI: {kospi_ret*100:+.1f}%")

    print(f"\n[3/3] Report...")
    html_report(results, kospi_ret, REPORT_DIR/"gen4_core_backtest.html")

    # Save trades
    for name, data in results.items():
        if data["tr"]:
            pd.DataFrame(data["tr"]).to_csv(
                REPORT_DIR/f"trades_gen4_{name.replace('.','_')}.csv",
                index=False, encoding="utf-8-sig")

    # Final ranking
    print(f"\n{'='*70}")
    print(f"  RANKING (by Alpha)")
    print(f"{'='*70}")
    ranked = sorted(results.items(), key=lambda x:x[1]["m"].get("alpha",0), reverse=True)
    for r,(n,d) in enumerate(ranked,1):
        m=d["m"]
        print(f"  #{r} {n:16s}  Alpha={m['alpha']*100:+.1f}%  "
              f"Return={m['total_return']*100:+.1f}%  MDD={m['mdd']*100:.1f}%  "
              f"PF={m['profit_factor']:.2f}  Trades={m['n_trades']}  "
              f"Hold={m['avg_hold_days']:.0f}d  Turn={m.get('turnover_annual',0):.1f}x")
    print(f"\n  KOSPI: {kospi_ret*100:+.1f}%")
    print(f"{'='*70}")


if __name__ == "__main__":
    main()
