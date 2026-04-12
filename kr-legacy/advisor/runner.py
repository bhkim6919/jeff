"""
Advisor Runner - CLI entry point for daily analysis.

Usage:
    cd kr-legacy
    python -m advisor.runner --date 20260401 --mode paper
    python -m advisor.runner --date 20260401 --mode paper_test
"""
from __future__ import annotations

import argparse
import json
import sys
import time
from datetime import datetime
from pathlib import Path

# Ensure kr-legacy is on path
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))


def run_analysis(date: str, mode: str) -> dict:
    """Run full Phase 1 analysis for a single day.

    All errors are caught - advisor failure never affects engine.
    """
    from advisor.ingestion.snapshot_builder import build_daily_snapshot
    from advisor.ingestion.schema import DailySnapshot
    from advisor.ingestion.data_filter import build_window, OperationalDataError
    from advisor.pipeline.validator import validate_snapshot
    from advisor.analyzers.pnl_analyzer import analyze_pnl
    from advisor.analyzers.mdd_analyzer import analyze_mdd_contributors
    from advisor.alerters.regime_alerter import check_regime_alerts
    from advisor.alerters.concentration import check_concentration_alerts
    from advisor.alerters.operational import check_operational_alerts
    from advisor.pipeline.priority_filter import filter_and_prioritize
    from advisor.recommenders.param_recommender import recommend_params
    from advisor.pipeline.drift_guard import AdvisorDriftGuard
    from advisor.pipeline.metrics import load_metrics, save_metrics, record_recommendation
    from advisor.config import OUTPUT_DIR, REPORT_DIR, REPORT_DIR_TEST

    start_time = time.time()
    result = {
        "date": date,
        "mode": mode,
        "status": "UNKNOWN",
        "errors": [],
    }

    try:
        # ── 1. Build snapshot ──
        snapshot = build_daily_snapshot(date, mode)
        result["snapshot_hash"] = snapshot.snapshot_hash
        result["operational_flags"] = snapshot.operational_flags

        # ── 2. Validate ──
        is_valid, validation_errors = validate_snapshot(snapshot)
        result["validation_errors"] = validation_errors

        if not is_valid:
            result["status"] = "VALIDATION_FAILED"
            result["errors"] = [e for e in validation_errors
                                if e.startswith("REJECT")]
            return result

        # ── 3. Analyze ──
        pnl = analyze_pnl(snapshot)
        mdd_contributors = analyze_mdd_contributors(snapshot)

        result["pnl_summary"] = pnl
        result["mdd_contributors"] = mdd_contributors

        # ── 4. Alerts (Phase 2) ──
        all_alerts = []
        all_alerts.extend(check_regime_alerts(snapshot))
        all_alerts.extend(check_concentration_alerts(snapshot))
        all_alerts.extend(check_operational_alerts(snapshot))

        # Priority filter: dedup + limit + diversity
        filtered_alerts = filter_and_prioritize(all_alerts)
        result["alerts"] = filtered_alerts
        result["alerts_total_before_filter"] = len(all_alerts)

        # ── 5. Recommendations (Phase 3) ──
        try:
            # Build multi-day window from equity_log dates
            from advisor.ingestion.snapshot_builder import build_daily_snapshot as _build
            report_dir = REPORT_DIR_TEST if mode == "paper_test" else REPORT_DIR
            import csv as _csv
            _eq_path = report_dir / "equity_log.csv"
            all_snapshots = [snapshot]  # today
            if _eq_path.exists():
                with open(_eq_path, "r", encoding="utf-8") as _f:
                    _eq_rows = list(_csv.DictReader(_f))
                # Build snapshots for recent dates (lightweight: equity only)
                # Also load close_log for window-level analysis
                _cl_path = report_dir / "close_log.csv"
                _closes_by_date = {}
                if _cl_path.exists():
                    with open(_cl_path, "r", encoding="utf-8") as _cf:
                        for _cr in _csv.DictReader(_cf):
                            _cd = _cr.get("date", "").strip()
                            _closes_by_date.setdefault(_cd, []).append(_cr)

                for row in _eq_rows[-20:]:
                    d = row.get("date", "").strip().replace("-", "")
                    d_dash = row.get("date", "").strip()
                    if d and d != date and len(d) == 8:
                        mini = DailySnapshot(
                            trading_day=d,
                            data_cutoff_time=f"{d[:4]}-{d[4:6]}-{d[6:]}T16:30:00",
                            reference_point="EOD",
                            meta=snapshot.meta,
                            equity=row,
                            closes=_closes_by_date.get(d_dash, []),
                        )
                        recon = int(row.get("reconcile_corrections", "0").strip() or "0")
                        if recon > 10:
                            mini.operational_flags = ["RECON_SAFETY"]
                        all_snapshots.append(mini)

            all_snapshots.sort(key=lambda s: s.trading_day)
            try:
                multi_window = build_window(
                    all_snapshots, date,
                    window=min(20, len(all_snapshots)))
            except Exception:
                # Fallback: ignore operational flags for recommender
                # (FRESH start always triggers RECON_SAFETY)
                for s in all_snapshots:
                    s.operational_flags = []
                multi_window = build_window(
                    all_snapshots, date,
                    window=min(20, len(all_snapshots)))
            param_recs = recommend_params(multi_window)

            # Drift guard
            metrics = load_metrics()
            drift = AdvisorDriftGuard()
            rec_history = metrics.get("recommendation_history", [])
            drift_warnings = drift.check(rec_history, [])

            # Suspend params if drift detected
            if drift.should_suspend_params(drift_warnings):
                param_recs = []
                drift_warnings.append("PARAM recommendations SUSPENDED by drift guard")

            # Record recommendations
            for rec in param_recs:
                record_recommendation(metrics, rec["id"], rec.get("parameter", ""))
            save_metrics(metrics)

            result["recommendations"] = param_recs
            result["drift_warnings"] = drift_warnings
        except Exception as e:
            result["recommendations"] = []
            result["drift_warnings"] = [f"Phase 3 error: {e}"]

        # ── 6. Intraday Risk (Phase 4) ──
        try:
            from advisor.analyzers.intraday_risk import analyze_intraday_risk
            report_dir = REPORT_DIR_TEST if mode == "paper_test" else REPORT_DIR
            intra_path = report_dir / f"intraday_summary_{date}.json"
            if not intra_path.exists():
                # Try dash-format date
                _dd = f"{date[:4]}{date[4:6]}{date[6:]}"
                intra_path = report_dir / f"intraday_summary_{_dd}.json"

            intraday_alerts = analyze_intraday_risk(intra_path)
            result["intraday_alerts"] = intraday_alerts

            # Merge into main alerts (before final filter)
            if intraday_alerts:
                all_alerts = result.get("alerts", []) + intraday_alerts
                filtered_alerts = filter_and_prioritize(all_alerts)
                result["alerts"] = filtered_alerts
                result["alerts_total_before_filter"] = (
                    result.get("alerts_total_before_filter", 0) + len(intraday_alerts))
        except Exception as e:
            result["intraday_alerts"] = []
            result.setdefault("errors", []).append(f"Phase 4 error: {e}")

        result["status"] = "OK"

    except Exception as e:
        result["status"] = "ERROR"
        result["errors"].append(f"{type(e).__name__}: {e}")

    # ── 4. Save output ──
    elapsed = time.time() - start_time
    result["elapsed_sec"] = round(elapsed, 2)

    meta = {
        "snapshot_hash": result.get("snapshot_hash", ""),
        "advisor_version": "1.0.0",
        "strategy_version": "4.0",
        "mode": mode,
        "elapsed_sec": result["elapsed_sec"],
        "deterministic": True,
        "llm_used": False,
    }

    try:
        out_dir = OUTPUT_DIR / date
        out_dir.mkdir(parents=True, exist_ok=True)

        (out_dir / "daily_analysis.json").write_text(
            json.dumps(result, indent=2, ensure_ascii=False, default=str),
            encoding="utf-8")

        # Save alerts separately for GUI consumption
        alerts_data = result.get("alerts", [])
        (out_dir / "alerts.json").write_text(
            json.dumps(alerts_data, indent=2, ensure_ascii=False, default=str),
            encoding="utf-8")

        # Save recommendations (Phase 3)
        recs_data = result.get("recommendations", [])
        (out_dir / "recommendations.json").write_text(
            json.dumps(recs_data, indent=2, ensure_ascii=False, default=str),
            encoding="utf-8")

        # Save intraday alerts (Phase 4)
        intra_data = result.get("intraday_alerts", [])
        (out_dir / "intraday_alerts.json").write_text(
            json.dumps(intra_data, indent=2, ensure_ascii=False, default=str),
            encoding="utf-8")

        (out_dir / "meta.json").write_text(
            json.dumps(meta, indent=2, ensure_ascii=False),
            encoding="utf-8")

    except Exception as e:
        print(f"[ADVISOR_SAVE_ERROR] {e}")

    return result


def main():
    parser = argparse.ArgumentParser(description="Q-TRON AI Advisor (read-only)")
    parser.add_argument("--date", default=datetime.now().strftime("%Y%m%d"),
                        help="Trading day (YYYYMMDD)")
    parser.add_argument("--mode", default="paper",
                        choices=["mock", "paper", "paper_test", "live"],
                        help="Trading mode")
    args = parser.parse_args()

    print("=" * 60)
    print(f"  Q-TRON AI Advisor v1.1 - Phase 1+2+3+4")
    print(f"  Date: {args.date}  Mode: {args.mode}")
    print("=" * 60)

    result = run_analysis(args.date, args.mode)

    # Console summary
    print(f"\nStatus: {result['status']}")

    if result.get("errors"):
        for e in result["errors"]:
            print(f"  ERROR: {e}")

    if result.get("operational_flags"):
        print(f"  Operational flags: {result['operational_flags']}")

    pnl = result.get("pnl_summary", {})
    if pnl and not pnl.get("error"):
        print(f"\n  Daily PnL: {pnl.get('daily_pnl_pct', 0):+.2%}")
        print(f"  Monthly DD: {pnl.get('monthly_dd_pct', 0):+.2%}")
        print(f"  Risk Mode: {pnl.get('risk_mode', 'N/A')}")
        print(f"  Positions: {pnl.get('n_positions', 0)}")
        print(f"  Trades: {pnl.get('n_buys', 0)} buys, {pnl.get('n_sells', 0)} sells")
        print(f"  Closes: {pnl.get('n_closes', 0)}")

        if pnl.get("top_contributors"):
            print(f"  Top: {pnl['top_contributors'][:2]}")
        if pnl.get("bottom_contributors"):
            print(f"  Bottom: {pnl['bottom_contributors'][:2]}")

    mdd = result.get("mdd_contributors", {})
    if mdd and mdd.get("contributors"):
        print(f"\n  MDD Contributors (worst DD from HWM):")
        for c in mdd["contributors"][:3]:
            print(f"    {c['code']}: {c['dd_from_hwm_pct']:+.1%}")

    # Alerts (Phase 2)
    alerts = result.get("alerts", [])
    total_before = result.get("alerts_total_before_filter", 0)
    if alerts:
        print(f"\n  Alerts ({len(alerts)}/{total_before} after filter):")
        for a in alerts:
            marker = "!!" if a["priority"] == "HIGH" else " >"
            print(f"    {marker} [{a['priority']}] {a['message']}")
    else:
        print(f"\n  Alerts: none")

    # Recommendations (Phase 3)
    recs = result.get("recommendations", [])
    if recs:
        print(f"\n  Recommendations ({len(recs)}):")
        for r in recs:
            print(f"    [{r['confidence']}] {r['parameter']}: {r['rationale'][:80]}")
    drift = result.get("drift_warnings", [])
    if drift:
        print(f"\n  Drift Warnings:")
        for w in drift:
            print(f"    !! {w}")

    # Intraday Risk (Phase 4)
    intra = result.get("intraday_alerts", [])
    if intra:
        print(f"\n  Intraday Risk ({len(intra)} alerts):")
        for a in intra:
            marker = "!!" if a["priority"] == "HIGH" else " >"
            print(f"    {marker} [{a['priority']}] {a['message']}")
            if a.get("debug_hint"):
                print(f"       hint: {a['debug_hint'][:100]}")

    print(f"\n  Elapsed: {result.get('elapsed_sec', 0):.1f}s")
    print(f"  Hash: {result.get('snapshot_hash', 'N/A')}")
    print(f"  Output: advisor/output/{args.date}/")
    print("=" * 60)


if __name__ == "__main__":
    main()
