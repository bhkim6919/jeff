"""S9 driver: build crypto/data/quality_report.html.

Per Jeff S9 spec (2026-04-27):
    Sections:
        1. Coverage   — first_seen 기준, threshold ≥ 95% per pair
        2. Gap        — max consecutive missing days, threshold ≤ 7
        3. Duplicate  — (pair, candle_dt_kst) PK duplicates, threshold = 0
        4. Outlier    — daily_return |Δ| > 50%, volume top 1% (report only)
        5. Time consistency — candle_dt_kst ↔ candle_dt_utc mismatch = 0

    PASS = (1) AND (2) AND (3) AND (5). Outlier (4) is informational.

Output:
    crypto/data/quality_report.html         — main report
    crypto/data/_verification/data_quality_<utc_date>.json — machine-readable
                                                            evidence dump

Usage (worktree root):
    "C:/Q-TRON-32_ARCHIVE/.venv64/Scripts/python.exe" -X utf8 \
        scripts/crypto/data_quality_report.py
"""

from __future__ import annotations

import html
import json
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

HERE = Path(__file__).resolve()
WORKTREE_ROOT = HERE.parents[2]
sys.path.insert(0, str(WORKTREE_ROOT))

from crypto.data.quality import (  # noqa: E402
    compute_coverage,
    compute_duplicates,
    compute_gaps,
    compute_outliers,
    compute_summary,
    compute_time_consistency,
)
from crypto.db.env import ensure_main_project_env_loaded  # noqa: E402


HTML_OUT_PATH = WORKTREE_ROOT / "crypto" / "data" / "quality_report.html"
EVIDENCE_DIR = WORKTREE_ROOT / "crypto" / "data" / "_verification"


# --- HTML rendering helpers --------------------------------------------------


CSS = """
* { box-sizing: border-box; }
body { font-family: ui-sans-serif, -apple-system, "Segoe UI", sans-serif;
       margin: 24px; color: #1f2937; background: #f9fafb; }
h1, h2, h3 { color: #111827; }
h1 { border-bottom: 2px solid #2563eb; padding-bottom: 6px; }
h2 { margin-top: 32px; padding: 4px 0; border-bottom: 1px solid #d1d5db; }
.summary { display: grid; grid-template-columns: repeat(auto-fit, minmax(180px, 1fr));
           gap: 12px; margin-bottom: 24px; }
.card { background: #fff; border: 1px solid #d1d5db; border-radius: 6px;
        padding: 12px 14px; }
.card .label { font-size: 11px; text-transform: uppercase;
               letter-spacing: 0.05em; color: #6b7280; }
.card .value { font-size: 18px; font-weight: 600; margin-top: 4px;
               font-variant-numeric: tabular-nums; }
.card.pass .value { color: #059669; }
.card.fail .value { color: #dc2626; }
.card.info .value { color: #2563eb; }
table { border-collapse: collapse; margin-top: 8px; background: #fff;
        font-size: 13px; font-variant-numeric: tabular-nums; }
th, td { border: 1px solid #e5e7eb; padding: 4px 8px; text-align: right; }
th { background: #f3f4f6; font-weight: 600; text-align: center; }
td.pair, th.pair, td.text { text-align: left; }
tr.fail { background: #fef2f2; }
tr.pass td.verdict { color: #059669; }
tr.fail td.verdict { color: #dc2626; font-weight: 600; }
.bar { display: inline-block; height: 12px; background: #2563eb;
       vertical-align: middle; }
.gate-pass { color: #059669; }
.gate-fail { color: #dc2626; }
.muted { color: #6b7280; }
"""


def _esc(s: Any) -> str:
    return html.escape(str(s))


def _fmt_int(n: int) -> str:
    return f"{n:,}"


def _fmt_float(x: float, places: int = 4) -> str:
    return f"{x:,.{places}f}"


# --- Section renderers -------------------------------------------------------


def render_summary(summary: dict, totals: dict, gate_pass: bool) -> str:
    cards = []
    gate_class = "pass" if gate_pass else "fail"
    gate_label = "PASS" if gate_pass else "FAIL"
    cards.append(
        f'<div class="card {gate_class}"><div class="label">D1 quality gate</div>'
        f'<div class="value">{gate_label}</div></div>'
    )
    cards.append(
        f'<div class="card info"><div class="label">pairs</div>'
        f'<div class="value">{_fmt_int(summary["pair_count"])}</div></div>'
    )
    cards.append(
        f'<div class="card info"><div class="label">rows</div>'
        f'<div class="value">{_fmt_int(summary["row_count"])}</div></div>'
    )
    cards.append(
        f'<div class="card info"><div class="label">date range</div>'
        f'<div class="value">{_esc(summary["earliest_kst"])} → '
        f'{_esc(summary["latest_kst"])}</div></div>'
    )
    cards.append(
        f'<div class="card {"pass" if totals["coverage_fail"]==0 else "fail"}">'
        f'<div class="label">coverage fails</div>'
        f'<div class="value">{totals["coverage_fail"]}</div></div>'
    )
    cards.append(
        f'<div class="card {"pass" if totals["gap_fail"]==0 else "fail"}">'
        f'<div class="label">gap fails (max&gt;7d)</div>'
        f'<div class="value">{totals["gap_fail"]}</div></div>'
    )
    cards.append(
        f'<div class="card {"pass" if totals["duplicate_count"]==0 else "fail"}">'
        f'<div class="label">duplicate keys</div>'
        f'<div class="value">{totals["duplicate_count"]}</div></div>'
    )
    cards.append(
        f'<div class="card {"pass" if totals["timecon_mismatch"]==0 else "fail"}">'
        f'<div class="label">kst↔utc mismatch (#13)</div>'
        f'<div class="value">{totals["timecon_mismatch"]}</div></div>'
    )
    return f'<div class="summary">{"".join(cards)}</div>'


def render_coverage(coverage: dict) -> str:
    pairs = coverage["pairs"]
    fails = [p for p in pairs if not p["pass"]]
    rows = []
    rows.append(
        '<tr><th class="pair">pair</th><th>rows</th>'
        '<th>first_seen</th><th>last_seen</th>'
        '<th>expected_days</th><th>coverage_pct</th><th>verdict</th></tr>'
    )
    # Always show fails first; cap to first 50 pairs total.
    show = fails + [p for p in pairs if p["pass"]][: max(0, 50 - len(fails))]
    for p in show:
        cls = "fail" if not p["pass"] else "pass"
        verdict = "FAIL" if not p["pass"] else "ok"
        rows.append(
            f'<tr class="{cls}"><td class="pair">{_esc(p["pair"])}</td>'
            f'<td>{_fmt_int(p["rows"])}</td>'
            f'<td>{_esc(p["first_seen"])}</td>'
            f'<td>{_esc(p["last_seen"])}</td>'
            f'<td>{_fmt_int(p["expected_days"])}</td>'
            f'<td>{_fmt_float(p["coverage_pct"])}</td>'
            f'<td class="verdict">{verdict}</td></tr>'
        )
    note = ""
    if len(pairs) > 50:
        note = f'<p class="muted">Showing {len(show)} of {len(pairs)} pairs (fails first).</p>'
    return (
        f'<h2>1. Coverage <span class="muted">— first_seen 기준</span></h2>'
        f'<p>Threshold: per-pair coverage_pct ≥ {coverage["threshold_pct"]}%. '
        f'Result: <strong>{"PASS" if coverage["pass"] else "FAIL"}</strong> '
        f'({coverage["fail_count"]} pair(s) failing).</p>'
        f'<table>{"".join(rows)}</table>{note}'
    )


def render_gap(gap: dict) -> str:
    pairs = gap["pairs"]
    fails = [p for p in pairs if not p["pass"]]
    rows = []
    rows.append(
        '<tr><th class="pair">pair</th><th>rows</th>'
        '<th>max_gap_days</th><th>gap_count</th><th>verdict</th></tr>'
    )
    show = fails + [p for p in pairs if p["pass"]][: max(0, 50 - len(fails))]
    for p in show:
        cls = "fail" if not p["pass"] else "pass"
        verdict = "FAIL" if not p["pass"] else "ok"
        rows.append(
            f'<tr class="{cls}"><td class="pair">{_esc(p["pair"])}</td>'
            f'<td>{_fmt_int(p["rows"])}</td>'
            f'<td>{_fmt_int(p["max_gap_days"])}</td>'
            f'<td>{_fmt_int(p["gap_count"])}</td>'
            f'<td class="verdict">{verdict}</td></tr>'
        )

    # Histogram (CSS bar)
    hist_rows = []
    if gap["histogram"]:
        max_count = max(gap["histogram"].values())
        for bucket, count in gap["histogram"].items():
            width_pct = int((count / max_count) * 100) if max_count else 0
            hist_rows.append(
                f'<tr><td class="text">{_esc(bucket)} day(s)</td>'
                f'<td>{_fmt_int(count)}</td>'
                f'<td class="text"><span class="bar" '
                f'style="width:{width_pct*2}px"></span></td></tr>'
            )
        hist_block = (
            '<h3>Gap-size histogram (across all pairs)</h3>'
            '<table><tr><th class="text">missing days</th>'
            '<th>occurrences</th><th class="text">distribution</th></tr>'
            + "".join(hist_rows) + '</table>'
        )
    else:
        hist_block = '<p class="muted">No gaps observed.</p>'

    note = ""
    if len(pairs) > 50:
        note = f'<p class="muted">Showing {len(show)} of {len(pairs)} pairs (fails first).</p>'
    return (
        f'<h2>2. Gap <span class="muted">— consecutive missing days</span></h2>'
        f'<p>Threshold: per-pair max_gap_days ≤ {gap["threshold_days"]}. '
        f'Result: <strong>{"PASS" if gap["pass"] else "FAIL"}</strong> '
        f'({gap["fail_count"]} pair(s) failing).</p>'
        f'<table>{"".join(rows)}</table>{note}{hist_block}'
    )


def render_duplicate(dup: dict) -> str:
    if dup["duplicate_count"] == 0:
        body = '<p>No (pair, candle_dt_kst) duplicates found — PASS.</p>'
    else:
        rows = ['<tr><th class="pair">pair</th><th>candle_dt_kst</th><th>count</th></tr>']
        for s in dup["samples"]:
            rows.append(
                f'<tr class="fail"><td class="pair">{_esc(s["pair"])}</td>'
                f'<td>{_esc(s["candle_dt_kst"])}</td>'
                f'<td>{_fmt_int(s["count"])}</td></tr>'
            )
        body = (
            f'<p>Total duplicate keys: <strong>{_fmt_int(dup["duplicate_count"])}</strong>. '
            f'Showing first {len(dup["samples"])}.</p>'
            f'<table>{"".join(rows)}</table>'
        )
    return f'<h2>3. Duplicate (PK)</h2>{body}'


def render_outlier(out: dict) -> str:
    big_rows = ['<tr><th class="pair">pair</th><th>candle_dt_kst</th>'
                '<th>close</th><th>prev_close</th><th>daily_return</th></tr>']
    for s in out["big_return_samples"]:
        big_rows.append(
            f'<tr><td class="pair">{_esc(s["pair"])}</td>'
            f'<td>{_esc(s["candle_dt_kst"])}</td>'
            f'<td>{_fmt_float(s["close"], 2) if s["close"] is not None else "—"}</td>'
            f'<td>{_fmt_float(s["prev_close"], 2) if s["prev_close"] is not None else "—"}</td>'
            f'<td>{_fmt_float(s["daily_return"]*100, 2) if s["daily_return"] is not None else "—"}%</td></tr>'
        )
    vol_rows = ['<tr><th class="pair">pair</th><th>candle_dt_kst</th>'
                '<th>volume</th><th>value_krw</th></tr>']
    for s in out["volume_top_samples"]:
        vol_rows.append(
            f'<tr><td class="pair">{_esc(s["pair"])}</td>'
            f'<td>{_esc(s["candle_dt_kst"])}</td>'
            f'<td>{_fmt_float(s["volume"], 2) if s["volume"] is not None else "—"}</td>'
            f'<td>{_fmt_float(s["value_krw"], 0) if s["value_krw"] is not None else "—"}</td></tr>'
        )
    return (
        '<h2>4. Outlier <span class="muted">— informational only</span></h2>'
        f'<p>Daily-return |Δ| &gt; {out["return_threshold"]*100:.0f}%: '
        f'<strong>{_fmt_int(out["big_return_count"])}</strong> rows '
        f'(showing top {len(out["big_return_samples"])}).</p>'
        f'<table>{"".join(big_rows)}</table>'
        f'<h3>Volume top 1%</h3>'
        f'<p>p99 volume threshold: {_fmt_float(out["volume_p99"], 4)}. '
        f'Rows above: <strong>{_fmt_int(out["volume_p99_count"])}</strong> '
        f'(showing top {len(out["volume_top_samples"])}).</p>'
        f'<table>{"".join(vol_rows)}</table>'
    )


def render_time_consistency(tc: dict) -> str:
    if tc["mismatch_count"] == 0:
        body = (
            '<p>Zero mismatches between candle_dt_kst and candle_dt_utc. '
            'D1 PASS #13 — <strong>PASS</strong> (S4 hypothesis B invariant '
            'preserved across the full dataset).</p>'
        )
    else:
        rows = ['<tr><th class="pair">pair</th><th>candle_dt_kst</th>'
                '<th>candle_dt_utc</th></tr>']
        for s in tc["samples"]:
            rows.append(
                f'<tr class="fail"><td class="pair">{_esc(s["pair"])}</td>'
                f'<td>{_esc(s["candle_dt_kst"])}</td>'
                f'<td>{_esc(s["candle_dt_utc"])}</td></tr>'
            )
        body = (
            f'<p><strong>FAIL</strong>: {_fmt_int(tc["mismatch_count"])} mismatches '
            f'detected. Re-verify §5.2 boundary hypothesis.</p>'
            f'<table>{"".join(rows)}</table>'
        )
    return f'<h2>5. Time Consistency <span class="muted">— D1 PASS #13</span></h2>{body}'


# --- Driver -------------------------------------------------------------------


def main() -> int:
    print("=" * 78)
    print("S9 data_quality_report — five sections, gate evaluation")
    print("=" * 78)

    ensure_main_project_env_loaded()
    from shared.db.pg_base import connection  # noqa: E402

    started = time.monotonic()
    with connection() as conn:
        summary = compute_summary(conn)
        coverage = compute_coverage(conn, threshold_pct=95.0)
        gap = compute_gaps(conn, threshold_days=7)
        dup = compute_duplicates(conn)
        outlier = compute_outliers(conn, return_threshold=0.50, volume_percentile=0.99)
        timecon = compute_time_consistency(conn)
    elapsed = time.monotonic() - started

    gate_pass = (
        coverage["pass"] and gap["pass"] and dup["pass"] and timecon["pass"]
    )

    totals = {
        "coverage_fail": coverage["fail_count"],
        "gap_fail": gap["fail_count"],
        "duplicate_count": dup["duplicate_count"],
        "timecon_mismatch": timecon["mismatch_count"],
    }

    # Render HTML ---------------------------------------------------------
    generated_at = datetime.now(timezone.utc).replace(microsecond=0).isoformat()
    html_doc = f"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="utf-8">
<title>Crypto Lab D1 — Data Quality Report</title>
<style>{CSS}</style>
</head>
<body>
<h1>Crypto Lab D1 — Data Quality Report</h1>
<p class="muted">Generated at {generated_at} (UTC). Source: PostgreSQL crypto_ohlcv.</p>
{render_summary(summary, totals, gate_pass)}
{render_coverage(coverage)}
{render_gap(gap)}
{render_duplicate(dup)}
{render_outlier(outlier)}
{render_time_consistency(timecon)}
</body>
</html>
"""
    HTML_OUT_PATH.parent.mkdir(parents=True, exist_ok=True)
    HTML_OUT_PATH.write_text(html_doc, encoding="utf-8")

    # Persist evidence JSON ----------------------------------------------
    EVIDENCE_DIR.mkdir(parents=True, exist_ok=True)
    evidence_path = EVIDENCE_DIR / f"data_quality_{generated_at[:10]}.json"
    evidence = {
        "generated_at_utc": generated_at,
        "elapsed_sec": round(elapsed, 3),
        "summary": summary,
        "coverage": {
            "threshold_pct": coverage["threshold_pct"],
            "fail_count": coverage["fail_count"],
            "pass": coverage["pass"],
        },
        "gap": {
            "threshold_days": gap["threshold_days"],
            "fail_count": gap["fail_count"],
            "pass": gap["pass"],
            "histogram": gap["histogram"],
        },
        "duplicate": {
            "duplicate_count": dup["duplicate_count"],
            "pass": dup["pass"],
        },
        "outlier": {
            "big_return_count": outlier["big_return_count"],
            "volume_p99": outlier["volume_p99"],
            "volume_p99_count": outlier["volume_p99_count"],
        },
        "time_consistency": {
            "mismatch_count": timecon["mismatch_count"],
            "pass": timecon["pass"],
        },
        "gate_pass": gate_pass,
    }
    evidence_path.write_text(json.dumps(evidence, indent=2, default=str), encoding="utf-8")

    # Console summary -----------------------------------------------------
    print(f"elapsed              : {elapsed:.2f}s")
    print(f"pairs / rows         : {summary['pair_count']} / {summary['row_count']:,}")
    print(f"coverage fail count  : {coverage['fail_count']:>4}  "
          f"(threshold {coverage['threshold_pct']}%)")
    print(f"gap fail count       : {gap['fail_count']:>4}  "
          f"(threshold ≤ {gap['threshold_days']} days)")
    print(f"duplicates           : {dup['duplicate_count']:>4}")
    print(f"|return|>50% rows    : {outlier['big_return_count']:>4}  (info only)")
    print(f"volume top 1% rows   : {outlier['volume_p99_count']:>4}  (info only)")
    print(f"kst↔utc mismatch    : {timecon['mismatch_count']:>4}  "
          f"(D1 PASS #13)")
    print()
    print(f"D1 QUALITY GATE      : {'PASS' if gate_pass else 'FAIL'}")
    print(f"html                 : {HTML_OUT_PATH.relative_to(WORKTREE_ROOT)}")
    print(f"evidence             : {evidence_path.relative_to(WORKTREE_ROOT)}")
    return 0 if gate_pass else 1


if __name__ == "__main__":
    raise SystemExit(main())
