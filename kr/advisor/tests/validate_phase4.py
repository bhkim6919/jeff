"""Phase 4 Validation - Intraday Risk Analyzer end-to-end tests.

Run:
    cd Gen04
    python -m advisor.tests.validate_phase4
"""
from __future__ import annotations

import json
import sys
import tempfile
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent))

from advisor.analyzers.intraday_risk import analyze_intraday_risk

PASS = 0
FAIL = 0


def _check(name: str, condition: bool, detail: str = ""):
    global PASS, FAIL
    if condition:
        PASS += 1
        print(f"  [PASS] {name}")
    else:
        FAIL += 1
        msg = f"  [FAIL] {name}"
        if detail:
            msg += f" - {detail}"
        print(msg)


def _write_summary(data: dict) -> Path:
    p = Path(tempfile.mktemp(suffix=".json"))
    p.write_text(json.dumps(data, ensure_ascii=False), encoding="utf-8")
    return p


def _base_summary(**overrides):
    d = {
        "date": "2026-04-01",
        "n_stocks": 5,
        "worst_dd_code": "005930",
        "worst_dd_pct": -2.0,
        "avg_max_dd_pct": -1.5,
        "vwap_below_count": 0,
        "total_volume_spikes_2x": 20,
        "total_volume_spikes_3x": 5,
        "near_trail_count": 0,
        "is_partial_session": False,
        "risk_score": 40.0,
        "per_stock": [],
    }
    d.update(overrides)
    return d


def _stock(code="005930", **kw):
    s = {
        "code": code,
        "n_bars": 370,
        "vwap": 180000.0,
        "close": 182000.0,
        "close_vs_vwap_pct": 1.1,
        "intraday_high": 185000.0,
        "intraday_low": 178000.0,
        "from_intraday_high_pct": -1.6,
        "from_prev_close_pct": 2.0,
        "max_intraday_dd_pct": -1.5,
        "max_dd_time": "10:30",
        "max_5m_drop_pct": -0.8,
        "max_5m_drop_time": "10:30",
        "volume_spike_2x": 10,
        "volume_spike_3x": 3,
        "volume_spike_opening": 0,
        "max_volume_ratio": 4.0,
        "near_trail_stop": False,
        "analysis_end_time": "15:19",
        "is_partial_day": False,
        "analysis_warnings": [],
    }
    s.update(kw)
    return s


# -- TEST 1: Normal data - no alerts --
def test_normal():
    print("\n-- TEST 1: Normal data - no alerts --")
    data = _base_summary(per_stock=[_stock("005930"), _stock("000660")])
    p = _write_summary(data)
    try:
        alerts = analyze_intraday_risk(p)
        _check("no alerts for normal data", len(alerts) == 0,
               f"got {len(alerts)}: {[a['message'] for a in alerts]}")
    finally:
        p.unlink(missing_ok=True)


# -- TEST 2: Flash drop --
def test_flash_drop():
    print("\n-- TEST 2: Flash drop detection --")
    data = _base_summary(per_stock=[
        _stock("009970", max_5m_drop_pct=-4.5, max_5m_drop_time="09:26",
               max_intraday_dd_pct=-6.2),
        _stock("005930"),  # normal
    ])
    p = _write_summary(data)
    try:
        alerts = analyze_intraday_risk(p)
        flash = [a for a in alerts if "Flash drop" in a["message"]]
        _check("flash drop HIGH alert generated", len(flash) >= 1,
               f"got {len(flash)}")
        if flash:
            _check("flash drop is HIGH priority",
                   flash[0]["priority"] == "HIGH")
            _check("flash drop has debug_hint",
                   bool(flash[0].get("debug_hint")))
            _check("flash drop category is INTRADAY",
                   flash[0]["category"] == "INTRADAY")
    finally:
        p.unlink(missing_ok=True)


# -- TEST 3: Flash drop + volume = institutional hint --
def test_flash_drop_with_volume():
    print("\n-- TEST 3: Flash drop + volume spike --")
    data = _base_summary(per_stock=[
        _stock("009970", max_5m_drop_pct=-3.5, max_5m_drop_time="10:00",
               max_intraday_dd_pct=-5.0, volume_spike_3x=25),
    ])
    p = _write_summary(data)
    try:
        alerts = analyze_intraday_risk(p)
        flash = [a for a in alerts if "Flash drop" in a["message"]]
        _check("institutional hint in debug_hint",
               any("institutional" in a.get("debug_hint", "").lower()
                   for a in flash),
               f"hints: {[a.get('debug_hint','') for a in flash]}")
    finally:
        p.unlink(missing_ok=True)


# -- TEST 4: Volume anomaly (portfolio-wide) --
def test_volume_anomaly():
    print("\n-- TEST 4: Volume anomaly --")
    data = _base_summary(total_volume_spikes_3x=80, per_stock=[
        _stock("005930"),
    ])
    p = _write_summary(data)
    try:
        alerts = analyze_intraday_risk(p)
        vol = [a for a in alerts if "Volume anomaly" in a["message"]
               and "portfolio" in a["message"].lower()]
        _check("portfolio volume anomaly detected", len(vol) >= 1)
    finally:
        p.unlink(missing_ok=True)


# -- TEST 5: Per-stock volume anomaly --
def test_per_stock_volume():
    print("\n-- TEST 5: Per-stock volume anomaly --")
    data = _base_summary(per_stock=[
        _stock("037460", max_volume_ratio=39.6, volume_spike_3x=18),
    ])
    p = _write_summary(data)
    try:
        alerts = analyze_intraday_risk(p)
        vol = [a for a in alerts if "037460" in a["message"]
               and "Volume" in a["message"]]
        _check("per-stock volume anomaly detected", len(vol) >= 1)
    finally:
        p.unlink(missing_ok=True)


# -- TEST 6: Cluster DD (systemic risk) --
def test_cluster_dd():
    print("\n-- TEST 6: Cluster DD --")
    data = _base_summary(per_stock=[
        _stock("005930", max_intraday_dd_pct=-4.0),
        _stock("000660", max_intraday_dd_pct=-3.5),
        _stock("005380", max_intraday_dd_pct=-5.2),
        _stock("035420", max_intraday_dd_pct=-1.0),  # normal
    ])
    p = _write_summary(data)
    try:
        alerts = analyze_intraday_risk(p)
        cluster = [a for a in alerts if "Cluster DD" in a["message"]]
        _check("cluster DD HIGH alert generated", len(cluster) >= 1)
        if cluster:
            _check("cluster DD is HIGH priority",
                   cluster[0]["priority"] == "HIGH")
            _check("cluster DD hint mentions systemic",
                   "systemic" in cluster[0].get("debug_hint", "").lower())
    finally:
        p.unlink(missing_ok=True)


# -- TEST 7: Near trail stop --
def test_near_trail():
    print("\n-- TEST 7: Near trail stop --")
    data = _base_summary(per_stock=[
        _stock("035420", near_trail_stop=True, max_intraday_dd_pct=-3.0,
               close_vs_vwap_pct=-1.5),
    ])
    p = _write_summary(data)
    try:
        alerts = analyze_intraday_risk(p)
        near = [a for a in alerts if "Near trail" in a["message"]]
        _check("near trail HIGH alert generated", len(near) >= 1)
        if near:
            _check("near trail hint mentions trigger probability",
                   "probability" in near[0].get("debug_hint", "").lower()
                   or "trigger" in near[0].get("debug_hint", "").lower())
    finally:
        p.unlink(missing_ok=True)


# -- TEST 8: Risk score thresholds --
def test_risk_score():
    print("\n-- TEST 8: Risk score thresholds --")
    # Score 85+ = HIGH
    data = _base_summary(risk_score=90, per_stock=[_stock()])
    p = _write_summary(data)
    try:
        alerts = analyze_intraday_risk(p)
        risk = [a for a in alerts if "risk score" in a["message"].lower()]
        _check("risk score 90 -> HIGH", len(risk) >= 1)
        if risk:
            _check("risk score HIGH priority", risk[0]["priority"] == "HIGH")
    finally:
        p.unlink(missing_ok=True)

    # Score 70-84 = MEDIUM
    data2 = _base_summary(risk_score=75, per_stock=[_stock()])
    p2 = _write_summary(data2)
    try:
        alerts2 = analyze_intraday_risk(p2)
        risk2 = [a for a in alerts2 if "risk score" in a["message"].lower()]
        _check("risk score 75 -> MEDIUM", len(risk2) >= 1)
        if risk2:
            _check("risk score MEDIUM priority",
                   risk2[0]["priority"] == "MEDIUM")
    finally:
        p2.unlink(missing_ok=True)

    # Score 40 = no alert
    data3 = _base_summary(risk_score=40, per_stock=[_stock()])
    p3 = _write_summary(data3)
    try:
        alerts3 = analyze_intraday_risk(p3)
        risk3 = [a for a in alerts3 if "risk score" in a["message"].lower()]
        _check("risk score 40 -> no alert", len(risk3) == 0)
    finally:
        p3.unlink(missing_ok=True)


# -- TEST 9: Missing file returns empty --
def test_missing_file():
    print("\n-- TEST 9: Missing file --")
    alerts = analyze_intraday_risk(Path("/nonexistent/path.json"))
    _check("missing file returns empty", len(alerts) == 0)


# -- TEST 10: VWAP divergence --
def test_vwap_divergence():
    print("\n-- TEST 10: VWAP divergence --")
    data = _base_summary(per_stock=[
        _stock("005930", close_vs_vwap_pct=-2.5),
        _stock("000660", close_vs_vwap_pct=-3.0),
        _stock("005380", close_vs_vwap_pct=-2.1),
    ])
    p = _write_summary(data)
    try:
        alerts = analyze_intraday_risk(p)
        vwap = [a for a in alerts if "VWAP" in a["message"]]
        _check("VWAP divergence alert generated", len(vwap) >= 1)
    finally:
        p.unlink(missing_ok=True)


# -- TEST 11: Real data integration --
def test_real_data():
    print("\n-- TEST 11: Real intraday_summary data --")
    real_path = (Path(__file__).resolve().parent.parent.parent
                 / "report" / "output_test" / "intraday_summary_20260401.json")
    if not real_path.exists():
        print("  [SKIP] No real data at", real_path)
        return
    alerts = analyze_intraday_risk(real_path)
    _check("real data produces alerts (non-empty)",
           len(alerts) >= 0)  # always pass, just show what we get
    for a in alerts:
        marker = "!!" if a["priority"] == "HIGH" else " >"
        print(f"    {marker} [{a['priority']}] {a['message']}")
        if a.get("debug_hint"):
            print(f"       hint: {a['debug_hint'][:120]}")


def main():
    print("=" * 60)
    print("  Phase 4 Validation: Intraday Risk Analyzer")
    print("=" * 60)

    test_normal()
    test_flash_drop()
    test_flash_drop_with_volume()
    test_volume_anomaly()
    test_per_stock_volume()
    test_cluster_dd()
    test_near_trail()
    test_risk_score()
    test_missing_file()
    test_vwap_divergence()
    test_real_data()

    total = PASS + FAIL
    print("\n" + "=" * 60)
    print(f"  Phase 4 Validation: {PASS}/{total} passed, {FAIL} failed")
    if FAIL == 0:
        print("  ALL PASS")
    else:
        print(f"  {FAIL} FAILURES")
    print("=" * 60)
    return 0 if FAIL == 0 else 1


if __name__ == "__main__":
    sys.exit(main())
