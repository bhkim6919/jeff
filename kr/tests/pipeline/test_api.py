# -*- coding: utf-8 -*-
"""Unit tests for pipeline.api (FastAPI router)."""
from __future__ import annotations

import sys
from datetime import date
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent))

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

from pipeline import api as pipeline_api
from pipeline import tray_integration
from pipeline.schema import STATUS_DONE, STATUS_FAILED, STATUS_SKIPPED
from pipeline.state import PipelineState


@pytest.fixture
def app_with_router(tmp_path, monkeypatch):
    """Mount the pipeline router against a real temp data_dir."""
    monkeypatch.setattr(
        pipeline_api, "default_data_dir", lambda: tmp_path,
    )
    monkeypatch.setattr(
        pipeline_api, "detect_mode", lambda: "paper_forward",
    )
    # Reset the holder between tests so last_summary doesn't leak.
    tray_integration.HOLDER._orch = None
    tray_integration.HOLDER._bootstrap_recorded = False
    tray_integration.HOLDER._last_tick_summary = None

    app = FastAPI()
    app.include_router(pipeline_api.router)
    return app


@pytest.fixture
def client(app_with_router):
    return TestClient(app_with_router)


# ---------- /api/pipeline/status ----------

def test_status_disabled_by_default(client, monkeypatch):
    monkeypatch.delenv(tray_integration.ENV_TOGGLE, raising=False)
    r = client.get("/api/pipeline/status")
    assert r.status_code == 200
    body = r.json()
    assert body["enabled"] is False
    assert body["primary"] is False
    assert body["trade_date"] is not None  # state was created fresh
    assert body["state"] is not None
    assert "history" not in body  # not requested


def test_status_enabled_shadow(client, monkeypatch):
    monkeypatch.setenv(tray_integration.ENV_TOGGLE, "1")
    r = client.get("/api/pipeline/status")
    body = r.json()
    assert body["enabled"] is True
    assert body["primary"] is False


def test_status_primary(client, monkeypatch):
    monkeypatch.setenv(tray_integration.ENV_TOGGLE, "2")
    r = client.get("/api/pipeline/status")
    body = r.json()
    assert body["enabled"] is True
    assert body["primary"] is True


def test_status_with_history_never_raises(client):
    # PG is not available in test env → history falls back to [].
    r = client.get("/api/pipeline/status?include_history=true")
    assert r.status_code == 200
    body = r.json()
    assert "history" in body
    assert isinstance(body["history"], list)


def test_status_reflects_recorded_steps(client, tmp_path):
    # Pre-seed today's state with a DONE step.
    state = PipelineState.load_or_create_today(
        data_dir=tmp_path, mode="paper_forward",
    )
    state.mark_done("batch", details={"tickers": 123})
    state.save()

    r = client.get("/api/pipeline/status")
    body = r.json()
    assert body["state"]["steps"]["batch"]["status"] == STATUS_DONE
    assert body["state"]["steps"]["batch"]["details"]["tickers"] == 123


# ---------- /api/pipeline/record_step ----------

def test_record_step_done(client, tmp_path):
    r = client.post("/api/pipeline/record_step", json={
        "step_name": "lab_eod_kr",
        "status": "DONE",
        "details": {"strategies": 9},
    })
    assert r.status_code == 200
    body = r.json()
    assert body["ok"] is True
    assert body["step_name"] == "lab_eod_kr"

    # Verify state file was actually written.
    state = PipelineState.load_or_create_today(
        data_dir=tmp_path, mode="paper_forward",
    )
    assert state.step("lab_eod_kr").status == STATUS_DONE
    assert state.step("lab_eod_kr").details["strategies"] == 9


def test_record_step_failed(client, tmp_path):
    r = client.post("/api/pipeline/record_step", json={
        "step_name": "lab_eod_us",
        "status": "FAILED",
        "error": "alpaca_rate_limit",
    })
    assert r.status_code == 200
    assert r.json()["ok"] is True

    state = PipelineState.load_or_create_today(
        data_dir=tmp_path, mode="paper_forward",
    )
    assert state.step("lab_eod_us").status == STATUS_FAILED
    assert "alpaca_rate_limit" in (state.step("lab_eod_us").last_error or "")


def test_record_step_skipped(client, tmp_path):
    r = client.post("/api/pipeline/record_step", json={
        "step_name": "gate_observer",
        "status": "SKIPPED",
        "details": {"reason": "module_not_found"},
    })
    assert r.status_code == 200
    assert r.json()["ok"] is True

    state = PipelineState.load_or_create_today(
        data_dir=tmp_path, mode="paper_forward",
    )
    assert state.step("gate_observer").status == STATUS_SKIPPED


def test_record_step_rejects_invalid_status(client):
    r = client.post("/api/pipeline/record_step", json={
        "step_name": "batch",
        "status": "BOGUS",
    })
    assert r.status_code == 200
    body = r.json()
    assert body["ok"] is False
    assert "invalid_status" in body["reason"]


def test_record_step_explicit_trade_date(client, tmp_path):
    r = client.post("/api/pipeline/record_step", json={
        "step_name": "batch",
        "status": "DONE",
        "trade_date": "2026-04-20",  # yesterday — allowed for late writes
    })
    assert r.status_code == 200
    body = r.json()
    assert body["ok"] is True
    assert body["trade_date"] == "2026-04-20"

    # load_date is read-only; use it to confirm file exists on disk.
    loaded = PipelineState.load_date(date(2026, 4, 20), data_dir=tmp_path)
    assert loaded is not None
    assert loaded.step("batch").status == STATUS_DONE


def test_record_step_bad_date_falls_back_to_today(client, tmp_path):
    r = client.post("/api/pipeline/record_step", json={
        "step_name": "batch",
        "status": "DONE",
        "trade_date": "not-a-date",
    })
    assert r.status_code == 200
    body = r.json()
    # _parse_trade_date returns None on malformed → defaults to today.
    assert body["ok"] is True
    assert body["trade_date"] != "not-a-date"
