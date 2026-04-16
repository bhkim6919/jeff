# -*- coding: utf-8 -*-
"""
run_id.py — 실행 단위 ID 생성기
================================
모든 write row에 포함되는 추적 ID.

[ID 체계]
- eod_run_id:    EOD 배치 실행 단위
- snapshot_id:   장중 스냅샷 단위
- ingest_run_id: CSV 적재 단위
"""
from __future__ import annotations

import hashlib
from datetime import datetime, timezone


def make_eod_run_id(
    trade_date: str,
    source: str,
    data_last_date: str,
    universe_count: int,
    matrix_hash: str,
) -> str:
    """
    EOD 배치 실행 ID. deterministic: 동일 입력 → 동일 출력.

    Args:
        trade_date: 거래일 (YYYY-MM-DD)
        source: 데이터 소스 ('db', 'csv', 'alpaca')
        data_last_date: 데이터 마지막 날짜
        universe_count: 유니버스 종목 수
        matrix_hash: 데이터 매트릭스 해시 (앞 8자리)
    """
    return f"{trade_date}:{source}:{data_last_date}:{universe_count}:{matrix_hash[:8]}"


def make_snapshot_id(
    market_date: str,
    epoch: int,
    seq: int = 0,
) -> str:
    """
    장중 스냅샷 ID. epoch 기반으로 유일.

    Args:
        market_date: 시장 날짜 (YYYY-MM-DD)
        epoch: Unix epoch seconds
        seq: 동일 epoch 내 순번 (기본 0)
    """
    return f"{market_date}:{epoch}:{seq}"


def make_ingest_run_id(
    trade_date: str,
    dataset: str,
    file_hash: str,
) -> str:
    """
    CSV 적재 ID. deterministic: 같은 파일 → 같은 ID.

    Args:
        trade_date: 거래일 (YYYY-MM-DD)
        dataset: 데이터셋 ('intraday', 'micro', 'swing')
        file_hash: CSV 파일 내용 해시 (앞 16자리)
    """
    return f"{trade_date}:{dataset}:{file_hash[:16]}"


def compute_file_hash(file_path: str) -> str:
    """파일 내용의 SHA-256 해시 (앞 16자리)."""
    h = hashlib.sha256()
    with open(file_path, "rb") as f:
        for chunk in iter(lambda: f.read(8192), b""):
            h.update(chunk)
    return h.hexdigest()[:16]


def now_utc() -> datetime:
    """현재 UTC 시각 (timezone-aware). run_ts 생성 기준."""
    return datetime.now(timezone.utc)
