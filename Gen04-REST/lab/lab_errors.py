"""
lab_errors.py — Lab 예외 분류
==============================
DATA_ERROR:     데이터 로드/변환 실패
STRATEGY_ERROR: 전략 로직 내부 예외
REPORT_ERROR:   리포트 생성 실패
STATUS_ERROR:   status.json 읽기/쓰기 실패
LOCK_ERROR:     run lock 획득/해제 실패
"""


class LabError(Exception):
    """Base lab exception."""


class LabDataError(LabError):
    """데이터 로드, 변환, 누락 등."""


class LabStrategyError(LabError):
    """전략 generate_signals / exit_policy 내부 오류."""


class LabReportError(LabError):
    """리포트 생성, 차트, CSV 쓰기 오류."""


class LabStatusError(LabError):
    """status.json atomic write / read 오류."""


class LabLockError(LabError):
    """Run lock 획득 실패, stale lock, PID 불일치."""


class LabStatusCorrupt(LabStatusError):
    """status.json + .bak 둘 다 parse 실패."""
