# -*- coding: utf-8 -*-
"""v001: 기존 PostgreSQL 테이블 baseline 등록."""
VERSION = 1
DESCRIPTION = "Baseline: record existing tables as version 1"


def up(conn):
    """기존 테이블이 이미 존재하므로 버전만 기록."""
    pass  # _schema_versions에 이 버전이 기록되는 것 자체가 baseline
