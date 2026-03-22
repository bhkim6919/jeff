"""
EntrySignal
===========
Gen3 핵심 데이터 계약: signals_YYYYMMDD.csv 로드 + 진입 후보 필터링.

v7.6 변경 (2026-03-16):
  - STRICT_SIGNAL_MODE: 오늘 세션 파일 + meta SUCCESS만 허용
  - validate_signal_file(): 파일 존재, 날짜, 컬럼, 행 수, NaN, meta, version 검증
  - .tmp.csv 중간 파일 사용 금지
  - 유효 시그널 없으면 신규 진입만 차단, 기존 포지션 모니터링 유지
  - expected_session_date() 기반: 자정 혼선 방지
    (calendar_date != trade_date != data_asof_date 분리)

signals.csv 포맷:
  date,ticker,qscore,entry,tp,sl,sector
  20260317,005930,0.82,72000,76000,69000,Semiconductor

런타임 흐름:
  1. expected_session_date()로 오늘 세션 trade_date 결정
  2. signals_{trade_date}.csv + .meta.json 검증
  3. qscore 내림차순 정렬
  4. 이미 보유 중인 종목 제외
  5. max_positions 슬롯 여유분만큼 선택
"""

import csv
import json
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import List, Dict, Any, Optional, Tuple

from config import Gen3Config
from trade_date_utils import expected_session_date

# 필수 컬럼 목록
REQUIRED_COLUMNS = {"ticker", "qscore", "entry", "tp", "sl"}


class SignalValidationError:
    """시그널 검증 실패 정보."""
    def __init__(self, reason: str, filepath: Optional[Path] = None):
        self.reason = reason
        self.filepath = filepath

    def __str__(self):
        name = self.filepath.name if self.filepath else "N/A"
        return f"[SignalValidation FAIL] {name}: {self.reason}"


class EntrySignal:

    def __init__(self, config: Gen3Config):
        self.config = config
        self.signals_dir = config.abs_path(config.signals_dir)
        self.last_validation_error: Optional[SignalValidationError] = None

    def load_today(self) -> List[Dict[str, Any]]:
        """
        오늘 세션에 유효한 시그널 파일 로드.

        STRICT_SIGNAL_MODE=True (기본):
          - expected_session_date() 기준 trade_date 파일만 사용
          - meta.json status=SUCCESS 필수
          - .tmp.csv 무시
          - strategy_version 일치 확인

        STRICT_SIGNAL_MODE=False:
          - 기존 방식 (가장 최근 파일 사용)

        유효한 파일이 없으면 빈 리스트 반환 -> 신규 진입 차단, 모니터링 유지.
        """
        self.last_validation_error = None

        if self.config.STRICT_SIGNAL_MODE:
            return self._load_strict()
        else:
            return self._load_legacy()

    # ── STRICT 모드 ─────────────────────────────────────────────────────────

    def _load_strict(self) -> List[Dict[str, Any]]:
        """오늘 세션 trade_date 파일 + meta 검증 후 로드."""
        session_date = expected_session_date()
        session_str = session_date.strftime("%Y%m%d")

        # 1. .tmp.csv만 있고 .csv 없으면 -> 배치 미완료
        tmp_path = self.signals_dir / f"signals_{session_str}.tmp.csv"
        csv_path = self.signals_dir / f"signals_{session_str}.csv"

        if tmp_path.exists() and not csv_path.exists():
            err = SignalValidationError(
                "tmp file only -- batch incomplete", tmp_path)
            self.last_validation_error = err
            print(f"[WARN] {err}")
            print(f"[WARN] entry skipped -- monitoring only")
            return []

        # 2. 오늘 세션 파일 존재 확인
        if not csv_path.exists():
            if self.config.ALLOW_PREV_SIGNAL:
                return self._try_prev_session(session_date)
            err = SignalValidationError(
                f"signals_{session_str}.csv not found "
                f"(session_date={session_date})", csv_path)
            self.last_validation_error = err
            print(f"[WARN] {err}")
            print(f"[WARN] entry skipped -- monitoring only")
            return []

        # 3. validate_signal_file()
        valid, error = self.validate_signal_file(csv_path, session_str)
        if not valid:
            self.last_validation_error = error
            print(f"[WARN] {error}")
            print(f"[WARN] entry skipped -- monitoring only")
            return []

        # 4. 파싱
        return self._parse_csv(csv_path)

    def _try_prev_session(self, session_date: date) -> List[Dict[str, Any]]:
        """ALLOW_PREV_SIGNAL=True 일 때 전 영업일 파일 fallback."""
        from trade_date_utils import prev_business_day
        prev_date = prev_business_day(session_date)
        prev_str = prev_date.strftime("%Y%m%d")
        csv_path = self.signals_dir / f"signals_{prev_str}.csv"

        if not csv_path.exists():
            err = SignalValidationError(
                f"prev-session fallback: signals_{prev_str}.csv not found")
            self.last_validation_error = err
            print(f"[WARN] {err}")
            print(f"[WARN] entry skipped -- monitoring only")
            return []

        valid, error = self.validate_signal_file(csv_path, prev_str)
        if not valid:
            self.last_validation_error = error
            print(f"[WARN] {error}")
            print(f"[WARN] entry skipped -- monitoring only")
            return []

        print(f"[EntrySignal] WARNING: ALLOW_PREV_SIGNAL -- "
              f"prev session file: {csv_path.name}")
        return self._parse_csv(csv_path)

    # ── 검증 ────────────────────────────────────────────────────────────────

    def validate_signal_file(
        self, filepath: Path, expected_trade_date: str
    ) -> Tuple[bool, Optional[SignalValidationError]]:
        """
        시그널 파일 유효성 검증.

        검증 항목:
          1. 파일 존재
          2. 파일명 날짜 = expected_trade_date
          3. 필수 컬럼 존재
          4. 행 수 >= SIGNAL_MIN_ROWS
          5. NaN 과다 여부 (qscore 기준)
          6. meta.json 존재 + status=SUCCESS
          7. meta trade_date 일치
          8. strategy_version 일치

        반환: (True, None) or (False, SignalValidationError)
        """
        # 1. 파일 존재
        if not filepath.exists():
            return False, SignalValidationError("file not found", filepath)

        # 2. 파일명 날짜 검증
        stem = filepath.stem  # signals_20260317
        file_date = stem.replace("signals_", "")
        if file_date != expected_trade_date:
            return False, SignalValidationError(
                f"date mismatch: file={file_date}, expected={expected_trade_date}",
                filepath)

        # 3~5. CSV 내용 검증
        try:
            with open(filepath, newline="", encoding="utf-8") as f:
                reader = csv.DictReader(f)
                headers = set(reader.fieldnames or [])

                # 3. 필수 컬럼
                missing = REQUIRED_COLUMNS - headers
                if missing:
                    return False, SignalValidationError(
                        f"missing columns: {missing}", filepath)

                # 4+5. 행 수 + NaN 카운트
                rows = list(reader)
                total = len(rows)
                if total < self.config.SIGNAL_MIN_ROWS:
                    return False, SignalValidationError(
                        f"too few rows: {total} < {self.config.SIGNAL_MIN_ROWS}",
                        filepath)

                nan_count = sum(
                    1 for r in rows
                    if not r.get("qscore") or r.get("qscore", "").strip() == ""
                )
                nan_pct = nan_count / total if total > 0 else 0
                if nan_pct > self.config.SIGNAL_NAN_MAX_PCT:
                    return False, SignalValidationError(
                        f"NaN excess: {nan_pct:.0%} > "
                        f"{self.config.SIGNAL_NAN_MAX_PCT:.0%} "
                        f"({nan_count}/{total} rows)", filepath)

        except Exception as e:
            return False, SignalValidationError(
                f"CSV parse error: {e}", filepath)

        # 6. meta.json 검증
        meta_path = filepath.parent / f"{filepath.stem}.meta.json"
        if not meta_path.exists():
            return False, SignalValidationError(
                f"meta file not found: {meta_path.name}", filepath)

        try:
            with open(meta_path, encoding="utf-8") as f:
                meta = json.load(f)
        except Exception as e:
            return False, SignalValidationError(
                f"meta parse error: {e}", filepath)

        # status=SUCCESS
        if meta.get("status") != "SUCCESS":
            return False, SignalValidationError(
                f"meta status={meta.get('status')} (expected SUCCESS)",
                filepath)

        # 7. trade_date 일치
        if meta.get("trade_date") != expected_trade_date:
            return False, SignalValidationError(
                f"meta trade_date={meta.get('trade_date')} "
                f"!= expected {expected_trade_date}", filepath)

        # 8. strategy_version 일치
        expected_ver = self.config.STRATEGY_VERSION
        meta_ver = meta.get("strategy_version", "")
        if meta_ver != expected_ver:
            return False, SignalValidationError(
                f"strategy_version mismatch: "
                f"meta={meta_ver}, config={expected_ver}", filepath)

        print(f"[EntrySignal] validate OK: {filepath.name} "
              f"(rows={total}, meta=SUCCESS, ver={meta_ver}, "
              f"trade_date={expected_trade_date})")
        return True, None

    # ── 레거시 모드 (하위 호환) ──────────────────────────────────────────────

    def _load_legacy(self) -> List[Dict[str, Any]]:
        """기존 방식: 오늘 -> 어제 fallback."""
        today = date.today()
        filepath = None
        max_age = 1

        for delta in range(max_age + 1):
            candidate = self.signals_dir / (
                f"signals_"
                f"{(today - timedelta(days=delta)).strftime('%Y%m%d')}.csv"
            )
            if candidate.exists():
                filepath = candidate
                if delta > 0:
                    print(f"[EntrySignal] WARNING: "
                          f"{delta}d old file: {filepath.name}")
                break

        if filepath is None:
            print(f"[EntrySignal] signals_"
                  f"{today.strftime('%Y%m%d')}.csv not found "
                  f"-- entry skipped")
            return []

        return self._parse_csv(filepath)

    # ── CSV 파싱 ─────────────────────────────────────────────────────────────

    def _parse_csv(self, filepath: Path) -> List[Dict[str, Any]]:
        """signals CSV -> dict 리스트. qscore 내림차순 정렬."""
        today = date.today()
        signals = []
        with open(filepath, newline="", encoding="utf-8") as f:
            reader = csv.DictReader(f)
            for row in reader:
                try:
                    rs_c = (float(row["rs_composite"])
                            if row.get("rs_composite")
                            else float(row["qscore"]))
                    signals.append({
                        "code":         row["ticker"],
                        "qscore":       float(row["qscore"]),
                        "entry":        int(row["entry"]),
                        "tp":           int(row["tp"]),
                        "sl":           int(row["sl"]),
                        "sector":       row.get("sector", "기타"),
                        "stage":        row.get("stage", "B"),
                        "date":         row.get("date", today),
                        # v7 컬럼
                        "rs_composite": rs_c,
                        "signal_entry": int(row.get("signal_entry", 1)),
                        "is_52w_high":  int(row.get("is_52w_high", 0)),
                        "above_ma20":   int(row.get("above_ma20", 0)),
                        "rs20_rank":    float(row.get("rs20_rank") or 0),
                        "rs60_rank":    float(row.get("rs60_rank") or 0),
                        "rs120_rank":   float(row.get("rs120_rank") or 0),
                        "atr20":        float(row.get("atr20") or 0),
                    })
                except (KeyError, ValueError) as e:
                    print(f"[EntrySignal] row parse error ({row}): {e}")

        signals.sort(key=lambda x: x["qscore"], reverse=True)
        print(f"[EntrySignal] {filepath.name} loaded -- {len(signals)} signals")
        return signals

    # ── 후보 필터링 ─────────────────────────────────────────────────────────

    def filter_candidates(
        self,
        signals: List[Dict[str, Any]],
        portfolio,
        max_new: int,
    ) -> List[Dict[str, Any]]:
        """이미 보유 중인 종목 제외 후 상위 max_new개 반환."""
        result = []
        for sig in signals:
            code = sig["code"]
            if portfolio.has_position(code):
                continue
            result.append(sig)
            if len(result) >= max_new:
                break
        print(f"[EntrySignal] entry candidates: {len(result)} (held excluded)")
        return result

    def latest_signal_file(self) -> Optional[Path]:
        """signals_dir 에서 가장 최신 signals_*.csv 반환."""
        files = sorted(self.signals_dir.glob("signals_*.csv"), reverse=True)
        return files[0] if files else None
