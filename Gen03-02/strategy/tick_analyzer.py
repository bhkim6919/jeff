"""
TickAnalyzer (v7.1)
===================
체결강도(FID 228) 기반 매수 타이밍 필터.

09:00~10:00 동안 시그널 종목의 체결강도를 수집하고,
10:00에 필터링된 시그널만 매수에 사용한다.

- 체결강도 > TICK_STRONG_THRESH (120): 매수세 강함 → 진입
- 체결강도 < TICK_WEAK_THRESH (80):    매도세 강함 → DROP
- 80~120: 중립 → config.TICK_NEUTRAL_ACTION 에 따라 결정

tick_source 상태:
  LIVE         — 실시간 FID 228 데이터 기반
  MOCK         — --mock 모드 랜덤 생성
  MOCK_FALLBACK — LIVE 모드에서 0건 수신 시 모의서버 폴백
  NEUTRAL_PASS  — 실서버 0건 수신 시 전 종목 NEUTRAL 통과
"""

from __future__ import annotations

import numpy as np
from typing import Any, Dict, List, Optional


class TickAnalyzer:

    def __init__(self, provider, config, signals: List[Dict[str, Any]],
                 mock: bool = False):
        self._provider = provider
        self._config   = config
        self._signals  = signals
        self._mock     = mock

        # {code: [value, value, ...]} — 관측 기간 동안 축적
        self._readings: Dict[str, List[float]] = {}
        # {code: avg} — stop_observation 후 계산
        self._avg_strength: Dict[str, float] = {}

        self._codes = [s["code"] for s in signals]
        self._observing = False

        # 데이터 소스 추적 (LIVE | MOCK | MOCK_FALLBACK | NEUTRAL_PASS)
        self._tick_source: str = "MOCK" if mock else "LIVE"

    @property
    def tick_source(self) -> str:
        return self._tick_source

    # ── 관측 시작 ─────────────────────────────────────────────────────

    def start_observation(self) -> None:
        """
        LIVE: SetRealReg 으로 체결강도 실시간 수신 등록.
        MOCK: 즉시 랜덤 데이터 생성 (별도 타이머 불필요).
        """
        if not self._codes:
            return

        self._observing = True

        # 수신 버퍼 초기화 (on_tick_strength 에서 code in _readings 체크)
        for code in self._codes:
            self._readings[code] = []

        if self._mock:
            self._generate_mock_data()
            print(f"[TickAnalyzer] 관측 시작 (mock): {len(self._codes)}개 종목")
        else:
            # LIVE: KiwoomProvider 에 콜백 등록 + SetRealReg
            self._provider.set_real_data_callback(self.on_tick_strength)
            self._provider.register_real(self._codes, fids="228")
            print(f"[TickAnalyzer] 관측 시작 (live): {len(self._codes)}개 종목 "
                  f"SetRealReg buffers_init={len(self._codes)}")

    # ── 체결강도 수신 콜백 ────────────────────────────────────────────

    def on_tick_strength(self, code: str, value: float) -> None:
        """OnReceiveRealData 콜백에서 호출. 체결강도 값 축적."""
        if code in self._readings:
            self._readings[code].append(value)

    # ── 관측 종료 ─────────────────────────────────────────────────────

    def stop_observation(self) -> None:
        """관측 종료 + 평균값 계산."""
        self._observing = False

        if not self._mock and hasattr(self._provider, 'unregister_real'):
            self._provider.unregister_real()
            self._provider.set_real_data_callback(None)

        # LIVE 모드 0건 감지 → 모의서버/실서버 분기 처리
        total_readings = sum(len(self._readings.get(c, [])) for c in self._codes)
        if not self._mock and self._codes and total_readings == 0:
            is_mock_server = self._detect_mock_server()
            if is_mock_server:
                # 모의서버: mock 폴백 (NEUTRAL 근처 보수적 분포)
                self._tick_source = "MOCK_FALLBACK"
                self._generate_conservative_mock_data()
                print(f"[TickAnalyzer] WARNING: LIVE 0건 수신 (모의서버 추정) "
                      f"→ filter_mode=MOCK_FALLBACK (보수적 NEUTRAL 분포)")
            else:
                # 실서버: 필터 비활성화 — 전 종목 NEUTRAL 고정 통과
                self._tick_source = "NEUTRAL_PASS"
                for code in self._codes:
                    self._readings[code] = [100.0]  # NEUTRAL 고정
                print(f"[TickAnalyzer] WARNING: LIVE 0건 수신 (실서버) "
                      f"→ filter_mode=NEUTRAL_PASS (체결강도 필터 비활성)")

        # 평균 계산
        for code in self._codes:
            readings = self._readings.get(code, [])
            if readings:
                self._avg_strength[code] = float(np.mean(readings))
            else:
                self._avg_strength[code] = 100.0   # 데이터 없으면 중립 (100)

        # 결과 출력 (tick_source 태그 포함)
        print(f"[TickAnalyzer] tick_source={self._tick_source} "
              f"total_readings={total_readings}")
        for code, avg in self._avg_strength.items():
            n = len(self._readings.get(code, []))
            label = self._classify(avg)
            print(f"[TickAnalyzer] {code} avg={avg:.1f} ({n}건) -> {label}")

    # ── 시그널 필터링 ─────────────────────────────────────────────────

    def filter_signals(self, signals: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """
        체결강도 기반 필터:
          avg > STRONG_THRESH → 유지 (STRONG)
          avg < WEAK_THRESH   → DROP
          사이                → TICK_NEUTRAL_ACTION에 따라

        NEUTRAL_PASS 모드: 필터 비활성 (전 종목 통과)
        """
        # NEUTRAL_PASS: 필터 비활성화 — 전 종목 통과
        if self._tick_source == "NEUTRAL_PASS":
            for sig in signals:
                sig["tick_label"] = "NEUTRAL_PASS"
                sig["tick_source"] = self._tick_source
            print(f"[TickAnalyzer] filter_mode=NEUTRAL_PASS — "
                  f"전 종목 통과 ({len(signals)}건)")
            return signals

        strong_thresh  = self._config.TICK_STRONG_THRESH
        weak_thresh    = self._config.TICK_WEAK_THRESH
        neutral_action = getattr(self._config, 'TICK_NEUTRAL_ACTION', "ENTER")

        filtered = []
        dropped  = 0

        for sig in signals:
            code = sig["code"]
            avg  = self._avg_strength.get(code, 100.0)
            sig["tick_source"] = self._tick_source

            if avg >= strong_thresh:
                sig["tick_label"] = "STRONG"
                filtered.append(sig)
            elif avg <= weak_thresh:
                sig["tick_label"] = "WEAK"
                dropped += 1
                # DROP — 시그널 제외
            else:
                # 중립 구간
                if neutral_action == "ENTER":
                    sig["tick_label"] = "NEUTRAL"
                    filtered.append(sig)
                else:
                    sig["tick_label"] = "NEUTRAL_SKIP"
                    dropped += 1

        if dropped > 0:
            print(f"[TickAnalyzer] DROP {dropped}건 (체결강도 < {weak_thresh})")

        return filtered

    # ── 요약 ──────────────────────────────────────────────────────────

    def get_summary(self) -> Dict[str, Any]:
        """관측 결과 요약."""
        strong = sum(1 for v in self._avg_strength.values()
                     if v >= self._config.TICK_STRONG_THRESH)
        weak   = sum(1 for v in self._avg_strength.values()
                     if v <= self._config.TICK_WEAK_THRESH)
        neutral = len(self._avg_strength) - strong - weak

        return {
            "total_codes":  len(self._codes),
            "strong":       strong,
            "weak":         weak,
            "neutral":      neutral,
            "tick_source":  self._tick_source,
            "avg_readings": {code: len(self._readings.get(code, []))
                             for code in self._codes},
        }

    # ── 내부 헬퍼 ─────────────────────────────────────────────────────

    def _classify(self, avg: float) -> str:
        if avg >= self._config.TICK_STRONG_THRESH:
            return "STRONG"
        elif avg <= self._config.TICK_WEAK_THRESH:
            return "WEAK (DROP)"
        else:
            return "NEUTRAL"

    def _detect_mock_server(self) -> bool:
        """모의서버 여부 판단 (계좌번호 패턴 기반)."""
        try:
            account = self._provider.get_account_no()
            # Kiwoom 모의투자 계좌: 8로 시작 (실계좌: 보통 다른 패턴)
            if account and account.startswith("8"):
                return True
        except Exception:
            pass
        return True  # 판단 불가 시 모의서버로 가정 (보수적)

    def _generate_mock_data(self) -> None:
        """Mock: 종목별 랜덤 체결강도 생성 (50~150 범위, 20회)."""
        for code in self._codes:
            # 종목별로 고유한 bias 부여 (일부는 강매수, 일부는 강매도)
            bias = (hash(code) % 100) - 50   # -50 ~ +49
            center = 100 + bias * 0.5         # 75 ~ 124.5
            readings = np.random.normal(center, 15, size=20).tolist()
            readings = [max(30, min(180, v)) for v in readings]
            self._readings[code] = readings

    def _generate_conservative_mock_data(self) -> None:
        """모의서버 폴백용: NEUTRAL 근처 보수적 분포 (95~105)."""
        for code in self._codes:
            readings = np.random.normal(100, 3, size=20).tolist()
            readings = [max(90, min(110, v)) for v in readings]
            self._readings[code] = readings

    def feed_mock_tick(self) -> None:
        """Mock 모드에서 추가 데이터 생성 (호출당 1회씩)."""
        if not self._mock:
            return
        for code in self._codes:
            bias = (hash(code) % 100) - 50
            center = 100 + bias * 0.5
            value = float(np.clip(np.random.normal(center, 15), 30, 180))
            self._readings.setdefault(code, []).append(value)
