"""
bt_engine.py
============
Q-TRON 백테스트 엔진.

동작 방식:
  1. 영업일 순서로 날짜 루프
  2. 매일: CsvProvider.set_date() → Stage1~5 파이프라인 실행
  3. 체결 시 슬리피지 + 수수료 적용
  4. 일별 자산 곡선, 거래 기록 수집
  5. 결과를 BtResult 객체로 반환 (bt_reporter가 사용)

슬리피지/수수료 모델:
  - 매수: 체결가 = 다음날 시가 × (1 + slippage)
  - 매도: 체결가 = 다음날 시가 × (1 - slippage)
  - 수수료: 매수/매도 각각 commission (편도)
  - 기본값: slippage=0.002 (0.2%), commission=0.00015 (0.015% 증권사 + 세금 별도)

사용:
  from backtest.bt_engine import BacktestEngine, BtConfig

  cfg = BtConfig(start="20220101", end="20231231", initial_cash=10_000_000)
  engine = BacktestEngine(provider, pipeline_factory, cfg)
  result = engine.run()
"""

from __future__ import annotations

import traceback
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
from typing import Callable, Dict, List, Optional

import pandas as pd

from backtest.csv_provider import CsvProvider


# ── 설정 ─────────────────────────────────────────────────────────────────────
@dataclass
class BtConfig:
    start:        str   = "20220101"
    end:          str   = "20231231"
    initial_cash: float = 10_000_000.0
    slippage:     float = 0.002        # 0.2% (편도)
    commission:   float = 0.00015      # 0.015% 편도 (증권사 온라인 기준)
    sell_tax:     float = 0.002        # 증권거래세 0.2% (매도 시)
    max_positions:int   = 5            # 최대 보유 종목 수
    # Train / Test 구분 (optimizer에서 사용)
    train_end:    str   = "20231231"
    test_start:   str   = "20240101"


# ── 포지션 ───────────────────────────────────────────────────────────────────
@dataclass
class BtPosition:
    code:       str
    quantity:   int
    avg_price:  float
    tp:         float = 0.0
    sl:         float = 0.0
    entry_date: str   = ""


# ── 거래 기록 ─────────────────────────────────────────────────────────────────
@dataclass
class BtTrade:
    date:       str
    code:       str
    side:       str    # BUY | SELL
    quantity:   int
    price:      float  # 실제 체결가 (슬리피지 적용 후)
    raw_price:  float  # 신호가 (슬리피지 적용 전)
    commission: float
    pnl:        float  = 0.0   # SELL 시만 유효
    close_type: str    = ""    # TP | SL | MA20 | EOD


# ── 결과 ────────────────────────────────────────────────────────────────────
@dataclass
class BtResult:
    config:          BtConfig
    equity_curve:    pd.DataFrame   # date, equity
    trades:          pd.DataFrame   # 전체 거래 내역
    monthly_returns: pd.DataFrame   # year, month, return
    metrics:         dict           # 주요 지표


# ── 엔진 ────────────────────────────────────────────────────────────────────
class BacktestEngine:
    """
    Q-TRON 전략 파이프라인을 날짜별로 재실행하는 백테스트 엔진.

    pipeline_factory:
      (provider, config) → 실행 가능한 객체 (run() 메서드 포함)
      실제로는 QTronConfig를 파라미터화한 버전을 주입.
    """

    def __init__(
        self,
        provider:         CsvProvider,
        pipeline_factory: Callable,
        bt_config:        BtConfig,
        qtron_config,                   # QTronConfig 인스턴스
    ):
        self.provider  = provider
        self.factory   = pipeline_factory
        self.bt_cfg    = bt_config
        self.qt_cfg    = qtron_config

        # 포트폴리오 상태
        self.cash:      float              = bt_config.initial_cash
        self.positions: Dict[str, BtPosition] = {}

        # 기록
        self.equity_records: List[dict]  = []
        self.trade_records:  List[BtTrade] = []

    # ── 공개 API ──────────────────────────────────────────────────────────────

    def run(self) -> BtResult:
        """백테스트 실행 후 BtResult 반환."""
        bdays = self._business_days()
        print(f"[BtEngine] 시작: {self.bt_cfg.start} ~ {self.bt_cfg.end} ({len(bdays)}일)")
        print(f"[BtEngine] 초기 자본: {self.bt_cfg.initial_cash:,.0f}원 | "
              f"슬리피지: {self.bt_cfg.slippage*100:.2f}% | "
              f"수수료: {self.bt_cfg.commission*100:.4f}%")

        for i, date_str in enumerate(bdays):
            self.provider.set_date(date_str)

            # 전일 TP/SL/MA20 청산 체크 (장 시작 전)
            self._check_exits(date_str)

            # 파이프라인 실행 (신규 진입 신호 생성)
            signals = self._run_pipeline_signals(date_str)

            # 신규 진입 체결 (당일 종가 → 실전은 다음날 시가, 여기선 단순화)
            self._execute_entries(date_str, signals)

            # 자산 평가
            equity = self._calc_equity(date_str)
            self.equity_records.append({"date": date_str, "equity": equity})

            # 진행 표시 (20일마다)
            if i % 20 == 0:
                print(f"  [{date_str}] 자산: {equity:,.0f}원 | "
                      f"보유: {len(self.positions)}개 | "
                      f"현금: {self.cash:,.0f}원")

        # 최종 강제 청산
        self._force_close_all(bdays[-1])

        return self._build_result()

    # ── 청산 로직 ─────────────────────────────────────────────────────────────

    def _check_exits(self, date_str: str):
        """TP / SL / MA20 청산 조건 체크."""
        to_sell = []
        for code, pos in self.positions.items():
            price = self.provider.get_current_price(code)
            if price <= 0:
                continue

            close_type = None

            # SL 우선
            if pos.sl > 0 and price <= pos.sl:
                close_type = "SL"
            # MA20 이탈
            elif self._is_below_ma20(code, price):
                close_type = "MA20"
            # TP
            elif pos.tp > 0 and price >= pos.tp:
                close_type = "TP"

            if close_type:
                to_sell.append((code, pos, price, close_type))

        for code, pos, price, close_type in to_sell:
            self._sell(date_str, code, pos, price, close_type)

    def _is_below_ma20(self, code: str, current: float) -> bool:
        try:
            df = self.provider.get_stock_ohlcv(code, days=25)
            if df is None or len(df) < 20:
                return False
            ma20 = float(df["close"].rolling(20).mean().iloc[-1])
            return current < ma20
        except Exception:
            return False

    def _force_close_all(self, date_str: str):
        """백테스트 종료 시 잔존 포지션 전량 청산."""
        for code, pos in list(self.positions.items()):
            price = self.provider.get_current_price(code)
            if price <= 0:
                price = pos.avg_price
            self._sell(date_str, code, pos, price, "EOD")

    # ── 신규 진입 ─────────────────────────────────────────────────────────────

    def _run_pipeline_signals(self, date_str: str) -> List[dict]:
        """
        파이프라인을 실행해 신규 진입 신호 목록을 반환.
        반환: [{"code": ..., "tp": ..., "sl": ..., "size": ...}, ...]
        """
        try:
            pipeline = self.factory(self.provider, self.qt_cfg)
            result   = pipeline.run()

            # pipeline.run()이 positioned 목록을 직접 반환하도록
            # bt_pipeline.py에서 래핑 예정 → 여기서는 result["positioned"] 참조
            if isinstance(result, dict) and "positioned" in result:
                return result["positioned"]

            # 기존 파이프라인 호환: positioned 없으면 빈 신호
            return []

        except Exception as e:
            print(f"  [BtEngine] {date_str} 파이프라인 오류: {type(e).__name__}: {e}")
            return []

    def _execute_entries(self, date_str: str, signals: List[dict]):
        """신규 진입 신호 체결 (슬리피지/수수료 적용)."""
        for sig in signals:
            code = sig.get("code", "")
            if not code:
                continue

            # 이미 보유 중이면 스킵
            if code in self.positions:
                continue

            # 최대 포지션 수 제한
            if len(self.positions) >= self.bt_cfg.max_positions:
                break

            raw_price = self.provider.get_current_price(code)
            if raw_price <= 0:
                continue

            # 슬리피지 적용 (매수: 불리하게)
            exec_price = raw_price * (1 + self.bt_cfg.slippage)

            # 투자금액 결정 (sig에 size 있으면 사용, 없으면 균등 배분)
            invest_amt = sig.get("size", self.cash / max(1, self.bt_cfg.max_positions - len(self.positions)))
            invest_amt = min(invest_amt, self.cash * 0.95)  # 현금의 95% 상한

            if invest_amt < exec_price:
                continue

            qty = int(invest_amt // exec_price)
            if qty <= 0:
                continue

            total_cost  = qty * exec_price
            commission  = total_cost * self.bt_cfg.commission
            total_debit = total_cost + commission

            if total_debit > self.cash:
                qty        = int((self.cash * 0.95) // (exec_price * (1 + self.bt_cfg.commission)))
                total_cost = qty * exec_price
                commission = total_cost * self.bt_cfg.commission
                total_debit= total_cost + commission

            if qty <= 0 or total_debit > self.cash:
                continue

            self.cash -= total_debit
            self.positions[code] = BtPosition(
                code       = code,
                quantity   = qty,
                avg_price  = exec_price,
                tp         = float(sig.get("tp", 0)),
                sl         = float(sig.get("sl", 0)),
                entry_date = date_str,
            )
            self.trade_records.append(BtTrade(
                date       = date_str,
                code       = code,
                side       = "BUY",
                quantity   = qty,
                price      = exec_price,
                raw_price  = raw_price,
                commission = commission,
            ))

    # ── 매도 실행 ─────────────────────────────────────────────────────────────

    def _sell(self, date_str: str, code: str, pos: BtPosition,
              raw_price: float, close_type: str):
        """매도 체결 + 손익 계산."""
        # 슬리피지 적용 (매도: 불리하게)
        exec_price = raw_price * (1 - self.bt_cfg.slippage)
        proceeds   = pos.quantity * exec_price
        commission = proceeds * self.bt_cfg.commission
        sell_tax   = proceeds * self.bt_cfg.sell_tax
        net_proceeds = proceeds - commission - sell_tax

        cost = pos.quantity * pos.avg_price
        pnl  = net_proceeds - cost

        self.cash += net_proceeds
        del self.positions[code]

        self.trade_records.append(BtTrade(
            date       = date_str,
            code       = code,
            side       = "SELL",
            quantity   = pos.quantity,
            price      = exec_price,
            raw_price  = raw_price,
            commission = commission + sell_tax,
            pnl        = pnl,
            close_type = close_type,
        ))

    # ── 자산 평가 ─────────────────────────────────────────────────────────────

    def _calc_equity(self, date_str: str) -> float:
        """현금 + 보유 포지션 평가액 합산."""
        pos_value = 0.0
        for code, pos in self.positions.items():
            price = self.provider.get_current_price(code)
            if price <= 0:
                price = pos.avg_price
            pos_value += pos.quantity * price
        return self.cash + pos_value

    # ── 결과 생성 ─────────────────────────────────────────────────────────────

    def _build_result(self) -> BtResult:
        """BtResult 객체 조립 + 핵심 지표 계산."""
        eq_df = pd.DataFrame(self.equity_records)
        eq_df["date"] = pd.to_datetime(eq_df["date"], format="%Y%m%d")
        eq_df = eq_df.set_index("date").sort_index()

        tr_df = pd.DataFrame([vars(t) for t in self.trade_records]) if self.trade_records \
            else pd.DataFrame(columns=["date","code","side","quantity","price",
                                       "raw_price","commission","pnl","close_type"])

        metrics = self._calc_metrics(eq_df, tr_df)
        monthly = self._calc_monthly_returns(eq_df)

        return BtResult(
            config          = self.bt_cfg,
            equity_curve    = eq_df.reset_index(),
            trades          = tr_df,
            monthly_returns = monthly,
            metrics         = metrics,
        )

    def _calc_metrics(self, eq_df: pd.DataFrame, tr_df: pd.DataFrame) -> dict:
        """핵심 성과 지표 계산."""
        if eq_df.empty:
            return {}

        equity   = eq_df["equity"]
        init     = self.bt_cfg.initial_cash
        final    = float(equity.iloc[-1])
        total_ret= (final - init) / init

        # CAGR
        days = (eq_df.index[-1] - eq_df.index[0]).days
        years= days / 365.25
        cagr = (final / init) ** (1 / max(years, 0.01)) - 1 if years > 0 else 0.0

        # MDD
        roll_max = equity.cummax()
        drawdown = (equity - roll_max) / roll_max
        mdd      = float(drawdown.min())

        # 일간 수익률 → Sharpe (연환산, rf=0)
        daily_ret = equity.pct_change().dropna()
        sharpe    = float(daily_ret.mean() / daily_ret.std() * (252 ** 0.5)) \
                    if daily_ret.std() > 0 else 0.0

        # 거래 통계
        sells    = tr_df[tr_df["side"] == "SELL"] if not tr_df.empty else pd.DataFrame()
        n_trades = len(sells)
        win_rate = float((sells["pnl"] > 0).mean()) if n_trades > 0 else 0.0
        avg_pnl  = float(sells["pnl"].mean()) if n_trades > 0 else 0.0
        total_pnl= float(sells["pnl"].sum()) if n_trades > 0 else 0.0

        return {
            "initial_cash":  init,
            "final_equity":  final,
            "total_return":  total_ret,
            "cagr":          cagr,
            "mdd":           mdd,
            "sharpe":        sharpe,
            "n_trades":      n_trades,
            "win_rate":      win_rate,
            "avg_pnl":       avg_pnl,
            "total_pnl":     total_pnl,
            "period_days":   days,
        }

    def _calc_monthly_returns(self, eq_df: pd.DataFrame) -> pd.DataFrame:
        """월별 수익률 계산."""
        if eq_df.empty:
            return pd.DataFrame()
        monthly = eq_df["equity"].resample("ME").last()
        monthly_ret = monthly.pct_change().dropna()
        df = monthly_ret.reset_index()
        df.columns = ["date", "return"]
        df["year"]  = df["date"].dt.year
        df["month"] = df["date"].dt.month
        return df[["year", "month", "return"]].reset_index(drop=True)

    # ── 유틸 ────────────────────────────────────────────────────────────────

    def _business_days(self) -> List[str]:
        """수집된 CSV 파일명에서 영업일 목록 추출."""
        data_dir = Path(self.provider._data_dir)
        start_dt = datetime.strptime(self.bt_cfg.start, "%Y%m%d")
        end_dt   = datetime.strptime(self.bt_cfg.end,   "%Y%m%d")

        days = []
        for p in sorted(data_dir.glob("????????.csv")):
            try:
                d = datetime.strptime(p.stem, "%Y%m%d")
                if start_dt <= d <= end_dt:
                    days.append(p.stem)
            except ValueError:
                continue
        return days
