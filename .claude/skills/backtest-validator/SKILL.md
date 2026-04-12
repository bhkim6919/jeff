---
name: backtest-validator
description: Validate Q-TRON backtesting results for statistical significance, bias detection, cost model comparison, and OOS performance verification across KR and US markets.
user_invocable: true
command: backtest-validator
---

# Q-TRON Backtest Validator

Systematically validate backtesting results for the Q-TRON trading system. Checks statistical robustness, common biases, cost model accuracy, and out-of-sample performance.

---

## Invocation

```
/backtest-validator                     Full validation (all checks)
/backtest-validator bias                Look-ahead and survivorship bias checks only
/backtest-validator cost                Cost model comparison (KR vs US)
/backtest-validator oos                 Out-of-sample performance verification
/backtest-validator compare <A> <B>     Compare two backtest result sets
```

---

## Key File Paths

### KR Market (kr)
- **Backtester engine**: `C:/Q-TRON-32_ARCHIVE/kr/backtest/backtester.py`
- **Regime backtester**: `C:/Q-TRON-32_ARCHIVE/kr/backtest/backtester_regime.py`
- **Regime v3**: `C:/Q-TRON-32_ARCHIVE/kr/backtest/backtester_regime_v3.py`
- **Theme proxy**: `C:/Q-TRON-32_ARCHIVE/kr/backtest/theme_proxy_backtest.py`
- **Theme compare**: `C:/Q-TRON-32_ARCHIVE/kr/backtest/theme_proxy_compare.py`
- **OHLCV collector**: `C:/Q-TRON-32_ARCHIVE/kr/backtest/ohlcv_collector.py`
- **Scoring (SHARED)**: `C:/Q-TRON-32_ARCHIVE/kr/strategy/scoring.py`
- **Factor ranker**: `C:/Q-TRON-32_ARCHIVE/kr/strategy/factor_ranker.py`
- **Config (LOCKED params)**: `C:/Q-TRON-32_ARCHIVE/kr/config.py`

### US Market (us)
- **Strategy Lab engine**: `C:/Q-TRON-32_ARCHIVE/us/lab/engine.py`
- **Lab runner**: `C:/Q-TRON-32_ARCHIVE/us/lab/runner.py`
- **Lab metrics**: `C:/Q-TRON-32_ARCHIVE/us/lab/metrics.py`
- **Lab forward test**: `C:/Q-TRON-32_ARCHIVE/us/lab/forward.py`
- **Lab config**: `C:/Q-TRON-32_ARCHIVE/us/lab/lab_config.py`
- **Scoring**: `C:/Q-TRON-32_ARCHIVE/us/strategy/scoring.py`

### Legacy / Reference
- **Gen04 backtester**: `C:/Q-TRON-32_ARCHIVE/kr-legacy/backtest/backtester.py`
- **Gen3 v7 repro**: `C:/Q-TRON-32_ARCHIVE/backtest/gen3v7/backtester.py`
- **Cross-strategy compare**: `C:/Q-TRON-32_ARCHIVE/backtest/compare_strategies.py`
- **Full backtest data**: `C:/Q-TRON-32_ARCHIVE/backtest/data_full/` (2561 symbols, 2019-2026)

### KR Strategy Lab
- **Lab engine**: `C:/Q-TRON-32_ARCHIVE/kr/lab/engine.py`
- **Lab runner**: `C:/Q-TRON-32_ARCHIVE/kr/lab/runner.py`
- **9 strategies**: `C:/Q-TRON-32_ARCHIVE/kr/lab/strategies/`

---

## Validation Procedure

### Phase 1: Look-Ahead Bias Detection

1. **Read the backtester source** to verify data access patterns:
   - Confirm OHLCV data is accessed only up to `t-1` for signals on day `t`
   - Check that `calc_volatility()` and `calc_momentum()` in scoring.py use only historical windows
   - Verify rebalance signals do NOT use same-day close prices for entry decisions
   - Check universe construction date alignment (universe built on data available at that point)

2. **Specific checks**:
   - Search for `.shift()` usage -- missing shifts on return/signal columns indicate look-ahead
   - Verify `factor_ranker.py` ranking uses data available at rebalance decision time
   - Check trail stop uses close-based (not intraday) prices consistently
   - For KR: confirm pykrx data does not include future-adjusted values

3. **Report format**:
   ```
   LOOK-AHEAD BIAS CHECK
   [PASS/FAIL] Signal generation uses t-1 data: <evidence>
   [PASS/FAIL] Universe construction date-aligned: <evidence>
   [PASS/FAIL] No future data leakage in ranking: <evidence>
   [PASS/FAIL] Trail stop uses historical closes only: <evidence>
   ```

### Phase 2: Survivorship Bias Detection

1. **Check universe construction**:
   - Read `universe_builder.py` (both KR and US) for delisted stock handling
   - Verify backtest universe is point-in-time (includes stocks that later delisted)
   - Check if data_full/ contains delisted tickers
   - For KR: verify pykrx_provider handles delisted/suspended stocks

2. **Specific checks**:
   - Look for hardcoded ticker lists (survivorship risk)
   - Check if universe is rebuilt each rebalance period from historical data
   - Verify `ffill` handling -- excessive ffill on delisted stocks inflates returns

3. **Report format**:
   ```
   SURVIVORSHIP BIAS CHECK
   [PASS/FAIL] Universe includes delisted stocks: <evidence>
   [PASS/FAIL] Point-in-time universe construction: <evidence>
   [PASS/FAIL] No hardcoded ticker lists: <evidence>
   [PASS/FAIL] Delisted stock handling (ffill limit): <evidence>
   ```

### Phase 3: Cost Model Comparison

1. **Extract cost parameters** from config files:
   - KR: `kr/config.py` -- BUY_FEE, SELL_FEE (tax + commission)
   - US: `us/config.py` -- commission model
   - Compare with known reference:
     - KR realistic: BUY 0.015% (commission) + SELL 0.23% (tax) + 0.015% (commission) = ~0.26% round-trip
     - US realistic: $0 commission (Alpaca) + SEC fee + TAF fee

2. **Known discrepancy** (from MEMORY.md):
   - validate_gen4.py used BUY 0.115%, SELL 0.295% => +472.5% (7yr)
   - backtest_gen4_core.py used BUY 0.65%, SELL 0.83% => +28.9% (3yr)
   - Current kr-legacy/backtester.py uses validate-style costs => +208.6% (7yr)
   - **Flag any cost model that deviates from realistic estimates**

3. **Slippage model**:
   - Check if slippage is applied (market impact, spread)
   - Verify slippage x2 stress test passes (Sharpe >= 1.0)

4. **Report format**:
   ```
   COST MODEL COMPARISON
   Source              BUY_FEE    SELL_FEE    Round-trip    Realistic?
   kr config   X.XXX%     X.XXX%      X.XXX%        [YES/NO]
   us config     $X.XX      $X.XX       X.XXX%        [YES/NO]
   Slippage applied:   [YES/NO]   Model: <description>
   Slippage x2 test:   Sharpe=X.XX [PASS/FAIL]
   ```

### Phase 4: Out-of-Sample (OOS) Performance Verification

1. **Split validation**:
   - Confirm training period and OOS period are clearly separated
   - KR benchmark: OOS 2023-2026, CAGR >= 15% to PASS
   - Check that no parameter tuning was done on OOS data

2. **Statistical significance**:
   - Calculate Sharpe ratio (annualized) -- PASS if >= 1.0
   - Calculate maximum drawdown -- PASS if <= -25% (KR), <= -30% (US)
   - Calculate win rate and profit factor
   - Bootstrap confidence interval on CAGR (if data available)
   - Check number of trades (>= 100 for statistical validity)

3. **Regime robustness**:
   - BULL period returns vs benchmark
   - BEAR period returns (MDD <= -25% to PASS)
   - Sideways period returns

4. **Report format**:
   ```
   OOS PERFORMANCE VERIFICATION
   Period: YYYY-MM-DD to YYYY-MM-DD (N trading days)
   CAGR:     XX.X%  [PASS/FAIL] (threshold: 15%)
   Sharpe:   X.XX   [PASS/FAIL] (threshold: 1.0)
   MDD:      -XX.X% [PASS/FAIL] (threshold: -25%)
   Win Rate: XX.X%
   Trades:   NNN    [PASS/FAIL] (threshold: 100)
   
   REGIME BREAKDOWN
   BULL:  CAGR XX.X%, MDD -XX.X%
   BEAR:  CAGR XX.X%, MDD -XX.X%
   SIDE:  CAGR XX.X%, MDD -XX.X%
   ```

### Phase 5: Cross-Validation Summary

Produce a final summary table:

```
BACKTEST VALIDATION SUMMARY
============================================================
Check                        Status    Confidence    Notes
------------------------------------------------------------
Look-ahead bias              [P/F]     [H/M/L]      ...
Survivorship bias            [P/F]     [H/M/L]      ...
Cost model realistic         [P/F]     [H/M/L]      ...
Slippage stress test         [P/F]     [H/M/L]      ...
OOS CAGR                     [P/F]     [H/M/L]      ...
OOS Sharpe                   [P/F]     [H/M/L]      ...
OOS MDD                      [P/F]     [H/M/L]      ...
Statistical significance     [P/F]     [H/M/L]      ...
============================================================
OVERALL VERDICT: [VALIDATED / CONCERNS / REJECTED]
```

---

## Safety Rules

- **NEVER modify** `kr/strategy/scoring.py` or `kr/config.py` (LOCKED per Engine Protection Rules)
- **Read-only analysis** -- this skill produces reports, never changes backtest code
- If cost model discrepancies are found, report them but do not auto-fix
- All findings must cite specific file paths and line numbers as evidence
