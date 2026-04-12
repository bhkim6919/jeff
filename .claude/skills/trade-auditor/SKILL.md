---
name: trade-auditor
description: Audit Q-TRON trading execution quality — fill rates, slippage, rebalance completeness, state consistency (broker vs engine), and order intent vs actual fills.
user_invocable: true
command: trade-auditor
---

# Q-TRON Trade Auditor

Audit live and paper trading execution quality across the Q-TRON system. Verifies order fills, slippage, rebalance execution, and state consistency between broker and engine.

---

## Invocation

```
/trade-auditor                      Full audit (all checks)
/trade-auditor fills                Fill rate and slippage analysis
/trade-auditor rebalance            Rebalance execution completeness
/trade-auditor state                State consistency (broker vs engine)
/trade-auditor orders               Order intent vs actual fills comparison
/trade-auditor kr                   KR market only
/trade-auditor us                   US market only
/trade-auditor <YYYY-MM-DD>         Audit specific date
```

---

## Key File Paths

### KR Market (Gen04-REST)
- **Main orchestrator**: `C:/Q-TRON-32_ARCHIVE/Gen04-REST/main.py`
- **REST provider**: `C:/Q-TRON-32_ARCHIVE/Gen04-REST/data/rest_provider.py`
- **Portfolio manager**: `C:/Q-TRON-32_ARCHIVE/Gen04-REST/core/portfolio_manager.py`
- **State manager**: `C:/Q-TRON-32_ARCHIVE/Gen04-REST/core/state_manager.py`
- **Rebalancer**: `C:/Q-TRON-32_ARCHIVE/Gen04-REST/strategy/rebalancer.py`
- **Trail stop**: `C:/Q-TRON-32_ARCHIVE/Gen04-REST/strategy/trail_stop.py`
- **Exposure guard**: `C:/Q-TRON-32_ARCHIVE/Gen04-REST/risk/exposure_guard.py`
- **Safety checks**: `C:/Q-TRON-32_ARCHIVE/Gen04-REST/risk/safety_checks.py`
- **Risk management**: `C:/Q-TRON-32_ARCHIVE/Gen04-REST/risk/risk_management.py`
- **Reporter**: `C:/Q-TRON-32_ARCHIVE/Gen04-REST/report/reporter.py`
- **Trade quality analyzer**: `C:/Q-TRON-32_ARCHIVE/Gen04-REST/report/trade_quality_analyzer.py`
- **Daily report**: `C:/Q-TRON-32_ARCHIVE/Gen04-REST/report/daily_report.py`
- **Config**: `C:/Q-TRON-32_ARCHIVE/Gen04-REST/config.py`

### KR State Files
- **Live portfolio**: `C:/Q-TRON-32_ARCHIVE/Gen04-REST/state/portfolio_state_live.json`
- **Live runtime**: `C:/Q-TRON-32_ARCHIVE/Gen04-REST/state/runtime_state_live.json`
- **Paper portfolio**: `C:/Q-TRON-32_ARCHIVE/Gen04-REST/state/portfolio_state_paper.json`
- **Paper runtime**: `C:/Q-TRON-32_ARCHIVE/Gen04-REST/state/runtime_state_paper.json`

### US Market (Gen04-US)
- **Main orchestrator**: `C:/Q-TRON-32_ARCHIVE/Gen04-US/main.py`
- **Alpaca provider**: `C:/Q-TRON-32_ARCHIVE/Gen04-US/data/alpaca_provider.py`
- **Portfolio manager**: `C:/Q-TRON-32_ARCHIVE/Gen04-US/core/portfolio_manager.py`
- **State manager**: `C:/Q-TRON-32_ARCHIVE/Gen04-US/core/state_manager.py`
- **Rebalancer**: `C:/Q-TRON-32_ARCHIVE/Gen04-US/strategy/rebalancer.py`
- **Trail stop**: `C:/Q-TRON-32_ARCHIVE/Gen04-US/strategy/trail_stop.py`
- **Execution gate**: `C:/Q-TRON-32_ARCHIVE/Gen04-US/strategy/execution_gate.py`
- **Snapshot guard**: `C:/Q-TRON-32_ARCHIVE/Gen04-US/strategy/snapshot_guard.py`

### US State Files
- **Paper portfolio**: `C:/Q-TRON-32_ARCHIVE/Gen04-US/state/portfolio_state_us_paper.json`
- **Paper runtime**: `C:/Q-TRON-32_ARCHIVE/Gen04-US/state/runtime_state_us_paper.json`

### Report / Log Outputs
- **KR trades CSV**: `C:/Q-TRON-32_ARCHIVE/Gen04-REST/report/output/trades.csv`
- **KR close log**: `C:/Q-TRON-32_ARCHIVE/Gen04-REST/report/output/close_log.csv`
- **KR daily reports**: `C:/Q-TRON-32_ARCHIVE/Gen04-REST/report/output/daily_*.html`

---

## Audit Procedure

### Phase 1: Fill Rate and Slippage Analysis

1. **Collect fill data**:
   - Read trades.csv or close_log.csv for executed trades
   - For KR: check REST provider logs for order submission -> fill confirmation flow
   - For US: check Alpaca provider fill records
   - Read `trade_quality_analyzer.py` for existing analysis logic

2. **Calculate metrics**:
   - **Fill rate**: orders submitted / orders filled (target: >= 95%)
   - **Partial fill rate**: partially filled / total orders
   - **Slippage**: (fill_price - intended_price) / intended_price
     - For BUY: positive slippage = paid more than expected
     - For SELL: negative slippage = received less than expected
   - **Timing**: time from order submission to fill (latency)

3. **Check for anomalies**:
   - Orders that were never filled (stuck in PENDING_EXTERNAL)
   - Ghost fills (fills for cancelled orders)
   - Duplicate fills (same order filled twice -- check `_processed_fill_keys`)
   - TIMEOUT events that were misclassified (TIMEOUT != failure per safety rules)

4. **Report format**:
   ```
   FILL RATE ANALYSIS — [KR/US] — [date range]
   Total orders submitted:  NNN
   Successfully filled:     NNN (XX.X%)
   Partially filled:        NNN (XX.X%)
   Failed/Cancelled:        NNN (XX.X%)
   
   SLIPPAGE ANALYSIS
   BUY avg slippage:   +X.XX bps
   SELL avg slippage:  -X.XX bps
   Worst slippage:     XX.XX bps on [ticker] [date]
   
   ANOMALIES
   Ghost fills:        NNN
   Duplicate fills:    NNN
   Stuck orders:       NNN
   TIMEOUT events:     NNN (verified non-failures: NNN)
   ```

### Phase 2: Rebalance Execution Completeness

1. **Read rebalance records**:
   - Check rebalancer.py `compute_orders()` output (intended orders)
   - Compare with actual fills from trades.csv
   - Verify SELL orders executed before BUY orders (T+2 settlement for KR)

2. **Completeness check**:
   - Target portfolio (from factor_ranker) vs actual portfolio (from state)
   - Missing positions: target stocks not in portfolio
   - Extra positions: portfolio stocks not in target
   - Weight deviation: abs(actual_weight - target_weight) per position

3. **Rebalance timing**:
   - Verify 21-trading-day cycle (KR) is maintained
   - Check if rebalance was blocked (DD guard, SAFE_MODE, trading halt)
   - Verify no positions were "topped up" between rebalances (fill gaps stay empty)

4. **Report format**:
   ```
   REBALANCE COMPLETENESS — [date]
   Target positions:     NN
   Actual positions:     NN
   Missing positions:    NN [list tickers]
   Extra positions:      NN [list tickers]
   
   SELL execution:       NN/NN (XX.X%)
   BUY execution:        NN/NN (XX.X%)
   
   Avg weight deviation: X.XX%
   Max weight deviation: X.XX% on [ticker]
   
   Rebalance cycle:      Day NN of 21
   Blocked events:       [list any blocks with reasons]
   ```

### Phase 3: State Consistency (Broker vs Engine)

**Core principle: Broker = Truth (RECON authoritative)**

1. **Read current state files**:
   - Parse `portfolio_state_*.json` for engine's view of positions
   - Parse `runtime_state_*.json` for engine's operational state

2. **Compare with broker** (if accessible):
   - KR: REST API position query (read-only)
   - US: Alpaca API position query (read-only)
   - Compare: ticker, quantity, avg_price, current_value

3. **Discrepancy detection**:
   - **QTY mismatch**: engine says N shares, broker says M shares
   - **Position existence**: engine has position, broker doesn't (or vice versa)
   - **Cash mismatch**: engine cash vs broker cash
   - **Trail stop state**: peak_price in engine vs actual historical high

4. **RECON history**:
   - Check logs for past RECON events and their resolution
   - Count RECON discrepancies per session
   - Verify RECON results were applied to state (state = RECON truth)

5. **Report format**:
   ```
   STATE CONSISTENCY CHECK — [date/time]
   Positions in engine:   NN
   Positions at broker:   NN
   
   DISCREPANCIES:
   [ticker]  Engine: NNN shares @ $XX.XX  |  Broker: NNN shares @ $XX.XX  [MISMATCH]
   
   Cash: Engine $XXX,XXX  |  Broker $XXX,XXX  [MATCH/MISMATCH]
   
   RECON history (last 7 days):
   [date] NN discrepancies found, NN resolved
   
   VERDICT: [CONSISTENT / DISCREPANCIES_FOUND]
   ```

### Phase 4: Order Intent vs Actual Fills

1. **Trace order lifecycle**:
   - Rebalancer `compute_orders()` -> order submission -> broker acknowledgement -> fill/cancel
   - For each intended order, trace its final status

2. **Classification**:
   - **EXACT**: filled at intended quantity and price
   - **PARTIAL**: filled but less than intended quantity
   - **SLIPPED**: filled at worse price than intended
   - **UPGRADED**: initially timed out, later confirmed via ghost fill
   - **CANCELLED**: explicitly cancelled (by system or user)
   - **LOST**: no record of fill or cancellation (requires investigation)

3. **Order flow safety**:
   - Verify no BUY->SELL->BUY triple on same ticker (HALT trigger)
   - Verify no orders during RECON
   - Verify no synchronous orders inside chejan callback (KR)
   - Check PENDING_EXTERNAL handling (TIMEOUT -> ghost -> FILLED flow)

4. **Report format**:
   ```
   ORDER LIFECYCLE AUDIT — [date range]
   Total intended orders:  NNN
   EXACT fills:           NNN (XX.X%)
   PARTIAL fills:         NNN (XX.X%)
   SLIPPED fills:         NNN (XX.X%)
   UPGRADED (ghost):      NNN (XX.X%)
   CANCELLED:             NNN (XX.X%)
   LOST:                  NNN (XX.X%) [CRITICAL if > 0]
   
   SAFETY VIOLATIONS:
   BUY-SELL-BUY triple:   NNN [CRITICAL if > 0]
   Orders during RECON:   NNN [CRITICAL if > 0]
   Sync chejan orders:    NNN [CRITICAL if > 0]
   ```

---

## Summary Report

```
TRADE AUDIT SUMMARY
============================================================
Check                      KR Status    US Status    Severity
------------------------------------------------------------
Fill rate >= 95%           [P/F]        [P/F]        ...
Avg slippage < 10 bps     [P/F]        [P/F]        ...
Rebalance completeness     [P/F]        [P/F]        ...
State consistency          [P/F]        [P/F]        ...
Order lifecycle clean      [P/F]        [P/F]        ...
No safety violations       [P/F]        [P/F]        ...
============================================================
OVERALL: [CLEAN / CONCERNS / CRITICAL_ISSUES]
```

---

## Safety Rules

- **Read-only audit** -- never submit orders or modify state
- **Broker = Truth** -- if discrepancy found, flag it; do not auto-correct engine state
- **SELL always allowed** -- never recommend blocking sells
- **TIMEOUT != failure** -- classify appropriately per global safety rules
- Follow Engine Protection Rules: do not modify portfolio_manager.py or state_manager.py
- If critical issues found (LOST orders, safety violations), escalate to P0 per execution policy
