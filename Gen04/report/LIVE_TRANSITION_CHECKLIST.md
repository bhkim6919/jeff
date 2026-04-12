# Paper to Live Transition Checklist

> Gen4 2026-04-03 live transition incident lessons.
> Reuse for Gen5 and future system transitions.
>
> **LIVE transition is not strategy validation.
> It is event handling + state machine + synchronization system validation.**

---

# Operating Principles

1. **msg is reference, chejan is truth**
2. **Unknown = WAIT, not FAIL** (UNCERTAIN, not REJECTED)
3. **RECON is recovery, not main path**
4. **State is modified in one place only** (single responsibility)

---

# [Phase 0] Pre-transition Structure Audit (D-3 to D-1)

## 0-1. Event Priority Rules

- [ ] `_on_msg` NEVER modifies order state
- [ ] `_on_msg` NEVER calls `_order_loop.quit()`
- [ ] State transitions allowed ONLY from:
  - chejan callback
  - Real timeout (30s+)

## 0-2. State Machine Structure

- [ ] SUBMITTED -> WAITING_CHEJAN -> FILLED / UNCERTAIN
- [ ] REJECTED only on explicit API failure (ret != 0)
- [ ] timeout = UNCERTAIN (not REJECTED)
- [ ] UNCERTAIN blocks re-order (no duplicate buy)

## 0-3. Duplicate Buy Prevention

- [ ] EXISTING_POSITION guard exists and tested
- [ ] pending_buys vs portfolio conflict prevented
- [ ] Same ticker re-order blocked
- [ ] pending_buys cleared in ONE place only (single authority)

## 0-4. RECON Structure

- [ ] Broker is always truth
- [ ] RECON can fully overwrite portfolio
- [ ] State save after RECON is atomic
- [ ] RECON corrections logged with detail

## 0-5. Config and Environment

- [ ] `TRADING_MODE` = `"live"` in config.py
- [ ] All bat files (desktop + project) match current mode
- [ ] Monitor GUI bat uses `--mode live`
- [ ] All `FORCE_*` flags = False
- [ ] `paper_test` state files removed or archived
- [ ] Python environment path correct in all bat files

## 0-6. State Files

- [ ] `portfolio_state_live.json` initialized with correct cash (not paper amount)
- [ ] `prev_close_equity` = actual starting capital
- [ ] `peak_equity` = actual starting capital
- [ ] `runtime_state_live.json` clean (no stale `test_cycle_id`, `pending_buys`)
- [ ] No leftover state that triggers `LIVE_BLOCKED_TEST_RESIDUE`

## 0-7. Broker Setup

- [ ] Kiwoom HTS login tested (real server, not mock)
- [ ] Account password entry flow confirmed
- [ ] `server_gubun` returns expected value for real server

---

# [Phase 1] Paper Environment Verification (MUST pass before LIVE)

> Test these in paper/mock environment first.
> Every failure here would be a real-money incident in LIVE.

## 1-1. Normal Fill Flow

- [ ] SendOrder -> chejan -> FILLED transition confirmed
- [ ] State transitions work WITHOUT _on_msg involvement
- [ ] order_no captured correctly

## 1-2. Chejan Delay Test

- [ ] chejan 5-10s delay: system waits normally
- [ ] No premature REJECTED
- [ ] No premature _order_loop.quit()

## 1-3. Chejan Missing Test

- [ ] 30s timeout -> UNCERTAIN (not REJECTED)
- [ ] Re-order blocked for UNCERTAIN ticker
- [ ] Next RECON resolves UNCERTAIN state

## 1-4. Partial Fill Test

- [ ] Partial fill: state remains consistent
- [ ] Remaining quantity tracked correctly
- [ ] Ghost order path handles late fills

## 1-5. RECON Recovery Test

- [ ] Force a missed fill, then run RECON
- [ ] Portfolio recovers to broker state
- [ ] No duplicate buy on next session
- [ ] corrections count logged accurately

## 1-6. Restart Consistency Test

- [ ] Kill process mid-execution
- [ ] Restart: state consistent
- [ ] RECON corrections <= 1
- [ ] pending_buys no conflict
- [ ] No SAFE_MODE false trigger

---

# [Phase 2] LIVE Pre-Switch Verification (D-day, pre-market)

## 2-1. Config Isolation

- [ ] TRADING_MODE = live
- [ ] signals_dir / state_mode match
- [ ] paper / live state files fully separated

## 2-2. Capital Safety

- [ ] Initial capital limited (small amount for test)
- [ ] MAX_POSITIONS limit confirmed
- [ ] Order quantity minimized for first run

## 2-3. Logging Readiness

- [ ] Order state transition logged (SUBMITTED/FILLED/REJECTED/UNCERTAIN)
- [ ] Chejan receive logged (ticker, order_no, qty, price, status)
- [ ] Timeout event logged
- [ ] RECON detail logged (corrections, types, before/after)

## 2-4. Fail-Safe Verification

- [ ] SAFE_MODE triggers correctly
- [ ] Recovery block functions
- [ ] Open orders cancel on startup works
- [ ] Graceful shutdown (Ctrl+C) saves state

---

# [Phase 3] LIVE First Execution

## 3-1. Single-Stock Smoke Test

> **CRITICAL: NEVER skip this.**
> 1 stock, minimum quantity, before any rebalance.

### Message Verification
- [ ] SendOrder returns ret=0
- [ ] `_on_msg` screen 7001: log only, no state change
- [ ] Check exact message code in log (document it: ______)
- [ ] If unknown code: STOP immediately

### Chejan Verification
- [ ] `_on_chejan_data` fires
- [ ] `order_no` not empty
- [ ] `order_status` decoded correctly
- [ ] Fill qty/price match broker HTS
- [ ] State: SUBMITTED -> ACCEPTED -> FILLED

### Result Verification
- [ ] `execute_buy()` returns `error=""`
- [ ] `portfolio.add_position()` called
- [ ] `trades.csv` has BUY entry
- [ ] Broker HTS matches engine state

### Timing
- [ ] SendOrder to chejan: ___ms
- [ ] SendOrder to FILLED: ___ms
- [ ] Compare with mock timing (expect real to be slower)

### Cleanup
- [ ] Sell or cancel test position
- [ ] RECON shows 0 corrections

## 3-2. Small Batch Test (3-5 stocks)

- [ ] Orders execute sequentially without stall
- [ ] Each order gets unique order_no
- [ ] No QEventLoop quit-flag contamination
- [ ] Total time for N orders: ___s (target: < 5s per order)
- [ ] portfolio == broker after batch
- [ ] RECON on restart: 0 corrections

### Error Injection
- [ ] Test 1 invalid ticker (delisted/suspended)
- [ ] Error handled, no crash
- [ ] Other orders continue normally

## 3-3. Full Rebalance

### Pre-Rebalance
- [ ] Signal file exists and fresh (age < 3 calendar days)
- [ ] Risk mode = NORMAL
- [ ] `last_rebalance_date` correct

### Execution
- [ ] Sells execute (if any), sell_status = COMPLETE
- [ ] Buys execute with correct quantities
- [ ] No REJECTED false-positives
- [ ] All orders tracked

### Post-Rebalance
- [ ] Position count matches target (minus high-price skips)
- [ ] Cash ratio reasonable
- [ ] trades.csv complete (all BUY/SELL entries)
- [ ] equity_log.csv updated
- [ ] Restart -> RECON corrections = 0

---

# [Phase 4] LIVE Operational Stability

## 4-1. EOD and Next Day

- [ ] Trail stop evaluation runs on all positions
- [ ] Daily report generated
- [ ] equity_log.csv row appended
- [ ] daily_positions.csv updated
- [ ] Next morning restart: RECON corrections = 0
- [ ] No SAFE_MODE triggered
- [ ] Monitor GUI shows all positions correctly
- [ ] pending_buys = 0

## 4-2. Performance

- [ ] 10-20 stock order processing time: ___s total
- [ ] Timeout occurrence rate: ___%
- [ ] Chejan average delay: ___ms

## 4-3. Risk

- [ ] duplicate buy = 0
- [ ] ghost order count = 0
- [ ] UNCERTAIN state accumulation = 0

## 4-4. RECON Dependency

- [ ] Normal operation: corrections ~ 0
- [ ] RECON is NOT the main execution path
- [ ] If corrections consistently > 0: investigate root cause

---

# [Phase 5] Critical Zero-Tolerance Items

> These MUST be zero. Any non-zero = STOP and investigate.

- [ ] **false REJECTED** = 0
- [ ] **duplicate buy** = 0
- [ ] **portfolio != broker** = 0 (after RECON)
- [ ] **pending_buys infinite accumulation** = 0
- [ ] **order_loop premature quit** = 0

---

# Known Mock vs Real Server Differences

| Area | Mock Server | Real Server | Impact |
|------|------------|-------------|--------|
| `_on_msg` order code | `[100000]` | `[107066]` buy, `[107046]` sell | False REJECTED if not handled |
| Chejan timing | Instant (~ms) | KRX relay (~100ms-5s) | Timeout if wait too short |
| `GetMasterLastPrice` | Current price | Previous close until realtime reg | Wrong order qty calculation |
| TR rate limit | Lenient | ~3.6s enforced | Query failures under load |
| Account fields | Simulated | Real settlement (T+2) | Cash availability differs |
| Cancel response | Screen 7002 codes | May differ | Unhandled cancel failures |

---

# Rollback Plan

If critical issues found during any phase:

1. **Ctrl+C** to stop live engine
2. Change `TRADING_MODE` back to `"paper"` in config.py
3. Manually cancel any open orders via Kiwoom HTS
4. Check broker for any filled orders not in engine state
5. Document: what happened, broker state vs engine state
6. Fix the issue in paper/mock mode first
7. Restart from Phase 1 (single-stock smoke test)

---

# Incident Log

## Template

```
Date:
Phase:
Symptom:
Root Cause:
Broker State:
Engine State:
Resolution:
Prevention:
```

## 2026-04-03 Gen4 First LIVE

```
Date: 2026-04-03
Phase: Skipped Phase 1-2, went directly to Phase 3 (full rebalance)
Symptom: All 17 buy orders REJECTED despite filling on broker
Root Cause: _on_msg treated [107066] (real server success) as error,
            killed chejan wait loop before fills arrived
Broker State: 17 positions filled, cash depleted
Engine State: 0 positions, cash unchanged (until RECON)
Resolution: _on_msg state transition removed, RECON recovered positions
Prevention: Phase 1 single-stock test would have caught this immediately
Lesson: LIVE transition is event/state verification, not strategy verification
```
