# Q-TRON Gen3 v7 Operations Checklist

## Daily Workflow

### Pre-Market (08:30~09:00)
1. Kiwoom HTS login
2. `run_live.bat` execute
3. Reconciliation auto-runs (Step -1: PRE_RUN)
   - OK → proceed
   - MISMATCH → resolve before trading (see below)

### Market Hours (09:00~15:30)
- Engine runs automatically
- Monitor console for:
  - `[GHOST ORDER]` — timeout, check HTS immediately
  - `[GHOST FILL]` — late fill detected, portfolio NOT updated
  - `[PARTIAL]` — partial fill in progress

### Post-Market (15:30~)
- EOD reconciliation auto-runs
- Check ghost order warnings
- Run `run_batch_v7.bat` (18:00~20:30) for next day signals

---

## Alert Response Guide

### GHOST_ORDER (TIMEOUT_PENDING)
**Meaning**: Order sent but no fill confirmation within 30s.
**Action**:
1. Open HTS > [0341] Unfilled Orders
2. Check if order exists and status
3. If filled: note the fill price/qty — engine will detect at EOD reconciliation
4. If unfilled: cancel in HTS > engine ignores it safely

### GHOST_FILL
**Meaning**: Late chejan arrived after timeout — fill confirmed but portfolio NOT updated.
**Action**:
1. Open HTS > [3310] Trade History
2. Verify fill qty and price
3. On next engine restart, reconciliation will detect KIWOOM_ONLY
4. Manual portfolio adjustment may be needed

### RECONCILE MISMATCH

#### KIWOOM_ONLY (account has it, engine doesn't)
- Likely cause: ghost fill or manual HTS trade
- Action: verify in HTS, consider adding to engine portfolio manually

#### ENGINE_ONLY (engine has it, account doesn't)
- Likely cause: paper_trading=True was used, or sell executed in HTS manually
- Action: if paper_trading, this is expected; if live, check trade history

#### QTY_MISMATCH (both have it, different quantities)
- Likely cause: partial fill recorded differently
- Action: check HTS fill history, adjust engine portfolio

---

## HTS Quick Reference

| Screen | Name | Use |
|--------|------|-----|
| 0341 | Unfilled Orders | Check pending/ghost orders |
| 3310 | Trade History | Verify fills |
| 0343 | Account Holdings | Compare with engine portfolio |

---

## Engine Restart Checklist
1. Check `state/portfolio_state.json` — does it reflect current holdings?
2. Check HTS [0343] — compare with portfolio_state.json
3. If mismatch: update portfolio_state.json manually before restart
4. Run `run_live.bat` — PRE_RUN reconciliation will verify

---

## Emergency Procedures

### Engine Crash During Trading
- Portfolio auto-saved on crash (main.py error recovery)
- Check `data/logs/run_live.log` for error details
- Verify HTS holdings match last saved state
- Restart engine — state restores from portfolio_state.json

### Network/API Disconnect
- TR timeout counter: 5 consecutive timeouts → engine stops
- Existing positions are safe (no auto-liquidation on disconnect)
- Reconnect and restart engine

### Daily Kill Switch Activated (DD > -4%)
- All new entries blocked automatically
- Existing positions maintained (no forced liquidation)
- Resumes next trading day with fresh daily PnL
