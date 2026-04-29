# US Alert-Gap Incident — Broker Truth Cross-Check (2026-04-29 23:09 KST)

## Trigger

Jeff observed log lines around 23:09 KST showing 18 STALE warnings + DD_GUARD
DAILY_BLOCKED `daily=-4.55% monthly=-12.76% buy_scale=0%` on the US tray, and
realized neither Telegram nor the dashboard ALERTS counter had emitted a
single notification for these events. Operator inference channel was silent
during what looked like a -12% portfolio drop.

> "이상신호가 있으면 내가 인지해야 하는데 전혀 노티스가 없네."

## Diagnosis

Read-only Alpaca cross-check via `us/scripts/broker_truth_diag.py`:

```
--- ACCOUNT (Alpaca = TRUTH) ---
  equity              $    104,804.53
  last_equity (PrevD) $    103,341.79
  → daily P&L                  +1.42%   ← positive, no DD
  cash                $     14,125.07

--- ENGINE (state file) ---
  equity (computed)   $    103,012.61   ← understates by $1,792
  dd_label            DD_CAUTION
  buy_blocked         False
  buy_scale           0.7

--- DIFF ---
  engine - broker     $     -1,791.92  (-1.71%)
```

**Per-symbol** (20 positions, 17 stale on engine side):
- `INTC`: engine $66.16 vs broker $91.03 (+37.58% gap)
- `AMD`:  engine $280.00 vs broker $327.82 (+17.08%)
- `COHR`: engine $341.35 vs broker $308.29 (-9.69%)
- `last_price_at` for 17 of 20 positions stuck at `2026-04-21T19:58:56+00:00`
  (186.5 hours = 7.77 days old — pre-merger ET close from 7 trading days ago)
- 3 fresh: `CMI`, `HAL`, `MPC` — today's rebalance BUYs

**Verdict:** real market `+1.42%` daily. The engine's `DAILY_BLOCKED -4.55%` /
`monthly -12.76%` reading was a phantom — DD computed from 17 stale prices
that the engine could not refresh.

**Open orders at broker:** 0 (the `STARTUP_BLOCKED (stale orders not cleared)`
banner shown earlier had self-resolved by the time of the check).

## Root cause (separate P0, fix not in this PR)

`us/core/portfolio_manager.py:162` has a stale-guard with reverse logic:

```python
if pos.last_price_at and _is_stale(pos.last_price_at, timestamp, 600):
    logger.warning(f"[STALE] {sym}: ...")
    continue   # rejects the new price
```

The check measures the gap between the position's last update and *now*, then
rejects the incoming update if too old. That creates a permanent lockout: once
a position falls behind by 10 minutes (e.g. across an overnight gap or after
a transient quote-feed hiccup), `last_price_at` is never advanced and every
subsequent update is rejected too.

Fix is intended for a separate P0 PR — `portfolio_manager.py` is in the
PROTECTED RUNTIME tier per CLAUDE.md and warrants its own scoped review.

## What this PR ships

Observability only — no engine logic changes. Five hooks from the operator's
mental model that were missing alerts:

1. **DD_GUARD transition** — fires on label change (NORMAL→DAILY_BLOCKED,
   recovery → NORMAL, level→level). One alert per transition.
2. **STALE burst summary** — single message listing the symbols that have
   been stale ≥6h. Throttle 1h between bursts. Recovery (count→0) always
   fires once. Market-hours only — overnight gaps don't trigger.
3. **Single-loop equity drop** — equity drop ≥5% in one loop iteration,
   one-shot until equity recovers. Includes "verify broker truth" hint.
4. **STARTUP_BLOCKED entry** — fires when `_buy_blocked_startup` is set
   from `STARTUP_CANCEL_INCOMPLETE` or `STARTUP_CANCEL_FAIL`. Recovery
   alert at `STARTUP_BLOCK_RELEASED` already exists (line 686).
5. **Dashboard ALERTS mirror** — already wired via `telegram_bot.send()`
   `finally` block (`recent_alerts.record()`). Every new `notify.send()`
   call here populates the ring automatically.

Constraints honored:
- BUY/SELL decision logic untouched
- Portfolio valuation / stale-guard logic untouched (separate P0)
- All hooks read engine state, never write
- Dedup via `us/notify/alert_dedup.py` — 20 unit tests cover transition,
  throttle, one-shot, and recovery paths

## Files

- `us/notify/alert_dedup.py` — new helper module (151 lines, pure functions)
- `us/main.py` — 5 alert call-sites, all behind `try/except: pass` so a
  notify failure can never break the trading loop
- `us/tests/test_alert_dedup.py` — 20 tests, all passing
- `us/scripts/broker_truth_diag.py` — read-only diagnostic that produced the
  evidence above; useful for future incident response

## Smoke output

```
20 passed in 0.06s
```
