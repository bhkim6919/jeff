# P0 — US stale-guard reverse logic causes 7-day position lockout

## Trigger

Jeff 2026-04-30 broker-truth diagnostic confirmed engine equity
drifted ~$1.8k below broker truth and the engine was reading INTC at
$66 while the broker quoted $91 — a 37.58% gap that the trail-stop
machinery never saw. 16 of 20 US positions had ``last_price_at``
stuck at the 2026-04-21 ET close timestamp for 7+ trading days. The
``[STALE]`` log lines were firing every minute but no price update
was ever accepted.

## Root cause

``us/core/portfolio_manager.py`` ``update_prices`` had a one-way
stale latch:

```python
if pos.last_price_at and _is_stale(pos.last_price_at, timestamp, 600):
    logger.warning(f"[STALE] {sym}: ...")
    continue                # ← creates the lockout
```

``_is_stale(last, current, 600)`` returns True when the gap between
``last_price_at`` and ``timestamp`` (now) exceeds 600s. But:

- ``last_price_at`` is the position's *prior* update time, not the
  quote's freshness.
- ``timestamp`` is "now", which the loop assigns after the for-loop
  ends — but only if the update isn't rejected.

So once a position fell behind by 10 minutes — across an overnight
quote-feed gap, an Alpaca hiccup, or any transient delay —
``last_price_at`` was never advanced, every subsequent update was
rejected, and the position was locked at the stale price forever.

The 7-day lockout in the live evidence is the natural consequence:
the engine restarted with stale state, the first iteration's
``timestamp`` was already too far from each ``last_price_at`` to
pass the guard, and the loop entered a permanent rejection state.

## Why this is P0

1. **Trail-stop blindness.** The trail-stop machinery reads
   ``pos.current_price``. With a stale cache, real drawdowns are
   invisible. ``COHR`` dropped from $366.78 (HWM) to $312.80 in the
   live market — -14.7%, exceeding the -12% trail trigger — but the
   engine kept reading $341.35 and never fired a SELL.
2. **DD calculation pollution.** ``get_equity()`` multiplies
   ``current_price * quantity``. Stale prices produce a stale equity,
   which feeds DD-guard thresholds. Yesterday's apparent
   ``DAILY_BLOCKED -4.55% / monthly -12.76%`` was a phantom — broker
   truth was +1.42% daily.
3. **Silent.** No alert fired for "16 positions locked out for 7
   days". PR #35 closed the alert-gap (now there's an
   ``[STALE_RECOVERY]`` warning per recovery + the burst summary on
   entry), but the lockout itself was invisible until Jeff happened
   to compare engine state to broker.
4. **Affects PROTECTED RUNTIME** (``portfolio_manager.py``) — fix
   needs JUG / USER review per CLAUDE.md.

## Fix

Treat the long gap as a *recovery event*, not a rejection:

- Detect the stale condition (same predicate as before).
- Log ``[STALE_RECOVERY] <SYM>: refreshing after <gap_h>h gap`` at
  WARNING so the operator can correlate with PR #35's
  ``[STALE]`` summary.
- One-shot widen the jump-guard caps to 100% for THIS update only.
  This absorbs the multi-day cumulative price move while still
  rejecting obvious typos (>2x or <0.5x of the cached price).
- Subsequent updates run with the standard 25% / 30% jump bounds.
- Update is accepted, ``last_price_at`` advances, lockout breaks.

The stale guard is no longer a binary skip; it's a state transition
between "tracking normally" and "first tick after a gap".

## Pre-deploy diagnostic

``us/scripts/preview_stale_recovery_impact.py`` (read-only) compares
each stale position to broker truth and classifies the impact:

```
SYM      qty  cached_px  broker_px cached_hwm   gap_h  classification
COHR      16     341.35     312.80     366.78   195.5  WOULD_SELL    ← fires SELL on recovery
GLW       29     164.55     153.00     173.75   195.5  NEAR_TRAIL    ← within 2% of trigger
ALB       27     198.45     189.00     203.78   195.5  OK
[...11 OK rows...]
AMD       20     280.00     346.00     286.96   195.5  HWM_ADVANCE
INTC      78      66.16      98.66      67.46   195.5  HWM_ADVANCE   ← +49% real, advances HWM
[...4 HWM_ADVANCE rows...]
```

Summary: **1 WOULD_SELL, 1 NEAR_TRAIL, 11 OK, 6 HWM_ADVANCE** out of
19 active positions.

The single SELL (COHR) is *correct* behaviour — the position
genuinely dropped 14.7% from HWM and trail-stop should have fired.
The fix surfaces the SELL that the bug was masking; it does not
introduce a false trigger.

## Deploy plan (Jeff morning approval required)

This PR is **DRAFT**. Do not auto-merge. Recommended sequence:

1. Re-run ``preview_stale_recovery_impact.py`` immediately before
   merge. Live broker truth may have moved; recheck the WOULD_SELL
   count.
2. **Prefer market-closed deploy.** ET 16:00 (KST 05:00) → next
   day's ET 09:30 (KST 22:30). The SELL queue won't fire until next
   open, so Jeff can review the queue and abort if anything looks
   wrong.
3. If deploying during market hours, expect:
   - 1 SELL of COHR queued within seconds of restart
   - PR #35 alert: "Trail Triggered COHR" Telegram on fill
   - PR #35 alert: "STALE positions recovered — all fresh"
4. Verify with ``broker_truth_diag.py`` post-deploy that engine
   equity now matches broker within $100.

## Tests

``us/tests/test_stale_guard_recovery.py`` — 14 passing:

  - First-ever update path (no regression)
  - Normal-window updates accepted
  - Steady-state 25%/30% jump rejection unchanged
  - INTC's 7.77-day +37.58% recovery accepted (was rejected pre-fix)
  - HWM advances on price-up recovery, holds on price-down recovery
  - 10x typo still rejected even in recovery mode
  - Recovery is one-shot — second update reverts to strict guard
  - Trail-stop fires correctly on real drops past trail
  - Trail-stop does NOT fire on drops within trail
  - DD calculation correctness post-recovery
