"""Crypto Lab D3+ recurring jobs.

Each job module exposes a ``run(args) -> int`` entry point and is launched by a
thin script in ``scripts/crypto/`` plus a Windows Task Scheduler entry. Jobs
must be:

    - Idempotent (re-runs produce zero net change against unchanged inputs)
    - Lock-protected (concurrent runs blocked, never silent overwrites)
    - All-or-nothing (PG transaction + atomic file rename, no partial writes)
    - Logger-first observable (Telegram is best-effort, never job-fatal)

D3 isolation: jobs only touch ``crypto_*`` PG tables and ``crypto/`` files.
"""
