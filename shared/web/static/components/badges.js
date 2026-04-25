/**
 * qc-badges — Q-TRON status badges (BATCH / AUTO GATE) component
 * ==============================================================
 * Phase 3 extraction. Renders the BATCH and AUTO GATE badges in the
 * top nav. Pure refactor: API, calc, state-judgment, engine logic
 * untouched. Nav.js continues to own polling/fetching; this component
 * owns the **decision** (compute) and **render** (set DOM) for each badge.
 *
 * Contract (matches docs/ui_data_contract_20260424.md §5)
 *   qc.badges.computeBatchDone(apiData, market) -> bool
 *     - market='US': uses ET wall-clock (P0-1 logic)
 *     - market='KR': uses kr_done flag from /api/batch/status response
 *   qc.badges.setBatch(el, done, market)
 *     - el: #nav-batch-badge element
 *     - done: bool
 *     - market: 'KR'|'US' (used for tooltip text)
 *   qc.badges.setAutoGate(el, auto, health, market)
 *     - el: #nav-auto-gate-badge element
 *     - auto, health: payload subobjects from /api/state or /api/status/summary
 *     - market: 'KR'|'US' (used for tooltip lines)
 *
 * No API changes. No calc changes. No state changes.
 */
(function () {
    'use strict';
    window.qc = window.qc || {};

    function computeBatchDone(d, market) {
        // Phase 4-A.1 (2026-04-25): unified contract — both markets'
        // /api/batch/status now returns {batch_done, business_date,
        // snapshot_created_at, snapshot_version}. Legacy fallbacks
        // (kr_done for KR, last_batch_business_date for US) stay for
        // bridge period.
        if (!d) return false;

        // KR fallback (server already commits the policy decision).
        if (market === 'KR') {
            if (typeof d.batch_done === 'boolean') return d.batch_done;
            return !!d.kr_done;  // legacy
        }

        // US: UI-P0-001 — must validate ET wall-clock 16:00 cutoff client-side.
        // Prefer unified fields; fall back to old shape for /api/rebalance/status.
        const businessDate = d.business_date || '';
        const lastBatchDate = d.last_batch_business_date || businessDate;
        const snapshotCreated = d.snapshot_created_at || '';

        if (typeof d.batch_done === 'boolean' && !d.batch_done) {
            // Server already says no — trust it
            return false;
        }
        if (!businessDate || !snapshotCreated || lastBatchDate !== businessDate) {
            return false;
        }
        try {
            const created = new Date(snapshotCreated);
            const fmt = new Intl.DateTimeFormat('en-US', {
                timeZone: 'America/New_York',
                year: 'numeric', month: '2-digit', day: '2-digit',
                hour: '2-digit', minute: '2-digit', hour12: false,
            });
            const parts = Object.fromEntries(
                fmt.formatToParts(created).map(p => [p.type, p.value])
            );
            const createdEtDate = `${parts.year}-${parts.month}-${parts.day}`;
            const createdEtHour = parseInt(parts.hour, 10);
            return (createdEtDate === businessDate) && (createdEtHour >= 16);
        } catch (_) {
            return false;
        }
    }

    function setBatch(el, done, market) {
        if (!el) return;
        if (done) {
            el.textContent = `BATCH ✓`;
            el.title = `${market} 오늘 배치 완료`;
            el.style.display = 'inline-flex';
        } else {
            el.style.display = 'none';
        }
    }

    function setAutoGate(el, auto, health, market) {
        if (!el) return;
        if (!auto) {
            el.textContent = 'AUTO GATE: UNKNOWN';
            el.className = 'qnav-gate-badge gate-unknown';
            el.title = 'auto_trading field missing from API response';
            el.style.display = 'inline-flex';
            return;
        }
        const mode = (auto.mode || 'advisory').toLowerCase();
        const enabled = auto.enabled === true;
        const top = auto.highest_priority_blocker || '';
        const blockers = Array.isArray(auto.blockers) ? auto.blockers : [];
        const computed = auto.computed_at || '';
        const healthStatus = (health && health.status) || auto.strategy_health || 'UNKNOWN';
        const warmup = !!(health && health.warmup_active);

        // Stale check: computed_at > 5 min old
        let stale = false;
        if (computed) {
            const t = Date.parse(computed);
            if (!isNaN(t) && (Date.now() - t) > 5 * 60 * 1000) stale = true;
        } else {
            stale = true;
        }

        // Color rule
        let label, cls;
        if (mode === 'enforcing' && !enabled) {
            label = 'AUTO GATE: BLOCKED';
            cls = 'gate-blocked';
        } else if (mode === 'enforcing' && enabled) {
            label = 'AUTO GATE: ENFORCING';
            cls = 'gate-enforcing';
        } else {
            // advisory (default)
            label = enabled ? 'AUTO GATE: ADVISORY (OK)' : 'AUTO GATE: ADVISORY';
            cls = 'gate-advisory';
        }
        if (stale) label += ' · STALE';

        el.textContent = top ? `${label} · ${top}` : label;
        el.className = 'qnav-gate-badge ' + cls + (stale ? ' gate-stale' : '');
        const tip = [
            `Market: ${market}`,
            `Mode: ${auto.mode || 'advisory'}`,
            `Enabled: ${enabled}`,
            `Top blocker: ${top || '(none)'}`,
            `Blockers: ${blockers.length ? blockers.join(', ') : '(none)'}`,
            `Health: ${healthStatus}${warmup ? ' (warm-up)' : ''}`,
            `Risk: ${auto.risk_level || 'NORMAL'}`,
            `Buy scale: ${auto.buy_scale != null ? auto.buy_scale : '-'}`,
            `Last eval: ${computed || '(unknown)'}${stale ? ' [STALE]' : ''}`,
        ].join('\n');
        el.title = tip;
        el.style.display = 'inline-flex';
    }

    window.qc.badges = { computeBatchDone, setBatch, setAutoGate };
})();
