/**
 * qc-summary-card — Q-TRON Summary 4-card component (KR side)
 * ==========================================================
 * Phase 3 extraction. Renders the existing #summary-cards section
 * without any logic change. This is a pure refactor: same API,
 * same calc, same DOM IDs, same behavior.
 *
 * Contract (matches docs/ui_data_contract_20260424.md §2)
 *   render(host, data)
 *     host — DOM element (decorative; internal lookup uses document.getElementById)
 *     data — SSE snapshot object (same shape updateSummaryCards received)
 *
 * No API changes. No calc changes. No state changes. Strictly extraction.
 */
(function () {
    'use strict';
    window.qc = window.qc || {};

    function _formatKRW(val) {
        if (val === null || val === undefined) return '--';
        const abs = Math.abs(val);
        const sign = val < 0 ? '-' : '';
        if (abs >= 1e8) return sign + (abs / 1e8).toFixed(1) + '억';
        if (abs >= 1e4) return sign + (abs / 1e4).toFixed(0) + '만';
        return val.toLocaleString();
    }

    function render(host, data) {
        // Account data may not always be in the snapshot.
        // If it is, render it; otherwise show from freshness hints.
        const account = (data && data.account) || {};

        const holdingsEl = document.getElementById('card-holdings');
        const cashEl = document.getElementById('card-cash');
        const pnlEl = document.getElementById('card-pnl');
        const totalEl = document.getElementById('card-total');

        // Defensive: bail quietly if section not on current page
        // (e.g., /debug page doesn't have #summary-cards).
        if (!holdingsEl || !cashEl || !pnlEl || !totalEl) return;

        if (account.holdings_count !== undefined) {
            holdingsEl.textContent = account.holdings_count;
        } else {
            holdingsEl.textContent = '--';
            holdingsEl.className = 'summary-value neutral';
        }

        if (account.cash !== undefined) {
            cashEl.textContent = _formatKRW(account.cash);
        } else {
            cashEl.textContent = '--';
            cashEl.className = 'summary-value neutral';
        }

        if (account.pnl_pct !== undefined) {
            const pct = account.pnl_pct;
            pnlEl.textContent = (pct >= 0 ? '+' : '') + pct.toFixed(2) + '%';
            pnlEl.className = 'summary-value ' + (pct > 0 ? 'positive' : pct < 0 ? 'negative' : 'neutral');
            const pnlAmtEl = document.getElementById('card-pnl-amt');
            if (pnlAmtEl && account.total_pnl !== undefined) {
                const amt = account.total_pnl;
                pnlAmtEl.textContent = (amt >= 0 ? '+' : '') + _formatKRW(amt);
                pnlAmtEl.className = 'summary-sub ' + (amt > 0 ? 'positive' : amt < 0 ? 'negative' : 'neutral');
            }
        } else {
            pnlEl.textContent = '--';
            pnlEl.className = 'summary-value neutral';
        }

        if (account.total_asset !== undefined) {
            totalEl.textContent = _formatKRW(account.total_asset);
            const evalEl = document.getElementById('card-eval');
            if (evalEl && account.total_eval !== undefined) {
                evalEl.textContent = '평가 ' + _formatKRW(account.total_eval);
            }
        } else {
            totalEl.textContent = '--';
            totalEl.className = 'summary-value neutral';
        }
    }

    window.qc.summary = { render };
})();
