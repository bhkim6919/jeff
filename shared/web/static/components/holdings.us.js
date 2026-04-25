/**
 * qc-holdings-table — Q-TRON Holdings table (US side)
 * ====================================================
 * Phase 3 extraction. Renders the existing #holdings-body table rows
 * without any logic change. Pure refactor — same column order, same
 * formatters, same color classes.
 *
 * Contract (matches docs/ui_data_contract_20260424.md §1)
 *   render(host, data, options)
 *     host    — DOM element (decorative; lookup uses document.getElementById)
 *     data    — { holdings: [...] } (subset of /api/portfolio response)
 *     options — { fmt: fn(n)->str, pctClass: fn(v)->str }
 *               formatters injected so the component doesn't duplicate
 *               the inline helpers.
 *
 * No API changes. No calc changes. No state changes.
 */
(function () {
    'use strict';
    window.qc = window.qc || {};

    function _defaultFmt(n) {
        if (n === null || n === undefined || isNaN(n)) return '--';
        return '$' + Number(n).toLocaleString('en-US', {
            minimumFractionDigits: 2, maximumFractionDigits: 2,
        });
    }
    function _defaultPctClass(v) { return v >= 0 ? 'gain' : 'loss'; }

    function render(host, data, options) {
        const tbody = document.getElementById('holdings-body');
        if (!tbody) return;

        const fmt = (options && options.fmt) || _defaultFmt;
        const pctClass = (options && options.pctClass) || _defaultPctClass;
        const holdings = (data && data.holdings) || [];

        if (holdings.length > 0) {
            tbody.innerHTML = holdings.map(h => `
                <tr>
                    <td><b>${h.code}</b></td>
                    <td>${h.qty}</td>
                    <td>${fmt(h.avg_price)}</td>
                    <td>${fmt(h.cur_price)}</td>
                    <td class="${pctClass(h.pnl)}">${fmt(h.pnl)}</td>
                    <td class="${pctClass(h.pnl_pct)}">${h.pnl_pct?.toFixed(1) || '--'}%</td>
                </tr>
            `).join('');
        } else {
            tbody.innerHTML = '<tr><td colspan="6" style="color:var(--muted);text-align:center;">No positions</td></tr>';
        }
    }

    window.qc.holdings = { render };
})();
