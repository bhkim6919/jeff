/**
 * qc-holdings-table — Q-TRON Holdings card grid (KR side)
 * =======================================================
 * Phase 3 extraction. Renders the existing #holdings-list mini-card
 * grid without any logic change. Pure refactor — same DOM structure,
 * same color rules, same percent thresholds, same bar widths.
 *
 * The mini-chart hover handler (bindMiniCardHoverChart, IIFE in
 * dashboard.js) is NOT moved here — it has private state and runs as
 * a singleton tooltip. The component's render() returns control to the
 * caller, which re-binds the hover handler.
 *
 * Contract (matches docs/ui_data_contract_20260424.md §1)
 *   render(host, data, options)
 *     host    — DOM element (decorative; lookup uses document.getElementById)
 *     data    — SSE snapshot { account: { holdings: [...] } }
 *     options — { formatKRW: fn(val) -> str }  (formatter injected so the
 *               component doesn't duplicate the dashboard.js helper)
 *
 * No API changes. No calc changes. No state changes.
 */
(function () {
    'use strict';
    window.qc = window.qc || {};

    function _defaultFormat(val) {
        if (val === null || val === undefined) return '--';
        return val.toLocaleString();
    }

    function render(host, data, options) {
        const container = document.getElementById('holdings-list');
        if (!container) return;

        const fmt = (options && options.formatKRW) || _defaultFormat;
        const account = (data && data.account) || {};
        const holdings = account.holdings || [];

        if (holdings.length === 0) {
            container.innerHTML = '<div class="holdings-empty">보유종목 없음</div>';
            return;
        }

        let html = '<div class="mini-cards-grid">';
        for (const h of holdings) {
            const code = h.code || '';
            const name = h.name || code;
            const qty = h.qty || 0;
            const curPrice = h.cur_price || 0;
            const evalAmt = h.eval_amt || (curPrice * qty);
            const pnl = parseInt(h.pnl) || 0;
            const pnlRate = parseFloat(h.pnl_rate || '0');
            const isPositive = pnl > 0;
            const isNegative = pnl < 0;
            const colorClass = isPositive ? 'positive' : isNegative ? 'negative' : 'neutral';
            const sign = isPositive ? '+' : '';
            const barColor = isPositive ? '#F04452' : isNegative ? '#3182F6' : 'var(--border)';
            const barWidth = Math.min(Math.abs(pnlRate) * 5, 100);

            // 전일대비 등락률
            const dayChg = h.day_change_pct;
            const dayReason = h.day_change_reason;
            const dayChgStr = dayChg != null
                ? `${dayChg >= 0 ? '+' : ''}${dayChg.toFixed(1)}%`
                : 'N/A';
            const dayColor = dayChg > 0 ? '#F04452' : dayChg < 0 ? '#3182F6' : 'var(--text-dim)';
            const dayTitle = dayReason ? `reason: ${dayReason}` : '전일종가 대비';

            html += `
            <div class="mini-card" data-code="${code}">
                <div class="mini-top">
                    <span class="mini-name">${name}</span>
                    <span class="mini-pnl ${colorClass}">${sign}${pnlRate.toFixed(1)}%</span>
                </div>
                <div class="mini-day-chg" style="color:${dayColor}" title="${dayTitle}">전일 ${dayChgStr}</div>
                <div class="mini-bar"><div class="mini-bar-fill" style="width:${barWidth}%;background:${barColor}"></div></div>
                <div class="mini-bottom">
                    <span class="mini-eval">${fmt(evalAmt)}</span>
                    <span class="mini-qty">${qty}주</span>
                </div>
            </div>`;
        }
        html += '</div>';
        container.innerHTML = html;
    }

    window.qc.holdings = { render };
})();
