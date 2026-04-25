/**
 * analytics.us.js — Q-TRON US Analytics (Phase 4-B.1)
 * =====================================================
 * Mirror of KR Analytics, US-specific data sources:
 *   - Equity Curve: /api/charts/equity-unified  (LIVE + SPY)
 *   - Trade History: TBD (Phase 4-B.2)
 *   - Risk Metrics:  TBD (Phase 4-B.3)
 *   - Rebal History: TBD (Phase 4-B.4)
 *   - Alert History: TBD (Phase 4-B.5)
 *
 * Read-only. No state mutation. Defensive null-checks throughout
 * so the script no-ops cleanly when its target DOM is absent
 * (e.g., on /debug or /surge pages).
 */
(function () {
    'use strict';

    // ── Equity Curve ────────────────────────────────────────────
    let equityChart = null;
    let equityUserOverride = {};  // {datasetKey: boolVisible}

    async function loadEquityCurve() {
        const ctx = document.getElementById('equity-chart');
        if (!ctx) return;  // section not on page
        if (typeof Chart === 'undefined') {
            console.warn('[Analytics.US] Chart.js not loaded; skipping equity curve');
            return;
        }
        const days = document.getElementById('equity-days')?.value || 90;
        try {
            const r = await fetch(`/api/charts/equity-unified?days=${days}`);
            const d = await r.json();

            if (d.error === 'no_us_live_data' || !d.series || !d.dates || d.dates.length === 0) {
                _renderEmptyState(ctx, d.error || 'no_data');
                return;
            }

            if (equityChart) equityChart.destroy();

            const labels = d.dates;
            const datasets = [];

            // Gen4 LIVE — prominent line
            if (d.series.live) {
                datasets.push({
                    key: 'live',
                    label: 'Gen4 LIVE',
                    data: d.series.live.pct,
                    borderColor: '#60a5fa',
                    backgroundColor: 'rgba(96,165,250,0.15)',
                    fill: true,
                    tension: 0.3,
                    pointRadius: 0,
                    borderWidth: 3,
                    hidden: _visibleFor('live', true) === false,
                });
            }

            // SPY — benchmark
            if (d.series.spy) {
                datasets.push({
                    key: 'spy',
                    label: 'SPY',
                    data: d.series.spy.pct,
                    borderColor: '#f87171',
                    borderDash: [4, 4],
                    tension: 0.3,
                    pointRadius: 0,
                    borderWidth: 2,
                    fill: false,
                    hidden: _visibleFor('spy', true) === false,
                });
            }

            equityChart = new Chart(ctx, {
                type: 'line',
                data: { labels, datasets },
                options: {
                    responsive: true,
                    interaction: { intersect: false, mode: 'index' },
                    plugins: {
                        legend: { display: false },
                        tooltip: {
                            callbacks: {
                                label: c => c.parsed.y == null
                                    ? null
                                    : `${c.dataset.label}: ${c.parsed.y.toFixed(2)}%`,
                            },
                        },
                    },
                    scales: {
                        x: {
                            ticks: { color: '#6b7280', font: { size: 10 }, maxTicksLimit: 10 },
                            grid: { color: 'rgba(75,85,99,0.2)' },
                        },
                        y: {
                            ticks: {
                                color: '#6b7280', font: { size: 10 },
                                callback: v => v.toFixed(1) + '%',
                            },
                            grid: { color: 'rgba(75,85,99,0.2)' },
                        },
                    },
                },
            });

            _renderLegendTable(datasets);
        } catch (err) {
            console.warn('[Analytics.US] equity chart error:', err);
            _renderEmptyState(ctx, 'fetch_error');
        }
    }

    function _visibleFor(key, defaultVisible) {
        if (key in equityUserOverride) return equityUserOverride[key];
        return defaultVisible;
    }

    function _renderEmptyState(ctx, reason) {
        // Replace canvas with a placeholder div without removing it
        // (so subsequent loads can rebuild on it).
        const host = document.getElementById('equity-legend-table');
        if (host) {
            const msg = reason === 'no_us_live_data'
                ? 'Not enough US LIVE history yet — chart will populate after EOD commits accumulate (typically ~7+ trading days).'
                : reason === 'fetch_error'
                ? 'Failed to load equity curve. Check :8081/api/charts/equity-unified.'
                : 'No data available.';
            host.innerHTML = `<div style="color:var(--muted);font-size:12px;padding:8px;">${msg}</div>`;
        }
    }

    function _renderLegendTable(datasets) {
        const host = document.getElementById('equity-legend-table');
        if (!host) return;
        const rows = datasets.map((ds, idx) => {
            const color = ds.borderColor;
            return `<label style="display:inline-flex;align-items:center;gap:6px;
                     padding:4px 10px;margin:2px;border:1px solid var(--border, #1f2937);
                     border-radius:6px;font-size:11px;cursor:pointer;
                     background:var(--card, #111827);">
                <input type="checkbox" data-idx="${idx}" ${ds.hidden ? '' : 'checked'}
                     style="accent-color:${color};">
                <span style="display:inline-block;width:10px;height:10px;
                     border-radius:2px;background:${color};"></span>
                <span>${ds.label}</span>
            </label>`;
        }).join('');
        host.innerHTML = rows;
        host.querySelectorAll('input[type=checkbox]').forEach(cb => {
            cb.addEventListener('change', (ev) => {
                const idx = parseInt(ev.target.dataset.idx, 10);
                const visible = ev.target.checked;
                const key = equityChart.data.datasets[idx].key;
                equityChart.setDatasetVisibility(idx, visible);
                equityChart.update();
                equityUserOverride[key] = visible;
            });
        });
    }

    // ── Trade History (Phase 4-B.2) ──────────────────────────────
    let _tradeDebounceTimer = null;

    async function loadTradeHistory() {
        const tableHost = document.getElementById('trade-table');
        if (!tableHost) return;  // section absent on this page
        const symbol = document.getElementById('trade-symbol-filter')?.value?.trim() || '';
        const side = document.getElementById('trade-side-filter')?.value || '';

        // Summary in parallel
        const summaryHost = document.getElementById('trade-summary');
        try {
            const params = new URLSearchParams();
            if (symbol) params.set('symbol', symbol);
            if (side) params.set('side', side);
            params.set('limit', '50');

            const [tradesRes, summaryRes] = await Promise.all([
                fetch('/api/trades?' + params.toString()),
                fetch('/api/trades/summary'),
            ]);
            const td = await tradesRes.json();
            const sm = await summaryRes.json();

            // Summary line
            if (summaryHost) {
                if (sm.error) {
                    summaryHost.innerHTML = `<span style="color:#f87171;">summary error: ${_escape(sm.error)}</span>`;
                } else if (sm.total_count === 0) {
                    summaryHost.textContent = 'No trades yet — US LIVE trade log empty.';
                } else {
                    summaryHost.textContent =
                        `BUY ${sm.buy_count} | SELL ${sm.sell_count} | Total ${sm.total_count} | ` +
                        `Cost $${(sm.total_cost || 0).toLocaleString('en-US', {minimumFractionDigits: 2, maximumFractionDigits: 2})}` +
                        (sm.last_date ? ` | Last ${sm.last_date.slice(0, 10)}` : '');
                }
            }

            // Table
            if (td.error) {
                tableHost.innerHTML = `<div style="color:#f87171;font-size:12px;padding:8px;">trades error: ${_escape(td.error)}</div>`;
                return;
            }
            const trades = td.trades || [];
            if (trades.length === 0) {
                tableHost.innerHTML = '<div style="color:var(--muted);font-size:12px;padding:8px;">No matching trades.</div>';
                return;
            }
            const rowsHtml = trades.map(t => {
                const sideColor = t.side === 'BUY' ? 'var(--green)' : 'var(--red)';
                const dateShort = (t.date || '').slice(0, 16).replace('T', ' ');
                const price = (t.price != null) ? `$${Number(t.price).toFixed(2)}` : '--';
                const cost = (t.cost != null) ? `$${Number(t.cost).toLocaleString('en-US', {minimumFractionDigits: 2, maximumFractionDigits: 2})}` : '--';
                return `<tr>
                    <td style="padding:4px 8px;">${_escape(dateShort)}</td>
                    <td style="padding:4px 8px;font-weight:600;">${_escape(t.symbol || t.code || '')}</td>
                    <td style="padding:4px 8px;color:${sideColor};font-weight:700;">${_escape(t.side || '')}</td>
                    <td style="padding:4px 8px;text-align:right;">${t.quantity || 0}</td>
                    <td style="padding:4px 8px;text-align:right;">${price}</td>
                    <td style="padding:4px 8px;text-align:right;">${cost}</td>
                    <td style="padding:4px 8px;color:var(--muted);">${_escape(t.reason || '')}</td>
                </tr>`;
            }).join('');
            tableHost.innerHTML = `
                <table style="width:100%;border-collapse:collapse;font-size:12px;">
                    <thead>
                        <tr style="border-bottom:1px solid var(--border);color:var(--muted);font-size:11px;text-transform:uppercase;">
                            <th style="text-align:left;padding:4px 8px;">Time</th>
                            <th style="text-align:left;padding:4px 8px;">Symbol</th>
                            <th style="text-align:left;padding:4px 8px;">Side</th>
                            <th style="text-align:right;padding:4px 8px;">Qty</th>
                            <th style="text-align:right;padding:4px 8px;">Price</th>
                            <th style="text-align:right;padding:4px 8px;">Cost</th>
                            <th style="text-align:left;padding:4px 8px;">Reason</th>
                        </tr>
                    </thead>
                    <tbody>${rowsHtml}</tbody>
                </table>`;
        } catch (err) {
            console.warn('[Analytics.US] trade history error:', err);
            if (summaryHost) summaryHost.innerHTML = `<span style="color:#f87171;">fetch error: ${_escape(String(err))}</span>`;
            tableHost.innerHTML = '';
        }
    }

    function _debouncedLoadTrades() {
        if (_tradeDebounceTimer) clearTimeout(_tradeDebounceTimer);
        _tradeDebounceTimer = setTimeout(loadTradeHistory, 250);
    }

    function _escape(s) {
        return String(s == null ? '' : s)
            .replace(/&/g, '&amp;').replace(/</g, '&lt;').replace(/>/g, '&gt;')
            .replace(/"/g, '&quot;').replace(/'/g, '&#39;');
    }

    // ── Init ─────────────────────────────────────────────────────
    function _init() {
        // Defer initial fetch slightly to let the page settle
        setTimeout(() => {
            loadEquityCurve();
            loadTradeHistory();
        }, 800);
        document.getElementById('equity-days')
            ?.addEventListener('change', loadEquityCurve);
        document.getElementById('trade-symbol-filter')
            ?.addEventListener('input', _debouncedLoadTrades);
        document.getElementById('trade-side-filter')
            ?.addEventListener('change', loadTradeHistory);
    }
    if (document.readyState === 'loading') {
        document.addEventListener('DOMContentLoaded', _init);
    } else {
        _init();
    }

    // Expose for downstream sub-blocks (4-B.3~5 will hook in here)
    window.qcAnalytics = window.qcAnalytics || {};
    window.qcAnalytics.loadEquityCurve = loadEquityCurve;
    window.qcAnalytics.loadTradeHistory = loadTradeHistory;
})();
