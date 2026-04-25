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

    // ── Init ─────────────────────────────────────────────────────
    function _init() {
        // Defer initial fetch slightly to let the page settle
        setTimeout(() => { loadEquityCurve(); }, 800);
        document.getElementById('equity-days')
            ?.addEventListener('change', loadEquityCurve);
    }
    if (document.readyState === 'loading') {
        document.addEventListener('DOMContentLoaded', _init);
    } else {
        _init();
    }

    // Expose for downstream sub-blocks (4-B.2~5 will hook in here)
    window.qcAnalytics = window.qcAnalytics || {};
    window.qcAnalytics.loadEquityCurve = loadEquityCurve;
})();
