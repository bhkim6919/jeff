/**
 * analytics.js -- Q-TRON Analytics (Equity Curve, Lab Comparison, Trade History)
 * ===============================================================================
 * PG read-only. No engine/state modification.
 */

// ── Equity Curve (Unified: Gen4 LIVE + 9 strategies + KOSPI) ──────
// Baseline = first Gen4 LIVE date (real-money start). All series normalized
// to %-change from that date so 11 lines share one axis. Default-visible =
// LIVE + KOSPI + Top-3 strategies by latest cumul_return. Remaining 6
// strategies are togglable via checkbox table below the chart.
let equityChart = null;
let equityUserOverride = {};  // {datasetKey: boolVisible} — survives within session

const STRATEGY_COLORS_MAP = {
    breakout_trend:   '#60a5fa',
    hybrid_qscore:    '#34d399',
    liquidity_signal: '#fbbf24',
    lowvol_momentum:  '#a78bfa',
    mean_reversion:   '#fb923c',
    momentum_base:    '#2dd4bf',
    quality_factor:   '#e879f9',
    sector_rotation:  '#f472b6',
    vol_regime:       '#94a3b8',
};

async function loadEquityCurve() {
    const days = document.getElementById('equity-days')?.value || 90;
    try {
        const r = await fetch(`/api/charts/equity-unified?days=${days}`);
        const d = await r.json();
        if (!d.series || !d.dates || d.dates.length === 0) return;

        const ctx = document.getElementById('equity-chart');
        if (!ctx) return;
        if (equityChart) equityChart.destroy();

        const labels = d.dates;
        const strategyKeys = Object.keys(d.series).filter(
            k => d.series[k].kind === 'strategy'
        );

        // Top-3 strategies by latest cumul_return (null-safe).
        const finalPct = {};
        strategyKeys.forEach(k => {
            const pct = d.series[k].pct;
            for (let i = pct.length - 1; i >= 0; i--) {
                if (pct[i] != null) { finalPct[k] = pct[i]; break; }
            }
        });
        const top3 = Object.keys(finalPct)
            .sort((a, b) => finalPct[b] - finalPct[a])
            .slice(0, 3);
        const top3Set = new Set(top3);

        function defaultVisible(key) {
            if (key === 'live' || key === 'kospi') return true;
            return top3Set.has(key);
        }
        function visibleFor(key) {
            return (key in equityUserOverride)
                ? equityUserOverride[key]
                : defaultVisible(key);
        }

        const datasets = [];
        // Gen4 LIVE — prominent
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
            hidden: !visibleFor('live'),
        });
        // KOSPI — benchmark
        datasets.push({
            key: 'kospi',
            label: 'KOSPI',
            data: d.series.kospi.pct,
            borderColor: '#f87171',
            borderDash: [4, 4],
            tension: 0.3,
            pointRadius: 0,
            borderWidth: 2,
            fill: false,
            hidden: !visibleFor('kospi'),
        });
        // 9 strategies
        strategyKeys.forEach(k => {
            const isTop = top3Set.has(k);
            datasets.push({
                key: k,
                label: isTop ? `🏆 ${k}` : k,
                data: d.series[k].pct,
                borderColor: STRATEGY_COLORS_MAP[k] || '#9ca3af',
                tension: 0.3,
                pointRadius: 0,
                borderWidth: isTop ? 1.8 : 1.2,
                borderDash: isTop ? [] : [2, 2],
                fill: false,
                hidden: !visibleFor(k),
            });
        });

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

        renderEquityCheckboxTable(datasets, top3Set, finalPct);
    } catch (e) {
        console.warn('[Analytics] equity chart error:', e);
    }
}

function renderEquityCheckboxTable(datasets, top3Set, finalPct) {
    const host = document.getElementById('equity-legend-table');
    if (!host) return;
    const rows = datasets.map((ds, idx) => {
        const key = ds.key;
        const isStrategy = !(key === 'live' || key === 'kospi');
        const color = ds.borderColor;
        const medal = top3Set.has(key) ? '🏆' : '';
        const pctLabel = (isStrategy && finalPct[key] != null)
            ? `${finalPct[key] >= 0 ? '+' : ''}${finalPct[key].toFixed(2)}%`
            : '';
        return `<label style="display:inline-flex;align-items:center;gap:6px;
                 padding:4px 10px;margin:2px;border:1px solid var(--border-color);
                 border-radius:6px;font-size:11px;cursor:pointer;
                 background:var(--card-bg);">
            <input type="checkbox" data-idx="${idx}" ${ds.hidden ? '' : 'checked'}
                 style="accent-color:${color};">
            <span style="display:inline-block;width:10px;height:10px;
                 border-radius:2px;background:${color};"></span>
            <span>${medal} ${ds.label.replace(/^🏆 /, '')}</span>
            ${pctLabel ? `<span style="color:${finalPct[key] >= 0 ? '#34d399' : '#f87171'};
                 margin-left:4px;">${pctLabel}</span>` : ''}
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

// ── Lab Strategy Comparison ─────────────────────────────────
let labChart = null;

const STRATEGY_COLORS = [
    '#60a5fa', '#f87171', '#34d399', '#fbbf24', '#a78bfa',
    '#fb923c', '#2dd4bf', '#e879f9', '#94a3b8',
];

async function loadLabComparison() {
    const days = document.getElementById('lab-comp-days')?.value || 30;
    try {
        const r = await fetch(`/api/charts/lab-comparison?days=${days}`);
        const d = await r.json();
        if (!d.strategies || Object.keys(d.strategies).length === 0) return;

        const ctx = document.getElementById('lab-chart');
        if (!ctx) return;

        if (labChart) labChart.destroy();

        const datasets = [];
        const stratNames = Object.keys(d.strategies).sort();
        stratNames.forEach((name, i) => {
            const points = d.strategies[name];
            datasets.push({
                label: name,
                data: points.map(p => ({ x: p.date, y: (p.cumul || 0) * 100 })),
                borderColor: STRATEGY_COLORS[i % STRATEGY_COLORS.length],
                tension: 0.3,
                pointRadius: 0,
                borderWidth: 1.5,
                fill: false,
            });
        });

        // Use first strategy's dates as labels
        const firstStrat = d.strategies[stratNames[0]] || [];
        const labels = firstStrat.map(p => p.date);

        labChart = new Chart(ctx, {
            type: 'line',
            data: { labels, datasets },
            options: {
                responsive: true,
                interaction: { intersect: false, mode: 'index' },
                plugins: {
                    legend: {
                        labels: { color: '#9ca3af', font: { size: 10 }, boxWidth: 12 },
                        position: 'bottom',
                    },
                    tooltip: {
                        callbacks: {
                            label: ctx => `${ctx.dataset.label}: ${ctx.parsed.y?.toFixed(2)}%`,
                        },
                    },
                },
                scales: {
                    x: {
                        ticks: { color: '#6b7280', font: { size: 10 }, maxTicksLimit: 8 },
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

        // Risk table
        const riskDiv = document.getElementById('lab-risk-table');
        if (riskDiv && d.risk) {
            let html = '<table style="width:100%;border-collapse:collapse;"><tr style="border-bottom:1px solid var(--border-color);">'
                + '<th style="text-align:left;padding:2px 6px;">Strategy</th>'
                + '<th style="text-align:right;padding:2px 6px;">MDD</th>'
                + '<th style="text-align:right;padding:2px 6px;">Vol(20d)</th>'
                + '<th style="text-align:right;padding:2px 6px;">Hit Rate</th></tr>';
            stratNames.forEach(name => {
                const rk = d.risk[name] || {};
                html += `<tr style="border-bottom:1px solid rgba(75,85,99,0.2);">`
                    + `<td style="padding:2px 6px;">${name}</td>`
                    + `<td style="text-align:right;padding:2px 6px;color:#f87171;">${rk.daily_mdd != null ? (rk.daily_mdd * 100).toFixed(2) + '%' : '-'}</td>`
                    + `<td style="text-align:right;padding:2px 6px;">${rk.realized_vol_20d != null ? (rk.realized_vol_20d * 100).toFixed(1) + '%' : '-'}</td>`
                    + `<td style="text-align:right;padding:2px 6px;">${rk.hit_rate_20d != null ? (rk.hit_rate_20d * 100).toFixed(0) + '%' : '-'}</td>`
                    + '</tr>';
            });
            html += '</table>';
            riskDiv.innerHTML = html;
        }
    } catch (e) {
        console.warn('[Analytics] lab chart error:', e);
    }
}

// ── Trade History ────────────────────────────────────────────

async function loadTradeHistory() {
    const code = document.getElementById('trade-code-filter')?.value || '';
    const side = document.getElementById('trade-side-filter')?.value || '';

    try {
        // Summary
        const sr = await fetch('/api/trades/summary');
        const summary = await sr.json();
        const sumDiv = document.getElementById('trade-summary');
        if (sumDiv && !summary.error) {
            sumDiv.textContent = `BUY ${summary.buy_count} | SELL ${summary.sell_count} | `
                + `Closed ${summary.closed_trades} | Win ${summary.win_rate}% | `
                + `Avg P&L ${summary.avg_pnl_pct}% | Avg Hold ${summary.avg_hold_days}d`;
        }

        // Trades table
        let url = `/api/trades?limit=50&code=${code}&side=${side}`;
        const tr = await fetch(url);
        const td = await tr.json();
        const tbl = document.getElementById('trade-table');
        if (!tbl || !td.trades) return;

        if (td.trades.length === 0) {
            tbl.innerHTML = '<div style="color:#6b7280;padding:8px;">No trades</div>';
            return;
        }

        let html = '<table style="width:100%;border-collapse:collapse;font-size:11px;">'
            + '<tr style="border-bottom:1px solid var(--border-color);">'
            + '<th style="text-align:left;padding:3px 6px;">Date</th>'
            + '<th style="text-align:left;padding:3px 6px;">Code</th>'
            + '<th style="text-align:center;padding:3px 6px;">Side</th>'
            + '<th style="text-align:right;padding:3px 6px;">Qty</th>'
            + '<th style="text-align:right;padding:3px 6px;">Price</th>'
            + '<th style="text-align:right;padding:3px 6px;">Cost</th></tr>';

        td.trades.forEach(t => {
            const sideColor = t.side === 'BUY' ? '#f87171' : '#60a5fa';
            html += `<tr style="border-bottom:1px solid rgba(75,85,99,0.15);cursor:pointer;" `
                + `onclick="loadPositionDetail('${t.code}')">`
                + `<td style="padding:3px 6px;">${t.date || ''}</td>`
                + `<td style="padding:3px 6px;">${t.code || ''}</td>`
                + `<td style="text-align:center;padding:3px 6px;color:${sideColor};font-weight:600;">${t.side || ''}</td>`
                + `<td style="text-align:right;padding:3px 6px;">${t.quantity || ''}</td>`
                + `<td style="text-align:right;padding:3px 6px;">${t.price ? Number(t.price).toLocaleString() : ''}</td>`
                + `<td style="text-align:right;padding:3px 6px;">${t.cost ? Number(t.cost).toLocaleString() : ''}</td>`
                + '</tr>';
        });
        html += '</table>';
        if (td.total > 50) {
            html += `<div style="color:#6b7280;font-size:10px;margin-top:4px;">Showing 50 of ${td.total}</div>`;
        }
        tbl.innerHTML = html;

        // Update export link
        const exportLink = document.getElementById('trade-export-link');
        if (exportLink) {
            exportLink.href = `/api/export/trades?start=&end=`;
        }
    } catch (e) {
        console.warn('[Analytics] trade history error:', e);
    }
}

// ── Position Detail (modal-like) ────────────────────────────

async function loadPositionDetail(code) {
    try {
        const [histR, closeR] = await Promise.all([
            fetch(`/api/positions/${code}/history?days=30`),
            fetch(`/api/positions/${code}/closes`),
        ]);
        const hist = await histR.json();
        const closes = await closeR.json();

        let html = `<div style="position:fixed;top:0;left:0;right:0;bottom:0;background:rgba(0,0,0,0.7);z-index:9999;display:flex;align-items:center;justify-content:center;" onclick="this.remove()">`;
        html += `<div style="background:var(--card-bg,#1f2937);border-radius:12px;padding:20px;max-width:600px;width:90%;max-height:80vh;overflow-y:auto;color:var(--text-color,#e5e7eb);" onclick="event.stopPropagation()">`;
        html += `<h3 style="margin:0 0 12px;font-size:16px;">${code} Position Detail</h3>`;

        // Close history
        if (closes.closes && closes.closes.length > 0) {
            html += '<h4 style="font-size:13px;margin:12px 0 6px;">Closed Trades</h4>';
            html += '<table style="width:100%;border-collapse:collapse;font-size:11px;">';
            html += '<tr style="border-bottom:1px solid var(--border-color);"><th>Date</th><th>Reason</th><th>Hold</th><th>P&L%</th><th>HWM%</th></tr>';
            closes.closes.forEach(c => {
                const pnlColor = (c.pnl_pct || 0) >= 0 ? '#f87171' : '#60a5fa';
                html += `<tr style="border-bottom:1px solid rgba(75,85,99,0.15);">`
                    + `<td style="padding:2px 4px;">${c.date}</td>`
                    + `<td style="padding:2px 4px;">${c.exit_reason}</td>`
                    + `<td style="padding:2px 4px;text-align:right;">${c.hold_days}d</td>`
                    + `<td style="padding:2px 4px;text-align:right;color:${pnlColor};">${(c.pnl_pct||0).toFixed(2)}%</td>`
                    + `<td style="padding:2px 4px;text-align:right;">${c.max_hwm_pct != null ? (c.max_hwm_pct).toFixed(1)+'%' : '-'}</td></tr>`;
            });
            html += '</table>';
        } else {
            html += '<p style="color:#6b7280;font-size:12px;">No closed trades</p>';
        }

        // Daily position history
        if (hist.history && hist.history.length > 0) {
            html += '<h4 style="font-size:13px;margin:16px 0 6px;">Daily Position (last 30d)</h4>';
            html += '<table style="width:100%;border-collapse:collapse;font-size:11px;">';
            html += '<tr style="border-bottom:1px solid var(--border-color);"><th>Date</th><th>Qty</th><th>Price</th><th>P&L%</th><th>HWM</th></tr>';
            hist.history.slice(0, 15).forEach(h => {
                const pColor = (h.pnl_pct || 0) >= 0 ? '#f87171' : '#60a5fa';
                html += `<tr style="border-bottom:1px solid rgba(75,85,99,0.15);">`
                    + `<td style="padding:2px 4px;">${h.date}</td>`
                    + `<td style="padding:2px 4px;text-align:right;">${h.quantity}</td>`
                    + `<td style="padding:2px 4px;text-align:right;">${Number(h.current_price||0).toLocaleString()}</td>`
                    + `<td style="padding:2px 4px;text-align:right;color:${pColor};">${(h.pnl_pct||0).toFixed(2)}%</td>`
                    + `<td style="padding:2px 4px;text-align:right;">${Number(h.high_watermark||0).toLocaleString()}</td></tr>`;
            });
            html += '</table>';
        }

        html += '<button style="margin-top:12px;padding:6px 16px;background:#374151;border:1px solid #4b5563;color:#e5e7eb;border-radius:6px;cursor:pointer;" onclick="this.closest(\'div[style*=fixed]\').remove()">Close</button>';
        html += '</div></div>';
        document.body.insertAdjacentHTML('beforeend', html);
    } catch (e) {
        console.warn('[Analytics] position detail error:', e);
    }
}

// ── Risk Metrics ─────────────────────────────────────────────

async function loadRiskMetrics() {
    const days = document.getElementById('risk-days')?.value || 60;
    try {
        const r = await fetch(`/api/risk/metrics?days=${days}`);
        const d = await r.json();
        const container = document.getElementById('risk-cards');
        const empty = document.getElementById('risk-empty');
        if (!container) return;

        if (d.error) {
            container.innerHTML = '';
            if (empty) empty.textContent = d.error === 'insufficient data'
                ? 'Equity 데이터 부족 (실거래 후 표시됩니다)'
                : d.error;
            return;
        }
        if (empty) empty.textContent = '';

        const metrics = [
            { label: 'Sharpe', value: d.sharpe, fmt: v => v.toFixed(2), color: v => v >= 1 ? '#34d399' : v >= 0.5 ? '#fbbf24' : '#f87171' },
            { label: 'Sortino', value: d.sortino, fmt: v => v.toFixed(2), color: v => v >= 1.5 ? '#34d399' : v >= 0.7 ? '#fbbf24' : '#f87171' },
            { label: 'MDD', value: d.mdd, fmt: v => v.toFixed(2) + '%', color: v => v > -10 ? '#34d399' : v > -20 ? '#fbbf24' : '#f87171' },
            { label: 'CAGR', value: d.cagr, fmt: v => v.toFixed(1) + '%', color: v => v > 15 ? '#34d399' : v > 5 ? '#fbbf24' : '#f87171' },
            { label: 'Win Rate', value: d.win_rate, fmt: v => v.toFixed(0) + '%', color: v => v >= 50 ? '#34d399' : '#f87171' },
            { label: 'Volatility', value: d.volatility, fmt: v => v.toFixed(1) + '%', color: () => '#9ca3af' },
            { label: 'Cum Return', value: d.cum_return, fmt: v => v.toFixed(2) + '%', color: v => v > 0 ? '#34d399' : '#f87171' },
            { label: 'KOSPI', value: d.kospi_return, fmt: v => v != null ? v.toFixed(2) + '%' : '-', color: v => v > 0 ? '#f87171' : '#60a5fa' },
        ];

        container.innerHTML = metrics.map(m => {
            const val = m.value != null ? m.value : null;
            const display = val != null ? m.fmt(val) : '-';
            const clr = val != null ? m.color(val) : '#6b7280';
            return `<div style="background:rgba(75,85,99,0.15);border-radius:8px;padding:10px 12px;text-align:center;">
                <div style="font-size:10px;color:#9ca3af;margin-bottom:4px;">${m.label}</div>
                <div style="font-size:18px;font-weight:700;color:${clr};">${display}</div>
            </div>`;
        }).join('');

        if (d.period) {
            container.innerHTML += `<div style="grid-column:1/-1;font-size:10px;color:#6b7280;text-align:right;margin-top:2px;">${d.period} (${d.days}d)</div>`;
        }
    } catch (e) {
        console.warn('[Analytics] risk metrics error:', e);
    }
}

// ── Rebalance History ────────────────────────────────────────

async function loadRebalHistory() {
    try {
        const r = await fetch('/api/rebalance/history?limit=10');
        const d = await r.json();
        const div = document.getElementById('rebal-history');
        if (!div) return;

        if (!d.rebalances || d.rebalances.length === 0) {
            div.innerHTML = '<div style="color:#6b7280;font-size:12px;">No rebalance history (실거래 후 표시됩니다)</div>';
            return;
        }

        let html = '<table style="width:100%;border-collapse:collapse;font-size:11px;">'
            + '<tr style="border-bottom:1px solid var(--border-color);">'
            + '<th style="text-align:left;padding:3px 6px;">Date</th>'
            + '<th style="text-align:right;padding:3px 6px;">BUY</th>'
            + '<th style="text-align:right;padding:3px 6px;">SELL</th>'
            + '<th style="text-align:right;padding:3px 6px;">Closed</th>'
            + '<th style="text-align:right;padding:3px 6px;">Win</th>'
            + '<th style="text-align:right;padding:3px 6px;">Avg P&L</th>'
            + '<th style="text-align:right;padding:3px 6px;">Avg Hold</th>'
            + '<th style="text-align:right;padding:3px 6px;">Cost</th></tr>';

        d.rebalances.forEach(rb => {
            const winRate = rb.closed > 0 ? Math.round(rb.close_wins / rb.closed * 100) : 0;
            const pnlColor = (rb.avg_pnl || 0) >= 0 ? '#34d399' : '#f87171';
            html += `<tr style="border-bottom:1px solid rgba(75,85,99,0.15);">`
                + `<td style="padding:3px 6px;">${rb.date}</td>`
                + `<td style="text-align:right;padding:3px 6px;color:#f87171;">${rb.buys}</td>`
                + `<td style="text-align:right;padding:3px 6px;color:#60a5fa;">${rb.sells}</td>`
                + `<td style="text-align:right;padding:3px 6px;">${rb.closed}</td>`
                + `<td style="text-align:right;padding:3px 6px;">${winRate}%</td>`
                + `<td style="text-align:right;padding:3px 6px;color:${pnlColor};">${rb.avg_pnl}%</td>`
                + `<td style="text-align:right;padding:3px 6px;">${rb.avg_hold}d</td>`
                + `<td style="text-align:right;padding:3px 6px;">${Number(rb.total_cost).toLocaleString()}</td></tr>`;
        });
        html += '</table>';
        div.innerHTML = html;
    } catch (e) {
        console.warn('[Analytics] rebal history error:', e);
    }
}

// ── Alert History ────────────────────────────────────────────

async function loadAlertHistory() {
    try {
        const r = await fetch('/api/alerts/history?limit=30');
        const d = await r.json();
        const div = document.getElementById('alert-history');
        if (!div) return;

        if (!d.alerts || d.alerts.length === 0) {
            div.innerHTML = '<div style="color:#6b7280;font-size:12px;">No alert history</div>';
            return;
        }

        let html = '<table style="width:100%;border-collapse:collapse;font-size:11px;">'
            + '<tr style="border-bottom:1px solid var(--border-color);">'
            + '<th style="text-align:left;padding:3px 6px;">Alert Key</th>'
            + '<th style="text-align:right;padding:3px 6px;">Count</th>'
            + '<th style="text-align:right;padding:3px 6px;">Suppressed</th>'
            + '<th style="text-align:right;padding:3px 6px;">Last Sent</th></tr>';

        d.alerts.forEach(a => {
            const lastSent = a.last_sent ? a.last_sent.substring(0, 19) : '-';
            html += `<tr style="border-bottom:1px solid rgba(75,85,99,0.15);">`
                + `<td style="padding:3px 6px;max-width:200px;overflow:hidden;text-overflow:ellipsis;white-space:nowrap;">${a.alert_key}</td>`
                + `<td style="text-align:right;padding:3px 6px;">${a.send_count}</td>`
                + `<td style="text-align:right;padding:3px 6px;">${a.suppressed}</td>`
                + `<td style="text-align:right;padding:3px 6px;color:#9ca3af;">${lastSent}</td></tr>`;
        });
        html += '</table>';
        div.innerHTML = html;
    } catch (e) {
        console.warn('[Analytics] alert history error:', e);
    }
}

// ── Init ─────────────────────────────────────────────────────

document.addEventListener('DOMContentLoaded', () => {
    // Delay load to not block SSE
    setTimeout(() => {
        loadEquityCurve();
        loadTradeHistory();
        loadRiskMetrics();
        loadRebalHistory();
        loadAlertHistory();
    }, 2000);

    // Event listeners
    document.getElementById('equity-days')?.addEventListener('change', loadEquityCurve);
    document.getElementById('trade-code-filter')?.addEventListener('change', loadTradeHistory);
    document.getElementById('trade-side-filter')?.addEventListener('change', loadTradeHistory);
    document.getElementById('risk-days')?.addEventListener('change', loadRiskMetrics);
});
