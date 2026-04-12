/* ================================================================
   strategy_lab.js — Strategy Lab (9-Strategy Comparison) Frontend
   ================================================================ */

(function() {
    'use strict';

    // ── Tab switching ────────────────────────────────────────
    document.querySelectorAll('.lab-tab').forEach(tab => {
        tab.addEventListener('click', () => {
            document.querySelectorAll('.lab-tab').forEach(t => t.classList.remove('active'));
            document.querySelectorAll('.tab-content').forEach(c => c.classList.remove('active'));
            tab.classList.add('active');
            const target = document.getElementById(tab.dataset.tab);
            if (target) target.classList.add('active');
        });
    });

    // ── Color palette for strategies ─────────────────────────
    const COLORS = [
        '#42a5f5', '#ef5350', '#66bb6a', '#ffa726', '#ab47bc',
        '#26c6da', '#ec407a', '#8d6e63', '#78909c',
    ];

    const GROUP_CLASS = {
        rebal: 'stlab-group-rebal',
        event: 'stlab-group-event',
        macro: 'stlab-group-macro',
        regime: 'stlab-group-regime',
    };

    // ── Load latest results ──────────────────────────────────
    async function loadResults(runId) {
        const url = runId ? `/api/lab/strategy/results?run_id=${runId}`
                         : '/api/lab/strategy/results?run_id=latest';
        try {
            const resp = await fetch(url);
            const data = await resp.json();
            if (data.error) {
                document.getElementById('stlab-status-badge').textContent = 'NO DATA';
                return;
            }
            renderResults(data);
        } catch (e) {
            console.error('Strategy Lab load error:', e);
        }
    }

    // ── Render results ───────────────────────────────────────
    function renderResults(data) {
        const resultsEl = document.getElementById('stlab-results');
        const chartEl = document.getElementById('stlab-chart-section');
        const cardsEl = document.getElementById('stlab-cards-section');

        resultsEl.style.display = '';
        chartEl.style.display = '';
        cardsEl.style.display = '';

        // Period
        const period = data.summary?.period || data.run_id || '';
        document.getElementById('stlab-period').textContent = period;
        document.getElementById('stlab-status-badge').textContent = 'COMPLETED';
        document.getElementById('stlab-status-badge').className = 'badge badge-ok';

        // Table
        renderTable(data.table || []);

        // Equity chart
        if (data.equity) {
            renderEquityChart(data.equity);
        }

        // Detail cards
        if (data.details) {
            renderDetailCards(data.details);
        }
    }

    function renderTable(rows) {
        const tbody = document.getElementById('stlab-tbody');
        tbody.innerHTML = '';

        // Find best MDD (closest to 0, excluding 0)
        const mdds = rows.map(r => parseFloat(r['MDD%'] || 0)).filter(v => v < 0);
        const bestMdd = mdds.length ? Math.max(...mdds) : null;
        const sharpes = rows.map(r => parseFloat(r['Sharpe'] || 0));
        const bestSharpe = sharpes.length ? Math.max(...sharpes) : null;

        rows.forEach(row => {
            const tr = document.createElement('tr');
            const group = (row.Group || '').trim();
            const groupCls = GROUP_CLASS[group] || '';

            const ret = parseFloat(row['Return%'] || 0);
            const mdd = parseFloat(row['MDD%'] || 0);
            const sharpe = parseFloat(row['Sharpe'] || 0);
            const calmar = parseFloat(row['Calmar'] || 0);

            tr.innerHTML = `
                <td><strong>${row.Strategy || ''}</strong></td>
                <td><span class="stlab-group-tag ${groupCls}">${group}</span></td>
                <td class="${ret >= 0 ? 'stlab-val-pos' : 'stlab-val-neg'}">${ret >= 0 ? '+' : ''}${ret.toFixed(1)}%</td>
                <td class="${mdd === bestMdd ? 'stlab-val-best' : ''}">${mdd.toFixed(1)}%</td>
                <td class="${sharpe === bestSharpe ? 'stlab-val-best' : ''}">${sharpe.toFixed(2)}</td>
                <td>${calmar.toFixed(2)}</td>
                <td>${parseFloat(row['WinRate%'] || 0).toFixed(1)}%</td>
                <td>${row.Trades || 0}</td>
                <td>${parseFloat(row['AvgHold'] || 0).toFixed(1)}</td>
                <td>${parseFloat(row['Turnover'] || 0).toFixed(2)}</td>
            `;
            tbody.appendChild(tr);
        });
    }

    function renderEquityChart(equity) {
        const canvas = document.getElementById('stlab-canvas');
        const ctx = canvas.getContext('2d');
        const dpr = window.devicePixelRatio || 1;
        const W = canvas.clientWidth;
        const H = 350;
        canvas.width = W * dpr;
        canvas.height = H * dpr;
        ctx.scale(dpr, dpr);

        ctx.clearRect(0, 0, W, H);

        const dates = equity.dates || [];
        const strategies = equity.strategies || {};
        const names = Object.keys(strategies);
        if (!dates.length || !names.length) return;

        // Find global min/max
        let gMin = Infinity, gMax = -Infinity;
        names.forEach(n => {
            const vals = strategies[n];
            vals.forEach(v => {
                if (v < gMin) gMin = v;
                if (v > gMax) gMax = v;
            });
        });
        const pad = (gMax - gMin) * 0.1 || 5;
        gMin -= pad;
        gMax += pad;

        const marginL = 55, marginR = 15, marginT = 20, marginB = 40;
        const plotW = W - marginL - marginR;
        const plotH = H - marginT - marginB;

        // Grid
        ctx.strokeStyle = 'rgba(255,255,255,.06)';
        ctx.lineWidth = 1;
        for (let i = 0; i <= 4; i++) {
            const y = marginT + plotH * i / 4;
            ctx.beginPath(); ctx.moveTo(marginL, y); ctx.lineTo(W - marginR, y); ctx.stroke();
            const val = gMax - (gMax - gMin) * i / 4;
            ctx.fillStyle = 'rgba(255,255,255,.4)';
            ctx.font = '11px monospace';
            ctx.textAlign = 'right';
            ctx.fillText(val.toFixed(1), marginL - 6, y + 4);
        }

        // 100 baseline
        if (gMin < 100 && gMax > 100) {
            const y100 = marginT + plotH * (1 - (100 - gMin) / (gMax - gMin));
            ctx.strokeStyle = 'rgba(255,255,255,.2)';
            ctx.setLineDash([4, 4]);
            ctx.beginPath(); ctx.moveTo(marginL, y100); ctx.lineTo(W - marginR, y100); ctx.stroke();
            ctx.setLineDash([]);
        }

        // Lines
        names.forEach((name, idx) => {
            const vals = strategies[name];
            ctx.strokeStyle = COLORS[idx % COLORS.length];
            ctx.lineWidth = 1.8;
            ctx.beginPath();
            vals.forEach((v, i) => {
                const x = marginL + (i / (dates.length - 1)) * plotW;
                const y = marginT + plotH * (1 - (v - gMin) / (gMax - gMin));
                if (i === 0) ctx.moveTo(x, y);
                else ctx.lineTo(x, y);
            });
            ctx.stroke();
        });

        // Legend
        const legendY = H - 12;
        let legendX = marginL;
        ctx.font = '11px sans-serif';
        names.forEach((name, idx) => {
            ctx.fillStyle = COLORS[idx % COLORS.length];
            ctx.fillRect(legendX, legendY - 8, 12, 3);
            ctx.fillStyle = 'rgba(255,255,255,.7)';
            ctx.textAlign = 'left';
            const shortName = name.length > 12 ? name.slice(0, 12) : name;
            ctx.fillText(shortName, legendX + 15, legendY - 2);
            legendX += ctx.measureText(shortName).width + 30;
        });

        // X-axis dates
        ctx.fillStyle = 'rgba(255,255,255,.4)';
        ctx.font = '10px monospace';
        ctx.textAlign = 'center';
        const step = Math.max(1, Math.floor(dates.length / 6));
        for (let i = 0; i < dates.length; i += step) {
            const x = marginL + (i / (dates.length - 1)) * plotW;
            ctx.fillText(dates[i].slice(5), x, H - marginB + 15);
        }
    }

    function renderDetailCards(details) {
        const container = document.getElementById('stlab-cards');
        container.innerHTML = '';

        const sorted = Object.entries(details).sort((a, b) => {
            const mddA = a[1].mdd || -1;
            const mddB = b[1].mdd || -1;
            return mddB - mddA; // best MDD first (closest to 0)
        });

        sorted.forEach(([name, m]) => {
            const group = m.group || '';
            const groupCls = GROUP_CLASS[group] || '';
            const ret = ((m.total_return || 0) * 100).toFixed(1);
            const retClass = parseFloat(ret) >= 0 ? 'stlab-val-pos' : 'stlab-val-neg';

            const card = document.createElement('div');
            card.className = 'stlab-card';
            card.innerHTML = `
                <div class="stlab-card-header">
                    <span class="stlab-card-name">${name}</span>
                    <span class="stlab-group-tag ${groupCls}">${group}</span>
                </div>
                <div class="stlab-card-row"><span>Return</span><span class="${retClass}">${parseFloat(ret) >= 0 ? '+' : ''}${ret}%</span></div>
                <div class="stlab-card-row"><span>MDD</span><span>${((m.mdd || 0) * 100).toFixed(1)}%</span></div>
                <div class="stlab-card-row"><span>Sharpe</span><span>${(m.sharpe || 0).toFixed(2)}</span></div>
                <div class="stlab-card-row"><span>Calmar</span><span>${(m.calmar || 0).toFixed(2)}</span></div>
                <div class="stlab-card-row"><span>Sortino</span><span>${(m.sortino || 0).toFixed(2)}</span></div>
                <div class="stlab-card-row"><span>Win Rate</span><span>${((m.win_rate || 0) * 100).toFixed(1)}%</span></div>
                <div class="stlab-card-row"><span>Trades</span><span>${m.n_trades || 0}</span></div>
                <div class="stlab-card-row"><span>Avg Hold</span><span>${(m.avg_hold_days || 0).toFixed(1)}d</span></div>
                <div class="stlab-card-row"><span>Profit Factor</span><span>${(m.profit_factor || 0).toFixed(2)}</span></div>
                <div class="stlab-card-row"><span>Avg Win</span><span class="stlab-val-pos">${((m.avg_win || 0) * 100).toFixed(1)}%</span></div>
                <div class="stlab-card-row"><span>Avg Loss</span><span class="stlab-val-neg">${((m.avg_loss || 0) * 100).toFixed(1)}%</span></div>
                <div class="stlab-card-row"><span>Exposure</span><span>${(m.avg_exposure || 0).toFixed(2)}</span></div>
                <div class="stlab-card-row"><span>Turnover</span><span>${(m.turnover || 0).toFixed(2)}</span></div>
            `;
            container.appendChild(card);
        });
    }

    // ── Run button ───────────────────────────────────────────
    document.getElementById('btn-stlab-run').addEventListener('click', async () => {
        const badge = document.getElementById('stlab-status-badge');
        badge.textContent = 'RUNNING...';
        badge.className = 'badge badge-mock';

        const body = {
            start: document.getElementById('stlab-start').value,
            end: document.getElementById('stlab-end').value,
            group: document.getElementById('stlab-group').value,
        };

        try {
            const resp = await fetch('/api/lab/strategy/run', {
                method: 'POST',
                headers: {'Content-Type': 'application/json'},
                body: JSON.stringify(body),
            });
            const data = await resp.json();
            if (data.ok) {
                badge.textContent = 'RUNNING...';
                // Poll for completion
                pollCompletion();
            } else {
                badge.textContent = data.reason || 'ERROR';
            }
        } catch (e) {
            badge.textContent = 'ERROR';
            console.error(e);
        }
    });

    async function pollCompletion() {
        const badge = document.getElementById('stlab-status-badge');
        for (let i = 0; i < 120; i++) {
            await new Promise(r => setTimeout(r, 2000));
            try {
                const resp = await fetch('/api/lab/strategy/status');
                const data = await resp.json();
                if (!data.running) {
                    if (data.latest?.state === 'completed') {
                        badge.textContent = 'COMPLETED';
                        badge.className = 'badge badge-ok';
                        loadResults('latest');
                    } else {
                        badge.textContent = data.latest?.state || 'DONE';
                    }
                    return;
                }
                const pct = data.latest?.progress_pct || 0;
                badge.textContent = `RUNNING ${pct.toFixed(0)}%`;
            } catch (e) { /* retry */ }
        }
        badge.textContent = 'TIMEOUT';
    }

    // ── Load button ──────────────────────────────────────────
    document.getElementById('btn-stlab-load').addEventListener('click', () => {
        loadResults('latest');
    });

    // ── Auto-load on tab switch ──────────────────────────────
    document.querySelector('[data-tab="strategy-tab"]').addEventListener('click', () => {
        loadResults('latest');
    });

})();
