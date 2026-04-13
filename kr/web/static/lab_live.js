/* ================================================================
   lab_live.js -- Forward Paper Trading Frontend
   ================================================================ */
(function() {
    'use strict';

    const GROUP_CLASS = {
        rebal: 'stlab-group-rebal', event: 'stlab-group-event',
        macro: 'stlab-group-macro', regime: 'stlab-group-regime',
    };

    let sseConnection = null;

    // ── Init button ─────────────────────────────────────────
    document.getElementById('btn-live-init')?.addEventListener('click', async () => {
        const badge = document.getElementById('live-status');
        badge.textContent = 'LOADING...';
        try {
            const resp = await fetch('/api/lab/live/start', {
                method: 'POST',
                headers: {'Content-Type': 'application/json'},
                body: JSON.stringify({}),
            });
            const data = await resp.json();
            if (data.ok) {
                badge.textContent = 'RUNNING';
                badge.className = 'badge badge-ok';
                document.getElementById('live-last-run').textContent =
                    data.last_run_date ? `Last: ${data.last_run_date}` : '';
                loadState();
                connectSSE();
            }
        } catch (e) { badge.textContent = 'ERROR'; }
    });

    // ── Run daily button ────────────────────────────────────
    document.getElementById('btn-live-run')?.addEventListener('click', async () => {
        const badge = document.getElementById('live-status');
        badge.textContent = 'RUNNING EOD...';
        try {
            const resp = await fetch('/api/lab/live/run-daily', {
                method: 'POST',
                headers: {'Content-Type': 'application/json'},
                body: JSON.stringify({update_ohlcv: true}),
            });
            const data = await resp.json();
            // 결과가 있으면 항상 표시 (skip이어도)
            if (data.initialized || data.ok || data.lanes) {
                badge.textContent = 'RUNNING';
                badge.className = 'badge badge-ok';
                renderState(data);
            } else {
                // state를 직접 다시 로드
                badge.textContent = 'RUNNING';
                badge.className = 'badge badge-ok';
                loadState();
            }
        } catch (e) { badge.textContent = 'ERROR'; }
    });

    // ── Reset button ────────────────────────────────────────
    document.getElementById('btn-live-reset')?.addEventListener('click', async () => {
        if (!confirm('9전략 모두 초기화합니다. 포지션과 이력이 삭제됩니다. 계속?')) return;
        try {
            await fetch('/api/lab/live/reset', {method: 'POST'});
            loadState();
        } catch (e) {}
    });

    // ── Load state ──────────────────────────────────────────
    async function loadState() {
        try {
            const resp = await fetch('/api/lab/live/state');
            const data = await resp.json();
            renderState(data);
        } catch (e) {}
    }

    // ── SSE Connection ──────────────────────────────────────
    function connectSSE() {
        if (sseConnection) sseConnection.close();
        sseConnection = new EventSource('/sse/lab-live');
        sseConnection.addEventListener('lab_live', e => {
            try { renderState(JSON.parse(e.data)); } catch(err) {}
        });
        sseConnection.addEventListener('error', () => {
            setTimeout(() => connectSSE(), 5000);
        });
    }

    // ── Render state ────────────────────────────────────────
    function renderState(data) {
        if (!data.initialized && (!data.lanes || data.lanes.length === 0)) {
            document.getElementById('live-cards').innerHTML =
                '<p style="color:var(--text-secondary)">시작 / 복원 버튼을 눌러 초기화하세요.</p>';
            return;
        }

        const badge = document.getElementById('live-status');
        badge.textContent = data.running ? 'RUNNING' : 'IDLE';
        badge.className = data.running ? 'badge badge-ok' : 'badge badge-mock';

        if (data.last_run_date) {
            document.getElementById('live-last-run').textContent = `Last: ${data.last_run_date}`;
        }

        // Strategy cards
        renderCards(data.lanes || []);

        // Positions table
        renderPositions(data.lanes || []);

        // Load trades
        loadTrades();
    }

    function renderCards(lanes) {
        const container = document.getElementById('live-cards');
        container.innerHTML = '';

        const sorted = [...lanes].sort((a, b) => b.total_return - a.total_return);

        sorted.forEach(lane => {
            const groupCls = GROUP_CLASS[lane.group] || '';
            const retClass = lane.total_return >= 0 ? 'stlab-val-pos' : 'stlab-val-neg';
            const card = document.createElement('div');
            card.className = 'stlab-card';
            card.dataset.strategy = lane.name;
            card.innerHTML = `
                <div class="stlab-card-header">
                    <span class="stlab-card-name">${lane.name}</span>
                    <span class="stlab-group-tag ${groupCls}">${lane.group}</span>
                </div>
                <div class="stlab-card-row">
                    <span>Return</span>
                    <span class="${retClass}" style="font-size:16px;font-weight:700">
                        ${lane.total_return >= 0 ? '+' : ''}${lane.total_return.toFixed(1)}%
                    </span>
                </div>
                <div class="stlab-card-row"><span>Equity</span><span>${(lane.equity/1e6).toFixed(1)}M</span></div>
                <div class="stlab-card-row"><span>MDD</span><span>${lane.mdd.toFixed(1)}%</span></div>
                <div class="stlab-card-row"><span>Positions</span><span>${lane.n_positions}</span></div>
                <div class="stlab-card-row"><span>Trades</span><span>${lane.n_trades}</span></div>
                <div class="stlab-card-row"><span>Pending</span><span>${lane.n_pending}</span></div>
                <div class="stlab-card-row"><span>Cash</span><span>${(lane.cash/1e6).toFixed(1)}M</span></div>
            `;
            container.appendChild(card);
        });

        // Meta Layer (non-blocking)
        loadMeta();
    }

    async function loadMeta() {
        try {
            const resp = await fetch('/api/lab/live/meta');
            const meta = await resp.json();
            if (!meta.ok) return;
            renderMarketBar(meta);
            renderStrategyFit(meta);
        } catch(e) {}
    }

    function renderMarketBar(meta) {
        let bar = document.getElementById('meta-market-bar');
        if (!bar) {
            bar = document.createElement('div');
            bar.id = 'meta-market-bar';
            bar.className = 'meta-market-bar';
            const cards = document.getElementById('live-cards');
            cards.parentElement.insertBefore(bar, cards);
        }
        const tags = (meta.market_tags || []).join(' | ');
        const conf = (meta.confidence || 'LOW').toLowerCase();
        const days = meta.data_days || 0;
        const warn = days < 30 ? ' (참고용)' : '';
        bar.innerHTML = `
            <span class="meta-market-tags">${tags || '데이터 수집 중'}</span>
            <span class="meta-confidence meta-conf-${conf}">
                신뢰도: ${meta.confidence}${warn} (${days}일)
            </span>
        `;
    }

    function renderStrategyFit(meta) {
        document.querySelectorAll('.stlab-card').forEach(card => {
            const key = card.dataset.strategy;
            const fit = meta.strategy_fit?.[key];
            if (!fit) return;

            card.querySelector('.meta-box')?.remove();

            const box = document.createElement('div');
            box.className = 'meta-box';
            const scoreCls = fit.score === 'HIGH' ? 'meta-high'
                           : fit.score === 'LOW'  ? 'meta-low'
                           : 'meta-mid';

            const reasons = (fit.reasons || []).map(r => {
                const cls = r.sign === '+' ? 'meta-reason-pos' : 'meta-reason-neg';
                return `<span class="${cls}">${r.sign} ${r.text}</span>`;
            }).join('');

            box.innerHTML = `
                <div class="meta-fit-header">
                    <span class="meta-fit-label">적합도</span>
                    <span class="meta-fit-score ${scoreCls}">${fit.score}</span>
                </div>
                <div class="meta-reasons">${reasons || '<span style="opacity:0.5">데이터 부족</span>'}</div>
            `;
            card.appendChild(box);
        });
    }

    function renderPositions(lanes) {
        const tbody = document.getElementById('live-pos-tbody');
        const section = document.getElementById('live-positions-section');
        const allPos = [];

        lanes.forEach(lane => {
            (lane.positions || []).forEach(pos => {
                allPos.push({...pos, strategy: lane.name, group: lane.group});
            });
        });

        if (allPos.length === 0) {
            section.style.display = 'none';
            return;
        }
        section.style.display = '';
        tbody.innerHTML = '';

        allPos.sort((a, b) => b.pnl_pct - a.pnl_pct);

        allPos.forEach(pos => {
            const tr = document.createElement('tr');
            const pnlCls = pos.pnl_pct >= 0 ? 'stlab-val-pos' : 'stlab-val-neg';
            tr.innerHTML = `
                <td>${pos.strategy}</td>
                <td>${pos.code}</td>
                <td>${pos.name}</td>
                <td>${pos.qty}</td>
                <td>${pos.entry_price?.toLocaleString()}</td>
                <td>${pos.current_price?.toLocaleString()}</td>
                <td class="${pnlCls}">${pos.pnl_pct >= 0 ? '+' : ''}${pos.pnl_pct}%</td>
                <td class="${pnlCls}">${pos.pnl_amount?.toLocaleString()}</td>
                <td>${pos.entry_date}</td>
            `;
            tbody.appendChild(tr);
        });
    }

    async function loadTrades() {
        try {
            const resp = await fetch('/api/lab/live/trades?limit=30');
            const data = await resp.json();
            const trades = data.trades || [];
            const section = document.getElementById('live-trades-section');
            const tbody = document.getElementById('live-trades-tbody');

            if (trades.length === 0) {
                section.style.display = 'none';
                return;
            }
            section.style.display = '';
            tbody.innerHTML = '';

            trades.reverse().forEach(t => {
                const tr = document.createElement('tr');
                const pnlCls = (t.pnl_pct || 0) >= 0 ? 'stlab-val-pos' : 'stlab-val-neg';
                tr.innerHTML = `
                    <td>${t.strategy || ''}</td>
                    <td>${t.ticker || ''}</td>
                    <td>${t.entry_price?.toLocaleString() || ''}</td>
                    <td>${t.exit_price?.toLocaleString() || ''}</td>
                    <td class="${pnlCls}">${(t.pnl_pct || 0) >= 0 ? '+' : ''}${t.pnl_pct}%</td>
                    <td>${t.exit_reason || ''}</td>
                    <td>${t.exit_date || ''}</td>
                `;
                tbody.appendChild(tr);
            });
        } catch (e) {}
    }

    // ── Auto-load on tab switch ─────────────────────────────
    document.querySelector('[data-tab="live-tab"]')?.addEventListener('click', () => {
        loadState();
    });

})();
