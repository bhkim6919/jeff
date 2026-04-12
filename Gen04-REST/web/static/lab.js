/* ================================================================
   Q-TRON LAB — Simulator Frontend
   ================================================================ */

(function () {
    'use strict';

    // ── State ────────────────────────────────────────────────
    let currentParams = {};
    let paramRanges = {};
    let currentRanking = [];
    let isSimulating = false;
    let isRealtimeRunning = false;
    let realtimeSSE = null;
    let previousClosedCounts = {};  // track closed trade counts for flash detection

    // ── DOM refs ─────────────────────────────────────────────
    const $paramGrid = document.getElementById('param-grid');
    const $btnSimulate = document.getElementById('btn-simulate');
    const $btnRealtime = document.getElementById('btn-realtime');
    const $btnRtStop = document.getElementById('btn-rt-stop');
    const $btnReset = document.getElementById('btn-reset');
    const $btnRefresh = document.getElementById('btn-refresh-ranking');
    const $resultsSection = document.getElementById('results-section');
    const $strategyGrid = document.getElementById('strategy-grid');
    const $rankingList = document.getElementById('ranking-list');
    const $rankingTitle = document.getElementById('ranking-title');
    const $resultTs = document.getElementById('result-ts');
    const $simStatus = document.getElementById('sim-status');
    const $clock = document.getElementById('clock');
    const $realtimeSection = document.getElementById('realtime-section');
    const $rtStrategyGrid = document.getElementById('rt-strategy-grid');
    const $rtTimer = document.getElementById('rt-timer');
    const $rtTickCount = document.getElementById('rt-tick-count');
    const $rtEvents = document.getElementById('rt-events');

    // ── Param Labels (Korean) ────────────────────────────────
    const PARAM_LABELS = {
        ranking_source:   '순위 기준',
        top_n:            '조회 종목수',
        entry_threshold:  '진입 기준',
        exit_target_a:    'A 목표 수익',
        stop_loss_a:      'A 손절',
        exit_target_b:    'B 목표 수익',
        stop_loss_b:      'B 손절',
        exit_target_c:    'C 초기 목표',
        trail_max_c:      'C 트레일 최대',
        max_positions:    '최대 종목수',
        position_size_pct:'종목당 비중',
        price_min:        '최소 주가',
    };

    // ── Regime Banner ────────────────────────────────────────
    function _regimeBarHtml(score, size) {
        const pct = Math.max(0, Math.min(100, ((parseFloat(score) + 3) / 6) * 100));
        const w = size === 'sm' ? 80 : 120;
        const h = size === 'sm' ? 4 : 6;
        const tw = size === 'sm' ? 8 : 12;
        const th = size === 'sm' ? 8 : 12;
        return `<span class="rb-bar" style="width:${w}px;height:${h}px">` +
            `<span class="rb-bar-thumb" style="left:${pct}%;width:${tw}px;height:${th}px;top:-${(th-h)/2}px"></span></span>`;
    }

    async function fetchRegime() {
        try {
            const resp = await fetch('/api/regime/current');
            const data = await resp.json();
            const a = data.actual || {};
            const el = document.getElementById('regime-banner');
            if (!el) return;
            if (!a.actual_label) {
                el.innerHTML = `<div class="rb-main">
                    <span class="rb-label">오늘 레짐</span>
                    <span class="rb-regime neutral">미판정</span>
                    <span class="rb-detail">데이터 수신 대기 중</span>
                </div>`;
                el.style.display = 'flex';
                return;
            }
            const label = a.actual_label || '--';
            const score = a.scores?.total ?? '--';
            const kospi = a.kospi_change != null ? `KOSPI ${(a.kospi_change*100).toFixed(1)}%` : '';
            const breadth = a.breadth_ratio != null ? `breadth ${(a.breadth_ratio*100).toFixed(0)}%` : '';
            const cls = label.toLowerCase().replace('_', '-');

            const opMap = {'10m': 0.7, '30m': 0.5, '1h': 0.35};
            const histHtml = (data.history || []).map(h => {
                const hCls = h.label.toLowerCase().replace('_', '-');
                const op = opMap[h.ago] || 0.2;
                return `<span class="rb-hist" style="opacity:${op}" title="${h.ago} ago: ${h.label} (${h.score})">` +
                    _regimeBarHtml(h.score, 'sm') +
                    `<span class="rb-hist-label">${h.ago}</span></span>`;
            }).join('');

            el.innerHTML = `
                <div class="rb-main">
                    <span class="rb-label">오늘 레짐</span>
                    <span class="rb-regime ${cls}">${label}</span>
                    <span class="rb-score">${score}</span>
                    ${_regimeBarHtml(score, 'lg')}
                    <span class="rb-detail">${kospi} | ${breadth}</span>
                </div>
                ${histHtml ? `<div class="rb-history">${histHtml}</div>` : ''}
            `;
            el.style.display = 'flex';
        } catch(e) {}
    }
    fetchRegime();
    setInterval(fetchRegime, 300000);

    // ── Init ─────────────────────────────────────────────────
    async function init() {
        startClock();
        await loadParams();
        buildParamUI();
        await loadRanking();

        $btnSimulate.addEventListener('click', runSimulation);
        $btnRealtime.addEventListener('click', startRealtime);
        $btnRtStop.addEventListener('click', stopRealtime);
        $btnReset.addEventListener('click', resetParams);
        $btnRefresh.addEventListener('click', loadRanking);

        // Check if a realtime sim is already running (page reload)
        checkRealtimeState();
    }

    // ── Clock ────────────────────────────────────────────────
    function startClock() {
        function tick() {
            const now = new Date();
            $clock.textContent = now.toLocaleTimeString('ko-KR', { hour12: false });
        }
        tick();
        setInterval(tick, 1000);
    }

    // ── Load Params ──────────────────────────────────────────
    async function loadParams() {
        try {
            const resp = await fetch('/api/lab/params');
            const data = await resp.json();
            currentParams = { ...data.defaults };
            paramRanges = data.ranges;
        } catch (e) {
            console.error('Failed to load params:', e);
            // Hardcoded fallback
            currentParams = {
                ranking_source: '등락률', top_n: 20, entry_threshold: 3.0,
                exit_target_a: 1.0, stop_loss_a: -0.5,
                exit_target_b: 2.0, stop_loss_b: -1.0,
                exit_target_c: 1.5, trail_max_c: 6.0,
                max_positions: 5, position_size_pct: 20.0, price_min: 5000,
            };
        }
    }

    // ── Build Param UI ───────────────────────────────────────
    function buildParamUI() {
        $paramGrid.innerHTML = '';
        const keys = Object.keys(currentParams);

        for (const key of keys) {
            const range = paramRanges[key] || {};
            const label = PARAM_LABELS[key] || key;
            const value = currentParams[key];

            const item = document.createElement('div');
            item.className = 'param-item';

            if (range.type === 'select') {
                item.innerHTML = `
                    <div class="param-header">
                        <span class="param-label">${label}</span>
                        <span class="param-value-display" id="pv-${key}">${value}</span>
                    </div>
                    <div class="param-input-wrap">
                        <select id="pi-${key}" data-key="${key}">
                            ${(range.options || []).map(o =>
                                `<option value="${o}" ${o === value ? 'selected' : ''}>${o}</option>`
                            ).join('')}
                        </select>
                    </div>
                `;
            } else {
                const min = range.min ?? 0;
                const max = range.max ?? 100;
                const step = range.step ?? 1;
                const unit = range.unit || '';
                const displayVal = formatParamValue(value, unit);

                item.innerHTML = `
                    <div class="param-header">
                        <span class="param-label">${label}</span>
                        <span class="param-value-display" id="pv-${key}">${displayVal}</span>
                    </div>
                    <div class="param-input-wrap">
                        <input type="range" id="pi-${key}" data-key="${key}"
                               min="${min}" max="${max}" step="${step}" value="${value}">
                    </div>
                `;
            }

            $paramGrid.appendChild(item);

            // Bind event
            const input = item.querySelector(`#pi-${key}`);
            input.addEventListener('input', (e) => {
                const k = e.target.dataset.key;
                let v = e.target.value;
                const r = paramRanges[k] || {};
                if (r.type !== 'select') {
                    v = parseFloat(v);
                }
                currentParams[k] = v;
                const display = document.getElementById(`pv-${k}`);
                if (display) {
                    display.textContent = r.type === 'select' ? v : formatParamValue(v, r.unit || '');
                }
            });
        }
    }

    function formatParamValue(val, unit) {
        if (unit === '원') return Number(val).toLocaleString() + '원';
        if (unit === '%') return val + '%';
        return String(val);
    }

    function resetParams() {
        loadParams().then(() => buildParamUI());
    }

    // ── Load Ranking ─────────────────────────────────────────
    async function loadRanking() {
        $rankingTitle.textContent = '순위 종목 (로딩 중...)';
        $rankingList.innerHTML = '<div style="padding:32px;text-align:center;color:var(--text-dim)"><span class="loading-spinner"></span>데이터 조회 중...</div>';

        try {
            const source = currentParams.ranking_source || '등락률';
            const topN = currentParams.top_n || 20;
            const resp = await fetch(`/api/lab/ranking?source=${encodeURIComponent(source)}&top_n=${topN}`);
            const data = await resp.json();
            currentRanking = data.ranking || [];
            const fallback = data.fallback ? ' (데모)' : '';
            $rankingTitle.textContent = `${source} 상위 ${currentRanking.length}종목${fallback}`;
            renderRanking();
        } catch (e) {
            $rankingTitle.textContent = '순위 종목 (오류)';
            $rankingList.innerHTML = '<div style="padding:32px;text-align:center;color:var(--red)">데이터 조회 실패</div>';
        }
    }

    function renderRanking() {
        $rankingList.innerHTML = '';
        for (const stock of currentRanking) {
            const isPositive = stock.change_pct > 0;
            const changeClass = isPositive ? 'positive' : (stock.change_pct < 0 ? 'negative' : '');
            const sign = isPositive ? '+' : '';
            const rankClass = stock.rank <= 3 ? 'top3' : '';

            const el = document.createElement('div');
            el.className = 'ranking-item';
            el.innerHTML = `
                <span class="ranking-rank ${rankClass}">${stock.rank}</span>
                <div class="ranking-info">
                    <div class="ranking-name">${stock.name}</div>
                    <div class="ranking-code">${stock.code}</div>
                </div>
                <div style="text-align:right">
                    <div class="ranking-change ${changeClass}">${sign}${stock.change_pct}%</div>
                    <div class="ranking-price">${Number(stock.price).toLocaleString()}원</div>
                </div>
            `;
            $rankingList.appendChild(el);
        }
    }

    // ── Run Simulation ───────────────────────────────────────
    async function runSimulation() {
        if (isSimulating) return;
        isSimulating = true;
        $btnSimulate.disabled = true;
        $simStatus.innerHTML = '<span class="loading-spinner"></span>시뮬레이션 실행 중...';

        try {
            const resp = await fetch('/api/lab/simulate', {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify({
                    params: currentParams,
                    ranking: currentRanking.length > 0 ? currentRanking : null,
                }),
            });
            const data = await resp.json();
            renderResults(data);
            $simStatus.textContent = '완료';
        } catch (e) {
            $simStatus.textContent = '오류: ' + e.message;
        } finally {
            isSimulating = false;
            $btnSimulate.disabled = false;
        }
    }

    // ── Render Results ───────────────────────────────────────
    function renderResults(data) {
        $resultsSection.hidden = false;
        $resultTs.textContent = data.timestamp || '';

        const strategies = data.strategies || [];
        // Find best strategy by total_pnl
        let bestIdx = 0;
        let bestPnl = -Infinity;
        strategies.forEach((s, i) => {
            if (s.total_pnl > bestPnl) {
                bestPnl = s.total_pnl;
                bestIdx = i;
            }
        });

        $strategyGrid.innerHTML = '';
        strategies.forEach((s, i) => {
            const card = document.createElement('div');
            card.className = 'strategy-card' + (i === bestIdx ? ' best' : '');

            const pnlClass = s.total_pnl > 0 ? 'positive' : (s.total_pnl < 0 ? 'negative' : 'neutral');
            const pnlSign = s.total_pnl > 0 ? '+' : '';
            const pnlPct = data.initial_cash > 0 ? (s.total_pnl / data.initial_cash * 100).toFixed(2) : '0.00';
            const pnlPctSign = s.total_pnl > 0 ? '+' : '';

            const totalTrades = s.win_count + s.loss_count;
            const sellTrades = s.trades ? s.trades.filter(t => t.side === 'SELL') : [];

            card.innerHTML = `
                <div class="strategy-header">
                    <div>
                        <div class="strategy-name">STRATEGY ${s.name}</div>
                        <div class="strategy-label">${s.label}</div>
                    </div>
                    ${i === bestIdx ? '<span class="strategy-badge-best">BEST</span>' : ''}
                </div>
                <div class="strategy-body">
                    <div class="strategy-pnl ${pnlClass}">${pnlSign}${Number(s.total_pnl).toLocaleString()}원</div>
                    <div class="strategy-pnl-pct ${pnlClass}">${pnlPctSign}${pnlPct}%</div>
                    <div class="strategy-stats">
                        <div class="stat-item">
                            <div class="stat-value">${totalTrades}</div>
                            <div class="stat-label">거래</div>
                        </div>
                        <div class="stat-item">
                            <div class="stat-value">${s.win_rate}%</div>
                            <div class="stat-label">승률</div>
                        </div>
                        <div class="stat-item">
                            <div class="stat-value">${Number(s.total_value).toLocaleString()}</div>
                            <div class="stat-label">평가액</div>
                        </div>
                    </div>
                    <button class="trade-toggle" onclick="this.nextElementSibling.hidden = !this.nextElementSibling.hidden">
                        거래내역 (${sellTrades.length}건)
                    </button>
                    <div class="trade-list" hidden>
                        ${renderTradeList(s.trades || [])}
                    </div>
                    ${s.positions && s.positions.length > 0 ? `
                        <div style="margin-top:10px;font-size:11px;color:var(--text-dim)">
                            보유 중: ${s.positions.length}종목
                        </div>
                    ` : ''}
                </div>
            `;
            $strategyGrid.appendChild(card);
        });

        // Scroll to results
        $resultsSection.scrollIntoView({ behavior: 'smooth', block: 'start' });
    }

    function renderTradeList(trades) {
        if (!trades || trades.length === 0) {
            return '<div style="padding:16px;text-align:center;color:var(--text-dim);font-size:12px">거래 없음</div>';
        }

        return trades
            .filter(t => t.side === 'SELL')
            .map(t => {
                const pnlClass = t.pnl > 0 ? 'positive' : (t.pnl < 0 ? 'negative' : 'neutral');
                const pnlSign = t.pnl > 0 ? '+' : '';
                const reasonClass = (t.reason || '').toLowerCase();

                return `
                    <div class="trade-item">
                        <div class="trade-item-left">
                            <span class="trade-name">${t.name}</span>
                            <span class="trade-meta">${t.code} | ${Number(t.price).toLocaleString()}원 x ${t.qty}주</span>
                        </div>
                        <div class="trade-item-right">
                            <div class="trade-pnl ${pnlClass}">${pnlSign}${Number(t.pnl).toLocaleString()}원</div>
                            <span class="trade-reason ${reasonClass}">${t.reason || '-'} ${t.pnl_pct ? `(${pnlSign}${t.pnl_pct}%)` : ''}</span>
                        </div>
                    </div>
                `;
            }).join('');
    }

    // ── Realtime Simulation ────────────────────────────────────

    async function checkRealtimeState() {
        try {
            const resp = await fetch('/api/lab/realtime/state');
            const data = await resp.json();
            if (data.running) {
                isRealtimeRunning = true;
                showRealtimeUI(true);
                connectRealtimeSSE();
            }
        } catch (e) {
            // Not running, that's fine
        }
    }

    async function startRealtime() {
        if (isRealtimeRunning || isSimulating) return;

        if (currentRanking.length === 0) {
            $simStatus.textContent = '먼저 순위 데이터를 조회하세요';
            return;
        }

        $btnRealtime.disabled = true;
        $simStatus.innerHTML = '<span class="loading-spinner"></span>실시간 시뮬레이션 시작 중...';

        try {
            const resp = await fetch('/api/lab/realtime/start', {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify({
                    params: currentParams,
                    ranking: currentRanking,
                }),
            });
            const data = await resp.json();

            if (data.error) {
                $simStatus.textContent = '오류: ' + data.error;
                $btnRealtime.disabled = false;
                return;
            }

            isRealtimeRunning = true;
            previousClosedCounts = {};
            $simStatus.textContent = `실시간 구독: ${(data.codes || []).length}종목`;
            showRealtimeUI(true);
            connectRealtimeSSE();
        } catch (e) {
            $simStatus.textContent = '오류: ' + e.message;
            $btnRealtime.disabled = false;
        }
    }

    async function stopRealtime() {
        if (!isRealtimeRunning) return;

        try {
            const resp = await fetch('/api/lab/realtime/stop', { method: 'POST' });
            const data = await resp.json();

            isRealtimeRunning = false;
            showRealtimeUI(false);
            disconnectRealtimeSSE();

            if (data.result) {
                renderResults(data.result);
            }
            $simStatus.textContent = '실시간 시뮬레이션 종료';
        } catch (e) {
            $simStatus.textContent = '중지 오류: ' + e.message;
        }
    }

    function showRealtimeUI(show) {
        $realtimeSection.hidden = !show;
        $btnRealtime.disabled = show;
        $btnSimulate.disabled = show;
        if (show) {
            $btnRealtime.classList.add('running');
            $realtimeSection.scrollIntoView({ behavior: 'smooth', block: 'start' });
        } else {
            $btnRealtime.classList.remove('running');
            $btnRealtime.disabled = false;
            $btnSimulate.disabled = false;
        }
    }

    function connectRealtimeSSE() {
        if (realtimeSSE) {
            realtimeSSE.close();
        }
        realtimeSSE = new EventSource('/sse/lab');
        realtimeSSE.addEventListener('lab', (e) => {
            try {
                const data = JSON.parse(e.data);
                renderRealtimeState(data);

                if (!data.running && isRealtimeRunning) {
                    // Server stopped the sim (market close, etc.)
                    isRealtimeRunning = false;
                    showRealtimeUI(false);
                    disconnectRealtimeSSE();
                    $simStatus.textContent = '실시간 시뮬레이션 자동 종료';
                }
            } catch (err) {
                console.error('SSE parse error:', err);
            }
        });
        realtimeSSE.onerror = () => {
            console.warn('Lab SSE connection error, retrying...');
        };
    }

    function disconnectRealtimeSSE() {
        if (realtimeSSE) {
            realtimeSSE.close();
            realtimeSSE = null;
        }
    }

    function renderRealtimeState(data) {
        // Timer
        const elapsed = data.elapsed_sec || 0;
        const mins = Math.floor(elapsed / 60);
        const secs = Math.floor(elapsed % 60);
        $rtTimer.textContent = String(mins).padStart(2, '0') + ':' + String(secs).padStart(2, '0');

        // Tick count
        $rtTickCount.textContent = (data.tick_count || 0).toLocaleString() + ' ticks';

        // Strategy cards
        const strategies = data.strategies || [];
        // Find best by total_pnl
        let bestIdx = 0;
        let bestPnl = -Infinity;
        strategies.forEach((s, i) => {
            if (s.total_pnl > bestPnl) {
                bestPnl = s.total_pnl;
                bestIdx = i;
            }
        });

        $rtStrategyGrid.innerHTML = '';
        strategies.forEach((s, i) => {
            const card = document.createElement('div');
            card.className = 'strategy-card' + (i === bestIdx ? ' best' : '');

            const pnlClass = s.total_pnl > 0 ? 'positive' : (s.total_pnl < 0 ? 'negative' : 'neutral');
            const pnlSign = s.total_pnl > 0 ? '+' : '';
            const initialCash = data.initial_cash || 10000000;
            const pnlPct = initialCash > 0 ? (s.total_pnl / initialCash * 100).toFixed(2) : '0.00';
            const pnlPctSign = s.total_pnl > 0 ? '+' : '';
            const totalTrades = (s.win_count || 0) + (s.loss_count || 0);

            // Detect new closed trades for flash
            const prevCount = previousClosedCounts[s.name] || 0;
            const newClosed = s.closed_count > prevCount;
            previousClosedCounts[s.name] = s.closed_count || 0;

            card.innerHTML = `
                <div class="strategy-header">
                    <div>
                        <div class="strategy-name">STRATEGY ${s.name}</div>
                        <div class="strategy-label">${s.label}</div>
                    </div>
                    ${i === bestIdx ? '<span class="strategy-badge-best">BEST</span>' : ''}
                </div>
                <div class="strategy-body">
                    <div class="strategy-pnl ${pnlClass}">${pnlSign}${Number(s.total_pnl).toLocaleString()}원</div>
                    <div class="strategy-pnl-pct ${pnlClass}">${pnlPctSign}${pnlPct}%</div>
                    <div class="strategy-stats">
                        <div class="stat-item">
                            <div class="stat-value">${s.open_count || 0}</div>
                            <div class="stat-label">보유</div>
                        </div>
                        <div class="stat-item">
                            <div class="stat-value">${totalTrades}</div>
                            <div class="stat-label">청산</div>
                        </div>
                        <div class="stat-item">
                            <div class="stat-value">${s.win_rate || 0}%</div>
                            <div class="stat-label">승률</div>
                        </div>
                    </div>
                    <div class="rt-positions">
                        ${renderRealtimePositions(s.open_positions || [])}
                    </div>
                    ${(s.closed_trades && s.closed_trades.length > 0) ? `
                        <div class="rt-closed-header">청산 ${s.closed_trades.length}건 | 실현 ${pnlSign}${Number(s.realized_pnl || 0).toLocaleString()}원</div>
                        <div class="trade-list" style="max-height:150px;">
                            ${renderClosedTrades(s.closed_trades)}
                        </div>
                    ` : ''}
                </div>
            `;
            $rtStrategyGrid.appendChild(card);
        });

        // Events
        renderRealtimeEvents(data.events || []);
    }

    function renderRealtimePositions(positions) {
        if (!positions || positions.length === 0) {
            return '<div style="padding:12px;text-align:center;color:var(--text-dim);font-size:11px">포지션 없음</div>';
        }

        return positions.map(p => {
            const pnlClass = p.unrealized_pnl > 0 ? 'positive' : (p.unrealized_pnl < 0 ? 'negative' : 'neutral');
            const pnlSign = p.unrealized_pnl > 0 ? '+' : '';
            const pctSign = p.unrealized_pnl_pct > 0 ? '+' : '';

            // How close to TP or SL (for bar visualization)
            const range = (p.tp_price || p.entry_price) - (p.sl_price || p.entry_price);
            const current = p.current_price - (p.sl_price || p.entry_price);
            const barPct = range > 0 ? Math.min(Math.max(current / range * 100, 0), 100) : 50;
            const barClass = p.unrealized_pnl >= 0 ? 'positive' : 'negative';

            // Proximity detection
            let posClass = '';
            if (p.tp_price > 0 && p.current_price >= p.tp_price * 0.995) posClass = 'tp-near';
            if (p.sl_price > 0 && p.current_price <= p.sl_price * 1.005) posClass = 'sl-near';

            return `
                <div class="rt-pos-item ${posClass}">
                    <div class="rt-pos-left">
                        <span class="rt-pos-name">${p.name}</span>
                        <div class="rt-triggers">
                            <span class="rt-trigger-tp">TP ${Number(p.tp_price || 0).toLocaleString()}</span>
                            <span class="rt-trigger-sl">SL ${Number(p.sl_price || 0).toLocaleString()}</span>
                        </div>
                    </div>
                    <div class="rt-pos-center">
                        <span class="rt-pos-price">${Number(p.current_price).toLocaleString()}</span>
                        <div class="rt-pos-bar">
                            <div class="rt-pos-bar-fill ${barClass}" style="width:${barPct}%"></div>
                        </div>
                    </div>
                    <div class="rt-pos-right">
                        <div class="rt-pos-pnl ${pnlClass}">${pnlSign}${Number(p.unrealized_pnl).toLocaleString()}</div>
                        <div class="rt-pos-pnl-pct">${pctSign}${p.unrealized_pnl_pct}%</div>
                    </div>
                </div>
            `;
        }).join('');
    }

    function renderClosedTrades(trades) {
        if (!trades || trades.length === 0) return '';
        return trades.map(t => {
            const pnlClass = t.pnl > 0 ? 'positive' : (t.pnl < 0 ? 'negative' : 'neutral');
            const pnlSign = t.pnl > 0 ? '+' : '';
            const reasonClass = (t.reason || '').toLowerCase();
            return `
                <div class="trade-item">
                    <div class="trade-item-left">
                        <span class="trade-name">${t.name}</span>
                        <span class="trade-meta">${t.code} | ${Number(t.price).toLocaleString()}원 x ${t.qty}주</span>
                    </div>
                    <div class="trade-item-right">
                        <div class="trade-pnl ${pnlClass}">${pnlSign}${Number(t.pnl).toLocaleString()}원</div>
                        <span class="trade-reason ${reasonClass}">${t.reason || '-'} ${t.pnl_pct ? `(${pnlSign}${t.pnl_pct}%)` : ''}</span>
                    </div>
                </div>
            `;
        }).join('');
    }

    function renderRealtimeEvents(events) {
        if (!events || events.length === 0) {
            $rtEvents.innerHTML = '<div style="padding:12px;text-align:center;color:var(--text-dim);font-size:11px">이벤트 대기 중...</div>';
            return;
        }

        // Reverse so newest is at top
        const reversed = [...events].reverse();
        $rtEvents.innerHTML = reversed.map(ev => {
            const typeClass = (ev.type || '').toLowerCase().replace(/_/g, '_');
            return `
                <div class="rt-event">
                    <span class="rt-event-time">${ev.time || ''}</span>
                    <span class="rt-event-type ${typeClass}">${ev.type || ''}</span>
                    <span class="rt-event-msg">${ev.message || ''}</span>
                </div>
            `;
        }).join('');
    }

    // ── Start ────────────────────────────────────────────────
    init();
})();
