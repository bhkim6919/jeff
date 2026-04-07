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

    // ── DOM refs ─────────────────────────────────────────────
    const $paramGrid = document.getElementById('param-grid');
    const $btnSimulate = document.getElementById('btn-simulate');
    const $btnReset = document.getElementById('btn-reset');
    const $btnRefresh = document.getElementById('btn-refresh-ranking');
    const $resultsSection = document.getElementById('results-section');
    const $strategyGrid = document.getElementById('strategy-grid');
    const $rankingList = document.getElementById('ranking-list');
    const $rankingTitle = document.getElementById('ranking-title');
    const $resultTs = document.getElementById('result-ts');
    const $simStatus = document.getElementById('sim-status');
    const $clock = document.getElementById('clock');

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

    // ── Init ─────────────────────────────────────────────────
    async function init() {
        startClock();
        await loadParams();
        buildParamUI();
        await loadRanking();

        $btnSimulate.addEventListener('click', runSimulation);
        $btnReset.addEventListener('click', resetParams);
        $btnRefresh.addEventListener('click', loadRanking);
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

    // ── Start ────────────────────────────────────────────────
    init();
})();
