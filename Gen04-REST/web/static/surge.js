/* ================================================================
   Q-TRON SURGE Simulator — 3-Strategy Comparison Mode
   A: 기본 (등락률만), B: +거래량급증, C: +거래량급증+체결강도
   ================================================================ */

let params = {};
let defaults = {};
let ranges = {};
let sse = null;
let running = false;

const LANE_COLORS = { A: '#58a6ff', B: '#00c853', C: '#ff9100' };

// ── Clock ────────────────────────────────────────────────────
function updateClock() {
    const now = new Date();
    document.getElementById('clock').textContent =
        `${String(now.getHours()).padStart(2,'0')}:${String(now.getMinutes()).padStart(2,'0')}:${String(now.getSeconds()).padStart(2,'0')}`;
}
setInterval(updateClock, 1000);
updateClock();

// ── Init ─────────────────────────────────────────────────────
async function init() {
    try {
        const resp = await fetch('/api/surge/params');
        const data = await resp.json();
        defaults = data.defaults || {};
        ranges = data.ranges || {};
        params = { ...defaults };
        renderParams();
    } catch (e) {
        document.getElementById('sim-status').textContent = 'API 연결 실패';
    }
    try {
        const resp = await fetch('/api/surge/state');
        const state = await resp.json();
        if (state.running) { running = true; showRunning(); connectSSE(); }
    } catch (e) {}
}

function renderParams() {
    const grid = document.getElementById('param-grid');
    grid.innerHTML = '';
    const paramOrder = [
        'ranking_source','ranking_top_n','min_change_pct','min_price',
        'tp_pct','sl_pct','max_hold_sec','cooldown_sec',
        'fill_safety_k','max_daily_entries','max_concurrent',
        'max_loss_per_stock','consecutive_loss_halt','max_tr_lag_sec',
        'initial_cash','per_trade_pct','scan_interval_sec',
    ];
    const labels = {
        ranking_source:'랭킹 소스', ranking_top_n:'상위 N개',
        min_change_pct:'최소 등락률', min_price:'최소 가격',
        tp_pct:'TP %', sl_pct:'SL %',
        max_hold_sec:'최대 보유(초)', cooldown_sec:'쿨다운(초)',
        fill_safety_k:'체결 안전계수', max_daily_entries:'일일 최대',
        max_concurrent:'동시 보유', max_loss_per_stock:'종목별 손실 한도',
        consecutive_loss_halt:'연속 손실 중지', max_tr_lag_sec:'TR 최대 지연',
        initial_cash:'초기 자본', per_trade_pct:'1회 비율',
        scan_interval_sec:'스캔 주기',
    };
    for (const key of paramOrder) {
        const r = ranges[key]; if (!r) continue;
        const item = document.createElement('div');
        item.className = 'surge-param-item';
        const label = document.createElement('label');
        label.textContent = labels[key] || key;
        item.appendChild(label);
        if (r.type === 'select') {
            const sel = document.createElement('select');
            sel.id = `p-${key}`;
            for (const opt of r.options) {
                const o = document.createElement('option');
                o.value = opt; o.textContent = opt;
                if (opt === params[key]) o.selected = true;
                sel.appendChild(o);
            }
            sel.addEventListener('change', () => { params[key] = sel.value; });
            item.appendChild(sel);
        } else if (r.type === 'range') {
            const row = document.createElement('div');
            row.className = 'surge-param-row';
            const input = document.createElement('input');
            input.type = 'range'; input.id = `p-${key}`;
            input.min = r.min; input.max = r.max; input.step = r.step; input.value = params[key];
            const val = document.createElement('span');
            val.className = 'param-value'; val.id = `pv-${key}`;
            val.textContent = fmtParam(params[key], r);
            input.addEventListener('input', () => { params[key] = Number(input.value); val.textContent = fmtParam(params[key], r); });
            row.appendChild(input); row.appendChild(val);
            item.appendChild(row);
        }
        grid.appendChild(item);
    }
}

function fmtParam(v, r) {
    if (r.unit === '원') return v >= 1e6 ? `${(v/1e6).toFixed(0)}M` : v.toLocaleString();
    if (r.unit === '%') return `${v}%`;
    if (r.unit === '초') return `${v}s`;
    return String(v);
}

function resetParams() { params = { ...defaults }; renderParams(); }

// ── Start / Stop ─────────────────────────────────────────────
async function startSim() {
    document.getElementById('btn-start').disabled = true;
    document.getElementById('sim-status').textContent = '시작 중...';
    try {
        const resp = await fetch('/api/surge/start', {
            method: 'POST', headers: {'Content-Type':'application/json'},
            body: JSON.stringify({ params }),
        });
        const result = await resp.json();
        if (result.error) {
            document.getElementById('sim-status').textContent = `오류: ${result.error}`;
            document.getElementById('btn-start').disabled = false;
            return;
        }
        running = true; showRunning(); connectSSE();
    } catch (e) {
        document.getElementById('sim-status').textContent = `연결 실패: ${e.message}`;
        document.getElementById('btn-start').disabled = false;
    }
}

async function stopSim() {
    document.getElementById('sim-status').textContent = '중지 중...';
    try {
        const resp = await fetch('/api/surge/stop', { method: 'POST' });
        const result = await resp.json();
        running = false; showStopped();
        if (sse) { sse.close(); sse = null; }
        if (result.strategies) {
            const labels = result.strategies.map(s => `${s.strategy}:${s.total_trades||0}건`).join(', ');
            document.getElementById('sim-status').textContent = `종료 — ${labels}`;
        }
    } catch (e) {
        document.getElementById('sim-status').textContent = `중지 실패: ${e.message}`;
    }
}

function showRunning() {
    document.getElementById('btn-start').style.display = 'none';
    document.getElementById('btn-stop').style.display = 'inline-block';
    document.getElementById('live-dashboard').style.display = 'block';
    document.getElementById('sim-status').textContent = '실행 중';
}
function showStopped() {
    document.getElementById('btn-start').style.display = 'inline-block';
    document.getElementById('btn-start').disabled = false;
    document.getElementById('btn-stop').style.display = 'none';
}

// ── SSE ──────────────────────────────────────────────────────
function connectSSE() {
    if (sse) sse.close();
    sse = new EventSource('/sse/surge');
    sse.addEventListener('surge', e => {
        try { renderState(JSON.parse(e.data)); } catch(err) {}
    });
    sse.addEventListener('heartbeat', () => {});
    sse.addEventListener('error', () => {
        setTimeout(() => { if (running) connectSSE(); }, 3000);
    });
}

// ── Render State ─────────────────────────────────────────────
function renderState(data) {
    if (!data.running && running) { running = false; showStopped(); return; }

    // Timer
    const e = data.elapsed_sec || 0;
    document.getElementById('timer').textContent =
        `${String(Math.floor(e/60)).padStart(2,'0')}:${String(Math.floor(e%60)).padStart(2,'0')}`;
    document.getElementById('tick-count').textContent = `${(data.tick_count||0).toLocaleString()} ticks`;

    // 3-Strategy Cards
    renderStrategyCards(data.strategies || []);

    // Candidates
    renderCandidates(data.candidates || []);

    // Events
    renderEvents(data.events || []);
}

function renderStrategyCards(strategies) {
    const container = document.getElementById('strategy-cards');
    container.innerHTML = strategies.map(s => {
        const color = LANE_COLORS[s.name] || '#58a6ff';
        const risk = s.risk || {};
        const sum = s.summary || {};
        const winRate = sum.win_rate || 0;
        const totalPnl = sum.total_pnl_pct || 0;
        const pnlClass = totalPnl >= 0 ? 'pnl-pos' : 'pnl-neg';

        // Positions table
        const posRows = (s.positions || []).map(p => {
            const pc = p.pnl_pct >= 0 ? 'pnl-pos' : 'pnl-neg';
            const hold = p.holding_sec >= 60 ? `${Math.floor(p.holding_sec/60)}m` : `${Math.floor(p.holding_sec)}s`;
            return `<tr>
                <td>${p.code}</td>
                <td class="${pc}">${p.pnl_pct>=0?'+':''}${p.pnl_pct.toFixed(2)}%</td>
                <td>${hold}</td>
            </tr>`;
        }).join('') || '<tr><td colspan="3" style="color:var(--text-dim);text-align:center">-</td></tr>';

        // Recent trades
        const tradeRows = (s.trades || []).slice(-5).reverse().map(t => {
            const ec = {'TP':'exit-tp','SL':'exit-sl','TIME_EXIT':'exit-time','FORCE_EXIT':'exit-force'}[t.exit_reason]||'';
            const nc = t.net_pnl_pct >= 0 ? 'pnl-pos' : 'pnl-neg';
            return `<tr>
                <td>${t.code}</td>
                <td class="${ec}">${t.exit_reason}</td>
                <td class="${nc}">${t.net_pnl_pct>=0?'+':''}${(t.net_pnl_pct||0).toFixed(2)}%</td>
            </tr>`;
        }).join('') || '<tr><td colspan="3" style="color:var(--text-dim);text-align:center">-</td></tr>';

        return `<section class="surge-section" style="border-top: 3px solid ${color};">
            <div class="section-title">
                <span style="color:${color};font-weight:700;">전략 ${s.name}</span>
                <span style="font-size:11px;color:var(--text-secondary)">${s.label}</span>
            </div>
            <div class="summary-grid">
                <div class="summary-card"><div class="s-label">거래</div><div class="s-value">${s.trade_count||0}</div></div>
                <div class="summary-card"><div class="s-label">승률</div><div class="s-value">${winRate.toFixed(1)}%</div></div>
                <div class="summary-card"><div class="s-label">Net PnL</div><div class="s-value ${pnlClass}">${totalPnl>=0?'+':''}${totalPnl.toFixed(2)}%</div></div>
                <div class="summary-card"><div class="s-label">잔고</div><div class="s-value">${(s.cash||0).toLocaleString()}</div></div>
                <div class="summary-card"><div class="s-label">동시보유</div><div class="s-value">${risk.concurrent||0}</div></div>
                <div class="summary-card"><div class="s-label">연속손실</div><div class="s-value">${risk.consecutive_losses||0}</div></div>
            </div>
            <div style="margin-top:8px;">
                <div style="font-size:10px;color:var(--text-dim);margin-bottom:4px;">보유 포지션</div>
                <div class="surge-scroll-sm">
                    <table class="surge-table"><thead><tr><th>종목</th><th>PnL%</th><th>보유</th></tr></thead>
                    <tbody>${posRows}</tbody></table>
                </div>
            </div>
            <div style="margin-top:8px;">
                <div style="font-size:10px;color:var(--text-dim);margin-bottom:4px;">최근 거래</div>
                <div class="surge-scroll-sm">
                    <table class="surge-table"><thead><tr><th>종목</th><th>사유</th><th>Net%</th></tr></thead>
                    <tbody>${tradeRows}</tbody></table>
                </div>
            </div>
        </section>`;
    }).join('');
}

function renderCandidates(candidates) {
    const tbody = document.getElementById('candidates-body');
    if (!candidates.length) {
        tbody.innerHTML = '<tr><td colspan="6" style="text-align:center;color:var(--text-dim)">후보 없음</td></tr>';
        return;
    }
    tbody.innerHTML = candidates.map(c => {
        const volBadge = c.volume_surge
            ? `<span style="color:var(--green);font-weight:600;">+${(c.volume_surge_pct||0).toFixed(0)}%</span>`
            : '<span style="color:var(--text-dim);">-</span>';
        const strBadge = c.strength_pass
            ? `<span style="color:var(--orange);font-weight:600;">${(c.strength||0).toFixed(0)}</span>`
            : `<span style="color:var(--text-dim);">${(c.strength||0).toFixed(0) || '-'}</span>`;
        return `<tr>
            <td>${c.rank}</td>
            <td>${c.code} <span style="font-size:10px;color:var(--text-dim)">${c.name||''}</span></td>
            <td>${(c.price||0).toLocaleString()}</td>
            <td style="color:var(--green)">+${c.change_pct}%</td>
            <td>${volBadge}</td>
            <td>${strBadge}</td>
        </tr>`;
    }).join('');
}

function renderEvents(events) {
    const list = document.getElementById('event-list');
    if (!events.length) {
        list.innerHTML = '<div style="color:var(--text-dim);font-size:11px;padding:8px;">이벤트 대기 중</div>';
        return;
    }
    list.innerHTML = events.slice().reverse().slice(0, 30).map(e => {
        const tag = e.tag || '';
        let tagClass = 'tag-info';
        if (tag.includes('FILLED') || tag.includes('TP')) tagClass = 'tag-entry';
        if (tag.includes('SL') || tag.includes('EXIT')) tagClass = 'tag-exit';
        if (tag.includes('BLOCKED') || tag.includes('SKIP') || tag.includes('STOP') || tag.includes('DUPLICATE')) tagClass = 'tag-blocked';
        const time = (e.timestamp || '').slice(11, 19);
        const reason = e.trigger_reason || '';
        const pnl = e.pnl_pct ? ` pnl=${e.pnl_pct.toFixed(2)}%` : '';
        return `<div class="event-item">
            <span class="event-time">${time}</span>
            <span class="event-tag ${tagClass}">${tag.replace('SURGE_','').slice(0,14)}</span>
            <span class="event-code">${e.code||''}</span>
            <span class="event-msg">${reason}${pnl}</span>
        </div>`;
    }).join('');
}

init();
