/* ================================================================
   Q-TRON SURGE Simulator — Client-side JS
   SSE + fetch API, vanilla JS (no framework)
   ================================================================ */

let params = {};
let defaults = {};
let ranges = {};
let sse = null;
let running = false;
let pnlHistory = [];

// ── Clock ────────────────────────────────────────────────────

function updateClock() {
    const now = new Date();
    const hh = String(now.getHours()).padStart(2, '0');
    const mm = String(now.getMinutes()).padStart(2, '0');
    const ss = String(now.getSeconds()).padStart(2, '0');
    document.getElementById('clock').textContent = `${hh}:${mm}:${ss}`;
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
        console.error('Failed to load params:', e);
        document.getElementById('sim-status').textContent = 'API 연결 실패';
    }

    // Check if already running
    try {
        const resp = await fetch('/api/surge/state');
        const state = await resp.json();
        if (state.running) {
            running = true;
            showRunning();
            connectSSE();
        }
    } catch (e) {}
}

function renderParams() {
    const grid = document.getElementById('param-grid');
    grid.innerHTML = '';

    const paramOrder = [
        'ranking_source', 'ranking_top_n', 'min_change_pct', 'min_price',
        'tp_pct', 'sl_pct', 'max_hold_sec', 'cooldown_sec',
        'fill_safety_k', 'max_daily_entries', 'max_concurrent',
        'max_loss_per_stock', 'consecutive_loss_halt', 'max_tr_lag_sec',
        'initial_cash', 'per_trade_pct', 'scan_interval_sec',
    ];

    const labels = {
        ranking_source: '랭킹 소스', ranking_top_n: '상위 N개',
        min_change_pct: '최소 등락률', min_price: '최소 가격',
        tp_pct: 'TP %', sl_pct: 'SL %',
        max_hold_sec: '최대 보유(초)', cooldown_sec: '쿨다운(초)',
        fill_safety_k: '체결 안전계수', max_daily_entries: '일일 최대',
        max_concurrent: '동시 보유', max_loss_per_stock: '종목별 손실 한도',
        consecutive_loss_halt: '연속 손실 중지', max_tr_lag_sec: 'TR 최대 지연',
        initial_cash: '초기 자본', per_trade_pct: '1회 비율',
        scan_interval_sec: '스캔 주기',
    };

    for (const key of paramOrder) {
        const r = ranges[key];
        if (!r) continue;

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
                o.value = opt;
                o.textContent = opt;
                if (opt === params[key]) o.selected = true;
                sel.appendChild(o);
            }
            sel.addEventListener('change', () => { params[key] = sel.value; });
            item.appendChild(sel);
        } else if (r.type === 'range') {
            const row = document.createElement('div');
            row.className = 'surge-param-row';

            const input = document.createElement('input');
            input.type = 'range';
            input.id = `p-${key}`;
            input.min = r.min;
            input.max = r.max;
            input.step = r.step;
            input.value = params[key];

            const val = document.createElement('span');
            val.className = 'param-value';
            val.id = `pv-${key}`;
            val.textContent = formatParamValue(params[key], r);

            input.addEventListener('input', () => {
                const v = Number(input.value);
                params[key] = v;
                val.textContent = formatParamValue(v, r);
            });

            row.appendChild(input);
            row.appendChild(val);
            item.appendChild(row);
        }

        grid.appendChild(item);
    }
}

function formatParamValue(v, r) {
    if (r.unit === '원') return v >= 1000000 ? `${(v / 1000000).toFixed(0)}M` : v.toLocaleString();
    if (r.unit === '%') return `${v}%`;
    if (r.unit === '초') return `${v}s`;
    return String(v);
}

function resetParams() {
    params = { ...defaults };
    renderParams();
}

// ── Start / Stop ─────────────────────────────────────────────

async function startSim() {
    const btn = document.getElementById('btn-start');
    btn.disabled = true;
    document.getElementById('sim-status').textContent = '시작 중...';

    try {
        const resp = await fetch('/api/surge/start', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({ params }),
        });
        const result = await resp.json();

        if (result.error) {
            document.getElementById('sim-status').textContent = `오류: ${result.error}`;
            btn.disabled = false;
            return;
        }

        running = true;
        pnlHistory = [];
        showRunning();
        connectSSE();
    } catch (e) {
        document.getElementById('sim-status').textContent = `연결 실패: ${e.message}`;
        btn.disabled = false;
    }
}

async function stopSim() {
    document.getElementById('sim-status').textContent = '중지 중...';
    try {
        const resp = await fetch('/api/surge/stop', { method: 'POST' });
        const result = await resp.json();
        running = false;
        showStopped();
        if (sse) { sse.close(); sse = null; }
        if (result.summary) {
            document.getElementById('sim-status').textContent =
                `종료 — ${result.summary.total_trades || 0}건, PnL ${result.summary.total_pnl_pct || 0}%`;
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

    sse.addEventListener('surge', (e) => {
        try {
            const data = JSON.parse(e.data);
            renderState(data);
        } catch (err) {
            console.error('SSE parse error:', err);
        }
    });

    sse.addEventListener('heartbeat', () => {});

    sse.addEventListener('error', () => {
        console.warn('SSE error, reconnecting in 3s...');
        setTimeout(() => {
            if (running) connectSSE();
        }, 3000);
    });
}

// ── Render State ─────────────────────────────────────────────

function renderState(data) {
    if (!data.running && running) {
        running = false;
        showStopped();
        return;
    }

    // Timer
    const elapsed = data.elapsed_sec || 0;
    const mm = String(Math.floor(elapsed / 60)).padStart(2, '0');
    const ss = String(Math.floor(elapsed % 60)).padStart(2, '0');
    document.getElementById('timer').textContent = `${mm}:${ss}`;
    document.getElementById('tick-count').textContent = `${(data.tick_count || 0).toLocaleString()} ticks`;

    // Risk cards
    const risk = data.risk || {};
    const cfg = params;
    document.getElementById('r-daily').textContent = `${risk.daily_entries || 0}/${cfg.max_daily_entries || 50}`;
    document.getElementById('r-concurrent').textContent = `${risk.concurrent || 0}/${cfg.max_concurrent || 5}`;
    document.getElementById('r-consec').textContent = risk.consecutive_losses || 0;
    const haltEl = document.getElementById('r-halt');
    haltEl.textContent = risk.halted ? 'ON' : 'OFF';
    document.getElementById('r-halt-card').className = `risk-card ${risk.halted ? 'risk-danger' : 'risk-ok'}`;
    document.getElementById('r-trades').textContent = data.trade_count || 0;
    document.getElementById('r-cash').textContent = (data.cash || 0).toLocaleString();

    // Positions
    renderPositions(data.positions || []);

    // Candidates
    renderCandidates(data.candidates || []);

    // Trades
    renderTrades(data.trades || []);

    // Events
    renderEvents(data.events || []);

    // PnL chart
    updatePnlChart(data.trades || []);
}

function renderPositions(positions) {
    const tbody = document.getElementById('positions-body');
    if (!positions.length) {
        tbody.innerHTML = '<tr><td colspan="7" style="text-align:center; color: var(--text-dim);">보유 없음</td></tr>';
        return;
    }
    tbody.innerHTML = positions.map(p => {
        const pnlClass = p.pnl_pct >= 0 ? 'pnl-pos' : 'pnl-neg';
        const holdStr = p.holding_sec >= 60 ? `${Math.floor(p.holding_sec / 60)}m${Math.floor(p.holding_sec % 60)}s` : `${Math.floor(p.holding_sec)}s`;
        return `<tr>
            <td>${p.code}<br><span style="font-size:10px;color:var(--text-dim)">${p.name || ''}</span></td>
            <td>${p.entry_price.toLocaleString()}</td>
            <td>${(p.bid || 0).toLocaleString()}</td>
            <td class="${pnlClass}">${p.pnl_pct >= 0 ? '+' : ''}${p.pnl_pct.toFixed(2)}%</td>
            <td style="color:var(--green);font-size:10px">${(p.tp_price || 0).toLocaleString()}</td>
            <td style="color:var(--red);font-size:10px">${(p.sl_price || 0).toLocaleString()}</td>
            <td>${holdStr}</td>
        </tr>`;
    }).join('');
}

function renderCandidates(candidates) {
    const tbody = document.getElementById('candidates-body');
    if (!candidates.length) {
        tbody.innerHTML = '<tr><td colspan="4" style="text-align:center; color: var(--text-dim);">후보 없음</td></tr>';
        return;
    }
    tbody.innerHTML = candidates.map(c => `<tr>
        <td>${c.rank}</td>
        <td>${c.code}<br><span style="font-size:10px;color:var(--text-dim)">${c.name || ''}</span></td>
        <td>${(c.price || 0).toLocaleString()}</td>
        <td style="color:var(--green)">+${c.change_pct}%</td>
    </tr>`).join('');
}

function renderTrades(trades) {
    const tbody = document.getElementById('trades-body');
    if (!trades.length) {
        tbody.innerHTML = '<tr><td colspan="9" style="text-align:center; color: var(--text-dim);">거래 없음</td></tr>';
        return;
    }
    // Show newest first
    const reversed = [...trades].reverse();
    tbody.innerHTML = reversed.map(t => {
        const exitClass = {
            'TP': 'exit-tp', 'SL': 'exit-sl',
            'TIME_EXIT': 'exit-time', 'FORCE_EXIT': 'exit-force',
        }[t.exit_reason] || '';
        const grossClass = t.gross_pnl_pct >= 0 ? 'pnl-pos' : 'pnl-neg';
        const netClass = t.net_pnl_pct >= 0 ? 'pnl-pos' : 'pnl-neg';
        const holdStr = t.holding_seconds >= 60
            ? `${Math.floor(t.holding_seconds / 60)}m${Math.floor(t.holding_seconds % 60)}s`
            : `${Math.floor(t.holding_seconds)}s`;
        return `<tr>
            <td>${t.trade_id}</td>
            <td>${t.code}<br><span style="font-size:10px;color:var(--text-dim)">${t.name || ''}</span></td>
            <td>${(t.entry_fill_price || 0).toLocaleString()}</td>
            <td>${(t.exit_fill_price || 0).toLocaleString()}</td>
            <td class="${exitClass}">${t.exit_reason}</td>
            <td class="${grossClass}">${t.gross_pnl_pct >= 0 ? '+' : ''}${(t.gross_pnl_pct || 0).toFixed(2)}%</td>
            <td class="${netClass}">${t.net_pnl_pct >= 0 ? '+' : ''}${(t.net_pnl_pct || 0).toFixed(2)}%</td>
            <td class="${netClass}">${Math.round(t.net_pnl_krw || 0).toLocaleString()}</td>
            <td>${holdStr}</td>
        </tr>`;
    }).join('');
}

function renderEvents(events) {
    const list = document.getElementById('event-list');
    if (!events.length) {
        list.innerHTML = '<div style="color:var(--text-dim);font-size:11px;padding:8px;">이벤트 대기 중</div>';
        return;
    }
    // newest first
    const reversed = [...events].reverse();
    list.innerHTML = reversed.slice(0, 30).map(e => {
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
            <span class="event-tag ${tagClass}">${tag.replace('SURGE_', '').slice(0, 14)}</span>
            <span class="event-code">${e.code || ''}</span>
            <span class="event-msg">${reason}${pnl}</span>
        </div>`;
    }).join('');
}

// ── PnL Chart (Canvas 2D) ────────────────────────────────────

function updatePnlChart(trades) {
    if (!trades.length) return;

    const canvas = document.getElementById('pnl-chart');
    const ctx = canvas.getContext('2d');
    const dpr = window.devicePixelRatio || 1;

    const rect = canvas.parentElement.getBoundingClientRect();
    canvas.width = rect.width * dpr;
    canvas.height = rect.height * dpr;
    ctx.scale(dpr, dpr);

    const W = rect.width;
    const H = rect.height;
    const PAD = 40;

    // Compute cumulative PnL
    let cumPnl = [];
    let sum = 0;
    for (const t of trades) {
        sum += (t.net_pnl_krw || 0);
        cumPnl.push(sum);
    }

    if (cumPnl.length < 2) return;

    const minY = Math.min(0, ...cumPnl);
    const maxY = Math.max(0, ...cumPnl);
    const rangeY = maxY - minY || 1;

    ctx.clearRect(0, 0, W, H);

    // Zero line
    const zeroY = PAD + (H - 2 * PAD) * (1 - (0 - minY) / rangeY);
    ctx.strokeStyle = 'rgba(255,255,255,0.1)';
    ctx.lineWidth = 1;
    ctx.beginPath();
    ctx.moveTo(PAD, zeroY);
    ctx.lineTo(W - PAD, zeroY);
    ctx.stroke();

    // PnL line
    ctx.beginPath();
    ctx.lineWidth = 2;
    for (let i = 0; i < cumPnl.length; i++) {
        const x = PAD + (W - 2 * PAD) * (i / (cumPnl.length - 1));
        const y = PAD + (H - 2 * PAD) * (1 - (cumPnl[i] - minY) / rangeY);
        if (i === 0) ctx.moveTo(x, y);
        else ctx.lineTo(x, y);
    }
    const lastPnl = cumPnl[cumPnl.length - 1];
    ctx.strokeStyle = lastPnl >= 0 ? '#00c853' : '#ff1744';
    ctx.stroke();

    // Fill area
    ctx.lineTo(PAD + (W - 2 * PAD), zeroY);
    ctx.lineTo(PAD, zeroY);
    ctx.closePath();
    ctx.fillStyle = lastPnl >= 0 ? 'rgba(0,200,83,0.1)' : 'rgba(255,23,68,0.1)';
    ctx.fill();

    // Labels
    ctx.fillStyle = '#8b949e';
    ctx.font = '10px system-ui';
    ctx.fillText(`${Math.round(maxY).toLocaleString()}`, 2, PAD + 4);
    ctx.fillText(`${Math.round(minY).toLocaleString()}`, 2, H - PAD + 12);
    ctx.fillText(`현재: ${Math.round(lastPnl).toLocaleString()}원`, W - 120, PAD - 8);
}

// ── Init on load ─────────────────────────────────────────────

init();
