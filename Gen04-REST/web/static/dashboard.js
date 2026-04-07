/**
 * dashboard.js -- Q-TRON REST Monitor v2
 * ========================================
 * SSE client, progressive disclosure mode switcher, data rendering.
 * Vanilla JS only, no dependencies.
 */

// ── State ────────────────────────────────────────────────────

const MODE_KEY = 'qtron_monitor_mode';
let currentMode = localStorage.getItem(MODE_KEY) || 'basic';
let sseSource = null;
let sseReconnectTimer = null;
let sseReconnectCount = 0;
const SSE_MAX_RECONNECT = 30;
const SSE_RECONNECT_DELAY = 3000;
const startTime = Date.now();
let lastState = null;
let prevState = null;     // for diff tracking
let diffEntries = [];     // state diff log (debug mode)
const MAX_DIFF_ENTRIES = 50;

// ── Initialization ───────────────────────────────────────────

document.addEventListener('DOMContentLoaded', () => {
    initModeSwitcher();
    initClock();
    initLogRefresh();
    initTraceFilters();
    initAlertClose();
    initCopyJson();
    initProfitTabs();
    connectSSE();
    setInterval(updateUptime, 1000);
});

// ── SSE Connection ───────────────────────────────────────────

function connectSSE() {
    if (sseSource) sseSource.close();
    updateSSEBar('connecting');

    sseSource = new EventSource('/sse/state');

    sseSource.addEventListener('state', (e) => {
        try {
            const data = JSON.parse(e.data);
            prevState = lastState;
            lastState = data;
            sseReconnectCount = 0;
            updateSSEBar('connected');
            updateDashboard(data);
        } catch (err) {
            console.error('SSE parse error:', err);
        }
    });

    sseSource.addEventListener('error', () => {
        updateSSEBar('disconnected');
        if (sseSource) sseSource.close();
        sseSource = null;

        if (sseReconnectCount < SSE_MAX_RECONNECT) {
            sseReconnectCount++;
            sseReconnectTimer = setTimeout(connectSSE, SSE_RECONNECT_DELAY);
        } else {
            updateSSEBar('dead');
        }
    });

    sseSource.addEventListener('open', () => {
        sseReconnectCount = 0;
        updateSSEBar('connected');
    });
}

function updateSSEBar(status) {
    const bar = document.getElementById('sse-bar');
    const label = document.getElementById('sse-status');
    bar.className = 'sse-bar';

    switch (status) {
        case 'connected':
            bar.classList.add('sse-connected');
            label.textContent = 'SSE: 연결됨';
            break;
        case 'connecting':
            bar.classList.add('sse-connecting');
            label.textContent = `SSE: 연결 중... (시도 ${sseReconnectCount + 1})`;
            break;
        case 'disconnected':
            bar.classList.add('sse-disconnected');
            label.textContent = `SSE: 연결 끊김 (${SSE_RECONNECT_DELAY / 1000}초 후 재시도)`;
            break;
        case 'dead':
            bar.classList.add('sse-dead');
            label.textContent = 'SSE: 연결 실패. 페이지를 새로고침하세요.';
            break;
    }
}

// ── Main Dashboard Update ────────────────────────────────────

function updateDashboard(data) {
    // Always update (Basic+)
    updateHero(data);
    updateSummaryCards(data);
    storeAccountData(data);
    updateRebalSchedule(data);
    updateHoldingsList(data);
    updateAlertBanner(data);
    updateControlCards(data);
    updateFreshnessGrid(data.freshness);
    updateUptime();

    // Operator+
    if (currentMode === 'operator' || currentMode === 'debug') {
        updateTracesTable(data.traces);
        updateWSCard(data.websocket);
        updateTimestampsCard(data.timestamps);
        updateSyncTable(data.sync);
    }

    // Debug
    if (currentMode === 'debug') {
        updateRawJson(data);
        updateLatencyHistogram();
        updateStateDiff(data);
    }
}

// ── Hero Status ──────────────────────────────────────────────

function updateHero(data) {
    const dot = document.getElementById('health-dot');
    const label = document.getElementById('health-label');
    const reason = document.getElementById('health-reason');
    const health = data.health;

    const statusLower = health.status.toLowerCase();
    dot.className = 'health-dot health-' + statusLower;

    const statusLabels = {
        green: '시스템 정상',
        yellow: '주의 필요',
        red: '오류 발생',
        black: '연결 안됨',
    };
    label.textContent = statusLabels[statusLower] || health.status;
    reason.textContent = health.reason;

    // Hero meta
    const serverEl = document.getElementById('hero-server');
    serverEl.textContent = data.server.type;

    const tokenEl = document.getElementById('hero-token');
    if (data.token.valid && data.token.remaining_sec > 0) {
        tokenEl.textContent = data.token.remaining_str + ' 남음';
        tokenEl.className = 'meta-value' + (data.token.remaining_sec < 600 ? ' status-warn' : '');
    } else {
        tokenEl.textContent = '만료됨';
        tokenEl.className = 'meta-value status-error';
    }

    const syncEl = document.getElementById('hero-sync');
    if (data.timestamps.last_request) {
        syncEl.textContent = data.timestamps.last_request;
    } else {
        syncEl.textContent = '대기 중';
    }

    // Server badge
    const badge = document.getElementById('server-badge');
    badge.textContent = data.server.type;
    badge.className = 'badge badge-' + data.server.type.toLowerCase();
}

// ── Summary Cards ────────────────────────────────────────────

function updateSummaryCards(data) {
    // Account data may not always be in the snapshot.
    // If it is, render it; otherwise show from freshness hints.
    const account = data.account || {};

    const holdingsEl = document.getElementById('card-holdings');
    const cashEl = document.getElementById('card-cash');
    const pnlEl = document.getElementById('card-pnl');
    const totalEl = document.getElementById('card-total');

    if (account.holdings_count !== undefined) {
        holdingsEl.textContent = account.holdings_count;
    } else {
        // Fall back: show request count as proxy for activity
        holdingsEl.textContent = '--';
        holdingsEl.className = 'summary-value neutral';
    }

    if (account.cash !== undefined) {
        cashEl.textContent = formatKRW(account.cash);
    } else {
        cashEl.textContent = '--';
        cashEl.className = 'summary-value neutral';
    }

    if (account.pnl_pct !== undefined) {
        const pct = account.pnl_pct;
        pnlEl.textContent = (pct >= 0 ? '+' : '') + pct.toFixed(2) + '%';
        pnlEl.className = 'summary-value ' + (pct > 0 ? 'positive' : pct < 0 ? 'negative' : 'neutral');
        // 손익 금액도 표시
        const pnlAmtEl = document.getElementById('card-pnl-amt');
        if (pnlAmtEl && account.total_pnl !== undefined) {
            const amt = account.total_pnl;
            pnlAmtEl.textContent = (amt >= 0 ? '+' : '') + formatKRW(amt);
            pnlAmtEl.className = 'summary-sub ' + (amt > 0 ? 'positive' : amt < 0 ? 'negative' : 'neutral');
        }
    } else {
        pnlEl.textContent = '--';
        pnlEl.className = 'summary-value neutral';
    }

    if (account.total_asset !== undefined) {
        totalEl.textContent = formatKRW(account.total_asset);
        // 평가금액 표시
        const evalEl = document.getElementById('card-eval');
        if (evalEl && account.total_eval !== undefined) {
            evalEl.textContent = '평가 ' + formatKRW(account.total_eval);
        }
    } else {
        totalEl.textContent = '--';
        totalEl.className = 'summary-value neutral';
    }
}

function formatKRW(val) {
    if (val === null || val === undefined) return '--';
    const abs = Math.abs(val);
    const sign = val < 0 ? '-' : '';
    if (abs >= 1e8) return sign + (abs / 1e8).toFixed(1) + '억';
    if (abs >= 1e4) return sign + (abs / 1e4).toFixed(0) + '만';
    return val.toLocaleString();
}

// ── Rebalance Schedule ───────────────────────────────
function updateRebalSchedule(data) {
    const rebal = data.rebalance || {};
    const lastEl = document.getElementById('rebal-last');
    const nextEl = document.getElementById('rebal-next');
    const ddayEl = document.getElementById('rebal-dday');
    if (!lastEl || !rebal.last) return;

    const lastDate = rebal.last; // "20260403" format
    const cycle = rebal.cycle || 21;

    // Format last date
    const ly = lastDate.substring(0,4);
    const lm = lastDate.substring(4,6);
    const ld = lastDate.substring(6,8);
    lastEl.textContent = `${ly}.${lm}.${ld}`;

    // Calculate next rebalance (approximate: last + cycle trading days ≈ cycle * 1.4 calendar days)
    const lastDt = new Date(parseInt(ly), parseInt(lm)-1, parseInt(ld));
    const calendarDays = Math.round(cycle * 1.4); // 21 trading days ≈ 30 calendar days
    const nextDt = new Date(lastDt.getTime() + calendarDays * 86400000);
    const ny = nextDt.getFullYear();
    const nm = String(nextDt.getMonth()+1).padStart(2,'0');
    const nd = String(nextDt.getDate()).padStart(2,'0');
    nextEl.textContent = `${ny}.${nm}.${nd}`;

    // D-day
    const today = new Date();
    today.setHours(0,0,0,0);
    const diffDays = Math.ceil((nextDt - today) / 86400000);
    if (diffDays > 0) {
        ddayEl.textContent = `D-${diffDays}`;
        ddayEl.className = 'rebal-dday' + (diffDays <= 3 ? ' urgent' : '');
    } else if (diffDays === 0) {
        ddayEl.textContent = 'D-DAY';
        ddayEl.className = 'rebal-dday urgent';
    } else {
        ddayEl.textContent = `D+${Math.abs(diffDays)}`;
        ddayEl.className = 'rebal-dday overdue';
    }
}

// ── Profit Analysis (수익분석) ───────────────────────
let _profitData = null;
let _currentPeriod = 'day';

function initProfitTabs() {
    document.querySelectorAll('.profit-tab').forEach(btn => {
        btn.addEventListener('click', () => {
            document.querySelectorAll('.profit-tab').forEach(b => b.classList.remove('active'));
            btn.classList.add('active');
            _currentPeriod = btn.dataset.period;
            renderProfit();
        });
    });
    // Fetch profit data
    fetchProfit();
    setInterval(fetchProfit, 60000); // Refresh every 60s
}

function fetchProfit() {
    fetch('/api/profit')
        .then(r => r.json())
        .then(data => { _profitData = data; renderProfit(); })
        .catch(() => {});
}

function renderProfit() {
    if (!_profitData) return;
    const realized = _profitData[_currentPeriod] || 0;
    const fees = _profitData.fees || 0;

    // Unrealized from account data
    const account = window._lastAccountData || {};
    const unrealized = account.total_pnl || 0;
    const total = unrealized + realized;

    const setVal = (id, val) => {
        const el = document.getElementById(id);
        if (!el) return;
        const sign = val >= 0 ? '+' : '';
        el.textContent = sign + formatKRW(val);
        el.className = 'profit-value ' + (val > 0 ? 'positive' : val < 0 ? 'negative' : 'neutral');
    };
    setVal('profit-unrealized', unrealized);
    setVal('profit-realized', realized);
    setVal('profit-total', total);
    const feesEl = document.getElementById('profit-fees');
    if (feesEl) {
        feesEl.textContent = '-' + formatKRW(Math.abs(fees));
        feesEl.className = 'profit-value neutral';
    }
}

// Store account data for profit section
function storeAccountData(data) {
    if (data.account) window._lastAccountData = data.account;
}

// ── Holdings List (토스 스타일) ──────────────────────
function updateHoldingsList(data) {
    const container = document.getElementById('holdings-list');
    if (!container) return;

    const account = data.account || {};
    const holdings = account.holdings || [];

    if (holdings.length === 0) {
        container.innerHTML = '<div class="holdings-empty">보유종목 없음</div>';
        return;
    }

    let html = '';
    for (const h of holdings) {
        const code = h.code || '';
        const name = h.name || code;
        const qty = h.qty || 0;
        const curPrice = h.cur_price || 0;
        const evalAmt = h.eval_amt || (curPrice * qty);
        const pnl = h.pnl || 0;
        const pnlRate = parseFloat(h.pnl_rate || '0');
        const isPositive = pnl > 0;
        const isNegative = pnl < 0;
        const colorClass = isPositive ? 'positive' : isNegative ? 'negative' : 'neutral';
        const sign = isPositive ? '+' : '';

        html += `
        <div class="holding-card">
            <div class="holding-left">
                <div class="holding-name">${name}</div>
                <div class="holding-meta">${code} · ${qty}주</div>
            </div>
            <div class="holding-right">
                <div class="holding-eval">${formatKRW(evalAmt)}</div>
                <div class="holding-pnl ${colorClass}">
                    ${sign}${formatKRW(pnl)} (${sign}${pnlRate.toFixed(1)}%)
                </div>
            </div>
        </div>`;
    }
    container.innerHTML = html;
}

// ── Alert Banner ─────────────────────────────────────────────

function updateAlertBanner(data) {
    const banner = document.getElementById('alert-banner');
    const icon = document.getElementById('alert-icon');
    const text = document.getElementById('alert-text');
    const health = data.health;

    // Show alert for non-green statuses
    if (health.status === 'RED') {
        banner.style.display = 'flex';
        banner.className = 'alert-banner alert-error';
        icon.textContent = '\u26A0';
        text.textContent = health.reason;
    } else if (health.status === 'YELLOW') {
        banner.style.display = 'flex';
        banner.className = 'alert-banner alert-warn';
        icon.textContent = '\u26A0';
        text.textContent = health.reason;
    } else {
        banner.style.display = 'none';
    }
}

function initAlertClose() {
    const btn = document.getElementById('alert-close');
    if (btn) {
        btn.addEventListener('click', () => {
            document.getElementById('alert-banner').style.display = 'none';
        });
    }
}

// ── Control Cards ────────────────────────────────────────────

function updateControlCards(data) {
    // Token
    const tokenStatusEl = document.getElementById('token-status');
    tokenStatusEl.textContent = data.token.valid ? '유효' : '만료';
    tokenStatusEl.className = 'v ' + (data.token.valid ? 'status-ok' : 'status-error');

    setText('token-remaining', data.token.remaining_str,
        data.token.remaining_sec > 3600 ? '' :
        data.token.remaining_sec > 600 ? 'status-warn' : 'status-error');
    document.getElementById('token-refresh').textContent = data.token.last_refresh;

    // Latency
    setText('lat-last', data.latency.last_ms + ' ms', latencyClass(data.latency.last_ms));
    setText('lat-avg', data.latency.avg_ms + ' ms', latencyClass(data.latency.avg_ms));
    setText('lat-p95', data.latency.p95_ms + ' ms', latencyClass(data.latency.p95_ms));

    // Counters
    setText('cnt-total', data.counters.total_requests.toLocaleString());
    setText('cnt-failures', data.counters.total_failures.toLocaleString(),
        data.counters.total_failures > 0 ? 'status-error' : '');
    setText('cnt-rate', data.counters.failure_rate_pct,
        data.counters.failure_rate > 0.15 ? 'status-error' :
        data.counters.failure_rate > 0.05 ? 'status-warn' : 'status-ok');
    setText('cnt-retries', data.counters.total_retries.toLocaleString(),
        data.counters.total_retries > 10 ? 'status-warn' : '');
}

function latencyClass(ms) {
    if (ms === 0) return '';
    if (ms > 3000) return 'status-error';
    if (ms > 1000) return 'status-warn';
    return 'status-ok';
}

// ── Freshness Grid ───────────────────────────────────────────

function updateFreshnessGrid(freshness) {
    const grid = document.getElementById('freshness-grid');
    if (!freshness) return;

    let html = '';
    for (const [key, fp] of Object.entries(freshness)) {
        const statusClass =
            fp.status === 'FRESH' ? 'fresh-ok' :
            fp.status === 'WARN'  ? 'fresh-warn' :
            fp.status === 'STALE' ? 'fresh-stale' : 'fresh-never';

        const ageText = fp.age_sec !== null
            ? (fp.age_sec < 60 ? Math.round(fp.age_sec) + '초' :
               Math.floor(fp.age_sec / 60) + '분')
            : 'N/A';

        const statusLabels = { FRESH: '정상', WARN: '주의', STALE: '오래됨', NEVER: '미수신' };

        html += `
            <div class="freshness-card ${statusClass}">
                <div class="freshness-source">${formatSourceName(fp.source)}</div>
                <div class="freshness-status">${statusLabels[fp.status] || fp.status}</div>
                <div class="freshness-age">경과: ${ageText}</div>
                <div class="freshness-meta">
                    갱신: ${fp.last_update_str} | 횟수: ${fp.update_count}
                </div>
            </div>
        `;
    }
    grid.innerHTML = html;
}

function formatSourceName(source) {
    const nameMap = {
        'account_summary': '계좌 요약',
        'holdings': '보유종목',
        'open_orders': '미체결',
        'price_tick': '시세',
        'ws_message': 'WebSocket',
    };
    return nameMap[source] || source.replace(/_/g, ' ').replace(/\b\w/g, c => c.toUpperCase());
}

// ── WebSocket Card (Operator+) ───────────────────────────────

function updateWSCard(ws) {
    const connEl = document.getElementById('ws-connected');
    connEl.textContent = ws.connected ? '연결됨' : '끊김';
    connEl.className = 'v ' + (ws.connected ? 'status-ok' : 'status-error');

    document.getElementById('ws-last-msg').textContent = ws.last_msg || '--';
    document.getElementById('ws-msg-count').textContent = ws.msg_count.toLocaleString();

    const reconEl = document.getElementById('ws-reconnects');
    reconEl.textContent = ws.reconnect_count.toString();
    reconEl.className = 'v mono' + (ws.reconnect_count > 3 ? ' status-warn' : '');
}

// ── Timestamps Card (Operator+) ──────────────────────────────

function updateTimestampsCard(ts) {
    document.getElementById('ts-first').textContent = ts.first_request || '--';
    document.getElementById('ts-last').textContent = ts.last_request || '--';
    document.getElementById('ts-success').textContent = ts.last_success || '--';

    const failEl = document.getElementById('ts-failure');
    failEl.textContent = ts.last_failure || '--';
    failEl.className = 'v dim' + (ts.last_failure ? ' status-warn' : '');
}

// ── Traces Table (Operator+) ─────────────────────────────────

function updateTracesTable(traces) {
    const tbody = document.getElementById('traces-body');
    if (!traces || traces.length === 0) {
        tbody.innerHTML = '<tr><td colspan="7" class="empty-row">추적 데이터 없음</td></tr>';
        return;
    }

    const statusFilter = document.getElementById('trace-filter-status').value;
    const searchFilter = document.getElementById('trace-filter-search').value.toLowerCase();

    let filtered = traces;
    if (statusFilter) {
        filtered = filtered.filter(t => t.status === statusFilter);
    }
    if (searchFilter) {
        filtered = filtered.filter(t =>
            (t.endpoint && t.endpoint.toLowerCase().includes(searchFilter)) ||
            (t.related_code && t.related_code.includes(searchFilter)) ||
            (t.api_id && t.api_id.toLowerCase().includes(searchFilter))
        );
    }

    const statusLabels = { ok: '성공', error: '오류', timeout: '타임아웃', retry: '재시도', pending: '대기' };

    let html = '';
    for (const t of filtered.slice(0, 50)) {
        const rowCls =
            t.status === 'error' ? 'trace-error' :
            t.status === 'timeout' ? 'trace-timeout' : '';

        const badgeCls =
            t.status === 'ok' ? 'badge-ok' :
            t.status === 'error' ? 'badge-error' :
            t.status === 'timeout' ? 'badge-timeout' :
            t.status === 'retry' ? 'badge-retry' : 'badge-pending';

        html += `
            <tr class="${rowCls}">
                <td class="mono">${t.request_time}</td>
                <td title="${esc(t.endpoint)}">${truncate(t.endpoint, 28)}</td>
                <td class="mono">${t.api_id}</td>
                <td class="mono ${latencyClass(t.latency_ms)}">${t.latency_ms}ms</td>
                <td><span class="status-badge ${badgeCls}">${(statusLabels[t.status] || t.status).toUpperCase()}${t.retry_count > 0 ? ' R' + t.retry_count : ''}</span></td>
                <td class="mono">${t.related_code || ''}</td>
                <td class="error-cell" title="${esc(t.error)}">${truncate(t.error, 35)}</td>
            </tr>
        `;
    }

    tbody.innerHTML = html || '<tr><td colspan="7" class="empty-row">조건에 맞는 데이터 없음</td></tr>';
}

function initTraceFilters() {
    const sel = document.getElementById('trace-filter-status');
    const inp = document.getElementById('trace-filter-search');
    if (sel) sel.addEventListener('change', () => { if (lastState) updateTracesTable(lastState.traces); });
    if (inp) inp.addEventListener('input', () => { if (lastState) updateTracesTable(lastState.traces); });
}

// ── Sync Table (Operator+) ───────────────────────────────────

function updateSyncTable(syncItems) {
    const tbody = document.getElementById('sync-body');
    if (!syncItems || syncItems.length === 0) {
        tbody.innerHTML = '<tr><td colspan="8" class="empty-row">동기화 데이터 없음</td></tr>';
        return;
    }

    let html = '';
    for (const s of syncItems) {
        const cls = s.match ? '' : 'sync-mismatch';
        html += `
            <tr class="${cls}">
                <td>${s.field}</td>
                <td class="mono">${fmtVal(s.rest)}</td>
                <td class="mono">${fmtVal(s.com)}</td>
                <td>${s.match ? '<span class="status-ok">일치</span>' : '<span class="status-error">불일치</span>'}</td>
                <td class="error-cell">${s.diff}</td>
                <td class="mono dim">${s.rest_ts}</td>
                <td class="mono dim">${s.com_ts}</td>
                <td class="mono dim">${s.last_match}</td>
            </tr>
        `;
    }
    tbody.innerHTML = html;
}

// ── Raw JSON Viewer (Debug) ──────────────────────────────────

function updateRawJson(data) {
    const el = document.getElementById('raw-json');
    if (!el) return;
    try {
        el.textContent = JSON.stringify(data, null, 2);
    } catch (err) {
        el.textContent = 'JSON 직렬화 오류: ' + err.message;
    }
}

function initCopyJson() {
    const btn = document.getElementById('btn-copy-json');
    if (!btn) return;
    btn.addEventListener('click', () => {
        const el = document.getElementById('raw-json');
        if (el && navigator.clipboard) {
            navigator.clipboard.writeText(el.textContent).then(() => {
                btn.textContent = '복사됨!';
                setTimeout(() => { btn.textContent = '복사'; }, 1500);
            });
        }
    });
}

// ── Latency Histogram (Debug) ────────────────────────────────

async function updateLatencyHistogram() {
    try {
        const resp = await fetch('/api/latency-histogram?buckets=15');
        const buckets = await resp.json();
        renderHistogram(buckets);
    } catch (err) {
        console.error('Histogram fetch error:', err);
    }
}

function renderHistogram(buckets) {
    const container = document.getElementById('histogram-container');
    if (!buckets || buckets.length === 0) {
        container.innerHTML = '<div class="empty-row">지연시간 데이터 없음</div>';
        return;
    }

    const maxCount = Math.max(...buckets.map(b => b.count), 1);

    let html = '<div class="histogram">';
    for (const b of buckets) {
        const pct = Math.max((b.count / maxCount) * 100, 3);
        const barClass = b.range_hi > 3000 ? 'bar-error' :
                         b.range_hi > 1000 ? 'bar-warn' : 'bar-ok';
        html += `
            <div class="hist-col">
                <div class="hist-count">${b.count}</div>
                <div class="hist-bar ${barClass}" style="height:${pct}%"
                     title="${b.label}: ${b.count}건"></div>
                <div class="hist-label">${b.range_lo}</div>
            </div>
        `;
    }
    html += '</div>';
    container.innerHTML = html;
}

// ── State Diff (Debug) ───────────────────────────────────────

function updateStateDiff(data) {
    if (!prevState) return;

    const diffs = computeDiff(prevState, data, '');
    if (diffs.length === 0) return;

    const now = new Date().toLocaleTimeString('ko-KR', { hour12: false });
    for (const d of diffs) {
        diffEntries.unshift({ time: now, ...d });
    }
    if (diffEntries.length > MAX_DIFF_ENTRIES) {
        diffEntries = diffEntries.slice(0, MAX_DIFF_ENTRIES);
    }

    const container = document.getElementById('diff-container');
    let html = '';
    for (const entry of diffEntries) {
        html += `<div class="diff-line">` +
            `<span class="diff-time">[${entry.time}]</span> ` +
            `<span class="diff-del">- ${esc(entry.path)}: ${esc(String(entry.old))}</span>` +
            `</div>` +
            `<div class="diff-line">` +
            `<span class="diff-time">[${entry.time}]</span> ` +
            `<span class="diff-add">+ ${esc(entry.path)}: ${esc(String(entry.new))}</span>` +
            `</div>`;
    }
    container.innerHTML = html || '<div class="empty-row">변경 사항 없음</div>';
}

function computeDiff(oldObj, newObj, prefix) {
    const diffs = [];
    // Skip heavy fields
    const skip = new Set(['traces', 'timestamp', 'timestamp_str']);

    if (typeof oldObj !== 'object' || typeof newObj !== 'object' ||
        oldObj === null || newObj === null) {
        if (oldObj !== newObj) {
            diffs.push({ path: prefix, old: oldObj, new: newObj });
        }
        return diffs;
    }

    const keys = new Set([...Object.keys(oldObj), ...Object.keys(newObj)]);
    for (const k of keys) {
        if (skip.has(k)) continue;
        const path = prefix ? prefix + '.' + k : k;
        const ov = oldObj[k];
        const nv = newObj[k];

        if (typeof ov === 'object' && typeof nv === 'object' && ov !== null && nv !== null) {
            if (Array.isArray(ov) && Array.isArray(nv)) {
                if (JSON.stringify(ov) !== JSON.stringify(nv)) {
                    diffs.push({ path, old: JSON.stringify(ov).slice(0, 80), new: JSON.stringify(nv).slice(0, 80) });
                }
            } else {
                diffs.push(...computeDiff(ov, nv, path));
            }
        } else if (ov !== nv) {
            diffs.push({ path, old: ov, new: nv });
        }
    }
    return diffs;
}

// ── Log Panel (Debug) ────────────────────────────────────────

function initLogRefresh() {
    const btn = document.getElementById('btn-refresh-logs');
    if (btn) btn.addEventListener('click', fetchLogs);
}

async function fetchLogs() {
    try {
        const resp = await fetch('/api/logs?max_lines=100');
        const logs = await resp.json();
        renderLogs(logs);
    } catch (err) {
        console.error('Log fetch error:', err);
    }
}

function renderLogs(logs) {
    const container = document.getElementById('log-container');
    if (!logs || logs.length === 0) {
        container.innerHTML = '<div class="empty-row">오늘 로그 없음</div>';
        return;
    }

    let html = '';
    for (const entry of logs) {
        const levelCls = entry.level === 'ERROR' ? 'log-error' :
                         entry.level === 'WARNING' ? 'log-warn' : 'log-info';
        const tagBadge = entry.tag ? `<span class="log-tag">${esc(entry.tag)}</span>` : '';

        html += `<div class="log-line ${levelCls}">` +
            `<span class="log-time">${entry.time}</span>` +
            `<span class="log-level">${entry.level}</span>` +
            tagBadge +
            `<span class="log-msg">${esc(entry.message)}</span>` +
            `</div>`;
    }
    container.innerHTML = html;
}

// ── Mode Switcher ────────────────────────────────────────────

function initModeSwitcher() {
    const buttons = document.querySelectorAll('.mode-btn');
    buttons.forEach(btn => {
        btn.addEventListener('click', () => switchMode(btn.dataset.mode));
    });

    // Restore saved mode
    switchMode(currentMode);
}

function switchMode(mode) {
    currentMode = mode;
    localStorage.setItem(MODE_KEY, mode);

    // Button states
    document.querySelectorAll('.mode-btn').forEach(btn => {
        btn.classList.toggle('active', btn.dataset.mode === mode);
    });

    // Show/hide sections with hidden attribute
    document.querySelectorAll('.mode-operator').forEach(el => {
        el.hidden = !(mode === 'operator' || mode === 'debug');
    });
    document.querySelectorAll('.mode-debug').forEach(el => {
        // Elements with BOTH mode-operator and mode-debug should show in operator too
        if (el.classList.contains('mode-operator')) {
            el.hidden = !(mode === 'operator' || mode === 'debug');
        } else {
            el.hidden = mode !== 'debug';
        }
    });

    // Load debug data
    if (mode === 'debug') {
        fetchLogs();
        updateLatencyHistogram();
    }

    // Re-render with current data
    if (lastState) updateDashboard(lastState);
}

// ── Clock ────────────────────────────────────────────────────

function initClock() {
    function tick() {
        const el = document.getElementById('clock');
        if (el) el.textContent = new Date().toLocaleTimeString('ko-KR', { hour12: false });
    }
    tick();
    setInterval(tick, 1000);
}

// ── Uptime ───────────────────────────────────────────────────

function updateUptime() {
    const elapsed = Math.floor((Date.now() - startTime) / 1000);
    const h = Math.floor(elapsed / 3600);
    const m = Math.floor((elapsed % 3600) / 60);
    const s = elapsed % 60;
    const str = h > 0 ? `${h}h ${m}m ${s}s` : `${m}m ${s}s`;
    const el = document.getElementById('footer-uptime');
    if (el) el.textContent = `Uptime: ${str}`;
}

// ── Utilities ────────────────────────────────────────────────

function setText(id, text, extraClass) {
    const el = document.getElementById(id);
    if (!el) return;
    el.textContent = text;
    if (extraClass !== undefined) {
        el.className = 'v mono ' + (extraClass || '');
    }
}

function truncate(str, max) {
    if (!str) return '';
    return str.length > max ? str.substring(0, max) + '...' : str;
}

function fmtVal(val) {
    if (val === null || val === undefined) return '--';
    if (typeof val === 'number') return val.toLocaleString();
    return String(val);
}

function esc(str) {
    if (!str) return '';
    return String(str)
        .replace(/&/g, '&amp;')
        .replace(/</g, '&lt;')
        .replace(/>/g, '&gt;')
        .replace(/"/g, '&quot;');
}
