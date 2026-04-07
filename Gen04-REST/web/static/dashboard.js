/**
 * dashboard.js -- Q-TRON REST Monitor SSE Client
 * ================================================
 * SSE 연결 관리, 상태 업데이트, Progressive UX 모드 전환.
 * 의존성 없음 (vanilla JS).
 */

// ── State ────────────────────────────────────────────────────

let currentMode = 'basic';   // basic | operator | debug
let sseSource = null;
let sseReconnectTimer = null;
let sseReconnectCount = 0;
const SSE_MAX_RECONNECT = 20;
const SSE_RECONNECT_DELAY = 3000;
let startTime = Date.now();
let lastState = null;

// ── Initialization ───────────────────────────────────────────

document.addEventListener('DOMContentLoaded', () => {
    initModeSwitcher();
    initClock();
    initLogRefresh();
    initTraceFilters();
    connectSSE();
});

// ── SSE Connection ───────────────────────────────────────────

function connectSSE() {
    if (sseSource) {
        sseSource.close();
    }

    updateSSEBar('connecting');

    sseSource = new EventSource('/sse/state');

    sseSource.addEventListener('state', (e) => {
        try {
            const data = JSON.parse(e.data);
            lastState = data;
            sseReconnectCount = 0;
            updateSSEBar('connected');
            updateDashboard(data);
        } catch (err) {
            console.error('SSE parse error:', err);
        }
    });

    sseSource.addEventListener('error', (e) => {
        console.error('SSE error:', e);
        updateSSEBar('disconnected');
        sseSource.close();
        sseSource = null;

        // Auto-reconnect
        if (sseReconnectCount < SSE_MAX_RECONNECT) {
            sseReconnectCount++;
            sseReconnectTimer = setTimeout(() => {
                connectSSE();
            }, SSE_RECONNECT_DELAY);
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
            label.textContent = 'SSE: Connected';
            break;
        case 'connecting':
            bar.classList.add('sse-connecting');
            label.textContent = `SSE: Connecting... (attempt ${sseReconnectCount + 1})`;
            break;
        case 'disconnected':
            bar.classList.add('sse-disconnected');
            label.textContent = `SSE: Disconnected (retry in ${SSE_RECONNECT_DELAY / 1000}s)`;
            break;
        case 'dead':
            bar.classList.add('sse-dead');
            label.textContent = 'SSE: Connection failed. Reload page.';
            break;
    }
}

// ── Dashboard Update (main render) ──────────────────────────

function updateDashboard(data) {
    updateHealthIndicator(data.health);
    updateServerBadge(data.server);
    updateTokenCard(data.token);
    updateLatencyCard(data.latency);
    updateCountersCard(data.counters);
    updateWebSocketCard(data.websocket);
    updateTimestampsCard(data.timestamps);
    updateFreshnessGrid(data.freshness);
    updateUptime();

    // Operator+ sections
    if (currentMode === 'operator' || currentMode === 'debug') {
        updateTracesTable(data.traces);
        updateSyncTable(data.sync);
        updateFlowVisualization(data);
    }

    // Debug sections
    if (currentMode === 'debug') {
        updateLatencyHistogram();
    }
}

// ── Health Indicator ─────────────────────────────────────────

function updateHealthIndicator(health) {
    const pill = document.getElementById('health-indicator');
    const text = document.getElementById('health-text');
    const reason = document.getElementById('health-reason');

    pill.className = 'health-pill health-' + health.status.toLowerCase();
    text.textContent = health.status;
    reason.textContent = health.reason;
}

// ── Server Badge ─────────────────────────────────────────────

function updateServerBadge(server) {
    const badge = document.getElementById('server-badge');
    badge.textContent = server.type;
    badge.className = 'badge badge-' + server.type.toLowerCase();
}

// ── Token Card ───────────────────────────────────────────────

function updateTokenCard(token) {
    const statusEl = document.getElementById('token-status');
    const remainEl = document.getElementById('token-remaining');
    const refreshEl = document.getElementById('token-refresh');

    statusEl.textContent = token.valid ? 'VALID' : 'EXPIRED';
    statusEl.className = 'metric-value ' + (token.valid ? 'status-ok' : 'status-error');

    remainEl.textContent = token.remaining_str;
    remainEl.className = 'metric-value ' + (
        token.remaining_sec > 3600 ? '' :
        token.remaining_sec > 600 ? 'status-warn' : 'status-error'
    );

    refreshEl.textContent = token.last_refresh;
}

// ── Latency Card ─────────────────────────────────────────────

function updateLatencyCard(lat) {
    setText('lat-last', lat.last_ms + ' ms', latencyClass(lat.last_ms));
    setText('lat-avg', lat.avg_ms + ' ms', latencyClass(lat.avg_ms));
    setText('lat-p95', lat.p95_ms + ' ms', latencyClass(lat.p95_ms));
}

function latencyClass(ms) {
    if (ms === 0) return '';
    if (ms > 3000) return 'status-error';
    if (ms > 1000) return 'status-warn';
    return 'status-ok';
}

// ── Counters Card ────────────────────────────────────────────

function updateCountersCard(c) {
    setText('cnt-total', c.total_requests.toLocaleString());
    setText('cnt-failures', c.total_failures.toLocaleString(),
        c.total_failures > 0 ? 'status-error' : '');
    setText('cnt-rate', c.failure_rate_pct,
        c.failure_rate > 0.15 ? 'status-error' :
        c.failure_rate > 0.05 ? 'status-warn' : 'status-ok');
    setText('cnt-retries', c.total_retries.toLocaleString(),
        c.total_retries > 10 ? 'status-warn' : '');
}

// ── WebSocket Card ───────────────────────────────────────────

function updateWebSocketCard(ws) {
    const connEl = document.getElementById('ws-connected');
    connEl.textContent = ws.connected ? 'YES' : 'NO';
    connEl.className = 'metric-value ' + (ws.connected ? 'status-ok' : 'status-error');

    setText('ws-last-msg', ws.last_msg || '--');
    setText('ws-msg-count', ws.msg_count.toLocaleString());
    setText('ws-reconnects', ws.reconnect_count.toString(),
        ws.reconnect_count > 3 ? 'status-warn' : '');
}

// ── Timestamps Card ──────────────────────────────────────────

function updateTimestampsCard(ts) {
    setText('ts-first', ts.first_request || '--');
    setText('ts-last', ts.last_request || '--');
    setText('ts-success', ts.last_success || '--');
    setText('ts-failure', ts.last_failure || '--',
        ts.last_failure ? 'status-warn' : 'dim');
}

// ── Freshness Grid ───────────────────────────────────────────

function updateFreshnessGrid(freshness) {
    const grid = document.getElementById('freshness-grid');
    let html = '';

    for (const [key, fp] of Object.entries(freshness)) {
        const statusClass =
            fp.status === 'FRESH' ? 'fresh-ok' :
            fp.status === 'WARN' ? 'fresh-warn' :
            fp.status === 'STALE' ? 'fresh-stale' : 'fresh-never';

        const ageText = fp.age_sec !== null
            ? (fp.age_sec < 60 ? fp.age_sec + 's' : Math.floor(fp.age_sec / 60) + 'm')
            : 'N/A';

        html += `
            <div class="freshness-card ${statusClass}">
                <div class="freshness-source">${formatSourceName(fp.source)}</div>
                <div class="freshness-status">${fp.status}</div>
                <div class="freshness-age">Age: ${ageText}</div>
                <div class="freshness-meta">
                    Last: ${fp.last_update_str} | Count: ${fp.update_count}
                </div>
            </div>
        `;
    }

    grid.innerHTML = html;
}

function formatSourceName(source) {
    return source.replace(/_/g, ' ').replace(/\b\w/g, c => c.toUpperCase());
}

// ── Traces Table ─────────────────────────────────────────────

function updateTracesTable(traces) {
    const tbody = document.getElementById('traces-body');
    if (!traces || traces.length === 0) {
        tbody.innerHTML = '<tr><td colspan="7" class="empty-row">No traces yet</td></tr>';
        return;
    }

    // Apply local filters
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

    let html = '';
    for (const t of filtered.slice(0, 50)) {
        const statusCls =
            t.status === 'ok' ? 'trace-ok' :
            t.status === 'error' ? 'trace-error' :
            t.status === 'timeout' ? 'trace-timeout' : 'trace-other';

        html += `
            <tr class="${statusCls}">
                <td class="mono">${t.request_time}</td>
                <td title="${t.endpoint}">${truncate(t.endpoint, 30)}</td>
                <td class="mono">${t.api_id}</td>
                <td class="mono ${latencyClass(t.latency_ms)}">${t.latency_ms}ms</td>
                <td><span class="status-badge status-${t.status}">${t.status.toUpperCase()}${t.retry_count > 0 ? ' (R' + t.retry_count + ')' : ''}</span></td>
                <td class="mono">${t.related_code || ''}</td>
                <td class="error-cell" title="${t.error}">${truncate(t.error, 40)}</td>
            </tr>
        `;
    }

    tbody.innerHTML = html || '<tr><td colspan="7" class="empty-row">No matching traces</td></tr>';
}

// ── Sync Table ───────────────────────────────────────────────

function updateSyncTable(syncItems) {
    const tbody = document.getElementById('sync-body');
    if (!syncItems || syncItems.length === 0) {
        tbody.innerHTML = '<tr><td colspan="8" class="empty-row">No sync data</td></tr>';
        return;
    }

    let html = '';
    for (const s of syncItems) {
        const matchCls = s.match ? 'sync-match' : 'sync-mismatch';
        html += `
            <tr class="${matchCls}">
                <td>${s.field}</td>
                <td class="mono">${formatValue(s.rest)}</td>
                <td class="mono">${formatValue(s.com)}</td>
                <td>${s.match ? '<span class="status-ok">MATCH</span>' : '<span class="status-error">MISMATCH</span>'}</td>
                <td class="error-cell">${s.diff}</td>
                <td class="mono dim">${s.rest_ts}</td>
                <td class="mono dim">${s.com_ts}</td>
                <td class="mono dim">${s.last_match}</td>
            </tr>
        `;
    }

    tbody.innerHTML = html;
}

// ── Flow Visualization ───────────────────────────────────────

function updateFlowVisualization(data) {
    if (!data.traces || data.traces.length === 0) return;

    const latest = data.traces[0];
    const flowReq = document.getElementById('flow-req');
    const flowResp = document.getElementById('flow-resp');
    const flowState = document.getElementById('flow-state');
    const flowUi = document.getElementById('flow-ui');

    flowReq.textContent = `${latest.api_id}\n${latest.request_time}`;
    flowReq.className = 'flow-box flow-active';

    if (latest.response_time) {
        flowResp.textContent = `${latest.status.toUpperCase()}\n${latest.latency_ms}ms`;
        flowResp.className = 'flow-box flow-' + (latest.status === 'ok' ? 'ok' : 'error');
    }

    flowState.textContent = latest.tag || 'API_RESP';
    flowState.className = 'flow-box flow-active';

    flowUi.textContent = new Date().toLocaleTimeString('ko-KR');
    flowUi.className = 'flow-box flow-active';

    // Failure points
    document.getElementById('flow-last-fail').textContent =
        data.timestamps.last_failure || 'None';
    document.getElementById('flow-last-good').textContent =
        data.timestamps.last_success || '--';
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
        container.innerHTML = '<div class="empty-row">No latency data</div>';
        return;
    }

    const maxCount = Math.max(...buckets.map(b => b.count), 1);

    let html = '<div class="histogram">';
    for (const b of buckets) {
        const pct = Math.max((b.count / maxCount) * 100, 2);
        const barClass = b.range_hi > 3000 ? 'bar-error' :
                         b.range_hi > 1000 ? 'bar-warn' : 'bar-ok';
        html += `
            <div class="hist-col">
                <div class="hist-bar ${barClass}" style="height:${pct}%"
                     title="${b.label}: ${b.count}"></div>
                <div class="hist-label">${b.range_lo}</div>
            </div>
        `;
    }
    html += '</div>';

    container.innerHTML = html;
}

// ── Log Panel (Debug) ────────────────────────────────────────

function initLogRefresh() {
    const btn = document.getElementById('btn-refresh-logs');
    if (btn) {
        btn.addEventListener('click', fetchLogs);
    }
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
        container.innerHTML = '<div class="empty-row">No log entries today</div>';
        return;
    }

    let html = '';
    for (const entry of logs) {
        const levelCls = entry.level === 'ERROR' ? 'log-error' :
                         entry.level === 'WARNING' ? 'log-warn' : 'log-info';
        const tagBadge = entry.tag ? `<span class="log-tag">${entry.tag}</span>` : '';

        html += `<div class="log-line ${levelCls}">` +
            `<span class="log-time">${entry.time}</span>` +
            `<span class="log-level">${entry.level}</span>` +
            `${tagBadge}` +
            `<span class="log-msg">${escapeHtml(entry.message)}</span>` +
            `</div>`;
    }

    container.innerHTML = html;
}

// ── Mode Switcher ────────────────────────────────────────────

function initModeSwitcher() {
    const buttons = document.querySelectorAll('.mode-btn');
    buttons.forEach(btn => {
        btn.addEventListener('click', () => {
            const mode = btn.dataset.mode;
            switchMode(mode);
        });
    });

    // Default to basic
    switchMode('basic');
}

function switchMode(mode) {
    currentMode = mode;

    // Update button states
    document.querySelectorAll('.mode-btn').forEach(btn => {
        btn.classList.toggle('active', btn.dataset.mode === mode);
    });

    // Show/hide sections based on mode
    const operatorSections = document.querySelectorAll('.mode-operator');
    const debugSections = document.querySelectorAll('.mode-debug');

    operatorSections.forEach(el => {
        el.style.display = (mode === 'operator' || mode === 'debug') ? '' : 'none';
    });

    debugSections.forEach(el => {
        el.style.display = (mode === 'debug') ? '' : 'none';
    });

    // Load debug data if switching to debug mode
    if (mode === 'debug') {
        fetchLogs();
        updateLatencyHistogram();
    }

    // Re-render with current state if available
    if (lastState) {
        updateDashboard(lastState);
    }
}

// ── Trace Filters ────────────────────────────────────────────

function initTraceFilters() {
    const statusSelect = document.getElementById('trace-filter-status');
    const searchInput = document.getElementById('trace-filter-search');

    if (statusSelect) {
        statusSelect.addEventListener('change', () => {
            if (lastState) updateTracesTable(lastState.traces);
        });
    }
    if (searchInput) {
        searchInput.addEventListener('input', () => {
            if (lastState) updateTracesTable(lastState.traces);
        });
    }
}

// ── Clock ────────────────────────────────────────────────────

function initClock() {
    function tick() {
        const now = new Date();
        document.getElementById('clock').textContent =
            now.toLocaleTimeString('ko-KR', { hour12: false });
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
    document.getElementById('footer-uptime').textContent = `Uptime: ${str}`;
}

// ── Utilities ────────────────────────────────────────────────

function setText(id, text, extraClass) {
    const el = document.getElementById(id);
    if (!el) return;
    el.textContent = text;
    if (extraClass !== undefined) {
        el.className = 'metric-value ' + (extraClass || '');
    }
}

function truncate(str, max) {
    if (!str) return '';
    return str.length > max ? str.substring(0, max) + '...' : str;
}

function formatValue(val) {
    if (val === null || val === undefined) return '--';
    if (typeof val === 'number') return val.toLocaleString();
    return String(val);
}

function escapeHtml(str) {
    if (!str) return '';
    return str.replace(/&/g, '&amp;')
              .replace(/</g, '&lt;')
              .replace(/>/g, '&gt;')
              .replace(/"/g, '&quot;');
}
