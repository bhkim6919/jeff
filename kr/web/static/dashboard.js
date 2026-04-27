/**
 * dashboard.js -- Q-TRON REST Monitor v2
 * ========================================
 * SSE client, progressive disclosure mode switcher, data rendering.
 * Vanilla JS only, no dependencies.
 */

// ── State ────────────────────────────────────────────────────

// Phase 2-F (2026-04-25): MODE_KEY / currentMode constants removed.
// They were the last residue of the Basic/Operator/Debug 3-mode system
// retired in P0-2 C3 — kept temporarily as forward-compat seeds, but
// nothing references them anymore (verified via grep over the whole
// file). The localStorage key `qtron_monitor_mode` is left untouched
// in browsers; it's harmless cookie data.
let sseSource = null;
let sseReconnectTimer = null;
let sseReconnectCount = 0;
const SSE_MAX_RECONNECT = 30;
const SSE_RECONNECT_DELAY = 3000;
const startTime = Date.now();
let lastState = null;
let prevState = null;     // for diff tracking
let lastSnapshotId = -1;  // out-of-order drop용
let diffEntries = [];     // state diff log (debug mode)
const MAX_DIFF_ENTRIES = 50;

// ── Initialization ───────────────────────────────────────────

document.addEventListener('DOMContentLoaded', async () => {
    // P0-2 C3: initModeSwitcher() removed — no 3-mode UI anymore.
    initClock();
    initLogRefresh();
    initBatchLogPanel();   // R14 D-BATCH
    initQobsPanel();       // R14 D-QOBS
    initTraceFilters();
    initAlertClose();
    initCopyJson();
    initProfitTabs();
    initTestOrder();
    initDataEventsPanel();    // P2.4: Data supply 이벤트 + Market Context 패널

    // SSE 연결 전 캐시 스냅샷 선호출 → 빈 화면 제거
    try {
        const r = await fetch('/api/state');
        const d = await r.json();
        if (d && !d.error && !d.loading) {
            if (d.snapshot_id !== undefined) lastSnapshotId = d.snapshot_id;
            updateDashboard(d);
        }
    } catch (_) {}

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
            // out-of-order drop: 역전된 패킷만 버림 (동일 id는 허용)
            if (lastSnapshotId !== -1
                && data.snapshot_id !== undefined
                && data.snapshot_id < lastSnapshotId) return;
            if (data.snapshot_id !== undefined) lastSnapshotId = data.snapshot_id;
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
        markDisconnected();
        if (sseSource) sseSource.close();
        sseSource = null;

        if (sseReconnectCount < SSE_MAX_RECONNECT) {
            sseReconnectCount++;
            sseReconnectTimer = setTimeout(connectSSE, SSE_RECONNECT_DELAY);
        } else {
            updateSSEBar('dead');
            markDisconnected();
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

// ── Disconnected State ───────────────────────────────────────

function markDisconnected() {
    // Hero → 연결 끊김
    const dot = document.getElementById('health-dot');
    const label = document.getElementById('health-label');
    const reason = document.getElementById('health-reason');
    if (dot) dot.className = 'health-dot health-red';
    if (label) label.textContent = '서버 연결 끊김';
    if (reason) reason.textContent = 'SSE 스트림 중단 — 서버 상태 확인 필요';

    // LED → 빨강
    const led = document.getElementById('status-led');
    if (led) led.className = 'status-led led-red';

    // BUY/RISK badges → unknown
    const buyBadge = document.getElementById('buy-status-badge');
    const riskBadge = document.getElementById('system-risk-badge');
    if (buyBadge) { buyBadge.className = 'status-badge badge-unknown'; buyBadge.textContent = 'BUY: --'; }
    if (riskBadge) { riskBadge.className = 'status-badge badge-readfail'; riskBadge.textContent = 'RISK: DISCONNECTED'; }

    // Emergency badge
    const emergBadge = document.getElementById('emergency-badge');
    if (emergBadge) { emergBadge.style.display = 'inline-flex'; emergBadge.textContent = 'DISCONNECTED'; }
}

// ── Main Dashboard Update ────────────────────────────────────

function updateDashboard(data) {
    // Always update
    updateHero(data);
    updateSummaryCards(data);
    storeAccountData(data);
    updateRebalSchedule(data);
    updateHoldingsList(data);
    updateAlertBanner(data);
    updateControlCards(data);
    updateFreshnessGrid(data.freshness);
    updateUptime();
    updateDDGuard(data);
    updateHeroBadges(data);
    updateRegimeDisplay(data);
    updateDonutChart(data);

    updateSectorView(data);
    updateSectorRegime(data);
    updateTrailStops(data);
    updateReconCard(data);
    updateIndexDisplay(data);
    updateTradesTimeline(data);

    // P0-2 C3: Mode gates removed — all update functions are called
    // unconditionally. Each one has its own null-check guard (C2) that
    // makes it a no-op when its target DOM isn't on the current page.
    //   Dashboard has sec-ws-sync → updateWSCard/updateTimestampsCard render.
    //   Dashboard lacks sec-traces/sec-sync/sec-raw-json/etc → those no-op.
    //   /debug has sec-traces/sec-sync/sec-raw-json/... → they render.
    updateTracesTable(data.traces);
    updateWSCard(data.websocket);
    updateTimestampsCard(data.timestamps);
    updateSyncTable(data.sync);
    updateRawJson(data);
    updateLatencyHistogram();
    updateStateDiff(data);

    // P1 (2026-04-26): Top status pill bar — synthesized worst-effective
    // state across ENGINE / BUY / RECON / DATA + recent ALERT count.
    // Renders nothing when all systems are normal (3-second test policy).
    try {
        const synth = synthesizeStatus(data);
        renderStatusBar(synth);
        // Cache for status cards (P2) so they render from same source
        window._qcLastSynth = synth;
        renderStatusCards(synth, data);
    } catch (e) {
        console.warn('[StatusBar] render error:', e);
    }
}

// ── Hero Status ──────────────────────────────────────────────

// Helper: KST market phase (PRE_OPEN / OPEN / AFTER_HOURS / WEEKEND).
// Pure client-side — relies on the user's wall clock interpreted as KST.
// Used to soften ENGINE_OFFLINE banner after market close.
function getKrMarketPhase(now) {
    const d = now || new Date();
    const dow = d.getDay();           // 0=Sun, 6=Sat
    if (dow === 0 || dow === 6) return 'WEEKEND';
    const minutes = d.getHours() * 60 + d.getMinutes();
    if (minutes < 9 * 60)        return 'PRE_OPEN';     // before 09:00
    if (minutes >= 15 * 60 + 30) return 'AFTER_HOURS';  // 15:30 onward
    return 'OPEN';
}

// ── synthesizeStatus (Phase P1, 2026-04-26) ──────────────────
// Read-only synthesis of the snapshot into a single object that
// drives the top status-pill bar. Pure function — does NOT mutate
// snapshot, does NOT call APIs, does NOT change behavior.
//
// Output:
//   {
//     engine: 'OK' | 'OFFLINE' | 'STALE',
//     buy:    'OK' | 'BLOCKED',
//     buyReason: '' | 'ENGINE' | 'DD' | 'RECON' | <blocker_code>,
//     recon:  'OK' | 'UNAVAILABLE' | 'UNRELIABLE',
//     data:   'OK' | 'AGING' | 'STALE',
//     alerts: <integer count of recent toasts (10min window)>,
//     worst:  'OK' | 'INFO' | 'WARN' | 'CRITICAL',
//     pills:  [{ key, label, sub, level, scrollTarget }, ...]
//   }
//
// `pills[]` is the source-of-truth for what to render in the bar.
// Empty array = all systems normal, hide the bar entirely.
//
// Worst-effective rule: BUY=BLOCKED + ENGINE_OFFLINE → single pill
// "신규 매수 차단 (ENGINE)" with critical level. The other pills
// can still surface (RECON, DATA) but BUY is the operational
// headline.
function synthesizeStatus(data) {
    const empty = {
        engine: 'UNKNOWN', buy: 'UNKNOWN', buyReason: '',
        recon: 'UNKNOWN', data: 'UNKNOWN', alerts: 0,
        worst: 'OK', pills: [],
    };
    if (!data || typeof data !== 'object') return empty;

    const engineSt = data.engine_status || {};
    const ddGuard  = data.dd_guard || {};
    const reconSt  = data.recon || {};
    const auto     = data.auto_trading || {};
    const cacheAge = Number(data.cache_age_sec || 0);

    // ENGINE
    let engine = 'OK';
    let engineReason = '';
    if ((engineSt.status || '').toUpperCase() === 'OFFLINE') {
        engine = 'OFFLINE';
        engineReason = engineSt.reason || 'engine offline';
    } else if (cacheAge > 600) {
        engine = 'STALE';
        engineReason = `cache age ${Math.round(cacheAge)}s`;
    }

    // BUY EFFECTIVE — synthesized priority: ENGINE > AUTO > DD > RECON
    let buy = 'OK', buyReason = '', buyDetail = '';
    if (engine === 'OFFLINE') {
        buy = 'BLOCKED'; buyReason = 'ENGINE'; buyDetail = engineReason;
    } else if (auto.enabled === false) {
        buy = 'BLOCKED';
        buyReason = (auto.highest_priority_blocker
                     || (Array.isArray(auto.blockers) && auto.blockers[0])
                     || 'AUTO');
        buyDetail = auto.reason_summary || '';
    } else if ((ddGuard.buy_permission || '').toUpperCase() === 'BLOCKED') {
        buy = 'BLOCKED'; buyReason = 'DD';
        const dailyDd   = ddGuard.daily_dd != null ? (ddGuard.daily_dd * 100).toFixed(1) + '%' : '?';
        const monthlyDd = ddGuard.monthly_dd != null ? (ddGuard.monthly_dd * 100).toFixed(1) + '%' : '?';
        buyDetail = `daily ${dailyDd}, monthly ${monthlyDd}`;
    } else if (reconSt.unreliable) {
        // RECON unreliability shouldn't outright block BUY but advisory only;
        // surface separately below as its own pill.
    }

    // RECON
    let recon = 'OK';
    let reconDetail = '';
    if ((reconSt.status || '').toUpperCase() === 'UNAVAILABLE') {
        recon = 'UNAVAILABLE';
        reconDetail = reconSt.engine_reason || 'recon unavailable';
    } else if (reconSt.unreliable) {
        recon = 'UNRELIABLE';
        reconDetail = reconSt.reason || 'broker sync unreliable';
    }

    // DATA freshness (cache_age_sec is the most authoritative single field)
    let dataFresh = 'OK';
    if (cacheAge > 600)      dataFresh = 'STALE';
    else if (cacheAge > 300) dataFresh = 'AGING';

    // ALERTS — toast ring buffer (qc-actions.js exposes recentCount)
    let alerts = 0;
    try {
        if (window.qcToast && typeof window.qcToast.recentCount === 'function') {
            alerts = window.qcToast.recentCount(10 * 60 * 1000) || 0;
        }
    } catch (_) { alerts = 0; }

    // Build pill array — only abnormal states surface
    const pills = [];
    const ICON = { ok: '✓', info: 'i', warn: '⚠', critical: '✗' };

    if (engine !== 'OK') {
        pills.push({
            key: 'engine',
            label: engine === 'OFFLINE' ? 'ENGINE OFFLINE' : 'ENGINE STALE',
            sub: engineReason,
            level: engine === 'OFFLINE' ? 'critical' : 'warn',
            icon: engine === 'OFFLINE' ? ICON.critical : ICON.warn,
            scrollTarget: 'hero',
        });
    }
    if (buy !== 'OK') {
        pills.push({
            key: 'buy',
            label: `신규 매수 차단 (${buyReason})`,
            sub: buyDetail,
            level: 'critical',
            icon: ICON.critical,
            scrollTarget: 'card-buy-permission',
        });
    }
    if (recon !== 'OK') {
        pills.push({
            key: 'recon',
            label: recon === 'UNAVAILABLE' ? 'RECON 불가' : 'RECON 불안정',
            sub: reconDetail,
            level: recon === 'UNAVAILABLE' ? 'critical' : 'warn',
            icon: recon === 'UNAVAILABLE' ? ICON.critical : ICON.warn,
            scrollTarget: 'card-data-freshness',
        });
    }
    if (dataFresh !== 'OK') {
        pills.push({
            key: 'data',
            label: dataFresh === 'STALE' ? `DATA STALE (${Math.round(cacheAge)}s)` : `DATA AGING (${Math.round(cacheAge)}s)`,
            sub: '',
            level: dataFresh === 'STALE' ? 'warn' : 'info',
            icon: dataFresh === 'STALE' ? ICON.warn : ICON.info,
            scrollTarget: 'card-data-freshness',
        });
    }
    if (alerts > 0) {
        pills.push({
            key: 'alerts',
            label: `${alerts} ALERTS`,
            sub: '최근 10분 토스트',
            level: 'info',
            icon: ICON.info,
            scrollTarget: null,
        });
    }

    // Worst-effective level
    const levelRank = { ok: 0, info: 1, warn: 2, critical: 3 };
    const worstRank = pills.reduce((m, p) => Math.max(m, levelRank[p.level] || 0), 0);
    const worst = ['OK', 'INFO', 'WARN', 'CRITICAL'][worstRank];

    return { engine, buy, buyReason, recon, data: dataFresh, alerts, worst, pills };
}

// Render the top status pill bar from synthesized state.
// Container: <div id="qc-status-bar" class="qc-status-bar"></div>
// Empty pills[] → bar hidden entirely (3-second test: zero noise when OK).
function renderStatusBar(synth) {
    const bar = document.getElementById('qc-status-bar');
    if (!bar) return;
    if (!synth || !synth.pills || synth.pills.length === 0) {
        bar.innerHTML = '';
        bar.classList.remove('qc-status-bar-active');
        return;
    }
    bar.classList.add('qc-status-bar-active');
    bar.innerHTML = synth.pills.map(p => {
        const safeSub = (p.sub || '').replace(/"/g, '&quot;');
        const tip = (p.label + (p.sub ? ' — ' + p.sub : ''));
        const target = p.scrollTarget ? `data-scroll="${p.scrollTarget}"` : '';
        return `<button class="qc-pill qc-pill-${p.level}" type="button"
                       title="${tip.replace(/"/g, '&quot;')}"
                       ${target}
                       onclick="qcPillClick(this)">
            <span class="qc-pill-icon">${p.icon}</span>
            <span class="qc-pill-label">${p.label}</span>
        </button>`;
    }).join('');
}

// Pill click → scrollIntoView the related card + brief highlight
function qcPillClick(btn) {
    const id = btn.getAttribute('data-scroll');
    if (!id) return;
    const target = document.getElementById(id);
    if (!target) return;
    target.scrollIntoView({ behavior: 'smooth', block: 'center' });
    target.classList.add('qc-highlight-pulse');
    setTimeout(() => target.classList.remove('qc-highlight-pulse'), 2000);
}

// ── P2 status cards (Hybrid summary, top row) ─────────────
// Renders the 4 OPERATIONAL status cards from synthesized state.
// The 4 ACCOUNT metric cards (holdings/cash/equity/pnl) keep their
// existing render path via qc.summary.render() — untouched.
function renderStatusCards(synth, data) {
    const set = (id, value, level, sub) => {
        const card = document.getElementById(id);
        if (!card) return;
        const v = card.querySelector('.status-card-value');
        const s = card.querySelector('.status-card-sub');
        if (v) v.textContent = value || '--';
        if (s) s.textContent = sub || '';
        card.className = 'status-card status-card-' + (level || 'unknown');
    };

    if (!synth) {
        set('card-buy-permission', '--', 'unknown', '');
        set('card-dd-status',      '--', 'unknown', '');
        set('card-rebal-status',   '--', 'unknown', '');
        set('card-data-freshness', '--', 'unknown', '');
        return;
    }

    // 1) Buy Permission
    set('card-buy-permission',
        synth.buy === 'OK' ? 'NORMAL' : `BLOCKED (${synth.buyReason})`,
        synth.buy === 'OK' ? 'ok' : 'critical',
        synth.buy === 'OK' ? '신규 매수 허용' : '신규 매수 차단');

    // 2) DD Status — pull from data.dd_guard
    const dd = (data && data.dd_guard) || {};
    const daily   = dd.daily_dd != null ? (dd.daily_dd * 100).toFixed(1) + '%' : '--';
    const monthly = dd.monthly_dd != null ? (dd.monthly_dd * 100).toFixed(1) + '%' : '--';
    let ddLevel = 'ok';
    if (dd.monthly_dd != null && dd.monthly_dd < -0.05) ddLevel = 'critical';
    else if (dd.daily_dd != null && dd.daily_dd < -0.03) ddLevel = 'warn';
    set('card-dd-status',
        `${daily} / ${monthly}`,
        ddLevel,
        'daily / monthly');

    // 3) Rebalance Status — D-day from rebalance schedule (if available)
    const reb = (data && data.rebalance) || {};
    const dDay = reb.d_day != null ? `D-${reb.d_day}` : (reb.next ? `next ${reb.next}` : '--');
    let rebLevel = 'ok';
    if (reb.d_day === 0) rebLevel = 'warn';   // today
    else if (reb.d_day === 1) rebLevel = 'info';
    set('card-rebal-status', dDay, rebLevel, reb.window || '');

    // 4) Data / RECON Freshness
    const cacheAge = Math.round(Number((data && data.cache_age_sec) || 0));
    let freshLabel = '--', freshLevel = 'ok';
    if (synth.data === 'STALE')      { freshLabel = `STALE ${cacheAge}s`; freshLevel = 'warn'; }
    else if (synth.data === 'AGING') { freshLabel = `AGING ${cacheAge}s`; freshLevel = 'info'; }
    else                              { freshLabel = `${cacheAge}s`; freshLevel = 'ok'; }
    if (synth.recon !== 'OK')        { freshLabel += ` · RECON ${synth.recon}`; freshLevel = 'warn'; }
    set('card-data-freshness', freshLabel, freshLevel, 'cache age / recon');
}

// Diagnostics toggle (P3 spec). Default = operator view (diag hidden).
// User opts in via the ⚙ button; choice persists in localStorage.
function qtronToggleDiagnostics() {
    const next = !document.body.classList.contains('qtron-diagnostics-on');
    document.body.classList.toggle('qtron-diagnostics-on', next);
    try { localStorage.setItem('qtron_diagnostics', next ? '1' : '0'); } catch (_) {}
    const btn = document.getElementById('qtron-diag-toggle');
    if (btn) btn.textContent = next ? '⚙ Hide Diagnostics' : '⚙ Show Diagnostics';
}
// Bootstrap on first paint
(function () {
    try {
        if (localStorage.getItem('qtron_diagnostics') === '1') {
            document.body.classList.add('qtron-diagnostics-on');
            const apply = () => {
                const btn = document.getElementById('qtron-diag-toggle');
                if (btn) btn.textContent = '⚙ Hide Diagnostics';
            };
            if (document.readyState === 'loading') {
                document.addEventListener('DOMContentLoaded', apply);
            } else {
                apply();
            }
        }
    } catch (_) {}
})();

// ── Telegram Alerts (24h) panel — polling lifecycle ────────
// Self-contained module; renders into #tg-alerts-list inside the
// .qtron-diag-only section. Polling only runs while the panel is
// visible (qtronToggleDiagnostics drives start/stop).
const qcAlerts = (function () {
    const POLL_MS = 30000;
    let timer = null;
    let inflight = false;

    function escapeHtml(s) {
        return String(s || '')
            .replace(/&/g, '&amp;').replace(/</g, '&lt;').replace(/>/g, '&gt;')
            .replace(/"/g, '&quot;').replace(/'/g, '&#39;');
    }
    function fmtTime(iso) {
        try {
            const d = new Date(iso);
            const hh = String(d.getHours()).padStart(2, '0');
            const mm = String(d.getMinutes()).padStart(2, '0');
            const ss = String(d.getSeconds()).padStart(2, '0');
            return `${hh}:${mm}:${ss}`;
        } catch (_) { return '--:--:--'; }
    }
    function render(items) {
        const list = document.getElementById('tg-alerts-list');
        const count = document.getElementById('tg-alerts-count');
        const upd = document.getElementById('tg-alerts-updated');
        if (!list) return;
        if (count) count.textContent = `${items.length} alerts`;
        if (upd) upd.textContent = `updated ${fmtTime(new Date().toISOString())}`;
        if (!items.length) {
            list.innerHTML = '<div class="tg-alerts-empty">No Telegram alerts in last 24h</div>';
            return;
        }
        list.innerHTML = items.map(it => {
            const lvl = (it.level || 'INFO').toLowerCase();
            const status = it.send_status === 'failed' ? 'failed' : 'sent';
            const cls = `tg-alert-row lvl-${lvl} status-${status}`;
            return `<div class="${cls}" onclick="qcAlerts.toggleRow(this)">
                <span class="tg-alert-time">${escapeHtml(fmtTime(it.ts))}</span>
                <span class="tg-alert-level">${escapeHtml(it.level || 'INFO')}</span>
                <span class="tg-alert-source">${escapeHtml(it.source || '?')}</span>
                <div class="tg-alert-body">${escapeHtml(it.message || it.title || '')}</div>
                <span class="tg-alert-status">${escapeHtml(status)}</span>
            </div>`;
        }).join('');
    }
    async function fetchOnce() {
        if (inflight) return;
        inflight = true;
        try {
            const r = await fetch('/api/alerts/recent');
            const j = await r.json();
            render(Array.isArray(j.items) ? j.items : []);
        } catch (e) {
            // Soft fail — leave previous render in place.
        } finally {
            inflight = false;
        }
    }
    function start() {
        if (timer) return;
        fetchOnce();
        timer = setInterval(fetchOnce, POLL_MS);
    }
    function stop() {
        if (timer) { clearInterval(timer); timer = null; }
    }
    function toggleRow(el) {
        if (el && el.classList) el.classList.toggle('expanded');
    }
    return { start, stop, fetchOnce, toggleRow };
})();

// Expose for debugging + nav.js + tests
window.synthesizeStatus = synthesizeStatus;
window.renderStatusBar  = renderStatusBar;
window.renderStatusCards = renderStatusCards;
window.qcPillClick      = qcPillClick;
window.qtronToggleDiagnostics = qtronToggleDiagnostics;
window.qcAlerts         = qcAlerts;

function updateHero(data) {
    const health = data.health;
    const statusLower = health.status.toLowerCase();

    // Hero block (Dashboard only — absent on /debug). Guard at first
    // element so /debug doesn't crash on dot.className = ...
    const dot = document.getElementById('health-dot');
    if (dot) {
        const label = document.getElementById('health-label');
        const reason = document.getElementById('health-reason');

        // After-hours softening: when status is RED purely because the
        // engine is OFFLINE and we're past 15:30 KST (or weekend), the
        // engine being idle is expected, not an incident. Show a friendly
        // "End of Market" label with yellow dot instead of the alarming
        // red "오류 발생" banner. Underlying engine_status, blockers, and
        // BUY:BLOCKED guards are unchanged.
        const phase = getKrMarketPhase();
        const reasonText = (health.reason || '');
        const isEngineOffline = reasonText.startsWith('ENGINE_OFFLINE');
        const isAfterHours = (phase === 'AFTER_HOURS' || phase === 'WEEKEND');
        const eodMode = (statusLower === 'red' && isEngineOffline && isAfterHours);

        const visualStatus = eodMode ? 'eod' : statusLower;
        dot.className = 'health-dot health-' + visualStatus;

        const statusLabels = {
            green: '시스템 정상',
            yellow: '주의 필요',
            red: '오류 발생',
            black: '연결 안됨',
            eod: 'End of Market',
        };
        if (label) label.textContent = statusLabels[visualStatus] || health.status;
        if (reason) {
            reason.textContent = eodMode
                ? '장 마감 — 엔진 대기 모드 (다음 영업일 준비)'
                : health.reason;
        }

        // Hero meta
        const serverEl = document.getElementById('hero-server');
        if (serverEl) serverEl.textContent = data.server.type;

        const tokenEl = document.getElementById('hero-token');
        if (tokenEl) {
            if (data.token.valid && data.token.remaining_sec > 0) {
                tokenEl.textContent = data.token.remaining_str + ' 남음';
                tokenEl.className = 'meta-value' + (data.token.remaining_sec < 600 ? ' status-warn' : '');
            } else {
                tokenEl.textContent = '만료됨';
                tokenEl.className = 'meta-value status-error';
            }
        }

        const syncEl = document.getElementById('hero-sync');
        if (syncEl) {
            if (data.timestamps.last_request) {
                syncEl.textContent = data.timestamps.last_request;
            } else {
                syncEl.textContent = '대기 중';
            }
        }
    }

    // Server badge + LED → nav bar (these may live on both pages, keep separate guards).
    const badge = document.getElementById('server-badge');
    if (badge) {
        badge.textContent = data.server.type;
        badge.className = 'badge badge-' + data.server.type.toLowerCase();
    }
    const led = document.getElementById('status-led');
    if (led) {
        const ledClass = {green:'led-green', yellow:'led-yellow', red:'led-red', black:'led-black'}[statusLower] || 'led-black';
        led.className = 'status-led ' + ledClass;
    }

    // Nav badges (REAL + dot) — present on both Dashboard and /debug.
    const nb = document.getElementById('qnav-badges');
    if (nb) {
        const svr = data.server?.type || '--';
        const dotCls = statusLower === 'green' ? 'ok' : statusLower === 'red' ? 'err' : '';
        // Build once, then update
        if (!nb._initialized) {
            // P0-2 C3: Basic/Operator/Debug toggle removed — Dashboard is
            // now always the full operator view; Debug lives on /debug.
            nb.innerHTML =
                `<span id="nav-svr" class="qnav-badge qnav-badge-${svr.toLowerCase()}">${svr}</span>` +
                `<span id="nav-dot" class="qnav-dot ${dotCls}"></span>`;
            nb._initialized = true;
        }
        // Always update badge + dot
        const navSvr = document.getElementById('nav-svr');
        if (navSvr) { navSvr.textContent = svr; navSvr.className = 'qnav-badge qnav-badge-' + svr.toLowerCase(); }
        const navDot = document.getElementById('nav-dot');
        if (navDot) navDot.className = 'qnav-dot ' + dotCls;
    }
}

// ── Summary Cards ────────────────────────────────────────────

function updateSummaryCards(data) {
    // Phase 3 (2026-04-25): delegate to qc-summary-card component.
    // No logic change — summary.js is a byte-for-byte extraction of the
    // previous body (git blame this commit to see the original).
    // If the component bundle didn't load for some reason, fall back silently
    // to avoid dashboard crash.
    if (window.qc && window.qc.summary && typeof window.qc.summary.render === 'function') {
        window.qc.summary.render(document.getElementById('summary-cards'), data);
    }

    // Flash on changed values — compares prev numeric text and pulses the
    // card green/red briefly. Pure visual — no behavior change.
    try {
        const cards = document.querySelectorAll('.summary-card');
        cards.forEach((card) => {
            const valEl = card.querySelector('.summary-value');
            if (!valEl) return;
            const cur = (valEl.textContent || '').replace(/[^0-9.\-]/g, '');
            const prev = card.dataset.prevVal;
            if (prev !== undefined && prev !== '' && cur !== '' && cur !== prev) {
                const up = parseFloat(cur) >= parseFloat(prev);
                card.classList.remove('flash-up', 'flash-down');
                void card.offsetWidth;
                card.classList.add(up ? 'flash-up' : 'flash-down');
            }
            card.dataset.prevVal = cur;
        });
    } catch (e) { /* visual only */ }
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

    // R24 (2026-04-23): prefer server-side trading-calendar values.
    // API returns next_date ("YYYY.MM.DD") + d_day (trading days remaining).
    // Only fall back to client-side calendar approx when API omits them.
    if (rebal.next_date) {
        nextEl.textContent = rebal.next_date;
    } else {
        // Legacy calendar approx (21 trading days ≈ 30 calendar days)
        const lastDt = new Date(parseInt(ly), parseInt(lm)-1, parseInt(ld));
        const calendarDays = Math.round(cycle * 1.4);
        const nextDt = new Date(lastDt.getTime() + calendarDays * 86400000);
        const ny = nextDt.getFullYear();
        const nm = String(nextDt.getMonth()+1).padStart(2,'0');
        const nd = String(nextDt.getDate()).padStart(2,'0');
        nextEl.textContent = `${ny}.${nm}.${nd}`;
    }

    if (typeof rebal.d_day === 'number') {
        const d = rebal.d_day;
        if (d > 0) {
            ddayEl.textContent = `D-${d}`;
            ddayEl.className = 'rebal-dday' + (d <= 3 ? ' urgent' : '');
        } else if (d === 0) {
            ddayEl.textContent = 'D-DAY';
            ddayEl.className = 'rebal-dday urgent';
        } else {
            ddayEl.textContent = `D+${Math.abs(d)}`;
            ddayEl.className = 'rebal-dday overdue';
        }
    }
    // Note: updateRebalStatus() also writes rebal-dday from /api/rebalance/status.
    // Both paths now share the same trading-calendar semantics, so whichever
    // fires last yields the same value (no race-induced flicker).
}

// ── Rebalance Control Panel ──────────────────────────
let _rebalConfirmAction = null;
let _rebalPreviewHash = '';
let _rebalCycleId = '';

function updateRebalStatus() {
    fetch('/api/rebalance/status')
        .then(r => r.json())
        .then(data => {
            const phaseEl = document.getElementById('rebal-phase');
            const actionsEl = document.getElementById('rebal-actions');
            const btnSell = document.getElementById('btn-rebal-sell');
            const btnBuy = document.getElementById('btn-rebal-buy');
            const btnSkip = document.getElementById('btn-rebal-skip');
            const modeBtn = document.getElementById('rebal-mode-btn');
            const blockedBadge = document.getElementById('rebal-blocked-badge');
            const ddayEl = document.getElementById('rebal-dday');

            if (!phaseEl) return;

            // Phase display
            phaseEl.textContent = data.phase || 'IDLE';
            phaseEl.style.color =
                data.phase === 'SELL_READY' ? '#e53e3e' :
                data.phase === 'BUY_READY' ? '#38a169' :
                data.phase === 'IDLE' ? '#a0aec0' : '#f6ad55';

            // D-day from server (accurate trading days)
            if (ddayEl && data.d_day !== undefined) {
                if (data.d_day > 0) {
                    ddayEl.textContent = `D-${data.d_day}`;
                    ddayEl.className = 'rebal-dday' + (data.d_day <= 5 ? ' urgent' : '');
                } else if (data.d_day === 0) {
                    ddayEl.textContent = 'D-DAY';
                    ddayEl.className = 'rebal-dday urgent';
                } else {
                    ddayEl.textContent = `D+${Math.abs(data.d_day)}`;
                    ddayEl.className = 'rebal-dday overdue';
                }
            }

            // Mode button
            if (modeBtn) {
                const mode = data.mode || 'manual';
                modeBtn.textContent = mode.toUpperCase();
                modeBtn.className = 'rebal-mode-btn mode-' + mode;
            }

            // Always show action panel (US와 동일 — 버튼 상태로 제어)
            if (actionsEl) actionsEl.style.display = 'block';

            // Blocked badge
            if (blockedBadge) {
                blockedBadge.style.display = data.blocked ? 'inline' : 'none';
                blockedBadge.title = data.blocked_reason || '';
            }

            // Buttons + disable reasons as title
            if (btnSell) {
                btnSell.disabled = !data.can_sell || data.is_running;
                btnSell.title = data.sell_disable_reason || '';
            }
            if (btnBuy) {
                btnBuy.disabled = !data.can_buy || data.is_running;
                btnBuy.title = data.buy_disable_reason || '';
            }
            if (btnSkip) btnSkip.disabled = !data.can_skip || data.is_running;

            // Preview button
            const btnPreview = document.getElementById('btn-rebal-preview');
            if (btnPreview) btnPreview.disabled = !data.can_preview || data.is_running;

            // Sell/Buy status
            const sellEl = document.getElementById('rebal-sell-status');
            const buyEl = document.getElementById('rebal-buy-status');
            if (sellEl) {
                const ss = data.sell_status || '';
                sellEl.textContent = ss || '--';
                sellEl.style.color = ss === 'COMPLETE' ? '#38a169' : ss ? '#f6ad55' : '#a0aec0';
            }
            if (buyEl) {
                const bs = data.buy_status || '';
                if (data.pending_buys_count > 0 && data.pending_age_warn) {
                    buyEl.textContent = `PENDING (${data.pending_age_days}d)`;
                    buyEl.style.color = '#e53e3e';
                } else if (data.pending_buys_count > 0) {
                    buyEl.textContent = `PENDING (${data.pending_age_days || 0}d)`;
                    buyEl.style.color = '#f6ad55';
                } else {
                    buyEl.textContent = bs || '--';
                    buyEl.style.color = bs === 'COMPLETE' ? '#38a169' : bs ? '#f6ad55' : '#a0aec0';
                }
            }

            // Window status
            const winEl = document.getElementById('rebal-window-status');
            if (winEl) {
                if (data.in_window) {
                    winEl.textContent = '✅ Open';
                    winEl.style.color = '#38a169';
                } else {
                    winEl.textContent = `Closed (${data.trading_days_since || 0}/${data.threshold || 21})`;
                    winEl.style.color = '#a0aec0';
                }
            }

            // Block reasons
            const brEl = document.getElementById('rebal-block-reasons');
            if (brEl) {
                const reasons = [];
                if (data.blocked) reasons.push(data.blocked_reason || 'BLOCKED');
                if (data.sell_disable_reason && data.phase !== 'IDLE') reasons.push('Sell: ' + data.sell_disable_reason);
                if (data.buy_disable_reason && data.phase !== 'IDLE') reasons.push('Buy: ' + data.buy_disable_reason);
                if (reasons.length > 0) {
                    brEl.textContent = reasons.join(' | ');
                    brEl.style.display = 'block';
                } else {
                    brEl.style.display = 'none';
                }
            }

            // Pending buys age warning
            if (data.pending_age_warn) {
                const resultEl = document.getElementById('rebal-result');
                if (resultEl) {
                    resultEl.style.display = 'block';
                    resultEl.className = 'rebal-result error';
                    resultEl.textContent = `Pending buys: ${data.pending_age_days} days old`;
                }
            }
        })
        .catch(() => {});
}

function toggleRebalMode() {
    const btn = document.getElementById('rebal-mode-btn');
    const current = (btn.textContent || '').toLowerCase();
    const next = current === 'manual' ? 'auto' : 'manual';
    fetch('/api/rebalance/mode', {
        method: 'POST',
        headers: {'Content-Type': 'application/json'},
        body: JSON.stringify({mode: next}),
    }).then(() => updateRebalStatus());
}

function rebalPreview() {
    const area = document.getElementById('rebal-preview-area');
    const btn = document.getElementById('btn-rebal-preview');
    if (!area) return;
    btn.textContent = 'Loading...';
    btn.disabled = true;

    fetch('/api/rebalance/preview')
        .then(r => r.json())
        .then(data => {
            area.style.display = 'block';
            btn.textContent = 'Preview';
            btn.disabled = false;

            if (data.error) {
                area.innerHTML = `<div class="rebal-errors">${data.error}</div>`;
                return;
            }

            // Save preview hash for SELL/BUY command validation
            _rebalPreviewHash = data.preview_hash || '';
            _rebalCycleId = data.cycle_id || '';

            // Sell list
            const sellList = document.getElementById('rebal-sell-list');
            const sellCount = document.getElementById('rebal-sell-count');
            sellCount.textContent = data.sells.length;
            sellList.innerHTML = data.sells.map(s =>
                `<div class="rebal-order-item">
                    <span class="code">${s.code}</span>
                    <span class="qty">${s.qty}@${Math.round(s.price).toLocaleString()}</span>
                    <span class="${s.pnl_pct >= 0 ? 'pnl-pos' : 'pnl-neg'}">${(s.pnl_pct*100).toFixed(1)}%</span>
                </div>`
            ).join('') || '<div style="color:#718096">No sells</div>';

            // Buy list
            const buyList = document.getElementById('rebal-buy-list');
            const buyCount = document.getElementById('rebal-buy-count');
            buyCount.textContent = data.buys.length;
            buyList.innerHTML = data.buys.map(b =>
                `<div class="rebal-order-item">
                    <span class="code">${b.code}</span>
                    <span class="qty">${b.est_qty}@${Math.round(b.price).toLocaleString()}</span>
                    <span style="color:#718096">mom:${b.mom.toFixed(2)}</span>
                </div>`
            ).join('') || '<div style="color:#718096">No buys</div>';

            // Errors
            const errEl = document.getElementById('rebal-preview-errors');
            if (data.errors && data.errors.length > 0) {
                errEl.textContent = data.errors.join(', ');
            } else {
                errEl.textContent = '';
            }

            // Enable sell button after preview
            const btnSell = document.getElementById('btn-rebal-sell');
            if (btnSell && data.sells.length > 0) btnSell.disabled = false;
        })
        .catch(e => {
            btn.textContent = 'Preview';
            btn.disabled = false;
        });
}

function rebalSell() {
    _rebalConfirmAction = 'sell';
    _showConfirm('Execute SELL', 'Sell orders will be executed immediately. Continue?');
}

function rebalBuy() {
    _rebalConfirmAction = 'buy';
    _showConfirm('Execute BUY', 'Buy orders will be executed immediately. Continue?');
}

function rebalSkip() {
    _rebalConfirmAction = 'skip';
    _showConfirm('Skip Cycle', 'Reset rebalance counter without buying. Continue?');
}

function _showConfirm(title, msg) {
    document.getElementById('rebal-confirm-title').textContent = title;
    document.getElementById('rebal-confirm-msg').textContent = msg;
    document.getElementById('rebal-confirm-overlay').style.display = 'flex';
}

function rebalConfirmCancel() {
    document.getElementById('rebal-confirm-overlay').style.display = 'none';
    _rebalConfirmAction = null;
}

function rebalConfirmOk() {
    document.getElementById('rebal-confirm-overlay').style.display = 'none';
    const resultEl = document.getElementById('rebal-result');
    const reqId = crypto.randomUUID ? crypto.randomUUID() : Date.now().toString();

    if (_rebalConfirmAction === 'sell') {
        _execRebalCmd('/api/rebalance/sell', {
            request_id: reqId,
            preview_hash: _rebalPreviewHash,
        }, resultEl);
    } else if (_rebalConfirmAction === 'buy') {
        _execRebalCmd('/api/rebalance/buy', {
            request_id: reqId,
            preview_hash: _rebalPreviewHash,
        }, resultEl);
    } else if (_rebalConfirmAction === 'skip') {
        _execRebalCmd('/api/rebalance/skip', {}, resultEl);
    }
    _rebalConfirmAction = null;
}

// P1-4: global in-flight lock — prevents duplicate fires of batch/rebal POST
// actions from rapid double-clicks (same or different buttons).
window._qtronExecuting = window._qtronExecuting || false;

function _execRebalCmd(url, body, resultEl) {
    if (window._qtronExecuting) {
        resultEl.style.display = 'block';
        resultEl.className = 'rebal-result error';
        resultEl.textContent = 'Another action is in progress — please wait';
        return;
    }
    window._qtronExecuting = true;
    resultEl.style.display = 'block';
    resultEl.className = 'rebal-result';
    resultEl.textContent = 'Executing...';

    const opts = {method: 'POST'};
    if (Object.keys(body).length > 0) {
        opts.headers = {'Content-Type': 'application/json'};
        opts.body = JSON.stringify(body);
    }

    fetch(url, opts)
        .then(r => r.json())
        .then(data => {
            if (data.ok) {
                resultEl.className = 'rebal-result success';
                resultEl.textContent = JSON.stringify(data, null, 1);
            } else {
                resultEl.className = 'rebal-result error';
                resultEl.textContent = data.error || 'Failed';
            }
            setTimeout(updateRebalStatus, 1000);
        })
        .catch(e => {
            resultEl.className = 'rebal-result error';
            resultEl.textContent = e.message;
        })
        .finally(() => {
            window._qtronExecuting = false;
        });
}

// Poll rebalance status every 30s
setInterval(updateRebalStatus, 30000);
// Initial load
setTimeout(updateRebalStatus, 2000);

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

function storeAccountData(data) {
    if (data.account) window._lastAccountData = data.account;
    updateProfitComparisons(data);
}

function updateProfitComparisons(data) {
    const dd = data.dd_guard || {};
    const asset = (data.account || {}).total_asset || 0;
    if (!asset || asset <= 0) return;

    const prevClose = dd.source_prev_close || 0;
    const peak = dd.source_peak || 0;
    const trough = dd.source_trough || 0;

    const setCmp = (id, cur, ref, titleDate) => {
        const el = document.getElementById(id);
        if (!el) return;
        if (!ref || ref <= 0) {
            el.textContent = '--';
            el.className = 'profit-sub-value neutral';
            el.title = '';
            return;
        }
        const diff = cur - ref;
        const pct = ((cur / ref) - 1) * 100;
        const sign = diff >= 0 ? '+' : '';
        el.textContent = `${sign}${formatKRW(diff)} (${sign}${pct.toFixed(2)}%)`;
        el.className = 'profit-sub-value ' + (diff > 0 ? 'positive' : diff < 0 ? 'negative' : 'neutral');
        if (titleDate) el.title = `기준: ${titleDate}`;
    };

    setCmp('cmp-prev-close', asset, prevClose, dd.prev_close_date);
    setCmp('cmp-peak', asset, peak, dd.peak_date);
    setCmp('cmp-trough', asset, trough, dd.trough_date);
}

// ── Holdings List (토스 스타일) ──────────────────────
function updateHoldingsList(data) {
    // Phase 3 (2026-04-25): delegate to qc-holdings-table component.
    // formatKRW is injected so the component stays decoupled from the
    // dashboard.js helper (used by other functions too).
    if (window.qc && window.qc.holdings && typeof window.qc.holdings.render === 'function') {
        window.qc.holdings.render(
            document.getElementById('holdings-list'),
            data,
            { formatKRW: formatKRW }
        );
    }
    // Re-bind hover chart after DOM is rebuilt. The IIFE owns its own
    // singleton state, so we just re-attach to the freshly rendered cards.
    bindMiniCardHoverChart();
}

// ── Mini Chart Hover (분봉 ka10080) ─────────────────────────
(function() {
    // 싱글톤 tooltip + 상태
    let tipEl = null;
    let currentCode = null;
    let hoverTimer = null;
    const chartCache = {};  // {code: {ts, data}}
    const CACHE_TTL_MS = 60 * 1000;

    function ensureTooltip() {
        if (tipEl) return tipEl;
        tipEl = document.createElement('div');
        tipEl.id = 'mini-chart-tip';
        tipEl.style.cssText = `
            position: fixed; z-index: 9999;
            background: #0d1420; color: #e6eefc;
            border: 1px solid #2d3748; border-radius: 8px;
            padding: 10px; min-width: 360px; max-width: 420px;
            box-shadow: 0 8px 24px rgba(0,0,0,0.5);
            font-size: 11px; pointer-events: none;
            opacity: 0; transition: opacity 0.15s;
            display: none;
        `;
        document.body.appendChild(tipEl);
        return tipEl;
    }

    function hideTip() {
        if (hoverTimer) { clearTimeout(hoverTimer); hoverTimer = null; }
        if (tipEl) {
            tipEl.style.opacity = '0';
            setTimeout(() => { if (tipEl && tipEl.style.opacity === '0') tipEl.style.display = 'none'; }, 160);
        }
        currentCode = null;
    }

    function positionTip(ev) {
        if (!tipEl) return;
        const pad = 10;
        const W = tipEl.offsetWidth || 380;
        const H = tipEl.offsetHeight || 180;
        let x = ev.clientX + pad;
        let y = ev.clientY + pad;
        if (x + W > window.innerWidth)  x = ev.clientX - W - pad;
        if (y + H > window.innerHeight) y = ev.clientY - H - pad;
        tipEl.style.left = x + 'px';
        tipEl.style.top  = y + 'px';
    }

    function renderChart(data, code, name) {
        const bars = (data && data.bars) || [];
        const tip = ensureTooltip();
        if (!bars.length) {
            tip.innerHTML = `<div style="font-weight:700;margin-bottom:6px;">${name} (${code})</div>
                             <div style="opacity:0.6">분봉 데이터 없음 ${data.error ? '('+data.error+')' : ''}</div>`;
            return;
        }
        const W = 360, H = 120, PAD = 8;
        const closes = bars.map(b => b.close);
        const highs  = bars.map(b => b.high);
        const lows   = bars.map(b => b.low);
        const vols   = bars.map(b => b.volume);
        const maxP = Math.max(...highs);
        const minP = Math.min(...lows);
        const rangeP = Math.max(1, maxP - minP);
        const maxV = Math.max(...vols, 1);

        const stepX = (W - 2 * PAD) / Math.max(1, bars.length - 1);

        // Close line path
        const linePath = closes.map((c, i) => {
            const x = PAD + i * stepX;
            const y = H - PAD - ((c - minP) / rangeP) * (H - 2 * PAD - 30);  // leave 30px for volume
            return `${i === 0 ? 'M' : 'L'}${x.toFixed(1)},${y.toFixed(1)}`;
        }).join(' ');

        // Volume bars
        const volBars = vols.map((v, i) => {
            const x = PAD + i * stepX;
            const h = (v / maxV) * 24;
            const y = H - PAD - h;
            return `<rect x="${(x-1).toFixed(1)}" y="${y.toFixed(1)}" width="2" height="${h.toFixed(1)}" fill="#3a4558"/>`;
        }).join('');

        const firstClose = closes[0];
        const lastClose = closes[closes.length - 1];
        const pct = firstClose ? ((lastClose / firstClose - 1) * 100) : 0;
        const pctCls = pct >= 0 ? '#F04452' : '#3182F6';
        const pctStr = (pct >= 0 ? '+' : '') + pct.toFixed(2) + '%';
        const firstBar = bars[0], lastBar = bars[bars.length - 1];

        tip.innerHTML = `
            <div style="display:flex;justify-content:space-between;align-items:center;margin-bottom:6px;">
                <div><b>${name}</b> <span style="color:#8fa3c0;font-size:10px;">${code}</span></div>
                <div style="color:${pctCls};font-weight:700;">${pctStr}</div>
            </div>
            <div style="display:flex;gap:10px;font-size:10px;color:#8fa3c0;margin-bottom:4px;">
                <span>현재 <b style="color:#e6eefc">${lastClose.toLocaleString()}</b></span>
                <span>고 ${Math.max(...highs).toLocaleString()}</span>
                <span>저 ${Math.min(...lows).toLocaleString()}</span>
                <span>vol ${lastBar.volume.toLocaleString()}</span>
            </div>
            <svg viewBox="0 0 ${W} ${H}" width="100%" height="${H}" style="display:block;">
                <line x1="${PAD}" y1="${H-PAD-24}" x2="${W-PAD}" y2="${H-PAD-24}" stroke="#2d3748" stroke-dasharray="2,2"/>
                ${volBars}
                <path d="${linePath}" fill="none" stroke="${pctCls}" stroke-width="1.5"/>
            </svg>
            <div style="display:flex;justify-content:space-between;font-size:9px;color:#6b7994;margin-top:2px;">
                <span>${firstBar.time ? firstBar.time.slice(0,2)+':'+firstBar.time.slice(2,4) : ''}</span>
                <span style="opacity:0.6">1분봉 · 최근 ${bars.length}개</span>
                <span>${lastBar.time ? lastBar.time.slice(0,2)+':'+lastBar.time.slice(2,4) : ''}</span>
            </div>
        `;
    }

    async function fetchAndShow(card, ev) {
        const code = card.dataset.code;
        if (!code) return;
        currentCode = code;
        const name = card.querySelector('.mini-name')?.textContent || code;

        const tip = ensureTooltip();
        tip.style.display = 'block';
        tip.innerHTML = `<div><b>${name}</b> <span style="opacity:0.6">${code}</span></div>
                         <div style="padding:16px 0;text-align:center;opacity:0.6;">분봉 불러오는 중…</div>`;
        tip.style.opacity = '1';
        positionTip(ev);

        // 캐시 확인
        const cached = chartCache[code];
        if (cached && (Date.now() - cached.ts) < CACHE_TTL_MS) {
            if (currentCode === code) {
                renderChart(cached.data, code, name);
                positionTip(ev);
            }
            return;
        }
        try {
            const resp = await fetch(`/api/chart/minute/${code}?tic_scope=1&bars=60`);
            const data = await resp.json();
            chartCache[code] = { ts: Date.now(), data };
            if (currentCode === code) {  // 다른 카드로 이동 안 했을 때만
                renderChart(data, code, name);
                positionTip(ev);
            }
        } catch(e) {
            if (currentCode === code) {
                tip.innerHTML = `<div><b>${name}</b></div><div style="color:#ef4444;">조회 실패: ${e}</div>`;
            }
        }
    }

    window.bindMiniCardHoverChart = function() {
        const container = document.getElementById('holdings-list');
        if (!container || container._hoverBound) return;
        container._hoverBound = true;

        container.addEventListener('mouseover', (ev) => {
            const card = ev.target.closest('.mini-card');
            if (!card) return;
            if (hoverTimer) clearTimeout(hoverTimer);
            // 250ms 지연 — 빠르게 지나가면 fetch 안 함
            hoverTimer = setTimeout(() => fetchAndShow(card, ev), 250);
        });
        container.addEventListener('mousemove', (ev) => {
            if (tipEl && tipEl.style.display === 'block') positionTip(ev);
        });
        container.addEventListener('mouseout', (ev) => {
            const related = ev.relatedTarget;
            if (related && related.closest && related.closest('.mini-card')) return;
            hideTip();
        });
        // 카드 영역 밖으로 나가면 close
        container.addEventListener('mouseleave', hideTip);
    };
})();

// ── Alert Banner ─────────────────────────────────────────────

function updateAlertBanner(data) {
    const banner = document.getElementById('alert-banner');
    // P0-2 hotfix: bail on /debug (no alert-banner element).
    if (!banner) return;
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
    // P0-2 hotfix: bail if sec-control absent on current page (/debug).
    // sec-control STAYS on Dashboard per option B, so /debug should no-op here.
    if (!tokenStatusEl) return;
    tokenStatusEl.textContent = data.token.valid ? '유효' : '만료';
    tokenStatusEl.className = 'v ' + (data.token.valid ? 'status-ok' : 'status-error');

    setText('token-remaining', data.token.remaining_str,
        data.token.remaining_sec > 3600 ? '' :
        data.token.remaining_sec > 600 ? 'status-warn' : 'status-error');
    const refreshEl = document.getElementById('token-refresh');
    if (refreshEl) refreshEl.textContent = data.token.last_refresh;

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
    // P0-2 hotfix: bail if section absent on current page (/debug).
    if (!grid) return;
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

// ── WebSocket Card ───────────────────────────────

function updateWSCard(ws) {
    const connEl = document.getElementById('ws-connected');
    // P0-2 hotfix: bail if sec-ws-sync absent on current page (/debug).
    if (!connEl) return;
    connEl.textContent = ws.connected ? '연결됨' : '끊김';
    connEl.className = 'v ' + (ws.connected ? 'status-ok' : 'status-error');

    const lastMsg = document.getElementById('ws-last-msg');
    if (lastMsg) lastMsg.textContent = ws.last_msg || '--';
    const msgCount = document.getElementById('ws-msg-count');
    if (msgCount) msgCount.textContent = ws.msg_count.toLocaleString();

    const reconEl = document.getElementById('ws-reconnects');
    if (reconEl) {
        reconEl.textContent = ws.reconnect_count.toString();
        reconEl.className = 'v mono' + (ws.reconnect_count > 3 ? ' status-warn' : '');
    }
}

// ── Timestamps Card ──────────────────────────────

function updateTimestampsCard(ts) {
    const tsFirst = document.getElementById('ts-first');
    // P0-2 hotfix: bail if sec-ws-sync absent on current page (/debug has no
    // ws-sync — it stays on Dashboard, option B).
    if (!tsFirst) return;
    tsFirst.textContent = ts.first_request || '--';
    const tsLast = document.getElementById('ts-last');
    if (tsLast) tsLast.textContent = ts.last_request || '--';
    const tsSuccess = document.getElementById('ts-success');
    if (tsSuccess) tsSuccess.textContent = ts.last_success || '--';

    const failEl = document.getElementById('ts-failure');
    if (failEl) {
        failEl.textContent = ts.last_failure || '--';
        failEl.className = 'v dim' + (ts.last_failure ? ' status-warn' : '');
    }
}

// ── Traces Table ─────────────────────────────────

function updateTracesTable(traces) {
    const tbody = document.getElementById('traces-body');
    // P0-2 C2/C3 hotfix: bail if section absent (Dashboard → moved to /debug).
    if (!tbody) return;
    if (!traces || traces.length === 0) {
        tbody.innerHTML = '<tr><td colspan="7" class="empty-row">추적 데이터 없음</td></tr>';
        return;
    }

    // Filter elements only exist on /debug. Fall back to empty strings on Dashboard.
    const statusEl = document.getElementById('trace-filter-status');
    const searchEl = document.getElementById('trace-filter-search');
    const statusFilter = statusEl ? statusEl.value : '';
    const searchFilter = searchEl ? searchEl.value.toLowerCase() : '';

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

// ── Sync Table ───────────────────────────────────

function updateSyncTable(syncItems) {
    const tbody = document.getElementById('sync-body');
    // P0-2 C2: skip if target section absent (Dashboard page, sync moved to /debug).
    if (!tbody) return;
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
    // P0-2 C2: skip if target section absent (Dashboard page, histogram moved to /debug).
    if (!document.getElementById('histogram-container')) return;
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
    // P0-2 C2: container may be missing on Dashboard (histogram moved to /debug).
    if (!container) return;
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
    const container = document.getElementById('diff-container');
    // P0-2 hotfix: bail if sec-diff absent on current page (Dashboard).
    if (!container) return;
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

// ── D-BATCH / D-QOBS Panels (R14 — 2026-04-23) ───────────────

let _batchLogTimer = null;
let _qobsTimer = null;

function initBatchLogPanel() {
    const btn = document.getElementById('btn-refresh-batch-log');
    const chk = document.getElementById('batch-log-autorefresh');
    if (btn) btn.addEventListener('click', fetchBatchLog);
    if (chk) {
        chk.addEventListener('change', () => {
            if (chk.checked) {
                fetchBatchLog();
                _batchLogTimer = setInterval(fetchBatchLog, 5000);
            } else if (_batchLogTimer) {
                clearInterval(_batchLogTimer);
                _batchLogTimer = null;
            }
        });
        // Initial: auto-refresh on if checked
        if (chk.checked) {
            fetchBatchLog();
            _batchLogTimer = setInterval(fetchBatchLog, 5000);
        }
    }
}

async function fetchBatchLog() {
    // P0-2 C2: skip if target section absent (Dashboard page, batch-log moved to /debug).
    if (!document.getElementById('batch-log-container')) return;
    try {
        const resp = await fetch('/api/debug/batch_log?lines=500');
        const data = await resp.json();
        renderBatchLog(data);
    } catch (err) {
        console.error('Batch log fetch error:', err);
        const c = document.getElementById('batch-log-container');
        if (c) c.innerHTML = `<div class="empty-row log-error">fetch error: ${esc(String(err))}</div>`;
    }
}

function renderBatchLog(data) {
    const container = document.getElementById('batch-log-container');
    const srcBadge = document.getElementById('batch-log-source');
    if (!container) return;

    if (!data.ok) {
        container.innerHTML = `<div class="empty-row">${esc(data.error || '로그 없음')}</div>`;
        if (srcBadge) srcBadge.textContent = '';
        return;
    }

    if (srcBadge) {
        const mtime = data.mtime
            ? new Date(data.mtime * 1000).toLocaleTimeString('ko-KR')
            : '';
        srcBadge.textContent = `${data.source} (${data.shown}/${data.total_lines} lines, ${mtime})`;
    }

    const lines = data.lines || [];
    if (lines.length === 0) {
        container.innerHTML = '<div class="empty-row">로그 내용 없음</div>';
        return;
    }

    // ── Progress bar detection ─────────────────────────────
    // Pattern: "Progress: N/TOTAL (N success)"
    // Handles dual-batch case (CLI + tray concurrent) — group by TOTAL and
    // extract latest CURRENT per group, then show one bar per distinct batch.
    const progressRe = /Progress:\s*(\d+)\/(\d+)\s*\((\d+)\s+success\)/;
    const tsRe = /^(\d{4}-\d{2}-\d{2}\s\d{2}:\d{2}:\d{2})/;

    // Walk from newest (end) → oldest, collect LATEST entry per (total, closestCurrent-group)
    // Simpler heuristic: collect last 2 distinct (current,total) pairs — if their
    // current values differ by >20, treat as 2 separate batches.
    const recentProgresses = [];
    for (let i = lines.length - 1; i >= 0 && recentProgresses.length < 4; i--) {
        const m = lines[i].match(progressRe);
        if (!m) continue;
        const current = parseInt(m[1]);
        const total = parseInt(m[2]);
        const tsM = lines[i].match(tsRe);
        const ts = tsM ? tsM[1] : '';
        // Deduplicate — skip if we already collected same current+total
        if (recentProgresses.some(p => p.current === current && p.total === total)) continue;
        recentProgresses.push({current, total, ts, lineIdx: i});
    }

    // Group into 2 sequences if values differ significantly
    let batches = [];
    if (recentProgresses.length === 0) {
        batches = [];
    } else if (recentProgresses.length === 1) {
        batches = [recentProgresses[0]];
    } else {
        // Sort by ts desc (newest first); pick top 2 with differing current
        const sorted = [...recentProgresses].sort((a, b) => b.ts.localeCompare(a.ts));
        batches.push(sorted[0]);
        for (let i = 1; i < sorted.length; i++) {
            if (Math.abs(sorted[i].current - batches[0].current) > 20) {
                batches.push(sorted[i]);
                break;
            }
        }
    }

    // Render progress bars
    let progressHtml = '';
    if (batches.length > 0) {
        progressHtml = '<div style="background:#1a1a1a;padding:10px;margin-bottom:8px;border-radius:4px;">';
        progressHtml += '<div style="font-weight:bold;margin-bottom:6px;font-size:12px;">📊 FUND Progress' + (batches.length > 1 ? ' (2 concurrent batches)' : '') + '</div>';
        batches.forEach((b, idx) => {
            const pct = b.total > 0 ? (b.current / b.total * 100) : 0;
            const pctStr = pct.toFixed(1);
            const label = batches.length > 1 ? `Batch ${String.fromCharCode(65 + idx)}` : 'Batch';
            // Color: green if >=75%, yellow if >=25%, else blue
            const barColor = pct >= 75 ? '#4caf50' : pct >= 25 ? '#ff9800' : '#2196f3';
            // Rate estimate — not critical, just raw progress
            progressHtml += `
                <div style="margin-bottom:6px;">
                    <div style="display:flex;justify-content:space-between;font-size:11px;margin-bottom:2px;">
                        <span><strong>${label}</strong> ${b.ts || ''}</span>
                        <span>${b.current.toLocaleString()}/${b.total.toLocaleString()} (${pctStr}%)</span>
                    </div>
                    <div style="background:#333;height:18px;border-radius:3px;overflow:hidden;position:relative;">
                        <div style="width:${pctStr}%;height:100%;background:${barColor};transition:width 0.4s;"></div>
                        <div style="position:absolute;top:0;left:0;right:0;bottom:0;display:flex;align-items:center;justify-content:center;font-size:10px;color:#fff;text-shadow:0 0 3px #000;">
                            ${pctStr}%
                        </div>
                    </div>
                </div>
            `;
        });
        // ETA estimate based on first batch's rate (1.48s/stock measured)
        if (batches[0] && batches[0].total > 0) {
            const remaining = batches[0].total - batches[0].current;
            const etaSec = Math.round(remaining * 1.48);
            const etaMin = Math.round(etaSec / 60);
            progressHtml += `<div style="font-size:11px;color:#888;margin-top:4px;">Batch A 예상 완료: 약 ${etaMin}분 후 (${remaining.toLocaleString()} stocks 남음 × 1.48s/stock)</div>`;
        }
        progressHtml += '</div>';
    }

    // Log line rendering
    const logHtml = lines.map(line => {
        let cls = 'log-info';
        if (/\bERROR\b|Traceback|CRITICAL|FAIL/i.test(line)) cls = 'log-error';
        else if (/\bWARN(ING)?\b/i.test(line)) cls = 'log-warn';
        else if (/Progress:|\[FUND_TIMEOUT/.test(line)) cls = 'log-info';
        return `<div class="log-line ${cls}">${esc(line)}</div>`;
    }).join('');

    container.innerHTML = progressHtml + logHtml;

    // Auto-scroll to bottom
    container.scrollTop = container.scrollHeight;
}

function initQobsPanel() {
    const btn = document.getElementById('btn-refresh-qobs');
    const chk = document.getElementById('qobs-autorefresh');
    if (btn) btn.addEventListener('click', fetchQobs);
    if (chk) {
        chk.addEventListener('change', () => {
            if (chk.checked) {
                fetchQobs();
                _qobsTimer = setInterval(fetchQobs, 10000);
            } else if (_qobsTimer) {
                clearInterval(_qobsTimer);
                _qobsTimer = null;
            }
        });
    }
    // Initial fetch
    fetchQobs();
}

async function fetchQobs() {
    // P0-2 C2: skip if target section absent (Dashboard page, qobs moved to /debug).
    if (!document.getElementById('qobs-container')) return;
    try {
        const resp = await fetch('/api/debug/qobs');
        const data = await resp.json();
        renderQobs(data);
    } catch (err) {
        console.error('Qobs fetch error:', err);
        const c = document.getElementById('qobs-container');
        if (c) c.innerHTML = `<div class="empty-row log-error">fetch error: ${esc(String(err))}</div>`;
    }
}

function renderQobs(data) {
    const container = document.getElementById('qobs-container');
    if (!container) return;

    // Heartbeat
    let hbHtml;
    if (data.heartbeat) {
        const hb = data.heartbeat;
        if (hb.error) {
            hbHtml = `<span class="log-error">ERROR: ${esc(hb.error)}</span>`;
        } else {
            const age = hb.age_sec;
            const cls = age == null ? 'log-warn' : (age > 120 ? 'log-error' : (age > 60 ? 'log-warn' : 'log-info'));
            hbHtml = `<span class="${cls}">age=${age}s</span> tick_seq=${hb.tick_seq} pid=${hb.pid} session=${esc(hb.tray_session || '-')}`;
        }
    } else {
        hbHtml = '<span class="log-error">MISSING (tray 미기동?)</span>';
    }

    // Marker runs
    let markerHtml = '';
    if (data.marker && data.marker.runs) {
        const runs = data.marker.runs;
        const rows = Object.entries(runs).map(([rt, r]) => {
            const statusCls = r.status === 'SUCCESS' ? 'log-info' :
                              (r.status === 'FAILED' || r.status === 'PRE_FLIGHT_FAIL' || r.status === 'PRE_FLIGHT_STALE_INPUT') ? 'log-error' :
                              r.status === 'PARTIAL' ? 'log-warn' :
                              r.status === 'RUNNING' ? 'log-warn' : 'log-info';
            const errMsg = r.error ? `<div style="margin-left:20px;font-size:11px;color:#c77;">err[${esc(r.error.stage || '')}]: ${esc((r.error.message || '').substring(0, 150))}</div>` : '';
            return `<div style="padding:4px 0;">
                <strong style="display:inline-block;width:110px;">${esc(rt)}</strong>
                <span class="${statusCls}">${esc(r.status)}</span>
                attempt=${r.attempt_no} worst=${esc(r.worst_status_today || '-')}
                ${errMsg}
            </div>`;
        }).join('');
        markerHtml = rows || '<div class="empty-row">runs 없음</div>';
    } else if (data.marker && data.marker.error) {
        markerHtml = `<div class="log-error">marker ERROR: ${esc(data.marker.error)}</div>`;
    } else {
        markerHtml = '<div class="empty-row">오늘 marker 없음</div>';
    }

    // Incidents
    const incCount = (data.incidents || []).length;
    const incHtml = incCount > 0
        ? (data.incidents || []).map(i => `<div>  ${esc(i.name)} (${i.size} bytes)</div>`).join('')
        : '<div class="empty-row">0 건</div>';

    // DEADMAN
    const dmHtml = data.deadman_configured
        ? '<span class="log-info">configured</span>'
        : '<span class="log-error">MISSING — env var 미설정</span>';

    // Known bombs
    const bombs = (data.marker && data.marker.known_bombs) || [];
    const bombHtml = bombs.length > 0
        ? bombs.map(b => `<div class="log-error">  ${esc(b.module)} state=${esc(b.state)} since=${esc(b.detected_since)}</div>`).join('')
        : '';

    // R13 (2026-04-23): Expected vs actual runs — schedule table
    let expectedHtml = '';
    if (data.expected_runs) {
        const rows = Object.entries(data.expected_runs).map(([rt, e]) => {
            const phaseCls = e.phase === 'in_window' ? 'log-info' :
                             e.phase === 'before_window' ? 'log-warn' : 'log-info';
            const phaseLabel = e.phase === 'in_window' ? '실행창' :
                               e.phase === 'before_window' ? '대기' : '종료';
            const statusCls = e.actual_status === 'SUCCESS' ? 'log-info' :
                              (e.actual_status === 'FAILED' || e.actual_status === 'PRE_FLIGHT_FAIL' || e.actual_status === 'PRE_FLIGHT_STALE_INPUT') ? 'log-error' :
                              e.actual_status === 'RUNNING' ? 'log-warn' :
                              e.actual_status === 'PARTIAL' ? 'log-warn' : '';
            const statusText = e.actual_status || '—';
            const alertMark = e.alert ? '<span class="log-error" style="margin-left:8px;">⚠ 기한 초과</span>' : '';
            return `<div style="padding:3px 0;font-size:11px;">
                <strong style="display:inline-block;width:110px;">${esc(rt)}</strong>
                <span style="display:inline-block;width:130px;color:#aaa;">${esc(e.earliest_kst)}~${esc(e.deadline_kst)}</span>
                <span class="${phaseCls}" style="display:inline-block;width:70px;">${phaseLabel}</span>
                <span class="${statusCls}">${esc(statusText)}</span>
                ${alertMark}
            </div>`;
        }).join('');
        expectedHtml = rows;
    } else if (data.expected_runs_error) {
        expectedHtml = `<div class="log-error">expected_runs ERROR: ${esc(data.expected_runs_error)}</div>`;
    }

    const now = new Date().toLocaleTimeString('ko-KR');
    const nowKst = data.now_kst ? ` (KST ${esc(data.now_kst)})` : '';
    container.innerHTML = `
        <div style="font-family:monospace;font-size:12px;line-height:1.6;">
            <div style="font-size:11px;color:#888;margin-bottom:8px;">fetched: ${now}${nowKst}   trade_date: ${esc(data.trade_date)}</div>
            <div style="margin-bottom:12px;">
                <strong>Heartbeat</strong><br>
                <span style="margin-left:20px;">${hbHtml}</span>
            </div>
            ${expectedHtml ? `<div style="margin-bottom:12px;">
                <strong>Expected vs Actual (R13)</strong>
                <div style="margin-left:20px;">${expectedHtml}</div>
            </div>` : ''}
            <div style="margin-bottom:12px;">
                <strong>Marker Runs</strong>
                ${markerHtml}
            </div>
            <div style="margin-bottom:12px;">
                <strong>Incidents (${incCount})</strong>
                <div style="margin-left:20px;font-size:11px;">${incHtml}</div>
            </div>
            <div style="margin-bottom:12px;">
                <strong>DEADMAN</strong>
                <span style="margin-left:20px;">${dmHtml}</span>
            </div>
            ${bombs.length > 0 ? `<div style="margin-bottom:12px;"><strong>Known Bombs</strong>${bombHtml}</div>` : ''}
        </div>
    `;
}

// ── Log Panel (Debug) ────────────────────────────────────────

function initLogRefresh() {
    const btn = document.getElementById('btn-refresh-logs');
    if (btn) btn.addEventListener('click', fetchLogs);
}

async function fetchLogs() {
    // P0-2 C2: skip if target section absent (Dashboard page, logs moved to /debug).
    if (!document.getElementById('log-container')) return;
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
// P0-2 C3 (2026-04-24): initModeSwitcher() / switchMode() removed.
// Reason: Dashboard is now unified (no Basic/Operator/Debug toggling).
// Debug sections moved to /debug (P0-2 C1+C2). Analytics and operator
// sections load unconditionally on DOMContentLoaded below.

(function _loadDashboardExtensions() {
    // Used to be called from switchMode('operator'). Now unconditional.
    if (document.readyState === 'loading') {
        document.addEventListener('DOMContentLoaded', _loadExt);
    } else {
        _loadExt();
    }
    function _loadExt() {
        if (typeof loadEquityCurve === 'function') loadEquityCurve();
        if (typeof loadTradeHistory === 'function') loadTradeHistory();
        if (typeof loadRiskMetrics === 'function') loadRiskMetrics();
        if (typeof loadRebalHistory === 'function') loadRebalHistory();
        if (typeof loadAlertHistory === 'function') loadAlertHistory();
        if (typeof loadRebalPreview === 'function') loadRebalPreview();
    }
})();

// ── Rebalance Preview ────────────────────────────────────────

// ── Advisor ──────────────────────────────────────────────────

async function loadAdvisor() {
    try {
        const resp = await fetch('/api/advisor/today');
        const data = await resp.json();
        const badge = document.getElementById('advisor-status');
        const alertsEl = document.getElementById('advisor-alerts');
        const recsEl = document.getElementById('advisor-recs');
        if (!alertsEl) return;

        // Priority: DISABLED (engine offline) > STALE > NO_DATA > OK.
        // For DISABLED / STALE we MUST NOT render any alert list — the
        // stored alerts describe a prior world and must not be mistaken
        // for the current one.
        if (data.status === 'DISABLED') {
            badge.textContent = 'DISABLED';
            badge.className = 'badge badge-readfail';
            alertsEl.innerHTML =
                '<div class="advisor-alert advisor-alert-high">' +
                '🛑 ' + (data.message || 'AI ADVISOR paused (engine offline)') +
                '</div>';
            if (recsEl) recsEl.style.display = 'none';
            return;
        }

        if (data.status === 'STALE') {
            badge.textContent = 'STALE';
            badge.className = 'badge badge-stale';
            const msg = data.message ||
                `AI ADVISOR unavailable — last run: ${data.last_run_date || '?'}`;
            alertsEl.innerHTML =
                `<div class="advisor-alert advisor-alert-high">⏸ ${msg}</div>`;
            if (recsEl) recsEl.style.display = 'none';
            return;
        }

        if (data.status === 'NO_DATA') {
            badge.textContent = 'PENDING';
            badge.className = 'badge badge-mock';
            alertsEl.innerHTML = '<div class="dim">배치 실행 후 분석 결과가 표시됩니다</div>';
            if (recsEl) recsEl.style.display = 'none';
            return;
        }

        badge.textContent = data.date || 'OK';
        badge.className = 'badge badge-ok';

        // Alerts
        const alerts = data.alerts || [];
        if (alerts.length > 0) {
            alertsEl.innerHTML = alerts.map(a => {
                const cls = a.priority === 'HIGH' ? 'advisor-alert-high' :
                            a.priority === 'MEDIUM' ? 'advisor-alert-medium' : 'advisor-alert-low';
                const icon = a.priority === 'HIGH' ? '🚨' : a.priority === 'MEDIUM' ? '⚠️' : 'ℹ️';
                return `<div class="advisor-alert ${cls}">${icon} <strong>[${a.priority}]</strong> ${a.message}</div>`;
            }).join('');
        } else {
            alertsEl.innerHTML = '<div class="advisor-alert advisor-alert-low">✅ 이상 없음</div>';
        }

        // Recommendations
        const recs = data.recommendations || [];
        if (recs.length > 0) {
            recsEl.style.display = '';
            recsEl.innerHTML = '<div style="font-size:12px;color:var(--text-secondary);margin-bottom:6px">📋 파라미터 추천</div>' +
                recs.map(r =>
                    `<div class="advisor-rec"><strong>[${r.confidence || ''}]</strong> ${r.parameter || ''}: ${(r.rationale || '').slice(0, 80)}</div>`
                ).join('');
        } else {
            recsEl.style.display = 'none';
        }
    } catch (e) {
        console.error('Advisor load error:', e);
    }
}

// Load advisor on page load
setTimeout(loadAdvisor, 2000);

async function loadDbHealth() {
    // P0-2 C2: skip if target section absent (Dashboard page, db-health moved to /debug).
    if (!document.getElementById('db-health-grid')) return;
    try {
        const resp = await fetch('/api/db/health');
        const data = await resp.json();
        const sizeEl = document.getElementById('db-size');
        const gridEl = document.getElementById('db-health-grid');
        if (!gridEl) return;

        if (data.status === 'ERROR') {
            sizeEl.textContent = 'OFFLINE';
            sizeEl.className = 'badge badge-red';
            gridEl.innerHTML = `<div style="color:#ef5350">DB 연결 실패: ${data.error || ''}</div>`;
            return;
        }

        sizeEl.textContent = data.db_size || '--';
        sizeEl.className = 'badge badge-ok';

        gridEl.innerHTML = (data.tables || []).map(t => {
            const color = t.status === 'OK' ? '#4caf50' : t.status === 'EMPTY' ? '#ffc107' : '#ef5350';
            return `<div class="freshness-card">
                <div class="freshness-label">${t.table}</div>
                <div class="freshness-value" style="color:${color}">${t.rows.toLocaleString()}</div>
                <div class="freshness-meta">${t.latest || '-'}</div>
                <div class="freshness-status" style="color:${color}">${t.status}</div>
            </div>`;
        }).join('');
    } catch (e) {
        console.error('DB health error:', e);
    }
}

// SVG sparkline from a numeric series. Color follows TODAY's direction
// (last segment, e.g. today_open → today_close in the backend payload)
// rather than the 14-day trend, so the line color always matches the
// change_pct shown next to it. KR convention: up=red, down=blue.
//
// `intraday=true` (Jeff 2026-04-27): the last segment is today's
// open→current-tick move, not a sealed close. We render that segment
// dashed and add a pulsing endpoint dot so a live look matches the
// Bing/Yahoo "장 진행중" convention. After 15:30 KST or on weekends
// the row falls back to the static look.
function rpSparkSvg(series, intraday = false, w = 64, h = 18) {
    if (!series || series.length < 2) return '';
    const min = Math.min(...series), max = Math.max(...series);
    const span = (max - min) || 1;
    const step = w / (series.length - 1);
    const xy = series.map((v, i) => [
        (i * step),
        (h - ((v - min) / span) * h),
    ]);
    const ptsAll = xy.map(p => `${p[0].toFixed(1)},${p[1].toFixed(1)}`).join(' ');
    const last = series[series.length - 1];
    const prev = series[series.length - 2];
    const stroke = last >= prev ? '#ef5350' : '#42a5f5';

    if (!intraday) {
        return `<svg class="rp-spark" width="${w}" height="${h}" viewBox="0 0 ${w} ${h}" preserveAspectRatio="none">
            <polyline fill="none" stroke="${stroke}" stroke-width="1.4" points="${ptsAll}" />
        </svg>`;
    }

    // Intraday: render two polylines so we can dash only the last segment
    // (a single polyline + dash array would dash the whole line). Endpoint
    // dot pulses via CSS keyframe defined in style.css (rpEndPulse).
    const settledPts = xy.slice(0, -1).map(p => `${p[0].toFixed(1)},${p[1].toFixed(1)}`).join(' ');
    const lastSeg = xy.slice(-2).map(p => `${p[0].toFixed(1)},${p[1].toFixed(1)}`).join(' ');
    const [ex, ey] = xy[xy.length - 1];
    return `<svg class="rp-spark rp-spark-live" width="${w}" height="${h}" viewBox="0 0 ${w} ${h}" preserveAspectRatio="none">
        <polyline fill="none" stroke="${stroke}" stroke-width="1.4" points="${settledPts}" />
        <polyline fill="none" stroke="${stroke}" stroke-width="1.4" stroke-dasharray="2.5 2" points="${lastSeg}" />
        <circle class="rp-spark-pulse" cx="${ex.toFixed(1)}" cy="${ey.toFixed(1)}" r="1.6" fill="${stroke}" />
    </svg>`;
}

// Build a single rebal preview row. Includes sparkline + click-to-drawer.
function rpRowHtml(s) {
    const cls  = s.change_pct >= 0 ? 'rp-change-pos' : 'rp-change-neg';
    const sign = s.change_pct >= 0 ? '+' : '';
    const open = (s.open != null ? s.open : 0).toLocaleString();
    const close = (s.close != null ? s.close : 0).toLocaleString();
    const payload = encodeURIComponent(JSON.stringify(s));
    return `<div class="rp-row" data-payload="${payload}" onclick="rpRowClick(this)">
        <span class="rp-code">${s.code}</span>
        <span class="rp-name">${s.name || s.code}</span>
        <span class="rp-spark-cell">${rpSparkSvg(s.spark, !!s.intraday)}</span>
        <span class="rp-px">${open}→${close}</span>
        <span class="${cls}">${sign}${s.change_pct}%</span>
    </div>`;
}

function rpRowClick(el) {
    try {
        const s = JSON.parse(decodeURIComponent(el.dataset.payload || '{}'));
        if (window.qcDrawer) window.qcDrawer.open(s);
    } catch (e) { /* noop */ }
}

async function loadRebalPreview() {
    // Skeleton placeholders while the API is in flight
    ['rp-new-entries', 'rp-exits', 'rp-unchanged'].forEach(id => {
        const el = document.getElementById(id);
        if (el && !el.dataset.loaded) {
            el.innerHTML = `<div class="rp-row rp-skeleton-row">
                <span class="skeleton skel-code"></span>
                <span class="skeleton skel-name"></span>
                <span class="skeleton skel-spark"></span>
                <span class="skeleton skel-px"></span>
                <span class="skeleton skel-pct"></span>
            </div>`.repeat(3);
        }
    });

    try {
        const resp = await fetch('/api/rebalance/preview-compare');
        const data = await resp.json();
        if (data.error) return;

        const hdr = document.getElementById('rp-target-date');
        const dday = document.getElementById('rp-dday');
        if (hdr) hdr.textContent = `Target: ${data.target_date} (${data.target_count}종목)`;
        if (dday) dday.textContent = `D-${data.days_remaining}`;

        // New entries / Exits / Unchanged — unified renderer (sparkline + drawer)
        const renderGroup = (id, items, emptyText) => {
            const el = document.getElementById(id);
            if (!el) return;
            el.dataset.loaded = '1';
            if (items && items.length > 0) {
                el.innerHTML = items.map(rpRowHtml).join('');
            } else {
                el.textContent = emptyText;
            }
        };
        renderGroup('rp-new-entries', data.new_entries, '변경 없음');
        renderGroup('rp-exits',       data.exits,       '변경 없음');
        renderGroup('rp-unchanged',   data.unchanged,   '--');

        // Rebalance Score
        const optEl = document.getElementById('rp-optimal');
        if (optEl && data.rebal_score) {
            const rs = data.rebal_score;
            const decColor = {
                HOLD: '#4caf50', WATCH: '#ffc107',
                SOFT_REBALANCE: '#ff9800', FULL_REBALANCE: '#ef5350',
            }[rs.decision] || '#fff';
            const decLabel = {
                HOLD: 'HOLD (유지)', WATCH: 'WATCH (관찰)',
                SOFT_REBALANCE: 'SOFT REBALANCE (부분 교체)',
                FULL_REBALANCE: 'FULL REBALANCE (전체 교체)',
            }[rs.decision] || rs.decision;

            optEl.innerHTML = `
                <div style="display:flex;justify-content:space-between;align-items:center;margin-bottom:8px">
                    <span style="font-size:15px;font-weight:700">REBALANCE SCORE: <span style="font-size:20px;color:${decColor}">${rs.total}</span></span>
                    <span style="font-size:14px;font-weight:700;color:${decColor};padding:4px 12px;border:1px solid ${decColor};border-radius:6px">${decLabel}</span>
                </div>
                <div style="display:grid;grid-template-columns:repeat(4,1fr);gap:8px;font-size:12px;margin-bottom:6px">
                    <div>Drift <strong>${rs.drift}</strong></div>
                    <div>Replace <strong>${rs.replacement}</strong></div>
                    <div>Quality <strong>${rs.quality}</strong></div>
                    <div>Market <strong>${rs.market}</strong></div>
                </div>
                ${rs.reasons.length ? `<div style="font-size:11px;color:var(--text-secondary)">사유: ${rs.reasons.join(', ')}</div>` : ''}
                ${rs.force_hold ? '<div style="font-size:11px;color:#4caf50;margin-top:4px">⚠ 과매매 방지: 교체 비율 낮음 → HOLD 강제</div>' : ''}
            `;
        }
    } catch (e) {
        console.error('Rebal preview error:', e);
    }
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

// ── Test Order Panel ────────────────────────────────────────

function initTestOrder() {
    const buyBtn = document.getElementById('btn-test-buy');
    const sellBtn = document.getElementById('btn-test-sell');
    if (!buyBtn || !sellBtn) return;

    buyBtn.addEventListener('click', () => _executeTestOrder('BUY'));
    sellBtn.addEventListener('click', () => _executeTestOrder('SELL'));

    // Load history on init
    _loadTestHistory();
}

function _executeTestOrder(side) {
    const codeInput = document.getElementById('test-code');
    const qtyInput = document.getElementById('test-qty');
    const resultEl = document.getElementById('test-result');
    const buyBtn = document.getElementById('btn-test-buy');
    const sellBtn = document.getElementById('btn-test-sell');

    const code = (codeInput.value || '').trim();
    const qty = parseInt(qtyInput.value) || 1;

    if (!code || code.length < 4) {
        resultEl.innerHTML = '<span class="fail">종목코드를 입력하세요</span>';
        return;
    }
    if (qty > 3) {
        resultEl.innerHTML = '<span class="fail">최대 3주까지만 가능</span>';
        return;
    }

    // Confirm
    const sideName = side === 'BUY' ? '매수' : '매도';
    if (!confirm(`${code} ${qty}주 ${sideName} 실행하시겠습니까? (실제 주문입니다)`)) return;

    // Disable buttons
    buyBtn.disabled = true;
    sellBtn.disabled = true;
    resultEl.innerHTML = `<span style="color:var(--yellow)">⏳ ${sideName} 주문 실행 중...</span>`;

    const url = side === 'BUY' ? '/api/test/buy' : '/api/test/sell';
    fetch(url, {
        method: 'POST',
        headers: {'Content-Type': 'application/json'},
        body: JSON.stringify({code: code, qty: qty}),
    })
    .then(r => r.json())
    .then(data => {
        if (data.error) {
            resultEl.innerHTML = `<span class="fail">❌ ${data.error}</span>`;
        } else {
            const status = data.status || 'SUBMITTED';
            const price = data.exec_price ? data.exec_price.toLocaleString() + '원' : '대기';
            resultEl.innerHTML = `<span class="success">✅ ${sideName} ${status} | 주문번호: ${data.order_no || '--'} | 체결가: ${price}</span>`;
        }
        _loadTestHistory();
    })
    .catch(e => {
        resultEl.innerHTML = `<span class="fail">❌ 네트워크 오류: ${e.message}</span>`;
    })
    .finally(() => {
        buyBtn.disabled = false;
        sellBtn.disabled = false;
    });
}

function _loadTestHistory() {
    const histEl = document.getElementById('test-history');
    if (!histEl) return;

    fetch('/api/test/orders')
        .then(r => r.json())
        .then(data => {
            if (data.error || !Array.isArray(data) || data.length === 0) {
                histEl.innerHTML = '<div style="color:var(--text-dim)">주문 이력 없음</div>';
                return;
            }
            histEl.innerHTML = data.map(o => {
                const sideClass = o.side === 'BUY' ? 'th-side-buy' : 'th-side-sell';
                const sideName = o.side === 'BUY' ? '매수' : '매도';
                const statusClass = o.status === 'FILLED' ? 'filled' : o.error ? 'error' : '';
                const price = o.exec_price ? o.exec_price.toLocaleString() + '원' : '--';
                const time = (o.ts || '').substring(11, 19);
                return `<div class="test-history-row">
                    <span class="th-time">${time}</span>
                    <span class="${sideClass}">${sideName}</span>
                    <span class="th-code">${o.code}</span>
                    <span class="th-detail">${o.qty}주 × ${price}</span>
                    <span class="th-status ${statusClass}">${o.status || o.error || '--'}</span>
                </div>`;
            }).join('');
        })
        .catch(() => {});
}

// ── DD Guard ────────────────────────────────────────────────

function updateDDGuard(data) {
    const dd = data.dd_guard;
    if (!dd) return;

    const dailyVal = document.getElementById('dd-daily-value');
    // P0-2 hotfix: bail if dd-guard-section absent (/debug).
    if (!dailyVal) return;
    const dailyFill = document.getElementById('dd-daily-fill');
    const monthlyVal = document.getElementById('dd-monthly-value');
    const monthlyFill = document.getElementById('dd-monthly-fill');
    const permBadge = document.getElementById('dd-permission-badge');
    const configVer = document.getElementById('dd-config-version');
    const badgesEl = document.getElementById('dd-badges');

    // Daily DD
    if (dd.daily_dd_available && dd.daily_dd !== null) {
        const pct = (dd.daily_dd * 100).toFixed(2);
        dailyVal.textContent = pct + '%';
        dailyVal.className = 'dd-gauge-value ' + (dd.daily_dd >= 0 ? 'positive' : 'negative');
        const width = Math.min(Math.abs(dd.daily_dd) / 0.30 * 100, 100);
        dailyFill.style.width = width + '%';
        dailyFill.className = 'dd-gauge-fill ' + ddColor(dd.daily_dd);
    } else {
        dailyVal.textContent = '측정 불가';
        dailyVal.className = 'dd-gauge-value unavailable';
        dailyFill.style.width = '0%';
    }

    // Monthly DD
    if (dd.monthly_dd_available && dd.monthly_dd !== null) {
        const pct = (dd.monthly_dd * 100).toFixed(2);
        monthlyVal.textContent = pct + '%';
        monthlyVal.className = 'dd-gauge-value ' + (dd.monthly_dd >= 0 ? 'positive' : 'negative');
        const width = Math.min(Math.abs(dd.monthly_dd) / 0.30 * 100, 100);
        monthlyFill.style.width = width + '%';
        monthlyFill.className = 'dd-gauge-fill ' + ddColor(dd.monthly_dd);
    } else {
        monthlyVal.textContent = '측정 불가';
        monthlyVal.className = 'dd-gauge-value unavailable';
        monthlyFill.style.width = '0%';
    }

    // Permission badge
    const permClass = {NORMAL:'perm-normal', REDUCED:'perm-reduced', BLOCKED:'perm-blocked'}[dd.buy_permission] || 'perm-unknown';
    permBadge.className = 'dd-perm-badge ' + permClass;
    permBadge.textContent = dd.buy_permission || '--';

    // Config version
    if (configVer) configVer.textContent = dd.config_version || '';

    // Source badges
    if (badgesEl) {
        let badges = '';
        if (dd.from_cache) badges += '<span class="src-badge cache">CACHE</span>';
        if (dd.stale) badges += '<span class="src-badge stale">&#9203;</span>';
        if (dd.expired) badges += '<span class="src-badge expired">EXPIRED</span>';
        badgesEl.innerHTML = badges;
    }
}

function ddColor(val) {
    const v = Math.abs(val);
    if (v < 0.05) return 'dd-green';
    if (v < 0.10) return 'dd-yellow';
    if (v < 0.15) return 'dd-orange';
    if (v < 0.20) return 'dd-red';
    return 'dd-deepred';
}

// ── Hero Badges (BUY STATUS + SYSTEM RISK) ──────────────────

function updateHeroBadges(data) {
    const buyBadge = document.getElementById('buy-status-badge');
    // P0-2 hotfix: bail if hero badges absent (/debug).
    if (!buyBadge) return;
    const riskBadge = document.getElementById('system-risk-badge');
    const emergBadge = document.getElementById('emergency-badge');

    // BUY STATUS
    const dd = data.dd_guard || {};
    const perm = dd.buy_permission || 'UNKNOWN';
    // ENGINE_OFFLINE overrides buy_permission to BLOCKED server-side, so
    // this map already handles it; the key is that 'NORMAL' must never
    // render while the engine is down.
    const buyClass = {NORMAL:'badge-normal', REDUCED:'badge-reduced', BLOCKED:'badge-blocked'}[perm] || 'badge-unknown';
    buyBadge.className = 'status-badge ' + buyClass;
    buyBadge.textContent = 'BUY: ' + perm;

    // SYSTEM RISK
    const sr = data.system_risk || {};
    const primary = sr.primary || 'OK';
    // ENGINE_OFFLINE reuses the red READ_FAIL styling — both mean "do
    // not trust auto-trading right now". Keeping the class name scheme
    // simple so no new CSS is needed.
    const riskClass = {OK:'badge-ok', STALE:'badge-stale', RECON_WARN:'badge-recon',
                       SAFE_MODE:'badge-safemode', READ_FAIL:'badge-readfail',
                       ENGINE_OFFLINE:'badge-readfail'}[primary] || 'badge-unknown';
    riskBadge.className = 'status-badge ' + riskClass;
    riskBadge.textContent = 'RISK: ' + primary;
    if (sr.reason_codes && sr.reason_codes.length > 1) {
        riskBadge.title = sr.reason_codes.join(', ');
    } else if (primary === 'ENGINE_OFFLINE' && sr.reason) {
        riskBadge.title = sr.reason;
    }

    // Emergency badge
    if (primary === 'SAFE_MODE' || primary === 'READ_FAIL' || primary === 'ENGINE_OFFLINE') {
        emergBadge.style.display = 'inline-flex';
        emergBadge.textContent = {
            SAFE_MODE: 'SAFE MODE',
            READ_FAIL: 'READ FAIL',
            ENGINE_OFFLINE: 'ENGINE OFFLINE',
        }[primary];
    } else {
        emergBadge.style.display = 'none';
    }
}

// ── Index Display ───────────────────────────────────────────

function updateIndexDisplay(data) {
    const idx = data.index;
    if (!idx) return;
    const label = document.getElementById('hero-index-label');
    const value = document.getElementById('hero-index');
    // P0-2 hotfix: bail if hero index display absent (/debug).
    if (!value) return;
    if (idx.error || !idx.price) {
        value.textContent = '--';
        value.className = 'meta-value';
        return;
    }
    if (idx.name) label.textContent = idx.name;
    const sign = idx.change_pct >= 0 ? '+' : '';
    value.textContent = idx.price.toLocaleString(undefined, {maximumFractionDigits: 1}) + ' ' + sign + idx.change_pct.toFixed(1) + '%';
    value.className = 'meta-value ' + (idx.change_pct >= 0 ? 'positive' : 'negative');
    if (idx.stale) value.className += ' stale-text';

    // Nav bar index (코스피: 상승=빨강, 하락=파랑 — 한국 관례)
    const ni = document.getElementById('qnav-index');
    if (ni && idx.price) {
        const chgCls = idx.change_pct >= 0 ? 'kr-up' : 'kr-dn';
        ni.innerHTML = `${idx.name || 'KOSPI'} ${idx.price.toLocaleString(undefined,{maximumFractionDigits:1})} <span class="chg ${chgCls}">${sign}${idx.change_pct.toFixed(1)}%</span>`;
    }
}

// ── Donut Chart ─────────────────────────────────────────────

const DONUT_COLORS = ['#3182F6','#F04452','#00B8D9','#36B37E','#FF991F',
    '#6554C0','#00875A','#FF5630','#0065FF','#8777D9',
    '#E56910','#5243AA','#00A3BF','#57D9A3','#FF8B00'];

let _donutSegments = [];
let _donutCanvas = null;
let _donutTooltip = null;
let _donutMode = 'sector';  // 'sector' (default) or 'stock'
let _lastDonutData = null;

function updateDonutChart(data) {
    _lastDonutData = data;
    const container = document.getElementById('donut-chart');
    const legend = document.getElementById('donut-legend');
    if (!container || !legend) return;

    const acct = data.account;
    if (!acct || !acct.holdings || acct.holdings.length === 0) {
        container.innerHTML = '<span class="dim" style="position:absolute;top:50%;left:50%;transform:translate(-50%,-50%)">데이터 없음</span>';
        legend.innerHTML = '';
        _donutCanvas = null;
        _donutTooltip = null;
        return;
    }

    // Prepare canvas
    if (!_donutCanvas) {
        const cvs = document.createElement('canvas');
        cvs.width = 280; cvs.height = 280;
        cvs.style.width = '140px'; cvs.style.height = '140px';
        container.innerHTML = '';
        container.appendChild(cvs);
        _donutCanvas = cvs;

        const tip = document.createElement('div');
        tip.className = 'donut-tooltip';
        tip.style.display = 'none';
        container.appendChild(tip);
        _donutTooltip = tip;

        cvs.addEventListener('mousemove', _donutHover);
        cvs.addEventListener('mouseleave', () => { _donutTooltip.style.display = 'none'; _drawDonut(-1); });
    }

    const total = acct.total_asset || 1;
    const cash = acct.cash || 0;

    _donutSegments = [];

    if (_donutMode === 'sector') {
        // ── Sector mode: sector_summary 기반 ──
        const summary = acct.sector_summary || [];
        summary.forEach((sec, i) => {
            const pct = sec.eval_amt / total * 100;
            if (pct > 0) _donutSegments.push({
                name: sec.sector, code: '', pct,
                amount: sec.eval_amt, color: DONUT_COLORS[i % DONUT_COLORS.length],
                pnl: sec.pnl || 0, pnl_rate: sec.eval_amt > 0 ? (sec.pnl / sec.eval_amt * 100) : 0,
                count: sec.count,
            });
        });
    } else {
        // ── Stock mode: holdings 기반, 비중순 + 소수 그룹핑 ──
        const GROUP_THRESHOLD = 2.0;
        let rawSegs = [];
        acct.holdings.forEach(h => {
            const amt = h.eval_amt || 0;
            const pct = amt / total * 100;
            if (pct > 0) rawSegs.push({
                name: h.name || h.code, code: h.code || '', pct,
                amount: amt, pnl: h.pnl || 0, pnl_rate: h.pnl_rate || 0,
            });
        });
        rawSegs.sort((a, b) => b.pct - a.pct);

        let etcAmount = 0, etcPct = 0, etcPnl = 0, etcCount = 0;
        rawSegs.forEach(s => {
            if (s.pct < GROUP_THRESHOLD) {
                etcAmount += s.amount; etcPct += s.pct; etcPnl += s.pnl; etcCount++;
            } else {
                _donutSegments.push({
                    ...s, color: DONUT_COLORS[_donutSegments.length % DONUT_COLORS.length],
                });
            }
        });
        if (etcPct > 0) {
            _donutSegments.push({
                name: `기타 (${etcCount}종목)`, code: '', pct: etcPct,
                amount: etcAmount, color: '#6B7280', pnl: etcPnl,
                pnl_rate: etcAmount > 0 ? (etcPnl / etcAmount * 100) : 0,
            });
        }
    }

    const cashPct = cash / total * 100;
    if (cashPct > 0) _donutSegments.push({name: '현금', code: '', pct: cashPct, amount: cash, color: '#484F58', pnl: 0, pnl_rate: 0});

    _drawDonut(-1);

    // Legend
    legend.innerHTML = _donutSegments.map((s, i) => {
        const countStr = s.count ? ` <span style="opacity:0.4">${s.count}</span>` : '';
        return `<div class="legend-item" data-idx="${i}" onmouseenter="_donutLegendHover(${i})" onmouseleave="_donutLegendOut()">` +
        `<span class="legend-dot" style="background:${s.color}"></span>` +
        `<span class="legend-name">${esc(s.name)}${countStr}</span>` +
        `<span class="legend-pct">${s.pct.toFixed(1)}%</span></div>`;
    }).join('');
}

function _drawDonut(hoverIdx) {
    if (!_donutCanvas) return;
    const ctx = _donutCanvas.getContext('2d');
    const W = 280, cx = W/2, cy = W/2, R = 120, r = 60;
    ctx.clearRect(0, 0, W, W);

    let startAngle = -Math.PI / 2;
    _donutSegments.forEach((seg, i) => {
        const sweep = (seg.pct / 100) * Math.PI * 2;
        const isHover = i === hoverIdx;
        const outerR = isHover ? R + 10 : R;
        const innerR = isHover ? r - 2 : r;

        ctx.beginPath();
        ctx.arc(cx, cy, outerR, startAngle, startAngle + sweep);
        ctx.arc(cx, cy, innerR, startAngle + sweep, startAngle, true);
        ctx.closePath();
        ctx.fillStyle = seg.color;
        ctx.globalAlpha = (hoverIdx >= 0 && !isHover) ? 0.4 : 1.0;
        ctx.fill();

        if (isHover) {
            ctx.strokeStyle = '#fff';
            ctx.lineWidth = 2;
            ctx.stroke();
        }
        startAngle += sweep;
    });
    ctx.globalAlpha = 1.0;

    // Center text
    ctx.textAlign = 'center';
    ctx.textBaseline = 'middle';
    if (_donutMode === 'sector') {
        const nSectors = _donutSegments.filter(s => s.name !== '현금').length;
        ctx.fillStyle = 'rgba(255,255,255,0.7)';
        ctx.font = 'bold 28px -apple-system, sans-serif';
        ctx.fillText(nSectors, cx, cy - 6);
        ctx.font = '14px -apple-system, sans-serif';
        ctx.fillStyle = 'rgba(255,255,255,0.35)';
        ctx.fillText('섹터', cx, cy + 16);
    } else {
        const nHoldings = _donutSegments.filter(s => s.name !== '현금' && !s.name.startsWith('기타')).length;
        const etcSeg = _donutSegments.find(s => s.name.startsWith('기타'));
        const totalStocks = nHoldings + (etcSeg ? parseInt(etcSeg.name.match(/\d+/)?.[0] || '0') : 0);
        ctx.fillStyle = 'rgba(255,255,255,0.7)';
        ctx.font = 'bold 28px -apple-system, sans-serif';
        ctx.fillText(totalStocks, cx, cy - 6);
        ctx.font = '14px -apple-system, sans-serif';
        ctx.fillStyle = 'rgba(255,255,255,0.35)';
        ctx.fillText('종목', cx, cy + 16);
    }
}

function _donutHover(e) {
    if (!_donutCanvas || _donutSegments.length === 0) return;
    const rect = _donutCanvas.getBoundingClientRect();
    const scaleX = 280 / rect.width, scaleY = 280 / rect.height;
    const mx = (e.clientX - rect.left) * scaleX;
    const my = (e.clientY - rect.top) * scaleY;
    const cx = 140, cy = 140;
    const dx = mx - cx, dy = my - cy;
    const dist = Math.sqrt(dx*dx + dy*dy);

    if (dist < 55 || dist > 135) {
        _donutTooltip.style.display = 'none';
        _drawDonut(-1);
        return;
    }

    let angle = Math.atan2(dy, dx) + Math.PI/2;
    if (angle < 0) angle += Math.PI * 2;
    const anglePct = angle / (Math.PI * 2) * 100;

    let cumPct = 0, hitIdx = -1;
    for (let i = 0; i < _donutSegments.length; i++) {
        cumPct += _donutSegments[i].pct;
        if (anglePct <= cumPct) { hitIdx = i; break; }
    }

    if (hitIdx < 0) { _donutTooltip.style.display = 'none'; _drawDonut(-1); return; }

    _drawDonut(hitIdx);
    const seg = _donutSegments[hitIdx];
    const pnl = parseFloat(seg.pnl) || 0;
    const pnlRate = parseFloat(seg.pnl_rate) || 0;
    const sign = pnl >= 0 ? '+' : '';
    _donutTooltip.innerHTML = `<strong>${esc(seg.name)}</strong>${seg.code ? ' ('+seg.code+')' : ''}<br>` +
        `${seg.pct.toFixed(1)}% · ${formatKRW(seg.amount)}<br>` +
        `<span class="${pnl >= 0 ? 'positive' : 'negative'}">${sign}${formatKRW(pnl)} (${sign}${pnlRate.toFixed(1)}%)</span>`;
    _donutTooltip.style.display = 'block';

    // Position tooltip near mouse
    const tipX = e.clientX - rect.left + 10;
    const tipY = e.clientY - rect.top - 10;
    _donutTooltip.style.left = tipX + 'px';
    _donutTooltip.style.top = tipY + 'px';
}

function _donutLegendHover(idx) { _drawDonut(idx); }
function _donutLegendOut() { _drawDonut(-1); }

// ── Holdings View Switch (종목/섹터) ────────────────────────

(function() {
    document.addEventListener('DOMContentLoaded', () => {
        const listBtn = document.getElementById('view-list-btn');
        const sectorBtn = document.getElementById('view-sector-btn');
        if (!listBtn || !sectorBtn) return;
        listBtn.addEventListener('click', () => {
            listBtn.classList.add('active'); sectorBtn.classList.remove('active');
            document.getElementById('holdings-list').style.display = '';
            document.getElementById('sector-view').style.display = 'none';
            _donutMode = 'stock';
            if (_lastDonutData) updateDonutChart(_lastDonutData);
        });
        sectorBtn.addEventListener('click', () => {
            sectorBtn.classList.add('active'); listBtn.classList.remove('active');
            document.getElementById('holdings-list').style.display = 'none';
            document.getElementById('sector-view').style.display = '';
            _donutMode = 'sector';
            if (_lastDonutData) updateDonutChart(_lastDonutData);
        });
    });
})();

function updateSectorView(data) {
    const container = document.getElementById('sector-view');
    if (!container) return;
    const acct = data.account || {};
    const summary = acct.sector_summary;
    if (!summary || summary.length === 0) {
        container.innerHTML = '<div class="holdings-empty">섹터 데이터 없음</div>';
        return;
    }
    const holdings = acct.holdings || [];
    const total = acct.total_asset || 1;

    let html = '';
    for (const sec of summary) {
        const pct = (sec.eval_amt / total * 100).toFixed(1);
        const pnl = sec.pnl || 0;
        const pnlSign = pnl >= 0 ? '+' : '';
        const pnlClass = pnl > 0 ? 'positive' : pnl < 0 ? 'negative' : 'neutral';
        const sectorHoldings = holdings.filter(h => h.sector === sec.sector);

        html += `<div class="sector-group">
            <div class="sector-header">
                <span class="sector-name">${esc(sec.sector)}</span>
                <span class="sector-meta">${sec.count}종목 · ${pct}%</span>
                <span class="sector-pnl ${pnlClass}">${pnlSign}${formatKRW(pnl)}</span>
            </div>
            <div class="sector-holdings">`;
        for (const h of sectorHoldings) {
            const hPnl = parseInt(h.pnl) || 0;
            const hRate = parseFloat(h.pnl_rate) || 0;
            const hSign = hPnl >= 0 ? '+' : '';
            const hClass = hPnl > 0 ? 'positive' : hPnl < 0 ? 'negative' : 'neutral';
            html += `<div class="sector-holding-item">
                <span class="sh-name">${esc(h.name || h.code)}</span>
                <span class="sh-pnl ${hClass}">${hSign}${hRate.toFixed(1)}%</span>
            </div>`;
        }
        html += `</div></div>`;
    }
    container.innerHTML = html;
}

// ── Sector Regime ───────────────────────────────────────────

function updateSectorRegime(data) {
    const grid = document.getElementById('sector-regime-grid');
    if (!grid) return;

    // 우선: theme_regime (ka90001 실시간), fallback: sector_summary (보유종목 기반)
    const themes = data.theme_regime;
    if (themes && themes.length > 0) {
        grid.innerHTML = themes.map(t => {
            const regime = (t.regime || 'SIDEWAYS').toUpperCase();
            const cls = regime === 'BULL' ? 'sr-bull' : regime === 'BEAR' ? 'sr-bear' : 'sr-sideways';
            const chg = t.change_pct || 0;
            const chgCls = chg > 0 ? 'positive' : chg < 0 ? 'negative' : 'neutral';
            const streak = t.streak || 1;
            const streakStr = `${streak}일째`;
            const code = t.code || '';
            return `<div class="sr-card ${cls}" data-theme-code="${code}" onmouseenter="_themeHover(this,'${code}')" onmouseleave="_themeOut(this)">` +
                `<div class="sr-name">${esc(t.name)}</div>` +
                `<div class="sr-regime-label">${regime}</div>` +
                `<div class="sr-pnl ${chgCls}">${chg >= 0 ? '+' : ''}${chg.toFixed(2)}%</div>` +
                `<div class="sr-meta">${t.count || 0}종목 · ${streakStr}</div>` +
                `<div class="sr-tooltip" style="display:none"></div>` +
            `</div>`;
        }).join('');
        return;
    }

    // Fallback: sector_summary
    const acct = data.account || {};
    const summary = acct.sector_summary;
    if (!summary || summary.length === 0) {
        grid.innerHTML = '<div class="dim" style="text-align:center;padding:12px">theme data loading...</div>';
        return;
    }
    const total = acct.total_asset || 1;
    grid.innerHTML = summary.map(s => {
        const regime = (s.regime || 'SIDEWAYS').toUpperCase();
        const cls = regime === 'BULL' ? 'sr-bull' : regime === 'BEAR' ? 'sr-bear' : 'sr-sideways';
        const pnlPct = s.pnl_pct || 0;
        const pnlCls = pnlPct > 0 ? 'positive' : pnlPct < 0 ? 'negative' : 'neutral';
        const pct = (s.eval_amt / total * 100).toFixed(1);
        return `<div class="sr-card ${cls}">` +
            `<div class="sr-name">${esc(s.sector)}</div>` +
            `<div class="sr-regime-label">${regime}</div>` +
            `<div class="sr-pnl ${pnlCls}">${pnlPct >= 0 ? '+' : ''}${pnlPct.toFixed(1)}%</div>` +
            `<div class="sr-meta">${s.count}종목 · ${pct}%</div>` +
        `</div>`;
    }).join('');
}

// ── Theme Hover (ka90002 on-demand) ─────────────────────────

const _themeDetailCache = {};  // {code: {data, ts}}

function _themeHover(el, code) {
    if (!code) return;
    const tip = el.querySelector('.sr-tooltip');
    if (!tip) return;

    const cached = _themeDetailCache[code];
    if (cached && (Date.now() - cached.ts) < 300000) {
        tip.innerHTML = cached.html;
        tip.style.display = 'block';
        return;
    }

    tip.innerHTML = '<span class="dim">loading...</span>';
    tip.style.display = 'block';

    fetch(`/api/theme/${code}`)
        .then(r => r.json())
        .then(d => {
            const stocks = d.stocks || [];
            if (stocks.length === 0) {
                tip.innerHTML = '<span class="dim">데이터 없음</span>';
                return;
            }
            const heldCount = stocks.filter(s => s.held).length;
            let html = `<div class="sr-tip-header">${stocks.length}종목`;
            if (heldCount > 0) html += ` · <span style="color:#F04452">보유 ${heldCount}</span>`;
            html += `</div>`;
            stocks.sort((a, b) => (b.change_pct || 0) - (a.change_pct || 0));
            html += stocks.slice(0, 10).map(s => {
                const chg = s.change_pct || 0;
                const cls = chg > 0 ? 'positive' : chg < 0 ? 'negative' : 'neutral';
                const held = s.held ? ' *' : '';
                return `<div class="sr-tip-row"><span class="sr-tip-name">${esc(s.name)}${held}</span>` +
                    `<span class="sr-tip-chg ${cls}">${chg >= 0 ? '+' : ''}${chg.toFixed(2)}%</span></div>`;
            }).join('');
            if (stocks.length > 10) html += `<div class="dim">...외 ${stocks.length - 10}종목</div>`;
            tip.innerHTML = html;
            _themeDetailCache[code] = {html, ts: Date.now()};
        })
        .catch(() => { tip.innerHTML = '<span class="dim">조회 실패</span>'; });
}

let _themeOutTimer = null;
function _themeOut(el) {
    // 딜레이: 마우스가 툴팁으로 이동할 시간 확보
    _themeOutTimer = setTimeout(() => {
        const tip = el.querySelector('.sr-tooltip');
        if (tip) tip.style.display = 'none';
    }, 200);
}

// 툴팁 위에 마우스가 있으면 숨기기 취소
document.addEventListener('mouseover', (e) => {
    if (e.target.closest && e.target.closest('.sr-tooltip')) {
        clearTimeout(_themeOutTimer);
    }
});

// ── Trail Stops ─────────────────────────────────────────────

function updateTrailStops(data) {
    const ts = data.trail_stops;
    if (!ts || !ts.stops) return;

    const holdingsCards = document.querySelectorAll('.holding-card');
    const stopMap = {};
    ts.stops.forEach(s => { stopMap[s.code] = s; });

    holdingsCards.forEach(card => {
        const code = card.dataset.code;
        if (!code) return;
        const stop = stopMap[code];
        let barEl = card.querySelector('.trail-stop-bar');

        if (!stop || stop.risk_zone === 'N/A') {
            if (barEl) barEl.style.display = 'none';
            return;
        }

        if (!barEl) {
            barEl = document.createElement('div');
            barEl.className = 'trail-stop-bar';
            barEl.innerHTML = '<div class="trail-fill"></div>';
            card.appendChild(barEl);
            const meta = document.createElement('div');
            meta.className = 'trail-meta';
            card.appendChild(meta);
        }
        barEl.style.display = '';

        const fill = barEl.querySelector('.trail-fill');
        const meta = card.querySelector('.trail-meta');

        if (stop.triggered === true) {
            fill.style.width = '100%';
            fill.className = 'trail-fill trail-triggered';
            if (meta) meta.innerHTML = '<span class="trail-triggered-label">TRIGGERED</span>';
            return;
        }

        // Progress: 100% at HWM, 0% at trail_price
        const range = stop.hwm - stop.trail_price;
        const pos = range > 0 ? (stop.current_price - stop.trail_price) / range * 100 : 50;
        const pct = Math.max(0, Math.min(100, pos));
        fill.style.width = pct + '%';

        const zoneClass = {SAFE:'trail-safe', CAUTION:'trail-caution', WARNING:'trail-warn', DANGER:'trail-danger'}[stop.risk_zone] || '';
        fill.className = 'trail-fill ' + zoneClass;

        if (meta) {
            const dropStr = stop.drop_from_hwm_pct !== null ? stop.drop_from_hwm_pct.toFixed(1) + '%' : '--';
            const distStr = stop.distance_to_trail_pct !== null ? stop.distance_to_trail_pct.toFixed(1) + '%' : '--';
            meta.textContent = `고점 ${dropStr} | 트리거까지 ${distStr}`;
        }
    });
}

// ── RECON Card ──────────────────────────────────────────────

function updateReconCard(data) {
    const recon = data.recon;
    if (!recon) return;

    const statusEl = document.getElementById('recon-status');
    // P0-2 hotfix: bail if recon-card absent (/debug).
    if (!statusEl) return;
    const lastEl = document.getElementById('recon-last-run');
    const ageEl = document.getElementById('recon-age');
    const badgesEl = document.getElementById('recon-badges');
    const card = document.getElementById('recon-card');

    if (recon.unreliable) {
        statusEl.textContent = '비신뢰';
        statusEl.className = 'v status-error';
        if (card) card.style.borderColor = 'var(--red)';
    } else {
        statusEl.textContent = '정상';
        statusEl.className = 'v status-ok';
        if (card) card.style.borderColor = '';
    }

    lastEl.textContent = recon.last_run ? String(recon.last_run).substring(0, 19) : '--';

    const ageSec = recon.age_sec || 0;
    if (ageSec < 60) ageEl.textContent = Math.round(ageSec) + '초';
    else if (ageSec < 3600) ageEl.textContent = Math.round(ageSec / 60) + '분';
    else ageEl.textContent = (ageSec / 3600).toFixed(1) + '시간';
    ageEl.className = 'v mono' + (recon.stale ? ' status-warn' : '');

    if (badgesEl) {
        let b = '';
        if (recon.from_cache) b += '<span class="src-badge cache">CACHE</span>';
        if (recon.stale) b += '<span class="src-badge stale">&#9203;</span>';
        if (recon.expired) b += '<span class="src-badge expired">EXPIRED</span>';
        badgesEl.innerHTML = b;
    }
}

// ── Trades Timeline ─────────────────────────────────────────

let _lastTradesSourceTs = 0;

function updateTradesTimeline(data) {
    const preview = data.recent_trades_preview;
    if (!preview || !preview.trades) return;
    const srcTs = preview.source_ts || 0;
    if (srcTs === _lastTradesSourceTs) return;
    _lastTradesSourceTs = srcTs;

    const container = document.getElementById('trades-timeline');
    if (!container) return;

    if (preview.trades.length === 0) {
        container.innerHTML = '<div class="holdings-empty">거래 내역 없음</div>';
        return;
    }

    const html = preview.trades.map(t => {
        const isBuy = t.side === 'BUY';
        const cls = isBuy ? 'trade-buy' : 'trade-sell';
        const icon = isBuy ? '&#9650;' : '&#9660;';
        const modeBadge = t.mode === 'REST' ? '<span class="trade-mode rest">REST</span>' : '';
        return `<div class="trade-card ${cls}" data-eid="${esc(t.event_id)}">
            <span class="trade-date">${esc(t.date)}</span>
            <span class="trade-side">${icon} ${t.side}</span>
            <span class="trade-code">${esc(t.code)}</span>
            <span class="trade-detail">${t.quantity}주 &times; ${t.price.toLocaleString()}원</span>
            ${modeBadge}
        </div>`;
    }).join('');
    container.innerHTML = html;
}


// ── Regime Display (Gradient Bar) ────────────────────────────

const REGIME_LABELS_MAP = {1:'STRONG_BEAR', 2:'BEAR', 3:'NEUTRAL', 4:'BULL', 5:'STRONG_BULL'};

// Phase 3 (2026-04-25): _regimeToBarPct / _regimeLabelColor moved to
// kr/web/static/components/regime.js (qc-regime-card component).
// They were only referenced by updateRegimeDisplay's previous body —
// confirmed unused elsewhere in this file before removal.

function updateRegimeDisplay(data) {
    if (window.qc && window.qc.regime && typeof window.qc.regime.render === 'function') {
        window.qc.regime.render(document.getElementById('regime-section'), data);
    }
}

// ── P2.4: Data Events + Market Context Panel ──────────────────
// Jeff 제약 #5: market_context (현재 상태) / data_events (로그) 명확히 분리.
// API endpoints:
//   GET /api/debug/market_context
//   GET /api/debug/data_events?limit=50&min_level=WARN&sources=KOSPI

let _dataEventsTimer = null;

function initDataEventsPanel() {
    // P0-2 C3: guard by target-DOM presence instead of mode. Data Events
    // panel lives on /debug only — on Dashboard the target section is
    // absent so loadMarketContext/loadDataEvents both no-op via their
    // own null-check guards (C2).
    const hasPanel = !!document.getElementById('devt-filter-level')
                     || !!document.getElementById('mctx-mode-badge');
    if (!hasPanel) return;
    loadMarketContext();
    loadDataEvents();
    _dataEventsTimer = setInterval(() => {
        loadMarketContext();
        loadDataEvents();
    }, 10000);

    // 필터 이벤트
    const lvl = document.getElementById('devt-filter-level');
    const src = document.getElementById('devt-filter-source');
    if (lvl) lvl.addEventListener('change', loadDataEvents);
    if (src) src.addEventListener('input', _debounce(loadDataEvents, 300));
}

function _debounce(fn, ms) {
    let t;
    return (...a) => { clearTimeout(t); t = setTimeout(() => fn(...a), ms); };
}

async function loadMarketContext() {
    // P0-2 C2: skip if target section absent (Dashboard page, market-ctx moved to /debug).
    if (!document.getElementById('mctx-mode-badge')) return;
    try {
        const r = await fetch('/api/debug/market_context');
        const d = await r.json();
        updateMarketContextPanel(d);
    } catch (e) { /* silent */ }
}

function updateMarketContextPanel(d) {
    const badge = document.getElementById('mctx-mode-badge');
    const ids = {
        'mctx-effective': '--',
        'mctx-stock': '--',
        'mctx-kospi': '--',
        'mctx-ready': '--',
        'mctx-runmode': '--',
        'mctx-lastrun': '--',
        'mctx-source': '--',
        'mctx-reasons': '--',
    };

    if (!d || !d.ok || !d.market_context) {
        Object.entries(ids).forEach(([id, v]) => {
            const el = document.getElementById(id);
            if (el) el.textContent = v;
        });
        if (badge) {
            badge.textContent = d && d.reason ? d.reason : 'NO DATA';
            badge.className = 'mctx-badge mctx-idle';
        }
        return;
    }

    const m = d.market_context;
    const runMode = m.run_mode || 'UNKNOWN';
    const setText = (id, v) => {
        const el = document.getElementById(id);
        if (el) el.textContent = v != null ? v : '--';
    };
    setText('mctx-effective', m.effective_trade_date);
    setText('mctx-stock', m.stock_last_date);
    setText('mctx-kospi', m.kospi_last_date);
    setText('mctx-ready', m.index_ready ? '✓ YES' : '✗ NO');
    setText('mctx-runmode', runMode);
    setText('mctx-lastrun', d.last_run_date);
    setText('mctx-source', d.selected_source);
    setText('mctx-reasons', (m.degraded_reasons || []).join(' · ') || '—');

    // ready 색상
    const readyEl = document.getElementById('mctx-ready');
    if (readyEl) readyEl.className = 'v ' + (m.index_ready ? 'status-ok' : 'status-warn');
    const rmEl = document.getElementById('mctx-runmode');
    if (rmEl) rmEl.className = 'v ' + (runMode === 'OK' ? 'status-ok' : 'status-warn');

    if (badge) {
        badge.textContent = runMode;
        badge.className = 'mctx-badge ' + (runMode === 'OK' ? 'mctx-ok' : 'mctx-degraded');
    }
}

async function loadDataEvents() {
    try {
        const lvl = document.getElementById('devt-filter-level');
        const src = document.getElementById('devt-filter-source');
        const params = new URLSearchParams({ limit: '50' });
        if (lvl && lvl.value) params.set('min_level', lvl.value);
        if (src && src.value.trim()) params.set('sources', src.value.trim());
        const r = await fetch('/api/debug/data_events?' + params);
        const d = await r.json();
        updateDataEventsTable(d.events || []);
    } catch (e) { /* silent */ }
}

function updateDataEventsTable(events) {
    const tbody = document.getElementById('devt-body');
    if (!tbody) return;
    if (!events || events.length === 0) {
        tbody.innerHTML = '<tr><td colspan="6" class="empty-row">이벤트 없음</td></tr>';
        return;
    }

    const levelCls = {
        'DEBUG': '',
        'INFO': 'badge-ok',
        'WARN': 'badge-retry',
        'ERROR': 'badge-error',
        'CRITICAL': 'badge-error',
    };

    let html = '';
    for (const ev of events) {
        const tsStr = _fmtEventTs(ev.ts);
        const dupBadge = ev.suppressed_count > 0
            ? `<span class="mctx-badge mctx-degraded" title="${ev.suppressed_count}건 중복 흡수">${ev.suppressed_count}x</span>`
            : '';
        const escalatedMark = ev.escalated ? ' ⚡' : '';
        const rowCls = (ev.level === 'CRITICAL' || ev.level === 'ERROR') ? 'trace-error'
                     : (ev.level === 'WARN' ? 'trace-timeout' : '');

        html += `
            <tr class="${rowCls}">
                <td class="mono">${tsStr}</td>
                <td class="mono" title="${esc(ev.source)}">${truncate(ev.source, 22)}</td>
                <td><span class="status-badge ${levelCls[ev.level] || ''}">${ev.level}${escalatedMark}</span></td>
                <td class="mono" title="${esc(ev.code)}">${truncate(ev.code, 22)}</td>
                <td class="error-cell" title="${esc(ev.message)}">${truncate(ev.message, 50)}</td>
                <td>${dupBadge}</td>
            </tr>
        `;
    }
    tbody.innerHTML = html;
}

function _fmtEventTs(ts) {
    if (!ts) return '--';
    const d = new Date(ts * 1000);
    const p = (n) => n.toString().padStart(2, '0');
    return `${p(d.getHours())}:${p(d.getMinutes())}:${p(d.getSeconds())}.${d.getMilliseconds().toString().padStart(3, '0')}`;
}
