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
    initTestOrder();
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
    updateDDGuard(data);
    updateHeroBadges(data);
    updateRegimeDisplay(data);
    updateDonutChart(data);
    updateCompareChart(data);
    updateSectorView(data);
    updateSectorRegime(data);
    updateTrailStops(data);
    updateReconCard(data);
    updateIndexDisplay(data);
    updateTradesTimeline(data);

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

    // Server badge + LED → nav bar
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

    // Nav badges (REAL + dot + mode switch)
    const nb = document.getElementById('qnav-badges');
    if (nb && !nb._initialized) {
        const svr = data.server?.type || 'REAL';
        const dotCls = statusLower === 'green' ? 'ok' : statusLower === 'red' ? 'err' : '';
        nb.innerHTML =
            `<span class="qnav-badge qnav-badge-${svr.toLowerCase()}">${svr}</span>` +
            `<span id="nav-dot" class="qnav-dot ${dotCls}"></span>` +
            `<div class="qnav-mode-toggle" id="nav-mode-switch">` +
                `<button class="qnav-mode" data-mode="basic" onclick="switchMode('basic')">Basic</button>` +
                `<button class="qnav-mode" data-mode="operator" onclick="switchMode('operator')">Operator</button>` +
                `<button class="qnav-mode" data-mode="debug" onclick="switchMode('debug')">Debug</button>` +
            `</div>`;
        nb._initialized = true;
        // Sync active mode button
        nb.querySelectorAll('.qnav-mode').forEach(b => {
            b.classList.toggle('active', b.dataset.mode === currentMode);
        });
    } else if (nb) {
        // Update dot color only
        const navDot = document.getElementById('nav-dot');
        if (navDot) navDot.className = 'qnav-dot ' + (statusLower === 'green' ? 'ok' : statusLower === 'red' ? 'err' : '');
    }
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

function _execRebalCmd(url, body, resultEl) {
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

// Store account data for profit section + comparison tracking
let _dayTrough = Infinity;  // track intraday lowest equity

function storeAccountData(data) {
    if (data.account) window._lastAccountData = data.account;
    // Track intraday trough
    const asset = (data.account || {}).total_asset || 0;
    if (asset > 0 && asset < _dayTrough) _dayTrough = asset;
    // Update comparisons from dd_guard
    updateProfitComparisons(data);
}

function updateProfitComparisons(data) {
    const dd = data.dd_guard || {};
    const asset = (data.account || {}).total_asset || 0;
    if (!asset || asset <= 0) return;

    const prevClose = dd.source_prev_close || 0;
    const peak = dd.source_peak || 0;
    const trough = _dayTrough < Infinity ? _dayTrough : 0;

    const setCmp = (id, cur, ref) => {
        const el = document.getElementById(id);
        if (!el || !ref || ref <= 0) { if (el) el.textContent = '--'; return; }
        const diff = cur - ref;
        const pct = ((cur / ref) - 1) * 100;
        const sign = diff >= 0 ? '+' : '';
        el.textContent = `${sign}${formatKRW(diff)} (${sign}${pct.toFixed(2)}%)`;
        el.className = 'profit-sub-value ' + (diff > 0 ? 'positive' : diff < 0 ? 'negative' : 'neutral');
    };

    setCmp('cmp-prev-close', asset, prevClose);
    setCmp('cmp-peak', asset, peak);
    setCmp('cmp-trough', asset, trough);
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

    let html = '<div class="mini-cards-grid">';
    for (const h of holdings) {
        const code = h.code || '';
        const name = h.name || code;
        const qty = h.qty || 0;
        const curPrice = h.cur_price || 0;
        const evalAmt = h.eval_amt || (curPrice * qty);
        const pnl = parseInt(h.pnl) || 0;
        const pnlRate = parseFloat(h.pnl_rate || '0');
        const isPositive = pnl > 0;
        const isNegative = pnl < 0;
        const colorClass = isPositive ? 'positive' : isNegative ? 'negative' : 'neutral';
        const sign = isPositive ? '+' : '';
        const barColor = isPositive ? '#F04452' : isNegative ? '#3182F6' : 'var(--border)';
        const barWidth = Math.min(Math.abs(pnlRate) * 5, 100);

        html += `
        <div class="mini-card" data-code="${code}">
            <div class="mini-top">
                <span class="mini-name">${name}</span>
                <span class="mini-pnl ${colorClass}">${sign}${pnlRate.toFixed(1)}%</span>
            </div>
            <div class="mini-bar"><div class="mini-bar-fill" style="width:${barWidth}%;background:${barColor}"></div></div>
            <div class="mini-bottom">
                <span class="mini-eval">${formatKRW(evalAmt)}</span>
                <span class="mini-qty">${qty}주</span>
            </div>
        </div>`;
    }
    html += '</div>';
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

    // Body class for CSS mode hiding
    document.body.className = 'mode-' + mode;

    // Button states (original + nav)
    document.querySelectorAll('.mode-btn').forEach(btn => {
        btn.classList.toggle('active', btn.dataset.mode === mode);
    });
    document.querySelectorAll('.qnav-mode').forEach(btn => {
        btn.classList.toggle('active', btn.dataset.mode === mode);
    });

    // Show/hide sections — each mode shows ONLY its own sections
    document.querySelectorAll('.mode-operator').forEach(el => {
        el.hidden = mode !== 'operator';
    });
    document.querySelectorAll('.mode-debug').forEach(el => {
        el.hidden = mode !== 'debug';
    });

    // Load debug data
    if (mode === 'debug') {
        fetchLogs();
        updateLatencyHistogram();
        loadDbHealth();
    }

    if (mode === 'operator') {
        loadRebalPreview();
    }

    // Re-render with current data
    if (lastState) updateDashboard(lastState);
}

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

        if (data.status === 'NO_DATA') {
            badge.textContent = 'PENDING';
            badge.className = 'badge badge-mock';
            alertsEl.innerHTML = '<div class="dim">배치 실행 후 분석 결과가 표시됩니다</div>';
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

async function loadRebalPreview() {
    try {
        const resp = await fetch('/api/rebalance/preview-compare');
        const data = await resp.json();
        if (data.error) return;

        const hdr = document.getElementById('rp-target-date');
        const dday = document.getElementById('rp-dday');
        if (hdr) hdr.textContent = `Target: ${data.target_date} (${data.target_count}종목)`;
        if (dday) dday.textContent = `D-${data.days_remaining}`;

        // New entries
        const newEl = document.getElementById('rp-new-entries');
        if (newEl) {
            if (data.new_entries && data.new_entries.length > 0) {
                newEl.innerHTML = data.new_entries.map(s => {
                    const cls = s.change_pct >= 0 ? 'rp-change-pos' : 'rp-change-neg';
                    const sign = s.change_pct >= 0 ? '+' : '';
                    return `<div class="rp-row"><span class="rp-code">${s.code}</span><span class="rp-name">${s.name}</span><span>${s.open?.toLocaleString()}→${s.close?.toLocaleString()}</span><span class="${cls}">${sign}${s.change_pct}%</span></div>`;
                }).join('');
            } else {
                newEl.textContent = '변경 없음';
            }
        }

        // Exits
        const exitEl = document.getElementById('rp-exits');
        if (exitEl) {
            if (data.exits && data.exits.length > 0) {
                exitEl.innerHTML = data.exits.map(s => {
                    const cls = s.change_pct >= 0 ? 'rp-change-pos' : 'rp-change-neg';
                    const sign = s.change_pct >= 0 ? '+' : '';
                    return `<div class="rp-row"><span class="rp-code">${s.code}</span><span class="rp-name">${s.name}</span><span>${s.open?.toLocaleString()}→${s.close?.toLocaleString()}</span><span class="${cls}">${sign}${s.change_pct}%</span></div>`;
                }).join('');
            } else {
                exitEl.textContent = '변경 없음';
            }
        }

        // Unchanged
        const keepEl = document.getElementById('rp-unchanged');
        if (keepEl) {
            if (data.unchanged && data.unchanged.length > 0) {
                keepEl.innerHTML = data.unchanged.map(s => {
                    const cls = s.change_pct >= 0 ? 'rp-change-pos' : 'rp-change-neg';
                    const sign = s.change_pct >= 0 ? '+' : '';
                    return `<div class="rp-row"><span class="rp-code">${s.code}</span><span class="rp-name">${s.name}</span><span>${s.open?.toLocaleString()}→${s.close?.toLocaleString()}</span><span class="${cls}">${sign}${s.change_pct}%</span></div>`;
                }).join('');
            } else {
                keepEl.textContent = '--';
            }
        }

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
    const riskBadge = document.getElementById('system-risk-badge');
    const emergBadge = document.getElementById('emergency-badge');

    // BUY STATUS
    const dd = data.dd_guard || {};
    const perm = dd.buy_permission || 'UNKNOWN';
    const buyClass = {NORMAL:'badge-normal', REDUCED:'badge-reduced', BLOCKED:'badge-blocked'}[perm] || 'badge-unknown';
    buyBadge.className = 'status-badge ' + buyClass;
    buyBadge.textContent = 'BUY: ' + perm;

    // SYSTEM RISK
    const sr = data.system_risk || {};
    const primary = sr.primary || 'OK';
    const riskClass = {OK:'badge-ok', STALE:'badge-stale', RECON_WARN:'badge-recon',
                       SAFE_MODE:'badge-safemode', READ_FAIL:'badge-readfail'}[primary] || 'badge-unknown';
    riskBadge.className = 'status-badge ' + riskClass;
    riskBadge.textContent = 'RISK: ' + primary;
    if (sr.reason_codes && sr.reason_codes.length > 1) {
        riskBadge.title = sr.reason_codes.join(', ');
    }

    // Emergency badge
    if (primary === 'SAFE_MODE' || primary === 'READ_FAIL') {
        emergBadge.style.display = 'inline-flex';
        emergBadge.textContent = primary === 'SAFE_MODE' ? 'SAFE MODE' : 'READ FAIL';
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

    // Nav bar index (코스피)
    const ni = document.getElementById('qnav-index');
    if (ni && idx.price) {
        const chgCls = idx.change_pct >= 0 ? 'up' : 'dn';
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

// ── KOSPI vs Portfolio Comparison Chart (DB-backed) ─────────

let _compareData = null;
let _compareLastFetch = 0;

function updateCompareChart(data) {
    const canvas = document.getElementById('compare-chart');
    if (!canvas) return;

    const now = Date.now();
    // Fetch from DB API every 60s
    if (!_compareData || (now - _compareLastFetch) > 60000) {
        _compareLastFetch = now;
        fetch('/api/chart/today')
            .then(r => r.json())
            .then(d => { _compareData = d.snapshots || []; _drawCompareChart(canvas); })
            .catch(() => {});
        return;
    }
    _drawCompareChart(canvas);
}

function _drawCompareChart(canvas) {
    if (!_compareData || _compareData.length < 2) return;

    const ctx = canvas.getContext('2d');
    const dpr = window.devicePixelRatio || 1;
    const W = canvas.width, H = canvas.height;
    const pad = {t: 12, b: 22, l: 44, r: 14};
    ctx.clearRect(0, 0, W, H);

    const kospiPts = _compareData.map(s => ({t: s.t, v: s.kospi || 0}));
    const portPts = _compareData.map(s => ({t: s.t, v: s.portfolio || 0}));

    const allVals = [...kospiPts.map(p=>p.v), ...portPts.map(p=>p.v)];
    let yMin = Math.min(0, ...allVals);
    let yMax = Math.max(0, ...allVals);
    const yPad = Math.max(0.5, (yMax - yMin) * 0.15);
    yMin -= yPad; yMax += yPad;

    const chartW = W - pad.l - pad.r;
    const chartH = H - pad.t - pad.b;
    function yToPixel(v) { return pad.t + chartH - (v - yMin) / (yMax - yMin) * chartH; }

    // Grid lines
    ctx.strokeStyle = 'rgba(255,255,255,0.06)';
    ctx.lineWidth = 1;
    const zeroY = yToPixel(0);
    ctx.beginPath(); ctx.moveTo(pad.l, zeroY); ctx.lineTo(W - pad.r, zeroY); ctx.stroke();

    // Draw line function
    function drawLine(points, color, lineW) {
        if (points.length < 2) return;
        const step = chartW / Math.max(points.length - 1, 1);
        ctx.strokeStyle = color;
        ctx.lineWidth = lineW;
        ctx.lineJoin = 'round';
        ctx.beginPath();
        points.forEach((p, i) => {
            const x = pad.l + i * step;
            const y = yToPixel(p.v);
            if (i === 0) ctx.moveTo(x, y); else ctx.lineTo(x, y);
        });
        ctx.stroke();

        // Area fill
        ctx.globalAlpha = 0.05;
        ctx.fillStyle = color;
        ctx.lineTo(pad.l + (points.length-1) * step, zeroY);
        ctx.lineTo(pad.l, zeroY);
        ctx.closePath();
        ctx.fill();
        ctx.globalAlpha = 1.0;
    }

    drawLine(kospiPts, '#58a6ff', 1.5);
    drawLine(portPts, '#F04452', 2);

    // Y labels
    ctx.fillStyle = 'rgba(255,255,255,0.35)';
    ctx.font = '10px monospace';
    ctx.textAlign = 'right';
    ctx.fillText(yMax.toFixed(1) + '%', pad.l - 4, pad.t + 10);
    ctx.fillText(yMin.toFixed(1) + '%', pad.l - 4, H - pad.b);
    ctx.fillText('0%', pad.l - 4, zeroY + 3);

    // Time labels
    ctx.textAlign = 'center';
    ctx.fillStyle = 'rgba(255,255,255,0.25)';
    const n = kospiPts.length;
    if (n > 0) {
        ctx.fillText(kospiPts[0].t, pad.l, H - 4);
        if (n > 1) ctx.fillText(kospiPts[n-1].t, pad.l + chartW, H - 4);
        if (n > 4) ctx.fillText(kospiPts[Math.floor(n/2)].t, pad.l + chartW/2, H - 4);
    }

    // Current values (top-right)
    const lastK = kospiPts[n-1]; const lastP = portPts[portPts.length-1];
    ctx.textAlign = 'right';
    ctx.font = '11px monospace';
    if (lastP) { ctx.fillStyle = '#F04452'; ctx.fillText((lastP.v>=0?'+':'') + lastP.v.toFixed(2) + '%', W - pad.r, pad.t + 12); }
    if (lastK) { ctx.fillStyle = '#58a6ff'; ctx.fillText((lastK.v>=0?'+':'') + lastK.v.toFixed(2) + '%', W - pad.r, pad.t + 24); }
}

// ── Regime Display (Gradient Bar) ────────────────────────────

const REGIME_LABELS_MAP = {1:'STRONG_BEAR', 2:'BEAR', 3:'NEUTRAL', 4:'BULL', 5:'STRONG_BULL'};

function _regimeToBarPct(level, score) {
    // 5단계: 1=0%, 2=25%, 3=50%, 4=75%, 5=100%
    // score로 미세 조정 (±10%)
    const base = ((level - 1) / 4) * 100;
    const adjust = (score || 0) * 5; // score 범위에 따라 조정
    return Math.max(2, Math.min(98, base + adjust));
}

function _regimeLabelColor(level) {
    if (level <= 1) return '#3182F6';
    if (level <= 2) return '#5B9CF6';
    if (level <= 3) return '#FFD600';
    if (level <= 4) return '#F07060';
    return '#F04452';
}

function updateRegimeDisplay(data) {
    const rp = data.regime_prediction;

    // ── 오늘 실제 레짐 (from SSE regime_actual or latest) ──
    const todayMarker = document.getElementById('regime-today-marker');
    const todayDetail = document.getElementById('regime-today-detail');
    const todayBreakdown = document.getElementById('regime-today-breakdown');
    const todayDate = document.getElementById('regime-today-date');

    // actual은 SSE에 포함되거나 별도 fetch
    const actual = data.regime_actual;
    if (actual && !actual.unavailable && todayMarker) {
        const lvl = actual.actual_regime || 3;
        const total = actual.scores ? actual.scores.total : 0;
        const pct = _regimeToBarPct(lvl, total);
        todayMarker.style.left = pct + '%';
        if (todayDetail) {
            todayDetail.innerHTML = `<span class="regime-detail-label" style="color:${_regimeLabelColor(lvl)}">${actual.actual_label||'--'}</span>` +
                `<span class="regime-detail-score">점수: ${total.toFixed?total.toFixed(1):total}</span>`;
        }
        if (todayBreakdown && actual.scores) {
            const s = actual.scores;
            todayBreakdown.innerHTML =
                `KOSPI ${((actual.kospi_change||0)*100).toFixed(1)}% | ` +
                `breadth ${((actual.breadth_ratio||0)*100).toFixed(0)}% | ` +
                `ret=${s.ret_score||0} br=${s.breadth_score||0} flow=${s.flow_score||0} stress=${s.stress_penalty||0}`;
        }
        if (todayDate) todayDate.textContent = actual.market_date || '';
    }

    // ── 내일 예측 ──
    if (!rp) return;

    const predictMarker = document.getElementById('regime-predict-marker');
    const predictDetail = document.getElementById('regime-predict-detail');
    const predictFlag = document.getElementById('regime-predict-flag');
    const conf = document.getElementById('regime-confidence');

    if (rp.unavailable) {
        if (predictDetail) predictDetail.innerHTML = '<span class="regime-unavailable-text">데이터 부족</span>';
        return;
    }

    const level = rp.predicted_regime || 3;
    const composite = rp.composite_score || 0;
    const pct = _regimeToBarPct(level, composite);

    if (predictMarker) predictMarker.style.left = pct + '%';
    if (predictDetail) {
        predictDetail.innerHTML =
            `<span class="regime-detail-label" style="color:${_regimeLabelColor(level)}">${rp.predicted_label||'--'}</span>` +
            `<span class="regime-detail-score">점수: ${composite.toFixed(3)}</span>`;
    }
    if (predictFlag) {
        const f = rp.confidence_flag || '';
        predictFlag.textContent = f;
        predictFlag.className = 'regime-flag ' + (f === 'FULL' ? 'flag-full' : f === 'PARTIAL' ? 'flag-partial' : 'flag-insuf');
    }

    // 5-axis bars
    const axes = ['global', 'vol', 'domestic', 'micro', 'fx'];
    for (const ax of axes) {
        const fill = document.getElementById('axis-' + ax);
        const val = document.getElementById('axis-' + ax + '-val');
        const s = rp[ax + '_score'];
        const avail = rp[ax + '_avail'];
        if (fill && val) {
            if (!avail) {
                fill.style.width = '0%';
                fill.style.background = 'var(--text-dim)';
                val.textContent = 'N/A';
                val.className = 'axis-val unavailable';
            } else {
                const absPct = Math.abs(s || 0) * 50;
                fill.style.width = absPct + '%';
                fill.style.marginLeft = s >= 0 ? '50%' : (50 - absPct) + '%';
                fill.style.background = s >= 0 ? '#F04452' : '#3182F6';
                val.textContent = (s >= 0 ? '+' : '') + (s||0).toFixed(2);
                val.className = 'axis-val ' + (s >= 0 ? 'positive' : 'negative');
            }
        }
    }

    // Confidence
    if (conf) {
        const aw = rp.available_weight || 0;
        const cflag = rp.confidence_flag || '';
        conf.textContent = `데이터 ${(aw*100).toFixed(0)}% 기반 [${cflag}]`;
    }

    // Rolling stats
    const stats = rp.rolling_stats;
    if (stats) {
        const s5 = document.getElementById('stat-5d');
        const s20 = document.getElementById('stat-20d');
        const sExact = document.getElementById('stat-exact');
        const sW1 = document.getElementById('stat-within1');
        if (s5) s5.textContent = stats.avg_confidence_5d != null ? stats.avg_confidence_5d + '%' : '--';
        if (s20) s20.textContent = stats.avg_confidence_20d != null ? stats.avg_confidence_20d + '%' : '--';
        if (sExact) sExact.textContent = stats.exact_match_rate != null ? stats.exact_match_rate + '%' : '--';
        if (sW1) sW1.textContent = stats.within_one_step_rate != null ? stats.within_one_step_rate + '%' : '--';
    }
}
