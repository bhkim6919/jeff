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

    // ── Reset button removed 2026-04-20 per user request ────
    // Previously: #btn-live-reset POST /api/lab/live/reset — too risky
    // (accidental click destroys weeks of accumulated equity_history).
    // API endpoint retained for programmatic access if ever needed.

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
            const period = data.start_date
                ? `${data.start_date} ~ ${data.last_run_date}`
                : `Last: ${data.last_run_date}`;
            document.getElementById('live-last-run').textContent = period;
        }

        // Strategy cards
        renderCards(data.lanes || []);

        // Positions table
        renderPositions(data.lanes || []);

        // Load trades
        loadTrades();

        // Meta Layer (카드 재생성 후 다시 붙이기)
        loadMeta();
    }

    function renderCards(lanes) {
        const container = document.getElementById('live-cards');
        const sorted = [...lanes].sort((a, b) => b.total_return - a.total_return);
        const seen = new Set();

        sorted.forEach(lane => {
            seen.add(lane.name);
            const groupCls = GROUP_CLASS[lane.group] || '';
            const retClass = lane.total_return >= 0 ? 'stlab-val-pos' : 'stlab-val-neg';
            const dayRet = lane.day_return;
            const dayRetStr = dayRet != null
                ? `${dayRet >= 0 ? '+' : ''}${dayRet.toFixed(2)}%`
                : 'N/A';
            const dayRetCls = dayRet > 0 ? 'stlab-val-pos' : dayRet < 0 ? 'stlab-val-neg' : '';
            const bodyHtml = `
                <div class="stlab-card-header">
                    <span class="stlab-card-name">${lane.name}</span>
                    <span class="stlab-group-tag ${groupCls}">${lane.group}</span>
                </div>
                <div class="stlab-card-row">
                    <span>누적</span>
                    <span class="${retClass}" style="font-size:16px;font-weight:700">
                        ${lane.total_return >= 0 ? '+' : ''}${lane.total_return.toFixed(2)}%
                    </span>
                </div>
                <div class="stlab-card-row">
                    <span>일일</span>
                    <span class="${dayRetCls}" style="font-size:13px;font-weight:600">${dayRetStr}</span>
                </div>
                <div class="stlab-card-row"><span>Equity</span><span>${(lane.equity/1e6).toFixed(2)}M</span></div>
                <div class="stlab-card-row"><span>MDD</span><span>${lane.mdd.toFixed(1)}%</span></div>
                <div class="stlab-card-row"><span>Positions</span><span>${lane.n_positions}</span></div>
                <div class="stlab-card-row"><span>Trades</span><span>${lane.n_trades}</span></div>
                <div class="stlab-card-row"><span>Pending</span><span>${lane.n_pending}</span></div>
                <div class="stlab-card-row"><span>Cash</span><span>${(lane.cash/1e6).toFixed(2)}M</span></div>
            `;

            let card = container.querySelector(`.stlab-card[data-strategy="${lane.name}"]`);
            if (!card) {
                card = document.createElement('div');
                card.className = 'stlab-card';
                card.dataset.strategy = lane.name;
                card.dataset.bodyHtml = '';
                // body 영역을 별도 wrapper로 분리 — innerHTML 교체 시 meta-box-slot / expanded-panel 보존
                const bodyWrap = document.createElement('div');
                bodyWrap.className = 'stlab-card-body';
                bodyWrap.innerHTML = bodyHtml;
                card.appendChild(bodyWrap);
                const slot = document.createElement('div');
                slot.className = 'meta-box-slot';
                card.appendChild(slot);
                container.appendChild(card);
                card.dataset.bodyHtml = bodyHtml;
            } else {
                // body만 교체 — meta-box-slot, expanded-panel DOM 보존 (플리커 방지)
                if (card.dataset.bodyHtml !== bodyHtml) {
                    let bodyWrap = card.querySelector('.stlab-card-body');
                    if (!bodyWrap) {
                        bodyWrap = document.createElement('div');
                        bodyWrap.className = 'stlab-card-body';
                        card.insertBefore(bodyWrap, card.firstChild);
                    }
                    bodyWrap.innerHTML = bodyHtml;
                    card.dataset.bodyHtml = bodyHtml;
                }
                // 순서 보정 (appendChild는 기존 노드를 끝으로 이동)
                if (container.lastElementChild !== card || card.previousElementSibling?.dataset?.strategy !== null) {
                    container.appendChild(card);
                }
            }
        });

        // Remove stale cards
        container.querySelectorAll('.stlab-card').forEach(c => {
            if (!seen.has(c.dataset.strategy)) c.remove();
        });
    }

    let _lastMetaJson = null;
    let _metaInFlight = false;
    async function loadMeta() {
        if (_metaInFlight) return;
        _metaInFlight = true;
        try {
            const resp = await fetch('/api/lab/live/meta');
            const meta = await resp.json();
            if (!meta.ok) return;
            const metaJson = JSON.stringify(meta);
            if (metaJson === _lastMetaJson) return;  // skip DOM update — no change
            _lastMetaJson = metaJson;
            renderMarketBar(meta);
            renderRecommendation(meta);
            renderStrategyFit(meta);
        } catch(e) {}
        finally { _metaInFlight = false; }
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
        const html = `
            <span class="meta-market-tags">${tags || '데이터 수집 중'}</span>
            <span class="meta-confidence meta-conf-${conf}">
                신뢰도: ${meta.confidence}${warn} (${days}일)
            </span>
        `;
        if (bar.innerHTML !== html) bar.innerHTML = html;
    }

    function renderRecommendation(meta) {
        const rec = meta.recommendation;
        if (!rec || !rec.top_strategy) return;

        let panel = document.getElementById('meta-recommendation');
        if (!panel) {
            panel = document.createElement('div');
            panel.id = 'meta-recommendation';
            panel.className = 'meta-recommendation';
            const bar = document.getElementById('meta-market-bar');
            if (bar) bar.parentElement.insertBefore(panel, bar.nextSibling);
        }

        const top3Html = (rec.top3 || []).map((s, i) => {
            const badge = i === 0 ? 'meta-high' : 'meta-mid';
            return `<span class="meta-fit-score ${badge}" style="margin-right:6px;">${s}</span>`;
        }).join('');

        const confPct = Math.round((rec.confidence_score || 0) * 100);
        const dqColor = rec.data_quality === 'OK' ? '#00ff88' :
                        rec.data_quality === 'BAD' ? '#ff3344' : '#ffdd00';

        const html = `
            <div style="display:flex;justify-content:space-between;align-items:center;flex-wrap:wrap;gap:8px;">
                <div>
                    <span style="color:var(--text-secondary);font-size:11px;">META 추천</span>
                    <span style="margin-left:8px;">${top3Html}</span>
                </div>
                <div style="display:flex;gap:12px;font-size:11px;">
                    <span style="color:var(--text-secondary);">신뢰도 <b>${confPct}%</b></span>
                    <span style="color:${dqColor};">DQ: ${rec.data_quality}</span>
                    <span style="color:var(--text-secondary);opacity:0.6;">${rec.execution_status}</span>
                </div>
            </div>
        `;
        if (panel.innerHTML !== html) panel.innerHTML = html;
    }

    function renderStrategyFit(meta) {
        document.querySelectorAll('.stlab-card').forEach(card => {
            const key = card.dataset.strategy;
            const fit = meta.strategy_fit?.[key];
            if (!fit) return;

            const displayScore = fit.final_score || fit.score;
            const scoreCls = displayScore === 'HIGH' ? 'meta-high'
                           : displayScore === 'LOW'  ? 'meta-low'
                           : displayScore === 'WARN' ? 'meta-warn'
                           : 'meta-mid';

            const reasons = (fit.reasons || []).map(r => {
                const cls = r.sign === '+' ? 'meta-reason-pos' : 'meta-reason-neg';
                return `<span class="${cls}">${r.sign} ${r.text}</span>`;
            }).join('');

            const hasDrivers = fit.drivers && fit.drivers.ok;
            const hintIcon = hasDrivers ? '<span class="meta-expand-hint" title="클릭하여 일일 분석 펼치기">🔍</span>' : '';

            // Live Promotion 상태 뱃지
            const promo = fit.promotion || null;
            const promoHtml = promo ? buildPromotionBadge(promo) : '';

            const html = `
                <div class="meta-fit-header">
                    <span class="meta-fit-label">적합도</span>
                    <span class="meta-fit-score ${scoreCls}">${displayScore}</span>
                    ${hintIcon}
                </div>
                <div class="meta-reasons">${reasons || '<span style="opacity:0.5">데이터 부족</span>'}</div>
                ${promoHtml}
            `;

            let slot = card.querySelector('.meta-box-slot');
            if (!slot) {
                slot = document.createElement('div');
                slot.className = 'meta-box-slot';
                card.appendChild(slot);
            }
            let box = slot.querySelector('.meta-box');
            if (!box) {
                box = document.createElement('div');
                box.className = 'meta-box';
                slot.appendChild(box);
            }
            if (box.innerHTML !== html) box.innerHTML = html;

            // Daily drivers 캐시 + 확장 토글 바인딩
            if (hasDrivers) {
                card._driversData = fit.drivers;
                bindCardExpand(card);
                // 이미 확장 상태면 재렌더
                if (card.classList.contains('stlab-card-expanded')) {
                    renderExpandedPanel(card, fit.drivers);
                }
            }
        });
    }

    // ── Live Promotion Badge ────────────────────────────────
    function buildPromotionBadge(promo) {
        if (!promo) return '';
        const status = promo.status || 'NOT_READY';
        const score = promo.total_score;
        const blockers = promo.blockers || [];
        const criticalFail = promo.critical_fail;
        const subs = promo.subscores || {};
        const group = promo.group || '?';
        const evMissing = promo.evidence_missing || [];
        const failCat = promo.failures_by_category || {};
        const coverage = promo.evidence_coverage;

        // Status → color mapping
        const statusColors = {
            'BLOCKED':           {bg: 'rgba(255,68,68,0.18)',  fg: '#ff7788', label: '❌ BLOCKED'},
            'NOT_READY':         {bg: 'rgba(160,160,160,0.15)',fg: '#aaa',    label: '⚠ NOT_READY'},
            'CANDIDATE':         {bg: 'rgba(255,200,66,0.15)', fg: '#ffd54f', label: '🟡 CANDIDATE'},
            'PAPER_READY':       {bg: 'rgba(74,163,255,0.18)', fg: '#4aa3ff', label: '📄 PAPER_READY'},
            'SMALL_LIVE_READY':  {bg: 'rgba(0,212,170,0.2)',   fg: '#00d4aa', label: '💰 SMALL_LIVE'},
            'FULL_LIVE_READY':   {bg: 'rgba(0,255,136,0.25)',  fg: '#00ff88', label: '✅ FULL_LIVE'},
        };
        const c = statusColors[status] || statusColors['NOT_READY'];

        // Critical 강조 링 — broker mismatch/duplicate/stale 등
        const criticalRing = criticalFail ? 'box-shadow: 0 0 0 1px rgba(255,85,119,0.6);' : '';

        // Score display (null if hard fail)
        const scoreDisplay = score !== null && score !== undefined
            ? `<span class="promo-score">${score}</span><span class="promo-score-unit">/100</span>`
            : `<span class="promo-score-na">—</span>`;

        // Blocker 분류별 우선순위 표시:
        //   evidence_missing: ...  — 운영 증거 소스 미연결 (⚡)
        //   failure: ...           — 실제 관측된 gate 실패 (⚠ / 📊 / —)
        // Top 2만 칩으로 표시. 나머지는 +N tooltip.
        const categorizedChips = [];
        if (evMissing.length > 0) {
            // 증거 결여는 최우선 — prefix 'evidence_missing:' 로 구분 표시
            for (const f of evMissing.slice(0, 2)) {
                categorizedChips.push({
                    cls: 'promo-chip-evidence',
                    label: `⚡ ${_shortenName(f)}`,
                    title: `evidence_missing: ${f}`,
                });
            }
        }
        if (failCat.ops && failCat.ops.length > 0) {
            for (const f of failCat.ops.slice(0, 1)) {
                if (categorizedChips.length < 2) {
                    categorizedChips.push({
                        cls: 'promo-chip-ops',
                        label: `⚠ ${_shortenName(f)}`,
                        title: `failure (ops): ${f}`,
                    });
                }
            }
        }
        if (failCat.sample && failCat.sample.length > 0) {
            for (const f of failCat.sample.slice(0, 1)) {
                if (categorizedChips.length < 2) {
                    categorizedChips.push({
                        cls: 'promo-chip-sample',
                        label: `📊 ${_shortenName(f)}`,
                        title: `failure (sample): ${f}`,
                    });
                }
            }
        }
        if ((failCat.quality?.length || 0) + (failCat.performance?.length || 0) > 0) {
            const q = (failCat.quality || []).concat(failCat.performance || []);
            for (const f of q.slice(0, 1)) {
                if (categorizedChips.length < 2) {
                    categorizedChips.push({
                        cls: 'promo-chip-other',
                        label: `— ${_shortenName(f)}`,
                        title: `failure: ${f}`,
                    });
                }
            }
        }

        const totalBlockers = blockers.length;
        const shownCount = categorizedChips.length;
        const remaining = Math.max(0, totalBlockers - shownCount);
        const blockersTitle = blockers.join(' | ').replace(/"/g, '&quot;');

        const chipsHtml = categorizedChips.map(c =>
            `<span class="promo-blocker-chip ${c.cls}" title="${c.title.replace(/"/g,'&quot;')}">${c.label}</span>`
        ).join('');

        // Evidence coverage indicator (known 비율)
        let coverageHtml = '';
        if (coverage !== null && coverage !== undefined && coverage < 1.0) {
            const pct = Math.round(coverage * 100);
            const covCls = pct >= 75 ? 'promo-cov-ok' : pct >= 50 ? 'promo-cov-mid' : 'promo-cov-low';
            coverageHtml = `<span class="promo-cov ${covCls}" title="증거 기반 점수 비율 (낮으면 감시자 미연결)">evidence ${pct}%</span>`;
        }

        const blockersHtml = (shownCount > 0 || coverageHtml) ? `
            <div class="promo-blockers" title="${blockersTitle}">
                ${chipsHtml}
                ${remaining > 0 ? `<span class="promo-blocker-more">+${remaining}</span>` : ''}
                ${coverageHtml}
            </div>
        ` : '';

        // Subscores mini bars (4개)
        const subNames = [
            ['performance', 'Perf'],
            ['stability',   'Stab'],
            ['operational', 'Ops'],
            ['cost_realism','Cost'],
        ];
        const subHtml = subNames.map(([k, lbl]) => {
            const v = subs[k] ?? 0;
            const barCls = v >= 75 ? 'promo-sub-bar-hi' : v >= 50 ? 'promo-sub-bar-mid' : 'promo-sub-bar-lo';
            return `
                <div class="promo-sub-row" title="${lbl}: ${v}/100">
                    <span class="promo-sub-lbl">${lbl}</span>
                    <div class="promo-sub-bar-wrap">
                        <div class="promo-sub-bar ${barCls}" style="width:${Math.max(0, Math.min(100, v))}%"></div>
                    </div>
                    <span class="promo-sub-val">${Math.round(v)}</span>
                </div>
            `;
        }).join('');

        return `
            <div class="promo-box" style="background:${c.bg};${criticalRing}">
                <div class="promo-header">
                    <span class="promo-status" style="color:${c.fg}">${c.label}</span>
                    <span class="promo-score-wrap">${scoreDisplay}</span>
                </div>
                <div class="promo-group">그룹: <b>${group}</b></div>
                <div class="promo-subs">${subHtml}</div>
                ${blockersHtml}
            </div>
        `;
    }

    function _shortenBlocker(s) {
        // "[CRITICAL] sample_days:4 < 60" → "sample_days 4<60"
        let clean = s.replace(/^\[CRITICAL\]\s*/, '').replace(/\s+/g, ' ').trim();
        if (clean.length > 24) clean = clean.slice(0, 22) + '…';
        return clean;
    }

    function _shortenName(s) {
        // gate name 축약: "unresolved_broker_mismatch" → "broker_mismatch", "sample_days" → "sample"
        const short = {
            'sample_days': 'sample',
            'total_trades': 'trades',
            'rebal_cycles': 'cycles',
            'regime_coverage': 'regime_cov',
            'regime_flip_observed': 'regime_flip',
            'recon_ok_streak': 'recon',
            'unresolved_broker_mismatch': 'broker_mismatch',
            'duplicate_execution_count': 'duplicate_exec',
            'stale_decision_input_count': 'stale_input',
            'dirty_exit_recovery_fail_count': 'dirty_exit',
            'pending_external_stale_cleanup_fail_count': 'pending_cleanup',
            'state_uncertain_days_recent': 'state_uncertain',
            'mdd_pct_floor': 'mdd',
            'sharpe_floor': 'sharpe',
            'cost_drag_ceiling': 'cost_drag',
            'kospi_stale_days': 'kospi_stale',
            'ohlcv_sync_status': 'ohlcv_sync',
        };
        return short[s] || s;
    }

    // ── Card Expand / Collapse ──────────────────────────────
    function bindCardExpand(card) {
        if (card._expandBound) return;
        card._expandBound = true;
        card.addEventListener('click', (ev) => {
            // 내부 interactive 요소 클릭은 expand 토글 제외
            const tag = ev.target.tagName;
            if (tag === 'A' || tag === 'BUTTON' || tag === 'SELECT' || tag === 'INPUT') return;
            if (ev.target.closest('.stlab-expanded-panel')) {
                // 패널 내부 클릭은 외부 닫힘 방지
                if (ev.target.closest('.stlab-expanded-close')) {
                    toggleCardExpand(card, false);
                }
                return;
            }
            const isExpanded = card.classList.contains('stlab-card-expanded');
            toggleCardExpand(card, !isExpanded);
        });
    }

    function toggleCardExpand(card, expand) {
        if (expand) {
            // 다른 카드 닫기 (single-open 정책)
            document.querySelectorAll('.stlab-card.stlab-card-expanded').forEach(c => {
                if (c !== card) {
                    c.classList.remove('stlab-card-expanded');
                    const p = c.querySelector('.stlab-expanded-panel');
                    if (p) p.remove();
                }
            });
            card.classList.add('stlab-card-expanded');
            renderExpandedPanel(card, card._driversData);
        } else {
            card.classList.remove('stlab-card-expanded');
            const p = card.querySelector('.stlab-expanded-panel');
            if (p) p.remove();
        }
    }

    function renderExpandedPanel(card, drv) {
        if (!drv || !drv.ok) return;
        // 데이터 동일 시 재렌더 skip (플리커 방지)
        const drvHash = JSON.stringify(drv);
        if (card._lastDrvHash === drvHash && card.querySelector('.stlab-expanded-panel')) {
            return;
        }
        card._lastDrvHash = drvHash;

        let panel = card.querySelector('.stlab-expanded-panel');
        if (!panel) {
            panel = document.createElement('div');
            panel.className = 'stlab-expanded-panel';
            card.appendChild(panel);
        }

        // 1. Market block
        const m = drv.market || {};
        const kospiPct = m.kospi_day_pct;
        const kospiCls = kospiPct > 0 ? 'stlab-val-pos' : kospiPct < 0 ? 'stlab-val-neg' : '';
        const kospiPctStr = kospiPct != null
            ? `${kospiPct >= 0 ? '+' : ''}${kospiPct.toFixed(2)}%`
            : 'N/A';

        // 2. Strategy block
        const s = drv.strategy || {};
        const dayPct = s.day_pct;
        const cumPct = s.cumul_pct;
        const deltaKospi = s.delta_vs_kospi;
        const dayCls = dayPct > 0 ? 'stlab-val-pos' : dayPct < 0 ? 'stlab-val-neg' : '';
        const cumCls = cumPct > 0 ? 'stlab-val-pos' : cumPct < 0 ? 'stlab-val-neg' : '';
        const deltaCls = deltaKospi > 0 ? 'stlab-val-pos' : deltaKospi < 0 ? 'stlab-val-neg' : '';
        const deltaStr = deltaKospi != null
            ? `${deltaKospi >= 0 ? '+' : ''}${deltaKospi.toFixed(2)}%p`
            : 'N/A';

        // 3. Sparkline SVG (equity curve, KOSPI overlay)
        const sparklineHtml = buildSparklineSVG(s.equity_series || [], m.kospi_series || []);

        // 4. Top contributors bar chart
        const contribHtml = buildContributorsHtml(drv.top_contributors || []);

        // 5. Sector donut
        const sectorHtml = buildSectorDonutHtml(drv.sectors || []);

        panel.innerHTML = `
            <div class="stlab-expanded-header">
                <span class="stlab-expanded-title">🔍 일일 분석</span>
                <button class="stlab-expanded-close" title="닫기">×</button>
            </div>
            <div class="stlab-expanded-grid">
                <div class="stlab-expanded-section">
                    <div class="stlab-expanded-section-title">시장 (KOSPI)</div>
                    <div class="stlab-expanded-kv">
                        <span>종가</span><b>${m.kospi_close ? m.kospi_close.toLocaleString() : 'N/A'}</b>
                    </div>
                    <div class="stlab-expanded-kv">
                        <span>일간</span>
                        <b class="${kospiCls}">${kospiPctStr}</b>
                    </div>
                    <div class="stlab-expanded-regime">${m.regime_hint || '-'}</div>
                </div>
                <div class="stlab-expanded-section">
                    <div class="stlab-expanded-section-title">전략 성과</div>
                    <div class="stlab-expanded-kv">
                        <span>누적</span>
                        <b class="${cumCls}">${cumPct != null ? (cumPct>=0?'+':'') + cumPct.toFixed(2) + '%' : 'N/A'}</b>
                    </div>
                    <div class="stlab-expanded-kv">
                        <span>일간</span>
                        <b class="${dayCls}">${dayPct != null ? (dayPct>=0?'+':'') + dayPct.toFixed(2) + '%' : 'N/A'}</b>
                    </div>
                    <div class="stlab-expanded-kv">
                        <span>vs KOSPI</span>
                        <b class="${deltaCls}">${deltaStr}</b>
                    </div>
                </div>
                <div class="stlab-expanded-section stlab-expanded-chart">
                    <div class="stlab-expanded-section-title">Equity 추이 (KOSPI overlay)</div>
                    ${sparklineHtml}
                </div>
                <div class="stlab-expanded-section stlab-expanded-wide">
                    <div class="stlab-expanded-section-title">Top 기여 종목 (일간 기준)</div>
                    ${contribHtml}
                </div>
                <div class="stlab-expanded-section stlab-expanded-wide">
                    <div class="stlab-expanded-section-title">섹터 비중</div>
                    ${sectorHtml}
                </div>
            </div>
        `;
    }

    // ── Sparkline SVG: equity (전략) + KOSPI overlay ──
    function buildSparklineSVG(equitySeries, kospiSeries) {
        if (!equitySeries.length) return '<div style="opacity:0.5">데이터 부족</div>';
        const W = 300, H = 80, PAD = 6;
        // normalize: equity return_pct [-X%, +Y%] → (PAD ~ H-PAD)
        const eqReturns = equitySeries.map(e => e.return_pct);
        const kospiReturns = [];
        if (kospiSeries.length > 1) {
            const base = kospiSeries[0].close;
            kospiSeries.forEach(k => {
                kospiReturns.push((k.close / base - 1) * 100);
            });
        }
        const all = eqReturns.concat(kospiReturns);
        const minV = Math.min(...all), maxV = Math.max(...all);
        const range = maxV - minV || 1;

        function toPath(values, totalW) {
            if (!values.length) return '';
            const step = totalW / Math.max(1, values.length - 1);
            return values.map((v, i) => {
                const x = PAD + i * (step - (2 * PAD / Math.max(1, values.length - 1)));
                const y = H - PAD - ((v - minV) / range) * (H - 2 * PAD);
                return `${i === 0 ? 'M' : 'L'}${x.toFixed(1)},${y.toFixed(1)}`;
            }).join(' ');
        }

        const eqPath = toPath(eqReturns, W);
        const kospiPath = toPath(kospiReturns, W);
        const zeroY = H - PAD - ((0 - minV) / range) * (H - 2 * PAD);

        return `
            <svg viewBox="0 0 ${W} ${H}" width="100%" height="${H}" style="display:block;">
                <line x1="${PAD}" y1="${zeroY}" x2="${W - PAD}" y2="${zeroY}"
                      stroke="rgba(255,255,255,0.15)" stroke-dasharray="2,2"/>
                ${kospiPath ? `<path d="${kospiPath}" fill="none" stroke="#888" stroke-width="1.5" stroke-dasharray="3,2" opacity="0.7"/>` : ''}
                <path d="${eqPath}" fill="none" stroke="#4aa3ff" stroke-width="2"/>
                <text x="${W - PAD}" y="12" text-anchor="end" fill="#4aa3ff" font-size="10">전략 ${eqReturns[eqReturns.length-1]?.toFixed(1)}%</text>
                ${kospiPath ? `<text x="${W - PAD}" y="24" text-anchor="end" fill="#888" font-size="10">KOSPI ${kospiReturns[kospiReturns.length-1]?.toFixed(1)}%</text>` : ''}
            </svg>
        `;
    }

    // ── Contributors bar chart ──
    function buildContributorsHtml(contributors) {
        if (!contributors.length) return '<div style="opacity:0.5">데이터 부족</div>';
        const maxAbs = Math.max(...contributors.map(c => Math.abs(c.daily_amount || 0))) || 1;

        return contributors.map(c => {
            const isPos = (c.daily_amount || 0) >= 0;
            const pctOfMax = Math.abs(c.daily_amount || 0) / maxAbs * 100;
            const color = isPos ? '#00d4aa' : '#ff5577';
            const dailyPctStr = c.daily_pct != null
                ? `${c.daily_pct >= 0 ? '+' : ''}${c.daily_pct.toFixed(2)}%`
                : 'N/A';
            const amtStr = c.daily_amount != null
                ? `${c.daily_amount >= 0 ? '+' : ''}${Math.round(c.daily_amount).toLocaleString()}`
                : '-';
            const baselineLabel = c.is_new_today
                ? '<span class="stlab-baseline-tag stlab-baseline-new" title="오늘 진입 — entry_price 대비">신규</span>'
                : '<span class="stlab-baseline-tag stlab-baseline-hold" title="기존 보유 — 전일 종가 대비">보유</span>';
            return `
                <div class="stlab-contrib-row">
                    <div class="stlab-contrib-name">
                        <span class="stlab-contrib-code">${c.code}</span>
                        <span>${c.name}${baselineLabel}</span>
                    </div>
                    <div class="stlab-contrib-bar-wrap">
                        <div class="stlab-contrib-bar" style="width:${pctOfMax.toFixed(1)}%;background:${color};"></div>
                    </div>
                    <div class="stlab-contrib-val">
                        <b style="color:${color}">${dailyPctStr}</b>
                        <small>${amtStr}원</small>
                    </div>
                </div>
            `;
        }).join('');
    }

    // ── Sector donut ──
    function buildSectorDonutHtml(sectors) {
        if (!sectors.length) return '<div style="opacity:0.5">데이터 부족</div>';
        const R = 40, C = 50, STROKE = 14;
        const CIRC = 2 * Math.PI * R;
        const colors = ['#4aa3ff','#00d4aa','#ffaa4a','#ff5577','#a465f5','#ffd633','#66d9ef','#f0938c'];
        let offset = 0;

        const arcs = sectors.map((s, i) => {
            const frac = Math.max(0, s.weight) / 100;
            const dash = frac * CIRC;
            const el = `<circle cx="${C}" cy="${C}" r="${R}" fill="none"
                        stroke="${colors[i % colors.length]}" stroke-width="${STROKE}"
                        stroke-dasharray="${dash.toFixed(1)} ${CIRC.toFixed(1)}"
                        stroke-dashoffset="${(-offset).toFixed(1)}"
                        transform="rotate(-90 ${C} ${C})"/>`;
            offset += dash;
            return el;
        }).join('');

        const legend = sectors.slice(0, 8).map((s, i) => `
            <div class="stlab-sector-legend-row">
                <span class="stlab-sector-swatch" style="background:${colors[i % colors.length]}"></span>
                <span class="stlab-sector-name">${s.sector}</span>
                <span class="stlab-sector-weight">${s.weight.toFixed(1)}%</span>
                <span class="stlab-sector-count">(${s.count})</span>
            </div>
        `).join('');

        return `
            <div class="stlab-sector-wrap">
                <svg viewBox="0 0 100 100" width="100" height="100" class="stlab-sector-donut">
                    ${arcs}
                </svg>
                <div class="stlab-sector-legend">${legend}</div>
            </div>
        `;
    }

    let _posFilterBound = false;
    let _lastAllPos = [];

    function renderPositions(lanes) {
        const tbody = document.getElementById('live-pos-tbody');
        const section = document.getElementById('live-positions-section');
        const filterSel = document.getElementById('live-pos-strategy-filter');
        const countEl = document.getElementById('live-pos-count');
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
        _lastAllPos = allPos;

        // Strategy filter dropdown: sync options with current lanes
        if (filterSel) {
            const current = filterSel.value || '__ALL__';
            const strategies = [...new Set(allPos.map(p => p.strategy))].sort();
            const desired = ['__ALL__', ...strategies];
            const existing = Array.from(filterSel.options).map(o => o.value);
            if (JSON.stringify(existing) !== JSON.stringify(desired)) {
                filterSel.innerHTML = '';
                desired.forEach(s => {
                    const opt = document.createElement('option');
                    opt.value = s;
                    opt.textContent = s === '__ALL__' ? '전체' : s;
                    filterSel.appendChild(opt);
                });
                filterSel.value = strategies.includes(current) || current === '__ALL__' ? current : '__ALL__';
            }
            if (!_posFilterBound) {
                filterSel.addEventListener('change', () => {
                    renderPositionRows(_lastAllPos);
                    renderTradeRows(_lastTrades);
                });
                _posFilterBound = true;
            }
        }

        renderPositionRows(allPos);
    }

    function renderPositionRows(allPos) {
        const tbody = document.getElementById('live-pos-tbody');
        const filterSel = document.getElementById('live-pos-strategy-filter');
        const countEl = document.getElementById('live-pos-count');
        const selected = filterSel?.value || '__ALL__';
        const filtered = selected === '__ALL__' ? allPos : allPos.filter(p => p.strategy === selected);

        tbody.innerHTML = '';
        if (countEl) {
            countEl.textContent = selected === '__ALL__'
                ? `${filtered.length}종목 (전 전략)`
                : `${filtered.length}종목 / ${selected}`;
        }

        filtered.sort((a, b) => b.pnl_pct - a.pnl_pct);

        filtered.forEach(pos => {
            const tr = document.createElement('tr');
            const pnlCls = pos.pnl_pct >= 0 ? 'stlab-val-pos' : 'stlab-val-neg';
            const dayChg = pos.day_change_pct;
            const dayChgStr = dayChg != null ? `${dayChg >= 0 ? '+' : ''}${dayChg.toFixed(2)}%` : 'N/A';
            const dayCls = dayChg > 0 ? 'stlab-val-pos' : dayChg < 0 ? 'stlab-val-neg' : '';
            const contrib = pos.contrib_pct;
            const contribCls = contrib >= 0 ? 'stlab-val-pos' : 'stlab-val-neg';
            const contribStr = contrib != null
                ? `${contrib >= 0 ? '+' : ''}${contrib.toFixed(2)}%p`
                : '-';
            const weight = pos.weight_pct != null ? `${pos.weight_pct.toFixed(1)}%` : '-';
            tr.innerHTML = `
                <td>${pos.strategy}</td>
                <td>${pos.code}</td>
                <td>${pos.name}</td>
                <td>${pos.qty}</td>
                <td>${pos.entry_price?.toLocaleString()}</td>
                <td>${pos.current_price?.toLocaleString()}</td>
                <td class="${dayCls}">${dayChgStr}</td>
                <td class="${pnlCls}">${pos.pnl_pct >= 0 ? '+' : ''}${pos.pnl_pct}%</td>
                <td class="${pnlCls}">${pos.pnl_amount?.toLocaleString()}</td>
                <td>${weight}</td>
                <td class="${contribCls}">${contribStr}</td>
                <td>${pos.entry_date}</td>
            `;
            tbody.appendChild(tr);
        });
    }

    let _lastTrades = [];

    async function loadTrades() {
        try {
            const resp = await fetch('/api/lab/live/trades?limit=30');
            const data = await resp.json();
            _lastTrades = data.trades || [];
            renderTradeRows(_lastTrades);
        } catch (e) {}
    }

    function renderTradeRows(trades) {
        const section = document.getElementById('live-trades-section');
        const tbody = document.getElementById('live-trades-tbody');
        const filterSel = document.getElementById('live-pos-strategy-filter');
        const selected = filterSel?.value || '__ALL__';

        const filtered = selected === '__ALL__'
            ? trades
            : trades.filter(t => (t.strategy || '') === selected);

        if (filtered.length === 0) {
            section.style.display = 'none';
            return;
        }
        section.style.display = '';
        tbody.innerHTML = '';

        // 원본 순서 보존: slice로 복사 후 reverse (최신이 위로)
        filtered.slice().reverse().forEach(t => {
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
    }

    // ── Auto-load on tab switch ─────────────────────────────
    document.querySelector('[data-tab="live-tab"]')?.addEventListener('click', () => {
        loadState();
    });

})();
