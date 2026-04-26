/**
 * shared/web/static/components/qc-actions.js
 * ===========================================
 * Cross-market UX action bundle. Lazy-injects its own DOM (toast root,
 * drawer container, help overlay) so any page that includes this script
 * gets the action layer without template changes.
 *
 *   window.qcToast.show(kind, title, body)       — kind: success|info|warn|err
 *   window.qcDrawer.open(payload)                — payload from rebal preview row
 *   window.qcDrawer.close()
 *   window.qcShortcuts.toggleHelp()
 *
 * Keyboard shortcuts (active when no input/textarea is focused):
 *   ?    — toggle help overlay
 *   Esc  — close drawer / help
 *   r    — refresh rebal preview (loadRebalPreview if defined)
 *   t    — cycle theme (window.QtronTheme.cycle if defined)
 *   g d  — go dashboard
 *   g l  — go lab
 */
(function () {
    'use strict';

    // ── Inject styles only once ───────────────────────────────
    function ensureStyles() {
        if (document.getElementById('qc-actions-styles')) return;
        const style = document.createElement('style');
        style.id = 'qc-actions-styles';
        style.textContent = `
        .qc-toast-root {
            position: fixed; top: 70px; right: 16px;
            display: flex; flex-direction: column; gap: 8px;
            z-index: 9000; pointer-events: none;
        }
        .qc-toast {
            background: var(--bg-card, #111827);
            border: 1px solid var(--border, #1f2937);
            border-left: 3px solid var(--accent, #3b82f6);
            border-radius: 6px;
            padding: 10px 14px;
            min-width: 260px; max-width: 360px;
            font-size: 13px;
            color: var(--text, #e5e7eb);
            box-shadow: 0 8px 24px rgba(0,0,0,0.35);
            animation: qc-toast-in 0.28s cubic-bezier(0.16,1,0.3,1);
            pointer-events: auto;
        }
        .qc-toast.success { border-left-color: #10b981; }
        .qc-toast.warn    { border-left-color: #f59e0b; }
        .qc-toast.err     { border-left-color: #ef4444; }
        .qc-toast.fade    { animation: qc-toast-out 0.22s cubic-bezier(0.16,1,0.3,1) forwards; }
        .qc-toast .qc-t-title { font-weight: 600; margin-bottom: 2px; }
        .qc-toast .qc-t-body  { color: var(--text-secondary, #9ca3af); font-size: 12px; }
        @keyframes qc-toast-in  { from { transform: translateX(20px); opacity: 0; } to { transform: translateX(0); opacity: 1; } }
        @keyframes qc-toast-out { to   { transform: translateX(20px); opacity: 0; } }

        .qc-drawer-overlay {
            position: fixed; inset: 0;
            background: rgba(0,0,0,0.5);
            opacity: 0; pointer-events: none;
            transition: opacity 0.28s cubic-bezier(0.16,1,0.3,1);
            z-index: 9100;
        }
        .qc-drawer-overlay.open { opacity: 1; pointer-events: auto; }
        .qc-drawer {
            position: fixed; top: 0; right: 0; bottom: 0;
            width: 420px; max-width: 90vw;
            background: var(--bg-card, #111827);
            border-left: 1px solid var(--border, #1f2937);
            transform: translateX(100%);
            transition: transform 0.28s cubic-bezier(0.16,1,0.3,1);
            z-index: 9110;
            display: flex; flex-direction: column;
            box-shadow: -8px 0 32px rgba(0,0,0,0.4);
            color: var(--text, #e5e7eb);
        }
        .qc-drawer.open { transform: translateX(0); }
        .qc-drawer-head {
            padding: 14px 18px; border-bottom: 1px solid var(--border, #1f2937);
            display: flex; align-items: center; gap: 10px;
        }
        .qc-drawer-head h3 { margin: 0; font-size: 15px; font-weight: 600; flex: 1; }
        .qc-drawer-head .qc-close {
            background: transparent; border: 1px solid var(--border, #30363d);
            color: var(--text-secondary, #9ca3af); cursor: pointer;
            border-radius: 4px; padding: 2px 9px; font-size: 16px; line-height: 1;
        }
        .qc-drawer-head .qc-close:hover { color: var(--text, #e5e7eb); }
        .qc-drawer-body { padding: 16px 18px; overflow-y: auto; flex: 1; font-size: 13px; }
        .qc-drawer-body .qc-row {
            display: flex; justify-content: space-between;
            padding: 6px 0; border-bottom: 1px solid var(--border, #1f2937);
        }
        .qc-drawer-body .qc-row .k { color: var(--text-muted, #6b7280); }
        .qc-drawer-body .qc-row .v { font-variant-numeric: tabular-nums; }
        .qc-drawer-body .v.gain { color: #ef5350; }
        .qc-drawer-body .v.loss { color: #42a5f5; }
        .qc-drawer-spark { width: 100%; height: 56px; margin-bottom: 10px; }

        .qc-help-overlay {
            position: fixed; inset: 0;
            background: rgba(0,0,0,0.7);
            display: flex; align-items: center; justify-content: center;
            opacity: 0; pointer-events: none;
            transition: opacity 0.22s ease;
            z-index: 9200; backdrop-filter: blur(4px);
        }
        .qc-help-overlay.open { opacity: 1; pointer-events: auto; }
        .qc-help-panel {
            background: var(--bg-card, #111827);
            border: 1px solid var(--border, #30363d);
            border-radius: 10px; padding: 22px 26px; min-width: 320px;
            transform: scale(0.96); transition: transform 0.22s cubic-bezier(0.16,1,0.3,1);
            color: var(--text, #e5e7eb);
        }
        .qc-help-overlay.open .qc-help-panel { transform: scale(1); }
        .qc-help-panel h3 { margin: 0 0 12px; font-size: 13px; }
        .qc-help-panel .qc-row {
            display: flex; justify-content: space-between;
            padding: 5px 0; font-size: 12px;
            color: var(--text-secondary, #9ca3af);
        }
        .qc-kbd {
            display: inline-block;
            background: var(--bg-hover, #1f2937);
            border: 1px solid var(--border, #30363d);
            border-bottom-width: 2px;
            border-radius: 3px;
            padding: 1px 5px; margin: 0 2px;
            font-family: ui-monospace, 'SF Mono', monospace;
            font-size: 10.5px; color: var(--text, #e5e7eb);
        }

        @media (prefers-reduced-motion: reduce) {
            .qc-toast, .qc-drawer, .qc-drawer-overlay,
            .qc-help-overlay, .qc-help-panel {
                animation: none !important; transition: none !important;
            }
        }
        `;
        document.head.appendChild(style);
    }

    // ── Toast ─────────────────────────────────────────────────
    function ensureToastRoot() {
        let r = document.getElementById('qc-toast-root');
        if (!r) {
            r = document.createElement('div');
            r.id = 'qc-toast-root';
            r.className = 'qc-toast-root';
            document.body.appendChild(r);
        }
        return r;
    }
    // Toast ring buffer — keeps last N events in memory so the top
    // status pill bar can show "N ALERTS" count over a sliding window.
    const _toastLog = [];     // {ts, kind, title, body, scrollTarget}
    const _TOAST_MAX = 50;

    const qcToast = {
        show(kind, title, body, opts) {
            ensureStyles();
            opts = opts || {};
            const root = ensureToastRoot();
            const el = document.createElement('div');
            el.className = 'qc-toast ' + (kind || 'info');
            el.innerHTML = `<div class="qc-t-title"></div><div class="qc-t-body"></div>`;
            el.querySelector('.qc-t-title').textContent = title || '';
            el.querySelector('.qc-t-body').textContent  = body  || '';

            // Click → scroll to a related card + brief highlight pulse
            const scrollTarget = (typeof opts === 'string') ? opts : opts.scrollTarget;
            if (scrollTarget) {
                el.style.cursor = 'pointer';
                el.title = '클릭 → 관련 카드로 이동';
                el.addEventListener('click', () => {
                    const t = document.getElementById(scrollTarget);
                    if (t) {
                        t.scrollIntoView({ behavior: 'smooth', block: 'center' });
                        t.classList.add('qc-highlight-pulse');
                        setTimeout(() => t.classList.remove('qc-highlight-pulse'), 2000);
                    }
                    el.classList.add('fade');
                    setTimeout(() => el.remove(), 240);
                });
            }

            root.appendChild(el);
            const ttl = (typeof opts === 'object' && opts.ttl) ? opts.ttl : 3200;
            setTimeout(() => {
                el.classList.add('fade');
                setTimeout(() => el.remove(), 240);
            }, ttl);

            // Append to ring buffer (for recentCount)
            _toastLog.push({ ts: Date.now(), kind, title, body, scrollTarget: scrollTarget || null });
            if (_toastLog.length > _TOAST_MAX) _toastLog.shift();
        },

        // Count of toasts fired within the last `windowMs` (default: 10min)
        recentCount(windowMs) {
            const w = (windowMs == null) ? (10 * 60 * 1000) : windowMs;
            const cutoff = Date.now() - w;
            let n = 0;
            for (let i = _toastLog.length - 1; i >= 0; i--) {
                if (_toastLog[i].ts >= cutoff) n++;
                else break;
            }
            return n;
        },

        // Read-only snapshot of the ring buffer (newest last)
        recent(windowMs) {
            const w = (windowMs == null) ? (10 * 60 * 1000) : windowMs;
            const cutoff = Date.now() - w;
            return _toastLog.filter(e => e.ts >= cutoff).slice();
        },
    };

    // ── Drawer ────────────────────────────────────────────────
    function ensureDrawer() {
        let ov = document.getElementById('qc-drawer-overlay');
        if (ov) return ov;
        ensureStyles();
        ov = document.createElement('div');
        ov.id = 'qc-drawer-overlay';
        ov.className = 'qc-drawer-overlay';
        ov.addEventListener('click', () => qcDrawer.close());
        document.body.appendChild(ov);

        const dr = document.createElement('aside');
        dr.id = 'qc-drawer';
        dr.className = 'qc-drawer';
        dr.innerHTML = `
            <div class="qc-drawer-head">
                <h3 id="qc-d-title">--</h3>
                <button class="qc-close" type="button">×</button>
            </div>
            <div class="qc-drawer-body" id="qc-d-body"></div>
        `;
        dr.querySelector('.qc-close').addEventListener('click', () => qcDrawer.close());
        document.body.appendChild(dr);
        return ov;
    }
    function sparkSvg(series, w, h) {
        if (!series || series.length < 2) return '';
        w = w || 360; h = h || 56;
        const min = Math.min(...series), max = Math.max(...series);
        const span = (max - min) || 1;
        const step = w / (series.length - 1);
        const pts = series.map((v, i) =>
            `${(i * step).toFixed(1)},${(h - ((v - min) / span) * h).toFixed(1)}`
        ).join(' ');
        // Color matches today's direction (last segment) so visual aligns
        // with the change_pct shown in the same drawer.
        const last = series[series.length - 1];
        const prev = series[series.length - 2];
        const stroke = last >= prev ? '#ef5350' : '#42a5f5';
        return `<svg class="qc-drawer-spark" viewBox="0 0 ${w} ${h}" preserveAspectRatio="none">
            <polyline fill="none" stroke="${stroke}" stroke-width="1.6" points="${pts}" />
        </svg>`;
    }
    const qcDrawer = {
        open(s) {
            ensureDrawer();
            const title = document.getElementById('qc-d-title');
            const body  = document.getElementById('qc-d-body');
            title.textContent = `${s.code || '?'} · ${s.name || ''}`;
            const cls   = (s.change_pct || 0) >= 0 ? 'gain' : 'loss';
            const sign  = (s.change_pct || 0) >= 0 ? '+' : '';
            body.innerHTML = `
                ${sparkSvg(s.spark)}
                <div class="qc-row"><span class="k">시가</span><span class="v">${(s.open||0).toLocaleString()}</span></div>
                <div class="qc-row"><span class="k">현재가</span><span class="v">${(s.close||0).toLocaleString()}</span></div>
                <div class="qc-row"><span class="k">등락률</span><span class="v ${cls}">${sign}${(s.change_pct||0).toFixed(2)}%</span></div>
                <div class="qc-row"><span class="k">섹터</span><span class="v">${s.sector || '--'}</span></div>
                <div class="qc-row"><span class="k">Mom 12-1</span><span class="v">${s.mom != null ? s.mom : '--'}</span></div>
                <div class="qc-row"><span class="k">Vol 12m</span><span class="v">${s.vol != null ? s.vol : '--'}</span></div>
            `;
            document.getElementById('qc-drawer-overlay').classList.add('open');
            document.getElementById('qc-drawer').classList.add('open');
        },
        close() {
            const ov = document.getElementById('qc-drawer-overlay');
            const dr = document.getElementById('qc-drawer');
            if (ov) ov.classList.remove('open');
            if (dr) dr.classList.remove('open');
        },
    };

    // ── Help overlay ──────────────────────────────────────────
    function ensureHelp() {
        let h = document.getElementById('qc-help-overlay');
        if (h) return h;
        ensureStyles();
        h = document.createElement('div');
        h.id = 'qc-help-overlay';
        h.className = 'qc-help-overlay';
        h.innerHTML = `
            <div class="qc-help-panel">
                <h3>Keyboard Shortcuts</h3>
                <div class="qc-row"><span>도움말 토글</span><span><span class="qc-kbd">?</span></span></div>
                <div class="qc-row"><span>Drawer / 도움말 닫기</span><span><span class="qc-kbd">Esc</span></span></div>
                <div class="qc-row"><span>Target Portfolio 새로고침</span><span><span class="qc-kbd">r</span></span></div>
                <div class="qc-row"><span>테마 순환</span><span><span class="qc-kbd">t</span></span></div>
                <div class="qc-row"><span>대시보드 이동</span><span><span class="qc-kbd">g</span><span class="qc-kbd">d</span></span></div>
                <div class="qc-row"><span>Lab 이동</span><span><span class="qc-kbd">g</span><span class="qc-kbd">l</span></span></div>
            </div>
        `;
        h.addEventListener('click', (e) => {
            if (e.target.id === 'qc-help-overlay') qcShortcuts.toggleHelp();
        });
        document.body.appendChild(h);
        return h;
    }
    const qcShortcuts = {
        toggleHelp() {
            ensureHelp();
            document.getElementById('qc-help-overlay').classList.toggle('open');
        },
    };

    // ── Keyboard handling ─────────────────────────────────────
    let _gPressed = false;
    let _gTimer = null;
    document.addEventListener('keydown', (e) => {
        const tag = (e.target && e.target.tagName) || '';
        if (tag === 'INPUT' || tag === 'TEXTAREA' || tag === 'SELECT') return;
        if (e.target && e.target.isContentEditable) return;

        // Two-key chord: g d / g l
        if (_gPressed) {
            _gPressed = false;
            clearTimeout(_gTimer);
            if (e.key === 'd') { window.location.href = '/'; return; }
            if (e.key === 'l') { window.location.href = '/lab'; return; }
        }
        if (e.key === 'g') {
            _gPressed = true;
            _gTimer = setTimeout(() => { _gPressed = false; }, 800);
            return;
        }

        switch (e.key) {
            case '?':
                qcShortcuts.toggleHelp();
                break;
            case 'Escape':
                qcDrawer.close();
                const h = document.getElementById('qc-help-overlay');
                if (h) h.classList.remove('open');
                break;
            case 'r':
                if (typeof window.loadRebalPreview === 'function') {
                    window.loadRebalPreview();
                    qcToast.show('info', 'Refresh', 'Target Portfolio 다시 로드');
                }
                break;
            case 't':
                if (window.QtronTheme && typeof window.QtronTheme.cycle === 'function') {
                    window.QtronTheme.cycle();
                }
                break;
        }
    });

    // ── Public API ────────────────────────────────────────────
    window.qcToast = qcToast;
    window.qcDrawer = qcDrawer;
    window.qcShortcuts = qcShortcuts;

    // Auto-init on DOM ready
    if (document.readyState === 'loading') {
        document.addEventListener('DOMContentLoaded', ensureStyles);
    } else {
        ensureStyles();
    }
})();
