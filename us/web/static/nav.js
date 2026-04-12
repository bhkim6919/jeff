/**
 * nav.js — Q-TRON Unified Navigation
 * ====================================
 * Market is global state (KR/US), stored in localStorage.
 * Menu is function-only (Dashboard/Lab/Unified).
 * Market toggle redirects to the correct server.
 */
(function() {
    'use strict';

    const KR_BASE = 'http://localhost:8080';
    const US_BASE = 'http://localhost:8081';

    // Route map: {function: {KR: url, US: url}}
    const ROUTES = {
        dashboard: { KR: KR_BASE + '/',        US: US_BASE + '/' },
        lab:       { KR: KR_BASE + '/lab',     US: US_BASE + '/lab' },
        unified:   { KR: KR_BASE + '/unified', US: KR_BASE + '/unified' },
    };

    function getCurrentMarket() {
        // Auto-detect from current port (truth > localStorage)
        const port = window.location.port;
        if (port === '8081') return 'US';
        if (port === '8080') {
            // On KR server: check if unified (market-neutral)
            if (window.location.pathname.includes('/unified')) {
                return localStorage.getItem('qtron_market') || 'KR';
            }
            return 'KR';
        }
        return localStorage.getItem('qtron_market') || 'KR';
    }

    function setMarket(market) {
        localStorage.setItem('qtron_market', market);
    }

    function getCurrentPage() {
        const path = window.location.pathname;
        if (path.includes('/lab')) return 'lab';
        if (path.includes('/unified')) return 'unified';
        return 'dashboard';
    }

    function switchMarket(market) {
        setMarket(market);
        const page = getCurrentPage();
        const url = ROUTES[page]?.[market] || ROUTES.dashboard[market];
        window.location.href = url;
    }

    function navigateTo(page) {
        const market = getCurrentMarket();
        const url = ROUTES[page]?.[market] || ROUTES.dashboard[market];
        window.location.href = url;
    }

    // Build nav header HTML
    function renderNav(containerEl) {
        const market = getCurrentMarket();
        const page = getCurrentPage();

        containerEl.innerHTML = `
            <div class="qnav">
                <div class="qnav-left">
                    <span class="qnav-logo">Q-TRON</span>
                    <div class="qnav-market-toggle">
                        <button class="qnav-market ${market === 'KR' ? 'active' : ''}"
                                onclick="window.__qtronNav.switchMarket('KR')">KR</button>
                        <button class="qnav-market ${market === 'US' ? 'active' : ''}"
                                onclick="window.__qtronNav.switchMarket('US')">US</button>
                    </div>
                </div>
                <nav class="qnav-menu">
                    <button class="qnav-item ${page === 'dashboard' ? 'active' : ''}"
                            onclick="window.__qtronNav.navigateTo('dashboard')">Dashboard</button>
                    <button class="qnav-item ${page === 'lab' ? 'active' : ''}"
                            onclick="window.__qtronNav.navigateTo('lab')">Lab</button>
                    <button class="qnav-item ${page === 'unified' ? 'active' : ''}"
                            onclick="window.__qtronNav.navigateTo('unified')">Unified</button>
                </nav>
                <div class="qnav-right">
                    <button class="qnav-tg-btn" onclick="window.__qtronNav.openTelegram()" title="Send to Telegram">
                        <svg viewBox="0 0 24 24" width="16" height="16" fill="currentColor"><path d="M11.944 0A12 12 0 0 0 0 12a12 12 0 0 0 12 12 12 12 0 0 0 12-12A12 12 0 0 0 12 0a12 12 0 0 0-.056 0zm4.962 7.224c.1-.002.321.023.465.14a.506.506 0 0 1 .171.325c.016.093.036.306.02.472-.18 1.898-.962 6.502-1.36 8.627-.168.9-.499 1.201-.82 1.23-.696.065-1.225-.46-1.9-.902-1.056-.693-1.653-1.124-2.678-1.8-1.185-.78-.417-1.21.258-1.91.177-.184 3.247-2.977 3.307-3.23.007-.032.014-.15-.056-.212s-.174-.041-.249-.024c-.106.024-1.793 1.14-5.061 3.345-.48.33-.913.49-1.302.48-.428-.008-1.252-.241-1.865-.44-.752-.245-1.349-.374-1.297-.789.027-.216.325-.437.893-.663 3.498-1.524 5.83-2.529 6.998-3.014 3.332-1.386 4.025-1.627 4.476-1.635z"/></svg>
                    </button>
                    <span class="qnav-clock" id="qnav-clock">--:--:--</span>
                </div>
                <!-- Telegram Modal -->
                <div id="tg-modal" class="tg-modal-overlay" style="display:none" onclick="if(event.target===this)window.__qtronNav.closeTelegram()">
                    <div class="tg-modal">
                        <div class="tg-modal-header">
                            <span>Send to Telegram</span>
                            <button class="tg-close" onclick="window.__qtronNav.closeTelegram()">&times;</button>
                        </div>
                        <div id="tg-editor" class="tg-editor" contenteditable="true" data-placeholder="Message or paste screenshot (Ctrl+V)..."></div>
                        <input type="file" id="tg-file" accept="image/*" style="display:none" onchange="window.__qtronNav._fileChanged(this)">
                        <div class="tg-file-row">
                            <label class="tg-file-label" onclick="document.getElementById('tg-file').click()">
                                <span>+ Attach Image</span>
                            </label>
                            <button id="tg-clear-file" style="display:none" onclick="window.__qtronNav._clearFile()">Clear Image &times;</button>
                        </div>
                        <div class="tg-actions">
                            <span id="tg-status" class="tg-status"></span>
                            <button class="tg-send-btn" onclick="window.__qtronNav.sendTelegram()">Send</button>
                        </div>
                    </div>
                </div>
            </div>
        `;

        // Clock
        function tick() {
            const now = new Date();
            const el = document.getElementById('qnav-clock');
            if (!el) return;
            const kst = now.toLocaleTimeString('ko-KR', {hour12: false});
            if (market === 'US') {
                const et = now.toLocaleTimeString('en-US', {hour12: false, timeZone: 'America/New_York'});
                el.innerHTML = `KST ${kst}<br><span style="font-size:10px;opacity:0.6;">ET ${et}</span>`;
            } else {
                el.textContent = kst;
            }
        }
        tick();
        setInterval(tick, 1000);
    }

    // ── Telegram Modal Functions ──
    let _tgPastedBlob = null;  // clipboard image blob

    function openTelegram() {
        document.getElementById('tg-modal').style.display = 'flex';
        const editor = document.getElementById('tg-editor');
        editor.innerHTML = '';
        editor.focus();

        // Clipboard paste handler
        if (!editor._pasteHandlerAttached) {
            editor.addEventListener('paste', (e) => {
                const items = (e.clipboardData || {}).items || [];
                for (const item of items) {
                    if (item.type.startsWith('image/')) {
                        e.preventDefault();
                        const blob = item.getAsFile();
                        _tgPastedBlob = blob;
                        // Show preview inside editor
                        const reader = new FileReader();
                        reader.onload = (ev) => {
                            // Remove existing images
                            editor.querySelectorAll('img').forEach(img => img.remove());
                            const img = document.createElement('img');
                            img.src = ev.target.result;
                            img.style.cssText = 'max-width:100%;max-height:200px;border-radius:6px;display:block;margin-top:6px';
                            editor.appendChild(img);
                            document.getElementById('tg-clear-file').style.display = 'inline';
                        };
                        reader.readAsDataURL(blob);
                        return;
                    }
                }
            });
            editor._pasteHandlerAttached = true;
        }
    }

    function closeTelegram() {
        document.getElementById('tg-modal').style.display = 'none';
        document.getElementById('tg-editor').innerHTML = '';
        _clearFile();
        document.getElementById('tg-status').textContent = '';
    }

    function _fileChanged(input) {
        const file = input.files[0];
        if (!file) return;
        _tgPastedBlob = file;
        const editor = document.getElementById('tg-editor');
        editor.querySelectorAll('img').forEach(img => img.remove());
        const reader = new FileReader();
        reader.onload = (e) => {
            const img = document.createElement('img');
            img.src = e.target.result;
            img.style.cssText = 'max-width:100%;max-height:200px;border-radius:6px;display:block;margin-top:6px';
            editor.appendChild(img);
            document.getElementById('tg-clear-file').style.display = 'inline';
        };
        reader.readAsDataURL(file);
    }

    function _clearFile() {
        _tgPastedBlob = null;
        const input = document.getElementById('tg-file');
        if (input) input.value = '';
        document.getElementById('tg-clear-file').style.display = 'none';
        const editor = document.getElementById('tg-editor');
        if (editor) editor.querySelectorAll('img').forEach(img => img.remove());
    }

    async function sendTelegram() {
        const editor = document.getElementById('tg-editor');
        const text = editor.innerText.trim();
        const fileInput = document.getElementById('tg-file');
        const fileFromInput = fileInput && fileInput.files[0];
        const imageBlob = _tgPastedBlob || fileFromInput;
        const status = document.getElementById('tg-status');

        if (!text && !imageBlob) {
            status.textContent = 'Enter message or paste/attach image';
            status.style.color = '#FF991F';
            return;
        }

        status.textContent = 'Sending...';
        status.style.color = '#8b949e';

        try {
            let ok = false;

            if (imageBlob) {
                const form = new FormData();
                form.append('photo', imageBlob, imageBlob.name || 'screenshot.png');
                form.append('caption', text);
                const resp = await fetch('/api/notify/telegram/photo', { method: 'POST', body: form });
                const data = await resp.json();
                ok = data.ok;
                if (!ok) { status.textContent = data.error || 'Failed'; status.style.color = '#F04452'; return; }
            } else {
                const resp = await fetch('/api/notify/telegram', {
                    method: 'POST',
                    headers: { 'Content-Type': 'application/json' },
                    body: JSON.stringify({ text }),
                });
                const data = await resp.json();
                ok = data.ok;
            }

            if (ok) {
                status.textContent = 'Sent!';
                status.style.color = '#00C853';
                setTimeout(closeTelegram, 1200);
            } else {
                status.textContent = 'Failed to send';
                status.style.color = '#F04452';
            }
        } catch (e) {
            status.textContent = 'Error: ' + e.message;
            status.style.color = '#F04452';
        }
    }

    // Expose API
    window.__qtronNav = { switchMarket, navigateTo, getCurrentMarket, renderNav,
                          openTelegram, closeTelegram, sendTelegram, _fileChanged, _clearFile };
})();
