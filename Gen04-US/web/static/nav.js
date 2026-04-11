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
                    <span class="qnav-clock" id="qnav-clock">--:--:--</span>
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

    // Expose API
    window.__qtronNav = { switchMarket, navigateTo, getCurrentMarket, renderNav };
})();
