/**
 * qc-summary-card — Q-TRON Summary hero meta component (US side)
 * ==============================================================
 * Phase 3 extraction. Renders the existing hero-meta block without any
 * logic change. Byte-for-byte extraction of the inline renderStatus()
 * Portfolio section (us/web/templates/index.html).
 *
 * Contract (matches docs/ui_data_contract_20260424.md §2)
 *   render(host, data)
 *     host — DOM element (decorative; internal lookup uses document.getElementById)
 *     data — { portfolio, marketOpen } where portfolio is the /api/portfolio
 *            response shape used previously. Kept identical so this commit
 *            is a pure refactor.
 *
 * No API changes. No calc changes. No state changes.
 */
(function () {
    'use strict';
    window.qc = window.qc || {};

    function _fmt(n, dec) {
        if (dec === undefined) dec = 2;
        if (n === null || n === undefined || isNaN(n)) return '--';
        return '$' + Number(n).toLocaleString('en-US', {
            minimumFractionDigits: dec, maximumFractionDigits: dec,
        });
    }

    function _pctClass(v) { return v >= 0 ? 'gain' : 'loss'; }

    function _by(id) { return document.getElementById(id); }

    function render(host, data) {
        const p = (data && data.portfolio) || {};
        const marketOpen = data && data.marketOpen;

        // Defensive: bail quietly if section not on current page.
        if (!_by('hero-equity')) return;

        _by('hero-equity').textContent = _fmt(p.equity);
        _by('hero-cash').textContent = _fmt(p.cash);

        // Total P&L (since inception, base $100k)
        const pnl = (p.equity || 0) - 100000;
        _by('hero-pnl').textContent = _fmt(pnl);
        _by('hero-pnl').className = 'meta-value ' + _pctClass(pnl);

        const pnlPct = pnl / 100000 * 100;
        _by('hero-pnl-pct').textContent = (pnlPct >= 0 ? '+' : '') + pnlPct.toFixed(2) + '%';
        _by('hero-pnl-pct').className = 'meta-value ' + _pctClass(pnlPct);

        // Today's P&L (vs last_equity from Alpaca)
        const lastEq = p.last_equity || 0;
        if (lastEq > 0) {
            const pnlToday = (p.equity || 0) - lastEq;
            const pnlTodayPct = (pnlToday / lastEq) * 100;
            const sign = pnlToday >= 0 ? '+' : '';
            _by('hero-pnl-today').textContent = 'vs Prev ' + sign + _fmt(pnlToday);
            _by('hero-pnl-today').className = 'meta-sub ' + _pctClass(pnlToday);
            _by('hero-pnl-pct-today').textContent = 'vs Prev ' + sign + pnlTodayPct.toFixed(2) + '%';
            _by('hero-pnl-pct-today').className = 'meta-sub ' + _pctClass(pnlTodayPct);
        } else {
            _by('hero-pnl-today').textContent = 'vs Prev --';
            _by('hero-pnl-pct-today').textContent = 'vs Prev --';
        }

        _by('hero-positions').textContent = p.n_holdings || 0;

        // Buying Power card (P0-3 §2 treats this as part of Summary)
        const bp = _by('card-bp');
        if (bp) bp.textContent = _fmt(p.buying_power);

        // Market Open/Close is a Badge (P0-3 §5) not Summary, but was rendered
        // in the same inline block historically. Preserve that behavior.
        const mkt = _by('card-market');
        if (mkt) mkt.textContent = marketOpen ? 'Open' : 'Closed';
    }

    window.qc.summary = { render };
})();
