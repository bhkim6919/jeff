/**
 * qc-regime-card — Q-TRON Regime card component (US side)
 * =======================================================
 * Phase 3 extraction. Renders the existing #regime-section (Today
 * actual + Tomorrow prediction + 4 axes) plus the inline Sector grid
 * without any logic change. Pure refactor.
 *
 * Note: the sector grid is technically a separate concept (qc-sector-regime
 * is its own component scheduled for Phase 4 per IA §3). For Phase 3
 * we keep it co-located here because the existing refreshRegime() code
 * renders it in a single pass — splitting now would require duplicating
 * the API fetch.
 *
 * Contract (matches docs/ui_data_contract_20260424.md §4)
 *   render(host, data)
 *     host — DOM element (decorative; lookups use document.getElementById)
 *     data — /api/regime/current response { today, prediction, sectors }
 *
 * No API changes. No calc changes. No state changes.
 */
(function () {
    'use strict';
    window.qc = window.qc || {};

    const REGIME_COLORS = {
        1:'#F04452', 2:'#FF991F', 3:'#FFD600', 4:'#36B37E', 5:'#00C853',
    };

    function _by(id) { return document.getElementById(id); }

    function _regimeToBarPct(level, score) {
        const base = ((level - 1) / 4) * 100;
        const adjust = (score || 0) * 10;
        return Math.max(2, Math.min(98, base + adjust));
    }

    function _renderAxis(id, score, avail) {
        const el = document.getElementById(id);
        const valEl = document.getElementById(id + '-val');
        if (!el) return;
        if (!avail) {
            el.style.width = '0%';
            el.style.left = '50%';
            valEl.textContent = 'N/A';
            valEl.style.color = 'var(--muted)';
            return;
        }
        const pct = Math.abs(score) * 50;
        if (score >= 0) {
            el.style.left = '50%';
            el.style.width = pct + '%';
            el.style.background = 'var(--green)';
        } else {
            el.style.left = (50 - pct) + '%';
            el.style.width = pct + '%';
            el.style.background = 'var(--red)';
        }
        valEl.textContent = (score >= 0 ? '+' : '') + score.toFixed(2);
        valEl.style.color = score >= 0 ? 'var(--green)' : 'var(--red)';
    }

    function render(host, r) {
        if (!r || r.error) return;
        // Defensive: bail if regime section not on current page.
        if (!_by('regime-today-marker') && !_by('regime-pred-marker')) return;

        // Today
        const today = r.today || {};
        if (today.available && _by('regime-today-marker')) {
            const pct = _regimeToBarPct(today.actual_regime, today.spy_change_pct / 3);
            _by('regime-today-marker').style.left = pct + '%';
            _by('regime-today-label').textContent = today.actual_label;
            _by('regime-today-label').style.color = REGIME_COLORS[today.actual_regime] || 'var(--text)';
            _by('regime-today-detail').textContent = `SPY ${today.spy_price} (${today.spy_change_pct >= 0 ? '+' : ''}${today.spy_change_pct}%) | Breadth ${(today.breadth_ratio*100).toFixed(0)}%`;
        }

        // Prediction
        const pred = r.prediction || {};
        const ppct = _regimeToBarPct(pred.predicted_regime || 3, pred.composite_score);
        if (_by('regime-pred-marker')) {
            _by('regime-pred-marker').style.left = ppct + '%';
            _by('regime-pred-label').textContent = pred.predicted_label || '--';
            _by('regime-pred-label').style.color = REGIME_COLORS[pred.predicted_regime] || 'var(--text)';
            const confEl = _by('regime-pred-conf');
            if (confEl) {
                confEl.textContent = `${pred.confidence_flag || '--'} (${((pred.available_weight||0)*100).toFixed(0)}%)`;
                confEl.className = 'badge ' + (pred.confidence_flag === 'FULL' ? 'badge-open' : pred.confidence_flag === 'PARTIAL' ? 'badge-paper' : 'badge-closed');
            }
        }

        // Axes (4 for US: index, vol, sector, fx)
        _renderAxis('ax-index', pred.index_score, pred.index_avail);
        _renderAxis('ax-vol', pred.vol_score, pred.vol_avail);
        _renderAxis('ax-sector', pred.sector_score, pred.sector_avail);
        _renderAxis('ax-fx', pred.fx_score, pred.fx_avail);

        // Sectors with on-demand tooltip (like KR theme_regime).
        // Co-located here because the inline implementation rendered them
        // in the same pass; future Phase 4 qc-sector-regime can split.
        const sectors = r.sectors || [];
        const sectorGrid = _by('sector-grid');
        if (sectors.length > 0 && sectorGrid) {
            sectorGrid.innerHTML = sectors.map(s => {
                const color = s.change_pct >= 1 ? 'var(--green)' : s.change_pct >= 0 ? '#36B37E' : s.change_pct >= -1 ? '#FF991F' : 'var(--red)';
                return `<div class="sector-chip" onmouseenter="_sectorHover(this,'${s.name}')" onmouseleave="_sectorOut(this)">
                    <div class="sector-name">${s.name}</div>
                    <div class="sector-change" style="color:${color}">${s.change_pct >= 0 ? '+' : ''}${s.change_pct.toFixed(2)}%</div>
                    <div class="sector-regime" style="color:${color}">${s.regime}</div>
                    <div class="sr-tooltip" style="display:none"></div>
                </div>`;
            }).join('');
        }
    }

    window.qc.regime = { render };
})();
