/**
 * qc-regime-card — Q-TRON Regime card component (KR side)
 * =======================================================
 * Phase 3 extraction. Renders the existing #regime-section (Today
 * actual + Tomorrow prediction + 5 axes + rolling stats) without
 * any logic change. Pure refactor — same DOM IDs, same calc, same
 * thresholds, same color palette.
 *
 * Contract (matches docs/ui_data_contract_20260424.md §4)
 *   render(host, data)
 *     host — DOM element (decorative; lookups use document.getElementById)
 *     data — SSE snapshot object containing:
 *              data.regime_actual     (today's actual)
 *              data.regime_prediction (tomorrow's prediction)
 *
 * No API changes. No calc changes. No state changes.
 */
(function () {
    'use strict';
    window.qc = window.qc || {};

    function _regimeToBarPct(level, score) {
        // 5단계: 1=0%, 2=25%, 3=50%, 4=75%, 5=100%
        // score로 미세 조정 (±10%)
        const base = ((level - 1) / 4) * 100;
        const adjust = (score || 0) * 5;
        return Math.max(2, Math.min(98, base + adjust));
    }

    function _regimeLabelColor(level) {
        if (level <= 1) return '#3182F6';
        if (level <= 2) return '#5B9CF6';
        if (level <= 3) return '#FFD600';
        if (level <= 4) return '#F07060';
        return '#F04452';
    }

    function render(host, data) {
        const rp = data && data.regime_prediction;

        // ── 오늘 실제 레짐 (from SSE regime_actual or latest) ──
        const todayMarker = document.getElementById('regime-today-marker');
        // Defensive: bail quietly if section not on current page (/debug).
        if (!todayMarker && !rp) return;

        const todayDetail = document.getElementById('regime-today-detail');
        const todayBreakdown = document.getElementById('regime-today-breakdown');
        const todayDate = document.getElementById('regime-today-date');

        const actual = data && data.regime_actual;
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

    window.qc.regime = { render };
})();
