/**
 * shared/themes.js — Q-TRON theme switcher
 * =========================================
 * Wires the .qtron-theme-tab[data-theme-set] buttons at the bottom
 * of each Dashboard. Persists choice in localStorage so the next page
 * load applies it before paint. Sets `data-theme` on <html> and toggles
 * the "✓ 저장됨" / "✓ Saved" indicator.
 *
 * Themes
 *   dark    — default (current Q-TRON look, no overrides)
 *   light   — high-luminance variant
 *   pastel  — soft accents, slightly desaturated
 *   pro     — high contrast, sharper accents
 *
 * Each theme is implemented as a CSS variable override block under
 * `html[data-theme="..."]` selectors in shared/web/static/style.css.
 *
 * Per-device save toggle (#qtron-save-default)
 *   checked  → write to localStorage (per-device)
 *   unchecked → use sessionStorage only (cleared on tab close)
 */
(function () {
    'use strict';

    const KEY = 'qtron_theme';
    const KEY_PERSIST = 'qtron_theme_persist';
    const VALID = ['dark', 'light', 'pastel', 'pro'];
    const DEFAULT = 'dark';

    function _store(persist) {
        return persist ? localStorage : sessionStorage;
    }

    function _readPersistFlag() {
        try {
            const v = localStorage.getItem(KEY_PERSIST);
            return v === null ? true : (v === 'true');
        } catch (_) { return true; }
    }

    function _readTheme() {
        try {
            const persist = _readPersistFlag();
            const v = _store(persist).getItem(KEY);
            if (v && VALID.includes(v)) return v;
            // Fallback: try the other store
            const alt = (persist ? sessionStorage : localStorage).getItem(KEY);
            if (alt && VALID.includes(alt)) return alt;
        } catch (_) {}
        return DEFAULT;
    }

    function _writeTheme(theme) {
        try {
            const persist = _readPersistFlag();
            _store(persist).setItem(KEY, theme);
            // Mirror to sessionStorage too so a same-tab toggle doesn't lose
            // immediate state if the persist flag changes.
            sessionStorage.setItem(KEY, theme);
        } catch (_) {}
    }

    function _writePersistFlag(persist) {
        try { localStorage.setItem(KEY_PERSIST, persist ? 'true' : 'false'); } catch (_) {}
    }

    function _applyTheme(theme) {
        if (!VALID.includes(theme)) theme = DEFAULT;
        document.documentElement.setAttribute('data-theme', theme);
        // Highlight the active tab
        document.querySelectorAll('.qtron-theme-tab').forEach(btn => {
            btn.classList.toggle('active', btn.dataset.themeSet === theme);
        });
        // Show saved indicator briefly
        const ind = document.getElementById('qtron-saved-indicator');
        if (ind) {
            ind.style.opacity = '1';
            clearTimeout(ind._fadeTimer);
            ind._fadeTimer = setTimeout(() => { ind.style.opacity = '0.5'; }, 1200);
        }
    }

    function setTheme(theme) {
        _writeTheme(theme);
        _applyTheme(theme);
    }

    function _wireUI() {
        // Theme tab clicks
        document.querySelectorAll('.qtron-theme-tab').forEach(btn => {
            btn.addEventListener('click', () => {
                const target = btn.dataset.themeSet;
                if (target) setTheme(target);
            });
        });
        // Persist checkbox
        const cb = document.getElementById('qtron-save-default');
        if (cb) {
            cb.checked = _readPersistFlag();
            cb.addEventListener('change', () => {
                const persist = cb.checked;
                _writePersistFlag(persist);
                // Move current value into the right store
                const cur = _readTheme();
                _writeTheme(cur);
            });
        }
    }

    function _init() {
        const theme = _readTheme();
        _applyTheme(theme);
        _wireUI();
    }

    // Apply theme ASAP to avoid FOUC; wire UI on DOMContentLoaded.
    _applyTheme(_readTheme());
    if (document.readyState === 'loading') {
        document.addEventListener('DOMContentLoaded', _init);
    } else {
        _init();
    }

    window.qcTheme = { set: setTheme, get: _readTheme, VALID };
})();
