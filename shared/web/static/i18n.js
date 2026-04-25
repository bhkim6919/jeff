/**
 * shared/i18n.js — Q-TRON i18n engine (Phase 5-A)
 * ================================================
 * Reads `data-i18n` attributes throughout the page and substitutes
 * text from a locale resource bundle. Toggle via window.qcI18n.set('en'|'ko').
 *
 * Resource files (loaded once, cached):
 *   /static/shared/i18n/ko.json
 *   /static/shared/i18n/en.json
 *
 * Attribute conventions
 *   data-i18n="dash.title.holdings"        — replace textContent
 *   data-i18n-attr="title:foo.bar.baz"     — replace specified attribute
 *   data-i18n-html="dash.help.something"   — innerHTML (use sparingly,
 *                                             only for trusted resource keys)
 *
 * Defensive: any missing key falls back to the original DOM text/attr,
 * so unmapped elements continue to render their authored content.
 */
(function () {
    'use strict';

    const STORAGE_KEY = 'qtron_lang';
    const DEFAULT_LOCALE = 'ko';
    const SUPPORTED = ['ko', 'en'];

    const _cache = {};   // locale -> resource object
    const _origText = new WeakMap();  // element -> original textContent
    const _origAttr = new Map();      // `${tagId}::${attr}` -> original value
    let _currentLocale = null;

    function _resolveKey(obj, dotKey) {
        return dotKey.split('.').reduce((o, k) => (o && o[k] !== undefined ? o[k] : null), obj);
    }

    async function _loadLocale(loc) {
        if (_cache[loc]) return _cache[loc];
        try {
            const r = await fetch(`/static/shared/i18n/${loc}.json`);
            const d = await r.json();
            _cache[loc] = d;
            return d;
        } catch (err) {
            console.warn(`[i18n] failed to load locale "${loc}":`, err);
            _cache[loc] = {};
            return _cache[loc];
        }
    }

    function _applyTranslations() {
        const res = _cache[_currentLocale] || {};
        // 1. textContent translations
        document.querySelectorAll('[data-i18n]').forEach(el => {
            const key = el.dataset.i18n;
            if (!key) return;
            // Stash the original text once so we can restore on missing-key fallback
            if (!_origText.has(el)) _origText.set(el, el.textContent);
            const val = _resolveKey(res, key);
            el.textContent = (val != null) ? val : _origText.get(el);
        });
        // 2. innerHTML translations (trusted keys only)
        document.querySelectorAll('[data-i18n-html]').forEach(el => {
            const key = el.dataset.i18nHtml;
            const val = _resolveKey(res, key);
            if (val != null) el.innerHTML = val;
        });
        // 3. Attribute translations: data-i18n-attr="title:dash.tip.foo"
        document.querySelectorAll('[data-i18n-attr]').forEach(el => {
            const spec = el.dataset.i18nAttr;
            if (!spec) return;
            spec.split(';').forEach(pair => {
                const [attr, key] = pair.split(':').map(s => s.trim());
                if (!attr || !key) return;
                const val = _resolveKey(res, key);
                if (val != null) el.setAttribute(attr, val);
            });
        });
        // 4. <html lang="..."> reflects locale (a11y + downstream Intl)
        document.documentElement.lang = _currentLocale;
    }

    async function setLocale(loc) {
        if (!SUPPORTED.includes(loc)) {
            console.warn(`[i18n] unsupported locale "${loc}", falling back to "${DEFAULT_LOCALE}"`);
            loc = DEFAULT_LOCALE;
        }
        _currentLocale = loc;
        try { localStorage.setItem(STORAGE_KEY, loc); } catch (_) {}
        await _loadLocale(loc);
        _applyTranslations();
        // Notify nav to update its toggle UI
        document.dispatchEvent(new CustomEvent('qc:i18n-changed', { detail: { locale: loc } }));
    }

    function getLocale() {
        if (_currentLocale) return _currentLocale;
        try {
            const stored = localStorage.getItem(STORAGE_KEY);
            if (stored && SUPPORTED.includes(stored)) return stored;
        } catch (_) {}
        return DEFAULT_LOCALE;
    }

    // Intl helpers — exposed for components / inline scripts.
    function fmtDate(d, opts) {
        try {
            return new Intl.DateTimeFormat(getLocale(), opts || {}).format(d instanceof Date ? d : new Date(d));
        } catch (_) {
            return String(d);
        }
    }
    function fmtNumber(n, opts) {
        try {
            return new Intl.NumberFormat(getLocale(), opts || {}).format(Number(n));
        } catch (_) {
            return String(n);
        }
    }

    // Auto-init on DOMContentLoaded
    function _init() {
        const loc = getLocale();
        setLocale(loc);
    }
    if (document.readyState === 'loading') {
        document.addEventListener('DOMContentLoaded', _init);
    } else {
        _init();
    }

    // Also re-apply translations whenever the page injects new
    // data-i18n elements dynamically (component re-renders).
    // Components can dispatch 'qc:reapply-i18n' to opt in.
    document.addEventListener('qc:reapply-i18n', _applyTranslations);

    window.qcI18n = {
        set: setLocale,
        get: getLocale,
        fmtDate,
        fmtNumber,
        SUPPORTED,
    };
})();
