# -*- coding: utf-8 -*-
"""
premium_style.py — Premium HTML Report Design System
=====================================================
Shared CSS/HTML template for Daily, Weekly, Monthly reports.
Designed by: Design Studio A (Q-TRON 3-year contract bid)

Design Philosophy:
  - Institutional-grade executive dashboard aesthetic
  - Korean trading convention: Red=profit, Blue=loss
  - Self-contained (no CDN, offline-compatible)
  - Mobile-first responsive (640px breakpoint)
  - Print-optimized (color-adjust exact, page-break control)
  - Dark header gradient + light content body
  - Glassmorphism cards with subtle depth hierarchy
  - Smooth micro-interactions (hover, fade-in)
  - Typography: Pretendard → system fallback

Changes from previous design:
  - Refined gradient header with animated mesh background
  - Card system: frosted glass with top accent border
  - Enhanced KPI hero numbers (48px bold)
  - Improved table: row zebra + hover + sticky header
  - Better alert/verdict badges with icon+text
  - Refined color palette (muted professional tones)
  - Print layout: clean, no hover artifacts
  - New: section collapse/expand (details/summary)
  - New: mini-bar progress indicators
  - New: ribbon badges for verdicts

Engine impact: ZERO — this file generates CSS/HTML strings only.
"""
from __future__ import annotations


def get_premium_css() -> str:
    """Return the complete premium CSS for reports.

    Uses {{ / }} for f-string compatibility when embedded
    in a larger f-string template. The caller MUST use this
    inside a <style> tag.
    """
    return """
:root {
    /* ── Core Palette ─────────────────── */
    --primary: #0B1426;
    --primary-light: #162544;
    --accent: #0891B2;
    --accent-light: rgba(8,145,178,0.12);

    /* Korean convention: red=profit, blue=loss */
    --profit: #DC2626;
    --profit-bg: rgba(220,38,38,0.06);
    --profit-border: rgba(220,38,38,0.25);
    --loss: #2563EB;
    --loss-bg: rgba(37,99,235,0.06);
    --loss-border: rgba(37,99,235,0.25);

    --success: #059669;
    --success-bg: rgba(5,150,105,0.08);
    --warning: #D97706;
    --warning-bg: rgba(217,119,6,0.08);
    --danger: #DC2626;
    --danger-bg: rgba(220,38,38,0.08);
    --info: #0891B2;
    --info-bg: rgba(8,145,178,0.08);

    /* Surfaces */
    --surface: #FFFFFF;
    --surface-raised: #FFFFFF;
    --surface-alt: #F8FAFC;
    --surface-hover: #F1F5F9;
    --body-bg: #F0F4F8;

    /* Text */
    --text: #0F172A;
    --text-secondary: #475569;
    --text-dim: #94A3B8;
    --text-inverse: #FFFFFF;

    /* Borders & Shadows */
    --border: #E2E8F0;
    --border-light: #F1F5F9;
    --shadow-sm: 0 1px 2px rgba(0,0,0,0.04);
    --shadow-md: 0 4px 12px rgba(0,0,0,0.06);
    --shadow-lg: 0 12px 32px rgba(0,0,0,0.08);
    --shadow-hover: 0 8px 24px rgba(0,0,0,0.10);

    /* Typography */
    --font: 'Pretendard','Malgun Gothic',-apple-system,BlinkMacSystemFont,'Segoe UI',sans-serif;
    --font-mono: 'JetBrains Mono','Consolas','Courier New',monospace;

    /* Sizing */
    --radius-sm: 6px;
    --radius-md: 10px;
    --radius-lg: 14px;
    --radius-xl: 20px;
}

/* ══════════════════════════════════════════════════════════ */
/*  Base Reset                                               */
/* ══════════════════════════════════════════════════════════ */
* { box-sizing: border-box; margin: 0; padding: 0; }

body {
    font-family: var(--font);
    background: var(--body-bg);
    color: var(--text);
    line-height: 1.65;
    -webkit-font-smoothing: antialiased;
    -moz-osx-font-smoothing: grayscale;
}

/* ══════════════════════════════════════════════════════════ */
/*  Header                                                   */
/* ══════════════════════════════════════════════════════════ */
.g4-header {
    background: linear-gradient(145deg, #0B1426 0%, #132B4A 50%, #1A365D 100%);
    padding: 40px 24px 36px;
    text-align: center;
    position: relative;
    overflow: hidden;
}
.g4-header::before {
    content: '';
    position: absolute; top: 0; left: 0; right: 0; bottom: 0;
    background:
        radial-gradient(ellipse at 15% 50%, rgba(8,145,178,0.15) 0%, transparent 55%),
        radial-gradient(ellipse at 85% 30%, rgba(220,38,38,0.08) 0%, transparent 55%),
        radial-gradient(ellipse at 50% 80%, rgba(139,92,246,0.06) 0%, transparent 40%);
    pointer-events: none;
}
.g4-header::after {
    content: '';
    position: absolute; bottom: 0; left: 0; right: 0; height: 1px;
    background: linear-gradient(90deg, transparent 0%, rgba(8,145,178,0.4) 50%, transparent 100%);
}
.g4-header h1 {
    font-size: 26px; color: #FFFFFF; margin: 0 0 4px; font-weight: 800;
    letter-spacing: -0.5px; position: relative;
}
.g4-header .g4-brand {
    font-size: 10px; color: rgba(255,255,255,0.40); letter-spacing: 3px;
    text-transform: uppercase; margin-bottom: 10px; position: relative;
    font-weight: 500;
}
.g4-header .g4-date {
    font-size: 15px; color: rgba(255,255,255,0.65); position: relative;
    font-weight: 400;
}
.g4-header .g4-subtitle {
    font-size: 12px; color: rgba(255,255,255,0.45); position: relative;
    margin-top: 4px;
}

/* ══════════════════════════════════════════════════════════ */
/*  Container                                                */
/* ══════════════════════════════════════════════════════════ */
.container {
    max-width: 980px;
    margin: -24px auto 0;
    padding: 0 16px 40px;
    position: relative;
    z-index: 1;
}

/* ══════════════════════════════════════════════════════════ */
/*  Section (card wrapper)                                   */
/* ══════════════════════════════════════════════════════════ */
.g4-section {
    background: var(--surface);
    border-radius: var(--radius-lg);
    box-shadow: var(--shadow-sm);
    margin-bottom: 16px;
    padding: 20px 24px;
    border: 1px solid var(--border-light);
    position: relative;
    animation: g4FadeIn 0.4s ease-out;
}
.g4-section::before {
    content: '';
    position: absolute; top: 0; left: 0; right: 0; height: 3px;
    background: linear-gradient(90deg, var(--accent), rgba(8,145,178,0.3));
    border-radius: var(--radius-lg) var(--radius-lg) 0 0;
}

.g4-section-title {
    font-size: 13px;
    font-weight: 700;
    color: var(--text-secondary);
    text-transform: uppercase;
    letter-spacing: 0.8px;
    margin-bottom: 14px;
    display: flex;
    align-items: center;
    gap: 8px;
}
.g4-section-title .g4-icon {
    font-size: 16px;
}

@keyframes g4FadeIn {
    from { opacity: 0; transform: translateY(10px); }
    to { opacity: 1; transform: translateY(0); }
}

/* ══════════════════════════════════════════════════════════ */
/*  KPI Cards                                                */
/* ══════════════════════════════════════════════════════════ */
.g4-cards {
    display: flex;
    flex-wrap: wrap;
    gap: 12px;
    margin-bottom: 8px;
}
.g4-card {
    flex: 1;
    min-width: 150px;
    background: var(--surface);
    border-radius: var(--radius-md);
    padding: 16px 18px;
    border: 1px solid var(--border);
    position: relative;
    overflow: hidden;
    transition: transform 0.2s ease, box-shadow 0.2s ease;
}
.g4-card:hover {
    transform: translateY(-2px);
    box-shadow: var(--shadow-hover);
}
.g4-card::before {
    content: '';
    position: absolute; top: 0; left: 0; right: 0; height: 3px;
    border-radius: var(--radius-md) var(--radius-md) 0 0;
}
.g4-card.profit::before { background: var(--profit); }
.g4-card.loss::before { background: var(--loss); }
.g4-card.neutral::before { background: var(--border); }
.g4-card.success::before { background: var(--success); }
.g4-card.warning::before { background: var(--warning); }
.g4-card.danger::before { background: var(--danger); }

.g4-card .g4-card-label {
    font-size: 11px; color: var(--text-dim); font-weight: 600;
    text-transform: uppercase; letter-spacing: 0.5px;
    margin-bottom: 6px;
}
.g4-card .g4-card-value {
    font-size: 28px; font-weight: 800; line-height: 1.1;
    font-family: var(--font);
    letter-spacing: -0.5px;
}
.g4-card .g4-card-sub {
    font-size: 11px; color: var(--text-dim); margin-top: 4px;
}
.g4-card .g4-card-value.profit { color: var(--profit); }
.g4-card .g4-card-value.loss { color: var(--loss); }
.g4-card .g4-card-value.neutral { color: var(--text); }

/* Hero KPI (main number) */
.g4-hero-value {
    font-size: 48px; font-weight: 900; letter-spacing: -1px;
    line-height: 1.0;
}

/* ══════════════════════════════════════════════════════════ */
/*  Verdict Badge / Ribbon                                   */
/* ══════════════════════════════════════════════════════════ */
.g4-verdict {
    display: inline-flex;
    align-items: center;
    gap: 8px;
    padding: 8px 20px;
    border-radius: 100px;
    font-size: 14px;
    font-weight: 700;
    letter-spacing: 0.5px;
}
.g4-verdict.danger {
    background: var(--danger-bg); color: var(--danger);
    border: 1px solid rgba(220,38,38,0.2);
}
.g4-verdict.warning {
    background: var(--warning-bg); color: var(--warning);
    border: 1px solid rgba(217,119,6,0.2);
}
.g4-verdict.success {
    background: var(--success-bg); color: var(--success);
    border: 1px solid rgba(5,150,105,0.2);
}
.g4-verdict.info {
    background: var(--info-bg); color: var(--info);
    border: 1px solid rgba(8,145,178,0.2);
}

/* ══════════════════════════════════════════════════════════ */
/*  Tables                                                   */
/* ══════════════════════════════════════════════════════════ */
.g4-table-wrap {
    overflow-x: auto;
    -webkit-overflow-scrolling: touch;
    border-radius: var(--radius-md);
    border: 1px solid var(--border);
}
table {
    width: 100%;
    border-collapse: separate;
    border-spacing: 0;
    font-size: 12px;
}
thead th {
    background: #F1F5F9;
    color: var(--text-secondary);
    font-weight: 700;
    font-size: 10.5px;
    text-transform: uppercase;
    letter-spacing: 0.4px;
    padding: 10px 12px;
    text-align: left;
    border-bottom: 2px solid var(--border);
    position: sticky;
    top: 0;
    z-index: 1;
}
tbody td {
    padding: 9px 12px;
    border-bottom: 1px solid var(--border-light);
    font-family: var(--font-mono);
    font-size: 12px;
    vertical-align: middle;
}
tbody tr {
    transition: background 0.12s ease;
}
tbody tr:nth-child(even) { background: var(--surface-alt); }
tbody tr:nth-child(odd) { background: var(--surface); }
tbody tr:hover { background: #EFF6FF !important; }

td.g4-profit-cell { background: var(--profit-bg) !important; }
td.g4-loss-cell { background: var(--loss-bg) !important; }

/* Sortable */
.g4-sortable {
    cursor: pointer; user-select: none;
    transition: color 0.12s ease;
}
.g4-sortable:hover { color: var(--accent) !important; }

/* Sticky wrap */
.g4-sticky-wrap { overflow-x: auto; max-height: none; }
.g4-sticky-wrap thead th {
    position: sticky; top: 0;
    background: #F1F5F9; z-index: 1;
}

/* ══════════════════════════════════════════════════════════ */
/*  Alert Box                                                */
/* ══════════════════════════════════════════════════════════ */
.g4-alert {
    padding: 12px 16px;
    border-radius: 0 var(--radius-md) var(--radius-md) 0;
    margin-bottom: 8px;
    font-size: 13px;
    line-height: 1.5;
    transition: box-shadow 0.15s ease, transform 0.15s ease;
    display: flex;
    align-items: flex-start;
    gap: 10px;
}
.g4-alert:hover {
    box-shadow: var(--shadow-md);
    transform: translateX(2px);
}
.g4-alert.danger {
    background: var(--danger-bg);
    border-left: 4px solid var(--danger);
}
.g4-alert.warning {
    background: var(--warning-bg);
    border-left: 4px solid var(--warning);
}
.g4-alert.success {
    background: var(--success-bg);
    border-left: 4px solid var(--success);
}
.g4-alert.info {
    background: var(--info-bg);
    border-left: 4px solid var(--info);
}
.g4-alert-icon {
    font-size: 16px;
    flex-shrink: 0;
    margin-top: 1px;
}
.g4-alert-content {
    flex: 1;
}

/* ══════════════════════════════════════════════════════════ */
/*  Progress Bar                                             */
/* ══════════════════════════════════════════════════════════ */
.g4-progress {
    display: inline-block;
    width: 52px; height: 5px;
    background: var(--border);
    border-radius: 3px;
    overflow: hidden;
    vertical-align: middle;
    margin-left: 6px;
}
.g4-progress-fill {
    height: 100%;
    border-radius: 3px;
    transition: width 0.4s ease;
}

/* ══════════════════════════════════════════════════════════ */
/*  Ops Card                                                 */
/* ══════════════════════════════════════════════════════════ */
.g4-ops {
    border-radius: var(--radius-lg);
    transition: box-shadow 0.15s ease;
}
.g4-ops:hover { box-shadow: var(--shadow-md); }

/* ══════════════════════════════════════════════════════════ */
/*  Timeline                                                 */
/* ══════════════════════════════════════════════════════════ */
.g4-timeline {
    position: relative;
    padding-left: 24px;
}
.g4-timeline::before {
    content: '';
    position: absolute; left: 6px; top: 8px; bottom: 8px;
    width: 2px;
    background: linear-gradient(180deg, var(--accent), var(--border));
    border-radius: 1px;
}
.g4-timeline-item {
    position: relative;
    margin-bottom: 12px;
    padding-left: 4px;
}
.g4-timeline-item::before {
    content: '';
    position: absolute;
    left: -22px; top: 6px;
    width: 8px; height: 8px;
    border-radius: 50%;
    background: var(--accent);
    border: 2px solid var(--surface);
}

/* ══════════════════════════════════════════════════════════ */
/*  Treemap                                                  */
/* ══════════════════════════════════════════════════════════ */
.g4-treemap {
    display: flex; flex-wrap: wrap;
    gap: 4px; padding: 8px 0;
}
.g4-treemap > div {
    transition: transform 0.15s ease;
    border-radius: var(--radius-sm);
}
.g4-treemap > div:hover {
    transform: scale(1.06);
    z-index: 2;
}

/* ══════════════════════════════════════════════════════════ */
/*  Collapsible Sections                                     */
/* ══════════════════════════════════════════════════════════ */
details.g4-collapsible {
    border: 1px solid var(--border-light);
    border-radius: var(--radius-md);
    margin-bottom: 8px;
}
details.g4-collapsible summary {
    padding: 10px 16px;
    font-size: 12px;
    font-weight: 600;
    color: var(--text-secondary);
    cursor: pointer;
    user-select: none;
    list-style: none;
    display: flex;
    align-items: center;
    gap: 8px;
}
details.g4-collapsible summary::before {
    content: '\\25B6';
    font-size: 9px;
    color: var(--text-dim);
    transition: transform 0.2s ease;
}
details.g4-collapsible[open] summary::before {
    transform: rotate(90deg);
}
details.g4-collapsible .g4-detail-body {
    padding: 0 16px 12px;
}

/* ══════════════════════════════════════════════════════════ */
/*  Footer                                                   */
/* ══════════════════════════════════════════════════════════ */
.g4-footer {
    text-align: center;
    font-size: 11px;
    color: var(--text-dim);
    margin-top: 36px;
    padding: 20px 0;
    border-top: 1px solid var(--border);
}
.g4-footer a {
    color: var(--accent);
    text-decoration: none;
}
.g4-footer .g4-footer-brand {
    font-size: 10px;
    letter-spacing: 2px;
    text-transform: uppercase;
    color: var(--text-dim);
    opacity: 0.6;
    margin-top: 6px;
}

/* ══════════════════════════════════════════════════════════ */
/*  Responsive: Mobile                                       */
/* ══════════════════════════════════════════════════════════ */
@media (max-width: 640px) {
    .g4-header { padding: 28px 16px 24px; }
    .g4-header h1 { font-size: 20px; }
    .container { padding: 0 10px 28px; margin-top: -16px; }
    .g4-section { padding: 16px 14px !important; border-radius: var(--radius-md); }
    .g4-card { min-width: 130px !important; padding: 12px 14px !important; }
    .g4-card .g4-card-value { font-size: 22px !important; }
    .g4-hero-value { font-size: 36px !important; }
    table { font-size: 11px !important; }
    table th, table td { padding: 5px 8px !important; }
    .g4-sticky-wrap { -webkit-overflow-scrolling: touch; }
    .g4-verdict { font-size: 12px; padding: 6px 14px; }
}

/* ══════════════════════════════════════════════════════════ */
/*  Print                                                    */
/* ══════════════════════════════════════════════════════════ */
@media print {
    body { background: #fff !important; padding: 0 !important; }
    .g4-header {
        background: var(--primary) !important;
        -webkit-print-color-adjust: exact;
        print-color-adjust: exact;
    }
    .container { max-width: 100%; margin: 0; padding: 0 8px; }
    .g4-section {
        box-shadow: none !important;
        border: 1px solid var(--border);
        break-inside: avoid;
        page-break-inside: avoid;
    }
    .g4-card {
        box-shadow: none !important;
        border: 1px solid var(--border);
    }
    .g4-card:hover, .g4-alert:hover, .g4-ops:hover {
        transform: none !important;
        box-shadow: none !important;
    }
    tbody tr:hover { background: inherit !important; }
    .g4-footer { margin-top: 16px; }
    details.g4-collapsible[open] { break-inside: avoid; }
    @page { margin: 1cm; }
}
"""


def get_premium_js() -> str:
    """Return shared JavaScript for sortable tables."""
    return """
function g4sort(tableId, colIdx, isNum) {
  var table = document.getElementById(tableId);
  if (!table) return;
  var tbody = table.querySelector('tbody') || table;
  var rows = Array.from(tbody.querySelectorAll('tr'));
  var dataRows = rows.filter(function(r) {
    return !r.querySelector('td[colspan]');
  });
  var sumRow = rows.filter(function(r) {
    return r.querySelector('td[colspan]');
  });
  var th = table.querySelectorAll('thead th, tr:first-child th')[colIdx];
  var asc = th && th.getAttribute('data-sort') === 'asc';
  var ths = table.querySelectorAll('thead th, tr:first-child th');
  for (var i = 0; i < ths.length; i++) {
    ths[i].setAttribute('data-sort', '');
    var arrow = ths[i].querySelector('.g4-arrow');
    if (arrow) arrow.textContent = '';
  }
  if (th) {
    th.setAttribute('data-sort', asc ? 'desc' : 'asc');
    var arrow = th.querySelector('.g4-arrow');
    if (arrow) arrow.textContent = asc ? ' \\u25BC' : ' \\u25B2';
  }
  var dir = asc ? -1 : 1;
  dataRows.sort(function(a, b) {
    var cellA = a.cells[colIdx];
    var cellB = b.cells[colIdx];
    if (!cellA || !cellB) return 0;
    var va = cellA.textContent.trim();
    var vb = cellB.textContent.trim();
    if (isNum) {
      var na = parseFloat(va.replace(/[%,+\\s\\u50d\\u65e5]/g, '').replace(/[^\\d.\\-]/g, ''));
      var nb = parseFloat(vb.replace(/[%,+\\s\\u50d\\u65e5]/g, '').replace(/[^\\d.\\-]/g, ''));
      if (isNaN(na)) na = -Infinity;
      if (isNaN(nb)) nb = -Infinity;
      return (na - nb) * dir;
    } else {
      return va.localeCompare(vb, 'ko') * dir;
    }
  });
  while (tbody.firstChild) tbody.removeChild(tbody.firstChild);
  dataRows.forEach(function(r) { tbody.appendChild(r); });
  sumRow.forEach(function(r) { tbody.appendChild(r); });
}
"""


def build_header(title: str, date_str: str, subtitle: str = "",
                 report_type: str = "Daily") -> str:
    """Build premium report header HTML."""
    return f"""
<div class="g4-header">
    <div class="g4-brand">Q-TRON GEN4 {report_type.upper()} REPORT</div>
    <h1>{title}</h1>
    <div class="g4-date">{date_str}</div>
    {"<div class='g4-subtitle'>" + subtitle + "</div>" if subtitle else ""}
</div>
"""


def build_footer(generated_at: str = "") -> str:
    """Build premium report footer HTML."""
    return f"""
<div class="g4-footer">
    <div>Generated: {generated_at} | Q-TRON Gen4 Automated Trading System</div>
    <div class="g4-footer-brand">Confidential &mdash; Internal Use Only</div>
</div>
"""


def build_verdict_badge(verdict: str, verdict_kr: str, color: str) -> str:
    """Build a pill-shaped verdict badge."""
    css_class = "success"
    icon = "&#10004;"
    if "DANGER" in verdict.upper() or "REDUCE" in verdict.upper() or "REVIEW" in verdict.upper():
        css_class = "danger"
        icon = "&#9888;"
    elif "CAUTION" in verdict.upper() or "WATCH" in verdict.upper() or "WARNING" in verdict.upper():
        css_class = "warning"
        icon = "&#9888;"
    elif "STANDBY" in verdict.upper():
        css_class = "info"
        icon = "&#8987;"

    return f"""
<div style="text-align:center; margin: 16px 0;">
    <span class="g4-verdict {css_class}">
        <span>{icon}</span>
        <span>{verdict_kr} ({verdict})</span>
    </span>
</div>
"""


def wrap_full_html(title: str, date_str: str, body_html: str,
                   report_type: str = "Daily",
                   subtitle: str = "",
                   generated_at: str = "") -> str:
    """Wrap body sections into a complete HTML document with premium styling."""
    css = get_premium_css()
    js = get_premium_js()
    header = build_header(title, date_str, subtitle, report_type)
    footer = build_footer(generated_at)

    return f"""<!DOCTYPE html>
<html lang="ko">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>Q-TRON Gen4 {report_type} Report — {date_str}</title>
<style>
{css}
</style>
<script>
{js}
</script>
</head>
<body>
{header}
<div class="container">
{body_html}
</div>
{footer}
</body>
</html>"""
