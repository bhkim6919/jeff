"""
GUI Widget Reference PDF Generator
Q-TRON Gen4 - GUI 위젯 용어 가이드
"""
from reportlab.lib.pagesizes import A4
from reportlab.lib.units import mm, cm
from reportlab.lib.colors import (
    HexColor, black, white, grey, lightgrey, darkgrey
)
from reportlab.pdfgen import canvas
from reportlab.lib.enums import TA_LEFT, TA_CENTER
from reportlab.pdfbase import pdfmetrics
from reportlab.pdfbase.ttfonts import TTFont
import os

# ── Font Registration ──────────────────────────────────────
FONT_DIR = "C:/Windows/Fonts"
pdfmetrics.registerFont(TTFont("Malgun", os.path.join(FONT_DIR, "malgun.ttf")))
pdfmetrics.registerFont(TTFont("MalgunBd", os.path.join(FONT_DIR, "malgunbd.ttf")))

# ── Colors ─────────────────────────────────────────────────
BG_WHITE = white
TITLE_BG = HexColor("#1a1a2e")
TITLE_FG = white
SECTION_BG = HexColor("#16213e")
CARD_BG = HexColor("#f8f9fa")
CARD_BORDER = HexColor("#dee2e6")
ACCENT = HexColor("#0d6efd")
ACCENT_GREEN = HexColor("#198754")
ACCENT_ORANGE = HexColor("#fd7e14")
TEXT_DARK = HexColor("#212529")
TEXT_GREY = HexColor("#6c757d")
LIGHT_BLUE = HexColor("#e7f1ff")

OUT = r"C:\Q-TRON-32_ARCHIVE\docs\GUI_Widget_Reference.pdf"


def draw_card(c, x, y, w, h, title_kr, title_en, desc, draw_widget_fn):
    """Draw a single widget card with border, title, widget example, description."""
    # Card background
    c.setFillColor(CARD_BG)
    c.setStrokeColor(CARD_BORDER)
    c.setLineWidth(0.5)
    c.roundRect(x, y, w, h, 3, fill=1, stroke=1)

    # Title bar
    c.setFillColor(ACCENT)
    c.roundRect(x, y + h - 18, w, 18, 3, fill=1, stroke=0)
    # Cover bottom corners of title bar
    c.rect(x, y + h - 18, w, 10, fill=1, stroke=0)

    # Title text
    c.setFillColor(white)
    c.setFont("MalgunBd", 8)
    c.drawString(x + 5, y + h - 14, f"{title_kr}")
    c.setFont("Malgun", 6.5)
    c.drawRightString(x + w - 5, y + h - 14, f"{title_en}")

    # Widget example area
    widget_area_y = y + 22
    widget_area_h = h - 45
    c.setFillColor(white)
    c.setStrokeColor(HexColor("#e9ecef"))
    c.setLineWidth(0.3)
    c.rect(x + 5, widget_area_y, w - 10, widget_area_h, fill=1, stroke=1)

    # Draw the widget
    draw_widget_fn(c, x + 5, widget_area_y, w - 10, widget_area_h)

    # Description
    c.setFillColor(TEXT_GREY)
    c.setFont("Malgun", 5.5)
    # Word wrap description
    max_chars = int((w - 10) / 3.2)
    lines = []
    while len(desc) > max_chars:
        cut = desc[:max_chars].rfind(' ')
        if cut <= 0:
            cut = max_chars
        lines.append(desc[:cut])
        desc = desc[cut:].strip()
    lines.append(desc)
    for i, line in enumerate(lines[:2]):
        c.drawString(x + 5, y + 14 - i * 8, line)


# ── Widget Drawing Functions ───────────────────────────────

def draw_button(c, x, y, w, h):
    cx, cy = x + w/2, y + h/2
    # Primary button
    c.setFillColor(ACCENT)
    c.roundRect(cx - 35, cy + 2, 70, 18, 3, fill=1, stroke=0)
    c.setFillColor(white)
    c.setFont("MalgunBd", 7)
    c.drawCentredString(cx, cy + 7, "확인")
    # Secondary button
    c.setFillColor(white)
    c.setStrokeColor(ACCENT)
    c.setLineWidth(0.8)
    c.roundRect(cx - 35, cy - 20, 70, 18, 3, fill=1, stroke=1)
    c.setFillColor(ACCENT)
    c.setFont("Malgun", 7)
    c.drawCentredString(cx, cy - 15, "취소")


def draw_radio(c, x, y, w, h):
    cx, cy = x + 15, y + h/2 + 8
    # Selected
    c.setStrokeColor(ACCENT)
    c.setLineWidth(0.8)
    c.circle(cx, cy, 5, fill=0, stroke=1)
    c.setFillColor(ACCENT)
    c.circle(cx, cy, 2.5, fill=1, stroke=0)
    c.setFillColor(TEXT_DARK)
    c.setFont("Malgun", 7)
    c.drawString(cx + 9, cy - 3, "전액 재투자")
    # Unselected
    cy2 = cy - 16
    c.setStrokeColor(darkgrey)
    c.circle(cx, cy2, 5, fill=0, stroke=1)
    c.setFillColor(TEXT_DARK)
    c.drawString(cx + 9, cy2 - 3, "수익 현금보유")


def draw_checkbox(c, x, y, w, h):
    cx, cy = x + 12, y + h/2 + 8
    # Checked
    c.setFillColor(ACCENT)
    c.roundRect(cx - 5, cy - 5, 10, 10, 1.5, fill=1, stroke=0)
    c.setStrokeColor(white)
    c.setLineWidth(1.2)
    p = c.beginPath()
    p.moveTo(cx - 3, cy - 0.5)
    p.lineTo(cx - 0.5, cy - 3)
    p.lineTo(cx + 3.5, cy + 3)
    c.drawPath(p, fill=0, stroke=1)
    c.setFillColor(TEXT_DARK)
    c.setFont("Malgun", 7)
    c.drawString(cx + 9, cy - 3, "트레일 스톱")
    # Unchecked
    cy2 = cy - 16
    c.setFillColor(white)
    c.setStrokeColor(darkgrey)
    c.setLineWidth(0.6)
    c.roundRect(cx - 5, cy2 - 5, 10, 10, 1.5, fill=1, stroke=1)
    c.setFillColor(TEXT_DARK)
    c.drawString(cx + 9, cy2 - 3, "Emergency Rebal")


def draw_dropdown(c, x, y, w, h):
    cx, cy = x + w/2, y + h/2
    bw = min(w - 16, 100)
    # Dropdown box
    c.setFillColor(white)
    c.setStrokeColor(CARD_BORDER)
    c.setLineWidth(0.6)
    c.roundRect(cx - bw/2, cy - 8, bw, 18, 2, fill=1, stroke=1)
    c.setFillColor(TEXT_DARK)
    c.setFont("Malgun", 7)
    c.drawString(cx - bw/2 + 6, cy - 3, "PAPER (모의투자)")
    # Arrow
    c.setFillColor(TEXT_GREY)
    p = c.beginPath()
    ax = cx + bw/2 - 12
    p.moveTo(ax - 3, cy + 1)
    p.lineTo(ax + 3, cy + 1)
    p.lineTo(ax, cy - 3)
    p.close()
    c.drawPath(p, fill=1, stroke=0)


def draw_textinput(c, x, y, w, h):
    cx, cy = x + w/2, y + h/2
    bw = min(w - 16, 100)
    # Label
    c.setFillColor(TEXT_DARK)
    c.setFont("Malgun", 6.5)
    c.drawString(cx - bw/2, cy + 12, "투입 금액 (원)")
    # Input box
    c.setFillColor(white)
    c.setStrokeColor(ACCENT)
    c.setLineWidth(0.8)
    c.roundRect(cx - bw/2, cy - 8, bw, 16, 2, fill=1, stroke=1)
    c.setFillColor(TEXT_DARK)
    c.setFont("Malgun", 7)
    c.drawString(cx - bw/2 + 6, cy - 4, "10,000,000")
    # Cursor
    c.setStrokeColor(ACCENT)
    c.setLineWidth(0.5)
    c.line(cx + 18, cy - 5, cx + 18, cy + 5)


def draw_slider(c, x, y, w, h):
    cx, cy = x + w/2, y + h/2
    sw = min(w - 20, 90)
    sx = cx - sw/2
    # Label
    c.setFillColor(TEXT_DARK)
    c.setFont("Malgun", 6.5)
    c.drawString(sx, cy + 14, "Exposure: 70%")
    # Track
    c.setFillColor(HexColor("#e9ecef"))
    c.roundRect(sx, cy - 2, sw, 4, 2, fill=1, stroke=0)
    # Filled portion (70%)
    c.setFillColor(ACCENT)
    c.roundRect(sx, cy - 2, sw * 0.7, 4, 2, fill=1, stroke=0)
    # Thumb
    c.setFillColor(white)
    c.setStrokeColor(ACCENT)
    c.setLineWidth(1)
    c.circle(sx + sw * 0.7, cy, 5, fill=1, stroke=1)
    # Min/Max labels
    c.setFillColor(TEXT_GREY)
    c.setFont("Malgun", 5)
    c.drawString(sx, cy - 12, "0%")
    c.drawRightString(sx + sw, cy - 12, "100%")


def draw_toggle(c, x, y, w, h):
    cx, cy = x + w/2, y + h/2
    # ON toggle
    ty = cy + 6
    c.setFillColor(ACCENT_GREEN)
    c.roundRect(cx - 30, ty - 7, 28, 14, 7, fill=1, stroke=0)
    c.setFillColor(white)
    c.circle(cx - 30 + 21, ty, 5, fill=1, stroke=0)
    c.setFillColor(TEXT_DARK)
    c.setFont("Malgun", 7)
    c.drawString(cx + 2, ty - 3, "자동 리밸런스 ON")
    # OFF toggle
    ty2 = cy - 10
    c.setFillColor(darkgrey)
    c.roundRect(cx - 30, ty2 - 7, 28, 14, 7, fill=1, stroke=0)
    c.setFillColor(white)
    c.circle(cx - 30 + 7, ty2, 5, fill=1, stroke=0)
    c.setFillColor(TEXT_GREY)
    c.setFont("Malgun", 7)
    c.drawString(cx + 2, ty2 - 3, "Emergency Rebal OFF")


def draw_tooltip(c, x, y, w, h):
    cx, cy = x + w/2, y + h/2
    # Button
    c.setFillColor(ACCENT)
    c.roundRect(cx - 25, cy - 12, 50, 16, 3, fill=1, stroke=0)
    c.setFillColor(white)
    c.setFont("MalgunBd", 7)
    c.drawCentredString(cx, cy - 7, "MDD")
    # Tooltip bubble
    c.setFillColor(HexColor("#333333"))
    c.roundRect(cx - 45, cy + 8, 90, 20, 3, fill=1, stroke=0)
    # Arrow
    p = c.beginPath()
    p.moveTo(cx - 5, cy + 8)
    p.lineTo(cx + 5, cy + 8)
    p.lineTo(cx, cy + 3)
    p.close()
    c.drawPath(p, fill=1, stroke=0)
    c.setFillColor(white)
    c.setFont("Malgun", 6)
    c.drawCentredString(cx, cy + 14, "Maximum Drawdown")
    c.drawCentredString(cx, cy + 22, "최대 낙폭 (-24%)")


def draw_progressbar(c, x, y, w, h):
    cx, cy = x + w/2, y + h/2
    pw = min(w - 20, 90)
    px = cx - pw/2
    # Label
    c.setFillColor(TEXT_DARK)
    c.setFont("Malgun", 6.5)
    c.drawString(px, cy + 10, "리밸런스 진행: 15/20")
    # Track
    c.setFillColor(HexColor("#e9ecef"))
    c.roundRect(px, cy - 4, pw, 8, 4, fill=1, stroke=0)
    # Fill (75%)
    c.setFillColor(ACCENT_GREEN)
    c.roundRect(px, cy - 4, pw * 0.75, 8, 4, fill=1, stroke=0)
    # Percentage
    c.setFillColor(TEXT_DARK)
    c.setFont("MalgunBd", 6)
    c.drawRightString(px + pw, cy - 10, "75%")


def draw_tabs(c, x, y, w, h):
    cx, cy = x + w/2, y + h/2
    tw = min(w - 10, 110)
    tx = cx - tw/2
    tab_w = tw / 3
    # Active tab
    c.setFillColor(white)
    c.setStrokeColor(ACCENT)
    c.setLineWidth(0.5)
    c.roundRect(tx, cy, tab_w, 16, 2, fill=1, stroke=1)
    c.setFillColor(ACCENT)
    c.rect(tx, cy, tab_w, 2, fill=1, stroke=0)  # bottom accent
    c.setFont("MalgunBd", 6.5)
    c.drawCentredString(tx + tab_w/2, cy + 5, "포트폴리오")
    # Inactive tabs
    for i, label in enumerate(["모니터링", "설정"], 1):
        c.setFillColor(HexColor("#f1f3f5"))
        c.roundRect(tx + tab_w * i, cy, tab_w, 16, 2, fill=1, stroke=0)
        c.setFillColor(TEXT_GREY)
        c.setFont("Malgun", 6.5)
        c.drawCentredString(tx + tab_w * i + tab_w/2, cy + 5, label)
    # Content area
    c.setFillColor(white)
    c.setStrokeColor(CARD_BORDER)
    c.rect(tx, cy - 18, tw, 18, fill=1, stroke=1)
    c.setFillColor(TEXT_GREY)
    c.setFont("Malgun", 6)
    c.drawString(tx + 5, cy - 13, "Tab content area...")


def draw_table(c, x, y, w, h):
    tw = min(w - 8, 115)
    tx = x + (w - tw) / 2
    ty = y + h - 6
    cols = [0, 30, 65, 90, tw]
    rows_data = [
        ("종목", "수량", "수익률", "상태"),
        ("삼성전자", "100", "+3.2%", "보유"),
        ("SK하이닉스", "50", "-1.1%", "보유"),
    ]
    rh = 11
    for ri, row in enumerate(rows_data):
        ry = ty - (ri + 1) * rh
        if ri == 0:
            c.setFillColor(HexColor("#e7f1ff"))
        else:
            c.setFillColor(white if ri % 2 == 1 else HexColor("#f8f9fa"))
        c.rect(tx, ry, tw, rh, fill=1, stroke=0)
        c.setStrokeColor(HexColor("#dee2e6"))
        c.setLineWidth(0.3)
        c.line(tx, ry, tx + tw, ry)
        c.setFillColor(TEXT_DARK if ri == 0 else TEXT_DARK)
        c.setFont("MalgunBd" if ri == 0 else "Malgun", 5.5)
        for ci, val in enumerate(row):
            c.drawString(tx + cols[ci] + 3, ry + 3, val)


def draw_dialog(c, x, y, w, h):
    # Dimmed background
    c.setFillColor(HexColor("#00000033"))
    c.rect(x, y, w, h, fill=1, stroke=0)
    # Dialog box
    dw, dh = min(w - 12, 95), min(h - 8, 42)
    dx = x + (w - dw) / 2
    dy = y + (h - dh) / 2
    c.setFillColor(white)
    c.setStrokeColor(CARD_BORDER)
    c.setLineWidth(0.5)
    c.roundRect(dx, dy, dw, dh, 4, fill=1, stroke=1)
    # Title
    c.setFillColor(TEXT_DARK)
    c.setFont("MalgunBd", 7)
    c.drawString(dx + 6, dy + dh - 12, "청산 확인")
    # Message
    c.setFillColor(TEXT_GREY)
    c.setFont("Malgun", 6)
    c.drawString(dx + 6, dy + dh - 23, "전종목을 청산하시겠습니까?")
    # Buttons
    bw = 30
    c.setFillColor(HexColor("#dc3545"))
    c.roundRect(dx + dw - bw * 2 - 12, dy + 4, bw, 12, 2, fill=1, stroke=0)
    c.setFillColor(white)
    c.setFont("Malgun", 6)
    c.drawCentredString(dx + dw - bw * 2 - 12 + bw/2, dy + 7, "확인")
    c.setFillColor(HexColor("#6c757d"))
    c.roundRect(dx + dw - bw - 6, dy + 4, bw, 12, 2, fill=1, stroke=0)
    c.setFillColor(white)
    c.drawCentredString(dx + dw - bw - 6 + bw/2, dy + 7, "취소")


def draw_spinbox(c, x, y, w, h):
    cx, cy = x + w/2, y + h/2
    # Label
    c.setFillColor(TEXT_DARK)
    c.setFont("Malgun", 6.5)
    c.drawCentredString(cx, cy + 14, "리밸런스 주기 (일)")
    # Box
    bw = 60
    c.setFillColor(white)
    c.setStrokeColor(CARD_BORDER)
    c.setLineWidth(0.6)
    c.rect(cx - bw/2, cy - 8, bw - 16, 16, fill=1, stroke=1)
    c.setFillColor(TEXT_DARK)
    c.setFont("MalgunBd", 8)
    c.drawCentredString(cx - 8, cy - 3, "21")
    # Up/down buttons
    bx = cx + bw/2 - 16
    c.setFillColor(HexColor("#e9ecef"))
    c.rect(bx, cy, 16, 8, fill=1, stroke=1)
    c.rect(bx, cy - 8, 16, 8, fill=1, stroke=1)
    c.setFillColor(TEXT_DARK)
    c.setFont("Malgun", 6)
    c.drawCentredString(bx + 8, cy + 2, "+")
    c.drawCentredString(bx + 8, cy - 6, "-")


def draw_label(c, x, y, w, h):
    cx, cy = x + w/2, y + h/2
    # Title label
    c.setFillColor(TEXT_DARK)
    c.setFont("MalgunBd", 9)
    c.drawCentredString(cx, cy + 10, "Gen4 Base")
    # Subtitle
    c.setFillColor(TEXT_GREY)
    c.setFont("Malgun", 6.5)
    c.drawCentredString(cx, cy - 2, "PAPER Trading Mode")
    # Badge
    c.setFillColor(ACCENT_GREEN)
    c.roundRect(cx - 15, cy - 16, 30, 10, 3, fill=1, stroke=0)
    c.setFillColor(white)
    c.setFont("MalgunBd", 5.5)
    c.drawCentredString(cx, cy - 13, "NORMAL")


def draw_statusbar(c, x, y, w, h):
    cy = y + h/2
    # Status bar background
    c.setFillColor(HexColor("#343a40"))
    c.rect(x, cy - 8, w, 16, fill=1, stroke=0)
    c.setFillColor(ACCENT_GREEN)
    c.circle(x + 8, cy, 3, fill=1, stroke=0)
    c.setFillColor(white)
    c.setFont("Malgun", 5.5)
    c.drawString(x + 14, cy - 2, "CONNECTED")
    c.setFillColor(HexColor("#adb5bd"))
    c.drawRightString(x + w - 4, cy - 2, "PAPER | 20pos | +3.5%")


def draw_menubar(c, x, y, w, h):
    cy = y + h/2 + 5
    # Menu bar
    c.setFillColor(HexColor("#f8f9fa"))
    c.rect(x, cy - 2, w, 14, fill=1, stroke=0)
    c.setStrokeColor(CARD_BORDER)
    c.setLineWidth(0.3)
    c.line(x, cy - 2, x + w, cy - 2)
    menus = ["파일", "실행", "설정", "도움말"]
    mx = x + 5
    for m in menus:
        c.setFillColor(TEXT_DARK)
        c.setFont("Malgun", 6.5)
        c.drawString(mx, cy + 2, m)
        mx += 28
    # Dropdown example
    c.setFillColor(white)
    c.setStrokeColor(CARD_BORDER)
    c.roundRect(x + 28, cy - 30, 50, 28, 2, fill=1, stroke=1)
    items = ["시작", "중지", "리밸런스"]
    for i, item in enumerate(items):
        iy = cy - 6 - i * 9
        if i == 0:
            c.setFillColor(LIGHT_BLUE)
            c.rect(x + 29, iy - 3, 48, 9, fill=1, stroke=0)
        c.setFillColor(TEXT_DARK)
        c.setFont("Malgun", 6)
        c.drawString(x + 33, iy - 1, item)


def draw_toolbar(c, x, y, w, h):
    cx, cy = x + w/2, y + h/2
    tw = min(w - 8, 110)
    tx = x + (w - tw) / 2
    # Toolbar background
    c.setFillColor(HexColor("#f1f3f5"))
    c.roundRect(tx, cy - 8, tw, 18, 2, fill=1, stroke=0)
    # Tool buttons
    icons = [(">>", ACCENT_GREEN, "시작"),
             ("||", ACCENT_ORANGE, "중지"),
             ("R", ACCENT, "리밸"),
             ("C", HexColor("#dc3545"), "청산")]
    bx = tx + 4
    for icon, color, label in icons:
        c.setFillColor(color)
        c.roundRect(bx, cy - 5, 20, 12, 2, fill=1, stroke=0)
        c.setFillColor(white)
        c.setFont("MalgunBd", 6)
        c.drawCentredString(bx + 10, cy - 2, icon)
        bx += 24
    # Separator
    c.setStrokeColor(CARD_BORDER)
    c.setLineWidth(0.5)
    c.line(bx, cy - 5, bx, cy + 7)


# ── Main PDF Generation ───────────────────────────────────

def generate_pdf():
    W, H = A4
    c_pdf = canvas.Canvas(OUT, pagesize=A4)

    # ── Page Title ─────────────────────────────────────────
    c_pdf.setFillColor(TITLE_BG)
    c_pdf.rect(0, H - 45, W, 45, fill=1, stroke=0)
    c_pdf.setFillColor(TITLE_FG)
    c_pdf.setFont("MalgunBd", 16)
    c_pdf.drawCentredString(W/2, H - 28, "GUI Widget Reference")
    c_pdf.setFont("Malgun", 9)
    c_pdf.setFillColor(HexColor("#adb5bd"))
    c_pdf.drawCentredString(W/2, H - 40, "Q-TRON Gen4  |  PyQt5 GUI 위젯 용어 가이드  |  각 위젯의 이름 / 예시 / 설명")

    # ── Widget Cards Grid ──────────────────────────────────
    widgets = [
        ("버튼", "Button", "클릭하여 동작 실행. 확인/취소/매수/매도 등", draw_button),
        ("라디오 버튼", "Radio Button", "여러 옵션 중 하나만 선택 (상호 배타적)", draw_radio),
        ("체크박스", "Checkbox", "여러 옵션을 독립적으로 ON/OFF 선택", draw_checkbox),
        ("드롭다운", "Dropdown/ComboBox", "목록에서 하나를 선택. 공간 절약에 유리", draw_dropdown),
        ("텍스트 입력", "Text Input", "사용자가 직접 값을 입력하는 필드", draw_textinput),
        ("슬라이더", "Slider", "범위 내 값을 드래그로 조절 (비율/수치)", draw_slider),
        ("토글 스위치", "Toggle Switch", "ON/OFF 두 상태 전환. 설정 활성화에 사용", draw_toggle),
        ("툴팁", "Tooltip", "마우스 올리면 나타나는 설명 말풍선", draw_tooltip),
        ("진행 표시줄", "Progress Bar", "작업 진행 상황을 시각적으로 표시", draw_progressbar),
        ("탭", "Tab", "같은 공간에서 화면을 전환하여 표시", draw_tabs),
        ("테이블/그리드", "Table / Grid", "데이터를 행/열로 정리하여 표시", draw_table),
        ("다이얼로그", "Dialog / Modal", "확인이 필요한 팝업 창. 중요 동작 전 사용", draw_dialog),
        ("스핀박스", "Spin Box", "+/- 버튼으로 숫자 값을 조절", draw_spinbox),
        ("레이블/배지", "Label / Badge", "텍스트 표시. 상태 표시 배지 포함", draw_label),
        ("상태 표시줄", "Status Bar", "화면 하단 연결/모드/수익률 등 상태 표시", draw_statusbar),
        ("메뉴 바", "Menu Bar", "상단 메뉴. 클릭 시 하위 메뉴 펼침", draw_menubar),
        ("도구 모음", "Toolbar", "자주 쓰는 기능을 아이콘 버튼으로 배치", draw_toolbar),
    ]

    # Grid layout: 3 columns
    margin_x = 20
    margin_top = H - 55
    cols = 3
    card_w = (W - margin_x * 2 - 10 * (cols - 1)) / cols
    card_h = 82
    gap_x = 10
    gap_y = 8

    for i, (kr, en, desc, fn) in enumerate(widgets):
        col = i % cols
        row = i // cols
        cx = margin_x + col * (card_w + gap_x)
        cy = margin_top - (row + 1) * (card_h + gap_y)

        if cy < 30:
            # New page if needed
            c_pdf.showPage()
            margin_top = H - 20
            row = 0
            cy = margin_top - (card_h + gap_y)

        draw_card(c_pdf, cx, cy, card_w, card_h, kr, en, desc, fn)

    # ── Footer ─────────────────────────────────────────────
    c_pdf.setFillColor(TEXT_GREY)
    c_pdf.setFont("Malgun", 6)
    c_pdf.drawCentredString(W/2, 12, "Q-TRON Gen4 GUI Widget Reference  |  PyQt5 기반  |  2026-03")

    c_pdf.save()
    print(f"[OK] PDF saved: {OUT}")


if __name__ == "__main__":
    generate_pdf()
