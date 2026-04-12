"""
Q-TRON Icon Generator — 3 Concepts x 4 Sizes
==============================================
Concept A: Minimal Tech (dark + neon accent)
Concept B: Bold Graphic (color blocks + circuit motif)
Concept C: Financial Pro (navy/gold/white, chart symbol)

Outputs:
  icons/a_gui_256.png, a_exe.ico, a_telegram_512.png, a_favicon_32.png
  icons/b_gui_256.png, b_exe.ico, b_telegram_512.png, b_favicon_32.png
  icons/c_gui_256.png, c_exe.ico, c_telegram_512.png, c_favicon_32.png
"""
from __future__ import annotations
import math
from pathlib import Path
from PIL import Image, ImageDraw, ImageFont

OUT = Path(__file__).parent / "icons"
OUT.mkdir(exist_ok=True)


def _get_font(size: int):
    """Try system fonts, fallback to default."""
    for name in [
        "C:/Windows/Fonts/consola.ttf",    # Consolas
        "C:/Windows/Fonts/seguisb.ttf",    # Segoe UI Semibold
        "C:/Windows/Fonts/arialbd.ttf",    # Arial Bold
        "C:/Windows/Fonts/arial.ttf",
    ]:
        try:
            return ImageFont.truetype(name, size)
        except Exception:
            continue
    return ImageFont.load_default()


def _draw_rounded_rect(draw, xy, radius, fill):
    """Draw rounded rectangle."""
    x0, y0, x1, y1 = xy
    r = radius
    draw.rectangle([x0 + r, y0, x1 - r, y1], fill=fill)
    draw.rectangle([x0, y0 + r, x1, y1 - r], fill=fill)
    draw.pieslice([x0, y0, x0 + 2*r, y0 + 2*r], 180, 270, fill=fill)
    draw.pieslice([x1 - 2*r, y0, x1, y0 + 2*r], 270, 360, fill=fill)
    draw.pieslice([x0, y1 - 2*r, x0 + 2*r, y1], 90, 180, fill=fill)
    draw.pieslice([x1 - 2*r, y1 - 2*r, x1, y1], 0, 90, fill=fill)


def _draw_chart_line(draw, bbox, color, width=3):
    """Draw a stylized uptrend chart line."""
    x0, y0, x1, y1 = bbox
    w = x1 - x0
    h = y1 - y0
    points = [
        (x0, y0 + h * 0.8),
        (x0 + w * 0.2, y0 + h * 0.6),
        (x0 + w * 0.35, y0 + h * 0.7),
        (x0 + w * 0.55, y0 + h * 0.3),
        (x0 + w * 0.75, y0 + h * 0.4),
        (x1, y0 + h * 0.1),
    ]
    draw.line(points, fill=color, width=width, joint="curve")


def _draw_circuit_lines(draw, size, color, count=6):
    """Draw circuit-board style lines."""
    s = size
    for i in range(count):
        y = int(s * (0.15 + i * 0.12))
        x_start = int(s * 0.05)
        x_mid = int(s * (0.1 + (i % 3) * 0.08))
        x_end = int(s * (0.25 + (i % 2) * 0.1))
        draw.line([(x_start, y), (x_mid, y), (x_mid, y + int(s*0.04)),
                    (x_end, y + int(s*0.04))],
                   fill=color, width=max(1, s // 128))
        # dot at end
        r = max(1, s // 100)
        draw.ellipse([x_end - r, y + int(s*0.04) - r,
                       x_end + r, y + int(s*0.04) + r], fill=color)


def _draw_grid_dots(draw, size, color, spacing=20, radius=1):
    """Draw subtle grid dots."""
    for x in range(spacing, size, spacing):
        for y in range(spacing, size, spacing):
            draw.ellipse([x-radius, y-radius, x+radius, y+radius],
                          fill=color)


# ─────────────────────────────────────────────────────────────────────────────
# Concept A: Minimal Tech — Dark bg + Neon Green/Cyan accent
# ─────────────────────────────────────────────────────────────────────────────

def make_concept_a(size: int) -> Image.Image:
    bg = (18, 18, 24)
    accent = (0, 230, 180)       # neon cyan-green
    accent_dim = (0, 150, 120)
    text_color = (240, 240, 245)

    img = Image.new("RGBA", (size, size), (0, 0, 0, 0))
    draw = ImageDraw.Draw(img)

    # Background: rounded dark rectangle
    margin = size // 16
    _draw_rounded_rect(draw, (margin, margin, size - margin, size - margin),
                        size // 8, bg)

    # Subtle grid dots
    _draw_grid_dots(draw, size, (40, 40, 50), spacing=max(8, size // 20),
                     radius=max(1, size // 200))

    # "Q" letter — large, left-aligned
    q_size = int(size * 0.52)
    font_q = _get_font(q_size)
    q_x = int(size * 0.12)
    q_y = int(size * 0.18)
    draw.text((q_x, q_y), "Q", fill=accent, font=font_q)

    # Chart line overlay (top-right area)
    chart_box = (int(size * 0.45), int(size * 0.15),
                  int(size * 0.88), int(size * 0.45))
    _draw_chart_line(draw, chart_box, accent_dim, width=max(2, size // 80))

    # "TRON" text — smaller, bottom
    tron_size = int(size * 0.14)
    font_t = _get_font(tron_size)
    bbox_t = draw.textbbox((0, 0), "TRON", font=font_t)
    tw = bbox_t[2] - bbox_t[0]
    tx = (size - tw) // 2
    ty = int(size * 0.78)
    draw.text((tx, ty), "TRON", fill=text_color, font=font_t)

    # Bottom accent line
    line_y = int(size * 0.93)
    draw.line([(int(size * 0.2), line_y), (int(size * 0.8), line_y)],
               fill=accent, width=max(2, size // 100))

    return img


# ─────────────────────────────────────────────────────────────────────────────
# Concept B: Bold Graphic — Vibrant blocks + circuit motif
# ─────────────────────────────────────────────────────────────────────────────

def make_concept_b(size: int) -> Image.Image:
    bg_dark = (25, 25, 35)
    block_blue = (30, 100, 220)
    block_purple = (120, 50, 200)
    accent_yellow = (255, 210, 0)
    text_white = (255, 255, 255)

    img = Image.new("RGBA", (size, size), (0, 0, 0, 0))
    draw = ImageDraw.Draw(img)

    # Background
    margin = size // 16
    _draw_rounded_rect(draw, (margin, margin, size - margin, size - margin),
                        size // 8, bg_dark)

    # Color blocks (diagonal split)
    # Top-left triangle: blue
    tri_points = [
        (margin, margin + size // 8),
        (size // 2, margin + size // 8),
        (margin, size // 2),
    ]
    draw.polygon(tri_points, fill=block_blue)

    # Bottom-right triangle: purple
    tri_points2 = [
        (size // 2, size - margin - size // 8),
        (size - margin, size // 2),
        (size - margin, size - margin - size // 8),
    ]
    draw.polygon(tri_points2, fill=block_purple)

    # Circuit lines
    _draw_circuit_lines(draw, size, (60, 60, 80), count=5)

    # "QT" bold center
    qt_size = int(size * 0.42)
    font_qt = _get_font(qt_size)
    bbox_qt = draw.textbbox((0, 0), "QT", font=font_qt)
    qtw = bbox_qt[2] - bbox_qt[0]
    qth = bbox_qt[3] - bbox_qt[1]
    qx = (size - qtw) // 2
    qy = (size - qth) // 2 - int(size * 0.05)
    draw.text((qx, qy), "QT", fill=accent_yellow, font=font_qt)

    # Underline dot pattern
    dot_y = qy + qth + int(size * 0.06)
    for i in range(5):
        dx = int(size * 0.3) + i * int(size * 0.1)
        r = max(2, size // 80)
        c = accent_yellow if i % 2 == 0 else text_white
        draw.ellipse([dx - r, dot_y - r, dx + r, dot_y + r], fill=c)

    return img


# ─────────────────────────────────────────────────────────────────────────────
# Concept C: Financial Pro — Navy/Gold/White, chart + Q
# ─────────────────────────────────────────────────────────────────────────────

def make_concept_c(size: int) -> Image.Image:
    navy = (15, 30, 65)
    gold = (212, 175, 55)
    gold_light = (240, 210, 100)
    white = (250, 250, 252)

    img = Image.new("RGBA", (size, size), (0, 0, 0, 0))
    draw = ImageDraw.Draw(img)

    # Background: navy rounded rect
    margin = size // 16
    _draw_rounded_rect(draw, (margin, margin, size - margin, size - margin),
                        size // 8, navy)

    # Gold border (inner)
    bw = max(2, size // 64)
    inner_m = margin + int(size * 0.04)
    _draw_rounded_rect(draw, (inner_m, inner_m,
                               size - inner_m, size - inner_m),
                        size // 10, None)
    # Draw border by overlapping two rounded rects
    for offset in range(bw):
        m = inner_m + offset
        draw.rounded_rectangle(
            [m, m, size - m, size - m],
            radius=size // 10,
            outline=gold,
            width=1,
        )

    # "Q" — elegant, gold, centered upper
    q_size = int(size * 0.45)
    font_q = _get_font(q_size)
    bbox_q = draw.textbbox((0, 0), "Q", font=font_q)
    qw = bbox_q[2] - bbox_q[0]
    qh = bbox_q[3] - bbox_q[1]
    qx = (size - qw) // 2
    qy = int(size * 0.12)
    draw.text((qx, qy), "Q", fill=gold, font=font_q)

    # Chart line — below Q, gold
    chart_box = (int(size * 0.18), int(size * 0.58),
                  int(size * 0.82), int(size * 0.78))
    _draw_chart_line(draw, chart_box, gold_light, width=max(2, size // 80))

    # Horizontal axis line
    ax_y = int(size * 0.80)
    draw.line([(int(size * 0.18), ax_y), (int(size * 0.82), ax_y)],
               fill=gold, width=max(1, size // 128))

    # "TRON" small text
    tron_size = int(size * 0.10)
    font_t = _get_font(tron_size)
    bbox_t = draw.textbbox((0, 0), "TRON", font=font_t)
    tw = bbox_t[2] - bbox_t[0]
    tx = (size - tw) // 2
    ty = int(size * 0.85)
    draw.text((tx, ty), "TRON", fill=white, font=font_t)

    return img


# ─────────────────────────────────────────────────────────────────────────────
# Generate all sizes
# ─────────────────────────────────────────────────────────────────────────────

def save_set(prefix: str, maker):
    """Generate GUI(256), Telegram(512), Favicon(32), EXE(.ico)."""
    # GUI 256px
    img256 = maker(256)
    img256.save(OUT / f"{prefix}_gui_256.png")

    # Telegram 512px
    img512 = maker(512)
    img512.save(OUT / f"{prefix}_telegram_512.png")

    # Favicon 32px
    img32 = maker(32)
    img32.save(OUT / f"{prefix}_favicon_32.png")

    # REST favicon 48px
    img48 = maker(48)
    img48.save(OUT / f"{prefix}_rest_48.png")

    # EXE .ico (multi-size: 16, 32, 48, 256)
    sizes_ico = [16, 32, 48, 256]
    ico_images = [maker(s) for s in sizes_ico]
    ico_images[0].save(
        OUT / f"{prefix}_exe.ico",
        format="ICO",
        sizes=[(s, s) for s in sizes_ico],
        append_images=ico_images[1:],
    )

    print(f"  [{prefix.upper()}] gui_256 + telegram_512 + favicon_32 + rest_48 + exe.ico")


if __name__ == "__main__":
    print("Generating Q-TRON icon sets...")
    save_set("a_minimal_tech", make_concept_a)
    save_set("b_bold_graphic", make_concept_b)
    save_set("c_financial_pro", make_concept_c)
    print(f"\nDone! {len(list(OUT.glob('*')))} files in {OUT}")
