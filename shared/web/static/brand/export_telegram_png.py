# -*- coding: utf-8 -*-
"""Export the Q-TRON Telegram avatar as 512×512 PNG.

cairosvg can't run on this Windows venv (libcairo not bundled), so the
mark is drawn with PIL instead. The geometry mirrors qtron-icon-telegram.svg
1:1 — same dimensions, same gradient stops, same stroke widths. If the
SVG is updated, update the constants here too (or replace this with a
proper SVG renderer once cairo is available).

Usage:
    python shared/web/static/brand/export_telegram_png.py

Output:
    shared/web/static/brand/qtron-icon-telegram.png  (512×512)
"""
from __future__ import annotations

from pathlib import Path
from PIL import Image, ImageDraw

OUT_PATH = Path(__file__).resolve().parent / "qtron-icon-telegram.png"
SIZE = 512


def lerp(a: tuple, b: tuple, t: float) -> tuple:
    return tuple(int(a[i] + (b[i] - a[i]) * t) for i in range(len(a)))


def make_diagonal_gradient(size: int, start_rgb, end_rgb) -> Image.Image:
    """Linear-gradient image (top-left → bottom-right)."""
    img = Image.new("RGB", (size, size), start_rgb)
    px = img.load()
    for y in range(size):
        for x in range(size):
            t = (x + y) / (2.0 * (size - 1))
            px[x, y] = lerp(start_rgb, end_rgb, t)
    return img


def main() -> None:
    # Background — deep blue radial-ish (PIL: linear gradient is acceptable).
    bg = make_diagonal_gradient(SIZE, (30, 64, 175), (12, 30, 96))
    img = Image.new("RGBA", (SIZE, SIZE), (0, 0, 0, 0))

    # Round mask
    mask = Image.new("L", (SIZE, SIZE), 0)
    ImageDraw.Draw(mask).ellipse((0, 0, SIZE - 1, SIZE - 1), fill=255)
    img.paste(bg, (0, 0), mask=mask)

    draw = ImageDraw.Draw(img, "RGBA")

    # Mark gradient stops (sky-300 → blue-400-ish — Telegram circle crops, so
    # we can use brighter colors than the dark-bg wordmark gradient)
    mark = (125, 211, 252, 255)  # #7dd3fc

    # Translate the SVG group transform translate(140 130) — we work in the
    # 512 frame directly with offsets matching qtron-icon-telegram.svg.
    OX, OY = 140, 130

    # Q outer ring — circle at (115,115) radius 90, stroke 28
    draw.ellipse(
        (OX + 115 - 90, OY + 115 - 90, OX + 115 + 90, OY + 115 + 90),
        outline=mark, width=28,
    )

    # Q tail diagonal: (165,167) → (205,207) stroke 28
    draw.line(
        (OX + 165, OY + 167, OX + 205, OY + 207),
        fill=mark, width=28, joint="curve",
    )

    # 3 circuit traces (stroke 13)
    for x1, y1, x2, y2 in [
        (175, 180, 240, 180),
        (185, 200, 245, 200),
        (195, 220, 230, 220),
    ]:
        draw.line(
            (OX + x1, OY + y1, OX + x2, OY + y2),
            fill=mark, width=13,
        )

    # Terminator dots (radius 8)
    for cx, cy in [(240, 180), (245, 200), (230, 220)]:
        draw.ellipse(
            (OX + cx - 8, OY + cy - 8, OX + cx + 8, OY + cy + 8),
            fill=mark,
        )

    img.save(OUT_PATH, "PNG")
    print(f"wrote {OUT_PATH}  ({OUT_PATH.stat().st_size:,} bytes)")


if __name__ == "__main__":
    main()
