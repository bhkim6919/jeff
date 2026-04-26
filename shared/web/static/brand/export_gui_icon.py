# -*- coding: utf-8 -*-
"""Export the Q-TRON GUI icon as PNG (256×256) + multi-res ICO.

Mirrors qtron-icon-gui.svg using PIL geometry. Produces:
  shared/web/static/brand/qtron-icon-gui.png   (256×256)
  shared/web/static/brand/qtron-icon-gui.ico   (multi-res: 16/32/48/64/128/256)
  C:/Users/User/Desktop/Q-TRON.png             (copy for desktop)
  C:/Users/User/Desktop/Q-TRON.ico             (copy for desktop)

Usage:
    python shared/web/static/brand/export_gui_icon.py
"""
from __future__ import annotations

import shutil
from pathlib import Path
from PIL import Image, ImageDraw

BRAND_DIR = Path(__file__).resolve().parent
OUT_PNG = BRAND_DIR / "qtron-icon-gui.png"
OUT_ICO = BRAND_DIR / "qtron-icon-gui.ico"
DESKTOP = Path("C:/Users/User/Desktop")
SIZE = 256


def lerp(a, b, t):
    return tuple(int(a[i] + (b[i] - a[i]) * t) for i in range(len(a)))


def make_diagonal_gradient(size, start_rgb, end_rgb):
    img = Image.new("RGB", (size, size), start_rgb)
    px = img.load()
    for y in range(size):
        for x in range(size):
            t = (x + y) / (2.0 * (size - 1))
            px[x, y] = lerp(start_rgb, end_rgb, t)
    return img


def draw_gui_icon(size=SIZE):
    bg = make_diagonal_gradient(size, (29, 78, 216), (30, 58, 138))
    img = Image.new("RGBA", (size, size), (0, 0, 0, 0))
    radius = int(size * 0.156)
    mask = Image.new("L", (size, size), 0)
    ImageDraw.Draw(mask).rounded_rectangle(
        (0, 0, size - 1, size - 1), radius=radius, fill=255
    )
    img.paste(bg, (0, 0), mask=mask)

    draw = ImageDraw.Draw(img, "RGBA")
    scale = size / 256.0
    OX, OY = int(48 * scale), int(40 * scale)

    white = (255, 255, 255, 255)
    cyan = (125, 211, 252, 255)

    s = scale
    draw.ellipse(
        (OX + int(22 * s), OY + int(22 * s),
         OX + int(146 * s), OY + int(146 * s)),
        outline=white, width=max(2, int(20 * s)),
    )
    draw.line(
        (OX + int(120 * s), OY + int(124 * s),
         OX + int(148 * s), OY + int(152 * s)),
        fill=white, width=max(2, int(20 * s)),
    )
    for x1, y1, x2, y2 in [
        (128, 132, 180, 132),
        (134, 148, 184, 148),
        (140, 164, 172, 164),
    ]:
        draw.line(
            (OX + int(x1 * s), OY + int(y1 * s),
             OX + int(x2 * s), OY + int(y2 * s)),
            fill=cyan, width=max(2, int(10 * s)),
        )
    for cx, cy in [(180, 132), (184, 148), (172, 164)]:
        r = max(1, int(7 * s))
        draw.ellipse(
            (OX + int(cx * s) - r, OY + int(cy * s) - r,
             OX + int(cx * s) + r, OY + int(cy * s) + r),
            fill=cyan,
        )
    return img


def main():
    base = draw_gui_icon(SIZE)
    base.save(OUT_PNG, "PNG")
    print(f"PNG: {OUT_PNG}  ({OUT_PNG.stat().st_size:,} bytes)")

    # ICO multi-res: PIL's append_images doesn't apply to .ico — instead
    # save the largest source with `sizes=[(s, s), ...]` and PIL embeds
    # all listed sizes by resampling internally.
    base.save(OUT_ICO, format="ICO",
              sizes=[(16, 16), (32, 32), (48, 48), (64, 64), (128, 128), (256, 256)])
    print(f"ICO: {OUT_ICO}  ({OUT_ICO.stat().st_size:,} bytes, multi-res)")

    if DESKTOP.exists():
        for src, dst_name in [
            (OUT_PNG, "Q-TRON.png"),
            (OUT_ICO, "Q-TRON.ico"),
        ]:
            dst = DESKTOP / dst_name
            shutil.copy2(src, dst)
            print(f"Desktop copy: {dst}")
    else:
        print(f"Desktop not found: {DESKTOP}")


if __name__ == "__main__":
    main()
