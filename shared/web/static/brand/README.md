# Q-TRON Brand Assets

Pure-SVG brand kit. All marks share one geometry (the Q letter ring +
3 circuit traces fanning to the bottom-right) so a future redesign
only needs to change one set of paths.

## Files

| File | Use | Notes |
|---|---|---|
| `qtron-mark.svg` | 64×64 logomark only | inline-friendly, used by nav strip |
| `qtron-wordmark.svg` | 200×64 mark + "Q-TRON" text | external link target, prints, slides |
| `qtron-icon-gui.svg` | 256×256 desktop GUI app icon | rounded blue tile, Q in white |
| `qtron-icon-exe.svg` | 256×256 .exe build icon | cyber HUD ring around Q |
| `qtron-icon-telegram.svg` | 512×512 Telegram bot avatar | round, simple — Telegram circle-crops |
| `qtron-icon-rest.svg` | 256×256 REST API doc icon | cloud + code brackets |

## Where they're used

- **Web nav strip** — `qtron-mark.svg` paths are inlined into
  `kr/web/static/nav.js` and `us/web/static/nav.js` (the `<a class="qnav-logo">` block). Inlined rather than `<img src=>` so the
  gradient picks up CSS `currentColor` for theming, and there's no
  extra HTTP round-trip on first paint.
- **Browser tab favicon** — KR uses 태극 (`kr/web/static/favicon.svg`),
  US uses Stars & Stripes (`us/web/static/favicon.svg`). Per-market
  flags are kept by design — when an operator has both KR and US tabs
  open the flag color codes which is which faster than a brand mark
  could.
- **Telegram bot avatar** — upload `qtron-icon-telegram.svg`
  (or a PNG export at 512×512) via @BotFather → `/setuserpic`.
  Not wired automatically; manual one-time op step.
- **Windows GUI / EXE icons** — set in PyInstaller / Inno Setup config
  to point at PNG/ICO exports of `qtron-icon-gui.svg` /
  `qtron-icon-exe.svg`. Not wired tonight; flagged in
  `docs/morning_review_20260427.md` for Jeff's call.

## Raster reference (Jeff's original sketches)

Jeff's reference designs (the Q + cyber-circle pictures shared
2026-04-26) are not committed — they were chat-attached PNG/JPEG.
The SVGs above were derived from those designs. If Jeff wants the
raster originals checked in for forensics:

```
shared/web/static/brand/raster/qtron-gui-ref.png
shared/web/static/brand/raster/qtron-exe-ref.png
shared/web/static/brand/raster/qtron-telegram-ref.png
shared/web/static/brand/raster/qtron-rest-ref.png
shared/web/static/brand/raster/qtron-wordmark-ref.png
```

… is the suggested layout. `.gitignore` already excludes `*.png` /
`*.jpg` patterns where required.

## Color tokens

Single linear gradient stop set used across all marks:

| Stop | Color | Hex |
|---|---|---|
| 0%   | sky-300       | `#7dd3fc` |
| 55%  | blue-500      | `#3b82f6` |
| 100% | blue-800      | `#1e40af` |

If we ever ship a light-theme variant, swap stops for
`#3b82f6` → `#1e3a8a` (deeper) and the marks read on white.
