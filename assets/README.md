# Brand assets

Built by [`src/build.py`](src/build.py); the rationale, colours and type are in
[`brand/DESIGN.md`](../brand/DESIGN.md). Do not edit the SVGs by hand: change the script and run it.

```sh
uv run --no-project --with fonttools --with brotli --with resvg-py python assets/src/build.py
```

| File | Use |
|---|---|
| `logo.svg`, `logo-dark.svg` | The mark in the accent, for light and dark grounds |
| `icon.svg` | The mark on the dark tile: favicon, avatars, app icons |
| `wordmark.svg`, `wordmark-dark.svg` | Mark and name: the docs header, small placements |
| `banner-light.svg`, `banner-dark.svg` | Lockup and tagline: the top of the README and the docs home |
| `social-preview.svg`, `social-preview.png` | Link previews (1280 × 640) |

The fonts are Atkinson Hyperlegible Next and Mono, SIL Open Font License, in
[`docs/assets/fonts`](../docs/assets/fonts).
