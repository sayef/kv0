"""Brand assets for kv0, with every word as outlines.

    uv run --no-project --with fonttools --with brotli --with resvg-py python assets/src/build.py

The fonts are Atkinson Hyperlegible Next and Mono (SIL Open Font License), read from
docs/assets/fonts, the same files the docs site serves. Writes to assets/ and copies what the docs
site uses into docs/assets/. Run it again after changing anything here; the output is
deterministic.
"""

import shutil
from pathlib import Path

import resvg_py
from fontTools.pens.svgPathPen import SVGPathPen
from fontTools.pens.transformPen import TransformPen
from fontTools.ttLib import TTFont
from fontTools.varLib.instancer import instantiateVariableFont

ROOT = Path(__file__).resolve().parents[2]
FONTS = ROOT / "docs/assets/fonts"
OUT = ROOT / "assets"
DOCS = ROOT / "docs/assets"


def instance(file, weight):
    return instantiateVariableFont(TTFont(FONTS / file), {"wght": weight})


WORD = instance("atkinson-hyperlegible-mono-latin-wght-normal.woff2", 600)
TEXT = instance("atkinson-hyperlegible-next-latin-wght-normal.woff2", 450)
MONO = instance("atkinson-hyperlegible-mono-latin-wght-normal.woff2", 450)

# Tokens from brand/DESIGN.md.
THEMES = {
    "light": {
        "ground": "#F4F5F6",
        "surface": "#FFFFFF",
        "ink": "#151719",
        "meta": "#4F5660",
        "accent": "#2A47C2",
        "rule": "#DDDFE3",
    },
    "dark": {
        "ground": "#0E0F10",
        "surface": "#151618",
        "ink": "#E6E8EA",
        "meta": "#A0A6AE",
        "accent": "#8FA8FF",
        "rule": "#26282C",
    },
}
TAGLINE = "An ordered key-value store on Lance, on any object store"


def mark(color, x=0.0, y=0.0, size=64.0):
    """The kv0 mark on a 64 grid: a zero, then three keys in ascending order.

    The ring is the zero of the name and the start of the keyspace; the rows grow in length from
    top to bottom, the way sorted keys read. Stroke 6 with round caps, like the family's marks.
    """
    s = size / 64
    return (
        f'<g transform="translate({x:.2f} {y:.2f}) scale({s:.4f})" fill="none" stroke="{color}" '
        'stroke-width="6" stroke-linecap="round" stroke-linejoin="round">'
        '<circle cx="19" cy="32" r="13"/>'
        '<path d="M41 21H47M41 32H52M41 43H57"/></g>'
    )


def text_path(font, text, x, baseline, size, tracking=0.0):
    """SVG path data for `text` and its advance width, in user units."""
    glyphs = font.getGlyphSet()
    cmap = font.getBestCmap()
    scale = size / font["head"].unitsPerEm
    pen = SVGPathPen(glyphs, ntos=lambda v: f"{v:.2f}".rstrip("0").rstrip("."))
    cursor = x
    for ch in text:
        name = cmap[ord(ch)]
        glyphs[name].draw(TransformPen(pen, (scale, 0, 0, -scale, cursor, baseline)))
        cursor += glyphs[name].width * scale + tracking * size
    return pen.getCommands(), cursor - x - tracking * size


def lockup(x, baseline, size, theme):
    """The mark, then `kv0` in the mono; `size` is the font size. Returns the body and width."""
    t = THEMES[theme]
    cap = WORD["OS/2"].sCapHeight * size / WORD["head"].unitsPerEm
    m = cap * 2.1  # the ring then matches the height of the capitals
    top = baseline - cap / 2 - m / 2
    gap = m * 1.02
    word, w = text_path(WORD, "kv0", x + gap, baseline, size, tracking=-0.02)
    return f'{mark(t["accent"], x, top, m)}<path d="{word}" fill="{t["ink"]}"/>', gap + w


def svg(width, height, body, label):
    return (
        f'<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 {width:g} {height:g}" width="{width:g}" '
        f'height="{height:g}" role="img" aria-label="{label}">\n{body}\n</svg>\n'
    )


def write(name, text, docs=True):
    (OUT / name).write_text(text)
    if docs:
        (DOCS / name).write_text(text)


def key_rows(width, top, height, theme, count=7):
    """Faint sorted key rows with one in accent: the subject, kept quiet behind the words."""
    t = THEMES[theme]
    out = []
    step = height / count
    for i in range(count):
        y = top + i * step + step / 2
        w = width * (0.3 + 0.7 * (i + 1) / count)  # ascending, as sorted keys read
        color, op = (t["accent"], 1) if i == count // 2 + 1 else (t["meta"], 0.22)
        out.append(
            f'<rect x="{width - w:.1f}" y="{y - 3:.1f}" width="{w:.1f}" height="6" rx="3" '
            f'fill="{color}" fill-opacity="{op}"/>'
        )
    return "".join(out)


# Directions explored before choosing the mark (brand/DESIGN.md says why each was set aside).
EXPLORATIONS = {
    "zero with cursor": '<g fill="none" stroke="C" stroke-width="6" stroke-linecap="round" stroke-linejoin="round"><rect x="13" y="5" width="38" height="54" rx="19"/><path d="M25 32H39"/></g>',
    "ring and menu rows": '<g fill="none" stroke="C" stroke-width="6" stroke-linecap="round"><circle cx="20" cy="32" r="14"/><path d="M42 20H58M42 32H54M42 44H50"/></g>',
    "key bow as 0": '<g fill="none" stroke="C" stroke-width="6" stroke-linecap="round" stroke-linejoin="round"><rect x="6" y="18" width="20" height="28" rx="10"/><path d="M26 32H58M48 32V42M56 32V40"/></g>',
    "ordered stack": '<g fill="none" stroke="C" stroke-width="6" stroke-linecap="round"><path d="M10 14H30M10 26H40M10 38H48M10 50H56"/></g>',
    "seek into ring": '<g fill="none" stroke="C" stroke-width="6" stroke-linecap="round" stroke-linejoin="round"><circle cx="38" cy="32" r="20"/><path d="M6 32H32M24 24L32 32L24 40"/></g>',
}


def explorations():
    """One sheet: every direction, and the chosen mark, at 16, 24, 32 and 64 px on both themes."""
    names = [*EXPLORATIONS, "chosen: zero, keys in order"]
    col, rows = 190, []
    for r, theme in enumerate(("light", "dark")):
        t = THEMES[theme]
        y0 = r * 140
        rows.append(f'<rect y="{y0}" width="{col * len(names)}" height="140" fill="{t["ground"]}"/>')
        for i, name in enumerate(names):
            x = i * col + 16
            for size in (16, 24, 32, 64):
                top = y0 + 88 - size
                if name in EXPLORATIONS:
                    body = EXPLORATIONS[name].replace('"C"', f'"{t["accent"]}"')
                    rows.append(f'<g transform="translate({x} {top}) scale({size / 64:g})">{body}</g>')
                else:
                    rows.append(mark(t["accent"], x, top, size))
                x += size + 10
            label, _ = text_path(TEXT, name, i * col + 16, y0 + 120, 12)
            rows.append(f'<path d="{label}" fill="{t["meta"]}"/>')
    return svg(col * len(names), 280, "".join(rows), "kv0 mark explorations")


def main():
    OUT.mkdir(exist_ok=True)
    DOCS.mkdir(exist_ok=True)
    (ROOT / "docs/design").mkdir(exist_ok=True)
    (ROOT / "docs/design/explorations.svg").write_text(explorations())

    for theme, suffix in (("light", ""), ("dark", "-dark")):
        t = THEMES[theme]
        write(f"logo{suffix}.svg", svg(64, 64, mark(t["accent"]), "kv0"))
        body, w = lockup(4, 60, 64, theme)
        write(f"wordmark{suffix}.svg", svg(round(w + 8), 80, body, "kv0"))
        # Banner: lockup and tagline over the faint key rows.
        bw, bh = 1280, 320
        body, w = lockup(96, 170, 96, theme)
        tag, _ = text_path(TEXT, TAGLINE, 100, 232, 28)
        write(
            f"banner-{theme}.svg",
            svg(
                bw,
                bh,
                f'<rect width="{bw}" height="{bh}" fill="{t["ground"]}"/>'
                f'<g transform="translate({bw - 400} 0)">{key_rows(320, 40, 240, theme)}</g>'
                + body
                + f'<path d="{tag}" fill="{t["meta"]}"/>',
                "kv0",
            ),
        )
        # A PNG copy for the README: GitHub's mobile app does not render SVG images.
        (OUT / f"banner-{theme}.png").write_bytes(
            bytes(resvg_py.svg_to_bytes(svg_string=(OUT / f"banner-{theme}.svg").read_text(), width=bw))
        )

    # The app icon and favicon: the mark on the dark ground tile, readable on any browser chrome.
    d = THEMES["dark"]
    icon = svg(
        64,
        64,
        f'<rect width="64" height="64" rx="14" fill="{d["ground"]}"/>' + mark(d["accent"], 8, 8, 48),
        "kv0",
    )
    write("icon.svg", icon)

    # Social preview, 1280 × 640.
    t = THEMES["light"]
    body, w = lockup(112, 300, 128, "light")
    tag, _ = text_path(TEXT, TAGLINE, 116, 384, 34)
    sub, _ = text_path(MONO, "Rust and Python. Local disk, S3, GCS, Azure.", 116, 440, 26)
    social = svg(
        1280,
        640,
        f'<rect width="1280" height="640" fill="{t["ground"]}"/>'
        f'<g transform="translate(860 100)">{key_rows(320, 0, 220, "light", 6)}</g>'
        + body
        + f'<path d="{tag}" fill="{t["ink"]}"/><path d="{sub}" fill="{t["meta"]}"/>',
        "kv0: an ordered key-value store on Lance",
    )
    write("social-preview.svg", social)
    png = resvg_py.svg_to_bytes(svg_string=social)
    (OUT / "social-preview.png").write_bytes(bytes(png))
    shutil.copy(OUT / "social-preview.png", DOCS / "social.png")
    print("wrote assets/ and docs/assets/")


if __name__ == "__main__":
    main()
