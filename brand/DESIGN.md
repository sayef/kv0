---
version: 1
name: kv0 design system
description: >
  Slate neutrals shared with its sibling projects, one cobalt accent, Atkinson Hyperlegible Next for
  text and Atkinson Hyperlegible Mono for keys, code and the wordmark. Flat surfaces one tone step
  from the ground, 6 px controls, no decoration a fact could replace. The mark is a zero followed by
  three keys in ascending order: the keyspace, sorted.
mode: read
taste: { bold: 2, motion: 1, density: 6 }
---

# kv0: brand tokens

kv0's surfaces are its documentation site, its README and its asset set. They are read by engineers
deciding whether to put their data in kv0, and then by the same engineers looking up an option at
2 a.m. The tokens are implemented in `docs/stylesheets/extra.css` and drawn by
`assets/src/build.py`; nothing else defines a colour.

## Name

**kv0**: a key-value store, and zero, the first key of an ordered keyspace and the number of
storage protocols of its own (Lance does the storing). Always lower case, set in the mono face
where it appears as a word on its own, like a crate name.

## Subject

An embedded engine: ordered keys in byte order, atomic batches, iterators that seek; versions,
write-ahead logs and compaction; local disk, S3, GCS and Azure; Rust and Python. Its readers care
about guarantees, numbers with their method, and what happens on a crash.

## Voice and tone

Cryptic in name, precise in prose, calm about failure.

- **Precise.** "Writes survive a killed process; they do not survive the loss of the machine", not
  "durable". Numbers carry their unit, their size and their method.
- **Calm.** No exclamation marks, no superlatives, no "blazing". Regressions are reported in the same
  table as wins.
- **Plain words.** Key, value, batch, iterator, version, flush, compaction, write-ahead log.
- **Sentence case** everywhere; `kv0` and API names in code formatting.

## Mark

A ring followed by three rounded rows of increasing length: the zero of the name, then keys in
ascending order. Drawn with one 6-unit stroke on a 64 grid, round caps and joins, the same
construction as its sibling marks, so they sit together without looking alike.

I drew four other directions and tested them at 16, 24, 32 and 64 px on both grounds
([`docs/design/explorations.svg`](../docs/design/explorations.svg)):

| Direction | Why I set it aside |
|---|---|
| Zero with a cursor | A pill with a bar reads as a minus sign, a battery or θ before it reads as 0. |
| Ring and menu rows | Equal rows read as the menu icon; ascending rows say "sorted". |
| Key bow as 0 | The generic key icon, which means security and passwords, not storage. |
| Ordered stack | The text-align icon of every editor; nothing says zero or kv. |
| Seek into a ring | The sign-in icon. |

The chosen mark keeps the ring's counter open at 16 px, its rows stay distinct at 16 px (1.5 px
strokes, 1.25 px apart), and it reads left to right the way a key is compared. It is drawn in the
accent on both themes, and on the dark ground tile for the favicon and app icon, so the favicon
reads on light and dark browser chrome alike.

**Wordmark.** The mark followed by `kv0` in Atkinson Hyperlegible Mono at weight 600, tracking
−2 %, outlined so it renders without the font. The mark's box is 2.1 times the cap height, its ring
the height of the capitals; the text starts 1.02 box widths from the mark's left edge. The mono face's
slashed zero repeats the ring without competing with it. Never stretch, recolour per letter, outline,
shadow or enclose the mark.

**Files.** Built by [`assets/src/build.py`](../assets/src/build.py) from the docs' font files, so
they are reproducible: `logo.svg` and `logo-dark.svg` (the mark), `icon.svg` (the mark on its tile;
also the favicon), `wordmark.svg` and `wordmark-dark.svg`, `banner-light.svg` and
`banner-dark.svg` (lockup and tagline over sorted rows), `social-preview.svg` and
`social-preview.png` (1280 × 640).

## Colour

```yaml
colors:            # light            dark
  ground:          ["#F4F5F6",       "#0E0F10"]   # the page behind panels and code
  surface:         ["#FFFFFF",       "#151618"]   # reading surface, one tone step up
  step:            ["#EAECEE",       "#1D2023"]   # hover, quiet fills, the search field
  ink:             ["#151719",       "#E6E8EA"]   # text
  ink-secondary:   ["#4F5660",       "#A0A6AE"]   # meta and secondary text, >= 6.2:1 on every surface
  accent:          ["#2A47C2",       "#8FA8FF"]   # cobalt: links, the current place, the primary action
  accent-tint:     ["#E3E8F8",       "#1A2140"]   # the current nav item, selection
  on-accent:       ["#FFFFFF",       "#0E0F10"]   # 7.6:1 and 8.4:1
  border:          ["#DDDFE3",       "#26282C"]   # table rules, panel outlines
  success:         ["#2E7D4F",       "#6CC28E"]   # "better" in benchmark verdicts
  warning:         ["#8F5400",       "#E5AE4A"]
  error:           ["#B42318",       "#F27A7C"]   # "worse" in benchmark verdicts
  focus:           accent
```

Contrast: accent on surface 7.6:1 (light) and 7.9:1 (dark); on ground 6.9:1 and 8.4:1.

Why cobalt: it is the ink of an engineering drawing, which suits a library whose documentation is
mostly guarantees and measurements. The neutrals and status colours are shared with the sibling
projects so the family reads as one; each project owns its accent, and kv0's stays clear of their
teal and of the green, amber and red that mean a state. It is used flat, never in a gradient.

**Charts.** One accent plus neutrals: kv0's bar in the accent, the baseline's in a neutral grey,
told apart by a legend with words. Never a rainbow.

## Typography

Both faces come from Braille Institute's Atkinson Hyperlegible project, SIL Open Font License,
self-hosted from `docs/assets/fonts/` (no font CDN; the site works offline and under a strict CSP).
They keep confusable glyphs apart (`0 O o`, `1 l I`), which matters in keys, hashes and option
names. Next has tabular figures, so benchmark tables align without switching to mono.

```yaml
typography:
  text: { family: "Atkinson Hyperlegible Next", fallback: "ui-sans-serif, system-ui, sans-serif" }
  code: { family: "Atkinson Hyperlegible Mono", fallback: "ui-monospace, SFMono-Regular, Menlo, monospace" }
  wordmark: { family: "Atkinson Hyperlegible Mono", weight: 600, tracking: "-0.02em" }
  scale:
    hero:    { size: 40px, weight: 650, lineHeight: 1.1, tracking: "-0.02em" }   # home only
    title:   { size: 28px, weight: 650, lineHeight: 1.2, tracking: "-0.01em" }
    heading: { size: 20px, weight: 600, lineHeight: 1.3 }
    body:    { size: 16px, weight: 400, lineHeight: 1.6, measure: "72ch" }
    small:   { size: 14px, weight: 400, lineHeight: 1.45 }
  numerals: tabular-nums in every table
```

## Shape and depth

```yaml
radius: { panel: 8px, control: 6px, code: 4px }
elevation: { rest: "surface one tone step from ground with a 1px border; no shadow" }
spacing: { base: 4px, scale: [4, 8, 12, 16, 24, 32, 48, 64] }
```

## Components

- Header: the wordmark for the current theme, tabs below on the surface, one hairline each.
- Home: banner, one paragraph, three actions, then code in Rust and Python side by side, then
  measured facts with their sizes and a link to the method. No eyebrows, no icon cards, no
  invented numbers.
- Tables: hairline rules, tabular figures, verdict words in success or error colour plus the word.
- Code: ground fill on surface, 4 px radius, the copy button.

## Motion

None beyond Material's state changes. `prefers-reduced-motion` removes those too.

## Browser surfaces

Selection in accent at 25 %, focus ring 2 px accent with 2 px offset, `theme-color` follows the
scheme, links underlined on hover with a 2 px offset.

## Principles

1. Every claim links to its measurement or its guarantee.
2. Colour means link, place or verdict, never decoration.
3. Show the API before describing it.
4. Failure modes are documented next to the feature, not in a separate page.
