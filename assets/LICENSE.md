# Vendored map assets

Served by `/fonts` and `/sprites` (`src/routes/map-assets.ts`) for the styles
in `src/styles/`. Baked into the image by the Dockerfile's `COPY . .`.

## Fonts

`fonts/Noto Sans Regular`, `fonts/Noto Sans Bold` and `fonts/Noto Sans Italic`
are glyph PBFs from the OpenMapTiles fonts release
[v2.0](https://github.com/openmaptiles/fonts/releases/tag/v2.0)
(`noto-sans.zip`), unchanged.

Only the ranges `0-255.pbf` through `8192-8447.pbf` (U+0000 to U+20FF) are
kept: 33 ranges per font, 99 files, about 7 MB. The full release carries all
256 ranges per font, about 100 MB in all, most of it CJK. A range missing here
is answered with an empty glyph set.

To add ranges, unzip the same release and copy the extra `<start>-<end>.pbf`
files into each font's directory.

The fonts are Noto Sans by Google, under the SIL Open Font License 1.1. The
licence text is in `fonts/LICENSE-OFL.txt`, copied from
`noto-sans/LICENSE` in the OpenMapTiles fonts repository.

## Sprites

| File | Upstream | Commit |
|---|---|---|
| `sprites/osm-bright{,@2x}.{json,png}` | https://github.com/openmaptiles/osm-bright-gl-style (`gh-pages`, `sprite*`) | 286b174adbd6e8693887841d6cfdf7445cd5f8c0 |
| `sprites/dark-matter{,@2x}.{json,png}` | https://github.com/openmaptiles/dark-matter-gl-style (`gh-pages`, `sprite*`) | 9769cd7a3642e607bb867c4c0c18bed714f52f5a |

Renamed from `sprite*` to the style's name and otherwise unchanged. They are
part of each style and under the same licences: code BSD-3-Clause, design
CC-BY 4.0. Both licences are reproduced in `src/styles/LICENSE.md`.
