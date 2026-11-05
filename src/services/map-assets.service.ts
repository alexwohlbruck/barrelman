import { existsSync, readdirSync, readFileSync } from 'node:fs'
import { join, resolve } from 'node:path'
import { etagOf } from '../lib/static-response'

/**
 * Glyphs and sprites for the published map styles, vendored under assets/.
 *
 * Glyphs are the OpenMapTiles Noto Sans build (v2.0, OFL 1.1), Regular, Bold
 * and Italic, for the ranges 0-255 through 8192-8447 only (U+0000-U+20FF):
 * Latin and its extensions, Greek, Cyrillic, Armenian, Hebrew, Arabic, the
 * Indic scripts, Thai, Georgian, Ethiopic, Khmer and general punctuation,
 * among others. The full set is about 100 MB because of CJK; these 99 files
 * are 7 MB. A range outside them answers an empty glyph set, which MapLibre
 * accepts, so a label in those scripts is left undrawn while the rest of the
 * map renders. Sources and licences are in assets/LICENSE.md.
 */

const ASSETS_DIR = resolve(import.meta.dir, '../../assets')
const FONTS_DIR = join(ASSETS_DIR, 'fonts')
const SPRITES_DIR = join(ASSETS_DIR, 'sprites')

/** The font served for a fontstack that names none of ours. */
export const FALLBACK_FONT = 'Noto Sans Regular'

export const VENDORED_FONTS: readonly string[] = existsSync(FONTS_DIR)
  ? readdirSync(FONTS_DIR, { withFileTypes: true }).filter((d) => d.isDirectory()).map((d) => d.name).sort()
  : []

/** Sprite sheets by the name in their URL. */
export const SPRITE_NAMES = ['osm-bright', 'dark-matter'] as const

export interface Asset {
  bytes: Uint8Array
  contentType: string
  etag: string
}

const EMPTY = new Uint8Array(0)
const PBF = 'application/x-protobuf'

/** Loaded on first request and kept: 7 MB at most, and a map asks for the same few ranges. */
const loaded = new Map<string, Asset>()

function load(path: string, contentType: string): Asset | null {
  const hit = loaded.get(path)
  if (hit) return hit
  if (!existsSync(path)) return null
  const bytes = new Uint8Array(readFileSync(path))
  const asset = { bytes, contentType, etag: etagOf(bytes) }
  loaded.set(path, asset)
  return asset
}

/** "0-255.pbf" → 0, or null unless it names one aligned 256-codepoint range. */
export function parseGlyphRange(range: string): number | null {
  const m = /^(\d{1,5})-(\d{1,5})\.pbf$/.exec(range)
  if (!m) return null
  const start = Number(m[1])
  const end = Number(m[2])
  if (start % 256 !== 0 || end !== start + 255 || end > 65535) return null
  return start
}

/**
 * The first font of a comma-separated stack that we carry, or the fallback.
 * MapLibre asks for the whole stack in one URL ("Noto Sans Regular,Arial
 * Unicode MS Regular") and expects the server to merge it; serving the first
 * one we have is what most glyph servers do.
 */
export function pickFont(fontstack: string): string {
  for (const name of fontstack.split(',')) {
    const font = name.trim()
    if (VENDORED_FONTS.includes(font)) return font
  }
  return FALLBACK_FONT
}

/**
 * The glyph PBF for a request, an empty one for a range we don't vendor, or
 * null when the range is malformed.
 */
export function getGlyphs(fontstack: string, range: string): (Asset & { empty: boolean; font: string }) | null {
  const start = parseGlyphRange(range)
  if (start === null) return null
  const font = pickFont(fontstack)
  const asset = load(join(FONTS_DIR, font, `${start}-${start + 255}.pbf`), PBF)
  if (asset) return { ...asset, empty: false, font }
  return { bytes: EMPTY, contentType: PBF, etag: '"empty"', empty: true, font }
}

/** "osm-bright@2x.png" and the like; null for anything else. */
export function getSprite(file: string): Asset | null {
  const m = /^([a-z-]+)(@2x)?\.(json|png)$/.exec(file)
  if (!m || !(SPRITE_NAMES as readonly string[]).includes(m[1])) return null
  return load(join(SPRITES_DIR, file), m[3] === 'json' ? 'application/json' : 'image/png')
}
