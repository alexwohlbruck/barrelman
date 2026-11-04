import Elysia, { t } from 'elysia'
import { createRateLimiter, perIpLimit, sendBytes } from '../lib/static-response'
import { getGlyphs as _getGlyphs, getSprite as _getSprite, SPRITE_NAMES } from '../services/map-assets.service'

/** An empty glyph range may be filled by a later release, so it is cached for a day only. */
const EMPTY_GLYPHS_CACHE = 'public, max-age=86400'

/**
 * A map's first view asks for a handful of glyph ranges per font and the two
 * sprite files, and the browser caches them for a month. 600 a minute leaves
 * room for many maps behind one NAT and stops a scraper looping over ranges.
 */
const ASSET_REQUESTS_PER_MINUTE = 600

/**
 * Glyphs and sprites for the published styles. Public and unmetered: MapLibre
 * fetches them from URLs in the style and never sends our key with them.
 * Outside every GROUP_PREFIXES entry, so they are never attributed or billed.
 */
export function createMapAssetRoutes(deps = { getGlyphs: _getGlyphs, getSprite: _getSprite }) {
  const limiter = createRateLimiter(ASSET_REQUESTS_PER_MINUTE, 60_000)
  return new Elysia()
    .onBeforeHandle(perIpLimit(limiter))
    .get(
      '/fonts/:fontstack/:range',
      ({ params, headers, set }) => {
        const glyphs = deps.getGlyphs(params.fontstack, params.range)
        if (!glyphs) {
          set.status = 404
          return { error: 'A glyph range is "<start>-<end>.pbf" for one block of 256 code points, e.g. 0-255.pbf' }
        }
        return sendBytes(set, headers, { ...glyphs, cacheControl: glyphs.empty ? EMPTY_GLYPHS_CACHE : undefined })
      },
      {
        params: t.Object({
          fontstack: t.String({
            description: 'One font or a comma-separated stack, e.g. `Noto Sans Regular,Arial Unicode MS Regular`. The first font we carry is served; a stack naming none of them gets Noto Sans Regular.',
            examples: ['Noto Sans Regular'],
          }),
          range: t.String({ description: 'A block of 256 code points, e.g. `0-255.pbf`.', examples: ['0-255.pbf'] }),
        }),
        detail: {
          tags: ['Tiles'],
          summary: 'Map glyphs',
          description:
            'Signed-distance-field glyphs for MapLibre, as protobuf. The styles under `/styles` point `glyphs` here. Fonts are Noto Sans Regular, Bold and Italic (SIL Open Font License 1.1).\n\n' +
            'Ranges 0-255 through 8192-8447 (U+0000 to U+20FF) are carried: Latin, Greek, Cyrillic, Armenian, Hebrew, Arabic, the Indic scripts, Thai, Georgian and general punctuation among them. Any other range answers 200 with an empty body, which MapLibre accepts, so labels in those scripts (CJK, for one) are left undrawn and the rest of the map renders.\n\n' +
            'No key needed and no credits charged; limited per address.',
        },
      },
    )
    .get(
      '/sprites/:file',
      ({ params, headers, set }) => {
        const sprite = deps.getSprite(params.file)
        if (!sprite) {
          set.status = 404
          return { error: `Unknown sprite. Available: ${SPRITE_NAMES.join(', ')}, each as .json, .png, @2x.json and @2x.png` }
        }
        return sendBytes(set, headers, sprite)
      },
      {
        params: t.Object({
          file: t.String({ description: 'e.g. `osm-bright.json`, `osm-bright@2x.png`, `dark-matter.png`', examples: ['osm-bright@2x.png'] }),
        }),
        detail: {
          tags: ['Tiles'],
          summary: 'Map sprites',
          description:
            'Icon sprite sheets for the published styles: `osm-bright` for `basemap.json` and `dark-matter` for `basemap-dark.json`, each as a JSON index and a PNG, at 1x and `@2x`. From the OpenMapTiles style repositories (code BSD-3-Clause, design CC-BY 4.0).\n\n' +
            'No key needed and no credits charged; limited per address.',
        },
      },
    )
}

export const mapAssetRoutes = createMapAssetRoutes()
