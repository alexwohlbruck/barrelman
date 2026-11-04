import Elysia, { t } from 'elysia'
import { apiAuth, apiAuthAfter } from '../middleware/api-auth'
import { serverOrigin } from '../config/accounts.config'
import { tileAttribution } from '../config/tile-attribution'
import { keyQuery, urlKeyFor } from '../services/tilejson.service'
import osmBright from '../styles/osm-bright.json' with { type: 'json' }
import darkMatter from '../styles/dark-matter.json' with { type: 'json' }

const CORS = 'access-control-allow-origin'

/** Glyphs and sprites are ours, vendored under assets/ (services/map-assets.service.ts). */
const glyphsUrl = (origin: string) => `${origin}/fonts/{fontstack}/{range}.pbf`
const spriteUrl = (origin: string, sprite: string) => `${origin}/sprites/${sprite}`

/** The OpenMapTiles schema source name both styles' layers refer to. */
const SOURCE_NAME = 'openmaptiles'

interface StyleSpec {
  name: string
  base: Record<string, unknown>
  /** Sprite sheet name under /sprites. */
  sprite: string
  design: string
}

/**
 * Every style we publish, by the name in its URL. Each is an OpenMapTiles
 * style, vendored under src/styles/ with its licence; see LICENSE.md there.
 */
const STYLES: Record<string, StyleSpec> = {
  basemap: {
    name: 'Barrelman Basemap',
    base: osmBright as Record<string, unknown>,
    sprite: 'osm-bright',
    design:
      'OSM Bright by OpenMapTiles, derived from Mapbox Open Styles. Code BSD-3-Clause, design CC-BY 4.0. https://github.com/openmaptiles/osm-bright-gl-style',
  },
  'basemap-dark': {
    name: 'Barrelman Basemap Dark',
    base: darkMatter as Record<string, unknown>,
    sprite: 'dark-matter',
    design:
      'Dark Matter by OpenMapTiles, derived from CartoDB Basemaps by Stamen and Paul Norman. Code BSD-3-Clause, design CC-BY 4.0. Fonts changed to Noto Sans. https://github.com/openmaptiles/dark-matter-gl-style',
  },
}

/**
 * A style document for one request.
 *
 * The source points at our basemap TileJSON rather than listing tiles, so its
 * zoom range and bounds always follow the archive Martin is serving. The key
 * travels the same way as in TileJSON: echoed only if it arrived in the URL.
 */
export function buildStyle(name: string, key: string | null, origin: string = serverOrigin): Record<string, unknown> | null {
  const spec = STYLES[name]
  if (!spec) return null
  return {
    ...spec.base,
    name: spec.name,
    metadata: {
      ...(spec.base.metadata as Record<string, unknown> | undefined),
      'barrelman:style-licence': spec.design,
      'barrelman:attribution': 'https://docs.barrelman.dev/usage/attribution',
    },
    sources: {
      [SOURCE_NAME]: {
        type: 'vector',
        url: `${origin}/tiles/basemap.json${keyQuery(key)}`,
        attribution: tileAttribution('basemap'),
      },
    },
    sprite: spriteUrl(origin, spec.sprite),
    glyphs: glyphsUrl(origin),
  }
}

export const STYLE_NAMES = Object.keys(STYLES)

/**
 * Map styles for the basemap. Under the tiles group: a map fetches its style
 * on the same key and terms as its tiles, and the style itself costs nothing.
 */
export function createStyleRoutes() {
  return new Elysia({ prefix: '/styles' })
    .onBeforeHandle(apiAuth('tiles', { units: () => 0 }))
    .onAfterHandle(apiAuthAfter)
    .get(
      '/:style',
      ({ params, query, headers, set }) => {
        const match = /^(.+)\.json$/.exec(params.style)
        const style = match ? buildStyle(match[1], urlKeyFor(headers, query)) : null
        if (!style) {
          set.status = 404
          return { error: `Unknown style. Available: ${STYLE_NAMES.map((name) => `${name}.json`).join(', ')}` }
        }
        set.headers['content-type'] = 'application/json'
        set.headers['cache-control'] = 'public, max-age=300'
        set.headers[CORS] = '*'
        return style
      },
      {
        params: t.Object({
          style: t.String({ description: '`basemap.json` (OSM Bright) or `basemap-dark.json` (Dark Matter)' }),
        }),
        detail: {
          tags: ['Tiles'],
          summary: 'Basemap map style',
          description:
            'A MapLibre style (version 8) that draws the basemap, ready to pass to `new maplibregl.Map({ style })`. Its source is `/tiles/basemap.json`, which carries the attribution the map must show.\n\n' +
            '`basemap.json` is based on OSM Bright and `basemap-dark.json` on Dark Matter, both by OpenMapTiles (code BSD-3-Clause, design CC-BY 4.0). Glyphs load from `/fonts` and sprites from `/sprites` on this server, both public and free; see those routes for what they cover.\n\n' +
            'Pass the key as `?api_key=` and it is carried into the source URL and on to every tile URL. A key sent as a Bearer header is never written into the document. Needs a key with the `tiles` scope and costs no credits.',
        },
      },
    )
}

export const styleRoutes = createStyleRoutes()
