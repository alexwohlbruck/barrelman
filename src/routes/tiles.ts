import Elysia, { t } from 'elysia'
import { join, resolve } from 'path'
import { apiAuth, apiAuthAfter } from '../middleware/api-auth'

function getMartinUrl() {
  return process.env.MARTIN_URL || 'http://barrelman-martin:3000'
}

const meteredTileAuth = apiAuth('tiles')

/**
 * Tiles authenticate exactly like every other endpoint: an ordinary
 * `brm_live_…` key, metered and revocable.
 *
 * There used to be a separate unmetered `BARRELMAN_TILE_KEY`, which meant tile
 * traffic was the one thing the platform could not see or bill. A map is not a
 * few requests — one view is thirty to sixty tiles — so that was the largest
 * unmetered surface in the product, and it had its own auth path to maintain.
 *
 * A map library cannot set an Authorization header, so `?api_key=` on the tile
 * URL stays supported. That means the key is visible to anyone reading the
 * page, which is why keys are scopeable: a browser-side key should carry the
 * `tiles` scope and nothing else.
 */
export const tileAuthHandler = meteredTileAuth

export interface TileFetcher {
  (url: string): Promise<Response>
}

/**
 * A Martin source name: one or more comma-separated table/function sources.
 *
 * Constrained because the four path segments are concatenated into the upstream
 * URL. Unvalidated, `..` segments walk out of the tile path and reach Martin's
 * other endpoints — its catalog and per-source metadata — which are not
 * intended to be republished through a metered route. Martin's own names are
 * Postgres identifiers, so this is not a real restriction on legitimate use.
 */
const SOURCE_RE = /^[A-Za-z0-9_-]+(,[A-Za-z0-9_-]+)*$/

/**
 * Tile coordinates are integers. `z` is a zoom level (Martin caps well below
 * 100), while `x`/`y` run to 2^z - 1 — seven digits at z22 — so they get a
 * generous bound rather than a tight one. `y` may carry a format suffix, which
 * some clients append (`3.pbf`).
 */
const Z_RE = /^\d{1,2}$/
const XY_RE = /^\d{1,10}$/
const Y_RE = /^\d{1,10}(\.[A-Za-z0-9]+)?$/

/**
 * A portolan feed key: one directory name under the tiles dir. Same guard idea
 * as SOURCE_RE above — the segments are joined into a filesystem path, so
 * anything that could traverse out of the pyramid (`..`, separators) must be
 * rejected before it reaches join(). Portolan keys are slug-shaped, so this is
 * not a restriction on legitimate use.
 */
const PORTOLAN_FEED_RE = /^[A-Za-z0-9_-]+$/

const CORS = 'access-control-allow-origin'

export function createTileRoutes(
  deps: { fetchTile?: TileFetcher; portolanTilesDir?: string } = {},
) {
  const fetchTile: TileFetcher = deps.fetchTile || ((url: string) => fetch(url))

  /**
   * Portolan tiles are static files, not Martin: `portolan sync` writes MVT
   * pyramids to <PORTOLAN_TILES_DIR>/<feed>/{z}/{x}/{y}.mvt plus a per-feed
   * tiles.json and a global index.json. Resolved once at startup; if the dir
   * does not exist (portolan not set up) every route answers 404 — missing
   * data is not a crash.
   */
  const portolanDir = resolve(
    deps.portolanTilesDir || process.env.PORTOLAN_TILES_DIR || './data/portolan/build/tiles',
  )

  return new Elysia({ prefix: '/tiles' })
    .onBeforeHandle(tileAuthHandler)
    .onAfterHandle(apiAuthAfter)
    .get(
      '/portolan/index.json',
      async ({ set }) => {
        // The global index: every feed with a cut pyramid, with its bounds —
        // entries of {feed, name, bounds, maxzoom}. It carries no tile URL
        // templates, so it is served verbatim.
        const file = Bun.file(join(portolanDir, 'index.json'))
        if (!(await file.exists())) {
          set.status = 404
          return { error: 'Portolan tile index not found' }
        }
        set.headers['content-type'] = 'application/json'
        set.headers['cache-control'] = 'public, max-age=60'
        set.headers[CORS] = '*'
        return await file.text()
      },
      {
        detail: {
          tags: ['Tiles'],
          summary: 'Portolan tile index',
          description:
            'Lists every feed with a corrected-geometry tile pyramid, with its bounds and max zoom.',
        },
      },
    )
    .get(
      '/portolan/:feed/tiles.json',
      async ({ params, query, set }) => {
        const { feed } = params
        if (!PORTOLAN_FEED_RE.test(feed)) {
          set.status = 400
          return { error: 'Invalid feed name' }
        }
        const file = Bun.file(join(portolanDir, feed, 'tiles.json'))
        if (!(await file.exists())) {
          set.status = 404
          return { error: 'Unknown portolan feed' }
        }

        // Portolan writes a relative template ("{z}/{x}/{y}.mvt") that is only
        // valid against its own on-disk layout. Rewrite the tiles array to the
        // public route, carrying the caller's api_key into the template — map
        // libraries can't set headers, so the key has to ride the tile URL.
        // The path is root-relative; MapLibre resolves it against the origin
        // this tiles.json was fetched from.
        const tileJson = await file.json()
        const apiKey = typeof query.api_key === 'string' && query.api_key
          ? `?api_key=${encodeURIComponent(query.api_key)}`
          : ''
        tileJson.tiles = [`/tiles/portolan/${feed}/{z}/{x}/{y}.mvt${apiKey}`]

        set.headers['content-type'] = 'application/json'
        set.headers['cache-control'] = 'public, max-age=60'
        set.headers[CORS] = '*'
        return JSON.stringify(tileJson)
      },
      {
        params: t.Object({
          feed: t.String({ description: 'Portolan feed key (e.g. "mta-subway")' }),
        }),
        detail: {
          tags: ['Tiles'],
          summary: 'Portolan feed TileJSON',
          description:
            'TileJSON for one feed\'s corrected-geometry pyramid, with tile URLs rewritten to this API.',
        },
      },
    )
    .get(
      '/portolan/:feed/style.json',
      async ({ params, set }) => {
        const { feed } = params
        if (!PORTOLAN_FEED_RE.test(feed)) {
          set.status = 400
          return { error: 'Invalid feed name' }
        }
        // The feed's resolved curation manifest (colors, widths, trunk policy),
        // written by `portolan sync` next to the pyramid. Clients that miss it
        // fall back to defaults, so 404 is fine — but never a crash.
        const file = Bun.file(join(portolanDir, feed, 'style.json'))
        if (!(await file.exists())) {
          set.status = 404
          return { error: 'Unknown portolan feed or no style manifest' }
        }
        set.headers['content-type'] = 'application/json'
        set.headers['cache-control'] = 'public, max-age=60'
        set.headers[CORS] = '*'
        return await file.text()
      },
      {
        params: t.Object({
          feed: t.String({ description: 'Portolan feed key (e.g. "mta-subway")' }),
        }),
        detail: {
          tags: ['Tiles'],
          summary: 'Portolan feed style manifest',
          description:
            "One feed's resolved portolan style manifest, served verbatim from the pyramid directory.",
        },
      },
    )
    .get(
      '/portolan/:feed/:z/:x/:y',
      async ({ params, set }) => {
        const { feed, z, x, y } = params
        if (!PORTOLAN_FEED_RE.test(feed) || !Z_RE.test(z) || !XY_RE.test(x) || !Y_RE.test(y)) {
          set.status = 400
          return { error: 'Invalid tile feed or coordinates' }
        }

        // The cutter only writes tiles a feature touches, so a missing file
        // inside the pyramid is a valid empty tile, not an error: 204 with no
        // body, which MapLibre renders as emptiness without logging a failure.
        const yBase = y.replace(/\.[A-Za-z0-9]+$/, '')
        const file = Bun.file(join(portolanDir, feed, z, x, `${yBase}.mvt`))
        if (!(await file.exists())) {
          return new Response(null, {
            status: 204,
            headers: { 'cache-control': 'public, max-age=3600', [CORS]: '*' },
          })
        }

        // Portolan writes raw (uncompressed) MVT — no Content-Encoding to
        // forward. Elysia may compress on the way out if the client accepts it.
        set.headers['content-type'] = 'application/x-protobuf'
        set.headers['cache-control'] = 'public, max-age=3600'
        set.headers[CORS] = '*'
        return file
      },
      {
        params: t.Object({
          feed: t.String({ description: 'Portolan feed key' }),
          z: t.String({ description: 'Zoom level' }),
          x: t.String({ description: 'Tile X coordinate' }),
          y: t.String({ description: 'Tile Y coordinate (optionally suffixed, e.g. "42.mvt")' }),
        }),
        detail: {
          tags: ['Tiles'],
          summary: 'Portolan corrected-geometry tile',
          description:
            'Serves a static MVT tile from the portolan pyramid. 204 for tiles inside the pyramid with no features.',
        },
      },
    )
    .get(
      '/:source/:z/:x/:y',
      async ({ params, set }) => {
        const { source, z, x, y } = params

        if (!SOURCE_RE.test(source) || !Z_RE.test(z) || !XY_RE.test(x) || !Y_RE.test(y)) {
          set.status = 400
          return { error: 'Invalid tile source or coordinates' }
        }

        const martinUrl = `${getMartinUrl()}/${source}/${z}/${x}/${y}`

        const response = await fetchTile(martinUrl)

        if (!response.ok) {
          set.status = response.status
          return { error: `Tile fetch failed: ${response.statusText}` }
        }

        // Forward the protobuf tile response.
        // Note: fetch() automatically decompresses gzip responses, so we must
        // NOT forward the original Content-Encoding header — the body we return
        // is already decompressed. Elysia may re-compress if the client accepts it.
        set.headers['content-type'] =
          response.headers.get('content-type') || 'application/x-protobuf'
        set.headers['cache-control'] = 'public, max-age=86400'
        set.headers['access-control-allow-origin'] = '*'

        return response.body
      },
      {
        params: t.Object({
          source: t.String({ description: 'Tile source name (e.g. "basemap", "basemap,parchment_pois")' }),
          z: t.String({ description: 'Zoom level' }),
          x: t.String({ description: 'Tile X coordinate' }),
          y: t.String({ description: 'Tile Y coordinate' }),
        }),
        detail: {
          tags: ['Tiles'],
          summary: 'Vector tile (Martin proxy)',
          description:
            'Proxies Mapbox Vector Tiles from the Martin tile server. Authenticates with an ordinary API key, via Bearer header or `?api_key=` for map libraries that cannot set headers.',
        },
      },
    )
}

export const tileRoutes = createTileRoutes()
