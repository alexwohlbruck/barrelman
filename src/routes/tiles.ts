import Elysia, { t } from 'elysia'
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

export function createTileRoutes(deps: { fetchTile?: TileFetcher } = {}) {
  const fetchTile: TileFetcher = deps.fetchTile || ((url: string) => fetch(url))

  return new Elysia({ prefix: '/tiles' })
    .onBeforeHandle(tileAuthHandler)
    .onAfterHandle(apiAuthAfter)
    .get(
      '/:source/:z/:x/:y',
      async ({ params, set }) => {
        const { source, z, x, y } = params
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
