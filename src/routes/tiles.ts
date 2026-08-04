import Elysia, { t } from 'elysia'
import { apiAuth, apiAuthAfter } from '../middleware/api-auth'

function getMartinUrl() {
  return process.env.MARTIN_URL || 'http://barrelman-martin:3000'
}

const meteredTileAuth = apiAuth('tiles')

/**
 * Tile auth: the dedicated `BARRELMAN_TILE_KEY` first, then the ordinary
 * metered path.
 *
 * The tile key predates accounts and stays as an unmetered service credential,
 * for a self-hosted deployment pointing its own map at its own tiles. Anything
 * else — including a customer's `brm_live_…` key in the `?token=` a tile URL
 * already carries — falls through to the metered guard.
 *
 * Note the change in behaviour when no tile key is set: that used to mean open
 * access, and now means "authenticate like every other endpoint". Local
 * development is still open, because the metered guard is itself open when
 * nothing is configured to check against.
 */
export function tileAuthHandler(context: {
  headers: Record<string, string | undefined>
  query: Record<string, string | undefined>
  request: Request
  set: { status?: number | string; headers: Record<string, string | number> }
}) {
  const tileKey = process.env.BARRELMAN_TILE_KEY
  const authorization = context.headers['authorization']

  if (tileKey) {
    if (authorization && authorization.replace('Bearer ', '').trim() === tileKey) return
    if (context.query.token === tileKey) return

    // Setting a tile key is an explicit decision that tiles are not public, so
    // an anonymous caller is refused here rather than being handed to the
    // metered guard — which treats "no credential and no service key
    // configured" as open development mode, and would make configuring a tile
    // key *loosen* access instead of tightening it.
    if (!authorization && !context.query.token && !context.query.api_key) {
      context.set.status = 401
      return { error: 'Invalid or missing tile key' }
    }
  }

  return meteredTileAuth(context)
}

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
            'Proxies Mapbox Vector Tiles from the Martin tile server. Optionally gated by BARRELMAN_TILE_KEY via Bearer header or `?token=`.',
        },
      },
    )
}

export const tileRoutes = createTileRoutes()
