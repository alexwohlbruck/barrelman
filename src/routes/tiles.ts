import Elysia, { t } from 'elysia'

function getMartinUrl() {
  return process.env.MARTIN_URL || 'http://barrelman-martin:3000'
}

/**
 * Tile auth handler — validates against BARRELMAN_TILE_KEY.
 * Checks both Bearer token header and ?token query param.
 */
export function tileAuthHandler({
  headers,
  query,
  set,
}: {
  headers: Record<string, string | undefined>
  query: Record<string, string | undefined>
  set: { status: number | string }
}) {
  const tileKey = process.env.BARRELMAN_TILE_KEY
  if (!tileKey) {
    // No key configured = open access (dev mode)
    return
  }

  // Check Authorization header first
  const authorization = headers['authorization']
  if (authorization) {
    const token = authorization.replace('Bearer ', '')
    if (token === tileKey) return
  }

  // Check ?token query parameter (used in tile URLs)
  if (query.token === tileKey) return

  set.status = 401
  return { error: 'Invalid or missing tile key' }
}

export interface TileFetcher {
  (url: string): Promise<Response>
}

export function createTileRoutes(deps: { fetchTile?: TileFetcher } = {}) {
  const fetchTile: TileFetcher = deps.fetchTile || ((url: string) => fetch(url))

  return new Elysia({ prefix: '/tiles' })
    .onBeforeHandle(tileAuthHandler)
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
      },
    )
}

export const tileRoutes = createTileRoutes()
