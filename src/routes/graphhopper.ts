import Elysia from 'elysia'
import { authHandler } from '../middleware/auth'

function getGraphHopperUrl() {
  return process.env.GRAPHHOPPER_URL || 'http://barrelman-graphhopper:8989'
}

export interface GraphHopperFetcher {
  (url: string, init: RequestInit): Promise<Response>
}

/**
 * Transparent proxy to the GraphHopper routing engine.
 *
 * Forwards every request under /graphhopper/* to the upstream GraphHopper
 * server, preserving method, path, query string, body, and content-type.
 * Auth is handled by the standard barrelman BARRELMAN_API_KEY check; the
 * inbound Authorization header is stripped before upstreaming.
 *
 * Standard GraphHopper endpoints supported via this proxy:
 *   /route, /isochrone, /route-optimization, /matrix, /geocode, /map-matching,
 *   /health, /info
 */
export function createGraphHopperRoutes(deps: { fetchGraphHopper?: GraphHopperFetcher } = {}) {
  const fetchGraphHopper: GraphHopperFetcher =
    deps.fetchGraphHopper || ((url, init) => fetch(url, init))

  return new Elysia({ prefix: '/graphhopper' })
    .onBeforeHandle(authHandler)
    .all('/*', async ({ request, params, set }) => {
      const subPath = (params as Record<string, string>)['*'] || ''
      const url = new URL(request.url)
      const target = `${getGraphHopperUrl()}/${subPath}${url.search}`

      // Forward only the headers GraphHopper cares about. Do NOT forward
      // the inbound Authorization header — API keys must not leak upstream.
      const headers: Record<string, string> = {}
      const ct = request.headers.get('content-type')
      if (ct) headers['content-type'] = ct
      const accept = request.headers.get('accept')
      if (accept) headers['accept'] = accept

      const init: RequestInit = { method: request.method, headers }
      if (request.method !== 'GET' && request.method !== 'HEAD') {
        init.body = await request.arrayBuffer()
      }

      let response: Response
      try {
        response = await fetchGraphHopper(target, init)
      } catch (err) {
        set.status = 502
        return { error: 'GraphHopper upstream unreachable', detail: String(err) }
      }

      set.status = response.status
      const respCt = response.headers.get('content-type')
      if (respCt) set.headers['content-type'] = respCt
      set.headers['cache-control'] = 'no-store'
      set.headers['access-control-allow-origin'] = '*'
      return response.body
    }, {
      detail: {
        summary: 'GraphHopper routing engine proxy',
        description:
          'Transparent proxy to the GraphHopper server. Forwards any sub-path under /graphhopper (e.g. /graphhopper/route, /graphhopper/isochrone) with method, query, body, and content-type preserved.',
        tags: ['GraphHopper'],
      },
    })
}

export const graphhopperRoutes = createGraphHopperRoutes()
