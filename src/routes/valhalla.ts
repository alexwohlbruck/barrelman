import Elysia from 'elysia'
import { authHandler } from '../middleware/auth'

function getValhallaUrl() {
  return process.env.VALHALLA_URL || 'http://barrelman-valhalla:8002'
}

export interface ValhallaFetcher {
  (url: string, init: RequestInit): Promise<Response>
}

/**
 * Transparent proxy to the Valhalla routing engine.
 *
 * Forwards every request under /valhalla/* to the upstream Valhalla server,
 * preserving method, path, query string, body, and content-type. Auth is
 * handled by the standard barrelman BARRELMAN_API_KEY check; the inbound
 * Authorization header is stripped before upstreaming.
 *
 * Standard Valhalla endpoints supported via this proxy:
 *   /status, /route, /optimized_route, /sources_to_targets, /isochrone,
 *   /locate, /height, /trace_route, /trace_attributes, /expansion,
 *   /transit_available
 *
 * The injectable fetcher mirrors the pattern in tiles.ts so tests can mock
 * upstream responses without hitting a real Valhalla container.
 */
export function createValhallaRoutes(deps: { fetchValhalla?: ValhallaFetcher } = {}) {
  const fetchValhalla: ValhallaFetcher =
    deps.fetchValhalla || ((url, init) => fetch(url, init))

  return new Elysia({ prefix: '/valhalla' })
    .onBeforeHandle(authHandler)
    .all('/*', async ({ request, params, set }) => {
      const subPath = (params as Record<string, string>)['*'] || ''
      const url = new URL(request.url)
      const target = `${getValhallaUrl()}/${subPath}${url.search}`

      // Forward only the headers Valhalla cares about. Specifically, do NOT
      // forward the inbound Authorization header — barrelman API keys must
      // never leak to the upstream container.
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
        response = await fetchValhalla(target, init)
      } catch (err) {
        set.status = 502
        return { error: 'Valhalla upstream unreachable', detail: String(err) }
      }

      set.status = response.status
      const respCt = response.headers.get('content-type')
      if (respCt) set.headers['content-type'] = respCt
      set.headers['cache-control'] = 'no-store'
      return response.body
    }, {
      detail: {
        summary: 'Valhalla routing engine proxy',
        description:
          'Transparent proxy to the standard Valhalla server. Forwards any sub-path under /valhalla (e.g. /valhalla/route, /valhalla/isochrone, /valhalla/status) with method, query, body, and content-type preserved. See https://valhalla.github.io/valhalla/api/ for the upstream API.',
        tags: ['Valhalla'],
      },
    })
}

export const valhallaRoutes = createValhallaRoutes()
