import Elysia from 'elysia'
import { apiAuth, apiAuthAfter } from '../middleware/api-auth'
import { graphHopperTimeoutMs, graphHopperUrl, isTimeoutError } from '../lib/graphhopper'

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
 *   /route, /isochrone, /route-optimization, /matrix, /geocode, /match,
 *   /health, /info
 *
 * Each call is cut off after GRAPHHOPPER_TIMEOUT_MS (504), or as soon as the
 * caller disconnects. For map matching prefer POST /match, which copes with
 * sparse traces that make the raw matcher grind.
 */
export function createGraphHopperRoutes(deps: { fetchGraphHopper?: GraphHopperFetcher } = {}) {
  const fetchGraphHopper: GraphHopperFetcher =
    deps.fetchGraphHopper || ((url, init) => fetch(url, init))

  return new Elysia({ prefix: '/graphhopper' })
    .onBeforeHandle(apiAuth('routing'))
    .onAfterHandle(apiAuthAfter)
    .all(
      '/*',
      async ({ request, params, set, body }) => {
        const subPath = (params as Record<string, string>)['*'] || ''
        const url = new URL(request.url)
        const target = `${graphHopperUrl()}/${subPath}${url.search}`

        // Forward only the headers GraphHopper cares about. Do NOT forward
        // the inbound Authorization header — API keys must not leak upstream.
        const headers: Record<string, string> = {}
        const ct = request.headers.get('content-type')
        if (ct) headers['content-type'] = ct
        const accept = request.headers.get('accept')
        if (accept) headers['accept'] = accept

        const init: RequestInit = {
          method: request.method,
          headers,
          // GraphHopper has no deadline of its own: a raw /match on a sparse
          // trace can run for minutes. Give up after the timeout, or when the
          // caller hangs up, rather than holding the request open.
          signal: AbortSignal.any([request.signal, AbortSignal.timeout(graphHopperTimeoutMs())]),
        }
        if (request.method !== 'GET' && request.method !== 'HEAD') {
          // From the `parse` hook below, not `request.arrayBuffer()`: Elysia
          // consumes the request stream before the handler runs, so reading it
          // here throws ERR_BODY_ALREADY_USED.
          init.body = (body as ArrayBuffer | undefined) ?? undefined
        }

        let response: Response
        try {
          response = await fetchGraphHopper(target, init)
        } catch (err) {
          if (isTimeoutError(err)) {
            set.status = 504
            return { error: 'GraphHopper upstream timed out', timeoutMs: graphHopperTimeoutMs() }
          }
          set.status = 502
          return { error: 'GraphHopper upstream unreachable', detail: String(err) }
        }

        set.status = response.status
        const respCt = response.headers.get('content-type')
        if (respCt) set.headers['content-type'] = respCt
        set.headers['cache-control'] = 'no-store'
        set.headers['access-control-allow-origin'] = '*'
        return response.body
      },
      {
        // Keep the body as raw bytes rather than letting Elysia decode it to
        // an object: this is a transparent proxy, and re-encoding JSON would
        // change what GraphHopper receives.
        parse: ({ request }) => request.arrayBuffer(),
        detail: {
          summary: 'GraphHopper routing engine proxy',
          description:
            'Transparent proxy to the GraphHopper server. Forwards any sub-path under /graphhopper (e.g. /graphhopper/route, /graphhopper/isochrone) with method, query, body, and content-type preserved.',
          tags: ['GraphHopper'],
        },
      },
    )
}

export const graphhopperRoutes = createGraphHopperRoutes()
