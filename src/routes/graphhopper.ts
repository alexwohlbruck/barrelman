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
 * Only the sub-paths in GRAPHHOPPER_PASSTHROUGH are forwarded; the rest 404.
 * The whole proxy is priced as one routing call, and GraphHopper's other
 * endpoints (/isochrone, /spt, /matrix, /match) do far more work per request
 * than a route. Those have their own endpoints here, with their own price and
 * limits: GET /isochrone and POST /match.
 *
 * Each call is cut off after GRAPHHOPPER_TIMEOUT_MS (504), or as soon as the
 * caller disconnects. For map matching prefer POST /match, which copes with
 * sparse traces that make the raw matcher grind.
 */
/**
 * GraphHopper sub-paths the proxy forwards. `route` is the raw router (still
 * bounded by `routing.max_visited_nodes` in graphhopper-config.yml); `info`
 * and `health` are cheap reads that describe the graph.
 */
export const GRAPHHOPPER_PASSTHROUGH: ReadonlySet<string> = new Set(['route', 'info', 'health'])

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
        // Exact match on the decoded path, so `route/../spt` or a trailing
        // segment cannot reach anything outside the allowlist.
        if (!GRAPHHOPPER_PASSTHROUGH.has(subPath)) {
          set.status = 404
          return { error: `Not available through this proxy. Allowed: ${[...GRAPHHOPPER_PASSTHROUGH].map((p) => `/graphhopper/${p}`).join(', ')}` }
        }
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
          console.error('[graphhopper] upstream unreachable', err)
          return { error: 'GraphHopper upstream unreachable' }
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
            'Proxy to the GraphHopper server for `/graphhopper/route`, `/graphhopper/info` and `/graphhopper/health`, with method, query, body, and content-type preserved. Any other sub-path answers 404: use `/isochrone` for reachability polygons and `POST /match` for map matching.',
          tags: ['GraphHopper'],
        },
      },
    )
}

export const graphhopperRoutes = createGraphHopperRoutes()
