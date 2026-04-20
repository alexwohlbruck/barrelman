import Elysia from 'elysia'
import { authHandler } from '../middleware/auth'
import {
  getEnrichedRoute as _getEnrichedRoute,
  GraphHopperError,
  type FetchFn,
} from '../services/route.service'

/**
 * Enriched routing endpoint.
 *
 * Accepts a GraphHopper /route request body and returns the GraphHopper
 * response enriched with:
 *   - Per-shape-point elevation (via elevation=true)
 *   - Per-edge surface, road class, bike network, smoothness, slope, speed
 *     (via details parameter)
 *   - Elevation statistics (total gain/loss, max/min)
 *
 * The transparent /graphhopper/* proxy is preserved unchanged — this endpoint
 * adds a higher-level API on top of the raw GraphHopper calls.
 */
export function createRouteRoutes(deps: {
  getEnrichedRoute?: typeof _getEnrichedRoute
  fetchFn?: FetchFn
} = {}) {
  const getEnrichedRoute = deps.getEnrichedRoute || _getEnrichedRoute
  const fetchFn = deps.fetchFn || undefined

  return new Elysia()
    .onBeforeHandle(authHandler)
    .post('/route', async ({ body, set }) => {
      try {
        const result = await getEnrichedRoute(body, fetchFn)
        return result
      } catch (err) {
        if (err instanceof GraphHopperError) {
          set.status = err.statusCode
          // Try to parse the error body as JSON, fall back to raw string
          try {
            return JSON.parse(err.body)
          } catch {
            return { error: err.body }
          }
        }

        set.status = 502
        return {
          error: 'Routing service unavailable',
          detail: err instanceof Error ? err.message : String(err),
        }
      }
    }, {
      detail: {
        summary: 'Get enriched route with elevation and path detail data',
        description:
          'Accepts a GraphHopper-compatible route request and returns the route ' +
          'response enriched with per-edge surface/road class/bike network/' +
          'smoothness/slope attributes and elevation statistics. Internally ' +
          'calls GraphHopper /route with details and elevation enabled.',
        tags: ['Routing'],
      },
    })
}

export const routeRoutes = createRouteRoutes()
