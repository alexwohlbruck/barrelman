import Elysia, { t } from 'elysia'
import { apiAuth, apiAuthAfter } from '../middleware/api-auth'
import {
  getEnrichedRoute as _getEnrichedRoute,
  GraphHopperError,
  type FetchFn,
} from '../services/route.service'
import {
  getRouteHazards as _getRouteHazards,
  RouteHazardsRequestError,
  MAX_COORDINATES,
  MAX_LENGTH_METERS,
  type RouteHazardsRequest,
} from '../services/route-hazards.service'
import { HAZARD_KINDS, PROFILES } from '../lib/route-hazards/classify'

const HAZARDS_DESCRIPTION = `
Hazards along a route from OSM tags the routing graph does not carry, placed by
metres along the \`coordinates\` sent.

Points: \`traffic_signals\`, \`stop\` (\`detail: "all"\` for an all-way stop) and
\`give_way\` on the route, honouring \`direction\` / \`traffic_signals:direction\`
(\`forward\`, \`backward\`, or the compass bearing the sign faces) against the
route's bearing. An untagged stop or give-way sign on a junction node counts
only when the route is on the minor road. \`speed_camera\` (\`detail\` is its
\`maxspeed\`), \`traffic_calming\` (\`detail\` is the device, \`raised_crossing\`
for a raised crosswalk), \`toll\` booths, \`low_clearance\` (\`detail\` in metres;
\`0\` for \`below_default\`), and posted \`hazard\` signs (\`detail\` is the
\`hazard\` value; school zones and flooding have kinds of their own).
\`tram_crossing\` is where the route crosses tram rails at under 30° (\`detail\`
is the angle).

Stretches, with \`endDistance\`: \`tram_tracks\` (street-embedded rails),
\`unlit\` (\`lit=no\`), \`flood_prone\`, \`toll\` roads, \`movable_bridge\`,
\`no_sidewalk\` (\`detail\` is the road class), \`low_clearance\`, \`school_zone\`
and \`hazard\`.

Each profile gets the kinds that matter to it: \`car\` no tram, lighting or
sidewalk kinds; \`bike\` no clearance, toll, camera or sidewalk kinds; \`foot\`
signals, lighting, sidewalks, flooding, movable bridges and posted hazards.
\`kinds\` narrows that further. Whether a hazard matters now (after dark,
in rain, on ice) is the caller's to judge. At most ${MAX_COORDINATES} coordinates
and ${MAX_LENGTH_METERS / 1000} km.
`.trim()

const ROUTE_DESCRIPTION = `
Accepts a GraphHopper-compatible route request and returns the route response
enriched with per-edge surface, road class, bike network, smoothness and slope
attributes (\`edge_segments\`) and elevation statistics. Internally calls
GraphHopper /route with details and elevation enabled.

Each path also describes what it passes, for voice guidance, read from OSM:

- \`edge_segments[].crossingName\`: on a crosswalk segment, the named road it crosses.
- \`overpasses\`: roads (\`kind: "road"\`) and railways (\`"rail"\`) that pass over the
  route at a higher layer, at \`distance\` metres along it. Twin carriageways and
  parallel tracks crossed together are one, ending at \`endDistance\`. Nothing is
  reported inside a tunnel. Not computed beyond 300 km.
- \`states\`: stretches inside a state or province (admin_level 4), with its \`name\`
  and ISO 3166-2 \`code\`, where the extract holds its polygon.
- \`parks\`: on a \`foot\` route, stretches inside a \`leisure=park\`, with its \`name\`.

Each is looked up under a time budget and comes back empty rather than late.
`.trim()

const hazardRequestBody = t.Object({
  coordinates: t.Array(t.Tuple([t.Number(), t.Number()]), { description: '[lon, lat]', maxItems: MAX_COORDINATES }),
  profile: t.Union(PROFILES.map(p => t.Literal(p))),
  kinds: t.Optional(t.Array(t.Union(HAZARD_KINDS.map(k => t.Literal(k))))),
})

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
  getRouteHazards?: typeof _getRouteHazards
  fetchFn?: FetchFn
} = {}) {
  const getEnrichedRoute = deps.getEnrichedRoute || _getEnrichedRoute
  const getRouteHazards = deps.getRouteHazards || _getRouteHazards
  const fetchFn = deps.fetchFn || undefined

  return new Elysia()
    .onBeforeHandle(apiAuth('routing'))
    .onAfterHandle(apiAuthAfter)
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
        description: ROUTE_DESCRIPTION,
        tags: ['Routing'],
      },
    })
    .post('/route/hazards', async ({ body, set }) => {
      try {
        return await getRouteHazards(body as RouteHazardsRequest)
      } catch (err) {
        if (err instanceof RouteHazardsRequestError) {
          set.status = 400
          return { error: err.message }
        }
        set.status = 502
        return { error: 'Route hazards unavailable', detail: err instanceof Error ? err.message : String(err) }
      }
    }, {
      body: hazardRequestBody,
      detail: {
        summary: 'Point and way hazards along a route',
        description: HAZARDS_DESCRIPTION,
        tags: ['Routing'],
      },
    })
}

export const routeRoutes = createRouteRoutes()
