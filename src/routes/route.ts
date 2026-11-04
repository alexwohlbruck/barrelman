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
import {
  getRouteLanes as _getRouteLanes,
  RouteLanesRequestError,
  MAX_COORDINATES as MAX_LANE_COORDINATES,
  MAX_WAYS,
  type RouteLanesRequest,
} from '../services/route-lanes.service'
import { TURNS } from '../lib/lanes/lane-plan'

const LANES_DESCRIPTION = `
Draws a route in its lanes and says which lanes lead on at each maneuver, from
the OSM lane tags of the ways it drives (\`lanes\`, \`lanes:forward\`,
\`turn:lanes\`, \`bus:lanes\`, \`width\`, parking and bike lanes, \`placement\`),
by the same rules the road markings are drawn with.

Send the route's \`coordinates\` and, in \`ways\`, the OSM way each stretch drives,
by metres along the route: \`/route\`'s \`edge_segments\` carry them as
\`startDistance\`, \`endDistance\` and \`osmWayId\` when the routing graph has
\`osm_way_id\`. \`maneuvers\` gives the distance of each turn; its \`turn\`
(turn:lanes vocabulary: ${TURNS.join(', ')}) is read from the geometry when left out.

\`laneGeometry\` is a LineString in the chosen lanes, with S-curves where the
lane changes (finished before the stop line) and arcs through each turn.
\`measures[i]\` is the distance along the input route that coordinate stands for,
so progress along one maps to the other. Where a way is not in the database
the line keeps to the route's centreline. \`lanePlan\` has one entry per maneuver
whose approach has tagged turn arrows on two lanes or more: its lanes left to
right, which lead on (\`valid\`) and the one to take (\`recommended\`).

With \`profile: "bike"\` the line runs down the middle of the bike lane on the
side of travel (\`cycleway\`, \`cycleway:right|left|both\`, with their \`:width\` and
\`:buffer\`; \`opposite_lane\` for riding against a one-way), else about a metre
inside the kerb-side edge of the outer traffic lane, clear of any parking.
Cycleways, paths and footways mapped on their own keep their own line. A bike
route has no \`lanePlan\`.

Traffic keeps to the side \`drivingSide\` gives, else the one for \`country\`
(ISO 3166-1 alpha-3, as \`edge_segments\` carry it), else the right.
At most ${MAX_LANE_COORDINATES} coordinates and ${MAX_WAYS} ways.
`.trim()

const laneRequestBody = t.Object({
  coordinates: t.Array(t.Tuple([t.Number(), t.Number()]), { description: '[lon, lat]', maxItems: MAX_LANE_COORDINATES }),
  ways: t.Array(t.Object({
    from: t.Number({ description: 'Metres along the route' }),
    to: t.Number({ description: 'Metres along the route' }),
    osmWayId: t.Number(),
  }), { maxItems: MAX_WAYS }),
  maneuvers: t.Optional(t.Array(t.Object({
    distance: t.Number({ description: 'Metres along the route' }),
    turn: t.Optional(t.Union(TURNS.map(turn => t.Literal(turn)))),
  }))),
  country: t.Optional(t.String({ description: 'ISO 3166-1 alpha-3' })),
  drivingSide: t.Optional(t.Union([t.Literal('right'), t.Literal('left')])),
  profile: t.Optional(t.Union([t.Literal('car'), t.Literal('bike')], { default: 'car' })),
})

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
GraphHopper /route with details and elevation enabled. Edge segments carry
\`osmWayId\` and \`country\` where the routing graph has them, for POST /route/lanes.

Each path also describes what it passes, for voice guidance, read from OSM:

- \`edge_segments[].crossingName\`: on a crosswalk segment, the named road it crosses.
- On \`foot\` and \`bike\` routes, edge segments also carry what their OSM ways and nodes
  are tagged: \`crossingIsland\` (a refuge island partway across), \`buttonOperated\`
  (the signal waits for its push button), \`crossingDiagonal\` (cutting across a junction
  for an all-way walk), \`passage\` (\`building\`, \`arcade\` or \`indoor\`), \`cyclewaySide\`
  (\`left\`, \`right\` or \`both\`, as the route travels), \`level\`, \`conveying\` (an escalator
  or moving walkway) and \`elevator\`. The segment leaving a station entrance carries
  \`entrance\` (\`name\`, \`ref\`); the one leaving an elevator node, \`elevator\`.
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
  getRouteLanes?: typeof _getRouteLanes
  fetchFn?: FetchFn
} = {}) {
  const getEnrichedRoute = deps.getEnrichedRoute || _getEnrichedRoute
  const getRouteHazards = deps.getRouteHazards || _getRouteHazards
  const getRouteLanes = deps.getRouteLanes || _getRouteLanes
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
    .post('/route/lanes', async ({ body, set }) => {
      try {
        return await getRouteLanes(body as RouteLanesRequest)
      } catch (err) {
        if (err instanceof RouteLanesRequestError) {
          set.status = 400
          return { error: err.message }
        }
        set.status = 502
        return { error: 'Lane geometry unavailable', detail: err instanceof Error ? err.message : String(err) }
      }
    }, {
      body: laneRequestBody,
      detail: {
        summary: 'Lane-level route geometry and lane plan',
        description: LANES_DESCRIPTION,
        tags: ['Routing'],
      },
    })
}

export const routeRoutes = createRouteRoutes()
