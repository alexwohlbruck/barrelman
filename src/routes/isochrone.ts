import Elysia, { t } from 'elysia'
import { apiAuth, apiAuthAfter } from '../middleware/api-auth'
import {
  getIsochrone as _getIsochrone,
  parseMode,
  parseDurations,
  IsochroneRequestError,
  GraphHopperError,
  MotisError,
  ISOCHRONE_MODES,
  MAX_STREET_DURATION,
  MAX_TRANSIT_DURATION,
  MAX_CONTOURS,
  type IsochroneRequest,
  type IsochroneMode,
  type FetchFn,
  type UnionFn,
} from '../services/isochrone.service'

/**
 * Isochrone endpoints.
 *
 * GET  /isochrone         — query-string form, for map clients and quick curls
 * POST /isochrone         — JSON form, same semantics
 * GET  /isochrone/modes   — supported modes and their limits
 *
 * Every mode Barrelman routes with is supported (walk, bike, car via
 * GraphHopper; transit via MOTIS + walk egress), at any set of durations the
 * caller asks for. Responses are a GeoJSON FeatureCollection with one polygon
 * per duration, bucket 0 innermost.
 */
export function createIsochroneRoutes(deps: {
  getIsochrone?: typeof _getIsochrone
  fetchFn?: FetchFn
  union?: UnionFn
} = {}) {
  const getIsochrone = deps.getIsochrone || _getIsochrone
  const serviceDeps = { fetchFn: deps.fetchFn, union: deps.union }

  const run = async (request: IsochroneRequest, set: { status?: number | string }) => {
    try {
      return await getIsochrone(request, serviceDeps)
    } catch (err) {
      if (err instanceof IsochroneRequestError) {
        set.status = 400
        return { error: err.message }
      }

      if (err instanceof GraphHopperError) {
        // 4xx from GraphHopper means we sent something it rejected; surface it
        // as-is. 5xx means the engine failed, which is a 502 to our caller.
        set.status = err.statusCode >= 500 ? 502 : err.statusCode
        try {
          return JSON.parse(err.body)
        } catch {
          return { error: err.body }
        }
      }

      if (err instanceof MotisError) {
        set.status = err.statusCode >= 500 ? 502 : err.statusCode
        try {
          return JSON.parse(err.body)
        } catch {
          return { error: err.body }
        }
      }

      set.status = 502
      return {
        error: 'Isochrone service unavailable',
        detail: err instanceof Error ? err.message : String(err),
      }
    }
  }

  const description =
    'Returns reachability polygons for a point. `durations` is a list of ' +
    'contour budgets in SECONDS — any values you like, not fixed buckets. ' +
    'Street modes (walk, bike, car) are computed by GraphHopper. Transit ' +
    'combines the stops MOTIS reports reachable within the budget with a ' +
    'GraphHopper walking isochrone sized to each stop\'s leftover time, ' +
    'unioned with the direct walk from the origin. Set arriveBy=true for a ' +
    'reverse isochrone (the area that can *reach* the point). The response ' +
    'is a GeoJSON FeatureCollection ordered smallest contour first ' +
    '(bucket 0), so renderers should draw it back to front.'

  return new Elysia({ prefix: '/isochrone' })
    .onBeforeHandle(apiAuth('isochrone'))
    .onAfterHandle(apiAuthAfter)

    // ── GET /isochrone ──────────────────────────────────────────────
    .get('/', async ({ query, set }) => {
      let request: IsochroneRequest
      try {
        request = {
          lat: Number(query.lat),
          lng: Number(query.lng),
          mode: parseMode(query.mode),
          durations: parseDurations(query.durations),
          arriveBy: query.arriveBy === 'true',
          time: query.time || undefined,
          simplify: query.simplify != null ? Number(query.simplify) : undefined,
          maxTransfers: query.maxTransfers != null ? Number(query.maxTransfers) : undefined,
          transitModes: query.transitModes
            ? query.transitModes.split(',').map((s) => s.trim()).filter(Boolean)
            : undefined,
          maxEgressTime: query.maxEgressTime != null ? Number(query.maxEgressTime) : undefined,
          wheelchair: query.wheelchair === 'true',
          maxStopIsochrones:
            query.maxStopIsochrones != null ? Number(query.maxStopIsochrones) : undefined,
        }
      } catch (err) {
        set.status = 400
        return { error: err instanceof Error ? err.message : String(err) }
      }

      return run(request, set)
    }, {
      query: t.Object({
        lat: t.String(),
        lng: t.String(),
        mode: t.Optional(t.String()),
        durations: t.Optional(t.String()),
        arriveBy: t.Optional(t.String()),
        time: t.Optional(t.String()),
        simplify: t.Optional(t.String()),
        maxTransfers: t.Optional(t.String()),
        transitModes: t.Optional(t.String()),
        maxEgressTime: t.Optional(t.String()),
        wheelchair: t.Optional(t.String()),
        maxStopIsochrones: t.Optional(t.String()),
      }),
      detail: {
        summary: 'Generate an isochrone for any travel mode',
        description:
          description +
          ' Example: /isochrone?lat=35.7796&lng=-78.6382&mode=walk&durations=300,600,900',
        tags: ['Isochrone'],
      },
    })

    // ── POST /isochrone ─────────────────────────────────────────────
    .post('/', async ({ body, set }) => {
      let request: IsochroneRequest
      try {
        request = {
          lat: body.lat,
          lng: body.lng,
          mode: parseMode(body.mode),
          durations: body.durations,
          arriveBy: body.arriveBy,
          time: body.time,
          simplify: body.simplify,
          maxTransfers: body.maxTransfers,
          transitModes: body.transitModes,
          maxEgressTime: body.maxEgressTime,
          wheelchair: body.wheelchair,
          maxStopIsochrones: body.maxStopIsochrones,
        }
      } catch (err) {
        set.status = 400
        return { error: err instanceof Error ? err.message : String(err) }
      }

      return run(request, set)
    }, {
      body: t.Object({
        lat: t.Number({ minimum: -90, maximum: 90 }),
        lng: t.Number({ minimum: -180, maximum: 180 }),
        mode: t.Optional(t.String()),
        durations: t.Optional(t.Array(t.Number({ minimum: 1 }))),
        arriveBy: t.Optional(t.Boolean()),
        time: t.Optional(t.String()),
        simplify: t.Optional(t.Number({ minimum: 0 })),
        maxTransfers: t.Optional(t.Number({ minimum: 0 })),
        transitModes: t.Optional(t.Array(t.String())),
        maxEgressTime: t.Optional(t.Number({ minimum: 0 })),
        wheelchair: t.Optional(t.Boolean()),
        maxStopIsochrones: t.Optional(t.Number({ minimum: 1 })),
      }),
      detail: {
        summary: 'Generate an isochrone for any travel mode (JSON body)',
        description,
        tags: ['Isochrone'],
      },
    })

    // ── GET /isochrone/modes ────────────────────────────────────────
    .get('/modes', () => ({
      modes: ISOCHRONE_MODES.map((mode: IsochroneMode) => ({
        mode,
        engine: mode === 'transit' ? 'motis+graphhopper' : 'graphhopper',
        maxDurationSeconds: mode === 'transit' ? MAX_TRANSIT_DURATION : MAX_STREET_DURATION,
        aliases: mode === 'walk' ? ['foot', 'walking', 'pedestrian']
          : mode === 'bike' ? ['bicycle', 'cycling']
          : mode === 'car' ? ['drive', 'driving', 'auto']
          : ['pt', 'public_transport'],
      })),
      maxContours: MAX_CONTOURS,
    }), {
      detail: {
        summary: 'List supported isochrone modes and their limits',
        tags: ['Isochrone'],
      },
    })
}

export const isochroneRoutes = createIsochroneRoutes()
