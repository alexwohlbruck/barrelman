import Elysia, { t } from 'elysia'
import { authHandler } from '../middleware/auth'
import {
  getTransitRoute as _getTransitRoute,
  getNearbyStops as _getNearbyStops,
  getRoutesForStop as _getRoutesForStop,
  MotisError,
  ALL_TRANSIT_MODES,
  type TransitRouteRequest,
  type TransitMode,
  type FetchFn,
} from '../services/transit.service'

export function createTransitRoutes(deps: {
  getTransitRoute?: typeof _getTransitRoute
  getNearbyStops?: typeof _getNearbyStops
  getRoutesForStop?: typeof _getRoutesForStop
  fetchFn?: FetchFn
} = {}) {
  const getTransitRoute = deps.getTransitRoute || _getTransitRoute
  const getNearbyStops = deps.getNearbyStops || _getNearbyStops
  const getRoutesForStop = deps.getRoutesForStop || _getRoutesForStop
  const fetchFn = deps.fetchFn || undefined

  return new Elysia({ prefix: '/transit' })
    .onBeforeHandle(authHandler)

    // ── POST /transit/route ─────────────────────────────────────────
    .post('/route', async ({ body, set }) => {
      try {
        const request: TransitRouteRequest = {
          from: { lat: body.from.lat, lng: body.from.lng },
          to: { lat: body.to.lat, lng: body.to.lng },
          time: body.time,
          arriveBy: body.arriveBy,
          numItineraries: body.numItineraries,
          searchWindow: body.searchWindow,
          transitModes: body.transitModes as TransitMode[] | undefined,
          maxWalkDistance: body.maxWalkDistance,
          maxTransfers: body.maxTransfers,
          wheelchair: body.wheelchair,
        }
        return await getTransitRoute(request, fetchFn)
      } catch (err) {
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
          error: 'Transit routing service unavailable',
          detail: err instanceof Error ? err.message : String(err),
        }
      }
    }, {
      body: t.Object({
        from: t.Object({
          lat: t.Number({ minimum: -90, maximum: 90 }),
          lng: t.Number({ minimum: -180, maximum: 180 }),
        }),
        to: t.Object({
          lat: t.Number({ minimum: -90, maximum: 90 }),
          lng: t.Number({ minimum: -180, maximum: 180 }),
        }),
        time: t.Optional(t.String()),
        arriveBy: t.Optional(t.Boolean()),
        numItineraries: t.Optional(t.Number({ minimum: 1, maximum: 10 })),
        searchWindow: t.Optional(t.Number({ minimum: 1 })),
        transitModes: t.Optional(t.Array(t.String())),
        maxWalkDistance: t.Optional(t.Number({ minimum: 0 })),
        maxTransfers: t.Optional(t.Number({ minimum: 0 })),
        wheelchair: t.Optional(t.Boolean()),
      }),
      detail: {
        summary: 'Get transit route between two points',
        description:
          'Queries the MOTIS transit router for itineraries between two coordinates. ' +
          'Returns transit legs with boarding/alighting stops, route info, and ' +
          'geometry. Walking legs are straight-line estimates — the Parchment ' +
          'server replaces them with actual GraphHopper walking routes.',
        tags: ['Transit'],
      },
    })

    // ── GET /transit/stops ──────────────────────────────────────────
    .get('/stops', async ({ query, set }) => {
      try {
        const lat = Number(query.lat)
        const lng = Number(query.lng)
        const radius = query.radius ? Number(query.radius) : 1000
        const limit = query.limit ? Number(query.limit) : 20

        if (isNaN(lat) || isNaN(lng)) {
          set.status = 400
          return { error: 'lat and lng must be valid numbers' }
        }

        if (lat < -90 || lat > 90 || lng < -180 || lng > 180) {
          set.status = 400
          return { error: 'lat must be [-90,90], lng must be [-180,180]' }
        }

        return await getNearbyStops({ lat, lng, radius, limit })
      } catch (err) {
        set.status = 500
        return {
          error: 'Failed to query nearby stops',
          detail: err instanceof Error ? err.message : String(err),
        }
      }
    }, {
      query: t.Object({
        lat: t.String(),
        lng: t.String(),
        radius: t.Optional(t.String()),
        limit: t.Optional(t.String()),
      }),
      detail: {
        summary: 'Find nearby transit stops',
        description:
          'Returns transit stops within a radius of the given coordinates, ' +
          'ordered by distance. Uses PostGIS spatial index for efficient ' +
          'radius queries. Only returns stops (location_type=0), not ' +
          'stations or entrances.',
        tags: ['Transit'],
      },
    })

    // ── GET /transit/routes ─────────────────────────────────────────
    .get('/routes', async ({ query, set }) => {
      try {
        if (!query.feedId || !query.stopId) {
          set.status = 400
          return { error: 'feedId and stopId are required' }
        }

        return await getRoutesForStop(query.feedId, query.stopId)
      } catch (err) {
        set.status = 500
        return {
          error: 'Failed to query routes for stop',
          detail: err instanceof Error ? err.message : String(err),
        }
      }
    }, {
      query: t.Object({
        feedId: t.String(),
        stopId: t.String(),
      }),
      detail: {
        summary: 'Get routes serving a stop',
        description:
          'Returns all transit routes that pass through the specified stop, ' +
          'including route name, color, type, and agency information.',
        tags: ['Transit'],
      },
    })
}

export const transitRoutes = createTransitRoutes()
