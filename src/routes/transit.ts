import Elysia, { t } from 'elysia'
import { authHandler } from '../middleware/auth'
import {
  getTransitRoute as _getTransitRoute,
  getIntermodalRoute as _getIntermodalRoute,
  getNearbyStops as _getNearbyStops,
  getRoutesForStop as _getRoutesForStop,
  MotisError,
  ALL_TRANSIT_MODES,
  type TransitRouteRequest,
  type IntermodalRouteRequest,
  type TransitMode,
  type MotisStreetMode,
  type RentalFormFactor,
  type FetchFn,
} from '../services/transit.service'
import {
  getDepartures as _getDepartures,
  type DepartureRequest,
} from '../services/departures.service'
import {
  getVehiclePositions as _getVehiclePositions,
  getVehiclesForRoute as _getVehiclesForRoute,
  getTripStopTimes as _getTripStopTimes,
  type VehiclePositionsRequest,
} from '../services/vehicles.service'
import {
  getRouteShape as _getRouteShape,
} from '../services/shapes.service'
import {
  discoverRtUrls as _discoverRtUrls,
} from '../services/gtfs.service'
import {
  getRouteDetail as _getRouteDetail,
} from '../services/route-detail.service'
import {
  getStationDetail as _getStationDetail,
  getNearestEntrance as _getNearestEntrance,
} from '../services/station.service'

export function createTransitRoutes(deps: {
  getTransitRoute?: typeof _getTransitRoute
  getIntermodalRoute?: typeof _getIntermodalRoute
  getNearbyStops?: typeof _getNearbyStops
  getRoutesForStop?: typeof _getRoutesForStop
  getDepartures?: typeof _getDepartures
  getVehiclePositions?: typeof _getVehiclePositions
  getRouteShape?: typeof _getRouteShape
  discoverRtUrls?: typeof _discoverRtUrls
  getRouteDetail?: typeof _getRouteDetail
  getVehiclesForRoute?: typeof _getVehiclesForRoute
  getTripStopTimes?: typeof _getTripStopTimes
  fetchFn?: FetchFn
  getStationDetail?: typeof _getStationDetail
  getNearestEntrance?: typeof _getNearestEntrance
} = {}) {
  const getTransitRoute = deps.getTransitRoute || _getTransitRoute
  const getIntermodalRoute = deps.getIntermodalRoute || _getIntermodalRoute
  const getNearbyStops = deps.getNearbyStops || _getNearbyStops
  const getRoutesForStop = deps.getRoutesForStop || _getRoutesForStop
  const getDepartures = deps.getDepartures || _getDepartures
  const getVehiclePositions = deps.getVehiclePositions || _getVehiclePositions
  const getRouteShape = deps.getRouteShape || _getRouteShape
  const discoverRtUrls = deps.discoverRtUrls || _discoverRtUrls
  const getRouteDetail = deps.getRouteDetail || _getRouteDetail
  const getVehiclesForRoute = deps.getVehiclesForRoute || _getVehiclesForRoute
  const getTripStopTimes = deps.getTripStopTimes || _getTripStopTimes
  const fetchFn = deps.fetchFn || undefined
  const getStationDetail = deps.getStationDetail || _getStationDetail
  const getNearestEntrance = deps.getNearestEntrance || _getNearestEntrance

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

    // ── POST /transit/intermodal-route ───────────────────────────────
    .post('/intermodal-route', async ({ body, set }) => {
      try {
        const request: IntermodalRouteRequest = {
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
          preTransitModes: body.preTransitModes as MotisStreetMode[] | undefined,
          postTransitModes: body.postTransitModes as MotisStreetMode[] | undefined,
          directModes: body.directModes as MotisStreetMode[] | undefined,
          maxDirectTime: body.maxDirectTime,
          maxPreTransitTime: body.maxPreTransitTime,
          maxPostTransitTime: body.maxPostTransitTime,
          preTransitRentalFormFactors: body.preTransitRentalFormFactors as RentalFormFactor[] | undefined,
          postTransitRentalFormFactors: body.postTransitRentalFormFactors as RentalFormFactor[] | undefined,
        }
        return await getIntermodalRoute(request, fetchFn)
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
          error: 'Intermodal routing service unavailable',
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
        preTransitModes: t.Optional(t.Array(t.String())),
        postTransitModes: t.Optional(t.Array(t.String())),
        directModes: t.Optional(t.Array(t.String())),
        maxDirectTime: t.Optional(t.Number({ minimum: 0 })),
        maxPreTransitTime: t.Optional(t.Number({ minimum: 0 })),
        maxPostTransitTime: t.Optional(t.Number({ minimum: 0 })),
        preTransitRentalFormFactors: t.Optional(t.Array(t.String())),
        postTransitRentalFormFactors: t.Optional(t.Array(t.String())),
      }),
      detail: {
        summary: 'Intermodal routing with mode selection',
        description:
          'Coordinate-based intermodal routing via MOTIS. Supports pre/post-transit ' +
          'mode selection (WALK, BIKE, CAR_PARKING, RENTAL) and direct non-transit ' +
          'modes. Requires MOTIS to have OSM street data loaded. Returns legs with ' +
          'real OSM geometry for walk/bike/car segments.',
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

    // ── GET /transit/departures ─────────────────────────────────────
    .get('/departures', async ({ query, set }) => {
      try {
        const lat = Number(query.lat)
        const lng = Number(query.lng)

        if (isNaN(lat) || isNaN(lng)) {
          // If no coordinates, feedId + stopId are required
          if (!query.feedId || !query.stopId) {
            set.status = 400
            return { error: 'Either lat/lng or feedId/stopId are required' }
          }
        }

        const request: DepartureRequest = {
          lat: isNaN(lat) ? 0 : lat,
          lng: isNaN(lng) ? 0 : lng,
          radius: query.radius ? Number(query.radius) : undefined,
          time: query.time || undefined,
          n: query.n ? Number(query.n) : undefined,
          feedId: query.feedId || undefined,
          stopId: query.stopId || undefined,
        }

        return await getDepartures(request, fetchFn)
      } catch (err) {
        set.status = 500
        return {
          error: 'Failed to fetch departures',
          detail: err instanceof Error ? err.message : String(err),
        }
      }
    }, {
      query: t.Object({
        lat: t.Optional(t.String()),
        lng: t.Optional(t.String()),
        radius: t.Optional(t.String()),
        time: t.Optional(t.String()),
        n: t.Optional(t.String()),
        feedId: t.Optional(t.String()),
        stopId: t.Optional(t.String()),
      }),
      detail: {
        summary: 'Get upcoming departures at nearby stops',
        description:
          'Returns upcoming departures from transit stops near the given ' +
          'coordinates. Queries the MOTIS timetable and enriches results ' +
          'with route colors from the GTFS database. Supports direct stop ' +
          'queries via feedId/stopId, or spatial search via lat/lng/radius.',
        tags: ['Transit'],
      },
    })

    // ── GET /transit/vehicles ──────────────────────────────────────────
    .get('/vehicles', async ({ query, set }) => {
      try {
        const north = Number(query.north)
        const south = Number(query.south)
        const east = Number(query.east)
        const west = Number(query.west)

        if ([north, south, east, west].some(isNaN)) {
          set.status = 400
          return { error: 'north, south, east, west must be valid numbers' }
        }

        if (north < south) {
          set.status = 400
          return { error: 'north must be >= south' }
        }

        const request: VehiclePositionsRequest = {
          north,
          south,
          east,
          west,
          feedId: query.feedId || undefined,
          routeId: query.routeId || undefined,
        }

        return await getVehiclePositions(request, fetchFn)
      } catch (err) {
        set.status = 500
        return {
          error: 'Failed to fetch vehicle positions',
          detail: err instanceof Error ? err.message : String(err),
        }
      }
    }, {
      query: t.Object({
        north: t.String(),
        south: t.String(),
        east: t.String(),
        west: t.String(),
        feedId: t.Optional(t.String()),
        routeId: t.Optional(t.String()),
      }),
      detail: {
        summary: 'Get live vehicle positions within a bounding box',
        description:
          'Fetches GTFS-RT VehiclePosition data from all configured feeds ' +
          'and returns vehicles within the specified bounding box. Results ' +
          'are enriched with route colors and short names. Responses are ' +
          'cached for 10 seconds per feed.',
        tags: ['Transit'],
      },
    })

    // ── GET /transit/shapes ───────────────────────────────────────────
    .get('/shapes', async ({ query, set }) => {
      try {
        if (!query.feedId || !query.routeId) {
          set.status = 400
          return { error: 'feedId and routeId are required' }
        }

        const result = await getRouteShape(query.feedId, query.routeId)
        if (!result) {
          set.status = 404
          return { error: 'Shape not found for this route' }
        }

        // Shapes are static GTFS data — cache aggressively
        set.headers['Cache-Control'] = 'public, max-age=86400'

        return result
      } catch (err) {
        set.status = 500
        return {
          error: 'Failed to fetch route shape',
          detail: err instanceof Error ? err.message : String(err),
        }
      }
    }, {
      query: t.Object({
        feedId: t.String(),
        routeId: t.String(),
      }),
      detail: {
        summary: 'Get route shape geometry',
        description:
          'Returns the GTFS shape coordinates for a specific route, ' +
          'used for snapping live vehicle positions to the actual route ' +
          'geometry on the map. Coordinates are in [lng, lat] order. ' +
          'Responses are cached for 24 hours (shapes are static GTFS data).',
        tags: ['Transit'],
      },
    })

    // ── GET /transit/route-detail ─────────────────────────────────────
    .get('/route-detail', async ({ query, set }) => {
      try {
        if (!query.feedId || !query.routeId) {
          set.status = 400
          return { error: 'feedId and routeId are required' }
        }

        const result = await getRouteDetail(query.feedId, query.routeId)
        if (!result) {
          set.status = 404
          return { error: 'Route not found' }
        }

        set.headers['Cache-Control'] = 'public, max-age=3600'
        return result
      } catch (err) {
        set.status = 500
        return {
          error: 'Failed to fetch route detail',
          detail: err instanceof Error ? err.message : String(err),
        }
      }
    }, {
      query: t.Object({
        feedId: t.String(),
        routeId: t.String(),
      }),
      detail: {
        summary: 'Get route detail with ordered stops and shape',
        description:
          'Returns route metadata, geographically ordered stops, shape ' +
          'geometry, and related route IDs (same trunk line) for rendering ' +
          'an isolated route detail view.',
        tags: ['Transit'],
      },
    })

    // ── GET /transit/route-vehicles ──────────────────────────────────
    .get('/route-vehicles', async ({ query, set }) => {
      try {
        if (!query.routeIds) {
          set.status = 400
          return { error: 'routeIds is required (comma-separated)' }
        }

        const routeIds = query.routeIds.split(',').map(s => s.trim()).filter(Boolean)
        if (routeIds.length === 0) {
          set.status = 400
          return { error: 'routeIds must contain at least one route ID' }
        }

        return await getVehiclesForRoute(routeIds, query.feedId || undefined, fetchFn)
      } catch (err) {
        set.status = 500
        return {
          error: 'Failed to fetch route vehicles',
          detail: err instanceof Error ? err.message : String(err),
        }
      }
    }, {
      query: t.Object({
        routeIds: t.String(),
        feedId: t.Optional(t.String()),
      }),
      detail: {
        summary: 'Get all vehicles on specific routes (no bounding box)',
        tags: ['Transit'],
      },
    })

    // ── GET /transit/trip-stops ──────────────────────────────────────
    .get('/trip-stops', async ({ query, set }) => {
      try {
        if (!query.feedId || !query.tripId) {
          set.status = 400
          return { error: 'feedId and tripId are required' }
        }
        const stops = await getTripStopTimes(query.feedId, query.tripId, fetchFn)
        return { stops }
      } catch (err) {
        set.status = 500
        return { error: 'Failed to fetch trip stops', detail: err instanceof Error ? err.message : String(err) }
      }
    }, {
      query: t.Object({ feedId: t.String(), tripId: t.String() }),
      detail: { summary: 'Get real-time stop times for a specific trip', tags: ['Transit'] },
    })

    // ── GET /transit/bikes-allowed ────────────────────────────────────
    .get('/bikes-allowed', async ({ query, set }) => {
      try {
        if (!query.routes) {
          set.status = 400
          return { error: 'routes is required (comma-separated feedId_routeId pairs)' }
        }
        const pairs = query.routes.split(',').map(s => s.trim()).filter(Boolean)
        const routes = pairs.map(pair => {
          const idx = pair.indexOf('_')
          if (idx === -1) return null
          return { feedId: pair.slice(0, idx), routeId: pair.slice(idx + 1) }
        }).filter(Boolean) as Array<{ feedId: string; routeId: string }>

        if (routes.length === 0) {
          set.status = 400
          return { error: 'No valid feedId_routeId pairs found' }
        }

        const { getBikesAllowed } = await import('../services/gtfs.service')
        return await getBikesAllowed(routes)
      } catch (err) {
        set.status = 500
        return { error: 'Failed to check bikes allowed', detail: err instanceof Error ? err.message : String(err) }
      }
    }, {
      query: t.Object({ routes: t.String() }),
      detail: { summary: 'Batch check bikes_allowed for routes', tags: ['Transit'] },
    })

    // ── POST /transit/discover-rt-urls ─────────────────────────────────
    .post('/discover-rt-urls', async ({ body, set }) => {
      try {
        const result = await discoverRtUrls(
          body.feedId || undefined,
          undefined, // uses TRANSITLAND_API_KEY from env
          fetchFn,
        )

        return {
          checked: result.checked,
          updated: result.updated,
          errors: result.errors,
        }
      } catch (err) {
        set.status = 500
        return {
          error: 'Failed to discover RT URLs',
          detail: err instanceof Error ? err.message : String(err),
        }
      }
    }, {
      body: t.Object({
        feedId: t.Optional(t.String()),
      }),
      detail: {
        summary: 'Discover GTFS-RT feed URLs',
        description:
          'Queries Transitland to discover GTFS-RT feed URLs (vehicle positions, ' +
          'trip updates, alerts) for existing feeds. If feedId is provided, only ' +
          'discovers for that feed; otherwise discovers for all feeds missing RT URLs.',
        tags: ['Transit'],
      },
    })

    // ── Station infrastructure ────────────────────────────────────

    // Station detail: geometry, entrances, buildings
    .get('/station/:feedId/:stopId', async ({ params, set }) => {
      try {
        const result = await getStationDetail(params.feedId, params.stopId)
        if (!result) {
          set.status = 404
          return { error: 'Station not found' }
        }
        return result
      } catch (err) {
        set.status = 500
        return {
          error: 'Failed to get station detail',
          detail: err instanceof Error ? err.message : String(err),
        }
      }
    }, {
      params: t.Object({
        feedId: t.String(),
        stopId: t.String(),
      }),
      detail: {
        summary: 'Get station detail with entrances and building geometry',
        description:
          'Returns a GTFS station with its OSM-linked entrances (subway/train station ' +
          'entrances within 200m) and building polygon geometry. Entrances include ' +
          'descriptions, wheelchair accessibility, and level information.',
        tags: ['Transit'],
      },
    })

    // Nearest entrance to a coordinate
    .get('/nearest-entrance', async ({ query, set }) => {
      try {
        const lat = parseFloat(query.lat)
        const lon = parseFloat(query.lon)
        const maxDistance = query.maxDistance ? parseFloat(query.maxDistance) : 500

        if (isNaN(lat) || isNaN(lon)) {
          set.status = 400
          return { error: 'Invalid lat/lon' }
        }

        const result = await getNearestEntrance(
          lat, lon, maxDistance, query.wheelchair === 'true',
        )
        if (!result) {
          set.status = 404
          return { error: 'No entrance found nearby' }
        }
        return result
      } catch (err) {
        set.status = 500
        return {
          error: 'Failed to find nearest entrance',
          detail: err instanceof Error ? err.message : String(err),
        }
      }
    }, {
      query: t.Object({
        lat: t.String(),
        lon: t.String(),
        maxDistance: t.Optional(t.String()),
        wheelchair: t.Optional(t.String()),
      }),
      detail: {
        summary: 'Find nearest station entrance to a coordinate',
        description:
          'Returns the closest subway or train station entrance to the given ' +
          'coordinate, within maxDistance meters (default 500m). Pass ' +
          'wheelchair=true for accessible access points only: entrances ' +
          'tagged wheelchair=no are excluded, vertical access requires ' +
          'elevators instead of stairs, and confirmed-accessible entrances ' +
          'are preferred.',
        tags: ['Transit'],
      },
    })
}

export const transitRoutes = createTransitRoutes()
