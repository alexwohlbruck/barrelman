/**
 * GBFS shared-mobility REST endpoints.
 *
 * Exposes nearby station queries with real-time availability
 * from GBFS feeds (bikeshare, scootershare, etc.).
 */

import { Elysia, t } from 'elysia'
import { authHandler } from '../middleware/auth'
import {
  getNearbyStations as _getNearbyStations,
  getSystemsInBounds as _getSystemsInBounds,
  getStation as _getStation,
} from '../services/gbfs.service'

export function createGbfsRoutes(deps: {
  getNearbyStations?: typeof _getNearbyStations
  getSystemsInBounds?: typeof _getSystemsInBounds
  getStation?: typeof _getStation
} = {}) {
  const getNearbyStations = deps.getNearbyStations || _getNearbyStations
  const getSystemsInBounds = deps.getSystemsInBounds || _getSystemsInBounds
  const getStation = deps.getStation || _getStation

  return new Elysia({ prefix: '/gbfs' })
    .onBeforeHandle(authHandler)

    // ── GET /gbfs/nearby-stations ──────────────────────────────────
    .get('/nearby-stations', async ({ query, set }) => {
      try {
        const lat = parseFloat(query.lat)
        const lng = parseFloat(query.lng)
        if (isNaN(lat) || isNaN(lng)) {
          set.status = 400
          return { error: 'lat and lng are required (numbers)' }
        }

        return await getNearbyStations({
          lat,
          lng,
          radius: query.radius ? parseFloat(query.radius) : undefined,
          vehicleType: query.vehicleType as 'bike' | 'ebike' | 'scooter' | undefined,
          limit: query.limit ? parseInt(query.limit, 10) : undefined,
        })
      } catch (err) {
        set.status = 500
        return {
          error: 'Failed to fetch nearby stations',
          detail: err instanceof Error ? err.message : String(err),
        }
      }
    }, {
      query: t.Object({
        lat: t.String(),
        lng: t.String(),
        radius: t.Optional(t.String()),
        vehicleType: t.Optional(t.String()),
        limit: t.Optional(t.String()),
      }),
      detail: {
        summary: 'Find nearby shared-mobility stations with real-time availability',
        tags: ['GBFS'],
      },
    })

    // ── GET /gbfs/station ──────────────────────────────────────────
    // Single station with live availability. Looked up by exact
    // (systemId, stationId) — from an OSM node's `ref:gbfs` tag — with a
    // proximity fallback when only coordinates are known.
    .get('/station', async ({ query, set }) => {
      try {
        const lat = query.lat ? parseFloat(query.lat) : undefined
        const lng = query.lng ? parseFloat(query.lng) : undefined

        if (!query.systemId && !query.stationId && (lat == null || lng == null)) {
          set.status = 400
          return { error: 'provide systemId+stationId or lat+lng' }
        }

        const station = await getStation({
          systemId: query.systemId,
          stationId: query.stationId,
          lat,
          lng,
          radius: query.radius ? parseFloat(query.radius) : undefined,
        })

        if (!station) {
          set.status = 404
          return { error: 'station not found' }
        }
        return station
      } catch (err) {
        set.status = 500
        return {
          error: 'Failed to fetch station',
          detail: err instanceof Error ? err.message : String(err),
        }
      }
    }, {
      query: t.Object({
        systemId: t.Optional(t.String()),
        stationId: t.Optional(t.String()),
        lat: t.Optional(t.String()),
        lng: t.Optional(t.String()),
        radius: t.Optional(t.String()),
      }),
      detail: {
        summary: 'Single shared-mobility station with real-time availability',
        tags: ['GBFS'],
      },
    })

    // ── GET /gbfs/systems ──────────────────────────────────────────
    .get('/systems', async ({ query, set }) => {
      try {
        const north = parseFloat(query.north)
        const south = parseFloat(query.south)
        const east = parseFloat(query.east)
        const west = parseFloat(query.west)

        if ([north, south, east, west].some(isNaN)) {
          set.status = 400
          return { error: 'north, south, east, west are required (numbers)' }
        }

        const systems = await getSystemsInBounds(north, south, east, west)
        return { systems }
      } catch (err) {
        set.status = 500
        return {
          error: 'Failed to fetch GBFS systems',
          detail: err instanceof Error ? err.message : String(err),
        }
      }
    }, {
      query: t.Object({
        north: t.String(),
        south: t.String(),
        east: t.String(),
        west: t.String(),
      }),
      detail: {
        summary: 'List GBFS systems within a bounding box',
        tags: ['GBFS'],
      },
    })
}

export const gbfsRoutes = createGbfsRoutes()
