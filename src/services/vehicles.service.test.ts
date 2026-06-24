import { describe, test, expect, mock, beforeEach } from 'bun:test'

// ── Mock dependencies ───────────────────────────────────────────────

// Mock the database
const mockDbExecute = mock(async () => [])
mock.module('../db', () => ({
  db: { execute: mockDbExecute },
}))

// Mock gtfs-realtime-bindings
const mockDecode = mock((buffer: Uint8Array) => ({
  header: { timestamp: { low: 1717200000, high: 0 } },
  entity: [
    {
      id: 'entity-1',
      vehicle: {
        trip: { tripId: 'trip-100', routeId: 'route-9x' },
        vehicle: { id: 'bus-42', label: 'Bus 42' },
        position: { latitude: 35.21, longitude: -80.85, bearing: 180, speed: 12.5 },
        timestamp: { low: 1717200000, high: 0 },
      },
    },
    {
      id: 'entity-2',
      vehicle: {
        trip: { tripId: 'trip-200', routeId: 'route-4' },
        vehicle: { id: 'bus-77', label: 'Bus 77' },
        position: { latitude: 35.22, longitude: -80.84, bearing: 90, speed: 8.0 },
        timestamp: { low: 1717200000, high: 0 },
      },
    },
    // Entity with no position — should be skipped
    {
      id: 'entity-3',
      vehicle: {
        trip: { tripId: 'trip-300' },
        vehicle: { id: 'ghost' },
        position: null,
      },
    },
  ],
}))

mock.module('gtfs-realtime-bindings', () => ({
  default: {
    transit_realtime: {
      FeedMessage: { decode: mockDecode },
    },
  },
}))

// ── Import under test ───────────────────────────────────────────────

const { getVehiclePositions } = await import('./vehicles.service')

// ── Test fixtures ───────────────────────────────────────────────────

function mockFetch(body: ArrayBuffer, status = 200): typeof fetch {
  return mock(async () =>
    new Response(body, { status }),
  ) as any
}

const CHARLOTTE_BBOX = {
  north: 35.30,
  south: 35.15,
  east: -80.70,
  west: -80.90,
}

// ── Tests ───────────────────────────────────────────────────────────

describe('VehiclesService — getVehiclePositions', () => {
  beforeEach(() => {
    mockDbExecute.mockReset()
    mockDecode.mockClear()
  })

  test('returns empty when no feeds have RT URLs', async () => {
    mockDbExecute.mockImplementation(async () => [])

    const result = await getVehiclePositions(CHARLOTTE_BBOX)
    expect(result.vehicles).toEqual([])
    expect(result.feedTimestamps).toEqual({})
  })

  test('fetches and parses vehicle positions from feeds', async () => {
    // First call: feeds query — return feed with vehicle position URL
    // Second call: route enrichment
    let callCount = 0
    mockDbExecute.mockImplementation(async () => {
      callCount++
      if (callCount === 1) {
        return [{
          feed_id: '86',
          rt_urls: JSON.stringify([
            { url: 'https://example.com/tripUpdates' },
            { url: 'https://example.com/vehiclePositions' },
            { url: 'https://example.com/alerts' },
          ]),
        }]
      }
      // Route info query
      return [{
        feed_id: '86',
        route_id: 'route-9x',
        route_short_name: '9X',
        route_color: '0000FF',
        route_text_color: 'FFFFFF',
        route_type: '3',
      }, {
        feed_id: '86',
        route_id: 'route-4',
        route_short_name: '4',
        route_color: 'FF0000',
        route_text_color: 'FFFFFF',
        route_type: '3',
      }]
    })

    const fetchFn = mockFetch(new ArrayBuffer(0))

    const result = await getVehiclePositions(CHARLOTTE_BBOX, fetchFn)

    expect(result.vehicles.length).toBe(2)
    expect(result.vehicles[0].vehicleId).toBe('86_bus-42')
    expect(result.vehicles[0].position.lat).toBe(35.21)
    expect(result.vehicles[0].position.lng).toBe(-80.85)
    expect(result.vehicles[0].bearing).toBe(180)
    expect(result.vehicles[0].speed).toBe(12.5)
    expect(result.vehicles[0].routeId).toBe('route-9x')
    expect(result.vehicles[0].feedId).toBe('86')

    // Route enrichment
    expect(result.vehicles[0].routeShortName).toBe('9X')
    expect(result.vehicles[0].routeColor).toBe('0000FF')
  })

  test('filters vehicles outside bounding box', async () => {
    let callCount = 0
    mockDbExecute.mockImplementation(async () => {
      callCount++
      if (callCount === 1) {
        return [{
          feed_id: '99',
          rt_urls: JSON.stringify([
            { url: 'https://example.com/vehiclePositions' },
          ]),
        }]
      }
      return []
    })

    const fetchFn = mockFetch(new ArrayBuffer(0))

    // Use a small bounding box that excludes vehicle at 35.22, -80.84
    const narrowBbox = {
      north: 35.215,
      south: 35.205,
      east: -80.845,
      west: -80.855,
    }

    const result = await getVehiclePositions(narrowBbox, fetchFn)

    // Only bus-42 at (35.21, -80.85) should be in the box
    expect(result.vehicles.length).toBe(1)
    expect(result.vehicles[0].vehicleId).toBe('99_bus-42')
  })

  test('handles feed fetch failure gracefully', async () => {
    let callCount = 0
    mockDbExecute.mockImplementation(async () => {
      callCount++
      if (callCount === 1) {
        return [{
          feed_id: '503-fail',
          rt_urls: JSON.stringify([{ url: 'https://failing.example.com/vehiclePositions' }]),
        }]
      }
      return []
    })

    // Return 503 error
    const fetchFn = mock(async () =>
      new Response('Service Unavailable', { status: 503 }),
    ) as any

    const result = await getVehiclePositions(CHARLOTTE_BBOX, fetchFn)
    expect(result.vehicles).toEqual([])
  })

  test('identifies vehicle position URLs by pattern', async () => {
    let callCount = 0
    mockDbExecute.mockImplementation(async () => {
      callCount++
      if (callCount === 1) {
        return [{
          feed_id: '777',
          rt_urls: JSON.stringify([
            { url: 'https://gtfs-rt.example.com/tripupdate/tripupdates.pb' },
            { url: 'https://gtfs-rt.example.com/vehicle/vehiclepositions.pb' },
          ]),
        }]
      }
      return []
    })

    const fetchFn = mockFetch(new ArrayBuffer(0))
    await getVehiclePositions(CHARLOTTE_BBOX, fetchFn)

    // Should have called fetch with the vehicle positions URL
    expect(fetchFn).toHaveBeenCalled()
    const calledUrl = (fetchFn as any).mock.calls[0][0]
    expect(calledUrl).toContain('vehiclepositions')
  })

  test('skips entities without position data', async () => {
    let callCount = 0
    mockDbExecute.mockImplementation(async () => {
      callCount++
      if (callCount === 1) {
        return [{
          feed_id: '888',
          rt_urls: JSON.stringify([{ url: 'https://example.com/vehiclePositions' }]),
        }]
      }
      return []
    })

    const fetchFn = mockFetch(new ArrayBuffer(0))
    const result = await getVehiclePositions(CHARLOTTE_BBOX, fetchFn)

    // entity-3 has null position, should be skipped
    expect(result.vehicles.every(v => v.vehicleId !== '888_ghost')).toBe(true)
  })
})
