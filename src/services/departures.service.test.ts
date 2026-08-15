/**
 * Mode-aware stop matching for the departure board.
 *
 * The first stop returned names the station and supplies its route list, so
 * picking it by distance alone mislabels every place whose GTFS point sits away
 * from its OSM node: a ferry terminal becomes the bus stop on the street, an
 * aerial tramway station becomes the bus stop underneath it (parchment PAR-288).
 *
 * Two properties matter and both are easy to break:
 *   - `routeTypes` is a PREFERENCE, not a filter. Feeds mistype their routes
 *     (the Roosevelt Island tram is published as a bus), so a nearby stop of
 *     any mode must still come back when nothing matches the mode.
 *   - omitting `routeTypes` must behave exactly as before.
 */
import { describe, test, expect, mock, beforeEach } from 'bun:test'
import { drizzle } from 'drizzle-orm/postgres-js'

const statements: Array<{ sql: string; params: unknown[] }> = []

const STOP_ROWS = [
  { stop_id: 's-ferry', feed_id: '917', stop_name: 'East 34th Street', stop_lat: 40.74, stop_lon: -73.97, distance: 165.3 },
  { stop_id: 's-bus', feed_id: '7', stop_name: 'MARGINAL ST/E 34 ST', stop_lat: 40.74, stop_lon: -73.97, distance: 88.2 },
]

// A postgres-js stand-in that records instead of connecting. Running a real
// drizzle instance on top means the assertions see the SQL this codebase
// actually generates, not a reconstruction of it.
const client: any = () => Promise.resolve([])
client.unsafe = (text: string, params: unknown[] = []) => {
  statements.push({ sql: text, params })
  const rows = text.includes('gtfs_stops') ? STOP_ROWS : []
  const result: any = Promise.resolve(rows)
  result.values = () => Promise.resolve(rows)
  result.execute = () => Promise.resolve(rows)
  return result
}
client.options = { parsers: {}, serializers: {} }

mock.module('../db', () => ({ db: drizzle(client), connection: client }))

const { getDepartures } = await import('./departures.service')

/** MOTIS stand-in — the stop search is what's under test, not the timetable. */
const fetchFn = async () =>
  new Response(JSON.stringify({ stopTimes: [], place: { name: '', stopId: '', lat: 0, lon: 0 } }), {
    headers: { 'content-type': 'application/json' },
  })

/** The stop-search statement, which is the only one that reads gtfs_stops. */
function stopSearch() {
  return statements.find((s) => s.sql.includes('gtfs_stops'))!
}

beforeEach(() => {
  statements.length = 0
})

describe('nearby stop search', () => {
  test('ranks stops of the requested mode first', async () => {
    await getDepartures({ lat: 40.74, lng: -73.97, routeTypes: [4, 1200] }, fetchFn)

    const { sql, params } = stopSearch()
    expect(sql).toContain('mode_match')
    expect(sql).toContain('gtfs_stop_routes')
    // Bound, never interpolated — the values come in off a query string.
    expect(params).toContain(4)
    expect(params).toContain(1200)
    expect(sql).not.toContain('1200')
  })

  test('reaches further for a mode match than for anything else', async () => {
    await getDepartures({ lat: 40.74, lng: -73.97, radius: 150, routeTypes: [4] }, fetchFn)

    const { params } = stopSearch()
    // The wide radius bounds the scan; the caller's radius still gates
    // non-matching stops, so a bus stop 300m away is not picked up.
    expect(params).toContain(400)
    expect(params).toContain(150)
  })

  test('keeps non-matching stops as a fallback', async () => {
    const result = await getDepartures({ lat: 40.74, lng: -73.97, routeTypes: [4] }, fetchFn)

    expect(result.map((r) => r.stop.stopId)).toEqual(['s-ferry', 's-bus'])
  })

  test('never widens the radius when no mode is requested', async () => {
    await getDepartures({ lat: 40.74, lng: -73.97, radius: 150 }, fetchFn)

    const { sql, params } = stopSearch()
    expect(sql).not.toContain('gtfs_stop_routes')
    expect(params).not.toContain(400)
  })

  test('skips the spatial search entirely for a direct stop query', async () => {
    await getDepartures({ lat: 0, lng: 0, feedId: 'f-1', stopId: 's-1', routeTypes: [4] }, fetchFn)

    expect(statements.some((s) => s.sql.includes('gtfs_stops'))).toBe(false)
  })
})
