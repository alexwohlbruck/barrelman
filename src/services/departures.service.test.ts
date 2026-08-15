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

const { getDepartures, parseServiceDate } = await import('./departures.service')

/** MOTIS stand-in — the stop search is what's under test, not the timetable. */
const fetchFn = async () =>
  new Response(JSON.stringify({ stopTimes: [], place: { name: '', stopId: '', lat: 0, lon: 0 } }), {
    headers: { 'content-type': 'application/json' },
  })

/** A MOTIS stoptimes response with runs at the given absolute times. */
function motisWith(times: string[]): typeof fetchFn {
  return async () =>
    new Response(
      JSON.stringify({
        place: { name: 'Somewhere', stopId: 'f_s', lat: 0, lon: 0, tz: 'America/New_York' },
        stopTimes: times.map((departure, i) => ({
          place: { name: 'Somewhere', stopId: 'f_s', lat: 0, lon: 0, departure, scheduledDeparture: departure },
          mode: 'BUS',
          realTime: false,
          routeId: 'f_R1',
          routeType: 3,
          tripId: `20260815_08:0${i}_f_${i}`,
        })),
      }),
      { headers: { 'content-type': 'application/json' } },
    )
}

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

describe('time window', () => {
  const base = '2026-08-15T10:00:00Z'

  test('drops runs past the window', async () => {
    const result = await getDepartures(
      {
        lat: 0, lng: 0, feedId: 'f', stopId: 's', time: base, windowMinutes: 60,
      },
      motisWith(['2026-08-15T10:30:00Z', '2026-08-15T12:30:00Z']),
    )

    expect(result[0].departures.map((d) => d.departureTime)).toEqual(['2026-08-15T10:30:00Z'])
  })

  test('flags that more exist once anything was trimmed', async () => {
    const result = await getDepartures(
      { lat: 0, lng: 0, feedId: 'f', stopId: 's', time: base, windowMinutes: 60 },
      motisWith(['2026-08-15T10:30:00Z', '2026-08-15T12:30:00Z']),
    )

    expect(result[0].hasMore).toBe(true)
  })

  test('flags more when MOTIS filled the page, even with nothing trimmed', async () => {
    // A full page means the timetable had more to give — the board can still
    // offer "show later" honestly.
    const result = await getDepartures(
      { lat: 0, lng: 0, feedId: 'f', stopId: 's', time: base, n: 2, windowMinutes: 600 },
      motisWith(['2026-08-15T10:30:00Z', '2026-08-15T10:40:00Z']),
    )

    expect(result[0].departures).toHaveLength(2)
    expect(result[0].hasMore).toBe(true)
  })

  test('comes back empty rather than reaching past a stop that is shut', async () => {
    // The caller has to be able to tell "nothing for hours" from "nothing at
    // all" — that distinction is what stops a run weeks out being rendered
    // beside trains due in minutes.
    const result = await getDepartures(
      { lat: 0, lng: 0, feedId: 'f', stopId: 's', time: base, windowMinutes: 60 },
      motisWith(['2026-08-16T06:00:00Z']),
    )

    expect(result[0].departures).toHaveLength(0)
    expect(result[0].hasMore).toBe(true)
  })

  test('keeps everything when no window is asked for', async () => {
    const result = await getDepartures(
      { lat: 0, lng: 0, feedId: 'f', stopId: 's', time: base },
      motisWith(['2026-08-15T10:30:00Z', '2026-10-03T12:30:00Z']),
    )

    expect(result[0].departures).toHaveLength(2)
  })
})

describe('parseServiceDate', () => {
  test('reads the service date off a MOTIS trip id', () => {
    expect(parseServiceDate('20260815_08:21_917_2729')).toBe('2026-08-15')
  })

  test('keeps the service date of an after-midnight run', () => {
    // 24:49 is Friday's timetable departing 01:00 Saturday. The prefix is the
    // only place that survives into the stoptimes response.
    expect(parseServiceDate('20260815_24:49_5_1633')).toBe('2026-08-15')
  })

  test('returns nothing for an id that carries no date', () => {
    expect(parseServiceDate('trip-42')).toBeUndefined()
    expect(parseServiceDate('')).toBeUndefined()
  })

  test('rejects a prefix that is not a real date', () => {
    expect(parseServiceDate('20261915_08:21_917_1')).toBeUndefined()
  })
})
