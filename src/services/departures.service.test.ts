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

let STOP_ROWS: any[] = [
  { stop_id: 's-ferry', feed_id: '917', stop_name: 'East 34th Street', stop_lat: 40.74, stop_lon: -73.97, distance: 165.3 },
  { stop_id: 's-bus', feed_id: '7', stop_name: 'MARGINAL ST/E 34 ST', stop_lat: 40.74, stop_lon: -73.97, distance: 88.2 },
]

/** The Roosevelt Island Tramway: its stop is metres away, the buses across the street. */
const TRAM_AND_BUSES = [
  { stop_id: '2436149', feed_id: '3364', stop_name: 'Manhattan Tram Station', stop_lat: 40.76, stop_lon: -73.96, distance: 2.4 },
  { stop_id: '450338', feed_id: '34', stop_name: 'E 60 ST/2 AV', stop_lat: 40.76, stop_lon: -73.96, distance: 43.7 },
  { stop_id: '981016', feed_id: '34', stop_name: 'E 60 ST/2 AV', stop_lat: 40.76, stop_lon: -73.96, distance: 49.6 },
]

// A postgres-js stand-in that records instead of connecting. Running a real
// drizzle instance on top means the assertions see the SQL this codebase
// actually generates, not a reconstruction of it.
const client: any = () => Promise.resolve([])
/** What `resolveStations` reads: one row per (seed stop, station member). */
let STATION_ROWS: any[] = []

client.unsafe = (text: string, params: unknown[] = []) => {
  statements.push({ sql: text, params })
  const rows = text.includes('station_id')
    ? STATION_ROWS
    : text.includes('gtfs_stops')
      ? STOP_ROWS
      : []
  const result: any = Promise.resolve(rows)
  result.values = () => Promise.resolve(rows)
  result.execute = () => Promise.resolve(rows)
  return result
}
client.options = { parsers: {}, serializers: {} }

mock.module('../db', () => ({ db: drizzle(client), connection: client }))

const { getDepartures, parseServiceDate } = await import('./departures.service')
import type { FetchFn } from './departures.service'

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

const DEFAULT_STOP_ROWS = [...STOP_ROWS]

beforeEach(() => {
  statements.length = 0
  STOP_ROWS = [...DEFAULT_STOP_ROWS]
  STATION_ROWS = []
})

describe('narrowing to the station', () => {
  /** Which stops actually got queried for departures. */
  function queriedStops(result: Awaited<ReturnType<typeof getDepartures>>) {
    return result.map((r) => r.stop.name)
  }

  test('drops the bus stops across the street from a station', async () => {
    // The reported bug: opening the Roosevelt Island Tramway showed a board of
    // Q32, M15 and Q60 buses bound for Penn Station. Mode can't separate them —
    // the tram and the buses were both published as route_type 3 — so the stop
    // sitting essentially on the place claims the board.
    STOP_ROWS = TRAM_AND_BUSES

    const result = await getDepartures(
      { lat: 40.76, lng: -73.96, routeTypes: [5, 6, 0] },
      fetchFn,
    )

    expect(queriedStops(result)).toEqual(['Manhattan Tram Station'])
  })

  test('keeps every platform of the same station', async () => {
    // Both directions of one stop, and every platform of a complex, share a
    // name — those belong on the board together.
    STOP_ROWS = [
      { stop_id: 'a', feed_id: '34', stop_name: 'E 60 ST/2 AV', stop_lat: 40.76, stop_lon: -73.96, distance: 4 },
      { stop_id: 'b', feed_id: '34', stop_name: 'E 60 ST/2 AV', stop_lat: 40.76, stop_lon: -73.96, distance: 22 },
      { stop_id: 'c', feed_id: '7', stop_name: 'Somewhere Else', stop_lat: 40.76, stop_lon: -73.96, distance: 40 },
    ]

    const result = await getDepartures(
      { lat: 40.76, lng: -73.96, routeTypes: [3] },
      fetchFn,
    )

    expect(result).toHaveLength(2)
    expect(new Set(queriedStops(result))).toEqual(new Set(['E 60 ST/2 AV']))
  })

  test('prefers the GTFS parent station over the name', async () => {
    STOP_ROWS = [
      { stop_id: 'n', feed_id: '5', stop_name: '34 St-Penn Station', parent_station: 'P1', stop_lat: 40.75, stop_lon: -73.99, distance: 3 },
      { stop_id: 's', feed_id: '5', stop_name: '34 St (Penn)', parent_station: 'P1', stop_lat: 40.75, stop_lon: -73.99, distance: 12 },
      { stop_id: 'x', feed_id: '5', stop_name: '34 St-Penn Station', parent_station: 'P2', stop_lat: 40.75, stop_lon: -73.99, distance: 15 },
    ]

    const result = await getDepartures(
      { lat: 40.75, lng: -73.99, routeTypes: [1] },
      fetchFn,
    )

    expect(result.map((r) => r.stop.stopId).sort()).toEqual(['n', 's'])
  })

  test('leaves the merge alone when the nearest stop is merely nearby', async () => {
    // The E 34th St ferry terminal: its landing is 165 m out on the pier, so
    // nothing is "at" the place and every candidate stays in play.
    const result = await getDepartures(
      { lat: 40.74, lng: -73.97, routeTypes: [4] },
      fetchFn,
    )

    expect(result).toHaveLength(2)
  })

  test('never narrows a bare coordinate lookup', async () => {
    // Right-clicking the map asks "what can I catch from here", not "what does
    // this station run" — every nearby stop still counts.
    STOP_ROWS = TRAM_AND_BUSES

    const result = await getDepartures({ lat: 40.76, lng: -73.96 }, fetchFn)

    expect(result).toHaveLength(3)
  })
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

    // The stop is already named, so nothing is searched for by position. It is
    // still looked up by id, to find the station it is a platform of.
    expect(statements.some((s) => s.sql.includes('ST_DWithin'))).toBe(false)
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

describe('identifying the station by name', () => {
  /**
   * Brooklyn Bridge–City Hall as it really sits: the OSM stop_position is 37.8 m
   * from the Chambers St (J/Z) platforms and 52.2 m from its own, so ranking by
   * distance hands the station to its neighbour. Both are subway, so mode
   * cannot break the tie either — the name is the only thing that can.
   */
  const BROOKLYN_BRIDGE = [
    { stop_id: 'M21N', feed_id: '5', stop_name: 'Chambers St', parent_station: 'M21', stop_lat: 40.713243, stop_lon: -74.003401, distance: 37.8, mode_match: true },
    { stop_id: 'M21S', feed_id: '5', stop_name: 'Chambers St', parent_station: 'M21', stop_lat: 40.713243, stop_lon: -74.003401, distance: 37.8, mode_match: true },
    { stop_id: '640N', feed_id: '5', stop_name: 'Brooklyn Bridge-City Hall', parent_station: '640', stop_lat: 40.713065, stop_lon: -74.004131, distance: 52.2, mode_match: true },
    { stop_id: '640S', feed_id: '5', stop_name: 'Brooklyn Bridge-City Hall', parent_station: '640', stop_lat: 40.713065, stop_lon: -74.004131, distance: 52.2, mode_match: true },
  ]

  test('takes the stop that shares the place name over a nearer one', async () => {
    STOP_ROWS = BROOKLYN_BRIDGE
    STATION_ROWS = [
      { feed_id: '5', seed_id: '640N', station_id: '640', member_id: '640N' },
      { feed_id: '5', seed_id: '640S', station_id: '640', member_id: '640S' },
    ]

    const result = await getDepartures(
      {
        lat: 40.7134428,
        lng: -74.0037637,
        name: 'Brooklyn Bridge–City Hall',
        routeTypes: [1],
      },
      fetchFn,
    )

    expect(result.map((r) => r.stop.name)).toEqual(['Brooklyn Bridge-City Hall'])
  })

  test('folds the spelling apart an OSM name and a GTFS one', async () => {
    // "Grand Central–42nd Street" against "Grand Central-42 St": an en dash, a
    // spelled-out word and an ordinal suffix, and the same station.
    STOP_ROWS = [
      { stop_id: '724N', feed_id: '5', stop_name: '5 Av', parent_station: '724', stop_lat: 40.75, stop_lon: -73.98, distance: 9, mode_match: true },
      { stop_id: '631N', feed_id: '5', stop_name: 'Grand Central-42 St', parent_station: '631', stop_lat: 40.75, stop_lon: -73.97, distance: 61, mode_match: true },
    ]

    const result = await getDepartures(
      { lat: 40.7518728, lng: -73.9769434, name: 'Grand Central–42nd Street', routeTypes: [1] },
      fetchFn,
    )

    expect(result.map((r) => r.stop.name)).toEqual(['Grand Central-42 St'])
  })

  test('keeps mode ahead of the name', async () => {
    // A ferry terminal and the bus stop outside it often carry the same name on
    // the sign. The mode the place claims still decides.
    STOP_ROWS = [
      { stop_id: 'bus', feed_id: '7', stop_name: 'Wall St/Pier 11', stop_lat: 40.7, stop_lon: -74, distance: 12, mode_match: false },
      { stop_id: 'pier', feed_id: '917', stop_name: 'Wall St/Pier 11', stop_lat: 40.7, stop_lon: -74, distance: 140, mode_match: true },
    ]

    const result = await getDepartures(
      { lat: 40.7, lng: -74, name: 'Wall St/Pier 11', routeTypes: [4] },
      fetchFn,
    )

    expect(result.map((r) => r.stop.stopId)).toEqual(['pier'])
  })

  test('falls back to distance when the name matches nothing', async () => {
    // An OSM name the feed never uses must leave the search exactly as it was,
    // not narrow the board to whichever stop happened to sort first.
    STOP_ROWS = BROOKLYN_BRIDGE

    const result = await getDepartures(
      { lat: 40.7134428, lng: -74.0037637, name: 'Some Other Place', routeTypes: [1] },
      fetchFn,
    )

    expect(result).toHaveLength(4)
  })

  test('looks at more candidates once a name is given', async () => {
    // The name is matched in TypeScript, so the shortlist has to be long enough
    // to still hold the right station after distance has sorted it down.
    await getDepartures({ lat: 40.71, lng: -74, name: 'Anywhere', routeTypes: [1] }, fetchFn)
    const named = stopSearch().params.at(-1)

    statements.length = 0
    await getDepartures({ lat: 40.71, lng: -74, routeTypes: [1] }, fetchFn)
    const unnamed = stopSearch().params.at(-1)

    expect(Number(named)).toBeGreaterThan(Number(unnamed))
  })
})

describe('one board per station', () => {
  /** MOTIS runs at the given stops, in order, one minute apart. */
  function motisAt(stopIds: string[]): FetchFn {
    return async (url: string | URL) => {
      motisCalls.push(String(url))
      return new Response(
        JSON.stringify({
          place: { name: 'Brooklyn Bridge-City Hall', stopId: '5_640', lat: 0, lon: 0, tz: 'America/New_York' },
          stopTimes: stopIds.map((stopId, i) => ({
            place: {
              name: 'Somewhere', stopId, lat: 0, lon: 0,
              departure: `2026-08-15T10:0${i}:00Z`,
              scheduledDeparture: `2026-08-15T10:0${i}:00Z`,
            },
            mode: 'SUBWAY',
            realTime: false,
            routeId: `5_R${i}`,
            routeType: 1,
            tripId: `20260815_10:0${i}_5_${i}`,
          })),
        }),
        { headers: { 'content-type': 'application/json' } },
      )
    }
  }

  let motisCalls: string[] = []

  beforeEach(() => {
    motisCalls = []
    STOP_ROWS = [
      { stop_id: '640N', feed_id: '5', stop_name: 'Brooklyn Bridge-City Hall', parent_station: '640', stop_lat: 40.713, stop_lon: -74.004, distance: 52.2, mode_match: true },
      { stop_id: '640S', feed_id: '5', stop_name: 'Brooklyn Bridge-City Hall', parent_station: '640', stop_lat: 40.713, stop_lon: -74.004, distance: 52.2, mode_match: true },
    ]
    STATION_ROWS = [
      { feed_id: '5', seed_id: '640N', station_id: '640', member_id: '640N' },
      { feed_id: '5', seed_id: '640N', station_id: '640', member_id: '640S' },
      { feed_id: '5', seed_id: '640S', station_id: '640', member_id: '640N' },
      { feed_id: '5', seed_id: '640S', station_id: '640', member_id: '640S' },
    ]
  })

  test('asks once at the parent instead of once per platform', async () => {
    // MOTIS answers 640N and 640S with the same station-level list, so querying
    // both put every train on the board twice — the "Now, Now" a rider sees.
    const result = await getDepartures(
      { lat: 40.7134428, lng: -74.0037637, name: 'Brooklyn Bridge–City Hall', routeTypes: [1] },
      motisAt(['5_640N']),
    )

    expect(motisCalls).toHaveLength(1)
    expect(motisCalls[0]).toContain('stopId=5_640')
    expect(result).toHaveLength(1)
    expect(result[0].stop.stopId).toBe('640')
    expect(result[0].departures).toHaveLength(1)
  })

  test('drops runs at a stop that only shares the station name', async () => {
    // MOTIS resolves a stoptimes query to every same-named stop, and New York
    // has two unrelated Chambers St complexes 200 m apart. A run at the other
    // one is not a departure from here.
    const result = await getDepartures(
      { lat: 40.7134428, lng: -74.0037637, name: 'Brooklyn Bridge–City Hall', routeTypes: [1] },
      motisAt(['5_640N', '5_137N', '5_A36S', '5_640S']),
    )

    expect(result[0].departures.map((d) => d.tripId)).toEqual([
      '20260815_10:00_5_0',
      '20260815_10:03_5_3',
    ])
  })

  test('still reports more when the page was full before filtering', async () => {
    // A page MOTIS filled with a neighbour's runs still means the timetable had
    // more to give here; counting only what survived would say otherwise.
    const result = await getDepartures(
      { lat: 40.7134428, lng: -74.0037637, name: 'Brooklyn Bridge–City Hall', routeTypes: [1], n: 3 },
      motisAt(['5_640N', '5_137N', '5_A36S']),
    )

    expect(result[0].departures).toHaveLength(1)
    expect(result[0].hasMore).toBe(true)
  })
})

describe('a board only ever carries its own stop', () => {
  /**
   * MOTIS resolves a stoptimes query to every stop that shares the requested
   * stop's name, so the Chambers St J/Z platform answers with the 1, 2, 3, A
   * and C of the unrelated Chambers St 200 m away. A board names the stop it is
   * for; carrying a neighbour's runs makes that name a lie. This held once the
   * caller identified the station, and not otherwise — which is exactly the
   * case a bare coordinate hits.
   */
  test('filters even when nothing identified the station', async () => {
    STOP_ROWS = [
      { stop_id: 'M21N', feed_id: '5', stop_name: 'Chambers St', parent_station: 'M21', stop_lat: 40.713, stop_lon: -74.003, distance: 96 },
    ]
    STATION_ROWS = [
      { feed_id: '5', seed_id: 'M21N', station_id: 'M21', member_id: 'M21N' },
      { feed_id: '5', seed_id: 'M21N', station_id: 'M21', member_id: 'M21S' },
    ]

    // No name and no routeTypes — nothing tells us which station this is, and
    // the one candidate is too far away to claim the place.
    const result = await getDepartures(
      { lat: 40.7134, lng: -74.0037 },
      motisMixed(['5_M21N', '5_137N', '5_A36S']),
    )

    expect(result).toHaveLength(1)
    expect(result[0].departures.map((d) => d.route.shortName)).toEqual(['J'])
  })

  /** Runs at the given stops, one route each, a minute apart. */
  function motisMixed(stopIds: string[]): FetchFn {
    const routes: Record<string, string> = { '5_M21N': 'J', '5_137N': '1', '5_A36S': 'C' }
    return async () =>
      new Response(
        JSON.stringify({
          place: { name: 'Chambers St', stopId: '5_M21', lat: 0, lon: 0, tz: 'America/New_York' },
          stopTimes: stopIds.map((stopId, i) => ({
            place: {
              name: 'Chambers St', stopId, lat: 0, lon: 0,
              departure: `2026-08-15T10:0${i}:00Z`,
              scheduledDeparture: `2026-08-15T10:0${i}:00Z`,
            },
            mode: 'SUBWAY', realTime: false,
            routeId: `5_${routes[stopId]}`, routeShortName: routes[stopId],
            routeType: 1, tripId: `20260815_10:0${i}_5_${i}`,
          })),
        }),
        { headers: { 'content-type': 'application/json' } },
      )
  }
})
