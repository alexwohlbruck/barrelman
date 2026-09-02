/**
 * Departures Service
 *
 * Queries the MOTIS stoptimes API to provide transit departure boards.
 * Enriches results with route colors from the GTFS database.
 *
 * This replaces the Transitland REST API as the departure data source.
 * The adapter pattern allows MOTIS to be swapped for another timetable
 * engine in the future without changing the external API contract.
 */

import { db } from '../db'
import { sql } from 'drizzle-orm'
import { sameStationName } from '../lib/station-name'

// ── Types ───────────────────────────────────────────────────────────

export type FetchFn = (url: string, init?: RequestInit) => Promise<Response>

export interface DepartureRequest {
  /** Coordinates to search near */
  lat: number
  lng: number
  /** Search radius in meters (default 150) */
  radius?: number
  /** ISO 8601 time to query from (default: now) */
  time?: string
  /** Max departures per stop (default 50) */
  n?: number
  /** Specific feedId (skip spatial search) */
  feedId?: string
  /** Specific stopId (skip spatial search) */
  stopId?: string
  /** Keep only departures whose route short name is in this set. Powers the
   *  merged "4 or 5" departure board — pass the interchangeable routes. */
  routeShortNames?: string[]
  /** Keep only departures in this GTFS direction ("0"/"1"). A platform stop
   *  can return both directions, so the board filters to the rider's way. */
  directionId?: string
  /** GTFS route types the caller expects here, derived from the place's own
   *  tags. Stops served by a matching route rank first and are searched out to
   *  a wider radius — see `MODE_MATCH_RADIUS`. */
  routeTypes?: number[]
  /** The place's own name. A station has an identity, and distance alone gets
   *  it wrong: the Brooklyn Bridge–City Hall node is nearer the Chambers St
   *  platforms than its own. A stop whose name folds to this one claims the
   *  board outright — see `src/lib/station-name.ts`. */
  name?: string
  /** Drop departures further out than this many minutes. MOTIS's `n` is a plain
   *  event count with no time bound, so the horizon it buys swings wildly with
   *  service frequency — 50 events is 45 minutes at a subway platform and five
   *  hours at a ferry landing. Bounding by time makes the board mean the same
   *  thing everywhere. Callers still choose `n`; this only trims. */
  windowMinutes?: number
}

export interface StopDepartures {
  stop: {
    stopId: string
    feedId: string
    name: string
    code?: string
    lat: number
    lng: number
    timezone: string
    distance?: number
  }
  departures: Departure[]
  /** More runs exist past what was returned — either trimmed by `windowMinutes`
   *  or cut off by `n`. Lets a caller offer "show more" honestly. */
  hasMore?: boolean
  nextPageCursor?: string
  previousPageCursor?: string
}

export interface Departure {
  tripId: string
  /** GTFS service date this run belongs to (YYYY-MM-DD), which is NOT always
   *  the calendar date it departs on: a 01:00 train is filed under the previous
   *  day's service, published as a 25:00 stop time. Lets a board tell "tonight's
   *  last run" from "tomorrow's first". */
  serviceDate?: string
  route: {
    id: string
    feedId: string
    shortName?: string
    longName?: string
    type: number
    color?: string
    textColor?: string
    agencyId?: string
    agencyName?: string
  }
  headsign?: string
  directionId?: string
  departureTime: string
  arrivalTime: string
  scheduledDepartureTime: string
  scheduledArrivalTime: string
  delay?: number
  realTime: boolean
  cancelled: boolean
  mode: string
  tripOrigin?: string
  tripDestination?: string
}

// ── MOTIS response types ────────────────────────────────────────────

interface MotisStopTimesResponse {
  stopTimes: MotisStopTime[]
  place: {
    name: string
    stopId: string
    lat: number
    lon: number
    stopCode?: string
    tz?: string
  }
  previousPageCursor?: string
  nextPageCursor?: string
}

interface MotisStopTime {
  place: {
    name: string
    stopId: string
    lat: number
    lon: number
    tz?: string
    arrival?: string
    departure?: string
    scheduledArrival?: string
    scheduledDeparture?: string
    stopCode?: string
    cancelled?: boolean
  }
  mode: string
  realTime: boolean
  headsign?: string
  tripFrom?: { name: string }
  tripTo?: { name: string }
  agencyId?: string
  agencyName?: string
  routeId: string
  routeShortName?: string
  routeLongName?: string
  routeType: number
  tripId: string
  directionId?: string
  cancelled?: boolean
  tripCancelled?: boolean
}

// ── Internals ───────────────────────────────────────────────────────

function getMotisUrl(): string {
  return process.env.MOTIS_URL || 'http://barrelman-motis:8080'
}

/**
 * Parse a MOTIS stop ID (format: `{feedId}_{stopId}`) into parts.
 */
function parseMotisId(motisId: string): { feedId: string; stopId: string } {
  const sep = motisId.indexOf('_')
  if (sep === -1) return { feedId: '', stopId: motisId }
  return { feedId: motisId.slice(0, sep), stopId: motisId.slice(sep + 1) }
}

/**
 * Pull the GTFS service date out of a MOTIS trip id.
 *
 * MOTIS mints ids as `{YYYYMMDD}_{HH:MM}_{feed}_{n}`, where the date is the
 * service date and the time is the trip's scheduled start — which may exceed
 * 24:00 for a run that crosses midnight. That prefix is the only place the
 * service date survives into the stoptimes response, and it is what separates
 * `20260815_24:49` (still Friday's timetable, departing 01:00 Saturday) from
 * `20260816_00:29` (Saturday's own first runs) at the same platform minute.
 *
 * Returns undefined for any id that doesn't carry the prefix, so a change in
 * MOTIS's id scheme degrades to "no service date" rather than to a wrong one.
 */
const MOTIS_TRIP_ID_DATE = /^(\d{4})(\d{2})(\d{2})_/

export function parseServiceDate(tripId: string): string | undefined {
  const match = MOTIS_TRIP_ID_DATE.exec(tripId)
  if (!match) return undefined

  const [, year, month, day] = match
  // Reject a prefix that parses as a date but isn't one (e.g. month 19).
  const asDate = new Date(`${year}-${month}-${day}T00:00:00Z`)
  if (isNaN(asDate.getTime()) || asDate.getUTCMonth() + 1 !== Number(month)) return undefined

  return `${year}-${month}-${day}`
}

/**
 * Compute delay in seconds from scheduled vs actual times.
 */
function computeDelay(actual?: string, scheduled?: string): number | undefined {
  if (!actual || !scheduled) return undefined
  const diff = new Date(actual).getTime() - new Date(scheduled).getTime()
  if (isNaN(diff)) return undefined
  return Math.round(diff / 1000)
}

/**
 * How far out to look for a stop whose mode matches the caller's, when
 * `routeTypes` is given. A ferry landing's GTFS point sits at the end of the
 * pier and an aerial tramway's out under the cables, so the stop that actually
 * belongs to the place is routinely further away than an unrelated bus stop on
 * the street outside. Mode-matched stops get this reach; everything else stays
 * within the caller's radius.
 */
const MODE_MATCH_RADIUS = 400

/** How many stops a board merges when nothing identifies a single station. */
const MERGED_STOPS = 5

/**
 * How many candidates to pull back when the caller named the place. The name
 * match is decided in TypeScript — the fold is a word dictionary, not something
 * to write twice in SQL — so the shortlist has to be long enough to still hold
 * the right station after distance has sorted it down the list. A station
 * complex can easily put a dozen platforms nearer than the one that shares the
 * place's name.
 */
const IDENTITY_CANDIDATES = 25

interface NearbyStop {
  feedId: string
  stopId: string
  name: string
  code?: string
  lat: number
  lng: number
  distance?: number
  parentStation?: string
  /** Served by a route of the mode the caller asked for. */
  modeMatch?: boolean
}

/**
 * Find nearby GTFS stops using PostGIS spatial index.
 *
 * When `routeTypes` is supplied, stops served by a route of one of those types
 * sort ahead of merely closer ones — the first stop returned names the station
 * and supplies its route list, so a ferry terminal must not be identified by
 * the bus stop across the street. The preference is a ranking, never a filter:
 * feeds mistype their routes (the Roosevelt Island tram is published as a bus),
 * so a nearby stop of any mode is still better than nothing.
 */
async function findNearbyStops(
  lat: number,
  lng: number,
  radius: number,
  limit: number,
  routeTypes?: number[],
): Promise<NearbyStop[]> {
  const modeMatch = routeTypes?.length
    ? sql`EXISTS (
        SELECT 1 FROM gtfs_stop_routes sr
        JOIN gtfs_routes r ON r.feed_id = sr.feed_id AND r.route_id = sr.route_id
        WHERE sr.feed_id = s.feed_id AND sr.stop_id = s.stop_id
          AND r.route_type IN (${sql.join(routeTypes.map((t) => sql`${t}`), sql`, `)})
      )`
    : sql`FALSE`
  const searchRadius = routeTypes?.length ? Math.max(radius, MODE_MATCH_RADIUS) : radius

  const result = await db.execute(sql`
    WITH candidates AS (
      SELECT
        s.stop_id,
        s.feed_id,
        s.stop_name,
        s.stop_code,
        s.stop_lat,
        s.stop_lon,
        s.parent_station,
        ST_Distance(
          s.geom::geography,
          ST_SetSRID(ST_MakePoint(${lng}, ${lat}), 4326)::geography
        ) AS distance,
        ${modeMatch} AS mode_match
      FROM gtfs_stops s
      WHERE ST_DWithin(
        s.geom::geography,
        ST_SetSRID(ST_MakePoint(${lng}, ${lat}), 4326)::geography,
        ${searchRadius}
      )
      AND (s.location_type = 0 OR s.location_type IS NULL)
    )
    SELECT * FROM candidates
    WHERE mode_match OR distance <= ${radius}
    ORDER BY mode_match DESC, distance
    LIMIT ${limit}
  `)

  return (result as any[]).map((row: any) => ({
    feedId: row.feed_id,
    stopId: row.stop_id,
    name: row.stop_name || '',
    code: row.stop_code || undefined,
    lat: row.stop_lat,
    lng: row.stop_lon,
    distance: Math.round(row.distance * 10) / 10,
    parentStation: row.parent_station || undefined,
    modeMatch: row.mode_match === true,
  }))
}

/**
 * How close a stop has to be before it counts as *being* the place rather than
 * standing near it. The Roosevelt Island Tramway's stop is 2.4 m from the OSM
 * way; the bus stops that were crowding its board are 43 m and further, across
 * the street.
 */
const AT_THE_PLACE_RADIUS = 25

/** Same station: the GTFS parent when both declare one, otherwise the name. */
function sameStation(
  a: { feedId: string; name: string; parentStation?: string },
  b: { feedId: string; name: string; parentStation?: string },
): boolean {
  if (a.feedId !== b.feedId) return false
  if (a.parentStation && b.parentStation) return a.parentStation === b.parentStation
  return a.name.trim().toLowerCase() === b.name.trim().toLowerCase()
}

/**
 * Promote the stop that shares the place's name, without disturbing anything
 * else about the order.
 *
 * Name outranks distance but not mode: a place tagged as a subway station whose
 * name also hangs on the bus stop outside is still the subway station. Within
 * each (mode, name) tier the sort is stable, so SQL's distance ordering — the
 * only thing that ranked stops before — decides as it always did. With no name
 * to match on, every stop ties and the list comes back untouched.
 */
function rankByIdentity<T extends NearbyStop>(stops: T[], name?: string): T[] {
  if (!name) return stops

  const tier = (stop: T) => (stop.modeMatch ? 2 : 0) + (sameStationName(name, stop.name) ? 1 : 0)
  return [...stops].sort((a, b) => tier(b) - tier(a))
}

/**
 * Narrow a nearby-stop list to the station the place actually *is*.
 *
 * Merging every stop within the radius is right for a bare coordinate — tell me
 * what I can catch from here — but wrong for a station, which has an identity.
 * Opening the Roosevelt Island Tramway and reading a board of Q32, M15 and Q60
 * buses bound for Penn Station is not a departure board for that station.
 *
 * Two things can establish that identity. A shared name is the strong one, and
 * it holds at any distance the search reached: the Brooklyn Bridge–City Hall
 * platforms are 52 m from the node that names them and still theirs, even
 * though Chambers St is nearer. Failing that, proximity — RIOC publishes the
 * Roosevelt Island tramway as `route_type=3`, the same code as the buses
 * underneath it, so mode cannot separate those and 2.4 m against 43 m can.
 *
 * When neither applies, nothing is dropped and the old merge stands.
 */
function narrowToStation<T extends NearbyStop>(stops: T[], name?: string): { stops: T[] } {
  const primary = stops[0]
  if (!primary) return { stops }

  const identified =
    (name != null && sameStationName(name, primary.name)) ||
    (primary.distance ?? Infinity) <= AT_THE_PLACE_RADIUS
  if (!identified) return { stops }

  const complex = stops.filter((stop) => sameStation(primary, stop))
  return { stops: complex.length ? complex : stops }
}

/** A station resolved from one of its stops: what to ask MOTIS for, and which
 *  stop ids the answer is allowed to be about. */
interface Station {
  /** The GTFS parent when the stop has one, otherwise the stop itself. */
  stationId: string
  /** The station and every platform under it, MOTIS-prefixed (`{feed}_{id}`). */
  members: Set<string>
}

/**
 * Resolve each stop to its station: the GTFS parent, and every platform filed
 * under it.
 *
 * Two problems need this. A board built from the platforms is doubled, because
 * MOTIS answers `640N` and `640S` with the same station-level list — that is
 * the "Now, Now" and the repeated clock times in a reported board. And MOTIS
 * widens a stoptimes query to every stop that *shares a name*, so asking about
 * the Chambers St J/Z platform returns the 1/2/3 and A/C running under a
 * different Chambers St 200 m away, which is a separate complex with no free
 * transfer between them. Asking once per station fixes the first; knowing the
 * station's real membership lets the caller throw out the second.
 *
 * A stop the GTFS tables have never heard of resolves to itself with itself as
 * its only member — an id from a caller is not ours to assume is real.
 */
async function resolveStations(
  stops: Array<{ feedId: string; stopId: string }>,
): Promise<Map<string, Station>> {
  const resolved = new Map<string, Station>()
  if (stops.length === 0) return resolved

  const unique = [...new Map(stops.map((s) => [stationKey(s.feedId, s.stopId), s])).values()]
  for (const { feedId, stopId } of unique) {
    resolved.set(stationKey(feedId, stopId), {
      stationId: stopId,
      members: new Set([`${feedId}_${stopId}`]),
    })
  }

  // One parametrized row per stop — the ids reach here off a query string, so
  // they stay bind parameters rather than being quoted into a VALUES list.
  const seed = sql.join(
    unique.map((s) => sql`(${s.feedId}::text, ${s.stopId}::text)`),
    sql`, `,
  )

  try {
    const rows = (await db.execute(sql`
      WITH seed (feed_id, stop_id) AS (VALUES ${seed}),
      station AS (
        SELECT
          seed.feed_id,
          seed.stop_id AS seed_id,
          COALESCE(NULLIF(s.parent_station, ''), seed.stop_id) AS station_id
        FROM seed
        LEFT JOIN gtfs_stops s ON s.feed_id = seed.feed_id AND s.stop_id = seed.stop_id
      )
      SELECT st.feed_id, st.seed_id, st.station_id, m.stop_id AS member_id
      FROM station st
      JOIN gtfs_stops m
        ON m.feed_id = st.feed_id
       AND (m.stop_id = st.station_id OR m.parent_station = st.station_id)
    `)) as any[]

    for (const row of rows) {
      const key = stationKey(row.feed_id, row.seed_id)
      const entry = resolved.get(key)
      if (!entry) continue
      entry.stationId = row.station_id
      entry.members.add(`${row.feed_id}_${row.member_id}`)
    }
  } catch (err) {
    // A board from the platforms is doubled, not wrong. Losing it entirely
    // because a lookup failed would be the worse outcome.
    console.error('[Departures] Failed to resolve station membership:', err)
  }

  return resolved
}

/** A separator no GTFS id contains, so ('a_b','c') and ('a','b_c') stay distinct. */
function stationKey(feedId: string, stopId: string): string {
  return `${feedId}\u0000${stopId}`
}

/**
 * Batch-fetch route colors from the database for a set of feed+route pairs.
 * Returns a map keyed by `{feedId}_{routeId}` → { color, textColor }.
 */
async function fetchRouteColors(
  pairs: Array<{ feedId: string; routeId: string }>,
): Promise<Map<string, { color?: string; textColor?: string }>> {
  const colorMap = new Map<string, { color?: string; textColor?: string }>()
  if (pairs.length === 0) return colorMap

  // Deduplicate
  const unique = [...new Map(pairs.map(p => [`${p.feedId}_${p.routeId}`, p])).values()]

  // Build WHERE clause for batch lookup. One parametrized fragment per pair,
  // OR-joined — the pairs come from MOTIS responses, so they are not ours to
  // trust, and a quoted string would be the same bug station.service.ts had.
  const conditions = sql.join(
    unique.map(p => sql`(feed_id = ${p.feedId} AND route_id = ${p.routeId})`),
    sql` OR `,
  )

  try {
    const result = await db.execute(sql`
      SELECT feed_id, route_id, route_color, route_text_color
      FROM gtfs_routes
      WHERE ${conditions}
    `)

    for (const row of result as any[]) {
      const key = `${row.feed_id}_${row.route_id}`
      colorMap.set(key, {
        color: row.route_color || undefined,
        textColor: row.route_text_color || undefined,
      })
    }
  } catch (err) {
    console.error('[Departures] Failed to fetch route colors:', err)
  }

  return colorMap
}

/**
 * Query the MOTIS stoptimes endpoint for a single stop.
 */
async function queryMotisStopTimes(
  stopId: string,
  n: number,
  time: string | undefined,
  fetchFn: FetchFn,
): Promise<MotisStopTimesResponse> {
  const motisUrl = getMotisUrl()
  const params = new URLSearchParams({ stopId, n: String(n) })
  if (time) params.set('time', time)

  const url = `${motisUrl}/api/v1/stoptimes?${params}`
  const response = await fetchFn(url, { signal: AbortSignal.timeout(10_000) })

  if (!response.ok) {
    const body = await response.text().catch(() => '')
    throw new Error(`MOTIS stoptimes returned ${response.status}: ${body}`)
  }

  return response.json() as Promise<MotisStopTimesResponse>
}

/**
 * Drop the runs MOTIS threw in that don't call at this station.
 *
 * MOTIS resolves a stoptimes query to every stop sharing the requested stop's
 * name, which is generous in a way that is usually invisible and occasionally
 * badly wrong: New York has two unrelated "Chambers St" complexes 200 m apart,
 * so a board for the J/Z platforms arrives carrying the 1, 2, 3, A and C from
 * the other one — lines a rider cannot reach without paying again. Each run
 * names the stop it actually calls at, so the ones that belong are the ones
 * whose stop is in this station.
 *
 * Applied to every board. Merging several nearby stops is still how a bare
 * coordinate is answered — but each board names the stop it is for, so it may
 * only carry that stop's own runs.
 */
function onlyAtStation(
  result: MotisStopTimesResponse,
  members: Set<string>,
): MotisStopTimesResponse {
  return {
    ...result,
    stopTimes: result.stopTimes.filter((st) => members.has(st.place.stopId)),
  }
}

/**
 * Transform MOTIS stopTimes into our Departure format, with route colors.
 */
function transformDepartures(
  stopTimes: MotisStopTime[],
  colorMap: Map<string, { color?: string; textColor?: string }>,
): Departure[] {
  return stopTimes
    .filter(st => !st.tripCancelled)
    .map(st => {
      const { feedId, stopId: routeId } = parseMotisId(st.routeId)
      const colors = colorMap.get(st.routeId)

      const departureTime = st.place.departure || st.place.arrival || ''
      const arrivalTime = st.place.arrival || st.place.departure || ''
      const scheduledDep = st.place.scheduledDeparture || st.place.scheduledArrival || ''
      const scheduledArr = st.place.scheduledArrival || st.place.scheduledDeparture || ''

      return {
        tripId: st.tripId,
        serviceDate: parseServiceDate(st.tripId),
        route: {
          id: routeId,
          feedId,
          shortName: st.routeShortName || undefined,
          longName: st.routeLongName || undefined,
          type: st.routeType,
          color: colors?.color,
          textColor: colors?.textColor,
          agencyId: st.agencyId || undefined,
          agencyName: st.agencyName || undefined,
        },
        headsign: st.headsign || undefined,
        directionId: st.directionId || undefined,
        departureTime,
        arrivalTime,
        scheduledDepartureTime: scheduledDep,
        scheduledArrivalTime: scheduledArr,
        delay: st.realTime ? computeDelay(departureTime, scheduledDep) : undefined,
        realTime: st.realTime,
        cancelled: st.cancelled || st.place.cancelled || false,
        mode: st.mode,
        tripOrigin: st.tripFrom?.name,
        tripDestination: st.tripTo?.name,
      }
    })
}

/**
 * Cut a stop's departures down to the requested window.
 *
 * Deliberately does NOT guarantee a non-empty result: a stop closed for the
 * night should come back empty here so the caller can tell "nothing for hours"
 * apart from "nothing at all", and decide for itself whether to reach past the
 * window. Losing that distinction is how a board ends up showing a single run
 * seven weeks out next to trains due in three minutes.
 */
function trimToWindow(
  departures: Departure[],
  windowEnd: Date | null,
  truncated: boolean,
): { departures: Departure[]; hasMore: boolean } {
  if (!windowEnd) return { departures, hasMore: truncated }

  const cutoff = windowEnd.getTime()
  const withinWindow = departures.filter((d) => {
    const at = Date.parse(d.departureTime || d.scheduledDepartureTime)
    return isNaN(at) || at <= cutoff
  })

  return {
    departures: withinWindow,
    hasMore: truncated || withinWindow.length < departures.length,
  }
}

// ── Public API ──────────────────────────────────────────────────────

/**
 * Get upcoming departures near a location or at a specific stop.
 *
 * When `feedId` + `stopId` are provided, queries that stop directly.
 * Otherwise, finds nearby stops via PostGIS and queries each.
 *
 * A query that identifies a station — by `feedId`/`stopId`, by `name`, or by a
 * stop sitting on the place — gets one board *for that station*: asked once at
 * the GTFS parent so its platforms aren't listed twice, and filtered to runs
 * that actually call there. A bare coordinate is still the old merge of
 * whatever is nearby, because "what can I catch from here" is a different
 * question from "what does this station run".
 *
 * Results are enriched with route colors from the GTFS database,
 * which MOTIS doesn't return in its stoptimes response.
 */
export async function getDepartures(
  request: DepartureRequest,
  fetchFn: FetchFn = globalThis.fetch,
): Promise<StopDepartures[]> {
  const {
    lat,
    lng,
    radius = 150,
    time,
    n = 50,
    feedId,
    stopId,
    routeShortNames,
    directionId,
    routeTypes,
    name,
    windowMinutes,
  } = request

  // The window is measured from the query time, not from wall-clock now — a
  // caller paging forward with `time` gets a window around what it asked for.
  const windowEnd = windowMinutes
    ? new Date((time ? new Date(time).getTime() : Date.now()) + windowMinutes * 60_000)
    : null

  const routeFilter =
    routeShortNames && routeShortNames.length
      ? new Set(routeShortNames)
      : null

  // 1. Determine which stops to query
  let stops: NearbyStop[]

  if (feedId && stopId) {
    // Direct stop query — skip spatial search
    stops = [{ feedId, stopId, name: '', lat, lng }]
  } else {
    stops = await findNearbyStops(
      lat,
      lng,
      radius,
      name ? IDENTITY_CANDIDATES : MERGED_STOPS,
      routeTypes,
    )
    if (stops.length === 0) return []
    // Only when the caller says this place is a transit stop — by naming it, or
    // by claiming a mode. A bare coordinate still gets everything nearby.
    if (routeTypes?.length || name) {
      stops = narrowToStation(rankByIdentity(stops, name), name).stops
    }
    stops = stops.slice(0, MERGED_STOPS)
  }

  // 2. Resolve each stop to its station, so a complex is asked about once
  //    rather than once per platform, and so a name-matched neighbour's runs
  //    can be told from this station's own.
  //
  //    Done for every query, not just an identified one. Each board says which
  //    stop it is for, and that has to be true even when the caller only gave a
  //    coordinate: MOTIS answers a stoptimes query with every same-named stop's
  //    runs, so an unfiltered board for the Chambers St J/Z platform carries the
  //    1, 2, 3, A and C of the Chambers St 200 m away and claims they depart
  //    from here. Merging several nearby stops is still fine — that is what a
  //    bare coordinate asks for — but each of them keeps its own departures.
  const stations = await resolveStations(stops)

  const boards = [
    ...new Map(
      stops.map((stop) => {
        const station = stations.get(stationKey(stop.feedId, stop.stopId))
        const stationId = station?.stationId ?? stop.stopId
        return [
          stationKey(stop.feedId, stationId),
          { stop: { ...stop, stopId: stationId }, members: station?.members ?? null },
        ]
      }),
    ).values(),
  ]

  // 3. Query MOTIS stoptimes for each board in parallel
  const motisResults = await Promise.allSettled(
    boards.map(async ({ stop, members }) => {
      const motisStopId = `${stop.feedId}_${stop.stopId}`
      const result = await queryMotisStopTimes(motisStopId, n, time, fetchFn)
      // `page` is what MOTIS returned before this station's filter, since a
      // full page is what says the timetable had more to give. Counting the
      // filtered runs would report "no more" for a station whose page was
      // mostly a same-named neighbour's.
      return {
        stop,
        page: result.stopTimes.length,
        result: members ? onlyAtStation(result, members) : result,
      }
    }),
  )

  // 4. Collect all route IDs for batch color lookup
  const routePairs: Array<{ feedId: string; routeId: string }> = []
  const successResults: Array<{ stop: NearbyStop; page: number; result: MotisStopTimesResponse }> = []

  for (const outcome of motisResults) {
    if (outcome.status !== 'fulfilled') continue
    const { stop, page, result } = outcome.value
    successResults.push({ stop, page, result })

    for (const st of result.stopTimes) {
      const { feedId: fid, stopId: rid } = parseMotisId(st.routeId)
      routePairs.push({ feedId: fid, routeId: rid })
    }
  }

  if (successResults.length === 0) return []

  // 5. Batch-fetch route colors
  const colorMap = await fetchRouteColors(routePairs)

  // 6. Transform and return
  return successResults.map(({ stop, page, result }) => {
    const motisPlace = result.place
    const timezone = motisPlace?.tz || result.stopTimes[0]?.place?.tz || 'UTC'

    return {
      stop: {
        stopId: stop.stopId,
        feedId: stop.feedId,
        name: motisPlace?.name || stop.name,
        code: motisPlace?.stopCode || stop.code,
        lat: motisPlace?.lat || stop.lat,
        lng: motisPlace?.lon || stop.lng,
        timezone,
        distance: stop.distance,
      },
      ...trimToWindow(
        transformDepartures(result.stopTimes, colorMap).filter(
          (d) =>
            (!routeFilter || routeFilter.has(d.route.shortName ?? '')) &&
            (directionId == null || d.directionId === directionId),
        ),
        windowEnd,
        // A full page means MOTIS had more to give, so more runs exist even
        // when none of what came back was trimmed.
        page >= n,
      ),
      nextPageCursor: result.nextPageCursor,
      previousPageCursor: result.previousPageCursor,
    }
  })
}
