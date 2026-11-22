import { sql } from 'drizzle-orm'
import { db } from '../db'
import {
  OFFLINE_PACKAGE_VERSION,
  type PackageBounds,
  type PackagePlace,
  type PlacesPackage,
  type StreetsPackage,
  type TimetablePackage,
} from '../types/offline-package.types'
import { bboxCells, inBounds } from '../lib/offline/bbox'
import { dedupePlaces, packagePlace, packageStop, SKIPPED_CATEGORY_PREFIXES, type PlaceRow, type StopRow } from '../lib/offline/places'
import { buildStreetGraph, type StreetWay } from '../lib/offline/street-graph'
import {
  addDays,
  createTimetableBuilder,
  parseGtfsTime,
  type CalendarDate,
  type CalendarRow,
  type FeedCall,
  type FeedRoute,
  type FeedStop,
  type FeedTimetable,
  type FeedTransfer,
  type FeedTrip,
} from '../lib/offline/timetable'
import { eachZipCsvRow, listZipEntries } from '../lib/offline/zip-stream'

/** About 2 km: each cell's query stays far inside the API's statement timeout. */
const CELL_DEGREES = 0.02
const gtfsDir = () => process.env.GTFS_DATA_DIR || '/gtfs-zips'

const rows = async <T>(query: ReturnType<typeof sql>): Promise<T[]> => Array.from((await db.execute(query)) as unknown as T[])
const envelope = ([w, s, e, n]: PackageBounds) => sql`ST_MakeEnvelope(${w}, ${s}, ${e}, ${n}, 4326)`

/** Run package builds one at a time, so a burst of downloads can't exhaust memory. */
let queue: Promise<unknown> = Promise.resolve()
function serialized<T>(build: () => Promise<T>): Promise<T> {
  const run = queue.then(build, build)
  queue = run.catch(() => undefined)
  return run
}

const CACHE_SIZE = 4
const cache = new Map<string, Promise<unknown>>()
function cached<T>(key: string, build: () => Promise<T>): Promise<T> {
  const hit = cache.get(key)
  if (hit) {
    cache.delete(key)
    cache.set(key, hit)
    return hit as Promise<T>
  }
  const result = serialized(build)
  cache.set(key, result)
  result.catch(() => cache.delete(key))
  while (cache.size > CACHE_SIZE) cache.delete(cache.keys().next().value!)
  return result
}

const boundsKey = (b: PackageBounds) => b.map(v => v.toFixed(3)).join(',')

// ── Places ────────────────────────────────────────────────────────────

const skippedCategories = sql.join(
  SKIPPED_CATEGORY_PREFIXES.map(prefix => sql`categories[1] LIKE ${`${prefix}%`}`),
  sql` OR `,
)

async function placesInCell(cell: PackageBounds): Promise<PackagePlace[]> {
  const env = envelope(cell)
  const places = await rows<PlaceRow>(sql`
    SELECT id, name, categories[1] AS category, ST_X(centroid) AS lng, ST_Y(centroid) AS lat,
      address, hours, phones[1] AS phone, websites[1] AS website
    FROM geo_places
    WHERE centroid && ${env} AND name IS NOT NULL AND geom_type <> 'line' AND osm_type <> 'X'
      AND cardinality(categories) > 0 AND NOT (${skippedCategories})
  `)
  const streets = await rows<PlaceRow>(sql`
    SELECT DISTINCT ON (name) id, name,
      'highway/' || (tags->>'highway') AS category,
      ST_X(ST_ClosestPoint(geom, ST_Centroid(geom))) AS lng, ST_Y(ST_ClosestPoint(geom, ST_Centroid(geom))) AS lat
    FROM geo_places
    WHERE geom && ${env} AND geom_type = 'line' AND tags ? 'highway' AND name IS NOT NULL
    ORDER BY name, ST_Length(geom) DESC
  `)
  return [...places, ...streets].filter(p => inBounds(cell, p.lng, p.lat)).map(packagePlace)
}

async function stopsInBounds(bounds: PackageBounds): Promise<PackagePlace[]> {
  const stops = await rows<StopRow>(sql`
    SELECT s.feed_id AS "feedId", s.stop_id AS "stopId", s.stop_name AS name, s.stop_lon AS lng, s.stop_lat AS lat,
      (SELECT json_agg(json_build_object(
          'shortName', COALESCE(NULLIF(r.route_short_name, ''), r.route_long_name),
          'color', NULLIF(r.route_color, ''), 'type', r.route_type))
        FROM gtfs_stop_routes sr
        JOIN gtfs_routes r ON r.feed_id = sr.feed_id AND r.route_id = sr.route_id
        WHERE sr.feed_id = s.feed_id AND (sr.stop_id = s.stop_id OR sr.stop_id IN (
          SELECT c.stop_id FROM gtfs_stops c WHERE c.feed_id = s.feed_id AND c.parent_station = s.stop_id))
      ) AS routes
    FROM gtfs_stops s
    WHERE s.geom && ${envelope(bounds)} AND s.stop_name IS NOT NULL
      AND (s.location_type = 1 OR (COALESCE(s.location_type, 0) = 0 AND COALESCE(s.parent_station, '') = ''))
  `)
  return stops.map(packageStop)
}

export function buildPlacesPackage(bounds: PackageBounds): Promise<PlacesPackage> {
  return cached(`places:${boundsKey(bounds)}`, async () => {
    const places: PackagePlace[] = []
    for (const cell of bboxCells(bounds, CELL_DEGREES)) places.push(...(await placesInCell(cell)))
    places.push(...(await stopsInBounds(bounds)))
    return {
      version: OFFLINE_PACKAGE_VERSION,
      bounds,
      generatedAt: new Date().toISOString(),
      places: dedupePlaces(places),
    }
  })
}

// ── Streets ───────────────────────────────────────────────────────────

export function buildStreetsPackage(bounds: PackageBounds): Promise<StreetsPackage> {
  return cached(`streets:${boundsKey(bounds)}`, async () => {
    const ways = new Map<string, StreetWay>()
    for (const cell of bboxCells(bounds, CELL_DEGREES)) {
      const found = await rows<{ id: string; tags: Record<string, string>; coords: [number, number][] }>(sql`
        SELECT id, tags, (ST_AsGeoJSON(geom, 7)::json)->'coordinates' AS coords
        FROM geo_places
        WHERE geom && ${envelope(cell)} AND geom_type = 'line' AND tags ? 'highway'
          AND GeometryType(geom) = 'LINESTRING'
      `)
      for (const way of found) if (!ways.has(way.id)) ways.set(way.id, way)
    }
    return buildStreetGraph([...ways.values()], { bounds })
  })
}

// ── Timetable ─────────────────────────────────────────────────────────

const flag = (v: string | undefined) => v?.trim() === '1'
/**
 * A copy that doesn't share the parsed chunk's memory. A split cell can keep its
 * whole 64 KB chunk alive, which over stop_times.txt holds the entire file.
 */
const own = (s: string) => Buffer.from(s, 'utf8').toString('utf8')
const cell = (cells: string[], i: number) => (i >= 0 ? cells[i]?.trim() ?? '' : '')

/** Today as `YYYYMMDD` in a time zone. */
export function localDate(timezone: string, now = new Date()): string {
  const parts = new Intl.DateTimeFormat('en-CA', { timeZone: timezone, year: 'numeric', month: '2-digit', day: '2-digit' })
    .formatToParts(now)
  const part = (type: string) => parts.find(p => p.type === type)!.value
  return `${part('year')}${part('month')}${part('day')}`
}

async function readFeed(feedId: string, bounds: PackageBounds): Promise<FeedTimetable | null> {
  const path = `${gtfsDir()}/${feedId}.zip`
  if (!(await Bun.file(path).exists())) return null
  const entries = await listZipEntries(path)
  const each = (name: string, onRow: Parameters<typeof eachZipCsvRow>[3]) => eachZipCsvRow(path, entries, name, onRow)

  let timezone = 'UTC'
  const agencies = new Map<string, string>()
  await each('agency.txt', (c, col) => {
    if (timezone === 'UTC' && cell(c, col('agency_timezone'))) timezone = cell(c, col('agency_timezone'))
    agencies.set(cell(c, col('agency_id')), cell(c, col('agency_name')))
  })

  const allStops = new Map<string, FeedStop & { id: string; inside: boolean }>()
  await each('stops.txt', (c, col) => {
    const lat = Number(cell(c, col('stop_lat')))
    const lng = Number(cell(c, col('stop_lon')))
    const id = own(cell(c, col('stop_id')))
    allStops.set(id, {
      id,
      name: own(cell(c, col('stop_name'))),
      lat,
      lng,
      parent: own(cell(c, col('parent_station'))) || undefined,
      inside: Number.isFinite(lat) && Number.isFinite(lng) && inBounds(bounds, lng, lat),
    })
  })
  const stops = new Map<string, FeedStop>()
  for (const [id, stop] of allStops) {
    if (!stop.inside) continue
    stops.set(id, { name: stop.name, lat: stop.lat, lng: stop.lng, parent: stop.parent })
    const parent = stop.parent && allStops.get(stop.parent)
    if (parent && parent.inside) stops.set(stop.parent!, { name: parent.name, lat: parent.lat, lng: parent.lng })
  }

  const calls = new Map<string, FeedCall[]>()
  const lastStop = new Map<string, { seq: number; stopId: string }>()
  await each('stop_times.txt', (c, col) => {
    const rawTrip = cell(c, col('trip_id'))
    const stop = allStops.get(cell(c, col('stop_id')))
    if (!stop) return
    const stopId = stop.id
    const seq = Number(cell(c, col('stop_sequence')))
    const last = lastStop.get(rawTrip)
    if (!last) lastStop.set(own(rawTrip), { seq, stopId })
    else if (seq > last.seq) { last.seq = seq; last.stopId = stopId }
    if (!stops.has(stopId)) return
    let list = calls.get(rawTrip)
    if (!list) calls.set(own(rawTrip), (list = []))
    list.push({
      seq,
      stopId,
      arrival: parseGtfsTime(cell(c, col('arrival_time'))),
      departure: parseGtfsTime(cell(c, col('departure_time'))),
    })
  })
  for (const [tripId, list] of calls) if (list.length < 2) calls.delete(tripId)

  const trips = new Map<string, FeedTrip>()
  await each('trips.txt', (c, col) => {
    const tripId = cell(c, col('trip_id'))
    if (!calls.has(tripId)) return
    const direction = cell(c, col('direction_id'))
    const last = lastStop.get(tripId)
    trips.set(tripId, {
      routeId: cell(c, col('route_id')),
      serviceId: cell(c, col('service_id')),
      headsign: cell(c, col('trip_headsign')) || (last && allStops.get(last.stopId)?.name) || '',
      ...(direction !== '' && { directionId: Number(direction) }),
    })
  })
  lastStop.clear()

  const usedRoutes = new Set([...trips.values()].map(t => t.routeId))
  const routes = new Map<string, FeedRoute>()
  await each('routes.txt', (c, col) => {
    const routeId = cell(c, col('route_id'))
    if (!usedRoutes.has(routeId)) return
    const agency = agencies.get(cell(c, col('agency_id'))) ?? (agencies.size === 1 ? [...agencies.values()][0] : '')
    const color = cell(c, col('route_color'))
    const textColor = cell(c, col('route_text_color'))
    routes.set(routeId, {
      shortName: cell(c, col('route_short_name')),
      longName: cell(c, col('route_long_name')),
      type: Number(cell(c, col('route_type'))) || 0,
      ...(color && { color }),
      ...(textColor && { textColor }),
      ...(agency && { agency }),
    })
  })

  const usedServices = new Set([...trips.values()].map(t => t.serviceId))
  const calendar: CalendarRow[] = []
  await each('calendar.txt', (c, col) => {
    const serviceId = cell(c, col('service_id'))
    if (!usedServices.has(serviceId)) return
    calendar.push({
      serviceId,
      weekdays: ['monday', 'tuesday', 'wednesday', 'thursday', 'friday', 'saturday', 'sunday'].map(d => flag(cell(c, col(d)))),
      start: cell(c, col('start_date')),
      end: cell(c, col('end_date')),
    })
  })
  const calendarDates: CalendarDate[] = []
  await each('calendar_dates.txt', (c, col) => {
    const serviceId = cell(c, col('service_id'))
    if (!usedServices.has(serviceId)) return
    calendarDates.push({ serviceId, date: cell(c, col('date')), type: Number(cell(c, col('exception_type'))) })
  })

  const transfers: FeedTransfer[] = []
  await each('transfers.txt', (c, col) => {
    const from = cell(c, col('from_stop_id'))
    const to = cell(c, col('to_stop_id'))
    const type = Number(cell(c, col('transfer_type')) || 0)
    if (type === 3 || type > 3 || !stops.has(from) || !stops.has(to)) return
    const time = cell(c, col('min_transfer_time'))
    transfers.push({ from, to, ...(time !== '' && { seconds: Number(time) }) })
  })

  return { feedId, timezone, stops, calls, trips, routes, calendar, calendarDates, transfers }
}

export const MAX_TIMETABLE_DAYS = 21

/**
 * Starts the day before today, so trips of last night's service day that run
 * past midnight are in the package.
 */
export async function buildTimetablePackage(bounds: PackageBounds, days: number): Promise<TimetablePackage> {
  const feedIds = (await rows<{ feedId: string }>(sql`
    SELECT DISTINCT feed_id AS "feedId" FROM gtfs_stops WHERE geom && ${envelope(bounds)} ORDER BY 1
  `)).map(r => r.feedId)
  const dateKey = new Date().toISOString().slice(0, 10)
  return cached(`timetable:${boundsKey(bounds)}:${days}:${dateKey}`, async () => {
    let builder: ReturnType<typeof createTimetableBuilder> | null = null
    for (const feedId of feedIds) {
      const feed = await readFeed(feedId, bounds)
      if (!feed?.calls.size) continue
      builder ??= createTimetableBuilder({ bounds, startDate: addDays(localDate(feed.timezone), -1), days: days + 1 })
      builder.addFeed(feed)
    }
    return (builder ?? createTimetableBuilder({ bounds, startDate: addDays(localDate('UTC'), -1), days: days + 1 })).finish()
  })
}
