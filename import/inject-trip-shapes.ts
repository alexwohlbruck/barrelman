/**
 * Give shapeless GTFS trips the shape of a sibling trip that runs over them.
 *
 * MOTIS draws a leg from the trip's shape. A trip with no `shape_id`, or one
 * naming a shape that shapes.txt lacks, gets only its stop coordinates, so the
 * leg renders as straight stop-to-stop segments. Feeds leave this gap on
 * short-turn and rush-hour variants: the MTA subway feed has 6,393 of 78,885
 * trips without a shape, while every stop they call at lies on a shaped trip
 * of the same route.
 *
 * A donor is a shaped trip on the same route whose stop sequence contains the
 * shapeless trip's stops as an ordered subsequence. Same direction is preferred,
 * then the donor with the fewest extra stops, then the shape the most trips of
 * the route use. A local whose only shaped siblings run express has stops no
 * donor calls at, so failing that, any shape of the route passing within
 * SHAPE_TOLERANCE_METERS of every stop in order is used, shortest first. MOTIS
 * cuts the shape at the recipient's own stops. A trip with neither keeps its
 * empty shape.
 *
 * Usage: bun run import/inject-trip-shapes.ts [--dir ./data/gtfs] [--dry-run]
 */
import { readdirSync, renameSync, writeFileSync } from 'fs'
import { join, basename } from 'path'
import JSZip from 'jszip'
import { parseArgs } from 'util'

import { parseCsvRows, serializeCsvRows, headerIndex } from '../src/lib/csv'
import { groupStopTimes, parseShapes, parseStops, type StopTimesByTrip } from '../src/services/gtfs.service'
import { routePath, type LonLat, type RoutePath } from '../src/lib/route-hazards/route-path'

/** How far a stop may sit from a borrowed shape: a platform or curb off the track or road centreline. */
export const SHAPE_TOLERANCE_METERS = 100

export interface DonorTrip {
  directionId: string
  stops: string[]
  shapeId: string
}

/** Whether `needle` appears in `haystack` in order, gaps allowed. */
export function isOrderedSubsequence(needle: string[], haystack: string[]): boolean {
  if (needle.length > haystack.length) return false
  let i = 0
  for (let j = 0; j < haystack.length && i < needle.length; j++) {
    if (haystack[j] === needle[i]) i++
  }
  return i === needle.length
}

/**
 * Pick the shape for a shapeless trip from its route's shaped trips, or null.
 *
 * `shapeTrips` counts how many of the route's trips use each shape, for the
 * final tie-break. Donors are expected deduplicated by (stops, shape).
 */
export function chooseDonorShape(
  trip: { directionId: string; stops: string[] },
  donors: DonorTrip[],
  shapeTrips: Map<string, number>,
): string | null {
  if (trip.stops.length < 2) return null
  const fits = donors.filter(d => isOrderedSubsequence(trip.stops, d.stops))
  const sameDirection = fits.filter(d => d.directionId === trip.directionId)
  const pool = sameDirection.length ? sameDirection : fits

  let best: DonorTrip | null = null
  const rank = (d: DonorTrip) => [d.stops.length, -(shapeTrips.get(d.shapeId) ?? 0)] as const
  for (const d of pool) {
    if (!best) { best = d; continue }
    const [a1, a2] = rank(d)
    const [b1, b2] = rank(best)
    if (a1 < b1 || (a1 === b1 && (a2 < b2 || (a2 === b2 && d.shapeId < best.shapeId)))) best = d
  }
  return best?.shapeId ?? null
}

/** Whether a shape passes every stop, in order, within `tolerance` metres. */
export function shapeFollowsStops(path: RoutePath, stops: LonLat[], tolerance = SHAPE_TOLERANCE_METERS): boolean {
  let along = 0
  for (const stop of stops) {
    const pass = path.project(stop, tolerance).find(p => p.distance >= along)
    if (!pass) return false
    along = pass.distance
  }
  return true
}

export interface RouteShape {
  shapeId: string
  path: RoutePath
  directions: Set<string>
  trips: number
}

/** The shortest route shape that follows the stops, same direction first, or null. */
export function chooseGeometricShape(
  trip: { directionId: string; coords: LonLat[] },
  shapes: RouteShape[],
): string | null {
  if (trip.coords.length < 2) return null
  const ranked = [...shapes].sort(
    (a, b) =>
      Number(b.directions.has(trip.directionId)) - Number(a.directions.has(trip.directionId)) ||
      a.path.length - b.path.length ||
      b.trips - a.trips ||
      (a.shapeId < b.shapeId ? -1 : 1),
  )
  return ranked.find(s => shapeFollowsStops(s.path, trip.coords))?.shapeId ?? null
}

/** The tables a fill reads beyond trips.txt, loaded only when a trip needs them. */
export interface FeedTables {
  stopTimes(): StopTimesByTrip | Promise<StopTimesByTrip>
  /** stop_id → [lon, lat] and shape_id → points; omitted, only stop sequences are compared. */
  geometry?(): Promise<{ stops: Map<string, LonLat>; shapes: Map<string, LonLat[]> }>
}

export interface TripShapeResult {
  skipped?: 'no-trips' | 'no-shape-column' | 'no-shapes'
  /** Rewritten trips.txt, absent when nothing changed. */
  tripsTxt?: string
  /** Trips that had no usable shape before the fill. */
  shapeless: number
  filled: number
  /** Shapeless trips left as they were, for want of a donor. */
  missing: number
}

/** Stop ids of each trip, ordered by stop_sequence. */
function orderedStops(stopTimes: StopTimesByTrip, tripId: string): string[] {
  const rows = stopTimes.get(tripId)
  if (!rows) return []
  return [...rows].sort((a, b) => a.seq - b.seq).map(r => r.stopId)
}

/**
 * Fill empty or dangling `shape_id`s in trips.txt.
 *
 * `stopTimes` is only read when some trip needs a shape, since it is most of a
 * feed and nearly every feed is complete.
 */
export async function backfillTripShapes(
  tripsTxt: string | null,
  shapeIds: Set<string>,
  tables: FeedTables,
): Promise<TripShapeResult> {
  const empty = { shapeless: 0, filled: 0, missing: 0 }
  if (!tripsTxt) return { skipped: 'no-trips', ...empty }
  const rows = parseCsvRows(tripsTxt)
  if (rows.length < 2) return { skipped: 'no-trips', ...empty }

  const index = headerIndex(rows[0]!)
  const iTrip = index.get('trip_id')
  const iRoute = index.get('route_id')
  const iDir = index.get('direction_id')
  const iShape = index.get('shape_id')
  if (iTrip === undefined || iRoute === undefined) return { skipped: 'no-trips', ...empty }

  const cell = (row: string[], i: number | undefined) => (i === undefined ? '' : (row[i] ?? '').trim())
  const data = rows.slice(1).filter(r => !(r.length === 1 && r[0] === '') && r.length > 0)
  const shapeless = data.filter(r => !shapeIds.has(cell(r, iShape)))
  if (!shapeless.length) return empty
  if (!shapeIds.size) return { skipped: 'no-shapes', ...empty, shapeless: shapeless.length, missing: shapeless.length }
  if (iShape === undefined) {
    return { skipped: 'no-shape-column', ...empty, shapeless: shapeless.length, missing: shapeless.length }
  }

  const needy = new Set(shapeless.map(r => cell(r, iRoute)))
  const byTrip = await tables.stopTimes()

  const donorsByRoute = new Map<string, Map<string, DonorTrip>>()
  const shapeTripsByRoute = new Map<string, Map<string, number>>()
  for (const r of data) {
    const route = cell(r, iRoute)
    const shapeId = cell(r, iShape)
    if (!needy.has(route) || !shapeIds.has(shapeId)) continue
    const counts = shapeTripsByRoute.get(route) ?? new Map<string, number>()
    shapeTripsByRoute.set(route, counts)
    counts.set(shapeId, (counts.get(shapeId) ?? 0) + 1)

    const stops = orderedStops(byTrip, cell(r, iTrip))
    if (stops.length < 2) continue
    const directionId = cell(r, iDir)
    const key = `${directionId}\u0000${shapeId}\u0000${stops.join('\u0001')}`
    const donors = donorsByRoute.get(route) ?? new Map<string, DonorTrip>()
    donorsByRoute.set(route, donors)
    if (!donors.has(key)) donors.set(key, { directionId, stops, shapeId })
  }

  const chosen = new Map<string, string | null>()
  const unresolved: Array<{ row: string[]; route: string; key: string; directionId: string; stops: string[] }> = []
  let filled = 0
  const assign = (row: string[], shapeId: string) => {
    while (row.length <= iShape) row.push('')
    row[iShape] = shapeId
    filled++
  }
  for (const r of shapeless) {
    const route = cell(r, iRoute)
    const trip = { directionId: cell(r, iDir), stops: orderedStops(byTrip, cell(r, iTrip)) }
    const key = `${route}\u0000${trip.directionId}\u0000${trip.stops.join('\u0001')}`
    let shapeId = chosen.get(key)
    if (shapeId === undefined) {
      const donors = [...(donorsByRoute.get(route)?.values() ?? [])]
      shapeId = chooseDonorShape(trip, donors, shapeTripsByRoute.get(route) ?? new Map())
      chosen.set(key, shapeId)
    }
    if (shapeId) assign(r, shapeId)
    else if (trip.stops.length >= 2 && shapeTripsByRoute.has(route)) unresolved.push({ row: r, route, key, ...trip })
  }

  if (unresolved.length && tables.geometry) {
    const { stops, shapes } = await tables.geometry()
    const routeShapes = new Map<string, RouteShape[]>()
    const shapesOf = (route: string): RouteShape[] => {
      let list = routeShapes.get(route)
      if (list) return list
      const directions = new Map<string, Set<string>>()
      for (const d of donorsByRoute.get(route)?.values() ?? []) {
        directions.set(d.shapeId, (directions.get(d.shapeId) ?? new Set()).add(d.directionId))
      }
      list = []
      for (const [shapeId, trips] of shapeTripsByRoute.get(route) ?? []) {
        const points = shapes.get(shapeId)
        if (points && points.length >= 2) {
          list.push({ shapeId, trips, path: routePath(points), directions: directions.get(shapeId) ?? new Set() })
        }
      }
      routeShapes.set(route, list)
      return list
    }
    const geometric = new Map<string, string | null>()
    for (const u of unresolved) {
      let shapeId = geometric.get(u.key)
      if (shapeId === undefined) {
        const coords = u.stops.map(id => stops.get(id))
        shapeId = coords.every(Boolean)
          ? chooseGeometricShape({ directionId: u.directionId, coords: coords as LonLat[] }, shapesOf(u.route))
          : null
        geometric.set(u.key, shapeId)
      }
      if (shapeId) assign(u.row, shapeId)
    }
  }

  const result = { shapeless: shapeless.length, filled, missing: shapeless.length - filled }
  return filled ? { ...result, tripsTxt: serializeCsvRows(rows) } : result
}

/** The shape_id column of shapes.txt, without parsing the points. */
export function listShapeIds(shapesTxt: string | null): Set<string> {
  const ids = new Set<string>()
  if (!shapesTxt) return ids
  const firstBreak = shapesTxt.indexOf('\n')
  const header = parseCsvRows(shapesTxt.slice(0, firstBreak === -1 ? undefined : firstBreak))[0] ?? []
  const col = headerIndex(header).get('shape_id')
  if (col === undefined || firstBreak === -1) return ids
  let last = ''
  for (let pos = firstBreak + 1; pos < shapesTxt.length;) {
    let end = shapesTxt.indexOf('\n', pos)
    if (end === -1) end = shapesTxt.length
    const line = shapesTxt.slice(pos, end)
    pos = end + 1
    const cells = line.includes('"') ? parseCsvRows(line)[0] ?? [] : line.split(',')
    const id = (cells[col] ?? '').trim()
    if (id && id !== last) ids.add((last = id))
  }
  return ids
}

async function readEntry(zip: JSZip, name: string): Promise<{ name: string; text: string } | null> {
  const entry = zip.file(name) ?? zip.file(new RegExp(`(^|/)${name}$`))[0]
  return entry ? { name: entry.name, text: await entry.async('string') } : null
}

/** Rewrite one feed zip's trips.txt in place. */
export async function injectTripShapes(zipPath: string, options: { dryRun?: boolean } = {}): Promise<TripShapeResult> {
  const zip = await JSZip.loadAsync(await Bun.file(zipPath).arrayBuffer())
  const trips = await readEntry(zip, 'trips.txt')
  const shapes = await readEntry(zip, 'shapes.txt')
  const result = await backfillTripShapes(trips?.text ?? null, listShapeIds(shapes?.text ?? null), {
    stopTimes: async () => groupStopTimes((await readEntry(zip, 'stop_times.txt'))?.text ?? ''),
    geometry: async () => {
      const stopsTxt = (await readEntry(zip, 'stops.txt'))?.text
      const stops = new Map<string, LonLat>(
        stopsTxt ? parseStops(stopsTxt, '').map(s => [s.stopId, [s.stopLon, s.stopLat]]) : [],
      )
      return { stops, shapes: shapes ? parseShapes(shapes.text) : new Map() }
    },
  })

  if (result.tripsTxt && !options.dryRun) {
    zip.file(trips!.name, result.tripsTxt)
    const tmp = `${zipPath}.tmp-${process.pid}`
    writeFileSync(tmp, await zip.generateAsync({ type: 'nodebuffer', compression: 'DEFLATE' }))
    renameSync(tmp, zipPath)
  }
  return result
}

/** Fill every zip, logging per-feed counts for the feeds that had shapeless trips. */
export async function injectTripShapesAll(zipPaths: string[], options: { dryRun?: boolean } = {}) {
  const total = { filled: 0, missing: 0, feedsWithoutShapes: 0 }
  for (const zipPath of zipPaths) {
    try {
      const r = await injectTripShapes(zipPath, options)
      if (r.skipped === 'no-shapes') total.feedsWithoutShapes++
      if (!r.shapeless || r.skipped) continue
      total.filled += r.filled
      total.missing += r.missing
      console.log(`  ✓ ${basename(zipPath)}: filled ${r.filled} of ${r.shapeless} shapeless trips, ${r.missing} still missing`)
    } catch (err) {
      console.error(`  ✗ ${basename(zipPath)}: ${err}`)
    }
  }
  console.log(
    `  trip shapes: ${total.filled} filled, ${total.missing} still missing; ` +
      `${total.feedsWithoutShapes} feed(s) publish no shapes at all`,
  )
  return total
}

if (import.meta.main) {
  const { values } = parseArgs({
    args: Bun.argv.slice(2),
    options: {
      dir: { type: 'string', default: './data/gtfs' },
      'dry-run': { type: 'boolean', default: false },
    },
  })
  const dir = values.dir!
  const zips = readdirSync(dir).filter(f => f.endsWith('.zip')).sort().map(f => join(dir, f))
  console.log(`Scanning ${zips.length} feeds in ${dir}${values['dry-run'] ? ' (dry run)' : ''}`)
  await injectTripShapesAll(zips, { dryRun: values['dry-run'] })
}
