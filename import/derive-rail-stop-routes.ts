/**
 * STREAMING stop->route derivation for feeds whose stop_times is too large to
 * parse in memory (e.g. CTA: 362MB, buses + trains — OOMs the in-memory
 * backfill). Streams stop_times.txt line-by-line from disk, filters to the
 * given route_types (default 1 = rail/metro), and upserts gtfs_stop_routes with
 * weekday-daytime trip counts (same "regular service" semantics as
 * deriveStopRoutes). Only the small files (routes/trips/calendar) are loaded
 * whole; stop_times never is.
 *
 * Usage (feed already imported; extract the txt files to a mounted dir first):
 *   unzip -o data/gtfs/29.zip routes.txt trips.txt calendar.txt \
 *     calendar_dates.txt stop_times.txt -d data/29-gtfs
 *   docker exec barrelman bun run import/derive-rail-stop-routes.ts 29 /data/29-gtfs 1
 */
import { createReadStream, readFileSync, existsSync } from 'fs'
import { createInterface } from 'readline'
import { join } from 'path'
import { parseGtfsRecords, importStopRoutes } from '../src/services/gtfs.service'

const feedId = process.argv[2]
const dir = process.argv[3]
const routeTypes = new Set((process.argv[4] || '1').split(',').map(s => s.trim()))

if (!feedId || !dir) {
  console.error('Usage: derive-rail-stop-routes.ts <feedId> <extractedDir> [routeTypes=1]')
  process.exit(1)
}

const read = (f: string) => (existsSync(join(dir, f)) ? readFileSync(join(dir, f), 'utf8') : '')

// Weekday service ids (calendar Mon-Fri, else calendar_dates weekday additions).
function weekdayServiceIds(): Set<string> | null {
  const cal = parseGtfsRecords(read('calendar.txt'))
  const wd = new Set<string>()
  const DAYS = ['monday', 'tuesday', 'wednesday', 'thursday', 'friday']
  for (const r of cal) if (r.service_id && DAYS.some(d => r[d] === '1')) wd.add(r.service_id)
  if (wd.size) return wd
  for (const r of parseGtfsRecords(read('calendar_dates.txt'))) {
    if (!r.service_id || r.exception_type !== '1' || !r.date) continue
    const dow = new Date(+r.date.slice(0, 4), +r.date.slice(4, 6) - 1, +r.date.slice(6, 8)).getDay()
    if (dow >= 1 && dow <= 5) wd.add(r.service_id)
  }
  return wd.size ? wd : null
}
function toSec(t: string): number {
  const p = t?.split(':')
  if (!p || p.length < 3) return NaN
  return +p[0] * 3600 + +p[1] * 60 + +p[2]
}
const DAY_LO = 6 * 3600, DAY_HI = 22 * 3600

async function main() {
  // Small files: rail route ids, trip -> route/service.
  const railRoutes = new Set(
    parseGtfsRecords(read('routes.txt'))
      .filter(r => routeTypes.has(r.route_type))
      .map(r => r.route_id),
  )
  const tripRoute = new Map<string, string>()
  const tripSvc = new Map<string, string>()
  for (const t of parseGtfsRecords(read('trips.txt'))) {
    if (!railRoutes.has(t.route_id)) continue
    tripRoute.set(t.trip_id, t.route_id)
    if (t.service_id) tripSvc.set(t.trip_id, t.service_id)
  }
  const weekday = weekdayServiceIds()
  console.log(`${railRoutes.size} rail routes, ${tripRoute.size} rail trips; streaming stop_times...`)

  // Stream stop_times.txt; count weekday-daytime trips per (stop, route).
  const counts = new Map<string, number>()
  const rl = createInterface({ input: createReadStream(join(dir, 'stop_times.txt')), crlfDelay: Infinity })
  let header: Record<string, number> | null = null
  let iTrip = 0, iStop = 0, iDep = 0, iArr = 0, n = 0
  for await (const line of rl) {
    if (!header) {
      const cols = line.replace(/^﻿/, '').split(',')
      header = {}
      cols.forEach((c, i) => (header![c.trim()] = i))
      iTrip = header['trip_id']; iStop = header['stop_id']
      iDep = header['departure_time']; iArr = header['arrival_time']
      continue
    }
    const f = line.split(',')
    const routeId = tripRoute.get(f[iTrip])
    if (!routeId) continue
    const stopId = f[iStop]
    if (!stopId) continue
    const key = `${stopId}|${routeId}`
    if (!counts.has(key)) counts.set(key, 0)
    if (weekday) {
      const svc = tripSvc.get(f[iTrip])
      if (!svc || !weekday.has(svc)) continue
    }
    const sec = toSec(f[iDep] || f[iArr])
    if (Number.isNaN(sec) || sec < DAY_LO || sec >= DAY_HI) continue
    counts.set(key, counts.get(key)! + 1)
    if (++n % 2_000_000 === 0) console.log(`  …${n} rail stop_times`)
  }

  const associations = [...counts.entries()].map(([k, v]) => {
    const i = k.lastIndexOf('|')
    return { feedId, stopId: k.slice(0, i), routeId: k.slice(i + 1), weekdayTrips: v }
  })
  const imported = await importStopRoutes(associations)
  const withSvc = associations.filter(a => a.weekdayTrips >= 2).length
  console.log(`Done: ${imported} rail (stop,route) pairs upserted (${withSvc} with >=2 weekday-daytime trips).`)
  process.exit(0)
}
main()
