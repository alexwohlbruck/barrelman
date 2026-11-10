/**
 * The slice of a GTFS feed that realtime matching needs: each trip's stops and
 * times, which days it runs, and the feed's timezone.
 */
import { headerIndex, parseCsvRows, splitCsvLines } from '../csv'

export interface StaticTrip {
  tripId: string
  routeId: string
  /** -1 when the feed leaves direction_id out. */
  directionId: number
  serviceId: string
  stopIds: string[]
  /** Seconds after the service day's noon-minus-12h, as GTFS writes them. */
  times: Int32Array
}

export interface StaticIndex {
  timezone: string
  trips: StaticTrip[]
  byTripId: Map<string, StaticTrip[]>
  byRoute: Map<string, StaticTrip[]>
  routeIds: Set<string>
  stopIds: Set<string>
  /** service_id → active dates as YYYYMMDD. */
  serviceDates: Map<string, Set<string>>
}

export interface GtfsFiles {
  agency?: string
  routes?: string
  trips: string
  stopTimes: string
  stops?: string
  calendar?: string
  calendarDates?: string
}

const DAYS = ['sunday', 'monday', 'tuesday', 'wednesday', 'thursday', 'friday', 'saturday']

export function parseGtfsTime(value: string): number | null {
  const m = /^\s*(\d+):(\d{2}):(\d{2})\s*$/.exec(value)
  return m ? Number(m[1]) * 3600 + Number(m[2]) * 60 + Number(m[3]) : null
}

export function formatGtfsTime(seconds: number): string {
  const pad = (n: number) => String(n).padStart(2, '0')
  return `${pad(Math.floor(seconds / 3600))}:${pad(Math.floor(seconds / 60) % 60)}:${pad(seconds % 60)}`
}

const ymd = (d: Date) =>
  `${d.getUTCFullYear()}${String(d.getUTCMonth() + 1).padStart(2, '0')}${String(d.getUTCDate()).padStart(2, '0')}`

const parseYmd = (s: string) => new Date(Date.UTC(+s.slice(0, 4), +s.slice(4, 6) - 1, +s.slice(6, 8)))

function rows(text: string | undefined): { col: Map<string, number>; body: string[][] } {
  if (!text) return { col: new Map(), body: [] }
  const all = parseCsvRows(text)
  return { col: headerIndex(all[0] ?? []), body: all.slice(1).filter(r => r.length > 1 || r[0]) }
}

export function expandServiceDates(calendar?: string, calendarDates?: string): Map<string, Set<string>> {
  const dates = new Map<string, Set<string>>()
  const set = (id: string) => dates.get(id) ?? dates.set(id, new Set()).get(id)!

  const cal = rows(calendar)
  for (const r of cal.body) {
    const id = r[cal.col.get('service_id')!]
    const start = r[cal.col.get('start_date')!]
    const end = r[cal.col.get('end_date')!]
    if (!id || !start || !end) continue
    const days = DAYS.map(d => r[cal.col.get(d)!]?.trim() === '1')
    const out = set(id)
    for (let d = parseYmd(start); ymd(d) <= end; d.setUTCDate(d.getUTCDate() + 1)) {
      if (days[d.getUTCDay()]) out.add(ymd(d))
    }
  }

  const cd = rows(calendarDates)
  for (const r of cd.body) {
    const id = r[cd.col.get('service_id')!]
    const date = r[cd.col.get('date')!]?.trim()
    const type = r[cd.col.get('exception_type')!]?.trim()
    if (!id || !date) continue
    if (type === '1') set(id).add(date)
    else if (type === '2') dates.get(id)?.delete(date)
  }
  return dates
}

export function buildStaticIndex(files: GtfsFiles): StaticIndex {
  const agency = rows(files.agency)
  const timezone = agency.body[0]?.[agency.col.get('agency_timezone') ?? -1]?.trim() || 'UTC'

  const tripRows = rows(files.trips)
  const tc = tripRows.col
  const trips: StaticTrip[] = []
  const byTripId = new Map<string, StaticTrip[]>()
  const byKey = new Map<string, StaticTrip>()
  for (const r of tripRows.body) {
    const tripId = r[tc.get('trip_id')!]?.trim()
    if (!tripId) continue
    const dir = r[tc.get('direction_id') ?? -1]?.trim()
    const trip: StaticTrip = {
      tripId,
      routeId: r[tc.get('route_id')!]?.trim() ?? '',
      directionId: dir === '0' || dir === '1' ? Number(dir) : -1,
      serviceId: r[tc.get('service_id')!]?.trim() ?? '',
      stopIds: [],
      times: new Int32Array(0),
    }
    trips.push(trip)
    ;(byTripId.get(tripId) ?? byTripId.set(tripId, []).get(tripId)!).push(trip)
    byKey.set(tripId, trip)
  }

  // stop_times.txt runs to millions of rows; walk it line by line rather than
  // holding it as rows.
  const stopRows = new Map<StaticTrip, { seq: number; stop: string; t: number }[]>()
  let col: Map<string, number> | null = null
  for (const line of splitCsvLines(files.stopTimes)) {
    if (!col) { col = headerIndex(line); continue }
    const trip = byKey.get(line[col.get('trip_id')!]?.trim() ?? '')
    if (!trip) continue
    const t = parseGtfsTime(line[col.get('departure_time')!] ?? '') ?? parseGtfsTime(line[col.get('arrival_time')!] ?? '')
    if (t === null) continue
    const list = stopRows.get(trip) ?? stopRows.set(trip, []).get(trip)!
    list.push({ seq: Number(line[col.get('stop_sequence')!]), stop: line[col.get('stop_id')!]!.trim(), t })
  }
  for (const [trip, list] of stopRows) {
    list.sort((a, b) => a.seq - b.seq)
    trip.stopIds = list.map(s => s.stop)
    trip.times = Int32Array.from(list.map(s => s.t))
  }

  const timed = trips.filter(t => t.stopIds.length > 0)
  const byRoute = new Map<string, StaticTrip[]>()
  for (const t of timed) (byRoute.get(t.routeId) ?? byRoute.set(t.routeId, []).get(t.routeId)!).push(t)

  const stops = rows(files.stops)
  const stopIds = new Set(stops.body.map(r => r[stops.col.get('stop_id')!]?.trim() ?? '').filter(Boolean))
  const routes = rows(files.routes)
  const routeIds = new Set(routes.body.map(r => r[routes.col.get('route_id')!]?.trim() ?? '').filter(Boolean))

  return {
    timezone,
    trips: timed,
    byTripId,
    byRoute,
    routeIds,
    stopIds,
    serviceDates: expandServiceDates(files.calendar, files.calendarDates),
  }
}

/** Offset of `timezone` from UTC at `epochSeconds`, in seconds. */
function tzOffset(timezone: string, epochSeconds: number): number {
  const parts = new Intl.DateTimeFormat('en-US', {
    timeZone: timezone, hourCycle: 'h23',
    year: 'numeric', month: '2-digit', day: '2-digit', hour: '2-digit', minute: '2-digit', second: '2-digit',
  }).formatToParts(new Date(epochSeconds * 1000))
  const get = (t: string) => Number(parts.find(p => p.type === t)!.value)
  const local = Date.UTC(get('year'), get('month') - 1, get('day'), get('hour'), get('minute'), get('second')) / 1000
  return local - epochSeconds
}

/**
 * Epoch seconds of a service day's origin: noon minus twelve hours, local
 * time, which is what GTFS times count from. It differs from midnight on the
 * days clocks change.
 */
export function serviceDayOrigin(date: string, timezone: string): number {
  const noonUtc = Date.UTC(+date.slice(0, 4), +date.slice(4, 6) - 1, +date.slice(6, 8), 12) / 1000
  return noonUtc - tzOffset(timezone, noonUtc) - 12 * 3600
}

/** The local calendar date at `epochSeconds`, as YYYYMMDD. */
export function localDate(epochSeconds: number, timezone: string): string {
  return ymd(new Date((epochSeconds + tzOffset(timezone, epochSeconds)) * 1000))
}

export function shiftDate(date: string, days: number): string {
  const d = parseYmd(date)
  d.setUTCDate(d.getUTCDate() + days)
  return ymd(d)
}
