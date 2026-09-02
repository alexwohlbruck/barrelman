import { describe, expect, test } from 'bun:test'
import JSZip from 'jszip'
import {
  normalizeSuffixTripIds,
  expandServiceDates,
  SUFFIX_TRIP_ID_FEEDS,
} from './normalize-trip-ids'

const SUBWAY = { feedId: '5', onestopId: 'f-dr5r-nyctsubway' }

/** A feed shaped like MTA's: prefixed trip ids, weekday/weekend services. */
function buildZip(files: Record<string, string>): Promise<ArrayBuffer> {
  const zip = new JSZip()
  for (const [name, content] of Object.entries(files)) zip.file(name, content)
  return zip.generateAsync({ type: 'arraybuffer' })
}

async function read(buffer: ArrayBuffer, name: string): Promise<string> {
  const zip = await JSZip.loadAsync(buffer)
  return await zip.file(name)!.async('string')
}

const CALENDAR =
  'service_id,monday,tuesday,wednesday,thursday,friday,saturday,sunday,start_date,end_date\n' +
  'Weekday,1,1,1,1,1,0,0,20260601,20260630\n' +
  'Saturday,0,0,0,0,0,1,0,20260601,20260630\n' +
  'Sunday,0,0,0,0,0,0,1,20260601,20260630\n'

const TRIPS =
  'route_id,trip_id,service_id,trip_headsign,direction_id\n' +
  'A,ASP26GEN-1038-Sunday-00_000600_A..S03R,Sunday,Far Rockaway,1\n' +
  'A,BSP26GEN-D085-Weekday-00_000600_A..S03R,Weekday,Far Rockaway,1\n' +
  'A,BSP26GEN-D085-Weekday-00_125400_A..N05R,Weekday,Inwood,0\n'

const STOP_TIMES =
  'trip_id,stop_id,arrival_time,departure_time,stop_sequence\n' +
  'ASP26GEN-1038-Sunday-00_000600_A..S03R,A01S,00:06:00,00:06:00,1\n' +
  'BSP26GEN-D085-Weekday-00_000600_A..S03R,A01S,00:06:00,00:06:00,1\n' +
  'BSP26GEN-D085-Weekday-00_125400_A..N05R,A02N,12:54:00,12:54:00,1\n'

describe('normalizeSuffixTripIds', () => {
  test('strips the schedule-version prefix from every trip_id-bearing file', async () => {
    const zip = await buildZip({ 'trips.txt': TRIPS, 'stop_times.txt': STOP_TIMES, 'calendar.txt': CALENDAR })
    const { buffer, result } = await normalizeSuffixTripIds(zip, SUBWAY)

    expect(result.applied).toBe(true)
    expect(result.rewrittenRows).toEqual({ 'trips.txt': 3, 'stop_times.txt': 3 })

    const trips = await read(buffer, 'trips.txt')
    expect(trips).toContain('000600_A..S03R')
    expect(trips).not.toContain('ASP26GEN')
    expect(await read(buffer, 'stop_times.txt')).not.toContain('BSP26GEN')
  })

  test('leaves other columns and the header untouched', async () => {
    const zip = await buildZip({ 'trips.txt': TRIPS, 'calendar.txt': CALENDAR })
    const { buffer } = await normalizeSuffixTripIds(zip, SUBWAY)
    const rows = (await read(buffer, 'trips.txt')).trim().split('\n')

    expect(rows[0]).toBe('route_id,trip_id,service_id,trip_headsign,direction_id')
    expect(rows[1]).toBe('A,000600_A..S03R,Sunday,Far Rockaway,1')
  })

  test('re-quotes fields that contain a comma', async () => {
    const trips =
      'route_id,trip_id,service_id,trip_headsign\n' +
      'A,ASP26GEN-1038-Sunday-00_000600_A..S03R,Sunday,"Rockaway, Beach 116"\n'
    const zip = await buildZip({ 'trips.txt': trips, 'calendar.txt': CALENDAR })
    const { buffer, result } = await normalizeSuffixTripIds(zip, SUBWAY)

    expect(result.applied).toBe(true)
    expect(await read(buffer, 'trips.txt')).toContain('"Rockaway, Beach 116"')
  })

  test('skips feeds that are not in scope', async () => {
    const zip = await buildZip({ 'trips.txt': TRIPS, 'calendar.txt': CALENDAR })
    const { buffer, result } = await normalizeSuffixTripIds(zip, {
      feedId: '7',
      onestopId: 'f-dr5r-mtabuscompany',
    })

    expect(result.applied).toBe(false)
    expect(result.skipReason).toBe('not-in-scope')
    expect(buffer).toBe(zip)
  })

  test('falls back to feedId when no onestop id is known', async () => {
    const zip = await buildZip({ 'trips.txt': TRIPS, 'calendar.txt': CALENDAR })
    const { result } = await normalizeSuffixTripIds(zip, { feedId: 'f-dr5r-nyctsubway' })
    expect(result.applied).toBe(true)
  })

  test('is idempotent — a second pass fails the shape guard and changes nothing', async () => {
    const zip = await buildZip({ 'trips.txt': TRIPS, 'stop_times.txt': STOP_TIMES, 'calendar.txt': CALENDAR })
    const once = await normalizeSuffixTripIds(zip, SUBWAY)
    const twice = await normalizeSuffixTripIds(once.buffer, SUBWAY)

    expect(twice.result.applied).toBe(false)
    expect(twice.result.skipReason).toBe('shape-guard')
    expect(twice.buffer).toBe(once.buffer)
  })

  test('skips when a stripped id would not have the expected shape', async () => {
    const trips =
      'route_id,trip_id,service_id\n' +
      'A,SOME_NEW_PREFIX_000600_A..S03R,Weekday\n'
    const zip = await buildZip({ 'trips.txt': trips, 'calendar.txt': CALENDAR })
    const { result } = await normalizeSuffixTripIds(zip, SUBWAY)

    expect(result.applied).toBe(false)
    expect(result.skipReason).toBe('shape-guard')
  })

  test('imports unmodified when two colliding trips run on the same date', async () => {
    // Both services run Saturdays, so the shared suffix would be ambiguous.
    const calendar =
      'service_id,monday,tuesday,wednesday,thursday,friday,saturday,sunday,start_date,end_date\n' +
      'Saturday,0,0,0,0,0,1,0,20260601,20260630\n' +
      'Saturday-H,0,0,0,0,0,1,0,20260601,20260630\n'
    const trips =
      'route_id,trip_id,service_id\n' +
      'A,ASP26GEN-1038-Saturday-00_000600_A..S03R,Saturday\n' +
      'A,BSP26GEN-D085-Saturday-00_000600_A..S03R,Saturday-H\n'
    const zip = await buildZip({ 'trips.txt': trips, 'calendar.txt': calendar })
    const { buffer, result } = await normalizeSuffixTripIds(zip, SUBWAY)

    expect(result.applied).toBe(false)
    expect(result.skipReason).toBe('date-overlap')
    expect(result.collidingSuffixes).toBe(1)
    expect(result.overlaps[0]!.sharedDates).toContain('20260606')
    expect(buffer).toBe(zip)
  })

  test('allows collisions between services that never share a date', async () => {
    const trips =
      'route_id,trip_id,service_id\n' +
      'A,ASP26GEN-1038-Sunday-00_000600_A..S03R,Sunday\n' +
      'A,BSP26GEN-D085-Saturday-00_000600_A..S03R,Saturday\n'
    const zip = await buildZip({ 'trips.txt': trips, 'calendar.txt': CALENDAR })
    const { result } = await normalizeSuffixTripIds(zip, SUBWAY)

    expect(result.applied).toBe(true)
    expect(result.collidingSuffixes).toBe(1)
    expect(result.overlaps).toEqual([])
  })

  test('counts a calendar_dates exception as a shared date', async () => {
    const trips =
      'route_id,trip_id,service_id\n' +
      'A,ASP26GEN-1038-Sunday-00_000600_A..S03R,Sunday\n' +
      'A,BSP26GEN-D085-Saturday-00_000600_A..S03R,Saturday\n'
    // Saturday service also runs on a Sunday, colliding with the Sunday trip.
    const zip = await buildZip({
      'trips.txt': trips,
      'calendar.txt': CALENDAR,
      'calendar_dates.txt': 'service_id,date,exception_type\nSaturday,20260607,1\n',
    })
    const { result } = await normalizeSuffixTripIds(zip, SUBWAY)

    expect(result.skipReason).toBe('date-overlap')
    expect(result.overlaps[0]!.sharedDates).toEqual(['20260607'])
  })

  test('refuses to run when extend_calendar is on', async () => {
    const zip = await buildZip({ 'trips.txt': TRIPS, 'calendar.txt': CALENDAR })
    await expect(
      normalizeSuffixTripIds(zip, SUBWAY, { extendCalendar: true }),
    ).rejects.toThrow(/extend_calendar/)
  })

  test('compresses the rewritten zip instead of storing it', async () => {
    // 400 trips of realistic width; STORE would leave this near its raw size.
    const rows = Array.from(
      { length: 400 },
      (_, i) => `A,ASP26GEN-1038-Sunday-00_${String(i).padStart(6, '0')}_A..S03R,Sunday,Far Rockaway,1`,
    )
    const trips = 'route_id,trip_id,service_id,trip_headsign,direction_id\n' + rows.join('\n') + '\n'
    const zip = await buildZip({ 'trips.txt': trips, 'calendar.txt': CALENDAR })
    const { buffer, result } = await normalizeSuffixTripIds(zip, SUBWAY)

    expect(result.applied).toBe(true)
    expect(buffer.byteLength).toBeLessThan(trips.length / 2)
  })

  test('skips a zip with no trips.txt', async () => {
    const zip = await buildZip({ 'calendar.txt': CALENDAR })
    const { result } = await normalizeSuffixTripIds(zip, SUBWAY)
    expect(result.skipReason).toBe('no-trips')
  })

  test('scope is keyed on feed identity, not id shape', () => {
    expect(SUFFIX_TRIP_ID_FEEDS.has('f-dr5r-nyctsubway')).toBe(true)
    expect(SUFFIX_TRIP_ID_FEEDS.has('f-dr5r-mtabuscompany')).toBe(false)
  })
})

describe('expandServiceDates', () => {
  test('expands a weekly pattern across the service window', () => {
    const dates = expandServiceDates(CALENDAR, null)
    // June 2026: Saturdays fall on the 6th, 13th, 20th and 27th.
    expect([...dates.get('Saturday')!].sort()).toEqual([
      '20260606', '20260613', '20260620', '20260627',
    ])
    expect(dates.get('Weekday')!.has('20260601')).toBe(true)
    expect(dates.get('Weekday')!.has('20260606')).toBe(false)
  })

  test('applies added and removed exception dates', () => {
    const dates = expandServiceDates(
      CALENDAR,
      'service_id,date,exception_type\nSaturday,20260704,1\nSaturday,20260606,2\n',
    )
    expect(dates.get('Saturday')!.has('20260704')).toBe(true)
    expect(dates.get('Saturday')!.has('20260606')).toBe(false)
  })

  test('handles a feed with no calendar.txt', () => {
    const dates = expandServiceDates(null, 'service_id,date,exception_type\nOnce,20260704,1\n')
    expect([...dates.get('Once')!]).toEqual(['20260704'])
  })
})
