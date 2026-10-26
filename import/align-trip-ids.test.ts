import { describe, expect, test, mock } from 'bun:test'
import JSZip from 'jszip'
import {
  alignRealtimeTripIds,
  chooseTransform,
  candidateTransforms,
  chooseRouteTransforms,
  derivedAffixTransforms,
  truncationTransforms,
  expandServiceDates,
  findDateOverlaps,
  resolvesClearlyMore,
  sampleRealtimeTrips,
  MIN_SAMPLE,
  MIN_ROUTE_SAMPLE,
  type RtSample,
} from './align-trip-ids'

// ── Fixtures ────────────────────────────────────────────────────────

const CALENDAR =
  'service_id,monday,tuesday,wednesday,thursday,friday,saturday,sunday,start_date,end_date\n' +
  'Weekday,1,1,1,1,1,0,0,20260601,20260630\n' +
  'Saturday,0,0,0,0,0,1,0,20260601,20260630\n' +
  'Sunday,0,0,0,0,0,0,1,20260601,20260630\n'

/** MTA's shape: a schedule-version prefix the realtime feed drops. */
function mtaTrips(n: number): { text: string; rtIds: string[] } {
  const rows: string[] = []
  const rtIds: string[] = []
  for (let i = 0; i < n; i++) {
    const suffix = `${String(i).padStart(6, '0')}_A..S03R`
    rows.push(`A,ASP26GEN-1038-Weekday-00_${suffix},Weekday,Far Rockaway,1`)
    rtIds.push(suffix)
  }
  return {
    text: 'route_id,trip_id,service_id,trip_headsign,direction_id\n' + rows.join('\n') + '\n',
    rtIds,
  }
}

function buildZip(files: Record<string, string>): Promise<ArrayBuffer> {
  const zip = new JSZip()
  for (const [name, content] of Object.entries(files)) zip.file(name, content)
  return zip.generateAsync({ type: 'arraybuffer' })
}

async function read(buffer: ArrayBuffer, name: string): Promise<string> {
  const zip = await JSZip.loadAsync(buffer)
  return await zip.file(name)!.async('string')
}

const FEED = { feedId: '5', onestopId: 'f-dr5r-nyctsubway' }

/** A realtime sample from trip_id → route_id pairs, undated. */
const sampleOf = (pairs: Iterable<readonly [string, string]>): RtSample =>
  new Map([...pairs].map(([id, routeId]) => [id, { routeId, startDate: '' }]))

// ── Choosing a transform ────────────────────────────────────────────

describe('chooseTransform', () => {
  test('finds the prefix strip a suffix-style feed needs', () => {
    const { text, rtIds } = mtaTrips(40)
    const staticIds = text.trim().split('\n').slice(1).map(l => l.split(',')[1]!)
    const choice = chooseTransform(staticIds, new Set(rtIds))

    expect(choice.reason).toBe('applied')
    expect(choice.transform!.id).toBe('drop-through-first:_')
    expect(choice.best!.matched).toBe(40)
  })

  test('finds a truncated variant suffix', () => {
    // Static carries a trailing variant the realtime feed omits.
    const staticIds = Array.from({ length: 40 }, (_, i) => `${100000 + i}_L..N01R`)
    const rtIds = new Set(staticIds.map(id => id.slice(0, id.lastIndexOf('.'))))
    const choice = chooseTransform(staticIds, rtIds)

    expect(choice.reason).toBe('applied')
    expect(choice.transform!.id).toBe('drop-from-last:.')
  })

  test('learns an agency prefix that contains no separator', () => {
    const staticIds = Array.from({ length: 40 }, (_, i) => `AGENCYXX${1000 + i}`)
    const rtIds = new Set(staticIds.map(id => id.slice('AGENCYXX'.length)))
    const choice = chooseTransform(staticIds, rtIds)

    expect(choice.reason).toBe('applied')
    expect(choice.transform!.id).toBe('drop-learned-prefix')
    expect(choice.transform!.describe).toContain('AGENCYXX')
  })

  test('learns a trailing variant the realtime feed omits', () => {
    const staticIds = Array.from({ length: 40 }, (_, i) => `TRIP${1000 + i}X0Y`)
    const rtIds = new Set(staticIds.map(id => id.slice(0, -3)))
    const choice = chooseTransform(staticIds, rtIds)

    expect(choice.reason).toBe('applied')
    expect(choice.transform!.id).toBe('drop-learned-suffix')
  })

  test('leaves a feed alone when its ids already resolve', () => {
    const staticIds = Array.from({ length: 40 }, (_, i) => `MQ_C6-Weekday-030000_M66_${i}`)
    const choice = chooseTransform(staticIds, new Set(staticIds.slice(0, 30)))

    expect(choice.transform).toBeNull()
    expect(choice.reason).toBe('already-resolving')
  })

  test('leaves a partially resolving feed alone', () => {
    // 20% already match — under the "broken" bar, so nothing is rewritten.
    const staticIds = Array.from({ length: 40 }, (_, i) => `pre_${i}`)
    const rtIds = new Set([...staticIds.slice(0, 8), ...Array.from({ length: 32 }, (_, i) => `zzz${i}`)])
    const choice = chooseTransform(staticIds, rtIds)

    expect(choice.transform).toBeNull()
    expect(choice.reason).toBe('already-resolving')
  })

  test('declines when no transform reaches enough of the sample', () => {
    const staticIds = Array.from({ length: 40 }, (_, i) => `SCHED-${i}-X`)
    // Realtime ids share nothing with the schedule, however it is sliced.
    const rtIds = new Set(Array.from({ length: 40 }, (_, i) => `wholly-different-${i}`))
    const choice = chooseTransform(staticIds, rtIds)

    expect(choice.transform).toBeNull()
    expect(choice.reason).toBe('no-improvement')
  })

  test('refuses to decide on a sample that is too small', () => {
    const { text, rtIds } = mtaTrips(MIN_SAMPLE - 1)
    const staticIds = text.trim().split('\n').slice(1).map(l => l.split(',')[1]!)
    const choice = chooseTransform(staticIds, new Set(rtIds))

    expect(choice.transform).toBeNull()
    expect(choice.reason).toBe('sample-too-small')
  })

  test('prefers the transform that collapses the fewest ids on a tie', () => {
    // Both `drop-through-first:_` and `drop-through-last:_` reach every
    // realtime id, but the last-underscore split throws away more.
    const staticIds = Array.from({ length: 40 }, (_, i) => `P_${i}_x`)
    const rtIds = new Set(staticIds.map(id => id.slice(2)))
    const choice = chooseTransform(staticIds, rtIds)

    expect(choice.transform!.id).toBe('drop-through-first:_')
  })
})

describe('candidateTransforms', () => {
  test('covers both directions for every separator', () => {
    const ids = candidateTransforms(['a_b-c:d.e'], new Set()).map(t => t.id)
    for (const sep of ['_', '-', ':', '.']) {
      expect(ids).toContain(`drop-through-first:${sep}`)
      expect(ids).toContain(`drop-from-last:${sep}`)
    }
  })
})

describe('derivedAffixTransforms', () => {
  test('learns nothing from a realtime id unrelated to any schedule id', () => {
    expect(derivedAffixTransforms(['abc1', 'abc2'], new Set(['zzz']))).toEqual([])
  })

  test('ignores an affix seen too few times to be a pattern', () => {
    // One coincidental tail match is not a rule.
    expect(derivedAffixTransforms(['PREFIX_1', 'OTHER_2', 'THIRD_3'], new Set(['1']))).toEqual([])
  })

  test('learns the prefix once enough realtime ids agree', () => {
    const staticIds = ['PRE1', 'PRE2', 'PRE3', 'PRE4']
    const learned = derivedAffixTransforms(staticIds, new Set(['1', '2', '3']))
    expect(learned.map(t => t.id)).toContain('drop-learned-prefix')
    expect(learned[0]!.apply('PRE9')).toBe('9')
  })
})

describe('truncationTransforms', () => {
  test('cuts static ids to the length realtime publishes, whatever the tail', () => {
    // Realtime drops a path suffix that differs from trip to trip.
    const staticIds = Array.from({ length: 30 }, (_, i) => `${String(i).padStart(6, '0')}_L..S0${i % 3}R`)
    const rtIds = new Set(staticIds.map(id => id.slice(0, 11)))
    const [cut] = truncationTransforms(staticIds, rtIds)

    expect(cut!.id).toBe('truncate-to:11')
    expect(cut!.apply('000007_L..S01R')).toBe('000007_L..S')
    expect(cut!.apply('short')).toBe('short')
  })

  test('learns nothing when realtime ids are not heads of static ids', () => {
    expect(truncationTransforms(['abc1', 'abc2', 'abc3'], new Set(['zz', 'yy', 'xx']))).toEqual([])
  })

  test('wins the feed-wide choice where no fixed suffix fits', () => {
    const staticIds = Array.from({ length: 40 }, (_, i) => `${String(i).padStart(6, '0')}_7..N9${i % 4}R`)
    const choice = chooseTransform(staticIds, new Set(staticIds.map(id => id.slice(0, 11))))

    expect(choice.transform!.id).toBe('truncate-to:11')
    expect(choice.best!.matched).toBe(40)
  })

  test('loses a tie to a separator, which also covers days the sample missed', () => {
    // Friday ids in the sample are 11 characters; Monday ids are 12.
    const staticIds = [
      ...Array.from({ length: 30 }, (_, i) => `Fri_${String(i).padStart(7, 'a')}-5`),
      ...Array.from({ length: 30 }, (_, i) => `M-Th_${String(i).padStart(7, 'a')}-2`),
    ]
    const rtIds = new Set(staticIds.slice(0, 30).map(id => id.slice(0, -2)))
    const choice = chooseTransform(staticIds, rtIds)

    expect(choice.transform!.id).toBe('drop-from-last:-')
    expect(choice.transform!.apply('M-Th_aaaaaa1-2')).toBe('M-Th_aaaaaa1')
  })
})

describe('chooseRouteTransforms', () => {
  const route = (routeId: string, n: number, tail: string) =>
    Array.from({ length: n }, (_, i) => ({
      id: `${String(i).padStart(6, '0')}_${routeId}..S${tail}`,
      serviceId: 'Weekday',
      routeId,
    }))

  test('refines only the route whose realtime ids still miss', () => {
    const trips = [...route('A', 20, '03R'), ...route('L', 20, '01R')]
    const rtTrips = sampleOf([
      ...trips.filter(t => t.routeId === 'A').map(t => [t.id, 'A'] as [string, string]),
      ...trips.filter(t => t.routeId === 'L').map(t => [t.id.slice(0, 11), 'L'] as [string, string]),
    ])
    const choices = chooseRouteTransforms(trips, new Map(), rtTrips)

    expect([...choices.keys()]).toEqual(['L'])
    expect(choices.get('L')!.best!.matched).toBe(20)
  })

  test('leaves a route that mostly resolves, even if some ids are short', () => {
    const trips = route('G', 20, '14R')
    const rtTrips = sampleOf(
      trips.map((t, i) => [i < 12 ? t.id : t.id.slice(0, 11), 'G'] as [string, string]),
    )
    expect(chooseRouteTransforms(trips, new Map(), rtTrips).size).toBe(0)
  })

  test('waits for a route seen too rarely to judge', () => {
    const trips = route('L', MIN_ROUTE_SAMPLE - 1, '01R')
    const rtTrips = sampleOf(trips.map(t => [t.id.slice(0, 11), 'L'] as [string, string]))
    expect(chooseRouteTransforms(trips, new Map(), rtTrips).size).toBe(0)
  })
})

// ── Sampling ────────────────────────────────────────────────────────

describe('sampleRealtimeTrips', () => {
  const encoded = new Uint8Array([1, 2, 3])

  test('unions trip ids across a feed’s realtime URLs', async () => {
    mock.module('gtfs-realtime-bindings', () => ({
      default: {
        transit_realtime: {
          FeedMessage: {
            decode: () => ({
              entity: [
                { tripUpdate: { trip: { tripId: 'a', routeId: 'L' } } },
                { tripUpdate: { trip: { tripId: 'b' } } },
                { vehicle: { trip: { tripId: 'ignored' } } },
              ],
            }),
          },
        },
      },
    }))
    const fetchFn = mock(async () => new Response(encoded, { status: 200 }))
    const ids = await sampleRealtimeTrips(
      [
        { url: 'https://example.test/a', type: 'tripUpdates' },
        { url: 'https://example.test/b', type: 'tripUpdates' },
      ],
      fetchFn as never,
    )

    expect(fetchFn).toHaveBeenCalledTimes(2)
    expect([...ids.keys()].sort()).toEqual(['a', 'b'])
    expect(ids.get('a')).toEqual({ routeId: 'L', startDate: '' })
    expect(ids.get('b')).toEqual({ routeId: '', startDate: '' })
  })

  test('ignores vehicle-position and alert URLs', async () => {
    const fetchFn = mock(async () => new Response(encoded, { status: 200 }))
    await sampleRealtimeTrips(
      [
        { url: 'https://example.test/vp', type: 'vehiclePositions' },
        { url: 'https://example.test/al', type: 'alerts' },
      ],
      fetchFn as never,
    )
    expect(fetchFn).not.toHaveBeenCalled()
  })

  test('returns nothing rather than throwing when realtime is unreachable', async () => {
    const warnings: string[] = []
    const ids = await sampleRealtimeTrips(
      [{ url: 'https://example.test/a', type: 'tripUpdates' }],
      (async () => { throw new Error('ECONNREFUSED') }) as never,
      m => warnings.push(m),
    )

    expect(ids.size).toBe(0)
    expect(warnings[0]).toContain('ECONNREFUSED')
  })

  test('reports a non-200 without failing the import', async () => {
    const warnings: string[] = []
    const ids = await sampleRealtimeTrips(
      [{ url: 'https://example.test/a', type: 'tripUpdates' }],
      (async () => new Response('nope', { status: 429 })) as never,
      m => warnings.push(m),
    )

    expect(ids.size).toBe(0)
    expect(warnings[0]).toContain('429')
  })

  test('handles a feed with no realtime URLs at all', async () => {
    expect((await sampleRealtimeTrips(undefined)).size).toBe(0)
  })
})

// ── End to end ──────────────────────────────────────────────────────

describe('alignRealtimeTripIds', () => {
  const withSample = (rtIds: string[], routeId = '') => ({ rtTrips: sampleOf(rtIds.map(id => [id, routeId] as const)) })

  test('rewrites every trip_id-bearing file', async () => {
    const { text, rtIds } = mtaTrips(40)
    const stopTimes =
      'trip_id,stop_id,arrival_time,departure_time,stop_sequence\n' +
      text.trim().split('\n').slice(1)
        .map(l => `${l.split(',')[1]},A01S,00:06:00,00:06:00,1`)
        .join('\n') + '\n'
    const zip = await buildZip({ 'trips.txt': text, 'stop_times.txt': stopTimes, 'calendar.txt': CALENDAR })

    const { buffer, result } = await alignRealtimeTripIds(zip, FEED, withSample(rtIds))

    expect(result.applied).toBe(true)
    expect(result.transform).toBe('drop-through-first:_')
    expect(result.rewrittenRows).toEqual({ 'trips.txt': 40, 'stop_times.txt': 40 })
    expect(result.matched).toBe(40)
    expect(await read(buffer, 'trips.txt')).not.toContain('ASP26GEN')
    expect(await read(buffer, 'stop_times.txt')).not.toContain('ASP26GEN')
  })

  test('leaves the header and other columns untouched', async () => {
    const { text, rtIds } = mtaTrips(40)
    const zip = await buildZip({ 'trips.txt': text, 'calendar.txt': CALENDAR })
    const { buffer } = await alignRealtimeTripIds(zip, FEED, withSample(rtIds))
    const rows = (await read(buffer, 'trips.txt')).trim().split('\n')

    expect(rows[0]).toBe('route_id,trip_id,service_id,trip_headsign,direction_id')
    expect(rows[1]).toBe('A,000000_A..S03R,Weekday,Far Rockaway,1')
  })

  test('re-quotes fields containing a comma', async () => {
    const { text, rtIds } = mtaTrips(40)
    const quoted = text.replace('Far Rockaway,1', '"Rockaway, Beach 116",1')
    const zip = await buildZip({ 'trips.txt': quoted, 'calendar.txt': CALENDAR })
    const { buffer, result } = await alignRealtimeTripIds(zip, FEED, withSample(rtIds))

    expect(result.applied).toBe(true)
    expect(await read(buffer, 'trips.txt')).toContain('"Rockaway, Beach 116"')
  })

  test('is idempotent — a second pass measures as already resolving', async () => {
    const { text, rtIds } = mtaTrips(40)
    const zip = await buildZip({ 'trips.txt': text, 'calendar.txt': CALENDAR })
    const once = await alignRealtimeTripIds(zip, FEED, withSample(rtIds))
    const twice = await alignRealtimeTripIds(once.buffer, FEED, withSample(rtIds))

    expect(twice.result.applied).toBe(false)
    expect(twice.result.skipReason).toBe('already-resolving')
    expect(twice.buffer).toBe(once.buffer)
  })

  test('leaves a feed whose ids already match completely alone', async () => {
    const trips =
      'route_id,trip_id,service_id\n' +
      Array.from({ length: 40 }, (_, i) => `M66,MQ_C6-Weekday-030000_M66_${i},Weekday`).join('\n') + '\n'
    const rtIds = Array.from({ length: 40 }, (_, i) => `MQ_C6-Weekday-030000_M66_${i}`)
    const zip = await buildZip({ 'trips.txt': trips, 'calendar.txt': CALENDAR })

    const { buffer, result } = await alignRealtimeTripIds(zip, { feedId: '7' }, withSample(rtIds))

    expect(result.applied).toBe(false)
    expect(result.skipReason).toBe('already-resolving')
    expect(buffer).toBe(zip)
  })

  test('imports unmodified when the rewrite would merge trips running the same day', async () => {
    const calendar =
      'service_id,monday,tuesday,wednesday,thursday,friday,saturday,sunday,start_date,end_date\n' +
      'Saturday,0,0,0,0,0,1,0,20260601,20260630\n' +
      'Saturday-H,0,0,0,0,0,1,0,20260601,20260630\n'
    // Every suffix is claimed by both services, and both run every Saturday.
    const rows: string[] = []
    const rtIds: string[] = []
    for (let i = 0; i < 40; i++) {
      const suffix = `${String(i).padStart(6, '0')}_A..S03R`
      rows.push(`A,ASP26GEN_${suffix},Saturday`)
      rows.push(`A,BSP26GEN_${suffix},Saturday-H`)
      rtIds.push(suffix)
    }
    const zip = await buildZip({
      'trips.txt': 'route_id,trip_id,service_id\n' + rows.join('\n') + '\n',
      'calendar.txt': calendar,
    })

    const { buffer, result } = await alignRealtimeTripIds(zip, FEED, withSample(rtIds))

    expect(result.applied).toBe(false)
    expect(result.skipReason).toBe('date-overlap')
    expect(result.collidingTripIds).toBe(40)
    expect(result.overlaps[0]!.sharedDates).toContain('20260606')
    expect(buffer).toBe(zip)
  })

  test('never writes a shared id: the variant running soonest takes it', async () => {
    // MOTIS drops every trip whose id is shared, even across days that never
    // overlap — MTA's supplemented subway feed lost 88% of its trips this way.
    const rows: string[] = []
    const rtIds: string[] = []
    for (let i = 0; i < 40; i++) {
      const suffix = `${String(i).padStart(6, '0')}_A..S03R`
      rows.push(`A,ASP26GEN_${suffix},Saturday`)
      rows.push(`A,BSP26GEN_${suffix},Sunday`)
      rtIds.push(suffix)
    }
    const zip = await buildZip({
      'trips.txt': 'route_id,trip_id,service_id\n' + rows.join('\n') + '\n',
      'calendar.txt': CALENDAR,
    })

    // Saturday 2026-06-13: Saturday's variant runs first.
    const sat = await alignRealtimeTripIds(zip, FEED, { ...withSample(rtIds), now: Date.UTC(2026, 5, 13) })
    expect(sat.result.applied).toBe(true)
    expect(sat.result.collidingTripIds).toBe(0)
    expect(sat.result.heldTrips).toBe(40)
    const satTrips = (await read(sat.buffer, 'trips.txt')).trim().split('\n').slice(1)
    expect(new Set(satTrips.map(r => r.split(',')[1])).size).toBe(80)
    expect(satTrips).toContain('A,000007_A..S03R,Saturday')
    expect(satTrips).toContain('A,BSP26GEN_000007_A..S03R,Sunday')

    // Sunday 2026-06-14: the id moves to Sunday's variant.
    const sun = await alignRealtimeTripIds(zip, FEED, { ...withSample(rtIds), now: Date.UTC(2026, 5, 14) })
    const sunTrips = await read(sun.buffer, 'trips.txt')
    expect(sunTrips).toContain('A,000007_A..S03R,Sunday')
    expect(sunTrips).toContain('A,ASP26GEN_000007_A..S03R,Saturday')
  })

  test('does nothing when the realtime feed yields no sample', async () => {
    const { text } = mtaTrips(40)
    const zip = await buildZip({ 'trips.txt': text, 'calendar.txt': CALENDAR })
    const { buffer, result } = await alignRealtimeTripIds(zip, FEED, { rtTrips: sampleOf([]) })

    expect(result.skipReason).toBe('no-realtime')
    expect(buffer).toBe(zip)
  })

  test('does nothing for a zip with no trips.txt', async () => {
    const zip = await buildZip({ 'calendar.txt': CALENDAR })
    const { result } = await alignRealtimeTripIds(zip, FEED, withSample(['x']))
    expect(result.skipReason).toBe('no-trips')
  })

  test('refuses to run when extend_calendar is on', async () => {
    const { text, rtIds } = mtaTrips(40)
    const zip = await buildZip({ 'trips.txt': text, 'calendar.txt': CALENDAR })
    await expect(
      alignRealtimeTripIds(zip, FEED, { ...withSample(rtIds), extendCalendar: true }),
    ).rejects.toThrow(/extend_calendar/)
  })

  /** A subway-shaped feed: route A matches after the prefix strip, route L also loses its path suffix. */
  function twoConventions(extraL: string[] = []) {
    const rows: string[] = []
    const rt: Array<[string, string]> = []
    for (let i = 0; i < 25; i++) {
      const origin = String(i * 100).padStart(6, '0')
      rows.push(`A,ASP26GEN-Weekday-00_${origin}_A..S03R,Weekday`)
      rows.push(`L,ASP26GEN-Weekday-00_${origin}_L..N0${i % 2}R,Weekday`)
      rt.push([`${origin}_A..S03R`, 'A'], [`${origin}_L..N`, 'L'])
    }
    rows.push(...extraL)
    const trips = 'route_id,trip_id,service_id\n' + rows.join('\n') + '\n'
    const stopTimes =
      'trip_id,stop_id,departure_time,stop_sequence\n' +
      rows.map(r => `${r.split(',')[1]},S1,08:00:00,1`).join('\n') + '\n'
    return { trips, stopTimes, rtTrips: sampleOf(rt) }
  }

  test('applies a route transform on top of the feed-wide one', async () => {
    const { trips, stopTimes, rtTrips } = twoConventions()
    const zip = await buildZip({ 'trips.txt': trips, 'stop_times.txt': stopTimes, 'calendar.txt': CALENDAR })
    const { buffer, result } = await alignRealtimeTripIds(zip, FEED, { rtTrips })

    expect(result.applied).toBe(true)
    expect(result.transform).toBe('drop-through-first:_')
    expect(Object.keys(result.routeTransforms)).toEqual(['L'])
    expect(result.matched).toBe(50)
    const rewritten = await read(buffer, 'trips.txt')
    expect(rewritten).toContain('L,000100_L..N,Weekday')
    expect(rewritten).toContain('A,000100_A..S03R,Weekday')
    expect(await read(buffer, 'stop_times.txt')).toContain('000100_L..N,S1')
  })

  test('holds back trips a route transform would merge, and rewrites the rest', async () => {
    // A second L train leaves the same terminal at the same minute on another path.
    const { trips, rtTrips } = twoConventions(['L,ASP26GEN-Weekday-00_000300_L..N05R,Weekday'])
    const zip = await buildZip({ 'trips.txt': trips, 'calendar.txt': CALENDAR })
    const { buffer, result } = await alignRealtimeTripIds(zip, FEED, { rtTrips })

    expect(result.applied).toBe(true)
    expect(result.heldTrips).toBe(2)
    const rewritten = await read(buffer, 'trips.txt')
    expect(rewritten).toContain('000300_L..N01R')
    expect(rewritten).toContain('000300_L..N05R')
    expect(rewritten).not.toContain('000300_L..N,')
    expect(rewritten).toContain('000500_L..N,')
  })

  test('holds back the few trips a feed-wide transform would merge', async () => {
    // Most ids carry a "-<service>" tail realtime drops; three carry none, so
    // the cut lands inside them and merges them onto one id.
    const rows = [
      ...Array.from({ length: 30 }, (_, i) => `R1,Fri_trip${i}-5,Weekday`),
      'R1,M-Th_a,Weekday',
      'R1,M-Th_b,Weekday',
      'R1,M-Th_c,Weekday',
    ]
    const zip = await buildZip({
      'trips.txt': 'route_id,trip_id,service_id\n' + rows.join('\n') + '\n',
      'calendar.txt': CALENDAR,
    })
    const rtTrips = sampleOf(Array.from({ length: 30 }, (_, i) => [`Fri_trip${i}`, '']))
    const { buffer, result } = await alignRealtimeTripIds(zip, FEED, { rtTrips })

    expect(result.applied).toBe(true)
    expect(result.heldTrips).toBe(3)
    expect(result.matched).toBe(30)
    const rewritten = await read(buffer, 'trips.txt')
    expect(rewritten).toContain('R1,Fri_trip7,Weekday')
    expect(rewritten).toContain('R1,M-Th_a,Weekday')
  })

  test('a route transform is idempotent too', async () => {
    const { trips, rtTrips } = twoConventions()
    const zip = await buildZip({ 'trips.txt': trips, 'calendar.txt': CALENDAR })
    const once = await alignRealtimeTripIds(zip, FEED, { rtTrips })
    const twice = await alignRealtimeTripIds(once.buffer, FEED, { rtTrips })

    expect(twice.result.applied).toBe(false)
    expect(twice.result.skipReason).toBe('already-resolving')
    expect(twice.buffer).toBe(once.buffer)
  })

  test('compresses the rewritten zip instead of storing it', async () => {
    const { text, rtIds } = mtaTrips(400)
    const zip = await buildZip({ 'trips.txt': text, 'calendar.txt': CALENDAR })
    const { buffer, result } = await alignRealtimeTripIds(zip, FEED, withSample(rtIds))

    expect(result.applied).toBe(true)
    expect(buffer.byteLength).toBeLessThan(text.length / 2)
  })
})

// ── Supporting pieces ───────────────────────────────────────────────

describe('findDateOverlaps', () => {
  test('reports only groups whose services run together', () => {
    const dates = expandServiceDates(CALENDAR, null)
    const { overlaps, collidingTripIds } = findDateOverlaps(
      new Map([
        ['shared', ['Saturday', 'Sunday']],
        ['clashing', ['Weekday', 'Weekday']],
        ['alone', ['Weekday']],
      ]),
      dates,
    )

    expect(collidingTripIds).toBe(2)
    expect(overlaps).toHaveLength(1)
    expect(overlaps[0]!.tripId).toBe('clashing')
  })
})

describe('expandServiceDates', () => {
  test('expands a weekly pattern across the service window', () => {
    const dates = expandServiceDates(CALENDAR, null)
    // June 2026: Saturdays fall on the 6th, 13th, 20th and 27th.
    expect([...dates.get('Saturday')!].sort()).toEqual(['20260606', '20260613', '20260620', '20260627'])
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

describe('resolvesClearlyMore', () => {
  const result = (resolving: number, sampleSize = 100) => ({
    applied: true, rewrittenRows: {}, collidingTripIds: 0, overlaps: [], routeTransforms: {}, heldTrips: 0,
    sampleSize, matched: resolving, resolving,
  })

  test('takes an alternate that reaches clearly more of the sample', () => {
    expect(resolvesClearlyMore(result(80), result(69))).toBe(true)
  })

  test('keeps the catalogued feed on a near tie', () => {
    expect(resolvesClearlyMore(result(71), result(69))).toBe(false)
  })

  test('never decides on a sample too small to judge', () => {
    expect(resolvesClearlyMore(result(10, MIN_SAMPLE - 1), result(0, MIN_SAMPLE - 1))).toBe(false)
  })

  test('a feed whose ids match but whose calendar has lapsed resolves nothing', async () => {
    const { text, rtIds } = mtaTrips(40)
    const zip = await buildZip({ 'trips.txt': text, 'calendar.txt': CALENDAR })
    const dated = (startDate: string) => ({
      rtTrips: new Map(rtIds.map(id => [id, { routeId: 'A', startDate }])) as RtSample,
    })

    const running = await alignRealtimeTripIds(zip, FEED, dated('20260603'))
    const lapsed = await alignRealtimeTripIds(zip, FEED, dated('20261016'))

    expect(running.result.resolving).toBe(40)
    expect(lapsed.result.matched).toBe(40)
    expect(lapsed.result.resolving).toBe(0)
    expect(resolvesClearlyMore(lapsed.result, running.result)).toBe(false)
  })
})
