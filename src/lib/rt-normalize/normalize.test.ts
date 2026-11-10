import { describe, expect, it } from 'bun:test'
import type { transit_realtime } from 'gtfs-realtime-bindings'
import { buildStaticIndex, serviceDayOrigin } from './static-index'
import { normalizeFeed } from './normalize'

const TZ = 'America/New_York'
const DATE = '20261109'
const origin = serviceDayOrigin(DATE, TZ)
const at = (hhmm: string) => {
  const [h, m] = hhmm.split(':').map(Number)
  return origin + h! * 3600 + m! * 60
}

/** Southbound D: fifteen stops 3 minutes apart, so a 42-minute trip, every 15 minutes from 23:00. */
const starts = ['23:00', '23:15', '23:30', '23:45']
const stops = Array.from({ length: 15 }, (_, i) => `S${i + 1}`)
const pad = (n: number) => String(n).padStart(2, '0')
const clock = (min: number) => `${pad(Math.floor(min / 60))}:${pad(min % 60)}:00`
const minutes = (hhmm: string) => Number(hhmm.slice(0, 2)) * 60 + Number(hhmm.slice(3))

const index = buildStaticIndex({
  agency: `agency_id,agency_name,agency_url,agency_timezone\nMTA,MTA,https://mta.info,${TZ}\n`,
  routes: 'route_id,route_type\nD,1\n',
  stops: 'stop_id,stop_name\n' + stops.map(s => `${s},${s}`).join('\n') + '\n',
  calendar: `service_id,monday,tuesday,wednesday,thursday,friday,saturday,sunday,start_date,end_date\nWKD,1,1,1,1,1,0,0,20261101,20261130\n`,
  trips: 'route_id,service_id,trip_id,direction_id\n' + starts.map(s => `D,WKD,T${s.replace(':', '')},1`).join('\n') + '\n',
  stopTimes:
    'trip_id,arrival_time,departure_time,stop_id,stop_sequence\n' +
    starts.flatMap(s => stops.map((stop, i) => {
      const t = clock(minutes(s) + 3 * i)
      return `T${s.replace(':', '')},${t},${t},${stop},${i + 1}`
    })).join('\n') + '\n',
})

/** A trip update reporting the given stops at `firstAt` plus 3 minutes each. */
function update(tripId: string, firstAt: string, from = 0, extra: object = {}): transit_realtime.IFeedEntity {
  return {
    id: tripId,
    tripUpdate: {
      trip: { tripId, routeId: 'D', startDate: DATE, ...extra },
      stopTimeUpdate: stops.slice(from).map((stopId, i) => ({ stopId, departure: { time: at(firstAt) + 3 * 60 * (i + from) } })),
    },
  }
}

const message = (now: string, ...entity: transit_realtime.IFeedEntity[]): transit_realtime.IFeedMessage =>
  ({ header: { gtfsRealtimeVersion: '2.0', timestamp: at(now) }, entity })

const trips = (out: ReturnType<typeof normalizeFeed>) =>
  (out.message.entity ?? []).map(e => e.tripUpdate?.trip).filter(Boolean)

describe('normalizeFeed', () => {
  it('leaves an update MOTIS already resolves untouched', () => {
    const exact = update('T2330', '23:30', 0, { startTime: '23:30:00' })
    const out = normalizeFeed(index, message('23:31', exact), at('23:31'))
    expect(out.message.entity![0]).toBe(exact)
    expect(out.stats.exact).toBe(1)
  })

  it("dates an update that names its trip but not its day, which MOTIS would read as UTC's today", () => {
    // 00:20 on the 10th, local: the 23:45 trip of the 9th's service is still running.
    const undated = update('T2345', '23:45', 10)
    delete (undated.tripUpdate!.trip as { startDate?: string }).startDate
    const out = normalizeFeed(index, message('24:20', undated), at('24:20'))
    expect(trips(out)[0]).toMatchObject({ tripId: 'T2345', startDate: DATE, startTime: '23:45:00' })
    expect(out.stats.dated).toBe(1)

    // The next evening the same trip_id is that day's run, not the one before.
    const tomorrow = update('T2345', '47:45', 2)
    delete (tomorrow.tripUpdate!.trip as { startDate?: string }).startDate
    expect(trips(normalizeFeed(index, message('47:50', tomorrow), at('47:50')))[0]).toMatchObject({ startDate: '20261110' })
  })

  it('restates a re-timed train under its scheduled run', () => {
    // The feed names the train by its actual dispatch, four minutes late.
    const out = normalizeFeed(index, message('23:40', update('141950_D..S', '23:34', 2, { startTime: '23:34:00' })), at('23:40'))
    expect(trips(out)[0]).toMatchObject({ tripId: 'T2330', startDate: DATE, startTime: '23:30:00', scheduleRelationship: 0 })
    expect(out.stats.matched).toBe(1)
  })

  it('gives two close trains a schedule each instead of both claiming the nearer', () => {
    const out = normalizeFeed(index, message('23:20', update('a', '23:16', 1), update('b', '23:03', 2)), at('23:20'))
    expect(trips(out).slice(0, 2).map(t => t!.tripId)).toEqual(['T2315', 'T2300'])
  })

  it('keeps a late train on its own schedule when the next one fits its times slightly better', () => {
    // T2330 runs 8 minutes late, a minute nearer T2345's times than its own.
    const out = normalizeFeed(index, message('23:41', update('T2315..S', '23:21', 1), update('T2330..S', '23:38', 1)), at('23:41'))
    expect(trips(out).slice(0, 2).map(t => t!.tripId)).toEqual(['T2315', 'T2330'])
  })

  it('adds a train the timetable never had as a NEW trip with the feed times', () => {
    const out = normalizeFeed(index, message('22:20', update('extra', '22:22')), at('22:20'))
    const added = out.message.entity!.find(e => e.tripUpdate?.trip?.scheduleRelationship === 8)!
    expect(added.tripUpdate!.trip!.tripId).toBe(`extra:${DATE}`)
    expect(added.tripUpdate!.stopTimeUpdate).toHaveLength(stops.length)
  })

  it('cancels an under-way run the feed is silent about on a route it covers', () => {
    // 23:00 and 23:15 are under way at 23:21; only 23:15 is reported.
    const out = normalizeFeed(index, message('23:21', update('T2315', '23:15', 2, { startTime: '23:15:00' })), at('23:21'))
    const cancelled = trips(out).filter(t => t!.scheduleRelationship === 3)
    expect(cancelled.map(t => t!.tripId)).toEqual(['T2300'])
  })

  it('cancels nothing on a route where the feed reports under half the running trains', () => {
    // 23:00, 23:15 and 23:30 are under way at 23:36; the feed reports one.
    const out = normalizeFeed(index, message('23:36', update('T2330', '23:30', 2, { startTime: '23:30:00' })), at('23:36'))
    expect(out.stats.cancelled).toBe(0)
  })
})

describe('serviceDayOrigin', () => {
  it('counts GTFS times from noon minus twelve hours across a clock change', () => {
    // Clocks fell back on Nov 1 2026: local midnight was 04:00 UTC, the GTFS origin 05:00.
    expect(serviceDayOrigin('20261101', TZ)).toBe(Date.UTC(2026, 10, 1, 5) / 1000)
    expect(serviceDayOrigin('20261102', TZ)).toBe(Date.UTC(2026, 10, 2, 5) / 1000)
  })
})
