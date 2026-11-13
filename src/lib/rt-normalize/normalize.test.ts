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

/** Southbound D: fifteen stops 3 minutes apart, so a 42-minute trip, from 23:00 to 23:50. */
const starts = ['23:00', '23:15', '23:30', '23:45', '23:50']
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

  it("moves an update dated by the calendar day onto the run its times fit, and cancels nothing for it", () => {
    // 00:20 on the 10th: the 23:45 run of the 9th's service, dated the 10th by the feed.
    const mislabelled = update('T2345', '23:45', 10, { startDate: '20261110' })
    const out = normalizeFeed(index, message('24:20', mislabelled), at('24:20'))
    expect(trips(out)[0]).toMatchObject({ tripId: 'T2345', startDate: DATE })
    expect(trips(out).filter(t => t!.scheduleRelationship === 3).map(t => t!.tripId)).not.toContain('T2345')
  })

  it("names a trip whose realtime id carries another version suffix by the timetable's id", () => {
    const versioned = buildStaticIndex({
      agency: `agency_id,agency_name,agency_url,agency_timezone\nCM,CM,https://x,${TZ}\n`,
      routes: 'route_id,route_type\n3,3\n',
      calendar: `service_id,monday,tuesday,wednesday,thursday,friday,saturday,sunday,start_date,end_date\nWKD,1,1,1,1,1,0,0,20261101,20261130\nOLD,1,1,1,1,1,0,0,20260101,20260131\n`,
      trips: 'route_id,service_id,trip_id\n3,WKD,3000797_6158\n3,OLD,3000797_6169\n',
      stopTimes: 'trip_id,arrival_time,departure_time,stop_id,stop_sequence\n3000797_6158,15:00:00,15:00:00,A,1\n3000797_6158,15:10:00,15:10:00,B,2\n3000797_6169,15:00:00,15:00:00,A,1\n3000797_6169,15:10:00,15:10:00,B,2\n',
    })
    const delayed: transit_realtime.IFeedEntity = {
      id: 'x',
      tripUpdate: { trip: { tripId: '3000797_5797', routeId: '3', startDate: DATE, startTime: '15:00:00' }, stopTimeUpdate: [{ stopSequence: 2, stopId: 'B', arrival: { delay: 120 } }] },
    }
    const out = normalizeFeed(versioned, message('15:05', delayed), at('15:05'))
    expect(trips(out)[0]).toMatchObject({ tripId: '3000797_6158', startDate: DATE })
  })

  it('restates a re-timed train under its scheduled run', () => {
    // The feed names the train by its actual dispatch, four minutes late.
    const out = normalizeFeed(index, message('23:40', update('141950_D..S', '23:34', 2, { startTime: '23:34:00' })), at('23:40'))
    expect(trips(out)[0]).toMatchObject({ tripId: 'T2330', startDate: DATE, startTime: '23:30:00', scheduleRelationship: 0 })
    expect(out.stats.matched).toBe(1)
  })

  it('gives two close trains a schedule each instead of both claiming the nearer', () => {
    const out = normalizeFeed(index, message('23:20', update('a', '23:16', 1), update('b', '23:03', 2)), at('23:20'))
    const tripOf = (id: string) => out.message.entity!.find(e => e.id === id)!.tripUpdate!.trip!.tripId
    expect([tripOf('a'), tripOf('b')]).toEqual(['T2315', 'T2300'])
  })

  it('keeps a late train on its own schedule when the next one fits its times slightly better', () => {
    // T2330 runs 8 minutes late, a minute nearer T2345's times than its own.
    const out = normalizeFeed(index, message('23:41', update('T2315..S', '23:21', 1), update('T2330..S', '23:38', 1)), at('23:41'))
    const tripOf = (id: string) => out.message.entity!.find(e => e.id === id)!.tripUpdate!.trip!.tripId
    expect([tripOf('T2315..S'), tripOf('T2330..S')]).toEqual(['T2315', 'T2330'])
  })

  it('marks a scheduled stop the update leaves out of its list as skipped', () => {
    const rerouted = update('T2330', '23:30', 0, { startTime: '23:30:00' })
    rerouted.tripUpdate!.stopTimeUpdate = rerouted.tripUpdate!.stopTimeUpdate!.filter(s => s.stopId !== 'S5')
    const others = [update('T2300', '23:00', 11, { startTime: '23:00:00' }), update('T2315', '23:15', 6, { startTime: '23:15:00' }), update('T2345', '23:45', 0, { startTime: '23:45:00' }), update('T2350', '23:50', 0, { startTime: '23:50:00' })]
    const out = normalizeFeed(index, message('23:31', rerouted, ...others), at('23:31'))
    const stops = out.message.entity![0]!.tripUpdate!.stopTimeUpdate!
    expect(stops.find(s => s.stopId === 'S5')?.scheduleRelationship).toBe(1)
    expect(stops.map(s => s.stopId)).toEqual(index.byTripId.get('T2330')![0]!.stopIds)
  })

  it('reads no skips into an update that lists only some of the stops it spans', () => {
    const timepoints = ['T2300', 'T2315', 'T2330', 'T2345'].map((id, n) => {
      const e = update(id, starts[n]!, 0, { startTime: `${starts[n]}:00` })
      e.tripUpdate!.stopTimeUpdate = e.tripUpdate!.stopTimeUpdate!.filter((_, i) => i % 3 === 0)
      return e
    })
    expect(normalizeFeed(index, message('23:31', ...timepoints), at('23:31')).stats.skipped).toBe(0)
  })

  it('reads a short list as the train ending early only where the feed lists whole runs', () => {
    const shortTurn = update('T2330', '23:30', 0, { startTime: '23:30:00' })
    shortTurn.tripUpdate!.stopTimeUpdate = shortTurn.tripUpdate!.stopTimeUpdate!.slice(0, 8)
    const whole = [
      update('T2300', '23:00', 12, { startTime: '23:00:00' }),
      update('T2315', '23:15', 5, { startTime: '23:15:00' }),
      update('T2345', '23:45', 0, { startTime: '23:45:00' }),
      update('T2350', '23:50', 0, { startTime: '23:50:00' }),
    ]
    const listed = normalizeFeed(index, message('23:31', shortTurn, ...whole), at('23:31'))
    expect(listed.stats.skipped).toBe(stops.length - 8)
    expect(listed.message.entity![0]!.tripUpdate!.stopTimeUpdate!.slice(8).every(s => s.scheduleRelationship === 1)).toBe(true)

    // A feed that only ever gives the next few stops.
    const nextFew = [shortTurn, ...whole.map(e => ({ ...e, tripUpdate: { ...e.tripUpdate!, stopTimeUpdate: e.tripUpdate!.stopTimeUpdate!.slice(0, 2) } }))]
    expect(normalizeFeed(index, message('23:31', ...nextFew), at('23:31')).stats.skipped).toBe(0)
  })

  it('sends updates in the order their trips run, so a late trip cannot overwrite the next one in its block', () => {
    // One vehicle: T2300, running late, then T2345. The feed lists the later trip first.
    const later = update('T2345', '23:45', 0, { startTime: '23:45:00' })
    const late = update('T2300', '23:05', 12, { startTime: '23:00:00' })
    const out = normalizeFeed(index, message('23:38', later, late), at('23:38'))
    expect(out.message.entity!.map(e => e.id).slice(0, 2)).toEqual(['T2300', 'T2345'])
  })

  it('fits a named trip by its times when its stated start is not in the timetable, and never adds it as new', () => {
    const out = normalizeFeed(index, message('23:40', update('T2330', '23:33', 2, { startTime: '23:31:00' })), at('23:40'))
    expect(trips(out)[0]).toMatchObject({ tripId: 'T2330', startTime: '23:30:00' })
    const stale = normalizeFeed(index, message('23:40', update('T2330', '18:00', 2, { startTime: '23:31:00' })), at('23:40'))
    expect(stale.stats.added).toBe(0)
  })

  it('keeps a train running far behind on the run its update names', () => {
    const late = update('T2315', '23:45', 0, { startTime: '23:15:00' })
    const out = normalizeFeed(index, message('23:50', late), at('23:50'))
    expect(trips(out)[0]).toMatchObject({ tripId: 'T2315', startDate: DATE })
    expect(out.stats.exact).toBe(1)

    delete (late.tripUpdate!.trip as { startDate?: string }).startDate
    expect(trips(normalizeFeed(index, message('23:50', late), at('23:50')))[0]).toMatchObject({ tripId: 'T2315', startDate: DATE })
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

  it('cancels the timetable on a route a service change reroutes onto trains it lacks', () => {
    // 23:00, 23:15 and 23:30 are under way at 23:36; the feed reports two trains on another pattern.
    const rerouted = (tripId: string, firstAt: string): transit_realtime.IFeedEntity => ({
      id: tripId,
      tripUpdate: {
        trip: { tripId, routeId: 'D', startDate: DATE },
        stopTimeUpdate: ['R1', 'R2', 'R3'].map((stopId, i) => ({ stopId, departure: { time: at(firstAt) + 3 * 60 * i } })),
      },
    })
    const out = normalizeFeed(index, message('23:36', rerouted('X1', '23:20'), rerouted('X2', '23:35')), at('23:36'))
    const cancelled = trips(out).filter(t => t!.scheduleRelationship === 3)
    expect(cancelled.map(t => t!.tripId).sort()).toEqual(['T2300', 'T2315', 'T2330'])
  })
})

describe('serviceDayOrigin', () => {
  it('counts GTFS times from noon minus twelve hours across a clock change', () => {
    // Clocks fell back on Nov 1 2026: local midnight was 04:00 UTC, the GTFS origin 05:00.
    expect(serviceDayOrigin('20261101', TZ)).toBe(Date.UTC(2026, 10, 1, 5) / 1000)
    expect(serviceDayOrigin('20261102', TZ)).toBe(Date.UTC(2026, 10, 2, 5) / 1000)
  })
})
