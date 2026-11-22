import { describe, expect, it } from 'bun:test'
import { buildTimetable, parseGtfsTime, serviceMasks, type FeedTimetable } from './timetable'

const allWeek = [true, true, true, true, true, true, true]

describe('serviceMasks', () => {
  // 2026-11-23 is a Monday.
  it('sets the running days of a weekday calendar', () => {
    const masks = serviceMasks(
      [{ serviceId: 'wk', weekdays: [true, true, true, true, true, false, false], start: '20260101', end: '20261231' }],
      [],
      '20261123',
      7,
    )
    expect(masks.get('wk')).toBe(0b0011111)
  })

  it('applies calendar_dates additions and removals, and calendar end dates', () => {
    const masks = serviceMasks(
      [{ serviceId: 'a', weekdays: allWeek, start: '20261101', end: '20261124' }],
      [
        { serviceId: 'a', date: '20261123', type: 2 },
        { serviceId: 'b', date: '20261125', type: 1 },
        { serviceId: 'b', date: '20270101', type: 1 },
      ],
      '20261123',
      4,
    )
    expect(masks.get('a')).toBe(0b0010)
    expect(masks.get('b')).toBe(0b0100)
  })
})

describe('parseGtfsTime', () => {
  it('reads times past midnight', () => {
    expect(parseGtfsTime('25:01:02')).toBe(25 * 3600 + 62)
    expect(parseGtfsTime(' 7:00:00')).toBe(7 * 3600)
    expect(parseGtfsTime('')).toBeNull()
  })
})

function feed(overrides: Partial<FeedTimetable> = {}): FeedTimetable {
  return {
    feedId: '5',
    timezone: 'America/New_York',
    stops: new Map([
      ['P', { name: 'Station', lng: -73.99, lat: 40.75 }],
      ['A', { name: 'Station', lng: -73.99, lat: 40.75, parent: 'P' }],
      ['B', { name: 'Station', lng: -73.9901, lat: 40.7501, parent: 'P' }],
      ['C', { name: 'Uptown', lng: -73.98, lat: 40.76 }],
    ]),
    calls: new Map([
      ['t1', [
        { seq: 2, stopId: 'C', arrival: 3700, departure: 3720 },
        { seq: 1, stopId: 'A', arrival: 3600, departure: 3600 },
      ]],
      ['t2', [
        { seq: 1, stopId: 'A', arrival: 7200, departure: 7200 },
        { seq: 2, stopId: 'C', arrival: null, departure: null },
        { seq: 3, stopId: 'B', arrival: 7400, departure: 7400 },
      ]],
      ['off', [
        { seq: 1, stopId: 'A', arrival: 1, departure: 1 },
        { seq: 2, stopId: 'C', arrival: 2, departure: 2 },
      ]],
    ]),
    trips: new Map([
      ['t1', { routeId: 'R1', serviceId: 'all', headsign: 'Uptown', directionId: 0 }],
      ['t2', { routeId: 'R1', serviceId: 'all', headsign: 'Loop' }],
      ['off', { routeId: 'R1', serviceId: 'never', headsign: 'Never' }],
    ]),
    routes: new Map([['R1', { shortName: '1', longName: 'Broadway', type: 1, color: 'EE352E' }]]),
    calendar: [{ serviceId: 'all', weekdays: allWeek, start: '20260101', end: '20261231' }],
    calendarDates: [],
    transfers: [{ from: 'C', to: 'A', seconds: 300 }],
    ...overrides,
  }
}

describe('buildTimetable', () => {
  const pkg = buildTimetable([feed()], { bounds: [-74, 40.7, -73.9, 40.8], startDate: '20261123', days: 3 })

  it('keeps running trips, grouped into patterns, with delta-encoded times', () => {
    expect(pkg.trips.map(t => t.tripId)).toEqual(['t1', 't2'])
    const t1 = pkg.trips[0]
    expect(t1.days).toBe(0b111)
    expect(t1.times).toEqual([3600, 0, 100, 20])
    const p1 = pkg.patterns[t1.pattern]
    expect(p1).toMatchObject({ route: 0, headsign: 'Uptown', directionId: 0 })
    expect(p1.stops.map(i => pkg.stops[i].stopId)).toEqual(['A', 'C'])
  })

  it('interpolates a call with no time from its neighbours', () => {
    const t2 = pkg.trips[1]
    const absolute = t2.times.reduce<number[]>((acc, d, i) => [...acc, i ? acc[i - 1] + d : d], [])
    expect(absolute).toEqual([7200, 7200, 7300, 7300, 7400, 7400])
    expect(pkg.patterns[t2.pattern].directionId).toBeUndefined()
  })

  it('links platforms to their station and adds transfers inside it', () => {
    const index = (id: string) => pkg.stops.findIndex(s => s.stopId === id)
    expect(pkg.stops[index('A')].parent).toBe(index('P'))
    expect(pkg.transfers).toContainEqual([index('C'), index('A'), 300])
    const inStation = pkg.transfers.find(([a, b]) => a === index('A') && b === index('B'))
    expect(inStation?.[2]).toBe(120)
  })

  it('carries the feed time zone and the route', () => {
    expect(pkg.feeds).toEqual([{ id: '5', timezone: 'America/New_York' }])
    expect(pkg.routes).toEqual([{ feed: 0, routeId: 'R1', shortName: '1', longName: 'Broadway', type: 1, color: 'EE352E' }])
  })
})
