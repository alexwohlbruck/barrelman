/**
 * The pure parts of placing the fleet: which station a train has just left,
 * how long one hop takes, and which source wins for a given train.
 */

import { describe, it, test, expect } from 'bun:test'
import { placeTrain, stopBefore, segmentSeconds } from './subway-position'

// The 4, both ways, as trip patterns store them (parent station ids).
const SOUTH = ['415', '416', '418', '419', '420', '423']
const NORTH = ['423', '420', '419', '418', '416', '415']

describe('stopBefore', () => {
  it('takes the station before the one being approached', () => {
    expect(stopBefore([SOUTH], '419', '420')).toBe('418')
  })

  it('uses the following stop to pick the direction', () => {
    // 419 sits in both patterns; only the next stop says which way the
    // train is going, and the answer differs.
    expect(stopBefore([SOUTH, NORTH], '419', '420')).toBe('418')
    expect(stopBefore([SOUTH, NORTH], '419', '418')).toBe('420')
  })

  it('has no answer at a terminal, or for a stop off the pattern', () => {
    expect(stopBefore([SOUTH], '415', '416')).toBeNull()
    expect(stopBefore([SOUTH], '999', null)).toBeNull()
  })

  it('falls back to the busiest pattern when nothing follows', () => {
    // Patterns arrive ordered by trip count, so the first match is the
    // one most trains run.
    expect(stopBefore([SOUTH, NORTH], '419', null)).toBe('418')
  })
})

describe('segmentSeconds', () => {
  const stu = (t: number) => ({ arrival: { time: t } })

  it('measures the gap between the next two arrivals', () => {
    expect(segmentSeconds([stu(1000), stu(1120)])).toBe(120)
  })

  it('rejects a gap that cannot be one hop', () => {
    expect(segmentSeconds([stu(1000), stu(1000)])).toBeNull()   // no time
    expect(segmentSeconds([stu(1000), stu(9000)])).toBeNull()   // a layover
    expect(segmentSeconds([stu(1000)])).toBeNull()              // last stop
  })

  it('ignores stops with no arrival time', () => {
    expect(segmentSeconds([{}, stu(1000), stu(1090)])).toBe(90)
  })
})

describe('placeTrain', () => {
  const NOW = 1_800_000_000
  const stops = new Map([
    ['128S', { lat: 40.7506, lng: -73.991, name: '34 St-Penn Station' }],
    ['129S', { lat: 40.7472, lng: -73.9935, name: '28 St' }],
    ['130S', { lat: 40.7441, lng: -73.9957, name: '23 St' }],
  ])
  const patterns = new Map([['1', [['127', '128', '129', '130']]]])
  const call = (stopId: string, offset: number) => ({ stopId, arrival: { time: NOW + offset }, departure: { time: NOW + offset } })
  const trip = (...stopTimeUpdate: any[]) => ({ trip: { tripId: 't', routeId: '1' }, stopTimeUpdate })
  const place = (tu: any, vehicle?: any) => {
    const p = placeTrain(tu, vehicle, stops, patterns, NOW)
    return p && { lat: p.lat, lng: p.lng }
  }
  const at = (id: string) => ({ lat: stops.get(id)!.lat, lng: stops.get(id)!.lng })

  test('a stopped train stays at the platform the feed names', () => {
    // MTA predicts no dwell: the stop it is standing at reads as already departed.
    expect(place(trip(call('128S', -30), call('129S', 60)), { stopId: '128S', currentStatus: 1 })).toEqual(at('128S'))
  })

  test('a stopped train whose stop was pruned is not moved on to the next one', () => {
    expect(place(trip(call('129S', 60), call('130S', 150)), { stopId: '128S', currentStatus: 1 })).toEqual(at('128S'))
  })

  test('a train under way is placed between the stations', () => {
    const p = place(trip(call('128S', -45), call('129S', 45)), { stopId: '129S', currentStatus: 2 })!
    expect(p.lat).toBeCloseTo((at('128S').lat + at('129S').lat) / 2, 4)
  })

  test('a train with no report, about to leave its first stop, waits there', () => {
    expect(place(trip(call('128S', 60), call('129S', 150)))).toEqual(at('128S'))
  })
})
