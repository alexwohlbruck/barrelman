import { describe, expect, test } from 'bun:test'
import { relocateTransfer, spliceLegs, type Ride, type StopPlace, type Trip } from './transfer-relocation'

const t = (hhmm: string) => `2026-11-08T${hhmm}:00.000Z`
const stop = (stopId: string, lng: number, time: string): StopPlace =>
  ({ name: stopId, stopId, lat: 40.74, lng, arrival: t(time), departure: t(time) })

// The G runs Greenpoint Av -> 21 St -> Court Sq; the 7 runs Vernon Blvd ->
// Hunters Point Av -> Court Sq -> Queensboro Plaza -> 74 St.
const G: Trip = {
  stops: [stop('G33N', -73.95, '19:55'), stop('G26N', -73.954, '20:07'), stop('G24N', -73.9497, '20:10'), stop('G22N', -73.9438, '20:12')],
  shape: [[-73.95, 40.74], [-73.954, 40.74], [-73.9497, 40.74], [-73.9438, 40.74]],
}
const SEVEN: Trip = {
  stops: [stop('721N', -73.953, '20:16'), stop('720N', -73.9489, '20:18'), stop('719N', -73.9452, '20:20'), stop('718N', -73.94, '20:22'), stop('710N', -73.89, '20:32')],
}

const leg = (trip: Trip, board: number, alight: number, mode = 'SUBWAY'): Ride => ({
  mode,
  from: trip.stops[board],
  to: trip.stops[alight],
  startTime: trip.stops[board].departure!,
  endTime: trip.stops[alight].arrival!,
  duration: 0,
  distance: 0,
  transitLeg: true,
})

const COURT_SQ = (a: string, b: string) => (a === 'G22N' && b === '719N' ? 180 : null)

describe('relocateTransfer', () => {
  const g = leg(G, 0, 2), seven = leg(SEVEN, 1, 4)

  test('moves the walk from 21 St to the in-system change at Court Sq', () => {
    const [before, walk, after] = relocateTransfer(g, seven, G, SEVEN, COURT_SQ)!
    expect(before.to.stopId).toBe('G22N')
    expect(before.endTime).toBe(t('20:12'))
    expect(before.intermediateStops!.map((s) => s.stopId)).toEqual(['G26N', 'G24N'])
    expect(walk.from.stopId).toBe('G22N')
    expect(walk.to.stopId).toBe('719N')
    expect(walk.duration).toBe(180)
    expect(after.from.stopId).toBe('719N')
    expect(after.startTime).toBe(t('20:20'))
    expect(after.to.stopId).toBe('710N')
    expect(after.intermediateStops!.map((s) => s.stopId)).toEqual(['718N'])
  })

  test('keeps both trains and the arrival', () => {
    const [before, , after] = relocateTransfer(g, seven, G, SEVEN, COURT_SQ)!
    expect(before.startTime).toBe(g.startTime)
    expect(after.endTime).toBe(seven.endTime)
  })

  test('slices the first train\'s shape to its new stop', () => {
    const [before] = relocateTransfer(g, seven, G, SEVEN, COURT_SQ)!
    expect(before.geometry!.coordinates).toEqual(G.shape!)
  })

  test('counts the router\'s margin in the change', () => {
    // 20:12 + 180 s + 360 s misses the 20:20.
    expect(relocateTransfer(g, seven, G, SEVEN, COURT_SQ, 360)).toBeNull()
    expect(relocateTransfer(g, seven, G, SEVEN, COURT_SQ, 120)).not.toBeNull()
  })

  test('leaves the plan alone when no filed change makes the connection', () => {
    expect(relocateTransfer(g, seven, G, SEVEN, () => null)).toBeNull()
    expect(relocateTransfer(g, seven, G, SEVEN, (a, b) => (a === 'G22N' && b === '719N' ? 600 : null))).toBeNull()
  })

  test('never changes before boarding the first train or after leaving the second', () => {
    const early = (a: string, b: string) => (a === 'G33N' || b === '710N' ? 0 : null)
    expect(relocateTransfer(g, seven, G, SEVEN, early)).toBeNull()
  })

  test('takes the shortest filed change when several connect', () => {
    const both = (a: string, b: string) =>
      a === 'G22N' && b === '719N' ? 180 : a === 'G26N' && b === '720N' ? 400 : null
    const [before] = relocateTransfer(g, seven, G, SEVEN, both)!
    expect(before.to.stopId).toBe('G22N')
  })
})

describe('spliceLegs', () => {
  test('moves the totals by what was swapped', () => {
    const walk = (duration: number, distance: number): Ride =>
      ({ ...leg(G, 2, 2), mode: 'WALK', transitLeg: false, duration, distance })
    const ride = (duration: number): Ride => ({ ...leg(G, 0, 2), duration })
    const it = { walkTime: 505, walkDistance: 700, transitTime: 1500, waitingTime: 300, legs: [walk(300, 400), ride(900), walk(205, 300), ride(600)] }

    spliceLegs(it, 1, 3, [ride(1020), walk(180, 100), ride(720)])

    expect(it.legs).toHaveLength(4)
    expect(it.walkTime).toBe(480)
    expect(it.walkDistance).toBe(500)
    expect(it.transitTime).toBe(1740)
    expect(it.waitingTime).toBe(85)
  })
})
