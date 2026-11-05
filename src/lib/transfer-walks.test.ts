import { describe, expect, test } from 'bun:test'
import { settleTransferWalks, type TimedItinerary, type TimedLeg } from './transfer-walks'

const at = (hms: string) => `2026-11-05T${hms}.000Z`
const ride = (start: string, end: string, transfer?: TimedLeg['transfer']): TimedLeg => ({
  transitLeg: true, startTime: at(start), endTime: at(end),
  duration: (Date.parse(at(end)) - Date.parse(at(start))) / 1000, distance: 0, to: {}, transfer,
})
const walk = (start: string, end: string): TimedLeg => ({
  transitLeg: false, startTime: at(start), endTime: at(end),
  duration: (Date.parse(at(end)) - Date.parse(at(start))) / 1000, distance: 148, to: { arrival: at(end) },
})
const itinerary = (legs: TimedLeg[]): TimedItinerary => ({ walkTime: 600, waitingTime: 180, walkDistance: 500, legs })

describe('settleTransferWalks', () => {
  test('takes the per-interchange pad out of a transfer walk and counts it as waiting', () => {
    const it = itinerary([ride('19:05:00', '19:10:00'), walk('19:10:00', '19:15:00'), ride('19:18:00', '19:26:00')])
    settleTransferWalks(it, 180)
    expect(it.legs[1].duration).toBe(120)
    expect(it.legs[1].endTime).toBe(at('19:12:00'))
    expect(it.legs[1].to.arrival).toBe(at('19:12:00'))
    expect(it.walkTime).toBe(420)
    expect(it.waitingTime).toBe(360)
  })

  test('times the walk from the station path when the transfer has one', () => {
    const it = itinerary([ride('19:05:00', '19:10:00'), walk('19:10:00', '19:15:00'), ride('19:18:00', '19:26:00', { seconds: 82 })])
    settleTransferWalks(it, 180)
    expect(it.legs[1].duration).toBe(82)
    expect(it.legs[1].endTime).toBe(at('19:11:22'))
  })

  test('measures the walk along a walked station path, not an estimate', () => {
    const walked = itinerary([ride('19:05:00', '19:10:00'), walk('19:10:00', '19:15:00'), ride('19:18:00', '19:26:00', { seconds: 82, walk: { metres: 70, estimate: false } })])
    settleTransferWalks(walked, 180)
    expect(walked.legs[1].distance).toBe(70)
    expect(walked.walkDistance).toBe(422)
    const guessed = itinerary([ride('19:05:00', '19:10:00'), walk('19:10:00', '19:15:00'), ride('19:18:00', '19:26:00', { seconds: 82, walk: { metres: 1, estimate: true } })])
    settleTransferWalks(guessed, 180)
    expect(guessed.legs[1].distance).toBe(148)
  })

  test('never lets the walk run past the next departure', () => {
    const it = itinerary([ride('19:05:00', '19:10:00'), walk('19:10:00', '19:12:00'), ride('19:12:00', '19:20:00', { seconds: 200 })])
    settleTransferWalks(it, 0)
    expect(it.legs[1].duration).toBe(120)
  })

  test('leaves access and egress walks alone', () => {
    const it = itinerary([walk('19:00:00', '19:05:00'), ride('19:05:00', '19:10:00'), walk('19:10:00', '19:15:00')])
    settleTransferWalks(it, 180)
    expect(it.legs.map((l) => l.duration)).toEqual([300, 300, 300])
    expect(it.walkTime).toBe(600)
  })
})
