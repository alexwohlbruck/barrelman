import { describe, expect, it } from 'bun:test'
import { exactCall, indexCalls, refineTime } from './exact-times'

const message = {
  entity: [
    { id: '1', tripUpdate: { trip: { tripId: '114850_5..S28R' }, stopTimeUpdate: [
      { stopId: '640S', arrival: { time: 1794535528 }, departure: { time: 1794535558 } },
      { stopId: '639S', scheduleRelationship: 1 },
    ] } },
    { id: '2', tripUpdate: { trip: { tripId: 'gone', scheduleRelationship: 3 }, stopTimeUpdate: [{ stopId: '640S', departure: { time: 1 } }] } },
  ],
}

describe('exact times', () => {
  const calls = indexCalls(message as any)

  it('finds a run under the trip id MOTIS prefixes with its date, time and feed', () => {
    expect(exactCall(calls, '20261112_19:08_5_114850_5..S28R', '640S')).toEqual({ arrival: 1794535528, departure: 1794535558 })
    expect(exactCall(calls, '20261112_19:08_5_114850_5..S28R', '639S')).toBeUndefined()
    expect(calls.has('gone|640S')).toBe(false)
  })

  it('replaces the floored minute with the agency second', () => {
    expect(refineTime('2026-11-13T02:05:00Z', 1794535558)).toBe(new Date(1794535558 * 1000).toISOString())
  })

  it('keeps MOTIS when the prediction is for another run of the id', () => {
    expect(refineTime('2026-11-14T01:05:00Z', 1794535558)).toBe('2026-11-14T01:05:00Z')
  })
})
