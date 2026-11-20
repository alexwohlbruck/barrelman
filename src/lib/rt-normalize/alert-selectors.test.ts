import { describe, expect, it } from 'bun:test'
import type { transit_realtime } from 'gtfs-realtime-bindings'
import { repairAlertSelectors, unresolvableTrip } from './alert-selectors'

const feed = (...informedEntity: transit_realtime.IEntitySelector[]): transit_realtime.IFeedMessage => ({
  header: { gtfsRealtimeVersion: '2.0' },
  entity: [{ id: 'a1', alert: { informedEntity } }],
})

const selectors = (m: transit_realtime.IFeedMessage) => m.entity![0]!.alert!.informedEntity

describe('repairAlertSelectors', () => {
  it('moves a route given as a bare trip descriptor onto the selector', () => {
    const { message, repaired } = repairAlertSelectors(feed({ agencyId: 'A', trip: { routeId: 'M66', directionId: 0 } }))
    expect(repaired).toBe(1)
    expect(selectors(message)).toEqual([{ agencyId: 'A', routeId: 'M66', directionId: 0 }])
  })

  it('keeps a selector route over the trip descriptor one', () => {
    const { message } = repairAlertSelectors(feed({ routeId: 'X', trip: { routeId: 'Y', directionId: 1 } }))
    expect(selectors(message)).toEqual([{ routeId: 'X' }])
  })

  it('drops a selector left naming nothing', () => {
    const { message } = repairAlertSelectors(feed({ trip: { startDate: '20261120' } }, { routeId: 'R' }))
    expect(selectors(message)).toEqual([{ routeId: 'R' }])
  })

  it('leaves trips that resolve alone', () => {
    const input = feed(
      { trip: { tripId: 'T1' } },
      { trip: { routeId: 'R', directionId: 0, startDate: '20261120', startTime: '08:00:00' } },
    )
    const { message, repaired } = repairAlertSelectors(input)
    expect(repaired).toBe(0)
    expect(message).toBe(input)
  })

  it('treats direction 0 as given', () => {
    expect(unresolvableTrip({ trip: { routeId: 'R', directionId: 0, startDate: '20261120', startTime: '08:00:00' } })).toBe(false)
  })
})
