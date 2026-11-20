import type { transit_realtime } from 'gtfs-realtime-bindings'

type Selector = transit_realtime.IEntitySelector

/**
 * A trip descriptor without a trip_id names one trip only together with its
 * route, direction, start date and start time. One that lacks those is a
 * route given in the wrong field, which MOTIS rejects with the whole feed.
 */
export function unresolvableTrip(selector: Selector): boolean {
  const trip = selector.trip
  return !!trip && !trip.tripId && !(trip.routeId && trip.directionId != null && trip.startDate && trip.startTime)
}

/** The selector such a trip descriptor means: its route and direction, on the selector itself. */
function lifted({ trip, ...rest }: Selector): Selector | null {
  const out: Selector = { ...rest }
  if (out.routeId == null && trip?.routeId) out.routeId = trip.routeId
  if (out.directionId == null && trip?.directionId != null && out.routeId === trip.routeId) out.directionId = trip.directionId
  const names = out.agencyId || out.routeId || out.routeType != null || out.stopId
  return names ? out : null
}

/** The feed with every unresolvable trip descriptor in its alerts turned into the selector it means. */
export function repairAlertSelectors(message: transit_realtime.IFeedMessage): { message: transit_realtime.IFeedMessage; repaired: number } {
  let repaired = 0
  const entity = (message.entity ?? []).map(e => {
    const selectors = e.alert?.informedEntity
    if (!selectors?.some(unresolvableTrip)) return e
    const informedEntity = selectors.flatMap(s => {
      if (!unresolvableTrip(s)) return [s]
      repaired++
      return lifted(s) ?? []
    })
    return { ...e, alert: { ...e.alert, informedEntity } }
  })
  return { message: repaired ? { ...message, entity } : message, repaired }
}
