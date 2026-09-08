/**
 * Which runs on a departure board a skip alert disowns.
 *
 * The boards read MOTIS — the schedule plus whatever realtime reached it —
 * and a planned skip often never does: a station closed for a parade until
 * 9pm goes on listing 2s "in 5 minutes". The agency's alert is the word
 * that outranks the schedule, and dropping the runs HERE cleans every
 * consumer at once — the departures widget, transfer boards, and anything
 * else that reads a board.
 *
 * Each run is judged at ITS OWN time against the alert's windows, not the
 * current one, so the trains that resume after the window stay on the
 * board — their times are what tell a rider when the station reopens.
 */

/** Effects that assert the named stops are NOT being served. */
const SKIP_EFFECTS = new Set(['NO_SERVICE', 'DETOUR'])

interface SkipAlert {
  effect: string
  activePeriods: Array<{ start?: string; end?: string }>
  informedEntities: Array<{ routeId?: string; stopId?: string }>
}

interface Run {
  route: { id: string }
  departureTime?: string
  arrivalTime?: string
}

function coversInstant(alert: SkipAlert, at: number): boolean {
  const periods = alert.activePeriods ?? []
  if (!periods.length) return true // no window given: in effect until lifted
  return periods.some((p) => {
    const start = p.start ? Date.parse(p.start) : null
    const end = p.end ? Date.parse(p.end) : null
    return (start === null || start <= at) && (end === null || end >= at)
  })
}

export function dropSkippedRuns<T extends Run>(
  departures: T[],
  alerts: SkipAlert[],
  stopIds: Iterable<string>,
): T[] {
  const ids = new Set(stopIds)
  const skips = alerts.filter(
    (a) =>
      SKIP_EFFECTS.has(a.effect) &&
      (a.informedEntities ?? []).some(
        (e) => e.routeId && e.stopId && ids.has(e.stopId),
      ),
  )
  if (!ids.size || !skips.length) return departures

  return departures.filter((d) => {
    const at = Date.parse(d.departureTime || d.arrivalTime || '')
    if (Number.isNaN(at)) return true
    return !skips.some(
      (a) =>
        coversInstant(a, at) &&
        (a.informedEntities ?? []).some(
          (e) => e.routeId === d.route.id && e.stopId && ids.has(e.stopId),
        ),
    )
  })
}
