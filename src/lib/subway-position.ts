/**
 * Reading a subway train's place in its route from the pieces a realtime
 * feed gives: the stop order it is running, and its own timings.
 *
 * Pure, and kept out of the service so a test that stubs the service does
 * not take them with it.
 */

/** GTFS-RT timestamps arrive as numbers, longs, or {low, high} pairs. */
export function toSeconds(ts: any): number {
  if (typeof ts === 'number') return ts
  if (ts && typeof ts.toNumber === 'function') return ts.toNumber()
  if (ts && typeof ts.low === 'number') return ts.low + (ts.high || 0) * 0x100000000
  return NaN
}

/**
 * The station before `next` on a pattern this trip is actually running.
 *
 * `after` — the stop the trip reaches AFTER `next` — picks the direction:
 * every route's stops appear in both patterns, so without it a
 * Woodlawn-bound train borrows the southbound predecessor and jumps a
 * station backwards.
 */
export function stopBefore(
  patterns: string[][],
  next: string,
  after: string | null,
): string | null {
  for (const p of patterns) {
    const i = p.indexOf(next)
    if (i <= 0) continue
    if (after && p[i + 1] !== after) continue
    return p[i - 1]
  }
  return null
}

/** A typical station-to-station time for this trip, from its own next two
 *  arrivals. */
export function segmentSeconds(stus: any[]): number | null {
  const times = stus
    .map((s: any) => toSeconds(s.arrival?.time))
    .filter((n: number | null): n is number => !!n)
  if (times.length < 2) return null
  const gap = times[1] - times[0]
  return gap > 0 && gap < 900 ? gap : null
}
