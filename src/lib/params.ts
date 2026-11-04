/**
 * Bounds for caller-supplied sizes that reach SQL or an upstream engine.
 *
 * Most query parameters arrive as strings, and `Number()` on them accepts
 * anything from `-1` to `1e9`. A `limit` or `radius` with no ceiling turns one
 * metered request into a table scan, so each is clamped to a range generous
 * enough for every real client (Parchment's largest calls included) and
 * silently: a client asking for too much gets the most we serve, not an error.
 */

/**
 * `raw` as a number within [min, max], or `fallback` when it is missing or not
 * a number. The fallback is returned unclamped, so a service default stays the
 * service's decision.
 */
export function clampNumber(
  raw: unknown,
  { min, max, fallback }: { min: number; max: number; fallback?: number },
): number | undefined {
  if (raw == null || raw === '') return fallback
  const n = Number(raw)
  if (!Number.isFinite(n)) return fallback
  return Math.min(max, Math.max(min, n))
}

/** As `clampNumber`, truncated to an integer. */
export function clampInt(
  raw: unknown,
  bounds: { min: number; max: number; fallback?: number },
): number | undefined {
  const n = clampNumber(raw, bounds)
  return n === undefined ? undefined : Math.trunc(n)
}

/**
 * Shared ceilings, so the same parameter means the same thing across routes.
 * Sized from the largest values Parchment sends: up to 750 departures for a
 * day-long board (departure-board.ts), a 100 km search widen, 60-row search
 * pages.
 */
export const LIMITS = {
  /** Rows in one list response. */
  maxRows: 200,
  /** Pagination depth. Past this, narrow the query instead. */
  maxOffset: 10_000,
  /** A "nearby" radius for stops, stations and reverse geocoding, in metres. */
  maxNearbyRadiusM: 10_000,
  /** A search radius, in metres. */
  maxSearchRadiusM: 500_000,
  /** A route-corridor buffer, in metres. */
  maxCorridorBufferM: 50_000,
  /** Ids in one comma-separated list parameter. */
  maxListItems: 500,
  /** Departure events on one board. */
  maxDepartures: 1_000,
  /** A departure board's time window, in minutes (one day). */
  maxWindowMinutes: 1_440,
} as const
