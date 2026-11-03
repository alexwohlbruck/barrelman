import { envNumber } from '../config/env'

/** Base URL of the GraphHopper routing engine. */
export function graphHopperUrl(): string {
  return process.env.GRAPHHOPPER_URL || 'http://barrelman-graphhopper:8989'
}

/**
 * How long a single upstream GraphHopper call may run before barrelman gives
 * up on it. GraphHopper has no request deadline of its own, and a map-matching
 * call with a wide `gps_accuracy` or a sparse trace can grind for minutes —
 * without this the caller's request hangs until their client gives up.
 */
export function graphHopperTimeoutMs(): number {
  return envNumber('GRAPHHOPPER_TIMEOUT_MS', 30_000)
}

/**
 * True for the error `fetch` rejects with when an AbortSignal.timeout() fires.
 * Deliberately not AbortError: that is a caller who hung up, not a slow engine.
 */
export function isTimeoutError(err: unknown): boolean {
  return (err as { name?: unknown } | null)?.name === 'TimeoutError'
}
