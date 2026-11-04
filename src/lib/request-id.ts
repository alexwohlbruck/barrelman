/**
 * A per-request id, returned as `X-Request-Id` and written beside every
 * server-side error log, so a customer's report of a failed call can be found
 * in the logs without the response having to carry the error itself.
 *
 * An incoming `X-Request-Id` is kept when it is well formed, so a caller (or a
 * proxy in front of barrelman) can trace one id end to end. The shape is
 * restricted because the id is written into log lines: anything with spaces,
 * quotes or control characters is replaced rather than echoed.
 */

const REQUEST_ID = Symbol.for('barrelman.requestId')
const WELL_FORMED = /^[A-Za-z0-9._:-]{8,128}$/

export const REQUEST_ID_HEADER = 'x-request-id'

type Tagged = Record<symbol, unknown>

export function isWellFormedRequestId(value: string | null | undefined): value is string {
  return typeof value === 'string' && WELL_FORMED.test(value)
}

/** The id for this request, assigned on first call and stable thereafter. */
export function requestIdFor(request: Request): string {
  const tagged = request as unknown as Tagged
  const existing = tagged[REQUEST_ID]
  if (typeof existing === 'string') return existing

  const incoming = request.headers.get(REQUEST_ID_HEADER)
  const id = isWellFormedRequestId(incoming) ? incoming : crypto.randomUUID()
  tagged[REQUEST_ID] = id
  return id
}
