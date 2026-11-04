/**
 * Reports handler errors that answer 5xx to error tracking.
 *
 * A global onError hook that returns nothing, so the response is left to
 * requestContext and to whatever a route does with its errors. It has to be
 * mounted before requestContext: Elysia stops at the first onError hook that
 * returns a value. Errors a route catches and answers with serverError() never
 * reach onError; serverError() reports those itself. 4xx outcomes
 * (validation, not found, a status a handler threw on purpose) are the
 * caller's problem and are not reported. With SENTRY_DSN unset this is one
 * function call that returns immediately.
 */
import Elysia from 'elysia'
import { captureError } from '../lib/error-tracking'
import { requestIdFor } from '../lib/request-id'

const CLIENT_ERROR_CODES = new Set(['NOT_FOUND', 'VALIDATION', 'PARSE', 'INVALID_COOKIE_SIGNATURE', 'INVALID_FILE_TYPE'])

/** The status an error will be answered with. Exported for tests. */
export function statusForError(code: string | number, error: unknown): number {
  if (typeof code === 'number') return code
  const status = (error as { status?: unknown } | null)?.status
  if (typeof status === 'number') return status
  return CLIENT_ERROR_CODES.has(code) ? 400 : 500
}

export const errorTracking = new Elysia({ name: 'error-tracking' }).onError(
  { as: 'global' },
  ({ code, error, request, route }) => {
    if (statusForError(code, error) < 500) return
    captureError(error, {
      tags: { method: request.method, route: route ?? 'unknown', requestId: requestIdFor(request) },
    })
  },
)
