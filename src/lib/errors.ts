import { requestIdFor } from './request-id'
import { captureError } from './error-tracking'

export interface ServerErrorBody {
  error: string
  requestId: string
}

/**
 * Answer a failed request with a generic message and log the real error.
 *
 * Exception messages name internal hosts, SQL, file paths and upstream
 * responses, none of which a client should read. The request id ties the
 * generic answer to the full log line. Use it for 5xx only: a 4xx message the
 * caller can act on (a bad parameter, a conflict) should still be returned.
 */
export function serverError(
  request: Request,
  set: { status?: number | string },
  err: unknown,
  message: string,
  status = 500,
): ServerErrorBody {
  const requestId = requestIdFor(request)
  const { pathname } = new URL(request.url)
  console.error(`[error] ${requestId} ${request.method} ${pathname}: ${message}`, err)
  // Routes that catch their own errors answer here and never reach onError,
  // so this is the only place those are reported. A no-op without SENTRY_DSN.
  if (status >= 500) captureError(err, { tags: { method: request.method, path: pathname, requestId } })
  set.status = status
  return { error: message, requestId }
}
