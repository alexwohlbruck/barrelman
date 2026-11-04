import { requestIdFor } from './request-id'

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
  set.status = status
  return { error: message, requestId }
}
