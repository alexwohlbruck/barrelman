/**
 * Request id and the last-resort error handler, for every route.
 *
 * Mounted first in index.ts. `onRequest` runs before routing, so every
 * response carries `X-Request-Id`, refusals and 404s included. `onError` is
 * global so it also sees errors thrown inside route plugins.
 *
 * Two things it changes about Elysia's defaults:
 *
 *  - An uncaught exception answers `{ error, requestId }` with a generic
 *    message. Elysia's default body is the exception's message, which can name
 *    internal hosts, SQL or file paths.
 *  - Elysia validates the request before `onBeforeHandle`, so a guarded route
 *    answered a caller with no credential by describing its schema in a 422.
 *    On a validation error for a guarded path, the credential is checked first,
 *    and a caller who would have been refused gets that refusal instead.
 */
import Elysia from 'elysia'
import { groupForPath } from '../billing/plans'
import { identifyCaller } from './api-auth'
import { adminAuthHandler } from './auth'
import { REQUEST_ID_HEADER, requestIdFor } from '../lib/request-id'
import { serverError } from '../lib/errors'

export interface Refusal {
  status: number
  body: unknown
}

/** The answer a guarded path's own guard would have given, or null to proceed. */
export type RefuseUnauthenticated = (request: Request) => Promise<Refusal | null>

export const refuseUnauthenticated: RefuseUnauthenticated = async (request) => {
  const path = new URL(request.url).pathname.toLowerCase()
  const headers = Object.fromEntries(request.headers) as Record<string, string | undefined>

  if (path === '/admin' || path.startsWith('/admin/')) {
    const set: { status?: number | string } = {}
    const body = await adminAuthHandler({ headers, request, set })
    return body ? { status: Number(set.status ?? 401), body } : null
  }

  if (groupForPath(path)) {
    const { error } = await identifyCaller(headers, request)
    return error ? { status: error.status, body: error.body } : null
  }

  return null
}

export function createRequestContext(deps: { refuseUnauthenticated?: RefuseUnauthenticated } = {}) {
  const refuse = deps.refuseUnauthenticated ?? refuseUnauthenticated

  return new Elysia({ name: 'request-context' })
    .onRequest(({ request, set }) => {
      set.headers[REQUEST_ID_HEADER] = requestIdFor(request)
    })
    .onError({ as: 'global' }, async ({ code, error, request, set }) => {
      set.headers[REQUEST_ID_HEADER] = requestIdFor(request)

      if (code === 'VALIDATION') {
        let refusal: Refusal | null = null
        try {
          refusal = await refuse(request)
        } catch (err) {
          return serverError(request, set, err, 'Internal server error')
        }
        if (refusal) {
          set.status = refusal.status
          return refusal.body
        }
        // An identified caller keeps Elysia's detailed validation message.
        return
      }

      if (code === 'UNKNOWN' || code === 'INTERNAL_SERVER_ERROR') {
        return serverError(request, set, error, 'Internal server error')
      }
    })
}

export const requestContext = createRequestContext()
