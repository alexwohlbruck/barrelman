import Elysia from 'elysia'
import { clientIp, identifyCaller } from '../middleware/api-auth'
import { checkHealth as _checkHealth } from '../services/health.service'
import { checkPenalty, penaltyKeyFor, recordRejection } from '../services/throttle.service'

export function createHealthRoutes(deps = { checkHealth: _checkHealth }) {
  return new Elysia({ prefix: '/health' })
    .get('/', deps.checkHealth, {
      detail: {
        summary: 'Public health check',
        description:
          'Liveness + database connectivity. No auth required — safe for load-balancer probes.',
        tags: ['Health'],
      },
    })
    .get(
      '/auth',
      async ({ headers, request, set }) => {
        // Accepts any valid credential — a customer's own key as well as the
        // shared service secret — so an integration can verify its key works
        // without spending credits on a real endpoint.
        //
        // "Without spending credits" is the whole point of the route, and it is
        // also what makes it the cheapest place to test whether a key is real.
        // So it carries the penalty box even though it carries no metering:
        // this calls identifyCaller directly rather than going through
        // apiAuth(), and without these two lines a caller refused everywhere
        // else could still sit here confirming stolen keys at full rate, with
        // no strikes recorded and nothing for abuse detection to see.
        //
        // Keyed on the address, not the account: every refusal identifyCaller
        // can return is an unidentified caller, so there is no account to
        // attribute a strike to. That is the same key apiAuth() uses for the
        // same callers, so strikes earned here and elsewhere accumulate together.
        const penaltyKey = penaltyKeyFor(clientIp(request))

        const boxed = checkPenalty(penaltyKey)
        if (!boxed.allowed) {
          set.status = 429
          set.headers['retry-after'] = String(boxed.retryAfterSeconds)
          return { error: boxed.message, layer: boxed.layer }
        }

        const { caller, error } = await identifyCaller(headers, request)
        if (error) {
          recordRejection(penaltyKey)
          set.status = error.status
          return error.body
        }
        return { ...(await deps.checkHealth()), authenticated: true, caller: caller.kind, plan: caller.plan ?? null }
      },
      {
        detail: {
          summary: 'Authenticated health check',
          description:
            'Same as /health but requires a valid credential — an account API key or the shared service key. Use it to check that a key works without spending credits.',
          tags: ['Health'],
        },
      },
    )
}

export const healthRoutes = createHealthRoutes()
