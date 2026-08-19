import Elysia from 'elysia'
import { clientIp, identifyCaller } from '../middleware/api-auth'
import { checkHealth as _checkHealth, redactHealth } from '../services/health.service'
import { checkPenalty, penaltyKeyFor, recordRejection } from '../services/throttle.service'

export function createHealthRoutes(deps = { checkHealth: _checkHealth }) {
  return new Elysia({ prefix: '/health' })
    // Redacted: the per-dependency `message` is a raw upstream error and can
    // carry internal hostnames and ports. Everything a caller needs — each
    // endpoint group's status and why — is authored by us and survives.
    .get('/', async () => redactHealth(await deps.checkHealth()), {
      detail: {
        summary: 'Public health check',
        description:
          'Liveness, backing-service reachability, and the resulting status of every API endpoint group — so a caller can tell "transit is down" from "the whole instance is down". No auth required; safe for load-balancer probes. Probe results are cached for a few seconds, and `checkedAt` says how fresh they are.',
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
        // Unredacted: a caller who authenticated gets the upstream error text,
        // which is what makes this useful for debugging a failing integration.
        return { ...(await deps.checkHealth()), authenticated: true, caller: caller.kind, plan: caller.plan ?? null }
      },
      {
        detail: {
          summary: 'Authenticated health check',
          description:
            'Same as /health but requires a valid credential — an account API key or the shared service key. Use it to check that a key works without spending credits. Unlike the public route it includes the underlying error for any unreachable service.',
          tags: ['Health'],
        },
      },
    )
}

export const healthRoutes = createHealthRoutes()
