import Elysia from 'elysia'
import { identifyCaller } from '../middleware/api-auth'
import { checkHealth as _checkHealth } from '../services/health.service'

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
        const { caller, error } = await identifyCaller(headers, request)
        if (error) {
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
