import Elysia from 'elysia'
import { authHandler } from '../middleware/auth'
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
      async (ctx) => {
        const unauthorized = authHandler(ctx)
        if (unauthorized) return unauthorized
        return { ...(await deps.checkHealth()), authenticated: true }
      },
      {
        detail: {
          summary: 'Authenticated health check',
          description:
            'Same as /health but requires a valid BARRELMAN_API_KEY bearer token. Use this to verify both reachability and API-key correctness from a client integration.',
          tags: ['Health'],
        },
      },
    )
}

export const healthRoutes = createHealthRoutes()
