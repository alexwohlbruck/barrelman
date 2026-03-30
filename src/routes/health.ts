import Elysia from 'elysia'
import { checkHealth as _checkHealth } from '../services/health.service'

export function createHealthRoutes(deps = { checkHealth: _checkHealth }) {
  return new Elysia({ prefix: '/health' }).get('/', deps.checkHealth)
}

export const healthRoutes = createHealthRoutes()
