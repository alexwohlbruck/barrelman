/**
 * Test support: a complete, valid `HealthResult`.
 *
 * `checkHealth` now returns a per-dependency and per-endpoint breakdown, which
 * three test files have to stand in for while testing something else entirely
 * (route wiring, auth, the penalty box). Building that literal in each of them
 * would be twenty duplicated lines that drift the moment a field is added — and
 * typing the mocks loosely instead would hide exactly the drift the types are
 * there to catch.
 *
 * Not imported by any production path.
 */
import { CREDIT_COSTS, GROUP_PREFIXES } from '../billing/plans'
import type { DependencyHealth, DependencyKey, HealthResult } from './health.service'

const HEALTHY: DependencyHealth = { status: 'ok', latencyMs: 1 }

const DEPENDS_ON: Record<string, DependencyKey[]> = {
  tiles: ['database', 'martin'],
  search: ['database'],
  geocode: ['database'],
  isochrone: ['graphhopper'],
  transit: ['database', 'motis'],
  routing: ['graphhopper'],
  spatial: ['database'],
  places: ['database'],
}

/** An all-green result. Pass overrides for the fields a given test cares about. */
export function healthFixture(overrides: Partial<HealthResult> = {}): HealthResult {
  return {
    status: 'ok',
    database: 'connected',
    motis: 'ok',
    uptimeSeconds: 42,
    checkedAt: '2026-08-08T00:00:00.000Z',
    dependencies: {
      database: HEALTHY,
      graphhopper: HEALTHY,
      martin: HEALTHY,
      motis: HEALTHY,
      pelias: { ...HEALTHY, optional: true },
    },
    endpoints: GROUP_PREFIXES.map(({ group, prefixes }) => ({
      group,
      paths: prefixes,
      status: 'ok' as const,
      creditCost: CREDIT_COSTS[group],
      dependsOn: DEPENDS_ON[group] ?? ['database'],
    })),
    ...overrides,
  }
}
