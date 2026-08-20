import { describe, test, expect, mock, beforeEach } from 'bun:test'

// ── Mocks ─────────────────────────────────────────────────────────────────────

const mockExecute = mock(async () => [{ '?column?': 1 }] as any[])

mock.module('../db', () => ({ db: { execute: mockExecute } }))

// Dynamic import ensures mocks are registered before the module loads
const { checkHealth, redactHealth, resetHealthCache } = await import('./health.service')

// Mock MOTIS health check — returns ok by default
const mockCheckMotisHealth = mock(async () => ({ status: 'ok' as const, message: undefined as string | undefined }))

/**
 * Every HTTP probe (GraphHopper, Martin, Pelias) goes through this. Default is
 * a healthy 200; individual tests fail specific hosts by URL.
 */
const okResponse = () => new Response('ok', { status: 200 })
let failingHosts: string[] = []

const mockFetch = mock(async (url: string | URL | Request) => {
  const href = String(url)
  if (failingHosts.some((host) => href.includes(host))) throw new Error(`connect ECONNREFUSED ${href}`)
  return okResponse()
}) as unknown as typeof fetch

const deps = () => ({ checkMotisHealth: mockCheckMotisHealth, fetchFn: mockFetch })

// ── Setup ─────────────────────────────────────────────────────────────────────

beforeEach(() => {
  // The probe result is cached across calls by design, so every test starts
  // from a cold cache — otherwise a test would assert against the *previous*
  // test's dependency states.
  resetHealthCache()
  mockExecute.mockReset()
  mockExecute.mockImplementation(async () => [{ '?column?': 1 }])
  mockCheckMotisHealth.mockReset()
  mockCheckMotisHealth.mockImplementation(async () => ({ status: 'ok' as const, message: undefined }))
  failingHosts = []
})

const endpoint = (result: Awaited<ReturnType<typeof checkHealth>>, group: string) =>
  result.endpoints.find((e) => e.group === group)!

// ── Tests ─────────────────────────────────────────────────────────────────────

describe('checkHealth — overall status', () => {
  test('returns ok/connected when every dependency is reachable', async () => {
    const result = await checkHealth(deps())
    expect(result.status).toBe('ok')
    expect(result.database).toBe('connected')
    expect(result.motis).toBe('ok')
  })

  test('returns error/disconnected when the DB throws', async () => {
    mockExecute.mockImplementation(async () => {
      throw new Error('Connection refused')
    })
    const result = await checkHealth(deps())
    expect(result.status).toBe('error')
    expect(result.database).toBe('disconnected')
  })

  test('returns degraded when the DB is up but MOTIS is down', async () => {
    mockCheckMotisHealth.mockImplementation(async () => ({ status: 'unavailable' as const, message: 'Connection refused' }))
    const result = await checkHealth(deps())
    expect(result.status).toBe('degraded')
    expect(result.database).toBe('connected')
    expect(result.motis).toBe('unavailable')
  })

  test('a required service other than MOTIS also degrades the instance', async () => {
    failingHosts = ['8989'] // GraphHopper
    const result = await checkHealth(deps())
    expect(result.status).toBe('degraded')
    expect(result.dependencies.graphhopper.status).toBe('unavailable')
  })

  test('an optional service being down never degrades the instance', async () => {
    // Pelias runs under a compose profile; plenty of instances never start it.
    // A self-hoster who chose not to run it must not see a permanently
    // degraded instance.
    failingHosts = ['pelias']
    const result = await checkHealth(deps())
    expect(result.status).toBe('ok')
    expect(result.dependencies.pelias.status).toBe('unavailable')
    expect(result.dependencies.pelias.optional).toBe(true)
  })

  test('does not rethrow DB errors — always returns a result', async () => {
    mockExecute.mockImplementation(async () => {
      throw new Error('timeout')
    })
    await expect(checkHealth(deps())).resolves.toBeDefined()
  })
})

describe('checkHealth — per-endpoint status', () => {
  test('reports every billing group, with its paths and credit cost', async () => {
    const result = await checkHealth(deps())
    const groups = result.endpoints.map((e) => e.group)
    expect(groups).toEqual(
      expect.arrayContaining(['tiles', 'search', 'geocode', 'isochrone', 'transit', 'routing', 'spatial', 'places']),
    )
    expect(endpoint(result, 'search').paths).toContain('/search')
    expect(endpoint(result, 'tiles').creditCost).toBe(1)
    expect(result.endpoints.every((e) => e.status === 'ok')).toBe(true)
  })

  test('a group is unavailable when a service it requires is down', async () => {
    mockCheckMotisHealth.mockImplementation(async () => ({ status: 'unavailable' as const, message: 'refused' }))
    const result = await checkHealth(deps())

    expect(endpoint(result, 'transit').status).toBe('unavailable')
    expect(endpoint(result, 'transit').message).toMatch(/motis/)
    // The point of the breakdown: MOTIS being down says nothing about search.
    expect(endpoint(result, 'search').status).toBe('ok')
    expect(endpoint(result, 'places').status).toBe('ok')
  })

  test('a group is only degraded when a service that merely enhances it is down', async () => {
    failingHosts = ['pelias']
    const result = await checkHealth(deps())

    // /search still returns POIs from PostGIS; it only loses street addresses.
    expect(endpoint(result, 'search').status).toBe('degraded')
    expect(endpoint(result, 'search').message).toMatch(/addresses/i)
    expect(endpoint(result, 'geocode').status).toBe('degraded')
    expect(endpoint(result, 'spatial').status).toBe('ok')
  })

  test('MOTIS down degrades isochrone but disables transit', async () => {
    // /isochrone reaches MOTIS only for mode=transit, so walk/bike/car survive.
    mockCheckMotisHealth.mockImplementation(async () => ({ status: 'unavailable' as const, message: undefined }))
    const result = await checkHealth(deps())

    expect(endpoint(result, 'isochrone').status).toBe('degraded')
    expect(endpoint(result, 'transit').status).toBe('unavailable')
  })

  test('GraphHopper down disables routing without touching database-only groups', async () => {
    failingHosts = ['8989']
    const result = await checkHealth(deps())

    expect(endpoint(result, 'routing').status).toBe('unavailable')
    expect(endpoint(result, 'isochrone').status).toBe('unavailable')
    expect(endpoint(result, 'places').status).toBe('ok')
    expect(endpoint(result, 'tiles').status).toBe('ok')
  })

  test('a database outage takes down every group that reads from it', async () => {
    mockExecute.mockImplementation(async () => {
      throw new Error('Connection refused')
    })
    const result = await checkHealth(deps())

    for (const group of ['places', 'spatial', 'search', 'geocode', 'tiles', 'transit']) {
      expect(endpoint(result, group).status).toBe('unavailable')
    }
    // /route proxies GraphHopper and touches no table, so it survives.
    expect(endpoint(result, 'routing').status).toBe('ok')
  })
})

describe('checkHealth — probe caching', () => {
  test('a second call within the TTL reuses the probe rather than re-running it', async () => {
    await checkHealth(deps())
    await checkHealth(deps())
    // Unauthenticated load-balancer probes must not fan out to five services
    // per request.
    expect(mockExecute).toHaveBeenCalledTimes(1)
    expect(mockCheckMotisHealth).toHaveBeenCalledTimes(1)
  })

  test('concurrent callers share one probe instead of each starting their own', async () => {
    await Promise.all([checkHealth(deps()), checkHealth(deps()), checkHealth(deps())])
    expect(mockExecute).toHaveBeenCalledTimes(1)
  })

  test('resetHealthCache forces a fresh probe', async () => {
    await checkHealth(deps())
    resetHealthCache()
    await checkHealth(deps())
    expect(mockExecute).toHaveBeenCalledTimes(2)
  })

  test('checkedAt reports when the probe actually ran', async () => {
    const result = await checkHealth(deps())
    expect(Date.parse(result.checkedAt)).toBeLessThanOrEqual(Date.now())
    expect(result.uptimeSeconds).toBeGreaterThanOrEqual(0)
  })
})

describe('redactHealth', () => {
  test('drops upstream error text but keeps statuses, latencies and our own messages', async () => {
    mockCheckMotisHealth.mockImplementation(async () => ({
      status: 'unavailable' as const,
      message: 'connect ECONNREFUSED 172.19.0.4:8080',
    }))
    const full = await checkHealth(deps())
    const publicView = redactHealth(full)

    // The raw error names an internal host and port — fine for an authenticated
    // operator, needless exposure on a public endpoint.
    expect(full.dependencies.motis.message).toContain('172.19.0.4')
    expect(publicView.dependencies.motis).not.toHaveProperty('message')

    expect(publicView.dependencies.motis.status).toBe('unavailable')
    expect(publicView.dependencies.database.latencyMs).toBeDefined()
    // Messages we author ourselves carry no internals and are the useful part.
    expect(publicView.endpoints.find((e) => e.group === 'transit')?.message).toMatch(/motis/)
  })
})
