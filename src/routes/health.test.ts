/**
 * HTTP-layer tests for /health and /health/auth.
 *
 * Uses createHealthRoutes() with a mocked checkHealth dep to avoid hitting a
 * real database. Covers:
 *   - /health is public and returns the checkHealth result
 *   - /health/auth requires a valid Bearer BARRELMAN_API_KEY
 *   - /health/auth is open when BARRELMAN_API_KEY is unset (dev mode)
 */

import { describe, test, expect, mock, beforeEach, afterEach } from 'bun:test'
import Elysia from 'elysia'
import { createHealthRoutes } from './health'
import { clearThrottleState as clearRateBuckets } from '../services/throttle.service'
import { redactHealth } from '../services/health.service'
import { healthFixture } from '../services/health.fixture'

const BASE = 'http://localhost'

function get(path: string, headers?: Record<string, string>) {
  return new Request(`${BASE}${path}`, { headers })
}

const okHealth = healthFixture()
/** What /health serves: the same result with upstream error text stripped. */
const publicHealth = JSON.parse(JSON.stringify(redactHealth(okHealth)))

const savedApiKey = process.env.BARRELMAN_API_KEY

beforeEach(() => {
  delete process.env.BARRELMAN_API_KEY
})

afterEach(() => {
  if (savedApiKey === undefined) delete process.env.BARRELMAN_API_KEY
  else process.env.BARRELMAN_API_KEY = savedApiKey
})

describe('GET /health', () => {
  test('returns checkHealth result without auth', async () => {
    const checkHealth = mock(async () => okHealth)
    const app = new Elysia().use(createHealthRoutes({ checkHealth }))

    const res = await app.handle(get('/health'))

    expect(res.status).toBe(200)
    expect(await res.json()).toEqual(publicHealth)
    expect(checkHealth).toHaveBeenCalledTimes(1)
  })

  test('still works when BARRELMAN_API_KEY is set (public endpoint)', async () => {
    process.env.BARRELMAN_API_KEY = 'secret'
    const checkHealth = mock(async () => okHealth)
    const app = new Elysia().use(createHealthRoutes({ checkHealth }))

    const res = await app.handle(get('/health'))

    expect(res.status).toBe(200)
    expect(await res.json()).toEqual(publicHealth)
  })
})

describe('GET /health/auth', () => {
  test('returns 401 when API key is required but missing', async () => {
    process.env.BARRELMAN_API_KEY = 'secret'
    const checkHealth = mock(async () => okHealth)
    const app = new Elysia().use(createHealthRoutes({ checkHealth }))

    const res = await app.handle(get('/health/auth'))

    expect(res.status).toBe(401)
    // The body now points at the docs and names the console, since an
    // unauthenticated caller is a developer who needs to go make a key.
    expect(await res.json()).toMatchObject({ error: expect.stringContaining('API key') })
    expect(checkHealth).not.toHaveBeenCalled()
  })

  test('returns 401 when Bearer token does not match', async () => {
    process.env.BARRELMAN_API_KEY = 'secret'
    const checkHealth = mock(async () => okHealth)
    const app = new Elysia().use(createHealthRoutes({ checkHealth }))

    const res = await app.handle(get('/health/auth', { authorization: 'Bearer wrong' }))

    expect(res.status).toBe(401)
    expect(await res.json()).toMatchObject({ error: 'Invalid API key' })
    expect(checkHealth).not.toHaveBeenCalled()
  })

  test('returns health + authenticated=true on valid key', async () => {
    process.env.BARRELMAN_API_KEY = 'secret'
    const checkHealth = mock(async () => okHealth)
    const app = new Elysia().use(createHealthRoutes({ checkHealth }))

    const res = await app.handle(get('/health/auth', { authorization: 'Bearer secret' }))

    expect(res.status).toBe(200)
    // Also reports which kind of credential was accepted, so a developer can
    // tell an account key from the shared service key.
    expect(await res.json()).toMatchObject({ ...okHealth, authenticated: true, caller: 'service' })
    expect(checkHealth).toHaveBeenCalledTimes(1)
  })

  test('is open when BARRELMAN_API_KEY is unset (dev mode)', async () => {
    const checkHealth = mock(async () => okHealth)
    const app = new Elysia().use(createHealthRoutes({ checkHealth }))

    const res = await app.handle(get('/health/auth'))

    expect(res.status).toBe(200)
    // No credential presented and none configured, so the caller is anonymous
    // rather than the shared service identity.
    expect(await res.json()).toMatchObject({ ...okHealth, authenticated: true, caller: 'anonymous' })
  })
})

// ── Penalty box ──────────────────────────────────────────────────────────────

describe('GET /health/auth throttling', () => {
  /**
   * This route validates a credential and charges nothing, which makes it the
   * cheapest place on the instance to test whether a stolen key is real. It
   * calls identifyCaller directly rather than going through apiAuth(), so the
   * penalty box has to be wired up by hand — and was not, which left an
   * unlimited, unlogged key-validation oracle that an address already refused
   * everywhere else could keep using.
   */
  // Credentials deliberately WITHOUT the `brm_` prefix: identifyCaller refuses
  // those at the prefix check, before it would resolve a key against Postgres.
  // With the prefix they reach the database, which passes on a developer's
  // machine and throws in CI — where this failed after passing locally.
  test('rejections earn strikes and eventually 429', async () => {
    clearRateBuckets()
    const checkHealth = mock(async () => okHealth)
    process.env.BARRELMAN_API_KEY = 'svc_secret'
    const app = new Elysia().use(createHealthRoutes({ checkHealth }))

    const statuses: number[] = []
    for (let i = 0; i < 80; i++) {
      const res = await app.handle(
        get('/health/auth', { Authorization: `Bearer not_a_real_key_${i}` }),
      )
      statuses.push(res.status)
    }

    expect(statuses[0]).toBe(401)
    expect(statuses).toContain(429)
    // Once boxed it stays boxed, rather than alternating.
    expect(statuses[statuses.length - 1]).toBe(429)
    clearRateBuckets()
  })

  test('a boxed address cannot validate a good credential either', async () => {
    clearRateBuckets()
    const checkHealth = mock(async () => okHealth)
    process.env.BARRELMAN_API_KEY = 'svc_secret'
    const app = new Elysia().use(createHealthRoutes({ checkHealth }))

    for (let i = 0; i < 80; i++) {
      await app.handle(get('/health/auth', { Authorization: `Bearer not_a_real_key_${i}` }))
    }

    const res = await app.handle(get('/health/auth', { Authorization: 'Bearer svc_secret' }))
    expect(res.status).toBe(429)
    clearRateBuckets()
  })

  test('a clean address is unaffected', async () => {
    clearRateBuckets()
    const checkHealth = mock(async () => okHealth)
    process.env.BARRELMAN_API_KEY = 'svc_secret'
    const app = new Elysia().use(createHealthRoutes({ checkHealth }))

    const res = await app.handle(get('/health/auth', { Authorization: 'Bearer svc_secret' }))
    expect(res.status).toBe(200)
    clearRateBuckets()
  })
})
