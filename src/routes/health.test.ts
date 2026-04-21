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
import type { HealthResult } from '../services/health.service'

const BASE = 'http://localhost'

function get(path: string, headers?: Record<string, string>) {
  return new Request(`${BASE}${path}`, { headers })
}

const okHealth: HealthResult = { status: 'ok', database: 'connected' }

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
    expect(await res.json()).toEqual(okHealth)
    expect(checkHealth).toHaveBeenCalledTimes(1)
  })

  test('still works when BARRELMAN_API_KEY is set (public endpoint)', async () => {
    process.env.BARRELMAN_API_KEY = 'secret'
    const checkHealth = mock(async () => okHealth)
    const app = new Elysia().use(createHealthRoutes({ checkHealth }))

    const res = await app.handle(get('/health'))

    expect(res.status).toBe(200)
    expect(await res.json()).toEqual(okHealth)
  })
})

describe('GET /health/auth', () => {
  test('returns 401 when API key is required but missing', async () => {
    process.env.BARRELMAN_API_KEY = 'secret'
    const checkHealth = mock(async () => okHealth)
    const app = new Elysia().use(createHealthRoutes({ checkHealth }))

    const res = await app.handle(get('/health/auth'))

    expect(res.status).toBe(401)
    expect(await res.json()).toEqual({ error: 'Missing Authorization header' })
    expect(checkHealth).not.toHaveBeenCalled()
  })

  test('returns 401 when Bearer token does not match', async () => {
    process.env.BARRELMAN_API_KEY = 'secret'
    const checkHealth = mock(async () => okHealth)
    const app = new Elysia().use(createHealthRoutes({ checkHealth }))

    const res = await app.handle(get('/health/auth', { authorization: 'Bearer wrong' }))

    expect(res.status).toBe(401)
    expect(await res.json()).toEqual({ error: 'Invalid API key' })
    expect(checkHealth).not.toHaveBeenCalled()
  })

  test('returns health + authenticated=true on valid key', async () => {
    process.env.BARRELMAN_API_KEY = 'secret'
    const checkHealth = mock(async () => okHealth)
    const app = new Elysia().use(createHealthRoutes({ checkHealth }))

    const res = await app.handle(get('/health/auth', { authorization: 'Bearer secret' }))

    expect(res.status).toBe(200)
    expect(await res.json()).toEqual({ ...okHealth, authenticated: true })
    expect(checkHealth).toHaveBeenCalledTimes(1)
  })

  test('is open when BARRELMAN_API_KEY is unset (dev mode)', async () => {
    const checkHealth = mock(async () => okHealth)
    const app = new Elysia().use(createHealthRoutes({ checkHealth }))

    const res = await app.handle(get('/health/auth'))

    expect(res.status).toBe(200)
    expect(await res.json()).toEqual({ ...okHealth, authenticated: true })
  })
})
