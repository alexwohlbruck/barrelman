import { describe, test, expect, beforeEach, afterEach } from 'bun:test'
import Elysia from 'elysia'
import { createPlaceMatchRoutes, type PlaceMatchDeps } from './place-matches'
import type { PlaceRef, ProviderMatch } from '../services/provider-matches.service'

const SERVICE_KEY = 'service-secret'
const savedApiKey = process.env.BARRELMAN_API_KEY
afterEach(() => {
  if (savedApiKey === undefined) delete process.env.BARRELMAN_API_KEY
  else process.env.BARRELMAN_API_KEY = savedApiKey
})

const LIVE_KEY = 'brm_live_abcdefghijklmnopqrstuvwxyz0123456789ABCD'
const accountAuth = {
  resolveApiKey: async () => ({
    keyId: 'key-1',
    userId: 'user-1',
    scopes: ['*'],
    allowedOrigins: [],
    plan: 'developer',
    suspended: false,
    suspensionReason: null,
  }) as never,
  checkQuota: async () => ({ allowed: true as const, overage: false }),
  recordUsage: () => undefined,
}

function app() {
  const stored = new Map<string, ProviderMatch>()
  const key = (place: PlaceRef, provider: string) => `${place.osmType}/${place.osmId}/${provider}`
  const deps: PlaceMatchDeps = {
    getProviderMatches: async (place) =>
      [...stored.entries()].filter(([k]) => k.startsWith(`${place.osmType}/${place.osmId}/`)).map(([, m]) => m),
    setProviderMatch: async (place, provider, match) => {
      const row = { provider, ...match, label: match.label ?? null, updatedAt: '2026-11-25T00:00:00.000Z' }
      stored.set(key(place, provider), row)
      return row
    },
    deleteProviderMatch: async (place, provider) => stored.delete(key(place, provider)),
  }
  return { app: new Elysia().use(createPlaceMatchRoutes(deps, accountAuth)), stored }
}

function request(method: string, path: string, body?: unknown, token?: string) {
  const headers: Record<string, string> = {}
  if (body !== undefined) headers['content-type'] = 'application/json'
  if (token) headers.authorization = `Bearer ${token}`
  return new Request(`http://localhost${path}`, {
    method,
    headers,
    body: body === undefined ? undefined : JSON.stringify(body),
  })
}

describe('place matches with a service key configured', () => {
  beforeEach(() => {
    process.env.BARRELMAN_API_KEY = SERVICE_KEY
  })

  test('the service credential sets, reads and deletes a match', async () => {
    const { app: a } = app()
    const put = await a.handle(
      request('PUT', '/place/node/6493733562/matches/foursquare', { providerPlaceId: '4cd3', confirmed: true }, SERVICE_KEY),
    )
    expect(put.status).toBe(200)
    expect(await put.json()).toMatchObject({ provider: 'foursquare', providerPlaceId: '4cd3', confirmed: true })

    const get = await a.handle(request('GET', '/place/node/6493733562/matches', undefined, SERVICE_KEY))
    expect((await get.json()).matches).toEqual([
      expect.objectContaining({ provider: 'foursquare', providerPlaceId: '4cd3' }),
    ])

    const del = await a.handle(request('DELETE', '/place/node/6493733562/matches/foursquare', undefined, SERVICE_KEY))
    expect(del.status).toBe(204)
    const again = await a.handle(request('DELETE', '/place/node/6493733562/matches/foursquare', undefined, SERVICE_KEY))
    expect(again.status).toBe(404)
  })

  test('a null id records that the place has no listing', async () => {
    const { app: a, stored } = app()
    const res = await a.handle(
      request('PUT', '/place/way/1/matches/google', { providerPlaceId: null, confirmed: true }, SERVICE_KEY),
    )
    expect(res.status).toBe(200)
    expect(stored.get('way/1/google')).toMatchObject({ providerPlaceId: null, confirmed: true })
  })

  test('stores and returns a listing’s display name', async () => {
    const { app: a } = app()
    const put = await a.handle(
      request('PUT', '/place/way/42/matches/google_hotels', { providerPlaceId: 'ChoIp8', confirmed: true, label: 'EVEN Hotel Brooklyn by IHG' }, SERVICE_KEY),
    )
    expect(put.status).toBe(200)
    const res = await a.handle(request('GET', '/place/way/42/matches', undefined, SERVICE_KEY))
    expect((await res.json()).matches[0]).toMatchObject({ providerPlaceId: 'ChoIp8', label: 'EVEN Hotel Brooklyn by IHG' })
  })

  test('rejects an over-long display name', async () => {
    const { app: a } = app()
    const res = await a.handle(
      request('PUT', '/place/way/42/matches/google_hotels', { providerPlaceId: 'ChoIp8', label: 'x'.repeat(301) }, SERVICE_KEY),
    )
    expect(res.status).toBe(422)
  })

  test('an unconfirmed write is the default', async () => {
    const { app: a, stored } = app()
    await a.handle(request('PUT', '/place/node/1/matches/google', { providerPlaceId: 'ChIJ' }, SERVICE_KEY))
    expect(stored.get('node/1/google')?.confirmed).toBe(false)
  })

  test('refuses writes without the service credential', async () => {
    const { app: a, stored } = app()
    const res = await a.handle(request('PUT', '/place/node/1/matches/google', { providerPlaceId: 'ChIJ' }))
    expect(res.status).toBe(401)
    expect(stored.size).toBe(0)
  })

  test('a customer key reads matches but cannot change them', async () => {
    const { app: a, stored } = app()
    stored.set('node/1/google', { provider: 'google', providerPlaceId: 'ChIJ', confirmed: false, label: null, updatedAt: '2026-11-25T00:00:00.000Z' })
    const get = await a.handle(request('GET', '/place/node/1/matches', undefined, LIVE_KEY))
    expect(get.status).toBe(200)
    const put = await a.handle(request('PUT', '/place/node/1/matches/google', { providerPlaceId: 'other' }, LIVE_KEY))
    expect(put.status).toBe(403)
    const del = await a.handle(request('DELETE', '/place/node/1/matches/google', undefined, LIVE_KEY))
    expect(del.status).toBe(403)
    expect(stored.get('node/1/google')?.providerPlaceId).toBe('ChIJ')
  })

  test('rejects ids that are not OSM elements before touching storage', async () => {
    const { app: a, stored } = app()
    for (const path of ['/place/intersection/1/matches/google', '/place/node/abc/matches/google', '/place/node/1/matches/Not%20A%20Provider']) {
      const res = await a.handle(request('PUT', path, { providerPlaceId: 'x' }, SERVICE_KEY))
      expect(res.status).toBe(422)
    }
    expect(stored.size).toBe(0)
  })
})

describe('place matches with no auth configured', () => {
  beforeEach(() => {
    delete process.env.BARRELMAN_API_KEY
  })

  test('local development is open', async () => {
    const { app: a, stored } = app()
    const res = await a.handle(request('PUT', '/place/node/1/matches/google', { providerPlaceId: 'ChIJ' }))
    expect(res.status).toBe(200)
    expect(stored.size).toBe(1)
  })
})
