import { describe, expect, test } from 'bun:test'
import { BarrelmanError, createClient, type ClientOptions } from '../src/index'
import { isRetryable, retryDelay } from '../src/client'
import { parseRetryAfter } from '../src/errors'

interface Call {
  url: string
  method: string
  headers: Record<string, string>
  body: unknown
}

/** A fetch that answers from a queue and records what it was asked. */
function mockFetch(...responses: (() => Response)[]) {
  const calls: Call[] = []
  const fetch = (async (input: RequestInfo | URL, init?: RequestInit) => {
    calls.push({
      url: String(input),
      method: init?.method ?? 'GET',
      headers: Object.fromEntries(new Headers(init?.headers).entries()),
      body: typeof init?.body === 'string' ? JSON.parse(init.body) : undefined,
    })
    const next = responses.shift()
    if (!next) throw new Error('mockFetch: no response queued')
    return next()
  }) as typeof globalThis.fetch
  return { fetch, calls }
}

const json = (body: unknown, status = 200, headers: Record<string, string> = {}) => () =>
  new Response(JSON.stringify(body), { status, headers: { 'content-type': 'application/json', ...headers } })

/** The BarrelmanError a call rejects with. */
async function rejection(call: Promise<unknown>): Promise<BarrelmanError> {
  try {
    await call
  } catch (error) {
    return error as BarrelmanError
  }
  throw new Error('expected the call to reject')
}

function client(fetch: typeof globalThis.fetch, extra: Partial<ClientOptions> = {}) {
  return createClient({ apiKey: 'brm_live_test', fetch, ...extra })
}

describe('requests', () => {
  test('sends the key as a Bearer header to the default base URL', async () => {
    const { fetch, calls } = mockFetch(json([{ name: 'Cafe' }]))
    const result = await client(fetch).search({ query: 'coffee', lat: 35.2, lng: -80.8 })

    expect(result).toEqual([{ name: 'Cafe' }])
    expect(calls[0]!.url).toBe('https://api.barrelman.dev/search')
    expect(calls[0]!.method).toBe('POST')
    expect(calls[0]!.headers.authorization).toBe('Bearer brm_live_test')
    expect(calls[0]!.headers['content-type']).toBe('application/json')
    expect(calls[0]!.body).toEqual({ query: 'coffee', lat: 35.2, lng: -80.8 })
  })

  test('a caller header cannot replace the key', async () => {
    const { fetch, calls } = mockFetch(json({}))
    await client(fetch, { headers: { Authorization: 'Bearer other' } }).request('/brands', {
      headers: { authorization: 'Bearer other' },
    })
    expect(calls[0]!.headers.authorization).toBe('Bearer brm_live_test')
  })

  test('autocomplete sets the autocomplete flag', async () => {
    const { fetch, calls } = mockFetch(json([]))
    await client(fetch).autocomplete({ query: 'sta', limit: 5 })
    expect(calls[0]!.body).toEqual({ query: 'sta', limit: 5, autocomplete: true })
  })

  test('query parameters are stringified and undefined ones dropped', async () => {
    const { fetch, calls } = mockFetch(json([]))
    await client(fetch, { baseUrl: 'http://localhost:5001/' }).geocode.reverse({
      lat: 35.3079,
      lng: -80.7332,
      limit: undefined,
    })
    expect(calls[0]!.url).toBe('http://localhost:5001/geocode/reverse?lat=35.3079&lng=-80.7332')
    expect(calls[0]!.method).toBe('GET')
    expect(calls[0]!.headers['content-type']).toBeUndefined()
  })

  test('path parameters are encoded', async () => {
    const { fetch, calls } = mockFetch(json({}))
    await client(fetch).place('node', 5718230659)
    expect(calls[0]!.url).toBe('https://api.barrelman.dev/place/node/5718230659')
  })

  test('requires an API key', () => {
    expect(() => createClient({ apiKey: '' })).toThrow(TypeError)
  })
})

describe('errors', () => {
  test('a refusal maps to BarrelmanError with code, request id and Retry-After', async () => {
    const { fetch } = mockFetch(
      json({ error: 'Rate limit reached', code: 'rate_limited', layer: 'key' }, 429, {
        'x-request-id': 'req_12345678',
        'retry-after': '3',
      }),
    )
    const error = await rejection(client(fetch).transit.stops({ lat: 35, lng: -80 }))

    expect(error).toBeInstanceOf(BarrelmanError)
    expect(error.status).toBe(429)
    expect(error.code).toBe('rate_limited')
    expect(error.error).toBe('Rate limit reached')
    expect(error.requestId).toBe('req_12345678')
    expect(error.retryAfter).toBe(3)
    expect(error.body).toEqual({ error: 'Rate limit reached', code: 'rate_limited', layer: 'key' })
    expect(error.message).toContain('rate_limited')
  })

  test('a non-JSON error body is kept as text', async () => {
    const { fetch } = mockFetch(() => new Response('Bad Gateway', { status: 502, statusText: 'Bad Gateway' }))
    const error = await rejection(client(fetch).route({ points: [[-80.8, 35.2], [-80.7, 35.3]] }))

    expect(error).toBeInstanceOf(BarrelmanError)
    expect(error.status).toBe(502)
    expect(error.code).toBeUndefined()
    expect(error.error).toBe('Bad Gateway')
    expect(error.requestId).toBeUndefined()
  })

  test('Retry-After accepts seconds and HTTP dates', () => {
    const now = Date.parse('2026-11-04T12:00:00Z')
    expect(parseRetryAfter('5', now)).toBe(5)
    expect(parseRetryAfter('Wed, 04 Nov 2026 12:00:30 GMT', now)).toBe(30)
    expect(parseRetryAfter('soon', now)).toBeUndefined()
    expect(parseRetryAfter(null, now)).toBeUndefined()
  })
})

describe('retry', () => {
  const fast = { retries: 2, minDelayMs: 1, maxDelayMs: 5 }

  test('is off by default', async () => {
    const { fetch, calls } = mockFetch(json({ error: 'down' }, 503), json({}))
    await expect(client(fetch).match({ points: [] })).rejects.toBeInstanceOf(BarrelmanError)
    expect(calls).toHaveLength(1)
  })

  test('retries 5xx and 429, then succeeds', async () => {
    const { fetch, calls } = mockFetch(
      json({ error: 'down' }, 503),
      json({ error: 'slow down', code: 'rate_limited' }, 429, { 'retry-after': '0' }),
      json({ ok: true }),
    )
    const result = await client(fetch, { retry: fast }).isochrone({ lat: 35, lng: -80 })
    expect(result).toEqual({ ok: true })
    expect(calls).toHaveLength(3)
  })

  test('gives up after the configured retries', async () => {
    const { fetch, calls } = mockFetch(json({}, 500), json({}, 500), json({}, 500), json({}))
    const error = await rejection(client(fetch, { retry: fast }).search({ query: 'x' }))
    expect(error.status).toBe(500)
    expect(calls).toHaveLength(3)
  })

  test('does not retry client errors or too_many_failures', async () => {
    for (const [status, code] of [[401, 'invalid_key'], [402, 'insufficient_credits'], [429, 'too_many_failures']] as const) {
      const { fetch, calls } = mockFetch(json({ error: 'no', code }, status), json({}))
      await client(fetch, { retry: fast }).search({ query: 'x' }).catch(() => {})
      expect(calls).toHaveLength(1)
    }
  })

  test('throws rather than wait out a Retry-After past maxDelayMs', async () => {
    const { fetch, calls } = mockFetch(json({ code: 'rate_limited' }, 429, { 'retry-after': '60' }), json({}))
    const error = await rejection(client(fetch, { retry: fast }).search({ query: 'x' }))
    expect(error.retryAfter).toBe(60)
    expect(calls).toHaveLength(1)
  })

  test('backoff obeys Retry-After and stays under the ceiling otherwise', () => {
    const opts = { retries: 5, minDelayMs: 100, maxDelayMs: 1000 }
    expect(retryDelay(new BarrelmanError({ status: 429, retryAfter: 1 }), 0, opts)).toBe(1000)
    for (let attempt = 0; attempt < 8; attempt++) {
      const delay = retryDelay(new BarrelmanError({ status: 503 }), attempt, opts)!
      expect(delay).toBeGreaterThanOrEqual(0)
      expect(delay).toBeLessThanOrEqual(Math.min(1000, 100 * 2 ** attempt))
    }
    expect(isRetryable(new BarrelmanError({ status: 404 }))).toBe(false)
  })

  test('an abort during backoff stops the retry', async () => {
    const { fetch, calls } = mockFetch(json({}, 503), json({}))
    const controller = new AbortController()
    const pending = client(fetch, { retry: { retries: 1, minDelayMs: 1000, maxDelayMs: 1000 } }).search(
      { query: 'x' },
      { signal: controller.signal },
    )
    setTimeout(() => controller.abort(new Error('cancelled')), 5)
    await expect(pending).rejects.toThrow('cancelled')
    expect(calls).toHaveLength(1)
  })
})

describe('tiles', () => {
  const tiles = createClient({ apiKey: 'brm_live_test', baseUrl: 'https://maps.example.com' }).tiles

  test('style and TileJSON URLs carry the key by default', () => {
    expect(tiles.styleUrl()).toBe('https://maps.example.com/styles/basemap.json?api_key=brm_live_test')
    expect(tiles.styleUrl('basemap-dark', { withKey: false })).toBe('https://maps.example.com/styles/basemap-dark.json')
    expect(tiles.tileJsonUrl('detail')).toBe('https://maps.example.com/tiles/detail.json?api_key=brm_live_test')
    expect(tiles.tileUrlTemplate('basemap')).toBe('https://maps.example.com/tiles/basemap/{z}/{x}/{y}?api_key=brm_live_test')
  })

  test('transformRequest adds the header only for this API', () => {
    expect(tiles.transformRequest('https://maps.example.com/tiles/basemap/1/2/3')).toEqual({
      url: 'https://maps.example.com/tiles/basemap/1/2/3',
      headers: { Authorization: 'Bearer brm_live_test' },
    })
    expect(tiles.transformRequest('https://maps.example.com.evil.net/x')).toEqual({ url: 'https://maps.example.com.evil.net/x' })
    expect(tiles.transformRequest('https://demotiles.maplibre.org/font/a/0-255.pbf')).toEqual({
      url: 'https://demotiles.maplibre.org/font/a/0-255.pbf',
    })
  })
})
