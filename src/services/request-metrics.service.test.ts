/**
 * The pure half of the request metrics: what gets counted where, and how a
 * percentile is read back out of the histogram. The SQL half needs Postgres.
 */
import { describe, test, expect, beforeEach } from 'bun:test'
import Elysia from 'elysia'
import {
  bufferedRequestMetrics,
  clearRequestMetricsBuffer,
  latencyBucket,
  LATENCY_BOUNDS_MS,
  percentileFromHistogram,
  recordRequest,
  UNIDENTIFIED_SUBJECT,
} from './request-metrics.service'
import { attribute, refusalFor, requestMetrics } from '../middleware/request-metrics'

beforeEach(clearRequestMetricsBuffer)

describe('recordRequest', () => {
  test('keeps refusals out of the latency figures', () => {
    // A refusal returns in microseconds. Counting it would make the endpoint
    // being throttled hardest look like the fastest one.
    recordRequest({ subject: 'u1', apiKeyId: 'k1', endpoint: 'search', status: 200, durationMs: 300 })
    recordRequest({ subject: 'u1', apiKeyId: 'k1', endpoint: 'search', status: 429, durationMs: 0, refusal: 'key' })

    const [row] = bufferedRequestMetrics()
    expect(row!.requests).toBe(2)
    expect(row!.served).toBe(1)
    expect(row!.limitedKey).toBe(1)
    expect(row!.latencyMsSum).toBe(300)
    expect(row!.hist.reduce((a, b) => a + b, 0)).toBe(1)
  })

  test('separates client and server errors among served requests', () => {
    recordRequest({ subject: 'u1', endpoint: 'routing', status: 400, durationMs: 5 })
    recordRequest({ subject: 'u1', endpoint: 'routing', status: 502, durationMs: 5 })
    const [row] = bufferedRequestMetrics()
    expect(row!.clientErrors).toBe(1)
    expect(row!.serverErrors).toBe(1)
  })
})

describe('percentileFromHistogram', () => {
  test('is null with nothing served', () => {
    expect(percentileFromHistogram(new Array(LATENCY_BOUNDS_MS.length + 1).fill(0), 0.95)).toBeNull()
  })

  test('interpolates inside the bucket it lands in', () => {
    const hist = new Array(LATENCY_BOUNDS_MS.length + 1).fill(0)
    hist[latencyBucket(75)] = 100 // all in the 50–100 ms bucket
    expect(percentileFromHistogram(hist, 0.5)).toBe(75)
  })

  test('reports the open top bucket by its lower bound', () => {
    const hist = new Array(LATENCY_BOUNDS_MS.length + 1).fill(0)
    hist[latencyBucket(60_000)] = 10
    expect(percentileFromHistogram(hist, 0.99)).toBe(10_000)
  })
})

describe('refusalFor', () => {
  test('names the throttle layer for a 429', () => {
    expect(refusalFor(429, 'concurrency')).toBe('concurrency')
    expect(refusalFor(429, 'penalty')).toBe('penalty')
  })

  test('separates quota from credential refusals', () => {
    expect(refusalFor(402)).toBe('quota')
    expect(refusalFor(401)).toBe('denied')
    expect(refusalFor(403)).toBe('denied')
  })
})

describe('the request-metrics hook', () => {
  function app() {
    return new Elysia()
      .use(requestMetrics)
      .onBeforeHandle(({ request, set }) => {
        if (!new URL(request.url).pathname.startsWith('/search')) return
        attribute(request, { subject: 'u1', apiKeyId: 'k1', endpoint: 'search' })
        if (request.url.includes('limited')) {
          attribute(request, { refusal: 'account' })
          set.status = 429
          return { error: 'slow down' }
        }
      })
      .get('/search', () => ({ ok: true }))
      .get('/search/limited', () => ({ ok: true }))
      .get('/search/broken', () => {
        throw new Error('upstream down')
      })
      .get('/console/thing', () => 'not metered')
  }

  // onAfterResponse runs once the response is out, so give it a tick.
  const settle = () => Bun.sleep(5)

  test('records a refusal the guard returned, against its caller', async () => {
    await app().handle(new Request('http://localhost/search/limited'))
    await settle()
    const [row] = bufferedRequestMetrics()
    expect(row).toMatchObject({ userId: 'u1', apiKeyId: 'k1', endpoint: 'search', limitedAccount: 1, served: 0 })
  })

  test('records a thrown handler as a server error', async () => {
    await app().handle(new Request('http://localhost/search/broken'))
    await settle()
    expect(bufferedRequestMetrics()[0]).toMatchObject({ served: 1, serverErrors: 1 })
  })

  test('ignores routes outside the metered groups', async () => {
    await app().handle(new Request('http://localhost/console/thing'))
    await settle()
    expect(bufferedRequestMetrics()).toHaveLength(0)
  })

  test('attributes a request the guard never saw to nobody in particular', async () => {
    await new Elysia().use(requestMetrics).get('/search', () => 'ok').handle(new Request('http://localhost/search'))
    await settle()
    expect(bufferedRequestMetrics()[0]!.userId).toBe(UNIDENTIFIED_SUBJECT)
  })
})
