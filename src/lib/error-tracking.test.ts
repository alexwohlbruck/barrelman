import { describe, test, expect } from 'bun:test'
import type { ErrorEvent } from '@sentry/bun'
import { scrubEvent } from './error-tracking'
import { statusForError } from '../middleware/error-tracking'

describe('scrubEvent', () => {
  test('keeps only the method and a query-less URL of the request', () => {
    const event = scrubEvent({
      type: undefined,
      request: {
        method: 'POST',
        url: 'https://api.example/search?q=x&key=brm_live_abc',
        headers: { authorization: 'Bearer brm_live_abc', cookie: 'session=1' },
        cookies: { session: '1' },
        data: '{"email":"a@b.co"}',
        query_string: 'q=x&key=brm_live_abc',
      },
    } as ErrorEvent)
    expect(event.request).toEqual({ method: 'POST', url: 'https://api.example/search?[redacted]' })
  })

  test('drops the user and redacts emails and keys in messages and breadcrumbs', () => {
    const event = scrubEvent({
      type: undefined,
      user: { email: 'a@b.co', ip_address: '1.2.3.4' },
      exception: { values: [{ type: 'Error', value: 'No account for jane@example.com using brm_live_abc123' }] },
      breadcrumbs: [{ message: 'Bearer abc.def', data: { url: 'https://x?key=1' } }],
    } as ErrorEvent)
    expect(event.user).toBeUndefined()
    expect(event.exception!.values![0].value).toBe('No account for [email] using [redacted]')
    expect(event.breadcrumbs).toEqual([{ message: '[redacted]' }])
  })
})

describe('statusForError', () => {
  test('client errors are not 5xx', () => {
    expect(statusForError('NOT_FOUND', new Error())).toBe(400)
    expect(statusForError('VALIDATION', new Error())).toBe(400)
    expect(statusForError(429, {})).toBe(429)
  })

  test('an error that carries its own status keeps it', () => {
    expect(statusForError('UNKNOWN', { status: 404 })).toBe(404)
    expect(statusForError('UNKNOWN', { status: 503 })).toBe(503)
  })

  test('anything else thrown from a handler is a 500', () => {
    expect(statusForError('UNKNOWN', new Error('boom'))).toBe(500)
    expect(statusForError('INTERNAL_SERVER_ERROR', new Error('boom'))).toBe(500)
  })
})

describe('errorTracking middleware', () => {
  test('reports a thrown handler error once, leaves 4xx alone, and keeps the response', async () => {
    const { Elysia } = await import('elysia')
    const { errorTracking } = await import('../middleware/error-tracking')
    const captured: unknown[] = []
    const key = Symbol.for('barrelman.sentry')
    const globals = globalThis as Record<symbol, unknown>
    globals[key] = { captureException: (err: unknown) => captured.push(err) }
    try {
      const app = new Elysia()
        .use(errorTracking)
        .get('/boom', () => {
          throw new Error('boom')
        })
        .get('/ok', () => 'ok')

      const boom = await app.handle(new Request('http://localhost/boom'))
      expect(boom.status).toBe(500)
      expect(captured).toHaveLength(1)
      expect((captured[0] as Error).message).toBe('boom')

      expect((await app.handle(new Request('http://localhost/missing'))).status).toBe(404)
      expect(captured).toHaveLength(1)
    } finally {
      globals[key] = undefined
    }
  })
})
