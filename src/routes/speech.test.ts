import { beforeEach, describe, expect, mock, test } from 'bun:test'
import { Elysia } from 'elysia'
let caller: 'service' | 'account' | 'anonymous' = 'service'
let authorized = true
mock.module('../middleware/api-auth', () => ({ identifyCaller: async () => ({ caller: { kind: caller }, ...(authorized ? {} : { error: { status: 401, body: { error: 'Unauthorized' } } }) }) }))
const getCatalog = mock(async () => ({ model: 'kokoro:v1.0', voices: [] }))
const synthesize = mock(async () => new Uint8Array([1, 2, 3]))
const { SpeechError } = await import('../services/speech.service')
mock.module('../services/speech.service', () => ({ SpeechError, speechService: { getCatalog, synthesize } }))
const { speechRoutes } = await import('./speech')
const app = new Elysia().use(speechRoutes)
const request = (method = 'GET', body?: unknown) => app.handle(new Request('http://localhost/speech', { method, ...(body ? { body: JSON.stringify(body), headers: { 'Content-Type': 'application/json' } } : {}) }))
beforeEach(() => { caller = 'service'; authorized = true; synthesize.mockClear() })
describe('speech service access', () => {
  test('guards both discovery and synthesis', async () => {
    authorized = false
    expect((await request()).status).toBe(401)
    expect((await request('POST', { text: 'Turn', language: 'en' })).status).toBe(401)
    expect(synthesize).not.toHaveBeenCalled()
  })
  test('does not expose unmetered inference to account keys', async () => {
    caller = 'account'
    expect((await request()).status).toBe(403)
    expect((await request('POST', { text: 'Turn', language: 'en' })).status).toBe(403)
    expect(synthesize).not.toHaveBeenCalled()
  })
  test('allows the instance service credential and returns audio', async () => {
    expect((await request()).status).toBe(200)
    const response = await request('POST', { text: 'Turn', language: 'en', rate: 1.25 })
    expect(response.status).toBe(200)
    expect(response.headers.get('content-type')).toBe('audio/mpeg')
    expect(new Uint8Array(await response.arrayBuffer())).toEqual(new Uint8Array([1, 2, 3]))
  })
})
