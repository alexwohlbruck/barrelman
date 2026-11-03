import { describe, expect, test, mock } from 'bun:test'
import { SpeechService } from './speech.service'

function setup() {
  const synthesize = mock(async (_url: string, _init?: RequestInit) => new Response(new Uint8Array([1, 2, 3]), { headers: { 'Content-Type': 'audio/mpeg' } }))
  const fetchFn = mock(async (url: string | URL | Request, init?: RequestInit) => {
    if (String(url).endsWith('/voices')) return Response.json({ voices: [{ id: 'af_alloy', name: 'af_alloy' }, { id: 'af_heart', name: 'af_heart' }, { id: 'bf_emma' }, { id: 'ef_dora' }, { id: 'ff_siwis' }, { id: 'unknown' }, null, 42], default_voice: 'af_heart' })
    return synthesize(String(url), init)
  }) as unknown as typeof fetch
  return { service: new SpeechService(() => 'http://tts:8880', fetchFn), synthesize, fetchFn }
}

describe('speech synthesis', () => {
  test('discovers real locale voices and advertises native rate without inflection', async () => {
    const { service } = setup()
    const catalog = await service.getCatalog()
    expect(catalog.voices.map(v => v.language)).toEqual(['en-US', 'en-US', 'en-GB', 'es', 'fr'])
    expect(catalog.voices[0]!.id).toBe('af_heart')
    expect(catalog.rate.native).toBe(true)
    expect(catalog.inflection).toBe(false)
  })
  test('selects locale voices, forwards native speed, and disables markup', async () => {
    const { service, synthesize } = setup()
    await service.synthesize({ text: 'Turn right', language: 'en-GB', rate: 1.25 })
    const body = JSON.parse(synthesize.mock.calls[0]![1]!.body as string)
    expect(body).toMatchObject({ voice: 'bf_emma', speed: 1.25, stream: false, ssml: false, allow_voice_tags: false })
  })
  test('caches identical clips and separates rates, languages and voices', async () => {
    const { service, synthesize } = setup()
    const request = { text: 'Turn right', language: 'en-US' }
    await service.synthesize(request)
    await service.synthesize(request)
    expect(synthesize).toHaveBeenCalledTimes(1)
    await service.synthesize({ ...request, rate: 1.25 })
    await service.synthesize({ ...request, language: 'en-GB' })
    expect(synthesize).toHaveBeenCalledTimes(3)
  })
  test('rejects unsupported languages and mismatched voice choices', async () => {
    const { service, synthesize } = setup()
    await expect(service.synthesize({ text: 'Turn right', language: 'de' })).rejects.toMatchObject({ status: 422 })
    await expect(service.synthesize({ text: 'Turn right', language: 'es', voice: 'af_heart' })).rejects.toMatchObject({ status: 422 })
    expect(synthesize).not.toHaveBeenCalled()
  })
  test('rejects invalid text and rates before inference', async () => {
    const { service, fetchFn } = setup()
    for (const text of [' ', 'x'.repeat(501)]) await expect(service.synthesize({ text, language: 'en' })).rejects.toMatchObject({ status: 400 })
    await expect(service.synthesize({ text: 'Turn', language: 'en', rate: 4 })).rejects.toMatchObject({ status: 400 })
    expect(fetchFn).not.toHaveBeenCalled()
  })
  test('disabled service performs no upstream requests', async () => {
    const fetchFn = mock(async () => Response.json({})) as unknown as typeof fetch
    await expect(new SpeechService(() => '', fetchFn).getCatalog()).rejects.toMatchObject({ status: 503 })
    expect(fetchFn).not.toHaveBeenCalled()
  })
  test('rejects invalid upstream audio', async () => {
    const { service, synthesize } = setup()
    synthesize.mockResolvedValue(Response.json({ error: 'not audio' }))
    await expect(service.synthesize({ text: 'Turn', language: 'en' })).rejects.toMatchObject({ status: 503 })
  })
  test('caps concurrent inference while allowing catalog reuse', async () => {
    const { service, synthesize } = setup()
    await service.getCatalog()
    let finish!: () => void
    const pending = new Promise<void>(resolve => { finish = resolve })
    synthesize.mockImplementation(async () => { await pending; return new Response(new Uint8Array([1]), { headers: { 'Content-Type': 'audio/mpeg' } }) })
    const first = service.synthesize({ text: 'First', language: 'en' })
    const second = service.synthesize({ text: 'Second', language: 'en' })
    await Promise.resolve()
    await expect(service.synthesize({ text: 'Third', language: 'en' })).rejects.toMatchObject({ status: 429 })
    finish()
    await Promise.all([first, second])
  })
})

test('deduplicates concurrent identical synthesis', async () => {
  const { service, synthesize } = setup()
  await service.getCatalog()
  const request = { text: 'Turn left', language: 'en' }
  const result = await Promise.all([service.synthesize(request), service.synthesize(request)])
  expect(result[0]).toEqual(result[1])
  expect(synthesize).toHaveBeenCalledTimes(1)
})
