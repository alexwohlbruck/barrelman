import { Elysia, t } from 'elysia'
import { identifyCaller } from '../middleware/api-auth'
import { SpeechError, speechService } from '../services/speech.service'

export function createSpeechRoutes(deps: {
  identifyCaller?: typeof identifyCaller
  service?: Pick<typeof speechService, 'getCatalog' | 'synthesize'>
} = {}) {
  const identify = deps.identifyCaller ?? identifyCaller
  const service = deps.service ?? speechService
  return new Elysia({ prefix: '/speech' })
  .onBeforeHandle(async ({ headers, request, set }) => {
    const { caller, error } = await identify(headers, request)
    if (error) { set.status = error.status; return error.body }
    if (caller.kind === 'account') {
      set.status = 403
      return { error: 'Speech requires the instance service credential' }
    }
  })
  .get('/', async ({ set }) => {
    try { return await service.getCatalog() } catch {
      set.status = 503
      return { error: 'Speech synthesis unavailable' }
    }
  }, { detail: { tags: ['Speech'], summary: 'List available speech voices and controls', description: 'Optional self-hosted TTS. Requires the instance service key; customer billing is not enabled for speech.' } })
  .post('/', async ({ body, set }) => {
    try {
      const audio = await service.synthesize(body)
      return new Response(new Uint8Array(audio).buffer, { headers: { 'Content-Type': 'audio/mpeg', 'Cache-Control': 'private, no-store' } })
    } catch (error) {
      set.status = error instanceof SpeechError ? error.status : 503
      return { error: error instanceof SpeechError ? error.message : 'Speech synthesis unavailable' }
    }
  }, {
    body: t.Object({
      text: t.String({ minLength: 1, maxLength: 500 }), language: t.String({ minLength: 2, maxLength: 35 }),
      voice: t.Optional(t.String({ maxLength: 100 })), rate: t.Optional(t.Number({ minimum: 0.75, maximum: 1.5 })),
    }),
    detail: { tags: ['Speech'], summary: 'Synthesize a short instruction as MP3', description: 'Plain text only. Native rate multiplier 0.75–1.5. Unsupported voices/locales return 422; busy returns 429; unavailable returns 503. Requires the instance service key.', responses: { 200: { description: 'MP3 audio', content: { 'audio/mpeg': { schema: { type: 'string', format: 'binary' } } } } } },
  })

}

export const speechRoutes = createSpeechRoutes()
