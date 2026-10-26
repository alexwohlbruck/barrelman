import { envString } from '../config/env'

export interface SpeechVoice { id: string; name: string; language: string }
export interface SpeechCatalog {
  model: string
  voices: SpeechVoice[]
  rate: { min: number; max: number; default: number; native: boolean }
  inflection: boolean
}
export interface SpeechRequest { text: string; language: string; voice?: string; rate?: number }

const LANGUAGES: Record<string, string> = {
  a: 'en-US', b: 'en-GB', e: 'es', f: 'fr', h: 'hi', i: 'it', j: 'ja', p: 'pt-BR', z: 'zh-CN',
}
const MAX_AUDIO_BYTES = 2 * 1024 * 1024

export class SpeechError extends Error {
  constructor(message: string, readonly status = 503) { super(message) }
}

export class SpeechService {
  private catalog: { value: SpeechCatalog; expires: number } | null = null
  private catalogPending: Promise<SpeechCatalog> | null = null
  private cache = new Map<string, Uint8Array>()
  private cacheBytes = 0
  private active = 0
  private pending = new Map<string, Promise<Uint8Array>>()

  constructor(
    private readonly host: () => string = () => envString('TTS_URL', ''),
    private readonly fetchFn: typeof fetch = fetch,
  ) {}

  private async upstream(path: string, init: RequestInit = {}) {
    const host = this.host().replace(/\/$/, '')
    if (!host) throw new SpeechError('Speech synthesis is disabled')
    const response = await this.fetchFn(`${host}${path}`, {
      ...init, signal: AbortSignal.timeout(3000),
    })
    if (!response.ok) throw new SpeechError('Speech engine unavailable')
    return response
  }

  async getCatalog(): Promise<SpeechCatalog> {
    if (this.catalog && this.catalog.expires > Date.now()) return this.catalog.value
    if (this.catalogPending) return this.catalogPending
    this.catalogPending = this.loadCatalog()
    try { return await this.catalogPending } finally { this.catalogPending = null }
  }

  private async loadCatalog(): Promise<SpeechCatalog> {
    const response = await this.upstream('/v1/audio/voices')
    const data = await response.json() as { voices?: unknown }
    if (!Array.isArray(data.voices)) throw new SpeechError('Invalid speech voice catalog')
    const voices = data.voices.filter((id): id is string => typeof id === 'string' && /^[a-z]{2}_[a-z0-9_]+$/.test(id) && !!LANGUAGES[id[0]])
      .map(id => ({ id, name: id.slice(3).replaceAll('_', ' '), language: LANGUAGES[id[0]]! }))
    if (!voices.length) throw new SpeechError('No speech voices available')
    const value: SpeechCatalog = {
      model: `kokoro:${envString('TTS_MODEL_REVISION', 'v1.0')}`,
      voices, rate: { min: 0.75, max: 1.5, default: 1, native: true }, inflection: false,
    }
    this.catalog = { value, expires: Date.now() + 60_000 }
    return value
  }

  async synthesize(request: SpeechRequest): Promise<Uint8Array> {
    if (!request.text.trim() || request.text.length > 500) throw new SpeechError('Text must contain 1–500 characters', 400)
    const rate = request.rate ?? 1
    if (!Number.isFinite(rate) || rate < 0.75 || rate > 1.5) throw new SpeechError('Rate must be between 0.75 and 1.5', 400)
    const catalog = await this.getCatalog()
    const language = request.language.toLowerCase()
    const base = language.split('-')[0]
    const eligible = catalog.voices.filter(voice => voice.language.toLowerCase().split('-')[0] === base)
    const voice = request.voice
      ? eligible.find(voice => voice.id === request.voice)
      : eligible.find(voice => voice.language.toLowerCase() === language) ?? eligible[0]
    if (!voice) throw new SpeechError('Unsupported speech voice or language', 422)
    const key = JSON.stringify([catalog.model, voice.id, language, rate, request.text])
    const cached = this.cache.get(key)
    if (cached) return cached
    const existing = this.pending.get(key)
    if (existing) return existing
    const pending = this.generate(request.text, voice.id, rate, key)
    this.pending.set(key, pending)
    try { return await pending } finally { this.pending.delete(key) }
  }

  private async generate(text: string, voice: string, rate: number, key: string): Promise<Uint8Array> {
    if (this.active >= 2) throw new SpeechError('Speech engine busy', 429)
    this.active++
    try {
      const response = await this.upstream('/v1/audio/speech', {
        method: 'POST', headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ model: 'kokoro', input: text, voice, speed: rate, response_format: 'mp3', stream: false, ssml: false, allow_voice_tags: false }),
      })
      if (!response.headers.get('content-type')?.startsWith('audio/')) throw new SpeechError('Invalid speech audio')
      const reader = response.body?.getReader()
      if (!reader) throw new SpeechError('Empty speech audio')
      const chunks: Uint8Array[] = []
      let size = 0
      while (true) {
        const { done, value } = await reader.read()
        if (done) break
        size += value.length
        if (size > MAX_AUDIO_BYTES) { await reader.cancel(); throw new SpeechError('Speech audio too large') }
        chunks.push(value)
      }
      if (!size) throw new SpeechError('Empty speech audio')
      const audio = new Uint8Array(size)
      let offset = 0
      for (const chunk of chunks) { audio.set(chunk, offset); offset += chunk.length }
      while (this.cache.size >= 128 || this.cacheBytes + size > 8 * 1024 * 1024) {
        const oldest = this.cache.keys().next().value!
        this.cacheBytes -= this.cache.get(oldest)!.length
        this.cache.delete(oldest)
      }
      this.cache.set(key, audio)
      this.cacheBytes += size
      return audio
    } finally { this.active-- }
  }
}

export const speechService = new SpeechService()
