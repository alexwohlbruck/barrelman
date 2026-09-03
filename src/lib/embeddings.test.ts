/**
 * Tests for the embedding client's behaviour on the request path.
 *
 * An instance with no Ollama answered every semantic search in exactly 30
 * seconds — the search finished in milliseconds and then waited for a vector
 * that was never coming, until the caller's own timeout fired and the request
 * failed outright.
 *
 * These target `generateEmbeddings` and `embeddingCircuit` rather than
 * `generateQueryEmbedding`, which `search.service.test.ts` module-mocks. Bun's
 * `mock.module` is process-global, so the wrapper is not observable from here
 * in a full run — the pieces it is built from are.
 */
import { describe, test, expect, beforeEach } from 'bun:test'
import {
  generateEmbeddings,
  embeddingCircuit,
  QUERY_EMBED_TIMEOUT_MS,
  type EmbeddingPoster,
} from './embeddings'

let posts: Array<{ timeout?: number }> = []

const record: EmbeddingPoster = async (_u, _b, opts) => {
  posts.push({ timeout: opts?.timeout })
  return { data: { embeddings: [[0.1, 0.2]] } }
}

describe('embedding timeouts', () => {
  beforeEach(() => {
    posts = []
    embeddingCircuit.reset()
  })

  test('a query gives up in seconds, not half a minute', () => {
    // The number that mattered: 30s on the request path is an outage, not a
    // timeout, because callers give up first and the request fails outright.
    expect(QUERY_EMBED_TIMEOUT_MS).toBeLessThanOrEqual(5000)
  })

  test('batch embedding keeps the long timeout, which runs offline', async () => {
    await generateEmbeddings(['a place'], undefined, record)

    expect(posts[0].timeout).toBe(30000)
  })

  test('passes through whatever timeout it is given', async () => {
    await generateEmbeddings(['a place'], QUERY_EMBED_TIMEOUT_MS, record)

    expect(posts[0].timeout).toBe(QUERY_EMBED_TIMEOUT_MS)
  })
})

describe('the outage memory', () => {
  beforeEach(() => embeddingCircuit.reset())

  test('is shut until something fails', () => {
    expect(embeddingCircuit.isOpen()).toBe(false)
  })

  test('opens on a refusal, so a burst does not each pay the timeout', () => {
    embeddingCircuit.trip()

    expect(embeddingCircuit.isOpen()).toBe(true)
  })

  test('a working service closes it again', () => {
    embeddingCircuit.trip()
    embeddingCircuit.reset()

    expect(embeddingCircuit.isOpen()).toBe(false)
  })
})
