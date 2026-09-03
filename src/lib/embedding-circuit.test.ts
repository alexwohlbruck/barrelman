/**
 * Tests for the embedding service's timeouts and outage memory.
 *
 * An instance with no Ollama answered every semantic search in exactly 30
 * seconds — the search finished in milliseconds and then waited for a vector
 * that was never coming, until the caller's own timeout fired and the request
 * failed outright.
 *
 * These live apart from `embeddings.ts` because `search.service.test.ts`
 * module-mocks that file, and bun's `mock.module` is process-global: a suite
 * importing a mocked module is at the mercy of file order, which is how the
 * 0.2.7 release build died.
 */
import { describe, test, expect, beforeEach } from 'bun:test'
import {
  QUERY_EMBED_TIMEOUT_MS,
  BATCH_EMBED_TIMEOUT_MS,
  embeddingCircuit,
} from './embedding-circuit'

describe('embedding timeouts', () => {
  test('a query gives up in seconds, not half a minute', () => {
    // The number that mattered: 30s on the request path is an outage, not a
    // timeout, because callers give up first and the request fails outright.
    expect(QUERY_EMBED_TIMEOUT_MS).toBeLessThanOrEqual(5000)
  })

  test('batch embedding keeps the long timeout, which runs offline', () => {
    expect(BATCH_EMBED_TIMEOUT_MS).toBe(30000)
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
