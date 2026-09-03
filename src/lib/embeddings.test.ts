/**
 * Tests for the embedding client's behaviour on the request path.
 *
 * An instance with no Ollama answered every semantic search in exactly 30
 * seconds — the search finished in milliseconds and then waited for a vector
 * that was never coming, until the caller's own timeout fired and the request
 * failed outright. What is pinned here is that a query gives up quickly and
 * that a burst does not each pay for it.
 */
import { describe, test, expect, mock, beforeEach, afterEach } from 'bun:test'

let posts: Array<{ timeout?: number }> = []
let behaviour: 'fail' | 'ok' = 'fail'

mock.module('axios', () => ({
  default: {
    post: async (_url: string, _body: unknown, opts: { timeout?: number }) => {
      posts.push({ timeout: opts?.timeout })
      if (behaviour === 'fail') throw new Error('connect ECONNREFUSED')
      return { data: { embeddings: [[0.1, 0.2]] } }
    },
  },
}))

const { generateQueryEmbedding, generateEmbeddings, resetEmbeddingCircuit } =
  await import('./embeddings')

describe('query embeddings', () => {
  beforeEach(() => {
    posts = []
    behaviour = 'fail'
    resetEmbeddingCircuit()
  })
  afterEach(() => resetEmbeddingCircuit())

  test('gives up on a query in seconds, not half a minute', async () => {
    await expect(generateQueryEmbedding('canal st')).rejects.toThrow()

    expect(posts).toHaveLength(1)
    expect(posts[0].timeout).toBeLessThanOrEqual(5000)
  })

  test('a burst after an outage does not each pay the timeout', async () => {
    await expect(generateQueryEmbedding('one')).rejects.toThrow()
    await expect(generateQueryEmbedding('two')).rejects.toThrow()
    await expect(generateQueryEmbedding('three')).rejects.toThrow()

    // Only the first actually reached the network; the rest short-circuited.
    expect(posts).toHaveLength(1)
  })

  test('batch embedding still gets the long timeout', async () => {
    behaviour = 'ok'
    await generateEmbeddings(['a place'])

    expect(posts[0].timeout).toBe(30000)
  })

  test('a working service clears the outage', async () => {
    await expect(generateQueryEmbedding('down')).rejects.toThrow()
    resetEmbeddingCircuit()
    behaviour = 'ok'

    expect(await generateQueryEmbedding('up')).toEqual([0.1, 0.2])
  })
})
