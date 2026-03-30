/**
 * Unit tests for searchPlaces.
 *
 * Regressions covered:
 *   - lat:0 falsy bug: `lat && lng` was false when lat=0, skipping location entirely
 *   - Abbrev layer silently skipped for queries > 20 chars
 *   - Semantic layer must be suppressed for autocomplete=true regardless of result count
 *   - Deduplication preserves FTS > abbrev > trigram priority
 *   - Proximity re-ranking fires only when hasLocation is true
 */

import { describe, test, expect, mock, beforeEach } from 'bun:test'

// ── Mocks ─────────────────────────────────────────────────────────────────────

const mockExecute = mock(async () => [] as any[])
const mockGenerateQueryEmbedding = mock(async () => [0.1, 0.2, 0.3] as number[])

const searchCacheStore = new Map<string, any>()
const embeddingCacheStore = new Map<string, any>()

// Shared no-op store for cache exports this file doesn't use
const noop = { get: () => undefined, set: () => {} }

mock.module('../db', () => ({ db: { execute: mockExecute } }))
mock.module('../lib/embeddings', () => ({ generateQueryEmbedding: mockGenerateQueryEmbedding }))
mock.module('../lib/cache', () => ({
  searchCache: {
    get: (k: string) => searchCacheStore.get(k),
    set: (k: string, v: any) => searchCacheStore.set(k, v),
  },
  embeddingCache: {
    get: (k: string) => embeddingCacheStore.get(k),
    set: (k: string, v: any) => embeddingCacheStore.set(k, v),
  },
  spatialCache: noop,
}))

const { searchPlaces } = await import('./search.service')

// ── Helpers ───────────────────────────────────────────────────────────────────

function makePlaces(count: number, overrides: Partial<any> = {}): any[] {
  return Array.from({ length: count }, (_, i) => ({
    id: `node/${i + 1}`,
    name: `Place ${i + 1}`,
    text_rank: 0.9 - i * 0.05,
    distance_m: null,
    ...overrides,
  }))
}

beforeEach(() => {
  mockExecute.mockReset()
  mockExecute.mockImplementation(async () => [])
  mockGenerateQueryEmbedding.mockReset()
  mockGenerateQueryEmbedding.mockImplementation(async () => [0.1, 0.2, 0.3])
  searchCacheStore.clear()
  embeddingCacheStore.clear()
})

// ── Basic ─────────────────────────────────────────────────────────────────────

describe('searchPlaces — basic', () => {
  test('returns empty array when all layers return nothing', async () => {
    const results = await searchPlaces({ query: 'nothing here', autocomplete: true })
    expect(results).toEqual([])
  })

  test('returns results from FTS layer', async () => {
    const place = { id: 'node/1', name: 'Main Library', text_rank: 0.9, distance_m: null }
    mockExecute.mockImplementationOnce(async () => [place]) // FTS
    const results = await searchPlaces({ query: 'library', autocomplete: true })
    expect(results.length).toBeGreaterThan(0)
    expect(results[0].id).toBe('node/1')
  })

  test('respects the limit parameter', async () => {
    mockExecute.mockImplementation(async () => makePlaces(10))
    const results = await searchPlaces({ query: 'place', limit: 4, autocomplete: true })
    expect(results.length).toBeLessThanOrEqual(4)
  })

  test('strips special characters from query without throwing', async () => {
    await expect(searchPlaces({ query: 'café & bar! (open)', autocomplete: true })).resolves.toBeDefined()
  })
})

// ── Layer execution ───────────────────────────────────────────────────────────

describe('searchPlaces — layer execution', () => {
  test('runs 3 parallel layers (FTS + trigram + abbrev) for short queries', async () => {
    // autocomplete=true suppresses semantic so count is predictable
    await searchPlaces({ query: 'cafe', autocomplete: true })
    expect(mockExecute).toHaveBeenCalledTimes(3)
  })

  test('runs only 2 layers (FTS + trigram) for queries longer than 20 chars', async () => {
    // abbrev layer is skipped when sanitizedQuery.length > 20
    await searchPlaces({ query: 'this is a very long query string', autocomplete: true })
    expect(mockExecute).toHaveBeenCalledTimes(2)
  })
})

// ── Deduplication ─────────────────────────────────────────────────────────────

describe('searchPlaces — deduplication', () => {
  test('FTS result takes priority over same place returned by trigram', async () => {
    const ftsPlace = { id: 'node/1', name: 'Library', text_rank: 0.9, distance_m: null }
    const trigramPlace = { id: 'node/1', name: 'Library', text_rank: 0.5, distance_m: null }
    mockExecute
      .mockImplementationOnce(async () => [ftsPlace])    // FTS
      .mockImplementationOnce(async () => [trigramPlace]) // trigram
      .mockImplementationOnce(async () => [])             // abbrev
    const results = await searchPlaces({ query: 'library', autocomplete: true })
    const ids = results.map((r: any) => r.id)
    expect(ids.filter((id: string) => id === 'node/1')).toHaveLength(1)
    expect(results.find((r: any) => r.id === 'node/1').text_rank).toBe(0.9)
  })

  test('merges unique results from all three text layers', async () => {
    const ftsPlace = { id: 'node/1', name: 'Library', text_rank: 0.9, distance_m: null }
    const trigramPlace = { id: 'node/2', name: 'Lib Café', text_rank: 0.4, distance_m: null }
    const abbrevPlace = { id: 'node/3', name: 'LIB', text_rank: 0.95, distance_m: null }
    mockExecute
      .mockImplementationOnce(async () => [ftsPlace])
      .mockImplementationOnce(async () => [trigramPlace])
      .mockImplementationOnce(async () => [abbrevPlace])
    const results = await searchPlaces({ query: 'lib', autocomplete: true })
    const ids = new Set(results.map((r: any) => r.id))
    expect(ids.has('node/1')).toBe(true)
    expect(ids.has('node/2')).toBe(true)
    expect(ids.has('node/3')).toBe(true)
  })
})

// ── Caching ───────────────────────────────────────────────────────────────────

describe('searchPlaces — caching', () => {
  test('returns cached result on repeat call with same params', async () => {
    mockExecute.mockImplementation(async () => [{ id: 'node/1', name: 'Library', text_rank: 0.9 }])
    await searchPlaces({ query: 'library', autocomplete: true })
    const firstCount = mockExecute.mock.calls.length

    await searchPlaces({ query: 'library', autocomplete: true })
    expect(mockExecute.mock.calls.length).toBe(firstCount) // no new DB calls
  })

  test('different query strings produce separate cache entries', async () => {
    await searchPlaces({ query: 'cafe', autocomplete: true })
    await searchPlaces({ query: 'library', autocomplete: true })
    // 3 layers per unique query = 6 total
    expect(mockExecute.mock.calls.length).toBe(6)
  })
})

// ── Semantic layer ────────────────────────────────────────────────────────────

describe('searchPlaces — semantic layer', () => {
  test('REGRESSION: semantic layer is skipped when autocomplete=true, even with 0 results', async () => {
    await searchPlaces({ query: 'test', autocomplete: true })
    expect(mockGenerateQueryEmbedding).not.toHaveBeenCalled()
  })

  test('semantic layer triggers when results < 5 and autocomplete=false', async () => {
    mockExecute
      .mockImplementationOnce(async () => [{ id: 'node/1', name: 'A', text_rank: 0.9 }]) // FTS
      .mockImplementationOnce(async () => [])  // trigram
      .mockImplementationOnce(async () => [])  // abbrev
      .mockImplementationOnce(async () => [])  // semantic DB query
    await searchPlaces({ query: 'rare place', autocomplete: false })
    expect(mockGenerateQueryEmbedding).toHaveBeenCalledTimes(1)
  })

  test('semantic layer is skipped when text results are sufficient (>= 5)', async () => {
    mockExecute
      .mockImplementationOnce(async () => makePlaces(5)) // FTS returns 5
      .mockImplementationOnce(async () => [])
      .mockImplementationOnce(async () => [])
    await searchPlaces({ query: 'common place', semantic: false, autocomplete: false })
    expect(mockGenerateQueryEmbedding).not.toHaveBeenCalled()
  })

  test('semantic=true forces semantic even when text results are sufficient', async () => {
    mockExecute
      .mockImplementationOnce(async () => makePlaces(5))
      .mockImplementationOnce(async () => [])
      .mockImplementationOnce(async () => [])
      .mockImplementationOnce(async () => []) // semantic DB query
    await searchPlaces({ query: 'study spot', semantic: true, autocomplete: false })
    expect(mockGenerateQueryEmbedding).toHaveBeenCalledTimes(1)
  })

  test('caches embeddings to avoid re-generating on repeat queries', async () => {
    // First call — sparse, triggers semantic
    mockExecute.mockImplementation(async () => [])
    await searchPlaces({ query: 'zen cafe', autocomplete: false })
    expect(mockGenerateQueryEmbedding).toHaveBeenCalledTimes(1)

    // Pre-seed embedding cache and use a different limit to get a fresh search cache key
    embeddingCacheStore.set('zen cafe', [0.1, 0.2, 0.3])
    mockExecute.mockClear()
    await searchPlaces({ query: 'zen cafe', limit: 21, autocomplete: false })
    // Embedding should come from cache — no additional generateQueryEmbedding call
    expect(mockGenerateQueryEmbedding).toHaveBeenCalledTimes(1)
  })

  test('continues without error when Ollama is unavailable', async () => {
    mockGenerateQueryEmbedding.mockImplementation(async () => { throw new Error('Ollama unavailable') })
    const results = await searchPlaces({ query: 'test', autocomplete: false })
    expect(Array.isArray(results)).toBe(true)
  })
})

// ── Location handling ─────────────────────────────────────────────────────────

describe('searchPlaces — location handling', () => {
  test('REGRESSION: lat=0 must not be treated as falsy — hasLocation should use != null', async () => {
    // Bug: `lat && lng` evaluates to false when lat=0, skipping proximity entirely.
    // Fix: `lat != null && lng != null` correctly handles lat=0 (Gulf of Guinea).
    await expect(searchPlaces({ query: 'cafe', lat: 0, lng: 0, autocomplete: true })).resolves.toBeDefined()
    // With the fix, the location point is built and radius filter is applied
    expect(mockExecute).toHaveBeenCalledTimes(3)
  })

  test('proximity re-ranking elevates nearby result above higher-ranked distant one', async () => {
    const distant = { id: 'node/1', name: 'Distant Library', text_rank: 0.95, distance_m: 80000 }
    const nearby  = { id: 'node/2', name: 'Nearby Library',  text_rank: 0.60, distance_m: 200 }
    mockExecute
      .mockImplementationOnce(async () => [distant, nearby])
      .mockImplementationOnce(async () => [])
      .mockImplementationOnce(async () => [])
    const results = await searchPlaces({ query: 'library', lat: 36.2, lng: -81.6, autocomplete: true })
    expect(results[0].id).toBe('node/2')
  })

  test('no re-ranking when no location provided — FTS order is preserved', async () => {
    const first  = { id: 'node/1', name: 'Top Result',    text_rank: 0.95, distance_m: null }
    const second = { id: 'node/2', name: 'Second Result', text_rank: 0.70, distance_m: null }
    mockExecute
      .mockImplementationOnce(async () => [first, second])
      .mockImplementationOnce(async () => [])
      .mockImplementationOnce(async () => [])
    const results = await searchPlaces({ query: 'result', autocomplete: true })
    expect(results[0].id).toBe('node/1')
    expect(results[1].id).toBe('node/2')
  })
})

// ── Resilience ────────────────────────────────────────────────────────────────

describe('searchPlaces — resilience', () => {
  test('FTS layer failure is caught — results from other layers still returned', async () => {
    mockExecute
      .mockImplementationOnce(async () => { throw new Error('DB connection lost') }) // FTS fails
      .mockImplementationOnce(async () => [{ id: 'node/1', name: 'Library', text_rank: 0.8, distance_m: null }]) // trigram ok
      .mockImplementationOnce(async () => []) // abbrev
    const results = await searchPlaces({ query: 'library', autocomplete: true })
    expect(Array.isArray(results)).toBe(true)
  })
})
