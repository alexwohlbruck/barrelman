/**
 * Unit tests for searchPlaces.
 *
 * Regressions covered:
 *   - lat:0 falsy bug: `lat && lng` was false when lat=0, skipping location entirely
 *   - Abbrev layer silently skipped for queries > 20 chars
 *   - Semantic layer must be suppressed for autocomplete=true regardless of result count
 *   - Deduplication preserves abbrev/codes > FTS > trigram priority
 *   - Proximity re-ranking fires only when hasLocation is true
 */

import { describe, test, expect, mock, beforeEach } from 'bun:test'
import * as realCache from '../lib/cache'

// ── Mocks ─────────────────────────────────────────────────────────────────────

const mockExecute = mock(async () => [] as any[])
const mockGenerateQueryEmbedding = mock(async () => [0.1, 0.2, 0.3] as number[])

const searchCacheStore = new Map<string, any>()
const embeddingCacheStore = new Map<string, any>()

// Shared no-op store for cache exports this file doesn't use
const noop = { get: () => undefined, set: () => {} }

// Spread the real module: a bare replacement drops `connection` and the
// ensure*Schema helpers for every test file loaded after this one, which
// fails whichever suite imports them next rather than this one.
const actualDb = await import('../db')

mock.module('../db', () => ({
  ...actualDb, db: { execute: mockExecute } }))
mock.module('../lib/embeddings', () => ({ generateQueryEmbedding: mockGenerateQueryEmbedding }))
// `mock.module` is process-global and replaces the module wholesale for every
// test file in the run, so spread the real exports and override only what this
// file cares about — otherwise a later file importing e.g. `isochroneCache`
// blows up with "Export named ... not found".
mock.module('../lib/cache', () => ({
  ...realCache,
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
  test('runs 6 parallel layers (FTS + trigram + codes + nameAbbrev + transit routes/stops) for 5+ char queries', async () => {
    // autocomplete=true suppresses semantic so count is predictable
    await searchPlaces({ query: 'coffee', autocomplete: true })
    expect(mockExecute).toHaveBeenCalledTimes(6)
  })

  test('skips trigram for short queries (≤4 chars) — 5 layers only', async () => {
    await searchPlaces({ query: 'cafe', autocomplete: true })
    expect(mockExecute).toHaveBeenCalledTimes(5)
  })

  test('skips codes/abbrev for queries longer than 20 chars — FTS + trigram + transit', async () => {
    // abbrev layer is skipped when sanitizedQuery.length > 20
    await searchPlaces({ query: 'this is a very long query string', autocomplete: true })
    expect(mockExecute).toHaveBeenCalledTimes(4)
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

  test('abbreviation/code result takes priority over same place returned by FTS', async () => {
    const ftsPlace = { id: 'node/1', name: 'University', text_rank: 0.7, distance_m: null }
    const abbrevPlace = { id: 'node/1', name: 'University', text_rank: 0.95, distance_m: null }
    mockExecute
      .mockImplementationOnce(async () => [ftsPlace])    // FTS
      .mockImplementationOnce(async () => [])             // trigram
      .mockImplementationOnce(async () => [abbrevPlace])  // abbrev
    const results = await searchPlaces({ query: 'uncc', autocomplete: true })
    const ids = results.map((r: any) => r.id)
    expect(ids.filter((id: string) => id === 'node/1')).toHaveLength(1)
    // Abbrev result should win since merge order is abbrev > FTS > trigram
    expect(results.find((r: any) => r.id === 'node/1').text_rank).toBe(0.95)
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
    await searchPlaces({ query: 'coffee', autocomplete: true })
    await searchPlaces({ query: 'library', autocomplete: true })
    // 6 db calls per unique query = 12 total
    expect(mockExecute.mock.calls.length).toBe(12)
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
    await expect(searchPlaces({ query: 'coffee', lat: 0, lng: 0, autocomplete: true })).resolves.toBeDefined()
    // With the fix, the location point is built and the layers run. Autocomplete
    // with coordinates takes the local fast path: FTS + codes + abbrev (trigram
    // is skipped) + the two transit layers, then — because the mocks return
    // nothing — the global retry adds FTS + trigram. lat=0 being treated as
    // falsy would have skipped the local path entirely and run the global
    // shape instead.
    expect(mockExecute).toHaveBeenCalledTimes(7)
  })

  test('non-autocomplete search keeps the 4-layer global shape', async () => {
    // Guards the fast path against leaking into submitted searches: those must
    // still run FTS + trigram + codes + abbrev + transit globally, with no
    // local retry — plus the semantic layer, which fires here because the mocks
    // return nothing and is suppressed for autocomplete.
    await expect(searchPlaces({ query: 'coffee', lat: 35.22, lng: -80.84 })).resolves.toBeDefined()
    expect(mockExecute).toHaveBeenCalledTimes(7)
    expect(mockGenerateQueryEmbedding).toHaveBeenCalled()
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

// ── Intersection search ──────────────────────────────────────────────────────

describe('searchPlaces — autocomplete fast path', () => {
  test('single-character query runs ONLY the exact transit-line lookup', async () => {
    // A 1-char prefix matches a sizeable fraction of the tsvector index and
    // measured ~20s uncached — but "7" or "q" is how a rider names a line, and
    // an exact short-name match on gtfs_routes is indexed and instant.
    await expect(searchPlaces({ query: 'd', lat: 35.22, lng: -80.84, autocomplete: true }))
      .resolves.toEqual([])
    expect(mockExecute).toHaveBeenCalledTimes(1)
  })

  test('single-character query surfaces a line by exact short name', async () => {
    const rawRouteRow = {
      id: 'transit-route/mta:7', kind: 'transit_route', name: 'Flushing Local',
      feed_id: 'mta', feed_onestop_id: 'f-dr5r-mta', route_id: '7',
      route_short_name: '7', route_long_name: 'Flushing Local', route_type: 1,
      geometry: { type: 'Point', coordinates: [-73.9, 40.75] },
      text_rank: 0.95, distance_m: 5400,
    }
    mockExecute.mockImplementationOnce(async () => [rawRouteRow])
    const results = await searchPlaces({ query: '7', lat: 40.75, lng: -73.98, autocomplete: true })
    expect(mockExecute).toHaveBeenCalledTimes(1)
    expect(results).toHaveLength(1)
    expect(results[0].kind).toBe('transit_route')
    expect(results[0].transit).toMatchObject({ feedId: 'mta', routeId: '7', shortName: '7' })
  })

  test('single-character query with a category filter stays empty — no scan', async () => {
    await expect(
      searchPlaces({ query: 'd', categories: ['amenity/cafe'], autocomplete: true }),
    ).resolves.toEqual([])
    expect(mockExecute).not.toHaveBeenCalled()
  })

  test('the gate is autocomplete-only — a submitted 1-char search still runs', async () => {
    await expect(searchPlaces({ query: 'd', lat: 35.22, lng: -80.84 })).resolves.toBeDefined()
    expect(mockExecute).toHaveBeenCalled()
  })

  test('local pass skips trigram; codes and abbrev layers still run', async () => {
    const local = makePlaces(6)
    mockExecute
      .mockImplementationOnce(async () => local) // FTS (local)
      .mockImplementationOnce(async () => [])    // codes
      .mockImplementationOnce(async () => [])    // nameAbbrev
    const results = await searchPlaces({ query: 'sycamore', lat: 35.22, lng: -80.84, autocomplete: true })
    // 6 local hits clears AUTOCOMPLETE_FALLBACK_MIN, so no global retry.
    expect(mockExecute).toHaveBeenCalledTimes(5)
    expect(results).toHaveLength(6)
  })

  test('a zero-result local pass triggers a global retry', async () => {
    const faraway = { id: 'node/global', name: 'Faraway Match', text_rank: 0.95, distance_m: 900000 }
    mockExecute
      .mockImplementationOnce(async () => [])        // FTS (local) — nothing
      .mockImplementationOnce(async () => [])        // codes
      .mockImplementationOnce(async () => [])        // nameAbbrev
      .mockImplementationOnce(async () => [faraway]) // FTS (global retry)
      .mockImplementationOnce(async () => [])        // trigram (global retry)
    const ids = (await searchPlaces({ query: 'sycamore', lat: 35.22, lng: -80.84, autocomplete: true }))
      .map((r: any) => r.id)
    expect(mockExecute).toHaveBeenCalledTimes(7)
    expect(ids).toContain('node/global')
  })

  test('a single local hit is a match, not a miss — no global retry', async () => {
    // The retry is dominated by the global trigram layer (240-330ms measured).
    // A precise query matching exactly one place is the success case; retrying
    // it just to pad the list with fuzzy near-misses cost ~250ms on every
    // specific query.
    const nearby = { id: 'node/local', name: 'Divine Barrel Brewing', text_rank: 0.9, distance_m: 300 }
    mockExecute
      .mockImplementationOnce(async () => [nearby]) // FTS (local)
      .mockImplementationOnce(async () => [])       // codes
      .mockImplementationOnce(async () => [])       // nameAbbrev
    const ids = (await searchPlaces({ query: 'divine barrel', lat: 35.22, lng: -80.84, autocomplete: true }))
      .map((r: any) => r.id)
    expect(mockExecute).toHaveBeenCalledTimes(5)
    expect(ids).toEqual(['node/local'])
  })

  test('address-intent queries skip the global retry — Pelias covers them', async () => {
    // A leading digit means the user is typing an address. Pelias answers those
    // in <10ms in parallel; a global POI scan for an address-shaped string
    // costs ~250ms and returns street rows Pelias already supplies.
    mockExecute
      .mockImplementationOnce(async () => []) // FTS (local)
      .mockImplementationOnce(async () => []) // codes
      .mockImplementationOnce(async () => []) // nameAbbrev
    await searchPlaces({ query: '1600 e 7th st', lat: 35.22, lng: -80.84, autocomplete: true })
    expect(mockExecute).toHaveBeenCalledTimes(5)
  })

  test('short prefixes never trigger the global retry', async () => {
    // "syc" is under AUTOCOMPLETE_FALLBACK_MIN_QUERY: matching it globally is
    // the exact scan the fast path exists to avoid.
    await searchPlaces({ query: 'syc', lat: 35.22, lng: -80.84, autocomplete: true })
    expect(mockExecute).toHaveBeenCalledTimes(5)
  })

  test('autocomplete without coordinates falls back to the global shape', async () => {
    // No viewport means no box to bound the scan, so the fast path can't apply.
    await searchPlaces({ query: 'sycamore', autocomplete: true })
    expect(mockExecute).toHaveBeenCalledTimes(6)
  })

  test('candidate pool is trimmed to limit after the proximity re-rank', async () => {
    // The local pass over-fetches so the re-rank can see the whole candidate
    // set; the caller must still get exactly `limit` rows back.
    const pool = Array.from({ length: 60 }, (_, i) => ({
      id: `node/${i}`, name: `Place ${i}`, text_rank: 0.5, distance_m: (60 - i) * 100,
    }))
    mockExecute
      .mockImplementationOnce(async () => pool)
      .mockImplementationOnce(async () => [])
      .mockImplementationOnce(async () => [])
    const results = await searchPlaces({ query: 'sycamore', lat: 35.22, lng: -80.84, limit: 10, autocomplete: true })
    expect(results).toHaveLength(10)
    // Re-ranked by proximity: the nearest (last generated) must lead.
    expect(results[0].id).toBe('node/59')
  })
})

describe('searchPlaces — intersections', () => {
  test('intersection result (osm_type X) is returned alongside regular places', async () => {
    const intersection = {
      id: 'intersection/42', osm_type: 'X', name: 'Trade St & Tryon St',
      categories: ['highway/intersection'], text_rank: 0.7, distance_m: 500,
    }
    const poi = {
      id: 'node/1', osm_type: 'N', name: 'Starbucks',
      categories: ['amenity/cafe'], text_rank: 0.9, distance_m: 200,
    }
    mockExecute
      .mockImplementationOnce(async () => [poi, intersection]) // FTS
      .mockImplementationOnce(async () => [])                  // trigram
      .mockImplementationOnce(async () => [])                  // codes
      .mockImplementationOnce(async () => [])                  // nameAbbrev
    const results = await searchPlaces({ query: 'trade and tryon', lat: 35.22, lng: -80.84, autocomplete: true })
    const ids = results.map((r: any) => r.id)
    expect(ids).toContain('intersection/42')
    expect(ids).toContain('node/1')
  })

  test('query with & is sanitized to spaces without throwing', async () => {
    await expect(
      searchPlaces({ query: 'trade & tryon', autocomplete: true }),
    ).resolves.toBeDefined()
  })

  test('intersection deduplication works — same intersection from FTS and trigram appears once', async () => {
    const intersection = {
      id: 'intersection/42', name: 'Trade St & Tryon St',
      categories: ['highway/intersection'], text_rank: 0.7, distance_m: 500,
    }
    mockExecute
      .mockImplementationOnce(async () => [intersection])                        // FTS
      .mockImplementationOnce(async () => [{ ...intersection, text_rank: 0.5 }]) // trigram
      .mockImplementationOnce(async () => [])                                    // codes
      .mockImplementationOnce(async () => [])                                    // nameAbbrev
    const results = await searchPlaces({ query: 'trade tryon', autocomplete: true })
    const matches = results.filter((r: any) => r.id === 'intersection/42')
    expect(matches).toHaveLength(1)
    expect(matches[0].text_rank).toBe(0.7)
  })
})

// ── Transit layers ────────────────────────────────────────────────────────────

describe('searchPlaces — transit layers', () => {
  // Raw SQL row shapes as the transit layer queries project them; the service
  // adapts them into hits with a nested `transit` object.
  const rawRouteRow = {
    id: 'transit-route/mta:A', kind: 'transit_route', name: 'Eighth Avenue Express',
    feed_id: 'mta', feed_onestop_id: 'f-dr5r-mta', route_id: 'A',
    route_short_name: 'A', route_long_name: 'Eighth Avenue Express', route_type: 1,
    route_color: '0039A6', route_text_color: 'FFFFFF', agency_name: 'MTA',
    geometry: { type: 'Point', coordinates: [-73.98, 40.75] },
    text_rank: 0.95, distance_m: 1200,
  }
  const rawStopRow = {
    id: 'transit-stop/mta:s1', kind: 'transit_stop', name: 'Whitlock Av',
    feed_id: 'mta', feed_onestop_id: 'f-dr5r-mta', stop_id: 's1', location_type: 1,
    min_route_type: 1,
    geometry: { type: 'Point', coordinates: [-73.886, 40.826] },
    text_rank: 0.8, distance_m: 900,
  }

  // Global autocomplete-without-coords shape: FTS, trigram, codes, abbrev,
  // transit routes, transit stops — Onces below follow that order.
  const queue = (perLayer: Record<number, any[]>) => {
    for (let i = 0; i < 6; i++) {
      const rows = perLayer[i] ?? []
      mockExecute.mockImplementationOnce(async () => rows)
    }
  }

  test('a GTFS line hit is adapted and surfaces with its transit ids', async () => {
    queue({ 4: [rawRouteRow] })
    const results = await searchPlaces({ query: 'eighth avenue', autocomplete: true })
    expect(results).toHaveLength(1)
    const hit = results[0]
    expect(hit.kind).toBe('transit_route')
    expect(hit.osm_type).toBeNull()
    expect(hit.categories).toEqual(['transit/route/subway'])
    expect(hit.transit).toMatchObject({
      feedId: 'mta', feedOnestopId: 'f-dr5r-mta', routeId: 'A',
      shortName: 'A', longName: 'Eighth Avenue Express', mode: 'subway',
      color: '0039A6', agency: 'MTA',
    })
  })

  test('an OSM route relation duplicated by a GTFS line hit is dropped', async () => {
    const relation = {
      id: 'relation/9', osm_type: 'R', name: 'Eighth Avenue Express',
      tags: { type: 'route', route: 'subway', ref: 'A' }, categories: [],
      geometry: { type: 'Point', coordinates: [-73.97, 40.74] },
      text_rank: 0.9, distance_m: 1100,
    }
    queue({ 0: [relation], 4: [rawRouteRow] })
    const results = await searchPlaces({ query: 'eighth avenue', autocomplete: true })
    const ids = results.map((r: any) => r.id)
    expect(ids).toContain('transit-route/mta:A')
    expect(ids).not.toContain('relation/9')
  })

  test('a GTFS stop hit surfaces with stop ids and a mode category', async () => {
    queue({ 5: [rawStopRow] })
    const results = await searchPlaces({ query: 'whitlock', autocomplete: true })
    expect(results).toHaveLength(1)
    expect(results[0].kind).toBe('transit_stop')
    expect(results[0].categories).toEqual(['transit/stop/subway'])
    expect(results[0].transit).toMatchObject({ feedId: 'mta', stopId: 's1', mode: 'subway' })
  })

  test('a GTFS stop duplicated by a same-name OSM station is dropped', async () => {
    const station = {
      id: 'node/5', osm_type: 'N', name: 'Whitlock Avenue',
      categories: ['railway/station'], tags: {},
      geometry: { type: 'Point', coordinates: [-73.8861, 40.8262] },
      text_rank: 0.85, distance_m: 950,
    }
    queue({ 0: [station], 5: [rawStopRow] })
    const results = await searchPlaces({ query: 'whitlock', autocomplete: true })
    const ids = results.map((r: any) => r.id)
    expect(ids).toContain('node/5')
    expect(ids).not.toContain('transit-stop/mta:s1')
  })

  test('transit layers are skipped when category or tag filters apply', async () => {
    await searchPlaces({ query: 'coffee', categories: ['amenity/cafe'], autocomplete: true })
    // FTS + trigram + codes + abbrev only — no transit calls.
    expect(mockExecute).toHaveBeenCalledTimes(4)
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
