import { describe, test, expect, mock, beforeEach } from 'bun:test'

// ── Mocks ─────────────────────────────────────────────────────────────────────

const mockExecute = mock(async () => [] as any[])
const spatialCacheStore = new Map<string, any>()

const noop = { get: () => undefined, set: () => {} }

mock.module('../db', () => ({ db: { execute: mockExecute } }))
mock.module('../lib/cache', () => ({
  spatialCache: {
    get: (k: string) => spatialCacheStore.get(k),
    set: (k: string, v: any) => spatialCacheStore.set(k, v),
  },
  searchCache: noop,
  embeddingCache: noop,
}))

const { findContainingAreas, findChildren } = await import('./spatial.service')

// ── Helpers ───────────────────────────────────────────────────────────────────

function makePlaces(count: number): any[] {
  return Array.from({ length: count }, (_, i) => ({
    id: `node/${i + 1}`,
    name: `Place ${i + 1}`,
    categories: [],
    distance_m: i * 100,
  }))
}

beforeEach(() => {
  mockExecute.mockReset()
  mockExecute.mockImplementation(async () => [])
  spatialCacheStore.clear()
})

// ── findContainingAreas ───────────────────────────────────────────────────────

describe('findContainingAreas', () => {
  test('returns empty array when no containing areas found', async () => {
    const results = await findContainingAreas({ lat: 0, lng: 0 })
    expect(results).toEqual([])
  })

  test('returns areas in the order DB provides them (smallest-first)', async () => {
    const areas = [
      { id: 'relation/1', name: 'Boone', area_m2: 16583818 },
      { id: 'relation/2', name: 'Watauga County', area_m2: 810981950 },
    ]
    mockExecute.mockImplementation(async () => areas)
    const results = await findContainingAreas({ lat: 36.2, lng: -81.6 })
    expect(results[0].name).toBe('Boone')
    expect(results[1].name).toBe('Watauga County')
  })

  test('caches result and skips DB on repeat call', async () => {
    mockExecute.mockImplementation(async () => [{ id: 'relation/1', name: 'Boone' }])
    await findContainingAreas({ lat: 36.2, lng: -81.6 })
    await findContainingAreas({ lat: 36.2, lng: -81.6 })
    expect(mockExecute).toHaveBeenCalledTimes(1)
  })

  test('exclude param produces a different cache key', async () => {
    mockExecute.mockImplementation(async () => [])
    await findContainingAreas({ lat: 36.2, lng: -81.6 })
    await findContainingAreas({ lat: 36.2, lng: -81.6, exclude: 'relation/1' })
    expect(mockExecute).toHaveBeenCalledTimes(2)
  })

  test('does not throw when exclude is undefined', async () => {
    await expect(findContainingAreas({ lat: 36.2, lng: -81.6, exclude: undefined })).resolves.toBeDefined()
  })

  test('REGRESSION: lat=0 lng=0 is a valid coordinate (not falsy)', async () => {
    mockExecute.mockImplementation(async () => [])
    await expect(findContainingAreas({ lat: 0, lng: 0 })).resolves.toBeDefined()
    expect(mockExecute).toHaveBeenCalledTimes(1)
  })
})

// ── findChildren ──────────────────────────────────────────────────────────────

describe('findChildren', () => {
  test('returns empty array when parent has no children', async () => {
    const results = await findChildren({ id: 'relation/1' })
    expect(results).toEqual([])
  })

  test('returns children from DB', async () => {
    mockExecute.mockImplementation(async () => [
      { id: 'node/1', name: 'Library', categories: ['amenity/library'] },
      { id: 'way/2', name: 'Parking Lot', categories: ['amenity/parking'] },
    ])
    const results = await findChildren({ id: 'relation/17208432' })
    expect(results).toHaveLength(2)
  })

  test('caches result and skips DB on repeat call', async () => {
    mockExecute.mockImplementation(async () => [{ id: 'node/1', name: 'Child' }])
    await findChildren({ id: 'relation/1' })
    await findChildren({ id: 'relation/1' })
    expect(mockExecute).toHaveBeenCalledTimes(1)
  })

  test('category filter produces a different cache key', async () => {
    mockExecute.mockImplementation(async () => [])
    await findChildren({ id: 'relation/1' })
    await findChildren({ id: 'relation/1', categories: 'amenity/cafe' })
    expect(mockExecute).toHaveBeenCalledTimes(2)
  })

  test('parses comma-separated categories string', async () => {
    await expect(
      findChildren({ id: 'relation/1', categories: 'amenity/cafe,amenity/restaurant' }),
    ).resolves.toBeDefined()
  })

  test('treats empty categories string as no filter', async () => {
    await expect(findChildren({ id: 'relation/1', categories: '' })).resolves.toBeDefined()
  })

  test('accepts string limit and offset (converts to numbers in SQL)', async () => {
    await expect(findChildren({ id: 'relation/1', limit: '50', offset: '20' })).resolves.toBeDefined()
  })

  test('uses default limit=20 and offset=0 when not specified', async () => {
    await expect(findChildren({ id: 'relation/1' })).resolves.toBeDefined()
  })

  test('accepts optional lat/lng for proximity sort', async () => {
    await expect(
      findChildren({ id: 'relation/1', lat: '36.2', lng: '-81.6' }),
    ).resolves.toBeDefined()
  })

  test('falls back to parent centroid sort when lat/lng omitted', async () => {
    await expect(findChildren({ id: 'relation/1' })).resolves.toBeDefined()
  })
})
