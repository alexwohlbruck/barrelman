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

const { reverseGeocode, reverseGeocodePlaces } = await import('./geocode.service')

// ── Helpers ───────────────────────────────────────────────────────────────────

function adminRow(name: string, level: number) {
  return { id: `relation/${level}`, name, admin_level: level, area_m2: 1000 * level }
}

/** Feed db.execute a scripted result per call, in call order. */
function queueDbResults(...results: any[][]) {
  let i = 0
  mockExecute.mockImplementation(async () => results[i++] ?? [])
}

/** Stub the Pelias HTTP call with the given GeoJSON features. */
function stubPelias(features: any[]) {
  globalThis.fetch = mock(async () => ({
    ok: true,
    json: async () => ({ features }),
  })) as any
}

function osmFeature(overrides: Record<string, any> = {}) {
  return {
    geometry: { type: 'Point', coordinates: [-80.8431, 35.2271] },
    properties: {
      gid: 'openstreetmap:venue:node/8415199022',
      source: 'openstreetmap',
      layer: 'venue',
      name: 'Industry',
      ...overrides,
    },
  }
}

function addressFeature(overrides: Record<string, any> = {}) {
  return {
    geometry: { type: 'Point', coordinates: [-80.8431, 35.2271] },
    properties: {
      gid: 'openaddresses:address:us/nc/mecklenburg:51ed4905',
      source: 'openaddresses',
      layer: 'address',
      housenumber: '105',
      street: 'South Tryon Street',
      locality: 'Charlotte',
      ...overrides,
    },
  }
}

const originalFetch = globalThis.fetch

beforeEach(() => {
  mockExecute.mockReset()
  mockExecute.mockImplementation(async () => [])
  spatialCacheStore.clear()
  globalThis.fetch = originalFetch
})

// ── Tests ─────────────────────────────────────────────────────────────────────

describe('reverseGeocode — address mapping', () => {
  test('returns empty address and hierarchy when no boundaries found', async () => {
    const result = await reverseGeocode(0, 0)
    expect(result.address).toEqual({})
    expect(result.hierarchy).toEqual([])
  })

  test('maps admin_level 8 to city', async () => {
    mockExecute.mockImplementation(async () => [adminRow('Boone', 8)])
    const { address } = await reverseGeocode(36.2, -81.6)
    expect(address.city).toBe('Boone')
  })

  test('maps admin_level 6 to county', async () => {
    mockExecute.mockImplementation(async () => [adminRow('Watauga County', 6)])
    const { address } = await reverseGeocode(36.2, -81.6)
    expect(address.county).toBe('Watauga County')
  })

  test('maps admin_level 4 to state', async () => {
    mockExecute.mockImplementation(async () => [adminRow('North Carolina', 4)])
    const { address } = await reverseGeocode(36.2, -81.6)
    expect(address.state).toBe('North Carolina')
  })

  test('maps admin_level 2 to country', async () => {
    mockExecute.mockImplementation(async () => [adminRow('United States', 2)])
    const { address } = await reverseGeocode(36.2, -81.6)
    expect(address.country).toBe('United States')
  })

  test('builds a full address from a complete hierarchy', async () => {
    mockExecute.mockImplementation(async () => [
      adminRow('Boone', 8),
      adminRow('Watauga County', 6),
      adminRow('North Carolina', 4),
      adminRow('United States', 2),
    ])
    const { address } = await reverseGeocode(36.2, -81.6)
    expect(address.city).toBe('Boone')
    expect(address.county).toBe('Watauga County')
    expect(address.state).toBe('North Carolina')
    expect(address.country).toBe('United States')
  })

  test('first match wins — inner admin level does not get overwritten', async () => {
    mockExecute.mockImplementation(async () => [
      adminRow('Inner District', 9),  // >= 8 → maps to city first
      adminRow('Outer City', 8),       // city already set — skipped
    ])
    const { address } = await reverseGeocode(36.2, -81.6)
    expect(address.city).toBe('Inner District')
  })

  test('missing levels produce undefined keys (no phantom values)', async () => {
    mockExecute.mockImplementation(async () => [adminRow('Boone', 8)])
    const { address } = await reverseGeocode(36.2, -81.6)
    expect(address.county).toBeUndefined()
    expect(address.state).toBeUndefined()
    expect(address.country).toBeUndefined()
  })
})

describe('reverseGeocode — hierarchy passthrough', () => {
  test('returns all raw boundary rows in hierarchy array', async () => {
    mockExecute.mockImplementation(async () => [
      adminRow('Boone', 8),
      adminRow('Watauga County', 6),
      adminRow('North Carolina', 4),
    ])
    const { hierarchy } = await reverseGeocode(36.2, -81.6)
    expect(hierarchy).toHaveLength(3)
    expect(hierarchy.map((r: any) => r.name)).toContain('Boone')
    expect(hierarchy.map((r: any) => r.name)).toContain('North Carolina')
  })
})

describe('reverseGeocode — caching', () => {
  test('caches result and skips DB on repeat call', async () => {
    mockExecute.mockImplementation(async () => [adminRow('Boone', 8)])
    await reverseGeocode(36.2, -81.6)
    await reverseGeocode(36.2, -81.6)
    expect(mockExecute).toHaveBeenCalledTimes(1)
  })

  test('different coordinates produce separate cache entries', async () => {
    mockExecute.mockImplementation(async () => [])
    await reverseGeocode(36.2, -81.6)
    await reverseGeocode(35.2, -80.8)
    expect(mockExecute).toHaveBeenCalledTimes(2)
  })

  test('REGRESSION: lat=0 lng=0 is cacheable (not treated as falsy/missing)', async () => {
    mockExecute.mockImplementation(async () => [])
    await reverseGeocode(0, 0)
    await reverseGeocode(0, 0)
    expect(mockExecute).toHaveBeenCalledTimes(1)
  })
})

describe('reverseGeocodePlaces — geocoder hits', () => {
  test('hydrates an OSM-sourced hit into its full geo_places row', async () => {
    stubPelias([osmFeature()])
    queueDbResults([
      { id: 'node/8415199022', name: 'Industry', tags: { tourism: 'artwork' }, geom_type: 'point' },
    ])

    const results = await reverseGeocodePlaces(35.2271, -80.8431)
    expect(results).toHaveLength(1)
    expect(results[0].tags).toEqual({ tourism: 'artwork' })
  })

  test('keeps the geocoder record when no geo_places row exists', async () => {
    stubPelias([osmFeature()])
    queueDbResults([]) // getPlace finds nothing — Pelias index ahead of the import

    const results = await reverseGeocodePlaces(35.2271, -80.8431)
    expect(results[0].id).toBe('node/8415199022')
    expect(results[0].name).toBe('Industry')
  })

  test('dedupes the venue and address layers of the same OSM element', async () => {
    stubPelias([
      osmFeature({ layer: 'venue' }),
      osmFeature({ gid: 'openstreetmap:address:node/8415199022', layer: 'address' }),
    ])
    queueDbResults([{ id: 'node/8415199022', name: 'Industry' }])

    const results = await reverseGeocodePlaces(35.2271, -80.8431)
    expect(results).toHaveLength(1)
  })

  test('lends the address-layer address to a hit that has none', async () => {
    stubPelias([osmFeature(), addressFeature()])
    queueDbResults(
      [{ id: 'node/8415199022', name: 'Industry', address: null }], // getPlace
      [], // findOsmByAddress for the OpenAddresses record
    )

    const results = await reverseGeocodePlaces(35.2271, -80.8431)
    expect(results[0].name).toBe('Industry')
    expect(results[0].address.street).toBe('South Tryon Street')
  })

  test('does not overwrite an address the hit already has', async () => {
    stubPelias([osmFeature(), addressFeature()])
    queueDbResults(
      [{ id: 'node/8415199022', name: 'Industry', address: { street: 'North Tryon Street' } }],
      [],
    )

    const results = await reverseGeocodePlaces(35.2271, -80.8431)
    expect(results[0].address.street).toBe('North Tryon Street')
  })
})

describe('reverseGeocodePlaces — admin fallback', () => {
  test('falls back to the smallest containing area when nothing is addressable', async () => {
    stubPelias([])
    queueDbResults(
      [adminRow('Charlotte', 8), adminRow('North Carolina', 4)], // hierarchy
      [{ id: 'relation/8', name: 'Charlotte', admin_level: 8 }], // getPlace
    )

    const results = await reverseGeocodePlaces(35.2, -80.95)
    expect(results).toHaveLength(1)
    expect(results[0].name).toBe('Charlotte')
  })

  test('returns empty when the point is outside every known boundary', async () => {
    stubPelias([])
    queueDbResults([])

    expect(await reverseGeocodePlaces(0, 0)).toEqual([])
  })

  test('a Pelias failure degrades to the admin fallback rather than throwing', async () => {
    globalThis.fetch = mock(async () => {
      throw new Error('ECONNREFUSED')
    }) as any
    queueDbResults(
      [adminRow('Charlotte', 8)],
      [{ id: 'relation/8', name: 'Charlotte' }],
    )

    const results = await reverseGeocodePlaces(35.2, -80.95)
    expect(results[0].name).toBe('Charlotte')
  })
})
