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

const { reverseGeocode } = await import('./geocode.service')

// ── Helpers ───────────────────────────────────────────────────────────────────

function adminRow(name: string, level: number) {
  return { id: `relation/${level}`, name, admin_level: level, area_m2: 1000 * level }
}

beforeEach(() => {
  mockExecute.mockReset()
  mockExecute.mockImplementation(async () => [])
  spatialCacheStore.clear()
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
