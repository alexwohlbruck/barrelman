import { describe, test, expect, mock, beforeEach } from 'bun:test'

// ── Mocks ─────────────────────────────────────────────────────────────────────

const mockExecute = mock(async () => [] as any[])

mock.module('../db', () => ({ db: { execute: mockExecute } }))

const { getPlace } = await import('./place.service')

// ── Fixtures ──────────────────────────────────────────────────────────────────

const fullPlace = {
  id: 'node/123',
  osm_type: 'node',
  osm_id: '123',
  name: 'Main Library',
  name_abbrev: null,
  names: [],
  categories: ['amenity/library'],
  tags: { amenity: 'library' },
  address: { city: 'Boone', state: 'NC', postcode: '28608' },
  hours: null,
  phones: [],
  websites: [],
  geom_type: 'point',
  admin_level: null,
  area_m2: null,
  geometry: { type: 'Point', coordinates: [-81.6, 36.2] },
  full_geometry: null,
}

beforeEach(() => {
  mockExecute.mockReset()
  mockExecute.mockImplementation(async () => [])
})

// ── Tests ─────────────────────────────────────────────────────────────────────

describe('getPlace', () => {
  test('returns the place record when found', async () => {
    mockExecute.mockImplementation(async () => [fullPlace])
    const result = await getPlace('node', '123')
    expect(result).not.toBeNull()
    expect(result.id).toBe('node/123')
    expect(result.name).toBe('Main Library')
  })

  test('returns null when place is not found', async () => {
    mockExecute.mockImplementation(async () => [])
    const result = await getPlace('node', '999999999999')
    expect(result).toBeNull()
  })

  test('executes exactly one DB query per call', async () => {
    mockExecute.mockImplementation(async () => [fullPlace])
    await getPlace('way', '456')
    expect(mockExecute).toHaveBeenCalledTimes(1)
  })

  test('returns first row when DB returns multiple rows', async () => {
    const place1 = { ...fullPlace, id: 'node/1', name: 'First' }
    const place2 = { ...fullPlace, id: 'node/2', name: 'Second' }
    mockExecute.mockImplementation(async () => [place1, place2])
    const result = await getPlace('node', '1')
    expect(result.name).toBe('First')
  })

  test('includes full_geometry for non-point geom_type', async () => {
    const areaPlace = {
      ...fullPlace,
      id: 'way/789',
      geom_type: 'area',
      full_geometry: { type: 'Polygon', coordinates: [] },
    }
    mockExecute.mockImplementation(async () => [areaPlace])
    const result = await getPlace('way', '789')
    expect(result.full_geometry).not.toBeNull()
  })

  test('full_geometry is null for point places', async () => {
    mockExecute.mockImplementation(async () => [fullPlace])
    const result = await getPlace('node', '123')
    expect(result.full_geometry).toBeNull()
  })

  test('works for all three OSM types', async () => {
    for (const osmType of ['node', 'way', 'relation']) {
      mockExecute.mockImplementationOnce(async () => [{ ...fullPlace, id: `${osmType}/1` }])
      const result = await getPlace(osmType, '1')
      expect(result).not.toBeNull()
      expect(result.id).toBe(`${osmType}/1`)
    }
  })
})
