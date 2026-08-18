/**
 * Region selection.
 *
 * This resolver decides what the importers download, so the cost of getting it
 * wrong is measured in tens of gigabytes. A blank REGIONS — which is what the
 * admin console sends when the regions field is left empty — used to resolve to
 * the global region, turning "import the two dev states" into "download the
 * planet", while still reporting `isGlobal: false`. Only the literal "global"
 * may select the planet.
 */

import { describe, test, expect } from 'bun:test'
import { resolveFromFile, GLOBAL_KEY, DEFAULT_REGIONS, type RegionsFile } from './regions'

const pelias = { openaddresses: [], wofIds: [], tigerStates: [] }

const FILE: RegionsFile = {
  regions: {
    'north-carolina': {
      label: 'North Carolina',
      osmExtracts: ['https://example.test/north-carolina-latest.osm.pbf'],
      bbox: [-84.4, 33.7, -75.4, 36.6],
      gtfsRegion: 'nc',
      pelias,
    },
    'nyc-metro': {
      label: 'NYC Metro',
      osmExtracts: ['https://example.test/new-york-latest.osm.pbf'],
      bbox: [-75.4, 40.4, -71.7, 42.1],
      gtfsRegion: 'nyc',
      pelias,
    },
    disabled: {
      label: 'Switched off',
      osmExtracts: ['https://example.test/nowhere.osm.pbf'],
      bbox: [0, 0, 1, 1],
      gtfsRegion: 'nowhere',
      pelias,
      enabled: false,
    },
  },
  global: {
    label: 'Global (planet)',
    osmExtracts: ['https://example.test/planet-latest.osm.pbf'],
    bbox: [-180, -90, 180, 90],
    gtfsRegion: 'global',
    pelias,
  },
}

const PLANET = 'https://example.test/planet-latest.osm.pbf'

describe('an unspecified selection falls back to the dev regions', () => {
  test.each([
    ['undefined', undefined],
    ['empty string', ''],
    ['whitespace', '   '],
  ])('%s resolves to the default pair, not the planet', (_label, value) => {
    const resolved = resolveFromFile(FILE, value)

    expect(resolved.isGlobal).toBe(false)
    expect(resolved.keys).toEqual(['north-carolina', 'nyc-metro'])
    expect(resolved.osmExtracts).not.toContain(PLANET)
  })

  test('the fallback is the documented default', () => {
    expect(resolveFromFile(FILE, '').keys.join(',')).toBe(DEFAULT_REGIONS)
  })

  test('a blank selection matches an absent one exactly', () => {
    expect(resolveFromFile(FILE, '')).toEqual(resolveFromFile(FILE, undefined))
  })
})

describe('explicit selections', () => {
  test('only the literal "global" reaches the planet', () => {
    const resolved = resolveFromFile(FILE, GLOBAL_KEY)

    expect(resolved.isGlobal).toBe(true)
    expect(resolved.osmExtracts).toEqual([PLANET])
  })

  test('a single region resolves to just that region', () => {
    const resolved = resolveFromFile(FILE, 'nyc-metro')

    expect(resolved.keys).toEqual(['nyc-metro'])
    expect(resolved.osmExtracts).toEqual([
      'https://example.test/new-york-latest.osm.pbf',
    ])
  })

  test('several regions combine their extracts and bbox', () => {
    const resolved = resolveFromFile(FILE, 'north-carolina,nyc-metro')

    expect(resolved.osmExtracts).toHaveLength(2)
    // The union spans from NC's west edge to NYC's north edge.
    expect(resolved.bbox).toEqual([-84.4, 33.7, -71.7, 42.1])
  })

  test('surrounding whitespace and empty entries are tolerated', () => {
    expect(resolveFromFile(FILE, ' nyc-metro , ').keys).toEqual(['nyc-metro'])
  })

  test('an unknown region is an error rather than a silent skip', () => {
    expect(() => resolveFromFile(FILE, 'atlantis')).toThrow(/Unknown region/)
  })

  test('a disabled region is an error rather than a silent import', () => {
    expect(() => resolveFromFile(FILE, 'disabled')).toThrow(/disabled/)
  })
})
