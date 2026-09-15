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
import { resolveFromFile, GLOBAL_KEY, type RegionsFile } from './regions'

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

describe('an unspecified selection never guesses, and never means the planet', () => {
  // The original bug this file was written for: a blank selection resolving to
  // the planet. It must still never do that — but it no longer silently picks
  // the sample regions either.
  test.each([
    ['undefined', undefined],
    ['empty string', ''],
    ['whitespace', '   '],
  ])('%s refuses rather than resolving to the planet', (_label, value) => {
    expect(() => resolveFromFile(FILE, value)).toThrow(/REGIONS is not set/)
  })

  test.each([
    ['undefined', undefined],
    ['empty string', ''],
    ['whitespace', '   '],
  ])('%s resolves to the sole region when there is only one', (_label, value) => {
    const single: RegionsFile = { regions: { 'north-carolina': FILE.regions['north-carolina'] } }
    const resolved = resolveFromFile(single, value)
    expect(resolved.isGlobal).toBe(false)
    expect(resolved.keys).toEqual(['north-carolina'])
    expect(resolved.osmExtracts).not.toContain(PLANET)
  })

  // Blank REGIONS used to resolve to the repo's two sample states. That is a
  // guess, and a wrong one on any instance that imports something else — so it
  // now only resolves when there is nothing to guess between.
  test('a blank selection refuses to guess between several regions', () => {
    expect(() => resolveFromFile(FILE, '')).toThrow(/REGIONS is not set/)
  })

  test('a blank selection resolves when exactly one region is configured', () => {
    const single: RegionsFile = { regions: { 'north-carolina': FILE.regions['north-carolina'] } }
    expect(resolveFromFile(single, '').keys).toEqual(['north-carolina'])
  })

  test('a disabled region does not count as the one to fall back to', () => {
    const single: RegionsFile = {
      regions: { 'north-carolina': FILE.regions['north-carolina'], disabled: FILE.regions.disabled },
    }
    expect(resolveFromFile(single, '').keys).toEqual(['north-carolina'])
  })

  test('a blank selection matches an absent one exactly', () => {
    const single: RegionsFile = { regions: { 'north-carolina': FILE.regions['north-carolina'] } }
    expect(resolveFromFile(single, '')).toEqual(resolveFromFile(single, undefined))
  })

  // Deleting the global region must actually remove it, not fall through to a
  // planet download.
  test('REGIONS=global errors when no global region is configured', () => {
    const noGlobal: RegionsFile = { regions: FILE.regions }
    expect(() => resolveFromFile(noGlobal, GLOBAL_KEY)).toThrow(/no global region is configured/)
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
