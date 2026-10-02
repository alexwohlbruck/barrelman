import { describe, test, expect } from 'bun:test'
import { lookupOpenAddresses, tigerStatesFor, type Boundary } from './boundary-catalog.service'

const boundary = (over: Partial<Boundary>): Boundary => ({
  id: 'us',
  label: 'United States',
  name: 'United States of America',
  parent: 'north-america',
  iso3166_1: ['US'],
  iso3166_2: [],
  pbfUrl: 'https://download.geofabrik.de/north-america/us-latest.osm.pbf',
  updatesUrl: null,
  bbox: [-125, 24, -66, 50],
  ...over,
})

const tree = (paths: string[], truncated = false) =>
  (async () =>
    Response.json({ truncated, tree: paths.map((path) => ({ path, type: 'blob' })) })) as unknown as typeof fetch

const SOURCES = [
  'sources/us/countrywide.json',
  'sources/us/co/denver.json',
  'sources/us/co/boulder.json',
  'sources/us/ny/statewide.json',
  'sources/us/ny/README.md',
  'sources/usa-fake/x.json',
  'sources/de/berlin.json',
]

describe('lookupOpenAddresses', () => {
  test('a state lists only its own sources', async () => {
    const { files, warning } = await lookupOpenAddresses(boundary({ iso3166_2: ['US-CO'] }), tree(SOURCES))
    expect(files).toEqual(['us/co/boulder.csv', 'us/co/denver.csv'])
    expect(warning).toBeUndefined()
  })

  test('a country lists every source beneath it, nested state directories included', async () => {
    const { files } = await lookupOpenAddresses(boundary({}), tree(SOURCES))
    expect(files).toEqual(['us/co/boulder.csv', 'us/co/denver.csv', 'us/countrywide.csv', 'us/ny/statewide.csv'])
  })

  test('warns when nothing covers the region', async () => {
    const { files, warning } = await lookupOpenAddresses(boundary({ iso3166_1: ['FR'] }), tree(SOURCES))
    expect(files).toEqual([])
    expect(warning).toContain('"fr"')
  })

  test('keeps the files but warns when GitHub truncates the listing', async () => {
    const { files, warning } = await lookupOpenAddresses(boundary({ iso3166_2: ['US-NY'] }), tree(SOURCES, true))
    expect(files).toEqual(['us/ny/statewide.csv'])
    expect(warning).toContain('truncated')
  })
})

describe('tigerStatesFor', () => {
  test('a state yields its own FIPS code', () => {
    expect(tigerStatesFor(boundary({ iso3166_2: ['US-NC'] }))).toEqual([37])
  })

  test('the whole country yields every state', () => {
    const states = tigerStatesFor(boundary({}))
    expect(states).toContain(37)
    expect(states).toContain(6)
    expect(states.length).toBeGreaterThan(50)
  })

  test('a non-US country yields none', () => {
    expect(tigerStatesFor(boundary({ iso3166_1: ['DE'] }))).toEqual([])
  })
})
