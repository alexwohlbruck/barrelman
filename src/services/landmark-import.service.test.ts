import { afterEach, describe, expect, it } from 'bun:test'
import { landmarkRow, landmarkSources, type LandmarkAsset, type LandmarkSource } from './landmark-import.service'
import { landmarkSourcePriority } from './landmarks.service'

const sha = (c: string) => c.repeat(64)
const asset = (over: Partial<LandmarkAsset> = {}): LandmarkAsset => ({
  id: 'arc-de-triomphe',
  name: 'Arc de Triomphe',
  anchor: [2.295, 48.8737],
  heading: 0,
  axes: 'X east / Y up / Z south',
  minZoom: 15,
  detailZoom: 17,
  lods: {
    low: { url: '/models/arc/a/low.glb', bytes: 100, sha256: sha('a') },
    detail: { url: '/models/arc/b/detail.glb', bytes: 900, sha256: sha('b') },
  },
  osm: { type: 'way', id: 226413508 },
  additionalOsm: [{ type: 'relation', id: 7 }],
  attribution: 'Open Landmarks; contains © OpenStreetMap contributors, ODbL-1.0',
  authors: ['Open Landmarks contributors'],
  artisticLicense: 'CC-BY-4.0',
  ...over,
})

const ENV = ['OPEN_LANDMARKS_URL', 'BARRELMAN_LANDMARKS_URL', 'LANDMARK_SOURCE_PRIORITY'] as const
const saved = Object.fromEntries(ENV.map((k) => [k, process.env[k]]))
afterEach(() => {
  for (const k of ENV) {
    if (saved[k] === undefined) delete process.env[k]
    else process.env[k] = saved[k]
  }
})

const source = (id: string): LandmarkSource => {
  for (const k of ENV) delete process.env[k]
  return landmarkSources().find((s) => s.id === id)!
}
const row = (src: LandmarkSource, a: LandmarkAsset) => {
  const r = landmarkRow(src, a)
  if ('skip' in r) throw new Error(r.skip)
  return r
}

describe('landmarkSources', () => {
  it('reads both datasets by default, Open Landmarks namespaced and ours not', () => {
    for (const k of ENV) delete process.env[k]
    expect(landmarkSources().map((s) => [s.id, s.base, s.idPrefix])).toEqual([
      ['openlandmarks', 'https://open-landmarks.benmaps.fr', 'ol-'],
      ['barrelman', 'https://alexwohlbruck.github.io/landmarks', ''],
    ])
  })

  it('switches a source off, and takes a local release directory', () => {
    process.env.OPEN_LANDMARKS_URL = 'off'
    process.env.BARRELMAN_LANDMARKS_URL = './dist/'
    const [ol, ours] = landmarkSources()
    expect(ol.base).toBeNull()
    expect(ours.base).toStartWith('/')
    expect(ours.base).toEndWith('/dist')
  })

  it('drops a trailing slash from a URL, and treats a blank one as unset', () => {
    process.env.OPEN_LANDMARKS_URL = 'https://example.org/ol/'
    process.env.BARRELMAN_LANDMARKS_URL = ''
    const [ol, ours] = landmarkSources()
    expect(ol.base).toBe('https://example.org/ol')
    expect(ours.base).toBe('https://alexwohlbruck.github.io/landmarks')
  })
})

describe('landmarkRow', () => {
  it('prefixes Open Landmarks ids so they cannot collide with ours', () => {
    const r = row(source('openlandmarks'), asset())
    expect(r.id).toBe('ol-arc-de-triomphe')
    expect(r.sourceId).toBe('arc-de-triomphe')
    expect(r.low.modelId).toBe('ol-arc-de-triomphe-low')
    expect(r.detail?.modelId).toBe('ol-arc-de-triomphe-detail')
  })

  it('keeps our ids as they were in the bundled catalog', () => {
    const r = row(source('barrelman'), asset({ id: 'eiffel-tower' }))
    expect(r.id).toBe('eiffel-tower')
    expect(r.low.modelId).toBe('eiffel-tower-low')
  })

  it('replaces the primary and additional OSM elements', () => {
    expect(row(source('openlandmarks'), asset()).replaces).toEqual(['way/226413508', 'relation/7'])
    expect(row(source('barrelman'), asset({ osm: null, additionalOsm: [] })).replaces).toEqual([])
  })

  it('keeps the CC BY credit on the model', () => {
    const r = row(source('openlandmarks'), asset())
    expect(r.license).toBe('CC-BY-4.0')
    expect(r.attribution).toContain('OpenStreetMap')
  })

  it('falls back to the source’s licence and credit where an asset states none', () => {
    const bare = asset({ artisticLicense: undefined, authors: undefined, attribution: undefined })
    expect(row(source('openlandmarks'), bare)).toMatchObject({
      license: 'CC-BY-4.0', author: 'Open Landmarks contributors', attribution: 'Open Landmarks; © OpenStreetMap contributors',
    })
    expect(row(source('barrelman'), bare)).toMatchObject({ license: 'CC0-1.0', author: 'Barrelman', attribution: null })
  })

  it('keeps a well-formed Wikidata id, for matching across sources', () => {
    expect(row(source('barrelman'), asset({ wikidata: 'Q243' })).wikidata).toBe('Q243')
    expect(row(source('barrelman'), asset({ wikidata: 'Eiffel' })).wikidata).toBeNull()
    expect(row(source('openlandmarks'), asset()).wikidata).toBeNull()
  })

  it('drops the detail zoom when there is no detail model', () => {
    const r = row(source('openlandmarks'), asset({ lods: { low: asset().lods.low } }))
    expect(r.detail).toBeNull()
    expect(r.detailZoom).toBeNull()
  })

  it('skips what it cannot place correctly rather than guessing', () => {
    const ol = source('openlandmarks')
    expect(landmarkRow(ol, asset({ heading: 90 }))).toEqual({ skip: 'arc-de-triomphe: heading 90 is not baked' })
    expect('skip' in landmarkRow(ol, asset({ axes: 'X east / Y north / Z up' }))).toBe(true)
    expect('skip' in landmarkRow(ol, asset({ id: '../etc' }))).toBe(true)
    expect('skip' in landmarkRow(ol, asset({ lods: { low: { url: '/x', bytes: 1, sha256: 'nope' } } }))).toBe(true)
  })

  it('carries a signed elevation, and stands an asset without one on the ground', () => {
    expect(row(source('barrelman'), asset({ elevation: 10 })).elevation).toBe(10)
    expect(row(source('barrelman'), asset({ elevation: -7 })).elevation).toBe(-7)
    // Open Landmarks has no such field, and every release before it had none.
    expect(row(source('openlandmarks'), asset()).elevation).toBe(0)
  })

  it('skips an elevation it could only draw wildly off the ground', () => {
    const ours = source('barrelman')
    expect(landmarkRow(ours, asset({ elevation: 250 }))).toEqual({
      skip: 'arc-de-triomphe: elevation 250 is not within ±200 m',
    })
    expect('skip' in landmarkRow(ours, asset({ elevation: Number.NaN }))).toBe(true)
    expect('skip' in landmarkRow(ours, asset({ elevation: '10' as unknown as number }))).toBe(true)
  })

  it('keeps only well-formed entrance points', () => {
    const r = row(source('openlandmarks'), asset({ entranceLights: [[1, 0, 2], [Number.NaN, 0, 0], [1, 2]] as number[][] }))
    expect(r.entrances).toEqual([[1, 0, 2]])
  })
})

describe('landmarkSourcePriority', () => {
  it('defaults to Open Landmarks first', () => {
    delete process.env.LANDMARK_SOURCE_PRIORITY
    expect(landmarkSourcePriority()).toEqual(['openlandmarks', 'barrelman'])
  })

  it('ranks a source left off the list last, and ignores unknown names', () => {
    process.env.LANDMARK_SOURCE_PRIORITY = 'barrelman, bogus'
    expect(landmarkSourcePriority()).toEqual(['barrelman', 'openlandmarks'])
  })

  it('reads the retired `catalog` as our dataset', () => {
    process.env.LANDMARK_SOURCE_PRIORITY = 'catalog,openlandmarks'
    expect(landmarkSourcePriority()).toEqual(['barrelman', 'openlandmarks'])
  })
})
