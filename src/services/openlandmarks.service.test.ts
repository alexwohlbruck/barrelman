import { afterEach, describe, expect, it } from 'bun:test'
import { openLandmarkRow, type OpenLandmarkAsset } from './openlandmarks.service'
import { landmarkSourcePriority } from './landmarks.service'

const sha = (c: string) => c.repeat(64)
const asset = (over: Partial<OpenLandmarkAsset> = {}): OpenLandmarkAsset => ({
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

describe('openLandmarkRow', () => {
  it('namespaces ids so they cannot collide with the catalog', () => {
    const row = openLandmarkRow(asset())
    if ('skip' in row) throw new Error(row.skip)
    expect(row.id).toBe('ol-arc-de-triomphe')
    expect(row.sourceId).toBe('arc-de-triomphe')
    expect(row.low.modelId).toBe('ol-arc-de-triomphe-low')
    expect(row.detail?.modelId).toBe('ol-arc-de-triomphe-detail')
  })

  it('replaces the primary and additional OSM elements', () => {
    const row = openLandmarkRow(asset())
    if ('skip' in row) throw new Error(row.skip)
    expect(row.replaces).toEqual(['way/226413508', 'relation/7'])
  })

  it('keeps the CC BY credit on the model', () => {
    const row = openLandmarkRow(asset())
    if ('skip' in row) throw new Error(row.skip)
    expect(row.license).toBe('CC-BY-4.0')
    expect(row.attribution).toContain('OpenStreetMap')
  })

  it('drops the detail zoom when there is no detail model', () => {
    const row = openLandmarkRow(asset({ lods: { low: asset().lods.low } }))
    if ('skip' in row) throw new Error(row.skip)
    expect(row.detail).toBeNull()
    expect(row.detailZoom).toBeNull()
  })

  it('skips what it cannot place correctly rather than guessing', () => {
    expect(openLandmarkRow(asset({ heading: 90 }))).toEqual({ skip: 'arc-de-triomphe: heading 90 is not baked' })
    expect('skip' in openLandmarkRow(asset({ axes: 'X east / Y north / Z up' }))).toBe(true)
    expect('skip' in openLandmarkRow(asset({ id: '../etc' }))).toBe(true)
    expect('skip' in openLandmarkRow(asset({ lods: { low: { url: '/x', bytes: 1, sha256: 'nope' } } }))).toBe(true)
  })

  it('keeps only well-formed entrance points', () => {
    const row = openLandmarkRow(asset({ entranceLights: [[1, 0, 2], [Number.NaN, 0, 0], [1, 2]] as number[][] }))
    if ('skip' in row) throw new Error(row.skip)
    expect(row.entrances).toEqual([[1, 0, 2]])
  })
})

describe('landmarkSourcePriority', () => {
  const saved = process.env.LANDMARK_SOURCE_PRIORITY
  afterEach(() => {
    if (saved === undefined) delete process.env.LANDMARK_SOURCE_PRIORITY
    else process.env.LANDMARK_SOURCE_PRIORITY = saved
  })

  it('defaults to Open Landmarks first', () => {
    delete process.env.LANDMARK_SOURCE_PRIORITY
    expect(landmarkSourcePriority()).toEqual(['openlandmarks', 'catalog'])
  })

  it('ranks a source left off the list last, and ignores unknown names', () => {
    process.env.LANDMARK_SOURCE_PRIORITY = 'catalog, bogus'
    expect(landmarkSourcePriority()).toEqual(['catalog', 'openlandmarks'])
  })
})
