import { describe, expect, it } from 'bun:test'
import { resolveFromFile, type Bbox, type RegionDef, type RegionsFile } from '../../config/regions'
import { cellsCovering, parseBbox, regionAreas } from './areas'

const pelias = { openaddresses: [], wofIds: [], tigerStates: [] }
const region = (bbox: Bbox, extra: Partial<RegionDef> = {}): RegionDef =>
  ({ label: 'r', bbox, osmExtracts: [], gtfsRegion: 'r', pelias, ...extra })
const file: RegionsFile = {
  regions: {
    us: region([-180, 18, -66, 72], { bboxes: [[-125, 24, -66, 50], [-170, 51, -129, 72], [-161, 18, -154, 23]] }),
    nc: region([-84.4, 33.8, -75.4, 36.6]),
    off: region([0, 0, 1, 1], { enabled: false }),
  },
  global: region([-180, -90, 180, 90]),
}

describe('parseBbox', () => {
  it('reads w,s,e,n and refuses anything else', () => {
    expect(parseBbox('-80.9, 35.1,-80.7,35.3')).toEqual([-80.9, 35.1, -80.7, 35.3])
    expect(() => parseBbox('-80.9,35.1,-80.7')).toThrow('west,south,east,north')
    expect(() => parseBbox('-80.7,35.1,-80.9,35.3')).toThrow()
    expect(() => parseBbox('a,b,c,d')).toThrow()
  })
})

describe('regionAreas', () => {
  it('takes each selected region by its own boxes, not their union', () => {
    expect(regionAreas(resolveFromFile(file, 'us,nc'))).toEqual([
      [-125, 24, -66, 50], [-170, 51, -129, 72], [-161, 18, -154, 23], [-84.4, 33.8, -75.4, 36.6],
    ])
  })

  it('honours REGIONS rather than every enabled region', () => {
    expect(regionAreas(resolveFromFile(file, 'nc'))).toEqual([[-84.4, 33.8, -75.4, 36.6]])
  })

  it('refuses a global selection, pointing at --bbox', () => {
    expect(() => regionAreas(resolveFromFile(file, 'global'))).toThrow('--bbox')
  })
})

describe('cellsCovering', () => {
  it('covers each area with grid cells, sharing the ones areas have in common', () => {
    const cells = cellsCovering([[-80.9, 35.1, -80.6, 35.3], [-80.7, 35.2, -80.6, 35.3]], 0.25)
    expect(cells).toEqual([[-81, 35, -80.75, 35.25], [-81, 35.25, -80.75, 35.5], [-80.75, 35, -80.5, 35.25], [-80.75, 35.25, -80.5, 35.5]])
  })

  it('refuses a cell size that would never advance', () => {
    expect(() => cellsCovering([[0, 0, 1, 1]], 0)).toThrow('--cell')
    expect(() => cellsCovering([[0, 0, 1, 1]], Number.NaN)).toThrow('--cell')
  })
})
