import { describe, expect, test } from 'bun:test'
import { overpassesOver, structureCrossings, wayLevel, type LevelAt, type Structure } from './overpasses'
import { routePath, type LonLat } from '../route-hazards/route-path'

const at = (east: number, north = 0): LonLat => [-73.99 + east / (111_320 * Math.cos(40.75 * Math.PI / 180)), 40.75 + north / 111_320]
/** Eastbound, 400 m. */
const path = routePath([at(0), at(200), at(400)])
const ground: LevelAt = () => ({ level: 0 })

const structure = (osmId: number, tags: Record<string, string>, coordinates: LonLat[]): Structure =>
  ({ osmId, tags, coordinates, firstSegment: 0, lastSegment: 1 })
/** A north-south way crossing the route `east` metres along. */
const across = (east: number) => [at(east, -50), at(east, 50)]

const over = (structures: Structure[], levelAt: LevelAt = ground) => overpassesOver(structureCrossings(path, structures), levelAt)

describe('overpasses', () => {
  test('a road bridge over the route is an overpass where the route passes under it', () => {
    const [o] = over([structure(1, { highway: 'primary', bridge: 'yes', name: 'Main Street' }, across(150))])
    expect(o).toMatchObject({ kind: 'road', name: 'Main Street' })
    expect(o.distance).toBeCloseTo(150, 0)
  })

  test('twin carriageways and parallel tracks are one overpass', () => {
    const found = over([
      structure(1, { highway: 'motorway', bridge: 'yes', name: 'I-95' }, across(150)),
      structure(2, { highway: 'motorway', bridge: 'yes', name: 'I-95' }, across(170)),
      structure(3, { railway: 'rail', bridge: 'yes' }, across(185)),
    ])
    expect(found).toHaveLength(1)
    expect(found[0].endDistance).toBeCloseTo(185, 0)
    expect(found[0].kind).toBe('rail')
    expect(found[0].name).toBeUndefined()
  })

  test('a way at the route\'s own level, or below it, is not over it', () => {
    expect(over([structure(1, { highway: 'primary', bridge: 'yes' }, across(150))], () => ({ level: 1 }))).toEqual([])
    expect(over([structure(1, { highway: 'primary', layer: '1' }, across(150))], () => ({ level: 2 }))).toEqual([])
  })

  test('a sunken route passes under the ground-level street crossing it', () => {
    expect(over([structure(1, { highway: 'residential', layer: '0' }, across(150))], () => ({ level: -1 }))).toHaveLength(1)
  })

  test('nothing is seen over a tunnel', () => {
    expect(over([structure(1, { railway: 'rail', bridge: 'yes' }, across(150))], () => ({ level: null }))).toEqual([])
  })

  test('the bridge the route itself rides is not an overpass', () => {
    expect(over([structure(7, { highway: 'primary', bridge: 'yes' }, across(150))], () => ({ level: 0, wayId: 7 }))).toEqual([])
  })

  test('an elevated line running along the street above is not crossed', () => {
    const along = [at(0, 3), at(400, 8)]
    expect(over([structure(1, { railway: 'subway', bridge: 'viaduct' }, along)])).toEqual([])
  })

  test('a junction the two ways share is not one over the other', () => {
    expect(over([structure(1, { highway: 'primary', bridge: 'yes' }, [at(200, -50), at(200), at(200, 50)])])).toEqual([])
  })

  test('footbridges and disused railways are not overpasses', () => {
    expect(over([
      structure(1, { highway: 'footway', bridge: 'yes' }, across(100)),
      structure(2, { railway: 'abandoned', bridge: 'yes' }, across(300)),
    ])).toEqual([])
  })
})

describe('wayLevel', () => {
  test('reads layer, and counts an unlayered bridge up and tunnel down', () => {
    expect(wayLevel({ layer: '2', bridge: 'yes' })).toBe(2)
    expect(wayLevel({ bridge: 'yes' })).toBe(1)
    expect(wayLevel({ tunnel: 'yes' })).toBe(-1)
    expect(wayLevel({ bridge: 'no' })).toBe(0)
  })
})
