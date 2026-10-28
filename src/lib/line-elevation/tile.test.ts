import { describe, expect, test } from 'bun:test'
import type { Point } from '../bridge-mesh/decks'
import { decodeElevation } from './encode'
import { encodeLayer, layerNames, readLayer, withProperty } from './mvt'
import type { HeightSource } from './sources'
import { at, ORIGIN } from './test-geometry'
import { elevateLayer, elevateTile, ribbonRides } from './tile'

const Z = 16, N = 2 ** Z
const [X, Y] = [Math.floor(ORIGIN[0] * N), Math.floor(ORIGIN[1] * N)]
const local = ([mx, my]: Point): [number, number] => [Math.round((mx * N - X) * 4096), Math.round((my * N - Y) * 4096)]
const concat = (...parts: Uint8Array[]) => {
  const out = new Uint8Array(parts.reduce((n, p) => n + p.length, 0))
  parts.reduce((o, p) => (out.set(p, o), o + p.length), 0)
  return out
}

const deck: HeightSource = { points: [at(40, 0), at(160, 0)], heights: [6, 6], relative: false, lifts: [6, 6], half: 5, kind: 'rail', ways: [42] }
const line = [at(0, 0), at(200, 0)].map(local)
const other = encodeLayer('buildings', [{ id: 1, properties: { height: 12 }, lines: [[[0, 0], [10, 10]]] }])
const tile = concat(
  encodeLayer('bicycle_ways', [
    { id: 42, properties: { name: 'Bridge path', bridge: true }, lines: [line] },
    { id: 43, properties: { name: 'Elsewhere' }, lines: [line] },
  ]),
  other,
)

describe('withProperty', () => {
  test('adds the property to the features asked for and keeps everything else', () => {
    const out = withProperty(tile, 'bicycle_ways', 'elevation', f => (f.id === 42 ? 'x' : null))
    const { features } = readLayer(out, 'bicycle_ways')!
    expect(features[0].properties).toEqual({ name: 'Bridge path', bridge: true, elevation: 'x' })
    expect(features[1].properties).toEqual({ name: 'Elsewhere' })
    expect(features[0].lines).toEqual([line])
    expect(layerNames(out)).toEqual(['bicycle_ways', 'buildings'])
    expect(out.subarray(out.length - other.length)).toEqual(other)
  })

  test('returns the very tile when nothing changes', () => {
    expect(withProperty(tile, 'bicycle_ways', 'elevation', () => null)).toBe(tile)
    expect(withProperty(tile, 'ribbons', 'elevation', () => 'x')).toBe(tile)
  })
})

describe('elevateLayer', () => {
  test('raises a cycle way along the deck of its own way only', () => {
    const out = elevateLayer(tile, 'bicycle_ways', Z, X, Y, [deck], f => (f.id === undefined ? null : { way: f.id }))
    const [raised, plain] = readLayer(out, 'bicycle_ways')!.features
    expect(plain.properties.elevation).toBeUndefined()
    const [run] = decodeElevation(raised.properties.elevation as string)
    expect(run.map(p => p[2])).toEqual([6, 6])
  })

  test('raises a rail ribbon on a rail deck by where it runs, but not a ferry', () => {
    const ribbons = encodeLayer('ribbons', [
      { id: 1, properties: { route_type: 1, mode: 'metro' }, lines: [line] },
      { id: 2, properties: { route_type: 4, mode: 'ferry' }, lines: [line] },
    ])
    const [metro, ferry] = readLayer(elevateLayer(ribbons, 'ribbons', Z, X, Y, [deck], ribbonRides), 'ribbons')!.features
    expect(typeof metro.properties.elevation).toBe('string')
    expect(ferry.properties.elevation).toBeUndefined()
  })
})

describe('elevateTile', () => {
  test('falls back to the tile as it came when its sources cannot be read', async () => {
    const out = await elevateTile(tile, 'bicycle_ways', Z, X, Y, async () => { throw new Error('down') })
    expect(out).toEqual({ bytes: tile, complete: false })
  })

  test('reads no sources below its zoom, or for a tile without the layer', async () => {
    let asked = 0
    const sources = async () => (asked++, [deck])
    await elevateTile(tile, 'bicycle_ways', 12, X >> 4, Y >> 4, sources)
    await elevateTile(other, 'bicycle_ways', Z, X, Y, sources)
    expect(asked).toBe(0)
  })
})

describe('ribbonRides', () => {
  test('rides by GTFS route type, falling back to the mode', () => {
    expect(ribbonRides({ type: 2, properties: { route_type: 3 }, lines: [] })).toEqual({ kinds: ['road'] })
    expect(ribbonRides({ type: 2, properties: { route_type: 401 }, lines: [] })).toEqual({ kinds: ['rail'] })
    expect(ribbonRides({ type: 2, properties: { mode: 'tram' }, lines: [] })).toEqual({ kinds: ['rail'] })
    expect(ribbonRides({ type: 2, properties: { route_type: 6 }, lines: [] })).toBeNull()
  })
})
