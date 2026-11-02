import { afterEach, beforeEach, describe, expect, mock, test } from 'bun:test'
import { linkSync, mkdirSync, mkdtempSync, readFileSync, rmSync, statSync, writeFileSync } from 'fs'
import { tmpdir } from 'os'
import { join } from 'path'
import type { Point } from '../bridge-mesh/decks'
import { boundsOf, batchByTile, wayElevations } from './cycle-ways'
import type { LineSources } from './lookup'
import { encodeLayer, readLayer } from './mvt'
import { elevatePyramid } from './pyramid'
import type { HeightSource } from './sources'
import { at, ORIGIN } from './test-geometry'

const Z = 16, N = 2 ** Z
const [X, Y] = [Math.floor(ORIGIN[0] * N), Math.floor(ORIGIN[1] * N)]
const local = ([mx, my]: Point): [number, number] => [Math.round((mx * N - X) * 4096), Math.round((my * N - Y) * 4096)]
const deck: HeightSource = { points: [at(40, 0), at(160, 0)], heights: [6, 6], relative: false, lifts: [6, 6], half: 5, kind: 'rail', ways: [42] }
const ribbons = encodeLayer('ribbons', [{ id: 1, properties: { route_type: 1 }, lines: [[at(0, 0), at(200, 0)].map(local)] }])

describe('elevatePyramid', () => {
  let dir: string
  const tilePath = (z: number, x: number, y: number) => join(dir, String(z), String(x), `${y}.mvt`)
  const put = (z: number, x: number, y: number) => {
    mkdirSync(join(dir, String(z), String(x)), { recursive: true })
    writeFileSync(tilePath(z, x, y), ribbons)
  }
  beforeEach(() => (dir = mkdtempSync(join(tmpdir(), 'pyramid-'))))
  afterEach(() => rmSync(dir, { recursive: true, force: true }))

  test('writes heights into tiles from zoom 13, reading sources once per zoom-13 tile', async () => {
    put(Z, X, Y)
    put(Z, X + 1, Y)
    put(12, X >> 4, Y >> 4)
    const sources = mock<LineSources>(async () => [deck])
    const tally = await elevatePyramid(dir, sources)
    expect(sources).toHaveBeenCalledTimes(1)
    expect(sources).toHaveBeenCalledWith(13, X >> 3, Y >> 3)
    expect(tally).toEqual({ tiles: 2, written: 1, failed: 0 })
    expect(typeof readLayer(readFileSync(tilePath(Z, X, Y)), 'ribbons')!.features[0].properties.elevation).toBe('string')
    expect(readFileSync(tilePath(12, X >> 4, Y >> 4))).toEqual(Buffer.from(ribbons))
    expect((await elevatePyramid(dir, sources)).written).toBe(0)
  })

  test('replaces a tile rather than writing through a hard link to the live pyramid', async () => {
    put(Z, X, Y)
    const live = join(dir, 'live.mvt')
    linkSync(tilePath(Z, X, Y), live)
    await elevatePyramid(dir, async () => [deck])
    expect(readFileSync(live)).toEqual(Buffer.from(ribbons))
    expect(statSync(live).nlink).toBe(1)
  })

  test('leaves a tile as it is when its sources cannot be read', async () => {
    put(Z, X, Y)
    const tally = await elevatePyramid(dir, async () => { throw new Error('db down') })
    expect(tally.failed).toBe(1)
    expect(readFileSync(tilePath(Z, X, Y))).toEqual(Buffer.from(ribbons))
  })
})

describe('cycle ways', () => {
  test('a way is raised along its own deck over its whole length, and a neighbour is not', () => {
    const ways = [{ id: 42, points: [at(-500, 0), at(500, 0)] }, { id: 43, points: [at(-500, 0), at(500, 0)] }]
    const [raised, plain] = wayElevations(ways, [deck])
    expect(raised).toBeString()
    expect(plain).toBeNull()
  })

  test('ways are batched by the zoom-13 tile they start in', () => {
    const near = { id: 1, points: [at(0, 0), at(10, 0)] }, also = { id: 2, points: [at(5, 0), at(3000, 0)] }, far = { id: 3, points: [at(20000, 0), at(20010, 0)] }
    const batches = batchByTile([near, also, far])
    expect(batches).toEqual([[near, also], [far]])
    expect(boundsOf(batches[0]).maxX).toBe(at(3000, 0)[0])
  })
})
