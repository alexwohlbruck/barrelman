import { expect, test } from 'bun:test'
import { bakeBridges } from './bake'
import { decodeBridge, encodeBridge, MATERIAL_PALETTE } from './encode'
import { mercator } from '../bridge-decks/profile'
import type { Deck } from '../bridge-decks/build'
import { cutPiece, cutStand, encodeDeckTile, readStreams, withDeckField, DECK_TILE_FIELD } from './tile-pieces'

// A deck across the boundary of two zoom-16 tiles (x 19296 | 19297).
const points = Array.from({ length: 35 }, (_, k) => mercator(-74 + (k * 0.002) / 34, 40.7))
const deck: Deck = { id: 'test', bridge: 'test', ways: [1], kind: 'road', layer: 1, edges: [4, 4], sides: null, caps: null,
  grounded: [true, true], points, heights: points.map((_, k) => 8 * Math.sin((Math.PI * k) / 34)), ground: points.map(() => 0), piers: [30, 60], length: 170, midpoint: points[17] }
const street = { properties: { bridge: true, kind: 'carriageway' }, geometry: { type: 'Polygon', coordinates: [[[-74.0003, 40.69997], [-73.9977, 40.69997], [-73.9977, 40.70003], [-74.0003, 40.70003], [-74.0003, 40.69997]]] } }
const baked = bakeBridges({ decks: [deck], outlines: [], roads: [], surfaces: [street], paint: [], ground: () => 0 } as any, MATERIAL_PALETTE)
const mesh = readStreams(encodeBridge(baked))
const z = 16, y = Math.floor(points[0][1] * 2 ** z)
const tiles = [19295, 19296, 19297, 19298].map(x => cutPiece(mesh, z, x, y)).filter((p): p is Uint8Array => !!p)
const triangles = (bytes: Uint8Array) => decodeBridge(bytes).indices.length / 3

test('the pieces of one zoom split every triangle of a deck between its tiles once', () => {
  expect(tiles).toHaveLength(2)
  expect(tiles.reduce((n, p) => n + triangles(p), 0)).toBe(mesh.indices.length / 3)
  const whole = decodeBridge(encodeBridge(baked))
  const drawn = (m: ReturnType<typeof decodeBridge>) => m.indices.map(i => m.position.slice(i * 3, i * 3 + 3).map(v => v.toFixed(9)).join()).sort()
  expect(tiles.flatMap(p => drawn(decodeBridge(p))).sort()).toEqual(drawn(whole))
})

test('a piece keeps its ranges and the stretch of surface over its tile', () => {
  for (const piece of tiles) {
    const { header } = decodeBridge(piece)
    expect(header.ranges.paintFrom).toBeLessThanOrEqual(header.ranges.railsFrom)
    expect(header.stands).toHaveLength(1)
    const { surface, chain } = header.stands[0]
    expect(surface.points.length).toBeLessThan(baked.stands[0].surface.points.length)
    expect(surface.z).toHaveLength(surface.points.length)
    expect(chain.points).toHaveLength(surface.points.length)
    if (surface.tops) expect(surface.tops).toHaveLength(surface.points.length - 1)
  }
})

test('at zoom 14 a piece leaves out the paint, rails, parapets and the ways under it', () => {
  const whole = decodeBridge(encodeBridge(baked))
  const { paintFrom, railsFrom, routesFrom } = whole.header.ranges
  expect(railsFrom).toBeGreaterThan(paintFrom)
  const at = (z: number) => decodeBridge(cutPiece(mesh, z, Math.floor(points[17][0] * 2 ** z), Math.floor(points[17][1] * 2 ** z))!)
  const mid = at(15)
  expect(mid.header.ranges).toEqual(whole.header.ranges)
  const far = at(14)
  expect(far.header.ranges.paintFrom).toBe(far.header.ranges.routesFrom)
  const parapets = whole.indices.slice(0, paintFrom).filter((_, k) => k % 3 === 0).filter(i => whole.color[i * 3] === 2).length
  expect(parapets).toBeGreaterThan(0)
  expect(far.indices.length / 3).toBe((paintFrom + whole.indices.length - routesFrom) / 3 - parapets)
  expect(far.header.stands).toHaveLength(1)
  expect(far.header.under).toEqual([])
})

test('a stand away from a tile is left out, and one wholly inside is kept whole', () => {
  const stand = baked.stands[0] as any
  expect(cutStand(stand, { minX: 0, minY: 0, maxX: 0.1, maxY: 0.1 })).toEqual([])
  expect(cutStand(stand, { minX: 0, minY: 0, maxX: 1, maxY: 1 })).toEqual([stand])
})

test('the deck tile rides in an MVT field that readers skip', () => {
  const payload = encodeDeckTile([{ file: `${'a'.repeat(64)}.brm`, bytes: tiles[0] }])
  const layer = Uint8Array.of(0x1a, 2, 0x78, 2)
  const tile = withDeckField(layer, payload)
  expect([...tile.subarray(0, 4)]).toEqual([...layer])
  expect(tile[4]).toBe(((DECK_TILE_FIELD << 3) | 2) & 0x7f | 0x80)
  const view = new DataView(payload.buffer)
  expect(view.getUint32(0, true)).toBe(0x31544442)
  expect(tile.byteLength).toBeGreaterThan(payload.byteLength + layer.byteLength)
})

test('a deck whose mesh cannot be read is named, and the tile is not cached', async () => {
  const { createDeckTiles, deckTileBuilder } = await import('./tile-pieces')
  const deckTile = createDeckTiles(async () => [`${'b'.repeat(64)}.brm`], deckTileBuilder('/nonexistent'))
  const first = await deckTile(16, 1, 1)
  expect(first.complete).toBe(false)
  const view = new DataView(first.bytes.buffer)
  expect(JSON.parse(new TextDecoder().decode(first.bytes.subarray(8, 8 + view.getUint32(4, true)))).failed).toEqual([`${'b'.repeat(64)}.brm`])
  expect((await deckTile(16, 1, 1)).complete).toBe(false)
})

test('a worker cuts deck tiles off the request thread', async () => {
  const { workerDeckTileBuilder } = await import('./tile-pieces')
  const tile = await workerDeckTileBuilder('/nonexistent')(16, 1, 1, [`${'c'.repeat(64)}.brm`])
  expect(tile.complete).toBe(false)
  expect(new DataView(tile.bytes.buffer, tile.bytes.byteOffset).getUint32(0, true)).toBe(0x31544442)
})
