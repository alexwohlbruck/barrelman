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

test('below zoom 16 a piece leaves out the paint and rails', () => {
  const whole = decodeBridge(encodeBridge(baked)).header.ranges
  expect(whole.railsFrom).toBeGreaterThan(whole.paintFrom)
  const low = cutPiece(mesh, 14, Math.floor(points[17][0] * 2 ** 14), Math.floor(points[17][1] * 2 ** 14))!
  const { header } = decodeBridge(low)
  expect(header.ranges.paintFrom).toBe(header.ranges.routesFrom)
  expect(triangles(low)).toBe(whole.paintFrom / 3 + (mesh.indices.length - whole.routesFrom) / 3)
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
