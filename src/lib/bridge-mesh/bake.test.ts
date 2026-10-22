import { expect, test } from 'bun:test'
import { bakeBridges } from './bake'
import { encodeBridge, liftField, MATERIAL_PALETTE } from './encode'
import { mercator } from '../bridge-decks/profile'
import type { Deck } from '../bridge-decks/build'

const points = [[-74, 40.7], [-73.999, 40.7], [-73.998, 40.7]].map(([x, y]) => mercator(x, y))
const deck: Deck = { id: 'test', bridge: 'test', ways: [1], kind: 'road', layer: 1, edges: [4, 4], sides: null, caps: null,
  grounded: [true, true], points, heights: [0, 8, 0], ground: [0, 0, 0], piers: [30, 60], length: 170, midpoint: points[1] }
const input = { decks: [deck], outlines: [], roads: [], surfaces: [], paint: [], ground: () => 0 }

test('bakes solved bridge geometry without a map or browser', () => {
  const result = bakeBridges(input, MATERIAL_PALETTE)
  expect(result.mesh.position.length).toBeGreaterThan(0)
  expect(result.mesh.position.every(Number.isFinite)).toBe(true)
  expect(result.ranges.paintFrom).toBeGreaterThan(0)
  expect(result.stands).toHaveLength(1)
  expect(result.origin).toEqual(points[1])
  expect(bakeBridges(input, MATERIAL_PALETTE)).toEqual(result)
})

test('encodes aligned GPU streams with a versioned header', () => {
  const result = bakeBridges(input, MATERIAL_PALETTE)
  const bytes = encodeBridge(result)
  const view = new DataView(bytes.buffer)
  expect(view.getUint32(0, true)).toBe(0x314d5242)
  const start = view.getUint32(8, true)
  expect(start % 4).toBe(0)
  const header = JSON.parse(new TextDecoder().decode(bytes.subarray(12, 12 + view.getUint32(4, true))))
  expect(header.version).toBe(2)
  expect(header.lengths).toHaveLength(9)
  expect(header.ranges).toEqual(result.ranges)
  const positions = new Float32Array(bytes.buffer, start, header.lengths[0])
  const indexOffset = start + header.lengths.slice(0, -1).reduce((a: number, b: number) => a + b * 4, 0)
  const indices = new Uint32Array(bytes.buffer, indexOffset, header.lengths.at(-1))
  expect(Array.from(indices).flatMap(i => Array.from(positions.subarray(i * 3, i * 3 + 3)))).toEqual(Array.from(new Float32Array(result.mesh.position)))
  expect(positions.length).toBeLessThan(result.mesh.position.length)
})

test('lifts a structure with the terrain at its feet and with the smoothed terrain overhead', () => {
  // A 40 m ridge every other 10 m: what a deck spans, not what it should follow.
  const ridged = ([x]: [number, number]) => (Math.floor(x * 4e6) % 2 ? 40 : 0)
  const lift = liftField(ridged)
  const p = points[1]
  expect(lift(p, ridged(p), ridged(p))).toBe(ridged(p))
  const overhead = [0, 1, 2, 3].map(k => lift([p[0] + k * 2.5e-7, p[1]], 80, ridged([p[0] + k * 2.5e-7, p[1]])))
  for (const h of overhead) expect(h).toBeGreaterThan(5)
  for (const h of overhead) expect(h).toBeLessThan(35)
  expect(Math.max(...overhead) - Math.min(...overhead)).toBeLessThan(10)
})

test('leaves a structure over unknown ground exaggerated as a whole', () => {
  expect(liftField(() => null)(points[1], 30, null)).toBe(30)
})

test('a deck under a landmark that models its bridge keeps its surface and drops its own piers and parapets', () => {
  const materialCount = (structures?: Array<Array<[number, number]>>) => {
    const { mesh } = bakeBridges({ ...input, structures }, MATERIAL_PALETTE)
    const counts = new Map<number, number>()
    for (let i = 0; i < mesh.color.length; i += 3) counts.set(mesh.color[i], (counts.get(mesh.color[i]) ?? 0) + 1)
    return counts
  }
  const [x0, y0] = mercator(-74.01, 40.71), [x1, y1] = mercator(-73.99, 40.69)
  const around: Array<[number, number]> = [[x0, y0], [x1, y0], [x1, y1], [x0, y1], [x0, y0]]
  const plain = materialCount(), modelled = materialCount([around])
  const [surface, concrete, parapet] = [0, 1, 2]
  expect(plain.get(parapet)).toBeGreaterThan(0)
  expect(modelled.get(parapet) ?? 0).toBe(0)
  expect(modelled.get(concrete)!).toBeLessThan(plain.get(concrete)!)
  expect(modelled.get(surface)).toBe(plain.get(surface))
})
