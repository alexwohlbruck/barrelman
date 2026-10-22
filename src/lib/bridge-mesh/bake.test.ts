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
  // An 8 m wide cutting 40 m deep, as a road or stream the deck spans.
  const metre = 1 / 30_400_000
  const cutting = ([x]: [number, number]) => (Math.abs(x - points[1][0]) < 4 * metre ? -40 : 0)
  const lift = liftField(cutting)
  const across = [-30, -10, 0, 10, 30].map(m => [points[1][0] + m * metre, points[1][1]] as [number, number])
  expect(lift(across[2], -40, -40)).toBe(-40)
  const overhead = across.map(p => lift(p, 20, cutting(p)))
  expect(Math.max(...overhead) - Math.min(...overhead)).toBeLessThan(8)
  expect(Math.min(...overhead)).toBeGreaterThan(-10)
})

test('lifts a point alike however the meshes around it were sampled, so decks that meet are lifted alike', () => {
  const hilly = ([x, y]: [number, number]) => 30 * Math.sin(x * 2e5) + 20 * Math.cos(y * 3e5)
  const near = Array.from({ length: 40 }, (_, k) => [points[0][0] + k * 3e-7, points[0][1] + (k % 7) * 2e-7] as [number, number])
  const forward = liftField(hilly), backward = liftField(hilly)
  const a = near.map(p => forward(p, 60, hilly(p)))
  const b = near.slice().reverse().map(p => backward(p, 60, hilly(p))).reverse()
  expect(a).toEqual(b)
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

test('a pier never stands through a deck below: it moves along to clear it, or is left out', () => {
  const metre = 1 / 30_400_000
  const across = [[-73.999, 40.6995], [-73.999, 40.7], [-73.999, 40.7005]].map(([x, y]) => mercator(x, y))
  const low: Deck = { ...deck, id: 'low', ways: [2], points: across, heights: [3, 3, 3], ground: [0, 0, 0], piers: [], length: 111, midpoint: across[1], edges: [5, 5] }
  // The high deck's middle pier is given right over the low deck.
  const high: Deck = { ...deck, id: 'high', heights: [12, 12, 12], piers: [42.5, 84.5, 126.5], grounded: [false, false] }
  const { mesh, origin } = bakeBridges({ ...input, decks: [high, low], own: ['high'] }, MATERIAL_PALETTE)
  const [x0, y0] = [across[1][0] - origin[0], across[1][1] - origin[1]]
  // A column's foot: a vertex at the ground, under the low deck.
  const feet = (x0: number) => {
    let n = 0
    for (let i = 0; i < mesh.position.length; i += 3)
      if (Math.abs(mesh.position[i] - x0) < 4.5 * metre && Math.abs(mesh.position[i + 1] - y0) < 20 * metre && mesh.position[i + 2] / metre < 1) n++
    return n
  }
  expect(feet(x0)).toBe(0)
  expect(feet(x0 + 42 * metre)).toBeGreaterThan(0)
})
