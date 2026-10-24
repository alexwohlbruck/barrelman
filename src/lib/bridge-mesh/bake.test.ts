import { expect, test } from 'bun:test'
import { bakeBridges } from './bake'
import { decodeBridge, encodeBridge, liftField, MATERIAL_PALETTE } from './encode'
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

test('encodes compact streams that decode to the baked mesh within 3 mm', () => {
  const result = bakeBridges(input, MATERIAL_PALETTE)
  const bytes = encodeBridge(result)
  const view = new DataView(bytes.buffer)
  expect(view.getUint32(0, true)).toBe(0x314d5242)
  expect(view.getUint32(8, true) % 4).toBe(0)
  const { header, position, color, normal, indices } = decodeBridge(bytes)
  expect(header.version).toBe(3)
  expect(header.lengths).toHaveLength(9)
  expect(header.ranges).toEqual(result.ranges)
  expect(header.origin).toEqual(result.origin)
  const drawn = indices.flatMap(i => position.slice(i * 3, i * 3 + 3))
  drawn.forEach((v, k) => expect(Math.abs(v - result.mesh.position[k])).toBeLessThan(1.1e-10))
  expect(indices.flatMap(i => color.slice(i * 3, i * 3 + 3))).toEqual(result.mesh.color)
  indices.flatMap(i => normal.slice(i * 3, i * 3 + 3)).forEach((v, k) => expect(v).toBeCloseTo(result.mesh.normal[k], 2))
  expect(position.length).toBeLessThan(result.mesh.position.length)
})

test('keeps the heights decks were solved and faired at where they run side by side', () => {
  const beside = (id: string, north: number, height: number): Deck => {
    const line = [[-74, 40.7], [-73.999, 40.7], [-73.998, 40.7]].map(([x, y]) => mercator(x, y + north / 111_000))
    return { ...deck, id, points: line, heights: [height, height, height], grounded: [false, false], midpoint: line[1] }
  }
  const { stands } = bakeBridges({ ...input, decks: [beside('low', 0, 10), beside('high', 9, 11)] }, MATERIAL_PALETTE)
  expect(Math.max(...stands[0].surface.z)).toBeCloseTo(10, 1)
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

test('lifts a high deck on a smooth curve where it passes from water to a bank', () => {
  const metre = 1 / 30_400_000
  const bank = ([x]: [number, number]) => (x < points[1][0] ? -2 : 8)
  const lift = liftField(bank)
  const z = Array.from({ length: 201 }, (_, k) => [points[1][0] + (k - 100) * metre, points[1][1]] as [number, number]).map(p => lift(p, 40, bank(p)))
  const bends = z.slice(6, -6).map((v, k) => Math.abs(z[k + 12] - 2 * v + z[k]) / 36)
  expect(Math.max(...bends)).toBeLessThan(0.04)
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
  const materialCount = (structures?: Array<{ ring: Array<[number, number]>; centre: [number, number]; radius: number }>) => {
    const { mesh } = bakeBridges({ ...input, structures }, MATERIAL_PALETTE)
    const counts = new Map<number, number>()
    for (let i = 0; i < mesh.color.length; i += 3) counts.set(mesh.color[i], (counts.get(mesh.color[i]) ?? 0) + 1)
    return counts
  }
  const [x0, y0] = mercator(-74.01, 40.71), [x1, y1] = mercator(-73.99, 40.69)
  const around: Array<[number, number]> = [[x0, y0], [x1, y0], [x1, y1], [x0, y1], [x0, y0]]
  const plain = materialCount(), modelled = materialCount([{ ring: around, centre: points[1], radius: 200 }])
  // A model reaching only 30 m from its anchor leaves the deck's far stretches their own parapets.
  expect(materialCount([{ ring: around, centre: points[1], radius: 30 }]).get(2)).toBeGreaterThan(0)
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

test('a walkway under a landmark that models its bridge is left to the model entirely', () => {
  const [x0, y0] = mercator(-74.01, 40.71), [x1, y1] = mercator(-73.99, 40.69)
  const around: Array<[number, number]> = [[x0, y0], [x1, y0], [x1, y1], [x0, y1], [x0, y0]]
  const walkway: Deck = { ...deck, kind: 'path', edges: [1.5, 1.5] }
  const plain = bakeBridges({ ...input, decks: [walkway] }, MATERIAL_PALETTE).mesh.position.length
  const modelled = bakeBridges({ ...input, decks: [walkway], structures: [{ ring: around, centre: points[1], radius: 200 }] }, MATERIAL_PALETTE).mesh.position.length
  expect(plain).toBeGreaterThan(0)
  expect(modelled).toBe(0)
})

test('lays a street deck\'s carriageway beside a low walkway the model draws', () => {
  const north = (m: number) => m / 30_400_000
  const walkway: Deck = { ...deck, id: 'walk', kind: 'path', edges: [1.5, 1.5], grounded: [false, false], heights: [0.5, 1, 0.5], ground: [0, 0, 0],
    points: points.map(([x, y]) => [x, y - north(5)] as [number, number]) }
  // The road deck's own carriageway, laid on it as on a street deck, and on the ground just past its ends.
  const street = { properties: { bridge: true, kind: 'carriageway' }, geometry: { type: 'Polygon', coordinates: [[[-74.0003, 40.69997], [-73.9977, 40.69997], [-73.9977, 40.70003], [-74.0003, 40.70003], [-74.0003, 40.69997]]] } }
  const past = { ...street, geometry: { type: 'Polygon', coordinates: [[[-74.0002, 40.69997], [-74.0001, 40.69997], [-74.0001, 40.70003], [-74.0002, 40.70003], [-74.0002, 40.69997]]] } }
  const [x0, y0] = mercator(-74.01, 40.71), [x1, y1] = mercator(-73.99, 40.69)
  const around: Array<[number, number]> = [[x0, y0], [x1, y0], [x1, y1], [x0, y1], [x0, y0]]
  const baked = bakeBridges({ ...input, decks: [deck, walkway], surfaces: [street, past], structures: [{ ring: around, centre: walkway.points[1], radius: 200 }] }, MATERIAL_PALETTE)
  expect(baked.mesh.position.length).toBeGreaterThan(0)
  expect(baked.mesh.position.every(Number.isFinite)).toBe(true)
})

test('lays no other bridge\'s carriageway on the ground beside a deck\'s end', () => {
  const metre = 1 / 30_400_000
  // A street deck sampled every 5 m, its carriageway laid on it; 12 m in from its west end, another bridge's carriageway crosses overhead with no deck here.
  const dense = Array.from({ length: 35 }, (_, k) => mercator(-74 + (k * 0.002) / 34, 40.7))
  const street: Deck = { ...deck, points: dense, heights: dense.map((_, k) => 8 * Math.sin((Math.PI * k) / 34)), ground: dense.map(() => 0), midpoint: dense[17] }
  const own = { properties: { bridge: true, kind: 'carriageway' }, geometry: { type: 'Polygon', coordinates: [[[-74.0003, 40.69997], [-73.9977, 40.69997], [-73.9977, 40.70003], [-74.0003, 40.70003], [-74.0003, 40.69997]]] } }
  const across = { properties: { bridge: true, kind: 'carriageway' }, geometry: { type: 'Polygon', coordinates: [[[-73.99989, 40.6998], [-73.99981, 40.6998], [-73.99981, 40.7002], [-73.99989, 40.7002], [-73.99989, 40.6998]]] } }
  const { mesh, origin } = bakeBridges({ ...input, decks: [street], surfaces: [own, across] }, MATERIAL_PALETTE)
  const [x, y] = mercator(-73.99985, 40.7)
  let stray = 0
  for (let i = 0; i < mesh.position.length; i += 3) {
    const [dx, dy, z] = [(mesh.position[i] + origin[0] - x) / metre, (y - mesh.position[i + 1] - origin[1]) / metre, mesh.position[i + 2] / metre]
    if (mesh.color[i] === 0 && Math.abs(dx) < 5 && Math.abs(dy) > 4.5 && Math.abs(dy) < 12 && z < 0.5) stray++
  }
  expect(stray).toBe(0)
})

test('lifts paint as the deck top under it is lifted, so bumpy ground beneath does not sink it into the deck', () => {
  const result = bakeBridges(input, MATERIAL_PALETTE)
  const { mesh } = result
  const top = Array.from({ length: result.ranges.paintFrom / 3 }, (_, k) => k * 3).find(t => mesh.normal[t * 3 + 2] > 0.9 && mesh.position[t * 3 + 2] > 0)!
  const corner = (k: number) => mesh.position.slice((top + k) * 3, (top + k) * 3 + 3)
  const weights = [[0.6, 0.2, 0.2], [0.2, 0.6, 0.2], [0.2, 0.2, 0.6]]
  for (const w of weights) {
    for (let axis = 0; axis < 3; axis++) mesh.position.push(w.reduce((sum, wk, k) => sum + wk * corner(k)[axis], 0))
    for (const stream of ['normal', 'color', 'low', 'spread'] as const) mesh[stream]!.push(...mesh[stream]!.slice(top * 3, top * 3 + 3))
    mesh.bias!.push(mesh.bias![top])
  }
  const bumpy = (p: [number, number]) => 3 * Math.sin(p[0] * 4e6) * Math.cos(p[1] * 3e6)
  const { lift, indices } = decodeBridge(encodeBridge(result, bumpy))
  const liftOf = (v: number) => lift[indices[v]]
  const painted = mesh.position.length / 3 - 3
  weights.forEach((w, k) => expect(liftOf(painted + k)).toBeCloseTo(w.reduce((sum, wk, j) => sum + wk * liftOf(top + j), 0), 9))
})

test('runs a cycle track on over the deck past the end of the bridge way it starts on', () => {
  const way = (id: string, from: number, to: number, brunnel: string | null) => ({
    properties: { id, class: 'path', brunnel, cycle: true },
    geometry: { type: 'LineString', coordinates: [[from, 40.7], [to, 40.7]] },
  })
  const roads = [way('1', -73.9995, -73.999, 'bridge'), way('2', -73.999, -73.9985, null)]
  const { mesh } = bakeBridges({ ...input, decks: [{ ...deck, heights: [8, 8, 8], grounded: [false, false] }], roads }, MATERIAL_PALETTE)
  const green = MATERIAL_PALETTE.green[0]
  const xs = mesh.position.filter((_, k) => k % 3 === 0 && mesh.color[k] === green).map(x => x + points[1][0])
  expect(Math.max(...xs)).toBeGreaterThan(mercator(-73.9986, 40.7)[0])
})
