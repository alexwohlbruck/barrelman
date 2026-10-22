import { metresPerUnit, type Point } from './decks'
import type { BakedBridge, DeckPalette } from './bake'

export const MATERIALS = ['surface', 'concrete', 'parapet', 'track', 'deck', 'lowRoad', 'lowMajor', 'lowHighway', 'lowPath', 'walkway', 'rail', 'white', 'yellow', 'green', 'red', 'route', 'routeCasing'] as const
export const MATERIAL_PALETTE = Object.fromEntries(MATERIALS.map((name, i) => [name, [i, 0, 0]])) as DeckPalette
const STREAMS = ['position', 'normal', 'color', 'low', 'bias', 'spread'] as const

/** Metres between the nodes of the smoothed ground a deck is lifted with, and how far each node reads around it. */
const DATUM_CELL = 25
/** Metres above the ground over which a structure goes from following it, foot on the ground, to the smoothed ground. */
const LIFT_BLEND = 6

/**
 * The ground a structure is lifted with as the map exaggerates terrain: the
 * terrain itself at its feet, and higher up the terrain smoothed over
 * DATUM_CELL, so a deck keeps its true height above the ground it spans as
 * buildings and landmarks do, without copying every bump beneath it. A
 * function of position alone, so decks that meet are lifted alike.
 */
export function liftField(at: (point: Point) => number | null) {
  const nodes = new Map<string, number | null>()
  const node = (i: number, j: number, cell: number): number | null => {
    const key = `${i},${j}`
    if (!nodes.has(key)) {
      const heights = [-1, 0, 1].flatMap(dx => [-1, 0, 1].map(dy => at([(i + dx) * cell, (j + dy) * cell]))).filter((h): h is number => h !== null && Number.isFinite(h))
      nodes.set(key, heights.length ? heights.reduce((a, b) => a + b, 0) / heights.length : null)
    }
    return nodes.get(key)!
  }
  const datum = ([x, y]: Point): number | null => {
    const cell = DATUM_CELL / metresPerUnit(y)
    const [fx, fy] = [x / cell, y / cell]
    const [i, j] = [Math.floor(fx), Math.floor(fy)]
    const [tx, ty] = [fx - i, fy - j]
    const corners = [node(i, j, cell), node(i + 1, j, cell), node(i, j + 1, cell), node(i + 1, j + 1, cell)]
    if (corners.some(c => c === null)) return null
    const [a, b, c, d] = corners as number[]
    return (a * (1 - tx) + b * tx) * (1 - ty) + (c * (1 - tx) + d * tx) * ty
  }
  /** Metres a point `z` metres up, over `ground`, rises per unit of exaggeration past 1; z itself where the ground is unknown. */
  return (p: Point, z: number, ground: number | null): number => {
    const smooth = datum(p)
    if (ground === null || smooth === null) return z
    const w = Math.min(1, Math.max(0, 1 - (z - ground) / LIFT_BLEND))
    return smooth + (ground - smooth) * w
  }
}

export function encodeBridge(built: BakedBridge, at: (point: [number, number]) => number | null = () => null): Uint8Array {
  let arrays: Array<Float32Array | Uint32Array> = STREAMS.map(name => new Float32Array(built.mesh[name] ?? []))
  const ground = new Float32Array(built.mesh.position.length / 3)
  const lift = new Float32Array(ground.length)
  const lifted = liftField(at)
  const sampled = new Map<string, number | null>()
  for (let i = 0; i < ground.length; i++) {
    const p: [number, number] = [built.mesh.position[i * 3] + built.origin[0], built.mesh.position[i * 3 + 1] + built.origin[1]]
    const key = p.join(',')
    if (!sampled.has(key)) sampled.set(key, at(p))
    const h = sampled.get(key) ?? null
    const scale = metresPerUnit(p[1])
    const z = built.mesh.position[i * 3 + 2]
    ground[i] = h === null ? z : h / scale
    lift[i] = lifted(p, z * scale, h) / scale
  }
  arrays.push(ground, lift)
  for (const { surface } of built.stands)
    surface.lift = surface.points.map((p, i) => Math.round(lifted(p, surface.z[i], at(p)) * 100) / 100)
  const strides = [3, 3, 3, 3, 1, 3, 1, 1]
  const unique = arrays.map(() => [] as number[])
  const seen = new Map<string, number>()
  const indices = new Uint32Array(ground.length)
  for (let i = 0; i < ground.length; i++) {
    const values = arrays.flatMap((array, stream) => Array.from(array.subarray(i * strides[stream], (i + 1) * strides[stream])))
    const key = values.join(',')
    let index = seen.get(key)
    if (index === undefined) {
      index = seen.size
      seen.set(key, index)
      for (let stream = 0, at = 0; stream < strides.length; stream++)
        for (let k = 0; k < strides[stream]; k++) unique[stream].push(values[at++])
    }
    indices[i] = index
  }
  arrays = [...unique.map(values => new Float32Array(values)), indices]
  const header = new TextEncoder().encode(JSON.stringify({
    version: 2, origin: built.origin, ranges: built.ranges, stands: built.stands, under: built.under, tracks: built.tracks,
    lengths: arrays.map(a => a.length),
  }))
  const start = 12 + Math.ceil(header.length / 4) * 4
  const bytes = new Uint8Array(start + arrays.reduce((n, a) => n + a.byteLength, 0))
  const view = new DataView(bytes.buffer)
  view.setUint32(0, 0x314d5242, true)
  view.setUint32(4, header.length, true)
  view.setUint32(8, start, true)
  bytes.set(header, 12)
  let offset = start
  for (const array of arrays) {
    bytes.set(new Uint8Array(array.buffer), offset)
    offset += array.byteLength
  }
  return bytes
}
