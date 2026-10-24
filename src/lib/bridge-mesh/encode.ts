import { metresPerUnit, type Point } from './decks'
import type { BakedBridge, DeckPalette } from './bake'

export const MATERIALS = ['surface', 'concrete', 'parapet', 'track', 'deck', 'lowRoad', 'lowMajor', 'lowHighway', 'lowPath', 'walkway', 'rail', 'white', 'yellow', 'green', 'red', 'route', 'routeCasing'] as const
export const MATERIAL_PALETTE = Object.fromEntries(MATERIALS.map((name, i) => [name, [i, 0, 0]])) as DeckPalette
const STREAMS = ['position', 'normal', 'color', 'low', 'bias', 'spread'] as const

/** Mercator units between the nodes of the smoothed ground a deck is lifted with (25 m at the equator, 19 m at 40°), and how many of them each node reads around it. */
const DATUM_CELL = 25 / 40075016.686
const DATUM_READ = 2
/** Metres above the ground over which a structure goes from following it, foot on the ground, to the smoothed ground. */
const LIFT_BLEND = 6

/** Uniform cubic B-spline weights at t in [0, 1) for the four nodes around it. */
const spline = (t: number) => [(1 - t) ** 3 / 6, (3 * t ** 3 - 6 * t ** 2 + 4) / 6, (-3 * t ** 3 + 3 * t ** 2 + 3 * t + 1) / 6, t ** 3 / 6]

/**
 * The ground a structure is lifted with as the map exaggerates terrain: the
 * terrain itself at its feet, and higher up the terrain smoothed over a few
 * DATUM_CELLs, so a deck keeps its true height above the ground it spans as
 * buildings and landmarks do, without copying every cutting beneath it (a
 * median, so a narrow road or stream below does not pull it down) or kinking
 * where it passes from water to a bank (a cubic spline over the medians). A
 * function of position alone, so decks that meet are lifted alike.
 */
export function liftField(at: (point: Point) => number | null) {
  const nodes = new Map<string, number | null>()
  const offsets = Array.from({ length: 4 * DATUM_READ + 1 }, (_, k) => k / 2 - DATUM_READ)
  const node = (i: number, j: number): number | null => {
    const key = `${i},${j}`
    if (!nodes.has(key)) {
      const heights = offsets.flatMap(dx => offsets.map(dy => at([(i + dx) * DATUM_CELL, (j + dy) * DATUM_CELL])))
        .filter((h): h is number => h !== null && Number.isFinite(h)).sort((a, b) => a - b)
      nodes.set(key, heights.length ? heights[heights.length >> 1] : null)
    }
    return nodes.get(key)!
  }
  const datum = ([x, y]: Point): number | null => {
    const [fx, fy] = [x / DATUM_CELL, y / DATUM_CELL]
    const [i, j] = [Math.floor(fx), Math.floor(fy)]
    const [wx, wy] = [spline(fx - i), spline(fy - j)]
    let sum = 0
    for (let a = 0; a < 4; a++)
      for (let b = 0; b < 4; b++) {
        const h = node(i + a - 1, j + b - 1)
        if (h === null) return null
        sum += h * wx[a] * wy[b]
      }
    return sum
  }
  /** Metres a point `z` metres up, over `ground`, rises per unit of exaggeration past 1; z itself where the ground is unknown. */
  return (p: Point, z: number, ground: number | null): number => {
    const smooth = datum(p)
    if (ground === null || smooth === null) return z
    const w = Math.min(1, Math.max(0, 1 - (z - ground) / LIFT_BLEND))
    return smooth + (ground - smooth) * w
  }
}

/** Metres a mark may lie off the deck top under it and still take that top's lift. */
const ON_TOP = 0.5

/** Paint, rails and routes take the lift of the deck-top triangle under them, as the GPU interpolates it there. */
function onTops(built: BakedBridge, lift: Float32Array) {
  const { position, normal } = built.mesh
  const from = built.ranges.paintFrom
  const cell = 4 / metresPerUnit(built.origin[1])
  const grid = new Map<string, number[]>()
  for (let t = 0; t < from; t += 3) {
    if (normal[t * 3 + 2] < 0.3) continue
    const xs = [0, 1, 2].map(k => position[(t + k) * 3]), ys = [0, 1, 2].map(k => position[(t + k) * 3 + 1])
    for (let i = Math.floor(Math.min(...xs) / cell); i <= Math.floor(Math.max(...xs) / cell); i++)
      for (let j = Math.floor(Math.min(...ys) / cell); j <= Math.floor(Math.max(...ys) / cell); j++) {
        const key = `${i},${j}`
        const bucket = grid.get(key)
        if (bucket) bucket.push(t)
        else grid.set(key, [t])
      }
  }
  const tolerance = ON_TOP / metresPerUnit(built.origin[1])
  for (let v = from; v < lift.length; v++) {
    const [x, y, z] = [position[v * 3], position[v * 3 + 1], position[v * 3 + 2]]
    let best: { off: number; lift: number } | null = null
    for (const t of grid.get(`${Math.floor(x / cell)},${Math.floor(y / cell)}`) ?? []) {
      const [a, b, c] = [t, t + 1, t + 2].map(k => [position[k * 3], position[k * 3 + 1], position[k * 3 + 2]])
      const det = (b[1] - c[1]) * (a[0] - c[0]) + (c[0] - b[0]) * (a[1] - c[1])
      if (!det) continue
      const w0 = ((b[1] - c[1]) * (x - c[0]) + (c[0] - b[0]) * (y - c[1])) / det
      const w1 = ((c[1] - a[1]) * (x - c[0]) + (a[0] - c[0]) * (y - c[1])) / det
      const w2 = 1 - w0 - w1
      if (w0 < -1e-6 || w1 < -1e-6 || w2 < -1e-6) continue
      const off = Math.abs(z - (w0 * a[2] + w1 * b[2] + w2 * c[2]))
      if (off > tolerance || (best && off >= best.off)) continue
      best = { off, lift: w0 * lift[t] + w1 * lift[t + 1] + w2 * lift[t + 2] }
    }
    if (best) lift[v] = best.lift
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
  onTops(built, lift)
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
