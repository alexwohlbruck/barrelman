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
  const arrays = STREAMS.map(name => new Float32Array(built.mesh[name] ?? []))
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
  // A vertex is keyed by the bits of its values, read as UTF-16 units: far cheaper than printing them.
  const values = new Float32Array(strides.reduce((a, b) => a + b, 0))
  const bits = new Uint16Array(values.buffer)
  for (let i = 0; i < ground.length; i++) {
    for (let stream = 0, at = 0; stream < strides.length; stream++)
      for (let k = 0; k < strides[stream]; k++) values[at++] = arrays[stream][i * strides[stream] + k]
    const key = String.fromCharCode(...bits)
    let index = seen.get(key)
    if (index === undefined) {
      index = seen.size
      seen.set(key, index)
      for (let stream = 0, at = 0; stream < strides.length; stream++)
        for (let k = 0; k < strides[stream]; k++) unique[stream].push(values[at++])
    }
    indices[i] = index
  }
  const geometry = JSON.parse(JSON.stringify({ stands: built.stands, under: built.under, tracks: built.tracks }, rounded))
  const header = new TextEncoder().encode(JSON.stringify({
    version: 3, quantum: QUANTUM, origin: built.origin, ranges: built.ranges, ...geometry,
    lengths: [...unique.map(values => values.length), indices.length],
  }))
  const streams = [...unique.map((values, i) => pack(LAYOUT[i], values)), pack(INDEX_LAYOUT, Array.from(indices))]
  const start = align(12 + header.length)
  const bytes = new Uint8Array(streams.reduce((n, s) => align(n + s.byteLength), start))
  const view = new DataView(bytes.buffer)
  view.setUint32(0, 0x314d5242, true)
  view.setUint32(4, header.length, true)
  view.setUint32(8, start, true)
  bytes.set(header, 12)
  streams.reduce((offset, s) => { bytes.set(new Uint8Array(s.buffer, s.byteOffset, s.byteLength), offset); return align(offset + s.byteLength) }, start)
  return bytes
}

/** Mercator units per step of the integer streams: 3 mm at 40°. */
const QUANTUM = 1e-10
type Layout = { type: Int32ArrayConstructor | Int8ArrayConstructor | Uint8ArrayConstructor | Float32ArrayConstructor; delta?: number; scale?: number }
/**
 * How each vertex stream is stored: spatial ones as QUANTUM steps, each
 * component the difference from the vertex before (`delta` is the stride), so
 * gzip finds the repeats a float hides; unit normals as bytes; palette slots as
 * the integers they are.
 */
const LAYOUT: Layout[] = [
  { type: Int32Array, delta: 3, scale: QUANTUM }, { type: Int8Array, scale: 1 / 127 }, { type: Uint8Array }, { type: Uint8Array },
  { type: Float32Array }, { type: Float32Array }, { type: Int32Array, delta: 1, scale: QUANTUM }, { type: Int32Array, delta: 1, scale: QUANTUM },
]
const INDEX_LAYOUT: Layout = { type: Int32Array, delta: 1 }
/** Header numbers to 3 mm: mercator coordinates (all under 1) to 10 places, metres to 3. */
const rounded = (_: string, value: unknown) => typeof value === 'number' && !Number.isInteger(value) ? +value.toFixed(Math.abs(value) < 1 ? 10 : 3) : value
const align = (n: number) => Math.ceil(n / 4) * 4

function pack({ type, delta = 0, scale = 1 }: Layout, values: number[]) {
  const out = new type(values.length)
  const steps = values.map(v => type === Float32Array ? v : Math.round(v / scale))
  if (type === Uint8Array && steps.some(v => v < 0 || v > 255)) throw new Error('Palette slot out of byte range')
  for (let i = 0; i < steps.length; i++) out[i] = steps[i] - (delta && i >= delta ? steps[i - delta] : 0)
  return out
}

/** Reads an encoded mesh back into float streams and indices, as the client draws them. */
export function decodeBridge(bytes: Uint8Array) {
  const view = new DataView(bytes.buffer, bytes.byteOffset, bytes.byteLength)
  const start = view.getUint32(8, true)
  const header = JSON.parse(new TextDecoder().decode(bytes.subarray(12, 12 + view.getUint32(4, true))))
  let offset = start
  const read = ({ type, delta = 0, scale = 1 }: Layout, length: number) => {
    const raw = new type((bytes.buffer as ArrayBuffer).slice(bytes.byteOffset + offset, bytes.byteOffset + offset + length * type.BYTES_PER_ELEMENT))
    offset = align(offset + raw.byteLength)
    const out = new Float64Array(length)
    for (let i = 0; i < length; i++) out[i] = raw[i] + (delta && i >= delta ? out[i - delta] : 0)
    return type === Float32Array ? Array.from(raw) : Array.from(out, v => v * scale)
  }
  const streams = [...STREAMS, 'ground', 'lift'].map((name, i) => [name, read(LAYOUT[i], header.lengths[i])] as const)
  return { header, ...Object.fromEntries(streams), indices: read(INDEX_LAYOUT, header.lengths.at(-1)) } as Record<typeof STREAMS[number] | 'ground' | 'lift' | 'indices', number[]> & { header: any }
}
