import { metresPerUnit } from './decks'
import type { BakedBridge, DeckPalette } from './bake'

export const MATERIALS = ['surface', 'concrete', 'parapet', 'track', 'deck', 'lowRoad', 'lowMajor', 'lowHighway', 'lowPath', 'walkway', 'rail', 'white', 'yellow', 'green', 'red', 'route', 'routeCasing'] as const
export const MATERIAL_PALETTE = Object.fromEntries(MATERIALS.map((name, i) => [name, [i, 0, 0]])) as DeckPalette
const STREAMS = ['position', 'normal', 'color', 'low', 'bias', 'spread'] as const

export function encodeBridge(built: BakedBridge, at: (point: [number, number]) => number | null = () => null): Uint8Array {
  let arrays: Array<Float32Array | Uint32Array> = STREAMS.map(name => new Float32Array(built.mesh[name] ?? []))
  const ground = new Float32Array(built.mesh.position.length / 3)
  const sampled = new Map<string, number | null>()
  for (let i = 0; i < ground.length; i++) {
    const p: [number, number] = [built.mesh.position[i * 3] + built.origin[0], built.mesh.position[i * 3 + 1] + built.origin[1]]
    const key = p.join(',')
    if (!sampled.has(key)) sampled.set(key, at(p))
    const h = sampled.get(key)
    ground[i] = h == null ? built.mesh.position[i * 3 + 2] : h / metresPerUnit(p[1])
  }
  arrays.push(ground)
  const strides = [3, 3, 3, 3, 1, 3, 1]
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
    version: 1, origin: built.origin, ranges: built.ranges, stands: built.stands, under: built.under, tracks: built.tracks,
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
