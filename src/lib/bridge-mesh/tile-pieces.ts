import { LRUCache } from 'lru-cache'
import { createHash } from 'node:crypto'
import { join } from 'node:path'
import { MATERIALS } from './encode'

/**
 * Deck meshes cut to vector tiles, so a client gets the decks of a tile in the
 * tile itself: no second request, and only the part of each deck the tile holds.
 *
 * A piece is a BRM v3 mesh holding the triangles of one deck whose centroid
 * lies in the tile, so the pieces of one zoom's tiles partition every deck.
 * Below DETAIL_ZOOM the paint and rails are left out: the client draws them
 * only from that zoom, and a lower tile is only drawn in the distance there.
 * At the lowest zoom, drawn only far off or zoomed out, parapets and the
 * ground ways under each deck (which tell objects on it from those below) are left out too.
 */
export const DECK_TILE_ZOOMS = { min: 14, max: 16 } as const
const DETAIL_ZOOM = 16
const PARAPET = MATERIALS.indexOf('parapet')
/** The protobuf field of an MVT `Tile` the pieces travel in: an extension number, skipped by every MVT reader. */
export const DECK_TILE_FIELD = 20
const MAGIC = 0x31544442

const STRIDES = [3, 3, 3, 3, 1, 3, 1, 1]
type Layout = { type: Int32ArrayConstructor | Int8ArrayConstructor | Uint8ArrayConstructor | Float32ArrayConstructor; delta?: number }
const LAYOUT: Layout[] = [
  { type: Int32Array, delta: 3 }, { type: Int8Array }, { type: Uint8Array }, { type: Uint8Array },
  { type: Float32Array }, { type: Float32Array }, { type: Int32Array, delta: 1 }, { type: Int32Array, delta: 1 },
]
const align = (n: number) => Math.ceil(n / 4) * 4

/** A BRM v3 mesh as its header and raw streams: integers summed out of their deltas, the rest as stored. */
export type Streams = { header: any; streams: Array<Int32Array | Int8Array | Uint8Array | Float32Array>; indices: Int32Array; headerBytes?: number; cells?: Map<string, number[]> }

export function readStreams(bytes: Uint8Array): Streams {
  const view = new DataView(bytes.buffer, bytes.byteOffset, bytes.byteLength)
  if (view.getUint32(0, true) !== 0x314d5242) throw new Error('Not a bridge mesh')
  const header = JSON.parse(new TextDecoder().decode(bytes.subarray(12, 12 + view.getUint32(4, true))))
  if (header.version !== 3) throw new Error(`Bridge mesh version ${header.version} cannot be cut`)
  let offset = view.getUint32(8, true)
  const read = ({ type, delta = 0 }: Layout, length: number) => {
    const out = new type(length)
    out.set(new type(bytes.buffer.slice(bytes.byteOffset + offset, bytes.byteOffset + offset + length * type.BYTES_PER_ELEMENT) as ArrayBuffer) as any)
    offset = align(offset + length * type.BYTES_PER_ELEMENT)
    if (delta) for (let i = delta; i < length; i++) out[i] += out[i - delta]
    return out
  }
  const streams = LAYOUT.map((layout, i) => read(layout, header.lengths[i]))
  const indices = read({ type: Int32Array, delta: 1 }, header.lengths[LAYOUT.length]) as Int32Array
  return { header, streams, indices, headerBytes: view.getUint32(4, true) }
}

function writeStreams(header: object, streams: Streams['streams'], indices: Int32Array): Uint8Array {
  const text = new TextEncoder().encode(JSON.stringify(header))
  const packed = [...streams.map((s, i) => deltas(s, LAYOUT[i].delta ?? 0)), deltas(indices, 1)]
  const start = align(12 + text.length)
  const bytes = new Uint8Array(packed.reduce((n, s) => align(n + s.byteLength), start))
  const view = new DataView(bytes.buffer)
  view.setUint32(0, 0x314d5242, true)
  view.setUint32(4, text.length, true)
  view.setUint32(8, start, true)
  bytes.set(text, 12)
  packed.reduce((at, s) => { bytes.set(new Uint8Array(s.buffer, s.byteOffset, s.byteLength), at); return align(at + s.byteLength) }, start)
  return bytes
}

function deltas<T extends Streams['streams'][number]>(values: T, delta: number): T {
  if (!delta) return values
  const out = values.slice() as T
  for (let i = values.length - 1; i >= delta; i--) out[i] -= values[i - delta]
  return out
}

/** The zoom triangles are bucketed at once per mesh, so a cut at or above it reads only its own. */
const CELL_ZOOM = 16

/** Each triangle (by its first index) under the zoom-CELL_ZOOM tile its centroid lies in, worked out once per mesh. */
function cellsOf(mesh: Streams): Map<string, number[]> {
  if (mesh.cells) return mesh.cells
  const { header, indices } = mesh, position = mesh.streams[0] as Int32Array
  const [ox, oy] = header.origin as [number, number], q = header.quantum as number, n = 2 ** CELL_ZOOM
  const cells = new Map<string, number[]>()
  for (let t = 0; t < indices.length; t += 3) {
    const a = indices[t] * 3, b = indices[t + 1] * 3, c = indices[t + 2] * 3
    const key = `${Math.floor((ox + ((position[a] + position[b] + position[c]) / 3) * q) * n)}/${Math.floor((oy + ((position[a + 1] + position[b + 1] + position[c + 1]) / 3) * q) * n)}`
    const list = cells.get(key)
    if (list) list.push(t)
    else cells.set(key, [t])
  }
  return (mesh.cells = cells)
}

/** The triangles of a mesh whose centroid lies in tile z/x/y, as a mesh of their own; null when there are none. */
export function cutPiece(mesh: Streams, z: number, x: number, y: number): Uint8Array | null {
  const { header, streams, indices } = mesh
  const n = 2 ** z
  const [ox, oy] = header.origin as [number, number]
  const position = streams[0] as Int32Array
  const q = header.quantum as number
  const [minX, minY, maxX, maxY] = [x / n - ox, y / n - oy, (x + 1) / n - ox, (y + 1) / n - oy].map(v => v / q)
  const { paintFrom, railsFrom, routesFrom } = header.ranges
  const detail = z >= DETAIL_ZOOM, coarse = z <= DECK_TILE_ZOOMS.min
  const color = streams[2] as Uint8Array
  const keep: number[] = []
  const counts = [0, 0, 0, 0]
  const take = (t: number) => {
    if (!detail && t >= paintFrom && t < routesFrom) return
    if (coarse && t < paintFrom && color[indices[t] * 3] === PARAPET) return
    keep.push(t)
    counts[t < paintFrom ? 0 : t < railsFrom ? 1 : t < routesFrom ? 2 : 3]++
  }
  if (z <= CELL_ZOOM) {
    const cells = cellsOf(mesh), up = CELL_ZOOM - z, found: number[] = []
    for (let i = x << up; i < (x + 1) << up; i++)
      for (let j = y << up; j < (y + 1) << up; j++) for (const t of cells.get(`${i}/${j}`) ?? []) found.push(t)
    found.sort((a, b) => a - b).forEach(take)
  } else {
    for (let t = 0; t < indices.length; t += 3) {
      const [a, b, c] = [indices[t] * 3, indices[t + 1] * 3, indices[t + 2] * 3]
      const cx = (position[a] + position[b] + position[c]) / 3, cy = (position[a + 1] + position[b + 1] + position[c + 1]) / 3
      if (cx >= minX && cx < maxX && cy >= minY && cy < maxY) take(t)
    }
  }
  if (!keep.length) return null
  const remap = new Int32Array(position.length / 3).fill(-1)
  const used: number[] = []
  const out = new Int32Array(keep.length * 3)
  keep.forEach((t, k) => {
    for (let j = 0; j < 3; j++) {
      const v = indices[t + j]
      if (remap[v] < 0) { remap[v] = used.length; used.push(v) }
      out[k * 3 + j] = remap[v]
    }
  })
  const picked = streams.map((s, i) => {
    const stride = STRIDES[i]
    const o = new (s.constructor as any)(used.length * stride)
    used.forEach((v, k) => { for (let j = 0; j < stride; j++) o[k * stride + j] = s[v * stride + j] })
    return o
  })
  const ranges = { paintFrom: counts[0] * 3, railsFrom: (counts[0] + counts[1]) * 3, routesFrom: (counts[0] + counts[1] + counts[2]) * 3 }
  const area = { minX: x / n, minY: y / n, maxX: (x + 1) / n, maxY: (y + 1) / n }
  const stands = header.stands.flatMap((s: Stand) => cutStand(s, area))
  const geometry = coarse ? { stands, under: [], tracks: [] } : { stands, under: cutWays(header.under, area), tracks: cutWays(header.tracks, area) }
  return writeStreams({ ...header, ...geometry, ranges, lengths: [...picked.map(p => p.length), out.length] }, picked, out)
}

type Box = { minX: number; minY: number; maxX: number; maxY: number }
type Pt = [number, number]
type Stand = { chain: { points: Pt[]; sides?: [number[], number[]]; [k: string]: unknown }; surface: { points: Pt[]; width: number; bounds?: Box; tops?: unknown[]; edges?: [Pt[], Pt[]]; [k: string]: unknown } }
/** Metres past a tile's edge whose ground ways and deck surface it carries, beyond each deck's half width: what a point in the tile may look at. */
const REACH = 20
const metres = (y: number) => 40075016.686 * Math.cos(Math.atan(Math.sinh(Math.PI * (1 - 2 * y))))
const grow = (b: Box, by: number): Box => ({ minX: b.minX - by, minY: b.minY - by, maxX: b.maxX + by, maxY: b.maxY + by })
const meets = (a: Box, b: Box) => a.minX <= b.maxX && b.minX <= a.maxX && a.minY <= b.maxY && b.minY <= a.maxY
const inside = (b: Box, [x, y]: Pt) => x >= b.minX && x <= b.maxX && y >= b.minY && y <= b.maxY

/** Ground ways under a deck that come near a tile. */
function cutWays(ways: Array<{ bounds: Box }> | undefined, area: Box) {
  if (!ways) return ways
  const near = grow(area, REACH / metres(area.minY))
  return ways.filter(w => meets(w.bounds, near))
}

/** The stretch of a deck's surface over and near a tile, its per-sample arrays cut alike; none when it does not come near. */
export function cutStand(stand: Stand, area: Box): Stand[] {
  const { chain, surface } = stand
  const pts = surface.points, n = pts.length
  const near = grow(area, (surface.width / 2 + REACH) / metres(area.minY))
  if (surface.bounds && !meets(surface.bounds, near)) return []
  // A segment counts when either end, or the middle of a long one, is near.
  let first = -1, last = -1
  for (let i = 0; i < n; i++) {
    const hit = inside(near, pts[i]) || (i + 1 < n && inside(near, [(pts[i][0] + pts[i + 1][0]) / 2, (pts[i][1] + pts[i + 1][1]) / 2]))
    if (hit) { if (first < 0) first = i; last = i }
  }
  if (first < 0) return []
  const [i0, i1] = [Math.max(0, first - 1), Math.min(n - 1, last + 2)]
  if (i0 === 0 && i1 === n - 1) return [stand]
  if (chain.points.length !== n) return [stand]
  const slice = (v: unknown, len: number, a: number, b: number) => (Array.isArray(v) && v.length === len ? v.slice(a, b) : v)
  const cut = Object.fromEntries(Object.entries(surface).map(([k, v]) => [k, k === 'tops' ? slice(v, n - 1, i0, i1) : slice(v, n, i0, i1 + 1)]))
  if (surface.edges) cut.edges = surface.edges.map(e => slice(e, n, i0, i1 + 1))
  if (surface.bounds) {
    let box = { minX: Infinity, minY: Infinity, maxX: -Infinity, maxY: -Infinity }
    for (const [px, py] of cut.points as Pt[]) box = { minX: Math.min(box.minX, px), minY: Math.min(box.minY, py), maxX: Math.max(box.maxX, px), maxY: Math.max(box.maxY, py) }
    // As the bake bounds a surface: its points grown by its width.
    cut.bounds = grow(box, surface.width / metres((cut.points as Pt[])[0][1]))
  }
  const cutChain = { ...chain, points: chain.points.slice(i0, i1 + 1), ...(chain.sides ? { sides: chain.sides.map(s => s.slice(i0, i1 + 1)) as [number[], number[]] } : {}) }
  return [{ chain: cutChain, surface: cut as Stand['surface'] }]
}

/** A tile's pieces: `BDT1`, a JSON header naming each piece's deck mesh and its length, then the pieces, 4-byte aligned. */
export function encodeDeckTile(pieces: Array<{ file: string; bytes: Uint8Array }>, failed: string[] = []): Uint8Array {
  const text = new TextEncoder().encode(JSON.stringify({ decks: pieces.map(p => p.file), lengths: pieces.map(p => p.bytes.byteLength), ...(failed.length ? { failed } : {}) }))
  const start = align(8 + text.length)
  const out = new Uint8Array(pieces.reduce((n, p) => align(n + p.bytes.byteLength), start))
  const view = new DataView(out.buffer)
  view.setUint32(0, MAGIC, true)
  view.setUint32(4, text.length, true)
  out.set(text, 8)
  pieces.reduce((at, p) => { out.set(p.bytes, at); return align(at + p.bytes.byteLength) }, start)
  return out
}

/** An MVT with the deck tile appended as field DECK_TILE_FIELD: a length-delimited field after the layers. */
export function withDeckField(mvt: Uint8Array, payload: Uint8Array): Uint8Array<ArrayBuffer> {
  const varint = (v: number) => { const b: number[] = []; while (v > 127) { b.push((v & 127) | 128); v = Math.floor(v / 128) } b.push(v); return b }
  const key = varint((DECK_TILE_FIELD << 3) | 2), len = varint(payload.byteLength)
  const out = new Uint8Array(mvt.byteLength + key.length + len.length + payload.byteLength)
  out.set(mvt, 0)
  out.set(key, mvt.byteLength)
  out.set(len, mvt.byteLength + key.length)
  out.set(payload, mvt.byteLength + key.length + len.length)
  return out
}

/** Deck mesh files that may have triangles in a tile. */
export type DeckFilesIn = (z: number, x: number, y: number) => Promise<string[]>

/** A tile's deck payload, and whether every deck in it could be read: one that could not is not cached, anywhere. */
export type DeckTile = { bytes: Uint8Array; complete: boolean }

/** Builds a deck tile from the mesh files of the decks it meets. */
export type DeckTileBuilder = (z: number, x: number, y: number, files: string[]) => Promise<DeckTile>

/** Cuts deck tiles from the mesh files in `directory`, keeping the meshes it has read. */
export function deckTileBuilder(directory: string, { meshBytes = 384 << 20 } = {}): DeckTileBuilder {
  // A parsed header weighs about three times its JSON as objects; a triangle's cell entry about 8 bytes.
  const meshes = new LRUCache<string, Streams>({ maxSize: meshBytes, sizeCalculation: m => m.streams.reduce((n, s) => n + s.byteLength, m.indices.byteLength) + 3 * (m.headerBytes ?? 0) + (8 * m.indices.length) / 3 + 1 })
  // An unreadable mesh is not tried again on every request for a while.
  const unreadable = new LRUCache<string, true>({ max: 10_000, ttl: 300_000 })
  const mesh = async (file: string) => {
    const hit = meshes.get(file)
    if (hit) return hit
    if (unreadable.has(file)) return null
    try {
      const gz = Bun.file(join(directory, `${file}.gz`)), plain = Bun.file(join(directory, file))
      const bytes = await gz.exists() ? Bun.gunzipSync(new Uint8Array(await gz.arrayBuffer())) : await plain.exists() ? new Uint8Array(await plain.arrayBuffer()) : null
      if (!bytes) throw new Error('no such mesh file')
      const read = readStreams(bytes)
      cellsOf(read)
      meshes.set(file, read)
      return read
    } catch (error) {
      console.warn(`[deck tiles] ${file}:`, error)
      unreadable.set(file, true)
      return null
    }
  }
  return async (z, x, y, files) => {
    const pieces: Array<{ file: string; bytes: Uint8Array }> = []
    const failed: string[] = []
    for (const file of files) {
      const read = await mesh(file)
      if (!read) { failed.push(file); continue }
      const bytes = cutPiece(read, z, x, y)
      if (bytes) pieces.push({ file, bytes })
    }
    return { bytes: encodeDeckTile(pieces, failed), complete: !failed.length }
  }
}

/**
 * Deck tiles off the request thread: one worker reads and cuts, so a cold
 * downtown tile (hundreds of meshes) does not stall the API. Falls back to
 * cutting here where workers are unavailable.
 */
export function workerDeckTileBuilder(directory: string): DeckTileBuilder {
  let worker: Worker | null = null
  let next = 0
  const jobs = new Map<number, { resolve: (tile: DeckTile) => void; reject: (error: Error) => void }>()
  const here = deckTileBuilder(directory)
  const start = () => {
    const w = new Worker(new URL('./tile-pieces.worker.ts', import.meta.url))
    w.onmessage = ({ data }: MessageEvent<{ id: number; bytes?: Uint8Array; complete?: boolean; error?: string }>) => {
      const job = jobs.get(data.id)
      jobs.delete(data.id)
      if (data.error !== undefined || !data.bytes) job?.reject(new Error(data.error ?? 'Deck tile worker failed'))
      else job?.resolve({ bytes: data.bytes, complete: !!data.complete })
    }
    w.onerror = event => {
      for (const job of jobs.values()) job.reject(new Error(`Deck tile worker: ${event.message}`))
      jobs.clear()
      worker = null
    }
    w.postMessage({ directory })
    return w
  }
  return (z, x, y, files) => {
    try {
      worker ??= start()
    } catch {
      return here(z, x, y, files)
    }
    const id = next++
    return new Promise((resolve, reject) => {
      jobs.set(id, { resolve, reject })
      worker!.postMessage({ id, z, x, y, files })
    })
  }
}

/** Caches deck tiles by the decks they hold, building each once however many ask at the same time. */
export function createDeckTiles(filesIn: DeckFilesIn, build: DeckTileBuilder, { tileBytes = 256 << 20 } = {}) {
  const tiles = new LRUCache<string, Uint8Array>({ maxSize: tileBytes, sizeCalculation: b => b.byteLength + 1 })
  const lists = new LRUCache<string, string[]>({ max: 20_000, ttl: 60_000 })
  const building = new Map<string, Promise<DeckTile>>()
  return async function deckTile(z: number, x: number, y: number): Promise<DeckTile> {
    const at = `${z}/${x}/${y}`
    let files = lists.get(at)
    if (!files) {
      files = [...new Set(await filesIn(z, x, y))].sort()
      lists.set(at, files)
    }
    const key = `${at}/${createHash('sha1').update(files.join()).digest('hex')}`
    const hit = tiles.get(key)
    if (hit) return { bytes: hit, complete: true }
    let pending = building.get(key)
    if (!pending) {
      pending = build(z, x, y, files).then(tile => {
        if (tile.complete) tiles.set(key, tile.bytes)
        return tile
      }).finally(() => building.delete(key))
      building.set(key, pending)
    }
    return pending
  }
}
