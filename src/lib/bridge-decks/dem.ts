/**
 * Ground heights from Mapterhorn's terrarium tiles, the terrain Parchment
 * draws, read at one fixed zoom and kept in a small cache so a run over a
 * region fetches each tile once.
 */
import decode from '@jsquash/webp/decode.js'
import { envString } from '../../config/env'
import type { Point } from './profile'

export const DEM_TILES = envString('BRIDGE_DECKS_DEM_TILES', 'https://tiles.mapterhorn.com/{z}/{x}/{y}.webp')
/** About 2 m a pixel: lidar detail where the source has it. */
export const DEM_ZOOM = 15

export type Heights = { size: number; data: Float32Array }

/** Metres from terrarium pixels. */
export function decodeTerrarium(rgba: ArrayLike<number>, size: number): Heights {
  const data = new Float32Array(size * size)
  for (let i = 0; i < data.length; i++) data[i] = rgba[i * 4] * 256 + rgba[i * 4 + 1] + rgba[i * 4 + 2] / 256 - 32768
  return { size, data }
}

/**
 * Bilinear height at a position within a tile, `u` and `v` from 0 to 1, with
 * each pixel's value at its top-left corner as MapLibre's terrain reads it.
 */
export function sampleHeights({ size, data }: Heights, u: number, v: number): number {
  const fx = Math.min(size - 1, Math.max(0, u * size))
  const fy = Math.min(size - 1, Math.max(0, v * size))
  const [x0, y0] = [Math.floor(fx), Math.floor(fy)]
  const [x1, y1] = [Math.min(size - 1, x0 + 1), Math.min(size - 1, y0 + 1)]
  const [tx, ty] = [fx - x0, fy - y0]
  const top = data[y0 * size + x0] * (1 - tx) + data[y0 * size + x1] * tx
  const bottom = data[y1 * size + x0] * (1 - tx) + data[y1 * size + x1] * tx
  return top * (1 - ty) + bottom * ty
}

export type TileLoader = (z: number, x: number, y: number) => Promise<Heights | null>

export const fetchTile: TileLoader = async (z, x, y) => {
  const url = DEM_TILES.replace('{z}', String(z)).replace('{x}', String(x)).replace('{y}', String(y))
  for (let attempt = 0; ; attempt++) {
    const res = await fetch(url).catch(() => null)
    if (res?.status === 404 || res?.status === 204) return null
    if (res?.ok) {
      const image = await decode(await res.arrayBuffer())
      return decodeTerrarium(image.data, image.width)
    }
    if (attempt >= 4) throw new Error(`terrain tile ${z}/${x}/${y}: ${res?.status ?? 'network error'}`)
    await Bun.sleep(1000 * 2 ** attempt)
  }
}

/**
 * Ground at mercator points, from tiles at `zoom` or, where the source has
 * none, the nearest parent down to `minZoom`. `load` fetches every tile the
 * points need, a few at a time; `at` then reads them synchronously.
 *
 * A decoded tile is about 1 MB, so the cache holds `capacity` of them and
 * drops the least recently loaded as it inserts. Tiles the current `load`
 * needs are never dropped by it, so one call wanting more than `capacity`
 * overshoots rather than evicting its own tiles and fetching them forever;
 * callers keep each call to a handful of decks.
 */
export class Dem {
  private tiles = new Map<string, Heights | null>()

  constructor(private loader: TileLoader = fetchTile, private options = { zoom: DEM_ZOOM, minZoom: DEM_ZOOM - 5, capacity: 128, concurrency: 8 }) {}

  async load(points: Point[]) {
    const pinned = new Set<string>()
    for (;;) {
      const wanted = [...new Set(points.map(p => this.missing(p, pinned)).filter((k): k is string => !!k))]
      if (!wanted.length) return
      for (let i = 0; i < wanted.length; i += this.options.concurrency)
        await Promise.all(wanted.slice(i, i + this.options.concurrency).map(async k => {
          const [z, x, y] = k.split('/').map(Number)
          const tile = await this.loader(z, x, y)
          pinned.add(k)
          this.tiles.set(k, tile)
          this.evict(pinned)
        }))
    }
  }

  /**
   * The next tile a point needs fetched, or null once it has one (or none
   * exists). Each tile it passes is pinned and marked as recently used.
   */
  private missing([x, y]: Point, pinned: Set<string>): string | null {
    for (let z = this.options.zoom; z >= this.options.minZoom; z--) {
      const n = 2 ** z
      const k = `${z}/${Math.floor(x * n)}/${Math.floor(y * n)}`
      if (!this.tiles.has(k)) return k
      const tile = this.tiles.get(k)!
      if (!pinned.has(k)) {
        pinned.add(k)
        // Map keeps insertion order, so re-inserting moves it to the back of the eviction queue.
        this.tiles.delete(k)
        this.tiles.set(k, tile)
      }
      if (tile) return null
    }
    return null
  }

  /** Height in the source's own datum, or NaN where no tile has data. */
  at([x, y]: Point): number {
    for (let z = this.options.zoom; z >= this.options.minZoom; z--) {
      const n = 2 ** z
      const [tx, ty] = [Math.floor(x * n), Math.floor(y * n)]
      const tile = this.tiles.get(`${z}/${tx}/${ty}`)
      if (tile) return sampleHeights(tile, x * n - tx, y * n - ty)
    }
    return NaN
  }

  /** Tiles held, empty ones (where the source has none) included. */
  get size() {
    return this.tiles.size
  }

  private evict(pinned: Set<string>) {
    for (const k of this.tiles.keys()) {
      if (this.tiles.size <= this.options.capacity) break
      if (!pinned.has(k)) this.tiles.delete(k)
    }
  }
}
