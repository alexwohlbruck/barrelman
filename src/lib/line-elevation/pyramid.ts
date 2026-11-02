/**
 * Writes the heights of ribbons, stations and stop marks into a portolan
 * pyramid's tiles, so the tile server serves them as files.
 */
import { readdirSync, readFileSync, renameSync, writeFileSync } from 'fs'
import { basename, dirname, join } from 'path'
import type { LineSources } from './lookup'
import { ELEVATION_MIN_ZOOM, elevateRibbonTile, hasLines } from './tile'

/** Sources are read once per tile at this zoom and shared by the tiles under it. */
export const SOURCE_ZOOM = ELEVATION_MIN_ZOOM

type Tile = { z: number; x: number; y: number; path: string }

/** The pyramid's tiles from the lowest zoom lines are given heights at, grouped by the tile their sources are read for. */
export function tilesToElevate(dir: string): Map<string, Tile[]> {
  const groups = new Map<string, Tile[]>()
  for (const zName of readdirSync(dir)) {
    const z = Number(zName)
    if (!/^\d+$/.test(zName) || z < ELEVATION_MIN_ZOOM) continue
    for (const xName of readdirSync(join(dir, zName))) {
      if (!/^\d+$/.test(xName)) continue
      for (const file of readdirSync(join(dir, zName, xName))) {
        const m = /^(\d+)\.mvt$/.exec(file)
        if (!m) continue
        const [x, y] = [Number(xName), Number(m[1])]
        const shift = z - SOURCE_ZOOM
        const key = `${x >> shift}/${y >> shift}`
        const tiles = groups.get(key) ?? []
        tiles.push({ z, x, y, path: join(dir, zName, xName, file) })
        groups.set(key, tiles)
      }
    }
  }
  return groups
}

/** Replaces a file whole, never writing through it: a pyramid being built hard-links its unchanged tiles from the live one. */
function replace(path: string, bytes: Uint8Array) {
  const tmp = join(dirname(path), `.${basename(path)}.${process.pid}`)
  writeFileSync(tmp, bytes)
  renameSync(tmp, path)
}

const same = (a: Uint8Array, b: Uint8Array) => a.length === b.length && a.every((v, i) => v === b[i])

export type PyramidTally = { tiles: number; written: number; failed: number }

/** Gives every tile of the pyramid in `dir` its heights. A tile whose sources cannot be read is left as it is. */
export async function elevatePyramid(dir: string, sources: LineSources, log: (line: string) => void = () => {}): Promise<PyramidTally> {
  const tally: PyramidTally = { tiles: 0, written: 0, failed: 0 }
  for (const [key, tiles] of tilesToElevate(dir)) {
    const [px, py] = key.split('/').map(Number)
    let read: Awaited<ReturnType<LineSources>> | null = null
    for (const tile of tiles) {
      tally.tiles++
      const bytes = new Uint8Array(readFileSync(tile.path))
      if (!hasLines(bytes, 'ribbons')) continue
      try {
        read ??= await sources(SOURCE_ZOOM, px, py)
        const out = elevateRibbonTile(bytes, tile.z, tile.x, tile.y, read)
        if (out !== bytes && !same(out, bytes)) {
          replace(tile.path, out)
          tally.written++
        }
      } catch (error) {
        tally.failed++
        log(`${tile.z}/${tile.x}/${tile.y}: ${error}`)
      }
    }
  }
  return tally
}
