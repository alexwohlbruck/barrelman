/**
 * Heights moved onto EGM96, the datum OSM's `ele` uses.
 *
 * Mapterhorn passes each source's own vertical datum through: USGS 3DEP (all
 * of the contiguous US) is NAVD88, Copernicus GLO-30 (most elsewhere) is
 * EGM2008. The two differ from EGM96 by up to a couple of metres in the US
 * and more in the mountains. The grids here are those differences in
 * centimetres, made once with PROJ by `geoid/make-grids.py`.
 */
import { readFileSync } from 'fs'
import { join } from 'path'

type Grid = { data: Int16Array; lat0: number; lon0: number; step: number; rows: number; cols: number }

const load = (file: string, lat0: number, lat1: number, lon0: number, lon1: number, step: number): Grid => {
  const bytes = readFileSync(join(import.meta.dir, 'geoid', file))
  const data = new Int16Array(bytes.buffer, bytes.byteOffset, bytes.byteLength / 2)
  return { data, lat0, lon0, step, rows: Math.round((lat1 - lat0) / step) + 1, cols: Math.round((lon1 - lon0) / step) + 1 }
}

let grids: { navd88: Grid; egm2008: Grid } | null = null

/** No value: outside the grid's model. */
const NONE = -32768

/** Bilinear centimetres from a grid, or null outside it or where a corner has no value. */
function sample(g: Grid, lng: number, lat: number): number | null {
  const fy = (lat - g.lat0) / g.step
  const fx = (lng - g.lon0) / g.step
  if (fy < 0 || fx < 0 || fy > g.rows - 1 || fx > g.cols - 1) return null
  const [y0, x0] = [Math.min(Math.floor(fy), g.rows - 2), Math.min(Math.floor(fx), g.cols - 2)]
  const [ty, tx] = [fy - y0, fx - x0]
  const at = (y: number, x: number) => g.data[y * g.cols + x]
  const corners = [at(y0, x0), at(y0, x0 + 1), at(y0 + 1, x0), at(y0 + 1, x0 + 1)]
  if (corners.includes(NONE)) return null
  return (corners[0] * (1 - tx) + corners[1] * tx) * (1 - ty) + (corners[2] * (1 - tx) + corners[3] * tx) * ty
}

/** Metres to add to a Mapterhorn height at a point to make it an EGM96 height. */
export function toEgm96(lng: number, lat: number): number {
  grids ??= {
    navd88: load('navd88-egm96.i16', 24, 50, -125, -66, 0.25),
    egm2008: load('egm2008-egm96.i16', -90, 90, -180, 180, 1),
  }
  return (sample(grids.navd88, lng, lat) ?? sample(grids.egm2008, lng, lat) ?? 0) / 100
}
