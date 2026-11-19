/**
 * Builds config/geoid/: the grid that turns an OSM `ele` (metres above the
 * EGM96 geoid) into the datum of the terrain bridge decks stand on, and the
 * mask of where that grid applies. A maintainer tool, run once when a geoid
 * model changes; nothing at import time runs it.
 *
 *   bun add --no-save geotiff@2.1.3
 *   bun run scripts/build-geoid-gap.ts <us_noaa_g2018u0.tif> <egm96-5.pgm> <ne_10m_admin_0_countries.geojson>
 *
 * Inputs, all public domain (see config/geoid/README.md):
 *   us_noaa_g2018u0.tif               NGS GEOID18 for CONUS, from https://cdn.proj.org
 *   egm96-5.pgm                       EGM96 on a 5′ grid, GeographicLib's geoid data
 *   ne_10m_admin_0_countries.geojson  Natural Earth 1:10m countries, v5.1.2
 */
import { readFileSync, writeFileSync } from 'node:fs'
import { gzipSync } from 'node:zlib'
// @ts-ignore: a maintainer-only dependency, installed with --no-save.
import { fromFile } from 'geotiff'
import { GAP_BOX, GAP_STEP, NONE } from '../src/lib/bridge-decks/datum'

const [geoidPath, egmPath, countriesPath] = process.argv.slice(2)
if (!countriesPath) throw new Error('usage: build-geoid-gap.ts <us_noaa_g2018u0.tif> <egm96-5.pgm> <ne_10m_admin_0_countries.geojson>')
const out = new URL('../config/geoid/', import.meta.url)

type Lookup = (lat: number, lon: number) => number
const bilinear = (g: (x: number, y: number) => number, fx: number, fy: number) => {
  const [x0, y0] = [Math.floor(fx), Math.floor(fy)]
  const [u, v] = [fx - x0, fy - y0]
  return (g(x0, y0) * (1 - u) + g(x0 + 1, y0) * u) * (1 - v) + (g(x0, y0 + 1) * (1 - u) + g(x0 + 1, y0 + 1) * u) * v
}

/** GEOID18: a 1′ grid, pixel-is-point, its first sample at 230°E, 58°N. NaN off the grid or where it has no value. */
async function geoid18(path: string): Promise<Lookup> {
  const image = await (await fromFile(path)).getImage()
  const [w, h] = [image.getWidth(), image.getHeight()]
  const [x0, y0] = image.getOrigin()
  const [rx, ry] = image.getResolution()
  const [data] = (await image.readRasters()) as Float32Array[]
  return (lat, lon) => {
    const fx = ((((lon % 360) + 360) % 360) - x0) / rx
    const fy = (lat - y0) / ry
    if (fx < 0 || fy < 0 || fx > w - 1 || fy > h - 1) return NaN
    // The last row and column are samples too: step back inside so the cell after them is never read.
    return bilinear((x, y) => (Math.abs(data[y * w + x]) > 1000 ? NaN : data[y * w + x]), Math.min(fx, w - 1 - 1e-9), Math.min(fy, h - 1 - 1e-9))
  }
}

/** EGM96 from GeographicLib's PGM, read bilinearly as GeoidEval does without -c. */
function egm96(path: string): Lookup {
  const data = readFileSync(path)
  const header: Record<string, string> = {}
  const tokens: string[] = []
  let pos = 0
  while (tokens.length < 4) {
    const end = data.indexOf(10, pos)
    const line = data.subarray(pos, end).toString()
    pos = end + 1
    if (line.startsWith('#')) {
      const [key, value] = line.slice(1).trim().split(/\s+/)
      if (value !== undefined) header[key] = value
    } else tokens.push(...line.trim().split(/\s+/))
  }
  const [w, h] = [Number(tokens[1]), Number(tokens[2])]
  const [offset, scale] = [Number(header.Offset), Number(header.Scale)]
  const g = (x: number, y: number) => data.readUInt16BE(pos + 2 * (y * w + (((x % w) + w) % w))) * scale + offset
  return (lat, lon) => bilinear(g, ((((lon % 360) + 360) % 360) / 360) * w, ((90 - lat) / 180) * (h - 1))
}

/** The gap, GEOID18 N − EGM96 N, in centimetres at each grid node: rows south to north, each west to east, as deltas along the row. */
function gapGrid(geoid: Lookup, egm: Lookup) {
  const [west, south, east, north] = GAP_BOX
  const width = Math.round((east - west) / GAP_STEP) + 1
  const height = Math.round((north - south) / GAP_STEP) + 1
  const cells = new Int16Array(width * height)
  for (let j = 0; j < height; j++) {
    let previous = 0
    for (let i = 0; i < width; i++) {
      const [lat, lon] = [south + j * GAP_STEP, west + i * GAP_STEP]
      const gap = geoid(lat, lon) - egm(lat, lon)
      if (!Number.isFinite(gap)) {
        cells[j * width + i] = NONE
        continue
      }
      const cm = Math.round(gap * 100)
      cells[j * width + i] = cm - previous
      previous = cm
    }
  }
  return { width, height, cells }
}

type P = [number, number]

/** A ring clipped to a box (Sutherland–Hodgman). */
function clip(ring: P[], [w, s, e, n]: readonly number[]): P[] {
  const at = (a: P, b: P, x: number | null, y: number | null): P =>
    x !== null ? [x, a[1] + ((b[1] - a[1]) * (x - a[0])) / (b[0] - a[0])] : [a[0] + ((b[0] - a[0]) * (y! - a[1])) / (b[1] - a[1]), y!]
  const sides: [(p: P) => boolean, (a: P, b: P) => P][] = [
    [p => p[0] >= w, (a, b) => at(a, b, w, null)],
    [p => p[0] <= e, (a, b) => at(a, b, e, null)],
    [p => p[1] >= s, (a, b) => at(a, b, null, s)],
    [p => p[1] <= n, (a, b) => at(a, b, null, n)],
  ]
  let pts = ring
  for (const [ins, cut] of sides) {
    const src = pts
    pts = []
    for (let i = 0; i < src.length; i++) {
      const [a, b] = [src[i], src[(i + 1) % src.length]]
      if (ins(b)) {
        if (!ins(a)) pts.push(cut(a, b))
        pts.push(b)
      } else if (ins(a)) pts.push(cut(a, b))
    }
  }
  return pts
}

/** Douglas–Peucker, in degrees. */
function simplify(pts: P[], tolerance: number): P[] {
  const keep = new Uint8Array(pts.length)
  keep[0] = keep[pts.length - 1] = 1
  const stack: P[] = [[0, pts.length - 1]]
  while (stack.length) {
    const [i, j] = stack.pop()!
    const [a, b] = [pts[i], pts[j]]
    const [dx, dy] = [b[0] - a[0], b[1] - a[1]]
    const length = Math.hypot(dx, dy)
    let [best, far] = [-1, 0]
    for (let k = i + 1; k < j; k++) {
      const p = pts[k]
      const d = length ? Math.abs((p[0] - a[0]) * dy - (p[1] - a[1]) * dx) / length : Math.hypot(p[0] - a[0], p[1] - a[1])
      if (d > far) [best, far] = [k, d]
    }
    if (far > tolerance) {
      keep[best] = 1
      stack.push([i, best], [best, j])
    }
  }
  return pts.filter((_, k) => keep[k])
}

const area = (r: P[]) => Math.abs(r.reduce((sum, p, i) => sum + p[0] * r[(i + 1) % r.length][1] - r[(i + 1) % r.length][0] * p[1], 0) / 2)

/** Other countries' land inside the grid's box, by ISO code: about 200 m simplified, islets under ~1 km² left out. */
function notUs(path: string): Record<string, P[][]> {
  const countries = JSON.parse(readFileSync(path, 'utf8'))
  const out: Record<string, P[][]> = {}
  for (const f of countries.features) {
    const code: string = f.properties.ADM0_A3
    if (code === 'USA') continue
    const polygons: P[][][] = f.geometry.type === 'Polygon' ? [f.geometry.coordinates] : f.geometry.type === 'MultiPolygon' ? f.geometry.coordinates : []
    for (const polygon of polygons)
      for (const ring of polygon) {
        const clipped = clip(ring.slice(0, -1), GAP_BOX)
        if (clipped.length < 3) continue
        const ringOut = simplify([...clipped, clipped[0]], 0.002).slice(0, -1)
        if (ringOut.length < 3 || area(ringOut) < 1e-4) continue
        ;(out[code] ??= []).push(ringOut.map(([x, y]) => [Math.round(x * 1e5) / 1e5, Math.round(y * 1e5) / 1e5]))
      }
  }
  return out
}

const grid = gapGrid(await geoid18(geoidPath), egm96(egmPath))
writeFileSync(new URL('conus-gap.bin.gz', out), gzipSync(Buffer.from(grid.cells.buffer), { level: 9 }))
writeFileSync(new URL('conus-not-us.json', out), JSON.stringify(notUs(countriesPath)) + '\n')
console.log(`conus-gap.bin.gz: ${grid.width} × ${grid.height} nodes; conus-not-us.json written`)
