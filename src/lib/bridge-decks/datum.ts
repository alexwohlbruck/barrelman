/**
 * OSM's `ele` in the datum of the terrain bridge decks stand on.
 *
 * OSM defines `ele` as metres above the EGM96 geoid. Mapterhorn's terrain
 * keeps each source's own vertical datum: Mapterhorn does no vertical
 * transformation (mapterhorn/mapterhorn#255). So the two agree only where the
 * source is on EGM96, and differ by the gap between the geoids elsewhere:
 *
 *   conterminous US  USGS 3DEP (1 m lidar DEMs and the 1/3″ seamless DEM),
 *                    NAVD88, which GEOID18 models. EGM96 heights there are
 *                    0.4–1 m above NAVD88 in the east and midwest. Converted.
 *   Canada           NRCan HRDEM, CGVD2013. Not converted yet: the gap,
 *                    CGG2013a − EGM96, is about 1.0–1.25 m in its cities.
 *   elsewhere        national DEMs on their national datums, and Copernicus
 *                    GLO-30 (EGM2008) where there is none. Not converted.
 *
 * Where the gap is not known, `ele` is used as it stands, as it was before
 * this conversion: never worse than that, and within EGM2008 − EGM96 (mostly
 * under half a metre) wherever GLO-30 is the terrain.
 *
 * The conterminous US is a box, with the land of Canada, Mexico and the
 * Bahamas inside it taken out, and the gap there, GEOID18 N − EGM96 N, is
 * read bilinearly from a 0.05° grid. Over water inside the box the terrain is
 * 3DEP's too. The data and its sources are in config/geoid/; the tags it
 * undoes are written as lidar NAVD88 + (GEOID18 N − EGM96 N), with both
 * geoid heights as published (landmarks docs/bridge-heights.md), so this
 * gives back the NAVD88 they came from.
 */
import { readFileSync } from 'node:fs'
import { gunzipSync } from 'node:zlib'

/** The grid's extent in degrees, west, south, east, north: its first and last nodes. */
export const GAP_BOX = [-125, 24, -66, 50] as const
/** Degrees between grid nodes: 0.05°, within 0.01 m rms (0.1 m at worst, in mountains) of the full 1′ GEOID18. */
export const GAP_STEP = 0.05
/** A node with no gap: GEOID18 has no value there. */
export const NONE = -32768

const [WEST, SOUTH, EAST, NORTH] = GAP_BOX
const WIDTH = Math.round((EAST - WEST) / GAP_STEP) + 1
const HEIGHT = Math.round((NORTH - SOUTH) / GAP_STEP) + 1

type Ring = { ring: [number, number][]; box: [number, number, number, number] }

let gaps: Float32Array | null = null
let notUs: Ring[] | null = null

const file = (name: string) => readFileSync(new URL(`../../../config/geoid/${name}`, import.meta.url))

/** The gap at each node in metres, NaN where there is none. Read once, on first use. */
function grid(): Float32Array {
  if (gaps) return gaps
  const raw = gunzipSync(file('conus-gap.bin.gz'))
  const cells = new Int16Array(raw.buffer, raw.byteOffset, raw.byteLength / 2)
  if (cells.length !== WIDTH * HEIGHT) throw new Error(`config/geoid/conus-gap.bin.gz holds ${cells.length} nodes, not ${WIDTH * HEIGHT}`)
  gaps = new Float32Array(cells.length)
  for (let j = 0; j < HEIGHT; j++) {
    let cm = 0
    for (let i = 0; i < WIDTH; i++) {
      const k = j * WIDTH + i
      if (cells[k] === NONE) {
        gaps[k] = NaN
        continue
      }
      cm += cells[k]
      gaps[k] = cm / 100
    }
  }
  return gaps
}

function rings(): Ring[] {
  if (notUs) return notUs
  const countries: Record<string, [number, number][][]> = JSON.parse(file('conus-not-us.json').toString())
  notUs = Object.values(countries).flat().map(ring => {
    const xs = ring.map(p => p[0]), ys = ring.map(p => p[1])
    return { ring, box: [Math.min(...xs), Math.min(...ys), Math.max(...xs), Math.max(...ys)] }
  })
  return notUs
}

/** Even–odd: a ring inside another (a lake with an island) is in neither. */
function inForeignLand(lng: number, lat: number): boolean {
  let inside = false
  for (const { ring, box } of rings()) {
    if (lng < box[0] || lng > box[2] || lat < box[1] || lat > box[3]) continue
    for (let i = 0, j = ring.length - 1; i < ring.length; j = i++) {
      const [xi, yi] = ring[i], [xj, yj] = ring[j]
      if (yi > lat !== yj > lat && lng < ((xj - xi) * (lat - yi)) / (yj - yi) + xi) inside = !inside
    }
  }
  return inside
}

/**
 * Metres an EGM96 height stands above the same point's height in the
 * terrain's datum there: GEOID18 N − EGM96 N in the conterminous US, null
 * where the terrain's datum is not one this converts from.
 */
export function geoidGap(lng: number, lat: number): number | null {
  if (!(lng >= WEST && lng <= EAST && lat >= SOUTH && lat <= NORTH)) return null
  const fx = Math.min((lng - WEST) / GAP_STEP, WIDTH - 1 - 1e-9)
  const fy = Math.min((lat - SOUTH) / GAP_STEP, HEIGHT - 1 - 1e-9)
  const [x0, y0] = [Math.floor(fx), Math.floor(fy)]
  const [u, v] = [fx - x0, fy - y0]
  const g = grid()
  const at = (x: number, y: number) => g[y * WIDTH + x]
  const gap = (at(x0, y0) * (1 - u) + at(x0 + 1, y0) * u) * (1 - v) + (at(x0, y0 + 1) * (1 - u) + at(x0 + 1, y0 + 1) * u) * v
  if (!Number.isFinite(gap) || inForeignLand(lng, lat)) return null
  return gap
}

/** An OSM `ele`, metres above EGM96, as a height in the terrain's datum at that point: unchanged where the gap is not known. */
export function terrainHeight(ele: number, lng: number, lat: number): number {
  return ele - (geoidGap(lng, lat) ?? 0)
}
