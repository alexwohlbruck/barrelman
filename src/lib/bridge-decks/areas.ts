/**
 * Which areas a bridge deck run covers, and the cells it works through.
 */
import type { Bbox, ResolvedRegions } from '../../config/regions'

/** A `w,s,e,n` box, refused unless it is four numbers the right way round. */
export function parseBbox(value: string): Bbox {
  const parts = value.split(',').map((v) => Number(v.trim()))
  const [w, s, e, n] = parts
  if (parts.length !== 4 || parts.some((v) => !Number.isFinite(v)) || w >= e || s >= n)
    throw new Error(`--bbox must be west,south,east,north in degrees, got "${value}"`)
  return [w, s, e, n]
}

/**
 * The areas of the selected regions: each region's own boxes, not their union,
 * so a run over a region with outlying parts does not take in the gap.
 *
 * A global selection is refused. Building the planet's decks is days of work
 * and a terrain fetch for every bridge on Earth, which is not something to
 * start by leaving the area blank; name the area with --bbox instead.
 */
export function regionAreas(regions: Pick<ResolvedRegions, 'isGlobal' | 'boxes'>): Bbox[] {
  if (regions.isGlobal)
    throw new Error(
      'REGIONS=global selects the whole planet, and Build Bridge Decks will not start a planet-wide run ' +
        'unasked: it would fetch terrain for every bridge on Earth over several days. Pass --bbox w,s,e,n ' +
        'for the area to build, once per area.',
    )
  return regions.boxes
}

/** The `size`-degree cells, on a fixed grid, that the areas touch; each once, however many areas share it. */
export function cellsCovering(areas: Bbox[], size: number): Bbox[] {
  if (!(size > 0)) throw new Error(`--cell must be a positive number of degrees, got ${size}`)
  const cells = new Map<string, Bbox>()
  for (const [w, s, e, n] of areas)
    for (let x = Math.floor(w / size); x * size < e; x++)
      for (let y = Math.floor(s / size); y * size < n; y++)
        cells.set(`${x},${y}`, [x * size, y * size, (x + 1) * size, (y + 1) * size])
  return [...cells.values()]
}
