/**
 * Cell arithmetic for bridge decks, in whole units of 1e-7°.
 *
 * A deck belongs to the cell its anchor lies in. Worked out in floating-point
 * degrees, `Math.floor(lng / size)` and the box `[cx * size, (cx + 1) * size)`
 * disagree for thousands of values near cell edges, and SQL rounds differently
 * again, so a deck could be written by one cell and deleted by none. Anchors
 * are stored as whole units (bridge_decks.anchor_x, anchor_y) and every box is
 * whole units, so TS and SQL compare integers and always agree.
 */
import type { Bbox } from '../../config/regions'

export const UNITS_PER_DEGREE = 1e7

/** A box in whole units, west and south inclusive, east and north exclusive. */
export type UnitBox = [w: number, s: number, e: number, n: number]
export type UnitPoint = [x: number, y: number]

export const toUnits = (degrees: number) => Math.round(degrees * UNITS_PER_DEGREE)
export const toDegrees = (units: number) => units / UNITS_PER_DEGREE

export const unitBox = (b: Bbox): UnitBox => [toUnits(b[0]), toUnits(b[1]), toUnits(b[2]), toUnits(b[3])]
export const degreeBox = (b: UnitBox): Bbox => [toDegrees(b[0]), toDegrees(b[1]), toDegrees(b[2]), toDegrees(b[3])]

export const contains = ([w, s, e, n]: UnitBox, [x, y]: UnitPoint) => x >= w && x < e && y >= s && y < n

/** Where two boxes overlap, or null when they only touch or are apart. */
export function clip(a: UnitBox, b: UnitBox): UnitBox | null {
  const out: UnitBox = [Math.max(a[0], b[0]), Math.max(a[1], b[1]), Math.min(a[2], b[2]), Math.min(a[3], b[3])]
  return out[0] < out[2] && out[1] < out[3] ? out : null
}

/** A fixed grid of `size`-unit cells, keyed "cx,cy". */
export function grid(size: number) {
  if (!Number.isInteger(size) || size < 1) throw new Error(`a cell must be a whole number of 1e-7° units, got ${size}`)
  const index = (u: number) => Math.floor(u / size)
  return {
    at: ([x, y]: UnitPoint) => `${index(x)},${index(y)}`,
    box(cell: string): UnitBox {
      const [cx, cy] = cell.split(',').map(Number)
      return [cx * size, cy * size, (cx + 1) * size, (cy + 1) * size]
    },
    /** Every cell a box touches, its far edges included. */
    over([w, s, e, n]: UnitBox): string[] {
      const out: string[] = []
      for (let cx = index(w); cx <= index(e); cx++) for (let cy = index(s); cy <= index(n); cy++) out.push(`${cx},${cy}`)
      return out
    },
  }
}
