/**
 * Planning a run of Update Bridge Decks over its queue: which grid cells to
 * rebuild so that every deck an OSM change touched, as it was and as it is
 * now, ends up rebuilt in the one cell that owns it.
 */
import type { Bbox } from '../../config/regions'

/** Side of a rebuilt cell, in degrees: about 2 km. */
export const UPDATE_CELL = 0.02

type LngLat = [number, number]

/** A deck as built: where it is anchored and the box it spans, in degrees. */
export type Built = { id: string; anchor: LngLat; box: Bbox }

/** A queued box, and the anchors of the stored decks that cross it. */
export type Entry = { id: string; box: Bbox; anchors: LngLat[] }

export const cellAt = ([lng, lat]: LngLat, size = UPDATE_CELL) => `${Math.floor(lng / size)},${Math.floor(lat / size)}`

export function cellBox(cell: string, size = UPDATE_CELL): Bbox {
  const [cx, cy] = cell.split(',').map(Number)
  return [cx * size, cy * size, (cx + 1) * size, (cy + 1) * size]
}

/** Cells per side of a block rebuilt in one go: about 6 km, sharing one read of the bridges around it. */
export const BLOCK = 3

export function blockOf(cell: string): string {
  const [cx, cy] = cell.split(',').map(Number)
  return `${Math.floor(cx / BLOCK)},${Math.floor(cy / BLOCK)}`
}

export function cellsOver([w, s, e, n]: Bbox, size = UPDATE_CELL): string[] {
  const out: string[] = []
  for (let cx = Math.floor(w / size); cx <= Math.floor(e / size); cx++)
    for (let cy = Math.floor(s / size); cy <= Math.floor(n / size); cy++) out.push(`${cx},${cy}`)
  return out
}

export const meets = (a: Bbox, b: Bbox) => a[0] <= b[2] && b[0] <= a[2] && a[1] <= b[3] && b[1] <= a[3]

/**
 * The cells an entry needs rebuilt: those under its box, where a new deck may
 * now be anchored, and those its stored decks are anchored in, which may lie
 * well away along a long bridge and would otherwise keep the old deck.
 */
export function entryCells(entry: Entry, size = UPDATE_CELL): Set<string> {
  return new Set([...cellsOver(entry.box, size), ...entry.anchors.map(a => cellAt(a, size))])
}

/**
 * Entries, in queue order, while the cells they need fit in `maxCells`. An
 * entry is taken whole or not at all, and the first is always taken, however
 * many cells it needs, so the queue moves.
 */
export function pick(entries: Entry[], maxCells: number, size = UPDATE_CELL): { picked: Entry[]; cells: Map<string, string[]> } {
  const cells = new Map<string, string[]>()
  const picked: Entry[] = []
  for (const entry of entries) {
    const own = entryCells(entry, size)
    const added = [...own].filter(c => !cells.has(c)).length
    if (picked.length && cells.size + added > maxCells) continue
    picked.push(entry)
    for (const c of own) cells.set(c, [...(cells.get(c) ?? []), entry.id])
  }
  return { picked, cells }
}

/**
 * Cells not yet planned that a build showed hold a deck crossing a picked
 * entry, with the entries each serves. A deck that grew or shrank at one end
 * moves its midpoint, possibly far along it into a cell the change never
 * reached, and that cell has to be rebuilt to take it.
 */
export function strays(built: Built[], picked: Entry[], planned: ReadonlyMap<string, unknown>, size = UPDATE_CELL): Map<string, string[]> {
  const out = new Map<string, string[]>()
  for (const deck of built) {
    const cell = cellAt(deck.anchor, size)
    if (planned.has(cell)) continue
    const by = picked.filter(e => meets(e.box, deck.box)).map(e => e.id)
    if (by.length) out.set(cell, [...new Set([...(out.get(cell) ?? []), ...by])])
  }
  return out
}

/** The entries planned over any of `failed`, to be tried again later. */
export function entriesOver(failed: Iterable<string>, cells: ReadonlyMap<string, string[]>): Set<string> {
  const out = new Set<string>()
  for (const cell of failed) for (const id of cells.get(cell) ?? []) out.add(id)
  return out
}

/** Cells per run from BRIDGE_DECKS_MAX_CELLS: a whole number of at least one. */
export function maxCellsFrom(raw: string | undefined, fallback: number): number {
  if (raw === undefined || raw.trim() === '') return fallback
  const n = Number(raw)
  if (!Number.isInteger(n) || n < 1) throw new Error(`BRIDGE_DECKS_MAX_CELLS must be a whole number of at least 1, got "${raw}"`)
  return n
}

export function missingMessage(missing: string[]): string {
  return `${missing.join(' and ')} ${missing.length > 1 ? 'do' : 'does'} not exist; start the API once, or run import/create-detail-views.sql.`
}

/** Why a run has nothing to do, or null when it can go ahead. */
export function skipReason(state: { missing: string[]; queue: boolean; terrain: boolean }): string | null {
  if (state.missing.length) return missingMessage(state.missing)
  if (!state.queue) return 'nothing queued.'
  if (!state.terrain) return 'BRIDGE_DECKS_DEM_TILES is off (or not a {z}/{x}/{y} template), so there is no terrain to build decks on.'
  return null
}
