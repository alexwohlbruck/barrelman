/**
 * Planning a run of Update Bridge Decks over its queue: which grid cells to
 * rebuild so that every deck an OSM change touched, as it was and as it is
 * now, ends up rebuilt in the one cell that owns it.
 */
import type { Bbox } from '../../config/regions'
import { grid, unitBox, type UnitPoint } from './grid'

/**
 * Side of a rebuilt cell: 0.025°, about 2.5 km. It divides Build Bridge
 * Decks' default 0.25° cell, so an update cell lies inside one build cell.
 */
export const UPDATE_CELL = 250_000
export const cells = grid(UPDATE_CELL)

/** A deck as built: its anchor in units and the box it spans in degrees. */
export type Built = { id: string; anchor: UnitPoint; box: Bbox }

/** A queued box, in degrees, and the anchors of the stored decks that cross it. */
export type Entry = { id: string; box: Bbox; anchors: UnitPoint[] }

/** Cells per side of a block rebuilt in one go: about 7.5 km, sharing one read of the bridges around it. */
export const BLOCK = 3

export function blockOf(cell: string): string {
  const [cx, cy] = cell.split(',').map(Number)
  return `${Math.floor(cx / BLOCK)},${Math.floor(cy / BLOCK)}`
}

export const meets = (a: Bbox, b: Bbox) => a[0] <= b[2] && b[0] <= a[2] && a[1] <= b[3] && b[1] <= a[3]

/**
 * The cells an entry needs rebuilt: those under its box, where a new deck may
 * now be anchored, and those its stored decks are anchored in, which may lie
 * well away along a long bridge and would otherwise keep the old deck.
 */
export function entryCells(entry: Entry): Set<string> {
  return new Set([...cells.over(unitBox(entry.box)), ...entry.anchors.map(cells.at)])
}

/**
 * Entries, in queue order, while the cells they need fit in `maxCells`. An
 * entry is taken whole or not at all, and the first is always taken, however
 * many cells it needs, so the queue moves.
 */
export function pick(entries: Entry[], maxCells: number): { picked: Entry[]; cells: Map<string, string[]> } {
  const planned = new Map<string, string[]>()
  const picked: Entry[] = []
  for (const entry of entries) {
    const own = entryCells(entry)
    let added = 0
    for (const c of own) if (!planned.has(c)) added++
    if (picked.length && planned.size + added > maxCells) continue
    picked.push(entry)
    for (const c of own) {
      const by = planned.get(c)
      if (by) by.push(entry.id)
      else planned.set(c, [entry.id])
    }
  }
  return { picked, cells: planned }
}

/** Picked entries by the cells under their boxes, for finding the ones a deck crosses. */
export function entryIndex(picked: Entry[]): Map<string, Entry[]> {
  const index = new Map<string, Entry[]>()
  for (const entry of picked)
    for (const c of cells.over(unitBox(entry.box))) {
      const at = index.get(c)
      if (at) at.push(entry)
      else index.set(c, [entry])
    }
  return index
}

/**
 * Cells not yet planned that a build showed hold a deck crossing a picked
 * entry, with the entries each serves. A deck that grew or shrank at one end
 * moves its midpoint, possibly far along it into a cell the change never
 * reached, and that cell has to be rebuilt to take it.
 */
export function strays(built: Built[], index: ReadonlyMap<string, Entry[]>, planned: ReadonlyMap<string, unknown>): Map<string, string[]> {
  const out = new Map<string, Set<string>>()
  for (const deck of built) {
    const cell = cells.at(deck.anchor)
    if (planned.has(cell)) continue
    for (const c of cells.over(unitBox(deck.box)))
      for (const entry of index.get(c) ?? [])
        if (meets(entry.box, deck.box)) {
          const by = out.get(cell) ?? new Set<string>()
          by.add(entry.id)
          out.set(cell, by)
        }
  }
  return new Map([...out].map(([cell, by]) => [cell, [...by]]))
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
