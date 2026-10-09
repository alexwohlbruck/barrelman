/**
 * Build bridge_decks: every bridge way joined into decks, fitted to its
 * outline, and given a height profile from Mapterhorn terrain.
 *
 *   bun run import/generate-bridge-decks.ts                        the REGIONS areas
 *   bun run import/generate-bridge-decks.ts --bbox w,s,e,n         one area
 *
 * Without --bbox it covers each area of the regions REGIONS selects (their
 * own `bboxes`, not one box around them). REGIONS=global is refused: a planet
 * run is days of work and should be started deliberately, area by area.
 *
 * The area is worked through in cells (`--cell`, degrees), each rebuilt in
 * place (see bridge-deck-cells.ts), so a run can be stopped and resumed. Each
 * cell built is recorded in bridge_deck_cells, which is where Update Bridge
 * Decks builds bridges that OSM updates add. Terrain tiles are fetched as
 * needed and kept in a bounded cache.
 */
import postgres from 'postgres'
import { resolveRegions } from '../src/config/regions'
import { dbUrl, onnotice } from '../src/db'
import { argValue } from '../src/lib/cli-args'
import { cellsCovering, parseBbox, regionAreas } from '../src/lib/bridge-decks/areas'
import { Dem, DEM_TILES, demConfigured } from '../src/lib/bridge-decks/dem'
import { degreeBox, grid, toUnits, type UnitBox } from '../src/lib/bridge-decks/grid'
import { missingMessage } from '../src/lib/bridge-decks/queue'
import { coveredParts, DeckConflict, LOCK, missingTables, rebuildCells } from './bridge-deck-cells'

const args = process.argv.slice(2)
const flag = (name: string) => argValue(args, name)
const CELL = Number(flag('cell') ?? 0.25)

type Sql = postgres.Sql

async function areas() {
  const bbox = flag('bbox')
  return bbox ? [parseBbox(bbox)] : regionAreas(await resolveRegions())
}

/**
 * Rebuilds and records the `size`-degree `cells` of a run. A cell is only
 * ever recorded when it is one of the run's own. Outside them a rebuild
 * writes only where cells are already recorded: when a clashing id or a deck
 * that moved takes it into a neighbouring cell, it takes in just the part of
 * that cell an earlier build covered.
 */
export async function buildArea(sql: Sql, dem: Dem, cells: UnitBox[], size: number) {
  const g = grid(toUnits(size))
  const run = new Map(cells.map(b => [g.at([b[0], b[1]]), b]))
  const cell = {
    partsOf: (k: string) => (run.has(k) ? [g.box(k)] : coveredParts(sql, g.box(k))),
    keyOf: g.at,
    record: (k: string) => run.has(k),
  }
  const started = Date.now()
  const pending = [...run.keys()]
  const queued = new Set(pending)
  const done = new Set<string>()
  let total = 0
  let clashes = 0
  while (pending.length) {
    const key = pending.shift()!
    if (done.has(key)) continue
    try {
      const { stored, keys, moved } = await rebuildCells(sql, dem, [key], cell)
      for (const k of keys) done.add(k)
      total += stored
      // A deck this cell held whose midpoint now lies elsewhere goes there.
      for (const k of moved.map(g.at))
        if (!done.has(k) && !queued.has(k)) {
          queued.add(k)
          pending.push(k)
        }
      const also = keys.length > 1 ? `, with ${keys.slice(1).join(' ')}, which held some of its ids` : ''
      if (stored || also) console.log(`[${done.size}/${queued.size}] cell ${degreeBox(g.box(key)).slice(0, 2).join(',')}: ${stored} decks${also} (${Math.round((Date.now() - started) / 1000)} s)`)
    } catch (err) {
      if (!(err instanceof DeckConflict)) throw err
      clashes++
      console.error(`ERROR: cell ${degreeBox(g.box(key)).join(',')} not rebuilt: ${err.message}. See Troubleshooting, "Bridge decks clash".`)
    }
  }
  return { total, clashes, built: done.size }
}

async function main(sql: Sql) {
  const missing = await missingTables(sql)
  if (missing.length) throw new Error(missingMessage(missing))
  if (!demConfigured(DEM_TILES)) throw new Error(`BRIDGE_DECKS_DEM_TILES is "${DEM_TILES}": bridge decks need a terrain source`)
  const cells = cellsCovering(await areas(), CELL)

  const lock = await sql.reserve()
  const [{ free }] = await lock`SELECT pg_try_advisory_lock(${LOCK[0]}, ${LOCK[1]}) AS free`
  if (!free) {
    console.log('Waiting for an Update Bridge Decks run to finish...')
    await lock`SELECT pg_advisory_lock(${LOCK[0]}, ${LOCK[1]})`
  }

  const { total, clashes, built } = await buildArea(sql, new Dem(), cells, CELL)
  console.log(`Bridge decks: ${total} built in ${built} cells.`)
  if (clashes) process.exitCode = 1
  await lock`SELECT pg_advisory_unlock(${LOCK[0]}, ${LOCK[1]})`
  lock.release()
  await sql.end()
}

if (import.meta.main) {
  await main(postgres(dbUrl, { onnotice, max: 3 }))
  // resolveRegions may have opened the shared DB handle (region store); exit
  // rather than hang on an idle connection.
  process.exit(process.exitCode ?? 0)
}
