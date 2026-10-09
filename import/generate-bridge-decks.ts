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
import { degreeBox, grid, toUnits } from '../src/lib/bridge-decks/grid'
import { missingMessage } from '../src/lib/bridge-decks/queue'
import { DeckConflict, LOCK, missingTables, rebuildCells } from './bridge-deck-cells'

const args = process.argv.slice(2)
const flag = (name: string) => argValue(args, name)
const CELL = Number(flag('cell') ?? 0.25)

const sql = postgres(dbUrl, { onnotice, max: 3 })

async function areas() {
  const bbox = flag('bbox')
  return bbox ? [parseBbox(bbox)] : regionAreas(await resolveRegions())
}

async function main() {
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

  const dem = new Dem()
  const started = Date.now()
  const g = grid(toUnits(CELL))
  const cell = { partsOf: (key: string) => [g.box(key)], keyOf: g.at, record: true }
  let total = 0
  let clashes = 0
  for (const [k, box] of cells.entries()) {
    const key = g.at([box[0], box[1]])
    try {
      const { stored, keys } = await rebuildCells(sql, dem, [key], cell)
      total += stored
      const also = keys.length > 1 ? `, with ${keys.slice(1).join(' ')}, which held some of its ids` : ''
      if (stored || also) console.log(`[${k + 1}/${cells.length}] cell ${degreeBox(box).slice(0, 2).join(',')}: ${stored} decks${also} (${Math.round((Date.now() - started) / 1000)} s)`)
    } catch (err) {
      if (!(err instanceof DeckConflict)) throw err
      clashes++
      console.error(`ERROR: cell ${degreeBox(box).join(',')} not rebuilt: ${err.message}. See Troubleshooting, "Bridge decks clash".`)
    }
  }
  console.log(`Bridge decks: ${total} built in ${cells.length} cells, ${Math.round((Date.now() - started) / 1000)} s.`)
  if (clashes) process.exitCode = 1
  await lock`SELECT pg_advisory_unlock(${LOCK[0]}, ${LOCK[1]})`
  lock.release()
  await sql.end()
}

if (import.meta.main) {
  await main()
  // resolveRegions may have opened the shared DB handle (region store); exit
  // rather than hang on an idle connection.
  process.exit(process.exitCode ?? 0)
}
