/**
 * Build sidewalk_street_names: the street each unnamed sidewalk runs beside,
 * along its length, so walking directions can say which street they follow.
 *
 *   bun run import/generate-sidewalk-street-names.ts                  the REGIONS areas
 *   bun run import/generate-sidewalk-street-names.ts --bbox w,s,e,n   one area
 *
 * Works through the area cell by cell, each rebuilt in place
 * (see sidewalk-street-name-cells.ts), so a run can be stopped and started
 * again. Update Sidewalk Street Names keeps them in step with OSM afterwards.
 */
import postgres from 'postgres'
import { resolveRegions } from '../src/config/regions'
import { dbUrl, onnotice } from '../src/db'
import { argValue } from '../src/lib/cli-args'
import { cellsCovering, parseBbox, regionAreas } from '../src/lib/bridge-decks/areas'
import { degreeBox } from '../src/lib/bridge-decks/grid'
import { LOCK, rebuildBox, tableMissing } from './sidewalk-street-name-cells'

const args = process.argv.slice(2)
const CELL = Number(argValue(args, 'cell') ?? 0.02)

type Sql = postgres.Sql

async function main(sql: Sql) {
  if (await tableMissing(sql)) throw new Error('sidewalk_street_names does not exist; start the API once, or run import/create-detail-views.sql.')
  const bbox = argValue(args, 'bbox')
  const areas = bbox ? [parseBbox(bbox)] : regionAreas(await resolveRegions())
  const cells = cellsCovering(areas, CELL).map(degreeBox)

  const lock = await sql.reserve()
  const [{ free }] = await lock`SELECT pg_try_advisory_lock(${LOCK[0]}, ${LOCK[1]}) AS free`
  if (!free) {
    console.log('Waiting for an Update Sidewalk Street Names run to finish...')
    await lock`SELECT pg_advisory_lock(${LOCK[0]}, ${LOCK[1]})`
  }
  const started = Date.now()
  let [read, named] = [0, 0]
  for (const [i, cell] of cells.entries()) {
    const result = await rebuildBox(sql, cell)
    read += result.read
    named += result.named
    if (result.read)
      console.log(`[${i + 1}/${cells.length}] cell ${cell.slice(0, 2).map(v => v.toFixed(3)).join(',')}: ${result.named}/${result.read} ways named (${Math.round((Date.now() - started) / 1000)} s)`)
  }
  console.log(`Sidewalk street names: ${named} of ${read} ways named in ${cells.length} cells, ${((Date.now() - started) / 1000).toFixed(1)} s.`)
  await lock`SELECT pg_advisory_unlock(${LOCK[0]}, ${LOCK[1]})`
  lock.release()
  await sql.end({ timeout: 5 })
}

if (import.meta.main) {
  // A connection past its lifetime keeps `end()` waiting forever once a reserved one has been released: the lock's session lasts the run.
  await main(postgres(dbUrl, { onnotice, max: 3, max_lifetime: null }))
  process.exit(process.exitCode ?? 0)
}
