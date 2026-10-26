/**
 * Build tunnel_cuts: the cut down to every portal where a tunnel comes out of
 * the ground, solved from OSM and Mapterhorn terrain.
 *
 *   bun run import/generate-tunnel-cuts.ts                  the REGIONS areas
 *   bun run import/generate-tunnel-cuts.ts --bbox w,s,e,n   one area
 *
 * The area's tunnels are read once, by their tag's index, and only the cells
 * holding a tunnel's end are rebuilt, each in place (see tunnel-cut-cells.ts),
 * so a run can be stopped and started again. Update Tunnel Cuts keeps them in
 * step with OSM afterwards.
 */
import postgres from 'postgres'
import { resolveRegions } from '../src/config/regions'
import { dbUrl, onnotice } from '../src/db'
import { argValue } from '../src/lib/cli-args'
import { parseBbox, regionAreas } from '../src/lib/bridge-decks/areas'
import { Dem, DEM_TILES, demConfigured } from '../src/lib/bridge-decks/dem'
import { degreeBox, grid, toUnits, type UnitBox } from '../src/lib/bridge-decks/grid'
import { unprojected } from '../src/lib/bridge-mesh/decks'
import type { Way } from '../src/lib/tunnel-cuts/build'
import { LOCK, rebuildBoxes, tableMissing, tunnelsIn } from './tunnel-cut-cells'

const args = process.argv.slice(2)
const CELL = Number(argValue(args, 'cell') ?? 0.05)

type Sql = postgres.Sql

/**
 * Rebuilds every `size`-degree cell of `area` a tunnel ends in or near, each
 * handed only the tunnels around it. Returns cuts stored and cells built.
 */
export async function buildArea(sql: Sql, dem: Dem, area: [number, number, number, number], size = CELL): Promise<{ total: number; built: number }> {
  const g = grid(toUnits(size))
  const tunnels = (await tunnelsIn(sql, area)).filter(t => t.layer < 0)
  const cellOf = (p: [number, number]) => {
    const [lng, lat] = unprojected(p)
    return g.at([toUnits(lng), toUnits(lat)])
  }
  // A portal lies within a cut's length of its tunnel's end, and may be in the cell beside it.
  const near = new Map<string, Way[]>()
  const ends = new Set<string>()
  for (const t of tunnels) {
    const cells = new Set(t.points.map(cellOf))
    for (const end of [t.points[0], t.points.at(-1)!]) ends.add(cellOf(end))
    for (const c of cells) {
      const [cx, cy] = c.split(',').map(Number)
      for (let dx = -1; dx <= 1; dx++) for (let dy = -1; dy <= 1; dy++) {
        const k = `${cx + dx},${cy + dy}`
        near.set(k, [...(near.get(k) ?? []), t])
      }
    }
  }
  const wanted = new Set<string>()
  for (const c of ends) {
    const [cx, cy] = c.split(',').map(Number)
    for (let dx = -1; dx <= 1; dx++) for (let dy = -1; dy <= 1; dy++) wanted.add(`${cx + dx},${cy + dy}`)
  }
  const [w, s, e, n] = area.map(toUnits)
  const cells = [...wanted].map(k => [k, g.box(k)] as [string, UnitBox]).filter(([, b]) => b[0] < e && b[2] > w && b[1] < n && b[3] > s)
  const started = Date.now()
  let total = 0
  for (const [i, [key, box]] of cells.entries()) {
    const stored = await rebuildBoxes(sql, dem, [box], [...new Set(near.get(key) ?? [])])
    total += stored
    if (stored) console.log(`[${i + 1}/${cells.length}] cell ${degreeBox(box).slice(0, 2).join(',')}: ${stored} cuts (${Math.round((Date.now() - started) / 1000)} s)`)
  }
  return { total, built: cells.length }
}

async function main(sql: Sql) {
  if (await tableMissing(sql)) throw new Error('tunnel_cuts does not exist; start the API once, or run import/create-detail-views.sql.')
  if (!demConfigured(DEM_TILES)) throw new Error(`BRIDGE_DECKS_DEM_TILES is "${DEM_TILES}": tunnel cuts need a terrain source`)
  const bbox = argValue(args, 'bbox')
  const areas = bbox ? [parseBbox(bbox)] : regionAreas(await resolveRegions())

  const lock = await sql.reserve()
  const [{ free }] = await lock`SELECT pg_try_advisory_lock(${LOCK[0]}, ${LOCK[1]}) AS free`
  if (!free) {
    console.log('Waiting for an Update Tunnel Cuts run to finish...')
    await lock`SELECT pg_advisory_lock(${LOCK[0]}, ${LOCK[1]})`
  }
  const dem = new Dem()
  let total = 0
  let built = 0
  for (const area of areas) {
    const result = await buildArea(sql, dem, area)
    total += result.total
    built += result.built
  }
  console.log(`Tunnel cuts: ${total} built in ${built} cells.`)
  await lock`SELECT pg_advisory_unlock(${LOCK[0]}, ${LOCK[1]})`
  lock.release()
  await sql.end()
}

if (import.meta.main) {
  await main(postgres(dbUrl, { onnotice, max: 3 }))
  process.exit(process.exitCode ?? 0)
}
