/**
 * Build tunnel_cuts: the cut down to every portal where a tunnel comes out of
 * the ground, solved from OSM and Mapterhorn terrain.
 *
 *   bun run import/generate-tunnel-cuts.ts                  the REGIONS areas
 *   bun run import/generate-tunnel-cuts.ts --bbox w,s,e,n   one area
 *
 * The area's tunnels are read once, by their tag's index, and only the cells
 * a tunnel's portals can lie in are rebuilt, each in place (see tunnel-cut-cells.ts),
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
import { CUT_MAX } from '../src/lib/tunnel-cuts/tunnels'
import { LOCK, READ_PAST, rebuildBoxes, tableMissing, tunnelsIn } from './tunnel-cut-cells'

const args = process.argv.slice(2)
const CELL = Number(argValue(args, 'cell') ?? 0.25)

type Sql = postgres.Sql

/**
 * Rebuilds every `size`-degree cell of `area` a tunnel ends in or near, each
 * handed only the tunnels around it. Returns cuts stored and cells built.
 */
export async function buildArea(sql: Sql, dem: Dem, area: [number, number, number, number], size = CELL): Promise<{ total: number; built: number }> {
  const g = grid(toUnits(size))
  const tunnels = (await tunnelsIn(sql, area)).filter(t => t.layer < 0)
  const cellsAround = (p: [number, number], metres: number) => {
    const [lng, lat] = unprojected(p)
    const [dx, dy] = [metres / (111320 * Math.cos((lat * Math.PI) / 180)), metres / 111320]
    return [-1, 0, 1].flatMap(i => [-1, 0, 1].map(j => g.at([toUnits(lng + i * dx), toUnits(lat + j * dy)])))
  }
  // A portal lies within a cut's length of its tunnel's end, so in the end's cell or, near its edge, the one beside.
  const wanted = new Set(tunnels.flatMap(t => [t.points[0], t.points.at(-1)!].flatMap(p => cellsAround(p, 2 * CUT_MAX))))
  // Each cell is handed every tunnel within what it reads, so one running on into another is known to.
  const reading = new Map<string, Set<Way>>()
  for (const t of tunnels)
    for (const c of new Set(t.points.flatMap(p => cellsAround(p, READ_PAST * 1.1)))) if (wanted.has(c)) reading.set(c, (reading.get(c) ?? new Set()).add(t))
  const [w, s, e, n] = area.map(toUnits)
  const cells = [...wanted].map(k => [k, g.box(k)] as [string, UnitBox]).filter(([, b]) => b[0] < e && b[2] > w && b[1] < n && b[3] > s)
  const started = Date.now()
  let total = 0
  for (const [i, [key, box]] of cells.entries()) {
    const stored = await rebuildBoxes(sql, dem, [box], [...(reading.get(key) ?? [])])
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
  await sql.end({ timeout: 5 })
}

if (import.meta.main) {
  // A connection past its lifetime keeps `end()` waiting forever once a reserved one has been released: the lock's session lasts the run.
  await main(postgres(dbUrl, { onnotice, max: 3, max_lifetime: null }))
  process.exit(process.exitCode ?? 0)
}
