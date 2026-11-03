/**
 * Write each cycle way's heights on decks and through tunnels into
 * bicycle_ways.elevation, which its tiles serve as is.
 *
 *   bun run import/elevate-bicycle-ways.ts                 every bridge and tunnel way
 *   bun run import/elevate-bicycle-ways.ts --bbox w,s,e,n  those in one area
 *
 * Run after Build Bridge Decks and Build Tunnel Cuts, which it reads.
 */
import { connection } from '../src/db'
import { argValue } from '../src/lib/cli-args'
import { parseBbox } from '../src/lib/bridge-decks/areas'
import { mercator } from '../src/lib/bridge-mesh/decks'
import { batchByTile, boundsOf, wayElevations, type CycleWay } from '../src/lib/line-elevation/cycle-ways'
import { sourcesIn } from '../src/lib/line-elevation/lookup'

const sql = connection
const bbox = argValue(process.argv.slice(2), 'bbox')
const area = bbox ? parseBbox(bbox) : null
const within = area ? sql`AND geom && ST_MakeEnvelope(${area[0]}, ${area[1]}, ${area[2]}, ${area[3]}, 4326)` : sql``

await sql`ALTER TABLE bicycle_ways ADD COLUMN IF NOT EXISTS elevation text`
const rows = await sql<Array<{ id: string; coords: number[][] }>>`
  SELECT osm_id::text AS id, ST_AsGeoJSON(geom)::json->'coordinates' AS coords
  FROM bicycle_ways WHERE (bridge OR tunnel) ${within}`
const ways: CycleWay[] = rows.map(r => ({ id: Number(r.id), points: r.coords.map(mercator) }))
const batches = batchByTile(ways)
console.log(`${ways.length} cycle ways on bridges or in tunnels, in ${batches.length} batches`)

const started = Date.now()
let raised = 0
for (const [i, batch] of batches.entries()) {
  const elevations = wayElevations(batch, await sourcesIn(boundsOf(batch)))
  raised += elevations.filter(e => e !== null).length
  await sql`
    UPDATE bicycle_ways b SET elevation = d.e
    FROM unnest(${batch.map(w => String(w.id))}::bigint[], ${elevations}::text[]) AS d(id, e)
    WHERE b.osm_id = d.id AND b.elevation IS DISTINCT FROM d.e`
  if ((i + 1) % 200 === 0) console.log(`[${i + 1}/${batches.length}] ${raised} raised or sunk (${Math.round((Date.now() - started) / 1000)} s)`)
}
const cleared = await sql`
  UPDATE bicycle_ways SET elevation = NULL
  WHERE elevation IS NOT NULL AND NOT coalesce(bridge OR tunnel, false) ${within}`
console.log(`Done: ${raised} of ${ways.length} ways raised or sunk, ${cleared.count} cleared, in ${Math.round((Date.now() - started) / 1000)} s.`)
await sql.end({ timeout: 5 })
process.exit(0)
