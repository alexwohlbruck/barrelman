/**
 * Build road_beds: the stretches of major roads on the ground whose
 * carriageway the terrain does not hold level (see src/lib/road-beds/beds.ts),
 * solved from OSM and Mapterhorn terrain.
 *
 *   bun run import/generate-road-beds.ts                  the REGIONS areas
 *   bun run import/generate-road-beds.ts --bbox w,s,e,n   one area
 *
 * Built cell by cell (--cell degrees), each replacing the beds of every road
 * crossing it, so a run can be stopped and started again.
 */
import postgres from 'postgres'
import { dbUrl, onnotice } from '../src/db'
import { argValue } from '../src/lib/cli-args'
import { resolveRegions } from '../src/config/regions'
import { parseBbox, regionAreas } from '../src/lib/bridge-decks/areas'
import { Dem, DEM_TILES, demConfigured } from '../src/lib/bridge-decks/dem'
import { mercator, unprojected, type Point } from '../src/lib/bridge-mesh/decks'
import { beds } from '../src/lib/road-beds/beds'

/**
 * Roads wide enough that a lean across them shows, at their widths per lane in
 * metres. Not their ramps: narrow, and squeezed in beside paths and walls a
 * fill would lift.
 */
const LANE: Record<string, number> = { motorway: 3.65, trunk: 3.5, primary: 3.3 }

type Row = { id: string; highway: string; width: string | null; lanes: string | null; coords: number[][] }

/** Metres of shoulder each side of a carriageway's lanes. */
const SHOULDER = 1

/** Metres from a road's centreline to its carriageway's edge: tagged, else its lanes at their class's width and a shoulder. */
export function halfWidth(r: Pick<Row, 'highway' | 'width' | 'lanes'>): number {
  const tagged = Number.parseFloat(r.width ?? '')
  if (tagged >= 3 && tagged <= 60) return tagged / 2
  const lanes = Number.parseInt(r.lanes ?? '') || 2
  return (lanes * (LANE[r.highway] ?? 3.3)) / 2 + SHOULDER
}

export async function buildArea(sql: postgres.Sql, dem: Dem, [w, s, e, n]: [number, number, number, number]): Promise<number> {
  const rows = await sql<Row[]>`
    SELECT osm_id::text AS id, tags->>'highway' AS highway, COALESCE(tags->>'width:carriageway', tags->>'width') AS width,
           tags->>'lanes' AS lanes, ST_AsGeoJSON(geom)::json->'coordinates' AS coords
    FROM geo_places
    WHERE osm_type = 'W' AND geom_type = 'line' AND geom && ST_MakeEnvelope(${w}, ${s}, ${e}, ${n}, 4326)
      AND tags->>'highway' = ANY(${Object.keys(LANE)})
      AND COALESCE(tags->>'bridge', 'no') = 'no' AND COALESCE(tags->>'tunnel', 'no') = 'no' AND COALESCE(tags->>'covered', 'no') = 'no'`
  const roads = rows.map(r => ({ id: r.id, half: halfWidth(r), points: r.coords.map(c => mercator(c)) }))
  await dem.load(roads.flatMap(r => r.points))
  const at = (p: Point) => {
    const h = dem.at(p)
    return Number.isFinite(h) ? h : null
  }
  const found = roads.flatMap(r => beds(r.points, r.half, at).map((b, k) => ({
    id: `bed:${r.id}:${k}`, way: r.id, half: r.half, floor: b.floor,
    line: `LINESTRING(${b.points.map(p => unprojected(p).join(' ')).join(',')})`,
  })))
  await sql.begin(async tx => {
    await tx`DELETE FROM road_beds WHERE way = ANY(${roads.map(r => r.id)}::bigint[])`
    for (const b of found)
      await tx`INSERT INTO road_beds (id, way, half, floor, geom) VALUES (${b.id}, ${b.way}, ${b.half}, ${b.floor}, ST_GeomFromText(${b.line}, 4326))`
  })
  return found.length
}

if (import.meta.main) {
  const args = process.argv.slice(2)
  const bbox = argValue(args, 'bbox')
  const cell = Number(argValue(args, 'cell') ?? 0.25)
  if (!demConfigured(DEM_TILES)) throw new Error(`BRIDGE_DECKS_DEM_TILES is "${DEM_TILES}": road beds need a terrain source`)
  const areas = bbox ? [parseBbox(bbox)] : regionAreas(await resolveRegions())
  const sql = postgres(dbUrl, { onnotice, max: 2, max_lifetime: null })
  const started = Date.now()
  let total = 0
  for (const [w, s, e, n] of areas)
    for (let x = w; x < e; x += cell)
      for (let y = s; y < n; y += cell) {
        // Terrain read afresh per cell, so a region's tiles are not all held at once.
        const count = await buildArea(sql, new Dem(), [x, y, Math.min(e, x + cell), Math.min(n, y + cell)])
        total += count
        if (count) console.log(`cell ${x.toFixed(2)},${y.toFixed(2)}: ${count} beds (${Math.round((Date.now() - started) / 1000)} s)`)
      }
  console.log(`Road beds: ${total} in ${((Date.now() - started) / 1000).toFixed(1)} s`)
  await sql.end()
}
