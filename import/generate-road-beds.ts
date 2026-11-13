/**
 * Build road_beds: the stretches of major roads on the ground whose
 * carriageway the terrain does not hold level (see src/lib/road-beds/beds.ts),
 * solved from OSM and Mapterhorn terrain.
 *
 *   bun run import/generate-road-beds.ts --bbox w,s,e,n
 *
 * The area's beds are replaced whole: those of every road crossing it.
 */
import postgres from 'postgres'
import { dbUrl, onnotice } from '../src/db'
import { argValue } from '../src/lib/cli-args'
import { parseBbox } from '../src/lib/bridge-decks/areas'
import { Dem, DEM_TILES, demConfigured } from '../src/lib/bridge-decks/dem'
import { mercator, unprojected, type Point } from '../src/lib/bridge-mesh/decks'
import { beds } from '../src/lib/road-beds/beds'

/** Roads wide and fast enough that a lean across them shows, at their widths per lane in metres. */
const LANE: Record<string, number> = { motorway: 3.65, motorway_link: 3.65, trunk: 3.5, trunk_link: 3.5, primary: 3.3, primary_link: 3.3 }

type Row = { id: string; highway: string; width: string | null; lanes: string | null; oneway: string | null; coords: number[][] }

/** Metres from a road's centreline to its carriageway's edge: tagged, else its lanes at their class's width. */
export function halfWidth(r: Pick<Row, 'highway' | 'width' | 'lanes' | 'oneway'>): number {
  const tagged = Number.parseFloat(r.width ?? '')
  if (tagged >= 3 && tagged <= 60) return tagged / 2
  const lanes = Number.parseInt(r.lanes ?? '') || 2
  return (lanes * (LANE[r.highway] ?? 3.3)) / 2
}

export async function buildArea(sql: postgres.Sql, dem: Dem, [w, s, e, n]: [number, number, number, number]): Promise<number> {
  const rows = await sql<Row[]>`
    SELECT osm_id::text AS id, tags->>'highway' AS highway, COALESCE(tags->>'width:carriageway', tags->>'width') AS width,
           tags->>'lanes' AS lanes, tags->>'oneway' AS oneway, ST_AsGeoJSON(geom)::json->'coordinates' AS coords
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
  const bbox = argValue(process.argv.slice(2), 'bbox')
  if (!bbox) throw new Error('--bbox=w,s,e,n is required')
  if (!demConfigured(DEM_TILES)) throw new Error(`BRIDGE_DECKS_DEM_TILES is "${DEM_TILES}": road beds need a terrain source`)
  const sql = postgres(dbUrl, { onnotice, max: 2, max_lifetime: null })
  const started = Date.now()
  const count = await buildArea(sql, new Dem(), parseBbox(bbox))
  console.log(`Road beds: ${count} in ${((Date.now() - started) / 1000).toFixed(1)} s`)
  await sql.end()
}
