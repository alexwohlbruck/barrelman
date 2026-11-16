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
import { mercator, metresPerUnit, unprojected, type Point } from '../src/lib/bridge-mesh/decks'
import { beds, type BedRoad } from '../src/lib/road-beds/beds'
import { nodeHeights } from './bridge-deck-cells'

/**
 * Roads wide enough that a lean across them shows, at their widths per lane in
 * metres. Not their ramps: narrow, and squeezed in beside paths and walls a
 * fill would lift.
 */
const LANE: Record<string, number> = { motorway: 3.65, trunk: 3.5, primary: 3.3 }

type Row = { id: string; highway: string; road: string; width: string | null; lanes: string | null; layer: string | null; covered: boolean; coords: number[][] }

/** Metres of shoulder each side of a carriageway's lanes. */
const SHOULDER = 1

/** Metres from a road's centreline to its carriageway's edge: tagged, else its lanes at their class's width and a shoulder. */
export function halfWidth(r: Pick<Row, 'highway' | 'width' | 'lanes'>): number {
  const tagged = Number.parseFloat(r.width ?? '')
  if (tagged >= 3 && tagged <= 60) return tagged / 2
  const lanes = Number.parseInt(r.lanes ?? '') || 2
  return (lanes * (LANE[r.highway] ?? 3.3)) / 2 + SHOULDER
}

const layerOf = (r: Pick<Row, 'layer'>) => Number.parseInt(r.layer ?? '') || 0
const key = (c: number[]) => c.join(',')

/** Ways of one road joined end to end, each road's ways in order along it. */
export function roadsOf<T extends Pick<Row, 'road' | 'coords'>>(ways: T[]): T[][] {
  const starting = new Map<string, T[]>()
  for (const w of ways) starting.set(key(w.coords[0]), [...(starting.get(key(w.coords[0])) ?? []), w])
  const next = (w: T) => (starting.get(key(w.coords[w.coords.length - 1])) ?? []).find(o => o !== w && o.road === w.road)
  const followed = new Set(ways.map(next).filter(Boolean))
  const used = new Set<T>()
  const out: T[][] = []
  for (const w of [...ways.filter(w => !followed.has(w)), ...ways]) {
    if (used.has(w)) continue
    const road: T[] = []
    for (let c: T | undefined = w; c && !used.has(c); c = next(c)) {
      used.add(c)
      road.push(c)
    }
    out.push(road)
  }
  return out
}

/** Metres past a higher way's own half width that a road's terrain is still that way's: a map's terrain tiles blur a deck the lidar took for ground this far past its edge. */
const UNDER_REACH = 5

const roadRows = (sql: postgres.Sql, where: postgres.PendingQuery<postgres.Row[]>) => sql<Row[]>`
  SELECT osm_id::text AS id, tags->>'highway' AS highway,
         tags->>'highway' || '/' || COALESCE(tags->>'ref', tags->>'name', '') || '/' || COALESCE(tags->>'oneway', '') AS road,
         COALESCE(tags->>'width:carriageway', tags->>'width') AS width, tags->>'lanes' AS lanes, tags->>'layer' AS layer,
         COALESCE(tags->>'covered', 'no') NOT IN ('no') AS covered, ST_AsGeoJSON(geom)::json->'coordinates' AS coords
  FROM geo_places
  WHERE osm_type = 'W' AND geom_type = 'line' AND ${where}
    AND tags->>'highway' = ANY(${Object.keys(LANE)})
    AND COALESCE(tags->>'bridge', 'no') = 'no' AND COALESCE(tags->>'tunnel', 'no') = 'no'`

export async function buildArea(sql: postgres.Sql, dem: Dem, [w, s, e, n]: [number, number, number, number]): Promise<number> {
  const box = sql`ST_MakeEnvelope(${w}, ${s}, ${e}, ${n}, 4326)`
  const rows = await roadRows(sql, sql`geom && ${box}`)
  if (!rows.length) {
    await sql`DELETE FROM road_beds WHERE geom && ${box}`
    return 0
  }
  // The ways joining each end, so a road's beds run on the same whichever cell builds them.
  const ends = rows.flatMap(r => [r.coords[0], r.coords[r.coords.length - 1]])
  const beyond = await roadRows(sql, sql`geom && ST_Expand(${box}, 0.005) AND ST_Intersects(geom, ST_SetSRID(ST_Collect(ARRAY(
    SELECT ST_MakePoint(x, y) FROM unnest(${ends.map(c => c[0])}::float8[], ${ends.map(c => c[1])}::float8[]) AS t(x, y))), 4326))`)
  const ways = [...new Map([...rows, ...beyond].map(r => [r.id, r])).values()]
  // Every way above one of them: a road's terrain beneath one is the structure's, not its own.
  const above: Array<{ layer: number; half: number; points: Point[] }> = (await sql<Row[]>`
    SELECT osm_id::text AS id, tags->>'highway' AS highway, tags->>'width' AS width, tags->>'lanes' AS lanes, tags->>'layer' AS layer,
           ST_AsGeoJSON(geom)::json->'coordinates' AS coords
    FROM geo_places
    WHERE osm_type = 'W' AND geom_type = 'line' AND geom && ST_Expand(${box}, 0.005) AND tags ? 'highway'
      AND COALESCE(tags->>'tunnel', 'no') = 'no' AND (COALESCE(tags->>'layer', '0') <> '0' OR COALESCE(tags->>'bridge', 'no') <> 'no')
      AND GeometryType(geom) = 'LINESTRING'`).map(r => ({ layer: Math.max(1, layerOf(r)), half: halfWidth(r), points: r.coords.map(c => mercator(c)) }))
  const pins = await nodeHeights(sql, ways.map(r => r.id))
  const built = new Set(rows.map(r => r.id))
  const roads: BedRoad[] = roadsOf(ways).map(road => ({
    ways: road.map(r => {
      const own = layerOf(r)
      const higher = above.filter(a => a.layer > own)
      return {
        id: Number(r.id),
        points: r.coords.map(c => mercator(c)),
        half: halfWidth(r),
        pins: r.coords.map(c => pins.get(key(c)) ?? null),
        under: r.covered || ((p: Point) => higher.some(a => nearer(a.points, p, (a.half + UNDER_REACH) / metresPerUnit(p[1])))),
      }
    }),
  }))
  await dem.load(roads.flatMap(r => r.ways.flatMap(w => w.points)))
  const at = (p: Point) => {
    const h = dem.at(p)
    return Number.isFinite(h) ? h : null
  }
  const counts = new Map<number, number>()
  const found = roads.flatMap(r => beds(r, at)).filter(b => built.has(String(b.way))).map(b => {
    const k = counts.get(b.way) ?? 0
    counts.set(b.way, k + 1)
    const half = halfWidth(rows.find(r => r.id === String(b.way))!)
    return { id: `bed:${b.way}:${k}`, way: b.way, half, floor: b.floor, level: b.level, line: `LINESTRING(${b.points.map(p => unprojected(p).join(' ')).join(',')})` }
  })
  await sql.begin(async tx => {
    // Beds in the cell of a way now a bridge, tunnel or gone go too.
    await tx`DELETE FROM road_beds WHERE way = ANY(${rows.map(r => r.id)}::bigint[]) OR geom && ${box}`
    for (const b of found)
      await tx`INSERT INTO road_beds (id, way, half, floor, level, geom) VALUES (${b.id}, ${b.way}, ${b.half}, ${b.floor}, ${b.level}, ST_GeomFromText(${b.line}, 4326))`
  })
  return found.length
}

/** Whether a point lies within `reach` mercator units of a line. */
function nearer(points: Point[], [x, y]: Point, reach: number): boolean {
  for (let i = 1; i < points.length; i++) {
    const [a, b] = [points[i - 1], points[i]]
    const [ex, ey] = [b[0] - a[0], b[1] - a[1]]
    const length2 = ex * ex + ey * ey
    const t = length2 ? Math.max(0, Math.min(1, ((x - a[0]) * ex + (y - a[1]) * ey) / length2)) : 0
    if (Math.hypot(a[0] + ex * t - x, a[1] + ey * t - y) < reach) return true
  }
  return false
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
