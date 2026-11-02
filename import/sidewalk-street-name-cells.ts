/**
 * Rebuilding sidewalk_street_names one box at a time, shared by Build Sidewalk
 * Street Names (import/generate-sidewalk-street-names.ts) and Update Sidewalk
 * Street Names (import/update-sidewalk-street-names.ts).
 *
 * A box covers every nameable pedestrian way whose bounding box meets it. Each
 * is matched against the named roads around it and its zones are swapped in,
 * with the box's stale rows dropped, in one transaction. Rows are keyed on the
 * way they describe and rewritten whole, so boxes may overlap and be rebuilt
 * as often as wanted.
 */
import type postgres from 'postgres'
import type { Bbox } from '../src/config/regions'
import { localProjection, streetMatcher, ROAD_RANK, SEARCH_RADIUS, type Road, type XY } from '../src/lib/sidewalk-names/zones'
import { levelOf, taggedStreet, PEDESTRIAN_PREDICATE, type StoredZone } from '../src/lib/sidewalk-names/ways'

type Sql = postgres.Sql

/** Held by every run that writes sidewalk_street_names. */
export const LOCK = [5393739, 3] as const

interface LevelColumns {
  layer: string | null
  bridge: string | null
  tunnel: string | null
}

type PathRow = LevelColumns & { id: string; sidepath_of: string | null; street_name: string | null; coords: XY[] }
type RoadRow = LevelColumns & { name: string; highway: string; coords: XY[] }

const ROAD_CLASSES = Object.keys(ROAD_RANK)
/** Degrees of latitude a road is read past the paths, a little over the search radius. */
const READ_PAST = (SEARCH_RADIUS * 1.5) / 111_320
const INSERT_BATCH = 1000

const levelColumns = (sql: Sql) => sql`g.tags->>'layer' AS layer, g.tags->>'bridge' AS bridge, g.tags->>'tunnel' AS tunnel`

export interface WayZones {
  id: string
  coords: XY[]
  zones: StoredZone[]
}

/** The zones of every nameable way meeting `box`, from the roads around them. */
export async function zonesIn(sql: Sql, [w, s, e, n]: Bbox): Promise<{ read: number; named: WayZones[] }> {
  const paths: PathRow[] = await sql`
    SELECT g.osm_id::text AS id, ${levelColumns(sql)},
           g.tags->>'is_sidepath:of:name' AS sidepath_of, g.tags->>'street:name' AS street_name,
           ST_AsGeoJSON(CASE WHEN g.geom_type = 'area' THEN ST_ExteriorRing(g.geom) ELSE g.geom END)::json->'coordinates' AS coords
    FROM geo_places g
    WHERE g.osm_type = 'W' AND g.tags ? 'highway' AND ${sql.unsafe(PEDESTRIAN_PREDICATE)}
      AND g.geom && ST_MakeEnvelope(${w}, ${s}, ${e}, ${n}, 4326)`
  if (!paths.length) return { read: 0, named: [] }

  let [minX, minY, maxX, maxY] = [Infinity, Infinity, -Infinity, -Infinity]
  for (const p of paths)
    for (const [x, y] of p.coords) [minX, minY, maxX, maxY] = [Math.min(minX, x), Math.min(minY, y), Math.max(maxX, x), Math.max(maxY, y)]
  const dx = READ_PAST / Math.cos((((minY + maxY) / 2) * Math.PI) / 180)
  const roads: RoadRow[] = await sql`
    SELECT g.tags->>'name' AS name, g.tags->>'highway' AS highway, ${levelColumns(sql)},
           ST_AsGeoJSON(g.geom)::json->'coordinates' AS coords
    FROM geo_places g
    WHERE g.geom_type = 'line' AND g.tags ? 'highway' AND GeometryType(g.geom) = 'LINESTRING'
      AND g.tags->>'highway' IN ${sql(ROAD_CLASSES)} AND COALESCE(g.tags->>'name', '') <> ''
      AND g.geom && ST_MakeEnvelope(${minX - dx}, ${minY - READ_PAST}, ${maxX + dx}, ${maxY + READ_PAST}, 4326)`

  const project = localProjection([(w + e) / 2, (s + n) / 2])
  const byLevel = new Map<number, Road[]>()
  for (const r of roads) {
    const level = levelOf(r)
    if (!byLevel.has(level)) byLevel.set(level, [])
    byLevel.get(level)!.push({ name: r.name.trim(), rank: ROAD_RANK[r.highway], points: r.coords.map(project) })
  }

  const matchers = new Map([...byLevel].map(([level, rs]) => [level, streetMatcher(rs)]))
  const named: WayZones[] = []
  for (const p of paths) {
    const tagged = taggedStreet({ sidepathOf: p.sidepath_of, streetName: p.street_name })
    const zones: StoredZone[] = tagged
      ? [{ from: 0, to: 1, name: tagged, source: 'tag' }]
      : (matchers.get(levelOf(p))?.(p.coords.map(project)) ?? []).map(z => ({ ...z, source: 'geometry' }))
    if (zones.length) named.push({ id: p.id, coords: p.coords, zones })
  }
  return { read: paths.length, named }
}

/** Rebuilds the rows of every way meeting `box`; how many ways it read and named. */
export async function rebuildBox(sql: Sql, box: Bbox): Promise<{ read: number; named: number }> {
  const { read, named } = await zonesIn(sql, box)
  const [w, s, e, n] = box
  await sql.begin(async rawTx => {
    const tx = rawTx as unknown as Sql
    await tx`DELETE FROM sidewalk_street_names WHERE geom && ST_MakeEnvelope(${w}, ${s}, ${e}, ${n}, 4326)`
    for (let i = 0; i < named.length; i += INSERT_BATCH) {
      const batch = named.slice(i, i + INSERT_BATCH).map(r => ({
        id: r.id,
        line: `LINESTRING(${r.coords.map(c => c.join(' ')).join(',')})`,
        zones: r.zones.map(z => ({ ...z })),
      }))
      await tx`
        INSERT INTO sidewalk_street_names (way_id, geom, zones)
        SELECT r.id, ST_GeomFromText(r.line, 4326), r.zones
        FROM jsonb_to_recordset(${tx.json(batch)}::jsonb) AS r(id bigint, line text, zones jsonb)
        ON CONFLICT (way_id) DO UPDATE SET geom = EXCLUDED.geom, zones = EXCLUDED.zones, updated_at = now()`
    }
  })
  return { read, named: named.length }
}

export async function tableMissing(sql: Sql): Promise<boolean> {
  const [{ found }] = await sql`SELECT to_regclass('sidewalk_street_names') IS NOT NULL AS found`
  return !found
}
