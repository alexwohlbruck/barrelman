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
 * The area is worked through in cells (`--cell`, degrees). A cell rebuilds the
 * decks whose midpoint lies in it and reads beyond its edges as far as its
 * decks run, so cells can be rebuilt one at a time, in any order, as often as
 * the map changes. Terrain tiles are fetched as needed and kept in a bounded cache.
 */
import postgres from 'postgres'
import { resolveRegions } from '../src/config/regions'
import { dbUrl, onnotice } from '../src/db'
import { argValue } from '../src/lib/cli-args'
import { cellsCovering, parseBbox, regionAreas } from '../src/lib/bridge-decks/areas'
import { buildDecks, STEP, type Crossed, type DeckInput } from '../src/lib/bridge-decks/build'
import { Dem } from '../src/lib/bridge-decks/dem'
import { lngLat, mercator, type Kind, type Point, type Way } from '../src/lib/bridge-decks/profile'

const ROADS = ['motorway', 'motorway_link', 'trunk', 'trunk_link', 'primary', 'primary_link', 'secondary', 'secondary_link',
  'tertiary', 'tertiary_link', 'unclassified', 'residential', 'living_street', 'service', 'busway', 'track', 'road']
const PATHS = ['footway', 'cycleway', 'path', 'pedestrian', 'steps', 'bridleway']
const RAILS = ['rail', 'light_rail', 'subway', 'tram', 'narrow_gauge', 'monorail', 'preserved', 'funicular']

/** Carriageway width when none is tagged, by lanes at a class's lane width. */
const LANE: Record<string, number> = { motorway: 3.6, trunk: 3.6, motorway_link: 3.4, trunk_link: 3.4, primary: 3.4, service: 2.8 }

const metres = (value: string | null): number | null => {
  const m = value?.match(/^\s*(-?[0-9]+(?:\.[0-9]+)?)\s*(m|ft|')?\s*$/)
  if (!m) return null
  return Number(m[1]) * (m[2] === 'ft' || m[2] === "'" ? 0.3048 : 1)
}

const args = process.argv.slice(2)
const flag = (name: string) => argValue(args, name)
const CELL = Number(flag('cell') ?? 0.25)
/** Decks per INSERT: one statement per batch rather than a round trip per deck. */
const INSERT_BATCH = 500

const sql = postgres(dbUrl, { onnotice, max: 2 })

type Row = { id: string; highway: string | null; railway: string | null; layer: string | null; width: string | null;
  lanes: string | null; oneway: string | null; wikidata: string | null; coords: number[][] }

const BRIDGE_WAYS = sql`
  SELECT osm_id::text AS id, tags->>'highway' AS highway, tags->>'railway' AS railway, tags->>'layer' AS layer,
         COALESCE(tags->>'width:carriageway', tags->>'width') AS width, tags->>'lanes' AS lanes, tags->>'oneway' AS oneway,
         COALESCE(tags->>'bridge:wikidata', tags->>'wikidata') AS wikidata,
         ST_AsGeoJSON(geom)::json->'coordinates' AS coords
  FROM geo_places`
const IS_BRIDGE = sql`
  osm_type = 'W' AND geom_type = 'line' AND COALESCE(tags->>'bridge', 'no') NOT IN ('no', 'abandoned')
  AND COALESCE(tags->>'tunnel', 'no') = 'no'
  AND (tags->>'highway' = ANY(${[...ROADS, ...PATHS]}) OR tags->>'railway' = ANY(${RAILS}))
  AND GeometryType(geom) = 'LINESTRING'`

function toWay(r: Row): Way {
  const kind: Kind = r.railway ? 'rail' : PATHS.includes(r.highway!) ? 'path' : 'road'
  const lanes = Number.parseInt(r.lanes ?? '') || (r.oneway === 'yes' || r.highway?.startsWith('motorway') ? 1 : 2)
  const tagged = metres(r.width)
  const width = kind === 'rail' ? 5 : kind === 'path' ? (tagged && tagged < 12 ? tagged : 3)
    : tagged && tagged >= 2.5 && tagged <= 60 ? tagged : lanes * (LANE[r.highway!] ?? 3.3)
  return { id: Number(r.id), points: r.coords.map(([lng, lat]) => mercator(lng, lat)), kind, layer: Math.max(1, Number.parseInt(r.layer ?? '') || 1), width }
}

/** Bridge ways around a cell, followed beyond it for as long as a deck carries on. */
async function waysAround(w: number, s: number, e: number, n: number): Promise<Row[]> {
  const margin = 0.01
  const rows: Row[] = await sql`${BRIDGE_WAYS} WHERE geom && ST_MakeEnvelope(${w - margin}, ${s - margin}, ${e + margin}, ${n + margin}, 4326) AND ${IS_BRIDGE}`
  const seen = new Set(rows.map(r => r.id))
  for (let round = 0; round < 100; round++) {
    const degree = new Map<string, number>()
    for (const r of rows) for (const c of [r.coords[0], r.coords[r.coords.length - 1]]) degree.set(c.join(','), (degree.get(c.join(',')) ?? 0) + 1)
    const open = [...degree].filter(([, d]) => d === 1).map(([k]) => k.split(',').map(Number))
    if (!open.length) break
    const found: Row[] = await sql`
      SELECT DISTINCT ON (b.id) b.* FROM unnest(${open.map(c => c[0])}::float8[], ${open.map(c => c[1])}::float8[]) AS o(lng, lat)
      CROSS JOIN LATERAL (${BRIDGE_WAYS}
        WHERE geom && ST_Expand(ST_SetSRID(ST_MakePoint(o.lng, o.lat), 4326), 1e-7) AND ${IS_BRIDGE}) b`
    const fresh = found.filter(r => !seen.has(r.id))
    if (!fresh.length) break
    for (const r of fresh) seen.add(r.id)
    rows.push(...fresh)
  }
  return rows
}

async function inputFor(rows: Row[]): Promise<DeckInput> {
  const ways = rows.map(toWay)
  const xs = rows.flatMap(r => r.coords.map(c => c[0]))
  const ys = rows.flatMap(r => r.coords.map(c => c[1]))
  const [w, s, e, n] = [Math.min(...xs) - 0.001, Math.min(...ys) - 0.001, Math.max(...xs) + 0.001, Math.max(...ys) + 0.001]
  const box = sql`ST_MakeEnvelope(${w}, ${s}, ${e}, ${n}, 4326)`

  const ends = [...new Map(rows.flatMap(r => [r.coords[0], r.coords[r.coords.length - 1]]).map(c => [c.join(','), c])).values()]
  const landed: Array<{ i: number }> = await sql`
    SELECT o.i FROM unnest(${ends.map(c => c[0])}::float8[], ${ends.map(c => c[1])}::float8[]) WITH ORDINALITY AS o(lng, lat, i)
    WHERE EXISTS (
      SELECT 1 FROM geo_places g
      WHERE g.geom_type = 'line' AND g.geom && ST_Expand(ST_SetSRID(ST_MakePoint(o.lng, o.lat), 4326), 1e-7)
        AND (g.tags ? 'highway' OR g.tags ? 'railway') AND COALESCE(g.tags->>'bridge', 'no') = 'no'
        AND ST_DWithin(g.geom, ST_SetSRID(ST_MakePoint(o.lng, o.lat), 4326), 1e-7))`
  const onGround = new Set(landed.map(({ i }) => {
    const [lng, lat] = ends[Number(i) - 1]
    return mercator(lng, lat).join(',')
  }))

  const outlineRows: Array<{ id: string; coords: number[][][][] }> = await sql`
    SELECT (CASE osm_type WHEN 'W' THEN 'way/' ELSE 'relation/' END) || osm_id AS id,
           ST_AsGeoJSON(ST_Multi(geom))::json->'coordinates' AS coords
    FROM geo_places WHERE geom_type = 'area' AND geom && ${box} AND tags->>'man_made' = 'bridge'`
  const outlines = outlineRows.flatMap(o => o.coords.map(rings => ({ id: o.id, rings: rings.map(ring => ring.map(([lng, lat]) => mercator(lng, lat))) })))

  const [{ surfaces }] = await sql`SELECT to_regclass('road_surfaces') IS NOT NULL AS surfaces`
  const kerbs: Point[] = surfaces
    ? (await sql`SELECT ST_X((dp).geom) AS lng, ST_Y((dp).geom) AS lat FROM (
        SELECT ST_DumpPoints(geom) AS dp FROM road_surfaces WHERE bridge AND geom && ${box}) d`).map(r => mercator(r.lng, r.lat))
    : []

  const crossedRows: Array<{ kind: Crossed['kind']; coords: number[][][] }> = await sql`
    SELECT CASE WHEN tags ? 'waterway' THEN 'water' WHEN tags ? 'railway' THEN 'rail'
                WHEN tags->>'highway' = ANY(${PATHS}) THEN 'path' ELSE 'road' END AS kind,
           ST_AsGeoJSON(ST_Multi(geom))::json->'coordinates' AS coords
    FROM geo_places
    WHERE geom_type = 'line' AND geom && ${box} AND COALESCE(tags->>'bridge', 'no') = 'no' AND COALESCE(tags->>'tunnel', 'no') = 'no'
      AND COALESCE(tags->>'location', '') NOT IN ('underground', 'underwater')
      AND (tags->>'highway' = ANY(${[...ROADS, ...PATHS]}) OR tags->>'railway' = ANY(${RAILS})
           OR tags->>'waterway' IN ('river', 'stream', 'canal', 'drain', 'ditch'))`
  const crossed = crossedRows.flatMap(r => r.coords.map(line => ({ kind: r.kind, points: line.map(([lng, lat]) => mercator(lng, lat)) })))

  const waterRows: Array<{ coords: number[][][][] }> = await sql`
    SELECT ST_AsGeoJSON(ST_Multi(ST_CollectionExtract(ST_Intersection(geom, ${box}), 3)))::json->'coordinates' AS coords
    FROM geo_places
    WHERE geom_type = 'area' AND geom && ${box}
      AND (tags->>'natural' = 'water' OR tags->>'waterway' = 'riverbank' OR tags->>'landuse' = 'reservoir')`
  const water = waterRows.flatMap(r => (r.coords ?? []).map(polygon => polygon[0].map(([lng, lat]) => mercator(lng, lat))))

  const wikidata = new Map(rows.flatMap(r => (r.wikidata ? [[Number(r.id), r.wikidata] as [number, string]] : [])))
  return { ways, onGround, outlines, kerbs, crossed, water, wikidata }
}

async function cell(dem: Dem, w: number, s: number, e: number, n: number) {
  const rows = await waysAround(w, s, e, n)
  const decks = rows.length ? await buildDecks(await inputFor(rows), dem) : []
  const mine = decks.flatMap(d => {
    const [lng, lat] = lngLat(d.midpoint)
    return lng >= w && lng < e && lat >= s && lat < n ? [{ d, at: [lng, lat] }] : []
  })
  await sql.begin(async rawTx => {
    const tx = rawTx as unknown as typeof sql
    await tx`DELETE FROM bridge_decks WHERE anchor && ST_MakeEnvelope(${w}, ${s}, ${e}, ${n}, 4326)
      AND ST_X(anchor) >= ${w} AND ST_X(anchor) < ${e} AND ST_Y(anchor) >= ${s} AND ST_Y(anchor) < ${n}`
    for (let i = 0; i < mine.length; i += INSERT_BATCH) {
      // One row per deck as JSON, unpacked by jsonb_to_recordset: a multi-row
      // VALUES list cannot carry the arrays, and unnest would flatten them.
      const batch = mine.slice(i, i + INSERT_BATCH).map(({ d, at }) => ({
        id: d.id, bridge: d.bridge, ways: d.ways, kind: d.kind, layer: d.layer, edges: d.edges, grounded: d.grounded,
        length: d.length, heights: d.heights, ground: d.ground, piers: d.piers, lng: at[0], lat: at[1],
        line: `LINESTRING(${d.points.map(p => lngLat(p).join(' ')).join(',')})`,
      }))
      await tx`
        INSERT INTO bridge_decks (id, bridge, ways, kind, layer, edges, grounded, step, length, heights, ground, piers, anchor, geom)
        SELECT r.id, r.bridge, r.ways, r.kind, r.layer, r.edges, r.grounded, ${STEP}, r.length, r.heights, r.ground, r.piers,
               ST_SetSRID(ST_MakePoint(r.lng, r.lat), 4326), ST_GeomFromText(r.line, 4326)
        FROM jsonb_to_recordset(${tx.json(batch)}::jsonb) AS r(id text, bridge text, ways bigint[], kind text, layer int,
          edges real[], grounded boolean[], length real, heights real[], ground real[], piers real[], lng float8, lat float8, line text)
        ON CONFLICT (id) DO UPDATE SET bridge = EXCLUDED.bridge, ways = EXCLUDED.ways, kind = EXCLUDED.kind, layer = EXCLUDED.layer,
          edges = EXCLUDED.edges, grounded = EXCLUDED.grounded, step = EXCLUDED.step, length = EXCLUDED.length,
          heights = EXCLUDED.heights, ground = EXCLUDED.ground, piers = EXCLUDED.piers, anchor = EXCLUDED.anchor,
          geom = EXCLUDED.geom, updated_at = now()`
    }
  })
  return mine.length
}

async function areas() {
  const bbox = flag('bbox')
  return bbox ? [parseBbox(bbox)] : regionAreas(await resolveRegions())
}

async function main() {
  const [{ ready }] = await sql`SELECT to_regclass('bridge_decks') IS NOT NULL AS ready`
  if (!ready) throw new Error('bridge_decks does not exist: start the API once, or run import/create-detail-views.sql')
  const cells = cellsCovering(await areas(), CELL)
  const dem = new Dem()
  const started = Date.now()
  let total = 0
  for (const [k, [w, s, e, n]] of cells.entries()) {
    const built = await cell(dem, w, s, e, n)
    total += built
    if (built) console.log(`[${k + 1}/${cells.length}] cell ${w},${s}: ${built} decks (${Math.round((Date.now() - started) / 1000)} s)`)
  }
  console.log(`Bridge decks: ${total} built in ${cells.length} cells, ${Math.round((Date.now() - started) / 1000)} s.`)
  await sql.end()
}

if (import.meta.main) {
  await main()
  // resolveRegions may have opened the shared DB handle (region store); exit
  // rather than hang on an idle connection.
  process.exit(0)
}
