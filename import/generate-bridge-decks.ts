/**
 * Build bridge_decks: every bridge way joined into decks, fitted to its
 * outline, and given a height profile from Mapterhorn terrain.
 *
 *   bun run import/generate-bridge-decks.ts                        every enabled region
 *   bun run import/generate-bridge-decks.ts --bbox w,s,e,n         one area
 *
 * The area is worked through in cells (`--cell`, degrees). A cell rebuilds the
 * decks anchored in it — the start of a deck's lowest way — and reads beyond
 * its edges as far as its decks run, so cells can be rebuilt one at a time and
 * in any order. Terrain tiles are fetched as needed and cached for the run.
 */
import postgres from 'postgres'
import { dbUrl, onnotice } from '../src/db'
import { buildDecks, STEP, type DeckInput } from '../src/lib/bridge-decks/build'
import { Dem } from '../src/lib/bridge-decks/dem'
import { toEgm96 } from '../src/lib/bridge-decks/geoid'
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
const flag = (name: string) => {
  const i = args.indexOf(`--${name}`)
  return i >= 0 ? args[i + 1] : undefined
}
const CELL = Number(flag('cell') ?? 0.25)

const sql = postgres(dbUrl, { onnotice, max: 2 })

type Row = { id: string; highway: string | null; railway: string | null; layer: string | null; width: string | null;
  lanes: string | null; oneway: string | null; ele: string | null; coords: number[][] }

const BRIDGE_WAYS = sql`
  SELECT osm_id::text AS id, tags->>'highway' AS highway, tags->>'railway' AS railway, tags->>'layer' AS layer,
         COALESCE(tags->>'width:carriageway', tags->>'width') AS width, tags->>'lanes' AS lanes, tags->>'oneway' AS oneway,
         COALESCE(tags->>'ele:road', tags->>'ele:surface', tags->>'ele') AS ele,
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

  const nodeEle: Array<{ way: string; lng: number; lat: number; ele: string }> = await sql`
    SELECT w.osm_id::text AS way, ST_X(p.geom) AS lng, ST_Y(p.geom) AS lat, p.tags->>'ele' AS ele
    FROM geo_places p JOIN geo_places w ON w.osm_type = 'W' AND w.osm_id = ANY(${rows.map(r => r.id)}::bigint[])
      AND w.geom && ST_Expand(p.geom, 1e-6) AND ST_DWithin(w.geom, p.geom, 1e-6)
    WHERE p.geom_type = 'point' AND p.geom && ${box} AND p.tags ? 'ele'`
  const anchors = [
    ...nodeEle.flatMap(a => {
      const ele = metres(a.ele)
      return ele === null ? [] : [{ way: Number(a.way), point: mercator(a.lng, a.lat), ele }]
    }),
    ...rows.flatMap(r => {
      const ele = metres(r.ele)
      return ele === null ? [] : [{ way: Number(r.id), ele }]
    }),
  ]
  return { ways, onGround, outlines, kerbs, anchors }
}

async function cell(dem: Dem, w: number, s: number, e: number, n: number) {
  const rows = await waysAround(w, s, e, n)
  const owned = (p: number[]) => p[0] >= w && p[0] < e && p[1] >= s && p[1] < n
  const decks = rows.length ? await buildDecks(await inputFor(rows), dem, toEgm96) : []
  const start = new Map(rows.map(r => [r.id, r.coords[0]]))
  const mine = decks.flatMap(d => {
    const at = start.get(d.id.slice(4))!
    return owned(at) ? [{ d, at }] : []
  })
  await sql.begin(async rawTx => {
    const tx = rawTx as unknown as typeof sql
    await tx`DELETE FROM bridge_decks WHERE anchor && ST_MakeEnvelope(${w}, ${s}, ${e}, ${n}, 4326)
      AND ST_X(anchor) >= ${w} AND ST_X(anchor) < ${e} AND ST_Y(anchor) >= ${s} AND ST_Y(anchor) < ${n}`
    for (const { d, at } of mine) {
      const line = `LINESTRING(${d.points.map(p => lngLat(p).join(' ')).join(',')})`
      await tx`
        INSERT INTO bridge_decks (id, ways, kind, layer, outline, edges, grounded, step, length, heights, ground, anchor, geom)
        VALUES (${d.id}, ${sql.array(d.ways)}::bigint[], ${d.kind}, ${d.layer}, ${d.outline}, ${sql.array(d.edges)}::real[],
                ${sql.array(d.grounded)}::boolean[], ${STEP}, ${d.length}, ${sql.array(d.heights)}::real[], ${sql.array(d.ground)}::real[], ST_SetSRID(ST_MakePoint(${at[0]}, ${at[1]}), 4326), ST_GeomFromText(${line}, 4326))
        ON CONFLICT (id) DO UPDATE SET ways = EXCLUDED.ways, kind = EXCLUDED.kind, layer = EXCLUDED.layer,
          outline = EXCLUDED.outline, edges = EXCLUDED.edges, grounded = EXCLUDED.grounded, step = EXCLUDED.step,
          length = EXCLUDED.length, heights = EXCLUDED.heights, ground = EXCLUDED.ground, anchor = EXCLUDED.anchor,
          geom = EXCLUDED.geom, updated_at = now()`
    }
  })
  return mine.length
}

async function areas(): Promise<number[][]> {
  const bbox = flag('bbox')
  if (bbox) return [bbox.split(',').map(Number)]
  const regions: Array<{ bbox: number[] }> = await sql`SELECT bbox FROM import_regions WHERE enabled AND NOT is_global ORDER BY sort_order`
  return regions.map(r => r.bbox)
}

async function main() {
  const [{ ready }] = await sql`SELECT to_regclass('bridge_decks') IS NOT NULL AS ready`
  if (!ready) throw new Error('bridge_decks does not exist: start the API once, or run import/create-detail-views.sql')
  const dem = new Dem()
  const cells = new Set<string>()
  for (const [w, s, e, n] of await areas())
    for (let x = Math.floor(w / CELL); x * CELL < e; x++) for (let y = Math.floor(s / CELL); y * CELL < n; y++) cells.add(`${x},${y}`)
  const started = Date.now()
  let total = 0
  let k = 0
  for (const c of cells) {
    const [x, y] = c.split(',').map(Number)
    const built = await cell(dem, x * CELL, y * CELL, (x + 1) * CELL, (y + 1) * CELL)
    total += built
    k++
    if (built) console.log(`[${k}/${cells.size}] cell ${x},${y}: ${built} decks (${Math.round((Date.now() - started) / 1000)} s)`)
  }
  console.log(`Bridge decks: ${total} built in ${cells.size} cells, ${Math.round((Date.now() - started) / 1000)} s.`)
  await sql.end()
}

if (import.meta.main) await main()
