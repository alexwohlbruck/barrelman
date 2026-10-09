/**
 * Rebuilding bridge_decks one box at a time, shared by Build Bridge Decks
 * (import/generate-bridge-decks.ts) and Update Bridge Decks
 * (import/update-bridge-decks.ts).
 *
 * A box owns the decks whose anchor, their midpoint, lies in it. Rebuilding it
 * reads every bridge way near it, follows each deck beyond its edges for as
 * long as the deck carries on, builds them all, and swaps the box's own decks
 * in: those anchored there are deleted and those whose midpoint falls there
 * written, in one transaction. Any box works, on any grid, as often as wanted.
 */
import type postgres from 'postgres'
import type { Bbox } from '../src/config/regions'
import { buildDecks, STEP, type Crossed, type Deck, type DeckInput } from '../src/lib/bridge-decks/build'
import type { Dem } from '../src/lib/bridge-decks/dem'
import { clip, contains, degreeBox, toDegrees, toUnits, UNITS_PER_DEGREE, type UnitBox, type UnitPoint } from '../src/lib/bridge-decks/grid'
import type { Built } from '../src/lib/bridge-decks/queue'
import { lngLat, mercator, type Kind, type Point, type Way } from '../src/lib/bridge-decks/profile'

type Sql = postgres.Sql

/** Held by every run that writes bridge_decks, so two never rebuild the same box at once. */
export const LOCK = [5393739, 1] as const

/** Carriageway width when none is tagged, by lanes at a class's lane width. */
const LANE: Record<string, number> = { motorway: 3.6, trunk: 3.6, motorway_link: 3.4, trunk_link: 3.4, primary: 3.4, service: 2.8 }

/** Decks per INSERT: one statement per batch rather than a round trip per deck. */
const INSERT_BATCH = 500

const metres = (value: string | null): number | null => {
  const m = value?.match(/^\s*(-?[0-9]+(?:\.[0-9]+)?)\s*(m|ft|')?\s*$/)
  if (!m) return null
  return Number(m[1]) * (m[2] === 'ft' || m[2] === "'" ? 0.3048 : 1)
}

type Row = { id: string; kind: Kind; highway: string | null; layer: string | null; width: string | null;
  lanes: string | null; oneway: string | null; wikidata: string | null; coords: number[][] }

const bridgeWays = (sql: Sql) => sql`
  SELECT osm_id::text AS id, bridge_deck_class(tags) AS kind, tags->>'highway' AS highway, tags->>'layer' AS layer,
         COALESCE(tags->>'width:carriageway', tags->>'width') AS width, tags->>'lanes' AS lanes, tags->>'oneway' AS oneway,
         COALESCE(tags->>'bridge:wikidata', tags->>'wikidata') AS wikidata,
         ST_AsGeoJSON(geom)::json->'coordinates' AS coords
  FROM geo_places`
// Which ways make decks is bridge_deck_class() in create-detail-views.sql,
// which queue-bridge-decks.sql reads too.
const isBridge = (sql: Sql) => sql`
  osm_type = 'W' AND geom_type = 'line' AND COALESCE(tags->>'bridge', 'no') NOT IN ('no', 'abandoned')
  AND COALESCE(tags->>'tunnel', 'no') = 'no' AND bridge_deck_class(tags) IS NOT NULL
  AND GeometryType(geom) = 'LINESTRING'`

function toWay(r: Row): Way {
  const kind = r.kind
  const lanes = Number.parseInt(r.lanes ?? '') || (r.oneway === 'yes' || r.highway?.startsWith('motorway') ? 1 : 2)
  const tagged = metres(r.width)
  const width = kind === 'rail' ? 5 : kind === 'path' ? (tagged && tagged < 12 ? tagged : 3)
    : tagged && tagged >= 2.5 && tagged <= 60 ? tagged : lanes * (LANE[r.highway!] ?? 3.3)
  return { id: Number(r.id), points: r.coords.map(([lng, lat]) => mercator(lng, lat)), kind, layer: Math.max(1, Number.parseInt(r.layer ?? '') || 1), width }
}

/** Bridge ways around a box, followed beyond it for as long as a deck carries on. */
async function waysAround(sql: Sql, [w, s, e, n]: Bbox): Promise<Row[]> {
  const margin = 0.01
  const rows: Row[] = await sql`${bridgeWays(sql)} WHERE geom && ST_MakeEnvelope(${w - margin}, ${s - margin}, ${e + margin}, ${n + margin}, 4326) AND ${isBridge(sql)}`
  const seen = new Set(rows.map(r => r.id))
  for (let round = 0; round < 100; round++) {
    const degree = new Map<string, number>()
    for (const r of rows) for (const c of [r.coords[0], r.coords[r.coords.length - 1]]) degree.set(c.join(','), (degree.get(c.join(',')) ?? 0) + 1)
    const open = [...degree].filter(([, d]) => d === 1).map(([k]) => k.split(',').map(Number))
    if (!open.length) break
    const found: Row[] = await sql`
      SELECT DISTINCT ON (b.id) b.* FROM unnest(${open.map(c => c[0])}::float8[], ${open.map(c => c[1])}::float8[]) AS o(lng, lat)
      CROSS JOIN LATERAL (${bridgeWays(sql)}
        WHERE geom && ST_Expand(ST_SetSRID(ST_MakePoint(o.lng, o.lat), 4326), 1e-7) AND ${isBridge(sql)}) b`
    const fresh = found.filter(r => !seen.has(r.id))
    if (!fresh.length) break
    for (const r of fresh) seen.add(r.id)
    rows.push(...fresh)
  }
  return rows
}

async function inputFor(sql: Sql, rows: Row[]): Promise<DeckInput> {
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
    SELECT CASE WHEN tags ? 'waterway' THEN 'water' ELSE bridge_deck_class(tags) END AS kind,
           ST_AsGeoJSON(ST_Multi(geom))::json->'coordinates' AS coords
    FROM geo_places
    WHERE geom_type = 'line' AND geom && ${box} AND COALESCE(tags->>'bridge', 'no') = 'no' AND COALESCE(tags->>'tunnel', 'no') = 'no'
      AND COALESCE(tags->>'location', '') NOT IN ('underground', 'underwater')
      AND (bridge_deck_class(tags) IS NOT NULL OR tags->>'waterway' IN ('river', 'stream', 'canal', 'drain', 'ditch'))`
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

/**
 * A rebuilt deck whose id another box's deck holds. Nothing was written; the
 * boxes can be rebuilt together with the ones holding `anchors`.
 */
export class DeckConflict extends Error {
  constructor(readonly ids: string[], readonly anchors: UnitPoint[]) {
    super(`deck id(s) ${ids.join(', ')} already belong to a deck anchored elsewhere`)
  }
}

/** The tables a deck build writes, by name, that do not exist yet. */
export async function missingTables(sql: Sql): Promise<string[]> {
  const [row] = await sql`SELECT to_regclass('bridge_decks') IS NOT NULL AS decks, to_regclass('bridge_deck_cells') IS NOT NULL AS cells`
  return [...(row.decks ? [] : ['bridge_decks']), ...(row.cells ? [] : ['bridge_deck_cells'])]
}

/** A box's envelope, a hair wider so the index never trims an anchor on its edge. */
const envelope = (sql: Sql, [w, s, e, n]: UnitBox) =>
  sql`ST_MakeEnvelope(${toDegrees(w - 1)}, ${toDegrees(s - 1)}, ${toDegrees(e + 1)}, ${toDegrees(n + 1)}, 4326)`

/**
 * Rebuilds the decks anchored in `boxes`, read in one go: neighbouring boxes
 * share most of what they read, so building them together is cheaper than one
 * at a time. `record` lists build cells to mark as covered in the same
 * transaction. Returns how many decks it stored, every deck the build
 * produced, its neighbours' included, and where the decks it deleted but did
 * not write back now lie, so a caller can tell which other boxes a changed
 * deck now belongs to.
 */
export async function rebuildBoxes(sql: Sql, dem: Dem, boxes: UnitBox[], record: UnitBox[] = []): Promise<{ stored: number; built: Built[]; moved: UnitPoint[] }> {
  const around: Bbox = degreeBox([Math.min(...boxes.map(b => b[0])), Math.min(...boxes.map(b => b[1])),
    Math.max(...boxes.map(b => b[2])), Math.max(...boxes.map(b => b[3]))])
  const rows = await waysAround(sql, around)
  const decks: Deck[] = rows.length ? await buildDecks(await inputFor(sql, rows), dem) : []
  const placed = decks.map(d => {
    const lngLats = d.points.map(p => lngLat(p) as [number, number])
    const xs = lngLats.map(p => p[0])
    const ys = lngLats.map(p => p[1])
    const [lng, lat] = lngLat(d.midpoint)
    return { d, at: [toUnits(lng), toUnits(lat)] as UnitPoint, lngLats, box: [Math.min(...xs), Math.min(...ys), Math.max(...xs), Math.max(...ys)] as Bbox }
  })
  const mine = placed.filter(p => boxes.some(b => contains(b, p.at)))
  const deleted = new Set<string>()
  await sql.begin(async rawTx => {
    const tx = rawTx as unknown as Sql
    for (const box of boxes) {
      const [w, s, e, n] = box
      const gone: Array<{ id: string }> = await tx`DELETE FROM bridge_decks WHERE anchor && ${envelope(tx, box)}
        AND anchor_x >= ${w} AND anchor_x < ${e} AND anchor_y >= ${s} AND anchor_y < ${n}
        RETURNING id`
      for (const { id } of gone) deleted.add(id)
    }
    for (let i = 0; i < mine.length; i += INSERT_BATCH) {
      // One row per deck as JSON, unpacked by jsonb_to_recordset: a multi-row
      // VALUES list cannot carry the arrays, and unnest would flatten them.
      const batch = mine.slice(i, i + INSERT_BATCH).map(({ d, at, lngLats }) => ({
        id: d.id, bridge: d.bridge, ways: d.ways, kind: d.kind, layer: d.layer, edges: d.edges, grounded: d.grounded,
        length: d.length, heights: d.heights, ground: d.ground, piers: d.piers, x: at[0], y: at[1],
        line: `LINESTRING(${lngLats.map(p => p.join(' ')).join(',')})`,
      }))
      // The boxes' own decks are gone by now, so a clash is with a deck
      // another box owns, under the same id: refuse rather than take it over.
      const stored: Array<{ id: string }> = await tx`
        INSERT INTO bridge_decks (id, bridge, ways, kind, layer, edges, grounded, step, length, heights, ground, piers, anchor, anchor_x, anchor_y, geom)
        SELECT r.id, r.bridge, r.ways, r.kind, r.layer, r.edges, r.grounded, ${STEP}, r.length, r.heights, r.ground, r.piers,
               ST_SetSRID(ST_MakePoint(r.x / ${UNITS_PER_DEGREE}::float8, r.y / ${UNITS_PER_DEGREE}::float8), 4326), r.x, r.y,
               ST_GeomFromText(r.line, 4326)
        FROM jsonb_to_recordset(${tx.json(batch)}::jsonb) AS r(id text, bridge text, ways bigint[], kind text, layer int,
          edges real[], grounded boolean[], length real, heights real[], ground real[], piers real[], x int, y int, line text)
        ON CONFLICT (id) DO NOTHING
        RETURNING id`
      if (stored.length < batch.length) {
        const kept = new Set(stored.map(r => r.id))
        const ids = batch.map(r => r.id).filter(id => !kept.has(id))
        const held: Array<{ x: number; y: number }> = await tx`SELECT anchor_x AS x, anchor_y AS y FROM bridge_decks WHERE id = ANY(${ids})`
        throw new DeckConflict(ids, held.map(h => [h.x, h.y]))
      }
    }
    for (const [w, s, e, n] of record)
      await tx`INSERT INTO bridge_deck_cells (cell, w, s, e, n, box)
        VALUES (${[w, s, e, n].join(',')}, ${w}, ${s}, ${e}, ${n},
                ST_MakeEnvelope(${toDegrees(w)}, ${toDegrees(s)}, ${toDegrees(e)}, ${toDegrees(n)}, 4326))
        ON CONFLICT (cell) DO NOTHING`
  })
  const kept = new Set(mine.map(p => p.d.id))
  return {
    stored: mine.length,
    built: placed.map(p => ({ id: p.d.id, anchor: p.at, box: p.box })),
    moved: placed.filter(p => deleted.has(p.d.id) && !kept.has(p.d.id)).map(p => p.at),
  }
}

/** The recorded build cells a box overlaps, clipped to it: where an update may write. */
export async function coveredParts(sql: Sql, box: UnitBox): Promise<UnitBox[]> {
  const rows: Array<{ w: number; s: number; e: number; n: number }> = await sql`
    SELECT w, s, e, n FROM bridge_deck_cells WHERE box && ${envelope(sql, box)}`
  return rows.map(r => clip(box, [r.w, r.s, r.e, r.n])).filter((b): b is UnitBox => b !== null)
}

/** Rounds of taking in the cells that hold a clashing id before giving up. */
const CLASH_ROUNDS = 4

/**
 * Rebuilds cells by key, and when a rebuilt deck's id is held by a deck in
 * another cell, rebuilds again with that cell taken in, in one transaction.
 * The holding cell's copy is deleted as this one is written, so two cells
 * holding each other's ids resolve at once instead of each waiting on the
 * other. `partsOf` says where in a cell may be written; `record` which cells
 * to mark as built. Returns the keys finally rebuilt, which include any taken
 * in.
 */
export async function rebuildCells(
  sql: Sql,
  dem: Dem,
  keys: string[],
  cell: {
    partsOf: (key: string) => UnitBox[] | Promise<UnitBox[]>
    keyOf: (anchor: UnitPoint) => string
    record?: (key: string) => boolean
  },
): Promise<{ stored: number; built: Built[]; moved: UnitPoint[]; keys: string[] }> {
  const taken = [...new Set(keys)]
  const parts = new Map<string, UnitBox[]>()
  for (let round = 0; ; round++) {
    for (const k of taken) if (!parts.has(k)) parts.set(k, await cell.partsOf(k))
    const boxes = taken.flatMap(k => parts.get(k)!)
    if (!boxes.length) return { stored: 0, built: [], moved: [], keys: taken }
    const record = cell.record ? taken.filter(cell.record).flatMap(k => parts.get(k)!) : []
    try {
      const result = await rebuildBoxes(sql, dem, boxes, record)
      return { ...result, keys: taken }
    } catch (err) {
      if (!(err instanceof DeckConflict) || round + 1 >= CLASH_ROUNDS) throw err
      const holders = [...new Set(err.anchors.map(cell.keyOf))].filter(k => !taken.includes(k))
      if (!holders.length) throw err
      taken.push(...holders)
    }
  }
}
