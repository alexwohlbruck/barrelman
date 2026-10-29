/**
 * Rebuilding tunnel_cuts one box at a time, shared by Build Tunnel Cuts
 * (import/generate-tunnel-cuts.ts) and Update Tunnel Cuts
 * (import/update-tunnel-cuts.ts).
 *
 * A box owns the cuts whose portal lies in it. Rebuilding it reads the
 * tunnels around it, and around each tunnel's ends the roads, surfaces,
 * buildings and walls a cut is fitted to; solves every portal; and swaps the
 * box's own cuts in, in one transaction. A cut belongs to one box whatever the
 * grid, so any box works, as often as wanted.
 */
import type postgres from 'postgres'
import type { Dem } from '../src/lib/bridge-decks/dem'
import { contains, toDegrees, toUnits, UNITS_PER_DEGREE, type UnitBox, type UnitPoint } from '../src/lib/bridge-decks/grid'
import { mercator, unprojected, type Bounds, type Point } from '../src/lib/bridge-mesh/decks'
import { groundPoints, openings, portalsOf, solveCuts, type Surroundings, type TunnelCut, type Way } from '../src/lib/tunnel-cuts/build'
import { isWay, toWay, tunnelsIn, wayColumns, type WayRow } from '../src/lib/tunnel-cuts/way-rows'
import { CUT_MAX } from '../src/lib/tunnel-cuts/tunnels'

type Sql = postgres.Sql

/** Held by every run that writes tunnel_cuts, so two never rebuild the same box at once. */
export const LOCK = [5393739, 2] as const

/**
 * Metres read past a box. A portal moves out from its tunnel's end to where
 * the approach leaves the buildings over it, at most a cut's length, and the
 * approach then runs on that far again.
 */
export const READ_PAST = 2 * CUT_MAX + 60
/** Metres read around each tunnel end for what its cut is fitted to. */
const AROUND_END = CUT_MAX + 40

const degreesPer = (metres: number, lat: number): [number, number] => [metres / (111320 * Math.cos((lat * Math.PI) / 180)), metres / 111320]

/** A degree box `metres` wider each side. */
function grown([w, s, e, n]: [number, number, number, number], metres: number): [number, number, number, number] {
  const [dx, dy] = degreesPer(metres, Math.max(Math.abs(s), Math.abs(n)))
  return [w - dx, s - dy, e + dx, n + dy]
}

/** A degree box's mercator bounds. */
function bounds([w, s, e, n]: [number, number, number, number]): Bounds {
  const [minX, maxY] = mercator([w, s])
  const [maxX, minY] = mercator([e, n])
  return { minX, minY, maxX, maxY }
}

/** Degrees a side of the grid the ground around portals is read in, so neighbouring portals share their reads. */
const READ_CELL = 0.005

/** Everything a cut at these portals is fitted to, read in grid cells around each. */
export async function surroundings(sql: Sql, portals: Point[]): Promise<Surroundings> {
  const cells = new Set<string>()
  for (const p of portals) {
    const [lng, lat] = unprojected(p)
    const [w, s, e, n] = grown([lng, lat, lng, lat], AROUND_END)
    for (let x = Math.floor(w / READ_CELL); x <= Math.floor(e / READ_CELL); x++)
      for (let y = Math.floor(s / READ_CELL); y <= Math.floor(n / READ_CELL); y++) cells.add(`${x},${y}`)
  }
  const boxes = [...cells].map(c => c.split(',').map(Number))
  const [ws, ss] = [boxes.map(b => b[0] * READ_CELL), boxes.map(b => b[1] * READ_CELL)]
  const near = sql`unnest(${ws}::float8[], ${ss}::float8[]) AS b(w, s)`
  const hit = sql`g.geom && ST_MakeEnvelope(b.w, b.s, b.w + ${READ_CELL}, b.s + ${READ_CELL}, 4326)`
  const ways: WayRow[] = await sql`
    SELECT DISTINCT ON (g.osm_type, g.osm_id) ${wayColumns(sql)} FROM ${near} JOIN geo_places g ON ${hit} WHERE ${isWay(sql)}`
  const [{ hasSurfaces }] = await sql`SELECT to_regclass('road_surfaces') IS NOT NULL AS "hasSurfaces"`
  const surfaceRows: Array<{ coords: number[][][][] }> = hasSurfaces ? await sql`
    SELECT DISTINCT ON (g.fid) ST_AsGeoJSON(ST_Multi(g.geom))::json->'coordinates' AS coords
    FROM ${near} JOIN road_surfaces g ON ${hit} WHERE g.bridge IS NOT TRUE` : []
  const buildingRows: Array<{ coords: number[][][][] }> = await sql`
    SELECT DISTINCT ON (g.osm_type, g.osm_id) ST_AsGeoJSON(ST_Multi(g.geom))::json->'coordinates' AS coords
    FROM ${near} JOIN geo_places g ON ${hit}
    WHERE g.geom_type = 'area' AND COALESCE(g.tags->>'location', '') <> 'underground'
      AND (COALESCE(g.tags->>'building', 'no') <> 'no' OR COALESCE(g.tags->>'building:part', 'no') <> 'no')
      AND GeometryType(g.geom) IN ('POLYGON', 'MULTIPOLYGON')`
  const wallRows: WayRow[] = await sql`
    SELECT DISTINCT ON (g.osm_type, g.osm_id) ST_AsGeoJSON(g.geom)::json->'coordinates' AS coords
    FROM ${near} JOIN geo_places g ON ${hit}
    WHERE g.geom_type = 'line' AND g.tags->>'barrier' IN ('wall', 'retaining_wall', 'city_wall') AND GeometryType(g.geom) = 'LINESTRING'`
  const project = (ring: number[][]) => ring.map(([lng, lat]) => mercator([lng, lat]))
  return {
    ways: ways.map(toWay).filter((x): x is Way => x !== null && x.brunnel !== 'tunnel'),
    surfaces: surfaceRows.flatMap(r => r.coords.flatMap(polygon => polygon.map(project))),
    buildings: buildingRows.flatMap(r => r.coords.map(polygon => polygon.map(project))),
    walls: wallRows.map(r => project(r.coords)),
  }
}

/** The cuts whose portal lies in one of `boxes`, solved from the tunnels read around them; `tunnels` when already read. */
export async function cutsIn(sql: Sql, dem: Dem, boxes: UnitBox[], tunnels?: Way[]): Promise<Array<TunnelCut & { anchor: UnitPoint }>> {
  const around: [number, number, number, number] = [
    toDegrees(Math.min(...boxes.map(b => b[0]))), toDegrees(Math.min(...boxes.map(b => b[1]))),
    toDegrees(Math.max(...boxes.map(b => b[2]))), toDegrees(Math.max(...boxes.map(b => b[3]))),
  ]
  const read = grown(around, READ_PAST)
  const readBounds = bounds(read)
  const found = (tunnels ?? (await tunnelsIn(sql, read))).filter(t => t.points.some(p => p[0] >= readBounds.minX && p[0] <= readBounds.maxX && p[1] >= readBounds.minY && p[1] <= readBounds.maxY))
  // Only portals that can open into the boxes are read around.
  const reach = bounds(grown(around, 2 * CUT_MAX))
  const ends = portalsOf(found, readBounds).filter(({ line: [p] }) => p[0] >= reach.minX && p[0] <= reach.maxX && p[1] >= reach.minY && p[1] <= reach.maxY)
  if (!ends.length) return []
  const portals = openings(ends, await surroundings(sql, ends.map(p => p.line[0])), readBounds)
  if (!portals.length) return []
  await dem.load(groundPoints(portals))
  const anchor = (p: Point): UnitPoint => {
    const [lng, lat] = unprojected(p)
    return [toUnits(lng), toUnits(lat)]
  }
  return solveCuts(portals, p => dem.at(p), p => boxes.some(b => contains(b, anchor(p))))
    .map(c => ({ ...c, anchor: anchor(c.points[c.at]) }))
}

/** A box's envelope, a hair wider so the index never trims an anchor on its edge. */
const envelope = (sql: Sql, [w, s, e, n]: UnitBox) =>
  sql`ST_MakeEnvelope(${toDegrees(w - 1)}, ${toDegrees(s - 1)}, ${toDegrees(e + 1)}, ${toDegrees(n + 1)}, 4326)`

/** Cuts per INSERT. */
const INSERT_BATCH = 500

/** Rebuilds the cuts anchored in `boxes`; how many it stored. */
export async function rebuildBoxes(sql: Sql, dem: Dem, boxes: UnitBox[], tunnels?: Way[]): Promise<number> {
  const cuts = await cutsIn(sql, dem, boxes, tunnels)
  await sql.begin(async rawTx => {
    const tx = rawTx as unknown as Sql
    for (const [w, s, e, n] of boxes)
      await tx`DELETE FROM tunnel_cuts WHERE anchor && ${envelope(tx, [w, s, e, n])}
        AND anchor_x >= ${w} AND anchor_x < ${e} AND anchor_y >= ${s} AND anchor_y < ${n}`
    for (let i = 0; i < cuts.length; i += INSERT_BATCH) {
      const batch = cuts.slice(i, i + INSERT_BATCH).map(c => ({
        id: c.id, kind: c.kind, at: c.at, open: c.cut.open, edges: c.edges, floor: c.cut.floor, wall_left: c.cut.walls[0], wall_right: c.cut.walls[1],
        crown: c.cut.crown, headroom: c.cut.headroom, base: c.cut.base, face: c.face ?? null, piers: c.piers?.flat() ?? null,
        x: c.anchor[0], y: c.anchor[1], line: `LINESTRING(${c.points.map(p => unprojected(p).join(' ')).join(',')})`,
      }))
      await tx`
        INSERT INTO tunnel_cuts (id, kind, at, open, edges, floor, wall_left, wall_right, crown, headroom, base, face, piers, anchor, anchor_x, anchor_y, geom)
        SELECT r.id, r.kind, r.at, r.open, r.edges, r.floor, r.wall_left, r.wall_right, r.crown, r.headroom, r.base, r.face, r.piers,
               ST_SetSRID(ST_MakePoint(r.x / ${UNITS_PER_DEGREE}::float8, r.y / ${UNITS_PER_DEGREE}::float8), 4326), r.x, r.y, ST_GeomFromText(r.line, 4326)
        FROM jsonb_to_recordset(${tx.json(batch)}::jsonb) AS r(id text, kind text, at int, open int, edges real[], floor real[], wall_left real[],
          wall_right real[], crown real, headroom real, base real, face real[], piers real[], x int, y int, line text)
        ON CONFLICT (id) DO UPDATE SET kind = EXCLUDED.kind, at = EXCLUDED.at, open = EXCLUDED.open, edges = EXCLUDED.edges, floor = EXCLUDED.floor,
          wall_left = EXCLUDED.wall_left, wall_right = EXCLUDED.wall_right, crown = EXCLUDED.crown, headroom = EXCLUDED.headroom, base = EXCLUDED.base,
          face = EXCLUDED.face, piers = EXCLUDED.piers, anchor = EXCLUDED.anchor, anchor_x = EXCLUDED.anchor_x, anchor_y = EXCLUDED.anchor_y,
          geom = EXCLUDED.geom, updated_at = now()`
    }
  })
  return cuts.length
}

/** Whether tunnel_cuts exists yet. */
export async function tableMissing(sql: Sql): Promise<boolean> {
  const [{ found }] = await sql`SELECT to_regclass('tunnel_cuts') IS NOT NULL AS found`
  return !found
}
