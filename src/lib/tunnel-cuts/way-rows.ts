/** Reading highway and railway ways out of geo_places, as the tunnel solvers take them. */
import type postgres from 'postgres'
import { mercator } from '../bridge-mesh/decks'
import { classOf, type Way } from './build'

type Sql = postgres.Sql

/** The values of `tunnel` the basemap draws as one, found by the tag's index rather than the box's. */
const TUNNEL_VALUES = ['yes', 'building_passage', 'covered', 'avalanche_protector']
const truthy = (v: string | null) => v !== null && !['no', 'false', '0'].includes(v)

export type WayRow = { id?: string | number; highway: string | null; railway: string | null; tunnel: string | null; bridge: string | null; ford: string | null; layer: string | null; coords: number[][] }

// A closed way is stored as an area, but unless it is tagged area=yes the basemap draws it as a line, as a roundabout or a loop in a garage.
export const wayColumns = (sql: Sql) => sql`
  g.osm_id AS id, g.tags->>'highway' AS highway, g.tags->>'railway' AS railway, g.tags->>'tunnel' AS tunnel, g.tags->>'bridge' AS bridge,
  g.tags->>'ford' AS ford, g.tags->>'layer' AS layer,
  ST_AsGeoJSON(CASE WHEN g.geom_type = 'area' THEN ST_ExteriorRing(g.geom) ELSE g.geom END)::json->'coordinates' AS coords`

export const isWay = (sql: Sql) => sql`(g.tags ? 'highway' OR g.tags ? 'railway') AND (
  (g.geom_type = 'line' AND GeometryType(g.geom) = 'LINESTRING')
  OR (g.geom_type = 'area' AND GeometryType(g.geom) = 'POLYGON' AND COALESCE(g.tags->>'area', 'no') <> 'yes'))`

export function toWay(r: WayRow): Way | null {
  const cls = classOf(r.highway, r.railway)
  if (!cls) return null
  const brunnel = truthy(r.bridge) ? 'bridge' : truthy(r.tunnel) ? 'tunnel' : r.ford === 'yes' ? 'ford' : null
  const id = r.id === undefined ? undefined : Number(r.id)
  return { ...(id === undefined ? {} : { id }), cls, brunnel, layer: Number.parseInt(r.layer ?? '') || 0, points: r.coords.map(([lng, lat]) => mercator([lng, lat])) }
}

const tunnelTag = (sql: Sql) => TUNNEL_VALUES.map(v => sql`g.tags @> ${sql.json({ tunnel: v })}`).reduce((a, b) => sql`${a} OR ${b}`)

/** Tunnel ways in a box, in degrees, found by their tag's index; with `underground`, only OSM ways on a negative layer. */
export async function tunnelsIn(sql: Sql, [w, s, e, n]: [number, number, number, number], { underground = false } = {}): Promise<Way[]> {
  const rows: WayRow[] = await sql`
    SELECT ${wayColumns(sql)} FROM geo_places g
    WHERE (${tunnelTag(sql)}) AND ${isWay(sql)} AND g.geom && ST_MakeEnvelope(${w}, ${s}, ${e}, ${n}, 4326)
      ${underground ? sql`AND g.osm_type = 'W' AND g.tags->>'layer' LIKE '-%'` : sql``}`
  return rows.map(toWay).filter((x): x is Way => x !== null)
}
