/**
 * Street names for a route's stretches along unnamed sidewalks, read from
 * sidewalk_street_names (import/generate-sidewalk-street-names.ts) by the ways
 * GraphHopper says the route follows. One indexed lookup per route, by way id;
 * without the table, or the graph's osm_way_id, a route simply has none.
 */
import { sql } from 'drizzle-orm'
import { executeWithin } from '../lib/bounded-query'
import { streetStretches, type StreetStretch, type WayNames } from '../lib/sidewalk-names/stretches'
import type { StoredZone } from '../lib/sidewalk-names/ways'
import type { XY } from '../lib/sidewalk-names/zones'

const BUDGET_MS = 800

export type WayLookup = (ids: number[]) => Promise<Map<number, WayNames>>

export const lookupWays: WayLookup = async ids => {
  if (!ids.length) return new Map()
  const rows: Array<{ way_id: string | number; coords: XY[]; zones: StoredZone[] }> = await executeWithin(sql`
    SELECT way_id, ST_AsGeoJSON(geom)::json->'coordinates' AS coords, zones
    FROM sidewalk_street_names WHERE way_id = ANY(${`{${ids.join(',')}}`}::bigint[])`, BUDGET_MS)
  return new Map(rows.map(r => [Number(r.way_id), { coords: r.coords, zones: r.zones }]))
}

/** The named sidewalk stretches of one GraphHopper path. */
export async function pathStreetStretches(
  coordinates: number[][],
  distances: number[],
  details: Record<string, unknown[]> | undefined,
  lookup: WayLookup = lookupWays,
): Promise<StreetStretch[]> {
  const wayIds = (details?.osm_way_id ?? []).filter(
    (e): e is [number, number, number] => Array.isArray(e) && typeof e[2] === 'number',
  )
  if (!wayIds.length) return []
  const ways = await lookup([...new Set(wayIds.map(e => e[2]))])
  return ways.size ? streetStretches(coordinates, distances, wayIds, ways) : []
}
