/** Which ways get a street name, and which streets they may take it from. */

export interface StoredZone {
  from: number
  to: number
  name: string
  /** `tag`: the way's own is_sidepath:of:name or street:name; `geometry`: the street found beside it. */
  source: 'tag' | 'geometry'
}

export interface LevelTags {
  layer?: string | null
  bridge?: string | null
  tunnel?: string | null
}

const isSet = (v: string | null | undefined) => v != null && v !== '' && !['no', 'false', '0'].includes(v)

/**
 * The level a way is on, so a sidewalk never takes the name of a road passing
 * over or under it. A bridge or tunnel missing its layer is taken to be one up
 * or down, as renderers do.
 */
export function levelOf({ layer, bridge, tunnel }: LevelTags): number {
  const tagged = Number.parseInt(layer ?? '')
  if (Number.isFinite(tagged)) return tagged
  if (isSet(bridge)) return 1
  if (isSet(tunnel) && tunnel !== 'building_passage') return -1
  return 0
}

/** The street a way names itself the sidewalk of. */
export function taggedStreet(tags: { sidepathOf?: string | null; streetName?: string | null }): string | null {
  return tags.sidepathOf?.trim() || tags.streetName?.trim() || null
}

/**
 * Pedestrian ways that can be named: unnamed footways, paths and cycleways,
 * not crossings or islands. A closed one is stored as an area unless tagged
 * area=yes, a plaza.
 */
export const PEDESTRIAN_PREDICATE = `g.tags->>'highway' IN ('footway', 'path', 'cycleway')
  AND COALESCE(g.tags->>'name', '') = ''
  AND COALESCE(g.tags->>'footway', '') NOT IN ('crossing', 'traffic_island', 'access_aisle')
  AND COALESCE(g.tags->>'path', '') <> 'crossing'
  AND COALESCE(g.tags->>'cycleway', '') <> 'crossing'
  AND ((g.geom_type = 'line' AND GeometryType(g.geom) = 'LINESTRING')
       OR (g.geom_type = 'area' AND GeometryType(g.geom) = 'POLYGON' AND COALESCE(g.tags->>'area', 'no') = 'no'))`
