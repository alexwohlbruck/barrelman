import type postgres from 'postgres'
import type { Deck } from '../src/lib/bridge-decks/build'
import type { Dem } from '../src/lib/bridge-decks/dem'
import { lngLat, mercator, type Point } from '../src/lib/bridge-decks/profile'
import { bakeBridges, type MeshFeature } from '../src/lib/bridge-mesh/bake'
import { encodeBridge, MATERIAL_PALETTE } from '../src/lib/bridge-mesh/encode'
import { writeBridgeMesh } from '../src/lib/bridge-mesh/files'

/**
 * Outlines of the bridges a landmark model stands in for, by its `replaces`:
 * the model draws their structure, so their decks drop their own.
 */
export async function modelledBridges(sql: postgres.Sql, decks: Deck[]): Promise<Point[][]> {
  const [{ present }] = await sql`SELECT to_regclass('landmarks') IS NOT NULL AS present`
  if (!present || !decks.length) return []
  const coords = decks.flatMap(d => d.points.map(lngLat))
  const box = sql`ST_MakeEnvelope(${Math.min(...coords.map(c => c[0]))}, ${Math.min(...coords.map(c => c[1]))}, ${Math.max(...coords.map(c => c[0]))}, ${Math.max(...coords.map(c => c[1]))}, 4326)`
  const rows: Array<{ coords: number[][][][] }> = await sql`
    SELECT ST_AsGeoJSON(ST_Multi(g.geom))::json->'coordinates' AS coords FROM geo_places g
    WHERE g.tags @> '{"man_made": "bridge"}' AND g.geom_type = 'area' AND g.geom && ${box}
      AND (CASE g.osm_type WHEN 'W' THEN 'way/' ELSE 'relation/' END) || g.osm_id IN (SELECT unnest(replaces) FROM landmarks WHERE active)`
  return rows.flatMap(r => r.coords.map(polygon => polygon[0].map(([lng, lat]) => mercator(lng, lat))))
}

export async function bakeDeckFiles(sql: postgres.Sql, dem: Dem, decks: Deck[], context: Deck[] = decks): Promise<Map<string, string>> {
  const files = new Map<string, string>()
  const structures = await modelledBridges(sql, decks)
  for (const deck of decks) {
    const coords = deck.points.map(lngLat)
    const w = Math.min(...coords.map(p => p[0])) - 0.001
    const e = Math.max(...coords.map(p => p[0])) + 0.001
    const s = Math.min(...coords.map(p => p[1])) - 0.001
    const n = Math.max(...coords.map(p => p[1])) + 0.001
    const box = sql`ST_MakeEnvelope(${w}, ${s}, ${e}, ${n}, 4326)`
    const [surfaces, paint, roads] = await Promise.all([
      sql<MeshFeature[]>`SELECT json_build_object('bridge', bridge, 'kind', kind) AS properties,
        ST_AsGeoJSON(ST_ClipByBox2D(geom, ${box}))::json AS geometry FROM road_surfaces WHERE geom && ${box} ORDER BY fid`,
      sql<MeshFeature[]>`SELECT json_build_object('bridge', bridge, 'kind', kind, 'pattern', pattern, 'color', color, 'style', style) AS properties,
        ST_AsGeoJSON(ST_ClipByBox2D(geom, ${box}))::json AS geometry FROM road_markings WHERE geom && ${box} ORDER BY fid`,
      sql<Array<{ id: string; tags: Record<string, string>; geometry: any }>>`SELECT osm_id::text AS id, tags,
        ST_AsGeoJSON(ST_ClipByBox2D(geom, ${box}))::json AS geometry FROM geo_places
        WHERE geom && ${box} AND geom_type = 'line' AND (tags ? 'highway' OR tags ? 'railway') ORDER BY osm_type, osm_id`,
    ])
    const points = [...deck.points]
    const nw = mercator(w, n), se = mercator(e, s)
    const scale = 2 ** 15
    for (let x = Math.floor(nw[0] * scale); x <= Math.floor(se[0] * scale); x++)
      for (let y = Math.floor(nw[1] * scale); y <= Math.floor(se[1] * scale); y++) points.push([(x + 0.5) / scale, (y + 0.5) / scale])
    await dem.load(points)
    const ways = roads.map(r => ({ geometry: r.geometry, properties: {
      id: r.id, class: r.tags.railway ? 'rail' : ['footway', 'cycleway', 'path', 'pedestrian', 'steps'].includes(r.tags.highway) ? 'path' : r.tags.highway,
      brunnel: r.tags.bridge && r.tags.bridge !== 'no' ? 'bridge' : r.tags.tunnel && r.tags.tunnel !== 'no' ? 'tunnel' : null,
    } }))
    const mesh = bakeBridges({ decks: context.filter(d => d.points.some(p => p[0] >= nw[0] && p[0] <= se[0] && p[1] >= nw[1] && p[1] <= se[1]) || d.id === deck.id), own: [deck.id], outlines: [], roads: ways, surfaces, paint,
      ground: p => { const h = dem.at(p); return Number.isFinite(h) ? h : null }, structures }, MATERIAL_PALETTE)
    files.set(deck.id, await writeBridgeMesh(encodeBridge(mesh, p => { const h = dem.at(p); return Number.isFinite(h) ? h : null })))
  }
  return files
}
