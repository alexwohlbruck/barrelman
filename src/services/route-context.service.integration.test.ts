/**
 * The route context's queries against synthetic states and ways in a
 * throwaway schema: states read from state_parts land where the whole
 * polygons put them, and each structure is found once across the pieces it
 * spans.
 *
 * Run: BARRELMAN_INTEGRATION_TESTS=1 DATABASE_URL=postgresql://barrelman:barrelman@localhost:5434/barrelman \
 *      bun test src/services/route-context.service.integration.test.ts
 */
import { afterAll, beforeAll, describe, expect, test } from 'bun:test'
import postgres from 'postgres'
import { drizzle } from 'drizzle-orm/postgres-js'
import { readFileSync } from 'fs'
import { join } from 'path'
import { getRouteContext, routeContextSource } from './route-context.service'
import { haversine, routePath, routePieces, toleranceDegrees, type LonLat } from '../lib/route-hazards/route-path'

const DATABASE_URL = process.env.BARRELMAN_INTEGRATION_TESTS ? process.env.DATABASE_URL : undefined
const SCHEMA = 'route_context_test'
const run = DATABASE_URL ? describe : describe.skip

/** A border of 2001 vertices zigzagging 0.001° either side of -80.9, from 34°N to 36°N. */
const border: LonLat[] = Array.from({ length: 2001 }, (_, k) => [-80.9 + (k % 2) * 0.001, 34 + k * 0.001])
const polygon = (outerLon: number) => {
  const ring = [[outerLon, 34], ...border, [outerLon, 36], [outerLon, 34]]
  return `POLYGON((${ring.map(p => p.join(' ')).join(', ')}))`
}
const line = (points: LonLat[]) => `LINESTRING(${points.map(p => p.join(' ')).join(', ')})`

function input(coordinates: LonLat[]) {
  const distances = [0]
  for (let i = 1; i < coordinates.length; i++) distances.push(distances[i - 1] + haversine(coordinates[i - 1], coordinates[i]))
  return { coordinates, distances, profile: 'car' }
}

/** North along -80.85, a vertex every 0.01°, wholly inside the eastern state and over many of its parts. */
const northward: LonLat[] = Array.from({ length: 161 }, (_, i) => [-80.85, 34.2 + i * 0.01])

let client: postgres.Sql
let source: ReturnType<typeof routeContextSource>

run('route context queries', () => {
  beforeAll(async () => {
    const admin = postgres(DATABASE_URL!, { max: 1, onnotice: () => {} })
    await admin.unsafe(`DROP SCHEMA IF EXISTS ${SCHEMA} CASCADE; CREATE SCHEMA ${SCHEMA}`)
    await admin.end()
    client = postgres(DATABASE_URL!, { max: 1, onnotice: () => {}, connection: { search_path: `${SCHEMA}, public` } })
    await client.unsafe(`
      CREATE TABLE geo_places (id text, osm_type char(1), osm_id bigint, name text, tags jsonb, geom geometry(Geometry, 4326), geom_type text, admin_level int);
      INSERT INTO geo_places (id, osm_type, osm_id, name, tags, geom, geom_type, admin_level) VALUES
        ('relation/1', 'R', 1, 'Westland', '{"ISO3166-2": "XX-W"}', ST_GeomFromText('${polygon(-82)}', 4326), 'area', 4),
        ('relation/2', 'R', 2, 'Eastland', '{"ISO3166-2": "XX-E"}', ST_GeomFromText('${polygon(-79)}', 4326), 'area', 4),
        ('relation/3', 'R', 3, 'Not a state', '{}', ST_GeomFromText('${polygon(-79)}', 4326), 'area', 6),
        ('way/10', 'W', 10, 'Crossing Bridge', '{"highway": "primary", "bridge": "yes", "layer": "1"}', ST_GeomFromText('${line([[-80.86, 35.0005], [-80.84, 35.0005]])}', 4326), 'line', NULL),
        ('way/11', 'W', 11, 'Viaduct', '{"highway": "motorway", "bridge": "viaduct", "layer": "1"}', ST_GeomFromText('${line([[-80.85, 35.105], [-80.85, 35.195]])}', 4326), 'line', NULL),
        ('way/20', 'W', 20, 'Broadway', '{"highway": "secondary", "cycleway:right": "lane", "surface": "asphalt"}', ST_GeomFromText('${line([[-80.5, 34.499], [-80.5, 34.501]])}', 4326), 'line', NULL),
        ('way/21', 'W', 21, 'Main Street', '{"highway": "residential"}', ST_GeomFromText('${line([[-80.501, 34.5], [-80.499, 34.5]])}', 4326), 'line', NULL),
        ('way/22', 'W', 22, NULL, '{"highway": "footway", "tunnel": "building_passage", "level": "1"}', ST_GeomFromText('${line([[-80.6, 34.5], [-80.6, 34.501]])}', 4326), 'line', NULL),
        ('node/30', 'N', 30, '125 St', '{"railway": "subway_entrance", "ref": "B", "wheelchair": "no"}', ST_SetSRID(ST_MakePoint(-80.6, 34.5005), 4326), 'point', NULL),
        ('node/31', 'N', 31, 'Not on the route', '{"railway": "subway_entrance"}', ST_SetSRID(ST_MakePoint(-80.5999, 34.5005), 4326), 'point', NULL),
        ('node/32', 'N', 32, 'Cafe', '{"amenity": "cafe"}', ST_SetSRID(ST_MakePoint(-80.6, 34.5006), 4326), 'point', NULL);
    `)
    await client.unsafe(readFileSync(join(import.meta.dir, '../../import/build-state-parts.sql'), 'utf8'))
    const db = drizzle(client)
    source = routeContextSource(query => db.execute(query).then(rows => Array.from(rows)))
  })

  afterAll(async () => {
    await client?.unsafe(`DROP SCHEMA IF EXISTS ${SCHEMA} CASCADE`)
    await client?.end()
  })

  test('cuts each state into parts of at most 256 vertices', async () => {
    const [parts] = await client`SELECT count(*)::int AS n, max(ST_NPoints(geom))::int AS most, count(DISTINCT id)::int AS states FROM state_parts`
    expect(parts.states).toBe(2)
    expect(parts.n).toBeGreaterThan(8)
    expect(parts.most).toBeLessThanOrEqual(256)
  })

  test('a route crossing the border changes state where it crosses', async () => {
    const start: LonLat = [-81.2, 35.0005]
    const { states } = await getRouteContext(input([start, [-80.6, 35.0005]]), source)
    // At 35.0005°N the zigzag runs from -80.9 to -80.899, so it is crossed at -80.8995.
    const crossing = haversine(start, [-80.8995, 35.0005])
    expect(states.map(s => s.code)).toEqual(['XX-W', 'XX-E'])
    expect(states[0].startDistance).toBe(0)
    expect(Math.abs(states[0].endDistance - crossing)).toBeLessThan(1)
    expect(Math.abs(states[1].startDistance - crossing)).toBeLessThan(1)
    expect(states[1].endDistance).toBeCloseTo(haversine(start, [-80.6, 35.0005]), 0)
  })

  test('a route inside one state is one stretch, across every part it passes', async () => {
    const route = input(northward)
    const { states } = await getRouteContext(route, source)
    expect(states).toEqual([{ startDistance: 0, endDistance: Math.round(route.distances.at(-1)! * 10) / 10, id: 'relation/2', name: 'Eastland', code: 'XX-E' }])
  })

  test('each structure once, from the first piece it meets to the last', async () => {
    const structures = await source.structures(routePieces(routePath(northward), 150))
    const byId = new Map(structures.map(s => [s.osmId, s]))
    expect(structures).toHaveLength(2)
    expect(byId.get(10)).toMatchObject({ firstSegment: 80, lastSegment: 80 })
    expect(byId.get(11)).toMatchObject({ firstSegment: 90, lastSegment: 99 })
  })

  test('each road a crossing cuts carries its bearing there', async () => {
    const diagonal: LonLat[] = [[-80.5002, 34.4998], [-80.4998, 34.5002]]
    const roads = await source.crossedRoads([{ from: 0, to: 1, line: diagonal, startDistance: 0, endDistance: 57 }])
    const byName = new Map(roads.map(r => [r.name, r.bearing!]))
    expect(Math.min(byName.get('Broadway')!, 360 - byName.get('Broadway')!)).toBeLessThan(1)
    expect(byName.get('Main Street')).toBeCloseTo(90, 0)
  })

  test('edge ways come with only the tags guidance reads, and a line only where a bike lane needs a direction', async () => {
    const ways = await source.edgeWays([20, 21, 22])
    expect(ways.get(20)).toEqual({ tags: { highway: 'secondary', 'cycleway:right': 'lane' }, coordinates: [[-80.5, 34.499], [-80.5, 34.501]] })
    expect(ways.get(22)).toEqual({ tags: { highway: 'footway', tunnel: 'building_passage', level: '1' } })
  })

  test('edge nodes are the tagged ones the route runs through', async () => {
    const along = routePath([[-80.6, 34.5], [-80.6, 34.501]])
    const nodes = await source.edgeNodes(routePieces(along, 150), toleranceDegrees(1.5, 34.5))
    expect(nodes).toEqual([{ tags: { railway: 'subway_entrance', name: '125 St', ref: 'B' }, coordinate: [-80.6, 34.5005] }])
  })
})
