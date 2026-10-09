/**
 * Runs generate-road-markings.sql over a synthetic junction in a throwaway
 * schema: a divided avenue with a planted median, crossed by a signalled street.
 * Then rebuilds part of it in place, and queues it as a replication diff would.
 *
 * Point it at a scratch database: the queue test uses the osm_replay schema
 * that replicate-extract.sh uses.
 *
 * Run: BARRELMAN_INTEGRATION_TESTS=1 DATABASE_URL=postgresql://barrelman:barrelman@localhost:5434/scratch \
 *      bun test import/generate-road-markings.integration.test.ts
 */
import { afterAll, beforeAll, describe, expect, test } from 'bun:test'
import postgres from 'postgres'
import { readFileSync } from 'fs'
import { join } from 'path'

const DATABASE_URL = process.env.BARRELMAN_INTEGRATION_TESTS ? process.env.DATABASE_URL : undefined
const SCHEMA = 'road_markings_test'

const SB_X = -74.0
const NB_X = -73.9997
const STREET_Y = 40.71

const line = (...pts: [number, number][]) => `LINESTRING(${pts.map(p => p.join(' ')).join(', ')})`
const ways: [number, string, Record<string, string>][] = [
  [1, line([SB_X, 40.711], [SB_X, STREET_Y]), { highway: 'trunk', oneway: 'yes', lanes: '3', 'turn:lanes': 'left|through|through' }],
  [2, line([SB_X, STREET_Y], [SB_X, 40.709]), { highway: 'trunk', oneway: 'yes', lanes: '3' }],
  [3, line([NB_X, 40.709], [NB_X, STREET_Y]), { highway: 'trunk', oneway: 'yes', lanes: '3', 'bus:lanes': '||designated' }],
  [4, line([NB_X, STREET_Y], [NB_X, 40.711]), { highway: 'trunk', oneway: 'yes', lanes: '3', 'bus:lanes': '||designated' }],
  [5, line([-74.001, STREET_Y], [SB_X, STREET_Y]), { highway: 'residential', lanes: '2', 'cycleway:right': 'lane' }],
  [6, line([SB_X, STREET_Y], [NB_X, STREET_Y]), { highway: 'residential', lanes: '2' }],
  [7, line([NB_X, STREET_Y], [-73.999, STREET_Y]), { highway: 'residential', lanes: '2' }],
  [10, line([-74.002, 40.712], [-74.002, 40.714]), { highway: 'tertiary', 'overtaking': 'yes' }],
  // A crossing drawn well past both kerbs, and askew to the street.
  [9, line([-74.0006, 40.70985], [-74.0004, 40.71015]), { highway: 'footway', footway: 'crossing', 'crossing:markings': 'zebra' }],
]

const run = DATABASE_URL ? describe : describe.skip
let sql: postgres.Sql

run('generate-road-markings.sql', () => {
  beforeAll(async () => {
    sql = postgres(DATABASE_URL!, { max: 1, onnotice: () => {} })
    await sql.unsafe(`DROP SCHEMA IF EXISTS ${SCHEMA} CASCADE; CREATE SCHEMA ${SCHEMA}; SET search_path TO ${SCHEMA}, public;
      CREATE TABLE geo_places (id text, osm_id bigint, tags jsonb NOT NULL, geom geometry(Geometry, 4326) NOT NULL, geom_type text NOT NULL);
      -- The script drops these by bare name; without them here the drop would reach public's.
      CREATE TABLE road_surfaces (); CREATE TABLE road_markings (); CREATE TABLE road_glyphs ();`)
    for (const [id, wkt, tags] of ways) {
      await sql`INSERT INTO geo_places VALUES (${'W' + id}, ${id}, ${sql.json(tags)}, ST_GeomFromText(${wkt}, 4326), 'line')`
    }
    await sql`INSERT INTO geo_places VALUES ('N8', 8, ${sql.json({ highway: 'traffic_signals' })}, ST_SetSRID(ST_MakePoint(${SB_X}, ${STREET_Y}), 4326), 'point')`
    await sql.unsafe(readFileSync(join(import.meta.dir, 'generate-road-markings.sql'), 'utf8'))
  }, 60_000)

  afterAll(async () => {
    await sql?.unsafe(`DROP SCHEMA IF EXISTS ${SCHEMA} CASCADE; DROP SCHEMA IF EXISTS osm_replay CASCADE`)
    await sql?.end()
  })

  const covered = async (x: number, y: number) =>
    (await sql`SELECT EXISTS (SELECT 1 FROM road_surfaces WHERE ST_Intersects(geom, ST_SetSRID(ST_MakePoint(${x}, ${y}), 4326))) as hit`)[0].hit

  test('paves each carriageway but leaves the median between them open', async () => {
    expect(await covered(SB_X, 40.7105)).toBe(true)
    expect(await covered(NB_X, 40.7105)).toBe(true)
    expect(await covered((SB_X + NB_X) / 2, 40.7105)).toBe(false)
  })

  test('paves the crossing street through the median', async () => {
    expect(await covered((SB_X + NB_X) / 2, STREET_Y)).toBe(true)
  })

  test('puts a stop line on the signalled approach only', async () => {
    const [{ sb, nb }] = await sql`
      SELECT count(*) FILTER (WHERE ST_Y(ST_Centroid(geom)) > ${STREET_Y + 0.00003} AND abs(ST_X(ST_Centroid(geom)) - ${SB_X}) < 0.0001)::int as sb,
             count(*) FILTER (WHERE ST_Y(ST_Centroid(geom)) < ${STREET_Y - 0.00003} AND abs(ST_X(ST_Centroid(geom)) - ${NB_X}) < 0.0001)::int as nb
      FROM road_markings WHERE kind = 'stop'`
    expect(sb).toBe(1)
    expect(nb).toBe(0)
  })

  test('draws the left-turn arrow in the lane on the driver\'s left', async () => {
    const arrows = await sql`SELECT ST_X(geom) as x FROM road_glyphs WHERE glyph = 'road-arrow-left'`
    expect(arrows.length).toBeGreaterThan(0)
    // Southbound, the driver's left is east of the centreline.
    for (const a of arrows) expect(a.x).toBeGreaterThan(SB_X)
  })

  test('paints the tagged bus lane red at the kerb side', async () => {
    const bus = await sql`SELECT ST_X(ST_Centroid(geom)) as x, color FROM road_markings WHERE kind = 'bus_lane'`
    expect(bus.length).toBeGreaterThan(0)
    // Northbound, the third lane from the left is the eastmost.
    for (const b of bus) {
      expect(b.color).toBe('red')
      expect(b.x).toBeGreaterThan(NB_X)
    }
  })

  test('paints the bike lane green', async () => {
    const [{ n }] = await sql`SELECT count(*)::int as n FROM road_markings WHERE kind = 'bike_lane' AND color = 'green'`
    expect(n).toBeGreaterThan(0)
  })

  test('splits two-way streets with a double yellow unless passing is tagged', async () => {
    const rows = await sql`
      SELECT pattern, color, ST_X(ST_Centroid(geom)) < -74.0015 as passing FROM road_markings WHERE kind = 'centre'`
    expect(rows.filter(r => !r.passing).every(r => r.pattern === 'double' && r.color === 'yellow')).toBe(true)
    expect(rows.filter(r => r.passing).map(r => r.pattern)).toContain('dashed')
  })

  test('dashes lanes long on trunk roads and short on streets', async () => {
    const [{ trunk }] = await sql`SELECT count(*) FILTER (WHERE pattern = 'dashed_long')::int as trunk FROM road_markings WHERE kind = 'lane'`
    expect(trunk).toBeGreaterThan(0)
  })

  test('keeps every crosswalk bar and stop line on the carriageway', async () => {
    const [{ n, outside }] = await sql`
      SELECT count(*)::int as n,
             COALESCE(max(ST_Area(ST_Difference(m.geom, (SELECT ST_Union(geom) FROM road_surfaces))::geography)), 0) as outside
      FROM road_markings m WHERE m.kind IN ('crosswalk', 'stop')`
    expect(n).toBeGreaterThan(1)
    expect(outside).toBeLessThan(0.01)
  })

  test('keeps lane lines out of the crossing carriageway', async () => {
    const [{ n }] = await sql`
      SELECT count(*)::int as n FROM road_markings
      WHERE kind IN ('lane', 'centre') AND ST_DWithin(geom::geography, ST_SetSRID(ST_MakePoint(${SB_X}, ${STREET_Y}), 4326)::geography, 3)`
    expect(n).toBe(0)
  })

  describe('rebuilding one box', () => {
    const BOX = sql => sql`ST_MakeEnvelope(-74.0003, 40.7095, -73.9985, 40.7105, 4326)`
    const footprint = async () => (await sql`
      SELECT ST_Area(ST_Union(geom)::geography) as union_m2, sum(ST_Area(geom::geography)) as sum_m2 FROM road_surfaces`)[0]
    let before: any

    beforeAll(async () => {
      before = await footprint()
      await sql`TRUNCATE _rm_scope`
      await sql`INSERT INTO _rm_scope VALUES (${BOX(sql)})`
      await sql.unsafe(readFileSync(join(import.meta.dir, 'generate-road-markings.sql'), 'utf8'))
    }, 60_000)

    test('leaves the road surface whole, with nothing doubled at the box edge', async () => {
      const after = await footprint()
      expect(Math.abs(after.union_m2 - before.union_m2) / before.union_m2).toBeLessThan(0.01)
      expect(Math.abs(after.sum_m2 - after.union_m2) / after.union_m2).toBeLessThan(0.01)
    })

    test('keeps the paint inside and outside the box', async () => {
      const [{ inside, outside }] = await sql`
        SELECT count(*) FILTER (WHERE ST_CoveredBy(geom, ${BOX(sql)}))::int as inside,
               count(*) FILTER (WHERE NOT ST_Intersects(geom, ${BOX(sql)}))::int as outside
        FROM road_markings`
      expect(inside).toBeGreaterThan(0)
      expect(outside).toBeGreaterThan(0)
    })
  })

  describe('queueing a replication diff', () => {
    test('queues the old and new outlines of touched roads, and the cells they fall in', async () => {
      await sql.unsafe(`DROP SCHEMA IF EXISTS osm_replay CASCADE; CREATE SCHEMA osm_replay;
        CREATE TABLE osm_replay.old_places AS
          SELECT id, 'W'::char(1) as osm_type, osm_id, NULL::text as name, NULL::text[] as categories, ST_Translate(geom, 0.05, 0) as geom, geom_type, NULL::int as admin_level
          FROM geo_places WHERE id = 'W5';
        CREATE TABLE osm_replay.changed AS SELECT id FROM geo_places WHERE id IN ('W5', 'N8');`)
      await sql.unsafe(readFileSync(join(import.meta.dir, 'queue-road-markings.sql'), 'utf8'))
      const [{ n }] = await sql`SELECT count(*)::int as n FROM road_markings_dirty`
      expect(n).toBe(3)

      const cells = await sql.unsafe(readFileSync(join(import.meta.dir, 'road-markings-dirty-cells.sql'), 'utf8')
        .replaceAll(':max_cells', '100').replaceAll(':cell', '0.02'))
      // The old outline lies 0.05° east, where nothing was built.
      expect(cells.length).toBeGreaterThan(0)
      for (const c of cells) expect(c.cx * 0.02).toBeLessThan(-73.98)
    })
  })
})
