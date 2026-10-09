/**
 * Runs generate-road-markings.sql over a synthetic junction in a throwaway
 * schema: a divided avenue with a planted median, crossed by a signalled street.
 * Then rebuilds part of it in place, queues it as a replication diff would and
 * plans the rebuild, and builds one box into a database that has no road tables.
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
  [11, line([-74.003, 40.712], [-74.003, 40.714]), { highway: 'service' }],
  [12, line([-74.004, 40.712], [-74.004, 40.714]), { highway: 'service', lanes: '1' }],
  // A one-way that gains a lane, and one drawn along its right kerb.
  [20, line([-74.010, 40.712], [-74.010, 40.7125]), { highway: 'residential', oneway: 'yes', lanes: '2' }],
  [21, line([-74.010, 40.7125], [-74.010, 40.7135]), { highway: 'residential', oneway: 'yes', lanes: '3' }],
  [22, line([-74.012, 40.712], [-74.012, 40.713]), { highway: 'residential', oneway: 'yes', lanes: '2', placement: 'right_of:2' }],
  [27, line([-74.010, 40.7135], [-74.010, 40.7145]), { highway: 'residential', oneway: 'yes', lanes: '3' }],
  // A one-way that gains a lane well south of a cell boundary at 40.718.
  [28, line([-74.020, 40.712], [-74.020, 40.7125]), { highway: 'residential', oneway: 'yes', lanes: '2' }],
  [29, line([-74.020, 40.7125], [-74.020, 40.722]), { highway: 'residential', oneway: 'yes', lanes: '3' }],
  // A one-way that gains a lane, then runs on as nine more ways.
  [40, line([-74.040, 40.712], [-74.040, 40.7125]), { highway: 'residential', oneway: 'yes', lanes: '2' }],
  [41, line([-74.040, 40.7125], [-74.040, 40.7134]), { highway: 'residential', oneway: 'yes', lanes: '3' }],
  [42, line([-74.040, 40.7134], [-74.040, 40.7143]), { highway: 'residential', oneway: 'yes', lanes: '3' }],
  [43, line([-74.040, 40.7143], [-74.040, 40.7152]), { highway: 'residential', oneway: 'yes', lanes: '3' }],
  [44, line([-74.040, 40.7152], [-74.040, 40.7161]), { highway: 'residential', oneway: 'yes', lanes: '3' }],
  [45, line([-74.040, 40.7161], [-74.040, 40.7170]), { highway: 'residential', oneway: 'yes', lanes: '3' }],
  [46, line([-74.040, 40.7170], [-74.040, 40.7179]), { highway: 'residential', oneway: 'yes', lanes: '3' }],
  [47, line([-74.040, 40.7179], [-74.040, 40.7188]), { highway: 'residential', oneway: 'yes', lanes: '3' }],
  [48, line([-74.040, 40.7188], [-74.040, 40.7197]), { highway: 'residential', oneway: 'yes', lanes: '3' }],
  [49, line([-74.040, 40.7197], [-74.040, 40.7206]), { highway: 'residential', oneway: 'yes', lanes: '3' }],
  // The same across a cell boundary at 40.72, its first way changed below.
  [50, line([-74.050, 40.718], [-74.050, 40.7195]), { highway: 'residential', oneway: 'yes', lanes: '2' }],
  [51, line([-74.050, 40.7195], [-74.050, 40.7215]), { highway: 'residential', oneway: 'yes', lanes: '3' }],
  [52, line([-74.050, 40.7215], [-74.050, 40.723]), { highway: 'residential', oneway: 'yes', lanes: '3' }],
  // A left-bay approach that runs on as two lanes.
  [60, line([-74.070, 40.712], [-74.070, 40.713]), { highway: 'residential', oneway: 'yes', lanes: '3', 'turn:lanes': 'left|through|through' }],
  [61, line([-74.070, 40.713], [-74.070, 40.714]), { highway: 'residential', oneway: 'yes', lanes: '2' }],
  // One unsplit two-way street with bike lanes, through a crossroads.
  [62, line([-74.080, 40.7145], [-74.079, 40.7145], [-74.078, 40.7145]), { highway: 'residential', lanes: '2', 'cycleway:both': 'lane' }],
  [63, line([-74.079, 40.714], [-74.079, 40.7145], [-74.079, 40.715]), { highway: 'residential', lanes: '2' }],
  // A crossing that only clips the kerb of road 12, at 25° to it.
  [30, line([-74.004 + 0.9 / 84300, 40.7125], [-74.004 + (0.9 + 4.226) / 84300, 40.7125 + 9.063 / 111000]),
    { highway: 'footway', footway: 'crossing', 'crossing:markings': 'zebra' }],
  // A street with bike lanes on both sides of a crossroads.
  [23, line([-74.008, 40.7145], [-74.007, 40.7145]), { highway: 'residential', lanes: '2', 'cycleway:both': 'lane' }],
  [24, line([-74.007, 40.7145], [-74.006, 40.7145]), { highway: 'residential', lanes: '2', 'cycleway:both': 'lane' }],
  [25, line([-74.007, 40.714], [-74.007, 40.7145]), { highway: 'residential', lanes: '2' }],
  [26, line([-74.007, 40.7145], [-74.007, 40.715]), { highway: 'residential', lanes: '2' }],
  // A crossing drawn well past both kerbs, and askew to the street.
  [9, line([-74.0006, 40.70985], [-74.0004, 40.71015]), { highway: 'footway', footway: 'crossing', 'crossing:markings': 'zebra' }],
]

// Every build drops the *_next tables by bare name too, scoped ones included.
const GUARD_NEXT = 'CREATE TABLE road_surfaces_next (); CREATE TABLE road_markings_next (); CREATE TABLE road_glyphs_next ();'

const run = DATABASE_URL ? describe : describe.skip
let sql: postgres.Sql

run('generate-road-markings.sql', () => {
  beforeAll(async () => {
    sql = postgres(DATABASE_URL!, { max: 1, onnotice: () => {} })
    await sql.unsafe(`DROP SCHEMA IF EXISTS ${SCHEMA} CASCADE; CREATE SCHEMA ${SCHEMA}; SET search_path TO ${SCHEMA}, public;
      CREATE TABLE geo_places (id text, osm_id bigint, tags jsonb NOT NULL, geom geometry(Geometry, 4326) NOT NULL, geom_type text NOT NULL);
      -- The script drops these by bare name; without them here the drop would reach public's.
      CREATE TABLE road_surfaces (); CREATE TABLE road_markings (); CREATE TABLE road_glyphs (); ${GUARD_NEXT}`)
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

  test('paves a service road only where its lanes are mapped', async () => {
    expect(await covered(-74.003, 40.713)).toBe(false)
    expect(await covered(-74.004, 40.713)).toBe(true)
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

  // Metres east of a longitude near 40.71°N.
  const east = (x: number, m: number) => x + m / 84300

  test('widens a one-way for an added lane on its right only', async () => {
    for (const y of [40.7122, 40.7133]) {
      expect(await covered(east(-74.010, -2.5), y)).toBe(true)
      expect(await covered(east(-74.010, -3.5), y)).toBe(false)
    }
    expect(await covered(east(-74.010, 3.5), 40.7122)).toBe(false)
    expect(await covered(east(-74.010, 5.5), 40.7133)).toBe(true)
  })

  test('eases the way after it back to the middle of the road', async () => {
    expect(await covered(east(-74.010, -4), 40.7142)).toBe(true)
    expect(await covered(east(-74.010, -5), 40.7142)).toBe(false)
    expect(await covered(east(-74.010, 4), 40.7142)).toBe(true)
    expect(await covered(east(-74.010, 5), 40.7142)).toBe(false)
  })

  test('runs on without a jog at any join down a long chain', async () => {
    // Where the lane opens the left kerb holds and the right one moves out.
    for (const y of [40.71245, 40.71255]) {
      expect(await covered(east(-74.040, -2.5), y)).toBe(true)
      expect(await covered(east(-74.040, -3.5), y)).toBe(false)
    }
    for (let k = 1; k < 8; k++) {
      const join = 40.7125 + (k + 1) * 0.0009
      for (const y of [join - 0.00005, join + 0.00005]) {
        expect(await covered(east(-74.040, -4), y)).toBe(true)
        expect(await covered(east(-74.040, -5), y)).toBe(false)
        expect(await covered(east(-74.040, 4), y)).toBe(true)
        expect(await covered(east(-74.040, 5), y)).toBe(false)
      }
    }
  })

  test('builds with a skewed crossing too short for one bar', async () => {
    const [{ n }] = await sql`SELECT count(*)::int as n FROM road_surfaces`
    expect(n).toBeGreaterThan(0)
  })

  test('gives back no band, rather than failing, for a line it cannot taper', async () => {
    const [{ band }] = await sql`SELECT road_taper_band('POINT(0 0)'::geometry, 0, 1, 2, 3, 10) as band`
    expect(band).toBeNull()
  })

  test('lines a way up with the through lanes of a left-bay approach before it', async () => {
    for (const [m, hit] of [[2.5, true], [3.5, false], [-2.5, true], [-3.5, false]] as const) {
      expect(await covered(east(-74.070, m), 40.7135)).toBe(hit)
    }
  })

  test('paints bike lanes across a crossroads in both directions of one street', async () => {
    for (const side of [1, -1]) {
      const [{ n }] = await sql`
        SELECT count(*)::int as n FROM road_markings
        WHERE kind IN ('bike_lane', 'bike') AND ST_DWithin(geom::geography,
          ST_SetSRID(ST_MakePoint(-74.079, ${40.7145 + (side * 3.8) / 111000}), 4326)::geography, 3)`
      expect(n).toBeGreaterThan(0)
    }
  })

  test('lays a way tagged placement=right_of:2 along its right kerb', async () => {
    expect(await covered(east(-74.012, -5), 40.7125)).toBe(true)
    expect(await covered(east(-74.012, 1), 40.7125)).toBe(false)
  })

  test('carries a bike lane across a junction to the lane beyond it', async () => {
    // Eastbound, the lane centre is 3.8 m right of the way.
    const [{ fill, edges }] = await sql`
      SELECT count(*) FILTER (WHERE kind = 'bike_lane')::int as fill,
             count(*) FILTER (WHERE kind = 'bike' AND pattern = 'dashed')::int as edges
      FROM road_markings
      WHERE ST_DWithin(geom::geography, ST_SetSRID(ST_MakePoint(-74.007, 40.7145 - 3.8 / 111000), 4326)::geography, 3)`
    expect(fill).toBeGreaterThan(0)
    expect(edges).toBeGreaterThan(0)
  })

  test('runs crosswalk bars with the traffic on the road they cross', async () => {
    const bars = await sql`
      SELECT (ST_XMax(b.geom) - ST_XMin(b.geom)) * 84300 as dx, (ST_YMax(b.geom) - ST_YMin(b.geom)) * 111000 as dy
      FROM road_markings m, ST_Dump(m.geom) b
      WHERE m.kind = 'crosswalk' AND ST_DWithin(m.geom::geography, ST_SetSRID(ST_MakePoint(-74.0005, ${STREET_Y}), 4326)::geography, 8)`
    expect(bars.length).toBeGreaterThan(3)
    // The street runs east-west and the crossing is drawn askew across it.
    for (const b of bars) expect(b.dx).toBeGreaterThan(2 * b.dy)
  })

  test('keeps lane lines out of the crossing carriageway', async () => {
    const [{ n }] = await sql`
      SELECT count(*)::int as n FROM road_markings
      WHERE kind IN ('lane', 'centre') AND ST_DWithin(geom::geography, ST_SetSRID(ST_MakePoint(${SB_X}, ${STREET_Y}), 4326)::geography, 3)`
    expect(n).toBe(0)
  })

  describe('rebuilding one box', () => {
    const BOX = (sql: postgres.Sql) => sql`ST_MakeEnvelope(-74.0003, 40.7095, -73.9985, 40.7105, 4326)`
    const footprint = async () => (await sql`
      SELECT ST_Area(ST_Union(geom)::geography) as union_m2, sum(ST_Area(geom::geography)) as sum_m2 FROM road_surfaces`)[0]
    let before: any

    beforeAll(async () => {
      before = await footprint()
      await sql.unsafe(GUARD_NEXT)
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

  describe('a diff upstream of a cell boundary', () => {
    let queued = 0
    let ownTest = false
    beforeAll(async () => {
      await sql`UPDATE geo_places SET tags = tags || '{"lanes": "1"}' WHERE id = 'W50'`
      await sql.unsafe(`DROP SCHEMA IF EXISTS osm_replay CASCADE; CREATE SCHEMA osm_replay;
        CREATE TABLE osm_replay.old_places AS
          SELECT id, 'W'::char(1) as osm_type, osm_id, NULL::text as name, '{highway/residential}'::text[] as categories, geom, geom_type, NULL::int as admin_level
          FROM geo_places WHERE id = 'W50';
        CREATE TABLE osm_replay.changed AS SELECT id FROM geo_places WHERE id = 'W50';`)
      await sql.unsafe(readFileSync(join(import.meta.dir, 'detail-queue-table.sql'), 'utf8'))
      await sql`DELETE FROM detail_dirty`
      // As on a database last built before the queue's road test existed.
      await sql`DROP FUNCTION road_is_marked_way(jsonb)`
      await sql.unsafe(readFileSync(join(import.meta.dir, 'queue-road-markings.sql'), 'utf8'))
      queued = (await sql`SELECT count(*)::int as n FROM detail_dirty`)[0].n
      ownTest = (await sql`SELECT to_regprocedure(${SCHEMA + '.road_is_marked_way(jsonb)'}) IS NOT NULL as ok`)[0].ok
      const out = await sql.unsafe(readFileSync(join(import.meta.dir, 'road-markings-dirty-cells.sql'), 'utf8')
        .replaceAll(':max_cells', '100').replaceAll(':max_attempts', '3').replaceAll(':cell', '0.02'))
      const rows = (Array.isArray(out.at(-1)) ? out.at(-1) : out) as { kind: string; a: string; b: string }[]
      for (const c of rows.filter(r => r.kind === 'cell')) {
        const [cx, cy] = [Number(c.a), Number(c.b)]
        await sql`TRUNCATE _rm_scope`
        await sql`INSERT INTO _rm_scope VALUES (ST_MakeEnvelope(${cx * 0.02}, ${cy * 0.02}, ${(cx + 1) * 0.02}, ${(cy + 1) * 0.02}, 4326))`
        await sql.unsafe(readFileSync(join(import.meta.dir, 'generate-road-markings.sql'), 'utf8'))
      }
    }, 180_000)

    afterAll(async () => {
      await sql`DELETE FROM detail_dirty`
    })

    test('queues it on a database whose last build had no road test of its own', async () => {
      expect(ownTest).toBe(true)
      // The way, its old outline, and the two ways carrying on from it.
      expect(queued).toBe(4)
    })

    test('rebuilds the ways downstream as well, on both sides of the boundary', async () => {
      // One lane before: the next way's left kerb moves in to 1.5 m.
      for (const y of [40.7198, 40.71995, 40.72005, 40.7212]) {
        expect(await covered(east(-74.050, -1), y)).toBe(true)
        expect(await covered(east(-74.050, -2), y)).toBe(false)
        expect(await covered(east(-74.050, 7), y)).toBe(true)
      }
    })
  })

  test('keeps the queue\'s road test the same as the build\'s', () => {
    const body = (file: string) => {
      const text = readFileSync(join(import.meta.dir, file), 'utf8')
      const start = text.indexOf('CREATE OR REPLACE FUNCTION road_is_marked_way')
      return text.slice(start, text.indexOf('$$;', start))
    }
    expect(body('queue-road-markings.sql')).toBe(body('generate-road-markings.sql'))
  })

  describe('two neighbouring cells', () => {
    const cell = (y0: number, y1: number) => sql`ST_MakeEnvelope(-74.03, ${y0}, -74.01, ${y1}, 4326)`
    beforeAll(async () => {
      for (const [y0, y1] of [[40.70, 40.718], [40.718, 40.73]]) {
        await sql`TRUNCATE _rm_scope`
        await sql`INSERT INTO _rm_scope VALUES (${cell(y0, y1)})`
        await sql.unsafe(readFileSync(join(import.meta.dir, 'generate-road-markings.sql'), 'utf8'))
      }
    }, 120_000)

    test('place a way the same on both sides of the boundary, far from where it widened', async () => {
      for (const y of [40.7179, 40.7181, 40.720]) {
        expect(await covered(east(-74.020, -2.5), y)).toBe(true)
        expect(await covered(east(-74.020, -3.5), y)).toBe(false)
        expect(await covered(east(-74.020, 5.5), y)).toBe(true)
      }
    })

    test('meet without a gap or an overlap', async () => {
      const [{ union_m2, sum_m2 }] = await sql`
        SELECT ST_Area(ST_Union(geom)::geography) as union_m2, sum(ST_Area(geom::geography)) as sum_m2
        FROM road_surfaces WHERE geom && ST_MakeEnvelope(-74.021, 40.717, -74.019, 40.719, 4326)`
      expect(Math.abs(sum_m2 - union_m2) / union_m2).toBeLessThan(0.01)
      expect(union_m2).toBeGreaterThan(9 * 200)
    })
  })

  describe('queueing a replication diff', () => {
    const plan = async (maxCells: number) =>
      sql.unsafe(readFileSync(join(import.meta.dir, 'road-markings-dirty-cells.sql'), 'utf8')
        .replaceAll(':max_cells', String(maxCells)).replaceAll(':max_attempts', '3').replaceAll(':cell', '0.02'))
    const rows = (result: any) => (Array.isArray(result.at(-1)) ? result.at(-1) : result) as { kind: string; a: string; b: string }[]

    beforeAll(async () => {
      await sql.unsafe(`DROP SCHEMA IF EXISTS osm_replay CASCADE; CREATE SCHEMA osm_replay;
        CREATE TABLE osm_replay.old_places AS
          SELECT id, 'W'::char(1) as osm_type, osm_id, NULL::text as name, NULL::text[] as categories, ST_Translate(geom, 0.05, 0) as geom, geom_type, NULL::int as admin_level
          FROM geo_places WHERE id = 'W5';
        -- A crossing deleted where its crosswalk is painted, a sidewalk away
        -- from any, and a building: only the first matters to road markings.
        INSERT INTO osm_replay.old_places (id, osm_type, osm_id, categories, geom, geom_type) VALUES
          ('W98', 'W', 98, '{highway/footway}', ST_GeomFromText('LINESTRING(-74.0006 40.70985, -74.0004 40.71015)', 4326), 'line'),
          ('W99', 'W', 99, '{highway/footway}', ST_GeomFromText('LINESTRING(-74.0035 40.7125, -74.0035 40.7135)', 4326), 'line'),
          ('W97', 'W', 97, '{building/house}', ST_GeomFromText('POLYGON((-74.0005 40.7101, -74.0004 40.7101, -74.0004 40.7102, -74.0005 40.7101))', 4326), 'area');
        CREATE TABLE osm_replay.changed AS SELECT id FROM geo_places WHERE id IN ('W5', 'N8');`)
      await sql.unsafe(readFileSync(join(import.meta.dir, 'detail-queue-table.sql'), 'utf8'))
      await sql.unsafe(readFileSync(join(import.meta.dir, 'queue-road-markings.sql'), 'utf8'))
    })

    test('queues the old and new outlines of what road markings are drawn from, and nothing else', async () => {
      // W5 old and new, the signal N8, and the crossing W98.
      const [{ n }] = await sql`SELECT count(*)::int as n FROM detail_dirty WHERE layer = 'road_markings'`
      expect(n).toBe(4)
    })

    test('drops what lies where nothing was built, and plans the cells of the rest', async () => {
      const out = rows(await plan(100))
      const dropped = out.find(r => r.kind === 'dropped')!
      // The old outline lies 0.05° east, where nothing was built.
      expect(dropped.a).toBe('1')
      expect(out.find(r => r.kind === 'entries')!.a.split(',')).toHaveLength(3)
      const cells = out.filter(r => r.kind === 'cell')
      expect(cells.length).toBeGreaterThan(0)
      for (const c of cells) expect(Number(c.a) * 0.02).toBeLessThan(-73.98)
      const [{ n }] = await sql`SELECT count(*)::int as n FROM detail_dirty WHERE layer = 'road_markings'`
      expect(n).toBe(3)
    })

    test('always takes the oldest entry, however many cells it spans', async () => {
      const entries = rows(await plan(1)).find(r => r.kind === 'entries')!.a.split(',')
      const [{ oldest }] = await sql`SELECT min(id)::text as oldest FROM detail_dirty WHERE layer = 'road_markings'`
      expect(entries).toContain(oldest)
    })

    test('takes a failed entry last', async () => {
      const [{ oldest }] = await sql`SELECT min(id)::text as oldest FROM detail_dirty WHERE layer = 'road_markings'`
      await sql`UPDATE detail_dirty SET attempts = 1 WHERE id = ${oldest}`
      const [{ next }] = await sql`SELECT min(id)::text as next FROM detail_dirty WHERE layer = 'road_markings' AND attempts = 0`
      const entries = rows(await plan(1)).find(r => r.kind === 'entries')!.a.split(',')
      expect(entries).toContain(next)
      await sql`UPDATE detail_dirty SET attempts = 3 WHERE id = ${oldest}`
      expect(rows(await plan(100)).find(r => r.kind === 'dropped')!.b).toBe('1')
    })
  })

  describe('a first scoped build', () => {
    const BOX = 'ST_MakeEnvelope(-74.0003, 40.7095, -73.9985, 40.7105, 4326)'

    beforeAll(async () => {
      await sql.unsafe(`DROP TABLE road_surfaces, road_markings, road_glyphs; ${GUARD_NEXT}
        TRUNCATE _rm_scope; INSERT INTO _rm_scope VALUES (${BOX});`)
      await sql.unsafe(readFileSync(join(import.meta.dir, 'generate-road-markings.sql'), 'utf8'))
    }, 60_000)

    test('creates the live tables and fills only the box', async () => {
      const [{ inside, outside }] = await sql.unsafe(`
        SELECT count(*) FILTER (WHERE ST_CoveredBy(geom, ${BOX}))::int as inside,
               count(*) FILTER (WHERE NOT ST_CoveredBy(geom, ${BOX}))::int as outside
        FROM road_surfaces`)
      expect(inside).toBeGreaterThan(0)
      expect(outside).toBe(0)
      const [{ n }] = await sql`SELECT count(*)::int as n FROM road_markings`
      expect(n).toBeGreaterThan(0)
    })
  })
})
