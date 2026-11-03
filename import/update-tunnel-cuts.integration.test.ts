/**
 * Builds tunnel cuts over a synthetic road tunnelling into a hillside, in a
 * throwaway schema, then replays replication diffs as replicate-extract.sh
 * would: the queue (queue-tunnel-cuts.sql) and an Update Tunnel Cuts run,
 * checked against a full rebuild of the same data.
 *
 * Run: BARRELMAN_INTEGRATION_TESTS=1 DATABASE_URL=postgresql://barrelman:barrelman@localhost:5434/scratch \
 *      bun test import/update-tunnel-cuts.integration.test.ts
 */
import { afterAll, beforeAll, describe, expect, test } from 'bun:test'
import postgres from 'postgres'
import { readFileSync } from 'fs'
import { join } from 'path'
import { Dem } from '../src/lib/bridge-decks/dem'
import { buildArea } from './generate-tunnel-cuts'
import { updateCuts } from './update-tunnel-cuts'

const DATABASE_URL = process.env.BARRELMAN_INTEGRATION_TESTS ? process.env.DATABASE_URL : undefined
const SCHEMA = 'tunnel_cuts_test'
const Y = 35.225
const AREA: [number, number, number, number] = [-80.85, 35.2, -80.8, 35.25]

const line = (...pts: [number, number][]) => `LINESTRING(${pts.map(p => p.join(' ')).join(', ')})`
const ways: [number, string, Record<string, string>][] = [
  [1, line([-80.836, Y], [-80.834, Y]), { highway: 'secondary' }],
  [2, line([-80.834, Y], [-80.832, Y]), { highway: 'secondary', tunnel: 'yes', layer: '-1' }],
  // A passage under a building: a tunnel, but not below ground.
  [3, line([-80.826, 35.23], [-80.8255, 35.23]), { highway: 'service' }],
  [4, line([-80.8255, 35.23], [-80.825, 35.23]), { highway: 'service', tunnel: 'building_passage' }],
]

/** Flat at 200 m west of -80.834, rising 20% east of it: the hillside the tunnel goes into. */
const hill = () => new Dem(async (z, x, y) => {
  const size = 256
  const data = new Float32Array(size * size)
  for (let i = 0; i < size; i++) {
    const lng = ((x + i / size) / 2 ** z) * 360 - 180
    const east = (lng + 80.834) * 111320 * Math.cos((Y * Math.PI) / 180)
    for (let j = 0; j < size; j++) data[j * size + i] = 200 + Math.max(0, east) * 0.2
  }
  return { size, data }
})

const run = DATABASE_URL ? describe : describe.skip
let sql: postgres.Sql
const file = (name: string) => readFileSync(join(import.meta.dir, name), 'utf8')
const cuts = async () => (await sql`SELECT id, ST_AsText(anchor) AS anchor, floor::text, wall_left::text, wall_right::text, crown FROM tunnel_cuts ORDER BY id`).map(r => JSON.stringify(r))
const queued = async () => (await sql`SELECT count(*)::int AS n FROM detail_dirty WHERE layer = 'tunnel_cuts'`)[0].n

async function replay(ids: string[], edited: string[], change: string) {
  await sql.unsafe(`DROP SCHEMA IF EXISTS osm_replay CASCADE; CREATE SCHEMA osm_replay;
    CREATE TABLE osm_replay.old_places AS
      SELECT id, osm_type, osm_id, name, categories, geom, geom_type, admin_level FROM geo_places WHERE id = ANY('{${ids}}');
    CREATE TABLE osm_replay.edited AS
      SELECT 'W'::char(1) AS osm_type, split_part(x, '/', 2)::bigint AS osm_id FROM unnest('{${edited}}'::text[]) x;
    ${change};
    CREATE TABLE osm_replay.changed AS SELECT id FROM geo_places WHERE id = ANY('{${ids}}');`)
  await sql.unsafe(file('detail-queue-table.sql'))
  await sql.unsafe(file('queue-tunnel-cuts.sql'))
}

run('tunnel cuts', () => {
  beforeAll(async () => {
    sql = postgres(DATABASE_URL!, { max: 1, onnotice: () => {}, connection: { search_path: `${SCHEMA}, public` } })
    const [{ shared }] = await sql`SELECT to_regclass('public.tunnel_cuts') IS NOT NULL AS shared`
    if (shared) throw new Error('public has a tunnel_cuts table of its own: point DATABASE_URL at an empty scratch database')
    const ddl = file('create-detail-views.sql')
    await sql.unsafe(`DROP SCHEMA IF EXISTS ${SCHEMA} CASCADE; CREATE SCHEMA ${SCHEMA};
      CREATE TABLE geo_places (id text, osm_type char(1), osm_id bigint, name text, categories text[], tags jsonb,
        geom geometry(Geometry, 4326), geom_type text, admin_level int);
      CREATE TABLE road_surfaces (fid bigint, bridge boolean, geom geometry(MultiPolygon, 4326), kind text);
      ${ddl.slice(ddl.indexOf('CREATE TABLE IF NOT EXISTS tunnel_cuts'))}`)
    for (const [id, wkt, tags] of ways)
      await sql`INSERT INTO geo_places (id, osm_type, osm_id, categories, tags, geom, geom_type)
        VALUES (${'way/' + id}, 'W', ${id}, ${[`highway/${tags.highway}`]}, ${sql.json(tags)}, ST_GeomFromText(${wkt}, 4326), 'line')`
    await buildArea(sql, hill(), AREA)
  }, 60_000)

  afterAll(async () => {
    await sql?.unsafe(`DROP SCHEMA IF EXISTS ${SCHEMA} CASCADE; DROP SCHEMA IF EXISTS osm_replay CASCADE`)
    await sql?.end()
  })

  test('cuts down to the portal where the road goes into the hill, and not under the building', async () => {
    const rows = await sql`SELECT kind, ST_X(anchor) AS lng, open, at FROM tunnel_cuts`
    expect(rows).toHaveLength(1)
    expect(rows[0].kind).toBe('road')
    expect(rows[0].lng).toBeCloseTo(-80.834, 4)
    expect(rows[0].open).toBeLessThan(rows[0].at)
  })

  test('serves each vertex its heights, in decimetres along the line', async () => {
    const [tile] = await sql`SELECT line, floor, wall_left, at, ST_NPoints(geom) AS n FROM tunnel_cut_tiles`
    expect(tile.line.split(';')).toHaveLength(tile.n)
    expect(tile.floor.split(',')).toHaveLength(tile.n)
    expect(Number(tile.floor.split(',')[tile.at]) / 10).toBeLessThan(200)
  })

  test('builds the same cuts again in place', async () => {
    const before = await cuts()
    await buildArea(sql, hill(), AREA)
    expect(await cuts()).toEqual(before)
  }, 60_000)

  test('queues nothing when the road is only rewritten', async () => {
    await replay(['way/1'], [], 'SELECT 1')
    expect(await queued()).toBe(0)
  })

  describe('a cycle that moves the tunnel', () => {
    beforeAll(async () => {
      await replay(['way/1', 'way/2'], ['way/1', 'way/2'], `
        UPDATE geo_places SET geom = ST_GeomFromText('${line([-80.836, Y], [-80.8335, Y])}', 4326) WHERE id = 'way/1';
        UPDATE geo_places SET geom = ST_GeomFromText('${line([-80.8335, Y], [-80.832, Y])}', 4326) WHERE id = 'way/2'`)
    })

    test('queues the tunnel as it is now and the cut as it was', async () => {
      expect(await queued()).toBe(2)
    })

    test('rebuilds the cut at the new portal, as a full rebuild would', async () => {
      expect(await updateCuts(sql, hill(), 100)).toBe(0)
      expect(await queued()).toBe(0)
      const [row] = await sql`SELECT ST_X(anchor) AS lng FROM tunnel_cuts`
      expect(row.lng).toBeCloseTo(-80.8335, 4)
      const incremental = await cuts()
      await sql`TRUNCATE tunnel_cuts`
      await buildArea(sql, hill(), AREA)
      expect(await cuts()).toEqual(incremental)
    }, 60_000)
  })

  test('drops the cut of a tunnel retagged out of the ground', async () => {
    await replay(['way/2'], ['way/2'], `UPDATE geo_places SET tags = tags - 'tunnel' WHERE id = 'way/2'`)
    expect(await queued()).toBe(1)
    expect(await updateCuts(sql, hill(), 100)).toBe(0)
    expect(await sql`SELECT id FROM tunnel_cuts`).toHaveLength(0)
  }, 60_000)
})
