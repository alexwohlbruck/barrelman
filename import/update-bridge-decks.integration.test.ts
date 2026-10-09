/**
 * Builds bridge decks over a synthetic crossing in a throwaway schema, on flat
 * terrain, then replays replication diffs as replicate-extract.sh would: the
 * queue (queue-bridge-decks.sql) and an Update Bridge Decks run, checked
 * against a full rebuild of the same data.
 *
 * Point it at a scratch database: the queue uses the osm_replay schema that
 * replicate-extract.sh uses.
 *
 * Run: BARRELMAN_INTEGRATION_TESTS=1 DATABASE_URL=postgresql://barrelman:barrelman@localhost:5434/scratch \
 *      bun test import/update-bridge-decks.integration.test.ts
 */
import { afterAll, beforeAll, describe, expect, test } from 'bun:test'
import postgres from 'postgres'
import { readFileSync } from 'fs'
import { join } from 'path'
import { Dem } from '../src/lib/bridge-decks/dem'
import { cellBox } from '../src/lib/bridge-decks/queue'
import { rebuildBoxes, recordCell } from './bridge-deck-cells'
import { updateDecks } from './update-bridge-decks'

const DATABASE_URL = process.env.BARRELMAN_INTEGRATION_TESTS ? process.env.DATABASE_URL : undefined
const SCHEMA = 'bridge_decks_test'
const CELL = cellBox('-4042,1761')
const Y = 35.225

const line = (...pts: [number, number][]) => `LINESTRING(${pts.map(p => p.join(' ')).join(', ')})`
const road = (extra: Record<string, string> = {}) => ({ highway: 'primary', ...extra })
const bridge = { bridge: 'yes', layer: '1' }
const ways: [number, string, Record<string, string>][] = [
  [1, line([-80.836, Y], [-80.834, Y]), road()],
  [2, line([-80.834, Y], [-80.832, Y]), road(bridge)],
  [3, line([-80.832, Y], [-80.830, Y]), road(bridge)],
  [4, line([-80.830, Y], [-80.828, Y]), road()],
  [5, line([-80.826, 35.23], [-80.8255, 35.23]), { highway: 'footway', ...bridge }],
  [10, line([-80.833, 35.224], [-80.833, 35.226]), { waterway: 'river' }],
]

const flat = () => new Dem(async () => ({ size: 4, data: new Float32Array(16).fill(200) }))
const run = DATABASE_URL ? describe : describe.skip
let sql: postgres.Sql

const file = (name: string) => readFileSync(join(import.meta.dir, name), 'utf8')
const decks = async () => sql<{ id: string; ways: string[] }[]>`SELECT id, ways::text[] AS ways FROM bridge_decks ORDER BY id`
const snapshot = async () => (await sql`SELECT id, ST_AsText(geom) AS geom, heights::text, piers::text, bridge FROM bridge_decks ORDER BY id`)
  .map(r => JSON.stringify(r))
const queued = async () => (await sql`SELECT count(*)::int AS n FROM detail_dirty WHERE layer = 'bridge_decks'`)[0].n

/**
 * One replication cycle: `ids` are the rows rewritten, `edited` the objects
 * the diff itself changed, and `change` the SQL that rewrites them.
 */
async function replay(ids: string[], edited: string[], change: string) {
  await sql.unsafe(`DROP SCHEMA IF EXISTS osm_replay CASCADE; CREATE SCHEMA osm_replay;
    CREATE TABLE osm_replay.old_places AS
      SELECT id, osm_type, osm_id, name, categories, geom, geom_type, admin_level FROM geo_places WHERE id = ANY('{${ids}}');
    CREATE TABLE osm_replay.edited AS
      SELECT 'W'::char(1) AS osm_type, split_part(x, '/', 2)::bigint AS osm_id FROM unnest('{${edited}}'::text[]) x;
    ${change};
    CREATE TABLE osm_replay.changed AS SELECT id FROM geo_places WHERE id = ANY('{${ids}}');`)
  await sql.unsafe(file('detail-queue-table.sql'))
  await sql.unsafe(file('queue-bridge-decks.sql'))
}

run('incremental bridge decks', () => {
  beforeAll(async () => {
    sql = postgres(DATABASE_URL!, { max: 1, onnotice: () => {}, connection: { search_path: `${SCHEMA}, public` } })
    const ddl = file('create-detail-views.sql')
    const tables = ddl.slice(ddl.indexOf('CREATE TABLE IF NOT EXISTS bridge_decks ('), ddl.indexOf('CREATE OR REPLACE VIEW bridge_deck_tiles'))
    await sql.unsafe(`DROP SCHEMA IF EXISTS ${SCHEMA} CASCADE; CREATE SCHEMA ${SCHEMA};
      CREATE TABLE geo_places (id text, osm_type char(1), osm_id bigint, name text, categories text[], tags jsonb,
        geom geometry(Geometry, 4326), geom_type text, admin_level int);
      ${tables}`)
    for (const [id, wkt, tags] of ways) {
      const categories = Object.entries(tags).filter(([k]) => ['highway', 'waterway'].includes(k)).map(([k, v]) => `${k}/${v}`)
      await sql`INSERT INTO geo_places (id, osm_type, osm_id, categories, tags, geom, geom_type)
        VALUES (${'way/' + id}, 'W', ${id}, ${categories}, ${sql.json(tags)}, ST_GeomFromText(${wkt}, 4326), 'line')`
    }
    await rebuildBoxes(sql, flat(), [CELL])
    await recordCell(sql, CELL)
  }, 60_000)

  afterAll(async () => {
    await sql?.unsafe(`DROP SCHEMA IF EXISTS ${SCHEMA} CASCADE; DROP SCHEMA IF EXISTS osm_replay CASCADE`)
    await sql?.end()
  })

  test('builds one deck per bridge to start with', async () => {
    expect((await decks()).map(d => d.ways)).toEqual(expect.arrayContaining([['2', '3'], ['5']]))
  })

  describe('a cycle that only rewrites a road', () => {
    test('queues nothing when the road is as it was', async () => {
      await replay(['way/4'], [], 'SELECT 1')
      expect(await queued()).toBe(0)
    })

    test('queues the deck it meets when the diff edited it', async () => {
      await replay(['way/4'], ['way/4'], 'SELECT 1')
      expect(await queued()).toBe(1)
      await sql`DELETE FROM detail_dirty`
    })
  })

  describe('a cycle that deletes, untags and adds bridges', () => {
    beforeAll(async () => {
      await replay(['way/3', 'way/5', 'way/6', 'way/7'], ['way/3', 'way/5', 'way/6', 'way/7'], `
        DELETE FROM geo_places WHERE id = 'way/5';
        UPDATE geo_places SET tags = tags || '{"bridge": "no"}' WHERE id = 'way/3';
        INSERT INTO geo_places (id, osm_type, osm_id, categories, tags, geom, geom_type) VALUES
          ('way/6', 'W', 6, '{highway/footway}', '{"highway": "footway", "bridge": "yes"}', ST_GeomFromText('${line([-80.838, 35.235], [-80.8375, 35.235])}', 4326), 'line'),
          ('way/7', 'W', 7, '{highway/footway}', '{"highway": "footway", "bridge": "yes"}', ST_GeomFromText('${line([-100, 40], [-99.9995, 40])}', 4326), 'line')`)
    })

    test('queues the new bridges and the decks the change touched', async () => {
      // Bridges 6 and 7 as they are now; the decks of 2+3 and of 5 as they were.
      expect(await queued()).toBe(4)
    })

    test('rebuilds them, and drops what lies where no decks were built', async () => {
      expect(await updateDecks(sql, flat(), 100)).toBe(0)
      expect(await queued()).toBe(0)
      const ways = (await decks()).map(d => d.ways)
      expect(ways).toContainEqual(['2'])
      expect(ways).toContainEqual(['6'])
      expect(ways.flat()).not.toContain('3')
      expect(ways.flat()).not.toContain('5')
      expect(ways.flat()).not.toContain('7')
    }, 60_000)

    test('leaves exactly what a full rebuild builds, with nothing doubled or left over', async () => {
      const incremental = await snapshot()
      await sql`TRUNCATE bridge_decks`
      await rebuildBoxes(sql, flat(), [CELL])
      expect(await snapshot()).toEqual(incremental)
    }, 60_000)
  })
})
