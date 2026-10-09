/**
 * Builds bridge decks over a synthetic crossing in a throwaway schema, on flat
 * terrain, then replays replication diffs as replicate-extract.sh would: the
 * queue (queue-bridge-decks.sql) and an Update Bridge Decks run, checked
 * against a full rebuild of the same data.
 *
 * Point it at an empty scratch database with PostGIS: the queue uses the
 * osm_replay schema that replicate-extract.sh uses, and a bridge table in
 * public would stand in for one the test drops.
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
import { DeckConflict, missingTables, rebuildBoxes, recordCell } from './bridge-deck-cells'
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
  // East of the built cell, about 1 km from its decks.
  [8, line([-80.8193, 35.23], [-80.8188, 35.23]), { highway: 'footway', ...bridge }],
]

const flat = () => new Dem(async () => ({ size: 4, data: new Float32Array(16).fill(200) }))
const run = DATABASE_URL ? describe : describe.skip
let sql: postgres.Sql

const file = (name: string) => readFileSync(join(import.meta.dir, name), 'utf8')
const decks = async () => sql<{ id: string; ways: string[] }[]>`SELECT id, ways::text[] AS ways FROM bridge_decks ORDER BY id`
const snapshot = async () => (await sql`SELECT id, ST_AsText(geom) AS geom, heights::text, piers::text, bridge FROM bridge_decks ORDER BY id`)
  .map(r => JSON.stringify(r))
const ofWay = async (way: number) => (await decks()).filter(d => d.ways.includes(String(way)))
const enqueue = (w: number, s: number, e: number, n: number) =>
  sql`INSERT INTO detail_dirty (layer, box) VALUES ('bridge_decks', ST_MakeEnvelope(${w}, ${s}, ${e}, ${n}, 4326))`
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
    const [{ shared }] = await sql`SELECT to_regclass('public.bridge_decks') IS NOT NULL OR to_regclass('public.bridge_deck_cells') IS NOT NULL AS shared`
    if (shared) throw new Error('public has bridge tables of its own: point DATABASE_URL at an empty scratch database')
    const ddl = file('create-detail-views.sql')
    const tables = ddl.slice(ddl.indexOf('CREATE OR REPLACE FUNCTION bridge_deck_class'), ddl.indexOf('CREATE OR REPLACE VIEW bridge_deck_tiles'))
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

  describe('which entries count as built', () => {
    const near = () => enqueue(-80.8195, 35.2295, -80.8185, 35.2305)

    test('drops an entry outside the recorded cells, however near a deck', async () => {
      await near()
      expect(await updateDecks(sql, flat(), 100)).toBe(0)
      expect(await queued()).toBe(0)
      expect(await ofWay(8)).toHaveLength(0)
    }, 60_000)

    test('falls back to nearness to a deck only while no cells are recorded', async () => {
      await sql`TRUNCATE bridge_deck_cells`
      await near()
      expect(await updateDecks(sql, flat(), 100)).toBe(0)
      expect(await ofWay(8)).toHaveLength(1)
      await sql`DELETE FROM bridge_decks WHERE '8' = ANY(ways::text[])`
      await recordCell(sql, CELL)
    }, 60_000)
  })

  describe('a deck whose id another cell still holds', () => {
    // As if the deck had last been stored by its neighbour to the east.
    const elsewhere = async () => {
      const [deck] = await ofWay(2)
      await sql`UPDATE bridge_decks SET anchor = ST_SetSRID(ST_MakePoint(-80.81, 35.23), 4326) WHERE id = ${deck.id}`
      return deck.id
    }

    test('is refused, leaving both cells as they were', async () => {
      const id = await elsewhere()
      const before = await snapshot()
      const err = await rebuildBoxes(sql, flat(), [CELL]).catch(e => e)
      expect(err).toBeInstanceOf(DeckConflict)
      expect(err.ids).toEqual([id])
      expect(await snapshot()).toEqual(before)
    }, 60_000)

    test('moves home once an update has rebuilt the cell holding it', async () => {
      const [deck] = await ofWay(2)
      await enqueue(-80.8335, 35.2245, -80.8325, 35.2255)
      expect(await updateDecks(sql, flat(), 100)).toBe(0)
      const [{ n, x }] = await sql`SELECT count(*)::int AS n, max(ST_X(anchor)) AS x FROM bridge_decks WHERE id = ${deck.id}`
      expect(n).toBe(1)
      expect(x).toBeLessThan(CELL[2])
    }, 60_000)
  })

  describe('before the API has created bridge_deck_cells', () => {
    beforeAll(async () => {
      await sql`DROP TABLE bridge_deck_cells`
      await replay(['way/9'], ['way/9'], `INSERT INTO geo_places (id, osm_type, osm_id, categories, tags, geom, geom_type) VALUES
        ('way/9', 'W', 9, '{highway/footway}', '{"highway": "footway", "bridge": "yes"}', ST_GeomFromText('${line([-80.836, 35.23], [-80.8355, 35.23])}', 4326), 'line')`)
    })

    test('queues nothing, so nothing piles up that an update would skip', async () => {
      expect(await missingTables(sql)).toEqual(['bridge_deck_cells'])
      expect(await queued()).toBe(0)
    })
  })
})
