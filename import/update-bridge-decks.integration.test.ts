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
import { grid, toUnits, type UnitPoint } from '../src/lib/bridge-decks/grid'
import { DeckConflict, missingTables, rebuildBoxes, rebuildCells } from './bridge-deck-cells'
import { buildArea } from './generate-bridge-decks'
import { updateDecks } from './update-bridge-decks'

const DATABASE_URL = process.env.BARRELMAN_INTEGRATION_TESTS ? process.env.DATABASE_URL : undefined
const SCHEMA = 'bridge_decks_test'
// Build cells of 0.02°, which update cells of 0.025° straddle.
const build = grid(toUnits(0.02))
const [A, EAST] = ['-4042,1761', '-4041,1761']
const CELL = build.box(A)
const BUILD = { partsOf: (key: string) => [build.box(key)], keyOf: build.at, record: () => true }
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
const anchorOf = async (way: number) =>
  (await sql<{ x: number; y: number }[]>`SELECT anchor_x AS x, anchor_y AS y FROM bridge_decks WHERE ${String(way)} = ANY(ways::text[])`)
    .map(r => build.at([r.x, r.y]))
const recorded = async () => (await sql<{ cell: string }[]>`SELECT cell FROM bridge_deck_cells`).map(r => r.cell).sort()
/** Stores a deck's anchor as if it had last been written by another cell. */
const anchorIn = (way: number, [x, y]: UnitPoint) =>
  sql`UPDATE bridge_decks SET anchor_x = ${x}, anchor_y = ${y}, anchor = ST_SetSRID(ST_MakePoint(${x / 1e7}, ${y / 1e7}), 4326)
    WHERE ${String(way)} = ANY(ways::text[])`
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
    await buildArea(sql, flat(), [CELL], 0.02)
  }, 60_000)

  afterAll(async () => {
    await sql?.unsafe(`DROP SCHEMA IF EXISTS ${SCHEMA} CASCADE; DROP SCHEMA IF EXISTS osm_replay CASCADE`)
    await sql?.end()
  })

  test('builds one deck per bridge in the cell, and records the cell', async () => {
    expect((await decks()).map(d => d.ways).sort()).toEqual([['2', '3'], ['5']])
    expect(await recorded()).toEqual([CELL.join(',')])
  })

  describe('a cycle that only rewrites a road', () => {
    test('queues nothing when the road is as it was', async () => {
      await replay(['way/4'], [], 'SELECT 1')
      expect(await queued()).toBe(0)
    })

    test('queues the deck it meets when the diff edited it, once however often', async () => {
      await replay(['way/4'], ['way/4'], 'SELECT 1')
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

    test('queues the new bridge and the decks the change touched', async () => {
      // Bridge 6 as it is now; the decks of 2+3 and of 5 as they were.
      expect(await queued()).toBe(3)
    })

    test('queues nothing that reaches no recorded cell, like bridge 7', async () => {
      const [{ n }] = await sql`SELECT count(*)::int AS n FROM detail_dirty d
        WHERE NOT EXISTS (SELECT 1 FROM bridge_deck_cells c WHERE c.box && d.box)`
      expect(n).toBe(0)
    })

    test('rebuilds them, and drops what lies where no decks were built', async () => {
      expect(await updateDecks(sql, flat(), 100)).toBe(0)
      expect(await queued()).toBe(0)
      const all = (await decks()).map(d => d.ways)
      expect(all).toContainEqual(['2'])
      expect(all).toContainEqual(['6'])
      expect(all.flat()).not.toContain('3')
      expect(all.flat()).not.toContain('5')
      expect(all.flat()).not.toContain('7')
    }, 60_000)

    test('leaves exactly what a full rebuild builds, with nothing doubled or left over', async () => {
      const incremental = await snapshot()
      await sql`TRUNCATE bridge_decks`
      await buildArea(sql, flat(), [CELL], 0.02)
      expect(await snapshot()).toEqual(incremental)
    }, 60_000)
  })

  describe('where an update writes', () => {
    test('drops an entry outside the recorded cells', async () => {
      await enqueue(-80.8195, 35.2295, -80.8185, 35.2305)
      expect(await updateDecks(sql, flat(), 100)).toBe(0)
      expect(await queued()).toBe(0)
      expect(await ofWay(8)).toHaveLength(0)
    }, 60_000)

    test('stores nothing past the edge of a recorded cell its own cell straddles', async () => {
      const before = await snapshot()
      // Crosses the built cell's east edge, into the update cell that holds bridge 8.
      await enqueue(-80.8205, 35.2295, -80.8185, 35.2305)
      expect(await updateDecks(sql, flat(), 100)).toBe(0)
      expect(await ofWay(8)).toHaveLength(0)
      expect(await snapshot()).toEqual(before)
    }, 60_000)
  })

  describe('a deck whose id another cell holds', () => {
    test('is refused whole, recording nothing', async () => {
      await sql`DELETE FROM bridge_deck_cells`
      const [w, s] = build.box(EAST)
      await anchorIn(2, [w, s])
      const before = await snapshot()
      const err = await rebuildBoxes(sql, flat(), [CELL], [CELL]).catch(e => e)
      expect(err).toBeInstanceOf(DeckConflict)
      expect(await snapshot()).toEqual(before)
      expect(await recorded()).toEqual([])
    }, 60_000)

    test('Build takes in only the recorded part of the holding cell, and records only its own', async () => {
      // An earlier build covered only the west half of the cell holding deck 2's id.
      const [w, s, , n] = build.box(EAST)
      const half = [w, s, w + 100_000, n]
      await sql`INSERT INTO bridge_deck_cells (cell, w, s, e, n, box) VALUES (${half.join(',')}, ${half[0]}, ${half[1]}, ${half[2]}, ${half[3]},
        ST_MakeEnvelope(${half[0] / 1e7}, ${half[1] / 1e7}, ${half[2] / 1e7}, ${half[3] / 1e7}, 4326))`
      const { clashes } = await buildArea(sql, flat(), [CELL], 0.02)
      expect(clashes).toBe(0)
      expect(await anchorOf(2)).toEqual([A])
      expect(await recorded()).toEqual([CELL.join(','), half.join(',')].sort())
    }, 60_000)

    test('Build carries a deck that moved out of its cell to where it lies now', async () => {
      await buildArea(sql, flat(), [build.box(EAST)], 0.02)
      const [w, s] = CELL
      await anchorIn(8, [w + 10, s + 10])
      const { built } = await buildArea(sql, flat(), [CELL], 0.02)
      expect(built).toBe(2)
      expect(await anchorOf(8)).toEqual([EAST])
    }, 60_000)

    // Each deck stored as if the other cell had written it last.
    const swap = async () => {
      const [a, b] = [build.box(A), build.box(EAST)]
      await anchorIn(2, [b[0] + 10, b[1] + 10])
      await anchorIn(8, [a[0] + 10, a[1] + 10])
    }

    test('settles two cells holding each other\'s ids, which neither can alone', async () => {
      await swap()
      expect(await rebuildBoxes(sql, flat(), [build.box(A)]).catch(e => e)).toBeInstanceOf(DeckConflict)
      expect(await rebuildBoxes(sql, flat(), [build.box(EAST)]).catch(e => e)).toBeInstanceOf(DeckConflict)
      const { keys } = await rebuildCells(sql, flat(), [EAST], BUILD)
      expect(keys).toEqual([EAST, A])
      expect(await anchorOf(2)).toEqual([A])
      expect(await anchorOf(8)).toEqual([EAST])
    }, 60_000)

    test('settles them in an update too', async () => {
      await swap()
      await enqueue(-80.8335, 35.2245, -80.8325, 35.2255)
      expect(await updateDecks(sql, flat(), 100)).toBe(0)
      expect(await anchorOf(2)).toEqual([A])
      expect(await anchorOf(8)).toEqual([EAST])
      const [{ n, ids }] = await sql`SELECT count(*)::int AS n, count(DISTINCT id)::int AS ids FROM bridge_decks`
      expect(n).toBe(ids)
    }, 60_000)
  })

  describe('a road markings queue from before detail_dirty', () => {
    test('moves over with the failed runs it counted', async () => {
      await sql.unsafe(`CREATE TABLE road_markings_dirty (box geometry(Polygon, 4326), queued_at timestamptz DEFAULT now(), attempts int);
        INSERT INTO road_markings_dirty (box, attempts) VALUES (ST_MakeEnvelope(0, 0, 1, 1, 4326), 2);`)
      await sql.unsafe(file('detail-queue-table.sql'))
      const rows = await sql`SELECT attempts FROM detail_dirty WHERE layer = 'road_markings'`
      expect(rows.map(r => r.attempts)).toEqual([2])
      expect((await sql`SELECT to_regclass('road_markings_dirty') AS t`)[0].t).toBeNull()
      await sql`DELETE FROM detail_dirty`
    })
  })

  describe('setting up the queue in a replication cycle', () => {
    let other: postgres.Sql
    beforeAll(async () => {
      other = postgres(DATABASE_URL!, { max: 1, onnotice: () => {}, connection: { search_path: 'detail_queue_test, public' } })
      await other.unsafe('DROP SCHEMA IF EXISTS detail_queue_test CASCADE; CREATE SCHEMA detail_queue_test')
    })
    afterAll(async () => {
      await other.unsafe('DROP SCHEMA IF EXISTS detail_queue_test CASCADE')
      await other.end()
    })
    const exists = async (table: string) => (await other`SELECT to_regclass(${table}) IS NOT NULL AS t`)[0].t

    test('creates nothing where no layer has been built', async () => {
      await other.unsafe(file('detail-queue-table.sql'))
      expect(await exists('detail_dirty')).toBe(false)
    })

    test('reports a failure rather than raising it, so the cycle commits', async () => {
      await other.unsafe(`CREATE TABLE road_markings_dirty (box text, queued_at timestamptz DEFAULT now());
        INSERT INTO road_markings_dirty (box) VALUES ('not a box')`)
      await other.unsafe(file('detail-queue-table.sql'))
      expect(await exists('detail_dirty')).toBe(false)
      expect(await exists('road_markings_dirty')).toBe(true)
    })
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
