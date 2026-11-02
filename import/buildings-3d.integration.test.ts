/**
 * Builds buildings_3d over synthetic part-mapped buildings in a throwaway
 * schema, to check how overlapping building:parts are cut.
 *
 * Run: BARRELMAN_INTEGRATION_TESTS=1 DATABASE_URL=postgresql://barrelman:barrelman@localhost:5434/scratch \
 *      bun test import/buildings-3d.integration.test.ts
 */
import { afterAll, beforeAll, describe, expect, test } from 'bun:test'
import postgres from 'postgres'
import { readFileSync } from 'fs'
import { join } from 'path'

const DATABASE_URL = process.env.BARRELMAN_INTEGRATION_TESTS ? process.env.DATABASE_URL : undefined
const SCHEMA = 'buildings_3d_test'

const box = (x0: number, y0: number, x1: number, y1: number) =>
  `POLYGON((${x0} ${y0}, ${x1} ${y0}, ${x1} ${y1}, ${x0} ${y1}, ${x0} ${y0}))`

const shapes: [number, string, Record<string, string>][] = [
  // A podium filling its outline, with a tower on one side of it.
  [1, box(0, 0, 0.001, 0.001), { building: 'commercial', height: '200' }],
  [2, box(0, 0, 0.001, 0.001), { 'building:part': 'yes', height: '15' }],
  [3, box(0, 0, 0.0005, 0.0005), { 'building:part': 'yes', height: '200' }],
  // A setback standing on its base, and a spire starting partway up the base.
  [10, box(0.01, 0, 0.011, 0.001), { 'building:part': 'yes', height: '20' }],
  [11, box(0.01, 0, 0.0105, 0.0005), { 'building:part': 'yes', height: '40', min_height: '20' }],
  [12, box(0.0102, 0.0002, 0.0103, 0.0003), { 'building:part': 'yes', height: '60', min_height: '5' }],
  // Neighbours whose shared wall is drawn a little off.
  [20, box(0.02, 0, 0.021, 0.001), { 'building:part': 'yes', height: '10' }],
  [21, box(0.02099, 0, 0.022, 0.001), { 'building:part': 'yes', height: '30' }],
  // The same part mapped twice, and a part wholly inside a taller one.
  [30, box(0.03, 0, 0.031, 0.001), { 'building:part': 'yes', height: '10' }],
  [31, box(0.03, 0, 0.031, 0.001), { 'building:part': 'yes', height: '10' }],
  [32, box(0.0302, 0.0002, 0.0303, 0.0003), { 'building:part': 'yes', height: '8' }],
]

const run = DATABASE_URL ? describe : describe.skip
let sql: postgres.Sql
const file = (name: string) => readFileSync(join(import.meta.dir, name), 'utf8')
const area = async (osmId: number) =>
  (await sql`SELECT ST_Area(geom) / 1e-6 AS a FROM buildings_3d WHERE id = ${'way/' + osmId}`)[0]?.a as number | undefined

run('buildings_3d part overlaps', () => {
  beforeAll(async () => {
    sql = postgres(DATABASE_URL!, { max: 1, onnotice: () => {}, connection: { search_path: `${SCHEMA}, public` } })
    const ddl = file('create-detail-views.sql')
    const start = ddl.indexOf('-- ─── 3D buildings')
    await sql.unsafe(`DROP SCHEMA IF EXISTS ${SCHEMA} CASCADE; CREATE SCHEMA ${SCHEMA};
      CREATE TABLE geo_places (id text, osm_type char(1), osm_id bigint, tags jsonb,
        geom geometry(Geometry, 4326), geom_type text);
      ${ddl.slice(start, ddl.indexOf('-- ─── ', start + 1))}`)
    for (const [id, wkt, tags] of shapes)
      await sql`INSERT INTO geo_places (id, osm_type, osm_id, tags, geom, geom_type)
        VALUES (${'way/' + id}, 'W', ${id}, ${sql.json(tags)}, ST_GeomFromText(${wkt}, 4326), 'area')`
    await sql`REFRESH MATERIALIZED VIEW buildings_3d`
  }, 60_000)

  afterAll(async () => {
    await sql?.unsafe(`DROP SCHEMA IF EXISTS ${SCHEMA} CASCADE`)
    await sql?.end()
  })

  test('cuts the tower out of the podium and leaves the tower whole', async () => {
    expect(await area(2)).toBeCloseTo(0.75, 6)
    expect(await area(3)).toBeCloseTo(0.25, 6)
    const [outline] = await sql`SELECT hide_3d FROM buildings_3d WHERE id = 'way/1'`
    expect(outline.hide_3d).toBe(true)
  })

  test('cuts only parts the taller one spans from the bottom up', async () => {
    expect(await area(10)).toBeCloseTo(1, 6)
    expect(await area(11)).toBeCloseTo(0.24, 6)
    expect(await area(12)).toBeCloseTo(0.01, 6)
  })

  test('ignores a sliver of overlap along a shared wall', async () => {
    expect(await area(20)).toBeCloseTo(1, 6)
  })

  test('drops a duplicate and a part hidden inside a taller one', async () => {
    const rows = await sql`SELECT id FROM buildings_3d WHERE id IN ('way/30', 'way/31', 'way/32')`
    expect(rows.map(r => r.id)).toEqual(['way/30'])
  })
})
