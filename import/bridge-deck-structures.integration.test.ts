/**
 * Builds bridge decks for structures that carry more than one way, in a
 * throwaway schema on ground falling 1 in 10 to the south: a street with a tram running in each
 * carriageway, twin tracks on one viaduct, and a carriageway meeting its slip
 * road at a gore on the bridge.
 *
 * Run: BARRELMAN_INTEGRATION_TESTS=1 DATABASE_URL=postgresql://barrelman:barrelman@localhost:5434/scratch \
 *      bun test import/bridge-deck-structures.integration.test.ts
 */
import { afterAll, beforeAll, describe, expect, test } from 'bun:test'
import postgres from 'postgres'
import { readFileSync } from 'fs'
import { join } from 'path'
import { Dem } from '../src/lib/bridge-decks/dem'
import { grid, toUnits } from '../src/lib/bridge-decks/grid'
import { buildArea } from './generate-bridge-decks'

const DATABASE_URL = process.env.BARRELMAN_INTEGRATION_TESTS ? process.env.DATABASE_URL : undefined
const SCHEMA = 'bridge_deck_structures_test'
const CELL = grid(toUnits(0.02)).box('-4042,1761')
const Y = 35.225
/** Degrees of latitude in a metre. */
const M = 1 / 110574

const line = (...pts: [number, number][]) => `LINESTRING(${pts.map(p => p.join(' ')).join(', ')})`
const bridge = { bridge: 'yes', layer: '1' }
const ways: [number, string, Record<string, string>][] = [
  // A street of two carriageways 9 m apart, a tram in each, over a river.
  [1, line([-80.836, Y], [-80.835, Y]), { highway: 'tertiary', oneway: 'yes', ...bridge }],
  [2, line([-80.835, Y + 9 * M], [-80.836, Y + 9 * M]), { highway: 'tertiary', oneway: 'yes', ...bridge }],
  [3, line([-80.836, Y], [-80.835, Y]), { railway: 'tram', electrified: 'contact_line', ...bridge }],
  [4, line([-80.835, Y + 9 * M], [-80.836, Y + 9 * M]), { railway: 'tram', electrified: 'contact_line', ...bridge }],
  [5, line([-80.8355, Y - 30 * M], [-80.8355, Y + 40 * M]), { waterway: 'river' }],
  // Twin light rail tracks 4 m apart on one viaduct, over a road.
  [10, line([-80.832, 35.23], [-80.829, 35.23]), { railway: 'light_rail', ...bridge }],
  [11, line([-80.832, 35.23 + 4 * M], [-80.829, 35.23 + 4 * M]), { railway: 'light_rail', ...bridge }],
  [12, line([-80.8305, 35.229], [-80.8305, 35.231]), { highway: 'primary' }],
  // A carriageway and its slip road, both on the bridge, leaving one node westward.
  [20, line([-80.826, 35.235], [-80.8275, 35.235]), { highway: 'secondary', oneway: 'yes', ...bridge }],
  [21, line([-80.826, 35.235], [-80.8275, 35.235 + 14 * M]), { highway: 'secondary_link', oneway: 'yes', ...bridge }],
]

// Each 250 m pixel row 25 m lower than the one north of it, so twin tracks 4 m apart stand on ground 0.4 m apart.
const sloped = () => new Dem(async () => ({ size: 4, data: Float32Array.from({ length: 16 }, (_, i) => 200 - 25 * Math.floor(i / 4)) }))
const run = DATABASE_URL ? describe : describe.skip
let sql: postgres.Sql

const file = (name: string) => readFileSync(join(import.meta.dir, name), 'utf8')
type Row = { id: string; kind: string; ways: string[]; heights: number[]; edges: number[] }
const decks = async () => sql<Row[]>`SELECT id, kind, ways::text[] AS ways, heights, edges FROM bridge_decks ORDER BY id`
const holding = async (way: number) => (await decks()).filter(d => d.ways.includes(String(way)))

run('bridge decks for shared structures', () => {
  beforeAll(async () => {
    sql = postgres(DATABASE_URL!, { max: 1, onnotice: () => {}, connection: { search_path: `${SCHEMA}, public` } })
    const [{ shared }] = await sql`SELECT to_regclass('public.bridge_decks') IS NOT NULL AS shared`
    if (shared) throw new Error('public has bridge tables of its own: point DATABASE_URL at an empty scratch database')
    const ddl = file('create-detail-views.sql')
    const tables = ddl.slice(ddl.indexOf('CREATE OR REPLACE FUNCTION bridge_deck_class'), ddl.indexOf('CREATE OR REPLACE VIEW bridge_deck_tiles'))
    await sql.unsafe(`DROP SCHEMA IF EXISTS ${SCHEMA} CASCADE; CREATE SCHEMA ${SCHEMA};
      CREATE TABLE geo_places (id text, osm_type char(1), osm_id bigint, name text, categories text[], tags jsonb,
        geom geometry(Geometry, 4326), geom_type text, admin_level int);
      ${tables}`)
    for (const [id, wkt, tags] of ways) {
      const categories = Object.entries(tags).filter(([k]) => ['highway', 'railway', 'waterway'].includes(k)).map(([k, v]) => `${k}/${v}`)
      await sql`INSERT INTO geo_places (id, osm_type, osm_id, categories, tags, geom, geom_type)
        VALUES (${'way/' + id}, 'W', ${id}, ${categories}, ${sql.json(tags)}, ST_GeomFromText(${wkt}, 4326), 'line')`
    }
    await buildArea(sql, sloped(), [CELL], 0.02)
  }, 60_000)

  afterAll(async () => {
    await sql?.unsafe(`DROP SCHEMA IF EXISTS ${SCHEMA} CASCADE`)
    await sql?.end()
  })

  test('a tram running in a carriageway is carried on its road deck, not a deck of its own', async () => {
    const [a] = await holding(3)
    const [b] = await holding(4)
    expect(a.kind).toBe('road')
    expect(a.ways).toContain('1')
    expect(b.ways).toContain('2')
    expect((await decks()).filter(d => d.kind === 'rail' && (d.ways.includes('3') || d.ways.includes('4')))).toHaveLength(0)
  })

  test('twin tracks on one viaduct stand at one height', async () => {
    const at = async (way: number, lng: number) => {
      const [{ z }] = await sql<{ z: number }[]>`
        SELECT heights[round(ST_LineLocatePoint(geom, ST_SetSRID(ST_MakePoint(${lng}, 35.23), 4326)) * length / step)::int + 1] AS z
        FROM bridge_decks WHERE ${String(way)} = ANY(ways::text[])`
      return z
    }
    for (const lng of [-80.8311, -80.8305, -80.8295]) expect(Math.abs((await at(10, lng)) - (await at(11, lng)))).toBeLessThan(0.05)
  })

  test('a carriageway is never folded back onto its slip road', async () => {
    const [main] = await holding(20)
    const [slip] = await holding(21)
    expect(main.id).not.toBe(slip.id)
    expect(main.ways).toEqual(['20'])
  })
})
