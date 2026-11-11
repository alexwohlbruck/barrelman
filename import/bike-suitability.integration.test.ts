/**
 * Scores synthetic ways with the bike suitability functions.
 *
 * Run: BARRELMAN_INTEGRATION_TESTS=1 DATABASE_URL=postgresql://barrelman:barrelman@localhost:5434/scratch \
 *      bun test import/bike-suitability.integration.test.ts
 */
import { afterAll, beforeAll, describe, expect, test } from 'bun:test'
import postgres from 'postgres'
import { readFileSync } from 'fs'
import { join } from 'path'

const DATABASE_URL = process.env.BARRELMAN_INTEGRATION_TESTS ? process.env.DATABASE_URL : undefined
const SCHEMA = 'bike_suitability_test'

const run = DATABASE_URL ? describe : describe.skip
let sql: postgres.Sql

type Tags = Record<string, string>

const score = async (tags: Tags) =>
  (await sql`SELECT bike_suitability_score(${sql.json(tags)}::jsonb, bike_facility(${sql.json(tags)}::jsonb)) AS s`)[0].s as number
const facility = async (tags: Tags) =>
  (await sql`SELECT bike_facility(${sql.json(tags)}::jsonb) AS f`)[0].f as string
const rideable = async (tags: Tags) =>
  (await sql`SELECT bike_rideable(${sql.json(tags)}::jsonb) AS r`)[0].r as boolean

const arterial = { highway: 'primary', lanes: '4', maxspeed: '35 mph' }

run('bike suitability', () => {
  beforeAll(async () => {
    sql = postgres(DATABASE_URL!, { max: 1, onnotice: () => {}, connection: { search_path: `${SCHEMA}, public` } })
    const ddl = readFileSync(join(import.meta.dir, 'create-bike-suitability.sql'), 'utf8')
    await sql.unsafe(`DROP SCHEMA IF EXISTS ${SCHEMA} CASCADE; CREATE SCHEMA ${SCHEMA};
      ${ddl.slice(0, ddl.indexOf('DROP VIEW'))}`)
  })

  afterAll(async () => {
    await sql?.unsafe(`DROP SCHEMA IF EXISTS ${SCHEMA} CASCADE`)
    await sql?.end()
  })

  test('separation from traffic ranks the same arterial', async () => {
    const track = await score({ ...arterial, 'cycleway:right': 'track' })
    const buffered = await score({ ...arterial, 'cycleway:right': 'lane', 'cycleway:right:buffer': 'yes' })
    const lane = await score({ ...arterial, 'cycleway:both': 'lane' })
    const sharrow = await score({ ...arterial, cycleway: 'shared_lane' })
    const none = await score(arterial)
    expect(track).toBeGreaterThan(buffered)
    expect(buffered).toBeGreaterThan(lane)
    expect(lane).toBeGreaterThan(sharrow)
    expect(sharrow).toBeGreaterThan(none)
    expect(none).toBeLessThan(35)
    expect(track).toBeGreaterThanOrEqual(85)
  })

  test('a kerb or posts make a painted lane protected', async () => {
    expect(await facility({ ...arterial, 'cycleway:left': 'lane', 'cycleway:left:separation': 'flex_post' })).toBe('track')
  })

  test('speed and lanes raise stress', async () => {
    const quiet = await score({ highway: 'residential', maxspeed: '20 mph' })
    const fast = await score({ highway: 'residential', maxspeed: '35 mph' })
    const wide = await score({ highway: 'residential', maxspeed: '35 mph', lanes: '4' })
    expect(quiet).toBeGreaterThan(fast)
    expect(fast).toBeGreaterThan(wide)
    expect(quiet).toBeGreaterThanOrEqual(80)
  })

  test('an untagged limit falls back to the class', async () => {
    expect(await score({ highway: 'residential' })).toBeGreaterThan(await score({ highway: 'secondary' }))
  })

  test('a rough surface costs points on a car-free path', async () => {
    const paved = await score({ highway: 'cycleway', surface: 'asphalt' })
    const gravel = await score({ highway: 'cycleway', surface: 'gravel' })
    const rutted = await score({ highway: 'cycleway', surface: 'gravel', smoothness: 'very_bad' })
    expect(paved).toBe(100)
    expect(paved).toBeGreaterThan(gravel)
    expect(gravel).toBeGreaterThan(rutted)
  })

  test('sharing with pedestrians costs less when segregated', async () => {
    const shared = await score({ highway: 'footway', bicycle: 'yes' })
    const segregated = await score({ highway: 'path', bicycle: 'designated', segregated: 'yes' })
    expect(segregated).toBeGreaterThan(shared)
  })

  test('parked cars beside a painted lane cost points', async () => {
    const lane = { highway: 'secondary', maxspeed: '25 mph', 'cycleway:right': 'lane' }
    expect(await score({ ...lane, 'parking:right': 'lane' })).toBeLessThan(await score(lane))
  })

  test('a steep grade costs points', async () => {
    expect(await score({ highway: 'residential', incline: '12%' })).toBeLessThan(await score({ highway: 'residential' }))
  })

  test('ways bikes cannot ride are left off', async () => {
    expect(await rideable({ highway: 'motorway' })).toBe(false)
    expect(await rideable({ highway: 'primary', bicycle: 'no' })).toBe(false)
    expect(await rideable({ highway: 'footway', footway: 'sidewalk' })).toBe(false)
    expect(await rideable({ highway: 'service', service: 'driveway' })).toBe(false)
    expect(await rideable({ highway: 'residential', access: 'private' })).toBe(false)
    expect(await rideable({ highway: 'footway', bicycle: 'yes' })).toBe(true)
    expect(await rideable({ highway: 'residential' })).toBe(true)
  })

  test('scores stay within 0-100', async () => {
    expect(await score({ highway: 'trunk', lanes: '8', maxspeed: '65 mph', hgv: 'designated' })).toBe(0)
    expect(await score({ highway: 'cycleway', lit: 'yes' })).toBe(100)
  })
})
