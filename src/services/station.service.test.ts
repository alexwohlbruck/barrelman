/**
 * Regression guard for the SQL injection that `/transit/station/:feedId/:stopId`
 * carried: both path segments were spliced into a `sql.raw` string with no
 * escaping, so `x' OR '1'='1` returned another agency's station and `'; UPDATE …`
 * ran as a second statement.
 *
 * The assertion is deliberately about SHAPE, not about results: whatever the
 * caller passes must arrive as a bind parameter and must never appear in the
 * SQL text. That is the property that makes the endpoint safe — a parametrized
 * query goes out on Postgres's extended protocol, which rejects multiple
 * statements outright. Asserting "returns null for a quote" would pass again
 * the moment someone reintroduces string building with escaping that looks
 * right; asserting the payload never reaches the SQL text would not.
 */
import { describe, test, expect, mock } from 'bun:test'
import { drizzle } from 'drizzle-orm/postgres-js'

/** Every statement drizzle handed to the driver, with its bind parameters. */
const statements: Array<{ sql: string; params: unknown[] }> = []

// A postgres-js stand-in that records instead of connecting. Using a real
// drizzle instance on top means the SQL under test is the SQL this codebase
// actually generates, not a reconstruction.
/**
 * What each query resolves to, so a test can stand the service up over a
 * database that does or does not have the optional station-link relations.
 * Keyed by a substring of the generated SQL; first match wins.
 */
let responses: Array<[string, unknown[]]> = []

const client: any = () => Promise.resolve([])
client.unsafe = (text: string, params: unknown[] = []) => {
  statements.push({ sql: text, params })
  const rows = responses.find(([needle]) => text.includes(needle))?.[1] ?? []
  const result: any = Promise.resolve(rows)
  result.values = () => Promise.resolve(rows)
  result.execute = () => Promise.resolve(rows)
  return result
}
client.options = { parsers: {}, serializers: {} }

// Spread the real module: a bare replacement drops `connection` and the
// ensure*Schema helpers for every test file loaded after this one, which
// fails whichever suite imports them next rather than this one.
const actualDb = await import('../db')

mock.module('../db', () => ({
  ...actualDb, db: drizzle(client), connection: client }))

const { getStationDetail, getNearestEntrance, __resetRelationCacheForTests } =
  await import('./station.service')

/** Payloads that would each have been catastrophic through string building. */
const PAYLOADS = [
  `x' OR '1'='1`,
  `x'; UPDATE accounts_users SET role='admin'; --`,
  `x' UNION SELECT NULL,NULL,NULL,NULL,NULL,NULL --`,
  `x'--`,
  `Grand Central's Platform`, // a legitimate apostrophe must work the same way
]

describe('station.service parametrization', () => {
  test('feedId and stopId never reach the SQL text', async () => {
    for (const payload of PAYLOADS) {
      statements.length = 0
      await getStationDetail(payload, payload)

      expect(statements.length).toBeGreaterThan(0)
      for (const { sql } of statements) {
        expect(sql).not.toContain(payload)
      }
      // …and it did travel, as a parameter.
      expect(statements.some((s) => s.params.includes(payload))).toBe(true)
    }
  })

  test('no statement is ever built by concatenating a quote', async () => {
    statements.length = 0
    await getStationDetail(`a'b`, `c'd`)
    for (const { sql } of statements) {
      // A bare apostrophe in generated SQL is the signature of the old
      // `'${value}'` pattern. Drizzle emits $1/$2 placeholders instead.
      expect(sql).not.toContain(`a'b`)
      expect(sql).not.toContain(`c'd`)
    }
  })

  test('getNearestEntrance binds coordinates rather than embedding them', async () => {
    statements.length = 0
    await getNearestEntrance(40.7527, -73.9772, 500, false)

    expect(statements.length).toBeGreaterThan(0)
    const bound = statements.flatMap((s) => s.params)
    expect(bound).toContain(40.7527)
    expect(bound).toContain(-73.9772)
  })
})

// ── Optional station-link relations ──────────────────────────────────────────

describe('station.service on an instance without the station-link import', () => {
  /**
   * `station_entrances` and `station_buildings` are materialized views built by
   * `import/create-station-links.sql`, which is a separate step from the GTFS
   * import. A GTFS-only instance has stations but neither view — a normal state,
   * and one that made GET /transit/station/:feedId/:stopId answer 500 with
   * `relation "station_entrances" does not exist` in the response body.
   *
   * getNearestEntrance had always probed for its own optional relation
   * (stop_area_members); getStationDetail simply did not.
   */
  test('reports no entrances or buildings instead of throwing', async () => {
    __resetRelationCacheForTests()
    statements.length = 0
    // A station exists; neither optional view does.
    responses = [
      ['to_regclass', [{ ok: false }]],
      ['gtfs_stops', [['S1', 'F1', 'Test Station', 40.7, -73.9]]],
    ]

    const detail = await getStationDetail('F1', 'S1')

    expect(detail).not.toBeNull()
    expect(detail!.stopName).toBe('Test Station')
    expect(detail!.entrances).toEqual([])
    expect(detail!.buildings).toEqual([])

    // It asked rather than assumed, and never queried the missing views.
    const text = statements.map((s) => s.sql).join('\n')
    expect(statements.some((s) => s.params.includes('station_entrances'))).toBe(true)
    expect(statements.some((s) => s.params.includes('station_buildings'))).toBe(true)
    expect(text).not.toContain('from "station_entrances"')
    expect(text).not.toContain('from "station_buildings"')

    responses = []
    __resetRelationCacheForTests()
  })

  test('queries the views when they are present', async () => {
    __resetRelationCacheForTests()
    statements.length = 0
    responses = [
      ['to_regclass', [{ ok: true }]],
      ['gtfs_stops', [['S1', 'F1', 'Test Station', 40.7, -73.9]]],
    ]

    await getStationDetail('F1', 'S1')

    const text = statements.map((s) => s.sql).join('\n').toLowerCase()
    expect(text).toContain('station_entrances')
    expect(text).toContain('station_buildings')

    responses = []
    __resetRelationCacheForTests()
  })
})
