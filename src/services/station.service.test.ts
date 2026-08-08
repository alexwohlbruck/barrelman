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
const client: any = () => Promise.resolve([])
client.unsafe = (text: string, params: unknown[] = []) => {
  statements.push({ sql: text, params })
  const result: any = Promise.resolve([])
  result.values = () => Promise.resolve([])
  result.execute = () => Promise.resolve([])
  return result
}
client.options = { parsers: {}, serializers: {} }

mock.module('../db', () => ({ db: drizzle(client), connection: client }))

const { getStationDetail, getNearestEntrance } = await import('./station.service')

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
