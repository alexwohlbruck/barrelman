/**
 * Resolving a bare GTFS route id to the feed that means it HERE.
 *
 * The map hands us "2" and a point. That id is the IRT Seventh Avenue line
 * in one feed and the Long Island Rail Road's Ronkonkoma branch in another,
 * so the two properties under test are: proximity decides, and the mode
 * class is a preference that breaks ties without ever emptying the answer.
 */
import { describe, test, expect, beforeEach } from 'bun:test'
import { mock } from 'bun:test'
import { drizzle } from 'drizzle-orm/postgres-js'

const statements: Array<{ sql: string; params: unknown[] }> = []

/** Rows the spatial (LATERAL) query returns, and the id-only fallback's. */
let NEARBY_ROWS: any[] = []
let FALLBACK_ROWS: any[] = []

// A postgres-js stand-in that records instead of connecting, so the
// assertions see the SQL this codebase actually generates.
const client: any = () => Promise.resolve([])
client.unsafe = (text: string, params: unknown[] = []) => {
  statements.push({ sql: text, params })
  const rows = text.includes('LATERAL') ? NEARBY_ROWS : FALLBACK_ROWS
  const result: any = Promise.resolve(rows)
  result.values = () => Promise.resolve(rows)
  result.execute = () => Promise.resolve(rows)
  return result
}
client.options = { parsers: {}, serializers: {} }

mock.module('../db', () => ({ db: drizzle(client), connection: client }))

const { resolveRoute, pickRoute } = await import('./route-detail.service')

const row = (feedId: string, routeType: number, distance?: number) => ({
  feed_id: feedId,
  route_id: '2',
  route_short_name: '2',
  route_long_name: null,
  route_type: routeType,
  ...(distance == null ? {} : { distance }),
})

beforeEach(() => {
  statements.length = 0
  NEARBY_ROWS = []
  FALLBACK_ROWS = []
})

describe('resolveRoute', () => {
  test('answers with the nearest feed carrying the id', async () => {
    NEARBY_ROWS = [row('mta-subway', 1, 84.2), row('lirr', 2, 1830.5)]

    const result = await resolveRoute({ routeId: '2', lat: 40.71, lng: -74.0 })

    expect(result).toMatchObject({ feedId: 'mta-subway', routeId: '2', distance: 84.2 })
    expect(statements).toHaveLength(1) // no fallback query when the point answers
  })

  test('prefers the requested mode class over a nearer route of another', async () => {
    NEARBY_ROWS = [row('lirr', 2, 120.4), row('mta-subway', 1, 260.9)]

    const result = await resolveRoute({ routeId: '2', lat: 40.75, lng: -73.99, mode: 'metro' })

    expect(result?.feedId).toBe('mta-subway')
  })

  test('a mode nothing nearby matches still returns the nearest route', async () => {
    // Feeds mistype their routes; a mode filter would answer "no such line"
    // where the nearest stop is plainly the one being pointed at.
    NEARBY_ROWS = [row('roosevelt-tram', 3, 12.1)]

    const result = await resolveRoute({ routeId: '2', lat: 40.76, lng: -73.96, mode: 'aerial' })

    expect(result?.feedId).toBe('roosevelt-tram')
  })

  test('falls back to the id alone when no stop is within the radius', async () => {
    FALLBACK_ROWS = [row('rural-rail', 2)]

    const result = await resolveRoute({ routeId: '2', lat: 44.1, lng: -72.5 })

    expect(result).toMatchObject({ feedId: 'rural-rail', distance: null })
    expect(statements).toHaveLength(2)
  })

  test('refuses to guess when the fallback is ambiguous', async () => {
    FALLBACK_ROWS = [row('feed-a', 3), row('feed-b', 3)]

    expect(await resolveRoute({ routeId: '2', lat: 44.1, lng: -72.5 })).toBeNull()
  })

  test('an empty route id never reaches the database', async () => {
    expect(await resolveRoute({ routeId: '', lat: 40.71, lng: -74.0 })).toBeNull()
    expect(statements).toHaveLength(0)
  })

  test('the radius rides as a bound parameter', async () => {
    NEARBY_ROWS = [row('mta-subway', 1, 10)]

    await resolveRoute({ routeId: '2', lat: 40.71, lng: -74.0, radius: 750 })

    expect(statements[0].params).toContain(750)
  })
})

describe('pickRoute', () => {
  test('no candidates, no answer', () => {
    expect(pickRoute([], 'metro')).toBeNull()
  })

  test('without a mode it is simply the first (nearest)', () => {
    const candidates = [
      { feedId: 'a', routeId: '2', routeShortName: '2', routeLongName: null, routeType: 3, distance: 5 },
      { feedId: 'b', routeId: '2', routeShortName: '2', routeLongName: null, routeType: 1, distance: 9 },
    ]
    expect(pickRoute(candidates)?.feedId).toBe('a')
  })
})
