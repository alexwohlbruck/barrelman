/**
 * Label rules against a real database. Runs with BARRELMAN_INTEGRATION_TESTS=1
 * and DATABASE_URL pointing at a throwaway database.
 */
import { afterAll, beforeAll, describe, expect, test } from 'bun:test'

const enabled = Boolean(process.env.BARRELMAN_INTEGRATION_TESTS && process.env.DATABASE_URL)

describe.skipIf(!enabled)('provider match labels', () => {
  let service: typeof import('./provider-matches.service')
  let sql: typeof import('../db').connection
  const place = { osmType: 'way' as const, osmId: '575449026' }
  const label = async () => (await service.getProviderMatches(place)).find((m) => m.provider === 'google_hotels')?.label

  beforeAll(async () => {
    ;({ connection: sql } = await import('../db'))
    await sql`DELETE FROM place_provider_matches WHERE osm_id = 575449026`.catch(() => {})
    service = await import('./provider-matches.service')
  })

  afterAll(async () => {
    await sql`DELETE FROM place_provider_matches WHERE osm_id = 575449026`
  })

  test('stores a label with the match', async () => {
    await service.setProviderMatch(place, 'google_hotels', { providerPlaceId: 'tok-1', confirmed: false, label: ' EVEN Hotel ' })
    expect(await label()).toBe('EVEN Hotel')
  })

  test('keeps the label when a write leaves it out and the listing is unchanged', async () => {
    await service.setProviderMatch(place, 'google_hotels', { providerPlaceId: 'tok-1', confirmed: true })
    expect(await label()).toBe('EVEN Hotel')
  })

  test('clears the label when the listing changes without one', async () => {
    await service.setProviderMatch(place, 'google_hotels', { providerPlaceId: 'tok-2', confirmed: true })
    expect(await label()).toBeNull()
  })

  test('clears it on request', async () => {
    await service.setProviderMatch(place, 'google_hotels', { providerPlaceId: 'tok-2', confirmed: true, label: 'Hotel' })
    await service.setProviderMatch(place, 'google_hotels', { providerPlaceId: 'tok-2', confirmed: true, label: null })
    expect(await label()).toBeNull()
  })
})
