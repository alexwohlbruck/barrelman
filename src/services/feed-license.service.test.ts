import { describe, test, expect, afterEach, beforeEach } from 'bun:test'
import {
  __setFeedTermsForTests,
  attributionFor,
  excludedFeedIds,
  invalidateFeedTerms,
  servableOnly,
} from './feed-license.service'

const saved = process.env.GTFS_LICENCE_POLICY

beforeEach(() => {
  __setFeedTermsForTests({
    mta: { name: 'MTA New York City Transit', url: 'https://www.mta.info/developers/terms', commercialUseAllowed: 'yes' },
    closed: { name: 'Closed Transit', attributionText: 'Data © Closed Transit', redistributionAllowed: 'no' },
    silent: { name: 'Silent Bus' },
  })
})

afterEach(() => {
  invalidateFeedTerms()
  if (saved === undefined) delete process.env.GTFS_LICENCE_POLICY
  else process.env.GTFS_LICENCE_POLICY = saved
})

describe('excludedFeedIds', () => {
  test('is empty when the policy is off', async () => {
    delete process.env.GTFS_LICENCE_POLICY
    expect([...(await excludedFeedIds())]).toEqual([])
  })

  test('holds only feeds that explicitly forbid, under `commercial`', async () => {
    process.env.GTFS_LICENCE_POLICY = 'commercial'
    expect([...(await excludedFeedIds())]).toEqual(['closed'])
  })
})

describe('servableOnly', () => {
  const rows = [{ feedId: 'mta' }, { feedId: 'closed' }, { feedId: 'silent' }, { feedId: 'unlisted' }]

  test('drops rows of excluded feeds and keeps unknown ones', async () => {
    process.env.GTFS_LICENCE_POLICY = 'commercial'
    expect((await servableOnly(rows, (r) => r.feedId)).map((r) => r.feedId)).toEqual(['mta', 'silent', 'unlisted'])
  })

  test('returns the same rows when the policy is off', async () => {
    delete process.env.GTFS_LICENCE_POLICY
    expect(await servableOnly(rows, (r) => r.feedId)).toBe(rows)
  })
})

describe('attributionFor', () => {
  test('lists each feed once, in first-seen order', async () => {
    expect(await attributionFor(['closed', 'mta', 'closed', undefined, 'mta'])).toEqual([
      { feedId: 'closed', agency: 'Closed Transit', text: 'Data © Closed Transit', url: null },
      { feedId: 'mta', agency: 'MTA New York City Transit', text: null, url: 'https://www.mta.info/developers/terms' },
    ])
  })

  test('names a feed the table does not know from the agency the response carries', async () => {
    expect(await attributionFor(['unlisted'], new Map([['unlisted', 'CATS']]))).toEqual([
      { feedId: 'unlisted', agency: 'CATS', text: null, url: null },
    ])
  })

  test('is empty for a response with no transit', async () => {
    expect(await attributionFor([])).toEqual([])
  })
})
