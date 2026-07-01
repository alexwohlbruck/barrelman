import { describe, expect, test } from 'bun:test'
import { selectFeeds, type GtfsFeedInfo } from './gtfs.service'

const feed = (over: Partial<GtfsFeedInfo>): GtfsFeedInfo => ({
  feedId: '1',
  onestopId: 'f-a',
  name: 'A',
  url: 'http://x/a.zip',
  ...over,
})

describe('selectFeeds (DMFR-style curated selection)', () => {
  const feeds = [
    feed({ feedId: '1', onestopId: 'f-mta' }),
    feed({ feedId: '2', onestopId: 'f-cats' }),
    feed({
      feedId: '3',
      onestopId: 'f-locked',
      license: { redistribution_allowed: 'no' },
    }),
  ]

  test('passes everything through by default', () => {
    expect(selectFeeds(feeds)).toHaveLength(3)
  })

  test('excludes feeds that disallow redistribution', () => {
    const out = selectFeeds(feeds, { excludeUnredistributable: true })
    expect(out.map(f => f.onestopId)).toEqual(['f-mta', 'f-cats'])
  })

  test('allow-list keeps only listed feeds (by onestop or feed id)', () => {
    expect(selectFeeds(feeds, { allow: ['f-cats'] }).map(f => f.onestopId)).toEqual(['f-cats'])
    expect(selectFeeds(feeds, { allow: ['1'] }).map(f => f.onestopId)).toEqual(['f-mta'])
  })

  test('deny-list always drops listed feeds and wins over allow', () => {
    expect(selectFeeds(feeds, { deny: ['f-mta'] }).map(f => f.onestopId)).toEqual(['f-cats', 'f-locked'])
    expect(selectFeeds(feeds, { allow: ['f-mta'], deny: ['f-mta'] })).toHaveLength(0)
  })

  test('filters compose', () => {
    const out = selectFeeds(feeds, { excludeUnredistributable: true, deny: ['f-cats'] })
    expect(out.map(f => f.onestopId)).toEqual(['f-mta'])
  })
})
