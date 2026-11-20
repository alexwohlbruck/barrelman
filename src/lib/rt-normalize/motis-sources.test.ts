import { describe, expect, it } from 'bun:test'
import { motisRtSources } from './motis-sources'

const relay = { baseUrl: 'http://barrelman:5001', serviceKey: 'brm_key' }

describe('motisRtSources', () => {
  it('routes a marked URL through the relay once, by its position, with the service key', () => {
    const sources = motisRtSources('5', [
      { url: 'https://mta/alerts', type: 'alerts' },
      { url: 'https://mta/bdfm', type: 'tripUpdates', normalize: true, headers: { 'x-api-key': 'upstream' } },
      { url: 'https://mta/bdfm', type: 'vehiclePositions' },
    ], relay)
    expect(sources).toEqual([
      { url: 'https://mta/alerts', type: 'alerts' },
      { url: 'http://barrelman:5001/gtfs-rt/5/1', headers: { Authorization: 'Bearer brm_key' } },
    ])
  })

  it('routes a URL that redirects through the relay too', () => {
    expect(motisRtSources('280', [{ url: 'https://cap/download/tu', type: 'tripUpdates', redirects: true }], relay))
      .toEqual([{ url: 'http://barrelman:5001/gtfs-rt/280/0', headers: { Authorization: 'Bearer brm_key' } }])
  })

  it('leaves unmarked URLs as published', () => {
    const rt = { url: 'https://agency/tu', headers: { key: 'k' } }
    expect(motisRtSources('9', [rt], relay)).toEqual([rt])
  })

  it('leaves out vehicle positions beside a trip-update URL, which MOTIS never reads', () => {
    expect(motisRtSources('7', [
      { url: 'https://agency/tu', type: 'tripUpdates' },
      { url: 'https://agency/vp', type: 'vehiclePositions' },
      { url: 'https://agency/alerts', type: 'alerts' },
    ], relay).map(s => s.url)).toEqual(['https://agency/tu', 'https://agency/alerts'])
  })

  it('keeps a vehicle-position URL on a feed with no trip updates, since it may carry both', () => {
    expect(motisRtSources('8', [{ url: 'https://transloc/vehicles', type: 'vehiclePositions' }], relay).map(s => s.url))
      .toEqual(['https://transloc/vehicles'])
  })

  it('leaves out trip updates that match nothing in the timetable', () => {
    expect(motisRtSources('9', [
      { url: 'https://region/tu', type: 'tripUpdates', unmatched: true },
      { url: 'https://region/alerts', type: 'alerts' },
    ], relay).map(s => s.url)).toEqual(['https://region/alerts'])
  })

  it('keeps the relay position of a URL that comes after one left out', () => {
    expect(motisRtSources('10', [
      { url: 'https://a/vp', type: 'vehiclePositions' },
      { url: 'https://a/tu', type: 'tripUpdates', normalize: true },
    ], relay).map(s => s.url)).toEqual(['http://barrelman:5001/gtfs-rt/10/1'])
  })
})
