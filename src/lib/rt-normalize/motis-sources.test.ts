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

  it('leaves unmarked URLs as published', () => {
    const rt = { url: 'https://agency/tu', headers: { key: 'k' } }
    expect(motisRtSources('9', [rt], relay)).toEqual([rt])
  })
})
