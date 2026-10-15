import { describe, expect, mock, test } from 'bun:test'

// Other suites replace this module process-wide, so this one does too: a feed
// is its entities as JSON.
mock.module('gtfs-realtime-bindings', () => ({
  default: {
    transit_realtime: {
      FeedMessage: { decode: (buf: Uint8Array) => JSON.parse(new TextDecoder().decode(buf)) },
    },
  },
}))

const { selectRtUrls } = await import('./rt-url-selection')

function feedOf(routeIds: string[]): Uint8Array {
  const entity = routeIds.map((routeId, i) => ({ tripUpdate: { trip: { tripId: `t${i}`, routeId } } }))
  return new TextEncoder().encode(JSON.stringify({ entity }))
}

/** Serves each URL's body or status; counts requests per URL. */
function serving(responses: Record<string, Uint8Array | number | Error>) {
  const calls: Record<string, number> = {}
  const fetchFn = (async (url: string) => {
    calls[url] = (calls[url] ?? 0) + 1
    const r = responses[url]
    if (r instanceof Error) throw r
    if (typeof r === 'number') return new Response('', { status: r })
    return new Response(r ?? new Uint8Array(), { status: 200 })
  }) as unknown as typeof fetch
  return { calls, fetchFn }
}

const SUBWAY = new Set(['A', 'C', 'E', 'B', 'D', 'F', 'M'])

describe('selectRtUrls', () => {
  test('drops a feed whose trips run on none of the static routes', async () => {
    const { fetchFn } = serving({
      'https://mta.example/ace': feedOf(['A', 'C']),
      'https://mta.example/bustime': feedOf(['B63', 'M15']),
    })

    const { kept, dropped } = await selectRtUrls(
      [
        { url: 'https://mta.example/ace', type: 'tripUpdates' },
        { url: 'https://mta.example/bustime', type: 'tripUpdates' },
      ],
      SUBWAY,
      fetchFn,
    )

    expect(kept.map(k => k.url)).toEqual(['https://mta.example/ace'])
    expect(dropped).toEqual([{ url: 'https://mta.example/bustime', reason: 'foreign-routes' }])
  })

  test('drops a feed that refuses us for lack of credentials', async () => {
    const { fetchFn } = serving({ 'https://bus.example/tu': 401, 'https://bus.example/alerts': 403 })

    const { kept, dropped } = await selectRtUrls(
      [
        { url: 'https://bus.example/tu', type: 'tripUpdates' },
        { url: 'https://bus.example/alerts', type: 'alerts' },
      ],
      SUBWAY,
      fetchFn,
    )

    expect(kept).toEqual([])
    expect(dropped.map(d => d.reason)).toEqual(['unauthorized', 'unauthorized'])
  })

  test('keeps what it cannot judge', async () => {
    const { fetchFn } = serving({
      'https://x.example/down': 503,
      'https://x.example/timeout': new Error('timeout'),
      'https://x.example/empty': feedOf([]),
      'https://x.example/alerts': new TextEncoder().encode('not a feed'),
    })

    const { kept } = await selectRtUrls(
      [
        { url: 'https://x.example/down' },
        { url: 'https://x.example/timeout' },
        { url: 'https://x.example/empty' },
        { url: 'https://x.example/alerts', type: 'alerts' },
      ],
      SUBWAY,
      fetchFn,
    )

    expect(kept).toHaveLength(4)
  })

  test('fetches a URL shared by trip updates and vehicle positions once', async () => {
    const { calls, fetchFn } = serving({ 'https://mta.example/bdfm': feedOf(['B', 'D']) })

    const { kept } = await selectRtUrls(
      [
        { url: 'https://mta.example/bdfm', type: 'tripUpdates' },
        { url: 'https://mta.example/bdfm', type: 'vehiclePositions' },
      ],
      SUBWAY,
      fetchFn,
    )

    expect(kept).toHaveLength(2)
    expect(calls['https://mta.example/bdfm']).toBe(1)
  })
})
