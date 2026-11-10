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

  test('keeps a moved feed at the address it redirects to', async () => {
    const fetchFn = (async (_url: string, init?: RequestInit) => {
      if (init?.redirect === 'manual') return new Response(null, { status: 308 })
      const response = new Response(feedOf(['A']) as Uint8Array<ArrayBuffer>, { status: 200 })
      Object.defineProperties(response, {
        redirected: { value: true },
        url: { value: 'https://rtd.example/api/download?filename=TripUpdate.pb' },
      })
      return response
    }) as unknown as typeof fetch

    const { kept } = await selectRtUrls([{ url: 'https://rtd.example/files/TripUpdate.pb', type: 'tripUpdates' }], SUBWAY, fetchFn)

    expect(kept).toEqual([{ url: 'https://rtd.example/api/download?filename=TripUpdate.pb', type: 'tripUpdates' }])
  })

  test('marks a feed that redirects each time to go through the relay, at its own address', async () => {
    const fetchFn = (async (_url: string, init?: RequestInit) => {
      if (init?.redirect === 'manual') return new Response(null, { status: 302 })
      const response = new Response(feedOf(['A']) as Uint8Array<ArrayBuffer>, { status: 200 })
      Object.defineProperties(response, { redirected: { value: true }, url: { value: 'https://cap.example/files/ce01?filename=tu.pb' } })
      return response
    }) as unknown as typeof fetch

    const { kept } = await selectRtUrls([{ url: 'https://cap.example/download/tu', type: 'tripUpdates' }], SUBWAY, fetchFn)

    expect(kept).toEqual([{ url: 'https://cap.example/download/tu', type: 'tripUpdates', redirects: true }])
  })

  test('does not follow a redirect to something that is not a feed', async () => {
    const fetchFn = (async () => {
      const response = new Response('<html>domain for sale</html>', { status: 200 })
      Object.defineProperties(response, { redirected: { value: true }, url: { value: 'https://parked.example/' } })
      return response
    }) as unknown as typeof fetch

    const { kept } = await selectRtUrls([{ url: 'https://gone.example/tu', type: 'tripUpdates' }], SUBWAY, fetchFn)

    expect(kept).toEqual([{ url: 'https://gone.example/tu', type: 'tripUpdates' }])
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

  test('judges a feed that names no routes by its trips and stops', async () => {
    const trains = (stopId: string) =>
      new TextEncoder().encode(JSON.stringify({ entity: [{ tripUpdate: { trip: { tripId: 'Train428' }, stopTimeUpdate: [{ stopId }] } }] }))
    const { fetchFn } = serving({ 'https://marc.example/tu': trains('11958'), 'https://marc.example/own': trains('B1') })
    const bus = { trips: new Set(['3401']), stops: new Set(['B1']) }

    const { kept, dropped } = await selectRtUrls(
      [{ url: 'https://marc.example/tu', type: 'tripUpdates' }, { url: 'https://marc.example/own', type: 'tripUpdates' }],
      new Set(['11638']),
      fetchFn,
      bus,
    )

    expect(kept.map(rt => rt.url)).toEqual(['https://marc.example/own'])
    expect(dropped).toEqual([{ url: 'https://marc.example/tu', reason: 'foreign-routes' }])
  })

  test('drops a feed that is gone or has stopped updating', async () => {
    const old = new TextEncoder().encode(JSON.stringify({ header: { timestamp: Date.now() / 1000 - 2 * 3600 }, entity: [] }))
    const frozen = new TextEncoder().encode(JSON.stringify({ header: { timestamp: Date.now() / 1000 - 26 * 3600 }, entity: [] }))
    const { fetchFn } = serving({ 'https://a.example/gone': 404, 'https://a.example/frozen': frozen, 'https://a.example/quiet': old })

    const { kept, dropped } = await selectRtUrls(
      ['gone', 'frozen', 'quiet'].map(p => ({ url: `https://a.example/${p}`, type: 'tripUpdates' })),
      SUBWAY,
      fetchFn,
    )

    expect(kept.map(rt => rt.url)).toEqual(['https://a.example/quiet'])
    expect(dropped.map(d => d.reason)).toEqual(['gone', 'stale'])
  })

  test('marks a feed that refuses this network but answers through the egress proxy', async () => {
    process.env.RT_EGRESS_PROXY = 'http://egress.example:8888'
    const fetchFn = (async (_url: string, init?: RequestInit & { proxy?: string }) => {
      if (!init?.proxy) throw new Error('connect timeout')
      return new Response(feedOf(['A']) as Uint8Array<ArrayBuffer>, { status: 200 })
    }) as unknown as typeof fetch

    const { kept } = await selectRtUrls([{ url: 'https://blocked.example/tu', type: 'tripUpdates', egress: true }], SUBWAY, fetchFn)
    const direct = await selectRtUrls([{ url: 'https://blocked.example/tu', type: 'tripUpdates', egress: true }], SUBWAY, serving({ 'https://blocked.example/tu': feedOf(['A']) }).fetchFn)
    delete process.env.RT_EGRESS_PROXY

    expect(kept).toEqual([{ url: 'https://blocked.example/tu', type: 'tripUpdates', egress: true }])
    expect(direct.kept).toEqual([{ url: 'https://blocked.example/tu', type: 'tripUpdates' }])
  })
})
