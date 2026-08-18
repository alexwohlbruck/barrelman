import { describe, test, expect, mock, beforeEach } from 'bun:test'

// ── Mock dependencies ───────────────────────────────────────────────

const mockDbExecute = mock(async () => [] as any[])
mock.module('../db', () => ({
  db: { execute: mockDbExecute },
}))

/** The decoded FeedMessage each fetch resolves to. Set per test. */
let decoded: any = { header: {}, entity: [] }

mock.module('gtfs-realtime-bindings', () => ({
  default: {
    transit_realtime: {
      FeedMessage: { decode: mock(() => decoded) },
    },
  },
}))

// ── Import under test ───────────────────────────────────────────────

const {
  getServiceAlerts,
  isAlertActive,
  alertMatches,
  alertRank,
} = await import('./alerts.service')

// ── Fixtures ────────────────────────────────────────────────────────

/** Anchored to the real clock: the service filters on "in effect now", so a
 *  hard-coded epoch would quietly start failing once it drifted into the past. */
const NOW_SEC = Math.floor(Date.now() / 1000)
const NOW_MS = NOW_SEC * 1000

function translated(text: string) {
  return { translation: [{ text, language: 'en' }] }
}

/**
 * A decoded ServiceAlert entity as the protobuf bindings hand it over:
 * numeric enums, TranslatedStrings, Long-ish timestamps.
 */
function alertEntity(id: string, overrides: any = {}) {
  return {
    id,
    alert: {
      cause: 10, // CONSTRUCTION
      effect: 4, // DETOUR
      severityLevel: 3, // WARNING
      headerText: translated('Southbound B48 buses are detoured'),
      descriptionText: translated('Buses will not stop at Franklin Ave.'),
      activePeriod: [{ start: NOW_SEC - 3600 }],
      informedEntity: [{ routeId: 'B48', agencyId: 'MTA' }],
      ...overrides,
    },
  }
}

function feedRow(feedId: string, urls: any[]) {
  return { feed_id: feedId, rt_urls: urls }
}

const okFetch = mock(async () => new Response(new ArrayBuffer(8), { status: 200 })) as any

beforeEach(() => {
  mockDbExecute.mockClear()
  decoded = { header: { timestamp: NOW_SEC }, entity: [] }
  // Each test starts with a distinct feed id so the 60s per-feed cache in the
  // service can't carry a previous test's alerts into this one.
  feedCounter++
})

let feedCounter = 0
const feedId = () => `feed${feedCounter}`

function withFeed(entities: any[], urls?: any[]) {
  decoded = { header: { timestamp: NOW_SEC }, entity: entities }
  mockDbExecute.mockImplementation(async () => [
    feedRow(feedId(), urls ?? [{ url: 'https://agency.example/gtfs-rt/alerts.pb' }]),
  ])
}

// ── Decoding ────────────────────────────────────────────────────────

describe('getServiceAlerts — decoding', () => {
  test('normalises numeric enums to spec names', async () => {
    withFeed([alertEntity('a1')])

    const { alerts } = await getServiceAlerts({}, okFetch)

    expect(alerts).toHaveLength(1)
    expect(alerts[0]).toMatchObject({
      cause: 'CONSTRUCTION',
      effect: 'DETOUR',
      severity: 'WARNING',
      header: 'Southbound B48 buses are detoured',
      description: 'Buses will not stop at Franklin Ave.',
      feedId: feedId(),
    })
  })

  test('prefixes the entity id with the feed so ids are unique across agencies', async () => {
    withFeed([alertEntity('a1')])

    const { alerts } = await getServiceAlerts({}, okFetch)

    expect(alerts[0].id).toBe(`${feedId()}_a1`)
  })

  test('promotes the description when an alert has no header', async () => {
    withFeed([alertEntity('a1', { headerText: undefined })])

    const { alerts } = await getServiceAlerts({}, okFetch)

    expect(alerts[0].header).toBe('Buses will not stop at Franklin Ave.')
    expect(alerts[0].description).toBeUndefined()
  })

  test('skips alerts with no text at all — nothing to show a rider', async () => {
    withFeed([
      alertEntity('a1', { headerText: undefined, descriptionText: undefined }),
      alertEntity('a2'),
    ])

    const { alerts } = await getServiceAlerts({}, okFetch)

    expect(alerts).toHaveLength(1)
    expect(alerts[0].id).toBe(`${feedId()}_a2`)
  })

  test('ignores non-alert entities in a combined feed', async () => {
    withFeed([
      { id: 'v1', vehicle: { position: { latitude: 1, longitude: 2 } } },
      { id: 'tu1', tripUpdate: { trip: { tripId: 't' } } },
      alertEntity('a1'),
    ])

    const { alerts } = await getServiceAlerts({}, okFetch)

    expect(alerts).toHaveLength(1)
  })

  test('reports the feed header timestamp so clients can judge freshness', async () => {
    withFeed([alertEntity('a1')])

    const { feedTimestamps } = await getServiceAlerts({}, okFetch)

    expect(feedTimestamps[feedId()]).toBe(new Date(NOW_MS).toISOString())
  })

  test('a feed whose alert URL errors yields no alerts rather than throwing', async () => {
    withFeed([alertEntity('a1')])
    const failing = mock(async () => new Response('nope', { status: 503 })) as any

    const { alerts } = await getServiceAlerts({}, failing)

    expect(alerts).toEqual([])
  })

  test('feeds with several RT URLs and no alert URL are skipped', async () => {
    withFeed([alertEntity('a1')], [
      { url: 'https://agency.example/gtfs-rt/vehiclepositions.pb' },
      { url: 'https://agency.example/gtfs-rt/tripupdates.pb' },
    ])

    const { alerts } = await getServiceAlerts({}, okFetch)

    expect(alerts).toEqual([])
  })

  test('a feed with one combined RT URL is tried anyway', async () => {
    withFeed([alertEntity('a1')], [{ url: 'https://agency.example/gtfs-rt/all.pb' }])

    const { alerts } = await getServiceAlerts({}, okFetch)

    expect(alerts).toHaveLength(1)
  })
})

// ── Active periods ──────────────────────────────────────────────────

describe('isAlertActive', () => {
  const alert = (periods: Array<{ start?: string; end?: string }>) =>
    ({ activePeriods: periods }) as any

  test('an alert with no period at all is in effect until further notice', () => {
    expect(isAlertActive(alert([]), NOW_MS)).toBe(true)
  })

  test('an open-ended period that has started is in effect', () => {
    expect(
      isAlertActive(alert([{ start: new Date(NOW_MS - 1000).toISOString() }]), NOW_MS),
    ).toBe(true)
  })

  test('a finished period is not', () => {
    expect(
      isAlertActive(alert([{ end: new Date(NOW_MS - 1000).toISOString() }]), NOW_MS),
    ).toBe(false)
  })

  test('a future period is excluded by default and included on request', () => {
    const future = alert([{ start: new Date(NOW_MS + 86_400_000).toISOString() }])

    expect(isAlertActive(future, NOW_MS)).toBe(false)
    expect(isAlertActive(future, NOW_MS, true)).toBe(true)
  })

  test('any period in effect makes the alert active', () => {
    const weekends = alert([
      { end: new Date(NOW_MS - 86_400_000).toISOString() },
      { start: new Date(NOW_MS - 60_000).toISOString() },
    ])

    expect(isAlertActive(weekends, NOW_MS)).toBe(true)
  })
})

// ── Matching ────────────────────────────────────────────────────────

describe('alertMatches', () => {
  const withEntities = (informedEntities: any[]) => ({ informedEntities }) as any

  test('matches with no filters at all', () => {
    expect(alertMatches(withEntities([{ routeId: 'B48' }]), {})).toBe(true)
  })

  test('matches a route the caller named', () => {
    expect(
      alertMatches(withEntities([{ routeId: 'B48' }]), { routeIds: ['B48', 'B49'] }),
    ).toBe(true)
  })

  test('does not match a route the caller did not name', () => {
    expect(
      alertMatches(withEntities([{ routeId: 'B62' }]), { routeIds: ['B48'] }),
    ).toBe(false)
  })

  test('an agency-wide alert reaches every page', () => {
    expect(
      alertMatches(withEntities([{ agencyId: 'MTA' }]), { routeIds: ['B48'] }),
    ).toBe(true)
  })

  test('a route+stop entity only matches at that stop', () => {
    const alert = withEntities([{ routeId: 'B48', stopId: 'S1' }])

    expect(alertMatches(alert, { routeIds: ['B48'], stopIds: ['S1'] })).toBe(true)
    expect(alertMatches(alert, { routeIds: ['B48'], stopIds: ['S2'] })).toBe(false)
  })

  test('a trip-scoped alert does not leak onto a stop board that never asked about trips', () => {
    expect(
      alertMatches(withEntities([{ tripId: 'trip-9' }]), { stopIds: ['S1'] }),
    ).toBe(false)
  })

  test('matches when any one informed entity matches', () => {
    expect(
      alertMatches(
        withEntities([{ routeId: 'B62' }, { routeId: 'B48' }]),
        { routeIds: ['B48'] },
      ),
    ).toBe(true)
  })
})

// ── Ranking ─────────────────────────────────────────────────────────

describe('alertRank', () => {
  test('orders by declared severity', () => {
    const rank = (severity: string) => alertRank({ severity, effect: 'OTHER_EFFECT' } as any)

    expect(rank('SEVERE')).toBeGreaterThan(rank('WARNING'))
    expect(rank('WARNING')).toBeGreaterThan(rank('INFO'))
  })

  test('falls back to the effect when the feed left severity unset', () => {
    const unrated = (effect: string) =>
      alertRank({ severity: 'UNKNOWN_SEVERITY', effect } as any)

    expect(unrated('NO_SERVICE')).toBeGreaterThan(unrated('DETOUR'))
    expect(unrated('DETOUR')).toBeGreaterThan(unrated('MODIFIED_SERVICE'))
    expect(unrated('NO_EFFECT')).toBe(0)
  })
})

// ── Filtering end to end ────────────────────────────────────────────

describe('getServiceAlerts — filtering', () => {
  test('returns only alerts informing the requested route', async () => {
    withFeed([
      alertEntity('a1', { informedEntity: [{ routeId: 'B48' }] }),
      alertEntity('a2', { informedEntity: [{ routeId: 'B62' }] }),
    ])

    const { alerts } = await getServiceAlerts({ routeIds: ['B48'] }, okFetch)

    expect(alerts.map(a => a.id)).toEqual([`${feedId()}_a1`])
  })

  test('drops alerts whose period has passed', async () => {
    withFeed([
      alertEntity('a1', { activePeriod: [{ end: NOW_SEC - 3600 }] }),
      alertEntity('a2'),
    ])

    const { alerts } = await getServiceAlerts({}, okFetch)

    expect(alerts.map(a => a.id)).toEqual([`${feedId()}_a2`])
  })

  test('sorts the worst news first', async () => {
    withFeed([
      alertEntity('info', { severityLevel: 2, effect: 6 }),
      alertEntity('severe', { severityLevel: 4, effect: 1 }),
      alertEntity('warning', { severityLevel: 3 }),
    ])

    const { alerts } = await getServiceAlerts({}, okFetch)

    expect(alerts.map(a => a.id)).toEqual([
      `${feedId()}_severe`,
      `${feedId()}_warning`,
      `${feedId()}_info`,
    ])
  })
})
