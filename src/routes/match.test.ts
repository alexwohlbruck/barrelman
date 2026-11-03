/**
 * HTTP-layer tests for POST /match.
 *
 * The service is injected, so nothing here needs GraphHopper. Covers parsing
 * of both body forms into the service request, error mapping, and that the
 * route is not public (see the auth footgun in CLAUDE.md).
 */

import { describe, test, expect, beforeEach, afterEach } from 'bun:test'
import Elysia from 'elysia'
import { createMatchRoutes } from './match'
import { GraphHopperTimeoutError, type MatchRequest, type MatchResponse } from '../services/match.service'
import { GraphHopperError } from '../services/route.service'

const EMPTY: MatchResponse = {
  paths: [],
  map_matching: { input_points: 0, matched_points: 0, routed_points: 0, gps_accuracy: 10 },
}

/** A plain function, not mock(): records what the route handed the service. */
function makeApp(result: () => Promise<MatchResponse> = async () => EMPTY) {
  const seen: MatchRequest[] = []
  const matchTrace = async (req: MatchRequest) => {
    seen.push(req)
    return result()
  }
  return { app: new Elysia().use(createMatchRoutes({ matchTrace })), seen }
}

function post(path: string, body: string, contentType: string, headers: Record<string, string> = {}) {
  return new Request(`http://localhost${path}`, {
    method: 'POST',
    headers: { 'content-type': contentType, ...headers },
    body,
  })
}

const savedApiKey = process.env.BARRELMAN_API_KEY
beforeEach(() => {
  delete process.env.BARRELMAN_API_KEY
})
afterEach(() => {
  if (savedApiKey === undefined) delete process.env.BARRELMAN_API_KEY
  else process.env.BARRELMAN_API_KEY = savedApiKey
})

const JSON_BODY = JSON.stringify({
  profile: 'bike',
  points: [
    { lat: 35.2271, lon: -80.8431, time: '2026-05-01T10:00:00Z' },
    { lat: 35.2085, lon: -80.812 },
  ],
  gps_accuracy: 15,
  snap_prevention: ['bridge', 'motorway'],
})

describe('POST /match', () => {
  test('parses a JSON body into the service request', async () => {
    const { app, seen } = makeApp()
    const res = await app.handle(post('/match', JSON_BODY, 'application/json'))

    expect(res.status).toBe(200)
    expect(seen[0]).toEqual({
      profile: 'bike',
      points: [
        { lat: 35.2271, lon: -80.8431, time: Date.parse('2026-05-01T10:00:00Z') },
        { lat: 35.2085, lon: -80.812, time: undefined },
      ],
      gpsAccuracy: 15,
      snapPrevention: ['bridge', 'motorway'],
      maxGapMeters: undefined,
      maxGapSeconds: undefined,
    })
  })

  test('takes a GPX body with options from the query string', async () => {
    const { app, seen } = makeApp()
    const gpx = '<gpx><trk><trkseg><trkpt lat="1" lon="2"/><trkpt lat="3" lon="4"/></trkseg></trk></gpx>'
    const res = await app.handle(
      post('/match?profile=foot&snap_prevention=bridge,motorway&max_gap_meters=250', gpx, 'application/gpx+xml'),
    )

    expect(res.status).toBe(200)
    expect(seen[0].profile).toBe('foot')
    expect(seen[0].points.map((p) => [p.lat, p.lon])).toEqual([[1, 2], [3, 4]])
    expect(seen[0].snapPrevention).toEqual(['bridge', 'motorway'])
    expect(seen[0].maxGapMeters).toBe(250)
  })

  test('400 for a body that is neither JSON nor GPX', async () => {
    const { app, seen } = makeApp()
    const res = await app.handle(post('/match', 'not json', 'application/json'))
    expect(res.status).toBe(400)
    expect(seen).toHaveLength(0)
  })

  test('504 when GraphHopper runs out of time, 502 when it fails', async () => {
    const timedOut = makeApp(async () => { throw new GraphHopperTimeoutError() })
    expect((await timedOut.app.handle(post('/match', JSON_BODY, 'application/json'))).status).toBe(504)

    const failed = makeApp(async () => { throw new GraphHopperError(500, '{"message":"boom"}') })
    const res = await failed.app.handle(post('/match', JSON_BODY, 'application/json'))
    expect(res.status).toBe(502)
    expect(await res.json()).toEqual({ message: 'boom' })
  })

  test('passes a GraphHopper 4xx through with its message', async () => {
    const { app } = makeApp(async () => { throw new GraphHopperError(400, '{"message":"Cannot find point 0"}') })
    const res = await app.handle(post('/match', JSON_BODY, 'application/json'))
    expect(res.status).toBe(400)
    expect(await res.json()).toEqual({ message: 'Cannot find point 0' })
  })
})

describe('POST /match auth', () => {
  const API_KEY = 'test_api_secret'

  test('refuses an unauthenticated request without running the matcher', async () => {
    process.env.BARRELMAN_API_KEY = API_KEY
    const { app, seen } = makeApp()
    const res = await app.handle(post('/match', JSON_BODY, 'application/json'))

    expect(res.status).toBe(401)
    expect(seen).toHaveLength(0)
  })

  test('refuses a wrong key', async () => {
    process.env.BARRELMAN_API_KEY = API_KEY
    const { app, seen } = makeApp()
    const res = await app.handle(post('/match', JSON_BODY, 'application/json', { Authorization: 'Bearer wrong' }))

    expect(res.status).toBe(401)
    expect(seen).toHaveLength(0)
  })

  test('accepts the service key', async () => {
    process.env.BARRELMAN_API_KEY = API_KEY
    const { app, seen } = makeApp()
    const res = await app.handle(
      post('/match', JSON_BODY, 'application/json', { Authorization: `Bearer ${API_KEY}` }),
    )

    expect(res.status).toBe(200)
    expect(seen).toHaveLength(1)
  })
})
