import { describe, test, expect, beforeEach, afterEach } from 'bun:test'
import Elysia from 'elysia'
import { createRouteRoutes } from './route'
import { RouteLanesRequestError, type RouteLanesRequest, type RouteLanesResponse } from '../services/route-lanes.service'

const body: RouteLanesRequest = {
  coordinates: [[-73.99, 40.75], [-73.98, 40.75]],
  ways: [{ from: 0, to: 840, osmWayId: 1 }],
  maneuvers: [{ distance: 400, turn: 'left' }],
}
const answer: RouteLanesResponse = { drivingSide: 'right', lanePlan: [], laneGeometry: null, ways: [] }

function post(path: string, payload: unknown, headers: Record<string, string> = {}) {
  return new Request(`http://localhost${path}`, {
    method: 'POST',
    headers: { 'content-type': 'application/json', ...headers },
    body: JSON.stringify(payload),
  })
}

function makeApp(getRouteLanes: (r: RouteLanesRequest) => Promise<RouteLanesResponse>) {
  return new Elysia().use(createRouteRoutes({ getRouteLanes: getRouteLanes as never }))
}

const savedApiKey = process.env.BARRELMAN_API_KEY
beforeEach(() => {
  delete process.env.BARRELMAN_API_KEY
})
afterEach(() => {
  if (savedApiKey === undefined) delete process.env.BARRELMAN_API_KEY
  else process.env.BARRELMAN_API_KEY = savedApiKey
})

describe('POST /route/lanes', () => {
  test('hands the body to the lanes service', async () => {
    const seen: RouteLanesRequest[] = []
    const res = await makeApp(async r => (seen.push(r), answer)).handle(post('/route/lanes', body))
    expect(res.status).toBe(200)
    expect(await res.json()).toEqual(answer)
    expect(seen[0].maneuvers?.[0]).toEqual({ distance: 400, turn: 'left' })
  })

  test('422 for a turn outside the turn:lanes vocabulary', async () => {
    const res = await makeApp(async () => answer).handle(post('/route/lanes', { ...body, maneuvers: [{ distance: 1, turn: 'sideways' }] }))
    expect(res.status).toBe(422)
  })

  test('400 for a request the service refuses', async () => {
    const res = await makeApp(async () => { throw new RouteLanesRequestError('coordinates needs at least 2 points') })
      .handle(post('/route/lanes', body))
    expect(res.status).toBe(400)
    expect((await res.json()).error).toContain('coordinates')
  })

  test('401 without a bearer token when a key is configured', async () => {
    process.env.BARRELMAN_API_KEY = 'test-key'
    let calls = 0
    const res = await makeApp(async () => (calls++, answer)).handle(post('/route/lanes', body))
    expect(res.status).toBe(401)
    expect(calls).toBe(0)
  })
})
