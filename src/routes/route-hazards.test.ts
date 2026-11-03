import { describe, test, expect, beforeEach, afterEach } from 'bun:test'
import Elysia from 'elysia'
import { createRouteRoutes } from './route'
import { RouteHazardsRequestError, type RouteHazardsRequest, type RouteHazardsResponse } from '../services/route-hazards.service'

const body: RouteHazardsRequest = { coordinates: [[-73.99, 40.75], [-73.98, 40.75]], profile: 'bike' }
const answer: RouteHazardsResponse = { hazards: [{ kind: 'stop', distance: 12, coordinate: [-73.99, 40.75], osm: 'node/1' }] }

function post(payload: unknown) {
  return new Request('http://localhost/route/hazards', {
    method: 'POST',
    headers: { 'content-type': 'application/json' },
    body: JSON.stringify(payload),
  })
}

const makeApp = (getRouteHazards: (r: RouteHazardsRequest) => Promise<RouteHazardsResponse>) =>
  new Elysia().use(createRouteRoutes({ getRouteHazards: getRouteHazards as never }))

const savedApiKey = process.env.BARRELMAN_API_KEY
beforeEach(() => {
  delete process.env.BARRELMAN_API_KEY
})
afterEach(() => {
  if (savedApiKey === undefined) delete process.env.BARRELMAN_API_KEY
  else process.env.BARRELMAN_API_KEY = savedApiKey
})

describe('POST /route/hazards', () => {
  test('hands the body to the hazards service', async () => {
    const seen: RouteHazardsRequest[] = []
    const res = await makeApp(async r => (seen.push(r), answer)).handle(post(body))
    expect(res.status).toBe(200)
    expect(await res.json()).toEqual(answer)
    expect(seen[0].profile).toBe('bike')
  })

  test('422 for a profile or kind it does not know', async () => {
    expect((await makeApp(async () => answer).handle(post({ ...body, profile: 'boat' }))).status).toBe(422)
    expect((await makeApp(async () => answer).handle(post({ ...body, kinds: ['potholes'] }))).status).toBe(422)
  })

  test('400 for a request the service refuses', async () => {
    const res = await makeApp(async () => { throw new RouteHazardsRequestError('coordinates needs at least 2 points') }).handle(post(body))
    expect(res.status).toBe(400)
    expect((await res.json()).error).toContain('coordinates')
  })

  test('401 without a bearer token when a key is configured', async () => {
    process.env.BARRELMAN_API_KEY = 'test-key'
    let calls = 0
    const res = await makeApp(async () => (calls++, answer)).handle(post(body))
    expect(res.status).toBe(401)
    expect(calls).toBe(0)
  })
})
