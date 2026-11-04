import { describe, test, expect, beforeEach, afterEach } from 'bun:test'
import Elysia from 'elysia'
import { createTransitAttributionRoutes } from './transit-attribution'

const savedKey = process.env.BARRELMAN_API_KEY

beforeEach(() => {
  // Open access, as the other transit route tests run.
  delete process.env.BARRELMAN_API_KEY
})

afterEach(() => {
  if (savedKey === undefined) delete process.env.BARRELMAN_API_KEY
  else process.env.BARRELMAN_API_KEY = savedKey
})

function app() {
  const asked: string[][] = []
  const routes = createTransitAttributionRoutes({
    attributionFor: async (ids) => {
      const list = [...ids] as string[]
      asked.push(list)
      return list.map((feedId) => ({ feedId, agency: `Agency ${feedId}`, text: null, url: null }))
    },
  })
  return { app: new Elysia().use(routes), asked }
}

describe('GET /transit/attribution', () => {
  test('resolves a comma-separated list, ignoring blanks', async () => {
    const { app: a, asked } = app()
    const res = await a.handle(new Request('http://localhost/transit/attribution?feedIds=5,%20mta,,'))
    expect(res.status).toBe(200)
    expect(asked).toEqual([['5', 'mta']])
    expect((await res.json()).attribution.map((r: any) => r.feedId)).toEqual(['5', 'mta'])
  })

  test('caps how many feeds one request may ask for', async () => {
    const { app: a, asked } = app()
    const ids = Array.from({ length: 80 }, (_, i) => `f${i}`).join(',')
    await a.handle(new Request(`http://localhost/transit/attribution?feedIds=${ids}`))
    expect(asked[0]).toHaveLength(50)
  })

  test('refuses a caller without a key once one is configured', async () => {
    process.env.BARRELMAN_API_KEY = 'brm_test_key'
    const { app: a, asked } = app()
    const res = await a.handle(new Request('http://localhost/transit/attribution?feedIds=5'))
    expect(res.status).toBe(401)
    expect(asked).toEqual([])
  })

  test('requires feedIds', async () => {
    const { app: a } = app()
    const res = await a.handle(new Request('http://localhost/transit/attribution'))
    expect(res.status).toBe(422)
  })
})
