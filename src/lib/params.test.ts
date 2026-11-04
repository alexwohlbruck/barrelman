import { describe, test, expect } from 'bun:test'
import Elysia from 'elysia'
import { clampInt, clampNumber, LIMITS } from './params'
import { createTransitRoutes } from '../routes/transit'
import { createChildrenRoutes } from '../routes/children'

describe('clampNumber / clampInt', () => {
  test('clamps into range and keeps values inside it', () => {
    expect(clampNumber('1e9', { min: 1, max: 10_000 })).toBe(10_000)
    expect(clampNumber('-5', { min: 1, max: 10_000 })).toBe(1)
    expect(clampNumber('250.5', { min: 1, max: 10_000 })).toBe(250.5)
    expect(clampInt('250.9', { min: 1, max: 10_000 })).toBe(250)
  })

  test('missing or non-numeric values take the fallback', () => {
    expect(clampNumber(undefined, { min: 1, max: 5, fallback: 3 })).toBe(3)
    expect(clampNumber('', { min: 1, max: 5 })).toBeUndefined()
    expect(clampInt('abc', { min: 1, max: 5, fallback: 2 })).toBe(2)
    expect(clampNumber('Infinity', { min: 1, max: 5 })).toBeUndefined()
  })
})

describe('routes pass clamped sizes to SQL', () => {
  test('/transit/stops caps radius and limit', async () => {
    const seen: Array<{ radius: number; limit: number }> = []
    const getNearbyStops = (async (args: { radius: number; limit: number }) => {
      seen.push(args)
      return []
    }) as never
    const app = new Elysia().use(createTransitRoutes({ getNearbyStops }))

    await app.handle(new Request('http://localhost/transit/stops?lat=40&lng=-74&radius=99999999&limit=1000000'))
    await app.handle(new Request('http://localhost/transit/stops?lat=40&lng=-74'))

    expect(seen[0].radius).toBe(LIMITS.maxNearbyRadiusM)
    expect(seen[0].limit).toBe(LIMITS.maxRows)
    expect(seen[1]).toMatchObject({ radius: 1000, limit: 20 })
  })

  test('/children caps limit and offset', async () => {
    const seen: Array<{ limit?: string; offset?: string }> = []
    const findChildren = (async (args: { limit?: string; offset?: string }) => {
      seen.push(args)
      return []
    }) as never
    const app = new Elysia().use(createChildrenRoutes({ findChildren }))

    await app.handle(new Request('http://localhost/children?id=way/1&limit=50000&offset=-3'))

    expect(seen[0].limit).toBe(String(LIMITS.maxRows))
    expect(seen[0].offset).toBe('0')
  })
})
