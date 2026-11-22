import { afterEach, beforeEach, describe, expect, test } from 'bun:test'
import { gunzipSync } from 'node:zlib'
import { createOfflineRoutes } from './offline'

const savedApiKey = process.env.BARRELMAN_API_KEY
beforeEach(() => { delete process.env.BARRELMAN_API_KEY })
afterEach(() => {
  if (savedApiKey === undefined) delete process.env.BARRELMAN_API_KEY
  else process.env.BARRELMAN_API_KEY = savedApiKey
})

const calls: { kind: string; bounds: number[]; days?: number }[] = []
const app = createOfflineRoutes({
  buildPlacesPackage: async bounds => { calls.push({ kind: 'places', bounds }); return { places: [] } as never },
  buildStreetsPackage: async bounds => { calls.push({ kind: 'streets', bounds }); return { nodes: [] } as never },
  buildTimetablePackage: async (bounds, days) => { calls.push({ kind: 'timetable', bounds, days }); return { trips: [] } as never },
})
const get = (path: string, headers: Record<string, string> = {}) =>
  app.handle(new Request(`http://localhost${path}`, { headers }))

describe('offline package routes', () => {
  test('rejects a missing, inverted or oversized bbox', async () => {
    for (const bbox of ['1,2,3', '-73,40,-74,41', '-74,40,-73,40.2']) {
      const res = await get(`/route/graph?bbox=${bbox}`)
      expect(res.status).toBe(400)
    }
  })

  test('builds each package for the bbox and gzips it when asked', async () => {
    calls.length = 0
    const res = await get('/search/region?bbox=-74,40.7,-73.9,40.8', { 'accept-encoding': 'gzip, br' })
    expect(res.headers.get('content-encoding')).toBe('gzip')
    expect(JSON.parse(gunzipSync(Buffer.from(await res.arrayBuffer())).toString())).toEqual({ places: [] })
    expect(calls).toEqual([{ kind: 'places', bounds: [-74, 40.7, -73.9, 40.8] }])

    const plain = await get('/route/graph?bbox=-74,40.7,-73.9,40.8')
    expect(plain.headers.get('content-encoding')).toBeNull()
    expect(await plain.json()).toEqual({ nodes: [] })
  })

  test('clamps the timetable to the most days it serves', async () => {
    calls.length = 0
    await get('/transit/timetable?bbox=-74,40.7,-73.9,40.8&days=90')
    await get('/transit/timetable?bbox=-74,40.7,-73.9,40.8')
    expect(calls.map(c => c.days)).toEqual([21, 14])
  })
})
