import { beforeEach, describe, expect, mock, test } from 'bun:test'
import { Elysia } from 'elysia'
import { createGtfsRtRoutes } from './gtfs-rt'

let caller: 'service' | 'account' | 'anonymous' = 'service'
const getNormalizedFeed = mock(async (_feedId: string, index: number) =>
  index === 0 ? { body: new Uint8Array([8, 1]), stats: null } : null)
const app = new Elysia().use(createGtfsRtRoutes({
  identifyCaller: async () => ({ caller: { kind: caller } }),
  getNormalizedFeed,
}))
const get = (path: string) => app.handle(new Request(`http://localhost/gtfs-rt/${path}`))

beforeEach(() => { caller = 'service'; getNormalizedFeed.mockClear() })

describe('GTFS-RT relay', () => {
  test('refuses account keys, since upstream feeds may carry the operator key', async () => {
    caller = 'account'
    expect((await get('5/0')).status).toBe(403)
    expect(getNormalizedFeed).not.toHaveBeenCalled()
  })

  test('serves the rewritten feed to the service credential', async () => {
    const response = await get('5/0')
    expect(response.status).toBe(200)
    expect(response.headers.get('content-type')).toBe('application/x-protobuf')
    expect(new Uint8Array(await response.arrayBuffer())).toEqual(new Uint8Array([8, 1]))
  })

  test('404s a URL the feed has not marked for normalization', async () => {
    expect((await get('5/3')).status).toBe(404)
  })
})
