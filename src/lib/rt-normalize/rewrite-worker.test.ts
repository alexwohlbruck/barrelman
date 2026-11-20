import { describe, expect, it } from 'bun:test'
import { decodeFeed, encodeFeed } from './feed-codec'
import { workerRewriter } from './rewrite-worker'

const message = {
  header: { gtfsRealtimeVersion: '2.0', timestamp: 1794535000 },
  entity: [
    { id: '1', tripUpdate: { trip: { tripId: 'A1', routeId: 'A' }, stopTimeUpdate: [{ stopId: 'S1', departure: { time: 1794535558 } }] } },
  ],
}

describe('realtime worker', () => {
  const rewriter = workerRewriter()

  it('passes a message through when it is not normalized', async () => {
    const { body, stats } = await rewriter.rewrite({ zipPath: '/nonexistent.zip', raw: encodeFeed(message), normalize: false })
    expect(stats).toBeNull()
    expect(decodeFeed(body).entity?.[0]?.tripUpdate?.trip?.tripId).toBe('A1')
  })

  it('passes a message through unrewritten when its timetable is missing', async () => {
    const { body, stats } = await rewriter.rewrite({ zipPath: '/nonexistent.zip', raw: encodeFeed(message), normalize: true })
    expect(stats).toBeNull()
    expect(decodeFeed(body).entity).toHaveLength(1)
  })

  it('refuses a message that does not decode', async () => {
    await expect(rewriter.rewrite({ zipPath: '/nonexistent.zip', raw: new Uint8Array([0xff, 0xff, 0xff]), normalize: false })).rejects.toThrow()
  })

  it('indexes the calls in a message and leaves the body usable', async () => {
    const body = encodeFeed(message)
    const live = await rewriter.calls(body)
    expect([...live.trips]).toEqual(['A1'])
    expect(live.calls.get('A1|S1')).toEqual({ departure: 1794535558 })
    expect(body.byteLength).toBeGreaterThan(0)
  })
})

describe('transfer', () => {
  it('leaves bytes that share a buffer intact', async () => {
    const slab = new Uint8Array(4096)
    const encoded = encodeFeed(message)
    slab.set(encoded, 100)
    const view = slab.subarray(100, 100 + encoded.byteLength)
    await workerRewriter().rewrite({ zipPath: '/nonexistent.zip', raw: view, normalize: false })
    expect(slab.byteLength).toBe(4096)
  })
})
