import { describe, expect, it } from 'bun:test'
import { workerRewriter } from './rewrite-worker'

// Encoded once rather than here: other test files mock gtfs-realtime-bindings for the whole run.
// One trip update: trip A1 on route A, departing S1 at 1794535558.
const FEED = 'CgsKAzIuMBjY3NnXBhIcCgExGhcKBwoCQTEqAUESDBoGEIbh2dcGIgJTMQ=='
const feed = () => new Uint8Array(Buffer.from(FEED, 'base64'))

describe('realtime worker', () => {
  const rewriter = workerRewriter()

  it('passes a message through when it is not normalized', async () => {
    const { body, stats } = await rewriter.rewrite({ zipPath: '/nonexistent.zip', raw: feed(), normalize: false })
    expect(stats).toBeNull()
    expect([...(await rewriter.calls(body)).trips]).toEqual(['A1'])
  })

  it('passes a message through unrewritten when its timetable is missing', async () => {
    const { body, stats } = await rewriter.rewrite({ zipPath: '/nonexistent.zip', raw: feed(), normalize: true })
    expect(stats).toBeNull()
    expect([...(await rewriter.calls(body)).trips]).toEqual(['A1'])
  })

  it('refuses a message that does not decode', async () => {
    await expect(rewriter.rewrite({ zipPath: '/nonexistent.zip', raw: new Uint8Array([0xff, 0xff, 0xff]), normalize: false })).rejects.toThrow()
  })

  it('indexes the calls in a message and leaves the body usable', async () => {
    const body = feed()
    const live = await rewriter.calls(body)
    expect(live.calls.get('A1|S1')).toEqual({ departure: 1794535558 })
    expect(body.byteLength).toBe(Buffer.from(FEED, 'base64').byteLength)
  })
})

describe('transfer', () => {
  it('leaves bytes that share a buffer intact', async () => {
    const slab = new Uint8Array(4096)
    const bytes = feed()
    slab.set(bytes, 100)
    await workerRewriter().rewrite({ zipPath: '/nonexistent.zip', raw: slab.subarray(100, 100 + bytes.byteLength), normalize: false })
    expect(slab.byteLength).toBe(4096)
  })
})
