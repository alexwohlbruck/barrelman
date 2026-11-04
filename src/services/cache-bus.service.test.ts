/**
 * NOTIFY payload handling. The channel is writable by any session on the
 * database, so what matters is that a well-formed event reaches every cache, a
 * malformed one reaches none, and a replica ignores its own echo.
 */
import { describe, test, expect } from 'bun:test'
import {
  encodeCacheEvent,
  handleCacheNotification,
  parseCacheEvent,
  type CacheEvent,
  type CacheHandler,
} from './cache-bus.service'

const HASH = 'a'.repeat(64)

function bus(...handlers: CacheHandler[]) {
  return {
    instanceId: 'self',
    handlers: new Map(handlers.map((h, i) => [`h${i}`, h])),
    started: true,
    connectedOnce: true,
  }
}

function recorder() {
  const seen: CacheEvent[] = []
  return { seen, handler: { onEvent: (e: CacheEvent) => void seen.push(e), onResync: () => {} } }
}

describe('parseCacheEvent', () => {
  test('round-trips both kinds', () => {
    expect(parseCacheEvent(encodeCacheEvent({ kind: 'apiKey', hash: HASH }, 'x'))).toEqual({
      event: { kind: 'apiKey', hash: HASH },
      from: 'x',
    })
    expect(parseCacheEvent(encodeCacheEvent({ kind: 'user', id: 'usr_1' }, 'x'))?.event).toEqual({
      kind: 'user',
      id: 'usr_1',
    })
  })

  test('rejects malformed payloads', () => {
    for (const bad of [
      'not json',
      'null',
      '"user"',
      JSON.stringify({ kind: 'apiKey', hash: 'short' }),
      JSON.stringify({ kind: 'user', id: '' }),
      JSON.stringify({ kind: 'user', id: 42 }),
      JSON.stringify({ kind: 'everything' }),
      JSON.stringify({ kind: 'user', id: 'u', pad: 'x'.repeat(2_000) }),
    ]) {
      expect(parseCacheEvent(bad)).toBeNull()
    }
  })

  test('drops fields it does not know', () => {
    const parsed = parseCacheEvent(JSON.stringify({ kind: 'user', id: 'u1', hash: HASH, from: 'x' }))
    expect(parsed?.event).toEqual({ kind: 'user', id: 'u1' })
  })
})

describe('handleCacheNotification', () => {
  test('applies an event from another instance to every handler', () => {
    const a = recorder()
    const b = recorder()
    const payload = encodeCacheEvent({ kind: 'user', id: 'u1' }, 'other')
    expect(handleCacheNotification(payload, bus(a.handler, b.handler))).toBe(true)
    expect(a.seen).toEqual([{ kind: 'user', id: 'u1' }])
    expect(b.seen).toEqual([{ kind: 'user', id: 'u1' }])
  })

  test('ignores its own echo, which it already applied synchronously', () => {
    const a = recorder()
    const payload = encodeCacheEvent({ kind: 'apiKey', hash: HASH }, 'self')
    expect(handleCacheNotification(payload, bus(a.handler))).toBe(false)
    expect(a.seen).toEqual([])
  })

  test('one failing handler does not stop the others', () => {
    const b = recorder()
    const failing: CacheHandler = {
      onEvent: () => {
        throw new Error('boom')
      },
      onResync: () => {},
    }
    const original = console.error
    console.error = () => {}
    try {
      handleCacheNotification(encodeCacheEvent({ kind: 'user', id: 'u1' }, 'other'), bus(failing, b.handler))
    } finally {
      console.error = original
    }
    expect(b.seen).toHaveLength(1)
  })
})
