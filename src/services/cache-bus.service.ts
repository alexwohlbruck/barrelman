/**
 * Cross-instance cache invalidation over Postgres LISTEN/NOTIFY.
 *
 * The API-key cache and the balance cache are per process. Revoking a key or
 * suspending an account evicts them here at once, but every other replica kept
 * serving the old verdict until its TTL ran out — a minute, for a key. Every
 * eviction that matters is now also published on CACHE_CHANNEL, and every API
 * instance listens and evicts the same entries.
 *
 * Same-process eviction stays synchronous and does not wait for the round trip:
 * the replica that made the change is already correct when the change returns.
 * A notification carries the sender's instance id so it can ignore its own.
 *
 * Postgres drops notifications for a listener that is disconnected, so after a
 * reconnect each cache is cleared outright rather than trusted. A cold cache
 * costs one lookup per key; a stale one can keep a revoked key working.
 *
 * Nothing is published until `startCacheBus()` has run. Only the API calls it;
 * tests and CLI scripts that import these services stay off the database.
 */
import { randomUUID } from 'node:crypto'
import { accountsConnection } from '../db'

export const CACHE_CHANNEL = 'barrelman_cache'

export type CacheEvent =
  | { kind: 'apiKey'; hash: string }
  | { kind: 'user'; id: string }

export interface CacheHandler {
  onEvent(event: CacheEvent): void
  /** Called after a reconnect, when notifications may have been missed. */
  onResync(): void
}

interface BusState {
  instanceId: string
  /** By name, so a module re-evaluated by `bun --hot` replaces its handler. */
  handlers: Map<string, CacheHandler>
  started: boolean
  connectedOnce: boolean
}

/**
 * Kept on globalThis: under `bun --hot` the listener from the first evaluation
 * stays connected, and it must dispatch to the caches the CURRENT modules hold,
 * not the ones that were thrown away.
 */
const STATE_KEY = Symbol.for('barrelman.cacheBus')

function state(): BusState {
  const globals = globalThis as Record<symbol, BusState | undefined>
  return (globals[STATE_KEY] ??= {
    instanceId: randomUUID(),
    handlers: new Map(),
    started: false,
    connectedOnce: false,
  })
}

export function registerCacheHandler(name: string, handler: CacheHandler): void {
  state().handlers.set(name, handler)
}

// ── Wire format ─────────────────────────────────────────────────────────

/** NOTIFY payloads are capped at 8000 bytes; ours are a fraction of that. */
const MAX_PAYLOAD = 1_000

export function encodeCacheEvent(event: CacheEvent, from: string): string {
  return JSON.stringify({ ...event, from })
}

/**
 * Parse a notification, or null for anything malformed. The channel is open to
 * any session on the database, so the payload is validated rather than cast.
 */
export function parseCacheEvent(payload: string): { event: CacheEvent; from: string } | null {
  if (payload.length > MAX_PAYLOAD) return null
  let raw: unknown
  try {
    raw = JSON.parse(payload)
  } catch {
    return null
  }
  if (!raw || typeof raw !== 'object') return null
  const { kind, hash, id, from } = raw as Record<string, unknown>
  const sender = typeof from === 'string' ? from : ''

  if (kind === 'apiKey' && typeof hash === 'string' && /^[0-9a-f]{64}$/.test(hash)) {
    return { event: { kind, hash }, from: sender }
  }
  if (kind === 'user' && typeof id === 'string' && id.length > 0 && id.length <= 200) {
    return { event: { kind, id }, from: sender }
  }
  return null
}

/**
 * Apply one notification to every registered cache. Returns whether it was
 * applied: our own echo and malformed payloads are not.
 */
export function handleCacheNotification(payload: string, bus: BusState = state()): boolean {
  const parsed = parseCacheEvent(payload)
  if (!parsed || parsed.from === bus.instanceId) return false
  for (const [name, handler] of bus.handlers) {
    try {
      handler.onEvent(parsed.event)
    } catch (err) {
      console.error(`[cache-bus] ${name} failed to apply ${parsed.event.kind}`, err)
    }
  }
  return true
}

function resyncAll(bus: BusState): void {
  for (const [name, handler] of bus.handlers) {
    try {
      handler.onResync()
    } catch (err) {
      console.error(`[cache-bus] ${name} failed to resync`, err)
    }
  }
}

// ── Publishing ──────────────────────────────────────────────────────────

/**
 * Coalesced per tick. Suspension evicts the key cache and the balance cache for
 * the same account back to back, and both are one `user` event to a listener.
 */
let pending = new Map<string, CacheEvent>()

function eventKey(event: CacheEvent): string {
  return event.kind === 'apiKey' ? `k:${event.hash}` : `u:${event.id}`
}

export function publishCacheEvent(event: CacheEvent): void {
  const bus = state()
  if (!bus.started) return

  const first = pending.size === 0
  pending.set(eventKey(event), event)
  if (!first) return

  queueMicrotask(() => {
    const batch = [...pending.values()]
    pending = new Map()
    for (const e of batch) {
      accountsConnection
        .notify(CACHE_CHANNEL, encodeCacheEvent(e, bus.instanceId))
        .catch((err) => console.error(`[cache-bus] publishing ${e.kind} failed`, err))
    }
  })
}

// ── Listening ───────────────────────────────────────────────────────────

const RETRY_MS = 5_000

/**
 * Start listening, once per process. postgres-js holds LISTEN on a dedicated
 * connection outside the pool and re-issues it after a reconnect, calling
 * `onlisten` each time; every call after the first is a reconnect.
 */
export async function startCacheBus(): Promise<void> {
  const bus = state()
  if (bus.started) return
  bus.started = true

  const listen = async (): Promise<void> => {
    try {
      await accountsConnection.listen(
        CACHE_CHANNEL,
        (payload) => handleCacheNotification(payload),
        () => {
          if (bus.connectedOnce) {
            console.warn('[cache-bus] reconnected; clearing caches that may have missed invalidations')
            resyncAll(bus)
          }
          bus.connectedOnce = true
        },
      )
    } catch (err) {
      // Only the first connect can fail here; postgres-js handles reconnects.
      console.error(`[cache-bus] listen failed, retrying in ${RETRY_MS / 1000}s`, err)
      setTimeout(() => void listen(), RETRY_MS).unref?.()
    }
  }

  await listen()
}
