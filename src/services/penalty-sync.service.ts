/**
 * Moves penalty-box state between memory and `accounts_throttle_penalties`.
 *
 * The box itself never waits on this. A rejection updates memory and marks the
 * entry dirty; every FLUSH_MS the dirty entries are written in one statement.
 * Every REFRESH_MS the live rows are read back and folded in, which is how a
 * caller boxed on one replica is refused by the others, and the same read at
 * startup is how a box survives a deploy. Expired rows are pruned on the same
 * tick. That is at most one write and two short statements per replica per
 * tick, regardless of traffic — nothing runs per request.
 *
 * Losing a flush (a crash between ticks, the database briefly down) costs a
 * few seconds of strikes, which a caller who is really misbehaving rebuilds
 * just as fast. That is why this is write-behind and not write-through.
 */
import { accountsConnection as sql } from '../db'
import { penaltyBox, STRIKE_DECAY_MS } from './throttle.service'
import type { PenaltyBox, PenaltyRow } from './penalty-box'

const FLUSH_MS = 2_000
const REFRESH_MS = 15_000

/** The four statements the sync needs, so the logic can be tested without a database. */
export interface PenaltyStore {
  /** Rows still live: boxed now, or with strikes that have not decayed. */
  load(): Promise<PenaltyRow[]>
  upsert(rows: PenaltyRow[]): Promise<void>
  remove(ids: string[]): Promise<void>
  prune(): Promise<number>
}

const DECAY_SECONDS = STRIKE_DECAY_MS / 1000

export const pgPenaltyStore: PenaltyStore = {
  async load() {
    const rows = await sql<{ id: string; strikes: number; penalised_until: Date; last_strike_at: Date }[]>`
      SELECT id, strikes, penalised_until, last_strike_at
      FROM accounts_throttle_penalties
      WHERE penalised_until > now() OR last_strike_at > now() - make_interval(secs => ${DECAY_SECONDS})`
    return rows.map((r) => ({
      id: r.id,
      strikes: r.strikes,
      until: r.penalised_until.getTime(),
      lastStrikeAt: r.last_strike_at.getTime(),
    }))
  },

  async upsert(rows) {
    if (rows.length === 0) return
    const values = rows.map((r) => ({
      id: r.id,
      strikes: r.strikes,
      penalised_until: new Date(r.until),
      last_strike_at: new Date(r.lastStrikeAt),
    }))
    // The stricter value wins, as in mergePenalty(): another replica may have
    // boxed the same caller for longer, and this write must not shorten it.
    // Strikes restart rather than accumulate once the stored run has decayed.
    // No table alias: postgres-js picks the helper's form from the keyword
    // before it, and `INSERT INTO t AS p ${...}` makes it build a select list.
    await sql`
      INSERT INTO accounts_throttle_penalties ${sql(values, 'id', 'strikes', 'penalised_until', 'last_strike_at')}
      ON CONFLICT (id) DO UPDATE SET
        strikes = CASE
          WHEN accounts_throttle_penalties.last_strike_at
               < EXCLUDED.last_strike_at - make_interval(secs => ${DECAY_SECONDS})
            THEN EXCLUDED.strikes
          ELSE GREATEST(accounts_throttle_penalties.strikes, EXCLUDED.strikes)
        END,
        penalised_until = GREATEST(accounts_throttle_penalties.penalised_until, EXCLUDED.penalised_until),
        last_strike_at = GREATEST(accounts_throttle_penalties.last_strike_at, EXCLUDED.last_strike_at),
        updated_at = now()`
  },

  async remove(ids) {
    if (ids.length === 0) return
    // Only rows that are no longer boxing anyone: a success here says nothing
    // about a penalty another replica has just written.
    await sql`DELETE FROM accounts_throttle_penalties WHERE id IN ${sql(ids)} AND penalised_until <= now()`
  },

  async prune() {
    const rows = await sql`
      DELETE FROM accounts_throttle_penalties
      WHERE penalised_until <= now() AND last_strike_at <= now() - make_interval(secs => ${DECAY_SECONDS})
      RETURNING id`
    return rows.length
  },
}

/** Write what changed. A failed write is queued again for the next flush. */
export async function flushPenalties(box: PenaltyBox = penaltyBox, store: PenaltyStore = pgPenaltyStore): Promise<void> {
  const batch = box.takeDirty()
  if (batch.upserts.length === 0 && batch.deletes.length === 0) return
  try {
    await store.upsert(batch.upserts)
    await store.remove(batch.deletes)
  } catch (err) {
    box.requeue(batch)
    throw err
  }
}

/** Flush, prune, then fold in everything still live. Also the startup load. */
export async function refreshPenalties(box: PenaltyBox = penaltyBox, store: PenaltyStore = pgPenaltyStore): Promise<void> {
  // Flushed first, so the rows read back include this replica's own changes
  // and the merge does not briefly undo them.
  await flushPenalties(box, store)
  await store.prune()
  box.applyStored(await store.load())
}

/**
 * Replaced rather than skipped on a second call. `bun --hot` re-evaluates
 * throttle.service.ts, which builds a new box; a timer kept from the first
 * evaluation would go on syncing the old one.
 */
const TIMERS_KEY = Symbol.for('barrelman.throttle.penaltySync')

export async function startPenaltySync(): Promise<void> {
  stopPenaltySync()

  const globals = globalThis as Record<symbol, unknown>
  const flush = setInterval(() => {
    flushPenalties().catch((err) => console.error('[throttle] penalty flush failed', err))
  }, FLUSH_MS)
  const refresh = setInterval(() => {
    refreshPenalties().catch((err) => console.error('[throttle] penalty refresh failed', err))
  }, REFRESH_MS)
  flush.unref?.()
  refresh.unref?.()
  globals[TIMERS_KEY] = [flush, refresh]

  // Startup load. Not fatal: the box works from memory alone, which is how it
  // worked before it was persisted, and the next refresh tries again.
  await refreshPenalties().catch((err) => console.error('[throttle] loading penalties failed', err))
}

export function stopPenaltySync(): void {
  const globals = globalThis as Record<symbol, unknown>
  const timers = globals[TIMERS_KEY] as ReturnType<typeof setInterval>[] | undefined
  for (const t of timers ?? []) clearInterval(t)
  globals[TIMERS_KEY] = undefined
}
