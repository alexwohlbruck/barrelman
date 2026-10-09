import { db } from '../db'
import { sql, type SQL } from 'drizzle-orm'

/**
 * Run a read query that Postgres itself gives up on after `ms`, resolving to
 * no rows if it does (or fails at all).
 *
 * A JS-side race (Promise.race against a timer) answers on time but leaves the
 * query running: it holds a pool connection and keeps reading heap until the
 * pool-wide statement timeout, 10s by default. With one search per keystroke,
 * every abandoned query was still competing for I/O with the searches typed
 * after it. A statement timeout set for the transaction cancels it in the
 * server and frees the connection the moment the budget is spent.
 *
 * The statement timeout only starts once the query reaches the server. Time
 * spent waiting for a pool connection is not covered by it, so a JS timer
 * backs it up, a little later, for that case alone.
 *
 * The budget is in milliseconds; 0 or less runs the query under the pool's
 * default timeout only.
 */
export async function executeWithin(query: SQL, ms: number): Promise<any[]> {
  if (ms <= 0) return Array.from(await db.execute(query).catch(() => []) as any[])
  const run = db.transaction(async (tx) => {
    // Sent together, so the transaction's connection pipelines them and the
    // timeout costs no extra round trip. SET LOCAL takes no bind parameters;
    // the value is a rounded integer.
    const [, rows] = await Promise.all([
      tx.execute(sql.raw(`SET LOCAL statement_timeout = ${Math.max(1, Math.round(ms))}`)),
      tx.execute(query),
    ])
    return Array.from(rows as any[])
  }).catch(() => [] as any[])
  let timer: ReturnType<typeof setTimeout> | undefined
  return Promise.race([
    run,
    new Promise<any[]>((resolve) => { timer = setTimeout(() => resolve([]), ms + POOL_WAIT_GRACE_MS) }),
  ]).finally(() => clearTimeout(timer))
}

/** How long past its statement timeout a bounded query may still be waiting
 *  for a connection before the caller stops waiting for it. */
const POOL_WAIT_GRACE_MS = 250
