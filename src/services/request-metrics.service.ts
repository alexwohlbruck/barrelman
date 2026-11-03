/**
 * Operational request metrics: how each metered request was answered.
 *
 * `accounts_usage` says what an account spent. It cannot say whether that
 * account is being throttled, by which layer, or how slow its responses are —
 * its single `rejected` counter lumps a 429 in with a bad key, and nothing
 * anywhere recorded latency. This fills that gap, for operators.
 *
 * Same shape as the usage meter, for the same reason: autocomplete is a request
 * per keystroke, so counters are buffered in memory and upserted on an interval.
 * Rows are hourly per (account, key, endpoint group). Service-key and anonymous
 * traffic is recorded too, under sentinel ids, since the busiest caller on a
 * deployment is usually its own first-party app and leaving it out would leave
 * the latency figures describing everybody except the traffic that matters.
 *
 * Latency is kept as a fixed-bucket histogram rather than raw samples, so rows
 * from any number of replicas and hours sum into a percentile estimate.
 */
import { connection as sql } from '../db'
import { envNumber } from '../config/env'

const FLUSH_INTERVAL_MS = envNumber('BARRELMAN_USAGE_FLUSH_MS', 5_000)
const MAX_BUFFERED_ROWS = 500
const RETENTION_DAYS = envNumber('BARRELMAN_REQUEST_METRICS_RETENTION_DAYS', 90)

/** Recorded in place of a user id for callers that have no account. */
export const SERVICE_SUBJECT = '@service'
export const ANONYMOUS_SUBJECT = '@anonymous'
/**
 * Requests that never reached the guard — Elysia rejects a malformed query in
 * validation, before `onBeforeHandle` runs — so the caller was never identified.
 */
export const UNIDENTIFIED_SUBJECT = '@unidentified'
const NO_KEY = '-'

/**
 * Upper bounds, in milliseconds, of every histogram bucket but the last, which
 * is open-ended. Spaced for an API whose answers range from a cached tile to a
 * multi-second isochrone.
 */
export const LATENCY_BOUNDS_MS = [10, 25, 50, 100, 250, 500, 1_000, 2_500, 5_000, 10_000] as const
const BUCKETS = LATENCY_BOUNDS_MS.length + 1

/** Why the guard refused a request. Each maps to one counter column. */
export type Refusal = 'ip' | 'key' | 'account' | 'concurrency' | 'penalty' | 'quota' | 'denied'

const REFUSAL_COLUMN: Record<Refusal, keyof Counters> = {
  ip: 'limitedIp',
  key: 'limitedKey',
  account: 'limitedAccount',
  concurrency: 'limitedConcurrency',
  penalty: 'penalised',
  quota: 'quotaRefused',
  denied: 'denied',
}

interface Counters {
  requests: number
  served: number
  clientErrors: number
  serverErrors: number
  limitedIp: number
  limitedKey: number
  limitedAccount: number
  limitedConcurrency: number
  penalised: number
  quotaRefused: number
  denied: number
  latencyMsSum: number
}

interface BufferEntry extends Counters {
  hour: string
  userId: string
  apiKeyId: string
  endpoint: string
  hist: number[]
}

const buffer = new Map<string, BufferEntry>()

function emptyCounters(): Counters {
  return {
    requests: 0,
    served: 0,
    clientErrors: 0,
    serverErrors: 0,
    limitedIp: 0,
    limitedKey: 0,
    limitedAccount: 0,
    limitedConcurrency: 0,
    penalised: 0,
    quotaRefused: 0,
    denied: 0,
    latencyMsSum: 0,
  }
}

export function latencyBucket(ms: number): number {
  const index = LATENCY_BOUNDS_MS.findIndex((bound) => ms <= bound)
  return index === -1 ? BUCKETS - 1 : index
}

function hourStamp(date: Date): string {
  const hour = new Date(date)
  hour.setUTCMinutes(0, 0, 0)
  return hour.toISOString()
}

export interface RequestOutcome {
  /** Account id, or one of the sentinel subjects above. */
  subject: string
  apiKeyId?: string | null
  endpoint: string
  status: number
  durationMs: number
  /** Set when the guard refused the request rather than a handler answering it. */
  refusal?: Refusal
  at?: Date
}

/** Buffer one request's outcome. Never awaited on the request path. */
export function recordRequest(outcome: RequestOutcome): void {
  const hour = hourStamp(outcome.at ?? new Date())
  const apiKeyId = outcome.apiKeyId || NO_KEY
  const mapKey = `${hour} ${outcome.subject} ${apiKeyId} ${outcome.endpoint}`

  let entry = buffer.get(mapKey)
  if (!entry) {
    entry = {
      hour,
      userId: outcome.subject,
      apiKeyId,
      endpoint: outcome.endpoint,
      ...emptyCounters(),
      hist: new Array(BUCKETS).fill(0),
    }
    buffer.set(mapKey, entry)
  }

  entry.requests += 1
  if (outcome.refusal) {
    entry[REFUSAL_COLUMN[outcome.refusal]] += 1
  } else {
    // Latency only for requests a handler answered. A refusal is a map lookup
    // and returns in microseconds; counting those would make a heavily
    // throttled endpoint look fast.
    entry.served += 1
    if (outcome.status >= 500) entry.serverErrors += 1
    else if (outcome.status >= 400) entry.clientErrors += 1
    const ms = Math.max(0, Math.round(outcome.durationMs))
    entry.latencyMsSum += ms
    entry.hist[latencyBucket(ms)]! += 1
  }

  if (buffer.size >= MAX_BUFFERED_ROWS) void flushRequestMetrics()
}

// ── Flush ───────────────────────────────────────────────────────────────

/** Column per counter, in one place so the insert and the upsert agree. */
const COLUMNS: [keyof Counters, string][] = [
  ['requests', 'requests'],
  ['served', 'served'],
  ['clientErrors', 'client_errors'],
  ['serverErrors', 'server_errors'],
  ['limitedIp', 'limited_ip'],
  ['limitedKey', 'limited_key'],
  ['limitedAccount', 'limited_account'],
  ['limitedConcurrency', 'limited_concurrency'],
  ['penalised', 'penalised'],
  ['quotaRefused', 'quota_refused'],
  ['denied', 'denied'],
  ['latencyMsSum', 'latency_ms_sum'],
]

/**
 * Element-wise histogram addition, spelled out per index. `unnest` over two
 * arrays would read better, but nothing guarantees the order it hands rows
 * back in, and a shuffled histogram is silently wrong rather than broken.
 */
const HIST_MERGE = `ARRAY[${Array.from(
  { length: BUCKETS },
  (_, i) => `accounts_request_metrics.latency_hist[${i + 1}] + EXCLUDED.latency_hist[${i + 1}]`,
).join(', ')}]`

const UPSERT_SET = [
  ...COLUMNS.map(([, column]) => `${column} = accounts_request_metrics.${column} + EXCLUDED.${column}`),
  `latency_hist = ${HIST_MERGE}`,
].join(',\n  ')

let flushing: Promise<void> | null = null

export async function flushRequestMetrics(): Promise<void> {
  if (flushing) return flushing
  if (buffer.size === 0) return

  const rows = [...buffer.values()]
  buffer.clear()

  flushing = (async () => {
    try {
      const columns = ['hour', 'user_id', 'api_key_id', 'endpoint', ...COLUMNS.map(([, c]) => c), 'latency_hist']
      const params: unknown[] = []
      const tuples = rows.map((row) => {
        const values = [
          row.hour,
          row.userId,
          row.apiKeyId,
          row.endpoint,
          ...COLUMNS.map(([field]) => row[field]),
          `{${row.hist.join(',')}}`,
        ]
        const start = params.length
        params.push(...values)
        return `(${values.map((_, i) => `$${start + i + 1}`).join(', ')})`
      })
      // Built by hand rather than with postgres.js's row helper, which would
      // need the array column's type spelled out per row. Every identifier
      // here is a constant; every value is a bound parameter.
      await sql.unsafe(
        `INSERT INTO accounts_request_metrics (${columns.join(', ')})
         SELECT hour::timestamptz, user_id, api_key_id, endpoint, ${COLUMNS.map(([, c]) => `${c}::bigint`).join(', ')}, latency_hist::integer[]
         FROM (VALUES ${tuples.join(', ')}) AS v(${columns.join(', ')})
         ON CONFLICT (hour, user_id, api_key_id, endpoint) DO UPDATE SET
           ${UPSERT_SET}`,
        params as never[],
      )
    } catch (err) {
      // Put the counters back so a transient database error costs nothing.
      for (const row of rows) {
        const mapKey = `${row.hour} ${row.userId} ${row.apiKeyId} ${row.endpoint}`
        const current = buffer.get(mapKey)
        if (!current) {
          buffer.set(mapKey, row)
          continue
        }
        for (const [field] of COLUMNS) current[field] += row[field]
        row.hist.forEach((n, i) => (current.hist[i]! += n))
      }
      console.error('[request-metrics] flush failed, counters retained for retry', err)
    } finally {
      flushing = null
    }
  })()

  return flushing
}

const INTERVAL_KEY = Symbol.for('barrelman.requestMetrics.flushInterval')

/** Guarded on globalThis so `bun --hot` does not stack intervals. */
export function startRequestMetricsFlush(): void {
  const globals = globalThis as Record<symbol, unknown>
  if (globals[INTERVAL_KEY]) return
  const timer = setInterval(() => void flushRequestMetrics(), FLUSH_INTERVAL_MS)
  timer.unref?.()
  globals[INTERVAL_KEY] = timer
}

/** Hourly rows age out; the billing record in accounts_usage is kept forever. */
export async function pruneRequestMetrics(): Promise<number> {
  const deleted = await sql`
    DELETE FROM accounts_request_metrics
    WHERE hour < now() - make_interval(days => ${RETENTION_DAYS})
    RETURNING 1`
  return deleted.length
}

/** Exposed for tests. */
export function bufferedRequestMetrics(): BufferEntry[] {
  return [...buffer.values()]
}

export function clearRequestMetricsBuffer(): void {
  buffer.clear()
}

// ── Reporting ───────────────────────────────────────────────────────────

export interface TrafficStats {
  requests: number
  served: number
  clientErrors: number
  serverErrors: number
  /** Refused by any throttle layer, penalty box included. */
  rateLimited: number
  limitedBy: { ip: number; key: number; account: number; concurrency: number; penalty: number }
  quotaRefused: number
  denied: number
  /** Milliseconds, over served requests. Null when nothing was served. */
  avgMs: number | null
  p50Ms: number | null
  p95Ms: number | null
  p99Ms: number | null
}

/**
 * Estimate a percentile from the histogram, interpolating linearly inside the
 * bucket it falls in. The open top bucket reports its lower bound — "at least
 * ten seconds" is the honest answer there.
 */
export function percentileFromHistogram(hist: number[], p: number): number | null {
  const total = hist.reduce((sum, n) => sum + n, 0)
  if (total === 0) return null

  const target = p * total
  let seen = 0
  for (let i = 0; i < hist.length; i++) {
    const count = hist[i] ?? 0
    if (count === 0) continue
    if (seen + count >= target) {
      const lower = i === 0 ? 0 : LATENCY_BOUNDS_MS[i - 1]!
      const upper = LATENCY_BOUNDS_MS[i]
      if (upper === undefined) return lower
      return Math.round(lower + ((target - seen) / count) * (upper - lower))
    }
    seen += count
  }
  return LATENCY_BOUNDS_MS[LATENCY_BOUNDS_MS.length - 1]!
}

interface RawRow {
  dim: string
  dim2: string
  requests: string
  served: string
  client_errors: string
  server_errors: string
  limited_ip: string
  limited_key: string
  limited_account: string
  limited_concurrency: string
  penalised: string
  quota_refused: string
  denied: string
  latency_ms_sum: string
  hist: string[]
}

export function toStats(row: RawRow): TrafficStats {
  const n = (v: string) => Number(v ?? 0)
  const hist = (row.hist ?? []).map(Number)
  const limitedBy = {
    ip: n(row.limited_ip),
    key: n(row.limited_key),
    account: n(row.limited_account),
    concurrency: n(row.limited_concurrency),
    penalty: n(row.penalised),
  }
  const served = n(row.served)
  return {
    requests: n(row.requests),
    served,
    clientErrors: n(row.client_errors),
    serverErrors: n(row.server_errors),
    rateLimited: Object.values(limitedBy).reduce((sum, v) => sum + v, 0),
    limitedBy,
    quotaRefused: n(row.quota_refused),
    denied: n(row.denied),
    avgMs: served ? Math.round(n(row.latency_ms_sum) / served) : null,
    p50Ms: percentileFromHistogram(hist, 0.5),
    p95Ms: percentileFromHistogram(hist, 0.95),
    p99Ms: percentileFromHistogram(hist, 0.99),
  }
}

const SUMS = [
  ...COLUMNS.map(([, column]) => `SUM(${column})::bigint AS ${column}`),
  `ARRAY[${Array.from({ length: BUCKETS }, (_, i) => `SUM(latency_hist[${i + 1}])`).join(', ')}]::bigint[] AS hist`,
].join(', ')

/** The groupings a report can ask for. Constants, so safe to interpolate. */
const DIMENSIONS = {
  total: [`''`, `''`],
  endpoint: ['endpoint', `''`],
  caller: ['user_id', 'api_key_id'],
  // `AT TIME ZONE 'UTC'` because to_char renders a timestamptz in the
  // session's zone, and the buckets the console draws are UTC.
  hour: [`to_char(hour AT TIME ZONE 'UTC', 'YYYY-MM-DD"T"HH24:00:00"Z"')`, `''`],
  day: [`to_char(hour AT TIME ZONE 'UTC', 'YYYY-MM-DD')`, `''`],
} as const

async function aggregate(
  dimension: keyof typeof DIMENSIONS,
  from: Date,
  userId?: string,
): Promise<RawRow[]> {
  const [dim, dim2] = DIMENSIONS[dimension]
  const where = userId ? 'hour >= $1 AND user_id = $2' : 'hour >= $1'
  const params = userId ? [from.toISOString(), userId] : [from.toISOString()]
  return (await sql.unsafe(
    `SELECT ${dim} AS dim, ${dim2} AS dim2, ${SUMS}
     FROM accounts_request_metrics
     WHERE ${where}
     GROUP BY 1, 2
     ORDER BY 1, 2`,
    params,
  )) as unknown as RawRow[]
}

export type TrafficRange = '24h' | '7d' | '30d'

const RANGE_HOURS: Record<TrafficRange, number> = { '24h': 24, '7d': 24 * 7, '30d': 24 * 30 }

export interface TrafficPoint extends TrafficStats {
  /** ISO hour for a 24-hour range, a UTC day otherwise. */
  t: string
}

export interface CallerTraffic extends TrafficStats {
  /** Account id, or a sentinel subject for service and anonymous traffic. */
  subject: string
  apiKeyId: string | null
  email: string | null
  plan: string | null
  keyName: string | null
  keyPrefix: string | null
  rateLimitExempt: boolean
  revoked: boolean
}

export interface TrafficReport {
  range: TrafficRange
  from: string
  bucket: 'hour' | 'day'
  latencyBoundsMs: readonly number[]
  totals: TrafficStats
  series: TrafficPoint[]
  byEndpoint: (TrafficStats & { endpoint: string })[]
  byCaller: CallerTraffic[]
}

/**
 * Everything the console's traffic view needs, for the whole instance or one
 * account. Buffered counters are flushed first so a view opened right after a
 * burst shows the burst.
 */
export async function trafficReport(range: TrafficRange, userId?: string): Promise<TrafficReport> {
  await flushRequestMetrics()

  const bucket = range === '24h' ? 'hour' : 'day'
  const from = new Date(Date.now() - RANGE_HOURS[range] * 3_600_000)
  from.setUTCMinutes(0, 0, 0)

  const [totals, series, endpoints, callers] = await Promise.all([
    aggregate('total', from, userId),
    aggregate(bucket, from, userId),
    aggregate('endpoint', from, userId),
    aggregate('caller', from, userId),
  ])

  const byCaller = await describeCallers(callers)

  return {
    range,
    from: from.toISOString(),
    bucket,
    latencyBoundsMs: LATENCY_BOUNDS_MS,
    totals: totals[0] ? toStats(totals[0]) : toStats({} as RawRow),
    series: fillSeries(
      series.map((row) => ({ t: row.dim, ...toStats(row) })),
      from,
      bucket,
    ),
    byEndpoint: endpoints
      .map((row) => ({ endpoint: row.dim, ...toStats(row) }))
      .sort((a, b) => b.requests - a.requests),
    byCaller,
  }
}

/** Callers most worth an operator's attention first: the ones being refused. */
const CALLER_LIMIT = 100

async function describeCallers(rows: RawRow[]): Promise<CallerTraffic[]> {
  const ranked = rows
    .map((row) => ({ row, stats: toStats(row) }))
    .sort((a, b) => b.stats.rateLimited - a.stats.rateLimited || b.stats.requests - a.stats.requests)
    .slice(0, CALLER_LIMIT)

  const userIds = [...new Set(ranked.map(({ row }) => row.dim).filter((id) => !id.startsWith('@')))]
  const keyIds = [...new Set(ranked.map(({ row }) => row.dim2).filter((id) => id !== NO_KEY))]

  const [accounts, keys] = await Promise.all([
    userIds.length
      ? sql<{ id: string; email: string; plan: string }[]>`
          SELECT id, email, plan FROM accounts_users WHERE id IN ${sql(userIds)}`
      : [],
    keyIds.length
      ? sql<{ id: string; name: string; prefix: string; rate_limit_exempt: boolean; revoked_at: Date | null }[]>`
          SELECT id, name, prefix, rate_limit_exempt, revoked_at FROM accounts_api_keys WHERE id IN ${sql(keyIds)}`
      : [],
  ])
  const accountById = new Map(accounts.map((a) => [a.id, a]))
  const keyById = new Map(keys.map((k) => [k.id, k]))

  return ranked.map(({ row, stats }) => {
    const account = accountById.get(row.dim)
    const key = keyById.get(row.dim2)
    return {
      subject: row.dim,
      apiKeyId: row.dim2 === NO_KEY ? null : row.dim2,
      email: account?.email ?? null,
      plan: account?.plan ?? null,
      keyName: key?.name ?? null,
      keyPrefix: key?.prefix ?? null,
      rateLimitExempt: key?.rate_limit_exempt ?? false,
      revoked: Boolean(key?.revoked_at),
      ...stats,
    }
  })
}

/** Zero-fill the gaps, so a quiet hour reads as a gap in the chart rather than vanishing. */
function fillSeries(points: TrafficPoint[], from: Date, bucket: 'hour' | 'day'): TrafficPoint[] {
  const byT = new Map(points.map((p) => [p.t, p]))
  const empty = toStats({} as RawRow)
  const out: TrafficPoint[] = []
  const step = bucket === 'hour' ? 3_600_000 : 86_400_000
  const cursor = new Date(from)
  if (bucket === 'day') cursor.setUTCHours(0)
  for (; cursor.getTime() <= Date.now(); cursor.setTime(cursor.getTime() + step)) {
    const t = bucket === 'hour' ? cursor.toISOString().replace('.000Z', 'Z') : cursor.toISOString().slice(0, 10)
    out.push(byT.get(t) ?? { t, ...empty })
  }
  return out
}
