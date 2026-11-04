/**
 * Integration tests for pack settlement and overage reporting against the real
 * database, with Polar replaced by a fake.
 *
 * The properties here only exist in SQL: a pack written down once per period,
 * a staged batch resent under the same ids after a failure, a lock that keeps
 * two passes apart, and overage staged when an account leaves its plan. A
 * mocked `sql` cannot show any of them.
 *
 * Every row is created by this file under a `ovtest-` id and removed in
 * afterAll. Billing is switched on in-process only, by flipping the config
 * object the services read.
 *
 * Run: bun run test:integration
 */
import { describe, test, expect, afterAll, beforeAll, beforeEach } from 'bun:test'

const DATABASE_URL = process.env.BARRELMAN_INTEGRATION_TESTS ? process.env.DATABASE_URL : undefined

type Mods = {
  sql: typeof import('../db').connection
  overage: typeof import('./overage.service')
  credits: typeof import('./credits.service')
  billingSvc: typeof import('./billing.service')
  billing: typeof import('../config/billing.config').billing
}
let m: Mods | null = null

if (DATABASE_URL) {
  const dbMod = await import('../db')
  const accounts = await import('./accounts.service')
  await accounts.ensureAccountsSchema()
  m = {
    sql: dbMod.connection,
    overage: await import('./overage.service'),
    credits: await import('./credits.service'),
    billingSvc: await import('./billing.service'),
    billing: (await import('../config/billing.config')).billing,
  }
}

const PREFIX = `ovtest-${Date.now()}-`
let n = 0
const ingested: Array<{ externalCustomerId: string; externalId: string; metadata: { credits: number } }> = []
let failNext = false

async function makeUser(plan: string, anchor: string | null = null): Promise<string> {
  const id = `${PREFIX}${(n += 1)}`
  await m!.sql`
    INSERT INTO accounts_users (id, email, email_normalized, plan, billing_period_start)
    VALUES (${id}, ${`${id}@example.test`}, ${`${id}@example.test`}, ${plan}, ${anchor})`
  return id
}

async function use(userId: string, day: string, credits: number): Promise<void> {
  await m!.sql`
    INSERT INTO accounts_usage (user_id, day, endpoint, requests, credits)
    VALUES (${userId}, ${day}, 'search', 1, ${credits})`
}

async function reports(userId: string): Promise<Array<{ cycle: string; reported: number; pending: number | null }>> {
  const rows = await m!.sql<Array<{ cycle: string; reported: number; pending: number | null }>>`
    SELECT cycle::text AS cycle, reported_credits AS reported, pending_credits AS pending
    FROM accounts_overage_reports WHERE user_id = ${userId} ORDER BY cycle`
  return [...rows]
}

const saved = { enabled: false, meterUsage: false }

beforeAll(() => {
  if (!m) return
  saved.enabled = m.billing.enabled
  saved.meterUsage = m.billing.meterUsage
  m.billing.enabled = true
  m.billing.meterUsage = true
  m.billingSvc.__setPolarClient({
    events: {
      ingest: async ({ events }: { events: typeof ingested }) => {
        if (failNext) {
          failNext = false
          throw new Error('polar is down')
        }
        const seen = new Set(ingested.map((event) => event.externalId))
        const fresh = events.filter((event) => !seen.has(event.externalId))
        ingested.push(...fresh)
        return { inserted: fresh.length, duplicates: events.length - fresh.length }
      },
    },
  } as never)
})

beforeEach(() => {
  ingested.length = 0
  failNext = false
})

afterAll(async () => {
  if (!m) return
  m.billing.enabled = saved.enabled
  m.billing.meterUsage = saved.meterUsage
  m.billingSvc.__setPolarClient(null)
  await m.sql`DELETE FROM accounts_users WHERE id LIKE ${`${PREFIX}%`}`
})

describe.skipIf(!DATABASE_URL)('pack settlement', () => {
  test('a pack spent in a closed month is not available again the next month', async () => {
    const { credits, sql } = m!
    const user = await makeUser('free')
    const lastMonth = new Date()
    lastMonth.setUTCDate(1)
    lastMonth.setUTCMonth(lastMonth.getUTCMonth() - 1)
    const lastMonthDay = lastMonth.toISOString().slice(0, 10)

    await credits.grantCredits({ userId: user, amount: 50_000, kind: 'purchase', externalId: `${user}-order` })
    await sql`UPDATE accounts_credit_ledger SET created_at = ${lastMonth.toISOString()} WHERE user_id = ${user}`
    await use(user, lastMonthDay, 130_000)

    credits.invalidateBalance(user)
    const balance = await credits.getBalance(user)
    expect(balance.purchased).toBe(20_000)
    expect(balance.remaining).toBe(100_000 + 20_000)

    // Settling again writes nothing more.
    expect(await credits.settlePackUsage(user, balance.plan, null)).toBe(0)
    const [row] = await sql<Array<{ count: number }>>`
      SELECT count(*)::int AS count FROM accounts_credit_ledger WHERE user_id = ${user} AND kind = 'usage'`
    expect(row!.count).toBe(1)
  })
})

describe.skipIf(!DATABASE_URL)('overage reporting', () => {
  const now = new Date('2026-11-14T00:10:00Z')

  test('reports the closed period and sends a deterministic id', async () => {
    const user = await makeUser('developer', '2026-10-14T15:00:00Z')
    await use(user, '2026-10-20', 1_000_000)
    await use(user, '2026-11-13', 80_000)

    const result = await m!.overage.reportPendingOverage({ now })

    expect(result.credits).toBe(80_000)
    const mine = ingested.filter((event) => event.externalCustomerId === user)
    expect(mine).toEqual([
      expect.objectContaining({ externalId: m!.overage.overageEventId(user, '2026-10-14', 80_000), metadata: { credits: 80_000 } }),
    ])
    expect(await reports(user)).toEqual([{ cycle: '2026-10-14', reported: 80_000, pending: null }])
  })

  test('a failed send is kept and resent unchanged, even after more usage', async () => {
    const user = await makeUser('developer', '2026-10-14T15:00:00Z')
    await use(user, '2026-11-14', 1_010_000)
    const later = new Date('2026-11-20T00:00:00Z')

    failNext = true
    expect((await m!.overage.reportPendingOverage({ now: later })).credits).toBe(0)
    expect(await reports(user)).toEqual([{ cycle: '2026-11-14', reported: 0, pending: 10_000 }])

    await use(user, '2026-11-15', 5_000)
    await m!.overage.reportPendingOverage({ now: later })

    // First the staged 10k under its original id, then the 5k that came after.
    const mine = ingested.filter((event) => event.externalCustomerId === user).map((event) => event.externalId)
    expect(mine).toEqual([
      m!.overage.overageEventId(user, '2026-11-14', 10_000),
      m!.overage.overageEventId(user, '2026-11-14', 15_000),
    ])
    expect(await reports(user)).toEqual([{ cycle: '2026-11-14', reported: 15_000, pending: null }])
  })

  test('a pass is skipped while another replica holds the lock', async () => {
    const user = await makeUser('developer', '2026-10-14T15:00:00Z')
    await use(user, '2026-11-14', 1_001_000)

    const holder = await m!.sql.reserve()
    try {
      await holder`SELECT pg_advisory_lock(${0x6f766572}::int, 1)`
      expect(await m!.overage.reportPendingOverage({ now: new Date('2026-11-20T00:00:00Z') })).toEqual({ accounts: 0, credits: 0 })
    } finally {
      await holder`SELECT pg_advisory_unlock(${0x6f766572}::int, 1)`
      holder.release()
    }
    expect(ingested.filter((event) => event.externalCustomerId === user)).toEqual([])
  })

  test('overage accrued before a downgrade to free is staged and then sent', async () => {
    // Anchored today, so the open period is the one the real clock is in.
    const today = new Date().toISOString().slice(0, 10)
    const user = await makeUser('developer', today)
    await use(user, today, 1_040_000)

    await m!.credits.setPlan(user, 'free')
    const staged = await reports(user)
    expect(staged).toHaveLength(1)
    expect(staged[0]).toMatchObject({ reported: 0, pending: 40_000 })

    await m!.overage.reportPendingOverage()
    expect(ingested.filter((event) => event.externalCustomerId === user).map((event) => event.metadata.credits)).toEqual([
      40_000,
    ])
  })

  test('suspended accounts are still billed for what they used', async () => {
    const user = await makeUser('developer', '2026-10-14T15:00:00Z')
    await m!.sql`UPDATE accounts_users SET suspended_at = now() WHERE id = ${user}`
    await use(user, '2026-11-14', 1_002_000)

    await m!.overage.reportPendingOverage({ now: new Date('2026-11-20T00:00:00Z') })
    expect(ingested.filter((event) => event.externalCustomerId === user).map((event) => event.metadata.credits)).toEqual([
      2_000,
    ])
  })

  test('reporting one account throws when Polar refuses, and leaves it staged', async () => {
    const user = await makeUser('developer', '2026-10-14T15:00:00Z')
    await use(user, '2026-11-14', 1_004_000)
    const later = new Date('2026-11-20T00:00:00Z')

    failNext = true
    await expect(m!.overage.reportPendingOverage({ userId: user, now: later })).rejects.toThrow(/refused/)
    expect(await reports(user)).toEqual([{ cycle: '2026-11-14', reported: 0, pending: 4_000 }])

    await m!.overage.reportPendingOverage({ userId: user, now: later })
    expect(await reports(user)).toEqual([{ cycle: '2026-11-14', reported: 4_000, pending: null }])
  })

  test('does nothing while billing is disabled', async () => {
    const user = await makeUser('developer', '2026-10-14T15:00:00Z')
    await use(user, '2026-11-14', 1_003_000)

    m!.billing.enabled = false
    try {
      expect(await m!.overage.reportPendingOverage({ now: new Date('2026-11-20T00:00:00Z') })).toEqual({ accounts: 0, credits: 0 })
      await m!.credits.setPlan(user, 'free')
    } finally {
      m!.billing.enabled = true
    }
    expect(await reports(user)).toEqual([])
  })
})
