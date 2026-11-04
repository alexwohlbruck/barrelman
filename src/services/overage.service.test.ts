/**
 * Tests for the overage delta computation — the code that decides what a
 * customer is invoiced for beyond their subscription.
 *
 * The property under test throughout is idempotence. This runs on a timer in a
 * process that can die mid-pass, so reporting must be expressed as "the total
 * for this cycle, minus what has already been reported", never as "usage since
 * last time". A regression here bills people twice.
 */
import { describe, test, expect } from 'bun:test'
import {
  buildOverageRows,
  computePendingOverage,
  overageEventId,
  type AccountUsage,
  type OverageRow,
} from './overage.service'
import { getPlan } from '../billing/plans'

const developer = getPlan('developer') // 1,000,000 credits, overage allowed
const free = getPlan('free') // 100,000 credits, no overage

function row(overrides: Partial<OverageRow> = {}): OverageRow {
  return {
    user_id: 'user-1',
    plan: 'developer',
    cycle: '2026-11-01',
    used: 0,
    purchased: 0,
    reported: 0,
    ...overrides,
  }
}

describe('accounts with nothing to report', () => {
  test('an account inside its allowance', () => {
    expect(computePendingOverage([row({ used: 500_000 })])).toEqual([])
  })

  test('an account exactly at its allowance', () => {
    expect(computePendingOverage([row({ used: developer.monthlyCredits })])).toEqual([])
  })

  test('a free account, however far it overshoots', () => {
    // The free plan has no overage, so it can never be invoiced — the quota
    // check is supposed to have stopped it long before this.
    const rows = [row({ plan: 'free', used: free.monthlyCredits * 50 })]

    expect(computePendingOverage(rows)).toEqual([])
  })

  test('an unknown plan id falls back to free and reports nothing', () => {
    const rows = [row({ plan: 'legacy-tier-that-no-longer-exists', used: 99_000_000 })]

    expect(computePendingOverage(rows)).toEqual([])
  })
})

describe('the delta against what was already reported', () => {
  test('first pass reports the whole overage', () => {
    const rows = [row({ used: developer.monthlyCredits + 50_000, reported: 0 })]

    expect(computePendingOverage(rows)).toEqual([{ userId: 'user-1', cycle: '2026-11-01', credits: 50_000, total: 50_000 }])
  })

  test('a later pass reports only what is new', () => {
    const rows = [row({ used: developer.monthlyCredits + 80_000, reported: 50_000 })]

    expect(computePendingOverage(rows)).toEqual([{ userId: 'user-1', cycle: '2026-11-01', credits: 30_000, total: 80_000 }])
  })

  test('re-running with nothing new reports nothing', () => {
    // The idempotence property: a pass that crashed after ingesting but before
    // recording is retried, and a pass with no new usage is a no-op.
    const rows = [row({ used: developer.monthlyCredits + 80_000, reported: 80_000 })]

    expect(computePendingOverage(rows)).toEqual([])
  })

  test('a reported figure ahead of usage never produces a negative charge', () => {
    // Should not happen, but a credit refund or an adjustment could get here.
    // Emitting a negative would credit the customer through the usage meter,
    // which is not what that channel is for.
    const rows = [row({ used: developer.monthlyCredits + 10_000, reported: 40_000 })]

    expect(computePendingOverage(rows)).toEqual([])
  })

  test('the stored total is the cycle total, not the delta', () => {
    // Storing the delta would make the next pass compute against the wrong
    // baseline and re-report everything before it.
    const rows = [row({ used: developer.monthlyCredits + 120_000, reported: 100_000 })]
    const [pending] = computePendingOverage(rows)

    expect(pending!.credits).toBe(20_000)
    expect(pending!.total).toBe(120_000)
  })
})

describe('purchased credits', () => {
  test('are consumed before any overage is billed', () => {
    const rows = [row({ used: developer.monthlyCredits + 30_000, purchased: 50_000 })]

    // Billing these would charge twice for credits already paid for.
    expect(computePendingOverage(rows)).toEqual([])
  })

  test('overage begins only past the pack', () => {
    const rows = [row({ used: developer.monthlyCredits + 70_000, purchased: 50_000 })]

    expect(computePendingOverage(rows)).toEqual([{ userId: 'user-1', cycle: '2026-11-01', credits: 20_000, total: 20_000 }])
  })

  test('a pack bought mid-cycle retroactively reduces what is owed', () => {
    // 60k already reported, then the customer buys 50k. The recomputed total is
    // 10k, below what was reported, so nothing further is charged.
    const rows = [row({ used: developer.monthlyCredits + 60_000, purchased: 50_000, reported: 60_000 })]

    expect(computePendingOverage(rows)).toEqual([])
  })
})

describe('batches', () => {
  test('reports each account independently', () => {
    const rows = [
      row({ user_id: 'a', used: developer.monthlyCredits + 10_000 }),
      row({ user_id: 'b', used: 5_000 }),
      row({ user_id: 'c', plan: 'free', used: free.monthlyCredits * 3 }),
      row({ user_id: 'd', used: developer.monthlyCredits + 90_000, reported: 40_000 }),
    ]

    expect(computePendingOverage(rows)).toEqual([
      { userId: 'a', cycle: '2026-11-01', credits: 10_000, total: 10_000 },
      { userId: 'd', cycle: '2026-11-01', credits: 50_000, total: 90_000 },
    ])
  })

  test('an empty result set produces no work', () => {
    expect(computePendingOverage([])).toEqual([])
  })
})

describe('event ids', () => {
  test('are the same for the same account, period and total', () => {
    // What lets a failed batch be resent: Polar skips an id it already has.
    expect(overageEventId('u', '2026-10-14', 5_000)).toBe(overageEventId('u', '2026-10-14', 5_000))
    expect(overageEventId('u', '2026-10-14', 5_000)).not.toBe(overageEventId('u', '2026-10-14', 6_000))
    expect(overageEventId('u', '2026-10-14', 5_000)).not.toBe(overageEventId('u', '2026-11-14', 5_000))
  })
})

describe('buildOverageRows — period boundaries', () => {
  function account(overrides: Partial<AccountUsage> = {}): AccountUsage {
    return {
      userId: 'user-1',
      plan: 'developer',
      anchor: '2026-10-14T15:00:00Z',
      days: [],
      ledgerBalance: 0,
      packUsage: {},
      reported: {},
      ...overrides,
    }
  }

  test('the closed period is still reported after it rolls over', () => {
    // 80k of overage in the period ending 14 November; the last pass before
    // the boundary had sent 50k. The first pass after it must send the rest.
    const now = new Date('2026-11-14T00:10:00Z')
    const rows = buildOverageRows(
      [
        account({
          days: [
            { day: '2026-10-20', credits: developer.monthlyCredits },
            { day: '2026-11-13', credits: 80_000 },
            { day: '2026-11-14', credits: 1_000 },
          ],
          reported: { '2026-10-14': 50_000 },
        }),
      ],
      now,
    )

    expect(computePendingOverage(rows)).toEqual([{ userId: 'user-1', cycle: '2026-10-14', credits: 30_000, total: 80_000 }])
  })

  test('periods follow the subscription, not the calendar month', () => {
    const rows = buildOverageRows([account({ days: [{ day: '2026-11-02', credits: 5 }] })], new Date('2026-11-04T00:00:00Z'))

    expect(rows.map((row) => row.cycle)).toEqual(['2026-10-14'])
  })

  test('a closed period is charged against what it took from the pack, not the current balance', () => {
    const rows = buildOverageRows(
      [
        account({
          days: [{ day: '2026-11-10', credits: developer.monthlyCredits + 30_000 }],
          packUsage: { '2026-10-14': 20_000 },
          ledgerBalance: 0,
        }),
      ],
      new Date('2026-11-15T00:00:00Z'),
    )

    expect(computePendingOverage(rows)).toEqual([{ userId: 'user-1', cycle: '2026-10-14', credits: 10_000, total: 10_000 }])
  })

  test('overage is reported on the plan an account is leaving, even for free', () => {
    // A downgrade to free mid-period. Against the free plan nothing is owed;
    // against the developer plan it was on, 40k is.
    const accounts = [account({ plan: 'free', days: [{ day: '2026-11-02', credits: developer.monthlyCredits + 40_000 }] })]
    const now = new Date('2026-11-04T00:00:00Z')

    expect(computePendingOverage(buildOverageRows(accounts, now))).toEqual([])
    expect(computePendingOverage(buildOverageRows(accounts, now, developer), developer)).toEqual([
      { userId: 'user-1', cycle: '2026-10-14', credits: 40_000, total: 40_000 },
    ])
  })
})
