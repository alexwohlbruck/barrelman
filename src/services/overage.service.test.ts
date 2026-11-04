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
    cycle_end: '2026-12-01',
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

    expect(computePendingOverage(rows)).toEqual([{ userId: 'user-1', cycle: '2026-11-01', cycleEnd: '2026-12-01', credits: 50_000, total: 50_000 }])
  })

  test('a later pass reports only what is new', () => {
    const rows = [row({ used: developer.monthlyCredits + 80_000, reported: 50_000 })]

    expect(computePendingOverage(rows)).toEqual([{ userId: 'user-1', cycle: '2026-11-01', cycleEnd: '2026-12-01', credits: 30_000, total: 80_000 }])
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

    expect(computePendingOverage(rows)).toEqual([{ userId: 'user-1', cycle: '2026-11-01', cycleEnd: '2026-12-01', credits: 20_000, total: 20_000 }])
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
      { userId: 'a', cycle: '2026-11-01', cycleEnd: '2026-12-01', credits: 10_000, total: 10_000 },
      { userId: 'd', cycle: '2026-11-01', cycleEnd: '2026-12-01', credits: 50_000, total: 90_000 },
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
      meteredFrom: null,
      days: [],
      ledgerBalance: 0,
      packUsage: {},
      reports: {},
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
          reports: { '2026-10-14': 50_000 },
        }),
      ],
      now,
    )

    expect(computePendingOverage(rows)).toEqual([{ userId: 'user-1', cycle: '2026-10-14', cycleEnd: '2026-11-14', credits: 30_000, total: 80_000 }])
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

    expect(computePendingOverage(rows)).toEqual([{ userId: 'user-1', cycle: '2026-10-14', cycleEnd: '2026-11-14', credits: 10_000, total: 10_000 }])
  })

  test('overage is reported on the plan an account is leaving, even for free', () => {
    // A downgrade to free mid-period. Against the free plan nothing is owed;
    // against the developer plan it was on, 40k is.
    const accounts = [account({ plan: 'free', days: [{ day: '2026-11-02', credits: developer.monthlyCredits + 40_000 }] })]
    const now = new Date('2026-11-04T00:00:00Z')

    expect(computePendingOverage(buildOverageRows(accounts, now))).toEqual([])
    expect(computePendingOverage(buildOverageRows(accounts, now, developer), developer)).toEqual([
      { userId: 'user-1', cycle: '2026-10-14', cycleEnd: '2026-11-14', credits: 40_000, total: 40_000 },
    ])
  })

  test('a resubscription on a different day bills the new subscription from the change', () => {
    // Subscription A, anchored on the 1st, reported 2.2M of overage before it
    // was revoked. B anchors on 28 October, which is when metering restarts.
    // A's report is under its own key and is never subtracted from B, and A's
    // days are never billed again under B.
    const accounts = [
      account({
        anchor: '2026-10-28T09:00:00Z',
        meteredFrom: '2026-10-28',
        days: [
          { day: '2026-10-10', credits: developer.monthlyCredits + 2_200_000 },
          { day: '2026-11-05', credits: developer.monthlyCredits + 2_200_000 },
        ],
        reports: { '2026-10-01': 2_200_000 },
      }),
    ]

    expect(computePendingOverage(buildOverageRows(accounts, new Date('2026-11-10T00:00:00Z')))).toEqual([
      { userId: 'user-1', cycle: '2026-10-28', cycleEnd: '2026-11-28', credits: 2_200_000, total: 2_200_000 },
    ])
  })

  test('the first anchor for an existing calendar-month subscriber starts a fresh segment', () => {
    // Before deploy the account was metered by calendar month, with 500k
    // reported under 1 November. Its first webhook after deploy anchors it on
    // the 14th; the days before were staged under the calendar key then.
    const accounts = [
      account({
        anchor: '2026-11-14T00:00:00Z',
        meteredFrom: '2026-11-14',
        days: [
          { day: '2026-11-05', credits: developer.monthlyCredits + 500_000 },
          { day: '2026-11-20', credits: developer.monthlyCredits + 100_000 },
        ],
        reports: { '2026-11-01': 500_000 },
      }),
    ]

    expect(computePendingOverage(buildOverageRows(accounts, new Date('2026-11-21T00:00:00Z')))).toEqual([
      { userId: 'user-1', cycle: '2026-11-14', cycleEnd: '2026-12-14', credits: 100_000, total: 100_000 },
    ])
  })

  test('a downgrade mid-period judges only the days after it against the smaller plan', () => {
    // 9M used on business by the 10th, inside its 10M. Developer's 1M applies
    // only from the change, so the 9M is never billed as 8M of overage.
    const business = getPlan('business')
    const days = [
      { day: '2026-11-05', credits: 9_000_000 },
      { day: '2026-11-12', credits: developer.monthlyCredits + 200_000 },
    ]
    const before = [account({ plan: 'business', anchor: '2026-11-01T00:00:00Z', days })]
    const after = [account({ anchor: '2026-11-01T00:00:00Z', meteredFrom: '2026-11-10', days })]

    const staged = computePendingOverage(buildOverageRows(before, new Date('2026-11-10T08:00:00Z'), business, '2026-11-10'), business)
    expect(staged).toEqual([])
    expect(computePendingOverage(buildOverageRows(after, new Date('2026-11-15T00:00:00Z')))).toEqual([
      { userId: 'user-1', cycle: '2026-11-10', cycleEnd: '2026-12-01', credits: 200_000, total: 200_000 },
    ])
  })

  test('an upgrade mid-period bills overage on the old plan at the change, and none after', () => {
    const business = getPlan('business')
    const days = [
      { day: '2026-11-05', credits: developer.monthlyCredits + 500_000 },
      { day: '2026-11-12', credits: 2_000_000 },
    ]
    const before = [account({ anchor: '2026-11-01T00:00:00Z', days })]
    const after = [account({ plan: 'business', anchor: '2026-11-01T00:00:00Z', meteredFrom: '2026-11-10', days })]

    expect(computePendingOverage(buildOverageRows(before, new Date('2026-11-10T08:00:00Z'), developer, '2026-11-10'), developer)).toEqual([
      { userId: 'user-1', cycle: '2026-11-01', cycleEnd: '2026-11-10', credits: 500_000, total: 500_000 },
    ])
    expect(computePendingOverage(buildOverageRows(after, new Date('2026-11-15T00:00:00Z')))).toEqual([])
  })

  test('a pack refunded after it was spent is not billed as overage', () => {
    const rows = [row({ used: developer.monthlyCredits + 10_000, purchased: -50_000 })]

    expect(computePendingOverage(rows)).toEqual([
      { userId: 'user-1', cycle: '2026-11-01', cycleEnd: '2026-12-01', credits: 10_000, total: 10_000 },
    ])
  })
})
