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
import { computePendingOverage, type OverageRow } from './overage.service'
import { getPlan } from '../billing/plans'

const developer = getPlan('developer') // 1,000,000 credits, overage allowed
const free = getPlan('free') // 100,000 credits, no overage

function row(overrides: Partial<OverageRow> = {}): OverageRow {
  return {
    user_id: 'user-1',
    plan: 'developer',
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

    expect(computePendingOverage(rows)).toEqual([{ userId: 'user-1', credits: 50_000, total: 50_000 }])
  })

  test('a later pass reports only what is new', () => {
    const rows = [row({ used: developer.monthlyCredits + 80_000, reported: 50_000 })]

    expect(computePendingOverage(rows)).toEqual([{ userId: 'user-1', credits: 30_000, total: 80_000 }])
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

    expect(computePendingOverage(rows)).toEqual([{ userId: 'user-1', credits: 20_000, total: 20_000 }])
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
      { userId: 'a', credits: 10_000, total: 10_000 },
      { userId: 'd', credits: 50_000, total: 90_000 },
    ])
  })

  test('an empty result set produces no work', () => {
    expect(computePendingOverage([])).toEqual([])
  })
})
