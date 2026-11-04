/**
 * Tests for the credit arithmetic — the code that decides whether a customer is
 * served or refused, and what they are billed.
 *
 * `computeBalance` and `decideQuota` are pure, so this needs no database. The
 * cases that matter are the boundaries (exactly at the allowance, one credit
 * short) and the interaction between the monthly allowance and purchased
 * credits, which is where double-charging would hide.
 */
import { describe, test, expect } from 'bun:test'
import { computeBalance, computePackUsage, decideQuota, packUsageId } from './credits.service'
import { calendarPeriod, periodsBetween } from '../billing/periods'
import { getPlan } from '../billing/plans'

const free = getPlan('free') // 100,000 credits, no overage
const developer = getPlan('developer') // 1,000,000 credits, overage allowed

describe('computeBalance — allowance only', () => {
  test('a fresh account has its whole allowance', () => {
    const balance = computeBalance(free, 0, 0)

    expect(balance.used).toBe(0)
    expect(balance.allowanceRemaining).toBe(free.monthlyCredits)
    expect(balance.remaining).toBe(free.monthlyCredits)
    expect(balance.overage).toBe(0)
  })

  test('spending reduces what is left, one for one', () => {
    const balance = computeBalance(free, 30_000, 0)

    expect(balance.allowanceRemaining).toBe(70_000)
    expect(balance.remaining).toBe(70_000)
  })

  test('exactly at the allowance leaves nothing and owes nothing', () => {
    const balance = computeBalance(free, free.monthlyCredits, 0)

    expect(balance.remaining).toBe(0)
    expect(balance.overage).toBe(0)
  })

  test('never reports a negative balance', () => {
    // A free account can overshoot slightly: the quota check reads a cache, and
    // concurrent requests can both be approved against the last few credits.
    const balance = computeBalance(free, free.monthlyCredits + 5_000, 0)

    expect(balance.allowanceRemaining).toBe(0)
    expect(balance.remaining).toBe(0)
  })

  test('a plan without overage never accrues any, however far it overshoots', () => {
    const balance = computeBalance(free, free.monthlyCredits * 10, 0)

    // Otherwise a free account could be invoiced for credits it was told were
    // free, which is the whole promise of the tier.
    expect(balance.overage).toBe(0)
    expect(balance.overageAllowed).toBe(false)
  })
})

describe('computeBalance — purchased credits', () => {
  test('are not touched while allowance remains', () => {
    const balance = computeBalance(free, 40_000, 500_000)

    // The pack must survive an idle month rather than being consumed by usage
    // the plan already covered.
    expect(balance.purchased).toBe(500_000)
    expect(balance.allowanceRemaining).toBe(60_000)
    expect(balance.remaining).toBe(60_000 + 500_000)
  })

  test('absorb spend once the allowance is gone', () => {
    const balance = computeBalance(free, free.monthlyCredits + 200_000, 500_000)

    expect(balance.allowanceRemaining).toBe(0)
    expect(balance.remaining).toBe(300_000)
  })

  test('are exhausted exactly, never over-drawn', () => {
    const balance = computeBalance(free, free.monthlyCredits + 500_000, 500_000)

    expect(balance.remaining).toBe(0)
  })

  test('spending past both leaves zero rather than a negative', () => {
    const balance = computeBalance(free, free.monthlyCredits + 900_000, 500_000)

    expect(balance.remaining).toBe(0)
  })

  test('are spent before overage begins on a paid plan', () => {
    const used = developer.monthlyCredits + 300_000
    const balance = computeBalance(developer, used, 500_000)

    // 300k past the allowance, all covered by the 500k pack — nothing is
    // metered. Billing this as overage would charge twice for credits the
    // customer already bought.
    expect(balance.overage).toBe(0)
    expect(balance.remaining).toBe(200_000)
  })

  test('overage starts only once purchased credits run out', () => {
    const used = developer.monthlyCredits + 700_000
    const balance = computeBalance(developer, used, 500_000)

    expect(balance.remaining).toBe(0)
    expect(balance.overage).toBe(200_000)
  })
})

describe('computeBalance — cycle reset', () => {
  test('reports the next cycle boundary as a UTC instant', () => {
    const balance = computeBalance(free, 0, 0)

    // Consumed by the console and by the 402 body, so it has to be an
    // unambiguous instant rather than a local-time date.
    expect(balance.cycleResetsAt).toMatch(/^\d{4}-\d{2}-01T00:00:00\.000Z$/)
    expect(new Date(balance.cycleResetsAt).getTime()).toBeGreaterThan(Date.now())
  })
})

describe('decideQuota', () => {
  test('allows a request that fits', () => {
    const decision = decideQuota(computeBalance(free, 0, 0), 6)

    expect(decision.allowed).toBe(true)
    if (decision.allowed) expect(decision.overage).toBe(false)
  })

  test('allows a request that exactly exhausts the balance', () => {
    const decision = decideQuota(computeBalance(free, free.monthlyCredits - 6, 0), 6)

    expect(decision.allowed).toBe(true)
  })

  test('refuses a free account one credit short', () => {
    const decision = decideQuota(computeBalance(free, free.monthlyCredits - 5, 0), 6)

    // The defining promise of the free tier: it stops rather than billing.
    expect(decision.allowed).toBe(false)
    if (!decision.allowed) {
      expect(decision.reason).toBe('out-of-credits')
      expect(decision.balance.remaining).toBe(5)
    }
  })

  test('refuses a free account that is fully spent', () => {
    const decision = decideQuota(computeBalance(free, free.monthlyCredits, 0), 1)

    expect(decision.allowed).toBe(false)
  })

  test('serves a paid account past its allowance, flagged as overage', () => {
    const decision = decideQuota(computeBalance(developer, developer.monthlyCredits, 0), 12)

    // Stopping a production integration dead at the boundary would be worse
    // than the bill — but the caller must know it is now metered.
    expect(decision.allowed).toBe(true)
    if (decision.allowed) expect(decision.overage).toBe(true)
  })

  test('a paid account still inside its allowance is not flagged', () => {
    const decision = decideQuota(computeBalance(developer, 10_000, 0), 12)

    expect(decision.allowed).toBe(true)
    if (decision.allowed) expect(decision.overage).toBe(false)
  })

  test('purchased credits keep a paid account out of overage', () => {
    const balance = computeBalance(developer, developer.monthlyCredits + 100, 50_000)
    const decision = decideQuota(balance, 12)

    expect(decision.allowed).toBe(true)
    if (decision.allowed) expect(decision.overage).toBe(false)
  })

  test('the cost of the request is what is checked, not merely non-zero balance', () => {
    // 5 credits left, a 40-credit isochrone: refused. Checking only for a
    // non-empty balance would serve it and drive the account negative.
    const decision = decideQuota(computeBalance(free, free.monthlyCredits - 5, 0), 40)

    expect(decision.allowed).toBe(false)
  })
})

describe('the overage spend cap', () => {
  const capped = getPlan('developer') // cap = 3x the allowance

  test('a paid account is served while under the cap', () => {
    const cap = capped.overageCapMultiple * capped.monthlyCredits
    const balance = computeBalance(capped, capped.monthlyCredits + cap / 2, 0)

    expect(decideQuota(balance, 12).allowed).toBe(true)
  })

  test('a request that would cross the cap is refused', () => {
    const cap = capped.overageCapMultiple * capped.monthlyCredits
    const balance = computeBalance(capped, capped.monthlyCredits + cap, 0)
    const decision = decideQuota(balance, 12)

    // Without this a leaked key accrues charges with no ceiling, and the only
    // backstop runs on the hourly sweep.
    expect(decision.allowed).toBe(false)
    if (!decision.allowed) expect(decision.reason).toBe('overage-cap-reached')
  })

  test('the refusal is distinguishable from a plain allowance exhaustion', () => {
    // The console and the 402 body say different things: one is "add credits",
    // the other is "raise your limit".
    const exhausted = decideQuota(computeBalance(free, free.monthlyCredits, 0), 1)
    const cap = capped.overageCapMultiple * capped.monthlyCredits
    const capReached = decideQuota(computeBalance(capped, capped.monthlyCredits + cap, 0), 12)

    expect(exhausted.allowed).toBe(false)
    expect(capReached.allowed).toBe(false)
    if (!exhausted.allowed && !capReached.allowed) {
      expect(exhausted.reason).toBe('out-of-credits')
      expect(capReached.reason).toBe('overage-cap-reached')
    }
  })

  test('a cap of zero means uncapped, not "no overage allowed"', () => {
    // Enterprise volume is contractual, so its cap is 0 and it keeps serving
    // however far past the allowance it goes.
    const enterprise = getPlan('enterprise')
    expect(enterprise.overageCapMultiple).toBe(0)

    const balance = computeBalance(enterprise, enterprise.monthlyCredits * 100, 0)
    expect(decideQuota(balance, 40).allowed).toBe(true)
  })

  test('the cap never applies to a plan without overage', () => {
    // Free is refused by the allowance check long before the cap is consulted.
    const decision = decideQuota(computeBalance(free, free.monthlyCredits * 10, 0), 1)

    expect(decision.allowed).toBe(false)
    if (!decision.allowed) expect(decision.reason).toBe('out-of-credits')
  })
})

describe('computeBalance — billing period', () => {
  test('reports the start and end of the period it was given', () => {
    const balance = computeBalance(developer, 0, 0, { start: '2026-10-14', end: '2026-11-14' })

    expect(balance.cycleStartedAt).toBe('2026-10-14T00:00:00.000Z')
    expect(balance.cycleResetsAt).toBe('2026-11-14T00:00:00.000Z')
  })
})

describe('computePackUsage — packs are used up across periods', () => {
  const user = 'pack-user'
  const at = (iso: string) => new Date(iso)
  const pack = (amount: number, iso: string, externalId: string | null = null) => ({
    amount,
    createdAt: at(iso),
    externalId,
  })
  const october = calendarPeriod('2026-10-15')
  const november = calendarPeriod('2026-11-15')

  test('a period that spends past its allowance takes the excess from the pack', () => {
    const usage = computePackUsage({
      userId: user,
      plan: free,
      periods: [october],
      from: october.start,
      days: [{ day: '2026-10-20', credits: free.monthlyCredits + 30_000 }],
      ledger: [pack(50_000, '2026-10-05T00:00:00Z')],
    })

    expect(usage).toEqual([{ period: october, credits: 30_000 }])
  })

  test('the pack does not refill in the next period', () => {
    // The bug this replaces: the ledger total stayed at 50k while usage reset,
    // so every month started with the whole pack again.
    const ledger = [pack(50_000, '2026-10-05T00:00:00Z'), pack(-30_000, '2026-11-01T00:00:00Z', packUsageId(user, '2026-10-01'))]
    const remaining = ledger.reduce((sum, entry) => sum + entry.amount, 0)
    const balance = computeBalance(free, free.monthlyCredits + 25_000, remaining, november)

    expect(remaining).toBe(20_000)
    // 25k past the allowance against the 20k left: the pack is gone.
    expect(balance.remaining).toBe(0)
  })

  test('two periods in a row use the pack up and then stop at zero', () => {
    const periods = periodsBetween(free, null, '2026-10-01', '2026-12-01')
    const usage = computePackUsage({
      userId: user,
      plan: free,
      periods,
      from: october.start,
      days: [
        { day: '2026-10-20', credits: free.monthlyCredits + 30_000 },
        { day: '2026-11-20', credits: free.monthlyCredits + 40_000 },
      ],
      ledger: [pack(50_000, '2026-10-05T00:00:00Z')],
    })

    expect(usage).toEqual([
      { period: october, credits: 30_000 },
      { period: november, credits: 20_000 },
    ])
  })

  test('a settled period is not settled again', () => {
    const usage = computePackUsage({
      userId: user,
      plan: free,
      periods: [october],
      from: october.start,
      days: [{ day: '2026-10-20', credits: free.monthlyCredits + 30_000 }],
      ledger: [pack(50_000, '2026-10-05T00:00:00Z'), pack(-30_000, '2026-11-01T00:00:00Z', packUsageId(user, '2026-10-01'))],
    })

    expect(usage).toEqual([])
  })

  test('a period inside its allowance leaves the pack alone', () => {
    const usage = computePackUsage({
      userId: user,
      plan: free,
      periods: [october],
      from: october.start,
      days: [{ day: '2026-10-20', credits: free.monthlyCredits }],
      ledger: [pack(50_000, '2026-10-05T00:00:00Z')],
    })

    expect(usage).toEqual([])
  })

  test('a pack bought after a period closed does not pay for it', () => {
    const usage = computePackUsage({
      userId: user,
      plan: developer,
      periods: [october],
      from: october.start,
      days: [{ day: '2026-10-20', credits: developer.monthlyCredits + 30_000 }],
      ledger: [pack(50_000, '2026-11-02T10:00:00Z')],
    })

    expect(usage).toEqual([])
  })

  test('overage already billed for a period is not also taken from the pack', () => {
    // 30k past the allowance, 20k of it reported as overage before the pack
    // was bought. Only the 10k nobody has paid for comes out of the pack.
    const usage = computePackUsage({
      userId: user,
      plan: developer,
      periods: [october],
      from: october.start,
      days: [{ day: '2026-10-20', credits: developer.monthlyCredits + 30_000 }],
      ledger: [pack(50_000, '2026-10-25T00:00:00Z')],
      reported: [{ start: '2026-10-01', end: '2026-11-01', amount: 20_000 }],
    })

    expect(usage).toEqual([{ period: october, credits: 10_000 }])
  })

  test('days settled before are not judged again, even under another plan', () => {
    // October was settled on the developer plan with nothing owed, so
    // pack_settled_through moved to 1 November. A downgrade to free must not
    // reach back and charge October's usage to the pack.
    const usage = computePackUsage({
      userId: user,
      plan: free,
      periods: periodsBetween(free, null, '2026-11-01', '2026-12-01'),
      from: '2026-11-01',
      days: [
        { day: '2026-10-20', credits: 900_000 },
        { day: '2026-11-20', credits: free.monthlyCredits + 5_000 },
      ],
      ledger: [pack(50_000, '2026-10-05T00:00:00Z')],
    })

    expect(usage).toEqual([{ period: november, credits: 5_000 }])
  })

  test('a move from calendar months to a subscription anchor does not count days twice', () => {
    // Settled through 1 November on calendar months, then subscribed with an
    // anchor on the 14th. The first anchored period starts 14 October, but
    // only its days from 1 November on are settled.
    const periods = periodsBetween(developer, '2026-10-14', '2026-11-01', '2026-11-14')
    const usage = computePackUsage({
      userId: user,
      plan: developer,
      periods,
      from: '2026-11-01',
      days: [
        { day: '2026-10-20', credits: 2_000_000 },
        { day: '2026-11-05', credits: developer.monthlyCredits + 7_000 },
      ],
      ledger: [pack(50_000, '2026-10-05T00:00:00Z')],
    })

    expect(usage).toEqual([{ period: { start: '2026-11-01', end: '2026-11-14' }, credits: 7_000 }])
  })
})
