/**
 * The subscription lifecycle, driven through sequences of Polar events.
 *
 * Each test replays deliveries in the order Polar might send them and checks
 * the plan the account ends on, since that plan is what the customer is billed
 * and served for.
 */
import { describe, test, expect } from 'bun:test'
import {
  reduceSubscription,
  REVOKED,
  type IncomingSubscription,
  type StoredSubscription,
} from './subscription-state'

const PRODUCTS: Record<string, { id: string; rank: number }> = {
  prod_dev: { id: 'developer', rank: 1 },
  prod_business: { id: 'business', rank: 2 },
}
const RANKS: Record<string, number> = { free: 0, developer: 1, business: 2, demo: -1 }

const NOW = new Date('2026-11-15T00:00:00Z')

const options = (revoked = false) => ({
  revoked,
  now: NOW,
  planForProduct: (productId: string) => PRODUCTS[productId] ?? null,
  rankOf: (planId: string) => RANKS[planId] ?? 0,
})

const fresh: StoredSubscription = {
  plan: 'free',
  subscriptionId: null,
  subscriptionStatus: null,
  subscriptionCancelAtPeriodEnd: false,
  subscriptionPeriodEnd: null,
  subscriptionModifiedAt: null,
}

function sub(overrides: Partial<IncomingSubscription> = {}): IncomingSubscription {
  return {
    id: 'sub_1',
    status: 'active',
    productId: 'prod_dev',
    cancelAtPeriodEnd: false,
    currentPeriodStart: new Date('2026-11-01T00:00:00Z'),
    currentPeriodEnd: new Date('2026-12-01T00:00:00Z'),
    endsAt: null,
    endedAt: null,
    modifiedAt: new Date('2026-11-01T00:00:00Z'),
    createdAt: new Date('2026-10-01T00:00:00Z'),
    ...overrides,
  }
}

/** Replay events the way applySubscriptionEvent does, returning the final account. */
function replay(start: StoredSubscription, events: Array<[IncomingSubscription, boolean?]>) {
  let stored = start
  const ignored: string[] = []
  const pastDueNotices: number[] = []
  events.forEach(([incoming, revoked], index) => {
    const outcome = reduceSubscription(stored, incoming, options(revoked))
    if (outcome.ignored !== undefined) {
      ignored.push(outcome.ignored)
      return
    }
    let plan = stored.plan
    if (outcome.plan.kind === 'set') plan = outcome.plan.planId
    if (outcome.plan.kind === 'free') plan = 'free'
    if (outcome.enteredPastDue) pastDueNotices.push(index)
    stored = { ...outcome.state, plan }
  })
  return { stored, ignored, pastDueNotices }
}

describe('reduceSubscription', () => {
  test('an active subscription applies its plan', () => {
    const { stored } = replay(fresh, [[sub()]])
    expect(stored.plan).toBe('developer')
    expect(stored.subscriptionId).toBe('sub_1')
  })

  test('a scheduled cancellation keeps the plan and records the end date', () => {
    // Polar sends subscription.canceled with the subscription still active and
    // cancel_at_period_end set. The customer has paid through the period.
    const { stored } = replay(fresh, [
      [sub()],
      [sub({ cancelAtPeriodEnd: true, modifiedAt: new Date('2026-11-10T00:00:00Z') })],
    ])
    expect(stored.plan).toBe('developer')
    expect(stored.subscriptionCancelAtPeriodEnd).toBe(true)
    expect(stored.subscriptionPeriodEnd?.toISOString()).toBe('2026-12-01T00:00:00.000Z')
  })

  test('a canceled status with time left on the period keeps the plan', () => {
    const { stored } = replay(fresh, [
      [sub()],
      [sub({ status: 'canceled', endsAt: new Date('2026-12-01T00:00:00Z'), modifiedAt: new Date('2026-11-10T00:00:00Z') })],
    ])
    expect(stored.plan).toBe('developer')
  })

  test('revocation ends the plan', () => {
    const { stored } = replay(fresh, [
      [sub()],
      [sub({ status: 'canceled', endedAt: NOW, modifiedAt: new Date('2026-11-14T00:00:00Z') }), true],
    ])
    expect(stored.plan).toBe('free')
    expect(stored.subscriptionStatus).toBe(REVOKED)
  })

  test('a stale updated arriving after revoked leaves the account free', () => {
    const { stored, ignored } = replay(fresh, [
      [sub()],
      [sub({ status: 'canceled', modifiedAt: new Date('2026-11-14T00:00:00Z') }), true],
      // Delayed delivery of the state from before the revocation.
      [sub({ status: 'active', modifiedAt: new Date('2026-11-05T00:00:00Z') })],
    ])
    expect(stored.plan).toBe('free')
    expect(ignored).toHaveLength(1)
  })

  test('a revoked subscription stays revoked even when the late event carries no older timestamp', () => {
    // modifiedAt alone cannot order events Polar stamped identically, so the
    // revoked marker is final for that subscription.
    const at = new Date('2026-11-14T00:00:00Z')
    const { stored } = replay(fresh, [
      [sub({ modifiedAt: at })],
      [sub({ status: 'canceled', modifiedAt: at }), true],
      [sub({ status: 'active', modifiedAt: at })],
    ])
    expect(stored.plan).toBe('free')
  })

  test('an older updated arriving after a newer one is ignored', () => {
    const { stored, ignored } = replay(fresh, [
      [sub({ productId: 'prod_business', modifiedAt: new Date('2026-11-10T00:00:00Z') })],
      [sub({ productId: 'prod_dev', modifiedAt: new Date('2026-11-02T00:00:00Z') })],
    ])
    expect(stored.plan).toBe('business')
    expect(ignored).toEqual(['older than the stored state'])
  })

  test('past_due keeps the paid plan and notifies once', () => {
    const { stored, pastDueNotices } = replay(fresh, [
      [sub()],
      [sub({ status: 'past_due', modifiedAt: new Date('2026-11-12T00:00:00Z') })],
      // Polar sends subscription.updated and subscription.past_due for the same change.
      [sub({ status: 'past_due', modifiedAt: new Date('2026-11-12T00:00:00Z') })],
    ])
    expect(stored.plan).toBe('developer')
    expect(stored.subscriptionStatus).toBe('past_due')
    expect(pastDueNotices).toEqual([1])
  })

  test('a failed payment that is never recovered ends in revocation', () => {
    const { stored } = replay(fresh, [
      [sub()],
      [sub({ status: 'past_due', modifiedAt: new Date('2026-11-12T00:00:00Z') })],
      [sub({ status: 'canceled', modifiedAt: new Date('2026-11-14T00:00:00Z') }), true],
    ])
    expect(stored.plan).toBe('free')
  })

  test.each([['unpaid'], ['incomplete_expired']])('%s ends the plan', (status) => {
    const { stored } = replay(fresh, [
      [sub()],
      [sub({ status, modifiedAt: new Date('2026-11-12T00:00:00Z') })],
    ])
    expect(stored.plan).toBe('free')
  })

  test('an incomplete subscription does not grant the plan yet', () => {
    const { stored } = replay(fresh, [[sub({ status: 'incomplete' })]])
    expect(stored.plan).toBe('free')
  })

  test('revoking a different, older subscription leaves the live one alone', () => {
    const { stored, ignored } = replay(fresh, [
      [sub({ id: 'sub_new', productId: 'prod_dev' })],
      [sub({ id: 'sub_old', productId: 'prod_dev', status: 'canceled' }), true],
    ])
    expect(stored.plan).toBe('developer')
    expect(stored.subscriptionId).toBe('sub_new')
    expect(ignored).toHaveLength(1)
  })

  test('only removes the plan the revoked subscription was paying for', () => {
    // An operator moved the account to business; a developer subscription
    // ending must not take that away.
    const start = { ...fresh, plan: 'business' }
    const { stored } = replay(start, [[sub({ status: 'canceled' }), true]])
    expect(stored.plan).toBe('business')
  })

  test('of two live subscriptions, the richer one keeps the plan', () => {
    const { stored } = replay(fresh, [
      [sub({ id: 'sub_business', productId: 'prod_business' })],
      [sub({ id: 'sub_dev', productId: 'prod_dev', modifiedAt: new Date('2026-11-10T00:00:00Z') })],
    ])
    expect(stored.plan).toBe('business')
    expect(stored.subscriptionId).toBe('sub_business')
  })

  test('a plan change on the same subscription applies the new plan', () => {
    const { stored } = replay(fresh, [
      [sub()],
      [sub({ productId: 'prod_business', modifiedAt: new Date('2026-11-10T00:00:00Z') })],
    ])
    expect(stored.plan).toBe('business')
  })

  test('an uncancel restores the renewal without changing the plan', () => {
    const { stored } = replay(fresh, [
      [sub()],
      [sub({ cancelAtPeriodEnd: true, modifiedAt: new Date('2026-11-10T00:00:00Z') })],
      [sub({ cancelAtPeriodEnd: false, modifiedAt: new Date('2026-11-11T00:00:00Z') })],
    ])
    expect(stored.plan).toBe('developer')
    expect(stored.subscriptionCancelAtPeriodEnd).toBe(false)
  })
})
