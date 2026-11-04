/**
 * What a Polar subscription event does to an account, as a pure function.
 *
 * Polar's webhooks are at-least-once and unordered. The same delivery can
 * arrive twice, `subscription.updated` can land after the `subscription.revoked`
 * that followed it, and Polar sends `subscription.canceled` the moment a
 * customer schedules a cancellation, weeks before the subscription ends. Each
 * of those, applied naively, moves the account onto the wrong plan. This module
 * decides from the stored state and the incoming subscription alone, so every
 * one of those orderings can be tested without a database or a network.
 */

/** Stored on the account in place of Polar's status once Polar revokes the subscription. */
export const REVOKED = 'revoked'

/**
 * Statuses under which the customer is still owed the plan. `past_due` is in
 * here: Polar retries a failed payment for a while, and the subscription is
 * revoked (which we do act on) if the retries run out.
 */
const LIVE_STATUSES = new Set(['active', 'trialing', 'past_due'])

export function isLiveStatus(status: string | null | undefined): boolean {
  return status != null && LIVE_STATUSES.has(status)
}

/** The fields of a Polar subscription this module reads. */
export interface IncomingSubscription {
  id: string
  status: string
  productId: string | null
  cancelAtPeriodEnd: boolean
  currentPeriodStart: Date | null
  currentPeriodEnd: Date | null
  endsAt: Date | null
  endedAt: Date | null
  modifiedAt: Date | null
  createdAt: Date | null
}

/** The subscription columns on the account, plus its current plan. */
export interface StoredSubscription {
  plan: string
  subscriptionId: string | null
  subscriptionStatus: string | null
  subscriptionCancelAtPeriodEnd: boolean
  subscriptionPeriodEnd: Date | null
  subscriptionModifiedAt: Date | null
}

export type PlanChange = { kind: 'none' } | { kind: 'set'; planId: string } | { kind: 'free' }

export type SubscriptionOutcome =
  | { ignored: string }
  | {
      ignored?: undefined
      /** The subscription columns to write. */
      state: Omit<StoredSubscription, 'plan'>
      plan: PlanChange
      /** True only on the transition into `past_due`, so the notice is sent once. */
      enteredPastDue: boolean
    }

export interface ReduceOptions {
  /** The event was `subscription.revoked`: access has ended, whatever the status says. */
  revoked: boolean
  now: Date
  /** Maps a Polar product to a plan id and its rank, or null when unmapped. */
  planForProduct: (productId: string) => { id: string; rank: number } | null
  /** Rank of a plan id, for choosing between two live subscriptions. */
  rankOf: (planId: string) => number
}

/** What the subscription's status alone says should happen to the plan. */
export function planEffect(sub: IncomingSubscription, revoked: boolean, now: Date): 'apply' | 'keep' | 'free' {
  if (revoked) return 'free'
  switch (sub.status) {
    case 'active':
    case 'trialing':
      return 'apply'
    case 'past_due':
      // Polar is retrying the payment. Revocation is what ends the plan.
      return 'keep'
    case 'canceled': {
      // Already over: revoked outright, or the period has run out.
      if (sub.endedAt && sub.endedAt.getTime() <= now.getTime()) return 'free'
      // Paid through the end of the period, so the plan runs until then.
      const end = sub.endsAt ?? sub.currentPeriodEnd
      return end && end.getTime() > now.getTime() ? 'keep' : 'free'
    }
    case 'incomplete_expired':
    case 'unpaid':
    case 'paused':
      return 'free'
    default:
      // `incomplete` (first payment not through yet) and anything Polar adds
      // later. Neither is evidence the customer stopped paying.
      return 'keep'
  }
}

function timestampOf(sub: IncomingSubscription): Date | null {
  return sub.modifiedAt ?? sub.createdAt
}

export function reduceSubscription(
  stored: StoredSubscription,
  incoming: IncomingSubscription,
  options: ReduceOptions,
): SubscriptionOutcome {
  const { revoked, now, planForProduct, rankOf } = options
  const sameSubscription = stored.subscriptionId === incoming.id

  if (sameSubscription) {
    // A revoked subscription never comes back, so anything else about it
    // that arrives afterwards is a delayed delivery of an older state.
    if (stored.subscriptionStatus === REVOKED && !revoked) {
      return { ignored: 'subscription already revoked' }
    }
    const incomingAt = timestampOf(incoming)
    if (stored.subscriptionModifiedAt && incomingAt && incomingAt < stored.subscriptionModifiedAt) {
      return { ignored: 'older than the stored state' }
    }
  } else if (stored.subscriptionId && isLiveStatus(stored.subscriptionStatus)) {
    // The account already has a live subscription and this event is about a
    // different one: an old subscription ending, or a duplicate opened before
    // plan changes went through `subscriptions.update`. Ending one must not
    // take away the plan the other pays for.
    if (revoked || planEffect(incoming, false, now) !== 'apply') {
      return { ignored: 'not the subscription behind the current plan' }
    }
    // Two live subscriptions: the richer plan wins, as in syncSubscription.
    const incomingPlan = incoming.productId ? planForProduct(incoming.productId) : null
    if (!incomingPlan || incomingPlan.rank < rankOf(stored.plan)) {
      return { ignored: 'a richer subscription is already live' }
    }
  }

  const effect = planEffect(incoming, revoked, now)
  let plan: PlanChange = { kind: 'none' }

  if (effect === 'apply' && incoming.productId) {
    const mapped = planForProduct(incoming.productId)
    // An unmapped product leaves the plan alone; applyPlanFromProduct logs it.
    plan = mapped ? { kind: 'set', planId: mapped.id } : { kind: 'none' }
  } else if (effect === 'free') {
    // Only end the plan this subscription is paying for. An account moved
    // onto another plan by an operator, or by another subscription, keeps it.
    const mapped = incoming.productId ? planForProduct(incoming.productId) : null
    if (mapped && mapped.id === stored.plan) plan = { kind: 'free' }
  }

  const status = revoked ? REVOKED : incoming.status

  return {
    state: {
      subscriptionId: incoming.id,
      subscriptionStatus: status,
      subscriptionCancelAtPeriodEnd: incoming.cancelAtPeriodEnd,
      subscriptionPeriodEnd: incoming.endsAt ?? incoming.currentPeriodEnd,
      subscriptionModifiedAt: timestampOf(incoming) ?? now,
    },
    plan,
    enteredPastDue: status === 'past_due' && !(sameSubscription && stored.subscriptionStatus === 'past_due'),
  }
}
