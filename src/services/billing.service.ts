/**
 * Polar integration: checkout, the customer portal, subscription state, and
 * metered overage reporting.
 *
 * The local database is the authority on what an account may spend right now —
 * quota decisions never make a network call. Polar is the authority on what has
 * been paid for, and its webhooks push plan changes down to us. When the two
 * disagree, `syncSubscription()` re-reads Polar and wins, which is what the
 * console's "refresh" does.
 */
import { Polar } from '@polar-sh/sdk'
import { and, eq, isNull, lt, sql as dsql, type Column } from 'drizzle-orm'
import { db } from '../db'
import { billing } from '../config/billing.config'
import { getPlan, listPlans, planForProductId, planProductId, type Plan } from '../billing/plans'
import {
  isLiveStatus,
  reduceSubscription,
  REVOKED,
  type IncomingSubscription,
  type StoredSubscription,
} from '../billing/subscription-state'
import { billingWebhookEvents, users, type User } from '../schema/accounts'
import { findUserByPolarCustomerId, linkPolarCustomer, findUserById } from './accounts.service'
import { grantCredits, invalidateBalance, ledgerTotalForExternalPrefix, setPlan } from './credits.service'
import { invalidateUserKeys } from './api-keys.service'
import { sendPaymentFailedNotice } from './mailer.service'
import { DEFAULT_PLAN } from '../billing/plans'

let client: Polar | null = null

function polar(): Polar {
  if (!client) {
    client = new Polar({
      accessToken: billing.accessToken,
      server: billing.sandbox ? 'sandbox' : 'production',
    })
  }
  return client
}

/** Test seam — lets the billing tests drive a fake without a network. */
export function __setPolarClient(fake: Polar | null): void {
  client = fake
}

// ── Products ────────────────────────────────────────────────────────────

export interface ProductInfo {
  planId: string
  productId: string
  name: string
  priceAmount: number
  priceCurrency: string
  interval: string
}

const productCache = new Map<string, { info: ProductInfo; fetchedAt: number }>()
const PRODUCT_CACHE_TTL_MS = 60 * 60 * 1000

export function clearProductCache(): void {
  productCache.clear()
}

async function fetchProduct(plan: Plan): Promise<ProductInfo | null> {
  const productId = planProductId(plan)
  if (!productId) return null

  const cached = productCache.get(productId)
  if (cached && Date.now() - cached.fetchedAt < PRODUCT_CACHE_TTL_MS) return cached.info

  try {
    const product = await polar().products.get({ id: productId })
    // Take the most recently created active fixed price, so a price change in
    // Polar is picked up without a redeploy.
    const prices = (product.prices ?? []).filter(
      (p: { amountType?: string; isArchived?: boolean }) => p.amountType === 'fixed' && !p.isArchived,
    ) as Array<{ priceAmount: number; priceCurrency: string }>
    const price = prices[prices.length - 1]

    const info: ProductInfo = {
      planId: plan.id,
      productId,
      name: product.name,
      priceAmount: price?.priceAmount ?? 0,
      priceCurrency: price?.priceCurrency ?? 'usd',
      interval: product.recurringInterval ?? 'month',
    }

    productCache.set(productId, { info, fetchedAt: Date.now() })
    return info
  } catch (err) {
    // A pricing page that renders without live prices beats one that 500s.
    console.warn(`[billing] could not fetch product ${productId}:`, err)
    return null
  }
}

export async function getProducts(): Promise<ProductInfo[]> {
  if (!billing.enabled) return []
  const results = await Promise.all(listPlans().map(fetchProduct))
  return results.filter((info): info is ProductInfo => info !== null)
}

// ── Checkout and portal ─────────────────────────────────────────────────

export async function createCheckout(options: {
  userId: string
  email: string
  productId: string
  successUrl: string
  /** The account's linked Polar customer, if any. */
  polarCustomerId?: string | null
}): Promise<string> {
  // A linked customer from before external ids were set has none, and a
  // checkout naming an external id it does not carry would open a second
  // customer for the same person. Backfill it first so Polar matches the two.
  if (options.polarCustomerId) await ensureCustomerExternalId(options.polarCustomerId, options.userId)

  const checkout = await polar().checkouts.create({
    products: [options.productId],
    // Carried back on the webhook so a subscription can be attributed even
    // before a customer record is linked.
    metadata: { barrelmanUserId: options.userId },
    customerEmail: options.email,
    successUrl: options.successUrl,
    // The Polar customer this checkout creates or reuses carries our user id
    // as its external id. reportOverage() meters against that id, so without
    // it the events match no customer and the overage is never billed.
    externalCustomerId: options.userId,
  })
  return checkout.url
}

/**
 * Make sure a Polar customer carries our user id as its external id.
 *
 * Customers created before checkout set `externalCustomerId` have none, so the
 * overage meter cannot resolve them. Polar lets an external id be set once and
 * never changed, so one already set to something else is left alone and
 * reported. Best-effort: a failure here must not fail a webhook or a sync, and
 * the next one tries again.
 */
export async function ensureCustomerExternalId(
  customerId: string,
  userId: string,
  knownExternalId?: string | null,
): Promise<void> {
  if (!billing.enabled) return
  try {
    const current =
      knownExternalId !== undefined
        ? knownExternalId
        : ((await polar().customers.get({ id: customerId })).externalId ?? null)
    if (current === userId) return
    if (current) {
      console.warn(
        `[billing] Polar customer ${customerId} has external id ${current}, not ${userId}; ` +
          'its overage cannot be metered against this account',
      )
      return
    }
    await polar().customers.update({ id: customerId, customerUpdate: { externalId: userId } })
    console.log(`[billing] set external id ${userId} on Polar customer ${customerId}`)
  } catch (err) {
    console.warn(`[billing] could not set the external id on Polar customer ${customerId}:`, err)
  }
}

export async function getPortalUrl(polarCustomerId: string): Promise<string> {
  const session = await polar().customerSessions.create({ customerId: polarCustomerId })
  return session.customerPortalUrl
}

/** The Polar product for a plan id, if that plan is purchasable. */
export function productIdForPlan(planId: string): string | null {
  return planProductId(getPlan(planId))
}

/** Credits a purchased product grants, or null when it isn't a credit pack. */
export function creditsForProduct(productId: string): number | null {
  return billing.creditPacks[productId] ?? null
}

// ── Applying subscription state ─────────────────────────────────────────

/**
 * Move an account onto the plan a Polar product represents.
 *
 * Both caches keyed on the plan must be dropped: the credit balance (which
 * holds the monthly allowance) and the API-key cache (which carries the plan
 * used for rate limiting). Otherwise an upgrade would be invisible for up to a
 * minute, which is exactly when the customer is watching.
 */
export async function applyPlanFromProduct(
  userId: string,
  productId: string | undefined,
  // The subscription period the plan starts from. Accepted so callers pass it
  // already; the plan store does not keep it yet.
  _periodStart?: Date | null,
): Promise<string | null> {
  const plan = productId ? planForProductId(productId) : null

  if (!plan) {
    // A subscription event is evidence the customer is PAYING. Falling back to
    // the free plan here would revoke the plan of anyone whose product we
    // cannot currently map — which happens whenever an environment is missing
    // one of the POLAR_*_PRODUCT_ID vars, or a new product is added to Polar
    // before it is configured here. Leave the plan alone and shout.
    console.error(
      `[billing] cannot map product ${productId ?? '(none)'} to a plan for ${userId} — ` +
        'leaving their plan unchanged. Check the POLAR_*_PRODUCT_ID configuration.',
    )
    return null
  }

  await setPlan(userId, plan.id)
  invalidateUserKeys(userId)
  return plan.id
}

export async function downgradeToFree(userId: string): Promise<void> {
  // An operator-assigned plan is not the billing provider's to revoke. Without
  // this, opening the billing page on the demo account is enough to end the
  // demo: there is no Polar customer behind it, so the sync concludes the
  // subscription lapsed and moves it to free — where it works until the hero
  // has served 100,000 credits and then hard-stops for everyone.
  const current = (await findUserById(userId))?.plan
  if (current && getPlan(current).internal) {
    console.warn(`[billing] leaving ${userId} on the ${current} plan; internal plans are operator-assigned`)
    return
  }

  await setPlan(userId, DEFAULT_PLAN)
  invalidateUserKeys(userId)
}

/**
 * Grant a credit pack, keyed on the Polar order id so a retried webhook cannot
 * grant it twice. Returns false when the order was already applied.
 */
export async function applyCreditPack(
  userId: string,
  productId: string,
  orderId: string,
): Promise<{ granted: boolean; credits: number }> {
  const credits = creditsForProduct(productId)
  if (!credits) return { granted: false, credits: 0 }

  const granted = await grantCredits({
    userId,
    amount: credits,
    kind: 'purchase',
    description: `Credit pack (${credits.toLocaleString()} credits)`,
    externalId: orderId,
  })

  return { granted, credits }
}

/** The order fields a refund clawback reads. */
export interface RefundedOrder {
  id: string
  productId: string | null
  status: string
  netAmount: number
  refundedAmount: number
}

/**
 * How many credits a refund takes back, as a pure function.
 *
 * Polar reports `refundedAmount` as the running total across every refund on
 * the order, so the clawback is a target for the whole order minus what earlier
 * refunds already took. A partial refund takes back the same share of the pack.
 * Tax is refunded separately and is left out of both figures.
 */
export function creditsToClawBack(packCredits: number, order: RefundedOrder, alreadyClawedBack: number): number {
  const fraction =
    order.status === 'refunded' || order.netAmount <= 0 ? 1 : Math.min(1, order.refundedAmount / order.netAmount)
  return Math.max(0, Math.round(packCredits * fraction) - alreadyClawedBack)
}

/**
 * Take back the credits of a refunded credit pack.
 *
 * Written as a negative ledger entry, since the ledger is append-only. The
 * entry is keyed on the order and its refunded total, so a retried webhook
 * writes nothing and a second partial refund writes only its own share.
 * Credits already spent stay spent, so the entry can take the ledger total
 * below zero. On a plan with overage that shortfall is billed as overage. On
 * the free plan purchased credits stop counting until the ledger is positive
 * again, and the monthly allowance is untouched.
 *
 * Two partial refunds delivered at once would each read the same running
 * total and both take their share of it; their keys differ, so the unique
 * index does not stop the second. The read and the insert therefore share a
 * transaction holding an advisory lock on the order.
 */
export async function clawBackCreditPack(userId: string, order: RefundedOrder): Promise<{ clawedBack: number }> {
  const credits = order.productId ? creditsForProduct(order.productId) : null
  if (!credits) return { clawedBack: 0 }

  const prefix = `refund:${order.id}:`
  const clawedBack = await db.transaction(async (tx) => {
    await tx.execute(dsql`SELECT pg_advisory_xact_lock(hashtext(${`refund:${order.id}`}))`)

    // Refund entries are negative, so what was already taken back is the negation.
    const already = -(await ledgerTotalForExternalPrefix(userId, prefix, tx))
    const amount = creditsToClawBack(credits, order, already)
    if (amount === 0) return 0

    const written = await grantCredits(
      {
        userId,
        amount: -amount,
        kind: 'refund',
        description: `Refund of credit pack (${amount.toLocaleString()} credits)`,
        externalId: `${prefix}${order.refundedAmount}`,
      },
      tx,
    )
    return written ? amount : 0
  })

  // Again after commit: the in-transaction invalidation can be refilled from
  // the pre-commit ledger by a request landing in between.
  if (clawedBack) invalidateBalance(userId)
  return { clawedBack }
}

// ── Subscription lifecycle ──────────────────────────────────────────────

/** Read the lifecycle fields from a Polar subscription, as an SDK object or a webhook payload. */
export function toIncomingSubscription(data: object): IncomingSubscription {
  const record = data as Record<string, unknown>
  const date = (value: unknown): Date | null => {
    if (value instanceof Date) return value
    if (typeof value === 'string' && value) return new Date(value)
    return null
  }
  return {
    id: String(record.id),
    status: String(record.status ?? ''),
    productId: (record.productId as string | undefined) ?? null,
    cancelAtPeriodEnd: record.cancelAtPeriodEnd === true,
    currentPeriodStart: date(record.currentPeriodStart),
    currentPeriodEnd: date(record.currentPeriodEnd),
    endsAt: date(record.endsAt),
    endedAt: date(record.endedAt),
    modifiedAt: date(record.modifiedAt),
    createdAt: date(record.createdAt),
  }
}

function storedSubscription(user: User): StoredSubscription {
  return {
    plan: user.plan,
    subscriptionId: user.subscriptionId,
    subscriptionStatus: user.subscriptionStatus,
    subscriptionCancelAtPeriodEnd: user.subscriptionCancelAtPeriodEnd,
    subscriptionPeriodEnd: user.subscriptionPeriodEnd,
    subscriptionModifiedAt: user.subscriptionModifiedAt,
  }
}

const planForProduct = (productId: string) => {
  const plan = planForProductId(productId)
  return plan ? { id: plan.id, rank: plan.rank } : null
}
const rankOf = (planId: string) => getPlan(planId).rank
const rankOfProduct = (productId: string) => rankOf(planForProductId(productId)?.id ?? DEFAULT_PLAN)

/** `col = value`, or `col IS NULL` when the value is null. */
function matches(column: Column, value: unknown) {
  return value === null ? isNull(column) : eq(column, value)
}

export interface SubscriptionEventResult {
  applied: boolean
  /** The plan the account is on afterwards. */
  planId: string | null
  reason?: string
}

/**
 * Apply one Polar subscription state to an account.
 *
 * `reduceSubscription()` makes the decision. The write is conditional on the
 * subscription columns still holding what the decision was made from, so two
 * deliveries racing each other cannot both apply: the loser re-reads and
 * decides again against the winner's state. That is also what sends the
 * past-due notice once.
 */
export async function applySubscriptionEvent(
  userId: string,
  incoming: IncomingSubscription,
  options: { revoked?: boolean } = {},
): Promise<SubscriptionEventResult> {
  for (let attempt = 0; attempt < 3; attempt++) {
    const user = await findUserById(userId)
    if (!user) return { applied: false, planId: null, reason: 'unknown account' }

    const stored = storedSubscription(user)
    const outcome = reduceSubscription(stored, incoming, {
      revoked: options.revoked === true,
      now: new Date(),
      planForProduct,
      rankOf,
    })

    if (outcome.ignored !== undefined) {
      console.log(`[billing] ignoring subscription ${incoming.id} for ${userId}: ${outcome.ignored}`)
      return { applied: false, planId: user.plan, reason: outcome.ignored }
    }

    const written = await db
      .update(users)
      .set({ ...outcome.state, updatedAt: new Date() })
      .where(
        and(
          eq(users.id, userId),
          matches(users.subscriptionId, stored.subscriptionId),
          matches(users.subscriptionStatus, stored.subscriptionStatus),
          matches(users.subscriptionModifiedAt, stored.subscriptionModifiedAt),
        ),
      )
      .returning({ id: users.id })
    if (written.length === 0) continue

    // The billing period the applied plan belongs to, handed to
    // applyPlanFromProduct alongside the product.
    const periodStart = incoming.currentPeriodStart

    let planId: string | null = user.plan
    if (outcome.plan.kind === 'set') {
      planId = (await applyPlanFromProduct(userId, incoming.productId ?? undefined, periodStart)) ?? user.plan
    } else if (outcome.plan.kind === 'free') {
      await endPlanOrFallBack(user, incoming.id)
      planId = (await findUserById(userId))?.plan ?? DEFAULT_PLAN
    }

    if (outcome.enteredPastDue) {
      // Not awaited: a mail outage must not fail the webhook, and a redelivery
      // would not resend anyway, since the account is already past_due.
      sendPaymentFailedNotice(user.email, getPlan(user.plan).name).catch((err) =>
        console.error('[billing] could not send the payment-failed notice', err),
      )
    }

    return { applied: true, planId }
  }

  // Three lost races in a row. Failing makes Polar redeliver, and the next
  // attempt decides afresh against whatever state won.
  throw new Error(`could not apply subscription ${incoming.id} to ${userId}: concurrent updates`)
}

export interface EndPlanDeps {
  findActive: typeof findActiveSubscription
  apply: typeof applySubscriptionEvent
  downgrade: typeof downgradeToFree
}

/**
 * The subscription behind the plan has ended. Move the account onto any other
 * subscription it still has live, and to free only when there is none.
 *
 * An account can hold two live subscriptions (duplicates opened before plan
 * changes went through `subscriptions.update`). While both are live the richer
 * one owns the plan and the other's events are ignored, so when the richer one
 * ends, the other has to be found here or the account drops to free while it
 * still bills. A failed lookup throws, so the webhook answers 500 and Polar
 * retries, rather than downgrading an account that may still be paying.
 * Returns the subscription that took over, or null.
 */
export async function endPlanOrFallBack(
  account: BillingAccount,
  endedSubscriptionId: string,
  deps: EndPlanDeps = { findActive: findActiveSubscription, apply: applySubscriptionEvent, downgrade: downgradeToFree },
): Promise<string | null> {
  // Skip the stored subscription: it is the one that just ended.
  const replacement = await deps.findActive(
    { ...account, subscriptionId: null, subscriptionStatus: null },
    { exclude: endedSubscriptionId },
  )
  if (replacement) {
    console.log(`[billing] ${account.id}: ${endedSubscriptionId} ended, ${replacement.id} is still live; applying it`)
    await deps.apply(account.id, toIncomingSubscription(replacement))
    return replacement.id
  }
  await deps.downgrade(account.id)
  return null
}

/** Overwrite the stored subscription columns. For a sync, where Polar's current state wins. */
async function storeSubscriptionState(userId: string, incoming: IncomingSubscription | null): Promise<void> {
  await db
    .update(users)
    .set({
      subscriptionId: incoming?.id ?? null,
      subscriptionStatus: incoming?.status ?? null,
      subscriptionCancelAtPeriodEnd: incoming?.cancelAtPeriodEnd ?? false,
      subscriptionPeriodEnd: incoming ? (incoming.endsAt ?? incoming.currentPeriodEnd) : null,
      subscriptionModifiedAt: incoming ? (incoming.modifiedAt ?? incoming.createdAt ?? new Date()) : null,
      updatedAt: new Date(),
    })
    .where(eq(users.id, userId))
}

/** Re-read one subscription from Polar and apply it. Used when a refund may have ended it. */
export async function refreshSubscription(userId: string, subscriptionId: string): Promise<SubscriptionEventResult> {
  if (!billing.enabled) return { applied: false, planId: null, reason: 'billing disabled' }
  const sub = await polar().subscriptions.get({ id: subscriptionId })
  return applySubscriptionEvent(userId, toIncomingSubscription(sub))
}

export type BillingAccount = Pick<User, 'id' | 'polarCustomerId' | 'subscriptionId' | 'subscriptionStatus'>

/**
 * The account's live Polar subscription, if it has one.
 *
 * Tries the stored subscription first, then asks Polar for any live one on the
 * customer, since accounts that subscribed before the subscription id was
 * stored have only a customer link. Errors from the listing propagate: callers
 * use the answer to decide whether a second subscription is safe to open, or
 * an account safe to delete.
 */
export async function findActiveSubscription(account: BillingAccount, options: { exclude?: string } = {}) {
  const p = polar()

  if (account.subscriptionId && account.subscriptionStatus !== REVOKED && account.subscriptionId !== options.exclude) {
    try {
      const sub = await p.subscriptions.get({ id: account.subscriptionId })
      if (isLiveStatus(sub.status)) return sub
    } catch (err) {
      console.warn(`[billing] could not read subscription ${account.subscriptionId}; checking the customer`, err)
    }
  }

  if (!account.polarCustomerId) return null

  const subs = await p.subscriptions.list({ customerId: account.polarCustomerId, active: true, limit: 10 })
  // Richest plan first, matching syncSubscription.
  const live = subs.result.items
    .filter((sub) => isLiveStatus(sub.status) && sub.id !== options.exclude)
    .sort((a, b) => rankOfProduct(b.productId) - rankOfProduct(a.productId))
  return live[0] ?? null
}

export type ChangePlanResult =
  | { kind: 'no-subscription' }
  | { kind: 'unchanged'; subscription: IncomingSubscription }
  | { kind: 'changed'; subscription: IncomingSubscription }

/**
 * Move a live subscription onto another product.
 *
 * A plan change used to start a fresh checkout, which left the customer paying
 * for two subscriptions. Updating the one they have keeps a single subscription
 * and lets Polar prorate. Upgrades are invoiced at once, so the larger
 * allowance is paid for before it is spent; downgrades are prorated onto the
 * next invoice as a credit.
 */
export async function changeSubscriptionProduct(account: BillingAccount, productId: string): Promise<ChangePlanResult> {
  const current = await findActiveSubscription(account)
  if (!current) return { kind: 'no-subscription' }

  if (current.productId === productId) {
    return { kind: 'unchanged', subscription: toIncomingSubscription(current) }
  }

  const upgrade = rankOfProduct(productId) > rankOfProduct(current.productId)
  const updated = await polar().subscriptions.update({
    id: current.id,
    subscriptionUpdate: { productId, prorationBehavior: upgrade ? 'invoice' : 'prorate' },
  })
  return { kind: 'changed', subscription: toIncomingSubscription(updated) }
}

/**
 * Whether `err` is one of the SDK's named Polar errors. Matched on `name`, which
 * every SDK error class sets, so this holds across the esm and cjs builds.
 */
function isPolarError(err: unknown, ...names: string[]): boolean {
  return err instanceof Error && names.includes(err.name)
}

/** Raised when an account cannot be closed because its billing could not be settled. */
export class BillingTeardownError extends Error {
  constructor(message: string) {
    super(message)
    this.name = 'BillingTeardownError'
  }
}

/**
 * Revoke the account's live subscription ahead of deleting the account.
 * Returns the revoked subscription id, or null when there was none.
 *
 * Throws BillingTeardownError rather than carrying on: a deleted account whose
 * subscription keeps renewing is a customer charged every month with no
 * account left to cancel from.
 */
export async function revokeActiveSubscription(account: BillingAccount): Promise<string | null> {
  if (!billing.enabled) return null
  let subscriptionId: string
  try {
    const sub = await findActiveSubscription(account)
    if (!sub) return null
    subscriptionId = sub.id
    try {
      await polar().subscriptions.revoke({ id: subscriptionId })
    } catch (err) {
      // Gone or already ended between the lookup and the revoke: the outcome
      // we wanted, and refusing would leave the account undeletable.
      if (!isPolarError(err, 'ResourceNotFound', 'AlreadyCanceledSubscription')) throw err
      console.log(`[billing] subscription ${subscriptionId} of ${account.id} was already gone or revoked`)
      return subscriptionId
    }
  } catch (err) {
    console.error(`[billing] could not revoke the subscription of ${account.id}`, err)
    throw new BillingTeardownError(
      'The Polar subscription on this account could not be revoked, so the account was not deleted. ' +
        'Try again, or revoke the subscription in Polar first.',
    )
  }
  console.log(`[billing] revoked subscription ${subscriptionId} of ${account.id} ahead of deletion`)
  return subscriptionId
}

// ── Webhook deliveries ──────────────────────────────────────────────────

/*
 * Deliveries are checked before handling and recorded only after it succeeds.
 * Claiming first would lose an event whenever the process died mid-handler:
 * the claim would outlive it and Polar's retry would be skipped as a
 * duplicate. Two copies of one delivery arriving together can both run, which
 * is safe because every handler is idempotent on its own (keyed ledger
 * entries, ordered subscription writes).
 */

/** Whether a delivery with this `webhook-id` has already been handled. */
export async function isWebhookEventHandled(id: string): Promise<boolean> {
  const [row] = await db
    .select({ id: billingWebhookEvents.id })
    .from(billingWebhookEvents)
    .where(eq(billingWebhookEvents.id, id))
    .limit(1)
  return Boolean(row)
}

/** Record a delivery as handled, so a redelivery is acknowledged and skipped. */
export async function recordWebhookEvent(id: string, type: string): Promise<void> {
  await db.insert(billingWebhookEvents).values({ id, type }).onConflictDoNothing({ target: billingWebhookEvents.id })
}

/** Drop delivery ids older than Polar's retry window. Called from the account sweep. */
export async function pruneWebhookEvents(maxAgeDays = 30): Promise<number> {
  const deleted = await db
    .delete(billingWebhookEvents)
    .where(lt(billingWebhookEvents.receivedAt, dsql`now() - make_interval(days => ${maxAgeDays})`))
    .returning({ id: billingWebhookEvents.id })
  return deleted.length
}

// ── Reconciliation ──────────────────────────────────────────────────────

export interface SubscriptionSummary {
  planId: string
  status: string | null
  currentPeriodEnd: string | null
  cancelAtPeriodEnd: boolean
  amount: number | null
  currency: string | null
}

/**
 * Re-read Polar and make the local plan match. Used by the console's manual
 * refresh and as a repair path when a webhook was missed.
 */
export async function syncSubscription(userId: string, email: string): Promise<SubscriptionSummary> {
  const user = await findUserById(userId)
  const free: SubscriptionSummary = {
    planId: DEFAULT_PLAN,
    status: null,
    currentPeriodEnd: null,
    cancelAtPeriodEnd: false,
    amount: null,
    currency: null,
  }

  if (!billing.enabled) return { ...free, planId: user?.plan ?? DEFAULT_PLAN }

  const p = polar()

  let customerId = user?.polarCustomerId ?? null
  let externalId: string | null = null
  if (customerId) {
    try {
      externalId = (await p.customers.get({ id: customerId })).externalId ?? null
    } catch {
      // Deleted on Polar's side; fall through to the email lookup.
      customerId = null
    }
  }

  if (!customerId) {
    const customers = await p.customers.list({
      email,
      organizationId: billing.organizationId || undefined,
      limit: 1,
    })
    const found = customers.result.items[0]
    customerId = found?.id ?? null
    externalId = found?.externalId ?? null
  }

  if (!customerId) {
    await downgradeToFree(userId)
    await storeSubscriptionState(userId, null)
    return free
  }

  // Refuse to steal a customer already attached to another account — that
  // would move someone else's subscription onto this one.
  const existing = await findUserByPolarCustomerId(customerId)
  if (existing && existing.id !== userId) {
    console.warn(`[billing] Polar customer ${customerId} already linked to ${existing.id}; refusing to re-link`)
    await downgradeToFree(userId)
    return free
  }

  await linkPolarCustomer(userId, customerId)
  // Customers from before checkout set an external id have none, and the
  // overage meter cannot find them without it.
  await ensureCustomerExternalId(customerId, userId, externalId)

  const summarise = (planId: string, sub: { status: string; currentPeriodEnd: Date | null; cancelAtPeriodEnd: boolean; amount: number; currency: string }): SubscriptionSummary => ({
    planId,
    status: sub.status ?? null,
    currentPeriodEnd: sub.currentPeriodEnd?.toISOString() ?? null,
    cancelAtPeriodEnd: sub.cancelAtPeriodEnd ?? false,
    amount: sub.amount ?? null,
    currency: sub.currency ?? null,
  })

  // Check plans richest-first so an account holding two subscriptions lands on
  // the better one rather than whichever came back first.
  for (const plan of [...listPlans()].reverse()) {
    const productId = planProductId(plan)
    if (!productId) continue

    const subs = await p.subscriptions.list({ customerId, productId, active: true, limit: 1 })
    const sub = subs.result.items[0]
    if (!sub) continue

    await setPlan(userId, plan.id)
    invalidateUserKeys(userId)
    await storeSubscriptionState(userId, toIncomingSubscription(sub))

    return summarise(plan.id, sub)
  }

  // A past-due subscription keeps its plan while Polar retries the payment.
  // Read the stored one directly rather than trusting the `active` listing to
  // include it.
  if (user?.subscriptionId && user.subscriptionStatus !== REVOKED) {
    try {
      const sub = await p.subscriptions.get({ id: user.subscriptionId })
      if (sub.customerId === customerId && sub.status === 'past_due') {
        await storeSubscriptionState(userId, toIncomingSubscription(sub))
        return summarise(user.plan, sub)
      }
    } catch {
      // Gone on Polar's side; treat as no subscription.
    }
  }

  await downgradeToFree(userId)
  await storeSubscriptionState(userId, null)
  return free
}

// ── Metered overage ─────────────────────────────────────────────────────

/**
 * Report overage credits to Polar's meter.
 *
 * Only credits beyond the plan allowance are reported — the allowance itself is
 * already paid for in the subscription price, so metering all usage would bill
 * it twice. Events are keyed by the barrelman user id as the external customer,
 * so Polar resolves them without us holding its customer id here.
 *
 * Best-effort by design: a failure to report is logged and dropped rather than
 * retried, because the local usage table remains the durable record and can be
 * replayed. Never let a billing call fail a customer's request.
 */
export async function reportOverage(entries: Array<{ userId: string; credits: number }>): Promise<number> {
  if (!billing.enabled || !billing.meterUsage) return 0

  const events = entries
    .filter((entry) => entry.credits > 0)
    .map((entry) => ({
      name: billing.usageEventName,
      externalCustomerId: entry.userId,
      metadata: { credits: entry.credits },
    }))

  if (events.length === 0) return 0

  try {
    await polar().events.ingest({ events })
    return events.length
  } catch (err) {
    console.error('[billing] failed to report overage to Polar', err)
    return 0
  }
}
