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
import { billing } from '../config/billing.config'
import { getPlan, listPlans, planForProductId, planProductId, type Plan } from '../billing/plans'
import { findUserByPolarCustomerId, linkPolarCustomer, findUserById } from './accounts.service'
import { grantCredits, setPlan } from './credits.service'
import { invalidateUserKeys } from './api-keys.service'
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
}): Promise<string> {
  const checkout = await polar().checkouts.create({
    products: [options.productId],
    // Carried back on the webhook so a subscription can be attributed even
    // before a customer record is linked.
    metadata: { barrelmanUserId: options.userId },
    customerEmail: options.email,
    successUrl: options.successUrl,
  })
  return checkout.url
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
export async function applyPlanFromProduct(userId: string, productId: string | undefined): Promise<string | null> {
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
  if (customerId) {
    try {
      await p.customers.get({ id: customerId })
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
    customerId = customers.result.items[0]?.id ?? null
  }

  if (!customerId) {
    await downgradeToFree(userId)
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

    return {
      planId: plan.id,
      status: sub.status ?? null,
      currentPeriodEnd: sub.currentPeriodEnd?.toISOString() ?? null,
      cancelAtPeriodEnd: sub.cancelAtPeriodEnd ?? false,
      amount: sub.amount ?? null,
      currency: sub.currency ?? null,
    }
  }

  await downgradeToFree(userId)
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
