/**
 * Billing routes: plan configuration, checkout, plan changes, the customer
 * portal, and the Polar webhook.
 *
 * The webhook is the only unauthenticated route here, and it is authenticated
 * in a different way — by Polar's signature over the raw body. It therefore
 * lives on its own Elysia instance, so the session guard on the others cannot
 * accidentally apply to it (or, worse, fail to).
 */
import Elysia, { t } from 'elysia'
import { validateEvent, WebhookVerificationError } from '@polar-sh/sdk/webhooks'
import { billing } from '../config/billing.config'
import {
  applyCreditPack as _applyCreditPack,
  applySubscriptionEvent as _applySubscriptionEvent,
  changeSubscriptionProduct as _changeSubscriptionProduct,
  isWebhookEventHandled as _isWebhookEventHandled,
  clawBackCreditPack as _clawBackCreditPack,
  createCheckout as _createCheckout,
  ensureCustomerExternalId as _ensureCustomerExternalId,
  findActiveSubscription as _findActiveSubscription,
  getPortalUrl as _getPortalUrl,
  getProducts as _getProducts,
  productIdForPlan as _productIdForPlan,
  refreshSubscription as _refreshSubscription,
  recordWebhookEvent as _recordWebhookEvent,
  syncSubscription as _syncSubscription,
  toIncomingSubscription,
} from '../services/billing.service'
import {
  findUserById as _findUserById,
  findUserByPolarCustomerId as _findUserByPolarCustomerId,
  linkPolarCustomer as _linkPolarCustomer,
} from '../services/accounts.service'
import { getBalance as _getBalance } from '../services/credits.service'
import { resolveSession as _resolveSession, requireUser } from '../middleware/session'
import { consoleOrigin } from '../config/accounts.config'
import { getPlan, listPlans, publicPlan } from '../billing/plans'

export interface BillingDeps {
  getProducts: typeof _getProducts
  createCheckout: typeof _createCheckout
  getPortalUrl: typeof _getPortalUrl
  syncSubscription: typeof _syncSubscription
  applySubscriptionEvent: typeof _applySubscriptionEvent
  refreshSubscription: typeof _refreshSubscription
  findActiveSubscription: typeof _findActiveSubscription
  changeSubscriptionProduct: typeof _changeSubscriptionProduct
  ensureCustomerExternalId: typeof _ensureCustomerExternalId
  applyCreditPack: typeof _applyCreditPack
  clawBackCreditPack: typeof _clawBackCreditPack
  isWebhookEventHandled: typeof _isWebhookEventHandled
  recordWebhookEvent: typeof _recordWebhookEvent
  productIdForPlan: typeof _productIdForPlan
  findUserById: typeof _findUserById
  findUserByPolarCustomerId: typeof _findUserByPolarCustomerId
  linkPolarCustomer: typeof _linkPolarCustomer
  getBalance: typeof _getBalance
  resolveSession: typeof _resolveSession
  validateEvent: typeof validateEvent
}

const defaultDeps: BillingDeps = {
  getProducts: _getProducts,
  createCheckout: _createCheckout,
  getPortalUrl: _getPortalUrl,
  syncSubscription: _syncSubscription,
  applySubscriptionEvent: _applySubscriptionEvent,
  refreshSubscription: _refreshSubscription,
  findActiveSubscription: _findActiveSubscription,
  changeSubscriptionProduct: _changeSubscriptionProduct,
  ensureCustomerExternalId: _ensureCustomerExternalId,
  applyCreditPack: _applyCreditPack,
  clawBackCreditPack: _clawBackCreditPack,
  isWebhookEventHandled: _isWebhookEventHandled,
  recordWebhookEvent: _recordWebhookEvent,
  productIdForPlan: _productIdForPlan,
  findUserById: _findUserById,
  findUserByPolarCustomerId: _findUserByPolarCustomerId,
  linkPolarCustomer: _linkPolarCustomer,
  getBalance: _getBalance,
  resolveSession: _resolveSession,
  validateEvent,
}

/** Every subscription event carries the full subscription, so all of them go through one path. */
const SUBSCRIPTION_EVENTS = new Set([
  'subscription.created',
  'subscription.active',
  'subscription.updated',
  'subscription.canceled',
  'subscription.uncanceled',
  'subscription.past_due',
  'subscription.revoked',
])

const disabled = { error: 'Billing is not enabled on this instance' }

export function createBillingRoutes(overrides: Partial<BillingDeps> = {}) {
  const deps = { ...defaultDeps, ...overrides }

  const derive = async ({
    request,
    set,
  }: {
    request: Request
    set: { headers: Record<string, string | number> }
  }) => {
    const { user, session, refreshedCookie } = await deps.resolveSession(request)
    if (refreshedCookie) set.headers['set-cookie'] = refreshedCookie
    return { user, session }
  }

  /**
   * Resolve a plan id to a purchasable product, server-side, so a caller cannot
   * name an arbitrary product in the organization. Enterprise is negotiated,
   * and saying "unknown plan" for it would read as a bug to someone who can
   * see it on the pricing page.
   */
  const resolvePlanProduct = (planId: string) => {
    const plan = getPlan(planId)
    if (plan.id === planId && plan.contactOnly) {
      return {
        status: 409,
        error: {
          error: `The ${plan.name} plan is arranged directly — get in touch and we will set it up.`,
          contactOnly: true,
          plan: plan.id,
        },
      } as const
    }
    const productId = deps.productIdForPlan(planId)
    if (!productId) return { status: 400, error: { error: 'Unknown plan' } } as const
    return { productId, plan } as const
  }

  // ── Public configuration ──────────────────────────────────────────────
  const publicRoutes = new Elysia({ prefix: '/billing' }).get(
    '/config',
    async () => ({
      billingEnabled: billing.enabled,
      plans: listPlans().map(publicPlan),
      products: await deps.getProducts(),
      creditPacks: Object.entries(billing.creditPacks).map(([productId, credits]) => ({ productId, credits })),
    }),
    {
      detail: {
        summary: 'Billing configuration',
        description: 'Whether billing is enabled, and the plans and credit packs on offer.',
        tags: ['Billing'],
      },
    },
  )

  // ── Polar webhook — signature-authenticated, never session-authenticated ─
  const webhookRoutes = new Elysia({ prefix: '/billing' }).post(
    '/webhook',
    async ({ body, request, set }) => {
      if (!billing.enabled) {
        set.status = 404
        return disabled
      }

      const headers: Record<string, string> = {}
      request.headers.forEach((value, key) => {
        headers[key] = value
      })

      let event: { type: string; data: Record<string, unknown> }
      try {
        event = deps.validateEvent(body as string, headers, billing.webhookSecret) as typeof event
      } catch (err) {
        if (err instanceof WebhookVerificationError) {
          console.warn('[billing] webhook signature verification failed')
          set.status = 403
          return { error: 'Invalid webhook signature' }
        }
        set.status = 400
        return { error: 'Invalid webhook payload' }
      }

      const { type, data } = event

      // Polar redelivers until it sees a 2xx, so the same event can arrive
      // twice. The delivery id is covered by the signature, so it can be
      // trusted as a dedupe key. It is recorded only once handling succeeds,
      // so a crash part-way leaves nothing behind and the retry runs.
      const deliveryId = headers['webhook-id']
      if (deliveryId && (await deps.isWebhookEventHandled(deliveryId))) {
        return { received: true, duplicate: true }
      }

      try {
        await handleEvent(type, data)
      } catch (err) {
        // Non-2xx, so Polar retries.
        console.error(`[billing] failed to handle ${type}`, err)
        set.status = 500
        return { error: 'Webhook handling failed' }
      }

      if (deliveryId) {
        // The event is applied. Failing to note that only costs a harmless
        // re-run on redelivery, so it must not turn the response into a 500.
        await deps
          .recordWebhookEvent(deliveryId, type)
          .catch((err) => console.warn(`[billing] could not record delivery ${deliveryId}`, err))
      }

      return { received: true }
    },
    {
      // The signature covers the raw bytes, so the body must not be parsed
      // into an object before validateEvent sees it.
      parse: ({ request, contentType }) => {
        if (contentType === 'application/json') return request.text()
      },
      detail: {
        summary: 'Polar webhook',
        description:
          'Applies subscription, credit-pack and refund events. Authenticated by Polar signature; ' +
          'deliveries are deduplicated by their webhook-id.',
        tags: ['Billing'],
      },
    },
  )

  async function handleEvent(type: string, data: Record<string, unknown>): Promise<void> {
    const polarCustomerId = data.customerId as string | undefined
    const productId = data.productId as string | undefined
    const metadataUserId = (data.metadata as Record<string, string> | undefined)?.barrelmanUserId
    const customer = data.customer as { externalId?: string | null } | undefined

    /**
     * Resolve the account this event belongs to. The linked customer wins over
     * checkout metadata: metadata is whatever we set when the checkout was
     * created, while the link reflects who owns the customer now. The
     * customer's external id is our user id when it is set.
     */
    const resolveUserId = async (): Promise<string | null> => {
      const linked = polarCustomerId ? await deps.findUserByPolarCustomerId(polarCustomerId) : null
      if (linked) return linked.id
      for (const candidate of [customer?.externalId, metadataUserId]) {
        if (candidate && (await deps.findUserById(candidate))) return candidate
      }
      return null
    }

    if (SUBSCRIPTION_EVENTS.has(type)) {
      const userId = await resolveUserId()
      if (!userId) {
        console.warn(`[billing] ${type} for unknown account (customer ${polarCustomerId})`)
        return
      }
      if (polarCustomerId) {
        await deps.linkPolarCustomer(userId, polarCustomerId)
        await deps.ensureCustomerExternalId(polarCustomerId, userId, customer?.externalId ?? null)
      }
      const result = await deps.applySubscriptionEvent(userId, toIncomingSubscription(data), {
        revoked: type === 'subscription.revoked',
      })
      if (result.applied) console.log(`[billing] ${userId} → ${result.planId} (${type})`)
      return
    }

    switch (type) {
      case 'order.paid': {
        // Credit packs are one-off purchases, so they arrive as orders rather
        // than subscription events.
        const userId = await resolveUserId()
        const orderId = data.id as string | undefined
        if (!userId || !productId || !orderId) return

        const { granted, credits } = await deps.applyCreditPack(userId, productId, orderId)
        if (granted) console.log(`[billing] granted ${credits} credits to ${userId} (order ${orderId})`)
        return
      }

      case 'order.refunded': {
        const userId = await resolveUserId()
        const orderId = data.id as string | undefined
        if (!userId || !orderId) {
          console.warn(`[billing] order.refunded for unknown account or order (customer ${polarCustomerId})`)
          return
        }

        const subscriptionId = data.subscriptionId as string | null | undefined
        if (subscriptionId) {
          // Refunding a subscription payment does not by itself end the
          // subscription; Polar revokes it when the refund asks for that. Read
          // its current state rather than guessing from the refund.
          await deps.refreshSubscription(userId, subscriptionId)
          return
        }

        const { clawedBack } = await deps.clawBackCreditPack(userId, {
          id: orderId,
          productId: productId ?? null,
          status: String(data.status ?? ''),
          netAmount: Number(data.netAmount ?? 0),
          refundedAmount: Number(data.refundedAmount ?? 0),
        })
        if (clawedBack) console.log(`[billing] took back ${clawedBack} credits from ${userId} (refund of ${orderId})`)
        return
      }

      default:
        // Everything else is acknowledged and ignored, so Polar stops retrying
        // events we have no opinion about.
        return
    }
  }

  // ── Account-authenticated ─────────────────────────────────────────────
  const guardedRoutes = new Elysia({ prefix: '/billing' })
    .derive(derive)
    .onBeforeHandle(requireUser)

    .get(
      '/status',
      async ({ user }) => {
        const balance = await deps.getBalance(user!.id)
        const account = await deps.findUserById(user!.id)
        const subscription = account?.subscriptionId
          ? {
              status: account.subscriptionStatus,
              cancelAtPeriodEnd: account.subscriptionCancelAtPeriodEnd,
              currentPeriodEnd: account.subscriptionPeriodEnd?.toISOString() ?? null,
            }
          : null
        return {
          billingEnabled: billing.enabled,
          plan: getPlan(user!.plan),
          hasSubscription: Boolean(account?.polarCustomerId),
          subscription,
          balance,
        }
      },
      {
        detail: {
          summary: 'Current plan and balance',
          description:
            'The plan, the credit balance, and the subscription behind the plan: its status ' +
            '(`past_due` while a failed payment is retried), and whether it is cancelled at the period end.',
          tags: ['Billing'],
        },
      },
    )

    .post(
      '/checkout',
      async ({ user, body, set }) => {
        if (!billing.enabled) {
          set.status = 404
          return disabled
        }

        let productId: string | null = null
        if (body.plan) {
          const resolved = resolvePlanProduct(body.plan)
          if ('error' in resolved) {
            set.status = resolved.status
            return resolved.error
          }
          productId = resolved.productId
        } else if (body.creditPack && billing.creditPacks[body.creditPack]) {
          productId = body.creditPack
        }

        if (!productId) {
          set.status = 400
          return { error: 'Unknown plan or credit pack' }
        }

        const account = await deps.findUserById(user!.id)

        // A second checkout for a plan opens a second subscription, and the
        // customer pays for both. Plan changes go through /billing/change-plan.
        let existing = null
        if (body.plan && account) {
          try {
            existing = await deps.findActiveSubscription(account)
          } catch (err) {
            // Without an answer, a checkout could open a second subscription.
            console.error(`[billing] could not check ${account.id} for a subscription before checkout`, err)
            set.status = 502
            return { error: 'Could not reach Polar to check your subscription. Nothing was charged; try again shortly.' }
          }
        }
        if (existing) {
          set.status = 409
          return {
            error: 'This account already has a subscription. Change plan instead of checking out again.',
            hasSubscription: true,
          }
        }

        const url = await deps.createCheckout({
          userId: user!.id,
          email: account?.email ?? user!.email,
          productId,
          successUrl: `${consoleOrigin}/console/billing?checkout=success`,
          polarCustomerId: account?.polarCustomerId ?? null,
        })
        return { checkoutUrl: url }
      },
      {
        body: t.Object({
          plan: t.Optional(t.String()),
          creditPack: t.Optional(t.String()),
        }),
        detail: {
          summary: 'Start a checkout',
          description:
            'Pass a plan id to subscribe, or a credit-pack product id to buy credits. A plan checkout ' +
            'is refused with 409 when the account already has a subscription; use /billing/change-plan.',
          tags: ['Billing'],
        },
      },
    )

    .post(
      '/change-plan',
      async ({ user, body, set }) => {
        if (!billing.enabled) {
          set.status = 404
          return disabled
        }

        const resolved = resolvePlanProduct(body.plan)
        if ('error' in resolved) {
          set.status = resolved.status
          return resolved.error
        }

        const account = await deps.findUserById(user!.id)
        if (!account) {
          set.status = 404
          return { error: 'Account not found' }
        }

        let result
        try {
          result = await deps.changeSubscriptionProduct(account, resolved.productId)
        } catch (err) {
          console.error(`[billing] plan change for ${account.id} failed`, err)
          set.status = 502
          return { error: 'Polar did not accept the plan change. Nothing was charged; try again shortly.' }
        }

        if (result.kind === 'no-subscription') {
          // Nothing to change yet, so this is a first subscription.
          const checkoutUrl = await deps.createCheckout({
            userId: account.id,
            email: account.email,
            productId: resolved.productId,
            successUrl: `${consoleOrigin}/console/billing?checkout=success`,
            polarCustomerId: account.polarCustomerId,
          })
          return { changed: false, checkoutUrl }
        }

        // Apply now rather than waiting for the webhook, so the console shows
        // the new plan as soon as it reloads. The webhook then finds nothing
        // newer to do.
        const applied = await deps.applySubscriptionEvent(account.id, result.subscription)
        return { changed: result.kind === 'changed', planId: applied.planId ?? account.plan }
      },
      {
        body: t.Object({ plan: t.String({ minLength: 1 }) }),
        detail: {
          summary: 'Change plan',
          description:
            'Moves the existing subscription onto another plan, prorated by Polar: upgrades are invoiced ' +
            'at once, downgrades credited to the next invoice. With no subscription yet, returns a ' +
            '`checkoutUrl` instead.',
          tags: ['Billing'],
        },
      },
    )

    .get(
      '/portal',
      async ({ user, set }) => {
        if (!billing.enabled) {
          set.status = 404
          return disabled
        }
        const account = await deps.findUserById(user!.id)
        if (!account?.polarCustomerId) {
          set.status = 404
          return { error: 'No billing account yet — subscribe first' }
        }
        return { portalUrl: await deps.getPortalUrl(account.polarCustomerId) }
      },
      {
        detail: {
          summary: 'Customer portal link',
          description: 'Where the customer manages payment methods, invoices and cancellation.',
          tags: ['Billing'],
        },
      },
    )

    .post(
      '/sync',
      async ({ user, set }) => {
        if (!billing.enabled) {
          set.status = 404
          return disabled
        }
        const account = await deps.findUserById(user!.id)
        return deps.syncSubscription(user!.id, account?.email ?? user!.email)
      },
      {
        detail: {
          summary: 'Re-read subscription state from Polar',
          description: 'Repair path for a missed webhook — Polar wins over the local plan.',
          tags: ['Billing'],
        },
      },
    )

  return new Elysia().use(publicRoutes).use(webhookRoutes).use(guardedRoutes)
}

export const billingRoutes = createBillingRoutes()
