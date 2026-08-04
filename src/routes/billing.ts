/**
 * Billing routes: plan configuration, checkout, the customer portal, and the
 * Polar webhook.
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
  applyPlanFromProduct as _applyPlanFromProduct,
  createCheckout as _createCheckout,
  downgradeToFree as _downgradeToFree,
  getPortalUrl as _getPortalUrl,
  getProducts as _getProducts,
  productIdForPlan as _productIdForPlan,
  syncSubscription as _syncSubscription,
} from '../services/billing.service'
import {
  findUserById as _findUserById,
  findUserByPolarCustomerId as _findUserByPolarCustomerId,
  linkPolarCustomer as _linkPolarCustomer,
} from '../services/accounts.service'
import { getBalance as _getBalance } from '../services/credits.service'
import { resolveSession as _resolveSession, requireUser } from '../middleware/session'
import { consoleOrigin } from '../config/accounts.config'
import { getPlan, listPlans } from '../billing/plans'

export interface BillingDeps {
  getProducts: typeof _getProducts
  createCheckout: typeof _createCheckout
  getPortalUrl: typeof _getPortalUrl
  syncSubscription: typeof _syncSubscription
  applyPlanFromProduct: typeof _applyPlanFromProduct
  applyCreditPack: typeof _applyCreditPack
  downgradeToFree: typeof _downgradeToFree
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
  applyPlanFromProduct: _applyPlanFromProduct,
  applyCreditPack: _applyCreditPack,
  downgradeToFree: _downgradeToFree,
  productIdForPlan: _productIdForPlan,
  findUserById: _findUserById,
  findUserByPolarCustomerId: _findUserByPolarCustomerId,
  linkPolarCustomer: _linkPolarCustomer,
  getBalance: _getBalance,
  resolveSession: _resolveSession,
  validateEvent,
}

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

  // ── Public configuration ──────────────────────────────────────────────
  const publicRoutes = new Elysia({ prefix: '/billing' }).get(
    '/config',
    async () => ({
      billingEnabled: billing.enabled,
      plans: listPlans(),
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
        return { error: 'Billing is not enabled on this instance' }
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
      const polarCustomerId = data.customerId as string | undefined
      const productId = data.productId as string | undefined
      const metadataUserId = (data.metadata as Record<string, string> | undefined)?.barrelmanUserId

      /**
       * Resolve the account this event belongs to. The linked customer wins
       * over checkout metadata: metadata is whatever we set when the checkout
       * was created, while the link reflects who actually owns the customer
       * now.
       */
      const resolveUserId = async (): Promise<string | null> => {
        const linked = polarCustomerId ? await deps.findUserByPolarCustomerId(polarCustomerId) : null
        if (linked) return linked.id
        if (metadataUserId && (await deps.findUserById(metadataUserId))) return metadataUserId
        return null
      }

      switch (type) {
        case 'subscription.active':
        case 'subscription.updated': {
          const userId = await resolveUserId()
          if (!userId) {
            console.warn(`[billing] ${type} for unknown account (customer ${polarCustomerId})`)
            break
          }
          if (polarCustomerId) await deps.linkPolarCustomer(userId, polarCustomerId)
          const planId = await deps.applyPlanFromProduct(userId, productId)
          console.log(`[billing] ${userId} → ${planId} (${type})`)
          break
        }

        case 'subscription.canceled':
        case 'subscription.revoked': {
          const userId = await resolveUserId()
          if (!userId) {
            console.warn(`[billing] ${type} for unknown account (customer ${polarCustomerId})`)
            break
          }
          await deps.downgradeToFree(userId)
          console.log(`[billing] ${userId} → free (${type})`)
          break
        }

        case 'order.paid': {
          // Credit packs are one-off purchases, so they arrive as orders
          // rather than subscription events.
          const userId = await resolveUserId()
          const orderId = data.id as string | undefined
          if (!userId || !productId || !orderId) break

          const { granted, credits } = await deps.applyCreditPack(userId, productId, orderId)
          if (granted) console.log(`[billing] granted ${credits} credits to ${userId} (order ${orderId})`)
          break
        }

        default:
          // Everything else is acknowledged and ignored, so Polar stops
          // retrying events we have no opinion about.
          break
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
        description: 'Applies subscription and credit-pack changes. Authenticated by Polar signature.',
        tags: ['Billing'],
      },
    },
  )

  // ── Account-authenticated ─────────────────────────────────────────────
  const guardedRoutes = new Elysia({ prefix: '/billing' })
    .derive(derive)
    .onBeforeHandle(requireUser)

    .get(
      '/status',
      async ({ user }) => {
        const balance = await deps.getBalance(user!.id)
        const account = await deps.findUserById(user!.id)
        return {
          billingEnabled: billing.enabled,
          plan: getPlan(user!.plan),
          hasSubscription: Boolean(account?.polarCustomerId),
          balance,
        }
      },
      { detail: { summary: 'Current plan and balance', tags: ['Billing'] } },
    )

    .post(
      '/checkout',
      async ({ user, body, set }) => {
        if (!billing.enabled) {
          set.status = 404
          return { error: 'Billing is not enabled on this instance' }
        }

        // An enterprise plan is negotiated, not bought. Saying "unknown plan"
        // would read as a bug to someone who can see it on the pricing page.
        if (body.plan) {
          const plan = getPlan(body.plan)
          if (plan.contactOnly) {
            set.status = 409
            return {
              error: `The ${plan.name} plan is arranged directly — get in touch and we will set it up.`,
              contactOnly: true,
              plan: plan.id,
            }
          }
        }

        // A plan id is resolved to a product here rather than accepting a
        // product id from the client, so a caller cannot check out against an
        // arbitrary product in the organization.
        const productId = body.plan
          ? deps.productIdForPlan(body.plan)
          : body.creditPack && billing.creditPacks[body.creditPack]
            ? body.creditPack
            : null

        if (!productId) {
          set.status = 400
          return { error: 'Unknown plan or credit pack' }
        }

        const account = await deps.findUserById(user!.id)
        const url = await deps.createCheckout({
          userId: user!.id,
          email: account?.email ?? user!.email,
          productId,
          successUrl: `${consoleOrigin}/console/billing?checkout=success`,
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
          description: 'Pass a plan id to subscribe, or a credit-pack product id to buy credits.',
          tags: ['Billing'],
        },
      },
    )

    .get(
      '/portal',
      async ({ user, set }) => {
        if (!billing.enabled) {
          set.status = 404
          return { error: 'Billing is not enabled on this instance' }
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
          return { error: 'Billing is not enabled on this instance' }
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
