/**
 * HTTP-layer tests for /billing.
 *
 * The webhook gets the most attention because it is the route that grants plans
 * and credits: it must reject an unsigned payload, must not be fooled into
 * attributing a subscription to the wrong account, and must be safe to replay —
 * Polar retries, and a retried credit pack must not be granted twice.
 */
import { describe, test, expect, mock, beforeEach, afterEach } from 'bun:test'
import { WebhookVerificationError } from '@polar-sh/sdk/webhooks'
import { createBillingRoutes, type BillingDeps } from './billing'
import { billing } from '../config/billing.config'

const BASE = 'http://localhost'

function makeUser(overrides: Record<string, unknown> = {}) {
  return {
    id: 'user-1',
    email: 'user@example.com',
    name: null,
    picture: null,
    role: 'user',
    plan: 'free',
    polarCustomerId: null,
    suspendedAt: null,
    createdAt: new Date('2026-01-01T00:00:00Z'),
    ...overrides,
  } as never
}

const balance = {
  plan: { id: 'free', name: 'Free' },
  monthlyCredits: 50_000,
  used: 31,
  purchased: 0,
  allowanceRemaining: 49_969,
  remaining: 49_969,
  overageAllowed: false,
  overage: 0,
  cycleResetsAt: '2026-09-01T00:00:00.000Z',
} as never

function deps(overrides: Partial<BillingDeps> = {}): Partial<BillingDeps> {
  return {
    getProducts: mock(async () => []),
    createCheckout: mock(async () => 'https://polar.sh/checkout/abc'),
    getPortalUrl: mock(async () => 'https://polar.sh/portal/abc'),
    syncSubscription: mock(async () => ({
      planId: 'developer',
      status: 'active',
      currentPeriodEnd: null,
      cancelAtPeriodEnd: false,
      amount: 1900,
      currency: 'usd',
    })),
    applySubscriptionEvent: mock(async () => ({ applied: true, planId: 'developer' })),
    refreshSubscription: mock(async () => ({ applied: true, planId: 'free' })),
    findActiveSubscription: mock(async () => null) as never,
    changeSubscriptionProduct: mock(async () => ({ kind: 'no-subscription' as const })) as never,
    ensureCustomerExternalId: mock(async () => undefined),
    applyCreditPack: mock(async () => ({ granted: true, credits: 100_000 })),
    clawBackCreditPack: mock(async () => ({ clawedBack: 100_000 })),
    claimWebhookEvent: mock(async () => true),
    releaseWebhookEvent: mock(async () => undefined),
    productIdForPlan: mock((plan: string) => (plan === 'developer' ? 'prod_dev' : plan === 'business' ? 'prod_business' : null)),
    findUserById: mock(async () => makeUser()),
    findUserByPolarCustomerId: mock(async () => null),
    linkPolarCustomer: mock(async () => undefined),
    getBalance: mock(async () => balance),
    resolveSession: mock(async () => ({ user: null, session: null })),
    validateEvent: mock(() => ({ type: 'subscription.active', data: {} })) as never,
    ...overrides,
  }
}

const signedIn = (user = makeUser()) => ({
  resolveSession: mock(async () => ({ user, session: { id: 'session-1' } as never })),
})

function post(path: string, body: unknown, headers: Record<string, string> = {}) {
  return new Request(`${BASE}${path}`, {
    method: 'POST',
    headers: { 'content-type': 'application/json', ...headers },
    body: typeof body === 'string' ? body : JSON.stringify(body),
  })
}

/**
 * The webhook and checkout routes are inert unless billing is configured, so
 * flip the flag for the duration of those tests rather than requiring a real
 * Polar token.
 */
const savedEnabled = billing.enabled

function withBillingEnabled(enabled: boolean) {
  ;(billing as { enabled: boolean }).enabled = enabled
}

beforeEach(() => withBillingEnabled(true))
afterEach(() => withBillingEnabled(savedEnabled))

describe('GET /billing/config', () => {
  test('is public and lists the plans on offer', async () => {
    const app = createBillingRoutes(deps())

    const res = await app.handle(new Request(`${BASE}/billing/config`))
    const body = await res.json()

    expect(res.status).toBe(200)
    expect(body.billingEnabled).toBe(true)
    expect(body.plans.map((p: { id: string }) => p.id)).toContain('developer')
  })
})

describe('POST /billing/webhook', () => {
  const webhook = (
    type: string,
    data: Record<string, unknown>,
    d: Partial<BillingDeps>,
    headers: Record<string, string> = {},
  ) => {
    const app = createBillingRoutes({ ...d, validateEvent: mock(() => ({ type, data })) as never })
    return app.handle(post('/billing/webhook', { any: 'payload' }, headers))
  }

  const sub = (overrides: Record<string, unknown> = {}) => ({
    id: 'sub_1',
    customerId: 'cus_1',
    productId: 'prod_dev',
    status: 'active',
    cancelAtPeriodEnd: false,
    currentPeriodEnd: new Date('2026-12-01T00:00:00Z'),
    modifiedAt: new Date('2026-11-01T00:00:00Z'),
    ...overrides,
  })

  test('rejects a payload whose signature does not verify', async () => {
    const app = createBillingRoutes(
      deps({
        validateEvent: mock(() => {
          throw new WebhookVerificationError('bad signature')
        }) as never,
      }),
    )

    const res = await app.handle(post('/billing/webhook', { forged: true }))

    // Anyone who guessed this URL could otherwise grant themselves a plan.
    expect(res.status).toBe(403)
  })

  test('400s a payload that verifies but cannot be parsed', async () => {
    const app = createBillingRoutes(
      deps({
        validateEvent: mock(() => {
          throw new Error('malformed')
        }) as never,
      }),
    )

    expect((await app.handle(post('/billing/webhook', {}))).status).toBe(400)
  })

  test('applies subscription.active through the lifecycle', async () => {
    const d = deps({ findUserByPolarCustomerId: mock(async () => makeUser()) })

    const res = await webhook('subscription.active', sub(), d)

    expect(res.status).toBe(200)
    expect(d.applySubscriptionEvent).toHaveBeenCalledWith(
      'user-1',
      expect.objectContaining({ id: 'sub_1', productId: 'prod_dev', status: 'active' }),
      { revoked: false },
    )
    expect(d.linkPolarCustomer).toHaveBeenCalledWith('user-1', 'cus_1')
  })

  test('marks subscription.revoked as a revocation', async () => {
    const d = deps({ findUserByPolarCustomerId: mock(async () => makeUser({ plan: 'developer' })) })

    await webhook('subscription.revoked', sub({ status: 'canceled' }), d)

    expect(d.applySubscriptionEvent).toHaveBeenCalledWith('user-1', expect.objectContaining({ id: 'sub_1' }), {
      revoked: true,
    })
  })

  test('does not treat a scheduled cancellation as a revocation', async () => {
    // Polar sends subscription.canceled as soon as the customer schedules it;
    // the plan has been paid for until the period end.
    const d = deps({ findUserByPolarCustomerId: mock(async () => makeUser({ plan: 'developer' })) })

    await webhook('subscription.canceled', sub({ cancelAtPeriodEnd: true }), d)

    expect(d.applySubscriptionEvent).toHaveBeenCalledWith(
      'user-1',
      expect.objectContaining({ cancelAtPeriodEnd: true }),
      { revoked: false },
    )
  })

  test('backfills the external id on a customer that lacks one', async () => {
    const d = deps({ findUserByPolarCustomerId: mock(async () => makeUser()) })

    await webhook('subscription.updated', sub({ customer: { externalId: null } }), d)

    expect(d.ensureCustomerExternalId).toHaveBeenCalledWith('cus_1', 'user-1', null)
  })

  test('skips a redelivery with a webhook-id it has already handled', async () => {
    const d = deps({
      findUserByPolarCustomerId: mock(async () => makeUser()),
      claimWebhookEvent: mock(async () => false),
    })

    const res = await webhook('subscription.updated', sub(), d, { 'webhook-id': 'msg_1' })

    expect(res.status).toBe(200)
    expect(d.claimWebhookEvent).toHaveBeenCalledWith('msg_1', 'subscription.updated')
    expect(d.applySubscriptionEvent).not.toHaveBeenCalled()
  })

  test('releases the delivery id and answers 500 when handling fails', async () => {
    // Otherwise Polar's retry would be skipped as a duplicate and the event lost.
    const d = deps({
      findUserByPolarCustomerId: mock(async () => makeUser()),
      applySubscriptionEvent: mock(async () => {
        throw new Error('database down')
      }),
    })

    const res = await webhook('subscription.updated', sub(), d, { 'webhook-id': 'msg_2' })

    expect(res.status).toBe(500)
    expect(d.releaseWebhookEvent).toHaveBeenCalledWith('msg_2')
  })

  test('falls back to checkout metadata when no customer is linked yet', async () => {
    // The first subscription for an account arrives before any link exists.
    const d = deps({
      findUserByPolarCustomerId: mock(async () => null),
      findUserById: mock(async () => makeUser({ id: 'user-42' })),
    })

    await webhook('subscription.active', sub({ customerId: 'cus_new', metadata: { barrelmanUserId: 'user-42' } }), d)

    expect(d.applySubscriptionEvent).toHaveBeenCalledWith('user-42', expect.anything(), expect.anything())
  })

  test('prefers the linked customer over conflicting metadata', async () => {
    // Metadata is whatever we set at checkout time; the link reflects who owns
    // the customer now. Trusting metadata would let a stale checkout move a
    // subscription onto the wrong account.
    const d = deps({ findUserByPolarCustomerId: mock(async () => makeUser({ id: 'real-owner' })) })

    await webhook('subscription.active', sub({ metadata: { barrelmanUserId: 'someone-else' } }), d)

    expect(d.applySubscriptionEvent).toHaveBeenCalledWith('real-owner', expect.anything(), expect.anything())
  })

  test('ignores an event naming a metadata user that does not exist', async () => {
    const d = deps({
      findUserByPolarCustomerId: mock(async () => null),
      findUserById: mock(async () => null),
    })

    const res = await webhook('subscription.active', sub({ customerId: 'cus_x', metadata: { barrelmanUserId: 'ghost' } }), d)

    expect(res.status).toBe(200)
    expect(d.applySubscriptionEvent).not.toHaveBeenCalled()
  })

  test('grants a credit pack on order.paid, keyed by order id', async () => {
    const d = deps({ findUserByPolarCustomerId: mock(async () => makeUser()) })

    await webhook('order.paid', { id: 'order_1', customerId: 'cus_1', productId: 'prod_pack' }, d)

    // The order id is what makes a retried webhook idempotent.
    expect(d.applyCreditPack).toHaveBeenCalledWith('user-1', 'prod_pack', 'order_1')
  })

  test('ignores an order with no id rather than granting an unkeyed pack', async () => {
    const d = deps({ findUserByPolarCustomerId: mock(async () => makeUser()) })

    await webhook('order.paid', { customerId: 'cus_1', productId: 'prod_pack' }, d)

    expect(d.applyCreditPack).not.toHaveBeenCalled()
  })

  test('takes back a refunded credit pack, keyed by order', async () => {
    const d = deps({ findUserByPolarCustomerId: mock(async () => makeUser()) })

    await webhook(
      'order.refunded',
      {
        id: 'order_1',
        customerId: 'cus_1',
        productId: 'prod_pack',
        subscriptionId: null,
        status: 'refunded',
        netAmount: 1000,
        refundedAmount: 1000,
      },
      d,
    )

    expect(d.clawBackCreditPack).toHaveBeenCalledWith('user-1', {
      id: 'order_1',
      productId: 'prod_pack',
      status: 'refunded',
      netAmount: 1000,
      refundedAmount: 1000,
    })
    expect(d.refreshSubscription).not.toHaveBeenCalled()
  })

  test('re-reads the subscription when a subscription payment is refunded', async () => {
    // Whether a refund ends the subscription is Polar's call; ask it.
    const d = deps({ findUserByPolarCustomerId: mock(async () => makeUser({ plan: 'developer' })) })

    await webhook(
      'order.refunded',
      { id: 'order_2', customerId: 'cus_1', productId: 'prod_dev', subscriptionId: 'sub_1', status: 'refunded' },
      d,
    )

    expect(d.refreshSubscription).toHaveBeenCalledWith('user-1', 'sub_1')
    expect(d.clawBackCreditPack).not.toHaveBeenCalled()
  })

  test('acknowledges event types it has no opinion about', async () => {
    const d = deps()

    const res = await webhook('customer.updated', { customerId: 'cus_1' }, d)

    // A non-200 would make Polar retry forever.
    expect(res.status).toBe(200)
    expect(d.applySubscriptionEvent).not.toHaveBeenCalled()
  })

  test('404s when billing is not configured on this instance', async () => {
    withBillingEnabled(false)

    const res = await createBillingRoutes(deps()).handle(post('/billing/webhook', {}))

    expect(res.status).toBe(404)
  })
})

describe('authenticated billing routes', () => {
  test.each([
    ['GET', '/billing/status'],
    ['POST', '/billing/checkout'],
    ['POST', '/billing/change-plan'],
    ['GET', '/billing/portal'],
    ['POST', '/billing/sync'],
  ])('%s %s requires a session', async (method, path) => {
    const app = createBillingRoutes(deps())

    const res = await app.handle(
      new Request(`${BASE}${path}`, {
        method,
        headers: { 'content-type': 'application/json' },
        // A body valid for every route, so the refusal tested is the session's.
        body: method === 'POST' ? JSON.stringify({ plan: 'developer' }) : undefined,
      }),
    )

    expect(res.status).toBe(401)
  })

  test('status reports the plan and balance', async () => {
    const app = createBillingRoutes(deps(signedIn()))

    const res = await app.handle(new Request(`${BASE}/billing/status`))
    const body = await res.json()

    expect(res.status).toBe(200)
    expect(body.plan.id).toBe('free')
    expect(body.balance.remaining).toBe(49_969)
    expect(body.hasSubscription).toBe(false)
    expect(body.subscription).toBeNull()
  })

  test('status reports a scheduled cancellation and its end date', async () => {
    const app = createBillingRoutes(
      deps({
        ...signedIn(makeUser({ plan: 'developer' })),
        findUserById: mock(async () =>
          makeUser({
            plan: 'developer',
            polarCustomerId: 'cus_1',
            subscriptionId: 'sub_1',
            subscriptionStatus: 'active',
            subscriptionCancelAtPeriodEnd: true,
            subscriptionPeriodEnd: new Date('2026-12-01T00:00:00Z'),
          }),
        ),
      }),
    )

    const body = await (await app.handle(new Request(`${BASE}/billing/status`))).json()

    expect(body.subscription).toEqual({
      status: 'active',
      cancelAtPeriodEnd: true,
      currentPeriodEnd: '2026-12-01T00:00:00.000Z',
    })
  })

  test('checkout refuses a second subscription for an account that has one', async () => {
    // A second checkout opens a second subscription, billed alongside the first.
    const d = deps({
      ...signedIn(),
      findActiveSubscription: mock(async () => ({ id: 'sub_1', productId: 'prod_dev' })) as never,
    })
    const app = createBillingRoutes(d)

    const res = await app.handle(post('/billing/checkout', { plan: 'business' }))

    expect(res.status).toBe(409)
    expect(d.createCheckout).not.toHaveBeenCalled()
  })

  test('change-plan updates the existing subscription rather than opening a checkout', async () => {
    const changed = { id: 'sub_1', productId: 'prod_business', status: 'active' }
    const d = deps({
      ...signedIn(),
      changeSubscriptionProduct: mock(async () => ({ kind: 'changed' as const, subscription: changed })) as never,
      applySubscriptionEvent: mock(async () => ({ applied: true, planId: 'business' })),
    })
    const app = createBillingRoutes(d)

    const res = await app.handle(post('/billing/change-plan', { plan: 'business' }))

    expect(res.status).toBe(200)
    expect(await res.json()).toEqual({ changed: true, planId: 'business' })
    expect(d.changeSubscriptionProduct).toHaveBeenCalledWith(expect.objectContaining({ id: 'user-1' }), 'prod_business')
    expect(d.applySubscriptionEvent).toHaveBeenCalledWith('user-1', changed)
    expect(d.createCheckout).not.toHaveBeenCalled()
  })

  test('change-plan falls back to a checkout when there is no subscription yet', async () => {
    const d = deps(signedIn())
    const app = createBillingRoutes(d)

    const res = await app.handle(post('/billing/change-plan', { plan: 'developer' }))
    const body = await res.json()

    expect(res.status).toBe(200)
    expect(body.checkoutUrl).toContain('polar.sh')
    expect(d.createCheckout).toHaveBeenCalledWith(expect.objectContaining({ productId: 'prod_dev' }))
  })

  test('change-plan keeps enterprise as contact-us', async () => {
    const d = deps(signedIn())
    const app = createBillingRoutes(d)

    const res = await app.handle(post('/billing/change-plan', { plan: 'enterprise' }))

    expect(res.status).toBe(409)
    expect((await res.json()).contactOnly).toBe(true)
    expect(d.changeSubscriptionProduct).not.toHaveBeenCalled()
  })

  test('change-plan reports a Polar failure as 502 without applying anything', async () => {
    const d = deps({
      ...signedIn(),
      changeSubscriptionProduct: mock(async () => {
        throw new Error('polar down')
      }),
    })
    const app = createBillingRoutes(d)

    const res = await app.handle(post('/billing/change-plan', { plan: 'business' }))

    expect(res.status).toBe(502)
    expect(d.applySubscriptionEvent).not.toHaveBeenCalled()
  })

  test('checkout resolves a plan id to a product server-side', async () => {
    const d = deps(signedIn())
    const app = createBillingRoutes(d)

    const res = await app.handle(post('/billing/checkout', { plan: 'developer' }))
    const body = await res.json()

    expect(res.status).toBe(200)
    expect(body.checkoutUrl).toContain('polar.sh')
    expect(d.createCheckout).toHaveBeenCalledWith(expect.objectContaining({ productId: 'prod_dev', userId: 'user-1' }))
  })

  test('checkout refuses an unknown plan', async () => {
    const d = deps(signedIn())
    const app = createBillingRoutes(d)

    const res = await app.handle(post('/billing/checkout', { plan: 'enterprise-unlimited' }))

    expect(res.status).toBe(400)
    expect(d.createCheckout).not.toHaveBeenCalled()
  })

  test('checkout refuses an arbitrary product id passed as a credit pack', async () => {
    // Accepting a raw product id would let a caller check out against any
    // product in the organization, including one priced at zero.
    const d = deps(signedIn())
    const app = createBillingRoutes(d)

    const res = await app.handle(post('/billing/checkout', { creditPack: 'prod_not_a_configured_pack' }))

    expect(res.status).toBe(400)
    expect(d.createCheckout).not.toHaveBeenCalled()
  })

  test('portal 404s before the account has a billing customer', async () => {
    const app = createBillingRoutes(deps(signedIn()))

    const res = await app.handle(new Request(`${BASE}/billing/portal`))

    expect(res.status).toBe(404)
  })

  test('portal returns a link once a customer exists', async () => {
    const app = createBillingRoutes(
      deps({
        ...signedIn(),
        findUserById: mock(async () => makeUser({ polarCustomerId: 'cus_1' })),
      }),
    )

    const res = await app.handle(new Request(`${BASE}/billing/portal`))

    expect(res.status).toBe(200)
    expect((await res.json()).portalUrl).toContain('polar.sh')
  })

  test('sync re-reads the provider and reports the resolved plan', async () => {
    const d = deps(signedIn())
    const app = createBillingRoutes(d)

    const res = await app.handle(post('/billing/sync', {}))

    expect(res.status).toBe(200)
    expect((await res.json()).planId).toBe('developer')
    expect(d.syncSubscription).toHaveBeenCalledWith('user-1', 'user@example.com')
  })

  test('checkout, portal and sync 404 when billing is disabled', async () => {
    withBillingEnabled(false)
    const app = createBillingRoutes(deps(signedIn()))

    expect((await app.handle(post('/billing/checkout', { plan: 'developer' }))).status).toBe(404)
    expect((await app.handle(post('/billing/change-plan', { plan: 'developer' }))).status).toBe(404)
    expect((await app.handle(new Request(`${BASE}/billing/portal`))).status).toBe(404)
    expect((await app.handle(post('/billing/sync', {}))).status).toBe(404)
  })
})
