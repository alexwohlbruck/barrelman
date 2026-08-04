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
    applyPlanFromProduct: mock(async () => 'developer'),
    applyCreditPack: mock(async () => ({ granted: true, credits: 100_000 })),
    downgradeToFree: mock(async () => undefined),
    productIdForPlan: mock((plan: string) => (plan === 'developer' ? 'prod_dev' : null)),
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
  const webhook = (type: string, data: Record<string, unknown>, d: Partial<BillingDeps>) => {
    const app = createBillingRoutes({ ...d, validateEvent: mock(() => ({ type, data })) as never })
    return app.handle(post('/billing/webhook', { any: 'payload' }))
  }

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

  test('upgrades the plan on subscription.active', async () => {
    const d = deps({ findUserByPolarCustomerId: mock(async () => makeUser()) })

    const res = await webhook('subscription.active', { customerId: 'cus_1', productId: 'prod_dev' }, d)

    expect(res.status).toBe(200)
    expect(d.applyPlanFromProduct).toHaveBeenCalledWith('user-1', 'prod_dev')
    expect(d.linkPolarCustomer).toHaveBeenCalledWith('user-1', 'cus_1')
  })

  test('falls back to checkout metadata when no customer is linked yet', async () => {
    // The first subscription for an account arrives before any link exists.
    const d = deps({
      findUserByPolarCustomerId: mock(async () => null),
      findUserById: mock(async () => makeUser({ id: 'user-42' })),
    })

    await webhook(
      'subscription.active',
      { customerId: 'cus_new', productId: 'prod_dev', metadata: { barrelmanUserId: 'user-42' } },
      d,
    )

    expect(d.applyPlanFromProduct).toHaveBeenCalledWith('user-42', 'prod_dev')
  })

  test('prefers the linked customer over conflicting metadata', async () => {
    // Metadata is whatever we set at checkout time; the link reflects who owns
    // the customer now. Trusting metadata would let a stale checkout move a
    // subscription onto the wrong account.
    const d = deps({ findUserByPolarCustomerId: mock(async () => makeUser({ id: 'real-owner' })) })

    await webhook(
      'subscription.active',
      { customerId: 'cus_1', productId: 'prod_dev', metadata: { barrelmanUserId: 'someone-else' } },
      d,
    )

    expect(d.applyPlanFromProduct).toHaveBeenCalledWith('real-owner', 'prod_dev')
  })

  test('ignores an event naming a metadata user that does not exist', async () => {
    const d = deps({
      findUserByPolarCustomerId: mock(async () => null),
      findUserById: mock(async () => null),
    })

    const res = await webhook(
      'subscription.active',
      { customerId: 'cus_x', productId: 'prod_dev', metadata: { barrelmanUserId: 'ghost' } },
      d,
    )

    expect(res.status).toBe(200)
    expect(d.applyPlanFromProduct).not.toHaveBeenCalled()
  })

  test.each([['subscription.canceled'], ['subscription.revoked']])('%s downgrades to free', async (type) => {
    const d = deps({ findUserByPolarCustomerId: mock(async () => makeUser({ plan: 'developer' })) })

    await webhook(type, { customerId: 'cus_1', productId: 'prod_dev' }, d)

    expect(d.downgradeToFree).toHaveBeenCalledWith('user-1')
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

  test('acknowledges event types it has no opinion about', async () => {
    const d = deps()

    const res = await webhook('customer.updated', { customerId: 'cus_1' }, d)

    // A non-200 would make Polar retry forever.
    expect(res.status).toBe(200)
    expect(d.applyPlanFromProduct).not.toHaveBeenCalled()
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
    ['GET', '/billing/portal'],
    ['POST', '/billing/sync'],
  ])('%s %s requires a session', async (method, path) => {
    const app = createBillingRoutes(deps())

    const res = await app.handle(
      new Request(`${BASE}${path}`, {
        method,
        headers: { 'content-type': 'application/json' },
        body: method === 'POST' ? JSON.stringify({}) : undefined,
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
    expect((await app.handle(new Request(`${BASE}/billing/portal`))).status).toBe(404)
    expect((await app.handle(post('/billing/sync', {}))).status).toBe(404)
  })
})
