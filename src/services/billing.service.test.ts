/**
 * The requests barrelman sends to Polar.
 *
 * Polar is driven through a fake client, so these pin the request shapes that
 * matter for money: the external customer id that lets overage be billed at
 * all, the single subscription a plan change must reuse, and the revocation an
 * account deletion must not skip.
 */
import { describe, test, expect, mock, beforeEach, afterEach } from 'bun:test'
import {
  __setPolarClient,
  BillingTeardownError,
  changeSubscriptionProduct,
  createCheckout,
  creditsToClawBack,
  ensureCustomerExternalId,
  reportOverage,
  revokeActiveSubscription,
} from './billing.service'
import { billing } from '../config/billing.config'

const saved = { enabled: billing.enabled, meterUsage: billing.meterUsage }
const savedEnv = {
  dev: process.env.POLAR_DEVELOPER_PRODUCT_ID,
  business: process.env.POLAR_BUSINESS_PRODUCT_ID,
}

beforeEach(() => {
  Object.assign(billing, { enabled: true, meterUsage: true })
  process.env.POLAR_DEVELOPER_PRODUCT_ID = 'prod_dev'
  process.env.POLAR_BUSINESS_PRODUCT_ID = 'prod_business'
})

afterEach(() => {
  Object.assign(billing, saved)
  process.env.POLAR_DEVELOPER_PRODUCT_ID = savedEnv.dev
  process.env.POLAR_BUSINESS_PRODUCT_ID = savedEnv.business
  if (savedEnv.dev === undefined) delete process.env.POLAR_DEVELOPER_PRODUCT_ID
  if (savedEnv.business === undefined) delete process.env.POLAR_BUSINESS_PRODUCT_ID
  __setPolarClient(null)
})

function fakePolar(overrides: Record<string, Record<string, unknown>> = {}) {
  const fake = {
    checkouts: { create: mock(async () => ({ url: 'https://polar.sh/checkout/abc' })) },
    customers: {
      get: mock(async () => ({ id: 'cus_1', externalId: null })),
      update: mock(async () => ({ id: 'cus_1' })),
    },
    events: { ingest: mock(async () => ({ inserted: 1 })) },
    subscriptions: {
      get: mock(async () => ({ id: 'sub_1', status: 'active', productId: 'prod_dev' })),
      list: mock(async () => ({ result: { items: [] } })),
      update: mock(async () => ({ id: 'sub_1', status: 'active', productId: 'prod_business' })),
      revoke: mock(async () => ({ id: 'sub_1', status: 'canceled' })),
    },
  }
  for (const [group, methods] of Object.entries(overrides)) Object.assign((fake as never)[group], methods)
  __setPolarClient(fake as never)
  return fake
}

const account = {
  id: 'user-1',
  polarCustomerId: 'cus_1',
  subscriptionId: 'sub_1',
  subscriptionStatus: 'active',
}

describe('createCheckout', () => {
  test('names our user id as the external customer id', async () => {
    const polar = fakePolar()

    await createCheckout({
      userId: 'user-1',
      email: 'user@example.com',
      productId: 'prod_dev',
      successUrl: 'https://console.example/billing',
    })

    // reportOverage meters against this id; without it overage bills nobody.
    expect(polar.checkouts.create).toHaveBeenCalledWith({
      products: ['prod_dev'],
      metadata: { barrelmanUserId: 'user-1' },
      customerEmail: 'user@example.com',
      successUrl: 'https://console.example/billing',
      externalCustomerId: 'user-1',
    })
  })

  test('backfills the external id on an already linked customer first', async () => {
    const polar = fakePolar()

    await createCheckout({
      userId: 'user-1',
      email: 'user@example.com',
      productId: 'prod_dev',
      successUrl: 'https://console.example/billing',
      polarCustomerId: 'cus_1',
    })

    expect(polar.customers.update).toHaveBeenCalledWith({ id: 'cus_1', customerUpdate: { externalId: 'user-1' } })
  })
})

describe('reportOverage', () => {
  test('ingests one event per account, keyed by our user id as the external customer', async () => {
    const polar = fakePolar()

    const sent = await reportOverage([
      { userId: 'user-1', credits: 1200 },
      { userId: 'user-2', credits: 0 },
    ])

    expect(sent).toBe(1)
    const [request] = polar.events.ingest.mock.calls[0] as unknown as [{ events: Array<Record<string, unknown>> }]
    expect(request.events).toHaveLength(1)
    expect(request.events[0]).toMatchObject({
      name: billing.usageEventName,
      externalCustomerId: 'user-1',
      metadata: { credits: 1200 },
    })
  })
})

describe('ensureCustomerExternalId', () => {
  test('sets the external id when the customer has none', async () => {
    const polar = fakePolar()
    await ensureCustomerExternalId('cus_1', 'user-1', null)
    expect(polar.customers.update).toHaveBeenCalledWith({ id: 'cus_1', customerUpdate: { externalId: 'user-1' } })
  })

  test('leaves an external id that is already set', async () => {
    // Polar refuses to change one, and it may belong to another system.
    const polar = fakePolar()
    await ensureCustomerExternalId('cus_1', 'user-1', 'someone-else')
    await ensureCustomerExternalId('cus_1', 'user-1', 'user-1')
    expect(polar.customers.update).not.toHaveBeenCalled()
  })

  test('does nothing when billing is disabled', async () => {
    const polar = fakePolar()
    billing.enabled = false
    await ensureCustomerExternalId('cus_1', 'user-1', null)
    expect(polar.customers.get).not.toHaveBeenCalled()
    expect(polar.customers.update).not.toHaveBeenCalled()
  })
})

describe('changeSubscriptionProduct', () => {
  test('updates the existing subscription, invoicing an upgrade at once', async () => {
    const polar = fakePolar()

    const result = await changeSubscriptionProduct(account, 'prod_business')

    expect(result.kind).toBe('changed')
    expect(polar.subscriptions.update).toHaveBeenCalledWith({
      id: 'sub_1',
      subscriptionUpdate: { productId: 'prod_business', prorationBehavior: 'invoice' },
    })
    expect(polar.checkouts.create).not.toHaveBeenCalled()
  })

  test('prorates a downgrade onto the next invoice', async () => {
    const polar = fakePolar({
      subscriptions: { get: mock(async () => ({ id: 'sub_1', status: 'active', productId: 'prod_business' })) },
    })

    await changeSubscriptionProduct(account, 'prod_dev')

    expect(polar.subscriptions.update).toHaveBeenCalledWith({
      id: 'sub_1',
      subscriptionUpdate: { productId: 'prod_dev', prorationBehavior: 'prorate' },
    })
  })

  test('finds the live subscription on the customer when none is stored', async () => {
    // Accounts that subscribed before the subscription id was stored.
    const polar = fakePolar({
      subscriptions: {
        list: mock(async () => ({ result: { items: [{ id: 'sub_legacy', status: 'active', productId: 'prod_dev' }] } })),
      },
    })

    await changeSubscriptionProduct({ ...account, subscriptionId: null, subscriptionStatus: null }, 'prod_business')

    expect(polar.subscriptions.update).toHaveBeenCalledWith(expect.objectContaining({ id: 'sub_legacy' }))
  })

  test('reports no subscription so the caller can check out instead', async () => {
    fakePolar()
    const result = await changeSubscriptionProduct({ ...account, subscriptionId: null }, 'prod_business')
    expect(result.kind).toBe('no-subscription')
  })

  test('leaves a subscription already on the product alone', async () => {
    const polar = fakePolar()
    const result = await changeSubscriptionProduct(account, 'prod_dev')
    expect(result.kind).toBe('unchanged')
    expect(polar.subscriptions.update).not.toHaveBeenCalled()
  })
})

describe('revokeActiveSubscription', () => {
  test('revokes the live subscription', async () => {
    const polar = fakePolar()
    expect(await revokeActiveSubscription(account)).toBe('sub_1')
    expect(polar.subscriptions.revoke).toHaveBeenCalledWith({ id: 'sub_1' })
  })

  test('also revokes a past-due subscription', async () => {
    const polar = fakePolar({
      subscriptions: { get: mock(async () => ({ id: 'sub_1', status: 'past_due', productId: 'prod_dev' })) },
    })
    await revokeActiveSubscription({ ...account, subscriptionStatus: 'past_due' })
    expect(polar.subscriptions.revoke).toHaveBeenCalledWith({ id: 'sub_1' })
  })

  test('refuses with BillingTeardownError when Polar fails', async () => {
    // Carrying on would delete the account and leave the card being charged.
    fakePolar({
      subscriptions: {
        revoke: mock(async () => {
          throw new Error('polar down')
        }),
      },
    })
    await expect(revokeActiveSubscription(account)).rejects.toBeInstanceOf(BillingTeardownError)
  })

  test('refuses when Polar cannot say whether a subscription exists', async () => {
    fakePolar({
      subscriptions: {
        get: mock(async () => {
          throw new Error('timeout')
        }),
        list: mock(async () => {
          throw new Error('timeout')
        }),
      },
    })
    await expect(revokeActiveSubscription(account)).rejects.toBeInstanceOf(BillingTeardownError)
  })

  test('is a no-op with billing disabled', async () => {
    const polar = fakePolar()
    billing.enabled = false
    expect(await revokeActiveSubscription(account)).toBeNull()
    expect(polar.subscriptions.get).not.toHaveBeenCalled()
  })
})

describe('creditsToClawBack', () => {
  const order = { id: 'order_1', productId: 'prod_pack', status: 'refunded', netAmount: 1000, refundedAmount: 1000 }

  test('takes back the whole pack on a full refund', () => {
    expect(creditsToClawBack(100_000, order, 0)).toBe(100_000)
  })

  test('takes back the refunded share on a partial refund', () => {
    expect(creditsToClawBack(100_000, { ...order, status: 'partially_refunded', refundedAmount: 250 }, 0)).toBe(25_000)
  })

  test('takes only the difference on a second partial refund', () => {
    // refundedAmount is cumulative across refunds on the order.
    expect(creditsToClawBack(100_000, { ...order, status: 'partially_refunded', refundedAmount: 600 }, 25_000)).toBe(
      35_000,
    )
  })

  test('takes nothing more once the pack is fully taken back', () => {
    expect(creditsToClawBack(100_000, order, 100_000)).toBe(0)
  })
})
