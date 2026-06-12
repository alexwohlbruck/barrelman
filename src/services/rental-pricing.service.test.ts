import { describe, test, expect, mock } from 'bun:test'

// One system with a pricing feed URL.
const mockExecute = mock(async () => [
  { system_id: 'test_sys', url: 'https://example.test/pricing.json' },
])
mock.module('../db', () => ({ db: { execute: mockExecute } }))

// Swappable fetch payload — each test sets `plansPayload`.
let plansPayload: any = { data: { plans: [] } }
globalThis.fetch = mock(async () => ({
  ok: true,
  status: 200,
  json: async () => plansPayload,
})) as any

const { ensurePricing, rateFor, estimateCost, pricingForLeg, __resetCachesForTests } =
  await import('./rental-pricing.service')

// Each test starts from a cold cache with its own system id.
let n = 0
async function loadPlans(plans: any[]): Promise<string> {
  __resetCachesForTests()
  const id = `test_sys_${n++}`
  mockExecute.mockImplementation(async () => [
    { system_id: id, url: 'https://example.test/pricing.json' },
  ])
  plansPayload = { data: { plans } }
  await ensurePricing([id])
  return id
}

describe('rental pricing', () => {
  test('coerces string prices and rates to numbers', async () => {
    // GBFS feeds emit `price` as a string ("4.99") in the wild.
    const id = await loadPlans([
      {
        plan_id: 'p1',
        name: 'EBIKE SINGLE RIDE',
        currency: 'USD',
        price: '4.99',
        per_min_pricing: [{ start: 0, rate: 0.41, interval: 1 }],
      },
    ])
    const rate = rateFor(id)!
    expect(rate.unlockPrice).toBe(4.99)
    expect(rate.perMinuteRate).toBe(0.41)
    expect(rate.currency).toBe('USD')
  })

  test('estimates unlock + per-minute over the ride duration', async () => {
    const id = await loadPlans([
      { plan_id: 'p1', price: 4.99, per_min_pricing: [{ start: 0, rate: 0.41, interval: 1 }] },
    ])
    // 1044s = 17.4 min → 4.99 + 17.4*0.41 = 12.124 → 12.12
    expect(estimateCost(rateFor(id)!, 1044, 0)).toBe(12.12)
  })

  test('applies tiered per-minute pricing across thresholds', async () => {
    const id = await loadPlans([
      {
        plan_id: 'p1',
        price: 1,
        // free first 5 min, then $0.30/min
        per_min_pricing: [
          { start: 0, rate: 0, interval: 1 },
          { start: 5, rate: 0.3, interval: 1 },
        ],
      },
    ])
    // 20 min: 1 unlock + 0*(5) + 0.30*(15) = 5.50
    expect(estimateCost(rateFor(id)!, 20 * 60, 0)).toBe(5.5)
  })

  test('adds per-km charges', async () => {
    const id = await loadPlans([
      {
        plan_id: 'p1',
        price: 1,
        per_km_pricing: [{ start: 0, rate: 0.5, interval: 1 }],
      },
    ])
    // 4 km → 1 + 0.5*4 = 3.00
    expect(estimateCost(rateFor(id)!, 0, 4000)).toBe(3)
  })

  test('prefers the metered plan over a flat membership', async () => {
    const id = await loadPlans([
      { plan_id: 'annual', name: 'Annual Member', price: 219.99 },
      { plan_id: 'single', name: 'Single Ride', price: 3.99, per_min_pricing: [{ start: 0, rate: 0.2, interval: 1 }] },
    ])
    expect(rateFor(id)!.unlockPrice).toBe(3.99)
  })

  test('pricingForLeg returns undefined for systems with no published fares', async () => {
    const id = await loadPlans([])
    expect(pricingForLeg(id, 600, 1000)).toBeUndefined()
  })
})
