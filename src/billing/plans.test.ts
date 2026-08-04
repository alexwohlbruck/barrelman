/**
 * Tests for credit pricing, plan lookup and path→group classification.
 *
 * `groupForPath` decides what every request costs, so a path falling into the
 * wrong group is a billing error. The overlapping-prefix cases are the ones
 * that actually bite.
 */
import { describe, test, expect } from 'bun:test'
import {
  ALL_SCOPES,
  CREDIT_COSTS,
  creditCost,
  DEFAULT_PLAN,
  getPlan,
  groupForPath,
  isValidScope,
  listPlans,
  PLANS,
  planForProductId,
  scopeAllows,
  type EndpointGroup,
} from './plans'

describe('credit costs', () => {
  test('every endpoint group has a positive integer cost', () => {
    for (const [group, cost] of Object.entries(CREDIT_COSTS)) {
      expect(Number.isInteger(cost)).toBe(true)
      expect(cost).toBeGreaterThan(0)
      expect(creditCost(group as EndpointGroup)).toBe(cost)
    }
  })

  test('prices expensive engines above cheap reads', () => {
    // A tile is one indexed read; an isochrone fans out to hundreds of routing
    // calls. If this ordering ever inverts, the pricing is wrong.
    expect(CREDIT_COSTS.tiles).toBeLessThan(CREDIT_COSTS.search)
    expect(CREDIT_COSTS.search).toBeLessThan(CREDIT_COSTS.routing)
    expect(CREDIT_COSTS.routing).toBeLessThan(CREDIT_COSTS.isochrone)
    expect(CREDIT_COSTS.routing).toBeLessThan(CREDIT_COSTS.transit)
  })
})

describe('groupForPath', () => {
  test.each([
    ['/search', 'search'],
    ['/autocomplete?q=x', 'search'],
    ['/place/node/123', 'places'],
    ['/brands?q=star', 'places'],
    ['/contains?lat=1&lng=2', 'spatial'],
    ['/children?id=node/1', 'spatial'],
    ['/geocode/reverse', 'geocode'],
    ['/tiles/pois/12/1/2', 'tiles'],
    ['/route?from=a&to=b', 'routing'],
    ['/graphhopper/route', 'routing'],
    ['/isochrone?lat=1', 'isochrone'],
    ['/transit/departures', 'transit'],
    ['/gbfs/stations', 'transit'],
  ])('%s → %s', (path, expected) => {
    expect(groupForPath(path)).toBe(expected as EndpointGroup)
  })

  test('is case-insensitive', () => {
    expect(groupForPath('/Search')).toBe('search')
    expect(groupForPath('/TILES/x/1/2/3')).toBe('tiles')
  })

  test('leaves unmetered paths unclassified', () => {
    for (const path of ['/health', '/docs', '/console/keys', '/auth/session', '/account/keys', '/admin/jobs']) {
      expect(groupForPath(path)).toBeNull()
    }
  })

  test('classifies /route without capturing /routes-like siblings into transit', () => {
    // `/route` and `/transit` both plausibly match a naive rule; ordering in
    // the classifier is what keeps them apart.
    expect(groupForPath('/route')).toBe('routing')
    expect(groupForPath('/transit/route')).toBe('transit')
  })
})

describe('getPlan', () => {
  test('returns the named plan', () => {
    expect(getPlan('developer').id).toBe('developer')
    expect(getPlan('scale').id).toBe('scale')
  })

  test('falls back to free for unknown, null and undefined', () => {
    expect(getPlan('nonsense').id).toBe(DEFAULT_PLAN)
    expect(getPlan(null).id).toBe(DEFAULT_PLAN)
    expect(getPlan(undefined).id).toBe(DEFAULT_PLAN)
  })
})

describe('plan definitions', () => {
  test('free never incurs overage', () => {
    // Nobody should be able to run up a bill on a plan they did not pay for.
    expect(PLANS.free!.overageAllowed).toBe(false)
    expect(PLANS.free!.overageCentsPerThousand).toBe(0)
  })

  test('paid plans allow overage and price it', () => {
    for (const plan of listPlans().filter((p) => p.id !== 'free')) {
      expect(plan.overageAllowed).toBe(true)
      expect(plan.overageCentsPerThousand).toBeGreaterThan(0)
    }
  })

  test('allowances, rate limits and unit prices move monotonically with rank', () => {
    const ordered = listPlans()
    for (let i = 1; i < ordered.length; i += 1) {
      expect(ordered[i]!.monthlyCredits).toBeGreaterThan(ordered[i - 1]!.monthlyCredits)
      expect(ordered[i]!.requestsPerMinute).toBeGreaterThan(ordered[i - 1]!.requestsPerMinute)
    }
    // Buying a bigger plan should make each overage credit cheaper, not dearer.
    const paid = ordered.filter((p) => p.overageAllowed)
    for (let i = 1; i < paid.length; i += 1) {
      expect(paid[i]!.overageCentsPerThousand).toBeLessThanOrEqual(paid[i - 1]!.overageCentsPerThousand)
    }
  })

  test('listPlans is sorted by rank', () => {
    const ranks = listPlans().map((p) => p.rank)
    expect(ranks).toEqual([...ranks].sort((a, b) => a - b))
  })
})

describe('planForProductId', () => {
  test('returns null when no product ids are configured', () => {
    expect(planForProductId('prod_not_configured')).toBeNull()
  })

  test('maps a configured product id back to its plan', () => {
    const saved = process.env.POLAR_DEVELOPER_PRODUCT_ID
    process.env.POLAR_DEVELOPER_PRODUCT_ID = 'prod_dev_123'
    try {
      expect(planForProductId('prod_dev_123')?.id).toBe('developer')
      expect(planForProductId('prod_other')).toBeNull()
    } finally {
      if (saved === undefined) delete process.env.POLAR_DEVELOPER_PRODUCT_ID
      else process.env.POLAR_DEVELOPER_PRODUCT_ID = saved
    }
  })
})

describe('scopes', () => {
  test('a wildcard key may call every group', () => {
    for (const group of Object.keys(CREDIT_COSTS) as EndpointGroup[]) {
      expect(scopeAllows(['*'], group)).toBe(true)
    }
  })

  test('a narrow key may call only its groups', () => {
    expect(scopeAllows(['tiles', 'search'], 'tiles')).toBe(true)
    expect(scopeAllows(['tiles', 'search'], 'search')).toBe(true)
    // The point of a narrow key: a tile key embedded in a web map cannot be
    // used to run up an isochrone bill.
    expect(scopeAllows(['tiles', 'search'], 'isochrone')).toBe(false)
    expect(scopeAllows(['tiles'], 'routing')).toBe(false)
  })

  test('an empty scope list allows nothing', () => {
    expect(scopeAllows([], 'search')).toBe(false)
  })

  test('isValidScope accepts the wildcard and every group, and nothing else', () => {
    expect(isValidScope('*')).toBe(true)
    for (const group of Object.keys(CREDIT_COSTS)) expect(isValidScope(group)).toBe(true)
    expect(isValidScope('admin')).toBe(false)
    expect(isValidScope('')).toBe(false)
  })

  test('ALL_SCOPES covers the wildcard plus each group exactly once', () => {
    expect(new Set(ALL_SCOPES).size).toBe(ALL_SCOPES.length)
    expect(ALL_SCOPES).toContain('*')
    expect(ALL_SCOPES.length).toBe(Object.keys(CREDIT_COSTS).length + 1)
  })
})
