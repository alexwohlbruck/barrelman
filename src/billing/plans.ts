/**
 * What each endpoint costs and what each plan includes.
 *
 * Usage is billed in credits rather than requests because barrelman's endpoints
 * are wildly unequal: a vector tile is a single indexed read, while an
 * isochrone fans out to hundreds of GraphHopper calls and a transit search
 * wakes MOTIS. Charging both as "one request" would either give tiles away at a
 * loss or price routing as if it were free.
 *
 * Costs are integers with the cheapest operation as the unit, so a balance is
 * always exact — no floating-point drift across millions of small charges.
 */

/**
 * Billing groups. Deliberately coarser than the route list: callers should be
 * able to reason about cost without memorising every path, and a new endpoint
 * in an existing group needs no pricing decision.
 */
export type EndpointGroup =
  | 'tiles'
  | 'places'
  | 'search'
  | 'geocode'
  | 'spatial'
  | 'routing'
  | 'isochrone'
  | 'transit'

export const CREDIT_COSTS: Record<EndpointGroup, number> = {
  /** A single indexed read from PostGIS via Martin — the unit. */
  tiles: 1,
  /** Point lookups and brand/catalog reads. */
  places: 2,
  /** Containment and children queries — PostGIS spatial joins. */
  spatial: 2,
  /** Forward and reverse geocoding. */
  geocode: 2,
  /** Multi-layer search: full-text, trigram, abbreviation, semantic. */
  search: 3,
  /** Street routing through GraphHopper. */
  routing: 10,
  /** Transit routing through MOTIS, including realtime. */
  transit: 25,
  /** Reachability polygons — fans out to hundreds of routing calls. */
  isochrone: 25,
}

export interface Plan {
  id: string
  name: string
  description: string
  /** Credits granted at the start of each calendar month (UTC). */
  monthlyCredits: number
  /** Ceiling on burst traffic, independent of the credit balance. */
  requestsPerMinute: number
  /**
   * Whether requests continue past the included allowance, billed as metered
   * overage. Free accounts stop instead, so nobody can run up a bill they did
   * not agree to.
   */
  overageAllowed: boolean
  /** Price in cents per 1,000 credits beyond the allowance. */
  overageCentsPerThousand: number
  /** Env var naming the Polar product for this plan; free has none. */
  polarProductEnv?: string
  /** Ordering for display, and for deciding which plan outranks which. */
  rank: number
}

export const PLANS: Record<string, Plan> = {
  free: {
    id: 'free',
    name: 'Free',
    description: 'For evaluation, side projects and local development.',
    monthlyCredits: 50_000,
    requestsPerMinute: 60,
    overageAllowed: false,
    overageCentsPerThousand: 0,
    rank: 0,
  },
  developer: {
    id: 'developer',
    name: 'Developer',
    description: 'For production applications with moderate traffic.',
    monthlyCredits: 1_000_000,
    requestsPerMinute: 600,
    overageAllowed: true,
    overageCentsPerThousand: 20,
    polarProductEnv: 'POLAR_DEVELOPER_PRODUCT_ID',
    rank: 1,
  },
  scale: {
    id: 'scale',
    name: 'Scale',
    description: 'For high-volume applications, with priority capacity.',
    monthlyCredits: 10_000_000,
    requestsPerMinute: 3_000,
    overageAllowed: true,
    overageCentsPerThousand: 10,
    polarProductEnv: 'POLAR_SCALE_PRODUCT_ID',
    rank: 2,
  },
}

export const DEFAULT_PLAN = 'free'

export function getPlan(id: string | null | undefined): Plan {
  return PLANS[id ?? ''] ?? PLANS[DEFAULT_PLAN]!
}

export function listPlans(): Plan[] {
  return Object.values(PLANS).sort((a, b) => a.rank - b.rank)
}

/** Polar product id configured for a plan, if any. */
export function planProductId(plan: Plan): string | null {
  if (!plan.polarProductEnv) return null
  return process.env[plan.polarProductEnv] || null
}

/** Reverse lookup: which plan a Polar product id corresponds to. */
export function planForProductId(productId: string): Plan | null {
  for (const plan of Object.values(PLANS)) {
    const configured = planProductId(plan)
    if (configured && configured === productId) return plan
  }
  return null
}

export function creditCost(group: EndpointGroup): number {
  return CREDIT_COSTS[group]
}

/**
 * The billing group a path belongs to, or null when the path is not metered
 * (health checks, the console, auth, docs).
 *
 * Ordering matters where prefixes overlap — `/transit/...` must be tested
 * before any broader rule that might also match it.
 */
export function groupForPath(pathname: string): EndpointGroup | null {
  const path = pathname.toLowerCase()

  if (path.startsWith('/tiles')) return 'tiles'
  if (path.startsWith('/search') || path.startsWith('/autocomplete')) return 'search'
  if (path.startsWith('/geocode')) return 'geocode'
  if (path.startsWith('/isochrone')) return 'isochrone'
  if (path.startsWith('/transit') || path.startsWith('/gbfs')) return 'transit'
  if (path.startsWith('/route') || path.startsWith('/graphhopper') || path.startsWith('/directions')) return 'routing'
  if (path.startsWith('/contains') || path.startsWith('/children')) return 'spatial'
  if (path.startsWith('/place') || path.startsWith('/brands')) return 'places'

  return null
}

/**
 * Scopes are the billing groups plus `*`. A key's scopes decide which groups it
 * may call, so a key embedded in a web map can be limited to tiles and search
 * and is then worthless for driving up a routing bill.
 */
export type Scope = EndpointGroup | '*'

export const ALL_SCOPES: Scope[] = ['*', ...(Object.keys(CREDIT_COSTS) as EndpointGroup[])]

export function scopeAllows(scopes: readonly string[], group: EndpointGroup): boolean {
  return scopes.includes('*') || scopes.includes(group)
}

export function isValidScope(scope: string): scope is Scope {
  return (ALL_SCOPES as string[]).includes(scope)
}
