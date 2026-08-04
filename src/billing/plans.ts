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
 *
 * The ratios are calibrated against the market rather than invented. Taking a
 * vector tile as 1, Mapbox prices geocoding at ~3× a tile and directions at ~8×;
 * Stadia Maps prices both geocoding and routing at 20×. Barrelman sits between
 * them, which is roughly where the real cost sits too: a geocode is more than a
 * tile read but nowhere near a routing solve.
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
  /** One indexed read from PostGIS via Martin. The unit everything else is priced against. */
  tiles: 1,
  /** Point lookups and brand/catalog reads. */
  places: 3,
  /** Containment and children queries — PostGIS spatial joins. */
  spatial: 3,
  /** Forward and reverse geocoding. */
  geocode: 5,
  /** Multi-layer search: full-text, trigram, abbreviation, semantic. */
  search: 6,
  /** Street routing through GraphHopper. */
  routing: 12,
  /** Transit routing through MOTIS, including realtime. */
  transit: 20,
  /** Reachability polygons — one request fans out to hundreds of routing calls. */
  isochrone: 40,
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
   * List price in cents per month. Polar is the source of truth once
   * configured; this is what the pricing page shows when it is not, so a
   * self-hosted or pre-launch instance still renders a sensible table.
   */
  priceCents: number
  /**
   * Whether requests continue past the included allowance, billed as metered
   * overage. Free accounts stop instead, so nobody can run up a bill they did
   * not agree to.
   */
  overageAllowed: boolean
  /**
   * Overage price in micro-dollars per credit — millionths of a dollar, as an
   * integer. Cents per thousand cannot express the rates the market actually
   * charges at the top of the ladder (1.2¢ per 1,000), and money in floats is
   * a bug waiting to happen.
   */
  overageMicrosPerCredit: number
  /**
   * Whether the licence permits commercial use. Free tiers at MapTiler and
   * Stadia are evaluation-only; ours matches, and the pricing table has to say
   * so where someone will actually read it.
   */
  commercialUse: boolean
  /** No self-serve checkout — the plan is negotiated. */
  contactOnly?: boolean
  /** Env var naming the Polar product for this plan; free and enterprise have none. */
  polarProductEnv?: string
  /**
   * Hard ceiling on overage credits per cycle, beyond which requests are
   * refused with a 402 rather than billed.
   *
   * Without one, a leaked key on a plan with overage accrues charges with no
   * upper bound — a Developer key driven at its own rate limit against
   * /isochrone is roughly $1/minute, and the burn-rate detector only runs on
   * the hourly sweep. This is the same "spend limit" control Mapbox and Google
   * expose; a customer who genuinely wants more raises it or moves up a plan.
   *
   * Expressed as a multiple of the included allowance so it scales with the
   * plan. 0 disables the cap.
   */
  overageCapMultiple: number
  /** Ordering for display, and for deciding which plan outranks which. */
  rank: number
}

/**
 * The ladder.
 *
 * Benchmarked against the providers a developer choosing barrelman would
 * otherwise pick. Stadia Maps is the closest comparison — OSM data, weighted
 * credits, the same hard-stop free tier — at $20 for 1M credits, $80 for 7.5M
 * and $250 for 25M, with overage from 3¢ down to 1.5¢ per 1,000. MapTiler is
 * $30 for 500k requests. Mapbox and Google are an order of magnitude dearer.
 *
 * Two properties are deliberate:
 *
 *   - **Overage stays close to the included rate.** Developer includes credits
 *     at 1.9¢ per 1,000 and charges 3¢ over. An earlier draft charged 20¢ —
 *     ten times the included rate — which punishes exactly the customer who is
 *     growing into the next plan.
 *   - **Every paid step is cheaper per credit than the one below it.** Buying a
 *     bigger plan should never raise anyone's unit price.
 */
export const PLANS: Record<string, Plan> = {
  free: {
    id: 'free',
    name: 'Free',
    description: 'Evaluation, prototypes and local development. No card required.',
    monthlyCredits: 100_000,
    requestsPerMinute: 300,
    priceCents: 0,
    // Hard stop rather than overage: a free account must never be able to
    // generate a bill nobody agreed to pay.
    overageAllowed: false,
    overageMicrosPerCredit: 0,
    commercialUse: false,
    // No overage at all, so nothing to cap.
    overageCapMultiple: 0,
    rank: 0,
  },
  developer: {
    id: 'developer',
    name: 'Developer',
    description: 'Production applications with moderate traffic. Commercial use included.',
    monthlyCredits: 1_000_000,
    requestsPerMinute: 900,
    priceCents: 1_900,
    overageAllowed: true,
    // 3¢ per 1,000 credits.
    overageMicrosPerCredit: 30,
    commercialUse: true,
    polarProductEnv: 'POLAR_DEVELOPER_PRODUCT_ID',
    overageCapMultiple: 3,
    rank: 1,
  },
  business: {
    id: 'business',
    name: 'Business',
    description: 'Growing products that need headroom and a lower unit price.',
    monthlyCredits: 10_000_000,
    requestsPerMinute: 1_800,
    priceCents: 9_900,
    overageAllowed: true,
    // 1.8¢ per 1,000 credits — 1.8x the included rate, matching where Stadia's
    // equivalent step lands.
    overageMicrosPerCredit: 18,
    commercialUse: true,
    polarProductEnv: 'POLAR_BUSINESS_PRODUCT_ID',
    overageCapMultiple: 3,
    rank: 2,
  },
  scale: {
    id: 'scale',
    name: 'Scale',
    description: 'High-volume applications with priority capacity.',
    monthlyCredits: 40_000_000,
    requestsPerMinute: 6_000,
    priceCents: 29_900,
    overageAllowed: true,
    // 1.2¢ per 1,000 credits.
    overageMicrosPerCredit: 12,
    commercialUse: true,
    polarProductEnv: 'POLAR_SCALE_PRODUCT_ID',
    overageCapMultiple: 2,
    rank: 3,
  },
  enterprise: {
    id: 'enterprise',
    name: 'Enterprise',
    description: 'Custom volume, an SLA, and a dedicated or self-hosted deployment.',
    // Nominal — an enterprise account is provisioned by an operator, not by
    // checkout, and its real allowance is whatever the contract says.
    monthlyCredits: 250_000_000,
    requestsPerMinute: 30_000,
    priceCents: 0,
    overageAllowed: true,
    overageMicrosPerCredit: 8,
    commercialUse: true,
    contactOnly: true,
    // Uncapped: the volume is contractual, not discovered at runtime.
    overageCapMultiple: 0,
    rank: 4,
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

/** Overage price in dollars per 1,000 credits, for display. */
export function overagePerThousand(plan: Plan): number {
  return (plan.overageMicrosPerCredit * 1_000) / 1_000_000
}

/**
 * Effective price of the included credits, in dollars per 1,000. Useful for
 * showing that a bigger plan really is cheaper per unit — and for asserting it
 * in tests, so a future pricing edit cannot quietly invert the ladder.
 */
export function includedPricePerThousand(plan: Plan): number {
  if (plan.monthlyCredits === 0) return 0
  return (plan.priceCents / 100 / plan.monthlyCredits) * 1_000
}

/** Plans a customer can buy without talking to anyone. */
export function purchasablePlans(): Plan[] {
  return listPlans().filter((plan) => plan.priceCents > 0 && !plan.contactOnly)
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
