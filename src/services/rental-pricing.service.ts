/**
 * GBFS rental pricing.
 *
 * Bike/scooter/car-share systems publish their fares in the standard GBFS
 * `system_pricing_plans.json` feed (89% of the systems we index advertise
 * one). This module fetches those plans on demand, normalizes them into a
 * simple rate card, and estimates the cost of a single ride — so trip pricing
 * is sourced automatically from the operator's own feed, with no hand-kept
 * metadata.
 *
 * Lookups are by GBFS `system_id`, which MOTIS reports verbatim on rental
 * legs (e.g. `lyft_nyc`), so a leg joins directly to its operator's pricing.
 */
import { db } from '../db'
import { sql } from 'drizzle-orm'

/** A `{start, rate, interval}` tier from per_min_pricing / per_km_pricing. */
interface PricingTier {
  /** Unit offset (minutes or km) at which this tier starts. */
  start: number
  /** Price charged per `interval` units within the tier. */
  rate: number
  /** Unit step the rate is charged over (minutes or km). */
  interval: number
}

/** Normalized single-ride rate card for one system. */
export interface RentalRate {
  currency: string
  /** Base/unlock fee. */
  unlockPrice: number
  /** First-tier per-minute rate, for display. */
  perMinuteRate: number
  /** First-tier per-km rate, for display. */
  perKmRate: number
  planName?: string
  /** Full per-minute tiers, for accurate estimation. */
  perMinuteTiers: PricingTier[]
  /** Full per-km tiers, for accurate estimation. */
  perKmTiers: PricingTier[]
}

/** Pricing attached to a rental leg: the rate card plus a ride estimate. */
export interface RentalPricing {
  currency: string
  unlockPrice: number
  perMinuteRate: number
  perKmRate: number
  planName?: string
  /** Estimated single-ride cost for this leg's duration/distance. */
  estimatedCost: number
}

// system_id → pricing-feed URL, loaded once from gbfs_systems.
let urlMap: Map<string, string> | null = null
let urlMapExpiry = 0
const URL_TTL_MS = 60 * 60 * 1000 // systems list changes rarely

// system_id → rate card (null = no plan / fetch failed), with per-entry TTL.
const rateCache = new Map<string, { rate: RentalRate | null; expiry: number }>()
const RATE_TTL_MS = 12 * 60 * 60 * 1000 // fares change a few times a year
const NEG_TTL_MS = 30 * 60 * 1000 // retry failures sooner

async function loadUrlMap(): Promise<Map<string, string>> {
  const now = Date.now()
  if (urlMap && now < urlMapExpiry) return urlMap
  const rows = (await db.execute(
    sql.raw(`
      SELECT system_id, feed_urls->>'system_pricing_plans' AS url
      FROM gbfs_systems
      WHERE enabled = TRUE AND feed_urls ? 'system_pricing_plans'
    `),
  )) as any[]
  const m = new Map<string, string>()
  for (const r of rows) if (r.url) m.set(r.system_id, r.url)
  urlMap = m
  urlMapExpiry = now + URL_TTL_MS
  return m
}

/** GBFS prices/rates may be JSON numbers or strings ("4.99") — coerce. */
function num(v: any, fallback = 0): number {
  const n = typeof v === 'string' ? parseFloat(v) : v
  return Number.isFinite(n) ? n : fallback
}

function toTiers(raw: any[]): PricingTier[] {
  return (raw ?? []).map((t) => ({
    start: num(t.start, 0),
    rate: num(t.rate, 0),
    interval: num(t.interval, 1) || 1,
  }))
}

/**
 * Pick a representative single-ride plan and flatten it to a rate card.
 *
 * Systems list anywhere from one plan (Citi Bike: one e-bike single ride) to
 * several (memberships, day passes, per-vehicle tiers). We want the casual
 * pay-as-you-go price, so prefer plans that actually meter the ride (per-min
 * or per-km tiers) and, among those, the lowest unlock fee.
 */
function normalize(plans: any[]): RentalRate | null {
  if (!plans?.length) return null
  const metered = plans.filter(
    (p) => p.per_min_pricing?.length || p.per_km_pricing?.length,
  )
  const pool = metered.length ? metered : plans
  pool.sort((a, b) => (a.price ?? 0) - (b.price ?? 0))
  const p = pool[0]
  const perMinuteTiers = toTiers(p.per_min_pricing)
  const perKmTiers = toTiers(p.per_km_pricing)
  return {
    currency: p.currency || 'USD',
    unlockPrice: num(p.price, 0),
    perMinuteRate: perMinuteTiers[0]?.rate ?? 0,
    perKmRate: perKmTiers[0]?.rate ?? 0,
    planName: p.name || undefined,
    perMinuteTiers,
    perKmTiers,
  }
}

/**
 * Warm the rate cache for the given systems. Fetches each uncached/expired
 * system's pricing feed in parallel; failures are negatively cached so one
 * bad feed doesn't stall every rental query.
 */
export async function ensurePricing(systemIds: string[]): Promise<void> {
  const now = Date.now()
  const need = [...new Set(systemIds)].filter((id) => {
    const c = rateCache.get(id)
    return !c || now >= c.expiry
  })
  if (!need.length) return

  const urls = await loadUrlMap()
  await Promise.all(
    need.map(async (id) => {
      const url = urls.get(id)
      if (!url) {
        rateCache.set(id, { rate: null, expiry: now + RATE_TTL_MS })
        return
      }
      try {
        const res = await fetch(url, { signal: AbortSignal.timeout(4000) })
        if (!res.ok) throw new Error(`HTTP ${res.status}`)
        const json = (await res.json()) as any
        const rate = normalize(json?.data?.plans ?? [])
        rateCache.set(id, {
          rate,
          expiry: now + (rate ? RATE_TTL_MS : NEG_TTL_MS),
        })
      } catch {
        rateCache.set(id, { rate: null, expiry: now + NEG_TTL_MS })
      }
    }),
  )
}

/**
 * Warm pricing for every enabled GBFS system that advertises a pricing feed,
 * so the first shared-mobility trip after a process start shows fares instead
 * of a blank price (the per-query fire-and-forget warm-up in transit.service
 * only covers systems that happen to appear in that query). Batched to avoid a
 * fetch stampede; intended to be called fire-and-forget on startup.
 */
export async function warmAllPricing(): Promise<void> {
  let urls: Map<string, string>
  try {
    urls = await loadUrlMap()
  } catch {
    return
  }
  const ids = [...urls.keys()]
  const BATCH = 25
  for (let i = 0; i < ids.length; i += BATCH) {
    await ensurePricing(ids.slice(i, i + BATCH)).catch(() => {})
  }
}

/** Synchronous rate-card lookup. Returns null until warmed by ensurePricing. */
export function rateFor(systemId?: string): RentalRate | null {
  if (!systemId) return null
  return rateCache.get(systemId)?.rate ?? null
}

/** Test-only: clear caches so each case starts from a cold state. */
export function __resetCachesForTests(): void {
  urlMap = null
  urlMapExpiry = 0
  rateCache.clear()
}

/** Tiered cost across `units` (minutes or km) for one pricing dimension. */
function tierCost(tiers: PricingTier[], units: number): number {
  if (!tiers.length || units <= 0) return 0
  const sorted = [...tiers].sort((a, b) => a.start - b.start)
  let total = 0
  for (let i = 0; i < sorted.length; i++) {
    const segStart = sorted[i].start
    const segEnd = i + 1 < sorted.length ? sorted[i + 1].start : Infinity
    if (units <= segStart) break
    const inSeg = Math.min(units, segEnd) - segStart
    total += (inSeg / (sorted[i].interval || 1)) * sorted[i].rate
  }
  return total
}

/** Estimated single-ride cost: unlock + metered time + metered distance. */
export function estimateCost(
  rate: RentalRate,
  durationSec: number,
  distanceM: number,
): number {
  const cost =
    rate.unlockPrice +
    tierCost(rate.perMinuteTiers, durationSec / 60) +
    tierCost(rate.perKmTiers, distanceM / 1000)
  return Math.round(cost * 100) / 100
}

/** Build the leg-level pricing object (rate card + ride estimate). */
export function pricingForLeg(
  systemId: string | undefined,
  durationSec: number,
  distanceM: number,
): RentalPricing | undefined {
  const rate = rateFor(systemId)
  if (!rate) return undefined
  return {
    currency: rate.currency,
    unlockPrice: rate.unlockPrice,
    perMinuteRate: rate.perMinuteRate,
    perKmRate: rate.perKmRate,
    planName: rate.planName,
    estimatedCost: estimateCost(rate, durationSec, distanceM),
  }
}
