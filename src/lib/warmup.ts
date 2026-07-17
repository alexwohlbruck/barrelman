/**
 * Keep MOTIS — and GBFS rental pricing — hot.
 *
 * MOTIS pays a one-off RAPTOR/timetable warm-up the first time it serves a
 * given query shape after the process starts (or after its working set is
 * paged out during an idle gap): measured at ~2–5s for the first `/plan`, then
 * <0.3s once warm. Because every real trip request carries a live `new Date()`
 * departure, that penalty otherwise lands on the first user after any quiet
 * period and surfaces as a 4–20s trip plan.
 *
 * This module fires representative intermodal queries on startup and on a
 * periodic timer so MOTIS's search structures (and the OS page cache backing
 * its multi-GB timetable) stay resident. It does NOT cache user results — it
 * warms the engine; every real request still runs a fresh search.
 *
 * Warm points are derived from the GTFS data actually loaded (one per feed,
 * busiest feeds first), so this is region-agnostic and needs no extra config:
 * a query anchored in each transit system exercises that system's timetable
 * load + RAPTOR init — the expensive part of a cold query, which runs even
 * when the search finds nothing nearby.
 */
import { db } from '../db'
import { sql } from 'drizzle-orm'
import {
  getIntermodalRoute,
  transitIdleMs,
  type IntermodalRouteRequest,
} from '../services/transit.service'
import { warmAllPricing } from '../services/rental-pricing.service'

/** Re-warm cadence. MOTIS's per-area search working set drifts cold within a
 *  couple of idle minutes (a fresh-time query climbs from ~0.6s back toward
 *  multiple seconds), so re-warm well inside that window. Each pass is a few
 *  seconds of background work — negligible duty cycle. */
const WARMUP_INTERVAL_MS = 90 * 1000

/** How many transit feeds to keep warm (busiest first). Caps total warm-up
 *  queries so a planet-scale deployment doesn't fire hundreds per cycle. */
const MAX_WARM_FEEDS = 12

interface WarmPoint {
  lat: number
  lng: number
}

let cachedPoints: WarmPoint[] | null = null

/** One representative coordinate per feed (its stop centroid), busiest feeds
 *  first. Cached after first load — feed geography doesn't change at runtime. */
async function loadWarmPoints(): Promise<WarmPoint[]> {
  if (cachedPoints) return cachedPoints
  try {
    const rows = (await db.execute(
      sql.raw(`
        SELECT AVG(stop_lat)::float AS lat, AVG(stop_lon)::float AS lng
        FROM gtfs_stops
        WHERE (location_type = 0 OR location_type IS NULL)
          AND stop_lat IS NOT NULL AND stop_lon IS NOT NULL
        GROUP BY feed_id
        ORDER BY COUNT(*) DESC
        LIMIT ${MAX_WARM_FEEDS}
      `),
    )) as any[]
    cachedPoints = rows
      .map((r) => ({ lat: Number(r.lat), lng: Number(r.lng) }))
      .filter((p) => Number.isFinite(p.lat) && Number.isFinite(p.lng))
    return cachedPoints
  } catch {
    return []
  }
}

async function warmPoint({ lat, lng }: WarmPoint): Promise<void> {
  const base: IntermodalRouteRequest = {
    from: { lat, lng },
    // ~1.3 km north-east — enough to involve transit access/egress.
    to: { lat: lat + 0.012, lng: lng + 0.012 },
    time: new Date().toISOString(),
    arriveBy: false,
    maxPreTransitTime: 600,
    maxPostTransitTime: 600,
    preTransitModes: ['WALK'],
    postTransitModes: ['WALK'],
  }
  // Warm both query shapes the multimodal planner fires concurrently per trip:
  // WALK-access transit, and direct-RENTAL shared mobility. MOTIS warms these
  // independently, so both are needed.
  await getIntermodalRoute({ ...base, numItineraries: 3, maxTransfers: 3 }).catch(
    () => {},
  )
  await getIntermodalRoute({
    ...base,
    numItineraries: 8,
    maxTransfers: 0,
    directModes: ['RENTAL'],
    maxDirectTime: 3600,
  }).catch(() => {})
}

async function warmOnce(): Promise<number> {
  const points = await loadWarmPoints()
  // Sequential, so a burst of warm-up queries never piles onto live traffic.
  for (const point of points) {
    await warmPoint(point).catch(() => {})
  }
  return points.length
}

/**
 * Start the keep-warm loop. Fire-and-forget — never throws into startup, and
 * the periodic timer is `unref`'d so it can't keep the process alive on its own.
 */
export function startTransitWarmup(): void {
  const t0 = Date.now()
  // Warm-up is best-effort: a warmOnce() rejection (e.g. MOTIS returns 404 for a
  // stale sample stop) must never surface as an unhandled rejection — that kills
  // the whole bun process and takes search/geocoding down with it. Always .catch.
  void warmOnce()
    .then((n) => {
      console.log(
        `MOTIS warmup: first pass warmed ${n} feed(s) in ${Date.now() - t0}ms`,
      )
    })
    .catch((e) => console.warn('MOTIS warmup (first pass) failed:', e?.message ?? e))
  // Pricing changes a few times a year (12h cache TTL), so warm it once.
  void warmAllPricing().catch(() => {})
  const timer = setInterval(() => {
    // Stand down while live traffic is already keeping MOTIS hot — warm-up is
    // only here to fill genuine idle gaps, not to compete with real requests.
    if (transitIdleMs() < WARMUP_INTERVAL_MS) return
    void warmOnce().catch((e) => console.warn('MOTIS warmup failed:', e?.message ?? e))
  }, WARMUP_INTERVAL_MS)
  timer.unref?.()
  console.log(
    `MOTIS warmup: scheduled (every ${WARMUP_INTERVAL_MS / 1000}s) to keep transit routing hot`,
  )
}
