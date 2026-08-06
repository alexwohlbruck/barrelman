/**
 * Isochrone Service
 *
 * Generates reachability polygons ("how far can I get in N minutes?") for
 * every travel mode Barrelman supports, at arbitrary caller-supplied
 * durations.
 *
 * Street modes (walk / bike / car) come straight from GraphHopper's
 * /isochrone endpoint — one polygon per requested duration. When the
 * requested durations happen to be evenly spaced from zero (300/600/900),
 * GraphHopper can produce all of them from a single graph search via its
 * `buckets` parameter, so we take that shortcut.
 *
 * Transit is composed here rather than delegated, the same way
 * transit.service.ts owns access/egress instead of leaning on MOTIS's street
 * router:
 *   1. MOTIS /api/v1/one-to-all gives every stop reachable within the budget
 *      and how long it takes to get there.
 *   2. Each reachable stop gets a GraphHopper walking isochrone sized to the
 *      *leftover* budget (capped by maxEgressTime) — that walk-off-the-stop
 *      area is what actually gives a transit isochrone its shape.
 *   3. Those polygons, plus a direct-walk polygon from the origin, are
 *      unioned in PostGIS.
 *
 * Reverse isochrones (`arriveBy`) invert every step: GraphHopper runs with
 * reverse_flow and MOTIS with arriveBy, so the result is the area that can
 * *reach* the point rather than the area reachable *from* it.
 */

import { db } from '../db'
import { sql } from 'drizzle-orm'
import { isochroneCache } from '../lib/cache'
import { markTransitActivity, MotisError } from './transit.service'
import { GraphHopperError } from './route.service'

// ── Types ───────────────────────────────────────────────────────────

export type IsochroneMode = 'walk' | 'bike' | 'car' | 'transit'

/** GraphHopper profile backing each street mode (see graphhopper-config.yml) */
const STREET_PROFILES: Record<Exclude<IsochroneMode, 'transit'>, string> = {
  walk: 'foot',
  bike: 'bike',
  car: 'car',
}

/** Aliases accepted for each canonical mode name */
const MODE_ALIASES: Record<string, IsochroneMode> = {
  walk: 'walk',
  walking: 'walk',
  foot: 'walk',
  pedestrian: 'walk',
  bike: 'bike',
  biking: 'bike',
  bicycle: 'bike',
  cycling: 'bike',
  car: 'car',
  drive: 'car',
  driving: 'car',
  auto: 'car',
  transit: 'transit',
  public_transport: 'transit',
  pt: 'transit',
}

export const ISOCHRONE_MODES: IsochroneMode[] = ['walk', 'bike', 'car', 'transit']

export interface IsochroneRequest {
  lat: number
  lng: number
  /** Travel mode. Default 'walk'. */
  mode?: IsochroneMode
  /** Contour durations in SECONDS. Default [900]. */
  durations?: number[]
  /** Reverse isochrone — the area that can reach the point. Default false. */
  arriveBy?: boolean
  /** ISO 8601 departure (or arrival, when arriveBy) time. Transit only. */
  time?: string
  /** Douglas–Peucker tolerance in meters. 0 (default) = no simplification. */
  simplify?: number
  /** Transit only: maximum number of transfers. */
  maxTransfers?: number
  /** Transit only: MOTIS transit modes (BUS, RAIL, SUBWAY, …). */
  transitModes?: string[]
  /**
   * Transit only: cap (seconds) on the walk away from each reachable stop.
   * Keeps a 60-minute transit isochrone from growing 40-minute walking blobs
   * around every stop it touches. Default 600.
   */
  maxEgressTime?: number
  /** Transit only: restrict to wheelchair-accessible stops and vehicles. */
  wheelchair?: boolean
  /**
   * Transit only: how many reachable stops may get their own GraphHopper walk
   * isochrone per contour. When more stops are reachable than that, the stop
   * set is thinned by coarsening the dedupe grid — evenly, so the contour's
   * frontier survives — rather than by dropping the tail. Default 250.
   */
  maxStopIsochrones?: number
}

export interface IsochroneFeature {
  type: 'Feature'
  properties: {
    mode: IsochroneMode
    durationSeconds: number
    durationMinutes: number
    /** 0 = innermost (shortest) contour, matching GraphHopper's convention. */
    bucket: number
    /** Transit only: reachable stops that shaped this contour. */
    stops?: number
  }
  geometry: any
}

export interface IsochroneResponse {
  mode: IsochroneMode
  origin: { lat: number; lng: number }
  arriveBy: boolean
  /** Resolved query time (transit only). */
  time?: string
  isochrones: {
    type: 'FeatureCollection'
    features: IsochroneFeature[]
  }
  meta: {
    durations: number[]
    /** ms spent in this request */
    computeMs: number
    /** Transit only: stops MOTIS reported reachable within the largest budget. */
    reachableStops?: number
    /** Transit only: GraphHopper walk isochrones actually computed. */
    stopIsochrones?: number
    /**
     * Transit only: resolution (meters) the stop set was thinned to in order
     * to fit maxStopIsochrones. Stays at ~55m until a dense network forces it
     * coarser, so it doubles as an accuracy readout.
     */
    stopGridMeters?: number
    /** Transit only: true when maxStopIsochrones still clipped the stop set. */
    truncated?: boolean
  }
}

export type FetchFn = (url: string, init?: RequestInit) => Promise<Response>

/** Injectable so unit tests don't need a live PostGIS. */
export type UnionFn = (
  polygons: any[],
  circles: Array<{ lat: number; lon: number; radiusM: number }>,
  simplifyMeters: number,
) => Promise<any>

// ── Limits ──────────────────────────────────────────────────────────

/** Longest street-mode contour we'll ask GraphHopper for (3h). */
export const MAX_STREET_DURATION = 10_800
/** Longest transit contour (2h) — MOTIS one-to-all cost grows with the window. */
export const MAX_TRANSIT_DURATION = 7_200
/** Most contours in a single request. */
export const MAX_CONTOURS = 8

/** Default cap on the walk away from a reachable transit stop. */
const DEFAULT_MAX_EGRESS = 600
/** Default cap on per-contour GraphHopper walk isochrones for transit. */
const DEFAULT_MAX_STOP_ISOCHRONES = 250
/**
 * Direct walking from the origin is a legitimate way to "travel by transit",
 * but a 2-hour foot isochrone is both slow and pointless — nobody walks that
 * far when the question is where transit takes them. Cap it at an hour.
 */
const DIRECT_WALK_CAP = 3_600
/** Below this leftover budget a stop gets a frontier circle, not a real walk. */
const MIN_EGRESS_SECONDS = 60
/** Radius of that frontier circle — enough to show the stop is reachable. */
const FRONTIER_RADIUS_M = 60
/** Grid cell (degrees, ~55m) used to collapse clustered stops into one call. */
const STOP_GRID_DEG = 0.0005
/** Coarsest the grid may get (~1.8km) while thinning stops to the call budget. */
const MAX_STOP_GRID_DEG = 0.016
/**
 * Parallel GraphHopper isochrone requests. A transit contour fans out to
 * hundreds of per-stop walk isochrones, so this is what sets the wall-clock —
 * but GraphHopper saturates its own thread pool well before we run out of
 * slots (measured on a 45-min NYC contour: 8 → 15.4s, 16 → 13.9s), and the
 * same engine serves interactive /route traffic. Stay polite by default and
 * let a dedicated deployment raise it via ISOCHRONE_CONCURRENCY.
 */
const GH_CONCURRENCY = Number(process.env.ISOCHRONE_CONCURRENCY) || 8

// ── Upstream URLs ───────────────────────────────────────────────────

function getGraphHopperUrl(): string {
  return process.env.GRAPHHOPPER_URL || 'http://barrelman-graphhopper:8989'
}

function getMotisUrl(): string {
  return process.env.MOTIS_URL || 'http://barrelman-motis:8080'
}

// ── Request validation ──────────────────────────────────────────────

export class IsochroneRequestError extends Error {
  constructor(message: string) {
    super(message)
    this.name = 'IsochroneRequestError'
  }
}

/** Resolve a caller-supplied mode string (or alias) to a canonical mode. */
export function parseMode(raw: string | undefined): IsochroneMode {
  if (!raw) return 'walk'
  const mode = MODE_ALIASES[raw.trim().toLowerCase()]
  if (!mode) {
    throw new IsochroneRequestError(
      `Unsupported mode "${raw}". Supported: ${ISOCHRONE_MODES.join(', ')}`,
    )
  }
  return mode
}

/** Parse a comma-separated seconds list ("300,600,900") into numbers. */
export function parseDurations(raw: string | undefined): number[] | undefined {
  if (raw == null || raw.trim() === '') return undefined
  const parts = raw.split(',').map((s) => s.trim()).filter(Boolean)
  const durations = parts.map((p) => {
    const n = Number(p)
    if (!Number.isFinite(n)) {
      throw new IsochroneRequestError(`Invalid duration "${p}" — expected seconds`)
    }
    return n
  })
  return durations
}

/** Validate + normalise durations: positive, deduped, ascending, within limits. */
function normaliseDurations(durations: number[] | undefined, mode: IsochroneMode): number[] {
  const list = durations?.length ? durations : [900]
  const max = mode === 'transit' ? MAX_TRANSIT_DURATION : MAX_STREET_DURATION

  for (const d of list) {
    if (!Number.isFinite(d) || d <= 0) {
      throw new IsochroneRequestError('Each duration must be a positive number of seconds')
    }
    if (d > max) {
      throw new IsochroneRequestError(
        `Duration ${d}s exceeds the ${max}s limit for mode "${mode}"`,
      )
    }
  }

  const unique = [...new Set(list.map((d) => Math.round(d)))].sort((a, b) => a - b)
  if (unique.length > MAX_CONTOURS) {
    throw new IsochroneRequestError(`At most ${MAX_CONTOURS} durations per request`)
  }
  return unique
}

function validateOrigin(lat: number, lng: number): void {
  if (!Number.isFinite(lat) || !Number.isFinite(lng)) {
    throw new IsochroneRequestError('lat and lng must be valid numbers')
  }
  if (lat < -90 || lat > 90 || lng < -180 || lng > 180) {
    throw new IsochroneRequestError('lat must be [-90,90], lng must be [-180,180]')
  }
}

// ── Concurrency helper ──────────────────────────────────────────────

async function mapWithConcurrency<T, R>(
  items: T[],
  limit: number,
  fn: (item: T, index: number) => Promise<R>,
): Promise<R[]> {
  const results = new Array<R>(items.length)
  let next = 0

  const workers = Array.from({ length: Math.min(limit, items.length) }, async () => {
    while (true) {
      const i = next++
      if (i >= items.length) return
      results[i] = await fn(items[i], i)
    }
  })

  await Promise.all(workers)
  return results
}

// ── GraphHopper isochrone ───────────────────────────────────────────

/**
 * True when `durations` are the evenly spaced multiples GraphHopper's
 * `buckets` parameter produces (n buckets over time_limit = n equal slices),
 * letting one graph search cover every contour.
 */
export function canUseBuckets(durations: number[]): boolean {
  if (durations.length < 2) return false
  const step = durations[0]
  return durations.every((d, i) => Math.abs(d - step * (i + 1)) < 1)
}

function isochroneCacheKey(
  profile: string, lat: number, lon: number, seconds: number, reverse: boolean,
): string {
  return `${profile}:${lat.toFixed(5)}:${lon.toFixed(5)}:${seconds}:${reverse ? 'r' : 'f'}`
}

/**
 * One GraphHopper isochrone polygon. Cached by (profile, rounded point,
 * seconds, direction) — the street graph only changes on OSM re-import, and
 * transit isochrones ask for the same stop/duration pairs over and over.
 */
async function fetchStreetIsochrone(
  profile: string,
  lat: number,
  lon: number,
  seconds: number,
  reverse: boolean,
  fetchFn: FetchFn,
): Promise<any> {
  const key = isochroneCacheKey(profile, lat, lon, seconds, reverse)
  const cached = isochroneCache.get(key)
  if (cached) return cached

  const geometries = await fetchStreetIsochroneBuckets(
    profile, lat, lon, [seconds], reverse, fetchFn,
  )
  const geometry = geometries[0]
  if (geometry) isochroneCache.set(key, geometry)
  return geometry
}

/**
 * Fetch one polygon per requested duration, using a single bucketed
 * GraphHopper search when the durations allow it.
 */
async function fetchStreetIsochroneBuckets(
  profile: string,
  lat: number,
  lon: number,
  durations: number[],
  reverse: boolean,
  fetchFn: FetchFn,
): Promise<any[]> {
  const ghUrl = getGraphHopperUrl()

  const request = async (timeLimit: number, buckets: number): Promise<any[]> => {
    const params = new URLSearchParams({
      point: `${lat},${lon}`,
      profile,
      time_limit: String(Math.round(timeLimit)),
      buckets: String(buckets),
      type: 'geojson',
    })
    if (reverse) params.set('reverse_flow', 'true')

    let response: Response
    try {
      response = await fetchFn(`${ghUrl}/isochrone?${params}`, {
        signal: AbortSignal.timeout(60_000),
      })
    } catch (err) {
      throw new GraphHopperError(502, `GraphHopper unreachable: ${String(err)}`)
    }

    if (!response.ok) {
      throw new GraphHopperError(response.status, await response.text())
    }

    const data = await response.json() as any
    const features = data.features || []
    // GraphHopper emits bucket 0 as the innermost contour; keep that order.
    return features
      .slice()
      .sort((a: any, b: any) => (a.properties?.bucket ?? 0) - (b.properties?.bucket ?? 0))
      .map((f: any) => f.geometry)
  }

  if (canUseBuckets(durations)) {
    const geometries = await request(durations[durations.length - 1], durations.length)
    if (geometries.length === durations.length) return geometries
    // Bucket count mismatch (GraphHopper collapsed empty contours) — fall
    // through to one request per duration rather than mislabelling polygons.
  }

  return mapWithConcurrency(durations, GH_CONCURRENCY, async (d) => {
    const geometries = await request(d, 1)
    return geometries[0]
  })
}

// ── PostGIS union ───────────────────────────────────────────────────

/**
 * Union GeoJSON polygons (plus optional frontier circles) into one geometry.
 *
 * PostGIS does the work — it's already the project's geometry engine, and
 * cascaded ST_Union over a few hundred detailed polygons is far cheaper than
 * anything we'd hand-roll in JS.
 */
export async function unionPolygons(
  polygons: any[],
  circles: Array<{ lat: number; lon: number; radiusM: number }>,
  simplifyMeters: number,
): Promise<any> {
  const usable = polygons.filter(Boolean)
  if (usable.length === 0 && circles.length === 0) return null

  // ST_Simplify works in the geometry's own units, so convert the metre
  // tolerance to degrees. Latitude scaling is close enough for smoothing.
  const toleranceDeg = simplifyMeters > 0 ? simplifyMeters / 111_320 : 0

  const rows = await db.execute(sql`
    WITH polys AS (
      SELECT ST_MakeValid(ST_GeomFromGeoJSON(value::text)) AS g
      FROM jsonb_array_elements(${JSON.stringify(usable)}::jsonb) AS value
    ),
    circles AS (
      SELECT ST_Buffer(
               ST_SetSRID(
                 ST_MakePoint((value->>'lon')::float8, (value->>'lat')::float8), 4326
               )::geography,
               (value->>'r')::float8
             )::geometry AS g
      FROM jsonb_array_elements(
        ${JSON.stringify(circles.map((c) => ({ lat: c.lat, lon: c.lon, r: c.radiusM })))}::jsonb
      ) AS value
    ),
    merged AS (
      SELECT ST_Union(g) AS g FROM (
        SELECT g FROM polys UNION ALL SELECT g FROM circles
      ) s
    )
    -- Clean up on the way out, in this order for a reason:
    --   SnapToGrid(1e-9) — ST_Union over hundreds of overlapping polygons
    --     leaves intersection vertices sub-nanodegree apart. That geometry is
    --     perfectly valid in the database, but ST_AsGeoJSON rounds to 9
    --     decimals, collapsing those vertices into rings of two distinct
    --     points; consumers then hit "Too few points in geometry component" on
    --     a polygon PostGIS swore was valid. Snapping to the same grid we
    --     serialize on moves that collapse *before* the repair step instead of
    --     after it, so what we validate is exactly what we emit.
    --   MakeValid(structure) — rebuilds the geometry rather than patching
    --     linework, dropping the zero-area rings the snap just created.
    --   CollectionExtract(…, 3) — repair can emit stray lines/points; callers
    --     get a Polygon or MultiPolygon, always.
    SELECT ST_AsGeoJSON(
      ST_CollectionExtract(
        ST_MakeValid(
          ST_SnapToGrid(
            CASE WHEN ${toleranceDeg}::float8 > 0
              THEN ST_SimplifyPreserveTopology(g, ${toleranceDeg}::float8)
              ELSE g
            END,
            0.000000001
          ),
          'method=structure'
        ),
        3
      ),
      9
    ) AS geojson
    FROM merged
  `)

  const row = (rows as any[])[0]
  if (!row?.geojson) return null
  return JSON.parse(row.geojson)
}

// ── Street-mode isochrones ──────────────────────────────────────────

async function streetIsochrone(
  request: IsochroneRequest,
  durations: number[],
  mode: Exclude<IsochroneMode, 'transit'>,
  fetchFn: FetchFn,
  union: UnionFn,
): Promise<IsochroneFeature[]> {
  const profile = STREET_PROFILES[mode]
  const geometries = await fetchStreetIsochroneBuckets(
    profile,
    request.lat,
    request.lng,
    durations,
    request.arriveBy ?? false,
    fetchFn,
  )

  const simplifyM = request.simplify ?? 0
  const simplified = simplifyM > 0
    ? await Promise.all(geometries.map((g) => (g ? union([g], [], simplifyM) : g)))
    : geometries

  return durations.map((seconds, i) => ({
    type: 'Feature' as const,
    properties: {
      mode,
      durationSeconds: seconds,
      durationMinutes: Math.round((seconds / 60) * 10) / 10,
      bucket: i,
    },
    geometry: simplified[i] ?? null,
  }))
}

// ── Transit isochrones ──────────────────────────────────────────────

interface ReachableStop {
  lat: number
  lon: number
  /** Seconds from origin (or to origin when arriveBy). */
  seconds: number
  stopId?: string
  name?: string
}

/** Ask MOTIS which stops are reachable within the largest requested budget. */
async function fetchReachableStops(
  request: IsochroneRequest,
  maxDuration: number,
  time: Date,
  fetchFn: FetchFn,
): Promise<ReachableStop[]> {
  const params = new URLSearchParams({
    one: `${request.lat},${request.lng}`,
    time: time.toISOString(),
    maxTravelTime: String(Math.ceil(maxDuration / 60)),
    arriveBy: String(request.arriveBy ?? false),
    // MOTIS's 25m default strands off-street platforms (see transit.service.ts)
    maxMatchingDistance: '250',
  })
  if (request.maxTransfers != null) params.set('maxTransfers', String(request.maxTransfers))
  if (request.transitModes?.length) params.set('transitModes', request.transitModes.join(','))
  if (request.wheelchair) params.set('wheelchair', 'true')

  const response = await fetchFn(`${getMotisUrl()}/api/v1/one-to-all?${params}`, {
    signal: AbortSignal.timeout(30_000),
  })

  if (!response.ok) {
    throw new MotisError(response.status, await response.text())
  }

  const data = await response.json() as any
  return (data.all || [])
    .filter((entry: any) => entry?.place && Number.isFinite(entry.duration))
    .map((entry: any) => ({
      lat: entry.place.lat,
      lon: entry.place.lon,
      // MOTIS reports one-to-all durations in whole minutes.
      seconds: entry.duration * 60,
      stopId: entry.place.stopId,
      name: entry.place.name,
    }))
}

/**
 * Collapse stops onto a grid, keeping the largest leftover budget in each cell.
 * Dense complexes (a subway station's dozen platform nodes, a bus stop on every
 * corner) otherwise burn one GraphHopper call each for polygons that land on
 * top of one another — and the survivor, having the most walking time left,
 * covers what its neighbours would have drawn.
 */
function dedupeStopsByCell(
  stops: Array<ReachableStop & { remaining: number }>,
  cellDeg: number,
): Array<ReachableStop & { remaining: number }> {
  const cells = new Map<string, ReachableStop & { remaining: number }>()
  for (const stop of stops) {
    const key = `${Math.round(stop.lat / cellDeg)}:${Math.round(stop.lon / cellDeg)}`
    const existing = cells.get(key)
    if (!existing || stop.remaining > existing.remaining) cells.set(key, stop)
  }
  return [...cells.values()]
}

/**
 * Reduce the stop set to the per-contour call budget by coarsening the grid
 * rather than truncating a sorted list. Truncation always sacrifices the same
 * stops — the far ones with the least walking time left — which are exactly
 * the ones defining the contour's outer edge. Coarsening thins evenly and
 * keeps the frontier.
 */
function reduceStopsToBudget(
  stops: Array<ReachableStop & { remaining: number }>,
  maxCells: number,
): { stops: Array<ReachableStop & { remaining: number }>; cellDeg: number } {
  let cellDeg = STOP_GRID_DEG
  let reduced = dedupeStopsByCell(stops, cellDeg)

  while (reduced.length > maxCells && cellDeg < MAX_STOP_GRID_DEG) {
    cellDeg *= 2
    reduced = dedupeStopsByCell(stops, cellDeg)
  }

  return { stops: reduced, cellDeg }
}

async function transitIsochrone(
  request: IsochroneRequest,
  durations: number[],
  time: Date,
  fetchFn: FetchFn,
  union: UnionFn,
): Promise<{
  features: IsochroneFeature[]
  reachableStops: number
  stopIsochrones: number
  truncated: boolean
  stopGridMeters: number
}> {
  const reverse = request.arriveBy ?? false
  const maxEgress = request.maxEgressTime ?? DEFAULT_MAX_EGRESS
  const maxStops = request.maxStopIsochrones ?? DEFAULT_MAX_STOP_ISOCHRONES
  const simplifyM = request.simplify ?? 0
  const maxDuration = durations[durations.length - 1]

  const stops = await fetchReachableStops(request, maxDuration, time, fetchFn)

  let stopIsochrones = 0
  let truncated = false
  let coarsestGridDeg = STOP_GRID_DEG
  let previous: any = null
  const features: IsochroneFeature[] = []

  // Contours run one at a time, smallest first, so the whole request keeps a
  // single GH_CONCURRENCY budget against GraphHopper however many were asked
  // for — and so each contour can absorb the one before it.
  for (const [bucket, seconds] of durations.entries()) {
    // Direct walking counts as reachable too, and it's what fills the hole
    // around the origin before the first stop is boarded.
    const directWalk = fetchStreetIsochrone(
      STREET_PROFILES.walk,
      request.lat,
      request.lng,
      Math.min(seconds, DIRECT_WALK_CAP),
      reverse,
      fetchFn,
    ).catch(() => null)

    const inBudget = stops
      .filter((s) => s.seconds <= seconds)
      .map((s) => ({ ...s, remaining: Math.min(seconds - s.seconds, maxEgress) }))

    const { stops: deduped, cellDeg } = reduceStopsToBudget(inBudget, maxStops)
    coarsestGridDeg = Math.max(coarsestGridDeg, cellDeg)

    // Walking out from a stop needs at least a minute of slack to be worth a
    // graph search; below that the stop still gets a frontier circle, which
    // keeps the contour's outer edge honest rather than silently dropping the
    // stops that define it.
    const walkable = deduped
      .filter((s) => s.remaining >= MIN_EGRESS_SECONDS)
      .sort((a, b) => b.remaining - a.remaining)
    const chosen = walkable.slice(0, maxStops)
    if (walkable.length > chosen.length) truncated = true

    const chosenSet = new Set(chosen)
    const frontier = deduped
      .filter((s) => !chosenSet.has(s))
      .map((s) => ({ lat: s.lat, lon: s.lon, radiusM: FRONTIER_RADIUS_M }))

    const stopPolygons = await mapWithConcurrency(chosen, GH_CONCURRENCY, async (stop) => {
      stopIsochrones++
      return fetchStreetIsochrone(
        STREET_PROFILES.walk,
        stop.lat,
        stop.lon,
        // Quantise to whole minutes so nearby stops share cache entries.
        Math.max(MIN_EGRESS_SECONDS, Math.round(stop.remaining / 60) * 60),
        reverse,
        fetchFn,
      ).catch(() => null)
    })

    const polygons = [await directWalk, ...stopPolygons].filter(Boolean)
    let geometry = await union(polygons, frontier, simplifyM)

    // Contours must nest: anything reachable in 30 minutes is reachable in 45.
    // Each contour picks its own representative stops (the dedupe grid
    // coarsens as more come into budget), so the larger one can miss a pocket
    // the smaller one drew — and independent simplification nudges shared
    // edges apart on top of that. Folding the finished previous contour in
    // *after* this one is simplified makes nesting exact rather than likely:
    // no further vertex movement can push the boundary back inside.
    if (previous) geometry = await union([geometry, previous], [], 0)
    previous = geometry

    features.push({
      type: 'Feature',
      properties: {
        mode: 'transit',
        durationSeconds: seconds,
        durationMinutes: Math.round((seconds / 60) * 10) / 10,
        bucket,
        stops: inBudget.length,
      },
      geometry,
    })
  }

  return {
    features,
    reachableStops: stops.length,
    stopIsochrones,
    truncated,
    stopGridMeters: Math.round(coarsestGridDeg * 111_320),
  }
}

// ── Entry point ─────────────────────────────────────────────────────

export async function getIsochrone(
  request: IsochroneRequest,
  deps: { fetchFn?: FetchFn; union?: UnionFn } = {},
): Promise<IsochroneResponse> {
  const fetchFn = deps.fetchFn || ((url, init) => fetch(url, init))
  const union = deps.union || unionPolygons

  validateOrigin(request.lat, request.lng)
  const mode = request.mode ?? 'walk'
  const durations = normaliseDurations(request.durations, mode)

  if (request.simplify != null && (!Number.isFinite(request.simplify) || request.simplify < 0)) {
    throw new IsochroneRequestError('simplify must be a non-negative distance in meters')
  }

  const startedAt = Date.now()

  if (mode !== 'transit') {
    const features = await streetIsochrone(request, durations, mode, fetchFn, union)
    return {
      mode,
      origin: { lat: request.lat, lng: request.lng },
      arriveBy: request.arriveBy ?? false,
      isochrones: { type: 'FeatureCollection', features },
      meta: { durations, computeMs: Date.now() - startedAt },
    }
  }

  // Real traffic keeps MOTIS hot; tell the warm-up loop to stand down.
  markTransitActivity()

  const time = request.time ? new Date(request.time) : new Date()
  if (Number.isNaN(time.getTime())) {
    throw new IsochroneRequestError(`Invalid time "${request.time}" — expected ISO 8601`)
  }

  const result = await transitIsochrone(request, durations, time, fetchFn, union)

  return {
    mode,
    origin: { lat: request.lat, lng: request.lng },
    arriveBy: request.arriveBy ?? false,
    time: time.toISOString(),
    isochrones: { type: 'FeatureCollection', features: result.features },
    meta: {
      durations,
      computeMs: Date.now() - startedAt,
      reachableStops: result.reachableStops,
      stopIsochrones: result.stopIsochrones,
      stopGridMeters: result.stopGridMeters,
      truncated: result.truncated,
    },
  }
}

export { GraphHopperError, MotisError }
