/**
 * Map matching (GraphHopper)
 *
 * Snaps a GPS trace to the road network and returns the path it most likely
 * took, enriched the same way /route is (elevation, edge segments, path
 * details).
 *
 * GraphHopper's /match is a hidden-Markov matcher: for every pair of
 * consecutive points it routes between every candidate edge of one and every
 * candidate of the next. That is cheap when points are seconds apart and
 * ruinous when they are kilometres apart, so a raw sparse trace (photo geotags,
 * a GPS log with dropouts) can hold a GraphHopper thread for minutes. Its cost
 * also grows steeply with `gps_accuracy`, the candidate search radius, and with
 * the number of points in one call.
 *
 * So the trace is planned before anything is sent:
 *   - dense runs (consecutive points close in space and time) are matched with
 *     /match, in chunks of MATCH_CHUNK_POINTS so long runs match in parallel;
 *   - the gaps between them — and any isolated points — are routed with a
 *     multi-point /route, anchored on the matched geometry either side;
 *   - a chunk the matcher rejects ("Sequence is broken…") or does not finish in
 *     time is routed through its own points instead, so one bad stretch never
 *     fails the whole trace.
 */

import {
  DETAIL_KEYS,
  GraphHopperError,
  buildCumulativeDistances,
  buildEdgeSegments,
  computeElevationStats,
  haversine,
  type ElevationStats,
  type FetchFn,
  type RouteEdgeSegment,
} from './route.service'
import { graphHopperTimeoutMs, graphHopperUrl, isTimeoutError } from '../lib/graphhopper'

// ── Limits and defaults ─────────────────────────────────────────────

export const MAX_POINTS = 2000

/**
 * Candidate radius in metres. GraphHopper defaults to 40, which on bike/foot
 * profiles takes tens of seconds for a couple of kilometres of trace; 10 is
 * an order of magnitude faster and still tolerates ordinary phone GPS error.
 */
export const DEFAULT_GPS_ACCURACY = 10
export const MIN_GPS_ACCURACY = 1
export const MAX_GPS_ACCURACY = 50

/** Consecutive points farther apart than this are a gap, routed rather than matched. */
export const DEFAULT_MAX_GAP_METERS = 400
/** Consecutive points further apart in time than this are a gap too. */
export const DEFAULT_MAX_GAP_SECONDS = 60

/** A dense run shorter than this isn't worth a /match call — it is routed through. */
const MIN_MATCH_POINTS = 3
/** Points per /match call. Matching cost is superlinear per call, so long runs are split. */
const MATCH_CHUNK_POINTS = 20
/** Concurrent GraphHopper calls per request, so one trace can't monopolise the engine. */
const UPSTREAM_CONCURRENCY = 4
/**
 * Deadline for one /match call. Shorter than the general GraphHopper timeout:
 * a chunk that hasn't matched by now is routed instead, which takes well under
 * a second, so waiting longer only delays an answer that exists.
 */
const MATCH_CHUNK_TIMEOUT_MS = 10_000
/** Routed fallback for a failed chunk keeps points at least this far apart. */
const FALLBACK_MIN_SPACING_METERS = 30

/** Path details requested on every call. street_name is on top of what /route enriches. */
const MATCH_DETAIL_KEYS = [...DETAIL_KEYS, 'street_name']

// ── Types ───────────────────────────────────────────────────────────

export interface TracePoint {
  lat: number
  lon: number
  /** Epoch milliseconds. Optional — without times, gaps are judged on distance alone. */
  time?: number
}

export interface MatchRequest {
  profile: string
  points: TracePoint[]
  gpsAccuracy?: number
  /** Forwarded to GraphHopper /route as snap_preventions (e.g. bridge, motorway). */
  snapPrevention?: string[]
  maxGapMeters?: number
  maxGapSeconds?: number
}

/** One planned piece of a trace, by inclusive input-point index. Neighbours share their boundary point. */
export interface TracePiece {
  kind: 'match' | 'route'
  start: number
  end: number
}

export interface MatchSegment {
  type: 'matched' | 'routed'
  from_point: number
  to_point: number
  distance: number
  time: number
  /** Set when a dense run was routed because the matcher rejected it or ran out of time. */
  fallback?: 'match_failed' | 'match_timeout'
}

export interface SnappedPoint {
  input_index: number
  lat: number
  lon: number
  elevation?: number
  /** Metres between the input point and where it landed on the path. */
  offset: number
  /** Metres along the path from its start. */
  distance_along: number
}

export interface MatchResponse {
  paths: Array<{
    distance: number
    time: number
    ascend: number
    descend: number
    points: { type: 'LineString'; coordinates: number[][] }
    bbox: number[]
    details: Record<string, any[]>
    edge_segments: RouteEdgeSegment[]
    elevation_stats: ElevationStats
    snapped_points: SnappedPoint[]
    segments: MatchSegment[]
  }>
  map_matching: {
    input_points: number
    matched_points: number
    routed_points: number
    gps_accuracy: number
  }
}

export class MatchRequestError extends Error {
  constructor(message: string) {
    super(message)
    this.name = 'MatchRequestError'
  }
}

/** GraphHopper didn't answer within the deadline. */
export class GraphHopperTimeoutError extends Error {
  constructor() {
    super('GraphHopper did not respond in time')
    this.name = 'GraphHopperTimeoutError'
  }
}

// ── Input parsing ───────────────────────────────────────────────────

/**
 * A point's time as epoch milliseconds. Numbers below 1e11 are taken as epoch
 * seconds — that cut-off is the year 5138 in seconds and 1973 in milliseconds,
 * so no real timestamp is ambiguous.
 */
function parseTime(value: unknown): number | undefined {
  if (value == null || value === '') return undefined
  if (typeof value === 'number') return value < 1e11 ? value * 1000 : value
  if (typeof value === 'string') {
    const ms = Date.parse(value)
    if (Number.isNaN(ms)) throw new MatchRequestError(`Unparseable time "${value}"`)
    return ms
  }
  throw new MatchRequestError('time must be an ISO 8601 string or an epoch number')
}

function validPoint(lat: number, lon: number): boolean {
  return Number.isFinite(lat) && Number.isFinite(lon) && Math.abs(lat) <= 90 && Math.abs(lon) <= 180
}

/** JSON points: [{lat, lon, time?}]. Also accepts lng for lon. */
export function parseJsonPoints(raw: unknown): TracePoint[] {
  if (!Array.isArray(raw)) throw new MatchRequestError('points must be an array of {lat, lon, time?}')
  return raw.map((p, i) => {
    const lat = Number(p?.lat)
    const lon = Number(p?.lon ?? p?.lng)
    if (!validPoint(lat, lon)) throw new MatchRequestError(`points[${i}] has no valid lat/lon`)
    return { lat, lon, time: parseTime(p?.time) }
  })
}

const GPX_POINT = /<(trkpt|rtept)\b([^>]*?)(\/>|>([\s\S]*?)<\/\1>)/g

function gpxAttr(attrs: string, name: string): string | undefined {
  return new RegExp(`\\b${name}\\s*=\\s*["']([^"']*)["']`).exec(attrs)?.[1]
}

/**
 * Track points (or, failing those, route points) from a GPX document. A
 * regex rather than an XML parser: GPX points are flat and the API carries no
 * XML dependency, and nothing here needs namespaces or entities.
 */
export function parseGpx(xml: string): TracePoint[] {
  const trk: TracePoint[] = []
  const rte: TracePoint[] = []
  for (const m of xml.matchAll(GPX_POINT)) {
    const lat = Number(gpxAttr(m[2], 'lat'))
    const lon = Number(gpxAttr(m[2], 'lon'))
    if (!validPoint(lat, lon)) continue
    const time = /<time>\s*([^<]+?)\s*<\/time>/.exec(m[4] ?? '')?.[1]
    ;(m[1] === 'trkpt' ? trk : rte).push({ lat, lon, time: parseTime(time) })
  }
  return trk.length ? trk : rte
}

// ── Planning ────────────────────────────────────────────────────────

function isGap(a: TracePoint, b: TracePoint, maxGapMeters: number, maxGapSeconds: number): boolean {
  if (haversine(a.lat, a.lon, b.lat, b.lon) > maxGapMeters) return true
  if (a.time != null && b.time != null && Math.abs(b.time - a.time) > maxGapSeconds * 1000) return true
  return false
}

/** Split [start, end] into near-equal chunks of at most `size` points that share boundaries. */
function chunk(start: number, end: number, size: number): TracePiece[] {
  const steps = end - start
  const count = Math.ceil(steps / (size - 1))
  const pieces: TracePiece[] = []
  for (let c = 0; c < count; c++) {
    pieces.push({
      kind: 'match',
      start: start + Math.round((steps * c) / count),
      end: start + Math.round((steps * (c + 1)) / count),
    })
  }
  return pieces
}

/**
 * Decide which stretches of a trace to match and which to route. Pure, so the
 * dense/sparse split is testable without GraphHopper.
 *
 * Pieces cover the trace end to end in order, and each starts on the index
 * the previous one ended on.
 */
export function planTrace(
  points: TracePoint[],
  opts: { maxGapMeters?: number; maxGapSeconds?: number; chunkPoints?: number } = {},
): TracePiece[] {
  const maxGapMeters = opts.maxGapMeters ?? DEFAULT_MAX_GAP_METERS
  const maxGapSeconds = opts.maxGapSeconds ?? DEFAULT_MAX_GAP_SECONDS
  const chunkPoints = Math.max(MIN_MATCH_POINTS, opts.chunkPoints ?? MATCH_CHUNK_POINTS)
  const last = points.length - 1
  if (last < 1) return []

  // Maximal dense runs, by inclusive index.
  const runs: Array<[number, number]> = []
  let runStart = 0
  for (let i = 0; i < last; i++) {
    if (isGap(points[i], points[i + 1], maxGapMeters, maxGapSeconds)) {
      runs.push([runStart, i])
      runStart = i + 1
    }
  }
  runs.push([runStart, last])

  const pieces: TracePiece[] = []
  let cursor = 0
  for (const [a, b] of runs) {
    if (b - a + 1 < MIN_MATCH_POINTS) continue
    if (a > cursor) pieces.push({ kind: 'route', start: cursor, end: a })
    pieces.push(...chunk(a, b, chunkPoints))
    cursor = b
  }
  if (cursor < last) pieces.push({ kind: 'route', start: cursor, end: last })
  return pieces
}

// ── Upstream calls ──────────────────────────────────────────────────

interface PieceGeometry {
  coordinates: number[][]
  details: Record<string, any[]>
  distance: number
  time: number
  ascend: number
  descend: number
}

async function callGraphHopper(
  fetchFn: FetchFn,
  url: string,
  init: RequestInit,
  timeoutMs = graphHopperTimeoutMs(),
): Promise<PieceGeometry> {
  let res: Response
  try {
    res = await fetchFn(url, { ...init, signal: AbortSignal.timeout(timeoutMs) })
  } catch (err) {
    if (isTimeoutError(err)) throw new GraphHopperTimeoutError()
    throw err
  }
  if (!res.ok) throw new GraphHopperError(res.status, await res.text())

  const path = ((await res.json()) as any).paths?.[0]
  if (!path) throw new GraphHopperError(502, JSON.stringify({ message: 'GraphHopper returned no path' }))
  return {
    coordinates: path.points?.coordinates ?? [],
    details: path.details ?? {},
    distance: path.distance ?? 0,
    time: path.time ?? 0,
    ascend: path.ascend ?? 0,
    descend: path.descend ?? 0,
  }
}

function toGpx(points: TracePoint[]): string {
  const pts = points
    .map((p) => {
      const time = p.time != null ? `<time>${new Date(p.time).toISOString()}</time>` : ''
      return `<trkpt lat="${p.lat}" lon="${p.lon}">${time}</trkpt>`
    })
    .join('')
  return `<?xml version="1.0"?><gpx version="1.1" creator="barrelman"><trk><trkseg>${pts}</trkseg></trk></gpx>`
}

function matchPiece(fetchFn: FetchFn, profile: string, gpsAccuracy: number, points: TracePoint[]) {
  const qs = new URLSearchParams({
    profile,
    gps_accuracy: String(gpsAccuracy),
    points_encoded: 'false',
    elevation: 'true',
    instructions: 'false',
  })
  for (const key of MATCH_DETAIL_KEYS) qs.append('details', key)
  return callGraphHopper(
    fetchFn,
    `${graphHopperUrl()}/match?${qs}`,
    { method: 'POST', headers: { 'content-type': 'application/gpx+xml' }, body: toGpx(points) },
    Math.min(MATCH_CHUNK_TIMEOUT_MS, graphHopperTimeoutMs()),
  )
}

/** waypoints are [lon, lat]. */
function routePiece(fetchFn: FetchFn, profile: string, waypoints: number[][], snapPrevention?: string[]) {
  return callGraphHopper(fetchFn, `${graphHopperUrl()}/route`, {
    method: 'POST',
    headers: { 'content-type': 'application/json' },
    body: JSON.stringify({
      profile,
      points: waypoints,
      points_encoded: false,
      elevation: true,
      instructions: false,
      details: MATCH_DETAIL_KEYS,
      ...(snapPrevention?.length ? { snap_preventions: snapPrevention } : {}),
    }),
  })
}

/** Thin a dense run for routing: endpoints kept, interior points at least `spacing` apart. */
function thin(points: TracePoint[], spacing: number): TracePoint[] {
  const kept = [points[0]]
  for (let i = 1; i < points.length - 1; i++) {
    const prev = kept[kept.length - 1]
    if (haversine(prev.lat, prev.lon, points[i].lat, points[i].lon) >= spacing) kept.push(points[i])
  }
  kept.push(points[points.length - 1])
  return kept
}

async function mapLimit<T, R>(items: T[], limit: number, fn: (item: T, i: number) => Promise<R>): Promise<R[]> {
  const out = new Array<R>(items.length)
  let next = 0
  const worker = async () => {
    while (next < items.length) {
      const i = next++
      out[i] = await fn(items[i], i)
    }
  }
  await Promise.all(Array.from({ length: Math.min(limit, items.length) }, worker))
  return out
}

// ── Stitching ───────────────────────────────────────────────────────

/** Coordinates closer than this at a join are the same point, and the duplicate is dropped. */
const JOIN_EPSILON_METERS = 1

/** Append a piece's coordinates and details to the running path, re-indexing the details. */
function appendPiece(coords: number[][], details: Record<string, any[]>, piece: PieceGeometry) {
  if (!piece.coordinates.length) return
  const tail = coords[coords.length - 1]
  const head = piece.coordinates[0]
  const shared = tail != null && haversine(tail[1], tail[0], head[1], head[0]) < JOIN_EPSILON_METERS
  const offset = shared ? coords.length - 1 : coords.length
  coords.push(...(shared ? piece.coordinates.slice(1) : piece.coordinates))

  for (const [key, entries] of Object.entries(piece.details)) {
    const list = (details[key] ??= [])
    for (const [from, to, value] of entries) {
      const prev = list[list.length - 1]
      // Merge a run that continues across the join rather than splitting it.
      if (prev && prev[1] === from + offset && prev[2] === value) prev[1] = to + offset
      else list.push([from + offset, to + offset, value])
    }
  }
}

/**
 * Nearest point on the path for each input point. Points within a piece are
 * snapped onto that piece's stretch of the path, searching forward from the
 * previous point's segment, so a trace that doubles back along the same
 * street snaps to the right pass.
 */
function snapPoints(
  points: TracePoint[],
  coords: number[][],
  cumulative: number[],
  ranges: Array<{ start: number; end: number; coordStart: number; coordEnd: number }>,
): SnappedPoint[] {
  const snapped: SnappedPoint[] = []
  for (const range of ranges) {
    let seg = range.coordStart
    const first = snapped.length ? range.start + 1 : range.start
    for (let i = first; i <= range.end; i++) {
      const p = points[i]
      const cosLat = Math.cos((p.lat * Math.PI) / 180)
      let best = { dist: Infinity, seg, t: 0 }
      for (let s = seg; s < Math.max(range.coordEnd, range.coordStart + 1); s++) {
        const a = coords[s]
        const b = coords[s + 1] ?? a
        // Local equirectangular projection — plenty at GPS-error distances.
        const ax = a[0] * cosLat, ay = a[1]
        const bx = b[0] * cosLat, by = b[1]
        const px = p.lon * cosLat, py = p.lat
        const dx = bx - ax, dy = by - ay
        const len2 = dx * dx + dy * dy
        const t = len2 ? Math.max(0, Math.min(1, ((px - ax) * dx + (py - ay) * dy) / len2)) : 0
        const d = (px - ax - t * dx) ** 2 + (py - ay - t * dy) ** 2
        if (d < best.dist) best = { dist: d, seg: s, t }
      }
      seg = best.seg
      const a = coords[best.seg]
      const b = coords[best.seg + 1] ?? a
      const lon = a[0] + (b[0] - a[0]) * best.t
      const lat = a[1] + (b[1] - a[1]) * best.t
      const elevation = a[2] != null && b[2] != null ? a[2] + (b[2] - a[2]) * best.t : undefined
      const along = cumulative[best.seg] + (cumulative[best.seg + 1] - cumulative[best.seg] || 0) * best.t
      snapped.push({
        input_index: i,
        lat: round(lat, 6),
        lon: round(lon, 6),
        ...(elevation != null ? { elevation: round(elevation, 1) } : {}),
        offset: round(haversine(p.lat, p.lon, lat, lon), 1),
        distance_along: round(along, 1),
      })
    }
  }
  return snapped
}

function round(n: number, places: number): number {
  const f = 10 ** places
  return Math.round(n * f) / f
}

function bboxOf(coords: number[][]): number[] {
  let minLon = Infinity, minLat = Infinity, maxLon = -Infinity, maxLat = -Infinity
  for (const [lon, lat] of coords) {
    if (lon < minLon) minLon = lon
    if (lon > maxLon) maxLon = lon
    if (lat < minLat) minLat = lat
    if (lat > maxLat) maxLat = lat
  }
  return [minLon, minLat, maxLon, maxLat]
}

// ── Main service function ───────────────────────────────────────────

type ResolvedMatchRequest = MatchRequest & Required<Pick<MatchRequest, 'gpsAccuracy' | 'maxGapMeters' | 'maxGapSeconds'>>

export function validateMatchRequest(req: MatchRequest): ResolvedMatchRequest {
  if (!req.profile || typeof req.profile !== 'string') throw new MatchRequestError('profile is required')
  if (req.points.length < 2) throw new MatchRequestError('At least 2 points are required')
  if (req.points.length > MAX_POINTS) throw new MatchRequestError(`At most ${MAX_POINTS} points per request`)

  const gpsAccuracy = req.gpsAccuracy ?? DEFAULT_GPS_ACCURACY
  if (!Number.isFinite(gpsAccuracy) || gpsAccuracy < MIN_GPS_ACCURACY || gpsAccuracy > MAX_GPS_ACCURACY) {
    throw new MatchRequestError(`gps_accuracy must be between ${MIN_GPS_ACCURACY} and ${MAX_GPS_ACCURACY} metres`)
  }
  const maxGapMeters = req.maxGapMeters ?? DEFAULT_MAX_GAP_METERS
  if (!Number.isFinite(maxGapMeters) || maxGapMeters < 10 || maxGapMeters > 5000) {
    throw new MatchRequestError('max_gap_meters must be between 10 and 5000')
  }
  const maxGapSeconds = req.maxGapSeconds ?? DEFAULT_MAX_GAP_SECONDS
  if (!Number.isFinite(maxGapSeconds) || maxGapSeconds < 1 || maxGapSeconds > 86_400) {
    throw new MatchRequestError('max_gap_seconds must be between 1 and 86400')
  }
  return { ...req, gpsAccuracy, maxGapMeters, maxGapSeconds }
}

export async function matchTrace(
  request: MatchRequest,
  fetchFn: FetchFn = (url, init) => fetch(url, init),
): Promise<MatchResponse> {
  const req = validateMatchRequest(request)
  const { points, profile } = req
  const pieces = planTrace(points, req)
  // Past this, chunks not yet started are routed without trying the matcher,
  // so a long trace on a slow engine still answers in bounded time.
  const deadline = Date.now() + graphHopperTimeoutMs()

  // Phase 1: match the dense chunks. A chunk the matcher can't handle is
  // routed through its own (thinned) points so the trace still comes back
  // whole; any other upstream failure — the engine down, a bad profile —
  // fails the request.
  const results = new Array<(PieceGeometry & { fallback?: MatchSegment['fallback'] }) | undefined>(pieces.length)
  const matchIdx = pieces.flatMap((p, i) => (p.kind === 'match' ? [i] : []))
  await mapLimit(matchIdx, UPSTREAM_CONCURRENCY, async (i) => {
    const piece = pieces[i]
    const slice = points.slice(piece.start, piece.end + 1)
    try {
      if (Date.now() > deadline) throw new GraphHopperTimeoutError()
      results[i] = await matchPiece(fetchFn, profile, req.gpsAccuracy, slice)
    } catch (err) {
      const timedOut = err instanceof GraphHopperTimeoutError
      // GraphHopper answers an unmatchable trace with a 400 ("Sequence is
      // broken…"); a bad profile is a 400 too, but /route will reject it again
      // below and that error is what the caller sees.
      if (!timedOut && !(err instanceof GraphHopperError && err.statusCode === 400)) throw err
      const waypoints = thin(slice, FALLBACK_MIN_SPACING_METERS).map((p) => [p.lon, p.lat])
      results[i] = {
        ...(await routePiece(fetchFn, profile, waypoints, req.snapPrevention)),
        fallback: timedOut ? 'match_timeout' : 'match_failed',
      }
    }
  })

  // Phase 2: route the gaps, starting and ending on the matched geometry
  // either side so the path is continuous where the two meet.
  const routeIdx = pieces.flatMap((p, i) => (p.kind === 'route' ? [i] : []))
  await mapLimit(routeIdx, UPSTREAM_CONCURRENCY, async (i) => {
    const piece = pieces[i]
    const waypoints = points.slice(piece.start, piece.end + 1).map((p) => [p.lon, p.lat])
    const before = results[i - 1]?.coordinates
    const after = results[i + 1]?.coordinates
    if (before?.length) waypoints[0] = before[before.length - 1].slice(0, 2)
    if (after?.length) waypoints[waypoints.length - 1] = after[0].slice(0, 2)
    results[i] = await routePiece(fetchFn, profile, waypoints, req.snapPrevention)
  })

  // Stitch.
  const coords: number[][] = []
  const details: Record<string, any[]> = {}
  const ranges: Array<{ start: number; end: number; coordStart: number; coordEnd: number }> = []
  const segments: MatchSegment[] = []
  let distance = 0, ascend = 0, descend = 0, time = 0
  pieces.forEach((piece, i) => {
    const r = results[i]!
    const coordStart = Math.max(0, coords.length - 1)
    appendPiece(coords, details, r)
    ranges.push({ start: piece.start, end: piece.end, coordStart, coordEnd: coords.length - 1 })
    distance += r.distance
    ascend += r.ascend
    descend += r.descend
    time += r.time
    segments.push({
      type: piece.kind === 'match' && !r.fallback ? 'matched' : 'routed',
      from_point: piece.start,
      to_point: piece.end,
      distance: round(r.distance, 1),
      time: r.time,
      ...(r.fallback ? { fallback: r.fallback } : {}),
    })
  })

  // GraphHopper's own distances, summed, as /route reports them. Edge segments
  // and distance_along use the haversine cumulative, also as /route does.
  const cumulative = buildCumulativeDistances(coords)
  const matchedPoints = new Set<number>()
  for (const s of segments) {
    if (s.type !== 'matched') continue
    for (let p = s.from_point; p <= s.to_point; p++) matchedPoints.add(p)
  }

  return {
    paths: [{
      distance: round(distance, 1),
      time,
      ascend: round(ascend, 1),
      descend: round(descend, 1),
      points: { type: 'LineString', coordinates: coords },
      bbox: bboxOf(coords),
      details,
      edge_segments: buildEdgeSegments(details, cumulative),
      elevation_stats: computeElevationStats(coords),
      snapped_points: snapPoints(points, coords, cumulative, ranges),
      segments,
    }],
    map_matching: {
      input_points: points.length,
      matched_points: matchedPoints.size,
      routed_points: points.length - matchedPoints.size,
      gps_accuracy: req.gpsAccuracy,
    },
  }
}
