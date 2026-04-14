/**
 * Enriched Route Service (GraphHopper)
 *
 * Calls GraphHopper /route with elevation and path details, returning a
 * unified enriched response with per-edge attributes (surface, road class,
 * bike network, smoothness, slope, speed, etc.) and elevation statistics.
 *
 * GraphHopper returns all of this in a single /route call — no need for a
 * separate trace_attributes call like Valhalla required.
 */

// ── Types ───────────────────────────────────────────────────────────

export interface RouteEdgeSegment {
  startDistance: number  // meters from leg start
  endDistance: number    // meters from leg start
  surface?: string
  roadClass?: string
  roadEnvironment?: string
  roadAccess?: string
  bikeNetwork?: string
  getOffBike?: boolean
  smoothness?: string
  trackType?: string
  averageSlope?: number
  maxSlope?: number
  averageSpeed?: number
}

export interface ElevationStats {
  totalGain: number
  totalLoss: number
  maxElevation: number
  minElevation: number
}

export interface EnrichedLeg {
  distance: number       // meters
  time: number           // milliseconds
  ascend: number         // meters
  descend: number        // meters
  points: Array<{ lat: number; lon: number; elevation?: number }>
  instructions: any[]
  edge_segments: RouteEdgeSegment[]
  elevation_stats: ElevationStats
  bbox: number[]
}

export interface EnrichedRouteResponse {
  paths: Array<{
    distance: number
    time: number
    ascend: number
    descend: number
    points: { type: string; coordinates: number[][] }
    instructions: any[]
    edge_segments: RouteEdgeSegment[]
    elevation_stats: ElevationStats
    bbox: number[]
    details: Record<string, any[]>
  }>
}

export type FetchFn = (url: string, init: RequestInit) => Promise<Response>

// ── Distance computation ────────────────────────────────────────────

const DEG_TO_RAD = Math.PI / 180
const EARTH_RADIUS = 6371000 // meters

/** Haversine distance between two points in meters */
function haversine(
  lat1: number, lon1: number,
  lat2: number, lon2: number,
): number {
  const dLat = (lat2 - lat1) * DEG_TO_RAD
  const dLon = (lon2 - lon1) * DEG_TO_RAD
  const a =
    Math.sin(dLat / 2) ** 2 +
    Math.cos(lat1 * DEG_TO_RAD) *
      Math.cos(lat2 * DEG_TO_RAD) *
      Math.sin(dLon / 2) ** 2
  return 2 * EARTH_RADIUS * Math.asin(Math.sqrt(a))
}

/** Build cumulative distance array for coordinate array */
function buildCumulativeDistances(
  coordinates: number[][],
): number[] {
  const distances = [0]
  for (let i = 1; i < coordinates.length; i++) {
    const [lon1, lat1] = coordinates[i - 1]
    const [lon2, lat2] = coordinates[i]
    const d = haversine(lat1, lon1, lat2, lon2)
    distances.push(distances[i - 1] + d)
  }
  return distances
}

// ── Elevation stats ─────────────────────────────────────────────────

function computeElevationStats(coordinates: number[][]): ElevationStats {
  let totalGain = 0
  let totalLoss = 0
  let maxElevation = -Infinity
  let minElevation = Infinity

  for (let i = 0; i < coordinates.length; i++) {
    const e = coordinates[i][2] // [lon, lat, elevation]
    if (e == null) continue

    if (e > maxElevation) maxElevation = e
    if (e < minElevation) minElevation = e

    if (i > 0 && coordinates[i - 1][2] != null) {
      const diff = e - coordinates[i - 1][2]
      if (diff > 0) totalGain += diff
      else totalLoss += Math.abs(diff)
    }
  }

  if (maxElevation === -Infinity) maxElevation = 0
  if (minElevation === Infinity) minElevation = 0

  return {
    totalGain: Math.round(totalGain * 10) / 10,
    totalLoss: Math.round(totalLoss * 10) / 10,
    maxElevation: Math.round(maxElevation * 10) / 10,
    minElevation: Math.round(minElevation * 10) / 10,
  }
}

// ── Path details → edge segments ────────────────────────────────────

/**
 * GraphHopper returns details as arrays of [fromIndex, toIndex, value].
 * Merge all detail types into unified edge segments keyed by distance ranges.
 */
function buildEdgeSegments(
  details: Record<string, any[]>,
  cumulativeDistances: number[],
): RouteEdgeSegment[] {
  // Build a map of unique segment boundaries from all detail types
  const breakpoints = new Set<number>()
  for (const key of Object.keys(details)) {
    for (const [from, to] of details[key]) {
      breakpoints.add(from)
      breakpoints.add(to)
    }
  }

  const sorted = Array.from(breakpoints).sort((a, b) => a - b)
  if (sorted.length < 2) return []

  // For each segment boundary, find the detail value that covers it
  const segments: RouteEdgeSegment[] = []

  for (let i = 0; i < sorted.length - 1; i++) {
    const fromIdx = sorted[i]
    const toIdx = sorted[i + 1]

    const startDistance = Math.round((cumulativeDistances[fromIdx] ?? 0) * 10) / 10
    const endDistance = Math.round((cumulativeDistances[toIdx] ?? startDistance) * 10) / 10

    if (endDistance - startDistance < 0.1) continue

    const seg: RouteEdgeSegment = { startDistance, endDistance }

    // Find each detail's value for this index range
    for (const [key, entries] of Object.entries(details)) {
      for (const [from, to, value] of entries as any[]) {
        if (from > fromIdx) break  // entries are sorted, no more can match
        if (from <= fromIdx && to >= toIdx) {
          switch (key) {
            case 'surface': seg.surface = value; break
            case 'road_class': seg.roadClass = value; break
            case 'road_environment': seg.roadEnvironment = value; break
            case 'road_access': seg.roadAccess = value; break
            case 'bike_network': seg.bikeNetwork = value; break
            case 'get_off_bike': seg.getOffBike = value; break
            case 'smoothness': seg.smoothness = value; break
            case 'track_type': seg.trackType = value; break
            case 'average_slope': seg.averageSlope = value; break
            case 'max_slope': seg.maxSlope = value; break
            case 'average_speed': seg.averageSpeed = value; break
          }
          break
        }
      }
    }

    segments.push(seg)
  }

  // Merge consecutive segments with identical attributes
  const merged: RouteEdgeSegment[] = []
  for (const seg of segments) {
    const prev = merged[merged.length - 1]
    if (prev &&
        prev.surface === seg.surface &&
        prev.roadClass === seg.roadClass &&
        prev.roadEnvironment === seg.roadEnvironment &&
        prev.bikeNetwork === seg.bikeNetwork &&
        prev.smoothness === seg.smoothness &&
        prev.averageSlope === seg.averageSlope &&
        prev.averageSpeed === seg.averageSpeed &&
        prev.getOffBike === seg.getOffBike) {
      prev.endDistance = seg.endDistance
    } else {
      merged.push({ ...seg })
    }
  }

  return merged
}

// ── Main service function ───────────────────────────────────────────

function getGraphHopperUrl(): string {
  return process.env.GRAPHHOPPER_URL || 'http://barrelman-graphhopper:8989'
}

/** All path details we request from GraphHopper for enriched responses */
const DETAIL_KEYS = [
  'surface',
  'road_class',
  'road_environment',
  'road_access',
  'bike_network',
  'get_off_bike',
  'smoothness',
  'track_type',
  'average_slope',
  'max_slope',
  'average_speed',
]

export async function getEnrichedRoute(
  requestBody: any,
  fetchFn: FetchFn = (url, init) => fetch(url, init),
): Promise<EnrichedRouteResponse> {
  const ghUrl = getGraphHopperUrl()

  // Build the GraphHopper route request with all enrichment details
  const routeRequest = {
    ...requestBody,
    elevation: true,
    points_encoded: false,
    instructions: requestBody.instructions ?? true,
    details: [...new Set([...(requestBody.details || []), ...DETAIL_KEYS])],
  }

  if (!routeRequest.points || !Array.isArray(routeRequest.points) || routeRequest.points.length < 2) {
    throw new GraphHopperError(400, 'Request must include at least 2 points')
  }

  const routeResponse = await fetchFn(`${ghUrl}/route`, {
    method: 'POST',
    headers: { 'content-type': 'application/json' },
    body: JSON.stringify(routeRequest),
  })

  if (!routeResponse.ok) {
    const errorBody = await routeResponse.text()
    throw new GraphHopperError(routeResponse.status, errorBody)
  }

  const routeData = await routeResponse.json() as any

  // Enrich each path with edge segments and elevation stats
  const enrichedPaths = (routeData.paths || []).map((path: any) => {
    const coordinates: number[][] = path.points?.coordinates || []
    const cumulativeDistances = buildCumulativeDistances(coordinates)
    const elevationStats = computeElevationStats(coordinates)
    const edgeSegments = buildEdgeSegments(path.details || {}, cumulativeDistances)

    return {
      ...path,
      edge_segments: edgeSegments,
      elevation_stats: elevationStats,
    }
  })

  return {
    paths: enrichedPaths,
  }
}

/** Custom error for GraphHopper upstream failures */
export class GraphHopperError extends Error {
  constructor(
    public readonly statusCode: number,
    public readonly body: string,
  ) {
    super(`GraphHopper returned ${statusCode}`)
    this.name = 'GraphHopperError'
  }
}
