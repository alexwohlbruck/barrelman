// Street legs to and from transit, owned by barrelman rather than MOTIS's
// street graph: MOTIS cannot load a continent-scale street network, so every
// access mode is composed around its stop-to-stop search instead.

export type FetchFn = (url: string, init?: RequestInit) => Promise<Response>

export type AccessMode = 'WALK' | 'BIKE' | 'CAR' | 'CAR_PARKING'

export interface AccessProfile {
  /** Typical door-to-door speed including detours, m/s. */
  speedMps: number
  minRadius: number
  maxRadius: number
  /** Locking a bike, parking and walking from the car. */
  bufferSec: number
  /** GraphHopper profile; WALK stays an estimate the caller re-routes. */
  ghProfile: 'bike' | 'car' | null
  legMode: 'WALK' | 'BIKE' | 'CAR'
  /** Nobody drives to a bus stop: vehicle access only targets rail and ferry. */
  railOnly: boolean
}

export const ACCESS_PROFILES: Record<AccessMode, AccessProfile> = {
  WALK: { speedMps: 1.25, minRadius: 500, maxRadius: 2500, bufferSec: 0, ghProfile: null, legMode: 'WALK', railOnly: false },
  BIKE: { speedMps: 4.2, minRadius: 1500, maxRadius: 7000, bufferSec: 60, ghProfile: 'bike', legMode: 'BIKE', railOnly: false },
  CAR: { speedMps: 9, minRadius: 3000, maxRadius: 20000, bufferSec: 60, ghProfile: 'car', legMode: 'CAR', railOnly: true },
  CAR_PARKING: { speedMps: 9, minRadius: 3000, maxRadius: 20000, bufferSec: 300, ghProfile: 'car', legMode: 'CAR', railOnly: true },
}

/** Shared bikes: ride time plus the walks to and from the docks. */
export const RENTAL_PROFILE = {
  speedMps: 4.2,
  minRadius: 1500,
  maxRadius: 5000,
  dockWalkRadius: 350,
  unlockSec: 60,
}

export function accessRadius(profile: { speedMps: number; minRadius: number; maxRadius: number }, maxSec: number): number {
  return Math.min(profile.maxRadius, Math.max(profile.minRadius, maxSec * profile.speedMps))
}

export interface LatLng {
  lat: number
  lng: number
}

export interface StreetLeg {
  mode: 'WALK' | 'BIKE' | 'CAR' | 'RENTAL'
  from: LatLng & { name: string; stopId?: string }
  to: LatLng & { name: string; stopId?: string }
  duration: number
  distance: number
  geometry: { type: 'LineString'; coordinates: [number, number][] }
  rental?: {
    provider: string
    systemId: string
    stationId: string
    stationName: string
    toStationName: string
    formFactor: string
  }
}

export function haversineM(a: LatLng, b: LatLng): number {
  const R = 6371000
  const toRad = Math.PI / 180
  const dLat = (b.lat - a.lat) * toRad
  const dLng = (b.lng - a.lng) * toRad
  const s =
    Math.sin(dLat / 2) ** 2 + Math.cos(a.lat * toRad) * Math.cos(b.lat * toRad) * Math.sin(dLng / 2) ** 2
  return 2 * R * Math.asin(Math.sqrt(s))
}

/** Straight-line distance at the mode's door-to-door speed. */
export function estimateLeg(speedMps: number, from: LatLng, to: LatLng): Pick<StreetLeg, 'duration' | 'distance' | 'geometry'> {
  const distance = Math.round(haversineM(from, to))
  return {
    duration: Math.max(1, Math.round(distance / speedMps)),
    distance,
    geometry: { type: 'LineString', coordinates: [[from.lng, from.lat], [to.lng, to.lat]] },
  }
}

function graphhopperUrl(): string {
  return process.env.GRAPHHOPPER_URL || 'http://barrelman-graphhopper:8989'
}

/** A routed street leg, or the estimate when GraphHopper can't answer. */
export async function routeLeg(
  profile: 'bike' | 'car',
  speedMps: number,
  from: LatLng,
  to: LatLng,
  fetchFn: FetchFn,
): Promise<Pick<StreetLeg, 'duration' | 'distance' | 'geometry'>> {
  try {
    const response = await fetchFn(`${graphhopperUrl()}/route`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({
        profile,
        points: [[from.lng, from.lat], [to.lng, to.lat]],
        points_encoded: false,
        instructions: false,
      }),
      signal: AbortSignal.timeout(5_000),
    })
    if (!response.ok) throw new Error(`GraphHopper ${response.status}`)
    const path = ((await response.json()) as any)?.paths?.[0]
    const coordinates = path?.points?.coordinates
    if (!path || !Array.isArray(coordinates) || coordinates.length < 2) throw new Error('no path')
    return {
      duration: Math.max(1, Math.round(path.time / 1000)),
      distance: Math.round(path.distance),
      geometry: { type: 'LineString', coordinates: coordinates.map((c: number[]) => [c[0], c[1]]) },
    }
  } catch {
    return estimateLeg(speedMps / 1.3, from, to)
  }
}

/** Legs of a mode between two points: routed for vehicles, estimated on foot. */
export async function streetLeg(
  mode: AccessMode,
  from: StreetLeg['from'],
  to: StreetLeg['to'],
  fetchFn: FetchFn,
): Promise<StreetLeg> {
  const profile = ACCESS_PROFILES[mode]
  const shape = profile.ghProfile
    ? await routeLeg(profile.ghProfile, profile.speedMps, from, to, fetchFn)
    : estimateLeg(profile.speedMps, from, to)
  return { mode: profile.legMode, from, to, ...shape }
}

export function legsDuration(legs: StreetLeg[], bufferSec = 0): number {
  return legs.reduce((total, leg) => total + leg.duration, 0) + bufferSec
}
