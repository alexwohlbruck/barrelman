/**
 * Station guidance on transit plans (PAR-351/352/354/357): each transfer's
 * class and walk, the entrance and exit that make the whole trip shortest,
 * where to stand on the train, and the walked path with written steps where
 * the station graph has one. Everything comes from station_paths, built from
 * OSM; the agency's transfers and fares only say whether a change stays
 * inside fare control.
 */
import { sql } from 'drizzle-orm'
import { db } from '../db'
import {
  arrivalBearing, boardingHint, chooseEntrance, classifyTransfer, guidanceSteps, isWalked, positionIsMeasured,
  representativeRow, type BoardingHint, type GuidanceStep, type TransferClass, type WalkContext,
} from '../lib/station-graph/guidance'
import { DEFAULT_PARAMS } from '../lib/station-graph/params'
import { platformsForStop, STOP_MATCH_M, type StopCandidate } from '../lib/station-graph/stops'
import type { EndpointKind, PathSource, Profile, StationPath } from '../lib/station-graph/types'
import { entranceLabelsFor } from './station-labels.service'

/** One end of a walk inside a station. */
export interface StationEndpoint {
  id: string
  kind: EndpointKind
  name: string | null
  lat: number
  lng: number
  level: number | null
}

/**
 * A walk inside a station. `graph` and `bridged` walks carry the drawn line
 * (with a level per vertex) and written steps; an estimate is a time between
 * its two ends only, and should not be drawn or narrated.
 */
export interface StationWalk {
  source: PathSource
  estimate: boolean
  seconds: number
  uncertaintyS: number
  metres: number
  levels: number
  from: StationEndpoint
  to: StationEndpoint
  geometry?: { type: 'LineString'; coordinates: Array<[number, number]> }
  vertexLevels?: Array<number | null>
  steps?: GuidanceStep[]
}

/** The entrance a ride is boarded through, or the exit it is left by. */
export interface StationAccessChoice {
  id: string
  name: string | null
  ref: string | null
  /** "Broadway & West 28th Street · NW corner", where the streets around it say. */
  cornerLabel: string | null
  lat: number
  lng: number
  /** Street walk between the entrance and the trip's origin or destination, estimated from the straight line. */
  streetSeconds: number
  /** The walk inside the station, entrance to platform or platform to exit. */
  stationSeconds: number
  totalSeconds: number
  walk: StationWalk
}

/** How a rider gets from the previous ride to this one. */
export interface TransitTransfer {
  class: TransferClass
  /** Goes up to street level. Null when only an estimate is known. */
  surfaces: boolean | null
  /** For `out_of_system`: whether re-entering is free. Null when unknown or not applicable. */
  reentryFree: boolean | null
  /** Seconds for tapping out and back in, included in `seconds`. */
  fareGateDelayS: number
  /** The walk plus any fare gates; null when no station path joins the two platforms. */
  seconds: number | null
  walk: StationWalk | null
}

/** Where to stand on the platform so the train leaves the rider next to their exit or transfer. */
export interface BoardingPosition extends BoardingHint {
  toward: 'exit' | 'transfer'
  /** The exit's name or the line transferred to. */
  landmark: string | null
}

export interface GuidedLeg {
  transitLeg: boolean
  mode: string
  from: { lat: number; lng: number; stopId?: string }
  to: { lat: number; lng: number; stopId?: string }
  routeShortName?: string
  routeId?: string
  directionLabel?: string
  geometry?: { type: 'LineString'; coordinates: [number, number][] }
  entrance?: StationAccessChoice
  exit?: StationAccessChoice
  transfer?: TransitTransfer
  boardingPosition?: BoardingPosition
}

export interface GuidanceDeps {
  profile: Profile
  walkMps: number
  /** Lines reachable from a stop without leaving the paid area, as MOTIS route ids (`feed_route`); null unknown. */
  inSystemRoutes: (feedId: string, stopId: string) => Promise<Set<string> | null>
  /** Whether one fare covers both legs; null when there is no fare data. */
  oneFare: (a: GuidedLeg, b: GuidedLeg) => boolean | null
}

const splitId = (id: string | undefined) => {
  const i = id?.indexOf('_') ?? -1
  return id && i > 0 ? { feedId: id.slice(0, i), stopId: id.slice(i + 1) } : null
}

let tablesReady: boolean | null = null
async function ready(): Promise<boolean> {
  if (tablesReady) return true
  const [row] = (await db.execute(sql`SELECT to_regclass('station_paths') IS NOT NULL AND to_regclass('station_platforms') IS NOT NULL AS ok`)) as Array<{ ok: boolean }>
  tablesReady = row?.ok === true
  return tablesReady
}

interface PlatformInfo {
  id: string
  complexId: string
  name: string | null
  stationName: string | null
  level: number | null
  axisBearing: number
  lengthM: number
  refs: string[]
  lat: number
  lng: number
}

interface EntranceInfo {
  id: string
  name: string | null
  ref: string | null
  level: number | null
  lat: number
  lng: number
  cornerLabel: string | null
  entryAllowed: boolean | null
  exitAllowed: boolean | null
}

const values = (rows: Array<[string, string]>) => sql.join(rows.map(([a, b]) => sql`(${a}::text, ${b}::text)`), sql`, `)

/** The OSM platforms each (feed, stop) boards from, narrowed by the line ridden. */
async function resolveStops(stops: Array<{ feedId: string; stopId: string; route: string | undefined }>) {
  const out = new Map<string, string[]>()
  const unique = [...new Map(stops.map((s) => [`${s.feedId}\u0000${s.stopId}\u0000${s.route ?? ''}`, s])).values()]
  if (!unique.length) return out
  const rows = (await db.execute(sql`
    SELECT q.feed_id, q.stop_id, p.id, p.complex_id, p.refs, p.gtfs_stop_ids,
           ST_Distance(p.geom::geography, s.geom::geography) AS distance_m
    FROM (VALUES ${values([...new Set(unique.map((s) => `${s.feedId}\u0000${s.stopId}`))].map((k) => k.split('\u0000') as [string, string]))}) AS q(feed_id, stop_id)
    JOIN gtfs_stops s ON s.feed_id = q.feed_id AND s.stop_id = q.stop_id
    JOIN station_platforms p
      ON p.geom && ST_Expand(s.geom, ${STOP_MATCH_M}::float8 / 80000)
     AND ST_DWithin(p.geom::geography, s.geom::geography, ${STOP_MATCH_M})
  `)) as Array<{ feed_id: string; stop_id: string; id: string; complex_id: string; refs: string[]; gtfs_stop_ids: string[]; distance_m: number }>
  const candidates = new Map<string, StopCandidate[]>()
  for (const r of rows) {
    const k = `${r.feed_id}\u0000${r.stop_id}`
    candidates.set(k, [...(candidates.get(k) ?? []), {
      platformId: r.id, complexId: r.complex_id, distanceM: Number(r.distance_m), refs: r.refs ?? [], gtfsStopIds: r.gtfs_stop_ids ?? [],
    }])
  }
  for (const s of unique) {
    const found = candidates.get(`${s.feedId}\u0000${s.stopId}`) ?? []
    out.set(`${s.feedId}\u0000${s.stopId}\u0000${s.route ?? ''}`, platformsForStop({ stopId: s.stopId, parentId: null, routes: s.route ? [s.route] : [] }, found))
  }
  return out
}

async function loadPlatforms(ids: string[]): Promise<Map<string, PlatformInfo>> {
  if (!ids.length) return new Map()
  const rows = (await db.execute(sql`
    SELECT p.id, p.complex_id, p.name, p.level, p.axis_bearing, p.length_m, p.refs,
           ST_X(ST_PointOnSurface(p.geom)) AS lng, ST_Y(ST_PointOnSurface(p.geom)) AS lat,
           (SELECT g.name FROM station_complexes c JOIN geo_places g ON g.id = ANY(c.member_ids)
            WHERE c.id = p.complex_id ORDER BY g.centroid <-> ST_PointOnSurface(p.geom) LIMIT 1) AS station_name
    FROM station_platforms p WHERE p.id IN (${sql.join(ids.map((id) => sql`${id}`), sql`, `)})
  `)) as Array<Record<string, any>>
  return new Map(rows.map((r) => [r.id, {
    id: r.id, complexId: r.complex_id, name: r.name, stationName: r.station_name, level: r.level, axisBearing: r.axis_bearing,
    lengthM: r.length_m, refs: r.refs ?? [], lat: r.lat, lng: r.lng,
  }]))
}

async function loadEntrances(ids: string[]): Promise<Map<string, EntranceInfo>> {
  if (!ids.length) return new Map()
  const [rows, labels] = await Promise.all([
    db.execute(sql`
      SELECT id, name, tags->>'ref' AS ref, tags->>'level' AS level, ST_X(centroid) AS lng, ST_Y(centroid) AS lat
      FROM geo_places WHERE id IN (${sql.join(ids.map((id) => sql`${id}`), sql`, `)})
    `) as Promise<Array<Record<string, any>>>,
    entranceLabelsFor(ids).catch(() => new Map()),
  ])
  return new Map(rows.map((r) => {
    const label = labels.get(r.id)
    const level = r.level !== null && r.level !== undefined && Number.isFinite(Number(r.level)) ? Number(r.level) : 0
    return [r.id, {
      id: r.id, name: r.name, ref: r.ref, level, lat: r.lat, lng: r.lng, cornerLabel: label?.cornerLabel ?? null,
      entryAllowed: label?.entryAllowed ?? null, exitAllowed: label?.exitAllowed ?? null,
    }]
  }))
}

function toPath(r: Record<string, any>): StationPath {
  return {
    fromId: r.from_id, fromKind: r.from_kind, toId: r.to_id, toKind: r.to_kind, profile: r.profile, source: r.source,
    confidence: r.confidence, uncertaintyS: r.uncertainty_s, seconds: r.seconds, bestSeconds: r.best_seconds,
    bestPosition: r.best_position, metres: r.metres, levels: r.levels, crossesFareGate: r.crosses_fare_gate,
    surfaces: r.surfaces, geometry: r.geometry?.coordinates ?? null, vertexLevels: r.vertex_levels, steps: r.steps,
  }
}

/** Every path needed: between transfer platforms, and between boarding/alighting platforms and their entrances. */
async function loadPaths(profile: Profile, pairs: Array<[string, string]>, boarding: string[], alighting: string[]) {
  const clauses = [
    pairs.length ? sql`(from_id, to_id) IN (VALUES ${values(pairs)})` : null,
    boarding.length ? sql`(from_kind = 'entrance' AND to_id IN (${sql.join(boarding.map((id) => sql`${id}`), sql`, `)}))` : null,
    alighting.length ? sql`(to_kind = 'entrance' AND from_id IN (${sql.join(alighting.map((id) => sql`${id}`), sql`, `)}))` : null,
  ].filter((c) => c !== null)
  if (!clauses.length) return [] as StationPath[]
  const rows = (await db.execute(sql`
    SELECT from_id, from_kind, to_id, to_kind, profile, source, confidence, uncertainty_s, seconds, best_seconds, best_position,
           metres, levels, crosses_fare_gate, surfaces, ST_AsGeoJSON(geom, 7)::json AS geometry, vertex_levels, steps
    FROM station_paths
    WHERE profile = ${profile} AND (${sql.join(clauses, sql` OR `)})
  `)) as Array<Record<string, any>>
  return rows.map(toPath)
}

const routeSign = (leg: GuidedLeg) => {
  if (!leg.routeShortName) return null
  return `the ${leg.routeShortName}${leg.directionLabel ? ` · ${leg.directionLabel}` : ''}`
}

const entranceName = (e: EntranceInfo) => e.cornerLabel ?? e.name ?? e.ref ?? null

function endpoint(id: string, kind: EndpointKind, platforms: Map<string, PlatformInfo>, entrances: Map<string, EntranceInfo>): StationEndpoint | null {
  if (kind === 'platform') {
    const p = platforms.get(id)
    return p ? { id, kind, name: p.name, lat: p.lat, lng: p.lng, level: p.level } : null
  }
  const e = entrances.get(id)
  return e ? { id, kind, name: entranceName(e), lat: e.lat, lng: e.lng, level: e.level } : null
}

function stationWalk(
  row: StationPath, ctx: WalkContext, platforms: Map<string, PlatformInfo>, entrances: Map<string, EntranceInfo>,
): StationWalk | null {
  const from = endpoint(row.fromId, row.fromKind, platforms, entrances)
  const to = endpoint(row.toId, row.toKind, platforms, entrances)
  if (!from || !to) return null
  const walk: StationWalk = {
    source: row.source, estimate: !isWalked(row.source), seconds: row.seconds, uncertaintyS: row.uncertaintyS,
    metres: row.metres, levels: row.levels, from, to,
  }
  if (isWalked(row.source) && row.geometry && row.geometry.length > 1) {
    walk.geometry = { type: 'LineString', coordinates: row.geometry }
    walk.vertexLevels = row.vertexLevels ?? undefined
    walk.steps = guidanceSteps(row, ctx)
  }
  return walk
}

/**
 * Attach station guidance to every itinerary's legs, in place. A leg the
 * station paths know nothing about is left as it was; before the first build
 * this does nothing at all.
 */
export async function enrichStationGuidance(itineraries: Array<{ legs: GuidedLeg[] }>, deps: GuidanceDeps): Promise<void> {
  if (!itineraries.length || !(await ready())) return

  type End = { feedId: string; stopId: string; route: string | undefined }
  const endOf = (leg: GuidedLeg, side: 'from' | 'to'): End | null => {
    const id = splitId(leg[side].stopId)
    return id ? { ...id, route: leg.routeShortName } : null
  }
  const keyOf = (e: End) => `${e.feedId}\u0000${e.stopId}\u0000${e.route ?? ''}`
  const transit = itineraries.map((it) => it.legs.filter((l) => l.transitLeg))
  const ends = transit.flat().flatMap((leg) => [endOf(leg, 'from'), endOf(leg, 'to')]).filter((e): e is End => !!e)
  const platformsOf = await resolveStops(ends)
  const platformIds = (leg: GuidedLeg, side: 'from' | 'to') => {
    const e = endOf(leg, side)
    return e ? platformsOf.get(keyOf(e)) ?? [] : []
  }

  const pairs: Array<[string, string]> = []
  const boarding = new Set<string>(), alighting = new Set<string>()
  for (const legs of itineraries.map((it) => it.legs)) {
    const rides = legs.filter((l) => l.transitLeg)
    if (!rides.length) continue
    rides.slice(1).forEach((b, i) => {
      for (const pa of platformIds(rides[i], 'to')) for (const pb of platformIds(b, 'from')) if (pa !== pb) pairs.push([pa, pb])
    })
    if (!legs[0].transitLeg) platformIds(rides[0], 'from').forEach((p) => boarding.add(p))
    if (!legs[legs.length - 1].transitLeg) platformIds(rides[rides.length - 1], 'to').forEach((p) => alighting.add(p))
  }

  const paths = await loadPaths(deps.profile, [...new Map(pairs.map((p) => [p.join('>'), p])).values()], [...boarding], [...alighting])
  const byPair = new Map(paths.map((p) => [`${p.fromId}>${p.toId}`, p]))
  const entranceIds = [...new Set(paths.flatMap((p) => [p.fromKind === 'entrance' ? p.fromId : null, p.toKind === 'entrance' ? p.toId : null]).filter((x): x is string => !!x))]
  const allPlatformIds = [...new Set([...platformsOf.values()].flat())]
  const [platforms, entrances] = await Promise.all([loadPlatforms(allPlatformIds), loadEntrances(entranceIds)])

  const routesCache = new Map<string, Promise<Set<string> | null>>()
  const inSystem = (feedId: string, stopId: string) => {
    const k = `${feedId}\u0000${stopId}`
    if (!routesCache.has(k)) routesCache.set(k, deps.inSystemRoutes(feedId, stopId).catch(() => null))
    return routesCache.get(k)!
  }

  const scale = DEFAULT_PARAMS.costs.scale
  const walkMps = DEFAULT_PARAMS.costs.walkMps

  for (const it of itineraries) {
    const legs = it.legs
    const rides = legs.filter((l) => l.transitLeg)
    if (!rides.length) continue

    // Access: the entrance that makes origin → entrance → platform shortest.
    const first = rides[0]
    if (!legs[0].transitLeg) {
      const candidates = platformIds(first, 'from').flatMap((pid) =>
        paths.filter((p) => p.toId === pid && p.fromKind === 'entrance').flatMap((row) => {
          const e = entrances.get(row.fromId)
          return e && e.entryAllowed !== false ? [{ id: e.id, lat: e.lat, lng: e.lng, row, info: e }] : []
        }))
      const choice = chooseEntrance(legs[0].from, candidates, deps.walkMps)
      if (choice) {
        const e = choice.candidate.info
        const walk = stationWalk(choice.candidate.row, { kind: 'access', entrance: entranceName(e), sign: routeSign(first) }, platforms, entrances)
        if (walk) {
          first.entrance = {
            id: e.id, name: e.name, ref: e.ref, cornerLabel: e.cornerLabel, lat: e.lat, lng: e.lng,
            streetSeconds: choice.streetSeconds, stationSeconds: choice.candidate.row.seconds, totalSeconds: choice.totalSeconds, walk,
          }
        }
      }
    }

    // Egress: alighting platform → exit → destination.
    const last = rides[rides.length - 1]
    let exitRow: StationPath | null = null
    if (!legs[legs.length - 1].transitLeg) {
      const candidates = platformIds(last, 'to').flatMap((pid) =>
        paths.filter((p) => p.fromId === pid && p.toKind === 'entrance').flatMap((row) => {
          const e = entrances.get(row.toId)
          return e && e.exitAllowed !== false ? [{ id: e.id, lat: e.lat, lng: e.lng, row, info: e }] : []
        }))
      const choice = chooseEntrance(legs[legs.length - 1].to, candidates, deps.walkMps)
      if (choice) {
        const e = choice.candidate.info
        exitRow = choice.candidate.row
        const walk = stationWalk(exitRow, { kind: 'egress', entrance: entranceName(e) }, platforms, entrances)
        if (walk) {
          last.exit = {
            id: e.id, name: e.name, ref: e.ref, cornerLabel: e.cornerLabel, lat: e.lat, lng: e.lng,
            streetSeconds: choice.streetSeconds, stationSeconds: exitRow.seconds, totalSeconds: choice.totalSeconds, walk,
          }
        }
      }
    }

    // Transfers, and where to stand on the ride before each.
    const transferRows = new Map<GuidedLeg, StationPath>()
    await Promise.all(rides.slice(1).map(async (b, i) => {
      const a = rides[i]
      const pa = platformIds(a, 'to'), pb = platformIds(b, 'from')
      const row = representativeRow(pa.flatMap((x) => pb.map((y) => byPair.get(`${x}>${y}`))).filter((r): r is StationPath => !!r))
      const alight = splitId(a.to.stopId)
      const reachable = alight ? await inSystem(alight.feedId, alight.stopId) : null
      const facts = {
        samePlatform: pa.some((p) => pb.includes(p)),
        row,
        inSystem: reachable?.size && b.routeId ? reachable.has(b.routeId) : null,
        namedApart: !!row && (() => {
          const x = platforms.get(row.fromId)?.stationName, y = platforms.get(row.toId)?.stationName
          return !!x && !!y && x.trim().toLowerCase() !== y.trim().toLowerCase()
        })(),
        oneFare: deps.oneFare(a, b),
      }
      const cls = classifyTransfer(facts)
      const walk = row ? stationWalk(row, { kind: 'transfer', sign: routeSign(b), reentryFree: cls.reentryFree }, platforms, entrances) : null
      b.transfer = { ...cls, seconds: row ? row.seconds + cls.fareGateDelayS : null, walk }
      if (row) transferRows.set(a, row)
    }))

    for (const ride of rides) {
      const transferRow = transferRows.get(ride)
      const row = transferRow ?? (ride === last ? exitRow : null)
      if (!row || !positionIsMeasured(row)) continue
      const platform = platforms.get(row.fromId)
      const heading = ride.geometry ? arrivalBearing(ride.geometry.coordinates) : null
      if (!platform || heading === null) continue
      const hint = boardingHint(platform, row.bestPosition!, heading, walkMps, scale)
      if (!hint) continue
      const next = transferRow ? rides[rides.indexOf(ride) + 1] : null
      ride.boardingPosition = {
        ...hint,
        toward: transferRow ? 'transfer' : 'exit',
        landmark: next ? routeSign(next) : last.exit ? last.exit.cornerLabel ?? last.exit.name : null,
      }
    }
  }
}
