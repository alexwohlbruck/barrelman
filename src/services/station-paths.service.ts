/**
 * Station pedestrian graph and precomputed paths (PAR-350).
 *
 * `buildStationPaths` clusters every rail station into a complex, builds a
 * level-aware walking graph of each from OSM, and writes a time for every
 * platform-to-platform and platform-to-entrance pair, each tagged with how it
 * was made (graph, bridged, agency, estimated). It builds `*_next` tables and
 * swaps them in, so readers see the previous build until the new one is whole.
 * The OSM update and the full import run it; nothing in it is hand-maintained,
 * so a complex moves up the tiers by itself as OSM improves.
 */
import { sql } from 'drizzle-orm'
import { db, maintenanceConnection } from '../db'
import { agencyTimesByOsm, type AgencyStop } from '../lib/station-graph/agency'
import {
  assignToComplexes, boxAround, boxesTouch, clusterStations, geometryBox, type Complex, type StationElement,
} from '../lib/station-graph/complexes'
import { DEFAULT_PARAMS, type StationPathParams } from '../lib/station-graph/params'
import { solveComplex } from '../lib/station-graph/solve'
import { platformsForStop, stopPairTime, STOP_MATCH_M, type StopCandidate } from '../lib/station-graph/stops'
import { isEntrance, isPlatform, levelOf } from '../lib/station-graph/tags'
import type { ComplexReport, OsmNode, OsmWay, PathSource, Profile, StationPath, StationPlatform } from '../lib/station-graph/types'

type Log = (line: string) => void
type Sql = ReturnType<typeof maintenanceConnection>

/** Complexes are fetched and solved a cell of this many degrees at a time. */
const CELL_DEG = 0.02
const BATCH = 500

const STATION_ELEMENTS = `
  (tags @> '{"railway":"station"}' OR tags @> '{"railway":"halt"}' OR tags @> '{"public_transport":"station"}')
  AND (tags->>'station' IN ('subway', 'light_rail', 'train', 'monorail')
       OR tags->>'subway' = 'yes' OR tags->>'train' = 'yes' OR tags->>'light_rail' = 'yes'
       OR (tags->>'railway' IN ('station', 'halt') AND NOT tags ? 'station'))`

const WAY_FILTER = `
  osm_type IN ('W', 'R') AND geom_type IN ('line', 'area')
  AND (tags->>'highway' IN ('footway', 'corridor', 'steps', 'elevator', 'path', 'pedestrian')
       OR tags ? 'conveying' OR tags->>'railway' = 'platform' OR tags->>'public_transport' = 'platform'
       OR tags->>'indoor' IN ('corridor', 'area', 'room'))`

const NODE_FILTER = `
  geom_type = 'point'
  AND (tags->>'railway' IN ('subway_entrance', 'train_station_entrance') OR tags->>'highway' = 'elevator'
       OR tags->>'barrier' IN ('turnstile', 'full-height_turnstile') OR (tags->>'barrier' = 'gate' AND tags ? 'fee'))`

async function relationExists(q: Sql, name: string): Promise<boolean> {
  const [row] = await q`SELECT to_regclass(${name}) IS NOT NULL AS ok`
  return row?.ok === true
}

async function createNextTables(q: Sql) {
  await q.unsafe(`
    DROP TABLE IF EXISTS station_paths_next;
    DROP TABLE IF EXISTS station_platforms_next;
    DROP TABLE IF EXISTS station_complexes_next;
    CREATE TABLE station_complexes_next (
      id TEXT PRIMARY KEY,
      name TEXT,
      member_ids TEXT[] NOT NULL,
      geom GEOMETRY(Point, 4326) NOT NULL,
      platforms INTEGER NOT NULL,
      entrances INTEGER NOT NULL,
      tiers JSONB NOT NULL,
      previous_tiers JSONB,
      components INTEGER NOT NULL,
      disconnected BOOLEAN NOT NULL,
      built_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
    );
    CREATE TABLE station_platforms_next (
      id TEXT PRIMARY KEY,
      complex_id TEXT NOT NULL,
      name TEXT,
      level REAL,
      underground BOOLEAN NOT NULL,
      axis_bearing REAL NOT NULL,
      length_m REAL NOT NULL,
      refs TEXT[] NOT NULL,
      gtfs_stop_ids TEXT[] NOT NULL,
      geom GEOMETRY(Geometry, 4326) NOT NULL
    );
    CREATE TABLE station_paths_next (
      complex_id TEXT NOT NULL,
      from_id TEXT NOT NULL,
      from_kind TEXT NOT NULL,
      to_id TEXT NOT NULL,
      to_kind TEXT NOT NULL,
      profile TEXT NOT NULL,
      source TEXT NOT NULL,
      confidence REAL NOT NULL,
      uncertainty_s INTEGER NOT NULL,
      seconds INTEGER NOT NULL,
      best_seconds INTEGER,
      best_position REAL,
      metres INTEGER NOT NULL,
      levels INTEGER NOT NULL,
      crosses_fare_gate BOOLEAN NOT NULL,
      surfaces BOOLEAN,
      geom GEOMETRY(LineString, 4326),
      vertex_levels REAL[],
      steps JSONB,
      PRIMARY KEY (from_id, to_id, profile)
    );
  `)
}

async function swapIn(q: Sql) {
  await q.unsafe(`
    CREATE INDEX station_complexes_next_members_idx ON station_complexes_next USING GIN (member_ids);
    CREATE INDEX station_complexes_next_geom_idx ON station_complexes_next USING GIST (geom);
    CREATE INDEX station_platforms_next_complex_idx ON station_platforms_next (complex_id);
    CREATE INDEX station_platforms_next_geom_idx ON station_platforms_next USING GIST (geom);
    CREATE INDEX station_paths_next_complex_idx ON station_paths_next (complex_id, profile);
    ANALYZE station_complexes_next;
    ANALYZE station_platforms_next;
    ANALYZE station_paths_next;
  `)
  await q.begin(async (tx) => {
    for (const t of ['station_paths', 'station_platforms', 'station_complexes']) {
      await tx.unsafe(`DROP TABLE IF EXISTS ${t}`)
      await tx.unsafe(`ALTER TABLE ${t}_next RENAME TO ${t}`)
    }
    for (const [t, idx] of [
      ['station_complexes', 'members_idx'], ['station_complexes', 'geom_idx'], ['station_platforms', 'complex_idx'],
      ['station_platforms', 'geom_idx'], ['station_paths', 'complex_idx'],
    ]) {
      await tx.unsafe(`ALTER INDEX ${t}_next_${idx} RENAME TO ${t}_${idx}`)
    }
    for (const t of ['station_complexes', 'station_platforms', 'station_paths']) {
      await tx.unsafe(`ALTER TABLE ${t} RENAME CONSTRAINT ${t}_next_pkey TO ${t}_pkey`)
    }
  })
}

function chunks<T>(items: T[], size = BATCH): T[][] {
  const out: T[][] = []
  for (let i = 0; i < items.length; i += size) out.push(items.slice(i, i + size))
  return out
}

async function loadCell(q: Sql, box: number[]) {
  const [w, s, e, n] = box
  const ways = (await q.unsafe(
    `SELECT id, tags, ST_AsGeoJSON(geom, 7)::json AS geometry FROM geo_places
     WHERE geom && ST_MakeEnvelope($1, $2, $3, $4, 4326) AND ${WAY_FILTER}`,
    [w, s, e, n],
  )) as unknown as OsmWay[]
  const nodes = (await q.unsafe(
    `SELECT id, tags, ST_X(centroid) AS lon, ST_Y(centroid) AS lat FROM geo_places
     WHERE centroid && ST_MakeEnvelope($1, $2, $3, $4, 4326) AND ${NODE_FILTER}`,
    [w, s, e, n],
  )) as unknown as OsmNode[]
  return { ways, nodes }
}

async function loadAgency(q: Sql, box: number[]): Promise<{ stops: AgencyStop[]; times: Array<{ from: string; to: string; seconds: number }> }> {
  const [w, s, e, n] = box
  const rows = await q`
    SELECT feed_id, from_stop_id, to_stop_id, seconds, from_kind, to_kind,
           from_lon, from_lat, from_level, to_lon, to_lat, to_level
    FROM gtfs_station_times
    WHERE from_lon BETWEEN ${w} AND ${e} AND from_lat BETWEEN ${s} AND ${n}`
  const stops = new Map<string, AgencyStop>()
  const times = rows.map((r) => {
    const from = `${r.feed_id}:${r.from_stop_id}`, to = `${r.feed_id}:${r.to_stop_id}`
    stops.set(from, { stopId: from, kind: r.from_kind, lon: r.from_lon, lat: r.from_lat, level: r.from_level })
    stops.set(to, { stopId: to, kind: r.to_kind, lon: r.to_lon, lat: r.to_lat, level: r.to_level })
    return { from, to, seconds: r.seconds }
  })
  return { stops: [...stops.values()], times }
}

interface BuiltComplex {
  complex: Complex
  platforms: StationPlatform[]
  paths: StationPath[]
  report: ComplexReport
  entrances: number
}

async function writeCell(q: Sql, built: BuiltComplex[], previous: Map<string, unknown>) {
  const complexes = built.map((b) => ({
    id: b.complex.id, name: b.complex.name, member_ids: b.complex.members.map((m) => m.id),
    lon: b.complex.lon, lat: b.complex.lat, platforms: b.platforms.length, entrances: b.entrances,
    tiers: b.report.tiers, previous_tiers: previous.get(b.complex.id) ?? null,
    components: b.report.components, disconnected: b.report.disconnected,
  }))
  for (const batch of chunks(complexes)) {
    await q`
      INSERT INTO station_complexes_next (id, name, member_ids, geom, platforms, entrances, tiers, previous_tiers, components, disconnected)
      SELECT v.id, v.name, v.member_ids, ST_SetSRID(ST_MakePoint(v.lon, v.lat), 4326), v.platforms, v.entrances,
             v.tiers, v.previous_tiers, v.components, v.disconnected
      FROM jsonb_to_recordset(${q.json(batch as never)}) AS v(id text, name text, member_ids text[], lon float8, lat float8,
        platforms int, entrances int, tiers jsonb, previous_tiers jsonb, components int, disconnected boolean)
      ON CONFLICT (id) DO NOTHING`
  }
  const platforms = built.flatMap((b) => b.platforms.map((p) => ({
    id: p.id, complex_id: b.complex.id, name: p.name, level: p.level, underground: p.underground,
    axis_bearing: p.axisBearing, length_m: p.lengthM, refs: p.refs, gtfs_stop_ids: p.gtfsStopIds, geometry: JSON.stringify(p.geometry),
  })))
  for (const batch of chunks(platforms)) {
    await q`
      INSERT INTO station_platforms_next (id, complex_id, name, level, underground, axis_bearing, length_m, refs, gtfs_stop_ids, geom)
      SELECT v.id, v.complex_id, v.name, v.level, v.underground, v.axis_bearing, v.length_m, v.refs, v.gtfs_stop_ids,
             ST_SetSRID(ST_GeomFromGeoJSON(v.geometry), 4326)
      FROM jsonb_to_recordset(${q.json(batch as never)}) AS v(id text, complex_id text, name text, level real, underground boolean,
        axis_bearing real, length_m real, refs text[], gtfs_stop_ids text[], geometry text)
      ON CONFLICT (id) DO NOTHING`
  }
  const paths = built.flatMap((b) => b.paths.map((p) => ({
    complex_id: b.complex.id, from_id: p.fromId, from_kind: p.fromKind, to_id: p.toId, to_kind: p.toKind, profile: p.profile,
    source: p.source, confidence: p.confidence, uncertainty_s: p.uncertaintyS, seconds: p.seconds, best_seconds: p.bestSeconds,
    best_position: p.bestPosition, metres: p.metres, levels: p.levels, crosses_fare_gate: p.crossesFareGate, surfaces: p.surfaces,
    geometry: p.geometry && p.geometry.length > 1 ? JSON.stringify({ type: 'LineString', coordinates: p.geometry }) : null,
    vertex_levels: p.vertexLevels, steps: p.steps,
  })))
  for (const batch of chunks(paths)) {
    await q`
      INSERT INTO station_paths_next (complex_id, from_id, from_kind, to_id, to_kind, profile, source, confidence, uncertainty_s,
        seconds, best_seconds, best_position, metres, levels, crosses_fare_gate, surfaces, geom, vertex_levels, steps)
      SELECT v.complex_id, v.from_id, v.from_kind, v.to_id, v.to_kind, v.profile, v.source, v.confidence, v.uncertainty_s,
             v.seconds, v.best_seconds, v.best_position, v.metres, v.levels, v.crosses_fare_gate, v.surfaces,
             CASE WHEN v.geometry IS NULL THEN NULL ELSE ST_SetSRID(ST_GeomFromGeoJSON(v.geometry), 4326) END,
             v.vertex_levels, v.steps
      FROM jsonb_to_recordset(${q.json(batch as never)}) AS v(complex_id text, from_id text, from_kind text, to_id text, to_kind text,
        profile text, source text, confidence real, uncertainty_s int, seconds int, best_seconds int, best_position real, metres int,
        levels int, crosses_fare_gate boolean, surfaces boolean, geometry text, vertex_levels real[], steps jsonb)
      ON CONFLICT DO NOTHING`
  }
}

export interface BuildSummary {
  complexes: number
  withPlatforms: number
  disconnected: number
  paths: number
  tiers: Record<'pair' | 'street', Partial<Record<PathSource, number>>>
  upgraded: number
  downgraded: number
}

const TIER_ORDER: PathSource[] = ['graph', 'bridged', 'agency', 'estimated', 'straight_line']

/** A complex's tier mix as one number, to tell whether a rebuild moved it up or down. */
function tierScore(tiers: unknown): number | null {
  if (!tiers || typeof tiers !== 'object') return null
  let total = 0, weighted = 0
  for (const kind of Object.values(tiers as Record<string, Record<string, number>>)) {
    for (const [source, n] of Object.entries(kind)) {
      total += n
      weighted += n * TIER_ORDER.indexOf(source as PathSource)
    }
  }
  return total ? weighted / total : null
}

/** Rebuild every complex's graph and paths. Run after each OSM update and full import. */
export async function buildStationPaths(opts: { log?: Log; params?: StationPathParams } = {}): Promise<BuildSummary> {
  const log = opts.log ?? (() => {})
  const params = opts.params ?? DEFAULT_PARAMS
  const q = maintenanceConnection()
  try {
    const elements = (await q.unsafe(
      `SELECT id, name, ST_X(centroid) AS lon, ST_Y(centroid) AS lat FROM geo_places WHERE ${STATION_ELEMENTS}`,
    )) as unknown as StationElement[]
    const complexes = clusterStations(elements, params.graph.complexRadiusM)
    log(`  ${elements.length} station elements in ${complexes.length} complexes`)

    const previous = new Map<string, unknown>()
    if (await relationExists(q, 'station_complexes')) {
      for (const r of await q`SELECT id, tiers FROM station_complexes`) previous.set(r.id, r.tiers)
    }
    const agencyAvailable = await relationExists(q, 'gtfs_station_times')
    await createNextTables(q)

    const cells = new Map<string, Complex[]>()
    for (const c of complexes) {
      const key = `${Math.floor(c.lon / CELL_DEG)}:${Math.floor(c.lat / CELL_DEG)}`
      cells.set(key, [...(cells.get(key) ?? []), c])
    }

    const summary: BuildSummary = { complexes: complexes.length, withPlatforms: 0, disconnected: 0, paths: 0, tiers: { pair: {}, street: {} }, upgraded: 0, downgraded: 0 }
    const margin = params.graph.apronM + params.graph.complexRadiusM
    let done = 0
    for (const owned of cells.values()) {
      const box = boxAround(owned.flatMap((c) => c.members), margin)
      const { ways, nodes } = await loadCell(q, box)
      const agency = agencyAvailable ? await loadAgency(q, box) : null
      // Neighbouring cells' complexes compete for platforms near the edge, so each is assigned once.
      const nearby = complexes.filter((c) => c.members.some((m) => m.lon >= box[0] && m.lon <= box[2] && m.lat >= box[1] && m.lat <= box[3]))
      const platformWays = ways.filter((w) => isPlatform(w.tags))
      const platformOf = assignToComplexes(platformWays, nearby, params.graph.complexRadiusM)
      const entranceNodes = nodes.filter((n) => isEntrance(n.tags))
      const entranceOf = assignToComplexes(entranceNodes, nearby, params.graph.entranceRadiusM)

      const built: BuiltComplex[] = []
      for (const complex of owned) {
        const platforms = platformWays.filter((w) => platformOf.get(w) === complex)
        const entrances = entranceNodes.filter((n) => entranceOf.get(n) === complex)
        const empty = { pair: {}, street: {} }
        if (!platforms.length) {
          built.push({ complex, platforms: [], paths: [], report: { tiers: empty, components: 0, disconnected: false }, entrances: entrances.length })
          continue
        }
        const area = boxAround([...complex.members, ...platforms.flatMap((p) => {
          const b = geometryBox(p.geometry)
          return [{ lon: b[0], lat: b[1] }, { lon: b[2], lat: b[3] }]
        })], params.graph.apronM)
        const localWays = ways.filter((w) => boxesTouch(geometryBox(w.geometry), area))
        const localNodes = nodes.filter((n) => n.lon >= area[0] && n.lon <= area[2] && n.lat >= area[1] && n.lat <= area[3])
        const agencyTimes = agency && agency.stops.length
          ? agencyTimesByOsm(agency, platforms.map((p) => ({
              id: p.id, level: levelOf(p.tags), rings: p.geometry.type === 'MultiPolygon' ? p.geometry.coordinates.flat() : p.geometry.type === 'LineString' ? [p.geometry.coordinates] : p.geometry.coordinates,
              area: p.geometry.type === 'Polygon' || p.geometry.type === 'MultiPolygon',
            })), entrances)
          : undefined
        try {
          const result = solveComplex({
            platformIds: platforms.map((p) => p.id), entranceIds: entrances.map((e) => e.id),
            ways: localWays, nodes: localNodes, agency: agencyTimes,
          }, params)
          built.push({ complex, ...result, entrances: entrances.length })
        } catch (err) {
          log(`  ! ${complex.id} (${complex.name ?? 'unnamed'}): ${(err as Error).message}`)
        }
      }
      await writeCell(q, built, previous)

      for (const b of built) {
        if (!b.platforms.length) continue
        summary.withPlatforms++
        if (b.report.disconnected) summary.disconnected++
        summary.paths += b.paths.length
        for (const kind of ['pair', 'street'] as const) {
          for (const [source, n] of Object.entries(b.report.tiers[kind])) {
            summary.tiers[kind][source as PathSource] = (summary.tiers[kind][source as PathSource] ?? 0) + (n ?? 0)
          }
        }
        const before = tierScore(previous.get(b.complex.id)), after = tierScore(b.report.tiers)
        if (before !== null && after !== null && after < before - 1e-9) summary.upgraded++
        if (before !== null && after !== null && after > before + 1e-9) summary.downgraded++
      }
      done += owned.length
      if (cells.size > 20 && done % 500 < owned.length) log(`  ${done}/${complexes.length} complexes`)
    }

    await swapIn(q)
    const fmt = (t: Partial<Record<PathSource, number>>) => TIER_ORDER.filter((s) => t[s]).map((s) => `${s} ${t[s]}`).join(', ') || 'none'
    log(`  ${summary.withPlatforms} complexes with platforms, ${summary.disconnected} not fully on the graph, ${summary.paths} paths`)
    log(`  platform pairs: ${fmt(summary.tiers.pair)}`)
    log(`  platform-entrance: ${fmt(summary.tiers.street)}`)
    if (previous.size) log(`  since the last build: ${summary.upgraded} complexes moved up a tier, ${summary.downgraded} down`)
    return summary
  } finally {
    await q.end({ timeout: 5 })
  }
}

// ── Readers ─────────────────────────────────────────────────────────

const present = new Set<string>()
async function tableReady(name: string): Promise<boolean> {
  if (present.has(name)) return true
  const [row] = (await db.execute(sql`SELECT to_regclass(${name}) IS NOT NULL AS ok`)) as Array<{ ok: boolean }>
  if (row?.ok) present.add(name)
  return row?.ok === true
}

export interface StationPathsResponse {
  complex: {
    id: string
    name: string | null
    memberIds: string[]
    disconnected: boolean
    components: number
    tiers: ComplexReport['tiers']
  }
  platforms: Array<Omit<StationPlatform, 'geometry'> & { geometry: unknown }>
  paths: Array<Omit<StationPath, 'geometry'> & { geometry: unknown }>
}

/**
 * A complex's precomputed paths, found by the complex id, any member
 * station's id, or one of its platforms' ids. Null when there is no such
 * complex (or no build yet).
 */
export async function getStationPaths(
  id: string,
  filter: { from?: string; to?: string; profile?: Profile } = {},
): Promise<StationPathsResponse | null> {
  if (!(await tableReady('station_paths'))) return null
  const [complex] = (await db.execute(sql`
    SELECT c.id, c.name, c.member_ids, c.disconnected, c.components, c.tiers
    FROM station_complexes c
    WHERE c.id = ${id} OR ${id} = ANY(c.member_ids)
       OR c.id = (SELECT complex_id FROM station_platforms WHERE id = ${id})
    LIMIT 1
  `)) as Array<{ id: string; name: string | null; member_ids: string[]; disconnected: boolean; components: number; tiers: ComplexReport['tiers'] }>
  if (!complex) return null

  const platforms = (await db.execute(sql`
    SELECT id, name, level, underground, axis_bearing, length_m, refs, gtfs_stop_ids, ST_AsGeoJSON(geom, 7)::json AS geometry
    FROM station_platforms WHERE complex_id = ${complex.id} ORDER BY id
  `)) as Array<Record<string, any>>
  const paths = (await db.execute(sql`
    SELECT from_id, from_kind, to_id, to_kind, profile, source, confidence, uncertainty_s, seconds, best_seconds, best_position,
           metres, levels, crosses_fare_gate, surfaces, ST_AsGeoJSON(geom, 7)::json AS geometry, vertex_levels, steps
    FROM station_paths
    WHERE complex_id = ${complex.id} AND profile = ${filter.profile ?? 'foot'}
      ${filter.from ? sql`AND from_id = ${filter.from}` : sql.empty()}
      ${filter.to ? sql`AND to_id = ${filter.to}` : sql.empty()}
    ORDER BY from_id, to_id
  `)) as Array<Record<string, any>>

  return {
    complex: { id: complex.id, name: complex.name, memberIds: complex.member_ids, disconnected: complex.disconnected, components: complex.components, tiers: complex.tiers },
    platforms: platforms.map((p) => ({
      id: p.id, name: p.name, level: p.level, underground: p.underground, axisBearing: p.axis_bearing, lengthM: p.length_m,
      refs: p.refs, gtfsStopIds: p.gtfs_stop_ids, geometry: p.geometry,
    })),
    paths: paths.map((p) => {
      const drawn = p.source === 'graph' || p.source === 'bridged'
      return {
        fromId: p.from_id, fromKind: p.from_kind, toId: p.to_id, toKind: p.to_kind, profile: p.profile, source: p.source,
        confidence: p.confidence, uncertaintyS: p.uncertainty_s, seconds: p.seconds, bestSeconds: p.best_seconds,
        bestPosition: p.best_position, metres: p.metres, levels: p.levels, crossesFareGate: p.crosses_fare_gate,
        surfaces: p.surfaces, geometry: drawn ? p.geometry : null, vertexLevels: drawn ? p.vertex_levels : null,
        steps: drawn ? p.steps : null,
      }
    }),
  }
}

export interface StationTransfer {
  feedId: string
  fromStopId: string
  toStopId: string
  seconds: number
  source: PathSource
}

/** Route types that board from a platform a station graph knows: rail, metro, tram, monorail. */
const PLATFORM_ROUTE_TYPE = `(r.route_type IN (0, 1, 2, 12) OR r.route_type BETWEEN 100 AND 199
  OR r.route_type BETWEEN 400 AND 499 OR r.route_type BETWEEN 900 AND 999)`

/**
 * In-station transfer times between the rail stops of each feed, from the
 * station paths: every pair of stops in one complex gets a time with its
 * source, so transfers.txt never falls back to an agency's flat minimum for a
 * pair the build has seen. Empty before the first build.
 */
export async function stationTransferTimes(feedIds: string[], params: StationPathParams = DEFAULT_PARAMS): Promise<StationTransfer[]> {
  if (!feedIds.length) return []
  const q = maintenanceConnection()
  try {
    if (!(await relationExists(q, 'station_paths'))) return []
    const rows = await q.unsafe(`
      WITH stops AS MATERIALIZED (
        SELECT s.feed_id, s.stop_id, s.parent_station, s.stop_lon, s.stop_lat, s.geom,
               array_agg(DISTINCT COALESCE(NULLIF(r.route_short_name, ''), r.route_id)) AS routes
        FROM gtfs_stops s
        JOIN gtfs_stop_routes sr ON sr.feed_id = s.feed_id AND sr.stop_id = s.stop_id
        JOIN gtfs_routes r ON r.feed_id = sr.feed_id AND r.route_id = sr.route_id
        WHERE s.feed_id = ANY($1) AND COALESCE(s.location_type, 0) = 0 AND ${PLATFORM_ROUTE_TYPE}
        GROUP BY s.feed_id, s.stop_id, s.parent_station, s.stop_lon, s.stop_lat, s.geom
      )
      SELECT st.feed_id, st.stop_id, st.parent_station, st.stop_lon, st.stop_lat, st.routes,
             p.id AS platform_id, p.complex_id, p.refs, p.gtfs_stop_ids,
             ST_Distance(p.geom::geography, st.geom::geography) AS distance_m
      FROM stops st
      JOIN station_platforms p
        ON p.geom && ST_Expand(st.geom, $2::float8 / 80000)
       AND ST_DWithin(p.geom::geography, st.geom::geography, $2)
    `, [feedIds, STOP_MATCH_M])

    interface Stop { feedId: string; stopId: string; lon: number; lat: number; routes: string[]; candidates: StopCandidate[] }
    const stops = new Map<string, Stop>()
    for (const r of rows) {
      const key = `${r.feed_id}\u0000${r.stop_id}`
      const stop = stops.get(key) ?? { feedId: r.feed_id, stopId: r.stop_id, lon: r.stop_lon, lat: r.stop_lat, routes: r.routes ?? [], candidates: [] as StopCandidate[] }
      stop.candidates.push({ platformId: r.platform_id, complexId: r.complex_id, distanceM: Number(r.distance_m), refs: r.refs ?? [], gtfsStopIds: r.gtfs_stop_ids ?? [] })
      stops.set(key, stop)
    }

    const byComplex = new Map<string, Array<Stop & { platforms: string[] }>>()
    for (const stop of stops.values()) {
      const platforms = platformsForStop({ stopId: stop.stopId, parentId: null, routes: stop.routes }, stop.candidates)
      const nearest = [...stop.candidates].sort((a, b) => a.distanceM - b.distanceM)[0]
      const complexId = platforms.length ? stop.candidates.find((c) => c.platformId === platforms[0])!.complexId : nearest.complexId
      byComplex.set(complexId, [...(byComplex.get(complexId) ?? []), { ...stop, platforms }])
    }

    const times = new Map<string, { seconds: number; source: PathSource }>()
    const complexIds = [...byComplex.keys()]
    for (const batch of chunks(complexIds, 2000)) {
      const paths = await q`
        SELECT from_id, to_id, seconds, source FROM station_paths
        WHERE complex_id = ANY(${batch}) AND profile = 'foot' AND from_kind = 'platform' AND to_kind = 'platform'`
      for (const p of paths) times.set(`${p.from_id}>${p.to_id}`, { seconds: p.seconds, source: p.source })
    }

    const out: StationTransfer[] = []
    for (const members of byComplex.values()) {
      for (const a of members) {
        for (const b of members) {
          if (a === b || a.feedId !== b.feedId) continue
          const kx = Math.cos((a.lat * Math.PI) / 180) * 111320
          const straightM = Math.hypot((a.lon - b.lon) * kx, (a.lat - b.lat) * 110540)
          const t = stopPairTime(params, a, b, straightM, (x, y) => times.get(`${x}>${y}`))
          if (t) out.push({ feedId: a.feedId, fromStopId: a.stopId, toStopId: b.stopId, seconds: t.seconds, source: t.source })
        }
      }
    }
    return out
  } finally {
    await q.end({ timeout: 5 })
  }
}

/** Agency pathway times for one feed, stored at import for the next build to match onto OSM. */
export async function storeAgencyStationTimes(
  feedId: string,
  agency: { stops: AgencyStop[]; times: Array<{ from: string; to: string; seconds: number }> },
) {
  const q = maintenanceConnection()
  try {
    await q.unsafe(`
      CREATE TABLE IF NOT EXISTS gtfs_station_times (
        feed_id TEXT NOT NULL,
        from_stop_id TEXT NOT NULL,
        to_stop_id TEXT NOT NULL,
        seconds INTEGER NOT NULL,
        from_kind TEXT NOT NULL,
        to_kind TEXT NOT NULL,
        from_lon DOUBLE PRECISION NOT NULL,
        from_lat DOUBLE PRECISION NOT NULL,
        from_level REAL,
        to_lon DOUBLE PRECISION NOT NULL,
        to_lat DOUBLE PRECISION NOT NULL,
        to_level REAL,
        PRIMARY KEY (feed_id, from_stop_id, to_stop_id)
      );
      CREATE INDEX IF NOT EXISTS gtfs_station_times_from_idx ON gtfs_station_times (from_lon, from_lat);
    `)
    const at = new Map(agency.stops.map((s) => [s.stopId, s]))
    const rows = agency.times.flatMap((t) => {
      const a = at.get(t.from), b = at.get(t.to)
      return a && b ? [{
        from_stop_id: t.from, to_stop_id: t.to, seconds: t.seconds, from_kind: a.kind, to_kind: b.kind,
        from_lon: a.lon, from_lat: a.lat, from_level: a.level, to_lon: b.lon, to_lat: b.lat, to_level: b.level,
      }] : []
    })
    await q.begin(async (tx) => {
      await tx`DELETE FROM gtfs_station_times WHERE feed_id = ${feedId}`
      for (const batch of chunks(rows, 5000)) {
        await tx`
          INSERT INTO gtfs_station_times
          SELECT ${feedId}, v.from_stop_id, v.to_stop_id, v.seconds, v.from_kind, v.to_kind, v.from_lon, v.from_lat, v.from_level,
                 v.to_lon, v.to_lat, v.to_level
          FROM jsonb_to_recordset(${tx.json(batch as never)}) AS v(from_stop_id text, to_stop_id text, seconds int, from_kind text,
            to_kind text, from_lon float8, from_lat float8, from_level real, to_lon float8, to_lat float8, to_level real)
          ON CONFLICT DO NOTHING`
      }
    })
    return rows.length
  } finally {
    await q.end({ timeout: 5 })
  }
}
