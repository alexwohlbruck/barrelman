/**
 * Metrics for the admin console: data-table stats and downstream service health.
 * Every query is defensive — a missing table or unreachable service degrades to
 * a null/`unavailable` value instead of failing the whole payload.
 */
import { sql } from 'drizzle-orm'
import { db } from '../db'
import { checkMotisHealth } from './transit.service'

async function scalar<T = number>(query: ReturnType<typeof sql>, fallback: T | null = null): Promise<T | null> {
  try {
    const rows = (await db.execute(query)) as any[]
    const row = rows[0]
    if (!row) return fallback
    const val = Object.values(row)[0]
    return (val === null || val === undefined ? fallback : (Number.isNaN(Number(val)) ? val : Number(val))) as T
  } catch {
    return fallback
  }
}

/** Count rows in a table, returning null if the table does not exist. */
async function tableCount(table: string): Promise<number | null> {
  return scalar<number>(sql.raw(`SELECT count(*)::bigint AS c FROM ${table}`), null)
}

export interface DataMetrics {
  database: {
    sizeBytes: number | null
    sizePretty: string | null
  }
  geoPlaces: {
    /**
     * geo_places is ~22M rows; exact conditional counts take 30s+, so these are
     * estimates: `total` from pg_class.reltuples and the rest scaled from a
     * table sample. `approx` flags that the console should label them ≈.
     */
    total: number | null
    named: number | null
    intersections: number | null
    withParentContext: number | null
    withEmbedding: number | null
    withCodes: number | null
    parentContextCoverage: number | null
    embeddingCoverage: number | null
    approx: boolean
  }
  gtfs: {
    feeds: number | null
    stops: number | null
    routes: number | null
    transfers: number | null
    tripPatterns: number | null
    shapes: number | null
    feedsWithRt: number | null
    lastImport: string | null
  }
  gbfs: {
    systems: number | null
    stations: number | null
  }
  transit: {
    stopAreaMembers: number | null
  }
}

interface GeoSample {
  s_total: number
  s_named: number
  s_x: number
  s_pc: number
  s_emb: number
  s_codes: number
}

/** Sample geo_places (~0.5% of pages) for fast, approximate coverage ratios. */
async function geoPlacesSample(): Promise<GeoSample | null> {
  try {
    const rows = (await db.execute(sql`
      SELECT
        count(*)::bigint AS s_total,
        count(*) FILTER (WHERE name IS NOT NULL)::bigint AS s_named,
        count(*) FILTER (WHERE osm_type = 'X')::bigint AS s_x,
        count(*) FILTER (WHERE parent_context IS NOT NULL)::bigint AS s_pc,
        count(*) FILTER (WHERE embedding IS NOT NULL)::bigint AS s_emb,
        count(*) FILTER (WHERE codes IS NOT NULL)::bigint AS s_codes
      FROM geo_places TABLESAMPLE SYSTEM (0.5)
    `)) as any[]
    const r = rows[0]
    if (!r) return null
    return {
      s_total: Number(r.s_total),
      s_named: Number(r.s_named),
      s_x: Number(r.s_x),
      s_pc: Number(r.s_pc),
      s_emb: Number(r.s_emb),
      s_codes: Number(r.s_codes),
    }
  } catch {
    return null
  }
}

export async function getDataMetrics(): Promise<DataMetrics> {
  const [
    sizeBytes,
    sizePretty,
    reltuples,
    sample,
    feeds,
    stops,
    routes,
    transfers,
    tripPatterns,
    shapes,
    feedsWithRt,
    lastImport,
    gbfsSystems,
    gbfsStations,
    stopAreaMembers,
  ] = await Promise.all([
    scalar<number>(sql`SELECT pg_database_size(current_database()) AS s`),
    scalar<string>(sql`SELECT pg_size_pretty(pg_database_size(current_database())) AS s`),
    scalar<number>(sql`SELECT reltuples::bigint AS c FROM pg_class WHERE relname = 'geo_places'`),
    geoPlacesSample(),
    tableCount('gtfs_feeds'),
    tableCount('gtfs_stops'),
    tableCount('gtfs_routes'),
    tableCount('gtfs_transfers'),
    tableCount('gtfs_trip_patterns'),
    tableCount('gtfs_shapes'),
    scalar<number>(sql`SELECT count(*)::bigint AS c FROM gtfs_feeds WHERE rt_urls IS NOT NULL AND rt_urls::text NOT IN ('null', '{}', '[]')`),
    scalar<string>(sql`SELECT max(imported_at)::text AS s FROM gtfs_feeds`),
    tableCount('gbfs_systems'),
    tableCount('gbfs_stations'),
    tableCount('stop_area_members'),
  ])

  const total = reltuples && reltuples > 0 ? reltuples : null
  const pct = (num: number, den: number) => (den > 0 ? Math.round((num / den) * 10000) / 100 : null)
  // Scale a sampled sub-count up to the estimated table total.
  const scale = (sub: number) =>
    sample && total && sample.s_total > 0 ? Math.round((sub / sample.s_total) * total) : null

  const geoPlaces: DataMetrics['geoPlaces'] = sample
    ? {
        total,
        named: scale(sample.s_named),
        intersections: scale(sample.s_x),
        withParentContext: scale(sample.s_pc),
        withEmbedding: scale(sample.s_emb),
        withCodes: scale(sample.s_codes),
        parentContextCoverage: sample.s_named > 0 ? pct(sample.s_pc, sample.s_named) : null,
        embeddingCoverage: sample.s_named > 0 ? pct(sample.s_emb, sample.s_named) : null,
        approx: true,
      }
    : {
        total,
        named: null,
        intersections: null,
        withParentContext: null,
        withEmbedding: null,
        withCodes: null,
        parentContextCoverage: null,
        embeddingCoverage: null,
        approx: true,
      }

  return {
    database: { sizeBytes, sizePretty },
    geoPlaces,
    gtfs: { feeds, stops, routes, transfers, tripPatterns, shapes, feedsWithRt, lastImport },
    gbfs: { systems: gbfsSystems, stations: gbfsStations },
    transit: { stopAreaMembers },
  }
}

export interface ServiceStatus {
  name: string
  key: string
  status: 'ok' | 'unavailable'
  url?: string
  latencyMs?: number
  message?: string
}

async function pingHttp(name: string, key: string, url: string, path: string): Promise<ServiceStatus> {
  const start = performance.now()
  try {
    const res = await fetch(`${url}${path}`, { signal: AbortSignal.timeout(3000) })
    const latencyMs = Math.round(performance.now() - start)
    if (res.ok) return { name, key, status: 'ok', url, latencyMs }
    return { name, key, status: 'unavailable', url, latencyMs, message: `HTTP ${res.status}` }
  } catch (err) {
    return {
      name,
      key,
      status: 'unavailable',
      url,
      message: err instanceof Error ? err.message : 'Connection failed',
    }
  }
}

export async function getServiceStatuses(): Promise<ServiceStatus[]> {
  const graphhopperUrl = process.env.GRAPHHOPPER_URL || 'http://barrelman-graphhopper:8989'
  const martinUrl = process.env.MARTIN_URL || 'http://barrelman-martin:3000'
  const motisUrl = process.env.MOTIS_URL || 'http://barrelman-motis:8080'

  const dbCheck = (async (): Promise<ServiceStatus> => {
    const start = performance.now()
    try {
      await db.execute(sql`SELECT 1`)
      return { name: 'PostgreSQL / PostGIS', key: 'database', status: 'ok', latencyMs: Math.round(performance.now() - start) }
    } catch (err) {
      return { name: 'PostgreSQL / PostGIS', key: 'database', status: 'unavailable', message: err instanceof Error ? err.message : 'Connection failed' }
    }
  })()

  const motisCheck = (async (): Promise<ServiceStatus> => {
    const start = performance.now()
    const r = await checkMotisHealth()
    return {
      name: 'MOTIS (transit routing)',
      key: 'motis',
      status: r.status,
      url: motisUrl,
      latencyMs: Math.round(performance.now() - start),
      message: r.message,
    }
  })()

  return Promise.all([
    dbCheck,
    motisCheck,
    pingHttp('GraphHopper (routing)', 'graphhopper', graphhopperUrl, '/health'),
    pingHttp('Martin (vector tiles)', 'martin', martinUrl, '/health'),
  ])
}
