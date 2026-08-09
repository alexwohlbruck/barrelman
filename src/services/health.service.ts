/**
 * Health reporting.
 *
 * `/health` used to answer "is the process up and can it reach Postgres?",
 * which tells a load balancer what it needs and tells an API consumer almost
 * nothing. A caller whose `/directions` requests are failing wants to know
 * whether *transit* is down, not whether some database is reachable.
 *
 * So the result now carries two extra layers:
 *
 *   - `dependencies` — one probe per backing service (Postgres, GraphHopper,
 *     Martin, MOTIS, Pelias).
 *   - `endpoints` — one entry per billing group, resolved from those probes.
 *     `/transit/*` is unavailable when MOTIS is down; `/search` is merely
 *     degraded when Pelias is, because it still returns POIs from PostGIS and
 *     only loses street addresses.
 *
 * Two rules keep this honest:
 *
 *   - **Optional dependencies never change the top-level `status`.** Pelias
 *     runs under a compose profile and plenty of instances never start it. A
 *     self-hoster who chose not to run it should not see a permanently
 *     `degraded` instance — but the `/geocode` entry should still say that
 *     address coverage is missing, because it genuinely is.
 *   - **Probes are cached.** `/health` is an unauthenticated endpoint a load
 *     balancer may hit every second; fanning out to five services on each call
 *     would make it both expensive and an amplification vector. Results are
 *     reused for BARRELMAN_HEALTH_CACHE_MS, and `checkedAt` reports how fresh
 *     they are.
 */
import { db } from '../db'
import { sql } from 'drizzle-orm'
import { checkMotisHealth as _checkMotisHealth } from './transit.service'
import { CREDIT_COSTS, GROUP_PREFIXES, type EndpointGroup } from '../billing/plans'
import { envNumber } from '../config/env'

export type DependencyKey = 'database' | 'graphhopper' | 'martin' | 'motis' | 'pelias'
export type DependencyStatus = 'ok' | 'unavailable'
export type EndpointStatus = 'ok' | 'degraded' | 'unavailable'

export interface DependencyHealth {
  status: DependencyStatus
  /** Round-trip of the probe. Absent when the probe never got a response. */
  latencyMs?: number
  /**
   * Why it is unavailable. Omitted from the public `/health` response — see
   * `redactHealth` — because upstream errors can carry internal hostnames.
   */
  message?: string
  /** True for services an instance may legitimately not run at all. */
  optional?: boolean
}

export interface EndpointHealth {
  group: EndpointGroup
  /** Path prefixes this group serves, from the same table metering uses. */
  paths: readonly string[]
  status: EndpointStatus
  /** Credits a request to this group costs, so one call answers "what and how much". */
  creditCost: number
  /** Services this group needs. A failure here takes the group down. */
  dependsOn: readonly DependencyKey[]
  /** Services that only add coverage. A failure here degrades, never disables. */
  enhancedBy?: readonly DependencyKey[]
  /** Present when not `ok` — what a caller should expect to happen. */
  message?: string
}

export interface HealthResult {
  status: 'ok' | 'degraded' | 'error'
  /** Retained verbatim from the original response shape — probes still exist below. */
  database: 'connected' | 'disconnected'
  motis?: 'ok' | 'unavailable'
  /** Seconds since this process started. */
  uptimeSeconds: number
  /** When the dependency probes last ran, ISO-8601. May be up to the cache TTL old. */
  checkedAt: string
  dependencies: Record<DependencyKey, DependencyHealth>
  endpoints: EndpointHealth[]
}

/**
 * What each billing group actually calls at request time.
 *
 * Verified against the services rather than assumed: `/isochrone` reaches MOTIS
 * only for `mode=transit` (one-to-all), so MOTIS enhances rather than gates it;
 * `/route` proxies GraphHopper and touches no table at all; `/search` folds in
 * `forwardGeocode`, which is why Pelias appears there and not only under
 * `/geocode`.
 */
const ENDPOINT_DEPENDENCIES: Record<
  EndpointGroup,
  { dependsOn: readonly DependencyKey[]; enhancedBy?: readonly DependencyKey[]; partialMessage?: string }
> = {
  tiles: { dependsOn: ['database', 'martin'] },
  places: { dependsOn: ['database'] },
  spatial: { dependsOn: ['database'] },
  search: {
    dependsOn: ['database'],
    enhancedBy: ['pelias'],
    partialMessage: 'Street addresses are unavailable; POI and place results are unaffected',
  },
  geocode: {
    dependsOn: ['database'],
    enhancedBy: ['pelias'],
    partialMessage: 'Forward geocoding and place-level reverse are unavailable; coordinate reverse still works',
  },
  routing: { dependsOn: ['graphhopper'] },
  isochrone: {
    dependsOn: ['graphhopper'],
    enhancedBy: ['motis'],
    partialMessage: 'Transit isochrones are unavailable; walk, bike and car are unaffected',
  },
  transit: { dependsOn: ['database', 'motis'] },
}

/** Services an instance may legitimately not run. These never fail the instance. */
const OPTIONAL: ReadonlySet<DependencyKey> = new Set(['pelias'])

const PROBE_TIMEOUT_MS = 3000

const PATHS_BY_GROUP = new Map(GROUP_PREFIXES.map((entry) => [entry.group, entry.prefixes]))

export const dependencyUrls = (): Record<Exclude<DependencyKey, 'database'>, string> => ({
  graphhopper: process.env.GRAPHHOPPER_URL || 'http://barrelman-graphhopper:8989',
  martin: process.env.MARTIN_URL || 'http://barrelman-martin:3000',
  motis: process.env.MOTIS_URL || 'http://barrelman-motis:8080',
  pelias: process.env.PELIAS_URL || 'http://pelias_api:4000',
})

/**
 * GET a service's health path. Shared with the admin console's service list, so
 * the two can't disagree about whether something is up.
 */
export async function probeHttp(
  url: string,
  path: string,
  fetchFn: typeof fetch = globalThis.fetch,
): Promise<DependencyHealth> {
  const start = performance.now()
  try {
    const res = await fetchFn(`${url}${path}`, { signal: AbortSignal.timeout(PROBE_TIMEOUT_MS) })
    const latencyMs = Math.round(performance.now() - start)
    if (res.ok) return { status: 'ok', latencyMs }
    return { status: 'unavailable', latencyMs, message: `HTTP ${res.status}` }
  } catch (err) {
    return { status: 'unavailable', message: err instanceof Error ? err.message : 'Connection failed' }
  }
}

export async function probeDatabase(): Promise<DependencyHealth> {
  const start = performance.now()
  try {
    await db.execute(sql`SELECT 1`)
    return { status: 'ok', latencyMs: Math.round(performance.now() - start) }
  } catch (err) {
    return { status: 'unavailable', message: err instanceof Error ? err.message : 'Connection failed' }
  }
}

export interface HealthDeps {
  checkMotisHealth?: typeof _checkMotisHealth
  fetchFn?: typeof fetch
}

/** Run every probe concurrently — a slow one must not serialise the rest. */
async function probeAll(deps: HealthDeps): Promise<Record<DependencyKey, DependencyHealth>> {
  const checkMotis = deps.checkMotisHealth || _checkMotisHealth
  const urls = dependencyUrls()

  const motisProbe = async (): Promise<DependencyHealth> => {
    const start = performance.now()
    const r = await checkMotis()
    return { status: r.status, latencyMs: Math.round(performance.now() - start), message: r.message }
  }

  const [database, graphhopper, martin, motis, pelias] = await Promise.all([
    probeDatabase(),
    probeHttp(urls.graphhopper, '/health', deps.fetchFn),
    probeHttp(urls.martin, '/health', deps.fetchFn),
    motisProbe(),
    // /status is the only unauthenticated 200 Pelias offers — /v1/status is a 404.
    probeHttp(urls.pelias, '/status', deps.fetchFn),
  ])

  return { database, graphhopper, martin, motis, pelias: { ...pelias, optional: true } }
}

/** Resolve each billing group's status from the probe results. */
function resolveEndpoints(dependencies: Record<DependencyKey, DependencyHealth>): EndpointHealth[] {
  return GROUP_PREFIXES.map(({ group }) => {
    const { dependsOn, enhancedBy, partialMessage } = ENDPOINT_DEPENDENCIES[group]

    const down = dependsOn.filter((key) => dependencies[key].status !== 'ok')
    const partial = (enhancedBy ?? []).filter((key) => dependencies[key].status !== 'ok')

    const base: EndpointHealth = {
      group,
      paths: PATHS_BY_GROUP.get(group) ?? [],
      status: 'ok',
      creditCost: CREDIT_COSTS[group],
      dependsOn,
      ...(enhancedBy ? { enhancedBy } : {}),
    }

    if (down.length) {
      return { ...base, status: 'unavailable', message: `Requires ${down.join(' and ')}, currently unreachable` }
    }
    if (partial.length) {
      return { ...base, status: 'degraded', message: partialMessage ?? `${partial.join(' and ')} unreachable` }
    }
    return base
  })
}

interface Snapshot {
  at: number
  dependencies: Record<DependencyKey, DependencyHealth>
}

let snapshot: Snapshot | null = null
let inFlight: Promise<Snapshot> | null = null

const cacheMs = () => envNumber('BARRELMAN_HEALTH_CACHE_MS', 5000)

/**
 * Probe, or reuse a recent result.
 *
 * Concurrent callers share one `inFlight` promise rather than each starting
 * their own fan-out: a burst of probes arriving together is exactly the case
 * the cache exists for, and without this they would all miss it.
 */
async function currentSnapshot(deps: HealthDeps): Promise<Snapshot> {
  const ttl = cacheMs()
  if (snapshot && Date.now() - snapshot.at < ttl) return snapshot
  if (inFlight) return inFlight

  const probe: Promise<Snapshot> = probeAll(deps)
    .then((dependencies) => {
      snapshot = { at: Date.now(), dependencies }
      return snapshot
    })
    .finally(() => {
      // Only clear our own. A resetHealthCache() mid-probe (the admin console
      // forcing a re-check) starts a second probe, and an unconditional clear
      // here would wipe that newer one's slot the moment this older one landed
      // — leaving a probe running that nobody could join.
      if (inFlight === probe) inFlight = null
    })
  inFlight = probe

  return probe
}

/** Discard the cached probe result. For tests, and for a forced re-check. */
export function resetHealthCache(): void {
  snapshot = null
  inFlight = null
}

export async function checkHealth(deps: HealthDeps = {}): Promise<HealthResult> {
  const { at, dependencies } = await currentSnapshot(deps)
  const endpoints = resolveEndpoints(dependencies)

  const database = dependencies.database.status === 'ok' ? 'connected' : 'disconnected'

  // Overall status. The database is the one thing nothing works without, so it
  // alone is an error. Any other REQUIRED dependency failing takes some group
  // down, which is what degraded means. Optional services are excluded on
  // purpose — see the note at the top of this file.
  let status: HealthResult['status'] = 'ok'
  if (database === 'disconnected') {
    status = 'error'
  } else if (
    (Object.entries(dependencies) as [DependencyKey, DependencyHealth][]).some(
      ([key, dep]) => dep.status !== 'ok' && !OPTIONAL.has(key),
    )
  ) {
    status = 'degraded'
  }

  return {
    status,
    database,
    motis: dependencies.motis.status,
    uptimeSeconds: Math.round(process.uptime()),
    checkedAt: new Date(at).toISOString(),
    dependencies,
    endpoints,
  }
}

/**
 * Strip probe error text for the unauthenticated response.
 *
 * A raw upstream error carries internal hostnames and ports ("connect
 * ECONNREFUSED 172.19.0.4:8989"), which is fine for an authenticated operator
 * and needless exposure on a public endpoint. Statuses, latencies and the
 * per-endpoint messages we author ourselves all survive.
 */
export function redactHealth(result: HealthResult): HealthResult {
  const dependencies = Object.fromEntries(
    Object.entries(result.dependencies).map(([key, dep]) => {
      const { message: _dropped, ...rest } = dep
      return [key, rest]
    }),
  ) as Record<DependencyKey, DependencyHealth>

  return { ...result, dependencies }
}
