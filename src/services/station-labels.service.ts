/**
 * Station labels, derived and stored so a request only reads them:
 *
 * - `stop_direction_labels`: what each directional platform is signed for,
 *   from agency open data and from the platform's own OSM tags.
 * - `entrance_labels`: where each station entrance is ("Broadway & West 28th
 *   Street · NW corner") and whether it can be used to enter, exit or both.
 *
 * Both are rebuilt from scratch by `refreshDirectionLabels` and
 * `refreshEntranceLabels`, which the GTFS import and the OSM update run, so
 * nothing in them is ever hand-maintained. Agency data is enrichment only: a
 * source that is down keeps its previous rows, and an instance without one
 * still gets every OSM-derived label.
 */
import { sql, type SQL } from 'drizzle-orm'
import { drizzle } from 'drizzle-orm/postgres-js'
import { db, maintenanceConnection } from '../db'
import {
  DIRECTION_LABEL_SOURCES,
  ENTRANCE_SOURCES,
  type AgencyEntrance,
  type StopLabel,
} from '../lib/station-labels/agency-sources'
import { cleanLabel, directionPhrase, pickDirectionLabel, type DirectionLabelRow } from '../lib/station-labels/direction-labels'
import { cornerLabel, type CornerStreet } from '../lib/station-labels/corner-label'
import { entranceAccess } from '../lib/station-labels/entrance-access'

export type FetchFn = (url: string, init?: RequestInit) => Promise<Response>
type Log = (line: string) => void

type Db = typeof db

const USER_AGENT = 'barrelman-station-labels'
const BATCH = 1000

async function fetchRows(url: string, fetchFn: FetchFn): Promise<unknown[]> {
  const res = await fetchFn(url, { headers: { 'User-Agent': USER_AGENT, Accept: 'application/json' } })
  if (!res.ok) throw new Error(`${res.status} ${res.statusText}`)
  const body = await res.json()
  if (!Array.isArray(body)) throw new Error('expected a JSON array')
  return body
}

/** Only a hit is cached: the tables appear once the first refresh has run. */
const present = new Set<string>()

async function relationAvailable(name: string): Promise<boolean> {
  if (present.has(name)) return true
  const [row] = (await db.execute(sql`SELECT to_regclass(${name}) IS NOT NULL AS ok`)) as Array<{ ok: boolean }>
  if (row?.ok === true) present.add(name)
  return row?.ok === true
}

function chunks<T>(items: T[], size = BATCH): T[][] {
  const out: T[][] = []
  for (let i = 0; i < items.length; i += size) out.push(items.slice(i, i + size))
  return out
}

/** The refreshes scan whole regions, past the request pool's statement timeout. */
async function withMaintenance<T>(run: (q: Db) => Promise<T>): Promise<T> {
  const client = maintenanceConnection()
  try {
    return await run(drizzle(client) as unknown as Db)
  } finally {
    await client.end({ timeout: 5 })
  }
}

interface FeedRef {
  feedId: string
  onestopId: string | null
}

async function feedsToLabel(q: Db, feedId?: string): Promise<FeedRef[]> {
  const rows = (await q.execute(sql`
    SELECT feed_id, onestop_id FROM gtfs_feeds
    ${feedId ? sql`WHERE feed_id = ${feedId}` : sql.empty()}
  `)) as Array<{ feed_id: string; onestop_id: string | null }>
  return rows.map((r) => ({ feedId: r.feed_id, onestopId: r.onestop_id }))
}

/** Replace one source's labels for one feed, keeping only stops the feed has. */
async function replaceLabels(q: Db, feedId: string, sources: string[], rows: Array<StopLabel & { source: string }>) {
  await q.transaction(async (tx) => {
    await tx.execute(sql`
      DELETE FROM stop_direction_labels
      WHERE feed_id = ${feedId} AND source IN (${sql.join(sources.map((s) => sql`${s}`), sql`, `)})
    `)
    for (const batch of chunks(rows)) {
      await tx.execute(sql`
        INSERT INTO stop_direction_labels (feed_id, stop_id, label, source)
        SELECT ${feedId}, v.stop_id, v.label, v.source
        FROM jsonb_to_recordset(${JSON.stringify(batch.map((r) => ({ stop_id: r.stopId, label: r.label, source: r.source })))}::jsonb)
          AS v(stop_id text, label text, source text)
        JOIN gtfs_stops s ON s.feed_id = ${feedId} AND s.stop_id = v.stop_id
        ON CONFLICT (feed_id, stop_id, source) DO UPDATE SET label = EXCLUDED.label, updated_at = NOW()
      `)
    }
  })
}

/**
 * Labels from the directional platforms OSM ties to a GTFS stop through
 * `gtfs:stop_id`. A platform tagged for several stops serves both directions,
 * so it says nothing about either.
 */
async function osmLabels(q: Db, feedId?: string): Promise<Map<string, Array<StopLabel & { source: string }>>> {
  const rows = (await q.execute(sql`
    WITH p AS (
      SELECT tags->>'gtfs:stop_id' AS stop_id, tags->>'destination' AS destination,
             name, tags->>'description' AS description, centroid
      FROM geo_places
      WHERE (tags @> '{"public_transport":"platform"}' OR tags @> '{"railway":"platform"}'
             OR tags @> '{"public_transport":"stop_position"}')
        AND tags ? 'gtfs:stop_id'
        AND strpos(tags->>'gtfs:stop_id', ';') = 0
    )
    SELECT DISTINCT ON (s.feed_id, s.stop_id)
      s.feed_id, s.stop_id, p.destination, p.name, p.description
    FROM p
    JOIN gtfs_stops s ON s.stop_id = p.stop_id
    WHERE COALESCE(s.parent_station, '') <> ''
      ${feedId ? sql`AND s.feed_id = ${feedId}` : sql.empty()}
      AND ST_DWithin(p.centroid::geography,
                     ST_SetSRID(ST_MakePoint(s.stop_lon, s.stop_lat), 4326)::geography, 300)
    ORDER BY s.feed_id, s.stop_id, (p.destination IS NOT NULL) DESC,
             p.centroid <-> ST_SetSRID(ST_MakePoint(s.stop_lon, s.stop_lat), 4326)
  `)) as Array<{ feed_id: string; stop_id: string; destination: string | null; name: string | null; description: string | null }>

  const byFeed = new Map<string, Array<StopLabel & { source: string }>>()
  for (const r of rows) {
    const destination = cleanLabel(r.destination)
    const words = destination ? null : directionPhrase(r.name, r.description)
    const label = destination ?? words
    if (!label) continue
    const list = byFeed.get(r.feed_id) ?? []
    list.push({ stopId: r.stop_id, label, source: destination ? 'osm-destination' : 'osm-direction' })
    byFeed.set(r.feed_id, list)
  }
  return byFeed
}

/**
 * Rebuild direction labels for one feed, or every feed. Run after a feed is
 * imported and after each OSM update.
 */
export function refreshDirectionLabels(opts: { feedId?: string; fetchFn?: FetchFn; log?: Log } = {}) {
  return withMaintenance((q) => directionLabelsRefresh(q, opts))
}

async function directionLabelsRefresh(q: Db, opts: { feedId?: string; fetchFn?: FetchFn; log?: Log }) {
  const log = opts.log ?? (() => {})
  const fetchFn = opts.fetchFn ?? fetch
  const feeds = await feedsToLabel(q, opts.feedId)
  const counts: Record<string, number> = {}

  for (const source of DIRECTION_LABEL_SOURCES) {
    const matching = feeds.filter((f) => f.onestopId && source.onestopIds.includes(f.onestopId))
    if (!matching.length) continue
    let labels: StopLabel[]
    try {
      labels = source.parse(await fetchRows(source.url, fetchFn))
    } catch (err) {
      log(`  ${source.id}: fetch failed (${(err as Error).message}); keeping its previous labels`)
      continue
    }
    for (const feed of matching) {
      await replaceLabels(q, feed.feedId, [source.id], labels.map((l) => ({ ...l, source: source.id })))
    }
    counts[source.id] = labels.length
  }

  const osm = await osmLabels(q, opts.feedId)
  for (const feed of feeds) {
    const rows = osm.get(feed.feedId) ?? []
    await replaceLabels(q, feed.feedId, ['osm-destination', 'osm-direction'], rows)
    counts.osm = (counts.osm ?? 0) + rows.length
  }
  log(`  direction labels: ${Object.entries(counts).map(([k, v]) => `${k} ${v}`).join(', ') || 'none'}`)
  return counts
}

async function refreshAgencyEntrances(q: Db, fetchFn: FetchFn, log: Log) {
  const onestops = new Set((await feedsToLabel(q)).map((f) => f.onestopId).filter(Boolean))
  for (const source of ENTRANCE_SOURCES) {
    if (!source.onestopIds.some((id) => onestops.has(id))) continue
    let entrances: AgencyEntrance[]
    try {
      entrances = source.parse(await fetchRows(source.url, fetchFn))
    } catch (err) {
      log(`  ${source.id}: fetch failed (${(err as Error).message}); keeping its previous entrances`)
      continue
    }
    await q.transaction(async (tx) => {
      await tx.execute(sql`DELETE FROM agency_entrances WHERE source = ${source.id}`)
      for (const batch of chunks(entrances)) {
        await tx.execute(sql`
          INSERT INTO agency_entrances (source, ref, stop_id, entrance_type, entry_allowed, exit_allowed, geom)
          SELECT ${source.id}, v.ref, v.stop_id, v.entrance_type, v.entry_allowed, v.exit_allowed,
                 ST_SetSRID(ST_MakePoint(v.lon, v.lat), 4326)
          FROM jsonb_to_recordset(${JSON.stringify(batch.map((e) => ({
            ref: e.ref, stop_id: e.stopId, entrance_type: e.entranceType,
            entry_allowed: e.entryAllowed, exit_allowed: e.exitAllowed, lat: e.lat, lon: e.lon,
          })))}::jsonb)
            AS v(ref text, stop_id text, entrance_type text, entry_allowed boolean, exit_allowed boolean, lat float8, lon float8)
          ON CONFLICT (source, ref) DO NOTHING
        `)
      }
    })
    log(`  ${source.id}: ${entrances.length} entrances`)
  }
}

/** Degrees either side of a point an entrance's street context is looked for in. */
const INTERSECTION_BOX = 0.001
const STREET_BOX = 0.0006
const ROAD_BOX = 0.0005
/** An agency entrance this close to an OSM one is the same opening. */
const AGENCY_MATCH_M = 10

/**
 * Highways within `box` degrees of a point. OFFSET 0 keeps the planner on the
 * highway-lines geometry index: pushed inside, a name filter picks the trigram
 * index instead, which costs ~100 ms a lookup in Manhattan.
 */
const highwaysNear = (point: SQL, box: number) => sql`
  SELECT name, geom, tags->>'highway' AS highway FROM geo_places
  WHERE geom_type = 'line' AND tags ? 'highway' AND geom && ST_Expand(${point}, ${box})
  OFFSET 0`

interface EntranceContextRow {
  id: string
  lng: number
  lat: number
  entrance: string | null
  ilng: number | null
  ilat: number | null
  idist: number | null
  streets: CornerStreet[] | null
  road: string | null
  road_dist: number | null
  a_entry: boolean | null
  a_exit: boolean | null
  a_source: string | null
}

/**
 * Each station entrance with what is needed to name it: the nearest named
 * intersection, the bearing of each of its streets there (measured over ~30 m
 * in Web Mercator, which keeps angles true), the nearest named road, and the
 * agency entrance it coincides with.
 */
async function entranceContext(q: Db): Promise<EntranceContextRow[]> {
  const agency = await relationAvailable('agency_entrances')
  return (await q.execute(sql`
    WITH e AS (
      SELECT id, centroid, tags->>'entrance' AS entrance
      FROM geo_places
      WHERE geom_type = 'point'
        AND (tags @> '{"railway":"subway_entrance"}' OR tags @> '{"railway":"train_station_entrance"}')
    )
    SELECT e.id, ST_X(e.centroid) AS lng, ST_Y(e.centroid) AS lat, e.entrance,
           i.lng AS ilng, i.lat AS ilat, i.distance_m AS idist, i.streets,
           r.name AS road, r.distance_m AS road_dist,
           ${agency ? sql`a.entry_allowed AS a_entry, a.exit_allowed AS a_exit, a.source AS a_source`
                    : sql`NULL::boolean AS a_entry, NULL::boolean AS a_exit, NULL::text AS a_source`}
    FROM e
    LEFT JOIN LATERAL (
      SELECT ST_X(x.centroid) AS lng, ST_Y(x.centroid) AS lat,
             ST_Distance(x.centroid::geography, e.centroid::geography) AS distance_m,
             (SELECT json_agg(json_build_object('name', sn, 'azimuth', w.az, 'distanceM', w.d))
              FROM unnest(x.names) sn
              LEFT JOIN LATERAL (
                SELECT degrees(ST_Azimuth(
                         ST_LineInterpolatePoint(g.l, greatest(ff.f - ff.step, 0)),
                         ST_LineInterpolatePoint(g.l, least(ff.f + ff.step, 1)))) AS az,
                       ST_Distance(r.geom::geography, e.centroid::geography) AS d
                FROM (${highwaysNear(sql`x.centroid`, STREET_BOX)}) r
                CROSS JOIN LATERAL (SELECT ST_Transform(ST_GeometryN(ST_LineMerge(r.geom), 1), 3857) AS l) g
                CROSS JOIN LATERAL (
                  SELECT ST_LineLocatePoint(g.l, ST_Transform(x.centroid, 3857)) AS f,
                         15 / NULLIF(ST_Length(g.l), 0) AS step
                ) ff
                WHERE r.name = sn
                  AND ST_GeometryType(g.l) = 'ST_LineString'
                  AND ff.step IS NOT NULL
                ORDER BY r.geom <-> x.centroid
                LIMIT 1
              ) w ON true) AS streets
      FROM geo_places x
      WHERE x.osm_type = 'X' AND x.centroid && ST_Expand(e.centroid, ${INTERSECTION_BOX})
      ORDER BY x.centroid <-> e.centroid
      LIMIT 1
    ) i ON true
    LEFT JOIN LATERAL (
      SELECT r.name, ST_Distance(r.geom::geography, e.centroid::geography) AS distance_m
      FROM (${highwaysNear(sql`e.centroid`, ROAD_BOX)}) r
      WHERE r.name IS NOT NULL
        AND r.highway IN ('trunk', 'primary', 'secondary', 'tertiary', 'unclassified',
                          'residential', 'living_street', 'pedestrian')
      ORDER BY r.geom <-> e.centroid
      LIMIT 1
    ) r ON true
    ${agency ? sql`
    LEFT JOIN LATERAL (
      SELECT entry_allowed, exit_allowed, source FROM agency_entrances a
      WHERE a.geom && ST_Expand(e.centroid, 0.0002)
        AND ST_DWithin(a.geom::geography, e.centroid::geography, ${AGENCY_MATCH_M})
      ORDER BY a.geom <-> e.centroid
      LIMIT 1
    ) a ON true` : sql.empty()}
  `)) as unknown as EntranceContextRow[]
}

/** Rebuild every entrance's corner label and entry/exit flags. Run after each OSM update. */
export function refreshEntranceLabels(opts: { fetchFn?: FetchFn; log?: Log } = {}) {
  return withMaintenance((q) => entranceLabelsRefresh(q, opts))
}

async function entranceLabelsRefresh(q: Db, opts: { fetchFn?: FetchFn; log?: Log }) {
  const log = opts.log ?? (() => {})
  await refreshAgencyEntrances(q, opts.fetchFn ?? fetch, log)

  const rows = await entranceContext(q)
  const labelled = rows.map((r) => {
    const access = entranceAccess(r.entrance, r.a_source ? { entryAllowed: r.a_entry, exitAllowed: r.a_exit, source: r.a_source } : null)
    return {
      id: r.id,
      corner_label: cornerLabel({
        entrance: { lng: r.lng, lat: r.lat },
        intersection: r.ilng != null && r.ilat != null && r.idist != null
          ? { lng: r.ilng, lat: r.ilat, distanceM: Number(r.idist), streets: r.streets ?? [] }
          : null,
        nearestRoad: r.road ? { name: r.road, distanceM: Number(r.road_dist) } : null,
      }),
      entry_allowed: access.entryAllowed,
      exit_allowed: access.exitAllowed,
      access_source: access.source,
    }
  })

  await q.transaction(async (tx) => {
    await tx.execute(sql`DELETE FROM entrance_labels`)
    for (const batch of chunks(labelled)) {
      await tx.execute(sql`
        INSERT INTO entrance_labels (id, corner_label, entry_allowed, exit_allowed, access_source)
        SELECT v.id, v.corner_label, v.entry_allowed, v.exit_allowed, v.access_source
        FROM jsonb_to_recordset(${JSON.stringify(batch)}::jsonb)
          AS v(id text, corner_label text, entry_allowed boolean, exit_allowed boolean, access_source text)
        ON CONFLICT (id) DO NOTHING
      `)
    }
  })

  const cornered = labelled.filter((l) => l.corner_label).length
  const restricted = labelled.filter((l) => l.entry_allowed === false || l.exit_allowed === false).length
  log(`  entrance labels: ${labelled.length} entrances, ${cornered} with a corner label, ${restricted} entry- or exit-only`)
  return { entrances: labelled.length, cornered, restricted }
}

// ── Readers ─────────────────────────────────────────────────────────

export const stopKey = (feedId: string, stopId: string) => `${feedId}\u0000${stopId}`

/** The signed direction for each (feed, stop) that has one. Absent tables read as no labels. */
export async function directionLabelsFor(
  stops: Array<{ feedId: string; stopId: string }>,
): Promise<Map<string, string>> {
  const out = new Map<string, string>()
  const unique = [...new Map(stops.map((s) => [stopKey(s.feedId, s.stopId), s])).values()]
  if (!unique.length || !(await relationAvailable('stop_direction_labels'))) return out

  const rows = (await db.execute(sql`
    SELECT l.feed_id, l.stop_id, l.label, l.source
    FROM stop_direction_labels l
    JOIN (VALUES ${sql.join(unique.map((s) => sql`(${s.feedId}::text, ${s.stopId}::text)`), sql`, `)}) AS q(feed_id, stop_id)
      ON q.feed_id = l.feed_id AND q.stop_id = l.stop_id
  `)) as unknown as Array<{ feed_id: string; stop_id: string } & DirectionLabelRow>

  const grouped = new Map<string, DirectionLabelRow[]>()
  for (const r of rows) {
    const key = stopKey(r.feed_id, r.stop_id)
    grouped.set(key, [...(grouped.get(key) ?? []), { label: r.label, source: r.source }])
  }
  for (const [key, labels] of grouped) {
    const label = pickDirectionLabel(labels)
    if (label) out.set(key, label)
  }
  return out
}

export interface EntranceLabel {
  cornerLabel: string | null
  entryAllowed: boolean | null
  exitAllowed: boolean | null
}

/** Corner label and entry/exit flags for each geo_places entrance id that has them. */
export async function entranceLabelsFor(ids: string[]): Promise<Map<string, EntranceLabel>> {
  const out = new Map<string, EntranceLabel>()
  const unique = [...new Set(ids)]
  if (!unique.length || !(await relationAvailable('entrance_labels'))) return out
  const rows = (await db.execute(sql`
    SELECT id, corner_label, entry_allowed, exit_allowed FROM entrance_labels
    WHERE id IN (${sql.join(unique.map((id) => sql`${id}`), sql`, `)})
  `)) as Array<{ id: string; corner_label: string | null; entry_allowed: boolean | null; exit_allowed: boolean | null }>
  for (const r of rows) {
    out.set(r.id, { cornerLabel: r.corner_label, entryAllowed: r.entry_allowed, exitAllowed: r.exit_allowed })
  }
  return out
}
