#!/usr/bin/env bun
/**
 * GTFS update watcher — detect which imported feeds have a newer version
 * published upstream, so the nightly job can refresh only what changed.
 *
 * GTFS static feeds are republished by agencies on no fixed cadence (a few times
 * a year up to ~weekly). Transitland tracks each feed's current version with a
 * content sha1 + fetched_at. We store the sha we last imported per feed
 * (gtfs_feeds.feed_version_sha); this script compares the stored sha against
 * Transitland's current sha and reports which feeds/regions drifted.
 *
 * It is intentionally self-contained (imports only `postgres` + global fetch) so
 * the host orchestrator can `docker cp` it into the running barrelman container
 * and run it with the baked image — no redeploy needed.
 *
 * Modes:
 *   --check    (default) Compare stored vs upstream sha. Prints a human summary
 *              plus machine-readable lines the host wrapper parses:
 *                 BASELINE=1                 (no feed has a stored sha yet)
 *                 CHANGED_REGIONS=nyc,nc     (regions with >=1 updated feed)
 *              Does NOT modify the database.
 *   --record   Fetch current upstream shas and write them to gtfs_feeds. Used to
 *              establish the baseline on first run, and to commit the new state
 *              after a successful re-import.
 *
 * Env: DATABASE_URL (required), TRANSITLAND_API_KEY (required).
 */

import postgres from 'postgres'

const MODE = process.argv.includes('--record') ? 'record' : 'check'
const API_KEY = process.env.TRANSITLAND_API_KEY ?? ''
const DB_URL = process.env.DATABASE_URL ?? ''
const BATCH = 8

if (!DB_URL) { console.error('gtfs-watch: DATABASE_URL is required'); process.exit(2) }
if (!API_KEY) { console.error('gtfs-watch: TRANSITLAND_API_KEY is required'); process.exit(2) }

interface FeedRow { feed_id: string; onestop_id: string; region: string | null; feed_version_sha: string | null }
interface Upstream { sha1: string | null; fetchedAt: string | null }

/** Query Transitland for a feed's current version sha1 + fetched_at. */
async function upstreamVersion(onestopId: string): Promise<Upstream | null> {
  const url = `https://transit.land/api/v2/rest/feeds/${encodeURIComponent(onestopId)}?apikey=${API_KEY}`
  try {
    const res = await fetch(url)
    if (!res.ok) return null
    const data = await res.json() as any
    const fv = data?.feeds?.[0]?.feed_state?.feed_version
    if (!fv) return null
    return { sha1: fv.sha1 ?? null, fetchedAt: fv.fetched_at ?? null }
  } catch {
    return null
  }
}

async function main() {
  const sql = postgres(DB_URL, { max: 4 })
  try {
    // Self-heal schema: the sha columns may not exist on older installs.
    await sql`ALTER TABLE gtfs_feeds ADD COLUMN IF NOT EXISTS feed_version_sha TEXT`
    await sql`ALTER TABLE gtfs_feeds ADD COLUMN IF NOT EXISTS feed_version_fetched_at TIMESTAMPTZ`

    const feeds = await sql<FeedRow[]>`
      SELECT feed_id, onestop_id, region, feed_version_sha
      FROM gtfs_feeds
      WHERE onestop_id IS NOT NULL AND onestop_id <> ''
      ORDER BY region, feed_id
    `

    if (feeds.length === 0) {
      console.log('gtfs-watch: no feeds with an onestop_id to watch')
      console.log('BASELINE=0')
      console.log('CHANGED_REGIONS=')
      return
    }

    const anyStored = feeds.some(f => f.feed_version_sha)
    const isBaseline = !anyStored

    // Fetch upstream versions in small batches to be gentle on the API.
    const upstream = new Map<string, Upstream | null>()
    for (let i = 0; i < feeds.length; i += BATCH) {
      const slice = feeds.slice(i, i + BATCH)
      const results = await Promise.all(slice.map(f => upstreamVersion(f.onestop_id)))
      slice.forEach((f, j) => upstream.set(f.onestop_id, results[j]))
    }

    if (MODE === 'record') {
      let recorded = 0
      for (const f of feeds) {
        const u = upstream.get(f.onestop_id)
        if (!u?.sha1) continue
        await sql`
          UPDATE gtfs_feeds
          SET feed_version_sha = ${u.sha1},
              feed_version_fetched_at = ${u.fetchedAt ? new Date(u.fetchedAt) : null}
          WHERE onestop_id = ${f.onestop_id}
        `
        recorded++
      }
      console.log(`gtfs-watch: recorded ${recorded}/${feeds.length} feed versions`)
      return
    }

    // --check: compare stored vs upstream.
    const changed: FeedRow[] = []
    let unchanged = 0
    let unknown = 0
    for (const f of feeds) {
      const u = upstream.get(f.onestop_id)
      if (!u?.sha1) { unknown++; continue }
      if (isBaseline) continue
      if (f.feed_version_sha !== u.sha1) changed.push(f)
      else unchanged++
    }

    const regions = Array.from(
      new Set(changed.map(f => f.region).filter((r): r is string => Boolean(r))),
    ).sort()

    console.log(
      `gtfs-watch: ${feeds.length} feeds — ${isBaseline ? 'baseline (no prior shas)' : `${changed.length} changed, ${unchanged} unchanged`}, ${unknown} unresolved`,
    )
    for (const f of changed) {
      console.log(`  changed: ${f.feed_id} [${f.region ?? '?'}] ${f.feed_version_sha?.slice(0, 8) ?? 'none'} -> ${upstream.get(f.onestop_id)?.sha1?.slice(0, 8)}`)
    }

    // Machine-readable lines for the host wrapper.
    console.log(`BASELINE=${isBaseline ? 1 : 0}`)
    console.log(`CHANGED_REGIONS=${regions.join(',')}`)
  } finally {
    await sql.end({ timeout: 5 })
  }
}

main().catch((err) => {
  console.error('gtfs-watch: fatal', err instanceof Error ? err.message : err)
  process.exit(1)
})
