#!/usr/bin/env bun
/**
 * Portolan street extracts, cut from PostGIS.
 *
 * Portolan draws bus routes only where the feed entry names a `streets`
 * extract — the highway layer its matcher snaps bus patterns onto. Without
 * one a feed builds rail-only, silently: the log says "streets configured but
 * missing … building rail-only" and the bus routes simply never appear.
 *
 * Upstream portolan fetches that layer from Overpass (`tools/feed.sh streets`),
 * which takes minutes per city and connection-throttles burst clients — its own
 * docs rule it out at continent scale (docs/FEEDS.md). But barrelman already
 * holds the same OpenStreetMap data that Overpass would serve, in geo_places,
 * and osm2pgsql's flex output preserves OSM way identity one-to-one, so it is a
 * lossless source for this. Way identity is the load-bearing part: portolan's
 * track graph welds ways only where they touch exactly and MATCH hands off
 * between them by id, so a source that split or merged ways would break bus
 * matching in ways that are hard to see on a map.
 *
 * Measured over Denver: 134,794 ways in 5.8s, against minutes via Overpass.
 *
 *   bun run import/portolan-streets.ts --feeds=rtd,trimet
 *   bun run import/portolan-streets.ts --all-bus
 *
 * Flags (--flag=value form, matching the other import scripts):
 *   --workspace   portolan workspace root (default $PORTOLAN_WORKSPACE)
 *   --feeds       comma list of portolan feed keys
 *   --all-bus     every feed whose window holds bus routes
 *   --min-ways    skip a feed yielding fewer than this (default 1000)
 *   --force       regenerate even when the extract already exists
 */
import { parseArgs } from 'util'
import { existsSync, mkdirSync, renameSync, writeFileSync, readFileSync, statSync } from 'fs'
import { dirname, join, resolve } from 'path'
import { connection } from '../src/db'
import { envString } from '../src/config/env'

/**
 * The drivable ways a bus can run on, mirroring the Overpass query in
 * portolan's tools/feed.sh exactly so a feed can move between the two
 * sources without its matching changing. Footways and cycleways are
 * deliberately absent: a bus must never match onto a pedestrian alley that
 * happens to parallel its street.
 */
const HIGHWAY_KINDS = [
  'motorway', 'trunk', 'primary', 'secondary', 'tertiary',
  'unclassified', 'residential', 'living_street', 'busway', 'bus_guideway',
  'motorway_link', 'trunk_link', 'primary_link', 'secondary_link', 'tertiary_link',
]

/** The tag subset portolan's osm.Load reads for street ways. */
const TAGS = ['highway', 'service', 'bridge', 'tunnel', 'layer', 'oneway']

export interface FeedEntry {
  bbox?: [number, number, number, number]
  streets?: string
  [k: string]: unknown
}

export interface StreetResult {
  feed: string
  ways: number
  path: string
  skipped?: 'no-bbox' | 'too-few-ways' | 'exists'
}

/**
 * Stream one feed's street ways straight out of PostGIS into a GeoJSON file.
 *
 * Written incrementally rather than assembled in memory: a metro-scale street
 * grid is 10-50x its rail ways (Denver is 61 MB), and the importer shares a
 * host with the API.
 */
export async function extractStreets(
  feed: string,
  bbox: [number, number, number, number],
  outPath: string,
): Promise<number> {
  const [w, s, e, n] = bbox
  mkdirSync(dirname(outPath), { recursive: true })
  const tmp = `${outPath}.tmp`
  const chunks: string[] = ['{"type":"FeatureCollection","features":[']
  let count = 0

  const rows = await connection`
    SELECT osm_id,
           tags->>'highway'  AS highway,
           tags->>'service'  AS service,
           tags->>'bridge'   AS bridge,
           tags->>'tunnel'   AS tunnel,
           tags->>'layer'    AS layer,
           tags->>'oneway'   AS oneway,
           ST_AsGeoJSON(geom) AS geom
      FROM geo_places
     WHERE osm_type = 'W'
       AND geom_type = 'line'
       AND geom && ST_MakeEnvelope(${w}, ${s}, ${e}, ${n}, 4326)
       AND tags->>'highway' = ANY(${HIGHWAY_KINDS})`

  for (const r of rows as any[]) {
    const props: Record<string, unknown> = {}
    for (const t of TAGS) props[t] = r[t] ?? null
    chunks.push(
      `${count ? ',' : ''}{"type":"Feature","id":"way/${r.osm_id}",` +
        `"properties":${JSON.stringify(props)},"geometry":${r.geom}}`,
    )
    count++
  }
  chunks.push(']}\n')
  writeFileSync(tmp, chunks.join(''))
  renameSync(tmp, outPath)
  return count
}

/** Register the extract on the feed entry so portolan's next build reads it. */
export function registerStreets(configPath: string, feed: string, relPath: string): void {
  const cfg = JSON.parse(readFileSync(configPath, 'utf8'))
  if (!cfg.feeds?.[feed]) throw new Error(`unknown portolan feed "${feed}"`)
  cfg.feeds[feed].streets = relPath
  writeFileSync(configPath, JSON.stringify(cfg))
}

if (import.meta.main) {
  const { values } = parseArgs({
    args: Bun.argv.slice(2),
    options: {
      workspace: { type: 'string' },
      feeds: { type: 'string' },
      'all-bus': { type: 'boolean' },
      'min-ways': { type: 'string' },
      force: { type: 'boolean' },
    },
    strict: false,
  })

  const workspace = resolve(
    String(values.workspace || envString('PORTOLAN_WORKSPACE', './data/portolan')),
  )
  const configPath = join(workspace, 'portolan.json')
  if (!existsSync(configPath)) {
    console.error(`No portolan.json at ${configPath} — set PORTOLAN_WORKSPACE.`)
    process.exit(2)
  }
  const cfg = JSON.parse(readFileSync(configPath, 'utf8'))
  const minWays = Number(values['min-ways'] ?? 1000)

  let keys: string[] = []
  if (values.feeds) keys = String(values.feeds).split(',').map((s) => s.trim()).filter(Boolean)
  else if (values['all-bus']) keys = Object.keys(cfg.feeds ?? {})
  else {
    console.error('Pass --feeds=<keys> or --all-bus.')
    process.exit(2)
  }

  const results: StreetResult[] = []
  for (const feed of keys) {
    const entry = cfg.feeds?.[feed] as FeedEntry | undefined
    if (!entry?.bbox) {
      console.log(`${feed}: no bbox — skipped`)
      results.push({ feed, ways: 0, path: '', skipped: 'no-bbox' })
      continue
    }
    const rel = entry.streets || `build/${feed}-streets.geojson`
    const out = join(workspace, rel)
    if (existsSync(out) && statSync(out).size > 200 && !values.force) {
      console.log(`${feed}: extract present — skipped (use --force)`)
      results.push({ feed, ways: 0, path: out, skipped: 'exists' })
      continue
    }
    const started = Date.now()
    const ways = await extractStreets(feed, entry.bbox, out)
    const secs = ((Date.now() - started) / 1000).toFixed(1)

    // A window outside the imported OSM region yields almost nothing. Drawing
    // buses on an empty street layer is worse than not drawing them, so the
    // extract is discarded rather than registered — and reported, never
    // silently dropped.
    if (ways < minWays) {
      console.log(`${feed}: ${ways} ways (<${minWays}) — window outside imported OSM, not registered`)
      results.push({ feed, ways, path: out, skipped: 'too-few-ways' })
      continue
    }
    registerStreets(configPath, feed, rel)
    console.log(`${feed}: ${ways} ways in ${secs}s -> ${rel}`)
    results.push({ feed, ways, path: out })
  }

  const ok = results.filter((r) => !r.skipped)
  console.log(
    `\n${ok.length} feed(s) registered, ${results.length - ok.length} skipped. ` +
      `Run portolan-patch-import for these keys to draw their buses.`,
  )
  await connection.end()
  process.exit(0)
}
