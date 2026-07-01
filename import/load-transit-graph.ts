#!/usr/bin/env bun
/**
 * Load a LOOM line-graph (GeoJSON from `gtfs2graph | topo | loom`) into the
 * transit_graph_* tables, mapping each LOOM line back to our GTFS route (for
 * id + colour, incl. manual overrides). Then refresh the offset matview.
 *
 * Usage:
 *   bun run import/load-transit-graph.ts \
 *     --geojson ./nyc-subway-loom.json --build-key nyc:subway --feed 5 \
 *     --mode subway --route-type 1
 */
import { readFileSync } from 'fs'
import { parseArgs } from 'util'
import { sql } from 'drizzle-orm'
import { db, ensureTransitGraphSchema } from '../src/db'

const { values: args } = parseArgs({
  options: {
    geojson: { type: 'string' },
    'build-key': { type: 'string' },
    feed: { type: 'string' },
    mode: { type: 'string', default: '' },
    'route-type': { type: 'string' },
  },
})

const geojsonPath = args.geojson!
const buildKey = args['build-key']!
const feedId = args.feed!
const mode = args.mode!
const routeType = args['route-type'] ? parseInt(args['route-type']) : null

const esc = (v: string) => v.replace(/'/g, "''")
const q = (v: string | null | undefined) => (v == null ? 'NULL' : `'${esc(v)}'`)

async function main() {
  if (!geojsonPath || !buildKey || !feedId) {
    console.error('Required: --geojson, --build-key, --feed')
    process.exit(1)
  }

  await ensureTransitGraphSchema()

  const geo = JSON.parse(readFileSync(geojsonPath, 'utf8'))
  const nodes = geo.features.filter((f: any) => f.geometry?.type === 'Point')
  const edges = geo.features.filter((f: any) => f.geometry?.type === 'LineString')
  console.log(`Loaded ${nodes.length} nodes, ${edges.length} edges for build '${buildKey}'`)

  // Map LOOM line label → our GTFS route (id, colours, type). LOOM's `label`
  // is the route_short_name; our DB row carries any manual override.
  const routeRows: any = await db.execute(sql.raw(
    `SELECT route_id, route_short_name, route_type, route_color, route_text_color
     FROM gtfs_routes WHERE feed_id = ${q(feedId)}`,
  ))
  const routeByName = new Map<string, any>()
  for (const r of routeRows) routeByName.set(r.route_short_name, r)

  // Replace this build.
  await db.execute(sql.raw(`DELETE FROM transit_graph_nodes WHERE build_key = ${q(buildKey)}`))
  await db.execute(sql.raw(`DELETE FROM transit_graph_edges WHERE build_key = ${q(buildKey)}`)) // cascades edge_lines

  // Nodes.
  for (let i = 0; i < nodes.length; i += 200) {
    const chunk = nodes.slice(i, i + 200)
    const values = chunk.map((n: any) => {
      const [lng, lat] = n.geometry.coordinates
      const p = n.properties || {}
      return `(${q(buildKey)}, ${q(p.id)}, ${q(p.station_id)}, ${q(p.station_label)}, ST_SetSRID(ST_MakePoint(${lng}, ${lat}), 4326))`
    }).join(',\n')
    await db.execute(sql.raw(
      `INSERT INTO transit_graph_nodes (build_key, loom_id, station_id, station_label, geom) VALUES ${values}`,
    ))
  }

  // Edges (geometry from the GeoJSON LineString).
  for (let i = 0; i < edges.length; i += 100) {
    const chunk = edges.slice(i, i + 100)
    const values = chunk.map((e: any) => {
      const p = e.properties || {}
      const geomJson = esc(JSON.stringify(e.geometry))
      const lineCount = Array.isArray(p.lines) ? p.lines.length : 0
      return `(${q(buildKey)}, ${q(p.id)}, ${lineCount}, ST_SetSRID(ST_GeomFromGeoJSON('${geomJson}'), 4326))`
    }).join(',\n')
    await db.execute(sql.raw(
      `INSERT INTO transit_graph_edges (build_key, loom_id, line_count, geom) VALUES ${values}`,
    ))
  }

  // Map loom edge id → our serial id.
  const edgeRows: any = await db.execute(sql.raw(
    `SELECT id, loom_id FROM transit_graph_edges WHERE build_key = ${q(buildKey)}`,
  ))
  const edgeIdByLoom = new Map<string, number>()
  for (const r of edgeRows) edgeIdByLoom.set(r.loom_id, r.id)

  // Edge lines — one row per (edge, slot). Slot = index in LOOM's ordered list.
  const lineValues: string[] = []
  for (const e of edges) {
    const p = e.properties || {}
    const edgeId = edgeIdByLoom.get(p.id)
    if (!edgeId || !Array.isArray(p.lines)) continue
    p.lines.forEach((line: any, slot: number) => {
      const label = String(line.label ?? '')
      const route = routeByName.get(label)
      const color = route?.route_color || line.color || ''
      lineValues.push(
        `(${edgeId}, ${slot}, ${q(feedId)}, ${q(route?.route_id ?? label)}, ${q(label)}, ${route?.route_type ?? (routeType ?? 'NULL')}, ${q(color)}, ${q(route?.route_text_color ?? '')})`,
      )
    })
  }
  for (let i = 0; i < lineValues.length; i += 500) {
    await db.execute(sql.raw(
      `INSERT INTO transit_graph_edge_lines
        (edge_id, slot, feed_id, route_id, route_short_name, route_type, route_color, route_text_color)
       VALUES ${lineValues.slice(i, i + 500).join(',\n')}`,
    ))
  }
  console.log(`Inserted ${lineValues.length} edge-line rows`)

  // Record the build.
  await db.execute(sql.raw(`
    INSERT INTO transit_graph_builds (build_key, feed_id, mode, route_type, built_at)
    VALUES (${q(buildKey)}, ${q(feedId)}, ${q(mode)}, ${routeType ?? 'NULL'}, NOW())
    ON CONFLICT (build_key) DO UPDATE SET
      feed_id = EXCLUDED.feed_id, mode = EXCLUDED.mode,
      route_type = EXCLUDED.route_type, built_at = NOW()
  `))

  console.log('Done. Now refresh the offset view: import/create-transit-lines-offset.sql')
  process.exit(0)
}

main().catch((err) => {
  console.error('Fatal error:', err)
  process.exit(1)
})
