/**
 * Declarative catalog of every runnable script/task in barrelman.
 *
 * This manifest is the single source of truth for the admin console: it drives
 * the "Scripts" UI (what buttons exist, what parameters each takes, which ones
 * are destructive) and the job runner (how to actually execute each task).
 *
 * The whole manifest is serialised to JSON and sent to the browser, so it must
 * stay PURE DATA — no functions. Execution behaviour is expressed declaratively
 * via the `exec` field:
 *   - kind:'process'  → spawn `command args...` (bash / bun / psql), stream logs
 *   - kind:'internal' → call a server-side handler (see job-runner.service.ts),
 *                       used for SQL run in-process via the existing db client
 *
 * User-supplied parameters are applied three ways (`apply`):
 *   - 'flag'       → appended as `--name value` (or bare `--name` for booleans)
 *   - 'positional' → appended as a raw positional argument
 *   - 'env'        → merged into the child process environment
 */

export type ScriptCategory =
  | 'osm'
  | 'transit'
  | 'gbfs'
  | 'search'
  | 'routing'
  | 'database'
  | 'config'

export type DangerLevel = 'safe' | 'caution' | 'destructive'
export type ParamApply = 'flag' | 'env' | 'positional'
export type ParamType = 'string' | 'number' | 'boolean' | 'select'

export interface ScriptParam {
  name: string
  label: string
  type: ParamType
  apply: ParamApply
  /** Override the emitted flag (defaults to `--<name>`). Only for apply:'flag'. */
  flag?: string
  /** Environment variable name. Only for apply:'env'. */
  envVar?: string
  default?: string | number | boolean
  options?: { label: string; value: string }[]
  placeholder?: string
  required?: boolean
  /** Mask the value in the UI (API keys, secrets). */
  secret?: boolean
  description?: string
}

export type ScriptExec =
  | { kind: 'process'; command: string; args: string[] }
  | { kind: 'internal'; handler: string }

export interface ScriptDef {
  id: string
  name: string
  description: string
  category: ScriptCategory
  danger: DangerLevel
  /** Long jobs stream logs and are expected to run for minutes+. */
  longRunning: boolean
  /** Require an explicit confirmation dialog before running. */
  confirm: boolean
  /** Only one instance of this script may run at a time. */
  exclusive?: boolean
  exec: ScriptExec
  params?: ScriptParam[]
  /** Static environment additions applied to every run. */
  env?: Record<string, string>
  /** Source file (for the UI "view source" reference). */
  source?: string
  /** Freeform notes surfaced in the UI. */
  notes?: string
}

export const CATEGORY_LABELS: Record<ScriptCategory, string> = {
  osm: 'OSM Import & Updates',
  transit: 'GTFS / Transit',
  gbfs: 'GBFS / Micromobility',
  search: 'Search Enrichment',
  routing: 'Routing Graphs',
  database: 'Database & Migration',
  config: 'Config Generation',
}

export const CATEGORY_ORDER: ScriptCategory[] = [
  'osm',
  'transit',
  'gbfs',
  'search',
  'routing',
  'database',
  'config',
]

const REGIONS_PARAM: ScriptParam = {
  name: 'REGIONS',
  label: 'Regions override',
  type: 'string',
  apply: 'env',
  envVar: 'REGIONS',
  placeholder: 'north-carolina,nyc-metro  (blank = use .env)',
  description: 'Comma-separated region keys, or "global". Leave blank to use the server default.',
}

export const SCRIPTS: ScriptDef[] = [
  // ── OSM Import & Updates ──────────────────────────────────────────────
  {
    id: 'osm-full-import',
    name: 'Full OSM Import',
    description:
      'Download the configured region PBF(s), run the osm2pgsql flex import, then all post-processing (codes, abbreviations, intersections, parent context, tsvectors) and rebuild the GraphHopper graph. This is a full reload.',
    category: 'osm',
    danger: 'destructive',
    longRunning: true,
    confirm: true,
    exclusive: true,
    exec: { kind: 'process', command: 'bash', args: ['scripts/run-import.sh'] },
    params: [REGIONS_PARAM],
    source: 'scripts/run-import.sh',
    notes:
      'osm2pgsql --create drops and recreates the geo_places tables. Expect 20–40+ minutes for a US state; longer for larger regions.',
  },
  {
    id: 'osm-update',
    name: 'OSM Update',
    description:
      'Apply an incremental replication diff (fast) or re-run a full re-import, then re-run incremental post-processing and rebuild the routing graph.',
    category: 'osm',
    danger: 'caution',
    longRunning: true,
    confirm: true,
    exclusive: true,
    exec: { kind: 'process', command: 'bash', args: ['scripts/update-osm.sh'] },
    params: [
      {
        name: 'UPDATE_MODE',
        label: 'Update mode',
        type: 'select',
        apply: 'env',
        envVar: 'UPDATE_MODE',
        default: 'replication',
        options: [
          { label: 'Replication (incremental diff — fast)', value: 'replication' },
          { label: 'Full (re-download + re-import — destructive)', value: 'full' },
        ],
      },
    ],
    source: 'scripts/update-osm.sh',
    notes: 'Full mode is a destructive re-import. Replication requires init-replication to have been run once.',
  },
  {
    id: 'osm-init-replication',
    name: 'Initialize Replication State',
    description:
      'One-time setup of osm2pgsql replication state after the first full import. Records the current Geofabrik replication sequence so incremental updates can begin.',
    category: 'osm',
    danger: 'caution',
    longRunning: false,
    confirm: true,
    exec: { kind: 'process', command: 'bash', args: ['scripts/init-replication.sh'] },
    source: 'scripts/init-replication.sh',
  },
  {
    id: 'osm-stop-areas',
    name: 'Import Stop-Area Relations',
    description:
      'Extract public_transport=stop_area relations from the OSM PBF and load stop_area_members (Tier 0 of nearest-entrance search).',
    category: 'osm',
    danger: 'caution',
    longRunning: false,
    confirm: true,
    exec: { kind: 'process', command: 'bash', args: ['scripts/import-stop-areas.sh'] },
    source: 'scripts/import-stop-areas.sh',
    notes: 'TRUNCATEs stop_area_members then atomically rebuilds it.',
  },

  // ── GTFS / Transit ────────────────────────────────────────────────────
  {
    id: 'gtfs-download',
    name: 'Download & Import GTFS',
    description:
      'For each region: fetch the feed list from Transitland, download GTFS ZIPs, import stops/routes into PostGIS, compute walking transfers via GraphHopper, and generate the MOTIS config.',
    category: 'transit',
    danger: 'caution',
    longRunning: true,
    confirm: true,
    exclusive: true,
    exec: { kind: 'process', command: 'bash', args: ['scripts/download-gtfs.sh'] },
    params: [
      {
        name: 'TRANSITLAND_API_KEY',
        label: 'Transitland API key',
        type: 'string',
        apply: 'env',
        envVar: 'TRANSITLAND_API_KEY',
        secret: true,
        placeholder: 'tlk_…  (blank = use server env)',
        description: 'Required unless already set in the server environment.',
      },
      REGIONS_PARAM,
      {
        name: 'GTFS_REGION',
        label: 'Single region override',
        type: 'string',
        apply: 'env',
        envVar: 'GTFS_REGION',
        placeholder: 'e.g. nc, nyc, global',
      },
    ],
    source: 'scripts/download-gtfs.sh',
  },
  {
    id: 'gtfs-import',
    name: 'GTFS Importer (advanced)',
    description:
      'Direct invocation of the core GTFS pipeline with fine-grained flags — download, parse, derive trip patterns, import, compute transfers, inject Fares v2.',
    category: 'transit',
    danger: 'caution',
    longRunning: true,
    confirm: true,
    exclusive: true,
    exec: { kind: 'process', command: 'bun', args: ['run', 'import/import-gtfs.ts'] },
    params: [
      { name: 'region', label: 'Region', type: 'string', apply: 'flag', flag: '--region', default: 'nc' },
      { name: 'api-key', label: 'Transitland API key', type: 'string', apply: 'flag', flag: '--api-key', secret: true, placeholder: 'tlk_…' },
      { name: 'output-dir', label: 'Output dir', type: 'string', apply: 'flag', flag: '--output-dir', default: './data/gtfs' },
      { name: 'max-feeds', label: 'Max feeds', type: 'number', apply: 'flag', flag: '--max-feeds', placeholder: 'blank = all' },
      { name: 'transfer-distance', label: 'Transfer distance (m)', type: 'number', apply: 'flag', flag: '--transfer-distance', default: 500 },
      { name: 'skip-download', label: 'Skip download (use local zips)', type: 'boolean', apply: 'flag', flag: '--skip-download', default: false },
      { name: 'skip-transfers', label: 'Skip transfer computation', type: 'boolean', apply: 'flag', flag: '--skip-transfers', default: false },
    ],
    source: 'import/import-gtfs.ts',
  },
  {
    id: 'gtfs-shapes',
    name: 'Import GTFS Shapes',
    description: 'Re-import shapes.txt geometry for all (or selected) feeds from local ZIPs and link routes to shapes.',
    category: 'transit',
    danger: 'safe',
    longRunning: true,
    confirm: false,
    exec: { kind: 'process', command: 'bun', args: ['run', 'import/import-all-shapes.ts'] },
    params: [
      { name: 'feeds', label: 'Feed IDs (space-separated)', type: 'string', apply: 'positional', placeholder: 'blank = all feeds' },
    ],
    source: 'import/import-all-shapes.ts',
  },
  {
    id: 'gtfs-backfill-rt',
    name: 'Backfill GTFS-RT URLs',
    description: 'Discover GTFS-RT vehicle/trip-update/alert URLs from Transitland for feeds missing them.',
    category: 'transit',
    danger: 'safe',
    longRunning: false,
    confirm: false,
    exec: { kind: 'process', command: 'bun', args: ['run', 'import/backfill-rt-urls.ts'] },
    params: [
      { name: 'api-key', label: 'Transitland API key', type: 'string', apply: 'env', envVar: 'TRANSITLAND_API_KEY', secret: true, placeholder: 'tlk_… (blank = server env)' },
      { name: 'feed-id', label: 'Single feed ID', type: 'string', apply: 'flag', flag: '--feed-id', placeholder: 'blank = all feeds' },
      { name: 'dry-run', label: 'Dry run (preview only)', type: 'boolean', apply: 'flag', flag: '--dry-run', default: true },
    ],
    source: 'import/backfill-rt-urls.ts',
  },
  {
    id: 'gtfs-backfill-transfers',
    name: 'Backfill Transfers',
    description: 'Backfill gtfs_transfers from transfers.txt inside local feed ZIPs (for DBs imported before transfers were captured).',
    category: 'transit',
    danger: 'safe',
    longRunning: false,
    confirm: false,
    exec: { kind: 'process', command: 'bun', args: ['run', 'import/backfill-transfers.ts'] },
    params: [
      { name: 'dir', label: 'GTFS dir', type: 'string', apply: 'flag', flag: '--dir', default: './data/gtfs' },
    ],
    source: 'import/backfill-transfers.ts',
  },
  {
    id: 'gtfs-backfill-patterns',
    name: 'Backfill Trip Patterns',
    description: 'Rebuild gtfs_trip_patterns for already-imported feeds without a full re-import. Safe to re-run.',
    category: 'transit',
    danger: 'safe',
    longRunning: true,
    confirm: false,
    exec: { kind: 'process', command: 'bun', args: ['run', 'import/backfill-trip-patterns.ts'] },
    params: [
      { name: 'dir', label: 'GTFS dir', type: 'string', apply: 'flag', flag: '--dir', default: './data/gtfs' },
      { name: 'feeds', label: 'Feed IDs (space-separated)', type: 'string', apply: 'positional', placeholder: 'blank = all feeds' },
    ],
    source: 'import/backfill-trip-patterns.ts',
  },
  {
    id: 'gtfs-inject-fares',
    name: 'Inject Fares v2',
    description: 'Convert GTFS Fares v1 → v2 inside feed ZIPs so MOTIS can compute fares. Rewrites local ZIPs.',
    category: 'transit',
    danger: 'caution',
    longRunning: false,
    confirm: true,
    exec: { kind: 'process', command: 'bun', args: ['run', 'import/inject-fares-v2.ts'] },
    params: [
      { name: 'dir', label: 'GTFS dir', type: 'string', apply: 'flag', flag: '--dir', default: './data/gtfs' },
      { name: 'dry-run', label: 'Dry run (preview only)', type: 'boolean', apply: 'flag', flag: '--dry-run', default: true },
    ],
    source: 'import/inject-fares-v2.ts',
  },
  {
    id: 'motis-config',
    name: 'Generate MOTIS Config',
    description: 'Regenerate motis/config.yml from the gtfs_feeds table (feeds + GTFS-RT URLs) without re-downloading feeds.',
    category: 'transit',
    danger: 'safe',
    longRunning: false,
    confirm: false,
    exec: { kind: 'process', command: 'bun', args: ['run', 'import/generate-motis-config.ts'] },
    params: [
      { name: 'output', label: 'Output path', type: 'string', apply: 'flag', flag: '--output', default: './motis/config.yml' },
      { name: 'rt-update-interval', label: 'RT poll interval (s)', type: 'number', apply: 'flag', flag: '--rt-update-interval', placeholder: 'blank = default 60' },
      { name: 'no-gbfs', label: 'Exclude GBFS', type: 'boolean', apply: 'flag', flag: '--no-gbfs', default: false },
    ],
    source: 'import/generate-motis-config.ts',
  },
  {
    id: 'transit-station-links',
    name: 'Rebuild Station Links',
    description: 'Rebuild materialized views linking GTFS stations to nearby OSM infrastructure (entrances, buildings). Powers /transit/station.',
    category: 'transit',
    danger: 'caution',
    longRunning: true,
    confirm: true,
    exec: { kind: 'internal', handler: 'sql:create-station-links.sql' },
    source: 'import/create-station-links.sql',
    notes: 'DROPs and rebuilds materialized views. Re-run after OSM + GTFS imports.',
  },
  {
    id: 'transit-views',
    name: 'Rebuild Transit Tile Views',
    description: 'Recreate the transit vector-tile source views (station buildings, etc.) that Martin serves.',
    category: 'transit',
    danger: 'caution',
    longRunning: false,
    confirm: true,
    exec: { kind: 'internal', handler: 'sql:create-transit-views.sql' },
    source: 'import/create-transit-views.sql',
    notes: 'Uses DROP VIEW … CASCADE — dependent views are dropped and rebuilt.',
  },

  // ── GBFS ──────────────────────────────────────────────────────────────
  {
    id: 'gbfs-import',
    name: 'Import GBFS Systems',
    description: 'Fetch the MobilityData GBFS systems catalog, resolve each auto-discovery URL, and import station locations.',
    category: 'gbfs',
    danger: 'safe',
    longRunning: true,
    confirm: false,
    exec: { kind: 'process', command: 'bun', args: ['run', 'import/import-gbfs-systems.ts'] },
    params: [
      { name: 'country', label: 'Country filter', type: 'string', apply: 'flag', flag: '--country', placeholder: 'e.g. US' },
      { name: 'bbox', label: 'Bounding box', type: 'string', apply: 'flag', flag: '--bbox', placeholder: 'w,s,e,n (blank = REGIONS bbox)' },
    ],
    source: 'import/import-gbfs-systems.ts',
  },

  // ── Search Enrichment ─────────────────────────────────────────────────
  {
    id: 'search-embed',
    name: 'Generate Embeddings',
    description: 'Generate semantic embeddings for all named POIs via Ollama (nomic-embed-text). Re-runnable — skips already-embedded rows.',
    category: 'search',
    danger: 'safe',
    longRunning: true,
    confirm: false,
    exclusive: true,
    exec: { kind: 'process', command: 'bun', args: ['run', 'import/embed-places.ts'] },
    source: 'import/embed-places.ts',
    notes: 'Requires Ollama running. Can take hours on large datasets.',
  },
  {
    id: 'search-intersections',
    name: 'Generate Intersections',
    description: 'Find points where differently-named roads cross and insert synthetic intersection rows for search.',
    category: 'search',
    danger: 'caution',
    longRunning: true,
    confirm: true,
    exec: { kind: 'internal', handler: 'sql:generate-intersections.sql' },
    source: 'import/generate-intersections.sql',
    notes: 'Deletes stale intersection rows (osm_type=X) then rebuilds.',
  },

  // ── Routing Graphs ────────────────────────────────────────────────────
  {
    id: 'routing-graphhopper',
    name: 'Rebuild GraphHopper',
    description: 'Wipe the GraphHopper graph cache and restart the container so it re-imports the PBF and rebuilds the routing graph.',
    category: 'routing',
    danger: 'caution',
    longRunning: true,
    confirm: true,
    exec: { kind: 'process', command: 'bash', args: ['scripts/rebuild-graphhopper.sh'] },
    source: 'scripts/rebuild-graphhopper.sh',
    notes: 'The script returns quickly; the actual graph build runs in the container. Watch `docker logs -f barrelman-graphhopper`.',
  },
  {
    id: 'routing-valhalla',
    name: 'Rebuild Valhalla',
    description: 'Wipe Valhalla tiles, regenerate + patch valhalla.json, and restart the container to rebuild tiles from the PBF.',
    category: 'routing',
    danger: 'caution',
    longRunning: true,
    confirm: true,
    exec: { kind: 'process', command: 'bash', args: ['scripts/rebuild-valhalla.sh'] },
    source: 'scripts/rebuild-valhalla.sh',
  },
  {
    id: 'routing-motis-osm',
    name: 'Prepare MOTIS OSM',
    description: 'Produce region-transit.osm.pbf by synthesizing platform connector ways so MOTIS can route to underground platforms.',
    category: 'routing',
    danger: 'safe',
    longRunning: true,
    confirm: false,
    exec: { kind: 'process', command: 'bash', args: ['scripts/prepare-motis-osm.sh'] },
    source: 'scripts/prepare-motis-osm.sh',
  },

  // ── Database & Migration (in-process via existing admin service) ───────
  {
    id: 'db-full-migration',
    name: 'Run Full Migration',
    description: 'Post-import → generate codes → abbreviations → resolve parent context → rebuild tsvectors. Makes the DB search-ready after an import.',
    category: 'database',
    danger: 'caution',
    longRunning: true,
    confirm: true,
    exclusive: true,
    exec: { kind: 'internal', handler: 'admin:full-migration' },
    source: 'src/services/admin.service.ts',
  },
  {
    id: 'db-post-import',
    name: 'Run Post-Import SQL',
    description: 'Add post-import columns, extract structured fields (address, hours, phones…), and build indexes.',
    category: 'database',
    danger: 'safe',
    longRunning: false,
    confirm: false,
    exec: { kind: 'internal', handler: 'admin:post-import' },
    source: 'import/post-import.sql',
  },
  {
    id: 'db-generate-codes',
    name: 'Generate Codes',
    description: 'Extract IATA, ICAO, ref, short_name, abbreviation, and alt_name codes from OSM tags into the codes column.',
    category: 'database',
    danger: 'safe',
    longRunning: false,
    confirm: false,
    exec: { kind: 'internal', handler: 'admin:generate-codes' },
    source: 'src/services/admin.service.ts',
  },
  {
    id: 'db-generate-abbreviations',
    name: 'Generate Abbreviations',
    description: 'Generate first-letter abbreviations for multi-word Latin-script names into name_abbrev.',
    category: 'database',
    danger: 'safe',
    longRunning: false,
    confirm: false,
    exec: { kind: 'internal', handler: 'admin:generate-abbreviations' },
    source: 'src/services/admin.service.ts',
  },
  {
    id: 'db-resolve-parent-context',
    name: 'Resolve Parent Context (full)',
    description: 'Spatial join to populate parent_context (admin boundary names + address) for all named places.',
    category: 'database',
    danger: 'caution',
    longRunning: true,
    confirm: true,
    exclusive: true,
    exec: { kind: 'internal', handler: 'admin:resolve-parent-context' },
    source: 'import/resolve-parent-context.sql',
    notes: 'A large spatial join — minutes to tens of minutes on large datasets.',
  },
  {
    id: 'db-resolve-parent-context-incremental',
    name: 'Resolve Parent Context (incremental)',
    description: 'Resolve parent_context only for new/changed POIs plus a boundary-change cascade. For daily diff updates.',
    category: 'database',
    danger: 'safe',
    longRunning: true,
    confirm: false,
    exec: { kind: 'internal', handler: 'admin:resolve-parent-context-incremental' },
    source: 'import/resolve-parent-context-incremental.sql',
  },
  {
    id: 'db-rebuild-tsvectors',
    name: 'Rebuild TSVectors',
    description: 'Rebuild full-text search vectors for all named places (includes name_abbrev, categories, parent_context).',
    category: 'database',
    danger: 'caution',
    longRunning: true,
    confirm: true,
    exec: { kind: 'internal', handler: 'admin:rebuild-tsvectors' },
    source: 'src/services/admin.service.ts',
  },

  // ── Config Generation ─────────────────────────────────────────────────
  {
    id: 'config-pelias',
    name: 'Generate Pelias Config',
    description: 'Regenerate the imports block of pelias/pelias.json from the unified REGIONS config.',
    category: 'config',
    danger: 'safe',
    longRunning: false,
    confirm: false,
    exec: { kind: 'process', command: 'bun', args: ['run', 'scripts/generate-pelias-config.ts'] },
    params: [REGIONS_PARAM],
    source: 'scripts/generate-pelias-config.ts',
  },
]

export function getScript(id: string): ScriptDef | undefined {
  return SCRIPTS.find((s) => s.id === id)
}
