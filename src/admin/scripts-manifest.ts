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

/**
 * One follow-up script a parent script invokes on its way through.
 *
 * DESCRIPTIVE, NOT EXECUTIVE. The job runner does not read this and will never
 * start a second job from it — `startJob()` takes one script id and runs one
 * command. The chaining is done in bash, by the parent script shelling out to a
 * sibling (`update-osm.sh` → `rebuild-graphhopper.sh`, and so on), which is why
 * a chain is one job with one log stream under one exclusive lock.
 *
 * It exists so the console can say so. Before this, "OSM Update" looked like a
 * single step in the UI while actually rebuilding the routing graph and the
 * basemap too, and the only way to find that out was to read the shell.
 *
 * Keeping it as data means it can drift from the shell it describes, so
 * scripts-manifest.test.ts asserts every id resolves — a dangling one renders
 * as a blank row.
 */
export interface ScriptChainStep {
  /** Manifest id of the script the parent invokes. */
  script: string
  /** Describes when the step is skipped. Omit for steps that always run. */
  when?: string
}

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
  /**
   * Follow-up scripts this one invokes internally, in the order they run.
   * Surfaced in the console — see ScriptChainStep for why it is not executed.
   */
  postScripts?: ScriptChainStep[]
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

/**
 * Re-download the extracts instead of reusing whatever `region.osm.pbf` is on
 * disk.
 *
 * `import-osm.sh` keeps an existing PBF by default, which quietly turns a "full
 * import" into a replay of the last download. If that file is older than the
 * data in the database — or covers fewer regions than the current REGIONS —
 * the import silently rolls the database backwards, because osm2pgsql --create
 * drops the tables first and only then discovers it has nothing new to load.
 */
const FORCE_DOWNLOAD_PARAM: ScriptParam = {
  name: 'FORCE_DOWNLOAD',
  label: 'Re-download extracts',
  type: 'boolean',
  apply: 'env',
  envVar: 'FORCE_DOWNLOAD',
  default: true,
  description:
    'Fetch the region extracts again rather than reusing the PBF on disk. Turn this off only to replay an import from the exact file already downloaded.',
}

/**
 * Re-render the PMTiles basemap at the end of an import or update.
 *
 * On by default because the basemap is the one output that has no other way to
 * track the data: martin serves every `parchment_*` source live from PostGIS,
 * but `basemap` is a static archive, so without this an update moves the
 * database and the map keeps showing the last hand-built render.
 *
 * Turning it off is for deployments that would rather rebuild on a slower
 * cadence than pay a planetiler run per update. The script skips itself anyway
 * on installs that have no basemap.pmtiles at all.
 */
const REBUILD_BASEMAP_PARAM: ScriptParam = {
  name: 'REBUILD_BASEMAP',
  label: 'Rebuild basemap',
  type: 'boolean',
  apply: 'env',
  envVar: 'REBUILD_BASEMAP',
  default: true,
  description:
    'Re-render basemap.pmtiles with planetiler once the extract has changed. Adds a few minutes. Turn off to refresh the basemap separately.',
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
    params: [REGIONS_PARAM, FORCE_DOWNLOAD_PARAM, REBUILD_BASEMAP_PARAM],
    postScripts: [
      { script: 'routing-graphhopper' },
      { script: 'osm-basemap', when: 'unless "Rebuild basemap" is off, or the install has no basemap' },
    ],
    source: 'scripts/run-import.sh',
    notes:
      'osm2pgsql --create drops and recreates the geo_places tables. Expect 20–40+ minutes for a US state; longer for larger regions. Leave "Re-download extracts" on unless the PBF on disk is known to match the selected regions — a stale file covers fewer regions than expected and the tables are already dropped by the time that shows.',
  },
  {
    id: 'osm-update',
    name: 'OSM Update',
    description:
      'Apply an incremental replication diff (fast) or re-run a full re-import, then re-run incremental post-processing. Diffs are applied to both Postgres and region.osm.pbf, and the routing graph and PMTiles basemap are rebuilt only when the extract actually changed.',
    category: 'osm',
    danger: 'caution',
    longRunning: true,
    confirm: true,
    exclusive: true,
    exec: { kind: 'process', command: 'bash', args: ['scripts/update-osm.sh'] },
    params: [
      REGIONS_PARAM,
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
      REBUILD_BASEMAP_PARAM,
    ],
    postScripts: [
      { script: 'routing-graphhopper', when: 'only when the extract actually changed' },
      { script: 'osm-basemap', when: 'only when the extract changed, and "Rebuild basemap" is on' },
    ],
    source: 'scripts/update-osm.sh',
    notes:
      'Full mode is a destructive re-import. Replication requires init-replication to have been run once. Safe to run at any interval — the cursor is stored in the database — but Geofabrik only retains about four months of diffs, so a database further behind than that needs a full re-import.',
  },
  {
    id: 'osm-basemap',
    name: 'Rebuild Basemap',
    description:
      'Re-render basemap.pmtiles from region.osm.pbf with planetiler, swap it in atomically and restart martin so the new archive is served.',
    category: 'osm',
    danger: 'caution',
    longRunning: true,
    confirm: true,
    exclusive: true,
    exec: { kind: 'process', command: 'bash', args: ['scripts/rebuild-basemap.sh'] },
    params: [
      {
        name: 'BASEMAP_FORCE',
        label: 'Build even if none exists',
        type: 'boolean',
        apply: 'env',
        envVar: 'BASEMAP_FORCE',
        default: false,
        description:
          'By default this only refreshes an existing basemap. Turn on to render the first one — you must also uncomment the pmtiles source in martin-config.yaml for it to be served.',
      },
      {
        name: 'PLANETILER_MEMORY',
        label: 'Render heap',
        type: 'string',
        apply: 'env',
        envVar: 'PLANETILER_MEMORY',
        default: '4g',
        placeholder: '4g',
        description: 'JVM heap for the planetiler container. Raise for larger extracts.',
      },
    ],
    source: 'scripts/rebuild-basemap.sh',
    notes:
      'The basemap is a static PMTiles archive, so unlike every parchment_* source it does not follow the database — this is what makes it track an import. PMTiles cannot be patched in place (write-once, absolute offsets, content-deduplicated) and planetiler has no incremental mode, so this is always a full re-render: about four minutes for NC+NY. Runs automatically after a full import and after an OSM update that moved the extract. The previous archive is kept as basemap.pmtiles.prev.',
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
    id: 'gtfs-watch',
    name: 'Check for GTFS Updates',
    description:
      "Compare each imported feed's stored version sha against Transitland's current one, re-import only the regions that changed, then rebuild the MOTIS dataset so the new schedules go live. Exits early and cheaply when nothing has changed.",
    category: 'transit',
    danger: 'caution',
    longRunning: true,
    confirm: false,
    exclusive: true,
    exec: { kind: 'process', command: 'bash', args: ['scripts/gtfs-watch.sh'] },
    params: [
      REGIONS_PARAM,
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
    ],
    postScripts: [{ script: 'routing-motis', when: 'only when a feed actually changed' }],
    source: 'scripts/gtfs-watch.sh',
    notes:
      'The intended nightly job — see the Schedules page. The very first run only records a baseline of current feed versions; it cannot detect drift until it has a prior sha to compare against.',
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
  // ── Portolan (corrected transit geometry) ─────────────────────────────
  // All three wrap `portolan sync` via import/portolan-sync.ts: rebuild
  // corrected route geometry + tile pyramids in the portolan workspace, then
  // re-import the corrected GTFS zips and rebuild the MOTIS dataset.
  {
    id: 'portolan-global-import',
    name: 'Portolan Global Import',
    description:
      'Run `portolan sync global`: rebuild corrected geometry for EVERY registered feed, retile all pyramids, re-export corrected GTFS, then re-import the corrected zips into PostGIS and rebuild the MOTIS dataset.',
    category: 'transit',
    danger: 'caution',
    longRunning: true,
    confirm: true,
    exclusive: true,
    exec: { kind: 'process', command: 'bun', args: ['run', 'import/portolan-sync.ts', '--global'] },
    params: [
      {
        name: 'skip-motis',
        label: 'Skip MOTIS rebuild',
        type: 'boolean',
        apply: 'flag',
        flag: '--skip-motis',
        default: false,
        description: 'Leave the MOTIS dataset stale; the corrected schedules go live only after scripts/rebuild-motis.sh runs.',
      },
      {
        name: 'dry-run',
        label: 'Dry run (plan only)',
        type: 'boolean',
        apply: 'flag',
        flag: '--dry-run',
        default: false,
      },
    ],
    source: 'import/portolan-sync.ts',
    notes:
      'A full-fleet rebuild — expect roughly an hour for a global registry. Requires the portolan binary (PORTOLAN_BIN) and workspace (PORTOLAN_WORKSPACE) to be set up; see the Portolan page under Self-hosting docs.',
  },
  {
    id: 'portolan-patch-import',
    name: 'Portolan Patch Import',
    description:
      'Run `portolan sync patch` for specific feed keys: rebuild exactly the builds whose inputs changed (including interleaved neighbors and group closure — byte-identical to a global run), then re-import the corrected zips and rebuild MOTIS.',
    category: 'transit',
    danger: 'caution',
    longRunning: true,
    confirm: true,
    exclusive: true,
    exec: { kind: 'process', command: 'bun', args: ['run', 'import/portolan-sync.ts'] },
    params: [
      {
        name: 'feeds',
        label: 'Portolan feed keys',
        type: 'string',
        apply: 'flag',
        flag: '--patch',
        required: true,
        placeholder: 'mta-subway,marc',
        description: 'Comma-separated portolan feed keys (registry keys in portolan.json, not barrelman feed ids).',
      },
      {
        name: 'skip-motis',
        label: 'Skip MOTIS rebuild',
        type: 'boolean',
        apply: 'flag',
        flag: '--skip-motis',
        default: false,
      },
    ],
    source: 'import/portolan-sync.ts',
    notes:
      'The patch may rebuild more than the named feeds: anything sharing steel with a changed feed rebuilds too, so the tile/export trees stay identical to a global run.',
  },
  {
    id: 'portolan-check-updates',
    name: 'Portolan Check for Updates',
    description:
      "Run `portolan sync check`: diff every registered feed's transitland sha against the sync state, download the changed ones, run the patch flow on them, then re-import + rebuild MOTIS. Exits early and cheaply when nothing moved.",
    category: 'transit',
    danger: 'safe',
    longRunning: true,
    confirm: false,
    exclusive: true,
    exec: { kind: 'process', command: 'bun', args: ['run', 'import/portolan-sync.ts', '--check'] },
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
      {
        name: 'dry-run',
        label: 'Dry run (diff only, no downloads)',
        type: 'boolean',
        apply: 'flag',
        flag: '--dry-run',
        default: false,
      },
    ],
    source: 'import/portolan-sync.ts',
    notes:
      'The intended nightly job for portolan-corrected feeds — point a daily schedule at this (Schedules page), the same way gtfs-watch is scheduled for plain feeds.',
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
  {
    id: 'detail-views',
    name: 'Rebuild Map Detail Tile Views',
    description:
      'Recreate the map detail vector-tile source views (parking surfaces, street trees, tree rows, street furniture) that Martin serves.',
    category: 'osm',
    danger: 'caution',
    longRunning: false,
    confirm: true,
    exec: { kind: 'internal', handler: 'sql:create-detail-views.sql' },
    source: 'import/create-detail-views.sql',
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
      REGIONS_PARAM,
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
    notes:
      'No Valhalla service ships in docker-compose.yml — this is only useful on a deployment that adds one. GraphHopper is the street router the API actually calls.',
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
  {
    id: 'routing-motis',
    name: 'Rebuild MOTIS Dataset',
    description:
      'Regenerate motis config from gtfs_feeds, clean-rebuild the timetable + street graph with `motis import`, and restart the server on the fresh dataset.',
    category: 'routing',
    danger: 'caution',
    longRunning: true,
    confirm: true,
    exclusive: true,
    exec: { kind: 'process', command: 'bash', args: ['scripts/rebuild-motis.sh'] },
    source: 'scripts/rebuild-motis.sh',
    notes:
      'REQUIRED after every GTFS import and after any MOTIS version bump — `motis server` only serves the pre-built dataset and never re-imports, so a plain restart keeps serving stale schedules. The previous dataset is kept at /data/data.prev and restored automatically if the import fails.',
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
  {
    id: 'config-fetch-boundaries',
    name: 'Fetch Boundary Catalog',
    description:
      "Download Geofabrik's extract index (every importable region, with its real boundary geometry and download URLs) into the boundary_catalog table. Run this before defining regions by name in Regions → Add by name. No API key required.",
    category: 'config',
    danger: 'safe',
    longRunning: false,
    confirm: false,
    exec: { kind: 'process', command: 'bun', args: ['run', 'scripts/fetch-boundaries.ts'] },
    params: [
      {
        name: 'search',
        label: 'Search after fetching',
        type: 'string',
        apply: 'flag',
        flag: '--search',
        placeholder: 'e.g. colorado (blank = just refresh)',
      },
    ],
    source: 'scripts/fetch-boundaries.ts',
    notes:
      'Replaces the cached catalog wholesale, so retired upstream extracts stop being offered. Safe to re-run.',
  },
]

export function getScript(id: string): ScriptDef | undefined {
  return SCRIPTS.find((s) => s.id === id)
}
