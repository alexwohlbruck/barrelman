// Mirrors the backend manifest / job-runner / metrics shapes.

export type ScriptCategory = 'osm' | 'transit' | 'gbfs' | 'search' | 'routing' | 'database' | 'config'
export type DangerLevel = 'safe' | 'caution' | 'destructive'
export type ParamApply = 'flag' | 'env' | 'positional'
export type ParamType = 'string' | 'number' | 'boolean' | 'select'

export interface ScriptParam {
  name: string
  label: string
  type: ParamType
  apply: ParamApply
  flag?: string
  envVar?: string
  default?: string | number | boolean
  options?: { label: string; value: string }[]
  placeholder?: string
  required?: boolean
  secret?: boolean
  description?: string
}

export interface ScriptDef {
  id: string
  name: string
  description: string
  category: ScriptCategory
  danger: DangerLevel
  longRunning: boolean
  confirm: boolean
  exclusive?: boolean
  exec: { kind: 'process'; command: string; args: string[] } | { kind: 'internal'; handler: string }
  params?: ScriptParam[]
  env?: Record<string, string>
  source?: string
  notes?: string
}

export interface ScriptCategoryGroup {
  key: ScriptCategory
  label: string
  scripts: ScriptDef[]
}

export interface ScriptsResponse {
  categories: ScriptCategoryGroup[]
  scripts: ScriptDef[]
}

export type JobStatus = 'running' | 'succeeded' | 'failed' | 'canceled'
export type LogStreamName = 'stdout' | 'stderr' | 'system'

export interface LogLine {
  seq: number
  t: number
  stream: LogStreamName
  text: string
}

export interface Job {
  id: string
  scriptId: string
  scriptName: string
  category: ScriptCategory
  danger: DangerLevel
  status: JobStatus
  params: Record<string, unknown>
  displayCommand: string
  startedAt: number
  endedAt?: number
  durationMs?: number
  exitCode?: number | null
  error?: string
  logCount: number
}

export interface JobStats {
  total: number
  running: number
  succeeded: number
  failed: number
}

export interface DataMetrics {
  database: { sizeBytes: number | null; sizePretty: string | null }
  geoPlaces: {
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
  gbfs: { systems: number | null; stations: number | null }
  transit: { stopAreaMembers: number | null }
  boundaries: { count: number | null; fetchedAt: string | null }
}

// ── Import regions (DB-backed region store) ───────────────────────────
export type Bbox = [west: number, south: number, east: number, north: number]

export interface RegionPelias {
  openaddresses: string[]
  wofIds: string[]
  tigerStates: number[]
  countryCode?: string
}

export interface ImportRegion {
  key: string
  label: string
  osmExtracts: string[]
  osmReplication: string[]
  bbox: Bbox
  gtfsRegion: string
  pelias: RegionPelias
  isGlobal: boolean
  enabled: boolean
}

/** An entry in the boundary catalog (Geofabrik's published extract index). */
export interface Boundary {
  id: string
  label: string
  name: string
  parent: string | null
  iso3166_1: string[]
  iso3166_2: string[]
  pbfUrl: string
  updatesUrl: string | null
  bbox: Bbox
}

export interface BoundarySearchResponse {
  catalog: { count: number; fetchedAt: string | null }
  boundaries: Boundary[]
}

/** A region definition auto-filled from a boundary, before it is saved. */
export interface DerivedRegion {
  region: {
    key: string
    label: string
    osmExtracts: string[]
    osmReplication: string[]
    bbox: Bbox
    gtfsRegion: string
    pelias: RegionPelias
    enabled: boolean
  }
  warnings: string[]
  sources: Record<string, string>
}

export interface ServiceStatus {
  name: string
  key: string
  status: 'ok' | 'unavailable'
  url?: string
  latencyMs?: number
  message?: string
}

export interface TestResult {
  ok: boolean
  status: number
  statusText: string
  durationMs: number
  contentType?: string
  bytes?: number
  body?: unknown
  error?: string
}
