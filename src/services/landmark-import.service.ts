/**
 * Import 3D landmarks from datasets published in the Open Landmarks release
 * format (https://github.com/benjamintd/open-landmarks).
 *
 * Every source is read the same way, so adding one is a config entry:
 *
 *   openlandmarks  Open Landmarks itself.
 *   barrelman      Barrelman's own models, built in their own repo
 *                  (https://github.com/alexwohlbruck/landmarks) and published
 *                  to GitHub Pages in the same format.
 *
 * Each source publishes immutable releases: a small channel pointer names the
 * current release, whose catalogue links one JSON file holding every model's
 * metadata, and each model is a content-addressed GLB. That shape makes an
 * import cheap to repeat:
 *
 *   1. fetch the pointer (a few hundred bytes); if its release is the one
 *      already imported, stop: the common case costs one request;
 *   2. fetch the catalogue and its asset list (two requests, whatever the
 *      dataset's size);
 *   3. download only GLBs whose sha256 is not already in the cache, gzipped,
 *      verified against the published hash before use;
 *   4. replace this source's rows in one transaction, so tiles never see a
 *      half-imported release.
 *
 * Then, once for all sources, conflicts between them are resolved and the
 * served-file lookup and the cache are brought up to date.
 *
 * The model frame is ours (X east, Y up, Z south, metres, ground at Y=0,
 * WGS84 anchor) with the heading already baked in, so a placement is the
 * anchor with bearing 0 and scale 1.
 *
 * Licensing travels with each model: the artistic licence of the model, and
 * ODbL for the OSM-derived placement, credited through the per-model
 * attribution string the tiles already carry.
 */
import { join, resolve } from 'path'
import { mkdir, readdir, rename, unlink } from 'fs/promises'
import { connection as sql } from '../db'
import { envNumber, envRaw, envString } from '../config/env'
import {
  ensureLandmarksSchema,
  glbBounds,
  LANDMARK_SOURCE_IDS,
  modelFileName,
  refreshModelFiles,
  resolveLandmarkConflicts,
  type LandmarkSourceId,
} from './landmarks.service'

export const LANDMARK_AXES = 'X east / Y up / Z south'

const SLUG_RE = /^[a-z0-9]+(-[a-z0-9]+)*$/
const SHA_RE = /^[0-9a-f]{64}$/
const WIKIDATA_RE = /^Q\d+$/
const OSM_TYPES = new Set(['node', 'way', 'relation'])

export type LandmarkSource = {
  /** Written to the `origin` column and sent as the tile feature's `source`. */
  id: LandmarkSourceId
  /** For logs. */
  name: string
  /** The variable that sets `base`, for log lines that tell an operator what to change. */
  urlEnv: string
  /**
   * Dataset origin: an http(s) URL, or a local directory holding a built
   * release. Null when the source is switched off.
   */
  base: string | null
  channel: 'latest' | 'preview'
  /** Prepended to the dataset's ids, so two sources' ids cannot collide. */
  idPrefix: string
  /** Used where an asset does not state its own. */
  license: string
  author: string
  attribution: string | null
}

/** A configured URL, with `off` meaning switched off and a trailing slash dropped. */
function sourceBase(name: string, fallback: string): string | null {
  const url = envString<string>(name, fallback)
  if (url === 'off') return null
  return /^https?:\/\//.test(url) ? url.replace(/\/+$/, '') : resolve(url)
}

/**
 * Every source, in a fixed order. A source switched off is still listed, so
 * the import can clear the rows it left behind.
 */
export function landmarkSources(): LandmarkSource[] {
  return [
    {
      id: 'openlandmarks',
      name: 'Open Landmarks',
      urlEnv: 'OPEN_LANDMARKS_URL',
      base: sourceBase('OPEN_LANDMARKS_URL', 'https://open-landmarks.benmaps.fr'),
      channel: envString('OPEN_LANDMARKS_CHANNEL', 'latest', ['latest', 'preview'] as const),
      idPrefix: 'ol-',
      license: 'CC-BY-4.0',
      author: 'Open Landmarks contributors',
      attribution: 'Open Landmarks; © OpenStreetMap contributors',
    },
    {
      id: 'barrelman',
      name: 'Barrelman landmarks',
      urlEnv: 'BARRELMAN_LANDMARKS_URL',
      base: sourceBase('BARRELMAN_LANDMARKS_URL', 'https://alexwohlbruck.github.io/landmarks'),
      channel: 'latest',
      // Unprefixed: these ids are the ones the bundled catalog used, so a
      // placement keeps its id, and its tile feature id, across the move.
      idPrefix: '',
      license: 'CC0-1.0',
      author: 'Barrelman',
      // CC0 models need no credit; the map credits OpenStreetMap already.
      attribution: null,
    },
  ]
}

export function landmarkImportConfig() {
  return {
    cacheDir: resolve(envString('LANDMARKS_CACHE_DIR', './data/landmarks')),
    concurrency: Math.max(1, envNumber('LANDMARKS_CONCURRENCY', 4)),
  }
}

type Lod = { url: string; bytes: number; sha256: string; gzip?: { url: string; bytes: number; sha256: string } }
type OsmRef = { type: string; id: number }

/** The parts of an Open Landmarks asset record the import reads. */
export type LandmarkAsset = {
  id: string
  name: string
  anchor: [number, number]
  heading: number
  axes: string
  minZoom?: number
  detailZoom?: number
  lods: { low: Lod; detail?: Lod }
  osm?: OsmRef | null
  additionalOsm?: OsmRef[]
  entranceLights?: number[][]
  /** Not in Open Landmarks' schema; the Barrelman dataset sends it. */
  wikidata?: string
  attribution?: string
  authors?: string[]
  artisticLicense?: string
  metadata?: string
}

/** One landmark ready to write: the placement and its one or two models. */
export type LandmarkRow = {
  id: string
  sourceId: string
  name: string
  lng: number
  lat: number
  minZoom: number
  detailZoom: number | null
  replaces: string[]
  wikidata: string | null
  entrances: number[][] | null
  license: string
  author: string
  attribution: string | null
  metadata: string | null
  low: { modelId: string; lod: Lod }
  detail: { modelId: string; lod: Lod } | null
}

/**
 * Check one asset and turn it into a row for `source`, or say why it was
 * skipped. Pure, so the contract with the upstream schema is tested without a
 * network.
 */
export function landmarkRow(source: LandmarkSource, asset: LandmarkAsset): LandmarkRow | { skip: string } {
  const id = asset?.id
  if (typeof id !== 'string' || !SLUG_RE.test(id)) return { skip: `bad id ${JSON.stringify(id)}` }
  if (asset.axes !== LANDMARK_AXES) return { skip: `${id}: unsupported axes "${asset.axes}"` }
  // A non-zero heading would mean the geometry is no longer baked, which this
  // import does not handle; better to skip than to draw it turned.
  if (asset.heading !== 0) return { skip: `${id}: heading ${asset.heading} is not baked` }
  const [lng, lat] = asset.anchor ?? []
  if (!(Math.abs(lng) <= 180 && Math.abs(lat) <= 85)) return { skip: `${id}: anchor out of range` }
  const lodOk = (lod?: Lod) => !!lod && SHA_RE.test(lod.sha256) && lod.url?.startsWith('/') && lod.bytes > 0
  if (!lodOk(asset.lods?.low)) return { skip: `${id}: no usable low LOD` }
  const detail = lodOk(asset.lods.detail) ? asset.lods.detail! : null

  const replaces = [asset.osm, ...(asset.additionalOsm ?? [])]
    .filter((r): r is OsmRef => !!r && OSM_TYPES.has(r.type) && Number.isInteger(r.id))
    .map((r) => `${r.type}/${r.id}`)
  const entrances = (asset.entranceLights ?? []).filter(
    (p) => Array.isArray(p) && p.length === 3 && p.every(Number.isFinite),
  )
  const ours = `${source.idPrefix}${id}`
  return {
    id: ours,
    sourceId: id,
    name: asset.name || id,
    lng,
    lat,
    minZoom: asset.minZoom ?? 15,
    detailZoom: detail ? (asset.detailZoom ?? 17) : null,
    replaces,
    wikidata: typeof asset.wikidata === 'string' && WIKIDATA_RE.test(asset.wikidata) ? asset.wikidata : null,
    entrances: entrances.length ? entrances : null,
    license: asset.artisticLicense || source.license,
    author: (asset.authors ?? []).join(', ') || source.author,
    attribution: asset.attribution || source.attribution,
    metadata: asset.metadata ?? null,
    low: { modelId: `${ours}-low`, lod: asset.lods.low },
    detail: detail ? { modelId: `${ours}-detail`, lod: detail } : null,
  }
}

type Log = (line: string) => void

const isRemote = (base: string) => /^https?:\/\//.test(base)

/** A file from a source: fetched over HTTP, or read from a local release directory. */
async function readSource(base: string, path: string, timeoutMs: number): Promise<Uint8Array<ArrayBuffer>> {
  if (!isRemote(base)) {
    const file = Bun.file(join(base, path))
    if (!(await file.exists())) throw new Error(`${join(base, path)}: not found`)
    return new Uint8Array(await file.arrayBuffer())
  }
  const res = await fetch(base + path, { signal: AbortSignal.timeout(timeoutMs) })
  if (!res.ok) throw new Error(`${base + path}: HTTP ${res.status}`)
  return new Uint8Array(await res.arrayBuffer())
}

async function getJson<T>(base: string, path: string): Promise<T> {
  return JSON.parse(new TextDecoder().decode(await readSource(base, path, 30_000))) as T
}

/**
 * The GLB for a LOD, from the content-addressed cache or downloaded into it.
 * The gzip file is fetched when offered (a third of the bytes) and the raw
 * GLB is checked against the published sha256 before it is kept, so a
 * truncated or tampered download never reaches a map. Concurrent requests for
 * one hash share a download, so two placements of identical bytes never race
 * on the same temp file.
 */
const inFlight = new Map<string, Promise<{ path: string; bytes: Uint8Array; fetched: boolean }>>()
function cachedGlb(base: string, cacheDir: string, lod: Lod, refetch: boolean) {
  const key = `${cacheDir}/${lod.sha256}`
  let pending = inFlight.get(key)
  if (!pending) {
    pending = fetchGlb(base, cacheDir, lod, refetch).finally(() => inFlight.delete(key))
    inFlight.set(key, pending)
  }
  return pending
}

async function fetchGlb(base: string, cacheDir: string, lod: Lod, refetch: boolean) {
  const path = join(cacheDir, `${lod.sha256}.glb`)
  const file = Bun.file(path)
  if (!refetch && (await file.exists())) return { path, bytes: new Uint8Array(await file.arrayBuffer()), fetched: false }

  const gz = lod.gzip?.url
  const body = await readSource(base, gz ?? lod.url, 120_000)
  // `.glb.gz` is a gzip *file*, not a transfer encoding: decompress it here.
  const bytes = gz ? Bun.gunzipSync(body) : body
  const sha = new Bun.CryptoHasher('sha256').update(bytes).digest('hex')
  if (sha !== lod.sha256 || bytes.length !== lod.bytes) throw new Error(`${lod.url}: hash or size mismatch`)
  // Write then rename, so a crash mid-write never leaves a file the cache
  // would trust next time.
  const tmp = `${path}.${process.pid}.tmp`
  await Bun.write(tmp, bytes)
  await rename(tmp, path)
  return { path, bytes, fetched: true }
}

/** Run `fn` over `items`, at most `limit` at a time. */
async function mapLimit<T, R>(items: T[], limit: number, fn: (item: T) => Promise<R>): Promise<R[]> {
  const out: R[] = new Array(items.length)
  let next = 0
  await Promise.all(
    Array.from({ length: Math.min(limit, items.length) }, async () => {
      while (next < items.length) {
        const i = next++
        out[i] = await fn(items[i])
      }
    }),
  )
  return out
}

export type SourceResult = {
  source: LandmarkSourceId
  off: boolean
  release: string | null
  unchanged: boolean
  landmarks: number
  downloaded: number
  skipped: string[]
  error: string | null
}

export type ImportResult = { sources: SourceResult[]; superseded: number; retired: number }

type Mode = { force: boolean; full: boolean; log: Log }

/** Remove every row a source owns, for a source switched off or withdrawn. */
async function clearSource(id: string): Promise<number> {
  return sql.begin(async (rawTx) => {
    const tx = rawTx as unknown as typeof sql
    const gone = await tx`DELETE FROM landmarks WHERE origin = ${id} RETURNING id`
    await tx`
      DELETE FROM landmark_models m WHERE origin = ${id}
        AND NOT EXISTS (SELECT 1 FROM landmarks l WHERE l.model_id = m.id OR l.detail_model_id = m.id)`
    await tx`DELETE FROM landmark_sources WHERE source = ${id}`
    return gone.length
  })
}

/** Bring one source's rows in line with its current release. */
async function importSource(source: LandmarkSource, { force, full, log }: Mode): Promise<SourceResult> {
  const result: SourceResult = {
    source: source.id, off: false, release: null, unchanged: false, landmarks: 0, downloaded: 0, skipped: [], error: null,
  }
  const label = `${source.name} (${source.id})`
  if (!source.base) {
    result.off = true
    const cleared = await clearSource(source.id)
    log(`${label}: off (${source.urlEnv}=off)${cleared ? `, removed its ${cleared} landmarks` : ''}`)
    return result
  }
  const base = source.base
  const { cacheDir, concurrency } = landmarkImportConfig()

  const pointer = await getJson<{ release: string | null; catalogue: string | null }>(base, `/api/v1/${source.channel}.json`)
  result.release = pointer.release
  const [state] = await sql<{ release: string | null }[]>`SELECT release FROM landmark_sources WHERE source = ${source.id}`
  if (!force && state && state.release === pointer.release) {
    log(`${label} ${source.channel}: release ${pointer.release} already imported`)
    result.unchanged = true
    return result
  }

  // A channel with nothing published is a real answer, not an error: it
  // empties this source rather than keeping a release the dataset withdrew.
  let assets: LandmarkAsset[] = []
  if (pointer.catalogue) {
    const catalogue = await getJson<{ assets: string }>(base, pointer.catalogue)
    assets = (await getJson<{ assets: LandmarkAsset[] }>(base, catalogue.assets)).assets ?? []
  }
  let rows: LandmarkRow[] = []
  for (const asset of assets) {
    const row = landmarkRow(source, asset)
    if ('skip' in row) result.skipped.push(row.skip)
    else rows.push(row)
  }

  // An id another source already owns stays with it. Rows left by a retired
  // source (the bundled catalog's `catalog`) may be taken over, which is how a
  // placement keeps its id and tile feature id when its source moves.
  const rowOf = new Map<string, string>()
  for (const r of rows) for (const id of [r.id, r.low.modelId, ...(r.detail ? [r.detail.modelId] : [])]) rowOf.set(id, r.id)
  const others = LANDMARK_SOURCE_IDS.filter((id) => id !== source.id)
  const owned = await sql<{ id: string; origin: string }[]>`
    SELECT id, origin FROM landmarks WHERE id = ANY(${rows.map((r) => r.id)}) AND origin = ANY(${others})
    UNION ALL
    SELECT id, origin FROM landmark_models WHERE id = ANY(${[...rowOf.keys()]}) AND origin = ANY(${others})`
  if (owned.length) {
    const taken = new Map(owned.map((o) => [rowOf.get(o.id)!, o.origin]))
    for (const [id, origin] of taken) result.skipped.push(`${id}: id already belongs to ${origin}`)
    rows = rows.filter((r) => !taken.has(r.id))
  }
  log(`${label} ${source.channel}: release ${pointer.release}, ${rows.length} landmarks` +
    (result.skipped.length ? `, ${result.skipped.length} skipped` : ''))

  await mkdir(cacheDir, { recursive: true })
  type Model = { modelId: string; sha256: string; path: string; bytes: number; height: number; radius: number }
  const lods = rows.flatMap((r) => [r.low, ...(r.detail ? [r.detail] : [])])
  const models = new Map<string, Model>()
  await mapLimit(lods, concurrency, async ({ modelId, lod }) => {
    const { path, bytes, fetched } = await cachedGlb(base, cacheDir, lod, full)
    if (fetched) result.downloaded++
    models.set(modelId, { modelId, sha256: lod.sha256, path, bytes: bytes.length, ...glbBounds(bytes) })
  })
  log(`  ${result.downloaded} model files downloaded, ${models.size - result.downloaded} from cache`)

  await sql.begin(async (rawTx) => {
    // postgres-js types a transaction as not callable as a tagged template,
    // though it is; see boundary-catalog.service.ts.
    const tx = rawTx as unknown as typeof sql
    for (const r of rows) {
      for (const { modelId } of [r.low, ...(r.detail ? [r.detail] : [])]) {
        const m = models.get(modelId)!
        await tx`
          INSERT INTO landmark_models (id, file, path, sha256, bytes, height_m, radius_m, license, author, source, attribution, origin)
          VALUES (${modelId}, ${modelFileName(modelId, m.sha256)}, ${m.path}, ${m.sha256}, ${m.bytes}, ${m.height}, ${m.radius},
                  ${r.license}, ${r.author}, ${r.metadata ? base + r.metadata : null}, ${r.attribution}, ${source.id})
          ON CONFLICT (id) DO UPDATE SET
            file = EXCLUDED.file, path = EXCLUDED.path, sha256 = EXCLUDED.sha256, bytes = EXCLUDED.bytes,
            height_m = EXCLUDED.height_m, radius_m = EXCLUDED.radius_m, license = EXCLUDED.license,
            author = EXCLUDED.author, source = EXCLUDED.source, attribution = EXCLUDED.attribution,
            origin = EXCLUDED.origin, updated_at = now()`
      }
      // The reach covers whichever LOD is larger, so a tile carries the
      // landmark wherever either could be drawn.
      const radius = Math.max(models.get(r.low.modelId)!.radius, r.detail ? models.get(r.detail.modelId)!.radius : 0)
      // Bearing, scale and elevation are reset, not left alone: a row taken
      // over from the bundled catalog had its own, and here they are baked in.
      await tx`
        INSERT INTO landmarks (id, name, model_id, detail_model_id, detail_zoom, geom, reach, bearing, scale, elevation,
                               min_zoom, replaces, wikidata, entrances, source_id, origin)
        SELECT ${r.id}, ${r.name}, ${r.low.modelId}, ${r.detail?.modelId ?? null}, ${r.detailZoom}, p.geom,
               ST_Expand(ST_Transform(p.geom, 3857), ${radius} / cos(radians(${r.lat}))),
               0, 1, 0, ${r.minZoom}, ${r.replaces}, ${r.wikidata}, ${r.entrances ? JSON.stringify(r.entrances) : null}::jsonb,
               ${r.sourceId}, ${source.id}
        FROM (SELECT ST_SetSRID(ST_MakePoint(${r.lng}, ${r.lat}), 4326) AS geom) p
        ON CONFLICT (id) DO UPDATE SET
          name = EXCLUDED.name, model_id = EXCLUDED.model_id, detail_model_id = EXCLUDED.detail_model_id,
          detail_zoom = EXCLUDED.detail_zoom, geom = EXCLUDED.geom, reach = EXCLUDED.reach,
          bearing = 0, scale = 1, elevation = 0, min_zoom = EXCLUDED.min_zoom, replaces = EXCLUDED.replaces,
          wikidata = EXCLUDED.wikidata, entrances = EXCLUDED.entrances, source_id = EXCLUDED.source_id,
          origin = EXCLUDED.origin, updated_at = now()`
    }
    const keep = rows.map((r) => r.id)
    await tx`DELETE FROM landmarks WHERE origin = ${source.id} AND NOT (id = ANY(${keep}))`
    await tx`
      DELETE FROM landmark_models m
      WHERE origin = ${source.id} AND NOT (id = ANY(${[...models.keys()]}))
        AND NOT EXISTS (SELECT 1 FROM landmarks l WHERE l.model_id = m.id OR l.detail_model_id = m.id)`
    await tx`
      INSERT INTO landmark_sources (source, release, landmarks, imported_at)
      VALUES (${source.id}, ${pointer.release}, ${rows.length}, now())
      ON CONFLICT (source) DO UPDATE SET release = EXCLUDED.release, landmarks = EXCLUDED.landmarks, imported_at = now()`
  })
  result.landmarks = rows.length
  return result
}

/**
 * Drop rows whose origin is no source this version knows: the bundled
 * catalog's `catalog` rows that no source took over. Models first lose their
 * placements, then go once nothing points at them.
 */
async function retireUnknownOrigins(): Promise<number> {
  return sql.begin(async (rawTx) => {
    const tx = rawTx as unknown as typeof sql
    const known = [...LANDMARK_SOURCE_IDS]
    const gone = await tx`DELETE FROM landmarks WHERE NOT (origin = ANY(${known})) RETURNING id`
    await tx`
      DELETE FROM landmark_models m WHERE NOT (origin = ANY(${known}))
        AND NOT EXISTS (SELECT 1 FROM landmarks l WHERE l.model_id = m.id OR l.detail_model_id = m.id)`
    await tx`DELETE FROM landmark_sources WHERE NOT (source = ANY(${known}))`
    return gone.length
  })
}

let running: Promise<unknown> = Promise.resolve()

/**
 * Bring every source's rows in line with its current release, then resolve
 * conflicts between sources once.
 *
 * Two modes:
 *
 *   update (default) stops after one request per source when its release
 *          hasn't moved, and downloads only model files the cache doesn't
 *          already hold.
 *   full   re-reads every release even when its id hasn't moved, downloads
 *          every model again and re-verifies it against its published hash,
 *          and rewrites every row. For a cache that may be damaged or a
 *          database that may have drifted; it costs every dataset's bytes.
 *
 * `force` alone re-reads unchanged releases but still trusts the cache.
 *
 * One source failing does not stop the others; the import still throws at
 * the end, naming each failure, so a job or a schedule records it.
 * Imports run one at a time in a process, since startup, the schedule and
 * the console can all start one and they share the cache.
 */
export function importLandmarks(
  { force = false, full = false, log = () => {} }: { force?: boolean; full?: boolean; log?: Log } = {},
): Promise<ImportResult> {
  const next = running.then(() => runImport({ force: force || full, full, log }))
  running = next.catch(() => {})
  return next
}

async function runImport(mode: Mode): Promise<ImportResult> {
  await ensureLandmarksSchema()
  if (envRaw('LANDMARKS_DIR'))
    mode.log('LANDMARKS_DIR is no longer read: point BARRELMAN_LANDMARKS_URL at a built release directory instead')
  const sources: SourceResult[] = []
  for (const source of landmarkSources()) {
    try {
      sources.push(await importSource(source, mode))
    } catch (err) {
      const message = err instanceof Error ? err.message : String(err)
      mode.log(`${source.name} (${source.id}): import failed: ${message}`)
      sources.push({
        source: source.id, off: false, release: null, unchanged: false, landmarks: 0, downloaded: 0, skipped: [], error: message,
      })
    }
  }
  const retired = await retireUnknownOrigins()
  if (retired) mode.log(`removed ${retired} landmarks left by a retired source`)
  const { superseded } = await resolveLandmarkConflicts()
  await refreshModelFiles()
  await pruneCache(landmarkImportConfig().cacheDir)
  mode.log(`done; ${superseded} placements superseded by a higher-priority source`)

  const failed = sources.filter((s) => s.error)
  if (failed.length) throw new Error(`landmark import failed for ${failed.map((s) => `${s.source} (${s.error})`).join('; ')}`)
  return { sources, superseded, retired }
}

/**
 * Drop cached GLBs no source's rows point at any more, and stray temp files.
 * Every source shares the cache, so what to keep comes from all of them.
 */
async function pruneCache(dir: string): Promise<void> {
  const rows = await sql<{ sha256: string }[]>`SELECT DISTINCT sha256 FROM landmark_models`
  const keep = new Set(rows.map((r) => `${r.sha256}.glb`))
  for (const name of await readdir(dir).catch(() => [] as string[])) {
    if ((name.endsWith('.glb') && !keep.has(name)) || name.endsWith('.tmp')) await unlink(join(dir, name)).catch(() => {})
  }
}
