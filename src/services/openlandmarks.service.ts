/**
 * Import the Open Landmarks dataset (https://github.com/benjamintd/open-landmarks)
 * into the landmark tables, beside our own catalog.
 *
 * Open Landmarks publishes immutable releases: a small channel pointer names
 * the current release, whose catalogue links one JSON file holding every
 * model's metadata, and each model is a content-hashed GLB in two levels of
 * detail. That shape makes an import cheap to repeat:
 *
 *   1. fetch the pointer (a few hundred bytes); if its release is the one
 *      already imported, stop — the common case costs one request;
 *   2. fetch the catalogue and its asset list (two requests, whatever the
 *      dataset's size);
 *   3. download only GLBs whose sha256 is not already in the cache, gzipped,
 *      verified against the published hash before use;
 *   4. replace this source's rows in one transaction, so tiles never see a
 *      half-imported release, then re-run conflict resolution.
 *
 * Their model frame is ours (X east, Y up, Z south, metres, ground at Y=0,
 * WGS84 anchor) with the heading already baked in, so a placement is the
 * anchor with bearing 0 and scale 1.
 *
 * Licensing travels with each model: CC BY 4.0 for the artistic model and
 * ODbL for the OSM-derived placement, credited through the per-model
 * attribution string the tiles already carry.
 */
import { join, resolve } from 'path'
import { mkdir, readdir, rename, unlink } from 'fs/promises'
import { connection as sql } from '../db'
import { envNumber, envString } from '../config/env'
import {
  ensureLandmarksSchema,
  glbBounds,
  modelFileName,
  refreshModelFiles,
  resolveLandmarkConflicts,
} from './landmarks.service'

export const OPEN_LANDMARKS_SOURCE = 'openlandmarks'
export const OPEN_LANDMARKS_AXES = 'X east / Y up / Z south'

const SLUG_RE = /^[a-z0-9]+(-[a-z0-9]+)*$/
const SHA_RE = /^[0-9a-f]{64}$/
const OSM_TYPES = new Set(['node', 'way', 'relation'])

export type OpenLandmarksConfig = {
  /** Dataset origin; null when the import is switched off. */
  base: string | null
  channel: 'latest' | 'preview'
  cacheDir: string
  concurrency: number
}

export function openLandmarksConfig(): OpenLandmarksConfig {
  const url = envString<string>('OPEN_LANDMARKS_URL', 'https://open-landmarks.benmaps.fr')
  return {
    base: url === 'off' ? null : url.replace(/\/+$/, ''),
    channel: envString('OPEN_LANDMARKS_CHANNEL', 'latest', ['latest', 'preview'] as const),
    cacheDir: resolve(envString('LANDMARKS_CACHE_DIR', './data/landmarks')),
    concurrency: Math.max(1, envNumber('OPEN_LANDMARKS_CONCURRENCY', 4)),
  }
}

type Lod = { url: string; bytes: number; sha256: string; gzip?: { url: string; bytes: number; sha256: string } }
type OsmRef = { type: string; id: number }

/** The parts of an Open Landmarks asset record the import reads. */
export type OpenLandmarkAsset = {
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
  attribution?: string
  authors?: string[]
  artisticLicense?: string
  metadata?: string
}

/** One landmark ready to write: the placement and its one or two models. */
export type OpenLandmarkRow = {
  id: string
  sourceId: string
  name: string
  lng: number
  lat: number
  minZoom: number
  detailZoom: number | null
  replaces: string[]
  entrances: number[][] | null
  license: string
  author: string
  attribution: string
  metadata: string | null
  low: { modelId: string; lod: Lod }
  detail: { modelId: string; lod: Lod } | null
}

/**
 * Check one asset and turn it into a row, or say why it was skipped. Pure,
 * so the contract with the upstream schema is tested without a network.
 */
export function openLandmarkRow(asset: OpenLandmarkAsset): OpenLandmarkRow | { skip: string } {
  const id = asset?.id
  if (typeof id !== 'string' || !SLUG_RE.test(id)) return { skip: `bad id ${JSON.stringify(id)}` }
  if (asset.axes !== OPEN_LANDMARKS_AXES) return { skip: `${id}: unsupported axes "${asset.axes}"` }
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
  return {
    id: `ol-${id}`,
    sourceId: id,
    name: asset.name || id,
    lng,
    lat,
    minZoom: asset.minZoom ?? 15,
    detailZoom: detail ? (asset.detailZoom ?? 17) : null,
    replaces,
    entrances: entrances.length ? entrances : null,
    license: asset.artisticLicense || 'CC-BY-4.0',
    author: (asset.authors ?? []).join(', ') || 'Open Landmarks contributors',
    attribution: asset.attribution || 'Open Landmarks; © OpenStreetMap contributors',
    metadata: asset.metadata ?? null,
    low: { modelId: `ol-${id}-low`, lod: asset.lods.low },
    detail: detail ? { modelId: `ol-${id}-detail`, lod: detail } : null,
  }
}

type Log = (line: string) => void

async function getJson<T>(url: string): Promise<T> {
  const res = await fetch(url, { signal: AbortSignal.timeout(30_000) })
  if (!res.ok) throw new Error(`${url}: HTTP ${res.status}`)
  return (await res.json()) as T
}

/**
 * The GLB for a LOD, from the content-addressed cache or downloaded into it.
 * The gzip file is fetched when offered (a third of the bytes) and the raw
 * GLB is checked against the published sha256 before it is kept, so a
 * truncated or tampered download never reaches a map.
 */
async function cachedGlb(base: string, cacheDir: string, lod: Lod): Promise<{ path: string; bytes: Uint8Array; fetched: boolean }> {
  const path = join(cacheDir, `${lod.sha256}.glb`)
  const file = Bun.file(path)
  if (await file.exists()) return { path, bytes: new Uint8Array(await file.arrayBuffer()), fetched: false }

  const gz = lod.gzip?.url
  const res = await fetch(base + (gz ?? lod.url), { signal: AbortSignal.timeout(120_000) })
  if (!res.ok) throw new Error(`${gz ?? lod.url}: HTTP ${res.status}`)
  const body = new Uint8Array(await res.arrayBuffer())
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

export type ImportResult = {
  release: string | null
  unchanged: boolean
  landmarks: number
  downloaded: number
  skipped: string[]
  superseded: number
}

/**
 * Bring this source's rows in line with the current Open Landmarks release.
 * `force` re-reads the release even when its id has not moved.
 */
export async function importOpenLandmarks(
  { force = false, log = () => {} }: { force?: boolean; log?: Log } = {},
): Promise<ImportResult> {
  const config = openLandmarksConfig()
  const result: ImportResult = { release: null, unchanged: false, landmarks: 0, downloaded: 0, skipped: [], superseded: 0 }
  if (!config.base) {
    log('Open Landmarks import is off (OPEN_LANDMARKS_URL=off)')
    return result
  }
  await ensureLandmarksSchema()
  const base = config.base

  const pointer = await getJson<{ release: string | null; catalogue: string | null; status?: string }>(
    `${base}/api/v1/${config.channel}.json`,
  )
  result.release = pointer.release
  const [state] = await sql<{ release: string | null }[]>`SELECT release FROM landmark_sources WHERE source = ${OPEN_LANDMARKS_SOURCE}`
  if (!force && state && state.release === pointer.release) {
    log(`Open Landmarks ${config.channel}: release ${pointer.release} already imported`)
    result.unchanged = true
    return result
  }

  // A channel with nothing published is a real answer, not an error: it
  // empties this source rather than keeping a release the dataset withdrew.
  let assets: OpenLandmarkAsset[] = []
  if (pointer.catalogue) {
    const catalogue = await getJson<{ assets: string }>(base + pointer.catalogue)
    assets = (await getJson<{ assets: OpenLandmarkAsset[] }>(base + catalogue.assets)).assets ?? []
  }
  const rows: OpenLandmarkRow[] = []
  for (const asset of assets) {
    const row = openLandmarkRow(asset)
    if ('skip' in row) result.skipped.push(row.skip)
    else rows.push(row)
  }
  log(`Open Landmarks ${config.channel}: release ${pointer.release}, ${rows.length} landmarks` +
    (result.skipped.length ? `, ${result.skipped.length} skipped` : ''))

  await mkdir(config.cacheDir, { recursive: true })
  type Model = { modelId: string; sha256: string; path: string; bytes: number; height: number; radius: number }
  const lods = rows.flatMap((r) => [r.low, ...(r.detail ? [r.detail] : [])])
  const models = new Map<string, Model>()
  await mapLimit(lods, config.concurrency, async ({ modelId, lod }) => {
    const { path, bytes, fetched } = await cachedGlb(base, config.cacheDir, lod)
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
                  ${r.license}, ${r.author}, ${r.metadata ? base + r.metadata : null}, ${r.attribution}, ${OPEN_LANDMARKS_SOURCE})
          ON CONFLICT (id) DO UPDATE SET
            file = EXCLUDED.file, path = EXCLUDED.path, sha256 = EXCLUDED.sha256, bytes = EXCLUDED.bytes,
            height_m = EXCLUDED.height_m, radius_m = EXCLUDED.radius_m, license = EXCLUDED.license,
            author = EXCLUDED.author, source = EXCLUDED.source, attribution = EXCLUDED.attribution,
            updated_at = now()`
      }
      // The reach covers whichever LOD is larger, so a tile carries the
      // landmark wherever either could be drawn.
      const radius = Math.max(models.get(r.low.modelId)!.radius, r.detail ? models.get(r.detail.modelId)!.radius : 0)
      await tx`
        INSERT INTO landmarks (id, name, model_id, detail_model_id, detail_zoom, geom, reach, bearing, scale, elevation,
                               min_zoom, replaces, entrances, source_id, origin)
        SELECT ${r.id}, ${r.name}, ${r.low.modelId}, ${r.detail?.modelId ?? null}, ${r.detailZoom}, p.geom,
               ST_Expand(ST_Transform(p.geom, 3857), ${radius} / cos(radians(${r.lat}))),
               0, 1, 0, ${r.minZoom}, ${r.replaces}, ${r.entrances ? JSON.stringify(r.entrances) : null}::jsonb,
               ${r.sourceId}, ${OPEN_LANDMARKS_SOURCE}
        FROM (SELECT ST_SetSRID(ST_MakePoint(${r.lng}, ${r.lat}), 4326) AS geom) p
        ON CONFLICT (id) DO UPDATE SET
          name = EXCLUDED.name, model_id = EXCLUDED.model_id, detail_model_id = EXCLUDED.detail_model_id,
          detail_zoom = EXCLUDED.detail_zoom, geom = EXCLUDED.geom, reach = EXCLUDED.reach,
          min_zoom = EXCLUDED.min_zoom, replaces = EXCLUDED.replaces, entrances = EXCLUDED.entrances,
          source_id = EXCLUDED.source_id, updated_at = now()`
    }
    const ids = rows.map((r) => r.id)
    await tx`DELETE FROM landmarks WHERE origin = ${OPEN_LANDMARKS_SOURCE} AND NOT (id = ANY(${ids}))`
    await tx`
      DELETE FROM landmark_models m
      WHERE origin = ${OPEN_LANDMARKS_SOURCE} AND NOT (id = ANY(${[...models.keys()]}))
        AND NOT EXISTS (SELECT 1 FROM landmarks l WHERE l.model_id = m.id OR l.detail_model_id = m.id)`
    await tx`
      INSERT INTO landmark_sources (source, release, landmarks, imported_at)
      VALUES (${OPEN_LANDMARKS_SOURCE}, ${pointer.release}, ${rows.length}, now())
      ON CONFLICT (source) DO UPDATE SET release = EXCLUDED.release, landmarks = EXCLUDED.landmarks, imported_at = now()`
  })
  result.landmarks = rows.length

  result.superseded = (await resolveLandmarkConflicts()).superseded
  await refreshModelFiles()
  await pruneCache(config.cacheDir, new Set([...models.values()].map((m) => `${m.sha256}.glb`)))
  log(`  done; ${result.superseded} placements superseded by a higher-priority source`)
  return result
}

/** Drop cached GLBs no release row points at any more, and stray temp files. */
async function pruneCache(dir: string, keep: Set<string>): Promise<void> {
  for (const name of await readdir(dir).catch(() => [] as string[])) {
    if ((name.endsWith('.glb') && !keep.has(name)) || name.endsWith('.tmp')) await unlink(join(dir, name)).catch(() => {})
  }
}
