/**
 * 3D landmarks: hand-made models that stand in for a building's extrusion.
 *
 * Two things are stored, and they are kept apart on purpose:
 *
 *   models     a GLB and what is known about it — its size, licence, author.
 *   landmarks  a placement of a model: where, which way round, how big, and
 *              which OSM buildings it replaces.
 *
 * One model can be placed many times. The Eiffel Tower stands in Paris and,
 * at half scale, on the Las Vegas Strip; a chain's standard storefront could
 * stand in a thousand places. It is also the split a crowdsourced repository
 * would need: a model is reviewed once, a placement is a few numbers.
 *
 * Clients get the placements as a vector tile layer (`/tiles/landmarks`), so a
 * map engine loads them the way it loads everything else — by viewport, from
 * a cacheable URL — and fetches each model once, by a content-addressed name
 * that can be cached forever.
 *
 * `replaces` holds OSM refs (`way/5013364`) rather than a footprint because a
 * client hiding buildings by id is a filter expression; hiding them by
 * geometry, which is what Mapbox does, needs the engine to intersect every
 * extrusion with every landmark footprint, which MapLibre has no hook for.
 * The catch is that the list has to name every `building:part` as well as the
 * outline — see `landmarks/README.md`.
 *
 * Placements come from more than one source, each owning its own rows (the
 * `origin` column), and are merged in the database:
 *
 *   catalog        our own, in the repo (`landmarks/catalog.json`), synced at
 *                  startup.
 *   openlandmarks  the Open Landmarks dataset, imported from its published
 *                  releases — see `openlandmarks.service.ts`.
 *
 * Where two sources model the same building, `resolveLandmarkConflicts` keeps
 * the one whose source ranks first in LANDMARK_SOURCE_PRIORITY and marks the
 * other inactive, so the tiles never carry two models of one building. The
 * losing row stays, so changing the priority or withdrawing the winner brings
 * it back without a re-import.
 */
import { join, resolve } from 'path'
import { connection as sql } from '../db'
import { envString } from '../config/env'

/** Sources that own landmark rows, by the `origin` value they write. */
export type LandmarkOrigin = 'catalog' | 'openlandmarks'

/**
 * Which source wins when two model the same building, first wins. Open
 * Landmarks leads by default: its models are reviewed for the shared dataset,
 * and our catalog fills the gaps until its models are contributed there.
 */
export function landmarkSourcePriority(): LandmarkOrigin[] {
  const listed = envString('LANDMARK_SOURCE_PRIORITY', 'openlandmarks,catalog')
    .split(',').map((s) => s.trim()).filter((s): s is LandmarkOrigin => s === 'catalog' || s === 'openlandmarks')
  // A source left off the list still ranks, after the listed ones.
  for (const origin of ['openlandmarks', 'catalog'] as const) if (!listed.includes(origin)) listed.push(origin)
  return listed
}

export function resolveLandmarksDir(override?: string): string {
  return resolve(override || envString('LANDMARKS_DIR', './landmarks'))
}

/** The vector tile layer name, and the source name under /tiles. */
export const LANDMARKS_LAYER = 'landmarks'

/**
 * Below this, a landmark is a dot. The tiles still carry it from here so a
 * client can start fetching the model before it is big enough to draw.
 */
export const LANDMARKS_MIN_TILE_ZOOM = 12

const OSM_REF_RE = /^(node|way|relation)\/\d+$/
const SLUG_RE = /^[a-z0-9]+(-[a-z0-9]+)*$/

export type CatalogModel = {
  id: string
  /** Relative to the landmarks dir. */
  file: string
  license: string
  author: string
  /** Where the model came from — a generator script, a URL. */
  source?: string
  /**
   * The credit a map has to show while drawing it, for licences that require
   * one (CC-BY). Travels on every tile feature so a client can show it only
   * when the model is actually on screen.
   */
  attribution?: string
}

export type CatalogLandmark = {
  id: string
  name: string
  model: string
  lng: number
  lat: number
  /** Degrees clockwise from north that the model's -Z axis is turned to. */
  bearing?: number
  scale?: number
  /** Metres above the ground the model's origin sits. */
  elevation?: number
  /** Smallest zoom a client should draw it at. */
  minzoom?: number
  replaces?: string[]
  wikidata?: string
}

export type Catalog = { models: CatalogModel[]; landmarks: CatalogLandmark[] }

/**
 * Check a catalog before any of it reaches the database. Returns the problems
 * rather than throwing on the first, so one run reports everything wrong with
 * a pull request's catalog edit.
 */
export function validateCatalog(catalog: Catalog): string[] {
  const problems: string[] = []
  const models = new Set<string>()
  for (const m of catalog.models ?? []) {
    if (!SLUG_RE.test(m.id)) problems.push(`model "${m.id}": id must be a lowercase slug`)
    if (models.has(m.id)) problems.push(`model "${m.id}": duplicate id`)
    models.add(m.id)
    if (!m.file?.endsWith('.glb')) problems.push(`model "${m.id}": file must be a .glb`)
    if (m.file?.includes('..')) problems.push(`model "${m.id}": file must stay inside the landmarks dir`)
    if (!m.license) problems.push(`model "${m.id}": license is required`)
    if (/^CC-BY/i.test(m.license ?? '') && !m.attribution)
      problems.push(`model "${m.id}": a ${m.license} model needs an attribution`)
  }
  const ids = new Set<string>()
  for (const l of catalog.landmarks ?? []) {
    const at = `landmark "${l.id}"`
    if (!SLUG_RE.test(l.id)) problems.push(`${at}: id must be a lowercase slug`)
    if (ids.has(l.id)) problems.push(`${at}: duplicate id`)
    ids.add(l.id)
    if (!models.has(l.model)) problems.push(`${at}: unknown model "${l.model}"`)
    if (!(Math.abs(l.lng) <= 180 && Math.abs(l.lat) <= 85)) problems.push(`${at}: lng/lat out of range`)
    if (l.scale !== undefined && !(l.scale > 0)) problems.push(`${at}: scale must be positive`)
    for (const ref of l.replaces ?? [])
      if (!OSM_REF_RE.test(ref)) problems.push(`${at}: "${ref}" is not an OSM ref like way/123`)
  }
  return problems
}

/**
 * The model's extent in its own metres, from the POSITION accessors' min/max
 * — glTF requires them, so a model is sized without decoding any geometry.
 *
 * Ignores node transforms. That is the frame contract rather than a shortcut:
 * a landmark model is authored in place, origin at its anchor, so a
 * transform on its root would be a model that does not follow it.
 */
export function glbBounds(glb: Uint8Array): { height: number; radius: number } {
  const view = new DataView(glb.buffer, glb.byteOffset, glb.byteLength)
  if (view.getUint32(0, true) !== 0x46546c67) throw new Error('not a GLB')
  const length = view.getUint32(12, true)
  if (view.getUint32(16, true) !== 0x4e4f534a) throw new Error('GLB has no JSON chunk first')
  const json = JSON.parse(new TextDecoder().decode(glb.subarray(20, 20 + length)))

  let height = 0
  let radius = 0
  for (const mesh of json.meshes ?? [])
    for (const primitive of mesh.primitives ?? []) {
      const accessor = json.accessors?.[primitive.attributes?.POSITION]
      if (!accessor?.min || !accessor?.max) throw new Error('POSITION accessor has no min/max')
      const [x0, , z0] = accessor.min
      const [x1, y1, z1] = accessor.max
      height = Math.max(height, y1)
      // Farthest corner of the plan box from the origin: the tile query uses
      // it to send a landmark to every tile it overhangs, not just the one
      // its anchor falls in.
      for (const x of [x0, x1]) for (const z of [z0, z1]) radius = Math.max(radius, Math.hypot(x, z))
    }
  return { height, radius }
}

/**
 * The URL name a model is served under: its id plus a content hash. A new
 * GLB is a new name, so the old one can be cached as immutable.
 */
export function modelFileName(id: string, sha256: string): string {
  return `${id}.${sha256.slice(0, 12)}.glb`
}

export const MODEL_FILE_RE = /^[a-z0-9]+(-[a-z0-9]+)*\.[0-9a-f]{12}\.glb$/

let schemaReady: Promise<void> | null = null

export function ensureLandmarksSchema(): Promise<void> {
  schemaReady ??= (async () => {
    await sql`
      CREATE TABLE IF NOT EXISTS landmark_models (
        id         text PRIMARY KEY,
        file       text NOT NULL,
        sha256     text NOT NULL,
        bytes      integer NOT NULL,
        height_m   real NOT NULL,
        radius_m   real NOT NULL,
        license    text NOT NULL,
        author     text NOT NULL,
        source     text,
        origin     text NOT NULL DEFAULT 'catalog',
        updated_at timestamptz NOT NULL DEFAULT now()
      )`
    await sql`ALTER TABLE landmark_models ADD COLUMN IF NOT EXISTS attribution text`
    // Where the GLB is on disk, so any API process can serve any source's
    // models without that source being loaded in it.
    await sql`ALTER TABLE landmark_models ADD COLUMN IF NOT EXISTS path text`
    await sql`
      CREATE TABLE IF NOT EXISTS landmarks (
        fid        serial UNIQUE,
        id         text PRIMARY KEY,
        name       text NOT NULL,
        model_id   text NOT NULL REFERENCES landmark_models(id),
        geom       geometry(Point, 4326) NOT NULL,
        reach      geometry(Polygon, 3857) NOT NULL,
        bearing    real NOT NULL DEFAULT 0,
        scale      real NOT NULL DEFAULT 1,
        elevation  real NOT NULL DEFAULT 0,
        min_zoom   real NOT NULL DEFAULT 14,
        replaces   text[] NOT NULL DEFAULT '{}',
        wikidata   text,
        origin     text NOT NULL DEFAULT 'catalog',
        updated_at timestamptz NOT NULL DEFAULT now()
      )`
    // `reach` is the plan-view square the model can cover, in web mercator,
    // so the tile query is an index lookup rather than a scan.
    await sql`CREATE INDEX IF NOT EXISTS landmarks_reach_idx ON landmarks USING gist (reach)`
    // A finer model to switch to from `detail_zoom` up, where the source has one.
    await sql`ALTER TABLE landmarks ADD COLUMN IF NOT EXISTS detail_model_id text REFERENCES landmark_models(id)`
    await sql`ALTER TABLE landmarks ADD COLUMN IF NOT EXISTS detail_zoom real`
    // Points in the model's own axes where a lit entrance glows at night.
    await sql`ALTER TABLE landmarks ADD COLUMN IF NOT EXISTS entrances jsonb`
    // The landmark's id in its source, when that differs from ours.
    await sql`ALTER TABLE landmarks ADD COLUMN IF NOT EXISTS source_id text`
    await sql`ALTER TABLE landmarks ADD COLUMN IF NOT EXISTS active boolean NOT NULL DEFAULT true`
    await sql`ALTER TABLE landmarks ADD COLUMN IF NOT EXISTS superseded_by text`
    // What each imported source last brought in, so an unchanged release is
    // recognised from its pointer alone.
    await sql`
      CREATE TABLE IF NOT EXISTS landmark_sources (
        source      text PRIMARY KEY,
        release     text,
        landmarks   integer NOT NULL DEFAULT 0,
        imported_at timestamptz NOT NULL DEFAULT now()
      )`
  })()
  return schemaReady
}

/** Served model files by URL name, rebuilt by every sync. */
let modelFiles = new Map<string, string>()

/**
 * Bring the database in line with the catalog on disk.
 *
 * Only rows the catalog owns (`origin = 'catalog'`) are touched, so a removal
 * from the file is a removal here, and anything added some other way survives.
 */
export async function syncLandmarkCatalog(dir = resolveLandmarksDir()): Promise<{ models: number; landmarks: number }> {
  await ensureLandmarksSchema()
  const file = Bun.file(join(dir, 'catalog.json'))
  if (!(await file.exists())) return { models: 0, landmarks: 0 }

  const catalog = (await file.json()) as Catalog
  const problems = validateCatalog(catalog)
  if (problems.length) throw new Error(`landmarks/catalog.json:\n  ${problems.join('\n  ')}`)

  const models: Array<CatalogModel & { sha256: string; bytes: number; height: number; radius: number; path: string }> = []
  for (const m of catalog.models) {
    const path = join(dir, m.file)
    const bytes = new Uint8Array(await Bun.file(path).arrayBuffer())
    const sha256 = new Bun.CryptoHasher('sha256').update(bytes).digest('hex')
    models.push({ ...m, path, sha256, bytes: bytes.length, ...glbBounds(bytes) })
  }

  await sql.begin(async (rawTx) => {
    // postgres-js types a transaction as not callable as a tagged template,
    // though it is; see boundary-catalog.service.ts.
    const tx = rawTx as unknown as typeof sql
    for (const m of models) {
      await tx`
        INSERT INTO landmark_models (id, file, path, sha256, bytes, height_m, radius_m, license, author, source, attribution, origin)
        VALUES (${m.id}, ${modelFileName(m.id, m.sha256)}, ${m.path}, ${m.sha256}, ${m.bytes}, ${m.height}, ${m.radius},
                ${m.license}, ${m.author}, ${m.source ?? null}, ${m.attribution ?? null}, 'catalog')
        ON CONFLICT (id) DO UPDATE SET
          file = EXCLUDED.file, path = EXCLUDED.path, sha256 = EXCLUDED.sha256, bytes = EXCLUDED.bytes,
          height_m = EXCLUDED.height_m, radius_m = EXCLUDED.radius_m, license = EXCLUDED.license,
          author = EXCLUDED.author, source = EXCLUDED.source, attribution = EXCLUDED.attribution,
          updated_at = now()`
    }
    for (const l of catalog.landmarks) {
      const scale = l.scale ?? 1
      await tx`
        INSERT INTO landmarks (id, name, model_id, geom, reach, bearing, scale, elevation, min_zoom, replaces, wikidata, origin)
        SELECT ${l.id}, ${l.name}, m.id, p.geom,
               -- Mercator stretches a metre by 1/cos(lat), so the reach does too.
               ST_Expand(ST_Transform(p.geom, 3857), m.radius_m * ${scale} / cos(radians(${l.lat}))),
               ${l.bearing ?? 0}, ${scale}, ${l.elevation ?? 0}, ${l.minzoom ?? 14},
               ${l.replaces ?? []}, ${l.wikidata ?? null}, 'catalog'
        FROM landmark_models m, (SELECT ST_SetSRID(ST_MakePoint(${l.lng}, ${l.lat}), 4326) AS geom) p
        WHERE m.id = ${l.model}
        ON CONFLICT (id) DO UPDATE SET
          name = EXCLUDED.name, model_id = EXCLUDED.model_id, geom = EXCLUDED.geom, reach = EXCLUDED.reach,
          bearing = EXCLUDED.bearing, scale = EXCLUDED.scale, elevation = EXCLUDED.elevation,
          min_zoom = EXCLUDED.min_zoom, replaces = EXCLUDED.replaces, wikidata = EXCLUDED.wikidata,
          updated_at = now()`
    }
    const landmarkIds = catalog.landmarks.map((l) => l.id)
    const modelIds = catalog.models.map((m) => m.id)
    await tx`DELETE FROM landmarks WHERE origin = 'catalog' AND NOT (id = ANY(${landmarkIds}))`
    await tx`
      DELETE FROM landmark_models m
      WHERE origin = 'catalog' AND NOT (id = ANY(${modelIds}))
        AND NOT EXISTS (SELECT 1 FROM landmarks l WHERE l.model_id = m.id)`
  })

  await resolveLandmarkConflicts()
  await refreshModelFiles()
  return { models: models.length, landmarks: catalog.landmarks.length }
}

/**
 * Mark which placements are drawn. Two placements from different sources
 * clash when they replace a building in common, share a Wikidata item, or
 * stand within 30 m of each other; the lower-ranked one goes inactive,
 * pointing at the one that won. Placements from the same source never clash:
 * a source is trusted not to model its own building twice.
 */
export async function resolveLandmarkConflicts(): Promise<{ superseded: number }> {
  const priority = landmarkSourcePriority()
  return sql.begin(async (rawTx) => {
    const tx = rawTx as unknown as typeof sql
    await tx`UPDATE landmarks SET active = true, superseded_by = NULL WHERE NOT active OR superseded_by IS NOT NULL`
    const rows = await tx`
      UPDATE landmarks loser SET active = false, superseded_by = winner.id
      FROM landmarks winner
      WHERE winner.origin <> loser.origin
        AND array_position(${priority}::text[], winner.origin) < array_position(${priority}::text[], loser.origin)
        AND (winner.replaces && loser.replaces
             OR winner.wikidata = loser.wikidata
             OR ST_DWithin(winner.geom::geography, loser.geom::geography, 30))
      RETURNING loser.id`
    return { superseded: rows.length }
  })
}

/** Rebuild the served-file lookup from the database, for every source. */
export async function refreshModelFiles(): Promise<void> {
  const rows = await sql<{ file: string; path: string }[]>`SELECT file, path FROM landmark_models WHERE path IS NOT NULL`
  modelFiles = new Map(rows.map((r) => [r.file, r.path]))
}

/** Where a served model name lives on disk, or null if it is not one we serve. */
export function modelPath(name: string): string | null {
  if (!MODEL_FILE_RE.test(name)) return null
  return modelFiles.get(name) ?? null
}

/**
 * One vector tile of landmark placements.
 *
 * A landmark is a point, but it is sent to every tile its model overhangs
 * (`reach`), with its point allowed to fall outside the tile (a full tile's
 * buffer). Otherwise a tower whose anchor is just off screen would be missing
 * while its upper half filled the view — the client draws whatever the
 * visible tiles hold, and de-duplicates by feature id.
 *
 * MVT has no array type, so `replaces` travels as a space-separated string
 * and `entrances` as JSON text.
 */
export async function landmarkTile(z: number, x: number, y: number): Promise<Uint8Array> {
  if (z < LANDMARKS_MIN_TILE_ZOOM) return new Uint8Array()
  const [row] = await sql`
    WITH bounds AS (SELECT ST_TileEnvelope(${z}, ${x}, ${y}) AS env)
    SELECT ST_AsMVT(t, ${LANDMARKS_LAYER}, 4096, 'geom', 'fid') AS mvt FROM (
      SELECT l.fid, l.id, l.name, m.file AS model, d.file AS detail, l.detail_zoom AS detailzoom,
             l.bearing, l.scale, l.elevation, l.min_zoom AS minzoom,
             round((m.height_m * l.scale)::numeric, 1)::real AS height,
             array_to_string(l.replaces, ' ') AS replaces, l.wikidata, m.attribution,
             l.entrances::text AS entrances, l.origin AS source,
             ST_AsMVTGeom(ST_Transform(l.geom, 3857), bounds.env, 4096, 4096, true) AS geom
      FROM landmarks l
      JOIN landmark_models m ON m.id = l.model_id
      LEFT JOIN landmark_models d ON d.id = l.detail_model_id
      CROSS JOIN bounds
      WHERE l.reach && bounds.env AND l.active
    ) t
    WHERE geom IS NOT NULL`
  return new Uint8Array(row?.mvt ?? [])
}
