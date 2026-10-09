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
 * outline; see the landmarks repo's README.
 *
 * Placements come from more than one source, each owning its own rows (the
 * `origin` column), and are merged in the database. Every source publishes in
 * the Open Landmarks release format and is imported the same way; see
 * `landmark-import.service.ts`:
 *
 *   openlandmarks  the Open Landmarks dataset.
 *   barrelman      Barrelman's own models, from github.com/alexwohlbruck/landmarks.
 *
 * Where two sources model the same building, `resolveLandmarkConflicts` keeps
 * the one whose source ranks first in LANDMARK_SOURCE_PRIORITY and marks the
 * other inactive, so the tiles never carry two models of one building. The
 * losing row stays, so changing the priority or withdrawing the winner brings
 * it back without a re-import.
 */
import { connection as sql } from '../db'
import { envString } from '../config/env'

/** Every source that can own landmark rows, by the `origin` value it writes. */
export const LANDMARK_SOURCE_IDS = ['openlandmarks', 'barrelman'] as const
export type LandmarkSourceId = (typeof LANDMARK_SOURCE_IDS)[number]

/**
 * Which source wins when two model the same building, first wins. Open
 * Landmarks leads by default: its models are reviewed for the shared dataset,
 * and ours fill the gaps until they are contributed there.
 */
export function landmarkSourcePriority(): LandmarkSourceId[] {
  const listed: LandmarkSourceId[] = []
  for (const raw of envString('LANDMARK_SOURCE_PRIORITY', 'openlandmarks,barrelman').split(',')) {
    // `catalog` was the bundled catalog's name before it moved to its own
    // repo; a priority written for it means the same models.
    const name = raw.trim() === 'catalog' ? 'barrelman' : raw.trim()
    if ((LANDMARK_SOURCE_IDS as readonly string[]).includes(name) && !listed.includes(name as LandmarkSourceId))
      listed.push(name as LandmarkSourceId)
  }
  // A source left off the list still ranks, after the listed ones.
  for (const id of LANDMARK_SOURCE_IDS) if (!listed.includes(id)) listed.push(id)
  return listed
}

/** The vector tile layer name, and the source name under /tiles. */
export const LANDMARKS_LAYER = 'landmarks'

/**
 * Below this, a landmark is a dot. The tiles still carry it from here so a
 * client can start fetching the model before it is big enough to draw.
 */
export const LANDMARKS_MIN_TILE_ZOOM = 12

/**
 * The model's extent in its own metres: `height` above the origin, and
 * `radius`, the farthest it reaches from the origin in plan.
 *
 * Static geometry is sized from the POSITION accessors' min/max, which glTF
 * requires, so no geometry is decoded. Each box's corners are carried
 * through the node transforms above it. A model authored in place has only
 * an untransformed root, so for almost every landmark that changes nothing.
 *
 * A node that an animation moves (experimental, outside the Open Landmarks
 * contract) can be anywhere its clip takes it. It is bounded by a sphere
 * about its pivot that holds everything under it in any pose. Its radius
 * comes from the actual vertices, since a box corner would overstate a
 * wheel's reach by √2. A pivot that itself moves on LINEAR or STEP keyframes
 * never leaves the box around them, so the sphere is swept over that box;
 * otherwise it is centred on the parent's origin and grown by the farthest
 * keyframe, which for a lift would turn its height into plan reach.
 */
export function glbBounds(glb: Uint8Array): { height: number; radius: number } {
  const view = new DataView(glb.buffer, glb.byteOffset, glb.byteLength)
  if (view.getUint32(0, true) !== 0x46546c67) throw new Error('not a GLB')
  const length = view.getUint32(12, true)
  if (view.getUint32(16, true) !== 0x4e4f534a) throw new Error('GLB has no JSON chunk first')
  const json = JSON.parse(new TextDecoder().decode(glb.subarray(20, 20 + length)))
  const binAt = 20 + length + ((4 - (length % 4)) % 4)
  const bin = binAt + 8 <= glb.length && view.getUint32(binAt + 4, true) === 0x004e4942
    ? glb.subarray(binAt + 8, binAt + 8 + view.getUint32(binAt, true))
    : null

  const nodes: any[] = json.nodes ?? []
  const boxOf = (primitive: any) => {
    const accessor = json.accessors?.[primitive.attributes?.POSITION]
    if (!accessor?.min || !accessor?.max) throw new Error('POSITION accessor has no min/max')
    return accessor as { min: number[]; max: number[] }
  }
  const corners = ({ min, max }: { min: number[]; max: number[] }) =>
    [0, 1, 2, 3, 4, 5, 6, 7].map((i) => [i & 1 ? max[0] : min[0], i & 2 ? max[1] : min[1], i & 4 ? max[2] : min[2]])

   // What each animated node's channels do: the farthest any translation
  // keyframe takes it, the box its keyframes span (while every translation
  // channel interpolates within it), and the largest scale any reaches.
  type Motion = { translation?: number; scale?: number; path?: { min: number[]; max: number[] } | null }
  const moving = new Map<number, Motion>()
  for (const animation of json.animations ?? [])
    for (const channel of animation.channels ?? []) {
      const node = channel.target?.node
      if (node === undefined) continue
      const entry = moving.get(node) ?? {}
      moving.set(node, entry)
      const path: string = channel.target.path
      if (path !== 'translation' && path !== 'scale') continue
      const values = floats(json, bin, animation.samplers?.[channel.sampler]?.output)
      // Keyframes it cannot read leave nothing to bound the node by.
      if (!values) throw new Error(`animated ${path} on node ${node} is not readable`)
      let most = entry[path] ?? 0
      for (let i = 0; i + 2 < values.length; i += 3) {
        const [x, y, z] = [values[i], values[i + 1], values[i + 2]]
        most = Math.max(most, path === 'translation' ? Math.hypot(x, y, z) : Math.max(Math.abs(x), Math.abs(y), Math.abs(z)))
      }
      entry[path] = most
      if (path !== 'translation' || entry.path === null) continue
      // A cubic spline can overshoot its keyframes, and its output also
      // carries tangents, so it gets no box.
      const interpolation = animation.samplers?.[channel.sampler]?.interpolation ?? 'LINEAR'
      if (interpolation !== 'LINEAR' && interpolation !== 'STEP') {
        entry.path = null
        continue
      }
      const box = entry.path ?? { min: [Infinity, Infinity, Infinity], max: [-Infinity, -Infinity, -Infinity] }
      for (let i = 0; i + 2 < values.length; i += 3)
        for (let k = 0; k < 3; k++) {
          box.min[k] = Math.min(box.min[k], values[i + k])
          box.max[k] = Math.max(box.max[k], values[i + k])
        }
      entry.path = box
    }

  /**
   * The farthest anything under a node reaches from its own origin, in any
   * pose, in its own frame (before its own transform).
   */
  const reach = (index: number): number => {
    const node = nodes[index]
    let most = 0
    for (const primitive of node.mesh !== undefined ? json.meshes?.[node.mesh]?.primitives ?? [] : []) {
      const position = floats(json, bin, primitive.attributes?.POSITION)
      if (position)
        for (let i = 0; i + 2 < position.length; i += 3)
          most = Math.max(most, Math.hypot(position[i], position[i + 1], position[i + 2]))
      else for (const c of corners(boxOf(primitive))) most = Math.max(most, Math.hypot(c[0], c[1], c[2]))
    }
    for (const child of node.children ?? []) {
      const m = nodeMatrix(nodes[child])
      const motion = moving.get(child)
      const shift = Math.max(Math.hypot(m[12], m[13], m[14]), motion?.translation ?? 0)
      most = Math.max(most, shift + Math.max(scaleOf(m), motion?.scale ?? 0) * reach(child))
    }
    return most
  }

  let height = 0
  let radius = 0
  const visit = (index: number, parent: number[]) => {
    const node = nodes[index]
    const motion = moving.get(index)
    if (motion) {
      // The sphere's centre is the node's pivot. A pivot that moves sweeps
      // the box of its keyframes, so the sphere is placed at each corner;
      // without a box, it is centred on the parent's origin instead and
      // grown by the farthest keyframe.
      const own = nodeMatrix(node)
      const scale = Math.max(scaleOf(own), motion.scale ?? 0)
      const swept = motion.translation !== undefined && motion.path ? motion.path : null
      const pivots = motion.translation === undefined ? [[own[12], own[13], own[14]]]
        : swept ? corners(swept) : [[0, 0, 0]]
      const r = scaleOf(parent) * ((swept ? 0 : motion.translation ?? 0) + scale * reach(index))
      for (const pivot of pivots) {
        const [cx, cy, cz] = transform(parent, pivot)
        height = Math.max(height, cy + r)
        radius = Math.max(radius, Math.hypot(cx, cz) + r)
      }
      return
    }
    const world = multiply(parent, nodeMatrix(node))
    for (const primitive of node.mesh !== undefined ? json.meshes?.[node.mesh]?.primitives ?? [] : [])
      for (const corner of corners(boxOf(primitive))) {
        const [x, y, z] = transform(world, corner)
        height = Math.max(height, y)
        // Farthest corner of the plan box from the origin: the tile query
        // uses it to send a landmark to every tile it overhangs, not just
        // the one its anchor falls in.
        radius = Math.max(radius, Math.hypot(x, z))
      }
    for (const child of node.children ?? []) visit(child, world)
  }
  // No scene: every node no other node claims as a child is a root.
  const children = new Set(nodes.flatMap((n) => n.children ?? []))
  const roots = json.scenes?.[json.scene ?? 0]?.nodes ?? nodes.map((_, i) => i).filter((i) => !children.has(i))
  for (const root of roots) visit(root, IDENTITY)
  return { height, radius }
}

const IDENTITY = [1, 0, 0, 0, 0, 1, 0, 0, 0, 0, 1, 0, 0, 0, 0, 1]

/** A node's local transform, column-major: its matrix, or T · R · S. */
function nodeMatrix(node: any): number[] {
  if (node?.matrix) return node.matrix
  const [x, y, z, w] = node?.rotation ?? [0, 0, 0, 1]
  const [sx, sy, sz] = node?.scale ?? [1, 1, 1]
  const [tx, ty, tz] = node?.translation ?? [0, 0, 0]
  return [
    (1 - 2 * (y * y + z * z)) * sx, 2 * (x * y + w * z) * sx, 2 * (x * z - w * y) * sx, 0,
    2 * (x * y - w * z) * sy, (1 - 2 * (x * x + z * z)) * sy, 2 * (y * z + w * x) * sy, 0,
    2 * (x * z + w * y) * sz, 2 * (y * z - w * x) * sz, (1 - 2 * (x * x + y * y)) * sz, 0,
    tx, ty, tz, 1,
  ]
}

function multiply(a: number[], b: number[]): number[] {
  const out = new Array(16).fill(0)
  for (let c = 0; c < 4; c++)
    for (let r = 0; r < 4; r++)
      for (let k = 0; k < 4; k++) out[c * 4 + r] += a[k * 4 + r] * b[c * 4 + k]
  return out
}

function transform(m: number[], [x, y, z]: number[]): number[] {
  return [0, 1, 2].map((r) => m[r] * x + m[4 + r] * y + m[8 + r] * z + m[12 + r])
}

/** The most a matrix stretches any length: its longest basis column. */
function scaleOf(m: number[]): number {
  return Math.max(Math.hypot(m[0], m[1], m[2]), Math.hypot(m[4], m[5], m[6]), Math.hypot(m[8], m[9], m[10]))
}

/**
 * A float accessor's values, or null when it is not one this can read
 * (no BIN chunk, not float, interleaved or sparse).
 */
function floats(json: any, bin: Uint8Array | null, index: number | undefined): Float32Array | null {
  const accessor = index === undefined ? undefined : json.accessors?.[index]
  if (!bin || !accessor || accessor.componentType !== 5126 || accessor.sparse || accessor.bufferView === undefined) return null
  const view = json.bufferViews?.[accessor.bufferView]
  const per = ({ SCALAR: 1, VEC2: 2, VEC3: 3, VEC4: 4 } as Record<string, number>)[accessor.type]
  if (!view || !per || (view.byteStride && view.byteStride !== 4 * per)) return null
  const start = (view.byteOffset ?? 0) + (accessor.byteOffset ?? 0)
  const bytes = bin.slice(start, start + accessor.count * per * 4)
  return bytes.length === accessor.count * per * 4 ? new Float32Array(bytes.buffer) : null
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
        origin     text NOT NULL,
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
        origin     text NOT NULL,
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

/** How often a lookup miss may reload the model index from the database. */
export const MODEL_INDEX_RELOAD_MS = 10_000

/**
 * Served model files by URL name, held in memory so a model request never
 * waits on the database.
 *
 * An import rebuilds it, but only in its own process. `bun run
 * landmarks:import` runs beside the API, not in it, so the API's copy went
 * stale and every new model 404'd until a restart. A miss therefore reloads
 * the index from the database before answering. Names are content-addressed,
 * so a miss for a well-formed name almost always means an import added the
 * model since the last load — there is nothing else for a stale index to get
 * wrong. Reloads are rate-limited, and concurrent misses share one, so a
 * client asking for names that do not exist cannot turn each request into a
 * query.
 */
export function createModelIndex(opts: {
  load: () => Promise<Iterable<[string, string]>>
  reloadMs?: number
  now?: () => number
}) {
  const reloadMs = opts.reloadMs ?? MODEL_INDEX_RELOAD_MS
  const now = opts.now ?? Date.now
  let files = new Map<string, string>()
  let loadedAt = -Infinity
  let loading: Promise<void> | null = null

  async function refresh(): Promise<void> {
    loadedAt = now()
    files = new Map(await opts.load())
  }

  async function path(name: string): Promise<string | null> {
    if (!MODEL_FILE_RE.test(name)) return null
    const hit = files.get(name)
    if (hit) return hit
    if (!loading && now() - loadedAt >= reloadMs) {
      loading = refresh()
        .catch((err) => console.error('[landmarks] model index reload failed:', err))
        .finally(() => { loading = null })
    }
    if (loading) await loading
    return files.get(name) ?? null
  }

  return { refresh, path }
}

const modelIndex = createModelIndex({
  load: async () => {
    const rows = await sql<{ file: string; path: string }[]>`SELECT file, path FROM landmark_models WHERE path IS NOT NULL`
    return rows.map((r) => [r.file, r.path] as [string, string])
  },
})

/** Rebuild the served-file lookup from the database, for every source. */
export const refreshModelFiles = modelIndex.refresh

/** Where a served model name lives on disk, or null if it is not one we serve. */
export const modelPath = modelIndex.path

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
             -- How high it reaches above the ground it stands on: a model
             -- raised or sunk by its elevation reaches that much higher or lower.
             round((m.height_m * l.scale + l.elevation)::numeric, 1)::real AS height,
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
