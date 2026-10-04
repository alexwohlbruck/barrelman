/**
 * A small kit for building stylised landmark models in code and writing them
 * as GLB, with no dependencies.
 *
 * Geometry is authored in a map-friendly frame — x east, y north, z up,
 * metres, origin at the anchor on the ground — and converted to glTF's Y-up
 * frame on write. That is the frame contract every landmark GLB follows (see
 * `landmarks/README.md`): +Y up, -Z north, +X east, metres, origin at the
 * anchor, so a client places a model with nothing but a position, a bearing
 * and a scale.
 *
 * `Part` emits a fresh vertex per triangle corner, so a face is flat-shaded
 * unless explicit normals are given; `writeGlb` then merges the corners that
 * came out identical, so smooth surfaces cost what they should.
 */
import { deflateSync } from 'node:zlib'

export type V3 = [number, number, number]

export const sub = (a: V3, b: V3): V3 => [a[0] - b[0], a[1] - b[1], a[2] - b[2]]
export const cross = (a: V3, b: V3): V3 => [
  a[1] * b[2] - a[2] * b[1],
  a[2] * b[0] - a[0] * b[2],
  a[0] * b[1] - a[1] * b[0],
]
export const len = (a: V3) => Math.hypot(a[0], a[1], a[2])
export const mid = (a: V3, b: V3): V3 => [(a[0] + b[0]) / 2, (a[1] + b[1]) / 2, (a[2] + b[2]) / 2]
export const lerp = (a: number, b: number, t: number) => a + (b - a) * t

const normalize = (a: V3): V3 => {
  const l = len(a) || 1
  return [a[0] / l, a[1] / l, a[2] / l]
}

/** A square ring at height `z`, corners counter-clockwise from above. */
export const square = (half: number, z: number): V3[] => [
  [-half, -half, z],
  [half, -half, z],
  [half, half, z],
  [-half, half, z],
]

/** One material's worth of triangles. */
export class Part {
  pos: number[] = []
  nrm: number[] = []
  uv: number[] = []

  get triangles() {
    return this.pos.length / 9
  }

  tri(a: V3, b: V3, c: V3, ua: number[] = [0, 0], ub: number[] = [0, 0], uc: number[] = [0, 0], normals?: V3[]) {
    const n = normalize(cross(sub(b, a), sub(c, a)))
    const corners = [[a, ua], [b, ub], [c, uc]] as [V3, number[]][]
    corners.forEach(([p, t], i) => {
      const m = normals?.[i] ?? n
      // map frame → glTF frame: (x, y, z) → (x, z, -y). A rotation, so the
      // counter-clockwise winding survives the trip.
      this.pos.push(p[0], p[2], -p[1])
      this.nrm.push(m[0], m[2], -m[1])
      this.uv.push(t[0], t[1])
    })
  }

  /** Counter-clockwise as seen from the side the face points to. */
  quad(a: V3, b: V3, c: V3, d: V3, uv = [[0, 0], [1, 0], [1, 1], [0, 1]]) {
    this.tri(a, b, c, uv[0], uv[1], uv[2])
    this.tri(a, c, d, uv[0], uv[2], uv[3])
  }

  /**
   * The sides of a lofted closed ring: `rings[k]` are the same corners at
   * successive levels, counter-clockwise from above.
   *
   * `cell` turns on lattice texturing: u runs 0–1 across each face and v
   * advances by the face's length over its width, so a texture cell stays
   * roughly square however the face tapers.
   */
  loft(rings: V3[][], cell?: number) {
    const n = rings[0].length
    for (let i = 0; i < n; i++) {
      const j = (i + 1) % n
      let v = 0
      for (let k = 0; k < rings.length - 1; k++) {
        const b0 = rings[k][i], b1 = rings[k][j]
        const t0 = rings[k + 1][i], t1 = rings[k + 1][j]
        const width = (len(sub(b1, b0)) + len(sub(t1, t0))) / 2
        const dv = cell ? len(sub(mid(t0, t1), mid(b0, b1))) / (width * cell) : 1
        this.quad(b0, b1, t1, t0, [[0, v], [1, v], [1, v + dv], [0, v + dv]])
        v += dv
      }
    }
  }

  /** A flat cap over a convex ring, facing up or down. */
  cap(ring: V3[], up: boolean) {
    const pts = up ? ring : [...ring].reverse()
    for (let i = 1; i < pts.length - 1; i++) this.tri(pts[0], pts[i], pts[i + 1])
  }

  /**
   * A square slab between two heights, solid or — with `inner` — a hollow
   * ring, which is what an observation deck around a lattice shaft is.
   */
  slab(z0: number, z1: number, outer: number, inner = 0) {
    this.loft([square(outer, z0), square(outer, z1)])
    if (inner <= 0) {
      this.cap(square(outer, z1), true)
      this.cap(square(outer, z0), false)
      return
    }
    // The inner walls face the hole, so their ring runs the other way round.
    this.loft([square(inner, z0).reverse(), square(inner, z1).reverse()])
    for (const [z, up] of [[z1, true], [z0, false]] as [number, boolean][]) {
      const o = square(outer, z), i = square(inner, z)
      for (let k = 0; k < 4; k++) {
        const l = (k + 1) % 4
        if (up) this.quad(i[k], o[k], o[l], i[l])
        else this.quad(i[k], i[l], o[l], o[k])
      }
    }
  }

  /**
   * A rectangular tube swept along a path, for arches and braces. Each
   * section is four points in order around the tube; both windings are
   * emitted so a thin band never shows a hole from behind.
   */
  sweep(sections: V3[][]) {
    for (let k = 0; k < sections.length - 1; k++) {
      const r0 = sections[k], r1 = sections[k + 1]
      for (let i = 0; i < 4; i++) {
        const j = (i + 1) % 4
        this.quad(r0[j], r0[i], r1[i], r1[j])
        this.quad(r0[i], r0[j], r1[j], r1[i])
      }
    }
  }
}

/** 8-bit RGBA → PNG, the only image format a GLB here needs. */
export function encodePng(width: number, height: number, rgba: Uint8Array): Uint8Array {
  const table = Array.from({ length: 256 }, (_, n) => {
    let c = n
    for (let k = 0; k < 8; k++) c = c & 1 ? 0xedb88320 ^ (c >>> 1) : c >>> 1
    return c >>> 0
  })
  const crc = (bytes: Uint8Array) => {
    let c = 0xffffffff
    for (const b of bytes) c = table[(c ^ b) & 0xff] ^ (c >>> 8)
    return (c ^ 0xffffffff) >>> 0
  }
  const chunk = (type: string, data: Uint8Array) => {
    const out = new Uint8Array(12 + data.length)
    const view = new DataView(out.buffer)
    view.setUint32(0, data.length)
    out.set(new TextEncoder().encode(type), 4)
    out.set(data, 8)
    view.setUint32(8 + data.length, crc(out.subarray(4, 8 + data.length)))
    return out
  }
  const header = new Uint8Array(13)
  const view = new DataView(header.buffer)
  view.setUint32(0, width)
  view.setUint32(4, height)
  header.set([8, 6, 0, 0, 0], 8) // 8-bit, RGBA, deflate, no filter, no interlace
  const raw = new Uint8Array(height * (width * 4 + 1))
  for (let y = 0; y < height; y++)
    raw.set(rgba.subarray(y * width * 4, (y + 1) * width * 4), y * (width * 4 + 1) + 1)
  return concat([
    new Uint8Array([137, 80, 78, 71, 13, 10, 26, 10]),
    chunk('IHDR', header),
    chunk('IDAT', deflateSync(raw)),
    chunk('IEND', new Uint8Array()),
  ])
}

export type MaterialSpec = {
  name: string
  /** sRGB hex, as a designer would write it. */
  color: number
  roughness?: number
  doubleSided?: boolean
  /** A PNG whose alpha cuts the surface — lattice, railings, glazing bars. */
  mask?: { png: Uint8Array; cutoff?: number }
  /**
   * A PNG painted onto the surface, multiplied by `color` (leave that white
   * to use the image's colours as they are). How a facade carries detail too
   * fine for geometry — a grid of punched windows, brick banding, ribs —
   * without aliasing: the renderer mipmaps it, so a far facade averages to
   * the right tone instead of shimmering. Uses the part's UVs; `loft` with
   * `cell` and `quad` with explicit UVs set them.
   */
  texture?: { png: Uint8Array }
}

/**
 * Write parts as a single-mesh GLB, one primitive per material.
 *
 * Indexed, with identical corners merged — see `weld`. Some readers
 * (Parchment's own among them) only take indexed triangles anyway.
 */
export function writeGlb(
  name: string,
  parts: Array<{ part: Part; material: MaterialSpec }>,
  extras: Record<string, unknown> = {},
): Uint8Array {
  const chunks: Uint8Array[] = []
  let byteLength = 0
  const bufferViews: any[] = []
  const accessors: any[] = []

  const push = (bytes: Uint8Array, target?: number) => {
    const pad = (4 - (byteLength % 4)) % 4
    if (pad) {
      chunks.push(new Uint8Array(pad))
      byteLength += pad
    }
    bufferViews.push({ buffer: 0, byteOffset: byteLength, byteLength: bytes.length, ...(target ? { target } : {}) })
    chunks.push(bytes)
    byteLength += bytes.length
    return bufferViews.length - 1
  }

  const accessor = (data: Float32Array | Uint16Array | Uint32Array, type: string, target: number, bounds = false) => {
    const view = push(new Uint8Array(data.buffer, data.byteOffset, data.byteLength), target)
    const per = type === 'VEC3' ? 3 : type === 'VEC2' ? 2 : 1
    const entry: any = {
      bufferView: view,
      componentType: data instanceof Float32Array ? 5126 : data instanceof Uint16Array ? 5123 : 5125,
      count: data.length / per,
      type,
    }
    if (bounds) {
      // glTF requires min/max on POSITION; the server also reads them to size
      // a landmark without parsing its geometry.
      const min = Array(per).fill(Infinity), max = Array(per).fill(-Infinity)
      for (let i = 0; i < data.length; i++) {
        min[i % per] = Math.min(min[i % per], data[i])
        max[i % per] = Math.max(max[i % per], data[i])
      }
      entry.min = min
      entry.max = max
    }
    accessors.push(entry)
    return accessors.length - 1
  }

  const images: any[] = []
  const textures: any[] = []
  const materials: any[] = []
  const primitives: any[] = []

  const linear = (hex: number) =>
    [16, 8, 0].map((shift) => Math.pow(((hex >> shift) & 255) / 255, 2.2))

  for (const { part, material } of parts) {
    if (!part.triangles) continue
    const m: any = {
      name: material.name,
      pbrMetallicRoughness: {
        baseColorFactor: [...linear(material.color), 1],
        metallicFactor: 0,
        roughnessFactor: material.roughness ?? 0.85,
      },
    }
    if (material.doubleSided) m.doubleSided = true
    if (material.texture && !material.mask) {
      images.push({ bufferView: push(material.texture.png), mimeType: 'image/png' })
      textures.push({ source: images.length - 1 })
      m.pbrMetallicRoughness.baseColorTexture = { index: textures.length - 1 }
    }
    if (material.mask) {
      images.push({ bufferView: push(material.mask.png), mimeType: 'image/png' })
      textures.push({ source: images.length - 1 })
      m.pbrMetallicRoughness.baseColorTexture = { index: textures.length - 1 }
      m.alphaMode = 'MASK'
      m.alphaCutoff = material.mask.cutoff ?? 0.5
    }
    materials.push(m)

    const textured = !!(material.mask || material.texture)
    const { pos, nrm, uv, index } = weld(part, textured)
    primitives.push({
      attributes: {
        POSITION: accessor(pos, 'VEC3', 34962, true),
        NORMAL: accessor(nrm, 'VEC3', 34962),
        ...(textured ? { TEXCOORD_0: accessor(uv, 'VEC2', 34962) } : {}),
      },
      indices: accessor(index, 'SCALAR', 34963),
      material: materials.length - 1,
    })
  }

  const json = {
    asset: { version: '2.0', generator: 'barrelman scripts/landmarks', extras },
    scene: 0,
    scenes: [{ name, nodes: [0] }],
    nodes: [{ name, mesh: 0 }],
    meshes: [{ name, primitives }],
    materials,
    ...(textures.length ? { textures, images } : {}),
    accessors,
    bufferViews,
    buffers: [{ byteLength: align4(byteLength) }],
  }

  const bin = concat(chunks, align4(byteLength))
  const jsonBytes = padTo4(new TextEncoder().encode(JSON.stringify(json)), 0x20)
  const glb = new Uint8Array(12 + 8 + jsonBytes.length + 8 + bin.length)
  const view = new DataView(glb.buffer)
  view.setUint32(0, 0x46546c67, true) // "glTF"
  view.setUint32(4, 2, true)
  view.setUint32(8, glb.length, true)
  view.setUint32(12, jsonBytes.length, true)
  view.setUint32(16, 0x4e4f534a, true) // JSON
  glb.set(jsonBytes, 20)
  const at = 20 + jsonBytes.length
  view.setUint32(at, bin.length, true)
  view.setUint32(at + 4, 0x004e4942, true) // BIN
  glb.set(bin, at + 8)
  return glb
}

/**
 * Share identical corners between triangles.
 *
 * `Part` emits three fresh vertices per triangle, which is what a flat face
 * needs and wasteful everywhere else: across a smooth-shaded figure, and
 * along every flat face made of two or more triangles, the same position,
 * normal and UV is written again and again. Merging exact duplicates (to a
 * tenth of a millimetre) changes nothing on screen and roughly halves a
 * model, more for a smooth one.
 */
function weld(part: Part, withUv: boolean) {
  const count = part.pos.length / 3
  const key = (i: number) => {
    const q = (v: number, k = 1e4) => Math.round(v * k)
    const p = part.pos, n = part.nrm, t = part.uv
    return `${q(p[i * 3])},${q(p[i * 3 + 1])},${q(p[i * 3 + 2])},${q(n[i * 3], 1e3)},${q(n[i * 3 + 1], 1e3)},${q(n[i * 3 + 2], 1e3)}` +
      (withUv ? `,${q(t[i * 2])},${q(t[i * 2 + 1])}` : '')
  }
  const seen = new Map<string, number>()
  const order: number[] = []
  const remap = new Uint32Array(count)
  for (let i = 0; i < count; i++) {
    const k = key(i)
    let at = seen.get(k)
    if (at === undefined) {
      at = order.length
      seen.set(k, at)
      order.push(i)
    }
    remap[i] = at
  }
  const pos = new Float32Array(order.length * 3)
  const nrm = new Float32Array(order.length * 3)
  const uv = new Float32Array(order.length * 2)
  order.forEach((src, i) => {
    pos.set(part.pos.slice(src * 3, src * 3 + 3), i * 3)
    nrm.set(part.nrm.slice(src * 3, src * 3 + 3), i * 3)
    uv.set(part.uv.slice(src * 2, src * 2 + 2), i * 2)
  })
  const index = order.length > 65535 ? new Uint32Array(remap) : Uint16Array.from(remap)
  return { pos, nrm, uv, index }
}

const align4 = (n: number) => n + ((4 - (n % 4)) % 4)

function padTo4(bytes: Uint8Array, fill: number) {
  const out = new Uint8Array(align4(bytes.length)).fill(fill)
  out.set(bytes)
  return out
}

function concat(parts: Uint8Array[], length = parts.reduce((n, p) => n + p.length, 0)) {
  const out = new Uint8Array(length)
  let at = 0
  for (const p of parts) {
    out.set(p, at)
    at += p.length
  }
  return out
}

/** One primitive of a GLB as read back: positions, triangle indices, material. */
export type ReadPrimitive = {
  position: Float32Array
  index: Uint32Array
  material: { name: string; color: [number, number, number, number] }
}

/**
 * Read a static GLB back into flat arrays, applying node transforms, for
 * normalising a model from elsewhere into the frame contract. Float positions
 * and plain indices only — the same subset a stylised landmark needs.
 */
export function readGlb(glb: Uint8Array): ReadPrimitive[] {
  const view = new DataView(glb.buffer, glb.byteOffset, glb.byteLength)
  if (view.getUint32(0, true) !== 0x46546c67) throw new Error('not a GLB')
  let json: any = null
  let bin: Uint8Array | null = null
  for (let at = 12; at + 8 <= glb.length;) {
    const length = view.getUint32(at, true)
    const type = view.getUint32(at + 4, true)
    const body = glb.subarray(at + 8, at + 8 + length)
    if (type === 0x4e4f534a) json = JSON.parse(new TextDecoder().decode(body))
    else if (type === 0x004e4942) bin = body
    at += 8 + length + ((4 - (length % 4)) % 4)
  }
  if (!json || !bin) throw new Error('GLB is missing its JSON or BIN chunk')

  const read = (index: number) => {
    const a = json.accessors[index]
    const v = json.bufferViews[a.bufferView]
    const per = { SCALAR: 1, VEC2: 2, VEC3: 3, VEC4: 4 }[a.type as string]!
    const Type = ({ 5121: Uint8Array, 5123: Uint16Array, 5125: Uint32Array, 5126: Float32Array } as any)[a.componentType]
    if (!Type || (v.byteStride && v.byteStride !== Type.BYTES_PER_ELEMENT * per))
      throw new Error('unsupported accessor layout')
    const bytes = bin!.slice((v.byteOffset ?? 0) + (a.byteOffset ?? 0))
    return new Type(bytes.buffer, 0, a.count * per)
  }

  const out: ReadPrimitive[] = []
  const visit = (i: number, parent: number[]) => {
    const node = json.nodes[i]
    const m = multiply(parent, nodeMatrix(node))
    for (const p of node.mesh !== undefined ? json.meshes[node.mesh].primitives : []) {
      const raw = read(p.attributes.POSITION) as Float32Array
      const position = new Float32Array(raw.length)
      for (let k = 0; k < raw.length; k += 3) {
        const [x, y, z] = [raw[k], raw[k + 1], raw[k + 2]]
        position[k] = m[0] * x + m[4] * y + m[8] * z + m[12]
        position[k + 1] = m[1] * x + m[5] * y + m[9] * z + m[13]
        position[k + 2] = m[2] * x + m[6] * y + m[10] * z + m[14]
      }
      const index = p.indices !== undefined
        ? Uint32Array.from(read(p.indices))
        : Uint32Array.from({ length: raw.length / 3 }, (_, k) => k)
      const mat = json.materials?.[p.material]
      out.push({
        position,
        index,
        material: { name: mat?.name ?? '', color: mat?.pbrMetallicRoughness?.baseColorFactor ?? [1, 1, 1, 1] },
      })
    }
    for (const child of node.children ?? []) visit(child, m)
  }
  const scene = json.scenes?.[json.scene ?? 0]
  for (const root of scene?.nodes ?? []) visit(root, identity())
  return out
}

const identity = () => [1, 0, 0, 0, 0, 1, 0, 0, 0, 0, 1, 0, 0, 0, 0, 1]

function multiply(a: number[], b: number[]) {
  const out = new Array(16).fill(0)
  for (let c = 0; c < 4; c++)
    for (let r = 0; r < 4; r++)
      for (let k = 0; k < 4; k++) out[c * 4 + r] += a[k * 4 + r] * b[c * 4 + k]
  return out
}

function nodeMatrix(node: any): number[] {
  if (node.matrix) return node.matrix.slice()
  const [x, y, z, w] = node.rotation ?? [0, 0, 0, 1]
  const [sx, sy, sz] = node.scale ?? [1, 1, 1]
  const [tx, ty, tz] = node.translation ?? [0, 0, 0]
  return [
    (1 - 2 * (y * y + z * z)) * sx, 2 * (x * y + w * z) * sx, 2 * (x * z - w * y) * sx, 0,
    2 * (x * y - w * z) * sy, (1 - 2 * (x * x + z * z)) * sy, 2 * (y * z + w * x) * sy, 0,
    2 * (x * z + w * y) * sz, 2 * (y * z - w * x) * sz, (1 - 2 * (x * x + y * y)) * sz, 0,
    tx, ty, tz, 1,
  ]
}

/**
 * Add a primitive's triangles to a part, in glTF space (Y up) already
 * normalised by the caller. `Part` writes map-frame points, so this converts
 * back: glTF (x, y, z) is map (x, -z, y).
 */
export function addGltfTriangles(
  part: Part,
  position: Float32Array,
  index: Uint32Array,
  options: { creaseDegrees?: number } = {},
) {
  const at = (i: number): V3 => [position[i * 3], -position[i * 3 + 2], position[i * 3 + 1]]
  const faces: Array<{ v: V3[]; n: V3; area: number }> = []
  for (let t = 0; t < index.length; t += 3) {
    const v = [at(index[t]), at(index[t + 1]), at(index[t + 2])]
    const c = cross(sub(v[1], v[0]), sub(v[2], v[0]))
    const area = len(c)
    if (area > 0) faces.push({ v, n: normalize(c), area })
  }
  if (options.creaseDegrees === undefined) {
    for (const f of faces) part.tri(f.v[0], f.v[1], f.v[2])
    return
  }

  // Smooth shading with a crease: a corner averages the faces that meet at
  // its position, weighted by area, but only those within the crease angle
  // of its own face — so a robe reads as cloth and a pedestal keeps its
  // edges. Positions are matched, not indices: imported meshes are often
  // split at every material and UV seam.
  const key = (p: V3) => p.map((x) => Math.round(x * 1e4)).join(',')
  const around = new Map<string, number[]>()
  faces.forEach((f, i) => f.v.forEach((p) => {
    const k = key(p)
    const list = around.get(k)
    if (list) list.push(i)
    else around.set(k, [i])
  }))
  const cos = Math.cos((options.creaseDegrees * Math.PI) / 180)
  // Winding in an imported mesh is not to be trusted, so a neighbour facing
  // the opposite way is folded onto this face's side before comparing.
  const dot = (a: V3, b: V3) => a[0] * b[0] + a[1] * b[1] + a[2] * b[2]
  for (const f of faces) {
    const normals = f.v.map((p) => {
      const sum: V3 = [0, 0, 0]
      for (const j of around.get(key(p))!) {
        const g = faces[j]
        const d = dot(f.n, g.n)
        const sign = d < 0 ? -1 : 1
        if (Math.abs(d) < cos) continue
        for (let k = 0; k < 3; k++) sum[k] += g.n[k] * g.area * sign
      }
      return normalize(sum)
    })
    part.tri(f.v[0], f.v[1], f.v[2], undefined, undefined, undefined, normals)
  }
}
