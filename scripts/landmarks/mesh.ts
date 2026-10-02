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
 * Faces are flat-shaded (unshared vertices). At landmark sizes that is both the
 * look we want and nearly free: these models are a few thousand triangles.
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

  tri(a: V3, b: V3, c: V3, ua: number[] = [0, 0], ub: number[] = [0, 0], uc: number[] = [0, 0]) {
    const n = normalize(cross(sub(b, a), sub(c, a)))
    for (const [p, t] of [[a, ua], [b, ub], [c, uc]] as [V3, number[]][]) {
      // map frame → glTF frame: (x, y, z) → (x, z, -y). A rotation, so the
      // counter-clockwise winding survives the trip.
      this.pos.push(p[0], p[2], -p[1])
      this.nrm.push(n[0], n[2], -n[1])
      this.uv.push(t[0], t[1])
    }
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
}

/**
 * Write parts as a single-mesh GLB, one primitive per material.
 *
 * Indexed, even though no vertex is shared: some readers (Parchment's own
 * among them) only take indexed triangles, and the index buffer is cheap.
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
    if (material.mask) {
      images.push({ bufferView: push(material.mask.png), mimeType: 'image/png' })
      textures.push({ source: images.length - 1 })
      m.pbrMetallicRoughness.baseColorTexture = { index: textures.length - 1 }
      m.alphaMode = 'MASK'
      m.alphaCutoff = material.mask.cutoff ?? 0.5
    }
    materials.push(m)

    const count = part.pos.length / 3
    const index = count > 65535 ? new Uint32Array(count) : new Uint16Array(count)
    for (let i = 0; i < count; i++) index[i] = i
    primitives.push({
      attributes: {
        POSITION: accessor(new Float32Array(part.pos), 'VEC3', 34962, true),
        NORMAL: accessor(new Float32Array(part.nrm), 'VEC3', 34962),
        ...(material.mask ? { TEXCOORD_0: accessor(new Float32Array(part.uv), 'VEC2', 34962) } : {}),
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
