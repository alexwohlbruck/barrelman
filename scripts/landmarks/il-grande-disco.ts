/**
 * Il Grande Disco, Arnaldo Pomodoro's bronze disc at Trade and Tryon,
 * Charlotte — procedural, CC0-1.0.
 *
 *   bun scripts/landmarks/il-grande-disco.ts [out.glb]
 *
 * Map frame: x east, y north, z up, metres. The origin is the disc's centre on
 * the ground (OSM node 8415199023), the disc stands in the x–z plane and its
 * two faces look along ±y; the catalog's bearing turns them towards the
 * crossing.
 *
 * The sculpture (1974) is a cast bronze disc 15 ft (4.57 m) across, about
 * 0.85 m thick, standing on its edge on the plaza paving. Both faces are
 * polished gold bronze broken open by a dark, star-shaped fissure — a central
 * mass with arms of uneven length, several of them reaching and breaking the
 * rim — and the rim itself is the same dark, rough bronze. Here the faces are
 * flat discs with a bevelled edge, the fissure a flush dark star on each face
 * with the gold block at its heart, and the rim a dark band.
 */
import { Part, addGltfTriangles, writeGlb, type V3 } from './mesh'
import { finish } from './palette'

const GOLD = finish('gilded-bronze', 0xd6b27a, 0.45)
const DARK = finish('bronze-dark', 0x76695f)

const R = 4.57 / 2
const T = 0.85 // thickness
const BEVEL = 0.07
const SINK = 0.1 // the disc sits a little into the paving, flattening its foot
const C = R - SINK // centre height
const SEG = 40

const gold = new Part()
const dark = new Part()

/** Round the disc's edge: points on a circle of radius r in the x–z plane at depth y. */
const ring = (r: number, y: number): V3[] => Array.from({ length: SEG }, (_, i) => {
  const a = (i / SEG) * Math.PI * 2
  // Counter-clockwise seen from +y: x → -x as the angle grows, so the loft faces out.
  return [-r * Math.cos(a), y, Math.max(0, C + r * Math.sin(a))]
})

function smooth(target: Part, build: (p: Part) => void, crease = 40) {
  const p = new Part()
  build(p)
  addGltfTriangles(target, new Float32Array(p.pos),
    Uint32Array.from({ length: p.pos.length / 3 }, (_, i) => i), { creaseDegrees: crease })
}

// The dark rim band and the bevels that turn it onto each face.
const h = T / 2
smooth(dark, (p) => p.loft([ring(R, -h + BEVEL), ring(R, h - BEVEL)]))
smooth(gold, (p) => {
  p.loft([ring(R, h - BEVEL), ring(R - BEVEL, h)])
  p.loft([ring(R - BEVEL, -h), ring(R, -h + BEVEL)])
})

// The faces: a fan over each.
function face(y: number, front: boolean) {
  const rim = ring(R - BEVEL, y)
  const c: V3 = [0, y, C]
  for (let i = 0; i < SEG; i++) {
    const j = (i + 1) % SEG
    // ring() runs clockwise seen from +y, so the +y face takes it reversed.
    if (front) gold.tri(c, rim[j], rim[i])
    else gold.tri(c, rim[i], rim[j])
  }
}
face(h, true)
face(-h, false)

/**
 * The fissure: arms as [angle°, length as a fraction of R, half-width at the
 * root in metres], read off the photos of the north face. Lengths of 1 run
 * through the bevel and break the rim.
 */
const ARMS: number[][] = [
  [-10, 0.74, 0.2], [27, 1.0, 0.24], [82, 1.0, 0.24], [122, 0.6, 0.12],
  [165, 1.0, 0.26], [224, 1.0, 0.3], [268, 0.76, 0.17], [314, 0.9, 0.22],
]
const CORE = 0.42 * R

/** The star's outline in the face plane, as (u, v): u across, v up. */
function star(): Array<[number, number]> {
  const pts: Array<[number, number]> = []
  const at = (deg: number, r: number): [number, number] => [r * Math.cos((deg * Math.PI) / 180), r * Math.sin((deg * Math.PI) / 180)]
  ARMS.forEach(([a, l, w], k) => {
    const next = ARMS[(k + 1) % ARMS.length][0] + (k === ARMS.length - 1 ? 360 : 0)
    // A torn channel narrowing from the core to a square end, not a pointed ray.
    const d = [Math.cos((a * Math.PI) / 180), Math.sin((a * Math.PI) / 180)], n = [-d[1], d[0]]
    const reach = Math.min(l * R, Math.sqrt((R - BEVEL * 0.5) ** 2 - (w * 0.75) ** 2))
    const p = (r: number, s: number): [number, number] => [d[0] * r + n[0] * s, d[1] * r + n[1] * s]
    const wr = w * 1.6 // wider where the arm leaves the core
    const root = Math.sqrt(CORE ** 2 - wr ** 2)
    pts.push(p(root, -wr), p(reach, -w * 0.75), p(reach, w * 0.75), p(root, wr))
    // A notch of gold between arms, so the core reads ragged rather than round.
    pts.push(at((a + next) / 2, CORE * 0.86))
  })
  return pts
}

const LIFT = 0.015 // flush, just clear of the face
function fissure(y: number, front: boolean) {
  const pts = star()
  const s = front ? 1 : -1
  // The back face mirrors the front, as seen from its own side.
  const P = ([u, v]: [number, number]): V3 => [front ? -u : u, y + s * LIFT, C + v]
  for (let i = 0; i < pts.length; i++) {
    const j = (i + 1) % pts.length
    const a = P(pts[i]), b = P(pts[j]), c: V3 = [0, y + s * LIFT, C]
    if (front) dark.tri(c, b, a)
    else dark.tri(c, a, b)
  }
  // The gold block at the heart of the star.
  const k = 0.24, d = 0.12
  const y0 = y + s * LIFT, y1 = y + s * (LIFT + d)
  const sq = (yy: number): V3[] => [[-k, yy, C - k], [k, yy, C - k], [k, yy, C + k], [-k, yy, C + k]]
  const lo = sq(y0), hi = sq(y1)
  for (let i = 0; i < 4; i++) {
    const j = (i + 1) % 4
    if (front) gold.quad(lo[i], lo[j], hi[j], hi[i])
    else gold.quad(lo[j], lo[i], hi[i], hi[j])
  }
  if (front) gold.quad(hi[0], hi[1], hi[2], hi[3])
  else gold.quad(hi[3], hi[2], hi[1], hi[0])
}
fissure(h, true)
fissure(-h, false)

/**
 * Turn every triangle to face out, rather than tracking the winding of each
 * piece by hand. On a face, "out" is along ±y, except for the central block's
 * sides, which face away from its axis; on the rim and bevels it is away from
 * the disc's centre.
 */
function orient(p: Part) {
  const out = new Part()
  const at = (t: number, k: number, src: number[]): V3 => {
    const i = t * 9 + k * 3
    return [src[i], -src[i + 2], src[i + 1]] // glTF → map frame
  }
  for (let t = 0; t < p.triangles; t++) {
    const [a, b, c] = [0, 1, 2].map((k) => at(t, k, p.pos))
    const ns = [0, 1, 2].map((k) => at(t, k, p.nrm))
    const m: V3 = [0, 1, 2].map((i) => (a[i] + b[i] + c[i]) / 3) as V3
    const e1 = [b[0] - a[0], b[1] - a[1], b[2] - a[2]], e2 = [c[0] - a[0], c[1] - a[1], c[2] - a[2]]
    const n = [e1[1] * e2[2] - e1[2] * e2[1], e1[2] * e2[0] - e1[0] * e2[2], e1[0] * e2[1] - e1[1] * e2[0]]
    const facing = Math.abs(n[1]) > 0.9 * Math.hypot(n[0], n[1], n[2])
    const dir = Math.abs(m[1]) > h - 0.005
      ? (facing ? [0, Math.sign(m[1]), 0] : [m[0], 0, m[2] - C])
      : [m[0], m[1], m[2] - C]
    const flip = n[0] * dir[0] + n[1] * dir[1] + n[2] * dir[2] < 0
    // Re-emit in map frame; tri() converts back to glTF.
    const fix = (v: V3) => (flip ? v.map((x) => -x) : v) as V3
    if (flip) out.tri(a, c, b, undefined, undefined, undefined, [fix(ns[0]), fix(ns[2]), fix(ns[1])])
    else out.tri(a, b, c, undefined, undefined, undefined, ns)
  }
  return out
}

const parts = [{ part: orient(gold), material: GOLD }, { part: orient(dark), material: DARK }]
const tris = parts.reduce((n, { part }) => n + part.triangles, 0)
if (tris > 3000) throw new Error(`over budget: ${tris} triangles`)
const glb = writeGlb('Il Grande Disco', parts, {
  title: 'Il Grande Disco',
  artist: 'Arnaldo Pomodoro',
  license: 'CC0-1.0',
  source: 'scripts/landmarks/il-grande-disco.ts',
})
const out = process.argv[2] ?? new URL('../../landmarks/models/il-grande-disco.glb', import.meta.url).pathname
await Bun.write(out, glb)
console.log(`${out}: ${tris} triangles, ${glb.length} bytes`)
