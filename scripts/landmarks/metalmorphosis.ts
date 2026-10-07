/**
 * Metalmorphosis, David Černý's mirrored head at Whitehall Corporate Center,
 * Charlotte — procedural, CC0-1.0.
 *
 *   bun scripts/landmarks/metalmorphosis.ts [out.glb]
 *
 * Map frame: x east, y north, z up, metres. The origin is the head's vertical
 * axis on the ground (OSM node 4880354210), and the face looks along +y; the
 * catalog's bearing turns that towards Whitehall Corporate Center IV's plaza.
 *
 * The real sculpture (2007) is about 7 m of polished stainless steel in a
 * reflecting pool: a head on a thick neck, cut into 40 horizontal slabs that
 * the motors spin apart and bring back into a face. This is the face pose,
 * modelled static. Each slab is its own glTF node (`slab-01` at the bottom to
 * `slab-40` at the crown), authored about the shared vertical axis through the
 * origin with no node transform, so a later animation only has to add a
 * rotation per node. The stepped plinth the head stands on is the root mesh;
 * the pool is the map's.
 *
 * Each slab is a flat prism whose outline is the head's horizontal section at
 * that height — skull, cheeks, the nose, lips and brow standing proud, the eye
 * sockets set back — so the face reads in the stepped "pixel" way the real one
 * does. Proportions come from the photos credited in the report: chin at about
 * 3.1 m, the neck nearly as deep as the face, the head 4.5 m chin to crown.
 */
import { Part, addGltfTriangles, writeGlb, type NodeSpec, type V3 } from './mesh'
import { finish } from './palette'

const STEEL = finish('stainless', 0xcdd5dc, 0.3)
const STEP = finish('stainless-2', 0xb3bdc6, 0.3)
// Slab undersides mirror the pool and the warm paving, so they read darker and
// warmer than the sky-lit sides; that is what draws the brow, nose and lips.
const UNDER = finish('stainless-under', 0xa49c94, 0.3)

const PLINTH_TOP = 0.7
const TOP = 7.6
const SLABS = 40
const PITCH = (TOP - PLINTH_TOP) / SLABS
const N = 16 // outline points around a slab; 0° is +x (the right ear), 90° the face

type XY = [number, number]

/** Linear interpolation through a table of [z, value] rows. */
function table(rows: number[][], z: number): number {
  if (z <= rows[0][0]) return rows[0][1]
  for (let i = 1; i < rows.length; i++) {
    if (z <= rows[i][0]) {
      const [z0, a] = rows[i - 1], [z1, b] = rows[i]
      return a + (b - a) * (z - z0) / (z1 - z0)
    }
  }
  return rows[rows.length - 1][1]
}

// The head's sections, as [z, value]. yFront is the face (cheek) plane and
// yBack the back of the skull, both from the axis; rx is the half-width.
const yFront = [[0.6, 1.6], [2.9, 1.62], [3.0, 2.0], [3.35, 2.02], [3.45, 1.66], [3.6, 1.66], [3.7, 2.02],
  [3.9, 2.02], [4.0, 1.7], [4.1, 2.06], [4.3, 2.06], [4.4, 1.62], [5.3, 1.62], [5.6, 2.0], [5.85, 2.0], [6.0, 1.72],
  [6.4, 1.56], [6.8, 1.3]]
const yBack = [[0.6, 1.62], [2.9, 1.52], [3.2, 1.85], [3.8, 2.1], [4.6, 2.25], [5.6, 2.3]]
const rx = [[0.6, 1.62], [2.9, 1.5], [3.2, 1.74], [3.8, 1.9], [4.4, 1.99], [5.0, 2.05], [5.6, 2.06]]

/** The crown: above the eyes every section shrinks like an ellipsoid's. */
const CROWN = 5.6
function dome(z: number) {
  const c = CROWN, h = TOP - c + 0.08
  return z <= c ? 1 : Math.sqrt(Math.max(0, 1 - ((z - c) / h) ** 2))
}

/** Deterministic jitter, so the neck slabs sit a little unevenly, as they do. */
const jitter = (i: number, k: number) => Math.sin(i * 12.9898 + k * 78.233) * 0.5

/** A slab's outline at height z, counter-clockwise from above. */
function outline(z: number, i: number): XY[] {
  const s = dome(z)
  const w = table(rx, z) * s
  // The brow stands out from the dome; above it the face falls back with it.
  // The face plane, and what the brow, lips and chin add to it on the front
  // only, so they read as features rather than as bands round the head.
  const plane = Math.min(table(yFront, z), 1.62)
  const f = z <= 5.85 ? plane : Math.min(plane, 2.1 * s)
  const relief = Math.max(0, table(yFront, z) - 1.62)
  const b = table(yBack, z) * s
  const neck = z < 3.0
  const pts: XY[] = []
  for (let k = 0; k < N; k++) {
    const a = (k / N) * Math.PI * 2
    const c = Math.cos(a), sn = Math.sin(a)
    // A slightly squared ellipse: a head is fuller at the cheeks than an oval.
    const e = 2.4
    const ex = Math.sign(c) * Math.abs(c) ** (2 / e), ey = Math.sign(sn) * Math.abs(sn) ** (2 / e)
    let x = w * ex, y = (sn > 0 ? f : b) * ey + (sn > 0 ? relief * sn ** 4 : 0)
    // Eye sockets: the face either side of the nose drops back.
    if (z > 5.0 && z < 5.55 && (k === 3 || k === 5)) y -= 0.45
    if (z > 5.0 && z < 5.55 && (k === 2 || k === 6)) y -= 0.15
    pts.push([x, y])
  }
  // The nose: a narrow block on the face, deepest at its tip.
  const nose = z > 4.35 && z < 5.55 ? table([[4.35, 2.6], [4.6, 2.5], [5.1, 2.12], [5.55, 1.95]], z) : 0
  if (nose) {
    const half = z < 4.7 ? 0.42 : 0.3
    const at = N / 4 // the +y point
    const yf = pts[at][1]
    pts.splice(at, 1, [half + 0.04, yf], [half, nose], [-half, nose], [-half - 0.04, yf])
  }
  if (neck) {
    const dx = jitter(i, 1) * 0.07, dy = jitter(i, 2) * 0.07, t = jitter(i, 3) * 0.05
    return pts.map(([x, y]) => [x * Math.cos(t) - y * Math.sin(t) + dx, x * Math.sin(t) + y * Math.cos(t) + dy])
  }
  return pts
}

/**
 * A flat prism over a star-shaped outline, smooth round its sides. Faces
 * pointing down go to `under` when given.
 */
function prism(target: Part, pts: XY[], z0: number, z1: number, under = target) {
  const p = new Part()
  const lo: V3[] = pts.map(([x, y]) => [x, y, z0])
  const hi: V3[] = pts.map(([x, y]) => [x, y, z1])
  p.loft([lo, hi])
  const cx = pts.reduce((s, q) => s + q[0], 0) / pts.length
  const cy = pts.reduce((s, q) => s + q[1], 0) / pts.length
  for (let k = 0; k < pts.length; k++) {
    const j = (k + 1) % pts.length
    p.tri([cx, cy, z1], hi[k], hi[j])
    p.tri([cx, cy, z0], lo[j], lo[k])
  }
  const smooth = new Part()
  addGltfTriangles(smooth, new Float32Array(p.pos),
    Uint32Array.from({ length: p.pos.length / 3 }, (_, i) => i), { creaseDegrees: 50 })
  // Part stores glTF space (y up), so a downward face has normal y near -1.
  for (let t = 0; t < smooth.triangles; t++) {
    const dest = smooth.nrm[t * 9 + 1] < -0.9 ? under : target
    dest.pos.push(...smooth.pos.slice(t * 9, t * 9 + 9))
    dest.nrm.push(...smooth.nrm.slice(t * 9, t * 9 + 9))
    dest.uv.push(...smooth.uv.slice(t * 6, t * 6 + 6))
  }
}

// The stepped plinth: four slabs flaring out under the neck, reaching
// further towards the face than behind, as in the photos.
const plinth = new Part()
const steps = [[2.75, 2.3, 2.45], [2.45, 2.05, 2.2], [2.15, 1.85, 1.95], [1.85, 1.7, 1.75]] // front, back, half-width
steps.forEach(([f, b, w], k) => {
  const z0 = (k * PLINTH_TOP) / steps.length, z1 = ((k + 1) * PLINTH_TOP) / steps.length
  const pts: XY[] = Array.from({ length: N }, (_, i) => {
    const a = (i / N) * Math.PI * 2
    const c = Math.cos(a), s = Math.sin(a)
    return [w * Math.sign(c) * Math.abs(c) ** 0.8, (s > 0 ? f : b) * Math.sign(s) * Math.abs(s) ** 0.8]
  })
  prism(plinth, pts, k === 0 ? 0 : z0 - 0.01, z1)
})

const nodes: NodeSpec[] = []
for (let i = 0; i < SLABS; i++) {
  const z0 = PLINTH_TOP + i * PITCH, z1 = z0 + PITCH
  const part = new Part(), under = new Part()
  prism(part, outline((z0 + z1) / 2, i), z0, z1, under)
  nodes.push({
    name: `slab-${String(i + 1).padStart(2, '0')}`,
    parts: [{ part, material: STEEL }, { part: under, material: UNDER }],
  })
}

const parts = [{ part: plinth, material: STEP }]
const tris = [plinth, ...nodes.flatMap((n) => n.parts.map((q) => q.part))].reduce((s, p) => s + p.triangles, 0)
if (tris > 3000 || 0) throw new Error(`over budget: ${tris} triangles`)
const glb = writeGlb('Metalmorphosis', parts, {
  title: 'Metalmorphosis',
  artist: 'David Černý',
  license: 'CC0-1.0',
  source: 'scripts/landmarks/metalmorphosis.ts',
  note: 'Static face pose; each of the 40 slabs is its own node about the vertical axis through the origin.',
}, { nodes })
const out = process.argv[2] ?? new URL('../../landmarks/models/metalmorphosis.glb', import.meta.url).pathname
await Bun.write(out, glb)
console.log(`${out}: ${tris} triangles, ${glb.length} bytes, ${nodes.length} slab nodes`)
