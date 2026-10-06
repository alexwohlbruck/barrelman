/**
 * Lady Liberty, authored in metres: east +x, north +y, up +z.
 *
 *   bun scripts/landmarks/statue-of-liberty.ts [out.glb]
 *
 * The pedestal base is 40 m square, the heel is at 36.9 m and the flame tip
 * at 83 m. The catalog raises the pedestal onto Fort Wood; the fort remains
 * map geometry, not part of this asset. She faces south (−y), with her right
 * (west, −x) arm holding the torch. Styled after Apple Maps' landmarks
 * (landmarks/STYLE.md): bevelled granite blocks, the loggia as recessed bands,
 * and a smooth verdigris figure whose head, crown and arm read at 200 px.
 * All geometry is procedural and CC0-1.0; no source mesh or textures.
 */
import { Part, addGltfTriangles, cross, sub, len, writeGlb, type V3 } from './mesh'

const granite = new Part()
const trim = new Part()
const recess = new Part()
const copper = new Part()
const folds = new Part()
const flame = new Part()
const TAU = Math.PI * 2
const unit = (v: V3): V3 => v.map(n => n / (len(v) || 1)) as V3
const plus = (a: V3, b: V3): V3 => a.map((n, i) => n + b[i]) as V3
const scale = (a: V3, s: number): V3 => a.map(n => n * s) as V3
const clamp = (x: number, a = 0, b = 1) => Math.min(b, Math.max(a, x))
const smoothstep = (a: number, b: number, x: number) => { const t = clamp((x - a) / (b - a)); return t * t * (3 - 2 * t) }

// ---------------------------------------------------------------- pedestal

type XY = [number, number]

/** A rectangle's outline with its corners cut by `b`, and per-corner normals. */
function rim(cx: number, cy: number, hx: number, hy: number, b: number): { pts: XY[]; nrm: XY[] } {
  const pts: XY[] = [
    [cx - hx + b, cy - hy], [cx + hx - b, cy - hy], [cx + hx, cy - hy + b], [cx + hx, cy + hy - b],
    [cx + hx - b, cy + hy], [cx - hx + b, cy + hy], [cx - hx, cy + hy - b], [cx - hx, cy - hy + b],
  ]
  const nrm: XY[] = [[0, -1], [0, -1], [1, 0], [1, 0], [0, 1], [0, 1], [-1, 0], [-1, 0]]
  return { pts, nrm }
}

/**
 * A bevelled stone block. Flat faces keep their own normal; the corner and
 * top-edge chamfers interpolate between neighbours, so every edge reads as a
 * soft rounded highlight rather than a razor-sharp extrusion. `top` is the
 * half-size at z1, for battered (narrowing) or overhanging (widening) blocks.
 */
function block(p: Part, z0: number, z1: number, hx: number, hy = hx,
  { cx = 0, cy = 0, b = 0.45, top = [hx, hy] as XY, cap = true, bevelTop = true } = {}) {
  const tb = bevelTop ? Math.min(b, (z1 - z0) * 0.4) : 0
  const lo = rim(cx, cy, hx, hy, b)
  const hi = rim(cx, cy, top[0], top[1], b)
  const lid = rim(cx, cy, top[0] - tb, top[1] - tb, Math.max(0.05, b - tb * 0.4))
  const tilt = (hx - top[0]) / (z1 - z0)
  const side = (n: XY): V3 => unit([n[0], n[1], tilt])
  const ring = (r: XY[], z: number): V3[] => r.map(([x, y]) => [x, y, z])
  const band = (a: V3[], na: V3[], c: V3[], nc: V3[]) => {
    for (let i = 0; i < 8; i++) {
      const j = (i + 1) % 8
      p.tri(a[i], a[j], c[j], undefined, undefined, undefined, [na[i], na[j], nc[j]])
      p.tri(a[i], c[j], c[i], undefined, undefined, undefined, [na[i], nc[j], nc[i]])
    }
  }
  const sideN = lo.nrm.map(side)
  const A = ring(lo.pts, z0), B = ring(hi.pts, z1 - tb)
  band(A, sideN, B, sideN)
  if (tb > 0) band(B, sideN, ring(lid.pts, z1), lid.pts.map(() => [0, 0, 1] as V3))
  if (cap) {
    const C = ring((tb > 0 ? lid : hi).pts, z1)
    for (let i = 1; i < 7; i++) p.tri(C[0], C[i], C[i + 1])
  }
}

// Fort Wood's terraces, then the granite pedestal, bottom to top.
block(granite, 0, 4, 20, 20, { b: 0.6 })
block(granite, 4, 8, 12.75, 12.75, { b: 0.6 })
block(trim, 8, 9.6, 9.7, 9.7, { b: 0.5 })
// Battered lower body, crossed by the projecting course that carries the
// frieze of discs (too fine to model; the band itself is what reads).
block(granite, 9.6, 14.2, 9.2, 9.2, { top: [8.55, 8.55], bevelTop: false })
block(trim, 12.2, 13.5, 9.0, 9.0, { b: 0.35 })
block(trim, 14.2, 15, 8.75, 8.75, { b: 0.4 })
// Shaft: plain panels between raised rusticated corner piers.
block(granite, 15, 24.2, 6.95, 6.95, { cap: false, bevelTop: false })
for (const sx of [-1, 1]) for (const sy of [-1, 1])
  block(trim, 15, 28.7, 1.3, 1.3, { cx: sx * 5.95, cy: sy * 5.95, b: 0.4, cap: false, bevelTop: false })
block(trim, 23.4, 24.3, 7.4, 7.4, { b: 0.35 })
// Loggia: three deep openings per face, between four pilasters. The recess
// is one dark band per face, set 0.8 m behind the pilaster faces.
block(recess, 24.3, 28.7, 6.4, 6.4, { cap: false, bevelTop: false, b: 0.3 })
for (let face = 0; face < 4; face++) {
  for (const along of [-4.35, -1.5, 1.5, 4.35]) {
    const ns = face % 2 ? (face === 1 ? 1 : -1) : 0, ew = face % 2 ? 0 : (face === 0 ? -1 : 1)
    // Faces: 0 south, 1 east, 2 north, 3 west.
    const cx = ns ? ns * 6.8 : along, cy = ns ? along : ew * 6.8
    block(trim, 24.3, 28.7, ns ? 0.4 : 0.45, ns ? 0.45 : 0.4, { cx, cy, b: 0.18, cap: false, bevelTop: false })
  }
}
block(trim, 28.7, 29.7, 7.45, 7.45, { b: 0.35 })
// Overhanging cornice, then the balcony with its parapet.
block(trim, 29.7, 30.7, 7.6, 7.6, { top: [8.3, 8.3], b: 0.45 })
block(granite, 30.7, 32, 8.1, 8.1, { b: 0.3, bevelTop: false })
for (const s of [-1, 1]) {
  block(trim, 32, 33.1, 8.1, 0.38, { cy: s * 7.72, b: 0.25 })
  block(trim, 32, 33.1, 0.38, 7.4, { cx: s * 7.72, b: 0.25 })
}
// The stone top: a chamfered attic block, a capping moulding, and the
// verdigris plinth plate the figure stands on.
block(granite, 32, 35.6, 6.5, 6.5, { b: 1.5, bevelTop: false })
block(trim, 35.6, 36.4, 6.85, 6.85, { b: 1.6, top: [7.05, 7.05] })
block(copper, 36.4, 36.9, 5.35, 5.35, { b: 1.1 })

// ------------------------------------------------------------------ figure

/** Smooth each sculpted piece separately, keeping edges at joins and caps. */
function smooth(target: Part, build: (p: Part) => void, crease = 65) {
  const p = new Part()
  build(p)
  addGltfTriangles(target, new Float32Array(p.pos),
    Uint32Array.from({ length: p.pos.length / 3 }, (_, i) => i), { creaseDegrees: crease })
}

function solid(p: Part, rings: V3[][]) {
  p.loft(rings)
  p.cap(rings[0], false)
  p.cap(rings[rings.length - 1], true)
}

function oval(c: V3, rx: number, ry: number, n = 16): V3[] {
  return Array.from({ length: n }, (_, i) => {
    const a = i / n * TAU
    return [c[0] + rx * Math.cos(a), c[1] + ry * Math.sin(a), c[2]]
  })
}

function ellipsoid(p: Part, c: V3, radii: V3, n = 14, rows = 7) {
  smooth(p, mesh => {
    const rings = Array.from({ length: rows + 1 }, (_, j) => {
      const a = -Math.PI / 2 + j / rows * Math.PI
      const k = j === 0 || j === rows ? 0 : Math.cos(a)
      return oval([c[0], c[1], c[2] + radii[2] * Math.sin(a)], radii[0] * k, radii[1] * k, n)
    })
    mesh.loft(rings)
  }, 80)
}

/** Rounded, tapered limbs and crown rays; each ring is square to the path. */
function tube(p: Part, path: V3[], widths: number[], depths = widths, sides = 12) {
  smooth(p, mesh => {
    const rings = path.map((c, i) => {
      const axis = unit(sub(path[Math.min(i + 1, path.length - 1)], path[Math.max(0, i - 1)]))
      const u = unit(cross(Math.abs(axis[1]) > 0.9 ? [1, 0, 0] : [0, 1, 0], axis))
      const v = cross(axis, u)
      return Array.from({ length: sides }, (_, j) => {
        const a = j / sides * TAU
        return plus(c, plus(scale(u, widths[i] * Math.cos(a)), scale(v, depths[i] * Math.sin(a))))
      })
    })
    solid(mesh, rings)
  })
}

// The robe is one lofted surface: a broad, nearly columnar body (the real
// figure is barely narrower at the chest than at the knees) with the
// mantle's hem running diagonally from her right knee up to her left hip.
// Rows: z, half-width, half-depth, centre y.
const profile: [number, number, number, number][] = [
  [36.9, 4.7, 3.45, 0.1], [38, 4.85, 3.55, 0.1], [42, 4.75, 3.4, 0.05], [46, 4.6, 3.3, 0],
  [50, 4.45, 3.15, 0], [54, 4.25, 3.0, 0], [58, 4.15, 2.9, 0], [61, 3.95, 2.75, 0.05],
  [62.6, 3.6, 2.55, 0.1], [63.8, 2.95, 2.2, 0.15], [64.7, 1.9, 1.7, 0.15], [65.3, 0.9, 0.9, 0.15],
]
const at = (z: number) => {
  let i = 0
  while (i < profile.length - 2 && profile[i + 1][0] < z) i++
  const [z0, ...a] = profile[i], [z1, ...b] = profile[i + 1]
  const t = clamp((z - z0) / (z1 - z0))
  return a.map((v, k) => v + (b[k] - v) * t)
}
// Hem height by azimuth (a = 0 east, −π/2 south): diagonal across the front,
// low behind, where the cloak falls almost to the feet.
const hem = (a: number) => {
  const front = smoothstep(-0.25, 0.55, -Math.sin(a))
  return 40.5 * (1 - front) + (48.6 + 4.6 * Math.cos(a)) * front
}
// Her left knee pushes the cloth forward.
const knee = (a: number, z: number) => 0.5 * Math.exp(-((a + 1.15) ** 2) / 0.12 - ((z - 45) ** 2) / 18)
const ringAt = (n: number, z: (a: number) => number, d: (a: number, z: number) => number) =>
  Array.from({ length: n }, (_, i): V3 => {
    const a = i / n * TAU, h = z(a), [rx, ry, cy] = at(h), o = d(a, h) + knee(a, h)
    return [(rx + o) * Math.cos(a), cy + (ry + o * 0.8) * Math.sin(a), h]
  })

// The chiton: the long underskirt, its six deep vertical folds tinted dark.
// It runs up under the mantle, which hides its top edge.
const CHITON = 36
const chiton = (a: number) => Math.cos(6 * a + 0.4)
const chitonZ = [36.9, 37.8, 40, 43, 46.5, 50.5, 54.5]
const skirt = new Part()
smooth(skirt, p => p.loft(chitonZ.map(z => ringAt(CHITON, () => z, (a, h) =>
  0.34 * chiton(a) * smoothstep(56, 40, h) + 0.1 * smoothstep(38.5, 36.9, h)))), 72)
{
  const levels = chitonZ.length - 1
  for (let t = 0; t < skirt.triangles; t++) {
    const a = (Math.floor(t / (levels * 2)) + 0.5) / CHITON * TAU
    const target = chiton(a) < -0.5 ? folds : copper
    target.pos.push(...skirt.pos.slice(t * 9, t * 9 + 9))
    target.nrm.push(...skirt.nrm.slice(t * 9, t * 9 + 9))
    target.uv.push(...skirt.uv.slice(t * 6, t * 6 + 6))
  }
}

// The mantle: a thicker layer over the chiton, its hem a clean diagonal with
// a real lip, so the edge shades itself instead of being painted on.
const MANTLE = 32
const drape = (a: number, z: number) => (0.45 + 0.2 * Math.cos(4 * a + 0.32 * z)) * smoothstep(64.4, 60.5, z)
const mantleRings = [
  ringAt(MANTLE, a => hem(a) + 0.45, () => 0.02),
  ringAt(MANTLE, hem, drape),
  ...[0.12, 0.3, 0.5, 0.72, 1].map(t => ringAt(MANTLE, a => hem(a) + (61 - hem(a)) * t, drape)),
  ...[62.6, 63.8, 64.7, 65.3].map(z => ringAt(MANTLE, () => z, drape)),
]
{
  // The underside of the hem lip is the one dark line across the front.
  const mantle = new Part()
  smooth(mantle, p => p.loft(mantleRings), 60)
  const levels = mantleRings.length - 1
  for (let t = 0; t < mantle.triangles; t++) {
    const target = Math.floor(t / 2) % levels === 0 ? folds : copper
    target.pos.push(...mantle.pos.slice(t * 9, t * 9 + 9))
    target.nrm.push(...mantle.nrm.slice(t * 9, t * 9 + 9))
    target.uv.push(...mantle.uv.slice(t * 6, t * 6 + 6))
  }
}
const front = (x: number, z: number, out = 0.15): number => {
  const [rx, ry, cy] = at(z)
  return cy - (ry + 0.35) * Math.sqrt(clamp(1 - (x / (rx + 0.4)) ** 2)) - out
}

// The left foot steps out from under the hem.
ellipsoid(copper, [1.7, -3.3, 37.45], [1.05, 1.55, 0.55], 12, 5)

// The palla's edge sweeps from the left shoulder across the chest to under
// the raised arm: one heavy fold, not fine drapery.
tube(copper, [[2.9, front(2.9, 63.2), 63.2], [0.8, front(0.8, 60.6), 60.6],
  [-1.8, front(-1.8, 58.6), 58.6], [-3.7, front(-3.7, 58.2, -0.4), 58.2]],
[0.35, 0.6, 0.6, 0.35], [0.3, 0.4, 0.4, 0.3], 8)

// Raised right arm: a full sleeve at the shoulder, then a bare, slightly
// forward-leaning forearm. Thicker than life so it reads at phone size.
const shoulder: V3 = [-3.3, 0.2, 63.2], elbow: V3 = [-4.35, -0.55, 69.2], wrist: V3 = [-4.75, -1.25, 74.6]
tube(copper, [[-2.3, 0.2, 61.5], shoulder, [-4.0, -0.15, 66.2], [-4.3, -0.45, 68.2]],
  [1.7, 2.05, 1.75, 1.3], [1.6, 1.85, 1.6, 1.25])
tube(copper, [[-4.25, -0.4, 67.6], elbow, [-4.6, -0.95, 72], wrist],
  [1.2, 1.12, 0.98, 0.88])
ellipsoid(copper, [-4.8, -1.3, 75.3], [0.95, 0.95, 1.05], 12, 6)

// Left arm hangs to the elbow, then the forearm comes forward to hold the
// tablet at the hip.
tube(copper, [[2.6, 0.2, 62.8], [3.75, 0.3, 61.2], [4.4, 0.3, 58.5], [4.5, -0.3, 56.6]],
  [1.3, 1.4, 1.2, 1.0])
tube(copper, [[4.5, -0.3, 56.6], [4.35, -1.6, 55.9], [3.9, -2.6, 56.4]], [0.95, 0.85, 0.75])
// Tablet: 7.2 × 4.1 × 0.6 m. Its face turns south-east and its top leans
// back against the arm and a little outward, as in the photos.
{
  const tablet = new Part()
  const W = 2.05, T = 0.32, H = 3.6
  const corners = [-1, 1].map(s => [[-W, -T], [W, -T], [W, T], [-W, T]].map(([x, y]): V3 => [x, y, s * H]))
  solid(tablet, corners)
  const yaw = -0.42, lean = 0.22, roll = 0.08
  const pts = new Float32Array(tablet.pos)
  for (let i = 0; i < pts.length; i += 3) {
    // glTF → map, rotate, then back.
    let [x, y, z] = [pts[i], -pts[i + 2], pts[i + 1]]
    ;[y, z] = [y * Math.cos(lean) + z * Math.sin(lean), -y * Math.sin(lean) + z * Math.cos(lean)]
    ;[x, z] = [x * Math.cos(roll) + z * Math.sin(roll), -x * Math.sin(roll) + z * Math.cos(roll)]
    ;[x, y] = [x * Math.cos(yaw) - y * Math.sin(yaw), x * Math.sin(yaw) + y * Math.cos(yaw)]
    x += 4.3; y += -1.6; z += 59.2
    pts[i] = x; pts[i + 1] = z; pts[i + 2] = -y
  }
  addGltfTriangles(copper, pts, Uint32Array.from({ length: pts.length / 3 }, (_, i) => i))
}
ellipsoid(copper, [3.85, -2.75, 56.6], [0.8, 0.7, 0.75], 10, 5)

// Neck, the back of the head, and a long calm face: chin at 65.6 m, crown of
// the head at 70.9 m (the real 5.26 m), a touch broad so it reads small.
tube(copper, [[0, 0.25, 64.4], [0, 0.05, 66.4]], [1.05, 0.95], [1.0, 0.9], 12)
ellipsoid(copper, [0, 0.55, 68.4], [2.0, 1.95, 2.45], 14, 7)
smooth(copper, p => solid(p, [
  oval([0, -0.35, 65.6], 0.55, 0.45, 14),
  oval([0, -0.5, 66.2], 1.3, 1.05, 14),
  oval([0, -0.45, 67.6], 1.75, 1.4, 14),
  oval([0, -0.3, 69.1], 1.8, 1.4, 14),
  oval([0, 0, 70.0], 1.5, 1.2, 14),
]), 80)
tube(copper, [[0, -1.55, 68.7], [0, -1.95, 67.4], [0, -1.7, 67.1]], [0.24, 0.32, 0.28], [0.16, 0.26, 0.16], 6)

// Diadem and its seven rays, spread round the front and rising most at the
// centre. Broad roots and generous length keep them legible at 200 px.
smooth(copper, p => solid(p, [oval([0, 0.25, 69.5], 2.1, 1.85, 20),
  oval([0, 0.25, 70.4], 2.2, 1.95, 20), oval([0, 0.25, 70.95], 1.9, 1.65, 20)]), 60)
for (let i = 0; i < 7; i++) {
  // From just behind due east, round the face, to just behind due west.
  const a = (-8 + i * 32.67) * Math.PI / 180
  const dir = unit([Math.cos(a), -Math.sin(a) * 0.9, 0.4 + 0.6 * Math.max(0, Math.sin(a))])
  const root: V3 = [1.95 * Math.cos(a), 0.25 - 1.7 * Math.sin(a), 70.3]
  tube(copper, [root, plus(root, scale(dir, 1.1)), plus(root, scale(dir, 4.3))],
    [0.6, 0.44, 0.03], [0.4, 0.3, 0.03], 6)
}

// Torch: handle, flared cup with its balcony rim, and the gold flame, whose
// tip is exactly z = 83.
const T: XY = [-4.85, -1.45]
smooth(folds, p => solid(p, [
  oval([T[0], T[1], 75.6], 0.55, 0.55, 14),
  oval([T[0], T[1], 77.6], 0.62, 0.62, 14),
  oval([T[0], T[1], 78.5], 1.35, 1.35, 14),
  oval([T[0], T[1], 79.3], 1.6, 1.6, 14),
  oval([T[0], T[1], 79.75], 1.5, 1.5, 14),
]), 50)
smooth(flame, p => solid(p, [
  oval([T[0], T[1], 79.7], 1.05, 1.0, 14),
  oval([T[0], T[1], 80.5], 1.3, 1.25, 14),
  oval([T[0], T[1], 81.4], 1.1, 1.05, 14),
  oval([T[0] + 0.1, T[1], 82.3], 0.6, 0.58, 14),
  oval([T[0], T[1], 83], 0.02, 0.02, 14),
]), 80)

// Colours are sRGB patch medians from the reference photos (/tmp/nyc-refs):
// 01.jpg (1280 × 960) and 02.jpg (1280 × 3171), rectangles [l, t, r, b].
// The writer converts them to linear; they are not adjusted for the map.
const parts = [
  // Granite walls, terraces and attic; photo 02 [638, 2736, 740, 2783].
  { part: granite, material: { name: 'granite', color: 0xada89e } },
  // Lighter dressed stone: piers, pilasters, courses, cornice, parapet;
  // photo 02 [348, 2410, 375, 2489].
  { part: trim, material: { name: 'stone-trim', color: 0xc2b9ae } },
  // Shadowed loggia openings, kept a mid tone so they read as a band, not a
  // hole; photo 02 [565, 2385, 611, 2458] lifted toward the sunlit reveals.
  { part: recess, material: { name: 'loggia-recess', color: 0x55514b } },
  // Sunlit verdigris, photo 01 [608, 265, 665, 387]; the tablet
  // [685, 251, 707, 294] gives a corroborating #50a59c.
  { part: copper, material: { name: 'verdigris', color: 0x5aa89f } },
  // Deep folds and the torch's shaded metal; photo 01 [643, 397, 651, 433].
  { part: folds, material: { name: 'verdigris-deep', color: 0x1f7568 } },
  // Sunlit gold leaf on the flame; photo 02 [364, 137, 368, 140].
  { part: flame, material: { name: 'flame', color: 0xf0cf6a } },
]
const glb = writeGlb('Statue of Liberty', parts, {
  license: 'CC0-1.0',
  frame: 'Y up, -Z north, +X east, metres; origin at pedestal base, placed 10 m above ground',
  facing: 'south (-y map frame)', heel: 36.9, flameTip: 83, crownRays: 7,
})
const triangles = parts.reduce((n, { part }) => n + part.triangles, 0)
if (triangles > 5000 || glb.length > 250_000) throw new Error(`Landmark exceeds budget: ${triangles} triangles`)
const out = process.argv[2] ?? new URL('../../landmarks/models/statue-of-liberty.glb', import.meta.url).pathname
await Bun.write(out, glb)
console.log(`${out}: ${triangles} triangles, ${glb.length} bytes (${(glb.length / 1024).toFixed(1)} KiB)`)
