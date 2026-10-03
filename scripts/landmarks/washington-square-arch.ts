/**
 * Washington Square Arch — procedural, CC0-1.0, no textures or source mesh.
 * bun scripts/landmarks/washington-square-arch.ts
 * Map frame: x = v/east, y = u/Fifth Avenue/north, z up. Place at bearing 32°.
 * The supplied OSM envelope is 19.1 × 7 m; the requested real height is 23.5 m.
 * Sculpture and the blank inscription panel are broad architectural reliefs.
 */
import { Part, addGltfTriangles, cross, sub, len, writeGlb, type V3 } from './mesh'

const stone = new Part()
const trim = new Part()
const recess = new Part()
const weathering = new Part()
const relief = new Part()
const statues = new Part()
let sculpture = relief
const SPRING = 9.8
const RADIUS = 4.5
const SEGMENTS = 32
const unit = (v: V3): V3 => v.map(x => x / (len(v) || 1)) as V3

function solid(p: Part, rings: V3[][]) {
  p.loft(rings)
  p.cap(rings[0], false)
  p.cap(rings[rings.length - 1], true)
}

function box(p: Part, x0: number, x1: number, y0: number, y1: number, z0: number, z1: number) {
  solid(p, [z0, z1].map(z => [[x0, y0, z], [x1, y0, z], [x1, y1, z], [x0, y1, z]] as V3[]))
}

function smooth(p: Part, build: (m: Part) => void, crease = 65) {
  const m = new Part()
  build(m)
  addGltfTriangles(p, new Float32Array(m.pos),
    Uint32Array.from({ length: m.pos.length / 3 }, (_, i) => i), { creaseDegrees: crease })
}

function oval(c: V3, rx: number, ry: number, n = 10): V3[] {
  return Array.from({ length: n }, (_, i) => {
    const a = i * Math.PI * 2 / n
    return [c[0] + rx * Math.cos(a), c[1] + ry * Math.sin(a), c[2]]
  })
}

function egg(c: V3, r: V3, n = 10, rows = 5) {
  smooth(sculpture, p => {
    const point = (i: number, j: number): V3 => {
      const a = i * Math.PI * 2 / n, b = -Math.PI / 2 + j * Math.PI / rows
      return [c[0] + r[0] * Math.cos(b) * Math.cos(a),
        c[1] + r[1] * Math.cos(b) * Math.sin(a), c[2] + r[2] * Math.sin(b)]
    }
    for (let j = 0; j < rows; j++) for (let i = 0; i < n; i++) {
      if (j > 0) p.tri(point(i, j), point(i + 1, j), point(i + 1, j + 1))
      if (j < rows - 1) p.tri(point(i, j), point(i + 1, j + 1), point(i, j + 1))
    }
  }, 78)
}

function limb(path: V3[], radii: number[], depth = 1, n = 8) {
  smooth(sculpture, p => solid(p, path.map((c, i) => {
    const axis = unit(sub(path[Math.min(i + 1, path.length - 1)], path[Math.max(0, i - 1)]))
    const u = unit(cross([0, 1, 0], axis)), v = cross(axis, u)
    return Array.from({ length: n }, (_, j): V3 => {
      const a = j * Math.PI * 2 / n
      return c.map((x, k) => x + radii[i] * (u[k] * Math.cos(a) + depth * v[k] * Math.sin(a))) as V3
    })
  })))
}

/** A segment of an annulus, including outward front/back and inward soffit. */
function archBand(p: Part, ri: number, ro: number, y0: number, y1: number, a0 = 0, a1 = Math.PI, steps = SEGMENTS) {
  const pt = (r: number, a: number, y: number): V3 => [r * Math.cos(a), y, SPRING + r * Math.sin(a)]
  for (let i = 0; i < steps; i++) {
    const a = a0 + (a1 - a0) * i / steps, b = a0 + (a1 - a0) * (i + 1) / steps
    p.quad(pt(ri, a, y0), pt(ro, a, y0), pt(ro, b, y0), pt(ri, b, y0))
    p.quad(pt(ri, b, y1), pt(ro, b, y1), pt(ro, a, y1), pt(ri, a, y1))
    p.quad(pt(ri, a, y1), pt(ri, a, y0), pt(ri, b, y0), pt(ri, b, y1))
    p.quad(pt(ro, a, y0), pt(ro, a, y1), pt(ro, b, y1), pt(ro, b, y0))
  }
  p.quad(pt(ri, a0, y1), pt(ro, a0, y1), pt(ro, a0, y0), pt(ri, a0, y0))
  p.quad(pt(ri, a1, y0), pt(ro, a1, y0), pt(ro, a1, y1), pt(ri, a1, y1))
}

// Two separate feet preserve the open passage down to ground level.
for (const s of [-1, 1]) {
  const x0 = s < 0 ? -9.55 : 4.5, x1 = s < 0 ? -4.5 : 9.55
  box(trim, x0, x1, -3.5, 3.5, 0, 0.42)
  box(stone, s < 0 ? -9.25 : 4.5, s < 0 ? -4.5 : 9.25, -3.18, 3.18, 0.42, 1.3)
  box(trim, s < 0 ? -9.35 : 4.5, s < 0 ? -4.5 : 9.35, -3.28, 3.28, 1.3, 1.58)
  box(stone, s < 0 ? -8.95 : 4.5, s < 0 ? -4.5 : 8.95, -2.65, 2.65, 1.58, 16.5)
  // Paneled pier faces: a broad recessed centre and raised, narrow borders.
  for (const face of [-1, 1]) {
    const cx = s * 6.94
    // Lower pier panels are clean flat marble in the references; only the
    // ornamented upper panel takes the greyer recessed-stone colour.
    box(stone, cx - 1.37, cx + 1.37, face * 2.70 - 0.035, face * 2.70 + 0.035, 2.2, 9.8)
    box(recess, cx - 1.37, cx + 1.37, face * 2.70 - 0.035, face * 2.70 + 0.035, 9.8, 12.7)
    for (const edge of [-1, 1]) box(trim, cx + edge * 1.62 - 0.16, cx + edge * 1.62 + 0.16,
      face * 2.8 - 0.15, face * 2.8 + 0.15, 1.58, 15.9)
    for (const z of [2.05, 12.72]) box(trim, cx - 1.46, cx + 1.46,
      face * 2.78 - 0.1, face * 2.78 + 0.1, z, z + 0.19)
  }
  box(trim, s < 0 ? -9.07 : 4.5, s < 0 ? -4.5 : 9.07, -2.99, 2.99, 9.58, 9.8)
  // Same-colour side frames catch light quietly, leaving the archivolt dominant.
  for (const y of [-2.12, 2.12]) box(stone, s * 9.0 - 0.075, s * 9.0 + 0.075,
    y - 0.13, y + 0.13, 1.58, 15.92)
  for (const z of [2.05, 15.72]) box(stone, s * 9.0 - 0.075, s * 9.0 + 0.075,
    -2.12, 2.12, z, z + 0.18)
}

// The wall above the circular opening is made of non-overlapping vertical
// strips, so there is a true tunnel rather than a dark painted arch shape.
for (let i = 0; i < SEGMENTS; i++) {
  const a = i * Math.PI / SEGMENTS, b = (i + 1) * Math.PI / SEGMENTS
  const xa = RADIUS * Math.cos(a), xb = RADIUS * Math.cos(b)
  const za = SPRING + RADIUS * Math.sin(a), zb = SPRING + RADIUS * Math.sin(b)
  stone.quad([xa, -2.65, za], [xa, -2.65, 16.5], [xb, -2.65, 16.5], [xb, -2.65, zb])
  stone.quad([xb, 2.65, zb], [xb, 2.65, 16.5], [xa, 2.65, 16.5], [xa, 2.65, za])
  stone.quad([xa, 2.65, za], [xa, -2.65, za], [xb, -2.65, zb], [xb, 2.65, zb])
}
for (const face of [-1, 1]) {
  // One uninterrupted broad archivolt survives aliased phone-sized views.
  // The archivolt is greyer than the adjacent flat marble in photo 01.
  archBand(recess, 4.5, 5.45, face > 0 ? 2.65 : -3.08, face > 0 ? 3.08 : -2.65)
  for (const side of [-1, 1]) {
    box(trim, side > 0 ? 4.5 : -4.98, side > 0 ? 4.98 : -4.5,
      face > 0 ? 2.65 : -3.08, face > 0 ? 3.08 : -2.65, 1.58, SPRING)
  }
}

// A few broad, low-contrast coffer fields replace the subpixel vault grid.
function soffit(p: Part, a: number, b: number, y0: number, y1: number, r: number) {
  const pt = (angle: number, y: number): V3 => [r * Math.cos(angle), y, SPRING + r * Math.sin(angle)]
  p.quad(pt(a, y1), pt(a, y0), pt(b, y0), pt(b, y1))
}
for (let k = 0; k < 4; k++) for (let row = 0; row < 2; row++) {
  const a = k * Math.PI / 4 + 0.10, b = (k + 1) * Math.PI / 4 - 0.10
  const y0 = -2.32 + row * 2.42, y1 = y0 + 2.12
  soffit(recess, a, b, y0, y1, 4.43)
}

// Architrave, frieze and gently projecting cornice; then the inscription attic.
for (const [z0, z1, hx, hy, p] of [
  [16.5, 16.78, 9.12, 2.98, trim], [16.78, 17.5, 8.97, 2.82, weathering],
  [17.5, 17.7, 9.22, 3.05, weathering], [17.7, 18.03, 9.4, 3.22, trim],
  [18.03, 18.25, 9.55, 3.5, trim], [18.25, 22.68, 8.94, 2.71, stone],
  [22.68, 22.9, 9.1, 3.04, trim], [22.9, 23.15, 9.32, 3.26, trim],
  [23.15, 23.32, 9.55, 3.5, trim],
] as Array<[number, number, number, number, Part]>) box(p, -hx, hx, -hy, hy, z0, z1)
// A shallow coping rim makes the stone roof legible from above, while the
// highest edge still lands exactly at the requested 23.5 m.
for (const side of [-1, 1]) {
  box(trim, -9.55, 9.55, side > 0 ? 3.18 : -3.5, side > 0 ? 3.5 : -3.18, 23.32, 23.5)
  box(trim, side > 0 ? 9.23 : -9.55, side > 0 ? 9.55 : -9.23, -3.18, 3.18, 23.32, 23.5)
}

// Actual shallow recesses instead of thin inscription marks that alias on the
// map. Surrounding stone stays flush with the attic's original face plane.
for (const face of [-1, 1]) {
  const y0 = face > 0 ? 2.71 : -2.87, y1 = face > 0 ? 2.87 : -2.71
  box(stone, -8.94, 8.94, y0, y1, 18.25, 19.22)
  box(stone, -8.94, 8.94, y0, y1, 21.87, 22.68)
  box(stone, -8.94, -7.68, y0, y1, 19.22, 21.87)
  box(stone, 7.68, 8.94, y0, y1, 19.22, 21.87)
  box(recess, -7.68, 7.68, face * 2.715 - 0.005, face * 2.715 + 0.005, 19.22, 21.87)
}

// A broad folded wing in the face plane; its ridge catches light as carving.
function wing(cx: number, y: number, z: number, side: number, scale = 1) {
  const outline = [[0, 0], [0.5, 0.85], [1.6, 1.05], [1.06, -0.1]]
  const ridge: V3 = [cx + side * 0.65 * scale, y + Math.sign(y) * 0.18, z + 0.35 * scale]
  for (let i = 0; i < outline.length; i++) {
    const a = outline[i], b = outline[(i + 1) % outline.length]
    const va: V3 = [cx + side * a[0] * scale, y, z + a[1] * scale]
    const vb: V3 = [cx + side * b[0] * scale, y, z + b[1] * scale]
    if (side * Math.sign(y) > 0) sculpture.tri(va, vb, ridge)
    else sculpture.tri(vb, va, ridge)
  }
}

// Winged victories in all four spandrels and a central spread-wing eagle.
for (const face of [-1, 1]) {
  for (const side of [-1, 1]) {
    const x = side * 5.95, y = face * 2.95
    wing(x, y, 14.3, side, 0.92)
    egg([x, y, 14.43], [0.32, 0.22, 0.32], 8, 4)
    limb([[x, y, 14.05], [x + side * 0.7, y, 13.72], [x + side * 1.75, y, 13.14]], [0.30, 0.38, 0.16], 0.55)
  }
  wing(-0.08, face * 3.04, 16.02, -1, 0.93)
  wing(0.08, face * 3.04, 16.02, 1, 0.93)
  egg([0, face * 3.15, 16.12], [0.3, 0.18, 0.52], 8, 4)
  egg([0, face * 3.17, 16.67], [0.18, 0.2, 0.2], 8, 4)
}

// North only: Washington at Peace (west) and at War (east). Each has its
// own stance, mantle and attributes, simplified to survive map distances.
sculpture = statues
for (const side of [-1, 1]) {
  const x = side * 6.94, y = 2.89
  box(trim, x - 1.22, x + 1.22, 2.1, 3.5, 1.58, 2.06)
  box(statues, x - 1.05, x + 1.05, 2.15, 3.42, 2.06, 2.7)
  for (const leg of [-1, 1]) {
    limb([[x + leg * 0.3, y, 2.72], [x + leg * 0.3, y - 0.02, 3.7], [x + leg * 0.2, y - 0.04, 4.55]], [0.21, 0.22, 0.27], 0.8)
    egg([x + leg * 0.3, 3.08, 2.84], [0.23, 0.32, 0.13], 8, 4)
  }
  smooth(sculpture, p => solid(p, [
    oval([x, y - 0.12, 3.62], 0.76, 0.36), oval([x, y - 0.14, 4.55], 0.61, 0.38),
    oval([x, y - 0.1, 5.51], 0.72, 0.42), oval([x, y - 0.08, 5.88], 0.49, 0.34),
    oval([x, y - 0.07, 6.0], 0.21, 0.2),
  ]))
  // The mantle is one broad silhouette; small folds and facial relief disappear
  // at phone sizes and are intentionally omitted.
  smooth(sculpture, p => solid(p, [
    oval([x + side * 0.4, 2.77, 3.2], 0.47, 0.30, 8),
    oval([x + side * 0.38, 2.8, 4.48], 0.43, 0.32, 8),
    oval([x + side * 0.27, 2.85, 5.78], 0.34, 0.31, 8),
  ]))
  egg([x, y - 0.05, 6.34], [0.36, 0.34, 0.46], 8, 4)
  limb([[x - 0.5, y, 5.65], [x - 0.74, y, 4.95], [x - 0.68, 3.14, 4.65]], [0.27, 0.22, 0.17], 0.85)
  limb([[x + 0.49, y, 5.65], [x + 0.7, y, 5.04], [x + 0.46, 3.18, 4.91]], [0.27, 0.22, 0.17], 0.85)
  // Peace holds a broad document; War's sword merges quietly into the mantle.
  if (side < 0) box(sculpture, x + 0.12, x + 0.62, 3.12, 3.28, 4.5, 5.04)
}

// Raw sRGB photo samples, without lightening, desaturation or map compensation.
// Coordinates are pixel rectangles in the supplied 1280 px reference photos.
// The writer performs its usual sRGB-to-linear conversion for baseColorFactor.
// Photo 01 carries a cool sky cast on the lower piers; photo 02's north-facing
// statue is more weathered and less brightly lit. Retain the recorded samples
// rather than inventing unobserved cream paint or baking in white highlights.
const parts = [
  // 01: (1005,1040,16,16), exposed right pier.
  { part: stone, material: { name: 'marble', color: 0xcdcacf } },
  // 01: (385,530,12,12), exposed attic moulding.
  { part: trim, material: { name: 'marble-mouldings', color: 0xcdc5c7 } },
  // 01: (737,480,60,15), inscription-panel stone.
  { part: recess, material: { name: 'marble-recesses', color: 0xbeb6bb } },
  // 01: (761,565,30,5), exposed weathered cornice lip; exclude its black shadow.
  { part: weathering, material: { name: 'marble-weathering', color: 0xb2a7a9 } },
  // 01: (751,637,29,24), brightest 20% of relief pixels to exclude carved shadows.
  { part: relief, material: { name: 'marble-relief', color: 0xcec4c7, doubleSided: true } },
  // 02: (486,780,4,4), exposed robe ridge, avoiding the adjacent stained crease.
  { part: statues, material: { name: 'marble-statues', color: 0x9a9692, doubleSided: true } },
]
const triangles = parts.reduce((sum, { part }) => sum + part.triangles, 0)
if (triangles > 5000) throw new Error(`Triangle budget exceeded: ${triangles}`)
const glb = writeGlb('Washington Square Arch', parts, {
  license: 'CC0-1.0', frame: 'Y up, -Z north, +X east, metres; origin at ground anchor',
  bearing: 32, elevation: 0, footprint: [19.1, 7], height: 23.5,
  openingWidth: 9, openingHeight: SPRING + RADIUS, northFace: 'War east; Peace west',
})
const out = new URL('../../landmarks/models/washington-square-arch.glb', import.meta.url).pathname
await Bun.write(out, glb)
console.log(`${out}: ${triangles} triangles, ${glb.length} bytes`)
