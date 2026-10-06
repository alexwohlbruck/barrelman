/**
 * Bank of America Corporate Center (1992, Cesar Pelli), Charlotte —
 * original procedural geometry, CC0-1.0.
 * bun scripts/landmarks/bank-of-america-corporate-center.ts [out.glb]
 *
 * Map frame: x east, y north, z up, metres; origin at the anchor on the
 * ground. Placed at bearing 48.8°, so the tower's faces, which sit square to
 * Uptown's street grid, are square to this frame.
 *
 * Plan: a square core with a broad, slightly bowed slab standing proud of the
 * middle of each face. The corners between the slabs are re-entrant and
 * glazed. Going up, the core corners step in (161, 177 m) and the slabs step
 * back (177, 208, 224 m) without narrowing, so the plan becomes a stepped
 * cross; a silver-finned square band (224–235 m) carries a stepped crown of
 * silver rods in three rings, ending at 264 m.
 *
 * Sources: heights are OSM's building:parts (exact). Plan dimensions are
 * measured from Google's photorealistic 3D renders (top and elevation views),
 * which put the slabs at ±24.6 m and 25 m wide. OSM's tier outlines trace the
 * core corners, not the slabs, so they are used for the core steps only. The
 * crown's ring radii are from the elevation silhouettes.
 */
import { Part, encodePng, writeGlb, type V3 } from './mesh'

type XY = [number, number]

const granite = new Part(), base = new Part(), glass = new Part()
const silver = new Part(), steel = new Part(), terrace = new Part(), notchGlass = new Part()

// ---------- helpers ----------

const norm2 = (v: XY): XY => { const l = Math.hypot(v[0], v[1]) || 1; return [v[0] / l, v[1] / l] }
const add2 = (a: XY, b: XY, k = 1): XY => [a[0] + b[0] * k, a[1] + b[1] * k]
const at = (p: XY, z: number): V3 => [p[0], p[1], z]
const rot = (p: XY, s: number): XY => {
  let [x, y] = p
  for (let k = 0; k < s; k++) [x, y] = [-y, x]
  return [x, y]
}

/** A planar convex polygon, wound to face `hint`. */
function poly(part: Part, pts: V3[], hint: V3) {
  const a = pts[0], b = pts[1], c = pts[2]
  const n = [
    (b[1] - a[1]) * (c[2] - a[2]) - (b[2] - a[2]) * (c[1] - a[1]),
    (b[2] - a[2]) * (c[0] - a[0]) - (b[0] - a[0]) * (c[2] - a[2]),
    (b[0] - a[0]) * (c[1] - a[1]) - (b[1] - a[1]) * (c[0] - a[0]),
  ]
  const p = n[0] * hint[0] + n[1] * hint[1] + n[2] * hint[2] < 0 ? [...pts].reverse() : pts
  for (let i = 1; i < p.length - 1; i++) part.tri(p[0], p[i], p[i + 1])
}

/** Outward normal of a counter-clockwise edge. */
const outward = (a: XY, b: XY): XY => norm2([b[1] - a[1], -(b[0] - a[0])])

/** A plain wall over a ring edge. */
function wall(part: Part, a: XY, b: XY, z0: number, z1: number) {
  part.quad(at(a, z0), at(b, z0), at(b, z1), at(a, z1))
}

/**
 * A window band: the edge's wall with a full-height opening recessed `r`,
 * leaving `sill` and `head` of solid wall below and above.
 */
function band(a: XY, b: XY, z0: number, z1: number, sill: number, head: number, headPart = granite, r = .6) {
  const n = outward(a, b), ai = add2(a, n, -r), bi = add2(b, n, -r)
  const lo = z0 + sill, hi = z1 - head
  if (sill > 0) wall(granite, a, b, z0, lo)
  if (head > 0) wall(headPart, a, b, hi, z1)
  wall(glass, ai, bi, lo, hi)
  const t = norm2([b[0] - a[0], b[1] - a[1]])
  poly(granite, [at(a, lo), at(ai, lo), at(ai, hi), at(a, hi)], [t[0], t[1], 0])
  poly(granite, [at(b, lo), at(bi, lo), at(bi, hi), at(b, hi)], [-t[0], -t[1], 0])
  if (sill > 0) poly(granite, [at(a, lo), at(b, lo), at(bi, lo), at(ai, lo)], [0, 0, 1])
  if (head > 0) poly(granite, [at(a, hi), at(b, hi), at(bi, hi), at(ai, hi)], [0, 0, -1])
}

/** A star-shaped ring capped by a fan from the origin. */
function cap(part: Part, ring: XY[], z: number) {
  for (let i = 0; i < ring.length; i++) {
    const a = ring[i], b = ring[(i + 1) % ring.length]
    part.tri([0, 0, z], at(a, z), at(b, z))
  }
}

/**
 * A crown rod: a slim fin of triangular section standing on the wall at `p`,
 * facing `n`, from `z0` to `z1` and then a short point.
 */
function fin(part: Part, p: XY, n: XY, z0: number, z1: number, tip: number, w = .6, d = 1.1) {
  const t: XY = [-n[1], n[0]]
  const b1 = add2(add2(p, t, -w / 2), n, -.2), b2 = add2(add2(p, t, w / 2), n, -.2), o = add2(p, n, d)
  const apex: V3 = [p[0] + n[0] * d * .3, p[1] + n[1] * d * .3, z1 + tip]
  for (const [a, b] of [[b1, o], [o, b2]] as [XY, XY][]) {
    const m = outward(a, b)
    poly(part, [at(a, z0), at(b, z0), at(b, z1), at(a, z1)], [m[0], m[1], 0])
    poly(part, [at(a, z1), at(b, z1), apex], [m[0], m[1], .5])
  }
  poly(part, [at(b1, z1), at(b2, z1), apex], [-n[0], -n[1], .5])
}

// ---------- the tower ----------

const W = 12.6      // slab half-width (25 m slabs on a 49 m face)
const BOW = .8      // how far a slab's middle stands proud of its ends
const CH = .6       // chamfer on every convex vertical edge
const SLAB_SEGS = 6  // facets across a bowed slab face

/**
 * The facade is a painted window grid, not geometry: one texture tile is one
 * window module, a bay wide and a floor tall, with a dark window on granite.
 * The renderer mipmaps it, so at map distance the grid averages to an even,
 * light texture instead of shimmering, as a fine geometric grid would.
 */
const BAY = 1.5, FLOOR = 3.9
const GRANITE: [number, number, number] = [0xc8, 0xc0, 0xb7]
const WINDOW: [number, number, number] = [0x5d, 0x6b, 0x7c]
const WINDOW_GRID = (() => {
  const w = 32, h = 32, data = new Uint8Array(w * h * 4)
  // window ≈ 45% of the bay's width and 55% of the floor's height
  const x0 = Math.round(w * .275), x1 = Math.round(w * .725), y0 = Math.round(h * .2), y1 = Math.round(h * .75)
  for (let y = 0; y < h; y++) for (let x = 0; x < w; x++)
    data.set([...(x >= x0 && x < x1 && y >= y0 && y < y1 ? WINDOW : GRANITE), 255], (y * w + x) * 4)
  return encodePng(w, h, data)
})()
const facade = new Part()

/**
 * A run of wall edges that make one face, painted with the window grid. The
 * module is stretched so a whole number of bays fits the face edge to edge,
 * and v counts floors from the ground, so floors line up across every face.
 */
function paintedFace(pts: XY[], z0: number, z1: number) {
  const lens = pts.slice(1).map((p, i) => Math.hypot(p[0] - pts[i][0], p[1] - pts[i][1]))
  const total = lens.reduce((s, l) => s + l, 0)
  const bay = total / Math.max(1, Math.round(total / BAY))
  let u = 0
  lens.forEach((l, i) => {
    const a = pts[i], b = pts[i + 1], u1 = u + l / bay
    facade.quad(at(a, z0), at(b, z0), at(b, z1), at(a, z1), [[u, z0 / FLOOR], [u1, z0 / FLOOR], [u1, z1 / FLOOR], [u, z1 / FLOOR]])
    u = u1
  })
}

/**
 * Tier plans. `c` is the core corner and `n` the glazed notch cut out of it.
 * The notches are kept narrow so they read as a seam; `c` is set so that
 * 2c − n, the corner's reach in the diagonal views, matches the renders.
 */
type Tier = { z0: number; z1: number; P: number; c: number; n: number }
const tiers: Tier[] = [
  { z0: 42, z1: 161, P: 24.6, c: 20.1, n: 2.2 },
  { z0: 161, z1: 177, P: 24.6, c: 18.6, n: 1.8 },
  { z0: 177, z1: 208, P: 22.3, c: 17.45, n: 1.5 },
  { z0: 208, z1: 224, P: 19.5, c: 17.1, n: 1.2 },
]

type Kind = 'slab' | 'trim' | 'return' | 'core' | 'notch'
type Run = { pts: XY[]; kind: Kind }

/** One tier's ring as runs of edges (one run per face), counter-clockwise. */
function outline(P: number, c: number, n: number): Run[] {
  const yf = (x: number) => -(P - BOW * (x / W) ** 2)
  const side: Run[] = []
  const face: XY[] = []
  for (let k = 0; k <= SLAB_SEGS; k++) {
    const x = -(W - CH) + 2 * (W - CH) * k / SLAB_SEGS
    face.push([x, yf(x)])
  }
  side.push({ pts: face, kind: 'slab' })
  const end: XY = [W, yf(W) + CH]
  side.push({ pts: [face[SLAB_SEGS], end], kind: 'trim' })
  if (c > W + 1) {
    side.push({ pts: [end, [W, -c]], kind: 'return' })
    side.push({ pts: [[W, -c], [c - n, -c]], kind: 'core' })
    side.push({ pts: [[c - n, -c], [c - n, -c + n], [c, -c + n]], kind: 'notch' })
    side.push({ pts: [[c, -c + n], [c, -W]], kind: 'core' })
    side.push({ pts: [[c, -W], [P - BOW - CH, -W]], kind: 'return' })
  } else side.push({ pts: [end, [W, -W], [P - BOW - CH, -W]], kind: 'return' })
  // the next slab's chamfer, ending at that slab's first point
  side.push({ pts: [[P - BOW - CH, -W], rot(face[0], 1)], kind: 'trim' })
  const runs: Run[] = []
  for (let s = 0; s < 4; s++) for (const r of side) runs.push({ kind: r.kind, pts: r.pts.map((p) => rot(p, s)) })
  return runs
}

function tierWalls(t: Tier, next: Tier | undefined, silverHead: boolean) {
  const runs = outline(t.P, t.c, t.n)
  for (const r of runs) {
    // a slab that carries on into the next tier has no parapet here
    const head = silverHead && !(r.kind === 'slab' && next?.P === t.P) ? 2.5 : 0
    const top = t.z1 - head
    if (r.kind === 'notch') r.pts.slice(1).forEach((b, i) => wall(notchGlass, r.pts[i], b, t.z0, t.z1))
    else if (r.kind === 'trim') wall(granite, r.pts[0], r.pts[1], t.z0, top)
    else paintedFace(r.pts, t.z0, top)
    if (head) r.pts.slice(1).forEach((b, i) => wall(silver, r.pts[i], b, top, t.z1))
  }
  const ring = runs.flatMap((r) => r.pts.slice(0, -1))
  cap(terrace, ring, t.z1)
}

tiers.forEach((t, i) => tierWalls(t, tiers[i + 1], i >= 1))

// ---------- base, 0–42 m: a full square, the slabs' own width ----------
// A 12 m plinth of the darker stone with three tall glazed openings a face,
// then the window grid up to 42 m.
{
  const H = 24.4, Z1 = 42, PLINTH = 12, DOOR = 2.2
  const ring: XY[] = []
  for (let s = 0; s < 4; s++) {
    const xs = [-(H - CH), -14 - DOOR, -14 + DOOR, -DOOR, DOOR, 14 - DOOR, 14 + DOOR, H - CH]
    const pts = xs.map((x) => rot([x, -H], s))
    pts.slice(1).forEach((b, i) => {
      if (i % 2) band(pts[i], b, 0, PLINTH, 0, 3, base, .8)
      else wall(base, pts[i], b, 0, PLINTH)
    })
    paintedFace(pts, PLINTH, Z1)
    const corner: [XY, XY] = [rot([H - CH, -H], s), rot([H, -H + CH], s)]
    wall(base, corner[0], corner[1], 0, PLINTH)
    wall(granite, corner[0], corner[1], PLINTH, Z1)
    ring.push(...pts)
  }
  cap(terrace, ring, Z1)
}

// ---------- the silver band, 224–235 m ----------
// A rounded square (a superellipse, |x|³ + |y|³ = S³): the renders show it
// ±17.3 m on the axes but only ≈19 m out on the diagonals, a curved band
// rather than a square with corners.
const S = 17.3, BAND_SEGS = 32
const BAND0 = 224, GLASS1 = 228.5, BAND1 = 235
const bandRing: XY[] = [], bandNormals: XY[] = []
for (let i = 0; i < BAND_SEGS; i++) {
  const a = (i + .5) / BAND_SEGS * 2 * Math.PI, c = Math.cos(a), sn = Math.sin(a)
  const k = S / Math.cbrt(Math.abs(c) ** 3 + Math.abs(sn) ** 3)
  const p: XY = [k * c, k * sn]
  bandRing.push(p)
  bandNormals.push(norm2([Math.sign(p[0]) * p[0] ** 2, Math.sign(p[1]) * p[1] ** 2]))
}
for (let i = 0; i < BAND_SEGS; i++) {
  const a = bandRing[i], b = bandRing[(i + 1) % BAND_SEGS]
  wall(glass, a, b, BAND0, GLASS1)
  wall(steel, a, b, GLASS1, BAND1)
}
cap(steel, bandRing, BAND1)
// rods at every vertex and every edge's middle; the four on the diagonals
// stand a little taller
bandRing.forEach((p, i) => {
  const q = bandRing[(i + 1) % BAND_SEGS]
  const m = norm2([p[0] + q[0], p[1] + q[1]]), k = S / Math.cbrt(Math.abs(m[0]) ** 3 + Math.abs(m[1]) ** 3)
  const mp: XY = [m[0] * k, m[1] * k]
  const diagonal = i % (BAND_SEGS / 4) === BAND_SEGS / 8 - 1 || i % (BAND_SEGS / 4) === BAND_SEGS / 8
  fin(silver, p, bandNormals[i], GLASS1 - 1, BAND1 - .5 + (diagonal ? 1.5 : 0), 1.2)
  fin(silver, mp, norm2([Math.sign(mp[0]) * mp[0] ** 2, Math.sign(mp[1]) * mp[1] ** 2]), GLASS1 - 1, BAND1 - .5, 1.2)
})

// ---------- the crown: rings of rods ----------
function ring(r: number, z0: number, z1: number, rods: number, rodTop: number, segs = 16) {
  const pts: XY[] = []
  for (let i = 0; i < segs; i++) {
    const a = (i + .5) / segs * 2 * Math.PI
    pts.push([r * Math.cos(a), r * Math.sin(a)])
  }
  for (let i = 0; i < segs; i++) wall(steel, pts[i], pts[(i + 1) % segs], z0, z1)
  cap(steel, pts, z1)
  for (let i = 0; i < rods; i++) {
    const a = i / rods * 2 * Math.PI, n: XY = [Math.cos(a), Math.sin(a)]
    fin(silver, [n[0] * r * .98, n[1] * r * .98], n, z0 + 1, rodTop, 1.4)
  }
}
// Rings of rods. The elevations step 14.4 m → 245, 11.4 m → 253, ≈7.5 m →
// 256 and a 5.2 m drum → 264; the top view shows another rim at ≈12.5 m.
const RINGS: [number, number, number, number][] = [
  // radius, wall top, rod top, rods
  [14.4, 241.5, 243.6, 68],
  [12.7, 245, 246.8, 60],
  [11.0, 249.5, 251.6, 52],
  [7.8, 254, 255.2, 36],
  [5.2, 261.5, 262.6, 32],
]
RINGS.forEach(([r, top, rod, n], i) => ring(r, i ? RINGS[i - 1][1] : BAND1, top, n, rod, i > 2 ? 12 : 16))

// ---------- write ----------
const parts = [
  { part: facade, material: { name: 'facade-window-grid', color: 0xffffff, texture: { png: WINDOW_GRID } } },
  { part: granite, material: { name: 'granite', color: 0xc8c0b7 } },
  { part: base, material: { name: 'granite-base', color: 0xb3a99f } },
  { part: glass, material: { name: 'glass', color: 0x5d6b7c } },
  { part: notchGlass, material: { name: 'corner-notch-glass', color: 0x7d8b9a } },
  { part: silver, material: { name: 'crown-rods', color: 0xdfe3e6, roughness: .5 } },
  { part: steel, material: { name: 'crown-body', color: 0xa9b8c6, roughness: .6 } },
  { part: terrace, material: { name: 'ledges', color: 0xd4cdc4 } },
]
const triangles = parts.reduce((n, { part }) => n + part.triangles, 0)
if (triangles > 6500) throw new Error(`Triangle budget exceeded: ${triangles}`)
const glb = writeGlb('Bank of America Corporate Center', parts, {
  license: 'CC0-1.0', bearing: 48.8, elevation: 0, height: 264,
  frame: 'Y up, -Z north, +X east, metres; origin at the anchor on the ground',
  replaces: ['way/341587198', 'way/341587200', 'way/341587199', 'way/341587201', 'way/341587202', 'way/341587204', 'way/766639185', 'way/341587207', 'way/341587210', 'way/766639186', 'way/341587214'],
})
if (glb.length > 256000) throw new Error(`File budget exceeded: ${glb.length}`)
const out = process.argv[2] ?? new URL('../../landmarks/models/bank-of-america-corporate-center.glb', import.meta.url).pathname
await Bun.write(out, glb)
console.log(`${out}: ${triangles} triangles, ${glb.length} bytes (${(glb.length / 1024).toFixed(1)} KiB)`)
