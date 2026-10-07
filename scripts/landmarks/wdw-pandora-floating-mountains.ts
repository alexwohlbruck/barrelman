/**
 * The floating mountains of the Valley of Mo'ara, Pandora – The World of
 * Avatar, Disney's Animal Kingdom — procedural, CC0-1.0.
 *   bun scripts/landmarks/wdw-pandora-floating-mountains.ts
 *
 * Map frame: x east, y north, z up, metres. Written in a park frame about
 * (-81.5925, 28.3555) and shifted so the origin is the centroid of the five
 * rocks' outlines; the script prints that anchor. Bearing 0.
 *
 * Five rock masses hang over the valley: craggy, leaning, faceted blocks,
 * top-heavy with an overhanging brow, a grey-violet keel and a chunk fused on
 * one flank, moss over the tops. They are held up by a tangle of vines that
 * run down at angles in sagging bundles to roots spread round each rock, and
 * swoop from rock to rock in catenaries; three thin waterfalls drop from the
 * brows. Two small rocks float higher, slung on vines of their own.
 *
 * Evidence
 * - OSM (measured): the rocks are the five `building=roof` + `layer=1` blobs
 *   in the valley, ways 668089393, 668089395, 668089396, 668089397 and
 *   668089398 (21 × 16 m down to 6 × 5 m), between the Flight of Passage
 *   rock ridge (way/668089387, not modelled here) and the Na'vi River pool.
 *   Each rock's plan is its outline, smoothed.
 * - USGS NAIP orthophoto (public domain): each outline sits over a grey-green
 *   rock top with vegetation, the valley floor visible between them.
 * - Photos (Wikimedia Commons; credits in the placement report): "Floating
 *   Mountain (33858129633).jpg", "… (34537473301).jpg", "… (34667777355).jpg"
 *   (Theme Park Tourist, CC BY 2.0) and the elisfkc set (CC BY-SA 2.0): the
 *   top-heavy anvil shape, tan-ochre rock over grey-violet lower strata, moss
 *   and plants over the tops, thick twisted vine stalks from the keels to the
 *   ground, vines draped between rocks.
 * - Estimated: every height. The tallest rock tops out about 40 m; the
 *   undersides float 11 to 28 m up; no licensed source gives the real
 *   figures, so these come from the photos' proportions against people and
 *   trees. Where the vines root is invented, placed so each rock reads as held
 *   up from three sides.
 * - Not modelled: the pool and the plants on the valley
 *   floor (ground, per STYLE.md), the Flight of Passage ridge.
 */
import { Part, addGltfTriangles, cross, len, sub, writeGlb, type V3 } from './mesh'
import { finish } from './palette'

type XY = [number, number]
const TAU = Math.PI * 2
const unit = (v: V3): V3 => { const l = len(v) || 1; return [v[0] / l, v[1] / l, v[2] / l] }
const plus = (a: V3, b: V3): V3 => [a[0] + b[0], a[1] + b[1], a[2] + b[2]]
const scale = (a: V3, k: number): V3 => [a[0] * k, a[1] * k, a[2] * k]
const clamp = (x: number, a = 0, b = 1) => Math.min(b, Math.max(a, x))
const hash = (n: number) => { const s = Math.sin(n * 127.1 + 311.7) * 43758.5453; return s - Math.floor(s) }

const LAT0 = 28.3555, LON0 = -81.5925
const MX = 111320 * Math.cos((LAT0 * Math.PI) / 180), MY = 110574

/** The five rocks: OSM outline (park frame), keel and top heights (estimated). */
const ROCKS: { id: string; outline: XY[]; keel: number; top: number }[] = [
  { id: 'way/668089393', keel: 13, top: 33, outline: [[52.4, -30.0], [51.1, -27.4], [50.7, -26.3], [48.4, -26.1], [47.0, -25.4], [45.8, -24.2], [48.1, -24.0], [49.1, -23.7], [49.6, -22.7], [49.9, -20.9], [50.9, -19.3], [52.3, -18.2], [53.3, -18.0], [55.1, -16.9], [56.8, -16.0], [58.4, -15.8], [60.7, -16.3], [62.2, -16.8], [63.5, -18.0], [66.0, -21.5], [66.9, -23.4], [67.0, -26.3], [66.4, -28.4], [64.3, -30.3], [61.8, -31.6], [59.1, -32.4], [55.5, -32.1], [53.6, -31.8], [52.8, -31.3]] },
  { id: 'way/668089395', keel: 11, top: 23, outline: [[44.5, -1.0], [46.6, -1.3], [48.1, -2.0], [49.3, -3.4], [49.9, -5.1], [50.4, -5.9], [51.8, -6.2], [52.6, -6.8], [52.7, -7.6], [51.5, -8.9], [50.3, -10.3], [48.2, -10.9], [45.3, -11.2], [44.0, -11.1], [42.6, -10.0], [42.1, -8.9], [42.2, -8.0], [43.0, -7.0], [43.7, -6.9], [44.8, -7.1], [42.9, -5.8], [41.9, -4.5], [41.4, -3.2], [41.6, -2.4], [43.2, -1.3]] },
  { id: 'way/668089396', keel: 19, top: 40, outline: [[38.0, 6.5], [39.9, 4.7], [41.1, 3.4], [43.0, 2.7], [44.6, 2.6], [46.6, 3.4], [47.7, 4.5], [48.6, 6.2], [49.0, 9.2], [48.7, 10.9], [48.0, 12.2], [46.9, 13.3], [45.3, 14.2], [43.7, 14.7], [42.1, 15.2], [40.8, 16.2], [39.9, 17.3], [38.2, 18.2], [36.9, 18.1], [35.3, 17.5], [33.9, 16.0], [32.7, 13.7], [32.7, 11.1], [33.0, 9.5], [34.5, 8.0], [35.7, 8.2], [36.4, 8.3], [37.2, 7.8]] },
  { id: 'way/668089397', keel: 22, top: 33, outline: [[54.6, -2.2], [53.8, -1.1], [52.3, 0.5], [51.2, 1.9], [50.8, 2.6], [50.5, 3.1], [50.6, 4.5], [51.9, 5.6], [53.6, 6.1], [55.8, 6.2], [57.4, 5.7], [59.0, 4.9], [60.6, 3.6], [60.8, 3.0], [61.2, 2.1], [61.1, 1.0], [60.4, -0.2], [58.5, -1.6], [57.2, -1.9], [55.5, -2.4]] },
  { id: 'way/668089398', keel: 27, top: 34, outline: [[61.9, 9.1], [60.7, 8.2], [60.2, 7.6], [60.4, 6.8], [61.3, 5.4], [62.5, 4.9], [63.5, 4.7], [64.4, 4.9], [65.7, 6.0], [66.3, 7.3], [66.0, 8.4], [65.0, 9.1], [63.2, 9.4]] },
]

const centroid = (p: XY[]) => {
  let a = 0, cx = 0, cy = 0
  for (let i = 0; i < p.length; i++) {
    const [x0, y0] = p[i], [x1, y1] = p[(i + 1) % p.length], c = x0 * y1 - x1 * y0
    a += c; cx += (x0 + x1) * c; cy += (y0 + y1) * c
  }
  return { c: [cx / (3 * a), cy / (3 * a)] as XY, area: Math.abs(a / 2) }
}
// The anchor: the area-weighted centre of the five outlines.
const [AX, AY] = (() => {
  let w = 0, x = 0, y = 0
  for (const r of ROCKS) { const { c, area } = centroid(r.outline); w += area; x += c[0] * area; y += c[1] * area }
  return [x / w, y / w]
})()

const rockTan = new Part(), rockGrey = new Part(), moss = new Part(), vine = new Part()

function smooth(target: Part, build: (p: Part) => void, crease = 50) {
  const p = new Part()
  build(p)
  addGltfTriangles(target, new Float32Array(p.pos), Uint32Array.from({ length: p.pos.length / 3 }, (_, i) => i), { creaseDegrees: crease })
}

/** Distance from `c` to the outline along azimuth `a`. */
function rayOut(poly: XY[], c: XY, a: number) {
  const d = [Math.cos(a), Math.sin(a)]
  let best = 0
  for (let i = 0; i < poly.length; i++) {
    const x0 = poly[i][0] - c[0], y0 = poly[i][1] - c[1]
    const x1 = poly[(i + 1) % poly.length][0] - c[0], y1 = poly[(i + 1) % poly.length][1] - c[1]
    const ex = x1 - x0, ey = y1 - y0, den = d[0] * ey - d[1] * ex
    if (Math.abs(den) < 1e-9) continue
    const t = (x0 * ey - y0 * ex) / den, u = (x0 * d[1] - y0 * d[0]) / den
    if (t > 0 && u >= 0 && u <= 1) best = Math.max(best, t)
  }
  return best
}
/** The outline's radius about its centroid, fitted to order 4: its shape without its kinks. */
function planOf(poly: XY[], c: XY) {
  const N = 120, r = Array.from({ length: N }, (_, i) => rayOut(poly, c, (i / N) * TAU))
  const k4 = Array.from({ length: 5 }, (_, k) => {
    let a = 0, b = 0
    for (let i = 0; i < N; i++) { const t = (i / N) * TAU; a += r[i] * Math.cos(k * t); b += r[i] * Math.sin(k * t) }
    return [(a * (k ? 2 : 1)) / N, (b * 2) / N]
  })
  return (a: number) => k4.reduce((s, [ca, cb], k) => s + ca * Math.cos(k * a) + (k ? cb * Math.sin(k * a) : 0), 0)
}

/**
 * A faceted block: rings of `n` corners at given heights, each ring its own
 * irregular polygon, flat-shaded so every face reads as a cut of rock.
 * `levels` are [z, scale, shift x, shift y]; scale is of the plan radius.
 */
function block(p: Part, c: XY, plan: (a: number) => number, levels: [number, number, number, number][], seed: number, n = 8, jag = 0.22) {
  const turn = hash(seed + 1) * TAU
  const rings = levels.map(([z, k, dx, dy], j) => Array.from({ length: n }, (_, i): V3 => {
    const a = (i / n) * TAU + turn + (hash(seed * 7 + i) - 0.5) * 0.35
    const r = plan(a) * k * (1 + jag * (hash(seed * 31 + i * 5 + j) - 0.5))
    return [c[0] + dx + r * Math.cos(a), c[1] + dy + r * Math.sin(a), z]
  }))
  p.loft(rings)
  return rings
}
const fan = (p: Part, ring: V3[], apex: V3, up: boolean) => {
  for (let i = 0; i < ring.length; i++) {
    const a = ring[i], b = ring[(i + 1) % ring.length]
    if (up) p.tri(a, b, apex)
    else p.tri(b, a, apex)
  }
}

/**
 * One floating rock: a craggy, top-heavy mass that leans. The upper block is
 * widest at its brow and overhangs; a recessed band of strata cuts under it;
 * the lower block, smaller and shifted to one side, hangs in a grey-violet
 * keel. A second, smaller chunk is fused on the flank so no rock is a turned
 * pot. Moss covers the top and spills over the brow.
 */
function rock(c: XY, plan: (a: number) => number, keel: number, top: number, seed: number) {
  const H = top - keel
  const lean = hash(seed + 5) * TAU, L = 0.18 * H
  const lx = Math.cos(lean) * L, ly = Math.sin(lean) * L
  // Upper block: brow overhanging the waist below it.
  const upper = block(rockTan, c, plan, [
    [keel + 0.42 * H, 0.72, lx * 0.5, ly * 0.5],
    [keel + 0.5 * H, 0.86, lx * 0.6, ly * 0.6],
    [keel + 0.56 * H, 0.8, lx * 0.65, ly * 0.65],   // the recessed band
    [keel + 0.82 * H, 1.0, lx * 0.85, ly * 0.85],   // the brow
    [top, 0.92, lx, ly],
  ], seed)
  // Lower block, grey, smaller and off-centre, down to a blunt keel.
  const kx = -Math.sin(lean) * 0.12 * H, ky = Math.cos(lean) * 0.12 * H
  const lower = block(rockGrey, c, plan, [
    [keel, 0.22, kx, ky],
    [keel + 0.15 * H, 0.5, kx * 0.7, ky * 0.7],
    [keel + 0.3 * H, 0.66, kx * 0.4, ky * 0.4],
    [keel + 0.44 * H, 0.74, lx * 0.5, ly * 0.5],
  ], seed + 40, 7, 0.3)
  fan(rockGrey, lower[0], [c[0] + kx, c[1] + ky, keel - 1.5], false)
  // A flank chunk on the side away from the lean.
  if (H > 9) {
    const fc: XY = [c[0] - Math.cos(lean) * plan(lean + Math.PI) * 0.7, c[1] - Math.sin(lean) * plan(lean + Math.PI) * 0.7]
    const s = plan(lean + Math.PI) * 0.42
    const flank = block(rockTan, fc, () => s, [
      [keel + 0.25 * H, 0.45, 0, 0], [keel + 0.45 * H, 1, 0, 0], [keel + 0.72 * H, 0.9, 0, 0],
    ], seed + 80, 6, 0.3)
    fan(rockGrey, flank[0], [fc[0], fc[1], keel + 0.12 * H], false)
    fan(moss, flank[2], [fc[0], fc[1], keel + 0.72 * H + 1], true)
  }
  // Moss on top, spilling over the brow in tongues of different lengths.
  const brow = upper[3], rim = upper[4]
  const lip = rim.map((v, i): V3 => {
    const o = unit([v[0] - c[0] - lx, v[1] - c[1] - ly, 0])
    return [v[0] + o[0] * 0.5, v[1] + o[1] * 0.5, v[2] + 0.4]
  })
  const drip = brow.map((v, i): V3 => {
    const o = unit([v[0] - c[0] - lx, v[1] - c[1] - ly, 0]), t = hash(seed * 17 + i) ** 1.5
    return [v[0] + o[0] * 0.4, v[1] + o[1] * 0.4, top - 1.2 - t * (top - v[2]) * 0.9]
  })
  moss.loft([drip, lip])
  fan(moss, lip, [c[0] + lx, c[1] + ly, top + 1.8], true)
  // A crag standing up from the larger tops.
  if (H > 14) {
    const a = lean + 0.9, d = plan(a) * 0.4
    const sc: XY = [c[0] + lx + Math.cos(a) * d, c[1] + ly + Math.sin(a) * d], s = plan(a) * 0.32, h = H * 0.3
    const crag = block(rockTan, sc, () => s, [[top, 0.85, 0, 0], [top + h * 0.7, 1, 0.6, 0.3], [top + h, 0.8, 0.9, 0.5]], seed + 120, 6, 0.3)
    fan(moss, crag[2], [sc[0] + 0.9, sc[1] + 0.5, top + h + 0.8], true)
  }
  return { c, lean: [lx, ly] as XY, under: lower[1], belly: lower[2], brow }
}

/** A round tube along a path; ends left open sit in the ground or a rock. */
function tube(p: Part, path: V3[], radii: number[], sides = 4) {
  let prevU: V3 | null = null
  const rings = path.map((c, i) => {
    const axis = unit(sub(path[Math.min(i + 1, path.length - 1)], path[Math.max(0, i - 1)]))
    let u = unit(cross(Math.abs(axis[2]) > 0.95 ? [1, 0, 0] : [0, 0, 1], axis))
    if (prevU && u[0] * prevU[0] + u[1] * prevU[1] + u[2] * prevU[2] < 0) u = scale(u, -1)
    prevU = u
    const v = cross(axis, u)
    return Array.from({ length: sides }, (_, j) => {
      const a = (j / sides) * TAU + Math.PI / sides
      return plus(c, plus(scale(u, radii[i] * Math.cos(a)), scale(v, radii[i] * Math.sin(a))))
    })
  })
  p.loft(rings)
}
/**
 * A vine hanging between two points under its own weight: a catenary-like
 * sag below the chord, deepest a little nearer the lower end.
 */
function hang(a: V3, b: V3, sag: number, r: number, n = 6) {
  const path = Array.from({ length: n + 1 }, (_, i): V3 => {
    const t = i / n
    return [a[0] + (b[0] - a[0]) * t, a[1] + (b[1] - a[1]) * t, a[2] + (b[2] - a[2]) * t - sag * 4 * t * (1 - t)]
  })
  smooth(vine, (p) => tube(p, path, path.map((_, i) => r * (1 - 0.25 * (i / n))), 4), 70)
}

// --------------------------------------------------------------- build

const L = (p: XY): XY => [p[0] - AX, p[1] - AY]
const built = ROCKS.map((r, k) => {
  const { c } = centroid(r.outline)
  const lc = L(c)
  const plan = planOf(r.outline.map(L), lc)
  return { ...rock(lc, (a) => plan(a) * 1.02, r.keel, r.top, k + 1), plan, spec: r }
})

// The tangle that holds them up: from under each rock, many vines run down
// at angles to roots spread round it, sagging as they go. None is vertical,
// and together they read as the rock's support.
built.forEach((b, k) => {
  const H = b.spec.top - b.spec.keel
  const n = H > 15 ? 11 : H > 10 ? 8 : 6
  // The vines gather into three bundles, each rooted in one patch of ground
  // to one side, so they sweep down in ropes rather than splaying like legs.
  const roots = [0, 1, 2].map((m): V3 => {
    const a = hash(k * 53) * TAU + (m / 3) * TAU + (hash(k * 59 + m) - 0.5) * 0.8
    const out = 0.32 * b.spec.keel + 3 + hash(k * 61 + m) * 3
    return [b.c[0] + Math.cos(a) * (b.plan(a) * 0.6 + out), b.c[1] + Math.sin(a) * (b.plan(a) * 0.6 + out), -0.5]
  })
  for (let v = 0; v < n; v++) {
    const root = roots[v % 3]
    const ra = Math.atan2(root[1] - b.c[1], root[0] - b.c[0])
    // Leave from the side of the keel facing its root.
    let best = 0, bd = -Infinity
    const ring = v % 2 ? b.belly : b.under
    ring.forEach((p, i) => { const d = (p[0] - b.c[0]) * Math.cos(ra) + (p[1] - b.c[1]) * Math.sin(ra) + hash(k * 71 + v * 3 + i) * 3; if (d > bd) { bd = d; best = i } })
    const from = ring[best]
    const to: V3 = [root[0] + (hash(k * 29 + v) - 0.5) * 3, root[1] + (hash(k * 31 + v) - 0.5) * 3, -0.5]
    hang(from, to, 1 + hash(k * 37 + v) * 2.5, v < 3 ? 0.65 : 0.45)
  }
  // Vines draped over the brow and down the rock's face, bowing out from it
  // and tucking back under the keel: they cling, never hang straight.
  for (let v = 0; v < (H > 15 ? 4 : 2); v++) {
    const i = Math.floor(hash(k * 37 + v) * b.brow.length)
    const at = b.brow[i], low = b.under[Math.floor((i / b.brow.length) * b.under.length) % b.under.length]
    const o = unit([at[0] - b.c[0], at[1] - b.c[1], 0])
    const bow: V3 = [(at[0] + low[0]) / 2 + o[0] * 3, (at[1] + low[1]) / 2 + o[1] * 3, (at[2] + low[2]) / 2]
    const path = Array.from({ length: 6 }, (_, n): V3 => {
      const t = n / 5, u = 1 - t
      return [u * u * at[0] + 2 * u * t * bow[0] + t * t * low[0], u * u * at[1] + 2 * u * t * bow[1] + t * t * low[1], u * u * at[2] + 2 * u * t * bow[2] + t * t * low[2]]
    })
    smooth(vine, (p) => tube(p, path, path.map(() => 0.4), 4), 70)
  }
})
// Vines swooping from rock to rock in catenaries, several per pair.
const pairs: [number, number][] = [[1, 2], [2, 3], [3, 4], [0, 1], [0, 3], [1, 3]]
for (const [i, j] of pairs) {
  const A = built[i], B = built[j]
  for (let v = 0; v < 3; v++) {
    const pa = A.brow[Math.floor(hash(i * 41 + j * 7 + v) * A.brow.length)]
    const pb = B.brow[Math.floor(hash(j * 43 + i * 5 + v) * B.brow.length)]
    const d = Math.hypot(pa[0] - pb[0], pa[1] - pb[1])
    hang(pa, pb, d * (0.25 + 0.15 * v), 0.4, 7)
  }
}
// Two small rocks floating higher, slung on vines from their neighbours.
for (const [x, y, z, s, k, to] of [[48, 22, 35, 3.4, 21, 2], [70, -10, 30, 2.8, 22, 0]] as [number, number, number, number, number, number][]) {
  const c = L([x, y])
  const r = rock(c, () => s, z, z + s * 2, k)
  for (let v = 0; v < 3; v++) {
    const a = (v / 3) * TAU + k
    hang(r.under[v * 2 % r.under.length], [c[0] + Math.cos(a) * 12, c[1] + Math.sin(a) * 12, -0.5], 2, 0.38)
  }
  const nb = built[to]
  hang(r.brow[0], nb.brow[3], 5, 0.38)
}

// Waterfalls: thin pale strips falling from the brows of the two largest
// rocks into the valley, as in photos 03 and 10.
const water = new Part()
for (const [k, i] of [[0, 2], [0, 5], [2, 1]] as [number, number][]) {
  const b = built[k], at = b.brow[i]
  const o = unit([at[0] - b.c[0], at[1] - b.c[1], 0]), side: V3 = [-o[1], o[0], 0]
  const w = 0.9, top: V3 = [at[0] + o[0] * 0.9, at[1] + o[1] * 0.9, at[2] - 0.5]
  const foot: V3 = [top[0] + o[0] * 2.5, top[1] + o[1] * 2.5, 0]
  const q = [plus(top, scale(side, -w)), plus(top, scale(side, w)), plus(foot, scale(side, w * 1.6)), plus(foot, scale(side, -w * 1.6))] as V3[]
  water.quad(q[3], q[2], q[1], q[0])
  water.quad(q[0], q[1], q[2], q[3])
}

// ------------------------------------------------------------------ output

// Identity finishes from the photos, at the palette's lightness: the ochre
// rock, its grey-violet lower strata, the moss and plants on the tops, the
// dark vines (no darker than the charcoal limit), and the waterfalls' pale
// blue-white.
const parts = [
  { part: rockTan, material: finish('pandora-rock', 0xc2a07f) },
  { part: rockGrey, material: finish('pandora-rock-grey', 0xa69a99) },
  { part: moss, material: finish('pandora-moss', 0x7f9e5e) },
  { part: vine, material: finish('pandora-vine', 0x75604f) },
  { part: water, material: finish('pandora-waterfall', 0xcfe4ee) },
]
const triangles = parts.reduce((sum, { part }) => sum + part.triangles, 0)
console.log(parts.map(({ part, material }) => `${material.name} ${part.triangles}`).join(', '))
if (triangles > 6500) throw new Error(`Triangle budget exceeded: ${triangles}`)
const glb = writeGlb("Pandora's floating mountains", parts, {
  license: 'CC0-1.0', frame: 'Y up, -Z north, +X east, metres; origin at ground anchor',
  bearing: 0, elevation: 0, height: 42,
})
if (glb.length > 256000) throw new Error(`File budget exceeded: ${glb.length}`)
const out = new URL('../../landmarks/models/wdw-pandora-floating-mountains.glb', import.meta.url).pathname
await Bun.write(out, glb)
console.log(`${out}: ${triangles} triangles, ${glb.length} bytes`)
console.log(`anchor ${(LON0 + AX / MX).toFixed(7)}, ${(LAT0 + AY / MY).toFixed(7)}`)
