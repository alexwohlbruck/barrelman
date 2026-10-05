/**
 * New York Hall of Science, Flushing Meadows–Corona Park — original
 * procedural geometry, CC0-1.0.
 * bun scripts/landmarks/new-york-hall-of-science.ts
 *
 * Map frame: x east, y north, z up, metres. Bearing 0, so x/y are true
 * east/north. Origin is the centroid of OSM way 284860788 (40.7472904,
 * -73.8516846); that single outline (31.5 m, no building:parts) is the
 * whole complex, so the model covers all of it:
 *
 * - the 1964 Great Hall (Harrison & Abramovitz): an undulating, lobed
 *   concrete drum, 31.5 m tall, its walls a grid of cobalt dalle-de-verre
 *   panels, drawn as a few broad rows of recessed blue cells;
 * - the later wings, plain: a low brick podium over the rest of the
 *   outline, the brick entrance rotunda on 111th Street and the sloped
 *   2004 north wing;
 * - Rocket Park to the north-west: the Mercury-Atlas and Gemini-Titan II,
 *   at their OSM positions, as simple white rockets.
 *
 * The Great Hall's plan (about 29 x 42 m) is taken from aerial imagery,
 * registered to OSM on the entrance rotunda.
 */
import { Part, writeGlb, type V3 } from './mesh'

type XY = [number, number]
const concrete = new Part(), cobalt = new Part(), brick = new Part(), roof = new Part()
const metal = new Part(), glass = new Part(), white = new Part(), dark = new Part()
const up: V3 = [0, 0, 1]
const unit = (v: V3): V3 => { const l = Math.hypot(...v) || 1; return v.map(n => n / l) as V3 }

function quad(p: Part, a: V3, b: V3, c: V3, d: V3, na?: V3, nb?: V3, nc?: V3, nd?: V3) {
  if (!na) { p.quad(a, b, c, d); return }
  p.tri(a, b, c, undefined, undefined, undefined, [na, nb!, nc!])
  p.tri(a, c, d, undefined, undefined, undefined, [na, nc!, nd!])
}

// ---------------------------------------------------------------- outline
// OSM way 284860788, counter-clockwise, relative to its centroid.
const OUTLINE: XY[] = [[-17.0,86.6],[-33.5,80.4],[-9.3,23.0],[-13.7,17.4],[-15.2,18.6],[-28.5,1.7],[-27.1,0.6],[-30.3,-3.4],[-31.2,-2.7],[-33.1,-1.4],[-35.2,-0.4],[-37.4,0.3],[-39.6,0.8],[-40.3,-2.8],[-41.9,-2.7],[-42.7,-2.8],[-44.3,-3.1],[-45.8,-3.7],[-44.2,-6.7],[-44.9,-6.9],[-46.2,-7.6],[-47.4,-8.5],[-48.4,-9.6],[-49.2,-10.9],[-49.8,-12.2],[-50.1,-13.7],[-50.1,-14.4],[-50.1,-15.9],[-49.9,-16.6],[-49.7,-17.3],[-49.2,-18.7],[-48.4,-20.0],[-47.9,-20.5],[-47.4,-21.0],[-46.2,-22.0],[-51.2,-27.8],[-43.0,-47.2],[-38.2,-47.9],[-37.8,-48.6],[-36.8,-50.1],[-35.6,-51.4],[-34.9,-51.9],[-33.5,-52.9],[-31.9,-53.6],[-30.2,-54.1],[-28.5,-54.3],[-26.7,-54.2],[-26.0,-54.0],[-24.5,-53.4],[-23.2,-52.6],[-22.1,-51.5],[-21.1,-50.2],[29.3,-57.2],[30.3,-51.3],[49.5,-26.8],[23.8,34.6],[24.7,35.0],[26.2,36.1],[27.6,37.3],[28.8,38.8],[29.3,39.5],[29.7,40.5],[30.4,42.3],[30.8,44.3],[30.9,45.3],[31.0,47.3],[30.9,48.3],[30.6,50.3],[30.4,51.2],[29.4,54.0],[28.5,55.8],[27.4,57.4],[26.1,58.9],[25.4,59.6],[23.8,60.8],[23.0,61.3],[21.2,62.2],[19.7,62.8],[18.0,63.1],[17.2,63.2],[15.5,63.2],[13.9,62.9],[13.0,62.7],[12.3,62.4],[10.7,61.7],[9.4,60.8],[8.7,60.2],[7.6,59.0],[6.6,57.6],[5.9,56.2],[5.6,55.4],[5.3,54.6],[5.1,52.9],[4.2,53.0],[-1.9,70.0],[-9.5,68.3]]

/** Douglas–Peucker on a closed ring: drops the survey jitter, keeps the shape. */
function simplify(ring: XY[], tol: number): XY[] {
  const keep = new Array(ring.length).fill(false)
  const rec = (i: number, j: number) => {
    const a = ring[i], b = ring[j % ring.length]
    const l = Math.hypot(b[0] - a[0], b[1] - a[1]) || 1
    let best = -1, at = -1
    for (let k = i + 1; k < j; k++) {
      const p = ring[k]
      const d = Math.abs((b[0] - a[0]) * (a[1] - p[1]) - (a[0] - p[0]) * (b[1] - a[1])) / l
      if (d > best) { best = d; at = k }
    }
    if (best > tol) { keep[at] = true; rec(i, at); rec(at, j) }
  }
  keep[0] = true
  const far = ring.reduce((m, p, k) => Math.hypot(p[0] - ring[0][0], p[1] - ring[0][1]) > Math.hypot(ring[m][0] - ring[0][0], ring[m][1] - ring[0][1]) ? k : m, 0)
  keep[far] = true
  rec(0, far); rec(far, ring.length)
  return ring.filter((_, k) => keep[k])
}

/** Ear clipping for a simple counter-clockwise ring. */
function triangulate(ring: XY[]): [number, number, number][] {
  const idx = ring.map((_, i) => i), out: [number, number, number][] = []
  const crossz = (a: XY, b: XY, c: XY) => (b[0] - a[0]) * (c[1] - a[1]) - (b[1] - a[1]) * (c[0] - a[0])
  const inTri = (p: XY, a: XY, b: XY, c: XY) => crossz(a, b, p) >= 0 && crossz(b, c, p) >= 0 && crossz(c, a, p) >= 0
  let guard = 0
  while (idx.length > 3 && guard++ < 10000) {
    let clipped = false
    for (let k = 0; k < idx.length; k++) {
      const i = idx[(k + idx.length - 1) % idx.length], j = idx[k], l = idx[(k + 1) % idx.length]
      const a = ring[i], b = ring[j], c = ring[l]
      if (crossz(a, b, c) <= 1e-9) continue
      if (idx.some(m => m !== i && m !== j && m !== l && inTri(ring[m], a, b, c))) continue
      out.push([i, j, l]); idx.splice(k, 1); clipped = true; break
    }
    if (!clipped) throw new Error('triangulation failed')
  }
  out.push([idx[0], idx[1], idx[2]])
  return out
}

/** Offset a ring inward along each corner's bisector. */
function inset(ring: XY[], d: number): XY[] {
  return ring.map((b, i) => {
    const a = ring[(i + ring.length - 1) % ring.length], c = ring[(i + 1) % ring.length]
    const u = unit([b[1] - a[1], a[0] - b[0], 0]), v = unit([c[1] - b[1], b[0] - c[0], 0])
    const k = 1 + u[0] * v[0] + u[1] * v[1]
    return [b[0] - (u[0] + v[0]) * d / k, b[1] - (u[1] + v[1]) * d / k]
  })
}

/** Straight-walled block with a chamfered coping and a flat roof. */
function block(ring: XY[], z0: number, z1: number, side: Part, top: Part, bevel = 0.45) {
  const n = ring.length, inner = inset(ring, bevel), zc = z1 - bevel
  for (let i = 0; i < n; i++) {
    const j = (i + 1) % n, a = ring[i], b = ring[j]
    const nrm = unit([b[1] - a[1], a[0] - b[0], 0]), slope = unit([nrm[0], nrm[1], 1])
    quad(side, [...a, z0], [...b, z0], [...b, zc], [...a, zc])
    quad(side, [...a, zc], [...b, zc], [...inner[j], z1], [...inner[i], z1], nrm, nrm, slope, slope)
  }
  for (const [i, j, k] of triangulate(inner)) top.tri([...inner[i], z1], [...inner[j], z1], [...inner[k], z1])
}

// ---------------------------------------------------------------- podium
// The low later wings, plain brick, at the height of the side wings.
const PODIUM = 9
block(simplify(OUTLINE, 0.35), 0, PODIUM, brick, roof)

// ---------------------------------------------------------------- rotunda
// The entrance drum on 111th Street: brick, a glazed clerestory, a coping.
function circumcircle(a: XY, b: XY, c: XY): [number, number, number] {
  const d = 2 * (a[0] * (b[1] - c[1]) + b[0] * (c[1] - a[1]) + c[0] * (a[1] - b[1]))
  const s = (p: XY) => p[0] * p[0] + p[1] * p[1]
  const x = (s(a) * (b[1] - c[1]) + s(b) * (c[1] - a[1]) + s(c) * (a[1] - b[1])) / d
  const y = (s(a) * (c[0] - b[0]) + s(b) * (a[0] - c[0]) + s(c) * (b[0] - a[0])) / d
  return [x, y, Math.hypot(a[0] - x, a[1] - y)]
}
const [RX, RY, RR] = circumcircle([5.3, 54.6], [17.2, 63.2], [31.0, 47.3])
function ring(cx: number, cy: number, r: number, z: number, n = 24): V3[] {
  return Array.from({ length: n }, (_, i) => { const a = (i / n) * Math.PI * 2; return [cx + r * Math.cos(a), cy + r * Math.sin(a), z] })
}
/** A smooth-shaded vertical band of a drum. */
function drumBand(p: Part, cx: number, cy: number, r0: number, r1: number, z0: number, z1: number, n = 24, nz = 0) {
  const a = ring(cx, cy, r0, z0, n), b = ring(cx, cy, r1, z1, n)
  for (let i = 0; i < n; i++) {
    const j = (i + 1) % n
    const ni = unit([a[i][0] - cx, a[i][1] - cy, nz]), nj = unit([a[j][0] - cx, a[j][1] - cy, nz])
    quad(p, a[i], a[j], b[j], b[i], ni, nj, nj, ni)
  }
}
const DRUM = 13, CLERESTORY = 15.6
drumBand(brick, RX, RY, RR, RR, 0, DRUM)
drumBand(brick, RX, RY, RR, RR - 0.6, DRUM, DRUM + 0.01, 24, 3) // ledge under the glazing
drumBand(glass, RX, RY, RR - 0.6, RR - 0.6, DRUM, CLERESTORY - 0.4)
drumBand(metal, RX, RY, RR - 0.6, RR - 0.6, CLERESTORY - 0.4, CLERESTORY - 0.1)
drumBand(metal, RX, RY, RR - 0.6, RR - 1.0, CLERESTORY - 0.1, CLERESTORY, 24, 1)
roof.cap(ring(RX, RY, RR - 1.0, CLERESTORY), true)

// ---------------------------------------------------------------- north wing
// The 2004 Polshek wing: a long, plain, pale-metal block whose roof rises
// toward its north end. Its west and east faces are the OSM outline's.
const W_SW: XY = [-13.9, 34.0], W_SE: XY = [2.3, 40.8]
const W_NW: XY = [-33.5, 80.4], W_NE: XY = [-17.0, 86.6]
const LOW = 11, HIGH = 19
{
  // Pushed a hair outside the outline so the wing, not the podium's brick,
  // owns the faces the two share.
  const [sw, se, ne, nw] = inset([W_SW, W_SE, W_NE, W_NW], -0.06)
  const pts: [XY, number][] = [[sw, LOW], [se, LOW], [ne, HIGH], [nw, HIGH]]
  for (let i = 0; i < 4; i++) {
    const [a, ha] = pts[i], [b, hb] = pts[(i + 1) % 4]
    quad(metal, [...a, 0], [...b, 0], [...b, hb - 0.4], [...a, ha - 0.4])
  }
  // A slim chamfered coping, then the sloped roof.
  const inner = inset([sw, se, ne, nw], 0.46)
  const hs = [LOW, LOW, HIGH, HIGH]
  for (let i = 0; i < 4; i++) {
    const j = (i + 1) % 4, a = pts[i][0], b = pts[j][0]
    quad(metal, [...a, hs[i] - 0.4], [...b, hs[j] - 0.4], [...inner[j], hs[j]], [...inner[i], hs[i]])
  }
  quad(metal, [...inner[0], LOW], [...inner[1], LOW], [...inner[2], HIGH], [...inner[3], HIGH])
  // A tall glazed slot near the high end, as on the real wing's flank.
  const along = (t: number, z: number, out = 0.05): V3 => {
    const x = W_SE[0] + (W_NE[0] - W_SE[0]) * t, y = W_SE[1] + (W_NE[1] - W_SE[1]) * t
    const e = unit([W_NE[1] - W_SE[1], -(W_NE[0] - W_SE[0]), 0])
    return [x + e[0] * (out + 0.06), y + e[1] * (out + 0.06), z]
  }
  quad(glass, along(0.12, PODIUM + 0.6), along(0.3, PODIUM + 0.6), along(0.3, LOW + 1.0), along(0.12, LOW + 0.3))
}

// ---------------------------------------------------------------- great hall
// The plan: an ellipse whose radius ripples in seven lobes, resampled by arc
// length so every bay of the wall is the same width.
const GX = 8.5, GY = -14.8, GA = 12.4, GB = 18.6, LOBES = 7, AMP = 2.3, PHASE = 0.4
const H = 31.5
const raw = (t: number): XY => {
  const a = t * Math.PI * 2, r = AMP * Math.sin(LOBES * a + PHASE)
  return [GX + (GA + r) * Math.cos(a), GY + (GB + r) * Math.sin(a)]
}
const DENSE = 2000
const dense = Array.from({ length: DENSE + 1 }, (_, i) => raw(i / DENSE))
const cum = [0]
for (let i = 1; i <= DENSE; i++) cum.push(cum[i - 1] + Math.hypot(dense[i][0] - dense[i - 1][0], dense[i][1] - dense[i - 1][1]))
const PERIM = cum[DENSE]
/** Point on the hall's plan at arc-length fraction s, and its outward normal. */
function hall(s: number, offset = 0): { p: V3; n: V3 } {
  s = ((s % 1) + 1) % 1
  const target = s * PERIM
  let lo = 0, hi = DENSE
  while (hi - lo > 1) { const m = (lo + hi) >> 1; if (cum[m] < target) lo = m; else hi = m }
  const f = (target - cum[lo]) / (cum[hi] - cum[lo] || 1)
  const t = (lo + f) / DENSE, e = 1e-4
  const p = raw(t), a = raw(t - e), b = raw(t + e)
  const n = unit([b[1] - a[1], -(b[0] - a[0]), 0])
  return { p: [p[0] - n[0] * offset, p[1] - n[1] * offset, 0], n }
}
const at = (s: number, z: number, offset = 0): V3 => { const { p } = hall(s, offset); return [p[0], p[1], z] }
const nAt = (s: number) => hall(s).n

// Bays round the wall, rows up it. Cells are broad on purpose: the real grid
// is far finer, but at map distance it reads as a band of blue.
const COLS = 26, SUB = 4 // plan segments per bay: rib, cell, cell, rib
const RECESS = 0.6, RIB = 0.13 // rib is a fraction of the bay width, each side
const ROWS: [number, number][] = [[13.4, 17.6], [18.3, 22.5], [23.2, 27.4]]
const BASE = PODIUM - 1, COPING = H - 0.5

function wallStrip(s0: number, s1: number, z0: number, z1: number, segs: number, p = concrete) {
  for (let k = 0; k < segs; k++) {
    const a = s0 + (s1 - s0) * k / segs, b = s0 + (s1 - s0) * (k + 1) / segs
    const na = nAt(a), nb = nAt(b)
    quad(p, at(a, z0), at(b, z0), at(b, z1), at(a, z1), na, nb, nb, na)
  }
}
for (let c = 0; c < COLS; c++) {
  const s0 = c / COLS, s3 = (c + 1) / COLS, w = s3 - s0
  const s1 = s0 + w * RIB, s2 = s3 - w * RIB, sm = (s1 + s2) / 2
  const cellS = [s1, sm, s2]
  // Horizontal concrete between the rows, and the plain wall below and above.
  const bands: [number, number][] = [[BASE, ROWS[0][0]], ...ROWS.slice(1).map((r, i): [number, number] => [ROWS[i][1], r[0]]), [ROWS.at(-1)![1], COPING]]
  for (const [z0, z1] of bands) wallStrip(s0, s3, z0, z1, SUB)
  for (const [z0, z1] of ROWS) {
    wallStrip(s0, s1, z0, z1, 1)
    wallStrip(s2, s3, z0, z1, 1)
    // The recessed cell: blue back, concrete reveals.
    for (let k = 0; k < 2; k++) {
      const a = cellS[k], b = cellS[k + 1], na = nAt(a), nb = nAt(b)
      quad(cobalt, at(a, z0, RECESS), at(b, z0, RECESS), at(b, z1, RECESS), at(a, z1, RECESS), na, nb, nb, na)
      quad(concrete, at(a, z0), at(b, z0), at(b, z0, RECESS), at(a, z0, RECESS)) // sill, faces up
      quad(concrete, at(b, z1), at(a, z1), at(a, z1, RECESS), at(b, z1, RECESS)) // head, faces down
    }
    quad(concrete, at(s1, z0, RECESS), at(s1, z1, RECESS), at(s1, z1), at(s1, z0)) // left jamb
    quad(concrete, at(s2, z0), at(s2, z1), at(s2, z1, RECESS), at(s2, z0, RECESS)) // right jamb
  }
}
// Rounded coping and a flat roof.
{
  const N = COLS * SUB
  const rim: V3[] = [], mid: V3[] = [], inner: XY[] = []
  for (let i = 0; i < N; i++) {
    const s = i / N
    rim.push(at(s, COPING)); mid.push(at(s, H - 0.15, 0.3))
    const q = at(s, H, 0.6); inner.push([q[0], q[1]])
  }
  for (let i = 0; i < N; i++) {
    const j = (i + 1) % N, ni = nAt(i / N), nj = nAt(j / N)
    const si = unit([ni[0], ni[1], 1.2]), sj = unit([nj[0], nj[1], 1.2])
    quad(concrete, rim[i], rim[j], mid[j], mid[i], ni, nj, sj, si)
    quad(concrete, mid[i], mid[j], [...inner[j], H], [...inner[i], H], si, sj, up, up)
  }
  for (const [i, j, k] of triangulate(inner)) roof.tri([...inner[i], H], [...inner[j], H], [...inner[k], H])
}

// ---------------------------------------------------------------- rockets
/** A smooth lathe from a [z, r] profile, bottom to top, closed at both ends. */
function lathe(p: Part, cx: number, cy: number, profile: [number, number][], n = 12) {
  for (let k = 0; k < profile.length - 1; k++) {
    const [z0, r0] = profile[k], [z1, r1] = profile[k + 1]
    if (r0 === 0 && r1 === 0) continue
    const slope = (r0 - r1) / Math.max(z1 - z0, 1e-6)
    for (let i = 0; i < n; i++) {
      const a0 = (i / n) * Math.PI * 2, a1 = ((i + 1) / n) * Math.PI * 2
      const pt = (a: number, z: number, r: number): V3 => [cx + r * Math.cos(a), cy + r * Math.sin(a), z]
      const nr = (a: number) => unit([Math.cos(a), Math.sin(a), slope])
      if (r1 === 0) p.tri(pt(a0, z0, r0), pt(a1, z0, r0), [cx, cy, z1], undefined, undefined, undefined, [nr(a0), nr(a1), nr((a0 + a1) / 2)])
      else quad(p, pt(a0, z0, r0), pt(a1, z0, r0), pt(a1, z1, r1), pt(a0, z1, r1), nr(a0), nr(a1), nr(a1), nr(a0))
    }
  }
  const [zb, rb] = profile[0]
  if (rb > 0) p.cap(ring(cx, cy, rb, zb, n), false)
  const [zt, rt] = profile.at(-1)!
  if (rt > 0) p.cap(ring(cx, cy, rt, zt, n), true)
}
// Mercury-Atlas, 29 m: a stand, the flared booster skirt, the long body
// tapering to the capsule, and the escape tower.
const ATLAS: XY = [-58.8, 66.5]
lathe(dark, ...ATLAS, [[0, 1.0], [3.0, 1.0]], 12)
lathe(white, ...ATLAS, [[3.0, 2.3], [6.0, 1.75], [7.0, 1.55], [18.0, 1.55], [22.6, 0.95]], 12)
lathe(dark, ...ATLAS, [[22.6, 0.95], [24.8, 0.45]], 12)
lathe(white, ...ATLAS, [[24.8, 0.22], [28.9, 0.1], [29.2, 0]], 8)
// Gemini-Titan II, 33 m: a straight two-stage body and the Gemini capsule.
const TITAN: XY = [-50.7, 94.4]
lathe(dark, ...TITAN, [[0, 1.0], [2.5, 1.0]], 12)
lathe(white, ...TITAN, [[2.5, 1.6], [28.6, 1.55], [30.0, 1.25]], 12)
lathe(dark, ...TITAN, [[30.0, 1.25], [32.6, 0.5], [33.2, 0]], 12)

// ---------------------------------------------------------------- write
const parts = [
  { part: concrete, material: { name: 'great-hall-concrete', color: 0xc6bcac } },
  { part: cobalt, material: { name: 'dalle-de-verre-cobalt', color: 0x2f4c9c, roughness: 0.6 } },
  { part: brick, material: { name: 'tan-brick', color: 0xcfb48e } },
  { part: metal, material: { name: 'pale-metal', color: 0xd3d4d0, roughness: 0.6 } },
  { part: glass, material: { name: 'glazing', color: 0x7f939d } },
  { part: roof, material: { name: 'roofs', color: 0xbdb9b1 } },
  { part: white, material: { name: 'rocket-white', color: 0xeeeeea, roughness: 0.5 } },
  { part: dark, material: { name: 'rocket-dark', color: 0x4a4d52 } },
]
const triangles = parts.reduce((n, { part }) => n + part.triangles, 0)
if (triangles > 6500) throw new Error(`Triangle budget exceeded: ${triangles}`)
const glb = writeGlb('New York Hall of Science', parts, {
  license: 'CC0-1.0', bearing: 0, elevation: 0, anchor: [40.7472904, -73.8516846], height: H,
  frame: 'Y up, -Z north, +X east, metres; origin at the ground anchor',
  replaces: ['way/284860788'],
  note: 'Lobed Great Hall with broad recessed cobalt cells; plain brick podium, rotunda and north wing; Rocket Park Atlas and Titan II',
})
if (glb.length > 256000) throw new Error(`File budget exceeded: ${glb.length}`)
const out = new URL('../../landmarks/models/new-york-hall-of-science.glb', import.meta.url).pathname
await Bun.write(out, glb)
console.log(`${out}: ${triangles} triangles, ${glb.length} bytes (${(glb.length / 1024).toFixed(1)} KiB)`)
