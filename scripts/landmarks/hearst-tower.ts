/**
 * Hearst Tower — original procedural geometry, CC0-1.0.
 * bun scripts/landmarks/hearst-tower.ts
 *
 * Map frame turned to the Manhattan grid (bearing 29°): x runs along 57th
 * Street (+x toward 8th Avenue), y along 8th Avenue (+y toward 57th Street),
 * z up, metres. Anchor is the centroid of the base outline (way/265151748),
 * 40.7665639,-73.9836231. The ground falls about 1.3 m from 57th Street down
 * to 56th Street (USGS 23.5 → 22.2 m), so z = 0 is the 56th Street ground and
 * the plain 1.6 m plinth takes up the difference on the 57th Street side.
 *
 * Two parts, both from OSM:
 *  - the 1928 cast-stone base, six storeys to 30 m, on the outline's own
 *    footprint, with the tall pylons that carry its statues: one on each
 *    chamfered 8th Avenue corner and a pair framing the middle of each street
 *    front;
 *  - the 2006 tower (way/147448071, 51.2 × 39.3 m, 182 m), set into it.
 *
 * The tower is the diagrid and nothing else. Nine tiers, each four storeys,
 * of big triangles: broad silver frames standing proud of recessed dark glass,
 * the same construction as a stone facade's window bands. The tier levels
 * alternate between the full rectangle and a plan with every corner cut back
 * by half a bay; between them each corner is a tilted triangular facet, the
 * "bird's mouth". Because the cut is exactly half a bay, every face stays one
 * flat plane of whole triangles. The bottom level is a cut one and the roof a
 * full rectangle, as in photos; beneath the diagrid, the lobby storeys are
 * recessed glass on silver columns that stand under the bottom nodes.
 */
import { Part, writeGlb, type V3 } from './mesh'

type XY = [number, number]
const stone = new Part(), windows = new Part(), glass = new Part()
const silver = new Part(), roof = new Part(), skylight = new Part()

const unit = (v: V3): V3 => { const l = Math.hypot(...v) || 1; return [v[0] / l, v[1] / l, v[2] / l] }
const add = (a: V3, b: V3, k = 1): V3 => [a[0] + b[0] * k, a[1] + b[1] * k, a[2] + b[2] * k]
const sub = (a: V3, b: V3): V3 => [a[0] - b[0], a[1] - b[1], a[2] - b[2]]
const dot = (a: V3, b: V3) => a[0] * b[0] + a[1] * b[1] + a[2] * b[2]
const cross = (a: V3, b: V3): V3 => [a[1] * b[2] - a[2] * b[1], a[2] * b[0] - a[0] * b[2], a[0] * b[1] - a[1] * b[0]]
const at = (ring: XY[], z: number): V3[] => ring.map(([x, y]) => [x, y, z])

function quadN(p: Part, a: V3, b: V3, c: V3, d: V3, na: V3, nb: V3, nc = nb, nd = na) {
  p.tri(a, b, c, undefined, undefined, undefined, [na, nb, nc])
  p.tri(a, c, d, undefined, undefined, undefined, [na, nc, nd])
}
function fan(p: Part, poly: V3[]) {
  for (let i = 1; i < poly.length - 1; i++) p.tri(poly[0], poly[i], poly[i + 1])
}

/** Offset a counter-clockwise ring inward (d > 0) or outward (d < 0), mitred. */
function inset(ring: XY[], d: number): XY[] {
  return ring.map((b, i) => {
    const a = ring[(i + ring.length - 1) % ring.length], c = ring[(i + 1) % ring.length]
    const u = unit([b[1] - a[1], a[0] - b[0], 0]), v = unit([c[1] - b[1], b[0] - c[0], 0])
    const k = 1 + u[0] * v[0] + u[1] * v[1]
    return [b[0] - (u[0] + v[0]) * d / k, b[1] - (u[1] + v[1]) * d / k]
  })
}

/** Keep the part of a convex polygon where f ≥ 0 (Sutherland–Hodgman). */
function clip(poly: XY[], f: (p: XY) => number): XY[] {
  const out: XY[] = []
  for (let i = 0; i < poly.length; i++) {
    const p = poly[i], q = poly[(i + 1) % poly.length], fp = f(p), fq = f(q)
    if (fp >= 0) out.push(p)
    if ((fp >= 0) !== (fq >= 0)) {
      const t = fp / (fp - fq)
      out.push([p[0] + (q[0] - p[0]) * t, p[1] + (q[1] - p[1]) * t])
    }
  }
  return out
}

// ---- The 1928 base -----------------------------------------------------------
// The OSM outline, squared to the grid: a 1.4 m step on 56th Street, and the
// two chamfered corners on 8th Avenue. The west side is a party wall with the
// Sheffield and stays plain.
const BASE: XY[] = [
  [-29.3, -30.45], [4.47, -30.45], [4.47, -29.06], [26.26, -29.06],
  [29.85, -25.77], [29.85, 25.49], [26.42, 29.9], [-29.3, 29.9],
]
const PARAPET = 30
const PYLON = { w: 3.4, out: 1.3, z0: 9.6, z1: 36.6 }
// Pylon centres along each edge of BASE, measured from the edge's start.
const pylons: Record<number, number[]> = {
  0: [29.3 - 8, 29.3 + 1], // 56th Street: a pair on the long west segment
  3: [], 4: [], 6: [],
}
{
  const len = (i: number) => { const a = BASE[i], b = BASE[(i + 1) % BASE.length]; return Math.hypot(b[0] - a[0], b[1] - a[1]) }
  // 8th Avenue: a pair framing the middle, and one on each chamfered corner.
  pylons[4] = [len(4) / 2 - 5.35, len(4) / 2 + 5.35]
  pylons[3] = [len(3) / 2]
  pylons[5] = [len(5) / 2]
  // 57th Street runs east to west in this ring; its middle is x = -1.4.
  pylons[6] = [26.42 - (-1.4) - 5.5, 26.42 - (-1.4) + 5.5]
}
const PLAIN = new Set([7]) // the party wall

/** Storey zones of the cast-stone fronts, [bottom, top]; the attic stays plain. */
const ZONES: [number, number][] = [[1.6, 8.6], [10.2, 24.2]]
const RECESS = .6, BEVEL = .4

function baseFront(i: number) {
  const a = BASE[i], b = BASE[(i + 1) % BASE.length]
  const length = Math.hypot(b[0] - a[0], b[1] - a[1]), ux = (b[0] - a[0]) / length, uy = (b[1] - a[1]) / length
  const n: V3 = [uy, -ux, 0]
  const v = (s: number, z: number, depth = 0): V3 => [a[0] + ux * s - uy * depth, a[1] + uy * s + ux * depth, z]
  const panel = (p: Part, s0: number, s1: number, z0: number, z1: number) => p.quad(v(s0, z0), v(s1, z0), v(s1, z1), v(s0, z1))
  // Free spans between pylons, where the window bands go.
  const blocked = (pylons[i] ?? []).map((c): XY => [c - PYLON.w / 2 - .5, c + PYLON.w / 2 + .5])
  const spans: XY[] = []
  let from = 0
  for (const [b0, b1] of blocked) { spans.push([from, b0]); from = b1 }
  spans.push([from, length])
  let zLast = 0
  for (const [zb, zt] of ZONES) {
    panel(stone, 0, length, zLast, zb)
    zLast = zt
    if (PLAIN.has(i)) { panel(stone, 0, length, zb, zt); continue }
    let last = 0
    for (const [s0, s1] of spans) {
      const span = s1 - s0, count = span < 3.2 ? 0 : Math.max(1, Math.round(span / 4.6))
      if (!count) continue
      const pitch = span / count, width = pitch * .56
      for (let k = 0; k < count; k++) {
        const w0 = s0 + pitch * (k + .5) - width / 2, w1 = w0 + width
        panel(stone, last, w0, zb, zt)
        // A recessed band with bevelled stone returns, as on the other models.
        const o: XY[] = [[w0, zb], [w1, zb], [w1, zt], [w0, zt]]
        const inner: XY[] = [[w0 + BEVEL, zb + BEVEL], [w1 - BEVEL, zb + BEVEL], [w1 - BEVEL, zt - BEVEL], [w0 + BEVEL, zt - BEVEL]]
        const back = inner.map(([s, z]) => v(s, z, RECESS))
        windows.quad(back[0], back[1], back[2], back[3])
        for (let e = 0; e < 4; e++) {
          const f = (e + 1) % 4
          const ds = o[f][0] - o[e][0], dz = o[f][1] - o[e][1], l = Math.hypot(ds, dz)
          const innerN = unit([n[0] * .4 - ux * dz / l, n[1] * .4 - uy * dz / l, ds / l])
          quadN(stone, v(...o[e]), v(...o[f]), back[f], back[e], n, n, innerN, innerN)
        }
        last = w1
      }
    }
    panel(stone, last, length, zb, zt)
  }
  panel(stone, 0, length, zLast, PARAPET - .5)

  // Pylons: tall bevelled piers carrying a statue group at their foot and a
  // tapered finial above the parapet.
  for (const c of pylons[i] ?? []) {
    const side = (sgn: number): V3 => [ux * sgn, uy * sgn, 0]
    // Left side, bevel, front, bevel, right side; the back stays in the wall.
    const nn: V3[] = [side(-1), side(-1), n, n, side(1), side(1)]
    const box = (hw: number, out: number, z0: number, z1: number, topHw = hw, topOut = out) => {
      const ring = (h: number, o: number, z: number) => {
        const r = Math.min(.45, h * .3)
        return [v(c - h, z, .3), v(c - h, z, -o + r), v(c - h + r, z, -o), v(c + h - r, z, -o), v(c + h, z, -o + r), v(c + h, z, .3)]
      }
      const lo = ring(hw, out, z0), hi = ring(topHw, topOut, z1)
      for (let k = 0; k < 5; k++) {
        if (k % 2 === 0) stone.quad(lo[k], lo[k + 1], hi[k + 1], hi[k])
        else quadN(stone, lo[k], lo[k + 1], hi[k + 1], hi[k], nn[k], nn[k + 1])
      }
      return hi
    }
    // Statue group: a broad block at the foot of the pier.
    const g = box(2.3, 1.7, PYLON.z0, 16.5)
    fan(stone, g)
    box(PYLON.w / 2, PYLON.out, 16.5, PYLON.z1)
    const top = box(PYLON.w / 2, PYLON.out, PYLON.z1, PYLON.z1 + 4, .6, .5)
    fan(stone, top)
  }
}
BASE.forEach((_, i) => baseFront(i))

// Two projecting stone bands: the balustrade over the ground storeys and the
// main cornice, with bevelled lips.
function band(z0: number, z1: number, out: number) {
  const r0 = at(BASE, z0), r1 = at(inset(BASE, -out), z0 + .3), r2 = at(inset(BASE, -out), z1 - .3), r3 = at(BASE, z1)
  stone.loft([r0, r1, r2, r3])
}
band(8.6, 10.2, .35)
band(24.2, 25.6, .45)

// Parapet coping, and the atrium's glass roof between it and the tower.
{
  const outer = at(BASE, PARAPET - .5), mid = at(inset(BASE, .4), PARAPET), inner = at(inset(BASE, .9), PARAPET - .4)
  stone.loft([outer, mid, inner])
}

// ---- The 2006 tower ----------------------------------------------------------
const TX = -1.92, TY = .07 // centre of way/147448071 in this frame
const W = 25.6, H = 19.65 // half the long (57th Street) and short sides
const Z0 = 42, TIERS = 9, TIER = (182 - Z0) / TIERS
const NL = 4, NS = 3 // bays on the long and short faces
const FRAME = 1.2, DEPTH = .8

type Face = { o: V3; u: V3; n: V3; L: number; bays: number }
// Counter-clockwise from above: south, east, north, west.
const faces: Face[] = [
  { o: [TX - W, TY - H, 0], u: [1, 0, 0], n: [0, -1, 0], L: 2 * W, bays: NL },
  { o: [TX + W, TY - H, 0], u: [0, 1, 0], n: [1, 0, 0], L: 2 * H, bays: NS },
  { o: [TX + W, TY + H, 0], u: [-1, 0, 0], n: [0, 1, 0], L: 2 * W, bays: NL },
  { o: [TX - W, TY + H, 0], u: [0, -1, 0], n: [-1, 0, 0], L: 2 * H, bays: NS },
]
const P = (f: Face, s: number, z: number): V3 => [f.o[0] + f.u[0] * s, f.o[1] + f.u[1] * s, z]
const centre: V3 = [TX, TY, 0]

/**
 * One diagrid triangle: a broad silver frame on the face plane, with the glass
 * set back DEPTH behind it. The frame's inner edge rolls into the recess, so
 * it catches light like a bevelled pier.
 */
function framed(a: V3, b: V3, c: V3, pane = glass) {
  let n = unit(cross(sub(b, a), sub(c, a)))
  const mid3 = add(add(a, b, 1), c, 1).map(x => x / 3) as V3
  const out = sub(mid3, [centre[0], centre[1], mid3[2]])
  if (dot(n, out) < 0) { [b, c] = [c, b]; n = n.map(x => -x) as V3 }
  // Incentre and inradius: a uniform inset is a scale about the incentre.
  const la = Math.hypot(...sub(b, c)), lb = Math.hypot(...sub(c, a)), lc = Math.hypot(...sub(a, b))
  const per = la + lb + lc
  const I: V3 = [(a[0] * la + b[0] * lb + c[0] * lc) / per, (a[1] * la + b[1] * lb + c[1] * lc) / per, (a[2] * la + b[2] * lb + c[2] * lc) / per]
  const area = Math.hypot(...cross(sub(b, a), sub(c, a))) / 2, r = 2 * area / per
  const k = Math.max(.2, (r - FRAME) / r)
  const outer = [a, b, c]
  const inner = outer.map(p => add(I, sub(p, I), k))
  const back = inner.map(p => add(p, n, -DEPTH))
  fan(pane, back)
  for (let i = 0; i < 3; i++) {
    const j = (i + 1) % 3
    silver.quad(outer[i], outer[j], inner[j], inner[i])
    // The return faces the pane's centre; at its lip it shares the face normal.
    const toward = unit(sub(I, add(inner[i], sub(inner[j], inner[i]), .5)))
    const retN = unit(add(toward, n, .45))
    quadN(silver, inner[i], inner[j], back[j], back[i], unit(add(n, toward, .5)), unit(add(n, toward, .5)), retN, retN)
  }
}

/** Level i is the full rectangle when odd; even levels have the corners cut. */
const cut = (i: number) => i % 2 === 0
const levelZ = (i: number) => Z0 + TIER * i

for (let t = 0; t < TIERS; t++) {
  const z0 = levelZ(t), z1 = levelZ(t + 1), bottomCut = cut(t)
  for (const f of faces) {
    const b = f.L / f.bays
    for (let k = 0; k < f.bays; k++) {
      if (!bottomCut) {
        // Full bottom: upright triangles on the bays, inverted between them.
        framed(P(f, k * b, z0), P(f, (k + 1) * b, z0), P(f, (k + .5) * b, z1))
        if (k > 0) framed(P(f, k * b, z0), P(f, (k + .5) * b, z1), P(f, (k - .5) * b, z1))
      } else {
        framed(P(f, (k + .5) * b, z0), P(f, (k + 1) * b, z1), P(f, k * b, z1))
        if (k > 0) framed(P(f, (k - .5) * b, z0), P(f, (k + .5) * b, z0), P(f, k * b, z1))
      }
    }
  }
  // The bird's mouths: a tilted triangle at every corner.
  for (let i = 0; i < 4; i++) {
    const A = faces[i], B = faces[(i + 1) % 4]
    const zFull = bottomCut ? z1 : z0, zCut = bottomCut ? z0 : z1
    framed(P(A, A.L, zFull), P(A, A.L - A.L / A.bays / 2, zCut), P(B, B.L / B.bays / 2, zCut))
  }
}

// The cut plan, used by the lobby storeys below the diagrid.
const cutRing = (z: number, d = 0): V3[] => {
  const ring: V3[] = []
  for (const f of faces) {
    const h = f.L / f.bays / 2
    ring.push(add(P(f, h, z), f.n, -d), add(P(f, f.L - h, z), f.n, -d))
  }
  return ring
}

// ---- Lobby storeys under the diagrid ----------------------------------------
// Recessed glass from the atrium roof up to the first nodes, on silver
// columns standing under each bottom node, with a silver soffit closing the
// gap between the glass line and the diagrid's face.
{
  const ZL = PARAPET - .4, SET = 1.4
  glass.loft([cutRing(ZL, SET), cutRing(Z0, SET)])
  const outerRing = cutRing(Z0), innerRing = cutRing(Z0, SET)
  for (let i = 0; i < 8; i++) {
    const j = (i + 1) % 8
    silver.quad(innerRing[i], innerRing[j], outerRing[j], outerRing[i])
  }
  const COL = .8
  for (const f of faces) {
    const b = f.L / f.bays
    for (let k = 0; k < f.bays; k++) {
      // The end nodes sit at the cut corners: keep their columns on the face.
      const s = Math.min(Math.max((k + .5) * b, b / 2 + COL), f.L - b / 2 - COL)
      const lo = [P(f, s - COL, ZL), P(f, s + COL, ZL), add(P(f, s + COL, ZL), f.n, -SET), add(P(f, s - COL, ZL), f.n, -SET)]
      const hi = lo.map(p => [p[0], p[1], Z0] as V3)
      // Front, then the two sides; the back is inside the glass.
      silver.quad(lo[0], lo[1], hi[1], hi[0])
      silver.quad(lo[1], lo[2], hi[2], hi[1])
      silver.quad(lo[3], lo[0], hi[0], hi[3])
    }
  }
}

// ---- Roofs ------------------------------------------------------------------
// The tower's roof is the full rectangle, behind a bevelled silver coping.
{
  const zt = levelZ(TIERS)
  const rect = (d: number, z: number): V3[] => [
    [TX - W + d, TY - H + d, z], [TX + W - d, TY - H + d, z], [TX + W - d, TY + H - d, z], [TX - W + d, TY + H - d, z],
  ]
  silver.loft([rect(0, zt), rect(.5, zt + .35), rect(1, zt - .1)])
  fan(roof, rect(1, zt - .1))
}

// The atrium skylight fills the base's parapet ring; the part under the tower
// is hidden by the lobby glass. The ring is not convex (the 56th Street step),
// so it is cut into two convex halves.
{
  const ring = inset(BASE, .9)
  for (const half of [clip(ring, ([x]) => 4.47 - x), clip(ring, ([x]) => x - 4.47)])
    fan(skylight, at(half, PARAPET - .4))
}

// sRGB colours from daylight photos.
const parts = [
  { part: stone, material: { name: 'cast-stone', color: 0xd6c4a5 } },
  { part: windows, material: { name: 'window', color: 0x6c757c } },
  { part: glass, material: { name: 'window-2', color: 0x3b5774, roughness: .45 } },
  { part: silver, material: { name: 'silver-diagrid', color: 0xd3d8dc, roughness: .5 } },
  { part: roof, material: { name: 'roof', color: 0xbdb9b1 } },
  { part: skylight, material: { name: 'glass', color: 0xa9b7be, roughness: .5 } },
]
const triangles = parts.reduce((n, { part }) => n + part.triangles, 0)
if (triangles > 5000) throw new Error(`Triangle budget exceeded: ${triangles}`)
const glb = writeGlb('Hearst Tower', parts, {
  license: 'CC0-1.0', bearing: 29, elevation: 0, anchor: [40.7665639, -73.9836231], height: 182,
  frame: 'Y up, -Z north, +X east, metres; origin at the ground anchor',
  note: '1928 cast-stone base with pylons; nine-tier diagrid tower with bird\'s-mouth corners',
})
if (glb.length > 256000) throw new Error(`File budget exceeded: ${glb.length}`)
const out = new URL('../../landmarks/models/hearst-tower.glb', import.meta.url).pathname
await Bun.write(out, glb)
console.log(`${out}: ${triangles} triangles, ${glb.length} bytes (${(glb.length / 1024).toFixed(1)} KiB)`)
