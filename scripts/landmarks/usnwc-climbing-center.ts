/**
 * U.S. National Whitewater Center, Climbing Center — procedural, CC0-1.0.
 * bun scripts/landmarks/usnwc-climbing-center.ts
 *
 * Map frame: x east, y north, z up, metres; placed at bearing 0. The origin is
 * midway between the two OSM buildings it replaces: the covered wall
 * (way/223914924, a 16 x 10.9 m roof whose long side runs at 115.5°) and the
 * free-standing spire (way/1456416417, a 6 m square).
 *
 * Two structures, after Commons "USNWC Climbing Center" (taken from the lawn
 * to the south):
 * - the covered wall: a sculpted concrete rock face 14 m (46 ft) high with a
 *   lower boulder lobe beside it, under a thin mono-pitch steel roof on four
 *   slender columns, open to the south;
 * - the spire: 14.3 m (47 ft) of stacked faux-rock strata, a tall east pillar
 *   with a tilted, flared cap and a shorter west pillar with an overhanging
 *   lip about two-thirds of the way up.
 *
 * Heights are the operator's published figures (whitewater.org, Wikipedia).
 * The rock is built as stacked, bevelled, irregular blocks: chunky strata that
 * read at map distance, not a sculpted surface.
 */
import { Part, writeGlb, type V3 } from './mesh'
import { PALETTE, finish } from './palette'

type XY = [number, number]
const rock = new Part(), rockDark = new Part(), roof = new Part(), trim = new Part()

const unit = (v: V3): V3 => { const l = Math.hypot(...v) || 1; return [v[0] / l, v[1] / l, v[2] / l] }

function quadN(p: Part, a: V3, b: V3, c: V3, d: V3, n?: V3) {
  const N = n ? [n, n, n] : undefined
  p.tri(a, b, c, undefined, undefined, undefined, N)
  p.tri(a, c, d, undefined, undefined, undefined, N)
}

/** Ear-clipping triangulation of a simple counter-clockwise polygon. */
function earcut(pts: XY[]): [number, number, number][] {
  const idx = pts.map((_, i) => i), out: [number, number, number][] = []
  const cz = (a: XY, b: XY, c: XY) => (b[0] - a[0]) * (c[1] - a[1]) - (b[1] - a[1]) * (c[0] - a[0])
  const inside = (p: XY, a: XY, b: XY, c: XY) => cz(a, b, p) > 0 && cz(b, c, p) > 0 && cz(c, a, p) > 0
  let guard = 0
  while (idx.length > 3 && guard++ < 10000) {
    let cut = false
    for (let k = 0; k < idx.length; k++) {
      const i0 = idx[(k + idx.length - 1) % idx.length], i1 = idx[k], i2 = idx[(k + 1) % idx.length]
      if (cz(pts[i0], pts[i1], pts[i2]) <= 1e-9) continue
      if (idx.some((j) => j !== i0 && j !== i1 && j !== i2 && inside(pts[j], pts[i0], pts[i1], pts[i2]))) continue
      out.push([i0, i1, i2]); idx.splice(k, 1); cut = true; break
    }
    if (!cut) break
  }
  if (idx.length === 3) out.push([idx[0], idx[1], idx[2]])
  return out
}

/** Move every edge of a counter-clockwise ring inward by d (mitred). */
function inset(pts: XY[], d: number): XY[] {
  const n = pts.length
  return pts.map((p, i) => {
    const a = pts[(i + n - 1) % n], b = pts[(i + 1) % n]
    const e0 = unit([p[0] - a[0], p[1] - a[1], 0]), e1 = unit([b[0] - p[0], b[1] - p[1], 0])
    const n0: XY = [-e0[1], e0[0]], n1: XY = [-e1[1], e1[0]] // inward normals
    const m = unit([n0[0] + n1[0], n0[1] + n1[1], 0]), k = d / Math.max(0.5, m[0] * n0[0] + m[1] * n0[1])
    return [p[0] + m[0] * k, p[1] + m[1] * k]
  })
}

/** A point `u` along bearing `ang` and `v` to its right, from `c`. */
const frame = (c: XY, ang: number, u: number, v: number): XY =>
  [c[0] + Math.sin(ang) * u + Math.cos(ang) * v, c[1] + Math.cos(ang) * u - Math.sin(ang) * v]
const rect = (c: XY, ang: number, hu: number, hv: number): XY[] =>
  [frame(c, ang, -hu, -hv), frame(c, ang, -hu, hv), frame(c, ang, hu, hv), frame(c, ang, hu, -hv)]

type Z = (p: XY) => number
const flat = (z: number): Z => () => z

/**
 * A prism over a counter-clockwise ring from z0 up to a (possibly sloping)
 * top, with a 45° bevel of `b` round the top edge and the lid in `lid`.
 */
function prism(p: Part, ring: XY[], bottom: number | Z, top: Z, b: number, lid: Part | null = p, bottomLid: Part | null = null) {
  const n = ring.length, inner = b > 0 ? inset(ring, b) : ring
  const zb: Z = typeof bottom === 'number' ? flat(bottom) : bottom
  for (let i = 0; i < n; i++) {
    const A = ring[i], B = ring[(i + 1) % n]
    const out = unit([B[1] - A[1], A[0] - B[0], 0])
    quadN(p, [A[0], A[1], zb(A)], [B[0], B[1], zb(B)], [B[0], B[1], top(B) - b], [A[0], A[1], top(A) - b], out)
    if (b > 0) {
      const a2 = inner[i], b2 = inner[(i + 1) % n]
      quadN(p, [A[0], A[1], top(A) - b], [B[0], B[1], top(B) - b], [b2[0], b2[1], top(b2)], [a2[0], a2[1], top(a2)], unit([out[0], out[1], 1]))
    }
  }
  if (lid) for (const [i, j, k] of earcut(inner)) {
    const P = (q: XY): V3 => [q[0], q[1], top(q)]
    lid.tri(P(inner[i]), P(inner[j]), P(inner[k]))
  }
  if (bottomLid) for (const [i, j, k] of earcut(ring)) {
    const P = (q: XY): V3 => [q[0], q[1], zb(q)]
    bottomLid.tri(P(ring[i]), P(ring[k]), P(ring[j]))
  }
}


/**
 * One stratum of rock: an irregular octagon around `c` (u along bearing `ang`,
 * v to its right), half-sizes `hu`/`hv`, corners cut by varying amounts so no
 * two blocks match.
 */
function stratum(p: Part, c: XY, ang: number, hu: number, hv: number, z0: number, z1: number, cut: number[], b = 0.3) {
  const [a, bb, cc, d] = cut
  const ring: XY[] = [
    frame(c, ang, -hu + a, -hv), frame(c, ang, -hu, -hv + a),
    frame(c, ang, -hu, hv - bb), frame(c, ang, -hu + bb, hv),
    frame(c, ang, hu - cc, hv), frame(c, ang, hu, hv - cc),
    frame(c, ang, hu, -hv + d), frame(c, ang, hu - d, -hv),
  ]
  prism(p, ring, z0, flat(z1), b)
}

// ---------------------------------------------------------------------------
// The covered wall.

const WALL_C: XY = [-13.1, 4.85], WA = (115.5 * Math.PI) / 180
// Roof: the OSM outline, a little proud all round; high at the open south
// front, falling to the back.
{
  const ring = rect(WALL_C, WA, 8.3, 5.75)
  const front = (q: XY) => {
    const d: XY = [q[0] - WALL_C[0], q[1] - WALL_C[1]]
    return d[0] * Math.cos(WA) - d[1] * Math.sin(WA) // v: + toward the open front
  }
  const top: Z = (q) => 16.2 + 0.06 * front(q)
  prism(trim, ring, (q) => top(q) - 0.55, top, 0.2, roof, roof)
  // Four slender columns, inset from the roof's corners.
  for (const [u, v] of [[-7.4, 4.9], [7.4, 4.9], [-7.4, -4.9], [7.4, -4.9]]) {
    prism(roof, rect(frame(WALL_C, WA, u, v), WA, 0.28, 0.28), 0, (q) => top(q) - 0.55, 0, null)
  }
}
// The rock face along the back of the shelter, its left two-thirds: three
// abutting buttresses, the middle one tallest and reaching just under the
// roof. Each is one sculpted mass that leans out toward the open front as it
// rises — the overhang the routes climb — so it reads as a cliff, not as
// stacked blocks.
{
  const B: [number, number, number, number, number][] = [
    // u centre, half-width, base depth, height, lean
    [-5.4, 2.2, 3.0, 13.0, 0.9],
    [-1.3, 2.1, 3.4, 14.0, 1.3],
    [2.6, 1.9, 3.0, 12.6, 0.8],
  ]
  for (const [u, hw, depth, h, lean] of B) {
    const oct = (dv: number, hv: number, w: number, z: number): V3[] => {
      const c = frame(WALL_C, WA, u, -5.0 + dv)
      const k = 0.7
      return ([[-w + k, -hv], [-w, -hv + k], [-w, hv - k], [-w + k, hv], [w - k, hv], [w, hv - k], [w, -hv + k], [w - k, -hv]] as XY[])
        .map(([a, bv]) => { const q = frame(c, WA, a, bv); return [q[0], q[1], z] as V3 })
    }
    const r0 = oct(depth / 2, depth / 2, hw, 0)
    const r1 = oct(depth / 2 + lean, depth / 2, hw - 0.15, h - 0.4)
    const r2 = oct(depth / 2 + lean - 0.1, depth / 2 - 0.4, hw - 0.55, h)
    rockDark.loft([r0, r1, r2])
    rockDark.cap(r2, true)
  }
  // The boulder lobe east of the face: lower and deeper, one mass that
  // bulges out toward the front and rounds over at the top.
  {
    const oct = (u: number, dv: number, hv: number, w: number, z: number): V3[] => {
      const c = frame(WALL_C, WA, u, -5.0 + dv), k = 0.9
      return ([[-w + k, -hv], [-w, -hv + k], [-w, hv - k], [-w + k, hv], [w - k, hv], [w, hv - k], [w, -hv + k], [w - k, -hv]] as XY[])
        .map(([a, bv]) => { const q = frame(c, WA, a, bv); return [q[0], q[1], z] as V3 })
    }
    const rings = [oct(5.3, 2.0, 2.0, 2.0, 0), oct(5.4, 2.6, 2.6, 2.3, 4.0), oct(5.3, 2.5, 2.4, 2.1, 6.4), oct(5.0, 2.2, 1.7, 1.5, 7.4)]
    rockDark.loft(rings)
    rockDark.cap(rings[3], true)
  }
}

// ---------------------------------------------------------------------------
// The spire. Built facing the lawn to the south: u east, v south.

const SPIRE_C: XY = [13.15, -4.85], SA = (123.1 * Math.PI) / 180 // the OSM square's own axis
{
  // East pillar: seven strata to 12.9 m, the top two flaring east, then a
  // tilted cap to 14.3 m.
  const east: [number, number, number, number][] = [
    // du, half-width, half-depth, height
    [0.5, 2.0, 2.5, 2.4], [0.8, 1.7, 2.25, 1.6], [0.45, 1.85, 2.2, 2.1], [0.75, 1.6, 2.1, 1.5],
    [0.7, 1.8, 2.05, 2.0], [1.05, 1.85, 2.0, 1.6], [1.3, 2.15, 2.05, 1.7],
  ]
  let z = 0
  east.forEach(([du, hu, hv, h], i) => {
    stratum(rock, frame(SPIRE_C, SA, du, 0), SA, hu, hv, z, z + h, [0.5, 0.6, 0.4 + 0.1 * i, 0.5])
    z += h
  })
  // The cap: a thin slab, longer than the pillar is wide, rising to the east.
  {
    const c = frame(SPIRE_C, SA, 1.5, 0)
    const ring = rect(c, SA + Math.PI / 2, 2.0, 2.6)
    const rise: Z = (q) => {
      const d: XY = [q[0] - c[0], q[1] - c[1]]
      return z + 0.75 + 0.18 * (d[0] * Math.sin(SA) + d[1] * Math.cos(SA))
    }
    prism(rock, ring, (q) => rise(q) - 0.9, rise, 0.25, rock, rock)
  }
  // West pillar: shorter, leaning on the east one, with an overhanging lip
  // at the top on its west side.
  const west: [number, number, number, number][] = [
    [-1.6, 1.4, 2.2, 2.1], [-1.65, 1.35, 2.0, 2.0], [-1.6, 1.3, 1.9, 1.9], [-1.7, 1.35, 1.9, 1.8],
  ]
  z = 0
  west.forEach(([du, hu, hv, h], i) => {
    stratum(rock, frame(SPIRE_C, SA, du, -0.2), SA, hu, hv, z, z + h, [0.4, 0.5, 0.3, 0.6])
    z += h
  })
  stratum(rock, frame(SPIRE_C, SA, -1.9, -0.2), SA, 1.6, 1.8, z, z + 1.0, [0.4, 0.5, 0.3, 0.5])
  stratum(rock, frame(SPIRE_C, SA, -2.4, -0.2), SA, 1.8, 1.9, z + 1.0, z + 2.1, [0.5, 0.6, 0.3, 0.6])
}

// ---------------------------------------------------------------------------

// The faux rock: the spire a warm sandy tan in sun, the covered wall a
// greyer, browner stone; both pulled to the palette's lightness. Roof and
// columns are the palette roof grey, the roof's fascia trim.
const parts = [
  { part: rock, material: finish('usnwc-rock', 0xd6c4a6) },
  { part: rockDark, material: finish('usnwc-rock-shade', 0xbdb2a2) },
  { part: roof, material: PALETTE.roof },
  { part: trim, material: PALETTE.trim },
]
const triangles = parts.reduce((sum, { part }) => sum + part.triangles, 0)
if (triangles > 5000) throw new Error(`Triangle budget exceeded: ${triangles}`)
const glb = writeGlb('U.S. National Whitewater Center Climbing Center', parts, {
  license: 'CC0-1.0', frame: 'Y up, -Z north, +X east, metres; origin at ground anchor',
  bearing: 0, elevation: 0, height: 16.6,
})
if (glb.length > 256000) throw new Error(`File budget exceeded: ${glb.length}`)
const out = new URL('../../landmarks/models/usnwc-climbing-center.glb', import.meta.url).pathname
await Bun.write(out, glb)
console.log(`${out}: ${triangles} triangles, ${glb.length} bytes`)
