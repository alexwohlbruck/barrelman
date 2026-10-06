/**
 * Freedom Park Bandshell, Charlotte — procedural, CC0-1.0.
 * bun scripts/landmarks/freedom-park-bandshell.ts
 *
 * Map frame: x across the stage, y towards the audience, z up, metres.
 * Placed at bearing 320°: the shell opens north-west across the moat to the
 * grass amphitheatre (way/1185804824). The anchor is the centroid of the OSM
 * outline (way/921811959), 21.4 m across and 16 m deep in this frame; the
 * model keeps to that extent, but not to its oval shape, which is too smooth
 * to be the roof.
 *
 * A thin white concrete shell vault on an island in the lake. It runs front
 * to back, a rounded trapezoid in plan that widens towards the audience. The
 * front edge sweeps up into a broad arch (7.3 m at the crest) and kicks out
 * into upturned gull-wing tips at both front corners; the back edge is lower
 * and plainer. Three curved concrete ribs carry it: each has two legs that
 * rise from the ground and flare out into the wings, with an arch springing
 * between them, and the side bays between the front two ribs are X-braced.
 * Behind the stage stands a pale acoustic wall with a curved top, and the
 * stage is a deck on a stacked-stone base whose front bows out towards the
 * audience.
 *
 * Heights are from the photos, scaled to the OSM width: crest 7.3 m, shell
 * 0.3 m thick, arches springing at 3.4 m, stage 1.1 m. The island is flat,
 * so y = 0 is its ground. The water and the island's walls are the map's.
 */
import { Part, cross, sub, writeGlb, type V3 } from './mesh'
import { PALETTE, finish } from './palette'

const concrete = new Part()  // frames, columns, soffit and fascia
const top = new Part()       // the roof's upper surface
const wall = new Part()      // acoustic back wall
const stoneBase = new Part() // stage base
const deck = new Part()      // stage floor

const TAU = Math.PI * 2
const dot = (a: V3, b: V3) => a[0] * b[0] + a[1] * b[1] + a[2] * b[2]
const add = (a: V3, b: V3, k = 1): V3 => [a[0] + b[0] * k, a[1] + b[1] * k, a[2] + b[2] * k]
const unit = (a: V3): V3 => { const l = Math.hypot(...a) || 1; return [a[0] / l, a[1] / l, a[2] / l] }

/** A triangle whose winding agrees with its normals, whatever order it came in. */
function tri(p: Part, a: V3, b: V3, c: V3, n: V3[]) {
  const f = cross(sub(b, a), sub(c, a))
  if (Math.hypot(...f) < 1e-10) return
  if (dot(f, add(add(n[0], n[1]), n[2])) >= 0) p.tri(a, b, c, undefined, undefined, undefined, n)
  else p.tri(a, c, b, undefined, undefined, undefined, [n[0], n[2], n[1]])
}
function quad(p: Part, a: V3, b: V3, c: V3, d: V3, n: V3 | V3[]) {
  const N = Array.isArray(n[0]) ? (n as V3[]) : [n as V3, n as V3, n as V3, n as V3]
  tri(p, a, b, c, [N[0], N[1], N[2]])
  tri(p, a, c, d, [N[0], N[2], N[3]])
}
function face(p: Part, pts: V3[], n: V3) {
  for (let i = 1; i < pts.length - 1; i++) tri(p, pts[0], pts[i], pts[i + 1], [n, n, n])
}

// --- The roof -----------------------------------------------------------------
// A thin concrete vault running front to back. (u, v) parametrise it: u runs
// across from -1 to 1, v from the back edge (0) to the front edge (1).
// In plan it is a rounded trapezoid, widening to the front; in section it is
// an arch that rises towards the front, where its edge sweeps up into a broad
// arch and kicks out into gull-wing tips. The back edge is lower and plainer.
const SHELL = 0.3
const halfWidth = (v: number) => 7.0 + 3.6 * v * v
const yBack = (u: number) => -7.0 - 0.5 * (1 - u * u)
const yFront = (u: number) => 6.7 + 0.7 * (1 - u * u)
const crest = (v: number) => 5.6 + 1.7 * v ** 1.5
const eave = (v: number) => 3.9 + 0.4 * v
const kick = (v: number) => 1.0 * v ** 3
const smooth = (a: number, b: number, t: number) => { const s = Math.min(1, Math.max(0, (t - a) / (b - a))); return s * s * (3 - 2 * s) }
/** Height of the upper surface at (u, v). */
const zTop = (u: number, v: number) => {
  const a = Math.abs(u)
  return eave(v) + (crest(v) - eave(v)) * Math.cos((Math.PI / 2) * a) ** 0.85 + kick(v) * smooth(0.72, 1, a) ** 2
}
const point = (u: number, v: number): [number, number] => [u * halfWidth(v), yBack(u) + (yFront(u) - yBack(u)) * v]
/** (u, v) under a plan point, by a few fixed-point steps. */
function uvAt(x: number, y: number): [number, number] {
  let u = 0, v = 0.5
  for (let i = 0; i < 8; i++) {
    u = Math.max(-1, Math.min(1, x / halfWidth(v)))
    v = Math.max(0, Math.min(1, (y - yBack(u)) / (yFront(u) - yBack(u))))
  }
  return [u, v]
}
/** The vault's underside above a plan point. */
const soffit = (x: number, y: number) => { const [u, v] = uvAt(x, y); return zTop(u, v) - SHELL }
const CREST = crest(1)
{
  const NU = 20, NV = 8
  const P = (i: number, k: number, upper: boolean): V3 => {
    const u = -1 + (2 * i) / NU, v = k / NV, [x, y] = point(u, v)
    return [x, y, zTop(u, v) - (upper ? 0 : SHELL)]
  }
  // Smooth normals from the surface's own neighbours.
  const normal = (i: number, k: number): V3 => {
    const a = P(Math.max(0, i - 1), k, true), b = P(Math.min(NU, i + 1), k, true)
    const c = P(i, Math.max(0, k - 1), true), d = P(i, Math.min(NV, k + 1), true)
    const n = unit(cross(sub(b, a), sub(d, c)))
    return n[2] >= 0 ? n : [-n[0], -n[1], -n[2]]
  }
  const down = (n: V3): V3 => [-n[0], -n[1], -n[2]]
  for (let i = 0; i < NU; i++)
    for (let k = 0; k < NV; k++) {
      const N = [normal(i, k), normal(i + 1, k), normal(i + 1, k + 1), normal(i, k + 1)]
      quad(top, P(i, k, true), P(i + 1, k, true), P(i + 1, k + 1, true), P(i, k + 1, true), N)
      quad(concrete, P(i, k, false), P(i + 1, k, false), P(i + 1, k + 1, false), P(i, k + 1, false), N.map(down))
    }
  // The thin edge all round, facing out in plan.
  const ring: [number, number][] = []
  for (let i = 0; i <= NU; i++) ring.push([i, NV])
  for (let k = NV - 1; k >= 0; k--) ring.push([NU, k])
  for (let i = NU - 1; i >= 0; i--) ring.push([i, 0])
  for (let k = 1; k <= NV; k++) ring.push([0, k])
  for (let r = 0; r < ring.length - 1; r++) {
    const [i0, k0] = ring[r], [i1, k1] = ring[r + 1]
    const a = P(i0, k0, false), b = P(i1, k1, false), c = P(i1, k1, true), d = P(i0, k0, true)
    const e = sub(b, a)
    if (Math.hypot(e[0], e[1]) < 1e-6) continue
    // Clockwise round the plan seen from above, so the outward side is (e.y, -e.x)… or its opposite.
    const mx = (a[0] + b[0]) / 2, my = (a[1] + b[1]) / 2
    let n = unit([e[1], -e[0], 0])
    if (n[0] * mx + n[1] * (my + 0.3) < 0) n = [-n[0], -n[1], 0]
    quad(concrete, a, b, c, d, n)
  }
}

// --- Frames -------------------------------------------------------------------
/** Ear-clip a simple polygon given counter-clockwise. */
function triangulate(poly: [number, number][]): [number, number, number][] {
  const idx = poly.map((_, i) => i), out: [number, number, number][] = []
  const area2 = (a: number[], b: number[], c: number[]) => (b[0] - a[0]) * (c[1] - a[1]) - (b[1] - a[1]) * (c[0] - a[0])
  let guard = 0
  while (idx.length > 3 && guard++ < 10000) {
    let clipped = false
    for (let i = 0; i < idx.length; i++) {
      const a = idx[(i + idx.length - 1) % idx.length], b = idx[i], c = idx[(i + 1) % idx.length]
      if (area2(poly[a], poly[b], poly[c]) <= 1e-9) continue
      const inside = idx.some((j) => j !== a && j !== b && j !== c &&
        area2(poly[a], poly[b], poly[j]) >= 0 && area2(poly[b], poly[c], poly[j]) >= 0 && area2(poly[c], poly[a], poly[j]) >= 0)
      if (inside) continue
      out.push([a, b, c]); idx.splice(i, 1); clipped = true; break
    }
    if (!clipped) break
  }
  if (idx.length === 3) out.push([idx[0], idx[1], idx[2]])
  return out
}
/** A flat x–z outline, `d` thick about plane y, as a solid. */
function plate(p: Part, poly: [number, number][], y: number, d: number) {
  const y0 = y - d / 2, y1 = y + d / 2
  for (const [a, b, c] of triangulate(poly)) {
    const A = poly[a], B = poly[b], C = poly[c]
    tri(p, [A[0], y1, A[1]], [B[0], y1, B[1]], [C[0], y1, C[1]], [[0, 1, 0], [0, 1, 0], [0, 1, 0]])
    tri(p, [A[0], y0, A[1]], [B[0], y0, B[1]], [C[0], y0, C[1]], [[0, -1, 0], [0, -1, 0], [0, -1, 0]])
  }
  for (let i = 0; i < poly.length; i++) {
    const [ax, az] = poly[i], [bx, bz] = poly[(i + 1) % poly.length]
    // Counter-clockwise in (x, z): the outside is to the right of each edge.
    const n = unit([bz - az, 0, -(bx - ax)])
    quad(p, [ax, y0, az], [bx, y0, bz], [bx, y1, bz], [ax, y1, az], n)
  }
}
const span = (a: number, b: number, n: number) => Array.from({ length: n + 1 }, (_, i) => a + ((b - a) * i) / n)
const SPRING = 3.4, LEG = 0.36
/**
 * One transverse frame at depth v: two legs that rise from the ground and
 * flare outward into the vault's wings, and an arch springing between them,
 * all one curved concrete rib, its top buried in the shell.
 */
function frame(v: number, d: number) {
  const y = yBack(0.75) + (yFront(0.75) - yBack(0.75)) * v
  const hw = halfWidth(v), c = 0.78 * hw, tip = hw * 0.97
  const under = (x: number) => soffit(x, y) + 0.12
  const crown = soffit(0, y) - 0.55
  const poly: [number, number][] = []
  // Right leg's outer face: straight up off the ground, then curving out to the wing.
  const leg = (s: number) => span(0, 1, 7).map((t) => {
    const x = c + LEG + (tip - c - LEG) * t ** 2.2
    return [s * x, (under(s * tip) - 0.12) * t] as [number, number]
  })
  poly.push([c - LEG, 0], ...leg(1))
  // Across under the shell, right to left.
  for (const x of span(tip, -tip, 16)) poly.push([x, under(x)])
  poly.push(...leg(-1).reverse(), [-c + LEG, 0])
  // Up the left leg's inner face, over the arch, and down the right one.
  for (const t of span(Math.PI, 0, 12)) poly.push([(c - LEG) * Math.cos(t), SPRING + (crown - SPRING) * Math.sin(t)])
  // That last point is (c - LEG, SPRING); the loop closes back to the foot.
  // Remove duplicate consecutive points.
  const clean = poly.filter((q, i) => { const r = poly[(i + 1) % poly.length]; return Math.hypot(q[0] - r[0], q[1] - r[1]) > 1e-4 })
  // The outline runs counter-clockwise in (x, z): out along the top, back under the arch.
  plate(concrete, clean, y, d)
  return { y, c }
}
const F = [frame(0.86, 0.5), frame(0.5, 0.45), frame(0.15, 0.45)]
// The X-braces in the side bays between the front and middle frames.
function beam(p: Part, a: V3, b: V3, r: number) {
  const T = unit(sub(b, a)), side = unit(cross(T, [0, 0, 1])), up = unit(cross(side, T))
  const corner = (q: V3, i: number): V3 => {
    const sx = i === 0 || i === 3 ? -r : r, sz = i < 2 ? -r : r
    return add(add(q, side, sx), up, sz)
  }
  const A = [0, 1, 2, 3].map((i) => corner(a, i)), B = [0, 1, 2, 3].map((i) => corner(b, i))
  const mid: V3 = [(a[0] + b[0]) / 2, (a[1] + b[1]) / 2, (a[2] + b[2]) / 2]
  const order = [0, 1, 2, 3]
  for (let i = 0; i < 4; i++) {
    const j = (i + 1) % 4
    const q = [A[order[i]], A[order[j]], B[order[j]], B[order[i]]]
    const m: V3 = [(q[0][0] + q[2][0]) / 2 - mid[0], (q[0][1] + q[2][1]) / 2 - mid[1], (q[0][2] + q[2][2]) / 2 - mid[2]]
    const along = dot(m, T)
    const n = unit(add(m, T, -along))
    quad(p, q[0], q[1], q[2], q[3], n)
  }
}
for (const s of [-1, 1]) {
  const [f, m] = [F[0], F[1]]
  beam(concrete, [s * f.c, f.y - 0.25, 0.4], [s * m.c, m.y + 0.25, SPRING - 0.2], 0.2)
  beam(concrete, [s * f.c, f.y - 0.25, SPRING - 0.2], [s * m.c, m.y + 0.25, 0.4], 0.2)
}

// --- Back wall ------------------------------------------------------------------
{
  const y = -5.7, d = 0.35, half = 4.8
  const topZ = (x: number) => 3.4 + 1.6 * Math.sqrt(Math.max(0, 1 - (x / half) ** 2))
  const xs = span(-half, half, 12)
  for (let i = 0; i < xs.length - 1; i++) {
    const a = xs[i], b = xs[i + 1]
    for (const [yy, n] of [[y + d / 2, [0, 1, 0]], [y - d / 2, [0, -1, 0]]] as [number, V3][])
      quad(wall, [a, yy, 0], [b, yy, 0], [b, yy, topZ(b)], [a, yy, topZ(a)], n)
    const t: V3 = [b - a, 0, topZ(b) - topZ(a)]
    const n = unit([-t[2], 0, t[0]])
    quad(wall, [a, y - d / 2, topZ(a)], [b, y - d / 2, topZ(b)], [b, y + d / 2, topZ(b)], [a, y + d / 2, topZ(a)], n[2] >= 0 ? n : [-n[0], 0, -n[2]])
  }
  for (const [x, s] of [[-half, -1], [half, 1]] as [number, number][])
    quad(wall, [x, y - d / 2, 0], [x, y + d / 2, 0], [x, y + d / 2, topZ(x)], [x, y - d / 2, topZ(x)], [s, 0, 0])
}

// --- Stage ------------------------------------------------------------------------
{
  const H = 1.1, back = -5.5, sideFront = 2.6, apex = 6.0, half = 5.4
  // Plan, counter-clockwise from the back left corner: back edge, right side,
  // the bowed front, left side.
  const plan: [number, number][] = [[-4.6, back], [4.6, back], [half, -4.2]]
  const n = 12
  for (let i = 0; i <= n; i++) {
    const t = i / n, x = half * Math.cos(Math.PI * t)
    plan.push([x, sideFront + (apex - sideFront) * Math.sin(Math.PI * t)])
  }
  plan.push([-half, -4.2])
  const lip = 0.12
  for (let i = 0; i < plan.length; i++) {
    const [ax, ay] = plan[i], [bx, by] = plan[(i + 1) % plan.length]
    const nrm = unit([by - ay, -(bx - ax), 0])
    quad(stoneBase, [ax, ay, 0], [bx, by, 0], [bx, by, H - lip], [ax, ay, H - lip], nrm)
    // A concrete lip round the deck's edge, chamfered.
    const ia: V3 = [ax - nrm[0] * lip, ay - nrm[1] * lip, H], ib: V3 = [bx - nrm[0] * lip, by - nrm[1] * lip, H]
    quad(deck, [ax, ay, H - lip], [bx, by, H - lip], ib, ia, unit(add(nrm, [0, 0, 1])))
  }
  face(deck, plan.map(([x, y]) => {
    const cx = 0, cy = (back + apex) / 2
    const l = Math.hypot(x - cx, y - cy)
    return [x - ((x - cx) / l) * 0.12, y - ((y - cy) / l) * 0.12, H] as V3
  }), [0, 0, 1])
}

const parts = [
  // White painted concrete from the 2022 daylight photo; the roof's upper
  // face a shade darker, as weathered concrete is; the acoustic wall a warm
  // pale grey; the stage base the island's tan stacked fieldstone.
  { part: concrete, material: PALETTE.stone },
  { part: top, material: finish('shell-roof', 0xdcd8d0) },
  { part: wall, material: finish('acoustic-wall', 0xcac6bc) },
  { part: stoneBase, material: finish('fieldstone', 0xb9ab95) },
  { part: deck, material: finish('stage-deck', 0xd2cec6) },
]
const triangles = parts.reduce((n, { part }) => n + part.triangles, 0)
console.log(parts.map(({ part, material }) => `${material.name}: ${part.triangles}`).join('\n'))
if (triangles > 3000) throw new Error(`Triangle budget exceeded: ${triangles}`)
const glb = writeGlb('Freedom Park Bandshell', parts, {
  license: 'CC0-1.0', frame: 'Y up, -Z north, +X east, metres, origin at ground', height: CREST, bearing: 320,
})
await Bun.write(new URL('../../landmarks/models/freedom-park-bandshell.glb', import.meta.url), glb)
console.log(`freedom-park-bandshell.glb: ${triangles} triangles, ${glb.length} bytes`)
