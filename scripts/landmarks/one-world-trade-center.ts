/**
 * One World Trade Center — original procedural geometry, CC0-1.0.
 * bun scripts/landmarks/one-world-trade-center.ts
 *
 * x/y run along the tower's square faces (the catalog applies the 27.4°
 * bearing), z is metres up. Anchor at the OSM outline's centre
 * (way/713565776), 40.7130000,-74.0131893.
 *
 * The tower is an octagonal antiprism: four tall isosceles triangles stand on
 * the podium's edges and rise to the corners of a square turned 45°, and four
 * inverted ones hang from that square's edges down to the base corners. That
 * shape is the building's identity, so it is exact; everything else is kept
 * broad. Heights follow OSM: podium 57 m, roof 417 m, ring 417–421 m, spire
 * tip 541 m.
 */
import { Part, writeGlb, type V3 } from './mesh'

type XY = [number, number]
const glass = new Part(), mullion = new Part(), arris = new Part()
const wall = new Part(), fins = new Part(), metal = new Part()
const roof = new Part(), ring = new Part(), spire = new Part()

const H = 32.4 // podium half-width: the OSM outline is a 64.8 m square
const T = H - .6 // tower base half-width, leaving a coping lip on the podium
const Z0 = 57, Z1 = 404, ZR = 417
const FOLD = 1.3 // inward crease along each triangle's median: faceted glass
const unit = (v: V3): V3 => { const l = Math.hypot(...v); return v.map(n => n / l) as V3 }
const add = (a: V3, b: V3, s = 1): V3 => [a[0] + b[0] * s, a[1] + b[1] * s, a[2] + b[2] * s]
const dot = (a: V3, b: V3) => a[0] * b[0] + a[1] * b[1] + a[2] * b[2]
const at = (r: XY[], z: number): V3[] => r.map(([x, y]) => [x, y, z])

function quadN(p: Part, a: V3, b: V3, c: V3, d: V3, na: V3, nb: V3, nc = nb, nd = na) {
  p.tri(a, b, c, undefined, undefined, undefined, [na, nb, nc])
  p.tri(a, c, d, undefined, undefined, undefined, [na, nc, nd])
}
function fan(p: Part, poly: V3[]) {
  for (let i = 1; i < poly.length - 1; i++) p.tri(poly[0], poly[i], poly[i + 1])
}

/** A plan ring with every convex corner rounded, with smooth normals. */
function soften(r: XY[], radius: number) {
  const points: XY[] = [], normals: V3[] = []
  for (let i = 0; i < r.length; i++) {
    const a = r[(i + r.length - 1) % r.length], b = r[i], c = r[(i + 1) % r.length]
    const l0 = Math.hypot(b[0] - a[0], b[1] - a[1]), l1 = Math.hypot(c[0] - b[0], c[1] - b[1])
    const u: XY = [(b[0] - a[0]) / l0, (b[1] - a[1]) / l0], v: XY = [(c[0] - b[0]) / l1, (c[1] - b[1]) / l1]
    points.push([b[0] - u[0] * radius, b[1] - u[1] * radius], [b[0] + v[0] * radius, b[1] + v[1] * radius])
    normals.push([u[1], -u[0], 0], [v[1], -v[0], 0])
  }
  return { points, normals }
}
function solid(p: Part, r: { points: XY[]; normals: V3[] }, z0: number, z1: number, top = p, bottom = true) {
  const a = at(r.points, z0), b = at(r.points, z1), n = a.length
  for (let i = 0; i < n; i++) {
    const j = (i + 1) % n
    quadN(p, a[i], a[j], b[j], b[i], r.normals[i], r.normals[j])
  }
  fan(top, b)
  if (bottom) fan(p, [...a].reverse())
}
const square = (h: number): XY[] => [[-h, -h], [h, -h], [h, h], [-h, h]]
const box = (x0: number, x1: number, y0: number, y1: number): XY[] => [[x0, y0], [x1, y0], [x1, y1], [x0, y1]]

// ---- Podium: a windowless 57 m block clad in glass fins -------------------
// A darker core set 0.45 m back, broad pale fins standing out to the face,
// and bevelled corner piers, so the fins never pass a corner.
const CORE = H - .45, PIER = 3.2, FIN_W = 3, FINS = 8, PZ = 56.2
solid(wall, soften(square(CORE), .3), 0, PZ, wall, false)
for (const [sx, sy] of [[-1, -1], [1, -1], [1, 1], [-1, 1]]) {
  const xs = [sx * H, sx * (H - PIER)].sort((a, b) => a - b), ys = [sy * H, sy * (H - PIER)].sort((a, b) => a - b)
  solid(fins, soften(box(xs[0], xs[1], ys[0], ys[1]), .45), 0, PZ, fins, false)
}
const span = 2 * (H - PIER), pitch = span / FINS
for (let f = 0; f < 4; f++) {
  // Each face built along +x at y = -H, then turned a quarter at a time.
  const rot = (x: number, y: number): XY => {
    const c = Math.round(Math.cos(f * Math.PI / 2)), s = Math.round(Math.sin(f * Math.PI / 2))
    return [x * c - y * s, x * s + y * c]
  }
  for (let k = 0; k < FINS; k++) {
    const m = -H + PIER + pitch * (k + .5)
    const r = [rot(m - FIN_W / 2, -H), rot(m + FIN_W / 2, -H), rot(m + FIN_W / 2, -CORE - .05), rot(m - FIN_W / 2, -CORE - .05)]
    const a = at(r, 0), b = at(r, PZ)
    for (let i = 0; i < 3; i++) fins.quad(a[i], a[i + 1], b[i + 1], b[i])
    fins.quad(b[0], b[1], b[2], b[3])
  }
}
// Stainless coping where the tower takes over, bevelled on both edges.
{
  const r = soften(square(H), .45), inner = soften(square(H - .5), .45)
  const a = at(r.points, PZ), b = at(r.points, Z0 - .3), c = at(inner.points, Z0)
  for (let i = 0; i < a.length; i++) {
    const j = (i + 1) % a.length, ni = r.normals[i], nj = r.normals[j]
    const ui = unit(add(ni, [0, 0, 1])), uj = unit(add(nj, [0, 0, 1]))
    quadN(metal, a[i], a[j], b[j], b[i], ni, nj, uj, ui)
    quadN(metal, b[i], b[j], c[j], c[i], uj, ui, [0, 0, 1], [0, 0, 1])
  }
  fan(roof, c) // hidden under the tower except at the lip
}

// ---- Tower: the octagonal antiprism -----------------------------------------
const B: V3[] = square(T).map(([x, y]) => [x, y, Z0]) // base corners, ccw
const A: V3[] = [[0, -T, Z1], [T, 0, Z1], [0, T, Z1], [-T, 0, Z1]] // top corners
const inward = (p: V3): V3 => { const l = Math.hypot(p[0], p[1]); return [p[0] - p[0] / l * FOLD, p[1] - p[1] / l * FOLD, p[2]] }
const midpoint = (a: V3, b: V3): V3 => [(a[0] + b[0]) / 2, (a[1] + b[1]) / 2, (a[2] + b[2]) / 2]

/** Sutherland–Hodgman: keep the part of a planar polygon where f ≥ 0. */
function clip(poly: V3[], f: (p: V3) => number): V3[] {
  const out: V3[] = []
  for (let i = 0; i < poly.length; i++) {
    const p = poly[i], q = poly[(i + 1) % poly.length], fp = f(p), fq = f(q)
    if (fp >= 0) out.push(p)
    if ((fp >= 0) !== (fq >= 0)) {
      const t = fp / (fp - fq)
      out.push([p[0] + (q[0] - p[0]) * t, p[1] + (q[1] - p[1]) * t, p[2] + (q[2] - p[2]) * t])
    }
  }
  return out
}

/**
 * Lay one facet in strips across its horizontal edge: glass, with a few
 * broad pale mullion bands running up the face like the real vertical
 * mullions, and a stainless strip along the sloping arrises where the
 * triangles meet — the bright lines that draw the shape in photos.
 */
const STRIPE = 2.4, ARRIS = 1
function facet(tri: V3[], origin: V3, along: V3, stripes: number[], arrisEdges: [V3, V3][]) {
  const u = (p: V3) => dot([p[0] - origin[0], p[1] - origin[1], 0], along)
  // Strips from the mullion bands.
  const cuts: { a: number; b: number; part: Part }[] = []
  let last = -1e3
  for (const s of [...stripes].sort((a, b) => a - b)) {
    cuts.push({ a: last, b: s - STRIPE / 2, part: glass }, { a: s - STRIPE / 2, b: s + STRIPE / 2, part: mullion })
    last = s + STRIPE / 2
  }
  cuts.push({ a: last, b: 1e3, part: glass })
  // Distance from an arris, measured in the facet's plane.
  const n = unit([...(() => { const e1 = [tri[1][0] - tri[0][0], tri[1][1] - tri[0][1], tri[1][2] - tri[0][2]], e2 = [tri[2][0] - tri[0][0], tri[2][1] - tri[0][1], tri[2][2] - tri[0][2]]; return [e1[1] * e2[2] - e1[2] * e2[1], e1[2] * e2[0] - e1[0] * e2[2], e1[0] * e2[1] - e1[1] * e2[0]] })()] as V3)
  const inside = arrisEdges.map(([p, q]) => {
    const e = unit([q[0] - p[0], q[1] - p[1], q[2] - p[2]])
    let d: V3 = [n[1] * e[2] - n[2] * e[1], n[2] * e[0] - n[0] * e[2], n[0] * e[1] - n[1] * e[0]]
    const c = tri.reduce((s, v) => add(s, v, 1 / 3), [0, 0, 0] as V3)
    if (dot([c[0] - p[0], c[1] - p[1], c[2] - p[2]], d) < 0) d = [-d[0], -d[1], -d[2]]
    return (x: V3) => dot([x[0] - p[0], x[1] - p[1], x[2] - p[2]], d)
  })
  let body: V3[] = tri
  for (const f of inside) {
    fan(arris, clip(body, x => ARRIS - f(x)))
    body = clip(body, x => f(x) - ARRIS)
  }
  for (const { a, b, part } of cuts) {
    const piece = clip(clip(body, x => u(x) - a), x => b - u(x))
    if (piece.length >= 3) fan(part, piece)
  }
}

const UP_STRIPES = [-20, -10, 10, 20], DOWN_STRIPES = [-11, 11]
for (let k = 0; k < 4; k++) {
  const b0 = B[k], b1 = B[(k + 1) % 4], a0 = A[k], a1 = A[(k + 1) % 4]
  // Upright triangle on the podium edge, apex at a top corner.
  const m = inward(midpoint(b0, b1)), along = unit([b1[0] - b0[0], b1[1] - b0[1], 0])
  facet([b0, m, a0], midpoint(b0, b1), along, UP_STRIPES, [[b0, a0]])
  facet([m, b1, a0], midpoint(b0, b1), along, UP_STRIPES, [[b1, a0]])
  // Inverted triangle hanging from the top edge to the base corner.
  const t = inward(midpoint(a0, a1)), down = unit([a1[0] - a0[0], a1[1] - a0[1], 0])
  facet([b1, a1, t], midpoint(a0, a1), down, DOWN_STRIPES, [[b1, a1]])
  facet([b1, t, a0], midpoint(a0, a1), down, DOWN_STRIPES, [[b1, a0]])
}

// ---- Parapet, roof, ring and spire -------------------------------------------
// The top square rises straight as a glass parapet with a stainless coping.
const top: V3[] = []
for (let k = 0; k < 4; k++) top.push(A[k], inward(midpoint(A[k], A[(k + 1) % 4])))
const lift = (r: V3[], z: number) => r.map(([x, y]): V3 => [x, y, z])
const PC = ZR - .8
glass.loft([lift(top, Z1), lift(top, PC)])
const shrink = (r: V3[], d: number) => r.map(([x, y, z]): V3 => { const l = Math.hypot(x, y); return [x - x / l * d * Math.SQRT2, y - y / l * d * Math.SQRT2, z] })
// Corners sit on the axes here, so the inset along the bisector is d·√2;
// the creased midpoints are near enough to use the same.
metal.loft([lift(top, PC), lift(shrink(top, .45), ZR)])
const roofRing = lift(shrink(top, .9), ZR - .5)
metal.loft([lift(shrink(top, .45), ZR), roofRing])
fan(roof, roofRing)

function cylinder(p: Part, sections: [number, number][], segments = 16, capTop = true) {
  const rings = sections.map(([z, r]) => Array.from({ length: segments }, (_, i): V3 => {
    const a = (i + .5) * 2 * Math.PI / segments
    return [r * Math.cos(a), r * Math.sin(a), z]
  }))
  // Smooth round sides.
  for (let k = 0; k < rings.length - 1; k++) for (let i = 0; i < segments; i++) {
    const j = (i + 1) % segments
    const n = (v: V3): V3 => unit([v[0], v[1], 0])
    quadN(p, rings[k][i], rings[k][j], rings[k + 1][j], rings[k + 1][i], n(rings[k][i]), n(rings[k][j]))
  }
  if (capTop) fan(p, rings.at(-1)!)
}
/** The spire's circular base ring: a dark annulus on the roof. */
function annulus(p: Part, z0: number, z1: number, outer: number, inner: number, segments = 16) {
  const r = (rad: number, z: number) => Array.from({ length: segments }, (_, i): V3 => {
    const a = (i + .5) * 2 * Math.PI / segments
    return [rad * Math.cos(a), rad * Math.sin(a), z]
  })
  const o0 = r(outer, z0), o1 = r(outer, z1), i0 = r(inner, z0), i1 = r(inner, z1)
  for (let i = 0; i < segments; i++) {
    const j = (i + 1) % segments
    const n = (v: V3): V3 => unit([v[0], v[1], 0]), m = (v: V3): V3 => unit([-v[0], -v[1], 0])
    quadN(p, o0[i], o0[j], o1[j], o1[i], n(o0[i]), n(o0[j]))
    quadN(p, i0[j], i0[i], i1[i], i1[j], m(i0[j]), m(i0[i]))
    p.quad(o1[i], o1[j], i1[j], i1[i])
  }
}
annulus(ring, ZR - .5, ZR + 4, 17.7, 15.4)
annulus(metal, ZR + 4, ZR + 4.6, 17.9, 15.2) // pale lip that catches the sun
// Spire: a stocky base, a long tapering mast with pale platform collars, a
// pale beacon tip. Real lattice and stays are too fine to read.
cylinder(ring, [[ZR - .5, 5.2], [ZR + 7, 5.2], [ZR + 9, 3.4]], 12)
cylinder(spire, [[ZR + 9, 2.9], [470, 2.4], [471, 1.9], [520, 1.3], [530, .9]], 10, false)
for (const [z, r] of [[440, 3.4], [470, 3.0], [500, 2.2]] as [number, number][])
  cylinder(metal, [[z - .9, r * .7], [z - .9, r], [z + .9, r], [z + .9, r * .7]], 10)
cylinder(metal, [[530, .9], [537, .55], [541, .12]], 8)

// sRGB colours from daylight photos.
const parts = [
  { part: glass, material: { name: 'blue-grey-glass', color: 0x6180a6, roughness: .4 } },
  { part: mullion, material: { name: 'mullion-bands', color: 0x8099b6, roughness: .45 } },
  { part: arris, material: { name: 'stainless-arris', color: 0xbfc8d1, roughness: .4 } },
  { part: wall, material: { name: 'podium-core', color: 0x858d93 } },
  { part: fins, material: { name: 'podium-glass-fins', color: 0xc6cacc, roughness: .6 } },
  { part: metal, material: { name: 'stainless', color: 0xc3c9ce, roughness: .5 } },
  { part: roof, material: { name: 'roof', color: 0xc8968a } },
  { part: ring, material: { name: 'spire-ring', color: 0x464c53 } },
  { part: spire, material: { name: 'spire-mast', color: 0x5b6168 } },
]
const triangles = parts.reduce((n, { part }) => n + part.triangles, 0)
if (triangles > 5000) throw new Error(`Triangle budget exceeded: ${triangles}`)
const glb = writeGlb('One World Trade Center', parts, {
  license: 'CC0-1.0', bearing: 27.4, elevation: 0, anchor: [40.713, -74.0131893], height: 541,
  frame: 'Y up, -Z north, +X east, metres; origin at the ground anchor',
  note: 'Octagonal antiprism tower on a finned podium; ring and spire from OSM parts',
})
if (glb.length > 256000) throw new Error(`File budget exceeded: ${glb.length}`)
const out = new URL('../../landmarks/models/one-world-trade-center.glb', import.meta.url).pathname
await Bun.write(out, glb)
console.log(`${out}: ${triangles} triangles, ${glb.length} bytes (${(glb.length / 1024).toFixed(1)} KiB)`)
