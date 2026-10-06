/**
 * Solomon R. Guggenheim Museum — original procedural geometry, CC0-1.0.
 * Run: bun scripts/landmarks/guggenheim-museum.ts
 *
 * Map frame: x = v (east, into the block), y = u (uptown, along Fifth
 * Avenue), z = metres up. Catalog placement: 40.7829932, -73.958925;
 * bearing 29°, elevation 0. The OSM envelope (footprint, bounds) is fixed and
 * checked below; the masses inside it are stylised from photographs.
 *
 * The rotunda is the hero: one continuous helix of off-white bands, each
 * leaning out and overhanging the one below, with rounded lips and dark
 * charcoal reveals between them, capped by a horizontal parapet. The helix
 * is clipped between the ground-floor band and the parapet, so it emerges
 * and disappears as a tapering wedge, with no end caps.
 */
import { Part, writeGlb, type V3 } from './mesh'

type XY = [number, number]
/** A profile point in (radius, height) with its 2D normal; `m` paints the edge leaving it. */
type Pt = { r: number; z: number; nr: number; nz: number; m?: Part }

const concrete = new Part(), limestone = new Part(), charcoal = new Part()
const glass = new Part(), metal = new Part(), roof = new Part()
const TAU = Math.PI * 2
const MAIN: XY = [0.2, -10.8]
const MONITOR: XY = [-5.7, 21.3]
const SEG = 40 // around the rotunda; a multiple of four so the rim meets its envelope

const unit = (x: number, y: number): [number, number] => { const l = Math.hypot(x, y) || 1; return [x / l, y / l] }
const clamp = (v: number, a: number, b: number) => Math.min(b, Math.max(a, v))

/** A triangle with explicit smooth normals; slivers left by a collapsed profile are dropped. */
function tri(p: Part, a: V3, b: V3, c: V3, na: V3, nb: V3, nc: V3) {
  const ux = b[0] - a[0], uy = b[1] - a[1], uz = b[2] - a[2]
  const vx = c[0] - a[0], vy = c[1] - a[1], vz = c[2] - a[2]
  if (Math.hypot(uy * vz - uz * vy, uz * vx - ux * vz, ux * vy - uy * vx) < 1e-7) return
  p.tri(a, b, c, undefined, undefined, undefined, [na, nb, nc])
}
function quad(p: Part, a: V3, b: V3, c: V3, d: V3, na: V3, nb: V3, nc: V3, nd: V3) {
  tri(p, a, b, c, na, nb, nc)
  tri(p, a, c, d, na, nc, nd)
}

/**
 * Sweep profiles around a vertical axis. Each section is a profile at an
 * angle; profiles run inner-bottom → out → up → in, so faces point outwards.
 */
function sweep([cx, cy]: XY, sections: { a: number; pts: Pt[] }[], fallback = concrete) {
  const P = (a: number, q: Pt): V3 => [cx + q.r * Math.cos(a), cy + q.r * Math.sin(a), q.z]
  const N = (a: number, q: Pt): V3 => [q.nr * Math.cos(a), q.nr * Math.sin(a), q.nz]
  for (let s = 0; s < sections.length - 1; s++) {
    const A = sections[s], B = sections[s + 1]
    for (let k = 0; k < A.pts.length - 1; k++) {
      const part = A.pts[k].m ?? fallback
      quad(part, P(A.a, A.pts[k]), P(B.a, B.pts[k]), P(B.a, B.pts[k + 1]), P(A.a, A.pts[k + 1]),
        N(A.a, A.pts[k]), N(B.a, B.pts[k]), N(B.a, B.pts[k + 1]), N(A.a, A.pts[k + 1]))
    }
  }
}
function revolve(c: XY, pts: Pt[], segs = SEG, phase = 0) {
  sweep(c, Array.from({ length: segs + 1 }, (_, i) => ({ a: phase + i / segs * TAU, pts })))
}
/** Profile points along a quarter round: centre (cr, cz), from angle f0 to f1 (degrees). */
function arc(cr: number, cz: number, rad: number, f0: number, f1: number, steps: number, m?: Part): Pt[] {
  return Array.from({ length: steps + 1 }, (_, i) => {
    const f = (f0 + (f1 - f0) * i / steps) * Math.PI / 180
    return { r: cr + rad * Math.cos(f), z: cz + rad * Math.sin(f), nr: Math.cos(f), nz: Math.sin(f), m }
  })
}
const pt = (r: number, z: number, nr: number, nz: number, m?: Part): Pt => {
  const [a, b] = unit(nr, nz)
  return { r, z, nr: a, nz: b, m }
}
/** A plain drum with rounded lips; `top`, if given, paints its flat top. */
function drum(c: XY, z0: number, z1: number, r0: number, r1: number, lip: number, wall: Part, top: Part | null, segs: number) {
  const lean = (r1 - r0) / (z1 - z0)
  const face = (z: number) => r0 + lean * (z - z0)
  const [fn, fz] = unit(1, -lean)
  const lo = Math.min(lip, (z1 - z0) / 3)
  const rim = arc(face(z1 - lo) - lo, z1 - lo, lo, 45, 90, 1, wall)
  const pts: Pt[] = [
    ...arc(face(z0 + lo) - lo, z0 + lo, lo, -90, -45, 1, wall),
    { r: face(z0 + lo), z: z0 + lo, nr: fn, nz: fz, m: wall },
    { r: face(z1 - lo), z: z1 - lo, nr: fn, nz: fz, m: wall },
    ...(top ? [rim[0], { ...rim[1], m: top }, pt(0, z1, 0, 1)] : rim),
  ]
  revolve(c, pts, segs)
}

// ---------------------------------------------------------------- rotunda
const zF = 9.4          // top of the ground-floor band: where the spiral emerges
const zRim = 27.1       // the parapet's top, and the envelope's rim
const zP = 22.6         // the parapet's horizontal underside
const RV = 1.1          // reveal height
const zC = zP - RV      // the spiral is clipped here, under the parapet's reveal
const PITCH = 4.1       // one band plus one reveal per turn, as on Fifth Avenue
const BAND = PITCH - RV
const LEAN = 0.16       // each band leans out this much per metre of height...
const SPREAD = 0.178    // ...but the spiral widens faster, so each band overhangs the last
const RIM = 17.3
const LIP_LO = 0.5, LIP_HI = 0.32
const Rb = (z: number) => 14.3 + SPREAD * (z - zF)   // a band's face at its own bottom
const core = (z: number) => Rb(z) - 1.05             // the recessed charcoal drum behind the reveals
const [FN, FZ] = unit(1, -LEAN)

/** One section of the spiral band whose unclipped bottom is at `lo`. */
function bandSection(lo: number): Pt[] {
  const b = clamp(lo, zF, zC), t = clamp(lo + BAND, zF, zC), h = t - b
  const face = (z: number) => Rb(lo) + LEAN * (z - lo)
  // Clipping at the ground band cuts through the rounded lip, not under it.
  const rb = Math.min(Math.max(0, LIP_LO - (b - lo)), h * 0.4)
  const rt = Math.min(LIP_HI, h * 0.4)
  return [
    pt(core(b) - 0.4, b, 0, -1, charcoal),
    ...arc(face(b + rb) - rb, b + rb, rb, -90, -45, 1, concrete),
    { r: face(b + rb), z: b + rb, nr: FN, nz: FZ, m: concrete },
    { r: face(t - rt), z: t - rt, nr: FN, nz: FZ, m: concrete },
    ...arc(face(t - rt) - rt, t - rt, rt, 45, 90, 1, concrete).map((q, i) => i === 1 ? { ...q, m: charcoal } : q),
    pt(core(t) - 0.4, t, 0, 1),
  ]
}
// Start where the first band's top just reaches the ground band, end where
// the last band's bottom reaches the parapet: both ends have zero height.
const lo0 = zF - BAND, turns = (zC - lo0) / PITCH
const PHASE = 200 * Math.PI / 180 // where the spiral starts, so it wedges out at the back
const steps = Math.ceil(turns * 32) // 11.25° a step: smooth at phone sizes, and within budget
sweep(MAIN, Array.from({ length: steps + 1 }, (_, i) => {
  const s = i / steps * turns
  return { a: PHASE + s * TAU, pts: bandSection(lo0 + s * PITCH) }
}))
// The dark drum seen through the reveals, and the plain base under the spiral.
revolve(MAIN, [pt(core(zF - 0.3), zF - 0.3, 1, -SPREAD, charcoal), pt(core(zP + 0.2), zP + 0.2, 1, -SPREAD)], 32)
revolve(MAIN, [pt(13.0, 0, 1, 0), pt(13.0, zF + 0.05, 1, 0)], 24)

// Horizontal parapet band, roof ring and the shallow twelve-sided skylight.
const Rp = (z: number) => RIM - LEAN * (zRim - LIP_HI - z)
const ROOF = 26.6, CURB = 14.0
revolve(MAIN, [
  pt(core(zP) - 0.4, zP, 0, -1, charcoal),
  ...arc(Rp(zP + LIP_LO) - LIP_LO, zP + LIP_LO, LIP_LO, -90, -45, 1, concrete),
  { r: Rp(zP + LIP_LO), z: zP + LIP_LO, nr: FN, nz: FZ, m: concrete },
  { r: RIM, z: zRim - LIP_HI, nr: FN, nz: FZ, m: concrete },
  ...arc(RIM - LIP_HI, zRim - LIP_HI, LIP_HI, 45, 90, 1, concrete),
  pt(16.0, zRim, 0, 1), pt(16.0, zRim, -1, 0), pt(16.0, ROOF, -1, 0),
  pt(16.0, ROOF, 0, 1, roof), pt(CURB, ROOF, 0, 1), pt(CURB, ROOF, 1, 0, concrete),
  pt(CURB, 26.95, 1, 0), pt(CURB, 26.95, 0, 1, concrete), pt(CURB - 0.3, 26.95, 0, 1),
])
{
  const D = 12
  const skirt: XY[] = [[CURB - 0.3, 26.95], [12.4, 28.0], [10.6, 28.7]]
  const cap: XY[] = [[10.6, 28.7], [7.4, 29.9], [3.6, 30.6], [0, 30.8]]
  const ring = ([r, z]: XY, i: number): V3 => {
    const a = (i + 0.5) / D * TAU
    return [MAIN[0] + r * Math.cos(a), MAIN[1] + r * Math.sin(a), z]
  }
  for (const [prof, part] of [[skirt, glass], [cap, metal]] as const)
    for (let k = 0; k < prof.length - 1; k++)
      for (let i = 0; i < D; i++)
        part.quad(ring(prof[k], i), ring(prof[k], i + 1), ring(prof[k + 1], i + 1), ring(prof[k + 1], i))
}

// ------------------------------------------------------------ plan prisms
/** Offset a counter-clockwise ring inward along each vertex's bisector. */
function inset(ring: XY[], d: number): XY[] {
  return ring.map((b, i) => {
    const a = ring[(i + ring.length - 1) % ring.length], c = ring[(i + 1) % ring.length]
    const u = unit(b[1] - a[1], a[0] - b[0]), v = unit(c[1] - b[1], b[0] - c[0])
    const k = 1 + u[0] * v[0] + u[1] * v[1]
    return [b[0] - (u[0] + v[0]) * d / k, b[1] - (u[1] + v[1]) * d / k]
  })
}
/** Ear-clipping triangulation of a simple counter-clockwise ring. */
function fill(p: Part, ring: XY[], z: number, up = true) {
  const idx = ring.map((_, i) => i)
  const cross = (o: XY, a: XY, b: XY) => (a[0] - o[0]) * (b[1] - o[1]) - (a[1] - o[1]) * (b[0] - o[0])
  const n: V3 = [0, 0, up ? 1 : -1]
  const emit = (a: XY, b: XY, c: XY) => up
    ? tri(p, [a[0], a[1], z], [b[0], b[1], z], [c[0], c[1], z], n, n, n)
    : tri(p, [a[0], a[1], z], [c[0], c[1], z], [b[0], b[1], z], n, n, n)
  let guard = 0
  while (idx.length > 3 && guard++ < 10000) {
    for (let i = 0; i < idx.length; i++) {
      const a = ring[idx[(i + idx.length - 1) % idx.length]], b = ring[idx[i]], c = ring[idx[(i + 1) % idx.length]]
      if (cross(a, b, c) <= 1e-9) continue
      const inside = idx.some(j => {
        const q = ring[j]
        if (q === a || q === b || q === c) return false
        return cross(a, b, q) >= 0 && cross(b, c, q) >= 0 && cross(c, a, q) >= 0
      })
      if (inside) continue
      emit(a, b, c)
      idx.splice(i, 1)
      break
    }
  }
  if (idx.length === 3) emit(ring[idx[0]], ring[idx[1]], ring[idx[2]])
}
/**
 * Extrude a plan ring with rounded lips. Corners sharper than 35° keep crisp
 * normals so flat walls stay flat; arcs in the plan shade as curves.
 */
function prism(ring: XY[], z0: number, z1: number, lipLo: number, lipHi: number, wall: Part, top: Part | null, bottom = false) {
  const n = ring.length
  const edgeN = ring.map((a, i) => { const b = ring[(i + 1) % n]; return unit(b[1] - a[1], a[0] - b[0]) })
  const levels: { d: number; z: number; h: number; v: number }[] = []
  const lip = (r: number, zc: number, f0: number, f1: number) => {
    for (let i = 0; i <= 2; i++) {
      const f = (f0 + (f1 - f0) * i / 2) * Math.PI / 180
      levels.push({ d: r * (1 - Math.cos(f)), z: zc + r * Math.sin(f), h: Math.cos(f), v: Math.sin(f) })
    }
  }
  lip(lipLo, z0 + lipLo, -90, 0)
  lip(lipHi, z1 - lipHi, 0, 90)
  const rings = levels.map(l => l.d ? inset(ring, l.d) : ring)
  for (let i = 0; i < n; i++) {
    const j = (i + 1) % n
    const ends = [i, j].map(v => {
      const prev = edgeN[(v + n - 1) % n], next = edgeN[v]
      const smooth = prev[0] * next[0] + prev[1] * next[1] > Math.cos(35 * Math.PI / 180)
      return smooth ? unit(prev[0] + next[0], prev[1] + next[1]) : edgeN[i]
    })
    for (let k = 0; k < levels.length - 1; k++) {
      const L0 = levels[k], L1 = levels[k + 1]
      const nrm = (e: [number, number], L: typeof L0): V3 => [e[0] * L.h, e[1] * L.h, L.v]
      quad(wall,
        [rings[k][i][0], rings[k][i][1], L0.z], [rings[k][j][0], rings[k][j][1], L0.z],
        [rings[k + 1][j][0], rings[k + 1][j][1], L1.z], [rings[k + 1][i][0], rings[k + 1][i][1], L1.z],
        nrm(ends[0], L0), nrm(ends[1], L0), nrm(ends[1], L1), nrm(ends[0], L1))
    }
  }
  fill(top ?? wall, rings[rings.length - 1], z1)
  if (bottom) fill(wall, rings[0], z0, false)
}
const arcXY = (c: XY, r: number, a0: number, a1: number, steps: number): XY[] =>
  Array.from({ length: steps + 1 }, (_, i) => {
    const a = (a0 + (a1 - a0) * i / steps) * Math.PI / 180
    return [c[0] + r * Math.cos(a), c[1] + r * Math.sin(a)]
  })

// ------------------------------------------------- ground-floor band, 5th Ave
// One long white band along the avenue, from the Monitor to the rotunda,
// bulging round the rotunda's base and ending in a rounded nose on 88th St.
{
  const OUT = 16.6, IN = 12.5, END = 280
  const mid = (OUT + IN) / 2, half = (OUT - IN) / 2
  const endC: XY = [MAIN[0] + mid * Math.cos(END * Math.PI / 180), MAIN[1] + mid * Math.sin(END * Math.PI / 180)]
  const joinA = Math.asin((-2 - MAIN[1]) / IN) * 180 / Math.PI // where the inner arc meets y = -2
  const band: XY[] = [
    [7.3, -2], [7.3, 27], [-5.7, 27], [-16.4, MONITOR[1]],
    ...arcXY(MAIN, OUT, 180, END, 10),
    ...arcXY(endC, half, END, END + 180, 6).slice(1, -1),
    ...arcXY(MAIN, IN, END, 180 - joinA, 14),
  ]
  // Under it, the recessed lobby glazing in the band's shade.
  prism(inset(band, 2.0), 0, 3.85, 0, 0, glass, glass)
  prism(band, 3.8, zF, 0.6, 0.4, concrete, roof, true)
}

// --------------------------------------------------------------- Monitor
// A recessed white base, a lower band (one with the avenue band), a glass ribbon, a wider band that
// leans out over it, a dark clerestory and a thin charcoal roof slab.
drum(MONITOR, 0, 3.85, 8.6, 8.6, 0, concrete, null, 32)
drum(MONITOR, 3.8, zF, 10.7, 10.7, 0.5, concrete, roof, 32)
drum(MONITOR, zF, 11.0, 9.7, 9.7, 0, glass, null, 32)
drum(MONITOR, 11.0, 14.3, 10.0, 11.4, 0.4, concrete, roof, 32)
drum(MONITOR, 14.3, 15.3, 9.5, 9.5, 0, glass, null, 32)
drum(MONITOR, 15.3, 15.9, 10.3, 10.3, 0.2, charcoal, roof, 32)

// -------------------------------------------------- rear wing, service core
const rect = (x0: number, y0: number, x1: number, y1: number, c = 0.5): XY[] =>
  [[x0 + c, y0], [x1 - c, y0], [x1, y0 + c], [x1, y1 - c], [x1 - c, y1], [x0 + c, y1], [x0, y1 - c], [x0, y0 + c]]
prism(rect(6.5, -28.1, 21.9, 3.8, 0.6), 0, 7.4, 0, 0.45, concrete, roof)
// The white stair-and-lift core that stands on the rotunda's north-east rim.
prism(rect(2.6, -1.8, 8.8, 4.2, 0.5), zF, 32.6, 0, 0.45, concrete, roof)

// ------------------------------------------------------- Gwathmey annex
// Plain buff limestone, chamfered corners, a few broad recessed window bands.
{
  const X0 = 7.3, X1 = 21.9, Y0 = 3.8, Y1 = 32.9, TOP = 41.6, LIP = 0.45, C = 0.5, DEPTH = 0.65
  const zW = TOP - LIP
  type Rect = [number, number, number, number] // s0, s1, z0, z1 along the face
  /** A flat wall from `a` to `b` (counter-clockwise), with recessed glazed bands cut into it. */
  function wall(a: XY, b: XY, rects: Rect[]) {
    const L = Math.hypot(b[0] - a[0], b[1] - a[1]), d = unit(b[0] - a[0], b[1] - a[1]), nn: V3 = [d[1], -d[0], 0]
    const v = (s: number, z: number, depth = 0): V3 => [a[0] + d[0] * s - nn[0] * depth, a[1] + d[1] * s - nn[1] * depth, z]
    const ss = [...new Set([0, L, ...rects.flatMap(r => [r[0], r[1]])])].sort((p, q) => p - q)
    const zs = [...new Set([0, zW, ...rects.flatMap(r => [r[2], r[3]])])].sort((p, q) => p - q)
    for (let i = 0; i < ss.length - 1; i++) for (let k = 0; k < zs.length - 1; k++) {
      const sm = (ss[i] + ss[i + 1]) / 2, zm = (zs[k] + zs[k + 1]) / 2
      if (rects.some(([s0, s1, z0, z1]) => sm > s0 && sm < s1 && zm > z0 && zm < z1)) continue
      quad(limestone, v(ss[i], zs[k]), v(ss[i + 1], zs[k]), v(ss[i + 1], zs[k + 1]), v(ss[i], zs[k + 1]), nn, nn, nn, nn)
    }
    const up: V3 = [0, 0, 1], dn: V3 = [0, 0, -1], along: V3 = [d[0], d[1], 0], back: V3 = [-d[0], -d[1], 0]
    for (const [s0, s1, z0, z1] of rects) {
      quad(glass, v(s0, z0, DEPTH), v(s1, z0, DEPTH), v(s1, z1, DEPTH), v(s0, z1, DEPTH), nn, nn, nn, nn)
      quad(limestone, v(s0, z0), v(s1, z0), v(s1, z0, DEPTH), v(s0, z0, DEPTH), up, up, up, up)
      quad(limestone, v(s0, z1, DEPTH), v(s1, z1, DEPTH), v(s1, z1), v(s0, z1), dn, dn, dn, dn)
      quad(limestone, v(s0, z0), v(s0, z0, DEPTH), v(s0, z1, DEPTH), v(s0, z1), along, along, along, along)
      quad(limestone, v(s1, z0, DEPTH), v(s1, z0), v(s1, z1), v(s1, z1, DEPTH), back, back, back, back)
    }
  }
  // Fifth Avenue (west) face: a stack of gallery ribbons above the Monitor,
  // and one tall slot towards 89th Street, as in the aerial photographs.
  const ribbons = (s0: number, s1: number, zs: number[], h = 1.9): Rect[] => zs.map(z => [s0, s1, z, z + h])
  wall([X0, Y1 - C], [X0, Y0 + C], [...ribbons(8.5, 21.5, [18.6, 23.6, 28.6], 2.6), [2.6, 5.6, 17.5, 37.5]])
  // South face, over the rotunda: two broad bands near the top.
  wall([X0 + C, Y0], [X1 - C, Y0], ribbons(1.6, 12, [30.4, 35.6], 2.2))
  // East and north faces, to the neighbours and 89th Street.
  wall([X1, Y0 + C], [X1, Y1 - C], ribbons(3.5, 24.5, [16.5, 23.5, 30.5], 2.2))
  wall([X1 - C, Y1], [X0 + C, Y1], ribbons(1.8, 11.8, [12.5, 20.5, 28.5], 2.2))
  // Chamfered corners, then a rounded coping and a grey roof.
  const ring = rect(X0, Y0, X1, Y1, C)
  for (const i of [1, 3, 5, 7]) {
    const a = ring[i], b = ring[(i + 1) % 8]
    // Normals blend from the face that ends at `a` to the one that starts at `b`.
    const faceN = (e: number): V3 => {
      const p = ring[e % 8], q = ring[(e + 1) % 8], [x, y] = unit(q[1] - p[1], p[0] - q[0])
      return [x, y, 0]
    }
    const m0 = faceN(i + 7), m1 = faceN(i + 1)
    quad(limestone, [a[0], a[1], 0], [b[0], b[1], 0], [b[0], b[1], zW], [a[0], a[1], zW], m0, m1, m1, m0)
  }
  prism(ring, zW - 0.001, TOP, 0, LIP, limestone, roof)
}

// --------------------------------------------------------------- output
const parts = [
  // Off-white paint: the sunlit band in 02/03, without their sunset and overcast casts.
  { part: concrete, material: { name: 'off-white concrete', color: 0xe3e0d6, roughness: 0.8 } },
  // Buff limestone: annex against the bands in p4 (≈0.88 of the white, warmer).
  { part: limestone, material: { name: 'buff limestone', color: 0xcec6b2 } },
  // Reveals and the shade under the avenue band: 03 at 880,192.
  { part: charcoal, material: { name: 'charcoal reveals', color: 0x3d3e3a } },
  { part: glass, material: { name: 'window', color: 0x6e7c86, roughness: 0.5 } },
  { part: metal, material: { name: 'light grey skylight cap', color: 0xb4b9ba, roughness: 0.55 } },
  { part: roof, material: { name: 'roofs', color: 0x9a9893 } },
]
const triangles = parts.reduce((n, { part }) => n + part.triangles, 0)
if (triangles > 5000) throw new Error(`Triangle budget exceeded: ${triangles}`)
// The placement is fixed: the authored geometry must keep the OSM envelope.
const min = [Infinity, Infinity, Infinity], max = [-Infinity, -Infinity, -Infinity]
for (const { part } of parts) for (let i = 0; i < part.pos.length; i++) {
  if (!Number.isFinite(part.pos[i])) throw new Error('Non-finite vertex')
  min[i % 3] = Math.min(min[i % 3], part.pos[i])
  max[i % 3] = Math.max(max[i % 3], part.pos[i])
}
// glTF is (v, height, -u), so the north/south bounds swap signs.
const expected = [[-17.1, 0, -32.9], [21.9, 41.6, 28.1]]
for (let axis = 0; axis < 3; axis++)
  if (Math.abs(min[axis] - expected[0][axis]) > 1e-4 || Math.abs(max[axis] - expected[1][axis]) > 1e-4)
    throw new Error(`OSM envelope mismatch: ${JSON.stringify({ min, max })}`)
const glb = writeGlb('Solomon R. Guggenheim Museum', parts, {
  license: 'CC0-1.0', bearing: 29, elevation: 0,
  frame: 'Y up, -Z north, +X east, metres; origin at the anchor on the ground',
  footprint: { v: [-17.1, 21.9], u: [-28.1, 32.9] },
  note: 'Spiral rotunda, Monitor and avenue band by Wright; Gwathmey Siegel annex behind. Massing stylised within the OSM envelope.',
})
if (glb.length > 256000) throw new Error(`File budget exceeded: ${glb.length}`)
const out = new URL('../../landmarks/models/guggenheim-museum.glb', import.meta.url).pathname
await Bun.write(out, glb)
console.log(`${out}: ${triangles} triangles, ${glb.length} bytes (${(glb.length / 1024).toFixed(1)} KiB)`)
