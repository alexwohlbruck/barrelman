/**
 * AvidXchange Music Factory: the mill — procedural, CC0-1.0.
 * bun scripts/landmarks/music-factory-mill.ts
 *
 * The Music Factory is the John B. Ross and Company Mill (1904–c.1960), later
 * the Southern Asbestos Manufacturing Company plant, at the corner of Hamilton
 * Street and Seaboard Street (now NC Music Factory Boulevard). It is two
 * red-brick mills set in an inverted V round a courtyard that opens south
 * onto the boulevard, joined at the courtyard's north end by a bridge
 * section (Charlotte-Mecklenburg Historic Landmarks Commission survey).
 *
 * - Mill #2, the west arm along Hamilton Street, holds The Fillmore Charlotte
 *   and The Underground: `fillmore-charlotte.ts`.
 * - This model is Mill #1, the east arm, with the bridge between them. Its
 *   1904 core is one storey on the courtyard with a low gable roof and a
 *   deep eave, segmental-arched windows in a long row; 1920s–1950s additions
 *   wrap it in two-storey flat-roofed brick, with the c.1955 dust-collector
 *   room and its two metal dust towers in the middle of the east side. The
 *   bridge carries a tall silver metal-clad tower (the AvidXchange sign
 *   tower) and, on its courtyard face, the courtyard stage under a canopy.
 * - The amphitheatre north of the mill is `music-factory-amphitheatre.ts`.
 *
 * OSM: way/957549179 is Mill #1 (tagged there, wrongly, with The Fillmore's
 * name and address); way/414800516 is an older outline over both mills and
 * the bridge, which `fillmore-charlotte` lists, so the two models must be
 * shown together; way/1202433658 is the courtyard stage canopy. None has a
 * height. Heights come from the survey's storey counts, Mapillary street
 * panoramas and the 2009 and 2018 Commons photos, with the ground from the
 * terrarium DEM (the site falls ~7 m from the boulevard to the north end).
 *
 * This file also holds the small massing kit the three Music Factory
 * generators share (site frame, DEM, zoned prisms with eaves or bevelled
 * parapets, arched window panels). Its own model is built only when it is
 * run directly.
 *
 * Map frame: x east, y north, z up, metres; bearing 0. Everything is drawn in
 * a site frame round lng −80.8450, lat 35.2390 at absolute elevation, then
 * moved so each model's origin is its outline's centroid on its lowest ground.
 */
import { Part, writeGlb, type V3 } from './mesh'
import { PALETTE, finish, type Swatch } from './palette'

// ---------------------------------------------------------------------------
// Site frame and ground.

export type XY = [number, number]
export const ORIGIN = { lng: -80.845, lat: 35.239 }
const KX = 111320 * Math.cos((ORIGIN.lat * Math.PI) / 180), KY = 110574
export const toLngLat = (p: XY) => [+(ORIGIN.lng + p[0] / KX).toFixed(7), +(ORIGIN.lat + p[1] / KY).toFixed(7)]

/** Terrarium z15 DEM, 10 m grid: x −110…70, y 170 (first row) … −30. */
const DEM = `212.4 211.7 211.6 210.8 210.8 210.1 209.2 209.0 208.9 209.0 209.2 209.3 209.2 209.7 210.1 210.2 210.4 210.0 209.6
212.8 212.4 211.6 211.3 211.2 210.8 209.5 208.9 209.2 209.4 209.4 209.2 209.4 210.3 210.5 210.8 210.7 210.5 210.3
212.0 211.8 211.7 211.8 211.5 211.3 210.3 209.9 209.6 209.6 209.6 209.7 209.9 210.8 211.0 211.2 211.3 211.0 211.0
212.2 212.0 212.0 212.0 211.8 211.4 210.6 210.3 209.9 209.8 209.7 209.8 209.8 211.1 211.3 211.4 211.6 211.3 211.5
213.8 212.5 212.6 212.4 212.1 211.7 211.1 210.7 210.3 210.0 209.8 209.8 210.3 211.3 211.7 212.0 211.9 212.1 212.1
214.0 212.8 213.0 212.8 212.2 211.9 211.3 211.0 210.5 210.2 209.9 209.8 210.7 211.4 212.0 212.2 212.3 212.6 212.8
213.9 213.4 213.5 212.9 212.4 212.2 211.7 211.4 210.9 210.5 209.9 209.9 211.0 212.2 212.3 212.7 213.1 213.2 214.4
214.0 214.2 213.9 213.1 212.7 212.5 212.1 211.9 211.4 211.0 210.7 210.9 212.0 212.7 212.9 213.2 213.5 214.1 215.1
214.3 214.6 214.1 213.1 212.6 212.6 212.5 212.3 212.0 211.6 212.2 212.1 212.6 212.8 213.1 213.3 213.7 214.3 214.9
214.8 215.0 214.3 213.4 212.8 212.9 213.0 213.0 213.2 213.4 213.5 213.2 213.3 213.2 213.4 213.6 213.7 214.4 214.7
215.2 215.2 214.5 213.7 213.3 213.3 213.4 213.4 213.6 213.7 213.6 213.4 213.3 213.5 213.6 213.8 214.1 214.5 214.7
215.6 215.0 214.8 214.0 213.9 213.9 214.0 214.0 214.0 214.0 213.8 213.5 213.4 213.6 213.9 214.4 214.7 214.9 214.7
215.6 215.0 214.9 214.2 214.2 214.3 214.4 214.5 214.5 214.6 214.0 213.5 213.5 213.6 213.9 214.2 214.6 215.5 215.7
215.7 215.2 214.9 214.5 214.9 215.0 215.2 215.3 215.4 215.2 214.4 214.0 213.5 213.7 213.9 214.1 214.1 215.1 216.0
216.0 215.6 215.4 215.5 215.7 215.9 216.1 216.2 216.0 215.7 215.0 214.6 214.1 213.9 213.9 214.0 214.1 214.5 215.9
216.4 216.0 215.9 216.2 216.3 216.4 216.7 216.5 216.5 216.3 215.5 215.0 214.4 214.1 213.9 214.0 214.1 214.5 215.8
217.1 217.1 216.9 216.9 216.9 217.0 217.1 217.0 216.9 216.7 216.0 215.5 214.8 214.3 214.1 214.1 213.9 215.4 215.9
217.1 217.2 217.2 217.3 217.3 217.1 217.1 217.1 217.0 216.9 216.4 215.9 215.1 214.4 214.1 213.9 214.2 215.8 215.8
215.8 216.7 217.2 217.3 217.3 217.3 217.2 217.2 217.2 217.1 216.9 216.8 215.6 214.3 214.1 214.1 215.4 215.7 215.7
217.7 216.9 216.3 216.2 216.9 217.1 217.6 217.5 217.4 217.4 217.3 217.6 217.2 216.1 215.7 215.7 215.8 215.7 215.6
219.1 218.9 218.4 217.0 216.6 216.3 216.9 217.2 217.6 217.5 217.4 217.3 217.3 216.9 216.5 216.0 215.7 215.6 215.6`
  .split('\n').map((r) => r.trim().split(/\s+/).map(Number))
/** Ground elevation at a site point, bilinear on the DEM grid. */
export function ground(p: XY): number {
  const fx = Math.min(17.999, Math.max(0, (p[0] + 110) / 10)), fy = Math.min(19.999, Math.max(0, (170 - p[1]) / 10))
  const i = Math.floor(fx), j = Math.floor(fy), u = fx - i, v = fy - j
  return (DEM[j][i] * (1 - u) + DEM[j][i + 1] * u) * (1 - v) + (DEM[j + 1][i] * (1 - u) + DEM[j + 1][i + 1] * u) * v
}

// ---------------------------------------------------------------------------
// Polygons.

const area2 = (P: XY[]) => P.reduce((s, p, i) => { const q = P[(i + 1) % P.length]; return s + p[0] * q[1] - q[0] * p[1] }, 0)
/** Counter-clockwise, without repeated or collinear corners. */
export function clean(P: XY[]): XY[] {
  let Q = P.filter((p, i) => { const q = P[(i + 1) % P.length]; return Math.hypot(p[0] - q[0], p[1] - q[1]) > 0.05 })
  if (area2(Q) < 0) Q = Q.reverse()
  for (let again = true; again && Q.length > 3;) {
    again = false
    for (let i = 0; i < Q.length; i++) {
      const a = Q[(i + Q.length - 1) % Q.length], b = Q[i], c = Q[(i + 1) % Q.length]
      const cr = (b[0] - a[0]) * (c[1] - b[1]) - (b[1] - a[1]) * (c[0] - b[0])
      const L = Math.hypot(c[0] - a[0], c[1] - a[1])
      if (Math.abs(cr) / Math.max(L, 1e-6) < 0.02) { Q.splice(i, 1); again = true; break }
    }
  }
  return Q
}
/** Keep the side of the line through `a` along `d` that lies to its left. */
export function clip(P: XY[], a: XY, d: XY): XY[] {
  const side = (p: XY) => d[0] * (p[1] - a[1]) - d[1] * (p[0] - a[0])
  const out: XY[] = []
  for (let i = 0; i < P.length; i++) {
    const p = P[i], q = P[(i + 1) % P.length], sp = side(p), sq = side(q)
    if (sp >= 0) out.push(p)
    if ((sp >= 0) !== (sq >= 0)) { const t = sp / (sp - sq); out.push([p[0] + (q[0] - p[0]) * t, p[1] + (q[1] - p[1]) * t]) }
  }
  return out
}
/** An axis frame: s along bearing `deg`, t to its right (east of north-up). */
export function axes(deg: number) {
  const b = (deg * Math.PI) / 180, u: XY = [Math.sin(b), Math.cos(b)], r: XY = [Math.cos(b), -Math.sin(b)]
  return {
    u, r,
    s: (p: XY) => p[0] * u[0] + p[1] * u[1],
    t: (p: XY) => p[0] * r[0] + p[1] * r[1],
    at: (s: number, t: number): XY => [u[0] * s + r[0] * t, u[1] * s + r[1] * t],
    /** The part of P with s0 ≤ s ≤ s1 and t0 ≤ t ≤ t1. */
    band(P: XY[], s0 = -1e4, s1 = 1e4, t0 = -1e4, t1 = 1e4): XY[] {
      let Q = P
      const L: XY = [-r[0], -r[1]]
      Q = clip(Q, this.at(s0, 0), r)      // keep s ≥ s0: moving along +r, left is +u
      Q = clip(Q, this.at(s1, 0), L)
      Q = clip(Q, this.at(0, t0), [-u[0], -u[1]])
      Q = clip(Q, this.at(0, t1), u)
      return Q.length >= 3 ? clean(Q) : []
    },
  }
}
export function inside(P: XY[], p: XY) {
  let c = false
  for (let i = 0, j = P.length - 1; i < P.length; j = i++) {
    const [xi, yi] = P[i], [xj, yj] = P[j]
    if ((yi > p[1]) !== (yj > p[1]) && p[0] < ((xj - xi) * (p[1] - yi)) / (yj - yi) + xi) c = !c
  }
  return c
}
export const centroid = (P: XY[]): XY => {
  const Q = clean(P); let a = 0, x = 0, y = 0
  for (let i = 0; i < Q.length; i++) {
    const p = Q[i], q = Q[(i + 1) % Q.length], c = p[0] * q[1] - q[0] * p[1]
    a += c; x += (p[0] + q[0]) * c; y += (p[1] + q[1]) * c
  }
  return [x / (3 * a), y / (3 * a)]
}
/** Lowest ground under a footprint: its corners and a 4 m grid inside. */
export function lowest(P: XY[]): number {
  let m = Math.min(...P.map(ground))
  const xs = P.map((p) => p[0]), ys = P.map((p) => p[1])
  for (let x = Math.min(...xs); x <= Math.max(...xs); x += 4)
    for (let y = Math.min(...ys); y <= Math.max(...ys); y += 4) if (inside(P, [x, y])) m = Math.min(m, ground([x, y]))
  return m
}

/** Ear-clipping triangulation of a simple counter-clockwise polygon. */
export function earcut(pts: XY[]): [number, number, number][] {
  const idx = pts.map((_, i) => i), out: [number, number, number][] = []
  const cz = (a: XY, b: XY, c: XY) => (b[0] - a[0]) * (c[1] - a[1]) - (b[1] - a[1]) * (c[0] - a[0])
  const tin = (p: XY, a: XY, b: XY, c: XY) => cz(a, b, p) > 1e-9 && cz(b, c, p) > 1e-9 && cz(c, a, p) > 1e-9
  for (let guard = 0; idx.length > 3 && guard < 10000; guard++) {
    let cut = false
    for (let k = 0; k < idx.length; k++) {
      const i0 = idx[(k + idx.length - 1) % idx.length], i1 = idx[k], i2 = idx[(k + 1) % idx.length]
      if (cz(pts[i0], pts[i1], pts[i2]) <= 1e-9) continue
      if (idx.some((j) => j !== i0 && j !== i1 && j !== i2 && tin(pts[j], pts[i0], pts[i1], pts[i2]))) continue
      out.push([i0, i1, i2]); idx.splice(k, 1); cut = true; break
    }
    if (!cut) break
  }
  if (idx.length === 3) out.push([idx[0], idx[1], idx[2]])
  return out
}

// ---------------------------------------------------------------------------
// Faces with explicit normals, winding fixed to agree.

const unit = (v: V3): V3 => { const l = Math.hypot(v[0], v[1], v[2]) || 1; return [v[0] / l, v[1] / l, v[2] / l] }
export function tri(p: Part, a: V3, b: V3, c: V3, n?: V3) {
  const e1: V3 = [b[0] - a[0], b[1] - a[1], b[2] - a[2]], e2: V3 = [c[0] - a[0], c[1] - a[1], c[2] - a[2]]
  const f: V3 = [e1[1] * e2[2] - e1[2] * e2[1], e1[2] * e2[0] - e1[0] * e2[2], e1[0] * e2[1] - e1[1] * e2[0]]
  if (Math.hypot(...f) < 1e-9) return
  const N = unit(n ?? f)
  if (f[0] * N[0] + f[1] * N[1] + f[2] * N[2] >= 0) p.tri(a, b, c, undefined, undefined, undefined, [N, N, N])
  else p.tri(a, c, b, undefined, undefined, undefined, [N, N, N])
}
export function quad(p: Part, a: V3, b: V3, c: V3, d: V3, n?: V3) { tri(p, a, b, c, n); tri(p, a, c, d, n) }
/** A closed box between two heights over a convex counter-clockwise ring. */
export function solid(p: Part, ring: XY[], z0: number, z1: number | ((q: XY) => number), top: Part = p) {
  const Z = typeof z1 === 'number' ? () => z1 : z1
  for (let i = 0; i < ring.length; i++) {
    const A = ring[i], B = ring[(i + 1) % ring.length]
    quad(p, [A[0], A[1], z0], [B[0], B[1], z0], [B[0], B[1], Z(B)], [A[0], A[1], Z(A)], [B[1] - A[1], A[0] - B[0], 0])
  }
  for (let i = 1; i < ring.length - 1; i++) {
    const P = (q: XY): V3 => [q[0], q[1], Z(q)]
    tri(top, P(ring[0]), P(ring[i]), P(ring[i + 1]), [0, 0, 1])
  }
}

// ---------------------------------------------------------------------------
// Zoned massing. A building is a set of zones that tile its footprint; each
// has a planar roof. Walls on the outline run from below ground to the roof;
// steps between zones get a wall where one roof stands above the next. Flat
// zones get a bevelled parapet edge; gabled zones a deep eave.

export type Mats = { wall: Part; roof: Part; eave: Part; win: Part }
export type Zone = {
  poly: XY[]
  /** Roof elevation at a point (planar within the zone). */
  z: (p: XY) => number
  /** Overhanging eave instead of a bevelled parapet. */
  eave?: boolean
  /** Window rows, absolute elevations [bottom, top], and their pitch. */
  rows?: [number, number][]
  pitch?: number
  winW?: number
  /** Square-headed (mid-century steel) windows rather than segmental arches. */
  square?: boolean
  wall?: Part
  roof?: Part
}
export const flatZ = (z: number) => () => z
/** A gable half: ridge at t = tc, falling `k` per metre away from it. */
export const gableZ = (ax: ReturnType<typeof axes>, tc: number, ridge: number, k: number) => (p: XY) => ridge - k * Math.abs(ax.t(p) - tc)

const BEVEL = 0.55, EAVE = 0.8, BURY = 3
/**
 * Build zones. `party` lists outline segments shared with a neighbouring
 * model, which get no windows.
 */
export function build(zones: Zone[], m: Mats, party: [XY, XY][] = []) {
  for (const zn of zones) zn.poly = clean(zn.poly)
  const zoneAt = (q: XY) => zones.find((zn) => inside(zn.poly, q))
  const onParty = (A: XY, B: XY) => party.some(([p, q]) => {
    const d = (r: XY) => { const L = Math.hypot(q[0] - p[0], q[1] - p[1]); return Math.abs((q[0] - p[0]) * (r[1] - p[1]) - (q[1] - p[1]) * (r[0] - p[0])) / L }
    return d(A) < 0.6 && d(B) < 0.6
  })
  for (const zn of zones) {
    const P = zn.poly, n = P.length, wall = zn.wall ?? m.wall, roof = zn.roof ?? m.roof
    // Classify each edge: outline (no zone beyond it) or a step to a neighbour.
    const edges = P.map((A, i) => {
      const B = P[(i + 1) % n], L = Math.hypot(B[0] - A[0], B[1] - A[1])
      const o: XY = [(B[1] - A[1]) / L, (A[0] - B[0]) / L]
      const probe = (f: number): XY => [A[0] + (B[0] - A[0]) * f + o[0] * 0.15, A[1] + (B[1] - A[1]) * f + o[1] * 0.15]
      const nb = zoneAt(probe(0.5)) ?? zoneAt(probe(0.25)) ?? zoneAt(probe(0.75))
      return { A, B, L, o, nb }
    })
    const outer = edges.map((e) => !e.nb)
    // Bevelled parapets inset the lid along outline edges only.
    const d = edges.map((e, i) => (!zn.eave && outer[i] ? BEVEL : 0))
    const lid: XY[] = P.map((p, i) => {
      const j = (i + n - 1) % n, e0 = edges[j], e1 = edges[i]
      const n0: XY = [-e0.o[0], -e0.o[1]], n1: XY = [-e1.o[0], -e1.o[1]]
      // Intersect the two edge lines, each moved inward by its own offset.
      const a0: XY = [p[0] + n0[0] * d[j], p[1] + n0[1] * d[j]], a1: XY = [p[0] + n1[0] * d[i], p[1] + n1[1] * d[i]]
      const t0: XY = [e0.B[0] - e0.A[0], e0.B[1] - e0.A[1]], t1: XY = [e1.B[0] - e1.A[0], e1.B[1] - e1.A[1]]
      const den = t0[0] * t1[1] - t0[1] * t1[0]
      if (Math.abs(den) < 1e-6) return a1
      const k = ((a1[0] - a0[0]) * t1[1] - (a1[1] - a0[1]) * t1[0]) / den
      return [a0[0] + t0[0] * k, a0[1] + t0[1] * k]
    })
    for (const [i, j, k] of earcut(lid)) {
      const V = (q: XY): V3 => [q[0], q[1], zn.z(q)]
      tri(roof, V(lid[i]), V(lid[j]), V(lid[k]), [0, 0, 1])
    }
    edges.forEach((e, i) => {
      const { A, B, o } = e, top = (q: XY) => zn.z(q) - d[i] * 1
      if (outer[i]) {
        const base = Math.min(ground(A), ground(B)) - BURY
        quad(wall, [A[0], A[1], base], [B[0], B[1], base], [B[0], B[1], top(B)], [A[0], A[1], top(A)], [o[0], o[1], 0])
        if (d[i]) {
          const a2 = lid[i], b2 = lid[(i + 1) % n]
          // The parapet's bevel stays brick, so the walls still read red from above.
          quad(wall, [A[0], A[1], top(A)], [B[0], B[1], top(B)], [b2[0], b2[1], zn.z(b2)], [a2[0], a2[1], zn.z(a2)], [o[0], o[1], 1])
        }
        if (zn.eave) {
          // The eave: roof carried out past the wall, a fascia and a soffit.
          const ext = (q: XY, s: number): XY => [q[0] + o[0] * EAVE + (B[0] - A[0]) / e.L * s, q[1] + o[1] * EAVE + (B[1] - A[1]) / e.L * s]
          const convex = (k: number) => { const f = edges[k], g = edges[(k + 1) % n]; return f.o[0] * g.o[1] - f.o[1] * g.o[0] > 0 }
          const sa = outer[(i + n - 1) % n] && convex((i + n - 1) % n) ? -EAVE : 0, sb = outer[(i + 1) % n] && convex(i) ? EAVE : 0
          const a3 = ext(A, sa), b3 = ext(B, sb)
          const za = zn.z(A) - 0.15, zb = zn.z(B) - 0.15
          quad(roof, [A[0], A[1], zn.z(A) + 0.02], [B[0], B[1], zn.z(B) + 0.02], [b3[0], b3[1], zb], [a3[0], a3[1], za], [o[0] * 0.3, o[1] * 0.3, 1])
          quad(m.eave, [a3[0], a3[1], za], [b3[0], b3[1], zb], [b3[0], b3[1], zb - 0.35], [a3[0], a3[1], za - 0.35], [o[0], o[1], 0])
          quad(m.eave, [A[0], A[1], zn.z(A) - 0.6], [B[0], B[1], zn.z(B) - 0.6], [b3[0], b3[1], zb - 0.35], [a3[0], a3[1], za - 0.35], [0, 0, -1])
        }
        if (zn.rows && e.L > 4 && !onParty(A, B)) windows(m.win, e, zn, top)
      } else {
        // A step: wall only where this roof stands above the neighbour's.
        const nb = e.nb!
        const hA = zn.z(A) - nb.z(A), hB = zn.z(B) - nb.z(B)
        if (hA > 0.05 || hB > 0.05)
          quad(wall, [A[0], A[1], Math.min(nb.z(A), zn.z(A))], [B[0], B[1], Math.min(nb.z(B), zn.z(B))], [B[0], B[1], zn.z(B)], [A[0], A[1], zn.z(A)], [o[0], o[1], 0])
      }
    })
  }
}

/** Door positions on the outline; windows keep clear of them. */
export const DOORS: { p: XY; r: number }[] = []

/** Rooftop plant: small pale boxes, `[x, y, roof z, bearing]` each. */
export function plant(p: Part, units: [number, number, number, number][]) {
  for (const [x, y, z, deg] of units) {
    const a = axes(deg), c: XY = [x, y]
    const ring = clean([[-1.6, -1.1], [1.6, -1.1], [1.6, 1.1], [-1.6, 1.1]].map(([s, t]) => { const q = a.at(s, t); return [q[0] + c[0], q[1] + c[1]] as XY }))
    solid(p, ring, z - 0.2, z + 1.5)
  }
}

/** Window panels on an outline wall, each with a shallow segmental head. */
function windows(win: Part, e: { A: XY; B: XY; L: number; o: XY }, zn: Zone, top: (q: XY) => number) {
  const pitch = zn.pitch ?? 4, w = zn.winW ?? 1.7, count = Math.floor((e.L - 1.2) / pitch)
  if (count < 1) return
  const u: XY = [(e.B[0] - e.A[0]) / e.L, (e.B[1] - e.A[1]) / e.L], start = (e.L - count * pitch) / 2
  for (let k = 0; k < count; k++) {
    const c = start + pitch * (k + 0.5)
    const P = (s: number, z: number): V3 => [e.A[0] + u[0] * s + e.o[0] * 0.05, e.A[1] + u[1] * s + e.o[1] * 0.05, z]
    const mid: XY = [e.A[0] + u[0] * c, e.A[1] + u[1] * c]
    for (const [z0, z1] of zn.rows!) {
      if (ground(mid) > z0 - 0.4 || z1 + 0.6 > top(mid)) continue
      if (DOORS.some((d) => Math.hypot(mid[0] - d.p[0], mid[1] - d.p[1]) < d.r + w / 2)) continue
      const h = z1 - 0.3, s0 = c - w / 2, s1 = c + w / 2
      quad(win, P(s0, z0), P(s1, z0), P(s1, h), P(s0, h), [e.o[0], e.o[1], 0])
      if (zn.square) { quad(win, P(s0, h), P(s1, h), P(s1, z1), P(s0, z1), [e.o[0], e.o[1], 0]); continue }
      // Segmental arch: three segments rising 0.3 m.
      const arc = [0, 1 / 3, 2 / 3, 1].map((f) => { const s = s0 + (s1 - s0) * f; return P(s, h + 0.3 * Math.sin(Math.PI * f) ** 0.6 * 1) })
      for (let a = 0; a < 3; a++) tri(win, P(c, h), arc[a], arc[a + 1], [e.o[0], e.o[1], 0])
    }
  }
}

// ---------------------------------------------------------------------------
// Output.

/** Move a site-frame part into a model frame: anchor at origin, base at 0. */
export function place(part: Part, anchor: XY, base: number): Part {
  const out = new Part()
  out.pos = part.pos.map((v, i) => (i % 3 === 0 ? v - anchor[0] : i % 3 === 1 ? v - base : v + anchor[1]))
  out.nrm = part.nrm.slice(); out.uv = part.uv.slice()
  return out
}
export type Built = { id: string; name: string; anchor: XY; base: number; height: number; parts: { part: Part; material: Swatch }[] }
export async function write(b: Built, budget = 5000) {
  const parts = b.parts.filter((p) => p.part.triangles > 0).map(({ part, material }) => ({ part: place(part, b.anchor, b.base), material }))
  const triangles = parts.reduce((s, { part }) => s + part.triangles, 0)
  if (triangles > budget) throw new Error(`${b.id}: triangle budget exceeded: ${triangles}`)
  const glb = writeGlb(b.name, parts, {
    license: 'CC0-1.0', frame: 'Y up, -Z north, +X east, metres; origin at ground anchor',
    bearing: 0, elevation: 0, height: b.height,
  })
  if (glb.length > 256000) throw new Error(`${b.id}: file budget exceeded: ${glb.length}`)
  const out = new URL(`../../landmarks/models/${b.id}.glb`, import.meta.url).pathname
  await Bun.write(out, glb)
  const [lng, lat] = toLngLat(b.anchor)
  console.log(`${out}: ${triangles} triangles, ${glb.length} bytes; anchor ${lng}, ${lat}; base ${b.base.toFixed(1)} m`)
}

// The palette shared by the mill models: red brick pulled to the palette's
// lightness (it is the complex's identity), roof grey for the low roofs, trim
// for the eaves and copings, slate windows, and the silver of the metal
// towers and cladding.
export const BRICK = finish('mill-brick', 0xc27d66)
export const SILVER = finish('mill-silver', 0xd3d6d9)

// ---------------------------------------------------------------------------
// OSM outlines (site frame, metres).

/** way/414800516: both mills and the bridge, as one outline. */
export const OUTLINE: XY[] = [[-21.4, 66.8], [-2.9, 68.0], [-0.3, 23.7], [11.8, 24.1], [12.2, 14.8], [15.1, 14.9], [14.0, 38.4], [55.1, 40.4], [56.4, 15.2], [46.3, 14.7], [42.8, 14.6], [43.1, 8.3], [39.4, 8.2], [36.2, 8.0], [37.4, -8.0], [16.6, -9.6], [11.3, -9.9], [11.7, -13.5], [6.3, -24.9], [-5.7, -21.0], [-5.1, -18.7], [-5.4, -18.0], [-9.3, -18.1], [-11.8, -10.4], [-13.6, -10.4], [-17.7, 3.8], [-18.0, 7.0], [-21.8, 39.0], [-30.0, 27.3], [-30.6, 27.6], [-41.0, 32.3], [-46.3, 34.7], [-54.7, 17.5], [-65.5, -5.0], [-90.5, 5.1], [-104.8, 11.0], [-99.4, 23.1], [-87.6, 17.2], [-75.7, 42.4], [-79.2, 43.9], [-73.9, 55.6], [-103.2, 68.9], [-91.3, 94.6], [-65.4, 81.9], [-58.8, 96.2], [-63.6, 98.1], [-57.2, 111.1], [-48.4, 106.7], [-46.1, 111.2], [-65.1, 120.5], [-60.6, 129.5], [-63.2, 130.8], [-58.1, 140.9], [-50.3, 157.1], [-41.9, 153.1], [-44.0, 149.0], [-41.0, 147.5], [-33.0, 163.4], [4.2, 144.9], [-3.1, 130.5], [-2.1, 125.4], [-33.9, 57.2], [-21.1, 51.9]]
/** Mill #2's east wall runs straight on from the courtyard to the amphitheatre. */
export const MILL2_EAST: [XY, XY] = [[-54.7, 17.5], [-33.9, 57.2]]
/** Mill #1 (east arm) with its east wing: the outline east of x ≈ −21.5. */
export const MILL1: XY[] = OUTLINE.slice(0, 28).concat([[-21.1, 51.9]] as XY[])
/** The bridge section between the mills, north of the courtyard. */
export const BRIDGE: XY[] = [[-46.3, 34.7], [-41.0, 32.3], [-30.6, 27.6], [-30.0, 27.3], [-21.8, 39.0], [-21.1, 51.9], [-33.9, 57.2]]
/** Mill #2: the rest of the outline. */
export const MILL2: XY[] = OUTLINE.slice(31, 62)
/** way/1202433658: the courtyard stage canopy. */
const STAGE_CANOPY: XY[] = [[-41.0, 32.3], [-42.2, 29.7], [-43.4, 27.0], [-32.9, 22.0], [-30.6, 27.6]]

// ---------------------------------------------------------------------------
// Mill #1 and the bridge.

export function buildMill(): Built {
  const wall = new Part(), roof = new Part(), eave = new Part(), win = new Part(), silver = new Part(), stage = new Part()
  const m: Mats = { wall, roof, eave, win }
  const ax = axes(-2.5)               // Mill #1 runs a little west of north
  const P = MILL1
  const zones: Zone[] = []
  // The 1904 mill along the courtyard: one storey, low gable, deep eave, a
  // long row of segmental-arched windows; a basement row shows on the east
  // where the ground falls away.
  const GX0 = -24, GX1 = -3.5, tc = (GX0 + GX1) / 2, ridge = 223.8, eaveZ = 222.0, k = (ridge - eaveZ) / ((GX1 - GX0) / 2)
  const mill1904 = { rows: [[218.0, 220.6], [214.0, 216.2]] as [number, number][], pitch: 3.9 }
  zones.push({ poly: ax.band(P, -12, 39, GX0, tc), z: gableZ(ax, tc, ridge, k), eave: true, ...mill1904 })
  zones.push({ poly: ax.band(P, -12, 39, tc, GX1), z: gableZ(ax, tc, ridge, k), eave: true, ...mill1904 })
  // The 1920s southern L on the boulevard: flat, a short parapet.
  zones.push({ poly: ax.band(P, -40, -12, -40, 12.5), z: flatZ(222.2), rows: [[218.2, 220.8], [214.4, 216.6]], pitch: 3.6 })
  // The c.1955 dust-collector room: two storeys, flat.
  zones.push({ poly: ax.band(P, -12, 40, GX1, 13), z: flatZ(224.6), rows: [[218.6, 221.0], [214.6, 216.8]], pitch: 4.2, square: true })
  // The c.1946 wing to the east: a storey on a raised basement.
  zones.push({ poly: ax.band(P, -40, 13, 12.5, 60), z: flatZ(221.6), rows: [[217.8, 220.2], [214.0, 216.2]], pitch: 3.6, square: true })
  // The east building north of it (Comedy Zone side): two storeys, flat.
  zones.push({ poly: ax.band(P, 13, 60, 13, 60), z: flatZ(222.8), rows: [[218.6, 221.0], [214.8, 217.0]], pitch: 4.2, square: true })
  // The 1920s–40s two-storey additions north of the 1904 block.
  zones.push({ poly: ax.band(P, 39, 80, -40, 13), z: flatZ(223.6), rows: [[218.6, 221.2], [214.6, 217.0]], pitch: 4.0 })
  // The bridge section: two storeys of brick.
  zones.push({ poly: BRIDGE, z: flatZ(224.2), rows: [[219.6, 222.0]], pitch: 4.2, square: true })
  build(zones, m, [MILL2_EAST])
  plant(silver, [[-12, 50, 223.6, 0], [-8, 58, 223.6, 0], [3, 2, 224.6, 0], [26, 30, 222.8, 0], [40, 25, 222.8, 90], [-12, 20, 223.2, 0]])

  // The two metal dust towers between the 1904 mill and the dust-collector
  // room, standing well above it side by side (the grey box behind the seats
  // in the 2009 photo), with shallow gabled tops.
  for (const s0 of [12.6, 18.6]) {
    const ring = clean([ax.at(s0, -4.2), ax.at(s0 + 5.4, -4.2), ax.at(s0 + 5.4, 2.0), ax.at(s0, 2.0)])
    // Each top is a shallow gable along s: four faces over the box.
    solid(silver, ring, 222, 230.4, roof)
    const r = (t: number, z: number) => [ax.at(s0 - 0.3, t), ax.at(s0 + 5.7, t)].map((p) => [p[0], p[1], z] as V3)
    const [a0, a1] = r(-4.5, 230.4), [b0, b1] = r(-1.1, 231.4), [c0, c1] = r(2.3, 230.4)
    quad(roof, a0, a1, b1, b0, [0, 0, 1]); quad(roof, b0, b1, c1, c0, [0, 0, 1])
    tri(silver, a0, b0, c0, [-ax.u[0], -ax.u[1], 0]); tri(silver, a1, c1, b1, [ax.u[0], ax.u[1], 0])
  }

  // The sign tower over the bridge: a tall silver box on the brick, its cap
  // a little proud (photos 11 and the courtyard panorama).
  {
    const tw = axes(25.5), c: XY = [-34.5, 44.5]
    const box = (hs: number, ht: number): XY[] => [[-hs, -ht], [hs, -ht], [hs, ht], [-hs, ht]].map(([s, t]) => { const p = tw.at(s, t); return [p[0] + c[0], p[1] + c[1]] as XY }).reverse()
    const ring = clean(box(6.0, 5.5))
    solid(silver, ring, 223.0, 232.4, roof)
    solid(eave, clean(box(6.4, 5.9)), 232.4, 233.2, roof)
  }

  // The courtyard stage on the bridge's south face: a dark stage box under a
  // canopy that tilts up towards the courtyard, on two slim front posts.
  {
    const canopy = clean(STAGE_CANOPY), cx = axes(25.5), g = 216.2
    const tilt = (q: XY) => 222.6 + 0.3 * (11.5 - cx.s(q))
    solid(stage, cx.band(canopy, 7.6), g - 1, g + 6.2, roof)
    for (let i = 1; i < canopy.length - 1; i++) {
      const V = (q: XY, dz = 0): V3 => [q[0], q[1], tilt(q) + dz]
      tri(roof, V(canopy[0]), V(canopy[i]), V(canopy[i + 1]), [0, 0, 1])
      tri(eave, V(canopy[0], -0.35), V(canopy[i + 1], -0.35), V(canopy[i], -0.35), [0, 0, -1])
    }
    for (let i = 0; i < canopy.length; i++) {
      const A = canopy[i], B = canopy[(i + 1) % canopy.length]
      quad(eave, [A[0], A[1], tilt(A) - 0.35], [B[0], B[1], tilt(B) - 0.35], [B[0], B[1], tilt(B)], [A[0], A[1], tilt(A)], [B[1] - A[1], A[0] - B[0], 0])
    }
    for (const p of [cx.at(6.2, cx.t(canopy[2]) + 0.6), cx.at(6.2, cx.t(canopy[3]) - 0.6)]) {
      const r = 0.2, ring: XY[] = [[p[0] - r, p[1] - r], [p[0] + r, p[1] - r], [p[0] + r, p[1] + r], [p[0] - r, p[1] + r]]
      solid(stage, ring, g - 1, tilt(p) - 0.3)
    }
  }

  const anchor = centroid(MILL1)
  const base = lowest(clean(MILL1.concat()))
  return {
    id: 'music-factory-mill', name: 'AvidXchange Music Factory mill', anchor, base, height: 233.2 - base,
    parts: [
      { part: wall, material: BRICK },
      { part: roof, material: PALETTE.roof },
      { part: eave, material: PALETTE.trim },
      { part: win, material: PALETTE.window },
      { part: silver, material: SILVER },
      { part: stage, material: finish('stage-charcoal', 0x4a4f57) },
    ],
  }
}

if (import.meta.main) await write(buildMill())
