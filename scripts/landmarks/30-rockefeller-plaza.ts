/**
 * 30 Rockefeller Plaza (Comcast Building) — original procedural geometry, CC0-1.0.
 * bun scripts/landmarks/30-rockefeller-plaza.ts
 *
 * Built in the street-grid frame: x along the cross streets (Manhattan "east",
 * true bearing 119°), y along the avenues (Manhattan "north", bearing 29°),
 * z up, metres. The catalog places it with bearing 29°. Anchor is the
 * centroid of OSM outline way/487519790; every tier below is the footprint of
 * one of its OSM building:parts, measured in this frame.
 *
 * The look is the limestone piers running unbroken up the slab with dark
 * window bands recessed between them, so window bands are laid on one global
 * grid per axis and merged across tiers wherever a face carries straight on:
 * a stripe only stops where the face it is on actually steps back.
 */
import { Part, writeGlb, type V3 } from './mesh'

type XY = [number, number]
const stone = new Part(), glass = new Part(), roof = new Part()

const BEVEL = 0.5     // rounded vertical corners
const RECESS = 0.65   // window band depth
const LIP = 0.35      // chamfer round a band's opening
const PARAPET = 0.6   // coping height above each wall top
const unit = (v: V3): V3 => { const l = Math.hypot(...v) || 1; return v.map(n => n / l) as V3 }
const up: V3 = [0, 0, 1]

function quadN(p: Part, a: V3, b: V3, c: V3, d: V3, na: V3, nb: V3, nc: V3, nd: V3) {
  p.tri(a, b, c, undefined, undefined, undefined, [na, nb, nc])
  p.tri(a, c, d, undefined, undefined, undefined, [na, nc, nd])
}

// ---------------------------------------------------------------- footprints

const YC = -0.6          // the slab's centreline
const ARM = 9.6          // half-width of the narrow ends and the crown
const WIDE = 15.6        // half-width of the wide middle

/** The slab's plan at one tier: narrow west arm, wide middle, narrow east arm. */
function slab(w: number, wc: number, ec: number, e: number): XY[] {
  const a = YC - ARM, b = YC - WIDE, c = YC + ARM, d = YC + WIDE
  return [[w, a], [wc, a], [wc, b], [ec, b], [ec, a], [e, a], [e, c], [ec, c], [ec, d], [wc, d], [wc, c], [w, c]]
}
const rect = (x0: number, x1: number, y0: number, y1: number): XY[] => [[x0, y0], [x1, y0], [x1, y1], [x0, y1]]

type Solid = { ring: XY[]; z0: number; h: number; kind: 'tower' | 'wing' | 'base' }
const solids: Solid[] = []
const add = (ring: XY[], z0: number, h: number, kind: Solid['kind']) => solids.push({ ring, z0, h, kind })
const wt = (s: Solid) => s.h - PARAPET    // wall top: the next tier starts here

// way/771918436 and the outline: the whole block at street-wall height.
add([[-82.8, -23.7], [-63, -23.7], [-63, -28.4], [78, -28.4], [78, -16.5], [79.9, -16.5], [79.9, 15.7],
  [78, 15.7], [78, 28.7], [-61.7, 28.7], [-61.7, 22.3], [-82.8, 22.3]], 0, 10, 'base')
// Low wings on 49th and 50th Streets: ways 145341279 / 265 / 264.
add(rect(-57.2, 64.8, -24.3, 24), 10 - PARAPET, 20, 'wing')
add(rect(-57.2, 40.6, -24.3, 24), 20 - PARAPET, 40, 'wing')
add(rect(-57.2, 15.1, -24.3, 24), 40 - PARAPET, 43, 'wing')
// Sixth Avenue block, way/145341294.
add(rect(-82.8, -57.2, -23.7, 22.3), 10 - PARAPET, 70, 'wing')
// The slab, ways 145341256 … 145341259, each tier's full footprint.
const tiers: [number, number, number, number][] = [
  // height, east end of the wide middle, east end of the east arm, west end
  [45, 75.1, 79.9, -20.4],
  [125, 70.5, 79.9, -20.4],
  [133, 64.7, 79.9, -20.4],
  [175, 64.7, 77.4, -20.4],
  [190, 57.7, 77.4, -20.4],
  [220, 57.7, 75.1, -20.4],
  [235, 49.5, 75.1, -20.4],
  [245, 49.5, 70.5, -20.4],
]
let floor = 10 - PARAPET
for (const [h, ec, e, w] of tiers) { add(slab(w, -2.9, ec, e), floor, h, 'tower'); floor = h - PARAPET }
// The Top of the Rock crown, way/145341259.
add(rect(0.2, 70.5, YC - ARM, YC + ARM), floor, 260, 'tower')

// --------------------------------------------------------------- plan tools

type Edge = { a: XY; b: XY; na: V3; nb: V3; n: V3; main: boolean; corner: number }

/** Round each convex corner with a short bevel edge; long faces stay flat-shaded. */
function edges(ring: XY[]): Edge[] {
  const pts: { p: XY; n: V3; main: boolean; corner: number }[] = []   // n: normal of the edge leaving p
  const out: Edge[] = []
  const nOf = (a: XY, b: XY): V3 => unit([b[1] - a[1], a[0] - b[0], 0])
  for (let i = 0; i < ring.length; i++) {
    const a = ring[(i + ring.length - 1) % ring.length], b = ring[i], c = ring[(i + 1) % ring.length]
    const u = unit([b[0] - a[0], b[1] - a[1], 0]), v = unit([c[0] - b[0], c[1] - b[1], 0])
    const turn = u[0] * v[1] - u[1] * v[0]
    if (turn > 1e-5) {
      pts.push({ p: [b[0] - u[0] * BEVEL, b[1] - u[1] * BEVEL], n: nOf(a, b), main: false, corner: i })
      pts.push({ p: [b[0] + v[0] * BEVEL, b[1] + v[1] * BEVEL], n: nOf(b, c), main: true, corner: i })
    } else pts.push({ p: b, n: nOf(b, c), main: true, corner: i })
  }
  for (let i = 0; i < pts.length; i++) {
    const p = pts[i], q = pts[(i + 1) % pts.length]
    if (p.main) out.push({ a: p.p, b: q.p, na: p.n, nb: p.n, n: p.n, main: true, corner: p.corner })
    else out.push({ a: p.p, b: q.p, na: p.n, nb: q.n, n: unit([p.n[0] + q.n[0], p.n[1] + q.n[1], 0]), main: false, corner: p.corner })
  }
  return out
}

function inset(ring: XY[], d: number): XY[] {
  return ring.map((b, i) => {
    const a = ring[(i + ring.length - 1) % ring.length], c = ring[(i + 1) % ring.length]
    const u = unit([b[1] - a[1], a[0] - b[0], 0]), v = unit([c[1] - b[1], b[0] - c[0], 0])
    const k = 1 + u[0] * v[0] + u[1] * v[1]
    return [b[0] - (u[0] + v[0]) * d / k, b[1] - (u[1] + v[1]) * d / k]
  })
}

function inside([x, y]: XY, ring: XY[]) {
  let c = false
  for (let i = 0, j = ring.length - 1; i < ring.length; j = i++) {
    const [xi, yi] = ring[i], [xj, yj] = ring[j]
    if ((yi > y) !== (yj > y) && x < xi + (y - yi) * (xj - xi) / (yj - yi)) c = !c
  }
  return c
}

/** Ear clipping for the flat roofs; the plans are simple but not convex. */
function fill(p: Part, ring: XY[], z: number) {
  const idx = ring.map((_, i) => i)
  const area = (a: XY, b: XY, c: XY) => (b[0] - a[0]) * (c[1] - a[1]) - (b[1] - a[1]) * (c[0] - a[0])
  let guard = 0
  while (idx.length > 3 && guard++ < 1000) {
    for (let i = 0; i < idx.length; i++) {
      const a = ring[idx[(i + idx.length - 1) % idx.length]], b = ring[idx[i]], c = ring[idx[(i + 1) % idx.length]]
      if (area(a, b, c) <= 1e-9) continue
      const blocked = idx.some(k => {
        const q = ring[k]
        if (q === a || q === b || q === c) return false
        return area(a, b, q) > 0 && area(b, c, q) > 0 && area(c, a, q) > 0
      })
      if (blocked) continue
      p.tri([a[0], a[1], z], [b[0], b[1], z], [c[0], c[1], z])
      idx.splice(i, 1)
      break
    }
  }
  const [a, b, c] = idx.map(k => ring[k])
  p.tri([a[0], a[1], z], [b[0], b[1], z], [c[0], c[1], z])
}

// ----------------------------------------------------------- window bands

const PITCH = 4.8, BAND = 2.6, CORNER = 1.1, END = 0.9
const X0 = 35.35, Y0 = YC + 2.4  // grid phase: symmetric about the crown

/** Window-band centres along one main edge, as offsets along it. */
function columns(e: Edge, kind: Solid['kind']): number[] {
  const along = Math.abs(e.n[0]) < 0.5  // runs along x
  const [s0, s1] = along ? [Math.min(e.a[0], e.b[0]), Math.max(e.a[0], e.b[0])] : [Math.min(e.a[1], e.b[1]), Math.max(e.a[1], e.b[1])]
  const lo = s0 + CORNER - BEVEL + BAND / 2, hi = s1 - CORNER + BEVEL - BAND / 2
  const origin = along ? X0 : Y0
  const out: number[] = []
  // The low blocks have punched windows at twice the tower's rhythm.
  const step = kind === 'tower' ? 1 : 2
  for (let k = Math.ceil((lo - origin) / PITCH); origin + k * PITCH <= hi + 1e-6; k++)
    if (k % step === 0) out.push(+(origin + k * PITCH).toFixed(3))
  // A short return too narrow for the grid still gets one centred band.
  if (!out.length && s1 - s0 >= BAND + 2 * CORNER) out.push(+((s0 + s1) / 2).toFixed(3))
  return out
}
const lineKey = (e: Edge) => {
  const along = Math.abs(e.n[0]) < 0.5
  return `${along ? 'y' : 'x'}${Math.sign(along ? e.n[1] : e.n[0])}@${(along ? e.a[1] : e.a[0]).toFixed(1)}`
}
const pointOn = (e: Edge, c: number, out = 0): XY => {
  const along = Math.abs(e.n[0]) < 0.5
  return along ? [c, e.a[1] + e.n[1] * out] : [e.a[0] + e.n[0] * out, c]
}

// Gather each band's vertical runs, clipped where a lower block stands in front.
const plans = solids.map(s => edges(s.ring))
const runs = new Map<string, { lo: number; hi: number; e: Edge }[]>()
solids.forEach((s, i) => {
  for (const e of plans[i]) {
    if (!e.main) continue
    for (const c of columns(e, s.kind)) {
      let lo = s.z0
      const probe = pointOn(e, c, 0.3)
      for (let moved = true; moved;) {
        moved = false
        for (const o of solids) if (o !== s && o.z0 <= lo + 0.01 && o.h > lo && inside(probe, o.ring)) { lo = o.h; moved = true }
      }
      if (lo >= wt(s) - 0.5) continue
      const key = `${lineKey(e)}#${c}`
      runs.set(key, [...(runs.get(key) ?? []), { lo, hi: wt(s), e }])
    }
  }
})
// Merge runs that carry straight on through a tier, then trim their ends.
const bands = new Map<string, { lo: number; hi: number }[]>()
for (const [key, list] of runs) {
  list.sort((a, b) => a.lo - b.lo)
  const merged: { lo: number; hi: number; e: Edge }[] = []
  for (const r of list) {
    const last = merged.at(-1)
    if (last && r.lo <= last.hi + 0.01) last.hi = Math.max(last.hi, r.hi)
    else merged.push({ ...r })
  }
  const c = +key.split('#')[1]
  const trimmed = merged.map(m => ({ lo: m.lo + END, hi: m.hi - END })).filter(m => m.hi - m.lo > 1)
  bands.set(key, trimmed)
  for (const m of trimmed) recess(merged[0].e, c, m.lo, m.hi)
}

/** A recessed band: a chamfered stone reveal round a dark glazed back. */
function recess(e: Edge, c: number, lo: number, hi: number) {
  // t: the direction that keeps the face counter-clockwise seen from outside.
  const t: V3 = [-e.n[1], e.n[0], 0]
  const base = pointOn(e, c)
  const P = (s: number, z: number, d = 0): V3 => [base[0] + t[0] * s - e.n[0] * d, base[1] + t[1] * s - e.n[1] * d, z]
  const h = BAND / 2, i = h - LIP
  const outer = [P(-h, lo), P(h, lo), P(h, hi), P(-h, hi)]
  const inner = [P(-i, lo + LIP, RECESS), P(i, lo + LIP, RECESS), P(i, hi - LIP, RECESS), P(-i, hi - LIP, RECESS)]
  glass.quad(inner[0], inner[1], inner[2], inner[3])
  const sides: V3[] = [[0, 0, 1], [-t[0], -t[1], 0], [0, 0, -1], [t[0], t[1], 0]]   // inward normal of each reveal
  for (let k = 0; k < 4; k++) {
    const l = (k + 1) % 4, s = sides[k]
    const ni = unit([e.n[0] * 0.5 + s[0], e.n[1] * 0.5 + s[1], s[2]])
    quadN(stone, outer[k], outer[l], inner[l], inner[k], e.n, e.n, ni, ni)
  }
}

// ------------------------------------------------------------- walls & roofs

solids.forEach((s, i) => {
  const top = wt(s)
  for (const e of plans[i]) {
    const A = (u: XY, z: number): V3 => [u[0], u[1], z]
    if (!e.main) { quadN(stone, A(e.a, s.z0), A(e.b, s.z0), A(e.b, top), A(e.a, top), e.na, e.nb, e.nb, e.na); continue }
    const along = Math.abs(e.n[0]) < 0.5
    const coord = (p: XY) => along ? p[0] : p[1]
    const sa = coord(e.a), sb = coord(e.b), dir = Math.sign(sb - sa)
    const at = (s0: number): XY => along ? [s0, e.a[1]] : [e.a[0], s0]
    const panel = (u0: number, u1: number, z0: number, z1: number) => {
      if (z1 - z0 < 1e-3 || Math.abs(u1 - u0) < 1e-3) return
      stone.quad(A(at(u0), z0), A(at(u1), z0), A(at(u1), z1), A(at(u0), z1))
    }
    // Holes, in walking order along the edge.
    const holes = columns(e, s.kind).map(c => {
      const runs = bands.get(`${lineKey(e)}#${c}`) ?? []
      const r = runs.find(m => m.lo < top - 1e-3 && m.hi > s.z0 + 1e-3)
      return r ? { c, lo: Math.max(r.lo, s.z0), hi: Math.min(r.hi, top) } : null
    }).filter(Boolean) as { c: number; lo: number; hi: number }[]
    holes.sort((p, q) => (p.c - q.c) * dir)
    let cur = sa
    for (const hole of holes) {
      const near = hole.c - dir * BAND / 2, far = hole.c + dir * BAND / 2
      panel(cur, near, s.z0, top)
      panel(near, far, s.z0, hole.lo)
      panel(near, far, hole.hi, top)
      cur = far
    }
    panel(cur, sb, s.z0, top)
  }
  // Coping: a rounded lip rising from the wall top, then the terrace inside it.
  const rim = plans[i].map(e => e.a)
  const normals = plans[i].map(e => e.na)
  // Inset the sharp plan, not the bevelled rim: an inset wider than a bevel
  // folds its two corner points over each other. Both bevel points share
  // their corner's inset point, so the bevel's coping closes to a fan.
  const corner = plans[i].map(e => e.corner)
  const sharpMid = inset(s.ring, 0.45), sharpInner = inset(s.ring, 1.1)
  const mid = corner.map(c => sharpMid[c]), inner = sharpInner
  for (let k = 0; k < rim.length; k++) {
    const l = (k + 1) % rim.length, nk = normals[k], nl = plans[i][k].nb
    const a0: V3 = [...rim[k], top], a1: V3 = [...rim[l], top]
    const b0: V3 = [...mid[k], s.h], b1: V3 = [...mid[l], s.h]
    const c0: V3 = [...inner[corner[k]], s.h - 0.5], c1: V3 = [...inner[corner[l]], s.h - 0.5]
    if (corner[k] === corner[l]) { stone.tri(a0, a1, b0, undefined, undefined, undefined, [nk, nl, up]); continue }
    // Where the tier above carries this face straight on, the coping would be
    // inside it — and seen through every window band crossing the joint.
    const e = plans[i][k], along = Math.abs(e.n[0]) < 0.5
    const sa = along ? e.a[0] : e.a[1], sb = along ? e.b[0] : e.b[1]
    const covered = solids.filter(o => Math.abs(o.z0 - top) < 0.01).flatMap(o => o.ring.map((p, j): [number, number] | null => {
      const q = o.ring[(j + 1) % o.ring.length]
      const f: Edge = { a: p, b: q, na: up, nb: up, n: unit([q[1] - p[1], p[0] - q[0], 0]), main: true, corner: 0 }
      if (lineKey(f) !== lineKey(e)) return null
      const [u, v] = along ? [p[0], q[0]] : [p[1], q[1]]
      return [Math.min(u, v), Math.max(u, v)]
    }).filter(Boolean) as [number, number][])
    // Walk the edge, emitting coping only over the stretches left open.
    const cuts = new Set([0, 1])
    for (const [u, v] of covered) for (const x of [u, v]) { const t = (x - sa) / (sb - sa); if (t > 0 && t < 1) cuts.add(t) }
    const ts = [...cuts].sort((p, q) => p - q)
    const L = (p: V3, q: V3, t: number): V3 => [p[0] + (q[0] - p[0]) * t, p[1] + (q[1] - p[1]) * t, p[2]]
    for (let m = 0; m < ts.length - 1; m++) {
      const t0 = ts[m], t1 = ts[m + 1], sm = sa + (sb - sa) * (t0 + t1) / 2
      if (covered.some(([u, v]) => sm > u && sm < v)) continue
      quadN(stone, L(a0, a1, t0), L(a0, a1, t1), L(b0, b1, t1), L(b0, b1, t0), nk, nl, up, up)
      quadN(stone, L(b0, b1, t0), L(b0, b1, t1), L(c0, c1, t1), L(c0, c1, t0), up, up, [-nl[0], -nl[1], 0], [-nk[0], -nk[1], 0])
    }
  }
  fill(roof, inner, s.h - 0.5)
})

// ------------------------------------------------------------------ output

const parts = [
  { part: stone, material: { name: 'indiana-limestone', color: 0xd4cab7 } },
  { part: glass, material: { name: 'window', color: 0x5c6468, roughness: 0.8 } },
  { part: roof, material: { name: 'terracotta-terraces', color: 0xc8968a } },
]
const triangles = parts.reduce((n, { part }) => n + part.triangles, 0)
const glb = writeGlb('30 Rockefeller Plaza', parts, {
  license: 'CC0-1.0', bearing: 29, elevation: 0, anchor: [40.7591554, -73.9796355], height: 260,
  frame: 'Y up, -Z north, +X east, metres; origin at the ground anchor',
  note: 'OSM tier footprints; limestone piers with recessed window bands merged across continuous faces',
})
const out = new URL('../../landmarks/models/30-rockefeller-plaza.glb', import.meta.url).pathname
await Bun.write(out, glb)
console.log(`${out}: ${triangles} triangles, ${glb.length} bytes (${(glb.length / 1024).toFixed(1)} KiB)`)
if (triangles > 5000) throw new Error(`Triangle budget exceeded: ${triangles}`)
if (glb.length > 256000) throw new Error(`File budget exceeded: ${glb.length}`)
