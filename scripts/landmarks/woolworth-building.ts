/**
 * Woolworth Building — original procedural geometry, CC0-1.0.
 * bun scripts/landmarks/woolworth-building.ts
 *
 * Authoring frame: x across Broadway's frontage (bearing 121.5°), y along
 * Broadway (bearing 31.5°), z metres up. Catalog bearing 31.5°, anchor at the
 * OSM outline centroid (40.7124439,-74.0083116). Envelopes and heights follow
 * way/75363809 and its eleven building:parts: the U-shaped 120 m base, the
 * 120–170 m shaft with its corner turrets, the 170–194 m stage, and the crown
 * (corner pinnacles and the copper pyramid, lantern to 241 m). No textures:
 * piers, recessed window bands and Gothic arches are geometry.
 */
import { Part, writeGlb, type V3 } from './mesh'

type XY = [number, number]
type Rim = { points: XY[]; normals: V3[]; raw: XY[] }
type Arch = 'flat' | 'round' | 'pointed'

const terracotta = new Part(), glass = new Part(), roof = new Part()
const copper = new Part(), copperDark = new Part()
const BEVEL = .5, RECESS = .65
const unit = (v: V3): V3 => { const l = Math.hypot(...v); return v.map(n => n / l) as V3 }
const up: V3 = [0, 0, 1]
const at = (ring: XY[], z: number): V3[] => ring.map(([x, y]) => [x, y, z])

function quad(p: Part, a: V3, b: V3, c: V3, d: V3, na: V3, nb: V3, nc = nb, nd = na) {
  p.tri(a, b, c, undefined, undefined, undefined, [na, nb, nc])
  p.tri(a, c, d, undefined, undefined, undefined, [na, nc, nd])
}
/** An axis-aligned rectangle with 45° corner chamfers, counter-clockwise. */
function chamfer(x0: number, x1: number, y0: number, y1: number, c: number): XY[] {
  return [[x0 + c, y0], [x1 - c, y0], [x1, y0 + c], [x1, y1 - c], [x1 - c, y1], [x0 + c, y1], [x0, y1 - c], [x0, y0 + c]]
}
/** A regular polygon; `phase` turns it so a flat face can sit on an axis. */
function ngon(cx: number, cy: number, r: number, n: number, phase = Math.PI / n): XY[] {
  return Array.from({ length: n }, (_, i): XY => [cx + r * Math.cos(phase + i * 2 * Math.PI / n), cy + r * Math.sin(phase + i * 2 * Math.PI / n)])
}

/** Round every convex plan corner with a small smooth-shaded bevel. */
function soften(ring: XY[], radius = BEVEL): Rim {
  const points: XY[] = [], normals: V3[] = []
  for (let i = 0; i < ring.length; i++) {
    const a = ring[(i + ring.length - 1) % ring.length], b = ring[i], c = ring[(i + 1) % ring.length]
    const l0 = Math.hypot(b[0] - a[0], b[1] - a[1]), l1 = Math.hypot(c[0] - b[0], c[1] - b[1])
    const u: XY = [(b[0] - a[0]) / l0, (b[1] - a[1]) / l0], v: XY = [(c[0] - b[0]) / l1, (c[1] - b[1]) / l1]
    const turn = u[0] * v[1] - u[1] * v[0]
    if (turn < -1e-5) { points.push(b); normals.push(unit([u[1] + v[1], -u[0] - v[0], 0])); continue }
    if (Math.abs(turn) < 1e-5) { points.push(b); normals.push([u[1], -u[0], 0]); continue }
    const r = Math.min(radius, l0 * .2, l1 * .2)
    points.push([b[0] - u[0] * r, b[1] - u[1] * r], [b[0] + v[0] * r, b[1] + v[1] * r])
    normals.push([u[1], -u[0], 0], [v[1], -v[0], 0])
  }
  return { points, normals, raw: ring }
}
/** Offset a ring inwards along each corner's bisector (convex or concave). */
function inset(ring: XY[], d: number): XY[] {
  return ring.map((b, i) => {
    const a = ring[(i + ring.length - 1) % ring.length], c = ring[(i + 1) % ring.length]
    const u = unit([b[1] - a[1], a[0] - b[0], 0]), v = unit([c[1] - b[1], b[0] - c[0], 0])
    const k = 1 + u[0] * v[0] + u[1] * v[1]
    return [b[0] - (u[0] + v[0]) * d / k, b[1] - (u[1] + v[1]) * d / k]
  })
}
/** Ear-clipped flat cap, so the U-shaped base roof fills correctly. */
function fill(p: Part, ring: XY[], z: number, upward = true) {
  const idx = ring.map((_, i) => i)
  const cross2 = (o: XY, a: XY, b: XY) => (a[0] - o[0]) * (b[1] - o[1]) - (a[1] - o[1]) * (b[0] - o[0])
  let guard = 0
  while (idx.length > 3 && guard++ < 1000) {
    for (let k = 0; k < idx.length; k++) {
      const a = ring[idx[(k + idx.length - 1) % idx.length]], b = ring[idx[k]], c = ring[idx[(k + 1) % idx.length]]
      if (cross2(a, b, c) <= 1e-9) continue
      const blocked = idx.some(j => {
        const q = ring[j]
        if (q === a || q === b || q === c) return false
        return cross2(a, b, q) > 0 && cross2(b, c, q) > 0 && cross2(c, a, q) > 0
      })
      if (blocked) continue
      if (upward) p.tri([...a, z], [...b, z], [...c, z]); else p.tri([...a, z], [...c, z], [...b, z])
      idx.splice(k, 1)
      break
    }
  }
  const [a, b, c] = idx.map(i => ring[i])
  if (upward) p.tri([...a, z], [...b, z], [...c, z]); else p.tri([...a, z], [...c, z], [...b, z])
}
function wall(p: Part, rim: Rim, z0: number, z1: number) {
  const a = at(rim.points, z0), b = at(rim.points, z1)
  for (let i = 0; i < a.length; i++) {
    const j = (i + 1) % a.length
    quad(p, a[i], a[j], b[j], b[i], rim.normals[i], rim.normals[j])
  }
}

/**
 * Broad window bands recessed between raised terracotta piers, one band per
 * real bay group, running the stage's full height. Arched tops are true
 * semicircles or equilateral Gothic points.
 */
function facade(rim: Rim, bottom: number, top: number, o: { spacing: number; arch: Arch; frac?: number; max?: number }) {
  const n = rim.points.length
  for (let i = 0; i < n; i++) {
    const a = rim.points[i], b = rim.points[(i + 1) % n]
    const length = Math.hypot(b[0] - a[0], b[1] - a[1]), ux = (b[0] - a[0]) / length, uy = (b[1] - a[1]) / length
    const nrm: V3 = [uy, -ux, 0]
    const v = (s: number, z: number, depth = 0): V3 => [a[0] + ux * s + uy * depth, a[1] + uy * s - ux * depth, z]
    const panel = (s0: number, s1: number, z0: number, z1: number) => terracotta.quad(v(s0, z0), v(s1, z0), v(s1, z1), v(s0, z1))
    const count = length < 4.5 ? 0 : Math.max(1, Math.min(o.max ?? 8, Math.round(length / o.spacing)))
    if (!count) {
      quad(terracotta, v(0, bottom), v(length, bottom), v(length, top), v(0, top), rim.normals[i], rim.normals[(i + 1) % n])
      continue
    }
    const spacing = length / count, width = spacing * (o.frac ?? .6), lo = bottom + .8, hi = top - .8
    panel(0, length, bottom, lo)
    panel(0, length, hi, top)
    let last = 0
    for (let j = 0; j < count; j++) {
      const s0 = spacing * (j + .5) - width / 2, s1 = s0 + width, mid = (s0 + s1) / 2
      panel(last, s0, lo, hi)
      let outer: XY[]
      if (o.arch === 'round') {
        const r = width / 2, spring = hi - r
        outer = [[s0, lo], [s1, lo], ...Array.from({ length: 9 }, (_, k): XY => {
          const t = k * Math.PI / 8
          return [mid + r * Math.cos(t), spring + r * Math.sin(t)]
        })]
      } else if (o.arch === 'pointed') {
        // Equilateral arch: each side is an arc centred on the opposite jamb.
        const spring = hi - width * Math.sin(Math.PI / 3), steps = 3
        outer = [[s0, lo], [s1, lo]]
        for (let k = 0; k <= steps; k++) {
          const t = k / steps * Math.PI / 3
          outer.push([s0 + width * Math.cos(t), spring + width * Math.sin(t)])
        }
        for (let k = steps - 1; k >= 0; k--) {
          const t = k / steps * Math.PI / 3
          outer.push([s1 - width * Math.cos(t), spring + width * Math.sin(t)])
        }
      } else outer = [[s0, lo], [s1, lo], [s1, hi], [s0, hi]]
      // Drop coincident points (apex, springing) so every edge has length.
      outer = outer.filter((p, k) => { const q = outer[(k + 1) % outer.length]; return Math.hypot(p[0] - q[0], p[1] - q[1]) > 1e-6 })
      const inner = inset(outer, BEVEL * .9)
      const back = inner.map(([s, z]) => v(s, z, -RECESS))
      for (let k = 1; k < back.length - 1; k++) glass.tri(back[0], back[k], back[k + 1])
      for (let k = 0; k < outer.length; k++) {
        const l = (k + 1) % outer.length
        const ds = outer[l][0] - outer[k][0], dz = outer[l][1] - outer[k][1], e = Math.hypot(ds, dz)
        const inN = unit([nrm[0] * .4 - ux * dz / e, nrm[1] * .4 - uy * dz / e, ds / e])
        quad(terracotta, v(...outer[k]), v(...outer[l]), back[l], back[k], nrm, nrm, inN, inN)
      }
      if (o.arch !== 'flat') {
        // Spandrels: fill between the arch and the band head.
        for (let k = 2; k < outer.length - 1; k++) {
          const p = outer[k], q = outer[k + 1]
          if (Math.abs(p[1] - hi) < 1e-6) terracotta.tri(v(...p), v(q[0], hi), v(...q))
          else if (Math.abs(q[1] - hi) < 1e-6) terracotta.tri(v(...p), v(p[0], hi), v(...q))
          else terracotta.quad(v(...p), v(p[0], hi), v(q[0], hi), v(...q))
        }
      }
      last = s1
    }
    panel(last, length, lo, hi)
  }
}

/** A bevelled parapet coping around a tinted roof terrace. */
function terrace(rim: Rim, bottom: number, top: number, surface = roof) {
  // Offset the unbevelled plan, then re-bevel it finely: offsetting the
  // bevelled ring by more than its bevel would fold the corners inside out.
  const middle = soften(inset(rim.raw, .5), .05).points, inner = soften(inset(rim.raw, 1.1), .05).points
  const a = at(rim.points, bottom), b = at(middle, top), c = at(inner, top - .6)
  for (let i = 0; i < a.length; i++) {
    const j = (i + 1) % a.length, ni = rim.normals[i], nj = rim.normals[j]
    quad(terracotta, a[i], a[j], b[j], b[i], ni, nj, up, up)
    quad(terracotta, b[i], b[j], c[j], c[i], up, up, [-nj[0], -nj[1], 0], [-ni[0], -ni[1], 0])
  }
  fill(surface, inner, top - .6)
}

/** A flat-shaded prism, optionally capped. */
function prism(p: Part, ring: XY[], z0: number, z1: number, top = true) {
  p.loft([at(ring, z0), at(ring, z1)])
  if (top) p.cap(at(ring, z1), true)
}
/** A spire or hipped roof: a ring lofted through smaller rings to a point. */
function spire(p: Part, ring: XY[], levels: [number, number][], tip: number) {
  const cx = ring.reduce((s, q) => s + q[0], 0) / ring.length, cy = ring.reduce((s, q) => s + q[1], 0) / ring.length
  const rings = levels.map(([z, k]) => ring.map(([x, y]): V3 => [cx + (x - cx) * k, cy + (y - cy) * k, z]))
  p.loft(rings)
  const last = rings.at(-1)!, apex: V3 = [cx, cy, tip]
  for (let i = 0; i < last.length; i++) p.tri(last[i], last[(i + 1) % last.length], apex)
}
/** Flush dark slots on each face of a small prism: turret lancets. */
function slots(ring: XY[], z0: number, z1: number, frac = .4) {
  const cx = ring.reduce((s, q) => s + q[0], 0) / ring.length, cy = ring.reduce((s, q) => s + q[1], 0) / ring.length
  for (let i = 0; i < ring.length; i++) {
    const a = ring[i], b = ring[(i + 1) % ring.length]
    const m: XY = [(a[0] + b[0]) / 2, (a[1] + b[1]) / 2], h = (1 - frac) / 2
    const push = (q: XY): XY => { const dx = q[0] - cx, dy = q[1] - cy, l = Math.hypot(dx, dy); return [q[0] + dx / l * .04, q[1] + dy / l * .04] }
    const p0 = push([a[0] + (b[0] - a[0]) * h, a[1] + (b[1] - a[1]) * h]), p1 = push([b[0] + (a[0] - b[0]) * h, b[1] + (a[1] - b[1]) * h])
    const apex = push(m)
    const w = Math.hypot(p1[0] - p0[0], p1[1] - p0[1])
    glass.quad([...p0, z0], [...p1, z0], [...p1, z1 - w * .8], [...p0, z1 - w * .8])
    glass.tri([...p0, z1 - w * .8], [...p1, z1 - w * .8], [...apex, z1])
  }
}

// ── Base: the U-shaped 30-storey block, 0–120 m, light court open to the west.
const FRONT = 26.8, BACK = -31.2, HALF = 22, COURT = 5.85, COURT_END = -2.7
const TOWER = { x0: -2.7, x1: 26.8, y: 14.85, c: 2.5 }
const base: XY[] = [
  [BACK, -HALF], [FRONT, -HALF], [FRONT, -(TOWER.y - TOWER.c)], [FRONT, TOWER.y - TOWER.c], [FRONT, HALF],
  [BACK, HALF], [BACK, COURT], [COURT_END, COURT], [COURT_END, -COURT], [BACK, -COURT],
]
const baseRim = soften(base)
facade(baseRim, 0, 14, { spacing: 8.2, arch: 'flat', frac: .5 })
facade(baseRim, 14, 119.3, { spacing: 8.2, arch: 'pointed', frac: .5 })
terrace(baseRim, 119.3, 120)
fill(terracotta, base, 0, false)

// Green copper hipped roofs on the four outer corner pavilions of the U.
for (const sy of [1, -1]) for (const [x0, x1] of [[19.4, 26.1], [-30.5, -23.8]]) {
  const y0 = sy * 15.5, y1 = sy * 21.3
  const ring: XY[] = sy > 0 ? [[x0, y0], [x1, y0], [x1, y1], [x0, y1]] : [[x0, y1], [x1, y1], [x1, y0], [x0, y0]]
  prism(terracotta, ring, 119.4, 121.2, false)
  spire(copper, ring, [[121.2, 1], [124, .62]], 126.5)
}

// ── Tower shaft, 120–170 m, flush with the Broadway front.
const shaft = chamfer(TOWER.x0, TOWER.x1, -TOWER.y, TOWER.y, TOWER.c)
const shaftRim = soften(shaft)
facade(shaftRim, 120, 169.3, { spacing: 8.2, arch: 'pointed', frac: .5 })
terrace(shaftRim, 169.3, 170)
// Corner turrets in the chamfers (the four 120–175 m parts), with pinnacles.
for (const [cx, cy] of [[24.9, -12.75], [24.9, 12.75], [-.8, 12.75], [-.8, -12.75]] as XY[]) {
  const ring = ngon(cx, cy, 1.55, 8, Math.PI / 8)
  prism(terracotta, ring, 119.9, 171.6, false)
  spire(terracotta, ring, [[171.6, 1]], 175.5)
}

// ── Upper stage, 170–194 m.
const stage = chamfer(-.1, 24.1, -12.2, 12.2, 3.4)
const stageRim = soften(stage)
facade(stageRim, 170, 193.3, { spacing: 6, arch: 'pointed', frac: .55 })
terrace(stageRim, 193.3, 194)

// ── Crown: octagonal attic, copper pyramid with dormers, lantern and spire.
const CX = 12, APOTHEM = 10.1, R = APOTHEM / Math.cos(Math.PI / 8)
const crown = ngon(CX, 0, R, 8, Math.PI / 8)
const crownRim = soften(crown, .35)
facade(crownRim, 194, 204, { spacing: 8, arch: 'pointed', frac: .5 })
spire(copper, crown, [[204, 1], [208, .86], [229, .29]], 229)
const lantern = ngon(CX, 0, 2.9, 8, Math.PI / 8)
prism(copper, lantern, 228.9, 235.5, false)
slots(lantern, 230, 234.6, .45)
spire(copper, lantern, [[235.5, 1], [235.5, 1.12], [236.2, 1.12], [236.2, .7]], 241.4)

// Cream Gothic dormers on the four flat faces, their gables continuing the
// attic wall below, so nothing stands proud of the crown's plan.
const DW = 2.3, EAVE = 210.5, RIDGE = 215, DEPTH = 4.2
for (let k = 0; k < 4; k++) {
  const ang = k * Math.PI / 2, c = Math.cos(ang), s = Math.sin(ang)
  // Local (d outwards, w across to the left, z) → plan.
  const P = (d: number, w: number, z: number): V3 => [CX + c * d - s * w, s * d + c * w, z]
  const f = APOTHEM - .02, b = APOTHEM - DEPTH
  terracotta.quad(P(f, -DW, 204), P(f, DW, 204), P(f, DW, EAVE), P(f, -DW, EAVE))
  terracotta.tri(P(f, -DW, EAVE), P(f, DW, EAVE), P(f, 0, RIDGE))
  terracotta.quad(P(b, DW, 204), P(b, DW, EAVE), P(f, DW, EAVE), P(f, DW, 204))
  terracotta.quad(P(b, -DW, 204), P(f, -DW, 204), P(f, -DW, EAVE), P(b, -DW, EAVE))
  terracotta.quad(P(f, DW, EAVE), P(b, DW, EAVE), P(b, 0, RIDGE), P(f, 0, RIDGE))
  terracotta.quad(P(b, -DW, EAVE), P(f, -DW, EAVE), P(f, 0, RIDGE), P(b, 0, RIDGE))
  // A flush pointed lancet in the gable.
  const g = f + .04, lw = 1.1
  glass.quad(P(g, -lw, 205.2), P(g, lw, 205.2), P(g, lw, 210.6), P(g, -lw, 210.6))
  glass.tri(P(g, -lw, 210.6), P(g, lw, 210.6), P(g, 0, 212.6))
}

// Four corner pinnacle turrets on the 194 m terrace (the 194–215 m parts).
for (const [cx, cy] of [[3.25, -8.7], [20.75, -8.7], [20.75, 8.7], [3.25, 8.7]] as XY[]) {
  const ring = ngon(cx, cy, 2.55, 8, Math.PI / 8)
  prism(terracotta, ring, 193.9, 209, false)
  slots(ring, 196.5, 207, .42)
  spire(terracotta, ring, [[209, 1], [209.8, .8]], 217)
}

// sRGB colours from daylight photos: cream glazed terracotta, dark-glazed
// bays, verdigris copper.
const parts = [
  { part: terracotta, material: { name: 'cream-terracotta', color: 0xe6dfcb } },
  { part: glass, material: { name: 'window-bands', color: 0x87949b } },
  { part: roof, material: { name: 'terracotta-terraces', color: 0xc8968a } },
  { part: copper, material: { name: 'green-copper', color: 0x67a08a, roughness: .7 } },
  { part: copperDark, material: { name: 'copper-shadow', color: 0x46705f } },
]
const triangles = parts.reduce((n, { part }) => n + part.triangles, 0)
if (triangles > 5000) throw new Error(`Triangle budget exceeded: ${triangles}`)
const glb = writeGlb('Woolworth Building', parts, {
  license: 'CC0-1.0', bearing: 31.5, elevation: 0, anchor: [40.7124439, -74.0083116], height: 241.4,
  frame: 'Y up, -Z north, +X east, metres; origin at the ground anchor',
  footprint: { x: [BACK, FRONT], y: [-HALF, HALF] },
})
if (glb.length > 256000) throw new Error(`File budget exceeded: ${glb.length}`)
const out = new URL('../../landmarks/models/woolworth-building.glb', import.meta.url).pathname
await Bun.write(out, glb)
console.log(`${out}: ${triangles} triangles, ${glb.length} bytes (${(glb.length / 1024).toFixed(1)} KiB)`)
