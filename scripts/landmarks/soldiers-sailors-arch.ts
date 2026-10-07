/**
 * Soldiers' and Sailors' Memorial Arch, Grand Army Plaza, Brooklyn.
 * Procedural CC0 geometry, styled to landmarks/STYLE.md.
 * bun scripts/landmarks/soldiers-sailors-arch.ts
 *
 * Map frame: x = passage axis (+x is the park/south face, which carries the
 * Army and Navy groups), y = the long facade, z = up. Placement in the
 * catalog: 40.6729891, -73.9699033; bearing 81; elevation 0.
 *
 * The footprint is the OSM envelope: masonry x ±3.85, y -11.7..11.8, with the
 * paired-column bays reaching x ±5.4. That envelope is 0.05 m off-centre in
 * y, so the model is built symmetric about y = 0 and shifted by +0.05 at the
 * end, which keeps the bounds and origin exactly where they were.
 * Reference: https://s-media.nyc.gov/agencies/lpc/lp/0821.pdf
 */
import { Part, addGltfTriangles, cross, sub, len, writeGlb, type V3 } from './mesh'

const granite = new Part(), trim = new Part(), recess = new Part(), base = new Part()
const bronze = new Part(), patina = new Part()
/** Sculpture is gathered raw, then smoothed and split into bronze/patina. */
const cast = new Part()
const TAU = Math.PI * 2
const Y_SHIFT = 0.05
const TOP = 32.6303 // the existing model's height, kept so the bounds are unchanged

const unit = (v: V3): V3 => v.map(n => n / (len(v) || 1)) as V3
const add = (a: V3, b: V3): V3 => a.map((n, i) => n + b[i]) as V3
const mul = (a: V3, s: number): V3 => a.map(n => n * s) as V3
const dot = (a: V3, b: V3) => a[0] * b[0] + a[1] * b[1] + a[2] * b[2]

/** A quad wound so its face normal points away from `inside`. */
function face(p: Part, a: V3, b: V3, c: V3, d: V3, inside: V3) {
  const n = cross(sub(b, a), sub(c, a))
  const centre = mul(add(add(a, b), add(c, d)), .25)
  if (dot(n, sub(centre, inside)) >= 0) p.quad(a, b, c, d)
  else p.quad(d, c, b, a)
}
/** Copy `src` into `dst` with smooth shading inside the crease angle. */
function smoothInto(dst: Part, src: Part, crease = 50) {
  addGltfTriangles(dst, new Float32Array(src.pos), Uint32Array.from({ length: src.pos.length / 3 }, (_, i) => i), { creaseDegrees: crease })
}
function smooth(p: Part, build: (q: Part) => void, crease = 50) {
  const q = new Part(); build(q); smoothInto(p, q, crease)
}
function solid(p: Part, rings: V3[][]) {
  p.loft(rings); p.cap(rings[0], false); p.cap(rings.at(-1)!, true)
}

/**
 * A box with chamfered vertical edges and, optionally, chamfered top and
 * bottom edges, smooth-shaded so each edge catches a soft highlight.
 */
function block(p: Part, x0: number, x1: number, y0: number, y1: number, z0: number, z1: number,
  b = .4, { top = true, bottom = false } = {}) {
  const ring = (d: number, z: number): V3[] => {
    const X0 = x0 + d, X1 = x1 - d, Y0 = y0 + d, Y1 = y1 - d
    const c = Math.min(b, (X1 - X0) / 3, (Y1 - Y0) / 3)
    return ([[X0 + c, Y0], [X1 - c, Y0], [X1, Y0 + c], [X1, Y1 - c], [X1 - c, Y1], [X0 + c, Y1], [X0, Y1 - c], [X0, Y0 + c]] as [number, number][])
      .map(([x, y]) => [x, y, z])
  }
  const e = Math.min(b, (z1 - z0) / 2.2)
  const rings = [
    ...(bottom ? [ring(e, z0), ring(0, z0 + e)] : [ring(0, z0)]),
    ...(top ? [ring(0, z1 - e), ring(e, z1)] : [ring(0, z1)]),
  ]
  smooth(p, q => solid(q, rings))
}
/** Mirror a block across the passage axis (x) or the facade's centre (y). */
const span = (s: number, a: number, b: number): [number, number] => s > 0 ? [a, b] : [-b, -a]

// ---------------------------------------------------------------- masonry
const WX = 3.6, WY = 11.5 // wall faces
const OX = 3.85, OY = 11.75 // cornices and base course: the footprint
const R = 5.5, ZS = 9.5 // a true semicircle: span 11 m, crown at 15 m
const ZT = 18.4 // underside of the main cornice
const B = .45, b = .3 // outer and passage-corner chamfers
const N = 12 // segments per semicircle
const arc = Array.from({ length: N + 1 }, (_, i) => {
  const a = Math.PI * i / N
  return [R * Math.cos(a), ZS + R * Math.sin(a)] as [number, number]
})

// The body: one mass with the passage cut through it, so the arch faces run
// unbroken from pier to pier without a seam where a pier would meet a lintel.
smooth(granite, q => {
  const mid: V3 = [0, 0, 14]
  for (const sx of [-1, 1]) {
    const x = sx * WX
    for (const sy of [-1, 1]) {
      face(q, [x, sy * (R + b), 0.9], [x, sy * (WY - B), 0.9], [x, sy * (WY - B), ZT], [x, sy * (R + b), ZT], [0, sy * 8, 9])
      face(q, [x, sy * R, ZS], [x, sy * (R + b), ZS], [x, sy * (R + b), ZT], [x, sy * R, ZT], [0, sy * 4, 14])
      face(q, [x, sy * (WY - B), 0.9], [sx * (WX - B), sy * WY, 0.9], [sx * (WX - B), sy * WY, ZT], [x, sy * (WY - B), ZT], [0, sy * 8, 9])
      face(q, [x, sy * (R + b), 0.9], [sx * (WX - b), sy * R, 0.9], [sx * (WX - b), sy * R, ZS], [x, sy * (R + b), ZS], [0, sy * 8.5, 5])
    }
    for (let i = 0; i < N; i++) {
      const [ya, za] = arc[i], [yb, zb] = arc[i + 1]
      face(q, [x, ya, za], [x, yb, zb], [x, yb, ZT], [x, ya, ZT], [0, (ya + yb) / 2, 17])
    }
  }
  for (const sy of [-1, 1]) {
    face(q, [-(WX - B), sy * WY, 0.9], [WX - B, sy * WY, 0.9], [WX - B, sy * WY, ZT], [-(WX - B), sy * WY, ZT], mid)
    face(q, [-(WX - b), sy * R, 0.9], [WX - b, sy * R, 0.9], [WX - b, sy * R, ZS], [-(WX - b), sy * R, ZS], [0, sy * 8.5, 5])
  }
})
// The passage vault, a shade darker than the faces so the opening reads deep.
smooth(recess, q => {
  for (let i = 0; i < N; i++) {
    const [ya, za] = arc[i], [yb, zb] = arc[i + 1]
    face(q, [-WX, ya, za], [WX, ya, za], [WX, yb, zb], [-WX, yb, zb], [0, (ya + yb) / 2 * 1.3, ZS + (za + zb - 2 * ZS) / 2 * 1.3])
  }
}, 40)

// Dark granite base course under both piers.
for (const sy of [-1, 1]) block(base, -OX, OX, ...span(sy, R - .15, OY), 0, 0.9, .2)
// Impost band wrapping each pier at the arch's springing.
for (const sy of [-1, 1]) block(trim, -3.72, 3.72, ...span(sy, R - .12, 11.62), 9.1, 9.6, .15, { bottom: true })

// Archivolt: a raised moulding around the arch on both faces, with rounded
// lips, and a keystone at the crown.
for (const sx of [-1, 1]) {
  const profile: [number, number][] = [[R, 0], [R, .08], [R + .12, .2], [R + .66, .2], [R + .78, .08], [R + .78, 0]]
  smooth(trim, q => {
    const at = (a: number, [r, d]: [number, number]): V3 => [sx * (WX + d), r * Math.cos(a), ZS + r * Math.sin(a)]
    for (let i = 0; i < N; i++) {
      const a0 = Math.PI * i / N, a1 = Math.PI * (i + 1) / N, am = (a0 + a1) / 2
      const inside: V3 = [sx * (WX + .04), (R + .39) * Math.cos(am), ZS + (R + .39) * Math.sin(am)]
      for (let k = 0; k < profile.length - 1; k++)
        face(q, at(a0, profile[k]), at(a0, profile[k + 1]), at(a1, profile[k + 1]), at(a1, profile[k]), inside)
    }
  }, 50)
  block(trim, ...span(sx, WX - .1, OX), -.5, .5, 14.75, 16.45, .14, { bottom: true })
}

// Main cornice, a plain attic band and the attic cornice.
block(trim, -OX, OX, -OY, OY, 18.0, 18.9, .3, { bottom: true })
block(granite, -WX, WX, -WY, WY, 18.9, 22.75, B, { top: false })
block(trim, -3.8, 3.8, -11.7, 11.7, 22.75, 23.5, .26, { bottom: true })
block(granite, -3.66, 3.66, -11.56, 11.56, 23.5, 24.0, .3)

// Paired-column bays on all four pier faces.
for (const sx of [-1, 1]) for (const sy of [-1, 1]) {
  const [y0, y1] = span(sy, 7.0, 11.35), yc = (y0 + y1) / 2
  block(base, ...span(sx, WX, 5.4), ...span(sy, 6.85, 11.5), 0, 0.9, .2)
  block(granite, ...span(sx, WX - .1, 4.55), y0, y1, 0.9, 8.0, .25, { top: false })
  block(trim, ...span(sx, WX - .1, 5.3), y0 - .05, y1 + .05, 0.9, 2.0, .2)
  block(trim, ...span(sx, WX - .1, 5.3), y0 - .05, y1 + .05, 7.9, 8.85, .15, { bottom: true })
  block(trim, ...span(sx, WX - .1, 5.4), ...span(sy, 6.85, 11.5), 8.85, 9.6, .25, { bottom: true })
  // Recessed centre panel between the pairs, flush and darker.
  const px = sx * 4.56
  face(recess, [px, yc - .55, 2.6], [px, yc + .55, 2.6], [px, yc + .55, 7.3], [px, yc - .55, 7.3], [0, yc, 5])
  for (const dy of [-1.725, -.925, .925, 1.725]) {
    const c: V3 = [sx * 4.93, yc + dy, 0]
    const rings = ([[2.0, .42], [2.42, .31], [7.45, .28], [7.9, .43]] as [number, number][])
      .map(([z, r]) => Array.from({ length: 12 }, (_, i): V3 => [c[0] + r * Math.cos(i * TAU / 12), c[1] + r * Math.sin(i * TAU / 12), z]))
    smooth(granite, q => q.loft(rings), 55)
  }
}

// ---------------------------------------------------------------- bronze
type Xf = (v: V3) => V3
function ellipsoid(p: Part, c: V3, r: V3, n = 10, rows = 5, pitch = 0) {
  const co = Math.cos(pitch), si = Math.sin(pitch)
  const point = (a: number, t: number): V3 => {
    const lx = r[0] * Math.cos(t) * Math.cos(a), ly = r[1] * Math.cos(t) * Math.sin(a), lz = r[2] * Math.sin(t)
    return [c[0] + lx * co - lz * si, c[1] + ly, c[2] + lx * si + lz * co]
  }
  for (let j = 0; j < rows; j++) for (let i = 0; i < n; i++) {
    const a = i * TAU / n, a2 = (i + 1) * TAU / n, lo = -Math.PI / 2 + j * Math.PI / rows, hi = lo + Math.PI / rows
    const v0 = point(a, lo), v1 = point(a2, lo), v2 = point(a2, hi), v3 = point(a, hi)
    if (j > 0) p.tri(v0, v1, v2)
    if (j < rows - 1) p.tri(v0, v2, v3)
  }
}
function tube(p: Part, path: V3[], radii: number[], sides = 6) {
  solid(p, path.map((c, i) => {
    const axis = unit(sub(path[Math.min(i + 1, path.length - 1)], path[Math.max(0, i - 1)]))
    const u = unit(cross(Math.abs(axis[2]) > 0.9 ? [0, 1, 0] : [0, 0, 1], axis)), v = cross(axis, u)
    return Array.from({ length: sides }, (_, j) => add(c, add(mul(u, radii[i] * Math.cos(j * TAU / sides)), mul(v, radii[i] * Math.sin(j * TAU / sides)))))
  }))
}
/** A thick flat slab with a convex outline, given as 2D points in the (U, V) plane at `o`. */
function slab(p: Part, o: V3, U: V3, V: V3, outline: [number, number][], thick: number) {
  const W = mul(unit(cross(U, V)), thick / 2)
  const at = ([u, v]: [number, number], s: number) => add(add(o, add(mul(U, u), mul(V, v))), mul(W, s))
  const front = outline.map(q => at(q, 1)), back = outline.map(q => at(q, -1))
  solid(p, [back, front])
}
/** Robed figure facing +x: a tapering robe, a head, and `arms` as paths from the shoulders. */
function figure(p: Part, c: V3, h: number, arms: V3[][]) {
  const ring = (z: number, rx: number, ry: number) => Array.from({ length: 10 }, (_, i): V3 =>
    [c[0] + rx * h * Math.cos(i * TAU / 10), c[1] + ry * h * Math.sin(i * TAU / 10), c[2] + z * h])
  solid(p, [ring(0, .15, .17), ring(.42, .12, .14), ring(.74, .1, .15), ring(.8, .05, .06)])
  ellipsoid(p, [c[0] + .01 * h, c[1], c[2] + .88 * h], [.075 * h, .07 * h, .095 * h], 8, 4)
  for (const path of arms) tube(p, path, [.045 * h, .038 * h, .034 * h], 6)
}
/** Mirror the raw sculpture added since `from` across y = 0. */
function mirrorSince(p: Part, from: number) {
  const end = p.pos.length
  for (let k = from; k < end; k += 9) {
    // Part stores glTF frame (x, z, -y): negate y by negating the third axis.
    const v = [0, 1, 2].map(j => [p.pos[k + j * 3], -(-p.pos[k + j * 3 + 2]), p.pos[k + j * 3 + 1]] as V3)
    p.tri(v[2], v[1], v[0])
  }
}

// Quadriga plinth: a dark bronze-clad block on the attic.
block(patina, -2.8, 2.8, -4.75, 4.75, 24.0, 24.6, .2)
const Z0 = 24.6

/** A horse facing +x, standing on the plinth at y = hy, lifting the fore leg on `lift`'s side. */
function horse(hy: number, lift: number) {
  ellipsoid(cast, [.15, hy, Z0 + 2.05], [1.15, .5, .62], 10, 4)
  tube(cast, [[.9, hy, Z0 + 2.15], [1.3, hy, Z0 + 2.85], [1.5, hy, Z0 + 3.3]], [.48, .37, .28], 8)
  ellipsoid(cast, [1.8, hy, Z0 + 3.12], [.5, .24, .27], 8, 4, -.7)
  for (const s of [-1, 1]) {
    const y = hy + s * .22
    tube(cast, [[-.65, y, Z0 + 1.85], [-.78, y, Z0 + .95], [-.62, y, Z0]], [.3, .23, .2])
    if (s === lift) tube(cast, [[.95, y, Z0 + 1.85], [1.45, y, Z0 + 1.35], [1.3, y, Z0 + .8]], [.3, .23, .2])
    else tube(cast, [[.95, y, Z0 + 1.85], [1.02, y, Z0 + .95], [.98, y, Z0]], [.3, .23, .2])
  }
  tube(cast, [[-.95, hy, Z0 + 2.2], [-1.3, hy, Z0 + 1.7], [-1.35, hy, Z0 + .95]], [.13, .16, .09])
}
/** A winged Victory beside the outer horse, trumpet raised outward. */
function victory(vy: number) {
  const s = Math.sign(vy), h = 3.1, c: V3 = [.95, vy, Z0]
  figure(cast, c, h, [
    [[c[0], vy + s * .3, Z0 + 2.3], [c[0] + .35, vy + s * .55, Z0 + 2.55], [c[0] + .7, vy + s * .5, Z0 + 2.9]],
    [[c[0], vy - s * .3, Z0 + 2.3], [c[0] + .35, vy - s * .55, Z0 + 2.2], [c[0] + .75, vy - s * .75, Z0 + 2.5]],
  ])
  tube(cast, [[c[0] + .65, vy + s * .5, Z0 + 2.88], [c[0] + 1.55, vy + s * .45, Z0 + 3.25]], [.07, .16], 8)
  // Wings sweep back and outward from the shoulders.
  for (const w of [-1, 1]) slab(cast, [c[0] - .25, vy + w * .15, Z0 + 2.25], unit([-.7, w * .7, 0]), [0, 0, 1],
    [[0, 0], [.7, -.4], [1.6, .05], [1.85, 1.0], [1.3, 1.5], [.4, .85]], .22)
}

/** Scale what was added to `p` since `from` about `o` (map frame). */
function scaleSince(p: Part, from: number, o: V3, k: number) {
  const g = [o[0], o[2], -o[1]] // glTF frame
  for (let i = from; i < p.pos.length; i++) p.pos[i] = g[i % 3] + (p.pos[i] - g[i % 3]) * k
}
const castStart = cast.pos.length
for (const hy of [1.15, 2.85]) {
  const from = cast.pos.length
  horse(hy, 1)
  // Colossal horses: the real team stands about as tall as the Victories' wings.
  scaleSince(cast, from, [0, hy, Z0], 1.15)
}
victory(4.05)
mirrorSince(cast, castStart)

// Chariot between the inner horses, and Columbia in it, with a banner hung
// from her standard and a laurel wreath at its head. Built on the axis, so
// the group stays symmetrical.
solid(cast, [[Z0 + .55, .7, .45], [Z0 + 1.15, .85, .58], [Z0 + 1.75, .95, .62]].map(([z, rx, ry]) =>
  Array.from({ length: 12 }, (_, i): V3 => [-.1 + rx * Math.cos(i * TAU / 12), ry * Math.sin(i * TAU / 12), z])))
tube(cast, [[-.1, 0, Z0], [-.1, 0, Z0 + .6]], [.3, .3], 8)
// Front shield, the chariot's eagle panel, as a broad rounded boss.
ellipsoid(cast, [.82, 0, Z0 + 1.25], [.16, .55, .55], 10, 4)
const colH = 4.9, colC: V3 = [-.2, 0, Z0 + 1.0]
figure(cast, colC, colH, [-1, 1].map(s => [[-.2, s * .4, Z0 + 1 + .76 * colH], [-.45, s * .75, Z0 + 1 + .85 * colH], [-.62, s * .95, Z0 + 1 + .98 * colH]]))
const staffX = -.75, wreathR = .42, wreathT = .1, wreathZ = TOP - wreathR - wreathT
tube(cast, [[staffX, 0, Z0 + 1.0], [staffX, 0, wreathZ - wreathR]], [.14, .13], 6)
tube(cast, [[staffX, -1.08, Z0 + 1 + 1.08 * colH], [staffX, 1.08, Z0 + 1 + 1.08 * colH]], [.11, .11], 6)
// The banner bows back from the crossbar, hanging to Columbia's shoulders.
smooth(cast, q => {
  const top = Z0 + 1 + 1.08 * colH, bottom = Z0 + 1 + .84 * colH
  const pt = (t: number, z: number, d: number): V3 => [staffX - .1 - .5 * (1 - t * t) * (z === top ? .3 : 1) + d, 1.0 * t, z]
  const cols = 8
  for (let i = 0; i < cols; i++) {
    const t0 = -1 + 2 * i / cols, t1 = -1 + 2 * (i + 1) / cols
    const lo = (t: number) => bottom - .5 * t * t
    face(q, pt(t0, lo(t0), .08), pt(t1, lo(t1), .08), pt(t1, top, .08), pt(t0, top, .08), [staffX - 2, 0, top - 1])
    face(q, pt(t0, lo(t0), -.08), pt(t1, lo(t1), -.08), pt(t1, top, -.08), pt(t0, top, -.08), [staffX + 2, 0, top - 1])
    face(q, pt(t0, lo(t0), .08), pt(t1, lo(t1), .08), pt(t1, lo(t1), -.08), pt(t0, lo(t0), -.08), [staffX, 0, top])
  }
  for (const t of [-1, 1]) face(q, pt(t, bottom - .5, .08), pt(t, top, .08), pt(t, top, -.08), pt(t, bottom - .5, -.08), [staffX, 0, top - 1])
}, 60)
// Wreath: a torus facing the park, its crown setting the model's height.
for (let i = 0; i < 12; i++) for (let j = 0; j < 4; j++) {
  const pt = (a: number, t: number): V3 => {
    const r = wreathR + wreathT * Math.cos(t)
    return [staffX + wreathT * Math.sin(t), r * Math.cos(a), wreathZ + r * Math.sin(a)]
  }
  const a0 = i * TAU / 12 + TAU / 4, a1 = (i + 1) * TAU / 12 + TAU / 4, t0 = j * TAU / 4, t1 = (j + 1) * TAU / 4
  face(cast, pt(a0, t0), pt(a1, t0), pt(a1, t1), pt(a0, t1), [staffX, ((Math.cos(a0) + Math.cos(a1)) / 2) * wreathR, wreathZ + ((Math.sin(a0) + Math.sin(a1)) / 2) * wreathR])
}

// The Army and Navy groups on the south (park) bays: a broad mound of
// figures rising to a winged Spirit, read as one bold mass.
{
  const from = cast.pos.length
  const gy = 9.175, gx = 4.45, zb = 9.6
  block(cast, 3.65, 5.3, gy - 2.0, gy + 2.0, zb, zb + .3, .12)
  const bump = (i: number, k: number) => 1 + .1 * Math.sin(i * 2.7 + k * 1.9) + .06 * Math.cos(i * 5.1 - k * 3.3)
  const rings = ([[zb + .3, .82, 1.95], [zb + 1.6, .86, 1.95], [zb + 3.0, .84, 1.88], [zb + 4.0, .75, 1.65], [zb + 4.7, .55, 1.15]] as [number, number, number][])
    .map(([z, rx, ry], k) => Array.from({ length: 12 }, (_, i): V3 => {
      const a = i * TAU / 12, f = k === 0 ? 1 : bump(i, k)
      // Flat against the pier: the back half never passes the wall face.
      const x = gx + rx * Math.cos(a) * (Math.cos(a) < 0 ? Math.min(1, (gx - 3.62) / rx) : Math.min(f, (5.32 - gx) / rx))
      return [x, gy + ry * f * Math.sin(a), z]
    }))
  solid(cast, rings)
  // Heads and shoulders breaking the mound's surface.
  for (const [x, y, z, r] of [[5.0, -1.05, 11.4, .3], [5.05, .95, 11.7, .3], [5.0, -.25, 12.6, .28], [4.9, 1.1, 13.1, .26], [4.85, -1.0, 13.3, .26], [4.9, .3, 14.1, .26]])
    ellipsoid(cast, [x, gy + y, z], [r, r, r * 1.15], 6, 3)
  // The winged Spirit crowning the group.
  ellipsoid(cast, [4.55, gy, zb + 5.0], [.45, .5, .8], 10, 4)
  ellipsoid(cast, [4.7, gy, zb + 5.95], [.27, .26, .33], 8, 4)
  for (const w of [-1, 1]) slab(cast, [4.2, gy + w * .2, zb + 4.9], [0, w, 0], [0, 0, 1],
    [[0, -.4], [1.1, -.2], [2.0, .5], [2.05, 1.15], [1.4, 1.25], [.3, .75]], .26)
  // Only the park face carries the groups; mirror the east group to the west.
  mirrorSince(cast, from)
}

// Smooth the bronze, then let the upward-facing surfaces carry the patina.
{
  const sm = new Part(); smoothInto(sm, cast, 62)
  for (let k = 0; k < sm.pos.length; k += 9) {
    const v = [0, 1, 2].map(j => [sm.pos[k + j * 3], sm.pos[k + j * 3 + 1], sm.pos[k + j * 3 + 2]] as V3)
    const n = unit(cross(sub(v[1], v[0]), sub(v[2], v[0])))
    const target = n[1] > .3 ? patina : bronze // glTF y is up
    target.pos.push(...sm.pos.slice(k, k + 9)); target.nrm.push(...sm.nrm.slice(k, k + 9)); target.uv.push(...sm.uv.slice((k / 3) * 2, (k / 3) * 2 + 6))
  }
}

// Median sRGB samples from the reference photographs (sunlit faces).
const parts = [
  { part: granite, material: { name: 'granite', color: 0xc9c3b8 } }, // 01.jpg sunlit attic, warm grey
  { part: trim, material: { name: 'granite-trim', color: 0xd6d0c4 } }, // cornices catch the light
  { part: recess, material: { name: 'granite-shade', color: 0xa39d94 } }, // vault and panels
  { part: base, material: { name: 'granite-base', color: 0x666867 } }, // 01.jpg dark base course
  { part: bronze, material: { name: 'bronze', color: 0x1c3237, roughness: .75 } }, // 02.jpg shadowed bronze
  { part: patina, material: { name: 'bronze-patina', color: 0x46605b, roughness: .75 } }, // 02.jpg lit bronze, plinth
]
// Re-centre on the OSM envelope (glTF z = -y).
for (const { part } of parts) for (let k = 2; k < part.pos.length; k += 3) part.pos[k] -= Y_SHIFT
const triangles = parts.reduce((n, { part }) => n + part.triangles, 0)
if (triangles > 6500) throw new Error(`Triangle budget exceeded: ${triangles}`)
const glb = writeGlb("Soldiers' and Sailors' Memorial Arch", parts, {
  license: 'CC0-1.0', bearing: 81, elevation: 0,
  frame: 'Y up, -Z north, +X east, metres; ground anchor; map x = passage axis, y = facade',
  masonryHeight: 24, archSpan: 11, archSpring: ZS, archCrown: ZS + R,
  source: 'OSM envelope for way/20679503 and parts; NYC LPC LP-0821',
})
if (glb.length > 256000) throw new Error(`File budget exceeded: ${glb.length}`)
const out = new URL('../../landmarks/models/soldiers-sailors-arch.glb', import.meta.url).pathname
await Bun.write(out, glb)
console.log(`${out}: ${triangles} triangles, ${glb.length} bytes (${(glb.length / 1024).toFixed(1)} KiB)`)
