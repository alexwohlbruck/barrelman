/**
 * Mint Museum Uptown, Levine Center for the Arts, Charlotte — procedural,
 * CC0-1.0, no textures.
 * bun scripts/landmarks/mint-museum-uptown.ts
 *
 * Map frame: x across the building (the Duke Energy side -x, Levine Avenue of
 * the Arts +x), y from the Tryon front (-y), which faces South Tryon Street
 * across Duke Energy Plaza, back towards Church Street (+y), z up, metres.
 * Placed at bearing 318.5°, the long axis of the OSM outline (way/131139755).
 *
 * That outline runs all the way back to Church Street, but its rear 40 m is
 * the footprint of Museum Tower (way/649245473, which shares its nodes). The
 * museum itself is the front 42 × 60.5 m, and that is what is modelled; the
 * anchor is the centre of that rectangle. Height 31 m from OSM, five storeys.
 *
 * Masses, from photos: a block clad in buff sandstone, its faces folded into
 * a few big tilted planes. On the Tryon front, a stone frame carries a deep
 * soffit of dark-stained wood over the left 60% of the face, rising towards
 * the street so it reads from below as a big angled ceiling. Under it is the
 * void, with a lower stone block (a portal at its foot) on the Duke Energy
 * side, the atrium's glass in a slot and across the back, and on the Levine
 * side the projecting mass, whose top rises on a diagonal to the roof. That
 * mass overhangs a tall glass lobby set 8 m back, under a sloping underside
 * and a slim round column, and has a recessed window high on its right.
 *
 * Seen from above, the real roof covers the void, so from the usual high
 * three-quarter view the building read as a blank buff box. Here the void
 * behind the soffit is open to the sky, a notch with a dark floor (the
 * soffit's wood carried down onto it), closed at the back by the atrium glass
 * run up to the coping.
 *
 * The Levine Avenue side follows the photos, with fewer and bigger openings
 * than the real one: glazing low down near the front, the full-height glass
 * bay under a red-brown panel, one tall window and the rear shopfronts. No
 * photo shows the Duke Energy side, which faces the tower's plaza; it is given
 * the same folded sandstone and the same kinds of openings.
 */
import { Part, cross, sub, writeGlb, type V3 } from './mesh'
import { PALETTE, finish } from './palette'

const stone = new Part(), roof = new Part(), metal = new Part()
const win = new Part(), glass = new Part(), wood = new Part()

const unit = (v: V3): V3 => { const l = Math.hypot(...v) || 1; return [v[0] / l, v[1] / l, v[2] / l] }

// ---------------------------------------------------------------------------
// Boxes with a material per face and optional rounded chamfers on the four
// vertical edges (sw, se, ne, nw). A chamfer's corners carry the normals of
// the faces either side, so it shades as a soft rounded edge.

type Faces = { s?: Part | null; e?: Part | null; n?: Part | null; w?: Part | null; top?: Part | null; bottom?: Part | null }
function box(x0: number, x1: number, y0: number, y1: number, z0: number, z1: number,
  f: Faces = {}, c: [number, number, number, number] = [0, 0, 0, 0]) {
  const S: V3 = [0, -1, 0], E: V3 = [1, 0, 0], N: V3 = [0, 1, 0], W: V3 = [-1, 0, 0]
  const pick = (k: keyof Faces) => (f[k] === undefined ? stone : f[k])
  // Ring counter-clockwise from above: each edge carries its material and
  // the normals at its two ends.
  type Edge = { a: [number, number]; b: [number, number]; p: Part | null; na: V3; nb: V3 }
  const [cs, ce, cn, cw] = c
  const edges: Edge[] = [
    { a: [x0 + cs, y0], b: [x1 - ce, y0], p: pick('s'), na: S, nb: S },
    { a: [x1 - ce, y0], b: [x1, y0 + ce], p: stone, na: S, nb: E },
    { a: [x1, y0 + ce], b: [x1, y1 - cn], p: pick('e'), na: E, nb: E },
    { a: [x1, y1 - cn], b: [x1 - cn, y1], p: stone, na: E, nb: N },
    { a: [x1 - cn, y1], b: [x0 + cw, y1], p: pick('n'), na: N, nb: N },
    { a: [x0 + cw, y1], b: [x0, y1 - cw], p: stone, na: N, nb: W },
    { a: [x0, y1 - cw], b: [x0, y0 + cs], p: pick('w'), na: W, nb: W },
    { a: [x0, y0 + cs], b: [x0 + cs, y0], p: stone, na: W, nb: S },
  ]
  const ring: [number, number][] = []
  for (const e of edges) {
    if (Math.hypot(e.b[0] - e.a[0], e.b[1] - e.a[1]) < 1e-6) continue
    ring.push(e.a)
    if (!e.p) continue
    const A0: V3 = [e.a[0], e.a[1], z0], B0: V3 = [e.b[0], e.b[1], z0], B1: V3 = [e.b[0], e.b[1], z1], A1: V3 = [e.a[0], e.a[1], z1]
    const isChamfer = e.na !== e.nb
    const na = isChamfer ? unit([e.na[0] + e.nb[0] * 0.2, e.na[1] + e.nb[1] * 0.2, 0]) : e.na
    const nb = isChamfer ? unit([e.nb[0] + e.na[0] * 0.2, e.nb[1] + e.na[1] * 0.2, 0]) : e.nb
    e.p.tri(A0, B0, B1, undefined, undefined, undefined, [na, nb, nb])
    e.p.tri(A0, B1, A1, undefined, undefined, undefined, [na, nb, na])
  }
  const top = pick('top'), bottom = f.bottom ?? null
  if (top) for (let i = 1; i < ring.length - 1; i++)
    top.tri([...ring[0], z1], [...ring[i], z1], [...ring[i + 1], z1])
  if (bottom) for (let i = 1; i < ring.length - 1; i++)
    bottom.tri([...ring[0], z0], [...ring[i + 1], z0], [...ring[i], z0])
}

// ---------------------------------------------------------------------------
// A moulding swept round the chamfered outline, for the parapet: each step
// runs from (d0, z0) to (d1, z1), d outward from the wall plane, with a 2D
// normal (out, up) at each end so the bevel rolls from face to top.

type Rect = { x0: number; x1: number; y0: number; y1: number; c: number }
function ring(r: Rect, d: number, z: number) {
  const x0 = r.x0 - d, x1 = r.x1 + d, y0 = r.y0 - d, y1 = r.y1 + d
  const c = Math.max(0.05, r.c + d * 0.414)
  const pts: V3[] = [[x0 + c, y0, z], [x1 - c, y0, z], [x1, y0 + c, z], [x1, y1 - c, z],
    [x1 - c, y1, z], [x0 + c, y1, z], [x0, y1 - c, z], [x0, y0 + c, z]]
  const nrm: V3[] = [[0, -1, 0], [0, -1, 0], [1, 0, 0], [1, 0, 0], [0, 1, 0], [0, 1, 0], [-1, 0, 0], [-1, 0, 0]]
  return { pts, nrm }
}
type Step = { p: Part; d0: number; z0: number; d1: number; z1: number; n0: [number, number]; n1: [number, number] }
function sweep(r: Rect, steps: Step[]) {
  for (const s of steps) {
    const a = ring(r, s.d0, s.z0), b = ring(r, s.d1, s.z1)
    const at = (h: V3, n: [number, number]): V3 => unit([h[0] * n[0], h[1] * n[0], n[1]])
    for (let i = 0; i < 8; i++) {
      const j = (i + 1) % 8
      const n = [at(a.nrm[i], s.n0), at(a.nrm[j], s.n0), at(a.nrm[j], s.n1), at(a.nrm[i], s.n1)]
      s.p.tri(a.pts[i], a.pts[j], b.pts[j], undefined, undefined, undefined, [n[0], n[1], n[2]])
      s.p.tri(a.pts[i], b.pts[j], b.pts[i], undefined, undefined, undefined, [n[0], n[2], n[3]])
    }
  }
}
const OUT: [number, number] = [1, 0], UP: [number, number] = [0, 1], IN: [number, number] = [-1, 0]

// ---------------------------------------------------------------------------
// Flat polygons and prisms for the carved front, and the crumpled stone.

const dot = (a: V3, b: V3) => a[0] * b[0] + a[1] * b[1] + a[2] * b[2]

/** A flat polygon fanned from its first point (it must see every edge), wound to face `n`. */
function poly(p: Part, pts: V3[], n: V3) {
  for (let i = 1; i < pts.length - 1; i++) {
    const a = pts[0], b = pts[i], c = pts[i + 1]
    if (dot(cross(sub(b, a), sub(c, a)), n) >= 0) p.tri(a, b, c)
    else p.tri(a, c, b)
  }
}

/**
 * A prism: a polygon in the x–z plane pushed back from y0 to y1. Only the
 * front face and the listed side faces are drawn; `sides[k]` is the material
 * of the edge from point k to k + 1, or null where a neighbour hides it.
 */
function prism(front: Part | null, pts: [number, number][], y0: number, y1: number, sides: (Part | null)[]) {
  // The outline may be concave, so each side's outward normal comes from the
  // winding (signed area), not from the direction to the centroid.
  const area = pts.reduce((s, P, k) => { const Q = pts[(k + 1) % pts.length]; return s + P[0] * Q[1] - Q[0] * P[1] }, 0)
  const w = Math.sign(area)
  if (front) poly(front, pts.map(([x, z]): V3 => [x, y0, z]), [0, -1, 0])
  pts.forEach((P, k) => {
    const Q = pts[(k + 1) % pts.length], part = sides[k]
    if (!part) return
    const n: V3 = [w * (Q[1] - P[1]), 0, -w * (Q[0] - P[0])]
    poly(part, [[P[0], y0, P[1]], [Q[0], y0, Q[1]], [Q[0], y1, Q[1]], [P[0], y1, P[1]]], n)
  })
}

/**
 * A panel of the sandstone folded into a few big tilted planes: every edge of
 * the outline stays in the wall plane, and the planes meet at an apex sunk
 * `depth` into the wall, so the folds read as the cladding's broad facets and
 * nothing passes the outline. `pts` must all be visible from the apex.
 */
function fold(p: Part, pts: V3[], apex: V3, facing: V3) {
  pts.forEach((P, k) => poly(p, [apex, P, pts[(k + 1) % pts.length]], facing))
}

/**
 * A long side wall at x = `x` (facing ±x), cut by recessed openings. The wall
 * between the openings is split into as few rectangles as possible, each
 * folded into four tilted planes round an apex set off its centre, so the
 * creases run on the diagonal as the real cladding's do. Each opening is a
 * panel of its own material set `depth` into the wall, with stone reveals.
 */
type Opening = { y0: number; y1: number; z0: number; z1: number; p: Part; depth?: number }
function facade(x: number, facing: 1 | -1, y0: number, y1: number, z0: number, z1: number, ops: Opening[]) {
  const cuts = (a: number, b: number, vs: number[]) => [...new Set([a, b, ...vs.filter(v => v > a && v < b)])].sort((p, q) => p - q)
  const ys = cuts(y0, y1, ops.flatMap(o => [o.y0, o.y1])), zs = cuts(z0, z1, ops.flatMap(o => [o.z0, o.z1]))
  const open = (y: number, z: number) => ops.some(o => y > o.y0 && y < o.y1 && z > o.z0 && z < o.z1)
  // Each column of the grid becomes its runs of solid wall; neighbouring
  // columns with the same run merge into one wider panel.
  type R = { y0: number; y1: number; z0: number; z1: number }
  const panels: R[] = []
  for (let i = 0; i < ys.length - 1; i++) {
    const cy = (ys[i] + ys[i + 1]) / 2
    for (let j = 0; j < zs.length - 1;) {
      if (open(cy, (zs[j] + zs[j + 1]) / 2)) { j++; continue }
      let k = j
      while (k + 1 < zs.length - 1 && !open(cy, (zs[k + 1] + zs[k + 2]) / 2)) k++
      const r = { y0: ys[i], y1: ys[i + 1], z0: zs[j], z1: zs[k + 1] }
      const prev = panels.find(q => q.y1 === r.y0 && q.z0 === r.z0 && q.z1 === r.z1)
      if (prev) prev.y1 = r.y1
      else panels.push(r)
      j = k + 1
    }
  }
  const n: V3 = [facing, 0, 0]
  for (const r of panels) {
    const w = r.y1 - r.y0, h = r.z1 - r.z0
    // Off-centre apex, alternating side, so neighbouring panels fold differently.
    const t = (Math.round(r.y0 + r.z0) % 2) ? 0.35 : 0.65
    const apex: V3 = [x - facing * Math.min(2.2, 0.15 * Math.min(w, h)), r.y0 + w * t, r.z0 + h * (1 - t)]
    fold(stone, [[x, r.y0, r.z0], [x, r.y1, r.z0], [x, r.y1, r.z1], [x, r.y0, r.z1]], apex, n)
  }
  for (const o of ops) {
    const xi = x - facing * (o.depth ?? 0.6)
    poly(o.p, [[xi, o.y0, o.z0], [xi, o.y1, o.z0], [xi, o.y1, o.z1], [xi, o.y0, o.z1]], n)
    if (o.z0 > z0) poly(stone, [[x, o.y0, o.z0], [xi, o.y0, o.z0], [xi, o.y1, o.z0], [x, o.y1, o.z0]], [0, 0, 1])
    if (o.z1 < z1) poly(stone, [[x, o.y0, o.z1], [xi, o.y0, o.z1], [xi, o.y1, o.z1], [x, o.y1, o.z1]], [0, 0, -1])
    poly(stone, [[x, o.y0, o.z0], [xi, o.y0, o.z0], [xi, o.y0, o.z1], [x, o.y0, o.z1]], [0, 1, 0])
    poly(stone, [[x, o.y1, o.z0], [xi, o.y1, o.z0], [xi, o.y1, o.z1], [x, o.y1, o.z1]], [0, -1, 0])
  }
}

// ---------------------------------------------------------------------------
// Plan and section.

const X0 = -21, X1 = 21          // Duke Energy side, Levine Avenue side
const Y0 = -30.25, Y1 = 30.25    // Tryon front, Museum Tower behind
const C = 0.5                    // chamfer on the building's outer corners
const ROOF = 30.3, TOP = 31      // roof membrane, parapet and coping top
const YB = Y0 + 17               // back of the void: the atrium glass
const ATRIUM = YB - 1.2          // the atrium glass's front plane
const XM = 1                     // joint between the lower block and the projecting mass
const SLOT0 = -3                 // the atrium slot runs from here to XM
const BLOCK = 18                 // top of the lower stone block
const FRAME = X0 + 1.6           // inner face of the frame's Duke Energy side
const XT = 6                     // where the mass's sloping top reaches the roof
const SOFFIT = Y0 + 8            // back of the soffit
const SOFFIT_FRONT = 28.6, SOFFIT_BACK = 25.2  // its underside, rising to the front
const COPE = 0.5                 // the coping round the notch
const sw: [number, number, number, number] = [C, 0, 0, 0]
const se: [number, number, number, number] = [0, C, 0, 0]

// The body behind the Tryon zone. Its long sides are the facades below.
box(X0, X1, YB, Y1, 0, ROOF, { s: null, e: null, w: null, top: roof }, [0, 0, C, C])

// The lower block in the Duke Energy half, with a portal at its foot. Its top
// is the floor of the void, in the soffit's dark wood so the notch reads dark.
const PORTAL0 = X0 + 3.4, PORTAL1 = SLOT0 - 2.6, PORTAL_TOP = 11, PORTAL_BACK = Y0 + 3
// (The corner pier's top is drawn only inside the frame: run out to the wall
// plane, it would show through the Duke Energy side's sunken facets.)
box(X0, PORTAL0, Y0, YB, 0, BLOCK, { top: null, n: null, w: null }, sw)
poly(wood, [[FRAME, Y0, BLOCK], [PORTAL0, Y0, BLOCK], [PORTAL0, YB, BLOCK], [FRAME, YB, BLOCK]], [0, 0, 1])
box(PORTAL1, SLOT0, Y0, YB, 0, BLOCK, { top: wood, n: null, e: null })
box(PORTAL0, PORTAL1, Y0, YB, PORTAL_TOP, BLOCK, { top: wood, bottom: wood, n: null, e: null, w: null })
box(PORTAL0, PORTAL1, PORTAL_BACK, YB, 0, PORTAL_TOP, { s: win, e: null, w: null, n: null, top: null })
// The block's top falls towards the slot, as the projecting mass's rises
// away from it, so the void's floor is the V the photos show.
prism(stone, [[FRAME, BLOCK], [SLOT0, BLOCK], [FRAME, BLOCK + 3.5]], Y0, ATRIUM, [null, wood, null])
// The atrium: a slot of glass from the ground between the block and the mass,
// and its glass wall across the back of the void, run up to the coping so
// its top edge closes the notch in plan.
box(SLOT0, XM, Y0 + 2.5, ATRIUM, 0, BLOCK, { s: glass, e: glass, w: null, n: null, top: wood })
box(FRAME, XT, ATRIUM, YB, BLOCK, TOP, { s: glass, top: glass, n: glass, e: null, w: null })

// The frame's Duke Energy side, and the soffit: a deep slab of the frame over
// the left 60% of the front, its dark wood underside rising towards Tryon so
// it shows from the street as the photos' big angled ceiling. Behind it the
// void is open to the sky, so from above it is a notch with a dark floor.
box(X0, FRAME, Y0, YB, BLOCK, ROOF, { e: stone, w: null, n: null, top: roof }, sw)
{
  const f0 = SOFFIT_FRONT, b0 = SOFFIT_BACK
  poly(wood, [[FRAME, Y0, f0], [XT, Y0, f0], [XT, SOFFIT, b0], [FRAME, SOFFIT, b0]], [0, -(f0 - b0), -(SOFFIT - Y0)])
  poly(stone, [[FRAME, Y0, f0], [XT, Y0, f0], [XT, Y0, ROOF], [FRAME, Y0, ROOF]], [0, -1, 0])
  poly(stone, [[FRAME, SOFFIT, b0], [XT, SOFFIT, b0], [XT, SOFFIT, ROOF], [FRAME, SOFFIT, ROOF]], [0, 1, 0])
  poly(roof, [[FRAME, Y0, ROOF], [XT, Y0, ROOF], [XT, SOFFIT, ROOF], [FRAME, SOFFIT, ROOF]], [0, 0, 1])
}
// A darker coping round the notch's rim, so it stands out against the roof.
box(FRAME, XT, SOFFIT - COPE, SOFFIT, ROOF, TOP, { s: metal, n: stone, top: metal, e: null, w: null })
box(FRAME - COPE, FRAME, SOFFIT - COPE, YB, ROOF, TOP, { w: metal, e: stone, top: metal, s: null, n: metal })
box(XT, XT + COPE, SOFFIT - COPE, YB, ROOF, TOP, { e: metal, w: stone, top: metal, s: null, n: metal })

// The projecting mass: from the block its top rises on a long diagonal to the
// roof, the far side of the void (wood, as part of its floor). Below, it
// overhangs a deep glass lobby: its underside slopes from LIFT at the front
// down to LOBBY at the glass, set LOBBY_BACK behind the face, and a slim round
// column stands under the front edge. At Levine Avenue a stone wall comes
// down to the ground and closes the lobby's side.
const LIFT = 12, LOBBY = 9.5, LOBBY_BACK = Y0 + 8, SIDE = X1 - 1.2
const massFront: [number, number][] = [[X1 - C, LIFT], [X1 - C, ROOF], [XT, ROOF], [XM, BLOCK], [XM, LIFT]]
prism(null, massFront, Y0, YB, [null, roof, wood, stone, null])
poly(wood, [[XM, Y0, LIFT], [SIDE, Y0, LIFT], [SIDE, LOBBY_BACK, LOBBY], [XM, LOBBY_BACK, LOBBY]], [0, -(LIFT - LOBBY), -(LOBBY_BACK - Y0)])
poly(stone, [[XM, Y0, LIFT], [XM, LOBBY_BACK, LOBBY], [XM, LOBBY_BACK, LIFT]], [-1, 0, 0])
box(XM, SIDE, LOBBY_BACK, YB, 0, LOBBY, { s: win, w: null, e: null, n: null, top: null })
box(SIDE, X1, Y0, YB, 0, LIFT, { w: stone, e: null, n: null, top: null }, se)
// The corner chamfer the prism stops short of.
poly(stone, [[X1 - C, Y0, LIFT], [X1, Y0 + C, LIFT], [X1, Y0 + C, ROOF], [X1 - C, Y0, ROOF]], [1, -1, 0])
{
  const cx = 13.8, cy = Y0 + 2.2, r = 0.45, seg = 12, top = LIFT - (LIFT - LOBBY) * (cy - Y0) / (LOBBY_BACK - Y0)
  for (let k = 0; k < seg; k++) {
    const a = (k / seg) * 2 * Math.PI, b = ((k + 1) / seg) * 2 * Math.PI
    const na: V3 = [Math.cos(a), Math.sin(a), 0], nb: V3 = [Math.cos(b), Math.sin(b), 0]
    const A0: V3 = [cx + r * na[0], cy + r * na[1], 0], B0: V3 = [cx + r * nb[0], cy + r * nb[1], 0]
    const A1: V3 = [A0[0], A0[1], top], B1: V3 = [B0[0], B0[1], top]
    roof.tri(A0, B0, B1, undefined, undefined, undefined, [na, nb, nb])
    roof.tri(A0, B1, A1, undefined, undefined, undefined, [na, nb, na])
  }
}
// The mass's Tryon face: a few big tilted planes. The lower part folds round
// an apex low on the left, so its creases run from the overhang up to the
// right as in the photos; the upper band folds the same way beside the
// recessed window the photos show high on its right.
const BAND = 21, XB = XM + ((BAND - BLOCK) / (ROOF - BLOCK)) * (XT - XM)
const WIN = { x0: 11.8, x1: 17, z0: 22.5, z1: 27 }
{
  const at = (x: number, z: number): V3 => [x, Y0, z]
  const F: V3 = [0, -1, 0]
  fold(stone, [at(XM, LIFT), at(X1 - C, LIFT), at(X1 - C, BAND), at(XB, BAND), at(XM, BLOCK)], [7, Y0 + 2.6, 14.5], F)
  fold(stone, [at(XB, BAND), at(WIN.x0, BAND), at(WIN.x0, ROOF), at(XT, ROOF)], [8.5, Y0 + 1.6, 23.5], F)
  poly(stone, [at(WIN.x0, BAND), at(WIN.x1, BAND), at(WIN.x1, WIN.z0), at(WIN.x0, WIN.z0)], F)
  poly(stone, [at(WIN.x0, WIN.z1), at(WIN.x1, WIN.z1), at(WIN.x1, ROOF), at(WIN.x0, ROOF)], F)
  fold(stone, [at(WIN.x1, BAND), at(X1 - C, BAND), at(X1 - C, ROOF), at(WIN.x1, ROOF)], [19, Y0 + 0.5, 24], F)
  const d = Y0 + 0.9
  poly(win, [[WIN.x0, d, WIN.z0], [WIN.x1, d, WIN.z0], [WIN.x1, d, WIN.z1], [WIN.x0, d, WIN.z1]], F)
  poly(stone, [[WIN.x0, Y0, WIN.z0], [WIN.x1, Y0, WIN.z0], [WIN.x1, d, WIN.z0], [WIN.x0, d, WIN.z0]], [0, 0, 1])
  poly(stone, [[WIN.x0, Y0, WIN.z1], [WIN.x1, Y0, WIN.z1], [WIN.x1, d, WIN.z1], [WIN.x0, d, WIN.z1]], [0, 0, -1])
  poly(stone, [[WIN.x0, Y0, WIN.z0], [WIN.x0, d, WIN.z0], [WIN.x0, d, WIN.z1], [WIN.x0, Y0, WIN.z1]], [1, 0, 0])
  poly(stone, [[WIN.x1, Y0, WIN.z0], [WIN.x1, d, WIN.z0], [WIN.x1, d, WIN.z1], [WIN.x1, Y0, WIN.z1]], [-1, 0, 0])
}

// Levine Avenue side, from the photos, front to back: big folded panels of
// sandstone, glazing low down near the front, the full-height glass bay under
// its red-brown panel, one tall window behind it, and the rear shopfronts.
const BAY0 = Y0 + 22, BAY1 = Y0 + 29.5
facade(X1, 1, Y0 + C, Y1 - C, 0, ROOF, [
  { y0: Y0 + 13, y1: Y0 + 20, z0: 0, z1: 9, p: win, depth: 1 },
  { y0: BAY0, y1: BAY1, z0: 0, z1: 19.5, p: glass, depth: 1.5 },
  { y0: BAY0, y1: BAY1, z0: 19.5, z1: 28, p: metal, depth: 1.5 },
  { y0: 6, y1: 9.5, z0: 9, z1: 24, p: win },
  { y0: 2, y1: 28.5, z0: 0, z1: 5, p: win, depth: 1 },
])

// Duke Energy side: the frame and the lower block at the front, then the
// same folded sandstone with a high window, a tall glass bay and a glazed
// ground floor.
facade(X0, -1, Y0 + C, Y1 - C, 0, ROOF, [
  { y0: -12, y1: 1, z0: 22, z1: 28, p: win, depth: 0.9 },
  { y0: 5, y1: 10, z0: 0, z1: 26, p: glass, depth: 1.2 },
  { y0: 13, y1: 28.5, z0: 0, z1: 5, p: win, depth: 1 },
])

// Parapet round the whole roof: the wall runs up to a darker coping with a
// rounded outer lip.
const outline: Rect = { x0: X0, x1: X1, y0: Y0, y1: Y1, c: C }
sweep(outline, [
  { p: stone, d0: 0, z0: ROOF, d1: 0, z1: TOP - 0.35, n0: OUT, n1: OUT },
  { p: metal, d0: 0, z0: TOP - 0.35, d1: -0.35, z1: TOP, n0: OUT, n1: UP },
  { p: metal, d0: -0.35, z0: TOP, d1: -0.7, z1: TOP, n0: UP, n1: UP },
  { p: metal, d0: -0.7, z0: TOP, d1: -0.7, z1: ROOF, n0: IN, n1: IN },
])

// The shared palette (landmarks/STYLE.md). The buff sandstone is the
// building's identity, so it is a finish: the photos' golden buff pulled up
// to the palette's lightness. The soffit's dark-stained wood is the other
// defining colour, kept dark enough to read but no darker than charcoal.
// `metal` is the coping and the red-brown panel on the Levine side; the
// roof membrane and the corner column are `roof`; the wall openings are
// `window`, the atrium and the Levine bay `glass`.
const parts = [
  { part: stone, material: finish('mint-sandstone', 0xdcc298) },
  { part: wood, material: finish('mint-soffit-wood', 0x6b4c39) },
  { part: roof, material: PALETTE.roof },
  { part: metal, material: PALETTE.metal },
  { part: win, material: PALETTE.window },
  { part: glass, material: PALETTE.glass },
]
const triangles = parts.reduce((s, { part }) => s + part.triangles, 0)
if (triangles > 5000) throw new Error(`Triangle budget exceeded: ${triangles}`)
const glb = writeGlb('Mint Museum Uptown', parts, {
  license: 'CC0-1.0', frame: 'Y up, -Z north, +X east, metres; origin at the ground anchor',
  bearing: 318.5, osm: 'way/131139755', footprint: [X1 - X0, Y1 - Y0], height: TOP,
})
if (glb.length > 256000) throw new Error(`File budget exceeded: ${glb.length}`)
const out = new URL('../../landmarks/models/mint-museum-uptown.glb', import.meta.url).pathname
await Bun.write(out, glb)
console.log(`${out}: ${triangles} triangles, ${glb.length} bytes (${(glb.length / 1024).toFixed(1)} KiB)`)
