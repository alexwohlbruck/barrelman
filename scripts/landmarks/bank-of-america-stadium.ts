/**
 * Bank of America Stadium, Charlotte — procedural, CC0-1.0, no textures.
 * bun scripts/landmarks/bank-of-america-stadium.ts
 *
 * Map frame: x across the field, y along it, z up, metres. Placed at bearing
 * 320°, the field's long axis (OSM pitch way/187011790), so +y points up the
 * field to the north-west end. The anchor is the area centroid of the stadium
 * outline (way/180768146, the outer ring of building=stadium relation/12346952).
 *
 * The bowl is a set of rounded rectangles (superellipses) fitted to the OSM
 * building:part rings, centred on the field 8 m up the y axis from the
 * anchor, so the bowl is exactly symmetrical:
 *   R0 the field wall (inner ring of relation/12346952 and of the 7 m part),
 *   R1 the 7 m ring (relation/17145756), R2 the 15 m ring (relation/17917759),
 *   R3 the 43 m upper-deck ring (relation/17917766).
 * Seating is broad bands with no rows drawn: the blue lower bowl, a cross
 * aisle, the silver club seats behind it, the black suite fascia and the blue
 * upper deck. Outside R3 the
 * concourse podium follows the real outline (mirrored across the field's axis
 * so it stays symmetrical) at the heights of the OSM perimeter parts, 24 to
 * 32 m. Above it runs the white arcade of round arches and the upper deck's
 * grey back. Six black stair towers carry the teal glass cupolas, the two end
 * video boards stand on the rim, and white arched light rigs lean over the
 * upper deck.
 */
import { Part, cross, sub, writeGlb, type V3 } from './mesh'

type XY = [number, number]

const seats = new Part(), precast = new Part(), black = new Part(), glass = new Part()
const white = new Part(), cupola = new Part(), turf = new Part(), silver = new Part()

const dot = (a: V3, b: V3) => a[0] * b[0] + a[1] * b[1] + a[2] * b[2]
const add = (a: V3, b: V3): V3 => [a[0] + b[0], a[1] + b[1], a[2] + b[2]]
const mul = (a: V3, k: number): V3 => [a[0] * k, a[1] * k, a[2] * k]
const unit = (a: V3): V3 => mul(a, 1 / (Math.hypot(...a) || 1))

/** A quad, wound to face `hint`; per-corner normals if given, else flat. */
function quad(p: Part, P: V3[], hint: V3, ns?: V3[]) {
  const face = cross(sub(P[1], P[0]), sub(P[2], P[0]))
  const ord = dot(face, hint) >= 0 ? [0, 1, 2, 3] : [0, 3, 2, 1]
  const n = ns?.map(unit)
  const t = (a: number, b: number, c: number) => {
    const A = P[ord[a]], B = P[ord[b]], C = P[ord[c]]
    if (Math.hypot(...cross(sub(B, A), sub(C, A))) < 1e-9) return
    p.tri(A, B, C, undefined, undefined, undefined, n && [n[ord[a]], n[ord[b]], n[ord[c]]])
  }
  t(0, 1, 2)
  t(0, 2, 3)
}

// ---------------------------------------------------------------------------
// Bowl rings. Fitted to the OSM parts in this frame: half width a, half
// length b, exponent n, centred on (0, C).

const N = 64 // samples round every ring, uniform along R3's perimeter
const C = 8
type SE = { a: number; b: number; n: number }
const R0: SE = { a: 42, b: 60.8, n: 6.7 }
const R1: SE = { a: 61.8, b: 80, n: 4.9 }
const R2: SE = { a: 78.2, b: 95.1, n: 3.6 }
const R3: SE = { a: 105.9, b: 112.7, n: 3.8 }

/** Ray directions from the bowl centre, evenly spaced along R3. */
const dirs: XY[] = (() => {
  const pts: XY[] = [], M = 4000
  const sp = (v: number, e: number) => Math.sign(v) * Math.pow(Math.abs(v), e)
  for (let i = 0; i <= M; i++) {
    const t = (i / M) * 2 * Math.PI
    pts.push([R3.a * sp(Math.cos(t), 2 / R3.n), R3.b * sp(Math.sin(t), 2 / R3.n)])
  }
  const cum = [0]
  for (let i = 1; i <= M; i++) cum.push(cum[i - 1] + Math.hypot(pts[i][0] - pts[i - 1][0], pts[i][1] - pts[i - 1][1]))
  const out: XY[] = []
  let j = 0
  for (let k = 0; k < N; k++) {
    const s = (k / N) * cum[M]
    while (cum[j + 1] < s) j++
    const f = (s - cum[j]) / (cum[j + 1] - cum[j] || 1)
    const x = pts[j][0] + (pts[j + 1][0] - pts[j][0]) * f, y = pts[j][1] + (pts[j + 1][1] - pts[j][1]) * f
    const l = Math.hypot(x, y)
    out.push([x / l, y / l])
  }
  return out
})()

const radius = (se: SE, k: number, off = 0) => {
  const [dx, dy] = dirs[k % N]
  return 1 / Math.pow(Math.pow(Math.abs(dx) / (se.a + off), se.n) + Math.pow(Math.abs(dy) / (se.b + off), se.n), 1 / se.n)
}
const at = (k: number, r: number, z: number): V3 => [dirs[k % N][0] * r, C + dirs[k % N][1] * r, z]
const ringPt = (se: SE, k: number, z: number, off = 0) => at(k, radius(se, k, off), z)
/** Mirror across the field's long axis (x → -x). */
const mirrorV = (k: number) => (N / 2 - k + N) % N
const mirrorS = (k: number) => (N / 2 - k - 1 + N) % N

/**
 * A band between two rings, smooth-shaded round the bowl. `z` gives each
 * ring's height per sample; `face` is +1 for a band facing out or up-and-out,
 * -1 for one facing in towards the field (or up, for a flat ring).
 */
function band(p: Part, lo: (k: number) => V3, hi: (k: number) => V3, face: 1 | -1 | 'up') {
  const faceN = (k: number): V3 => {
    const a = lo(k), b = lo(k + 1), c = hi(k)
    let n = unit(cross(sub(b, a), sub(c, a)))
    const out: V3 = [dirs[k % N][0], dirs[k % N][1], 0]
    const want = face === 'up' ? [0, 0, 1] as V3 : mul(out, face)
    if (dot(n, want) < 0) n = mul(n, -1)
    return n
  }
  const fn = Array.from({ length: N }, (_, k) => faceN(k))
  const vn = (k: number) => unit(add(fn[(k - 1 + N) % N], fn[k % N]))
  for (let k = 0; k < N; k++) {
    const P = [lo(k), lo(k + 1), hi(k + 1), hi(k)]
    const n = vn(k), m = vn(k + 1)
    quad(p, P, fn[k], [n, m, m, n])
  }
}

// Rim height: the upper deck is deepest on the long sides, where it reaches
// the OSM 43 m; it keeps the same rake towards the ends, so the rim drops to
// about 39 m behind the end zones.
const depth = Array.from({ length: N }, (_, k) => radius(R3, k) - radius(R2, k))
const dMin = Math.min(...depth), dMax = Math.max(...depth)
const rim = (k: number) => 39 + (4 * (depth[k % N] - dMin)) / (dMax - dMin)

const FIELD = 0, WALL = 1.8, AISLE = 7, CLUB = 14, DECK = 21

// No field: the map draws the pitch, and a second copy would float above
// the terrain on a slope (STYLE.md, "Don't model the ground").

// Bowl, from the field outwards.
band(black, k => ringPt(R0, k, FIELD), k => ringPt(R0, k, WALL), -1) // field wall
band(seats, k => ringPt(R0, k, WALL), k => ringPt(R1, k, AISLE), -1) // lower bowl
band(precast, k => ringPt(R1, k, AISLE), k => ringPt(R1, k, AISLE, 1.8), 'up') // cross aisle
band(silver, k => ringPt(R1, k, AISLE, 1.8), k => ringPt(R2, k, CLUB), -1) // silver club seats
band(black, k => ringPt(R2, k, CLUB), k => ringPt(R2, k, DECK), -1) // suite fascia
band(precast, k => ringPt(R2, k, DECK), k => ringPt(R2, k, DECK, 0.6), 'up') // fascia lip
band(seats, k => ringPt(R2, k, DECK, 0.6), k => ringPt(R3, k, rim(k) - 1.1, -1.2), -1) // upper deck
band(precast, k => ringPt(R3, k, rim(k) - 1.1, -1.2), k => ringPt(R3, k, rim(k), -1.2), -1) // parapet
band(precast, k => ringPt(R3, k, rim(k), -1.2), k => ringPt(R3, k, rim(k), -0.45), 'up') // rim
band(precast, k => ringPt(R3, k, rim(k) - 0.45), k => ringPt(R3, k, rim(k), -0.45), 1) // bevel

// ---------------------------------------------------------------------------
// Podium: the concourse and office blocks between R3 and the outline.

// OSM outline (way/180768146) in this frame.
const OUTLINE: XY[] = [[120.6,38.7],[120.6,24.4],[120.6,20.2],[120.5,13.5],[120.5,2.4],[120.5,-4.8],[120.5,-7.5],[120.8,-22.0],[120.5,-30.1],[122.8,-30.5],[122.0,-39.5],[121.0,-47.3],[119.4,-56.4],[116.8,-66.9],[113.2,-75.6],[105.5,-72.2],[103.2,-77.1],[93.7,-91.8],[80.8,-103.6],[75.4,-107.4],[80.7,-115.2],[70.7,-121.7],[54.2,-128.5],[46.1,-130.2],[44.3,-130.6],[30.9,-131.0],[16.3,-131.0],[13.4,-131.0],[0.7,-131.0],[-13.1,-131.0],[-15.5,-131.0],[-30.3,-131.0],[-34.8,-131.1],[-34.7,-125.9],[-42.1,-130.8],[-45.3,-130.2],[-53.7,-128.3],[-70.8,-121.3],[-79.8,-114.9],[-75.2,-108.0],[-80.5,-104.4],[-93.9,-91.2],[-102.9,-77.2],[-104.3,-74.2],[-105.8,-71.0],[-113.7,-75.1],[-117.2,-64.1],[-120.7,-50.0],[-121.5,-37.5],[-121.9,-30.3],[-113.9,-30.1],[-114.2,-22.6],[-120.5,-22.6],[-120.7,-7.4],[-120.7,7.7],[-120.7,24.4],[-120.9,38.1],[-116.5,38.0],[-114.9,38.0],[-114.1,50.3],[-112.5,62.1],[-110.6,70.2],[-107.5,82.4],[-102.7,94.1],[-96.2,91.2],[-91.0,99.3],[-85.1,106.9],[-74.5,116.8],[-63.7,124.5],[-58.4,126.3],[-48.1,129.8],[-33.0,132.2],[-31.6,125.1],[-21.1,125.1],[-21.1,133.0],[-12.0,135.8],[-0.6,137.2],[10.3,135.9],[20.1,132.8],[20.2,125.1],[37.2,125.1],[37.3,121.6],[44.9,121.6],[60.8,119.6],[63.4,124.2],[76.1,115.5],[76.7,114.9],[82.0,110.0],[86.7,104.7],[87.5,103.8],[88.1,103.1],[91.5,97.5],[95.5,90.9],[102.2,95.0],[108.2,83.1],[112.2,66.8],[113.6,55.3],[113.6,38.8]]

// The OSM perimeter building:parts that set the podium's height, in this
// frame. Anything not covered is 24 m, the commonest height.
const BLOCKS: [number, XY[]][] = [
  [27,[[-46.5,121.5],[-59.3,119.4],[-72.3,114.1],[-82.8,106.2],[-91.0,99.3],[-85.1,106.9],[-74.5,116.8],[-63.7,124.5],[-58.4,126.3],[-48.1,129.8]]],
  [31,[[-113.9,-30.1],[-121.9,-30.3],[-121.5,-37.5],[-120.7,-50.0],[-117.2,-64.1],[-113.7,-75.1],[-105.8,-71.0],[-109.3,-61.5],[-112.7,-46.0]]],
  [27,[[-96.2,-73.8],[-102.9,-77.2],[-93.9,-91.2],[-80.5,-104.4],[-74.2,-96.2],[-86.9,-87.0]]],
  [31,[[-120.7,24.4],[-120.7,7.7],[-120.7,-7.4],[-113.3,-7.3],[-113.5,24.5]]],
  [31,[[41.9,-121.2],[44.3,-130.6],[46.1,-130.2],[54.2,-128.5],[70.7,-121.7],[80.7,-115.2],[75.4,-107.4],[66.7,-112.5],[51.3,-119.1]]],
  [27,[[75.3,-95.7],[87.1,-86.5],[96.3,-73.5],[103.2,-77.1],[93.7,-91.8],[80.8,-103.6]]],
  [31,[[105.5,-72.2],[108.9,-63.5],[111.5,-53.0],[113.2,-42.7],[114.1,-29.7],[120.5,-30.1],[122.8,-30.5],[122.0,-39.5],[121.0,-47.3],[119.4,-56.4],[116.8,-66.9],[113.2,-75.6]]],
  [31,[[-9.9,-113.4],[-9.9,-115.4],[-15.6,-115.4],[-15.5,-131.0],[-13.1,-131.0],[0.7,-131.0],[13.4,-131.0],[16.3,-131.0],[16.2,-115.5],[10.0,-115.5],[10.0,-113.4]]],
  [31,[[114.4,24.5],[114.4,-7.5],[120.5,-7.5],[120.5,-4.8],[120.5,2.4],[120.5,13.5],[120.6,20.2],[120.6,24.4]]],
  [27,[[88.1,103.1],[87.5,103.8],[86.7,104.7],[82.0,110.0],[76.7,114.9],[76.1,115.5],[63.4,124.2],[60.8,119.6],[74.3,113.1]]],
  [32,[[10.3,135.9],[-0.6,137.2],[-12.0,135.8],[-21.1,133.0],[-21.1,125.1],[20.2,125.1],[20.1,132.8]]],
  [31,[[-40.2,-121.7],[-42.1,-130.8],[-45.3,-130.2],[-53.7,-128.3],[-70.8,-121.3],[-79.8,-114.9],[-75.2,-108.0],[-66.4,-113.5],[-50.4,-119.7]]],
]

function inside([x, y]: XY, poly: XY[]) {
  let c = false
  for (let i = 0, j = poly.length - 1; i < poly.length; j = i++) {
    const [x1, y1] = poly[i], [x2, y2] = poly[j]
    if ((y1 > y) !== (y2 > y) && x < ((x2 - x1) * (y - y1)) / (y2 - y1) + x1) c = !c
  }
  return c
}
/** Farthest crossing of the outline along a ray from the bowl centre. */
function outlineRadius([dx, dy]: XY) {
  let best = 0
  for (let i = 0; i < OUTLINE.length; i++) {
    const [x1, y1] = OUTLINE[i], [x2, y2] = OUTLINE[(i + 1) % OUTLINE.length]
    const ex = x2 - x1, ey = y2 - y1, den = dx * ey - dy * ex
    if (Math.abs(den) < 1e-9) continue
    const t = (x1 * ey - (y1 - C) * ex) / den, s = (x1 * dy - (y1 - C) * dx) / den
    if (t > 0 && s >= 0 && s <= 1) best = Math.max(best, t)
  }
  return best
}
// Mirrored so the podium is symmetrical; the wider side wins, so the model
// still covers every part it replaces.
const rawOut = dirs.map(outlineRadius)
const rOut = rawOut.map((r, k) => Math.max(r, rawOut[mirrorV(k)]))
const segDir = (k: number): XY => {
  const a = dirs[k], b = dirs[(k + 1) % N], l = Math.hypot(a[0] + b[0], a[1] + b[1])
  return [(a[0] + b[0]) / l, (a[1] + b[1]) / l]
}
const rawH = Array.from({ length: N }, (_, k) => {
  const d = segDir(k), r0 = (radius(R3, k) + radius(R3, k + 1)) / 2, r1 = (rOut[k] + rOut[(k + 1) % N]) / 2
  let h = 24
  for (const f of [0.3, 0.6, 0.9]) {
    const r = r0 + (r1 - r0) * f, pt: XY = [d[0] * r, C + d[1] * r]
    for (const [bh, poly] of BLOCKS) if (inside(pt, poly)) h = Math.max(h, bh)
  }
  return h
})
const podH = rawH.map((h, k) => Math.max(h, rawH[mirrorS(k)]))

const SLOT = 1.6 // depth of the arcade, behind the arches
const PLINTH = 4, BEVEL = 0.5

for (let k = 0; k < N; k++) {
  const k1 = (k + 1) % N, h = podH[k]
  const o0 = at(k, rOut[k], 0), o1 = at(k1, rOut[k1], 0)
  const i0 = ringPt(R3, k, 0, -SLOT), i1 = ringPt(R3, k1, 0, -SLOT)
  const L = Math.hypot(o1[0] - o0[0], o1[1] - o0[1])
  const u: V3 = [(o1[0] - o0[0]) / L, (o1[1] - o0[1]) / L, 0], out: V3 = [u[1], -u[0], 0]
  const v = (s: number, z: number, d = 0): V3 => [o0[0] + u[0] * s - out[0] * d, o0[1] + u[1] * s - out[1] * d, z]
  const up: V3 = [0, 0, 1]

  // Roof, with a rounded edge back from the facade.
  const zr = (p: V3, z: number): V3 => [p[0], p[1], z]
  quad(precast, [v(0, h, BEVEL), v(L, h, BEVEL), zr(i1, h), zr(i0, h)], up)
  const nb = unit(add(out, up))
  quad(precast, [v(0, h - BEVEL), v(L, h - BEVEL), v(L, h, BEVEL), v(0, h, BEVEL)], nb, [out, out, nb, nb].map((n, i) => i < 2 ? unit(add(out, mul(nb, 0.6))) : unit(add(up, mul(nb, 0.6)))))

  // Facade: a black plinth, then one recessed band of glass per bay between
  // dark piers, under a grey precast parapet.
  const pier = Math.min(1.8, L * 0.16), top = h - 3.2, D = 0.7
  if (L > 4) {
    quad(black, [v(0, 0), v(L, 0), v(L, PLINTH), v(0, PLINTH)], out)
    quad(black, [v(0, PLINTH), v(pier, PLINTH), v(pier, top), v(0, top)], out)
    quad(black, [v(L - pier, PLINTH), v(L, PLINTH), v(L, top), v(L - pier, top)], out)
    quad(precast, [v(0, top), v(L, top), v(L, h - BEVEL), v(0, h - BEVEL)], out)
    quad(glass, [v(pier, PLINTH, D), v(L - pier, PLINTH, D), v(L - pier, top, D), v(pier, top, D)], out)
    quad(black, [v(pier, PLINTH), v(L - pier, PLINTH), v(L - pier, PLINTH, D), v(pier, PLINTH, D)], up)
    quad(black, [v(pier, PLINTH), v(pier, PLINTH, D), v(pier, top, D), v(pier, top)], u)
    quad(black, [v(L - pier, PLINTH), v(L - pier, PLINTH, D), v(L - pier, top, D), v(L - pier, top)], mul(u, -1))
  } else quad(precast, [v(0, 0), v(L, 0), v(L, h - BEVEL), v(0, h - BEVEL)], out)

  // A step between neighbouring blocks of different heights.
  const hp = podH[(k - 1 + N) % N]
  if (hp !== h) {
    const lo = Math.min(h, hp), hi = Math.max(h, hp)
    const t: V3 = [-dirs[k][1], dirs[k][0], 0]
    quad(precast, [zr(i0, lo), zr(o0, lo), zr(o0, hi), zr(i0, hi)], h > hp ? mul(t, -1) : t)
  }
}

// ---------------------------------------------------------------------------
// Upper structure outside R3: white arcade of round arches springing from the
// podium roof, a dark concourse behind, the grey back of the upper deck above.

const ARC = 8
for (let k = 0; k < N; k++) {
  const k1 = (k + 1) % N, h = podH[k]
  const a = ringPt(R3, k, 0), b = ringPt(R3, k1, 0)
  const L = Math.hypot(b[0] - a[0], b[1] - a[1])
  const u: V3 = [(b[0] - a[0]) / L, (b[1] - a[1]) / L, 0], out: V3 = [u[1], -u[0], 0]
  const v = (s: number, z: number): V3 => [a[0] + u[0] * s, a[1] + u[1] * s, z]
  const pier = 1.6, r = (L - pier) / 2, sc = L / 2
  const zs = h + 2.6, zt = Math.min(zs + r + 0.9, Math.min(rim(k), rim(k1)) - 1.2)
  const top = (s: number) => {
    const t = s / L
    return rim(k) * (1 - t) + rim(k1) * t - 0.45
  }
  // Grey band up to the rim's bevel.
  quad(precast, [v(0, zt), v(L, zt), [b[0], b[1], top(L)], [a[0], a[1], top(0)]], out)
  // Concourse wall behind the arches.
  const ia = ringPt(R3, k, 0, -SLOT), ib = ringPt(R3, k1, 0, -SLOT)
  quad(glass, [[ia[0], ia[1], h], [ib[0], ib[1], h], [ib[0], ib[1], zt], [ia[0], ia[1], zt]], out)
  if (zs + r > zt - 0.3) {
    quad(white, [v(0, h), v(L, h), v(L, zt), v(0, zt)], out)
    continue
  }
  quad(white, [v(0, h), v(pier / 2, h), v(pier / 2, zt), v(0, zt)], out)
  quad(white, [v(L - pier / 2, h), v(L, h), v(L, zt), v(L - pier / 2, zt)], out)
  for (let j = 0; j < ARC; j++) {
    const t0 = Math.PI - (j * Math.PI) / ARC, t1 = Math.PI - ((j + 1) * Math.PI) / ARC
    const s0 = sc + r * Math.cos(t0), s1 = sc + r * Math.cos(t1)
    quad(white, [v(s0, zs + r * Math.sin(t0)), v(s1, zs + r * Math.sin(t1)), v(s1, zt), v(s0, zt)], out)
  }
}

// ---------------------------------------------------------------------------
// Stair towers: black, square, each under a teal glass cupola.

const TOWERS: XY[] = [[113.5, C - 23], [113.5, C + 23], [-113.5, C - 23], [-113.5, C + 23], [23.2, -123], [-23.2, -123]]
const TH = 33, HALF = 7.5, CH = 1.0, SEG = 12
for (const [cx, cy] of TOWERS) {
  // Octagonal ring: a square with chamfered corners, smooth at the chamfers.
  const ring = (d: number, z: number): { p: V3; n: V3 }[] => {
    const h = HALF - d, c = CH - d * 0.414
    const pts: XY[] = [[h - c, -h], [h, -h + c], [h, h - c], [h - c, h], [-h + c, h], [-h, h - c], [-h, -h + c], [-h + c, -h]]
    const nrm: XY[] = [[0, -1], [1, 0], [1, 0], [0, 1], [0, 1], [-1, 0], [-1, 0], [0, -1]]
    return pts.map((p, i) => ({ p: [cx + p[0], cy + p[1], z], n: [nrm[i][0], nrm[i][1], 0] }))
  }
  const lo = ring(0, 0), mid = ring(0, TH - 0.6), hi = ring(0.6, TH)
  for (let i = 0; i < 8; i++) {
    const j = (i + 1) % 8
    const corner = i % 2 === 0 // edges 0,2,4,6 run round a chamfer
    const nA = corner ? unit(add(lo[i].n, lo[j].n)) : lo[i].n, nB = corner ? unit(add(lo[i].n, lo[j].n)) : lo[j].n
    const fn = unit(add(lo[i].n, lo[j].n))
    quad(black, [lo[i].p, lo[j].p, mid[j].p, mid[i].p], fn, corner ? [lo[i].n, lo[j].n, lo[j].n, lo[i].n] : [nA, nB, nB, nA])
    const tA = unit(add(lo[i].n, [0, 0, 1])), tB = unit(add(lo[j].n, [0, 0, 1]))
    quad(black, [mid[i].p, mid[j].p, hi[j].p, hi[i].p], unit(add(fn, [0, 0, 1])), [lo[i].n, lo[j].n, tB, tA])
  }
  for (let i = 1; i < 7; i++) quad(black, [hi[0].p, hi[i].p, hi[i + 1].p, hi[i + 1].p], [0, 0, 1])
  // A tall Panthers-blue panel on the outward face, as on the real towers.
  const side = Math.abs(cx) > 60 ? ([Math.sign(cx), 0] as XY) : ([0, -1] as XY)
  const t: XY = [-side[1], side[0]]
  const fx = cx + side[0] * (HALF + 0.05), fy = cy + side[1] * (HALF + 0.05)
  const P = (s: number, z: number): V3 => [fx + t[0] * s, fy + t[1] * s, z]
  quad(seats, [P(-3, 9), P(3, 9), P(3, 28), P(-3, 28)], [side[0], side[1], 0])

  // Drum and cupola: a short glass cylinder under a low dome.
  const RD = 6.2, Z0 = TH, Z1 = TH + 2.2, ZT = 38.6, ROWS = 3
  const pt = (i: number, r: number, z: number): V3 => [cx + r * Math.cos((i * 2 * Math.PI) / SEG), cy + r * Math.sin((i * 2 * Math.PI) / SEG), z]
  const rn = (i: number, up = 0): V3 => unit([Math.cos((i * 2 * Math.PI) / SEG), Math.sin((i * 2 * Math.PI) / SEG), up])
  for (let i = 0; i < SEG; i++) {
    quad(cupola, [pt(i, RD, Z0), pt(i + 1, RD, Z0), pt(i + 1, RD, Z1), pt(i, RD, Z1)], rn(i), [rn(i), rn(i + 1), rn(i + 1), rn(i)])
    for (let j = 0; j < ROWS; j++) {
      const a0 = (j * Math.PI) / 2 / ROWS, a1 = ((j + 1) * Math.PI) / 2 / ROWS
      const H = ZT - Z1
      const r0 = RD * Math.cos(a0), r1 = RD * Math.cos(a1), z0 = Z1 + H * Math.sin(a0), z1 = Z1 + H * Math.sin(a1)
      // Normals of the squashed hemisphere.
      const n = (ii: number, a: number) => unit([Math.cos((ii * 2 * Math.PI) / SEG) * Math.cos(a) / RD, Math.sin((ii * 2 * Math.PI) / SEG) * Math.cos(a) / RD, Math.sin(a) / H])
      quad(cupola, [pt(i, r0, z0), pt(i + 1, r0, z0), pt(i + 1, r1, z1), pt(i, r1, z1)], n(i, (a0 + a1) / 2), [n(i, a0), n(i + 1, a0), n(i + 1, a1), n(i, a1)])
    }
  }
}

// ---------------------------------------------------------------------------
// Video boards over both end zones, on the rim: black screens facing the
// field, grey backs. OSM gives them 76 m wide and 58 m to the top.

for (const s of [-1, 1]) {
  const y = C + s * R3.b, k = s > 0 ? N / 4 : (3 * N) / 4
  const z0 = rim(k) - 1.0, z1 = 58, x = 38, d = 2.0
  const yIn = y - s * d, yOut = y + s * d
  const P = (xx: number, yy: number, z: number): V3 => [xx, yy, z]
  quad(black, [P(-x, yIn, z0), P(x, yIn, z0), P(x, yIn, z1), P(-x, yIn, z1)], [0, -s, 0])
  quad(precast, [P(-x, yOut, z0), P(x, yOut, z0), P(x, yOut, z1), P(-x, yOut, z1)], [0, s, 0])
  quad(black, [P(-x, yIn, z1), P(x, yIn, z1), P(x, yOut, z1), P(-x, yOut, z1)], [0, 0, 1])
  for (const e of [-1, 1]) quad(black, [P(e * x, yIn, z0), P(e * x, yOut, z0), P(e * x, yOut, z1), P(e * x, yIn, z1)], [e, 0, 0])
  quad(black, [P(-x, yIn, z0), P(x, yIn, z0), P(x, yOut, z0), P(-x, yOut, z0)], [0, 0, -1])
}

// ---------------------------------------------------------------------------
// Light rigs: pairs of white arms rising from the rim and curving in over
// the upper deck, carrying a bank of lamps. At the four corners and two along
// each long side.

/** A square-section tube along a path, closed at the top end. */
function tube(p: Part, path: V3[], side: V3, w: number) {
  const secs = path.map((c, i) => {
    const f = unit(sub(path[Math.min(i + 1, path.length - 1)], path[Math.max(i - 1, 0)]))
    const n = unit(cross(side, f)) // the other cross axis
    const s = mul(side, w / 2), m = mul(n, w / 2)
    return [add(add(c, s), m), add(sub(c, s), m), sub(sub(c, s), m), sub(add(c, s), m)] as V3[]
  })
  for (let i = 0; i < secs.length - 1; i++) {
    const c = path[i]
    for (let e = 0; e < 4; e++) {
      const f = (e + 1) % 4
      const P = [secs[i][e], secs[i][f], secs[i + 1][f], secs[i + 1][e]]
      const mc = mul(add(add(P[0], P[1]), add(P[2], P[3])), 0.25)
      quad(p, P, sub(mc, mul(add(c, path[i + 1]), 0.5)))
    }
  }
}
/** An oriented box: centre, half extents along three axes. */
function box(p: Part, c: V3, ax: [V3, V3, V3], h: [number, number, number]) {
  const P = (a: number, b: number, d: number) => add(add(add(c, mul(ax[0], a * h[0])), mul(ax[1], b * h[1])), mul(ax[2], d * h[2]))
  for (const [i, sgn] of [[0, 1], [0, -1], [1, 1], [1, -1], [2, 1]] as [number, number][]) {
    const o = [0, 1, 2].filter(x => x !== i)
    const corner = (a: number, b: number) => {
      const v = [0, 0, 0]
      v[i] = sgn; v[o[0]] = a; v[o[1]] = b
      return P(v[0], v[1], v[2])
    }
    quad(p, [corner(-1, -1), corner(1, -1), corner(1, 1), corner(-1, 1)], mul(ax[i], sgn))
  }
}

const RIGS = [3, N / 8, N / 2 - N / 8, N / 2 - 3, N / 2 + 3, N / 2 + N / 8, N - N / 8, N - 3]
for (const k of RIGS) {
  const base = ringPt(R3, k, rim(k), -0.8)
  const outD: V3 = [dirs[k][0], dirs[k][1], 0], t: V3 = [-dirs[k][1], dirs[k][0], 0]
  const RC = 15, SPAN = Math.PI / 3, STEPS = 4
  const tips: V3[] = []
  for (const e of [-1, 1]) {
    const root = add(base, mul(t, e * 3.2))
    const path = Array.from({ length: STEPS + 1 }, (_, i) => {
      const a = (i / STEPS) * SPAN
      return add(add(root, mul(outD, -RC * (1 - Math.cos(a)))), [0, 0, RC * Math.sin(a)])
    })
    tube(white, path, t, 1.5)
    tips.push(path[STEPS])
  }
  const c = mul(add(tips[0], tips[1]), 0.5)
  box(precast, add(c, [0, 0, 1.4]), [t, outD, [0, 0, 1]], [6.5, 1.0, 2.2])
}

// ---------------------------------------------------------------------------
// sRGB colours read off daylight photos: the Panthers-blue seats in sun, the
// silver club seats, the pale grey precast and podium roofs, black towers, fascia and screens, the
// blue-grey glazing, white arches and light rigs and the teal glass cupolas;
// the field is the map's.
const parts = [
  { part: seats, material: { name: 'seats', color: 0x2a8fd2 } },
  { part: silver, material: { name: 'silver-seats', color: 0xa3a8ae } },
  { part: precast, material: { name: 'precast', color: 0xbdbab3 } },
  { part: black, material: { name: 'black', color: 0x26282d } },
  { part: glass, material: { name: 'glass', color: 0x6c7e90 } },
  { part: white, material: { name: 'white', color: 0xe9e7e1 } },
  { part: cupola, material: { name: 'cupola-glass', color: 0x8fbab3 } },
]
const triangles = parts.reduce((s, { part }) => s + part.triangles, 0)
for (const { part, material } of parts) console.log(material.name.padEnd(14), part.triangles)
if (triangles > 6500) throw new Error(`Triangle budget exceeded: ${triangles}`)
const glb = writeGlb('Bank of America Stadium', parts, {
  license: 'CC0-1.0', frame: 'Y up, -Z north, +X east, metres; origin at the ground anchor',
  bearing: 320, osm: 'relation/12346952', footprint: [243, 268], height: 58,
})
if (glb.length > 256000) throw new Error(`File budget exceeded: ${glb.length}`)
const out = new URL('../../landmarks/models/bank-of-america-stadium.glb', import.meta.url).pathname
await Bun.write(out, glb)
console.log(`${out}: ${triangles} triangles, ${glb.length} bytes (${(glb.length / 1024).toFixed(1)} KiB)`)
