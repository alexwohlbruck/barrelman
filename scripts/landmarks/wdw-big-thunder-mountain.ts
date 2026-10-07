/**
 * Big Thunder Mountain Railroad, Frontierland, Magic Kingdom — the
 * rockwork, the track and the station. Procedural, CC0-1.0.
 * bun scripts/landmarks/wdw-big-thunder-mountain.ts
 *
 * Map frame: x east, y north, z up, metres. Origin at OSM's peak node
 * (node/12797882777, -81.5846891, 28.4205673), on the ground. Bearing 0.
 *
 * One fused mass of red sandstone, after Monument Valley:
 *  - a broad, low rock base over the whole ride site (OSM's rock area,
 *    way/698843396), 3–9 m high, terraced in 1.3 m steps, rising over the
 *    track's tunnels and cut down into canyons where the track runs open;
 *  - hoodoos rising out of it in fused clusters, each a tapering fluted
 *    column with irregular ledges and a narrowing, jagged crown;
 *  - the main peak, a jagged pinnacle group whose tallest spire is 32 m
 *    (OSM's peak ele, read as metres above the park);
 *  - horizontal strata in three tones, dark rust at the foot to pale
 *    sandstone above.
 *
 * The coaster is drawn as a simple swept track, not with coaster-kit.ts:
 * most of it runs in rock cuts and tunnels at a few metres, and the kit's
 * energy model has nothing to check on a mine train with three chain lifts
 * and short drops. The centreline is OSM's circuit (888 m, against the
 * published 2,780 ft / 847 m), chained in ride order; the height at the end
 * of each way is set by hand from the ride's published layout (three lifts,
 * the helix off the second, the drop off the third into the tunnel under
 * the peak) and the OSM layer tags, interpolated and smoothed. Tunnels are
 * not drawn: the rock closes over them. Open track is a wooden deck with
 * dark rails, on timber bents where it stands clear of the rock. The two
 * sheds OSM tags covered get a plank roof.
 *
 * The station (way/324922573) is a plain timber building with a gabled
 * roof, 9 m at the ridge as OSM tags it.
 *
 * Evidence:
 *  - OSM: the rock area, the peak node, every track way with its tunnel,
 *    covered and layer tags, the station, and the rock mapped as a building
 *    (way/310114442) which is the rock over the covered run into the second
 *    lift.
 *  - Published: track length, ride order (Wikipedia).
 *  - Photos: an aerial from the Contemporary side (larrywkoester, Flickr,
 *    CC BY 2.0) for the layout and how the rock fuses; the main peak from
 *    the queue side (AmaryllisGardener, CC BY-SA 4.0); Haydn Blackey
 *    (Flickr, CC BY-SA 2.0) from the east.
 *
 * Estimated: every track height (no published profile; the lifts are put
 * at 12–14 m), every hoodoo's position and height but the peak's, and the
 * base's relief, which is a terraced noise field shaped by the track.
 */
import { Part, addGltfTriangles, len, writeGlb, type V3 } from './mesh'
import { PALETTE, finish } from './palette'

type XY = [number, number]
type Way = { id: number; kind: 'open' | 'covered' | 'tunnel'; z: number; pts: XY[] }
const TAU = Math.PI * 2

const rust = new Part()   // the dark foot of the strata
const sand = new Part()   // the main red-orange
const pale = new Part()   // pale sandstone bands and crowns
const rail = new Part()   // rails
const wood = new Part()   // deck, bents, sheds, station walls
const roof = new Part()   // station roof

// ---------------------------------------------------------------------------
// Kit

const unit = (a: V3): V3 => { const l = len(a) || 1; return [a[0] / l, a[1] / l, a[2] / l] }
function tri(p: Part, a: V3, b: V3, c: V3, n?: V3[]) { p.tri(a, b, c, undefined, undefined, undefined, n) }
function quad(p: Part, a: V3, b: V3, c: V3, d: V3, n?: [V3, V3, V3, V3]) {
  tri(p, a, b, c, n && [n[0], n[1], n[2]])
  tri(p, a, c, d, n && [n[0], n[2], n[3]])
}

/**
 * A smooth-shaded frustum about a vertical axis: radius r0 at z0 to r1 at z1.
 * r1 = 0 makes a cone. `n` segments, rotated by `phase` turns.
 */
function frustum(p: Part, cx: number, cy: number, r0: number, z0: number, r1: number, z1: number,
  n: number, o: { top?: boolean; bottom?: boolean; phase?: number; flat?: boolean } = {}) {
  const ph = (o.phase ?? 0.5 / n) * TAU
  const k = (r0 - r1) / (z1 - z0)
  const at = (i: number, r: number, z: number): V3 => {
    const t = ph + (i / n) * TAU
    return [cx + r * Math.cos(t), cy + r * Math.sin(t), z]
  }
  const nrm = (t: number): V3 => unit([Math.cos(t), Math.sin(t), k])
  for (let i = 0; i < n; i++) {
    const j = i + 1
    const ti = ph + (i / n) * TAU, tj = ph + (j / n) * TAU
    const ni = o.flat ? nrm((ti + tj) / 2) : nrm(ti), nj = o.flat ? ni : nrm(tj)
    if (r1 <= 0) {
      tri(p, at(i, r0, z0), at(j, r0, z0), [cx, cy, z1], [ni, nj, nrm((ti + tj) / 2)])
    } else {
      quad(p, at(i, r0, z0), at(j, r0, z0), at(j, r1, z1), at(i, r1, z1), [ni, nj, nj, ni])
    }
  }
  if (o.top && r1 > 0) for (let i = 1; i < n - 1; i++) tri(p, at(0, r1, z1), at(i, r1, z1), at(i + 1, r1, z1))
  if (o.bottom) for (let i = 1; i < n - 1; i++) tri(p, at(0, r0, z0), at(i + 1, r0, z0), at(i, r0, z0))
}

const area = (q: XY[]) => q.reduce((s, a, i) => { const b = q[(i + 1) % q.length]; return s + a[0] * b[1] - b[0] * a[1] }, 0) / 2

/** Keep the part of a polygon where a·x + b·y + c ≥ 0 (Sutherland–Hodgman). */
function clip(poly: XY[], a: number, b: number, c: number): XY[] {
  const f = (p: XY) => a * p[0] + b * p[1] + c
  const out: XY[] = []
  for (let i = 0; i < poly.length; i++) {
    const p = poly[i], q = poly[(i + 1) % poly.length], fp = f(p), fq = f(q)
    if (fp >= 0) out.push(p)
    if ((fp >= 0) !== (fq >= 0)) {
      const t = fp / (fp - fq)
      out.push([p[0] + (q[0] - p[0]) * t, p[1] + (q[1] - p[1]) * t])
    }
  }
  return out
}

/** Ear-clipping triangulation of a simple counter-clockwise polygon. */
function earcut(poly: XY[]): [number, number, number][] {
  const idx = poly.map((_, i) => i), out: [number, number, number][] = []
  const crossz = (a: XY, b: XY, c: XY) => (b[0] - a[0]) * (c[1] - a[1]) - (b[1] - a[1]) * (c[0] - a[0])
  const inside = (p: XY, a: XY, b: XY, c: XY) => crossz(a, b, p) > 1e-9 && crossz(b, c, p) > 1e-9 && crossz(c, a, p) > 1e-9
  let guard = 0
  while (idx.length > 3 && guard++ < 10000) {
    let cut = false
    for (let k = 0; k < idx.length; k++) {
      const i0 = idx[(k + idx.length - 1) % idx.length], i1 = idx[k], i2 = idx[(k + 1) % idx.length]
      const a = poly[i0], b = poly[i1], c = poly[i2]
      if (crossz(a, b, c) <= 1e-9) continue
      if (idx.some((j) => j !== i0 && j !== i1 && j !== i2 && inside(poly[j], a, b, c))) continue
      out.push([i0, i1, i2]); idx.splice(k, 1); cut = true; break
    }
    if (!cut) idx.splice(0, 1) // degenerate remainder
  }
  if (idx.length === 3) out.push([idx[0], idx[1], idx[2]])
  return out
}

/** A vertical prism over a counter-clockwise polygon, flat-shaded. */
function prism(p: Part, poly: XY[], z0: number, z1: number, o: { top?: boolean; bottom?: boolean; skip?: (a: XY, b: XY) => boolean } = {}) {
  if (area(poly) < 0) poly = [...poly].reverse()
  for (let i = 0; i < poly.length; i++) {
    const a = poly[i], b = poly[(i + 1) % poly.length]
    if (Math.hypot(b[0] - a[0], b[1] - a[1]) < 1e-6 || o.skip?.(a, b)) continue
    quad(p, [a[0], a[1], z0], [b[0], b[1], z0], [b[0], b[1], z1], [a[0], a[1], z1])
  }
  const tris = earcut(poly)
  if (o.top !== false) for (const [i, j, k] of tris) tri(p, [...poly[i], z1] as V3, [...poly[j], z1] as V3, [...poly[k], z1] as V3)
  if (o.bottom) for (const [i, j, k] of tris) tri(p, [...poly[i], z0] as V3, [...poly[k], z0] as V3, [...poly[j], z0] as V3)
}

/** A box with its vertical edges chamfered by c. */
function cbox(p: Part, x0: number, x1: number, y0: number, y1: number, z0: number, z1: number, c = 0.4, top = true) {
  prism(p, [[x0 + c, y0], [x1 - c, y0], [x1, y0 + c], [x1, y1 - c], [x1 - c, y1], [x0 + c, y1], [x0, y1 - c], [x0, y0 + c]], z0, z1, { top })
}

/**
 * A steep gable roof over a rectangle, ridge along y (`along` 'y') or x.
 * The slopes go to `roof`, the triangular gable ends to `wall`.
 */
function gable(roof: Part, wall: Part | null, x0: number, x1: number, y0: number, y1: number, ze: number, zr: number, along: 'x' | 'y', ends: [boolean, boolean] = [true, true]) {
  if (along === 'y') {
    const xm = (x0 + x1) / 2
    quad(roof, [x1, y0, ze], [x1, y1, ze], [xm, y1, zr], [xm, y0, zr])
    quad(roof, [x0, y1, ze], [x0, y0, ze], [xm, y0, zr], [xm, y1, zr])
    if (wall && ends[0]) tri(wall, [x0, y0, ze], [x1, y0, ze], [xm, y0, zr])
    if (wall && ends[1]) tri(wall, [x1, y1, ze], [x0, y1, ze], [xm, y1, zr])
  } else {
    const ym = (y0 + y1) / 2
    quad(roof, [x0, y0, ze], [x1, y0, ze], [x1, ym, zr], [x0, ym, zr])
    quad(roof, [x1, y1, ze], [x0, y1, ze], [x0, ym, zr], [x1, ym, zr])
    if (wall && ends[0]) tri(wall, [x0, y1, ze], [x0, y0, ze], [x0, ym, zr])
    if (wall && ends[1]) tri(wall, [x1, y0, ze], [x1, y1, ze], [x1, ym, zr])
  }
}

/** A hipped roof over a rectangle, ridge along its longer side. */
function hip(p: Part, x0: number, x1: number, y0: number, y1: number, ze: number, zr: number) {
  const w = x1 - x0, d = y1 - y0
  if (w >= d) {
    const ym = (y0 + y1) / 2, a = x0 + d / 2, b = x1 - d / 2
    quad(p, [x0, y0, ze], [x1, y0, ze], [b, ym, zr], [a, ym, zr])
    quad(p, [x1, y1, ze], [x0, y1, ze], [a, ym, zr], [b, ym, zr])
    tri(p, [x1, y0, ze], [x1, y1, ze], [b, ym, zr])
    tri(p, [x0, y1, ze], [x0, y0, ze], [a, ym, zr])
  } else {
    const xm = (x0 + x1) / 2, a = y0 + w / 2, b = y1 - w / 2
    quad(p, [x1, y0, ze], [x1, y1, ze], [xm, b, zr], [xm, a, zr])
    quad(p, [x0, y1, ze], [x0, y0, ze], [xm, a, zr], [xm, b, zr])
    tri(p, [x0, y0, ze], [x1, y0, ze], [xm, a, zr])
    tri(p, [x1, y1, ze], [x0, y1, ze], [xm, b, zr])
  }
}

/** Merlons along a polygon's edges, set in from the face by nothing: flush with the wall. */
function merlons(p: Part, poly: XY[], z: number, h: number, w: number, d: number, pitch: number, keep?: (a: XY, b: XY) => boolean) {
  if (area(poly) < 0) poly = [...poly].reverse()
  for (let i = 0; i < poly.length; i++) {
    const a = poly[i], b = poly[(i + 1) % poly.length]
    if (keep && !keep(a, b)) continue
    const L = Math.hypot(b[0] - a[0], b[1] - a[1])
    const n = Math.floor((L - w) / pitch)
    if (n < 0 || L < w + 0.4) continue
    const ux = (b[0] - a[0]) / L, uy = (b[1] - a[1]) / L, nx = -uy, ny = ux // inward normal (CCW)
    const start = (L - n * pitch - w) / 2
    for (let k = 0; k <= n; k++) {
      const s = start + k * pitch
      const c0: XY = [a[0] + ux * s, a[1] + uy * s], c1: XY = [a[0] + ux * (s + w), a[1] + uy * (s + w)]
      prism(p, [c0, c1, [c1[0] + nx * d, c1[1] + ny * d], [c0[0] + nx * d, c0[1] + ny * d]], z, z + h)
    }
  }
}

/** Merlons round a circle. */
function ringMerlons(p: Part, cx: number, cy: number, r: number, z: number, h: number, count: number, d = 0.35) {
  for (let k = 0; k < count; k++) {
    const t0 = (k / count) * TAU, t1 = t0 + (TAU / count) * 0.5
    const P = (t: number, rr: number): XY => [cx + rr * Math.cos(t), cy + rr * Math.sin(t)]
    prism(p, [P(t0, r - d), P(t0, r), P(t1, r), P(t1, r - d)], z, z + h)
  }
}

/**
 * A flat window panel on a wall plane, 0.05 m proud: rectangular, or with a
 * pointed (gothic) head. `face` is the wall's outward normal axis.
 */
function windowPanel(p: Part, face: 'n' | 's' | 'e' | 'w', plane: number, u: number, w: number, z0: number, z1: number, pointed = false) {
  const pts: [number, number][] = [[-w / 2, z0], [w / 2, z0]]
  if (pointed) {
    const spring = z1 - w * 0.75
    pts.push([w / 2, spring], [w * 0.36, spring + w * 0.42], [0, z1], [-w * 0.36, spring + w * 0.42], [-w / 2, spring])
  } else pts.push([w / 2, z1], [-w / 2, z1])
  const off = 0.05
  const to = ([s, z]: [number, number]): V3 =>
    face === 's' ? [u + s, plane - off, z] : face === 'n' ? [u - s, plane + off, z]
      : face === 'e' ? [plane + off, u + s, z] : [plane - off, u - s, z]
  for (let i = 1; i < pts.length - 1; i++) tri(p, to(pts[0]), to(pts[i]), to(pts[i + 1]))
}

/** A small deterministic random source, so the rock comes out the same. */
let seed = 11
const rnd = () => { seed = (seed * 16807) % 2147483647; return (seed - 1) / 2147483646 }

const inPoly = (p: XY, poly: XY[]) => {
  let c = false
  for (let i = 0, j = poly.length - 1; i < poly.length; j = i++) {
    const [xi, yi] = poly[i], [xj, yj] = poly[j]
    if ((yi > p[1]) !== (yj > p[1]) && p[0] < xi + ((p[1] - yi) * (xj - xi)) / (yj - yi)) c = !c
  }
  return c
}

/** Strata: the rock's tone by height — rust at the foot, then bands. */
function stratum(z: number): Part {
  if (z < 4.2) return rust
  const band = Math.floor((z - 4.2) / 2.8)
  if (z < 14) return band % 3 === 2 ? pale : sand
  return band % 3 === 1 ? sand : pale
}
/**
 * Rock triangles are collected per stratum and written at the end with
 * creased smooth shading (mesh.ts's addGltfTriangles): terrace tops and the
 * flutes of a hoodoo share their corners, steep breaks keep a hard edge.
 * Flat-shaded, the rock would cost three corners a triangle and blow the
 * file budget.
 */
const rockTris = new Map<Part, number[]>()
function rockTri(a: V3, b: V3, c: V3, p = stratum((a[2] + b[2] + c[2]) / 3)) {
  if (!rockTris.has(p)) rockTris.set(p, [])
  // map frame → glTF frame: (x, y, z) → (x, z, -y)
  for (const v of [a, b, c]) rockTris.get(p)!.push(v[0], v[2], -v[1])
}
/** A quad keeps one stratum, so a band's edge runs level, not zigzag. */
function rockQuad(a: V3, b: V3, c: V3, d: V3) {
  const p = stratum((a[2] + b[2] + c[2] + d[2]) / 4)
  rockTri(a, b, c, p); rockTri(a, c, d, p)
}
function flushRock(creaseDegrees: number) {
  for (const [part, pos] of rockTris) {
    addGltfTriangles(part, new Float32Array(pos), Uint32Array.from({ length: pos.length / 3 }, (_, i) => i), { creaseDegrees })
  }
  rockTris.clear()
}

// ---------------------------------------------------------------------------
// OSM data, metres about the peak node.

/** The ride's circuit in ride order from the station; `z` is the height at each way's end. */
const CIRCUIT: Way[] = [
  { id: 71194272, kind: 'tunnel', z: 8, pts: [[11.8,-61.3],[15.8,-57.7],[16.9,-53.5],[13.8,-43.1],[9.1,-18.3]] },
  { id: 820348480, kind: 'tunnel', z: 10, pts: [[9.1,-18.3],[6.1,19.2]] },
  { id: 71194300, kind: 'open', z: 4, pts: [[6.1,19.2],[5.6,21.8],[4.6,23.9],[3.0,25.2],[0.3,26.0],[-2.3,25.8],[-4.6,24.9],[-6.7,23.4],[-8.1,20.1],[-8.3,16.4],[-6.6,7.5]] },
  { id: 71194287, kind: 'tunnel', z: 3, pts: [[-6.6,7.5],[-4.6,-4.0],[-4.6,-6.5],[-5.5,-8.9]] },
  { id: 1351939293, kind: 'open', z: 3, pts: [[-5.5,-8.9],[-6.8,-10.7],[-9.0,-12.4],[-11.3,-13.3],[-13.6,-13.3],[-16.4,-11.6],[-19.0,-9.0]] },
  { id: 1351939292, kind: 'covered', z: 2.5, pts: [[-19.0,-9.0],[-25.6,9.3]] },
  { id: 71194288, kind: 'open', z: 2.5, pts: [[-25.6,9.3],[-32.9,29.5],[-33.3,32.6],[-32.9,35.3],[-31.0,38.2],[-27.9,39.6],[-23.6,39.8],[-20.3,38.7],[-18.4,36.4],[-17.2,33.0],[-17.7,29.3]] },
  { id: 71194279, kind: 'tunnel', z: 2, pts: [[-17.7,29.3],[-19.9,26.2],[-23.1,24.1],[-39.5,18.5],[-43.0,16.3]] },
  { id: 1351939291, kind: 'open', z: 1.5, pts: [[-43.0,16.3],[-44.8,13.7],[-45.8,10.6],[-46.2,1.8],[-45.2,-7.8],[-45.7,-16.7],[-44.6,-26.1]] },
  { id: 1351939290, kind: 'covered', z: 1.5, pts: [[-44.6,-26.1],[-44.1,-40.7],[-43.2,-43.3],[-40.9,-44.7],[-38.4,-45.1]] },
  { id: 683642206, kind: 'open', z: 5, pts: [[-38.4,-45.1],[-36.2,-44.3],[-33.7,-42.3],[-32.4,-39.5],[-31.0,-32.3],[-30.6,-30.2]] },
  { id: 1351939289, kind: 'covered', z: 7, pts: [[-30.6,-30.2],[-29.0,-22.4]] },
  { id: 71194292, kind: 'open', z: 13, pts: [[-29.0,-22.4],[-22.5,4.8]] },
  { id: 71194289, kind: 'open', z: 4, pts: [[-22.5,4.8],[-21.1,16.2],[-21.1,18.7],[-22.1,20.7],[-24.2,22.4],[-26.3,22.9],[-28.6,22.8],[-30.9,21.6],[-32.6,19.4],[-33.3,16.6],[-32.9,13.7],[-21.8,-25.8],[-20.3,-28.6],[-17.2,-30.4],[-13.6,-30.3],[-11.4,-29.0],[-10.0,-26.5],[-9.4,-23.7],[-10.3,-20.5],[-12.2,-18.3],[-14.8,-17.1],[-17.8,-17.1]] },
  { id: 71194297, kind: 'tunnel', z: 3, pts: [[-17.8,-17.1],[-20.2,-18.3],[-21.6,-20.2],[-22.2,-23.7],[-22.0,-26.9]] },
  { id: 1351939288, kind: 'open', z: 3, pts: [[-22.0,-26.9],[-20.9,-29.7],[-18.5,-31.4],[-15.1,-32.6],[-12.0,-32.4]] },
  { id: 71194298, kind: 'tunnel', z: 7, pts: [[-12.0,-32.4],[-8.6,-30.7],[-7.1,-27.8],[-6.7,-23.6],[-7.3,-18.5],[-9.4,-11.3]] },
  { id: 1351939287, kind: 'open', z: 11, pts: [[-9.4,-11.3],[-12.6,3.1],[-13.1,8.5],[-13.2,14.3]] },
  { id: 71194274, kind: 'tunnel', z: 14, pts: [[-13.2,14.3],[-13.6,30.0]] },
  { id: 71194276, kind: 'open', z: 12, pts: [[-13.6,30.0],[-13.9,39.1],[-13.6,41.3],[-13.1,43.2],[-12.1,44.9],[-9.7,46.4],[-6.7,46.7],[-4.6,46.2],[-3.3,45.3],[-2.9,44.2]] },
  { id: 71194281, kind: 'tunnel', z: 2, pts: [[-2.9,44.2],[0.0,-17.1]] },
  { id: 71194273, kind: 'open', z: 2, pts: [[0.0,-17.1],[0.0,-31.3],[0.5,-35.7],[1.6,-38.1],[4.4,-40.2],[8.0,-40.7],[12.0,-39.4],[14.9,-37.0],[19.0,-31.3],[21.7,-27.4]] },
  { id: 1351939284, kind: 'open', z: 3, pts: [[21.7,-27.4],[31.4,-14.0],[32.3,-11.1],[32.1,-7.8],[30.6,-5.4],[28.6,-3.6],[26.5,-3.0],[23.1,-3.6]] },
  { id: 1351939283, kind: 'tunnel', z: 2, pts: [[23.1,-3.6],[20.1,-5.5],[18.7,-8.0],[18.1,-12.4],[18.7,-16.2]] },
  { id: 71194299, kind: 'open', z: 1.5, pts: [[18.7,-16.2],[21.2,-41.0],[21.0,-43.3],[20.0,-45.5],[16.8,-47.6],[13.0,-48.0],[-8.9,-41.6],[-11.8,-41.5],[-14.3,-42.4],[-16.1,-44.7],[-22.3,-64.6],[-22.8,-68.1],[-22.3,-71.0],[-20.2,-73.4],[-17.4,-74.3],[-13.5,-73.7],[-11.4,-72.3],[-10.1,-71.0],[-8.8,-68.5]] },
  { id: 71194271, kind: 'covered', z: 1.5, pts: [[-8.8,-68.5],[-7.6,-67.2]] },
  { id: 1228692341, kind: 'covered', z: 1.5, pts: [[-7.6,-67.2],[4.5,-62.5]] },
  { id: 1228692337, kind: 'covered', z: 1.5, pts: [[4.5,-62.5],[8.0,-62.2],[11.8,-61.3]] },
]
// The second station track (way/71194269, 71194270, 1228692340, 1228692339), level under the roof.
const SECOND: XY[][] = [[[-13.5,-73.7],[-10.1,-73.0]],[[-10.1,-73.0],[-6.9,-71.9],[-1.9,-72.1]],[[-1.9,-72.1],[9.9,-65.1]],[[9.9,-65.1],[11.8,-61.3]]]

/** OSM's rock area (way/698843396). */
const ROCK: XY[] = [[29.6, -57.6], [31.8, -57.5], [33.2, -54.7], [35.2, -50.2], [35.5, -45.4], [31.9, -42.2], [26.0, -47.9], [25.2, -47.0], [23.6, -45.5], [25.2, -39.0], [16.4, -38.5], [9.6, -30.7], [10.3, -16.5], [21.6, -11.7], [23.2, -8.2], [23.6, -5.6], [21.8, -2.6], [17.5, -6.6], [11.7, -8.0], [14.4, 3.5], [11.5, 13.6], [10.7, 20.4], [9.8, 26.5], [6.9, 25.2], [4.3, 28.3], [5.1, 30.1], [0.8, 33.1], [5.9, 37.2], [3.2, 39.5], [1.0, 37.8], [-3.2, 37.0], [-4.7, 37.6], [-6.0, 31.8], [-9.0, 30.3], [-12.3, 30.1], [-14.6, 30.2], [-16.7, 30.5], [-19.2, 31.0], [-21.9, 34.3], [-27.2, 36.0], [-30.7, 35.1], [-29.0, 30.3], [-30.2, 26.6], [-30.6, 25.2], [-32.2, 24.5], [-34.3, 23.7], [-42.2, 21.9], [-46.5, 21.4], [-48.2, 19.2], [-46.1, 17.7], [-42.9, 18.1], [-41.1, 14.1], [-37.2, 15.4], [-34.8, 15.2], [-31.4, 14.9], [-28.8, 11.1], [-28.4, 6.1], [-30.5, -2.0], [-32.4, -13.3], [-26.4, -20.7], [-22.9, -27.9], [-18.5, -32.9], [-11.6, -33.4], [-8.1, -31.0], [-2.5, -33.4], [-0.5, -35.5], [3.9, -41.6], [10.4, -44.0], [11.4, -46.2], [12.7, -49.1], [13.5, -51.0], [11.0, -57.6], [18.0, -54.5], [20.6, -54.4], [26.2, -51.6], [28.6, -55.9], [26.0, -57.3]]
/** The rock OSM maps as a building (way/310114442). */
const ROCK_BUILDING: XY[] = [[-41.5, -24.9], [-37.5, -24.9], [-36.2, -28.3], [-36.2, -32.5], [-35.9, -38.5], [-38.0, -41.9], [-40.7, -45.8], [-44.6, -43.7], [-48.8, -32.0], [-47.2, -25.2], [-44.6, -26.1]]
/** The station (way/324922573). */
const STATION: XY[] = [[-4.2, -57.9], [-0.4, -59.5], [8.9, -55.6], [10.4, -57.9], [11.0, -57.6], [18.0, -54.5], [20.6, -54.4], [26.2, -51.6], [28.6, -55.9], [26.0, -57.3], [29.6, -57.6], [29.3, -59.7], [29.0, -61.8], [25.5, -61.6], [12.7, -68.1], [7.3, -70.8], [8.3, -74.8], [2.0, -78.0], [0.2, -78.6], [-1.3, -79.5], [-2.7, -80.1], [-4.2, -76.9], [-7.2, -79.8], [-11.2, -80.9], [-13.2, -75.3], [-10.6, -74.0], [-8.4, -67.0], [-9.0, -65.7], [-10.0, -63.9], [-9.3, -60.5]]

// ---------------------------------------------------------------------------
// The track's centreline: OSM's circuit resampled every STEP metres, with
// heights interpolated along each way and smoothed.

const STEP = 3.0
const STATION_Z = 1.2
type Sample = { x: number; y: number; z: number; kind: Way['kind'] }
const samples: Sample[] = []
{
  // Densify each way, carrying the kind of the segment and the height.
  const pts: Sample[] = []
  let zPrev = CIRCUIT[CIRCUIT.length - 1].z
  for (const w of CIRCUIT) {
    const L = w.pts.slice(1).reduce((s, p, i) => s + Math.hypot(p[0] - w.pts[i][0], p[1] - w.pts[i][1]), 0)
    let acc = 0
    for (let i = 0; i < w.pts.length - 1; i++) {
      const a = w.pts[i], b = w.pts[i + 1], l = Math.hypot(b[0] - a[0], b[1] - a[1])
      const n = Math.max(1, Math.round(l / 0.5))
      for (let k = 0; k < n; k++) {
        const t = k / n, d = acc + l * t
        pts.push({ x: a[0] + (b[0] - a[0]) * t, y: a[1] + (b[1] - a[1]) * t, z: zPrev + (w.z - zPrev) * (d / L), kind: w.kind })
      }
      acc += l
    }
    zPrev = w.z
  }
  // Resample at STEP along the closed loop.
  let carry = 0
  for (let i = 0; i < pts.length; i++) {
    const a = pts[i], b = pts[(i + 1) % pts.length], l = Math.hypot(b.x - a.x, b.y - a.y)
    if (carry <= 0) { samples.push({ ...a }); carry += STEP }
    carry -= l
  }
  // Smooth the heights (a circular moving average, twice).
  for (let pass = 0; pass < 2; pass++) {
    const z = samples.map((s) => s.z), W = 3
    samples.forEach((s, i) => {
      let sum = 0
      for (let k = -W; k <= W; k++) sum += z[(i + k + z.length) % z.length]
      s.z = sum / (2 * W + 1)
    })
  }
}

/** Nearest sample of a kind: distance and the track's height there. */
function nearest(x: number, y: number, tunnel: boolean): [number, number] {
  let best = Infinity, z = 0
  for (const s of samples) {
    if ((s.kind === 'tunnel') !== tunnel) continue
    const d = Math.hypot(s.x - x, s.y - y)
    if (d < best) { best = d; z = s.z }
  }
  return [best, z]
}

// ---------------------------------------------------------------------------
// The rock base: a terraced height field over the rock area, raised over
// the tunnels and cut down to canyon floors along the open track.

const hash = (i: number, j: number) => {
  const s = Math.sin(i * 127.1 + j * 311.7) * 43758.5453
  return s - Math.floor(s)
}
function noise(x: number, y: number, scale: number) {
  const u = x / scale, v = y / scale, i = Math.floor(u), j = Math.floor(v)
  const fu = u - i, fv = v - j, su = fu * fu * (3 - 2 * fu), sv = fv * fv * (3 - 2 * fv)
  const a = hash(i, j), b = hash(i + 1, j), c = hash(i, j + 1), d = hash(i + 1, j + 1)
  return (a * (1 - su) + b * su) * (1 - sv) + (c * (1 - su) + d * su) * sv
}

function baseHeight(x: number, y: number): number {
  const p: XY = [x, y]
  if (inPoly(p, STATION)) return 0
  const [dT, zT] = nearest(x, y, true)
  const inside = inPoly(p, ROCK) || inPoly(p, ROCK_BUILDING) || dT < 7
  if (!inside) return 0
  let h = 2.2 + 3.8 * noise(x + 40, y + 70, 15) + 1.4 * noise(x + 3, y - 9, 6)
  h += 4 * Math.max(0, 1 - Math.hypot(x - 0.5, y - 1) / 18) // the peak's shoulders
  h = Math.round(h / 1.3) * 1.3                             // terraces
  if (dT < 7) h = Math.max(h, zT + 3.0 - Math.max(0, dT - 4.0) * 1.8)
  const [dO, zO] = nearest(x, y, false)
  // Canyon floor: at most 2 m, or just under low track; trestles span the rest.
  const floor = Math.max(0, Math.min(zO - 0.6, 2.0))
  if (dO < 3.2) h = Math.min(h, floor)
  else if (dO < 6.5) h = Math.min(h, floor + (dO - 3.2) * 2.0)
  return Math.max(0, h)
}

const G = 3.5, GX0 = -52, GY0 = -60, NX = 27, NY = 30
const HF: number[][] = []
for (let j = 0; j <= NY; j++) {
  HF.push([])
  for (let i = 0; i <= NX; i++) HF[j].push(baseHeight(GX0 + i * G, GY0 + j * G))
}
// Each grid corner is nudged off the lattice (deterministically), so the
// terraces break along irregular lines instead of a visible square grid.
const jx = (i: number, j: number) => (hash(i * 3.1, j * 7.7) - 0.5) * 1.8, jy = (i: number, j: number) => (hash(i * 5.3, j * 2.9) - 0.5) * 1.8
for (let j = 0; j < NY; j++) for (let i = 0; i < NX; i++) {
  const P = (ii: number, jj: number): V3 => [GX0 + ii * G + jx(ii, jj), GY0 + jj * G + jy(ii, jj), HF[jj][ii]]
  const a = P(i, j), b = P(i + 1, j), c = P(i + 1, j + 1), d = P(i, j + 1)
  // Split each cell along the diagonal that follows the terraces better.
  const pairs: [V3, V3, V3][] = Math.abs(a[2] - c[2]) < Math.abs(b[2] - d[2]) ? [[a, b, c], [a, c, d]] : [[a, b, d], [b, c, d]]
  for (const [p, q, r] of pairs) if (p[2] + q[2] + r[2] > 0.05) rockTri(p, q, r)
}
const groundAt = (x: number, y: number) => {
  const u = (x - GX0) / G, v = (y - GY0) / G
  const i = Math.max(0, Math.min(NX - 1, Math.floor(u))), j = Math.max(0, Math.min(NY - 1, Math.floor(v)))
  const fu = Math.min(1, Math.max(0, u - i)), fv = Math.min(1, Math.max(0, v - j))
  return (HF[j][i] * (1 - fu) + HF[j][i + 1] * fu) * (1 - fv) + (HF[j + 1][i] * (1 - fu) + HF[j + 1][i + 1] * fu) * fv
}

// ---------------------------------------------------------------------------
// Hoodoos: tapering fluted columns with ledges and a jagged crown.

function hoodoo(cx: number, cy: number, r: number, h: number, n = h < 14 ? 6 : 7) {
  const rot = rnd() * TAU
  const flute = Array.from({ length: n }, (_, i) => (i % 2 ? 0.8 : 1.0) * (0.92 + rnd() * 0.16))
  const ring = (rad: number, z: number, jag = 0): V3[] => flute.map((f, i) => {
    const t = rot + (i / n) * TAU
    return [cx + rad * f * Math.cos(t), cy + rad * f * Math.sin(t), z + (jag ? (rnd() - 0.5) * jag : 0)]
  })
  const radius = (t: number) => r * (1 - 0.42 * Math.pow(t, 1.6))
  const rings: V3[][] = []
  const levels = h > 12 ? [0, 0.3, 0.58, 0.82] : [0, 0.4, 0.75]
  const lip = 1 + Math.floor(rnd() * (levels.length - 1))
  levels.forEach((t, k) => {
    const z = t * h, rad = radius(t)
    rings.push(ring(rad, z))
    if (k === lip && h > 12) {
      // An irregular ledge: an overhang out, a short face, a step back in.
      const out = 1.1 + rnd() * 0.08
      rings.push(ring(rad * out, z + 0.3), ring(rad * out, z + 0.9), ring(rad * 0.94, z + 1.0))
    }
  })
  rings.push(ring(radius(1) * 0.85, h - 0.6, 1.4))   // the jagged, narrowing crown
  for (let k = 0; k < rings.length - 1; k++) {
    const lo = rings[k], hi = rings[k + 1]
    for (let a = 0; a < n; a++) { const b = (a + 1) % n; rockQuad(lo[a], lo[b], hi[b], hi[a]) }
  }
  const top = rings[rings.length - 1], apex: V3 = [cx, cy, h + 0.3]
  for (let a = 0; a < n; a++) rockTri(top[a], top[(a + 1) % n], apex)
}

// The main peak: a jagged pinnacle group, fused on the base's shoulder.
// Its stepped rubble pyramid first, half the spire's height, then the group.
hoodoo(0.8, 1.0, 13.5, 15.0, 11)
for (const [x, y, r, h] of [
  [-0.8, 1.0, 3.6, 32.0], [1.8, 3.0, 2.8, 25.0], [-3.0, 2.8, 2.5, 22.5], [3.8, -0.8, 3.0, 20.5],
  [6.2, 2.0, 2.8, 17.0], [-3.8, -1.8, 2.8, 17.5], [0.6, -3.4, 3.0, 18.5],
  [1.4, -0.4, 2.2, 28.0], [-2.4, 0.2, 2.0, 26.0], [0.2, 3.6, 2.2, 21.0],
] as [number, number, number, number][]) hoodoo(x, y, r * 1.15, h, h > 30 ? 9 : 8)

// The other clusters, over the tunnels and between the open track.
for (const [x, y, r, h] of [
  // North-west, over the third lift's tunnel and the run out to the west.
  [-12.5, 22, 4.2, 17], [-9.5, 29.5, 3.4, 13], [-34, 23, 3.4, 11],
  // North, over the drop into the long tunnel under the peak.
  [-3.2, 37.5, 3.6, 15], [0.8, 30, 3.0, 12],
  // East, over the tunnel out of the first drop's turn.
  [17.5, -8.5, 3.6, 13], [14.5, 1.5, 3.0, 10.5],
  // South, over the third lift's tunnel and the helix's tunnel.
  [-11, -23, 3.6, 12.5], [-21, -22, 3.0, 10],
  // South-east, over the first lift's tunnel.
  [15.5, -46, 3.2, 11],
  // The rock building over the run into the second lift.
  [-42, -31, 3.6, 10.5], [-40.5, -39.5, 3.0, 8.5],
] as [number, number, number, number][]) hoodoo(x, y, r * 1.35, h * 1.05)

flushRock(38)

// ---------------------------------------------------------------------------
// The track: a wooden deck with two dark rails, swept along the open and
// covered stretches; timber bents where it stands clear of the rock.

const W = 1.9, DEPTH = 0.45, GAUGE = 0.55, RW = 0.18
// A frame per sample — the right-hand side (horizontal) and the deck's up —
// so neighbouring segments share their corners and the deck shades smooth.
const frames = samples.map((s, i) => {
  const a = samples[(i - 1 + samples.length) % samples.length], b = samples[(i + 1) % samples.length]
  const t: V3 = [b.x - a.x, b.y - a.y, b.z - a.z]
  const l = Math.hypot(t[0], t[1]) || 1
  const side: V3 = [t[1] / l, -t[0] / l, 0]
  const up = cross3(side, t)
  return { side, up: unitV(up) }
})
function cross3(a: V3, b: V3): V3 { return [a[1] * b[2] - a[2] * b[1], a[2] * b[0] - a[0] * b[2], a[0] * b[1] - a[1] * b[0]] }
function unitV(a: V3): V3 { const l = Math.hypot(...a) || 1; return [a[0] / l, a[1] / l, a[2] / l] }
const at = (i: number, off: number, dz: number): V3 => {
  const s = samples[i], f = frames[i]
  return [s.x + f.side[0] * off, s.y + f.side[1] * off, s.z + dz]
}
let bentEvery = 0
for (let i = 0; i < samples.length; i++) {
  const j = (i + 1) % samples.length
  const s0 = samples[i], s1 = samples[j]
  if (s0.kind === 'tunnel' && s1.kind === 'tunnel') continue
  const f0 = frames[i], f1 = frames[j]
  const out0 = f0.side, out1 = f1.side
  const in0: V3 = [-out0[0], -out0[1], 0], in1: V3 = [-out1[0], -out1[1], 0]
  // Deck: the top, and one fascia, on whichever side faces south or west,
  // the side the map's usual view sees; the deck is thin at map scale.
  quad(wood, at(i, -W / 2, 0), at(i, W / 2, 0), at(j, W / 2, 0), at(j, -W / 2, 0), [f0.up, f0.up, f1.up, f1.up])
  if (f0.side[0] + f0.side[1] < 0) quad(wood, at(j, -W / 2, -DEPTH), at(i, -W / 2, -DEPTH), at(i, -W / 2, 0), at(j, -W / 2, 0), [in1, in0, in0, in1])
  else quad(wood, at(i, W / 2, -DEPTH), at(j, W / 2, -DEPTH), at(j, W / 2, 0), at(i, W / 2, 0), [out0, out1, out1, out0])
  // Rails: dark strips standing just proud of the deck.
  for (const g of [-GAUGE, GAUGE])
    quad(rail, at(i, g - RW / 2, 0.1), at(i, g + RW / 2, 0.1), at(j, g + RW / 2, 0.1), at(j, g - RW / 2, 0.1), [f0.up, f0.up, f1.up, f1.up])
  // A timber bent every other sample (6 m) where the deck is clear of the
  // rock: two posts and a cap beam, a ledger halfway up the tall ones, each
  // a flat double-sided panel facing along the track.
  const g0 = groundAt(s0.x, s0.y)
  if (s0.z - DEPTH - g0 > 1.2 && bentEvery++ % 2 === 0) { // every 6 m
    const zt = s0.z - DEPTH, p = 0.34
    const panel = (o0: number, o1: number, z0: number, z1: number) => {
      const a = at(i, o0, 0), b = at(i, o1, 0)
      quad(wood, [a[0], a[1], z0], [b[0], b[1], z0], [b[0], b[1], z1], [a[0], a[1], z1])
      quad(wood, [b[0], b[1], z0], [a[0], a[1], z0], [a[0], a[1], z1], [b[0], b[1], z1])
    }
    panel(-W / 2 + 0.05, -W / 2 + 0.05 + p, g0 - 0.2, zt)
    panel(W / 2 - 0.05 - p, W / 2 - 0.05, g0 - 0.2, zt)
    panel(-W / 2 - 0.2, W / 2 + 0.2, zt - 0.4, zt)
    if (zt - g0 > 5) panel(-W / 2, W / 2, g0 + (zt - g0) / 2 - 0.2, g0 + (zt - g0) / 2 + 0.2)
  }
}
// The second station track, level under the station roof.
for (const line of SECOND) {
  for (let i = 0; i < line.length - 1; i++) {
    const a = line[i], b = line[i + 1], l = Math.hypot(b[0] - a[0], b[1] - a[1]), sx = (b[1] - a[1]) / l, sy = -(b[0] - a[0]) / l
    const P = (p: XY, off: number, z: number): V3 => [p[0] + sx * off, p[1] + sy * off, z]
    quad(wood, P(a, -W / 2, STATION_Z), P(a, W / 2, STATION_Z), P(b, W / 2, STATION_Z), P(b, -W / 2, STATION_Z))
    for (const g of [-GAUGE, GAUGE]) quad(rail, P(a, g - RW / 2, STATION_Z + 0.1), P(a, g + RW / 2, STATION_Z + 0.1), P(b, g + RW / 2, STATION_Z + 0.1), P(b, g - RW / 2, STATION_Z + 0.1))
  }
}

// Plank sheds over the two covered runs outside the station
// (way/1351939292, way/1351939289): a gabled roof on the track.
for (const w of CIRCUIT.filter((w) => w.id === 1351939292 || w.id === 1351939289)) {
  const a = w.pts[0], b = w.pts[w.pts.length - 1], l = Math.hypot(b[0] - a[0], b[1] - a[1])
  const ux = (b[0] - a[0]) / l, uy = (b[1] - a[1]) / l, sx = uy, sy = -ux, half = 2.0
  const zA = nearest(a[0], a[1], false)[1], zB = nearest(b[0], b[1], false)[1]
  const P = (p: XY, off: number, z: number): V3 => [p[0] + sx * off, p[1] + sy * off, z]
  const e = 3.0, rr = 4.3
  quad(wood, P(a, half, zA + e), P(b, half, zB + e), P(b, 0, zB + rr), P(a, 0, zA + rr))
  quad(wood, P(b, -half, zB + e), P(a, -half, zA + e), P(a, 0, zA + rr), P(b, 0, zB + rr))
  for (const off of [half, -half]) for (const p of [a, b]) {
    const z = p === a ? zA : zB
    const c = P(p, off, 0)
    prism(wood, [[c[0] - 0.15, c[1] - 0.15], [c[0] + 0.15, c[1] - 0.15], [c[0] + 0.15, c[1] + 0.15], [c[0] - 0.15, c[1] + 0.15]], Math.max(0, groundAt(c[0], c[1]) - 0.2), z + e, { top: false })
  }
}

// ---------------------------------------------------------------------------
// The station: timber walls under a gabled roof along its long axis.

{
  const cx = STATION.reduce((s, p) => s + p[0], 0) / STATION.length, cy = STATION.reduce((s, p) => s + p[1], 0) / STATION.length
  // Main axis by the polygon's second moments.
  let sxx = 0, syy = 0, sxy = 0
  for (const [x, y] of STATION) { sxx += (x - cx) ** 2; syy += (y - cy) ** 2; sxy += (x - cx) * (y - cy) }
  const ang = 0.5 * Math.atan2(2 * sxy, sxx - syy), ux = Math.cos(ang), uy = Math.sin(ang)
  const d = (p: XY) => (p[0] - cx) * -uy + (p[1] - cy) * ux // signed distance from the ridge line
  const span = Math.max(...STATION.map((p) => Math.abs(d(p))))
  const EAVE = 4.8, RIDGE = 9.0
  const zRoof = (p: XY) => EAVE + (RIDGE - EAVE) * (1 - Math.abs(d(p)) / span)
  // Walls up to the roof's underside, each corner at its own height.
  for (let i = 0; i < STATION.length; i++) {
    const a = STATION[i], b = STATION[(i + 1) % STATION.length]
    const ea = Math.min(zRoof(a), EAVE + 0.8), eb = Math.min(zRoof(b), EAVE + 0.8)
    quad(wood, [b[0], b[1], 0], [a[0], a[1], 0], [a[0], a[1], ea], [b[0], b[1], eb])
  }
  // The roof: the polygon split along the ridge, each half a plane.
  for (const half of [clip(STATION, -uy, ux, -(-uy * cx + ux * cy)), clip(STATION, uy, -ux, -(uy * cx - ux * cy))]) {
    if (half.length < 3) continue
    const ccw = area(half) > 0 ? half : [...half].reverse()
    for (const [i, j, k] of earcut(ccw)) {
      const A = ccw[i], B = ccw[j], C = ccw[k]
      tri(roof, [A[0], A[1], zRoof(A) + 0.15], [B[0], B[1], zRoof(B) + 0.15], [C[0], C[1], zRoof(C) + 0.15])
    }
  }
}

// ---------------------------------------------------------------------------

// Big Thunder's sandstone in three strata — rust at the foot, OSM's own
// colour tag (#D38568) through the middle, pale sandstone in bands and at
// the crowns — with weathered timber and dark rails.
const parts = [
  { part: rust, material: finish('thunder-rust', 0xb7694c) },
  { part: sand, material: finish('thunder-rock', 0xd38568) },
  { part: pale, material: finish('thunder-sandstone', 0xe8b993) },
  { part: rail, material: finish('thunder-rail', 0x4a4f57) },
  { part: wood, material: finish('thunder-timber', 0x9a7a60) },
  { part: roof, material: PALETTE.roof },
]
const triangles = parts.reduce((sum, { part }) => sum + part.triangles, 0)
if (process.env.REPORT) {
  const byKind = parts.map(({ part, material }) => `${material.name} ${part.triangles}`).join(', ')
  console.log(byKind, 'samples', samples.length, 'max track z', Math.max(...samples.map((s) => s.z)).toFixed(1))
}
if (triangles > 6500 && !process.env.REPORT) throw new Error(`Triangle budget exceeded: ${triangles}`)
const glb = writeGlb('Big Thunder Mountain Railroad', parts, {
  license: 'CC0-1.0', frame: 'Y up, -Z north, +X east, metres; origin at ground anchor',
  bearing: 0, elevation: 0, height: 32.3,
})
if (glb.length > 256000) throw new Error(`File budget exceeded: ${glb.length}`)
const out = new URL('../../landmarks/models/wdw-big-thunder-mountain.glb', import.meta.url).pathname
await Bun.write(out, glb)
console.log(`${out}: ${triangles} triangles, ${glb.length} bytes`)
