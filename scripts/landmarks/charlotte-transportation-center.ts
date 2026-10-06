/**
 * Charlotte Transportation Center (CTC), Uptown Charlotte — procedural, CC0-1.0.
 * bun scripts/landmarks/charlotte-transportation-center.ts
 *
 * Map frame: x across the station (the LYNX Blue Line side −x, Brevard Street
 * +x), y along it (4th Street −y, Trade Street +y), z up, metres. Placed at
 * bearing 47°, the axis of the OSM outline (way/48908467). The origin is the
 * outline's centroid; every coordinate below is measured from the OSM ways in
 * that frame.
 *
 * The CATS bus station of 1995 still stands as built: the 2023 plan to replace
 * it with an underground bus facility under a tower was put on hold in 2024–25,
 * and CATS is instead renovating the existing station (retail closed in 2025
 * for a fare-paid zone). So this is the existing building:
 *
 * - a long segmental barrel vault over the bus lanes (OSM way/1501142074,
 *   to 17 m, springing at about 7 m against the wings), teal on top, open at
 *   both ends;
 * - at each end, a white steel truss screen filling the arch over the bus
 *   opening, with CATS-blue columns, braces and arch chord (Commons photos
 *   "Charlotte Transportation Center 04" and "…viewed from Third St");
 * - two low wings along the long sides under their own teal vaults: the
 *   concourse and shops on the light-rail side (way/1501142073, 11 m) and
 *   the two-storey block on Brevard (way/1501142075, 12 m), with the flat
 *   stair and lift boxes in their notches folded into them;
 * - the white columns with blue bases inside the shed and the two small
 *   hipped kiosks on the bus islands.
 *
 * The LYNX CTC/Arena light-rail platforms have their own glass canopies on
 * the Blue Line beside and beyond the station; they are a separate structure
 * and not modelled here.
 */
import { Part, cross, len, writeGlb, type V3 } from './mesh'
import { PALETTE, finish } from './palette'

const teal = new Part(), stone = new Part(), white = new Part()
const blue = new Part(), glazing = new Part(), soffit = new Part()

type XY = [number, number]
const unit = (v: V3): V3 => { const l = len(v) || 1; return [v[0] / l, v[1] / l, v[2] / l] }
const add = (a: V3, b: V3): V3 => [a[0] + b[0], a[1] + b[1], a[2] + b[2]]
const dot = (a: V3, b: V3) => a[0] * b[0] + a[1] * b[1] + a[2] * b[2]

/** A triangle with per-corner normals, its winding fixed to agree with them. */
function tri(p: Part, a: V3, b: V3, c: V3, n: V3[]) {
  const face = cross([b[0] - a[0], b[1] - a[1], b[2] - a[2]], [c[0] - a[0], c[1] - a[1], c[2] - a[2]])
  if (len(face) < 1e-9) return
  const avg = add(add(n[0], n[1]), n[2])
  if (dot(face, avg) >= 0) p.tri(a, b, c, undefined, undefined, undefined, n)
  else p.tri(a, c, b, undefined, undefined, undefined, [n[0], n[2], n[1]])
}
function quad(p: Part, a: V3, b: V3, c: V3, d: V3, n: V3 | V3[]) {
  const N = Array.isArray(n[0]) ? (n as V3[]) : [n as V3, n as V3, n as V3, n as V3]
  tri(p, a, b, c, [N[0], N[1], N[2]])
  tri(p, a, c, d, [N[0], N[2], N[3]])
}

/** An axis-aligned box, all six faces (or five, without the bottom). */
function box(p: Part, x0: number, x1: number, y0: number, y1: number, z0: number, z1: number, bottom = false) {
  quad(p, [x0, y0, z0], [x1, y0, z0], [x1, y0, z1], [x0, y0, z1], [0, -1, 0])
  quad(p, [x1, y1, z0], [x0, y1, z0], [x0, y1, z1], [x1, y1, z1], [0, 1, 0])
  quad(p, [x0, y1, z0], [x0, y0, z0], [x0, y0, z1], [x0, y1, z1], [-1, 0, 0])
  quad(p, [x1, y0, z0], [x1, y1, z0], [x1, y1, z1], [x1, y0, z1], [1, 0, 0])
  quad(p, [x0, y0, z1], [x1, y0, z1], [x1, y1, z1], [x0, y1, z1], [0, 0, 1])
  if (bottom) quad(p, [x0, y0, z0], [x0, y1, z0], [x1, y1, z0], [x1, y0, z0], [0, 0, -1])
}

/** A round column, smooth-shaded, open at both ends. */
function column(p: Part, cx: number, cy: number, r: number, z0: number, z1: number, seg = 10) {
  for (let k = 0; k < seg; k++) {
    const a = (k / seg) * 2 * Math.PI, b = ((k + 1) / seg) * 2 * Math.PI
    const na: V3 = [Math.cos(a), Math.sin(a), 0], nb: V3 = [Math.cos(b), Math.sin(b), 0]
    quad(p, [cx + r * na[0], cy + r * na[1], z0], [cx + r * nb[0], cy + r * nb[1], z0],
      [cx + r * nb[0], cy + r * nb[1], z1], [cx + r * na[0], cy + r * na[1], z1], [na, nb, nb, na])
  }
}

/** A square-section strut between two points in an x–z plane at depth y. */
function strut(p: Part, a: XY, b: XY, y: number, w: number, d: number) {
  const L = Math.hypot(b[0] - a[0], b[1] - a[1])
  const t: XY = [(b[0] - a[0]) / L, (b[1] - a[1]) / L], s: XY = [-t[1] * w / 2, t[0] * w / 2]
  const P = (q: XY, k: number, dy: number): V3 => [q[0] + s[0] * k, y + dy, q[1] + s[1] * k]
  const side: V3 = unit([-t[1], 0, t[0]])
  for (const dy of [-d / 2, d / 2])
    quad(p, P(a, -1, dy), P(b, -1, dy), P(b, 1, dy), P(a, 1, dy), [0, Math.sign(dy), 0])
  quad(p, P(a, 1, -d / 2), P(b, 1, -d / 2), P(b, 1, d / 2), P(a, 1, d / 2), side)
  quad(p, P(a, -1, -d / 2), P(b, -1, -d / 2), P(b, -1, d / 2), P(a, -1, d / 2), [-side[0], 0, -side[2]])
}

// ---------------------------------------------------------------------------
// Segmental barrel vaults, axis along y.

type Arc = { h: number; R: number; zc0: number; x0: number; x1: number }
function arc(x0: number, x1: number, zs: number, zc: number): Arc {
  const c = (x1 - x0) / 2, s = zc - zs
  const R = (c * c + s * s) / (2 * s)
  return { h: (x0 + x1) / 2, R, zc0: zc - R, x0, x1 }
}
const zAt = (a: Arc, x: number, off = 0) => a.zc0 + Math.sqrt((a.R + off) ** 2 - (x - a.h) ** 2)
/** Points along the arc (offset `off` radially), seg+1 of them, x0 to x1. */
function arcPts(a: Arc, seg: number, off = 0): { x: number; z: number; n: XY }[] {
  const t0 = Math.asin((a.x0 - a.h) / a.R), t1 = Math.asin((a.x1 - a.h) / a.R)
  return Array.from({ length: seg + 1 }, (_, i) => {
    const t = t0 + ((t1 - t0) * i) / seg
    return { x: a.h + (a.R + off) * Math.sin(t), z: a.zc0 + (a.R + off) * Math.cos(t), n: [Math.sin(t), Math.cos(t)] as XY }
  })
}

/**
 * A vault shell from y0 to y1: smooth top, a rounded bevel into a fascia at
 * each end, the underside, and vertical fascias along both eaves that drop
 * to `eave`.
 */
function vault(a: Arc, y0: number, y1: number, thick: number, seg: number, eave: number, under: Part) {
  const b = 0.45
  const top = arcPts(a, seg), bev = arcPts(a, seg, -b), bot = arcPts(a, seg, -thick)
  for (let i = 0; i < seg; i++) {
    const p = top[i], q = top[i + 1], n1: V3 = [p.n[0], 0, p.n[1]], n2: V3 = [q.n[0], 0, q.n[1]]
    quad(teal, [p.x, y0 + b, p.z], [q.x, y0 + b, q.z], [q.x, y1 - b, q.z], [p.x, y1 - b, p.z], [n1, n2, n2, n1])
    for (const [ye, s] of [[y0, -1], [y1, 1]] as [number, number][]) {
      const bp = bev[i], bq = bev[i + 1]
      const m1 = unit([p.n[0], s, p.n[1]]), m2 = unit([q.n[0], s, q.n[1]])
      // Bevel: from the top surface, b in from the end, round to the fascia.
      quad(teal, [p.x, ye - s * b, p.z], [q.x, ye - s * b, q.z], [bq.x, ye, bq.z], [bp.x, ye, bp.z], [n1, n2, m2, m1])
      // Fascia down to the underside.
      const u = bot[i], v = bot[i + 1]
      quad(teal, [bp.x, ye, bp.z], [bq.x, ye, bq.z], [v.x, ye, v.z], [u.x, ye, u.z], [0, s, 0])
    }
    const u = bot[i], v = bot[i + 1]
    quad(under, [u.x, y0, u.z], [v.x, y0, v.z], [v.x, y1, v.z], [u.x, y1, u.z], [[-u.n[0], 0, -u.n[1]], [-v.n[0], 0, -v.n[1]], [-v.n[0], 0, -v.n[1]], [-u.n[0], 0, -u.n[1]]])
  }
  // Eave fascias, from the shell's edge down to the eave line.
  for (const [e, s] of [[top[0], -1], [top[seg], 1]] as [{ x: number; z: number }, number][]) {
    const x = e.x
    quad(teal, [x, y0, eave], [x, y1, eave], [x, y1, e.z], [x, y0, e.z], [s, 0, 0])
    const zb = s < 0 ? bot[0].z : bot[seg].z, xb = s < 0 ? bot[0].x : bot[seg].x
    if (zb > eave) quad(under, [xb, y0, eave], [xb, y1, eave], [xb, y1, zb], [xb, y0, zb], [-s, 0, 0])
    quad(under, [x, y0, eave], [x, y1, eave], [xb, y1, eave], [xb, y0, eave], [0, 0, -1])
  }
}

// ---------------------------------------------------------------------------
// Walls with window panels: a face from a to b (counter-clockwise footprint),
// stone, with slate panels laid 0.04 m proud in bays and bands.

function wall(a: XY, b: XY, z0: number, z1: number, bands: XY[], bay = 6, pier = 0.9, margin = 1.2) {
  const L = Math.hypot(b[0] - a[0], b[1] - a[1])
  const u: XY = [(b[0] - a[0]) / L, (b[1] - a[1]) / L]
  const n: V3 = [u[1], -u[0], 0]
  const v = (s: number, z: number, d = 0): V3 => [a[0] + u[0] * s + n[0] * d, a[1] + u[1] * s + n[1] * d, z]
  quad(stone, v(0, z0), v(L, z0), v(L, z1), v(0, z1), n)
  const inner = L - 2 * margin
  if (inner < bay * 0.6) return
  const k = Math.max(1, Math.round(inner / bay)), w = inner / k
  for (let i = 0; i < k; i++) {
    const s0 = margin + i * w + pier / 2, s1 = margin + (i + 1) * w - pier / 2
    for (const [zb, zt] of bands) quad(glazing, v(s0, zb, 0.04), v(s1, zb, 0.04), v(s1, zt, 0.04), v(s0, zt, 0.04), n)
  }
}
/** A wing body: four walls under an eave, windows on the faces listed. */
function body(x0: number, x1: number, y0: number, y1: number, z1: number, faces: Partial<Record<'w' | 'e' | 's' | 'n', XY[]>>) {
  wall([x0, y0], [x1, y0], 0, z1, faces.s ?? [])
  wall([x1, y0], [x1, y1], 0, z1, faces.e ?? [])
  wall([x1, y1], [x0, y1], 0, z1, faces.n ?? [])
  wall([x0, y1], [x0, y0], 0, z1, faces.w ?? [])
}
/** End walls of a vault: stone filling the arc above the eave. */
function gableFill(p: Part, a: Arc, y: number, s: number, zb: number, seg: number, off = 0) {
  const pts = arcPts(a, seg, off)
  for (let i = 0; i < seg; i++) {
    const P = pts[i], Q = pts[i + 1]
    quad(p, [P.x, y, zb], [Q.x, y, zb], [Q.x, y, Q.z], [P.x, y, P.z], [0, s, 0])
  }
}

// ---------------------------------------------------------------------------
// The bus shed.

const XA = -30.5, XB = 23.4            // OSM way/1501142074, across
const YS = -52.7, YN = 53.4            // its ends: 4th Street, Trade Street
// OSM gives the vault 8–17 m; the photos from both ends put its springing
// lower, at about 7 m, tucked against the wings' walls, which rise to 8 m.
const EAVE = 8, SPRING = 7, CROWN = 17
const T = 0.9                          // roof depth: truss and decking
const main = arc(XA, XB, SPRING, CROWN)
vault(main, YS, YN, T, 18, SPRING - 0.2, soffit)
const under = (x: number) => zAt(main, Math.min(XB - 0.01, Math.max(XA + 0.01, x)), -T)

// The end screens stand under the roof's overhang, on the column line.
const COLS = [-29.6, -7.4, 1.7, 22.0]   // OSM blue columns, both ends
const CHORD0 = 5.4, CHORD1 = 6.6        // the bottom truss over the bus opening
for (const [yg, s] of [[-51.7, -1], [52.4, 1]] as [number, number][]) {
  // The dark interior behind the open truss: a recessed slate panel under
  // the arch. It is `window` so that it glows at night, as the lit shed does
  // through the real truss.
  const ar = arc(XA, XB, SPRING, CROWN)
  const pts = arcPts(ar, 18, -T)
  for (let i = 0; i < 18; i++) {
    const P = pts[i], Q = pts[i + 1]
    quad(glazing, [P.x, yg - s * 0.8, CHORD1], [Q.x, yg - s * 0.8, CHORD1], [Q.x, yg - s * 0.8, Q.z], [P.x, yg - s * 0.8, P.z], [0, s, 0])
  }
  // Bottom truss: a deep white beam across the whole opening.
  box(white, XA, XB, yg - 0.5, yg + 0.5, CHORD0, CHORD1, true)
  // The truss grid above it: white verticals and two horizontal chords.
  const step = (XB - XA) / 15
  for (let i = 1; i < 15; i++) {
    const x = XA + i * step
    if (COLS.some((c) => Math.abs(c - x) < 1.2) || under(x) - 0.2 < CHORD1 + 0.4) continue
    box(white, x - 0.2, x + 0.2, yg - 0.25, yg + 0.25, CHORD1, under(x) - 0.2)
  }
  for (const z of [10.0, 13.4]) {
    // Where the chord meets the arch on each side.
    const dx = Math.sqrt((main.R - T) ** 2 - (z + 0.2 - main.zc0) ** 2)
    box(white, main.h - dx, main.h + dx, yg - 0.25, yg + 0.25, z - 0.2, z + 0.2)
  }
  // The blue arch chord under the teal edge.
  const ch = arcPts(main, 18, -T)
  for (let i = 0; i < 18; i++) strut(blue, [ch[i].x, ch[i].z - 0.4], [ch[i + 1].x, ch[i + 1].z - 0.4], yg + s * 0.3, 0.8, 0.5)
  // The blue columns: the outer pair to the eave, the centre pair to the arch.
  for (const x of COLS) column(blue, x, yg + s * 0.3, 0.55, 0, under(x) - 0.2, 10)
  // Blue braces: a diagonal from each centre column out to the arch, and an X between them.
  strut(blue, [-7.4, CHORD0], [-18.5, under(-18.5) - 0.4], yg, 0.5, 0.5)
  strut(blue, [1.7, CHORD0], [12.8, under(12.8) - 0.4], yg, 0.5, 0.5)
  strut(blue, [-7.4, CHORD1], [1.7, under(1.7) - 0.6], yg, 0.45, 0.45)
  strut(blue, [1.7, CHORD1], [-7.4, under(-7.4) - 0.6], yg, 0.45, 0.45)
}

// Columns inside the shed: white, with the blue bases of the photos.
for (const x of [-18.4, 11.0])
  for (const y of [-17.3, -8.2, 0.5, 9.3, 18.5]) {
    column(blue, x, y, 0.55, 0, 2.4, 10)
    column(white, x, y, 0.5, 2.4, under(x) + 0.05, 10)
  }

// Kiosks on the bus islands (OSM way/1501142094, way/1501142095): pale
// concrete boxes under dark hipped roofs.
function kiosk(x0: number, x1: number, y0: number, y1: number) {
  body(x0, x1, y0, y1, 3.0, { w: [[0.6, 2.4]], e: [[0.6, 2.4]] })
  const z0 = 3.0, z1 = 5.6, o = 0.4, r = Math.min(x1 - x0, y1 - y0) / 2 + o
  const c: V3[] = [[x0 - o, y0 - o, z0], [x1 + o, y0 - o, z0], [x1 + o, y1 + o, z0], [x0 - o, y1 + o, z0]]
  const mx = (x0 + x1) / 2
  const ridge: V3[] = [[mx, y0 - o + r, z1], [mx, y1 + o - r, z1]]
  const k = (z1 - z0) / r
  quad(soffit, c[0], c[1], ridge[0], ridge[0], unit([0, -k, 1]))
  quad(soffit, c[2], c[3], ridge[1], ridge[1], unit([0, k, 1]))
  quad(soffit, c[1], c[2], ridge[1], ridge[0], unit([k, 0, 1]))
  quad(soffit, c[3], c[0], ridge[0], ridge[1], unit([-k, 0, 1]))
}
kiosk(-6.4, 0.9, -49.9, -40.4)
kiosk(-9.5, -2.2, 25.8, 35.2)

// ---------------------------------------------------------------------------
// The wings, each under its own low teal vault with a small eave overhang.

const SHOP: XY = [1.4, 3.8], CLERE: XY = [4.9, 7.2]
// Light-rail side: concourse and shops (OSM 11 m, roof 3 m).
{
  const x0 = -43.2, x1 = XA, y0 = -40.9, y1 = 42.0
  body(x0, x1, y0, y1, EAVE, { w: [SHOP, CLERE], e: [SHOP], s: [SHOP, CLERE], n: [SHOP, CLERE] })
  const a = arc(x0 - 0.5, x1, EAVE, 11)
  vault(a, y0 - 0.5, y1 + 0.5, 0.5, 8, EAVE - 0.3, soffit)
  for (const [y, s] of [[y0, -1], [y1, 1]] as [number, number][]) gableFill(stone, arc(x0, x1, EAVE, 11 - 0.3), y, s, EAVE, 8)
}
// The small pavilion at its 4th Street end (way/1501142072: 5 m, roof 2 m).
{
  const x0 = -43.2, x1 = -30.6, y0 = -52.8, y1 = -47.8
  body(x0, x1, y0, y1, 3.0, { s: [[0.5, 2.6]], w: [] })
  const a = arc(x0 - 0.3, x1 + 0.3, 3.0, 5)
  vault(a, y0 - 0.3, y1 + 0.3, 0.35, 6, 2.8, soffit)
  for (const [y, s] of [[y0, -1], [y1, 1]] as [number, number][]) gableFill(stone, arc(x0, x1, 3.0, 4.8), y, s, 3.0, 6)
}
// Brevard side: the two-storey block (OSM 12 m, roof 4 m).
{
  const x0 = XB, x1 = 42.0, y0 = -50.2, y1 = 50.9
  body(x0, x1, y0, y1, EAVE, { e: [SHOP, CLERE], w: [SHOP], s: [SHOP, CLERE], n: [SHOP, CLERE] })
  const a = arc(x0, x1 + 0.5, EAVE, 12)
  vault(a, y0 - 0.5, y1 + 0.5, 0.5, 10, EAVE - 0.3, soffit)
  for (const [y, s] of [[y0, -1], [y1, 1]] as [number, number][]) gableFill(stone, arc(x0, x1, EAVE, 12 - 0.3), y, s, EAVE, 10)
}

// ---------------------------------------------------------------------------

// The shared palette (landmarks/STYLE.md). Two finishes carry the station's
// identity: the teal of its vaults, and CATS blue on the end columns and
// braces. Both pulled to the palette's lightness. The shed's underside and
// the interior seen through the end trusses are a mid grey, lighter than the
// real dark soffit so the large surface doesn't read as a hole.
const parts = [
  { part: teal, material: finish('ctc-teal', 0x86bfca) },
  { part: stone, material: PALETTE.stone },
  { part: white, material: PALETTE.trim },
  { part: blue, material: finish('cats-blue', 0x4f7fcf) },
  { part: glazing, material: PALETTE.window },
  { part: soffit, material: finish('shed-soffit', 0x7f878e) },
]
const triangles = parts.reduce((sum, { part }) => sum + part.triangles, 0)
if (triangles > 5000) throw new Error(`Triangle budget exceeded: ${triangles}`)
const glb = writeGlb('Charlotte Transportation Center', parts, {
  license: 'CC0-1.0', frame: 'Y up, -Z north, +X east, metres; origin at ground anchor',
  bearing: 47, elevation: 0, height: CROWN,
})
if (glb.length > 256000) throw new Error(`File budget exceeded: ${glb.length}`)
const out = new URL('../../landmarks/models/charlotte-transportation-center.glb', import.meta.url).pathname
await Bun.write(out, glb)
console.log(`${out}: ${triangles} triangles, ${glb.length} bytes`)
