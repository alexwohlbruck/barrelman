/**
 * Belk Theater, Blumenthal Performing Arts Center, Charlotte (Cesar Pelli,
 * 1992) — procedural, CC0-1.0.
 * bun scripts/landmarks/belk-theater.ts
 *
 * Map frame: x east, y north, z up, metres, placed at bearing 317°: the
 * model's +y runs north-west along the outline's long side (way/502718738,
 * 41 × 111 m) toward the plaza off North Tryon Street, so the curved glass
 * lobby is the model's north end, East 5th Street runs along its east face and
 * North College Street along its south face. The anchor is the outline's
 * centroid. Only the theatre's own outline is modelled: Founders Hall and the
 * Bank of America tower to the west are left to the map.
 *
 * What makes it the Belk is its lobby: a low curved wall of glass between
 * silver mullions, a silver band along its top, a glass canopy over the doors
 * and a small glass dome on its roof. Behind it the hall is a tall plain
 * block of buff brick with rounded front corners, on a red-brown base whose
 * piers make an arcade along 5th Street and College Street.
 *
 * The ground falls about 4.7 m from the lobby to College Street (terrarium
 * DEM), so y = 0 is the College Street end and the lobby stands on G_LOBBY.
 *
 * References (visual only): BroadwayTour.net, "Blumenthal Performing Arts
 * Center in Charlotte, NC" (Flickr 7043851939 and 7043851891, CC BY-SA 2.0);
 * James Willamor, "Blumenthal Performing Arts, Charlotte" (Flickr 2947782874,
 * CC BY-SA 2.0, the roof and dome from above); Mapillary street views on 5th
 * and College Streets (allen 2016, JordanAnderson 2022, CC BY-SA 4.0); USGS
 * NAIP orthoimagery (public domain) for the lobby plan and the dome.
 */
import { Part, writeGlb, type V3 } from './mesh'
import { PALETTE, finish } from './palette'

const tan = new Part(), brick = new Part(), alu = new Part()
const win = new Part(), glass = new Part(), roof = new Part()

type XY = [number, number]
const unit = (v: V3): V3 => { const l = Math.hypot(...v) || 1; return [v[0] / l, v[1] / l, v[2] / l] }
function quadN(p: Part, a: V3, b: V3, c: V3, d: V3, n?: V3[]) {
  p.tri(a, b, c, undefined, undefined, undefined, n && [n[0], n[1], n[2]])
  p.tri(a, c, d, undefined, undefined, undefined, n && [n[0], n[2], n[3]])
}

// ---------------------------------------------------------------------------
// Polygon helpers: rings are counter-clockwise from above.

function area(r: XY[]) { let a = 0; r.forEach((p, i) => { const q = r[(i + 1) % r.length]; a += p[0] * q[1] - q[0] * p[1] }); return a / 2 }
function triangulate(r: XY[]): [number, number, number][] {
  const idx = r.map((_, i) => i), out: [number, number, number][] = []
  const cr = (a: XY, b: XY, c: XY) => (b[0] - a[0]) * (c[1] - a[1]) - (b[1] - a[1]) * (c[0] - a[0])
  const inside = (p: XY, a: XY, b: XY, c: XY) => cr(a, b, p) > 1e-9 && cr(b, c, p) > 1e-9 && cr(c, a, p) > 1e-9
  let guard = 0
  while (idx.length > 3 && guard++ < 5000) {
    for (let i = 0; i < idx.length; i++) {
      const a = idx[(i + idx.length - 1) % idx.length], b = idx[i], c = idx[(i + 1) % idx.length]
      if (cr(r[a], r[b], r[c]) <= 1e-9) continue
      if (idx.some((j) => j !== a && j !== b && j !== c && inside(r[j], r[a], r[b], r[c]))) continue
      out.push([a, b, c]); idx.splice(i, 1); break
    }
  }
  out.push([idx[0], idx[1], idx[2]])
  return out
}
function offset(r: XY[], d: number): XY[] {
  const n = r.length
  return r.map((p, i) => {
    const a = r[(i + n - 1) % n], b = r[(i + 1) % n]
    const e1 = unit([p[0] - a[0], p[1] - a[1], 0]), e2 = unit([b[0] - p[0], b[1] - p[1], 0])
    const n1: XY = [e1[1], -e1[0]], n2: XY = [e2[1], -e2[0]]
    const m = unit([n1[0] + n2[0], n1[1] + n2[1], 0]), cos = m[0] * n1[0] + m[1] * n1[1]
    const k = d / Math.max(cos, 0.3)
    return [p[0] + m[0] * k, p[1] + m[1] * k]
  })
}
const edgeN = (a: XY, b: XY): V3 => unit([b[1] - a[1], -(b[0] - a[0]), 0])
/** Per-vertex normals: averaged across gentle bends (curves), split at corners. */
function vertexNormals(r: XY[], i: number, j: number): [V3, V3] {
  const n = r.length, N = edgeN(r[i], r[j])
  const prev = edgeN(r[(i + n - 1) % n], r[i]), next = edgeN(r[j], r[(j + 1) % n])
  const smooth = (a: V3, b: V3) => (a[0] * b[0] + a[1] * b[1] > Math.cos(Math.PI / 7) ? unit([a[0] + b[0], a[1] + b[1], 0]) : a)
  return [smooth(N, prev), smooth(N, next)]
}
function walls(p: Part, r: XY[], z0: number, z1: number) {
  for (let i = 0; i < r.length; i++) {
    const j = (i + 1) % r.length, a = r[i], b = r[j], [na, nb] = vertexNormals(r, i, j)
    quadN(p, [a[0], a[1], z0], [b[0], b[1], z0], [b[0], b[1], z1], [a[0], a[1], z1], [na, nb, nb, na])
  }
}
function cap(p: Part, r: XY[], z: number, up = true) {
  const N: V3 = [0, 0, up ? 1 : -1]
  for (const [a, b, c] of triangulate(r)) {
    const A: V3 = [r[a][0], r[a][1], z], B: V3 = [r[b][0], r[b][1], z], C: V3 = [r[c][0], r[c][1], z]
    if (up) p.tri(A, B, C, undefined, undefined, undefined, [N, N, N])
    else p.tri(A, C, B, undefined, undefined, undefined, [N, N, N])
  }
}
/** A solid with a chamfered top edge of size b; the top goes to `top`. */
function prism(p: Part, r: XY[], z0: number, z1: number, b: number, top: Part = p) {
  walls(p, r, z0, z1 - b)
  const r2 = offset(r, -b)
  for (let i = 0; i < r.length; i++) {
    const j = (i + 1) % r.length, [na, nb] = vertexNormals(r, i, j)
    const ma = unit([na[0], na[1], 1]), mb = unit([nb[0], nb[1], 1])
    quadN(p, [r[i][0], r[i][1], z1 - b], [r[j][0], r[j][1], z1 - b], [r2[j][0], r2[j][1], z1], [r2[i][0], r2[i][1], z1], [ma, mb, mb, ma])
  }
  cap(top, r2, z1)
}
/** A quarter arc of radius R about c, from angle a0 to a1 (degrees), n segments. */
function arc(c: XY, R: number, a0: number, a1: number, n: number): XY[] {
  const out: XY[] = []
  for (let i = 0; i <= n; i++) { const t = (a0 + (a1 - a0) * i / n) * Math.PI / 180; out.push([c[0] + R * Math.cos(t), c[1] + R * Math.sin(t)]) }
  return out
}
/** A flat panel lying on a vertical wall, from p to q along it, z0 to z1, pushed out by o. */
function panel(part: Part, p: XY, q: XY, z0: number, z1: number, o = 0.04) {
  const N = edgeN(p, q), a: XY = [p[0] + N[0] * o, p[1] + N[1] * o], b: XY = [q[0] + N[0] * o, q[1] + N[1] * o]
  quadN(part, [a[0], a[1], z0], [b[0], b[1], z0], [b[0], b[1], z1], [a[0], a[1], z1], [N, N, N, N])
}

// ---------------------------------------------------------------------------
// Dimensions, from the OSM outline in the model frame (u east-ish along +x,
// v north-west along +y), the DEM and the photos.

const XW = -22.0, XE = 19.4      // Founders Hall side, 5th Street side
const YS = -49.5                 // College Street face
const YH = 33                    // the hall's front, behind the lobby
const G_LOBBY = 4.6              // lobby floor above the College Street end
const Z_HALL = 31                // hall roof (OSM 30 m, from the higher end)
const Z_LOBBY = G_LOBBY + 15.5     // lobby roof
const Z_ARC = 10                 // top of the arcade, level along the slope
const RC = 8                     // how far the drum's ends sit behind its crown

// The hall: a buff brick block with rounded front corners.
const HALL: XY[] = [
  [XW, YS], [XE, YS],
  // The front, behind the lobby, is a shallow drum: an arc across the full width.
  ...(() => { const R = 30.8, c: XY = [(XW + XE) / 2, YH - R], h = Math.asin((XE - XW) / 2 / R) * 180 / Math.PI
    return arc(c, R, 90 - h, 90 + h, 12) })(),
]
// The lobby: the OSM curve, smoothed, from the 5th Street side round to the plaza.
const bez = (p0: XY, c: XY, p2: XY, n: number): XY[] => Array.from({ length: n + 1 }, (_, i) => {
  const t = i / n, s = 1 - t
  return [s * s * p0[0] + 2 * s * t * c[0] + t * t * p2[0], s * s * p0[1] + 2 * s * t * c[1] + t * t * p2[1]] as XY
})
const CURVE = bez([16.6, 60.9], [-8.5, 60.2], [-10.2, 46.5], 18)
const LOBBY: XY[] = [[XW, YH - RC], [XE, YH - RC], [18.8, 49.2], [16.6, 49.1], ...CURVE, [-21.1, 46.7]]
for (const r of [HALL, LOBBY]) if (area(r) <= 0) throw new Error('rings must be counter-clockwise')

// Hall walls. Along 5th Street (east) and College Street (south) the ground
// storey is an arcade: brick piers on the building line carrying a brick
// beam, in front of a buff wall set 2.5 m back. Above the beam a pale stone
// band, buff brick to the top and a pale coping.
const AR = 2.5, Z_BEAM = Z_ARC - 1.6
const BASE: XY[] = [[XW, YS + AR], [XE - AR, YS + AR], [XE - AR, YH - RC], ...HALL.slice(2)]
if (area(BASE) <= 0) throw new Error('rings must be counter-clockwise')
prism(tan, BASE, 0, Z_BEAM, 0, tan)
// On the Founders Hall side there is no arcade: the brick base runs solid.
panel(brick, [XW, YH - RC], [XW, YS + AR], 0, Z_BEAM, 0.03)
walls(brick, HALL, Z_BEAM, Z_ARC)
cap(brick, HALL, Z_BEAM, false)
prism(alu, offset(HALL, 0.12), Z_ARC, Z_ARC + 0.8, 0.12, tan)
prism(tan, HALL, Z_ARC + 0.8, Z_HALL - 1.2, 0)
// Two pale stone bands round the hall, as on the drum seen from above.
for (const z of [Z_LOBBY + 1.5, Z_LOBBY + 5]) walls(alu, offset(HALL, 0.05), z, z + 0.5)
prism(alu, offset(HALL, 0.15), Z_HALL - 1.2, Z_HALL, 0.4, roof)
{
  const PW = 1.4
  const pier = (x0: number, x1: number, y0: number, y1: number) => prism(brick, [[x0, y0], [x1, y0], [x1, y1], [x0, y1]], 0, Z_BEAM, 0, brick)
  // East: piers from the College Street corner to where the lobby meets it,
  // with shopfront glass on the back wall between them.
  const n = 12, ys = Array.from({ length: n + 1 }, (_, i) => YS + PW / 2 + (YH - RC - PW / 2 - YS - PW / 2) * i / n)
  ys.forEach((y, i) => {
    pier(XE - AR, XE, y - PW / 2, y + PW / 2)
    if (i < n) panel(win, [XE - AR, y + PW / 2 + 0.8], [XE - AR, ys[i + 1] - PW / 2 - 0.8], 0, Z_BEAM - 2.2)
  })
  // South: the same, corner piers shared.
  const m = 6, xs = Array.from({ length: m + 1 }, (_, i) => XW + PW / 2 + (XE - XW - PW) * i / m)
  xs.forEach((x, i) => {
    if (i < m) pier(x - PW / 2, x + PW / 2, YS, YS + AR)
    if (i < m) panel(win, [xs[i + 1] - PW / 2 - 0.8, YS + AR], [x + PW / 2 + 0.8, YS + AR], 0, Z_BEAM - 2.2)
  })
}

// The lobby: a curved glass wall under a silver band, on the higher ground.
{
  const front = [[18.8, 49.2] as XY, [16.6, 49.1] as XY, ...CURVE, [-21.1, 46.7] as XY]
  // The solid behind the glass, then the glass itself just outside it.
  prism(tan, LOBBY, 0, Z_LOBBY - 3.4, 0, roof)
  for (let i = 0; i < front.length - 1; i++) {
    const a = front[i], b = front[i + 1], [na, nb] = vertexNormals(LOBBY, (i + 2) % LOBBY.length, (i + 3) % LOBBY.length)
    const o = 0.04, A: XY = [a[0] + na[0] * o, a[1] + na[1] * o], B: XY = [b[0] + nb[0] * o, b[1] + nb[1] * o]
    quadN(win, [A[0], A[1], 0], [B[0], B[1], 0], [B[0], B[1], Z_LOBBY - 3.4], [A[0], A[1], Z_LOBBY - 3.4], [na, nb, nb, na])
  }
  // The straight run along 5th Street to the hall, glazed too.
  panel(win, [XE, YH - RC + 0.5], [18.8, 49.2], 0, Z_LOBBY - 3.4)
  // Its mullions, and those of the short straight face to the plaza.
  for (const [p, q] of [[[XE, YH - RC], [18.8, 49.2]], [[-10.2, 46.5], [-21.1, 46.7]]] as [XY, XY][]) {
    const L = Math.hypot(q[0] - p[0], q[1] - p[1]), k = Math.max(1, Math.round(L / 3.4)), N = edgeN(p, q)
    const t: XY = [(q[0] - p[0]) / L, (q[1] - p[1]) / L]
    for (let i = 1; i < k; i++) {
      const c: XY = [p[0] + t[0] * L * i / k + N[0] * 0.05, p[1] + t[1] * L * i / k + N[1] * 0.05], w = 0.22, d = 0.35
      const r: XY[] = [[c[0] - t[0] * w, c[1] - t[1] * w], [c[0] + t[0] * w, c[1] + t[1] * w], [c[0] + t[0] * w + N[0] * d, c[1] + t[1] * w + N[1] * d], [c[0] - t[0] * w + N[0] * d, c[1] - t[1] * w + N[1] * d]]
      prism(alu, area(r) > 0 ? r : r.reverse(), G_LOBBY - 1, Z_LOBBY - 3.4, 0, alu)
    }
  }
  // The silver band, bevelled.
  prism(alu, offset(LOBBY, 0.15), Z_LOBBY - 3.4, Z_LOBBY, 0.35, roof)
  // Silver mullions: one at every other vertex of the curve, full height.
  for (let i = 0; i < CURVE.length; i += 1) {
    const p = CURVE[i], n = vertexNormals(CURVE, Math.max(i - 1, 0), i)[1]
    const t: XY = [-n[1], n[0]], w = 0.22, o = 0.05, d = 0.35
    const r: XY[] = [
      [p[0] + t[0] * w + n[0] * o, p[1] + t[1] * w + n[1] * o], [p[0] - t[0] * w + n[0] * o, p[1] - t[1] * w + n[1] * o],
      [p[0] - t[0] * w + n[0] * (o + d), p[1] - t[1] * w + n[1] * (o + d)], [p[0] + t[0] * w + n[0] * (o + d), p[1] + t[1] * w + n[1] * (o + d)],
    ]
    prism(alu, area(r) > 0 ? r : r.reverse(), G_LOBBY - 1, Z_LOBBY - 3.4, 0, alu)
  }
  // A transom line across the glass, where the canopy meets it.
  for (let i = 0; i < CURVE.length - 1; i++) {
    const a = CURVE[i], b = CURVE[i + 1], N = edgeN(a, b), o = 0.08
    quadN(alu, [a[0] + N[0] * o, a[1] + N[1] * o, G_LOBBY + 4.3], [b[0] + N[0] * o, b[1] + N[1] * o, G_LOBBY + 4.3], [b[0] + N[0] * o, b[1] + N[1] * o, G_LOBBY + 4.8], [a[0] + N[0] * o, a[1] + N[1] * o, G_LOBBY + 4.8], [N, N, N, N])
  }
  // The glass canopy, a thin slab 2.6 m out from the curve.
  const out = CURVE.map((p, i) => {
    const n = i === 0 ? edgeN(p, CURVE[1]) : i === CURVE.length - 1 ? edgeN(CURVE[i - 1], p) : unit([...((a, b) => [a[0] + b[0], a[1] + b[1]])(edgeN(CURVE[i - 1], p), edgeN(p, CURVE[i + 1])), 0] as V3)
    return [p[0] + n[0] * 2.2, p[1] + n[1] * 2.2] as XY
  })
  const ring: XY[] = [...CURVE.slice(1, -1), ...out.slice(1, -1).reverse()]
  const zc = G_LOBBY + 4.6
  prism(glass, area(ring) > 0 ? ring : ring.reverse(), zc - 0.25, zc, 0, glass)
  // Underside of the canopy.
  cap(glass, area(ring) > 0 ? ring : ring.reverse(), zc - 0.25, false)
}
// The hall's front above the lobby roof is part of the hall ring already.

// The glass dome on the lobby roof, on a short silver drum.
{
  const c: XY = [1, 43], R = 5.0, z0 = Z_LOBBY, seg = 16, rings = 5
  const drum = Array.from({ length: seg }, (_, i) => [c[0] + R * Math.cos(i / seg * 2 * Math.PI), c[1] + R * Math.sin(i / seg * 2 * Math.PI)] as XY)
  prism(alu, drum, z0 - 0.5, z0 + 0.7, 0.15, alu)
  const zb = z0 + 0.7, Rd = R - 0.15
  const pt = (i: number, k: number): V3 => {
    const ph = (k / rings) * Math.PI / 2, th = (i / seg) * 2 * Math.PI
    return [c[0] + Rd * Math.cos(ph) * Math.cos(th), c[1] + Rd * Math.cos(ph) * Math.sin(th), zb + Rd * 0.95 * Math.sin(ph)]
  }
  const nrm = (i: number, k: number): V3 => {
    const ph = (k / rings) * Math.PI / 2, th = (i / seg) * 2 * Math.PI
    return unit([Math.cos(ph) * Math.cos(th), Math.cos(ph) * Math.sin(th), Math.sin(ph) / 0.95])
  }
  for (let k = 0; k < rings; k++) for (let i = 0; i < seg; i++) {
    const j = (i + 1) % seg
    if (k === rings - 1) glass.tri(pt(i, k), pt(j, k), pt(i, k + 1), undefined, undefined, undefined, [nrm(i, k), nrm(j, k), nrm(i, k + 1)])
    else quadN(glass, pt(i, k), pt(j, k), pt(j, k + 1), pt(i, k + 1), [nrm(i, k), nrm(j, k), nrm(j, k + 1), nrm(i, k + 1)])
  }
}


// ---------------------------------------------------------------------------

// Colours from the daylight photos: the hall's buff brick, a red-brown base
// pulled light, brushed aluminium, slate lobby glass, the pale dome.
const parts = [
  { part: tan, material: finish('buff-brick', 0xe3d2b6) },
  { part: brick, material: finish('red-brick', 0xb98a78) },
  { part: alu, material: finish('aluminium', 0xd3d7da, 0.5) },
  // The glass reads light and silvery by day, so a lighter slate than the default.
  { part: win, material: { ...PALETTE.window, color: 0x8299ab } },
  { part: glass, material: PALETTE.glass },
  { part: roof, material: PALETTE.roof },
]
const triangles = parts.reduce((s, { part }) => s + part.triangles, 0)
console.log(parts.map(({ part, material }) => `${material.name}: ${part.triangles}`).join(', '))
if (triangles > 5000) throw new Error(`Triangle budget exceeded: ${triangles}`)
const glb = writeGlb('Belk Theater', parts, {
  license: 'CC0-1.0', frame: 'Y up, -Z north, +X east, metres; origin at the ground anchor',
  bearing: 317, osm: 'way/502718738', height: Z_HALL,
})
if (glb.length > 256000) throw new Error(`File budget exceeded: ${glb.length}`)
const outFile = new URL('../../landmarks/models/belk-theater.glb', import.meta.url).pathname
await Bun.write(outFile, glb)
console.log(`${outFile}: ${triangles} triangles, ${glb.length} bytes`)
