/**
 * Unisphere — original, stylised CC0 geometry in metres, x east/y north/z up.
 * Run: bun scripts/landmarks/unisphere.ts
 * The 36.6 m globe is tilted 23.5 degrees; North America faces west.
 * No pool is included: this artwork replaces no OSM building extrusion.
 */
import { Part, cross, sub, len, writeGlb, type V3 } from './mesh'

const RAD = Math.PI / 180
const TAU = Math.PI * 2
const RADIUS = 18.3
const CENTRE: V3 = [0, 0, 24.5]
const TILT = 23.5 * RAD
const grid = new Part()
const land = new Part()
const orbits = new Part()
const pedestal = new Part()
const unit = (v: V3): V3 => v.map(n => n / (len(v) || 1)) as V3
const add = (a: V3, b: V3): V3 => a.map((n, i) => n + b[i]) as V3
const mul = (v: V3, s: number): V3 => v.map(n => n * s) as V3
const tilt = ([x, y, z]: V3): V3 => [x, y * Math.cos(TILT) + z * Math.sin(TILT),
  z * Math.cos(TILT) - y * Math.sin(TILT)]

type Geo = [number, number]
function globe([lon, lat]: Geo, radius = RADIUS): V3 {
  const a = lon * RAD, b = lat * RAD
  return add(CENTRE, tilt([radius * Math.cos(b) * Math.sin(a),
    -radius * Math.cos(b) * Math.cos(a), radius * Math.sin(b)]))
}

/** Closed tubes with analytic normals, so tiny struts shade round at map scale. */
function tube(part: Part, path: V3[], radius: number, sides = 3) {
  const rings = path.map((p, i) => {
    const axis = unit(sub(path[(i + 1) % path.length], path[(i + path.length - 1) % path.length]))
    // All paths are circles centred on the globe; radial frames have no seam.
    const u = unit(sub(p, CENTRE))
    const v = unit(cross(axis, u))
    return Array.from({ length: sides }, (_, j) => {
      const n = add(mul(u, Math.cos(j / sides * TAU)), mul(v, Math.sin(j / sides * TAU)))
      return { p: add(p, mul(n, radius)), n }
    })
  })
  for (let i = 0; i < rings.length; i++) for (let j = 0; j < sides; j++) {
    const a = rings[i][j], b = rings[i][(j + 1) % sides]
    const c = rings[(i + 1) % rings.length][(j + 1) % sides], d = rings[(i + 1) % rings.length][j]
    part.tri(a.p, b.p, c.p, undefined, undefined, undefined, [a.n, b.n, c.n])
    part.tri(a.p, c.p, d.p, undefined, undefined, undefined, [a.n, c.n, d.n])
  }
}

// 24 longitude half-circles and eleven parallels, genuinely open on both sides.
for (let lon = 0; lon < 180; lon += 15) {
  const a = lon * RAD
  tube(grid, Array.from({ length: 48 }, (_, i) => {
    const t = i / 48 * TAU
    return add(CENTRE, tilt([RADIUS * Math.cos(t) * Math.sin(a),
      -RADIUS * Math.cos(t) * Math.cos(a), RADIUS * Math.sin(t)]))
  }), 0.105)
}
for (let lat = -75; lat <= 75; lat += 15) {
  const steps = Math.max(16, Math.round(48 * Math.cos(lat * RAD) / 4) * 4)
  tube(grid, Array.from({ length: steps }, (_, i) => globe([i / steps * 360, lat])), lat === 0 ? 0.14 : 0.105)
}

// Hand-drawn geographic outlines. Bays, narrow isthmuses and the major
// peninsulas carry recognition without country borders or tiny islands.
const continents: Record<string, Geo[]> = {
  northAmerica: [[-168,66],[-158,71],[-145,70],[-139,61],[-129,70],[-113,73],[-98,73],[-88,70],[-81,64],[-83,57],[-91,57],[-95,61],[-96,53],[-87,50],[-80,53],[-75,62],[-63,59],[-53,52],[-61,47],[-68,45],[-73,40],[-76,35],[-80,32],[-80,25],[-83,26],[-85,30],[-90,29],[-97,26],[-97,21],[-90,18],[-86,21],[-87,16],[-83,15],[-82,9],[-77,8],[-80,6],[-85,10],[-91,14],[-98,16],[-105,22],[-110,24],[-115,29],[-112,31],[-107,24],[-109,30],[-117,33],[-124,41],[-125,49],[-133,55],[-142,59],[-151,59],[-161,55],[-166,57],[-159,61],[-166,64]],
  southAmerica: [[-81,11],[-73,11],[-66,10],[-60,6],[-51,4],[-49,0],[-43,-3],[-35,-6],[-35,-12],[-39,-18],[-41,-23],[-48,-28],[-53,-34],[-58,-39],[-63,-43],[-65,-50],[-68,-55],[-73,-52],[-75,-43],[-74,-34],[-71,-29],[-70,-19],[-76,-14],[-81,-5],[-78,1]],
  greenland: [[-53,59],[-43,60],[-39,65],[-25,71],[-19,78],[-28,83],[-46,83],[-61,79],[-68,75],[-57,68]],
  africa: [[-17,21],[-17,15],[-13,8],[-7,5],[3,5],[9,3],[10,-4],[13,-10],[12,-17],[16,-28],[19,-35],[27,-34],[33,-26],[35,-18],[40,-11],[42,-3],[51,12],[44,11],[39,17],[35,23],[32,30],[25,32],[20,33],[12,33],[10,37],[2,36],[-6,36],[-12,29]],
  eurasia: [[-10,36],[-9,43],[-2,44],[-5,48],[2,51],[7,54],[9,58],[5,59],[6,63],[15,70],[25,71],[32,70],[29,64],[40,66],[49,68],[61,70],[72,73],[90,77],[109,77],[122,73],[141,72],[153,70],[168,69],[180,66],[190,65],[189,60],[177,61],[170,58],[162,60],[163,55],[158,51],[153,55],[146,59],[140,54],[140,46],[133,43],[129,35],[125,39],[121,37],[122,30],[119,24],[111,20],[109,11],[105,9],[101,15],[104,2],[100,2],[97,15],[91,22],[87,21],[80,8],[76,9],[72,20],[67,24],[60,25],[57,23],[58,18],[51,13],[44,13],[40,20],[35,29],[36,35],[28,36],[27,41],[23,40],[24,35],[20,39],[18,40],[16,38],[13,42],[8,44],[3,42],[0,39]],
  australia: [[113,-22],[114,-28],[116,-34],[126,-34],[130,-31],[136,-35],[145,-39],[151,-34],[153,-26],[149,-20],[145,-15],[143,-11],[140,-17],[136,-12],[130,-12],[129,-16],[122,-17],[119,-20]],
  madagascar: [[49,-12],[50,-17],[47,-25],[44,-25],[44,-19]],
  japan: [[131,31],[133,34],[137,36],[140,41],[141,45],[145,44],[143,40],[141,36],[136,33]],
  britain: [[-6,50],[1,51],[0,54],[-3,59],[-6,58],[-5,54]],
  ireland: [[-10,51],[-6,52],[-6,55],[-9,55]],
  sumatra: [[95,6],[101,1],[106,-6],[103,-6],[98,0]],
  java: [[105,-6],[114,-7],[115,-9],[107,-8]],
  borneo: [[109,1],[110,5],[117,7],[119,1],[116,-4],[111,-3]],
  philippines: [[120,19],[123,18],[123,14],[126,10],[126,6],[122,7],[121,12]],
  newGuinea: [[131,-1],[140,-3],[150,-6],[150,-10],[141,-9],[135,-5]],
  tasmania: [[144,-40],[148,-41],[147,-44],[145,-43]],
  newZealand: [[173,-35],[178,-38],[176,-41],[172,-42],[168,-46],[166,-45],[170,-41],[174,-39]],
  cuba: [[-85,23],[-79,23],[-74,20],[-78,20]],
  iceland: [[-24,64],[-14,64],[-14,66],[-21,67]],
}

const area2 = (a: Geo, b: Geo, c: Geo) => (b[0] - a[0]) * (c[1] - a[1]) - (b[1] - a[1]) * (c[0] - a[0])
function triangulate(outline: Geo[]): Geo[][] {
  const signed = outline.reduce((n, p, i) => {
    const q = outline[(i + 1) % outline.length]
    return n + p[0] * q[1] - p[1] * q[0]
  }, 0)
  const points = signed > 0 ? [...outline] : [...outline].reverse()
  const result: Geo[][] = []
  while (points.length > 3) {
    let found = false
    for (let i = 0; i < points.length; i++) {
      const a = points[(i + points.length - 1) % points.length], b = points[i], c = points[(i + 1) % points.length]
      if (area2(a, b, c) <= 1e-8) continue
      if (points.some(p => p !== a && p !== b && p !== c &&
        area2(a, b, p) >= -1e-8 && area2(b, c, p) >= -1e-8 && area2(c, a, p) >= -1e-8)) continue
      result.push([a, b, c]); points.splice(i, 1); found = true; break
    }
    if (!found) throw new Error('Self-intersecting continent outline')
  }
  result.push(points)
  return result
}

// Subdivide in geographic space: a single triangle fan would cut through
// the globe and turn the large continents into flat slabs.
function panel(a: Geo, b: Geo, c: Geo) {
  const pairs = [[a, b, c], [b, c, a], [c, a, b]]
  const distance = (p: Geo, q: Geo) => Math.hypot((p[0] - q[0]) * Math.cos((p[1] + q[1]) / 2 * RAD), p[1] - q[1])
  pairs.sort((p, q) => distance(q[0], q[1]) - distance(p[0], p[1]))
  const [p, q, r] = pairs[0]
  if (distance(p, q) > 11) {
    const mid: Geo = [(p[0] + q[0]) / 2, (p[1] + q[1]) / 2]
    panel(p, mid, r); panel(mid, q, r)
    return
  }
  const vertices = [a, b, c].map(p => globe(p, RADIUS + 0.18))
  const normals = vertices.map(p => unit(sub(p, CENTRE)))
  land.tri(vertices[0], vertices[1], vertices[2], undefined, undefined, undefined, normals)
}
for (const outline of Object.values(continents)) {
  for (const [a, b, c] of triangulate(outline)) panel(a, b, c)
  // A narrow dark edge makes the raised panels read as cut steel, not paint.
  for (let i = 0; i < outline.length; i++) {
    const a = outline[i], b = outline[(i + 1) % outline.length]
    const steps = Math.ceil(Math.hypot(a[0] - b[0], a[1] - b[1]) / 5)
    for (let j = 0; j < steps; j++) {
      const at = (t: number): Geo => [a[0] + (b[0] - a[0]) * t, a[1] + (b[1] - a[1]) * t]
      const p = at(j / steps), q = at((j + 1) / steps)
      land.quad(globe(p, RADIUS + 0.04), globe(q, RADIUS + 0.04), globe(q, RADIUS + 0.18), globe(p, RADIUS + 0.18))
    }
  }
}

// Antarctica is a polar cap, not a longitude/latitude polygon crossing a
// seam. Its peninsula reaches toward South America; the rest stays compact.
const antarcticCoast = [-70, -70, -69, -68, -67, -66, -67, -67, -68, -69, -71, -75,
  -78, -77, -75, -73, -72, -73, -71, -65, -63, -69, -73, -72]
for (let i = 0; i < antarcticCoast.length; i++) {
  const a: Geo = [i * 15, antarcticCoast[i]]
  const b: Geo = [(i + 1) * 15, antarcticCoast[(i + 1) % antarcticCoast.length]]
  panel([i * 15 + 7.5, -90], b, a)
  land.quad(globe(b, RADIUS + 0.04), globe(a, RADIUS + 0.04), globe(a, RADIUS + 0.18), globe(b, RADIUS + 0.18))
}

// Three satellite orbits: different inclinations and ascending nodes, with
// enough separation outside the globe to remain readable in small renders.
for (const [inclination, node, radius] of [[32, -30, 20.1], [49, 20, 20.35], [63, 66, 20.5]]) {
  const i = inclination * RAD, n = node * RAD
  const u: V3 = [Math.cos(n), Math.sin(n), 0]
  const v: V3 = [-Math.sin(n) * Math.cos(i), Math.cos(n) * Math.cos(i), Math.sin(i)]
  tube(orbits, Array.from({ length: 96 }, (_, k) => {
    const a = k / 96 * TAU
    return add(CENTRE, add(mul(u, radius * Math.cos(a)), mul(v, radius * Math.sin(a))))
  }), 0.19, 4)
}

// Three broad, tapered steel legs, curving inward into the globe's underside.
for (let leg = 0; leg < 3; leg++) {
  const angle = (leg * 120 + 30) * RAD
  const rings = [[0, 4.3, 0, 0.85], [0.45, 4.3, -0.1, 0.85], [2.0, 3.35, -0.6, 0.88],
    [4.0, 2.3, -1.2, 0.77], [5.7, 1.55, -1.9, 0.65], [7.2, 1.1, -2.3, 0.5]].map(([z, r, y, w]) => {
    const x = Math.cos(angle) * r, yy = Math.sin(angle) * r + y
    return [[-w, -0.65], [w, -0.65], [w, 0.65], [-w, 0.65]].map(([a, b]): V3 =>
      [x + a * Math.cos(angle) - b * Math.sin(angle), yy + a * Math.sin(angle) + b * Math.cos(angle), z])
  })
  pedestal.loft(rings)
  pedestal.cap(rings[0], false)
  pedestal.cap(rings[rings.length - 1], true)
}
// A short faceted saddle joins the legs into one conical support and meets
// the lower globe framing, avoiding three disconnected plank-like supports.
const saddle = [[5.4, 1.7, -1.8], [6.2, 1.45, -2.1], [7.2, 1.2, -2.3]].map(([z, r, y]) =>
  Array.from({ length: 12 }, (_, j): V3 => [r * Math.cos(j / 12 * TAU), y + r * Math.sin(j / 12 * TAU), z]))
pedestal.loft(saddle)
pedestal.cap(saddle[0], false)
pedestal.cap(saddle[saddle.length - 1], true)

const parts = [
  { part: grid, material: { name: 'steel-grid', color: 0xa9b7bf, roughness: 0.6, doubleSided: true } },
  { part: land, material: { name: 'steel-continents', color: 0xc9d2d7, roughness: 0.65, doubleSided: true } },
  { part: orbits, material: { name: 'steel-orbits', color: 0xa4b1bb, roughness: 0.5, doubleSided: true } },
  { part: pedestal, material: { name: 'steel-base', color: 0x98a6af, roughness: 0.7 } },
]
const triangles = parts.reduce((n, { part }) => n + part.triangles, 0)
if (triangles > 12000) throw new Error(`Triangle budget exceeded: ${triangles}`)
const glb = writeGlb('Unisphere', parts, {
  license: 'CC0-1.0', frame: 'Y up, -Z north, +X east, metres, origin at ground',
  globeDiameter: 36.6, axialTilt: 23.5, orbitalRings: 3, bearing: 0, elevation: 0,
})
await Bun.write(new URL('../../landmarks/models/unisphere.glb', import.meta.url), glb)
console.log(`unisphere.glb: ${triangles} triangles, ${glb.length} bytes`)
console.log(parts.map(({ part, material }) => `${material.name}: ${part.triangles}`).join('\n'))
