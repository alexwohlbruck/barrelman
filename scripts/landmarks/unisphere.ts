/**
 * Unisphere — original, stylised CC0 geometry in metres, x east/y north/z up.
 * Run: bun scripts/landmarks/unisphere.ts
 *
 * A 36.6 m open steel globe tilted 23.5 degrees (north pole leaning north),
 * with the Americas facing west, three orbital rings and a conical tripod
 * base. Catalog bearing 0: the model's north is the map's north. No pool is
 * included: this artwork replaces no OSM building extrusion.
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
const dot = (a: V3, b: V3) => a[0] * b[0] + a[1] * b[1] + a[2] * b[2]
const tilt = ([x, y, z]: V3): V3 => [x, y * Math.cos(TILT) + z * Math.sin(TILT),
  z * Math.cos(TILT) - y * Math.sin(TILT)]

type Geo = [number, number]
function globe([lon, lat]: Geo, radius = RADIUS): V3 {
  const a = lon * RAD, b = lat * RAD
  return add(CENTRE, tilt([radius * Math.cos(b) * Math.sin(a),
    -radius * Math.cos(b) * Math.cos(a), radius * Math.sin(b)]))
}

/** A triangle facing along `normals`, whatever order its corners came in. */
function face(part: Part, v: V3[], normals: V3[]) {
  const n = cross(sub(v[1], v[0]), sub(v[2], v[0]))
  const mean = add(add(normals[0], normals[1]), normals[2])
  if (dot(n, mean) >= 0) part.tri(v[0], v[1], v[2], undefined, undefined, undefined, normals)
  else part.tri(v[0], v[2], v[1], undefined, undefined, undefined, [normals[0], normals[2], normals[1]])
}

/**
 * Sweep a closed cross-section round a closed path centred on the globe.
 * The profile is [radial, lateral] offsets, counter-clockwise; each facet is
 * flat across the section and smooth along the path, so a chamfer reads as a
 * bevel catching light rather than as a faceted tube.
 */
function sweep(part: Part, path: V3[], profile: [number, number][]) {
  const m = profile.length
  const facets = profile.map(([a, b], k) => {
    const [c, d] = profile[(k + 1) % m]
    const l = Math.hypot(c - a, d - b)
    return [(d - b) / l, -(c - a) / l] as [number, number]
  })
  const frames = path.map((p, i) => {
    const axis = unit(sub(path[(i + 1) % path.length], path[(i + path.length - 1) % path.length]))
    const u = unit(sub(p, CENTRE))
    const v = unit(cross(axis, u))
    return { p, u, v }
  })
  const at = (f: typeof frames[0], [a, b]: [number, number]) => add(f.p, add(mul(f.u, a), mul(f.v, b)))
  const dir = (f: typeof frames[0], [a, b]: [number, number]) => unit(add(mul(f.u, a), mul(f.v, b)))
  for (let i = 0; i < frames.length; i++) {
    const f = frames[i], g = frames[(i + 1) % frames.length]
    for (let k = 0; k < m; k++) {
      const p0 = profile[k], p1 = profile[(k + 1) % m]
      const nf = dir(f, facets[k]), ng = dir(g, facets[k])
      const a = at(f, p0), b = at(f, p1), c = at(g, p1), d = at(g, p0)
      face(part, [a, b, c], [nf, nf, ng])
      face(part, [a, c, d], [nf, ng, ng])
    }
  }
}

// The grid is flat steel strap: a broad outer face with a keel behind it,
// so it reads as a bold band from outside and still has depth on the far
// side seen through the openings. Twelve meridians and seven parallels keep
// big quiet cells that survive at phone size.
const STRAP: [number, number][] = [[0.06, -0.3], [0.06, 0.3], [-0.5, 0]]
const EQUATOR: [number, number][] = [[0.08, -0.36], [0.08, 0.36], [-0.52, 0]]
for (let lon = 0; lon < 180; lon += 30) {
  const a = lon * RAD
  sweep(grid, Array.from({ length: 28 }, (_, i) => {
    const t = i / 28 * TAU
    return add(CENTRE, tilt([RADIUS * Math.cos(t) * Math.sin(a),
      -RADIUS * Math.cos(t) * Math.cos(a), RADIUS * Math.sin(t)]))
  }), STRAP)
}
for (let lat = -60; lat <= 60; lat += 20) {
  const steps = Math.max(14, Math.round(28 * Math.cos(lat * RAD) / 2) * 2)
  sweep(grid, Array.from({ length: steps }, (_, i) => globe([i / steps * 360, lat])), lat === 0 ? EQUATOR : STRAP)
}
// A small plate where the meridians meet at the north pole, so twelve straps
// don't converge into a spiky star. Antarctica already covers the south pole.
{
  const rim = Array.from({ length: 12 }, (_, i) => globe([i * 30 + 15, 80], RADIUS + 0.08))
  const lip = Array.from({ length: 12 }, (_, i) => globe([i * 30 + 15, 80], RADIUS - 0.3))
  const pole = globe([0, 90], RADIUS + 0.08), up = unit(sub(pole, CENTRE))
  for (let i = 0; i < 12; i++) {
    const j = (i + 1) % 12
    face(land, [pole, rim[i], rim[j]], [up, unit(sub(rim[i], CENTRE)), unit(sub(rim[j], CENTRE))])
    const n = unit(sub(add(rim[i], rim[j]), add(pole, pole)))
    face(land, [lip[i], lip[j], rim[j]], [n, n, n])
    face(land, [lip[i], rim[j], rim[i]], [n, n, n])
  }
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
  borneo: [[109,1],[110,5],[117,7],[119,1],[116,-4],[111,-3]],
  newGuinea: [[131,-1],[140,-3],[150,-6],[150,-10],[141,-9],[135,-5]],
  newZealand: [[173,-35],[178,-38],[176,-41],[172,-42],[168,-46],[166,-45],[170,-41],[174,-39]],
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

// Plates stand proud of the straps: the top surface at PLATE, a wall down
// to SKIRT, which sits just above the straps' outer face.
const PLATE = RADIUS + 0.31
const SKIRT = RADIUS - 0.04
const outward = (p: V3) => unit(sub(p, CENTRE))

// Subdivide in geographic space: a single triangle fan would cut through
// the globe and turn the large continents into flat slabs.
function panel(a: Geo, b: Geo, c: Geo) {
  const pairs = [[a, b, c], [b, c, a], [c, a, b]]
  const distance = (p: Geo, q: Geo) => Math.hypot((p[0] - q[0]) * Math.cos((p[1] + q[1]) / 2 * RAD), p[1] - q[1])
  pairs.sort((p, q) => distance(q[0], q[1]) - distance(p[0], p[1]))
  const [p, q, r] = pairs[0]
  if (distance(p, q) > 14) {
    const mid: Geo = [(p[0] + q[0]) / 2, (p[1] + q[1]) / 2]
    panel(p, mid, r); panel(mid, q, r)
    return
  }
  const vertices = [a, b, c].map(p => globe(p, PLATE))
  face(land, vertices, vertices.map(outward))
}

/** The plate's edge wall, facing out of the continent and slightly upward. */
function edge(a: Geo, b: Geo, inside: Geo) {
  const top = [globe(a, PLATE), globe(b, PLATE)], foot = [globe(a, SKIRT), globe(b, SKIRT)]
  const along = sub(top[1], top[0])
  let n = unit(cross(along, outward(top[0])))
  if (dot(n, sub(globe(inside, PLATE), top[0])) > 0) n = mul(n, -1)
  const nn = n
  face(land, [foot[0], foot[1], top[1]], [nn, nn, nn])
  face(land, [foot[0], top[1], top[0]], [nn, nn, nn])
}

for (const outline of Object.values(continents)) {
  for (const [a, b, c] of triangulate(outline)) panel(a, b, c)
  const centroid: Geo = [outline.reduce((s, p) => s + p[0], 0) / outline.length,
    outline.reduce((s, p) => s + p[1], 0) / outline.length]
  for (let i = 0; i < outline.length; i++) {
    const a = outline[i], b = outline[(i + 1) % outline.length]
    const steps = Math.ceil(Math.hypot(a[0] - b[0], a[1] - b[1]) / 9)
    for (let j = 0; j < steps; j++) {
      const at = (t: number): Geo => [a[0] + (b[0] - a[0]) * t, a[1] + (b[1] - a[1]) * t]
      // The inward side is found locally: a point nudged off the edge's
      // midpoint, tested against the outline's winding.
      const p = at(j / steps), q = at((j + 1) / steps)
      const m: Geo = [(p[0] + q[0]) / 2, (p[1] + q[1]) / 2]
      const left: Geo = [m[0] - (q[1] - p[1]) * 0.05, m[1] + (q[0] - p[0]) * 0.05]
      edge(p, q, inPolygon(left, outline) ? left : centroid)
    }
  }
}
function inPolygon([x, y]: Geo, polygon: Geo[]) {
  let hit = false
  for (let i = 0, j = polygon.length - 1; i < polygon.length; j = i++) {
    const a = polygon[i], b = polygon[j]
    if ((a[1] > y) !== (b[1] > y) && x < (b[0] - a[0]) * (y - a[1]) / (b[1] - a[1]) + a[0]) hit = !hit
  }
  return hit
}

// Antarctica is a polar cap, not a longitude/latitude polygon crossing a
// seam. Its peninsula reaches toward South America; the rest stays compact.
const antarcticCoast = [-70, -70, -69, -68, -67, -66, -67, -67, -68, -69, -71, -75,
  -78, -77, -75, -73, -72, -73, -71, -65, -63, -69, -73, -72]
for (let i = 0; i < antarcticCoast.length; i += 2) {
  const a: Geo = [i * 15, antarcticCoast[i]]
  const b: Geo = [(i + 2) * 15, antarcticCoast[(i + 2) % antarcticCoast.length]]
  const pole: Geo = [i * 15 + 15, -90]
  const mid: Geo = [i * 15 + 15, antarcticCoast[i + 1]]
  panel(pole, mid, a); panel(pole, b, mid)
  edge(a, mid, pole); edge(mid, b, pole)
}

// Three orbits at different inclinations and nodes. Each ring is a flat band
// with chamfered edges, so it catches a highlight along its rim.
const RING: [number, number][] = [[0.38, -0.08], [0.38, 0.08], [0.26, 0.2], [-0.26, 0.2],
  [-0.38, 0.08], [-0.38, -0.08], [-0.26, -0.2], [0.26, -0.2]]
for (const [inclination, node, radius] of [[32, -30, 19.95], [49, 20, 20.206], [63, 66, 20.333]]) {
  const i = inclination * RAD, n = node * RAD
  const u: V3 = [Math.cos(n), Math.sin(n), 0]
  const v: V3 = [-Math.sin(n) * Math.cos(i), Math.cos(n) * Math.cos(i), Math.sin(i)]
  sweep(orbits, Array.from({ length: 36 }, (_, k) => {
    const a = k / 36 * TAU
    return add(CENTRE, add(mul(u, radius * Math.cos(a)), mul(v, radius * Math.sin(a))))
  }), RING)
}

// The base: three chamfered legs splay from a hub to the ground, and a
// sixteen-sided cone flares from the hub up into the globe's underside.
function ring(r: number, z: number, sides: number, phase = 0): V3[] {
  return Array.from({ length: sides }, (_, j) => {
    const a = (j + phase) / sides * TAU
    return [r * Math.cos(a), r * Math.sin(a), z] as V3
  })
}
function revolve(part: Part, profile: [number, number][], sides: number) {
  const rings = profile.map(([r, z]) => ring(r, z, sides))
  for (let k = 0; k < rings.length - 1; k++) {
    const [r0, z0] = profile[k], [r1, z1] = profile[k + 1]
    const slope = Math.atan2(r0 - r1, z1 - z0)
    for (let j = 0; j < sides; j++) {
      const l = (j + 1) % sides
      const nj = (i: number): V3 => {
        const a = i / sides * TAU
        return [Math.cos(a) * Math.cos(slope), Math.sin(a) * Math.cos(slope), Math.sin(slope)]
      }
      face(part, [rings[k][j], rings[k][l], rings[k + 1][l]], [nj(j), nj(l), nj(l)])
      face(part, [rings[k][j], rings[k + 1][l], rings[k + 1][j]], [nj(j), nj(l), nj(j)])
    }
  }
  const top = rings[rings.length - 1]
  for (let j = 1; j < sides - 1; j++) face(part, [top[0], top[j], top[j + 1]], [[0, 0, 1], [0, 0, 1], [0, 0, 1]])
}
revolve(pedestal, [[1.9, 0], [1.75, 1.6], [1.85, 3.2], [2.5, 5.2], [3.4, 7.0]], 16)
const legSection = (w: number, d: number, c: number): [number, number][] =>
  [[-w + c, -d], [w - c, -d], [w, -d + c], [w, d - c], [w - c, d], [-w + c, d], [-w, d - c], [-w, -d + c]]
for (let leg = 0; leg < 3; leg++) {
  const angle = (leg * 120 + 30) * RAD
  const radial: V3 = [Math.cos(angle), Math.sin(angle), 0], side: V3 = [-Math.sin(angle), Math.cos(angle), 0]
  const rings = [[6.0, 0, 1.3, 0.8], [5.7, 0.7, 1.25, 0.8], [3.7, 3.2, 1.1, 0.9], [1.6, 5.6, 0.95, 1.0]].map(([r, z, w, d]) =>
    legSection(w, d, 0.3).map(([s, t]): V3 => add([0, 0, z], add(mul(radial, r + t), mul(side, s)))))
  pedestal.loft(rings)
  pedestal.cap(rings[rings.length - 1], true)
}

const parts = [
  // Mid-grey brushed steel, taken from the shaded half of photo 02 rather
  // than its specular glints; the continents a step lighter, the base the
  // dark painted steel of the real pedestal.
  { part: grid, material: { name: 'steel-grid', color: 0x7f868e, roughness: 0.55, doubleSided: true } },
  { part: land, material: { name: 'steel-continents', color: 0xbcc0c5, roughness: 0.6, doubleSided: true } },
  { part: orbits, material: { name: 'steel-orbits', color: 0xaab0b7, roughness: 0.45, doubleSided: true } },
  { part: pedestal, material: { name: 'steel-base', color: 0x50565f, roughness: 0.7 } },
]
const triangles = parts.reduce((n, { part }) => n + part.triangles, 0)
if (triangles > 6500) throw new Error(`Triangle budget exceeded: ${triangles}`)
const glb = writeGlb('Unisphere', parts, {
  license: 'CC0-1.0', frame: 'Y up, -Z north, +X east, metres, origin at ground',
  globeDiameter: 36.6, axialTilt: 23.5, orbitalRings: 3, bearing: 0, elevation: 0,
})
await Bun.write(new URL('../../landmarks/models/unisphere.glb', import.meta.url), glb)
console.log(`unisphere.glb: ${triangles} triangles, ${glb.length} bytes`)
console.log(parts.map(({ part, material }) => `${material.name}: ${part.triangles}`).join('\n'))
