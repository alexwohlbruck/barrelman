/**
 * Soldiers' and Sailors' Memorial Arch, Brooklyn. Procedural CC0 geometry.
 * bun scripts/landmarks/soldiers-sailors-arch.ts
 * Map frame: x = v (passage / south), y = u (long facade), z = up.
 * Placement: 40.6729891, -73.9699033; bearing 81; elevation 0.
 * OSM controls the masonry envelope; sculpture is deliberately simplified.
 * Reference: https://s-media.nyc.gov/agencies/lpc/lp/0821.pdf
 */
import { Part, addGltfTriangles, cross, sub, len, writeGlb, type V3 } from './mesh'

const stone = new Part(), trim = new Part(), recess = new Part(), bronze = new Part()
const TAU = Math.PI * 2
const unit = (v: V3): V3 => v.map(n => n / (len(v) || 1)) as V3
const add = (a: V3, b: V3): V3 => a.map((n, i) => n + b[i]) as V3
const mul = (a: V3, s: number): V3 => a.map(n => n * s) as V3
function solid(p: Part, rings: V3[][]) {
  p.loft(rings); p.cap(rings[0], false); p.cap(rings.at(-1)!, true)
}
function box(p: Part, x0: number, x1: number, y0: number, y1: number, z0: number, z1: number) {
  solid(p, [z0, z1].map(z => [[x0, y0, z], [x1, y0, z], [x1, y1, z], [x0, y1, z]]))
}
function smooth(p: Part, build: (q: Part) => void, crease = 65) {
  const q = new Part(); build(q)
  addGltfTriangles(p, new Float32Array(q.pos), Uint32Array.from({ length: q.pos.length / 3 }, (_, i) => i), { creaseDegrees: crease })
}
function oval(c: V3, rx: number, ry: number, n = 10): V3[] {
  return Array.from({ length: n }, (_, i) => [c[0] + rx * Math.cos(i * TAU / n), c[1] + ry * Math.sin(i * TAU / n), c[2]])
}
/** Pole fans avoid the zero-area triangles of collapsed loft rings. */
function ellipsoid(p: Part, c: V3, r: V3, n = 8, rows = 4) {
  const point = (a: number, b: number): V3 => [c[0] + r[0] * Math.cos(b) * Math.cos(a), c[1] + r[1] * Math.cos(b) * Math.sin(a), c[2] + r[2] * Math.sin(b)]
  const normal = (v: V3): V3 => unit(v.map((t, i) => (t - c[i]) / (r[i] * r[i])) as V3)
  const tri = (a: V3, b: V3, d: V3) => p.tri(a, b, d, undefined, undefined, undefined, [normal(a), normal(b), normal(d)])
  for (let j = 0; j < rows; j++) for (let i = 0; i < n; i++) {
    const a = i * TAU / n, b = (i + 1) * TAU / n, lo = -Math.PI / 2 + j * Math.PI / rows, hi = lo + Math.PI / rows
    const v0 = point(a, lo), v1 = point(b, lo), v2 = point(b, hi), v3 = point(a, hi)
    if (j > 0) tri(v0, v1, v2)
    if (j < rows - 1) tri(v0, v2, v3)
  }
}
function tube(p: Part, path: V3[], radii: number[], sides = 6) {
  smooth(p, q => solid(q, path.map((c, i) => {
    const axis = unit(sub(path[Math.min(i + 1, path.length - 1)], path[Math.max(0, i - 1)]))
    const u = unit(cross(Math.abs(axis[2]) > 0.9 ? [0, 1, 0] : [0, 0, 1], axis)), v = cross(axis, u)
    return Array.from({ length: sides }, (_, j) => add(c, add(mul(u, radii[i] * Math.cos(j * TAU / sides)), mul(v, radii[i] * Math.sin(j * TAU / sides)))))
  })))
}
function column(x: number, y: number) {
  box(trim, x - .65, x + .65, y - .65, y + .65, .65, 1.1)
  smooth(stone, q => solid(q, [[1.1, .54], [1.45, .55], [1.65, .43], [8.85, .38], [9.12, .5], [9.5, .64]].map(([z, r]) => oval([x, y, z], r, r, 10))), 48)
  box(trim, x - .67, x + .67, y - .67, y + .67, 9.5, 9.85)
  // Four broad leaf lobes suggest the Composite capitals without tiny foliage.
  for (const dy of [-.4, .4]) ellipsoid(trim, [x + Math.sign(x) * .32, y + dy, 9.25], [.22, .24, .3], 6, 3)
}

// The asymmetric 23.5 m OSM envelope is intentional: origin is the catalog anchor.
for (const [y0, y1] of [[-11.7, -5.5], [5.5, 11.8]]) {
  box(stone, -3.8, 3.8, y0, y1, .52, 18.4)
  box(recess, -3.8, 3.8, y0, y1, 0, .52)
  for (const side of [-1, 1]) {
    const ym = (y0 + y1) / 2
    // Broad rustication on the unadorned ends of the piers.
    for (const z of [2.7, 5.1, 7.5, 11.3, 14.0, 16.7]) {
      box(trim, -3.8, 3.8, side < 0 ? y0 : y1 - .045, side < 0 ? y0 + .045 : y1, z, z + .065)
    }
    const xa = side < 0 ? -5.4 : 3.8, xb = side < 0 ? -3.8 : 5.4
    const a = ym < 0 ? -11.2 : 7.1, b = ym < 0 ? -7.1 : 11.2
    box(trim, xa, xb, a, b, 0, .65)
    box(stone, side < 0 ? -4.65 : 3.8, side < 0 ? -3.8 : 4.65, a + .15, b - .15, .65, 10)
    column(side * 4.65, a + .7); column(side * 4.65, b - .7)
    box(trim, xa, xb, a, b, 9.85, 10.25)
    box(stone, xa + .15, xb - .15, a + .15, b - .15, 10.25, 10.7)
    box(trim, xa, xb, a, b, 10.7, 11)
    for (const z of [3.7, 6.25, 8.2]) ellipsoid(trim, [side * 4.69, (a + b) / 2, z], [.095, .38, .4], 6, 3)
  }
}

// A genuine circular segment meets all three OSM constraints: half-span 5.5,
// spring 12 and crown 15. A semicircle with this width would crown at 17.5.
const radius = (5.5 * 5.5 + 3 * 3) / 6
const centerZ = 15 - radius
const archZ = (y: number) => centerZ + Math.sqrt(radius * radius - y * y)
const steps = 28
for (let i = 0; i < steps; i++) {
  const a = -5.5 + 11 * i / steps, b = -5.5 + 11 * (i + 1) / steps
  const za = archZ(a), zb = archZ(b)
  // Both recessed facades and the downward-facing tunnel soffit.
  stone.quad([2.65, a, za], [2.65, b, zb], [2.65, b, 18.4], [2.65, a, 18.4])
  stone.quad([-2.65, b, zb], [-2.65, a, za], [-2.65, a, 18.4], [-2.65, b, 18.4])
  stone.quad([-2.65, a, za], [-2.65, b, zb], [2.65, b, zb], [2.65, a, za])
  for (const s of [-1, 1]) {
    const q = new Part(), x = s * 2.76
    q.quad([x, a, za + .025], [x, b, zb + .025], [x, b, zb + .66], [x, a, za + .66])
    if (s < 0) {
      for (let k = 0; k < q.pos.length; k += 9) {
        const v = [0, 1, 2].map(j => [q.pos[k + j * 3], -q.pos[k + j * 3 + 2], q.pos[k + j * 3 + 1]] as V3)
        trim.tri(v[2], v[1], v[0])
      }
    } else addGltfTriangles(trim, new Float32Array(q.pos), Uint32Array.from({ length: 6 }, (_, k) => k))
  }
}
for (const s of [-1, 1]) {
  box(trim, s * 2.65 - .15, s * 2.65 + .15, -.38, .38, 14.8, 16.3)
  for (const y of [-5.5, 5.5]) box(trim, -2.8, 2.8, y - .16, y + .16, 11.65, 12)
}
// Entablature, quiet tall attic, and stepped granite crown, all below 24 m.
for (const [z0, z1, d, y0, y1, p] of [
  [18.4, 18.8, 3.9, -11.7, 11.8, trim], [18.8, 19.45, 3.65, -11.5, 11.6, stone],
  [19.45, 19.85, 4.15, -11.7, 11.8, trim], [19.85, 23.3, 3.45, -11.4, 11.5, stone],
  [23.3, 23.62, 3.75, -11.65, 11.75, trim], [23.62, 24, 3.9, -11.7, 11.8, trim],
] as [number, number, number, number, number, Part][]) box(p, -d, d, y0, y1, z0, z1)
for (const s of [-1, 1]) {
  box(recess, s * 3.46 - .018, s * 3.46 + .018, -7.0, 7.1, 20.65, 22.5)
  box(stone, s * 3.5 - .02, s * 3.5 + .02, -6.83, 6.93, 20.8, 22.35)
}


// Shallow, deliberately broad engraved letter strokes stay readable at map size.
// Each pair of coordinates is a pen stroke in a one-unit square.
const letters: Record<string, number[][]> = {
  T: [[0,1,1,1],[.5,1,.5,0]], H: [[0,0,0,1],[1,0,1,1],[0,.5,1,.5]],
  E: [[0,0,0,1],[0,1,1,1],[0,.5,.8,.5],[0,0,1,0]],
  D: [[0,0,0,1],[0,1,.7,1],[.7,1,1,.75],[1,.75,1,.25],[1,.25,.7,0],[.7,0,0,0]],
  F: [[0,0,0,1],[0,1,1,1],[0,.5,.8,.5]], N: [[0,0,0,1],[0,1,1,0],[1,0,1,1]],
  R: [[0,0,0,1],[0,1,1,1],[1,1,1,.5],[1,.5,0,.5],[.4,.5,1,0]],
  S: [[1,1,0,1],[0,1,0,.5],[0,.5,1,.5],[1,.5,1,0],[1,0,0,0]],
  O: [[0,0,0,1],[0,1,1,1],[1,1,1,0],[1,0,0,0]],
  U: [[0,1,0,0],[0,0,1,0],[1,0,1,1]], I: [[.5,0,.5,1]],
  '1': [[.1,.75,.5,1],[.5,1,.5,0],[0,0,1,0]],
  '8': [[0,0,0,1],[0,1,1,1],[1,1,1,0],[1,0,0,0],[0,.5,1,.5]],
  '6': [[0,0,0,1],[0,1,1,1],[0,.5,1,.5],[1,.5,1,0],[1,0,0,0]],
  '5': [[1,1,0,1],[0,1,0,.5],[0,.5,1,.5],[1,.5,1,0],[1,0,0,0]],
  '-': [[0,.5,1,.5]],
}
function inscription(text: string, z: number, h: number, s: number) {
  const advance = h * .84, width = h * .57
  for (let i = 0; i < text.length; i++) for (const [a,b,c,d] of letters[text[i]] ?? []) {
    const u0 = (i - text.length / 2) * advance + a * width, u1 = (i - text.length / 2) * advance + c * width
    const z0 = z + b * h, z1 = z + d * h, dy = u1 - u0, dz = z1 - z0, length = Math.hypot(dy, dz)
    const oy = -dz / length * .032, oz = dy / length * .032
    const point = (y: number, z: number): V3 => [s * 3.526, s * y, z]
    recess.quad(point(u0 - oy, z0 - oz), point(u1 - oy, z1 - oz), point(u1 + oy, z1 + oz), point(u0 + oy, z0 + oz))
  }
}
for (const s of [-1, 1]) {
  inscription('TO THE', 21.94, .28, s)
  inscription('DEFENDERS OF THE UNION', 21.35, .48, s)
  inscription('1861-1865', 20.93, .28, s)
}

/** Thin sculpted wing in the yz plane; ridge and scalloped feather edge. */
function wing(p: Part, c: V3, sign: number, scale = 1) {
  const outline = [[0, 0], [.6, .85], [1.6, 1.7], [2.3, 1.85], [2.03, 1.25], [1.78, 1.12], [1.52, .68], [1.2, .61], [.85, .2], [.35, -.1]]
  const front: V3 = [c[0] + .16 * scale, c[1] + .75 * sign * scale, c[2] + .7 * scale]
  const back: V3 = [c[0] - .12 * scale, front[1], front[2]]
  const pts: V3[] = outline.map(([y, z]) => [c[0], c[1] + y * sign * scale, c[2] + z * scale])
  for (let i = 0; i < pts.length; i++) {
    const a = pts[i], b = pts[(i + 1) % pts.length]
    if (sign > 0) { p.tri(front, a, b); p.tri(back, b, a) }
    else { p.tri(front, b, a); p.tri(back, a, b) }
  }
}
/** Robed bronze figure, facing +x. Height excludes raised arm. */
function person(c: V3, h: number, pose: 'raised' | 'reins' | 'point', depth = 1) {
  const at = (x: number, y: number, z: number): V3 => [c[0] + x * h * depth, c[1] + y * h, c[2] + z * h]
  smooth(bronze, p => solid(p, [[0, .2, .24], [.35, .13, .15], [.65, .12, .19], [.8, .105, .18]].map(([z, rx, ry]) => oval(at(0, 0, z), h * rx * depth, h * ry, 6).map((v, i) => [v[0] + (i % 2 ? .035 : -.035), v[1], v[2]] as V3))))
  ellipsoid(bronze, at(.015, 0, .91), [h * .105 * depth, h * .11, h * .135], 6, 4)
  for (const s of [-1, 1]) {
    const end = pose === 'raised' && s > 0 ? at(.03, s * .31, 1.32) : pose === 'point' && s > 0 ? at(.27, s * .36, .84) : at(.28, s * .25, .58)
    tube(bronze, [at(0, s * .16, .73), at(.12, s * .25, .64 + (pose === 'raised' && s > 0 ? .34 : 0)), end], [h * .085, h * .064, h * .04], 5)
  }
}

// Quadriga: four independently legged horses face the park (+x), pulling a
// chariot behind them. Columbia rises above two winged Victories.
box(bronze, -2.7, 2.7, -5.1, 5.1, 24, 24.45)
box(bronze, -2.5, 2.65, -4.9, 4.9, 24.45, 24.67)
function horse(y: number, phase: number) {
  const first = bronze.pos.length
  ellipsoid(bronze, [.55, y, 27.05], [1.38, .47, .65], 10, 5)
  tube(bronze, [[1.35, y, 27.15], [1.73, y, 28.1], [1.83, y, 28.95]], [.53, .44, .3], 8)
  ellipsoid(bronze, [2.08, y, 28.92], [.58, .3, .36], 8, 4)
  ellipsoid(bronze, [2.48, y, 28.7], [.36, .24, .25], 8, 3)
  for (const s of [-1, 1]) {
    tube(bronze, [[1.81, y + s * .19, 29.12], [1.73, y + s * .24, 29.62]], [.12, .025], 5)
    tube(bronze, [[-.35, y + s * .3, 26.9], [-.62, y + s * .32, 25.77], [-.34, y + s * .34, 24.77]], [.22, .13, .13], 6)
    const lift = s === phase ? .65 : 0
    tube(bronze, [[1.2, y + s * .3, 26.95], [1.68 + lift, y + s * .34, 25.83 + lift], [1.32 + lift, y + s * .34, 24.8 + lift]], [.2, .12, .12], 6)
  }
  tube(bronze, [[-.7, y, 27.2], [-1.25, y, 26.6], [-1.38, y + .16, 25.5]], [.19, .21, .08], 5)
  // A raised mane catches light; slight splaying distinguishes all four horses.
  tube(bronze, [[1.15, y, 27.6], [1.52, y, 29.05]], [.15, .12], 4)
  const angle = y * .055, co = Math.cos(angle), si = Math.sin(angle)
  for (let k = first; k < bronze.pos.length; k += 3) {
    const x = bronze.pos[k], localY = -bronze.pos[k + 2] - y
    bronze.pos[k] = x * co - localY * si
    bronze.pos[k + 2] = -(y + x * si + localY * co)
    const nx = bronze.nrm[k], ny = -bronze.nrm[k + 2]
    bronze.nrm[k] = nx * co - ny * si
    bronze.nrm[k + 2] = -(nx * si + ny * co)
  }
  tube(bronze, [[-1.0, y * .42, 28.5], [.25, y * .83, 28.2], [2.24, y + 2.24 * si, 28.78]], [.038, .038, .035], 4)
}
for (const [i, y] of [-2.45, -.82, .82, 2.45].entries()) horse(y, i % 2 ? 1 : -1)
box(bronze, -2.2, -.65, -1.15, 1.15, 25.3, 26.2)
// Curved chariot breastplate.
smooth(bronze, p => solid(p, [oval([-1.1, 0, 25.7], .85, 1.05, 10), oval([-1.1, 0, 27.05], 1.05, 1.25, 10)]))
function wheel(c: V3, r: number) {
  // A real open rim, axis y, with four spokes.
  const n = 12, m = 4
  const pt = (a: number, b: number): V3 => [c[0] + (r + .10 * Math.cos(b)) * Math.cos(a), c[1] + .1 * Math.sin(b), c[2] + (r + .10 * Math.cos(b)) * Math.sin(a)]
  smooth(bronze, p => { for (let i = 0; i < n; i++) for (let j = 0; j < m; j++) p.quad(pt(i * TAU / n, j * TAU / m), pt(i * TAU / n, (j + 1) * TAU / m), pt((i + 1) * TAU / n, (j + 1) * TAU / m), pt((i + 1) * TAU / n, j * TAU / m)) })
  for (let i = 0; i < 4; i++) tube(bronze, [c, [c[0] + r * Math.cos(i * TAU / 4), c[1], c[2] + r * Math.sin(i * TAU / 4)]], [.08, .07], 4)
}
wheel([-1.35, -1.22, 25.7], .9); wheel([-1.35, 1.22, 25.7], .9)
person([-1.3, 0, 26.25], 4.75, 'raised', .85)
for (const s of [-1, 1]) {
  person([-.1, s * 3.6, 24.67], 3.75, 'reins', .8)
  wing(bronze, [-.42, s * 3.65, 27.1], s, .95)
  wing(bronze, [-.65, s * 3.6, 27.1], -s, .55)
}

// The Army and Navy groups sit on the SOUTH pedestals only. Broad overlapping
// figures read as a sculptural group rather than a row of isolated figurines.
for (const s of [-1, 1]) {
  const y = s * 9.15
  box(bronze, 3.82, 5.25, y - 1.65, y + 1.65, 11, 11.35)
  person([4.24, y, 11.35], 4.7, 'raised', .48)
  person([4.55, y - 1.12, 11.35], 2.95, 'point', .42)
  person([4.52, y + 1.06, 11.35], 3.15, 'reins', .42)
  // Staff/standard on Army; broken mast on Navy.
  tube(bronze, [[4.05, y - .65, 11.5], [4.05, y - .65, s < 0 ? 17.1 : 15.8]], [.075, .055], 5)
}
// Winged spandrel relief on the south, seal-like medallions on the north.
for (const s of [-1, 1]) {
  ellipsoid(trim, [2.84, s * 4.15, 16.55], [.17, .33, .62], 8, 4)
  wing(trim, [2.83, s * 4.05, 16.15], -s, .6)
  ellipsoid(trim, [-2.8, s * 4.15, 16.6], [.15, .68, .72], 10, 4)
}

const parts = [
  { part: stone, material: { name: 'stone', color: 0xd6cebb } },
  { part: trim, material: { name: 'stone-carving', color: 0xe3dccb } },
  { part: recess, material: { name: 'stone-recess', color: 0xa59e8b } },
  { part: bronze, material: { name: 'bronze', color: 0x435b50, roughness: .78 } },
]
const triangles = parts.reduce((n, { part }) => n + part.triangles, 0)
if (triangles > 7000) throw new Error(`Triangle budget exceeded: ${triangles}`)
const glb = writeGlb("Soldiers' and Sailors' Memorial Arch", parts, {
  license: 'CC0-1.0', bearing: 81, elevation: 0,
  frame: 'Y up, -Z north, +X east, metres; ground anchor; map x=v, y=u',
  masonryHeight: 24, archSpan: 11, archSpring: 12, archCrown: 15,
  source: 'OSM envelope supplied for way/20679503 and parts; NYC LPC LP-0821',
})
const out = new URL('../../landmarks/models/soldiers-sailors-arch.glb', import.meta.url).pathname
await Bun.write(out, glb)
console.log(`${out}: ${triangles} triangles, ${glb.length} bytes (${(glb.length / 1024).toFixed(1)} KiB)`)
