/**
 * Expedition Everest – Legend of the Forbidden Mountain, Disney's Animal
 * Kingdom — procedural, CC0-1.0.
 *   bun scripts/landmarks/wdw-expedition-everest.ts
 *
 * Map frame: x east, y north, z up, metres. Geometry is written in a park
 * frame about (-81.5866, 28.3588) and shifted so the origin is the centroid
 * of the mountain's OSM outline; the script prints that anchor. Bearing 0.
 *
 * What the model is: the Forbidden Mountain (a rock-and-snow heightfield over
 * the show building's outline, three peaks), the temple tower the lift hill
 * runs through, the station village as one low block, and the coaster track
 * wherever it runs in the open, with chunky dark supports.
 *
 * Evidence
 * - OSM (measured): the mountain's outline is the show building
 *   way/146269361 (80 × 74 m); the temple way/146269359; the station
 *   relation/3937134 (outer way/296055418, a courtyard as its inner); the
 *   summit node/12797880133 ("Mount Everest", ele 60); every track way, with
 *   `covered` marking the stretches inside the mountain; cave_entrance nodes
 *   where the track goes in and out, most of them vertices of the outline.
 * - USGS NAIP orthophoto (public domain), checked against OSM: the outline,
 *   the white snowfields over its west and middle, the flat grey show-building
 *   roof behind (south-east), and both open-air track rings (the outer loop
 *   west of the mountain, the helix north of it) where OSM draws them.
 * - Published: mountain 199.5 ft (61 m); lift 112 ft (34 m); drop 80 ft;
 *   track 4,424 ft (Wikipedia, RCDB).
 * - Photos (Wikimedia Commons; credits in the placement report), mostly from
 *   across the river to the south-west: "Expedition Everest.jpg" (bdesham,
 *   CC BY-SA 4.0) for the massing: the spire, a lower snowy peak to its
 *   north-west and a brown craggy one to its south-east, the rust-brown rock
 *   streaked with snow, the cliffs at its foot, the dark lift climbing from
 *   the red temple tower into the mountain's west face. Its projection checks
 *   the summit node: the spire stands where OSM puts it.
 * - Estimated: the two lesser peaks' positions and heights (45 and 46 m, from
 *   that photo's projection), the cliff heights (about 20 to 28 m), every
 *   track height except the lift's, the temple (24 m tower on a 10 m body),
 *   the village's 7.5 m. The back of the mountain (the show building, seen in
 *   no licensed photo) is drawn as a lower rock plateau.
 * - The coaster is drawn here rather than with coaster-kit.ts: the ride runs
 *   forward, backward and forward again through two switches, so it is not
 *   one circuit, and most of it is inside the mountain. The visible stretches
 *   are swept straight from the OSM ways with heights set by hand.
 */
import { Part, addGltfTriangles, cross, len, sub, writeGlb, type V3 } from './mesh'
import { PALETTE, finish } from './palette'

type XY = [number, number]
const TAU = Math.PI * 2
const unit = (v: V3): V3 => { const l = len(v) || 1; return [v[0] / l, v[1] / l, v[2] / l] }
const plus = (a: V3, b: V3): V3 => [a[0] + b[0], a[1] + b[1], a[2] + b[2]]
const scale = (a: V3, k: number): V3 => [a[0] * k, a[1] * k, a[2] * k]
const clamp = (x: number, a = 0, b = 1) => Math.min(b, Math.max(a, x))
const smoothstep = (a: number, b: number, x: number) => { const t = clamp((x - a) / (b - a)); return t * t * (3 - 2 * t) }
const hash2 = (i: number, j: number) => { const s = Math.sin(i * 127.1 + j * 311.7) * 43758.5453; return s - Math.floor(s) }
/** Smooth value noise in [0, 1). */
function noise(x: number, y: number) {
  const i = Math.floor(x), j = Math.floor(y), u = x - i, v = y - j
  const su = u * u * (3 - 2 * u), sv = v * v * (3 - 2 * v)
  const a = hash2(i, j), b = hash2(i + 1, j), c = hash2(i, j + 1), d = hash2(i + 1, j + 1)
  return (a * (1 - su) + b * su) * (1 - sv) + (c * (1 - su) + d * su) * sv
}

const LAT0 = 28.3588, LON0 = -81.5866
const MX = 111320 * Math.cos((LAT0 * Math.PI) / 180), MY = 110574

// ------------------------------------------------------------------- OSM

/** way/146269361, the show building: the mountain's outline (park frame). */
const MOUNTAIN: XY[] = [
  [-1.5, -76.8], [-3.4, -81.6], [-5.5, -84.8], [-7.0, -87.2], [-7.0, -91.3], [-6.4, -93.2], [0.2, -94.8], [0.7, -91.4],
  [1.1, -89.3], [3.7, -88.9], [6.7, -90.5], [9.0, -94.4], [10.2, -97.3], [9.6, -99.1], [10.0, -100.7], [15.4, -102.9],
  [18.0, -105.8], [21.3, -106.3], [28.2, -108.1], [40.8, -101.6], [60.9, -90.0], [64.8, -87.9], [71.0, -80.8], [67.7, -79.0],
  [70.3, -75.6], [70.9, -56.5], [69.4, -55.0], [67.8, -54.5], [66.1, -51.1], [60.9, -46.8], [63.6, -45.1], [66.3, -41.3],
  [66.3, -38.0], [63.5, -35.8], [61.8, -35.6], [60.0, -35.4], [58.0, -34.0], [50.5, -35.7], [48.0, -37.9], [45.6, -42.4],
  [43.2, -42.3], [39.2, -42.2], [30.5, -42.4], [28.7, -43.9], [18.3, -44.7], [12.1, -42.3], [8.6, -41.1], [7.7, -42.5],
  [6.8, -43.9], [6.2, -44.9], [6.1, -45.4], [5.8, -46.4], [5.4, -47.7], [-1.8, -51.1], [-5.7, -53.7], [-6.8, -56.0],
  [-8.5, -59.9], [-9.3, -61.5], [-5.1, -63.2],
]
/** way/146269359, the temple the lift hill climbs through. */
const TEMPLE: XY[] = [
  [13.6, -19.1], [9.6, -19.7], [7.1, -20.0], [3.4, -2.1], [2.3, -2.4], [3.1, -17.1], [2.7, -25.2], [3.7, -29.4],
  [4.5, -32.7], [5.9, -33.6], [8.8, -35.8], [13.4, -34.7], [15.6, -32.9], [18.2, -30.8], [17.5, -26.8], [17.0, -23.4],
]
/** relation/3937134, the station: outer way/296055418 and its courtyard way/296055423. */
const STATION: XY[] = [
  [-24.7, -37.1], [-42.5, -25.0], [-44.6, -21.9], [-43.7, -21.9], [-47.7, 0.9], [-52.4, 0.1], [-55.0, -0.3], [-56.8, -0.6],
  [-57.2, 1.5], [-61.0, 0.9], [-66.1, 0.0], [-65.8, -1.8], [-72.4, -2.9], [-72.2, -4.0], [-78.8, -5.2], [-78.0, -9.4],
  [-82.3, -11.6], [-82.8, -10.5], [-93.2, -16.0], [-87.9, -25.9], [-77.3, -20.3], [-77.3, -23.6], [-78.6, -22.9], [-82.7, -30.8],
  [-71.8, -36.5], [-70.3, -33.7], [-67.9, -35.0], [-66.5, -31.8], [-62.5, -35.8], [-54.7, -28.4], [-52.1, -31.0], [-47.8, -30.7],
  [-31.0, -42.5], [-29.1, -43.5], [-26.7, -40.0],
]
const COURTYARD: XY[] = [
  [-71.0, -11.6], [-73.4, -12.0], [-71.4, -18.2], [-72.7, -18.6], [-72.8, -25.6], [-70.4, -26.9], [-69.4, -25.1], [-64.7, -27.7],
  [-66.1, -30.8], [-60.0, -24.9], [-61.2, -23.7], [-63.6, -24.0], [-65.1, -16.3], [-66.1, -16.5], [-66.4, -14.8], [-70.1, -15.6],
]

/**
 * The open-air track, as OSM ways in ride order, with the height of the
 * running rails above the ground at each node (estimated, except the lift's
 * ends, which the published lift height and the photo fix). Stretches OSM
 * marks `covered` are left out: they run inside the mountain, the temple or
 * the station.
 */
const RUNS: { name: string; pts: [number, number, number][] }[] = [
  // way/773718967: out of the station, round the north loop, back past the
  // temple and round again to the foot of the lift. At grade.
  { name: 'station run', pts: [
    [-52.4, 0.1, 1.2], [-51.2, 3.6, 1.2], [-50.2, 5.7, 1.2], [-48.9, 8.4, 1.2], [-45.3, 14.3, 1.2], [-42.8, 15.6, 1.2], [-39.0, 16.8, 1.2],
    [-35.1, 17.2, 1.2], [-32.2, 15.5, 1.2], [-30.1, 13.4, 1.2], [-27.9, 9.5, 1.2], [-13.7, -24.9, 1.2], [-10.8, -29.7, 1.2], [-7.8, -32.8, 1.2],
    [-4.2, -34.4, 1.2], [-0.3, -34.9, 1.2], [3.3, -34.8, 1.2], [5.9, -33.6, 1.2], [10.1, -31.9, 1.2], [13.3, -30.9, 1.2], [17.5, -26.8, 1.2],
    [19.4, -24.0, 1.2], [20.2, -19.3, 1.2], [21.8, 22.6, 1.2], [21.3, 25.1, 1.2], [20.4, 27.9, 1.2], [17.6, 31.0, 1.2], [14.8, 32.4, 1.2],
    [12.3, 32.9, 1.2], [9.4, 32.8, 1.2], [6.4, 32.0, 1.2], [4.1, 29.5, 1.2], [1.7, 26.2, 1.2], [1.3, 22.9, 1.4], [4.5, 6.0, 2.5],
  ] },
  // way/187631602 then way/1228688575: the lift, into the temple tower and
  // out of it, then up into the mountain's west face. 34 m at the top.
  { name: 'lift', pts: [[4.5, 6.0, 2.5], [9.6, -19.7, 10.5]] },
  { name: 'lift upper', pts: [[13.4, -34.7, 17], [15.1, -43.4, 21], [19.3, -64.1, 31]] },
  // ways/187631580, 187631574: over the shoulder at the top, to the switch.
  { name: 'summit run', pts: [[19.3, -64.1, 31], [20.5, -69.8, 33.5], [21.8, -76.2, 34], [23.0, -80.7, 33.8], [24.8, -83.8, 33.3], [27.4, -85.6, 32.8], [30.5, -85.7, 32.3], [32.7, -85.6, 32]] },
  // way/957958011: the broken track, out of the mountain's north-east face.
  { name: 'broken track', pts: [[49.5, -68.0, 37], [55.3, -59.3, 38], [61.1, -50.5, 39]] },
  // way/187631578: the 80 ft drop, out of the west face; way/187631576: the
  // outer turn round the south-west and back into the mountain.
  { name: 'drop', pts: [
    [5.6, -58.8, 27], [-6.8, -56.0, 21], [-10.4, -55.8, 14.5], [-15.3, -56.1, 6], [-19.7, -57.1, 3.2], [-23.8, -60.4, 2.6],
    [-25.5, -61.8, 2.6], [-27.1, -65.4, 2.8], [-28.8, -70.1, 3], [-28.8, -74.2, 3.2], [-26.9, -81.8, 3.4], [-23.9, -84.5, 3.4],
    [-20.6, -87.4, 3.4], [-14.5, -89.1, 3.4], [-8.6, -88.8, 3.4], [-3.2, -86.7, 3.4], [1.6, -82.4, 3.5], [4.2, -77.1, 3.6],
  ] },
  // way/1228688573: the helix north of the mountain, out of the north face
  // and spiralling down to the yeti tunnel; way/957958010 as far as its cover.
  { name: 'helix', pts: [
    [61.8, -35.6, 15], [63.7, -31.1, 14.6], [64.8, -26.7, 14.2], [64.1, -21.3, 13.8], [62.3, -17.0, 13.4], [57.9, -11.7, 12.9], [53.3, -8.2, 12.4],
    [47.9, -6.3, 11.9], [42.7, -6.6, 11.4], [38.4, -7.8, 10.9], [33.7, -10.3, 10.4], [29.3, -14.8, 9.9], [28.1, -22.8, 9.3], [29.0, -28.3, 8.8],
    [33.2, -33.9, 8.3], [37.2, -36.2, 7.9], [40.3, -36.6, 7.5], [45.3, -36.4, 7.1], [49.0, -35.6, 6.7], [54.1, -31.4, 6.3], [56.0, -26.6, 5.9],
    [56.6, -23.0, 5.6], [56.2, -19.9, 5.3], [54.6, -16.1, 5.0], [51.2, -12.7, 4.7], [46.6, -11.3, 4.4], [39.5, -13.1, 4.0], [35.1, -19.9, 3.5],
    [35.2, -27.1, 3.0], [37.2, -31.0, 2.8],
  ] },
  // way/1228688570: out of the yeti tunnel and back to the station.
  { name: 'return', pts: [[6.8, -43.9, 2.4], [2.1, -42.9, 2.0], [-4.6, -42.2, 1.6], [-17.6, -41.5, 1.3], [-24.0, -41.2, 1.2], [-26.7, -40.0, 1.2]] },
]
/** Where the open track meets a rock face: a dark tunnel mouth. [x, y, rail z, heading°] */
const PORTALS: [number, number, number, number?][] = [
  [18.4, -59.6, 29.2],   // lift into the west face
  [-6.8, -56.0, 21, 1.5], // the drop out of the west face: a tall dark mouth (photo e10)
  [-0.4, -79.3, 3.6],    // the outer turn back in
  [61.8, -35.6, 15],     // the helix out of the north face
  [6.8, -43.9, 2.4],     // out of the yeti tunnel
]

// ------------------------------------------------------------- the frame

const polyCentroid = (p: XY[]) => {
  let a = 0, cx = 0, cy = 0
  for (let i = 0; i < p.length; i++) {
    const [x0, y0] = p[i], [x1, y1] = p[(i + 1) % p.length], c = x0 * y1 - x1 * y0
    a += c; cx += (x0 + x1) * c; cy += (y0 + y1) * c
  }
  return [cx / (3 * a), cy / (3 * a)] as XY
}
const [AX, AY] = polyCentroid(MOUNTAIN)
const local = (p: XY): XY => [p[0] - AX, p[1] - AY]

const rock = new Part(), snow = new Part(), temple = new Part(), village = new Part(), roof = new Part(), track = new Part()

function smooth(target: Part, build: (p: Part) => void, crease = 40) {
  const p = new Part()
  build(p)
  addGltfTriangles(target, new Float32Array(p.pos), Uint32Array.from({ length: p.pos.length / 3 }, (_, i) => i), { creaseDegrees: crease })
}

// --------------------------------------------------------- the mountain

const area = (p: XY[]) => p.reduce((s, [x0, y0], i) => { const [x1, y1] = p[(i + 1) % p.length]; return s + x0 * y1 - x1 * y0 }, 0) / 2
const ccw = (p: XY[]) => (area(p) < 0 ? [...p].reverse() : p)
const OUT = ccw(MOUNTAIN)
/** Distance from a point to the outline (positive inside). */
function inside(x: number, y: number) {
  let c = false, d = Infinity
  for (let i = 0; i < OUT.length; i++) {
    const [x0, y0] = OUT[i], [x1, y1] = OUT[(i + 1) % OUT.length]
    if ((y0 > y) !== (y1 > y) && x < x0 + ((y - y0) / (y1 - y0)) * (x1 - x0)) c = !c
    const ex = x1 - x0, ey = y1 - y0, t = clamp(((x - x0) * ex + (y - y0) * ey) / (ex * ex + ey * ey))
    d = Math.min(d, Math.hypot(x - x0 - t * ex, y - y0 - t * ey))
  }
  return c ? d : -d
}

/** A peak: summit height and a profile of fall against distance; `turn` sets its ridgelines. */
type Peak = { x: number; y: number; h: number; fall: (d: number) => number; turn: number; snowy: boolean }
const PEAKS: Peak[] = [
  // The spire: the summit node, 61 m published. Steep near the top.
  { x: 54.1, y: -68.7, h: 61, fall: (d) => (d < 5 ? 1.8 * d : 9 + 1.1 * (d - 5)), turn: 0.35, snowy: true },
  // The lower snowy peak north-west of it (photo 01, left).
  { x: 31, y: -62, h: 46, fall: (d) => (d < 4 ? 2.2 * d : 8.8 + 1.0 * (d - 4)), turn: 0.9, snowy: true },
  // The brown, craggy peak south-east of it (photo 01, right).
  { x: 56, y: -82, h: 45, fall: (d) => (d < 5 ? 0.3 * d * d : 7.5 + 1.15 * (d - 5)), turn: 0.1, snowy: false },
]
/** Distance with ridges: part round, part a turned square, so the faces meet in ridgelines. */
const ridged = (dx: number, dy: number, turn: number) => {
  const c = Math.cos(turn), s = Math.sin(turn), u = dx * c + dy * s, v = -dx * s + dy * c
  return 0.55 * Math.hypot(dx, dy) + 0.45 * (Math.abs(u) + Math.abs(v)) / Math.SQRT2
}
const smax = (a: number, b: number, k = 3) => { const h = clamp(0.5 + (a - b) / (2 * k)); return b + (a - b) * h + k * h * (1 - h) }

/** Height of the rock and snow above the ground. */
function heightAt(x: number, y: number) {
  const d = Math.max(0, inside(x, y))
  // Cliffs at the foot, about 20 m, taller on the west face the photos show,
  // lower on the north where the lift passes over; a lumpy plateau inside.
  // Cliffs at the foot: about 16 m, lower round the north-west corner, where
  // the lift crosses over the rock and the photos see only treetops, and a
  // lumpy plateau inside.
  const nw = smoothstep(-62, -50, y) * smoothstep(28, 15, x)
  const se = smoothstep(-70, -95, y) * smoothstep(35, 60, x)
  const edge = 16 - 6 * nw - 4 * se + 4 * (noise(x * 0.12, y * 0.12) - 0.5)
  let z = edge + (7 - 4 * nw) * smoothstep(0, 12, d) + 4 * (noise(x * 0.08 + 7, y * 0.08) - 0.5)
  for (const p of PEAKS) {
    const r = ridged(x - p.x, y - p.y, p.turn)
    const crag = 2.2 * (noise(x * 0.25 + p.h, y * 0.25) - 0.5) * smoothstep(0, 6, r)
    z = smax(z, p.h - p.fall(r) + crag, 2.5)
  }
  return z
}

/** The open track carves a groove where it crosses the rock, so it never sinks in. */
const CARVE = RUNS.filter((r) => ['lift upper', 'summit run', 'broken track'].includes(r.name))
function carved(x: number, y: number, z: number) {
  for (const run of CARVE)
    for (let i = 0; i < run.pts.length - 1; i++) {
      const [x0, y0, z0] = run.pts[i], [x1, y1, z1] = run.pts[i + 1]
      const ex = x1 - x0, ey = y1 - y0, t = clamp(((x - x0) * ex + (y - y0) * ey) / (ex * ex + ey * ey))
      // Stop the groove short of where the lift and the broken track enter
      // the rock, so they go into a face, not along a trench.
      if (run.name === 'lift upper' && i === 1 && t > 0.75) continue
      if (run.name === 'broken track' && i === 0) continue
      const dist = Math.hypot(x - x0 - t * ex, y - y0 - t * ey)
      const zt = z0 + (z1 - z0) * t - 1.6
      if (dist < 7) z = Math.min(z, zt + Math.max(0, dist - 2.2) * 5)
    }
  return z
}
const H = (x: number, y: number) => carved(x, y, heightAt(x, y))

// The mesh: the outline resampled evenly, and rings shrunk towards a centre
// to the middle, lofted at the heightfield's height. Walls run from the
// ground up to the first ring.
{
  const N = 72
  const per: number[] = [0]
  for (let i = 0; i < OUT.length; i++) per.push(per[i] + Math.hypot(OUT[(i + 1) % OUT.length][0] - OUT[i][0], OUT[(i + 1) % OUT.length][1] - OUT[i][1]))
  const total = per[OUT.length]
  const edge: XY[] = Array.from({ length: N }, (_, k) => {
    const s = (k / N) * total
    let i = 0
    while (per[i + 1] < s) i++
    const t = (s - per[i]) / (per[i + 1] - per[i]), a = OUT[i], b = OUT[(i + 1) % OUT.length]
    return [a[0] + (b[0] - a[0]) * t, a[1] + (b[1] - a[1]) * t]
  })
  // Keep the outline's corners: move each to its nearest sample.
  for (const c of OUT) {
    let best = 0
    edge.forEach((e, k) => { if (Math.hypot(e[0] - c[0], e[1] - c[1]) < Math.hypot(edge[best][0] - c[0], edge[best][1] - c[1])) best = k })
    if (Math.hypot(edge[best][0] - c[0], edge[best][1] - c[1]) < 2.2) edge[best] = c
  }
  const C: XY = [36, -70]
  const ts = [0.93, 0.87, 0.8, 0.72, 0.63, 0.54, 0.45, 0.36, 0.27, 0.18, 0.1]
  const ring = (t: number): XY[] => edge.map(([x, y]) => [C[0] + (x - C[0]) * t, C[1] + (y - C[1]) * t])
  const rings: V3[][] = ts.map((t) => ring(t).map(([x, y]): V3 => [x, y, H(x, y)]))
  // Pin a vertex on each summit so the spires keep their points.
  for (const p of PEAKS) {
    let bi = 0, bk = 0, bd = Infinity
    rings.forEach((r, i) => r.forEach((v, k) => { const d = Math.hypot(v[0] - p.x, v[1] - p.y); if (d < bd) { bd = d; bi = i; bk = k } }))
    rings[bi][bk] = [p.x, p.y, heightAt(p.x, p.y)]
  }
  const apex: V3 = [C[0], C[1], H(C[0], C[1])]
  // Snow lies on the slopes that aren't sheer, above a ragged snowline; the
  // brown peak's top stays bare (photo 01).
  const isSnow = (a: V3, b: V3, c: V3) => {
    const n = unit(cross(sub(b, a), sub(c, a)))
    const x = (a[0] + b[0] + c[0]) / 3, y = (a[1] + b[1] + c[1]) / 3, z = (a[2] + b[2] + c[2]) / 3
    const line = 27 + 9 * noise(x * 0.18, y * 0.18)
    const bare = Math.hypot(x - PEAKS[2].x, y - PEAKS[2].y) < 8 && z > 38
    return !bare && z > line && n[2] > 0.42 - 0.25 * smoothstep(40, 58, z)
  }
  const tri = (a: V3, b: V3, c: V3) => (isSnow(a, b, c) ? snowTris : rockTris).push([a, b, c])
  const snowTris: V3[][] = [], rockTris: V3[][] = []
  for (let i = 0; i < rings.length - 1; i++)
    for (let k = 0; k < N; k++) {
      const j = (k + 1) % N, a = rings[i][k], b = rings[i][j], c = rings[i + 1][j], d = rings[i + 1][k]
      // Split each quad along its shorter diagonal, so ridges stay sharp.
      if (len(sub(a, c)) < len(sub(b, d))) { tri(a, b, c); tri(a, c, d) } else { tri(a, b, d); tri(b, c, d) }
    }
  const last = rings[rings.length - 1]
  for (let k = 0; k < N; k++) tri(last[k], last[(k + 1) % N], apex)
  const L = (v: V3): V3 => [v[0] - AX, v[1] - AY, v[2]]
  smooth(snow, (p) => snowTris.forEach(([a, b, c]) => p.tri(L(a), L(b), L(c))), 38)
  // The cliffs: from the ground to the rim, battered a little and broken into
  // vertical buttresses (the rock's striations in the photos).
  smooth(rock, (p) => {
    rockTris.forEach(([a, b, c]) => p.tri(L(a), L(b), L(c)))
    const top = rings[0]
    // Every other column of the cliff stands proud: vertical ribs, the
    // striated rock of the photos. The face leans back as it rises.
    const at = (k: number, t: number, z: number): V3 => {
      const [x, y] = edge[k]
      return [C[0] + (x - C[0]) * t, C[1] + (y - C[1]) * t, z]
    }
    const rib = (k: number) => (k % 2 ? 0.035 : 0) + 0.02 * hash2(k, 7)
    const foot = edge.map((_, k): V3 => at(k, 1, -0.5))
    const low = edge.map((_, k): V3 => at(k, 0.995 - rib(k), top[k][2] * 0.3))
    const mid = edge.map((_, k): V3 => at(k, 0.975 - rib(k) * 1.2, top[k][2] * 0.65))
    p.loft([foot, low, mid, top].map((r) => r.map(L)))
  }, 38)
}

// ------------------------------------------------------------ the track

/** Rail height of the open track wherever it is: for supports. */
const W = 1.7, DEPTH = 0.7
function sweepRun(pts: [number, number, number][]) {
  // Resample to about 2.5 m, round the corners with a Catmull–Rom pass.
  const dense: V3[] = []
  for (let i = 0; i < pts.length - 1; i++) {
    const p0 = pts[Math.max(0, i - 1)], p1 = pts[i], p2 = pts[i + 1], p3 = pts[Math.min(pts.length - 1, i + 2)]
    const L = Math.hypot(p2[0] - p1[0], p2[1] - p1[1], p2[2] - p1[2])
    const n = Math.max(1, Math.round(L / (L > 12 ? 6 : 2.5)))
    for (let s = 0; s < n; s++) {
      const t = s / n, t2 = t * t, t3 = t2 * t
      const f = (k: number) => 0.5 * (2 * p1[k] + (-p0[k] + p2[k]) * t + (2 * p0[k] - 5 * p1[k] + 4 * p2[k] - p3[k]) * t2 + (-p0[k] + 3 * p1[k] - 3 * p2[k] + p3[k]) * t3)
      dense.push([f(0) - AX, f(1) - AY, f(2)])
    }
  }
  const e = pts[pts.length - 1]
  dense.push([e[0] - AX, e[1] - AY, e[2]])
  // Banking from plan curvature, up to 35°.
  const sections = dense.map((c, i) => {
    const a = dense[Math.max(0, i - 1)], b = dense[Math.min(dense.length - 1, i + 1)]
    const fwd = unit(sub(b, a))
    const h0 = Math.atan2(c[1] - a[1], c[0] - a[0]), h1 = Math.atan2(b[1] - c[1], b[0] - c[0])
    let turn = h1 - h0
    while (turn > Math.PI) turn -= TAU
    while (turn < -Math.PI) turn += TAU
    const ds = (Math.hypot(c[0] - a[0], c[1] - a[1]) + Math.hypot(b[0] - c[0], b[1] - c[1])) / 2 || 1
    const bank = clamp((turn / ds) * 9, -0.6, 0.6)
    const side0 = unit(cross(fwd, [0, 0, 1]))
    const up0 = cross(side0, fwd)
    const side = plus(scale(side0, Math.cos(bank)), scale(up0, -Math.sin(bank)))
    const up = unit(cross(side, fwd))
    const o = (s: number, u: number) => plus(c, plus(scale(side, s), scale(up, u)))
    return [o(-W / 2, 0), o(W / 2, 0), o(W / 2, -DEPTH), o(-W / 2, -DEPTH)]
  })
  for (let k = 0; k < sections.length - 1; k++) {
    const r0 = sections[k], r1 = sections[k + 1]
    for (let i = 0; i < 4; i++) {
      const j = (i + 1) % 4
      track.quad(r0[j], r0[i], r1[i], r1[j])
    }
  }
  return dense
}
/** A square column with chamfered corners, from z0 to z1. */
function column(p: Part, x: number, y: number, z0: number, z1: number, r: number) {
  const ring = (z: number) => Array.from({ length: 6 }, (_, i): V3 => [x + r * Math.cos((i / 6) * TAU), y + r * Math.sin((i / 6) * TAU), z])
  p.loft([ring(z0), ring(z1)])
}
for (const run of RUNS) {
  const dense = sweepRun(run.pts)
  // Supports about every 9 m wherever the rails stand clear of what's below.
  let since = 99
  for (let i = 1; i < dense.length - 1; i++) {
    const [x, y, z] = dense[i]
    since += Math.hypot(x - dense[i - 1][0], y - dense[i - 1][1])
    const under = inside(x + AX, y + AY) > 0 ? H(x + AX, y + AY) : 0
    if (since < 9 || z - DEPTH - under < 1.2) continue
    since = 0
    column(track, x, y, under - 0.3, z - DEPTH + 0.05, z > 8 ? 0.55 : 0.4)
  }
}
// The broken track's end bends up and twists, torn off in mid-air.
{
  const [x1, y1, z1] = RUNS.find((r) => r.name === 'broken track')!.pts[2]
  const dir = unit([61.1 - 55.3, -50.5 + 59.3, 0])
  const at = (t: number, lift: number): V3 => [x1 - AX + dir[0] * t, y1 - AY + dir[1] * t, z1 + lift]
  const side = unit(cross(dir, [0, 0, 1]))
  const sec = (c: V3, roll: number) => {
    const s = plus(scale(side, Math.cos(roll)), [0, 0, Math.sin(roll)])
    return [plus(c, scale(s, -W / 2)), plus(c, scale(s, W / 2)), plus(plus(c, scale(s, W / 2)), [0, 0, -DEPTH]), plus(plus(c, scale(s, -W / 2)), [0, 0, -DEPTH])]
  }
  const r0 = sec(at(0, 0), 0), r1 = sec(at(3, 1.6), 0.5)
  for (let i = 0; i < 4; i++) track.quad(r0[(i + 1) % 4], r0[i], r1[i], r1[(i + 1) % 4])
  track.quad(r1[0], r1[1], r1[2], r1[3])
}
// Tunnel mouths: a dark arch-topped slab set into the rock where the track goes in.
for (const [x, y, z, k = 1] of PORTALS) {
  const cx = x - AX, cy = y - AY
  const toC = unit([36 - x, -70 - y, 0])
  const side = unit(cross(toC, [0, 0, 1]))
  const w = 2.8 * k, h = 5.6 * k
  const pts: V3[] = []
  pts.push(plus([cx, cy, z - DEPTH - 0.6], scale(side, -w)), plus([cx, cy, z - DEPTH - 0.6], scale(side, w)))
  for (let i = 0; i <= 6; i++) {
    const a = (i / 6) * Math.PI
    pts.push(plus([cx, cy, z + h - w + Math.sin(a) * w * 0.9], scale(side, w * Math.cos(a))))
  }
  // A slab 3 m deep into the rock, so it shows on any face angle.
  const back = pts.map((p) => plus(p, scale(toC, 3)))
  const front = pts.map((p) => plus(p, scale(toC, -0.6)))
  const outline = [front[0], front[1], ...front.slice(2)]
  for (let i = 0; i < outline.length; i++) {
    const j = (i + 1) % outline.length
    track.quad(outline[j], outline[i], back[i === 0 ? 0 : i === 1 ? 1 : i], back[j === 0 ? 0 : j === 1 ? 1 : j])
  }
  for (let i = 2; i < outline.length - 1; i++) track.tri(outline[1], outline[i + 1], outline[i])
  track.tri(outline[0], outline[outline.length - 1], outline[1])
}

// ------------------------------------------------- temple and the village

/** Ear-clip a simple polygon (counter-clockwise), returning index triples. */
function earclip(p: XY[]): [number, number, number][] {
  const idx = p.map((_, i) => i), out: [number, number, number][] = []
  const cz = (o: XY, a: XY, b: XY) => (a[0] - o[0]) * (b[1] - o[1]) - (a[1] - o[1]) * (b[0] - o[0])
  let guard = 0
  while (idx.length > 3 && guard++ < 5000) {
    let cut = false
    for (let i = 0; i < idx.length; i++) {
      const ia = idx[(i - 1 + idx.length) % idx.length], ib = idx[i], ic = idx[(i + 1) % idx.length]
      const A = p[ia], B = p[ib], Cc = p[ic]
      if (cz(A, B, Cc) <= 1e-9) continue
      const same = (q: XY, r: XY) => Math.abs(q[0] - r[0]) < 1e-6 && Math.abs(q[1] - r[1]) < 1e-6
      if (idx.some((j) => j !== ia && j !== ib && j !== ic && !same(p[j], A) && !same(p[j], B) && !same(p[j], Cc) && cz(A, B, p[j]) >= 0 && cz(B, Cc, p[j]) >= 0 && cz(Cc, A, p[j]) >= 0)) continue
      out.push([ia, ib, ic]); idx.splice(i, 1); cut = true; break
    }
    if (!cut) break
  }
  if (idx.length === 3) out.push([idx[0], idx[1], idx[2]])
  return out
}
/** A prism over a polygon (with an optional hole), walls in `wall`, lid in `lid`. */
function prism(poly: XY[], z0: number, z1: number, wall: Part, lid: Part, hole?: XY[]) {
  const o = ccw(poly).map(local)
  const walls = (ring: XY[], flip: boolean) => {
    for (let i = 0; i < ring.length; i++) {
      const a = ring[i], b = ring[(i + 1) % ring.length]
      const q: V3[] = [[a[0], a[1], z0], [b[0], b[1], z0], [b[0], b[1], z1], [a[0], a[1], z1]]
      if (flip) wall.quad(q[1], q[0], q[3], q[2])
      else wall.quad(q[0], q[1], q[2], q[3])
    }
  }
  walls(o, false)
  let shape = o
  if (hole) {
    const h = ccw(hole).map(local).reverse()
    walls(h, true)
    // Bridge the hole into the outline at their closest pair of corners.
    let bi = 0, bj = 0, bd = Infinity
    o.forEach((a, i) => h.forEach((b, j) => { const d = Math.hypot(a[0] - b[0], a[1] - b[1]); if (d < bd) { bd = d; bi = i; bj = j } }))
    shape = [...o.slice(0, bi + 1), ...h.slice(bj), ...h.slice(0, bj + 1), ...o.slice(bi)]
  }
  for (const [a, b, c] of earclip(shape)) lid.tri([shape[a][0], shape[a][1], z1], [shape[b][0], shape[b][1], z1], [shape[c][0], shape[c][1], z1])
}
/** A box with its edges chamfered, in the local frame. */
function box(p: Part, x0: number, y0: number, x1: number, y1: number, z0: number, z1: number, lid: Part = p) {
  prism([[x0, y0], [x1, y0], [x1, y1], [x0, y1]], z0, z1, p, lid)
}

// The temple: a low body over its OSM outline, and the tower the lift runs
// through at its south end, a stepped, crumbling top of uneven blocks.
prism(TEMPLE, 0, 9, temple, roof)
box(temple, 5.2, -34.5, 17, -21.5, 9, 22)
box(temple, 6.2, -33.5, 16, -22.5, 22, 25)
const crown: [number, number, number, number, number][] = [
  [6.2, -33.5, 9.8, -29.5, 28.5], [10.4, -33.5, 13.6, -30, 27], [13.8, -33.5, 16, -29.5, 29.5],
  [6.2, -28.6, 9, -25.5, 27.2], [12.5, -29.2, 16, -25.8, 26.4], [6.2, -25, 10.5, -22.5, 29], [11, -25.2, 16, -22.5, 27.6],
  [9.4, -29.4, 12.6, -25.6, 30.5],
]
for (const [x0, y0, x1, y1, z] of crown) box(temple, x0, y0, x1, y1, 25, z)
// Dark window slots on the tower's west and south faces.
for (const zz of [12.5, 17.5]) {
  for (const yy of [-31, -25]) box(track, 5.0, yy, 5.3, yy + 2, zz, zz + 2.8)
  for (const xx of [7.5, 13]) box(track, xx, -34.7, xx + 2, -34.4, zz, zz + 2.8)
}

// The station village: one low plastered block over the station's outline,
// round its courtyard, with three small pagoda roofs over it.
prism(STATION, 0, 7.5, village, roof, COURTYARD)
for (const [x, y, s] of [[-50, -14, 4.5], [-80, -15, 4], [-40, -33, 3.8]] as [number, number, number][]) {
  const [cx, cy] = local([x, y])
  box(temple, x - s * 0.7, y - s * 0.7, x + s * 0.7, y + s * 0.7, 7.5, 10)
  const base: V3[] = [[cx - s, cy - s, 10], [cx + s, cy - s, 10], [cx + s, cy + s, 10], [cx - s, cy + s, 10]]
  const top: V3 = [cx, cy, 10 + s * 0.8]
  for (let i = 0; i < 4; i++) roof.tri(base[i], base[(i + 1) % 4], top)
}

// ---------------------------------------------------------------- output

// Identity finishes from photo 01: the rust-brown rock, lightened to the
// palette (STYLE.md), and the snow, a warm white; the temple and pagodas in
// the palette's terracotta; the dark track (lightened only to the charcoal
// limit), which also fills the tunnel mouths and window slots.
const parts = [
  { part: rock, material: finish('everest-rock', 0xa6755f) },
  { part: snow, material: finish('everest-snow', 0xf1f0ea) },
  { part: temple, material: PALETTE.terracotta },
  { part: village, material: PALETTE.stone },
  { part: roof, material: PALETTE.roof },
  { part: track, material: finish('everest-track', 0x4f4c4a) },
]
const triangles = parts.reduce((sum, { part }) => sum + part.triangles, 0)
console.log(parts.map(({ part, material }) => `${material.name} ${part.triangles}`).join(', '))
if (triangles > 6500) throw new Error(`Triangle budget exceeded: ${triangles}`)
const glb = writeGlb('Expedition Everest', parts, {
  license: 'CC0-1.0', frame: 'Y up, -Z north, +X east, metres; origin at ground anchor',
  bearing: 0, elevation: 0, height: 61,
})
if (glb.length > 256000) throw new Error(`File budget exceeded: ${glb.length}`)
const out = new URL('../../landmarks/models/wdw-expedition-everest.glb', import.meta.url).pathname
await Bun.write(out, glb)
console.log(`${out}: ${triangles} triangles, ${glb.length} bytes`)
console.log(`anchor ${(LON0 + AX / MX).toFixed(7)}, ${(LAT0 + AY / MY).toFixed(7)}`)
