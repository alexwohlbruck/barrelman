/**
 * Slinky Dog Dash, Toy Story Land, Disney's Hollywood Studios: procedural, CC0-1.0.
 * bun scripts/landmarks/wdw-slinky-dog-dash.ts   (REPORT=1 for the element table)
 *
 * Map frame: x east, y north, z up, metres; bearing 0, so the frame is true
 * north. The park frame here is equirectangular about 28.3565 N, 81.5610 W
 * (`origin`); the model's origin is the middle of the ride's footprint (see
 * the anchor), at the lowest ground under the track. Built with
 * ./coaster-kit.ts.
 *
 * Plan. OSM chains the ride as a closed, oneway circuit from the station
 * (way/685092736, "Slinky Dog Dash Show Building"): way/751717116, 1294595151,
 * 1294595160, 1294595158, 798615126, 1294595156, 1294595155, 1294595157,
 * 1294595154, 685772990, 1294595159, 1294595153, 1294595152 and the covered
 * way/685772991 through the station: 837 m. The sidings and transfer table
 * of the maintenance bay (way/685772992, 685772993, 685772994, 685772995,
 * 685772996, 685772997, 685772998, 685772999) are inside the show building
 * and are not drawn. Laid over the USGS NAIP orthophoto (public domain), the
 * OSM trace is up to 10 m out in places: the north loop and the helix sit
 * 7-10 m too far north, the south loop 9 m too far south, the second launch
 * 4-5 m west. So the circuit below keeps OSM's topology and ride direction
 * but is redrawn node by node over NAIP: 787 m round, the 38 m through the
 * station taken from OSM (it is under the roof). No length is published.
 *
 * Profile: a custom Mack Rides Launch Coaster (RCDB 13377, opened 2018), two
 * LSM launches, the first rolling (RCDB, Coasterpedia); 50 ft (15.2 m) high,
 * 40 mph (64 km/h) (Wikipedia). The element order is Wikipedia's "Layout".
 * Heights are estimated from the photos below; only the 15.2 m top and the
 * 64 km/h are published. Positions are metres along the traced polyline
 * (787 m round) and % of it:
 *
 *   out of the station, a right turn climbing onto the launch   s 0-24 (0-3%)
 *   launch 1 on the elevated straight, 5.5 m                   s 24-55 (3-7%)
 *   up to 11.5 m, along the west side of the east loop, crest
 *   at 13 m over the plaza                                     s 55-96 (7-12%)
 *   dive through the left turn at the bottom of the east loop  s 96-118 (12-15%)
 *   east leg, low, with a small airtime hill                   s 118-168 (15-21%)
 *   left-handed helix round Jessie's blocks: a low lap under
 *   the exit, then up onto the top lap at 6.5 m                s 168-262 (21-33%)
 *   dip into the north loop, left turn, low run back east      s 262-357 (33-45%)
 *   right turn, through the blocks under the path bridge, stop s 357-400 (45-51%)
 *   launch 2 through the GO! rings, to 64 km/h                 s 400-452 (51-57%)
 *   top hill, 15.2 m, then the left turn of the south loop     s 452-528 (57-67%)
 *   bunny hills north, the last one over the GO! rings         s 528-600 (67-76%)
 *   left turn, right turn round the west loop                  s 600-700 (76-89%)
 *   final brake run (Wheezy), station                          s 708-787 (90-100%)
 *
 * Measured: the plan (NAIP). Published: the 15.2 m top, the 64 km/h, the
 * two launches and the element order. Estimated from the photos: every
 * other height (launch 1's straight at about 5.5 m from "Slinky Dog Dash
 * (43084268832)"; the 13 m crest from "(29262639308)"; the helix's top lap
 * from "(43134549171)"; the bunny hills and the crossing over the rings
 * from "(29262621908)" and Freddo's photo). Which hill is the 15.2 m one
 * is a judgement: the tall climb after launch 2 stands out above the rest
 * in every distant photo. The show building's 11 m is a guess.
 *
 * Photos (Wikimedia Commons): "Slinky Dog Dash (29262639308)", "(42415750424)",
 * "(43084268832)", "(29262625858)", "(29262621908)", "(43134549171)" and others
 * of the set, elisfkc, July 2018, CC BY-SA 2.0; "Slinky Dog Dash (Orlando)
 * May 2023" and "Slinky Dog Dash May 2023 (1)", Benoît Prieur, CC0; "Slinky
 * Dog Dash", Freddo, 2018, CC BY-SA 4.0. The train: Flickr via Openverse,
 * "Disney's Hollywood Studios" 54164351104 and 54164351039 (Roller Coaster
 * Philosophy, 2024, CC BY 2.0) and "Slinky Dog Dash - Roller Coaster"
 * 46303605301 (MagicGuides, CC BY 2.0). Plan: USGS NAIP (public domain).
 * Terrain: Mapzen/AWS Terrarium (open data).
 *
 * Style: the track painted raspberry red, rails and truss alike, on chunky
 * golden-yellow columns; periwinkle catwalks along the two launches and the
 * final brakes, and the four GO! rings over the second launch; one Slinky
 * Dog train (head car, coil-wrapped cars, spring tail) cresting the bunny
 * hill over the rings, as in most photos of it. The show building is plain
 * walls and a flat roof, 11 m (estimated; OSM has no height). Jessie, Rex
 * and the block towers are set pieces off the track and are left out.
 */
import { buildCoaster, type TrackFrame } from './coaster-kit'
import { Part, type V3 } from './mesh'
import { finish } from './palette'

// The circuit, traced over NAIP along the OSM ways, node by node in the
// direction of travel, in the park frame; from the station exit.
const CHAIN: [number, number][] = [
  [-201.0, 6.0], [-198.0, 8.6], [-194.0, 10.0], [-190.5, 11.5], [-187.2, 11.9], [-183.5, 10.8], [-180.0, 9.0], [-172.0, 3.0],
  [-165, -3.0], [-155, -11.5], [-146.8, -19.3], [-143.0, -23.1], [-140.2, -27.0], [-138.6, -30.8], [-137.8, -35.8], [-137.5, -41.3],
  [-136.4, -45.7], [-134.2, -48.4], [-130.9, -49.5], [-126.5, -49.0], [-122.1, -47.3], [-119.3, -45.1], [-117.7, -41.8], [-117.1, -38.5],
  [-116.9, -32], [-117.2, -24], [-117.4, -16], [-117.5, -8], [-117.5, 0], [-117.6, 6], [-118.0, 10.5], [-119.3, 13.6],
  [-121.6, 15.8], [-124.6, 16.9], [-128.6, 16.8], [-133.2, 15.8], [-137.7, 15.6], [-141.7, 13.5], [-144.5, 9.9], [-145.5, 5.5],
  [-144.5, 1.1], [-141.7, -2.5], [-137.7, -4.6], [-133.2, -4.8], [-129.0, -3.1], [-125.9, 0.2], [-124.5, 4.6], [-125.6, 9.5],
  [-127.9, 13.1], [-131.2, 15.3], [-135.0, 15.8], [-139, 16.2], [-143.5, 17.0], [-147.5, 18.5], [-150.5, 19.5], [-155, 21.2],
  [-160.2, 22.6], [-165, 24.0], [-170, 25.4], [-175, 26.2], [-179.7, 25.8], [-182.6, 24.2], [-184.4, 21.2], [-184.8, 17.5],
  [-183.4, 14.6], [-180.5, 12.4], [-177.5, 10.6], [-172.2, 8.6], [-167, 7.2], [-160, 5.7], [-155, 4.2], [-150.5, 2.2],
  [-147.3, -0.3], [-145, -3.5], [-144.3, -7.0], [-145.5, -10.5], [-148, -13.6], [-151.3, -17.6], [-161.5, -30.6], [-175, -48.2],
  [-190, -67.3], [-201, -81.0], [-205.8, -87.0], [-209.6, -90.6], [-213.5, -93.6], [-218.4, -97.2], [-222.2, -101.2], [-224.6, -105],
  [-226.5, -112.4], [-227.2, -119.4], [-224.7, -123.6], [-220.5, -126.4], [-215.6, -127.1], [-210, -125.7], [-205.8, -122.2], [-202.3, -116.6],
  [-200.2, -111], [-196, -100.5], [-191.8, -87.2], [-187.6, -73.2], [-185.5, -62], [-184.8, -53.6], [-185.9, -49.4], [-187.6, -46.6],
  [-191.8, -43.8], [-196, -43.1], [-201.6, -44.1], [-205.8, -48], [-207.9, -55], [-207.2, -62], [-204.8, -69], [-203.7, -75.3],
  [-205.1, -80.2], [-208.6, -83.7], [-213.5, -85.8], [-219.1, -86.1], [-224.7, -84], [-228.6, -80.2], [-230, -74.6], [-229.5, -69],
  [-228.6, -59.2], [-227.2, -51.5], [-224.7, -43.1], [-222.6, -34], [-221.2, -29.5], [-219.8, -25.2], [-218.4, -22.7], [-210, -8.6],
  [-203.5, 2.0],
]
// Terrain (Mapzen/AWS Terrarium, open data), 20 m cells from (-250, -145);
// rows south to north, columns west to east; metres above 29.3 m.
const GROUND = [
  [0.0, 0.2, 0.4, 0.5, 0.7, 0.9, 1.0, 1.1],
  [0.1, 0.3, 0.5, 0.6, 0.7, 0.9, 1.1, 1.2],
  [0.3, 0.5, 0.6, 0.8, 0.9, 1.0, 1.1, 1.2],
  [0.4, 0.6, 0.8, 0.9, 1.0, 1.1, 1.2, 1.3],
  [0.5, 0.7, 0.9, 1.0, 1.1, 1.2, 1.3, 1.3],
  [0.6, 0.8, 0.9, 1.0, 1.2, 1.2, 1.3, 1.3],
  [0.6, 0.8, 1.0, 1.1, 1.2, 1.3, 1.3, 1.3],
  [0.6, 0.8, 1.0, 1.1, 1.2, 1.3, 1.3, 1.3],
  [0.6, 0.8, 1.0, 1.1, 1.2, 1.3, 1.3, 1.3],
  [0.6, 0.8, 1.0, 1.1, 1.2, 1.3, 1.3, 1.3],
  [0.6, 0.8, 0.9, 1.1, 1.2, 1.3, 1.3, 1.3],
]
// The show building (way/685092736).
const STATION: [number, number][] = [
  [-191.7, 6.0], [-195.7, 8.3], [-197.1, 9.1], [-197.5, 9.3], [-197.9, 9.6], [-198.6, 10.0], [-201.4, 11.5], [-213.2, 18.3],
  [-216.3, 20.1], [-219.8, 22.1], [-223.0, 24.0], [-236.6, 0.8], [-239.8, -4.8], [-244.3, -12.6], [-238.6, -15.8], [-241.6, -21.0],
  [-234.8, -24.9], [-237.1, -28.9], [-228.2, -34.1], [-226.8, -33.4], [-224.9, -32.4], [-222.0, -28.0], [-222.2, -26.4], [-220.4, -27.6],
  [-219.3, -28.3], [-215.6, -30.4], [-213.0, -29.8], [-211.2, -29.5], [-208.6, -24.6], [-207.7, -22.9], [-205.0, -17.2], [-203.3, -13.8],
  [-202.1, -15.0], [-201.5, -13.1], [-201.4, -11.2], [-202.0, -10.8], [-200.6, -8.8], [-198.9, -14.8], [-195.2, -13.8], [-192.1, -13.0],
  [-188.2, -13.7], [-187.5, -13.8], [-186.6, -14.0], [-173.0, -16.4], [-171.4, -7.8], [-178.0, -6.6], [-182.0, -5.9], [-182.6, -6.4],
  [-186.2, 7.4],
]

const add = (a: V3, b: V3): V3 => [a[0] + b[0], a[1] + b[1], a[2] + b[2]]
const mul = (a: V3, k: number): V3 => [a[0] * k, a[1] * k, a[2] * k]
const unit = (a: V3): V3 => mul(a, 1 / (Math.hypot(...a) || 1))

const LAUNCH = finish('launch-periwinkle', 0x6a6eaf)
const TAN = finish('slinky-tan', 0xd8a465)

/** A point in a track frame: `a` along, `l` to the left, `u` up. */
const local = (f: TrackFrame, a: number, l: number, u: number): V3 => add(add(f.p, mul(f.t, a)), add(mul(f.left, l), mul(f.up, u)))

/** A box in a track frame, all six faces, flat-shaded. */
function box(part: Part, f: TrackFrame, a0: number, a1: number, l0: number, l1: number, u0: number, u1: number) {
  const P = (a: number, l: number, u: number) => local(f, a, l, u)
  const c = [P(a0, l0, u0), P(a1, l0, u0), P(a1, l1, u0), P(a0, l1, u0), P(a0, l0, u1), P(a1, l0, u1), P(a1, l1, u1), P(a0, l1, u1)]
  const q = (i: number, j: number, k: number, m: number) => part.quad(c[i], c[j], c[k], c[m])
  // Orientation: t × left = up, so (a, l, u) is right-handed.
  q(0, 3, 2, 1); q(4, 5, 6, 7); q(0, 1, 5, 4); q(2, 3, 7, 6); q(1, 2, 6, 5); q(3, 0, 4, 7)
}

/** An octagonal barrel along the track, centred `u` up, smooth-shaded sides, flat ends. */
function barrel(part: Part, f: TrackFrame, a0: number, a1: number, r: number, u: number, squash = 1) {
  const ring = (a: number) => Array.from({ length: 8 }, (_, k) => {
    const th = ((k + 0.5) / 8) * 2 * Math.PI
    return { p: local(f, a, Math.cos(th) * r, u + Math.sin(th) * r * squash), n: unit(add(mul(f.left, Math.cos(th)), mul(f.up, Math.sin(th) / squash))) }
  })
  const A = ring(a0), B = ring(a1)
  for (let k = 0; k < 8; k++) {
    const j = (k + 1) % 8
    part.tri(A[k].p, A[j].p, B[j].p, undefined, undefined, undefined, [A[k].n, A[j].n, B[j].n])
    part.tri(A[k].p, B[j].p, B[k].p, undefined, undefined, undefined, [A[k].n, B[j].n, B[k].n])
  }
  for (let k = 1; k < 7; k++) { part.tri(A[0].p, A[k + 1].p, A[k].p); part.tri(B[0].p, B[k].p, B[k + 1].p) }
}

/** A hoop of the coil spring: a half ring arching over the car, `seg` boxes. */
function hoop(part: Part, f: TrackFrame, a: number, r: number, u: number, w = 0.14, seg = 6) {
  for (let k = 0; k < seg; k++) {
    const t0 = -0.15 * Math.PI + (k / seg) * 1.3 * Math.PI, t1 = -0.15 * Math.PI + ((k + 1) / seg) * 1.3 * Math.PI
    const p0 = local(f, a, Math.cos(t0) * r, u + Math.sin(t0) * r), p1 = local(f, a, Math.cos(t1) * r, u + Math.sin(t1) * r)
    const n0 = unit(add(mul(f.left, Math.cos(t0)), mul(f.up, Math.sin(t0)))), n1 = unit(add(mul(f.left, Math.cos(t1)), mul(f.up, Math.sin(t1))))
    const o0 = mul(n0, w), o1 = mul(n1, w), d = mul(f.t, w)
    const quad = (A: V3, B: V3, C: V3, D: V3) => part.quad(A, B, C, D)
    // Outer, inner, front and back faces of the strip.
    quad(add(p0, add(o0, d)), add(p0, sub3(o0, d)), add(p1, sub3(o1, d)), add(p1, add(o1, d)))
    quad(add(p0, sub3(mul(o0, -1), d)), add(p0, add(mul(o0, -1), d)), add(p1, add(mul(o1, -1), d)), add(p1, sub3(mul(o1, -1), d)))
    quad(add(p0, add(o0, d)), add(p1, add(o1, d)), add(p1, add(mul(o1, -1), d)), add(p0, add(mul(o0, -1), d)))
    quad(add(p0, sub3(o0, d)), add(p0, sub3(mul(o0, -1), d)), add(p1, sub3(mul(o1, -1), d)), add(p1, sub3(o1, d)))
  }
}
const sub3 = (a: V3, b: V3): V3 => [a[0] - b[0], a[1] - b[1], a[2] - b[2]]

await buildCoaster({
  name: 'Slinky Dog Dash',
  out: '../../landmarks/models/wdw-slinky-dog-dash.glb',
  origin: [-81.561, 28.3565],
  chain: CHAIN,
  anchor: [-180, -50],
  ground: { x0: -250, y0: -145, step: 20, rows: GROUND, base: 29.3 },
  // Heights of the running surface above the local ground; s along the traced polyline (787 m round).
  profile: [
    { name: 'station exit', s: 0, z: 0.8 },
    { name: 'right turn', s: 6, z: 1.0 },
    { name: 'launch 1, in', s: 24, z: 5.5 },
    { name: 'launch 1, out', s: 55, z: 5.5, r: 20 },
    { name: 'up off the straight', s: 73, z: 11.5 },
    { name: 'dip, turning south', s: 83, z: 11 },
    { name: 'crest over the plaza', s: 96, z: 13 },
    { name: 'dive round the loop bottom', s: 118, z: 1.6, r: 14 },
    { name: 'east leg', s: 127, z: 1.6 },
    { name: 'airtime hill', s: 145, z: 3.4 },
    { name: 'helix, in', s: 168, z: 1.5 },
    { name: 'helix, low lap under the exit', s: 200, z: 1.8 },
    { name: 'helix, up onto the top lap', s: 234, z: 6.5 },
    { name: 'helix, out', s: 262, z: 6.5 },
    { name: 'dip into the north loop', s: 290, z: 1.5 },
    { name: 'north loop, low', s: 340, z: 1.5 },
    { name: 'block tunnel, brakes', s: 380, z: 0.8 },
    { name: 'launch 2, in', s: 400, z: 0.8 },
    { name: 'launch 2, out', s: 452, z: 0.8, r: 16 },
    { name: 'top hill', s: 484, z: 15.2 },
    { name: 'south loop, left turn', s: 528, z: 3.5, r: 16 },
    { name: 'bunny hill 1', s: 547, z: 7.3 },
    { name: 'valley', s: 559, z: 5.8 },
    { name: 'bunny hill 2', s: 571, z: 7.5 },
    { name: 'valley', s: 581, z: 6.9 },
    { name: 'over the GO! rings', s: 594, z: 9.5 },
    { name: 'left turn', s: 620, z: 4.5 },
    { name: 'valley', s: 642, z: 1.8 },
    { name: 'west loop hill', s: 664, z: 4 },
    { name: 'right turn, low', s: 692, z: 1.4 },
    { name: 'final brakes (Wheezy)', s: 708, z: 1.2 },
    { name: 'final brakes, out', s: 745, z: 1.2 },
  ],
  // The tyres out of the station and up onto the first launch.
  lift: [745, 24],
  liftHead: 1,
  launches: [
    { from: 24, to: 55, z: 5.5, head: 8.8, name: 'launch 1' },
    { from: 400, to: 452, z: 0.8, head: 16.0, name: 'launch 2' },
  ],
  brakes: [
    { from: 380, to: 400, z: 0.8, head: 0.9 },
    { from: 708, to: 745, z: 1.2, head: 0.4 },
  ],
  banks: [
    { from: 24, to: 55, deg: 0, name: 'launch 1' },
    { from: 398, to: 457, deg: 0, name: 'launch 2' },
    { from: 706, to: 24, deg: 0, name: 'brakes, station' },
  ],
  losses: [0.008, 0.0008],
  track: { W: 2.0, RAIL: 0.4, DEPTH: 1.2, SPINE: 0.9 },
  station: { ring: STATION, posts: [], roofZ: 11 },
  stationWalls: true,
  // Raspberry-red track, rails and truss alike; golden-yellow columns.
  colours: { deck: ['slinky-red', 0xc9566a], spine: 'deck', supports: ['slinky-yellow', 0xe9c35c] },
  supportR: [0.4, 0.45, 0.5],
  supportEvery: 9,
  meta: { height: 15.2, trackLength: 802 },
  extras: (kit) => {
    const launchPart = new Part(), tan = new Part()
    // Catwalks either side of the track on the launches and the final brakes.
    for (const [a, b] of [[24, 55], [400, 452], [708, 745]]) {
      for (let s = a; s < b; s += 4) {
        const f = kit.at(Math.min(s + 2, b))
        for (const sg of [1, -1]) box(launchPart, f, -2, 2, sg * 0.75, sg * 1.75, -0.35, -0.15)
      }
    }
    // The four GO! rings over the second launch, an arch each.
    for (const s of [428, 431, 434, 437]) {
      const f = kit.at(s), R = 3.4, T = 0.7, D = 0.3, SEG = 10
      for (let k = 0; k < SEG; k++) {
        const t0 = (k / SEG) * Math.PI, t1 = ((k + 1) / SEG) * Math.PI
        const pt = (t: number, r: number) => [Math.cos(t) * r, Math.sin(t) * r + 1.6] as const
        const [l0, u0] = pt(t0, R), [l1, u1] = pt(t1, R), [m0, v0] = pt(t0, R - T), [m1, v1] = pt(t1, R - T)
        const P = (a: number, l: number, u: number) => local(f, a, l, u)
        const quad = (A: V3, B: V3, C: V3, E: V3) => launchPart.quad(A, B, C, E)
        quad(P(-D, l0, u0), P(-D, l1, u1), P(D, l1, u1), P(D, l0, u0))
        quad(P(D, m0, v0), P(D, m1, v1), P(-D, m1, v1), P(-D, m0, v0))
        quad(P(-D, m0, v0), P(-D, m1, v1), P(-D, l1, u1), P(-D, l0, u0))
        quad(P(D, l0, u0), P(D, l1, u1), P(D, m1, v1), P(D, m0, v0))
      }
      // Legs down to the ground either side.
      for (const sg of [1, -1]) box(launchPart, f, -D, D, sg * (R - T), sg * R, -0.6, 1.6)
    }
    // One Slinky Dog train cresting the hill over the rings: the head car in
    // front, four cars wrapped in the coil spring, the tail car behind.
    const HEAD = 595, CAR = 2.3
    const roof = kit.parts.roof
    for (let k = 0; k < 5; k++) {
      const f = kit.at(HEAD - k * CAR - CAR / 2)
      barrel(tan, f, -CAR / 2 + 0.15, CAR / 2 - 0.15, 0.68, 0.75, 0.75)
      hoop(roof, f, -0.45, 0.85, 0.85)
      hoop(roof, f, 0.45, 0.85, 0.85)
    }
    {
      // The head: a big rounded block on a neck, the long snout and muzzle
      // in cream, the ears hanging down either side, the front paws on the
      // nose of the car.
      const f = kit.at(HEAD + 0.5), stone = kit.parts.stone
      box(tan, f, -0.9, 0.2, -0.45, 0.45, 0.6, 1.5)
      barrel(tan, f, -1.0, 0.55, 0.8, 2.05, 0.9)
      box(tan, f, 0.45, 1.2, -0.42, 0.42, 1.65, 2.25)
      box(stone, f, 1.2, 1.75, -0.36, 0.36, 1.55, 2.1)
      for (const sg of [1, -1]) box(tan, f, -0.75, 0.05, sg * 0.8, sg * 1.0, 1.0, 2.5)
      for (const sg of [1, -1]) box(stone, f, 0.35, 1.15, sg * 0.15, sg * 0.65, 0.3, 0.75)
    }
    {
      // The tail: the spring curling up behind the last car, with the tail tip.
      const f = kit.at(HEAD - 5 * CAR - 0.4)
      for (let k = 0; k < 3; k++) hoop(roof, f, -k * 0.35, 0.45 - k * 0.05, 1.1 + k * 0.25, 0.1, 5)
      box(tan, f, -1.3, -0.8, -0.18, 0.18, 1.6, 2.3)
    }
    return [{ part: launchPart, material: LAUNCH }, { part: tan, material: TAN }]
  },
})
