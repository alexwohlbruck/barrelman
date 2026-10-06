/**
 * Fury 325, Carowinds: procedural, CC0-1.0.
 * bun scripts/landmarks/carowinds-fury-325.ts
 *
 * Map frame: x east, y north, z up, metres; bearing 0, so the frame is true
 * north. The origin is the middle of the track's footprint (see ANCHOR), at the
 * lowest ground under it.
 *
 * Plan: the circuit is the chain of OSM `roller_coaster=track` ways named
 * Fury 325 (way/670967416 … way/895332251, all oneway, in the direction of
 * travel), from the station at the west end of the entrance plaza. The
 * transfer spur to the train shed (way/888530850) is left out.
 *
 * Profile (OSM has no heights): from the ride's published layout. A 325 ft
 * (99 m) chain lift at about 35°, an 81° drop of 320 ft curving left into
 * the 190 ft (58 m) barrel turn in the north-east corner, an s-curve east over
 * the north entrance, a left turn south into the 157 ft (48 m) overbanked
 * horseshoe at the south end by the car park, the "hive dive" back under the
 * entrance path, a 101 ft (31 m) banked turn left, a straight run to a 111 ft
 * (34 m) camelback, the low helix inside the barrel turn, and a pair of small
 * hills home, the second passing under the lift. Heights are given above the
 * local ground and lifted onto a coarse terrain grid (Mapzen/AWS Terrarium,
 * open data), since the park falls about 6 m across the footprint.
 *
 * Style: the track is one broad swept ribbon, banked from the train's speed
 * (energy from the crest) and the plan curvature, so the horseshoe stands
 * nearly on edge as it does. Teal running surface and rails over the lime
 * box spine, as the photos show. The white tubular supports are chunky
 * columns, single where the track is low and an A-frame of two splayed legs
 * where it is high. The station is its flat canopy roof on posts.
 */
import { Part, cross, sub, writeGlb, type V3 } from './mesh'
import { PALETTE, finish } from './palette'

// ---------------------------------------------------------------- frame ----
// OSM coordinates are projected about this reference point (equirectangular,
// metres), then shifted so the origin is ANCHOR.
const LAT0 = 35.103, LON0 = -80.943
const MX = 111320 * Math.cos((LAT0 * Math.PI) / 180), MY = 110574
// Centre of the track's bounding box, in the projected frame.
const ANCHOR: [number, number] = [250, 244]

// ------------------------------------------------------------- vectors -----
const add = (a: V3, b: V3): V3 => [a[0] + b[0], a[1] + b[1], a[2] + b[2]]
const mul = (a: V3, k: number): V3 => [a[0] * k, a[1] * k, a[2] * k]
const dot = (a: V3, b: V3) => a[0] * b[0] + a[1] * b[1] + a[2] * b[2]
const unit = (a: V3): V3 => mul(a, 1 / (Math.hypot(...a) || 1))

/** A quad whose winding is chosen so it faces `out`, with per-corner normals. */
function face(p: Part, P: V3[], N: V3[] | V3) {
  const ns = (Array.isArray(N[0]) ? N : [N, N, N, N]) as V3[]
  const f = cross(sub(P[1], P[0]), sub(P[2], P[0]))
  const avg = ns.reduce(add, [0, 0, 0] as V3)
  const [a, b, c, d] = dot(f, avg) >= 0 ? [0, 1, 2, 3] : [0, 3, 2, 1]
  p.tri(P[a], P[b], P[c], undefined, undefined, undefined, [ns[a], ns[b], ns[c]])
  p.tri(P[a], P[c], P[d], undefined, undefined, undefined, [ns[a], ns[c], ns[d]])
}

// ---------------------------------------------------------------- data -----
// The OSM circuit, node by node, in the projected frame, starting at the
// station exit (the first node of way/670967416).
const CHAIN: [number, number][] = [
  [38.8, 272.9], [56.5, 280.2], [62.0, 282.2], [80.3, 288.7], [202.2, 333.9], [215.9, 339.9], [266.7, 357.9], [284.7, 365.0],
  [294.9, 368.9], [301.6, 371.8], [309.3, 376.0], [316.4, 380.5], [319.2, 382.9], [322.0, 385.4], [324.5, 388.1], [327.0, 391.3],
  [329.4, 394.6], [331.8, 398.3], [335.0, 404.1], [337.4, 407.9], [339.2, 411.7], [342.6, 416.7], [345.5, 419.4], [352.2, 424.2],
  [355.7, 425.7], [360.6, 426.9], [363.4, 426.9], [365.7, 426.6], [368.1, 426.1], [370.9, 424.8], [373.3, 423.0], [375.7, 421.2],
  [378.3, 417.7], [379.6, 414.9], [381.0, 411.8], [381.7, 406.9], [382.1, 401.2], [381.6, 394.3], [380.8, 389.2], [379.8, 380.5],
  [379.0, 372.7], [379.3, 361.2], [380.0, 352.8], [381.5, 345.4], [383.4, 338.7], [386.1, 330.6], [388.8, 323.8], [392.3, 317.4],
  [395.4, 312.9], [399.9, 307.3], [405.2, 302.1], [410.2, 298.4], [414.8, 295.8], [421.1, 293.2], [428.1, 291.2], [433.1, 289.4],
  [440.3, 287.8], [447.4, 285.9], [456.2, 283.5], [464.2, 280.5], [468.9, 277.6], [473.1, 274.4], [476.4, 271.0], [479.2, 267.1],
  [481.6, 263.1], [483.2, 258.9], [484.1, 254.3], [484.4, 249.1], [484.0, 245.0], [483.4, 240.8], [482.1, 236.4], [480.2, 231.0],
  [473.1, 214.8], [461.9, 192.3], [455.9, 181.0], [451.9, 172.7], [449.8, 166.7], [448.8, 161.8], [447.8, 154.1], [447.8, 149.2],
  [448.1, 143.6], [449.0, 137.7], [449.9, 133.1], [451.9, 127.0], [454.7, 121.4], [460.0, 112.3], [467.2, 101.9], [472.4, 94.7],
  [475.3, 89.5], [477.2, 85.6], [478.5, 80.8], [478.9, 77.3], [478.5, 74.0], [477.2, 70.1], [475.2, 66.7], [472.9, 64.2],
  [470.4, 62.6], [467.3, 61.4], [463.9, 61.1], [460.9, 61.6], [457.2, 63.1], [454.3, 65.5], [452.4, 68.0], [451.4, 71.1],
  [451.4, 74.9], [452.1, 82.2], [453.1, 88.7], [454.9, 99.5], [457.0, 108.9], [459.4, 117.4], [462.0, 128.5], [463.1, 136.8],
  [464.4, 147.4], [464.6, 149.0], [466.4, 161.5], [466.7, 163.2], [468.9, 179.2], [470.6, 193.6], [471.3, 198.8], [472.0, 203.5],
  [473.4, 209.6], [474.2, 214.7], [474.5, 223.6], [474.1, 227.4], [473.4, 230.5], [472.2, 234.6], [470.9, 237.7], [469.2, 240.6],
  [466.4, 244.2], [464.1, 247.0], [460.4, 250.1], [452.7, 255.0], [446.8, 259.1], [439.7, 263.6], [432.5, 269.1], [426.3, 274.2],
  [419.6, 280.2], [411.9, 286.7], [402.5, 295.0], [396.6, 300.5], [389.4, 307.6], [381.1, 316.2], [374.0, 323.6], [368.0, 329.7],
  [362.6, 334.7], [356.8, 340.0], [351.6, 344.5], [344.9, 349.7], [342.0, 351.7], [333.2, 357.9], [325.8, 363.9], [321.6, 367.2],
  [317.6, 370.9], [314.7, 374.1], [313.0, 376.5], [311.1, 379.4], [309.1, 383.4], [308.2, 386.5], [307.6, 389.8], [307.3, 392.4],
  [307.4, 396.7], [308.0, 400.6], [309.3, 404.9], [312.1, 409.9], [314.6, 413.4], [317.0, 416.0], [320.8, 418.9], [323.3, 420.4],
  [326.9, 422.1], [331.2, 423.3], [334.5, 423.8], [338.4, 423.9], [343.6, 423.6], [348.3, 422.3], [351.7, 421.0], [354.6, 419.3],
  [358.3, 416.5], [361.6, 413.7], [364.3, 410.2], [366.6, 406.5], [368.5, 402.3], [369.7, 399.0], [370.5, 395.4], [370.8, 389.2],
  [370.7, 385.0], [370.1, 380.4], [369.0, 376.8], [367.2, 372.4], [364.9, 367.8], [362.2, 363.8], [358.4, 359.6], [355.2, 356.8],
  [351.9, 354.6], [348.2, 352.7], [339.1, 350.4], [335.2, 350.5], [330.6, 351.0], [326.7, 351.8], [322.6, 352.9], [317.8, 354.6],
  [308.3, 358.9], [293.1, 365.5], [279.8, 370.4], [265.2, 374.7], [258.8, 376.3], [253.1, 378.1], [248.9, 379.2], [245.3, 379.8],
  [241.5, 380.0], [237.2, 379.9], [232.6, 379.0], [229.4, 377.9], [224.7, 375.6], [220.9, 373.3], [218.5, 369.4], [214.1, 363.5],
  [210.0, 355.7], [207.3, 350.6], [203.7, 343.5], [196.8, 332.0], [193.6, 325.4], [191.1, 321.3], [188.4, 316.8], [183.8, 314.2],
  [178.9, 311.1], [171.1, 308.0], [153.5, 300.5], [140.7, 296.0], [121.0, 287.5], [86.9, 274.1], [70.7, 267.5], [30.8, 251.7],
  [26.0, 250.3], [23.2, 250.4], [20.6, 251.4], [17.7, 253.8], [15.9, 256.4], [15.6, 259.6], [16.2, 263.0], [18.3, 265.5],
  [22.3, 266.8],
]
const GROUND_X0 = -20, GROUND_Y0 = 40, GROUND_STEP = 20
// Rows run south to north, columns west to east; metres above the lowest cell.
const GROUND = [
  [10.7, 10.9, 11.2, 10.6, 10.8, 10.9, 11.4, 11.8, 11.8, 10.8, 10.0, 12.2, 12.4, 12.6, 12.6, 12.6, 12.4, 12.5, 12.1, 12.1, 11.6, 11.7, 11.6, 11.5, 10.4, 9.8, 9.3],
  [10.7, 10.5, 10.8, 10.6, 10.5, 10.4, 11.1, 11.5, 12.0, 11.7, 12.3, 12.2, 12.4, 12.5, 12.8, 12.4, 11.4, 11.2, 11.7, 11.9, 11.8, 11.6, 11.3, 10.2, 9.8, 9.5, 9.2],
  [10.2, 10.6, 10.9, 11.1, 10.4, 10.6, 11.3, 11.8, 11.2, 11.5, 11.7, 12.0, 12.9, 12.8, 13.0, 12.7, 11.4, 10.5, 12.0, 12.0, 11.6, 11.1, 10.4, 10.3, 9.7, 9.2, 9.4],
  [10.4, 10.5, 10.7, 10.7, 11.0, 11.4, 11.4, 11.7, 11.6, 11.5, 11.7, 12.3, 12.3, 12.9, 12.9, 13.0, 9.1, 12.3, 12.1, 11.2, 10.8, 10.8, 10.8, 10.4, 9.6, 9.2, 9.1],
  [9.9, 10.1, 10.3, 10.8, 10.8, 10.9, 11.4, 11.3, 11.2, 11.3, 12.1, 11.7, 12.6, 12.8, 12.9, 9.9, 13.0, 13.0, 12.3, 11.9, 11.1, 11.1, 10.5, 10.2, 8.9, 9.0, 9.3],
  [9.2, 9.9, 10.1, 10.7, 10.5, 10.9, 11.4, 10.8, 10.7, 11.0, 11.5, 11.4, 12.1, 12.3, 10.5, 12.5, 13.0, 13.0, 12.6, 12.7, 11.2, 10.9, 10.8, 8.8, 8.8, 9.0, 8.5],
  [9.9, 9.8, 10.2, 10.5, 10.7, 10.5, 10.7, 10.7, 10.4, 10.6, 9.9, 9.7, 10.1, 10.6, 10.1, 12.9, 13.0, 13.1, 12.7, 12.3, 11.6, 11.1, 9.5, 9.1, 9.2, 8.6, 8.2],
  [9.5, 9.5, 9.6, 9.0, 9.0, 10.5, 10.5, 10.3, 9.6, 7.1, 6.2, 6.4, 6.6, 8.8, 11.0, 12.1, 12.8, 12.8, 12.4, 12.1, 11.7, 10.3, 9.5, 9.6, 9.2, 8.0, 7.9],
  [8.7, 8.7, 9.0, 9.1, 9.1, 9.2, 10.3, 9.9, 10.3, 9.1, 7.5, 6.3, 6.4, 8.0, 9.4, 10.1, 11.1, 12.8, 12.3, 11.8, 11.9, 10.5, 9.8, 9.7, 9.5, 8.3, 7.3],
  [8.6, 8.7, 8.8, 9.5, 9.4, 9.8, 11.0, 10.4, 8.0, 8.3, 7.2, 6.2, 6.5, 7.0, 8.2, 9.2, 10.6, 12.5, 11.1, 11.7, 11.9, 10.5, 10.0, 10.0, 9.6, 8.6, 7.5],
  [8.8, 8.8, 8.6, 8.7, 8.6, 8.8, 8.3, 10.4, 8.7, 7.1, 7.0, 6.4, 6.1, 6.4, 7.5, 8.7, 9.9, 11.5, 11.0, 10.7, 10.5, 10.6, 10.2, 9.9, 9.3, 8.9, 7.8],
  [7.9, 8.5, 8.7, 8.5, 8.6, 8.3, 8.3, 9.5, 8.9, 6.3, 6.1, 5.1, 6.0, 5.9, 7.1, 8.8, 10.6, 12.4, 11.9, 10.3, 10.4, 10.4, 9.9, 9.8, 9.6, 8.9, 8.0],
  [8.6, 8.0, 8.7, 8.7, 8.4, 8.4, 8.1, 7.7, 10.1, 8.0, 8.2, 6.5, 6.4, 5.6, 6.8, 8.6, 11.4, 13.6, 10.4, 10.1, 10.3, 10.3, 10.4, 9.7, 9.4, 8.9, 8.6],
  [8.8, 8.1, 8.1, 8.0, 8.2, 8.1, 8.0, 7.8, 7.5, 6.9, 6.3, 6.4, 6.8, 7.1, 8.0, 8.6, 9.6, 12.3, 9.8, 9.8, 10.4, 10.4, 10.5, 10.5, 9.4, 9.2, 9.0],
  [7.5, 6.9, 6.8, 7.8, 8.0, 8.1, 8.0, 7.5, 6.9, 6.7, 7.0, 6.9, 7.2, 7.2, 7.4, 8.9, 9.4, 9.6, 9.5, 9.9, 10.3, 10.5, 10.4, 10.0, 9.6, 9.3, 9.2],
  [6.3, 5.7, 4.8, 4.8, 7.8, 8.0, 7.8, 7.5, 6.6, 6.5, 6.8, 7.2, 7.4, 7.6, 7.7, 7.9, 8.0, 9.4, 10.1, 10.5, 10.5, 10.7, 12.1, 10.5, 9.9, 9.8, 9.7],
  [5.2, 4.6, 3.6, 2.9, 3.0, 5.4, 7.7, 7.3, 4.7, 6.7, 6.7, 7.1, 7.6, 7.8, 8.0, 8.1, 8.3, 8.6, 8.5, 9.0, 10.1, 10.2, 10.4, 10.5, 10.2, 10.1, 10.2],
  [3.6, 3.3, 2.7, 2.0, 1.9, 1.8, 3.0, 5.7, 6.1, 6.6, 7.2, 7.5, 7.7, 7.9, 8.0, 7.8, 7.9, 8.2, 8.6, 8.7, 8.8, 9.0, 10.0, 10.6, 10.2, 10.0, 10.0],
  [2.5, 2.3, 1.8, 1.5, 1.5, 1.9, 2.6, 3.0, 3.2, 4.9, 6.3, 9.4, 9.7, 8.2, 8.1, 7.9, 7.6, 7.8, 8.1, 8.3, 9.7, 10.3, 9.8, 9.6, 9.8, 10.5, 11.3],
  [0.9, 1.0, 1.3, 1.3, 1.4, 1.9, 2.5, 2.9, 3.6, 3.9, 5.7, 7.4, 8.6, 10.4, 9.9, 8.2, 7.9, 7.5, 7.5, 7.8, 8.2, 8.6, 9.8, 12.7, 12.6, 11.9, 12.0],
  [0.0, 0.9, 1.5, 1.5, 1.5, 1.7, 2.1, 2.4, 2.8, 3.6, 4.9, 6.1, 7.9, 9.4, 10.3, 10.2, 8.8, 7.8, 7.2, 7.3, 7.6, 8.4, 9.8, 11.2, 13.0, 15.8, 14.3],
]


/** Ground height (m above the lowest cell) at a projected point, bilinear. */
function groundRaw(x: number, y: number) {
  const fx = Math.max(0, Math.min((x - GROUND_X0) / GROUND_STEP, GROUND[0].length - 1.001))
  const fy = Math.max(0, Math.min((y - GROUND_Y0) / GROUND_STEP, GROUND.length - 1.001))
  const i = Math.floor(fx), j = Math.floor(fy), u = fx - i, v = fy - j
  const R = GROUND
  return (R[j][i] * (1 - u) + R[j][i + 1] * u) * (1 - v) + (R[j + 1][i] * (1 - u) + R[j + 1][i + 1] * u) * v
}

// Heights above the local ground, keyed by distance along the OSM polyline
// (metres from the first node), from the published element heights.
const PROFILE: [number, number][] = [
  [0, 4], [44, 4.5], [118, 5.5], [138, 13],           // station, transfer, foot of the lift
  [262, 96], [276, 99.5], [288, 99.5],                // lift and crest
  [296, 97], [302, 90], [310, 64], [320, 24], [332, 7], [346, 3.5], // the 81° drop
  [364, 16], [382, 46], [398, 58], [414, 55], [434, 40], [456, 18], [476, 8],   // barrel turn
  [500, 10], [530, 20], [560, 16],                     // s-curve over the north entrance
  [592, 30], [622, 38], [646, 35],                     // banked turn left, south
  [680, 16], [712, 5], [750, 8], [762, 9],                      // down past the hive dive
  [786, 37], [802, 47], [815, 48], [830, 46], [856, 30], // the overbanked horseshoe
  [874, 28], [890, 21], [904, 8], [915, 1.2], [925, 1.2], [945, 5],             // hive dive under the entrance path
  [978, 17], [1012, 31], [1040, 27], [1080, 13], [1105, 10], // second banked turn
  [1150, 34], [1192, 17], [1225, 11],                  // 111 ft camelback
  [1260, 13], [1292, 18], [1330, 14], [1400, 7], [1440, 7], // helix inside the barrel turn
  [1482, 20], [1530, 7], [1572, 15], [1615, 6.5],      // two small hills, the second under the lift
  [1660, 6], [1740, 6], [1770, 5],                     // brakes and the turn into the station
]

// ------------------------------------------------------------ the spline ---
const W = 4.0         // running surface width
const RAIL = 0.7      // depth of the rail band, the running surface's colour
const DEPTH = 2.3     // overall depth, to the bottom of the box spine
const SPINE = 1.8     // width of the spine's underside

const pts = CHAIN.map(([x, y]) => [x - ANCHOR[0], y - ANCHOR[1]] as [number, number])
const n = pts.length
// Polyline distance at each OSM node, which PROFILE is keyed by.
const polyS = [0]
for (let i = 1; i < n; i++) polyS.push(polyS[i - 1] + Math.hypot(pts[i][0] - pts[i - 1][0], pts[i][1] - pts[i - 1][1]))
const polyTotal = polyS[n - 1] + Math.hypot(pts[0][0] - pts[n - 1][0], pts[0][1] - pts[n - 1][1])

// Dense centripetal Catmull–Rom through the plan points (closed).
const dense: { x: number; y: number; knot: number }[] = []
for (let i = 0; i < n; i++) {
  const p0 = pts[(i - 1 + n) % n], p1 = pts[i], p2 = pts[(i + 1) % n], p3 = pts[(i + 2) % n]
  const d = (a: number[], b: number[]) => Math.max(Math.hypot(a[0] - b[0], a[1] - b[1]) ** 0.5, 1e-3)
  const t0 = 0, t1 = t0 + d(p0, p1), t2 = t1 + d(p1, p2), t3 = t2 + d(p2, p3)
  const SUB = Math.max(4, Math.ceil(Math.hypot(p2[0] - p1[0], p2[1] - p1[1]) / 0.5))
  for (let k = 0; k < SUB; k++) {
    const t = t1 + ((t2 - t1) * k) / SUB
    const L = (a: number[], b: number[], ta: number, tb: number) =>
      [0, 1].map((c) => ((tb - t) * a[c] + (t - ta) * b[c]) / (tb - ta))
    const A1 = L(p0, p1, t0, t1), A2 = L(p1, p2, t1, t2), A3 = L(p2, p3, t2, t3)
    const B1 = L(A1, A2, t0, t2), B2 = L(A2, A3, t1, t3)
    const C = L(B1, B2, t1, t2)
    dense.push({ x: C[0], y: C[1], knot: k === 0 ? i : -1 })
  }
}
const dS = [0]
for (let i = 1; i <= dense.length; i++) {
  const a = dense[i - 1], b = dense[i % dense.length]
  dS.push(dS[i - 1] + Math.hypot(b.x - a.x, b.y - a.y))
}
const TOTAL = dS[dense.length]
const knotS = pts.map((_, i) => dS[dense.findIndex((p) => p.knot === i)])
/** Spline distance for a polyline distance, through the shared nodes. */
function splineS(ps: number) {
  const ks = [...knotS, TOTAL], ps0 = [...polyS, polyTotal]
  let j = 0
  while (j < ks.length - 2 && ps0[j + 1] <= ps) j++
  return ks[j] + ((ps - ps0[j]) / (ps0[j + 1] - ps0[j])) * (ks[j + 1] - ks[j])
}
function planAt(s: number): [number, number] {
  s = ((s % TOTAL) + TOTAL) % TOTAL
  let lo = 0, hi = dense.length
  while (hi - lo > 1) {
    const mid = (lo + hi) >> 1
    if (dS[mid] <= s) lo = mid
    else hi = mid
  }
  const a = dense[lo], b = dense[(lo + 1) % dense.length], t = (s - dS[lo]) / (dS[lo + 1] - dS[lo] || 1)
  return [a.x + (b.x - a.x) * t, a.y + (b.y - a.y) * t]
}

// The model's datum: the lowest ground under the track and the station.
const ground0 = (x: number, y: number) => groundRaw(x + ANCHOR[0], y + ANCHOR[1])
let DATUM = Infinity
for (const p of dense) DATUM = Math.min(DATUM, ground0(p.x, p.y))
const ground = (x: number, y: number) => ground0(x, y) - DATUM

// Height knots, lifted onto the ground, then a closed monotone cubic.
const zk = PROFILE.map(([ps, z]) => {
  const s = splineS(ps), [x, y] = planAt(s)
  return [s, z + ground(x, y)] as [number, number]
})
const ext = [-1, 0, 1].flatMap((w) => zk.map(([ks, z]) => [ks + w * TOTAL, z] as [number, number]))
function heightAt(s: number) {
  s = ((s % TOTAL) + TOTAL) % TOTAL
  const j = ext.findIndex(([ks], k) => ks <= s && ext[k + 1][0] > s)
  const [sm, zm] = ext[j - 1], [s0, z0] = ext[j], [s1, z1] = ext[j + 1], [s2, z2] = ext[j + 2]
  const d0 = (z1 - z0) / (s1 - s0), dm = (z0 - zm) / (s0 - sm), d2 = (z2 - z1) / (s2 - s1)
  const slope = (a: number, b: number) => (a * b <= 0 ? 0 : (2 * a * b) / (a + b))
  const m0 = slope(dm, d0), m1 = slope(d0, d2)
  const h = s1 - s0, t = (s - s0) / h
  const h00 = 2 * t ** 3 - 3 * t ** 2 + 1, h10 = t ** 3 - 2 * t ** 2 + t
  const h01 = -2 * t ** 3 + 3 * t ** 2, h11 = t ** 3 - t ** 2
  return h00 * z0 + h10 * h * m0 + h01 * z1 + h11 * h * m1
}

// ------------------------------------------------- frames along the track --
// Fine samples first (1 m), each with its own banked frame.
const STEP = 1
const N = Math.round(TOTAL / STEP)
const CREST_S = splineS(280)
const LIFT_END = splineS(282)
type Sample = { p: V3; t: V3; left: V3; up: V3; s: number }
const heading: number[] = [], zs: number[] = []
for (let i = 0; i < N; i++) {
  const s = (i * TOTAL) / N
  const [x0, y0] = planAt(s - 1), [x1, y1] = planAt(s + 1)
  heading.push(Math.atan2(y1 - y0, x1 - x0))
  zs.push(heightAt(s))
}
const zCrest = heightAt(CREST_S)
// Bank angle: atan(v²κ / g), v² from the drop below the crest with some loss.
// The chain lift, the station and the brakes run level.
const rawBank = heading.map((_, i) => {
  const s = (i * TOTAL) / N
  let d = heading[(i + 8) % N] - heading[(i - 8 + N) % N]
  while (d > Math.PI) d -= 2 * Math.PI
  while (d < -Math.PI) d += 2 * Math.PI
  const k = d / (16 * STEP)
  if (s < LIFT_END || s > splineS(1640)) return 0
  const v2 = 2 * 9.81 * Math.max(0, zCrest - zs[i]) * 0.8
  return Math.atan((v2 * k) / 9.81)
})
const bank = rawBank.map((_, i) => {
  let sum = 0, w = 0
  for (let k = -12; k <= 12; k++) {
    const wk = 13 - Math.abs(k)
    sum += rawBank[(i + k + N) % N] * wk
    w += wk
  }
  return Math.max(-1.6, Math.min(1.6, sum / w))
})
const fine: Sample[] = []
for (let i = 0; i < N; i++) {
  const s = (i * TOTAL) / N
  const [x, y] = planAt(s)
  const [xa, ya] = planAt(s + 0.8), [xb, yb] = planAt(s - 0.8)
  const t = unit([xa - xb, ya - yb, heightAt(s + 0.8) - heightAt(s - 0.8)])
  const up0 = unit(sub([0, 0, 1], mul(t, t[2])))
  const left0 = cross(up0, t)
  const b = bank[i], c = Math.cos(b), sn = Math.sin(b)
  const left = unit(sub(mul(left0, c), mul(up0, sn)))
  const up = unit(add(mul(up0, c), mul(left0, sn)))
  fine.push({ p: [x, y, zs[i]], t, left, up, s })
}
// Keep a sample wherever the track has turned, pitched or rolled enough
// since the last one kept, and at least every MAX_STEP metres.
const MAX_STEP = 12, TURN = Math.cos((9 * Math.PI) / 180), ROLL = Math.cos((12 * Math.PI) / 180)
const samples: Sample[] = [fine[0]]
for (let i = 1; i < N; i++) {
  const last = samples[samples.length - 1], q = fine[i]
  const dist = Math.hypot(...sub(q.p, last.p))
  if (dist >= MAX_STEP || dot(q.t, last.t) < TURN || dot(q.up, last.up) < ROLL) samples.push(q)
}
const M = samples.length

// ------------------------------------------------------------- the track ---
const deck = new Part(), spine = new Part(), white = new Part(), roofPart = new Part(), trimPart = new Part()

function section(q: Sample) {
  const { p, left, up } = q
  const at = (l: number, u: number) => add(p, add(mul(left, l), mul(up, u)))
  return {
    L: at(W / 2, 0), R: at(-W / 2, 0),
    Lr: at(W / 2 - 0.15, -RAIL), Rr: at(-W / 2 + 0.15, -RAIL),
    Lb: at(SPINE / 2, -DEPTH), Rb: at(-SPINE / 2, -DEPTH),
  }
}
for (let i = 0; i < M; i++) {
  const a = samples[i], b = samples[(i + 1) % M]
  const A = section(a), B = section(b)
  const sideN = (q: Sample, sgn: number) => unit(add(mul(q.left, sgn), mul(q.up, -0.25)))
  face(deck, [A.L, A.R, B.R, B.L], [a.up, a.up, b.up, b.up])
  face(deck, [A.L, B.L, B.Lr, A.Lr], [sideN(a, 1), sideN(b, 1), sideN(b, 1), sideN(a, 1)])
  face(deck, [A.R, A.Rr, B.Rr, B.R], [sideN(a, -1), sideN(a, -1), sideN(b, -1), sideN(b, -1)])
  const lN = (q: Sample, sgn: number) => unit(add(mul(q.left, sgn), mul(q.up, -0.5)))
  face(spine, [A.Lr, B.Lr, B.Lb, A.Lb], [lN(a, 1), lN(b, 1), lN(b, 1), lN(a, 1)])
  face(spine, [A.Rr, A.Rb, B.Rb, B.Rr], [lN(a, -1), lN(a, -1), lN(b, -1), lN(b, -1)])
  const da = mul(a.up, -1), db = mul(b.up, -1)
  face(spine, [A.Lb, B.Lb, B.Rb, A.Rb], [da, db, db, da])
}

// ------------------------------------------------------------- supports ----
/** A six-sided tube from a to b, radius r, smooth-shaded, open ends. */
function tube(p: Part, a: V3, b: V3, r: number) {
  const ax = unit(sub(b, a))
  const ref: V3 = Math.abs(ax[2]) < 0.9 ? [0, 0, 1] : [1, 0, 0]
  const u = unit(cross(ax, ref)), v = cross(ax, u)
  const K = 6
  const ring = (c: V3, k: number) => {
    const ang = (k / K) * 2 * Math.PI
    const nrm = add(mul(u, Math.cos(ang)), mul(v, Math.sin(ang)))
    return { P: add(c, mul(nrm, r)), N: nrm }
  }
  for (let k = 0; k < K; k++) {
    const a0 = ring(a, k), a1 = ring(a, k + 1), b0 = ring(b, k), b1 = ring(b, k + 1)
    face(p, [a0.P, a1.P, b1.P, b0.P], [a0.N, a1.N, b1.N, b0.N])
  }
}

/** Closest distance from point q to any track sample more than `skip` m along from s0. */
function clearOf(q: V3, s0: number, skip = 14) {
  let best = Infinity
  for (const f of samples) {
    const ds = Math.abs(f.s - s0)
    if (Math.min(ds, TOTAL - ds) < skip) continue
    best = Math.min(best, Math.hypot(...sub(f.p, q)))
  }
  return best
}
/** True if the straight leg a→b passes clear of every other stretch of track. */
function legClear(a: V3, b: V3, s0: number) {
  const L = Math.hypot(...sub(b, a))
  for (let d = 0; d <= L; d += 1.5) {
    const q = add(a, mul(sub(b, a), d / L))
    if (clearOf(q, s0) < W * 0.75 + 0.9) return false
  }
  return true
}

const SUPPORT_EVERY = 17
const supports: { s: number; legs: number }[] = []
const STATION_S = [splineS(1700), splineS(60)] as const
for (let s = 4; s < TOTAL - 4; s += SUPPORT_EVERY) {
  // Try the planned spot, then a few metres either side.
  for (const off of [0, 4, -4, 8, -8]) {
    const i = Math.round((s + off) / STEP) % N
    const q = fine[i]
    const zr = q.p[2] - ground(q.p[0], q.p[1])
    if (zr < 2.2) break
    const top = add(q.p, mul(q.up, -DEPTH + 0.3))
    // Low track and the lift: one column straight down. High track: an
    // A-frame splayed across the track.
    const across = unit([q.left[0], q.left[1], 0])
    const legs: [V3, V3][] = []
    if (zr < 16) {
      const foot: V3 = [top[0], top[1], ground(top[0], top[1]) - 1]
      legs.push([foot, top])
    } else {
      const spread = Math.min(0.3 * zr, 22)
      for (const sg of [1, -1]) {
        const fx = top[0] + across[0] * spread * sg, fy = top[1] + across[1] * spread * sg
        legs.push([[fx, fy, ground(fx, fy) - 1], top])
      }
    }
    if (!legs.every(([a, b]) => legClear(a, b, q.s))) continue
    const r = zr < 16 ? 0.95 : zr < 50 ? 1.1 : 1.3
    for (const [a, b] of legs) tube(white, a, b, r)
    supports.push({ s: q.s, legs: legs.length })
    break
  }
}

// -------------------------------------------------------------- station ----
// The canopy over the platform (way/543823785), a flat roof on white posts.
const STATION: [number, number][] = [
  [52.8, 284.4], [44.3, 281.1], [38.9, 278.8], [38.8, 272.9], [38.5, 272.0], [38.7, 271.6], [39.9, 269.4],
  [37.8, 265.4], [38.3, 264.9], [40.6, 262.1], [44.3, 262.8], [46.9, 263.2], [47.8, 268.1], [48.6, 268.4],
  [49.2, 268.6], [57.9, 271.8], [57.8, 273.9], [57.4, 279.1], [56.5, 280.2],
]
const ROOF_Z = 8.5, ROOF_T = 0.9
{
  const poly = STATION.map(([x, y]) => [x - ANCHOR[0], y - ANCHOR[1]] as [number, number])
  const area = poly.reduce((s, p, i) => s + p[0] * poly[(i + 1) % poly.length][1] - poly[(i + 1) % poly.length][0] * p[1], 0)
  const ring = area < 0 ? [...poly].reverse() : poly
  const g = Math.max(...ring.map(([x, y]) => ground(x, y)))
  const z0 = g + ROOF_Z - ROOF_T, z1 = g + ROOF_Z
  for (let i = 0; i < ring.length; i++) {
    const a = ring[i], b = ring[(i + 1) % ring.length]
    const o = unit([b[1] - a[1], -(b[0] - a[0]), 0])
    face(trimPart, [[a[0], a[1], z0], [b[0], b[1], z0], [b[0], b[1], z1], [a[0], a[1], z1]], o)
  }
  // Ear-clip the roof and its soffit.
  const idx = ring.map((_, i) => i)
  const cz = (o: number[], a: number[], b: number[]) => (a[0] - o[0]) * (b[1] - o[1]) - (a[1] - o[1]) * (b[0] - o[0])
  let guard = 0
  while (idx.length > 3 && guard++ < 1000) {
    for (let i = 0; i < idx.length; i++) {
      const ia = idx[(i - 1 + idx.length) % idx.length], ib = idx[i], ic = idx[(i + 1) % idx.length]
      const A = ring[ia], B = ring[ib], C = ring[ic]
      if (cz(A, B, C) <= 0) continue
      if (!idx.every((j) => j === ia || j === ib || j === ic ||
        !(cz(A, B, ring[j]) >= 0 && cz(B, C, ring[j]) >= 0 && cz(C, A, ring[j]) >= 0))) continue
      const T = [A, B, C]
      roofPart.tri(...(T.map(([x, y]) => [x, y, z1]) as [V3, V3, V3]), undefined, undefined, undefined, [[0, 0, 1], [0, 0, 1], [0, 0, 1]])
      trimPart.tri(...(T.reverse().map(([x, y]) => [x, y, z0]) as [V3, V3, V3]), undefined, undefined, undefined, [[0, 0, -1], [0, 0, -1], [0, 0, -1]])
      idx.splice(i, 1)
      break
    }
  }
  const [a, b, c] = idx.map((i) => ring[i])
  roofPart.tri([a[0], a[1], z1], [b[0], b[1], z1], [c[0], c[1], z1], undefined, undefined, undefined, [[0, 0, 1], [0, 0, 1], [0, 0, 1]])
  for (const [x, y] of [[41, 264], [56.5, 272.5], [40, 277.5], [53, 282.5]] as [number, number][]) {
    const px = x - ANCHOR[0], py = y - ANCHOR[1]
    tube(trimPart, [px, py, ground(px, py) - 1], [px, py, z0], 0.35)
  }
}

// ---------------------------------------------------------------- output ---
// Colours from the photos, pulled to the palette's lightness: the teal-blue
// running surface and rails, the lime-yellow box spine, white supports.
const parts = [
  { part: deck, material: finish('fury-teal', 0x5aaac8) },
  { part: spine, material: finish('fury-lime', 0xc6d566) },
  { part: white, material: PALETTE.trim },
  { part: trimPart, material: PALETTE.stone },
  { part: roofPart, material: PALETTE.roof },
]
const triangles = parts.reduce((sum, { part }) => sum + part.triangles, 0)
let maxZ = 0, steep = 0, len3 = 0
for (let i = 0; i < N; i++) {
  const a = fine[i].p, b = fine[(i + 1) % N].p
  maxZ = Math.max(maxZ, a[2])
  len3 += Math.hypot(...sub(b, a))
  steep = Math.max(steep, (Math.atan2(a[2] - b[2], Math.hypot(b[0] - a[0], b[1] - a[1])) * 180) / Math.PI)
}
console.log(`track ${len3.toFixed(0)} m (${TOTAL.toFixed(0)} m in plan), ${M} sections, crest ${maxZ.toFixed(1)} m, steepest ${steep.toFixed(0)}°, max bank ${(Math.max(...bank.map(Math.abs)) * 180 / Math.PI).toFixed(0)}°`)
console.log(`${supports.length} supports, ${supports.reduce((s, x) => s + x.legs, 0)} legs; datum ${DATUM.toFixed(1)} m on the grid`)
console.log(parts.map(({ part, material }) => `${material.name} ${part.triangles}`).join(', '))
if (triangles > 6500) throw new Error(`Triangle budget exceeded: ${triangles}`)
const glb = writeGlb('Fury 325', parts, {
  license: 'CC0-1.0', frame: 'Y up, -Z north, +X east, metres; origin at ground anchor',
  bearing: 0, elevation: 0, height: 99, trackLength: 2012,
})
if (glb.length > 256000) throw new Error(`File budget exceeded: ${glb.length}`)
const out = new URL('../../landmarks/models/carowinds-fury-325.glb', import.meta.url).pathname
await Bun.write(out, glb)
console.log(`${out}: ${triangles} triangles, ${glb.length} bytes`)
console.log(`anchor ${(LON0 + ANCHOR[0] / MX).toFixed(7)}, ${(LAT0 + ANCHOR[1] / MY).toFixed(7)}`)

// Clearance report: stretches of track within a deck's width of each other
// in plan with less than 4 m between them in height.
if (process.env.CLEARANCE) {
  for (let i = 0; i < N; i += 2) for (let k = i + 20; k < N; k += 2) {
    const ds = Math.abs(fine[i].s - fine[k].s)
    if (Math.min(ds, TOTAL - ds) < 20) continue
    const a = fine[i].p, b = fine[k].p
    const d = Math.hypot(a[0] - b[0], a[1] - b[1]), dz = Math.abs(a[2] - b[2])
    if (d < W + 1 && dz < 4.5)
      console.log(`clash s=${fine[i].s.toFixed(0)} & ${fine[k].s.toFixed(0)} at (${(a[0] + ANCHOR[0]).toFixed(0)}, ${(a[1] + ANCHOR[1]).toFixed(0)}) plan ${d.toFixed(1)} dz ${dz.toFixed(1)}`)
  }
}
if (process.env.PROFILE_DUMP) {
  for (let i = 0; i < N; i += 20) console.log(`s ${fine[i].s.toFixed(0)} (${(fine[i].p[0] + ANCHOR[0]).toFixed(0)}, ${(fine[i].p[1] + ANCHOR[1]).toFixed(0)}) z ${fine[i].p[2].toFixed(1)} g ${ground(fine[i].p[0], fine[i].p[1]).toFixed(1)} bank ${(bank[i] * 180 / Math.PI).toFixed(0)}`)
}
