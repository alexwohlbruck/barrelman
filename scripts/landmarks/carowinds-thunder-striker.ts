/**
 * Thunder Striker (formerly Intimidator), Carowinds: procedural, CC0-1.0.
 * bun scripts/landmarks/carowinds-thunder-striker.ts   (REPORT=1 for the element table)
 *
 * Map frame: x east, y north, z up, metres; bearing 0, so the frame is true
 * north. The origin is the middle of the track's footprint (see ANCHOR), at the
 * lowest ground under it. Built with ./coaster-kit.ts.
 *
 * Plan: the circuit is the chain of OSM `roller_coaster=track` ways from the
 * station in Thunder Road (way/669991776, way/890902047 … way/890901418, all
 * oneway, in the direction of travel). The spur into the train barn
 * (way/890901693) is left out.
 *
 * Profile: a B&M hyper of 1,620 m. Element heights are the published ones
 * (RCDB 8588, Wikipedia: a 232 ft lift with a 211 ft drop at 74°, then hills
 * of 178, 151, 105, 90, 62, 52 and 48 ft, and the 121 ft hammerhead turn).
 * Where each sits along the track was measured from photos with known camera
 * positions, by casting each crest back through the fitted camera onto the
 * OSM plan; some measured crests came out a few metres off the published
 * heights and were nudged to fit the photos. Positions are metres along the
 * OSM polyline (1,398 m round) and % of it:
 *
 *   lift crest, 71 m          s 100 (7%), a 45° lift straight off the station
 *   first drop, 74°           foot at s 177 (13%), 7 m up, so it falls 211 ft
 *   camelback, 55 m           s 251 (18%), at the head of the sharp right turn
 *   left turn                 s 345 (25%), low
 *   camelback, 48 m           s 421 (30%), outbound
 *   hammerhead, 40 m          s 594–606 (42–43%), a long straight ramp up
 *   camelback, 35 m           s 730 (52%), inbound beside the outbound track
 *   camelback, 31 m           s 841 (60%)
 *   mid-course brakes, 25 m   s 950–1004 (68–72%)
 *   hill, 21 m                s 1071 (77%), into the diving spiral
 *   diving spiral, foot       s 1185 (85%)
 *   hills, 17.5 m and 16 m    s 1225 (88%), s 1283 (92%)
 *   final brakes              from s 1314 (94%)
 *
 * Photos used for matching (Wikimedia Commons, ThrillZing, October 2024,
 * CC BY 4.0): "Thunder Striker's red track and airtime hills across the empty
 * Carowinds parking lot", "Thunder Striker's red hills stretching across the
 * parking lot", "Thunder Striker's full red layout from the parking lot,
 * Fury 325 at the right", "Thunder Striker's lift hill rising from the
 * station with a train on the hills".
 *
 * Style: one broad swept ribbon, banked from the train's speed and the plan
 * curvature. Red running surface and rails over a red box spine a shade
 * deeper, on pale grey tubular supports: single columns where the track is
 * low, A-frames of two splayed legs where it is high. The station is its flat
 * canopy roof on posts.
 */
import { buildCoaster } from './coaster-kit'

// The OSM circuit, node by node, in the park frame (see coaster-kit), starting
// at the station exit (the first node of way/669991776).
const CHAIN: [number, number][] = [
  [321.3, -0.1], [327.2, -8.6], [371.9, -73.4], [397.5, -110.5], [401.1, -115.6], [407.0, -123.8], [415.5, -135.6], [437.8, -166.8],
  [456.1, -192.4], [461.0, -197.9], [465.5, -203.1], [468.1, -206.3], [470.4, -210.6], [471.7, -214.4], [470.6, -220.2], [467.2, -223.3],
  [464.2, -225.2], [461.4, -226.1], [458.7, -226.9], [455.1, -226.8], [451.0, -226.3], [446.6, -225.7], [442.9, -225.4], [437.8, -225.5],
  [433.3, -226.1], [429.2, -227.2], [423.6, -229.2], [418.2, -232.1], [413.4, -235.3], [409.4, -238.6], [405.3, -243.0], [400.8, -248.7],
  [387.4, -266.5], [370.4, -288.2], [352.0, -311.8], [336.2, -333.2], [314.7, -361.2], [295.9, -385.9], [288.7, -396.1], [285.5, -399.9],
  [281.8, -403.1], [278.9, -405.2], [274.2, -407.4], [266.4, -410.5], [261.4, -412.4], [257.3, -414.4], [254.4, -416.7], [251.7, -419.6],
  [250.3, -422.4], [249.6, -425.3], [249.3, -428.2], [249.9, -431.2], [251.0, -433.6], [253.0, -436.3], [255.2, -437.9], [257.9, -439.0],
  [261.6, -439.1], [264.3, -438.6], [267.0, -437.2], [269.3, -435.3], [271.5, -433.0], [274.0, -429.3], [275.1, -425.8], [275.6, -422.4],
  [276.0, -417.3], [277.0, -410.7], [278.3, -406.5], [278.9, -405.2], [280.6, -401.6], [282.7, -397.8], [284.7, -394.5], [293.9, -382.6],
  [306.9, -366.0], [322.9, -344.3], [349.1, -310.5], [378.4, -272.8], [402.4, -240.6], [428.5, -207.6], [434.8, -199.6], [439.1, -193.4],
  [441.6, -188.6], [443.1, -184.1], [444.4, -177.6], [444.4, -173.5], [442.5, -169.7], [440.2, -164.5], [436.7, -154.4], [434.4, -145.1],
  [429.5, -127.0], [425.2, -111.5], [421.7, -95.6], [417.7, -77.8], [413.2, -64.0], [407.5, -46.8], [403.8, -36.0], [403.7, -32.0],
  [404.8, -28.0], [406.2, -25.3], [408.3, -23.0], [410.0, -21.4], [412.6, -19.8], [415.8, -18.5], [419.1, -17.8], [423.5, -17.7],
  [427.6, -18.4], [431.0, -19.5], [434.6, -21.4], [437.9, -24.3], [440.7, -27.9], [443.0, -32.4], [444.1, -36.7], [444.3, -41.3],
  [443.6, -45.4], [442.0, -49.2], [439.3, -53.3], [436.6, -55.9], [433.0, -57.5], [429.6, -58.3], [426.4, -58.5], [423.0, -57.5],
  [419.2, -54.9], [408.1, -43.3], [388.1, -24.8], [356.0, 3.2], [318.4, 37.4], [309.0, 45.6], [306.4, 48.0], [302.7, 49.3],
  [299.7, 49.3], [296.7, 48.0], [295.1, 45.9], [294.1, 42.6], [294.4, 39.2], [296.4, 35.9], [303.9, 25.1], [307.5, 20.2],
]
// Terrain (Mapzen/AWS Terrarium, open data), 20 m cells from (220, -460); rows
// south to north, columns west to east; metres above 190.1 m.
const GROUND = [
  [2.1, 1.9, 1.6, 1.1, 0.7, 0.5, 0.2, 0.0, 0.0, 0.2, 0.2, 0.4, 0.7, 0.9, 1.1],
  [2.4, 2.1, 1.8, 1.4, 1.1, 0.8, 0.5, 0.3, 0.3, 0.4, 0.6, 0.8, 1.1, 1.3, 1.5],
  [3.9, 3.6, 2.5, 1.8, 1.5, 1.1, 0.8, 0.7, 0.6, 0.9, 1.0, 1.2, 1.4, 1.6, 1.8],
  [4.4, 4.3, 3.8, 2.1, 1.9, 1.4, 1.1, 1.0, 1.0, 1.2, 1.3, 1.5, 1.7, 2.0, 2.2],
  [5.6, 5.0, 4.6, 3.9, 2.2, 1.8, 1.5, 1.3, 1.4, 1.6, 1.7, 1.9, 2.1, 2.4, 2.6],
  [8.2, 7.1, 6.2, 5.0, 3.2, 2.1, 1.8, 1.7, 1.8, 2.0, 2.2, 2.3, 2.6, 2.7, 2.8],
  [10.1, 8.7, 7.8, 6.1, 5.0, 2.6, 2.3, 2.1, 2.2, 2.4, 2.6, 2.7, 2.9, 2.9, 2.7],
  [10.0, 9.5, 9.0, 7.3, 6.2, 4.6, 2.6, 2.5, 2.5, 2.8, 3.0, 3.1, 3.1, 2.8, 2.5],
  [10.7, 9.6, 9.2, 8.4, 6.3, 5.3, 4.2, 2.8, 2.9, 3.1, 3.3, 3.3, 3.1, 2.7, 2.3],
  [11.3, 10.2, 9.6, 8.6, 6.5, 5.9, 5.4, 3.7, 3.2, 3.3, 3.4, 3.1, 2.8, 2.4, 2.1],
  [12.3, 10.8, 9.9, 9.4, 7.4, 6.1, 5.8, 5.0, 3.5, 3.5, 3.2, 3.0, 2.7, 2.2, 1.8],
  [12.0, 11.3, 10.3, 9.4, 7.8, 6.4, 5.8, 5.9, 5.2, 3.4, 3.3, 3.0, 2.8, 2.2, 1.8],
  [10.7, 10.7, 10.7, 8.9, 8.7, 6.8, 6.4, 5.8, 5.6, 4.7, 3.3, 3.2, 2.7, 2.4, 2.1],
  [9.7, 9.3, 9.8, 8.9, 8.1, 6.8, 6.5, 6.3, 5.7, 5.6, 4.0, 3.3, 3.0, 2.5, 2.5],
  [9.4, 9.2, 9.0, 8.5, 7.3, 7.0, 6.6, 6.4, 5.5, 5.4, 5.4, 3.4, 3.2, 2.9, 2.9],
  [8.7, 8.4, 8.7, 7.7, 6.7, 6.9, 6.4, 6.2, 5.8, 5.4, 6.7, 5.0, 3.4, 3.3, 3.3],
  [7.3, 7.5, 7.2, 7.0, 6.5, 6.3, 6.0, 5.2, 5.7, 5.8, 5.9, 5.1, 4.6, 3.7, 3.6],
  [7.3, 7.4, 7.2, 6.7, 6.4, 6.3, 6.1, 6.0, 5.5, 5.5, 5.4, 6.1, 5.4, 4.0, 3.9],
  [6.8, 6.9, 6.8, 6.2, 5.9, 5.8, 6.2, 6.4, 6.0, 5.7, 5.6, 5.2, 5.1, 4.1, 4.0],
  [6.4, 6.3, 5.9, 5.7, 5.7, 5.9, 6.7, 5.9, 6.1, 5.7, 5.7, 5.6, 4.9, 4.2, 3.9],
  [5.6, 5.8, 5.6, 5.8, 6.0, 5.8, 7.4, 6.5, 5.7, 5.6, 5.7, 7.2, 5.2, 4.1, 3.9],
  [3.1, 5.4, 5.4, 5.9, 6.3, 6.3, 6.5, 5.8, 5.5, 5.7, 5.8, 5.9, 5.1, 4.1, 3.8],
  [3.1, 4.4, 5.5, 6.1, 5.7, 5.5, 6.0, 6.4, 5.9, 5.6, 6.1, 5.2, 5.1, 3.8, 3.5],
  [3.9, 5.2, 5.7, 5.5, 5.3, 5.6, 6.2, 6.1, 5.9, 5.1, 4.7, 4.9, 4.4, 3.4, 3.1],
  [5.3, 5.4, 5.7, 5.6, 5.6, 5.7, 5.8, 5.8, 5.5, 4.3, 5.2, 4.5, 4.3, 3.1, 2.6],
  [5.4, 5.6, 5.6, 5.6, 5.4, 5.5, 5.1, 5.1, 4.6, 4.7, 4.6, 4.5, 3.4, 2.8, 2.3],
  [5.4, 5.5, 5.8, 5.4, 4.4, 4.2, 4.7, 4.9, 4.8, 4.6, 4.3, 3.2, 2.8, 2.5, 2.2],
  [5.9, 5.8, 6.0, 5.7, 4.4, 3.5, 5.0, 5.0, 4.6, 4.1, 3.4, 3.3, 2.7, 2.2, 2.4],
]
// The canopy over the platform (way/239268383).
const STATION: [number, number][] = [
  [302.8, 17.0], [314.6, 0.0], [313.1, -1.0], [312.6, -1.3], [312.3, -1.6], [312.7, -2.2], [314.5, -4.8],
  [315.3, -4.3], [321.3, -0.1], [325.9, 3.0], [327.3, 4.0], [319.2, 15.5], [317.4, 18.2], [313.2, 24.1], [307.5, 20.2],
]

await buildCoaster({
  name: 'Thunder Striker',
  out: '../../landmarks/models/carowinds-thunder-striker.glb',
  chain: CHAIN,
  anchor: [360, -195],
  ground: { x0: 220, y0: -460, step: 20, rows: GROUND, base: 190.1 },
  // Heights above the local ground; s along the OSM polyline (1,398 m round).
  profile: [
    { name: 'station', s: 0, z: 3 },
    { name: 'foot of the lift', s: 16, z: 3, r: 14 },
    { name: 'lift crest (232 ft)', s: 100, z: 71, r: 22, pitch: 74 },
    { name: 'first drop, foot (74°, 211 ft)', s: 177, z: 7 },
    { name: 'camelback (178 ft)', s: 251, z: 55, r: 20 },
    { name: 'sharp right turn, foot', s: 312, z: 2.5 },
    { name: 'left turn', s: 345, z: 3 },
    { name: 'camelback (151 ft)', s: 421, z: 48 },
    { name: 'valley', s: 490, z: 2.5, r: 42 },
    { name: 'hammerhead (121 ft), in', s: 594, z: 40, r: 16 },
    { name: 'hammerhead (121 ft), out', s: 606, z: 40, r: 16 },
    { name: 'valley', s: 662, z: 2.5 },
    { name: 'camelback (105 ft)', s: 730, z: 35 },
    { name: 'valley', s: 785, z: 2.5 },
    { name: 'camelback (90 ft)', s: 841, z: 31 },
    { name: 'valley before the left turn', s: 885, z: 5 },
    { name: 'mid-course brakes, in', s: 950, z: 25, r: 25 },
    { name: 'mid-course brakes, out', s: 1004, z: 25, r: 20 },
    { name: 'valley', s: 1040, z: 7 },
    { name: 'hill (62 ft)', s: 1071, z: 21 },
    { name: 'diving spiral, foot', s: 1185, z: 2 },
    { name: 'hill (52 ft)', s: 1225, z: 17.5 },
    { name: 'valley', s: 1257, z: 3 },
    { name: 'hill (48 ft)', s: 1283, z: 16 },
    { name: 'final brakes', s: 1314, z: 4, r: 24 },
    { name: 'final brakes, out', s: 1345, z: 4 },
  ],
  lift: [10, 100],
  brakes: [
    { from: 950, to: 1004, z: 25, head: 3 },
    { from: 1312, to: 10, z: 4, head: 0.5 },
  ],
  // The hammerhead turns on top at about 20 m/s, banked well over.
  banks: [{ from: 590, to: 610, deg: 55, name: 'hammerhead' }],
  losses: [0.007, 0.0004],
  track: { W: 3.2, RAIL: 0.7, DEPTH: 2.1, SPINE: 1.5 },
  station: { ring: STATION, posts: [[306, 17], [314.5, 1.5], [323, 3.5], [311, 21]], roofZ: 7.5 },
  // Colours from the photos, pulled to the palette's lightness: red track, its
  // spine a shade deeper, pale grey supports.
  colours: { deck: ['striker-red', 0xc9473f], spine: ['striker-red-2', 0xb03a35], supports: ['striker-grey', 0xc9cfd4] },
  supportR: [0.9, 1.05, 1.2],
  meta: { height: 71, trackLength: 1620 },
})
