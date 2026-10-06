/**
 * Wilderness Run (formerly Taxi Jam, Hey Arnold! Taxi Chase and Lucy's
 * Crabbie Cabbie), Carowinds: procedural, CC0-1.0.
 * bun scripts/landmarks/carowinds-wilderness-run.ts   (REPORT=1 for the element table)
 *
 * Map frame: x east, y north, z up, metres; bearing 0, so the frame is true
 * north. The origin is the middle of the ride's footprint (see the anchor), at
 * the lowest ground under the track. Built with ./coaster-kit.ts.
 *
 * Plan: the circuit is the two OSM `roller_coaster=track` ways from the
 * station (way/241559222): way/669205942 and the covered way/888643799
 * through the station, both oneway, closing on each other: 121 m against the
 * published 416 ft (127 m).
 *
 * Profile: an E&F Miler family coaster, "16 ft oval with helix on left" (RCDB
 * 498), 16 ft (4.9 m) high, a chain lift. Ride order from Wikipedia: the
 * 15 ft lift out of the station, a left downward banked turn, a camelback, a
 * smaller hill, a downward left helix and a run of bumps into the station.
 * Positions are metres along the OSM polyline (121 m round) and % of it:
 *
 *   lift                       s 113–11 (93–9%), from the end of the station, rails 4.6 m up
 *   left banked turn, falling  s 11–28 (9–23%)
 *   camelback, 2 m             s 33 (27%)
 *   top of the helix, 3 m      s 49 (40%), passed under a turn later
 *   left helix, falling        s 48–84 (40–69%)
 *   bump, brakes               s 97–113 (80–93%)
 *
 * Photos used for colour and shape: "Wilderness Run (Carowinds) 2026 01" and
 * "2026 02" (Time93, August 2026, CC BY-SA, Coasterpedia), "Wilderness Run
 * (Carowinds) 2018 01" (C. E. Beavers, Coasterpedia), "Lucy's Crabbie Cabbie
 * 1" (Martin Lewison, 2012, CC BY-SA 2.0, Wikimedia Commons).
 *
 * Style: a narrow sage-green ribbon on red-brown columns, as in the 2026
 * photos, the low runs a knee's height off the lawn; the station is its roof on
 * posts.
 */
import { buildCoaster } from './coaster-kit'

// The OSM circuit, node by node, in the park frame (see coaster-kit), starting
// at the station exit (the first node of way/669205942).
const CHAIN: [number, number][] = [
  [392.3, -211.4], [394.2, -208.9], [396.6, -205.5], [397.5, -203.6], [397.7, -200.7], [397.0, -198.4], [395.6, -196.7], [394.0, -195.8],
  [392.3, -195.6], [390.4, -195.9], [389.2, -196.8], [387.5, -198.1], [385.2, -200.7], [381.2, -206.1], [377.1, -211.7], [374.5, -215.2],
  [373.8, -216.0], [372.8, -217.3], [372.1, -218.7], [371.9, -220.1], [372.1, -221.6], [373.1, -223.0], [374.3, -224.1], [375.7, -224.7],
  [377.1, -224.8], [378.6, -224.4], [380.0, -223.4], [380.8, -222.2], [381.4, -220.5], [381.3, -218.8], [380.9, -217.7], [380.2, -216.6],
  [379.0, -215.8], [377.7, -215.3], [375.9, -215.0], [374.5, -215.2], [373.0, -215.5], [371.8, -216.2], [370.7, -217.6], [370.1, -218.8],
  [369.5, -220.4], [369.4, -222.1], [369.9, -223.8], [370.8, -225.2], [372.0, -226.7], [373.1, -227.7], [374.4, -228.1], [375.7, -228.3],
  [377.2, -228.1], [378.7, -227.7], [380.0, -227.0], [381.1, -225.5], [383.8, -221.8],
]
// Terrain (Mapzen/AWS Terrarium, open data), 10 m cells from (355, -240); rows
// south to north, columns west to east; metres above 193.4 m.
const GROUND = [
  [2.7, 2.3, 2.2, 1.4, 0.2, 0.0],
  [2.5, 2.3, 2.4, 2.0, 1.1, 0.2],
  [2.6, 2.3, 2.2, 2.2, 1.7, 0.7],
  [3.1, 2.8, 2.3, 2.6, 2.0, 1.5],
  [2.8, 3.0, 2.6, 2.2, 2.1, 2.2],
  [3.0, 2.6, 2.6, 2.4, 2.4, 2.8],
]
// The station building (way/241559222).
const STATION: [number, number][] = [
  [390.1, -209.8], [392.3, -211.4], [394.2, -212.9], [394.6, -213.1], [396.1, -214.3], [388.0, -224.9], [387.1, -224.2], [386.5, -223.8],
  [385.9, -223.4], [383.8, -221.8], [382.0, -220.5],
]

await buildCoaster({
  name: 'Wilderness Run',
  out: '../../landmarks/models/carowinds-wilderness-run.glb',
  chain: CHAIN,
  anchor: [384, -212],
  ground: { x0: 355, y0: -240, step: 10, rows: GROUND, base: 193.4 },
  // Heights of the rails above the local ground; s along the OSM polyline (121 m round).
  profile: [
    { name: 'lift crest', s: 11, z: 4.6, r: 10 },
    { name: 'left banked turn, foot', s: 26, z: 0.8, r: 8 },
    { name: 'camelback', s: 33, z: 2, r: 8 },
    { name: 'valley', s: 39, z: 1.1, r: 8 },
    { name: 'top of the helix', s: 49, z: 3, r: 11 },
    { name: 'helix, foot', s: 84, z: 0.7, r: 8 },
    { name: 'bump', s: 96, z: 1.5, r: 6 },
    { name: 'brakes', s: 104, z: 0.8, r: 8 },
    { name: 'station, foot of the lift', s: 113, z: 0.8, r: 10 },
  ],
  lift: [113, 11],
  brakes: [{ from: 104, to: 113, z: 0.8, head: 0.3 }],
  losses: [0.015, 0.001],
  track: { W: 1.1, RAIL: 0.25, DEPTH: 0.5, SPINE: 0.35 },
  station: { ring: STATION, posts: [[390.6, -210.7], [395.2, -214.1], [387.6, -224.1], [383.1, -220.9]], roofZ: 3.4 },
  // Colours from the 2026 photos, pulled to the palette's lightness: sage
  // track, red-brown supports.
  colours: { deck: ['wilderness-sage', 0x9aa086], spine: ['wilderness-sage-2', 0x868c74], supports: ['wilderness-rust', 0xb5644f] },
  supportR: [0.16, 0.18, 0.2],
  supportEvery: 5,
  supportMinZ: 0.9,
  sampleDeg: 12,
  meta: { height: 4.9, trackLength: 127 },
})
