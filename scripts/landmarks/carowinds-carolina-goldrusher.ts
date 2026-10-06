/**
 * Carolina Goldrusher, Carowinds: procedural, CC0-1.0.
 * bun scripts/landmarks/carowinds-carolina-goldrusher.ts   (REPORT=1 for the element table)
 *
 * Map frame: x east, y north, z up, metres; bearing 0. The origin is the
 * middle of the track's footprint (see anchor), at the lowest ground under it.
 * Built with ./coaster-kit.ts (a second chain lift, wooden bents, a tunnel).
 *
 * Plan: the OSM circuit (way/669205940, way/895071507, way/895071508,
 * way/895071510, way/895071511, way/895071512, way/895071506, way/895071509,
 * way/891133321), 728 m in plan, from the station exit. The brief's table
 * put way/895071506, way/895071511 and way/895071512 under Flying Cobras;
 * they close this circuit.
 *
 * Profile: an Arrow mine train (RCDB 83: 2,397 ft, two chain lifts, a
 * tunnel; no published height). The lifts are the two long straights the
 * OSM layers raise; heights are guesses kept low, as a mine train is:
 *
 *   station, elevated platform      s 0, 3.5 m (from the photo)
 *   lift 1, 11 m                    s 104–157 (14–22%)
 *   turnaround, dip, helix 1        s 200–366, helix over the tunnel
 *   lift 2, 12.5 m                  s 384–455 (53–63%)
 *   helix 2, 1.5 turns, falling     s 461–590
 *   mine tunnel (way/895071506)     s 630–700, drawn as a covered box
 *
 * Photo (Wikimedia Commons): "Carolina Goldrusher 1" (2023, Jeremy Thompson,
 * CC BY 2.0), matched by hand-placed camera. No other usable open photo of the ride was found.
 *
 * Style: sage-grey tubular track on brown timber bents, as the photo shows.
 */
import { buildCoaster } from './coaster-kit'

// The OSM circuit (way/669205940, way/895071507, way/895071508,
// way/895071510, way/895071511, way/895071512, way/895071506, way/895071509,
// way/891133321, all oneway, in the direction of travel), node by node in the
// park frame (see coaster-kit), from the station exit.
const CHAIN: [number, number][] = [
  [14.5, 32.4], [13.7, 34.4], [14.0, 38.1], [14.8, 40.9], [16.6, 43.3], [18.6, 45.3], [38.8, 61.2], [41.4, 62.2],
  [44.8, 62.1], [47.6, 61.3], [49.6, 59.9], [50.6, 57.4], [50.8, 55.4], [50.5, 52.5], [48.8, 49.6], [25.6, 29.9],
  [25.1, 27.8], [25.9, 25.4], [27.4, 23.0], [30.2, 20.9], [33.3, 20.0], [36.7, 20.2], [39.5, 21.4], [41.6, 23.4],
  [70.9, 51.5], [84.8, 64.4], [87.2, 65.2], [89.7, 65.4], [93.1, 64.7], [95.6, 63.4], [98.8, 60.5], [101.2, 57.0],
  [102.1, 53.2], [102.2, 49.2], [101.6, 45.9], [100.3, 43.3], [98.4, 40.5], [96.4, 38.5], [93.8, 36.8], [89.5, 34.8],
  [84.0, 32.8], [62.8, 26.3], [46.9, 21.2], [43.1, 18.9], [39.4, 15.4], [36.7, 12.1], [34.7, 8.1], [33.7, 3.4],
  [33.2, -1.7], [33.4, -4.4], [34.1, -7.1], [36.0, -10.1], [37.5, -11.7], [39.3, -13.5], [43.3, -15.3], [47.0, -15.6],
  [51.1, -15.1], [53.8, -13.0], [55.7, -8.9], [56.1, -5.1], [55.2, -1.6], [52.4, 1.4], [49.2, 3.1], [46.1, 3.4],
  [42.6, 2.5], [39.8, 0.8], [37.2, -2.6], [36.7, -4.1], [36.2, -6.0], [36.3, -7.6], [37.5, -11.7], [38.7, -14.3],
  [41.2, -16.6], [68.8, -30.8], [90.3, -40.2], [117.3, -53.1], [123.6, -56.1], [126.2, -58.9], [127.7, -63.4], [127.6, -68.3],
  [125.5, -72.3], [122.8, -74.4], [120.4, -75.6], [118.1, -75.8], [115.3, -75.5], [112.4, -74.5], [110.5, -73.4], [108.7, -71.5],
  [107.5, -69.7], [106.9, -66.9], [106.7, -64.4], [107.0, -62.2], [108.5, -59.2], [110.5, -56.3], [113.8, -54.2], [117.3, -53.1],
  [120.3, -53.6], [123.0, -54.4], [126.4, -57.5], [127.9, -59.9], [128.8, -63.6], [128.9, -68.4], [127.6, -71.8], [124.3, -75.4],
  [121.1, -77.3], [119.1, -78.0], [114.4, -78.8], [109.2, -78.7], [104.6, -77.8], [100.6, -76.5], [96.6, -74.1], [92.3, -70.7],
  [74.3, -56.0], [68.4, -51.0], [66.7, -49.5], [54.8, -38.7], [41.4, -28.0], [37.5, -24.0], [35.2, -18.4], [25.6, 7.8],
  [19.8, 20.2],
]
// Terrain (Mapzen/AWS Terrarium, open data), 20 m cells from (-20, -100); rows
// south to north, columns west to east; metres above 193.2 m.
const GROUND = [
  [1.2, 2.2, 2.3, 2.4, 2.4, 2.8, 2.7, 2.7, 2.1, 0.4],
  [1.3, 1.9, 2.3, 2.4, 2.4, 2.2, 1.9, 1.7, 2.4, 0.4],
  [1.6, 1.7, 1.9, 1.9, 1.9, 1.7, 1.6, 1.5, 2.1, 2.1],
  [1.8, 1.8, 1.7, 1.9, 1.8, 1.6, 1.7, 2.1, 2.4, 2.4],
  [1.9, 1.9, 1.9, 0.8, 1.7, 1.9, 2.0, 2.1, 2.1, 2.3],
  [1.9, 3.2, 4.1, 1.4, 1.2, 2.1, 2.0, 2.2, 1.8, 2.1],
  [0.8, 1.8, 3.0, 0.8, 1.3, 1.8, 1.6, 1.0, 0.0, 0.2],
  [0.6, 0.8, 1.1, 0.5, 0.7, 0.8, 1.3, 1.7, 1.7, 0.7],
  [0.6, 0.4, 0.7, 0.5, 0.4, 0.3, 1.0, 1.4, 1.9, 1.6],
  [0.1, 0.5, 0.8, 1.0, 0.3, 0.5, 1.2, 1.7, 1.1, 1.4],
  [0.3, 0.4, 0.6, 0.6, 0.9, 1.3, 1.3, 1.6, 1.5, 1.4],
]
// The station building (way/668730923).
const STATION: [number, number][] = [
  [7.2, 28.2], [9.2, 23.3], [7.6, 22.7], [11.0, 14.2], [11.3, 13.4], [12.0, 13.7], [12.9, 14.1], [18.6, 16.3], [17.5, 19.2],
  [19.8, 20.2], [21.9, 21.0], [17.4, 32.0], [16.9, 33.3], [14.5, 32.4], [12.6, 31.6], [11.6, 31.2], [12.0, 30.1],
]

await buildCoaster({
  name: 'Carolina Goldrusher',
  out: '../../landmarks/models/carowinds-carolina-goldrusher.glb',
  chain: CHAIN,
  anchor: [69, -5],
  ground: { x0: -20, y0: -100, step: 20, rows: GROUND, base: 193.2 },
  // Heights above the local ground; s along the OSM polyline (728 m round).
  profile: [
    { name: 'station', s: 0, z: 3.5 },
    { name: 'turn out of the station', s: 60, z: 3.5 },
    { name: 'foot of lift 1', s: 104, z: 1.5, r: 12 },
    { name: 'lift 1 crest', s: 157, z: 11, r: 12 },
    { name: 'turnaround', s: 200, z: 9.5 },
    { name: 'turnaround, out', s: 228, z: 8.5, r: 14 },
    { name: 'dip', s: 268, z: 4, r: 14 },
    { name: 'helix 1, in', s: 297, z: 6.5 },
    { name: 'helix 1, low', s: 345, z: 1.5 },
    { name: 'foot of lift 2', s: 380, z: 1.5, r: 12 },
    { name: 'lift 2 crest', s: 455, z: 12.5, r: 12 },
    { name: 'helix 2, top', s: 480, z: 11.5 },
    { name: 'helix 2, low', s: 590, z: 2 },
    { name: 'mine tunnel', s: 640, z: 1.5 },
    { name: 'brakes', s: 700, z: 3.5 },
  ],
  lift: [104, 157],
  extraLifts: [[384, 455]],
  brakes: [{ from: 700, to: 104, z: 3.5, head: 0.5 }],
  losses: [0.015, 0.0008],
  track: { W: 2.0, RAIL: 0.35, DEPTH: 0.8, SPINE: 0.7 },
  station: { ring: STATION, posts: [], roofZ: 7 },
  stationWalls: true,
  covers: [{ from: 630, to: 700, height: 3.2 }],
  trestle: { every: 4, along: 0.7, levelStep: 100 },
  colours: { deck: ['goldrusher-sage', 0x9ea594], spine: ['goldrusher-sage-2', 0x8a9282], supports: ['goldrusher-timber', 0xae9474] },
  supportR: [0.4, 0.5, 0.6],
  meta: { height: 11, trackLength: 731 },
})
