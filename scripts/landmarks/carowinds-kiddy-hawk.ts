/**
 * Kiddy Hawk (formerly Rugrats Runaway Reptar, then Flying Ace Aerial
 * Chase), Carowinds: procedural, CC0-1.0.
 * bun scripts/landmarks/carowinds-kiddy-hawk.ts   (REPORT=1 for the element table)
 *
 * Map frame: x east, y north, z up, metres; bearing 0, so the frame is true
 * north. The origin is the middle of the ride's footprint (see the anchor), at
 * the lowest ground under the track. Built with ./coaster-kit.ts.
 *
 * Plan: the circuit is the chain of OSM `roller_coaster=track` ways from the
 * station (way/241410480): way/669161369, 894786042, 894786045, 894786041,
 * 905173229, 894786044, 894786043 and the covered way/888642923 through the
 * station, all oneway, end to end, closing on the station. Their layer tags
 * fall along the ride (lift 4, crest 5, then 4, 3, 2, 1), and the loop is
 * 322 m against the published 342 m, in a 54 m × 37 m footprint against
 * RCDB's 59 m × 37 m. No other unnamed track lies near it.
 *
 * Profile: a Vekoma Suspended Family Coaster (342 m model; RCDB 1897): the
 * train hangs under the rails. 48.6 ft (14.8 m) to the top of the structure,
 * 26 mph, a booster-wheel lift. Ride order from Wikipedia ("Woodstock's Air
 * Rail") and the plan: a right turn out of the station, the straight lift,
 * a double helix to the left, under the lift, a right turn over the corner of
 * the station, a right turn down to near the ground, a left turnaround into
 * the brakes, and a right turn into the station. Positions are metres along
 * the OSM polyline (322 m round) and % of it:
 *
 *   lift, 15°                  s 14–46 (4–14%), rails 13 m up at the crest
 *   double helix left, falling s 50–125 (16–39%)
 *   under the lift             s 133 (41%), rails 5.2 m up
 *   right turn over the station s 150–180 (47–56%), 8 m up
 *   low right turn             s 185–225 (57–70%), 3.6 m up
 *   left turnaround            s 240–265 (75–82%), 5 m up
 *   brakes                     s 270–300 (84–93%)
 *
 * Photos used for matching: "A View of Kiddy Hawk from Carolina Skytower"
 * (ZackyWacky, April 2025, CC BY-SA 4.0) and "Kiddy Hawk 1" (Jeremy
 * Thompson, June 2023, CC BY 2.0), both Wikimedia Commons; colours also
 * checked against "Kiddy Hawk (Carowinds) 2018 01" (C. E. Beavers, Coasterpedia).
 *
 * Style: the rails as a narrow blue ribbon with the box spine above them, on
 * tan columns with an arm reaching over the track, as the photos show. The
 * station is its roof on posts.
 */
import { buildCoaster } from './coaster-kit'

// The OSM circuit, node by node, in the park frame (see coaster-kit), starting
// at the station exit (the first node of way/669161369).
const CHAIN: [number, number][] = [
  [186.2, -99.0], [185.8, -97.4], [186.0, -95.6], [187.0, -93.8], [188.6, -92.8], [190.7, -92.4], [192.6, -92.7], [207.0, -96.3],
  [213.4, -97.9], [217.1, -98.8], [220.3, -99.6], [225.4, -100.8], [226.5, -100.9], [227.9, -101.1], [230.2, -101.2], [232.1, -100.8],
  [233.8, -99.8], [235.0, -98.6], [235.7, -97.2], [235.9, -95.1], [235.5, -93.2], [234.5, -91.6], [233.1, -90.6], [231.4, -90.1],
  [229.7, -90.1], [228.0, -90.8], [226.7, -92.1], [225.7, -93.6], [225.3, -95.1], [225.3, -97.3], [225.6, -99.5], [226.5, -100.9],
  [227.8, -102.2], [229.7, -103.3], [232.0, -103.6], [234.3, -103.3], [236.4, -102.5], [237.8, -101.3], [238.9, -99.3], [239.6, -97.6],
  [239.8, -95.5], [239.6, -94.1], [239.0, -91.9], [237.9, -89.9], [236.5, -88.6], [234.9, -87.7], [233.0, -87.1], [231.0, -86.7],
  [229.0, -86.7], [226.8, -87.0], [225.4, -87.5], [223.2, -88.8], [222.2, -89.7], [219.9, -91.6], [216.9, -94.0], [213.4, -97.9],
  [211.0, -100.5], [209.1, -102.4], [205.2, -105.8], [202.2, -108.3], [200.5, -109.2], [198.8, -110.0], [196.7, -110.3], [195.1, -109.9],
  [193.8, -109.1], [192.8, -108.0], [192.1, -106.5], [191.8, -104.7], [192.0, -103.0], [192.7, -101.4], [193.9, -100.2], [195.4, -99.3],
  [197.3, -98.7], [198.9, -98.5], [201.2, -98.6], [203.3, -98.9], [208.4, -99.9], [211.0, -100.5], [214.1, -101.3], [216.3, -101.9],
  [219.1, -103.6], [221.2, -105.5], [222.1, -107.3], [222.4, -109.2], [222.1, -110.7], [221.6, -111.9], [220.9, -112.8], [220.0, -113.4],
  [218.7, -114.0], [217.4, -114.2], [215.8, -114.1], [214.1, -113.5], [212.9, -112.5], [211.8, -110.9], [211.5, -109.4], [211.4, -107.5],
  [212.0, -105.9], [213.3, -103.6], [217.1, -98.8], [219.9, -95.3], [220.8, -94.1], [221.8, -92.2], [222.1, -90.3], [222.2, -89.7],
  [222.0, -87.4], [221.3, -85.7], [220.1, -84.5], [218.7, -83.8], [217.4, -83.4], [215.8, -83.4], [214.2, -83.7], [212.8, -84.4],
  [211.8, -85.1], [210.9, -85.8], [210.2, -86.8], [209.3, -88.2], [208.6, -90.0], [208.1, -91.5], [207.0, -96.3], [203.4, -112.2],
  [202.7, -114.8], [202.2, -116.7], [201.5, -117.9], [200.5, -118.8], [199.5, -119.5], [197.9, -119.9], [196.9, -119.9], [195.6, -119.7],
  [194.5, -119.1], [193.7, -118.1], [193.0, -117.2],
]
// Terrain (Mapzen/AWS Terrarium, open data), 10 m cells from (175, -130); rows
// south to north, columns west to east; metres above 193.1 m. The low cells in
// the north-west are the lake's edge.
const GROUND = [
  [4.0, 3.7, 3.9, 4.5, 4.8, 4.5, 4.5, 4.5, 4.4],
  [4.0, 4.1, 4.3, 4.3, 4.3, 4.3, 4.4, 4.5, 4.4],
  [3.1, 4.2, 4.1, 3.9, 4.0, 4.1, 4.2, 4.1, 4.0],
  [2.4, 3.4, 3.8, 3.9, 3.6, 3.9, 4.0, 3.8, 3.7],
  [0.1, 1.3, 3.3, 3.5, 3.7, 3.7, 3.9, 3.5, 3.3],
  [0.4, 0.1, 0.2, 2.3, 3.5, 3.5, 3.5, 3.2, 3.0],
  [1.8, 0.2, 0.4, 0.1, 2.7, 2.7, 3.3, 3.2, 2.9],
]
// The station building (way/241410480).
const STATION: [number, number][] = [
  [178.9, -104.1], [180.3, -107.7], [184.0, -106.4], [184.8, -108.4], [185.2, -109.6], [185.9, -111.4], [188.6, -118.8], [193.0, -117.2],
  [195.3, -116.4], [192.2, -108.4], [192.0, -107.7], [191.6, -106.4], [189.3, -100.2], [188.5, -98.2], [186.2, -99.0], [182.6, -100.3],
  [181.9, -100.6], [182.7, -102.7],
]

await buildCoaster({
  name: 'Kiddy Hawk',
  out: '../../landmarks/models/carowinds-kiddy-hawk.glb',
  chain: CHAIN,
  anchor: [210, -101],
  ground: { x0: 175, y0: -130, step: 10, rows: GROUND, base: 193.1 },
  // Heights of the rails above the local ground; s along the OSM polyline (322 m round).
  profile: [
    { name: 'station', s: 0, z: 3.6 },
    { name: 'foot of the lift', s: 11, z: 3.8, r: 10 },
    { name: 'lift crest', s: 45, z: 14, r: 16 },
    { name: 'into the helix', s: 82, z: 9.8 },
    { name: 'under the lift', s: 133, z: 6.4, r: 14 },
    { name: 'right turn by the station', s: 152, z: 8.6, pitch: 5.5 },
    { name: 'low right turn', s: 222, z: 3 },
    { name: 'brakes', s: 276, z: 3.3 },
  ],
  lift: [11, 45],
  brakes: [{ from: 272, to: 11, z: 3.3, head: 0.5 }],
  losses: [0.02, 0.001],
  inverted: { clear: 3, reach: 2 },
  track: { W: 1.1, RAIL: 0.25, DEPTH: 0.75, SPINE: 0.45 },
  station: { ring: STATION, posts: [[179.8, -104.3], [188.9, -117.9], [194.4, -116.1], [188.6, -99.1], [191.5, -107.5], [185.6, -109.5]], roofZ: 5.4 },
  // Colours from the photos, pulled to the palette's lightness: blue rails and
  // spine, tan supports.
  colours: { deck: ['kiddy-blue', 0x5b9bd8], spine: ['kiddy-blue-2', 0x4c88c8], supports: ['kiddy-tan', 0xe0d4b0] },
  supportR: [0.28, 0.32, 0.36],
  supportEvery: 9,
  // The double helix hangs from spokes off a central mast, an umbrella.
  hubs: [{ at: [230.5, -95.5], top: 15, from: 57, to: 125, every: 9 }],
  sampleDeg: 12,
  meta: { height: 14.8, trackLength: 342 },
})
