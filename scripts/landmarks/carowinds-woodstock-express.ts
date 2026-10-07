/**
 * Woodstock Express (formerly Scooby-Doo, Scooby-Doo's Ghoster Coaster and
 * Fairly Odd Coaster), Carowinds: procedural, CC0-1.0.
 * bun scripts/landmarks/carowinds-woodstock-express.ts   (REPORT=1 for the element table)
 *
 * Map frame: x east, y north, z up, metres; bearing 0, so the frame is true
 * north. The origin is the middle of the ride's footprint (see the anchor), at
 * the lowest ground under the track. Built with ./coaster-kit.ts.
 *
 * Plan: the circuit is the OSM `roller_coaster=track` ways from the station
 * (way/241559218): way/888643797, 670967417, 241559217 and the covered
 * way/888643798 through the station, all oneway, end to end: 402 m against
 * the published 1,356 ft (413 m). The transfer siding (way/894079214,
 * 894079215) is left out.
 *
 * Profile: a PTC wooden family coaster of 1975 (RCDB 82, John C. Allen), a
 * figure eight with a 40 ft (12 m) chain lift (Wikipedia). OSM tags the whole
 * of way/670967417 layer 1, but the lift photos show the lift climbing to a
 * turnaround at the top, and the station on the right as you look up it from
 * the queue: so the lift is the end of the long straight south-west of the
 * station, and the first drop runs back north-north-east under nothing but
 * over the low run to the lift (the figure eight's crossing). Positions are
 * metres along the OSM polyline (402 m round) and % of it:
 *
 *   turnaround out of the station s 0–33 (0–8%), low
 *   lift, 12 m                 s 90–122 (22–30%)
 *   turnaround at the top      s 122–150 (30–37%)
 *   first drop, over the run to the lift  s 150–195 (37–49%)
 *   north turnaround, 6 m      s 205–227 (51–56%)
 *   drop, camelback, south-west turnaround  s 227–315 (56–78%)
 *   hill and brakes into the station  s 315–359 (78–89%)
 *
 * Photos used for matching: "Woodstock Express Carowinds" (the lift from the
 * queue) and "Carowinds 032", "Carowinds 034" (Jeremy Thompson, May 2008,
 * CC BY 2.0, Wikimedia Commons); the present colours from Wikipedia (the 2015
 * repaint: periwinkle structure, yellow side rails) and the structure in the
 * background of "Wilderness Run (Carowinds) 2026 01" (Time93, CC BY-SA,
 * Coasterpedia).
 *
 * Style: a wooden trestle of bents and ledgers in periwinkle, the deck
 * weathered wood; the station is its roof on posts.
 */
import { buildCoaster } from './coaster-kit'

// The OSM circuit, node by node, in the park frame (see coaster-kit), starting
// at the station exit (the first node of way/888643797).
const CHAIN: [number, number][] = [
  [350.4, -167.3], [351.0, -166.3], [352.6, -164.6], [354.5, -162.9], [356.7, -161.9], [358.8, -161.9], [360.9, -162.4], [362.3, -163.2],
  [363.6, -164.3], [365.3, -166.7], [365.8, -168.8], [365.7, -170.9], [365.4, -172.1], [364.7, -173.4], [363.9, -174.7], [362.7, -176.0],
  [360.2, -178.3], [348.5, -189.3], [331.2, -205.6], [298.6, -237.1], [296.5, -240.8], [296.4, -243.0], [297.1, -245.1], [298.0, -246.6],
  [299.2, -247.8], [300.8, -248.3], [302.6, -248.4], [304.1, -248.3], [305.4, -247.8], [306.8, -247.2], [308.3, -246.1], [311.6, -242.1],
  [335.4, -191.5], [336.9, -189.1], [339.1, -186.7], [341.2, -186.2], [343.2, -186.2], [345.2, -186.8], [347.0, -187.7], [348.5, -189.3],
  [349.4, -190.9], [349.5, -193.1], [349.4, -194.7], [347.2, -198.4], [315.1, -238.2], [310.8, -245.0], [309.4, -246.9], [307.9, -248.0],
  [306.0, -249.0], [304.2, -249.5], [302.4, -249.7], [300.4, -249.4], [298.6, -248.7], [296.8, -247.4], [295.6, -245.7], [294.9, -243.2],
  [294.9, -240.7], [295.9, -238.8], [297.4, -236.9], [319.9, -208.2], [324.0, -202.4],
]
// Terrain (Mapzen/AWS Terrarium, open data), 10 m cells from (285, -260); rows
// south to north, columns west to east; metres above 194 m.
const GROUND = [
  [4.8, 3.9, 3.0, 2.7, 1.9, 1.7, 2.3, 1.3, 1.1, 0.0],
  [5.0, 4.3, 3.3, 2.6, 2.2, 1.8, 2.3, 2.5, 1.4, 0.9],
  [5.3, 4.5, 3.5, 2.9, 2.6, 2.3, 2.1, 2.1, 1.7, 1.6],
  [5.2, 4.8, 3.5, 2.9, 2.5, 3.0, 2.1, 1.9, 1.7, 1.8],
  [4.9, 4.9, 4.3, 3.0, 2.8, 2.5, 2.2, 2.0, 1.7, 1.6],
  [5.0, 5.0, 5.4, 3.4, 2.8, 2.7, 2.3, 2.5, 2.2, 1.7],
  [4.8, 4.6, 3.9, 3.4, 2.8, 2.6, 2.5, 2.2, 2.4, 2.0],
  [4.3, 3.8, 3.2, 3.3, 3.0, 2.6, 2.6, 2.4, 2.0, 2.0],
  [4.1, 3.7, 3.1, 3.0, 3.2, 2.7, 2.6, 2.6, 2.5, 2.1],
  [4.1, 3.4, 2.7, 2.8, 2.6, 2.6, 2.6, 2.5, 2.4, 1.7],
  [3.7, 3.2, 2.8, 2.9, 2.6, 2.5, 2.5, 2.4, 2.2, 1.8],
  [3.0, 2.9, 2.7, 2.7, 2.3, 2.2, 2.2, 2.3, 2.0, 1.9],
]
// The station building (way/241559218).
const STATION: [number, number][] = [
  [346.8, -164.6], [348.5, -165.9], [350.4, -167.3], [352.7, -168.9], [341.1, -184.5], [339.4, -183.3], [324.7, -203.0], [324.0, -202.4],
  [322.3, -201.2], [317.0, -197.3], [319.5, -193.9], [328.7, -181.6], [332.2, -184.2], [335.1, -180.2], [335.9, -179.2], [336.7, -178.1],
  [341.5, -171.7], [341.9, -171.1], [343.1, -169.5],
]

await buildCoaster({
  name: 'Woodstock Express',
  out: '../../landmarks/models/carowinds-woodstock-express.glb',
  chain: CHAIN,
  anchor: [330, -206],
  ground: { x0: 285, y0: -260, step: 10, rows: GROUND, base: 194 },
  // Heights of the deck above the local ground; s along the OSM polyline (402 m round).
  profile: [
    { name: 'station', s: 0, z: 2 },
    { name: 'run to the lift', s: 60, z: 1.5 },
    { name: 'foot of the lift', s: 90, z: 1.5, r: 16 },
    { name: 'top turnaround, in', s: 124, z: 12, r: 14 },
    { name: 'top turnaround, out', s: 155, z: 12, r: 14 },
    { name: 'first drop, foot', s: 198, z: 1.5, r: 17 },
    { name: 'north turnaround', s: 216, z: 6 },
    { name: 'second drop, foot', s: 252, z: 1.5 },
    { name: 'camelback', s: 266, z: 3.5 },
    { name: 'south-west turnaround', s: 296, z: 1.5 },
    { name: 'hill', s: 328, z: 3.2 },
    { name: 'brakes', s: 345, z: 2 },
  ],
  lift: [90, 124],
  brakes: [{ from: 343, to: 90, z: 2, head: 0.4 }],
  losses: [0.025, 0.0015],
  track: { W: 2.6, RAIL: 0.5, DEPTH: 1, SPINE: 2.2 },
  trestle: { every: 5, levelStep: 3.6 },
  station: { ring: STATION, posts: [[350.4, -168.4], [340.5, -183.2], [324.4, -201.8], [318.4, -197.4], [329, -183], [340.5, -172.5]], roofZ: 5.6 },
  // Colours from the photos and the 2015 repaint, pulled to the palette's
  // lightness: a weathered wooden deck over periwinkle sides and trestle.
  colours: { deck: ['woodstock-deck', 0xc8b896], spine: ['woodstock-periwinkle-2', 0x8e97cc], supports: ['woodstock-periwinkle', 0xa3abdc] },
  supportR: [0.3, 0.35, 0.4],
  sampleDeg: 12,
  meta: { height: 12.2, trackLength: 413 },
})
