/**
 * Snoopy's Racing Railway, Carowinds: procedural, CC0-1.0.
 * bun scripts/landmarks/carowinds-snoopys-racing-railway.ts   (REPORT=1 for the element table)
 *
 * Map frame: x east, y north, z up, metres; bearing 0, so the frame is true
 * north. The origin is the middle of the ride's footprint (see the anchor), at
 * the lowest ground under the track. Built with ./coaster-kit.ts.
 *
 * Plan: the circuit is the OSM `roller_coaster=track` ways through the
 * station (way/1495972517): way/1495972532, 1495975650, 1495975649,
 * 1495975647, 1495975648, 1495972533, 1495972534 and 1495975646, end to end:
 * 250 m against the published 245 m. None is tagged oneway; the train is
 * taken to leave the station northwards, because that is the only straight
 * out of it long enough for the tyre launch the twin at Canada's Wonderland
 * starts with, and because the second straight shed (way/1495972514) then
 * boosts the train into the climb over the crossing that OSM tags layer 1.
 * The two small sheds the track runs through (way/1495972513, 1495972514)
 * are buildings of their own and stay on the map.
 *
 * Profile: an ART Engineering Family Launch Coaster (245 m model; RCDB 22011,
 * opened 2025), 39.4 ft (12 m) high, 31.1 mph, two tyre-driven launches, a
 * copy of Fridolin's Zauberexpress (Fantasiana) like the one at Canada's
 * Wonderland. Positions are metres along the OSM polyline (250 m round) and
 * % of it:
 *
 *   launch out of the station  s 0–16 (0–6%)
 *   north turnaround, 4.5 m    s 20–48 (8–19%)
 *   through the first shed     s 66–76 (26–30%)
 *   east turnaround, 4 m       s 105–128 (42–51%)
 *   boost in the second shed   s 132–145 (53–58%)
 *   over the crossing to the top, 10.5 m  s 158–166 (63–66%)
 *   down over the brake run    s 187 (75%), 6 m up
 *   south turnaround, brakes   s 195–238 (78–95%)
 *
 * Photos: no open-licence photo of the Carowinds ride could be found, so the
 * shape and colours come from its twin, Snoopy's Racing Railway at Canada's
 * Wonderland: "Snoopy's Racing Railway Offside", "Plaza", "Station" and
 * "Launch" (Bigtime Boy, 2023–24, CC BY-SA 4.0, Wikimedia Commons), and
 * Coasterpedia's "grey track and brown supports".
 *
 * Style: a grey ribbon on thick brown timber-look columns; the station is its
 * roof on posts.
 */
import { buildCoaster } from './coaster-kit'

// The OSM circuit, node by node, in the park frame (see coaster-kit), starting
// at the north end of the station (the first node of way/1495972532).
const CHAIN: [number, number][] = [
  [338.8, -109.7], [338.7, -89.7], [339.2, -86.2], [340.2, -83.9], [341.5, -82.4], [343.2, -81.4], [345.2, -81.0], [347.4, -81.3],
  [349.2, -82.0], [351.0, -83.6], [352.1, -85.5], [352.7, -87.6], [352.8, -90.0], [352.5, -92.9], [350.2, -106.9], [349.9, -109.4],
  [349.9, -110.7], [349.9, -111.8], [350.2, -113.7], [350.9, -115.4], [351.9, -116.8], [353.3, -118.0], [354.8, -118.7], [356.8, -119.1],
  [358.6, -119.0], [360.9, -118.2], [362.7, -116.9], [364.6, -115.1], [367.4, -112.4], [369.5, -111.0], [371.8, -110.2], [374.3, -109.9],
  [376.7, -110.1], [380.8, -110.6], [383.0, -110.5], [384.9, -110.0], [386.5, -109.0], [387.7, -107.5], [388.3, -105.7], [388.3, -104.0],
  [387.7, -102.3], [386.6, -100.9], [385.1, -100.0], [383.2, -99.6], [381.1, -99.7], [379.0, -100.1], [375.7, -101.3], [363.0, -105.6],
  [353.8, -108.7], [351.8, -109.6], [349.9, -110.7], [348.7, -111.9], [347.6, -113.8], [347.0, -115.6], [346.6, -117.4], [346.5, -119.9],
  [346.4, -123.0], [346.1, -126.0], [345.5, -127.8], [344.3, -129.9], [342.9, -131.4], [341.4, -132.4], [339.6, -133.2], [338.8, -133.4],
  [337.0, -133.9], [333.3, -134.7], [331.1, -135.7], [329.4, -136.9], [328.2, -138.5], [327.7, -140.2], [327.6, -141.9], [328.1, -143.9],
  [329.0, -145.2], [330.6, -146.2], [332.1, -146.6], [333.8, -146.6], [335.4, -146.0], [336.6, -145.0], [337.7, -143.5], [338.4, -141.9],
  [338.7, -139.9], [338.8, -138.0], [338.8, -133.4], [338.7, -122.1],
]
// Terrain (Mapzen/AWS Terrarium, open data), 10 m cells from (315, -155); rows
// south to north, columns west to east; metres above 195.4 m.
const GROUND = [
  [1.5, 1.0, 1.0, 0.9, 0.9, 0.6, 0.5, 0.6, 0.4],
  [1.0, 1.1, 0.8, 0.9, 0.8, 0.8, 0.5, 0.3, 0.1],
  [1.0, 0.9, 0.7, 0.7, 0.5, 0.6, 0.5, 0.3, 0.4],
  [1.0, 1.1, 1.0, 1.0, 1.3, 0.9, 0.3, 0.3, 0.1],
  [0.8, 0.8, 0.7, 0.6, 0.3, 0.3, 0.2, 0.2, 0.2],
  [0.4, 0.4, 0.6, 1.1, 1.0, 1.0, 1.0, 0.6, 0.3],
  [0.6, 0.6, 0.8, 1.1, 1.3, 1.6, 1.2, 0.4, 0.4],
  [0.6, 0.7, 0.7, 1.5, 1.6, 0.8, 0.9, 0.7, 0.5],
  [0.6, 0.7, 1.4, 1.8, 0.5, 1.6, 0.8, 0.5, 0.5],
]
// The station building (way/1495972517).
const STATION: [number, number][] = [
  [331.2, -109.7], [332.5, -109.7], [338.8, -109.7], [339.5, -109.7], [341.5, -109.7], [341.5, -110.8], [343.6, -110.8], [343.6, -111.4],
  [343.5, -118.4], [343.5, -120.6], [342.3, -120.6], [341.7, -120.6], [341.7, -122.1], [338.7, -122.1], [332.9, -122.1], [331.2, -122.1],
]

await buildCoaster({
  name: "Snoopy's Racing Railway",
  out: '../../landmarks/models/carowinds-snoopys-racing-railway.glb',
  chain: CHAIN,
  anchor: [358, -114],
  ground: { x0: 315, y0: -155, step: 10, rows: GROUND, base: 195.4 },
  // Heights of the rails above the local ground; s along the OSM polyline (250 m round).
  profile: [
    { name: 'station', s: 0, z: 1.2 },
    { name: 'end of the launch', s: 16, z: 1.2, r: 12 },
    { name: 'north turnaround', s: 34, z: 4.5 },
    { name: 'into the first shed', s: 68, z: 1.2 },
    { name: 'hill', s: 92, z: 3 },
    { name: 'valley', s: 104, z: 1.8, r: 12 },
    { name: 'east turnaround', s: 118, z: 4 },
    { name: 'boost, in', s: 132, z: 1.2 },
    { name: 'boost, out', s: 145, z: 1.2, r: 16 },
    { name: 'top, past the crossing', s: 167, z: 10.5, r: 13 },
    { name: 'south turnaround, over the brakes', s: 208, z: 1.5, r: 14 },
    { name: 'brakes', s: 226, z: 1.2 },
  ],
  lift: [238, 16],
  liftHead: 6,
  // Level through the launch sheds, the brakes and the station.
  banks: [{ from: 130, to: 147, deg: 0, name: 'boost' }, { from: 226, to: 16, deg: 0, name: 'brakes, station, launch' }],
  launches: [{ from: 132, to: 145, z: 1.2, head: 11.5, name: 'boost' }],
  brakes: [{ from: 224, to: 238, z: 1.2, head: 0.4 }],
  losses: [0.015, 0.001],
  track: { W: 1.3, RAIL: 0.3, DEPTH: 0.65, SPINE: 0.45 },
  station: { ring: STATION, posts: [[331.8, -110.3], [343, -111.4], [343, -120], [331.8, -121.5], [337.5, -110.3], [337.5, -121.5]], roofZ: 5.5 },
  // Colours from the twin's photos, pulled to the palette's lightness: grey
  // track, brown timber-look columns.
  colours: { deck: ['racing-grey', 0xa3a8ae], spine: ['racing-grey-2', 0x8d939a], supports: ['racing-brown', 0xa98a6c] },
  supportR: [0.3, 0.34, 0.38],
  supportEvery: 7,
  supportMinZ: 1.4,
  sampleDeg: 12,
  meta: { height: 12, trackLength: 245 },
})
