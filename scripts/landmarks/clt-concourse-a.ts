/**
 * Charlotte Douglas International Airport (CLT): Concourse A — procedural,
 * CC0-1.0.
 *
 *   bun scripts/landmarks/clt-concourse-a.ts
 *
 * One OSM outline (way/1414163869) holding three generations: the 1986 pier
 * running west from the terminal, the long connector north along runway
 * 18C/36C, and the two Concourse A North gate piers (2018 and its 2020s
 * extension) at the connector's end.
 *
 * The older parts are plain blocks, 10.5 m with the west end of the pier
 * at 9 m, with gate-level window panels and a pale roof. The A North piers
 * are glass halls with pale mullions under one long curved roof each: low
 * on the south side, rising to a broad crown north of the middle and
 * rolling down to a thin overhanging silver edge on the north, with a
 * glazed clerestory along the crown over the middle of the pier (Commons,
 * "CLT Concourse A North with United Aircraft (2020)": the silver wing-like
 * roof edge, dark glass walls, the clerestories on the roof). The piers end
 * square, as OSM draws them.
 *
 * Heights and the roof section (13.6 m at the south eave, 18 m crown, 17.2 m
 * at the north edge, clerestory 18.7 m) were checked against renders of
 * Mapbox's 3D buildings (tileset mapbox.mapbox-3dbuildings-v1), used only as
 * a visual reference; the geometry is OSM's outline and our own modelling.
 *
 * Site frame and helpers: see clt-terminal.ts.
 */
import { Part } from './mesh'
import { CLT, bandTo, block, cap, capTo, clean, clip, inset, panels, profile, rect, save, span, walls, type XY } from './clt-terminal'

const A: XY[] = [[-266.4,-44.5],[-314.1,-44.1],[-313.9,-41.2],[-355.5,31.9],[-355.2,188.0],[-351.6,188.0],[-349.3,233.9],[-355.0,234.0],[-354.5,450.6],[-350.5,450.6],[-347.8,496.6],[-497.5,496.9],[-497.0,450.6],[-367.8,450.8],[-368.1,373.4],[-372.6,373.4],[-372.7,365.0],[-371.2,365.0],[-371.2,360.8],[-382.7,360.5],[-383.5,322.4],[-372.5,322.6],[-371.6,298.6],[-367.5,298.6],[-368.0,234.9],[-368.0,233.5],[-376.1,233.7],[-421.4,233.8],[-466.6,234.0],[-496.2,234.4],[-503.1,234.5],[-503.7,221.2],[-503.3,195.2],[-503.4,188.8],[-488.8,188.5],[-457.3,188.0],[-393.9,187.7],[-387.0,187.7],[-367.4,187.8],[-368.7,115.7],[-384.2,116.0],[-384.7,71.9],[-368.6,71.6],[-368.8,27.5],[-357.7,8.7],[-357.2,-32.8],[-363.3,-32.8],[-381.2,-32.6],[-381.2,-36.1],[-383.9,-36.1],[-383.8,-32.7],[-385.6,-32.7],[-391.3,-32.7],[-391.3,-35.9],[-409.7,-35.9],[-409.8,-32.8],[-415.8,-32.7],[-415.8,-35.9],[-419.2,-35.9],[-431.0,-35.9],[-431.0,-32.3],[-435.9,-32.3],[-436.0,-30.6],[-453.3,-30.7],[-482.2,-30.5],[-482.3,-32.4],[-510.8,-32.4],[-518.4,-32.4],[-518.3,-29.0],[-524.0,-29.0],[-524.1,-32.3],[-542.3,-32.2],[-552.8,-32.2],[-552.8,-34.6],[-552.8,-44.1],[-556.6,-44.1],[-556.6,-51.0],[-556.6,-54.1],[-552.8,-54.1],[-552.8,-57.0],[-552.9,-60.5],[-535.2,-60.5],[-535.2,-64.1],[-533.3,-64.1],[-526.8,-64.2],[-526.7,-60.7],[-511.3,-60.7],[-511.3,-64.5],[-509.2,-64.5],[-502.5,-64.6],[-502.4,-60.8],[-492.8,-61.0],[-492.8,-64.1],[-480.1,-64.1],[-480.0,-60.6],[-475.5,-60.6],[-464.6,-60.6],[-464.6,-63.9],[-455.7,-64.0],[-455.7,-65.5],[-420.9,-65.6],[-394.1,-65.7],[-394.0,-63.9],[-385.3,-63.9],[-383.5,-64.0],[-383.6,-60.6],[-381.2,-60.6],[-381.3,-52.8],[-363.1,-52.8],[-329.2,-52.8],[-329.2,-57.7],[-266.5,-57.7],[-266.4,-53.5],[-266.4,-44.5]]
const CENTRE: XY = [-411.51, 215.57]

const stone = new Part(), roof = new Part(), win = new Part(), silver = new Part(), trim = new Part()

const a = clean(A)
const old = (cuts: [number, number, number][], h: number) => {
  let r = a
  for (const [p, q, c] of cuts) r = clip(r, p, q, c)
  if (r.length < 3) return
  const c = block(stone, roof, r, 0, h, 0.45)
  panels(win, c, 3.6, h - 2, { bay: 8, gap: 1.2, minLen: 4 })
}
old([[1, 0, -385], [0, 1, 0]], 9)       // the west end of the 1986 pier
old([[-1, 0, 385], [0, 1, 0]], 10.5)    // its east end, by the terminal
old([[0, -1, 0]], 10.5)                 // the connector north

// The A North piers.
const PIERS: [number, number, number, number][] = [
  [-503.8, 187.4, -348.9, 234.6],
  [-497.9, 450.3, -347.5, 497.2],
]
const SECTION: XY[] = [[-0.05, 13.4], [0, 13.6], [0.18, 15.3], [0.36, 16.8], [0.55, 18.0], [0.72, 17.9], [0.88, 17.6], [1, 17.1], [1.05, 16.6]]
const OVER = 2.2, T = 0.9
for (const [x0, y0, x1, y1] of PIERS) {
  const W = y1 - y0, zr = profile(SECTION.map(([t, z]) => [y0 + t * W, z] as XY))
  // glass walls up to the roof's underside, with pale mullions
  const body = clean(rect(x0 - 0.3, y0 - 0.3, x1 + 0.3, y1 + 0.3))
  bandTo(win, body, 0, (_x, y) => zr(y) - T, 6)
  for (const [y, n] of [[y0 - 0.36, -1], [y1 + 0.36, 1]] as [number, number][])
    for (let x = x0 + 6; x < x1 - 3; x += 12) {
      const q = n < 0 ? [x - 0.35, x + 0.35] : [x + 0.35, x - 0.35]
      trim.quad([q[0], y, 0], [q[1], y, 0], [q[1], y, zr(y) - T], [q[0], y, zr(y) - T])
    }
  bandTo(trim, inset(body, -0.06), 5.2, 6.4, 12)
  // the roof: overhanging on the long sides and the west end, silver edge, pale soffit
  const slab = clean(rect(x0 - OVER, y0 - OVER, x1 + 0.6, y1 + OVER))
  const cuts = span(y0 - OVER, y1 + OVER, 2.4)
  capTo(roof, slab, cuts, (_x, y) => zr(y))
  capTo(trim, slab, cuts, (_x, y) => zr(y) - T, false)
  bandTo(silver, slab, (_x, y) => zr(y) - T, (_x, y) => zr(y), 2.4)
  // the clerestory along the crown, over the middle of the pier
  const L = x1 - x0, ym = y0 + 0.5 * W
  const cl = rect(x0 + 0.45 * L, ym - 2.4, x0 + 0.73 * L, ym + 2.4)
  walls(win, cl, 17, 18.5)
  walls(silver, cl, 18.5, 18.8, inset(cl, 0.3))
  cap(roof, inset(cl, 0.3), 18.8)
}

await save('clt-concourse-a', 'Charlotte Douglas International Airport Concourse A', CENTRE, [
  { part: stone, material: CLT.stone },
  { part: win, material: CLT.window },
  { part: trim, material: CLT.trim },
  { part: roof, material: CLT.roof },
  { part: silver, material: CLT.silver },
])
