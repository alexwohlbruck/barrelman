/**
 * Charlotte Douglas International Airport (CLT): the Hourly Parking Deck —
 * procedural, CC0-1.0.
 *
 *   bun scripts/landmarks/clt-hourly-deck.ts
 *
 * The deck facing the terminal across the curbside roadway, built in the
 * CLT 2015 programme with the rental car facility on its lower levels. OSM
 * maps it as a multipolygon (relation/5933200) whose outline bulges round
 * the helix ramps at its two north corners; the round holes are the
 * helices' open wells (the two wedge-shaped holes beside them are left
 * covered).
 *
 * What makes it recognisable is its skin: every straight face is wrapped in
 * a silver metal shell that bulges out like a pillow, vertical at mid-height
 * and curving back in at the top and bottom, over a colonnade at the ground
 * (Flickr, formulanone, "Charlotte Airport Parking Garages - 7 Foot
 * Clearance", 2020, CC BY-SA 2.0: the ribbed silver shell rising behind an
 * open concrete helix). The shell is drawn on OSM's outline, its widest
 * point on the outline, set back 3 m at the roof edge; the short jogs in the
 * outline, where the stair towers break the shell, are dark slots. The
 * helices are solid concrete drums with a dark slot at each turn, standing
 * a parapet above the roof deck.
 *
 * Heights: the roof deck at 24.5 m (seven levels at 3.5 m), the shell's
 * bulge at 12 m, the colonnade 3.6 m. Mapbox's 3D buildings (tileset
 * mapbox.mapbox-3dbuildings-v1) were used only as a visual check of these
 * heights and of the shell's profile; no Mapbox geometry is used.
 *
 * Site frame and helpers: see clt-terminal.ts.
 */
import { Part } from './mesh'
import { finish } from './palette'
import { CLT, column, inset, parkingDeck, save, type XY } from './clt-terminal'

const OUTER: XY[] = [[-9.2,179.4],[-3.9,179.3],[-3.9,172.8],[-3.9,171.5],[-7.1,171.5],[-7.2,170.4],[-2.3,170.3],[-2.5,132.9],[-5.7,132.9],[-5.8,132.3],[-1.7,132.3],[-1.7,124.5],[-5.5,124.5],[-5.5,124.1],[-2.6,124.0],[-2.8,93.9],[-2.9,91.8],[-2.9,89.0],[-3.0,73.5],[-5.5,73.5],[-7.0,73.5],[-8.3,73.5],[-11.2,73.5],[-11.3,70.7],[-87.8,71.3],[-87.9,64.2],[-105.9,64.3],[-110.6,64.4],[-110.7,63.1],[-176.5,63.3],[-176.4,64.3],[-179.0,64.3],[-204.5,64.5],[-204.4,71.2],[-275.0,71.8],[-279.0,71.9],[-279.0,74.2],[-288.6,74.3],[-288.4,90.0],[-288.4,94.5],[-287.8,168.0],[-287.8,171.6],[-287.8,179.9],[-280.9,179.8],[-281.0,185.6],[-282.0,190.6],[-282.8,193.2],[-286.9,198.8],[-288.7,205.5],[-287.9,213.2],[-287.1,214.5],[-285.8,216.8],[-283.9,220.1],[-283.6,220.3],[-277.6,224.7],[-273.9,225.6],[-270.9,226.3],[-269.9,226.5],[-262.1,225.2],[-257.0,221.8],[-254.2,220.7],[-250.5,219.4],[-246.8,218.9],[-241.7,218.6],[-241.7,221.3],[-236.1,221.3],[-233.9,221.3],[-231.4,221.3],[-214.0,221.4],[-214.1,225.4],[-207.3,225.4],[-197.4,225.4],[-197.4,221.1],[-165.9,221.1],[-165.9,225.2],[-157.6,225.3],[-157.6,221.2],[-156.5,221.2],[-153.4,221.2],[-133.8,221.3],[-133.8,225.1],[-129.1,225.1],[-125.9,225.1],[-125.3,225.1],[-125.3,221.0],[-94.3,221.0],[-94.3,225.4],[-88.8,225.4],[-77.8,225.4],[-77.8,220.7],[-48.2,220.8],[-48.2,218.5],[-45.5,218.2],[-41.8,218.4],[-35.3,219.7],[-32.6,222.1],[-29.5,223.9],[-26.6,225.2],[-23.1,225.8],[-19.6,225.9],[-15.7,225.0],[-12.6,224.0],[-9.4,221.8],[-7.2,219.9],[-4.6,216.6],[-3.3,213.6],[-2.3,210.6],[-2.0,209.6],[-1.9,206.3],[-2.2,202.2],[-3.3,198.9],[-5.2,195.4],[-7.3,192.4],[-8.2,189.9],[-8.7,188.3],[-9.0,185.5],[-9.2,184.1],[-9.2,179.4]]
const HELICES: XY[][] = [
  [[-278.1,201.5],[-279.5,205.4],[-279.3,209.4],[-277.7,213.2],[-274.8,216.1],[-271.1,217.8],[-267.0,218.1],[-262.5,216.6],[-259.0,213.4],[-257.2,209.1],[-257.3,204.3],[-259.3,200.1],[-262.7,197.2],[-266.9,195.8],[-271.3,196.1],[-275.3,198.1],[-278.1,201.5]],
  [[-250.8,202.8],[-251.8,198.9],[-256.5,192.6],[-263.4,188.8],[-265.6,187.8],[-267.2,186.2],[-268.1,184.1],[-268.2,179.9],[-250.7,179.8],[-250.3,198.5],[-241.5,206.6],[-246.1,206.3],[-247.7,205.8],[-249.6,204.7],[-250.8,202.8]],
  [[-25.1,187.6],[-28.8,188.6],[-32.0,190.5],[-35.3,193.1],[-37.8,196.5],[-39.7,200.4],[-40.4,204.4],[-42.1,205.4],[-44.1,206.2],[-48.3,206.3],[-39.6,197.8],[-39.5,179.4],[-20.9,179.4],[-20.9,183.2],[-21.8,185.7],[-23.0,187.0],[-25.1,187.6]],
  [[-30.5,201.1],[-31.8,204.9],[-31.6,209.0],[-30.0,212.8],[-27.1,215.7],[-23.4,217.4],[-19.3,217.7],[-14.8,216.2],[-11.3,213.0],[-9.5,208.6],[-9.6,203.9],[-11.6,199.7],[-15.0,196.8],[-19.2,195.4],[-23.6,195.7],[-27.6,197.7],[-30.5,201.1]],
]
const CENTRE: XY = [-145.22, 144.79]
const FLOORS = [0, 3.5, 7, 10.5, 14, 17.5, 21, 24.5]
const TOP = FLOORS.at(-1)!

const parts = { concrete: new Part(), gap: new Part(), deck: new Part() }
const shell = new Part()
const { r, isDrum } = parkingDeck(parts, OUTER, FLOORS, { setback: 3.0, wells: [HELICES[0], HELICES[3]], bands: false, drumReach: 9, drumRise: 4 })

// The shell's section, as (set back from the outline, height).
const PROFILE: XY[] = [[2.4, 3.6], [1.3, 6], [0.45, 9], [0, 12], [0.25, 15.5], [0.9, 19], [1.9, 22.2], [3.0, TOP]]
const rings = PROFILE.map(([d]) => inset(r, d))
// Smooth normals from the section: (outwards, up).
const sn = PROFILE.map((_, k) => {
  const [d0, z0] = PROFILE[Math.max(0, k - 1)], [d1, z1] = PROFILE[Math.min(PROFILE.length - 1, k + 1)]
  const dz = z1 - z0, dd = d1 - d0, l = Math.hypot(dz, dd)
  return [dz / l, dd / l]
})
for (let i = 0; i < r.length; i++) {
  const j = (i + 1) % r.length, a = r[i], b = r[j]
  if (isDrum(a, b)) continue
  const L = Math.hypot(b[0] - a[0], b[1] - a[1]), n: XY = [(b[1] - a[1]) / L, -(b[0] - a[0]) / L]
  // A short jog is a stair tower's slot: the shell stops either side and
  // the dark core shows through.
  if (L < 6) continue
  for (let k = 0; k < PROFILE.length - 1; k++) {
    const z0 = PROFILE[k][1], z1 = PROFILE[k + 1][1], A = rings[k], B = rings[k + 1]
    const N = (s: number[]): [number, number, number] => [n[0] * s[0], n[1] * s[0], s[1]]
    shell.tri([A[i][0], A[i][1], z0], [A[j][0], A[j][1], z0], [B[j][0], B[j][1], z1], undefined, undefined, undefined, [N(sn[k]), N(sn[k]), N(sn[k + 1])])
    shell.tri([A[i][0], A[i][1], z0], [B[j][0], B[j][1], z1], [B[i][0], B[i][1], z1], undefined, undefined, undefined, [N(sn[k]), N(sn[k + 1]), N(sn[k + 1])])
  }
  // close the shell's section where it stops at a slot or a drum
  const C3 = inset(r, 3.0)
  for (const [v, w, dir] of [[i, (i - 1 + r.length) % r.length, -1], [j, (j + 1) % r.length, 1]] as [number, number, number][]) {
    const nb = r[w], here = r[v], Ln = Math.hypot(nb[0] - here[0], nb[1] - here[1])
    if (Ln >= 6 && !isDrum(dir < 0 ? nb : here, dir < 0 ? here : nb)) continue
    const Q: [number, number, number] = [C3[v][0], C3[v][1], PROFILE[0][1]]
    const u: XY = [((b[0] - a[0]) / L) * dir, ((b[1] - a[1]) / L) * dir]
    for (let k = 0; k < PROFILE.length - 1; k++) {
      const P0: [number, number, number] = [rings[k][v][0], rings[k][v][1], PROFILE[k][1]], P1: [number, number, number] = [rings[k + 1][v][0], rings[k + 1][v][1], PROFILE[k + 1][1]]
      // wind so the face looks along the shell, away from it
      const e1 = [P0[0] - Q[0], P0[1] - Q[1], P0[2] - Q[2]], e2 = [P1[0] - Q[0], P1[1] - Q[1], P1[2] - Q[2]]
      const nx = e1[1] * e2[2] - e1[2] * e2[1], ny = e1[2] * e2[0] - e1[0] * e2[2]
      if (nx * u[0] + ny * u[1] >= 0) shell.tri(Q, P0, P1)
      else shell.tri(Q, P1, P0)
    }
  }
  // the shell's underside, back to the core, over the colonnade
  const A = rings[0], C = inset(r, 3.0), z = PROFILE[0][1]
  shell.quad([A[i][0], A[i][1], z], [C[i][0], C[i][1], z], [C[j][0], C[j][1], z], [A[j][0], A[j][1], z])
  // columns under it
  const k = Math.max(1, Math.round(L / 9))
  for (let q = 0; q < k; q++) {
    const t = (q + 0.5) / k
    column(parts.concrete, a[0] + (b[0] - a[0]) * t - n[0] * 1.6, a[1] + (b[1] - a[1]) * t - n[1] * 1.6, 0.45, 0, z, 6)
  }
}

await save('clt-hourly-deck', 'Charlotte Douglas International Airport Hourly Parking Deck', CENTRE, [
  { part: parts.concrete, material: finish('concrete', 0xdcd9d2) },
  { part: parts.gap, material: finish('open-floor', 0x62676b) },
  { part: parts.deck, material: CLT.roof },
  { part: shell, material: CLT.silver },
])
