import { describe, expect, it } from 'bun:test'
import { geoidGap, terrainHeight } from './datum'

// Each `ele` was tagged as lidar NAVD88 + (GEOID18 N − EGM96 N), with the geoid
// heights from NGS and GeographicLib GeoidEval at the bridge (landmarks
// docs/bridge-heights.md); converting it gives the lidar's NAVD88 back.
const tagged = [
  { city: 'Pittsburgh, Fort Pitt Bridge', lng: -80.005, lat: 40.444, navd88: 233.4, gap: 0.83 },
  { city: 'Minneapolis, Stone Arch Bridge', lng: -93.25488, lat: 44.98065, navd88: 236.1, gap: 0.75 },
  { city: 'Cleveland, Hope Memorial Bridge', lng: -81.6926, lat: 41.4901, navd88: 207.6, gap: 0.52 },
]

describe('terrainHeight', () => {
  for (const t of tagged)
    it(`gives back the NAVD88 lidar an EGM96 ele was tagged from in ${t.city}`, () => {
      const ele = Math.round((t.navd88 + t.gap) * 100) / 100
      expect(Math.abs(terrainHeight(ele, t.lng, t.lat) - t.navd88)).toBeLessThan(0.05)
      expect(Math.abs(geoidGap(t.lng, t.lat)! - t.gap)).toBeLessThan(0.05)
    })

  it('leaves ele as it stands where the terrain is not on NAVD88', () => {
    for (const [place, lng, lat] of [
      ['Toronto, Canadian terrain on CGVD2013', -79.38, 43.65],
      ['Windsor, across the river from Detroit', -83.04, 42.3],
      ['Tijuana, in Mexico inside the grid', -117.03, 32.52],
      ['Anchorage, north of the grid', -149.9, 61.2],
      ['London, on Copernicus and national terrain', -0.12, 51.5],
    ] as const) {
      expect(geoidGap(lng, lat), place).toBeNull()
      expect(terrainHeight(12.3, lng, lat), place).toBe(12.3)
    }
  })

  it('converts right up to the border on the US side', () => {
    expect(geoidGap(-83.046, 42.331)).toBeCloseTo(0.71, 1) // Detroit
    expect(geoidGap(-117.04, 32.555)).not.toBeNull() // San Ysidro
  })
})
