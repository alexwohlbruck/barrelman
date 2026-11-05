import { describe, test, expect } from 'bun:test'
import { driveBand, pickParkAndRideStations, type ParkAndRideStation } from './park-and-ride'

const station = (stationId: string, distance: number, lot = false): ParkAndRideStation => ({
  feedId: 'rail',
  stationId,
  lat: 0,
  lng: 0,
  distance,
  ...(lot && { lot: { name: `${stationId} lot`, lat: 0, lng: 0 } }),
})

describe('pickParkAndRideStations', () => {
  test('takes the two nearest park_ride stations, then fills with the nearest stations', () => {
    const picked = pickParkAndRideStations(
      [station('pr3', 9000, true), station('pr1', 4000, true), station('pr2', 6000, true)],
      [station('n2', 2000), station('n1', 1500), station('n3', 2500)],
    )
    expect(picked.map((s) => s.stationId)).toEqual(['pr1', 'pr2', 'n1', 'n2'])
  })

  test('lists a station once, keeping its lot', () => {
    const picked = pickParkAndRideStations([station('a', 3000, true)], [station('a', 3000), station('b', 2000)])
    expect(picked.map((s) => s.stationId)).toEqual(['a', 'b'])
    expect(picked[0].lot?.name).toBe('a lot')
  })

  test('uses the nearest stations alone where no lot is tagged', () => {
    const picked = pickParkAndRideStations([], [station('a', 1300), station('b', 1400)])
    expect(picked.map((s) => s.stationId)).toEqual(['a', 'b'])
  })
})

describe('driveBand', () => {
  const midtown = { lat: 40.7549, lng: -73.984 }

  test('caps the drive at 60% of the trip, so the train does the rest', () => {
    const bayRidge = { lat: 40.6264, lng: -74.0299 }
    const band = driveBand(bayRidge, midtown, 1800)
    expect(band.max).toBeCloseTo(band.trip * 0.6, 0)
    expect(band.min).toBe(1200)
  })

  test('caps the drive by the access budget on a long trip', () => {
    const philadelphia = { lat: 39.9526, lng: -75.1652 }
    expect(driveBand(philadelphia, midtown, 1200).max).toBe(12_000)
  })

  test('leaves no room on a trip too short to drive part of', () => {
    const band = driveBand({ lat: 40.75, lng: -73.99 }, midtown, 1800)
    expect(band.max).toBeLessThan(band.min)
  })
})
