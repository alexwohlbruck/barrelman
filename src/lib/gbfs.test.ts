import { describe, expect, it } from 'bun:test'
import {
  gbfsId,
  gbfsNumber,
  lastReportedIso,
  localizedText,
  normalizeVehicleTypes,
  parseStationStatus,
} from './gbfs'

// Shapes taken from live feeds: Citi Bike (Lyft, GBFS 2.3) and Pittsburgh's
// POGOH (PBSC, GBFS 3.0).
const citiBikeTypes = normalizeVehicleTypes([
  { form_factor: 'bicycle', propulsion_type: 'human', vehicle_type_id: '1' },
  { form_factor: 'bicycle', propulsion_type: 'electric_assist', vehicle_type_id: '2' },
])
const pogohTypes = normalizeVehicleTypes([
  { vehicle_type_id: 'ICONIC', form_factor: 'bicycle', propulsion_type: 'human',
    name: [{ text: 'ICONIC', language: 'en' }, { text: 'ICONIC', language: 'fr' }] },
  { vehicle_type_id: 'EFIT', form_factor: 'bicycle', propulsion_type: 'electric_assist' },
  { vehicle_type_id: 'COSMO', form_factor: 'scooter_standing', propulsion_type: 'electric' },
])

describe('localizedText', () => {
  it('passes a v2 string through', () => {
    expect(localizedText('W 42 St & 6 Ave')).toBe('W 42 St & 6 Ave')
  })

  it('prefers English in v3 localized text', () => {
    expect(localizedText([
      { text: 'Rue Pierce', language: 'fr' },
      { text: 'Pierce St', language: 'en' },
    ])).toBe('Pierce St')
  })

  it('falls back to the first entry, and to empty for junk', () => {
    expect(localizedText([{ text: 'Calle Pierce', language: 'es' }])).toBe('Calle Pierce')
    expect(localizedText(undefined)).toBe('')
    expect(localizedText([{ language: 'en' }])).toBe('')
  })
})

describe('lastReportedIso', () => {
  it('reads v2 POSIX seconds and v3 RFC 3339', () => {
    expect(lastReportedIso(1791584327)).toBe('2026-10-09T22:18:47.000Z')
    expect(lastReportedIso('2026-10-08T22:19:10.389Z')).toBe('2026-10-08T22:19:10.389Z')
  })

  it('returns null rather than throwing on an unparseable value', () => {
    expect(lastReportedIso('not a date')).toBeNull()
    expect(lastReportedIso(undefined)).toBeNull()
  })
})

describe('normalizeVehicleTypes', () => {
  it('maps the stored snake_case feed objects to the documented shape', () => {
    expect(pogohTypes[0]).toEqual({
      vehicleTypeId: 'ICONIC', formFactor: 'bicycle', propulsionType: 'human', name: 'ICONIC',
    })
  })
})

describe('parseStationStatus', () => {
  it('splits a v2 total using the explicit e-bike count', () => {
    const s = parseStationStatus({
      station_id: 'x', num_bikes_available: 3, num_ebikes_available: 3,
      num_docks_available: 48, is_renting: 1, is_returning: 1, last_reported: 1791584327,
      vehicle_types_available: [{ vehicle_type_id: '1', count: 0 }, { vehicle_type_id: '2', count: 3 }],
    }, citiBikeTypes)
    expect(s).toMatchObject({ numBikesAvailable: 0, numEbikesAvailable: 3, numDocksAvailable: 48 })
  })

  it('reads a v3 station, classifying vehicles by their declared type', () => {
    const s = parseStationStatus({
      station_id: '1', num_vehicles_available: 9, num_docks_available: 9,
      last_reported: '2026-10-08T22:19:10.389Z', is_renting: true, is_returning: true,
      vehicle_types_available: [
        { vehicle_type_id: 'ICONIC', count: 5 },
        { vehicle_type_id: 'EFIT', count: 3 },
        { vehicle_type_id: 'COSMO', count: 1 },
      ],
    }, pogohTypes)
    expect(s).toEqual({
      numBikesAvailable: 5, numEbikesAvailable: 3, numScootersAvailable: 1,
      numDocksAvailable: 9, isRenting: true, isReturning: true,
      lastReported: '2026-10-08T22:19:10.389Z',
    })
  })
})

describe('gbfsId', () => {
  it('reads numeric ids as the text they are stored and looked up by', () => {
    // goabout sends "station_id": 542; gbfs_stations.station_id is text.
    expect(gbfsId(542)).toBe('542')
    expect(gbfsId('W 42 St')).toBe('W 42 St')
  })

  it('is null when there is no id, so id-less rows are skipped, not merged', () => {
    expect(gbfsId(undefined)).toBeNull()
    expect(gbfsId(null)).toBeNull()
    expect(gbfsId('  ')).toBeNull()
  })
})

describe('gbfsNumber', () => {
  it('accepts numbers, numeric strings and zero', () => {
    expect(gbfsNumber(52.11)).toBe(52.11)
    expect(gbfsNumber('52.11')).toBe(52.11)
    expect(gbfsNumber(0)).toBe(0)
  })

  it('is null for anything that is not a finite number', () => {
    expect(gbfsNumber('')).toBeNull()
    expect(gbfsNumber(undefined)).toBeNull()
    expect(gbfsNumber('10); DROP TABLE gbfs_stations; --')).toBeNull()
  })
})
