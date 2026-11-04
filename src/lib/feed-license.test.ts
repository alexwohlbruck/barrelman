import { describe, test, expect, afterEach } from 'bun:test'
import {
  forbidsCommercialUse,
  isExcludedByPolicy,
  licenseAnswer,
  licensePolicy,
  parseGbfsLicense,
  parseTransitlandLicense,
} from './feed-license'

/** A Transitland feed record's licence, as the REST API returns it. */
const MTA_LICENSE = {
  spdx_identifier: '',
  url: 'https://www.mta.info/developers/terms',
  use_without_attribution: 'yes',
  create_derived_product: 'yes',
  redistribution_allowed: 'unknown',
  commercial_use_allowed: 'yes',
  share_alike_optional: 'unknown',
  attribution_text: '',
  attribution_instructions: '',
}

describe('parseTransitlandLicense', () => {
  test('maps every field and reads blank strings as absent', () => {
    expect(parseTransitlandLicense({ onestop_id: 'f-dr5r-nyctsubway', license: MTA_LICENSE })).toEqual({
      spdxIdentifier: null,
      url: 'https://www.mta.info/developers/terms',
      useWithoutAttribution: 'yes',
      createDerivedProduct: 'yes',
      redistributionAllowed: 'unknown',
      commercialUseAllowed: 'yes',
      shareAlikeOptional: 'unknown',
      attributionText: null,
      attributionInstructions: null,
    })
  })

  test('returns null for a record with no licence, or one with every field blank', () => {
    expect(parseTransitlandLicense({ onestop_id: 'f-x' })).toBeNull()
    expect(parseTransitlandLicense({ license: { spdx_identifier: '', url: '  ', commercial_use_allowed: '' } })).toBeNull()
  })

  test('drops an answer outside yes/no/unknown rather than passing it on', () => {
    const parsed = parseTransitlandLicense({ license: { spdx_identifier: 'CC-BY-4.0', commercial_use_allowed: 'maybe' } })
    expect(parsed?.spdxIdentifier).toBe('CC-BY-4.0')
    expect(parsed?.commercialUseAllowed).toBeNull()
  })
})

describe('licenseAnswer', () => {
  test('accepts booleans and any case', () => {
    expect(licenseAnswer(false)).toBe('no')
    expect(licenseAnswer(true)).toBe('yes')
    expect(licenseAnswer(' NO ')).toBe('no')
    expect(licenseAnswer(undefined)).toBeNull()
  })
})

describe('the commercial policy', () => {
  const saved = process.env.GTFS_LICENCE_POLICY
  afterEach(() => {
    if (saved === undefined) delete process.env.GTFS_LICENCE_POLICY
    else process.env.GTFS_LICENCE_POLICY = saved
  })

  test('counts only an explicit no as forbidding', () => {
    expect(forbidsCommercialUse({ commercialUseAllowed: 'no' })).toBe(true)
    expect(forbidsCommercialUse({ redistributionAllowed: 'no' })).toBe(true)
    expect(forbidsCommercialUse({ commercialUseAllowed: 'unknown', redistributionAllowed: 'unknown' })).toBe(false)
    expect(forbidsCommercialUse({ commercialUseAllowed: 'yes', redistributionAllowed: null })).toBe(false)
    // No licence on record is served.
    expect(forbidsCommercialUse(null)).toBe(false)
  })

  test('excludes nothing when the policy is off', () => {
    expect(isExcludedByPolicy({ commercialUseAllowed: 'no' }, 'off')).toBe(false)
    expect(isExcludedByPolicy({ commercialUseAllowed: 'no' }, 'commercial')).toBe(true)
  })

  test('defaults to off, and falls back to off on a value it does not know', () => {
    delete process.env.GTFS_LICENCE_POLICY
    expect(licensePolicy()).toBe('off')
    process.env.GTFS_LICENCE_POLICY = ''
    expect(licensePolicy()).toBe('off')
    process.env.GTFS_LICENCE_POLICY = 'strict'
    expect(licensePolicy()).toBe('off')
    process.env.GTFS_LICENCE_POLICY = 'commercial'
    expect(licensePolicy()).toBe('commercial')
  })
})

describe('parseGbfsLicense', () => {
  test('reads v2.3 fields', () => {
    expect(parseGbfsLicense({
      system_id: 'citibike',
      license_url: 'https://ride.citibikenyc.com/data-sharing-policy',
      attribution_organization_name: 'Lyft',
      attribution_url: 'https://lyft.com',
    })).toEqual({
      licenseId: null,
      licenseUrl: 'https://ride.citibikenyc.com/data-sharing-policy',
      attributionOrganizationName: 'Lyft',
      attributionUrl: 'https://lyft.com',
    })
  })

  test('reads a v3 localised organisation name', () => {
    expect(parseGbfsLicense({
      license_id: 'CC0-1.0',
      attribution_organization_name: [{ text: 'Bcycle', language: 'en' }],
    })?.attributionOrganizationName).toBe('Bcycle')
  })

  test('returns null for a system that publishes none', () => {
    expect(parseGbfsLicense({ system_id: 'x', name: 'X' })).toBeNull()
    expect(parseGbfsLicense(undefined)).toBeNull()
  })
})
