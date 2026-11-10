import { describe, test, expect } from 'bun:test'
import { isNearMissOfStart, logoAttributionOf, logoLicenseOf, logoUrlOf } from './brands.service'
import { brandLogoUrl } from './brand-logo-files.service'

// Pairs measured against the US brand catalog: trigram scores put each of the
// rejected ones in the same range as a real typo.
describe('isNearMissOfStart', () => {
  test('accepts a typo of the brand name', () => {
    for (const [q, name] of [
      ['starbuks', 'Starbucks'], ['wallmart', 'Walmart'], ['mcdonals', "McDonald's"],
      ['chik fil a', 'Chick-fil-A'], ['harris teter', 'Harris Teeter'], ['walgren', 'Walgreens'],
      ['mc donalds', "McDonald's"], ['star bucks', 'Starbucks'],
    ]) expect(isNearMissOfStart(q, name)).toBe(true)
  })

  test('ignores a leading "The"', () => {
    expect(isNearMissOfStart('home depo', 'The Home Depot')).toBe(true)
  })

  test('rejects a different word that shares the letters', () => {
    for (const [q, name] of [
      ['charleston', 'Charles Schwab'], ['charleston', 'Charles Tyrwhitt'],
      ['columbus', 'Columbia'], ['power outlet', 'Home Outlet'],
    ]) expect(isNearMissOfStart(q, name)).toBe(false)
  })

  test('rejects a typed word that is a different word in the name', () => {
    for (const [q, name] of [
      ['central park', 'Central Market'], ['central park', 'Central Bank'],
    ]) expect(isNearMissOfStart(q, name)).toBe(false)
  })

  test('rejects a word that is only somewhere in the name', () => {
    expect(isNearMissOfStart('columbus', 'Knights of Columbus')).toBe(false)
  })
})

describe('brand logo fields', () => {
  const row = {
    wikidata: 'Q38076;Q1',
    logo_file: "McDonald's Golden Arches.svg",
    logo_license: 'Public domain',
    logo_artist: "McDonald's",
    logo_attribution_required: false,
    logo_public_domain: true,
    logo_trademarked: true,
    logo_page_url: 'https://commons.wikimedia.org/wiki/File:M',
    logo_download_url: 'https://upload.wikimedia.org/m.svg',
    logo_meta_at: new Date(),
  }

  test('logoUrl is our copy, keyed by the first QID and versioned by file', () => {
    const url = brandLogoUrl(row.wikidata, row.logo_file, 'https://api.test')!
    expect(url).toMatch(/^https:\/\/api\.test\/brand-logos\/Q38076\?v=[0-9a-f]{10}$/)
    expect(brandLogoUrl(row.wikidata, 'Other.svg', 'https://api.test')).not.toBe(url)
    expect(brandLogoUrl(null, row.logo_file)).toBeNull()
    expect(brandLogoUrl('Q38076', null)).toBeNull()
  })

  test('licence and credit come from the row once Commons has answered', () => {
    expect(logoLicenseOf(row)).toEqual({
      name: 'Public domain',
      url: null,
      attributionRequired: false,
      publicDomain: true,
      trademarked: true,
      source: 'https://commons.wikimedia.org/wiki/File:M',
    })
    expect(logoAttributionOf(row)).toBe("McDonald's, Public domain, via Wikimedia Commons")
    expect(logoLicenseOf({ ...row, logo_meta_at: null })).toBeNull()
    expect(logoAttributionOf({ ...row, logo_meta_at: null })).toBeNull()
  })

  test('a row from before logo_file existed still gets a URL', () => {
    const old = {
      wikidata: 'Q38076',
      logo_url: 'https://commons.wikimedia.org/wiki/Special:FilePath/McDonald%27s%20Golden%20Arches.svg?width=200',
    }
    expect(logoUrlOf(old)).toBe(brandLogoUrl('Q38076', "McDonald's Golden Arches.svg"))
  })

  test('no URL or licence for a file Commons no longer has', () => {
    const gone = { ...row, logo_download_url: null }
    expect(logoUrlOf(gone)).toBeNull()
    expect(logoLicenseOf(gone)).toBeNull()
    expect(logoAttributionOf(gone)).toBeNull()
  })
})
