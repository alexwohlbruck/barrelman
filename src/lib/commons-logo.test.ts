import { describe, test, expect } from 'bun:test'
import { fetchLogoInfo, logoFileFromUrl, parseLogoLicense, pickDownload, stripHtml, MAX_SVG_BYTES, WIKIMEDIA_UA } from './commons-logo'

// extmetadata as Commons returned it for File:McDonald's Golden Arches.svg.
const MCDONALDS = {
  Artist: { value: '<a href="https://en.wikipedia.org/wiki/McDonald%27s" class="extiw" title="w:McDonald\'s">McDonald&#39;s</a>' },
  LicenseShortName: { value: 'Public domain' },
  AttributionRequired: { value: 'false' },
  Restrictions: { value: 'trademarked' },
  License: { value: 'pd' },
}

describe('parseLogoLicense', () => {
  test('reads a public-domain trademark', () => {
    expect(parseLogoLicense(MCDONALDS)).toEqual({
      name: 'Public domain',
      url: null,
      artist: "McDonald's",
      attributionRequired: false,
      publicDomain: true,
      trademarked: true,
    })
  })

  test('reads a CC licence that asks for credit', () => {
    const l = parseLogoLicense({
      LicenseShortName: { value: 'CC BY-SA 4.0' },
      LicenseUrl: { value: 'https://creativecommons.org/licenses/by-sa/4.0' },
      License: { value: 'cc-by-sa-4.0' },
      Artist: { value: 'Jane Doe' },
      AttributionRequired: { value: 'true' },
    })
    expect(l.name).toBe('CC BY-SA 4.0')
    expect(l.url).toBe('https://creativecommons.org/licenses/by-sa/4.0')
    expect(l.publicDomain).toBe(false)
    expect(l.attributionRequired).toBe(true)
    expect(l.trademarked).toBe(false)
  })

  test('assumes credit is owed when Commons does not say', () => {
    expect(parseLogoLicense({}).attributionRequired).toBe(true)
    expect(parseLogoLicense(undefined).name).toBeNull()
  })
})

describe('stripHtml', () => {
  test('drops the hidden duplicate Commons puts in credits', () => {
    expect(stripHtml('Unknown source<span style="display: none;">Unknown source</span>, but may be obtained from X')).toBe(
      'Unknown source, but may be obtained from X',
    )
  })
})

describe('logoFileFromUrl', () => {
  test('recovers the file name from a stored Special:FilePath URL', () => {
    expect(
      logoFileFromUrl('https://commons.wikimedia.org/wiki/Special:FilePath/Starbucks%20Corporation%20Logo_2011.svg?width=200'),
    ).toBe('Starbucks Corporation Logo 2011.svg')
    expect(logoFileFromUrl('https://example.com/logo.png')).toBeNull()
    expect(logoFileFromUrl(null)).toBeNull()
  })
})

describe('pickDownload', () => {
  test('takes a small SVG as it is, and Commons\' thumbnail for anything else', () => {
    expect(pickDownload({ mime: 'image/svg+xml', size: 400, url: 'orig.svg', thumburl: 'thumb.png' })).toBe('orig.svg')
    expect(pickDownload({ mime: 'image/svg+xml', size: MAX_SVG_BYTES + 1, url: 'orig.svg', thumburl: 'thumb.png' })).toBe('thumb.png')
    expect(pickDownload({ mime: 'image/png', size: 400, url: 'orig.png', thumburl: 'thumb.png' })).toBe('thumb.png')
  })
})

describe('fetchLogoInfo', () => {
  test('maps answers back to the names asked for, and says who is asking', async () => {
    let ua = ''
    const fetchFn = (async (_url: string, init?: RequestInit) => {
      ua = (init?.headers as Record<string, string>)['User-Agent']
      return Response.json({
        query: {
          normalized: [{ from: 'File:gone.svg', to: 'File:Gone.svg' }],
          pages: [
            {
              title: "File:McDonald's Golden Arches.svg",
              imageinfo: [
                {
                  mime: 'image/svg+xml',
                  size: 389,
                  url: 'https://upload.test/m.svg',
                  thumburl: 'https://thumb.test/m.png',
                  descriptionurl: 'https://commons.test/File:M',
                  extmetadata: MCDONALDS,
                },
              ],
            },
            { title: 'File:Gone.svg', missing: true },
          ],
        },
      })
    }) as unknown as typeof fetch
    const out = await fetchLogoInfo(["McDonald's_Golden_Arches.svg", 'gone.svg'], fetchFn)
    expect(ua).toBe(WIKIMEDIA_UA)
    expect(out.get("McDonald's_Golden_Arches.svg")).toMatchObject({
      publicDomain: true,
      downloadUrl: 'https://upload.test/m.svg',
      pageUrl: 'https://commons.test/File:M',
    })
    expect(out.get('gone.svg')).toBeNull()
  })

  test('a refused request answers for no file, so none is recorded', async () => {
    const fetchFn = (async () => new Response('nope', { status: 403 })) as unknown as typeof fetch
    expect((await fetchLogoInfo(['A.svg'], fetchFn)).size).toBe(0)
  })
})
