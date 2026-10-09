import { describe, test, expect, afterEach } from 'bun:test'
import { fetchEntityBatch, queryableQid } from './brand-logos'

const realFetch = globalThis.fetch
afterEach(() => { globalThis.fetch = realFetch })

const entity = (logo: string | null, description: string | null) => ({
  descriptions: description ? { en: { value: description } } : {},
  claims: logo ? { P154: [{ mainsnak: { datavalue: { value: logo } } }] } : {},
})

describe('fetchEntityBatch', () => {
  test('a failed request answers for no QID, so none is recorded as logo-less', async () => {
    // Recording them is how Starbucks lost its logo for good.
    globalThis.fetch = (async () => new Response('down', { status: 503, headers: { 'retry-after': '1' } })) as any
    const out = await fetchEntityBatch(['Q37158', 'Q38076'])
    expect(out.size).toBe(0)
  }, 15_000)

  test('a rate-limited request is retried', async () => {
    let calls = 0
    globalThis.fetch = (async () => {
      calls++
      return calls === 1
        ? new Response('slow down', { status: 429, headers: { 'retry-after': '1' } })
        : Response.json({ entities: { Q37158: entity('Starbucks coffee wordmark.png', 'coffee company') } })
    }) as any
    const out = await fetchEntityBatch(['Q37158'])
    expect(calls).toBe(2)
    expect(out.get('Q37158')?.logoUrl).toContain('Starbucks%20coffee%20wordmark.png')
  }, 15_000)

  test('an entity Wikidata no longer has is recorded as logo-less', async () => {
    globalThis.fetch = (async () => Response.json({ entities: { Q1: { id: 'Q1', missing: '' } } })) as any
    const out = await fetchEntityBatch(['Q1'])
    expect(out.get('Q1')).toEqual({ logoUrl: null, description: null })
  })
})

describe('queryableQid', () => {
  test('takes the first of several IDs', () => {
    // One "Q155026;Q7771029" in a batch made Wikidata reject all 50.
    expect(queryableQid('Q155026;Q7771029')).toBe('Q155026')
    expect(queryableQid(' Q37158 ')).toBe('Q37158')
  })

  test('rejects a value that is not an ID', () => {
    for (const v of ['', 'starbucks', 'Q', 'Q0', 'https://www.wikidata.org/wiki/Q37158']) {
      expect(queryableQid(v)).toBeNull()
    }
  })
})
