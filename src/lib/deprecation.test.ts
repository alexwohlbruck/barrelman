import { describe, expect, test } from 'bun:test'
import Elysia from 'elysia'
import { deprecated, deprecationHeaders } from './deprecation'

describe('deprecationHeaders', () => {
  test('formats Deprecation as an RFC 9745 date and Sunset as an HTTP-date', () => {
    const headers = deprecationHeaders({
      since: '2027-01-01T00:00:00Z',
      sunset: '2027-04-01T00:00:00Z',
      link: 'https://docs.barrelman.dev/changelog',
    })

    expect(headers.deprecation).toBe('@1798761600')
    expect(headers.sunset).toBe('Thu, 01 Apr 2027 00:00:00 GMT')
    expect(headers.link).toBe('<https://docs.barrelman.dev/changelog>; rel="deprecation sunset"')
  })

  test('omits Sunset and Link when they are not given', () => {
    expect(deprecationHeaders({ since: new Date(0) })).toEqual({ deprecation: '@0' })
  })

  test('refuses an invalid date or a sunset before the deprecation', () => {
    expect(() => deprecationHeaders({ since: 'soon' })).toThrow()
    expect(() => deprecationHeaders({ since: '2027-04-01', sunset: '2027-01-01' })).toThrow()
  })
})

describe('deprecated guard', () => {
  test('stamps the headers on the response without changing it', async () => {
    const app = new Elysia()
      .onBeforeHandle(deprecated({ since: '2027-01-01T00:00:00Z', sunset: '2027-04-01T00:00:00Z' }))
      .get('/old', () => ({ ok: true }))

    const res = await app.handle(new Request('http://localhost/old'))

    expect(res.status).toBe(200)
    expect(await res.json()).toEqual({ ok: true })
    expect(res.headers.get('deprecation')).toBe('@1798761600')
    expect(res.headers.get('sunset')).toBe('Thu, 01 Apr 2027 00:00:00 GMT')
  })

  test('appends to a Link header the route already set', () => {
    const set = { headers: { link: '<https://a.example>; rel="next"' } as Record<string, string | number> }
    deprecated({ since: '2027-01-01', link: 'https://b.example' })({ set })

    expect(set.headers.link).toBe('<https://a.example>; rel="next", <https://b.example>; rel="deprecation"')
  })
})
