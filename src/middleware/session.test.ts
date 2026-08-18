import { describe, expect, test } from 'bun:test'
import { readSessionIds } from './session'
import { sessionCookieName } from '../config/accounts.config'

const SESSION_ID = 'w7lqmw6rq5s5b2s3ojfj6cbxbnjbzxs7oqzgh4rq'

function req(headers: Record<string, string>) {
  return new Request('http://localhost/admin/boundaries/refresh', { method: 'POST', headers })
}

describe('readSessionIds', () => {
  test('reads the session cookie', () => {
    expect(readSessionIds(req({ cookie: `${sessionCookieName}=${SESSION_ID}` }))).toEqual([
      { id: SESSION_ID, fromCookie: true },
    ])
  })

  test('reads a bearer session id', () => {
    expect(readSessionIds(req({ authorization: `Bearer ${SESSION_ID}` }))).toEqual([
      { id: SESSION_ID, fromCookie: false },
    ])
  })

  test('ignores an API key, which is a different credential entirely', () => {
    expect(readSessionIds(req({ authorization: 'Bearer brm_live_abc123' }))).toEqual([])
  })

  /**
   * The bug this file exists for. The console used to hold an admin bearer in
   * localStorage and send it alongside the session cookie; once that token went
   * stale, returning only the first candidate meant Lucia missed on the bearer
   * and the live cookie was never read. `/admin/*` then fell through to the API
   * key path and told a signed-in administrator they were "not an administrator
   * key".
   */
  test('a stale bearer does not hide the session cookie', () => {
    const candidates = readSessionIds(
      req({ authorization: 'Bearer old-shared-admin-secret', cookie: `${sessionCookieName}=${SESSION_ID}` }),
    )
    expect(candidates).toContainEqual({ id: SESSION_ID, fromCookie: true })
  })

  test('an API key alongside a cookie still leaves the cookie readable', () => {
    expect(
      readSessionIds(req({ authorization: 'Bearer brm_live_abc123', cookie: `${sessionCookieName}=${SESSION_ID}` })),
    ).toEqual([{ id: SESSION_ID, fromCookie: true }])
  })

  test('no credential at all', () => {
    expect(readSessionIds(req({}))).toEqual([])
  })
})
