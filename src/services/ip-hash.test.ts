import { describe, test, expect } from 'bun:test'
import { createHash } from 'node:crypto'
import { hashIp, ipHashKey } from './accounts.service'

describe('hashIp', () => {
  test('is keyed: not the bare SHA-256 an attacker could brute-force', () => {
    const bare = createHash('sha256').update('203.0.113.7').digest('hex')
    expect(hashIp('203.0.113.7')).not.toBe(bare)
  })

  test('is stable for one address and distinct across addresses', () => {
    expect(hashIp('203.0.113.7')).toBe(hashIp('203.0.113.7'))
    expect(hashIp('203.0.113.7')).not.toBe(hashIp('203.0.113.8'))
  })
})

describe('ipHashKey', () => {
  test('an explicit secret wins', () => {
    expect(ipHashKey({ secret: 's3cret', serviceKey: 'svc' })).toBe('s3cret')
  })

  test('falls back to a key derived from the service key, never the key itself', () => {
    const derived = ipHashKey({ secret: '', serviceKey: 'svc' })
    expect(derived).not.toBe('svc')
    expect(derived).toBe(ipHashKey({ secret: '', serviceKey: 'svc' }))
    expect(derived).not.toBe(ipHashKey({ secret: '', serviceKey: 'other' }))
  })
})
