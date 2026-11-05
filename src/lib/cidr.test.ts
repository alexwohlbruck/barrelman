import { describe, test, expect } from 'bun:test'
import { ipAllowed, parseCidr, parseCidrList, parseIp } from './cidr'

const list = (text: string) => parseCidrList(text).ranges

describe('parseIp', () => {
  test('reads IPv4, IPv6 and bracketed IPv6', () => {
    expect(parseIp('192.0.2.1')).toEqual(new Uint8Array([192, 0, 2, 1]))
    expect(parseIp('::1')?.length).toBe(16)
    expect(parseIp('[2001:db8::1]')?.length).toBe(16)
  })

  test('folds an IPv4-mapped address to IPv4', () => {
    expect(parseIp('::ffff:192.0.2.1')).toEqual(new Uint8Array([192, 0, 2, 1]))
  })

  test('rejects junk', () => {
    for (const bad of ['', 'unknown', '256.0.0.1', '1.2.3', '1::2::3', '12345::', 'g::1']) {
      expect(parseIp(bad)).toBeNull()
    }
  })
})

describe('parseCidr', () => {
  test('a bare address is a single host', () => {
    expect(parseCidr('192.0.2.1')?.prefix).toBe(32)
    expect(parseCidr('2001:db8::1')?.prefix).toBe(128)
  })

  test('rejects an out-of-range prefix or malformed text', () => {
    for (const bad of ['10.0.0.0/33', '::/129', '10.0.0.0/8/1', '10.0.0.0/x', 'example.com']) {
      expect(parseCidr(bad)).toBeNull()
    }
  })
})

describe('ipAllowed', () => {
  test('IPv4 ranges', () => {
    const ranges = list('10.0.0.0/8, 192.0.2.5')
    expect(ipAllowed('10.255.0.1', ranges)).toBe(true)
    expect(ipAllowed('192.0.2.5', ranges)).toBe(true)
    expect(ipAllowed('192.0.2.6', ranges)).toBe(false)
    expect(ipAllowed('11.0.0.1', ranges)).toBe(false)
  })

  test('a prefix that does not fall on a byte boundary', () => {
    const ranges = list('172.16.0.0/12')
    expect(ipAllowed('172.31.255.255', ranges)).toBe(true)
    expect(ipAllowed('172.32.0.0', ranges)).toBe(false)
  })

  test('IPv6 ranges', () => {
    const ranges = list('2001:db8::/32,fd7a:115c:a1e0::/48')
    expect(ipAllowed('2001:db8:1234::1', ranges)).toBe(true)
    expect(ipAllowed('fd7a:115c:a1e0:ab12::1', ranges)).toBe(true)
    expect(ipAllowed('2001:db9::1', ranges)).toBe(false)
  })

  test('an IPv4 range matches the mapped form a dual-stack socket reports', () => {
    expect(ipAllowed('::ffff:10.1.1.1', list('10.0.0.0/8'))).toBe(true)
  })

  test('the families never match each other', () => {
    expect(ipAllowed('::1', list('0.0.0.0/0'))).toBe(false)
    expect(ipAllowed('10.0.0.1', list('::/0'))).toBe(false)
  })

  test('an unparseable address is refused', () => {
    expect(ipAllowed('unknown', list('0.0.0.0/0'))).toBe(false)
  })

  test('bad entries are reported, good ones kept', () => {
    const parsed = parseCidrList('10.0.0.0/8, nonsense, 10.0.0.0/40')
    expect(parsed.ranges).toHaveLength(1)
    expect(parsed.invalid).toEqual(['nonsense', '10.0.0.0/40'])
  })
})
