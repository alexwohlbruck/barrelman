/**
 * Tests for automatic passkey naming.
 */
import { describe, test, expect } from 'bun:test'
import { passkeyNameFor } from './passkey-names'

const CHROME_MAC =
  'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36'
const SAFARI_IOS =
  'Mozilla/5.0 (iPhone; CPU iPhone OS 17_0 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.0 Mobile/15E148 Safari/604.1'
const EDGE_WINDOWS =
  'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36 Edg/131.0.0.0'
const FIREFOX_LINUX = 'Mozilla/5.0 (X11; Linux x86_64; rv:133.0) Gecko/20100101 Firefox/133.0'

describe('passkeyNameFor', () => {
  test('prefers the authenticator make when the AAGUID is known', () => {
    expect(passkeyNameFor('fbfc3007-154e-4ecc-8c0b-6e020557d7bd', CHROME_MAC)).toBe('iCloud Keychain')
    expect(passkeyNameFor('bada5566-a7aa-401f-bd96-45619a55120d', FIREFOX_LINUX)).toBe('1Password')
  })

  test('matches AAGUIDs case-insensitively', () => {
    expect(passkeyNameFor('BADA5566-A7AA-401F-BD96-45619A55120D', null)).toBe('1Password')
  })

  test('treats an all-zero AAGUID as no information', () => {
    // Privacy-preserving platform authenticators report zeros rather than
    // identifying themselves, so fall through to the user agent.
    expect(passkeyNameFor('00000000-0000-0000-0000-000000000000', SAFARI_IOS)).toBe('Safari on iOS')
  })

  test('falls back to browser and platform for an unknown AAGUID', () => {
    expect(passkeyNameFor('11111111-2222-3333-4444-555555555555', CHROME_MAC)).toBe('Chrome on macOS')
  })

  test('picks the right browser where user agents overlap', () => {
    // Edge claims Chrome, and Chrome claims Safari, so ordering matters.
    expect(passkeyNameFor(undefined, EDGE_WINDOWS)).toBe('Edge on Windows')
    expect(passkeyNameFor(undefined, CHROME_MAC)).toBe('Chrome on macOS')
    expect(passkeyNameFor(undefined, SAFARI_IOS)).toBe('Safari on iOS')
    expect(passkeyNameFor(undefined, FIREFOX_LINUX)).toBe('Firefox on Linux')
  })

  test('degrades to a generic label with nothing to go on', () => {
    expect(passkeyNameFor(undefined, null)).toBe('Passkey')
    expect(passkeyNameFor(undefined, 'some-unrecognised-agent')).toBe('Passkey')
  })

  test('never returns an empty name', () => {
    for (const ua of [CHROME_MAC, SAFARI_IOS, EDGE_WINDOWS, FIREFOX_LINUX, '', 'garbage']) {
      expect(passkeyNameFor(undefined, ua).length).toBeGreaterThan(0)
    }
  })
})
