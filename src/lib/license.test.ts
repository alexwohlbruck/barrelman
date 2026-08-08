import { describe, test, expect, beforeAll, afterAll } from 'bun:test'
import { verifyLicense, hasFeature, type LicensePayload } from './license'

function hex(b: ArrayBuffer | Uint8Array): string {
  return [...new Uint8Array(b)].map((x) => x.toString(16).padStart(2, '0')).join('')
}

let signingKey: CryptoKey
let publicKeyHex: string
let otherPublicKeyHex: string
const originalKey = process.env.BARRELMAN_LICENSE_PUBLIC_KEY

/** Sign a payload the same way scripts/generate-license.ts does. */
async function issue(payload: Record<string, unknown>, key = signingKey): Promise<string> {
  const bytes = new TextEncoder().encode(JSON.stringify(payload))
  const sig = await crypto.subtle.sign({ name: 'Ed25519' }, key, bytes)
  return `${Buffer.from(bytes).toString('base64')}.${hex(sig)}`
}

beforeAll(async () => {
  const kp = (await crypto.subtle.generateKey({ name: 'Ed25519' }, true, [
    'sign',
    'verify',
  ])) as CryptoKeyPair
  signingKey = kp.privateKey
  publicKeyHex = hex(await crypto.subtle.exportKey('raw', kp.publicKey))

  const other = (await crypto.subtle.generateKey({ name: 'Ed25519' }, true, [
    'sign',
    'verify',
  ])) as CryptoKeyPair
  otherPublicKeyHex = hex(await crypto.subtle.exportKey('raw', other.publicKey))

  process.env.BARRELMAN_LICENSE_PUBLIC_KEY = publicKeyHex
})

afterAll(() => {
  if (originalKey === undefined) delete process.env.BARRELMAN_LICENSE_PUBLIC_KEY
  else process.env.BARRELMAN_LICENSE_PUBLIC_KEY = originalKey
})

describe('verifyLicense', () => {
  test('accepts a correctly signed license', async () => {
    const token = await issue({ org: 'Barrelman Official', features: ['billing'] })
    const payload = await verifyLicense(token)

    expect(payload).not.toBeNull()
    expect(payload?.org).toBe('Barrelman Official')
    expect(payload?.features).toEqual(['billing'])
  })

  test('accepts a license that has not yet expired', async () => {
    const exp = Math.floor(Date.now() / 1000) + 3600
    const payload = await verifyLicense(await issue({ org: 'o', features: ['billing'], exp }))
    expect(payload?.exp).toBe(exp)
  })

  // The signature is genuine here, so this exercises the expiry branch rather
  // than falling out at the signature check.
  test('rejects a correctly signed but expired license', async () => {
    const exp = Math.floor(Date.now() / 1000) - 1
    const token = await issue({ org: 'o', features: ['billing'], exp })
    expect(await verifyLicense(token)).toBeNull()
  })

  test('rejects a valid license signed by a different key', async () => {
    process.env.BARRELMAN_LICENSE_PUBLIC_KEY = otherPublicKeyHex
    try {
      const token = await issue({ org: 'o', features: ['billing'] })
      expect(await verifyLicense(token)).toBeNull()
    } finally {
      process.env.BARRELMAN_LICENSE_PUBLIC_KEY = publicKeyHex
    }
  })

  test('rejects a tampered payload', async () => {
    const token = await issue({ org: 'o', features: ['billing'] })
    const [, sig] = token.split('.')
    const forged = Buffer.from(JSON.stringify({ org: 'me', features: ['billing'] })).toString(
      'base64',
    )
    expect(await verifyLicense(`${forged}.${sig}`)).toBeNull()
  })

  test('rejects a tampered signature', async () => {
    const token = await issue({ org: 'o', features: ['billing'] })
    const flipped = token.slice(0, -1) + (token.endsWith('a') ? 'b' : 'a')
    expect(await verifyLicense(flipped)).toBeNull()
  })

  test.each([
    ['empty', ''],
    ['no separator', 'not-a-token'],
    ['empty payload', '.abcd'],
    ['empty signature', 'eyJ9.'],
    ['non-hex signature', `${Buffer.from('{}').toString('base64')}.zzzz`],
    ['short signature', `${Buffer.from('{}').toString('base64')}.${'a'.repeat(10)}`],
  ])('rejects a malformed token (%s)', async (_label, token) => {
    expect(await verifyLicense(token)).toBeNull()
  })

  test('rejects a signed payload missing required fields', async () => {
    expect(await verifyLicense(await issue({ features: ['billing'] }))).toBeNull()
    expect(await verifyLicense(await issue({ org: 'o' }))).toBeNull()
  })

  // The shipped default is empty, so an unlicensed build cannot be tricked into
  // accepting a token by an attacker who has their own signing key.
  test('rejects every token when no public key is configured', async () => {
    const token = await issue({ org: 'o', features: ['billing'] })
    process.env.BARRELMAN_LICENSE_PUBLIC_KEY = ''
    try {
      expect(await verifyLicense(token)).toBeNull()
    } finally {
      process.env.BARRELMAN_LICENSE_PUBLIC_KEY = publicKeyHex
    }
  })
})

describe('hasFeature', () => {
  const licensed: LicensePayload = { org: 'o', features: ['billing'] }

  test('is true only for a granted feature', () => {
    expect(hasFeature(licensed, 'billing')).toBe(true)
    expect(hasFeature(licensed, 'federation')).toBe(false)
  })

  test('is false without a license', () => {
    expect(hasFeature(null, 'billing')).toBe(false)
  })
})
