/**
 * License verification.
 *
 * Barrelman is source-available under the Apache 2.0 license with the Commons
 * Clause: free to self-host and run, including inside a business, but not to
 * sell. Charging third parties for access is the one right the Commons Clause
 * removes, so the billing surface — Polar subscriptions, checkout, metered
 * overage — is gated on a signed license naming the `billing` feature. Only the
 * official deployment holds one. See LICENSING.md.
 *
 * This is a compliance boundary, not a security one. The source is public and
 * anyone can delete the check; the point is that doing so is a deliberate act
 * against the license rather than an accident, and that a self-hoster who never
 * intended to resell cannot switch billing on by fumbling an env var.
 *
 * Token format: `base64(JSON payload) + "." + hex(Ed25519 signature)`.
 * Sign one with `bun scripts/generate-license.ts`.
 */

/**
 * Ed25519 public key (hex, 32 bytes) that licenses are verified against. The
 * matching private key is held by the project and never lives in this repo.
 *
 * Empty until a keypair is generated, which means no license can verify and
 * billing stays off — the correct default for every deployment but the
 * official one. `BARRELMAN_LICENSE_PUBLIC_KEY` overrides it, for tests and for
 * a commercial licensee running their own signing key.
 */
/**
 * Verification key for `BARRELMAN_LICENSE`. Public by definition — it can only
 * check a signature, never make one — so it is committed rather than configured.
 * Its private half lives in the OS keychain (`dev.barrelman` /
 * `license-signing-key`); see `scripts/generate-license.ts`.
 *
 * Empty means no token can ever verify, which is why billing was off everywhere
 * including production until this was filled in.
 */
const DEFAULT_LICENSE_PUBLIC_KEY = 'adb594bb8940a8c277316a9efd3a3701b97fd00a198eb1e8829880c0adcb1ea8'

export interface LicensePayload {
  /** Who the license was issued to. */
  org: string
  /** Feature flags this license unlocks. `billing` is the only one today. */
  features: string[]
  /** Unix seconds. Omit for perpetual. */
  exp?: number
}

function publicKeyHex(): string {
  return process.env.BARRELMAN_LICENSE_PUBLIC_KEY || DEFAULT_LICENSE_PUBLIC_KEY
}

function hexToBytes(hex: string): Uint8Array | null {
  if (hex.length % 2 !== 0 || !/^[0-9a-f]*$/i.test(hex)) return null
  const out = new Uint8Array(hex.length / 2)
  for (let i = 0; i < out.length; i++) out[i] = parseInt(hex.slice(i * 2, i * 2 + 2), 16)
  return out
}

/**
 * Verify and decode a license token.
 *
 * Returns the payload when the signature checks out against the configured
 * public key and the license has not expired, otherwise `null`. Never throws —
 * a malformed token is just an absent license.
 */
export async function verifyLicense(token: string): Promise<LicensePayload | null> {
  try {
    const keyHex = publicKeyHex()
    if (!keyHex) return null

    const [payloadB64, signatureHex] = token.split('.')
    if (!payloadB64 || !signatureHex) return null

    const keyBytes = hexToBytes(keyHex)
    const signature = hexToBytes(signatureHex)
    if (!keyBytes || keyBytes.length !== 32) return null
    if (!signature || signature.length !== 64) return null

    const payloadBytes = new Uint8Array(Buffer.from(payloadB64, 'base64'))
    if (payloadBytes.length === 0) return null

    const key = await crypto.subtle.importKey('raw', keyBytes, { name: 'Ed25519' }, false, [
      'verify',
    ])
    if (!(await crypto.subtle.verify({ name: 'Ed25519' }, key, signature, payloadBytes))) {
      return null
    }

    const payload = JSON.parse(Buffer.from(payloadBytes).toString('utf-8')) as LicensePayload
    if (typeof payload?.org !== 'string' || !Array.isArray(payload?.features)) return null
    if (payload.exp && Date.now() > payload.exp * 1000) return null

    return payload
  } catch {
    return null
  }
}

/** Whether a verified license grants a named feature. */
export function hasFeature(license: LicensePayload | null, feature: string): boolean {
  return Boolean(license?.features?.includes(feature))
}
