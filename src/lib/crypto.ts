/**
 * Small crypto helpers shared by the auth and API-key layers.
 *
 * Everything secret that we store (session tokens, OTP codes, API keys) is kept
 * as a SHA-256 hex digest, never in the clear: a database dump then leaks no
 * usable credential. These are all high-entropy random values, so a plain
 * unsalted digest is the right primitive — password hashing (bcrypt/argon2)
 * buys nothing against 256 bits of randomness and would cost a KDF run on
 * every single API request.
 */
import { createHash, randomBytes, timingSafeEqual } from 'node:crypto'

const BASE62 = 'ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789'

/** SHA-256 of a UTF-8 string, hex-encoded. */
export function sha256Hex(input: string): string {
  return createHash('sha256').update(input, 'utf8').digest('hex')
}

/** URL-safe random token with `bytes` bytes of entropy. */
export function randomToken(bytes = 32): string {
  return randomBytes(bytes).toString('base64url')
}

/**
 * Random base62 string of `length` characters. Used for the human-visible part
 * of an API key, where base64url's `-`/`_` invite copy-paste and shell-quoting
 * mistakes.
 */
export function randomBase62(length: number): string {
  // Rejection sampling keeps the distribution uniform — plain `% 62` over a
  // byte would bias the first 8 characters of the alphabet.
  let out = ''
  while (out.length < length) {
    for (const byte of randomBytes(length)) {
      if (byte < 248) {
        out += BASE62[byte % 62]
        if (out.length === length) break
      }
    }
  }
  return out
}

/** Numeric one-time code, zero-padded to `digits`, drawn without modulo bias. */
export function randomNumericCode(digits = 8): string {
  let out = ''
  while (out.length < digits) {
    for (const byte of randomBytes(digits)) {
      if (byte < 250) {
        out += String(byte % 10)
        if (out.length === digits) break
      }
    }
  }
  return out
}

/** Constant-time string comparison. Returns false on length mismatch. */
export function safeEqual(a: string, b: string): boolean {
  const bufA = Buffer.from(a, 'utf8')
  const bufB = Buffer.from(b, 'utf8')
  if (bufA.length !== bufB.length) return false
  return timingSafeEqual(bufA, bufB)
}

/** Opaque identifier for a row (users, keys, jobs). */
export function generateId(): string {
  return randomBytes(16).toString('hex')
}
