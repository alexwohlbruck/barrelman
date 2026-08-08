/**
 * Offline license generator — run with your private key to produce a
 * signed BARRELMAN_LICENSE token.
 *
 * Usage:
 *   LICENSE_PRIVATE_KEY=<hex-seed> bun scripts/generate-license.ts [--exp 2027-01-01]
 *
 * To generate a keypair for the first time:
 *   bun scripts/generate-license.ts --keygen
 *
 * The private key is a hex-encoded 32-byte seed, the same format parchment's
 * generator uses. WebCrypto imports Ed25519 private keys as PKCS8 rather than
 * raw seeds, so the seed is wrapped in the fixed DER envelope below — that is
 * the only difference from parchment, which reaches @noble directly.
 */

/** DER prefix for a PKCS8-wrapped Ed25519 private key; the 32-byte seed follows. */
const PKCS8_PREFIX = Buffer.from('302e020100300506032b657004220420', 'hex')

function hex(bytes: ArrayBuffer | Uint8Array): string {
  return Buffer.from(bytes as ArrayBuffer).toString('hex')
}

async function keygen() {
  const kp = (await crypto.subtle.generateKey({ name: 'Ed25519' }, true, [
    'sign',
    'verify',
  ])) as CryptoKeyPair
  const pkcs8 = new Uint8Array(await crypto.subtle.exportKey('pkcs8', kp.privateKey))
  const seed = pkcs8.slice(PKCS8_PREFIX.length)
  const publicKey = await crypto.subtle.exportKey('raw', kp.publicKey)

  console.log('Private key (keep secret):')
  console.log(hex(seed))
  console.log('\nPublic key (embed in license.ts):')
  console.log(hex(publicKey))
}

async function generate() {
  const privateKeyHex = process.env.LICENSE_PRIVATE_KEY
  if (!privateKeyHex) {
    console.error('Set LICENSE_PRIVATE_KEY env var (hex-encoded 32-byte seed)')
    process.exit(1)
  }

  const args = process.argv.slice(2)
  const expIdx = args.indexOf('--exp')
  const expDate = expIdx !== -1 ? args[expIdx + 1] : undefined
  const orgIdx = args.indexOf('--org')

  const payload = {
    org: orgIdx !== -1 ? args[orgIdx + 1] : 'barrelman',
    features: ['billing'],
    ...(expDate ? { exp: Math.floor(new Date(expDate).getTime() / 1000) } : {}),
  }

  const payloadBytes = Buffer.from(JSON.stringify(payload), 'utf-8')
  const payloadB64 = payloadBytes.toString('base64')

  const seed = Buffer.from(privateKeyHex, 'hex')
  if (seed.length !== 32) {
    console.error(`LICENSE_PRIVATE_KEY must be a 32-byte hex seed (got ${seed.length} bytes)`)
    process.exit(1)
  }

  const key = await crypto.subtle.importKey(
    'pkcs8',
    Buffer.concat([PKCS8_PREFIX, seed]),
    { name: 'Ed25519' },
    false,
    ['sign'],
  )
  const signature = await crypto.subtle.sign({ name: 'Ed25519' }, key, payloadBytes)

  const token = `${payloadB64}.${hex(signature)}`

  console.log('License payload:', JSON.stringify(payload, null, 2))
  console.log('\nBARRELMAN_LICENSE token:')
  console.log(token)
}

if (process.argv.includes('--keygen')) {
  await keygen()
} else {
  await generate()
}
