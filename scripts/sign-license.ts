#!/usr/bin/env bun
/**
 * License keypair generation and token signing.
 *
 * For the project owner only. The private key authorises the `billing` feature
 * (see src/lib/license.ts) and must never be committed, pasted into an issue,
 * or stored on a deployed host — only the *public* key ships in the repo, and
 * only the signed *token* goes to a server.
 *
 *   # once, to create the keypair
 *   bun run scripts/sign-license.ts --generate
 *
 *   # then, to issue a license
 *   BARRELMAN_LICENSE_PRIVATE_KEY=<hex> bun run scripts/sign-license.ts \
 *     --org "Barrelman Official" --features billing --expires 2027-01-01
 */

function hex(bytes: ArrayBuffer | Uint8Array): string {
  return [...new Uint8Array(bytes)].map((b) => b.toString(16).padStart(2, '0')).join('')
}

function unhex(s: string): Uint8Array {
  const out = new Uint8Array(s.length / 2)
  for (let i = 0; i < out.length; i++) out[i] = parseInt(s.slice(i * 2, i * 2 + 2), 16)
  return out
}

function flag(name: string): string | undefined {
  const i = process.argv.indexOf(`--${name}`)
  return i >= 0 ? process.argv[i + 1] : undefined
}

async function generate() {
  const kp = (await crypto.subtle.generateKey({ name: 'Ed25519' }, true, [
    'sign',
    'verify',
  ])) as CryptoKeyPair
  const pub = hex(await crypto.subtle.exportKey('raw', kp.publicKey))
  // PKCS8 rather than the raw 32-byte seed: it is self-describing, so signing
  // needs nothing but this string.
  const priv = hex(await crypto.subtle.exportKey('pkcs8', kp.privateKey))

  console.log('\nKeypair generated.\n')
  console.log('PUBLIC KEY (paste into DEFAULT_LICENSE_PUBLIC_KEY in src/lib/license.ts):')
  console.log(`  ${pub}\n`)
  console.log('PRIVATE KEY (store in a password manager — never commit it):')
  console.log(`  ${priv}\n`)
}

async function sign() {
  const privHex = process.env.BARRELMAN_LICENSE_PRIVATE_KEY
  if (!privHex) {
    console.error('BARRELMAN_LICENSE_PRIVATE_KEY is required. Run with --generate to create one.')
    process.exit(1)
  }

  const org = flag('org')
  if (!org) {
    console.error('--org is required, e.g. --org "Barrelman Official"')
    process.exit(1)
  }

  const features = (flag('features') ?? 'billing').split(',').map((f) => f.trim()).filter(Boolean)
  const expires = flag('expires')
  const payload: Record<string, unknown> = { org, features }

  if (expires) {
    const ms = Date.parse(expires)
    if (Number.isNaN(ms)) {
      console.error(`--expires "${expires}" is not a date bun can parse (try 2027-01-01)`)
      process.exit(1)
    }
    payload.exp = Math.floor(ms / 1000)
  }

  const key = await crypto.subtle.importKey('pkcs8', unhex(privHex), { name: 'Ed25519' }, false, [
    'sign',
  ])

  const payloadBytes = new TextEncoder().encode(JSON.stringify(payload))
  const signature = await crypto.subtle.sign({ name: 'Ed25519' }, key, payloadBytes)
  const token = `${Buffer.from(payloadBytes).toString('base64')}.${hex(signature)}`

  console.log(`\nPayload:  ${JSON.stringify(payload)}`)
  console.log(`\nBARRELMAN_LICENSE=${token}\n`)
}

if (process.argv.includes('--generate')) await generate()
else await sign()
