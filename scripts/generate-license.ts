/**
 * Offline license generator — signs a BARRELMAN_LICENSE token with the
 * licensing private key.
 *
 * The seed is read from LICENSE_PRIVATE_KEY and from nowhere else, so this
 * stays provider-agnostic: whatever holds your secrets just has to put it in
 * the environment. We keep ours in Infisical, the same place parchment does:
 *
 *   infisical run --env=prod -- bun scripts/generate-license.ts --exp 2027-01-01
 *
 * A self-hoster with it in .env gets the same result. Neither path is special
 * to this script.
 *
 *   bun scripts/generate-license.ts --keygen        # once, ever
 *
 * --keygen writes the seed to a file rather than printing it, so it can be fed
 * straight to a secrets store without passing through a terminal, a shell
 * history, or argv (which every process on the machine can read):
 *
 *   bun scripts/generate-license.ts --keygen --out license-seed.key
 *   infisical secrets set LICENSE_PRIVATE_KEY=@license-seed.key --env=prod
 *   rm -P license-seed.key
 *
 * The private key is a hex-encoded 32-byte seed, the same format parchment's
 * generator uses. WebCrypto imports Ed25519 private keys as PKCS8 rather than
 * raw seeds, so the seed is wrapped in the fixed DER envelope below — that is
 * the only difference from parchment, which reaches @noble directly.
 *
 * Deliberately has no package.json script and no console SCRIPTS entry: it
 * handles the signing key, and neither a task runner nor a web UI is the right
 * place for that.
 */

/** DER prefix for a PKCS8-wrapped Ed25519 private key; the 32-byte seed follows. */
const PKCS8_PREFIX = Buffer.from('302e020100300506032b657004220420', 'hex')


function hex(bytes: ArrayBuffer | Uint8Array): string {
  return Buffer.from(bytes as ArrayBuffer).toString('hex')
}

/** Default landing spot for a freshly generated seed. Gitignored. */
const DEFAULT_SEED_FILE = 'license-seed.key'

async function keygen() {
  const args = process.argv.slice(2)
  const outIdx = args.indexOf('--out')
  const outPath = outIdx !== -1 ? args[outIdx + 1] : DEFAULT_SEED_FILE

  if (!outPath) {
    console.error('--out needs a path')
    process.exit(1)
  }

  // Never clobber: an existing file here is very likely a seed someone has not
  // moved into their secrets store yet, and overwriting it loses the key.
  if (await Bun.file(outPath).exists()) {
    console.error(
      `${outPath} already exists — refusing to overwrite it.\n` +
        'Move that seed into your secrets store and delete the file first.',
    )
    process.exit(1)
  }

  const kp = (await crypto.subtle.generateKey({ name: 'Ed25519' }, true, [
    'sign',
    'verify',
  ])) as CryptoKeyPair
  const pkcs8 = new Uint8Array(await crypto.subtle.exportKey('pkcs8', kp.privateKey))
  const seed = pkcs8.slice(PKCS8_PREFIX.length)
  const publicKey = await crypto.subtle.exportKey('raw', kp.publicKey)

  // Create the file empty and locked down *before* the seed goes in, so it is
  // never briefly world-readable.
  await Bun.write(outPath, '')
  await Bun.$`chmod 600 ${outPath}`.quiet()
  await Bun.write(outPath, hex(seed))

  // The public key is the only half that is safe to look at, and the only half
  // that needs copying anywhere. The seed is written, never printed.
  console.log('Public key (paste into DEFAULT_LICENSE_PUBLIC_KEY in src/lib/license.ts):')
  console.log(hex(publicKey))
  console.log(`\nPrivate seed written to ${outPath} (mode 600). It was not printed.`)
  console.log('Move it into your secrets store, then delete the file:')
  console.log(`  infisical secrets set LICENSE_PRIVATE_KEY=@${outPath} --env=prod`)
  console.log(`  rm -P ${outPath}`)
}

async function generate() {
  const privateKeyHex = process.env.LICENSE_PRIVATE_KEY
  if (!privateKeyHex) {
    console.error(
      'LICENSE_PRIVATE_KEY is not set.\n' +
        '  From Infisical:  infisical run --env=prod -- bun scripts/generate-license.ts …\n' +
        '  From .env:       set LICENSE_PRIVATE_KEY (hex-encoded 32-byte seed)\n' +
        'Run with --keygen if you have not created a keypair yet.',
    )
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
