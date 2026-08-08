/**
 * Offline license generator — signs a BARRELMAN_LICENSE token with the
 * licensing private key.
 *
 * The private key lives in the OS keychain, the same place parchment keeps its
 * master seed (`web/src-tauri/src/keychain.rs`, service `app.parchment`). Here
 * the service is `dev.barrelman`, so the entry shows up under "Barrelman" in
 * Keychain Access. Two reasons over an env var: it never appears in shell
 * history or in `ps` output for the seconds the command runs, and there is no
 * plaintext file anyone has to remember to shred afterwards.
 *
 *   bun scripts/generate-license.ts --keygen              # once, ever
 *   bun scripts/generate-license.ts --exp 2027-01-01      # per license
 *
 * `LICENSE_PRIVATE_KEY` still works and takes precedence, for CI, for Linux
 * hosts without a Secret Service, and for anyone holding the seed elsewhere.
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

/** Matches parchment's convention of naming the service after the app. */
const KEYCHAIN_SERVICE = 'dev.barrelman'
const KEYCHAIN_ACCOUNT = 'license-signing-key'

function hex(bytes: ArrayBuffer | Uint8Array): string {
  return Buffer.from(bytes as ArrayBuffer).toString('hex')
}

/**
 * Read the seed from the OS keychain. Returns null when there is no entry, or
 * when the platform has no keychain we know how to reach — both are ordinary,
 * and the caller falls back to LICENSE_PRIVATE_KEY.
 *
 * The value is passed to and from `security` on stdout only; it is never an
 * argument, since arguments are visible to every process on the machine.
 */
async function keychainGet(): Promise<string | null> {
  if (process.platform !== 'darwin') return null
  const proc = Bun.spawn(
    ['security', 'find-generic-password', '-s', KEYCHAIN_SERVICE, '-a', KEYCHAIN_ACCOUNT, '-w'],
    { stdout: 'pipe', stderr: 'ignore' },
  )
  const out = (await new Response(proc.stdout).text()).trim()
  return (await proc.exited) === 0 && out ? out : null
}

/** Store the seed, replacing any existing entry (`-U`). */
async function keychainSet(seedHex: string): Promise<boolean> {
  if (process.platform !== 'darwin') return false
  const proc = Bun.spawn(
    [
      'security', 'add-generic-password',
      '-s', KEYCHAIN_SERVICE,
      '-a', KEYCHAIN_ACCOUNT,
      '-l', 'Barrelman license signing key',
      '-D', 'Ed25519 private seed',
      '-U',
      // `-w` with no value makes `security` read the secret from stdin instead
      // of argv, which every process on the machine can see. It prompts twice —
      // once for the value and once to confirm — so the seed is written twice;
      // sending it once leaves an empty entry behind and reports success.
      '-w',
    ],
    { stdin: 'pipe', stdout: 'ignore', stderr: 'pipe' },
  )
  proc.stdin.write(`${seedHex}\n${seedHex}\n`)
  await proc.stdin.end()
  if ((await proc.exited) !== 0) return false

  // Read it back: an empty or truncated entry is worse than a failed write,
  // because the seed is gone by the time anyone notices.
  return (await keychainGet()) === seedHex
}

async function keygen() {
  if (await keychainGet()) {
    console.error(
      `A signing key already exists in the keychain (${KEYCHAIN_SERVICE} / ${KEYCHAIN_ACCOUNT}).\n` +
        'Generating a new one invalidates every license already issued under the old key.\n' +
        'Delete the entry in Keychain Access first if that is really what you want.',
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

  // The public key is the only half that is safe to look at, and the only half
  // anyone needs to copy anywhere. The private seed goes straight to the
  // keychain without ever being printed.
  console.log('Public key (paste into DEFAULT_LICENSE_PUBLIC_KEY in src/lib/license.ts):')
  console.log(hex(publicKey))

  if (await keychainSet(hex(seed))) {
    console.log(`\nPrivate key stored in the OS keychain: ${KEYCHAIN_SERVICE} / ${KEYCHAIN_ACCOUNT}`)
    console.log('It was not printed. Back the keychain up — losing it means reissuing every license.')
  } else {
    console.error(
      '\nCould not write to the OS keychain, and the private key has NOT been printed —' +
        ' it is gone. Re-run on macOS, or set LICENSE_PRIVATE_KEY from a seed you generate' +
        ' and store yourself.',
    )
    process.exit(1)
  }
}

async function generate() {
  const privateKeyHex = process.env.LICENSE_PRIVATE_KEY || (await keychainGet())
  if (!privateKeyHex) {
    console.error(
      `No signing key. Expected it in the OS keychain (${KEYCHAIN_SERVICE} / ${KEYCHAIN_ACCOUNT})` +
        ' or in LICENSE_PRIVATE_KEY.\nRun with --keygen if you have not created one yet.',
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
