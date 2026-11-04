/**
 * The pin-finding and pin-editing half of scripts/deploy-remote.sh, run
 * against throwaway compose files. Those are the parts that edit a production
 * host's config with sed, and a regex that matched `barrelman` inside
 * `barrelman-ops` would deploy the API image as the worker.
 */
import { afterEach, beforeEach, describe, expect, test } from 'bun:test'
import { mkdtempSync, readFileSync, rmSync, writeFileSync } from 'node:fs'
import { tmpdir } from 'node:os'
import { join, resolve } from 'node:path'

const SCRIPT = resolve(import.meta.dir, 'deploy-remote.sh')
let dir: string

beforeEach(() => {
  dir = mkdtempSync(join(tmpdir(), 'deploy-remote-'))
})
afterEach(() => {
  rmSync(dir, { recursive: true, force: true })
})

/** Run a snippet with the script's functions loaded, in the temp dir. */
function sh(snippet: string, env: Record<string, string> = {}): string {
  const res = Bun.spawnSync(['bash', '-c', `source "${SCRIPT}"; cd "${dir}"; ${snippet}`], {
    env: { PATH: process.env.PATH ?? '', HOME: dir, ...env },
  })
  if (res.exitCode !== 0) throw new Error(`exit ${res.exitCode}: ${res.stderr.toString()}`)
  return res.stdout.toString().trim()
}

const BASE = `services:
  barrelman:
    image: alexwohlbruck/barrelman:latest
  barrelman-ops:
    image: alexwohlbruck/barrelman-ops:latest
  barrelman-db:
    image: "alexwohlbruck/barrelman-db:latest"
`

describe('the deploy lock', () => {
  const lock = () => join(dir, '.deploy.lock')

  test('records the holder and time, and releases on exit', () => {
    const inside = sh(`take_lock "deploy 1.0.0"; cat "$LOCK_FILE"`, { DEPLOY_PATH: dir, DEPLOY_HOLDER: 'ops@laptop' })
    expect(inside).toMatch(/^holder=ops@laptop time=\d{4}-\d\d-\d\dT[\d:]+Z pid=\d+ action=deploy 1\.0\.0$/)
    expect(() => readFileSync(lock())).toThrow()
  })

  test('refuses while someone else holds it', () => {
    writeFileSync(lock(), 'holder=someone-else\n')
    expect(() => sh('take_lock deploy', { DEPLOY_PATH: dir })).toThrow(/someone-else/)
    expect(readFileSync(lock(), 'utf8')).toBe('holder=someone-else\n')
  })

  test('--force takes it over', () => {
    writeFileSync(lock(), 'holder=someone-else\n')
    expect(sh('take_lock deploy; cat "$LOCK_FILE"', { DEPLOY_PATH: dir, DEPLOY_FORCE: '1', DEPLOY_HOLDER: 'me' })).toContain(
      'holder=me',
    )
  })
})

describe('compose_files', () => {
  test('reads COMPOSE_FILE from .env, in order', () => {
    writeFileSync(join(dir, '.env'), 'FOO=1\nCOMPOSE_FILE="docker-compose.yml:docker-compose.prod.yml"\n')
    expect(sh('compose_files')).toBe('docker-compose.yml\ndocker-compose.prod.yml')
  })

  test('falls back to the default file and its override', () => {
    writeFileSync(join(dir, 'docker-compose.yml'), BASE)
    writeFileSync(join(dir, 'docker-compose.override.yml'), 'services: {}\n')
    expect(sh('compose_files')).toBe('docker-compose.yml\ndocker-compose.override.yml')
  })
})

describe('pins', () => {
  beforeEach(() => {
    writeFileSync(join(dir, 'docker-compose.yml'), BASE)
    writeFileSync(
      join(dir, 'docker-compose.prod.yml'),
      `services:
  barrelman:
    image: alexwohlbruck/barrelman:0.10.38  # pinned for prod
  barrelman-ops:
    image: 'alexwohlbruck/barrelman-ops:0.10.38'
`,
    )
    writeFileSync(join(dir, '.env'), 'COMPOSE_FILE=docker-compose.yml:docker-compose.prod.yml\n')
  })

  test('the last file naming an image is the one that pins it', () => {
    expect(sh('pin_file_for alexwohlbruck/barrelman')).toBe('docker-compose.prod.yml')
    expect(sh('pin_file_for alexwohlbruck/barrelman-db')).toBe('docker-compose.yml')
  })

  test('reads the pinned ref without confusing the API with the worker', () => {
    expect(sh('pinned_ref docker-compose.prod.yml alexwohlbruck/barrelman')).toBe('alexwohlbruck/barrelman:0.10.38')
    expect(sh('pinned_ref docker-compose.prod.yml alexwohlbruck/barrelman-ops')).toBe(
      'alexwohlbruck/barrelman-ops:0.10.38',
    )
  })

  test('set_pin edits only its own image, keeping quotes and comments', () => {
    sh('set_pin docker-compose.prod.yml alexwohlbruck/barrelman alexwohlbruck/barrelman:0.10.39')
    const text = readFileSync(join(dir, 'docker-compose.prod.yml'), 'utf8')
    expect(text).toContain('image: alexwohlbruck/barrelman:0.10.39  # pinned for prod')
    expect(text).toContain("image: 'alexwohlbruck/barrelman-ops:0.10.38'")

    sh('set_pin docker-compose.yml alexwohlbruck/barrelman-db alexwohlbruck/barrelman-db@sha256:abc')
    expect(readFileSync(join(dir, 'docker-compose.yml'), 'utf8')).toContain(
      'image: "alexwohlbruck/barrelman-db@sha256:abc"',
    )
  })

  test('a fixed version tag is restored as written', () => {
    expect(sh('restorable_ref barrelman alexwohlbruck/barrelman alexwohlbruck/barrelman:0.10.38')).toBe(
      'alexwohlbruck/barrelman:0.10.38',
    )
  })
})
