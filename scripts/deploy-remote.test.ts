/**
 * The pin-finding and pin-editing half of scripts/deploy-remote.sh, run
 * against throwaway compose files. Those are the parts that edit a production
 * host's config with sed, and a regex that matched `barrelman` inside
 * `barrelman-ops` would deploy the API image as the worker.
 */
import { afterEach, beforeEach, describe, expect, setDefaultTimeout, test } from 'bun:test'

// The deploy tests shell out to the real `docker compose config`, which can take
// several seconds on a cold CI runner.
setDefaultTimeout(30_000)
import { mkdirSync, mkdtempSync, readFileSync, rmSync, writeFileSync } from 'node:fs'
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
volumes:
  barrelman:
`,
    )
    writeFileSync(join(dir, '.env'), 'COMPOSE_FILE=docker-compose.yml:docker-compose.prod.yml\n')
  })

  test('the last file setting a service image is the one that pins it', () => {
    expect(sh('pin_file_for barrelman')).toBe('docker-compose.prod.yml')
    expect(sh('pin_file_for barrelman-db')).toBe('docker-compose.yml')
  })

  test('reads the pinned ref of the right service', () => {
    expect(sh('pinned_ref docker-compose.prod.yml barrelman')).toBe('alexwohlbruck/barrelman:0.10.38')
    expect(sh('pinned_ref docker-compose.prod.yml barrelman-ops')).toBe('alexwohlbruck/barrelman-ops:0.10.38')
  })

  test('set_pin edits only that service, keeping quotes and comments', () => {
    sh('set_pin docker-compose.prod.yml barrelman alexwohlbruck/barrelman:0.10.39')
    const text = readFileSync(join(dir, 'docker-compose.prod.yml'), 'utf8')
    expect(text).toContain('image: alexwohlbruck/barrelman:0.10.39  # pinned for prod')
    expect(text).toContain("image: 'alexwohlbruck/barrelman-ops:0.10.38'")

    sh('set_pin docker-compose.yml barrelman-db alexwohlbruck/barrelman-db@sha256:abc')
    expect(readFileSync(join(dir, 'docker-compose.yml'), 'utf8')).toContain(
      'image: "alexwohlbruck/barrelman-db@sha256:abc"',
    )
  })

  test('ref_in_repo does not mistake the worker for the API', () => {
    expect(() => sh('ref_in_repo alexwohlbruck/barrelman-ops:1 alexwohlbruck/barrelman')).toThrow()
    expect(() => sh('ref_in_repo alexwohlbruck/barrelman@sha256:a alexwohlbruck/barrelman')).not.toThrow()
  })

  test('a fixed version tag is restored as written', () => {
    expect(sh('restorable_ref barrelman alexwohlbruck/barrelman alexwohlbruck/barrelman:0.10.38')).toBe(
      'alexwohlbruck/barrelman:0.10.38',
    )
  })
})

// ── The whole deploy, against a fake docker ─────────────────────────────────
//
// `docker compose config` is the real one (it needs no daemon), so overrides
// resolve the way they would on a host. Everything that touches containers is
// faked from files in $FAKE: stuck-<svc> (keeps running that image after
// `up`), crashloop-<svc> (restart count climbs), down-<svc> (not running).

const REAL_DOCKER = Bun.which('docker')
// `$__{` stands for `${`, which a template literal would interpolate.
const FAKE_DOCKER = String.raw`#!/bin/bash
echo "docker $*" >> "$FAKE/calls"
args="$*"
svc_of() { echo "$__{1#cid-}"; }
case "$args" in
  "compose config"*) exec "$REAL_DOCKER" "$@" ;;
  *"deploy-gate.ts"*) exit 0 ;;
  *"bun -e"*) exit 0 ;;
  "compose pull"* | "compose logs"* | "compose up"*) exit 0 ;;
  "compose ps --status running -q "*) s="$__{args##* }"; [ -e "$FAKE/down-$s" ] || echo "cid-$s" ;;
  "compose ps -q "*) echo "cid-$__{args##* }" ;;
  "inspect -f {{.Config.Image}} "*)
    s="$(svc_of "$__{args##* }")"
    if [ -e "$FAKE/stuck-$s" ]; then cat "$FAKE/stuck-$s"; exit 0; fi
    "$REAL_DOCKER" compose config | awk -v s="$s" '/^[^ ]/{i=($0~/^services:/);b=0;next} i&&$0=="  " s ":"{b=1;next} i&&/^  [^ ]/{b=0} b&&/^    image: /{sub(/^    image: /,"");print;exit}' ;;
  "inspect -f {{.RestartCount}} {{.State.StartedAt}} "*)
    s="$(svc_of "$__{args##* }")"
    if [ -e "$FAKE/crashloop-$s" ]; then
      n=$(( $(cat "$FAKE/crashloop-$s") + 1 )); echo "$n" > "$FAKE/crashloop-$s"; echo "$n t$n"
    else echo "0 t0"; fi ;;
  "inspect -f {{.Image}} "*) echo sha256:img ;;
  "image inspect"*) echo "alexwohlbruck/barrelman@sha256:old"; echo "alexwohlbruck/barrelman-ops@sha256:oldops" ;;
esac
exit 0
`.replaceAll("$__{", "${")

describe.skipIf(!REAL_DOCKER)('deploy against a fake docker', () => {
  let fake: string

  beforeEach(() => {
    fake = join(dir, 'fake')
    mkdirSync(join(fake, 'bin'), { recursive: true })
    writeFileSync(join(fake, 'bin', 'docker'), FAKE_DOCKER, { mode: 0o755 })
    writeFileSync(join(dir, 'docker-compose.yml'), BASE)
    writeFileSync(join(dir, 'docker-compose.prod.yml'), 'services:\n  barrelman:\n    image: alexwohlbruck/barrelman:0.10.38\n')
    writeFileSync(join(dir, '.env'), 'COMPOSE_FILE=docker-compose.yml:docker-compose.prod.yml\n')
  })

  const read = (f: string) => {
    try {
      return readFileSync(join(dir, f), 'utf8')
    } catch {
      return ''
    }
  }

  function deploy(services = 'api,ops') {
    const res = Bun.spawnSync(['bash', SCRIPT, 'deploy', '0.10.39', services], {
      cwd: dir,
      env: {
        PATH: `${join(fake, 'bin')}:${process.env.PATH}`,
        HOME: dir,
        FAKE: fake,
        REAL_DOCKER: REAL_DOCKER!,
        DEPLOY_PATH: dir,
        DEPLOY_HOLDER: 'me@laptop',
        DEPLOY_TIMEOUT: '1',
        DEPLOY_STABLE_SECONDS: '0',
      },
    })
    return {
      code: res.exitCode,
      stderr: res.stderr.toString(),
      log: read('deploy.log'),
      calls: read('fake/calls'),
      base: read('docker-compose.yml'),
      prod: read('docker-compose.prod.yml'),
    }
  }

  test('moves the pins, recreates the services and records the old refs', () => {
    const r = deploy()
    expect(r.code).toBe(0)
    expect(r.prod).toContain('alexwohlbruck/barrelman:0.10.39')
    expect(r.base).toContain('alexwohlbruck/barrelman-ops:0.10.39')
    expect(r.calls).toContain('compose up -d --no-deps barrelman barrelman-ops')
    expect(r.log).toMatch(/me@laptop\tdeploy api,ops from api=barrelman:0\.10\.38 .* to 0\.10\.39 ok/)
    expect(read('.deploy-previous')).toContain('api|barrelman|docker-compose.prod.yml|alexwohlbruck/barrelman:0.10.38')
  })

  test('refuses to override a hotfix pinned to another image, touching nothing', () => {
    writeFileSync(join(dir, 'docker-compose.hotfix.yml'), 'services:\n  barrelman:\n    image: local/barrelman:hotfix-1\n')
    writeFileSync(join(dir, '.env'), 'COMPOSE_FILE=docker-compose.yml:docker-compose.prod.yml:docker-compose.hotfix.yml\n')
    const r = deploy()
    expect(r.code).not.toBe(0)
    expect(r.stderr).toContain('local/barrelman:hotfix-1')
    expect(r.stderr).toContain('hotfix')
    expect(r.calls).not.toContain('compose up')
    expect(r.prod).toContain('alexwohlbruck/barrelman:0.10.38')
    expect(r.log).toBe('')
  })

  test('fails and rolls back when the container comes up on another image', () => {
    writeFileSync(join(fake, 'stuck-barrelman'), 'alexwohlbruck/barrelman:0.10.38\n')
    const r = deploy('api')
    expect(r.code).toBe(1)
    expect(r.stderr).toContain('barrelman is running alexwohlbruck/barrelman:0.10.38, not alexwohlbruck/barrelman:0.10.39')
    expect(r.log).toContain('to 0.10.39 FAILED, rolled back')
    expect(r.prod).toContain('alexwohlbruck/barrelman:0.10.38')
  })

  test('a worker that keeps restarting fails the deploy even though /health passes', () => {
    writeFileSync(join(fake, 'crashloop-barrelman-ops'), '0')
    const r = deploy('ops')
    expect(r.code).toBe(1)
    expect(r.stderr).toContain('barrelman-ops restarted')
    expect(r.log).toContain('FAILED')
    // Put back as the digest of what was running, since the pin was :latest.
    expect(r.base).toContain('alexwohlbruck/barrelman-ops@sha256:oldops')
  })

  test('a service that is not running fails the deploy', () => {
    writeFileSync(join(fake, 'down-barrelman-ops'), '')
    const r = deploy('ops')
    expect(r.code).toBe(1)
    expect(r.stderr).toContain('barrelman-ops is not running')
  })
})

