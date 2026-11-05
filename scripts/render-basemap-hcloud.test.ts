import { describe, expect, it } from 'bun:test'
import { join } from 'node:path'

/**
 * The parts of render-basemap-hcloud.sh that decide what gets created and what
 * gets deleted on a billed account. Each test sources the script, replaces
 * hc_api with a fake that answers from a table and records every call, and
 * runs one function the way the script runs it.
 */
const SCRIPT = join(import.meta.dir, 'render-basemap-hcloud.sh')

interface Run {
  stdout: string
  calls: string[]
  exitCode: number
}

/**
 * `responses` maps "METHOD path" to [ok, body]. An unlisted call succeeds with
 * an empty object.
 */
function run(body: string, responses: Record<string, [boolean, unknown]> = {}, env: Record<string, string> = {}): Run {
  const table = Object.entries(responses)
    .map(([k, [ok, b]]) => `  '${k}') printf '%s' '${JSON.stringify(b)}'; return ${ok ? 0 : 1} ;;`)
    .join('\n')
  const script = `
set -uo pipefail
. '${SCRIPT}'
hc_api() {
  echo "CALL $1 $2" >&2
  case "$1 $2" in
${table}
  *) printf '{}'; return 0 ;;
  esac
}
hlog() { :; }
HCLOUD_LABEL_VALUE=prod
${body}
`
  const proc = Bun.spawnSync(['bash', '-c', script], { env: { ...process.env, ...env } })
  const calls = proc.stderr
    .toString()
    .split('\n')
    .filter((l) => l.startsWith('CALL '))
    .map((l) => l.slice(5))
  return { stdout: proc.stdout.toString().trim(), calls, exitCode: proc.exitCode ?? -1 }
}

const unavailable: [boolean, unknown] = [false, { error: { code: 'resource_unavailable' } }]
const created = (id: number): [boolean, unknown] => [true, { server: { id } }]

describe('hcloud_label_value', () => {
  it('keeps a host name that is already a valid label', () => {
    expect(run(`hcloud_label_value barrelman-prod`).stdout).toBe('barrelman-prod')
  })

  it('folds characters a label cannot hold and trims the ends', () => {
    expect(run(`hcloud_label_value '/my host!'`).stdout).toBe('my-host')
  })

  it('falls back to a fixed value when nothing usable is left', () => {
    expect(run(`hcloud_label_value '!!!'`).stdout).toBe('barrelman')
  })
})

describe('hcloud_create_server', () => {
  it('moves on through locations, then types, while each is out of stock', () => {
    // Every POST hits the same path, so the fake answers in sequence instead.
    // The count lives in a file: hc_api runs in a $(…) subshell.
    const r = run(
      `
count="$(mktemp)"; echo 0 > "$count"
hc_api() {
  echo "CALL $1 $2" >&2
  n=$(( $(cat "$count") + 1 )); echo "$n" > "$count"
  if [ "$n" -lt 4 ]; then printf '{"error":{"code":"resource_unavailable"}}'; return 1; fi
  printf '{"server":{"id":77}}'
}
hcloud_create_server test-render 5 && echo "$HCLOUD_SERVER_ID|$HCLOUD_PLACED"`,
      {},
      { BASEMAP_HCLOUD_SERVER_TYPES: 'cx53,cax41', BASEMAP_HCLOUD_LOCATIONS: 'nbg1,fsn1,hel1' },
    )
    expect(r.exitCode).toBe(0)
    // nbg1, fsn1, hel1 for cx53 all refused; cax41 in nbg1 accepted.
    expect(r.stdout).toBe('77|cax41 nbg1')
    expect(r.calls).toHaveLength(4)
  })

  it('stops at an error that is not about capacity, rather than trying every combination', () => {
    const r = run(`hcloud_create_server test-render 5`, { 'POST /servers': [false, { error: { code: 'unauthorized' } }] })
    expect(r.exitCode).not.toBe(0)
    expect(r.calls).toEqual(['POST /servers'])
  })

  it('fails once every type is out of stock everywhere, having created nothing', () => {
    const r = run(`hcloud_create_server test-render 5; echo "id=[$HCLOUD_SERVER_ID]"`, { 'POST /servers': unavailable }, {
      BASEMAP_HCLOUD_SERVER_TYPES: 'cx53',
      BASEMAP_HCLOUD_LOCATIONS: 'nbg1,fsn1',
    })
    expect(r.stdout).toBe('id=[]')
    expect(r.calls).toEqual(['POST /servers', 'POST /servers'])
  })

  it('sets the server id in the calling shell, so cleanup can find it', () => {
    const r = run(`hcloud_create_server test-render 5; HCLOUD_KEY_ID=9; hcloud_cleanup`, { 'POST /servers': created(42) })
    expect(r.calls).toEqual(['POST /servers', 'DELETE /servers/42', 'DELETE /ssh_keys/9'])
  })
})

describe('hcloud_cleanup', () => {
  it('deletes the server and the key once, however often it is called', () => {
    const r = run(`HCLOUD_SERVER_ID=42; HCLOUD_KEY_ID=9; hcloud_cleanup; hcloud_cleanup`)
    expect(r.calls).toEqual(['DELETE /servers/42', 'DELETE /ssh_keys/9'])
  })

  it('does nothing when nothing was created', () => {
    expect(run(`hcloud_cleanup`).calls).toEqual([])
  })

  it('still deletes the key when deleting the server fails', () => {
    const r = run(`HCLOUD_SERVER_ID=42; HCLOUD_KEY_ID=9; hcloud_cleanup`, { 'DELETE /servers/42': [false, {}] })
    expect(r.calls).toEqual(['DELETE /servers/42', 'DELETE /ssh_keys/9'])
  })
})

describe('hcloud_sweep', () => {
  it('deletes the servers and keys labelled for this install, and asks only for those', () => {
    const r = run(`hcloud_sweep`, {
      'GET /servers?label_selector=barrelman-basemap-render%3Dprod': [true, { servers: [{ id: 11 }, { id: 12 }] }],
      'GET /ssh_keys?label_selector=barrelman-basemap-render%3Dprod': [true, { ssh_keys: [{ id: 3 }] }],
    })
    expect(r.calls).toEqual([
      'GET /servers?label_selector=barrelman-basemap-render%3Dprod',
      'DELETE /servers/11',
      'DELETE /servers/12',
      'GET /ssh_keys?label_selector=barrelman-basemap-render%3Dprod',
      'DELETE /ssh_keys/3',
    ])
  })

  it('deletes nothing when nothing was left behind', () => {
    const r = run(`hcloud_sweep`, {
      'GET /servers?label_selector=barrelman-basemap-render%3Dprod': [true, { servers: [] }],
      'GET /ssh_keys?label_selector=barrelman-basemap-render%3Dprod': [true, { ssh_keys: [] }],
    })
    expect(r.calls.filter((c) => c.startsWith('DELETE'))).toEqual([])
  })
})

describe('render_basemap_hcloud', () => {
  it('refuses to start without a token, before calling the API', () => {
    const r = run(`render_basemap_hcloud`, {}, { HCLOUD_TOKEN: '' })
    expect(r.exitCode).not.toBe(0)
    expect(r.calls).toEqual([])
  })
})
