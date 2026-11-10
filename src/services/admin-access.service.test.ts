import { describe, test, expect } from 'bun:test'
import { createPasskeyPolicy } from './admin-access.service'

/**
 * The passkey requirement must never lock out every administrator, and once
 * on it must not be switchable off from a weaker session. These pin both.
 */

function quietLog() {
  const lines: string[] = []
  const push = (...args: unknown[]) => lines.push(args.map(String).join(' '))
  return { lines, log: { warn: push, info: push, error: push } }
}

describe('createPasskeyPolicy', () => {
  test('never enforces when the setting is off, and never queries', async () => {
    let queries = 0
    const policy = createPasskeyPolicy({
      required: false,
      countAdminPasskeys: async () => {
        queries++
        return 5
      },
    })
    await policy.startupCheck()
    expect(await policy.enforced()).toBe(false)
    expect(queries).toBe(0)
  })

  test('does not enforce, and warns at startup, when no administrator has a passkey', async () => {
    const { lines, log } = quietLog()
    const policy = createPasskeyPolicy({ required: true, countAdminPasskeys: async () => 0, log })
    await policy.startupCheck()
    expect(await policy.enforced()).toBe(false)
    expect(lines.join('\n')).toContain('NOT being enforced')
  })

  test('enforces from startup when an administrator has a passkey', async () => {
    const { log } = quietLog()
    const policy = createPasskeyPolicy({ required: true, countAdminPasskeys: async () => 1, log })
    await policy.startupCheck()
    expect(await policy.enforced()).toBe(true)
  })

  test('starts enforcing once the first passkey is registered, after the recheck interval', async () => {
    const { log } = quietLog()
    let count = 0
    let clock = 0
    const policy = createPasskeyPolicy({
      required: true,
      countAdminPasskeys: async () => count,
      recheckMs: 60_000,
      now: () => clock,
      log,
    })
    await policy.startupCheck()
    count = 1
    expect(await policy.enforced()).toBe(false) // inside the interval: cached
    clock = 60_000
    expect(await policy.enforced()).toBe(true)
  })

  test('stays enforced after the passkeys are deleted, until restart', async () => {
    const { log } = quietLog()
    let count = 1
    let clock = 0
    const policy = createPasskeyPolicy({
      required: true,
      countAdminPasskeys: async () => count,
      now: () => clock,
      log,
    })
    await policy.startupCheck()
    count = 0
    clock = 10 * 60_000
    expect(await policy.enforced()).toBe(true)

    // A restart builds a fresh policy, which sees no passkeys and stands down.
    const restarted = createPasskeyPolicy({ required: true, countAdminPasskeys: async () => count, log })
    await restarted.startupCheck()
    expect(await restarted.enforced()).toBe(false)
  })

  test('a failed count before the latch leaves it unenforced', async () => {
    const { log } = quietLog()
    const policy = createPasskeyPolicy({
      required: true,
      countAdminPasskeys: async () => {
        throw new Error('db down')
      },
      log,
    })
    await policy.startupCheck()
    expect(await policy.enforced()).toBe(false)
  })
})
