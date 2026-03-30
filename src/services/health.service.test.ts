import { describe, test, expect, mock, beforeEach } from 'bun:test'

// ── Mocks ─────────────────────────────────────────────────────────────────────

const mockExecute = mock(async () => [{ '?column?': 1 }] as any[])

mock.module('../db', () => ({ db: { execute: mockExecute } }))

// Dynamic import ensures mocks are registered before the module loads
const { checkHealth } = await import('./health.service')

// ── Setup ─────────────────────────────────────────────────────────────────────

beforeEach(() => {
  mockExecute.mockReset()
})

// ── Tests ─────────────────────────────────────────────────────────────────────

describe('checkHealth', () => {
  test('returns ok/connected when DB is reachable', async () => {
    mockExecute.mockImplementation(async () => [{ '?column?': 1 }])
    const result = await checkHealth()
    expect(result.status).toBe('ok')
    expect(result.database).toBe('connected')
  })

  test('returns error/disconnected when DB throws', async () => {
    mockExecute.mockImplementation(async () => { throw new Error('Connection refused') })
    const result = await checkHealth()
    expect(result.status).toBe('error')
    expect(result.database).toBe('disconnected')
  })

  test('does not rethrow DB errors — always returns a result', async () => {
    mockExecute.mockImplementation(async () => { throw new Error('timeout') })
    await expect(checkHealth()).resolves.toBeDefined()
  })

  test('executes exactly one DB query', async () => {
    mockExecute.mockImplementation(async () => [])
    await checkHealth()
    expect(mockExecute).toHaveBeenCalledTimes(1)
  })
})
