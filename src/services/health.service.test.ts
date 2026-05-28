import { describe, test, expect, mock, beforeEach } from 'bun:test'

// ── Mocks ─────────────────────────────────────────────────────────────────────

const mockExecute = mock(async () => [{ '?column?': 1 }] as any[])

mock.module('../db', () => ({ db: { execute: mockExecute } }))

// Dynamic import ensures mocks are registered before the module loads
const { checkHealth } = await import('./health.service')

// Mock MOTIS health check — returns ok by default
const mockCheckMotisHealth = mock(async () => ({ status: 'ok' as const }))

// ── Setup ─────────────────────────────────────────────────────────────────────

beforeEach(() => {
  mockExecute.mockReset()
  mockCheckMotisHealth.mockReset()
  mockCheckMotisHealth.mockImplementation(async () => ({ status: 'ok' as const }))
})

// ── Tests ─────────────────────────────────────────────────────────────────────

describe('checkHealth', () => {
  test('returns ok/connected when DB and MOTIS are reachable', async () => {
    mockExecute.mockImplementation(async () => [{ '?column?': 1 }])
    const result = await checkHealth({ checkMotisHealth: mockCheckMotisHealth })
    expect(result.status).toBe('ok')
    expect(result.database).toBe('connected')
    expect(result.motis).toBe('ok')
  })

  test('returns error/disconnected when DB throws', async () => {
    mockExecute.mockImplementation(async () => { throw new Error('Connection refused') })
    const result = await checkHealth({ checkMotisHealth: mockCheckMotisHealth })
    expect(result.status).toBe('error')
    expect(result.database).toBe('disconnected')
  })

  test('returns degraded when DB is up but MOTIS is down', async () => {
    mockExecute.mockImplementation(async () => [{ '?column?': 1 }])
    mockCheckMotisHealth.mockImplementation(async () => ({ status: 'unavailable' as const, message: 'Connection refused' }))
    const result = await checkHealth({ checkMotisHealth: mockCheckMotisHealth })
    expect(result.status).toBe('degraded')
    expect(result.database).toBe('connected')
    expect(result.motis).toBe('unavailable')
  })

  test('returns error when both DB and MOTIS are down', async () => {
    mockExecute.mockImplementation(async () => { throw new Error('timeout') })
    mockCheckMotisHealth.mockImplementation(async () => ({ status: 'unavailable' as const }))
    const result = await checkHealth({ checkMotisHealth: mockCheckMotisHealth })
    expect(result.status).toBe('error')
    expect(result.database).toBe('disconnected')
    expect(result.motis).toBe('unavailable')
  })

  test('does not rethrow DB errors — always returns a result', async () => {
    mockExecute.mockImplementation(async () => { throw new Error('timeout') })
    await expect(checkHealth({ checkMotisHealth: mockCheckMotisHealth })).resolves.toBeDefined()
  })

  test('executes exactly one DB query', async () => {
    mockExecute.mockImplementation(async () => [])
    await checkHealth({ checkMotisHealth: mockCheckMotisHealth })
    expect(mockExecute).toHaveBeenCalledTimes(1)
  })
})
