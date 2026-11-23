import { describe, test, expect, spyOn } from 'bun:test'
import { assertSecretsConfigured, DEFAULT_API_KEY } from './secrets'

const REAL_DB = 'postgresql://barrelman:s3cr3t-Long%2Fvalue@barrelman-db:5432/barrelman'
const DEFAULT_DB = 'postgresql://barrelman:barrelman@barrelman-db:5432/barrelman'
const prod = (key?: string) => ({ NODE_ENV: 'production', BARRELMAN_API_KEY: key })

describe('assertSecretsConfigured', () => {
  test('starts in production with real secrets', () => {
    expect(() => assertSecretsConfigured(prod('brm_9f2c41d0a7e6b5c3'), REAL_DB)).not.toThrow()
  })

  test('refuses the published service key in production', () => {
    expect(() => assertSecretsConfigured(prod(DEFAULT_API_KEY), REAL_DB)).toThrow(/BARRELMAN_API_KEY/)
  })

  test('refuses the published database password in production', () => {
    expect(() => assertSecretsConfigured(prod('brm_9f2c41d0a7e6b5c3'), DEFAULT_DB)).toThrow(/BARRELMAN_DB_PASSWORD/)
  })

  test('allows both defaults outside production', () => {
    expect(() => assertSecretsConfigured({ BARRELMAN_API_KEY: DEFAULT_API_KEY }, DEFAULT_DB)).not.toThrow()
  })

  test('only warns in production when the key is unset', () => {
    const warn = spyOn(console, 'warn').mockImplementation(() => {})
    try {
      expect(() => assertSecretsConfigured(prod(), REAL_DB)).not.toThrow()
      expect(warn).toHaveBeenCalledTimes(1)
    } finally {
      warn.mockRestore()
    }
  })
})
