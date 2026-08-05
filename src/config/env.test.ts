/**
 * Tests for the env readers. The blank-variable case is the one that matters:
 * Compose defines every optional setting as an empty string, and the previous
 * `process.env.X ?? default` form read that as a configured 0.
 */
import { describe, test, expect, afterEach } from 'bun:test'
import { envNumber, envRaw, envString } from './env'

const NAME = 'BARRELMAN_ENV_TEST_VALUE'

afterEach(() => {
  delete process.env[NAME]
})

describe('envRaw', () => {
  test('reads a configured value, trimmed', () => {
    process.env[NAME] = '  hello  '
    expect(envRaw(NAME)).toBe('hello')
  })

  test('treats absent, empty and whitespace-only alike', () => {
    expect(envRaw(NAME)).toBeUndefined()
    process.env[NAME] = ''
    expect(envRaw(NAME)).toBeUndefined()
    process.env[NAME] = '   '
    expect(envRaw(NAME)).toBeUndefined()
  })
})

describe('envNumber', () => {
  test('falls back when the variable is blank rather than reading it as 0', () => {
    process.env[NAME] = ''
    expect(envNumber(NAME, 15)).toBe(15)
  })

  test('falls back when the variable is absent', () => {
    expect(envNumber(NAME, 15)).toBe(15)
  })

  test('honours an explicit 0, which several knobs use to mean disabled', () => {
    process.env[NAME] = '0'
    expect(envNumber(NAME, 15)).toBe(0)
  })

  test('reads integers and fractions', () => {
    process.env[NAME] = '42'
    expect(envNumber(NAME, 1)).toBe(42)
    process.env[NAME] = '0.8'
    expect(envNumber(NAME, 1)).toBe(0.8)
  })

  test('falls back on an unparseable value', () => {
    process.env[NAME] = 'soon'
    expect(envNumber(NAME, 15)).toBe(15)
  })
})

describe('envString', () => {
  test('falls back on blank, keeps a configured value', () => {
    process.env[NAME] = ''
    expect(envString<'open' | 'invite'>(NAME, 'open')).toBe('open')
    process.env[NAME] = 'invite'
    expect(envString<'open' | 'invite'>(NAME, 'open')).toBe('invite')
  })
})
