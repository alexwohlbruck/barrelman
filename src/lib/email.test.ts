/**
 * Tests for address canonicalisation and throwaway-domain detection.
 *
 * The normalisation rules are the free-tier abuse control, so the cases that
 * matter most are the ones where two different-looking addresses must collapse
 * to one key — and, just as importantly, the ones where they must NOT.
 */
import { describe, test, expect } from 'bun:test'
import { emailDomain, isDisposableEmail, isValidEmail, normalizeEmail } from './email'

describe('isValidEmail', () => {
  test.each([
    ['alex@example.com', true],
    ['alex.wohlbruck+tag@gmail.com', true],
    ['a@b.co', true],
    ['no-at-sign', false],
    ['no@tld', false],
    ['two@@at.com', false],
    ['spaces in@example.com', false],
    ['', false],
  ])('%s → %p', (input, expected) => {
    expect(isValidEmail(input)).toBe(expected)
  })

  test('rejects addresses longer than the RFC limit', () => {
    expect(isValidEmail(`${'a'.repeat(250)}@example.com`)).toBe(false)
  })
})

describe('emailDomain', () => {
  test('lowercases and de-aliases known equivalents', () => {
    expect(emailDomain('Alex@Example.COM')).toBe('example.com')
    expect(emailDomain('alex@googlemail.com')).toBe('gmail.com')
    expect(emailDomain('alex@protonmail.com')).toBe('proton.me')
    expect(emailDomain('alex@pm.me')).toBe('proton.me')
  })

  test('returns empty string for malformed input', () => {
    expect(emailDomain('nonsense')).toBe('')
  })
})

describe('normalizeEmail', () => {
  test('folds plus-tags on providers that alias them', () => {
    expect(normalizeEmail('alex+barrelman@gmail.com')).toBe('alex@gmail.com')
    expect(normalizeEmail('alex+1@outlook.com')).toBe('alex@outlook.com')
    expect(normalizeEmail('alex+anything@fastmail.com')).toBe('alex@fastmail.com')
  })

  test('folds dots only where the provider ignores them', () => {
    expect(normalizeEmail('a.l.e.x@gmail.com')).toBe('alex@gmail.com')
    // Everyone else treats dots as significant, so they must survive.
    expect(normalizeEmail('a.l.e.x@example.com')).toBe('a.l.e.x@example.com')
  })

  test('leaves plus-tags alone on unknown domains', () => {
    // A self-hosted server may genuinely deliver these to different mailboxes,
    // so folding them would merge two unrelated people onto one account.
    expect(normalizeEmail('alex+work@self-hosted.dev')).toBe('alex+work@self-hosted.dev')
  })

  test('collapses every trivial variant of one gmail inbox to one key', () => {
    const variants = [
      'Alex.Wohlbruck@gmail.com',
      'alexwohlbruck@googlemail.com',
      'a.lexwohlbruck+barrelman@gmail.com',
      'ALEXWOHLBRUCK+2@GMAIL.COM',
    ]
    const keys = new Set(variants.map(normalizeEmail))
    expect(keys.size).toBe(1)
    expect([...keys][0]).toBe('alexwohlbruck@gmail.com')
  })

  test('is idempotent', () => {
    const once = normalizeEmail('Alex.W+tag@Gmail.com')
    expect(normalizeEmail(once)).toBe(once)
  })

  test('passes through malformed input unchanged but lowercased', () => {
    expect(normalizeEmail('  NotAnEmail ')).toBe('notanemail')
  })
})

describe('isDisposableEmail', () => {
  test('flags known throwaway providers', () => {
    expect(isDisposableEmail('someone@mailinator.com')).toBe(true)
    expect(isDisposableEmail('SOMEONE@YOPMAIL.COM')).toBe(true)
  })

  test('does not flag ordinary providers', () => {
    expect(isDisposableEmail('alex@gmail.com')).toBe(false)
    expect(isDisposableEmail('alex@parchment.app')).toBe(false)
  })
})
