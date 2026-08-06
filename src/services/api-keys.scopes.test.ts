/**
 * Tests for API key scope normalisation.
 *
 * This is the function that decides what a leaked key can reach, so the cases
 * that matter are the ones where a caller supplies something unexpected —
 * empty, unknown, or a wildcard mixed with narrow scopes — and the result has
 * to fail safe in the right direction.
 */
import { describe, test, expect } from 'bun:test'
import { normalizeScopes } from './api-keys.service'
import { CREDIT_COSTS, scopeAllows, type EndpointGroup } from '../billing/plans'

const groups = Object.keys(CREDIT_COSTS) as EndpointGroup[]

describe('normalizeScopes', () => {
  test('no scopes means full access, matching the documented default', () => {
    expect(normalizeScopes(undefined)).toEqual(['*'])
    expect(normalizeScopes([])).toEqual(['*'])
  })

  test('keeps the groups it was given', () => {
    expect(normalizeScopes(['tiles', 'search'])).toEqual(['tiles', 'search'])
  })

  test('collapses a wildcard mixed with narrow scopes to just the wildcard', () => {
    // Storing both would suggest to a later reader that the narrow entries
    // constrain something. They do not — `*` already allows everything.
    expect(normalizeScopes(['*', 'tiles'])).toEqual(['*'])
    expect(normalizeScopes(['tiles', '*', 'search'])).toEqual(['*'])
  })

  test('drops unknown scopes rather than storing them', () => {
    expect(normalizeScopes(['tiles', 'not-a-group'])).toEqual(['tiles'])
  })

  test('deduplicates', () => {
    expect(normalizeScopes(['tiles', 'tiles', 'search'])).toEqual(['tiles', 'search'])
  })

  test('a list of only unknown scopes falls back to full access', () => {
    // Debatable, and worth knowing: it fails OPEN. The route layer rejects
    // unknown scopes with a 400 before reaching here, so this path is only hit
    // by an internal caller — but if that ever changes, a typo would silently
    // widen a key instead of narrowing it.
    expect(normalizeScopes(['nonsense', 'also-nonsense'])).toEqual(['*'])
  })

  test('every valid group survives normalisation', () => {
    expect(normalizeScopes(groups)).toEqual(groups)
  })
})

describe('normalizeScopes composed with the enforcement check', () => {
  test('a wildcard key reaches every group', () => {
    const scopes = normalizeScopes(undefined)
    for (const group of groups) expect(scopeAllows(scopes, group)).toBe(true)
  })

  test('a narrow key reaches only its own groups', () => {
    const scopes = normalizeScopes(['tiles'])

    expect(scopeAllows(scopes, 'tiles')).toBe(true)
    // The point of a tiles-only key in a public web map: worthless for running
    // up a routing bill if it leaks.
    expect(scopeAllows(scopes, 'routing')).toBe(false)
    expect(scopeAllows(scopes, 'isochrone')).toBe(false)
  })

  test('a dropped unknown scope does not accidentally grant access', () => {
    const scopes = normalizeScopes(['tiles', 'routing-typo'])

    expect(scopeAllows(scopes, 'tiles')).toBe(true)
    expect(scopeAllows(scopes, 'routing')).toBe(false)
  })
})
