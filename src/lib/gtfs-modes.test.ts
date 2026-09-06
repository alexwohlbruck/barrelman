import { describe, test, expect } from 'bun:test'
import { gtfsModeClass, isGtfsModeClass } from './gtfs-modes'

describe('gtfsModeClass', () => {
  test('classes the basic route types', () => {
    expect(gtfsModeClass(0)).toBe('tram')
    expect(gtfsModeClass(1)).toBe('metro')
    expect(gtfsModeClass(2)).toBe('regional')
    expect(gtfsModeClass(3)).toBe('bus')
    expect(gtfsModeClass(4)).toBe('ferry')
    expect(gtfsModeClass(11)).toBe('bus')
    expect(gtfsModeClass(12)).toBe('monorail')
  })

  test('classes the extended HVT ranges', () => {
    expect(gtfsModeClass(109)).toBe('regional')
    expect(gtfsModeClass(401)).toBe('metro')
    expect(gtfsModeClass(700)).toBe('bus')
    expect(gtfsModeClass(900)).toBe('tram')
    expect(gtfsModeClass(1200)).toBe('ferry')
    expect(gtfsModeClass(1400)).toBe('funicular')
  })

  test('405 is a monorail, not the metro range it sits in', () => {
    expect(gtfsModeClass(405)).toBe('monorail')
  })

  test('an unclaimed or missing type matches nothing', () => {
    expect(gtfsModeClass(1600)).toBeNull()
    expect(gtfsModeClass(null)).toBeNull()
    expect(gtfsModeClass(undefined)).toBeNull()
    expect(gtfsModeClass(NaN)).toBeNull()
  })
})

describe('isGtfsModeClass', () => {
  test('accepts a class name and rejects anything else', () => {
    expect(isGtfsModeClass('metro')).toBe(true)
    expect(isGtfsModeClass('subway')).toBe(false)
    expect(isGtfsModeClass(undefined)).toBe(false)
  })
})
