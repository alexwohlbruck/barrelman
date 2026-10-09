import { describe, expect, it } from 'bun:test'
import { argValue } from './cli-args'

describe('argValue', () => {
  it('reads an option in either form', () => {
    expect(argValue(['--bbox', '1,2,3,4'], 'bbox')).toBe('1,2,3,4')
    // What the console sends for a value starting with "-".
    expect(argValue(['--bbox=-80.9,35.1,-80.7,35.3'], 'bbox')).toBe('-80.9,35.1,-80.7,35.3')
  })

  it('is undefined when the option is absent or has no value', () => {
    expect(argValue(['--cell', '0.1'], 'bbox')).toBeUndefined()
    expect(argValue(['--bbox'], 'bbox')).toBeUndefined()
  })

  it('does not take an option that only starts with the name', () => {
    expect(argValue(['--bboxes=1,2,3,4'], 'bbox')).toBeUndefined()
  })
})
