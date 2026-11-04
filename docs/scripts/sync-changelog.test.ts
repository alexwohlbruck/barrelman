import { describe, expect, test } from 'bun:test'
import { renderChangelog } from './sync-changelog'

const SAMPLE = `# Changelog

Preamble for contributors.

## [Unreleased]

### Added

* Not shipped yet.

## [0.2.0] - 2026-11-03

### Fixed

* A shipped fix with a \`<placeholder>\` and \`{z}\`.

## [0.1.0] - 2026-10-01

* First release.
`

describe('renderChangelog', () => {
  const page = renderChangelog(SAMPLE)

  test('keeps released versions and drops Unreleased and the preamble', () => {
    expect(page).toContain('## 0.2.0 (2026-11-03)')
    expect(page).toContain('## 0.1.0 (2026-10-01)')
    expect(page).toContain('A shipped fix')
    expect(page).not.toContain('Unreleased')
    expect(page).not.toContain('Not shipped yet')
    expect(page).not.toContain('Preamble for contributors')
  })

  test('starts with frontmatter the docs loader reads', () => {
    expect(page.startsWith('---\ntitle: Changelog\n')).toBe(true)
  })
})
