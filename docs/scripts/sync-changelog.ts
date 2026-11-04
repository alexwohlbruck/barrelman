/**
 * Copy the repo's CHANGELOG.md into the docs as content/docs/changelog.md, so
 * docs.barrelman.dev/changelog shows the released versions.
 *
 * Runs before `next dev` and `next build`. Netlify builds with `docs` as its
 * base directory but checks out the whole repository, so ../CHANGELOG.md is
 * there. The output is gitignored: CHANGELOG.md stays the one source.
 *
 * What changes on the way:
 *
 *  - `[Unreleased]` is dropped. It describes work that has not shipped, and a
 *    customer reading it would expect behaviour the API does not have yet.
 *  - The preamble is replaced. It is written for contributors (how a release
 *    is cut), and the page's own title comes from the frontmatter.
 *  - `## [0.10.38] - 2026-11-03` becomes `## 0.10.38 (2026-11-03)`. The file
 *    has no link definitions, so the brackets would render literally.
 *
 * The output is `.md`, which fumadocs-mdx compiles as plain markdown. As MDX,
 * any `<` or `{` outside a code span in a future entry would fail the build.
 */
import { readFileSync, writeFileSync } from 'node:fs'
import { dirname, resolve } from 'node:path'
import { fileURLToPath } from 'node:url'

const here = dirname(fileURLToPath(import.meta.url))
const source = resolve(here, '../../CHANGELOG.md')
const target = resolve(here, '../content/docs/changelog.md')

const VERSION_HEADING = /^## \[([^\]]+)\](?:\s*-\s*(.+))?\s*$/

export function renderChangelog(markdown: string): string {
  const lines = markdown.split('\n')
  const out: string[] = []
  let started = false
  let skipping = false

  for (const line of lines) {
    if (line.startsWith('## ')) {
      const match = VERSION_HEADING.exec(line)
      const version = match?.[1]
      skipping = !match || version?.toLowerCase() === 'unreleased'
      started = true
      if (!skipping) out.push(match?.[2] ? `## ${version} (${match[2].trim()})` : `## ${version}`)
      continue
    }
    if (started && !skipping) out.push(line)
  }

  const frontmatter = [
    '---',
    'title: Changelog',
    'description: What changed in each Barrelman release.',
    '---',
    '',
    'Each release of the API is listed here, newest first. The',
    '[stability policy](/usage/stability) describes how breaking changes are',
    'announced.',
    '',
  ]

  return [...frontmatter, ...out].join('\n').replace(/\n{3,}/g, '\n\n').trimEnd() + '\n'
}

if (import.meta.main) {
  writeFileSync(target, renderChangelog(readFileSync(source, 'utf8')))
  console.log(`[changelog] wrote ${target}`)
}
