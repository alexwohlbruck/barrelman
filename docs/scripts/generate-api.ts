// Regenerates the per-tag API reference pages under content/docs/api/ from the
// committed OpenAPI spec at docs/openapi.json.
//
// The spec is committed so Netlify builds are reproducible without a live
// server. To refresh it, re-download the source of truth:
//
//   curl -sSf https://api.barrelman.dev/docs/json -o openapi.json
//
// then run `bun run generate:api`. An endpoint that has not shipped yet is
// only in a branch's own spec, served at /docs/json by that branch's API.
import { generateFiles } from 'fumadocs-openapi'

const specPath = './openapi.json'

// fumadocs writes one page per entry in the spec's top-level `tags`, and
// nothing at all when that list is missing — which let a hand-trimmed spec
// "generate" successfully for months while new endpoints never got a page.
const spec = await Bun.file(specPath).json()
const tags: Array<{ name: string }> = spec.tags ?? []
const operations = Object.values(spec.paths ?? {}).flatMap((item: any) => Object.values(item)) as any[]
if (tags.length === 0) {
  throw new Error(`${specPath} has no top-level "tags", so no pages would be generated. Re-download it from /docs/json.`)
}
const untaggedTags = [...new Set(operations.flatMap((op) => op.tags ?? []))].filter(
  (t) => !tags.some((tag) => tag.name === t),
)
if (untaggedTags.length) {
  throw new Error(`Operations use tags missing from the top-level list, so they would get no page: ${untaggedTags.join(', ')}`)
}

console.log(`Generating API reference from: ${specPath} (${operations.length} operations, ${tags.length} tags)`)

await generateFiles({
  input: [specPath],
  output: './content/docs/api',
  per: 'tag',
  includeDescription: true,
})

console.log('API reference pages generated in content/docs/api/')
