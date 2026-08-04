// Regenerates the per-tag API reference pages under content/docs/api/ from the
// committed OpenAPI spec at docs/openapi.json.
//
// The spec is committed so Netlify builds are reproducible without a live
// server. To refresh it, re-download the source of truth:
//
//   curl -sSf https://barrelman.parchment.app/swagger/json -o openapi.json
//
// then run `bun run generate:api`.
import { generateFiles } from 'fumadocs-openapi'

const specPath = './openapi.json'

console.log(`Generating API reference from: ${specPath}`)

await generateFiles({
  input: [specPath],
  output: './content/docs/api',
  per: 'tag',
  includeDescription: true,
})

console.log('API reference pages generated in content/docs/api/')
