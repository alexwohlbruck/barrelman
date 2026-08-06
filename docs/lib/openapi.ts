import { createOpenAPI } from 'fumadocs-openapi/server'

// The API reference is rendered from the committed spec at docs/openapi.json.
// Each api/*.mdx page passes document={"./openapi.json"} to <APIPage>, so the
// build is fully reproducible on Netlify without a live Barrelman server.
//
// Source of truth: the live spec served at
//   https://barrelman.parchment.app/swagger/json
// To refresh, re-download it to docs/openapi.json (or run `bun run generate:api`).
export const openapi = createOpenAPI()
