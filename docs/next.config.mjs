import { createMDX } from 'fumadocs-mdx/next'

import { fileURLToPath } from 'url'
import { dirname, resolve } from 'path'

const __dirname = dirname(fileURLToPath(import.meta.url))

/** @type {import('next').NextConfig} */
const config = {
  reactStrictMode: true,
  // Prevent Next.js from walking up to the repo root for file tracing
  outputFileTracingRoot: resolve(__dirname, '..'),

  /**
   * The docs have no page of their own at `/` — every page comes from
   * content/docs, and nothing there claims the root.
   *
   * Done here rather than as an app/page.tsx, which is how it used to work.
   * With the docs moved to the root of the subdomain the catch-all already
   * matches `/`, and a static page beside it is two routes of equal
   * specificity claiming one URL.
   */
  async redirects() {
    return [{ source: '/', destination: '/introduction', permanent: false }]
  },
}

const withMDX = createMDX()

export default withMDX(config)
