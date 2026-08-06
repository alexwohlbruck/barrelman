import { docs } from 'collections/index'
import { loader } from 'fumadocs-core/source'

export const source = loader({
  // The site is its own subdomain, so the docs are the root of it. A `/docs`
  // base here put every page at docs.barrelman.dev/docs/…, which says the same
  // word twice and makes the host look like a path prefix that got left in.
  //
  // The routes live under app/(docs)/ — a route group, so the parentheses are
  // for the layout's benefit and never reach the URL.
  baseUrl: '/',
  source: docs.toFumadocsSource(),
})
