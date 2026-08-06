import './globals.css'
import { RootProvider } from 'fumadocs-ui/provider/next'
import type { ReactNode } from 'react'

export default function RootLayout({ children }: { children: ReactNode }) {
  return (
    <html lang="en" suppressHydrationWarning>
      <body className="flex flex-col min-h-screen">
        {/*
          Search is served from /docs-search, not the conventional /api/search.
          Now that the docs sit at the root of their own subdomain, every page
          under content/docs/api/ owns a real /api/* URL — and one of them is
          search.mdx, documenting the API's own /search endpoint. A route
          handler at /api/search takes precedence over the catch-all, so the
          endpoint everyone reads about would have been shadowed by this site's
          own search index.
        */}
        <RootProvider search={{ options: { api: '/docs-search' } }}>
          {children}
        </RootProvider>
      </body>
    </html>
  )
}
