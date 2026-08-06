import type { BaseLayoutProps } from 'fumadocs-ui/layouts/shared'

/**
 * Where the API itself lives. Its own subdomain, not a path on this one.
 *
 * The default used to be barrelman.parchment.app, from before the service had
 * a domain of its own — a link that now points at the wrong product.
 */
const apiOrigin = process.env.NEXT_PUBLIC_API_ORIGIN ?? 'https://api.barrelman.dev'

export function baseOptions(): BaseLayoutProps {
  return {
    nav: {
      title: 'Barrelman Docs',
    },
    links: [
      {
        text: 'API',
        url: apiOrigin,
        external: true,
      },
      {
        text: 'GitHub',
        url: 'https://github.com/alexwohlbruck/barrelman',
        external: true,
      },
    ],
  }
}
