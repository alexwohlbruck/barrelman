import type { BaseLayoutProps } from 'fumadocs-ui/layouts/shared'

const apiOrigin = process.env.NEXT_PUBLIC_API_ORIGIN ?? 'https://barrelman.parchment.app'

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
