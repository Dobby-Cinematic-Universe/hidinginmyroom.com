import { defineConfig } from 'astro/config';
import starlight from '@astrojs/starlight';
import { fileURLToPath } from 'node:url';
import { createDevWatchIgnore } from './scripts/dev-watch-ignore.mjs';
import { privateRagProxy } from './scripts/rag/dev-proxy.mjs';

const site = process.env.SITE_URL?.trim() || 'https://hidinginmyroom.com';

export default defineConfig({
  site,
  vite: {
    plugins: [privateRagProxy()],
    server: {
      watch: {
        ignored: [createDevWatchIgnore(fileURLToPath(new URL('.', import.meta.url)))],
      },
    },
  },
  integrations: [
    starlight({
      title: 'HIMR WIKI',
      description:
        'A community-built guide to Hiding In My Room, Daniel Lord, and the HIMR community.',
      favicon: '/images/wiki/daniel-channel-avatar-2026-08-25.jpg',
      customCss: ['./src/styles/starlight.css'],
      components: {
        SiteTitle: './src/components/wiki/WikiSiteTitle.astro',
      },
      sidebar: [
        { label: 'Main Site', link: '/' },
        { label: 'Transcript Corpus', link: '/corpus/' },
        { label: 'Wiki Portal', link: '/wiki/' },
        {
          label: 'Overview & Chronology',
          items: [{ autogenerate: { directory: 'wiki/overview' } }],
        },
        {
          label: 'Characters & Figures',
          items: [{ autogenerate: { directory: 'wiki/characters' } }],
        },
        {
          label: 'Eras & Locations',
          items: [{ autogenerate: { directory: 'wiki/eras' } }],
        },
        {
          label: 'Events & Controversies',
          items: [{ autogenerate: { directory: 'wiki/events' } }],
        },
        {
          label: 'Archive & Sources',
          items: [{ autogenerate: { directory: 'wiki/archive' } }],
        },
      ],
    }),
  ],
});
