import { defineConfig } from 'astro/config';
import starlight from '@astrojs/starlight';

const site = process.env.SITE_URL?.trim() || 'https://hidinginmyroom.com';

export default defineConfig({
  site,
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
