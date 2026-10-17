export type LinkTone = 'signal' | 'warm' | 'cool' | 'muted';

export interface SiteLink {
  label: string;
  handle: string;
  description: string;
  href: string;
  mark: string;
  tone: LinkTone;
  note?: string;
  wide?: boolean;
}

export const officialLinks: SiteLink[] = [
  {
    label: 'YouTube',
    handle: '@hidinginmyroom1111',
    description: 'Daniel’s main channel; another YouTube break was announced on 27 September 2026.',
    href: 'https://www.youtube.com/@hidinginmyroom1111',
    mark: '▶',
    tone: 'signal',
    note: 'On hiatus',
  },
  {
    label: 'Instagram',
    handle: '@danielhimr',
    description: 'Creator-linked profile. On 3 October 2026, Daniel said he had uninstalled the app; account deletion is not established.',
    href: 'https://www.instagram.com/danielhimr',
    mark: '◎',
    tone: 'warm',
    note: 'Profile link',
  },
  {
    label: 'Amazon page',
    handle: 'amazon.com/shop/hidinginmyroom',
    description:
      'A Hiding in my room-branded recommendations page; Amazon labels it as earning revenue.',
    href: 'https://www.amazon.com/shop/hidinginmyroom',
    mark: 'a',
    tone: 'muted',
    note: 'Affiliate storefront',
  },
  {
    label: 'Merch store',
    handle: "Daniel's Store",
    description: 'HIMR shirts, mugs, stickers, and other community-era designs.',
    href: 'https://daniels-store-154.creator-spring.com/',
    mark: '✦',
    tone: 'cool',
    note: 'Creator Spring',
  },
];

export const communityLinks: SiteLink[] = [
  {
    label: 'Discussion forum',
    handle: 'HIMR Forum',
    description: 'A dedicated community forum for Daniel Lord and Hiding In My Room discussion.',
    href: 'https://forum.hidinginmyroom.com/',
    mark: 'F',
    tone: 'signal',
    note: 'New forum',
  },
  {
    label: 'Primary subreddit',
    handle: 'r/HIMRFAM2',
    description: 'The main Reddit hub for current HIMR posts and community discussion.',
    href: 'https://www.reddit.com/r/HIMRFAM2/',
    mark: 'r/',
    tone: 'cool',
    note: 'Main community',
  },
  {
    label: 'Original subreddit',
    handle: 'r/HIMRFAM',
    description: 'The original subreddit remains readable and has recent posts, but posting is restricted.',
    href: 'https://www.reddit.com/r/HIMRFAM/',
    mark: 'r/',
    tone: 'muted',
    note: 'Restricted posting',
  },
  {
    label: 'Unofficial Discord',
    handle: 'HIMR community server',
    description:
      'Community-run server where Daniel previously posted. His departure is reported as 5 October 2026; the wiki preserves the source limits.',
    href: 'https://discord.gg/hgBhC5gNPU',
    mark: '#',
    tone: 'signal',
    note: 'Community server',
  },
  {
    label: 'Uncensored Discord',
    handle: 'Additional discussion',
    description: 'A separate unofficial server for uncensored HIMR community discussion.',
    href: 'https://discord.gg/jYgHFQPFzq',
    mark: '#',
    tone: 'warm',
    note: 'Unofficial',
  },
  {
    label: 'GitHub',
    handle: 'hidinginmyroom.com',
    description:
      'Browse the site source, report a problem, or propose a contribution to the homepage or wiki.',
    href: 'https://github.com/Dobby-Cinematic-Universe/hidinginmyroom.com',
    mark: '<>',
    tone: 'cool',
    note: 'Site repository',
  },
];
