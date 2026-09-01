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
    description: 'Daniel’s main channel and the home of new Hiding in my room uploads.',
    href: 'https://www.youtube.com/@hidinginmyroom1111',
    mark: '▶',
    tone: 'signal',
    note: 'Main channel',
  },
  {
    label: 'Instagram',
    handle: '@danielhimr',
    description: 'Daniel’s new Instagram account.',
    href: 'https://www.instagram.com/danielhimr',
    mark: '◎',
    tone: 'warm',
    note: 'New account',
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
      'Daniel is a member as notdaniel0594_05557, user ID 1528841527022059661.',
    href: 'https://discord.gg/X58d8hhxgU',
    mark: '#',
    tone: 'signal',
    note: 'Daniel is a member',
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
