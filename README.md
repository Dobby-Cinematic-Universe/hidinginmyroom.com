# HIMR Community Site

A static community homepage and growing wiki for Hiding In My Room, built with
Astro and Starlight.

- Production site: [hidinginmyroom.com](https://hidinginmyroom.com)
- Public repository: [Dobby-Cinematic-Universe/hidinginmyroom.com](https://github.com/Dobby-Cinematic-Universe/hidinginmyroom.com)

> [!IMPORTANT]
> This is the public, publication-safe repository. It contains the site source,
> reviewed public content, and approved web assets. Private research, raw source
> captures, members-only media, Discord exports,
> unpublished review notes, and local analysis files are kept in separate private
> storage and must never be committed here. See the
> [publication safety section](./docs/CLOUDFLARE_PAGES.md#repository-publication-safety)
> before importing material or connecting a deployment.

## Start here

- [Contributing guide](./CONTRIBUTING.md)
- [Editorial and verification policy](./EDITORIAL_POLICY.md)
- [Cloudflare Pages deployment guide](./docs/CLOUDFLARE_PAGES.md)
- [Security and private-reporting guidance](./SECURITY.md)
- [Rights and reuse status](./RIGHTS.md)

## Local development

Use the Node.js version in `.node-version` and install the locked dependency set:

```sh
npm ci
npm run dev
```

Before opening a pull request, run the same production gate used by CI and
Cloudflare Pages:

```sh
npm run build
```

The generated static site is written to `dist/`. Run `npm run preview` after a
successful build to inspect that output locally.

## Commands

| Command | Purpose |
| --- | --- |
| `npm run dev` | Start the local Astro development server |
| `npm run check` | Check release hygiene, referenced local images, and Astro types |
| `npm run build` | Run all checks and create the production site in `dist/` |
| `npm run preview` | Serve an existing production build locally |

Networked source-acquisition commands, private transcripts, and raw evidence bundles
belong to the maintainers' separate private research workspace. They are not normal
setup or deployment inputs for this public repository.

## Repository map

- `src/pages/index.astro` — homepage content and structure
- `src/data/links.ts` — official and community destinations
- `src/content/docs/wiki/` — reviewed, publicly visible wiki entries
- `src/components/` and `src/styles/` — shared interface and styling
- `public/` — static assets shipped with the site
- `scripts/` — build and public-content validation tooling

The public repository does not contain raw platform snapshots, NotebookLM drafts,
review queues, private Discord captures, local review videos, or internal claim
ledgers. Public sources are cited directly in the reviewed pages without copying the
source media into Git.

The homepage is a community directory and does not use the private editorial record.
Every contestable wiki statement and image must follow
[`EDITORIAL_POLICY.md`](./EDITORIAL_POLICY.md). Machine transcripts and NotebookLM
drafts are discovery aids, never evidence by themselves.

## Deployment

The repository builds as a static Astro site; it does not need a Cloudflare runtime
adapter or Functions. Use `npm run build` as the Cloudflare Pages build command and
`dist` as the output directory. The configured canonical origin is
`https://hidinginmyroom.com`. The complete setup, including production URL and preview
guidance, is in [`docs/CLOUDFLARE_PAGES.md`](./docs/CLOUDFLARE_PAGES.md).
