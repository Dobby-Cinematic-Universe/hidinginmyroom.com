# HIMR Community Site

A static community homepage, reviewed wiki, and source-aware transcript corpus for
Hiding In My Room, built with Astro and Starlight.

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
- [Corpus architecture](./docs/CORPUS_ARCHITECTURE.md)
- [Corpus roadmap](./docs/CORPUS_ROADMAP.md)
- [Corpus review and confidence guide](./docs/CORPUS_REVIEW.md)
- [Corpus rights, privacy, and takedown policy](./docs/CORPUS_RIGHTS_AND_TAKEDOWN.md)
- [Corpus engineering risk register](./docs/CORPUS_RISK_REGISTER.md)
- [Corpus static-release scale benchmark](./docs/CORPUS_SCALE_BENCHMARK.md)
- [Local pipeline operator console](./operator_console/README.md)
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

| Command                  | Purpose                                                                |
| ------------------------ | ---------------------------------------------------------------------- |
| `npm run dev`            | Start the local Astro development server                               |
| `npm run check`          | Check release hygiene, referenced local images, and Astro types        |
| `npm run build`          | Run all checks and create the production site in `dist/`               |
| `npm run operator:init`  | Initialize the private local pipeline-console workspace                |
| `npm run operator`       | Open the loopback-only registered pipeline operator console            |
| `npm run test:contracts` | Validate JSON Schemas and tracked work-order examples                  |
| `npm run test:operator`  | Run console registry, supervisor, HTTP, and UI contract tests          |
| `npm run test:python`    | Run the corpus, acquisition, preprocessing, and evaluation test suites |
| `npm run preview`        | Serve an existing production build locally                             |

Networked source-acquisition commands, private transcripts, and raw evidence bundles
belong to the maintainers' separate private research workspace. They are not normal
setup or deployment inputs for this public repository.

## Repository map

- `src/pages/index.astro` — homepage content and structure
- `src/data/links.ts` — official and community destinations
- `src/content/docs/wiki/` — reviewed, publicly visible wiki entries
- `src/pages/corpus/` — publication-safe source and transcript explorer
- `src/data/corpus/manifest.json` and `src/data/corpus/releases/` — deterministic,
  content-addressed, publication-gated corpus release (v1 `release.json` remains a
  migration fallback)
- `corpus/` — schema, import, validation, and export code; never raw media
- `acquisition/` — guarded local, public-HTTP, and public-YouTube acquisition contracts
- `pipeline/` — reproducible media-processing contracts; outputs stay private
- `operator_console/` — loopback-only registered pipeline controls; state stays private
- `src/components/` and `src/styles/` — shared interface and styling
- `public/` — static assets shipped with the site
- `scripts/` — build and public-content validation tooling

The public repository does not contain raw platform snapshots, NotebookLM drafts,
review queues, private Discord captures, local review videos, biometric embeddings,
or internal claim ledgers. It may contain a deterministic publication-safe corpus
release: reviewed public-source metadata and transcript material that passed the
separate rights, privacy, sensitivity, and publication gates. Transcript wording may
still be unreviewed machine output; the release carries that status and a prominent
non-quotation disclaimer with each revision and search result. Source media remains
outside Git.

The homepage is a community directory and does not use the private editorial record.
Every contestable wiki statement and image must follow
[`EDITORIAL_POLICY.md`](./EDITORIAL_POLICY.md). Corpus output, machine transcripts,
and NotebookLM drafts are discovery aids, never evidence by themselves. The corpus
is intentionally indexed separately from the wiki so raw search hits do not read as
editorial conclusions.

## Deployment

The repository builds as a static Astro site; it does not need a Cloudflare runtime
adapter or Functions. Use `npm run build` as the Cloudflare Pages build command and
`dist` as the output directory. The configured canonical origin is
`https://hidinginmyroom.com`. The complete setup, including production URL and preview
guidance, is in [`docs/CLOUDFLARE_PAGES.md`](./docs/CLOUDFLARE_PAGES.md).
