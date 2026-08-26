# Deploying the HIMR site with Cloudflare Pages

This project is a fully static Astro site for
[hidinginmyroom.com](https://hidinginmyroom.com). Cloudflare Pages can build it
directly from
[Dobby-Cinematic-Universe/hidinginmyroom.com](https://github.com/Dobby-Cinematic-Universe/hidinginmyroom.com)
and publish only the generated `dist/` directory. No Cloudflare adapter, Pages
Function, Worker, database, or application secret is required.

## Repository publication safety

Cloudflare's output-directory setting controls what becomes a website; it does not
control what GitHub exposes. This public repository must contain only the static-site
source, reviewed public content, and approved
web assets. Private research, raw source captures, members-only media, Discord
exports, unpublished review notes, and local analysis files are not part of it.

The public repository should be created with fresh history rather than by making a
private research repository public or copying its Git history. Deleting or ignoring
a file in a later commit does not remove it from earlier commits, forks, caches, or
notification messages.

Before the initial push and each substantial research import:

1. Confirm that the repository root is the intended fresh public project, not a
   parent working directory or a nested copy of another repository.
2. Review every staged path and binary; do not bulk-copy or merge the private
   research workspace into this repository.
3. Scan the complete public history for secrets, signed URLs, personal data, raw
   snapshots, private messages, and access-controlled media.
4. Confirm that all shipped assets have documented source, transformation, hash, and
   rights or permission basis.
5. Run `npm run build` from a clean checkout and inspect `dist/` before connecting
   Cloudflare Pages.

`.gitignore` and `"private": true` in `package.json` are useful safeguards, but they
do not make committed GitHub content private. Keep the private research workspace
and its backups outside the public repository. Promote only reviewed prose and the
minimum public metadata needed to audit it.

## Cloudflare Pages build settings

Create a Pages project, connect
`Dobby-Cinematic-Universe/hidinginmyroom.com`, and use:

| Setting | Value |
| --- | --- |
| Framework preset | `Astro` |
| Production branch | The repository's default branch |
| Root directory | Repository root (leave blank) |
| Build command | `npm run build` |
| Build output directory | `dist` |
| Node.js version | `22.16.0` from `.node-version` |

Keep the full `npm run build` command. It checks the public release boundary and
referenced local images, runs Astro diagnostics, and creates the static site, so
Cloudflare will not deploy a change that fails the same gate as GitHub Actions.

The Astro configuration already defaults to the production canonical origin. You
may also set this non-secret production environment variable explicitly in
Cloudflare:

```text
SITE_URL=https://hidinginmyroom.com
```

Use the origin only—no trailing page path. Preview builds intentionally retain the
production canonical origin unless `SITE_URL` is explicitly overridden.

Do not add a Discord token, GitHub token, Cloudflare API token, or other secret to
the Pages project. The static production build does not need one. Research sync
commands are intentionally not part of deployment.

## GitHub and preview behavior

- Pushes to the selected production branch trigger production builds.
- Other enabled branches and pull requests receive preview deployments.
- Cloudflare adds a no-index response header to preview deployments by default, but
  a preview is still a public URL unless access controls are configured.
- Pull-request preview links are available for branches in the connected repository;
  Cloudflare does not create the same preview status for a pull request from a fork.

Treat preview builds as public. Never use one to review private evidence, unpublished
research, secret-bearing logs, or content that would violate the editorial policy.

GitHub Actions also runs `npm run build`, but it does not deploy. In repository
settings, make the CI build a required status check before merging to the production
branch. Keep workflow token permissions read-only unless a future job has a documented
need for more access.

## Routes and custom domains

Astro writes real HTML routes and a top-level `404.html`. Do not add an SPA fallback
such as `/* /index.html 200`; it would hide genuine missing pages and break the custom
404 behavior.

When adding a custom domain:

1. Add and verify `hidinginmyroom.com` in the Cloudflare Pages dashboard.
2. Confirm the production `SITE_URL` is `https://hidinginmyroom.com` or leave it
   unset to use the configured default.
3. Trigger a new production deployment.
4. Check the homepage, several wiki pages, navigation, search, canonical metadata,
   sitemap, and a deliberately missing URL.

## Troubleshooting

### The build fails during a content check

Run `npm run build` locally and fix the first reported release-hygiene, missing-asset,
or Astro error. Cloudflare builds consume only publication-safe files committed to
this repository; do not work around a failure by removing a check or copying a raw
private evidence bundle into the public tree.

### The Node.js version differs

Confirm that `.node-version` is committed and that the Pages project is using the
current build image. If the dashboard overrides `NODE_VERSION`, set it to `22.16.0`
or remove the conflicting override.

### Canonical URLs or the sitemap are missing

Confirm that `astro.config.mjs` still defaults to `https://hidinginmyroom.com`, or set
the production `SITE_URL` to that origin and redeploy. Do not set it to an individual
branch-preview URL.

### A pull request has no Cloudflare preview

Check whether the branch belongs to a fork. GitHub Actions still validates fork pull
requests, but Cloudflare's Git integration does not attach the standard preview link
for them. A maintainer can review the CI result and, if appropriate, test the branch
without uploading private evidence.

## Official references

- [Cloudflare Pages: deploy an Astro site](https://developers.cloudflare.com/pages/framework-guides/deploy-an-astro-site/)
- [Cloudflare Pages: GitHub integration](https://developers.cloudflare.com/pages/configuration/git-integration/github-integration/)
- [Cloudflare Pages: preview deployments](https://developers.cloudflare.com/pages/configuration/preview-deployments/)
- [Cloudflare Pages: build image and language versions](https://developers.cloudflare.com/pages/configuration/build-image/)
- [Astro: Cloudflare integration](https://docs.astro.build/en/guides/integrations-guide/cloudflare/)
