# Contributing to the HIMR Community Site

Thank you for improving the homepage or wiki. Contributions should make the site
more useful without turning uncertain material into fact or exposing private people.

Repository: [Dobby-Cinematic-Universe/hidinginmyroom.com](https://github.com/Dobby-Cinematic-Universe/hidinginmyroom.com)

> [!IMPORTANT]
> Do not put secrets, private Discord material, leaked or intimate media, home
> addresses, private contact details, access-bearing attachment URLs, or non-public
> identities in an issue, commit, or pull request. Public GitHub content and its
> history may remain accessible after deletion. Use the private-reporting guidance
> in [`SECURITY.md`](./SECURITY.md) when even describing a concern would expose
> sensitive information.

## Choose the right contribution path

### Homepage, navigation, and interface

Use this path for layout, accessibility, styling, animation, navigation, or public
directory-link changes. Common files are:

- `src/pages/index.astro`
- `src/data/links.ts`
- `src/components/`
- `src/styles/`

Homepage directory copy does not require a private claim record or wiki review date.
Links should still be public, relevant, accurately labelled, and safe to visit.

### Wiki corrections and additions

Read [`EDITORIAL_POLICY.md`](./EDITORIAL_POLICY.md) before editing anything under
`src/content/docs/wiki/`. In particular:

1. State only the narrow claim supported by the source.
2. Attribute self-reports and allegations instead of presenting them as facts.
3. Give an exact public URL and a timestamp, post, page, or archive locator.
4. Explain the exact claim and evidence scope in the pull request. Maintainers update
   the corresponding private review record.
5. Use the verification state the evidence actually reached.
6. Include conflicting evidence, response status, and the required warning for a
   publishable allegation.
7. Keep invasive or primarily harassing detail out of the public wiki.

Machine transcripts, search snippets, video titles, AI summaries, and NotebookLM
drafts can locate possible evidence. They cannot verify a claim. A human reviewer
must inspect the underlying source as described in the policy. The policy contains a
rare attributed-self-account exception for a minimized, two-model machine paraphrase;
it remains an unverified allegation and requires an exact source locator, warning,
corroboration limit, response search, and strict sensitivity exclusions. Any raw
review copy belongs in the maintainers' private workspace, not this repository.

Community repost clips can support a narrow observation only after the complete
available copy and surrounding post have been reviewed and hashed. If retaining a
review copy is appropriate, maintainers keep it in private storage. The public record
is the reviewed article and its precise citation. Wording must
preserve the limits of a repost; follow the dedicated rules in the editorial policy
and review process.

### Images

A new wiki image needs all three of the following:

- the image in the appropriate public asset directory;
- source, locator, transformation, and rights information in the adjacent page
  caption; and
- evidence supporting the statement the image is used to illustrate.

Use an image you have the right to contribute, preserve its public source and exact
locator, record the rights or permission basis, and explain any crop or other
transformation. Public availability, a screenshot, or a claim that something is a
derivative work is not enough on its own. Do not contribute leaked, private, sexual,
or doxxing material. See [`RIGHTS.md`](./RIGHTS.md).

### Evidence and private research

This public repository does not accept raw research snapshots, platform exports,
members-only or local review videos, full transcripts, NotebookLM drafts, review
queues, or private claim ledgers. Those materials are maintained separately and are
not pull-request artifacts.

- Give public URLs and exact timestamps, post IDs, page numbers, or other locators.
- Link to a public source instead of uploading a copy whenever possible.
- Keep public provenance records limited to what readers need to inspect the claim.
- Do not include cookies, signed attachment URLs, request headers, account IDs that
  are not editorially necessary, private messages, or unrelated personal data.
- Describe retrieval failures as failures; a block, deleted page, or interstitial
  does not verify the alleged content.
- Let maintainers perform any permitted capture or transcription in the separate
  private workspace.

Never use a Discord user token for collection. Never place any Discord, GitHub, or
Cloudflare token in GitHub or Cloudflare Pages, and never submit message dumps in a
public issue or pull request.

## Development setup

Requirements:

- Git
- Node.js `22.16.0` (also recorded in `.node-version`)
- npm, using the committed `package-lock.json`

Fork the repository on GitHub, then clone your fork and retain the public project as
`upstream`:

```sh
git clone https://github.com/YOUR-USERNAME/hidinginmyroom.com.git
cd hidinginmyroom.com
git remote add upstream https://github.com/Dobby-Cinematic-Universe/hidinginmyroom.com.git
npm ci
npm run dev
```

Useful checks:

```sh
npm run check
npm run build
npm run preview
```

`npm run check` checks the public release boundary, verifies referenced local images,
and runs Astro diagnostics. `npm run build` runs those checks and generates `dist/`;
it is the required pre-PR command and the CI gate. Neither command fetches or reads
private research.

Use `npm install` only when intentionally changing dependencies. Commit the
resulting `package.json` and `package-lock.json` changes together. Do not commit
`node_modules/`, `.astro/`, `dist/`, `.env`, or private research files.

## Git and pull requests

1. Create a short branch from the repository's default branch.
2. Keep unrelated code, content, and provenance changes in separate pull requests.
3. Explain what changed, why, and which public evidence supports wiki edits.
4. Run `npm run build` and resolve every error.
5. For visual changes, check desktop and mobile layouts. Test keyboard use and
   reduced-motion behavior when the change affects interaction or animation.
6. Open a pull request and complete the template. Respond to review without
   rewriting or deleting relevant disagreement from the evidence record.

Do not include drive-by rewrites that make claims broader than their sources. If a
source is disputed or cannot be checked, open a content issue with public locators
and explain the uncertainty instead of silently resolving it.

## Before submitting

- [ ] The change is focused and contains no secrets or private material.
- [ ] Public-facing links work and use clear labels.
- [ ] Wiki claims follow the editorial policy and give precise public citations.
- [ ] New images have adjacent source, rights, and transformation information.
- [ ] No raw snapshot, transcript, private research, or access-controlled media is
      included.
- [ ] `npm run build` succeeds locally.
- [ ] Visual changes were checked at appropriate viewport sizes and interaction
      preferences.

The repository does not currently declare a repository-wide software, content, or
media license. Do not assume that public source or the absence of a license grants
reuse rights. Read [`RIGHTS.md`](./RIGHTS.md) before contributing third-party
material.
