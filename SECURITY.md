# Security and private reports

Do not disclose a credential, private research material, personal information, or a
sensitive removal request in a public issue, discussion, commit, or pull request.
Git history and notification emails can preserve material after a public edit is
deleted.

## Reporting privately

Use GitHub's **Report a vulnerability** option on this repository's Security tab for:

- exposed credentials or deployment secrets;
- a path that unintentionally publishes a private file or source artifact;
- a vulnerability in the site or its build/deployment configuration; or
- a privacy, safety, or removal report that cannot be described without repeating
  sensitive information.

If private vulnerability reporting is not available, contact an organization owner
privately and initially disclose only that you need a secure reporting channel. Do
not send raw evidence, identity documents, private conversations, or intimate media
until a maintainer confirms an appropriate channel.

Include the affected URL or repository path, a concise description of the risk, and
the least sensitive reproduction information needed. Redact tokens, personal data,
and unrelated account details. Do not test against accounts, systems, or data you do
not control.

Maintainers should enable GitHub private vulnerability reporting before public
launch and keep deployment credentials out of the repository and Cloudflare build
logs. A static production build does not require Discord, GitHub, or Cloudflare API
tokens.

Ordinary site bugs and fully public, source-based wiki corrections belong in the
repository's issue forms. See [`CONTRIBUTING.md`](./CONTRIBUTING.md) for the boundary
between a public correction and a private report.
