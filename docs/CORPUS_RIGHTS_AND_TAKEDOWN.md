# Corpus rights, privacy, and takedown policy

The corpus records publicly discoverable source metadata and may publish sanitized
transcript material for research, commentary, criticism, and source navigation. A
public URL or archive copy does not by itself establish ownership or redistribution
permission.

## Acquisition boundary

- Acquire only public material or material for which the maintainer has a documented
  right of access and processing.
- Do not bypass authentication, paywalls, membership controls, regional restrictions,
  or platform safeguards.
- Keep cookies, headers, tokens, signed attachment URLs, request logs, and private
  storage paths outside public releases.
- Do not automatically collect comments or unrelated account data.
- Record the source, retrieval time, access state, stated license, rights notes, and
  exact acquired-byte SHA-256.

## Publication boundary

Publication eligibility is deny-by-default even though eligible machine transcripts
are release-by-default. Before a transcript or observation is exported, record its
public source, rights basis, privacy gate, sensitivity gate, and publication decision.
Those policy gates do not imply that anyone reviewed the transcript wording.

Do not publish biometric embeddings, enrollment crops, voice samples retained only
for matching, private messages, addresses, contact details, credentials, financial
identifiers, leaked intimate material, or unrelated personal information. OCR and
transcripts must be screened for these categories before publication.

Where publishing a complete transcript is not justified, withhold it or publish only
a separately justified excerpt, exact timestamp, and link to the source. Every public
machine revision and search result must state that it is machine-generated,
unreviewed, may be wrong, and is not a verified quotation. It remains distinct from
human-corrected, media-checked, and disputed revisions.

## Corrections and removal

A public correction or removal request can use the repository's content-correction
issue form. A report that would expose private information must use the private path
described in `SECURITY.md`.

A removal decision must cover every derived location, including:

- public transcript and OCR records;
- static search shards and cached releases;
- thumbnails or representative frames;
- private face and voice clusters;
- model artifacts and review exports; and
- graph, timeline, and wiki backlinks.

Released artifacts are immutable for auditability, but a subsequent release can
withdraw the material and publish a non-sensitive, text-free tombstone explaining the
correction or removal. An explicit transcript dispute, retraction, or reinstatement
requires an active human review, a matching append-only lifecycle decision, and a
public explanation. Automated policy cannot create or reverse that lifecycle record.
The human retraction immediately replaces public transcript text with the tombstone;
the subsequent `remove` publication state is allowed only while that retraction is
current. Use `withhold`, rather than an unexplained `remove`, when policy requires the
entire public record to disappear. Before reinstatement, replace a current `remove`
with `publish` or `withhold`; the human reinstatement then controls whether text can
return.
If a privacy, rights, or sensitivity gate is not clear, even the tombstone explanation
is withheld. Private retention after a valid deletion request requires a separately
documented basis.
