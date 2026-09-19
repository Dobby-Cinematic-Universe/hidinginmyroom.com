# Private solo-voice attestation administration

Migration `0032` adds a narrow catalog bridge from an exact media interval to one
named entity. It is for a human reviewer who directly listens to the complete
interval and hears exactly one live human speaker. It is not diarization, active-
speaker detection, face or voice recognition, a source-ownership shortcut, or a
publication workflow.

The versioned contract is
[`private-solo-voice-attestation-manifest.schema.json`](../schemas/private-solo-voice-attestation-manifest.schema.json).
The tracked [example](../examples/private-solo-voice-attestation-manifest.example.json)
is entirely fictional and will not resolve in a real catalog.

## Authority boundaries

An `assert` decision is admitted only when all of these are true:

- the entity is a reviewed person or community figure;
- the source, recording, source-to-recording link, rendition, exact media object,
  media-to-source link, SHA-256, known duration, and half-open
  `[start_ms,end_ms)` interval resolve together;
- the source, recording, mapping, and rendition are currently reviewed, the media is
  verified, and the coordinate system is exactly `rendition_media_ms`;
- one governed, currently active human independently clears private named-voice use
  after reviewing its personal-data and biometric implications;
- a different governed, currently active human directly listens to the complete
  interval and signs the exact direct-audio attestation;
- the interval contains exactly one live human voice and no overlap, playback, TTS,
  synthetic voice, or unknown-origin audio; and
- source metadata, channel context, transcript text, machine identity output, and
  machine confidence are all explicitly excluded as identity evidence.

The catalog anchor confirms which bytes were heard. It does not identify the voice.
The human direct-audio decision supplies the identity attribution. The manifest has
no model, cluster, score, probability, face-track, transcript-text, or public-label
field; unknown fields fail closed.

This lane deliberately forbids biometric artifacts and machine identity outputs.
Those materials belong to the separately governed identity-cluster workflow. The
privacy review here still treats a named voice as personal/biometric-risk-bearing
metadata and clears only private storage. It never approves a public export.

## Append-only model

- `solo_voice_subjects` freezes entity, source, recording, rendition, media ID,
  media SHA-256, and exact rendition-media bounds.
- `solo_voice_privacy_reviews` is a chronological `clear_private_use`/`withhold`
  stream with an independent human review record.
- `solo_voice_attestation_decisions` is a chronological
  `assert`/`withdraw`/`reject`/`dispute` stream with its matching review record.
- `current_private_solo_voice_assignments` exposes only current assertions whose
  current privacy review is clear, whose two reviewers remain active and distinct,
  and whose catalog anchor remains reviewed and exact.

Whole-database validation independently rechecks every cited entity/source/recording/
rendition review state, the unmerged recording state, reviewed source mapping, exact
media digest and duration, and media-source link. A later anchor downgrade suppresses
the current view and fails validation rather than leaving a silently stale identity.

Rows and their cited generic review decisions cannot be updated or deleted. A
correction appends a later decision. To change the named entity or the time interval,
withdraw or dispute the old subject first, create a new immutable subject, obtain a
new independent privacy clearance, and append the new direct-audio assertion.
Overlapping current assertions for different entities on the same media bytes are
rejected. The conflict key is the exact `media_id` and overlapping media-local
interval, so a second rendition row cannot carry a contradictory identity.

Applying the identical manifest again is an exact no-op. Reusing a manifest ID,
input hash, subject/operation ID, review-decision ID, or natural entity/media interval
with different content fails. Each apply is one transaction; an error rolls back the
manifest receipt, subjects, privacy reviews, speaker decisions, and generic reviews.

## Commands

First stop catalog writers, checkpoint and close the catalog, make an exact backup,
and rehearse migration `0032` on a disposable copy. Migration is a separate
administrative action; the attestation command will not install pending migrations.

Validation and default import use a sidecar-free immutable audit connection and write
nothing:

```sh
PYTHONPATH=corpus/src python -m himr_corpus \
  validate-solo-voice-attestation-manifest \
  --db /absolute/private/corpus.sqlite3 \
  --manifest /absolute/private/solo-voice-manifest.json

PYTHONPATH=corpus/src python -m himr_corpus \
  import-solo-voice-attestation-manifest \
  --db /absolute/private/corpus.sqlite3 \
  --manifest /absolute/private/solo-voice-manifest.json
```

Validation returns `input_sha256`. Preserve that exact lowercase digest with the
reviewed manifest. After a human reviews those exact bytes and the validation result,
apply only with both explicit capability arguments:

```sh
PYTHONPATH=corpus/src python -m himr_corpus \
  import-solo-voice-attestation-manifest \
  --db /absolute/private/corpus.sqlite3 \
  --manifest /absolute/private/solo-voice-manifest.json \
  --apply \
  --expected-input-sha256 <exact-input_sha256-from-validation>
```

The importer stable-reads the manifest again and compares its digest before opening
an apply transaction. A missing, malformed, or stale expected digest fails without
catalog writes. The digest is external rather than self-declared inside the manifest,
so changing the path contents after review cannot preserve apply authority.

Close and checkpoint the writer, then run the ordinary immutable catalog validator.
Keep the manifest in owner-private storage. Do not place a real manifest in Git,
`src/`, `public/`, `dist/`, or the static corpus tree.

## Publication and router separation

The three subject/decision tables fix `visibility=private` and
`publication_authority=none`; the fourth table is only the immutable manifest-import
receipt. Migration preflight rejects any earlier publication or gate row using the
reserved object types, and database triggers reject every later publication/gate
state for them, including restrictive rows. Neither recording release exporter nor
graph exporter reads these tables.

The speaker-activity router continues to emit anonymous `unknown_single` processing
labels and may carry its narrow source-wide Daniel presumption only as private plan
metadata. A router result is not a catalog attestation. Persisting any named interval
through this bridge requires the separate direct listening and privacy decisions
above. Conversely, this private bridge creates no speaking-face or active-speaker
claim and does not alter a transcript.
