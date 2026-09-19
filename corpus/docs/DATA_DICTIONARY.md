# Corpus data dictionary

All time intervals use integer milliseconds and half-open `[start_ms, end_ms)`
semantics. Stable platform objects use deterministic UUIDv5-derived identifiers;
exact acquired bytes use SHA-256 identity.

## Identity layers

- `sources`: platform-visible listings, archive files, posts, or discovery manifests.
- `recordings`: conceptual continuous recordings, independent of mirrors or files.
- `import_batches` identifies exact importer input bytes; `import_observations` records
  distinct observation times for those bytes. `source_metadata_observations` and
  `recording_metadata_observations` are immutable winner inputs, while the source and
  recording rows are deterministic current projections under the documented metadata
  precedence policy.
- `source_relation_observations` and `external_id_observations` retain convergent
  provenance assertions and prevent `INSERT OR IGNORE` order from choosing the visible
  basis label.
- `media_objects`: exact locally available bytes. `first_cataloged_at` is when those
  bytes were first admitted to this catalog; it is not assumed to be their download
  time. Provider-declared hashes remain in `source_hashes` until bytes are available
  and SHA-256 is computed locally. Actual retrieval time and tool provenance belong
  in `media_sources.retrieved_at`.
- `renditions` and `timeline_map_spans`: transformations and piecewise mappings from
  media time into recording time.

## Processing and transcript layers

- `processing_runs`, `run_inputs`, and `artifacts` preserve reproducibility.
- `jobs` is keyed by logical stage and target; `job_attempts` connects deterministic
  attempts to completed processing runs. Producer work-order job labels remain external
  processing-run IDs rather than replacing the catalog job identity.
- `transcript_revisions` are immutable raw-ASR, contextual-ASR, human-verbatim, or
  readability revisions. `transcript_revision_parents` records derivation.
- `transcript_segments` and `transcript_words` retain timing and calibrated scores.
- The whisper.cpp result importer preserves producer IDs and raw metadata after
  recomputing their derivation and comparing normalized rows with both JSON artifacts.
  Zero-length word/token spans are allowed; machine revisions have no fabricated
  speaker, normalization, alignment, confidence band, or calibrated probability.
  With no explicit catalog context, only run provenance and private artifacts enter
  the catalog—no recording or transcript is inferred.
  Derived local-window context remains fail-closed in the ordinary importer because
  producer timestamps are artifact-local but transcript columns are recording-scoped.
  Migration 0020 supplies a separate private `rendition_local_*` lane instead. It
  preserves exact local segment/word times, an explicitly uncalibrated arithmetic
  source-time view, FTS search, and unresolved transform hypotheses without writing a
  recording timestamp. Null context remains available for importing only private
  run/artifact provenance.
- `media_local_*` tables retain private ASR text whose only asserted coordinate is
  `media_ms` on one exact normalized input artifact. Migration 0021 admits these rows
  only when producer catalog context is null and no recording/rendition transform
  exists. Private search returns null source and recording coordinates. See
  [the media-local ASR guide](MEDIA_LOCAL_ASR.md).
- `media_local_transcript_projection_batches`,
  `media_local_transcript_projections`, and
  `media_local_transcript_projection_pairs` bind migration 0029's transcript-text-free
  reviewed plan digest to exact full-file identity copies in ordinary recording
  coordinates. They require one exact normalized-audio derivation, a pinned parent
  rendition, a directly linked acquired-file source, and an immutable rank-700
  acquisition observation proving that source public at review time, no ASR overrun,
  exact normalized-parent duration equality, at most 5 ms normalized-recording
  disagreement, complete normalized-artifact/preprocess and raw/contextual
  receipt/pair/diff provenance, and exact segment/word equality. A governed human
  reviews only the pinned identity mapping. The v1 lane requires the direct
  `media_sources.source_snapshot_id` to be null and seals the preprocess producer's
  complete run-input/artifact sets. The approved plan also binds exact generated
  run/revision/projection payloads and a transcript-text-free digest of every
  deterministic target segment/word ID;
  the target revision's `rendition_id` remains null because the normalized ASR media
  had no rendition at admission, while `parent_rendition_id` stays in the immutable
  receipt. The batch embeds and re-hashes the exact reviewed manifest bytes; mutable
  current availability and recording metadata do not rewrite historical evidence.
  Projection selects no preferred hypothesis and has no identity, wording-review,
  gate, or publication authority. See
  [the full-file projection guide](MEDIA_LOCAL_TRANSCRIPT_PROJECTION.md).
- `source_recording_transform_candidates` contains immutable, competing timing
  hypotheses with dedicated human review tasks. Its rows fix timeline application
  and relationship assertion false; neither reviews nor candidates alter
  `recording_sources` or `timeline_map_spans`.
- `observations` provides common time/provenance fields; typed tables store speaker,
  face, active-speaker, OCR, sound, and action details.
- Preprocessing scene-change and silence outputs are private routing-candidate
  observations only when an existing rendition supplies recording context. Their time
  fields remain in rendition-media coordinates and their metadata says so explicitly.
- `sparse_frame_routing_candidate` observations bind sealed sparse PNG artifacts to
  exact decoded PTS/time-base evidence on an existing proxy rendition. They remain
  private machine routes with OCR `not_evaluated` and text presence `unknown`; they
  create no `ocr_observations`, face/identity row, score, or publication decision.
- `private_ocr_import_receipts`, `private_ocr_frame_admissions`, and
  `private_ocr_word_admissions` bind a completed Tesseract result to exact admitted
  sparse frames and source/proxy renditions. Word observations and their private FTS
  index retain raw machine text, source-pixel geometry, and raw 0–100 Tesseract
  scores. Frame time is local to the bound proxy rendition; `source_*` is provenance,
  not a source-time or recording-time transform. Calibration is null, redaction is
  pending, review is required, and the lane has no identity, event, claim,
  publication, gate, or export authority.
- `calibration_sets` and `observation_scores` keep task-specific raw and calibrated
  confidence separate. There is no overall confidence scalar.
- `fingerprints` stores a media-level raw-fingerprint identity; contextual imports
  add private `audio_fingerprint_observations` that bind the exact fingerprint,
  private raw artifact, half-open window kind, algorithm/format, and uncalibrated word
  count. A null-context result retains only run/input/artifact provenance.
- `audio_fingerprint_match_candidates` restricts generic `match_candidates` rows made
  by the historical v1 conservative exact-raw helper. Its recipe-qualified
  implementation equality semantics remain unchanged.
- `audio_fingerprint_match_candidates_v2` is a separate append-only cross-recording
  lane. It binds both extraction run/result identities and fingerprint IDs, requires
  private uncalibrated human review, fixes relationship assertion false and
  publication authority to none, and never creates `recording_relations`.
- `audio_fingerprint_compare_v2_receipts` binds each v2 subtype to exactly one sealed
  comparison-result hash/path/byte count and its exact-comparison import receipt.
  `audio_fingerprint_compare_v2_sides` supplies exactly one typed `query` and
  `candidate` binding for producer run JSON, engine/build identity, extraction receipt,
  normalized input, catalog context, selected fingerprint, and raw artifact. The
  companion rows are append-only staging evidence and database validation rejects any
  orphan that was not transactionally completed by a v2 subtype.
- `audio_fingerprint_result_imports` is the append-only extraction/comparison result
  ledger. These fingerprint tables are private and are not read by public views.
- `visual_fingerprint_observations` binds a private generic fingerprint and sealed
  32×32 grayscale artifact to the exact requested window, decoded PTS/time base,
  relative timestamp, drift, raw 64-bit pHash, and quality flags. Its calibration
  state is fixed to `not_calibrated`; human review is mandatory and identity,
  duplicate, and relationship assertions are fixed false.
- `visual_fingerprint_result_imports` is the append-only extraction-result receipt.
  Neither visual table is read by a public view. Null-context results retain only
  run/input/artifact provenance and do not create generic fingerprints or observations.
- `visual_fingerprint_compare_imports` binds one sealed comparison envelope to its
  completed comparison run, recipe/result identities, implementation hash, both
  admitted extraction-result hashes, explicit two-sided catalog context, and private
  no-publication policy. `visual_fingerprint_compare_sides` and
  `visual_fingerprint_compare_side_frames` retain the ordered query/candidate
  extraction runs, selected pHashes, exact gray hashes, and rendition coordinates.
- `visual_fingerprint_comparisons` retains the complete raw minimum-Hamming summary,
  configured threshold, calibration state, and six fixed-false identity/duplicate/
  parent/ownership/relationship/unrelated assertions.
  `visual_fingerprint_compare_top_pairs` stores the deterministically ranked bounded
  pair list, while `visual_fingerprint_compare_completion_receipts` seals the complete
  imported graph. A threshold pass creates one private generic candidate and direct-
  media review task. A below-threshold result creates neither: it remains an
  uncalibrated measurement and is never represented as `rejected` or `unrelated`.
  All comparison tables are absent from public views, and publication decisions or
  gate decisions for their raw objects are prohibited.
- `archive_bracket_reconciliation_imports` records the sealed Archive.org snapshot
  hash and deterministic reconciliation-plan hash. Its immutable
  `archive_bracket_youtube_candidates` subtype wraps only uncalibrated generic match
  candidates that require human review and fix relationship/merge assertions false.
  `archive_bracket_reconciliation_issues` stores disagreeing filename/title suffixes
  without creating a match. These tables never alter source/recording mappings and
  are not read by a public view.
- `torrent_bracket_reconciliation_imports` binds an exact torrent SHA-256, bencoded
  info-hash SHA-1, strict discovery SHA-256, original torrent import receipt, fixed
  four-directory scope, deterministic plan hash, and zero-mutation statistics.
  `torrent_bracket_youtube_candidates` preserves the file source/index, replacement-
  decoded importer path, exact Base64 raw path components and length-framed path hash,
  byte count, terminal bracket ID, and exact native-source lookup state. It wraps
  only an unscored private generic candidate, requires human review, fixes
  relationship/merge assertions false and publication authority to none, rejects
  direct publication decisions, and is not read by any public view.
- The separate torrent suffix/audio plan is deliberately absent from the database
  dictionary: it has no migration, importer, receipt, generic match candidate, or
  review-task table. Its private JSON contract keeps format-labelled video and
  audio-only filename locators in distinct evidence/confidence/routing profiles for
  possible later adjudication.

## Knowledge and review layers

- `identity_clusters` is the stable pseudonymous container. Its immutable
  `identity_cluster_versions` form a gapless parent chain and bind each snapshot to
  a completed `processing_run`, a `model`, and canonical hashes of both manifests.
  Once cited, those model/run rows cannot be changed or deleted.
- `identity_cluster_memberships` is the private, append-only membership snapshot for
  a particular version. Face clusters accept only face-track observations, voice
  clusters only speaker-turn observations, and audiovisual clusters only
  active-speaker observations. A calibrated probability is not accepted without an
  explicit `calibration_set_id`.
- `identity_cannot_link_decisions` is an append-only, symmetric observation-pair
  decision stream. Current `cannot_link` and `dispute` decisions prevent two tracks
  from entering the same version; a `clear` must be a genuinely later human decision
  backed by direct-media review. Historical versions and decisions remain auditable.
- `biometric_artifacts` and `identity_cluster_version_artifacts` register raw
  embeddings, centroids, enrollment samples, and indexes. The referenced artifact
  must already come from a completed processing run, be private and non-web, and sit
  outside public build paths; its declared modality must match the cluster it is
  linked to. Both the wrapper and cited artifact are immutable. These files belong
  only in ignored private storage (normally `research/`), never Git, `src/`,
  `public/`, `dist/`, or static release JSON.
- `identity_cluster_version_review_decisions`, `identity_assertion_subjects`, and
  `identity_assertion_decisions` separate machine clusters from a named identity.
  Cluster acceptance and identity assertion require an active human review record
  with the appropriate audio/video directly perceived. The mutable, unversioned
  `identity_assertions` table from migration 0004 is legacy-only and is not a public
  identity authority.
- `public_identity_assertions` is a deliberately narrow projection (assertion ID,
  entity ID, modality, and decision time). It requires the accepted current cluster
  version, a current human assertion, a separately public entity, and publish plus
  rights/privacy/sensitivity clearance for the assertion. Raw cluster IDs,
  membership, scores, observations, review notes, and biometric artifacts are never
  projected. The current static exporter does not export identity assertions at all.
  The publication-manifest administrator also does not yet accept this object type;
  enabling either path requires a separate reviewed schema and test change.
- `solo_voice_manifest_imports`, `solo_voice_subjects`,
  `solo_voice_privacy_reviews`, and `solo_voice_attestation_decisions` provide the
  non-biometric private solo-voice bridge from ADR 0012. A subject freezes one named
  entity plus exact source, recording, rendition, verified media SHA-256, and
  half-open `rendition_media_ms` interval. A current assertion requires one human to
  hear the complete interval as exactly one live voice with no overlap, playback,
  TTS, synthetic voice, or unknown origin, and a different human to clear private
  personal-data/biometric risk. Source/channel context, transcripts, model outputs,
  and confidence values are excluded as identity evidence. All streams are
  append-only; `current_private_solo_voice_assignments` is private and no exporter
  reads it. Identity conflicts use the shared `media_id` plus media-local bounds,
  not merely `rendition_id`; applying a manifest is capability-bound to the exact
  stable-read input SHA-256 returned during validation.
- `entities`, aliases, appearances, `events`, dates, participants, relations, and
  evidence support private timelines and maps. The entity/event manifest administrator
  forces private visibility and creates review tasks. Every appearance/event-evidence
  edge also receives a candidate `claim_catalog_links` anchor containing its current
  source, recording, rendition, optional reviewed transcript, and optional half-open
  interval. Timed edges receive a private observation. `evidence_basis_kind` remains
  in the anchor/observation provenance so a directly recorded unverified claim cannot
  be confused with a directly observed occurrence.
- `event_participant_publication_subjects` gives the historical composite participant
  row an append-only stable publication/review target. `human_public_graph_objects`
  accepts only graph roots and edges with a current human publish decision, complete
  matching review, nonempty public label, and three current human clear decisions,
  each backed by its own complete matching review. Appearance and event-evidence
  publication reviews must record direct audio or video perception.
- `reviewed_public_graph_catalog_anchors` resolves one exact reviewed claim link to a
  reviewed public source, reviewed public recording and source mapping, reviewed
  rendition, and verified media object. Timed edges require a known media duration
  and remain within it. Its public consumers expose the rendition ID and the fixed
  `rendition_media_ms` basis, never a local path, private artifact, observation,
  transcript wording, or provenance note.
- `public_graph_entities`, `public_graph_events`, and the five edge views are the only
  graph-export inputs. Every edge joins independently eligible public roots. Graph v1
  excludes transcript-backed event evidence, and its executable validators require
  real calendar dates, resolved references, at least one participant and evidence edge
  per event, referenced entities only, and an acyclic public relation graph.
- `review_tasks` and `review_decisions` are append-only human-work records.
- `reviewer_admin_manifest_imports` is the append-only receipt for a strict reviewer
  administration manifest. It stores the canonical input digest, operator identity,
  basis, actual import time, and declared registration/state/adoption counts, but not
  the private manifest path or body. Migration `0027` uses one reserved receipt to
  adopt every reviewer row that existed before governed administration.
- `reviewer_admin_events` is the immutable per-reviewer authority stream. A
  `register` event precedes insertion of one inactive reviewer, `set_active` records
  and applies one exact state transition, and migration-only `legacy_adopt` records
  the state observed when `0027` runs. Identity snapshots, contiguous manifest
  ordinals, strict effective-time ordering, and previous/new state chaining are
  validated independently of connection-local SQLite pragmas.
- `current_reviewer_admin_events` selects the highest database-assigned event
  sequence for each reviewer. Its identity and `new_active` snapshot must match the
  current `reviewers` row; publication admission also requires an active interval at
  the claimed decision time.
- `public_metadata_policy_publish_scope` is the complete database capability of the
  built-in metadata policy reviewer. It exposes only allowlisted, metadata-only
  Archive.org/official-channel source and recording IDs together with the exact
  required public label and basis. The importer, publication-manifest dry-run, and
  database insertion guard consume this same view; it grants no transcript, entity,
  event, or publication-gate authority.
- `machine_transcript_policy_human_gate_scope` exposes only current transcript gate
  clears made by a human reviewer during a recorded active interval.
  `machine_transcript_policy_publish_scope` intersects all three gate kinds with the
  complete v1 initial-publication capability: ordinary-coordinate raw/contextual
  machine revisions with at least one segment, no named segment speaker, no lifecycle
  history, and no prior publication row. It contains metadata, counts, gate IDs, and
  pinned policy literals, never transcript wording.
- `machine_transcript_publication_policy_runs` is the append-only receipt for one
  digest-authorized application of that scope. It binds canonical text-free plan JSON,
  its SHA-256, the dedicated reviewer and publication-manifest receipt, exact counts,
  and one application time. Every decision must be a plan member and use that same
  time; revision creation precedes its three gates, and all four precede application.
  Installing migration `0028` creates no run, reviewer, gate, review, lifecycle, or
  publication decision. Once applied, guards seal the planned segment set and word
  children. The migration also adds explicit `INSERT OR REPLACE` conflict guards to
  the append-only transcript, review, correction, and lifecycle evidence tables.
- `transcript_revisions`, their segments, words, parent links, and corrections are
  append-only. `transcript_lifecycle_decisions` records human-only `disputed`,
  `retracted`, and `reinstated` transitions with a constrained reason and public
  explanation. A current retraction projects immediately as a text-free tombstone while
  publication is still `publish`, remains a tombstone after `remove`, and disappears
  only when publication or one of the three gates is withheld. A retracted revision
  can move only to `reinstated`, and transcript `remove` requires the current
  retraction.
- `publication_decisions` is an independent append-only allowlist/removal stream;
  `publication_gate_decisions` separately records rights, privacy, and sensitivity
  clearance/withhold decisions. Manifest-admin records retain both a concise `basis`
  and fuller private `notes` provenance.
- `publication_manifest_imports` is an append-only private ledger of applied manifest
  IDs, canonical digests, versions, counts, and import times; it does not retain file
  paths or manifest bodies. Administrative decision rows link back to their manifest.
- `claim_catalog_links` connects existing atomic wiki claims to exact catalog objects
  and time spans without treating generated transcripts as evidence.

## Legacy quarantine

Legacy `machine_summary` and `speakers` fields are discarded. Word count, displayed
duration, and source checks are retained only under `legacy_audit` metadata on an
unavailable historical catalog source. They do not enter transcript or public views.

## Public static projection

- `manifest.json` is the sole active v2 pointer. Its deterministic `release_id`
  commits to counts, derived facets/statistics, and all catalog-shard descriptors.
- A catalog shard has at most `catalog_shard_size` transcript-free summaries. A
  summary repeats only browse metadata, derived counts/facets, and the path, byte
  count, and SHA-256 of one recording shard.
- A recording shard contains exactly one existing v1-shaped recording projection,
  including only source and transcript objects already admitted by the public views.
- Each transcript revision declares its provenance kind, machine/generated and
  unreviewed booleans, non-quotation status, disclaimer code, current lifecycle state,
  and public lifecycle history. Retracted tombstones contain no segment text.
- Shard filenames include a hash prefix for cache safety, while validation always
  checks the complete SHA-256. Paths are relative, allowlisted, non-symlink paths.
- `release_id`, `recording_id`, `revision_id`, and `segment_id` are stable anchors.
  Array indexes and shard ordinals are never citation identities.
- `graph/manifest.json` is a separate active graph pointer. It commits to one bounded,
  content-addressed entity/event shard without changing recording schemas v1/v2. The
  graph format exposes only reviewed labels, stable IDs/slugs, narrow edge fields,
  dates, and public source/recording/rendition anchors. Its checked initial fixture is
  empty and its release validator rejects unreferenced files, symlinks, hash or byte
  mismatches, path escapes, unknown fields, orphan roots, and cyclic relations.
