# September 17 livestream recovery

Recording: `Sr9Vv5pVG1g`, public ID `rec_60945e1342080afd3278598651a47620`.

The original acquisition/screen/transcription job finished AssemblyAI transcription,
but the summary importer requires a `cloudjob_` ID and rejected its `newarchive_` ID.
Do not retranscribe or restart that historical job to recover summaries.

`pipeline.new_archive_recovery` preserves the original transcript, provider job,
payment receipt, and timestamps. Its separate canonical transcript copy maps only
the internal job ID and completion reference, with an explicit identity-adapter
receipt. The normal strict source validator replays the retained provider output.
Four chunks and one transcript reduction ran through standard Gemini, using the
existing metadata-free input policy and internal evidence links. Responses are
persisted before normalization; uncertain requests are never automatically resent.

`pipeline.new_archive_integrate` prepares `release-20260918-v10` from v9, validates
the new detail, verifies catalog hashes, and reuses immutable historical detail
shards without a full-archive transcript verification. It retains all 139 broader
summaries unchanged; those syntheses have not been regenerated for this new video.
The snapshot contains 4,064 recordings, 3,991 transcript-bearing recordings and
2,691 transcript summaries. Its local search and entity/event groups are rebuilt.

Cloudflare received seven additional documents: one summary, one metadata entry,
and five transcript parts. Upload acceptance is not indexing completion. Production
chat remains disabled and no website deployment was performed.

Private recovery artifacts are under the original job's `summary-recovery-v1`.
Public reader output strips the private evidence paths; originals remain internal.

## Broader-summary continuation

`pipeline.new_archive_synthesis` subsequently refreshed September 2026, yearly
2026, and the archive overview. The other 136 scopes were retained. Six standard
Claude requests used strict request-local evidence schemas; all three final scopes
include the new transcript in their source membership and output links. The
conservative maximum reservation was $1.83403, within the reduced remaining
shared Anthropic allocation. This is a reservation, not the billed amount.

`scripts/finish-new-archive-release.mjs` integrates only after all 139 scopes are
complete. It refreshes the Cloudflare export, deletes the three superseded
synthesis objects (retaining local backups), refreshes event grouping, restarts
the local preview, and builds/audits `candidate-20260918-v9`. Neither staging nor
candidate preparation enables production chat or deploys the site. Cloudflare
indexing completion must still be checked separately from upload acceptance.

Explicit broader-reader arguments now take precedence over historical defaults;
default refreshes prefer this completed continuation so they cannot silently
roll the three summaries back to the September 17 versions.
