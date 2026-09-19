# Incremental Gemini retained-record checks

A [persistent-cache successor](GEMINI_PERSISTENT_CACHE.md) is prepared for a
future restart. It has not replaced or restarted the running 100-slot worker.
The memory-only cache behavior below still describes that running worker.

The current Gemini launch target is
`himr-cloud-gemini-20260913-hundred-v1.service`, using the
[100-slot adaptive release](GEMINI_HUNDRED_SCHEDULING.md). It started after the
refined worker's cooperative stop. The cache release described below
is retained as a predecessor. Gemini handovers do not restart the independent
cloud-transcription lane; no paid plans, receipts or budgets are reset.

Runtime, relative to `research/private-transcriptions/cloud-archive-20260913`:

`summary-cache-v2/runtime`

Execution release:
`summary-cache-v2/execution-release/release.json`, SHA-256
`0d3e80ee582c3e1af33a85f9ec665dcb28cdfb8339ac3a9c9900680b32593a70`.

The earlier cache worker used the unchanged `summaries-v2/manifest.json`, explicit repository
`.env`, eight concurrent Gemini batches, and its original $119.040085 ceiling
within the overall $150 Gemini authorization including historical reservations.
This is a transient, finite user-systemd service, not a boot-enabled service.
Do not launch an older Gemini worker concurrently against the same workspace.

## What no longer repeats

- Existing chunk requests are reused using the sealed plan's exact initial job
  IDs, source content, configuration, request bytes and character coverage.
  Restarting no longer requires rediscovering their chunk boundaries.
- New chunks use an exact incremental byte-size calculation, with the original
  final job builder. Chunk boundaries, IDs, prompts, evidence and costs stay the
  same; it avoids reconstructing the growing request for every subtitle segment.
- Unchanged records reuse a compact, previously validated status/accounting
  snapshot. New, changed, replaced or deleted files invalidate that snapshot.
- Completed exports are reused too, with their exact artifact bytes rehashed.
  A changed record invokes the original exporter; it may alter only its export
  directory, not receipts or other record proofs.

The caches are process-local, bounded and contain serialized copies. Initial
requests have a 256 MiB/4,096-entry limit. Record snapshots and export references
each have a separate 32 MiB/4,096-entry limit. Source and request bindings, the global reservation
ledger, aggregate spending, and actual paid submission/polling checks remain
current. Directory/file witnesses include inode, size, nanosecond modification
and change times, ownership and mode; symlinks are rejected. A record changing
during validation is rejected, not cached.

No full media-file verification, retranscription, paid retry, source replacement,
or anonymous-speaker naming was introduced. The subsequent
[single-label refinement](DIARIZATION_REFINEMENT.md) admits exactly one anonymous
label as unlabeled input; two or more still wait for speaker identification.
Segment timestamps and internal evidence remain stored; Gemini still receives
the existing timestamp-stripped projection.

Cycle logs expose `initial_job_reuse`, including `retained_seed_hits`, `hits`,
`fresh_builds`, `record_hits`, `record_validations`, `export_hits`,
`export_validations`, and bounded cache sizes.

## Focused validation

109 focused and existing lifecycle tests passed. A real 8,235-segment retained
record loaded its plan in 2.4 seconds; the old path had not completed within an
85-second profiling limit. Its repeat unchanged-record status check took
0.0006 seconds after an 18.1-second first validation. A separate synthetic
1,200-segment cold chunk build dropped from 44.6 to 0.13 seconds with byte-identical
jobs. These are local construction/validation timings, not API throughput claims.

The original shared summary modules and the live cloud/screen runtime were not
edited. New helpers are `pipeline/cloud_transcription_summary_cache.py` and
`pipeline/transcript_summary_fast_initial.py`; their exact copies and the scoped
worker integration are pinned in the versioned runtime above.
