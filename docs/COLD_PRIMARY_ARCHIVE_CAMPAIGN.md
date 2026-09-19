# Cold-primary Archive.org campaign

## Scope and capacity basis

The sealed 2026-08-29 planning snapshot contains all seven known Archive.org
collections and 3,923 Archive.org recording candidates. They are two distinct
execution classes:

- 3,199 normal-processing sources: 431,948,153,891 estimated bytes and 672.71
  hours.
- 724 cold-acquisition-only sources that require a later chunking pass:
  2,298,940,436,678 estimated bytes and 4,141.59 hours.

The 432 GB figure therefore covers only the normal-processing class. It is not the
size of the complete 3,923-file Archive.org corpus. The autonomous primary pass
acquires both classes, about 2.73 TB in total. The 724 long recordings remain a
visible postprocessing backlog until the chunk/reassembly successor exists, so the
primary pass must not be reported as a fully processed corpus.

## Production topology

Use one dedicated cold primary CAS for original media and keep all controls,
receipts, preprocessing products, GPU inputs, and controller state on the hot
filesystem:

```text
/mnt/archive/HIMR/corpus/raw/acquisition-cas/       original media and result.json
/srv/himr/research/corpus/...       sealed plans, epochs, receipts
/srv/himr/research/operator-state/autonomous-archive-all-known-2026-08-29/preprocess-output/  ASR-ready FLAC
```

The media root must be a strict descendant of `/mnt/archive/HIMR`; the mount root
itself and its ancestors are rejected. Acquisition writes directly to that
dedicated CAS, so no retained hot original needs to be deleted or copied. The
preprocess handoff may read exact payloads from that CAS, but its bundle, state,
and processing-output roots remain forbidden on cold storage. Every cold input is
still rebound to its sealed result, byte count, SHA-256, normalized probe, and
immutable preprocess receipt.

For the 3,199 normal-processing sources, 16 kHz mono signed-16 audio is at most
about 72.2 GiB before FLAC compression. This makes cold originals plus hot
ASR-ready audio feasible with the present roughly 160 GiB hot free space, while
the 724 long recordings do not enter the current hot preprocessing path.

## Queue epochs

Do not execute the corpus as one repeatedly replayed queue. Validation
strictly rehashes completed payloads, so repeatedly cycling one growing queue
causes quadratic cold-media reads. Materialize deterministic control epochs from
the sealed full-corpus plans instead. The production schedule set contains:

- 102 normal epochs, each capped at 32 items and 16 GiB estimated;
- 91 cold-acquisition-only epochs, each capped at 8 items and 128 GiB estimated;
- 193 epochs and exactly 3,923 unique files in total.

Normal epochs use:

- `epoch_max_items`: 32
- `epoch_max_estimated_bytes`: 16 GiB
- `maximum_network_concurrency`: 1
- `maximum_preprocess_concurrency`: 1
- shared `media_output_root`: the dedicated cold CAS above
- hot controls and preprocess state: a distinct directory per epoch

A normal epoch is complete only when every acquisition result and corresponding
preprocess receipt validates. A cold-only epoch advances on validated acquisition
results without manufacturing preprocess acknowledgements. A shared cold CAS
retains content-addressed reuse across all epochs, while immutable payload replay
stays bounded to the touched epoch during a continuous run.

The autonomous controller schedules acquisition, preprocessing, GPU supervision,
and the skipped retention adapter as independent lanes. On this host the admitted
heavy overlap is one network acquisition, one four-thread FFmpeg item, and one GPU
child. This is the maximum safe overlap under the sealed sources: all epochs share
one nonblocking CAS writer lock, preprocessing uses process-global guards, and both
CPU lanes read the same rotational USB disk. Increasing worker counts without a
new item-claim and multiwriter contract can turn ordinary contention into durable
failure attempts.

A controller process deep-validates completed acquisition media once at startup.
Same-process metadata witnesses then avoid rehashing unchanged multi-gigabyte
payloads on every producer and handoff scan; any new or metadata-changed result is
sent through the original exact validator. These witnesses are deliberately lost
on restart and are never checkpoint authority.

## Sealed production configuration

The production controller configuration is sealed at:

```text
/srv/himr/research/corpus/autonomous/archive-all-known-2026-08-29/controller-config.json
```

It has config ID `himrautocfg_fa3a67856c384c767bc10c1ce869efe8` and physical
SHA-256 `4a1fe84f9db6dd7d1afeb177245ae9d1457d1765379266740bc4eea154668dad`.
It binds the seven-collection inventory and schedule set
`bgacqscheduleset_cc0a7298263720a9c0aad56524b078a3`; it does not infer work from
directory scans or perform runtime Archive.org discovery.

```json
{
  "archive_candidate_count": 3923,
  "queue_ready_count": 3199,
  "requires_chunking_count": 724,
  "storage_mode": "cold_primary",
  "media_output_root": "/mnt/archive/HIMR/corpus/raw/acquisition-cas",
  "schedule_count": 193,
  "normal_schedule_count": 102,
  "cold_only_schedule_count": 91,
  "control_root": "/srv/himr/research/corpus/autonomous/archive-all-known-2026-08-29/producer-control",
  "processing_output_root": "/srv/himr/research/operator-state/autonomous-archive-all-known-2026-08-29/preprocess-output",
  "cold_free_space_floor_bytes": 549755813888,
  "normal_epoch_max_items": 32,
  "normal_epoch_max_estimated_bytes": 17179869184,
  "cold_epoch_max_items": 8,
  "cold_epoch_max_estimated_bytes": 137438953472,
  "long_recording_mode": "acquire_to_cold_then_park_postprocess"
}
```

`free_space_floor_bytes` in each acquisition work order applies to the filesystem
containing the cold media root. Hot output capacity is bounded here by excluding
the long-recording class from preprocessing and by the normal corpus's approximately
72.2-GiB uncompressed-audio ceiling; operators should still watch the console's
storage monitor if unrelated workloads consume the remaining hot space.

Cold-primary schedules use
`cold_storage_access: sealed_queue_media_output_root_write_only`; their preprocess
handoffs use
`cold_storage_access: read_exact_sealed_acquisition_payloads_only`. A summary that
claims cold storage is forbidden for this topology is invalid.

## Long-recording successor

The 724 long sources use the same cold primary CAS for full parent acquisition,
but they need a successor queue for processing rather than admission to the normal
preprocess queue. The successor must:

1. acquire and hash the complete parent to the cold CAS;
2. define deterministic sample-index chunks against that parent hash;
3. materialize only a bounded hot chunk working set;
4. run GPU ASR and seal chunk results plus reassembly coordinates;
5. release only explicitly declared, reproducible scratch chunks after their
   results are sealed.

It must never delete or replace the cold parent, immutable acquisition result,
chunk manifest, preprocess receipt, or transcript result. Until this successor
contract and its scratch-lifecycle tests exist, `requires_chunking` is a visible
postprocess backlog rather than an acquisition failure or a fully processed item.

## Operational limitation

The current implementation acquires all 3,923 sealed Archive files to cold storage,
preprocesses and dispatches GPU ASR for the 3,199 normal items, and leaves the 724
long items visible as pending successor work. It does not yet implement deterministic
long-recording chunk/reassembly or scratch release. A completed primary pass therefore
ends as `primary_pass_drained_with_postprocess_backlog`, not `campaign_drained`.
