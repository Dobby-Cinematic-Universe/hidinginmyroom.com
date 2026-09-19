# Active cloud title-policy release

The 2026-09-13 title-policy handover uses a separate versioned runtime at:

`research/private-transcriptions/cloud-archive-20260913/title-override-v1/runtime`

Its `pipeline` package supplies the changed cloud controllers and falls back to
the repository's unchanged, pinned shared modules. Do not edit this live bundle
or its shared dependencies while workers run. Original cloud and summary plans,
workspace markers, reservations, receipts, and transcript artifacts were not
rebuilt or resealed. Original production module bytes were retained unchanged.

The explicit execution release is
`title-override-v1/execution-release-v2/release.json`, relative to the campaign
root above. Its SHA-256 is
`1547656c1f37a210307316fff0cb270b6ac8c9ac58b4334666ff6315cbc5e2f4`.
The earlier prepared release remains as history; it is not the launch target.

Cloud and screening workers must run from that runtime directory, pass the release path and
`--execution-release-sha256`, and pass
`--env-file /srv/himr/.env` for paid stages. The staged default
dotenv path is not the repository's dotenv path.

## Independent workers

The active handover uses separate user-systemd units:

- `himr-cloud-screen-20260913-title-v1.service`
- `himr-cloud-transcription-20260913-refined-v1.service` (the
  [safe upload/GET retry runner](CLOUD_UPLOAD_RECOVERY.md), replacing the earlier
  `himr-cloud-transcription-20260913-title-v1.service`)
- `himr-cloud-gemini-20260913-hundred-v1.service` (the separate
  [100-slot adaptive Gemini release](GEMINI_HUNDRED_SCHEDULING.md), queued after
  the cooperative stop of `himr-cloud-gemini-20260913-refined-v1.service`)

An error in one does not terminate the others. There is no automatic service
restart or forced SIGKILL. Each starts behind its own workspace lock; in-flight
work keeps its original receipts. These are transient units, not boot-enabled
services. Cloud concurrency remains four; the successor Gemini target starts
at sixteen and grows to at most 100, subject to token and budget headroom.
The independent full-archive multimodal screen has completed.

The current [diarization refinement release](DIARIZATION_REFINEMENT.md) adds
targeted follow-up and single-label normalization. The title-only runtime and
release described above are retained history, not the current launch target.

The combined cloud ceiling remains $150. The Gemini worker retains its original
$119.040085 ceiling, with separately retained historical reservations bringing
the overall authorized Gemini ceiling to $150. These are ceilings, not spent
amounts. Do not launch the old parallel supervisor against these same roots.

## Routing and speed

An explicit conversational title enables diarization for a future unsubmitted
cloud recording even when its sampled screen is negative. The original screen
state remains negative. Existing paid requests keep their original setting.
Titles and text leads never automatically rebuy a usable current transcript.

Queue discovery defers unpaid screen replay until the actual paid gate. It
reports deferred decisions explicitly and does not claim verified diarization
booleans for those previews. Paid evidence is fully inspected; the first paid
reservation still requires full screen validation. Summary export ignores
incomplete cloud jobs and fully validates each completed transcript it admits.
The screening producer does not repeatedly replay previously generated screens.

Gemini can summarize eligible current third-party transcripts independently of
screening. Two or more anonymous labels remain held for identity resolution;
exactly one is normalized to the same unlabeled representation as non-diarized
input by the subsequent refinement release.
Historical local ASR stays excluded. Segment timestamps remain in transcripts
and internal evidence; Gemini receives stripped text, not timestamp metadata.

Focused handover validation passed 73 tests, with a further existing-runtime
regression pass performed during the speed changes. No full media verification
or archive recopy was performed.

See [the third-party-first pipeline](THIRD_PARTY_FIRST_CLOUD_PIPELINE.md) and
[text-only conversation leads](TRANSCRIPT_CONVERSATION_LEADS.md) for policy and
the separate lead-search command.
