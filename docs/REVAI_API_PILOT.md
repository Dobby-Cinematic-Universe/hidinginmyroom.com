# Rev AI end-to-end pilot — 2026-09-13

The live API pilot passed using the same client bytes as the running cloud
transcription bundle. No production client changes or worker restarts were needed.

The private pilot is at
`research/private-transcriptions/cloud-archive-20260913/revai-pilot-v1`.
It uses the 05:00–07:00 excerpt of the full sister Q&A, converted to the same
16 kHz, mono, 16-bit PCM upload format. The original recording was not modified.
The excerpt is explicitly excluded from archive transcript coverage and Gemini.

## Observed result

- Repository `.env` authentication succeeded via the account endpoint.
- One multipart paid submission succeeded with `transcriber: machine`, English,
  `skip_diarization: false`, and a retained pilot metadata fingerprint.
- Job `SVwE6k2B0xMGrW5f` completed for 120 seconds of audio (3,840,078 upload bytes).
- Transcript retrieval and the existing normalizer succeeded: 22 segments,
  two anonymous speaker labels, segment timestamps through 119.645 seconds.
- Normalized word-timing arrays are absent. Original provider output remains
  private and unchanged. Two labels are not an independently verified accuracy
  score or a speaker-identity assignment.
- Replaying collection reused the saved result without another provider request.

The estimated charge is $0.006667 at Rev AI's published English Reverb rate of
[$0.20/hour](https://www.rev.ai/pricing), not a settled billing receipt. A separate
$0.01 pilot reservation is retained in `reservation.json`. The unchanged cloud
plan's maximum for all listed jobs, its prior reservations, and this pilot
allowance total $67.325445, below the existing $150 aggregate authorization.
Any future additional campaign must include this retained pilot reservation;
the original production plan and ledger were not resealed or reset.

## Large-file coverage and limits

110 offline tests passed across the client, environment reader, audio preparation,
cloud lifecycle, pilot restart guards, and additional large-input preflight cases.
The new boundary cases exercise routing above 10 hours through 17 hours, rejection
beyond 17 hours, multipart framing overhead, and room for the longest metadata.
Seventeen hours of the configured PCM audio is approximately 1.9584 GB before
small headers, within the direct-upload guard.

Rev AI documents a [2 GB multipart request limit and 17-hour duration limit](https://docs.rev.ai/faq).
The client checks the whole multipart length and streams bounded chunks rather
than buffering gigabytes. This pilot did **not** upload a multi-gigabyte file or
test a 17-hour transcription; network interruptions and provider-side long-file
behavior remain real risks. A timed-out paid POST is retained for reconciliation,
never automatically repeated.

## Retained pilot commands

The isolated runner is `pipeline/revai_pilot.py`. From the repository root:

```sh
python3 -B -m pipeline.revai_pilot collect
```

This replays the completed pilot from saved artifacts. `submit --allow-paid-api`
is a no-op when its receipt already exists, and refuses to retry an intent
without a receipt. `prepare` refuses an existing pilot directory. Do not remove
the directory to create another paid attempt; preserve it as billing evidence.

The running cloud worker and its pinned shared modules were unchanged.
