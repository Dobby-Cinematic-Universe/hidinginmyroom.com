# Full-archive fast speaker triage

This is a separate, private screening campaign. It does not restart or modify
acquisition, ASR, transcripts, publication, or the existing CPU/resident/guided
screening workspaces. Sources are neither copied nor deleted.

The September 12 request includes the original completed archive acquisition and
the four newly admitted 699994 records. It accounts for every admission and
deduplicates only exact content identities, retaining all title/source aliases.
The metadata inventory contains 4,488 admissions and 4,063 unique payloads;
two unique payloads have no audio according to acquisition metadata.

## Sampling and limitations

Fast triage uses 10-second samples, a five-minute target interval, and a maximum
of 64 baseline samples per recording. The cap spreads samples across very long
recordings. Title information affects priority, not acoustic speaker labels.
No unreviewed transcript text or timestamp cues are admitted as evidence.

This archive-specific recipe excludes the opening 100 ms from sampling, after
checking that the first sample starting there decodes exactly. This avoids an
Opus decoder-priming gap at zero without padding audio or changing existing
screening recipes. All timestamps remain absolute source timestamps, and the
reported source duration remains the full measured audio EOF. The omitted
interval is explicit in each source admission, sampling plan and summary.

This is not full diarization or proof that a recording contains only one speaker.
The negative label means only that no second voice was detected in sampled audio.
Unsupported timing, decode failures, and insufficient evidence remain explicit.

Preparation checks acquisition result metadata and bounded decoded audio samples.
It does not hash or verify whole media files. The recorded acquisition content
identity relies on the immutable archive CAS and source metadata witnesses.
Preparation receipts retain timeline decisions and rejected-input reasons.

## Background job

Started September 12, 2026 at 17:56 EDT. The prelaunch native GPU check completed
65/65 probes across a short and a nearly 12-hour Opus recording, with no errors,
matching checkpoint replay and unchanged source witnesses. Screening took
22.8 seconds; the complete service peaked at 2.3 GiB host memory with zero swap.
That small check establishes execution readiness, not archive-wide accuracy or
a firm completion estimate. Its retained report is
`research/corpus/speaker-screen-campaigns/archive-canary-20260912/CANARY_REPORT.json`.

The user service is `himr-speaker-screen-fast-20260912.service`. It prepares the
complete inventory, seals runnable batches, then automatically runs the GPU
campaign. Preparation uses two bounded decoder processes. GPU batches use the
existing isolated CUDA environment, one model thread, four probes per model
batch, and two decoders. The service limits its complete process tree to 4 GiB
host memory and no swap. The model allocator is capped at half of GPU memory;
this is not a hard cap on all graphics-driver allocations.

The exact private request and state are:

```text
research/corpus/speaker-screen-campaigns/archive-fast-20260912.request.json
research/corpus/speaker-screen-campaigns/archive-fast-20260912/
```

Useful status commands:

```sh
systemctl --user status himr-speaker-screen-fast-20260912.service --no-pager
journalctl --user -u himr-speaker-screen-fast-20260912.service -n 30 --no-pager
```

The state directory contains `preparation-status.json` during input preparation,
`preparation-result.json` once plans are ready, `excluded-inputs.json` for files
needing review or containing no audio, and `campaign/status.json` during GPU work.
Campaign totals cover runnable unique recordings, not excluded files or aliases.
Active-batch receipt counts are explicitly unverified progress telemetry; committed
pass summaries provide verified counts. A completed runnable campaign is not a
claim that excluded files were screened.

To stop this screen without touching acquisition or ASR:

```sh
systemctl --user stop himr-speaker-screen-fast-20260912.service
```

Completed endpoint receipts and atomic probe checkpoints are retained. Resume
must use the same request, source implementation, tools, models and runtime;
changed inputs require a new plan. There is no automatic restart loop. Storage
I/O errors stop the campaign. Individual failed batches remain incomplete and
are reported while subsequent independent batches may proceed. Preparation has
an eight-hour bound; the campaign has at most eight passes per batch and a
persistent seven-day wall-clock deadline.

Once the prior unit is stopped and no longer loaded, the same launch command
resumes preserved preparation receipts and screening checkpoints. Do not run a
second differently named unit against this workspace:

```sh
systemd-run --user --unit=himr-speaker-screen-fast-20260912 \
  --property=MemoryMax=4G --property=MemorySwapMax=0 \
  --property=OOMPolicy=stop --property=KillMode=control-group \
  --property=TimeoutStopSec=60 --property=RuntimeMaxSec=604800 \
  --property=Restart=no --property=UMask=0077 --property=Nice=5 \
  --property=WorkingDirectory=/srv/himr \
  -- /srv/himr/research/corpus/speaker-screen-accelerated-runtime-20260912/venv/bin/python \
  -B /srv/himr/pipeline/speaker_screen_archive_prepare.py launch \
  --request /srv/himr/research/corpus/speaker-screen-campaigns/archive-fast-20260912.request.json \
  --expected-sha256 71f4efcdb01810fbfafa6235698a9ddfee35f67632af4c938529b824e494a7e1
```
