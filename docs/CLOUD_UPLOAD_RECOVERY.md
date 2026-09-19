# Cloud upload recovery

The [FLAC and per-recording cooldown successor](CLOUD_FLAC_UPLOADS.md) is now
queued after the refined worker's graceful stop. It replaces the repeated
inline AssemblyAI upload retry loop described below, while preserving safe GET
retries and paid-request protections. This document retains the predecessor's
behavior and incident history.

Current service is `himr-cloud-transcription-20260913-refined-v1.service` under
the [diarization refinement release](DIARIZATION_REFINEMENT.md), retaining the
retry runner described below. Its predecessor
`himr-cloud-transcription-20260913-resilient-v1.service` replaced the stopped
`himr-cloud-transcription-20260913-title-v1.service`. Gemini and the full-archive
screen are independent and were not restarted.

The new runner is `pipeline/cloud_transcription_resilient.py`, SHA-256
`bca97c65ada742340fa6d3aeb865aa734adf053081279ea570459803f96ad075`.
It runs from the existing `title-override-v1/runtime` under
`research/private-transcriptions/cloud-archive-20260913`, with the same original
cloud plan and title-policy execution release. The runner's hash and exact
runtime import location are checked separately; no pinned production module,
paid plan, receipt, source, or budget was resealed.

## Retry rules

- Retry only AssemblyAI blob upload and provider GET polling/result retrieval.
- Retry controlled transport errors, HTTP 408/429 and HTTP 5xx; respect
  Retry-After. Each safe-operation window has at most four attempts with
  interruptible backoff. Exhausted safe windows enter a cooldown, then resume
  the unchanged durable queue within the finite run deadline.
- Never retry either paid submission method, including Rev AI's multipart POST.
  An ambiguous paid submission keeps its reservation and stops for review.
- Authentication, input/provenance, response-validation and captured-untrusted-
  response failures are not classified as transient retries.

The original duplicate-submission and reservation checks still run on each
cycle. The cloud ceiling remains $150, with four active provider jobs maximum.
Keys are read from `/srv/himr/.env`; no key is included in argv
or retry logs. Diarization, source selection and Gemini input policies are
unchanged.

## Recovery handover

Both previously finished remote jobs were collected using GET-only operations.
This brought local new cloud completions to 14, plus the previously admitted
cloud result; one older normalization-review hold remained. Their 64.6 MB of
temporary upload WAVs were removed by the normal completion cleanup. Original
media remains available to regenerate those files.

The interrupted 480.7 MB upload was restarted without a paid intent or
reservation for that recording. The replacement service waits on the exact
transcription workspace lock so it cannot race that transfer. Once the transfer
finishes or releases the lock, the controller resumes automatically from saved
receipts. A queued `activating/start-pre` state during this handover means the
upload still owns that lock, not that another transcription was submitted.

The service allows two hours for that startup handover, then runs for a finite
24-hour interval with an outer 25-hour runtime timer. Shutdown is cooperative;
no forced SIGKILL or automatic paid retry is used. A runtime-limit exit does not
mean the archive is complete. Do not start the old controller alongside it.

Validation: 16 new retry/lifecycle tests plus 47 existing runtime tests passed.
The actual startup arguments, code/release bindings and plan were also checked
offline with network calls disabled.
