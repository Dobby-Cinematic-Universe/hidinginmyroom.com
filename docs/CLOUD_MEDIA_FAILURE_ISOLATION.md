# Active cloud worker: per-record media isolation

Service: `himr-cloud-transcription-20260914-isolated-v1.service`.
The preceding `himr-cloud-transcription-20260913-selective-v1.service` is stopped
and conflicts with this service to prevent simultaneous submitters.

Implementation: `pipeline/cloud_media_isolation_runner.py`; deployed immutable
copy and private holds directory:
`research/private-transcriptions/cloud-archive-20260913/media-isolation-v1/`.
The unit pins both the extension and existing runner SHA-256. It retains the
original plan, approved execution release, USD150 cap, four active-job limit,
finite 24-hour run and no automatic paid POST retries. The shared runtime and
Gemini service are not modified.

Known local pre-submission media failures are recorded as durable review holds.
The next cycle skips the held recording and continues with other jobs. No input,
partial decode, existing receipt, reservation, or submission evidence is deleted.
Original inspection runs before applying a hold. Three consecutive preparation
failures without success stop the worker for a possible broader problem.

This is not a blanket catch-all: I/O/storage exhaustion, changed source/decoder
or receipt evidence, authentication, ambiguous paid requests, and other unclassified
failures still stop or reconcile. Existing transient upload/GET retries and
normalization review handling remain. FLAC/model/screen exceptions are not newly
suppressed. See the deployment README for exact isolated messages.

Seven tests exercise narrow classification, persistent holds, unrelated-job
eligibility, original validation precedence, paid-state exclusion, circuit
opening/reset, and propagation of system errors:
`python3 -m unittest pipeline.tests.test_cloud_media_isolation_runner`.

The initial deployment failed its private-directory permission check before
processing; the extension directory was corrected to 0700 and restarted.
