# Read-only whisper.cpp batch archive disposition

`asr_whispercpp_batch_archive.py` is a separate archival validator for one closed
supersession record. It does not import the active ASR adapter or batch runner and has
no run, materialize, import, publication, database, network, or write operation.

The lane preserves the sealed v0.1.0 batch
`asrbatch_794f61d7ba9b883c8ab37cbcdc5a6d28` as immutable evidence. That batch pins adapter
0.2.2 and the historical whisper.cpp v1.8.3 executable. Its exact eight work orders were
superseded by the byte-identical work-order tree in
`asrbatch_a98bf085d254a67bb30b837b307436f6`, whose only manifest changes are its batch
identity/path and the complete adapter 0.2.3 identity tuple. Neither archive receives
execution authority: v1.8.3 remains validation-only, and recovery requires a newly
materialized current-profile subset.

## Private disposition receipt

The operational receipt is intentionally Git-ignored and stored outside both sealed
batch directories:

```text
research/corpus/private-asr-work-orders/batch-dispositions/
└── asrbatch_794f61d7ba9b883c8ab37cbcdc5a6d28.json  # mode 0400
```

The containing directory must have mode `0700` and contain exactly that receipt. Do not
put the receipt inside either batch directory: a sealed batch admits only its manifest
and `work-orders/` directory. Because `/research/` is ignored, the private workspace's
normal durable backup procedure must preserve the receipt separately from tracked code.
Its current SHA-256 is
`a97ff0258eee55c06a3f907a2b3972a47d6f64e8f2600625eb89d0433ae54ec4`.

The receipt binds both physical manifest hashes, full deterministic identities, exact
materializer/adapter/engine tuples, all ordered work-order hashes, the six declared
manifest differences, and limited historical result evidence. It records that the
adapter-0.2.2 job-1 association is consistent with adapter version, work-order identity,
and observed chronology but is not cryptographically batch-bound because result v1 has
no batch ID. It does not claim an undocumented adapter-0.2.2 job-2 attempt.

The receipt retains the adapter-0.2.2 digest and byte count as an identity pin. It binds
no copy of those historical source bytes and explicitly makes no source-reproducibility
claim. The byte-identical job-1 raw and normalized artifacts remain historical evidence;
the successor job-2 result independently records ten preserved
`invalid_upstream_inverted` tokens handled by adapter 0.2.3.

## Validate

```sh
pipeline/bin/asr-whispercpp-batch-archive validate \
  --receipt research/corpus/private-asr-work-orders/batch-dispositions/asrbatch_794f61d7ba9b883c8ab37cbcdc5a6d28.json
```

Validation is standard-library-only. It opens every receipt, manifest, work order, and
named evidence artifact read-only with symlink refusal and a single-link requirement;
retains the descriptors for the whole audit; verifies exact bytes, modes, entry sets,
canonical JSON identities, tree equality, and the closed allowlist; then checks every
descriptor and logical path again before returning. Success prints a non-authoritative
validation summary. A validation failure after argument parsing prints structured JSON
to stderr and exits 2.

The tracked strict shape is
[`schemas/asr-whispercpp-batch-disposition.schema.json`](schemas/asr-whispercpp-batch-disposition.schema.json).
JSON Schema checks structure and the paired closed software identities. Runtime
validation remains authoritative for bytes, filesystem modes, hard links, tree shape,
hashes, canonical serialization, and race detection.

## Tests

```sh
PYTHONPATH=pipeline python3 -m unittest \
  pipeline.tests.test_asr_whispercpp_batch_archive -v
python3 scripts/validate-json-contracts.py
```

The focused suite validates the real private fixtures when present, checks that evidence
bytes and filesystem metadata are unchanged, rejects cross-paired software identities,
wrong modes, hard links, symlinks, duplicate keys, invalid UTF-8, non-finite JSON, and a
swap/restore race, and asserts that the CLI exposes only `validate`.
