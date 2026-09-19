# Private model-registry manifests

ASR results may reference a model only after a maintainer has registered the exact
local weights and reviewed provenance/license label. Registration uses a private JSON
manifest; never commit a production manifest because it contains a local storage path.
The tracked example is
[`examples/model-registry-manifest.example.json`](../examples/model-registry-manifest.example.json).

```json
{
  "schema_version": 1,
  "manifest_id": "model-registry-whisper-small-en-2026-08-26",
  "created_at": "2026-08-26T22:00:00Z",
  "registered_by": "maintainer handle",
  "basis": "Weights hash checked locally; source revision and model-card license reviewed.",
  "models": [
    {
      "model_id": "model_whispercpp_small_en_example",
      "task": "asr",
      "name": "Whisper small.en ggml",
      "version": "upstream repository and immutable revision",
      "weights_path": "/srv/himr-private/models/ggml-small.en.bin",
      "weights_sha256": "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef",
      "weights_byte_count": 123456789,
      "license_label": "reviewed license label and review date",
      "configuration_json": {
        "source": "https://authoritative.example/model/blob/immutable-revision/weights.bin"
      }
    }
  ]
}
```

Unknown fields, relative/symlink/traversal paths, malformed timestamps, duplicate IDs,
missing files, wrong byte counts, and wrong SHA-256 values are rejected. Validation
rehashes every weight file with descriptor/path stat checks before and after the read:

```sh
PYTHONPATH=corpus/src python -m himr_corpus validate-model-registry-manifest \
  --manifest research/corpus/private-admin/model-registry-2026-08-26.json
```

Import is one SQLite transaction and is idempotent for the same canonical manifest:

```sh
PYTHONPATH=corpus/src python -m himr_corpus import-model-registry-manifest \
  --db research/corpus/corpus.sqlite3 \
  --manifest research/corpus/private-admin/model-registry-2026-08-26.json
```

The private ledger retains the canonical manifest digest, operator, basis, import time,
and a snapshot of each model including exact file URI and byte count. Registry rows,
manifest rows, and their links are append-only. Reusing a manifest ID with different
content or a model ID with different provenance fails the entire transaction.

Moving the same exact weights does not mutate a model row. Register a new manifest ID
whose otherwise identical model snapshot names the new resolved path; the ASR importer
will accept a result only when at least one checksummed snapshot exactly matches its
model path, bytes, hash, name, version, source, and license label.

Registration proves local byte identity and records reviewed metadata. It does not
prove model accuracy, grant publication rights, calibrate scores, approve transcript
text, or make model files public.
