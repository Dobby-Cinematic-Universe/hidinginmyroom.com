# Local related-account grouping

`npm run group:corpus-events` rebuilds the grouping artifact for the active private
corpus preview. Restart Astro afterward. No paid API or external inference service
receives text. Original event descriptions, transcripts, summary links and existing
event URLs are preserved. Production publication boundaries are unchanged.

## Implementation

The isolated runtime is `research/corpus/event-grouping-runtime-20260917`.
It uses the official quantized ONNX `sentence-transformers/all-MiniLM-L6-v2` model,
revision `1110a243fdf4706b3f48f1d95db1a4f5529b4d41`, variant
`onnx/model_quint8_avx2.onnx`. Only public model assets were downloaded. See the
[model card](https://huggingface.co/sentence-transformers/all-MiniLM-L6-v2).
Dependencies installed in its own venv: numpy 2.5.3, onnxruntime 1.30.0,
tokenizers 0.23.2. Existing ASR/screening runtimes are not modified.

Mask-aware mean pooling produces normalized 384-dimensional vectors. Descriptions
over 256 tokens use overlapping windows and pooled aggregation rather than silently
discarding their endings. SQLite caches each vector by exact description, model hash,
tokenizer hash and pooling recipe. Reruns only embed changed/new descriptions.

Clustering takes the nearest 32 candidates per description, then requires every
cross-pair to pass the applicable similarity threshold before two groups merge.
Groups are capped at 24 descriptions, preventing transitive chains of loosely related
accounts. The uncalibrated cosine thresholds are .82 with a shared informative name,
.86 with shared explicit year mentions, otherwise .90. Daniel alone does not lower
the threshold. Disjoint explicit years prevent grouping; recording dates never do.
These are conservative retrieval heuristics, not confidence or factual accuracy scores.
Explicit marriage and divorce transitions are kept apart rather than conflated because
they share names. Contradictory claims about the same transition are still retained.

The output is `event-groups.json` beside the active preview, bound to the exact event
input digest. A changed description/source set makes that artifact stale; the site
falls back to original descriptions and displays a refresh notice. Group IDs bind
membership and are deterministic for the same set of original event IDs.

## Presentation and limitations

The directory shows multi-recording groups first. Group pages retain each description
verbatim with its own transcript links and original event page. Existing event pages
link back to their related-account group. Entity event lists use the same grouping.
Unmatched descriptions stay searchable. The heading is a representative description,
not a generated synthesis. Contradictions are not resolved and no new identity or event
date is asserted. Repeated purchases, trips or streams may remain distinct or be grouped
as related accounts; the UI never claims a group proves one shared occurrence.

Source counts are physical recordings, not necessarily independent videos or witnesses.
Duplicate copies and retrospective retellings do not establish corroboration.

The initial active pass grouped 1,151 of 15,540 descriptions into 501 groups, 496
spanning multiple recordings. Corrected attention-masked encoding plus clustering
took about 50 seconds; subsequent fully cached grouping passes took 3–5 seconds.
One inspected group retains six separate Apple Watch gift descriptions with all six
recording links. UI navigation, source links, and unchanged original-description links
were tested in a browser. The larger number of unmatched entries is intentional:
conservative similarity does not force every archive description into a topic group.

Tests:

```
research/corpus/event-grouping-runtime-20260917/venv/bin/python -B -m unittest pipeline.tests.test_event_embedding_groups
node --test scripts/tests/event-groups.test.mjs scripts/tests/derived-corpus-graph.test.mjs
npx astro check
```
