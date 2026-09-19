# Two Claude copies and five unresolved Gemini submissions

The operator explicitly chose Claude for the two non-graphic copies that Gemini
refused again. `pipeline/claude_non_graphic_recovery.py` submitted exactly those
two chunk inputs in `msgbatch_01CfkJwyqvAdrC95huHsANtW`. The submitted evidence is
identical to the previously reviewed non-graphic Gemini input, not another rewrite.
The selected profile is the existing `anthropic_sonnet_batch` configuration.

The private audit directory is
`research/private-transcriptions/cloud-archive-20260913/summaries-v2/claude-non-graphic-20260916/`.
It retains immutable input bindings, implementation hashes, exact request bytes,
a durable intent, and the provider receipt. The isolated process-local projection
strips source metadata while keeping original citations locally. No main-worker
code, original transcript, prior receipt or saved review was changed.

`himr-claude-non-graphic-20260916.service` only collects these two requests. It does
not retry, switch providers, or promote chunk outputs into full summaries. Any
further refusal or invalid output remains held. Twenty-nine focused projection,
idempotency, ambiguous-POST and Anthropic transport tests passed before submission.

## Gemini reconciliation

A fresh complete provider listing checked **5,735 batches**. None matched the
five unresolved wave display names. The private
`summaries-v2/reconciliation-20260916/provider-listing.json` records the exact
targets, local intent hashes and timestamps, listing scope, and negative result.
Submission-time journal checks did not establish a definite rejection. Absence
from the listing is not proof that Google never accepted a request.

The five recordings remain held, with original intents and reservations intact:

- Am I a sociopath
- Polished to Perfection? | Galaxy Z Fold 6 In-Depth Review
- I Have Chicken And Beer - Lets Eat My Friends
- I Ran Out Of Money - Going Home To My Mum
- I Found Something Messed Up On My Girlfriend's Phone

## Explicit replacements authorized and submitted

The operator subsequently selected: “Allow one replacement attempt despite
duplicate-charge risk.” `pipeline/gemini_unknown_replacement.py` submitted five
distinct replacement batches, containing eight exact original job inputs. Google
accepted all five. Original unknown attempts, intents and reservations remain
unchanged; these are replacements, not claimed reconciliations of old requests.

The sealed manifest is in
`summaries-v2/reconciliation-20260916/replacements/manifest.json`, SHA-256
`64b874a2de9f40f256b5ff9d321ce1050fb19b8fd45abff9c59f1280d0127d87`.
Its conservative allowance is $1.052572, not actual invoice spend. Each new batch
has exact wire bytes, intent and receipt. An existing replacement intent prohibits
another POST, and a late original receipt prevents a not-yet-sent replacement.
Focused tests exercise both protections.

`himr-gemini-unknown-replacements-20260916.service` collects these five batches
without additional submissions or retries. Results remain explicit recovery
candidates until integrated into the full-recording dependency graphs; old holds
may therefore remain visible in the main worker status. No main-worker restart
or full-archive verification was performed.

Both Claude chunk requests completed and passed strict output/evidence validation
at 22:09 EDT. Their $0.118335 conservative allowance is not actual billed spend.
They also await downstream integration, rather than silently being counted as
completed recording summaries.
