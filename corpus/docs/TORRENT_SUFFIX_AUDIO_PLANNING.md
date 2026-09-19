# Torrent format-label video and audio-only locator planning

This is a separate, read-only planning lane for two filename shapes that the sealed
[terminal-bracket reconciler](TORRENT_BRACKET_RECONCILIATION.md) intentionally
rejects. It does not alter that parser, migration 0018, or its admission tables. This
lane has no migration and no import command.

The planner makes no network request, reads no torrent payload, creates no catalogue
row, and makes no recording relationship, identity, content-match, claim, event, or
publication decision. It emits private filename-locator candidates for later human
review only.

## Exact grammars and scope

The first raw path component must exactly equal one of the same four reviewed labels:

1. `YouTube Videos`
2. `Old YouTube Livestreams`
3. `New YouTube Livestreams`
4. `New New YouTube Livestreams`

The final raw component must end with exactly one of these byte grammars:

```text
\[[A-Za-z0-9_-]{11}\] 480p\.(mp4|webm|ogv|mkv|mov|m4v)
\[[A-Za-z0-9_-]{11}\]\.m4a
```

`480p` and its single preceding space are case-sensitive and exact. File-extension
matching is ASCII case-insensitive. Additional whitespace, another resolution label,
counters, trailing text, partial-download suffixes, a nested extension, a token of
the wrong length or alphabet, and any other variant are rejected. The parser operates
on raw filename bytes; malformed UTF-8 elsewhere in a historical path cannot change
the ASCII suffix result.

The existing `...[ID].<video extension>` grammar is explicitly excluded here and
continues to belong only to the terminal-bracket lane. Audio-only `.m4a` files never
enter its recognized-video grammar. Tests assert these three sets are disjoint.

## Separate evidence, confidence, and routing

The two shapes do not share a generic evidence label:

| Lane | Evidence basis | Confidence profile | Human-review route |
| --- | --- | --- | --- |
| Format-labelled video | `terminal_filename_bracket_before_480p_label_and_video_extension` | `uncalibrated_format_label_filename_locator` | `torrent_format_label_video_locator_review` |
| Audio-only | `terminal_filename_bracket_before_m4a_audio_extension` | `uncalibrated_audio_only_filename_locator` | `torrent_audio_only_locator_review` |

Both profiles retain null raw scores and null calibrated probabilities with
`not_calibrated`. The video route calls for filename-provenance review followed by
video content-identity review. The audio route instead requires audio content-identity
review. Neither route permits automatic catalogue admission, relationship creation,
or publication.

Each proposed candidate binds the original torrent manifest/file source, file index,
directory label, byte count, replacement-decoded catalogue path, exact Base64 raw
path components, length-framed raw-path SHA-256, parsed ID, file extension, current
exact native-YouTube-source resolution, and the lane-specific contract above. A
duplicate ID in two torrent paths remains two file-to-locator candidates, not a
duplicate-recording conclusion.

## Exact-input replay boundary

The planner reuses, without widening, the terminal lane's bounded stable-file,
canonical-bencode, strict discovery-JSON, and complete prior-import replay checks.
That includes all 4,719 torrent-file sources and their origin observations, not just
the paths selected by this grammar. Catalogue reads occur within one SQLite read
transaction. The default CLI uses a read-only, query-only SQLite connection and
returns a path-free aggregate summary:

```bash
PYTHONPATH=corpus/src python3 -m himr_corpus \
  plan-torrent-suffix-audio-reconciliation \
  --db "$PWD/research/corpus/corpus-v8.sqlite3" \
  --torrent "$PWD/research/corpus/discovery/reddit/1pqdsxm/hiding-in-my-room.torrent" \
  --discovery-metadata "$PWD/research/corpus/discovery/reddit/1pqdsxm/discovery.json"
```

`--full` prints raw private path evidence and should be redirected only into ignored
research storage. The complete output conforms to
[`torrent-suffix-audio-reconciliation-plan.schema.json`](../schemas/torrent-suffix-audio-reconciliation-plan.schema.json).

## 2026-08-27 live read-only plan

The exact live inputs produced plan
`tsap_c424e7ce694051a64d4969d200b8fa29`, canonical SHA-256
`c424e7ce694051a64d4969d200b8fa29e3007b0ec42c06773840cf58cdbbb724`.

| Measure | Count |
| --- | ---: |
| Complete provider file records checked | 4,719 |
| Files in the four reviewed directories | 1,462 |
| Accepted format-label video paths | 243 |
| Distinct IDs in the format-label lane | 242 |
| Accepted audio-only paths | 2 |
| Distinct IDs in the audio-only lane | 2 |
| Total file-to-locator candidates | 245 |
| Distinct IDs across both lanes | 244 |
| IDs shared across both lanes | 0 |
| Exact native YouTube sources already present | 0 |
| Format-label tokens rejected by the 11-character ID grammar | 3 |
| Other ambiguous/noncanonical suffixes in this exact live scope | 0 |

Format-label candidate counts by directory were one `YouTube Videos`, 166 `Old
YouTube Livestreams`, 58 `New YouTube Livestreams`, and 18 `New New YouTube
Livestreams`. Both audio-only paths were under `New YouTube Livestreams`.

The three rejected tokens are reported only as an aggregate grammar failure. The
planner does not reinterpret them as YouTube IDs or as another platform's IDs. The
live SQLite database remained at SHA-256
`d4e67b5c19c41b7e0a54f34b6571bf90a032a13f62efd3a68563958c468daccf`, with an
empty WAL before and after the read-only run. No migration or admission canary was
created for this plan-only lane.
