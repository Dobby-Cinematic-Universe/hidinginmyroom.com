# Local speaker review UI

Open http://127.0.0.1:8766 on the archive PC. The independent user service is
`himr-speaker-review-ui-20260914.service`; stop it with
`systemctl --user stop himr-speaker-review-ui-20260914.service`.

The UI loads the continuously refreshed `reviewed-transcript-feed-v1/review-index.json`.
It includes the original 41-recording review snapshot, the 12 previously omitted
multi-label recordings, and newly completed diarized replacements. Recovered
transcript copies are included explicitly. New records appear on the next list
request/page reload without restarting the server. It initially opens the full
Ice Poseidon recording.
Choose a label, click a timestamp to seek the original video, and use the
previous/next matching-turn buttons to sample different occurrences.
The video controls also include a "Jump within speaker" selector and dedicated
Previous/Next turn buttons. These skip other labels, starting from the selected
turn (or playback position before any turn is selected), and stop at the first/last
matching turn. Confirmed/saved names appear in the selector. A jump reveals its
target by clearing conflicting transcript filters if needed; it does not save.
Search, flag filtering, playback speed, ±5 second skips and manual time entry are available.
The "Only recordings with incomplete labeling" checkbox filters the recording
dropdown. Saved Participant decisions count as complete even without a name;
Playback / game audio / background noise and TTS also count. Unsaved turns remain
incomplete. Explicitly saved Uncertain decisions now count as complete while
retaining uncertainty; unsaved turns alone remain incomplete. The API separately
reports `reviewed_uncertain_turns`. Existing confirmed mappings count unless overridden by saved reviews.
Segment exceptions override label-wide decisions. Counts update after saving;
a just-completed video stays open until you select another, without interrupting
playback. This is review completeness, not automatic production approval.

Select a source category using the one-click Participant, Uncertain,
Playback / game audio / background noise, or Text to speech buttons, then optionally enter a confirmed
name and save. The selected category is highlighted. The default
decision scope is every turn with the selected label in the current recording.
Single-turn review remains available. Saving does not show a confirmation dialog.
Both scopes are one-click buttons. Optional Auto-save (off on page load) saves
explicit source/scope/name-button edits immediately and typed names/notes on field
change (leaving the field). Selecting a turn or enabling Auto-save does not save.
Participant auto-saves wait for a nonempty name; manual saving still supports
unnamed participants. Saves are serialized and capture their recording/turn at
edit time so navigation cannot redirect a queued decision. Wait for the Saved
status before closing the page. A failed save is reported and must be retried
manually. The editor is not reset when a save finishes, preserving ongoing edits.
Unreviewed turns default to Participant; existing saved source classifications
are restored rather than overwritten by this default.
Names can be selected from one-click buttons or autocomplete, shared across all
recordings. Buttons sort by current manual assignment count, highest first;
superseded save revisions do not inflate counts. Prior confirmed names seed the
list. Choosing a name fills the editor and selects Participant but never saves
or applies a cross-recording identity automatically.
The combined Playback / game audio / background noise category includes background
noise as well as recorded or game audio. Its stored value remains `playback` for
compatibility with existing reviews. Older decisions display the expanded category
without rewriting them; this does not infer which subtype any older decision meant.
These decisions count as reviewed nonparticipant material. Use notes if you want
to distinguish background noise from actual playback for a particular decision.
Uncertain, this combined category, and TTS decisions cannot carry participant names. Confirmed
Daniel/Kimberly and Daniel/Sabrina mappings are displayed as reference information.

Review saves are append-only, bound to the exact transcript hash, under
`research/private-transcriptions/cloud-archive-20260913/manual-video-reviews-v1/`.
All revisions remain retained; original transcripts are never overwritten.
The separate `himr-reviewed-transcript-feed-20260914.service` applies these reviews
to reversible copies and supplies eligible copies to the reviewed Gemini launcher.
Manual uncertainty counts as reviewed without being converted into a named identity.
See [reviewed transcript handoff](REVIEWED_DIARIZED_TRANSCRIPTS.md) for admission,
playback exclusion, short-source holds, and sealed-summary revision behavior.
The download button exports
the selected recording's saved decisions. Saved names and source categories appear
beside original label IDs in transcript rows and filters immediately after saving
and on reload. Search matches names too. Saved decisions populate the editor.
The latest decision wins within each scope; segment-specific exceptions take
precedence over label-wide decisions. An explicit uncertain or playback decision
suppresses any earlier confirmed-name fallback. Each save retains a new revision.

The server binds only to loopback, checks Host/Origin, requires an unpredictable
token for writes, serves only allowlisted records and assets, and supports HTTP
byte ranges for video seeking. No directory browsing or arbitrary filesystem paths
are exposed as endpoints. It neither rehashes full media nor transcodes it. Playback
depends on the browser supporting the original video/audio container and codecs.
The full Ice Poseidon source is MP4 with H.264/AAC. No new model or API is used.

Tests: `python3 -m unittest pipeline.tests.test_speaker_review_server` and
`node --check pipeline/speaker_review_ui/app.js`. HTTP tests cover byte-range
responses, invalid ranges, Host rejection, missing write tokens, decision validation,
append-only saves, and preservation of original transcript bytes. Interactive
browser playback was not automated in this environment.
