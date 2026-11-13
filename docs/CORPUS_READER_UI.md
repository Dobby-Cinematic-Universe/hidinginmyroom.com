# Corpus recording reader

Recording pages have a desktop two-column player/transcript layout, stacking on
smaller screens. The transcript has an independently scrollable, keyboard-focusable
pane, a text/speaker search, an optional full-height view, and opt-in playback
following for native video. Timestamp links retain their stable segment fragments
and cue playback without autoplay. Technical provenance, revisions and segment IDs
remain available in collapsible details; original transcript text is unchanged.

Players use only explicitly retained public HTTPS Archive download URLs or valid
YouTube watch URLs. A finite [access-copy manifest](../src/data/corpus-media-access-copies.json)
can prefer a verified MP4 copy, including existing Archive derivatives, when the
release, recording, original source ID and exact original URL match. Original
source links and transcript coordinates remain unchanged. The manifest carries
only public URLs and source bindings; it contains no private processing state.
Archive video uses browser-native controls and `preload=none`.
YouTube uses a click-to-load privacy-enhanced embed. There is no local disk proxy,
guessed filename or locally served video. Source links remain available when files
are missing, embedding is disallowed, or codecs cannot be decoded. YouTube seeking
reloads the embed at the requested timestamp; automatic transcript-following is
available only for native video.

Recording summaries expand on recording pages, corpus recording lists, summary
cards and filtered summary search results. Full text is fetched only when expanded
through `/corpus/summaries/data/index.json` and content-hashed data shards,
projected from the same validated summary release. Shared shards contain up to
64 recording summaries and stay below 4 MiB; clients verify the release, entry
mapping and checksum before displaying them. Failed loads can be retried by
closing and reopening the summary. Production builds use only the approved public
projection restored from the checksum-pinned release bundle; private draft exports
and review artifacts remain excluded. The source checkout's empty corpus/summary
placeholders and development preview pointer do not describe deployed content.
The UI capabilities below describe implemented reader behavior; the exact deployed
snapshot is established by its pin and Pages/live publication receipts. Model text
is rendered as text, never injected HTML; uncertainty and allegation tags remain.
Standalone summary links are retained, including as a no-JavaScript fallback.

Verification: source-URL allowlist tests; Astro type checks; desktop/mobile browser
checks for inline loading, filtering, scrolling, links and overflow; native seek
and current-turn highlighting with an intercepted small local video fixture.
External source availability is not guaranteed or verified archive-wide.
No transcription or summarization worker is changed by these UI features.

Desktop-app usability pass: the default desktop layout now gives video 60% of
the workspace, and a Wide player toggle spans the available width (remembered
locally in the browser). Video height is capped at 75vh. Watch/read and transcript
shortcuts support navigation, and single-source players hide the redundant
selector. Notices retain their visible warning labels with expandable detail.
The home page shows twelve recordings ordered by full date, direct search/browse/
summary shortcuts, and compact release metadata. Verified through the in-app
browser at desktop and phone widths; processing jobs are unaffected.

## Date estimates for undated recordings

An undated recording can show an estimate under "Date unresolved". Estimates live
in an optional sidecar beside the catalog (`date-estimates.json`, projected by
[public-date-estimates.mjs](../src/lib/corpus/public-date-estimates.mjs)); the
catalog date, date basis and URL are never changed, so estimates cannot enter
chronologies or trends. A date taken from an exact or full copy is the dated
copy's own catalog date and links to it. A matching transcript needs substantial
literal overlap in both directions; a short excerpt does not date a long stream.
Partial matches are shown as context only. An explicit date in a title can also
be shown as an estimate, with its source identified.
A wording-based estimate shows a month and an approximate range based on model
errors. The details explain that these ranges can miss the true date, especially
near the archive's earliest and latest years. Text matches can reflect re-uploads
or reused material. Browse cards explicitly say "Estimated"; filters and sorting
still treat these recordings as undated. Recordings without an entry keep
"Date unresolved" alone.

## Corpus discovery pass

- `/corpus/browse/1/` now provides whole-corpus title, year, recording length, content-availability
  and sort controls. It fetches a compact metadata-only `/corpus/recordings.json`
  projection, never transcript shards. Results are paginated in groups of 40;
  filters and page numbers survive reloads via the URL. Static 100-item pages
  remain the fallback without JavaScript or if the index fetch fails.
- Search clears both query and filters together. Whole-recording results no
  longer imply that the first segment timestamp is the matching passage. The
  actual highlighted word (including Pagefind's stemming) is carried into the
  recording's transcript filter. Exact segment indexes keep their anchors.
- Search and browse share an optional recording-length filter: under 5 minutes,
  5–20 minutes, 20–60 minutes, 1–3 hours, 3+ hours, or unknown length. Ranges use
  the full recording's duration, with each upper boundary excluded; a 20-minute
  recording belongs to 20–60 minutes. Any length includes unknown durations.
  The choice combines with other filters, survives reloads in the URL, and is
  cleared by Reset. Static search bundles must be regenerated when adding this
  facet; no Cloudflare chat reindex is needed.
- Summary level shortcuts separate yearly/monthly/recording discovery; summary
  pagination is URL-backed. The visible caution expands for full limitations.
- Entity/event directories retain their filters in URLs, offer clear controls
  and explicit empty states. Optional subsets select names with existing speaker
  labels or descriptions with multiple source recordings. These counts do not
  imply verified identity or independent corroboration. Entity pages have jumps
  to mentions, speaker-labeled recordings and related descriptions.
- Sub-minute recordings display seconds rather than rounded zero minutes.

Checked using the Desktop in-app browser: desktop/390px layouts, search-to-reader
links, inline summaries, metadata-only filtering, filtered pagination/reload,
summary-level filters, entity and grouped-event detail routes. Unit tests cover
date sorting, availability, title tokens and undated handling. This work does
not rebuild the archive index, alter source transcripts, deploy the site or
restart paid processing workers.

## Layout polish pass

- Transcript search keeps Search and a secondary "Clear search & filters" button
  on the query row; Load more uses the same secondary style.
- Recording provenance and revision panels wrap long release and revision IDs
  instead of widening phone layouts. Wide player is hidden below the single-column
  breakpoint, where it has no effect.
- Transcript turns, browse results and recording-list cards use tighter spacing;
  inline summaries in recording lists align with the card padding.
- On screens up to 600px the floating chat launcher shows its icon only and keeps
  its accessible name.

Checked with scripted headless Chromium at 1366px and 390px against the local
preview; no data, search index or processing job changed.
