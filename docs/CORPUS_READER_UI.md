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
A publication window from an archived channel listing, matched by video ID, is
shown as an estimated range; it is preferred over a wording estimate, is attached
to a copy date it agrees with, and replaces a copy date that falls outside it.
A wording-based estimate shows a month and an approximate range based on model
errors, widened after a check against archived listing dates. The details explain that these ranges can miss the true date, especially
near the archive's earliest and latest years. Text matches can reflect re-uploads
or reused material. Browse cards explicitly say "Estimated"; filters and sorting
still treat these recordings as undated, with one exception described below.
Recordings without an entry keep "Date unresolved" alone.

### Circa dates, folded copies, earlier titles and channel history

These are reader overlays from archived channel listings. The pinned corpus release,
its catalog dates and titles, the summaries and the transcript analysis are unchanged.

- **Circa dates.** An archived listing window of at most 31 days, matched by YouTube
  ID, places the recording in date order and in the year filter. Its card reads
  "circa 2024-02-01 to 2024-02-10 (archived listing)". Wider windows and wording
  estimates stay "Estimated" and undated for sorting.
- **Folded copies.** An undated recording with an identical transcript to a dated
  recording, whose listing window agrees with that date, is another copy of it. Browse
  lists show only the dated recording; the copy's page stays reachable, says which
  recording it is a copy of, and the dated recording lists its copies under "Other
  copies". The copy's title still finds the dated recording in the title filter.
- **Earlier titles.** `title-history.json` (projected by
  [public-title-history.mjs](../src/lib/corpus/public-title-history.mjs)) holds other
  titles the same YouTube video carried in archived listings. The recording page
  shows them as "Also titled", and the browse title filter matches them. They are
  observations of the public listing, not a verified edit history.
- **Channel history.** `/corpus/channel-history/` charts subscriber readings and
  typical views about a month after upload from `channel-history.json` (projected by
  [public-channel-history.mjs](../src/lib/corpus/public-channel-history.mjs)).
  Every subscriber reading links to the archived page it was read from. The file
  holds aggregates only.

All three sidecars are optional, validated when a release candidate is prepared, and
must be re-exported when a new preview is selected. Transcript search needs no
rebuild: it indexes spoken words, and titles are filtered from `recordings.json`.

A date read from an archived copy of the video's own YouTube page (`basis: archived_page`, or a `page` object
confirming a copy date) is shown as "<date> (archived video page)" with a link to the archived page. It orders and
filters like a circa date, and an identical transcript it confirms is folded like a listing-confirmed one. The page
shows YouTube's time zone, so the text says it can differ by one day. The catalog date is still not changed.

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

## Site-wide scan follow-up

- On screens up to 46rem the corpus navigation is one horizontally scrollable
  row, centred on the current section, instead of four wrapped rows.
- Breadcrumbs share one layout style, so analysis and related-account pages
  have spaced separators like the rest of the corpus.
- Summary card previews end at a word boundary with an ellipsis, and the
  "Read summary" control sits at the bottom of each card. Summary totals use
  singular nouns for a count of one.
- Related-account headings use a smaller, width-capped size, since they are
  full source sentences.
- Wiki verification badges leave space before the article text that follows.

Scanned the homepage, every corpus route type and all 65 wiki pages at 1366px
and 390px with scripted headless Chromium against the local preview; 2,965
internal links returned no errors.

## Search result dates and summaries

Transcript search results show the recording date and length. Undated recordings
show their labelled estimate ("Estimated …") or "Date unresolved". Results with a
recording summary get the same collapsible "Read summary" control used in
recording lists; it sits outside the result link and loads only when opened. Both
come from the metadata-only `/corpus/recordings.json` projection, fetched once per
page alongside the search index. If it fails to load, results still appear with
the index's own date label and without summary controls. No search index rebuild
is needed.

### Evidence dates, date notes and machine-derived reader aids (2026-11-14)

Sidecars beside the catalog, each optional and projected by `scripts/prepare-release-candidate.mjs`:

- `date-estimates.json` has two more forms. `basis: evidence_date` carries `evidence: {from, to, sources}` for a
  range of a week or less from combined evidence and is shown as "circa <date> (<main source>)"; it orders and
  filters like a circa date. A `text_estimate` may carry `narrowed_by`, and its explanation then names what narrowed
  the range. Source names are a fixed list in `src/lib/corpus/public-date-estimates.mjs`.
- `date-notes.json` explains a catalog date whose basis needs a sentence (`archived_video_page`,
  `torrent_filename_utc`). The recording page shows a readable date basis for every recording.
- `recordingDateTexts()` in `src/lib/corpus/date-estimates.ts` gives the date text for any linked recording: the
  catalog date, else the labelled estimate, else a fallback. Event, event-group, entity, event-description and
  summary pages use it, and `/corpus/date-texts.json` serves the same texts to client-rendered analysis views.
- `search-aliases.json`: other spellings of game, product and place names found in the machine transcripts.
  Transcript search also runs the other spellings and says which were added. No people.
- `related-recordings.json`: a recording that names an earlier one. Shown as "Refers back to" and "Referred to in"
  with a note that it is detected automatically.
- `chapters.json`: machine-detected topic shifts in long recordings, each with up to four frequent words after
  name-like and sensitive words are removed. Shown in a collapsed list that links into the transcript.
- `mention-bursts.json`: stretches in which a game, product or platform is named unusually often, shown as
  "What was talked about when" at the end of Entities & events (`/corpus/graph/#mention-bursts`). No people or places.
- `channel-history.json` may carry `names` per channel; the projection still accepts `null` for a withheld name
  (shown as a rename without the name), but the current export publishes every archived name. Channel history has its own corpus navigation link and home shortcut.
- "How recordings are dated" is a collapsed note under Provenance on every recording page; its counts are computed
  once per build (`src/lib/corpus/dating-overview.ts`).

`--local-analysis=<run directory>` lets a candidate rehearse with a completed local analysis run before its bundle
is pinned.

### Source recordings and passed-on estimates (2026-11-14, second update)

`date-estimates.json` has three more forms and one more optional field. All are projected and checked by
`src/lib/corpus/public-date-estimates.mjs`; nothing else on the site switches on the basis.

- `basis: source_recording` carries `source: {recording_id, date, tier, relation}`. The recording's words lie inside
  a dated recording (tier `A` firm, `B` probable) and it takes that recording's catalog date. With
  `relation: same_recording` it is shown as "circa <date> (part of a dated recording)" and orders and filters like a
  circa date. With `relation: not_after` the recording found may only be a later replay; it is shown as
  "<date> or earlier" and is never a circa date. The recording page links the source recording.
- `basis: source_estimate` carries `source: {recording_id, basis: text_estimate, relation: inside_undated_recording}`
  and repeats the containing recording's `estimate` (and `narrowed_by`). The projection rejects it unless both
  equal the containing recording's own row.
- `basis: series_period` carries `period: {from, to, rule}` with `rule` `same clip name` (one day) or
  `between numbered neighbours` (a range). Never a circa date.
- A dated `source` may also accompany `title_date`, `archived_listing` and `archived_page`. The explanation then
  adds that the words are also found inside a recording of that date, and says so when it differs from a title date
  by more than a month.
- Evidence source names gained `excerpt` (a dated excerpt cut from the recording) and `replay` (older dated
  material played back in it).

Two more reader additions of the same update:

- `version-links.json`: other recordings of the same video (a second archive copy, a re-upload, the same broadcast
  on another platform). Each link is `{target, phrase}`; the phrase describes the other recording compared with this
  one and must match the fixed forms of `VERSION_PHRASE` in `src/lib/corpus/public-reader-extras.mjs`. The
  recording page lists them as "Other versions of this video" under the title, beside "Refers back to", and leaves out
  copies it already lists under "Other copies".
- `channel-history.json` may carry, per channel, `monthly` (month-end subscriber and total-view values worked out
  from archived copies of a statistics site, each row with the archived pages it rests on), `view_readings` (total
  views shown on archived copies of the channel's About page) and `monthly_views_uncertainty`. The channel history
  page draws the month-end subscriber values as a thin line and has a "Total views" section. Sources are checked
  against fixed address forms in `src/lib/corpus/public-channel-history.mjs`.

## Text-analysis explorer for window-based releases (2026-11-14)

An analysis release may now say how it was made (`design` on the release) and how each recording entered it
(`design` on a recording). Both are optional and checked by `src/lib/analysis/explorer.mjs`; a release without
them is shown as before.

- **Release `design`:** `window_words` (every passage holds this many words), `min_words` (shorter transcripts are
  not rated), and `second_rater`: a second model's name, how many recordings and passages it rated, its agreement
  with the first model per dimension, and optionally the direction of each dimension's trend by kind and model.
- **Recording `design`:** `kind` (video, stream or clip), `words`, `windows`, `short_text` (shorter than one
  passage), `fit_cohort`, `factor_scores`, and `version_of` (the recording that represents this one).
  A recording may also carry `second`: the second model's score on each fitted dimension.
- **Recordings by dimension** opens on the first dimension. Filters: kind, year, category, subject, length
  (3 or 10 passages and more) and a title search. Two boxes, on by default, keep one version of each recording
  and leave out texts shorter than one passage. The table shows kind, words and passages, the score, and for a
  question the spread across passages.
- **Scores over time** draws videos and streams as separate lines with a 95% interval of each period mean.
  A period with fewer than five recordings is a hollow point without an interval. Hovering, or focusing the chart
  and using the arrow keys, shows every series at one period. "Which trends hold" under the chart needs a
  second model's scores (`design.second_rater`); the table and the row notes call it the second model.
- **Recording map** starts on the recording closest to the centre and can be coloured by kind. A copy or a short
  text has no place on the map; the panel names the recording that represents it.
- **Time, genre and candidate shifts** has a kind selector; `temporal-comparisons.json` carries the same analysis
  for videos and streams apart under `kinds`.
- **Inside recordings** lists, per question, the 500 recordings with the largest spread between passages
  (`ranking_cap` in `passage-insights.json`).
- Questions are named by the bank's `short_name`, which is a readable name from version 2 on
  (`questionLabel` in `explorer.mjs` falls back to a name made from the question id).
- The chart colours for videos, streams and clips (`--analysis-series-1` to `-3`) were checked against the corpus
  panel colour for lightness, chroma, colour-vision separation and contrast.
- Factors are oriented so that the largest loading is positive (`pipeline/typesafe_analysis.py`), so a higher
  score means more of what the dimension is named after.

`src/data/analysis/questions.json` is version 2 of the question bank. `src/data/analysis/factor-descriptions.json`
still belongs to the published analysis; it is replaced together with `analysis-release.json` at publication.

## Passage explorer

`/corpus/analysis/passage-explorer/` holds four views on single passages: subjects by year, the course of one
recording, topics, and a passage finder. They are computed in the browser from the release's **passage table**
(`passage-table.json`), an optional member of the analysis bundle: one row per full passage of every video and
stream, with the passage's dimension scores, subject, addressee and start time, and the passages that name each
topic (products, games, platforms and a few places; no people). Each recording is marked as the version the analysis
counts or as another version: the views that add passages up (subjects, topics) count one version of each recording,
while every version can be opened and, on request, searched. Years in the views come from catalog dates or,
when those are absent, labelled date estimates. A new year appears without a code change. The table holds no transcript text; each passage links
to its place in the transcript. `src/lib/analysis/passage-table.mjs` validates the table against the release and
holds the aggregations; `scripts/check-analysis-release.mjs` checks it when it is present, and
`scripts/analysis-release-bundle.py` packs and restores it. A release without the table stays valid, and the page
then says that the passage views are not available. Dimensions carry their standing from the human rating studies
(`src/lib/analysis/validation-studies.json`).

## Estimated dates in date order, year filters and periods (2026-11-16)

By the owner's decision, a recording without a catalog date is placed by its estimated date wherever recordings
are ordered or grouped by date: the browse page (order and year filter; a circa date still comes first where both
exist), the analysis explorer (date order, year filter, the periods of the trend chart, the year colours of the
recording map) and the passage explorer's views by year. The catalog date is never changed, a catalog date is never
replaced by an estimate, and an estimate is always shown as one. `/corpus/estimated-dates.json` serves the estimated
day or month per recording; `/corpus/date-texts.json` still serves the labelled text. The precomputed panels of the
analysis release (joint trends, temporal comparisons) keep the dates they were computed with.

## Loading the analysis explorer (2026-11-16)

A CPU profile of the page load showed that the largest single cost in the page's own code was checking each score
shard against its identity by serializing it again. `src/lib/analysis/shard-verify.mjs` now hashes the bytes of
`videos` where they lie in the response and parses the same bytes, with the old path kept for a body laid out
otherwise. Shards load six at a time, and numbers share one formatter.

Score shards are served in a compact lossless encoding (`src/lib/analysis/shard-codec.mjs`, shard schema version
2): a list of like-shaped rows is written as columns, so key names appear once per shard instead of once per score.
It decodes to exactly the rows of the release, key order included; `encodeShard` checks that for every shard at
build time and carries a shard as it is if the check fails. A shard's identity is the hash of the encoded rows it
serves. On the release of 2026-11-14 the shards shrink from 45.7 MB to 10.9 MB of JSON (5.0 MB to 3.5 MB
compressed). The release bundle and `release.json` are unchanged: only what the explorer downloads is encoded. The
local release checkers (`pipeline/check_analysis_site_release.mjs`, `check_analysis_live_release.mjs`) read both
the old and the new shard format. What remains at load is the validation of every score.

## Interaction and rendering measurements (2026-11-17)

Measured on a built candidate in a headless browser. Interactions of the explorers, the browse filter and the
corpus search respond in under 70 ms, with three exceptions that were changed: the recording map's selector listed
every recording on each keystroke (now at most 300 matches, options made when first listed, titles lowercased
once); the corpus search loaded its index only on the first query (now when the form is first focused or pressed);
the browse filter waited 150 ms after typing (now 80 ms).

Rendering cost is concentrated in the recording pages: the largest holds 16,000 segments in 112,000 elements and
spends over two seconds in layout and style at load. Two things were tried on the built page and **not** adopted:
`content-visibility: auto` on every segment made scrolling and the transcript filter far slower (the filter took
over a minute), and on the explorer's sections it made no measurable difference. Removing the per-segment
"Segment details" element (29% of the pages' HTML, three of seven elements per segment) halves the load time of the
largest page. The owner approved it on 2026-11-17: a segment's time range and identifier are now a `data-detail`
attribute, shown by CSS on the selected segment (`li:target::after`), with no script. On the built site the largest
recording page went from 112,000 to 64,000 elements and from 2.2 s to 1.0 s of layout and style at load, and the
whole site from 3.95 GB to 3.35 GB.

In the analysis explorer, rendering at load is small and no section costs more than 40 ms to lay out. A change of
the recording map's colouring now recolours the dots in place instead of drawing the chart again (62 ms to 22 ms),
and a dot gets its tooltip element when the pointer first reaches it. With the capped selector this takes the page
from 17,700 to 10,600 elements. The correlation matrix (about 130 ms to open, 110 ms per resize) was left as it is.

## One rule for title searches (2026-11-17)

Title searches follow one rule, in `src/lib/corpus/title-search.mjs`: every word of the query must occur in the
searched text, in any order and not necessarily adjacent; case does not matter. The searched text is the title and
the recording's earlier titles (with the titles of identical copies folded into it), plus what a view adds: the
category in the analysis explorer's filter, the date and category in the recording map's selector, the date in the
passage explorer's recording search. The browse page uses the same functions. `/corpus/title-aliases.json` serves
the earlier titles to the client-rendered views; a row found only through an earlier title says "Also titled".
Before, the three analysis searches needed the query as one unbroken string and did not search earlier titles.

## Dates from community re-uploads, and an upper bound for estimates (2026-11-17, 2026-11-18)

Four streams of November and December 2025 have no date in any archive. A community archivist re-uploaded them and
gave a day with each. Since 2026-11-18 those days are catalog dates:

- Catalog `date_basis: community_reupload_note`. `describeDateBasis` labels it "Day given by the community
  archivist u/HIMRedits with a re-upload of the stream". The analysis explorer calls it "Community re-upload note".
- `date-notes.json` carries one note per such recording: `basis: community_reupload_note` and `stated`, the day or
  the two consecutive days the archivist gave. `projectDateNotes` requires the catalog date to be the first stated
  day and rejects any other field, so no address of the re-upload or of the post reaches the site; the date note labels the source and the recording page says
  that the re-upload's audio was matched to the recording, that the day comes from the archivist and can be a day off, and both possible dates where applicable.
  Chat uses the same date note for these streams.
- The "How recordings are dated" note states the exception to the catalog rule and why these dates can be a day off
  (two other re-uploads can be checked against an archive date and differ from it by one day each).
- A clip whose words lie inside one of these streams is a `source_recording` estimate like any clip inside a dated
  recording. Its page says that the stream's date is the day the archivist gave: `projectDateEstimates` sets
  `source.stated_date` from the catalog's date basis (never from the estimates file) and `describeDateEstimate`
  words the sentence accordingly.

Still supported by `src/lib/corpus/public-date-estimates.mjs`, with no record using it at present: the evidence
source `reupload_note` for an `evidence_date`, and `basis: source_estimate` passing on an evidence date
(`source.basis` is `evidence_date`; the row repeats the containing recording's `date` and `evidence`, carries no
`estimate`, and is rejected unless date and range equal the containing recording's own row). The four streams were
shown that way for one local preview before they got catalog dates.

No estimate goes past 5 December 2025, the day of the last stream of 2025. The wording model and the date fusion
were refitted and re-run with that day as the latest possible one for an undated recording. The export enforces the same cut-off. Catalog dates are not affected. The "How
recordings are dated" note states the rule, the share of ranges that held the true date in the back-test (82% of
3,039 dated recordings; 85% of 79 cases where a dated excerpt is the main clue) and that about one wording
estimate in twenty is far off.
