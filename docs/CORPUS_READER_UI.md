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
