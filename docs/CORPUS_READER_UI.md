# Corpus recording reader

Recording pages have a desktop two-column player/transcript layout, stacking on
smaller screens. The transcript has an independently scrollable, keyboard-focusable
pane, a text/speaker search, an optional full-height view, and opt-in playback
following for native video. Timestamp links retain their stable segment fragments
and cue playback without autoplay. Technical provenance, revisions and segment IDs
remain available in collapsible details; original transcript text is unchanged.

Players use only explicitly retained public HTTPS Archive download URLs or valid
YouTube watch URLs. Archive video uses browser-native controls and `preload=none`.
YouTube uses a click-to-load privacy-enhanced embed. There is no local disk proxy,
new media upload, or guessed filename. Source links remain available when files
are missing, embedding is disallowed, or codecs cannot be decoded. YouTube seeking
reloads the embed at the requested timestamp; automatic transcript-following is
available only for native video.

Recording summaries expand on recording pages, corpus recording lists, summary
cards and filtered summary search results. Full text is fetched only when expanded
from `/corpus/summaries/data/[id].json`, projected from the same validated summary
release. Production builds still exclude private-preview data. Model text is
rendered as text, never injected HTML; uncertainty and allegation tags remain.
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

## Corpus discovery pass

- `/corpus/browse/1/` now provides whole-corpus title, year, content-availability
  and sort controls. It fetches a compact metadata-only `/corpus/recordings.json`
  projection, never transcript shards. Results are paginated in groups of 40;
  filters and page numbers survive reloads via the URL. Static 100-item pages
  remain the fallback without JavaScript or if the index fetch fails.
- Search clears both query and filters together. Whole-recording results no
  longer imply that the first segment timestamp is the matching passage. The
  actual highlighted word (including Pagefind's stemming) is carried into the
  recording's transcript filter. Exact segment indexes keep their anchors.
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
