# HIMR Wiki editorial and verification policy

The HIMR Wiki is an independent historical reference. Its job is to describe the
available record accurately, not to turn rumors, machine transcripts, video titles,
or a creator's account into unqualified fact.

This policy applies to every publicly visible page under `src/content/docs/wiki/`.
Separate private research notes may contain incomplete leads, but they must retain
clear warnings and must not be copied into the public wiki without review.

## 1. Evidence layers

Use the narrowest statement supported by the evidence.

| Source | What it can establish |
| --- | --- |
| Raw video or audio | What was audibly said or visibly shown at a stated time |
| Human-corrected transcript | Searchable wording for the segment that was checked |
| Machine transcript | A lead to a possible segment; never evidence, fact, or a quotation by itself |
| Video title or filename | How an item was labeled; not proof that the title is true or that its date is authoritative |
| First-party post or video | What the author publicly claimed |
| Independent primary record | The fact directly documented by that record |
| Reliable secondary source | Context or corroboration within the source's demonstrated scope |
| Reviewed public community image or repost | What that complete copy visibly or audibly contains, within its preserved context; not original provenance, completeness, identity, date, or proof of an off-camera event |
| Other community post, wiki, or forum | Community usage, an attributed allegation, or a research lead; not proof that the allegation is true |
| AI or NotebookLM summary | Topic discovery only |

A video can verify the sentence “Daniel said X.” It cannot, by itself, verify that X
happened. Use attribution whenever the underlying event has not been independently
corroborated.

Automatic captions, Whisper, and other speech-recognition output are machine
transcripts even when they were generated locally from the raw file. They can locate
a candidate passage, but an audio quotation or close paraphrase is **video checked**
only after a human reviewer directly listens to the raw audio and records that review.
Visual frame review does not imply that the soundtrack was heard.

A narrow public **unverified research lead** exception exists for benign research
navigation. Such a lead must be conspicuously labelled, must never be written as a
fact, quotation, or evidentiary paraphrase, and must identify the exact source,
candidate locator, raw-media status, and the named human-review queue. This
exception does not apply to alleged serious wrongdoing, sexualized claims involving
minors, or invasive private, relationship, financial, travel, or medical detail. Keep
those subjects in the non-public research record until the applicable verification and
sensitivity standards are met.

A separate narrow **attributed self-account** exception permits a high-level
machine-text paraphrase of a notable adult relationship or sexual claim made by the
public creator. It records the allegation that the creator made the account; it does
not make the machine text evidence that the speech or underlying event is true. Use
this exception only when all of the following are satisfied:

- the public raw source and exact candidate range are identified;
- at least two independent ASR passes materially agree on the central passage, while
  any single-machine supporting chronology remains explicitly labelled as such;
- no machine-text quotation is published, and the page plainly says that nobody
  directly perceived the audio;
- the summary is necessary for a notable topic, minimizes intimate detail, and does
  not identify otherwise unnamed people;
- the maintainers' review record states the underlying status, corroboration limit,
  and result of a bounded response search; and
- minor-related or age-dependent claims, alleged crimes or abuse, leaked intimate
  material, graphic detail, and claims whose gist depends on unresolved consent are
  omitted.

This exception should be rare. A visible creator caption may establish how the source
was edited, but it does not corroborate the machine-rendered narration or the event.

## 2. Verification states

Every material claim has one state:

- **Unverified** — imported or proposed but not checked. Keep in research only, except
  for a conspicuously labelled benign research-navigation lead that satisfies the
  requirements above. The exception does not upgrade the material into evidence.
- **Source matched** — the referenced file or post exists and its identity has been
  reconciled. This still does not verify its contents.
- **Video checked** — the cited audio or visual segment was reviewed against the raw
  media, with a precise locator and enough surrounding context.
- **Community media checked** — the entire available public repost and its post
  context were reviewed. No obvious substantive edit or credible authenticity dispute
  was found as of the review date. This verifies only what the preserved copy appears
  to show or say; it does not authenticate the original.
- **Corroborated** — a video-checked or otherwise directly supported claim is also
  supported by an independent source appropriate to the claim.
- **Disputed** — credible sources conflict. Preserve the disagreement and attribute
  each account.
- **Rejected** — the available source does not support the proposed wording.

“Source matched” is sufficient for a source catalogue. Narrative biographical,
financial, medical, relationship, legal, or controversy claims normally require
“video checked,” careful attribution, and independent corroboration where available.
“Community media checked” is sufficient only for a narrow direct observation about
the repost itself, not for the truth of its title or an event outside the recording.
“Unverified research lead” is not a lower evidentiary shortcut: it can expose only a
benign candidate locator and review task, not assert what the machine output says is
true or quote it as speech.

Public subreddit images and clips may therefore supplement a missing primary source.
Review the whole available item and the surrounding post, preserve the exact media
rendition and hash, and describe only what can be directly perceived. Platform
recompression, resizing, and a disclosed non-misleading crop are acceptable. Apparent
splices, dubbing, synthetic changes, misleading overlays, essential missing context,
or a credible authenticity dispute keep the item unverified or disputed. Use the
wording “no obvious substantive edit or credible dispute was found,” never
“authenticated,” “original,” or “unedited.”

## 3. Atomic claims and citations

Break prose into claims small enough that a reader can tell exactly what each citation
supports. A citation must identify:

1. a stable source ID;
2. the title and source URL;
3. an exact timestamp range, page, post, or archive filename;
4. the evidence type, such as self-report, visible action, or independent record;
5. whether the cited media was manually checked;
6. for a community repost, the exact media URL, media type, retained hash, whole-item
   review result, and provenance limitation; and
7. the review date.

Do not place one citation after a paragraph containing several unrelated facts. Do
not use a search result, an AI answer, or a link to an archive homepage in place of
the exact source item.

Short quotations must be transcribed from the raw source. If a word remains unclear,
use `[unclear]`, paraphrase conservatively, or omit it. Never silently “repair” an
ambiguous name, number, or date.

## 4. Dates and source identity

Track these separately when applicable:

- event date;
- recording or stream date;
- original publication date;
- archive upload or capture date; and
- wiki verification date.

A date embedded in a filename is a **filename date** until another source establishes
what it represents. Use `circa`, a date range, or `unknown` instead of inventing
precision.

Likewise, one transcript record is not automatically one distinct video. Duplicate,
chunked, renamed, and incorrectly mapped records must remain flagged until reconciled.

## 5. Conflicts, changing accounts, and clickbait

Video titles are labels, not conclusions. If the substance of a video differs from
its title, describe the substance. When a later source changes or contradicts an
earlier account:

- preserve the chronology;
- cite both accounts;
- say who made each claim and when; and
- do not resolve the conflict without supporting evidence.

Avoid narrative language that implies a motive, diagnosis, or causal connection the
sources do not establish.

## 6. People, privacy, and sensitive claims

Do not publish home addresses, private contact details, financial account data,
non-public legal names, leaked intimate material, or information whose main effect is
to facilitate harassment. Public availability elsewhere is not sufficient reason to
republish it.

Use the public name or pseudonym by which a non-public person appears in the source.
Do not diagnose a person. Describe medical or mental-health statements as self-report
unless supported by an appropriate public record and necessary to the article.

Claims of crimes, abuse, fraud, sexual conduct, or other serious wrongdoing require
particular care. An allegation may be included without adopting it as fact when the
wiki identifies exactly who or what source made it, labels its underlying truth as
unverified, disputed, or corroborated, places a prominent warning beside it, states
the corroboration status, and includes a relevant denial or says that no response was located. A
community title can establish only that the title made the allegation; a checked
repost can establish only the attributed words or visible conduct in that copy.

A warning does not make every allegation publishable. Omit doxxing, leaked intimate
material, diagnoses, gratuitous sexual detail, threats, and claims whose presentation
would mainly facilitate harassment. Keep the wording no broader than the cited source
and do not turn anonymous speculation into the wiki's own conclusion.

## 7. AI-assisted research

AI may search transcripts, cluster topics, propose candidate claims, and help format
citations. It may not promote its own summary into evidence. The reviewer must open
the underlying source and perform the required check.

NotebookLM drafts in the private research workspace are unverified research indexes.
Their titles, counts, summaries, entity links, and chronology must be re-established
from primary sources and the maintainers' review record before publication.

## 8. Publication checklist

Before publishing or materially expanding a page, confirm that:

- every contestable statement has a corresponding maintainer review record;
- every citation supports the exact adjacent wording;
- self-reports are attributed;
- dates state their basis and precision;
- contradictory sources are represented fairly;
- cited transcript segments were checked against available media;
- any public machine-only research lead is benign, conspicuously labelled, and gives
  its exact source, candidate locator, raw-media status, and named review queue;
- any machine-text attributed self-account satisfies the separate narrow exception,
  carries a prominent adjacent warning and response-search result, and omits minor-related,
  age-dependent, criminal, abusive, leaked, graphic, or consent-dependent material;
- reviewed reposts have a full-item integrity assessment and retained media hash;
- allegations carry an adjacent warning, attribution, underlying verification state,
  corroboration status, and response note;
- sensitive personal information has been minimized;
- the page shows its verification status and last review date; and
- another pass found no unsupported implications introduced during copy-editing.

Corrections should preserve the reason for the change in version control. Material
disputes belong in the private review record even when the rejected wording is removed
from the public page.
