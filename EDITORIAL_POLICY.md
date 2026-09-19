# HIMR Wiki editorial and verification policy

The HIMR Wiki is an independent historical reference. Its job is to describe the
available record accurately, not to turn rumors, machine transcripts, video titles,
or a creator's account into unqualified fact.

This policy applies to every publicly visible page under `src/content/docs/wiki/`.
Separate private research notes may contain incomplete leads, but they must retain
clear warnings and must not be copied into the public wiki without the source,
coordinate, rights, privacy, sensitivity, identity, and allegation checks below.

## 1. Evidence layers

Use the narrowest statement supported by the evidence.

| Source                                    | What it can establish                                                                                                                                                     |
| ----------------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| Raw video or audio                        | What was audibly said or visibly shown at a stated time                                                                                                                   |
| Human-corrected transcript                | Searchable wording for the segment that was checked                                                                                                                       |
| Machine transcript                        | A lead to a possible segment; never evidence, fact, or a quotation by itself                                                                                              |
| Video title or filename                   | How an item was labeled; not proof that the title is true or that its date is authoritative                                                                               |
| First-party post or video                 | What the author publicly claimed                                                                                                                                          |
| Independent primary record                | The fact directly documented by that record                                                                                                                               |
| Reliable secondary source                 | Context or corroboration within the source's demonstrated scope                                                                                                           |
| Reviewed public community image or repost | What that complete copy visibly or audibly contains, within its preserved context; not original provenance, completeness, identity, date, or proof of an off-camera event |
| Other community post, wiki, or forum      | Community usage, an attributed allegation, or a research lead; not proof that the allegation is true                                                                      |
| AI or NotebookLM summary                  | Topic discovery only                                                                                                                                                      |

A video can verify the sentence “Daniel said X.” It cannot, by itself, verify that X
happened. Use attribution whenever the underlying event has not been independently
corroborated.

Automatic captions, Whisper, and other speech-recognition output are machine
transcripts even when they were generated locally from the raw file. They can locate
a candidate passage, but an audio quotation or close paraphrase is **video checked**
only after a human reviewer directly listens to the raw audio and records that review.
Visual frame review does not imply that the soundtrack was heard.

An eligible **unverified machine-transcript lead** may be published without a human
wording review. It must never be written as a verified fact or quotation and must
identify the exact public source, candidate range, raw-media status, and machine
status. Every such passage must display the canonical warning: **“Machine-generated
and unreviewed; may be wrong; not a verified quotation.”** A second ASR pass can help
triage errors but is not a publication prerequisite and does not verify the wording.

Eligibility remains deny-by-default at the rights, privacy, sensitivity, identity,
and coordinate layers. Publication is permitted only after all applicable gates are
recorded as clear. Doxxing, leaked intimate material, sexualized claims involving
minors, unsupported diagnostic inference, gratuitous graphic detail, and content whose
main effect would be harassment remain private regardless of a warning.

A high-level machine paraphrase of a notable first-person claim by the public creator
may cover his own circumstances, conduct, intentions, public work, finances, housing,
travel, adult relationships, or a publicly discussed family event when the exact
source and range are given. Exact amounts or other sensitive detail should appear only
when material to the topic and should be minimized. Machine output alone cannot name a
speaker: attribution must come from independently checked source ownership/context or
a recorded speaker review. A visible face does not by itself prove who produced the
soundtrack.

Machine-only allegations need a second, adjacent **Unverified allegation** warning in
addition to the canonical machine warning. It must identify the attributed speaker,
state the corroboration status, and include a relevant response or say that none was
located. Publication does not verify the machine wording or underlying event. Claims
about another living person must satisfy the sensitive-claim rules in section 6.

## 2. Verification states

Every material claim has one state:

- **Unverified** — machine-generated, imported, or proposed but not checked. It may be
  exposed as an eligible machine-transcript lead with the exact warning and independent
  gates above. Publication does not upgrade it into evidence.
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
financial, medical, relationship, legal, or controversy claims require careful
attribution and independent corroboration where available. A machine-only account may
be shown solely as a warned unverified lead; “video checked” is required before the
wiki describes its wording as directly perceived or verified.
“Community media checked” is sufficient only for a narrow direct observation about
the repost itself, not for the truth of its title or an event outside the recording.
“Unverified machine-transcript lead” is not a lower evidentiary shortcut. It can
expose a warned, attributed machine paraphrase and candidate locator after independent
eligibility gates; it cannot assert that the machine output is correct or quote it as
verified speech.

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
citations. It may not promote its own summary into evidence. The exact source and
coordinates must be preserved, and the applicable publication gates and warnings must
be applied. Human perception remains necessary for verified quotations, factual wiki
conclusions, identity decisions, and lifecycle decisions.

NotebookLM drafts in the private research workspace are unverified research indexes.
Their titles, counts, summaries, entity links, and chronology must be re-established
from primary sources and the maintainers' review record before publication.

## 8. Publication checklist

Before publishing or materially expanding a page, confirm that:

- every contestable statement has a corresponding source record and explicit review
  state;
- every citation supports the exact adjacent wording;
- self-reports are attributed;
- dates state their basis and precision;
- contradictory sources are represented fairly;
- a cited transcript segment is called a verified quotation only when a human checked
  it against the media;
- every public machine-only lead gives its exact source, candidate locator,
  raw-media status, canonical warning, and independent gate status;
- a machine-text allegation also carries the adjacent allegation warning,
  corroboration status, and response-search result, and omits disallowed minor-related,
  leaked, diagnostic, harassing, or gratuitously graphic material;
- reviewed reposts have a full-item integrity assessment and retained media hash;
- allegations carry an adjacent warning, attribution, underlying verification state,
  corroboration status, and response note;
- sensitive personal information has been minimized;
- the page shows its verification status and last review date; and
- another pass found no unsupported implications introduced during copy-editing.

Corrections should preserve the reason for the change in version control. Machine
output is corrected append-only rather than silently rewritten. A dispute, retraction,
or reinstatement requires a human lifecycle decision and a public explanation;
automated policy cannot retract text. Material disputes also belong in the private
review record when active public wording changes.
