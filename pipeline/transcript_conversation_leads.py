"""Offline text-only conversation review leads, never a positive speaker screen.

Reads supplied transcripts once; no media, models, provider APIs, or changes to
existing transcripts/campaigns. Text cannot establish that two voices spoke.
"""
from __future__ import annotations

import argparse
from collections import Counter
import hashlib
import os
from pathlib import Path
import re
import time

from pipeline import cloud_transcription_import as imports
from pipeline import transcript_summary as io

KIND = "himr_transcript_conversation_review_leads"
MAX_EXAMPLES = 6
MAX_LEADS = 4000
MAX_CONTEXT_CHARS = 900
SEMANTICS = {
    "text_only_review_leads": True, "positive_speaker_screen": False,
    "calibrated_accuracy_claimed": False, "speaker_identity_inferred": False,
    "retranscription_authorized": False, "automatic_pipeline_routing": False,
    "source_mutation": False, "media_opened": False, "new_paid_requests": 0,
}
RECOUNT = re.compile(r"\b(?:he|she|they|i|we) (?:said|asked|replied|told|was like|were like)\b|"
                     r"\b(?:he|she|they|i|we) (?:would|will|have to|had to|used to) say\b|"
                     r"\b(?:i|he|she|we|they)['’](?:d|ll) say\b|"
                     r"\b(?:for example|imagine|pretend|remember when|back then)\b", re.I)
CHAT = re.compile(r"\b(?:chat (?:says|said|asks|asked)|(?:read|reading) (?:the |your )?(?:chat|comments?)|"
                  r"(?:someone|somebody|viewer) (?:says|said|asks|asked)|super\s?chat|donation|"
                  r"(?:comment|question) from|you guys|(?:tinder|bumble|hinge) conversation|"
                  r"waving emoji|(?:just )?raided you|chat (?:her|him) up|"
                  r"can (?:you|everyone) hear me (?:in|on) (?:the )?chat)\b", re.I)
LYRICS = re.compile(r"\b(?:lyrics|singing|song goes|in the song|reading aloud|quoting)\b", re.I)
LANGUAGE = re.compile(r"\b(?:how (?:do you|to) say|(?:say|saying) hello in|grammatically|"
                      r"self.introduction|language lesson|translation|translate)\b", re.I)
INVITE = re.compile(r"(?:^|[.!?,]\s*)(?:please\s+)?(?:say (?:hi|hello)|introduce yourself|"
                    r"tell (?:us|everyone) your name)\b|"
                    r"\b(?:can|could|would|will) you(?: (?:please|just))? (?:say (?:hi|hello)|introduce yourself)\b|"
                    r"\b(?:do you want to|would you like to) (?:say (?:hi|hello)|introduce yourself)\b", re.I)
INTRO = re.compile(r"\b(?:i(?:'m| am) joined by|joining me (?:today|now)|here with me is|"
                   r"(?:my|our) guest (?:today|is)|thanks? (?:you )?for joining (?:me|us))\b", re.I)
CALL = re.compile(r"\b(?:can you hear me|are you there|can you see me)\b", re.I)
CALL_REPLY = re.compile(r"\b(?:i (?:can (?:hear|see) you|(?:hear|see) you (?:fine|clearly))|"
                        r"loud and clear|(?:yes|yeah),? (?:i['’]m|i am) here)\b", re.I)
GREETING = re.compile(r"^[\s.!,?-]*(?:(?:hi|hello|hey)(?:[.!?,]|$)|"
                      r"(?:hi|hello|hey)\s+(?:everyone|daniel|pia|mila|chihiro)\b|"
                      r"good (?:morning|evening)\b|my name is\b)", re.I)
MEET = re.compile(r"\b(?:nice|good|pleased) to meet you\b", re.I)
MEET_REPLY = re.compile(r"\b(?:you too|likewise|(?:nice|good) to meet you too)\b", re.I)
DIRECT_Q = re.compile(r"\b(?:do you|did you|are you|were you|have you|would you|will you|"
                       r"what (?:do|did|are|would) you|how (?:do|did|are) you|where (?:do|are) you)\b", re.I)
ANSWER = re.compile(r"^[\s.!,?-]*(?:yes|no|yeah|yep|nope|i (?:do|did|am|have|would|will|don['’]t|"
                    r"didn['’]t)|i['’]m|my name is)\b", re.I)
RECIPROCAL = re.compile(r"\b(?:what about you|how about you|and you\s*\?)", re.I)
PERFORMANCE = re.compile(r"\b(?:pet hamster|act like|(?:do|doing) (?:the|an?|my|your) accent|"
                         r"(?:rubbish|fake) accent|(?:silly|funny) voice)\b", re.I)
GUEST = re.compile(r"\b(?:(?:my|your|our) (?:wife|girlfriend|boyfriend|sister|brother)|(?:our|my) guest|"
                   r"i(?:['’]m| am) with|we(?:['’]re| are) with|my name is)\b", re.I)


def _near(segments, left, right, seconds=30):
    start, end = segments[right]["start_ms"], segments[left]["end_ms"]
    return start is not None and end is not None and -1000 <= start - end <= seconds * 1000


def _discounts(text):
    result = []
    for label, pattern in (("reported_or_hypothetical_dialogue", RECOUNT),
                           ("chat_or_audience_reading", CHAT), ("lyrics_or_quotation", LYRICS),
                           ("language_example_or_translation", LANGUAGE),
                           ("performance_or_imitation", PERFORMANCE)):
        if pattern.search(text):
            result.append(label)
    if text.count('"') >= 2 or ("“" in text and "”" in text):
        result.append("quotation_marks")
    return result


def _invitation_at(segments, index):
    # A cue starting "say hello" can continue "I don't know if I should".
    # Reconstruct its preceding clause before accepting a direct invitation.
    prefix = " ".join(item["text"] for item in segments[max(0, index - 2):index])
    joined = (prefix + " " if prefix else "") + segments[index]["text"]
    boundary = len(prefix) + bool(prefix)
    for match in INVITE.finditer(joined):
        if match.end() <= boundary:
            continue
        prior = joined[:match.start()].rstrip().lower()
        if re.search(r"\b(?:why|how)\s*$", prior):
            continue
        return True
    return False


def analyze(parsed):
    """Return bounded text leads; subtitle cue boundaries are not speaker turns."""
    segments = parsed["segments"]
    candidates, counts, discarded, pairs = [], Counter(), Counter(), []

    def add(reason, left, right, score):
        context_left, context_right = max(0, left - 3), min(len(segments), right + 3)
        context = " ".join(item["text"] for item in segments[context_left:context_right])
        discounts = _discounts(context)
        if discounts and reason != "explicit_distinct_speaker_labels":
            discarded.update(discounts)
            return
        if reason == "invitation_and_nearby_greeting" and not GUEST.search(context):
            # Common viewer shout-outs remain possible even without an explicit
            # "chat says" marker. A bare invitation/hello is a weak lead only.
            score = 35
        evidence = [{"cue_index": index, "start_ms": segments[index]["start_ms"],
                     "end_ms": segments[index]["end_ms"], "speaker_label": segments[index]["speaker"],
                     "text": segments[index]["text"][:300]}
                    for index in range(left, min(right + 1, left + 7))]
        counts[reason] += 1
        candidates.append({"reason": reason, "score": score, "first_cue_index": left,
            "last_cue_index": right, "evidence": evidence,
            "context": context[:MAX_CONTEXT_CHARS], "discounts": discounts})

    for index, segment in enumerate(segments):
        text = segment["text"]
        label = segment["speaker"]
        if index and label is not None and segments[index - 1]["speaker"] not in {None, label}:
            add("explicit_distinct_speaker_labels", index - 1, index, 100)
        # Match at most three neighboring subtitle cues because ASR exports often
        # split even a short question mid-sentence. Never infer an actual turn.
        window = segments[index:min(index + 3, len(segments))]
        combined = " ".join(item["text"] for item in window)
        triggers = []
        if _invitation_at(segments, index):
            triggers.append(("invitation_and_nearby_greeting", GREETING, 75))
        if INTRO.search(text):
            triggers.append(("guest_introduction_and_nearby_reply", GREETING, 75))
        if CALL.search(text):
            triggers.append(("call_check_and_nearby_response", CALL_REPLY, 80))
        if MEET.search(text):
            triggers.append(("reciprocal_meeting_greetings", MEET_REPLY, 65))
        for reason, response, score in triggers:
            for other in range(index + 1, min(index + 7, len(segments))):
                if _near(segments, index, other) and response.search(segments[other]["text"]):
                    add(reason, index, other, score)
                    break
        if DIRECT_Q.search(combined) and "?" in combined:
            question_end = next((index + offset for offset, item in enumerate(window) if "?" in item["text"]), index)
            for other in range(question_end + 1, min(question_end + 3, len(segments))):
                if _near(segments, question_end, other, 15) and ANSWER.search(segments[other]["text"]):
                    if not pairs or question_end != pairs[-1][1]:
                        pairs.append((index, question_end, other))
                    break
    # Generic question/answer syntax is too common in monologues. Require a
    # compact cluster plus reciprocal address, and still rank it as weak only.
    for position, (left, _, right) in enumerate(pairs):
        cluster = [pair for pair in pairs[position:position + 6]
                   if _near(segments, left, pair[2], 120)]
        if len(cluster) < 3:
            continue
        end = cluster[-1][2]
        text = " ".join(item["text"] for item in segments[left:end + 1])
        if RECIPROCAL.search(text):
            add("reciprocal_question_answer_cluster", left, end, 35)
            break
    candidates.sort(key=lambda item: (-item["score"], item["first_cue_index"]))
    retained, covered = [], set()
    for candidate in candidates:
        # Overlapping patterns do not constitute independent proof.
        key = candidate["first_cue_index"]
        if any(abs(key - prior) < 5 for prior in covered):
            continue
        retained.append(candidate)
        covered.add(key)
        if len(retained) >= MAX_EXAMPLES:
            break
    if not retained:
        return {"tier": None, "score": 0, "reasons": {}, "examples": [],
                "discounted_candidate_reasons": dict(discarded)}
    score = retained[0]["score"]
    tier = "strong_text_lead" if score >= 75 else "moderate_text_lead" if score >= 60 else "weak_text_lead"
    return {"tier": tier, "score": score, "reasons": dict(counts), "examples": retained,
            "discounted_candidate_reasons": dict(discarded)}


def _paths(root):
    root = imports._path(root)
    if not root.is_dir():
        raise imports.ImportError("transcript root must be a directory")
    paths = []
    def failed(error):
        raise imports.ImportError("cannot inspect entire transcript tree") from error
    for folder, directories, files in os.walk(root, followlinks=False, onerror=failed):
        if any((Path(folder) / name).is_symlink() for name in directories):
            raise imports.ImportError("transcript tree contains a symlinked directory")
        paths.extend(Path(folder) / name for name in files if Path(name).suffix.lower() in {".txt", ".srt"})
        if len(paths) > imports.MAX_FILES:
            raise imports.ImportError("transcript file count exceeds bound")
    return sorted(paths)


def scan(root, *, inventory_ref=None):
    started, total = time.monotonic(), 0
    entries, leads, failures, discounted = [], [], [], Counter()
    for path in _paths(root):
        raw = imports._read(path)
        total += len(raw)
        if total > imports.MAX_TOTAL_BYTES:
            raise imports.ImportError("transcript bytes exceed scan bound")
        entry = {"path": str(path), "sha256": hashlib.sha256(raw).hexdigest(),
                 "byte_count": len(raw), "identity_keys": imports.identity_keys(path.name)}
        try:
            parsed = imports.parse_transcript(raw)
        except imports.ImportError as error:
            failures.append({**entry, "reason": str(error)})
            # Keep malformed candidates in exact-identity collision checks;
            # dropping one could falsely make its conflicting peer eligible.
            entries.append({**entry, "format": "invalid", "issues": ["malformed_transcript"],
                "status": "review_required", "last_end_ms": None, "content_sha256": None})
            continue
        segments = parsed["segments"]
        ends = [item["end_ms"] for item in segments if item["end_ms"] is not None]
        entry.update({"format": parsed["format"], "issues": parsed["issues"],
            "status": "review_required" if parsed["issues"] else "eligible",
            "last_end_ms": max(ends) if ends else None,
            "content_sha256": hashlib.sha256(imports._canonical(segments)).hexdigest()})
        entries.append(entry)
        result = analyze(parsed)
        discounted.update(result["discounted_candidate_reasons"])
        if result["tier"]:
            leads.append({"source": entry, "title": path.stem, **result})
    mapping = {}
    mapping_counts = {}
    if inventory_ref:
        inventory = io.read(inventory_ref)
        if inventory.get("kind") != "himr_cloud_transcription_archive_inventory":
            raise RuntimeError("expected canonical cloud archive inventory")
        recordings = inventory["recordings"]
        matches = imports.match_recordings(recordings, entries)
        mapping_counts = dict(Counter(match["status"] for match in matches))
        for recording, match in zip(recordings, matches, strict=True):
            for candidate in match["candidates"]:
                selected = match["status"] == "selected" and candidate == match["selected"]
                mapping.setdefault(candidate["path"], []).append({
                    "recording_id": recording["recording_id"], "title": recording.get("title"),
                    "match_status": match["status"], "selected_exact_match": selected,
                    "matched_keys": match["matched_keys"], "issues": match["issues"]})
    for lead in leads:
        lead["recording_mapping"] = mapping.get(lead["source"]["path"], [])
        lead["exact_selected_recording_ids"] = sorted({row["recording_id"] for row in lead["recording_mapping"]
                                                        if row["selected_exact_match"]})
    leads.sort(key=lambda row: (-row["score"], -len(row["examples"]), row["source"]["path"]))
    counts = dict(Counter(lead["tier"] for lead in leads))
    return {"kind": KIND, "schema_version": 1, "source_root": str(Path(root).absolute()),
        "implementation_sha256": hashlib.sha256(Path(__file__).read_bytes()).hexdigest(),
        "semantics": SEMANTICS, "inventory": inventory_ref,
        "counts": {"files_read": len(entries), "parsed_files": len(entries) - len(failures),
            "malformed_files": len(failures), "total_bytes_read": total, "lead_files": len(leads),
            "retained_lead_files": min(len(leads), MAX_LEADS), **counts},
        "recording_match_counts": mapping_counts, "discounted_candidate_reasons": dict(discounted),
        "leads": leads[:MAX_LEADS], "malformed_sources": failures,
        "elapsed_seconds": round(time.monotonic() - started, 3),
        "caveats": ["These are text review leads, not detected speakers or retranscription permission.",
            "One voice can read both sides of a dialogue; short subtitle cues are not speaker turns.",
            "Chat reading, reported speech, embedded clips, and unmarked quotations can remain false positives.",
            "No lead is not evidence of a single speaker; brief replies and untranscribed voices can be missed.",
            "Confidence tiers are heuristic priorities, not calibrated probabilities."]}


def readable(report):
    lines = ["# Transcript conversation review leads", "", "Text-only, offline; no positive speaker claims or automatic retranscription.",
             "", f"Scanned {report['counts']['files_read']} files; found {report['counts']['lead_files']} candidate files.", ""]
    for ordinal, lead in enumerate(report["leads"][:20], 1):
        lines.extend([f"{ordinal}. {lead['title']} — {lead['tier']}",
            "   Reasons: " + ", ".join(lead["reasons"]),
            "   Exact selected recordings: " + (", ".join(lead["exact_selected_recording_ids"]) or "none; retain identity review"),
            "   Source: " + lead["source"]["path"], "   SHA-256: " + lead["source"]["sha256"]])
        for example in lead["examples"][:2]:
            evidence = example["evidence"][0]
            lines.append(f"   Cue {evidence['cue_index']} ({evidence['start_ms']} ms): " + example["context"].replace("\n", " "))
        lines.append("")
    lines.extend(["## Limitations", "", *["- " + note for note in report["caveats"]], ""])
    return "\n".join(lines)


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--transcript-root", required=True)
    parser.add_argument("--inventory")
    parser.add_argument("--inventory-sha256")
    parser.add_argument("--output-root", required=True)
    args = parser.parse_args(argv)
    if bool(args.inventory) != bool(args.inventory_sha256):
        parser.error("inventory path and exact hash must be supplied together")
    reference = None if not args.inventory else {"path": args.inventory, "sha256": args.inventory_sha256}
    output = io.safe.path_value(args.output_root)
    io.protect(output, {"transcripts": str(Path(args.transcript_root).absolute()), "inventory": reference})
    if io.safe.exists(output):
        raise RuntimeError("lead output root must be fresh")
    report = scan(args.transcript_root, inventory_ref=reference)
    io.mkdir(output)
    result = io.put(output / "report.json", report)
    summary = io.put_bytes(output / "TOP-20.md", readable(report).encode())
    print(io.canonical({"report": result, "readable": summary, "counts": report["counts"],
                        "elapsed_seconds": report["elapsed_seconds"], "new_paid_requests": 0}).decode().strip())
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
