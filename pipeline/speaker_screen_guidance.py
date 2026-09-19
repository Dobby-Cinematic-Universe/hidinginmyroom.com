"""Private, hash-bound title/date/timestamp hints; never speaker evidence.

Reads only explicitly supplied metadata. Does not open media, discover files,
download transcripts, infer identities, or change the source/acquisition catalog.
"""
from __future__ import annotations

import argparse
from datetime import date, datetime
import json
import os
from pathlib import Path
import re
import sys
import time
import unicodedata

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))
from pipeline import speaker_screen as screen
from pipeline import speaker_screen_batch as batch

ScreenError = screen.ScreenError
MAX_METADATA_BYTES = 128 * 1024**2
MAX_FILE_BYTES = screen.MAX_JSON
MAX_SEGMENTS = 20000
MAX_TARGETS = 32
TITLE_RULES = (
    ("interview", r"\binterview(?:ing|s)?\b", 30),
    ("collaboration", r"\bcollab(?:oration)?s?\b", 30),
    ("conversation", r"\b(?:conversation|chatting|talking|call) with\b", 25),
    ("guest", r"\bguests?\b", 20),
    ("with", r"\bwith\b", 5),
)
CUE_RULES = (
    ("introduction", r"\b(?:joining me|join us|say hello|introduce (?:you|our)|welcome (?:our|my) guest)\b"),
    ("conversation", r"\b(?:on (?:the|this) call|interview with|speaking with|talking with|chatting with)\b"),
    ("guest", r"\b(?:guest arrives|guest joins|guest introduction|guest interview)\b"),
)


def _text(value, label, maximum=1000, *, empty=True):
    if not isinstance(value, str) or len(value) > maximum or (not empty and not value.strip()):
        raise ScreenError(f"invalid {label}")
    if any(ord(c) < 32 and c not in "\t\n\r" for c in value):
        raise ScreenError(f"control character in {label}")
    return value


def _date(value, label):
    if not isinstance(value, str) or not re.fullmatch(r"\d{4}-\d{2}-\d{2}", value):
        raise ScreenError(f"invalid {label}")
    try:
        return date.fromisoformat(value)
    except ValueError as error:
        raise ScreenError(f"invalid {label}") from error


def _sha(value, label):
    if not isinstance(value, str) or not screen.SHA.fullmatch(value):
        raise ScreenError(f"invalid {label}")
    return value


def _version(value, kind, fields):
    screen.exact(value, fields, kind)
    if value["kind"] != kind or type(value["schema_version"]) is not int or value["schema_version"] != 1:
        raise ScreenError(f"unsupported {kind}")


class MetadataReader:
    """Per-load bounded evidence cache; duplicate paths cannot change hashes."""
    def __init__(self):
        self.bindings = {}
        self.documents = {}
        self.total_bytes = 0

    def verify(self, binding):
        screen.file_binding(binding)
        key = binding["path"]
        if key in self.bindings:
            if self.bindings[key] != binding:
                raise ScreenError("metadata path has conflicting hashes")
            return
        with screen.opened(key) as descriptor:
            size = os.fstat(descriptor).st_size
            if not 0 < size <= MAX_FILE_BYTES or self.total_bytes + size > MAX_METADATA_BYTES:
                raise ScreenError("guidance metadata exceeds bounded byte budget")
            if screen.hash_fd(descriptor, MAX_FILE_BYTES, time.monotonic() + 30) != binding["sha256"]:
                raise ScreenError("guidance evidence SHA-256 mismatch")
        self.total_bytes += size
        self.bindings[key] = dict(binding)

    def read(self, binding):
        self.verify(binding)
        key = binding["path"]
        if key not in self.documents:
            self.documents[key] = screen.read_json(Path(key), binding["sha256"])
        return self.documents[key]


def witness_sources(source_bindings):
    """Hash once at run start and retain descriptor metadata for cheap checks."""
    reader = MetadataReader()
    result = []
    for binding in source_bindings:
        reader.verify(binding)
        with screen.opened(binding["path"]) as descriptor:
            before = screen.witness(descriptor)
            if screen.hash_fd(descriptor, MAX_FILE_BYTES, time.monotonic() + 30) != binding["sha256"]:
                raise ScreenError("guidance changed before execution")
            result.append({"binding": dict(binding), "witness": before})
    return result


def check_sources(witnesses):
    for value in witnesses:
        with screen.opened(value["binding"]["path"]) as descriptor:
            if screen.witness(descriptor) != value["witness"]:
                raise ScreenError("guidance evidence changed during execution")


def _acquisition_metadata(binding, recording, reader):
    value = reader.read(binding)
    if (type(value.get("schema_version")) is not int or value["schema_version"] != 1
            or value.get("status") != "completed" or value.get("dry_run") is not False
            or value.get("errors") != []):
        raise ScreenError("guidance requires a completed acquisition result")
    catalog = value.get("catalog_records")
    if not isinstance(catalog, dict) or not isinstance(catalog.get("media_objects"), list):
        raise ScreenError("acquisition result has no media identity")
    matches = [row for row in catalog["media_objects"] if isinstance(row, dict)
               and row.get("media_id") == recording["media_id"]]
    if (len(matches) != 1 or matches[0].get("sha256") != recording["sha256"]
            or type(matches[0].get("byte_count")) is not int
            or matches[0]["byte_count"] != recording["byte_count"]):
        raise ScreenError("acquisition metadata belongs to another media object")
    source = value.get("source")
    if not isinstance(source, dict):
        raise ScreenError("acquisition result source is missing")
    supplied_title = source.get("title")
    title = _text("" if supplied_title is None else supplied_title, "source title")
    native_id = _text(source.get("native_id"), "source native_id", 4096, empty=False)
    # Archive upload/mtime/rsync times are NEVER recording dates. Filename dates
    # remain explicitly lower-confidence labels; zero/invalid dates stay unknown.
    labeled_date = {"value": None, "basis": "unknown"}
    match = re.match(r"^(\d{4})(\d{2})(\d{2})-", native_id.rsplit("/", 1)[-1])
    if match:
        candidate = "-".join(match.groups())
        try:
            _date(candidate, "filename date")
            labeled_date = {"value": candidate, "basis": "filename_date"}
        except ScreenError:
            pass
    elif source.get("published_at") is not None:
        published = _text(source["published_at"], "published timestamp", 100)
        try:
            parsed = datetime.fromisoformat(published.replace("Z", "+00:00"))
            if parsed.tzinfo is None:
                raise ValueError("missing timezone")
            labeled_date = {"value": parsed.date().isoformat(), "basis": "upload_date"}
        except ValueError as error:
            raise ScreenError("invalid source published timestamp") from error
    return title, labeled_date, {"basis": "acquisition_source_metadata", "source_native_id": native_id,
                                  "binding": binding, "provider_metadata_is_content_truth": False}


def _events(bindings, reader):
    if not isinstance(bindings, list) or len(bindings) > 32:
        raise ScreenError("guidance allows at most 32 explicitly reviewed events")
    events, seen = [], set()
    for binding in bindings:
        value = reader.read(binding)
        _version(value, "himr_speaker_screen_reviewed_event", {"kind", "schema_version", "event_id", "date",
                 "radius_days", "reviewed", "reviewer", "evidence"})
        event_id = _text(value["event_id"], "event_id", 128, empty=False)
        if event_id in seen:
            raise ScreenError("duplicate reviewed event")
        seen.add(event_id)
        _date(value["date"], "event date")
        screen.integer(value["radius_days"], 0, 14, "event radius_days")
        if value["reviewed"] is not True:
            raise ScreenError("event proximity requires explicit reviewed evidence")
        _text(value["reviewer"], "event reviewer", 128, empty=False)
        reader.verify(value["evidence"])
        events.append({**value, "binding": binding})
    return sorted(events, key=lambda row: row["event_id"])


def _timed_targets(binding, recording, reader):
    value = reader.read(binding)
    _version(value, "himr_speaker_screen_timed_cues", {"kind", "schema_version", "media_id", "media_sha256",
             "origin", "evidence", "alignment", "segments"})
    if value["media_id"] != recording["media_id"] or value["media_sha256"] != recording["sha256"]:
        raise ScreenError("timed cues belong to another media object")
    if value["origin"] not in ("local_asr", "provider_chapters", "third_party", "manual_review"):
        raise ScreenError("unknown timed-cue origin")
    reader.verify(value["evidence"])
    alignment = value["alignment"]
    screen.exact(alignment, {"basis", "source_media_sha256", "source_duration_ms", "offset_ms",
                            "reviewed", "review_evidence"}, "cue alignment")
    _sha(alignment["source_media_sha256"], "cue source media SHA-256")
    screen.integer(alignment["source_duration_ms"], 1, 86400000, "cue source duration")
    screen.integer(alignment["offset_ms"], -86400000, 86400000, "cue alignment offset")
    if type(alignment["reviewed"]) is not bool:
        raise ScreenError("alignment reviewed must be boolean")
    # A content hash proves which text was supplied, not that its timestamps
    # describe this raw audio. Until a source-specific ASR mapping importer can
    # verify that relationship, every capsule needs an explicit attestation.
    if alignment["reviewed"] is not True or alignment["review_evidence"] is None:
        raise ScreenError("all timed cues require an explicit reviewed alignment attestation")
    if alignment["basis"] == "same_media":
        if (alignment["source_media_sha256"] != recording["sha256"] or alignment["offset_ms"] != 0
                or alignment["source_duration_ms"] != recording["duration_ms"]):
            raise ScreenError("same-media cue timeline differs from exact screen input")
    elif alignment["basis"] == "reviewed_offset":
        if alignment["reviewed"] is not True or alignment["review_evidence"] is None:
            raise ScreenError("offset mapping requires explicit alignment review")
    else:
        raise ScreenError("unsupported cue alignment; edited/drifting timelines need separate mapping")
    if value["origin"] in ("third_party", "manual_review") and (
            alignment["reviewed"] is not True or alignment["review_evidence"] is None):
        raise ScreenError("third-party/manual cues require explicit mapping review")
    if alignment["review_evidence"] is not None:
        reader.verify(alignment["review_evidence"])
    segments = value["segments"]
    if not isinstance(segments, list) or len(segments) > MAX_SEGMENTS:
        raise ScreenError("timed cue segment count exceeds limit")
    selected, previous, ids = [], -1, set()
    for segment in segments:
        screen.exact(segment, {"cue_id", "kind", "start_ms", "end_ms", "text"}, "timed cue segment")
        cue_id = _text(segment["cue_id"], "cue_id", 128, empty=False)
        if not screen.IDENTIFIER.fullmatch(cue_id) or cue_id in ids:
            raise ScreenError("duplicate or invalid timed cue id")
        ids.add(cue_id)
        if segment["kind"] not in ("transcript", "chapter", "reviewed_interval"):
            raise ScreenError("unsupported timed cue kind")
        screen.integer(segment["start_ms"], 0, alignment["source_duration_ms"] - 1, "cue start")
        screen.integer(segment["end_ms"], segment["start_ms"] + 1, alignment["source_duration_ms"], "cue end")
        if segment["start_ms"] < previous:
            raise ScreenError("timed cue segments must be chronological")
        previous = segment["start_ms"]
        content = _text(segment["text"], "cue text", 4000)
        start, end = segment["start_ms"] + alignment["offset_ms"], segment["end_ms"] + alignment["offset_ms"]
        if not 0 <= start < end <= recording["duration_ms"]:
            raise ScreenError("mapped cue lies outside the exact media timeline")
        normalized = unicodedata.normalize("NFKC", content).casefold()
        rules = [name for name, pattern in CUE_RULES if re.search(pattern, normalized)]
        if segment["kind"] == "chapter":
            rules += [name for name, pattern, _ in TITLE_RULES[:-1] if re.search(pattern, normalized)]
        if segment["kind"] == "reviewed_interval":
            if alignment["reviewed"] is not True or alignment["review_evidence"] is None:
                raise ScreenError("manual interval requires a review attestation")
            rules.append("reviewed_interval")
        if rules:
            selected.append({"hint_id": cue_id, "start_ms": start, "end_ms": end,
                             "reason_codes": sorted(set(rules)), "kind": segment["kind"]})
    # Choose spread-out hints if a transcript is unusually cue-dense; never a
    # capped prefix that quietly leaves the end of a long stream unrepresented.
    count = len(selected)
    if count > MAX_TARGETS:
        selected = [selected[index * (count - 1) // (MAX_TARGETS - 1)] for index in range(MAX_TARGETS)]
    return [{key: row[key] for key in ("hint_id", "start_ms", "end_ms")} for row in selected], {
        "binding": binding, "origin": value["origin"], "alignment": alignment,
        "matched_cues": count, "selected_cues": selected, "omitted_cues": count - len(selected),
        "selection": "chronologically_spread_capped_hints", "text_is_speaker_evidence": False,
        "alignment_authority": "explicit_operator_attestation_not_inferred_from_hash",
        "cue_text_is_automatically_verified_against_evidence": False}


def load_guidance(binding, original_plans):
    reader = MetadataReader()
    value = reader.read(binding)
    _version(value, "himr_speaker_screen_guidance", {"kind", "schema_version", "records", "events"})
    if not isinstance(original_plans, list) or not 1 <= len(original_plans) <= batch.MAX_JOBS:
        raise ScreenError("guidance requires 1..128 explicit recording plans")
    known = {plan["order"]["recording"]["media_id"]: plan["order"]["recording"] for plan in original_plans}
    if len(known) != len(original_plans):
        raise ScreenError("duplicate guidance recording identity")
    if not isinstance(value["records"], list) or len(value["records"]) > batch.MAX_JOBS:
        raise ScreenError("guidance records exceed bounded size")
    records = {}
    for row in value["records"]:
        screen.exact(row, {"media_id", "media_sha256", "acquisition_result", "timed_cues"}, "guidance record")
        media_id = _text(row["media_id"], "guidance media_id", 256, empty=False)
        if media_id in records or media_id not in known or row["media_sha256"] != known[media_id]["sha256"]:
            raise ScreenError("guidance has unknown, repeated, or mismatched media")
        records[media_id] = row
    events = _events(value["events"], reader)
    output = []
    for plan in original_plans:
        recording = plan["order"]["recording"]
        row = records.get(recording["media_id"])
        title, labeled_date, acquisition = "", {"value": None, "basis": "unknown"}, None
        targets, timed = [], None
        if row is not None and row["acquisition_result"] is not None:
            title, labeled_date, acquisition = _acquisition_metadata(row["acquisition_result"], recording, reader)
        if row is not None and row["timed_cues"] is not None:
            targets, timed = _timed_targets(row["timed_cues"], recording, reader)
        reasons, score = [], 0
        normalized = unicodedata.normalize("NFKC", title).casefold()
        for name, pattern, points in TITLE_RULES:
            if re.search(pattern, normalized):
                reasons.append("title:" + name)
                score += points
        score = min(score, 60)
        nearby = []
        if labeled_date["value"] is not None:
            day = _date(labeled_date["value"], "recording date label")
            for event in events:
                distance = abs((day - _date(event["date"], "reviewed event date")).days)
                if distance <= event["radius_days"]:
                    nearby.append({"event_id": event["event_id"], "distance_days": distance,
                                   "date_basis": labeled_date["basis"]})
                    reasons.append("date_proximity:" + event["event_id"])
        if nearby:
            score += 5  # Weak upload/filename dates never outweigh acoustic evidence.
        if targets:
            reasons.append("aligned_timed_cues")
            score += 20
        output.append({"media_id": recording["media_id"], "media_sha256": recording["sha256"],
                       "title": title, "date": labeled_date, "priority": {"score": score, "reasons": reasons},
                       "targets": targets, "acquisition": acquisition, "timed": timed,
                       "nearby_reviewed_events": nearby, "metadata_is_speaker_evidence": False,
                       "missing_metadata_does_not_skip_recording": True})
    return {"kind": "himr_speaker_screen_normalized_guidance", "schema_version": 1,
            "recordings": output, "source_bindings": [reader.bindings[key] for key in sorted(reader.bindings)],
            "metadata_bytes": reader.total_bytes, "events": events,
            "semantics": {"priority_is_probability": False, "identity_inferred": False,
                          "titles_are_instructions": False, "publication_authority": False,
                          "filesystem_dates_used": False, "baseline_may_be_reduced": False}}


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--guidance", required=True)
    parser.add_argument("--expected-sha256", required=True)
    parser.add_argument("--orders", required=True, help="JSON object containing work_orders references")
    parser.add_argument("--orders-sha256", required=True)
    args = parser.parse_args(argv)
    try:
        orders = screen.read_json(screen.path_value(args.orders), args.orders_sha256)
        screen.exact(orders, {"work_orders"}, "guidance inspection orders")
        plans = batch._load_orders(orders["work_orders"])
        result = load_guidance({"path": args.guidance, "sha256": args.expected_sha256}, plans)
        # Inspection is metadata-only and does not create a plan or launch workers.
        print(json.dumps(result, indent=2, sort_keys=True, allow_nan=False))
        return 0
    except (ScreenError, OSError, ValueError, RuntimeError) as error:
        print(f"SpeakerScreenGuidanceError: {error}", file=sys.stderr)
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
