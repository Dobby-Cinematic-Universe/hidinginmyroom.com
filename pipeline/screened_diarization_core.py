"""Pure, strict recording-local contracts for screened Community-1 diarization.

Metadata and reviewed bounds never supply turns or identities. Ordinary
overlap-aware output and exclusive transcript-alignment output stay separate.
No file access, model loading, chunk stitching or publication occurs here.
"""
from __future__ import annotations

from bisect import bisect_right
from datetime import datetime, timezone
from decimal import Decimal, InvalidOperation, ROUND_CEILING, ROUND_FLOOR, ROUND_HALF_UP
import json
import math
from pathlib import PurePosixPath
import re

MAX_DURATION_MS = 86_400_000
MAX_TURNS = 100_000
MAX_SPEAKERS = 256
MAX_DOCUMENT_BYTES = 16 * 1024**2
EDGE_TOLERANCE_MS = 1
FLOAT_ROUNDOFF_MS = Decimal("0.000001")
FLOAT_ROUNDOFF_SECONDS = FLOAT_ROUNDOFF_MS / 1000
RECIPE = "recording-local-outward-ms-exclusive-shared-boundary-v1"
SHA = re.compile(r"[0-9a-f]{64}")
RUN_ID = re.compile(r"diarjob_[0-9a-f]{32}")


class DiarizationError(RuntimeError):
    pass


def _exact(value, fields, label):
    if not isinstance(value, dict) or set(value) != set(fields):
        raise DiarizationError(label + " fields differ")


def _integer(value, low, high, label):
    if type(value) is not int or not low <= value <= high:
        raise DiarizationError(f"{label} must be an integer in {low}..{high}")
    return value


def _text(value, maximum, label):
    if (not isinstance(value, str) or not 1 <= len(value) <= maximum or not value.isprintable()
            or value != value.strip()):
        raise DiarizationError("invalid " + label)
    return value


def _sha(value):
    if not isinstance(value, str) or SHA.fullmatch(value) is None:
        raise DiarizationError("invalid media or evidence SHA-256")
    return value


def _binding(value):
    _exact(value, {"path", "sha256"}, "review evidence binding")
    path = _text(value["path"], 4096, "review evidence path")
    parsed = PurePosixPath(path)
    if (not parsed.is_absolute() or path == "/" or str(parsed) != path or "//" in path
            or ".." in parsed.parts or "\\" in path or len(path.encode("utf-8")) > 4096):
        raise DiarizationError("review evidence requires a normalized absolute non-root path")
    return {"path": path, "sha256": _sha(value["sha256"])}


def validate_speaker_bounds(value, *, media_sha256):
    """Default to automatic; explicit count bounds require a bound human review.

    The runner must hash-check the referenced evidence before planning and
    execution. This pure function validates the attestation contract, not whether
    a human actually listened. No title, screen result or likely-count guess is
    accepted in place of a direct-media review attestation.
    """
    _sha(media_sha256)
    if value is None:
        return {"parameters": {}, "review": None}
    _exact(value, {"parameters", "review"}, "speaker bounds")
    parameters = value["parameters"]
    allowed = {"num_speakers", "min_speakers", "max_speakers"}
    if not isinstance(parameters, dict) or set(parameters) - allowed:
        raise DiarizationError("unknown speaker-count parameters")
    parameters = dict(parameters)
    if not parameters:
        if value["review"] is not None:
            raise DiarizationError("automatic speaker count cannot carry an unused bound review")
        return {"parameters": {}, "review": None}
    for key, count in parameters.items():
        _integer(count, 1, MAX_SPEAKERS, key)
    if "num_speakers" in parameters and len(parameters) != 1:
        raise DiarizationError("num_speakers cannot be combined with min/max bounds")
    if parameters.get("min_speakers", 1) > parameters.get("max_speakers", MAX_SPEAKERS):
        raise DiarizationError("minimum speaker count exceeds maximum")
    review = value["review"]
    _exact(review, {"basis", "media_sha256", "reviewer", "reviewed_at", "evidence"}, "speaker-bound review")
    if review["basis"] != "direct_media_human_review" or review["media_sha256"] != media_sha256:
        raise DiarizationError("speaker bounds require direct-media human review of this exact source")
    reviewer = _text(review["reviewer"], 256, "human reviewer")
    timestamp = _text(review["reviewed_at"], 64, "review timestamp")
    if re.fullmatch(r"[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}(?:\.[0-9]{1,6})?(?:Z|[+-][0-9]{2}:[0-9]{2})", timestamp) is None:
        raise DiarizationError("review timestamp requires an explicit timezone")
    try:
        parsed = datetime.fromisoformat(timestamp.replace("Z", "+00:00"))
        timestamp = parsed.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")
    except ValueError as error:
        raise DiarizationError("invalid review timestamp") from error
    return {"parameters": parameters, "review": {"basis": review["basis"], "media_sha256": media_sha256,
            "reviewer": reviewer, "reviewed_at": timestamp, "evidence": _binding(review["evidence"])}}


def _seconds(value, *, decimal_text=False):
    if decimal_text:
        if not isinstance(value, str) or not 1 <= len(value) <= 400:
            raise DiarizationError("invalid retained raw timing")
        try:
            result = Decimal(value)
        except InvalidOperation as error:
            raise DiarizationError("invalid retained raw timing") from error
        if not result.is_finite() or str(result) != value:
            raise DiarizationError("retained raw timing is nonfinite or noncanonical")
        return result
    if type(value) not in (int, float) or (type(value) is float and not math.isfinite(value)):
        raise DiarizationError("raw turn times must be finite numeric seconds")
    if type(value) is int and abs(value) > 86401:
        raise DiarizationError("raw turn time exceeds recording bound")
    return Decimal(str(value))


def _raw(rows, duration, *, decimal_text=False):
    if not isinstance(rows, list) or len(rows) > MAX_TURNS:
        raise DiarizationError("diarization representation exceeds 100000 turns")
    result, seen = [], set()
    end_limit = Decimal(duration) / 1000
    tolerance = Decimal(EDGE_TOLERANCE_MS) / 1000
    for row in rows:
        _exact(row, {"start", "end", "speaker"}, "raw diarization turn")
        start, end = _seconds(row["start"], decimal_text=decimal_text), _seconds(row["end"], decimal_text=decimal_text)
        speaker = _text(row["speaker"], 256, "model speaker label")
        if (start >= end or start < -tolerance or end > end_limit + tolerance
                or end <= 0 or start >= end_limit):
            raise DiarizationError("raw turn is empty or outside the explicit recording-edge tolerance")
        identity = (start, end, speaker)
        if identity in seen:
            raise DiarizationError("duplicate raw diarization turn")
        seen.add(identity)
        result.append({"start": start, "end": end, "speaker": speaker})
    return sorted(result, key=lambda row: (row["start"], row["end"], row["speaker"]))


def _quantize(row, duration, representation, index, adjustments):
    result = {"speaker": row["speaker"], "overlap": False, "score_state": "unavailable"}
    for source, field, rounding in (("start", "start_ms", ROUND_FLOOR), ("end", "end_ms", ROUND_CEILING)):
        milliseconds = row[source] * 1000
        nearest = milliseconds.to_integral_value(rounding=ROUND_HALF_UP)
        if milliseconds != nearest and abs(milliseconds - nearest) <= FLOAT_ROUNDOFF_MS:
            adjustments.append({"representation": representation, "turn_index": index,
                "kind": "floating_roundoff_to_integer_ms", "field": field,
                "raw_ms": str(milliseconds), "boundary_ms": int(nearest)})
            milliseconds = nearest
        value = int(milliseconds.to_integral_value(rounding=rounding))
        bounded = min(duration, max(0, value))
        if bounded != value:
            adjustments.append({"representation": representation, "turn_index": index,
                "kind": "recording_edge_quantization", "field": field,
                "before_ms": value, "after_ms": bounded})
        result[field] = bounded
    if result["start_ms"] >= result["end_ms"]:
        raise DiarizationError("turn becomes empty at integer-ms resolution; no silent dropping")
    return result


def _overlaps(rows, duration):
    """Raw-time overlap of distinct anonymous labels; rounding cannot invent it."""
    events = {}
    upper = Decimal(duration) / 1000
    for row in rows:
        for when, delta in ((max(Decimal(0), row["start"]), 1), (min(upper, row["end"]), -1)):
            changes = events.setdefault(when, {})
            changes[row["speaker"]] = changes.get(row["speaker"], 0) + delta
    active, previous, intervals = {}, None, []
    for when in sorted(events):
        if previous is not None and len(active) >= 2 and when - previous > FLOAT_ROUNDOFF_SECONDS:
            if intervals and intervals[-1][1] == previous:
                intervals[-1] = (intervals[-1][0], when)
            else:
                intervals.append((previous, when))
        for speaker, delta in events[when].items():
            count = active.get(speaker, 0) + delta
            if count:
                active[speaker] = count
            else:
                active.pop(speaker, None)
        previous = when
    return intervals


def _union(intervals):
    merged = []
    for start, end in sorted(intervals):
        if merged and start <= merged[-1][1]:
            merged[-1] = (merged[-1][0], max(merged[-1][1], end))
        else:
            merged.append((start, end))
    return merged


def _canonical(value):
    try:
        body = json.dumps(value, sort_keys=True, separators=(",", ":"), allow_nan=False).encode("utf-8")
    except (TypeError, ValueError, RecursionError) as error:
        raise DiarizationError("diarization result must be finite JSON") from error
    if len(body) > MAX_DOCUMENT_BYTES:
        raise DiarizationError("whole-recording diarization result exceeds 16 MiB; silent chunk stitching is forbidden")
    return body


def _normalize(duration_ms, ordinary, exclusive, *, media_sha256, run_id, bounds, decimal_text=False):
    duration = _integer(duration_ms, 1, MAX_DURATION_MS, "recording duration_ms")
    _sha(media_sha256)
    if not isinstance(run_id, str) or RUN_ID.fullmatch(run_id) is None:
        raise DiarizationError("invalid stable recording-local diarization run_id")
    reviewed = validate_speaker_bounds(bounds, media_sha256=media_sha256)
    ordinary = _raw(ordinary, duration, decimal_text=decimal_text)
    exclusive = _raw(exclusive, duration, decimal_text=decimal_text)
    mapping = {}
    for row in ordinary:
        if row["speaker"] not in mapping:
            if len(mapping) >= MAX_SPEAKERS:
                raise DiarizationError("model output exceeds 256 anonymous speaker groups")
            mapping[row["speaker"]] = f"SPEAKER_{len(mapping):04d}"
    if any(row["speaker"] not in mapping for row in exclusive):
        raise DiarizationError("exclusive output references a speaker absent from ordinary output")
    if bool(ordinary) != bool(exclusive):
        raise DiarizationError("ordinary and exclusive output disagree about presence of speech")
    count = len(mapping)
    parameters = reviewed["parameters"]
    if count and (count < parameters.get("min_speakers", 1)
                  or count > parameters.get("max_speakers", MAX_SPEAKERS)
                  or ("num_speakers" in parameters and count != parameters["num_speakers"])):
        raise DiarizationError("nonempty model output violates the explicitly reviewed speaker bound")
    for rows in (ordinary, exclusive):
        for row in rows:
            row["speaker"] = mapping[row["speaker"]]
    for previous, row in zip(exclusive, exclusive[1:]):
        if previous["end"] - row["start"] > FLOAT_ROUNDOFF_SECONDS:
            raise DiarizationError("exclusive output contains genuine overlapping turns")
    adjustments = []
    ordinary_turns = [_quantize(row, duration, "ordinary", index, adjustments) for index, row in enumerate(ordinary)]
    exclusive_turns = [_quantize(row, duration, "exclusive", index, adjustments) for index, row in enumerate(exclusive)]
    for index in range(1, len(exclusive_turns)):
        left, right = exclusive_turns[index - 1], exclusive_turns[index]
        if left["end_ms"] > right["start_ms"]:
            if left["end_ms"] - right["start_ms"] > EDGE_TOLERANCE_MS:
                raise DiarizationError("exclusive quantization collision exceeds one millisecond")
            middle = (exclusive[index - 1]["end"] + exclusive[index]["start"]) * 500
            boundary = int(middle.to_integral_value(rounding=ROUND_HALF_UP))
            if not left["start_ms"] < boundary < right["end_ms"]:
                raise DiarizationError("exclusive quantization would erase a turn; no silent dropping")
            adjustments.append({"representation": "exclusive", "kind": "shared_boundary_quantization",
                "left_turn_index": index - 1, "right_turn_index": index,
                "previous_end_ms": left["end_ms"], "previous_start_ms": right["start_ms"], "boundary_ms": boundary})
            left["end_ms"], right["start_ms"] = boundary, boundary
    overlap = _overlaps(ordinary, duration)
    ends = [end for _start, end in overlap]
    for raw, turn in zip(ordinary, ordinary_turns):
        index = bisect_right(ends, max(Decimal(0), raw["start"]))
        turn["overlap"] = index < len(overlap) and overlap[index][0] < raw["end"]
    overlap_ms = _union([(max(0, int((start * 1000).to_integral_value(rounding=ROUND_FLOOR))),
                           min(duration, int((end * 1000).to_integral_value(rounding=ROUND_CEILING))))
                          for start, end in overlap])
    speech = _union([(row["start_ms"], row["end_ms"]) for row in ordinary_turns])
    provenance = {"recipe": RECIPE, "edge_tolerance_ms": EDGE_TOLERANCE_MS,
        "floating_roundoff_tolerance_ms": str(FLOAT_ROUNDOFF_MS),
        "ordinary": [{"start": str(row["start"]), "end": str(row["end"]), "speaker": row["speaker"]} for row in ordinary],
        "exclusive": [{"start": str(row["start"]), "end": str(row["end"]), "speaker": row["speaker"]} for row in exclusive],
        "rounding_adjustments": adjustments}
    result = {"kind": "himr_screened_diarization_result", "schema_version": 1, "run_id": run_id,
        "recording": {"media_sha256": media_sha256, "duration_ms": duration},
        "speaker_bounds": reviewed, "speakers": list(mapping.values()),
        "representations": {"ordinary": ordinary_turns, "exclusive": exclusive_turns},
        "overlap_intervals": [{"start_ms": start, "end_ms": end} for start, end in overlap_ms],
        "timing_provenance": provenance,
        "summary": {"status": "diarized" if ordinary else "no_speech_turns", "speaker_count": count,
            "ordinary_turns": len(ordinary_turns), "exclusive_turns": len(exclusive_turns),
            "speech_ms": sum(end - start for start, end in speech),
            "overlap_ms": sum(end - start for start, end in overlap_ms),
            "quantization_adjustments": len(adjustments), "score_state": "unavailable"},
        "semantics": {"labels_scope": "run_id_and_recording_media_sha256", "recording_local_labels": True,
            "ordinary_preserves_overlap": True, "exclusive_is_alignment_view": True,
            "overlap_flags_from_raw_distinct_speaker_intersection": True,
            "integer_intervals": "half_open_outward_rounded_except_explicit_exclusive_shared_boundaries",
            "scores_calibrated": False, "score_state": "unavailable", "identity_inferred": False,
            "groups_are_person_counts": False, "human_reviewed_output": False,
            "source_modified": False, "publication_authority": False,
            "whole_recording_input": True, "chunk_stitching": False, "all_speech_detected_claimed": False}}
    _canonical(result)
    return result


def normalize_output(duration_ms, ordinary, exclusive, *, media_sha256, run_id, bounds=None):
    return _normalize(duration_ms, ordinary, exclusive, media_sha256=media_sha256,
                      run_id=run_id, bounds=bounds)


def validate_result(value):
    """Replay retained anonymous raw timing; reject edits to any derived field."""
    _canonical(value)
    if not isinstance(value, dict) or not isinstance(value.get("recording"), dict) or not isinstance(value.get("timing_provenance"), dict):
        raise DiarizationError("invalid diarization result document")
    try:
        timing = value["timing_provenance"]
        expected = _normalize(value["recording"]["duration_ms"], timing["ordinary"], timing["exclusive"],
            media_sha256=value["recording"]["media_sha256"], run_id=value["run_id"],
            bounds=value["speaker_bounds"], decimal_text=True)
    except KeyError as error:
        raise DiarizationError("diarization result lacks required replay provenance") from error
    if _canonical(value) != _canonical(expected):
        raise DiarizationError("diarization result differs from deterministic timing replay")
    return expected
