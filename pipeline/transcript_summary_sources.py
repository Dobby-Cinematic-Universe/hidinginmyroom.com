"""Read-only, hash-bound transcript admission for private summarization.

This adapter reads only explicitly named JSON artifacts. It does not rediscover
media, verify an archive recursively, infer people or dates, or grant rights.
Completed ASR text remains an unreviewed hypothesis, not a verified quotation.
"""
from __future__ import annotations

import copy
from datetime import date as calendar_date
import hashlib
import json
import os
from pathlib import Path
import re

from pipeline import speaker_screen as safe


class SourceError(RuntimeError):
    pass


MAX_JSON_BYTES = 32 * 1024**2
MAX_SEGMENTS = 100_000
MAX_TEXT_CHARACTERS = 16 * 1024**2
MAX_TIME_MS = 366 * 24 * 3600 * 1000
SPEC_KEYS = {"transcript", "format", "recording_id", "title", "date", "completion"}
SOURCE_KEYS = {"kind", "schema_version", "source_id", "transcript", "format",
               "recording_id", "title", "date", "provenance", "segments"}
SEGMENT_KEYS = {"evidence_id", "ordinal", "start_ms", "end_ms", "text", "speaker",
                "timing_basis", "source_ref"}
REF_KEYS = {"collection", "index", "native_segment_id", "span_id", "chunk_id",
            "start_sample", "end_sample", "sample_rate_hz", "speaker_scope",
            "timeline_offset_ms"}
PROVENANCE_KEYS = {"origin", "completion", "completion_evidence", "source_kind",
                   "source_schema_version", "source_identity_sha256", "third_party_source",
                   "rights_granted", "person_identity_inferred", "verified_quotation",
                   "source_mutation", "machine_generated", "date_is_event_date",
                   "unplaced_text_present", "full_media_coverage_verified",
                   "source_completed_coverage_claimed"}
FORMATS = {"longform", "normalized", "salad", "third_party", "cloud"}
TIMING_BASES = {"parent_pcm_samples_presentation_ms", "media_ms",
                "recording_milliseconds", "approximate_provider_recording_ms",
                "third_party_supplied_ms", "unknown"}
DATE_BASES = {"operator_supplied_metadata", "direct_catalogue_metadata"}
SHA = re.compile(r"[0-9a-f]{64}\Z")
SPEAKER = re.compile(r"SPEAKER_[0-9]{4,8}\Z")


def canonical_bytes(value):
    try:
        return (json.dumps(value, ensure_ascii=False, sort_keys=True,
                           separators=(",", ":"), allow_nan=False) + "\n").encode("utf-8")
    except (ValueError, TypeError, UnicodeError, RecursionError) as exc:
        raise SourceError("invalid finite JSON value") from exc


def _hash(value):
    return hashlib.sha256(canonical_bytes(value)).hexdigest()


def _exact(value, keys, label):
    if not isinstance(value, dict) or set(value) != keys:
        raise SourceError(f"{label} fields differ")
    return value


def _integer(value, label, low=0, high=MAX_TIME_MS):
    if type(value) is not int or not low <= value <= high:
        raise SourceError(f"invalid {label}")
    return value


def _text(value, label, *, maximum=MAX_TEXT_CHARACTERS, empty=False):
    if (not isinstance(value, str) or len(value) > maximum or (not empty and not value.strip())
            or any(ord(ch) < 32 and ch not in "\n\r\t" for ch in value)):
        raise SourceError(f"invalid {label}")
    # Reject lone surrogate code points before they can reach an API encoder.
    try:
        value.encode("utf-8")
    except UnicodeError as exc:
        raise SourceError(f"invalid {label} encoding") from exc
    return value


def _binding(value):
    try:
        safe.file_binding(value)
    except (safe.ScreenError, TypeError, ValueError) as exc:
        raise SourceError("invalid source binding") from exc
    return value


def read_json(binding):
    """Read one retained no-symlink, bounded, hash-checked JSON file."""
    _binding(binding)
    try:
        with safe.opened(binding["path"]) as fd:
            before = safe.witness(fd)
            if not 0 < before["st_size"] <= MAX_JSON_BYTES:
                raise SourceError("source JSON exceeds its bound")
            body = os.pread(fd, MAX_JSON_BYTES + 1, 0)
            if len(body) != before["st_size"] or safe.witness(fd) != before:
                raise SourceError("source changed while reading")
    except safe.ScreenError as exc:
        raise SourceError(str(exc)) from exc
    if hashlib.sha256(body).hexdigest() != binding["sha256"]:
        raise SourceError("source SHA-256 mismatch")
    def pairs(items):
        value = {}
        for key, item in items:
            if key in value:
                raise SourceError("duplicate JSON field")
            value[key] = item
        return value
    try:
        value = json.loads(body, object_pairs_hook=pairs,
            parse_constant=lambda _: (_ for _ in ()).throw(SourceError("non-finite JSON")))
    except (ValueError, UnicodeError, RecursionError) as exc:
        raise SourceError("invalid source JSON") from exc
    if not isinstance(value, dict):
        raise SourceError("source JSON must be an object")
    canonical_bytes(value)
    return value


def _date_value(value):
    if not isinstance(value, str) or not re.fullmatch(r"[0-9]{4}-[0-9]{2}-[0-9]{2}", value):
        raise SourceError("date must be an explicit ISO calendar date")
    try:
        calendar_date.fromisoformat(value)
    except ValueError as exc:
        raise SourceError("invalid calendar date") from exc


def validate_spec(value):
    _exact(value, SPEC_KEYS, "summary source specification")
    _binding(value["transcript"])
    if not isinstance(value["format"], str) or value["format"] not in FORMATS:
        raise SourceError("unsupported explicit transcript format")
    _text(value["recording_id"], "recording ID", maximum=256)
    if value["title"] is not None:
        _text(value["title"], "title", maximum=4096)
    if value["completion"] is not None:
        _binding(value["completion"])
    if value["format"] in {"normalized", "cloud"} and value["completion"] is None:
        raise SourceError("normalized ASR needs an explicit completed-result binding")
    if value["format"] not in {"normalized", "cloud"} and value["completion"] is not None:
        raise SourceError("this format carries its own completion evidence")
    if value["date"] is not None:
        _exact(value["date"], {"value", "kind", "evidence"}, "date specification")
        _date_value(value["date"]["value"])
        if value["date"]["kind"] not in ("recorded", "published"):
            raise SourceError("date kind must be recorded or published")
        _binding(value["date"]["evidence"])
    return copy.deepcopy(value)


def _date(spec):
    if spec["date"] is None:
        return {"value": None, "kind": "unknown", "basis": "unknown", "evidence": None}
    requested = spec["date"]
    evidence = read_json(requested["evidence"])
    _exact(evidence, {"kind", "schema_version", "recording_id", "value", "date_kind", "basis"},
           "date evidence")
    if (evidence["kind"] != "himr_summary_date_evidence" or type(evidence["schema_version"]) is not int
            or evidence["schema_version"] != 1 or evidence["recording_id"] != spec["recording_id"]
            or evidence["value"] != requested["value"] or evidence["date_kind"] != requested["kind"]
            or not isinstance(evidence["basis"], str) or evidence["basis"] not in DATE_BASES):
        raise SourceError("date evidence differs from the selected recording/date")
    return {**copy.deepcopy(requested), "basis": evidence["basis"]}


def _identity(document, id_key, prefix):
    identity = document.get("identity_sha256")
    if not isinstance(identity, str) or not SHA.fullmatch(identity):
        raise SourceError("native transcript lacks its semantic identity")
    body = {key: value for key, value in document.items() if key not in {"identity_sha256", id_key}}
    if _hash(body) != identity or document.get(id_key) != prefix + identity[:32]:
        raise SourceError("native document semantic identity differs")
    return identity


def _rows(value, label):
    if not isinstance(value, list) or len(value) > MAX_SEGMENTS:
        raise SourceError(f"{label} must be a bounded array")
    if any(not isinstance(row, dict) for row in value):
        raise SourceError(f"{label} entries must be objects")
    return value


def _pair(start, end, *, nullable=False):
    if start is None or end is None:
        if not nullable or start is not None or end is not None:
            raise SourceError("timestamps must be paired, never fabricated")
    else:
        _integer(start, "timestamp start")
        _integer(end, "timestamp end", low=start)


def _ref(collection, index, **kwargs):
    return {"collection": collection, "index": index, "native_segment_id": None,
            "span_id": None, "chunk_id": None, "start_sample": None, "end_sample": None,
            "sample_rate_hz": None, "speaker_scope": None, "timeline_offset_ms": None, **kwargs}


def _unit(row, ref, timing_basis, *, speaker=None):
    start, end = row.get("start_ms"), row.get("end_ms")
    _pair(start, end, nullable=True)
    return {"ordinal": 0, "start_ms": start, "end_ms": end,
            "text": _text(row.get("text"), "segment text", empty=True),
            "speaker": speaker, "timing_basis": timing_basis if start is not None else "unknown",
            "source_ref": ref}


def _longform(doc, spec):
    if doc.get("kind") != "himr_longform_recording_transcript" or doc.get("schema_version") != 1:
        raise SourceError("unsupported longform transcript")
    identity = _identity(doc, "assembly_id", "lfassembly_")
    if doc.get("recording", {}).get("recording_id") != spec["recording_id"]:
        raise SourceError("longform recording ID differs")
    coverage, counts = doc.get("coverage", {}), doc.get("counts", {})
    sources = _rows(doc.get("sources"), "longform sources")
    if (not sources or coverage.get("complete") is not True
            or type(coverage.get("total_samples")) is not int or coverage["total_samples"] <= 0
            or coverage.get("decoded_samples") != coverage["total_samples"]
            or counts.get("pending_span_count") != 0 or counts.get("failed_span_count") != 0
            or counts.get("completed_span_count") != len(sources)
            or any(row.get("status") != "completed" for row in sources)):
        raise SourceError("longform transcript is not fully completed")
    timeline = doc.get("timeline", {})
    if (timeline.get("coordinate_system") != "parent_pcm_samples_half_open"
            or timeline.get("sample_rate_hz") != 16000 or timeline.get("start_sample") != 0
            or timeline.get("end_sample") != coverage["total_samples"]):
        raise SourceError("longform sample timeline differs")
    units = []
    for i, row in enumerate(_rows(doc.get("segments"), "longform segments")):
        if row.get("ordinal") != i:
            raise SourceError("longform ordinals differ")
        start = _integer(row.get("start_sample"), "sample start", high=coverage["total_samples"])
        end = _integer(row.get("end_sample"), "sample end", low=start, high=coverage["total_samples"])
        if row.get("start_ms") != (start + 8) // 16 or row.get("end_ms") != (end + 8) // 16:
            raise SourceError("longform presentation timestamps disagree with samples")
        segment_id = _text(row.get("segment_id"), "native segment ID", maximum=256)
        span_id = _text(row.get("source", {}).get("span_id"), "span ID", maximum=256)
        units.append(_unit(row, _ref("segments", i, native_segment_id=segment_id, span_id=span_id,
            start_sample=start, end_sample=end, sample_rate_hz=16000), "parent_pcm_samples_presentation_ms"))
    if counts.get("retained_segment_count") != len(units):
        raise SourceError("longform retained segment count differs")
    return units, identity, None, True


def _normalized(doc, spec):
    if not ((doc.get("kind") == "transcript_normalized" and doc.get("schema_version") == 5)
            or (doc.get("kind") == "himr_machine_transcript" and doc.get("schema_version") == 1)):
        raise SourceError("unsupported normalized transcript")
    identity = _identity(doc, "document_id", "gpuasrnorm_")
    receipt = read_json(spec["completion"])
    if (receipt.get("kind") != "himr_faster_whisper_gpu_result" or receipt.get("status") != "completed"
            or type(receipt.get("schema_version")) is not int or not 1 <= receipt["schema_version"] <= 5):
        raise SourceError("ASR completion result is not completed")
    _identity(receipt, "result_id", "gpuasrresult5_" if receipt["schema_version"] == 5 else "gpuasrresult_")
    matches = [row for row in _rows(receipt.get("artifacts"), "result artifacts")
               if row.get("artifact_kind") == "transcript_normalized_json"]
    if len(matches) != 1 or any(matches[0].get(key) != spec["transcript"][key] for key in ("path", "sha256")):
        raise SourceError("ASR result does not bind the selected transcript")
    if "identity_sha256" in matches[0] and matches[0]["identity_sha256"] != identity:
        raise SourceError("ASR result transcript identity differs")
    timeline = doc.get("timeline", {})
    basis = timeline.get("coordinate_system")
    if not isinstance(basis, str) or basis not in {"media_ms", "recording_milliseconds"}:
        raise SourceError("unsupported normalized timeline coordinates")
    offset = _integer(timeline.get("source_offset_ms"), "source timeline offset")
    duration = _integer(timeline.get("source_duration_ms"), "source duration", low=1)
    if timeline.get("end_ms") != offset + duration or (basis == "media_ms" and offset != 0):
        raise SourceError("normalized timeline endpoints differ")
    units = []
    for i, row in enumerate(_rows(doc.get("segments"), "normalized segments")):
        if row.get("ordinal") != i:
            raise SourceError("normalized segment ordinals differ")
        _pair(row.get("start_ms"), row.get("end_ms"))
        if not offset <= row["start_ms"] <= row["end_ms"] <= offset + duration:
            raise SourceError("normalized segment exceeds its source timeline")
        # No upstream named labels are accepted as person identity evidence.
        speaker = row.get("speaker")
        if speaker is not None and (not isinstance(speaker, str) or not SPEAKER.fullmatch(speaker)):
            speaker = None
        units.append(_unit(row, _ref("segments", i, timeline_offset_ms=offset,
            speaker_scope=spec["recording_id"] if speaker else None), basis, speaker=speaker))
    if doc.get("segment_count") != len(units):
        raise SourceError("normalized segment count differs")
    return units, identity, None, False


def _salad(doc, spec):
    if doc.get("kind") != "himr_salad_recording_transcript" or doc.get("schema_version") != 1:
        raise SourceError("unsupported Salad recording transcript")
    identity = _identity(doc, "transcript_id", "saladrecordingtranscript_")
    if doc.get("recording", {}).get("recording_id") != spec["recording_id"]:
        raise SourceError("Salad recording ID differs")
    if doc.get("coverage", {}).get("all_planned_jobs_collected") is not True:
        raise SourceError("Salad transcript has pending jobs")
    if doc.get("timing_semantics", {}).get("coordinate_system") != "recording_relative_milliseconds":
        raise SourceError("unsupported Salad timestamp coordinates")
    source_rows = _rows(doc.get("sources"), "Salad sources")
    chunks = [row.get("chunk_id") for row in source_rows]
    if (not chunks or any(not isinstance(c, str) or not c for c in chunks)
            or len(set(chunks)) != len(chunks)):
        raise SourceError("Salad chunk identities differ")
    arrays = {key: _rows(doc.get(key), f"Salad {key}") for key in ("segments", "words", "unplaced")}
    if any(row.get("chunk_id") not in chunks for rows in arrays.values() for row in rows):
        raise SourceError("Salad unit lacks a collected source chunk")
    units, speaker_map = [], {}
    for chunk in chunks:
        # A chunk-level fallback contains the same text as its segment/word
        # hypotheses. Select one representation rather than duplicate evidence.
        unplaced = [(i, row) for i, row in enumerate(arrays["unplaced"]) if row["chunk_id"] == chunk]
        fallback = [(i, row) for i, row in unplaced if row.get("unit_kind") == "chunk_text"]
        if len(fallback) > 1:
            raise SourceError("Salad repeats a chunk-text fallback")
        if fallback:
            chosen = [("unplaced", i, row) for i, row in fallback]
        else:
            category = "segments" if (any(row["chunk_id"] == chunk for row in arrays["segments"])
                or any(row.get("unit_kind") == "segments" for _, row in unplaced)) else "words"
            chosen = [(category, i, row) for i, row in enumerate(arrays[category]) if row["chunk_id"] == chunk]
            chosen += [("unplaced", i, row) for i, row in unplaced if row.get("unit_kind") == category]
        for collection, i, row in chosen:
            if collection == "unplaced" and (row.get("start_ms") is not None or row.get("end_ms") is not None):
                raise SourceError("unplaced Salad evidence cannot claim timestamps")
            speaker = None
            if row.get("speaker_id") is not None:
                raw_speaker = _text(row["speaker_id"], "provider speaker ID", maximum=256)
                key = (chunk, raw_speaker)
                if key not in speaker_map:
                    speaker_map[key] = f"SPEAKER_{len(speaker_map):04d}"
                speaker = speaker_map[key]
            units.append(_unit(row, _ref(collection, i, chunk_id=chunk,
                speaker_scope=chunk if speaker else None), "approximate_provider_recording_ms", speaker=speaker))
    return units, identity, None, False


def _third_party(doc, spec):
    _exact(doc, {"kind", "schema_version", "recording_id", "status", "provenance", "segments"},
           "third-party transcript import")
    if (doc["kind"] != "himr_third_party_transcript_import" or type(doc["schema_version"]) is not int
            or doc["schema_version"] != 1 or doc["recording_id"] != spec["recording_id"]
            or doc["status"] != "completed"):
        raise SourceError("third-party import is not an explicit completed recording")
    _third_party_provenance(doc["provenance"])
    units = []
    for i, row in enumerate(_rows(doc["segments"], "third-party segments")):
        _exact(row, {"start_ms", "end_ms", "text", "speaker"}, "third-party segment")
        speaker = row["speaker"]
        if speaker is not None and (not isinstance(speaker, str) or not SPEAKER.fullmatch(speaker)):
            raise SourceError("third-party speaker labels must be anonymous")
        units.append(_unit(row, _ref("segments", i, speaker_scope=spec["recording_id"] if speaker else None),
                           "third_party_supplied_ms", speaker=speaker))
    return units, None, copy.deepcopy(doc["provenance"]), False


def _cloud(doc, spec):
    """Replay whole-recording cloud output against its exact retained raw proof."""
    from pipeline import cloud_transcription_client as cloud
    normalized_fields = {"provider", "model", "duration_seconds", "text", "segments",
                         "speaker_labels_are_identities", "diarization_requested", "provider_speaker_labels"}
    fixed_fields = {"kind", "schema_version", "recording_id", "job_id", "source_media", "status",
        "provider_job_id", "raw_result", "provider_job", "screen_decision", "audio",
        "whole_recording_submitted", "machine_generated", "full_media_coverage_verified",
        "human_reviewed", "verified_quotation", "speaker_identity_inferred", "publication_authority",
        "normalizer_implementation_sha256"}
    _exact(doc, fixed_fields | normalized_fields, "whole-recording cloud transcript")
    if (doc["kind"] != "himr_cloud_recording_transcript" or doc["schema_version"] != 1
            or doc["recording_id"] != spec["recording_id"] or doc["status"] != "completed"
            or doc["whole_recording_submitted"] is not True or doc["machine_generated"] is not True
            or any(doc[key] is not False for key in ("full_media_coverage_verified", "human_reviewed",
                "verified_quotation", "speaker_identity_inferred", "publication_authority"))):
        raise SourceError("cloud transcript lacks bounded machine-generated completion semantics")
    normalizer_sha = hashlib.sha256(Path(cloud.__file__).read_bytes()).hexdigest()
    if doc["normalizer_implementation_sha256"] != normalizer_sha:
        raise SourceError("cloud transcript normalizer implementation changed")
    media = _exact(doc["source_media"], {"path", "sha256", "byte_count"}, "original cloud media")
    _binding({key: media[key] for key in ("path", "sha256")})
    _integer(media["byte_count"], "original media bytes", low=1, high=64 * 1024**3)
    if doc["recording_id"] != "media_sha256_" + media["sha256"]:
        raise SourceError("cloud recording and original media identity differ")
    if not isinstance(doc["job_id"], str) or not re.fullmatch(r"cloudjob_[0-9a-f]{32}", doc["job_id"]):
        raise SourceError("invalid cloud job identity")
    completion = read_json(spec["completion"])
    expected = {"kind": "himr_cloud_transcription_completion", "schema_version": 1,
                "job_id": doc["job_id"], "audio": doc["audio"], "raw_result": doc["raw_result"],
                "provider_job": doc["provider_job"], "screen_decision": doc["screen_decision"],
                "transcript": spec["transcript"]}
    if type(completion.get("schema_version")) is not int or completion != expected:
        raise SourceError("cloud completion differs from its transcript/input/provider bindings")
    raw, terminal = read_json(doc["raw_result"]), read_json(doc["provider_job"])
    # The screen is decision evidence, not speaker identity or audio equivalence.
    screen = read_json(doc["screen_decision"])
    if (screen.get("kind") != "himr_cloud_speaker_screen_decision"
            or type(screen.get("schema_version")) is not int or screen["schema_version"] != 1
            or screen.get("recording_id") != doc["recording_id"] or screen.get("media") != media
            or type(screen.get("diarization")) is not bool
            or screen["diarization"] is not doc["diarization_requested"]):
        raise SourceError("cloud screen decision and recording/diarization differ")
    audio = _exact(doc["audio"], {"path", "sha256", "byte_count", "duration_ms",
                                 "sample_rate_hz", "channels", "sample_width_bytes", "frames"},
                   "whole-recording upload audio")
    _binding({key: audio[key] for key in ("path", "sha256")})
    _integer(audio["byte_count"], "upload audio bytes", low=1, high=3 * 1024**3)
    duration_ms = _integer(audio.get("duration_ms"), "whole-recording audio duration", low=1)
    _integer(audio["frames"], "upload audio frames", low=1, high=MAX_TIME_MS * 16)
    if (any(type(audio[key]) is not int for key in ("sample_rate_hz", "channels", "sample_width_bytes"))
            or (audio["sample_rate_hz"], audio["channels"], audio["sample_width_bytes"]) != (16000, 1, 2)
            or (audio["frames"] * 1000 + 8000) // 16000 != duration_ms):
        raise SourceError("cloud audio timing/format metadata differs")
    if doc["provider"] == "assemblyai" and doc["raw_result"] != doc["provider_job"]:
        raise SourceError("AssemblyAI raw transcript must be its retained terminal job")
    try:
        cloud.validate_job(doc["provider"], terminal, expected_job_id=doc["provider_job_id"])
        replay = cloud.normalize_result(doc["provider"], raw, expected_duration_seconds=duration_ms / 1000,
                                       job=terminal, diarization=doc["diarization_requested"])
    except (RuntimeError, ValueError, TypeError, KeyError) as exc:
        raise SourceError("cloud provider transcript failed exact normalization replay") from exc
    if replay != {key: doc[key] for key in normalized_fields}:
        raise SourceError("cloud transcript differs from retained provider text, labels or timing")
    units = []
    for index, segment in enumerate(doc["segments"]):
        speaker = segment["speaker"]
        if speaker is not None and (not isinstance(speaker, str) or not SPEAKER.fullmatch(speaker)):
            raise SourceError("cloud speaker labels must remain anonymous")
        # Segment refs lead to the compact native document and unchanged raw
        # provider proof. Word timing exists only in that raw receipt; the
        # model receives text, not these local segment timestamps.
        units.append(_unit(segment, _ref("segments", index,
            native_segment_id=f"cloud-segment-{index}", speaker_scope=doc["provider_job_id"] if speaker else None),
            "approximate_provider_recording_ms", speaker=speaker))
    return units, spec["transcript"]["sha256"], None, False


def _third_party_provenance(value):
    _exact(value, {"label", "source_url", "attribution", "rights_note"}, "third-party provenance")
    _text(value["label"], "third-party origin", maximum=4096)
    for key in ("source_url", "attribution", "rights_note"):
        if value[key] is not None:
            _text(value[key], f"third-party {key}", maximum=8192)


def _evidence_id(binding, row):
    return "evidence_" + _hash({"transcript_sha256": binding["sha256"],
        **{key: value for key, value in row.items() if key not in {"ordinal", "evidence_id"}}})[:32]


def normalize_source(spec):
    spec = validate_spec(spec)
    document = read_json(spec["transcript"])
    if type(document.get("schema_version")) is not int:
        raise SourceError("transcript schema version must be an integer")
    try:
        units, identity, third_party, full_coverage = {
            "longform": _longform, "normalized": _normalized, "salad": _salad,
            "third_party": _third_party, "cloud": _cloud}[spec["format"]](document, spec)
    except (TypeError, KeyError, AttributeError, ValueError) as exc:
        raise SourceError("malformed native transcript structure") from exc
    if len(units) > MAX_SEGMENTS or sum(len(row["text"]) for row in units) > MAX_TEXT_CHARACTERS:
        raise SourceError("normalized transcript exceeds its bounds")
    if (isinstance(document.get("text"), str) and document["text"].strip()
            and not any(row["text"].strip() for row in units)):
        raise SourceError("nonempty transcript text lacks citable segment evidence")
    for ordinal, row in enumerate(units):
        row["ordinal"] = ordinal
        row["evidence_id"] = _evidence_id(spec["transcript"], row)
    body = {"kind": "himr_summary_source", "schema_version": 1,
        "transcript": spec["transcript"], "format": spec["format"],
        "recording_id": spec["recording_id"], "title": spec["title"], "date": _date(spec),
        "provenance": {"origin": {"longform": "local_asr", "normalized": "local_asr",
            "salad": "salad_asr", "third_party": "third_party", "cloud": "cloud_asr"}[spec["format"]],
            "completion": "completed", "completion_evidence": spec["completion"] or spec["transcript"],
            "source_kind": document["kind"], "source_schema_version": document["schema_version"],
            "source_identity_sha256": identity, "third_party_source": third_party,
            "rights_granted": False, "person_identity_inferred": False, "verified_quotation": False,
            "source_mutation": False, "machine_generated": spec["format"] != "third_party",
            "date_is_event_date": False,
            "unplaced_text_present": any(row["start_ms"] is None for row in units),
            "source_completed_coverage_claimed": full_coverage,
            "full_media_coverage_verified": False}, "segments": units}
    value = {**body, "source_id": "summarysrc_" + _hash(body)[:32]}
    return validate_source(value)


def validate_source(value):
    """Validate a normalized snapshot and replay its IDs, with no filesystem I/O."""
    _exact(value, SOURCE_KEYS, "normalized summary source")
    if value["kind"] != "himr_summary_source" or type(value["schema_version"]) is not int or value["schema_version"] != 1:
        raise SourceError("unsupported summary source")
    _binding(value["transcript"])
    if not isinstance(value["format"], str) or value["format"] not in FORMATS:
        raise SourceError("unsupported summary source format")
    _text(value["recording_id"], "recording ID", maximum=256)
    if value["title"] is not None:
        _text(value["title"], "title", maximum=4096)
    d = _exact(value["date"], {"value", "kind", "basis", "evidence"}, "normalized date")
    if d["value"] is None:
        if d != {"value": None, "kind": "unknown", "basis": "unknown", "evidence": None}:
            raise SourceError("unknown dates must remain explicit")
    else:
        _date_value(d["value"])
        if (d["kind"] not in ("recorded", "published") or not isinstance(d["basis"], str)
                or d["basis"] not in DATE_BASES):
            raise SourceError("invalid normalized date semantics")
        _binding(d["evidence"])
    p = _exact(value["provenance"], PROVENANCE_KEYS, "normalized provenance")
    expected_origin = {"longform": "local_asr", "normalized": "local_asr", "salad": "salad_asr", "third_party": "third_party", "cloud": "cloud_asr"}[value["format"]]
    if p["origin"] != expected_origin or p["completion"] != "completed":
        raise SourceError("source provenance/completion differs")
    _binding(p["completion_evidence"])
    for key in ("rights_granted", "person_identity_inferred", "verified_quotation", "source_mutation", "date_is_event_date", "full_media_coverage_verified"):
        if p[key] is not False:
            raise SourceError("source normalization grants no identity, date, rights or quotation authority")
    if p["machine_generated"] is not (value["format"] != "third_party"):
        raise SourceError("source generation semantics differ")
    for key in ("unplaced_text_present", "source_completed_coverage_claimed"):
        if type(p[key]) is not bool:
            raise SourceError("provenance flags must be boolean")
    _text(p["source_kind"], "native kind", maximum=128)
    _integer(p["source_schema_version"], "native version", low=1, high=5)
    native_pairs = {"longform": {("himr_longform_recording_transcript", 1)},
        "normalized": {("himr_machine_transcript", 1), ("transcript_normalized", 5)},
        "salad": {("himr_salad_recording_transcript", 1)},
        "third_party": {("himr_third_party_transcript_import", 1)},
        "cloud": {("himr_cloud_recording_transcript", 1)}}
    if (p["source_kind"], p["source_schema_version"]) not in native_pairs[value["format"]]:
        raise SourceError("native kind/version differs from its format")
    if p["source_completed_coverage_claimed"] is not (value["format"] == "longform"):
        raise SourceError("source coverage semantics differ")
    if value["format"] not in {"normalized", "cloud"} and p["completion_evidence"] != value["transcript"]:
        raise SourceError("source completion binding differs")
    if p["source_identity_sha256"] is not None and (not isinstance(p["source_identity_sha256"], str) or not SHA.fullmatch(p["source_identity_sha256"])):
        raise SourceError("invalid native identity digest")
    if value["format"] == "third_party":
        _third_party_provenance(p["third_party_source"])
    elif p["third_party_source"] is not None or p["source_identity_sha256"] is None:
        raise SourceError("native provenance differs")
    rows = _rows(value["segments"], "summary segments")
    evidence_ids = set()
    for ordinal, row in enumerate(rows):
        _exact(row, SEGMENT_KEYS, "summary segment")
        if type(row["ordinal"]) is not int or row["ordinal"] != ordinal:
            raise SourceError("summary segment ordinals differ")
        _pair(row["start_ms"], row["end_ms"], nullable=True)
        _text(row["text"], "summary evidence text", empty=True)
        if row["speaker"] is not None and (not isinstance(row["speaker"], str) or not SPEAKER.fullmatch(row["speaker"])):
            raise SourceError("speaker identity must remain anonymous")
        if (not isinstance(row["timing_basis"], str) or row["timing_basis"] not in TIMING_BASES
                or (row["timing_basis"] == "unknown") != (row["start_ms"] is None)):
            raise SourceError("summary timing basis differs")
        ref = _exact(row["source_ref"], REF_KEYS, "source evidence reference")
        if ref["collection"] not in ("segments", "words", "unplaced"):
            raise SourceError("invalid source evidence collection")
        _integer(ref["index"], "source evidence index", high=MAX_SEGMENTS - 1)
        for key in ("native_segment_id", "span_id", "chunk_id", "speaker_scope"):
            if ref[key] is not None:
                _text(ref[key], f"source reference {key}", maximum=256)
        if ref["sample_rate_hz"] is None:
            if ref["start_sample"] is not None or ref["end_sample"] is not None:
                raise SourceError("sample references require an explicit sample rate")
        else:
            if type(ref["sample_rate_hz"]) is not int or ref["sample_rate_hz"] != 16000:
                raise SourceError("unsupported sample rate")
            _integer(ref["start_sample"], "sample start", high=MAX_TIME_MS * 16)
            _integer(ref["end_sample"], "sample end", low=ref["start_sample"], high=MAX_TIME_MS * 16)
            if row["start_ms"] != (ref["start_sample"] + 8) // 16 or row["end_ms"] != (ref["end_sample"] + 8) // 16:
                raise SourceError("sample reference projection differs")
        if ref["timeline_offset_ms"] is not None:
            _integer(ref["timeline_offset_ms"], "source offset")
        expected = _evidence_id(value["transcript"], row)
        if row["evidence_id"] != expected or expected in evidence_ids:
            raise SourceError("evidence identity differs or repeats")
        evidence_ids.add(expected)
    if sum(len(row["text"]) for row in rows) > MAX_TEXT_CHARACTERS:
        raise SourceError("summary text exceeds its bound")
    if p["unplaced_text_present"] != any(row["start_ms"] is None for row in rows):
        raise SourceError("unplaced evidence accounting differs")
    body = {key: item for key, item in value.items() if key != "source_id"}
    if value["source_id"] != "summarysrc_" + _hash(body)[:32]:
        raise SourceError("summary source identity differs")
    return copy.deepcopy(value)
