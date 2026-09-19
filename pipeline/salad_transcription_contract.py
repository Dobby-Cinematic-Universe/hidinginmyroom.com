"""Pure, opt-in Salad cloud transcription contracts.

Planning reads only explicitly supplied recording-input metadata.  It never opens
media, uploads, submits jobs, changes a local campaign, or grants publication
authority.  Provider timestamps remain approximate machine hypotheses, not exact
sample measurements or verified quotations.
"""

from __future__ import annotations

import copy
from decimal import Decimal, InvalidOperation, ROUND_HALF_UP
import hashlib
import json
import os
from pathlib import Path
import re
import stat
from typing import Any

try:
    from himr_corpus.longform_asr_planner import (
        LongformPlanningError,
        validate_recording_manifest,
    )
except ModuleNotFoundError:
    from corpus.src.himr_corpus.longform_asr_planner import (
        LongformPlanningError,
        validate_recording_manifest,
    )


SAMPLE_RATE_HZ = 16_000
MAX_WAV_BYTES = 3_000_000_000
S4_MAX_UPLOAD_BYTES = 100_000_000
MAX_JOB_SECONDS = 9_000
MAX_RECORDINGS = 100_000
MAX_CHUNKS = 100_000
MAX_JSON_BYTES = 64 * 1024 * 1024
SHA256_RE = re.compile(r"^[0-9a-f]{64}$")
IDENTIFIER_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._:-]{0,255}$")
ORGANIZATION_RE = re.compile(r"^[a-z][a-z0-9-]{0,61}[a-z0-9]$")
DECIMAL_RE = re.compile(r"^[0-9]{1,12}(?:\.[0-9]{1,6})?$")
POLICY = {
    "visibility": "private",
    "machine_generated": True,
    "human_review_required": True,
    "verified_quotation": False,
    "provider_output_verbatim_guaranteed": False,
    "source_artifact_mutation": False,
    "source_controller_mutation": False,
    "catalogue_mutation_authority": "none",
    "publication_authority": "none",
    "deletion_authority": "none",
}
TIMING_SEMANTICS = {
    "coordinate_system": "recording_relative_milliseconds",
    "provider_basis": "analysis_chunk_relative_seconds",
    "precision": "approximate_provider_timestamps",
    "conversion": "add_exact_analysis_offset_then_nearest_integer_half_up",
    "missing_timestamps": "null_no_fabricated_timing",
    "scores_calibrated": False,
}
SPEAKER_SEMANTICS = {
    "labels": "unreviewed_provider_hypotheses",
    "speaker_id_scope": "one_analysis_chunk_only",
    "cross_chunk_speaker_linking": False,
    "person_identity_claimed": False,
}


class CloudContractError(RuntimeError):
    """A cloud input, plan, or result violates its explicit contract."""


def canonical_bytes(value: Any) -> bytes:
    try:
        return (
            json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"), allow_nan=False)
            + "\n"
        ).encode("utf-8")
    except (TypeError, ValueError, UnicodeError, RecursionError) as error:
        raise CloudContractError("value cannot be encoded as canonical JSON") from error


def _hash(value: Any) -> str:
    return hashlib.sha256(canonical_bytes(value)).hexdigest()


def _seal(core: dict[str, Any], id_key: str, prefix: str) -> dict[str, Any]:
    digest = _hash(core)
    return {**core, "identity_sha256": digest, id_key: prefix + digest[:32]}


def _exact(value: Any, keys: set[str], label: str) -> dict[str, Any]:
    if not isinstance(value, dict) or set(value) != keys:
        raise CloudContractError(f"{label} fields differ")
    return value


def _integer(value: Any, label: str, minimum: int = 0, maximum: int = 2**63 - 1) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or not minimum <= value <= maximum:
        raise CloudContractError(f"{label} must be a bounded integer")
    return value


def _digest(value: Any, label: str) -> str:
    if not isinstance(value, str) or not SHA256_RE.fullmatch(value):
        raise CloudContractError(f"{label} must be a SHA-256 digest")
    return value


def _identifier(value: Any, label: str) -> str:
    if not isinstance(value, str) or not IDENTIFIER_RE.fullmatch(value):
        raise CloudContractError(f"{label} must be a bounded identifier")
    return value


def _path(value: Any, label: str) -> str:
    if (
        not isinstance(value, str)
        or not value.startswith("/")
        or "\x00" in value
        or any(part in {".", ".."} for part in value.split("/"))
        or str(Path(value)) != value
        or value == "/"
    ):
        raise CloudContractError(f"{label} must be a normalized absolute file-system path")
    return value


def _decimal(value: Any, label: str) -> Decimal:
    if not isinstance(value, str) or not DECIMAL_RE.fullmatch(value):
        raise CloudContractError(f"{label} must be a positive decimal string with at most six decimal places")
    parsed = Decimal(value)
    if not parsed.is_finite() or parsed <= 0:
        raise CloudContractError(f"{label} must be positive")
    return parsed


def _decimal_string(value: Decimal) -> str:
    result = format(value, "f")
    return result.rstrip("0").rstrip(".") if "." in result else result


def _pairs(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
    result: dict[str, Any] = {}
    for key, value in pairs:
        if key in result:
            raise CloudContractError("recording input JSON contains duplicate fields")
        result[key] = value
    return result


def _read_json(path: Path, expected_sha256: str) -> dict[str, Any]:
    _path(str(path), "recording manifest path")
    _digest(expected_sha256, "recording manifest SHA-256")
    descriptor: int | None = None
    try:
        descriptor = os.open(path, os.O_RDONLY | os.O_CLOEXEC | os.O_NOFOLLOW | os.O_NONBLOCK)
        before = os.fstat(descriptor)
        if not stat.S_ISREG(before.st_mode) or not 0 < before.st_size <= MAX_JSON_BYTES:
            raise CloudContractError("recording manifest is not a bounded regular file")
        chunks: list[bytes] = []
        total = 0
        while total <= MAX_JSON_BYTES:
            body = os.read(descriptor, min(1024 * 1024, MAX_JSON_BYTES + 1 - total))
            if not body:
                break
            chunks.append(body)
            total += len(body)
        after = os.fstat(descriptor)
        signature = lambda item: (item.st_dev, item.st_ino, item.st_size, item.st_mtime_ns, item.st_ctime_ns)
        if signature(before) != signature(after) or total != before.st_size or total > MAX_JSON_BYTES:
            raise CloudContractError("recording manifest changed while being read")
        raw = b"".join(chunks)
        if hashlib.sha256(raw).hexdigest() != expected_sha256:
            raise CloudContractError("recording manifest SHA-256 differs")
        return json.loads(
            raw.decode("utf-8", errors="strict"),
            object_pairs_hook=_pairs,
            parse_constant=lambda _value: (_ for _ in ()).throw(CloudContractError("recording input contains nonfinite JSON")),
        )
    except (OSError, UnicodeError, ValueError, RecursionError) as error:
        raise CloudContractError("cannot read a valid recording manifest") from error
    finally:
        if descriptor is not None:
            os.close(descriptor)


def load_recording_input(path: Path, expected_sha256: str) -> dict[str, Any]:
    """Read one hash-bound manifest; never open or hash its media payload."""

    value = _read_json(path, expected_sha256)
    try:
        manifest = validate_recording_manifest(value)
    except (LongformPlanningError, TypeError, ValueError) as error:
        raise CloudContractError("recording input fails the existing long-form manifest contract") from error
    recording = manifest["recording"]
    audio = recording["input"]
    return {
        "manifest": {"path": str(path), "sha256": expected_sha256},
        "recording_id": recording["recording_id"],
        "media_id": recording["media_id"],
        "audio": {
            key: audio[key]
            for key in ("path", "sha256", "byte_count", "sample_rate_hz", "total_samples", "duration_ms")
        },
    }


def _recording(value: Any) -> dict[str, Any]:
    item = _exact(value, {"manifest", "recording_id", "media_id", "audio"}, "recording")
    manifest = _exact(item["manifest"], {"path", "sha256"}, "manifest binding")
    audio = _exact(
        item["audio"],
        {"path", "sha256", "byte_count", "sample_rate_hz", "total_samples", "duration_ms"},
        "recording audio",
    )
    _path(manifest["path"], "manifest path")
    _digest(manifest["sha256"], "manifest digest")
    _identifier(item["recording_id"], "recording ID")
    _identifier(item["media_id"], "media ID")
    _path(audio["path"], "audio path")
    _digest(audio["sha256"], "audio digest")
    _integer(audio["byte_count"], "audio byte count", 1)
    if _integer(audio["sample_rate_hz"], "sample rate", 1) != SAMPLE_RATE_HZ:
        raise CloudContractError("cloud input must be normalized mono 16 kHz audio")
    samples = _integer(audio["total_samples"], "total samples", 1)
    duration_ms = _integer(audio["duration_ms"], "duration milliseconds", 0)
    if duration_ms != (samples * 1000 + SAMPLE_RATE_HZ // 2) // SAMPLE_RATE_HZ:
        raise CloudContractError("recording duration differs from its exact sample count")
    return copy.deepcopy(item)


def build_plan(
    recordings: list[dict[str, Any]],
    *,
    organization: str,
    engine: str = "transcribe",
    output_root: str,
    ffmpeg: dict[str, str],
    rate_usd_per_hour: str,
    max_estimated_cost_usd: str,
    chunk_seconds: int = 9000,
    overlap_seconds: int = 5,
    diarization: bool = False,
    sentence_diarization: bool = False,
    summary_words: int = 0,
) -> dict[str, Any]:
    """Build a deterministic metadata-only plan with a bounded cost estimate."""

    if not isinstance(recordings, list) or not 1 <= len(recordings) <= MAX_RECORDINGS:
        raise CloudContractError("recordings must be a nonempty bounded list")
    if not isinstance(organization, str) or not ORGANIZATION_RE.fullmatch(organization):
        raise CloudContractError("organization must be a bounded API path identifier")
    if engine not in {"transcribe", "transcription-lite"}:
        raise CloudContractError("unsupported Salad transcription engine")
    if not isinstance(diarization, bool) or not isinstance(sentence_diarization, bool):
        raise CloudContractError("diarization options must be booleans")
    summary_words = _integer(summary_words, "summary words", 0, 2000)
    if engine == "transcription-lite" and summary_words:
        raise CloudContractError("summarization requires the transcribe engine")
    _path(output_root, "output root")
    ffmpeg = _exact(ffmpeg, {"path", "sha256"}, "ffmpeg binding")
    _path(ffmpeg["path"], "ffmpeg path")
    _digest(ffmpeg["sha256"], "ffmpeg digest")
    chunk_seconds = _integer(chunk_seconds, "chunk seconds", 1, MAX_JOB_SECONDS)
    overlap_seconds = _integer(overlap_seconds, "overlap seconds", 0, MAX_JOB_SECONDS)
    if overlap_seconds >= chunk_seconds:
        raise CloudContractError("overlap must be shorter than a core chunk")
    rate = _decimal(rate_usd_per_hour, "hourly rate")
    budget = _decimal(max_estimated_cost_usd, "estimated budget")
    normalized = sorted((_recording(row) for row in recordings), key=lambda row: row["recording_id"])
    seen_recordings: set[str] = set()
    seen_audio: set[str] = set()
    seen_manifests: set[str] = set()
    total_hundredths = 0
    total_chunks = 0
    for row in normalized:
        if row["recording_id"] in seen_recordings or row["audio"]["sha256"] in seen_audio or row["manifest"]["path"] in seen_manifests:
            raise CloudContractError("recordings repeat an ID, source audio digest, or manifest path")
        seen_recordings.add(row["recording_id"])
        seen_audio.add(row["audio"]["sha256"])
        seen_manifests.add(row["manifest"]["path"])
        total_samples = row["audio"]["total_samples"]
        if total_samples <= min(chunk_seconds, MAX_JOB_SECONDS) * SAMPLE_RATE_HZ:
            # Keep the entire recording in one provider job whenever it fits.
            # No overlap is needed because no split boundary exists.
            core_samples = total_samples
        else:
            core_seconds = min(chunk_seconds, MAX_JOB_SECONDS - 2 * overlap_seconds)
            if core_seconds <= 0:
                raise CloudContractError("requested overlap leaves no core within the provider duration limit")
            core_samples = core_seconds * SAMPLE_RATE_HZ
        count = (total_samples + core_samples - 1) // core_samples
        total_chunks += count
        if total_chunks > MAX_CHUNKS:
            raise CloudContractError("plan exceeds its bounded chunk count")
        row["chunks"] = []
        for ordinal, start in enumerate(range(0, total_samples, core_samples)):
            end = min(total_samples, start + core_samples)
            analysis_start = max(0, start - overlap_seconds * SAMPLE_RATE_HZ)
            analysis_end = min(total_samples, end + overlap_seconds * SAMPLE_RATE_HZ)
            samples = analysis_end - analysis_start
            wav_bytes = samples * 2 + 1024
            if wav_bytes > MAX_WAV_BYTES or samples > MAX_JOB_SECONDS * SAMPLE_RATE_HZ:
                raise CloudContractError("chunk exceeds the 3 GB WAV or 2.5 hour provider limit; choose shorter chunks")
            hundredths = max(1, (samples + 576_000 - 1) // 576_000)
            total_hundredths += hundredths
            core = {
                "recording_id": row["recording_id"],
                "ordinal": ordinal,
                "core": {"start_sample": start, "end_sample": end},
                "analysis": {"start_sample": analysis_start, "end_sample": analysis_end},
                "wav_max_bytes": wav_bytes,
                "upload_provider": "s4" if wav_bytes <= S4_MAX_UPLOAD_BYTES else "temp_sh",
                "billable_hours": _decimal_string(Decimal(hundredths) / 100),
            }
            # Bind source bytes as well as coordinates: identical recording labels
            # in separate plans cannot accidentally reuse another source's WAV.
            chunk_hash = _hash({"source": {key: row[key] for key in ("manifest", "audio", "media_id")}, "chunk": core})
            row["chunks"].append({"chunk_id": "saladchunk_" + chunk_hash[:32], **core})
    hours = Decimal(total_hundredths) / 100
    estimate = hours * rate
    if estimate > budget:
        raise CloudContractError("estimated transcription cost exceeds the explicit budget")
    core = {
        "kind": "himr_salad_transcription_plan",
        "schema_version": 1,
        "provider": {"name": "salad", "organization": organization, "engine": engine},
        "transcription_options": {
            "language_code": "en",
            "sentence_level_timestamps": True,
            "word_level_timestamps": True,
            "diarization": diarization,
            "sentence_diarization": sentence_diarization,
            "summarize": summary_words,
        },
        "output_root": output_root,
        "ffmpeg": copy.deepcopy(ffmpeg),
        "chunking": {
            "sample_rate_hz": SAMPLE_RATE_HZ, "chunk_seconds": chunk_seconds,
            "overlap_seconds": overlap_seconds,
            "strategy": "whole_recording_when_possible", "max_job_seconds": MAX_JOB_SECONDS,
            "s4_max_upload_bytes": S4_MAX_UPLOAD_BYTES, "upload_policy": "s4_then_temp_sh",
        },
        "estimate": {
            "rate_usd_per_hour": _decimal_string(rate),
            "max_estimated_cost_usd": _decimal_string(budget),
            "billable_hours": _decimal_string(hours),
            "estimated_cost_usd": _decimal_string(estimate),
            "basis": "per_job_ceil_0.01_hour_minimum_0.01",
            "hard_actual_price_cap": False,
        },
        "recordings": normalized,
        "policy": copy.deepcopy(POLICY),
    }
    return _seal(core, "plan_id", "saladplan_")


def validate_plan(plan: Any) -> dict[str, Any]:
    """Replay the entire deterministic plan without reading files or media."""

    item = _exact(plan, {"kind", "schema_version", "provider", "transcription_options", "output_root", "ffmpeg", "chunking", "estimate", "recordings", "policy", "identity_sha256", "plan_id"}, "cloud plan")
    provider = _exact(item["provider"], {"name", "organization", "engine"}, "provider")
    options = _exact(item["transcription_options"], {"language_code", "sentence_level_timestamps", "word_level_timestamps", "diarization", "sentence_diarization", "summarize"}, "transcription options")
    chunking = _exact(item["chunking"], {"sample_rate_hz", "chunk_seconds", "overlap_seconds", "strategy", "max_job_seconds", "s4_max_upload_bytes", "upload_policy"}, "chunking")
    estimate = _exact(item["estimate"], {"rate_usd_per_hour", "max_estimated_cost_usd", "billable_hours", "estimated_cost_usd", "basis", "hard_actual_price_cap"}, "estimate")
    if not isinstance(item["recordings"], list):
        raise CloudContractError("recordings must be a list")
    recordings = []
    for row in item["recordings"]:
        _exact(row, {"manifest", "recording_id", "media_id", "audio", "chunks"}, "planned recording")
        recordings.append({key: row[key] for key in ("manifest", "recording_id", "media_id", "audio")})
    replay = build_plan(
        recordings,
        organization=provider["organization"],
        engine=provider["engine"],
        output_root=item["output_root"],
        ffmpeg=item["ffmpeg"],
        rate_usd_per_hour=estimate["rate_usd_per_hour"],
        max_estimated_cost_usd=estimate["max_estimated_cost_usd"],
        chunk_seconds=chunking["chunk_seconds"],
        overlap_seconds=chunking["overlap_seconds"],
        diarization=options["diarization"],
        sentence_diarization=options["sentence_diarization"],
        summary_words=options["summarize"],
    )
    if canonical_bytes(replay) != canonical_bytes(item):
        raise CloudContractError("cloud plan differs from deterministic replay")
    return replay


def plan_chunks(plan: dict[str, Any]) -> list[dict[str, Any]]:
    """Return chunks from an already validated plan, in recording/core order."""

    return [copy.deepcopy(chunk) for recording in plan["recordings"] for chunk in recording["chunks"]]


def _selected_chunk(plan: dict[str, Any], chunk: dict[str, Any]) -> tuple[dict[str, Any], dict[str, Any]]:
    validate_plan(plan)
    if not isinstance(chunk, dict):
        raise CloudContractError("chunk must be a planned object")
    matches = [(row, saved) for row in plan["recordings"] for saved in row["chunks"] if saved["chunk_id"] == chunk.get("chunk_id")]
    if len(matches) != 1 or canonical_bytes(matches[0][1]) != canonical_bytes(chunk):
        raise CloudContractError("chunk differs from its unique planned binding")
    return matches[0]


def _text(value: Any, label: str) -> str:
    if not isinstance(value, str) or len(value) > MAX_JSON_BYTES:
        raise CloudContractError(f"{label} must be bounded text")
    try:
        value.encode("utf-8", errors="strict")
    except UnicodeError as error:
        raise CloudContractError(f"{label} is not valid UTF-8 text") from error
    return value


def _seconds(value: Any, label: str, duration: Decimal) -> Decimal:
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise CloudContractError(f"{label} must be a finite JSON number")
    try:
        number = Decimal(str(value))
    except InvalidOperation as error:
        raise CloudContractError(f"{label} is invalid") from error
    if not number.is_finite() or not 0 <= number <= duration:
        raise CloudContractError(f"{label} exceeds the analysis chunk")
    return number


def _timed_unit(row: Any, *, ordinal: int, chunk: dict[str, Any], text_key: str, label: str) -> dict[str, Any]:
    if not isinstance(row, dict):
        raise CloudContractError(f"{label} must be an object")
    text = _text(row.get(text_key), f"{label} text")
    speaker, speaker_id = _speaker(row.get("speaker"), chunk["chunk_id"])
    start = row.get("start")
    end = row.get("end")
    if (start is None) != (end is None):
        raise CloudContractError(f"{label} has only one endpoint")
    if "timestamp" in row:
        timestamps = row["timestamp"]
        if not isinstance(timestamps, list) or len(timestamps) != 2:
            raise CloudContractError(f"{label} duplicate timestamp pair is invalid")
        if start is None:
            if timestamps != [None, None]:
                raise CloudContractError(f"{label} timestamp pair lacks documented start/end endpoints")
        elif any(isinstance(value, bool) or not isinstance(value, (int, float)) for value in timestamps):
            raise CloudContractError(f"{label} duplicate timestamps must be numbers")
        elif timestamps != [start, end]:
            raise CloudContractError(f"{label} duplicate timestamp pair disagrees")
    if start is None:
        return {"ordinal": ordinal, "text": text, "start_ms": None, "end_ms": None,
                "speaker": speaker, "speaker_id": speaker_id}
    analysis = chunk["analysis"]
    duration = Decimal(analysis["end_sample"] - analysis["start_sample"]) / SAMPLE_RATE_HZ
    start_seconds = _seconds(start, f"{label} start", duration)
    end_seconds = _seconds(end, f"{label} end", duration)
    if start_seconds > end_seconds:
        raise CloudContractError(f"{label} timestamps are reversed")
    offset_ms = Decimal(analysis["start_sample"]) * 1000 / SAMPLE_RATE_HZ
    return {
        "ordinal": ordinal,
        "text": text,
        "start_ms": int((offset_ms + start_seconds * 1000).quantize(Decimal(1), rounding=ROUND_HALF_UP)),
        "end_ms": int((offset_ms + end_seconds * 1000).quantize(Decimal(1), rounding=ROUND_HALF_UP)),
        "speaker": speaker,
        "speaker_id": speaker_id,
    }


def _speaker(value: Any, chunk_id: str) -> tuple[str | None, str | None]:
    if value is None:
        return None, None
    label = _text(value, "provider speaker label")
    if not 1 <= len(label) <= 128 or not label.strip() or any(not character.isprintable() for character in label):
        raise CloudContractError("provider speaker label must be bounded nonempty printable text")
    # The exact label is evidence, not a filesystem component or person name.
    # Including the chunk ID prevents SPEAKER_00 in different jobs from silently
    # becoming a cross-recording or cross-chunk identity assertion.
    return label, chunk_id + ":" + label


def _summary(value: Any, requested_words: int) -> tuple[str | None, str]:
    if value is None:
        return None, "missing" if requested_words else "not_requested"
    return _text(value, "provider summary"), "returned"


def normalize_output(plan: dict[str, Any], chunk: dict[str, Any], provider_job_id: str, output: dict[str, Any]) -> dict[str, Any]:
    """Normalize one provider output without claiming native local span evidence."""

    recording, selected = _selected_chunk(plan, chunk)
    _identifier(provider_job_id, "provider job ID")
    if not isinstance(output, dict):
        raise CloudContractError("provider output must be an object")
    if len(canonical_bytes(output)) > MAX_JSON_BYTES:
        raise CloudContractError("provider output exceeds the bounded JSON size")
    text = _text(output.get("text"), "provider transcript")
    summary, summary_status = _summary(output.get("summary"), plan["transcription_options"]["summarize"])
    arrays = {}
    for source_key, target_key, text_key in (
        ("sentence_level_timestamps", "segments", "text"),
        ("word_segments", "words", "word"),
    ):
        rows = output.get(source_key, [])
        if rows is None:
            rows = []
        if not isinstance(rows, list) or len(rows) > 1_000_000:
            raise CloudContractError(f"{source_key} must be a bounded list")
        arrays[target_key] = [_timed_unit(row, ordinal=ordinal, chunk=selected, text_key=text_key, label=source_key) for ordinal, row in enumerate(rows)]
    core = {
        "kind": "himr_salad_chunk_transcript",
        "schema_version": 1,
        "plan": {"plan_id": plan["plan_id"], "identity_sha256": plan["identity_sha256"]},
        "recording_id": recording["recording_id"],
        "media_id": recording["media_id"],
        "chunk_id": selected["chunk_id"],
        "provider_job_id": provider_job_id,
        "provider": copy.deepcopy(plan["provider"]),
        "analysis": copy.deepcopy(selected["analysis"]),
        "core": copy.deepcopy(selected["core"]),
        "raw_output_sha256": _hash(output),
        "raw_output_hash_basis": "canonical_json_utf8_sorted_keys_newline",
        "text": text,
        "summary": summary,
        "summary_status": summary_status,
        **arrays,
        "timing_semantics": copy.deepcopy(TIMING_SEMANTICS),
        "speaker_semantics": copy.deepcopy(SPEAKER_SEMANTICS),
        "policy": copy.deepcopy(POLICY),
    }
    return _seal(core, "transcript_id", "saladchunktranscript_")


def _validate_chunk_transcript(plan: dict[str, Any], chunk: dict[str, Any], value: Any) -> dict[str, Any]:
    keys = {"kind", "schema_version", "plan", "recording_id", "media_id", "chunk_id", "provider_job_id", "provider", "analysis", "core", "raw_output_sha256", "raw_output_hash_basis", "text", "summary", "summary_status", "segments", "words", "timing_semantics", "speaker_semantics", "policy", "identity_sha256", "transcript_id"}
    item = _exact(value, keys, "chunk transcript")
    recording = next(row for row in plan["recordings"] if row["recording_id"] == chunk["recording_id"])
    expected = {
        "kind": "himr_salad_chunk_transcript", "schema_version": 1,
        "plan": {"plan_id": plan["plan_id"], "identity_sha256": plan["identity_sha256"]},
        "recording_id": recording["recording_id"], "media_id": recording["media_id"],
        "chunk_id": chunk["chunk_id"], "provider": plan["provider"],
        "analysis": chunk["analysis"], "core": chunk["core"],
        "raw_output_hash_basis": "canonical_json_utf8_sorted_keys_newline",
        "timing_semantics": TIMING_SEMANTICS, "speaker_semantics": SPEAKER_SEMANTICS, "policy": POLICY,
    }
    for key, expected_value in expected.items():
        if canonical_bytes(item[key]) != canonical_bytes(expected_value):
            raise CloudContractError("chunk transcript provenance differs")
    _identifier(item["provider_job_id"], "provider job ID")
    _digest(item["raw_output_sha256"], "provider output hash")
    _text(item["text"], "chunk text")
    summary, summary_status = _summary(item["summary"], plan["transcription_options"]["summarize"])
    if (summary, summary_status) != (item["summary"], item["summary_status"]):
        raise CloudContractError("chunk transcript summary status differs from its provider evidence")
    low = (chunk["analysis"]["start_sample"] * 1000 + 8000) // 16000
    high = (chunk["analysis"]["end_sample"] * 1000 + 8000) // 16000
    for key in ("segments", "words"):
        if not isinstance(item[key], list) or len(item[key]) > 1_000_000:
            raise CloudContractError("chunk transcript units must be bounded arrays")
        for ordinal, unit in enumerate(item[key]):
            _exact(unit, {"ordinal", "text", "start_ms", "end_ms", "speaker", "speaker_id"}, "transcript unit")
            if _integer(unit["ordinal"], "unit ordinal") != ordinal:
                raise CloudContractError("transcript unit ordinals differ")
            _text(unit["text"], "unit text")
            speaker, speaker_id = _speaker(unit["speaker"], chunk["chunk_id"])
            if (speaker, speaker_id) != (unit["speaker"], unit["speaker_id"]):
                raise CloudContractError("speaker label is not bound to its source chunk")
            start, end = unit["start_ms"], unit["end_ms"]
            if start is None and end is None:
                continue
            start = _integer(start, "unit start", low, high)
            end = _integer(end, "unit end", start, high)
    core = {key: value for key, value in item.items() if key not in {"identity_sha256", "transcript_id"}}
    if canonical_bytes(_seal(core, "transcript_id", "saladchunktranscript_")) != canonical_bytes(item):
        raise CloudContractError("chunk transcript identity differs")
    return copy.deepcopy(item)


def assemble_recording(plan: dict[str, Any], recording_id: str, chunk_transcripts: list[dict[str, Any]]) -> dict[str, Any]:
    """Assemble complete cloud chunks by deterministic midpoint core ownership.

    Missing timestamps remain preserved as unplaced hypotheses; they cannot grant
    timeline coverage or silently acquire fabricated positions.  Midpoint ownership
    removes chunk context deterministically but does not claim perfect semantic
    duplicate detection across approximate provider timestamps.
    """

    validate_plan(plan)
    _identifier(recording_id, "recording ID")
    matches = [row for row in plan["recordings"] if row["recording_id"] == recording_id]
    if len(matches) != 1:
        raise CloudContractError("recording lacks a unique plan binding")
    recording = matches[0]
    if not isinstance(chunk_transcripts, list) or len(chunk_transcripts) != len(recording["chunks"]):
        raise CloudContractError("assembly requires exactly one transcript for every planned chunk")
    by_id = {}
    for transcript in chunk_transcripts:
        if not isinstance(transcript, dict) or not isinstance(transcript.get("chunk_id"), str) or transcript["chunk_id"] in by_id:
            raise CloudContractError("assembly repeats or lacks chunk identity")
        by_id[transcript["chunk_id"]] = transcript
    if set(by_id) != {chunk["chunk_id"] for chunk in recording["chunks"]}:
        raise CloudContractError("assembly chunk set differs from plan")
    kept: dict[str, list[dict[str, Any]]] = {"segments": [], "words": []}
    unplaced = []
    sources = []
    chunk_summaries = []
    seen_provider_jobs: set[str] = set()
    excluded = {"segments": 0, "words": 0}
    text_parts = []
    for chunk in recording["chunks"]:
        transcript = _validate_chunk_transcript(plan, chunk, by_id[chunk["chunk_id"]])
        if transcript["provider_job_id"] in seen_provider_jobs:
            raise CloudContractError("assembly repeats a provider job for distinct chunks")
        seen_provider_jobs.add(transcript["provider_job_id"])
        sources.append({"chunk_id": chunk["chunk_id"], "provider_job_id": transcript["provider_job_id"], "transcript_id": transcript["transcript_id"], "identity_sha256": transcript["identity_sha256"], "raw_output_sha256": transcript["raw_output_sha256"], "core": copy.deepcopy(chunk["core"]), "analysis": copy.deepcopy(chunk["analysis"])})
        chunk_summaries.append({
            "chunk_id": chunk["chunk_id"], "provider_job_id": transcript["provider_job_id"],
            "core": copy.deepcopy(chunk["core"]), "analysis": copy.deepcopy(chunk["analysis"]),
            "text": transcript["summary"], "status": transcript["summary_status"],
        })
        local_kept = {"segments": [], "words": []}
        for key in ("segments", "words"):
            for unit in transcript[key]:
                projected = {**copy.deepcopy(unit), "chunk_id": chunk["chunk_id"], "provider_job_id": transcript["provider_job_id"]}
                if unit["start_ms"] is None:
                    unplaced.append({"unit_kind": key, **projected})
                    continue
                midpoint_times_32 = (unit["start_ms"] + unit["end_ms"]) * 16
                start_twice = chunk["core"]["start_sample"] * 2
                end_twice = chunk["core"]["end_sample"] * 2
                # Terminal zero-width detections can occur exactly at EOF.
                terminal = chunk["core"]["end_sample"] == recording["audio"]["total_samples"] and midpoint_times_32 == end_twice
                if start_twice <= midpoint_times_32 < end_twice or terminal:
                    kept[key].append(projected)
                    local_kept[key].append(projected)
                else:
                    excluded[key] += 1
        if transcript["words"] and all(unit["start_ms"] is not None for unit in transcript["words"]):
            text_parts.extend(unit["text"] for unit in local_kept["words"])
        elif transcript["segments"] and all(unit["start_ms"] is not None for unit in transcript["segments"]):
            text_parts.extend(unit["text"] for unit in local_kept["segments"])
        else:
            unplaced.append({"unit_kind": "chunk_text", "chunk_id": chunk["chunk_id"], "provider_job_id": transcript["provider_job_id"], "text": transcript["text"], "start_ms": None, "end_ms": None})
            text_parts.append(transcript["text"])
    for key in kept:
        kept[key].sort(key=lambda unit: (unit["start_ms"], unit["end_ms"], unit["chunk_id"], unit["ordinal"]))
    whole_recording_summary = len(chunk_summaries) == 1 and chunk_summaries[0]["status"] == "returned"
    core = {
        "kind": "himr_salad_recording_transcript",
        "schema_version": 1,
        "plan": {"plan_id": plan["plan_id"], "identity_sha256": plan["identity_sha256"]},
        "recording": {key: copy.deepcopy(recording[key]) for key in ("manifest", "recording_id", "media_id", "audio")},
        "provider": copy.deepcopy(plan["provider"]),
        "sources": sources,
        "chunk_summaries": chunk_summaries,
        "summary_semantics": {
            "scope": "whole_recording_provider_summary" if whole_recording_summary else "per_chunk_provider_summary",
            "whole_recording_summary_claimed": whole_recording_summary,
        },
        **kept,
        "unplaced": unplaced,
        "text": " ".join(text.strip() for text in text_parts if text.strip()),
        "text_semantics": {"may_include_unplaced_or_overlapping_text": bool(unplaced), "verbatim_guaranteed": False},
        "coverage": {"all_planned_jobs_collected": True, "provider_decoded_coverage_verified": False, "exact_sample_coverage_claimed": False},
        "overlap": {"method": "unique_half_open_core_midpoint_ownership", "semantic_deduplication_guaranteed": False, "excluded_context_units": excluded},
        "timing_semantics": copy.deepcopy(TIMING_SEMANTICS),
        "speaker_semantics": copy.deepcopy(SPEAKER_SEMANTICS),
        "policy": copy.deepcopy(POLICY),
    }
    return _seal(core, "transcript_id", "saladrecordingtranscript_")
