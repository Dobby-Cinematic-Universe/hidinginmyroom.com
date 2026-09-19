#!/usr/bin/env python3
"""Verified-dependency, language-explicit successor to private GPU ASR v3.

Versions 1 through 3 remain immutable evidence.  V4 preserves v3 media-local word
timing and anomaly behavior while closing two successor-contract gaps:

* the preserved v1 implementation is executed only from exact verified bytes; and
* a forced language is recorded as configuration, never as model detection with a
  synthetic probability of 1.0.

This source is not production authority by itself.  A runtime admission must bind
this adapter, its wrapper, this verified loader, the preserved implementation, the
model, the runtime, and an accuracy/resource benchmark before corpus execution.
"""

from __future__ import annotations

import copy
import hashlib
import os
import stat
import sys
from pathlib import Path
from types import ModuleType
from typing import Any


VERIFIED_LOADER_SOURCE_SHA256 = (
    "b6250aea1c8baf5ed54e867ffa6cc584220378e989dd1637138b3920130b083c"
)
V1_SOURCE_SHA256 = "bb5a2557e693cea1daeaefa339b1f3aca2634f124ea72e2f29910331c5ba816c"
V1_WORK_ORDER_CONTRACT_SHA256 = (
    "8a31b1e647805a910186cfecc5152a704e60be8fd2c35e206c9b3e740fdd121b"
)
V1_RESULT_CONTRACT_SHA256 = (
    "a6f9d11df49a7b7dda8a580918a1c1110168202b4d28a85184ffccdbaab967f4"
)
V3_SOURCE_SHA256 = "6624dcfa554c38c029090ab0dd7ceefa75c868ed0951cbbfefe059e386dec1a1"
V3_WORK_ORDER_CONTRACT_SHA256 = (
    "1fa8c90f70215d9766ae99ce221591bfd31b06fe011d2a8c27bac844b4e5e71a"
)
V3_RESULT_CONTRACT_SHA256 = (
    "a2508422b7baf8d6807bafb1441ab9a96b99ffd719fd51c76cbcb7b17b4c223d"
)
MODEL_ADMISSION_SOURCE_SHA256 = (
    "82e1ab544c64de36bf1d40fac287b64ed2546fe41085e84623186c76bfea57a1"
)
RUNTIME_ADMISSION_SOURCE_SHA256 = (
    "8f20e9efb2d9f293f9415a40845be9412644ad33e59c569b72a32acfca8261a2"
)
V4_WRAPPER_SHA256 = "9f1b2539a90071fc13b0332e7164e1c0c9beccc433d1778b4f8d02b3dd6d93b4"
CONTRACT_VERSION = 4
IMPLEMENTATION_VERSION = "0.4.0"
OUTPUT_CONTRACT = "private-faster-whisper-raw-media-local-language-explicit-envelope-v4"

WORD_TIMING_FLAG_NAMES = (
    "precedes_segment_start",
    "extends_beyond_segment_end",
    "start_regresses_from_previous",
    "overlaps_previous",
)
WORD_TIMING_NOTICE = (
    "Finite non-inverted raw model word times are preserved. Word timing can "
    "precede its segment or regress/overlap another word; these machine timing "
    "anomalies are explicit and require review rather than silent repair."
)
FORCED_LANGUAGE_BASIS = "forced_by_inference_profile"
ALLOWED_FORCED_LANGUAGES = ("en",)
AUTO_LANGUAGE_UNSUPPORTED = (
    "v4 requires a forced inference language because model language capability "
    "is not yet an explicitly admitted dependency"
)


def _bootstrap_verified_loader() -> ModuleType:
    """Verify the reusable loader before executing any of its candidate bytes."""

    path = Path(__file__).resolve().with_name("verified_dependency_loader.py")
    lexical = path.lstat()
    flags = (
        os.O_RDONLY
        | getattr(os, "O_CLOEXEC", 0)
        | getattr(os, "O_NONBLOCK", 0)
    )
    if hasattr(os, "O_NOFOLLOW"):
        flags |= os.O_NOFOLLOW
    descriptor = os.open(path, flags)
    try:
        before = os.fstat(descriptor)
        if (
            not stat.S_ISREG(before.st_mode)
            or before.st_nlink != 1
            or before.st_uid != os.getuid()
            or (before.st_dev, before.st_ino) != (lexical.st_dev, lexical.st_ino)
            or not 1 <= before.st_size <= 16 * 1024 * 1024
        ):
            raise RuntimeError("verified dependency loader file binding is unsafe")
        body_parts: list[bytes] = []
        remaining = before.st_size
        while remaining:
            chunk = os.read(descriptor, min(1024 * 1024, remaining))
            if not chunk:
                raise RuntimeError("verified dependency loader ended before its sealed size")
            body_parts.append(chunk)
            remaining -= len(chunk)
        if os.read(descriptor, 1):
            raise RuntimeError("verified dependency loader grew while it was read")
        after = os.fstat(descriptor)
        current = path.lstat()
        identity = lambda value: (
            value.st_dev,
            value.st_ino,
            value.st_mode,
            value.st_nlink,
            value.st_uid,
            value.st_gid,
            value.st_size,
            value.st_mtime_ns,
            value.st_ctime_ns,
        )
        if identity(before) != identity(after) or identity(before) != identity(current):
            raise RuntimeError("verified dependency loader changed while it was read")
        body = b"".join(body_parts)
        if hashlib.sha256(body).hexdigest() != VERIFIED_LOADER_SOURCE_SHA256:
            raise RuntimeError("verified dependency loader SHA-256 does not match its pin")
    finally:
        os.close(descriptor)

    name = "himr_gpu_verified_dependency_loader_for_v4"
    module = ModuleType(name)
    module.__file__ = str(path)
    module.__package__ = ""
    module.__loader__ = None
    module.__spec__ = None
    code = compile(body, str(path), "exec", dont_inherit=True, optimize=0)
    previous = sys.modules.get(name)
    sys.modules[name] = module
    try:
        exec(code, module.__dict__)
    except Exception:
        if previous is None:
            sys.modules.pop(name, None)
        else:
            sys.modules[name] = previous
        raise
    return module


_VERIFIED_LOADER = _bootstrap_verified_loader()
_V1_PATH = Path(__file__).resolve().with_name("production_asr.py")
_V1, _V1_SOURCE = _VERIFIED_LOADER.load_verified_module(
    "himr_production_asr_v1_verified_for_v4",
    _V1_PATH,
    V1_SOURCE_SHA256,
    label="preserved production ASR v1",
    exact_mode=0o644,
)
_V1.__file__ = __file__
_v1_normalize_work_order_core = _V1.normalize_work_order_core
_v1_normalize_raw_transcript = _V1.normalize_raw_transcript
_v1_build_raw_transcript = _V1.build_raw_transcript
_v1_validate_completed_result = _V1.validate_completed_result

_LOCAL_GPU_DEPENDENCY_DIRECTORY = Path(__file__).resolve().parent
_V4_WRAPPER_PATH = (
    _LOCAL_GPU_DEPENDENCY_DIRECTORY.parent
    / "bin"
    / "asr-faster-whisper-gpu-adapter-v4"
)
_LOCAL_GPU_DEPENDENCY_PINS = {
    "admit_hf_model.py": {
        "module_name": "himr_production_model_admission",
        "sha256": MODEL_ADMISSION_SOURCE_SHA256,
        "label": "production model admission helper",
    },
    "admit_runtime.py": {
        "module_name": "himr_production_runtime_admission",
        "sha256": RUNTIME_ADMISSION_SOURCE_SHA256,
        "label": "production runtime admission helper",
    },
}


def _binding_digest_map(value: Any, label: str) -> dict[str, str]:
    if not isinstance(value, list):
        raise _V1.ProductionASRError(f"{label} must be an array")
    result: dict[str, str] = {}
    for ordinal, item in enumerate(value):
        if not isinstance(item, dict):
            raise _V1.ProductionASRError(f"{label}[{ordinal}] must be an object")
        path = item.get("requested_path")
        digest = item.get("sha256")
        if (
            not isinstance(path, str)
            or not path
            or not isinstance(digest, str)
            or not _V1.SHA256_RE.fullmatch(digest)
        ):
            raise _V1.ProductionASRError(
                f"{label}[{ordinal}] path or SHA-256 is invalid"
            )
        if path in result:
            raise _V1.ProductionASRError(f"{label} contains duplicate path {path}")
        result[path] = digest
    return result


def _require_v4_runtime_dependency_bindings(admission: Any) -> None:
    """Require the runtime receipt to bind every v4 executable source dependency."""

    try:
        bindings = admission["evidence"]["bindings"]
        sources = _binding_digest_map(
            bindings["sources"], "runtime admission source bindings"
        )
        executables = _binding_digest_map(
            bindings["executables"], "runtime admission executable bindings"
        )
    except (KeyError, TypeError) as error:
        raise _V1.ProductionASRError(
            "runtime admission lacks v4 dependency bindings"
        ) from error

    required_sources = {
        str(
            _LOCAL_GPU_DEPENDENCY_DIRECTORY / "verified_dependency_loader.py"
        ): VERIFIED_LOADER_SOURCE_SHA256,
        str(_V1_PATH): V1_SOURCE_SHA256,
        str(
            _LOCAL_GPU_DEPENDENCY_DIRECTORY / "admit_hf_model.py"
        ): MODEL_ADMISSION_SOURCE_SHA256,
        str(
            _LOCAL_GPU_DEPENDENCY_DIRECTORY / "admit_runtime.py"
        ): RUNTIME_ADMISSION_SOURCE_SHA256,
    }
    for path, digest in required_sources.items():
        if sources.get(path) != digest:
            raise _V1.ProductionASRError(
                f"runtime admission does not bind exact v4 dependency {path}"
            )
    if executables.get(str(_V4_WRAPPER_PATH)) != V4_WRAPPER_SHA256:
        raise _V1.ProductionASRError(
            "runtime admission does not bind the exact v4 wrapper executable"
        )


def _verified_local_gpu_module(module_name: str, filename: str) -> ModuleType:
    """Replace v1's pathname loader with a closed exact-byte dependency dispatcher."""

    pin = _LOCAL_GPU_DEPENDENCY_PINS.get(filename)
    if pin is None or module_name != pin["module_name"]:
        raise _V1.ProductionASRError(
            f"v4 rejected unlisted local GPU dependency {module_name!r}/{filename!r}"
        )
    try:
        module, _source = _VERIFIED_LOADER.load_verified_module(
            module_name,
            _LOCAL_GPU_DEPENDENCY_DIRECTORY / filename,
            pin["sha256"],
            label=pin["label"],
        )
    except _VERIFIED_LOADER.VerifiedDependencyError as error:
        raise _V1.ProductionASRError(str(error)) from error

    if filename == "admit_runtime.py":
        validate_receipt = module.validate_receipt

        def validate_v4_runtime_receipt(*args: Any, **kwargs: Any) -> Any:
            admission = validate_receipt(*args, **kwargs)
            _require_v4_runtime_dependency_bindings(admission)
            return admission

        module.validate_receipt = validate_v4_runtime_receipt
    return module


_V1.load_local_gpu_module = _verified_local_gpu_module


def _require_media_local_null_context(work_order: dict[str, Any]) -> None:
    if work_order["input"]["timeline_offset_ms"] != 0:
        raise _V1.ProductionASRError(
            "v4 output is media-local; input.timeline_offset_ms must be zero"
        )
    if work_order["catalog_context"] is not None:
        raise _V1.ProductionASRError(
            "v4 producer output requires null catalog_context; projection is separate"
        )


def _require_forced_language(work_order: dict[str, Any]) -> str:
    requested = work_order["inference"]["language"]
    if requested == "auto":
        raise _V1.ProductionASRError(AUTO_LANGUAGE_UNSUPPORTED)
    if requested not in ALLOWED_FORCED_LANGUAGES:
        raise _V1.ProductionASRError(
            "v4 inference.language must be one of the exact admitted languages: "
            f"{list(ALLOWED_FORCED_LANGUAGES)}"
        )
    return requested


def normalize_work_order_core(value: Any) -> dict[str, Any]:
    normalized = _v1_normalize_work_order_core(value)
    _require_media_local_null_context(normalized)
    _require_forced_language(normalized)
    return normalized


def build_raw_transcript(
    info: Any, segments: list[dict[str, Any]], work_order: dict[str, Any]
) -> dict[str, Any]:
    """Make language selection provenance explicit in the immutable raw artifact."""

    _require_forced_language(work_order)
    raw = _v1_build_raw_transcript(info, segments, work_order)
    core = {
        key: copy.deepcopy(item)
        for key, item in raw.items()
        if key not in {"identity_sha256", "document_id"}
    }
    core["language"] = _raw_language_lineage(work_order)
    return _V1.transcript_document(core, "gpuasrraw")


def _word_timing_observations(
    raw: dict[str, Any], work_order: dict[str, Any]
) -> tuple[dict[str, Any], list[list[dict[str, Any]]]]:
    """Create a v1-validation copy while retaining exact raw timing observations."""

    if not isinstance(raw, dict):
        raise _V1.ProductionASRError("raw transcript must be an object")
    raw_segments = raw.get("segments")
    if not isinstance(raw_segments, list):
        raise _V1.ProductionASRError("raw transcript segments must be an array")
    duration_ms = work_order["input"]["expected_duration_ms"]
    validation_raw = copy.deepcopy(raw)
    validation_segments = validation_raw["segments"]
    observations: list[list[dict[str, Any]]] = []
    previous_segment_start = 0

    for segment_ordinal, raw_segment in enumerate(raw_segments):
        if not isinstance(raw_segment, dict):
            raise _V1.ProductionASRError(
                f"raw segment {segment_ordinal} must be an object"
            )
        segment_start, _ = _V1._timestamp_ms(
            raw_segment.get("start_seconds"),
            f"segment[{segment_ordinal}].start_seconds",
            duration_ms,
        )
        segment_end, _ = _V1._timestamp_ms(
            raw_segment.get("end_seconds"),
            f"segment[{segment_ordinal}].end_seconds",
            duration_ms,
        )
        if segment_end < segment_start or segment_start < previous_segment_start:
            raise _V1.ProductionASRError(
                "raw segment timing is inverted or non-monotonic"
            )
        previous_segment_start = segment_start
        raw_words = raw_segment.get("words")
        if not isinstance(raw_words, list):
            raise _V1.ProductionASRError(
                f"raw segment {segment_ordinal} words must be an array"
            )
        validation_words = validation_segments[segment_ordinal]["words"]
        segment_observations: list[dict[str, Any]] = []
        previous_raw_start: float | None = None
        previous_raw_end: float | None = None
        validation_previous_start = segment_start
        segment_start_seconds = _V1.finite_number(
            raw_segment.get("start_seconds"),
            f"segment[{segment_ordinal}].start_seconds",
            0,
            _V1.MAX_AUDIO_SECONDS + 10,
        )
        segment_end_seconds = _V1.finite_number(
            raw_segment.get("end_seconds"),
            f"segment[{segment_ordinal}].end_seconds",
            0,
            _V1.MAX_AUDIO_SECONDS + 10,
        )

        for word_ordinal, raw_word in enumerate(raw_words):
            if not isinstance(raw_word, dict):
                raise _V1.ProductionASRError("raw word must be an object")
            raw_start = raw_word.get("start_seconds")
            raw_end = raw_word.get("end_seconds")
            flags = {name: False for name in WORD_TIMING_FLAG_NAMES}
            if raw_start is None or raw_end is None:
                observation = {
                    "start_ms": None,
                    "end_ms": None,
                    "timing_clipped_to_input": False,
                    "flags": flags,
                }
            else:
                start_seconds = _V1.finite_number(
                    raw_start,
                    f"segment[{segment_ordinal}].word[{word_ordinal}].start_seconds",
                    0,
                    _V1.MAX_AUDIO_SECONDS + 10,
                )
                end_seconds = _V1.finite_number(
                    raw_end,
                    f"segment[{segment_ordinal}].word[{word_ordinal}].end_seconds",
                    0,
                    _V1.MAX_AUDIO_SECONDS + 10,
                )
                start_ms, start_clipped = _V1._timestamp_ms(
                    start_seconds,
                    f"segment[{segment_ordinal}].word[{word_ordinal}].start_seconds",
                    duration_ms,
                )
                end_ms, end_clipped = _V1._timestamp_ms(
                    end_seconds,
                    f"segment[{segment_ordinal}].word[{word_ordinal}].end_seconds",
                    duration_ms,
                )
                if end_ms < start_ms or end_seconds < start_seconds:
                    raise _V1.ProductionASRError(
                        f"segment[{segment_ordinal}].word[{word_ordinal}] has inverted timestamps"
                    )
                flags = {
                    "precedes_segment_start": start_seconds < segment_start_seconds,
                    "extends_beyond_segment_end": end_seconds > segment_end_seconds,
                    "start_regresses_from_previous": (
                        previous_raw_start is not None
                        and start_seconds < previous_raw_start
                    ),
                    "overlaps_previous": (
                        previous_raw_end is not None and start_seconds < previous_raw_end
                    ),
                }
                observation = {
                    "start_ms": start_ms,
                    "end_ms": end_ms,
                    "timing_clipped_to_input": start_clipped or end_clipped,
                    "flags": flags,
                }
                validation_start = max(
                    segment_start, start_ms, validation_previous_start
                )
                validation_end = max(end_ms, validation_start)
                validation_words[word_ordinal]["start_seconds"] = (
                    validation_start / 1_000
                )
                validation_words[word_ordinal]["end_seconds"] = validation_end / 1_000
                validation_previous_start = validation_start
                previous_raw_start = start_seconds
                previous_raw_end = end_seconds
            segment_observations.append(observation)
        observations.append(segment_observations)
    return validation_raw, observations


def _validate_language_contract(
    raw: dict[str, Any], work_order: dict[str, Any], validation_raw: dict[str, Any]
) -> dict[str, Any]:
    value = raw.get("language")
    if not isinstance(value, dict) or set(value) != {
        "value",
        "selection_basis",
        "detection_performed",
        "probability_raw",
        "all_probabilities_raw",
    }:
        raise _V1.ProductionASRError("v4 raw language contract is invalid")
    requested = _require_forced_language(work_order)
    if (
        value["value"] != requested
        or value["selection_basis"] != FORCED_LANGUAGE_BASIS
        or value["detection_performed"] is not False
        or value["probability_raw"] is not None
        or value["all_probabilities_raw"] != []
    ):
        raise _V1.ProductionASRError("forced language provenance is inconsistent")
    validation_raw["language"] = {"value": requested, "probability_raw": 1.0}
    return _normalized_language_lineage(work_order)


def normalize_raw_transcript(
    raw: dict[str, Any], work_order: dict[str, Any]
) -> dict[str, Any]:
    _require_media_local_null_context(work_order)
    validation_raw, observations = _word_timing_observations(raw, work_order)
    language = _validate_language_contract(raw, work_order, validation_raw)
    v1_document = _v1_normalize_raw_transcript(validation_raw, work_order)
    core = {
        key: copy.deepcopy(item)
        for key, item in v1_document.items()
        if key not in {"identity_sha256", "document_id"}
    }
    duration_ms = work_order["input"]["expected_duration_ms"]
    core["language"] = language
    core["timeline"] = {
        "coordinate_system": "media_ms",
        "source_duration_ms": duration_ms,
        "source_offset_ms": 0,
        "end_ms": duration_ms,
    }
    flag_counts = {name: 0 for name in WORD_TIMING_FLAG_NAMES}
    anomalous_word_count = 0
    for segment, segment_observations in zip(
        core["segments"], observations, strict=True
    ):
        segment["start_ms"] = segment["source_start_ms"]
        segment["end_ms"] = segment["source_end_ms"]
        segment_flag_counts = {name: 0 for name in WORD_TIMING_FLAG_NAMES}
        segment_anomalous_word_count = 0
        for word, observation in zip(
            segment["words"], segment_observations, strict=True
        ):
            local_start = observation["start_ms"]
            local_end = observation["end_ms"]
            word["start_ms"] = local_start
            word["end_ms"] = local_end
            word["source_start_ms"] = local_start
            word["source_end_ms"] = local_end
            word["timing_clipped_to_input"] = observation[
                "timing_clipped_to_input"
            ]
            word["timing_anomaly_flags"] = copy.deepcopy(observation["flags"])
            if any(observation["flags"].values()):
                anomalous_word_count += 1
                segment_anomalous_word_count += 1
            for name, present in observation["flags"].items():
                if present:
                    flag_counts[name] += 1
                    segment_flag_counts[name] += 1
        segment["word_timing_anomaly_count"] = segment_anomalous_word_count
        segment["word_timing_anomaly_flag_counts"] = segment_flag_counts
    core["word_timing_anomalies"] = {
        "anomalous_word_count": anomalous_word_count,
        "total_flag_count": sum(flag_counts.values()),
        "flag_counts": flag_counts,
        "notice": WORD_TIMING_NOTICE,
    }
    return _V1.transcript_document(core, "gpuasrnorm")


def _raw_language_lineage(work_order: dict[str, Any]) -> dict[str, Any]:
    requested = _require_forced_language(work_order)
    return {
        "value": requested,
        "selection_basis": FORCED_LANGUAGE_BASIS,
        "detection_performed": False,
        "probability_raw": None,
        "all_probabilities_raw": [],
    }


def _normalized_language_lineage(work_order: dict[str, Any]) -> dict[str, Any]:
    requested = _require_forced_language(work_order)
    return {
        "value": requested,
        "selection_basis": FORCED_LANGUAGE_BASIS,
        "detection_performed": False,
        "raw_probability": None,
        "calibrated_probability": None,
    }


def _require_completed_language_lineage(
    result: dict[str, Any],
    raw: dict[str, Any],
    normalized: dict[str, Any],
    work_order: dict[str, Any],
) -> None:
    """Cross-check forced-language provenance at every completed-result layer."""

    expected_raw_language = _raw_language_lineage(work_order)
    expected_normalized_language = _normalized_language_lineage(work_order)
    expected_engine = {
        "library": "faster-whisper",
        "library_version": work_order["runtime"]["packages"]["faster-whisper"],
        "model_identity_sha256": work_order["model"]["identity_sha256"],
        "model_revision": work_order["model"]["revision"],
    }
    expected_input = {
        "sha256": work_order["input"]["expected_sha256"],
        "duration_ms": work_order["input"]["expected_duration_ms"],
        "timeline_offset_ms": 0,
    }
    if (
        raw.get("kind") != _V1.RAW_TRANSCRIPT_KIND
        or raw.get("schema_version") != 1
        or raw.get("engine") != expected_engine
        or raw.get("input") != expected_input
        or raw.get("language") != expected_raw_language
        or raw.get("score_semantics") != "raw_model_outputs_uncalibrated"
        or raw.get("policy") != _V1.POLICY
    ):
        raise _V1.ProductionASRError(
            "completed raw transcript language or model/input lineage is inconsistent"
        )
    if normalized.get("language") != expected_normalized_language:
        raise _V1.ProductionASRError(
            "completed normalized transcript language lineage is inconsistent"
        )
    transcript = result.get("transcript")
    inference = result.get("inference")
    if (
        not isinstance(transcript, dict)
        or transcript.get("language") != expected_normalized_language
        or not isinstance(inference, dict)
        or inference.get("parameters") != work_order["inference"]
    ):
        raise _V1.ProductionASRError(
            "completed result language summary or inference lineage is inconsistent"
        )


def _read_completed_transcript(
    path: Path,
    *,
    label: str,
    maximum_bytes: int,
    id_prefix: str,
) -> tuple[dict[str, Any], bytes]:
    body = _V1.stable_file_bytes(
        path,
        label=label,
        maximum_bytes=maximum_bytes,
        exact_mode=0o400,
        single_link=True,
    )
    document = _V1._validate_identity_document(
        _V1.parse_json_bytes(body, label), label, id_prefix=id_prefix
    )
    if body != _V1.canonical_bytes(document):
        raise _V1.ProductionASRError(f"{label} is not canonical JSON")
    return document, body


def validate_completed_result(
    work_order: dict[str, Any], plan: dict[str, Any]
) -> dict[str, Any]:
    """Replay v1 integrity, then independently bind every language representation."""

    result = _v1_validate_completed_result(work_order, plan)
    maximum = work_order["inference"]["max_result_bytes"]
    raw, raw_body = _read_completed_transcript(
        Path(plan["raw_transcript_path"]),
        label="completed v4 raw transcript",
        maximum_bytes=maximum,
        id_prefix="gpuasrraw",
    )
    normalized, normalized_body = _read_completed_transcript(
        Path(plan["normalized_transcript_path"]),
        label="completed v4 normalized transcript",
        maximum_bytes=maximum,
        id_prefix="gpuasrnorm",
    )
    if normalize_raw_transcript(raw, work_order) != normalized:
        raise _V1.ProductionASRError(
            "completed v4 normalized transcript does not replay from raw transcript"
        )
    _require_completed_language_lineage(result, raw, normalized, work_order)

    artifact_bodies = {
        "faster_whisper_raw_transcript_json": raw_body,
        "transcript_normalized_json": normalized_body,
    }
    artifacts = result.get("artifacts")
    if not isinstance(artifacts, list):
        raise _V1.ProductionASRError("completed v4 result artifacts are invalid")
    observed_artifact_hashes = {
        item.get("artifact_kind"): item.get("sha256")
        for item in artifacts
        if isinstance(item, dict)
    }
    for kind, body in artifact_bodies.items():
        if observed_artifact_hashes.get(kind) != _V1.sha256_bytes(body):
            raise _V1.ProductionASRError(
                f"completed v4 {kind} changed after initial result replay"
            )
    return result


_V1.CONTRACT_VERSION = CONTRACT_VERSION
_V1.IMPLEMENTATION_VERSION = IMPLEMENTATION_VERSION
_V1.OUTPUT_CONTRACT = OUTPUT_CONTRACT

work_order_descriptor = copy.deepcopy(_V1.WORK_ORDER_CONTRACT_DESCRIPTOR)
work_order_descriptor["schema_version"] = CONTRACT_VERSION
work_order_descriptor["implementation_version"] = IMPLEMENTATION_VERSION
work_order_descriptor["coordinate_contract"] = {
    "producer_coordinate_system": "media_ms",
    "origin": "normalized_input_artifact_start",
    "timeline_offset_ms": 0,
    "catalog_context": "null_required",
    "recording_projection": "separate_reviewed_operation_required",
}
work_order_descriptor["verified_dependencies"] = {
    "loader_sha256": VERIFIED_LOADER_SOURCE_SHA256,
    "preserved_v1_source_sha256": V1_SOURCE_SHA256,
    "preserved_v1_work_order_contract_sha256": V1_WORK_ORDER_CONTRACT_SHA256,
    "preserved_v1_result_contract_sha256": V1_RESULT_CONTRACT_SHA256,
    "local_module_allowlist": {
        filename: {
            "module_name": pin["module_name"],
            "sha256": pin["sha256"],
        }
        for filename, pin in sorted(_LOCAL_GPU_DEPENDENCY_PINS.items())
    },
    "runtime_receipt_required_executable": {
        "filename": _V4_WRAPPER_PATH.name,
        "sha256": V4_WRAPPER_SHA256,
    },
    "load_policy": "stable_retained_bytes_verified_before_compile_and_exec",
}
work_order_descriptor["successor_lineage"] = {
    "preserved_v3_source_sha256": V3_SOURCE_SHA256,
    "preserved_v3_work_order_contract_sha256": V3_WORK_ORDER_CONTRACT_SHA256,
    "preserved_v3_result_contract_sha256": V3_RESULT_CONTRACT_SHA256,
    "v3_executed_as_dependency": False,
}
work_order_descriptor["language_contract"] = {
    "forced_language": "configuration_not_detection",
    "forced_language_probability": None,
    "allowed_languages": list(ALLOWED_FORCED_LANGUAGES),
    "auto_language": (
        "rejected_until_model_language_capability_is_explicitly_admitted"
    ),
}
work_order_descriptor["word_timing_contract"] = {
    "raw_model_word_times": "preserved",
    "paired_finite_word_times": "non_inverted_required",
    "segment_timing": "strict_non_inverted_monotonic_and_input_bounded",
    "word_vs_segment_or_previous": "retained_and_explicitly_flagged",
}
_V1.WORK_ORDER_CONTRACT_DESCRIPTOR = work_order_descriptor

result_descriptor = copy.deepcopy(_V1.RESULT_CONTRACT_DESCRIPTOR)
result_descriptor["output_contract"] = OUTPUT_CONTRACT
result_descriptor["coordinate_contract"] = {
    "producer_coordinate_system": "media_ms",
    "catalog_context": "null_required",
    "recording_projection": "not_claimed",
}
result_descriptor["language_contract"] = copy.deepcopy(
    work_order_descriptor["language_contract"]
)
result_descriptor["verified_dependencies"] = copy.deepcopy(
    work_order_descriptor["verified_dependencies"]
)
result_descriptor["word_timing_contract"] = {
    "raw_model_word_times": "preserved",
    "normalized_word_anomaly_flags": list(WORD_TIMING_FLAG_NAMES),
    "normalized_summary": "word_timing_anomalies",
    "human_review_required": True,
}
_V1.RESULT_CONTRACT_DESCRIPTOR = result_descriptor

_V1.normalize_work_order_core = normalize_work_order_core
_V1.build_raw_transcript = build_raw_transcript
_V1.normalize_raw_transcript = normalize_raw_transcript
_V1.validate_completed_result = validate_completed_result

ProductionASRError = _V1.ProductionASRError
canonical_bytes = _V1.canonical_bytes
sha256_bytes = _V1.sha256_bytes
contract_document = _V1.contract_document
make_work_order = _V1.make_work_order
validate_work_order = _V1.validate_work_order
result_plan = _V1.result_plan
raw_segment = _V1.raw_segment
main = _V1.main


if __name__ == "__main__":
    raise SystemExit(main())
