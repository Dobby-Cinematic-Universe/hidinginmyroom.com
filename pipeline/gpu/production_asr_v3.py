#!/usr/bin/env python3
"""Media-local v3 wrapper with explicit faster-whisper word-timing anomalies.

The admitted v1 and v2 adapters are immutable evidence and remain byte-for-byte
unchanged.  This wrapper loads the exact v1 implementation and changes only the
versioned post-inference contract:

* producer coordinates remain ``media_ms`` relative to the normalized input;
* ``timeline_offset_ms`` is zero and ``catalog_context`` is null;
* finite, paired word times must remain non-inverted and within the existing
  bounded input-overrun policy; and
* word times that precede their segment, regress, or overlap are retained and
  explicitly flagged instead of aborting an otherwise valid inference.

Segment inversion/order, input bounds, CUDA/NVML checks, sealed model/runtime
replay, the UUID-keyed GPU lock, offline execution, private result sealing, and
publication policy remain delegated to the preserved implementation.
"""

from __future__ import annotations

import copy
import importlib.util
from pathlib import Path
from types import ModuleType
from typing import Any


V1_SOURCE_SHA256 = "bb5a2557e693cea1daeaefa339b1f3aca2634f124ea72e2f29910331c5ba816c"
V1_WORK_ORDER_CONTRACT_SHA256 = (
    "8a31b1e647805a910186cfecc5152a704e60be8fd2c35e206c9b3e740fdd121b"
)
V1_RESULT_CONTRACT_SHA256 = (
    "a6f9d11df49a7b7dda8a580918a1c1110168202b4d28a85184ffccdbaab967f4"
)
V2_SOURCE_SHA256 = "f95410aa74095347d01c9757c9daf14b61dc7a30e634145db18c85e0cc51e9c9"
V2_WORK_ORDER_CONTRACT_SHA256 = (
    "901147b6142e0e4eb47679a34bf87cd11866afef4adbffc3e2c66feab707bf83"
)
V2_RESULT_CONTRACT_SHA256 = (
    "11ad607d8a9ddd3cac5558f4eae1fd4906bfd02cbcc7313d4d9a4937e1948bb1"
)
CONTRACT_VERSION = 3
IMPLEMENTATION_VERSION = "0.3.0"
OUTPUT_CONTRACT = "private-faster-whisper-raw-media-local-word-timing-envelope-v3"

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


def _load_v1() -> ModuleType:
    path = Path(__file__).with_name("production_asr.py")
    spec = importlib.util.spec_from_file_location(
        "himr_production_asr_v1_preserved_for_v3", path
    )
    if spec is None or spec.loader is None:
        raise RuntimeError(f"could not load preserved v1 adapter: {path}")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


_V1 = _load_v1()
_V1.__file__ = __file__
_v1_normalize_work_order_core = _V1.normalize_work_order_core
_v1_normalize_raw_transcript = _V1.normalize_raw_transcript


def _require_media_local_null_context(work_order: dict[str, Any]) -> None:
    if work_order["input"]["timeline_offset_ms"] != 0:
        raise _V1.ProductionASRError(
            "v3 output is media-local; input.timeline_offset_ms must be zero"
        )
    if work_order["catalog_context"] is not None:
        raise _V1.ProductionASRError(
            "v3 producer output requires null catalog_context; projection is separate"
        )


def normalize_work_order_core(value: Any) -> dict[str, Any]:
    """Validate v3 and forbid producer-side catalogue/time projection."""

    normalized = _v1_normalize_work_order_core(value)
    _require_media_local_null_context(normalized)
    return normalized


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
                # Preserve the v1 paired-null normalized representation. The raw
                # artifact continues to retain either model field exactly.
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
                        f"segment[{segment_ordinal}].word[{word_ordinal}] "
                        "has inverted timestamps"
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

                # The preserved v1 normalizer remains the authority for all other
                # transcript fields. Supply it a timing-only validation copy that
                # satisfies its older monotonic-word assumption, then restore the
                # exact observed word times below.
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


def normalize_raw_transcript(
    raw: dict[str, Any], work_order: dict[str, Any]
) -> dict[str, Any]:
    """Normalize media-local times and annotate tolerable raw word anomalies."""

    _require_media_local_null_context(work_order)
    validation_raw, observations = _word_timing_observations(raw, work_order)
    v1_document = _v1_normalize_raw_transcript(validation_raw, work_order)
    core = {
        key: copy.deepcopy(item)
        for key, item in v1_document.items()
        if key not in {"identity_sha256", "document_id"}
    }
    duration_ms = work_order["input"]["expected_duration_ms"]
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
result_descriptor["word_timing_contract"] = {
    "raw_model_word_times": "preserved",
    "normalized_word_anomaly_flags": list(WORD_TIMING_FLAG_NAMES),
    "normalized_summary": "word_timing_anomalies",
    "human_review_required": True,
}
_V1.RESULT_CONTRACT_DESCRIPTOR = result_descriptor

_V1.normalize_work_order_core = normalize_work_order_core
_V1.normalize_raw_transcript = normalize_raw_transcript

# Export the contract surface used by tests and offline orchestration. Function
# globals continue to point at the configured private v1 module above.
ProductionASRError = _V1.ProductionASRError
canonical_bytes = _V1.canonical_bytes
sha256_bytes = _V1.sha256_bytes
contract_document = _V1.contract_document
make_work_order = _V1.make_work_order
validate_work_order = _V1.validate_work_order
validate_completed_result = _V1.validate_completed_result
result_plan = _V1.result_plan
raw_segment = _V1.raw_segment
main = _V1.main


if __name__ == "__main__":
    raise SystemExit(main())
