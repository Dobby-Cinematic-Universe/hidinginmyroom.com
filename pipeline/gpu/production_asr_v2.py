#!/usr/bin/env python3
"""Media-local v2 wrapper for the private production faster-whisper adapter.

Version 1 is intentionally left byte-for-byte intact because its source digest is
part of sealed runtime admissions, work orders, and the first corpus result.  This
wrapper loads that exact implementation and changes only the versioned coordinate
contract:

* every producer timestamp is ``media_ms`` relative to the normalized input;
* ``timeline_offset_ms`` must be zero; and
* recording/source projection remains a separate reviewed operation.

Inference, CUDA/NVML checks, model and runtime replay, the UUID-keyed GPU lock,
offline execution, result sealing, and private/publication policy are delegated to
the preserved v1 implementation.
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
CONTRACT_VERSION = 2
IMPLEMENTATION_VERSION = "0.2.0"
OUTPUT_CONTRACT = "private-faster-whisper-raw-media-local-envelope-v2"


def _load_v1() -> ModuleType:
    path = Path(__file__).with_name("production_asr.py")
    spec = importlib.util.spec_from_file_location("himr_production_asr_v1_preserved", path)
    if spec is None or spec.loader is None:
        raise RuntimeError(f"could not load preserved v1 adapter: {path}")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


_V1 = _load_v1()
# The preserved functions deliberately bind ``Path(__file__)`` as the active
# adapter source.  Under v2 that source must be this wrapper, while the preserved
# v1 file remains a separately admitted dependency.
_V1.__file__ = __file__
_v1_normalize_work_order_core = _V1.normalize_work_order_core
_v1_normalize_raw_transcript = _V1.normalize_raw_transcript


def normalize_work_order_core(value: Any) -> dict[str, Any]:
    """Validate v2 and forbid an unproved timestamp projection."""

    normalized = _v1_normalize_work_order_core(value)
    if normalized["input"]["timeline_offset_ms"] != 0:
        raise _V1.ProductionASRError(
            "v2 output is media-local; input.timeline_offset_ms must be zero"
        )
    return normalized


def normalize_raw_transcript(
    raw: dict[str, Any], work_order: dict[str, Any]
) -> dict[str, Any]:
    """Normalize timestamps without claiming recording coordinates."""

    if work_order["input"]["timeline_offset_ms"] != 0:
        raise _V1.ProductionASRError(
            "v2 output is media-local; input.timeline_offset_ms must be zero"
        )
    v1_document = _v1_normalize_raw_transcript(raw, work_order)
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
    for segment in core["segments"]:
        segment["start_ms"] = segment["source_start_ms"]
        segment["end_ms"] = segment["source_end_ms"]
        for word in segment["words"]:
            word["start_ms"] = word["source_start_ms"]
            word["end_ms"] = word["source_end_ms"]
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
    "recording_projection": "separate_reviewed_operation_required",
}
_V1.WORK_ORDER_CONTRACT_DESCRIPTOR = work_order_descriptor

result_descriptor = copy.deepcopy(_V1.RESULT_CONTRACT_DESCRIPTOR)
result_descriptor["output_contract"] = OUTPUT_CONTRACT
result_descriptor["coordinate_contract"] = {
    "producer_coordinate_system": "media_ms",
    "recording_projection": "not_claimed",
}
_V1.RESULT_CONTRACT_DESCRIPTOR = result_descriptor

_V1.normalize_work_order_core = normalize_work_order_core
_V1.normalize_raw_transcript = normalize_raw_transcript

# Export the contract surface used by tests and offline orchestration.  Function
# globals continue to point at the configured private v1 module above.
ProductionASRError = _V1.ProductionASRError
canonical_bytes = _V1.canonical_bytes
sha256_bytes = _V1.sha256_bytes
contract_document = _V1.contract_document
make_work_order = _V1.make_work_order
validate_work_order = _V1.validate_work_order
validate_completed_result = _V1.validate_completed_result
result_plan = _V1.result_plan
main = _V1.main


if __name__ == "__main__":
    raise SystemExit(main())
