#!/usr/bin/env python3
"""Read-only validator for one closed whisper.cpp batch supersession archive.

This module deliberately has no execution, import, publication, database, or network
lane.  It validates one immutable disposition receipt and the two exact sealed batch
trees and historical result artifacts named by that receipt.
"""

from __future__ import annotations

import argparse
import copy
import hashlib
import json
import os
import stat
import sys
from contextlib import ExitStack
from dataclasses import dataclass
from pathlib import Path
from typing import Any


IMPLEMENTATION_VERSION = "0.1.0"
VALIDATOR_NAME = "himr-asr-whispercpp-batch-archive"
REPOSITORY_ROOT = Path(__file__).resolve().parents[1]
ARCHIVE_ORIGIN_ROOT = REPOSITORY_ROOT

MAX_RECEIPT_BYTES = 512 * 1024
MAX_MANIFEST_BYTES = 16 * 1024 * 1024
MAX_WORK_ORDER_BYTES = 4 * 1024 * 1024
MAX_RESULT_BYTES = 16 * 1024 * 1024
MAX_JSON_DEPTH = 128

SUBJECT_BATCH_ID = "asrbatch_794f61d7ba9b883c8ab37cbcdc5a6d28"
SUCCESSOR_BATCH_ID = "asrbatch_a98bf085d254a67bb30b837b307436f6"
RECEIPT_RELATIVE_PATH = (
    "research/corpus/private-asr-work-orders/batch-dispositions/"
    f"{SUBJECT_BATCH_ID}.json"
)
RECEIPT_SHA256 = "a97ff0258eee55c06a3f907a2b3972a47d6f64e8f2600625eb89d0433ae54ec4"
RECEIPT_BYTE_COUNT = 9_930

MATERIALIZER_IDENTITY = {
    "name": "himr-asr-whispercpp-raw-batch",
    "implementation_version": "0.1.0",
    "sha256": "c940b93d11a22f057c4c244d0d17d0e933050f9dafe8f1c5ef8ede2bcf971df5",
    "byte_count": 71_128,
}
ENGINE_IDENTITY = {
    "version_label": "whisper.cpp v1.8.3",
    "expected_sha256": "4831024debb4e60e9433d27967ba6dae033d4b0c770c4ef208c16fc5a8fe77d6",
    "byte_count": 1_010_544,
    "revision": "2eeeba56e9edd762b4b38467bab96c2517163158",
    "version_evidence": "source_revision_plus_executable_sha256",
}

ARCHIVES = {
    "subject": {
        "batch_id": SUBJECT_BATCH_ID,
        "manifest_relative_path": (
            "research/corpus/private-asr-work-orders/batches/"
            f"{SUBJECT_BATCH_ID}/manifest.json"
        ),
        "manifest_sha256": "55b52cba003c526b27bc48b1cd12b45d103ea843e963c9f3fc99df5c470b2007",
        "manifest_byte_count": 127_681,
        "identity_sha256": "794f61d7ba9b883c8ab37cbcdc5a6d2837836953e74bb978455c56eafbf5d08c",
        "implementation_version": "0.1.0",
        "materializer": MATERIALIZER_IDENTITY,
        "asr_adapter": {
            "name": "himr-asr-whispercpp",
            "contract_version": 1,
            "implementation_version": "0.2.2",
            "sha256": "e67fa795e4677de099e45319d3988dc09875366979990294df87ea93e162c98a",
            "byte_count": 55_093,
        },
        "engine": ENGINE_IDENTITY,
    },
    "successor": {
        "batch_id": SUCCESSOR_BATCH_ID,
        "manifest_relative_path": (
            "research/corpus/private-asr-work-orders/batches/"
            f"{SUCCESSOR_BATCH_ID}/manifest.json"
        ),
        "manifest_sha256": "a6345fd64151d6f78d946db2834a998a148d79318f5e9704c240297f1c189070",
        "manifest_byte_count": 127_681,
        "identity_sha256": "a98bf085d254a67bb30b837b307436f601eef46fb241d658dc837599d51dea91",
        "implementation_version": "0.1.0",
        "materializer": MATERIALIZER_IDENTITY,
        "asr_adapter": {
            "name": "himr-asr-whispercpp",
            "contract_version": 1,
            "implementation_version": "0.2.3",
            "sha256": "781f481a0f5ff155417795640ae089d484db0f56e7ec8e9e6e8375ef46c260f0",
            "byte_count": 57_311,
        },
        "engine": ENGINE_IDENTITY,
    },
}

ORDERED_WORK_ORDERS = [
    {
        "ordinal": 1,
        "job_id": "asr-raw-0a3e7b5c70562840b57fae243672bf16",
        "file_sha256": "be42f0987e6362cdff4d6b7bb99f0ee1fc1c2fbdeb3f47f9a6ec12b6c3235493",
        "canonical_sha256": "68b8a86037899d8b388b7edc1d2a7965b8a2f7c8060abc06cd54943e85a7e655",
        "byte_count": 2_827,
    },
    {
        "ordinal": 2,
        "job_id": "asr-raw-061133bfbe8fbfaf2247bb5ad604adde",
        "file_sha256": "2f2d63e860af3570742d730d2f6e5804b7446c9745f4e83363ba739e95bc1bd9",
        "canonical_sha256": "926502d0b4e55fb46f07a59e08dfdf4d87c49fd1a6585bef26c28f2d662efb82",
        "byte_count": 2_827,
    },
    {
        "ordinal": 3,
        "job_id": "asr-raw-88a001b1da571adb6977815861306e46",
        "file_sha256": "68d99ced4b9bd2132688cd567b3a18a6efb70d8da7002851edde4eaade781c8a",
        "canonical_sha256": "4925a723247317649f901abc4b7dbfee74c50ded6fba7f58c7b2136b98215986",
        "byte_count": 2_827,
    },
    {
        "ordinal": 4,
        "job_id": "asr-raw-104918d88ad95141fc027c7ea3af6648",
        "file_sha256": "834aba7e05d83b7c2835e8b13a231cb6f2b04b4abc0c3f68ff58c38d5a625d81",
        "canonical_sha256": "c6c48bb650100b192190306f135ebd5b6a7733fc5cdefa9f8178181937f9903c",
        "byte_count": 2_827,
    },
    {
        "ordinal": 5,
        "job_id": "asr-raw-fe7d028e3202a01b3c41e4901b98130c",
        "file_sha256": "eb501d7d39f936e65f730b45b303e91a8bb16c68accae66e88aa661e76afdf81",
        "canonical_sha256": "7d3ead0d22bacab315548ca26683350633582f98d27e49b262e2c59a02c4eb65",
        "byte_count": 2_827,
    },
    {
        "ordinal": 6,
        "job_id": "asr-raw-860ff7ee16bb8ff2ac4f3dc8821583c2",
        "file_sha256": "2c5d4d7d5b7c4dd8004b0c40c647d9f46d10078cadcd04788495637f8286cb12",
        "canonical_sha256": "7ff1388a104db0b098d3a45193f0aeea8f90443ee29ff67a6b83650e44ee2c2d",
        "byte_count": 2_827,
    },
    {
        "ordinal": 7,
        "job_id": "asr-raw-acd213b1f01759ef90dd22e37ffbba5e",
        "file_sha256": "2ec8dbfa62d9065d4fc14319ba65efe366aec5683cd902062379cdf5f7385972",
        "canonical_sha256": "7bc403e5e925a82698464ab4fda8a1a20985f5e90298cbe7738566381f9c326e",
        "byte_count": 2_827,
    },
    {
        "ordinal": 8,
        "job_id": "asr-raw-2c101bb70046b1ba94620014f053d25a",
        "file_sha256": "a2dc4428929ca7433fe7238b86f28f3c440601a3751d4b770b197dc9c1b142b9",
        "canonical_sha256": "8cf97ff476720f16594ebbd11479e74f2c77c61e38191f5b7a4f84a1bcd8a84b",
        "byte_count": 2_826,
    },
]

JOB_1_RESULT_COMMON = {
    "job_id": "asr-raw-0a3e7b5c70562840b57fae243672bf16",
    "work_order_sha256": "68b8a86037899d8b388b7edc1d2a7965b8a2f7c8060abc06cd54943e85a7e655",
    "input_sha256": "a44fd7efb2d5f78707320f67304391db40905173572ab8b5fab6a5c192f921b7",
    "engine_version": "whisper.cpp v1.8.3",
    "engine_sha256": ENGINE_IDENTITY["expected_sha256"],
}

RESULT_EVIDENCE = {
    "subject_job_1": {
        "result_relative_path": (
            "research/corpus/private-asr-results/asr/whispercpp/sha256/a4/"
            "a44fd7efb2d5f78707320f67304391db40905173572ab8b5fab6a5c192f921b7/"
            "results/ea15f82290bac6b736f34fab06faf9d9437865e94cc38f8e5fd6a9efadb9373b/"
            "result.json"
        ),
        "result_sha256": "818c0fd10016cab049ea326c7b2a692e81cd8bb430888dc694389e4dee02cb3c",
        "result_byte_count": 4_088_749,
        **JOB_1_RESULT_COMMON,
        "adapter_implementation_version": "0.2.2",
        "result_key": "ea15f82290bac6b736f34fab06faf9d9437865e94cc38f8e5fd6a9efadb9373b",
        "recipe_id": "recipe_asr_whispercpp_3e903094f560895c9fca18db3794bf1a",
        "recipe_sha256": "3e903094f560895c9fca18db3794bf1a16a239202f757ed1578570eb14b1e952",
        "started_at": "2026-08-27T07:39:39Z",
        "completed_at": "2026-08-27T07:47:41Z",
    },
    "successor_job_1": {
        "result_relative_path": (
            "research/corpus/private-asr-results/asr/whispercpp/sha256/a4/"
            "a44fd7efb2d5f78707320f67304391db40905173572ab8b5fab6a5c192f921b7/"
            "results/91eb930930bbf6fae7f5902b5a309bf5b500ac2049c8f40bd5136d559e190bed/"
            "result.json"
        ),
        "result_sha256": "16be35a68fa08dbdc462f9cd88eb9a1d885134d692e7b09847d6fcb61c50a7af",
        "result_byte_count": 4_088_749,
        **JOB_1_RESULT_COMMON,
        "adapter_implementation_version": "0.2.3",
        "result_key": "91eb930930bbf6fae7f5902b5a309bf5b500ac2049c8f40bd5136d559e190bed",
        "recipe_id": "recipe_asr_whispercpp_f9a663528de67be354a5b3264cbb9533",
        "recipe_sha256": "f9a663528de67be354a5b3264cbb9533d5c2faa6b8c613cbd4aaf84c2e908cad",
        "started_at": "2026-08-27T08:13:55Z",
        "completed_at": "2026-08-27T08:21:57Z",
    },
    "job_1_artifacts": {
        "raw": {
            "subject_relative_path": (
                "research/corpus/private-asr-results/asr/whispercpp/sha256/a4/"
                "a44fd7efb2d5f78707320f67304391db40905173572ab8b5fab6a5c192f921b7/"
                "results/ea15f82290bac6b736f34fab06faf9d9437865e94cc38f8e5fd6a9efadb9373b/"
                "whisper.raw.json"
            ),
            "successor_relative_path": (
                "research/corpus/private-asr-results/asr/whispercpp/sha256/a4/"
                "a44fd7efb2d5f78707320f67304391db40905173572ab8b5fab6a5c192f921b7/"
                "results/91eb930930bbf6fae7f5902b5a309bf5b500ac2049c8f40bd5136d559e190bed/"
                "whisper.raw.json"
            ),
            "sha256": "2f193c9f57e0aa1caeda362893cca96bf38a374772c69449b7f5131584b09d4e",
            "byte_count": 799_770,
        },
        "normalized": {
            "subject_relative_path": (
                "research/corpus/private-asr-results/asr/whispercpp/sha256/a4/"
                "a44fd7efb2d5f78707320f67304391db40905173572ab8b5fab6a5c192f921b7/"
                "results/ea15f82290bac6b736f34fab06faf9d9437865e94cc38f8e5fd6a9efadb9373b/"
                "transcript.normalized.json"
            ),
            "successor_relative_path": (
                "research/corpus/private-asr-results/asr/whispercpp/sha256/a4/"
                "a44fd7efb2d5f78707320f67304391db40905173572ab8b5fab6a5c192f921b7/"
                "results/91eb930930bbf6fae7f5902b5a309bf5b500ac2049c8f40bd5136d559e190bed/"
                "transcript.normalized.json"
            ),
            "sha256": "65ea66f108ec9acbc597330679963bb0cf162bc29fbb2fdb0071681e4604349c",
            "byte_count": 1_973_398,
        },
    },
    "successor_job_2_inverted_offsets": {
        "result_relative_path": (
            "research/corpus/private-asr-results/asr/whispercpp/sha256/19/"
            "194ff5d5e940a8a863a8ee4ac0640d935c5fbd15f65abebb43bccb05a657162d/"
            "results/112b00cde805d3dee07b8a63dba7f654aecbba52ede2fe9fe4b7bc0f52b8081b/"
            "result.json"
        ),
        "result_sha256": "62b881495a195b272be8b3c2c9ec295e661b6e550b62a704a527efe6e84d8313",
        "result_byte_count": 5_168_717,
        "normalized_relative_path": (
            "research/corpus/private-asr-results/asr/whispercpp/sha256/19/"
            "194ff5d5e940a8a863a8ee4ac0640d935c5fbd15f65abebb43bccb05a657162d/"
            "results/112b00cde805d3dee07b8a63dba7f654aecbba52ede2fe9fe4b7bc0f52b8081b/"
            "transcript.normalized.json"
        ),
        "normalized_sha256": "dffe5ebe93e7c82293f20587ea49cdc79e5da10a116f99590e155c0c9f195a92",
        "normalized_byte_count": 2_494_708,
        "job_id": "asr-raw-061133bfbe8fbfaf2247bb5ad604adde",
        "work_order_sha256": "926502d0b4e55fb46f07a59e08dfdf4d87c49fd1a6585bef26c28f2d662efb82",
        "adapter_implementation_version": "0.2.3",
        "timing_quality_flag": "invalid_upstream_inverted",
        "affected_token_count": 10,
    },
}

EXPECTED_RECEIPT = {
    "schema_version": 1,
    "receipt_kind": "asr_whispercpp_batch_archival_disposition",
    "validator_contract": {
        "name": VALIDATOR_NAME,
        "implementation_version": IMPLEMENTATION_VERSION,
        "semantics": "read_only_exact_archive_validation_only",
    },
    "subject": ARCHIVES["subject"],
    "disposition": {
        "state": "superseded_validation_only_non_executable",
        "reason_code": "adapter_contract_0.2.3_inverted_offset_preservation",
        "dispatch_allowed": False,
        "preserve_sealed_tree": True,
        "historical_adapter_source": {
            "identity_pin_retained": True,
            "source_artifact_bound_by_receipt": False,
            "source_reproducibility_claimed": False,
        },
    },
    "successor": ARCHIVES["successor"],
    "equivalence": {
        "work_orders_byte_identical": True,
        "work_order_count": 8,
        "totals": {
            "audio_byte_count": 160_111_673,
            "audio_duration_ms": 13_319_825,
        },
        "ordered_work_orders": ORDERED_WORK_ORDERS,
        "manifest_difference_paths": [
            "batch_id",
            "batch_relative_path",
            "identity_sha256",
            "software.asr_adapter.byte_count",
            "software.asr_adapter.implementation_version",
            "software.asr_adapter.sha256",
        ],
    },
    "execution_evidence": {
        "batch_attribution": (
            "consistent_by_adapter_version_work_order_identity_and_observed_chronology_"
            "not_cryptographically_batch_bound"
        ),
        "undocumented_legacy_job_2_attempt_claimed": False,
        **RESULT_EVIDENCE,
    },
    "safety": {
        "tool_exposes_execution": False,
        "tool_exposes_import": False,
        "tool_exposes_publication": False,
        "database_access": False,
        "network_access": False,
        "writes_performed_by_validator": False,
        "execution_authority": "none",
        "import_authority": "none",
        "publication_authority": "none",
        "visibility": "private",
    },
}


class ArchiveValidationError(RuntimeError):
    """The closed archival disposition failed read-only validation."""


def canonical_bytes(value: Any) -> bytes:
    try:
        return json.dumps(
            value,
            ensure_ascii=False,
            sort_keys=True,
            separators=(",", ":"),
            allow_nan=False,
        ).encode("utf-8")
    except (TypeError, ValueError) as error:
        raise ArchiveValidationError(f"value is not strict canonical JSON: {error}") from error


def sha256_bytes(value: bytes) -> str:
    return hashlib.sha256(value).hexdigest()


def _stat_identity(value: os.stat_result) -> tuple[int, ...]:
    return (
        value.st_dev,
        value.st_ino,
        value.st_size,
        value.st_mtime_ns,
        value.st_ctime_ns,
        stat.S_IFMT(value.st_mode),
        stat.S_IMODE(value.st_mode),
        value.st_nlink,
    )


def _check_depth(value: Any, *, depth: int = 0) -> None:
    if depth > MAX_JSON_DEPTH:
        raise ArchiveValidationError(f"JSON nesting exceeds {MAX_JSON_DEPTH}")
    if isinstance(value, dict):
        for child in value.values():
            _check_depth(child, depth=depth + 1)
    elif isinstance(value, list):
        for child in value:
            _check_depth(child, depth=depth + 1)


def parse_json(body: bytes, label: str) -> Any:
    try:
        text = body.decode("utf-8", errors="strict")
    except UnicodeDecodeError as error:
        raise ArchiveValidationError(f"{label} is not strict UTF-8: {error}") from error

    def exact_object(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
        result: dict[str, Any] = {}
        for key, value in pairs:
            if key in result:
                raise ArchiveValidationError(f"{label} contains duplicate key {key!r}")
            result[key] = value
        return result

    def reject_constant(value: str) -> None:
        raise ArchiveValidationError(f"{label} contains non-finite number {value}")

    try:
        result = json.loads(
            text,
            object_pairs_hook=exact_object,
            parse_constant=reject_constant,
        )
    except json.JSONDecodeError as error:
        raise ArchiveValidationError(f"{label} is invalid JSON: {error}") from error
    _check_depth(result)
    return result


def repository_path(repository_root: Path, relative: str, label: str) -> Path:
    if not isinstance(relative, str) or not relative or len(relative) > 4_096:
        raise ArchiveValidationError(f"{label} is not a bounded repository-relative path")
    value = Path(relative)
    if value.is_absolute() or any(part in {"", ".", ".."} for part in value.parts):
        raise ArchiveValidationError(f"{label} must be a normalized repository-relative path")
    root = repository_root.resolve(strict=True)
    candidate = root.joinpath(*value.parts)
    try:
        resolved = candidate.resolve(strict=True)
    except OSError as error:
        raise ArchiveValidationError(f"{label} is unavailable: {error}") from error
    if resolved != candidate.absolute() or (resolved != root and root not in resolved.parents):
        raise ArchiveValidationError(f"{label} traverses a symlink or escapes the repository")
    return candidate


@dataclass
class RetainedFile:
    path: Path
    descriptor: int
    identity: tuple[int, ...]
    body: bytes
    label: str

    def verify(self) -> None:
        try:
            descriptor_after = os.fstat(self.descriptor)
            path_after = self.path.lstat()
        except OSError as error:
            raise ArchiveValidationError(f"{self.label} disappeared during validation: {error}") from error
        if (
            _stat_identity(descriptor_after) != self.identity
            or _stat_identity(path_after) != self.identity
        ):
            raise ArchiveValidationError(f"{self.label} changed during validation")


@dataclass
class RetainedDirectory:
    path: Path
    identity: tuple[int, ...]
    entries: frozenset[str]
    label: str

    def verify(self) -> None:
        try:
            observed = self.path.lstat()
            entries = frozenset(item.name for item in self.path.iterdir())
        except OSError as error:
            raise ArchiveValidationError(f"{self.label} disappeared during validation: {error}") from error
        if _stat_identity(observed) != self.identity or entries != self.entries:
            raise ArchiveValidationError(f"{self.label} changed during validation")


class ReadOnlyAudit:
    def __init__(self) -> None:
        self.stack = ExitStack()
        self.files: list[RetainedFile] = []
        self.directories: list[RetainedDirectory] = []

    def __enter__(self) -> ReadOnlyAudit:
        self.stack.__enter__()
        return self

    def __exit__(self, exc_type: Any, exc: Any, traceback: Any) -> bool:
        try:
            if exc_type is None:
                for value in self.files:
                    value.verify()
                for value in self.directories:
                    value.verify()
        finally:
            self.stack.__exit__(exc_type, exc, traceback)
        return False

    def directory(
        self,
        path: Path,
        label: str,
        *,
        exact_mode: int,
        exact_entries: set[str],
    ) -> RetainedDirectory:
        try:
            observed = path.lstat()
        except OSError as error:
            raise ArchiveValidationError(f"{label} cannot be inspected: {error}") from error
        if (
            stat.S_ISLNK(observed.st_mode)
            or not stat.S_ISDIR(observed.st_mode)
            or stat.S_IMODE(observed.st_mode) != exact_mode
        ):
            raise ArchiveValidationError(
                f"{label} must be a non-symlink directory with mode {exact_mode:04o}"
            )
        entries = frozenset(item.name for item in path.iterdir())
        if entries != frozenset(exact_entries):
            raise ArchiveValidationError(f"{label} has missing or extra entries")
        retained = RetainedDirectory(path, _stat_identity(observed), entries, label)
        self.directories.append(retained)
        return retained

    def file(
        self,
        path: Path,
        label: str,
        *,
        maximum_bytes: int,
        exact_mode: int | None = None,
    ) -> RetainedFile:
        try:
            before = path.lstat()
        except OSError as error:
            raise ArchiveValidationError(f"{label} cannot be inspected: {error}") from error
        if stat.S_ISLNK(before.st_mode) or not stat.S_ISREG(before.st_mode):
            raise ArchiveValidationError(f"{label} must be a regular non-symlink file")
        mode = stat.S_IMODE(before.st_mode)
        if exact_mode is not None and mode != exact_mode:
            raise ArchiveValidationError(f"{label} must have mode {exact_mode:04o}")
        if before.st_nlink != 1:
            raise ArchiveValidationError(f"{label} must have exactly one hard link")
        if before.st_size <= 0 or before.st_size > maximum_bytes:
            raise ArchiveValidationError(f"{label} is empty or exceeds its byte cap")
        flags = os.O_RDONLY | getattr(os, "O_CLOEXEC", 0)
        if hasattr(os, "O_NOFOLLOW"):
            flags |= os.O_NOFOLLOW
        try:
            descriptor = os.open(path, flags)
        except OSError as error:
            raise ArchiveValidationError(f"{label} cannot be opened read-only: {error}") from error
        self.stack.callback(os.close, descriptor)
        opened = os.fstat(descriptor)
        identity = _stat_identity(opened)
        if not stat.S_ISREG(opened.st_mode) or identity != _stat_identity(before):
            raise ArchiveValidationError(f"{label} changed while being opened")
        chunks: list[bytes] = []
        offset = 0
        while offset < opened.st_size:
            chunk = os.pread(descriptor, min(1024 * 1024, opened.st_size - offset), offset)
            if not chunk:
                break
            chunks.append(chunk)
            offset += len(chunk)
        body = b"".join(chunks)
        try:
            path_after = path.lstat()
            descriptor_after = os.fstat(descriptor)
        except OSError as error:
            raise ArchiveValidationError(f"{label} disappeared while being read: {error}") from error
        if (
            len(body) != opened.st_size
            or _stat_identity(path_after) != identity
            or _stat_identity(descriptor_after) != identity
        ):
            raise ArchiveValidationError(f"{label} changed while being read")
        retained = RetainedFile(path, descriptor, identity, body, label)
        self.files.append(retained)
        return retained


def _software_projection(manifest: dict[str, Any]) -> tuple[dict[str, Any], dict[str, Any]]:
    try:
        materializer = manifest["software"]["materializer"]
        adapter = manifest["software"]["asr_adapter"]
    except (KeyError, TypeError) as error:
        raise ArchiveValidationError("archive manifest software block is malformed") from error
    return (
        {key: materializer.get(key) for key in MATERIALIZER_IDENTITY},
        {
            key: adapter.get(key)
            for key in (
                "name",
                "contract_version",
                "implementation_version",
                "sha256",
                "byte_count",
            )
        },
    )


def _engine_projection(manifest: dict[str, Any]) -> dict[str, Any]:
    try:
        engine = manifest["engine"]
        return {
            "version_label": engine["version_label"],
            "expected_sha256": engine["expected_sha256"],
            "byte_count": engine["byte_count"],
            "revision": engine["build"]["revision"],
            "version_evidence": engine["version_evidence"],
        }
    except (KeyError, TypeError) as error:
        raise ArchiveValidationError("archive manifest engine block is malformed") from error


def _manifest_identity(manifest: dict[str, Any]) -> dict[str, Any]:
    try:
        return {
            "schema_version": manifest["schema_version"],
            "implementation_version": manifest["implementation_version"],
            "database_path": manifest["catalog"]["database_path"],
            "software": manifest["software"],
            "model_registry": manifest["catalog"]["model_registry"],
            "engine": manifest["engine"],
            "model": manifest["model"],
            "profile": manifest["profile"],
            "batch_root": manifest["output"]["batch_root"],
            "asr_output_root": manifest["output"]["asr_output_root"],
            "catalog_binding_sha256": manifest["catalog"]["binding_sha256"],
            "entries": manifest["work_orders"],
        }
    except (KeyError, TypeError) as error:
        raise ArchiveValidationError("archive manifest identity fields are malformed") from error


def validate_manifest(
    manifest: Any,
    archive: dict[str, Any],
) -> list[dict[str, Any]]:
    if not isinstance(manifest, dict):
        raise ArchiveValidationError("archive manifest must be a JSON object")
    if (
        manifest.get("schema_version") != 1
        or manifest.get("implementation_version") != archive["implementation_version"]
        or manifest.get("materializer") != MATERIALIZER_IDENTITY["name"]
        or manifest.get("batch_id") != archive["batch_id"]
        or manifest.get("identity_sha256") != archive["identity_sha256"]
        or manifest.get("batch_relative_path") != f"batches/{archive['batch_id']}"
        or manifest.get("work_order_count") != len(ORDERED_WORK_ORDERS)
        or manifest.get("totals") != EXPECTED_RECEIPT["equivalence"]["totals"]
    ):
        raise ArchiveValidationError("archive manifest identity/totals are not allowlisted")
    materializer, adapter = _software_projection(manifest)
    if materializer != archive["materializer"] or adapter != archive["asr_adapter"]:
        raise ArchiveValidationError("archive manifest software tuple is not allowlisted")
    if _engine_projection(manifest) != archive["engine"]:
        raise ArchiveValidationError("archive manifest engine tuple is not allowlisted")
    identity_sha = sha256_bytes(canonical_bytes(_manifest_identity(manifest)))
    if identity_sha != archive["identity_sha256"] or archive["batch_id"] != f"asrbatch_{identity_sha[:32]}":
        raise ArchiveValidationError("archive manifest deterministic identity does not replay")
    entries = manifest.get("work_orders")
    if not isinstance(entries, list) or len(entries) != len(ORDERED_WORK_ORDERS):
        raise ArchiveValidationError("archive manifest has the wrong work-order count")
    projection = [
        {
            "ordinal": entry.get("ordinal") if isinstance(entry, dict) else None,
            "job_id": entry.get("job_id") if isinstance(entry, dict) else None,
            "file_sha256": entry.get("sha256") if isinstance(entry, dict) else None,
            "canonical_sha256": entry.get("canonical_sha256") if isinstance(entry, dict) else None,
            "byte_count": entry.get("byte_count") if isinstance(entry, dict) else None,
        }
        for entry in entries
    ]
    if projection != ORDERED_WORK_ORDERS:
        raise ArchiveValidationError("archive manifest ordered work-order pins are not allowlisted")
    return entries


def inspect_archive(
    audit: ReadOnlyAudit,
    repository_root: Path,
    archive: dict[str, Any],
) -> tuple[dict[str, Any], list[bytes]]:
    manifest_path = repository_path(
        repository_root,
        archive["manifest_relative_path"],
        f"{archive['batch_id']} manifest",
    )
    batch_dir = manifest_path.parent
    work_orders_dir = batch_dir / "work-orders"
    expected_names = {f"{index:06d}.json" for index in range(1, 9)}
    audit.directory(
        batch_dir,
        f"{archive['batch_id']} batch directory",
        exact_mode=0o500,
        exact_entries={"manifest.json", "work-orders"},
    )
    audit.directory(
        work_orders_dir,
        f"{archive['batch_id']} work-order directory",
        exact_mode=0o500,
        exact_entries=expected_names,
    )
    retained_manifest = audit.file(
        manifest_path,
        f"{archive['batch_id']} manifest",
        maximum_bytes=MAX_MANIFEST_BYTES,
        exact_mode=0o400,
    )
    if (
        len(retained_manifest.body) != archive["manifest_byte_count"]
        or sha256_bytes(retained_manifest.body) != archive["manifest_sha256"]
    ):
        raise ArchiveValidationError(f"{archive['batch_id']} manifest bytes are not allowlisted")
    manifest = parse_json(retained_manifest.body, f"{archive['batch_id']} manifest")
    entries = validate_manifest(manifest, archive)
    bodies: list[bytes] = []
    for index, (entry, expected) in enumerate(zip(entries, ORDERED_WORK_ORDERS, strict=True), start=1):
        expected_path = f"work-orders/{index:06d}.json"
        if entry.get("path") != expected_path:
            raise ArchiveValidationError("archive work-order relative paths are not canonical")
        retained_order = audit.file(
            batch_dir / expected_path,
            f"{archive['batch_id']} work order {index}",
            maximum_bytes=MAX_WORK_ORDER_BYTES,
            exact_mode=0o400,
        )
        body = retained_order.body
        if len(body) != expected["byte_count"] or sha256_bytes(body) != expected["file_sha256"]:
            raise ArchiveValidationError(f"{archive['batch_id']} work order {index} differs from its pin")
        order = parse_json(body, f"{archive['batch_id']} work order {index}")
        if sha256_bytes(canonical_bytes(order)) != expected["canonical_sha256"]:
            raise ArchiveValidationError(
                f"{archive['batch_id']} work order {index} canonical identity differs"
            )
        bodies.append(body)
    return manifest, bodies


def _equivalence_projection(manifest: dict[str, Any]) -> dict[str, Any]:
    value = copy.deepcopy(manifest)
    for key in ("batch_id", "batch_relative_path", "identity_sha256"):
        value.pop(key, None)
    try:
        adapter = value["software"]["asr_adapter"]
        for key in ("byte_count", "implementation_version", "sha256"):
            adapter.pop(key)
    except (KeyError, TypeError) as error:
        raise ArchiveValidationError("archive manifest equivalence projection is malformed") from error
    return value


def _result_projection(result: dict[str, Any]) -> dict[str, Any]:
    try:
        processing_run = result["processing_run"]
        engine = result["engine"]
        return {
            "job_id": result["job_id"],
            "work_order_sha256": result["work_order_sha256"],
            "input_sha256": result["input"]["sha256"],
            "engine_version": engine["version"],
            "engine_sha256": engine["sha256"],
            "adapter_implementation_version": processing_run["implementation_version"],
            "result_key": result["result_key"],
            "recipe_id": result["recipe_id"],
            "recipe_sha256": result["recipe_sha256"],
            "started_at": processing_run["started_at"],
            "completed_at": processing_run["completed_at"],
        }
    except (KeyError, TypeError) as error:
        raise ArchiveValidationError("historical result evidence is malformed") from error


def inspect_result(
    audit: ReadOnlyAudit,
    repository_root: Path,
    record: dict[str, Any],
    label: str,
) -> dict[str, Any]:
    result_path = repository_path(repository_root, record["result_relative_path"], label)
    retained = audit.file(result_path, label, maximum_bytes=MAX_RESULT_BYTES)
    if (
        len(retained.body) != record["result_byte_count"]
        or sha256_bytes(retained.body) != record["result_sha256"]
    ):
        raise ArchiveValidationError(f"{label} bytes differ from the receipt")
    result = parse_json(retained.body, label)
    if not isinstance(result, dict) or result.get("status") != "completed" or result.get("dry_run") is not False:
        raise ArchiveValidationError(f"{label} is not a completed non-dry result")
    expected = {
        key: value
        for key, value in record.items()
        if key not in {"result_relative_path", "result_sha256", "result_byte_count"}
    }
    if _result_projection(result) != expected:
        raise ArchiveValidationError(f"{label} provenance differs from the receipt")
    return result


def inspect_artifact(
    audit: ReadOnlyAudit,
    repository_root: Path,
    relative: str,
    *,
    expected_sha256: str,
    expected_bytes: int,
    label: str,
) -> tuple[bytes, Any]:
    retained = audit.file(
        repository_path(repository_root, relative, label),
        label,
        maximum_bytes=MAX_RESULT_BYTES,
    )
    if len(retained.body) != expected_bytes or sha256_bytes(retained.body) != expected_sha256:
        raise ArchiveValidationError(f"{label} bytes differ from the receipt")
    return retained.body, parse_json(retained.body, label)


def _artifact_metadata(result: dict[str, Any], kind: str) -> dict[str, Any]:
    artifacts = result.get("artifacts")
    if not isinstance(artifacts, list):
        raise ArchiveValidationError("historical result artifacts are malformed")
    matches = [item for item in artifacts if isinstance(item, dict) and item.get("artifact_kind") == kind]
    if len(matches) != 1:
        raise ArchiveValidationError(f"historical result must have one {kind} artifact")
    return matches[0]


def verify_artifact_metadata(
    result: dict[str, Any],
    *,
    kind: str,
    relative_path: str,
    expected_sha256: str,
    expected_bytes: int,
) -> None:
    artifact = _artifact_metadata(result, kind)
    expected_path = ARCHIVE_ORIGIN_ROOT / relative_path
    if (
        artifact.get("storage_uri") != expected_path.as_uri()
        or artifact.get("sha256") != expected_sha256
        or artifact.get("byte_count") != expected_bytes
        or artifact.get("visibility") != "private"
    ):
        raise ArchiveValidationError(f"historical {kind} artifact metadata differs from the receipt")


def inspect_execution_evidence(
    audit: ReadOnlyAudit,
    repository_root: Path,
) -> None:
    subject_record = RESULT_EVIDENCE["subject_job_1"]
    successor_record = RESULT_EVIDENCE["successor_job_1"]
    subject_result = inspect_result(audit, repository_root, subject_record, "subject job-1 result")
    successor_result = inspect_result(audit, repository_root, successor_record, "successor job-1 result")

    for artifact_name, kind in (
        ("raw", "whispercpp_output_json_full"),
        ("normalized", "transcript_normalized_json"),
    ):
        record = RESULT_EVIDENCE["job_1_artifacts"][artifact_name]
        subject_body, subject_json = inspect_artifact(
            audit,
            repository_root,
            record["subject_relative_path"],
            expected_sha256=record["sha256"],
            expected_bytes=record["byte_count"],
            label=f"subject job-1 {artifact_name} artifact",
        )
        successor_body, successor_json = inspect_artifact(
            audit,
            repository_root,
            record["successor_relative_path"],
            expected_sha256=record["sha256"],
            expected_bytes=record["byte_count"],
            label=f"successor job-1 {artifact_name} artifact",
        )
        if subject_body != successor_body or subject_json != successor_json:
            raise ArchiveValidationError(f"job-1 {artifact_name} artifacts are not byte-identical")
        verify_artifact_metadata(
            subject_result,
            kind=kind,
            relative_path=record["subject_relative_path"],
            expected_sha256=record["sha256"],
            expected_bytes=record["byte_count"],
        )
        verify_artifact_metadata(
            successor_result,
            kind=kind,
            relative_path=record["successor_relative_path"],
            expected_sha256=record["sha256"],
            expected_bytes=record["byte_count"],
        )
        if artifact_name == "normalized":
            if subject_json != subject_result.get("transcript") or successor_json != successor_result.get("transcript"):
                raise ArchiveValidationError("job-1 normalized artifacts differ from their result envelopes")

    job_2 = RESULT_EVIDENCE["successor_job_2_inverted_offsets"]
    result_record = {
        "result_relative_path": job_2["result_relative_path"],
        "result_sha256": job_2["result_sha256"],
        "result_byte_count": job_2["result_byte_count"],
    }
    job_2_path = repository_path(repository_root, result_record["result_relative_path"], "successor job-2 result")
    retained_job_2 = audit.file(job_2_path, "successor job-2 result", maximum_bytes=MAX_RESULT_BYTES)
    if (
        len(retained_job_2.body) != result_record["result_byte_count"]
        or sha256_bytes(retained_job_2.body) != result_record["result_sha256"]
    ):
        raise ArchiveValidationError("successor job-2 result bytes differ from the receipt")
    job_2_result = parse_json(retained_job_2.body, "successor job-2 result")
    if not isinstance(job_2_result, dict):
        raise ArchiveValidationError("successor job-2 result is malformed")
    if (
        job_2_result.get("job_id") != job_2["job_id"]
        or job_2_result.get("work_order_sha256") != job_2["work_order_sha256"]
        or job_2_result.get("status") != "completed"
        or job_2_result.get("processing_run", {}).get("implementation_version")
        != job_2["adapter_implementation_version"]
    ):
        raise ArchiveValidationError("successor job-2 result provenance differs from the receipt")
    _, normalized = inspect_artifact(
        audit,
        repository_root,
        job_2["normalized_relative_path"],
        expected_sha256=job_2["normalized_sha256"],
        expected_bytes=job_2["normalized_byte_count"],
        label="successor job-2 normalized artifact",
    )
    if not isinstance(normalized, dict) or normalized != job_2_result.get("transcript"):
        raise ArchiveValidationError("successor job-2 normalized artifact differs from result.json")
    verify_artifact_metadata(
        job_2_result,
        kind="transcript_normalized_json",
        relative_path=job_2["normalized_relative_path"],
        expected_sha256=job_2["normalized_sha256"],
        expected_bytes=job_2["normalized_byte_count"],
    )
    invalid_tokens: list[dict[str, Any]] = []
    try:
        for segment in normalized["segments"]:
            for token in segment["tokens"]:
                if token.get("timing_quality_flags") == [job_2["timing_quality_flag"]]:
                    invalid_tokens.append(token)
    except (KeyError, TypeError) as error:
        raise ArchiveValidationError("successor job-2 token evidence is malformed") from error
    if len(invalid_tokens) != job_2["affected_token_count"] or any(
        token.get("timing_state") != "unavailable"
        or token.get("start_ms") is not None
        or token.get("end_ms") is not None
        or not isinstance(token.get("original_offsets"), dict)
        or token["original_offsets"].get("from", 0) <= token["original_offsets"].get("to", 0)
        for token in invalid_tokens
    ):
        raise ArchiveValidationError("successor job-2 inverted-offset evidence differs from the receipt")


def validate_disposition(
    receipt_path: Path,
    *,
    repository_root: Path = REPOSITORY_ROOT,
) -> dict[str, Any]:
    root = repository_root.resolve(strict=True)
    expected_receipt_path = repository_path(root, RECEIPT_RELATIVE_PATH, "archival disposition receipt")
    try:
        supplied_receipt_path = receipt_path.absolute()
    except OSError as error:
        raise ArchiveValidationError(f"archival disposition receipt path is invalid: {error}") from error
    if supplied_receipt_path != expected_receipt_path:
        raise ArchiveValidationError("receipt is not stored at its one allowlisted repository-relative path")

    with ReadOnlyAudit() as audit:
        audit.directory(
            expected_receipt_path.parent,
            "archival disposition directory",
            exact_mode=0o700,
            exact_entries={expected_receipt_path.name},
        )
        retained_receipt = audit.file(
            expected_receipt_path,
            "archival disposition receipt",
            maximum_bytes=MAX_RECEIPT_BYTES,
            exact_mode=0o400,
        )
        receipt = parse_json(retained_receipt.body, "archival disposition receipt")
        if (
            len(retained_receipt.body) != RECEIPT_BYTE_COUNT
            or sha256_bytes(retained_receipt.body) != RECEIPT_SHA256
            or retained_receipt.body != canonical_bytes(receipt) + b"\n"
            or receipt != EXPECTED_RECEIPT
        ):
            raise ArchiveValidationError("receipt differs from the exact closed archival disposition")

        subject_manifest, subject_orders = inspect_archive(audit, root, ARCHIVES["subject"])
        successor_manifest, successor_orders = inspect_archive(audit, root, ARCHIVES["successor"])
        if subject_orders != successor_orders:
            raise ArchiveValidationError("subject and successor work-order trees are not byte-identical")
        if _equivalence_projection(subject_manifest) != _equivalence_projection(successor_manifest):
            raise ArchiveValidationError("archive manifests differ outside the declared supersession fields")
        inspect_execution_evidence(audit, root)

    return {
        "status": "valid",
        "validator": VALIDATOR_NAME,
        "implementation_version": IMPLEMENTATION_VERSION,
        "receipt_relative_path": RECEIPT_RELATIVE_PATH,
        "receipt_sha256": RECEIPT_SHA256,
        "subject_batch_id": SUBJECT_BATCH_ID,
        "successor_batch_id": SUCCESSOR_BATCH_ID,
        "disposition": EXPECTED_RECEIPT["disposition"]["state"],
        "execution_authority": "none",
        "import_authority": "none",
        "publication_authority": "none",
    }


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Validate the one closed, non-executable whisper.cpp batch archive disposition"
    )
    commands = parser.add_subparsers(dest="command", required=True)
    validate = commands.add_parser("validate", help="read and validate; never execute or write")
    validate.add_argument("--receipt", required=True)
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    if args.command != "validate":
        raise ArchiveValidationError("only validation is supported")
    try:
        result = validate_disposition(Path(args.receipt))
    except (ArchiveValidationError, OSError) as error:
        print(
            json.dumps(
                {
                    "status": "invalid",
                    "error": {
                        "type": type(error).__name__,
                        "message": str(error),
                    },
                },
                ensure_ascii=False,
                sort_keys=True,
            ),
            file=sys.stderr,
        )
        return 2
    print(json.dumps(result, ensure_ascii=False, sort_keys=True, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
