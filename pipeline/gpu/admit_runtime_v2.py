#!/usr/bin/env python3
"""Restart-portable runtime admission for the immutable GPU execution lane.

Version 1 receipts are frozen historical evidence.  This schema removes persisted
Linux device numbers, binds one stable Btrfs root registration, one deterministic
execution image, one complete production profile, and one root-owned external
launcher installation.  An admitted receipt requires every long-form accuracy,
throughput, thermal, isolation, scheduler, and crash gate.  Candidate receipts are
useful for bounded canaries but are not production execution authority.

Ordinary replay is intentionally cheap: it validates small canonical documents,
stable file hashes for the launcher/tools, root ownership/modes, Btrfs UUID, and the
execution-image receipt.  It does not rescan runtime, model, or wheelhouse trees.
``deep-audit`` separately hashes the complete SquashFS image.
"""

from __future__ import annotations

import argparse
import hashlib
import importlib.util
import json
import os
import re
import secrets
import stat
import sys
from pathlib import Path
from types import ModuleType
from typing import Any


KIND = "himr_gpu_runtime_admission_receipt"
SPEC_KIND = "himr_gpu_runtime_admission_spec"
SCHEMA_VERSION = 2
IMPLEMENTATION_VERSION = "0.3.0"
MAX_JSON_BYTES = 8 * 1024 * 1024
MAX_TOOL_BYTES = 128 * 1024 * 1024
MAX_RAW_GATE_EVIDENCE_BYTES = 512 * 1024 * 1024
CANDIDATE_CLOSURE_KIND = "himr_gpu_runtime_candidate_closure"
SHA256_RE = re.compile(r"[0-9a-f]{64}\Z")
PACKAGE_RE = re.compile(r"[a-z0-9][a-z0-9-]{0,127}\Z")
VERSION_RE = re.compile(r"[A-Za-z0-9][A-Za-z0-9.+_-]{0,127}\Z")
TOOL_NAME_RE = re.compile(r"[a-z][a-z0-9_]{0,63}\Z")

GPU_DIR = Path(__file__).resolve().parent


class RuntimeAdmissionV2Error(RuntimeError):
    """A portable runtime admission spec, receipt, or dependency failed."""


def _load_module(name: str, path: Path) -> ModuleType:
    spec = importlib.util.spec_from_file_location(name, path)
    if spec is None or spec.loader is None:
        raise RuntimeAdmissionV2Error(f"cannot load required module {path}")
    module = importlib.util.module_from_spec(spec)
    sys.modules[name] = module
    try:
        spec.loader.exec_module(module)
    except Exception:
        sys.modules.pop(name, None)
        raise
    return module


PORTABLE = _load_module("himr_gpu_portable_root_for_runtime_v2", GPU_DIR / "portable_root.py")
IMAGE = _load_module("himr_gpu_execution_image_for_runtime_v2", GPU_DIR / "build_execution_image.py")
PROFILE = _load_module("himr_gpu_profile_for_runtime_v2", GPU_DIR / "production_profile_v2.py")
GATES = _load_module("himr_gpu_gates_for_runtime_v2", GPU_DIR / "gpu_admission_evidence.py")


POLICY = {
    "visibility": "private",
    "network_access": False,
    "runtime_model_tree_scan_per_execution": False,
    "wheelhouse_execution_dependency": False,
    "deep_audit_is_separate": True,
    "persisted_numeric_device_authority": False,
    "external_trust_anchor_required": True,
    "publication_authority": "none",
    "catalogue_mutation_authority": "none",
    "identity_authority": "none",
    "archive_authority": "none",
    "deletion_authority": "none",
}

REQUIRED_MAPPING_LAYOUT = {
    "runtime_root": ("/opt/himr-gpu/runtime", "runtime_root"),
    "python_executable": (
        "/opt/himr-gpu/runtime/bin/python3.12",
        "executable",
    ),
    "model_bundle": ("/opt/himr-gpu/model", "model_bundle"),
    "model_root": ("/opt/himr-gpu/model/snapshot", "model_root"),
    "application_root": ("/opt/himr-gpu/app", "application_root"),
    "application_support_root": (
        "/opt/himr-gpu/corpus/src",
        "application_support_root",
    ),
    "adapter_source": (
        "/opt/himr-gpu/app/production_asr_v5.py",
        "python_source",
    ),
    "worker_source": (
        "/opt/himr-gpu/app/production_asr_batch_v2.py",
        "python_source",
    ),
    "verified_loader": (
        "/opt/himr-gpu/app/verified_dependency_loader.py",
        "python_source",
    ),
    "model_admission_helper": (
        "/opt/himr-gpu/app/admit_hf_model.py",
        "python_source",
    ),
    "runtime_admission_helper": (
        "/opt/himr-gpu/app/admit_runtime_v2.py",
        "python_source",
    ),
    "cublas_library_directory": (
        "/opt/himr-gpu/runtime/lib/python3.12/site-packages/nvidia/cublas/lib",
        "shared_library_directory",
    ),
}
REQUIRED_MAPPING_NAMES = frozenset(REQUIRED_MAPPING_LAYOUT)

REQUIRED_PACKAGE_VERSIONS = {
    "av": "18.1.0",
    "ctranslate2": "4.8.1",
    "faster-whisper": "1.2.1",
    "huggingface-hub": "1.29.0",
    "nvidia-cublas-cu12": "12.9.2.10",
    "nvidia-ml-py": "13.610.43",
    "numpy": "2.5.2",
    "pyyaml": "6.0.3",
    "tokenizers": "0.23.1",
    "tqdm": "4.70.0",
}
REQUIRED_PACKAGES = frozenset(REQUIRED_PACKAGE_VERSIONS)
REQUIRED_SYSTEM_TOOL_PATHS = {
    "bubblewrap": "/usr/bin/bwrap",
    "fusermount": "/usr/bin/fusermount3",
    "nvidia_smi": "/usr/bin/nvidia-smi",
    "host_python": "/usr/bin/python3.14",
    "squashfuse": "/usr/bin/squashfuse_ll",
    "systemctl": "/usr/bin/systemctl",
    "systemd_run": "/usr/bin/systemd-run",
}
TRUSTED_LAUNCHER_PATH = "/usr/local/libexec/himr-gpu/trusted-launcher-v2"
TRUSTED_LAUNCHER_PROFILE_PATH = "/etc/himr-gpu/launcher-profile-v2.json"

def canonical_bytes(value: Any) -> bytes:
    return (
        json.dumps(
            value,
            ensure_ascii=False,
            sort_keys=True,
            separators=(",", ":"),
            allow_nan=False,
        )
        + "\n"
    ).encode("utf-8")


def sha256_bytes(value: bytes) -> str:
    return hashlib.sha256(value).hexdigest()


def _exact(value: Any, label: str, keys: set[str] | frozenset[str]) -> dict[str, Any]:
    if not isinstance(value, dict) or set(value) != set(keys):
        observed = sorted(value) if isinstance(value, dict) else type(value).__name__
        raise RuntimeAdmissionV2Error(f"{label} has unexpected fields: {observed}")
    return value


def _integer(value: Any, label: str, minimum: int, maximum: int = 2**63 - 1) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or not minimum <= value <= maximum:
        raise RuntimeAdmissionV2Error(f"{label} must be an integer within [{minimum}, {maximum}]")
    return value


def _digest(value: Any, label: str) -> str:
    if not isinstance(value, str) or not SHA256_RE.fullmatch(value):
        raise RuntimeAdmissionV2Error(f"{label} must be a lowercase SHA-256")
    return value


def _absolute(value: Any, label: str) -> Path:
    try:
        return PORTABLE.normalized_absolute_path(value, label)
    except PORTABLE.PortableRootError as error:
        raise RuntimeAdmissionV2Error(str(error)) from error


def _unique(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
    result: dict[str, Any] = {}
    for key, value in pairs:
        if key in result:
            raise RuntimeAdmissionV2Error(f"JSON contains duplicate key {key!r}")
        result[key] = value
    return result


def _parse(body: bytes, label: str) -> Any:
    def reject(value: str) -> None:
        raise RuntimeAdmissionV2Error(f"{label} contains non-finite value {value}")

    try:
        return json.loads(
            body.decode("utf-8"), object_pairs_hook=_unique, parse_constant=reject
        )
    except RuntimeAdmissionV2Error:
        raise
    except (UnicodeDecodeError, ValueError, RecursionError) as error:
        raise RuntimeAdmissionV2Error(f"{label} is not strict JSON: {error}") from error


def _stable_file(
    path: Path,
    label: str,
    *,
    maximum: int,
    exact_mode: int | None = None,
    allowed_uids: set[int] | None = None,
) -> tuple[bytes, os.stat_result]:
    flags = os.O_RDONLY | getattr(os, "O_CLOEXEC", 0) | getattr(os, "O_NONBLOCK", 0)
    flags |= getattr(os, "O_NOFOLLOW", 0)
    try:
        lexical = path.lstat()
        descriptor = os.open(path, flags)
    except OSError as error:
        raise RuntimeAdmissionV2Error(f"cannot inspect {label}: {error}") from error
    try:
        opened = os.fstat(descriptor)
        identity = lambda item: (
            item.st_dev,
            item.st_ino,
            item.st_mode,
            item.st_nlink,
            item.st_uid,
            item.st_gid,
            item.st_size,
            item.st_mtime_ns,
            item.st_ctime_ns,
        )
        if (
            stat.S_ISLNK(lexical.st_mode)
            or not stat.S_ISREG(opened.st_mode)
            or identity(lexical) != identity(opened)
            or opened.st_nlink != 1
            or opened.st_size < 1
            or opened.st_size > maximum
            or (exact_mode is not None and stat.S_IMODE(opened.st_mode) != exact_mode)
            or (allowed_uids is not None and opened.st_uid not in allowed_uids)
        ):
            raise RuntimeAdmissionV2Error(f"{label} metadata is unsafe")
        chunks: list[bytes] = []
        remaining = opened.st_size
        while remaining:
            chunk = os.read(descriptor, min(1024 * 1024, remaining))
            if not chunk:
                raise RuntimeAdmissionV2Error(f"{label} ended before its sealed size")
            chunks.append(chunk)
            remaining -= len(chunk)
        if os.read(descriptor, 1):
            raise RuntimeAdmissionV2Error(f"{label} grew while read")
        after_fd = os.fstat(descriptor)
        after_path = path.lstat()
        if identity(after_fd) != identity(opened) or identity(after_path) != identity(opened):
            raise RuntimeAdmissionV2Error(f"{label} changed while read")
        return b"".join(chunks), opened
    finally:
        os.close(descriptor)


def _reference(value: Any, label: str) -> dict[str, Any]:
    item = _exact(value, label, {"path", "sha256"})
    return {"path": str(_absolute(item["path"], f"{label}.path")), "sha256": _digest(item["sha256"], f"{label}.sha256")}


def _read_reference(
    reference: dict[str, Any],
    label: str,
    *,
    maximum: int = MAX_JSON_BYTES,
    exact_mode: int | None = 0o400,
    allowed_uids: set[int] | None = None,
) -> tuple[bytes, dict[str, Any], os.stat_result]:
    body, info = _stable_file(
        Path(reference["path"]),
        label,
        maximum=maximum,
        exact_mode=exact_mode,
        allowed_uids=allowed_uids,
    )
    observed = sha256_bytes(body)
    if observed != reference["sha256"]:
        raise RuntimeAdmissionV2Error(f"{label} SHA-256 differs from its reference")
    return body, {**reference, "byte_count": len(body)}, info


def _json_reference(
    reference: dict[str, Any],
    label: str,
    *,
    admitted: bool = False,
) -> tuple[Any, dict[str, Any], os.stat_result]:
    body, normalized, info = _read_reference(
        reference,
        label,
        exact_mode=None,
        allowed_uids={0} if admitted else {os.geteuid()},
    )
    owner_mode = (info.st_uid, stat.S_IMODE(info.st_mode))
    expected_owner_mode = (0, 0o444) if admitted else (os.geteuid(), 0o400)
    if owner_mode != expected_owner_mode:
        raise RuntimeAdmissionV2Error(
            f"{label} must be current-user mode 0400 or root-owned mode 0444"
        )
    value = _parse(body, label)
    if body != canonical_bytes(value):
        raise RuntimeAdmissionV2Error(f"{label} is not canonical JSON")
    return value, normalized, info


def _tool_reference(value: Any, label: str) -> dict[str, Any]:
    item = _exact(value, label, {"name", "path", "sha256"})
    name = item["name"]
    if not isinstance(name, str) or not TOOL_NAME_RE.fullmatch(name):
        raise RuntimeAdmissionV2Error(f"{label}.name is invalid")
    reference = _reference({"path": item["path"], "sha256": item["sha256"]}, label)
    body, observed, info = _read_reference(
        reference,
        label,
        maximum=MAX_TOOL_BYTES,
        exact_mode=None,
        allowed_uids={0},
    )
    del body
    mode = stat.S_IMODE(info.st_mode)
    if not mode & 0o100 or mode & 0o022:
        raise RuntimeAdmissionV2Error(f"{label} must be root-owned executable and non-writable by group/other")
    return {"name": name, **observed, "uid": info.st_uid, "mode": f"{mode:04o}"}


def _trusted_ancestor_policy(path: Path, expected_uid: int, label: str) -> None:
    current = path
    while True:
        info = current.lstat()
        if stat.S_ISLNK(info.st_mode) or info.st_uid != expected_uid or stat.S_IMODE(info.st_mode) & 0o022:
            raise RuntimeAdmissionV2Error(f"{label} has a mutable or untrusted ancestor: {current}")
        if current == Path("/"):
            break
        current = current.parent


def _trusted_reference(
    value: Any, label: str, *, executable: bool, enforce_root: bool
) -> dict[str, Any]:
    reference = _reference(value, label)
    body, observed, info = _read_reference(
        reference,
        label,
        maximum=MAX_TOOL_BYTES,
        exact_mode=None,
        allowed_uids={0} if enforce_root else {0, os.geteuid()},
    )
    del body
    mode = stat.S_IMODE(info.st_mode)
    if mode & 0o022 or (executable and not mode & 0o100) or (not executable and mode & 0o111):
        raise RuntimeAdmissionV2Error(f"{label} mode is not trusted")
    if enforce_root:
        _trusted_ancestor_policy(Path(reference["path"]), 0, label)
    return {**observed, "uid": info.st_uid, "mode": f"{mode:04o}"}


SPEC_FIELDS = {
    "kind",
    "schema_version",
    "requested_state",
    "root_registration",
    "execution_image",
    "production_profile",
    "trusted_install",
    "runtime",
    "gates",
    "policy",
}

GATE_FIELDS = frozenset(GATES.GATES)


def _normalize_runtime(value: Any) -> dict[str, Any]:
    item = _exact(value, "runtime", {"python_version", "packages", "required_mapping_names"})
    python_version = item["python_version"]
    if python_version != "3.12.14":
        raise RuntimeAdmissionV2Error("runtime requires CPython 3.12.14")
    packages = item["packages"]
    if not isinstance(packages, dict) or set(packages) != REQUIRED_PACKAGES:
        raise RuntimeAdmissionV2Error("runtime packages are not the exact required set")
    normalized_packages: dict[str, str] = {}
    for name, version in sorted(packages.items()):
        if not PACKAGE_RE.fullmatch(name) or not isinstance(version, str) or not VERSION_RE.fullmatch(version):
            raise RuntimeAdmissionV2Error(f"runtime package pin is invalid: {name}")
        normalized_packages[name] = version
    if normalized_packages != REQUIRED_PACKAGE_VERSIONS:
        raise RuntimeAdmissionV2Error("runtime package versions are not exact")
    mapping_names = item["required_mapping_names"]
    if not isinstance(mapping_names, list) or mapping_names != sorted(REQUIRED_MAPPING_NAMES):
        raise RuntimeAdmissionV2Error("runtime required mapping names are not exact")
    return {
        "python_version": python_version,
        "packages": normalized_packages,
        "required_mapping_names": sorted(REQUIRED_MAPPING_NAMES),
    }


def _normalize_gate_references(value: Any) -> dict[str, dict[str, str] | None]:
    item = _exact(value, "gates", GATE_FIELDS)
    result: dict[str, dict[str, str] | None] = {}
    for name in GATES.GATES:
        result[name] = None if item[name] is None else _reference(item[name], f"gate {name}")
    return result


def normalize_spec(value: Any) -> dict[str, Any]:
    item = _exact(value, "runtime admission spec", SPEC_FIELDS)
    if item["kind"] != SPEC_KIND or item["schema_version"] != SCHEMA_VERSION:
        raise RuntimeAdmissionV2Error("runtime admission spec kind/version is unsupported")
    requested_state = item["requested_state"]
    if requested_state not in {"candidate", "admitted"}:
        raise RuntimeAdmissionV2Error("requested_state must be candidate or admitted")
    root = _exact(
        item["root_registration"],
        "root_registration",
        {"path", "sha256", "registration_id", "root_id"},
    )
    registration_id = root["registration_id"]
    root_id = root["root_id"]
    if not isinstance(registration_id, str) or not re.fullmatch(r"gpurootreg_[0-9a-f]{32}", registration_id):
        raise RuntimeAdmissionV2Error("root registration ID is invalid")
    if not isinstance(root_id, str) or not PORTABLE.ROOT_ID_RE.fullmatch(root_id):
        raise RuntimeAdmissionV2Error("root ID is invalid")
    image = _exact(
        item["execution_image"],
        "execution_image",
        {"receipt_path", "receipt_sha256", "identity_sha256"},
    )
    profile = _exact(
        item["production_profile"],
        "production_profile",
        {"path", "sha256", "identity_sha256"},
    )
    install = _exact(
        item["trusted_install"],
        "trusted_install",
        {"owner_uid", "launcher", "launcher_profile", "system_tools"},
    )
    tools = install["system_tools"]
    if not isinstance(tools, list) or not 3 <= len(tools) <= 16:
        raise RuntimeAdmissionV2Error("trusted system tool set is outside its bound")
    normalized_tools = []
    for ordinal, tool in enumerate(tools, start=1):
        row = _exact(tool, f"system tool {ordinal}", {"name", "path", "sha256"})
        name = row["name"]
        if not isinstance(name, str) or not TOOL_NAME_RE.fullmatch(name):
            raise RuntimeAdmissionV2Error("system tool name is invalid")
        normalized_tools.append({"name": name, **_reference({"path": row["path"], "sha256": row["sha256"]}, f"system tool {name}")})
    normalized_tools.sort(key=lambda row: row["name"])
    if len({row["name"] for row in normalized_tools}) != len(normalized_tools):
        raise RuntimeAdmissionV2Error("system tool names are duplicated")
    if {row["name"] for row in normalized_tools} != set(REQUIRED_SYSTEM_TOOL_PATHS):
        raise RuntimeAdmissionV2Error("trusted system tool set is not exact")
    if any(
        row["path"] != REQUIRED_SYSTEM_TOOL_PATHS[row["name"]]
        for row in normalized_tools
    ):
        raise RuntimeAdmissionV2Error("trusted system tool paths are not exact")
    if item["policy"] != POLICY:
        raise RuntimeAdmissionV2Error("runtime admission spec policy is unsupported")
    launcher_reference = _reference(install["launcher"], "trusted launcher")
    launcher_profile_reference = _reference(
        install["launcher_profile"], "trusted launcher profile"
    )
    owner_uid = _integer(
        install["owner_uid"], "trusted install owner UID", 0, 2**31 - 1
    )
    if requested_state == "admitted":
        if owner_uid != 0:
            raise RuntimeAdmissionV2Error("admitted trusted install must be root-owned")
        if launcher_reference["path"] != TRUSTED_LAUNCHER_PATH:
            raise RuntimeAdmissionV2Error("admitted trusted launcher path is not exact")
        if launcher_profile_reference["path"] != TRUSTED_LAUNCHER_PROFILE_PATH:
            raise RuntimeAdmissionV2Error("admitted launcher profile path is not exact")
    else:
        if owner_uid not in {0, os.geteuid()}:
            raise RuntimeAdmissionV2Error(
                "candidate trusted install must be root-owned or owned by the current user"
            )
        if owner_uid == 0 and (
            launcher_reference["path"] != TRUSTED_LAUNCHER_PATH
            or launcher_profile_reference["path"]
            != TRUSTED_LAUNCHER_PROFILE_PATH
        ):
            raise RuntimeAdmissionV2Error(
                "root-owned candidate install paths must remain exact"
            )
    return {
        "kind": SPEC_KIND,
        "schema_version": SCHEMA_VERSION,
        "requested_state": requested_state,
        "root_registration": {
            "path": str(_absolute(root["path"], "root registration path")),
            "sha256": _digest(root["sha256"], "root registration SHA-256"),
            "registration_id": registration_id,
            "root_id": root_id,
        },
        "execution_image": {
            "receipt_path": str(_absolute(image["receipt_path"], "execution image receipt path")),
            "receipt_sha256": _digest(image["receipt_sha256"], "execution image receipt SHA-256"),
            "identity_sha256": _digest(image["identity_sha256"], "execution image identity"),
        },
        "production_profile": {
            "path": str(_absolute(profile["path"], "production profile path")),
            "sha256": _digest(profile["sha256"], "production profile SHA-256"),
            "identity_sha256": _digest(profile["identity_sha256"], "production profile identity"),
        },
        "trusted_install": {
            "owner_uid": owner_uid,
            "launcher": launcher_reference,
            "launcher_profile": launcher_profile_reference,
            "system_tools": normalized_tools,
        },
        "runtime": _normalize_runtime(item["runtime"]),
        "gates": _normalize_gate_references(item["gates"]),
        "policy": dict(POLICY),
    }


def _load_profile(
    spec: dict[str, Any], *, admitted: bool
) -> tuple[dict[str, Any], dict[str, Any]]:
    reference = {
        "path": spec["production_profile"]["path"],
        "sha256": spec["production_profile"]["sha256"],
    }
    value, observed, _ = _json_reference(
        reference, "production profile", admitted=admitted
    )
    try:
        profile = PROFILE.validate_profile(value)
    except PROFILE.ProfileError as error:
        raise RuntimeAdmissionV2Error(f"production profile failed replay: {error}") from error
    if profile["identity_sha256"] != spec["production_profile"]["identity_sha256"]:
        raise RuntimeAdmissionV2Error("production profile identity differs from the spec")
    return profile, observed | {"identity_sha256": profile["identity_sha256"]}


def _load_root(
    spec: dict[str, Any], *, admitted: bool
) -> tuple[dict[str, Any], dict[str, Any]]:
    reference = spec["root_registration"]
    try:
        registration = PORTABLE.load_registration(
            reference["path"],
            reference["sha256"],
            expected_root_id=reference["root_id"],
            expected_tier="hot_main_drive",
            expected_document_uid=0 if admitted else os.geteuid(),
            expected_document_mode=0o444 if admitted else 0o400,
        )
        with PORTABLE.RetainedRoot.open(
            registration,
            expected_root_id=reference["root_id"],
            expected_tier="hot_main_drive",
        ) as retained:
            retained.verify()
    except PORTABLE.PortableRootError as error:
        raise RuntimeAdmissionV2Error(f"hot-root registration failed replay: {error}") from error
    if registration["registration_id"] != reference["registration_id"]:
        raise RuntimeAdmissionV2Error("hot-root registration ID differs from the spec")
    path = Path(reference["path"])
    return registration, {
        "path": str(path),
        "sha256": reference["sha256"],
        "byte_count": path.stat().st_size,
        "registration_id": registration["registration_id"],
        "identity_sha256": registration["identity_sha256"],
    }


def _require_mapping_layout(rows: Any) -> None:
    if not isinstance(rows, list):
        raise RuntimeAdmissionV2Error("execution image mappings must be an array")
    names = {row.get("name") for row in rows if isinstance(row, dict)}
    if len(names) != len(rows) or names != REQUIRED_MAPPING_NAMES:
        raise RuntimeAdmissionV2Error(
            "execution image mappings are not the exact runtime closure"
        )
    observed_layout = {
        row["name"]: (row.get("sandbox_path"), row.get("role")) for row in rows
    }
    if observed_layout != REQUIRED_MAPPING_LAYOUT:
        raise RuntimeAdmissionV2Error(
            "execution image mapping roles or sandbox paths differ from the exact layout"
        )


def _load_image(spec: dict[str, Any], *, deep: bool, admitted: bool) -> tuple[dict[str, Any], dict[str, Any]]:
    reference = spec["execution_image"]
    try:
        receipt = IMAGE.load_receipt(
            reference["receipt_path"],
            reference["receipt_sha256"],
            verify_image=deep,
            expected_image_uid=0 if admitted else None,
        )
    except IMAGE.ExecutionImageError as error:
        raise RuntimeAdmissionV2Error(f"execution image failed replay: {error}") from error
    if receipt["identity_sha256"] != reference["identity_sha256"]:
        raise RuntimeAdmissionV2Error("execution image identity differs from the spec")
    _require_mapping_layout(receipt["logical_mappings"])
    image_path = Path(receipt["image"]["path"])
    info = image_path.lstat()
    expected_mode = 0o444 if admitted else int(receipt["image"]["mode"], 8)
    if (
        stat.S_ISLNK(info.st_mode)
        or not stat.S_ISREG(info.st_mode)
        or info.st_nlink != 1
        or stat.S_IMODE(info.st_mode) != expected_mode
        or (not admitted and expected_mode not in {0o400, 0o444})
    ):
        raise RuntimeAdmissionV2Error("execution image metadata is unsafe")
    if admitted:
        if info.st_uid != 0:
            raise RuntimeAdmissionV2Error("admitted execution image must be root-owned")
        _trusted_ancestor_policy(image_path, 0, "execution image")
    return receipt, {
        "receipt_path": reference["receipt_path"],
        "receipt_sha256": reference["receipt_sha256"],
        "receipt_byte_count": Path(reference["receipt_path"]).stat().st_size,
        "identity_sha256": receipt["identity_sha256"],
        "image": receipt["image"],
        "logical_mappings": receipt["logical_mappings"],
    }


def _runtime_candidate_closure(
    *,
    root_registration: dict[str, Any],
    root: dict[str, Any],
    execution_image: dict[str, Any],
    production_profile: dict[str, Any],
    production_profile_file: dict[str, Any],
    trusted_install: dict[str, Any],
    runtime: dict[str, Any],
) -> dict[str, Any]:
    """Bind every mutable external input which can invalidate admission evidence.

    Gate evidence is expensive (the thermal soak alone is eight hours).  It may be
    reused only for byte-identical launcher, launcher profile, system tools, root
    registration, execution image, production profile, and runtime contract.
    """

    core = {
        "kind": CANDIDATE_CLOSURE_KIND,
        "schema_version": SCHEMA_VERSION,
        "root_registration": root_registration,
        "root": root,
        "execution_image": execution_image,
        "production_profile_identity_sha256": production_profile[
            "identity_sha256"
        ],
        "production_profile_file": production_profile_file,
        "trusted_install": trusted_install,
        "runtime": runtime,
        "policy": dict(POLICY),
    }
    return {
        **core,
        "identity_sha256": sha256_bytes(canonical_bytes(core)),
    }


def _load_gates(
    spec: dict[str, Any],
    *,
    require_all: bool,
    profile_identity: str,
    image_identity: str,
    runtime_candidate_identity: str,
    hot_root_path: str,
) -> dict[str, Any]:
    result: dict[str, Any] = {}
    for name in GATES.GATES:
        reference = spec["gates"][name]
        if reference is None:
            if require_all:
                raise RuntimeAdmissionV2Error(f"admitted runtime is missing gate {name}")
            result[name] = None
            continue
        value, observed, _ = _json_reference(
            reference, f"admission gate {name}", admitted=require_all
        )
        try:
            gate = GATES.validate_gate(value)
        except GATES.AdmissionEvidenceError as error:
            raise RuntimeAdmissionV2Error(f"admission gate {name} failed replay: {error}") from error
        if (
            gate["gate"] != name
            or gate["production_profile_identity_sha256"] != profile_identity
            or gate["execution_image_identity_sha256"] != image_identity
            or gate["runtime_candidate_identity_sha256"]
            != runtime_candidate_identity
        ):
            raise RuntimeAdmissionV2Error(f"admission gate {name} lineage is inconsistent")
        raw = gate["raw_evidence"]
        raw_path = _absolute(raw["path"], f"admission gate {name} raw evidence path")
        try:
            raw_path.relative_to(Path(hot_root_path))
        except ValueError as error:
            raise RuntimeAdmissionV2Error(
                f"admission gate {name} raw evidence is outside the hot root"
            ) from error
        raw_body, raw_info = _stable_file(
            raw_path,
            f"admission gate {name} raw evidence",
            maximum=MAX_RAW_GATE_EVIDENCE_BYTES,
            exact_mode=0o444 if require_all else 0o400,
            allowed_uids={0} if require_all else {os.geteuid()},
        )
        if len(raw_body) != raw["byte_count"] or sha256_bytes(raw_body) != raw["sha256"]:
            raise RuntimeAdmissionV2Error(
                f"admission gate {name} raw evidence differs from its binding"
            )
        raw_value = _parse(raw_body, f"admission gate {name} raw evidence")
        try:
            GATES.validate_gate_against_raw(gate, raw_value, raw_body=raw_body)
        except GATES.AdmissionEvidenceError as error:
            raise RuntimeAdmissionV2Error(
                f"admission gate {name} raw evidence failed deep replay: {error}"
            ) from error
        del raw_body, raw_info, raw_value
        result[name] = observed | {
            "identity_sha256": gate["identity_sha256"],
            "gate_id": gate["gate_id"],
            "status": gate["status"],
            "runtime_candidate_identity_sha256": gate[
                "runtime_candidate_identity_sha256"
            ],
        }
    return result


def _load_install(
    spec: dict[str, Any], *, admitted: bool, hot_root_path: str
) -> dict[str, Any]:
    install = spec["trusted_install"]
    launcher = _trusted_reference(
        install["launcher"], "trusted launcher", executable=True, enforce_root=admitted
    )
    launcher_profile = _trusted_reference(
        install["launcher_profile"],
        "trusted launcher profile",
        executable=False,
        enforce_root=admitted,
    )
    tools = [_tool_reference(row, f"system tool {row['name']}") for row in install["system_tools"]]
    tools.sort(key=lambda row: row["name"])
    owner_uid = install["owner_uid"]
    if launcher["uid"] != owner_uid or launcher_profile["uid"] != owner_uid:
        raise RuntimeAdmissionV2Error(
            "trusted launcher/profile ownership differs from the declared owner"
        )
    if admitted and owner_uid != 0:
        raise RuntimeAdmissionV2Error("admitted trusted install must remain root-owned")
    if not admitted and owner_uid == os.geteuid():
        hot_root = Path(hot_root_path)
        for label, reference in (
            ("trusted launcher", launcher),
            ("trusted launcher profile", launcher_profile),
        ):
            path = Path(reference["path"])
            if hot_root not in path.parents:
                raise RuntimeAdmissionV2Error(
                    f"local {label} must be a strict descendant of the hot root"
                )
        if launcher["mode"] != "0500" or launcher_profile["mode"] != "0400":
            raise RuntimeAdmissionV2Error(
                "local trusted launcher/profile must be mode 0500/0400"
            )
    return {
        "owner_uid": owner_uid,
        "launcher": launcher,
        "launcher_profile": launcher_profile,
        "system_tools": tools,
        "root_ownership_enforced": admitted,
    }


def build_semantic(spec: dict[str, Any], *, deep_image: bool = False) -> dict[str, Any]:
    admitted = spec["requested_state"] == "admitted"
    registration, root_reference = _load_root(spec, admitted=admitted)
    profile, profile_reference = _load_profile(spec, admitted=admitted)
    image, image_reference = _load_image(spec, deep=deep_image, admitted=admitted)
    install = _load_install(
        spec, admitted=admitted, hot_root_path=registration["path"]
    )
    root_semantic = {
        "root_id": registration["root_id"],
        "tier": registration["tier"],
        "path": registration["path"],
        "filesystem": registration["filesystem"],
    }
    candidate_closure = _runtime_candidate_closure(
        root_registration=root_reference,
        root=root_semantic,
        execution_image=image_reference,
        production_profile=profile,
        production_profile_file=profile_reference,
        trusted_install=install,
        runtime=spec["runtime"],
    )
    gates = _load_gates(
        spec,
        require_all=admitted,
        profile_identity=profile["identity_sha256"],
        image_identity=image["identity_sha256"],
        runtime_candidate_identity=candidate_closure["identity_sha256"],
        hot_root_path=registration["path"],
    )
    return {
        "kind": KIND,
        "schema_version": SCHEMA_VERSION,
        "implementation_version": IMPLEMENTATION_VERSION,
        "status": "admitted" if admitted else "candidate",
        "root_registration": root_reference,
        "root": root_semantic,
        "execution_image": image_reference,
        "production_profile": profile,
        "production_profile_file": profile_reference,
        "trusted_install": install,
        "runtime_candidate_identity_sha256": candidate_closure[
            "identity_sha256"
        ],
        "runtime": spec["runtime"],
        "gates": gates,
        "policy": dict(POLICY),
    }


RECEIPT_CORE_FIELDS = {
    "kind",
    "schema_version",
    "implementation_version",
    "status",
    "root_registration",
    "root",
    "execution_image",
    "production_profile",
    "production_profile_file",
    "trusted_install",
    "runtime_candidate_identity_sha256",
    "runtime",
    "gates",
    "policy",
}


def make_receipt(spec: dict[str, Any], *, deep_image: bool = False) -> dict[str, Any]:
    semantic = build_semantic(spec, deep_image=deep_image)
    identity = sha256_bytes(canonical_bytes(semantic))
    return {
        **semantic,
        "identity_sha256": identity,
        "receipt_id": f"gpurtv2_{identity[:32]}",
    }


def _spec_from_receipt(receipt: dict[str, Any]) -> dict[str, Any]:
    gates = {
        name: None
        if receipt["gates"][name] is None
        else {
            "path": receipt["gates"][name]["path"],
            "sha256": receipt["gates"][name]["sha256"],
        }
        for name in GATES.GATES
    }
    return {
        "kind": SPEC_KIND,
        "schema_version": SCHEMA_VERSION,
        "requested_state": receipt["status"],
        "root_registration": {
            "path": receipt["root_registration"]["path"],
            "sha256": receipt["root_registration"]["sha256"],
            "registration_id": receipt["root_registration"]["registration_id"],
            "root_id": receipt["root"]["root_id"],
        },
        "execution_image": {
            "receipt_path": receipt["execution_image"]["receipt_path"],
            "receipt_sha256": receipt["execution_image"]["receipt_sha256"],
            "identity_sha256": receipt["execution_image"]["identity_sha256"],
        },
        "production_profile": {
            "path": receipt["production_profile_file"]["path"],
            "sha256": receipt["production_profile_file"]["sha256"],
            "identity_sha256": receipt["production_profile"]["identity_sha256"],
        },
        "trusted_install": {
            "owner_uid": receipt["trusted_install"]["owner_uid"],
            "launcher": {
                "path": receipt["trusted_install"]["launcher"]["path"],
                "sha256": receipt["trusted_install"]["launcher"]["sha256"],
            },
            "launcher_profile": {
                "path": receipt["trusted_install"]["launcher_profile"]["path"],
                "sha256": receipt["trusted_install"]["launcher_profile"]["sha256"],
            },
            "system_tools": [
                {"name": row["name"], "path": row["path"], "sha256": row["sha256"]}
                for row in receipt["trusted_install"]["system_tools"]
            ],
        },
        "runtime": receipt["runtime"],
        "gates": gates,
        "policy": dict(POLICY),
    }


def validate_receipt(
    value: Any, *, require_admitted: bool = False, deep_image: bool = False
) -> dict[str, Any]:
    receipt = _exact(
        value,
        "runtime admission receipt",
        RECEIPT_CORE_FIELDS | {"identity_sha256", "receipt_id"},
    )
    if require_admitted and receipt["status"] != "admitted":
        raise RuntimeAdmissionV2Error("runtime admission receipt is not admitted")
    spec = normalize_spec(_spec_from_receipt(receipt))
    expected = make_receipt(spec, deep_image=deep_image)
    if receipt != expected:
        raise RuntimeAdmissionV2Error("runtime admission receipt failed exact replay")
    return expected


def load_receipt(
    path_value: str | Path,
    expected_sha256: str | None = None,
    *,
    require_admitted: bool = False,
    deep_image: bool = False,
) -> dict[str, Any]:
    path = _absolute(path_value, "runtime admission receipt path")
    body, info = _stable_file(
        path,
        "runtime admission receipt",
        maximum=MAX_JSON_BYTES,
        exact_mode=None,
        allowed_uids={0, os.geteuid()},
    )
    if (info.st_uid, stat.S_IMODE(info.st_mode)) not in {
        (os.geteuid(), 0o400),
        (0, 0o444),
    }:
        raise RuntimeAdmissionV2Error(
            "runtime admission receipt must be current-user mode 0400 or root-owned mode 0444"
        )
    if expected_sha256 is not None and sha256_bytes(body) != _digest(expected_sha256, "expected receipt SHA-256"):
        raise RuntimeAdmissionV2Error("runtime admission receipt SHA-256 differs")
    value = _parse(body, "runtime admission receipt")
    if body != canonical_bytes(value):
        raise RuntimeAdmissionV2Error("runtime admission receipt is not canonical JSON")
    return validate_receipt(value, require_admitted=require_admitted, deep_image=deep_image)


def _write_exclusive(path: Path, value: Any) -> None:
    if path.exists() or path.is_symlink():
        raise FileExistsError(f"refusing to replace {path}")
    parent = path.parent
    info = parent.lstat()
    if stat.S_ISLNK(info.st_mode) or not stat.S_ISDIR(info.st_mode) or info.st_uid != os.geteuid() or stat.S_IMODE(info.st_mode) != 0o700:
        raise RuntimeAdmissionV2Error("receipt parent must be current-user-owned mode 0700")
    body = canonical_bytes(value)
    temporary = parent / f".{path.name}.tmp-{os.getpid()}-{secrets.token_hex(16)}"
    descriptor = os.open(
        temporary,
        os.O_WRONLY | os.O_CREAT | os.O_EXCL | getattr(os, "O_NOFOLLOW", 0),
        0o400,
    )
    try:
        offset = 0
        while offset < len(body):
            offset += os.write(descriptor, body[offset:])
        os.fsync(descriptor)
        os.fchmod(descriptor, 0o400)
    finally:
        os.close(descriptor)
    try:
        os.link(temporary, path, follow_symlinks=False)
        temporary.unlink()
        directory = os.open(parent, os.O_RDONLY | os.O_DIRECTORY)
        try:
            os.fsync(directory)
        finally:
            os.close(directory)
    finally:
        if temporary.exists():
            temporary.unlink()


def _load_spec_file(path: Path, expected_sha256: str) -> dict[str, Any]:
    body, _ = _stable_file(
        path,
        "runtime admission spec",
        maximum=MAX_JSON_BYTES,
        exact_mode=0o400,
        allowed_uids={os.geteuid()},
    )
    if sha256_bytes(body) != _digest(expected_sha256, "expected spec SHA-256"):
        raise RuntimeAdmissionV2Error("runtime admission spec SHA-256 differs")
    value = _parse(body, "runtime admission spec")
    if body != canonical_bytes(value):
        raise RuntimeAdmissionV2Error("runtime admission spec is not canonical JSON")
    normalized = normalize_spec(value)
    if normalized != value:
        raise RuntimeAdmissionV2Error("runtime admission spec is not normalized")
    return normalized


def contract_document() -> dict[str, Any]:
    return {
        "kind": "himr_gpu_runtime_admission_v2_contract",
        "schema_version": SCHEMA_VERSION,
        "implementation_version": IMPLEMENTATION_VERSION,
        "required_gates": list(GATES.GATES),
        "required_mapping_names": sorted(REQUIRED_MAPPING_NAMES),
        "required_mapping_layout": {
            name: {"sandbox_path": path, "role": role}
            for name, (path, role) in sorted(REQUIRED_MAPPING_LAYOUT.items())
        },
        "required_packages": sorted(REQUIRED_PACKAGES),
        "required_package_versions": dict(sorted(REQUIRED_PACKAGE_VERSIONS.items())),
        "required_system_tool_paths": dict(sorted(REQUIRED_SYSTEM_TOOL_PATHS.items())),
        "runtime_candidate_closure_kind": CANDIDATE_CLOSURE_KIND,
        "trusted_launcher_path": TRUSTED_LAUNCHER_PATH,
        "trusted_launcher_profile_path": TRUSTED_LAUNCHER_PROFILE_PATH,
        "policy": dict(POLICY),
    }


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    commands = parser.add_subparsers(dest="command", required=True)
    commands.add_parser("contracts")
    create = commands.add_parser("create")
    create.add_argument("--spec", required=True)
    create.add_argument("--expected-spec-sha256", required=True)
    create.add_argument("--output", required=True)
    validate = commands.add_parser("validate")
    validate.add_argument("--receipt", required=True)
    validate.add_argument("--expected-receipt-sha256")
    validate.add_argument("--require-admitted", action="store_true")
    audit = commands.add_parser("deep-audit")
    audit.add_argument("--receipt", required=True)
    audit.add_argument("--expected-receipt-sha256")
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    try:
        if args.command == "contracts":
            response = contract_document()
        elif args.command == "create":
            spec = _load_spec_file(_absolute(args.spec, "spec path"), args.expected_spec_sha256)
            receipt = make_receipt(spec, deep_image=True)
            output = _absolute(args.output, "output path")
            _write_exclusive(output, receipt)
            response = {
                "status": receipt["status"],
                "receipt_id": receipt["receipt_id"],
                "identity_sha256": receipt["identity_sha256"],
                "path": str(output),
                "sha256": sha256_bytes(canonical_bytes(receipt)),
                "deep_image_hash_verified": True,
            }
        elif args.command == "validate":
            receipt = load_receipt(
                args.receipt,
                args.expected_receipt_sha256,
                require_admitted=args.require_admitted,
                deep_image=False,
            )
            response = {
                "status": "validated",
                "admission_state": receipt["status"],
                "receipt_id": receipt["receipt_id"],
                "identity_sha256": receipt["identity_sha256"],
                "deep_image_hash_verified": False,
                "gpu_queried": False,
                "files_written": False,
            }
        else:
            receipt = load_receipt(
                args.receipt,
                args.expected_receipt_sha256,
                deep_image=True,
            )
            response = {
                "status": "validated",
                "admission_state": receipt["status"],
                "receipt_id": receipt["receipt_id"],
                "identity_sha256": receipt["identity_sha256"],
                "deep_image_hash_verified": True,
                "gpu_queried": False,
                "files_written": False,
            }
        sys.stdout.buffer.write(canonical_bytes(response))
        return 0
    except (RuntimeAdmissionV2Error, FileExistsError, OSError, ValueError) as error:
        failure = {
            "status": "failed",
            "command": args.command,
            "error": {"type": type(error).__name__, "message": str(error)},
            "files_written": False,
        }
        sys.stderr.buffer.write(canonical_bytes(failure))
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
