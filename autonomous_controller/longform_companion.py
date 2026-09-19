"""Strict registration and status boundary for the long-form ASR companion.

The controller entrypoint treats the absence of the fixed adjacent registration as
the legacy, single-process deployment.  Presence is therefore an explicit opt-in:
the registration, its source binding, the long-form campaign configuration, and
the fixed companion executable all have to replay exactly before supervision is
enabled.

This module deliberately does not start a process or mutate either campaign.  It
is also usable by the controller's terminal-disposition code, which only needs the
small, owner-private ``status.json`` projection.
"""

from __future__ import annotations

import hashlib
import json
import os
import re
import stat
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any

from .config import ControllerConfig, load_config


REGISTRATION_FILENAME = "longform-asr-companion-registration.json"
REGISTRATION_KIND = "himr_longform_asr_companion_registration"
REGISTRATION_SCHEMA_VERSION = 1
STATUS_KIND = "himr_longform_asr_campaign_status"
STATUS_SCHEMA_VERSION = 1
MAX_DOCUMENT_BYTES = 1024 * 1024
SHA256_RE = re.compile(r"^[0-9a-f]{64}$")
CONTROLLER_ID_RE = re.compile(r"^himrautocfg_[0-9a-f]{32}$")
CAMPAIGN_ID_RE = re.compile(r"^himrlongcfg_[0-9a-f]{32}$")
JOB_ID_RE = re.compile(r"^himrlongjob_[0-9a-f]{32}$")
UTC_SECONDS_RE = re.compile(r"^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}Z$")


class LongformCompanionError(RuntimeError):
    """The opt-in registration or its bounded public status failed closed."""


@dataclass(frozen=True)
class LongformCompanionRegistration:
    document: dict[str, Any]
    path: Path
    physical_sha256: str

    @property
    def campaign_config_path(self) -> Path:
        return Path(self.document["companion"]["config_path"])

    @property
    def campaign_config_sha256(self) -> str:
        return self.document["companion"]["config_sha256"]

    @property
    def entrypoint_path(self) -> Path:
        return Path(self.document["companion"]["entrypoint_path"])

    @property
    def status_path(self) -> Path:
        return Path(self.document["companion"]["status_path"])


def canonical_bytes(value: Any) -> bytes:
    try:
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
    except (TypeError, ValueError, RecursionError) as error:
        raise LongformCompanionError(f"value is not canonical JSON: {error}") from error


def _sha256(body: bytes) -> str:
    return hashlib.sha256(body).hexdigest()


def _exact(value: Any, label: str, fields: set[str]) -> dict[str, Any]:
    if not isinstance(value, dict) or set(value) != fields:
        observed = sorted(value) if isinstance(value, dict) else type(value).__name__
        raise LongformCompanionError(f"{label} has unexpected fields: {observed}")
    return value


def _digest(value: Any, label: str) -> str:
    if not isinstance(value, str) or SHA256_RE.fullmatch(value) is None:
        raise LongformCompanionError(f"{label} must be a lowercase SHA-256")
    return value


def _identifier(value: Any, label: str, pattern: re.Pattern[str]) -> str:
    if not isinstance(value, str) or pattern.fullmatch(value) is None:
        raise LongformCompanionError(f"{label} is invalid")
    return value


def _bounded_integer(value: Any, label: str, minimum: int, maximum: int) -> int:
    if (
        isinstance(value, bool)
        or not isinstance(value, int)
        or not minimum <= value <= maximum
    ):
        raise LongformCompanionError(
            f"{label} must be an integer in [{minimum}, {maximum}]"
        )
    return value


def _absolute_path(value: Any, label: str) -> Path:
    if not isinstance(value, str) or not value or "\x00" in value:
        raise LongformCompanionError(f"{label} must be a normalized absolute path")
    path = Path(value)
    if (
        not path.is_absolute()
        or str(path) != value
        or os.path.normpath(value) != value
        or "//" in value
        or "\\" in value
        or value == "/"
    ):
        raise LongformCompanionError(f"{label} must be a normalized absolute path")
    return path


def _strict_json(body: bytes, label: str) -> Any:
    def unique(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
        result: dict[str, Any] = {}
        for key, value in pairs:
            if key in result:
                raise LongformCompanionError(f"{label} repeats key {key!r}")
            result[key] = value
        return result

    try:
        return json.loads(
            body.decode("utf-8", errors="strict"),
            object_pairs_hook=unique,
            parse_constant=lambda value: (_ for _ in ()).throw(
                LongformCompanionError(
                    f"{label} contains non-finite number {value}"
                )
            ),
        )
    except LongformCompanionError:
        raise
    except (UnicodeDecodeError, ValueError, RecursionError) as error:
        raise LongformCompanionError(f"{label} is not strict JSON: {error}") from error


def _stable_file(
    path: Path,
    label: str,
    *,
    allowed_modes: frozenset[int],
    executable: bool = False,
) -> bytes:
    try:
        inspected = path.lstat()
        descriptor = os.open(
            path,
            os.O_RDONLY | getattr(os, "O_CLOEXEC", 0) | getattr(os, "O_NOFOLLOW", 0),
        )
    except OSError as error:
        raise LongformCompanionError(f"cannot open {label}: {error}") from error
    try:
        opened = os.fstat(descriptor)
        mode = stat.S_IMODE(opened.st_mode)
        if (
            stat.S_ISLNK(inspected.st_mode)
            or not stat.S_ISREG(opened.st_mode)
            or opened.st_nlink != 1
            or opened.st_uid not in {os.geteuid(), 0}
            or opened.st_size < 1
            or opened.st_size > MAX_DOCUMENT_BYTES
            or mode not in allowed_modes
            or (opened.st_dev, opened.st_ino, opened.st_mode, opened.st_size)
            != (inspected.st_dev, inspected.st_ino, inspected.st_mode, inspected.st_size)
        ):
            raise LongformCompanionError(f"{label} has unsafe metadata")
        if executable and not os.access(f"/proc/self/fd/{descriptor}", os.X_OK):
            raise LongformCompanionError(f"{label} is not executable")
        body = bytearray()
        offset = 0
        while offset < opened.st_size:
            block = os.pread(descriptor, min(1024 * 1024, opened.st_size - offset), offset)
            if not block:
                raise LongformCompanionError(f"{label} ended while being read")
            body.extend(block)
            offset += len(block)
        after = os.fstat(descriptor)
        linked = path.lstat()
        fingerprint = lambda value: (
            value.st_dev,
            value.st_ino,
            value.st_mode,
            value.st_nlink,
            value.st_uid,
            value.st_size,
            value.st_mtime_ns,
            value.st_ctime_ns,
        )
        if fingerprint(opened) != fingerprint(after) or fingerprint(after) != fingerprint(linked):
            raise LongformCompanionError(f"{label} changed while being read")
        return bytes(body)
    finally:
        os.close(descriptor)


def registration_path_for(controller_config_path: Path) -> Path:
    """Return the one opt-in registration path for a controller configuration."""

    return controller_config_path.parent / REGISTRATION_FILENAME


def _normalize_registration(value: Any) -> dict[str, Any]:
    item = _exact(
        value,
        "long-form companion registration",
        {
            "kind",
            "schema_version",
            "source_controller",
            "companion",
            "supervision",
            "identity_sha256",
        },
    )
    if item["kind"] != REGISTRATION_KIND or item["schema_version"] != 1:
        raise LongformCompanionError("long-form companion registration header differs")
    source = _exact(
        item["source_controller"],
        "registration source controller",
        {"config_id", "config_path", "config_sha256"},
    )
    companion = _exact(
        item["companion"],
        "registration companion",
        {
            "config_id",
            "config_path",
            "config_sha256",
            "status_path",
            "entrypoint_path",
            "entrypoint_sha256",
        },
    )
    supervision = _exact(
        item["supervision"],
        "registration supervision",
        {"poll_interval_milliseconds", "graceful_stop_timeout_seconds"},
    )
    core = {
        "kind": REGISTRATION_KIND,
        "schema_version": REGISTRATION_SCHEMA_VERSION,
        "source_controller": {
            "config_id": _identifier(
                source["config_id"], "source controller config ID", CONTROLLER_ID_RE
            ),
            "config_path": str(
                _absolute_path(source["config_path"], "source controller config path")
            ),
            "config_sha256": _digest(
                source["config_sha256"], "source controller config SHA-256"
            ),
        },
        "companion": {
            "config_id": _identifier(
                companion["config_id"], "companion config ID", CAMPAIGN_ID_RE
            ),
            "config_path": str(
                _absolute_path(companion["config_path"], "companion config path")
            ),
            "config_sha256": _digest(
                companion["config_sha256"], "companion config SHA-256"
            ),
            "status_path": str(
                _absolute_path(companion["status_path"], "companion status path")
            ),
            "entrypoint_path": str(
                _absolute_path(companion["entrypoint_path"], "companion entrypoint")
            ),
            "entrypoint_sha256": _digest(
                companion["entrypoint_sha256"], "companion entrypoint SHA-256"
            ),
        },
        "supervision": {
            "poll_interval_milliseconds": _bounded_integer(
                supervision["poll_interval_milliseconds"],
                "supervision poll interval",
                50,
                5000,
            ),
            "graceful_stop_timeout_seconds": _bounded_integer(
                supervision["graceful_stop_timeout_seconds"],
                "graceful stop timeout",
                60,
                86400,
            ),
        },
    }
    identity = _sha256(canonical_bytes(core))
    normalized = {**core, "identity_sha256": identity}
    if item["identity_sha256"] != identity:
        raise LongformCompanionError("registration identity is inconsistent")
    if canonical_bytes(item) != canonical_bytes(normalized):
        raise LongformCompanionError("registration is not normalized")
    return normalized


def build_registration(
    *,
    controller: ControllerConfig,
    campaign_config: Any,
    entrypoint_path: Path,
    entrypoint_sha256: str,
    poll_interval_milliseconds: int = 250,
    graceful_stop_timeout_seconds: int = 900,
) -> dict[str, Any]:
    """Build a normalized registration from already replayed configurations."""

    source = campaign_config.document["source_controller"]
    if (
        source["config_id"] != controller.config_id
        or source["physical_sha256"] != controller.physical_sha256
        or Path(source["path"]).resolve(strict=True)
        != controller.path.resolve(strict=True)
    ):
        raise LongformCompanionError("companion campaign source binding differs")
    core = {
        "kind": REGISTRATION_KIND,
        "schema_version": REGISTRATION_SCHEMA_VERSION,
        "source_controller": {
            "config_id": controller.config_id,
            "config_path": str(controller.path.resolve(strict=True)),
            "config_sha256": controller.physical_sha256,
        },
        "companion": {
            "config_id": campaign_config.config_id,
            "config_path": str(campaign_config.path.resolve(strict=True)),
            "config_sha256": campaign_config.physical_sha256,
            "status_path": campaign_config.document["deployment"]["status_path"],
            "entrypoint_path": str(entrypoint_path.resolve(strict=True)),
            "entrypoint_sha256": _digest(
                entrypoint_sha256, "companion entrypoint SHA-256"
            ),
        },
        "supervision": {
            "poll_interval_milliseconds": poll_interval_milliseconds,
            "graceful_stop_timeout_seconds": graceful_stop_timeout_seconds,
        },
    }
    return _normalize_registration(
        {**core, "identity_sha256": _sha256(canonical_bytes(core))}
    )


def load_registration_if_present(
    controller_config_path: Path,
    expected_controller_sha256: str,
) -> LongformCompanionRegistration | None:
    """Replay the opt-in registration, or return ``None`` only when absent."""

    registration_path = registration_path_for(controller_config_path)
    try:
        registration_path.lstat()
    except FileNotFoundError:
        return None
    except OSError as error:
        raise LongformCompanionError(f"cannot inspect companion registration: {error}") from error

    controller = load_config(controller_config_path, expected_controller_sha256)
    body = _stable_file(
        registration_path,
        "long-form companion registration",
        allowed_modes=frozenset({0o400, 0o444}),
    )
    document = _normalize_registration(_strict_json(body, "long-form companion registration"))
    if body != canonical_bytes(document):
        raise LongformCompanionError("long-form companion registration bytes are not canonical")
    source = document["source_controller"]
    if (
        source["config_id"] != controller.config_id
        or source["config_sha256"] != controller.physical_sha256
        or Path(source["config_path"]).resolve(strict=True)
        != controller_config_path.resolve(strict=True)
    ):
        raise LongformCompanionError("registration source controller binding differs")

    # Import lazily so the legacy controller path has no pipeline dependency.
    from pipeline.longform_asr_campaign import load_campaign_config

    campaign = load_campaign_config(
        Path(document["companion"]["config_path"]),
        document["companion"]["config_sha256"],
    )
    registered = document["companion"]
    campaign_source = campaign.document["source_controller"]
    if (
        campaign.config_id != registered["config_id"]
        or campaign.document["deployment"]["status_path"] != registered["status_path"]
        or campaign_source["config_id"] != controller.config_id
        or campaign_source["physical_sha256"] != controller.physical_sha256
        or Path(campaign_source["path"]).resolve(strict=True)
        != controller_config_path.resolve(strict=True)
    ):
        raise LongformCompanionError("registered companion campaign binding differs")

    expected_entrypoint = (
        Path(__file__).resolve().parents[1] / "pipeline" / "bin" / "longform-asr-campaign"
    ).resolve(strict=True)
    entrypoint = Path(registered["entrypoint_path"])
    if entrypoint.resolve(strict=True) != expected_entrypoint:
        raise LongformCompanionError("registered companion entrypoint is not the fixed executable")
    entrypoint_body = _stable_file(
        entrypoint,
        "long-form companion entrypoint",
        allowed_modes=frozenset({0o500, 0o544, 0o555, 0o700, 0o744, 0o755}),
        executable=True,
    )
    if _sha256(entrypoint_body) != registered["entrypoint_sha256"]:
        raise LongformCompanionError("long-form companion entrypoint SHA-256 differs")
    return LongformCompanionRegistration(
        document=document,
        path=registration_path,
        physical_sha256=_sha256(body),
    )


def _status_integer(value: Any, label: str) -> int:
    return _bounded_integer(value, label, 0, 2**63 - 1)


def _bounded_text(value: Any, label: str, maximum: int = 2048) -> str:
    if (
        not isinstance(value, str)
        or not value
        or len(value) > maximum
        or "\x00" in value
    ):
        raise LongformCompanionError(f"{label} must be bounded non-empty text")
    return value


def read_companion_status(
    registration: LongformCompanionRegistration,
) -> dict[str, Any]:
    """Read and exactly validate the companion's bounded mutable projection."""

    body = _stable_file(
        registration.status_path,
        "long-form companion status",
        allowed_modes=frozenset({0o600}),
    )
    value = _exact(
        _strict_json(body, "long-form companion status"),
        "long-form companion status",
        {
            "kind",
            "schema_version",
            "source_controller",
            "campaign_config",
            "lifecycle",
            "expected_cold_backlog",
            "discovered",
            "jobs",
            "active_job",
            "updated_at",
            "last_error",
        },
    )
    if value["kind"] != STATUS_KIND or value["schema_version"] != STATUS_SCHEMA_VERSION:
        raise LongformCompanionError("long-form companion status header differs")
    source = _exact(
        value["source_controller"],
        "status source controller",
        {"config_id", "physical_sha256"},
    )
    campaign = _exact(
        value["campaign_config"],
        "status campaign config",
        {"config_id", "physical_sha256"},
    )
    registered_source = registration.document["source_controller"]
    registered_campaign = registration.document["companion"]
    if source != {
        "config_id": registered_source["config_id"],
        "physical_sha256": registered_source["config_sha256"],
    }:
        raise LongformCompanionError("status source controller binding differs")
    if campaign != {
        "config_id": registered_campaign["config_id"],
        "physical_sha256": registered_campaign["config_sha256"],
    }:
        raise LongformCompanionError("status campaign config binding differs")

    lifecycle = value["lifecycle"]
    if lifecycle not in {"ready", "running", "waiting", "stopped", "faulted"}:
        raise LongformCompanionError("long-form companion lifecycle is invalid")
    expected = _status_integer(value["expected_cold_backlog"], "expected cold backlog")
    discovered = _exact(
        value["discovered"],
        "status discovered counts",
        {"cold_candidates", "queue_candidates", "total_candidates"},
    )
    discovered_normalized = {
        key: _status_integer(discovered[key], f"status discovered {key}")
        for key in ("cold_candidates", "queue_candidates", "total_candidates")
    }
    if (
        discovered_normalized["cold_candidates"]
        + discovered_normalized["queue_candidates"]
        != discovered_normalized["total_candidates"]
        or discovered_normalized["cold_candidates"] > expected
    ):
        raise LongformCompanionError("status discovered counts are inconsistent")
    jobs = _exact(
        value["jobs"],
        "status job counts",
        {"unprepared", "preprocessed", "prepared", "incomplete", "completed"},
    )
    jobs_normalized = {
        key: _status_integer(jobs[key], f"status jobs {key}")
        for key in ("unprepared", "preprocessed", "prepared", "incomplete", "completed")
    }
    if sum(jobs_normalized.values()) != discovered_normalized["total_candidates"]:
        raise LongformCompanionError("status job counts do not cover discovered candidates")

    active_job = value["active_job"]
    if active_job is not None:
        _identifier(active_job, "active long-form job ID", JOB_ID_RE)
    updated_at = value["updated_at"]
    if not isinstance(updated_at, str) or UTC_SECONDS_RE.fullmatch(updated_at) is None:
        raise LongformCompanionError("status update time is not UTC second precision")
    try:
        datetime.strptime(updated_at, "%Y-%m-%dT%H:%M:%SZ")
    except ValueError as error:
        raise LongformCompanionError("status update time is invalid") from error
    last_error = value["last_error"]
    if last_error is not None:
        error = _exact(last_error, "status last error", {"type", "message"})
        _bounded_text(error["type"], "status error type", 256)
        _bounded_text(error["message"], "status error message")
    if lifecycle == "faulted" and last_error is None:
        raise LongformCompanionError("faulted status lacks its last error")
    if active_job is not None and lifecycle != "running":
        raise LongformCompanionError("only running status may name an active job")

    normalized = {
        **value,
        "expected_cold_backlog": expected,
        "discovered": discovered_normalized,
        "jobs": jobs_normalized,
    }
    if body != canonical_bytes(normalized):
        raise LongformCompanionError("long-form companion status bytes are not canonical")
    return normalized

