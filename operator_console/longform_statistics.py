"""Read-only, bounded recording statistics from the registered ASR companion.

This is a reporting boundary, not an execution/configuration admission check.
Only four small metadata documents are eligible for reads. No job discovery,
tool/media verification, locks, writes, or campaign runner imports are needed.
"""

from __future__ import annotations

from contextlib import contextmanager
import hashlib
import json
import math
import os
from pathlib import Path
import re
import stat
from typing import Any

from autonomous_controller.config import normalize_config
from autonomous_controller import longform_companion as companion


MAX_METADATA_BYTES = 256 * 1024
MAX_SAFE_INTEGER = 2**53 - 1
BASIS = "cached_companion_status_completed_recording_jobs"
_DIAGNOSTICS = {
    "controller_binding_invalid", "registration_invalid", "campaign_binding_invalid",
    "companion_status_invalid", "metadata_unsafe", "metadata_changed",
    "metadata_unavailable", "statistics_reader_failure", "statistics_unavailable",
}


class _StatisticsError(RuntimeError):
    pass


def _empty(state: str, diagnostic: str | None = None) -> dict[str, Any]:
    return {"schema_version": 1, "state": state, "updated_at": None,
            "lifecycle": None, "counts": None, "completion_percent": None,
            "basis": BASIS, "last_error": None, "diagnostic": diagnostic}


def unavailable_statistics(diagnostic: str) -> dict[str, Any]:
    """Return unknown statistics without exposing exception text or paths."""
    code = diagnostic if isinstance(diagnostic, str) and diagnostic in _DIAGNOSTICS else "statistics_unavailable"
    return _empty("unavailable", code)


def _path(value: Any) -> Path:
    text = str(value) if isinstance(value, Path) else value
    if (not isinstance(text, str) or not text or len(text) > 4096
            or "\x00" in text or "\\" in text or "//" in text):
        raise _StatisticsError("metadata_unsafe")
    path = Path(text)
    if not path.is_absolute() or str(path) != text or os.path.normpath(text) != text or text == "/":
        raise _StatisticsError("metadata_unsafe")
    return path


def _fingerprint(value):
    return (value.st_dev, value.st_ino, value.st_mode, value.st_uid,
            value.st_gid, value.st_nlink, value.st_size, value.st_mtime_ns, value.st_ctime_ns)


@contextmanager
def _open_metadata(path: Path, *, mutable: bool = False):
    # Resolve each component without following symlinks. The owner check also
    # prevents substituting a peer-owned ancestor under a writable parent.
    parent = os.open("/", os.O_RDONLY | os.O_DIRECTORY | os.O_CLOEXEC | os.O_NOFOLLOW)
    descriptor = None
    try:
        for component in path.parts[1:-1]:
            child = os.open(component, os.O_RDONLY | os.O_DIRECTORY | os.O_CLOEXEC | os.O_NOFOLLOW, dir_fd=parent)
            os.close(parent)
            parent = child
            directory = os.fstat(parent)
            if directory.st_uid not in {0, os.geteuid()}:
                raise _StatisticsError("metadata_unsafe")
        if mutable and (os.fstat(parent).st_uid != os.geteuid() or stat.S_IMODE(os.fstat(parent).st_mode) != 0o700):
            raise _StatisticsError("metadata_unsafe")
        descriptor = os.open(path.name, os.O_RDONLY | os.O_CLOEXEC | os.O_NOFOLLOW | os.O_NONBLOCK, dir_fd=parent)
        observed = os.fstat(descriptor)
        if (not stat.S_ISREG(observed.st_mode) or observed.st_uid not in {0, os.geteuid()}
                or observed.st_nlink != 1 or not 0 < observed.st_size <= MAX_METADATA_BYTES
                or stat.S_IMODE(observed.st_mode) not in ({0o600} if mutable else {0o400, 0o444})
                or (mutable and observed.st_uid != os.geteuid())):
            raise _StatisticsError("metadata_unsafe")
        yield descriptor, observed
        if (_fingerprint(observed) != _fingerprint(os.fstat(descriptor))
                or _fingerprint(observed) != _fingerprint(os.stat(path.name, dir_fd=parent, follow_symlinks=False))):
            raise _StatisticsError("metadata_changed")
    finally:
        if descriptor is not None:
            os.close(descriptor)
        os.close(parent)


def _json(body: bytes) -> dict:
    def pairs(items):
        result = {}
        for key, value in items:
            if key in result:
                raise ValueError("duplicate field")
            result[key] = value
        return result

    def number(value):
        result = float(value)
        if not math.isfinite(result):
            raise ValueError("nonfinite number")
        return result

    value = json.loads(body, object_pairs_hook=pairs, parse_float=number,
                       parse_constant=lambda _value: (_ for _ in ()).throw(ValueError("nonfinite number")))
    if not isinstance(value, dict) or companion.canonical_bytes(value) != body:
        raise ValueError("noncanonical document")
    return value


def _read(path: Path, witnesses: list, *, expected_sha256: str | None = None) -> dict:
    with _open_metadata(path) as (descriptor, before):
        parts, remaining = [], before.st_size
        while remaining:
            block = os.read(descriptor, remaining)
            if not block:
                raise _StatisticsError("metadata_changed")
            parts.append(block)
            remaining -= len(block)
        body = b"".join(parts)
    if expected_sha256 is not None and (
            not isinstance(expected_sha256, str) or re.fullmatch(r"[0-9a-f]{64}", expected_sha256) is None
            or hashlib.sha256(body).hexdigest() != expected_sha256):
        raise ValueError("digest binding differs")
    witnesses.append((path, _fingerprint(before), False))
    return _json(body)


def _campaign_identity(value: dict) -> None:
    core = {key: item for key, item in value.items() if key not in {"identity_sha256", "config_id"}}
    identity = hashlib.sha256(companion.canonical_bytes(core)).hexdigest()
    if (value.get("kind") != "himr_longform_asr_campaign_config"
            or type(value.get("schema_version")) is not int or value["schema_version"] != 1
            or value.get("identity_sha256") != identity
            or value.get("config_id") != "himrlongcfg_" + identity[:32]):
        raise ValueError("campaign identity differs")


def read_longform_statistics(controller_config_path: Path, expected_sha256: str) -> dict[str, Any]:
    """Report validated cached recording counts; failures affect this tile only."""
    phase = "controller_binding_invalid"
    try:
        if not isinstance(expected_sha256, str) or re.fullmatch(r"[0-9a-f]{64}", expected_sha256) is None:
            raise ValueError("expected controller digest is invalid")
        witnesses = []
        controller_path = _path(controller_config_path)
        controller = normalize_config(_read(controller_path, witnesses, expected_sha256=expected_sha256))
        phase = "registration_invalid"
        registration_path = companion.registration_path_for(controller_path)
        try:
            registration_path.lstat()
        except FileNotFoundError:
            return _empty("not_registered")
        registered = companion._normalize_registration(_read(registration_path, witnesses))
        source = registered["source_controller"]
        if source != {"config_id": controller["config_id"], "config_path": str(controller_path), "config_sha256": expected_sha256}:
            raise ValueError("registration controller binding differs")

        phase = "campaign_binding_invalid"
        target = registered["companion"]
        campaign_path = _path(target["config_path"])
        campaign = _read(campaign_path, witnesses, expected_sha256=target["config_sha256"])
        _campaign_identity(campaign)
        campaign_source = campaign["source_controller"]
        status_path = _path(target["status_path"])
        deployment = campaign["deployment"]
        if (campaign["config_id"] != target["config_id"]
                or campaign_source["config_id"] != controller["config_id"]
                or campaign_source["identity_sha256"] != controller["identity_sha256"]
                or campaign_source["physical_sha256"] != expected_sha256
                or _path(campaign_source["path"]) != controller_path
                or _path(deployment["status_path"]) != status_path
                or _path(deployment["root"]) / "status.json" != status_path):
            raise ValueError("campaign reporting binding differs")

        phase = "companion_status_invalid"
        registration = companion.LongformCompanionRegistration(registered, registration_path,
            hashlib.sha256(companion.canonical_bytes(registered)).hexdigest())
        # Hold and witness the bounded leaf while the existing strict validator
        # reads it. That validator independently caps reads and checks changes.
        with _open_metadata(status_path, mutable=True) as (_descriptor, before):
            status = companion.read_companion_status(registration)
        witnesses.append((status_path, _fingerprint(before), True))
        if type(status["schema_version"]) is not int:
            raise ValueError("status schema version must be an integer")
        discovered, jobs = status["discovered"], status["jobs"]
        if any(type(value) is not int or not 0 <= value <= MAX_SAFE_INTEGER
               for value in [status["expected_cold_backlog"], *discovered.values(), *jobs.values()]):
            raise ValueError("counter cannot be represented exactly by the UI")
        for path, witness, mutable in witnesses:
            with _open_metadata(path, mutable=mutable) as (_descriptor, info):
                if _fingerprint(info) != witness:
                    raise _StatisticsError("metadata_changed")
        total, completed = discovered["total_candidates"], jobs["completed"]
        counts = {"completed_recordings": completed, "discovered_recordings": total,
                  "remaining_discovered_recordings": total - completed,
                  **{key + "_recordings": jobs[key] for key in ("unprepared", "preprocessed", "prepared", "incomplete")},
                  "cold_candidates": discovered["cold_candidates"], "queue_candidates": discovered["queue_candidates"],
                  "expected_cold_backlog": status["expected_cold_backlog"],
                  "cold_candidates_not_discovered": status["expected_cold_backlog"] - discovered["cold_candidates"],
                  "active_recordings": int(status["active_job"] is not None)}
        return {"schema_version": 1, "state": "available", "updated_at": status["updated_at"],
                "lifecycle": status["lifecycle"], "counts": counts,
                "completion_percent": min(100.0, 100.0 * completed / total) if total else None,
                "basis": BASIS, "last_error": status["last_error"], "diagnostic": None}
    except _StatisticsError as error:
        return unavailable_statistics(str(error))
    except Exception:
        return unavailable_statistics(phase)
