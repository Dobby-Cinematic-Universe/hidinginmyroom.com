"""Credential-free, metadata-only producer for torrent availability probes.

The selective torrent planner emits an ordered request.  This module is the only
networked half of that workflow: it invokes one executable-file-hash- and
version-checked yt-dlp process for each target, using exactly the planner's
no-download flags.  Raw
stdout/stderr are held in unlinked temporary files and are reduced to the small
closed outcome vocabulary before an owner-only checkpoint is written.
"""

from __future__ import annotations

import fcntl
import hashlib
import json
import math
import os
import signal
import stat
import subprocess
import tempfile
import time
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Iterator

from .importers import canonical_json, sha256_bytes
from .torrent_bracket_reconciler import _stable_file, _strict_json_bytes, _timestamp
from .torrent_selective_planner import (
    MAX_PROBE_BYTES,
    MAX_PROBE_TARGETS,
    PLAN_KIND,
    PLANNER_VERSION,
    PROBE_CODES_BY_STATE,
    PROBE_FLAGS,
    PROBE_KIND,
    PROBE_REQUEST_KIND,
    SHA256_RE,
    YOUTUBE_ID_RE,
    _exact_keys,
    _validate_availability_probe,
)


CHECKPOINT_KIND = "youtube_no_download_availability_checkpoint_v1"
MAX_INPUT_BYTES = 64 * 1024 * 1024
MAX_CHECKPOINT_BYTES = 16 * 1024 * 1024
MAX_EXECUTABLE_BYTES = 256 * 1024 * 1024
MAX_STDOUT_BYTES = 16 * 1024 * 1024
MAX_STDERR_BYTES = 2 * 1024 * 1024
MONITOR_INTERVAL_SECONDS = 0.05
DEFAULT_TIMEOUT_SECONDS = 180.0
DEFAULT_INTER_TARGET_DELAY_SECONDS = 0.5

NETWORK_POLICY = {
    "cookies_sent": False,
    "authorization_sent": False,
    "media_payload_downloaded": False,
    "playlist_expansion": False,
}

PLAN_KEYS = {
    "schema_version",
    "plan_kind",
    "planner_version",
    "inputs",
    "catalog_binding",
    "archive_binding",
    "evidence_binding_sha256",
    "coverage",
    "availability_probe_request",
    "availability_probe",
    "probe_candidates",
    "malformed_manual_review",
    "selected_files",
    "selected_torrent_file_indices",
    "statistics",
    "policy",
    "plan_id",
    "plan_sha256",
}


class TorrentAvailabilityProbeError(RuntimeError):
    """The producer could not preserve its fail-closed contract."""


def _utc_now() -> str:
    return datetime.now(UTC).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _request_core(request: dict[str, Any]) -> dict[str, Any]:
    return {key: value for key, value in request.items() if key != "request_sha256"}


def validate_availability_probe_request(value: object) -> dict[str, Any]:
    """Validate and normalize one exact planner-emitted request."""

    try:
        request = _exact_keys(
            value,
            {
                "schema_version",
                "request_kind",
                "evidence_binding_sha256",
                "producer_contract",
                "network_policy",
                "targets",
                "request_sha256",
            },
            "availability probe request",
        )
    except Exception as error:
        raise TorrentAvailabilityProbeError(str(error)) from error
    if (
        request["schema_version"] != 1
        or request["request_kind"] != PROBE_REQUEST_KIND
        or not isinstance(request["evidence_binding_sha256"], str)
        or SHA256_RE.fullmatch(request["evidence_binding_sha256"]) is None
        or not isinstance(request["request_sha256"], str)
        or SHA256_RE.fullmatch(request["request_sha256"]) is None
    ):
        raise TorrentAvailabilityProbeError("availability probe request identity is invalid")
    try:
        producer = _exact_keys(
            request["producer_contract"],
            {"tool", "invocation_flags", "one_target_per_process"},
            "availability probe producer contract",
        )
    except Exception as error:
        raise TorrentAvailabilityProbeError(str(error)) from error
    if producer != {
        "tool": "yt-dlp",
        "invocation_flags": list(PROBE_FLAGS),
        "one_target_per_process": True,
    }:
        raise TorrentAvailabilityProbeError(
            "availability probe request changed the fixed yt-dlp invocation"
        )
    try:
        network = _exact_keys(
            request["network_policy"], set(NETWORK_POLICY), "availability probe network policy"
        )
    except Exception as error:
        raise TorrentAvailabilityProbeError(str(error)) from error
    if network != NETWORK_POLICY:
        raise TorrentAvailabilityProbeError(
            "availability probe request permits credentials, media, or playlists"
        )
    targets = request["targets"]
    if not isinstance(targets, list) or len(targets) > MAX_PROBE_TARGETS:
        raise TorrentAvailabilityProbeError("availability probe targets exceed the bounded cap")
    seen: set[str] = set()
    normalized_targets: list[dict[str, str]] = []
    for index, raw in enumerate(targets):
        try:
            target = _exact_keys(
                raw, {"youtube_video_id", "canonical_url"}, f"availability target {index}"
            )
        except Exception as error:
            raise TorrentAvailabilityProbeError(str(error)) from error
        video_id = target["youtube_video_id"]
        canonical_url = target["canonical_url"]
        expected_url = f"https://www.youtube.com/watch?v={video_id}"
        if (
            not isinstance(video_id, str)
            or YOUTUBE_ID_RE.fullmatch(video_id) is None
            or video_id in seen
            or canonical_url != expected_url
        ):
            raise TorrentAvailabilityProbeError(
                "availability probe targets contain a duplicate or noncanonical identity"
            )
        seen.add(video_id)
        normalized_targets.append(
            {"youtube_video_id": video_id, "canonical_url": canonical_url}
        )
    normalized = {
        "schema_version": 1,
        "request_kind": PROBE_REQUEST_KIND,
        "evidence_binding_sha256": request["evidence_binding_sha256"],
        "producer_contract": {
            "tool": "yt-dlp",
            "invocation_flags": list(PROBE_FLAGS),
            "one_target_per_process": True,
        },
        "network_policy": dict(NETWORK_POLICY),
        "targets": normalized_targets,
        "request_sha256": request["request_sha256"],
    }
    observed_digest = sha256_bytes(canonical_json(_request_core(normalized)).encode("utf-8"))
    if observed_digest != normalized["request_sha256"]:
        raise TorrentAvailabilityProbeError(
            "availability probe request SHA-256 does not match canonical content"
        )
    return normalized


def _request_from_document(value: dict[str, Any]) -> dict[str, Any]:
    if value.get("request_kind") == PROBE_REQUEST_KIND:
        return validate_availability_probe_request(value)
    if value.get("plan_kind") != PLAN_KIND:
        raise TorrentAvailabilityProbeError(
            "input must be a full selective plan or its availability_probe_request"
        )
    try:
        plan = _exact_keys(value, PLAN_KEYS, "selective acquisition plan")
    except Exception as error:
        raise TorrentAvailabilityProbeError(str(error)) from error
    if (
        plan["schema_version"] != 1
        or plan["planner_version"] != PLANNER_VERSION
        or not isinstance(plan["plan_sha256"], str)
        or SHA256_RE.fullmatch(plan["plan_sha256"]) is None
        or plan["plan_id"] != f"tslp_{plan['plan_sha256'][:32]}"
    ):
        raise TorrentAvailabilityProbeError("selective acquisition plan identity is invalid")
    core = {key: item for key, item in plan.items() if key not in {"plan_id", "plan_sha256"}}
    if sha256_bytes(canonical_json(core).encode("utf-8")) != plan["plan_sha256"]:
        raise TorrentAvailabilityProbeError(
            "selective acquisition plan SHA-256 does not match canonical content"
        )
    request = validate_availability_probe_request(plan["availability_probe_request"])
    if request["evidence_binding_sha256"] != plan["evidence_binding_sha256"]:
        raise TorrentAvailabilityProbeError(
            "selective plan and availability request evidence bindings differ"
        )
    return request


def load_availability_probe_request(path: Path) -> dict[str, Any]:
    """Load a stable full-plan or standalone-request JSON file."""

    try:
        _resolved, body = _stable_file(
            Path(path), MAX_INPUT_BYTES, "availability probe input"
        )
        value = _strict_json_bytes(body, "availability probe input")
    except Exception as error:
        raise TorrentAvailabilityProbeError(str(error)) from error
    return _request_from_document(value)


def _fingerprint(value: os.stat_result) -> tuple[int, int, int, int, int, int]:
    return (
        value.st_dev,
        value.st_ino,
        value.st_size,
        value.st_mtime_ns,
        value.st_ctime_ns,
        value.st_mode,
    )


def _stable_hash(path: Path, *, maximum: int, label: str) -> tuple[str, os.stat_result]:
    try:
        before = path.lstat()
    except OSError as error:
        raise TorrentAvailabilityProbeError(f"{label} cannot be inspected") from error
    if stat.S_ISLNK(before.st_mode) or not stat.S_ISREG(before.st_mode):
        raise TorrentAvailabilityProbeError(f"{label} must be a regular file, not a symlink")
    if before.st_size < 1 or before.st_size > maximum:
        raise TorrentAvailabilityProbeError(f"{label} exceeds its byte limit")
    flags = os.O_RDONLY | getattr(os, "O_CLOEXEC", 0) | getattr(os, "O_NOFOLLOW", 0)
    try:
        descriptor = os.open(path, flags)
    except OSError as error:
        raise TorrentAvailabilityProbeError(f"{label} cannot be opened safely") from error
    try:
        opened = os.fstat(descriptor)
        if _fingerprint(opened) != _fingerprint(before):
            raise TorrentAvailabilityProbeError(f"{label} changed while opening")
        digest = hashlib.sha256()
        offset = 0
        while offset < opened.st_size:
            chunk = os.pread(descriptor, min(1024 * 1024, opened.st_size - offset), offset)
            if not chunk:
                raise TorrentAvailabilityProbeError(f"{label} ended during hashing")
            digest.update(chunk)
            offset += len(chunk)
        after = os.fstat(descriptor)
    finally:
        os.close(descriptor)
    try:
        path_after = path.lstat()
    except OSError as error:
        raise TorrentAvailabilityProbeError(f"{label} changed during hashing") from error
    if (
        _fingerprint(opened) != _fingerprint(after)
        or _fingerprint(after) != _fingerprint(path_after)
    ):
        raise TorrentAvailabilityProbeError(f"{label} changed during hashing")
    return digest.hexdigest(), opened


@dataclass(frozen=True)
class _PinnedExecutable:
    path: Path
    sha256: str
    fingerprint: tuple[int, int, int, int, int, int]

    def verify_identity(self, *, rehash: bool = False) -> None:
        try:
            current = self.path.lstat()
        except OSError as error:
            raise TorrentAvailabilityProbeError("yt-dlp executable disappeared") from error
        if _fingerprint(current) != self.fingerprint:
            raise TorrentAvailabilityProbeError("yt-dlp executable changed during the probe")
        if rehash:
            digest, observed = _stable_hash(
                self.path, maximum=MAX_EXECUTABLE_BYTES, label="yt-dlp executable"
            )
            if digest != self.sha256 or _fingerprint(observed) != self.fingerprint:
                raise TorrentAvailabilityProbeError("yt-dlp executable SHA-256 changed")


def _pin_executable(path: Path, expected_sha256: str) -> _PinnedExecutable:
    requested = Path(path)
    if not requested.is_absolute():
        raise TorrentAvailabilityProbeError("yt-dlp executable path must be absolute")
    if not isinstance(expected_sha256, str) or SHA256_RE.fullmatch(expected_sha256) is None:
        raise TorrentAvailabilityProbeError("yt-dlp SHA-256 pin must be lowercase hexadecimal")
    try:
        if requested.is_symlink():
            raise TorrentAvailabilityProbeError("yt-dlp executable must not be a symlink")
        resolved = requested.resolve(strict=True)
    except OSError as error:
        raise TorrentAvailabilityProbeError("yt-dlp executable cannot be resolved") from error
    digest, metadata = _stable_hash(
        resolved, maximum=MAX_EXECUTABLE_BYTES, label="yt-dlp executable"
    )
    if not os.access(resolved, os.X_OK):
        raise TorrentAvailabilityProbeError("yt-dlp executable is not executable")
    if digest != expected_sha256:
        raise TorrentAvailabilityProbeError("yt-dlp executable does not match its SHA-256 pin")
    return _PinnedExecutable(resolved, digest, _fingerprint(metadata))


def _minimal_environment(home: Path) -> dict[str, str]:
    # No ambient proxy, netrc, Python import path, cookie, authorization, or yt-dlp
    # variables cross this boundary.  The exact executable and an empty HOME are
    # sufficient for public YouTube metadata extraction.
    return {
        "PATH": os.environ.get("PATH", "/usr/bin:/bin"),
        "HOME": str(home),
        "XDG_CONFIG_HOME": str(home / ".config"),
        "XDG_CACHE_HOME": str(home / ".cache"),
        "LC_ALL": "C",
        "LANG": "C",
        "TZ": "UTC",
    }


def _terminate_process_group(process: subprocess.Popen[bytes]) -> None:
    if process.poll() is not None:
        return
    try:
        os.killpg(process.pid, signal.SIGTERM)
    except ProcessLookupError:
        return
    try:
        process.wait(timeout=2)
        return
    except subprocess.TimeoutExpired:
        pass
    try:
        os.killpg(process.pid, signal.SIGKILL)
    except ProcessLookupError:
        return
    try:
        process.wait(timeout=2)
    except subprocess.TimeoutExpired as error:
        raise TorrentAvailabilityProbeError("could not terminate yt-dlp process group") from error


@dataclass(frozen=True)
class _CommandObservation:
    returncode: int
    stdout: bytes
    stderr: bytes
    termination: str | None


def _run_bounded_command(
    command: list[str], *, environment: dict[str, str], cwd: Path, timeout_seconds: float
) -> _CommandObservation:
    deadline = time.monotonic() + timeout_seconds
    termination: str | None = None
    with tempfile.TemporaryFile() as stdout, tempfile.TemporaryFile() as stderr:
        try:
            process = subprocess.Popen(
                command,
                stdin=subprocess.DEVNULL,
                stdout=stdout,
                stderr=stderr,
                cwd=cwd,
                env=environment,
                start_new_session=True,
            )
        except OSError as error:
            raise TorrentAvailabilityProbeError("yt-dlp could not be started") from error
        try:
            while process.poll() is None:
                if stdout.tell() > MAX_STDOUT_BYTES or stderr.tell() > MAX_STDERR_BYTES:
                    termination = "output_limit"
                    _terminate_process_group(process)
                    break
                if time.monotonic() >= deadline:
                    termination = "timeout"
                    _terminate_process_group(process)
                    break
                time.sleep(MONITOR_INTERVAL_SECONDS)
        except BaseException:
            _terminate_process_group(process)
            raise
        returncode = process.wait()
        stdout_size = os.fstat(stdout.fileno()).st_size
        stderr_size = os.fstat(stderr.fileno()).st_size
        if stdout_size > MAX_STDOUT_BYTES or stderr_size > MAX_STDERR_BYTES:
            termination = "output_limit"
        stdout.seek(0)
        stderr.seek(0)
        stdout_body = stdout.read(MAX_STDOUT_BYTES + 1)
        stderr_body = stderr.read(MAX_STDERR_BYTES + 1)
    return _CommandObservation(returncode, stdout_body, stderr_body, termination)


def _validate_version(value: object, label: str) -> str:
    if (
        not isinstance(value, str)
        or not value
        or len(value) > 128
        or value.strip() != value
        or any(ord(character) < 0x20 or ord(character) == 0x7F for character in value)
    ):
        raise TorrentAvailabilityProbeError(f"{label} is not bounded single-line text")
    return value


def _observe_version(
    executable: _PinnedExecutable, *, expected_version: str, timeout_seconds: float
) -> str:
    expected = _validate_version(expected_version, "expected yt-dlp version")
    executable.verify_identity(rehash=True)
    with tempfile.TemporaryDirectory(prefix="himr-ytdlp-probe-version-") as temporary:
        home = Path(temporary)
        observed = _run_bounded_command(
            [str(executable.path), "--ignore-config", "--version"],
            environment=_minimal_environment(home),
            cwd=home,
            timeout_seconds=min(timeout_seconds, 30.0),
        )
    executable.verify_identity(rehash=True)
    if observed.termination is not None or observed.returncode != 0:
        raise TorrentAvailabilityProbeError("yt-dlp version could not be verified")
    try:
        lines = observed.stdout.decode("utf-8").splitlines()
    except UnicodeDecodeError as error:
        raise TorrentAvailabilityProbeError("yt-dlp version was not UTF-8") from error
    version = _validate_version(
        lines[0].strip() if len(lines) == 1 else "", "observed yt-dlp version"
    )
    if version != expected:
        raise TorrentAvailabilityProbeError(
            f"yt-dlp version does not match the expected pin ({version!r})"
        )
    return version


def _strict_metadata(body: bytes) -> dict[str, Any] | None:
    if not body or len(body) > MAX_STDOUT_BYTES:
        return None

    def pairs(values: list[tuple[str, Any]]) -> dict[str, Any]:
        result: dict[str, Any] = {}
        for key, value in values:
            if key in result:
                raise ValueError("duplicate metadata key")
            result[key] = value
        return result

    def integer(value: str) -> int:
        if len(value.lstrip("-")) > 19:
            raise ValueError("oversized metadata integer")
        return int(value)

    def finite_float(value: str) -> float:
        if len(value) > 64:
            raise ValueError("oversized metadata number")
        parsed = float(value)
        if not math.isfinite(parsed) or abs(parsed) > 2**63 - 1:
            raise ValueError("invalid metadata number")
        return parsed

    def invalid_constant(_value: str) -> None:
        raise ValueError("invalid metadata constant")

    try:
        value = json.loads(
            body.decode("utf-8"),
            object_pairs_hook=pairs,
            parse_int=integer,
            parse_float=finite_float,
            parse_constant=invalid_constant,
        )
    except (UnicodeDecodeError, json.JSONDecodeError, ValueError, RecursionError):
        return None
    return value if isinstance(value, dict) else None


def _classify_failure(stderr: bytes, stdout: bytes) -> tuple[str, str]:
    diagnostic = (stderr + b"\n" + stdout).decode("utf-8", errors="replace").lower()
    if any(
        marker in diagnostic
        for marker in (
            "http error 429",
            "too many requests",
            "rate limit",
            "rate-limit",
            "ratelimit",
        )
    ):
        return "indeterminate", "rate_limited"
    if any(
        marker in diagnostic
        for marker in ("private video", "video is private", "this is a private video")
    ):
        return "unavailable", "private"
    if any(
        marker in diagnostic
        for marker in (
            "has been removed",
            "video was removed",
            "removed by the uploader",
            "associated youtube account has been terminated",
            "account associated with this video has been terminated",
        )
    ):
        return "unavailable", "removed"
    if any(
        marker in diagnostic
        for marker in (
            "sign in",
            "login required",
            "authentication required",
            "confirm your age",
            "age-restricted",
            "members-only",
            "members only",
            "channel members",
        )
    ):
        return "indeterminate", "sign_in_required"
    if any(
        marker in diagnostic
        for marker in (
            "video unavailable",
            "this video is unavailable",
            "this video is not available",
        )
    ):
        return "unavailable", "video_unavailable"
    if any(
        marker in diagnostic
        for marker in (
            "unable to download webpage",
            "unable to download api page",
            "temporary failure in name resolution",
            "name or service not known",
            "connection refused",
            "connection reset",
            "connection aborted",
            "network is unreachable",
            "timed out",
            "timeout",
            "remote end closed connection",
            "http error 502",
            "http error 503",
            "http error 504",
            "sslerror",
            "certificate verify failed",
        )
    ):
        return "indeterminate", "network_error"
    return "indeterminate", "extractor_error"


def _run_target(
    executable: _PinnedExecutable,
    target: dict[str, str],
    *,
    timeout_seconds: float,
) -> dict[str, str]:
    executable.verify_identity()
    with tempfile.TemporaryDirectory(prefix="himr-ytdlp-probe-target-") as temporary:
        home = Path(temporary)
        observation = _run_bounded_command(
            [str(executable.path), *PROBE_FLAGS, target["canonical_url"]],
            environment=_minimal_environment(home),
            cwd=home,
            timeout_seconds=timeout_seconds,
        )
    executable.verify_identity()
    if observation.termination == "timeout":
        state, code = "indeterminate", "network_error"
    elif observation.termination == "output_limit":
        state, code = "indeterminate", "extractor_error"
    elif observation.returncode == 0:
        metadata = _strict_metadata(observation.stdout)
        extractor = metadata.get("extractor") if metadata else None
        extractor_key = metadata.get("extractor_key") if metadata else None
        if (
            metadata is not None
            and metadata.get("id") == target["youtube_video_id"]
            and (extractor == "youtube" or extractor_key == "Youtube")
        ):
            state, code = "available", "metadata_resolved"
        else:
            state, code = "indeterminate", "extractor_error"
    else:
        state, code = _classify_failure(observation.stderr, observation.stdout)
    return {
        "youtube_video_id": target["youtube_video_id"],
        "canonical_url": target["canonical_url"],
        "availability_state": state,
        "evidence_code": code,
    }


def _validated_outcome(raw: object, target: dict[str, str], index: int) -> dict[str, str]:
    try:
        row = _exact_keys(
            raw,
            {"youtube_video_id", "canonical_url", "availability_state", "evidence_code"},
            f"checkpoint outcome {index}",
        )
    except Exception as error:
        raise TorrentAvailabilityProbeError(str(error)) from error
    state = row["availability_state"]
    code = row["evidence_code"]
    if (
        row["youtube_video_id"] != target["youtube_video_id"]
        or row["canonical_url"] != target["canonical_url"]
        or state not in PROBE_CODES_BY_STATE
        or code not in PROBE_CODES_BY_STATE[state]
    ):
        raise TorrentAvailabilityProbeError(
            "checkpoint outcomes are reordered or inconsistent with the request"
        )
    return {
        "youtube_video_id": row["youtube_video_id"],
        "canonical_url": row["canonical_url"],
        "availability_state": state,
        "evidence_code": code,
    }


def _checkpoint_document(
    *,
    request: dict[str, Any],
    producer: dict[str, Any],
    outcomes: list[dict[str, str]],
    started_at: str,
    updated_at: str,
) -> dict[str, Any]:
    core = {
        "schema_version": 1,
        "checkpoint_kind": CHECKPOINT_KIND,
        "request_sha256": request["request_sha256"],
        "producer": producer,
        "network_policy": dict(NETWORK_POLICY),
        "target_count": len(request["targets"]),
        "next_target_index": len(outcomes),
        "started_at": started_at,
        "updated_at": updated_at,
        "outcomes": outcomes,
    }
    return {
        **core,
        "checkpoint_sha256": sha256_bytes(canonical_json(core).encode("utf-8")),
    }


def _validate_checkpoint(
    value: object, *, request: dict[str, Any], producer: dict[str, Any]
) -> tuple[list[dict[str, str]], str]:
    try:
        checkpoint = _exact_keys(
            value,
            {
                "schema_version",
                "checkpoint_kind",
                "request_sha256",
                "producer",
                "network_policy",
                "target_count",
                "next_target_index",
                "started_at",
                "updated_at",
                "outcomes",
                "checkpoint_sha256",
            },
            "availability probe checkpoint",
        )
    except Exception as error:
        raise TorrentAvailabilityProbeError(str(error)) from error
    checkpoint_digest = checkpoint.get("checkpoint_sha256")
    checkpoint_core = {
        key: item for key, item in checkpoint.items() if key != "checkpoint_sha256"
    }
    if (
        not isinstance(checkpoint_digest, str)
        or SHA256_RE.fullmatch(checkpoint_digest) is None
        or sha256_bytes(canonical_json(checkpoint_core).encode("utf-8"))
        != checkpoint_digest
    ):
        raise TorrentAvailabilityProbeError(
            "checkpoint SHA-256 does not match canonical progress content"
        )
    if (
        checkpoint["schema_version"] != 1
        or checkpoint["checkpoint_kind"] != CHECKPOINT_KIND
        or checkpoint["request_sha256"] != request["request_sha256"]
        or checkpoint["producer"] != producer
        or checkpoint["network_policy"] != NETWORK_POLICY
        or checkpoint["target_count"] != len(request["targets"])
        or isinstance(checkpoint["next_target_index"], bool)
        or not isinstance(checkpoint["next_target_index"], int)
    ):
        raise TorrentAvailabilityProbeError(
            "checkpoint is not bound to this request, producer, and network policy"
        )
    try:
        started_at = _timestamp(checkpoint["started_at"], "checkpoint started_at")
        updated_at = _timestamp(checkpoint["updated_at"], "checkpoint updated_at")
    except Exception as error:
        raise TorrentAvailabilityProbeError(str(error)) from error
    if updated_at < started_at:
        raise TorrentAvailabilityProbeError("checkpoint update precedes its start")
    raw_outcomes = checkpoint["outcomes"]
    if (
        not isinstance(raw_outcomes, list)
        or len(raw_outcomes) != checkpoint["next_target_index"]
        or len(raw_outcomes) > len(request["targets"])
    ):
        raise TorrentAvailabilityProbeError("checkpoint outcome prefix is incomplete")
    outcomes = [
        _validated_outcome(raw, request["targets"][index], index)
        for index, raw in enumerate(raw_outcomes)
    ]
    return outcomes, started_at


def _destination(path: Path, label: str) -> Path:
    requested = Path(path)
    if not requested.is_absolute():
        raise TorrentAvailabilityProbeError(f"{label} path must be absolute")
    parent = requested.parent
    try:
        lexical_parent = Path(os.path.abspath(os.fspath(parent)))
        resolved_parent = parent.resolve(strict=True)
        metadata = parent.lstat()
    except OSError as error:
        raise TorrentAvailabilityProbeError(f"{label} parent must already exist") from error
    if lexical_parent != resolved_parent or not stat.S_ISDIR(metadata.st_mode):
        raise TorrentAvailabilityProbeError(f"{label} parent must not traverse a symlink")
    return resolved_parent / requested.name


def _fsync_directory(path: Path) -> None:
    descriptor = os.open(path, os.O_RDONLY | getattr(os, "O_DIRECTORY", 0))
    try:
        os.fsync(descriptor)
    finally:
        os.close(descriptor)


def _assert_private_regular(
    path: Path, label: str, *, require_nonwritable: bool = False
) -> None:
    try:
        metadata = path.lstat()
    except FileNotFoundError:
        return
    except OSError as error:
        raise TorrentAvailabilityProbeError(f"{label} cannot be inspected") from error
    if (
        stat.S_ISLNK(metadata.st_mode)
        or not stat.S_ISREG(metadata.st_mode)
        or metadata.st_nlink != 1
        or (hasattr(os, "getuid") and metadata.st_uid != os.getuid())
        or (require_nonwritable and metadata.st_mode & 0o222)
    ):
        raise TorrentAvailabilityProbeError(
            f"{label} must be an owner-controlled single-link regular file"
        )


def _atomic_replace_private(path: Path, value: dict[str, Any]) -> None:
    _assert_private_regular(path, "checkpoint")
    body = (json.dumps(value, ensure_ascii=False, sort_keys=True, indent=2) + "\n").encode("utf-8")
    descriptor, temporary_name = tempfile.mkstemp(
        prefix=f".{path.name}.", suffix=".tmp", dir=path.parent
    )
    temporary = Path(temporary_name)
    try:
        os.fchmod(descriptor, 0o600)
        offset = 0
        while offset < len(body):
            offset += os.write(descriptor, body[offset:])
        os.fsync(descriptor)
        os.close(descriptor)
        descriptor = -1
        _assert_private_regular(path, "checkpoint")
        os.replace(temporary, path)
        _fsync_directory(path.parent)
    finally:
        if descriptor >= 0:
            os.close(descriptor)
        temporary.unlink(missing_ok=True)


def _atomic_publish_new(path: Path, body: bytes) -> None:
    descriptor, temporary_name = tempfile.mkstemp(
        prefix=f".{path.name}.", suffix=".tmp", dir=path.parent
    )
    temporary = Path(temporary_name)
    try:
        os.fchmod(descriptor, 0o400)
        offset = 0
        while offset < len(body):
            offset += os.write(descriptor, body[offset:])
        os.fsync(descriptor)
        os.close(descriptor)
        descriptor = -1
        try:
            os.link(temporary, path, follow_symlinks=False)
        except FileExistsError as error:
            raise TorrentAvailabilityProbeError("probe output already exists") from error
        _fsync_directory(path.parent)
    finally:
        if descriptor >= 0:
            os.close(descriptor)
        temporary.unlink(missing_ok=True)


@contextmanager
def _producer_lock(checkpoint_path: Path, request_sha256: str) -> Iterator[None]:
    lock_path = checkpoint_path.with_name(f".{checkpoint_path.name}.lock")
    flags = os.O_RDWR | os.O_CREAT | getattr(os, "O_CLOEXEC", 0) | getattr(os, "O_NOFOLLOW", 0)
    try:
        descriptor = os.open(lock_path, flags, 0o600)
    except OSError as error:
        raise TorrentAvailabilityProbeError("probe lock cannot be opened safely") from error
    handle = os.fdopen(descriptor, "r+", encoding="utf-8")
    try:
        metadata = os.fstat(handle.fileno())
        if not stat.S_ISREG(metadata.st_mode) or metadata.st_nlink != 1:
            raise TorrentAvailabilityProbeError("probe lock must be a single-link regular file")
        os.fchmod(handle.fileno(), 0o600)
        try:
            fcntl.flock(handle.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError as error:
            raise TorrentAvailabilityProbeError(
                "another availability producer holds the lock"
            ) from error
        handle.seek(0)
        handle.truncate()
        handle.write(
            json.dumps(
                {"request_sha256": request_sha256, "pid": os.getpid(), "locked_at": _utc_now()},
                sort_keys=True,
            )
            + "\n"
        )
        handle.flush()
        os.fsync(handle.fileno())
        yield
    finally:
        try:
            fcntl.flock(handle.fileno(), fcntl.LOCK_UN)
        finally:
            handle.close()


def _read_checkpoint(
    path: Path, *, request: dict[str, Any], producer: dict[str, Any]
) -> tuple[list[dict[str, str]], str] | None:
    if not path.exists():
        return None
    _assert_private_regular(path, "checkpoint")
    try:
        _resolved, body = _stable_file(path, MAX_CHECKPOINT_BYTES, "availability checkpoint")
        value = _strict_json_bytes(body, "availability checkpoint")
    except Exception as error:
        raise TorrentAvailabilityProbeError(str(error)) from error
    return _validate_checkpoint(value, request=request, producer=producer)


def _result_document(
    *, request: dict[str, Any], producer: dict[str, Any], outcomes: list[dict[str, str]]
) -> dict[str, Any]:
    return {
        "schema_version": 1,
        "probe_kind": PROBE_KIND,
        "request_sha256": request["request_sha256"],
        "observed_at": _utc_now(),
        "producer": producer,
        "network_policy": dict(NETWORK_POLICY),
        "outcomes": outcomes,
    }


def _summary(binding: dict[str, Any], *, target_count: int, reused: bool) -> dict[str, Any]:
    counts = {state: binding["outcome_counts"].get(state, 0) for state in PROBE_CODES_BY_STATE}
    return {
        "valid": True,
        "complete": True,
        "reused_existing_output": reused,
        "request_sha256": binding["request_sha256"],
        "target_count": target_count,
        "outcome_counts": counts,
        "result_sha256": binding["probe_sha256"],
        "result_byte_count": binding["probe_byte_count"],
        "producer": binding["producer"],
        "executable_file_hash_pinned": True,
        "runtime_module_tree_pinned": False,
        "helper_runtime_executables_pinned": False,
        "network_policy": binding["network_policy"],
        "raw_diagnostics_retained": False,
        "torrent_client_invoked": False,
        "media_payload_downloaded": False,
        "credentials_used": False,
    }


def produce_availability_probe(
    request: dict[str, Any],
    *,
    yt_dlp_executable: Path,
    expected_executable_sha256: str,
    expected_version: str,
    checkpoint_path: Path,
    output_path: Path,
    timeout_seconds: float = DEFAULT_TIMEOUT_SECONDS,
    inter_target_delay_seconds: float = DEFAULT_INTER_TARGET_DELAY_SECONDS,
) -> dict[str, Any]:
    """Resume or complete one exact ordered no-download availability probe."""

    request = validate_availability_probe_request(request)
    if (
        isinstance(timeout_seconds, bool)
        or not isinstance(timeout_seconds, (int, float))
        or timeout_seconds < 1
        or timeout_seconds > 3600
    ):
        raise TorrentAvailabilityProbeError("timeout must be between 1 and 3600 seconds")
    if (
        isinstance(inter_target_delay_seconds, bool)
        or not isinstance(inter_target_delay_seconds, (int, float))
        or inter_target_delay_seconds < 0
        or inter_target_delay_seconds > 60
    ):
        raise TorrentAvailabilityProbeError("inter-target delay must be between 0 and 60 seconds")
    checkpoint = _destination(Path(checkpoint_path), "checkpoint")
    output = _destination(Path(output_path), "output")
    if checkpoint == output:
        raise TorrentAvailabilityProbeError("checkpoint and final output paths must differ")
    executable = _pin_executable(Path(yt_dlp_executable), expected_executable_sha256)
    version = _observe_version(
        executable, expected_version=expected_version, timeout_seconds=float(timeout_seconds)
    )
    producer = {
        "tool": "yt-dlp",
        "version": version,
        "executable_sha256": executable.sha256,
        "invocation_flags": list(PROBE_FLAGS),
    }

    with _producer_lock(checkpoint, request["request_sha256"]):
        if output.exists():
            _assert_private_regular(
                output, "probe output", require_nonwritable=True
            )
            try:
                _by_id, binding = _validate_availability_probe(output, request)
            except Exception as error:
                raise TorrentAvailabilityProbeError(
                    "existing probe output is not the exact valid sealed result"
                ) from error
            if binding["producer"] != producer:
                raise TorrentAvailabilityProbeError(
                    "existing probe output was produced by a different yt-dlp pin"
                )
            return _summary(binding, target_count=len(request["targets"]), reused=True)

        restored = _read_checkpoint(checkpoint, request=request, producer=producer)
        if restored is None:
            outcomes: list[dict[str, str]] = []
            started_at = _utc_now()
            _atomic_replace_private(
                checkpoint,
                _checkpoint_document(
                    request=request,
                    producer=producer,
                    outcomes=outcomes,
                    started_at=started_at,
                    updated_at=started_at,
                ),
            )
        else:
            outcomes, started_at = restored

        targets = request["targets"]
        for index in range(len(outcomes), len(targets)):
            outcome = _run_target(
                executable, targets[index], timeout_seconds=float(timeout_seconds)
            )
            outcome = _validated_outcome(outcome, targets[index], index)
            executable.verify_identity()
            outcomes.append(outcome)
            _atomic_replace_private(
                checkpoint,
                _checkpoint_document(
                    request=request,
                    producer=producer,
                    outcomes=outcomes,
                    started_at=started_at,
                    updated_at=_utc_now(),
                ),
            )
            if index + 1 < len(targets) and inter_target_delay_seconds:
                time.sleep(float(inter_target_delay_seconds))

        executable.verify_identity(rehash=True)
        _observe_version(
            executable, expected_version=expected_version, timeout_seconds=float(timeout_seconds)
        )
        result = _result_document(request=request, producer=producer, outcomes=outcomes)
        body = (json.dumps(result, ensure_ascii=False, sort_keys=True, indent=2) + "\n").encode(
            "utf-8"
        )
        if len(body) > MAX_PROBE_BYTES:
            raise TorrentAvailabilityProbeError("final probe result exceeds its byte limit")
        _atomic_publish_new(output, body)
        try:
            _by_id, binding = _validate_availability_probe(output, request)
        except Exception as error:
            raise TorrentAvailabilityProbeError(
                "newly sealed probe output failed its consumer validation"
            ) from error
        return _summary(binding, target_count=len(targets), reused=False)
