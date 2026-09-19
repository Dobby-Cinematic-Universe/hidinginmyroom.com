#!/usr/bin/env python3
"""Emit a fail-closed work order for one completed selected torrent file.

This is the acquisition side of a later catalogue-admission boundary.  It does not
copy or admit media, mutate a database, contact the network, inspect a torrent
process, or execute a payload.  The selected payload file is the only torrent
payload opened: it is pinned without following links, hashed, and passed to a pinned
ffprobe executable through an inherited read-only descriptor.

The implementation deliberately reuses the corpus torrent parser, selector replay,
and metadata-only acquisition auditor.  Those components bind the finalized plan,
exact torrent bytes, selector receipt, saved aria2 session, control geometry, and
the complete payload-tree shape before this module crosses the payload-read boundary.
"""

from __future__ import annotations

import argparse
import base64
import hashlib
import json
import math
import mimetypes
import os
import re
import shutil
import stat
import subprocess
import sys
import tempfile
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any


REPOSITORY_ROOT = Path(__file__).resolve().parents[1]
CORPUS_SOURCE_ROOT = REPOSITORY_ROOT / "corpus" / "src"
for module_root in (REPOSITORY_ROOT, CORPUS_SOURCE_ROOT):
    module_root_text = os.fspath(module_root)
    if module_root_text not in sys.path:
        sys.path.insert(0, module_root_text)

from acquisition import acquire  # noqa: E402
from himr_corpus.torrent_acquisition_audit import (  # noqa: E402
    MAX_CONTROL_BYTES,
    MAX_PLAN_BYTES,
    MAX_SELECTOR_RECEIPT_BYTES,
    MAX_SESSION_BYTES,
    TorrentAcquisitionAuditError,
    _manifest_rows,
    _merge_piece_ranges,
    _parse_control,
    _range_helpers,
    _stable_unlinked_file,
    build_torrent_acquisition_audit,
)
from himr_corpus.torrent_aria2_selector import (  # noqa: E402
    TorrentAria2SelectorError,
    build_aria2_selector_receipt,
)
from himr_corpus.torrent_bracket_reconciler import (  # noqa: E402
    MAX_TORRENT_BYTES,
    TorrentBracketReconciliationError,
    _parse_torrent,
    _raw_path_sha256,
    _strict_json_bytes,
)


SCHEMA_VERSION = 1
IMPLEMENTATION_VERSION = "0.1.0"
WORK_ORDER_KIND = "torrent_completed_selected_file_handoff"
PRODUCER_NAME = "himr-torrent-completed-handoff"
SHA256_RE = re.compile(r"^[0-9a-f]{64}$")
RFC3339_UTC_RE = re.compile(r"^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}Z$")
MAX_TARGET_BYTES = 64 * 1024**3
MAX_FFPROBE_EXECUTABLE_BYTES = 256 * 1024**2
MAX_FFPROBE_JSON_BYTES = 8 * 1024**2
MAX_FFPROBE_STDERR_BYTES = 256 * 1024
FFPROBE_TIMEOUT_SECONDS = 120
HASH_CHUNK_BYTES = 4 * 1024**2
MEDIA_SUFFIXES = frozenset(
    {
        ".3gp",
        ".aac",
        ".avi",
        ".flac",
        ".flv",
        ".m2ts",
        ".m4a",
        ".m4v",
        ".mkv",
        ".mov",
        ".mp3",
        ".mp4",
        ".mpeg",
        ".mpg",
        ".mts",
        ".ogg",
        ".ogv",
        ".opus",
        ".ts",
        ".wav",
        ".webm",
    }
)
FFPROBE_SHOW_ENTRIES = (
    "format=format_name,format_long_name,duration,bit_rate:"
    "stream=index,codec_type,codec_name,duration,bit_rate,width,height,pix_fmt,"
    "avg_frame_rate,sample_rate,channels,channel_layout:stream_tags=language"
)


class TorrentCompletedHandoffError(ValueError):
    """A binding, completion, filesystem, probe, or output failure."""


def _fail(message: str) -> None:
    raise TorrentCompletedHandoffError(message)


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
        raise TorrentCompletedHandoffError(
            f"work order cannot be canonically encoded: {error}"
        ) from error


def sha256_bytes(value: bytes) -> str:
    return hashlib.sha256(value).hexdigest()


def _timestamp(value: str) -> str:
    if not isinstance(value, str) or not RFC3339_UTC_RE.fullmatch(value):
        _fail("observed-at must be whole-second RFC3339 UTC")
    try:
        parsed = datetime.strptime(value, "%Y-%m-%dT%H:%M:%SZ")
    except ValueError as error:
        raise TorrentCompletedHandoffError(
            "observed-at is not a real UTC timestamp"
        ) from error
    if parsed.tzinfo is not None or parsed.strftime("%Y-%m-%dT%H:%M:%SZ") != value:
        _fail("observed-at is not canonical RFC3339 UTC")
    return value


def _fingerprint(value: os.stat_result) -> tuple[int, int, int, int, int, int, int]:
    return (
        value.st_dev,
        value.st_ino,
        value.st_mode,
        value.st_nlink,
        value.st_size,
        value.st_mtime_ns,
        value.st_ctime_ns,
    )


def _piece_indices_sha256(values: list[int]) -> str:
    digest = hashlib.sha256()
    for value in values:
        digest.update(value.to_bytes(8, "big"))
    return digest.hexdigest()


def _exact_selected_target(
    *,
    plan: dict[str, Any],
    selector: dict[str, Any],
    torrent: dict[str, Any],
    expected_file_index: int,
    expected_manifest_path: str,
    expected_declared_byte_count: int,
) -> tuple[dict[str, Any], dict[str, Any], list[int]]:
    if isinstance(expected_file_index, bool) or not isinstance(expected_file_index, int):
        _fail("expected torrent file index must be an integer")
    if expected_file_index < 0 or expected_file_index >= torrent["file_count"]:
        _fail("expected torrent file index is outside the exact manifest")
    if (
        not isinstance(expected_manifest_path, str)
        or not expected_manifest_path
        or "\x00" in expected_manifest_path
        or len(expected_manifest_path) > 16_384
    ):
        _fail("expected manifest path must be bounded non-empty text")
    if (
        isinstance(expected_declared_byte_count, bool)
        or not isinstance(expected_declared_byte_count, int)
        or expected_declared_byte_count < 1
        or expected_declared_byte_count > MAX_TARGET_BYTES
    ):
        _fail("expected declared byte count is outside the handoff limit")

    selected_indices = selector["selection"]["zero_based_file_indices"]
    if expected_file_index not in selected_indices:
        _fail("requested target is unselected or only a boundary artifact")

    manifest = torrent["files"][expected_file_index]
    if manifest["manifest_path"] != expected_manifest_path:
        _fail("expected manifest path differs from the exact torrent row")
    if manifest["byte_count"] != expected_declared_byte_count:
        _fail("expected declared byte count differs from the exact torrent row")

    selected_rows = plan.get("selected_files")
    if not isinstance(selected_rows, list):
        _fail("finalized plan selected-file rows are unavailable")
    matching = [
        row
        for row in selected_rows
        if isinstance(row, dict)
        and row.get("torrent_file_index") == expected_file_index
    ]
    if len(matching) != 1:
        _fail("requested target lacks one exact selected-file plan row")
    selected_row = matching[0]
    if (
        selected_row.get("manifest_path") != expected_manifest_path
        or selected_row.get("byte_count") != expected_declared_byte_count
        or selected_row.get("manifest_path_sha256")
        != _raw_path_sha256(manifest["raw_components"])
    ):
        _fail("selected-file plan row differs from the exact torrent row")

    suffix = Path(expected_manifest_path).suffix.lower()
    if suffix not in MEDIA_SUFFIXES:
        _fail("requested selected target is not an allowed media suffix")

    piece_length = torrent["piece_length_bytes"]
    start = sum(row["byte_count"] for row in torrent["files"][:expected_file_index])
    end = start + expected_declared_byte_count
    pieces = list(range(start // piece_length, (end - 1) // piece_length + 1))
    if not pieces:
        _fail("requested target has no covering torrent pieces")
    return manifest, selected_row, pieces


def _load_bound_inputs(
    plan_path: Path, selector_path: Path, torrent_path: Path
) -> tuple[
    dict[str, Any],
    bytes,
    dict[str, Any],
    bytes,
    dict[str, Any],
    bytes,
]:
    try:
        _plan_path, plan_body = _stable_unlinked_file(
            plan_path, MAX_PLAN_BYTES, "finalized torrent plan"
        )
        _selector_path, selector_body = _stable_unlinked_file(
            selector_path, MAX_SELECTOR_RECEIPT_BYTES, "aria2 selector receipt"
        )
        _torrent_path, torrent_body = _stable_unlinked_file(
            torrent_path, MAX_TORRENT_BYTES, "torrent manifest"
        )
        plan = _strict_json_bytes(plan_body, "finalized torrent plan")
        stored_selector = _strict_json_bytes(selector_body, "aria2 selector receipt")
        torrent = _parse_torrent(torrent_body)
        replayed_selector = build_aria2_selector_receipt(plan_path, torrent_path)
    except (
        TorrentAcquisitionAuditError,
        TorrentAria2SelectorError,
        TorrentBracketReconciliationError,
    ) as error:
        raise TorrentCompletedHandoffError(str(error)) from error
    if stored_selector != replayed_selector:
        _fail("aria2 selector receipt differs from the exact sealed plan and torrent")
    return plan, plan_body, stored_selector, selector_body, torrent, torrent_body


def _control_snapshot(
    *,
    control_path: Path,
    torrent: dict[str, Any],
    selected_indices: list[int],
    target_pieces: list[int],
) -> tuple[bytes, dict[str, Any]]:
    try:
        _control_path, body = _stable_unlinked_file(
            control_path, MAX_CONTROL_BYTES, "aria2 control file"
        )
        rows, piece_count = _manifest_rows(torrent)
        ranges = _merge_piece_ranges(rows, set(selected_indices))
        starts, ends = _range_helpers(ranges)
        state = _parse_control(body, torrent, piece_count, starts, ends)
    except TorrentAcquisitionAuditError as error:
        raise TorrentCompletedHandoffError(str(error)) from error
    if not state["authorized_only"]:
        _fail("aria2 control contains completed or in-flight state outside the selector")
    completed = set(state["completed_indices"])
    missing = [piece for piece in target_pieces if piece not in completed]
    if missing:
        _fail("not every torrent piece covering the selected target is complete")
    return body, state


@dataclass
class _PinnedSelectedFile:
    payload_root: Path
    raw_components: tuple[bytes, ...]
    expected_size: int
    directory_fds: list[int]
    named_entries: list[tuple[int, bytes, os.stat_result]]
    descriptor: int
    initial_stat: os.stat_result

    @classmethod
    def open(
        cls,
        *,
        payload_root: Path,
        torrent_root_name: str,
        raw_components: tuple[bytes, ...],
        expected_size: int,
    ) -> "_PinnedSelectedFile":
        requested_root = Path(os.path.abspath(os.fspath(payload_root)))
        try:
            resolved_root = requested_root.resolve(strict=True)
            root_lstat = requested_root.lstat()
        except OSError as error:
            raise TorrentCompletedHandoffError(
                "torrent payload root cannot be resolved"
            ) from error
        if requested_root != resolved_root or not stat.S_ISDIR(root_lstat.st_mode):
            _fail("torrent payload root must be a non-symlink directory")
        if not raw_components:
            _fail("selected torrent row has no path components")

        directory_flags = (
            os.O_RDONLY
            | getattr(os, "O_CLOEXEC", 0)
            | getattr(os, "O_DIRECTORY", 0)
            | getattr(os, "O_NOFOLLOW", 0)
        )
        file_flags = (
            os.O_RDONLY
            | getattr(os, "O_CLOEXEC", 0)
            | getattr(os, "O_NOFOLLOW", 0)
            | getattr(os, "O_NONBLOCK", 0)
        )
        directory_fds: list[int] = []
        named_entries: list[tuple[int, bytes, os.stat_result]] = []
        descriptor = -1
        try:
            payload_fd = os.open(requested_root, directory_flags)
            directory_fds.append(payload_fd)
            payload_opened = os.fstat(payload_fd)
            if _fingerprint(payload_opened) != _fingerprint(root_lstat):
                _fail("torrent payload root changed while pinning selected file")
            filesystem_device = payload_opened.st_dev

            components = (torrent_root_name.encode("utf-8"), *raw_components)
            current_fd = payload_fd
            for position, component in enumerate(components[:-1]):
                try:
                    before = os.stat(
                        component, dir_fd=current_fd, follow_symlinks=False
                    )
                except OSError as error:
                    raise TorrentCompletedHandoffError(
                        "selected payload parent cannot be inspected"
                    ) from error
                if (
                    stat.S_ISLNK(before.st_mode)
                    or not stat.S_ISDIR(before.st_mode)
                    or before.st_dev != filesystem_device
                ):
                    _fail("selected payload parent is a symlink, special node, or mount escape")
                try:
                    child_fd = os.open(component, directory_flags, dir_fd=current_fd)
                except OSError as error:
                    raise TorrentCompletedHandoffError(
                        "selected payload parent cannot be pinned"
                    ) from error
                opened = os.fstat(child_fd)
                if _fingerprint(opened) != _fingerprint(before):
                    os.close(child_fd)
                    _fail("selected payload parent changed while opening")
                named_entries.append((current_fd, component, before))
                directory_fds.append(child_fd)
                current_fd = child_fd

            leaf = components[-1]
            try:
                before_leaf = os.stat(leaf, dir_fd=current_fd, follow_symlinks=False)
            except OSError as error:
                raise TorrentCompletedHandoffError(
                    "selected payload file cannot be inspected"
                ) from error
            if stat.S_ISLNK(before_leaf.st_mode) or not stat.S_ISREG(before_leaf.st_mode):
                _fail("selected payload file must be a non-symlink regular file")
            if before_leaf.st_nlink != 1 or before_leaf.st_dev != filesystem_device:
                _fail("selected payload file is hard-linked or crosses a mount boundary")
            if before_leaf.st_size != expected_size:
                _fail("selected payload file size differs from the torrent manifest")
            if before_leaf.st_blocks <= 0:
                _fail("selected payload file has no allocated data blocks")
            try:
                descriptor = os.open(leaf, file_flags, dir_fd=current_fd)
            except OSError as error:
                raise TorrentCompletedHandoffError(
                    "selected payload file cannot be pinned without following links"
                ) from error
            opened_leaf = os.fstat(descriptor)
            if _fingerprint(opened_leaf) != _fingerprint(before_leaf):
                _fail("selected payload file changed while opening")
            named_entries.append((current_fd, leaf, before_leaf))
            return cls(
                payload_root=requested_root,
                raw_components=raw_components,
                expected_size=expected_size,
                directory_fds=directory_fds,
                named_entries=named_entries,
                descriptor=descriptor,
                initial_stat=opened_leaf,
            )
        except Exception:
            if descriptor >= 0:
                os.close(descriptor)
            for directory_fd in reversed(directory_fds):
                os.close(directory_fd)
            raise

    def assert_stable(self) -> None:
        current = os.fstat(self.descriptor)
        if _fingerprint(current) != _fingerprint(self.initial_stat):
            _fail("selected payload file metadata changed while hashing or probing")
        for parent_fd, name, original in self.named_entries:
            try:
                named = os.stat(name, dir_fd=parent_fd, follow_symlinks=False)
            except OSError as error:
                raise TorrentCompletedHandoffError(
                    "selected payload path changed while hashing or probing"
                ) from error
            if stat.S_ISLNK(named.st_mode) or _fingerprint(named) != _fingerprint(original):
                _fail("selected payload path changed while hashing or probing")

    def close(self) -> None:
        if self.descriptor >= 0:
            os.close(self.descriptor)
            self.descriptor = -1
        for descriptor in reversed(self.directory_fds):
            os.close(descriptor)
        self.directory_fds = []

    def __enter__(self) -> "_PinnedSelectedFile":
        return self

    def __exit__(self, exc_type: Any, exc: Any, traceback: Any) -> None:
        self.close()


def _hash_pinned(descriptor: int, expected_size: int) -> str:
    digest = hashlib.sha256()
    offset = 0
    while offset < expected_size:
        try:
            chunk = os.pread(
                descriptor, min(HASH_CHUNK_BYTES, expected_size - offset), offset
            )
        except OSError as error:
            raise TorrentCompletedHandoffError(
                "selected payload failed during pinned hashing"
            ) from error
        if not chunk:
            _fail("selected payload ended during pinned hashing")
        digest.update(chunk)
        offset += len(chunk)
    try:
        if os.pread(descriptor, 1, expected_size):
            _fail("selected payload grew during pinned hashing")
    except OSError as error:
        raise TorrentCompletedHandoffError(
            "selected payload could not finish pinned hashing"
        ) from error
    return digest.hexdigest()


@dataclass
class _PinnedExecutable:
    path: Path
    descriptor: int
    initial_stat: os.stat_result
    sha256: str

    @classmethod
    def open(cls, requested: str | None) -> "_PinnedExecutable":
        candidate = requested or shutil.which("ffprobe")
        if not candidate:
            _fail("ffprobe is required")
        try:
            path = Path(candidate).resolve(strict=True)
            before = path.lstat()
        except OSError as error:
            raise TorrentCompletedHandoffError("ffprobe cannot be resolved") from error
        if not path.is_absolute() or not stat.S_ISREG(before.st_mode):
            _fail("ffprobe must resolve to an absolute regular file")
        if before.st_size < 1 or before.st_size > MAX_FFPROBE_EXECUTABLE_BYTES:
            _fail("ffprobe executable size is outside the handoff limit")
        if not (before.st_mode & 0o111):
            _fail("ffprobe resolved file is not executable")
        flags = (
            os.O_RDONLY
            | getattr(os, "O_CLOEXEC", 0)
            | getattr(os, "O_NOFOLLOW", 0)
            | getattr(os, "O_NONBLOCK", 0)
        )
        try:
            descriptor = os.open(path, flags)
        except OSError as error:
            raise TorrentCompletedHandoffError("ffprobe cannot be pinned") from error
        try:
            opened = os.fstat(descriptor)
            if _fingerprint(opened) != _fingerprint(before):
                _fail("ffprobe changed while opening")
            digest = _hash_pinned(descriptor, opened.st_size)
            return cls(path=path, descriptor=descriptor, initial_stat=opened, sha256=digest)
        except Exception:
            os.close(descriptor)
            raise

    def assert_stable(self) -> None:
        current = os.fstat(self.descriptor)
        try:
            named = self.path.lstat()
        except OSError as error:
            raise TorrentCompletedHandoffError("ffprobe changed during use") from error
        if (
            _fingerprint(current) != _fingerprint(self.initial_stat)
            or _fingerprint(named) != _fingerprint(self.initial_stat)
            or _hash_pinned(self.descriptor, current.st_size) != self.sha256
        ):
            _fail("ffprobe changed during use")

    def close(self) -> None:
        if self.descriptor >= 0:
            os.close(self.descriptor)
            self.descriptor = -1

    def __enter__(self) -> "_PinnedExecutable":
        return self

    def __exit__(self, exc_type: Any, exc: Any, traceback: Any) -> None:
        self.close()


def _run_pinned_process(
    executable_fd: int,
    arguments: list[str],
    *,
    inherited_fds: tuple[int, ...],
    stdout_cap: int,
    stderr_cap: int,
) -> tuple[bytes, bytes]:
    executable = f"/proc/self/fd/{executable_fd}"
    command = [executable, *arguments]
    environment = {"LC_ALL": "C", "LANG": "C", "TZ": "UTC"}
    with tempfile.TemporaryFile() as stdout_file, tempfile.TemporaryFile() as stderr_file:
        try:
            process = subprocess.Popen(
                command,
                stdin=subprocess.DEVNULL,
                stdout=stdout_file,
                stderr=stderr_file,
                env=environment,
                close_fds=True,
                pass_fds=tuple(sorted(set((executable_fd, *inherited_fds)))),
            )
        except OSError as error:
            raise TorrentCompletedHandoffError("pinned ffprobe could not start") from error
        try:
            return_code = process.wait(timeout=FFPROBE_TIMEOUT_SECONDS)
        except subprocess.TimeoutExpired as error:
            process.kill()
            process.wait()
            raise TorrentCompletedHandoffError("pinned ffprobe timed out") from error
        stdout_size = os.fstat(stdout_file.fileno()).st_size
        stderr_size = os.fstat(stderr_file.fileno()).st_size
        if stdout_size > stdout_cap or stderr_size > stderr_cap:
            _fail("pinned ffprobe output exceeded its byte cap")
        stdout_file.seek(0)
        stderr_file.seek(0)
        stdout = stdout_file.read()
        stderr = stderr_file.read()
    if return_code != 0:
        tail = stderr.decode("utf-8", errors="replace")[-4096:]
        raise TorrentCompletedHandoffError(
            f"pinned ffprobe exited with status {return_code}: {tail}".rstrip()
        )
    return stdout, stderr


def _probe_pinned(
    selected_fd: int, ffprobe_path: str | None, expected_ffprobe_sha256: str | None
) -> tuple[dict[str, Any], dict[str, Any]]:
    if expected_ffprobe_sha256 is not None and not SHA256_RE.fullmatch(
        expected_ffprobe_sha256
    ):
        _fail("expected ffprobe SHA-256 must be lowercase hexadecimal")
    with _PinnedExecutable.open(ffprobe_path) as executable:
        if (
            expected_ffprobe_sha256 is not None
            and executable.sha256 != expected_ffprobe_sha256
        ):
            _fail("pinned ffprobe SHA-256 differs from the expected executable")
        version_stdout, _version_stderr = _run_pinned_process(
            executable.descriptor,
            ["-version"],
            inherited_fds=(),
            stdout_cap=64 * 1024,
            stderr_cap=64 * 1024,
        )
        try:
            version = version_stdout.decode("utf-8").splitlines()[0].strip()
        except (UnicodeDecodeError, IndexError) as error:
            raise TorrentCompletedHandoffError(
                "pinned ffprobe returned an invalid version"
            ) from error
        if not version or len(version) > 1_000:
            _fail("pinned ffprobe returned an invalid version")
        selected_descriptor_path = f"/proc/self/fd/{selected_fd}"
        arguments = [
            "-v",
            "error",
            "-show_format",
            "-show_streams",
            "-show_entries",
            FFPROBE_SHOW_ENTRIES,
            "-of",
            "json",
            selected_descriptor_path,
        ]
        probe_stdout, _probe_stderr = _run_pinned_process(
            executable.descriptor,
            arguments,
            inherited_fds=(selected_fd,),
            stdout_cap=MAX_FFPROBE_JSON_BYTES,
            stderr_cap=MAX_FFPROBE_STDERR_BYTES,
        )
        executable.assert_stable()
        executable_binding = {
            "path": os.fspath(executable.path),
            "sha256": executable.sha256,
            "byte_count": executable.initial_stat.st_size,
            "version": version,
            "command_contract": [
                "ffprobe",
                "-v",
                "error",
                "-show_format",
                "-show_streams",
                "-show_entries",
                FFPROBE_SHOW_ENTRIES,
                "-of",
                "json",
                "<pinned-selected-file-descriptor>",
            ],
        }
    try:
        raw = json.loads(probe_stdout.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError) as error:
        raise TorrentCompletedHandoffError(
            "pinned ffprobe returned invalid UTF-8 JSON"
        ) from error
    if not isinstance(raw, dict):
        _fail("pinned ffprobe result must be a JSON object")
    normalized = acquire.normalize_probe(raw, version)
    if not acquire._valid_probe(normalized):
        _fail("pinned ffprobe result is outside the normalized acquisition contract")
    if acquire.media_kind(normalized) not in {"video", "audio"}:
        _fail("selected target does not contain an audio or video stream")
    return normalized, executable_binding


def _safe_audit(**arguments: Any) -> dict[str, Any]:
    try:
        result = build_torrent_acquisition_audit(**arguments)
    except TorrentAcquisitionAuditError as error:
        raise TorrentCompletedHandoffError(str(error)) from error
    if not result["status"]["audit_passed"]:
        _fail("torrent acquisition audit did not pass the sealed selector scope")
    return result


def build_torrent_completed_handoff(
    *,
    plan_path: Path,
    selector_receipt_path: Path,
    torrent_path: Path,
    session_path: Path,
    control_path: Path,
    payload_root: Path,
    observed_at: str,
    expected_file_index: int,
    expected_manifest_path: str,
    expected_declared_byte_count: int,
    ffprobe_path: str | None = None,
    expected_ffprobe_sha256: str | None = None,
) -> dict[str, Any]:
    """Return a canonical-hash-bound, non-admitting work order for one file."""

    observed_at = _timestamp(observed_at)
    common_audit_arguments = {
        "plan_path": Path(plan_path),
        "selector_receipt_path": Path(selector_receipt_path),
        "torrent_path": Path(torrent_path),
        "session_path": Path(session_path),
        "control_path": Path(control_path),
        "payload_root": Path(payload_root),
        "observed_at": observed_at,
    }
    first_audit = _safe_audit(**common_audit_arguments)
    (
        plan,
        plan_body,
        selector,
        selector_body,
        torrent,
        torrent_body,
    ) = _load_bound_inputs(
        Path(plan_path), Path(selector_receipt_path), Path(torrent_path)
    )
    manifest, selected_row, target_pieces = _exact_selected_target(
        plan=plan,
        selector=selector,
        torrent=torrent,
        expected_file_index=expected_file_index,
        expected_manifest_path=expected_manifest_path,
        expected_declared_byte_count=expected_declared_byte_count,
    )
    selected_indices = selector["selection"]["zero_based_file_indices"]
    first_control_body, first_control_state = _control_snapshot(
        control_path=Path(control_path),
        torrent=torrent,
        selected_indices=selected_indices,
        target_pieces=target_pieces,
    )

    with _PinnedSelectedFile.open(
        payload_root=Path(payload_root),
        torrent_root_name=torrent["root_name"],
        raw_components=manifest["raw_components"],
        expected_size=expected_declared_byte_count,
    ) as selected_file:
        media_sha256 = _hash_pinned(
            selected_file.descriptor, expected_declared_byte_count
        )
        normalized_probe, ffprobe_binding = _probe_pinned(
            selected_file.descriptor, ffprobe_path, expected_ffprobe_sha256
        )
        selected_file.assert_stable()

    second_audit = _safe_audit(**common_audit_arguments)
    second_control_body, second_control_state = _control_snapshot(
        control_path=Path(control_path),
        torrent=torrent,
        selected_indices=selected_indices,
        target_pieces=target_pieces,
    )
    if first_audit != second_audit:
        _fail("sealed acquisition snapshot changed while hashing or probing; retry")
    if first_control_body != second_control_body:
        _fail("aria2 control state changed between stable reads; retry")
    if first_control_state != second_control_state:
        _fail("aria2 parsed control state changed between stable reads; retry")

    try:
        _session_path, session_body = _stable_unlinked_file(
            Path(session_path), MAX_SESSION_BYTES, "aria2 session"
        )
    except TorrentAcquisitionAuditError as error:
        raise TorrentCompletedHandoffError(str(error)) from error
    if sha256_bytes(session_body) != first_audit["bindings"]["session_sha256"]:
        _fail("aria2 session changed after the stable handoff audit")

    suffix = Path(expected_manifest_path).suffix.lower()
    media_kind = acquire.media_kind(normalized_probe)
    mime_type = mimetypes.guess_type(expected_manifest_path)[0]
    raw_components_base64 = [
        base64.b64encode(component).decode("ascii")
        for component in manifest["raw_components"]
    ]
    source_identity = {
        "platform": "bittorrent",
        "source_kind": "torrent_file_candidate",
        "native_id": f'{torrent["info_hash_sha1"]}/{expected_manifest_path}',
    }
    media_id = f"media_sha256_{media_sha256}"
    control_state = {
        key: value
        for key, value in first_control_state.items()
        if key != "completed_indices"
    }
    core = {
        "schema_version": SCHEMA_VERSION,
        "work_order_kind": WORK_ORDER_KIND,
        "producer": {"name": PRODUCER_NAME, "version": IMPLEMENTATION_VERSION},
        "observed_at": observed_at,
        "bindings": {
            "plan": {
                "plan_id": selector["source_plan"]["plan_id"],
                "canonical_sha256": selector["source_plan"]["plan_sha256"],
                "file_sha256": sha256_bytes(plan_body),
                "byte_count": len(plan_body),
            },
            "selector": {
                "receipt_id": selector["receipt_id"],
                "canonical_sha256": selector["receipt_sha256"],
                "file_sha256": sha256_bytes(selector_body),
                "byte_count": len(selector_body),
                "selected_file_count": len(selected_indices),
            },
            "torrent": {
                "sha256": torrent["torrent_sha256"],
                "byte_count": len(torrent_body),
                "info_hash_sha1": torrent["info_hash_sha1"],
                "file_count": torrent["file_count"],
                "piece_length_bytes": torrent["piece_length_bytes"],
                "piece_count": math.ceil(
                    torrent["total_bytes"] / torrent["piece_length_bytes"]
                ),
                "total_bytes": torrent["total_bytes"],
            },
            "session": {
                "sha256": sha256_bytes(session_body),
                "byte_count": len(session_body),
                "selector_matches": first_audit["selector"]["selector_matches"],
                "output_directory_matches": first_audit["selector"][
                    "output_directory_matches"
                ],
                "required_safety_options_match": first_audit["selector"][
                    "required_safety_options_match"
                ],
            },
            "control": {
                "sha256": sha256_bytes(first_control_body),
                "byte_count": len(first_control_body),
                "format_version": control_state["format_version"],
                "extension_marker": control_state["extension_marker"],
                "recorded_uploaded_bytes": control_state["recorded_uploaded_bytes"],
            },
        },
        "selected_file": {
            "torrent_file_index": expected_file_index,
            "aria2_file_index": expected_file_index + 1,
            "manifest_path": expected_manifest_path,
            "manifest_path_components_base64": raw_components_base64,
            "manifest_path_sha256": _raw_path_sha256(manifest["raw_components"]),
            "payload_relative_path": f'{torrent["root_name"]}/{expected_manifest_path}',
            "declared_byte_count": expected_declared_byte_count,
            "media_suffix": suffix,
            "first_piece_index": target_pieces[0],
            "last_piece_index": target_pieces[-1],
            "covering_piece_count": len(target_pieces),
            "covering_piece_indices_sha256": _piece_indices_sha256(target_pieces),
            "youtube_video_id": selected_row.get("youtube_video_id"),
            "availability_evidence_code": selected_row.get(
                "availability_evidence_code"
            ),
            "selection_reason": selected_row.get("selection_reason"),
            "source_identity": source_identity,
        },
        "verification": {
            "selector_receipt_replayed": True,
            "session_selector_matches": True,
            "target_is_selected": True,
            "target_is_boundary_artifact": False,
            "all_covering_pieces_complete": True,
            "covering_piece_in_flight_count": 0,
            "authorized_piece_state_only": True,
            "target_regular_non_symlink_single_link": True,
            "target_size_matches_manifest": True,
            "target_metadata_stable": True,
            "control_stable_double_read": True,
            "acquisition_snapshot_stable_before_and_after": True,
        },
        "media": {
            "media_id": media_id,
            "sha256": media_sha256,
            "byte_count": expected_declared_byte_count,
            "media_kind": media_kind,
            "mime_type": mime_type,
            "container": normalized_probe["format"]["format_name"],
            "duration_ms": normalized_probe["format"]["duration_ms"],
            "normalized_probe": normalized_probe,
        },
        "tooling": {"ffprobe": ffprobe_binding},
        "catalogue_handoff": {
            "required_action": (
                "resolve_existing_source_identity_copy_to_canonical_cache_and_admit_"
                "verified_rendition"
            ),
            "source_identity": source_identity,
            "source_must_preexist": True,
            "media_id": media_id,
            "requires_separate_catalogue_import": True,
            "catalogue_import_performed": False,
            "media_admitted": False,
            "publication_authority": False,
        },
        "policy": {
            "network_actions_performed": False,
            "torrent_client_invoked": False,
            "torrent_process_inspected": False,
            "torrent_state_mutated": False,
            "catalogue_mutated": False,
            "media_admitted": False,
            "payload_executed": False,
            "only_selected_payload_opened": True,
            "unselected_payload_files_opened": 0,
            "selected_payload_bytes_hashed": expected_declared_byte_count,
            "publication_authority": False,
            "human_review_required_before_catalogue_import": True,
        },
    }
    work_order_sha256 = sha256_bytes(canonical_bytes(core))
    return {
        **core,
        "work_order_id": f"tch_{work_order_sha256[:32]}",
        "work_order_sha256": work_order_sha256,
    }


def write_private_work_order(
    work_order: dict[str, Any], output_path: Path, *, protected_payload_root: Path
) -> dict[str, Any]:
    """Create one immutable mode-0400 canonical JSON file outside payload state."""

    requested = Path(output_path)
    if not requested.is_absolute() or requested.name in {"", ".", ".."}:
        _fail("handoff output must be an absolute file path")
    try:
        parent = requested.parent.resolve(strict=True)
        protected = Path(protected_payload_root).resolve(strict=True)
    except OSError as error:
        raise TorrentCompletedHandoffError("handoff output cannot be resolved") from error
    if parent != requested.parent or not parent.is_dir():
        _fail("handoff output parent must be a real non-symlink directory")
    try:
        parent.relative_to(protected)
    except ValueError:
        pass
    else:
        _fail("handoff output must be outside the torrent payload tree")
    destination = parent / requested.name
    try:
        destination.lstat()
    except FileNotFoundError:
        pass
    except OSError as error:
        raise TorrentCompletedHandoffError("handoff output cannot be inspected") from error
    else:
        _fail("handoff output already exists")

    body = canonical_bytes(work_order) + b"\n"
    descriptor, temporary_name = tempfile.mkstemp(
        prefix=f".{destination.name}.", suffix=".tmp", dir=parent
    )
    temporary = Path(temporary_name)
    try:
        os.fchmod(descriptor, 0o400)
        offset = 0
        while offset < len(body):
            written = os.write(descriptor, body[offset:])
            if written < 1:
                _fail("handoff output write made no progress")
            offset += written
        os.fsync(descriptor)
        os.close(descriptor)
        descriptor = -1
        try:
            os.link(temporary, destination, follow_symlinks=False)
        except FileExistsError as error:
            raise TorrentCompletedHandoffError(
                "handoff output already exists"
            ) from error
        directory_descriptor = os.open(
            parent, os.O_RDONLY | getattr(os, "O_DIRECTORY", 0)
        )
        try:
            os.fsync(directory_descriptor)
        finally:
            os.close(directory_descriptor)
    finally:
        if descriptor >= 0:
            os.close(descriptor)
        temporary.unlink(missing_ok=True)
    return {
        "valid": True,
        "work_order_written": True,
        "work_order_id": work_order["work_order_id"],
        "work_order_sha256": work_order["work_order_sha256"],
        "output_byte_count": len(body),
        "output_sha256": sha256_bytes(body),
        "output_mode": "0400",
        "catalogue_mutated": False,
        "media_admitted": False,
        "network_actions_performed": False,
    }


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=(
            "Hash and ffprobe exactly one complete selected torrent media file and "
            "emit a non-admitting catalogue handoff work order"
        )
    )
    parser.add_argument("--plan", required=True, type=Path)
    parser.add_argument("--selector-receipt", required=True, type=Path)
    parser.add_argument("--torrent", required=True, type=Path)
    parser.add_argument("--session", required=True, type=Path)
    parser.add_argument("--control", required=True, type=Path)
    parser.add_argument("--payload-root", required=True, type=Path)
    parser.add_argument("--observed-at", required=True)
    parser.add_argument("--file-index", required=True, type=int)
    parser.add_argument("--manifest-path", required=True)
    parser.add_argument("--declared-byte-count", required=True, type=int)
    parser.add_argument("--ffprobe-executable")
    parser.add_argument("--ffprobe-sha256")
    parser.add_argument(
        "--output",
        type=Path,
        help="write canonical JSON to this new absolute path outside the payload tree",
    )
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    try:
        work_order = build_torrent_completed_handoff(
            plan_path=args.plan,
            selector_receipt_path=args.selector_receipt,
            torrent_path=args.torrent,
            session_path=args.session,
            control_path=args.control,
            payload_root=args.payload_root,
            observed_at=args.observed_at,
            expected_file_index=args.file_index,
            expected_manifest_path=args.manifest_path,
            expected_declared_byte_count=args.declared_byte_count,
            ffprobe_path=args.ffprobe_executable,
            expected_ffprobe_sha256=args.ffprobe_sha256,
        )
        result: dict[str, Any] = work_order
        if args.output is not None:
            result = write_private_work_order(
                work_order, args.output, protected_payload_root=args.payload_root
            )
    except TorrentCompletedHandoffError as error:
        print(
            canonical_bytes(
                {
                    "error": {
                        "code": "torrent_completed_handoff_refused",
                        "message": str(error),
                    },
                    "valid": False,
                }
            ).decode("utf-8"),
            file=sys.stderr,
        )
        return 2
    print(canonical_bytes(result).decode("utf-8"))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
