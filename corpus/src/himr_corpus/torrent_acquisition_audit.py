"""Bounded, offline audit of one in-progress selective aria2 acquisition.

The auditor never imports or invokes aria2, opens a socket, executes a payload,
or reads payload bytes.  It binds an immutable finalized plan, selector receipt,
and torrent; validates aria2's saved selector; parses the pinned v1 control-file
format; and inventories filesystem metadata twice to reject a moving snapshot.

Only a separately requested, path-free JSON receipt may be written.  Acquisition
payload, client state, catalogue state, and publication state are never mutated.
"""

from __future__ import annotations

import argparse
import bisect
import hashlib
import json
import math
import os
import re
import stat
import struct
import tempfile
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any

from .importers import canonical_json, sha256_bytes
from .torrent_aria2_selector import (
    TorrentAria2SelectorError,
    build_aria2_selector_receipt,
)
from .torrent_bracket_reconciler import (
    MAX_TORRENT_BYTES,
    TorrentBracketReconciliationError,
    _parse_torrent,
    _raw_path_sha256,
    _stable_file,
    _strict_json_bytes,
)


AUDITOR_VERSION = "torrent_acquisition_audit_v1"
RECEIPT_KIND = "torrent_acquisition_read_only_audit"
MAX_PLAN_BYTES = 64 * 1024 * 1024
MAX_SELECTOR_RECEIPT_BYTES = 8 * 1024 * 1024
MAX_SESSION_BYTES = 4 * 1024 * 1024
MAX_CONTROL_BYTES = 64 * 1024 * 1024
MAX_AUDIT_PIECES = 2_000_000
MAX_TREE_ENTRIES = 250_000
MAX_SESSION_LINES = 4096
MAX_SESSION_LINE_BYTES = 128 * 1024
ARIA2_CONTROL_VERSION = 1
ARIA2_CONTROL_EXTENSION_MARKER = 1
ARIA2_BLOCK_BYTES = 16 * 1024
RFC3339_UTC_RE = re.compile(r"^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}Z$")
SESSION_KEY_RE = re.compile(r"^[a-z0-9-]{1,64}$")

RISK_SUFFIXES = {
    ".appimage": "executable",
    ".bat": "script",
    ".bash": "script",
    ".cjs": "script",
    ".cmd": "script",
    ".com": "executable",
    ".desktop": "shortcut",
    ".exe": "executable",
    ".fish": "script",
    ".hta": "script",
    ".jar": "executable",
    ".js": "script",
    ".lnk": "shortcut",
    ".mjs": "script",
    ".msi": "executable",
    ".pl": "script",
    ".ps1": "script",
    ".py": "script",
    ".rb": "script",
    ".reg": "script",
    ".scr": "executable",
    ".sh": "script",
    ".url": "shortcut",
    ".vbe": "script",
    ".vbs": "script",
    ".webloc": "shortcut",
    ".wsf": "script",
    ".zsh": "script",
}

REQUIRED_SESSION_OPTIONS = {
    "allow-overwrite": "false",
    "auto-file-renaming": "false",
    "bt-enable-lpd": "false",
    "bt-remove-unselected-file": "false",
    "check-integrity": "true",
    "continue": "true",
    "file-allocation": "none",
    "seed-time": "0",
}


class TorrentAcquisitionAuditError(ValueError):
    """Raised when a trustworthy, bounded audit receipt cannot be produced."""


def _fail(message: str) -> None:
    raise TorrentAcquisitionAuditError(message)


@dataclass(frozen=True)
class _ManifestRow:
    index: int
    raw_components: tuple[bytes, ...]
    path_sha256: str
    byte_count: int
    start: int
    end: int
    first_piece: int
    last_piece: int
    suffix: str | None


def _absolute_real(path: Path, label: str) -> Path:
    requested = Path(os.path.abspath(os.fspath(Path(path))))
    try:
        resolved = requested.resolve(strict=True)
    except OSError as error:
        raise TorrentAcquisitionAuditError(f"{label} cannot be resolved") from error
    if requested != resolved:
        _fail(f"{label} path contains a symlink")
    return requested


def _stable_unlinked_file(path: Path, maximum: int, label: str) -> tuple[Path, bytes]:
    requested = _absolute_real(path, label)
    try:
        resolved, body = _stable_file(requested, maximum, label)
        current = resolved.lstat()
    except TorrentBracketReconciliationError as error:
        raise TorrentAcquisitionAuditError(str(error)) from error
    except OSError as error:
        raise TorrentAcquisitionAuditError(f"{label} cannot be inspected") from error
    if current.st_nlink != 1:
        _fail(f"{label} must not be hard-linked")
    return resolved, body


def _strict_json(body: bytes, label: str) -> dict[str, Any]:
    try:
        return _strict_json_bytes(body, label)
    except TorrentBracketReconciliationError as error:
        raise TorrentAcquisitionAuditError(str(error)) from error


def _observed_at(value: str) -> str:
    if not isinstance(value, str) or not RFC3339_UTC_RE.fullmatch(value):
        _fail("observed-at must be whole-second RFC3339 UTC")
    try:
        parsed = datetime.strptime(value, "%Y-%m-%dT%H:%M:%SZ")
    except ValueError as error:
        raise TorrentAcquisitionAuditError("observed-at is not a real UTC time") from error
    if parsed.strftime("%Y-%m-%dT%H:%M:%SZ") != value:
        _fail("observed-at is not canonical RFC3339 UTC")
    return value


def _suffix(component: bytes) -> str | None:
    _stem, separator, extension = component.rpartition(b".")
    if not separator or not extension or len(extension) > 32:
        return None
    try:
        decoded = "." + extension.decode("ascii").lower()
    except UnicodeDecodeError:
        return None
    return decoded


def _manifest_rows(torrent: dict[str, Any]) -> tuple[list[_ManifestRow], int]:
    total = torrent["total_bytes"]
    piece_length = torrent["piece_length_bytes"]
    piece_count = math.ceil(total / piece_length) if total else 0
    if piece_count < 1 or piece_count > MAX_AUDIT_PIECES:
        _fail("torrent piece count exceeds the bounded acquisition-audit cap")
    rows: list[_ManifestRow] = []
    offset = 0
    for source in torrent["files"]:
        length = source["byte_count"]
        first_piece = offset // piece_length if length else 0
        last_piece = (offset + length - 1) // piece_length if length else -1
        raw = source["raw_components"]
        rows.append(
            _ManifestRow(
                index=source["file_index"],
                raw_components=raw,
                path_sha256=_raw_path_sha256(raw),
                byte_count=length,
                start=offset,
                end=offset + length,
                first_piece=first_piece,
                last_piece=last_piece,
                suffix=_suffix(raw[-1]),
            )
        )
        offset += length
    if offset != total:
        _fail("torrent manifest byte offsets are inconsistent")
    return rows, piece_count


def _merge_piece_ranges(rows: list[_ManifestRow], selected: set[int]) -> list[tuple[int, int]]:
    raw = sorted(
        (row.first_piece, row.last_piece)
        for row in rows
        if row.index in selected and row.byte_count > 0
    )
    if not raw:
        _fail("selector has no positive-length torrent files")
    merged: list[tuple[int, int]] = []
    for start, end in raw:
        if merged and start <= merged[-1][1] + 1:
            merged[-1] = (merged[-1][0], max(end, merged[-1][1]))
        else:
            merged.append((start, end))
    return merged


def _range_helpers(
    ranges: list[tuple[int, int]],
) -> tuple[list[int], list[int]]:
    return [start for start, _end in ranges], [end for _start, end in ranges]


def _piece_authorized(piece: int, starts: list[int], ends: list[int]) -> bool:
    position = bisect.bisect_right(starts, piece) - 1
    return position >= 0 and piece <= ends[position]


def _covered_bytes(
    row: _ManifestRow,
    ranges: list[tuple[int, int]],
    starts: list[int],
    ends: list[int],
    piece_length: int,
) -> int:
    if row.byte_count == 0:
        return 0
    position = bisect.bisect_left(ends, row.first_piece)
    covered = 0
    while position < len(ranges) and starts[position] <= row.last_piece:
        first = max(row.first_piece, starts[position])
        last = min(row.last_piece, ends[position])
        if first <= last:
            byte_start = max(row.start, first * piece_length)
            byte_end = min(row.end, (last + 1) * piece_length)
            covered += max(0, byte_end - byte_start)
        position += 1
    return covered


def _piece_bytes(piece: int, piece_length: int, total_bytes: int) -> int:
    return min(piece_length, total_bytes - piece * piece_length)


def _piece_span_bytes(
    ranges: list[tuple[int, int]], piece_length: int, total_bytes: int
) -> int:
    total = 0
    last_global_piece = math.ceil(total_bytes / piece_length) - 1
    for first, last in ranges:
        count = last - first + 1
        total += count * piece_length
        if first <= last_global_piece <= last:
            total -= piece_length - _piece_bytes(
                last_global_piece, piece_length, total_bytes
            )
    return total


def _indices_sha256(values: list[int]) -> str:
    digest = hashlib.sha256()
    for value in values:
        digest.update(value.to_bytes(8, "big"))
    return digest.hexdigest()


def _unused_bits_are_zero(bitmap: bytes, item_count: int) -> bool:
    remainder = item_count % 8
    if not remainder or not bitmap:
        return True
    return not (bitmap[-1] & ((1 << (8 - remainder)) - 1))


def _set_bits(bitmap: bytes, item_count: int) -> list[int]:
    return [
        byte_index * 8 + bit_index
        for byte_index, byte in enumerate(bitmap)
        for bit_index in range(8)
        if byte & (0x80 >> bit_index) and byte_index * 8 + bit_index < item_count
    ]


def _parse_control(
    body: bytes,
    torrent: dict[str, Any],
    piece_count: int,
    required_starts: list[int],
    required_ends: list[int],
) -> dict[str, Any]:
    position = 0

    def take(length: int, label: str) -> bytes:
        nonlocal position
        if length < 0 or position + length > len(body):
            _fail(f"aria2 control file ended inside {label}")
        result = body[position : position + length]
        position += length
        return result

    def integer(fmt: str, length: int, label: str) -> int:
        return struct.unpack(fmt, take(length, label))[0]

    version = integer(">H", 2, "format version")
    extension = integer(">I", 4, "extension marker")
    if version != ARIA2_CONTROL_VERSION or extension != ARIA2_CONTROL_EXTENSION_MARKER:
        _fail("aria2 control file uses an unsupported format")
    info_hash_length = integer(">I", 4, "info-hash length")
    if info_hash_length != 20:
        _fail("aria2 control file info-hash length differs")
    info_hash = take(info_hash_length, "info hash").hex()
    if info_hash != torrent["info_hash_sha1"]:
        _fail("aria2 control file is bound to a different torrent info hash")
    piece_length = integer(">I", 4, "piece length")
    total_bytes = integer(">Q", 8, "torrent byte count")
    uploaded_bytes = integer(">Q", 8, "uploaded-byte counter")
    if piece_length != torrent["piece_length_bytes"] or total_bytes != torrent["total_bytes"]:
        _fail("aria2 control file torrent geometry differs")

    bitfield_length = integer(">I", 4, "completed-piece bitfield length")
    expected_bitfield_length = math.ceil(piece_count / 8)
    if bitfield_length != expected_bitfield_length:
        _fail("aria2 control file completed-piece bitfield length differs")
    completed_bitmap = take(bitfield_length, "completed-piece bitfield")
    if not _unused_bits_are_zero(completed_bitmap, piece_count):
        _fail("aria2 control file sets completed-piece bits beyond the torrent")
    completed = _set_bits(completed_bitmap, piece_count)

    in_flight_count = integer(">I", 4, "in-flight piece count")
    if in_flight_count > piece_count:
        _fail("aria2 control file in-flight count exceeds the torrent")
    in_flight: list[tuple[int, int]] = []
    seen: set[int] = set()
    completed_set = set(completed)
    for item in range(in_flight_count):
        piece_index = integer(">I", 4, f"in-flight piece {item} index")
        piece_bytes = integer(">I", 4, f"in-flight piece {item} length")
        block_bitmap_length = integer(">I", 4, f"in-flight piece {item} bitmap length")
        if piece_index >= piece_count or piece_index in seen:
            _fail("aria2 control file has an invalid or duplicate in-flight piece")
        if piece_index in completed_set:
            _fail("aria2 control file marks one piece complete and in-flight")
        expected_piece_bytes = _piece_bytes(piece_index, piece_length, total_bytes)
        expected_blocks = math.ceil(expected_piece_bytes / ARIA2_BLOCK_BYTES)
        expected_block_bitmap_length = math.ceil(expected_blocks / 8)
        if (
            piece_bytes != expected_piece_bytes
            or block_bitmap_length != expected_block_bitmap_length
        ):
            _fail("aria2 control file in-flight piece geometry differs")
        bitmap = take(block_bitmap_length, f"in-flight piece {item} bitmap")
        if not _unused_bits_are_zero(bitmap, expected_blocks):
            _fail("aria2 control file sets in-flight bits beyond the piece")
        present = 0
        for block in _set_bits(bitmap, expected_blocks):
            present += min(
                ARIA2_BLOCK_BYTES,
                expected_piece_bytes - block * ARIA2_BLOCK_BYTES,
            )
        seen.add(piece_index)
        in_flight.append((piece_index, present))
    if position != len(body):
        _fail("aria2 control file has trailing bytes")

    authorized_completed = [
        piece
        for piece in completed
        if _piece_authorized(piece, required_starts, required_ends)
    ]
    unauthorized_completed = sorted(set(completed) - set(authorized_completed))
    authorized_in_flight = sorted(
        (piece, present)
        for piece, present in in_flight
        if _piece_authorized(piece, required_starts, required_ends)
    )
    unauthorized_in_flight = sorted(set(in_flight) - set(authorized_in_flight))

    def completed_summary(values: list[int]) -> tuple[int, int, str]:
        return (
            len(values),
            sum(_piece_bytes(piece, piece_length, total_bytes) for piece in values),
            _indices_sha256(values),
        )

    completed_authorized_count, completed_authorized_bytes, completed_authorized_sha = (
        completed_summary(authorized_completed)
    )
    completed_unauthorized_count, completed_unauthorized_bytes, completed_unauthorized_sha = (
        completed_summary(unauthorized_completed)
    )
    in_flight_authorized_indices = [piece for piece, _present in authorized_in_flight]
    in_flight_unauthorized_indices = [piece for piece, _present in unauthorized_in_flight]
    return {
        "format_version": version,
        "extension_marker": extension,
        "recorded_uploaded_bytes": uploaded_bytes,
        "completed": {
            "total_piece_count": len(completed),
            "total_bytes": completed_authorized_bytes + completed_unauthorized_bytes,
            "authorized_piece_count": completed_authorized_count,
            "authorized_bytes": completed_authorized_bytes,
            "authorized_indices_sha256": completed_authorized_sha,
            "unauthorized_piece_count": completed_unauthorized_count,
            "unauthorized_bytes": completed_unauthorized_bytes,
            "unauthorized_indices_sha256": completed_unauthorized_sha,
        },
        "in_flight": {
            "total_piece_count": len(in_flight),
            "total_present_bytes": sum(present for _piece, present in in_flight),
            "authorized_piece_count": len(authorized_in_flight),
            "authorized_present_bytes": sum(
                present for _piece, present in authorized_in_flight
            ),
            "authorized_indices_sha256": _indices_sha256(
                in_flight_authorized_indices
            ),
            "unauthorized_piece_count": len(unauthorized_in_flight),
            "unauthorized_present_bytes": sum(
                present for _piece, present in unauthorized_in_flight
            ),
            "unauthorized_indices_sha256": _indices_sha256(
                in_flight_unauthorized_indices
            ),
        },
        "authorized_only": not unauthorized_completed and not unauthorized_in_flight,
        "completed_indices": completed,
    }


def _parse_session(
    body: bytes,
    torrent_path: Path,
    payload_root: Path,
    expected_selector: str,
) -> dict[str, Any]:
    if len(body.splitlines()) > MAX_SESSION_LINES:
        _fail("aria2 session exceeds the line cap")
    if not body.endswith(b"\n"):
        _fail("aria2 session must end with a newline")
    try:
        text = body.decode("utf-8")
    except UnicodeDecodeError as error:
        raise TorrentAcquisitionAuditError("aria2 session is not UTF-8") from error
    lines = text.splitlines()
    if not lines or lines[0] != os.fspath(torrent_path):
        _fail("aria2 session torrent path differs")
    options: dict[str, str] = {}
    for line in lines[1:]:
        if len(line.encode("utf-8")) > MAX_SESSION_LINE_BYTES or not line.startswith(" "):
            _fail("aria2 session has an invalid option line")
        option = line[1:]
        key, separator, value = option.partition("=")
        if (
            not separator
            or not SESSION_KEY_RE.fullmatch(key)
            or not value
            or any(ord(character) < 32 for character in value)
            or key in options
        ):
            _fail("aria2 session has an invalid or duplicate option")
        options[key] = value
    if options.get("select-file") != expected_selector:
        _fail("aria2 session selector differs from the sealed receipt")
    if options.get("dir") != os.fspath(payload_root):
        _fail("aria2 session output directory differs")
    if any(options.get(key) != value for key, value in REQUIRED_SESSION_OPTIONS.items()):
        _fail("aria2 session safety options differ")
    return {
        "selector_matches": True,
        "torrent_path_matches": True,
        "output_directory_matches": True,
        "required_safety_options_match": True,
        "option_count": len(options),
    }


def _directory(path: Path, label: str) -> tuple[Path, os.stat_result]:
    requested = _absolute_real(path, label)
    try:
        current = requested.lstat()
    except OSError as error:
        raise TorrentAcquisitionAuditError(f"{label} cannot be inspected") from error
    if not stat.S_ISDIR(current.st_mode):
        _fail(f"{label} must be a real directory")
    return requested, current


def _fingerprint(value: os.stat_result) -> tuple[int, ...]:
    return (
        value.st_dev,
        value.st_ino,
        value.st_mode,
        value.st_nlink,
        value.st_size,
        value.st_blocks,
        value.st_mtime_ns,
        value.st_ctime_ns,
    )


def _scan_payload(
    payload_root: Path,
    control_path: Path,
    torrent: dict[str, Any],
    rows: list[_ManifestRow],
) -> dict[str, Any]:
    payload_root, payload_stat = _directory(payload_root, "torrent payload root")
    root_name = torrent["root_name"]
    if (
        root_name in {".", ".."}
        or "/" in root_name
        or "\\" in root_name
        or "\x00" in root_name
        or os.path.isabs(root_name)
    ):
        _fail("torrent root name is unsafe for filesystem audit")
    expected_control = payload_root / f"{root_name}.aria2"
    if control_path != expected_control:
        _fail("aria2 control path is not the exact payload-root control file")
    root_name_bytes = root_name.encode("utf-8")
    control_name_bytes = f"{root_name}.aria2".encode("utf-8")
    expected_files = {row.raw_components: row for row in rows}
    expected_directories: set[tuple[bytes, ...]] = set()
    for row in rows:
        for depth in range(1, len(row.raw_components)):
            expected_directories.add(row.raw_components[:depth])

    fingerprints: dict[tuple[str, tuple[bytes, ...]], tuple[int, ...]] = {
        ("payload-root", ()): _fingerprint(payload_stat)
    }
    directory_flags = (
        os.O_RDONLY
        | getattr(os, "O_CLOEXEC", 0)
        | getattr(os, "O_DIRECTORY", 0)
        | getattr(os, "O_NOFOLLOW", 0)
    )
    try:
        payload_descriptor = os.open(payload_root, directory_flags)
    except OSError as error:
        raise TorrentAcquisitionAuditError(
            "torrent payload root cannot be opened without following links"
        ) from error
    actual_files: dict[tuple[bytes, ...], os.stat_result] = {}
    created_directory_count = 1
    entry_count = 2
    try:
        opened_payload_stat = os.fstat(payload_descriptor)
        if (
            opened_payload_stat.st_dev != payload_stat.st_dev
            or opened_payload_stat.st_ino != payload_stat.st_ino
        ):
            _fail("torrent payload root changed before descriptor pinning")
        try:
            top_entries = sorted(
                os.scandir(payload_descriptor), key=lambda entry: os.fsencode(entry.name)
            )
        except OSError as error:
            raise TorrentAcquisitionAuditError(
                "torrent payload root cannot be scanned"
            ) from error
        top_names = {os.fsencode(entry.name) for entry in top_entries}
        if len(top_entries) != 2 or top_names != {
            root_name_bytes,
            control_name_bytes,
        }:
            _fail("torrent payload root contains an unknown or missing entry")
        root_entry = next(
            entry for entry in top_entries if os.fsencode(entry.name) == root_name_bytes
        )
        control_entry = next(
            entry for entry in top_entries if os.fsencode(entry.name) == control_name_bytes
        )
        root_stat = root_entry.stat(follow_symlinks=False)
        control_stat = control_entry.stat(follow_symlinks=False)
        if (
            root_entry.is_symlink()
            or not stat.S_ISDIR(root_stat.st_mode)
            or root_stat.st_dev != payload_stat.st_dev
        ):
            _fail("torrent payload directory is a symlink, special node, or mount escape")
        if (
            control_entry.is_symlink()
            or not stat.S_ISREG(control_stat.st_mode)
            or control_stat.st_nlink != 1
            or control_stat.st_dev != payload_stat.st_dev
        ):
            _fail("aria2 control entry is a symlink, hardlink, special node, or mount escape")
        fingerprints[("torrent-root", ())] = _fingerprint(root_stat)
        fingerprints[("control", ())] = _fingerprint(control_stat)
        try:
            root_descriptor = os.open(
                root_entry.name, directory_flags, dir_fd=payload_descriptor
            )
        except OSError as error:
            raise TorrentAcquisitionAuditError(
                "torrent payload directory cannot be pinned without following links"
            ) from error
        try:
            opened_root_stat = os.fstat(root_descriptor)
            if (
                opened_root_stat.st_dev != root_stat.st_dev
                or opened_root_stat.st_ino != root_stat.st_ino
            ):
                _fail("torrent payload directory changed before descriptor pinning")

            def walk(directory_descriptor: int, prefix: tuple[bytes, ...]) -> None:
                nonlocal created_directory_count, entry_count
                try:
                    entries = sorted(
                        os.scandir(directory_descriptor),
                        key=lambda entry: os.fsencode(entry.name),
                    )
                except OSError as error:
                    raise TorrentAcquisitionAuditError(
                        "torrent payload directory cannot be scanned"
                    ) from error
                entry_count += len(entries)
                if entry_count > MAX_TREE_ENTRIES:
                    _fail("torrent payload tree exceeds the audit entry cap")
                for entry in entries:
                    name = os.fsencode(entry.name)
                    relative = prefix + (name,)
                    current = entry.stat(follow_symlinks=False)
                    if entry.is_symlink() or stat.S_ISLNK(current.st_mode):
                        _fail("torrent payload contains a symlink")
                    if current.st_dev != payload_stat.st_dev:
                        _fail("torrent payload contains a mount escape")
                    if stat.S_ISDIR(current.st_mode):
                        if relative not in expected_directories:
                            _fail("torrent payload contains an unknown directory")
                        try:
                            child_descriptor = os.open(
                                entry.name,
                                directory_flags,
                                dir_fd=directory_descriptor,
                            )
                        except OSError as error:
                            raise TorrentAcquisitionAuditError(
                                "torrent payload child directory cannot be pinned"
                            ) from error
                        try:
                            opened = os.fstat(child_descriptor)
                            if (
                                opened.st_dev != current.st_dev
                                or opened.st_ino != current.st_ino
                            ):
                                _fail(
                                    "torrent payload child changed before descriptor pinning"
                                )
                            fingerprints[("directory", relative)] = _fingerprint(
                                opened
                            )
                            created_directory_count += 1
                            walk(child_descriptor, relative)
                        finally:
                            os.close(child_descriptor)
                    elif stat.S_ISREG(current.st_mode):
                        if current.st_nlink != 1:
                            _fail("torrent payload contains a hard-linked file")
                        if relative not in expected_files:
                            _fail("torrent payload contains an unknown file")
                        row = expected_files[relative]
                        if current.st_size > row.byte_count:
                            _fail("torrent payload file exceeds its manifest byte count")
                        fingerprints[("file", relative)] = _fingerprint(current)
                        actual_files[relative] = current
                    else:
                        _fail("torrent payload contains a special node")

            walk(root_descriptor, ())
        finally:
            os.close(root_descriptor)
    finally:
        os.close(payload_descriptor)
    return {
        "fingerprints": fingerprints,
        "actual_files": actual_files,
        "created_directory_count": created_directory_count,
    }


def _inventory_receipt(
    scan: dict[str, Any],
    rows: list[_ManifestRow],
    selected: set[int],
    ranges: list[tuple[int, int]],
    starts: list[int],
    ends: list[int],
    piece_length: int,
    completed_indices: list[int],
) -> tuple[dict[str, Any], list[dict[str, Any]]]:
    actual_files: dict[tuple[bytes, ...], os.stat_result] = scan["actual_files"]
    completed_sorted = sorted(completed_indices)

    selected_expected_bytes = 0
    selected_created_count = 0
    selected_logical = 0
    selected_allocated = 0
    selected_complete = 0
    boundary_eligible_count = 0
    boundary_covered_bytes = 0
    boundary_created_count = 0
    boundary_logical = 0
    boundary_allocated = 0
    boundary_complete = 0
    boundary_fully_piece_covered_count = 0
    boundary_fully_piece_covered_bytes = 0
    risky: list[dict[str, Any]] = []

    def all_pieces_complete(row: _ManifestRow) -> bool:
        if row.byte_count == 0:
            return True
        left = bisect.bisect_left(completed_sorted, row.first_piece)
        right = bisect.bisect_right(completed_sorted, row.last_piece)
        return right - left == row.last_piece - row.first_piece + 1

    for row in rows:
        current = actual_files.get(row.raw_components)
        selected_row = row.index in selected
        covered = _covered_bytes(row, ranges, starts, ends, piece_length)
        if current is not None and not selected_row and covered == 0:
            _fail("torrent payload contains a manifest file outside selected piece spans")
        control_and_size_complete = bool(
            current is not None
            and current.st_size == row.byte_count
            and (row.byte_count == 0 or current.st_blocks > 0)
            and all_pieces_complete(row)
        )
        if selected_row:
            selected_expected_bytes += row.byte_count
            if current is not None:
                selected_created_count += 1
                selected_logical += current.st_size
                selected_allocated += current.st_blocks * 512
                selected_complete += control_and_size_complete
        elif covered:
            boundary_eligible_count += 1
            boundary_covered_bytes += covered
            fully_piece_covered = covered == row.byte_count
            if fully_piece_covered:
                boundary_fully_piece_covered_count += 1
                boundary_fully_piece_covered_bytes += row.byte_count
            if current is not None:
                boundary_created_count += 1
                boundary_logical += current.st_size
                boundary_allocated += current.st_blocks * 512
                boundary_complete += control_and_size_complete
            risk_class = RISK_SUFFIXES.get(row.suffix or "")
            if risk_class:
                risky.append(
                    {
                        "torrent_file_index": row.index,
                        "manifest_path_sha256": row.path_sha256,
                        "suffix": row.suffix,
                        "risk_class": risk_class,
                        "byte_count": row.byte_count,
                        "piece_covered_bytes": covered,
                        "fully_piece_covered": fully_piece_covered,
                        "file_present": current is not None,
                        "fully_materialized": control_and_size_complete,
                    }
                )
    risky.sort(key=lambda row: row["torrent_file_index"])
    inventory = {
        "expected_manifest_file_count": len(rows),
        "created_manifest_file_count": len(actual_files),
        "created_directory_count": scan["created_directory_count"],
        "selected": {
            "expected_file_count": len(selected),
            "expected_bytes": selected_expected_bytes,
            "created_file_count": selected_created_count,
            "created_logical_bytes": selected_logical,
            "created_allocated_bytes": selected_allocated,
            "control_and_size_complete_file_count": selected_complete,
        },
        "boundary": {
            "eligible_file_count": boundary_eligible_count,
            "piece_covered_bytes": boundary_covered_bytes,
            "created_file_count": boundary_created_count,
            "created_logical_bytes": boundary_logical,
            "created_allocated_bytes": boundary_allocated,
            "control_and_size_complete_file_count": boundary_complete,
            "fully_piece_covered_file_count": boundary_fully_piece_covered_count,
            "fully_piece_covered_bytes": boundary_fully_piece_covered_bytes,
        },
        "unknown_file_count": 0,
        "ineligible_created_file_count": 0,
        "risky_unselected_artifact_count": len(risky),
        "fully_materialized_risky_unselected_artifact_count": sum(
            row["fully_materialized"] for row in risky
        ),
    }
    return inventory, risky


def build_torrent_acquisition_audit(
    *,
    plan_path: Path,
    selector_receipt_path: Path,
    torrent_path: Path,
    session_path: Path,
    control_path: Path,
    payload_root: Path,
    observed_at: str,
) -> dict[str, Any]:
    """Build one path-free receipt from a stable, metadata-only acquisition snapshot."""

    observed_at = _observed_at(observed_at)
    plan_path, plan_body = _stable_unlinked_file(
        plan_path, MAX_PLAN_BYTES, "finalized torrent plan"
    )
    selector_receipt_path, selector_body = _stable_unlinked_file(
        selector_receipt_path,
        MAX_SELECTOR_RECEIPT_BYTES,
        "aria2 selector receipt",
    )
    torrent_path, torrent_body = _stable_unlinked_file(
        torrent_path, MAX_TORRENT_BYTES, "torrent manifest"
    )
    session_path, session_body = _stable_unlinked_file(
        session_path, MAX_SESSION_BYTES, "aria2 session"
    )
    control_path, control_body = _stable_unlinked_file(
        control_path, MAX_CONTROL_BYTES, "aria2 control file"
    )
    payload_root, _payload_stat = _directory(payload_root, "torrent payload root")

    try:
        torrent = _parse_torrent(torrent_body)
    except TorrentBracketReconciliationError as error:
        raise TorrentAcquisitionAuditError(str(error)) from error
    rows, piece_count = _manifest_rows(torrent)
    try:
        expected_selector = build_aria2_selector_receipt(plan_path, torrent_path)
    except TorrentAria2SelectorError as error:
        raise TorrentAcquisitionAuditError(str(error)) from error
    stored_selector = _strict_json(selector_body, "aria2 selector receipt")
    if stored_selector != expected_selector:
        _fail("aria2 selector receipt differs from the exact sealed plan and torrent")
    selection = expected_selector["selection"]
    selected_indices = selection["zero_based_file_indices"]
    selected = set(selected_indices)
    ranges = _merge_piece_ranges(rows, selected)
    starts, ends = _range_helpers(ranges)
    selected_piece_count = sum(last - first + 1 for first, last in ranges)
    selected_piece_span = _piece_span_bytes(
        ranges, torrent["piece_length_bytes"], torrent["total_bytes"]
    )
    if selected_piece_span != selection["selected_piece_span_bytes"]:
        _fail("selector receipt piece span differs from the bounded audit replay")

    session_state = _parse_session(
        session_body,
        torrent_path,
        payload_root,
        selection["aria2_select_file_value"],
    )
    control_state = _parse_control(
        control_body, torrent, piece_count, starts, ends
    )
    first_scan = _scan_payload(payload_root, control_path, torrent, rows)
    inventory, risky = _inventory_receipt(
        first_scan,
        rows,
        selected,
        ranges,
        starts,
        ends,
        torrent["piece_length_bytes"],
        control_state["completed_indices"],
    )
    if (
        inventory["boundary"]["piece_covered_bytes"]
        != selection["boundary_piece_bytes_upper_bound"]
    ):
        _fail("boundary-file accounting differs from the sealed selector receipt")

    # Prove the acquisition metadata did not move while the receipt was assembled.
    _session_path_2, session_body_2 = _stable_unlinked_file(
        session_path, MAX_SESSION_BYTES, "aria2 session"
    )
    _control_path_2, control_body_2 = _stable_unlinked_file(
        control_path, MAX_CONTROL_BYTES, "aria2 control file"
    )
    second_scan = _scan_payload(payload_root, control_path, torrent, rows)
    if session_body_2 != session_body or control_body_2 != control_body:
        _fail("aria2 session or control state changed during audit; retry")
    if second_scan["fingerprints"] != first_scan["fingerprints"]:
        _fail("torrent payload metadata changed during audit; retry")
    for original_path, original_body, maximum, label in (
        (plan_path, plan_body, MAX_PLAN_BYTES, "finalized torrent plan"),
        (
            selector_receipt_path,
            selector_body,
            MAX_SELECTOR_RECEIPT_BYTES,
            "aria2 selector receipt",
        ),
        (torrent_path, torrent_body, MAX_TORRENT_BYTES, "torrent manifest"),
    ):
        _path_2, body_2 = _stable_unlinked_file(original_path, maximum, label)
        if body_2 != original_body:
            _fail(f"{label} changed during audit; retry")

    completed_indices = control_state.pop("completed_indices")
    if len(completed_indices) != control_state["completed"]["total_piece_count"]:
        _fail("internal completed-piece accounting differs")
    authorized_only = control_state["authorized_only"]
    core = {
        "schema_version": 1,
        "receipt_kind": RECEIPT_KIND,
        "auditor_version": AUDITOR_VERSION,
        "observed_at": observed_at,
        "bindings": {
            "plan_id": expected_selector["source_plan"]["plan_id"],
            "plan_sha256": expected_selector["source_plan"]["plan_sha256"],
            "plan_file_sha256": sha256_bytes(plan_body),
            "selector_receipt_id": expected_selector["receipt_id"],
            "selector_receipt_sha256": expected_selector["receipt_sha256"],
            "selector_file_sha256": sha256_bytes(selector_body),
            "torrent_sha256": torrent["torrent_sha256"],
            "info_hash_sha1": torrent["info_hash_sha1"],
            "torrent_file_count": torrent["file_count"],
            "torrent_piece_count": piece_count,
            "piece_length_bytes": torrent["piece_length_bytes"],
            "torrent_total_bytes": torrent["total_bytes"],
            "session_sha256": sha256_bytes(session_body),
            "session_byte_count": len(session_body),
            "control_sha256": sha256_bytes(control_body),
            "control_byte_count": len(control_body),
        },
        "selector": {
            "selected_file_count": len(selected_indices),
            "selected_payload_bytes": selection["selected_payload_bytes"],
            "selected_piece_count": selected_piece_count,
            "selected_piece_span_bytes": selected_piece_span,
            "boundary_piece_bytes_upper_bound": selection[
                "boundary_piece_bytes_upper_bound"
            ],
            **session_state,
        },
        "control_state": control_state,
        "inventory": inventory,
        "risky_unselected_artifacts": risky,
        "status": {
            "metadata_snapshot_stable": True,
            "selector_scope_status": (
                "authorized_only"
                if authorized_only
                else "unauthorized_piece_state_observed"
            ),
            "audit_passed": authorized_only,
            "fully_materialized_unselected_risky_artifacts_observed": bool(
                inventory["fully_materialized_risky_unselected_artifact_count"]
            ),
        },
        "policy": {
            "read_only_acquisition_audit": True,
            "acquisition_state_mutated": False,
            "network_actions_performed": False,
            "torrent_client_invoked_by_auditor": False,
            "runtime_process_inspected": False,
            "payload_files_opened": 0,
            "payload_bytes_read": 0,
            "payload_executed": False,
            "paths_disclosed": False,
            "publication_authority": False,
        },
    }
    receipt_sha256 = sha256_bytes(canonical_json(core).encode("utf-8"))
    return {
        **core,
        "receipt_id": f"taudit_{receipt_sha256[:32]}",
        "receipt_sha256": receipt_sha256,
    }


def publish_private_torrent_acquisition_audit(
    receipt: dict[str, Any], output_path: Path, *, protected_payload_root: Path
) -> dict[str, Any]:
    """Atomically write one private receipt outside the acquisition payload tree."""

    requested = Path(output_path)
    if not requested.is_absolute() or requested.name in {"", ".", ".."}:
        _fail("private acquisition-audit output must be an absolute file path")
    try:
        parent = requested.parent.resolve(strict=True)
        protected = Path(protected_payload_root).resolve(strict=True)
    except OSError as error:
        raise TorrentAcquisitionAuditError("private audit output cannot be resolved") from error
    if parent != requested.parent or not parent.is_dir():
        _fail("private acquisition-audit output parent must be a real directory")
    try:
        parent.relative_to(protected)
    except ValueError:
        pass
    else:
        _fail("private acquisition-audit output must be outside the payload tree")
    destination = parent / requested.name
    try:
        destination.lstat()
    except FileNotFoundError:
        pass
    except OSError as error:
        raise TorrentAcquisitionAuditError("private audit output cannot be inspected") from error
    else:
        _fail("private acquisition-audit output already exists")

    body = (json.dumps(receipt, ensure_ascii=False, sort_keys=True, indent=2) + "\n").encode(
        "utf-8"
    )
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
                _fail("private acquisition-audit output write made no progress")
            offset += written
        os.fsync(descriptor)
        os.close(descriptor)
        descriptor = -1
        try:
            os.link(temporary, destination, follow_symlinks=False)
        except FileExistsError as error:
            raise TorrentAcquisitionAuditError(
                "private acquisition-audit output already exists"
            ) from error
        directory_descriptor = os.open(parent, os.O_RDONLY | getattr(os, "O_DIRECTORY", 0))
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
        "audit_receipt_written": True,
        "receipt_id": receipt["receipt_id"],
        "receipt_sha256": receipt["receipt_sha256"],
        "output_byte_count": len(body),
        "output_sha256": sha256_bytes(body),
        "output_mode": "0400",
        "path_disclosed": False,
        "network_actions_performed": False,
        "torrent_client_invoked": False,
        "payload_bytes_read": 0,
    }


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=(
            "Audit one stable selective aria2 acquisition snapshot without invoking "
            "a client, opening a socket, or reading payload bytes"
        )
    )
    parser.add_argument("--plan", required=True, type=Path)
    parser.add_argument("--selector-receipt", required=True, type=Path)
    parser.add_argument("--torrent", required=True, type=Path)
    parser.add_argument("--session", required=True, type=Path)
    parser.add_argument("--control", required=True, type=Path)
    parser.add_argument("--payload-root", required=True, type=Path)
    parser.add_argument("--observed-at", required=True)
    parser.add_argument(
        "--output",
        type=Path,
        help="atomically write the path-free private receipt to this absolute new path",
    )
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    try:
        receipt = build_torrent_acquisition_audit(
            plan_path=args.plan,
            selector_receipt_path=args.selector_receipt,
            torrent_path=args.torrent,
            session_path=args.session,
            control_path=args.control,
            payload_root=args.payload_root,
            observed_at=args.observed_at,
        )
        if args.output:
            receipt = publish_private_torrent_acquisition_audit(
                receipt, args.output, protected_payload_root=args.payload_root
            )
    except TorrentAcquisitionAuditError as error:
        raise SystemExit(f"torrent acquisition audit refused: {error}") from error
    print(json.dumps(receipt, ensure_ascii=False, sort_keys=True, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
