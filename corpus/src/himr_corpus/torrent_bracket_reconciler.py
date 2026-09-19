"""Private review candidates from terminal YouTube IDs in a torrent manifest.

This module is deliberately narrower than the original torrent importer.  It
replays the exact torrent and reviewed discovery bytes, proves that their complete
file tree is already represented by one completed catalog import, and considers
only four explicitly reviewed top-level directories.  A parsed ID is a locator
hint: it never creates a source relation, recording relation, merge, identity,
claim, event, or publication decision.
"""

from __future__ import annotations

import base64
import hashlib
import json
import math
import os
import re
import sqlite3
import stat
import unicodedata
from collections import Counter
from datetime import datetime
from pathlib import Path
from typing import Any
from urllib.parse import urlsplit

from .db import transaction
from .ids import recording_id, source_id, stable_id
from .importers import _begin_batch, _complete_batch, canonical_json, sha256_bytes


IMPORTER_NAME = "torrent_bracket_reconciliation_v1"
PLAN_KIND = "torrent_bracket_youtube_reconciliation_review_plan"
SCOPED_DIRECTORY_LABELS = (
    "YouTube Videos",
    "Old YouTube Livestreams",
    "New YouTube Livestreams",
    "New New YouTube Livestreams",
)
SCOPED_DIRECTORY_BYTES = {value.encode("ascii"): value for value in SCOPED_DIRECTORY_LABELS}
VIDEO_EXTENSIONS = (b"mp4", b"webm", b"ogv", b"mkv", b"mov", b"m4v")
TERMINAL_BRACKET_BYTES_RE = re.compile(
    rb"\[([A-Za-z0-9_-]{11})\]\.((?:mp4|webm|ogv|mkv|mov|m4v))$", re.IGNORECASE
)
YOUTUBE_ID_RE = re.compile(r"^[A-Za-z0-9_-]{11}$")
SHA256_RE = re.compile(r"^[0-9a-f]{64}$")
SHA1_RE = re.compile(r"^[0-9a-f]{40}$")
RFC3339_UTC_RE = re.compile(r"^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}Z$")

MAX_TORRENT_BYTES = 32 * 1024 * 1024
MAX_DISCOVERY_BYTES = 1024 * 1024
MAX_TORRENT_FILES = 100_000
MAX_TORRENT_NODES = 500_000
MAX_BENCODE_DEPTH = 24
MAX_PATH_COMPONENTS = 32
MAX_PATH_COMPONENT_BYTES = 4096
MAX_MANIFEST_PATH_CHARACTERS = 16_384
MAX_SCOPED_FILES = 50_000
MAX_CANDIDATES = 50_000
MAX_YOUTUBE_MAPPINGS = 100

ALLOWED_WRITE_TABLES = frozenset(
    {
        "import_batches",
        "import_observations",
        "match_candidates",
        "review_tasks",
        "torrent_bracket_reconciliation_imports",
        "torrent_bracket_youtube_candidates",
    }
)


class TorrentBracketReconciliationError(ValueError):
    pass


def _expect_keys(value: dict[str, Any], required: set[str], label: str) -> None:
    if set(value) != required:
        missing = sorted(required - set(value))
        extra = sorted(set(value) - required)
        raise TorrentBracketReconciliationError(
            f"{label} has unknown shape (missing={missing}, extra={extra})"
        )


def _bounded_text(value: object, label: str, *, maximum: int = 16_384) -> str:
    if (
        not isinstance(value, str)
        or not value
        or len(value) > maximum
        or "\x00" in value
    ):
        raise TorrentBracketReconciliationError(f"{label} must be bounded non-empty text")
    return value


def _bounded_int(value: object, label: str, *, maximum: int = 2**63 - 1) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value < 0 or value > maximum:
        raise TorrentBracketReconciliationError(f"{label} must be a bounded non-negative integer")
    return value


def _strict_json_bytes(body: bytes, label: str) -> dict[str, Any]:
    def pairs(values: list[tuple[str, Any]]) -> dict[str, Any]:
        result: dict[str, Any] = {}
        for key, value in values:
            if key in result:
                raise TorrentBracketReconciliationError(f"{label} has duplicate key {key!r}")
            result[key] = value
        return result

    def integer(value: str) -> int:
        if len(value) > 19:
            raise TorrentBracketReconciliationError(f"{label} has an oversized integer")
        return int(value)

    def invalid_constant(value: str) -> None:
        raise TorrentBracketReconciliationError(f"{label} has invalid number {value}")

    def invalid_float(value: str) -> None:
        raise TorrentBracketReconciliationError(f"{label} does not permit floating-point values")

    try:
        decoded = body.decode("utf-8")
        value = json.loads(
            decoded,
            object_pairs_hook=pairs,
            parse_int=integer,
            parse_float=invalid_float,
            parse_constant=invalid_constant,
        )
    except TorrentBracketReconciliationError:
        raise
    except (UnicodeDecodeError, json.JSONDecodeError, ValueError, RecursionError) as error:
        raise TorrentBracketReconciliationError(f"{label} is not strict UTF-8 JSON") from error
    if not isinstance(value, dict):
        raise TorrentBracketReconciliationError(f"{label} must contain one object")
    return value


def _stable_file(path: Path, maximum: int, label: str) -> tuple[Path, bytes]:
    # Keep the lexical final entry throughout the read.  Resolving after lstat
    # creates a swap window in which a regular-file entry can be replaced by a
    # symlink before open().  O_NOFOLLOW plus before/open/after identity checks
    # instead prove that the descriptor and the caller-named entry are one stable
    # regular file.  Parent-directory symlinks are allowed, but swapping one to a
    # different inode is caught by the same identity comparison.
    requested = Path(os.path.abspath(os.fspath(Path(path))))
    try:
        requested_stat = requested.lstat()
    except OSError as error:
        raise TorrentBracketReconciliationError(f"{label} cannot be inspected") from error
    if stat.S_ISLNK(requested_stat.st_mode):
        raise TorrentBracketReconciliationError(f"{label} must not be a symlink")
    if not stat.S_ISREG(requested_stat.st_mode):
        raise TorrentBracketReconciliationError(f"{label} must be a regular file")
    try:
        flags = (
            os.O_RDONLY
            | getattr(os, "O_CLOEXEC", 0)
            | getattr(os, "O_NOFOLLOW", 0)
            | getattr(os, "O_NONBLOCK", 0)
        )
        descriptor = os.open(requested, flags)
    except OSError as error:
        raise TorrentBracketReconciliationError(f"{label} cannot be opened safely") from error
    try:
        before = os.fstat(descriptor)
        if not stat.S_ISREG(before.st_mode):
            raise TorrentBracketReconciliationError(f"{label} must be a regular file")
        if before.st_size < 1 or before.st_size > maximum:
            raise TorrentBracketReconciliationError(f"{label} exceeds its byte limit")
        chunks: list[bytes] = []
        offset = 0
        while offset < before.st_size:
            chunk = os.pread(descriptor, min(1024 * 1024, before.st_size - offset), offset)
            if not chunk:
                raise TorrentBracketReconciliationError(f"{label} ended during stable read")
            chunks.append(chunk)
            offset += len(chunk)
        after = os.fstat(descriptor)
        try:
            path_after = requested.lstat()
        except OSError as error:
            raise TorrentBracketReconciliationError(
                f"{label} changed during stable read"
            ) from error

        def fingerprint(value: os.stat_result) -> tuple[int, int, int, int, int]:
            return (
                value.st_dev,
                value.st_ino,
                value.st_size,
                value.st_mtime_ns,
                value.st_ctime_ns,
            )

        if (
            stat.S_ISLNK(path_after.st_mode)
            or fingerprint(requested_stat) != fingerprint(before)
            or fingerprint(before) != fingerprint(after)
            or fingerprint(after) != fingerprint(path_after)
        ):
            raise TorrentBracketReconciliationError(f"{label} changed during stable read")
        return requested, b"".join(chunks)
    finally:
        os.close(descriptor)


class _StrictBencodeParser:
    """Bounded canonical bencode parser retaining the exact info-dictionary span."""

    def __init__(self, data: bytes):
        self.data = data
        self.nodes = 0

    def _node(self) -> None:
        self.nodes += 1
        if self.nodes > MAX_TORRENT_NODES:
            raise TorrentBracketReconciliationError("torrent exceeds the bencode node cap")

    def _terminator(self, marker: bytes, start: int, label: str) -> int:
        end = self.data.find(marker, start)
        if end < 0:
            raise TorrentBracketReconciliationError(f"unterminated torrent {label}")
        return end

    def parse(self, index: int, depth: int = 0) -> tuple[Any, int]:
        if depth > MAX_BENCODE_DEPTH:
            raise TorrentBracketReconciliationError("torrent exceeds bencode depth cap")
        if index >= len(self.data):
            raise TorrentBracketReconciliationError("torrent ended inside a value")
        self._node()
        marker = self.data[index : index + 1]
        if marker == b"i":
            end = self._terminator(b"e", index + 1, "integer")
            token = self.data[index + 1 : end]
            if not re.fullmatch(rb"(?:0|-[1-9][0-9]*|[1-9][0-9]*)", token):
                raise TorrentBracketReconciliationError("torrent integer is not canonical")
            if len(token.lstrip(b"-")) > 19:
                raise TorrentBracketReconciliationError("torrent integer is oversized")
            return int(token), end + 1
        if marker.isdigit():
            colon = self._terminator(b":", index, "byte-string length")
            token = self.data[index:colon]
            if not re.fullmatch(rb"(?:0|[1-9][0-9]*)", token) or len(token) > 9:
                raise TorrentBracketReconciliationError("torrent byte-string length is not canonical")
            length = int(token)
            start = colon + 1
            end = start + length
            if end > len(self.data):
                raise TorrentBracketReconciliationError("torrent byte string exceeds input")
            return self.data[start:end], end
        if marker == b"l":
            values: list[Any] = []
            index += 1
            while True:
                if index >= len(self.data):
                    raise TorrentBracketReconciliationError("unterminated torrent list")
                if self.data[index : index + 1] == b"e":
                    return values, index + 1
                value, index = self.parse(index, depth + 1)
                values.append(value)
                if len(values) > MAX_TORRENT_NODES:
                    raise TorrentBracketReconciliationError("torrent list exceeds item cap")
        if marker == b"d":
            values: dict[bytes, Any] = {}
            previous: bytes | None = None
            index += 1
            while True:
                if index >= len(self.data):
                    raise TorrentBracketReconciliationError("unterminated torrent dictionary")
                if self.data[index : index + 1] == b"e":
                    return values, index + 1
                key, index = self.parse(index, depth + 1)
                if not isinstance(key, bytes) or len(key) > 1024:
                    raise TorrentBracketReconciliationError("torrent dictionary key is invalid")
                if previous is not None and key <= previous:
                    raise TorrentBracketReconciliationError(
                        "torrent dictionary keys are duplicate or noncanonical"
                    )
                previous = key
                value, index = self.parse(index, depth + 1)
                values[key] = value
        raise TorrentBracketReconciliationError(f"unsupported torrent marker at byte {index}")

    def root_with_info_span(self) -> tuple[dict[bytes, Any], tuple[int, int]]:
        if self.data[:1] != b"d":
            raise TorrentBracketReconciliationError("torrent root must be a dictionary")
        self._node()
        result: dict[bytes, Any] = {}
        previous: bytes | None = None
        info_span: tuple[int, int] | None = None
        index = 1
        while True:
            if index >= len(self.data):
                raise TorrentBracketReconciliationError("unterminated torrent root")
            if self.data[index : index + 1] == b"e":
                index += 1
                break
            key, index = self.parse(index, 1)
            if not isinstance(key, bytes) or len(key) > 1024:
                raise TorrentBracketReconciliationError("torrent root key is invalid")
            if previous is not None and key <= previous:
                raise TorrentBracketReconciliationError(
                    "torrent root keys are duplicate or noncanonical"
                )
            previous = key
            value_start = index
            value, index = self.parse(index, 1)
            if key == b"info":
                info_span = (value_start, index)
            result[key] = value
        if index != len(self.data):
            raise TorrentBracketReconciliationError("torrent has trailing bytes")
        if info_span is None:
            raise TorrentBracketReconciliationError("torrent has no info dictionary")
        return result, info_span


def _torrent_text(value: bytes) -> str:
    """Reproduce the original importer's deterministic display-path decoding."""

    return value.decode("utf-8", errors="replace")


def _raw_path_sha256(components: tuple[bytes, ...]) -> str:
    digest = hashlib.sha256()
    for component in components:
        digest.update(len(component).to_bytes(4, "big"))
        digest.update(component)
    return digest.hexdigest()


def terminal_bracketed_youtube_id_bytes(filename: object) -> str | None:
    """Parse only ``[11-char-ID].<recognized-video-extension>`` at byte end."""

    if not isinstance(filename, bytes) or not filename or len(filename) > MAX_PATH_COMPONENT_BYTES:
        return None
    match = TERMINAL_BRACKET_BYTES_RE.search(filename)
    return match.group(1).decode("ascii") if match else None


def _parse_torrent(body: bytes) -> dict[str, Any]:
    root, info_span = _StrictBencodeParser(body).root_with_info_span()
    allowed_root = {
        b"announce",
        b"announce-list",
        b"comment",
        b"created by",
        b"creation date",
        b"encoding",
        b"info",
    }
    if b"info" not in root or not set(root) <= allowed_root:
        raise TorrentBracketReconciliationError("torrent root has unknown shape")
    for key in (b"announce", b"comment", b"created by", b"encoding"):
        if key in root and not isinstance(root[key], bytes):
            raise TorrentBracketReconciliationError("torrent root metadata has unknown shape")
    if b"creation date" in root and (
        isinstance(root[b"creation date"], bool) or not isinstance(root[b"creation date"], int)
    ):
        raise TorrentBracketReconciliationError("torrent creation date has unknown shape")
    if b"announce-list" in root:
        announce_list = root[b"announce-list"]
        if (
            not isinstance(announce_list, list)
            or len(announce_list) > 100
            or any(
                not isinstance(tier, list)
                or len(tier) > 100
                or any(not isinstance(url, bytes) for url in tier)
                for tier in announce_list
            )
        ):
            raise TorrentBracketReconciliationError("torrent announce-list has unknown shape")

    info = root[b"info"]
    if not isinstance(info, dict) or set(info) != {b"files", b"name", b"piece length", b"pieces"}:
        raise TorrentBracketReconciliationError("torrent info dictionary has unknown shape")
    name = info[b"name"]
    files_value = info[b"files"]
    piece_length = info[b"piece length"]
    pieces = info[b"pieces"]
    if not isinstance(name, bytes) or not name or len(name) > 4096:
        raise TorrentBracketReconciliationError("torrent root name is invalid")
    try:
        root_name = name.decode("utf-8")
    except UnicodeDecodeError as error:
        raise TorrentBracketReconciliationError("torrent root name is not UTF-8") from error
    if unicodedata.normalize("NFC", root_name) != root_name:
        raise TorrentBracketReconciliationError("torrent root name is not NFC")
    if (
        isinstance(piece_length, bool)
        or not isinstance(piece_length, int)
        or piece_length < 1
        or piece_length > 128 * 1024 * 1024
    ):
        raise TorrentBracketReconciliationError("torrent piece length is invalid")
    if not isinstance(pieces, bytes):
        raise TorrentBracketReconciliationError("torrent pieces field is invalid")
    if not isinstance(files_value, list) or not files_value or len(files_value) > MAX_TORRENT_FILES:
        raise TorrentBracketReconciliationError("torrent file list is invalid or oversized")

    files: list[dict[str, Any]] = []
    raw_paths: set[tuple[bytes, ...]] = set()
    display_paths: set[str] = set()
    total_bytes = 0
    for index, file_record in enumerate(files_value):
        if not isinstance(file_record, dict) or set(file_record) != {b"length", b"path"}:
            raise TorrentBracketReconciliationError("torrent file record has unknown shape")
        length = _bounded_int(file_record[b"length"], "torrent file length")
        path_value = file_record[b"path"]
        if (
            not isinstance(path_value, list)
            or not path_value
            or len(path_value) > MAX_PATH_COMPONENTS
        ):
            raise TorrentBracketReconciliationError("torrent file path has unknown shape")
        components: list[bytes] = []
        for component in path_value:
            if (
                not isinstance(component, bytes)
                or not component
                or len(component) > MAX_PATH_COMPONENT_BYTES
                or component in (b".", b"..")
                or b"\x00" in component
                or b"/" in component
                or b"\\" in component
            ):
                raise TorrentBracketReconciliationError("torrent path component is unsafe")
            components.append(component)
        raw_path = tuple(components)
        display_path = "/".join(_torrent_text(component) for component in raw_path)
        if len(display_path) > MAX_MANIFEST_PATH_CHARACTERS:
            raise TorrentBracketReconciliationError(
                "torrent decoded file path exceeds catalog contract"
            )
        if raw_path in raw_paths or display_path in display_paths:
            raise TorrentBracketReconciliationError("torrent file path is duplicated or decoding-collides")
        raw_paths.add(raw_path)
        display_paths.add(display_path)
        total_bytes += length
        if total_bytes > 2**63 - 1:
            raise TorrentBracketReconciliationError("torrent byte total is oversized")
        files.append(
            {
                "file_index": index,
                "raw_components": raw_path,
                "manifest_path": display_path,
                "byte_count": length,
            }
        )
    expected_piece_bytes = math.ceil(total_bytes / piece_length) * 20 if total_bytes else 0
    if len(pieces) != expected_piece_bytes:
        raise TorrentBracketReconciliationError("torrent pieces length disagrees with file bytes")
    return {
        "info_hash_sha1": hashlib.sha1(body[info_span[0] : info_span[1]]).hexdigest(),
        "torrent_sha256": sha256_bytes(body),
        "root_name": root_name,
        "piece_length_bytes": piece_length,
        "file_count": len(files),
        "total_bytes": total_bytes,
        "files": files,
    }


def _https_url(value: object, label: str) -> str:
    text = _bounded_text(value, label, maximum=4096)
    if any(
        character.isspace() or unicodedata.category(character) == "Cc"
        for character in text
    ):
        raise TorrentBracketReconciliationError(f"{label} contains URL whitespace or control text")
    try:
        parsed = urlsplit(text)
        hostname = parsed.hostname
        parsed.port
    except ValueError as error:
        raise TorrentBracketReconciliationError(f"{label} is not a valid HTTPS URL") from error
    if (
        parsed.scheme != "https"
        or not hostname
        or parsed.username is not None
        or parsed.password is not None
        or parsed.fragment
    ):
        raise TorrentBracketReconciliationError(f"{label} must be a credential-free HTTPS URL")
    return text


def _timestamp(value: object, label: str) -> str:
    text = _bounded_text(value, label, maximum=32)
    if not RFC3339_UTC_RE.fullmatch(text):
        raise TorrentBracketReconciliationError(f"{label} must be whole-second UTC RFC3339")
    try:
        datetime.fromisoformat(text.replace("Z", "+00:00"))
    except ValueError as error:
        raise TorrentBracketReconciliationError(f"{label} is not a real timestamp") from error
    return text


def _top_level(files: list[dict[str, Any]]) -> dict[str, dict[str, int]]:
    result: dict[str, dict[str, int]] = {}
    for file_record in files:
        components = file_record["raw_components"]
        label = _torrent_text(components[0])
        row = result.setdefault(label, {"files": 0, "bytes": 0})
        row["files"] += 1
        row["bytes"] += file_record["byte_count"]
    return result


def _validate_discovery(
    body: bytes, *, discovery_path: Path, torrent_path: Path, torrent: dict[str, Any]
) -> dict[str, Any]:
    value = _strict_json_bytes(body, "torrent discovery metadata")
    _expect_keys(
        value,
        {
            "access",
            "canonical_url",
            "discovery_method",
            "interpretation_warning",
            "observed_at",
            "platform",
            "post_claims_unverified",
            "post_id",
            "schema_version",
            "source_id",
            "status",
            "subreddit",
            "title_label",
            "torrent",
        },
        "torrent discovery metadata",
    )
    if value["schema_version"] != 1 or value["platform"] != "reddit":
        raise TorrentBracketReconciliationError("torrent discovery identity is unsupported")
    post_id = _bounded_text(value["post_id"], "discovery post_id", maximum=32)
    subreddit = _bounded_text(value["subreddit"], "discovery subreddit", maximum=64)
    if not re.fullmatch(r"[A-Za-z0-9_]+", post_id) or not re.fullmatch(
        r"[A-Za-z0-9_]+", subreddit
    ):
        raise TorrentBracketReconciliationError("discovery Reddit identifiers are invalid")
    if value["source_id"] != f"reddit-post-{post_id}":
        raise TorrentBracketReconciliationError("discovery source_id disagrees with post_id")
    if value["status"] != "metadata_only_not_downloaded":
        raise TorrentBracketReconciliationError("discovery is not metadata-only")
    observed_at = _timestamp(value["observed_at"], "discovery observed_at")
    canonical_url = _https_url(value["canonical_url"], "discovery canonical_url")
    parsed_reddit = urlsplit(canonical_url)
    if parsed_reddit.hostname not in {"reddit.com", "www.reddit.com"} or (
        f"/comments/{post_id}/" not in parsed_reddit.path
    ):
        raise TorrentBracketReconciliationError("discovery canonical URL disagrees with post_id")
    _bounded_text(value["discovery_method"], "discovery method", maximum=256)
    _bounded_text(value["interpretation_warning"], "interpretation warning")
    _bounded_text(value["title_label"], "discovery title", maximum=512)

    access = value["access"]
    if not isinstance(access, dict):
        raise TorrentBracketReconciliationError("discovery access has unknown shape")
    _expect_keys(
        access,
        {"download_attempted", "publication_state", "reason", "rights_state"},
        "discovery access",
    )
    if (
        access["download_attempted"] is not False
        or access["publication_state"] != "research_lead_only"
        or access["rights_state"] != "unknown"
    ):
        raise TorrentBracketReconciliationError("discovery access policy is not candidate-only")
    _bounded_text(access["reason"], "discovery access reason")

    claims = value["post_claims_unverified"]
    if not isinstance(claims, dict):
        raise TorrentBracketReconciliationError("discovery post claims have unknown shape")
    _expect_keys(claims, {"described_contents", "nominal_size_label"}, "discovery post claims")
    described = claims["described_contents"]
    if (
        not isinstance(described, list)
        or len(described) > 64
        or any(not isinstance(item, str) or not item or len(item) > 256 for item in described)
    ):
        raise TorrentBracketReconciliationError("discovery described_contents is invalid")
    _bounded_text(claims["nominal_size_label"], "discovery nominal size", maximum=128)

    torrent_value = value["torrent"]
    if not isinstance(torrent_value, dict):
        raise TorrentBracketReconciliationError("discovery torrent has unknown shape")
    _expect_keys(
        torrent_value,
        {
            "file_count",
            "info_hash_sha1",
            "local_review_filename",
            "magnet_info_hash_matched",
            "piece_length_bytes",
            "root_name",
            "top_level",
            "torrent_sha256",
            "total_bytes",
            "url",
        },
        "discovery torrent",
    )
    info_hash = _bounded_text(torrent_value["info_hash_sha1"], "discovery info hash", maximum=40).lower()
    torrent_sha = _bounded_text(torrent_value["torrent_sha256"], "discovery torrent SHA-256", maximum=64)
    if not SHA1_RE.fullmatch(info_hash) or not SHA256_RE.fullmatch(torrent_sha):
        raise TorrentBracketReconciliationError("discovery torrent digest is invalid")
    if info_hash != torrent["info_hash_sha1"] or torrent_sha != torrent["torrent_sha256"]:
        raise TorrentBracketReconciliationError("discovery digests differ from exact torrent bytes")
    comparisons = {
        "file_count": torrent["file_count"],
        "piece_length_bytes": torrent["piece_length_bytes"],
        "total_bytes": torrent["total_bytes"],
    }
    for key, expected in comparisons.items():
        if _bounded_int(torrent_value[key], f"discovery torrent {key}") != expected:
            raise TorrentBracketReconciliationError(f"discovery torrent {key} differs")
    if torrent_value["root_name"] != torrent["root_name"]:
        raise TorrentBracketReconciliationError("discovery torrent root name differs")
    if torrent_value["local_review_filename"] != torrent_path.name:
        raise TorrentBracketReconciliationError("discovery local torrent filename differs")
    if torrent_value["magnet_info_hash_matched"] is not True:
        raise TorrentBracketReconciliationError("discovery did not match the magnet info hash")
    torrent_url = _https_url(torrent_value["url"], "discovery torrent URL")

    declared_top = torrent_value["top_level"]
    if not isinstance(declared_top, dict) or len(declared_top) > 64:
        raise TorrentBracketReconciliationError("discovery top_level has unknown shape")
    actual_top = _top_level(torrent["files"])
    for label, row in declared_top.items():
        _bounded_text(label, "discovery top-level label", maximum=256)
        if not isinstance(row, dict):
            raise TorrentBracketReconciliationError("discovery top-level row has unknown shape")
        _expect_keys(row, {"bytes", "files"}, f"discovery top-level row {label!r}")
        if label not in actual_top or {
            "files": _bounded_int(row["files"], "top-level file count"),
            "bytes": _bounded_int(row["bytes"], "top-level byte count"),
        } != actual_top[label]:
            raise TorrentBracketReconciliationError(
                f"discovery top-level summary differs for {label!r}"
            )
    if any(label not in declared_top for label in SCOPED_DIRECTORY_LABELS):
        raise TorrentBracketReconciliationError("discovery omits a reviewed scope directory")
    return {
        "discovery_sha256": sha256_bytes(body),
        "discovery_byte_count": len(body),
        "discovery_filename": discovery_path.name,
        "observed_at": observed_at,
        "post_id": post_id,
        "subreddit": subreddit,
        "canonical_url": canonical_url,
        "torrent_url": torrent_url,
    }


def _combined_import_digest(
    torrent_path: Path, torrent_body: bytes, discovery_path: Path, discovery_body: bytes
) -> str:
    digest = hashlib.sha256()
    inputs = [(torrent_path.name, torrent_body), (discovery_path.name, discovery_body)]
    for name, body in sorted(inputs, key=lambda value: value[0]):
        digest.update(name.encode("utf-8"))
        digest.update(b"\x00")
        digest.update(len(body).to_bytes(8, "big"))
        digest.update(body)
    return digest.hexdigest()


def _json_database(value: object, label: str) -> dict[str, Any]:
    if not isinstance(value, str):
        raise TorrentBracketReconciliationError(f"{label} is not JSON text")
    return _strict_json_bytes(value.encode("utf-8"), label)


def _catalog_binding(
    connection: sqlite3.Connection,
    *,
    torrent: dict[str, Any],
    discovery: dict[str, Any],
    combined_input_sha256: str,
    torrent_filename: str,
) -> dict[str, Any]:
    info_hash = torrent["info_hash_sha1"]
    manifest_source_id = source_id("bittorrent", "torrent_manifest", info_hash)
    source = connection.execute(
        """
        SELECT source_id, platform, source_kind, native_id, parent_source_id,
               created_by_import_batch_id
        FROM sources WHERE source_id = ?
        """,
        (manifest_source_id,),
    ).fetchone()
    if source is None:
        raise TorrentBracketReconciliationError("exact torrent manifest source is missing")
    if tuple(source)[:5] != (
        manifest_source_id,
        "bittorrent",
        "torrent_manifest",
        info_hash,
        None,
    ):
        raise TorrentBracketReconciliationError("exact torrent manifest source differs")
    torrent_import_batch_id = source["created_by_import_batch_id"]
    if not isinstance(torrent_import_batch_id, str):
        raise TorrentBracketReconciliationError("torrent source has no origin import")
    batch = connection.execute(
        """
        SELECT importer_name, importer_version, input_sha256, source_snapshot_date,
               started_at, completed_at, status, statistics_json
        FROM import_batches WHERE import_batch_id = ?
        """,
        (torrent_import_batch_id,),
    ).fetchone()
    if (
        batch is None
        or batch["importer_name"] != "torrent_manifest_metadata"
        or batch["input_sha256"] != combined_input_sha256
        or batch["status"] != "completed"
        or batch["started_at"] != discovery["observed_at"]
        or batch["completed_at"] != discovery["observed_at"]
        or batch["source_snapshot_date"] != discovery["observed_at"][:10]
        or not isinstance(batch["importer_version"], str)
        or not batch["importer_version"]
    ):
        raise TorrentBracketReconciliationError(
            "exact inputs are not bound to one completed torrent-manifest import"
        )
    torrent_importer_version = _bounded_text(
        batch["importer_version"], "torrent importer version", maximum=64
    )
    expected_statistics = {
        "info_hash_sha1": info_hash,
        "torrent_sha256": torrent["torrent_sha256"],
        "file_count": torrent["file_count"],
        "total_bytes": torrent["total_bytes"],
        "payload_files_downloaded": 0,
    }
    if _json_database(batch["statistics_json"], "torrent import statistics") != expected_statistics:
        raise TorrentBracketReconciliationError("torrent import statistics differ from exact inputs")
    manifest_metadata = {
        "info_hash_sha1": info_hash,
        "torrent_sha256": torrent["torrent_sha256"],
        "root_name": torrent["root_name"],
        "piece_length_bytes": torrent["piece_length_bytes"],
        "file_count": torrent["file_count"],
        "total_bytes": torrent["total_bytes"],
        "payload_downloaded": False,
    }
    observations = connection.execute(
        """
        SELECT parent_source_id, canonical_url, historical_url, title, published_at,
               observed_at, quality_rank, quality_basis, access_state, review_state,
               metadata_json
        FROM source_metadata_observations
        WHERE source_id = ? AND import_batch_id = ?
        """,
        (manifest_source_id, torrent_import_batch_id),
    ).fetchall()
    expected_manifest_observation = (
        None,
        discovery["torrent_url"],
        None,
        torrent["root_name"],
        None,
        discovery["observed_at"],
        250,
        "torrent_manifest_metadata: locally parsed discovery manifest",
        "public",
        "unreviewed",
        canonical_json(manifest_metadata),
    )
    if len(observations) != 1 or tuple(observations[0]) != expected_manifest_observation:
        raise TorrentBracketReconciliationError("torrent manifest origin observation differs")
    snapshots = connection.execute(
        """
        SELECT observed_at, request_url, final_url, http_status, payload_sha256,
               artifact_path, metadata_json, import_batch_id
        FROM source_snapshots WHERE source_id = ? AND import_batch_id = ?
        """,
        (manifest_source_id, torrent_import_batch_id),
    ).fetchall()
    expected_payload_sha = sha256_bytes(canonical_json(manifest_metadata).encode("utf-8"))
    if len(snapshots) != 1:
        raise TorrentBracketReconciliationError("torrent manifest origin snapshot differs")
    snapshot = snapshots[0]
    if (
        snapshot["observed_at"] != discovery["observed_at"]
        or snapshot["request_url"] != discovery["torrent_url"]
        or snapshot["final_url"] != discovery["torrent_url"]
        or snapshot["http_status"] is not None
        or snapshot["payload_sha256"] != expected_payload_sha
        or not isinstance(snapshot["artifact_path"], str)
        or Path(snapshot["artifact_path"]).name != torrent_filename
        or snapshot["metadata_json"] != "{}"
        or snapshot["import_batch_id"] != torrent_import_batch_id
    ):
        raise TorrentBracketReconciliationError("torrent manifest origin snapshot differs")
    external = connection.execute(
        """
        SELECT object_type, object_id, namespace, external_value, confidence_state,
               basis, source_id
        FROM external_ids
        WHERE object_type = 'source' AND object_id = ?
          AND namespace = 'bittorrent_info_hash_sha1'
        """,
        (manifest_source_id,),
    ).fetchall()
    if len(external) != 1 or tuple(external[0]) != (
        "source",
        manifest_source_id,
        "bittorrent_info_hash_sha1",
        info_hash,
        "metadata_only",
        "SHA-1 of exact bencoded info dictionary",
        manifest_source_id,
    ):
        raise TorrentBracketReconciliationError("torrent info-hash provenance differs")

    child_rows = connection.execute(
        """
        SELECT source_id, platform, source_kind, native_id, parent_source_id,
               created_by_import_batch_id
        FROM sources WHERE parent_source_id = ? ORDER BY source_id
        """,
        (manifest_source_id,),
    ).fetchall()
    if len(child_rows) != torrent["file_count"]:
        raise TorrentBracketReconciliationError("torrent catalog child count differs")
    children = {row["native_id"]: row for row in child_rows}
    if len(children) != len(child_rows):
        raise TorrentBracketReconciliationError("torrent catalog child native IDs collide")
    origin_rows = connection.execute(
        """
        SELECT observation.source_id, observation.parent_source_id,
               observation.canonical_url, observation.historical_url,
               observation.title, observation.published_at, observation.observed_at,
               observation.quality_rank, observation.quality_basis,
               observation.access_state, observation.review_state,
               observation.metadata_json
        FROM source_metadata_observations AS observation
        JOIN sources AS source ON source.source_id = observation.source_id
        WHERE source.parent_source_id = ? AND observation.import_batch_id = ?
        ORDER BY observation.source_id
        """,
        (manifest_source_id, torrent_import_batch_id),
    ).fetchall()
    if len(origin_rows) != torrent["file_count"]:
        raise TorrentBracketReconciliationError("torrent file origin observations differ")
    origin_by_source = {row["source_id"]: row for row in origin_rows}
    if len(origin_by_source) != len(origin_rows):
        raise TorrentBracketReconciliationError("torrent file origin observations are ambiguous")
    file_source_ids: dict[int, str] = {}
    for file_record in torrent["files"]:
        manifest_path = file_record["manifest_path"]
        native_id = f"{info_hash}/{manifest_path}"
        expected_source_id = source_id("bittorrent", "torrent_file_candidate", native_id)
        row = children.get(native_id)
        if row is None or tuple(row) != (
            expected_source_id,
            "bittorrent",
            "torrent_file_candidate",
            native_id,
            manifest_source_id,
            torrent_import_batch_id,
        ):
            raise TorrentBracketReconciliationError(
                f"torrent file projection differs at index {file_record['file_index']}"
            )
        metadata = {
            "manifest_path": manifest_path,
            "byte_count": file_record["byte_count"],
            "discovery_state": "private_manifest_candidate",
            "payload_downloaded": False,
        }
        origin = origin_by_source.get(expected_source_id)
        expected_origin = (
            expected_source_id,
            manifest_source_id,
            None,
            None,
            manifest_path.rsplit("/", 1)[-1] or manifest_path,
            None,
            discovery["observed_at"],
            250,
            "torrent_manifest_metadata: locally parsed discovery manifest",
            "unknown",
            "unreviewed",
            canonical_json(metadata),
        )
        if origin is None or tuple(origin) != expected_origin:
            raise TorrentBracketReconciliationError(
                f"torrent file origin evidence differs at index {file_record['file_index']}"
            )
        file_source_ids[file_record["file_index"]] = expected_source_id
    return {
        "torrent_manifest_source_id": manifest_source_id,
        "torrent_import_batch_id": torrent_import_batch_id,
        "torrent_importer_version": torrent_importer_version,
        "torrent_file_source_ids": file_source_ids,
    }


def _youtube_resolution(connection: sqlite3.Connection, video_id: str) -> dict[str, Any]:
    expected_source = source_id("youtube", "youtube_video", video_id)
    source = connection.execute(
        """
        SELECT source_id, platform, source_kind, native_id
        FROM sources
        WHERE platform = 'youtube' AND source_kind = 'youtube_video' AND native_id = ?
        """,
        (video_id,),
    ).fetchone()
    if source is None:
        return {
            "resolution_state": "missing_native_source",
            "youtube_source_id": None,
            "youtube_recording_id": None,
            "mapped_recording_ids": [],
        }
    if tuple(source) != (expected_source, "youtube", "youtube_video", video_id):
        raise TorrentBracketReconciliationError(
            f"deterministic YouTube source identity collision for {video_id}"
        )
    mapped = [
        row["recording_id"]
        for row in connection.execute(
            "SELECT DISTINCT recording_id FROM recording_sources WHERE source_id = ? ORDER BY recording_id",
            (expected_source,),
        ).fetchall()
    ]
    if len(mapped) > MAX_YOUTUBE_MAPPINGS:
        raise TorrentBracketReconciliationError("YouTube source exceeds mapping safety cap")
    if any(not re.fullmatch(r"[a-z]+_[0-9a-f]{32}", value) for value in mapped):
        raise TorrentBracketReconciliationError(
            f"noncanonical recording identity mapped to YouTube source {video_id}"
        )
    expected_recording = recording_id(f"youtube:video:{video_id}")
    canonical = connection.execute(
        "SELECT recording_id, canonical_key FROM recordings WHERE canonical_key = ?",
        (f"youtube:video:{video_id}",),
    ).fetchone()
    if canonical is not None and tuple(canonical) != (
        expected_recording,
        f"youtube:video:{video_id}",
    ):
        raise TorrentBracketReconciliationError(
            f"deterministic YouTube recording identity collision for {video_id}"
        )
    by_identifier = connection.execute(
        "SELECT canonical_key FROM recordings WHERE recording_id = ?",
        (expected_recording,),
    ).fetchone()
    if by_identifier is not None and by_identifier["canonical_key"] != f"youtube:video:{video_id}":
        raise TorrentBracketReconciliationError(
            f"deterministic YouTube recording identity collision for {video_id}"
        )
    if canonical is not None and mapped == [expected_recording]:
        return {
            "resolution_state": "unique_native_recording",
            "youtube_source_id": expected_source,
            "youtube_recording_id": expected_recording,
            "mapped_recording_ids": mapped,
        }
    return {
        "resolution_state": "native_source_without_unique_recording",
        "youtube_source_id": expected_source,
        "youtube_recording_id": None,
        "mapped_recording_ids": mapped,
    }


def _review_task(match_candidate_id: str, video_id: str, observed_at: str) -> dict[str, Any]:
    return {
        "review_task_id": stable_id(
            "rtk", "torrent_bracket_reconciliation_candidate", "match_candidate", match_candidate_id
        ),
        "task_kind": "torrent_bracket_reconciliation_candidate",
        "target_type": "match_candidate",
        "target_id": match_candidate_id,
        "reason": (
            f"Review terminal bracketed YouTube ID {video_id} from a torrent manifest path; "
            "the filename is a locator hint, not proof of content identity"
        ),
        "priority": 60,
        "created_at": observed_at,
    }


def build_torrent_bracket_reconciliation_plan(
    connection: sqlite3.Connection,
    torrent_path: Path,
    discovery_metadata_path: Path,
) -> dict[str, Any]:
    """Rebuild an exact private plan in one consistent catalog snapshot."""

    owns_read_transaction = not connection.in_transaction
    if owns_read_transaction:
        connection.execute("BEGIN")
    try:
        return _build_torrent_bracket_reconciliation_plan_in_snapshot(
            connection, torrent_path, discovery_metadata_path
        )
    finally:
        if owns_read_transaction and connection.in_transaction:
            connection.rollback()


def _build_torrent_bracket_reconciliation_plan_in_snapshot(
    connection: sqlite3.Connection,
    torrent_path: Path,
    discovery_metadata_path: Path,
) -> dict[str, Any]:
    """Build within the caller's already-open read or write transaction."""

    resolved_torrent, torrent_body = _stable_file(
        Path(torrent_path), MAX_TORRENT_BYTES, "torrent manifest"
    )
    resolved_discovery, discovery_body = _stable_file(
        Path(discovery_metadata_path), MAX_DISCOVERY_BYTES, "torrent discovery metadata"
    )
    if resolved_torrent == resolved_discovery:
        raise TorrentBracketReconciliationError("torrent and discovery inputs must differ")
    torrent = _parse_torrent(torrent_body)
    discovery = _validate_discovery(
        discovery_body,
        discovery_path=resolved_discovery,
        torrent_path=resolved_torrent,
        torrent=torrent,
    )
    combined_input_sha256 = _combined_import_digest(
        resolved_torrent, torrent_body, resolved_discovery, discovery_body
    )
    binding = _catalog_binding(
        connection,
        torrent=torrent,
        discovery=discovery,
        combined_input_sha256=combined_input_sha256,
        torrent_filename=resolved_torrent.name,
    )

    scoped_files = 0
    scoped_video_files = 0
    candidates: list[dict[str, Any]] = []
    directory_counts = {label: 0 for label in SCOPED_DIRECTORY_LABELS}
    resolution_counts = {
        "missing_native_source": 0,
        "native_source_without_unique_recording": 0,
        "unique_native_recording": 0,
    }
    distinct_ids: set[str] = set()
    resolution_cache: dict[str, dict[str, Any]] = {}
    for file_record in torrent["files"]:
        raw_components = file_record["raw_components"]
        directory_label = SCOPED_DIRECTORY_BYTES.get(raw_components[0])
        if directory_label is None:
            continue
        scoped_files += 1
        if scoped_files > MAX_SCOPED_FILES:
            raise TorrentBracketReconciliationError("torrent exceeds scoped-file cap")
        basename = raw_components[-1]
        lower = basename.lower()
        if any(lower.endswith(b"." + extension) for extension in VIDEO_EXTENSIONS):
            scoped_video_files += 1
        video_id = terminal_bracketed_youtube_id_bytes(basename)
        if video_id is None:
            continue
        if not YOUTUBE_ID_RE.fullmatch(video_id):
            raise TorrentBracketReconciliationError("terminal YouTube ID parser escaped grammar")
        directory_counts[directory_label] += 1
        distinct_ids.add(video_id)
        resolution = resolution_cache.get(video_id)
        if resolution is None:
            resolution = _youtube_resolution(connection, video_id)
            resolution_cache[video_id] = resolution
        resolution_counts[resolution["resolution_state"]] += 1
        torrent_file_source_id = binding["torrent_file_source_ids"][file_record["file_index"]]
        youtube_source_id = resolution["youtube_source_id"]
        right_type = "source" if youtube_source_id else "youtube_video_id"
        right_id = youtube_source_id or video_id
        match_candidate_id = stable_id(
            "mat",
            IMPORTER_NAME,
            torrent["torrent_sha256"],
            discovery["discovery_sha256"],
            torrent_file_source_id,
            video_id,
        )
        raw_components_base64 = [
            base64.b64encode(component).decode("ascii") for component in raw_components
        ]
        raw_path_sha256 = _raw_path_sha256(raw_components)
        evidence_json = {
            "torrent_sha256": torrent["torrent_sha256"],
            "discovery_sha256": discovery["discovery_sha256"],
            "info_hash_sha1": torrent["info_hash_sha1"],
            "torrent_manifest_source_id": binding["torrent_manifest_source_id"],
            "torrent_file_source_id": torrent_file_source_id,
            "torrent_file_index": file_record["file_index"],
            "directory_label": directory_label,
            "manifest_path": file_record["manifest_path"],
            "manifest_path_components_base64": raw_components_base64,
            "manifest_path_sha256": raw_path_sha256,
            "byte_count": file_record["byte_count"],
            "youtube_video_id": video_id,
            "evidence_basis": "terminal_filename_bracket_before_video_extension",
            "resolution_state": resolution["resolution_state"],
            "youtube_source_id": youtube_source_id,
            "youtube_recording_id": resolution["youtube_recording_id"],
            "mapped_recording_ids": resolution["mapped_recording_ids"],
            "requires_human_review": True,
            "relationship_asserted": False,
            "merge_performed": False,
            "publication_authority": False,
            "payload_downloaded_or_read": False,
        }
        generic_metadata = {
            "schema_version": 1,
            "candidate_kind": "torrent_file_to_youtube_locator",
            "evidence_basis": "terminal_filename_bracket_before_video_extension",
            "youtube_video_id": video_id,
            "resolution_state": resolution["resolution_state"],
            "calibration_state": "not_calibrated",
            "requires_human_review": True,
            "relationship_asserted": False,
            "merge_performed": False,
            "publication_authority": False,
        }
        task = _review_task(match_candidate_id, video_id, discovery["observed_at"])
        candidates.append(
            {
                "generic": {
                    "match_candidate_id": match_candidate_id,
                    "left_object_type": "source",
                    "left_object_id": torrent_file_source_id,
                    "right_object_type": right_type,
                    "right_object_id": right_id,
                    "match_method": "torrent_terminal_bracket_youtube_locator_v1",
                    "raw_score": None,
                    "calibrated_probability": None,
                    "decision_state": "candidate",
                    "metadata_json": generic_metadata,
                },
                "evidence": {
                    "match_candidate_id": match_candidate_id,
                    "review_task_id": task["review_task_id"],
                    "candidate_kind": "torrent_file_to_youtube_locator",
                    "torrent_manifest_source_id": binding["torrent_manifest_source_id"],
                    "torrent_file_source_id": torrent_file_source_id,
                    "torrent_file_index": file_record["file_index"],
                    "directory_label": directory_label,
                    "manifest_path": file_record["manifest_path"],
                    "manifest_path_components_base64": raw_components_base64,
                    "manifest_path_sha256": raw_path_sha256,
                    "byte_count": file_record["byte_count"],
                    "youtube_video_id": video_id,
                    "youtube_source_id": youtube_source_id,
                    "youtube_recording_id": resolution["youtube_recording_id"],
                    "evidence_basis": "terminal_filename_bracket_before_video_extension",
                    "resolution_state": resolution["resolution_state"],
                    "requires_human_review": 1,
                    "relationship_asserted": 0,
                    "merge_performed": 0,
                    "visibility": "private",
                    "publication_authority": "none",
                    "evidence_json": evidence_json,
                },
                "review_task": task,
            }
        )
        if len(candidates) > MAX_CANDIDATES:
            raise TorrentBracketReconciliationError("torrent exceeds reconciliation-candidate cap")

    candidates.sort(key=lambda value: value["generic"]["match_candidate_id"])
    candidate_ids = [value["generic"]["match_candidate_id"] for value in candidates]
    if len(candidate_ids) != len(set(candidate_ids)):
        raise TorrentBracketReconciliationError("torrent reconciliation plan has duplicate candidates")
    statistics = {
        "provider_file_records_scanned": torrent["file_count"],
        "scoped_file_records": scoped_files,
        "scoped_video_file_records": scoped_video_files,
        "terminal_bracket_candidates": len(candidates),
        "distinct_youtube_video_ids": len(distinct_ids),
        "candidates_by_directory": directory_counts,
        "resolution_state_counts": resolution_counts,
        "review_tasks_total": len(candidates),
        "payload_files_read": 0,
        "payload_bytes_read": 0,
        "source_or_recording_mutations": 0,
        "source_relations": 0,
        "recording_relations": 0,
        "recording_merges": 0,
        "publication_decisions": 0,
        "identity_assertions": 0,
        "claims": 0,
    }
    core = {
        "schema_version": 1,
        "plan_kind": PLAN_KIND,
        "inputs": {
            "torrent_sha256": torrent["torrent_sha256"],
            "torrent_byte_count": len(torrent_body),
            "torrent_filename": resolved_torrent.name,
            "info_hash_sha1": torrent["info_hash_sha1"],
            "discovery_sha256": discovery["discovery_sha256"],
            "discovery_byte_count": discovery["discovery_byte_count"],
            "discovery_filename": resolved_discovery.name,
            "combined_import_input_sha256": combined_input_sha256,
        },
        "catalog_binding": {
            "torrent_manifest_source_id": binding["torrent_manifest_source_id"],
            "torrent_import_batch_id": binding["torrent_import_batch_id"],
            "torrent_importer_name": "torrent_manifest_metadata",
            "torrent_importer_version": binding["torrent_importer_version"],
            "observed_at": discovery["observed_at"],
        },
        "scope": {"directory_labels": list(SCOPED_DIRECTORY_LABELS)},
        "candidates": candidates,
        "statistics": statistics,
        "policy": {
            "terminal_brackets_only": True,
            "bare_or_dash_suffixes_accepted": False,
            "torrent_paths_are_content_truth": False,
            "payload_download_or_read": False,
            "requires_human_review": True,
            "relationship_asserted": False,
            "merge_performed": False,
            "publication_authority": False,
            "capacity_limits": {
                "torrent_bytes": MAX_TORRENT_BYTES,
                "discovery_bytes": MAX_DISCOVERY_BYTES,
                "torrent_files": MAX_TORRENT_FILES,
                "scoped_files": MAX_SCOPED_FILES,
                "candidates": MAX_CANDIDATES,
            },
        },
    }
    plan_sha256 = sha256_bytes(canonical_json(core).encode("utf-8"))
    return {
        **core,
        "plan_id": f"tbrp_{plan_sha256[:32]}",
        "plan_sha256": plan_sha256,
    }


def summarize_torrent_bracket_reconciliation_plan(plan: dict[str, Any]) -> dict[str, Any]:
    return {
        "valid": True,
        "plan_id": plan["plan_id"],
        "plan_sha256": plan["plan_sha256"],
        "torrent_sha256": plan["inputs"]["torrent_sha256"],
        "discovery_sha256": plan["inputs"]["discovery_sha256"],
        "info_hash_sha1": plan["inputs"]["info_hash_sha1"],
        "torrent_manifest_source_id": plan["catalog_binding"]["torrent_manifest_source_id"],
        "torrent_import_batch_id": plan["catalog_binding"]["torrent_import_batch_id"],
        "observed_at": plan["catalog_binding"]["observed_at"],
        "scope": plan["scope"],
        "statistics": plan["statistics"],
        "requires_human_review": True,
        "relationship_asserted": False,
        "merge_performed": False,
        "publication_authority": False,
    }


def _protected_table_counts(connection: sqlite3.Connection) -> dict[str, int]:
    tables = [
        row["name"]
        for row in connection.execute(
            "SELECT name FROM sqlite_master WHERE type = 'table' AND name NOT LIKE 'sqlite_%' ORDER BY name"
        )
        if row["name"] not in ALLOWED_WRITE_TABLES
    ]
    result: dict[str, int] = {}
    for table in tables:
        quoted = table.replace('"', '""')
        result[table] = int(
            connection.execute(f'SELECT count(*) FROM "{quoted}"').fetchone()[0]
        )
    return result


def _insert_review_task(
    connection: sqlite3.Connection, task: dict[str, Any], *, allow_insert: bool
) -> None:
    if allow_insert:
        connection.execute(
            """
            INSERT OR IGNORE INTO review_tasks(
                review_task_id, task_kind, target_type, target_id, reason, priority,
                status, created_at, updated_at
            ) VALUES(?, ?, ?, ?, ?, ?, 'open', ?, ?)
            """,
            (
                task["review_task_id"],
                task["task_kind"],
                task["target_type"],
                task["target_id"],
                task["reason"],
                task["priority"],
                task["created_at"],
                task["created_at"],
            ),
        )
    row = connection.execute(
        """
        SELECT task_kind, target_type, target_id, reason, priority, created_at
        FROM review_tasks WHERE review_task_id = ?
        """,
        (task["review_task_id"],),
    ).fetchone()
    expected = (
        task["task_kind"],
        task["target_type"],
        task["target_id"],
        task["reason"],
        task["priority"],
        task["created_at"],
    )
    if row is None or tuple(row) != expected:
        raise TorrentBracketReconciliationError("existing torrent review task conflicts")


def _insert_candidate(
    connection: sqlite3.Connection,
    candidate: dict[str, Any],
    import_batch_id: str,
    *,
    allow_insert: bool,
) -> None:
    generic = candidate["generic"]
    generic_values = (
        generic["left_object_type"],
        generic["left_object_id"],
        generic["right_object_type"],
        generic["right_object_id"],
        generic["match_method"],
        None,
        None,
        "candidate",
        canonical_json(generic["metadata_json"]),
    )
    if allow_insert:
        connection.execute(
            """
            INSERT OR IGNORE INTO match_candidates(
                match_candidate_id, left_object_type, left_object_id,
                right_object_type, right_object_id, match_method, raw_score,
                calibrated_probability, decision_state, metadata_json
            ) VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (generic["match_candidate_id"], *generic_values),
        )
    row = connection.execute(
        """
        SELECT left_object_type, left_object_id, right_object_type, right_object_id,
               match_method, raw_score, calibrated_probability, decision_state,
               metadata_json
        FROM match_candidates WHERE match_candidate_id = ?
        """,
        (generic["match_candidate_id"],),
    ).fetchone()
    if row is None or tuple(row) != generic_values:
        raise TorrentBracketReconciliationError("existing torrent match candidate conflicts")

    evidence = candidate["evidence"]
    values = (
        import_batch_id,
        evidence["review_task_id"],
        evidence["candidate_kind"],
        evidence["torrent_manifest_source_id"],
        evidence["torrent_file_source_id"],
        evidence["torrent_file_index"],
        evidence["directory_label"],
        evidence["manifest_path"],
        canonical_json(evidence["manifest_path_components_base64"]),
        evidence["manifest_path_sha256"],
        evidence["byte_count"],
        evidence["youtube_video_id"],
        evidence["youtube_source_id"],
        evidence["youtube_recording_id"],
        evidence["evidence_basis"],
        evidence["resolution_state"],
        1,
        0,
        0,
        "private",
        "none",
        canonical_json(evidence["evidence_json"]),
    )
    if allow_insert:
        connection.execute(
            """
            INSERT OR IGNORE INTO torrent_bracket_youtube_candidates(
                match_candidate_id, import_batch_id, review_task_id, candidate_kind,
                torrent_manifest_source_id, torrent_file_source_id, torrent_file_index,
                directory_label, manifest_path, manifest_path_components_base64_json,
                manifest_path_sha256, byte_count, youtube_video_id, youtube_source_id,
                youtube_recording_id, evidence_basis, resolution_state,
                requires_human_review, relationship_asserted, merge_performed,
                visibility, publication_authority, evidence_json
            ) VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (generic["match_candidate_id"], *values),
        )
    row = connection.execute(
        """
        SELECT import_batch_id, review_task_id, candidate_kind,
               torrent_manifest_source_id, torrent_file_source_id, torrent_file_index,
               directory_label, manifest_path, manifest_path_components_base64_json,
               manifest_path_sha256, byte_count, youtube_video_id, youtube_source_id,
               youtube_recording_id, evidence_basis, resolution_state,
               requires_human_review, relationship_asserted, merge_performed,
               visibility, publication_authority, evidence_json
        FROM torrent_bracket_youtube_candidates WHERE match_candidate_id = ?
        """,
        (generic["match_candidate_id"],),
    ).fetchone()
    if row is None or tuple(row) != values:
        raise TorrentBracketReconciliationError("existing torrent bracket evidence conflicts")


def _verify_receipt(
    connection: sqlite3.Connection, plan: dict[str, Any], import_batch_id: str
) -> None:
    statistics = plan["statistics"]
    expected = (
        plan["catalog_binding"]["torrent_import_batch_id"],
        plan["catalog_binding"]["torrent_manifest_source_id"],
        plan["inputs"]["info_hash_sha1"],
        plan["inputs"]["torrent_sha256"],
        plan["inputs"]["discovery_sha256"],
        plan["inputs"]["combined_import_input_sha256"],
        plan["plan_sha256"],
        plan["catalog_binding"]["observed_at"],
        canonical_json(plan["scope"]["directory_labels"]),
        statistics["scoped_file_records"],
        statistics["terminal_bracket_candidates"],
        statistics["review_tasks_total"],
        canonical_json(statistics),
        plan["catalog_binding"]["observed_at"],
    )
    row = connection.execute(
        """
        SELECT torrent_import_batch_id, torrent_manifest_source_id, info_hash_sha1,
               torrent_sha256, discovery_sha256, combined_import_input_sha256,
               plan_sha256, observed_at, scoped_directory_labels_json,
               scoped_file_count, candidate_count, review_task_count,
               statistics_json, imported_at
        FROM torrent_bracket_reconciliation_imports WHERE import_batch_id = ?
        """,
        (import_batch_id,),
    ).fetchone()
    if row is None or tuple(row) != expected:
        raise TorrentBracketReconciliationError("torrent bracket import receipt differs")


def _verify_candidate_import_batch(
    connection: sqlite3.Connection, plan: dict[str, Any], import_batch_id: str
) -> None:
    row = connection.execute(
        """
        SELECT importer_name, importer_version, input_sha256, source_snapshot_date,
               started_at, completed_at, status, statistics_json
        FROM import_batches WHERE import_batch_id = ?
        """,
        (import_batch_id,),
    ).fetchone()
    if row is None:
        raise TorrentBracketReconciliationError("torrent candidate import batch is missing")
    importer_version = _bounded_text(
        row["importer_version"], "torrent candidate importer version", maximum=64
    )
    expected = (
        IMPORTER_NAME,
        importer_version,
        plan["plan_sha256"],
        plan["catalog_binding"]["observed_at"][:10],
        plan["catalog_binding"]["observed_at"],
        plan["catalog_binding"]["observed_at"],
        "completed",
        canonical_json(plan["statistics"]),
    )
    if tuple(row) != expected:
        raise TorrentBracketReconciliationError("torrent candidate import batch differs")
    observation = connection.execute(
        """
        SELECT import_observation_id, source_snapshot_date, observed_at, status,
               completed_at, statistics_json
        FROM import_observations
        WHERE import_batch_id = ? AND importer_version = ? AND observed_at = ?
        """,
        (
            import_batch_id,
            importer_version,
            plan["catalog_binding"]["observed_at"],
        ),
    ).fetchone()
    expected_observation = (
        stable_id(
            "iob",
            import_batch_id,
            importer_version,
            plan["catalog_binding"]["observed_at"],
        ),
        plan["catalog_binding"]["observed_at"][:10],
        plan["catalog_binding"]["observed_at"],
        "completed",
        plan["catalog_binding"]["observed_at"],
        canonical_json(plan["statistics"]),
    )
    if observation is None or tuple(observation) != expected_observation:
        raise TorrentBracketReconciliationError(
            "torrent candidate import observation differs"
        )


def _verify_plan_rows(
    connection: sqlite3.Connection,
    plan: dict[str, Any],
    import_batch_id: str,
    *,
    allow_insert: bool,
) -> None:
    statistics = plan["statistics"]
    if allow_insert:
        connection.execute(
            """
            INSERT INTO torrent_bracket_reconciliation_imports(
                import_batch_id, torrent_import_batch_id, torrent_manifest_source_id,
                info_hash_sha1, torrent_sha256, discovery_sha256,
                combined_import_input_sha256, plan_sha256, observed_at,
                scoped_directory_labels_json, scoped_file_count, candidate_count,
                review_task_count, statistics_json, imported_at
            ) VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                import_batch_id,
                plan["catalog_binding"]["torrent_import_batch_id"],
                plan["catalog_binding"]["torrent_manifest_source_id"],
                plan["inputs"]["info_hash_sha1"],
                plan["inputs"]["torrent_sha256"],
                plan["inputs"]["discovery_sha256"],
                plan["inputs"]["combined_import_input_sha256"],
                plan["plan_sha256"],
                plan["catalog_binding"]["observed_at"],
                canonical_json(plan["scope"]["directory_labels"]),
                statistics["scoped_file_records"],
                statistics["terminal_bracket_candidates"],
                statistics["review_tasks_total"],
                canonical_json(statistics),
                plan["catalog_binding"]["observed_at"],
            ),
        )
    _verify_receipt(connection, plan, import_batch_id)
    for candidate in plan["candidates"]:
        _insert_review_task(connection, candidate["review_task"], allow_insert=allow_insert)
        _insert_candidate(
            connection, candidate, import_batch_id, allow_insert=allow_insert
        )


def import_torrent_bracket_reconciliation(
    connection: sqlite3.Connection,
    torrent_path: Path,
    discovery_metadata_path: Path,
) -> dict[str, Any]:
    """Admit only immutable private candidates; never mutate source identity."""

    torrent_path = Path(torrent_path)
    discovery_metadata_path = Path(discovery_metadata_path)
    initial = build_torrent_bracket_reconciliation_plan(
        connection, torrent_path, discovery_metadata_path
    )
    with transaction(connection):
        protected_before = _protected_table_counts(connection)
        locked = build_torrent_bracket_reconciliation_plan(
            connection, torrent_path, discovery_metadata_path
        )
        if locked["plan_sha256"] != initial["plan_sha256"]:
            raise TorrentBracketReconciliationError(
                "torrent reconciliation inputs changed before the write lock"
            )
        batch_id, existing = _begin_batch(
            connection,
            IMPORTER_NAME,
            locked["plan_sha256"],
            locked["catalog_binding"]["observed_at"][:10],
            locked["catalog_binding"]["observed_at"],
        )
        receipt_exists = connection.execute(
            "SELECT 1 FROM torrent_bracket_reconciliation_imports WHERE import_batch_id = ?",
            (batch_id,),
        ).fetchone() is not None
        _verify_plan_rows(
            connection,
            locked,
            batch_id,
            allow_insert=existing is None and not receipt_exists,
        )
        if existing is None:
            _complete_batch(
                connection,
                batch_id,
                locked["catalog_binding"]["observed_at"],
                locked["statistics"],
            )
        elif existing != locked["statistics"]:
            raise TorrentBracketReconciliationError(
                "completed torrent reconciliation statistics differ"
            )
        _verify_candidate_import_batch(connection, locked, batch_id)
        final = build_torrent_bracket_reconciliation_plan(
            connection, torrent_path, discovery_metadata_path
        )
        if final["plan_sha256"] != locked["plan_sha256"]:
            raise TorrentBracketReconciliationError(
                "torrent reconciliation inputs changed during import"
            )
        protected_after = _protected_table_counts(connection)
        if protected_after != protected_before:
            changed = {
                table: [protected_before[table], protected_after[table]]
                for table in protected_before
                if protected_before[table] != protected_after[table]
            }
            raise TorrentBracketReconciliationError(
                f"torrent reconciliation touched protected catalog tables: {changed}"
            )
        return {
            "import_batch_id": batch_id,
            **summarize_torrent_bracket_reconciliation_plan(locked),
        }
