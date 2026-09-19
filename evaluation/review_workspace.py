"""Private loopback workspace for ASR-blind interval-selection review.

This module is deliberately outside the Astro application and public corpus export.
It accepts only the exact cohort, proposal inputs, incomplete selection template,
catalogue, and acquired parent-media root.  It never reads transcript, ASR, reference,
wiki, or network data.

The browser UI is an operational aid, not evidence that a reviewer fulfilled an
attestation.  Partial work lives in a separate private draft.  A completed selection
review is materialized only after explicit human attestations, exact-media rehashing,
and the existing authoritative review validator all succeed.
"""

from __future__ import annotations

import contextlib
import fcntl
import hashlib
import hmac
import json
import os
import re
import secrets
import socket
import sqlite3
import stat
import threading
import urllib.parse
import webbrowser
from dataclasses import dataclass
from datetime import datetime, timezone
from http import HTTPStatus
from http.cookies import SimpleCookie
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Any, Iterable, Sequence

from .selection_review import (
    MINIMUM_ACCEPTED_DURATION_MS,
    validate_interval_selection_review,
)
from .review_workspace_state import (
    IndeterminateDraftCommit,
    REQUIRED_COVERAGE_TOLERANCE_MS,
    REVIEW_TOOL_NAME,
    REVIEW_TOOL_VERSION,
)
from .validation import (
    ContractError,
    _canonical_bytes,
    _stable_id,
    canonical_manifest_sha256,
)


WORKSPACE_CONTRACT_REVISION = "selection_review_workspace_v1"
MAX_JSON_BYTES = 32 * 1024 * 1024
MAX_POST_BYTES = 64 * 1024
COPY_CHUNK_BYTES = 1024 * 1024
MEDIA_SEND_CHUNK_BYTES = 1024 * 1024
MAX_UI_ASSET_BYTES = 2 * 1024 * 1024
UI_ASSET_TYPES = {
    "index.html": "text/html; charset=utf-8",
    "app.js": "text/javascript; charset=utf-8",
    "styles.css": "text/css; charset=utf-8",
}
FORBIDDEN_INPUT_COMPONENT = re.compile(
    r"(?:^|[-_.])(asr|transcript|reference|hypothesis|caption|subtitle)(?:$|[-_.])",
    re.IGNORECASE,
)
SHA256_RE = re.compile(r"[0-9a-f]{64}\Z")
ID_RE = re.compile(r"[A-Za-z][A-Za-z0-9._:-]{2,127}\Z")
MEDIA_ID_RE = re.compile(r"media_sha256_([0-9a-f]{64})\Z")
SESSION_PATH_RE = re.compile(r"/w/[A-Za-z0-9_-]{32,192}/\Z")


def _fail(path: str, message: str) -> None:
    raise ContractError(f"{path}: {message}")


def _utc_now() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def _strict_json_bytes(body: bytes, label: str) -> dict[str, Any]:
    def unique_object(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
        output: dict[str, Any] = {}
        for key, value in pairs:
            if key in output:
                _fail(label, f"duplicate JSON object key {key!r}")
            output[key] = value
        return output

    def reject_constant(value: str) -> None:
        _fail(label, f"non-finite JSON number {value!r} is forbidden")

    try:
        value = json.loads(
            body.decode("utf-8"),
            object_pairs_hook=unique_object,
            parse_constant=reject_constant,
        )
    except ContractError:
        raise
    except (UnicodeDecodeError, ValueError) as error:
        _fail(label, f"invalid JSON: {error}")
    if not isinstance(value, dict):
        _fail(label, "top level must be an object")
    return value


def _fingerprint(value: os.stat_result) -> tuple[int, int, int, int, int, int]:
    return (
        value.st_dev,
        value.st_ino,
        value.st_size,
        value.st_mtime_ns,
        value.st_ctime_ns,
        value.st_mode,
    )


def _assert_path_fingerprint(
    path: Path,
    expected: tuple[int, int, int, int, int, int],
    label: str,
) -> None:
    """Require one pathname to still name the exact startup inode state."""

    try:
        current = path.lstat()
    except OSError as error:
        _fail(label, f"cannot re-stat pathname: {error}")
    if _fingerprint(current) != expected:
        _fail(label, "pathname no longer identifies the exact pinned file")


def _assert_private_directory(path: Path, *, create: bool) -> Path:
    absolute = path.absolute()
    if create:
        try:
            absolute.mkdir(mode=0o700, parents=True, exist_ok=True)
        except OSError as error:
            _fail("$.workspace", f"cannot create private directory: {error}")
    _assert_no_symlink_components(absolute, "$.workspace")
    try:
        info = absolute.lstat()
    except OSError as error:
        _fail("$.workspace", f"cannot stat private directory: {error}")
    if stat.S_ISLNK(info.st_mode) or not stat.S_ISDIR(info.st_mode):
        _fail("$.workspace", "must be a non-symlink directory")
    if info.st_uid != os.geteuid():
        _fail("$.workspace", "must be owned by the current user")
    if stat.S_IMODE(info.st_mode) != 0o700:
        _fail("$.workspace", "must have exact mode 0700")
    return absolute


def _assert_no_symlink_components(path: Path, label: str) -> None:
    absolute = path.absolute()
    current = Path(absolute.anchor)
    for component in absolute.parts[1:]:
        current /= component
        try:
            info = current.lstat()
        except OSError as error:
            _fail(label, f"cannot stat path component: {error}")
        if stat.S_ISLNK(info.st_mode):
            _fail(label, "symlink path components are forbidden")


def _reject_forbidden_path(
    path: Path, label: str, *, allow_asr_label: bool = False
) -> None:
    for component in path.parts:
        match = FORBIDDEN_INPUT_COMPONENT.search(component)
        if match and not (allow_asr_label and match.group(1).lower() == "asr"):
            _fail(label, "ASR/transcript/reference-shaped paths are forbidden")


def _sha256_fd(fd: int) -> str:
    digest = hashlib.sha256()
    offset = 0
    while True:
        block = os.pread(fd, COPY_CHUNK_BYTES, offset)
        if not block:
            break
        digest.update(block)
        offset += len(block)
    return digest.hexdigest()


@dataclass
class PinnedInput:
    role: str
    ordinal: int | None
    path: Path
    fd: int
    raw_sha256: str
    byte_count: int
    fingerprint: tuple[int, int, int, int, int, int]
    value: dict[str, Any]

    @classmethod
    def open_json(cls, role: str, ordinal: int | None, path: Path) -> "PinnedInput":
        absolute = path.absolute()
        _reject_forbidden_path(
            absolute,
            f"$.inputs.{role}",
            # The deliberately tracked cohort is named himr-asr-candidate-*.  It
            # contains no system output; its authoritative validator checks that
            # it is an unfrozen candidate manifest.
            allow_asr_label=role == "cohort",
        )
        _assert_no_symlink_components(absolute, f"$.inputs.{role}")
        flags = os.O_RDONLY | getattr(os, "O_CLOEXEC", 0) | getattr(os, "O_NOFOLLOW", 0)
        try:
            fd = os.open(absolute, flags)
        except OSError as error:
            _fail(f"$.inputs.{role}", f"cannot open input: {error}")
        try:
            before = os.fstat(fd)
            if not stat.S_ISREG(before.st_mode):
                _fail(f"$.inputs.{role}", "must be a regular file")
            if before.st_size > MAX_JSON_BYTES:
                _fail(f"$.inputs.{role}", f"exceeds {MAX_JSON_BYTES} bytes")
            body = os.pread(fd, before.st_size + 1, 0)
            after = os.fstat(fd)
            if len(body) != before.st_size or _fingerprint(before) != _fingerprint(after):
                _fail(f"$.inputs.{role}", "changed while being read")
            raw_sha = hashlib.sha256(body).hexdigest()
            value = _strict_json_bytes(body, f"$.inputs.{role}")
            return cls(
                role=role,
                ordinal=ordinal,
                path=absolute,
                fd=fd,
                raw_sha256=raw_sha,
                byte_count=before.st_size,
                fingerprint=_fingerprint(before),
                value=value,
            )
        except Exception:
            os.close(fd)
            raise

    def assert_identity(self) -> None:
        try:
            current = os.fstat(self.fd)
            path_info = self.path.lstat()
        except OSError as error:
            _fail(f"$.inputs.{self.role}", f"cannot re-stat input: {error}")
        if _fingerprint(current) != self.fingerprint:
            _fail(f"$.inputs.{self.role}", "open input changed after initialization")
        if _fingerprint(path_info) != self.fingerprint:
            _fail(f"$.inputs.{self.role}", "input pathname no longer identifies the pinned file")

    def reverify(self) -> None:
        self.assert_identity()
        observed = _sha256_fd(self.fd)
        # Hashing can be long enough for a pathname replacement or in-place write
        # to occur after the opening checks.  Close that interval before accepting
        # the digest; a bounded workspace-wide identity sweep follows as well.
        self.assert_identity()
        if observed != self.raw_sha256:
            _fail(f"$.inputs.{self.role}", "input bytes changed after initialization")

    def close(self) -> None:
        with contextlib.suppress(OSError):
            os.close(self.fd)


@dataclass
class MediaBinding:
    ordinal: int
    candidate_id: str
    recording_id: str
    source_id: str
    source_native_id: str
    title: str
    proposal_id: str
    proposal_schema_version: int
    media_id: str
    sha256: str
    byte_count: int
    duration_ms: int
    rendition_id: str
    rendition_kind: str
    media_location_id: str
    storage_uri: str
    storage_class: str
    path: Path
    mime_type: str
    codecs: tuple[str, ...]
    fd: int
    startup_fingerprint: tuple[int, int, int, int, int, int]
    startup_mode: int
    mutability_policy: str
    opaque_id: str

    def manifest_value(self) -> dict[str, Any]:
        return {
            "ordinal": self.ordinal,
            "candidate_id": self.candidate_id,
            "recording_id": self.recording_id,
            "source_id": self.source_id,
            "source_native_id": self.source_native_id,
            "proposal_id": self.proposal_id,
            "proposal_schema_version": self.proposal_schema_version,
            "parent_media_id": self.media_id,
            "parent_media_sha256": self.sha256,
            "parent_media_byte_count": self.byte_count,
            "parent_media_duration_ms": self.duration_ms,
            "parent_rendition_id": self.rendition_id,
            "parent_rendition_kind": self.rendition_kind,
            "timeline_coordinate_system": "parent_rendition_media_ms",
            "media_location_id": self.media_location_id,
            "storage_uri": self.storage_uri,
            "storage_class": self.storage_class,
            "path": str(self.path),
            "mime_type": self.mime_type,
            "codecs": list(self.codecs),
            "startup_mode": f"{self.startup_mode:04o}",
            "mutability_policy": self.mutability_policy,
        }

    def assert_unchanged(self, *, rehash: bool) -> None:
        label = f"$.media_bindings[{self.ordinal - 1}]"
        try:
            current = os.fstat(self.fd)
            path_info = self.path.lstat()
        except OSError as error:
            _fail(label, f"cannot re-stat media: {error}")
        if _fingerprint(current) != self.startup_fingerprint:
            _fail(label, "parent media changed after initialization")
        if _fingerprint(path_info) != self.startup_fingerprint:
            _fail(label, "media pathname no longer identifies the pinned file")
        if rehash:
            observed = _sha256_fd(self.fd)
            # Re-stat both the descriptor and pathname after the long read.  The
            # opening identity check alone cannot detect a replacement made while
            # this particular parent is being hashed.
            try:
                after = os.fstat(self.fd)
                path_after = self.path.lstat()
            except OSError as error:
                _fail(label, f"cannot re-stat media after hashing: {error}")
            if _fingerprint(after) != self.startup_fingerprint:
                _fail(label, "parent media changed while being hashed")
            if _fingerprint(path_after) != self.startup_fingerprint:
                _fail(label, "media pathname changed while being hashed")
            if observed != self.sha256:
                _fail(label, "parent media digest changed after initialization")

    def close(self) -> None:
        with contextlib.suppress(OSError):
            os.close(self.fd)


@dataclass(frozen=True)
class WorkspacePaths:
    root: Path
    lock: Path
    manifest: Path
    catalog_snapshot: Path
    draft: Path
    completed_review: Path
    receipt: Path

    @classmethod
    def under(cls, root: Path) -> "WorkspacePaths":
        return cls(
            root=root,
            lock=root / "workspace.lock",
            manifest=root / "workspace-manifest.json",
            catalog_snapshot=root / "catalog-snapshot.sqlite3",
            draft=root / "selection-review-draft.json",
            completed_review=root.parent / "interval-selection-review.json",
            receipt=root / "finalization-receipt.json",
        )


class WorkspaceLock:
    def __init__(self, path: Path):
        flags = os.O_RDWR | os.O_CREAT | getattr(os, "O_CLOEXEC", 0) | getattr(os, "O_NOFOLLOW", 0)
        old_umask = os.umask(0o077)
        try:
            self.fd = os.open(path, flags, 0o600)
        except OSError as error:
            _fail("$.workspace.lock", f"cannot open lock: {error}")
        finally:
            os.umask(old_umask)
        try:
            info = os.fstat(self.fd)
            if not stat.S_ISREG(info.st_mode) or info.st_nlink != 1:
                _fail("$.workspace.lock", "must be a single-link regular file")
            if info.st_uid != os.geteuid() or stat.S_IMODE(info.st_mode) != 0o600:
                _fail("$.workspace.lock", "must be current-user-owned with mode 0600")
            try:
                fcntl.flock(self.fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
            except BlockingIOError:
                _fail("$.workspace.lock", "another review process holds this workspace")
        except Exception:
            os.close(self.fd)
            raise

    def close(self) -> None:
        with contextlib.suppress(OSError):
            fcntl.flock(self.fd, fcntl.LOCK_UN)
            os.close(self.fd)


def _atomic_write_bytes(path: Path, body: bytes, *, mode: int, replace: bool) -> None:
    token = secrets.token_hex(16)
    temporary = path.parent / f".{path.name}.tmp-{token}"
    flags = os.O_WRONLY | os.O_CREAT | os.O_EXCL | getattr(os, "O_CLOEXEC", 0) | getattr(os, "O_NOFOLLOW", 0)
    old_umask = os.umask(0o077)
    try:
        fd = os.open(temporary, flags, mode)
    finally:
        os.umask(old_umask)
    try:
        os.fchmod(fd, mode)
        offset = 0
        while offset < len(body):
            written = os.write(fd, body[offset:])
            if written <= 0:
                raise OSError("short write")
            offset += written
        os.fsync(fd)
    except Exception:
        with contextlib.suppress(OSError):
            os.close(fd)
        with contextlib.suppress(OSError):
            temporary.unlink()
        raise
    os.close(fd)
    try:
        if replace:
            os.replace(temporary, path)
        else:
            try:
                os.link(temporary, path, follow_symlinks=False)
            except FileExistsError:
                raise
            finally:
                with contextlib.suppress(OSError):
                    temporary.unlink()
        directory_fd = os.open(path.parent, os.O_RDONLY | getattr(os, "O_CLOEXEC", 0))
        try:
            os.fsync(directory_fd)
        finally:
            os.close(directory_fd)
    except Exception:
        with contextlib.suppress(OSError):
            temporary.unlink()
        raise


def _atomic_write_json(path: Path, value: dict[str, Any], *, mode: int, replace: bool) -> None:
    body = _canonical_bytes(value) + b"\n"
    _atomic_write_bytes(path, body, mode=mode, replace=replace)


def _copy_catalog_snapshot(source: Path, destination: Path) -> None:
    if destination.exists():
        return
    _assert_no_symlink_components(source.absolute(), "$.catalog")
    try:
        info = source.lstat()
    except OSError as error:
        _fail("$.catalog", f"cannot stat catalogue: {error}")
    if stat.S_ISLNK(info.st_mode) or not stat.S_ISREG(info.st_mode):
        _fail("$.catalog", "must be a non-symlink regular file")
    source_fingerprint = _fingerprint(info)
    temporary = destination.parent / f".{destination.name}.tmp-{secrets.token_hex(16)}"
    old_umask = os.umask(0o077)
    try:
        target = sqlite3.connect(temporary)
    finally:
        os.umask(old_umask)
    source_uri = f"file:{urllib.parse.quote(str(source.absolute()))}?mode=ro"
    source_db: sqlite3.Connection | None = None
    try:
        source_db = sqlite3.connect(source_uri, uri=True)
        source_db.execute("PRAGMA query_only=ON")
        check = source_db.execute("PRAGMA quick_check").fetchall()
        if check != [("ok",)]:
            _fail("$.catalog", f"quick_check failed: {check!r}")
        source_db.backup(target)
        target.commit()
        journal_mode = target.execute("PRAGMA journal_mode=DELETE").fetchone()[0]
        if str(journal_mode).lower() != "delete":
            _fail(
                "$.catalog_snapshot",
                f"could not normalize private snapshot journal mode: {journal_mode!r}",
            )
        target.commit()
        target.execute("PRAGMA query_only=ON")
        snapshot_check = target.execute("PRAGMA quick_check").fetchall()
        if snapshot_check != [("ok",)]:
            _fail("$.catalog_snapshot", f"quick_check failed: {snapshot_check!r}")
    except Exception:
        target.close()
        if source_db is not None:
            source_db.close()
        with contextlib.suppress(OSError):
            temporary.unlink()
        raise
    target.close()
    assert source_db is not None
    source_db.close()
    try:
        after = source.lstat()
    except OSError as error:
        with contextlib.suppress(OSError):
            temporary.unlink()
        _fail("$.catalog", f"cannot re-stat catalogue after snapshot: {error}")
    if _fingerprint(after) != source_fingerprint:
        with contextlib.suppress(OSError):
            temporary.unlink()
        _fail("$.catalog", "catalogue main file changed while snapshotting")
    os.chmod(temporary, 0o400)
    fd = os.open(temporary, os.O_RDONLY | getattr(os, "O_CLOEXEC", 0))
    try:
        os.fsync(fd)
    finally:
        os.close(fd)
    try:
        os.link(temporary, destination, follow_symlinks=False)
    except FileExistsError:
        pass
    finally:
        with contextlib.suppress(OSError):
            temporary.unlink()
    directory_fd = os.open(destination.parent, os.O_RDONLY | getattr(os, "O_CLOEXEC", 0))
    try:
        os.fsync(directory_fd)
    finally:
        os.close(directory_fd)


def _hash_path(
    path: Path,
    *,
    expected_fingerprint: tuple[int, int, int, int, int, int] | None = None,
) -> str:
    label = str(path)
    try:
        before = path.lstat()
    except OSError as error:
        _fail(label, f"cannot stat for hashing: {error}")
    if stat.S_ISLNK(before.st_mode) or not stat.S_ISREG(before.st_mode):
        _fail(label, "must be a non-symlink regular file")
    if expected_fingerprint is not None and _fingerprint(before) != expected_fingerprint:
        _fail(label, "pathname no longer identifies the exact pinned file")
    flags = os.O_RDONLY | getattr(os, "O_CLOEXEC", 0) | getattr(os, "O_NOFOLLOW", 0)
    try:
        fd = os.open(path, flags)
    except OSError as error:
        _fail(label, f"cannot open for hashing: {error}")
    try:
        opened = os.fstat(fd)
        if _fingerprint(opened) != _fingerprint(before):
            _fail(label, "changed while being opened for hashing")
        digest = _sha256_fd(fd)
        after = os.fstat(fd)
        path_after = path.lstat()
        if (
            _fingerprint(opened) != _fingerprint(after)
            or _fingerprint(opened) != _fingerprint(path_after)
        ):
            _fail(label, "descriptor or pathname changed while being hashed")
        if (
            expected_fingerprint is not None
            and _fingerprint(path_after) != expected_fingerprint
        ):
            _fail(label, "pathname no longer identifies the exact pinned file")
        return digest
    except OSError as error:
        _fail(label, f"cannot hash exact file: {error}")
    finally:
        os.close(fd)


def _catalog_projection(
    connection: sqlite3.Connection, bindings: Sequence[MediaBinding]
) -> tuple[list[dict[str, Any]], str]:
    projection: list[dict[str, Any]] = []
    for binding in bindings:
        media_id = binding.media_id
        media_rows = connection.execute(
            "SELECT media_id,sha256,byte_count,media_kind,mime_type,container,duration_ms,"
            "ffprobe_json,integrity_state FROM media_objects WHERE media_id=?",
            (media_id,),
        ).fetchall()
        location_rows = connection.execute(
            "SELECT media_location_id,media_id,storage_uri,storage_class,is_primary "
            "FROM media_locations WHERE media_id=? AND storage_class='local_hot_cache' "
            "ORDER BY media_location_id",
            (media_id,),
        ).fetchall()
        rendition_rows = connection.execute(
            "SELECT rendition_id,recording_id,media_id,rendition_kind,label,review_state,metadata_json "
            "FROM renditions WHERE rendition_id=?",
            (binding.rendition_id,),
        ).fetchall()
        recording_rows = connection.execute(
            "SELECT recording_id,canonical_key,slug,title,date_label,date_year,date_basis,duration_ms,"
            "recording_type,review_state,merged_into_recording_id,metadata_json "
            "FROM recordings WHERE recording_id=?",
            (binding.recording_id,),
        ).fetchall()
        source_rows = connection.execute(
            "SELECT source_id,platform,source_kind,native_id,parent_source_id,canonical_url,"
            "historical_url,title,published_at,access_state,review_state,metadata_json "
            "FROM sources WHERE source_id=?",
            (binding.source_id,),
        ).fetchall()
        mapping_rows = connection.execute(
            "SELECT recording_source_id,recording_id,source_id,mapping_role,source_start_ms,"
            "source_end_ms,recording_start_ms,recording_end_ms,mapping_method,confidence_state,"
            "metadata_json FROM recording_sources WHERE recording_id=? AND source_id=? "
            "ORDER BY recording_source_id",
            (binding.recording_id, binding.source_id),
        ).fetchall()
        projection.append(
            {
                "media": [list(row) for row in media_rows],
                "locations": [list(row) for row in location_rows],
                "renditions": [list(row) for row in rendition_rows],
                "recordings": [list(row) for row in recording_rows],
                "sources": [list(row) for row in source_rows],
                "recording_sources": [list(row) for row in mapping_rows],
            }
        )
    digest = hashlib.sha256(_canonical_bytes(projection)).hexdigest()
    return projection, digest


def _mime_and_codecs(row: sqlite3.Row, label: str) -> tuple[str, tuple[str, ...]]:
    try:
        probe = json.loads(row["ffprobe_json"] or "{}")
    except json.JSONDecodeError as error:
        _fail(label, f"invalid ffprobe_json: {error}")
    streams = probe.get("streams", [])
    if not isinstance(streams, list):
        _fail(label, "ffprobe streams must be an array")
    if any(isinstance(item, dict) and item.get("codec_type") == "subtitle" for item in streams):
        _fail(label, "unexpected subtitle stream violates the blinded review boundary")
    codecs = tuple(
        str(item.get("codec_name"))
        for item in streams
        if isinstance(item, dict)
        and item.get("codec_type") in {"video", "audio"}
        and item.get("codec_name")
    )
    video_codecs = {
        str(item.get("codec_name"))
        for item in streams
        if isinstance(item, dict) and item.get("codec_type") == "video"
    }
    container = str(row["container"] or "")
    declared = row["mime_type"]
    if "mp4" in container or declared == "video/mp4":
        return "video/mp4", codecs
    if "webm" in container or "matroska" in container:
        if video_codecs <= {"vp8", "vp9", "av1"}:
            return "video/webm", codecs
        return "video/x-matroska", codecs
    _fail(label, f"unsupported exact parent container {container!r}")


def _open_media_bindings(
    catalog_snapshot: Path,
    template: dict[str, Any],
    proposals: Sequence[dict[str, Any]],
    media_root: Path,
    *,
    guard_writable_media: bool,
) -> tuple[list[MediaBinding], str]:
    _assert_no_symlink_components(media_root.absolute(), "$.media_root")
    root = media_root.absolute()
    proposal_recordings: dict[tuple[str, str], tuple[dict[str, Any], dict[str, Any]]] = {}
    for proposal in proposals:
        for recording in proposal["recordings"]:
            proposal_recordings[(proposal["proposal_id"], recording["recording_id"])] = (
                proposal,
                recording,
            )
    uri = f"file:{urllib.parse.quote(str(catalog_snapshot.absolute()))}?mode=ro&immutable=1"
    connection = sqlite3.connect(uri, uri=True)
    connection.row_factory = sqlite3.Row
    connection.execute("PRAGMA query_only=ON")
    try:
        result = connection.execute("PRAGMA quick_check").fetchall()
        if [row[0] for row in result] != ["ok"]:
            _fail("$.catalog_snapshot", f"quick_check failed: {result!r}")
        bindings: list[MediaBinding] = []
        for ordinal, review_recording in enumerate(template["recordings"], start=1):
            key = (review_recording["proposal_id"], review_recording["recording_id"])
            try:
                proposal, recording = proposal_recordings[key]
            except KeyError:
                _fail(f"$.recordings[{ordinal - 1}]", "cannot resolve proposal recording")
            media_id = recording["parent_media_id"]
            match = MEDIA_ID_RE.fullmatch(media_id)
            if match is None:
                _fail(f"$.recordings[{ordinal - 1}].parent_media_id", "invalid media ID")
            sha256 = match.group(1)
            expected_path = root / sha256[:2] / sha256 / "payload"
            locations = connection.execute(
                "SELECT media_location_id,storage_uri,storage_class,is_primary "
                "FROM media_locations WHERE media_id=? AND storage_class='local_hot_cache'",
                (media_id,),
            ).fetchall()
            if len(locations) != 1:
                _fail(
                    f"$.recordings[{ordinal - 1}].parent_media_id",
                    "requires exactly one local_hot_cache location",
                )
            location = locations[0]
            parsed = urllib.parse.urlsplit(location["storage_uri"])
            if parsed.scheme != "file" or parsed.netloc or parsed.query or parsed.fragment:
                _fail(f"$.recordings[{ordinal - 1}].storage_uri", "must be a local file URI")
            location_path = Path(urllib.parse.unquote(parsed.path))
            if location_path.absolute() != expected_path.absolute():
                _fail(
                    f"$.recordings[{ordinal - 1}].storage_uri",
                    "does not match the content-addressed media-root path",
                )
            _assert_no_symlink_components(location_path, f"$.recordings[{ordinal - 1}].media")
            media_rows = connection.execute(
                "SELECT media_id,sha256,byte_count,mime_type,container,duration_ms,ffprobe_json,integrity_state "
                "FROM media_objects WHERE media_id=?",
                (media_id,),
            ).fetchall()
            if len(media_rows) != 1:
                _fail(f"$.recordings[{ordinal - 1}].parent_media_id", "catalog media row is not unique")
            media = media_rows[0]
            for field, expected in (
                ("sha256", recording["parent_media_sha256"]),
                ("byte_count", recording["parent_media_byte_count"]),
                ("duration_ms", recording["parent_media_duration_ms"]),
            ):
                if media[field] != expected:
                    _fail(f"$.recordings[{ordinal - 1}].{field}", "catalog/proposal mismatch")
            if media["integrity_state"] != "verified":
                _fail(f"$.recordings[{ordinal - 1}].integrity_state", "must be verified")
            source_rows = connection.execute(
                "SELECT s.native_id,COALESCE(r.title,s.title,s.native_id) AS title "
                "FROM recordings r CROSS JOIN sources s "
                "WHERE r.recording_id=? AND s.source_id=?",
                (recording["recording_id"], recording["source_id"]),
            ).fetchall()
            if len(source_rows) != 1:
                _fail(f"$.recordings[{ordinal - 1}]", "catalog source/recording relationship is not unique")
            if source_rows[0]["native_id"] != recording["source_native_id"]:
                _fail(f"$.recordings[{ordinal - 1}].source_native_id", "catalog/proposal mismatch")
            mime_type, codecs = _mime_and_codecs(media, f"$.recordings[{ordinal - 1}].media")
            flags = os.O_RDONLY | getattr(os, "O_CLOEXEC", 0) | getattr(os, "O_NOFOLLOW", 0)
            try:
                fd = os.open(location_path, flags)
            except OSError as error:
                _fail(f"$.recordings[{ordinal - 1}].media", f"cannot open parent: {error}")
            try:
                before = os.fstat(fd)
                if not stat.S_ISREG(before.st_mode):
                    _fail(f"$.recordings[{ordinal - 1}].media", "must be a regular file")
                if before.st_nlink < 1:
                    _fail(f"$.recordings[{ordinal - 1}].media", "must have a live link")
                mode = stat.S_IMODE(before.st_mode)
                if mode & 0o022:
                    _fail(f"$.recordings[{ordinal - 1}].media", "group/world writable media is forbidden")
                writable = bool(mode & 0o200)
                if writable and not guard_writable_media:
                    _fail(
                        f"$.recordings[{ordinal - 1}].media",
                        "owner-writable media requires explicit --guard-writable-media",
                    )
                if before.st_size != recording["parent_media_byte_count"]:
                    _fail(f"$.recordings[{ordinal - 1}].media", "byte count mismatch")
                observed = _sha256_fd(fd)
                after = os.fstat(fd)
                if _fingerprint(before) != _fingerprint(after):
                    _fail(f"$.recordings[{ordinal - 1}].media", "changed while being hashed")
                if observed != sha256:
                    _fail(f"$.recordings[{ordinal - 1}].media", f"digest mismatch; observed {observed}")
                rendition_id = recording.get("parent_rendition_id", recording.get("rendition_id"))
                rendition_kind = recording.get("parent_rendition_kind", recording.get("rendition_kind"))
                opaque_id = secrets.token_urlsafe(24)
                bindings.append(
                    MediaBinding(
                        ordinal=ordinal,
                        candidate_id=recording["candidate_id"],
                        recording_id=recording["recording_id"],
                        source_id=recording["source_id"],
                        source_native_id=recording["source_native_id"],
                        title=source_rows[0]["title"],
                        proposal_id=proposal["proposal_id"],
                        proposal_schema_version=proposal["schema_version"],
                        media_id=media_id,
                        sha256=sha256,
                        byte_count=recording["parent_media_byte_count"],
                        duration_ms=recording["parent_media_duration_ms"],
                        rendition_id=rendition_id,
                        rendition_kind=rendition_kind,
                        media_location_id=location["media_location_id"],
                        storage_uri=location["storage_uri"],
                        storage_class=location["storage_class"],
                        path=location_path,
                        mime_type=mime_type,
                        codecs=codecs,
                        fd=fd,
                        startup_fingerprint=_fingerprint(after),
                        startup_mode=mode,
                        mutability_policy=(
                            "guarded_live_writable_exact_bytes" if writable else "read_only_exact_bytes"
                        ),
                        opaque_id=opaque_id,
                    )
                )
            except Exception:
                os.close(fd)
                raise
        _, projection_sha = _catalog_projection(connection, bindings)
        return bindings, projection_sha
    except Exception:
        for binding in locals().get("bindings", []):
            binding.close()
        raise
    finally:
        connection.close()


def _snapshot_catalog_metadata(path: Path) -> tuple[str, str]:
    """Return migration-ledger and raw snapshot digests for an immutable snapshot."""

    raw_sha = _hash_path(path)
    uri = f"file:{urllib.parse.quote(str(path.absolute()))}?mode=ro&immutable=1"
    connection = sqlite3.connect(uri, uri=True)
    connection.execute("PRAGMA query_only=ON")
    try:
        check = connection.execute("PRAGMA quick_check").fetchall()
        if check != [("ok",)]:
            _fail("$.catalog_snapshot", f"quick_check failed: {check!r}")
        rows = connection.execute(
            "SELECT version,name,sha256 FROM schema_migrations ORDER BY version"
        ).fetchall()
        if not rows:
            _fail("$.catalog_snapshot", "migration ledger must not be empty")
        migration_sha = hashlib.sha256(
            _canonical_bytes([list(row) for row in rows])
        ).hexdigest()
        return migration_sha, raw_sha
    finally:
        connection.close()


def _input_manifest_value(item: PinnedInput) -> dict[str, Any]:
    declared = item.value.get("manifest_sha256")
    if not isinstance(declared, str) or SHA256_RE.fullmatch(declared) is None:
        _fail(f"$.inputs.{item.role}.manifest_sha256", "missing or invalid declared digest")
    return {
        "role": item.role,
        "ordinal": item.ordinal,
        "path": str(item.path),
        "raw_sha256": item.raw_sha256,
        "byte_count": item.byte_count,
        "declared_manifest_sha256": declared,
    }


def _workspace_manifest(
    *,
    created_at: str,
    template_pin: PinnedInput,
    cohort_pin: PinnedInput,
    request_pins: Sequence[PinnedInput],
    proposal_pins: Sequence[PinnedInput],
    catalog_snapshot_sha256: str,
    migration_sha256: str,
    projection_sha256: str,
    bindings: Sequence[MediaBinding],
) -> dict[str, Any]:
    template = template_pin.value
    media_values = [binding.manifest_value() for binding in bindings]
    media_bindings_sha256 = hashlib.sha256(_canonical_bytes(media_values)).hexdigest()
    workspace_id = _stable_id(
        "selection_workspace",
        template["review_id"],
        template["manifest_sha256"],
        media_bindings_sha256,
        WORKSPACE_CONTRACT_REVISION,
    )
    manifest: dict[str, Any] = {
        "schema_version": 1,
        "manifest_kind": "interval_selection_workspace",
        "manifest_sha256": "0" * 64,
        "workspace_id": workspace_id,
        "contract_revision": WORKSPACE_CONTRACT_REVISION,
        "created_at": created_at,
        "review_id": template["review_id"],
        "template_manifest_sha256": template["manifest_sha256"],
        "template_raw_sha256": template_pin.raw_sha256,
        "cohort_id": template["cohort_id"],
        "cohort_manifest_sha256": template["cohort_manifest_sha256"],
        "proposal_inputs": template["proposal_inputs"],
        "input_files": [
            _input_manifest_value(cohort_pin),
            *[_input_manifest_value(item) for item in request_pins],
            *[_input_manifest_value(item) for item in proposal_pins],
            _input_manifest_value(template_pin),
        ],
        "catalog_snapshot": {
            "snapshot_raw_sha256": catalog_snapshot_sha256,
            "schema_migrations_sha256": migration_sha256,
            "identity_projection_sha256": projection_sha256,
        },
        "media_bindings_sha256": media_bindings_sha256,
        "media_bindings": media_values,
        "privacy": {
            "storage_policy": "private_only",
            "publication_authority": "none",
            "os_confidentiality_claimed": False,
        },
    }
    manifest["manifest_sha256"] = canonical_manifest_sha256(manifest)
    return manifest


def _load_private_json(path: Path, *, expected_mode: int | None = None) -> dict[str, Any]:
    body = _load_private_bytes(
        path, expected_mode=expected_mode, maximum_bytes=MAX_JSON_BYTES
    )
    return _strict_json_bytes(body, str(path))


def _load_private_bytes(
    path: Path,
    *,
    expected_mode: int | None,
    maximum_bytes: int = MAX_JSON_BYTES,
) -> bytes:
    try:
        before = path.lstat()
    except OSError as error:
        _fail(str(path), f"cannot stat private file: {error}")
    if stat.S_ISLNK(before.st_mode) or not stat.S_ISREG(before.st_mode):
        _fail(str(path), "must be a non-symlink regular file")
    if (
        before.st_uid != os.geteuid()
        or before.st_nlink != 1
        or (
            expected_mode is not None
            and stat.S_IMODE(before.st_mode) != expected_mode
        )
    ):
        mode_label = "owner-only" if expected_mode is None else f"mode-{expected_mode:04o}"
        _fail(
            str(path),
            f"must be a current-user-owned, single-link {mode_label} file",
        )
    if before.st_size > maximum_bytes:
        _fail(str(path), f"exceeds {maximum_bytes} bytes")
    flags = os.O_RDONLY | getattr(os, "O_CLOEXEC", 0) | getattr(os, "O_NOFOLLOW", 0)
    try:
        descriptor = os.open(path, flags)
    except OSError as error:
        _fail(str(path), f"cannot open private file: {error}")
    try:
        opened = os.fstat(descriptor)
        if _fingerprint(opened) != _fingerprint(before):
            _fail(str(path), "changed while being opened")
        body = os.pread(descriptor, opened.st_size + 1, 0)
        after = os.fstat(descriptor)
        path_after = path.lstat()
        if (
            len(body) != opened.st_size
            or _fingerprint(opened) != _fingerprint(after)
            or _fingerprint(opened) != _fingerprint(path_after)
        ):
            _fail(str(path), "changed while being read")
        return body
    except OSError as error:
        _fail(str(path), f"cannot read private file: {error}")
    finally:
        os.close(descriptor)


def _pin_ui_assets(root: Path) -> dict[str, dict[str, Any]]:
    """Load the fixed local UI once through race-resistant descriptors."""

    absolute = root.absolute()
    _assert_no_symlink_components(absolute, "$.review_ui")
    assets: dict[str, dict[str, Any]] = {}
    for name, content_type in UI_ASSET_TYPES.items():
        path = absolute / name
        label = f"$.review_ui.{name}"
        try:
            before = path.lstat()
        except OSError as error:
            _fail(label, f"cannot stat UI asset: {error}")
        if stat.S_ISLNK(before.st_mode) or not stat.S_ISREG(before.st_mode):
            _fail(label, "must be a non-symlink regular file")
        if before.st_nlink != 1 or stat.S_IMODE(before.st_mode) & 0o022:
            _fail(label, "must be single-link and not group/world writable")
        if before.st_size > MAX_UI_ASSET_BYTES:
            _fail(label, f"exceeds {MAX_UI_ASSET_BYTES} bytes")
        flags = (
            os.O_RDONLY
            | getattr(os, "O_CLOEXEC", 0)
            | getattr(os, "O_NOFOLLOW", 0)
        )
        descriptor: int | None = None
        try:
            descriptor = os.open(path, flags)
            opened = os.fstat(descriptor)
            if _fingerprint(opened) != _fingerprint(before):
                _fail(label, "changed while being opened")
            body = os.pread(descriptor, opened.st_size + 1, 0)
            after = os.fstat(descriptor)
            path_after = path.lstat()
        except OSError as error:
            _fail(label, f"cannot read UI asset: {error}")
        finally:
            if descriptor is not None:
                os.close(descriptor)
        if (
            len(body) != opened.st_size
            or _fingerprint(after) != _fingerprint(opened)
            or _fingerprint(path_after) != _fingerprint(opened)
        ):
            _fail(label, "changed while being read")
        assets[name] = {
            "body": body,
            "content_type": content_type,
            "sha256": hashlib.sha256(body).hexdigest(),
        }
    return assets


def _verify_workspace_manifest(value: object, expected: dict[str, Any]) -> dict[str, Any]:
    if not isinstance(value, dict):
        _fail("$.workspace_manifest", "must be an object")
    if set(value) != set(expected):
        _fail("$.workspace_manifest", "has missing or unexpected fields")
    declared = value.get("manifest_sha256")
    if not isinstance(declared, str) or SHA256_RE.fullmatch(declared) is None:
        _fail("$.workspace_manifest.manifest_sha256", "invalid digest")
    observed = canonical_manifest_sha256(value)
    if declared != observed:
        _fail("$.workspace_manifest.manifest_sha256", f"digest mismatch; expected {observed}")
    if value != expected:
        _fail("$.workspace_manifest", "does not match the exact current pinned inputs")
    return value


class ReviewWorkspace:
    """Own one private, locked review workspace and its exact media descriptors."""

    def __init__(
        self,
        *,
        cohort_path: Path,
        catalog_path: Path,
        request_paths: Sequence[Path],
        proposal_paths: Sequence[Path],
        template_path: Path,
        media_root: Path,
        workspace_path: Path,
        guard_writable_media: bool,
    ):
        if len(request_paths) != 2 or len(proposal_paths) != 2:
            _fail("$.proposal_inputs", "requires exactly two request/proposal pairs")
        self.paths = WorkspacePaths.under(
            _assert_private_directory(workspace_path, create=True)
        )
        self.lock = WorkspaceLock(self.paths.lock)
        self._mutex = threading.RLock()
        self._draft_state_unavailable = False
        self.pins: list[PinnedInput] = []
        self.media: list[MediaBinding] = []
        self.closed = False
        try:
            self.cohort_pin = PinnedInput.open_json("cohort", None, cohort_path)
            self.pins.append(self.cohort_pin)
            self.request_pins = [
                PinnedInput.open_json("request", index, path)
                for index, path in enumerate(request_paths, start=1)
            ]
            self.pins.extend(self.request_pins)
            self.proposal_pins = [
                PinnedInput.open_json("proposal", index, path)
                for index, path in enumerate(proposal_paths, start=1)
            ]
            self.pins.extend(self.proposal_pins)
            self.template_pin = PinnedInput.open_json("template", None, template_path)
            self.pins.append(self.template_pin)
            _copy_catalog_snapshot(catalog_path.absolute(), self.paths.catalog_snapshot)
            snapshot_info = self.paths.catalog_snapshot.lstat()
            if (
                stat.S_ISLNK(snapshot_info.st_mode)
                or not stat.S_ISREG(snapshot_info.st_mode)
                or snapshot_info.st_uid != os.geteuid()
                or snapshot_info.st_nlink != 1
                or stat.S_IMODE(snapshot_info.st_mode) != 0o400
            ):
                _fail(
                    "$.catalog_snapshot",
                    "must be a current-user-owned, single-link mode-0400 regular file",
                )
            self.catalog_snapshot_fingerprint = _fingerprint(snapshot_info)
            self.catalog_migration_sha256, self.catalog_snapshot_sha256 = (
                _snapshot_catalog_metadata(self.paths.catalog_snapshot)
            )
            self.requests = [item.value for item in self.request_pins]
            self.proposals = [item.value for item in self.proposal_pins]
            self.template = validate_interval_selection_review(
                self.template_pin.value,
                self.cohort_pin.value,
                self.requests,
                self.proposals,
                self.paths.catalog_snapshot,
                require_completed=False,
            )
            if self.template["review_state"] != "incomplete_template":
                _fail("$.template.review_state", "workspace requires an incomplete template")
            self.media, self.catalog_projection_sha256 = _open_media_bindings(
                self.paths.catalog_snapshot,
                self.template,
                self.proposals,
                media_root,
                guard_writable_media=guard_writable_media,
            )
            created_at = _utc_now()
            if self.paths.manifest.exists():
                existing = _load_private_json(self.paths.manifest, expected_mode=0o400)
                existing_created = existing.get("created_at")
                if not isinstance(existing_created, str):
                    _fail("$.workspace_manifest.created_at", "must be a UTC timestamp string")
                created_at = existing_created
            expected_manifest = _workspace_manifest(
                created_at=created_at,
                template_pin=self.template_pin,
                cohort_pin=self.cohort_pin,
                request_pins=self.request_pins,
                proposal_pins=self.proposal_pins,
                catalog_snapshot_sha256=self.catalog_snapshot_sha256,
                migration_sha256=self.catalog_migration_sha256,
                projection_sha256=self.catalog_projection_sha256,
                bindings=self.media,
            )
            if self.paths.manifest.exists():
                self.manifest = _verify_workspace_manifest(existing, expected_manifest)
            else:
                _atomic_write_json(
                    self.paths.manifest,
                    expected_manifest,
                    mode=0o400,
                    replace=False,
                )
                self.manifest = expected_manifest
            self._load_or_create_draft()
            if self.draft["lifecycle"] == "finalizing":
                self._complete_finalization()
            elif self.draft["lifecycle"] == "finalized":
                self._verify_finalized_output()
        except Exception:
            self.close()
            raise

    @property
    def workspace_id(self) -> str:
        return self.manifest["workspace_id"]

    def _require_draft_state(self) -> None:
        if getattr(self, "_draft_state_unavailable", False):
            _fail(
                "$.draft",
                "persistence state is indeterminate; restart the workspace",
            )

    def _save_draft_candidate(self, candidate: dict[str, Any]) -> None:
        """Persist before publishing, reconciling an after-replace fsync failure."""

        from .review_workspace_state import (
            load_selection_draft,
            save_selection_draft,
        )

        self._require_draft_state()
        try:
            save_selection_draft(self.paths.draft, candidate)
        except IndeterminateDraftCommit as error:
            try:
                persisted = load_selection_draft(
                    self.paths.draft,
                    self.template,
                    self.workspace_id,
                    self.manifest["manifest_sha256"],
                )
            except Exception as recovery_error:
                self._draft_state_unavailable = True
                raise ContractError(
                    "$.draft: indeterminate commit could not be reconciled; "
                    "restart the workspace"
                ) from recovery_error
            self.draft = persisted
            # Even an exact pathname reload cannot prove that the directory entry
            # survived the failed durability barrier.  Keep the reconciled value
            # for diagnostics, but require a fresh process to reopen the workspace
            # before any further state is exposed or mutated.
            self._draft_state_unavailable = True
            if persisted != candidate:
                raise ContractError(
                    "$.draft: indeterminate commit resolved to an unexpected state; "
                    "restart the workspace"
                ) from error
            raise
        self.draft = candidate

    def _load_or_create_draft(self) -> None:
        from .review_workspace_state import (
            create_selection_draft,
            load_selection_draft,
        )

        arguments = (
            self.template,
            self.workspace_id,
            self.manifest["manifest_sha256"],
        )
        if self.paths.draft.exists():
            self.draft = load_selection_draft(self.paths.draft, *arguments)
        else:
            candidate = create_selection_draft(*arguments, _utc_now())
            self._save_draft_candidate(candidate)

    def reverify_exact_inputs(self) -> None:
        for item in self.pins:
            item.reverify()
        if (
            _hash_path(
                self.paths.catalog_snapshot,
                expected_fingerprint=self.catalog_snapshot_fingerprint,
            )
            != self.catalog_snapshot_sha256
        ):
            _fail("$.catalog_snapshot", "snapshot bytes changed after initialization")
        for binding in self.media:
            binding.assert_unchanged(rehash=True)
        # A replacement can occur after an early item finishes hashing while later
        # multi-gigabyte parents are still being read.  One cheap, bounded identity
        # sweep closes that whole-pass window without hashing every input twice.
        for item in self.pins:
            item.assert_identity()
        _assert_path_fingerprint(
            self.paths.catalog_snapshot,
            self.catalog_snapshot_fingerprint,
            "$.catalog_snapshot",
        )
        for binding in self.media:
            binding.assert_unchanged(rehash=False)

    def close(self) -> None:
        if self.closed:
            return
        self.closed = True
        for binding in self.media:
            binding.close()
        for item in self.pins:
            item.close()
        if hasattr(self, "lock"):
            self.lock.close()

    def __enter__(self) -> "ReviewWorkspace":
        return self

    def __exit__(self, *_: object) -> None:
        self.close()

    def progress(self) -> dict[str, Any]:
        from .review_workspace_state import draft_readiness

        with self._mutex:
            self._require_draft_state()
            return draft_readiness(self.draft, self.template)

    def apply_operation(self, expected_revision: int, operation: object) -> None:
        from .review_workspace_state import apply_draft_operation

        with self._mutex:
            self._require_draft_state()
            if self.draft["revision"] != expected_revision:
                raise StaleRevision(self.draft["revision"])
            if not isinstance(operation, dict):
                _fail("$.operation", "must be an object")
            if "kind" not in operation or "operation" in operation:
                _fail("$.operation.kind", "must name one typed operation")
            internal_operation = dict(operation)
            internal_operation["operation"] = internal_operation.pop("kind")
            candidate = apply_draft_operation(
                self.draft,
                self.template,
                expected_revision,
                internal_operation,
                _utc_now(),
            )
            self._save_draft_candidate(candidate)

    def _state_recording_map(self) -> dict[str, dict[str, Any]]:
        return {row["recording_id"]: row for row in self.draft["recordings"]}

    def bootstrap(self, *, prefix: str, csrf_token: str) -> dict[str, Any]:
        with self._mutex:
            self._require_draft_state()
            if self.draft["lifecycle"] == "finalized":
                self._verify_finalized_output()
            return self._bootstrap_locked(prefix=prefix, csrf_token=csrf_token)

    def _bootstrap_locked(self, *, prefix: str, csrf_token: str) -> dict[str, Any]:
        state_recordings = self._state_recording_map()
        media_by_recording = {row.recording_id: row for row in self.media}
        output_recordings: list[dict[str, Any]] = []
        for template_recording in self.template["recordings"]:
            state_recording = state_recordings[template_recording["recording_id"]]
            media = media_by_recording[template_recording["recording_id"]]
            state_intervals = {
                item["selection_decision_id"]: item
                for item in state_recording["intervals"]
            }
            intervals: list[dict[str, Any]] = []
            for template_interval in template_recording["intervals"]:
                state_interval = state_intervals[
                    template_interval["selection_decision_id"]
                ]
                row = dict(state_interval)
                coverage_ranges = row.pop("coverage_ranges")
                row.update(
                    proposal_interval_id=template_interval["proposal_interval_id"],
                    proposal_start_ms=template_interval["proposal_start_ms"],
                    proposal_end_ms=template_interval["proposal_end_ms"],
                    playback_coverage_ranges=coverage_ranges,
                    playback_coverage_ms=sum(
                        item["end_ms"] - item["start_ms"]
                        for item in coverage_ranges
                    ),
                    required_playback_coverage_ms=max(
                        1,
                        template_interval["proposal_end_ms"]
                        - template_interval["proposal_start_ms"]
                        - REQUIRED_COVERAGE_TOLERANCE_MS,
                    ),
                )
                intervals.append(row)
            output_recordings.append(
                {
                    "candidate_id": template_recording["candidate_id"],
                    "recording_id": template_recording["recording_id"],
                    "source_native_id": media.source_native_id,
                    "title": media.title,
                    "proposal_id": template_recording["proposal_id"],
                    "proposal_schema_version": template_recording[
                        "proposal_schema_version"
                    ],
                    "split": state_recording["split"],
                    "media": {
                        "id": media.media_id,
                        "url": f"{prefix}media/{media.opaque_id}",
                        "mime_type": media.mime_type,
                        "byte_count": media.byte_count,
                        "duration_ms": media.duration_ms,
                        "sha256": media.sha256,
                        "verified": True,
                        "mutability_policy": media.mutability_policy,
                        "codecs": list(media.codecs),
                    },
                    "intervals": intervals,
                }
            )
        lifecycle = self.draft["lifecycle"]
        readiness = self.progress()
        state = (
            "completed"
            if lifecycle == "finalized"
            else "invalid"
            if lifecycle == "invalidated"
            else "ready"
            if (
                readiness["ready_to_begin_finalization"]
                or readiness["ready_to_materialize"]
            )
            else "editing"
        )
        return {
            "schema_version": 1,
            "workspace_id": self.workspace_id,
            "review_id": self.template["review_id"],
            "state": state,
            "revision": self.draft["revision"],
            "csrf_token": csrf_token,
            "privacy": {
                "storage_policy": "private_only",
                "publication_authority": "none",
                "asr_or_reference_data_available": False,
            },
            "minimum_accepted_duration_ms": MINIMUM_ACCEPTED_DURATION_MS,
            "progress": readiness,
            "recordings": output_recordings,
            "completed_manifest_sha256": self.draft.get(
                "finalization_manifest_sha256"
            ),
        }

    def finalize(self, expected_revision: int, payload: object) -> None:
        from .review_workspace_state import (
            begin_draft_finalization,
            draft_readiness,
        )

        if not isinstance(payload, dict):
            _fail("$.finalize", "must be an object")
        exact_keys = {
            "expected_revision",
            "reviewer_id",
            "direct_parent_media_reviewed",
            "asr_outputs_inspected",
            "reference_text_inspected",
            "selection_basis",
        }
        if set(payload) != exact_keys:
            _fail("$.finalize", "has missing or unexpected fields")
        if payload["expected_revision"] != expected_revision:
            _fail("$.finalize.expected_revision", "does not match request revision")
        reviewer_id = payload["reviewer_id"]
        if not isinstance(reviewer_id, str) or ID_RE.fullmatch(reviewer_id) is None:
            _fail("$.finalize.reviewer_id", "must be a pseudonymous identifier")
        constants = {
            "direct_parent_media_reviewed": True,
            "asr_outputs_inspected": False,
            "reference_text_inspected": False,
            "selection_basis": "source_metadata_and_direct_parent_media_only",
        }
        for key, expected in constants.items():
            if payload[key] != expected or type(payload[key]) is not type(expected):
                _fail(f"$.finalize.{key}", f"must equal {expected!r}")
        with self._mutex:
            self._require_draft_state()
            if self.draft["lifecycle"] == "finalized":
                intent = self.draft["finalization_intent"]
                if intent["reviewer_id"] != reviewer_id:
                    _fail("$.finalize.reviewer_id", "does not match finalized reviewer intent")
                self._verify_finalized_output()
                return
            if self.draft["revision"] != expected_revision:
                raise StaleRevision(self.draft["revision"])
            if self.draft["lifecycle"] == "invalidated":
                _fail("$.draft.lifecycle", "exact inputs changed; create a new workspace")
            if self.draft["lifecycle"] == "finalizing":
                intent = self.draft["finalization_intent"]
                if intent["reviewer_id"] != reviewer_id:
                    _fail("$.finalize.reviewer_id", "does not match persisted reviewer intent")
                self._complete_finalization()
                return
            if self.draft["lifecycle"] != "draft":
                _fail("$.draft.lifecycle", "is not editable")
            readiness = draft_readiness(self.draft, self.template)
            if not readiness["ready_to_begin_finalization"]:
                _fail("$.draft", "is not ready for attestation")
            timestamp = _utc_now()
            candidate = begin_draft_finalization(
                self.draft,
                self.template,
                expected_revision,
                reviewer_id,
                timestamp,
                timestamp,
                timestamp,
            )
            # This durable transition is the crash-recovery boundary.  No external
            # verification or output write occurs before the reviewer intent lands.
            self._save_draft_candidate(candidate)
            self._complete_finalization()

    def _invalidate_after_exact_input_failure(self) -> None:
        from .review_workspace_state import mark_draft_invalidated

        if self.draft["lifecycle"] not in {"draft", "finalizing"}:
            return
        candidate = mark_draft_invalidated(
            self.draft,
            self.template,
            self.draft["revision"],
            _utc_now(),
        )
        self._save_draft_candidate(candidate)

    def _completed_review_bytes(self) -> bytes:
        return _load_private_bytes(self.paths.completed_review, expected_mode=0o400)

    def _build_finalization_receipt(
        self, review: dict[str, Any]
    ) -> dict[str, Any]:
        intent = self.draft["finalization_intent"]
        receipt: dict[str, Any] = {
            "schema_version": 1,
            "manifest_kind": "interval_selection_finalization_receipt",
            "manifest_sha256": "0" * 64,
            "workspace_id": self.workspace_id,
            "workspace_manifest_sha256": self.manifest["manifest_sha256"],
            "review_tool": {"name": REVIEW_TOOL_NAME, "version": REVIEW_TOOL_VERSION},
            "completed_review_id": review["review_id"],
            "completed_review_manifest_sha256": review["manifest_sha256"],
            "catalog_snapshot_sha256": self.catalog_snapshot_sha256,
            "catalog_identity_projection_sha256": self.catalog_projection_sha256,
            "parent_media": [
                {
                    "media_id": row.media_id,
                    "sha256": row.sha256,
                    "byte_count": row.byte_count,
                }
                for row in self.media
            ],
            "finalized_at": intent["begun_at"],
            "privacy": {
                "storage_policy": "private_only",
                "publication_authority": "none",
            },
        }
        receipt["manifest_sha256"] = canonical_manifest_sha256(receipt)
        return receipt

    def _draft_review_recordings(self) -> list[dict[str, Any]]:
        recording_fields = (
            "candidate_id",
            "recording_id",
            "source_id",
            "proposal_id",
            "proposal_schema_version",
            "split",
        )
        decision_fields = (
            "selection_decision_id",
            "proposal_interval_id",
            "proposal_start_ms",
            "proposal_end_ms",
            "decision",
            "accepted_start_ms",
            "accepted_end_ms",
            "adjustment_reason",
            "rejection_reason",
            "flags",
        )
        return [
            {
                **{field: recording[field] for field in recording_fields},
                "intervals": [
                    {field: decision[field] for field in decision_fields}
                    for decision in recording["intervals"]
                ],
            }
            for recording in self.draft["recordings"]
        ]

    def _noncanonical_staging_path(self, role: str, digest: str) -> Path:
        if role not in {"review", "receipt"} or SHA256_RE.fullmatch(digest) is None:
            _fail("$.finalization_staging", "invalid staged-artifact identity")
        return self.paths.root / (
            f".noncanonical-finalization-{role}-{digest}.json"
        )

    def _exact_artifact_exists(
        self,
        path: Path,
        body: bytes,
        *,
        label: str,
        conflict_message: str,
    ) -> bool:
        """Return whether a sealed artifact already exists with the exact bytes."""

        try:
            path.lstat()
        except FileNotFoundError:
            return False
        except OSError as error:
            _fail(label, f"cannot inspect artifact: {error}")
        try:
            existing = _load_private_bytes(path, expected_mode=0o400)
        except ContractError:
            _fail(label, "existing artifact is not an exact private sealed file")
        if existing != body:
            _fail(label, conflict_message)
        return True

    def _write_absent_exact_artifact(
        self,
        path: Path,
        body: bytes,
        *,
        label: str,
        appeared_message: str,
    ) -> None:
        try:
            _atomic_write_bytes(path, body, mode=0o400, replace=False)
        except FileExistsError:
            _fail(label, appeared_message)

    def _ensure_noncanonical_staging_artifact(
        self, path: Path, body: bytes, *, label: str
    ) -> None:
        if self._exact_artifact_exists(
            path,
            body,
            label=label,
            conflict_message="a conflicting noncanonical staging artifact exists",
        ):
            return
        self._write_absent_exact_artifact(
            path,
            body,
            label=label,
            appeared_message="a staging artifact appeared during finalization",
        )

    def _assert_exact_canonical_artifacts(
        self,
        review_body: bytes,
        receipt_body: bytes,
        *,
        phase: str,
    ) -> None:
        """Boundedly reload both canonical files through private descriptors."""

        try:
            closing_review_body = self._completed_review_bytes()
        except ContractError:
            _fail(
                "$.completed_review",
                f"cannot reload the completed review during {phase}",
            )
        if closing_review_body != review_body:
            _fail("$.completed_review", f"changed during {phase}")
        try:
            closing_receipt_body = _load_private_bytes(
                self.paths.receipt, expected_mode=0o400
            )
        except ContractError:
            _fail(
                "$.receipt",
                f"cannot reload the finalization receipt during {phase}",
            )
        if closing_receipt_body != receipt_body:
            _fail("$.receipt", f"changed during {phase}")

    def _verify_finalized_output(self) -> None:
        self._require_draft_state()
        self.reverify_exact_inputs()
        expected = self.draft["finalization_manifest_sha256"]
        try:
            review_body = self._completed_review_bytes()
        except ContractError:
            _fail(
                "$.completed_review",
                "cannot load the exact finalized review",
            )
        value = _strict_json_bytes(review_body, "$.completed_review")
        if value.get("manifest_sha256") != expected:
            _fail("$.completed_review.manifest_sha256", "does not match finalized draft")
        if canonical_manifest_sha256(value) != expected:
            _fail("$.completed_review.manifest_sha256", "completed review digest is invalid")
        if review_body != _canonical_bytes(value) + b"\n":
            _fail("$.completed_review", "must use the canonical sealed serialization")
        value = validate_interval_selection_review(
            value,
            self.cohort_pin.value,
            self.requests,
            self.proposals,
            self.paths.catalog_snapshot,
            require_completed=True,
        )
        intent = self.draft["finalization_intent"]
        reviewer = value["reviewer"]
        for field in ("reviewer_id", "reviewed_at", "attested_at"):
            if reviewer[field] != intent[field]:
                _fail(
                    f"$.completed_review.reviewer.{field}",
                    "does not match the durable finalization intent",
                )
        if reviewer["review_tool"] != {
            "name": REVIEW_TOOL_NAME,
            "version": REVIEW_TOOL_VERSION,
        }:
            _fail("$.completed_review.reviewer.review_tool", "does not match this tool")
        if value["recordings"] != self._draft_review_recordings():
            _fail(
                "$.completed_review.recordings",
                "does not match the finalized draft decisions and splits",
            )
        try:
            receipt_body = _load_private_bytes(
                self.paths.receipt, expected_mode=0o400
            )
        except ContractError:
            _fail("$.receipt", "cannot load the exact finalization receipt")
        receipt = _strict_json_bytes(receipt_body, "$.receipt")
        if receipt_body != _canonical_bytes(receipt) + b"\n":
            _fail("$.receipt", "must use the canonical sealed serialization")
        if receipt.get("manifest_sha256") != canonical_manifest_sha256(receipt):
            _fail("$.receipt.manifest_sha256", "finalization receipt digest is invalid")
        if receipt != self._build_finalization_receipt(value):
            _fail("$.receipt", "does not bind the exact finalized review and workspace")
        # This is a point-in-time integrity guard, not an OS immutability claim.
        # A same-UID actor can still mutate files after the closing check; the next
        # access will fail its own opening/closing checks.
        self.reverify_exact_inputs()
        # The media rehash can be long.  Converge once by exact-reloading both
        # canonical artifacts after it, so a replacement during that closing pass
        # cannot be reported as completed.  This is deliberately bounded rather
        # than an unbounded retry loop; same-UID post-check mutation remains outside
        # the POSIX-permission integrity guarantee and is checked on next access.
        self._assert_exact_canonical_artifacts(
            review_body,
            receipt_body,
            phase="finalized verification",
        )

    def _complete_finalization(self) -> None:
        """Resume or complete one already-persisted finalization intent."""

        from .review_workspace_state import (
            draft_readiness,
            mark_draft_finalized,
            materialize_completed_review,
        )

        self._require_draft_state()
        if self.draft["lifecycle"] != "finalizing":
            _fail("$.draft.lifecycle", "must be finalizing")
        if not draft_readiness(self.draft, self.template)["ready_to_materialize"]:
            _fail("$.draft", "persisted finalization is no longer materializable")
        try:
            self.reverify_exact_inputs()
        except ContractError:
            self._invalidate_after_exact_input_failure()
            raise
        intent = self.draft["finalization_intent"]
        review = materialize_completed_review(
            self.template,
            self.draft,
            intent["reviewer_id"],
            intent["reviewed_at"],
            intent["attested_at"],
        )
        try:
            review = validate_interval_selection_review(
                review,
                self.cohort_pin.value,
                self.requests,
                self.proposals,
                self.paths.catalog_snapshot,
                require_completed=True,
            )
        except ContractError as validation_error:
            try:
                self.reverify_exact_inputs()
            except ContractError:
                self._invalidate_after_exact_input_failure()
                raise
            # The persisted reviewer intent remains recoverable. A validator or
            # materialization regression is not evidence that source bytes changed.
            raise validation_error
        try:
            self.reverify_exact_inputs()
        except ContractError:
            self._invalidate_after_exact_input_failure()
            raise
        body = _canonical_bytes(review) + b"\n"
        receipt = self._build_finalization_receipt(review)
        receipt_body = _canonical_bytes(receipt) + b"\n"

        # Staging files are deterministic, mode-0400, and live only under the
        # private mode-0700 workspace.  They are deliberately dot-prefixed and
        # noncanonical, so a crash or failed input check cannot publish a review.
        staged_review = self._noncanonical_staging_path(
            "review", review["manifest_sha256"]
        )
        staged_receipt = self._noncanonical_staging_path(
            "receipt", receipt["manifest_sha256"]
        )
        self._ensure_noncanonical_staging_artifact(
            staged_review,
            body,
            label="$.finalization_staging.review",
        )
        self._ensure_noncanonical_staging_artifact(
            staged_receipt,
            receipt_body,
            label="$.finalization_staging.receipt",
        )

        output_exists = self._exact_artifact_exists(
            self.paths.completed_review,
            body,
            label="$.completed_review",
            conflict_message="a conflicting completed review already exists",
        )
        receipt_exists = self._exact_artifact_exists(
            self.paths.receipt,
            receipt_body,
            label="$.receipt",
            conflict_message="a conflicting finalization receipt exists",
        )

        # This check occurs after *both* staged artifacts and all canonical
        # conflict reads.  Mutation during validation/staging cannot create a
        # canonical completed review or receipt.
        try:
            self.reverify_exact_inputs()
        except ContractError:
            self._invalidate_after_exact_input_failure()
            raise

        if not receipt_exists:
            self._write_absent_exact_artifact(
                self.paths.receipt,
                receipt_body,
                label="$.receipt",
                appeared_message="receipt appeared during finalization",
            )

        # The canonical review is the last publication boundary.  Rehash after
        # publishing the private receipt and immediately before the review write.
        # A malicious same-UID process can still race this final check/write pair;
        # POSIX permissions do not provide conditional publish-by-inode.  We make
        # no stronger immutability claim, and finalized access rechecks the inputs.
        try:
            self.reverify_exact_inputs()
        except ContractError:
            self._invalidate_after_exact_input_failure()
            raise
        if not output_exists:
            self._write_absent_exact_artifact(
                self.paths.completed_review,
                body,
                label="$.completed_review",
                appeared_message="output appeared during finalization",
            )
        # A bounded descriptor/path reload closes replacements made during either
        # canonical publish before the irreversible finalized lifecycle marker.
        self._assert_exact_canonical_artifacts(
            body,
            receipt_body,
            phase="finalization publish",
        )
        revision = self.draft["revision"]
        candidate = mark_draft_finalized(
            self.draft,
            self.template,
            revision,
            review["manifest_sha256"],
            intent["begun_at"],
        )
        self._save_draft_candidate(candidate)


class StaleRevision(ContractError):
    def __init__(self, current_revision: int):
        super().__init__(f"$.expected_revision: stale; current revision is {current_revision}")
        self.current_revision = current_revision


def _parse_single_range(value: str, size: int) -> tuple[int, int]:
    """Parse one RFC 7233 byte range and return an inclusive range."""

    if isinstance(size, bool) or not isinstance(size, int) or size <= 0:
        raise ValueError("representation is empty")
    if not value.startswith("bytes=") or "," in value:
        raise ValueError("only one bytes range is supported")
    specification = value[6:]
    if specification.count("-") != 1 or any(character.isspace() for character in specification):
        raise ValueError("malformed byte range")
    start_text, end_text = specification.split("-", 1)
    if not start_text:
        if not end_text.isascii() or not end_text.isdigit():
            raise ValueError("malformed suffix range")
        suffix = int(end_text)
        if suffix <= 0:
            raise ValueError("suffix range must be positive")
        start = max(0, size - suffix)
        return start, size - 1
    if not start_text.isascii() or not start_text.isdigit():
        raise ValueError("malformed range start")
    start = int(start_text)
    if start >= size:
        raise ValueError("range starts beyond end of file")
    if not end_text:
        return start, size - 1
    if not end_text.isascii() or not end_text.isdigit():
        raise ValueError("malformed range end")
    end = int(end_text)
    if end < start:
        raise ValueError("range end precedes start")
    return start, min(end, size - 1)


class _LoopbackHTTPServer(ThreadingHTTPServer):
    address_family = socket.AF_INET
    allow_reuse_address = False
    daemon_threads = True


class ReviewWorkspaceHTTP:
    """Hardened same-origin HTTP facade over :class:`ReviewWorkspace`."""

    def __init__(self, workspace: ReviewWorkspace, *, port: int = 0):
        if isinstance(port, bool) or not isinstance(port, int) or not 0 <= port <= 65535:
            _fail("$.port", "must be an integer from 0 through 65535")
        self.workspace = workspace
        self.bootstrap_token = secrets.token_urlsafe(32)
        self.bootstrap_available = True
        self.session_cookie = secrets.token_urlsafe(32)
        self.csrf_token = secrets.token_urlsafe(32)
        self.route_token = secrets.token_urlsafe(32)
        self.prefix = f"/w/{self.route_token}/"
        self._secret_lock = threading.Lock()
        self.ui_assets = _pin_ui_assets(
            Path(__file__).resolve().parent / "review_ui"
        )
        handler_type = self._handler_type()
        self.server = _LoopbackHTTPServer(("127.0.0.1", port), handler_type)
        self.port = int(self.server.server_address[1])
        self.origin = f"http://127.0.0.1:{self.port}"
        self.host_header = f"127.0.0.1:{self.port}"
        self.bootstrap_url = f"{self.origin}/bootstrap/{self.bootstrap_token}"

    def _handler_type(self) -> type[BaseHTTPRequestHandler]:
        app = self

        class Handler(BaseHTTPRequestHandler):
            protocol_version = "HTTP/1.1"
            server_version = ""
            sys_version = ""

            def version_string(self) -> str:
                return ""

            def log_message(self, _format: str, *_args: object) -> None:
                return

            def _common_headers(self, *, media: bool = False) -> None:
                self.send_header("Connection", "close")
                self.send_header("Cache-Control", "no-store, private, max-age=0")
                self.send_header("Pragma", "no-cache")
                self.send_header("Expires", "0")
                self.send_header("Referrer-Policy", "no-referrer")
                self.send_header("X-Content-Type-Options", "nosniff")
                self.send_header("X-Frame-Options", "DENY")
                self.send_header("Cross-Origin-Opener-Policy", "same-origin")
                self.send_header("Cross-Origin-Resource-Policy", "same-origin")
                self.send_header(
                    "Content-Security-Policy",
                    "default-src 'none'; media-src 'self'; script-src 'self'; "
                    "style-src 'self'; connect-src 'self'; img-src 'self'; "
                    "base-uri 'none'; form-action 'self'; frame-ancestors 'none'; "
                    "object-src 'none'",
                )
                if media:
                    self.send_header("Accept-Ranges", "bytes")

            def _send_body(
                self,
                status: int,
                body: bytes,
                content_type: str,
                *,
                head: bool = False,
                extra_headers: Iterable[tuple[str, str]] = (),
                media: bool = False,
            ) -> None:
                self.send_response(status)
                self._common_headers(media=media)
                self.send_header("Content-Type", content_type)
                self.send_header("Content-Length", str(len(body)))
                for key, value in extra_headers:
                    self.send_header(key, value)
                self.end_headers()
                if not head and body:
                    self.wfile.write(body)

            def _json(
                self,
                status: int,
                value: dict[str, Any],
                *,
                head: bool = False,
            ) -> None:
                self._send_body(
                    status,
                    _canonical_bytes(value),
                    "application/json; charset=utf-8",
                    head=head,
                )

            def _safe_error(self, status: int, message: str = "request rejected") -> None:
                self.close_connection = True
                self._json(
                    status,
                    {"error": "request_rejected", "message": message},
                    head=self.command == "HEAD",
                )

            def send_error(
                self,
                code: int,
                message: str | None = None,
                explain: str | None = None,
            ) -> None:
                del message, explain
                self._safe_error(code)

            def _valid_host(self) -> bool:
                values = self.headers.get_all("Host", failobj=[])
                return len(values) == 1 and hmac.compare_digest(values[0], app.host_header)

            def _cookie_authenticated(self) -> bool:
                values = self.headers.get_all("Cookie", failobj=[])
                if len(values) != 1:
                    return False
                try:
                    cookie = SimpleCookie()
                    cookie.load(values[0])
                except Exception:
                    return False
                morsel = cookie.get("himr_review_session")
                return morsel is not None and hmac.compare_digest(
                    morsel.value, app.session_cookie
                )

            def _authenticated(self) -> bool:
                return self._valid_host() and self._cookie_authenticated()

            def _valid_mutation_headers(self) -> bool:
                if not self._authenticated():
                    return False
                origins = self.headers.get_all("Origin", failobj=[])
                csrf_values = self.headers.get_all("X-HIMR-CSRF", failobj=[])
                fetch_sites = self.headers.get_all("Sec-Fetch-Site", failobj=[])
                if len(origins) != 1 or not hmac.compare_digest(origins[0], app.origin):
                    return False
                if len(csrf_values) != 1 or not hmac.compare_digest(
                    csrf_values[0], app.csrf_token
                ):
                    return False
                if fetch_sites and (
                    len(fetch_sites) != 1 or fetch_sites[0] != "same-origin"
                ):
                    return False
                return True

            def _request_path(self) -> str | None:
                if not self.path.startswith("/") or self.path.startswith("//"):
                    return None
                if "?" in self.path or "#" in self.path or "%" in self.path or "\\" in self.path:
                    return None
                return self.path

            def _valid_bodyless_framing(self) -> bool:
                if self.headers.get_all("Transfer-Encoding", failobj=[]):
                    return False
                lengths = self.headers.get_all("Content-Length", failobj=[])
                if not lengths:
                    return True
                if (
                    len(lengths) != 1
                    or len(lengths[0]) > 20
                    or not lengths[0].isascii()
                    or not lengths[0].isdigit()
                ):
                    return False
                try:
                    return int(lengths[0]) == 0
                except ValueError:
                    return False

            def _bootstrap(self, path: str, *, head: bool) -> bool:
                expected = f"/bootstrap/{app.bootstrap_token}"
                if path != expected:
                    return False
                if head:
                    self._safe_error(HTTPStatus.METHOD_NOT_ALLOWED)
                    return True
                with app._secret_lock:
                    if not app.bootstrap_available:
                        self._safe_error(HTTPStatus.GONE, "bootstrap URL has expired")
                        return True
                    app.bootstrap_available = False
                self.send_response(HTTPStatus.SEE_OTHER)
                self._common_headers()
                self.send_header("Content-Length", "0")
                self.send_header("Location", app.prefix)
                self.send_header(
                    "Set-Cookie",
                    f"himr_review_session={app.session_cookie}; Path={app.prefix}; "
                    "HttpOnly; SameSite=Strict",
                )
                self.end_headers()
                return True

            def _static(self, path: str, *, head: bool) -> bool:
                static = {
                    app.prefix: "index.html",
                    app.prefix + "app.js": "app.js",
                    app.prefix + "styles.css": "styles.css",
                }
                if path not in static:
                    return False
                if not self._authenticated():
                    self._safe_error(HTTPStatus.FORBIDDEN)
                    return True
                asset = app.ui_assets[static[path]]
                self._send_body(
                    HTTPStatus.OK,
                    asset["body"],
                    asset["content_type"],
                    head=head,
                    extra_headers=(("ETag", f'"sha256-{asset["sha256"]}"'),),
                )
                return True

            def _api_get(self, path: str, *, head: bool) -> bool:
                if path != app.prefix + "api/bootstrap":
                    return False
                if not self._authenticated():
                    self._safe_error(HTTPStatus.FORBIDDEN)
                    return True
                self._json(
                    HTTPStatus.OK,
                    app.workspace.bootstrap(prefix=app.prefix, csrf_token=app.csrf_token),
                    head=head,
                )
                return True

            def _media(self, path: str, *, head: bool) -> bool:
                prefix = app.prefix + "media/"
                if not path.startswith(prefix):
                    return False
                if not self._authenticated():
                    self._safe_error(HTTPStatus.FORBIDDEN)
                    return True
                opaque = path[len(prefix) :]
                matches = [item for item in app.workspace.media if item.opaque_id == opaque]
                if len(matches) != 1:
                    self._safe_error(HTTPStatus.NOT_FOUND)
                    return True
                media = matches[0]
                try:
                    media.assert_unchanged(rehash=False)
                except ContractError:
                    self._safe_error(HTTPStatus.CONFLICT, "exact parent media changed")
                    return True
                ranges = self.headers.get_all("Range", failobj=[])
                if len(ranges) > 1:
                    self._range_not_satisfiable(media)
                    return True
                start, end = 0, media.byte_count - 1
                status = HTTPStatus.OK
                extra: list[tuple[str, str]] = [
                    ("ETag", f'"sha256-{media.sha256}"'),
                ]
                if ranges:
                    try:
                        start, end = _parse_single_range(ranges[0], media.byte_count)
                    except ValueError:
                        self._range_not_satisfiable(media)
                        return True
                    status = HTTPStatus.PARTIAL_CONTENT
                    extra.append(
                        ("Content-Range", f"bytes {start}-{end}/{media.byte_count}")
                    )
                length = end - start + 1
                self.send_response(status)
                self._common_headers(media=True)
                self.send_header("Content-Type", media.mime_type)
                self.send_header("Content-Length", str(length))
                for key, value in extra:
                    self.send_header(key, value)
                self.end_headers()
                if not head:
                    offset = start
                    remaining = length
                    try:
                        while remaining:
                            block = os.pread(
                                media.fd,
                                min(MEDIA_SEND_CHUNK_BYTES, remaining),
                                offset,
                            )
                            if not block:
                                raise OSError("unexpected end of exact media")
                            self.wfile.write(block)
                            offset += len(block)
                            remaining -= len(block)
                    except (BrokenPipeError, ConnectionResetError):
                        return True
                    except OSError:
                        self.close_connection = True
                        return True
                try:
                    media.assert_unchanged(rehash=False)
                except ContractError:
                    self.close_connection = True
                return True

            def _range_not_satisfiable(self, media: MediaBinding) -> None:
                self.send_response(HTTPStatus.REQUESTED_RANGE_NOT_SATISFIABLE)
                self._common_headers(media=True)
                self.send_header("Content-Range", f"bytes */{media.byte_count}")
                self.send_header("Content-Type", "application/json; charset=utf-8")
                body = _canonical_bytes(
                    {"error": "request_rejected", "message": "range not satisfiable"}
                )
                self.send_header("Content-Length", str(len(body)))
                self.end_headers()
                if self.command != "HEAD":
                    self.wfile.write(body)

            def _read_json_body(self) -> dict[str, Any] | None:
                if self.headers.get_all("Transfer-Encoding", failobj=[]):
                    self._safe_error(HTTPStatus.BAD_REQUEST)
                    return None
                content_types = self.headers.get_all("Content-Type", failobj=[])
                lengths = self.headers.get_all("Content-Length", failobj=[])
                if content_types != ["application/json"] or len(lengths) != 1:
                    self._safe_error(HTTPStatus.UNSUPPORTED_MEDIA_TYPE)
                    return None
                if not lengths[0].isascii() or not lengths[0].isdigit():
                    self._safe_error(HTTPStatus.BAD_REQUEST)
                    return None
                if len(lengths[0]) > 20:
                    self._safe_error(HTTPStatus.REQUEST_ENTITY_TOO_LARGE)
                    return None
                try:
                    length = int(lengths[0])
                except ValueError:
                    self._safe_error(HTTPStatus.BAD_REQUEST)
                    return None
                if length <= 0 or length > MAX_POST_BYTES:
                    self._safe_error(HTTPStatus.REQUEST_ENTITY_TOO_LARGE)
                    return None
                body = self.rfile.read(length)
                if len(body) != length:
                    self._safe_error(HTTPStatus.BAD_REQUEST)
                    return None
                try:
                    return _strict_json_bytes(body, "$.request")
                except ContractError:
                    self._safe_error(HTTPStatus.BAD_REQUEST)
                    return None

            def do_GET(self) -> None:
                self._handle_get(head=False)

            def do_HEAD(self) -> None:
                self._handle_get(head=True)

            def _handle_get(self, *, head: bool) -> None:
                self.close_connection = True
                path = self._request_path()
                if (
                    path is None
                    or not self._valid_host()
                    or not self._valid_bodyless_framing()
                ):
                    self._safe_error(HTTPStatus.BAD_REQUEST)
                    return
                if self._bootstrap(path, head=head):
                    return
                if self._static(path, head=head):
                    return
                if self._api_get(path, head=head):
                    return
                if self._media(path, head=head):
                    return
                self._safe_error(HTTPStatus.NOT_FOUND)

            def do_POST(self) -> None:
                path = self._request_path()
                if path is None or not self._valid_mutation_headers():
                    self._safe_error(HTTPStatus.FORBIDDEN)
                    return
                if path not in {
                    app.prefix + "api/mutate",
                    app.prefix + "api/finalize",
                }:
                    self._safe_error(HTTPStatus.NOT_FOUND)
                    return
                value = self._read_json_body()
                if value is None:
                    return
                self.close_connection = True
                revision = value.get("expected_revision")
                if isinstance(revision, bool) or not isinstance(revision, int) or revision < 0:
                    self._safe_error(HTTPStatus.BAD_REQUEST)
                    return
                try:
                    if path.endswith("/mutate"):
                        if set(value) != {"expected_revision", "operation"}:
                            _fail("$.request", "has missing or unexpected fields")
                        app.workspace.apply_operation(revision, value["operation"])
                    else:
                        app.workspace.finalize(revision, value)
                except StaleRevision as error:
                    self._json(
                        HTTPStatus.CONFLICT,
                        {
                            "error": "stale_revision",
                            "current_revision": error.current_revision,
                        },
                    )
                    return
                except ContractError as error:
                    message = str(error)
                    if "/" in message or "\\" in message:
                        message = "operation violates the private review contract"
                    self._safe_error(HTTPStatus.UNPROCESSABLE_ENTITY, message[:300])
                    return
                self._json(
                    HTTPStatus.OK,
                    app.workspace.bootstrap(prefix=app.prefix, csrf_token=app.csrf_token),
                )

            def _method_not_allowed(self) -> None:
                self._safe_error(HTTPStatus.METHOD_NOT_ALLOWED)

            do_PUT = _method_not_allowed
            do_PATCH = _method_not_allowed
            do_DELETE = _method_not_allowed
            do_OPTIONS = _method_not_allowed
            do_TRACE = _method_not_allowed
            do_CONNECT = _method_not_allowed

        return Handler

    def serve_forever(self, *, open_browser: bool) -> None:
        print("Private ASR-blind review workspace ready.")
        print(f"Open this one-time loopback URL: {self.bootstrap_url}")
        print("No transcript, ASR, reference, wiki, or remote network data is exposed.")
        if open_browser:
            webbrowser.open(self.bootstrap_url, new=1, autoraise=True)
        try:
            self.server.serve_forever(poll_interval=0.25)
        finally:
            self.server.server_close()

    def close(self) -> None:
        self.server.server_close()


def serve_selection_review(
    *,
    cohort_path: Path,
    catalog_path: Path,
    request_paths: Sequence[Path],
    proposal_paths: Sequence[Path],
    template_path: Path,
    media_root: Path,
    workspace_path: Path,
    port: int = 0,
    guard_writable_media: bool = False,
    open_browser: bool = False,
    prepare_only: bool = False,
) -> dict[str, Any] | None:
    """Prepare or serve one exact private selection-review workspace."""

    expected_completed_path = (
        workspace_path.absolute().parent / "interval-selection-review.json"
    )
    completed_existed_before = expected_completed_path.exists()
    workspace = ReviewWorkspace(
        cohort_path=cohort_path,
        catalog_path=catalog_path,
        request_paths=request_paths,
        proposal_paths=proposal_paths,
        template_path=template_path,
        media_root=media_root,
        workspace_path=workspace_path,
        guard_writable_media=guard_writable_media,
    )
    if prepare_only:
        try:
            readiness = workspace.progress()
            completed_present = workspace.paths.completed_review.is_file()
            return {
                "prepared": True,
                "workspace_id": workspace.workspace_id,
                "workspace_manifest_sha256": workspace.manifest["manifest_sha256"],
                "review_id": workspace.template["review_id"],
                "recording_count": len(workspace.media),
                "interval_count": workspace.template["proposal_accounting"][
                    "interval_count"
                ],
                "parent_media_byte_count": sum(
                    item.byte_count for item in workspace.media
                ),
                "writable_media_guarded": sum(
                    item.mutability_policy == "guarded_live_writable_exact_bytes"
                    for item in workspace.media
                ),
                "draft_revision": workspace.draft["revision"],
                "draft_lifecycle": workspace.draft["lifecycle"],
                "ready": bool(
                    readiness["ready_to_begin_finalization"]
                    or readiness["ready_to_materialize"]
                ),
                "completed_review_present": completed_present,
                "completed_review_created": (
                    completed_present and not completed_existed_before
                ),
            }
        finally:
            workspace.close()
    server = ReviewWorkspaceHTTP(workspace, port=port)
    try:
        server.serve_forever(open_browser=open_browser)
    except KeyboardInterrupt:
        pass
    finally:
        server.close()
        workspace.close()
    return None
