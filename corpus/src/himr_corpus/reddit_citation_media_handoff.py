"""Sealed, private handoff for frozen Reddit citation media.

The 2026-08-26 citation snapshot predates the general Reddit discovery lane and
contains exact response bodies for a small, editorially reviewed set of reposts.
This module gives those bytes a deliberately narrow path into the private catalog:
it authenticates the frozen evidence, materializes content-addressed read-only
media, and imports source/media candidates plus review work.  It never creates a
recording, rendition, transcript, claim link, identity assertion, event assertion,
or publication decision.
"""

from __future__ import annotations

import ctypes
import errno
import hashlib
import json
import os
import re
import shutil
import sqlite3
import stat
import subprocess
import tempfile
import zlib
from contextlib import contextmanager
from dataclasses import dataclass
from decimal import Decimal, InvalidOperation, ROUND_HALF_UP
from html import unescape
from html.parser import HTMLParser
from pathlib import Path
from typing import Any
from urllib.parse import urlsplit

from . import __version__
from .db import transaction
from .ids import source_id, stable_id
from .importers import (
    _begin_batch,
    _complete_batch,
    _upsert_source,
    canonical_json,
    sha256_bytes,
)


FROZEN_SNAPSHOT_DATE = "2026-08-26"
FROZEN_PROVENANCE_SHA256 = (
    "325aa22d783ccc3a591c57242cad6bb9d82a04c3e0168e018ac17c57c1e9e508"
)
FROZEN_INVENTORY_SHA256 = (
    "4da9d184ac72de45a1bffc8f7c04b3509d31a1efdc6214d4dcb22b1d7d380f58"
)
FROZEN_PROVENANCE_SOURCE_COUNT = 199
FROZEN_CITATION_COUNT = 46
FROZEN_MEDIA_COUNT = 17
FROZEN_CONTEXT_COUNT = 14
FROZEN_POST_COUNT = 13
FROZEN_VIDEO_COUNT = 10
FROZEN_IMAGE_COUNT = 7
FROZEN_VIDEO_DURATION_MS = 1_482_600

FFPROBE_PATH = Path("/usr/bin/ffprobe")
FFPROBE_SHA256 = "b0303d039d7768418bb3746053b8fea88190df8ae18cb63bf032948c0af04feb"
FFPROBE_VERSION = "8.1.2"
FFPROBE_ENTRIES = (
    "format=format_name,duration:"
    "stream=index,codec_type,codec_name,pix_fmt,avg_frame_rate,width,height"
)

MAX_JSON_BYTES = 2 * 1024 * 1024
MAX_GZIP_BYTES = 128 * 1024 * 1024
MAX_MEDIA_BYTES = 128 * 1024 * 1024
MAX_CONTEXT_BYTES = 8 * 1024 * 1024
MAX_RECORDS = 1_000
SHA256_RE = re.compile(r"^[0-9a-f]{64}$")
UTC_RE = re.compile(r"^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(?:\.\d+)?Z$")
REDDIT_ID_RE = re.compile(r"^[a-z0-9]{5,16}$")

IMPORTER_NAME = "reddit_citation_media_handoff_v1"
RETRIEVAL_TOOL = "reddit_citation_snapshot_handoff"
RETRIEVAL_TOOL_VERSION = "1"
RELATION_KIND = "reddit_citation_declared_media_candidate"
RELATION_BASIS = (
    "frozen official embed identity plus exact declared-media response; candidate only"
)
RELATION_METADATA = {
    "assertion_state": "candidate_only_unreviewed",
    "context_complete": False,
    "identity_asserted": False,
    "publication_authority": False,
}
REVIEW_TASK_SPECS = (
    (
        "reddit_citation_media_context_review",
        "Private candidate: review edit/completeness, audiovisual context, rights, "
        "privacy, and sensitivity; no claim, identity, or event assertion.",
        45,
    ),
    (
        "reddit_citation_media_ocr_candidate",
        "Private OCR-routing candidate only; OCR has not been evaluated and must "
        "receive its own exact-coordinate evidence and review.",
        80,
    ),
    (
        "reddit_citation_media_visual_candidate",
        "Private visual-analysis candidate only; no person, identity, action, or "
        "event inference has been made.",
        75,
    ),
)

POLICY = {
    "assertion_state": "candidate_only_unreviewed",
    "claims_linked": False,
    "identity_asserted": False,
    "events_asserted": False,
    "recordings_created": False,
    "renditions_created": False,
    "transcripts_created": False,
    "publication_authority": False,
    "requires_human_review": True,
}

ALLOWED_WRITE_TABLES = frozenset(
    {
        "import_batches",
        "import_observations",
        "sources",
        "source_metadata_observations",
        "source_snapshots",
        "source_relations",
        "source_relation_observations",
        "media_objects",
        "media_locations",
        "media_sources",
        "review_tasks",
    }
)


class RedditCitationMediaHandoffError(ValueError):
    """Raised when the frozen evidence or its private handoff is not exact."""


class _HrefParser(HTMLParser):
    def __init__(self) -> None:
        super().__init__(convert_charrefs=True)
        self.hrefs: list[str] = []

    def handle_starttag(
        self, tag: str, attrs: list[tuple[str, str | None]]
    ) -> None:
        if tag.lower() != "a":
            return
        for name, value in attrs:
            if name.lower() == "href" and value is not None:
                self.hrefs.append(value)


def _canonical_bytes(value: Any) -> bytes:
    return json.dumps(
        value,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
        allow_nan=False,
    ).encode("utf-8")


def _derived_id(prefix: str, value: Any) -> str:
    return f"{prefix}_{hashlib.sha256(_canonical_bytes(value)).hexdigest()[:32]}"


def _exact(value: Any, keys: set[str], label: str) -> dict[str, Any]:
    if not isinstance(value, dict):
        raise RedditCitationMediaHandoffError(f"{label} must be an object")
    missing = sorted(keys - set(value))
    unknown = sorted(set(value) - keys)
    if missing or unknown:
        raise RedditCitationMediaHandoffError(
            f"{label} has an unknown shape; missing={missing}, unknown={unknown}"
        )
    return value


def _bounded_text(value: Any, label: str, maximum: int = 16_384) -> str:
    if (
        not isinstance(value, str)
        or not value
        or len(value) > maximum
        or "\x00" in value
    ):
        raise RedditCitationMediaHandoffError(f"{label} must be bounded text")
    return value


def _bounded_int(value: Any, label: str, maximum: int = 2**63 - 1) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or not 0 <= value <= maximum:
        raise RedditCitationMediaHandoffError(
            f"{label} must be a bounded non-negative integer"
        )
    return value


def _timestamp(value: Any, label: str) -> str:
    value = _bounded_text(value, label, 64)
    if not UTC_RE.fullmatch(value):
        raise RedditCitationMediaHandoffError(f"{label} is not an RFC3339 UTC timestamp")
    return value


def _sha256(value: Any, label: str) -> str:
    value = _bounded_text(value, label, 64)
    if not SHA256_RE.fullmatch(value):
        raise RedditCitationMediaHandoffError(f"{label} is not a lowercase SHA-256")
    return value


def _strict_json(body: bytes, label: str) -> dict[str, Any]:
    def pairs(values: list[tuple[str, Any]]) -> dict[str, Any]:
        result: dict[str, Any] = {}
        for key, value in values:
            if key in result:
                raise RedditCitationMediaHandoffError(
                    f"{label} has duplicate JSON key {key!r}"
                )
            result[key] = value
        return result

    def integer(value: str) -> int:
        if len(value.lstrip("-")) > 19:
            raise RedditCitationMediaHandoffError(f"{label} has an oversized integer")
        return int(value)

    def invalid_constant(value: str) -> None:
        raise RedditCitationMediaHandoffError(f"{label} has invalid number {value}")

    try:
        decoded = body.decode("utf-8")
        value = json.loads(
            decoded,
            object_pairs_hook=pairs,
            parse_int=integer,
            parse_float=invalid_constant,
            parse_constant=invalid_constant,
        )
    except RedditCitationMediaHandoffError:
        raise
    except (UnicodeDecodeError, json.JSONDecodeError, ValueError, RecursionError) as error:
        raise RedditCitationMediaHandoffError(f"{label} is not strict UTF-8 JSON") from error
    if not isinstance(value, dict):
        raise RedditCitationMediaHandoffError(f"{label} must contain one object")
    return value


def _fingerprint(value: os.stat_result) -> tuple[int, int, int, int, int, int, int]:
    return (
        value.st_dev,
        value.st_ino,
        value.st_size,
        value.st_mtime_ns,
        value.st_ctime_ns,
        value.st_mode,
        value.st_nlink,
    )


@dataclass
class _PinnedFile:
    requested_path: Path
    resolved_path: Path
    descriptor: int
    initial_stat: os.stat_result
    body: bytes
    digest: str
    label: str

    @classmethod
    def open(
        cls,
        path: Path,
        *,
        maximum: int,
        label: str,
        guard_writable: bool,
    ) -> "_PinnedFile":
        requested = Path(os.path.abspath(os.fspath(path)))
        try:
            path_stat = requested.lstat()
        except OSError as error:
            raise RedditCitationMediaHandoffError(f"{label} cannot be inspected") from error
        if (
            stat.S_ISLNK(path_stat.st_mode)
            or not stat.S_ISREG(path_stat.st_mode)
            or path_stat.st_nlink != 1
        ):
            raise RedditCitationMediaHandoffError(
                f"{label} must be a single-link regular file, not a symlink"
            )
        try:
            descriptor = os.open(
                requested,
                os.O_RDONLY | getattr(os, "O_CLOEXEC", 0) | getattr(os, "O_NOFOLLOW", 0),
            )
        except OSError as error:
            raise RedditCitationMediaHandoffError(f"{label} cannot be opened safely") from error
        try:
            opened = os.fstat(descriptor)
            if (
                not stat.S_ISREG(opened.st_mode)
                or opened.st_nlink != 1
                or _fingerprint(opened) != _fingerprint(path_stat)
            ):
                raise RedditCitationMediaHandoffError(
                    f"{label} changed between inspection and descriptor open"
                )
            try:
                resolved = requested.resolve(strict=True)
                resolved_stat = resolved.stat()
            except OSError as error:
                raise RedditCitationMediaHandoffError(
                    f"{label} cannot be resolved after descriptor open"
                ) from error
            if _fingerprint(resolved_stat) != _fingerprint(opened):
                raise RedditCitationMediaHandoffError(
                    f"{label} resolved identity differs from its descriptor"
                )
            if opened.st_size < 1 or opened.st_size > maximum:
                raise RedditCitationMediaHandoffError(f"{label} exceeds its byte limit")
            # Refuse bytes the current process can edit.  A root-owned executable
            # normally carries an owner-write bit but is not writable by this user;
            # mode bits alone would incorrectly reject the pinned system ffprobe.
            if os.access(resolved, os.W_OK) and not guard_writable:
                raise RedditCitationMediaHandoffError(
                    f"{label} is writable; pass the explicit writable-input guard"
                )
            chunks: list[bytes] = []
            offset = 0
            while offset < opened.st_size:
                chunk = os.pread(descriptor, min(1024 * 1024, opened.st_size - offset), offset)
                if not chunk:
                    raise RedditCitationMediaHandoffError(f"{label} ended during pinned read")
                chunks.append(chunk)
                offset += len(chunk)
            body = b"".join(chunks)
            pinned = cls(
                requested_path=requested,
                resolved_path=resolved,
                descriptor=descriptor,
                initial_stat=opened,
                body=body,
                digest=sha256_bytes(body),
                label=label,
            )
            pinned.verify()
            return pinned
        except Exception:
            os.close(descriptor)
            raise

    def verify(self) -> None:
        try:
            descriptor_stat = os.fstat(self.descriptor)
            requested_lstat = self.requested_path.lstat()
            if stat.S_ISLNK(requested_lstat.st_mode):
                raise RedditCitationMediaHandoffError(
                    f"{self.label} requested path became a symlink"
                )
            requested_resolved = self.requested_path.resolve(strict=True)
            path_stat = self.resolved_path.stat()
        except OSError as error:
            raise RedditCitationMediaHandoffError(
                f"{self.label} changed or disappeared during the operation"
            ) from error
        expected = _fingerprint(self.initial_stat)
        if (
            requested_resolved != self.resolved_path
            or _fingerprint(requested_lstat) != expected
            or _fingerprint(descriptor_stat) != expected
            or _fingerprint(path_stat) != expected
        ):
            raise RedditCitationMediaHandoffError(
                f"{self.label} identity or metadata changed during the operation"
            )
        chunks: list[bytes] = []
        offset = 0
        while offset < descriptor_stat.st_size:
            chunk = os.pread(
                self.descriptor,
                min(1024 * 1024, descriptor_stat.st_size - offset),
                offset,
            )
            if not chunk:
                raise RedditCitationMediaHandoffError(
                    f"{self.label} ended during closing verification"
                )
            chunks.append(chunk)
            offset += len(chunk)
        if sha256_bytes(b"".join(chunks)) != self.digest:
            raise RedditCitationMediaHandoffError(
                f"{self.label} bytes changed during the operation"
            )

    def close(self) -> None:
        os.close(self.descriptor)


class _Pins:
    def __init__(self) -> None:
        self.files: list[_PinnedFile] = []

    def open(
        self,
        path: Path,
        *,
        maximum: int,
        label: str,
        guard_writable: bool,
    ) -> _PinnedFile:
        pinned = _PinnedFile.open(
            path,
            maximum=maximum,
            label=label,
            guard_writable=guard_writable,
        )
        self.files.append(pinned)
        return pinned

    def verify(self) -> None:
        for pinned in self.files:
            pinned.verify()

    def close(self) -> None:
        for pinned in reversed(self.files):
            pinned.close()
        self.files.clear()


def _strict_gzip(pinned: _PinnedFile, *, expected_size: int, maximum: int) -> bytes:
    if expected_size > maximum:
        raise RedditCitationMediaHandoffError(
            f"{pinned.label} declares an oversized uncompressed body"
        )
    decoder = zlib.decompressobj(16 + zlib.MAX_WBITS)
    try:
        body = decoder.decompress(pinned.body, expected_size + 1)
        if len(body) > expected_size:
            raise RedditCitationMediaHandoffError(
                f"{pinned.label} expands beyond its declared size"
            )
        if decoder.unconsumed_tail:
            raise RedditCitationMediaHandoffError(
                f"{pinned.label} cannot finish within its declared size"
            )
        body += decoder.flush()
    except zlib.error as error:
        raise RedditCitationMediaHandoffError(
            f"{pinned.label} failed gzip CRC/stream validation"
        ) from error
    if not decoder.eof:
        raise RedditCitationMediaHandoffError(f"{pinned.label} has a truncated gzip member")
    if decoder.unused_data or decoder.unconsumed_tail:
        raise RedditCitationMediaHandoffError(
            f"{pinned.label} has a second gzip member or trailing bytes"
        )
    if len(body) != expected_size:
        raise RedditCitationMediaHandoffError(
            f"{pinned.label} uncompressed size differs from provenance"
        )
    return body


def _safe_relative(value: Any, label: str, *, prefix: str | None = None) -> Path:
    text = _bounded_text(value, label, 1024)
    relative = Path(text)
    if relative.is_absolute() or ".." in relative.parts or str(relative) != text:
        raise RedditCitationMediaHandoffError(f"{label} is not a canonical relative path")
    if prefix and (not relative.parts or relative.parts[0] != prefix):
        raise RedditCitationMediaHandoffError(f"{label} is outside {prefix}/")
    return relative


def _reject_symlink_components(root: Path, relative: Path, label: str) -> None:
    current = root
    for component in relative.parts:
        current = current / component
        try:
            if current.is_symlink():
                raise RedditCitationMediaHandoffError(
                    f"{label} must not traverse a symlink"
                )
        except OSError as error:
            raise RedditCitationMediaHandoffError(
                f"{label} path components cannot be inspected"
            ) from error


def _ffprobe_pin() -> _PinnedFile:
    pinned = _PinnedFile.open(
        FFPROBE_PATH,
        maximum=64 * 1024 * 1024,
        label="pinned ffprobe executable",
        guard_writable=False,
    )
    try:
        if pinned.digest != FFPROBE_SHA256:
            raise RedditCitationMediaHandoffError(
                "ffprobe executable SHA-256 is not approved"
            )
        result = subprocess.run(
            [str(pinned.resolved_path), "-version"],
            check=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            timeout=10,
        )
        first = result.stdout.splitlines()[0] if result.stdout else ""
        if not first.startswith(f"ffprobe version {FFPROBE_VERSION} "):
            raise RedditCitationMediaHandoffError(
                "ffprobe version output is not approved"
            )
        pinned.verify()
        return pinned
    except Exception:
        pinned.close()
        raise


def _probe(
    path: Path,
    ffprobe: _PinnedFile,
    media_kind: str,
    *,
    media_descriptor: int | None = None,
) -> dict[str, Any]:
    probe_input = (
        f"/proc/self/fd/{media_descriptor}"
        if media_descriptor is not None
        else str(path)
    )
    descriptor_demuxer = (
        ["-f", "image2"]
        if media_descriptor is not None and media_kind == "image"
        else []
    )
    try:
        result = subprocess.run(
            [
                str(ffprobe.resolved_path),
                "-v",
                "error",
                "-show_entries",
                FFPROBE_ENTRIES,
                "-of",
                "json",
                *descriptor_demuxer,
                probe_input,
            ],
            check=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            timeout=30,
            pass_fds=((media_descriptor,) if media_descriptor is not None else ()),
        )
    except (OSError, subprocess.SubprocessError) as error:
        raise RedditCitationMediaHandoffError(f"ffprobe failed for {path.name}") from error
    probe = _strict_json(result.stdout, f"ffprobe output for {path.name}")
    streams = probe.get("streams")
    format_row = probe.get("format")
    if not isinstance(streams, list) or not isinstance(format_row, dict):
        raise RedditCitationMediaHandoffError(f"ffprobe output for {path.name} is incomplete")
    if len(streams) != 1 or streams[0].get("codec_type") != "video":
        raise RedditCitationMediaHandoffError(
            f"{path.name} must contain exactly one visual stream and no audio"
        )
    stream = streams[0]
    width = _bounded_int(stream.get("width"), f"{path.name} width", 16_384)
    height = _bounded_int(stream.get("height"), f"{path.name} height", 16_384)
    if width < 1 or height < 1:
        raise RedditCitationMediaHandoffError(f"{path.name} has empty dimensions")
    if media_kind == "video":
        if (
            stream.get("codec_name") != "h264"
            or stream.get("pix_fmt") != "yuv420p"
            or stream.get("avg_frame_rate") != "30/1"
            or "mov" not in str(format_row.get("format_name", "")).split(",")
        ):
            raise RedditCitationMediaHandoffError(
                f"{path.name} differs from the frozen H.264/yuv420p/30fps contract"
            )
        try:
            duration_ms = int(
                (Decimal(str(format_row["duration"])) * 1000).quantize(
                    Decimal("1"), rounding=ROUND_HALF_UP
                )
            )
        except (KeyError, InvalidOperation, ValueError) as error:
            raise RedditCitationMediaHandoffError(
                f"{path.name} has no exact probe duration"
            ) from error
        if duration_ms < 1:
            raise RedditCitationMediaHandoffError(f"{path.name} has an empty duration")
        container = "mp4"
    else:
        if stream.get("codec_name") != "mjpeg" or format_row.get("format_name") != "image2":
            raise RedditCitationMediaHandoffError(
                f"{path.name} differs from the frozen JPEG still-image contract"
            )
        # FFprobe exposes JPEGs as one-frame video streams.  The sealed declaration,
        # MIME type, and JPEG magic are authoritative for catalog media_kind.
        duration_ms = None
        container = "jpeg"
    normalized = {
        "format": {"container": container, "duration_ms": duration_ms},
        "streams": [
            {
                "index": _bounded_int(stream.get("index"), f"{path.name} stream index", 64),
                "codec_type": "video",
                "codec_name": stream.get("codec_name"),
                "pixel_format": stream.get("pix_fmt"),
                "average_frame_rate": stream.get("avg_frame_rate"),
                "width": width,
                "height": height,
            }
        ],
        "audio_stream_count": 0,
        "ffprobe": {
            "path": str(FFPROBE_PATH),
            "sha256": FFPROBE_SHA256,
            "version": FFPROBE_VERSION,
            "show_entries": FFPROBE_ENTRIES,
        },
    }
    ffprobe.verify()
    return normalized


def _validate_common_inventory_identity(citation: dict[str, Any]) -> tuple[str, str, str | None]:
    citation_url = _bounded_text(citation.get("citation_url"), "citation URL", 4096)
    subreddit = _bounded_text(citation.get("subreddit"), "citation subreddit", 64)
    post_id = _bounded_text(citation.get("post_id_from_citation_url"), "citation post ID", 32)
    comment_id = citation.get("comment_id_from_citation_url")
    if not REDDIT_ID_RE.fullmatch(post_id):
        raise RedditCitationMediaHandoffError("citation post ID is invalid")
    if comment_id is not None and (
        not isinstance(comment_id, str) or not REDDIT_ID_RE.fullmatch(comment_id)
    ):
        raise RedditCitationMediaHandoffError("citation comment ID is invalid")
    parsed = urlsplit(citation_url)
    if parsed.scheme != "https" or (parsed.hostname or "").lower() not in {
        "reddit.com",
        "www.reddit.com",
    }:
        raise RedditCitationMediaHandoffError("citation URL is not an official HTTPS Reddit URL")
    path_parts = [part for part in parsed.path.split("/") if part]
    if len(path_parts) < 4 or path_parts[0].lower() != "r" or path_parts[1].lower() != subreddit.lower() or path_parts[2] != "comments" or path_parts[3] != post_id:
        raise RedditCitationMediaHandoffError("citation URL path does not match subreddit/post identity")
    if comment_id is not None and (not path_parts or path_parts[-1] != comment_id):
        raise RedditCitationMediaHandoffError("citation URL path does not end in its comment ID")
    observations = citation.get("observations")
    if not isinstance(observations, dict):
        raise RedditCitationMediaHandoffError("citation observations are missing")
    oembed = observations.get("oembed")
    embed = observations.get("embed_page")
    observable = citation.get("observable_ids")
    if not isinstance(oembed, dict) or not isinstance(embed, dict) or not isinstance(observable, dict):
        raise RedditCitationMediaHandoffError("citation official identity observations are incomplete")
    if not oembed.get("available") or not embed.get("available"):
        raise RedditCitationMediaHandoffError("citation lacks both official oEmbed and embed observations")
    for observed_post in (
        oembed.get("post_id"),
        embed.get("post_id"),
        observable.get("post_id_from_url"),
        observable.get("post_id_from_oembed"),
        observable.get("post_id_from_embed_page"),
    ):
        if observed_post != post_id:
            raise RedditCitationMediaHandoffError("official citation post IDs disagree")
    if str(oembed.get("subreddit", "")).lower() != subreddit.lower() or str(embed.get("subreddit", "")).lower() != subreddit.lower():
        raise RedditCitationMediaHandoffError("official citation subreddit identities disagree")
    if comment_id is None:
        for observed_comment in (
            oembed.get("comment_id"),
            embed.get("comment_id"),
            observable.get("comment_id_from_url"),
            observable.get("comment_id_from_oembed"),
            observable.get("comment_id_from_embed_page"),
        ):
            if observed_comment is not None:
                raise RedditCitationMediaHandoffError("post citation unexpectedly resolves to a comment")
    else:
        for observed_comment in (
            oembed.get("comment_id"),
            embed.get("comment_id"),
            observable.get("comment_id_from_url"),
            observable.get("comment_id_from_oembed"),
            observable.get("comment_id_from_embed_page"),
        ):
            if observed_comment != comment_id:
                raise RedditCitationMediaHandoffError(
                    "official embed comment ID does not equal the cited comment ID"
                )
        canonical_href = _bounded_text(oembed.get("canonical_href"), "oEmbed canonical URL", 4096)
        canonical_parts = [part for part in urlsplit(canonical_href).path.split("/") if part]
        if len(canonical_parts) < 6 or canonical_parts[3] != post_id or canonical_parts[-1] != comment_id:
            raise RedditCitationMediaHandoffError(
                "oEmbed canonical URL does not bind the cited post and comment IDs"
            )
        if embed.get("media_type") != "comment":
            raise RedditCitationMediaHandoffError("comment embed is not typed as a comment")
    return subreddit, post_id, comment_id


@dataclass
class _SnapshotSession:
    root: Path
    pins: _Pins
    provenance_pin: _PinnedFile
    inventory_pin: _PinnedFile
    provenance: dict[str, Any]
    inventory: dict[str, Any]
    contexts: list[dict[str, Any]]
    media: list[dict[str, Any]]

    def verify(self) -> None:
        self.pins.verify()

    def close(self) -> None:
        self.pins.close()


def _open_snapshot(
    snapshot_dir: Path, *, guard_writable_inputs: bool
) -> _SnapshotSession:
    root = Path(snapshot_dir)
    try:
        root = root.resolve(strict=True)
    except OSError as error:
        raise RedditCitationMediaHandoffError("snapshot directory does not exist") from error
    if not root.is_dir() or root.is_symlink():
        raise RedditCitationMediaHandoffError("snapshot root must be a real directory")
    pins = _Pins()
    try:
        provenance_pin = pins.open(
            root / "provenance.json",
            maximum=MAX_JSON_BYTES,
            label="frozen Reddit provenance",
            guard_writable=guard_writable_inputs,
        )
        inventory_pin = pins.open(
            root / "citation-inventory.json",
            maximum=MAX_JSON_BYTES,
            label="frozen Reddit citation inventory",
            guard_writable=guard_writable_inputs,
        )
        if provenance_pin.digest != FROZEN_PROVENANCE_SHA256:
            raise RedditCitationMediaHandoffError("provenance.json is not the frozen 2026-08-26 file")
        if inventory_pin.digest != FROZEN_INVENTORY_SHA256:
            raise RedditCitationMediaHandoffError(
                "citation-inventory.json is not the frozen 2026-08-26 file"
            )
        provenance = _strict_json(provenance_pin.body, "frozen Reddit provenance")
        inventory = _strict_json(inventory_pin.body, "frozen Reddit citation inventory")
        if (
            provenance.get("schema_version") != 4
            or provenance.get("snapshot_date") != FROZEN_SNAPSHOT_DATE
            or provenance.get("citation_count") != FROZEN_CITATION_COUNT
            or provenance.get("source_count") != FROZEN_PROVENANCE_SOURCE_COUNT
        ):
            raise RedditCitationMediaHandoffError("frozen provenance header differs from its contract")
        if (
            inventory.get("schema_version") != 4
            or inventory.get("snapshot_date") != FROZEN_SNAPSHOT_DATE
            or inventory.get("distinct_citation_count") != FROZEN_CITATION_COUNT
        ):
            raise RedditCitationMediaHandoffError("frozen inventory header differs from its contract")
        _timestamp(provenance.get("created_at"), "provenance created_at")
        sources = provenance.get("sources")
        citations = inventory.get("citations")
        if not isinstance(sources, list) or len(sources) != FROZEN_PROVENANCE_SOURCE_COUNT:
            raise RedditCitationMediaHandoffError("frozen provenance source array is incomplete")
        if not isinstance(citations, list) or len(citations) != FROZEN_CITATION_COUNT:
            raise RedditCitationMediaHandoffError("frozen citation array is incomplete")
        if len(sources) > MAX_RECORDS or len(citations) > MAX_RECORDS:
            raise RedditCitationMediaHandoffError("frozen snapshot exceeds record caps")
        provenance_by_id: dict[str, dict[str, Any]] = {}
        for index, row in enumerate(sources):
            if not isinstance(row, dict):
                raise RedditCitationMediaHandoffError(f"provenance source {index} is not an object")
            identifier = _bounded_text(row.get("source_id"), f"provenance source {index} ID", 128)
            if identifier in provenance_by_id:
                raise RedditCitationMediaHandoffError("provenance source IDs are not unique")
            provenance_by_id[identifier] = row

        contexts: list[dict[str, Any]] = []
        media_rows: list[dict[str, Any]] = []
        seen_oembed_sources: set[str] = set()
        seen_media_sources: set[str] = set()
        seen_media_hashes: set[str] = set()
        for citation_index, citation in enumerate(citations):
            if not isinstance(citation, dict):
                raise RedditCitationMediaHandoffError(
                    f"citation inventory row {citation_index} is not an object"
                )
            declared = citation.get("observations", {}).get("declared_media", [])
            if not isinstance(declared, list):
                raise RedditCitationMediaHandoffError("declared-media observation is not an array")
            matched = [row for row in declared if isinstance(row, dict) and row.get("semantic_match") is True]
            if not matched:
                continue
            if len(matched) != len(declared):
                raise RedditCitationMediaHandoffError(
                    "a media-bearing citation mixes matched and unmatched declared captures"
                )
            subreddit, post_id, comment_id = _validate_common_inventory_identity(citation)
            endpoint_results = citation.get("endpoint_results")
            if (
                not isinstance(endpoint_results, dict)
                or not isinstance(endpoint_results.get("oembed"), dict)
                or not isinstance(endpoint_results.get("embed_page"), dict)
            ):
                raise RedditCitationMediaHandoffError(
                    "media citation has no official oEmbed/embed endpoint result"
                )
            oembed_source_id = _bounded_text(
                endpoint_results["oembed"].get("source_id"),
                "oEmbed provenance source ID",
                128,
            )
            if oembed_source_id in seen_oembed_sources:
                raise RedditCitationMediaHandoffError(
                    "oEmbed provenance source is reused"
                )
            seen_oembed_sources.add(oembed_source_id)
            oembed_provenance = provenance_by_id.get(oembed_source_id)
            if (
                oembed_provenance is None
                or oembed_provenance.get("endpoint_kind") != "oembed"
            ):
                raise RedditCitationMediaHandoffError(
                    "oEmbed endpoint has no matching provenance row"
                )
            if (
                oembed_provenance.get("expected_post_id") != post_id
                or str(oembed_provenance.get("expected_subreddit", "")).lower()
                != subreddit.lower()
                or oembed_provenance.get("citation_url") != citation.get("citation_url")
                or oembed_provenance.get("classification") != "reddit_oembed_json"
                or oembed_provenance.get("content_type") != "application/json"
                or oembed_provenance.get("status") != 200
                or oembed_provenance.get("ok") is not True
                or endpoint_results["oembed"].get("status") != 200
                or endpoint_results["oembed"].get("content_type")
                != "application/json"
                or endpoint_results["oembed"].get("classification")
                != "reddit_oembed_json"
            ):
                raise RedditCitationMediaHandoffError(
                    "oEmbed provenance identity differs from inventory"
                )
            oembed_relative = _safe_relative(
                oembed_provenance.get("local_path"),
                "oEmbed artifact path",
                prefix="raw",
            )
            _reject_symlink_components(root, oembed_relative, "oEmbed artifact path")
            oembed_size = _bounded_int(
                oembed_provenance.get("compressed_bytes"),
                "oEmbed compressed byte count",
                MAX_GZIP_BYTES,
            )
            oembed_uncompressed_size = _bounded_int(
                oembed_provenance.get("uncompressed_bytes"),
                "oEmbed uncompressed byte count",
                MAX_CONTEXT_BYTES,
            )
            oembed_pin = pins.open(
                root / oembed_relative,
                maximum=MAX_GZIP_BYTES,
                label=f"oEmbed gzip {oembed_source_id}",
                guard_writable=guard_writable_inputs,
            )
            if len(oembed_pin.body) != oembed_size:
                raise RedditCitationMediaHandoffError(
                    "oEmbed gzip size differs from provenance"
                )
            oembed_body = _strict_gzip(
                oembed_pin,
                expected_size=oembed_uncompressed_size,
                maximum=MAX_CONTEXT_BYTES,
            )
            oembed_sha = _sha256(
                oembed_provenance.get("uncompressed_sha256"),
                "oEmbed uncompressed SHA-256",
            )
            if sha256_bytes(oembed_body) != oembed_sha:
                raise RedditCitationMediaHandoffError(
                    "oEmbed body SHA-256 differs from provenance"
                )
            oembed_document = _strict_json(oembed_body, "official oEmbed response")
            oembed_observation = citation["observations"]["oembed"]
            if (
                oembed_document.get("provider_name") != "reddit"
                or oembed_document.get("provider_url") != "https://www.reddit.com"
                or oembed_document.get("type") != "rich"
                or oembed_document.get("title") != oembed_observation.get("title")
                or oembed_document.get("author_name")
                != oembed_observation.get("author_name")
            ):
                raise RedditCitationMediaHandoffError(
                    "raw oEmbed response differs from its frozen observation"
                )
            oembed_html = _bounded_text(
                oembed_document.get("html"), "official oEmbed HTML", MAX_CONTEXT_BYTES
            )
            href_parser = _HrefParser()
            href_parser.feed(oembed_html)
            href_parser.close()
            if oembed_observation.get("canonical_href") not in href_parser.hrefs:
                raise RedditCitationMediaHandoffError(
                    "raw oEmbed response lacks its frozen canonical target"
                )
            embed_source_id = _bounded_text(
                endpoint_results["embed_page"].get("source_id"),
                "embed provenance source ID",
                128,
            )
            embed_provenance = provenance_by_id.get(embed_source_id)
            if embed_provenance is None or embed_provenance.get("endpoint_kind") != "embed_page":
                raise RedditCitationMediaHandoffError("embed endpoint has no matching provenance row")
            if (
                embed_provenance.get("expected_post_id") != post_id
                or str(embed_provenance.get("expected_subreddit", "")).lower()
                != subreddit.lower()
                or embed_provenance.get("citation_url") != citation.get("citation_url")
                or embed_provenance.get("classification") != "reddit_embed_page_html"
                or embed_provenance.get("status") != 200
                or embed_provenance.get("ok") is not True
            ):
                raise RedditCitationMediaHandoffError("embed provenance identity differs from inventory")
            embed_relative = _safe_relative(
                embed_provenance.get("local_path"), "embed artifact path", prefix="raw"
            )
            _reject_symlink_components(root, embed_relative, "embed artifact path")
            embed_size = _bounded_int(
                embed_provenance.get("compressed_bytes"), "embed compressed byte count", MAX_GZIP_BYTES
            )
            embed_uncompressed_size = _bounded_int(
                embed_provenance.get("uncompressed_bytes"),
                "embed uncompressed byte count",
                MAX_CONTEXT_BYTES,
            )
            embed_pin = pins.open(
                root / embed_relative,
                maximum=MAX_GZIP_BYTES,
                label=f"embed gzip {embed_source_id}",
                guard_writable=guard_writable_inputs,
            )
            if len(embed_pin.body) != embed_size:
                raise RedditCitationMediaHandoffError("embed gzip size differs from provenance")
            embed_body = _strict_gzip(
                embed_pin,
                expected_size=embed_uncompressed_size,
                maximum=MAX_CONTEXT_BYTES,
            )
            embed_sha = _sha256(
                embed_provenance.get("uncompressed_sha256"), "embed uncompressed SHA-256"
            )
            if sha256_bytes(embed_body) != embed_sha:
                raise RedditCitationMediaHandoffError("embed body SHA-256 differs from provenance")
            try:
                embed_text = unescape(embed_body.decode("utf-8"))
            except UnicodeDecodeError as error:
                raise RedditCitationMediaHandoffError(
                    "official embed body is not UTF-8 HTML"
                ) from error
            embed_observation = citation["observations"]["embed_page"]
            raw_identity_tokens = [post_id]
            if comment_id is not None:
                raw_identity_tokens.append(comment_id)
            content_url = embed_observation.get("content_url")
            gallery_urls = embed_observation.get("gallery_image_urls", [])
            if isinstance(content_url, str):
                raw_identity_tokens.append(content_url)
            if isinstance(gallery_urls, list):
                raw_identity_tokens.extend(
                    url for url in gallery_urls if isinstance(url, str)
                )
            if any(token not in embed_text for token in raw_identity_tokens):
                raise RedditCitationMediaHandoffError(
                    "raw embed response lacks its frozen post/comment/media identity"
                )
            context_id = stable_id("rctx", citation.get("citation_url"))
            native_id = post_id if comment_id is None else f"{post_id}:{comment_id}"
            source_kind = "reddit_post" if comment_id is None else "reddit_comment"
            context_source_id = source_id("reddit", source_kind, native_id)
            context = {
                "context_id": context_id,
                "context_source_id": context_source_id,
                "source_kind": source_kind,
                "native_id": native_id,
                "citation_url": citation["citation_url"],
                "subreddit": subreddit,
                "post_id": post_id,
                "comment_id": comment_id,
                "comment_body_sha256": embed_observation.get("comment_body_sha256"),
                "comment_body_bytes": embed_observation.get("comment_body_bytes"),
                "embed": {
                    "provenance_source_id": embed_source_id,
                    "request_url": embed_provenance.get("request_url"),
                    "final_url": embed_provenance.get("final_url"),
                    "http_status": 200,
                    "retrieved_at": _timestamp(
                        embed_provenance.get("retrieved_at"), "embed retrieved_at"
                    ),
                    "artifact_path": str(embed_relative),
                    "gzip_sha256": embed_pin.digest,
                    "compressed_bytes": embed_size,
                    "payload_sha256": embed_sha,
                    "uncompressed_bytes": embed_uncompressed_size,
                },
                "media_sha256s": [],
            }
            if comment_id is None:
                if context["comment_body_sha256"] is not None or context["comment_body_bytes"] is not None:
                    raise RedditCitationMediaHandoffError("post context unexpectedly has comment body data")
            else:
                _sha256(context["comment_body_sha256"], "comment body SHA-256")
                _bounded_int(context["comment_body_bytes"], "comment body bytes", MAX_CONTEXT_BYTES)

            gallery_urls = citation["observations"]["embed_page"].get("gallery_image_urls", [])
            content_url = citation["observations"]["embed_page"].get("content_url")
            claim_uses = citation.get("claim_uses")
            if not isinstance(claim_uses, list):
                raise RedditCitationMediaHandoffError("media citation has no frozen claim-use binding")
            for media_observation in matched:
                provenance_source_id = _bounded_text(
                    media_observation.get("source_id"), "declared-media provenance source ID", 128
                )
                if provenance_source_id in seen_media_sources:
                    raise RedditCitationMediaHandoffError("declared-media provenance source is reused")
                seen_media_sources.add(provenance_source_id)
                source = provenance_by_id.get(provenance_source_id)
                if source is None or source.get("endpoint_kind") != "declared_media":
                    raise RedditCitationMediaHandoffError("declared media has no provenance row")
                declared_media = source.get("declared_media")
                if not isinstance(declared_media, dict):
                    raise RedditCitationMediaHandoffError("declared-media provenance lacks declaration")
                media_url = _bounded_text(media_observation.get("declared_media_url"), "media URL", 4096)
                media_kind = media_observation.get("declared_media_type")
                media_sha = _sha256(media_observation.get("captured_sha256"), "media SHA-256")
                if media_kind not in {"video", "image"}:
                    raise RedditCitationMediaHandoffError("declared media kind is unsupported")
                parsed_media_url = urlsplit(media_url)
                expected_media_host = "v.redd.it" if media_kind == "video" else "preview.redd.it"
                if (
                    parsed_media_url.scheme != "https"
                    or (parsed_media_url.hostname or "").lower() != expected_media_host
                    or parsed_media_url.username
                    or parsed_media_url.password
                ):
                    raise RedditCitationMediaHandoffError(
                        "declared media URL/type is outside the frozen Reddit media boundary"
                    )
                if media_sha in seen_media_hashes:
                    raise RedditCitationMediaHandoffError("frozen media bodies are not content-distinct")
                seen_media_hashes.add(media_sha)
                expected_flags = {
                    "request_url_matches",
                    "final_url_matches",
                    "post_id_matches",
                    "subreddit_matches",
                    "embed_url_matches",
                    "embed_type_matches",
                    "response_type_matches",
                    "capture_hash_matches",
                    "response_ok",
                    "nonempty_body",
                    "semantic_match",
                }
                if any(media_observation.get(flag) is not True for flag in expected_flags):
                    raise RedditCitationMediaHandoffError("declared-media semantic match is incomplete")
                if (
                    source.get("citation_url") != citation.get("citation_url")
                    or source.get("expected_post_id") != post_id
                    or str(source.get("expected_subreddit", "")).lower() != subreddit.lower()
                    or declared_media.get("media_url") != media_url
                    or declared_media.get("media_type") != media_kind
                    or declared_media.get("capture_sha256") != media_sha
                    or media_observation.get("declared_capture_sha256") != media_sha
                    or media_observation.get("final_url") != media_url
                    or source.get("request_url") != media_url
                    or source.get("final_url") != media_url
                    or source.get("status") != 200
                    or source.get("ok") is not True
                    or source.get("classification") != "reddit_declared_media"
                ):
                    raise RedditCitationMediaHandoffError("declared-media provenance does not bind exactly")
                expected_content_type = "video/mp4" if media_kind == "video" else "image/jpeg"
                if (
                    source.get("content_type") != expected_content_type
                    or media_observation.get("content_type") != expected_content_type
                ):
                    raise RedditCitationMediaHandoffError("declared-media response type differs")
                if comment_id is not None:
                    if media_kind != "image" or media_url not in gallery_urls or content_url is not None:
                        raise RedditCitationMediaHandoffError(
                            "comment media is not bound to the exact official embed gallery URL"
                        )
                elif media_kind == "video":
                    if not isinstance(content_url, str):
                        raise RedditCitationMediaHandoffError("post video lacks official embed media identity")
                    content_parts = [part for part in urlsplit(content_url).path.split("/") if part]
                    media_parts = [part for part in parsed_media_url.path.split("/") if part]
                    if (
                        (urlsplit(content_url).hostname or "").lower() != "v.redd.it"
                        or not content_parts
                        or not media_parts
                        or content_parts[0] != media_parts[0]
                    ):
                        raise RedditCitationMediaHandoffError(
                            "post video rendition is not bound to the official embed media ID"
                        )
                elif media_url not in gallery_urls:
                    raise RedditCitationMediaHandoffError(
                        "post image is not the exact official embed gallery URL"
                    )
                matching_reviews = []
                for use in claim_uses:
                    if not isinstance(use, dict):
                        continue
                    review = use.get("repost_review")
                    if isinstance(review, dict) and (
                        review.get("media_url") == media_url
                        and review.get("media_type") == media_kind
                        and review.get("capture_sha256") == media_sha
                    ):
                        matching_reviews.append(review)
                if not matching_reviews:
                    raise RedditCitationMediaHandoffError(
                        "declared media lacks an exact frozen repost-review declaration"
                    )
                relative = _safe_relative(source.get("local_path"), "media gzip path", prefix="raw")
                _reject_symlink_components(root, relative, "media gzip path")
                compressed_size = _bounded_int(
                    source.get("compressed_bytes"), "media compressed byte count", MAX_GZIP_BYTES
                )
                uncompressed_size = _bounded_int(
                    source.get("uncompressed_bytes"), "media uncompressed byte count", MAX_MEDIA_BYTES
                )
                if media_observation.get("uncompressed_bytes") != uncompressed_size:
                    raise RedditCitationMediaHandoffError("inventory/provenance media byte counts differ")
                media_pin = pins.open(
                    root / relative,
                    maximum=MAX_GZIP_BYTES,
                    label=f"declared-media gzip {provenance_source_id}",
                    guard_writable=guard_writable_inputs,
                )
                if len(media_pin.body) != compressed_size:
                    raise RedditCitationMediaHandoffError("media gzip size differs from provenance")
                body = _strict_gzip(
                    media_pin,
                    expected_size=uncompressed_size,
                    maximum=MAX_MEDIA_BYTES,
                )
                if sha256_bytes(body) != media_sha or source.get("uncompressed_sha256") != media_sha:
                    raise RedditCitationMediaHandoffError("media body SHA-256 differs from declaration")
                if media_kind == "video":
                    if len(body) < 12 or body[4:8] != b"ftyp":
                        raise RedditCitationMediaHandoffError("declared MP4 has no ISO BMFF ftyp box")
                    suffix = ".mp4"
                    mime_type = "video/mp4"
                else:
                    if len(body) < 4 or not body.startswith(b"\xff\xd8\xff") or not body.endswith(b"\xff\xd9"):
                        raise RedditCitationMediaHandoffError("declared JPEG has invalid magic")
                    suffix = ".jpg"
                    mime_type = "image/jpeg"
                context["media_sha256s"].append(media_sha)
                media_rows.append(
                    {
                        "media_id": f"media_sha256_{media_sha}",
                        "sha256": media_sha,
                        "byte_count": uncompressed_size,
                        "media_kind": media_kind,
                        "mime_type": mime_type,
                        "suffix": suffix,
                        "materialized_path": f"media/sha256/{media_sha[:2]}/{media_sha}{suffix}",
                        "context_id": context_id,
                        "context_source_id": context_source_id,
                        "media_source_id": source_id(
                            "reddit", "reddit_declared_media", media_url
                        ),
                        "media_url": media_url,
                        "provenance_source_id": provenance_source_id,
                        "retrieved_at": _timestamp(source.get("retrieved_at"), "media retrieved_at"),
                        "request_url": media_url,
                        "final_url": media_url,
                        "http_status": 200,
                        "gzip_artifact_path": str(relative),
                        "gzip_sha256": media_pin.digest,
                        "gzip_byte_count": compressed_size,
                        "body": body,
                    }
                )
            context["media_sha256s"].sort()
            contexts.append(context)

        contexts.sort(key=lambda row: row["context_id"])
        media_rows.sort(key=lambda row: row["sha256"])
        if (
            len(contexts) != FROZEN_CONTEXT_COUNT
            or len(media_rows) != FROZEN_MEDIA_COUNT
            or len({row["post_id"] for row in contexts}) != FROZEN_POST_COUNT
            or sum(row["media_kind"] == "video" for row in media_rows) != FROZEN_VIDEO_COUNT
            or sum(row["media_kind"] == "image" for row in media_rows) != FROZEN_IMAGE_COUNT
        ):
            raise RedditCitationMediaHandoffError("frozen citation-media cardinalities differ")
        pins.verify()
        return _SnapshotSession(
            root=root,
            pins=pins,
            provenance_pin=provenance_pin,
            inventory_pin=inventory_pin,
            provenance=provenance,
            inventory=inventory,
            contexts=contexts,
            media=media_rows,
        )
    except Exception:
        pins.close()
        raise


def _write_exact(path: Path, body: bytes, mode: int = 0o400) -> None:
    path.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
    os.chmod(path.parent, 0o700)
    descriptor = os.open(
        path,
        os.O_WRONLY | os.O_CREAT | os.O_EXCL | getattr(os, "O_NOFOLLOW", 0),
        mode,
    )
    try:
        offset = 0
        while offset < len(body):
            offset += os.write(descriptor, body[offset:])
        os.fchmod(descriptor, mode)
        os.fsync(descriptor)
    finally:
        os.close(descriptor)


def _fsync_directory(path: Path) -> None:
    descriptor = os.open(path, os.O_RDONLY | getattr(os, "O_DIRECTORY", 0))
    try:
        os.fsync(descriptor)
    finally:
        os.close(descriptor)


def _rename_directory_noreplace(source: Path, destination: Path) -> None:
    """Publish one directory atomically without replacing any destination.

    ``os.rename`` may replace an empty directory that appears after the caller's
    existence check.  This private Linux workflow instead requires renameat2's
    RENAME_NOREPLACE contract and fails closed when the primitive is unavailable.
    """

    renameat2 = getattr(ctypes.CDLL(None, use_errno=True), "renameat2", None)
    if renameat2 is None:
        raise RedditCitationMediaHandoffError(
            "atomic no-replace directory publication is unavailable"
        )
    renameat2.argtypes = [
        ctypes.c_int,
        ctypes.c_char_p,
        ctypes.c_int,
        ctypes.c_char_p,
        ctypes.c_uint,
    ]
    renameat2.restype = ctypes.c_int
    at_fdcwd = -100
    rename_noreplace = 1
    result = renameat2(
        at_fdcwd,
        os.fsencode(source),
        at_fdcwd,
        os.fsencode(destination),
        rename_noreplace,
    )
    if result == 0:
        return
    error_number = ctypes.get_errno()
    if error_number in {errno.EEXIST, errno.ENOTEMPTY}:
        raise RedditCitationMediaHandoffError(
            "handoff output appeared before atomic publication"
        )
    raise RedditCitationMediaHandoffError(
        "atomic handoff-directory publication failed"
    ) from OSError(error_number, os.strerror(error_number), os.fspath(destination))


def _bundle_manifest(session: _SnapshotSession, media: list[dict[str, Any]]) -> dict[str, Any]:
    contexts = session.contexts
    public_media = []
    for row in media:
        public_media.append({key: value for key, value in row.items() if key != "body"})
    identity = {
        "snapshot_date": FROZEN_SNAPSHOT_DATE,
        "provenance_sha256": FROZEN_PROVENANCE_SHA256,
        "citation_inventory_sha256": FROZEN_INVENTORY_SHA256,
        "media": [row["sha256"] for row in public_media],
    }
    return {
        "schema_version": 1,
        "manifest_kind": "reddit_citation_media_handoff",
        "handoff_id": _derived_id("rcmh", identity),
        "snapshot_date": FROZEN_SNAPSHOT_DATE,
        "observed_at": session.provenance["created_at"],
        "policy": POLICY,
        "inputs": {
            "provenance_file": "provenance.json",
            "provenance_sha256": FROZEN_PROVENANCE_SHA256,
            "citation_inventory_file": "citation-inventory.json",
            "citation_inventory_sha256": FROZEN_INVENTORY_SHA256,
            "ffprobe_path": str(FFPROBE_PATH),
            "ffprobe_sha256": FFPROBE_SHA256,
            "ffprobe_version": FFPROBE_VERSION,
        },
        "statistics": {
            "contexts": len(contexts),
            "posts": len({row["post_id"] for row in contexts}),
            "media": len(public_media),
            "videos": sum(row["media_kind"] == "video" for row in public_media),
            "images": sum(row["media_kind"] == "image" for row in public_media),
            "video_duration_ms": sum(
                row["probe"]["format"]["duration_ms"] or 0 for row in public_media
            ),
            "media_bytes": sum(row["byte_count"] for row in public_media),
        },
        "contexts": contexts,
        "media": public_media,
    }


def materialize_reddit_citation_media_handoff(
    snapshot_dir: Path,
    output_dir: Path,
    *,
    guard_writable_inputs: bool = False,
) -> dict[str, Any]:
    """Authenticate the frozen snapshot and atomically materialize its private media."""

    requested_output = Path(output_dir)
    if requested_output.name in {"", ".", ".."}:
        raise RedditCitationMediaHandoffError("handoff output name is invalid")
    if requested_output.exists() or requested_output.is_symlink():
        raise RedditCitationMediaHandoffError("handoff output must not already exist")
    output_parent = requested_output.parent.resolve(strict=True)
    if not output_parent.is_dir() or output_parent.is_symlink():
        raise RedditCitationMediaHandoffError("handoff output parent must be a real directory")
    # Publish through the already-resolved parent rather than resolving an attacker-
    # mutable parent path again at rename time.
    output = output_parent / requested_output.name
    if output.exists() or output.is_symlink():
        raise RedditCitationMediaHandoffError("handoff output must not already exist")
    session = _open_snapshot(
        snapshot_dir, guard_writable_inputs=guard_writable_inputs
    )
    try:
        ffprobe = _ffprobe_pin()
    except Exception:
        session.close()
        raise
    staging: Path | None = None
    try:
        staging = Path(
            tempfile.mkdtemp(prefix=f".{output.name}.tmp-", dir=output_parent)
        ).resolve(strict=True)
        os.chmod(staging, 0o700)
        for row in session.media:
            destination = staging / row["materialized_path"]
            _write_exact(destination, row["body"], 0o400)
            materialized_pin = _PinnedFile.open(
                destination,
                maximum=MAX_MEDIA_BYTES,
                label=f"new materialized media {row['sha256']}",
                guard_writable=False,
            )
            try:
                if (
                    materialized_pin.digest != row["sha256"]
                    or len(materialized_pin.body) != row["byte_count"]
                ):
                    raise RedditCitationMediaHandoffError(
                        "new materialized media differs from its frozen body"
                    )
                row["probe"] = _probe(
                    destination,
                    ffprobe,
                    row["media_kind"],
                    media_descriptor=materialized_pin.descriptor,
                )
                materialized_pin.verify()
            finally:
                materialized_pin.close()
            row["container"] = row["probe"]["format"]["container"]
            row["duration_ms"] = row["probe"]["format"]["duration_ms"]
            row["processing_disposition"] = {
                "asr": (
                    "not_applicable_no_audio_stream"
                    if row["media_kind"] == "video"
                    else "not_applicable_non_audio_media"
                ),
                "ocr": "candidate_only_not_evaluated",
                "visual": "candidate_only_not_evaluated",
            }
        if sum((row["duration_ms"] or 0) for row in session.media) != FROZEN_VIDEO_DURATION_MS:
            raise RedditCitationMediaHandoffError("frozen video-duration total differs")
        manifest = _bundle_manifest(session, session.media)
        manifest_body = _canonical_bytes(manifest) + b"\n"
        _write_exact(staging / "handoff-manifest.json", manifest_body, 0o400)
        for directory in sorted(
            [path for path in staging.rglob("*") if path.is_dir()],
            key=lambda path: len(path.parts),
            reverse=True,
        ):
            os.chmod(directory, 0o700)
            _fsync_directory(directory)
        _fsync_directory(staging)
        session.verify()
        ffprobe.verify()
        _rename_directory_noreplace(staging, output)
        staging = None
        _fsync_directory(output_parent)
        return {
            "handoff_id": manifest["handoff_id"],
            "manifest": str((output / "handoff-manifest.json").resolve()),
            "manifest_sha256": sha256_bytes(manifest_body),
            "statistics": manifest["statistics"],
            "publication_authority": False,
        }
    finally:
        try:
            session.verify()
            ffprobe.verify()
        finally:
            session.close()
            ffprobe.close()
            if staging is not None and staging.exists():
                shutil.rmtree(staging)


@dataclass
class _BundleSession:
    root: Path
    pins: _Pins
    manifest_pin: _PinnedFile
    manifest: dict[str, Any]
    media_paths: dict[str, Path]

    def verify(self) -> None:
        self.pins.verify()

    def close(self) -> None:
        self.pins.close()


def _validate_bundle_manifest_shape(manifest: dict[str, Any]) -> None:
    _exact(
        manifest,
        {
            "schema_version",
            "manifest_kind",
            "handoff_id",
            "snapshot_date",
            "observed_at",
            "policy",
            "inputs",
            "statistics",
            "contexts",
            "media",
        },
        "handoff manifest",
    )
    if manifest["schema_version"] != 1 or manifest["manifest_kind"] != "reddit_citation_media_handoff":
        raise RedditCitationMediaHandoffError("unsupported handoff manifest")
    if manifest["snapshot_date"] != FROZEN_SNAPSHOT_DATE or manifest["policy"] != POLICY:
        raise RedditCitationMediaHandoffError("handoff policy or snapshot date differs")
    _timestamp(manifest["observed_at"], "handoff observed_at")
    inputs = _exact(
        manifest["inputs"],
        {
            "provenance_file",
            "provenance_sha256",
            "citation_inventory_file",
            "citation_inventory_sha256",
            "ffprobe_path",
            "ffprobe_sha256",
            "ffprobe_version",
        },
        "handoff inputs",
    )
    if inputs != {
        "provenance_file": "provenance.json",
        "provenance_sha256": FROZEN_PROVENANCE_SHA256,
        "citation_inventory_file": "citation-inventory.json",
        "citation_inventory_sha256": FROZEN_INVENTORY_SHA256,
        "ffprobe_path": str(FFPROBE_PATH),
        "ffprobe_sha256": FFPROBE_SHA256,
        "ffprobe_version": FFPROBE_VERSION,
    }:
        raise RedditCitationMediaHandoffError("handoff input seals differ")
    contexts = manifest["contexts"]
    media = manifest["media"]
    if not isinstance(contexts, list) or not isinstance(media, list):
        raise RedditCitationMediaHandoffError("handoff context/media arrays are invalid")
    identity = {
        "snapshot_date": FROZEN_SNAPSHOT_DATE,
        "provenance_sha256": FROZEN_PROVENANCE_SHA256,
        "citation_inventory_sha256": FROZEN_INVENTORY_SHA256,
        "media": [row.get("sha256") for row in media if isinstance(row, dict)],
    }
    if manifest["handoff_id"] != _derived_id("rcmh", identity):
        raise RedditCitationMediaHandoffError("handoff_id does not match frozen inputs/media")


def _open_bundle(
    manifest_path: Path, *, expected_media_sha256s: list[str]
) -> _BundleSession:
    pins = _Pins()
    ffprobe: _PinnedFile | None = None
    try:
        manifest_pin = pins.open(
            Path(manifest_path),
            maximum=MAX_JSON_BYTES,
            label="Reddit citation-media handoff manifest",
            guard_writable=False,
        )
        manifest = _strict_json(manifest_pin.body, "Reddit citation-media handoff manifest")
        if manifest_pin.body != _canonical_bytes(manifest) + b"\n":
            raise RedditCitationMediaHandoffError(
                "Reddit citation-media handoff manifest is not canonical JSON"
            )
        _validate_bundle_manifest_shape(manifest)
        manifest_media_sha256s = [
            row.get("sha256") if isinstance(row, dict) else None
            for row in manifest["media"]
        ]
        if manifest_media_sha256s != expected_media_sha256s:
            raise RedditCitationMediaHandoffError(
                "handoff media hashes/order differ from the frozen snapshot"
            )
        root = manifest_pin.resolved_path.parent
        media_paths: dict[str, Path] = {}
        seen_contexts: set[str] = set()
        context_by_id: dict[str, dict[str, Any]] = {}
        for index, context in enumerate(manifest["contexts"]):
            context = _exact(
                context,
                {
                    "context_id",
                    "context_source_id",
                    "source_kind",
                    "native_id",
                    "citation_url",
                    "subreddit",
                    "post_id",
                    "comment_id",
                    "comment_body_sha256",
                    "comment_body_bytes",
                    "embed",
                    "media_sha256s",
                },
                f"handoff context {index}",
            )
            context_id = _bounded_text(context["context_id"], "context_id", 64)
            if context_id in seen_contexts or context_id != stable_id("rctx", context["citation_url"]):
                raise RedditCitationMediaHandoffError("handoff context identity is duplicate or invalid")
            seen_contexts.add(context_id)
            expected_native = (
                context["post_id"]
                if context["comment_id"] is None
                else f"{context['post_id']}:{context['comment_id']}"
            )
            expected_kind = "reddit_post" if context["comment_id"] is None else "reddit_comment"
            if (
                context["native_id"] != expected_native
                or context["source_kind"] != expected_kind
                or context["context_source_id"]
                != source_id("reddit", expected_kind, expected_native)
            ):
                raise RedditCitationMediaHandoffError("handoff context source identity is invalid")
            if context["comment_id"] is not None:
                _sha256(context["comment_body_sha256"], "handoff comment body SHA-256")
                _bounded_int(context["comment_body_bytes"], "handoff comment body bytes", MAX_CONTEXT_BYTES)
            embed = _exact(
                context["embed"],
                {
                    "provenance_source_id",
                    "request_url",
                    "final_url",
                    "http_status",
                    "retrieved_at",
                    "artifact_path",
                    "gzip_sha256",
                    "compressed_bytes",
                    "payload_sha256",
                    "uncompressed_bytes",
                },
                "handoff context embed",
            )
            _sha256(embed["gzip_sha256"], "handoff embed gzip SHA-256")
            _sha256(embed["payload_sha256"], "handoff embed payload SHA-256")
            _timestamp(embed["retrieved_at"], "handoff embed retrieved_at")
            if embed["http_status"] != 200:
                raise RedditCitationMediaHandoffError("handoff embed snapshot is not HTTP 200")
            if not isinstance(context["media_sha256s"], list) or not context["media_sha256s"]:
                raise RedditCitationMediaHandoffError("handoff context has no media")
            context_by_id[context_id] = context
        if len(context_by_id) != FROZEN_CONTEXT_COUNT:
            raise RedditCitationMediaHandoffError("handoff context count differs")

        ffprobe = _ffprobe_pin()
        durations = 0
        kinds: dict[str, int] = {"video": 0, "image": 0}
        all_context_hashes: list[str] = []
        for context in context_by_id.values():
            all_context_hashes.extend(context["media_sha256s"])
        seen_media: set[str] = set()
        for index, row in enumerate(manifest["media"]):
            row = _exact(
                row,
                {
                    "media_id",
                    "sha256",
                    "byte_count",
                    "media_kind",
                    "mime_type",
                    "suffix",
                    "materialized_path",
                    "context_id",
                    "context_source_id",
                    "media_source_id",
                    "media_url",
                    "provenance_source_id",
                    "retrieved_at",
                    "request_url",
                    "final_url",
                    "http_status",
                    "gzip_artifact_path",
                    "gzip_sha256",
                    "gzip_byte_count",
                    "probe",
                    "container",
                    "duration_ms",
                    "processing_disposition",
                },
                f"handoff media {index}",
            )
            digest = _sha256(row["sha256"], "handoff media SHA-256")
            if digest in seen_media or row["media_id"] != f"media_sha256_{digest}":
                raise RedditCitationMediaHandoffError("handoff media identity is duplicate or invalid")
            seen_media.add(digest)
            context = context_by_id.get(row["context_id"])
            if context is None or row["context_source_id"] != context["context_source_id"]:
                raise RedditCitationMediaHandoffError("handoff media context binding is invalid")
            if digest not in context["media_sha256s"]:
                raise RedditCitationMediaHandoffError("handoff media is absent from its context")
            kind = row["media_kind"]
            if kind not in kinds:
                raise RedditCitationMediaHandoffError("handoff media kind is unsupported")
            expected_suffix = ".mp4" if kind == "video" else ".jpg"
            expected_mime = "video/mp4" if kind == "video" else "image/jpeg"
            if row["suffix"] != expected_suffix or row["mime_type"] != expected_mime:
                raise RedditCitationMediaHandoffError("handoff media type fields disagree")
            relative = _safe_relative(
                row["materialized_path"], "materialized media path", prefix="media"
            )
            expected_relative = Path("media") / "sha256" / digest[:2] / f"{digest}{expected_suffix}"
            if relative != expected_relative:
                raise RedditCitationMediaHandoffError("handoff media path is not content-addressed")
            _reject_symlink_components(root, relative, "materialized media path")
            pinned = pins.open(
                root / relative,
                maximum=MAX_MEDIA_BYTES,
                label=f"materialized media {digest}",
                guard_writable=False,
            )
            byte_count = _bounded_int(row["byte_count"], "handoff media byte count", MAX_MEDIA_BYTES)
            if pinned.digest != digest or len(pinned.body) != byte_count:
                raise RedditCitationMediaHandoffError("materialized media hash/size differs")
            if kind == "video" and (len(pinned.body) < 12 or pinned.body[4:8] != b"ftyp"):
                raise RedditCitationMediaHandoffError("materialized MP4 magic differs")
            if kind == "image" and (
                not pinned.body.startswith(b"\xff\xd8\xff") or not pinned.body.endswith(b"\xff\xd9")
            ):
                raise RedditCitationMediaHandoffError("materialized JPEG magic differs")
            actual_probe = _probe(
                pinned.resolved_path,
                ffprobe,
                kind,
                media_descriptor=pinned.descriptor,
            )
            if row["probe"] != actual_probe:
                raise RedditCitationMediaHandoffError("materialized media probe differs from manifest")
            if row["container"] != actual_probe["format"]["container"] or row["duration_ms"] != actual_probe["format"]["duration_ms"]:
                raise RedditCitationMediaHandoffError("handoff normalized media fields differ")
            expected_disposition = {
                "asr": (
                    "not_applicable_no_audio_stream"
                    if kind == "video"
                    else "not_applicable_non_audio_media"
                ),
                "ocr": "candidate_only_not_evaluated",
                "visual": "candidate_only_not_evaluated",
            }
            if row["processing_disposition"] != expected_disposition:
                raise RedditCitationMediaHandoffError("handoff processing disposition is unsafe")
            if row["media_source_id"] != source_id(
                "reddit", "reddit_declared_media", row["media_url"]
            ) or row["request_url"] != row["media_url"] or row["final_url"] != row["media_url"] or row["http_status"] != 200:
                raise RedditCitationMediaHandoffError("handoff media source identity differs")
            _timestamp(row["retrieved_at"], "handoff media retrieved_at")
            _sha256(row["gzip_sha256"], "handoff media gzip SHA-256")
            _bounded_int(row["gzip_byte_count"], "handoff gzip byte count", MAX_GZIP_BYTES)
            media_paths[digest] = pinned.resolved_path
            kinds[kind] += 1
            durations += row["duration_ms"] or 0
        if (
            len(seen_media) != FROZEN_MEDIA_COUNT
            or sorted(all_context_hashes) != sorted(seen_media)
            or kinds != {"video": FROZEN_VIDEO_COUNT, "image": FROZEN_IMAGE_COUNT}
            or durations != FROZEN_VIDEO_DURATION_MS
        ):
            raise RedditCitationMediaHandoffError("handoff media cardinalities differ")
        expected_statistics = {
            "contexts": FROZEN_CONTEXT_COUNT,
            "posts": FROZEN_POST_COUNT,
            "media": FROZEN_MEDIA_COUNT,
            "videos": FROZEN_VIDEO_COUNT,
            "images": FROZEN_IMAGE_COUNT,
            "video_duration_ms": FROZEN_VIDEO_DURATION_MS,
            "media_bytes": sum(row["byte_count"] for row in manifest["media"]),
        }
        if manifest["statistics"] != expected_statistics:
            raise RedditCitationMediaHandoffError("handoff statistics differ from exact rows")
        pins.verify()
        ffprobe.verify()
        ffprobe.close()
        ffprobe = None
        return _BundleSession(root, pins, manifest_pin, manifest, media_paths)
    except Exception:
        if ffprobe is not None:
            ffprobe.close()
        pins.close()
        raise


def _validate_snapshot_bundle(
    snapshot: _SnapshotSession, bundle: _BundleSession
) -> dict[str, Any]:
    expected = _bundle_manifest(
        snapshot,
        [
            {
                **{key: value for key, value in row.items() if key != "body"},
                "probe": bundle.manifest["media"][index]["probe"],
                "container": bundle.manifest["media"][index]["container"],
                "duration_ms": bundle.manifest["media"][index]["duration_ms"],
                "processing_disposition": bundle.manifest["media"][index][
                    "processing_disposition"
                ],
            }
            for index, row in enumerate(snapshot.media)
        ],
    )
    if bundle.manifest != expected:
        raise RedditCitationMediaHandoffError(
            "handoff manifest does not exactly derive from the frozen snapshot"
        )
    snapshot.verify()
    bundle.verify()
    return {
        "valid": True,
        "handoff_id": bundle.manifest["handoff_id"],
        "manifest_sha256": bundle.manifest_pin.digest,
        "statistics": bundle.manifest["statistics"],
        "publication_authority": False,
    }


def validate_reddit_citation_media_handoff(
    snapshot_dir: Path,
    manifest_path: Path,
    *,
    guard_writable_inputs: bool = False,
) -> dict[str, Any]:
    """Independently revalidate the frozen snapshot and materialized handoff."""

    snapshot = _open_snapshot(
        snapshot_dir, guard_writable_inputs=guard_writable_inputs
    )
    try:
        bundle = _open_bundle(
            manifest_path,
            expected_media_sha256s=[row["sha256"] for row in snapshot.media],
        )
    except Exception:
        snapshot.close()
        raise
    try:
        return _validate_snapshot_bundle(snapshot, bundle)
    finally:
        try:
            snapshot.verify()
            bundle.verify()
        finally:
            snapshot.close()
            bundle.close()


def _protected_tables(connection: sqlite3.Connection) -> list[str]:
    return [
        row["name"]
        for row in connection.execute(
            "SELECT name FROM sqlite_master WHERE type = 'table' "
            "AND name NOT LIKE 'sqlite_%' ORDER BY name"
        )
        if row["name"] not in ALLOWED_WRITE_TABLES
    ]


def _protected_counts(connection: sqlite3.Connection) -> dict[str, int]:
    return {
        table: int(connection.execute(f'SELECT count(*) FROM "{table}"').fetchone()[0])
        for table in _protected_tables(connection)
    }


def _install_protected_write_guards(connection: sqlite3.Connection) -> list[str]:
    """Install transaction-local TEMP triggers that reject protected writes.

    Count comparison remains a useful independent invariant, but cannot notice an
    in-place UPDATE.  These guards also catch writes performed by pre-existing main-
    schema triggers fired by an allowed-table insert.
    """

    trigger_names: list[str] = []
    for table in _protected_tables(connection):
        # SQLite cannot attach triggers to a virtual table itself.  Keep its
        # ordinary shadow tables protected: an attempted virtual-table write
        # reaches those tables and is rejected by the guards installed below.
        table_sql = connection.execute(
            "SELECT sql FROM sqlite_master WHERE type = 'table' AND name = ?",
            (table,),
        ).fetchone()[0]
        if table_sql and table_sql.lstrip().upper().startswith("CREATE VIRTUAL TABLE"):
            continue
        table_token = hashlib.sha256(table.encode("utf-8")).hexdigest()[:16]
        quoted_table = table.replace('"', '""')
        for operation in ("INSERT", "UPDATE", "DELETE"):
            name = f"rcmh_protected_{table_token}_{operation.lower()}"
            quoted_name = name.replace('"', '""')
            try:
                connection.execute(
                    f'CREATE TEMP TRIGGER "{quoted_name}" BEFORE {operation} '
                    f'ON main."{quoted_table}" BEGIN '
                    "SELECT RAISE(ABORT, 'reddit handoff protected-table write'); END"
                )
            except Exception:
                _drop_protected_write_guards(connection, trigger_names)
                raise
            trigger_names.append(name)
    return trigger_names


def _drop_protected_write_guards(
    connection: sqlite3.Connection, trigger_names: list[str]
) -> None:
    first_error: Exception | None = None
    for name in reversed(trigger_names):
        quoted_name = name.replace('"', '""')
        try:
            connection.execute(f'DROP TRIGGER temp."{quoted_name}"')
        except Exception as error:  # pragma: no cover - defensive connection failure
            if first_error is None:
                first_error = error
    if first_error is not None:
        raise first_error


@contextmanager
def _protected_write_guard(connection: sqlite3.Connection):
    trigger_names = _install_protected_write_guards(connection)
    try:
        yield
    finally:
        _drop_protected_write_guards(connection, trigger_names)


def _insert_source_snapshot(
    connection: sqlite3.Connection,
    *,
    snapshot_id: str,
    source: str,
    observed_at: str,
    request_url: str,
    final_url: str,
    http_status: int,
    payload_sha256: str,
    artifact_path: str,
    metadata: dict[str, Any],
    batch_id: str,
) -> None:
    values = (
        source,
        observed_at,
        request_url,
        final_url,
        http_status,
        payload_sha256,
        artifact_path,
        canonical_json(metadata),
        batch_id,
    )
    connection.execute(
        """
        INSERT OR IGNORE INTO source_snapshots(
            source_snapshot_id, source_id, observed_at, request_url, final_url,
            http_status, payload_sha256, artifact_path, metadata_json, import_batch_id
        ) VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (snapshot_id, *values),
    )
    row = connection.execute(
        """
        SELECT source_id, observed_at, request_url, final_url, http_status,
               payload_sha256, artifact_path, metadata_json, import_batch_id
        FROM source_snapshots WHERE source_snapshot_id = ?
        """,
        (snapshot_id,),
    ).fetchone()
    if row is None or tuple(row) != values:
        raise RedditCitationMediaHandoffError("existing source snapshot conflicts")


def _insert_candidate_relation(
    connection: sqlite3.Connection,
    *,
    from_source: str,
    to_source: str,
    batch_id: str,
    observed_at: str,
) -> None:
    relation_id = stable_id("sre", from_source, RELATION_KIND, to_source)
    metadata_text = canonical_json(RELATION_METADATA)
    connection.execute(
        """
        INSERT OR IGNORE INTO source_relations(
            source_relation_id, from_source_id, relation_kind, to_source_id,
            basis, confidence_state, metadata_json, import_batch_id
        ) VALUES(?, ?, ?, ?, ?, 'candidate', ?, ?)
        """,
        (
            relation_id,
            from_source,
            RELATION_KIND,
            to_source,
            RELATION_BASIS,
            metadata_text,
            batch_id,
        ),
    )
    row = connection.execute(
        """
        SELECT from_source_id, relation_kind, to_source_id, basis, confidence_state,
               metadata_json, import_batch_id
        FROM source_relations WHERE source_relation_id = ?
        """,
        (relation_id,),
    ).fetchone()
    expected = (
        from_source,
        RELATION_KIND,
        to_source,
        RELATION_BASIS,
        "candidate",
        metadata_text,
        batch_id,
    )
    if row is None or tuple(row) != expected:
        raise RedditCitationMediaHandoffError("existing candidate source relation conflicts")
    payload = {
        "basis": RELATION_BASIS,
        "confidence_state": "candidate",
        "metadata": RELATION_METADATA,
    }
    candidate_sha = sha256_bytes(_canonical_bytes(payload))
    import_observation_id = stable_id("iob", batch_id, __version__, observed_at)
    observation_id = stable_id("sro", relation_id, import_observation_id, candidate_sha)
    connection.execute(
        """
        INSERT OR IGNORE INTO source_relation_observations(
            source_relation_observation_id, source_relation_id, import_batch_id,
            import_observation_id, observed_at, quality_rank, quality_basis,
            candidate_sha256, basis, confidence_state, metadata_json
        ) VALUES(?, ?, ?, ?, ?, 200, ?, ?, ?, 'candidate', ?)
        """,
        (
            observation_id,
            relation_id,
            batch_id,
            import_observation_id,
            observed_at,
            f"{IMPORTER_NAME}: frozen exact response candidate",
            candidate_sha,
            RELATION_BASIS,
            metadata_text,
        ),
    )
    connection.execute(
        "UPDATE source_relations SET current_relation_observation_id = ? "
        "WHERE source_relation_id = ? AND current_relation_observation_id IS NULL",
        (observation_id, relation_id),
    )


def _insert_review_task(
    connection: sqlite3.Connection,
    *,
    task_kind: str,
    media_id: str,
    reason: str,
    priority: int,
    observed_at: str,
) -> None:
    task_id = stable_id("rtk", task_kind, "media_object", media_id)
    insert_values = (
        task_kind,
        "media_object",
        media_id,
        reason,
        priority,
        observed_at,
        observed_at,
    )
    connection.execute(
        """
        INSERT OR IGNORE INTO review_tasks(
            review_task_id, task_kind, target_type, target_id, reason, priority,
            status, created_at, updated_at
        ) VALUES(?, ?, ?, ?, ?, ?, 'open', ?, ?)
        """,
        (task_id, *insert_values),
    )
    row = connection.execute(
        """
        SELECT task_kind, target_type, target_id, reason, priority, created_at
        FROM review_tasks WHERE review_task_id = ?
        """,
        (task_id,),
    ).fetchone()
    expected = (task_kind, "media_object", media_id, reason, priority, observed_at)
    if row is None or tuple(row) != expected:
        raise RedditCitationMediaHandoffError("existing Reddit media review task conflicts")


def _verify_media_row(
    connection: sqlite3.Connection, row: dict[str, Any], media_path: Path, observed_at: str
) -> None:
    media = connection.execute(
        """
        SELECT sha256, byte_count, media_kind, mime_type, container, duration_ms,
               ffprobe_json, integrity_state
        FROM media_objects WHERE media_id = ?
        """,
        (row["media_id"],),
    ).fetchone()
    expected = (
        row["sha256"],
        row["byte_count"],
        row["media_kind"],
        row["mime_type"],
        row["container"],
        row["duration_ms"],
        canonical_json(row["probe"]),
        "verified",
    )
    if media is None or tuple(media) != expected:
        raise RedditCitationMediaHandoffError("existing media object conflicts")
    location = connection.execute(
        """
        SELECT media_id, storage_uri, storage_class, verified_at, is_primary
        FROM media_locations WHERE media_location_id = ?
        """,
        (stable_id("mlc", row["media_id"], media_path.as_uri()),),
    ).fetchone()
    if location is None or tuple(location) != (
        row["media_id"],
        media_path.as_uri(),
        "private_content_addressed_readonly",
        observed_at,
        0,
    ):
        raise RedditCitationMediaHandoffError("existing read-only media location conflicts")


def _verify_source_metadata_observation(
    connection: sqlite3.Connection,
    *,
    source: str,
    parent_source: str | None,
    canonical_url: str,
    metadata: dict[str, Any],
    batch_id: str,
    observed_at: str,
) -> None:
    metadata_text = canonical_json(metadata)
    candidate_payload = {
        "parent_source_id": parent_source,
        "canonical_url": canonical_url,
        "historical_url": None,
        "title": None,
        "published_at": None,
        "access_state": "public",
        "review_state": "unreviewed",
        "metadata": metadata,
    }
    candidate_digest = sha256_bytes(canonical_json(candidate_payload).encode("utf-8"))
    import_observation_id = stable_id("iob", batch_id, __version__, observed_at)
    metadata_observation_id = stable_id(
        "smo", source, import_observation_id, candidate_digest
    )
    row = connection.execute(
        """
        SELECT source_id, import_batch_id, import_observation_id, observed_at,
               quality_rank, quality_basis, candidate_sha256, parent_source_id,
               canonical_url, historical_url, title, published_at, access_state,
               review_state, metadata_json
        FROM source_metadata_observations
        WHERE source_metadata_observation_id = ?
        """,
        (metadata_observation_id,),
    ).fetchone()
    expected = (
        source,
        batch_id,
        import_observation_id,
        observed_at,
        200,
        f"{IMPORTER_NAME}: unclassified metadata importer",
        candidate_digest,
        parent_source,
        canonical_url,
        None,
        None,
        None,
        "public",
        "unreviewed",
        metadata_text,
    )
    if row is None or tuple(row) != expected:
        raise RedditCitationMediaHandoffError(
            "completed handoff source-metadata observation conflicts"
        )


def _verify_completed_import_rows(
    connection: sqlite3.Connection,
    *,
    snapshot: _SnapshotSession,
    bundle: _BundleSession,
    batch_id: str,
    observed_at: str,
    statistics: dict[str, Any],
) -> None:
    statistics_text = canonical_json(statistics)
    batch = connection.execute(
        """
        SELECT importer_name, importer_version, input_sha256, source_snapshot_date,
               started_at, completed_at, status, statistics_json
        FROM import_batches WHERE import_batch_id = ?
        """,
        (batch_id,),
    ).fetchone()
    if batch is None or tuple(batch) != (
        IMPORTER_NAME,
        __version__,
        bundle.manifest_pin.digest,
        FROZEN_SNAPSHOT_DATE,
        observed_at,
        observed_at,
        "completed",
        statistics_text,
    ):
        raise RedditCitationMediaHandoffError("completed handoff batch conflicts")
    import_observation_id = stable_id("iob", batch_id, __version__, observed_at)
    import_observation = connection.execute(
        """
        SELECT import_batch_id, importer_version, source_snapshot_date, observed_at,
               completed_at, status, statistics_json
        FROM import_observations WHERE import_observation_id = ?
        """,
        (import_observation_id,),
    ).fetchone()
    if import_observation is None or tuple(import_observation) != (
        batch_id,
        __version__,
        FROZEN_SNAPSHOT_DATE,
        observed_at,
        observed_at,
        "completed",
        statistics_text,
    ):
        raise RedditCitationMediaHandoffError(
            "completed handoff import observation conflicts"
        )

    expected_batch_counts = {
        "source_snapshots": FROZEN_CONTEXT_COUNT + FROZEN_MEDIA_COUNT,
        "source_metadata_observations": FROZEN_CONTEXT_COUNT + FROZEN_MEDIA_COUNT,
        "source_relations": FROZEN_MEDIA_COUNT,
        "source_relation_observations": FROZEN_MEDIA_COUNT,
    }
    for table, expected_count in expected_batch_counts.items():
        actual = int(
            connection.execute(
                f'SELECT count(*) FROM "{table}" WHERE import_batch_id = ?',
                (batch_id,),
            ).fetchone()[0]
        )
        if actual != expected_count:
            raise RedditCitationMediaHandoffError(
                f"completed handoff has {actual} {table} rows; expected {expected_count}"
            )

    for context in bundle.manifest["contexts"]:
        source = connection.execute(
            "SELECT platform, source_kind, native_id FROM sources WHERE source_id = ?",
            (context["context_source_id"],),
        ).fetchone()
        if source is None or tuple(source) != (
            "reddit",
            context["source_kind"],
            context["native_id"],
        ):
            raise RedditCitationMediaHandoffError(
                "completed handoff context-source identity conflicts"
            )
        _verify_source_metadata_observation(
            connection,
            source=context["context_source_id"],
            parent_source=None,
            canonical_url=context["citation_url"],
            metadata={
                "assertion_state": "candidate_only_unreviewed",
                "subreddit": context["subreddit"],
                "post_id": context["post_id"],
                "comment_id": context["comment_id"],
                "comment_body_sha256": context["comment_body_sha256"],
                "comment_body_bytes": context["comment_body_bytes"],
                "claims_linked": False,
                "publication_authority": False,
            },
            batch_id=batch_id,
            observed_at=observed_at,
        )
        embed = context["embed"]
        context_snapshot_id = stable_id(
            "ssn",
            context["context_source_id"],
            embed["retrieved_at"],
            embed["payload_sha256"],
        )
        source_snapshot = connection.execute(
            """
            SELECT source_id, observed_at, request_url, final_url, http_status,
                   payload_sha256, artifact_path, metadata_json, import_batch_id
            FROM source_snapshots WHERE source_snapshot_id = ?
            """,
            (context_snapshot_id,),
        ).fetchone()
        if source_snapshot is None or tuple(source_snapshot) != (
            context["context_source_id"],
            embed["retrieved_at"],
            embed["request_url"],
            embed["final_url"],
            embed["http_status"],
            embed["payload_sha256"],
            (snapshot.root / embed["artifact_path"]).as_uri(),
            canonical_json(
                {
                    "content_encoding": "gzip",
                    "gzip_sha256": embed["gzip_sha256"],
                    "compressed_bytes": embed["compressed_bytes"],
                    "uncompressed_bytes": embed["uncompressed_bytes"],
                    "provenance_source_id": embed["provenance_source_id"],
                    "semantic_scope": "citation identity only",
                }
            ),
            batch_id,
        ):
            raise RedditCitationMediaHandoffError(
                "completed handoff context snapshot conflicts"
            )

    relation_payload = {
        "basis": RELATION_BASIS,
        "confidence_state": "candidate",
        "metadata": RELATION_METADATA,
    }
    relation_candidate_sha = sha256_bytes(_canonical_bytes(relation_payload))
    for row in bundle.manifest["media"]:
        source = connection.execute(
            "SELECT platform, source_kind, native_id FROM sources WHERE source_id = ?",
            (row["media_source_id"],),
        ).fetchone()
        if source is None or tuple(source) != (
            "reddit",
            "reddit_declared_media",
            row["media_url"],
        ):
            raise RedditCitationMediaHandoffError(
                "completed handoff media-source identity conflicts"
            )
        _verify_source_metadata_observation(
            connection,
            source=row["media_source_id"],
            parent_source=row["context_source_id"],
            canonical_url=row["media_url"],
            metadata={
                "assertion_state": "candidate_only_unreviewed",
                "declared_media_type": row["media_kind"],
                "processing_disposition": row["processing_disposition"],
                "context_complete": False,
                "identity_asserted": False,
                "publication_authority": False,
            },
            batch_id=batch_id,
            observed_at=observed_at,
        )
        media_snapshot_id = stable_id(
            "ssn", row["media_source_id"], row["retrieved_at"], row["sha256"]
        )
        source_snapshot = connection.execute(
            """
            SELECT source_id, observed_at, request_url, final_url, http_status,
                   payload_sha256, artifact_path, metadata_json, import_batch_id
            FROM source_snapshots WHERE source_snapshot_id = ?
            """,
            (media_snapshot_id,),
        ).fetchone()
        if source_snapshot is None or tuple(source_snapshot) != (
            row["media_source_id"],
            row["retrieved_at"],
            row["request_url"],
            row["final_url"],
            row["http_status"],
            row["sha256"],
            bundle.media_paths[row["sha256"]].as_uri(),
            canonical_json(
                {
                    "original_content_encoding": "gzip",
                    "original_gzip_artifact_path": (
                        snapshot.root / row["gzip_artifact_path"]
                    ).as_uri(),
                    "original_gzip_sha256": row["gzip_sha256"],
                    "original_gzip_byte_count": row["gzip_byte_count"],
                    "provenance_source_id": row["provenance_source_id"],
                    "semantic_scope": "exact declared-media response only",
                }
            ),
            batch_id,
        ):
            raise RedditCitationMediaHandoffError(
                "completed handoff media snapshot conflicts"
            )
        relation_id = stable_id(
            "sre", row["context_source_id"], RELATION_KIND, row["media_source_id"]
        )
        relation = connection.execute(
            """
            SELECT from_source_id, relation_kind, to_source_id, basis,
                   confidence_state, metadata_json, import_batch_id
            FROM source_relations WHERE source_relation_id = ?
            """,
            (relation_id,),
        ).fetchone()
        if relation is None or tuple(relation) != (
            row["context_source_id"],
            RELATION_KIND,
            row["media_source_id"],
            RELATION_BASIS,
            "candidate",
            canonical_json(RELATION_METADATA),
            batch_id,
        ):
            raise RedditCitationMediaHandoffError(
                "completed handoff candidate relation identity conflicts"
            )
        relation_observation_id = stable_id(
            "sro", relation_id, import_observation_id, relation_candidate_sha
        )
        relation_observation = connection.execute(
            """
            SELECT source_relation_id, import_batch_id, import_observation_id,
                   observed_at, quality_rank, quality_basis, candidate_sha256,
                   basis, confidence_state, metadata_json
            FROM source_relation_observations
            WHERE source_relation_observation_id = ?
            """,
            (relation_observation_id,),
        ).fetchone()
        if relation_observation is None or tuple(relation_observation) != (
            relation_id,
            batch_id,
            import_observation_id,
            observed_at,
            200,
            f"{IMPORTER_NAME}: frozen exact response candidate",
            relation_candidate_sha,
            RELATION_BASIS,
            "candidate",
            canonical_json(RELATION_METADATA),
        ):
            raise RedditCitationMediaHandoffError(
                "completed handoff candidate observation conflicts"
            )
        media_source_link_id = stable_id(
            "mso", row["media_id"], row["media_source_id"]
        )
        media_source_link = connection.execute(
            """
            SELECT media_id, source_id, retrieved_at, retrieval_tool,
                   retrieval_tool_version, source_snapshot_id
            FROM media_sources WHERE media_source_id = ?
            """,
            (media_source_link_id,),
        ).fetchone()
        if media_source_link is None or tuple(media_source_link) != (
            row["media_id"],
            row["media_source_id"],
            row["retrieved_at"],
            RETRIEVAL_TOOL,
            RETRIEVAL_TOOL_VERSION,
            media_snapshot_id,
        ):
            raise RedditCitationMediaHandoffError(
                "completed handoff media-source lineage conflicts"
            )
        _verify_media_row(
            connection, row, bundle.media_paths[row["sha256"]], observed_at
        )
        for task_kind, reason, priority in REVIEW_TASK_SPECS:
            task_id = stable_id("rtk", task_kind, "media_object", row["media_id"])
            task = connection.execute(
                """
                SELECT task_kind, target_type, target_id, reason, priority, created_at
                FROM review_tasks WHERE review_task_id = ?
                """,
                (task_id,),
            ).fetchone()
            if task is None or tuple(task) != (
                task_kind,
                "media_object",
                row["media_id"],
                reason,
                priority,
                observed_at,
            ):
                raise RedditCitationMediaHandoffError(
                    "completed handoff review-task identity conflicts"
                )


def import_reddit_citation_media_handoff(
    connection: sqlite3.Connection,
    snapshot_dir: Path,
    manifest_path: Path,
    *,
    guard_writable_inputs: bool = False,
) -> dict[str, Any]:
    """Import only candidate source/media rows and private review tasks."""

    snapshot = _open_snapshot(
        snapshot_dir, guard_writable_inputs=guard_writable_inputs
    )
    try:
        bundle = _open_bundle(
            manifest_path,
            expected_media_sha256s=[row["sha256"] for row in snapshot.media],
        )
    except Exception:
        snapshot.close()
        raise
    try:
        # Exact derivation comparison also prevents a valid but unrelated bundle from
        # being paired with a writable snapshot guarded by the caller.  Reuse this
        # operation's pinned sessions rather than holding a second 300+ MiB pair.
        validation = _validate_snapshot_bundle(snapshot, bundle)
        observed_at = bundle.manifest["observed_at"]
        manifest_digest = bundle.manifest_pin.digest
        statistics = {
            **bundle.manifest["statistics"],
            "context_sources_total": FROZEN_CONTEXT_COUNT,
            "media_sources_total": FROZEN_MEDIA_COUNT,
            "source_snapshots_total": FROZEN_CONTEXT_COUNT + FROZEN_MEDIA_COUNT,
            "candidate_relations_total": FROZEN_MEDIA_COUNT,
            "review_tasks_total": FROZEN_MEDIA_COUNT * 3,
            "recordings_created": 0,
            "renditions_created": 0,
            "transcripts_created": 0,
            "claim_links_created": 0,
            "publication_decisions_created": 0,
            "publication_gate_decisions_created": 0,
        }
        with transaction(connection), _protected_write_guard(connection):
            protected_before = _protected_counts(connection)
            batch_id, existing = _begin_batch(
                connection,
                IMPORTER_NAME,
                manifest_digest,
                FROZEN_SNAPSHOT_DATE,
                observed_at,
            )
            if existing is not None:
                if existing != statistics:
                    raise RedditCitationMediaHandoffError(
                        "completed handoff import statistics conflict"
                    )
                _verify_completed_import_rows(
                    connection,
                    snapshot=snapshot,
                    bundle=bundle,
                    batch_id=batch_id,
                    observed_at=observed_at,
                    statistics=statistics,
                )
                snapshot.verify()
                bundle.verify()
                if _protected_counts(connection) != protected_before:
                    raise RedditCitationMediaHandoffError("replay changed a protected table")
                return {
                    "import_batch_id": batch_id,
                    "handoff_id": bundle.manifest["handoff_id"],
                    "statistics": statistics,
                    "replayed": True,
                    "publication_authority": False,
                }

            context_by_id = {row["context_id"]: row for row in bundle.manifest["contexts"]}
            for context in bundle.manifest["contexts"]:
                _upsert_source(
                    connection,
                    source=context["context_source_id"],
                    platform="reddit",
                    source_kind=context["source_kind"],
                    native_id=context["native_id"],
                    canonical_url=context["citation_url"],
                    observed_at=observed_at,
                    batch_id=batch_id,
                    access_state="public",
                    review_state="unreviewed",
                    metadata={
                        "assertion_state": "candidate_only_unreviewed",
                        "subreddit": context["subreddit"],
                        "post_id": context["post_id"],
                        "comment_id": context["comment_id"],
                        "comment_body_sha256": context["comment_body_sha256"],
                        "comment_body_bytes": context["comment_body_bytes"],
                        "claims_linked": False,
                        "publication_authority": False,
                    },
                )
                embed = context["embed"]
                context_snapshot_id = stable_id(
                    "ssn",
                    context["context_source_id"],
                    embed["retrieved_at"],
                    embed["payload_sha256"],
                )
                _insert_source_snapshot(
                    connection,
                    snapshot_id=context_snapshot_id,
                    source=context["context_source_id"],
                    observed_at=embed["retrieved_at"],
                    request_url=embed["request_url"],
                    final_url=embed["final_url"],
                    http_status=embed["http_status"],
                    payload_sha256=embed["payload_sha256"],
                    artifact_path=(snapshot.root / embed["artifact_path"]).as_uri(),
                    metadata={
                        "content_encoding": "gzip",
                        "gzip_sha256": embed["gzip_sha256"],
                        "compressed_bytes": embed["compressed_bytes"],
                        "uncompressed_bytes": embed["uncompressed_bytes"],
                        "provenance_source_id": embed["provenance_source_id"],
                        "semantic_scope": "citation identity only",
                    },
                    batch_id=batch_id,
                )

            for row in bundle.manifest["media"]:
                context = context_by_id[row["context_id"]]
                _upsert_source(
                    connection,
                    source=row["media_source_id"],
                    platform="reddit",
                    source_kind="reddit_declared_media",
                    native_id=row["media_url"],
                    parent_source=context["context_source_id"],
                    canonical_url=row["media_url"],
                    observed_at=observed_at,
                    batch_id=batch_id,
                    access_state="public",
                    review_state="unreviewed",
                    metadata={
                        "assertion_state": "candidate_only_unreviewed",
                        "declared_media_type": row["media_kind"],
                        "processing_disposition": row["processing_disposition"],
                        "context_complete": False,
                        "identity_asserted": False,
                        "publication_authority": False,
                    },
                )
                media_snapshot_id = stable_id(
                    "ssn", row["media_source_id"], row["retrieved_at"], row["sha256"]
                )
                _insert_source_snapshot(
                    connection,
                    snapshot_id=media_snapshot_id,
                    source=row["media_source_id"],
                    observed_at=row["retrieved_at"],
                    request_url=row["request_url"],
                    final_url=row["final_url"],
                    http_status=row["http_status"],
                    payload_sha256=row["sha256"],
                    artifact_path=bundle.media_paths[row["sha256"]].as_uri(),
                    metadata={
                        "original_content_encoding": "gzip",
                        "original_gzip_artifact_path": (
                            snapshot.root / row["gzip_artifact_path"]
                        ).as_uri(),
                        "original_gzip_sha256": row["gzip_sha256"],
                        "original_gzip_byte_count": row["gzip_byte_count"],
                        "provenance_source_id": row["provenance_source_id"],
                        "semantic_scope": "exact declared-media response only",
                    },
                    batch_id=batch_id,
                )
                _insert_candidate_relation(
                    connection,
                    from_source=context["context_source_id"],
                    to_source=row["media_source_id"],
                    batch_id=batch_id,
                    observed_at=observed_at,
                )
                connection.execute(
                    """
                    INSERT OR IGNORE INTO media_objects(
                        media_id, sha256, byte_count, media_kind, mime_type, container,
                        duration_ms, ffprobe_json, first_cataloged_at, integrity_state
                    ) VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, 'verified')
                    """,
                    (
                        row["media_id"],
                        row["sha256"],
                        row["byte_count"],
                        row["media_kind"],
                        row["mime_type"],
                        row["container"],
                        row["duration_ms"],
                        canonical_json(row["probe"]),
                        observed_at,
                    ),
                )
                location_uri = bundle.media_paths[row["sha256"]].as_uri()
                location_id = stable_id("mlc", row["media_id"], location_uri)
                connection.execute(
                    """
                    INSERT OR IGNORE INTO media_locations(
                        media_location_id, media_id, storage_uri, storage_class,
                        verified_at, is_primary
                    ) VALUES(?, ?, ?, 'private_content_addressed_readonly', ?, 0)
                    """,
                    (location_id, row["media_id"], location_uri, observed_at),
                )
                media_source_link_id = stable_id(
                    "mso", row["media_id"], row["media_source_id"]
                )
                connection.execute(
                    """
                    INSERT OR IGNORE INTO media_sources(
                        media_source_id, media_id, source_id, retrieved_at,
                        retrieval_tool, retrieval_tool_version, source_snapshot_id
                    ) VALUES(?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        media_source_link_id,
                        row["media_id"],
                        row["media_source_id"],
                        row["retrieved_at"],
                        RETRIEVAL_TOOL,
                        RETRIEVAL_TOOL_VERSION,
                        media_snapshot_id,
                    ),
                )
                media_source_link = connection.execute(
                    """
                    SELECT media_id, source_id, retrieved_at, retrieval_tool,
                           retrieval_tool_version, source_snapshot_id
                    FROM media_sources WHERE media_source_id = ?
                    """,
                    (media_source_link_id,),
                ).fetchone()
                expected_media_source_link = (
                    row["media_id"],
                    row["media_source_id"],
                    row["retrieved_at"],
                    RETRIEVAL_TOOL,
                    RETRIEVAL_TOOL_VERSION,
                    media_snapshot_id,
                )
                if (
                    media_source_link is None
                    or tuple(media_source_link) != expected_media_source_link
                ):
                    raise RedditCitationMediaHandoffError(
                        "existing media-source lineage conflicts"
                    )
                _verify_media_row(
                    connection, row, bundle.media_paths[row["sha256"]], observed_at
                )
                for task_kind, reason, priority in REVIEW_TASK_SPECS:
                    _insert_review_task(
                        connection,
                        task_kind=task_kind,
                        media_id=row["media_id"],
                        reason=reason,
                        priority=priority,
                        observed_at=observed_at,
                    )

            _complete_batch(connection, batch_id, observed_at, statistics)
            _verify_completed_import_rows(
                connection,
                snapshot=snapshot,
                bundle=bundle,
                batch_id=batch_id,
                observed_at=observed_at,
                statistics=statistics,
            )
            snapshot.verify()
            bundle.verify()
            if _protected_counts(connection) != protected_before:
                raise RedditCitationMediaHandoffError(
                    "Reddit citation-media import changed a protected table"
                )
        return {
            "import_batch_id": batch_id,
            "handoff_id": bundle.manifest["handoff_id"],
            "manifest_sha256": validation["manifest_sha256"],
            "statistics": statistics,
            "replayed": False,
            "publication_authority": False,
        }
    finally:
        try:
            snapshot.verify()
            bundle.verify()
        finally:
            snapshot.close()
            bundle.close()
