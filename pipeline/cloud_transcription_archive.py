"""Offline, hash-bound raw recording inventory for cloud transcription.

Replays small acquisition receipts, never raw media contents. All physical
recordings remain represented, including missing media, silent files, and
identity collisions requiring review. This is a selection snapshot, not an
upload authorization, a media integrity audit, or a reservation of live work.
"""
from __future__ import annotations

import argparse
from collections import Counter, defaultdict
from contextlib import contextmanager
from datetime import date as calendar_date
import hashlib
import json
import os
from pathlib import Path
import re
import stat
import sys
import uuid

KIND = "himr_cloud_transcription_archive_inventory"
SOURCE_KIND = "himr_private_speaker_screen_archive_inventory"
SHA = re.compile(r"[0-9a-f]{64}\Z")
YOUTUBE = re.compile(r"[A-Za-z0-9_-]{11}\Z")
MAX_JSON = 64 * 1024**2
MAX_RECORDS = 100_000
MAX_ALIASES = 200_000


class ArchiveInventoryError(RuntimeError):
    """The externally pinned archive metadata cannot be safely interpreted."""


def canonical(value):
    return (json.dumps(value, sort_keys=True, separators=(",", ":"),
                       ensure_ascii=True, allow_nan=False) + "\n").encode("utf-8")


def _path(value):
    raw = str(value) if isinstance(value, (str, Path)) else ""
    path = Path(raw)
    if (not path.is_absolute() or path == Path("/") or str(path) != raw
            or any(part in {".", ".."} for part in raw.split("/"))
            or "\\" in raw or any(ord(c) < 32 for c in raw) or len(raw.encode()) > 4096):
        raise ArchiveInventoryError("normalized absolute non-root path required")
    return path


def _ref(value):
    if (not isinstance(value, dict) or set(value) != {"path", "sha256"}
            or not isinstance(value["sha256"], str) or not SHA.fullmatch(value["sha256"])):
        raise ArchiveInventoryError("invalid hash-bound metadata reference")
    _path(value["path"])
    return value


@contextmanager
def _parent(path):
    """Retain every directory traversal; symlink ancestors are never followed."""
    path = _path(path)
    fd = os.open("/", os.O_RDONLY | os.O_DIRECTORY | os.O_CLOEXEC)
    try:
        for part in path.parts[1:-1]:
            child = os.open(part, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW | os.O_CLOEXEC,
                            dir_fd=fd)
            os.close(fd)
            fd = child
        yield fd
    finally:
        os.close(fd)


def _safe_file(info):
    if (not stat.S_ISREG(info.st_mode) or info.st_nlink != 1
            or info.st_uid not in {0, os.geteuid()} or info.st_mode & 0o022):
        raise ArchiveInventoryError("input must be an owned, non-peer-writable regular file")


def _witness(info):
    return {key: getattr(info, key) for key in (
        "st_dev", "st_ino", "st_size", "st_mtime_ns", "st_ctime_ns", "st_mode", "st_uid", "st_nlink")}


def _pairs(items):
    value = {}
    for key, item in items:
        if key in value:
            raise ArchiveInventoryError("duplicate JSON key")
        value[key] = item
    return value


def _nonfinite(_):
    raise ArchiveInventoryError("nonfinite JSON value")


def read_bound(reference, *, maximum=MAX_JSON):
    """Read only bounded metadata, pinned by a caller-supplied content hash."""
    reference = _ref(reference)
    path = _path(reference["path"])
    with _parent(path) as directory:
        fd = os.open(path.name, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK | os.O_CLOEXEC,
                     dir_fd=directory)
        try:
            before = os.fstat(fd)
            _safe_file(before)
            if not 0 < before.st_size <= maximum:
                raise ArchiveInventoryError("metadata file exceeds its size bound")
            with os.fdopen(os.dup(fd), "rb") as stream:
                body = stream.read(maximum + 1)
            if len(body) != before.st_size or _witness(os.fstat(fd)) != _witness(before):
                raise ArchiveInventoryError("metadata changed while being read")
        finally:
            os.close(fd)
    if hashlib.sha256(body).hexdigest() != reference["sha256"]:
        raise ArchiveInventoryError("metadata SHA-256 mismatch: " + str(path))
    try:
        value = json.loads(body, object_pairs_hook=_pairs, parse_constant=_nonfinite)
    except (ValueError, UnicodeError, RecursionError) as error:
        raise ArchiveInventoryError("invalid metadata JSON") from error
    if not isinstance(value, dict):
        raise ArchiveInventoryError("metadata JSON must be an object")
    return value


def _integer(value, label):
    if type(value) is not int or not 0 < value < 2**63:
        raise ArchiveInventoryError(label + " must be a positive integer")
    return value


def _date(value):
    if (not isinstance(value, dict) or set(value) != {"value", "basis"}
            or not isinstance(value["basis"], str) or len(value["basis"]) > 128):
        raise ArchiveInventoryError("invalid date metadata")
    if value["value"] is not None:
        try:
            if calendar_date.fromisoformat(value["value"]).isoformat() != value["value"]:
                raise ValueError()
        except (ValueError, TypeError):
            raise ArchiveInventoryError("invalid date metadata") from None
    return value


def youtube_id(native_id, platform):
    """Retain a filename ID hint, never treat a title/date guess as an ID."""
    if platform == "youtube" and YOUTUBE.fullmatch(native_id):
        return native_id, "source_native_id"
    if platform != "internet_archive":
        return None, None
    name = native_id.rsplit("/", 1)[-1]
    # A filename suffix is a hint, not proof that two differently encoded files
    # contain the same full recording. Physical content hashes remain identity.
    match = re.search(r"(?:-|\[)([A-Za-z0-9_-]{11})(?:\])?\.(?:mp4|webm|mkv|m4a|mp3|flac|wav|avi|mov)\Z", name, re.I)
    return (match.group(1), "archive_filename_suffix") if match else (None, None)


def media_witness(path, expected_bytes):
    """Stat the retained path only. Raw media is not opened or read here."""
    path = _path(path)
    try:
        with _parent(path) as directory:
            info = os.stat(path.name, dir_fd=directory, follow_symlinks=False)
            _safe_file(info)
    except FileNotFoundError:
        return None, "missing_media"
    if info.st_size != expected_bytes:
        return _witness(info), "media_size_mismatch"
    return _witness(info), None


def _alias(alias, recording, read):
    if not isinstance(alias, dict):
        raise ArchiveInventoryError("invalid source alias")
    result_ref = _ref(alias.get("result"))
    result = read(result_ref)
    if result.get("status") != "completed" or result.get("errors") != []:
        raise ArchiveInventoryError("acquisition receipt is not successfully completed")
    if result.get("job_id") != alias.get("job_id"):
        raise ArchiveInventoryError("acquisition job binding differs")
    admission = result.get("admission", {})
    if not isinstance(admission, dict):
        raise ArchiveInventoryError("invalid acquisition media admission")
    if any(admission.get(key) != recording[key] for key in ("path", "sha256", "byte_count", "media_id")):
        raise ArchiveInventoryError("acquisition media binding differs")
    source = result.get("source", {})
    if not isinstance(source, dict):
        raise ArchiveInventoryError("invalid acquisition source metadata")
    native = alias.get("source_native_id")
    if (not isinstance(native, str) or not native or len(native) > 8192
            or source.get("native_id") != native or source.get("title") != alias.get("title")):
        raise ArchiveInventoryError("acquisition source identity differs")
    title = alias.get("title")
    if title is not None and (not isinstance(title, str) or len(title) > 8192):
        raise ArchiveInventoryError("invalid source title")
    platform = source.get("platform")
    if not isinstance(platform, str) or len(platform) > 128:
        raise ArchiveInventoryError("invalid source platform")
    url = source.get("canonical_url")
    if url is not None and (not isinstance(url, str) or len(url) > 16384):
        raise ArchiveInventoryError("invalid source URL metadata")
    youtube, basis = youtube_id(native, platform)
    return {"source_native_id": native, "title": title, "date": _date(alias.get("date")),
            "youtube_id": youtube, "youtube_id_basis": basis, "platform": platform,
            "canonical_url": url, "acquisition_result": result_ref}


def build_inventory(inventory_path, expected_sha256, *, additional_inventories=()):
    """Replay named metadata inventories, preserving every unique raw recording.

    Closed snapshots may retain historical live-controller source references;
    those are provenance only. Every actual acquisition alias receipt is freshly
    hash-verified. No controller checkpoint, service, or media content is read.
    """
    refs = [_ref({"path": str(inventory_path), "sha256": expected_sha256})]
    refs += [_ref(ref) for ref in additional_inventories]
    if len(refs) > 32 or len({ref["path"] for ref in refs}) != len(refs):
        raise ArchiveInventoryError("duplicate or excessive source inventories")
    records, refs_by_path, cache = {}, {}, {}
    metadata_bytes = 0
    inventory_paths = {ref["path"] for ref in refs}

    def read(ref):
        nonlocal metadata_bytes
        _ref(ref)
        if ref["path"] in refs_by_path and refs_by_path[ref["path"]] != ref["sha256"]:
            raise ArchiveInventoryError("one metadata path has conflicting hashes")
        refs_by_path[ref["path"]] = ref["sha256"]
        key = (ref["path"], ref["sha256"])
        if key not in cache:
            value = read_bound(ref, maximum=MAX_JSON if ref["path"] in inventory_paths else 4 * 1024**2)
            metadata_bytes += len(canonical(value))
            if metadata_bytes > 256 * 1024**2:
                raise ArchiveInventoryError("metadata snapshot exceeds its aggregate size bound")
            cache[key] = value
        return cache[key]

    source_rows = source_aliases = 0
    for reference in refs:
        document = read(reference)
        rows = document.get("records")
        if (document.get("kind") != SOURCE_KIND or type(document.get("schema_version")) is not int
                or document["schema_version"] != 1 or not isinstance(rows, list)
                or len(rows) > MAX_RECORDS):
            raise ArchiveInventoryError("unsupported source inventory")
        aliases_in_source = 0
        ids_in_source = set()
        for item in rows:
            if not isinstance(item, dict) or not isinstance(item.get("recording"), dict):
                raise ArchiveInventoryError("invalid source inventory recording")
            source_rows += 1
            if source_rows > MAX_RECORDS:
                raise ArchiveInventoryError("too many source recordings")
            recording = item.get("recording", {})
            sha = recording.get("sha256")
            if not isinstance(sha, str) or not SHA.fullmatch(sha):
                raise ArchiveInventoryError("invalid raw media SHA-256")
            identity = "media_sha256_" + sha
            if recording.get("media_id") != identity or identity in ids_in_source:
                raise ArchiveInventoryError("source inventory media identity is invalid or repeated")
            ids_in_source.add(identity)
            media = {key: recording.get(key) for key in ("path", "sha256", "byte_count")}
            _path(media["path"])
            _integer(media["byte_count"], "raw media byte_count")
            duration = recording.get("duration_hint_ms")
            if duration is not None:
                _integer(duration, "duration hint")
            audio = item.get("cached_audio_state")
            if audio not in {"audio_present", "no_audio_stream"}:
                raise ArchiveInventoryError("unsupported source audio state")
            aliases = item.get("aliases")
            if not isinstance(aliases, list) or not aliases:
                raise ArchiveInventoryError("recording must retain its source aliases")
            aliases_in_source += len(aliases)
            source_aliases += len(aliases)
            if source_aliases > MAX_ALIASES:
                raise ArchiveInventoryError("too many source aliases")
            primary = _ref(item.get("acquisition_result"))
            if any(not isinstance(alias, dict) for alias in aliases):
                raise ArchiveInventoryError("invalid source alias")
            if primary not in [alias.get("result") for alias in aliases]:
                raise ArchiveInventoryError("primary acquisition is missing from aliases")
            normalized = [_alias(alias, recording, read) for alias in aliases]
            reasons = []
            if duration is None:
                reasons.append("duration_unknown")
            if item.get("duration_hint_disagreement") is True:
                reasons.append("duration_hint_disagreement")
            if identity in records:
                old = records[identity]
                if old["media"] != media or old["duration_ms"] != duration or old["audio_state"] != audio:
                    raise ArchiveInventoryError("same physical identity has conflicting media metadata")
                old["aliases"].extend(normalized)
                old["reasons"].extend(reasons)
            else:
                witness, issue = media_witness(media["path"], media["byte_count"])
                if issue:
                    reasons.append(issue)
                records[identity] = {"recording_id": identity, "media": media, "duration_ms": duration,
                    "audio_state": audio, "aliases": normalized, "reasons": reasons, "source_witness": witness}
        counts = document.get("counts", {})
        if (not isinstance(counts, dict) or type(counts.get("unique_media")) is not int
                or type(counts.get("admitted_sources")) is not int
                or counts["unique_media"] != len(rows) or counts["admitted_sources"] != aliases_in_source):
            raise ArchiveInventoryError("source inventory coverage counts differ")

    owners = defaultdict(set)
    for identity, row in records.items():
        # Repeated identical admissions across snapshots are provenance duplicates,
        # not additional videos or new transcription jobs.
        row["aliases"] = [json.loads(body) for body in sorted({canonical(alias) for alias in row["aliases"]})]
        youtube = sorted({alias["youtube_id"] for alias in row["aliases"] if alias["youtube_id"]})
        native = sorted({alias["source_native_id"] for alias in row["aliases"] if alias["platform"] == "internet_archive"})
        row["source_ids"] = {"youtube": youtube, "archive_native": native}
        row["title"] = row["aliases"][0]["title"]
        row["date"] = row["aliases"][0]["date"]
        for alias in row["aliases"]:
            owners[(alias["platform"], alias["source_native_id"])].add(identity)
        for value in youtube:
            owners[("youtube", value)].add(identity)
        if len(youtube) > 1:
            row["reasons"].append("multiple_youtube_ids_for_one_physical_recording")
    conflicts = []
    for (platform, native), identities in sorted(owners.items()):
        if len(identities) > 1:
            conflicts.append({"platform": platform, "source_native_id": native,
                              "recording_ids": sorted(identities), "reason": "source_id_maps_to_multiple_physical_recordings"})
            for identity in identities:
                records[identity]["reasons"].append("source_id_maps_to_multiple_physical_recordings")
    for row in records.values():
        if row["audio_state"] == "no_audio_stream":
            row["reasons"].append("no_audio_stream")
        row["reasons"] = sorted(set(row["reasons"]))
        row["state"] = ("missing_media" if "missing_media" in row["reasons"] else
                        "no_audio" if row["audio_state"] == "no_audio_stream" else
                        "review" if row["reasons"] else "ready")
    rows = [records[key] for key in sorted(records)]
    states = Counter(row["state"] for row in rows)
    return {"kind": KIND, "schema_version": 1, "source_inventories": refs, "recordings": rows,
        "identity_conflicts": conflicts,
        "counts": {"recordings": len(rows), "source_inventory_rows": source_rows,
            "source_inventory_aliases": source_aliases,
            "aliases": sum(len(row["aliases"]) for row in rows),
            "unique_media_bytes": sum(row["media"]["byte_count"] for row in rows),
            "known_duration_ms": sum(row["duration_ms"] or 0 for row in rows),
            "metadata_receipts_verified": len(cache) - len(refs),
            "identity_conflicts": len(conflicts),
            **{name: states[name] for name in ("ready", "review", "no_audio", "missing_media")}},
        "semantics": {"media_bytes_opened": False, "media_hashes_reverified": False,
            "duration_is_cached_probe_hint": True, "acquisition_receipts_reverified": True,
            "controller_history_replayed": False, "all_source_aliases_preserved": True,
            "youtube_filename_suffix_is_identity_proof": False, "paid_api_authority": False,
            "publication_authority": False, "catalogue_mutation_authority": False}}


def write_inventory(output, value):
    """Publish only into an existing private directory, never overwrite a file."""
    output = _path(output)
    body = canonical(value)
    if len(body) > MAX_JSON:
        raise ArchiveInventoryError("output exceeds metadata bound")
    with _parent(output) as directory:
        info = os.fstat(directory)
        if info.st_uid != os.geteuid() or info.st_mode & 0o077:
            raise ArchiveInventoryError("output parent must be owned and private (0700)")
        temporary = ".cloud-archive-" + uuid.uuid4().hex
        fd = os.open(temporary, os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW,
                     0o600, dir_fd=directory)
        try:
            with os.fdopen(fd, "wb") as stream:
                stream.write(body)
                stream.flush()
                os.fchmod(stream.fileno(), 0o400)
                os.fsync(stream.fileno())
            os.link(temporary, output.name, src_dir_fd=directory, dst_dir_fd=directory,
                    follow_symlinks=False)
        finally:
            os.unlink(temporary, dir_fd=directory)
            os.fsync(directory)
    return {"path": str(output), "sha256": hashlib.sha256(body).hexdigest()}


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--inventory", required=True)
    parser.add_argument("--expected-sha256", required=True)
    parser.add_argument("--output", required=True, help="Fresh file in an existing private directory")
    args = parser.parse_args(argv)
    try:
        value = build_inventory(args.inventory, args.expected_sha256)
        print(json.dumps({"inventory": write_inventory(args.output, value), "counts": value["counts"]}, sort_keys=True))
    except (ArchiveInventoryError, OSError, ValueError) as error:
        print("ArchiveInventoryError: " + str(error), file=sys.stderr)
        return 2
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
