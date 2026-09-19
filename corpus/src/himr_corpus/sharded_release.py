"""Deterministic, integrity-checked static corpus release shards.

Version 2 keeps the active manifest small.  It commits to bounded catalog
summary shards, which in turn commit to one full recording/transcript shard per
recording.  The private database and publication views remain the only input;
sharding never weakens the v1 fail-closed record validator.
"""

from __future__ import annotations

import hashlib
import json
import os
import re
import shutil
import sqlite3
import stat
import tempfile
from pathlib import Path, PurePosixPath
from typing import Iterable

from .exporter import build_release, validate_release_shape
from .importers import canonical_json


SHARDED_SCHEMA_VERSION = 2
DEFAULT_CATALOG_SHARD_SIZE = 250
MAX_CATALOG_SHARD_SIZE = 1_000

_RELEASE_ID_PATTERN = re.compile(r"^release_[a-f0-9]{24}$")
_SHA256_PATTERN = re.compile(r"^[a-f0-9]{64}$")
_RECORDING_ID_PATTERN = re.compile(r"^rec_[a-f0-9]{32}$")
_SHARD_PATH_PATTERN = re.compile(
    r"^(?:catalog/catalog-[0-9]{5}-[a-f0-9]{16}|"
    r"recordings/rec_[a-f0-9]{32}-[a-f0-9]{16})\.json$"
)

_MANIFEST_KEYS = {
    "schema_version",
    "release_id",
    "generated_at",
    "counts",
    "catalog_shard_size",
    "stats",
    "facets",
    "catalog_shards",
}
_COUNT_KEYS = {"recordings", "sources", "transcript_revisions", "segments"}
_STATS_KEYS = {
    "duration_ms",
    "searchable_transcript_segments",
    "source_listings",
    "transcript_revisions",
}
_FACET_KEYS = {
    "platforms",
    "years",
    "languages",
    "speakers",
    "review_states",
    "confidence_bands",
    "recording_types",
}
_SHARD_REF_KEYS = {"path", "sha256", "bytes"}
_CATALOG_DESCRIPTOR_KEYS = _SHARD_REF_KEYS | {
    "recording_count",
    "source_count",
    "transcript_revision_count",
    "segment_count",
    "first_recording_id",
    "last_recording_id",
}
_SUMMARY_KEYS = {
    "recording_id",
    "slug",
    "title",
    "date_label",
    "date_year",
    "date_basis",
    "duration_ms",
    "recording_type",
    "review_state",
    "source_count",
    "transcript_revision_count",
    "segment_count",
    "searchable_segment_count",
    "platforms",
    "languages",
    "detail",
}

def _encoded(value: object) -> bytes:
    """Return the byte-level shard representation used by every hash."""

    return (canonical_json(value) + "\n").encode("utf-8")


def _sha256(value: bytes) -> str:
    return hashlib.sha256(value).hexdigest()


def _manifest_identity_payload(manifest: dict) -> dict:
    return {key: manifest[key] for key in sorted(_MANIFEST_KEYS - {"release_id"})}


def _manifest_release_id(manifest: dict) -> str:
    digest = _sha256(canonical_json(_manifest_identity_payload(manifest)).encode("utf-8"))
    return f"release_{digest[:24]}"


def _sorted_strings(values: Iterable[object], *, reverse: bool = False) -> list[str]:
    return sorted({str(value) for value in values if str(value)}, reverse=reverse)


def _recording_sort_key(recording: dict) -> tuple[int, str, str]:
    year = recording["date_year"]
    return (-(year if isinstance(year, int) else -1), recording["title"].casefold(), recording["recording_id"])


def _summary(recording: dict, detail: dict) -> dict:
    searchable = [
        revision
        for revision in recording["transcript_revisions"]
        if revision["lifecycle_state"] != "retracted"
    ]
    return {
        "recording_id": recording["recording_id"],
        "slug": recording["slug"],
        "title": recording["title"],
        "date_label": recording["date_label"],
        "date_year": recording["date_year"],
        "date_basis": recording["date_basis"],
        "duration_ms": recording["duration_ms"],
        "recording_type": recording["recording_type"],
        "review_state": recording["review_state"],
        "source_count": len(recording["sources"]),
        "transcript_revision_count": len(recording["transcript_revisions"]),
        "segment_count": sum(
            len(revision["segments"]) for revision in recording["transcript_revisions"]
        ),
        "searchable_segment_count": sum(
            1
            for revision in searchable
            for segment in revision["segments"]
            if segment["text"].strip()
        ),
        "platforms": _sorted_strings(source["platform"] for source in recording["sources"]),
        "languages": _sorted_strings(revision["language"] for revision in searchable),
        "detail": detail,
    }


def _empty_derived_metadata() -> tuple[dict[str, int], dict[str, set[str]]]:
    return (
        {
            "duration_ms": 0,
            "searchable_transcript_segments": 0,
            "source_listings": 0,
            "transcript_revisions": 0,
        },
        {key: set() for key in _FACET_KEYS},
    )


def _accumulate_derived_metadata(
    stats: dict[str, int], facet_values: dict[str, set[str]], recording: dict
) -> None:
    stats["duration_ms"] += recording["duration_ms"] or 0
    stats["source_listings"] += len(recording["sources"])
    stats["transcript_revisions"] += len(recording["transcript_revisions"])
    if recording["date_year"] is not None:
        facet_values["years"].add(str(recording["date_year"]))
    facet_values["recording_types"].add(recording["recording_type"])
    facet_values["review_states"].add(recording["review_state"])
    facet_values["platforms"].update(
        source["platform"] for source in recording["sources"]
    )
    for revision in recording["transcript_revisions"]:
        if revision["lifecycle_state"] == "retracted":
            continue
        facet_values["languages"].add(revision["language"])
        facet_values["review_states"].add(revision["review_state"])
        for segment in revision["segments"]:
            if segment["text"].strip():
                stats["searchable_transcript_segments"] += 1
            facet_values["speakers"].add(segment["speaker_label"] or "Unknown speaker")
            facet_values["confidence_bands"].add(
                segment["confidence_band"] or "Uncalibrated"
            )


def _finalize_derived_metadata(
    stats: dict[str, int], facet_values: dict[str, set[str]]
) -> tuple[dict, dict]:
    facets = {
        key: sorted(values, reverse=(key == "years"))
        for key, values in facet_values.items()
    }
    return stats, facets


def _derived_release_metadata(recordings: Iterable[dict]) -> tuple[dict, dict]:
    stats, facet_values = _empty_derived_metadata()
    for recording in recordings:
        _accumulate_derived_metadata(stats, facet_values, recording)
    return _finalize_derived_metadata(stats, facet_values)


def _write_file(path: Path, value: object) -> dict:
    payload = _encoded(value)
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("xb") as handle:
        handle.write(payload)
        handle.flush()
        os.fsync(handle.fileno())
    return {"sha256": _sha256(payload), "bytes": len(payload)}


def _sync_directory(path: Path) -> None:
    if os.name != "posix":
        return
    descriptor = os.open(path, os.O_RDONLY)
    try:
        os.fsync(descriptor)
    finally:
        os.close(descriptor)


def _same_tree(left: Path, right: Path) -> bool:
    left_files = sorted(path.relative_to(left) for path in left.rglob("*") if path.is_file())
    right_files = sorted(path.relative_to(right) for path in right.rglob("*") if path.is_file())
    if left_files != right_files:
        return False
    return all(
        left.joinpath(relative).read_bytes() == right.joinpath(relative).read_bytes()
        for relative in left_files
    )


def export_release_v2_from_release(
    release: dict,
    output_directory: str | Path,
    *,
    catalog_shard_size: int = DEFAULT_CATALOG_SHARD_SIZE,
) -> dict:
    """Atomically publish a v2 tree converted from a validated v1 projection.

    The content-addressed release directory is installed before the small active
    manifest is replaced.  A crash therefore leaves either the previous complete
    manifest or a complete, currently unreferenced immutable release directory.
    """

    validate_release_shape(release)
    if type(catalog_shard_size) is not int or not (1 <= catalog_shard_size <= MAX_CATALOG_SHARD_SIZE):
        raise ValueError(f"catalog_shard_size must be between 1 and {MAX_CATALOG_SHARD_SIZE}")

    output_root = Path(output_directory).resolve()
    if output_root == Path(output_root.anchor):
        raise ValueError("Refusing to export a sharded release at a filesystem root")
    output_root.mkdir(parents=True, exist_ok=True)
    releases_root = output_root / "releases"
    releases_root.mkdir(parents=True, exist_ok=True)

    recordings = sorted(release["recordings"], key=_recording_sort_key)
    work = Path(tempfile.mkdtemp(prefix=".release-v2-", dir=output_root))
    candidate_manifest: Path | None = None
    try:
        summaries: list[dict] = []
        for recording in recordings:
            envelope = {
                "schema_version": SHARDED_SCHEMA_VERSION,
                "kind": "recording",
                "recording": recording,
            }
            provisional = _encoded(envelope)
            digest = _sha256(provisional)
            relative_path = (
                f"recordings/{recording['recording_id']}-{digest[:16]}.json"
            )
            integrity = _write_file(work / relative_path, envelope)
            summaries.append(
                _summary(recording, {"path": relative_path, **integrity})
            )

        catalog_descriptors: list[dict] = []
        for ordinal, start in enumerate(range(0, len(summaries), catalog_shard_size)):
            batch = summaries[start : start + catalog_shard_size]
            envelope = {
                "schema_version": SHARDED_SCHEMA_VERSION,
                "kind": "catalog",
                "ordinal": ordinal,
                "recordings": batch,
            }
            provisional = _encoded(envelope)
            digest = _sha256(provisional)
            relative_path = f"catalog/catalog-{ordinal:05d}-{digest[:16]}.json"
            integrity = _write_file(work / relative_path, envelope)
            catalog_descriptors.append(
                {
                    "path": relative_path,
                    **integrity,
                    "recording_count": len(batch),
                    "source_count": sum(item["source_count"] for item in batch),
                    "transcript_revision_count": sum(
                        item["transcript_revision_count"] for item in batch
                    ),
                    "segment_count": sum(item["segment_count"] for item in batch),
                    "first_recording_id": batch[0]["recording_id"],
                    "last_recording_id": batch[-1]["recording_id"],
                }
            )

        stats, facets = _derived_release_metadata(recordings)
        manifest = {
            "schema_version": SHARDED_SCHEMA_VERSION,
            "release_id": "",
            "generated_at": release["generated_at"],
            "counts": release["counts"],
            "catalog_shard_size": catalog_shard_size,
            "stats": stats,
            "facets": facets,
            "catalog_shards": catalog_descriptors,
        }
        manifest["release_id"] = _manifest_release_id(manifest)
        target = releases_root / manifest["release_id"]
        for child in (work / "recordings", work / "catalog"):
            if child.exists():
                _sync_directory(child)
        _sync_directory(work)
        if target.exists():
            if not target.is_dir() or target.is_symlink() or not _same_tree(work, target):
                raise RuntimeError(
                    f"Immutable release directory already differs: {target}"
                )
            shutil.rmtree(work)
        else:
            os.replace(work, target)
            _sync_directory(releases_root)

        # Validate from disk before switching the active pointer.
        candidate_manifest = output_root / f".manifest-{os.getpid()}.json"
        if candidate_manifest.exists():
            raise RuntimeError(f"Temporary manifest unexpectedly exists: {candidate_manifest}")
        manifest_bytes = _encoded(manifest)
        with candidate_manifest.open("xb") as handle:
            handle.write(manifest_bytes)
            handle.flush()
            os.fsync(handle.fileno())
        validate_sharded_release(candidate_manifest, release_root=target)
        os.replace(candidate_manifest, output_root / "manifest.json")
        _sync_directory(output_root)
        return {
            "path": str(output_root / "manifest.json"),
            "release_directory": str(target),
            "sha256": _sha256(manifest_bytes),
            "bytes": len(manifest_bytes),
            "catalog_shards": len(catalog_descriptors),
            **release["counts"],
            "release_id": manifest["release_id"],
            "generated_at": release["generated_at"],
        }
    finally:
        if work.exists():
            shutil.rmtree(work)
        if candidate_manifest is not None and candidate_manifest.exists():
            candidate_manifest.unlink()
        # An installed immutable release tree is deliberately retained if active
        # manifest replacement fails; it is complete and safe but unreferenced.


def export_release_v2(
    connection: sqlite3.Connection,
    output_directory: str | Path,
    *,
    catalog_shard_size: int = DEFAULT_CATALOG_SHARD_SIZE,
) -> dict:
    return export_release_v2_from_release(
        build_release(connection),
        output_directory,
        catalog_shard_size=catalog_shard_size,
    )


def _pairs_without_duplicates(pairs: list[tuple[str, object]]) -> dict:
    result: dict = {}
    for key, value in pairs:
        if key in result:
            raise ValueError(f"Duplicate JSON object key: {key}")
        result[key] = value
    return result


def _load_json_bytes(payload: bytes, path: Path) -> object:
    try:
        return json.loads(payload.decode("utf-8"), object_pairs_hook=_pairs_without_duplicates)
    except (UnicodeDecodeError, json.JSONDecodeError) as error:
        raise ValueError(f"Invalid UTF-8 JSON shard: {path}") from error


def _require_exact_keys(value: object, keys: set[str], label: str) -> dict:
    if not isinstance(value, dict) or set(value) != keys:
        raise ValueError(f"Invalid {label} fields")
    return value


def _require_nonnegative_integer(value: object, label: str) -> int:
    if type(value) is not int or value < 0:
        raise ValueError(f"{label} must be a non-negative integer")
    return value


def _validate_ref(value: object, label: str, *, expected_prefix: str) -> dict:
    reference = _require_exact_keys(value, _SHARD_REF_KEYS, label)
    path_value = reference["path"]
    if (
        not isinstance(path_value, str)
        or not _SHARD_PATH_PATTERN.fullmatch(path_value)
        or not path_value.startswith(expected_prefix)
        or PurePosixPath(path_value).is_absolute()
        or any(part in {"", ".", ".."} for part in PurePosixPath(path_value).parts)
    ):
        raise ValueError(f"Unsafe {label} path")
    if not isinstance(reference["sha256"], str) or not _SHA256_PATTERN.fullmatch(
        reference["sha256"]
    ):
        raise ValueError(f"Invalid {label} sha256")
    _require_nonnegative_integer(reference["bytes"], f"{label}.bytes")
    if reference["bytes"] == 0:
        raise ValueError(f"{label}.bytes must be positive")
    return reference


def _read_verified(root: Path, reference: dict, label: str) -> tuple[bytes, Path]:
    root = root.resolve()
    candidate = root.joinpath(*PurePosixPath(reference["path"]).parts)
    resolved_parent = candidate.parent.resolve()
    if resolved_parent != root and root not in resolved_parent.parents:
        raise ValueError(f"{label} escapes its release directory")
    before = candidate.lstat()
    if stat.S_ISLNK(before.st_mode) or not stat.S_ISREG(before.st_mode):
        raise ValueError(f"{label} is not a regular non-symlink file")
    payload = candidate.read_bytes()
    after = candidate.lstat()
    if (before.st_dev, before.st_ino, before.st_size, before.st_mtime_ns) != (
        after.st_dev,
        after.st_ino,
        after.st_size,
        after.st_mtime_ns,
    ):
        raise ValueError(f"{label} changed while it was being validated")
    if len(payload) != reference["bytes"] or _sha256(payload) != reference["sha256"]:
        raise ValueError(f"{label} failed its byte count or SHA-256 check")
    return payload, candidate


def _single_record_release(recording: dict, generated_at: str) -> dict:
    counts = {
        "recordings": 1,
        "sources": len(recording["sources"]),
        "transcript_revisions": len(recording["transcript_revisions"]),
        "segments": sum(
            len(revision["segments"]) for revision in recording["transcript_revisions"]
        ),
    }
    payload = {
        "schema_version": 1,
        "generated_at": generated_at,
        "counts": counts,
        "recordings": [recording],
    }
    digest = _sha256(canonical_json(payload).encode("utf-8"))
    return {
        "schema_version": 1,
        "release_id": f"release_{digest[:24]}",
        "generated_at": generated_at,
        "counts": counts,
        "recordings": [recording],
    }


def _summary_matches(recording: dict, summary: dict) -> bool:
    return _summary(recording, summary["detail"]) == summary


def validate_sharded_release(
    manifest_path: str | Path,
    *,
    release_root: str | Path | None = None,
) -> dict:
    """Validate the complete v2 hash tree without constructing a monolith."""

    path = Path(manifest_path)
    manifest_bytes = path.read_bytes()
    manifest = _load_json_bytes(manifest_bytes, path)
    manifest = _require_exact_keys(manifest, _MANIFEST_KEYS, "sharded release manifest")
    if manifest["schema_version"] != SHARDED_SCHEMA_VERSION:
        raise ValueError("Unsupported sharded release schema version")
    if not isinstance(manifest["release_id"], str) or not _RELEASE_ID_PATTERN.fullmatch(
        manifest["release_id"]
    ):
        raise ValueError("Invalid sharded release_id")
    expected_release_id = _manifest_release_id(manifest)
    if manifest["release_id"] != expected_release_id:
        raise ValueError(
            f"Sharded release identity mismatch: expected {expected_release_id}, "
            f"received {manifest['release_id']}"
        )
    counts = _require_exact_keys(manifest["counts"], _COUNT_KEYS, "manifest counts")
    for key, value in counts.items():
        _require_nonnegative_integer(value, f"counts.{key}")
    shard_size = _require_nonnegative_integer(
        manifest["catalog_shard_size"], "catalog_shard_size"
    )
    if not (1 <= shard_size <= MAX_CATALOG_SHARD_SIZE):
        raise ValueError("catalog_shard_size is outside the supported bound")
    _require_exact_keys(manifest["stats"], _STATS_KEYS, "manifest stats")
    for key, value in manifest["stats"].items():
        _require_nonnegative_integer(value, f"stats.{key}")
    facets = _require_exact_keys(manifest["facets"], _FACET_KEYS, "manifest facets")
    for key, values in facets.items():
        if (
            not isinstance(values, list)
            or any(not isinstance(item, str) or not item for item in values)
            or len(values) != len(set(values))
            or values != sorted(values, reverse=(key == "years"))
        ):
            raise ValueError(f"Invalid deterministic facet list: {key}")
    if not isinstance(manifest["catalog_shards"], list):
        raise ValueError("catalog_shards must be an array")

    root = (
        Path(release_root).resolve()
        if release_root is not None
        else (path.parent / "releases" / manifest["release_id"]).resolve()
    )
    if root.exists():
        if root.is_symlink() or not root.is_dir():
            raise ValueError(f"Release directory is unsafe: {root}")
    elif manifest["catalog_shards"] or any(counts.values()):
        raise ValueError(f"Non-empty release directory is missing: {root}")

    derived_stats, derived_facets = _empty_derived_metadata()
    recording_count = 0
    total_sources = total_revisions = total_segments = 0
    seen_recording_ids: set[str] = set()
    seen_slugs: set[str] = set()
    seen_revision_ids: set[str] = set()
    seen_segment_ids: set[str] = set()
    prior_sort_key: tuple[int, str, str] | None = None
    expected_ordinal = 0
    expected_files: set[str] = set()

    for descriptor_value in manifest["catalog_shards"]:
        descriptor = _require_exact_keys(
            descriptor_value, _CATALOG_DESCRIPTOR_KEYS, "catalog descriptor"
        )
        _validate_ref(
            {key: descriptor[key] for key in _SHARD_REF_KEYS},
            "catalog descriptor",
            expected_prefix="catalog/",
        )
        for key in (
            "recording_count",
            "source_count",
            "transcript_revision_count",
            "segment_count",
        ):
            _require_nonnegative_integer(descriptor[key], f"catalog descriptor {key}")
        payload, shard_path = _read_verified(root, descriptor, "catalog shard")
        if descriptor["path"] in expected_files:
            raise ValueError("Duplicate shard path in manifest hash tree")
        expected_files.add(descriptor["path"])
        catalog = _require_exact_keys(
            _load_json_bytes(payload, shard_path),
            {"schema_version", "kind", "ordinal", "recordings"},
            "catalog shard",
        )
        if (
            catalog["schema_version"] != SHARDED_SCHEMA_VERSION
            or catalog["kind"] != "catalog"
            or catalog["ordinal"] != expected_ordinal
            or not isinstance(catalog["recordings"], list)
            or not catalog["recordings"]
            or len(catalog["recordings"]) > shard_size
            or descriptor["recording_count"] != len(catalog["recordings"])
        ):
            raise ValueError("Catalog shard envelope or bound is invalid")
        expected_ordinal += 1
        shard_sources = shard_revisions = shard_segments = 0

        for summary_value in catalog["recordings"]:
            summary = _require_exact_keys(summary_value, _SUMMARY_KEYS, "recording summary")
            detail_ref = _validate_ref(
                summary["detail"], "recording detail", expected_prefix="recordings/"
            )
            for key in (
                "source_count",
                "transcript_revision_count",
                "segment_count",
                "searchable_segment_count",
            ):
                _require_nonnegative_integer(summary[key], f"recording summary {key}")
            detail_payload, detail_path = _read_verified(root, detail_ref, "recording detail")
            if detail_ref["path"] in expected_files:
                raise ValueError("Duplicate recording detail path in manifest hash tree")
            expected_files.add(detail_ref["path"])
            detail = _require_exact_keys(
                _load_json_bytes(detail_payload, detail_path),
                {"schema_version", "kind", "recording"},
                "recording detail shard",
            )
            if detail["schema_version"] != SHARDED_SCHEMA_VERSION or detail["kind"] != "recording":
                raise ValueError("Recording detail shard envelope is invalid")
            recording = detail["recording"]
            validate_release_shape(_single_record_release(recording, manifest["generated_at"]))
            if not _summary_matches(recording, summary):
                raise ValueError("Recording summary does not match its detail shard")
            recording_id = recording["recording_id"]
            if recording_id in seen_recording_ids or recording["slug"] in seen_slugs:
                raise ValueError("Duplicate recording ID or slug across shards")
            if recording_id != summary["recording_id"]:
                raise ValueError("Recording detail ID differs from catalog summary")
            seen_recording_ids.add(recording_id)
            seen_slugs.add(recording["slug"])
            sort_key = _recording_sort_key(recording)
            if prior_sort_key is not None and sort_key < prior_sort_key:
                raise ValueError("Catalog recordings are not deterministically ordered")
            prior_sort_key = sort_key
            for revision in recording["transcript_revisions"]:
                if revision["revision_id"] in seen_revision_ids:
                    raise ValueError("Duplicate transcript revision ID across recording shards")
                seen_revision_ids.add(revision["revision_id"])
                for segment in revision["segments"]:
                    if segment["segment_id"] in seen_segment_ids:
                        raise ValueError("Duplicate transcript segment ID across recording shards")
                    seen_segment_ids.add(segment["segment_id"])
            _accumulate_derived_metadata(derived_stats, derived_facets, recording)
            recording_count += 1
            shard_sources += summary["source_count"]
            shard_revisions += summary["transcript_revision_count"]
            shard_segments += summary["segment_count"]

        first_id = catalog["recordings"][0]["recording_id"]
        last_id = catalog["recordings"][-1]["recording_id"]
        if (
            descriptor["source_count"] != shard_sources
            or descriptor["transcript_revision_count"] != shard_revisions
            or descriptor["segment_count"] != shard_segments
            or descriptor["first_recording_id"] != first_id
            or descriptor["last_recording_id"] != last_id
        ):
            raise ValueError("Catalog descriptor totals or boundary IDs do not match its shard")
        total_sources += shard_sources
        total_revisions += shard_revisions
        total_segments += shard_segments

    observed_counts = {
        "recordings": recording_count,
        "sources": total_sources,
        "transcript_revisions": total_revisions,
        "segments": total_segments,
    }
    if observed_counts != counts:
        raise ValueError("Manifest counts do not match the complete shard tree")
    expected_stats, expected_facets = _finalize_derived_metadata(
        derived_stats, derived_facets
    )
    if expected_stats != manifest["stats"] or expected_facets != facets:
        raise ValueError("Manifest derived statistics or facets do not match recording shards")
    if root.exists():
        actual_files: set[str] = set()
        allowed_directories = {"catalog", "recordings"} if expected_files else set()
        for candidate in root.rglob("*"):
            relative = candidate.relative_to(root).as_posix()
            if candidate.is_symlink():
                raise ValueError(f"Symlink exists in immutable release tree: {relative}")
            if candidate.is_dir():
                if relative not in allowed_directories:
                    raise ValueError(f"Unexpected directory in immutable release tree: {relative}")
            elif candidate.is_file():
                actual_files.add(relative)
            else:
                raise ValueError(f"Unexpected object in immutable release tree: {relative}")
        if actual_files != expected_files:
            raise ValueError("Immutable release tree contains missing or unreferenced files")
    return {
        "path": str(path),
        "release_directory": str(root),
        "release_id": manifest["release_id"],
        "schema_version": SHARDED_SCHEMA_VERSION,
        "catalog_shards": len(manifest["catalog_shards"]),
        **counts,
    }
