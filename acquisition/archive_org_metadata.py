#!/usr/bin/env python3
"""Immutable, unauthenticated Archive.org item-metadata snapshots and deltas.

This lane fetches only ``/metadata/<identifier>`` JSON.  It has no media-download,
cookie, credential, identity, or publication capability.  Provider fields are
discovery metadata, not proof of content, authorship, dates, completeness, or rights.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import re
import shutil
import stat
import sys
import urllib.error
import urllib.parse
import urllib.request
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable


SCHEMA_VERSION = 1
USER_AGENT = "hidinginmyroom-corpus-archive-metadata/1.0 (public metadata research)"
ACCEPT = "application/json"
IDENTIFIER_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._-]{0,99}$")
SHA256_RE = re.compile(r"^[0-9a-f]{64}$")
UTC_RE = re.compile(r"^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}Z$")
ALLOWED_HOSTS = frozenset({"archive.org", "www.archive.org"})
BASES = frozenset({"catalog_archive_item", "catalog_file_hint", "manual_public_lead"})
MAX_ITEMS = 100
MAX_FILES_PER_ITEM = 200_000
MAX_COLLECTIONS_PER_ITEM = 10_000
DEFAULT_MAX_ITEM_BYTES = 128 * 1024 * 1024
MAX_DELTA_BYTES = 128 * 1024 * 1024


class ArchiveMetadataError(RuntimeError):
    pass


def canonical_bytes(value: Any) -> bytes:
    return json.dumps(
        value, ensure_ascii=False, sort_keys=True, separators=(",", ":"), allow_nan=False
    ).encode("utf-8")


def pretty_bytes(value: Any) -> bytes:
    return (
        json.dumps(value, ensure_ascii=False, sort_keys=True, indent=2, allow_nan=False)
        + "\n"
    ).encode("utf-8")


def sha256_bytes(value: bytes) -> str:
    return hashlib.sha256(value).hexdigest()


def stable_id(prefix: str, value: Any) -> str:
    return f"{prefix}_{sha256_bytes(canonical_bytes(value))[:32]}"


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds").replace("+00:00", "Z")


def _exact(value: Any, keys: set[str], label: str) -> dict[str, Any]:
    if not isinstance(value, dict):
        raise ArchiveMetadataError(f"{label} must be an object")
    missing = sorted(keys - set(value))
    unknown = sorted(set(value) - keys)
    if missing or unknown:
        raise ArchiveMetadataError(
            f"{label} keys differ from the contract; missing={missing}, unknown={unknown}"
        )
    return value


def _utc(value: Any, label: str) -> str:
    if not isinstance(value, str) or not UTC_RE.fullmatch(value):
        raise ArchiveMetadataError(f"{label} must be a whole-second UTC timestamp")
    try:
        datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError as error:
        raise ArchiveMetadataError(f"{label} is invalid") from error
    return value


def _identifier(value: Any, label: str) -> str:
    if not isinstance(value, str) or not IDENTIFIER_RE.fullmatch(value) or value in {".", ".."}:
        raise ArchiveMetadataError(f"{label} is not a safe Archive.org identifier")
    return value


def _sha256(value: Any, label: str) -> str:
    if not isinstance(value, str) or not SHA256_RE.fullmatch(value):
        raise ArchiveMetadataError(f"{label} must be a lowercase SHA-256")
    return value


def _positive_integer(value: Any, label: str, maximum: int | None = None) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value < 1:
        raise ArchiveMetadataError(f"{label} must be a positive integer")
    if maximum is not None and value > maximum:
        raise ArchiveMetadataError(f"{label} exceeds {maximum}")
    return value


def _read(path: Path, maximum: int, label: str, *, sealed: bool = False) -> bytes:
    try:
        before = path.lstat()
        resolved = path.resolve(strict=True)
    except (FileNotFoundError, OSError) as error:
        raise ArchiveMetadataError(f"cannot resolve {label}: {error}") from error
    if path != resolved or stat.S_ISLNK(before.st_mode) or not stat.S_ISREG(before.st_mode):
        raise ArchiveMetadataError(f"{label} must be a resolved regular file")
    if sealed and before.st_mode & 0o222:
        raise ArchiveMetadataError(f"{label} must be sealed read-only")
    if before.st_size > maximum:
        raise ArchiveMetadataError(f"{label} exceeds {maximum} bytes")
    try:
        with path.open("rb") as handle:
            opened = os.fstat(handle.fileno())
            identity = (opened.st_dev, opened.st_ino, opened.st_size, opened.st_mtime_ns)
            if identity != (before.st_dev, before.st_ino, before.st_size, before.st_mtime_ns):
                raise ArchiveMetadataError(f"{label} changed while opening")
            body = handle.read()
            after_fd = os.fstat(handle.fileno())
        after = path.stat()
    except OSError as error:
        raise ArchiveMetadataError(f"cannot read {label}: {error}") from error
    if identity != (after_fd.st_dev, after_fd.st_ino, after_fd.st_size, after_fd.st_mtime_ns) or identity != (after.st_dev, after.st_ino, after.st_size, after.st_mtime_ns):
        raise ArchiveMetadataError(f"{label} changed while reading")
    return body


def _strict_json_loads(body: bytes, label: str) -> Any:
    try:
        text = body.decode("utf-8")
    except UnicodeDecodeError as error:
        raise ArchiveMetadataError(f"{label} is not UTF-8 JSON") from error

    def no_duplicate_keys(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
        value: dict[str, Any] = {}
        for key, child in pairs:
            if key in value:
                raise ArchiveMetadataError(f"{label} contains duplicate JSON key {key!r}")
            value[key] = child
        return value

    def no_nonfinite_constant(value: str) -> None:
        raise ArchiveMetadataError(f"{label} contains non-finite JSON constant {value}")

    try:
        return json.loads(
            text,
            object_pairs_hook=no_duplicate_keys,
            parse_constant=no_nonfinite_constant,
        )
    except json.JSONDecodeError as error:
        raise ArchiveMetadataError(f"{label} is not UTF-8 JSON") from error


def _json(path: Path, maximum: int, label: str, *, sealed: bool = False) -> tuple[Any, bytes]:
    body = _read(path, maximum, label, sealed=sealed)
    return _strict_json_loads(body, label), body


def _safe_output_root(path: Path) -> Path:
    if not path.is_absolute() or path == Path("/"):
        raise ArchiveMetadataError("output root must be a specific absolute path")
    normalized = Path(os.path.normpath(str(path)))
    if normalized != path:
        raise ArchiveMetadataError("output root must not contain traversal")
    for unsafe in (Path("/tmp"), Path("/var/tmp")):
        try:
            path.relative_to(unsafe)
        except ValueError:
            pass
        else:
            raise ArchiveMetadataError(f"output root must not be under {unsafe}")
    path.mkdir(parents=True, exist_ok=True)
    if path.resolve(strict=True) != path or not path.is_dir():
        raise ArchiveMetadataError("output root must be a resolved directory")
    return path


def _request_url(identifier: str) -> str:
    return f"https://archive.org/metadata/{urllib.parse.quote(identifier, safe='')}"


def _validate_archive_url(value: Any, identifier: str, label: str) -> str:
    if not isinstance(value, str) or len(value) > 4096:
        raise ArchiveMetadataError(f"{label} must be an HTTPS Archive.org URL")
    parsed = urllib.parse.urlsplit(value)
    parts = [urllib.parse.unquote(part) for part in parsed.path.split("/") if part]
    if (
        parsed.scheme != "https"
        or (parsed.hostname or "").lower() not in ALLOWED_HOSTS
        or parsed.username
        or parsed.password
        or parsed.query
        or parsed.fragment
        or parts != ["metadata", identifier]
    ):
        raise ArchiveMetadataError(f"{label} leaves the exact public metadata endpoint")
    return value


def validate_request_manifest(path: Path) -> dict[str, Any]:
    raw, body = _json(Path(path).resolve(), 1024 * 1024, "Archive.org request manifest")
    value = _exact(
        raw,
        {"schema_version", "request_id", "request_kind", "requested_at", "items", "policy"},
        "request",
    )
    if value["schema_version"] != 1 or value["request_kind"] != "archive_org_metadata_targets":
        raise ArchiveMetadataError("unsupported Archive.org request manifest")
    _utc(value["requested_at"], "request.requested_at")
    items = value["items"]
    if not isinstance(items, list) or not 1 <= len(items) <= MAX_ITEMS:
        raise ArchiveMetadataError("request.items must contain 1..100 targets")
    identifiers: list[str] = []
    for index, raw_item in enumerate(items):
        item = _exact(raw_item, {"identifier", "basis"}, f"request.items[{index}]")
        identifier = _identifier(item["identifier"], f"request.items[{index}].identifier")
        if item["basis"] not in BASES:
            raise ArchiveMetadataError(f"request.items[{index}].basis is unsupported")
        identifiers.append(identifier)
    if identifiers != sorted(set(identifiers)):
        raise ArchiveMetadataError("request.items must be unique and sorted by identifier")
    expected_policy = {
        "public_unauthenticated_metadata_only": True,
        "media_download": False,
        "cookies_sent": False,
        "authorization_sent": False,
        "publication_authority": False,
    }
    if _exact(value["policy"], set(expected_policy), "request.policy") != expected_policy:
        raise ArchiveMetadataError("request policy differs from the no-auth metadata-only contract")
    identity = {key: value[key] for key in value if key != "request_id"}
    if value["request_id"] != stable_id("iamr", identity):
        raise ArchiveMetadataError("request_id does not match the canonical request")
    return {**value, "_body": body, "_sha256": sha256_bytes(body), "_byte_count": len(body)}


def _validate_document(payload: bytes, identifier: str) -> dict[str, Any]:
    document = _strict_json_loads(payload, f"Archive.org metadata for {identifier}")
    if not isinstance(document, dict):
        raise ArchiveMetadataError(f"Archive.org metadata for {identifier} must be an object")
    metadata = document.get("metadata")
    files = document.get("files")
    if not isinstance(metadata, dict) or str(metadata.get("identifier") or "") != identifier:
        raise ArchiveMetadataError(f"Archive.org metadata identifier differs for {identifier}")
    if not isinstance(files, list) or len(files) > MAX_FILES_PER_ITEM:
        raise ArchiveMetadataError(f"Archive.org files for {identifier} are missing or exceed the cap")
    names: list[str] = []
    for index, file_record in enumerate(files):
        if not isinstance(file_record, dict):
            raise ArchiveMetadataError(f"Archive.org {identifier} files[{index}] is not an object")
        name = file_record.get("name")
        if not isinstance(name, str) or not name or len(name) > 4096 or "\x00" in name:
            raise ArchiveMetadataError(f"Archive.org {identifier} files[{index}].name is invalid")
        names.append(name)
    if len(names) != len(set(names)):
        raise ArchiveMetadataError(f"Archive.org {identifier} contains duplicate file names")
    title = metadata.get("title")
    if not isinstance(title, str):
        title = identifier
    collection = metadata.get("collection")
    if isinstance(collection, str):
        collections = [collection]
    elif isinstance(collection, list):
        collections = sorted({str(item) for item in collection if isinstance(item, str) and item})
    else:
        collections = []
    if len(collections) > MAX_COLLECTIONS_PER_ITEM:
        raise ArchiveMetadataError(
            f"Archive.org {identifier} collections exceed the cap"
        )
    return {
        "document": document,
        "title": title[:1000],
        "collections": collections,
        "file_count": len(files),
    }


class _SafeRedirect(urllib.request.HTTPRedirectHandler):
    def __init__(self, identifier: str):
        self.identifier = identifier

    def redirect_request(self, req, fp, code, msg, headers, newurl):  # noqa: N802
        _validate_archive_url(newurl, self.identifier, "redirect URL")
        return super().redirect_request(req, fp, code, msg, headers, newurl)


def _fetch(
    identifier: str,
    *,
    timeout_seconds: int,
    max_item_bytes: int,
    opener: Any | None,
    clock: Callable[[], str],
) -> tuple[bytes, dict[str, Any]]:
    url = _request_url(identifier)
    request = urllib.request.Request(
        url,
        method="GET",
        headers={"Accept": ACCEPT, "User-Agent": USER_AGENT},
    )
    client = opener or urllib.request.build_opener(_SafeRedirect(identifier))
    started_at = clock()
    _utc(started_at, "capture started_at")
    try:
        response = client.open(request, timeout=timeout_seconds)
        with response:
            status = int(getattr(response, "status", response.getcode()))
            final_url = _validate_archive_url(response.geturl(), identifier, "final URL")
            if status != 200:
                raise ArchiveMetadataError(f"Archive.org metadata request returned HTTP {status}")
            declared = response.headers.get("Content-Length")
            declared_size: int | None = None
            if declared:
                try:
                    declared_size = int(declared)
                    if declared_size < 0:
                        raise ArchiveMetadataError(
                            "Archive.org returned invalid Content-Length"
                        )
                    if declared_size > max_item_bytes:
                        raise ArchiveMetadataError("Archive.org metadata response exceeds max-item-bytes")
                except ValueError as error:
                    raise ArchiveMetadataError("Archive.org returned invalid Content-Length") from error
            chunks: list[bytes] = []
            total = 0
            while True:
                chunk = response.read(min(64 * 1024, max_item_bytes + 1 - total))
                if not chunk:
                    break
                chunks.append(chunk)
                total += len(chunk)
                if total > max_item_bytes:
                    raise ArchiveMetadataError("Archive.org metadata response exceeds max-item-bytes")
            payload = b"".join(chunks)
            if declared_size is not None and declared_size != len(payload):
                raise ArchiveMetadataError(
                    "Archive.org metadata response Content-Length differs from received bytes"
                )
            headers = {
                "content_type": str(response.headers.get("Content-Type") or "")[:200],
                "etag": response.headers.get("ETag"),
                "last_modified": response.headers.get("Last-Modified"),
                "response_date": response.headers.get("Date"),
            }
    except urllib.error.HTTPError as error:
        error.close()
        raise ArchiveMetadataError(f"Archive.org metadata request returned HTTP {error.code}") from error
    except urllib.error.URLError as error:
        raise ArchiveMetadataError(f"Archive.org metadata request failed: {error.reason}") from error
    observed_at = clock()
    _utc(observed_at, "capture observed_at")
    if datetime.fromisoformat(observed_at.replace("Z", "+00:00")) < datetime.fromisoformat(started_at.replace("Z", "+00:00")):
        raise ArchiveMetadataError("capture timestamps are nonchronological")
    normalized = _validate_document(payload, identifier)
    if "json" not in headers["content_type"].lower():
        raise ArchiveMetadataError("Archive.org metadata response Content-Type is not JSON")
    for key in ("etag", "last_modified", "response_date"):
        value = headers[key]
        if value is not None:
            headers[key] = str(value)[:1000]
    return payload, {
        "identifier": identifier,
        "request_url": url,
        "final_url": final_url,
        "http_status": 200,
        "started_at": started_at,
        "observed_at": observed_at,
        **headers,
        "payload_sha256": sha256_bytes(payload),
        "byte_count": len(payload),
        "payload_file": f"item-{identifier}.metadata.json",
        "item_title": normalized["title"],
        "collections": normalized["collections"],
        "file_count": normalized["file_count"],
    }


def _seal(path: Path) -> None:
    path.chmod(path.stat().st_mode & ~0o222)


def capture_snapshot(
    request_path: Path,
    *,
    output_root: Path,
    timeout_seconds: int = 60,
    max_item_bytes: int = DEFAULT_MAX_ITEM_BYTES,
    opener_factory: Callable[[str], Any] | None = None,
    clock: Callable[[], str] = utc_now,
) -> Path:
    request_path = Path(request_path).resolve()
    request = validate_request_manifest(request_path)
    output_root = _safe_output_root(Path(output_root))
    if not 1 <= timeout_seconds <= 300:
        raise ArchiveMetadataError("timeout-seconds must be between 1 and 300")
    if not 1024 <= max_item_bytes <= 512 * 1024 * 1024:
        raise ArchiveMetadataError("max-item-bytes is outside 1 KiB..512 MiB")
    stage = output_root / f".archive-metadata-{uuid.uuid4().hex}"
    stage.mkdir(mode=0o700)
    created_final: Path | None = None
    try:
        (stage / "request.json").write_bytes(request["_body"])
        items: list[dict[str, Any]] = []
        for target in request["items"]:
            identifier = target["identifier"]
            opener = opener_factory(identifier) if opener_factory else None
            payload, response = _fetch(
                identifier,
                timeout_seconds=timeout_seconds,
                max_item_bytes=max_item_bytes,
                opener=opener,
                clock=clock,
            )
            if datetime.fromisoformat(response["started_at"].replace("Z", "+00:00")) < datetime.fromisoformat(request["requested_at"].replace("Z", "+00:00")):
                raise ArchiveMetadataError(
                    "Archive.org capture started before the sealed request timestamp"
                )
            (stage / response["payload_file"]).write_bytes(payload)
            items.append({"basis": target["basis"], **response})
        observed_at = max(item["observed_at"] for item in items)
        identity = {
            "request_id": request["request_id"],
            "observed_at": observed_at,
            "items": [
                {
                    "identifier": item["identifier"],
                    "payload_sha256": item["payload_sha256"],
                    "final_url": item["final_url"],
                }
                for item in items
            ],
        }
        snapshot_id = stable_id("iams", identity)
        result = {
            "schema_version": 1,
            "snapshot_id": snapshot_id,
            "snapshot_kind": "archive_org_item_metadata",
            "observed_at": observed_at,
            "request": {
                "request_id": request["request_id"],
                "request_sha256": request["_sha256"],
                "request_byte_count": request["_byte_count"],
                "request_file": "request.json",
            },
            "http_policy": {
                "method": "GET",
                "accept": ACCEPT,
                "user_agent": USER_AGENT,
                "cookies_sent": False,
                "authorization_sent": False,
            },
            "items": items,
            "assertion_policy": {
                "state": "unreviewed_provider_metadata",
                "provider_fields_are_content_truth": False,
                "media_downloaded": False,
                "identity_assertions": False,
                "publication_authority": False,
            },
            "errors": [],
        }
        (stage / "snapshot.json").write_bytes(pretty_bytes(result))
        for child in stage.iterdir():
            _seal(child)
        _seal(stage)
        final = output_root / snapshot_id
        try:
            os.rename(stage, final)
            created_final = final
        except FileExistsError:
            make_writable(stage)
            shutil.rmtree(stage)
        snapshot_path = final / "snapshot.json"
        validate_snapshot(snapshot_path)
        return snapshot_path
    except Exception:
        make_writable(stage)
        shutil.rmtree(stage, ignore_errors=True)
        if created_final is not None:
            make_writable(created_final)
            shutil.rmtree(created_final, ignore_errors=True)
        raise


def make_writable(root: Path) -> None:
    if not root.exists():
        return
    for path in [root, *root.rglob("*")]:
        try:
            path.chmod(path.stat().st_mode | 0o700)
        except OSError:
            pass


def validate_snapshot(path: Path) -> dict[str, Any]:
    path = Path(path).resolve()
    raw, body = _json(path, 64 * 1024 * 1024, "Archive.org snapshot", sealed=True)
    value = _exact(
        raw,
        {"schema_version", "snapshot_id", "snapshot_kind", "observed_at", "request", "http_policy", "items", "assertion_policy", "errors"},
        "snapshot",
    )
    if body != pretty_bytes(value) or value["schema_version"] != 1 or value["snapshot_kind"] != "archive_org_item_metadata" or value["errors"] != []:
        raise ArchiveMetadataError("snapshot is not a canonical completed v1 result")
    _utc(value["observed_at"], "snapshot.observed_at")
    request_row = _exact(value["request"], {"request_id", "request_sha256", "request_byte_count", "request_file"}, "snapshot.request")
    if request_row["request_file"] != "request.json":
        raise ArchiveMetadataError("snapshot request file is not canonical")
    request_path = path.parent / "request.json"
    request = validate_request_manifest(request_path)
    if (
        request["request_id"] != request_row["request_id"]
        or request["_sha256"] != _sha256(request_row["request_sha256"], "snapshot.request.request_sha256")
        or request["_byte_count"] != _positive_integer(request_row["request_byte_count"], "snapshot.request.request_byte_count")
    ):
        raise ArchiveMetadataError("snapshot request evidence differs")
    expected_http = {"method": "GET", "accept": ACCEPT, "user_agent": USER_AGENT, "cookies_sent": False, "authorization_sent": False}
    if _exact(value["http_policy"], set(expected_http), "snapshot.http_policy") != expected_http:
        raise ArchiveMetadataError("snapshot HTTP policy differs")
    expected_assertions = {"state": "unreviewed_provider_metadata", "provider_fields_are_content_truth": False, "media_downloaded": False, "identity_assertions": False, "publication_authority": False}
    if _exact(value["assertion_policy"], set(expected_assertions), "snapshot.assertion_policy") != expected_assertions:
        raise ArchiveMetadataError("snapshot assertion policy differs")
    items = value["items"]
    if not isinstance(items, list) or len(items) != len(request["items"]):
        raise ArchiveMetadataError("snapshot item count differs from request")
    normalized_items: list[dict[str, Any]] = []
    for index, (item, target) in enumerate(zip(items, request["items"], strict=True)):
        label = f"snapshot.items[{index}]"
        row = _exact(item, {"identifier", "basis", "request_url", "final_url", "http_status", "started_at", "observed_at", "content_type", "etag", "last_modified", "response_date", "payload_sha256", "byte_count", "payload_file", "item_title", "collections", "file_count"}, label)
        identifier = _identifier(row["identifier"], f"{label}.identifier")
        if identifier != target["identifier"] or row["basis"] != target["basis"]:
            raise ArchiveMetadataError("snapshot item order/target differs from request")
        if row["request_url"] != _request_url(identifier) or row["http_status"] != 200:
            raise ArchiveMetadataError(f"{label} request/status is invalid")
        _validate_archive_url(row["final_url"], identifier, f"{label}.final_url")
        started = _utc(row["started_at"], f"{label}.started_at")
        observed = _utc(row["observed_at"], f"{label}.observed_at")
        if (
            datetime.fromisoformat(started.replace("Z", "+00:00"))
            < datetime.fromisoformat(request["requested_at"].replace("Z", "+00:00"))
            or datetime.fromisoformat(observed.replace("Z", "+00:00"))
            < datetime.fromisoformat(started.replace("Z", "+00:00"))
        ):
            raise ArchiveMetadataError(f"{label} timestamps are nonchronological")
        if not isinstance(row["content_type"], str) or "json" not in row["content_type"].lower():
            raise ArchiveMetadataError(f"{label}.content_type is invalid")
        for key in ("etag", "last_modified", "response_date"):
            if row[key] is not None and (not isinstance(row[key], str) or len(row[key]) > 1000):
                raise ArchiveMetadataError(f"{label}.{key} is invalid")
        digest = _sha256(row["payload_sha256"], f"{label}.payload_sha256")
        size = _positive_integer(row["byte_count"], f"{label}.byte_count", 512 * 1024 * 1024)
        expected_name = f"item-{identifier}.metadata.json"
        if row["payload_file"] != expected_name:
            raise ArchiveMetadataError(f"{label}.payload_file is invalid")
        payload = _read(path.parent / expected_name, size, f"{label} payload", sealed=True)
        if len(payload) != size or sha256_bytes(payload) != digest:
            raise ArchiveMetadataError(f"{label} payload bytes differ")
        parsed = _validate_document(payload, identifier)
        if row["item_title"] != parsed["title"] or row["collections"] != parsed["collections"] or row["file_count"] != parsed["file_count"]:
            raise ArchiveMetadataError(f"{label} summary does not reproduce from payload")
        normalized_items.append(dict(row))
    if value["observed_at"] != max(item["observed_at"] for item in normalized_items):
        raise ArchiveMetadataError("snapshot observation time differs from its responses")
    identity = {"request_id": request["request_id"], "observed_at": value["observed_at"], "items": [{"identifier": item["identifier"], "payload_sha256": item["payload_sha256"], "final_url": item["final_url"]} for item in normalized_items]}
    if value["snapshot_id"] != stable_id("iams", identity) or path.parent.name != value["snapshot_id"]:
        raise ArchiveMetadataError("snapshot ID/path is inconsistent")
    if path.parent.stat().st_mode & 0o222:
        raise ArchiveMetadataError("snapshot directory must be sealed")
    return {**value, "_path": path, "_sha256": sha256_bytes(body), "_byte_count": len(body), "_request": request}


def _file_map(document: dict[str, Any]) -> dict[str, str]:
    return {
        row["name"]: sha256_bytes(canonical_bytes(row))
        for row in document.get("files", [])
        if isinstance(row, dict) and isinstance(row.get("name"), str)
    }


def build_delta(previous_path: Path, current_path: Path) -> dict[str, Any]:
    previous = validate_snapshot(previous_path)
    current = validate_snapshot(current_path)
    if previous["snapshot_id"] == current["snapshot_id"]:
        raise ArchiveMetadataError("delta requires two different snapshots")
    if datetime.fromisoformat(current["observed_at"].replace("Z", "+00:00")) < datetime.fromisoformat(previous["observed_at"].replace("Z", "+00:00")):
        raise ArchiveMetadataError("delta current snapshot predates previous snapshot")
    previous_items = {item["identifier"]: item for item in previous["items"]}
    current_items = {item["identifier"]: item for item in current["items"]}
    rows: list[dict[str, Any]] = []
    summary = {"added_items": 0, "removed_items": 0, "changed_items": 0, "unchanged_items": 0, "added_files": 0, "removed_files": 0, "changed_files": 0}
    for identifier in sorted(set(previous_items) | set(current_items)):
        old = previous_items.get(identifier)
        new = current_items.get(identifier)
        if old is None:
            state = "added"
            old_files: dict[str, str] = {}
            new_doc = _validate_document(_read(current["_path"].parent / new["payload_file"], new["byte_count"], "current delta payload", sealed=True), identifier)["document"]
            new_files = _file_map(new_doc)
        elif new is None:
            state = "removed"
            old_doc = _validate_document(_read(previous["_path"].parent / old["payload_file"], old["byte_count"], "previous delta payload", sealed=True), identifier)["document"]
            old_files = _file_map(old_doc)
            new_files = {}
        else:
            state = "unchanged" if old["payload_sha256"] == new["payload_sha256"] else "changed"
            old_doc = _validate_document(_read(previous["_path"].parent / old["payload_file"], old["byte_count"], "previous delta payload", sealed=True), identifier)["document"]
            new_doc = _validate_document(_read(current["_path"].parent / new["payload_file"], new["byte_count"], "current delta payload", sealed=True), identifier)["document"]
            old_files = _file_map(old_doc)
            new_files = _file_map(new_doc)
        added = sorted(set(new_files) - set(old_files))
        removed = sorted(set(old_files) - set(new_files))
        changed = sorted(name for name in set(old_files) & set(new_files) if old_files[name] != new_files[name])
        summary[f"{state}_items"] += 1
        summary["added_files"] += len(added)
        summary["removed_files"] += len(removed)
        summary["changed_files"] += len(changed)
        rows.append({"identifier": identifier, "state": state, "previous_payload_sha256": old["payload_sha256"] if old else None, "current_payload_sha256": new["payload_sha256"] if new else None, "added_files": added, "removed_files": removed, "changed_files": changed})
    body = {
        "schema_version": 1,
        "delta_kind": "archive_org_item_metadata_delta",
        "observed_at": current["observed_at"],
        "previous": {"snapshot_id": previous["snapshot_id"], "snapshot_sha256": previous["_sha256"]},
        "current": {"snapshot_id": current["snapshot_id"], "snapshot_sha256": current["_sha256"]},
        "summary": summary,
        "items": rows,
        "assertion_policy": {"state": "unreviewed_provider_metadata_delta", "content_change_asserted": False, "publication_authority": False},
    }
    return {"delta_id": stable_id("iamd", body), **body}


def validate_delta(
    path: Path,
    previous_path: Path,
    current_path: Path,
) -> dict[str, Any]:
    value, body = _json(Path(path).resolve(), MAX_DELTA_BYTES, "Archive.org delta", sealed=True)
    value = _exact(value, {"delta_id", "schema_version", "delta_kind", "observed_at", "previous", "current", "summary", "items", "assertion_policy"}, "delta")
    if body != pretty_bytes(value) or value["schema_version"] != 1 or value["delta_kind"] != "archive_org_item_metadata_delta":
        raise ArchiveMetadataError("delta is not canonical v1")
    _utc(value["observed_at"], "delta.observed_at")
    for side in ("previous", "current"):
        row = _exact(value[side], {"snapshot_id", "snapshot_sha256"}, f"delta.{side}")
        if not isinstance(row["snapshot_id"], str) or not row["snapshot_id"].startswith("iams_"):
            raise ArchiveMetadataError(f"delta.{side}.snapshot_id is invalid")
        _sha256(row["snapshot_sha256"], f"delta.{side}.snapshot_sha256")
    summary_keys = {"added_items", "removed_items", "changed_items", "unchanged_items", "added_files", "removed_files", "changed_files"}
    summary = _exact(value["summary"], summary_keys, "delta.summary")
    for key in summary_keys:
        if isinstance(summary[key], bool) or not isinstance(summary[key], int) or summary[key] < 0:
            raise ArchiveMetadataError(f"delta.summary.{key} is invalid")
    items = value["items"]
    if not isinstance(items, list) or len(items) > 200:
        raise ArchiveMetadataError("delta.items is invalid")
    identifiers: list[str] = []
    computed = {key: 0 for key in summary_keys}
    for index, item in enumerate(items):
        row = _exact(item, {"identifier", "state", "previous_payload_sha256", "current_payload_sha256", "added_files", "removed_files", "changed_files"}, f"delta.items[{index}]")
        identifiers.append(_identifier(row["identifier"], f"delta.items[{index}].identifier"))
        if row["state"] not in {"added", "removed", "changed", "unchanged"}:
            raise ArchiveMetadataError("delta item state is invalid")
        for digest_key in ("previous_payload_sha256", "current_payload_sha256"):
            if row[digest_key] is not None:
                _sha256(row[digest_key], f"delta.items[{index}].{digest_key}")
        for names_key in ("added_files", "removed_files", "changed_files"):
            names = row[names_key]
            if not isinstance(names, list) or names != sorted(set(names)) or any(not isinstance(name, str) or not name for name in names):
                raise ArchiveMetadataError(f"delta.items[{index}].{names_key} is invalid")
            computed[names_key] += len(names)
        computed[f"{row['state']}_items"] += 1
    if identifiers != sorted(set(identifiers)) or computed != summary:
        raise ArchiveMetadataError("delta ordering or summary is inconsistent")
    expected_policy = {"state": "unreviewed_provider_metadata_delta", "content_change_asserted": False, "publication_authority": False}
    if _exact(value["assertion_policy"], set(expected_policy), "delta.assertion_policy") != expected_policy:
        raise ArchiveMetadataError("delta assertion policy differs")
    payload = {key: value[key] for key in value if key != "delta_id"}
    if value["delta_id"] != stable_id("iamd", payload):
        raise ArchiveMetadataError("delta ID is inconsistent")
    expected = build_delta(previous_path, current_path)
    if body != pretty_bytes(expected):
        raise ArchiveMetadataError(
            "delta does not reproduce from the supplied sealed snapshots"
        )
    return value


def _write_delta(previous: Path, current: Path, output: Path) -> Path:
    if not output.is_absolute() or output.suffix != ".json":
        raise ArchiveMetadataError("delta output must be an absolute .json path")
    if Path(os.path.normpath(str(output))) != output:
        raise ArchiveMetadataError("delta output must not contain traversal")
    delta = build_delta(previous, current)
    payload = pretty_bytes(delta)
    if len(payload) > MAX_DELTA_BYTES:
        raise ArchiveMetadataError(f"delta exceeds {MAX_DELTA_BYTES} bytes")
    output.parent.mkdir(parents=True, exist_ok=True)
    if output.parent.resolve(strict=True) != output.parent or not output.parent.is_dir():
        raise ArchiveMetadataError("delta output parent must be a resolved directory")
    created = False
    try:
        with output.open("xb") as handle:
            handle.write(payload)
        created = True
    except FileExistsError:
        if _read(output, MAX_DELTA_BYTES, "existing delta", sealed=True) != payload:
            raise ArchiveMetadataError("refusing to overwrite a different delta")
    if created:
        _seal(output)
    try:
        validate_delta(output, previous, current)
    except Exception:
        if created:
            output.chmod(output.stat().st_mode | 0o600)
            output.unlink(missing_ok=True)
        raise
    return output


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="archive-metadata", description=__doc__)
    commands = parser.add_subparsers(dest="command", required=True)
    capture = commands.add_parser("capture")
    capture.add_argument("--request", required=True)
    capture.add_argument("--output-root", required=True)
    capture.add_argument("--timeout-seconds", type=int, default=60)
    capture.add_argument("--max-item-bytes", type=int, default=DEFAULT_MAX_ITEM_BYTES)
    validate = commands.add_parser("validate-snapshot")
    validate.add_argument("--snapshot", required=True)
    delta = commands.add_parser("delta")
    delta.add_argument("--previous", required=True)
    delta.add_argument("--current", required=True)
    delta.add_argument("--output", required=True)
    validate_delta_parser = commands.add_parser("validate-delta")
    validate_delta_parser.add_argument("--delta", required=True)
    validate_delta_parser.add_argument("--previous", required=True)
    validate_delta_parser.add_argument("--current", required=True)
    request = commands.add_parser("validate-request")
    request.add_argument("--request", required=True)
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    try:
        if args.command == "capture":
            path = capture_snapshot(Path(args.request), output_root=Path(args.output_root), timeout_seconds=args.timeout_seconds, max_item_bytes=args.max_item_bytes)
            result: Any = {"snapshot": str(path), "snapshot_id": validate_snapshot(path)["snapshot_id"]}
        elif args.command == "validate-snapshot":
            snapshot = validate_snapshot(Path(args.snapshot))
            result = {"valid": True, "snapshot_id": snapshot["snapshot_id"], "items": len(snapshot["items"]), "observed_at": snapshot["observed_at"]}
        elif args.command == "delta":
            path = _write_delta(Path(args.previous), Path(args.current), Path(args.output))
            delta = validate_delta(Path(path), Path(args.previous), Path(args.current))
            result = {"delta": str(path), "delta_id": delta["delta_id"], "summary": delta["summary"]}
        elif args.command == "validate-delta":
            delta = validate_delta(
                Path(args.delta), Path(args.previous), Path(args.current)
            )
            result = {"valid": True, "delta_id": delta["delta_id"], "summary": delta["summary"]}
        else:
            request = validate_request_manifest(Path(args.request))
            result = {"valid": True, "request_id": request["request_id"], "items": len(request["items"])}
    except (ArchiveMetadataError, OSError) as error:
        print(f"archive-metadata: {error}", file=sys.stderr)
        return 1
    print(json.dumps(result, ensure_ascii=False, sort_keys=True, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
