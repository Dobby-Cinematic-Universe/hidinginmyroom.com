#!/usr/bin/env python3
"""Credential-free, commit-pinned Hugging Face model snapshot admission.

The networked ``download`` command admits every file listed by the public model
API for one exact 40-hex commit.  ``validate`` and ``replay`` are offline-only:
they validate an already committed bundle without constructing a network
client.  A bundle contains the exact upstream API response, the complete model
snapshot, a canonical manifest, and a canonical receipt.  It is committed with
Linux ``renameat2(RENAME_NOREPLACE)`` and sealed to files 0400/directories 0500.

This tool has no token, cookie, proxy, telemetry, cache, catalog, publication,
identity, inference, or model-execution interface.
"""

from __future__ import annotations

import argparse
import ctypes
import datetime as dt
import errno
import hashlib
import json
import os
import re
import secrets
import shutil
import ssl
import stat
import sys
import time
import urllib.error
import urllib.parse
import urllib.request
from pathlib import Path, PurePosixPath
from typing import Any, BinaryIO, Iterable, NoReturn


IMPLEMENTATION_VERSION = "1.0.0"
SCHEMA_VERSION = 1
API_BASE = "https://huggingface.co"
API_CAPTURE_PATH = "provenance/api-response.json"
MANIFEST_NAME = "manifest.json"
RECEIPT_NAME = "receipt.json"
SNAPSHOT_DIRECTORY = "snapshot"
PROVENANCE_DIRECTORY = "provenance"
MAX_API_BYTES = 16 * 1024 * 1024
MAX_MANIFEST_BYTES = 64 * 1024 * 1024
MAX_MODEL_CARD_BYTES = 4 * 1024 * 1024
MAX_PATH_BYTES = 4096
MAX_SEGMENT_BYTES = 255
READ_CHUNK_BYTES = 1024 * 1024
REVISION_RE = re.compile(r"^[0-9a-f]{40}$")
SHA256_RE = re.compile(r"^[0-9a-f]{64}$")
SHA1_RE = re.compile(r"^[0-9a-f]{40}$")
REPOSITORY_RE = re.compile(
    r"^[A-Za-z0-9](?:[A-Za-z0-9._-]{0,95})/"
    r"[A-Za-z0-9](?:[A-Za-z0-9._-]{0,95})$"
)
UTC_TIMESTAMP_RE = re.compile(r"^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}Z$")
SENSITIVE_HEADER_NAMES = {
    "authorization",
    "cookie",
    "proxy-authorization",
    "set-cookie",
    "x-api-key",
}
SAFE_RESPONSE_HEADERS = {
    "content-length",
    "content-type",
    "content-encoding",
    "date",
    "etag",
    "last-modified",
    "x-linked-etag",
    "x-linked-size",
    "x-repo-commit",
    "x-request-id",
}
CREDENTIAL_ENVIRONMENT_NAMES = {
    "HF_TOKEN",
    "HF_API_TOKEN",
    "HF_OAUTH_TOKEN",
    "HF_TOKEN_PATH",
    "HUGGING_FACE_HUB_TOKEN",
    "HUGGINGFACE_HUB_TOKEN",
    "HUGGINGFACE_TOKEN",
    "HUGGINGFACE_TOKEN_FILE",
    "HTTP_AUTHORIZATION",
    "HTTPS_AUTHORIZATION",
}
ALLOWED_REDIRECT_STATUS = {301, 302, 303, 307, 308}
AT_FDCWD = -100
RENAME_NOREPLACE = 1


class AdmissionError(RuntimeError):
    """Expected fail-closed admission or validation error."""


def fail(message: str) -> NoReturn:
    raise AdmissionError(message)


def canonical_bytes(value: Any) -> bytes:
    try:
        text = json.dumps(
            value,
            sort_keys=True,
            separators=(",", ":"),
            ensure_ascii=False,
            allow_nan=False,
        )
    except (TypeError, ValueError) as exc:
        raise AdmissionError("value cannot be represented as canonical JSON") from exc
    return (text + "\n").encode("utf-8")


def sha256_bytes(payload: bytes) -> str:
    return hashlib.sha256(payload).hexdigest()


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(READ_CHUNK_BYTES), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _reject_constant(value: str) -> NoReturn:
    raise ValueError(f"non-finite JSON number is forbidden: {value}")


def _strict_object(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
    result: dict[str, Any] = {}
    for key, value in pairs:
        if key in result:
            raise ValueError(f"duplicate JSON key: {key}")
        result[key] = value
    return result


def strict_json_bytes(payload: bytes, *, label: str) -> Any:
    try:
        text = payload.decode("utf-8", errors="strict")
        return json.loads(
            text,
            object_pairs_hook=_strict_object,
            parse_constant=_reject_constant,
        )
    except (UnicodeDecodeError, json.JSONDecodeError, ValueError) as exc:
        raise AdmissionError(f"{label} is not strict UTF-8 JSON") from exc


def validate_model_card_license_declaration(
    payload: bytes, expected_license: str
) -> None:
    """Require exact card-metadata license evidence without claiming license text."""

    if len(payload) > MAX_MODEL_CARD_BYTES:
        fail("model card exceeds the license-declaration inspection limit")
    try:
        text = payload.decode("utf-8", errors="strict").replace("\r\n", "\n")
    except UnicodeDecodeError as exc:
        raise AdmissionError("model card is not strict UTF-8") from exc
    if not text.startswith("---\n"):
        fail("model card has no YAML front matter for license evidence")
    end = text.find("\n---\n", 4)
    if end < 0:
        fail("model card YAML front matter is not terminated")
    declared: list[str] = []
    for line in text[4:end].splitlines():
        key, separator, value = line.partition(":")
        if separator and key.strip() == "license":
            normalized = value.strip().strip("'\"")
            if normalized:
                declared.append(normalized)
    if declared != [expected_license]:
        fail(
            "model card license declaration does not exactly match "
            f"expected license {expected_license!r}"
        )


def read_bounded(path: Path, maximum: int, *, label: str) -> bytes:
    metadata = safe_regular_file(path, modes={0o400}, label=label)
    if metadata.st_size > maximum:
        fail(f"{label} exceeds its byte limit")
    with path.open("rb") as handle:
        payload = handle.read(maximum + 1)
    if len(payload) != metadata.st_size:
        fail(f"{label} changed while being read")
    return payload


def require_exact_keys(value: Any, expected: set[str], *, label: str) -> dict[str, Any]:
    if not isinstance(value, dict):
        fail(f"{label} must be an object")
    actual = set(value)
    if actual != expected:
        fail(
            f"{label} fields differ: missing={sorted(expected - actual)!r} "
            f"extra={sorted(actual - expected)!r}"
        )
    return value


def require_string(value: Any, *, label: str, maximum: int = 8192) -> str:
    if not isinstance(value, str) or not value or len(value.encode("utf-8")) > maximum:
        fail(f"{label} must be a bounded non-empty string")
    return value


def require_bool(value: Any, *, label: str) -> bool:
    if not isinstance(value, bool):
        fail(f"{label} must be boolean")
    return value


def require_int(value: Any, *, label: str, minimum: int = 0) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value < minimum:
        fail(f"{label} must be an integer >= {minimum}")
    return value


def require_sha256(value: Any, *, label: str) -> str:
    value = require_string(value, label=label, maximum=64)
    if not SHA256_RE.fullmatch(value):
        fail(f"{label} must be lowercase SHA-256")
    return value


def require_sha1(value: Any, *, label: str) -> str:
    value = require_string(value, label=label, maximum=40)
    if not SHA1_RE.fullmatch(value):
        fail(f"{label} must be lowercase SHA-1")
    return value


def validate_timestamp(value: Any, *, label: str) -> str:
    value = require_string(value, label=label, maximum=20)
    if not UTC_TIMESTAMP_RE.fullmatch(value):
        fail(f"{label} must be second-precision UTC ending in Z")
    try:
        dt.datetime.strptime(value, "%Y-%m-%dT%H:%M:%SZ")
    except ValueError as exc:
        raise AdmissionError(f"{label} is not a real UTC timestamp") from exc
    return value


def validate_repository(value: Any) -> str:
    value = require_string(value, label="repository", maximum=193)
    if not REPOSITORY_RE.fullmatch(value):
        fail("repository must be exactly owner/name, not a URL")
    if value in {".", ".."} or ".." in value.split("/"):
        fail("repository contains an unsafe path component")
    return value


def safe_relative_path(value: str, *, label: str) -> str:
    value = require_string(value, label=label, maximum=MAX_PATH_BYTES)
    if "\\" in value or value.startswith("/") or value.endswith("/"):
        fail(f"{label} must be a normalized POSIX relative file path")
    if any(ord(character) < 32 or ord(character) == 127 for character in value):
        fail(f"{label} contains a control character")
    pure = PurePosixPath(value)
    parts = pure.parts
    if not parts or any(part in {"", ".", ".."} for part in parts):
        fail(f"{label} contains an unsafe component")
    if pure.as_posix() != value:
        fail(f"{label} is not normalized")
    if any(len(part.encode("utf-8")) > MAX_SEGMENT_BYTES for part in parts):
        fail(f"{label} contains an overlong component")
    return value


def assert_credential_free_environment() -> None:
    present = sorted(name for name in CREDENTIAL_ENVIRONMENT_NAMES if name in os.environ)
    if present:
        fail(f"credential-bearing environment variables are forbidden: {present!r}")


def safe_regular_file(path: Path, *, modes: set[int], label: str) -> os.stat_result:
    try:
        metadata = path.lstat()
    except FileNotFoundError as exc:
        raise AdmissionError(f"{label} is missing") from exc
    if not stat.S_ISREG(metadata.st_mode):
        fail(f"{label} must be a regular non-symlink file")
    if metadata.st_uid != os.getuid():
        fail(f"{label} must be owned by the current user")
    if metadata.st_nlink != 1:
        fail(f"{label} must have exactly one hard link")
    if stat.S_IMODE(metadata.st_mode) not in modes:
        fail(f"{label} has an unsafe mode")
    return metadata


def safe_directory(path: Path, *, modes: set[int], label: str) -> os.stat_result:
    try:
        metadata = path.lstat()
    except FileNotFoundError as exc:
        raise AdmissionError(f"{label} is missing") from exc
    if not stat.S_ISDIR(metadata.st_mode) or stat.S_ISLNK(metadata.st_mode):
        fail(f"{label} must be a real directory")
    if metadata.st_uid != os.getuid():
        fail(f"{label} must be owned by the current user")
    if stat.S_IMODE(metadata.st_mode) not in modes:
        fail(f"{label} has an unsafe mode")
    return metadata


def validate_new_output(output: Path) -> tuple[Path, int]:
    if not output.is_absolute():
        fail("output bundle path must be absolute")
    output_name = output.name
    if (
        output_name in {"", ".", ".."}
        or len(output_name.encode("utf-8")) > 160
        or any(ord(character) < 32 or ord(character) == 127 for character in output_name)
    ):
        fail("output bundle name is unsafe or too long")
    parent = output.parent
    try:
        canonical_parent = parent.resolve(strict=True)
    except OSError as exc:
        raise AdmissionError("output parent is not an accessible existing directory") from exc
    if canonical_parent != parent:
        fail("output parent must be an existing non-symlinked canonical path")
    safe_directory(parent, modes={0o700}, label="output parent")
    if output.exists() or output.is_symlink():
        fail("output bundle already exists; replacement is forbidden")
    descriptor = os.open(parent, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW)
    observed = os.fstat(descriptor)
    expected = parent.stat()
    if (observed.st_dev, observed.st_ino) != (expected.st_dev, expected.st_ino):
        os.close(descriptor)
        fail("output parent changed while being opened")
    return parent, descriptor


def make_staging(parent: Path, parent_fd: int, final_name: str) -> tuple[str, Path]:
    for _ in range(16):
        name = f".{final_name}.staging-{os.getpid()}-{secrets.token_hex(16)}"
        try:
            os.mkdir(name, mode=0o700, dir_fd=parent_fd)
            return name, parent / name
        except FileExistsError:
            continue
    fail("could not allocate a unique staging directory")


def remove_owned_staging(path: Path, *, parent: Path, expected_prefix: str) -> None:
    try:
        metadata = path.lstat()
    except FileNotFoundError:
        return
    if path.parent != parent or not path.name.startswith(expected_prefix):
        fail("refusing to clean an unexpected staging path")
    if not stat.S_ISDIR(metadata.st_mode) or stat.S_ISLNK(metadata.st_mode):
        fail("refusing to clean an unsafe staging object")
    if metadata.st_uid != os.getuid():
        fail("refusing to clean staging owned by another user")
    for directory, directory_names, file_names in os.walk(path, topdown=False, followlinks=False):
        directory_path = Path(directory)
        for name in file_names:
            child = directory_path / name
            child_metadata = child.lstat()
            if (
                not stat.S_ISREG(child_metadata.st_mode)
                or stat.S_ISLNK(child_metadata.st_mode)
                or child_metadata.st_uid != os.getuid()
                or child_metadata.st_nlink != 1
            ):
                fail("refusing to clean an unsafe staging file")
            os.chmod(child, 0o600, follow_symlinks=False)
        for name in directory_names:
            child = directory_path / name
            child_metadata = child.lstat()
            if (
                not stat.S_ISDIR(child_metadata.st_mode)
                or stat.S_ISLNK(child_metadata.st_mode)
                or child_metadata.st_uid != os.getuid()
            ):
                fail("refusing to clean an unsafe staging directory")
            os.chmod(child, 0o700, follow_symlinks=False)
        os.chmod(directory_path, 0o700, follow_symlinks=False)
    shutil.rmtree(path)


def create_private_directory(path: Path) -> None:
    try:
        path.mkdir(mode=0o700)
    except FileExistsError:
        safe_directory(path, modes={0o700}, label=f"directory {path}")


def ensure_snapshot_parent(root: Path, relative: str) -> Path:
    current = root
    for part in PurePosixPath(relative).parent.parts:
        current = current / part
        if current.exists() or current.is_symlink():
            safe_directory(current, modes={0o700}, label=f"snapshot directory {current}")
        else:
            current.mkdir(mode=0o700)
            safe_directory(current, modes={0o700}, label=f"snapshot directory {current}")
    return current


def write_exclusive_file(path: Path, payload: bytes, *, mode: int = 0o600) -> None:
    descriptor = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW, mode)
    try:
        with os.fdopen(descriptor, "wb", closefd=False) as handle:
            handle.write(payload)
            handle.flush()
            os.fsync(handle.fileno())
        metadata = os.fstat(descriptor)
        if not stat.S_ISREG(metadata.st_mode) or metadata.st_nlink != 1:
            fail(f"new file is unsafe: {path}")
        if metadata.st_uid != os.getuid() or metadata.st_size != len(payload):
            fail(f"new file identity mismatch: {path}")
    finally:
        os.close(descriptor)


def implementation_descriptor() -> dict[str, Any]:
    path = Path(__file__)
    if not path.is_absolute():
        path = Path.cwd() / path
    if path.resolve(strict=True) != path:
        fail("implementation path must be canonical and non-symlinked")
    metadata = path.lstat()
    if (
        not stat.S_ISREG(metadata.st_mode)
        or stat.S_ISLNK(metadata.st_mode)
        or metadata.st_uid != os.getuid()
        or metadata.st_nlink != 1
    ):
        fail("implementation path is not a safe owner-only regular file")
    return {
        "name": "himr-hf-model-admission",
        "version": IMPLEMENTATION_VERSION,
        "path": str(path),
        "byte_count": metadata.st_size,
        "sha256": sha256_file(path),
    }


def allowed_download_host(host: str) -> bool:
    host = host.lower().rstrip(".")
    return (
        host == "huggingface.co"
        or host.endswith(".huggingface.co")
        or host == "hf.co"
        or host.endswith(".hf.co")
        or host == "xethub.hf.co"
        or host.endswith(".xethub.hf.co")
    )


def validate_https_url(url: str, *, api_only: bool = False) -> urllib.parse.SplitResult:
    try:
        parsed = urllib.parse.urlsplit(url)
        port = parsed.port
    except ValueError as exc:
        raise AdmissionError("upstream returned a malformed URL") from exc
    if parsed.scheme != "https" or not parsed.hostname or parsed.username or parsed.password:
        fail("only credential-free HTTPS URLs are permitted")
    if port not in {None, 443} or parsed.fragment:
        fail("upstream URL uses a forbidden port or fragment")
    host = parsed.hostname.lower().rstrip(".")
    if api_only and host != "huggingface.co":
        fail("model API request left huggingface.co")
    if not api_only and not allowed_download_host(host):
        fail("download redirect left the approved Hugging Face delivery hosts")
    return parsed


def sanitized_url(url: str, *, api_only: bool = False) -> dict[str, Any]:
    parsed = validate_https_url(url, api_only=api_only)
    query = parsed.query.encode("utf-8")
    return {
        "scheme": "https",
        "host": parsed.hostname.lower().rstrip("."),
        "port": parsed.port or 443,
        "path": parsed.path,
        "query_present": bool(query),
        "query_sha256": sha256_bytes(query) if query else None,
    }


def selected_headers(headers: Any) -> dict[str, str]:
    result: dict[str, str] = {}
    for name in SAFE_RESPONSE_HEADERS:
        value = headers.get(name)
        if value is None:
            continue
        if len(value.encode("utf-8", errors="strict")) > 8192:
            fail(f"upstream {name} header is too large")
        if "\r" in value or "\n" in value:
            fail(f"upstream {name} header is malformed")
        result[name] = value
    return dict(sorted(result.items()))


class GuardedRedirectHandler(urllib.request.HTTPRedirectHandler):
    def __init__(self) -> None:
        super().__init__()
        self.chain: list[dict[str, Any]] = []
        self.api_only = False

    def reset(self, *, api_only: bool) -> None:
        self.chain = []
        self.api_only = api_only

    def redirect_request(  # type: ignore[override]
        self,
        req: urllib.request.Request,
        fp: BinaryIO,
        code: int,
        msg: str,
        headers: Any,
        newurl: str,
    ) -> urllib.request.Request | None:
        if code not in ALLOWED_REDIRECT_STATUS:
            fail("upstream used an unsupported redirect status")
        for name in req.headers:
            if name.lower() in SENSITIVE_HEADER_NAMES:
                fail("a credential-bearing request header was detected")
        validate_https_url(newurl, api_only=self.api_only)
        self.chain.append(
            {
                "status": code,
                "from": sanitized_url(req.full_url, api_only=self.api_only),
                "to": sanitized_url(newurl, api_only=self.api_only),
                "headers": selected_headers(headers),
            }
        )
        redirected = super().redirect_request(req, fp, code, msg, headers, newurl)
        if redirected is not None:
            for name in redirected.headers:
                if name.lower() in SENSITIVE_HEADER_NAMES:
                    fail("redirect attempted to propagate a credential-bearing header")
        return redirected


class PublicHttpClient:
    def __init__(self, timeout_seconds: int) -> None:
        self.timeout_seconds = timeout_seconds
        self.redirects = GuardedRedirectHandler()
        context = ssl.create_default_context()
        self.opener = urllib.request.build_opener(
            urllib.request.ProxyHandler({}),
            urllib.request.HTTPSHandler(context=context),
            self.redirects,
        )

    @staticmethod
    def _request(url: str) -> urllib.request.Request:
        return urllib.request.Request(
            url,
            method="GET",
            headers={
                "Accept": "application/json, application/octet-stream;q=0.9",
                "Accept-Encoding": "identity",
                "User-Agent": f"HIMR-HF-Model-Admission/{IMPLEMENTATION_VERSION}",
            },
        )

    def open(self, url: str, *, api_only: bool) -> tuple[Any, dict[str, Any]]:
        validate_https_url(url, api_only=api_only)
        self.redirects.reset(api_only=api_only)
        request = self._request(url)
        try:
            response = self.opener.open(request, timeout=self.timeout_seconds)
        except urllib.error.HTTPError as exc:
            safe = sanitized_url(exc.geturl(), api_only=api_only)
            raise AdmissionError(f"upstream HTTP error {exc.code} at {safe!r}") from None
        except urllib.error.URLError as exc:
            reason = type(exc.reason).__name__
            raise AdmissionError(f"upstream transport error ({reason})") from None
        status = getattr(response, "status", None)
        if status != 200:
            response.close()
            fail(f"upstream returned unexpected status {status!r}")
        final_url = response.geturl()
        validate_https_url(final_url, api_only=api_only)
        headers = selected_headers(response.headers)
        encoding = headers.get("content-encoding")
        if encoding is not None and encoding.lower() != "identity":
            response.close()
            fail("compressed HTTP bodies are forbidden")
        observation = {
            "request": {
                "url": sanitized_url(url, api_only=api_only),
                "method": "GET",
                "headers": {
                    "accept_encoding": "identity",
                    "authorization": False,
                    "cookies": False,
                },
            },
            "redirects": list(self.redirects.chain),
            "response": {
                "status": status,
                "url": sanitized_url(final_url, api_only=api_only),
                "headers": headers,
            },
        }
        return response, observation


def read_response_bounded(response: Any, maximum: int, *, deadline: float) -> bytes:
    result = bytearray()
    while True:
        if time.monotonic() > deadline:
            fail("network admission exceeded the wall-clock bound")
        chunk = response.read(min(READ_CHUNK_BYTES, maximum + 1 - len(result)))
        if not chunk:
            break
        result.extend(chunk)
        if len(result) > maximum:
            fail("upstream API response exceeded the byte bound")
    return bytes(result)


def api_url(repository: str, revision: str) -> str:
    repo = "/".join(urllib.parse.quote(part, safe="") for part in repository.split("/"))
    return f"{API_BASE}/api/models/{repo}/revision/{revision}?blobs=true"


def file_url(repository: str, revision: str, relative_path: str) -> str:
    repo = "/".join(urllib.parse.quote(part, safe="") for part in repository.split("/"))
    path = "/".join(urllib.parse.quote(part, safe="") for part in relative_path.split("/"))
    return f"{API_BASE}/{repo}/resolve/{revision}/{path}?download=true"


def api_model_files(
    api: Any,
    *,
    repository: str,
    revision: str,
    expected_license: str,
    max_files: int,
    max_file_bytes: int,
    max_total_bytes: int,
) -> list[dict[str, Any]]:
    if not isinstance(api, dict):
        fail("model API response must be an object")
    identities = [api[key] for key in ("modelId", "id") if key in api]
    if not identities or any(identity != repository for identity in identities):
        fail("model API repository identity does not match the request")
    if api.get("sha") != revision:
        fail("model API did not resolve to the requested full commit")
    if api.get("private") is not False:
        fail("only a public repository may be admitted")
    gated = api.get("gated")
    if gated is not False and gated is not None:
        fail("gated repositories are outside the credential-free boundary")
    disabled = api.get("disabled")
    if disabled is not False and disabled is not None:
        fail("disabled repositories cannot be admitted")
    card_data = api.get("cardData")
    if not isinstance(card_data, dict) or card_data.get("license") != expected_license:
        fail("model-card license metadata does not match --expected-license")
    siblings = api.get("siblings")
    if not isinstance(siblings, list) or not siblings:
        fail("model API returned no snapshot files")
    if len(siblings) > max_files:
        fail("model API file count exceeds --max-files")
    output: list[dict[str, Any]] = []
    seen: set[str] = set()
    total = 0
    for ordinal, sibling in enumerate(siblings):
        if not isinstance(sibling, dict):
            fail("model API sibling entry is not an object")
        relative = safe_relative_path(sibling.get("rfilename"), label="API sibling path")
        if relative in seen:
            fail("model API returned a duplicate sibling path")
        seen.add(relative)
        lfs = sibling.get("lfs")
        blob_id = sibling.get("blobId")
        if lfs is not None:
            if not isinstance(lfs, dict):
                fail("model API LFS metadata is malformed")
            digest = require_sha256(lfs.get("sha256"), label="API LFS SHA-256")
            size = require_int(lfs.get("size"), label="API LFS byte count")
            top_size = sibling.get("size")
            if top_size is not None and require_int(top_size, label="API sibling size") != size:
                fail("model API LFS and sibling sizes disagree")
            upstream = {
                "kind": "huggingface_lfs_sha256",
                "byte_count": size,
                "sha256": digest,
                "git_blob_id": (
                    require_sha1(blob_id, label="API Git blob ID") if blob_id is not None else None
                ),
            }
        else:
            size = require_int(sibling.get("size"), label="API sibling size")
            upstream = {
                "kind": "git_blob_sha1",
                "byte_count": size,
                "git_blob_id": require_sha1(blob_id, label="API Git blob ID"),
            }
        if size > max_file_bytes:
            fail(f"snapshot file exceeds --max-file-bytes: {relative}")
        total += size
        if total > max_total_bytes:
            fail("declared snapshot size exceeds --max-total-bytes")
        output.append({"api_ordinal": ordinal, "path": relative, "upstream": upstream})
    return sorted(output, key=lambda item: item["path"])


def stream_download(
    response: Any,
    destination: Path,
    *,
    expected_size: int,
    expected_upstream: dict[str, Any],
    max_file_bytes: int,
    deadline: float,
) -> tuple[int, str, str]:
    descriptor = os.open(
        destination,
        os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW,
        0o600,
    )
    sha256 = hashlib.sha256()
    git_blob_sha1 = hashlib.sha1()
    git_blob_sha1.update(f"blob {expected_size}\0".encode("ascii"))
    byte_count = 0
    try:
        with os.fdopen(descriptor, "wb", closefd=False) as handle:
            while True:
                if time.monotonic() > deadline:
                    fail("network admission exceeded the wall-clock bound")
                chunk = response.read(READ_CHUNK_BYTES)
                if not chunk:
                    break
                byte_count += len(chunk)
                if byte_count > expected_size or byte_count > max_file_bytes:
                    fail("download exceeded its declared or configured byte bound")
                sha256.update(chunk)
                git_blob_sha1.update(chunk)
                handle.write(chunk)
            handle.flush()
            os.fsync(handle.fileno())
        metadata = os.fstat(descriptor)
        if not stat.S_ISREG(metadata.st_mode) or metadata.st_nlink != 1:
            fail("download destination became unsafe")
        if metadata.st_uid != os.getuid() or metadata.st_size != byte_count:
            fail("download destination identity mismatch")
    finally:
        os.close(descriptor)
    if byte_count != expected_size:
        fail("download byte count does not match the commit-pinned API metadata")
    sha256_value = sha256.hexdigest()
    git_blob_value = git_blob_sha1.hexdigest()
    if expected_upstream["kind"] == "huggingface_lfs_sha256":
        if sha256_value != expected_upstream["sha256"]:
            fail("download SHA-256 does not match Hugging Face LFS metadata")
    elif git_blob_value != expected_upstream["git_blob_id"]:
        fail("download Git blob identity does not match Hugging Face metadata")
    metadata = safe_regular_file(destination, modes={0o600}, label="downloaded snapshot file")
    if metadata.st_size != byte_count:
        fail("downloaded file changed after close")
    return byte_count, sha256_value, git_blob_value


def check_content_length(observation: dict[str, Any], expected_size: int) -> None:
    value = observation["response"]["headers"].get("content-length")
    if value is None:
        return
    if not value.isascii() or not value.isdigit() or int(value) != expected_size:
        fail("HTTP Content-Length does not match commit-pinned API metadata")


def ensure_repo_commit_headers(observation: dict[str, Any], revision: str) -> None:
    header_sets = [item["headers"] for item in observation["redirects"]]
    header_sets.append(observation["response"]["headers"])
    for headers in header_sets:
        value = headers.get("x-repo-commit")
        if value is not None and value != revision:
            fail("upstream X-Repo-Commit header does not match the requested commit")


def snapshot_tree(root: Path, expected_mode: int) -> tuple[list[Path], list[Path]]:
    directories: list[Path] = []
    files: list[Path] = []
    for directory, directory_names, file_names in os.walk(root, followlinks=False):
        directory_path = Path(directory)
        safe_directory(
            directory_path,
            modes={expected_mode},
            label=f"bundle directory {directory_path}",
        )
        directories.append(directory_path)
        for name in sorted(directory_names):
            child = directory_path / name
            metadata = child.lstat()
            if stat.S_ISLNK(metadata.st_mode) or not stat.S_ISDIR(metadata.st_mode):
                fail(f"bundle contains an unsafe directory entry: {child}")
        for name in sorted(file_names):
            path = directory_path / name
            safe_regular_file(
                path,
                modes={0o600 if expected_mode == 0o700 else 0o400},
                label=f"bundle file {path}",
            )
            files.append(path)
    return directories, files


def seal_tree(root: Path) -> None:
    directories, files = snapshot_tree(root, 0o700)
    before = [(path.relative_to(root).as_posix(), path.stat().st_size, sha256_file(path)) for path in files]
    for path in files:
        os.chmod(path, 0o400, follow_symlinks=False)
        with path.open("rb") as handle:
            os.fsync(handle.fileno())
    for directory in sorted(directories, key=lambda item: len(item.parts), reverse=True):
        os.chmod(directory, 0o500, follow_symlinks=False)
    _, after_files = snapshot_tree(root, 0o500)
    after = [
        (path.relative_to(root).as_posix(), path.stat().st_size, sha256_file(path))
        for path in after_files
    ]
    if before != after:
        fail("bundle bytes changed while sealing")


def rename_noreplace(parent_fd: int, source_name: str, destination_name: str) -> None:
    libc = ctypes.CDLL(None, use_errno=True)
    function = getattr(libc, "renameat2", None)
    if function is None:
        fail("Linux renameat2(RENAME_NOREPLACE) is required")
    function.argtypes = [ctypes.c_int, ctypes.c_char_p, ctypes.c_int, ctypes.c_char_p, ctypes.c_uint]
    function.restype = ctypes.c_int
    result = function(
        parent_fd,
        os.fsencode(source_name),
        parent_fd,
        os.fsencode(destination_name),
        RENAME_NOREPLACE,
    )
    if result != 0:
        error = ctypes.get_errno()
        if error == errno.EEXIST:
            fail("output bundle appeared concurrently; replacement was refused")
        raise AdmissionError(f"atomic no-replace commit failed with errno {error}")
    os.fsync(parent_fd)


def expected_api_upstream(sibling: dict[str, Any]) -> dict[str, Any]:
    lfs = sibling.get("lfs")
    blob_id = sibling.get("blobId")
    if lfs is not None:
        if not isinstance(lfs, dict):
            fail("captured API LFS metadata is malformed")
        return {
            "kind": "huggingface_lfs_sha256",
            "byte_count": require_int(lfs.get("size"), label="captured LFS size"),
            "sha256": require_sha256(lfs.get("sha256"), label="captured LFS SHA-256"),
            "git_blob_id": (
                require_sha1(blob_id, label="captured blob ID") if blob_id is not None else None
            ),
        }
    return {
        "kind": "git_blob_sha1",
        "byte_count": require_int(sibling.get("size"), label="captured sibling size"),
        "git_blob_id": require_sha1(blob_id, label="captured blob ID"),
    }


def validate_url_record(value: Any, *, label: str, api_only: bool = False) -> None:
    record = require_exact_keys(
        value,
        {"scheme", "host", "port", "path", "query_present", "query_sha256"},
        label=label,
    )
    if record["scheme"] != "https" or require_int(record["port"], label=f"{label}.port") != 443:
        fail(f"{label} does not describe HTTPS on port 443")
    host = require_string(record["host"], label=f"{label}.host", maximum=255)
    if host != host.lower().rstrip("."):
        fail(f"{label} host is not canonical")
    if api_only and host != "huggingface.co":
        fail(f"{label} is not a huggingface.co API URL")
    if not api_only and not allowed_download_host(host):
        fail(f"{label} contains an unapproved host")
    require_string(record["path"], label=f"{label}.path", maximum=MAX_PATH_BYTES * 3)
    present = require_bool(record["query_present"], label=f"{label}.query_present")
    if present:
        require_sha256(record["query_sha256"], label=f"{label}.query_sha256")
    elif record["query_sha256"] is not None:
        fail(f"{label}.query_sha256 must be null when no query was present")


def validate_http_observation(value: Any, *, label: str, api_only: bool = False) -> None:
    observation = require_exact_keys(value, {"request", "redirects", "response"}, label=label)
    request = require_exact_keys(
        observation["request"], {"url", "method", "headers"}, label=f"{label}.request"
    )
    validate_url_record(request["url"], label=f"{label}.request.url", api_only=api_only)
    if request["method"] != "GET":
        fail(f"{label} request method must be GET")
    headers = require_exact_keys(
        request["headers"], {"accept_encoding", "authorization", "cookies"}, label=f"{label}.request.headers"
    )
    if headers != {"accept_encoding": "identity", "authorization": False, "cookies": False}:
        fail(f"{label} request safety headers differ")
    redirects = observation["redirects"]
    if not isinstance(redirects, list) or len(redirects) > 16:
        fail(f"{label}.redirects must be a bounded list")
    previous_url = request["url"]
    for index, redirect in enumerate(redirects):
        item = require_exact_keys(
            redirect, {"status", "from", "to", "headers"}, label=f"{label}.redirects[{index}]"
        )
        if require_int(item["status"], label="redirect status") not in ALLOWED_REDIRECT_STATUS:
            fail("stored redirect has an unsupported status")
        validate_url_record(item["from"], label="redirect source", api_only=api_only)
        validate_url_record(item["to"], label="redirect target", api_only=api_only)
        if item["from"] != previous_url:
            fail(f"{label} redirect chain is discontinuous")
        previous_url = item["to"]
        validate_header_record(item["headers"], label="redirect headers")
    response = require_exact_keys(
        observation["response"], {"status", "url", "headers"}, label=f"{label}.response"
    )
    if response["status"] != 200:
        fail(f"{label} response status must be 200")
    validate_url_record(response["url"], label=f"{label}.response.url", api_only=api_only)
    if response["url"] != previous_url:
        fail(f"{label} final response URL does not close its redirect chain")
    validate_header_record(response["headers"], label=f"{label}.response.headers")
    encoding = response["headers"].get("content-encoding")
    if encoding is not None and encoding.lower() != "identity":
        fail(f"{label} records a compressed response body")


def validate_header_record(value: Any, *, label: str) -> None:
    if not isinstance(value, dict):
        fail(f"{label} must be an object")
    if not set(value).issubset(SAFE_RESPONSE_HEADERS):
        fail(f"{label} contains a forbidden header")
    for key, item in value.items():
        require_string(item, label=f"{label}.{key}", maximum=8192)


def validate_file_entry(value: Any, *, label: str) -> dict[str, Any]:
    entry = require_exact_keys(
        value,
        {"api_ordinal", "path", "byte_count", "sha256", "git_blob_sha1", "upstream", "http"},
        label=label,
    )
    require_int(entry["api_ordinal"], label=f"{label}.api_ordinal")
    safe_relative_path(entry["path"], label=f"{label}.path")
    require_int(entry["byte_count"], label=f"{label}.byte_count")
    require_sha256(entry["sha256"], label=f"{label}.sha256")
    require_sha1(entry["git_blob_sha1"], label=f"{label}.git_blob_sha1")
    upstream = entry["upstream"]
    if not isinstance(upstream, dict) or upstream.get("kind") not in {
        "huggingface_lfs_sha256",
        "git_blob_sha1",
    }:
        fail(f"{label}.upstream is invalid")
    expected_keys = (
        {"kind", "byte_count", "sha256", "git_blob_id"}
        if upstream["kind"] == "huggingface_lfs_sha256"
        else {"kind", "byte_count", "git_blob_id"}
    )
    require_exact_keys(upstream, expected_keys, label=f"{label}.upstream")
    require_int(upstream["byte_count"], label=f"{label}.upstream.byte_count")
    if upstream["kind"] == "huggingface_lfs_sha256":
        require_sha256(upstream["sha256"], label=f"{label}.upstream.sha256")
        if upstream["git_blob_id"] is not None:
            require_sha1(upstream["git_blob_id"], label=f"{label}.upstream.git_blob_id")
    else:
        require_sha1(upstream["git_blob_id"], label=f"{label}.upstream.git_blob_id")
    validate_http_observation(entry["http"], label=f"{label}.http")
    return entry


MANIFEST_KEYS = {
    "kind",
    "schema_version",
    "identity_sha256",
    "admitted_at",
    "repository",
    "revision",
    "expected_license",
    "api_declared_license",
    "license_evidence",
    "model_card_path",
    "license_path",
    "bundle_root",
    "bundle_layout",
    "api_capture",
    "files",
    "required_documents",
    "implementation",
    "limits",
    "network_policy",
    "authority_policy",
}
RECEIPT_KEYS = {
    "kind",
    "schema_version",
    "receipt_id",
    "identity_sha256",
    "admitted_at",
    "bundle_root",
    "manifest",
    "snapshot",
    "commit",
    "authority_policy",
}


def validate_bundle(bundle: Path, *, logical_root: Path | None = None) -> dict[str, Any]:
    if not bundle.is_absolute():
        fail("bundle path must be absolute")
    try:
        canonical_bundle = bundle.resolve(strict=True)
    except OSError as exc:
        raise AdmissionError("bundle root is not an accessible existing directory") from exc
    if canonical_bundle != bundle:
        fail("bundle path must be canonical and non-symlinked")
    root_for_records = logical_root or bundle
    safe_directory(bundle, modes={0o500}, label="bundle root")
    manifest_path = bundle / MANIFEST_NAME
    receipt_path = bundle / RECEIPT_NAME
    manifest_bytes = read_bounded(manifest_path, MAX_MANIFEST_BYTES, label="manifest")
    receipt_bytes = read_bounded(receipt_path, MAX_MANIFEST_BYTES, label="receipt")
    manifest = require_exact_keys(
        strict_json_bytes(manifest_bytes, label="manifest"), MANIFEST_KEYS, label="manifest"
    )
    receipt = require_exact_keys(
        strict_json_bytes(receipt_bytes, label="receipt"), RECEIPT_KEYS, label="receipt"
    )
    if canonical_bytes(manifest) != manifest_bytes or canonical_bytes(receipt) != receipt_bytes:
        fail("manifest and receipt must use exact canonical JSON bytes")
    if (
        manifest["kind"] != "himr_hf_model_snapshot_manifest"
        or require_int(manifest["schema_version"], label="manifest schema_version", minimum=1)
        != SCHEMA_VERSION
    ):
        fail("unsupported model snapshot manifest")
    if (
        receipt["kind"] != "himr_hf_model_admission_receipt"
        or require_int(receipt["schema_version"], label="receipt schema_version", minimum=1)
        != SCHEMA_VERSION
    ):
        fail("unsupported model admission receipt")
    manifest_identity = dict(manifest)
    stored_manifest_identity = require_sha256(
        manifest_identity.pop("identity_sha256"), label="manifest identity_sha256"
    )
    if sha256_bytes(canonical_bytes(manifest_identity)) != stored_manifest_identity:
        fail("manifest identity_sha256 mismatch")
    receipt_identity = dict(receipt)
    stored_receipt_identity = require_sha256(
        receipt_identity.pop("identity_sha256"), label="receipt identity_sha256"
    )
    if sha256_bytes(canonical_bytes(receipt_identity)) != stored_receipt_identity:
        fail("receipt identity_sha256 mismatch")
    repository = validate_repository(manifest["repository"])
    revision = require_string(manifest["revision"], label="revision", maximum=40)
    if not REVISION_RE.fullmatch(revision):
        fail("manifest revision is not a full lowercase commit")
    expected_license = require_string(
        manifest["expected_license"], label="expected license", maximum=512
    )
    if manifest["api_declared_license"] != expected_license:
        fail("manifest license fields disagree")
    model_card_path = safe_relative_path(manifest["model_card_path"], label="model card path")
    license_path = safe_relative_path(manifest["license_path"], label="license path")
    same_license_evidence = model_card_path == license_path
    expected_license_evidence = {
        "kind": (
            "api_and_model_card_metadata_declaration"
            if same_license_evidence
            else "api_declaration_and_standalone_repository_file"
        ),
        "api_card_data_verified": True,
        "model_card_front_matter_verified": same_license_evidence,
        "standalone_license_file_present": not same_license_evidence,
    }
    if manifest["license_evidence"] != expected_license_evidence:
        fail("manifest license evidence differs from its document layout")
    validate_timestamp(manifest["admitted_at"], label="manifest admitted_at")
    if manifest["bundle_root"] != str(root_for_records):
        fail("manifest bundle_root does not match the admitted path")
    layout = require_exact_keys(
        manifest["bundle_layout"], {"snapshot", "api_capture", "manifest", "receipt"}, label="bundle layout"
    )
    expected_layout = {
        "snapshot": SNAPSHOT_DIRECTORY,
        "api_capture": API_CAPTURE_PATH,
        "manifest": MANIFEST_NAME,
        "receipt": RECEIPT_NAME,
    }
    if canonical_bytes(layout) != canonical_bytes(expected_layout):
        fail("bundle layout differs from the admitted contract")
    network_policy = require_exact_keys(
        manifest["network_policy"],
        {
            "access",
            "authorization",
            "cookies",
            "proxies",
            "telemetry",
            "cache",
            "api_base",
            "full_commit_required",
            "complete_sibling_set_required",
        },
        label="network policy",
    )
    expected_network = {
        "access": "public_unauthenticated_https",
        "authorization": False,
        "cookies": False,
        "proxies": False,
        "telemetry": False,
        "cache": False,
        "api_base": API_BASE,
        "full_commit_required": True,
        "complete_sibling_set_required": True,
    }
    if canonical_bytes(network_policy) != canonical_bytes(expected_network):
        fail("network policy was weakened")
    authority = require_exact_keys(
        manifest["authority_policy"],
        {
            "catalogue_authority",
            "identity_authority",
            "publication_authority",
            "inference_authority",
            "training_performed",
        },
        label="authority policy",
    )
    expected_authority = {
        "catalogue_authority": "none",
        "identity_authority": "none",
        "publication_authority": "none",
        "inference_authority": "none",
        "training_performed": False,
    }
    if (
        canonical_bytes(authority) != canonical_bytes(expected_authority)
        or canonical_bytes(receipt["authority_policy"]) != canonical_bytes(expected_authority)
    ):
        fail("authority policy was weakened")
    limits = require_exact_keys(
        manifest["limits"],
        {"max_files", "max_file_bytes", "max_total_bytes", "timeout_seconds", "max_wall_seconds", "max_api_bytes"},
        label="limits",
    )
    for key in limits:
        require_int(limits[key], label=f"limits.{key}", minimum=1)
    if limits["max_api_bytes"] != MAX_API_BYTES:
        fail("limits.max_api_bytes differs from the implementation bound")
    implementation = require_exact_keys(
        manifest["implementation"], {"name", "version", "path", "byte_count", "sha256"}, label="implementation"
    )
    if implementation["name"] != "himr-hf-model-admission" or implementation["version"] != IMPLEMENTATION_VERSION:
        fail("implementation identity is unsupported")
    current_implementation = implementation_descriptor()
    if implementation != current_implementation:
        fail("implementation bytes differ from the admitted implementation")
    api_capture = require_exact_keys(
        manifest["api_capture"], {"path", "byte_count", "sha256", "http"}, label="API capture"
    )
    if api_capture["path"] != API_CAPTURE_PATH:
        fail("API capture path differs")
    require_int(api_capture["byte_count"], label="API capture byte count", minimum=1)
    require_sha256(api_capture["sha256"], label="API capture SHA-256")
    validate_http_observation(api_capture["http"], label="API capture HTTP", api_only=True)
    if api_capture["http"]["request"]["url"] != sanitized_url(
        api_url(repository, revision), api_only=True
    ):
        fail("API capture request does not match the commit-pinned endpoint")
    api_path = bundle / API_CAPTURE_PATH
    api_bytes = read_bounded(api_path, limits["max_api_bytes"], label="API response capture")
    if len(api_bytes) != api_capture["byte_count"] or sha256_bytes(api_bytes) != api_capture["sha256"]:
        fail("API response capture descriptor mismatch")
    check_content_length(api_capture["http"], len(api_bytes))
    ensure_repo_commit_headers(api_capture["http"], revision)
    api = strict_json_bytes(api_bytes, label="captured API response")
    api_files = api_model_files(
        api,
        repository=repository,
        revision=revision,
        expected_license=expected_license,
        max_files=limits["max_files"],
        max_file_bytes=limits["max_file_bytes"],
        max_total_bytes=limits["max_total_bytes"],
    )
    files = manifest["files"]
    if not isinstance(files, list) or not files:
        fail("manifest files must be a non-empty list")
    validated_files = [validate_file_entry(item, label=f"files[{index}]") for index, item in enumerate(files)]
    if [entry["path"] for entry in validated_files] != sorted(entry["path"] for entry in validated_files):
        fail("manifest files are not path-sorted")
    if len({entry["path"] for entry in validated_files}) != len(validated_files):
        fail("manifest contains duplicate file paths")
    api_by_path = {entry["path"]: entry for entry in api_files}
    if set(api_by_path) != {entry["path"] for entry in validated_files}:
        fail("manifest is not the complete commit-pinned API sibling set")
    actual_snapshot_files: set[str] = set()
    snapshot_root = bundle / SNAPSHOT_DIRECTORY
    safe_directory(snapshot_root, modes={0o500}, label="snapshot root")
    for directory, directory_names, file_names in os.walk(snapshot_root, followlinks=False):
        directory_path = Path(directory)
        safe_directory(directory_path, modes={0o500}, label="snapshot directory")
        for name in directory_names:
            safe_directory(directory_path / name, modes={0o500}, label="snapshot child directory")
        for name in file_names:
            path = directory_path / name
            safe_regular_file(path, modes={0o400}, label="snapshot file")
            actual_snapshot_files.add(path.relative_to(snapshot_root).as_posix())
    if actual_snapshot_files != set(api_by_path):
        fail("snapshot filesystem entries differ from the complete manifest")
    for entry in validated_files:
        api_entry = api_by_path[entry["path"]]
        if entry["api_ordinal"] != api_entry["api_ordinal"] or entry["upstream"] != api_entry["upstream"]:
            fail(f"manifest upstream metadata differs for {entry['path']}")
        check_content_length(entry["http"], entry["byte_count"])
        ensure_repo_commit_headers(entry["http"], revision)
        if entry["http"]["request"]["url"] != sanitized_url(
            file_url(repository, revision, entry["path"])
        ):
            fail(f"download request is not commit-pinned for {entry['path']}")
        path = snapshot_root / entry["path"]
        metadata = safe_regular_file(path, modes={0o400}, label=f"snapshot file {entry['path']}")
        if metadata.st_size != entry["byte_count"] or metadata.st_size != entry["upstream"]["byte_count"]:
            fail(f"snapshot byte count differs for {entry['path']}")
        digest = hashlib.sha256()
        blob = hashlib.sha1()
        blob.update(f"blob {metadata.st_size}\0".encode("ascii"))
        with path.open("rb") as handle:
            for chunk in iter(lambda: handle.read(READ_CHUNK_BYTES), b""):
                digest.update(chunk)
                blob.update(chunk)
        if digest.hexdigest() != entry["sha256"] or blob.hexdigest() != entry["git_blob_sha1"]:
            fail(f"snapshot content digest differs for {entry['path']}")
        if entry["upstream"]["kind"] == "huggingface_lfs_sha256":
            if entry["sha256"] != entry["upstream"]["sha256"]:
                fail(f"LFS SHA-256 differs for {entry['path']}")
        elif entry["git_blob_sha1"] != entry["upstream"]["git_blob_id"]:
            fail(f"Git blob ID differs for {entry['path']}")
    documents = manifest["required_documents"]
    by_path = {entry["path"]: entry for entry in validated_files}
    document_roles = (
        (("model_card_and_license_declaration", model_card_path),)
        if same_license_evidence
        else (("model_card", model_card_path), ("license_document", license_path))
    )
    if not isinstance(documents, list) or len(documents) != len(document_roles):
        fail("required_documents does not match the license evidence layout")
    expected_documents = []
    for role, path in document_roles:
        if path not in by_path:
            fail(f"required {role} is absent from the snapshot")
        item = by_path[path]
        expected_documents.append(
            {"role": role, "path": path, "byte_count": item["byte_count"], "sha256": item["sha256"]}
        )
    if canonical_bytes(documents) != canonical_bytes(expected_documents):
        fail("required document descriptors differ")
    if same_license_evidence:
        validate_model_card_license_declaration(
            read_bounded(
                bundle / SNAPSHOT_DIRECTORY / model_card_path,
                MAX_MODEL_CARD_BYTES,
                label="model card license declaration",
            ),
            expected_license,
        )
    expected_root_entries = {MANIFEST_NAME, RECEIPT_NAME, PROVENANCE_DIRECTORY, SNAPSHOT_DIRECTORY}
    if {path.name for path in bundle.iterdir()} != expected_root_entries:
        fail("bundle root contains unexpected entries")
    provenance = bundle / PROVENANCE_DIRECTORY
    safe_directory(provenance, modes={0o500}, label="provenance directory")
    if {path.name for path in provenance.iterdir()} != {"api-response.json"}:
        fail("provenance directory contains unexpected entries")
    receipt_manifest = require_exact_keys(
        receipt["manifest"], {"path", "byte_count", "sha256", "identity_sha256"}, label="receipt manifest descriptor"
    )
    expected_manifest_descriptor = {
        "path": MANIFEST_NAME,
        "byte_count": len(manifest_bytes),
        "sha256": sha256_bytes(manifest_bytes),
        "identity_sha256": stored_manifest_identity,
    }
    if receipt_manifest != expected_manifest_descriptor:
        fail("receipt manifest descriptor mismatch")
    snapshot = require_exact_keys(
        receipt["snapshot"], {"directory", "file_count", "total_bytes", "files_digest"}, label="receipt snapshot"
    )
    files_digest_basis = [
        {"path": entry["path"], "byte_count": entry["byte_count"], "sha256": entry["sha256"]}
        for entry in validated_files
    ]
    expected_snapshot = {
        "directory": SNAPSHOT_DIRECTORY,
        "file_count": len(validated_files),
        "total_bytes": sum(entry["byte_count"] for entry in validated_files),
        "files_digest": sha256_bytes(canonical_bytes(files_digest_basis)),
    }
    require_string(snapshot["directory"], label="receipt snapshot directory")
    require_int(snapshot["file_count"], label="receipt snapshot file_count")
    require_int(snapshot["total_bytes"], label="receipt snapshot total_bytes")
    require_sha256(snapshot["files_digest"], label="receipt snapshot files_digest")
    if canonical_bytes(snapshot) != canonical_bytes(expected_snapshot):
        fail("receipt snapshot summary mismatch")
    commit = require_exact_keys(
        receipt["commit"], {"method", "replacement_allowed", "sealed_file_mode", "sealed_directory_mode"}, label="commit"
    )
    expected_commit = {
        "method": "linux_renameat2_rename_noreplace",
        "replacement_allowed": False,
        "sealed_file_mode": "0400",
        "sealed_directory_mode": "0500",
    }
    if canonical_bytes(commit) != canonical_bytes(expected_commit):
        fail("receipt commit policy differs")
    expected_receipt_id = f"hfmodelreceipt_{stored_manifest_identity[:32]}"
    if receipt["receipt_id"] != expected_receipt_id:
        fail("receipt ID differs")
    if receipt["admitted_at"] != manifest["admitted_at"] or receipt["bundle_root"] != str(root_for_records):
        fail("receipt path or timestamp differs from manifest")
    return {
        "status": "valid",
        "bundle_root": str(root_for_records),
        "repository": repository,
        "revision": revision,
        "manifest_sha256": sha256_bytes(manifest_bytes),
        "manifest_identity_sha256": stored_manifest_identity,
        "receipt_sha256": sha256_bytes(receipt_bytes),
        "receipt_identity_sha256": stored_receipt_identity,
        "receipt_id": receipt["receipt_id"],
        "file_count": len(validated_files),
        "total_bytes": sum(entry["byte_count"] for entry in validated_files),
    }


def download_command(args: argparse.Namespace) -> dict[str, Any]:
    assert_credential_free_environment()
    repository = validate_repository(args.repository)
    if not REVISION_RE.fullmatch(args.revision):
        fail("revision must be a full lowercase 40-hex commit")
    expected_license = require_string(args.expected_license, label="expected license", maximum=512)
    model_card_path = safe_relative_path(args.model_card_path, label="model card path")
    license_path = safe_relative_path(
        args.license_path if args.license_path is not None else args.model_card_path,
        label="license path",
    )
    same_license_evidence = model_card_path == license_path
    admitted_at = validate_timestamp(args.admitted_at, label="admitted-at")
    for name in ("max_files", "max_file_bytes", "max_total_bytes", "timeout_seconds", "max_wall_seconds"):
        require_int(getattr(args, name), label=name, minimum=1)
    parent, parent_fd = validate_new_output(args.output)
    staging_name = ""
    staging: Path | None = None
    prefix = f".{args.output.name}.staging-"
    try:
        staging_name, staging = make_staging(parent, parent_fd, args.output.name)
        snapshot_root = staging / SNAPSHOT_DIRECTORY
        provenance_root = staging / PROVENANCE_DIRECTORY
        create_private_directory(snapshot_root)
        create_private_directory(provenance_root)
        deadline = time.monotonic() + args.max_wall_seconds
        client = PublicHttpClient(args.timeout_seconds)
        api_request_url = api_url(repository, args.revision)
        response, api_http = client.open(api_request_url, api_only=True)
        try:
            api_payload = read_response_bounded(response, MAX_API_BYTES, deadline=deadline)
        finally:
            response.close()
        api_object = strict_json_bytes(api_payload, label="model API response")
        api_files = api_model_files(
            api_object,
            repository=repository,
            revision=args.revision,
            expected_license=expected_license,
            max_files=args.max_files,
            max_file_bytes=args.max_file_bytes,
            max_total_bytes=args.max_total_bytes,
        )
        paths = {entry["path"] for entry in api_files}
        if model_card_path not in paths:
            fail("model card path is absent from the exact commit")
        if license_path not in paths:
            fail("license path is absent from the exact commit")
        write_exclusive_file(provenance_root / "api-response.json", api_payload)
        admitted_files: list[dict[str, Any]] = []
        for api_entry in api_files:
            if time.monotonic() > deadline:
                fail("network admission exceeded the wall-clock bound")
            relative = api_entry["path"]
            ensure_snapshot_parent(snapshot_root, relative)
            destination = snapshot_root / relative
            url = file_url(repository, args.revision, relative)
            response, http = client.open(url, api_only=False)
            try:
                ensure_repo_commit_headers(http, args.revision)
                expected_size = api_entry["upstream"]["byte_count"]
                check_content_length(http, expected_size)
                byte_count, digest, git_blob = stream_download(
                    response,
                    destination,
                    expected_size=expected_size,
                    expected_upstream=api_entry["upstream"],
                    max_file_bytes=args.max_file_bytes,
                    deadline=deadline,
                )
            finally:
                response.close()
            admitted_files.append(
                {
                    "api_ordinal": api_entry["api_ordinal"],
                    "path": relative,
                    "byte_count": byte_count,
                    "sha256": digest,
                    "git_blob_sha1": git_blob,
                    "upstream": api_entry["upstream"],
                    "http": http,
                }
            )
        admitted_files.sort(key=lambda item: item["path"])
        by_path = {entry["path"]: entry for entry in admitted_files}
        document_roles = (
            (("model_card_and_license_declaration", model_card_path),)
            if same_license_evidence
            else (("model_card", model_card_path), ("license_document", license_path))
        )
        documents = [
            {
                "role": role,
                "path": path,
                "byte_count": by_path[path]["byte_count"],
                "sha256": by_path[path]["sha256"],
            }
            for role, path in document_roles
        ]
        authority_policy = {
            "catalogue_authority": "none",
            "identity_authority": "none",
            "publication_authority": "none",
            "inference_authority": "none",
            "training_performed": False,
        }
        manifest_identity_basis = {
            "kind": "himr_hf_model_snapshot_manifest",
            "schema_version": SCHEMA_VERSION,
            "admitted_at": admitted_at,
            "repository": repository,
            "revision": args.revision,
            "expected_license": expected_license,
            "api_declared_license": api_object["cardData"]["license"],
            "license_evidence": {
                "kind": (
                    "api_and_model_card_metadata_declaration"
                    if same_license_evidence
                    else "api_declaration_and_standalone_repository_file"
                ),
                "api_card_data_verified": True,
                "model_card_front_matter_verified": same_license_evidence,
                "standalone_license_file_present": not same_license_evidence,
            },
            "model_card_path": model_card_path,
            "license_path": license_path,
            "bundle_root": str(args.output),
            "bundle_layout": {
                "snapshot": SNAPSHOT_DIRECTORY,
                "api_capture": API_CAPTURE_PATH,
                "manifest": MANIFEST_NAME,
                "receipt": RECEIPT_NAME,
            },
            "api_capture": {
                "path": API_CAPTURE_PATH,
                "byte_count": len(api_payload),
                "sha256": sha256_bytes(api_payload),
                "http": api_http,
            },
            "files": admitted_files,
            "required_documents": documents,
            "implementation": implementation_descriptor(),
            "limits": {
                "max_files": args.max_files,
                "max_file_bytes": args.max_file_bytes,
                "max_total_bytes": args.max_total_bytes,
                "timeout_seconds": args.timeout_seconds,
                "max_wall_seconds": args.max_wall_seconds,
                "max_api_bytes": MAX_API_BYTES,
            },
            "network_policy": {
                "access": "public_unauthenticated_https",
                "authorization": False,
                "cookies": False,
                "proxies": False,
                "telemetry": False,
                "cache": False,
                "api_base": API_BASE,
                "full_commit_required": True,
                "complete_sibling_set_required": True,
            },
            "authority_policy": authority_policy,
        }
        manifest = {
            **manifest_identity_basis,
            "identity_sha256": sha256_bytes(canonical_bytes(manifest_identity_basis)),
        }
        manifest_payload = canonical_bytes(manifest)
        write_exclusive_file(staging / MANIFEST_NAME, manifest_payload)
        files_digest_basis = [
            {"path": entry["path"], "byte_count": entry["byte_count"], "sha256": entry["sha256"]}
            for entry in admitted_files
        ]
        receipt_identity_basis = {
            "kind": "himr_hf_model_admission_receipt",
            "schema_version": SCHEMA_VERSION,
            "receipt_id": f"hfmodelreceipt_{manifest['identity_sha256'][:32]}",
            "admitted_at": admitted_at,
            "bundle_root": str(args.output),
            "manifest": {
                "path": MANIFEST_NAME,
                "byte_count": len(manifest_payload),
                "sha256": sha256_bytes(manifest_payload),
                "identity_sha256": manifest["identity_sha256"],
            },
            "snapshot": {
                "directory": SNAPSHOT_DIRECTORY,
                "file_count": len(admitted_files),
                "total_bytes": sum(entry["byte_count"] for entry in admitted_files),
                "files_digest": sha256_bytes(canonical_bytes(files_digest_basis)),
            },
            "commit": {
                "method": "linux_renameat2_rename_noreplace",
                "replacement_allowed": False,
                "sealed_file_mode": "0400",
                "sealed_directory_mode": "0500",
            },
            "authority_policy": authority_policy,
        }
        receipt = {
            **receipt_identity_basis,
            "identity_sha256": sha256_bytes(canonical_bytes(receipt_identity_basis)),
        }
        write_exclusive_file(staging / RECEIPT_NAME, canonical_bytes(receipt))
        seal_tree(staging)
        validate_bundle(staging, logical_root=args.output)
        rename_noreplace(parent_fd, staging_name, args.output.name)
        staging = None
        return validate_bundle(args.output)
    finally:
        if staging is not None:
            remove_owned_staging(staging, parent=parent, expected_prefix=prefix)
        os.close(parent_fd)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    subparsers = parser.add_subparsers(dest="command", required=True)
    download = subparsers.add_parser("download", help="download and atomically admit one exact public commit")
    download.add_argument("--repository", required=True)
    download.add_argument("--revision", required=True)
    download.add_argument("--expected-license", required=True)
    download.add_argument("--model-card-path", default="README.md")
    download.add_argument(
        "--license-path",
        help=(
            "standalone repository license file; omit only when the model card "
            "front matter itself carries the API-matching license declaration"
        ),
    )
    download.add_argument("--admitted-at", required=True)
    download.add_argument("--output", required=True, type=Path)
    download.add_argument("--max-files", required=True, type=int)
    download.add_argument("--max-file-bytes", required=True, type=int)
    download.add_argument("--max-total-bytes", required=True, type=int)
    download.add_argument("--timeout-seconds", required=True, type=int)
    download.add_argument("--max-wall-seconds", required=True, type=int)
    validate = subparsers.add_parser("validate", help="strictly validate one admitted bundle offline")
    validate.add_argument("--bundle", required=True, type=Path)
    replay = subparsers.add_parser("replay", help="validate offline and replay the exact canonical receipt")
    replay.add_argument("--bundle", required=True, type=Path)
    return parser


def main(argv: Iterable[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(list(argv) if argv is not None else None)
    try:
        if args.command == "download":
            summary = download_command(args)
            print(json.dumps(summary, sort_keys=True, indent=2))
        elif args.command == "validate":
            summary = validate_bundle(args.bundle)
            print(json.dumps(summary, sort_keys=True, indent=2))
        else:
            validate_bundle(args.bundle)
            payload = read_bounded(args.bundle / RECEIPT_NAME, MAX_MANIFEST_BYTES, label="receipt")
            sys.stdout.buffer.write(payload)
        return 0
    except AdmissionError as exc:
        error = {
            "status": "rejected",
            "error_type": "admission_error",
            "message": str(exc),
        }
        print(json.dumps(error, sort_keys=True), file=sys.stderr)
        return 2
    except KeyboardInterrupt:
        print(
            json.dumps(
                {
                    "status": "rejected",
                    "error_type": "interrupted",
                    "message": "operation interrupted",
                },
                sort_keys=True,
            ),
            file=sys.stderr,
        )
        return 130
    except Exception as exc:
        print(
            json.dumps(
                {
                    "status": "rejected",
                    "error_type": "unexpected_error",
                    "message": f"unexpected internal failure ({type(exc).__name__})",
                },
                sort_keys=True,
            ),
            file=sys.stderr,
        )
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
