"""Small, bounded Salad HTTP client; importing it performs no network work.

The managed APIs do not document idempotent submission.  This client never
retries requests.  An ambiguous mutation must be reconciled by the caller.
The module-level ``_transport`` is patchable for offline tests.

Reviewed 2026-09-06 against Salad's transcribe/transcription-lite OpenAPI and
https://docs.salad.com/reference/s4/upload-a-file . Signed result URLs are
documented in the official transcription Python SDK code examples.
"""

from __future__ import annotations

import datetime as dt
import email.utils
import hashlib
import http.client
import json
import math
import os
import re
import secrets
import stat
import urllib.error
import urllib.parse
import urllib.request
import uuid
from pathlib import Path
from typing import Any


MAX_RESPONSE_BYTES = 64 * 1024 * 1024
MAX_UPLOAD_BYTES = 100_000_000
API_ORIGIN = "https://api.salad.com/api/public"
STORAGE_ORIGIN = "https://storage-api.salad.com"
ENGINES = frozenset({"transcribe", "transcription-lite"})
_ORGANIZATION = re.compile(r"[a-z][a-z0-9-]{0,61}[a-z0-9]\Z")
_OBJECT_SEGMENT = re.compile(r"[A-Za-z0-9][A-Za-z0-9._-]{0,199}\Z")


class CloudClientError(RuntimeError):
    """A deliberately sanitized error; never includes credentials or URLs."""

    def __init__(
        self,
        message: str,
        *,
        status_code: int | None = None,
        retry_after_seconds: float | None = None,
        ambiguous: bool = False,
    ) -> None:
        super().__init__(message)
        self.status_code = status_code
        self.retry_after_seconds = retry_after_seconds
        self.ambiguous = ambiguous


class _NoRedirect(urllib.request.HTTPRedirectHandler):
    def redirect_request(self, req, fp, code, msg, headers, newurl):
        return None


def _transport(request: urllib.request.Request, timeout_seconds: float):
    # Disable environment proxy discovery, so an API credential goes only to
    # the named TLS origin. HTTPSHandler uses the system's verified TLS context.
    opener = urllib.request.build_opener(urllib.request.ProxyHandler({}), _NoRedirect())
    return opener.open(request, timeout=timeout_seconds)


def _retry_after(headers: Any) -> float | None:
    value = headers.get("Retry-After") if headers is not None else None
    if not isinstance(value, str):
        return None
    try:
        seconds = float(value)
    except (ValueError, OverflowError):
        try:
            deadline = email.utils.parsedate_to_datetime(value)
            if deadline.tzinfo is None:
                return None
            seconds = (deadline - dt.datetime.now(dt.timezone.utc)).total_seconds()
        except (ValueError, OverflowError, TypeError):
            return None
    return max(0.0, seconds) if math.isfinite(seconds) else None


def _json_object(raw: bytes) -> dict:
    def pairs(items):
        result = {}
        for key, value in items:
            if key in result:
                raise ValueError("duplicate JSON key")
            result[key] = value
        return result

    def constant(_value):
        raise ValueError("nonfinite JSON number")

    def finite_float(value):
        result = float(value)
        if not math.isfinite(result):
            raise ValueError("nonfinite JSON number")
        return result

    document = json.loads(
        raw.decode("utf-8"),
        object_pairs_hook=pairs,
        parse_constant=constant,
        parse_float=finite_float,
    )
    if not isinstance(document, dict):
        raise ValueError("expected JSON object")
    return document


def _object_name(value: str) -> str:
    if (
        not isinstance(value, str)
        or len(value) > 1024
        or not value
        or any(_OBJECT_SEGMENT.fullmatch(part) is None for part in value.split("/"))
    ):
        raise CloudClientError("invalid storage object name")
    return value


def _storage_url(value: str, *, expected_path: str | None = None) -> str:
    if (
        not isinstance(value, str)
        or len(value) > 8192
        or any(ord(character) < 33 or ord(character) > 126 for character in value)
        or "\\" in value
    ):
        raise CloudClientError("invalid signed storage URL")
    try:
        parsed = urllib.parse.urlsplit(value)
        if (
            parsed.scheme != "https"
            or parsed.netloc != "storage-api.salad.com"
            or parsed.fragment
            or not parsed.path.startswith("/organizations/")
        ):
            raise ValueError("origin or path")
        parts = parsed.path.split("/", 4)
        if len(parts) != 5 or _ORGANIZATION.fullmatch(parts[2]) is None or parts[3] != "files":
            raise ValueError("storage path")
        _object_name(parts[4])
        if expected_path is not None and parsed.path != expected_path:
            raise ValueError("unexpected object")
        query = urllib.parse.parse_qs(parsed.query, keep_blank_values=True, strict_parsing=True)
        if set(query) != {"token"} or len(query["token"]) != 1 or not query["token"][0]:
            raise ValueError("missing or invalid token")
        if any(ord(character) < 33 or ord(character) > 126 for character in query["token"][0]):
            raise ValueError("invalid token characters")
    except (ValueError, CloudClientError):
        raise CloudClientError("invalid signed storage URL") from None
    return value


def _file_bytes(path: Path) -> bytes:
    """Read one stable, owned file without following any symlink component."""
    descriptor = None
    directory = None
    try:
        absolute = Path(path).absolute()
        if ".." in absolute.parts:
            raise ValueError("parent traversal")
        directory = os.open(absolute.anchor, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW)
        for part in absolute.parts[1:-1]:
            child = os.open(part, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW, dir_fd=directory)
            os.close(directory)
            directory = child
        descriptor = os.open(
            absolute.name,
            os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK,
            dir_fd=directory,
        )
        before = os.fstat(descriptor)
        if (
            not stat.S_ISREG(before.st_mode)
            or before.st_uid != os.getuid()
            or before.st_nlink != 1
            or before.st_mode & 0o022
            or not 0 < before.st_size <= MAX_UPLOAD_BYTES
        ):
            raise ValueError("unsafe upload file")
        with os.fdopen(descriptor, "rb") as handle:
            descriptor = None
            contents = handle.read(MAX_UPLOAD_BYTES + 1)
            after = os.fstat(handle.fileno())
        witness = lambda item: (
            item.st_dev, item.st_ino, item.st_size, item.st_mtime_ns,
            item.st_ctime_ns, item.st_mode, item.st_uid, item.st_nlink,
        )
        if len(contents) != before.st_size or witness(before) != witness(after):
            raise ValueError("upload file changed")
        return contents
    except (OSError, ValueError, TypeError):
        raise CloudClientError("upload requires a stable owned regular file of at most 100 MB") from None
    finally:
        if descriptor is not None:
            os.close(descriptor)
        if directory is not None:
            os.close(directory)


class SaladClient:
    def __init__(
        self,
        organization: str,
        api_key: str | None = None,
        timeout_seconds: float = 60,
    ) -> None:
        if not isinstance(organization, str) or _ORGANIZATION.fullmatch(organization) is None:
            raise CloudClientError("invalid Salad organization")
        key = os.environ.get("SALAD_API_KEY") if api_key is None else api_key
        if (
            not isinstance(key, str)
            or not 1 <= len(key) <= 4096
            or any(ord(character) < 33 or ord(character) > 126 for character in key)
        ):
            raise CloudClientError("SALAD_API_KEY is missing or invalid")
        if (
            isinstance(timeout_seconds, bool)
            or not isinstance(timeout_seconds, (float, int))
            or not math.isfinite(timeout_seconds)
            or not 0 < timeout_seconds <= 300
        ):
            raise CloudClientError("timeout must be finite and between zero and 300 seconds")
        self.organization = organization
        self._api_key = key
        self.timeout_seconds = float(timeout_seconds)

    def _endpoint_url(self, engine: str) -> str:
        if not isinstance(engine, str) or engine not in ENGINES:
            raise CloudClientError("unsupported transcription engine")
        return f"{API_ORIGIN}/organizations/{self.organization}/inference-endpoints/{engine}"

    def _job_url(self, engine: str, job_id: str) -> str:
        try:
            if not isinstance(job_id, str) or str(uuid.UUID(job_id)) != job_id:
                raise ValueError("noncanonical UUID")
        except (ValueError, AttributeError):
            raise CloudClientError("invalid transcription job ID") from None
        return f"{self._endpoint_url(engine)}/jobs/{job_id}"

    def _request(
        self,
        method: str,
        url: str,
        *,
        data: bytes | None = None,
        content_type: str | None = None,
        authenticated: bool = True,
        expected_status: int = 200,
        empty_response: bool = False,
    ) -> dict | None:
        mutating = method in {"POST", "PUT", "DELETE"}
        headers = {"Accept": "application/json", "Accept-Encoding": "identity"}
        if authenticated:
            # Defense in depth even though every authenticated caller constructs
            # its URL from fixed origins and validated path components.
            destination = urllib.parse.urlsplit(url)
            if destination.scheme != "https" or destination.netloc not in {"api.salad.com", "storage-api.salad.com"} or destination.fragment:
                raise CloudClientError("authentication origin rejected")
            headers["Salad-Api-Key"] = self._api_key
        if content_type:
            headers["Content-Type"] = content_type
        request = urllib.request.Request(url, data=data, headers=headers, method=method)
        status_code = None
        try:
            with _transport(request, self.timeout_seconds) as response:
                status_code = response.status
                if status_code != expected_status:
                    raise CloudClientError(
                        "Salad request returned an unexpected HTTP status",
                        status_code=status_code,
                        retry_after_seconds=_retry_after(response.headers),
                        ambiguous=mutating and (status_code == 408 or not 400 <= status_code < 500),
                    )
                if empty_response:
                    return None
                content_length = response.headers.get("Content-Length")
                if content_length is not None:
                    try:
                        declared_size = int(content_length)
                    except (ValueError, TypeError):
                        raise CloudClientError("invalid cloud response length", ambiguous=mutating) from None
                    if not 0 <= declared_size <= MAX_RESPONSE_BYTES:
                        raise CloudClientError("cloud response exceeds size limit", ambiguous=mutating)
                raw = response.read(MAX_RESPONSE_BYTES + 1)
                if len(raw) > MAX_RESPONSE_BYTES:
                    raise CloudClientError("cloud response exceeds size limit", ambiguous=mutating)
                if content_length is not None and len(raw) != declared_size:
                    raise CloudClientError("cloud response length does not match", ambiguous=mutating)
                try:
                    return _json_object(raw)
                except (ValueError, UnicodeError, RecursionError):
                    raise CloudClientError(
                        "cloud response is not a strict JSON object",
                        status_code=status_code,
                        ambiguous=mutating,
                    ) from None
        except urllib.error.HTTPError as error:
            code = error.code
            retry_after = _retry_after(error.headers)
            error.close()
            raise CloudClientError(
                "Salad request failed with an HTTP status",
                status_code=code,
                retry_after_seconds=retry_after,
                ambiguous=mutating and (code == 408 or not 400 <= code < 500),
            ) from None
        except CloudClientError:
            raise
        except (OSError, ValueError, urllib.error.URLError, http.client.HTTPException):
            raise CloudClientError(
                "cloud request transport failed",
                status_code=status_code,
                ambiguous=mutating,
            ) from None

    def endpoint(self, engine: str) -> dict:
        return self._request("GET", self._endpoint_url(engine))

    def submit(self, engine: str, payload: dict) -> dict:
        url = f"{self._endpoint_url(engine)}/jobs"
        try:
            if not isinstance(payload, dict):
                raise ValueError("expected object")
            body = json.dumps(payload, allow_nan=False, separators=(",", ":")).encode("utf-8")
        except (ValueError, TypeError, UnicodeError, RecursionError):
            raise CloudClientError("invalid transcription request JSON") from None
        if len(body) > 1024 * 1024:
            raise CloudClientError("transcription request exceeds size limit")
        return self._request("POST", url, data=body, content_type="application/json", expected_status=201)

    def get_job(self, engine: str, job_id: str) -> dict:
        return self._request("GET", self._job_url(engine, job_id))

    def cancel_job(self, engine: str, job_id: str) -> dict | None:
        return self._request("DELETE", self._job_url(engine, job_id), expected_status=202, empty_response=True)

    def upload_file(
        self, path: Path, object_name: str, *, expires_seconds: int = 86400,
        expected_sha256: str | None = None, expected_byte_count: int | None = None,
    ) -> str:
        name = _object_name(object_name)
        if isinstance(expires_seconds, bool) or not isinstance(expires_seconds, int) or not 1 <= expires_seconds <= 30 * 86400:
            raise CloudClientError("invalid signed storage URL lifetime")
        if expected_sha256 is not None and (
            not isinstance(expected_sha256, str) or re.fullmatch(r"[0-9a-f]{64}", expected_sha256) is None
        ):
            raise CloudClientError("invalid expected upload digest")
        if expected_byte_count is not None and (
            isinstance(expected_byte_count, bool) or not isinstance(expected_byte_count, int)
            or not 0 < expected_byte_count <= MAX_UPLOAD_BYTES
        ):
            raise CloudClientError("invalid expected upload byte count")
        contents = _file_bytes(path)
        if expected_sha256 is not None and hashlib.sha256(contents).hexdigest() != expected_sha256:
            raise CloudClientError("upload bytes differ from their expected digest")
        if expected_byte_count is not None and len(contents) != expected_byte_count:
            raise CloudClientError("upload bytes differ from their expected byte count")
        boundary = "himr-salad-" + secrets.token_hex(24)
        while boundary.encode("ascii") in contents:
            boundary = "himr-salad-" + secrets.token_hex(24)
        fields = {"mimeType": "audio/wav", "sign": "true", "signatureExp": str(expires_seconds)}
        chunks = []
        for key, value in fields.items():
            chunks.append(
                f'--{boundary}\r\nContent-Disposition: form-data; name="{key}"\r\n\r\n{value}\r\n'.encode("ascii")
            )
        chunks.extend([
            f'--{boundary}\r\nContent-Disposition: form-data; name="file"; filename="{name.rsplit("/", 1)[-1]}"\r\nContent-Type: audio/wav\r\n\r\n'.encode("ascii"),
            contents,
            f"\r\n--{boundary}--\r\n".encode("ascii"),
        ])
        object_path = f"/organizations/{self.organization}/files/{name}"
        result = self._request(
            "PUT", f"{STORAGE_ORIGIN}{object_path}", data=b"".join(chunks),
            content_type=f"multipart/form-data; boundary={boundary}",
        )
        try:
            return _storage_url(result.get("url"), expected_path=object_path)
        except CloudClientError:
            raise CloudClientError("upload returned an invalid signed storage URL", ambiguous=True) from None

    def download_output(self, url: str) -> dict:
        return self._request("GET", _storage_url(url), authenticated=False)
