"""Bounded, dependency-free batch transports; imports do no network work.

The caller owns consent, spend authorization, durable submission journals, and
reconciliation. There are deliberately no retries, automatic cleanup, credential
discovery, redirects, or proxy-environment discovery. A mutation with an unknown
outcome must be reconciled before another submission.

Reviewed 2026-09-12 against official OpenAI documentation:
https://developers.openai.com/api/docs/guides/batch
https://developers.openai.com/api/reference/typescript/resources/files/methods/create
https://developers.openai.com/api/reference/typescript/resources/batches/methods/create
https://ai.google.dev/api/batch-api
https://ai.google.dev/gemini-api/docs/batch-api
"""

from __future__ import annotations

import datetime as dt
import email.utils
import http.client
import json
import math
import re
import secrets
import time
import urllib.error
import urllib.parse
import urllib.request
from typing import Any, Callable


API_ORIGIN = "https://api.openai.com"
MAX_RESPONSE_BYTES = 64 * 1024 * 1024
MAX_JSON_BYTES = 4 * 1024 * 1024
MAX_UPLOAD_BYTES = 64 * 1024 * 1024  # Deliberately below the provider's 200 MB.
MAX_BATCH_REQUESTS = 50_000
FILE_RETENTION_SECONDS = 7 * 24 * 3600
MAX_GEMINI_INLINE_BYTES = 16 * 1024 * 1024
GEMINI_CREATE_TIMEOUT_SECONDS = 180
BATCH_STATUSES = frozenset({
    "validating", "failed", "in_progress", "finalizing", "completed",
    "expired", "cancelling", "cancelled",
})
_FILE_ID = re.compile(r"file-[A-Za-z0-9_-]{1,128}\Z")
_BATCH_ID = re.compile(r"batch_[A-Za-z0-9_-]{1,128}\Z")
_CUSTOM_ID = re.compile(r"[A-Za-z0-9_-]{1,256}\Z")


class OpenAIClientError(RuntimeError):
    """Sanitized exception: no remote error body, credential, or user content."""

    def __init__(self, message: str, *, status_code: int | None = None,
                 ambiguous: bool = False,
                 retry_after_seconds: float | None = None) -> None:
        super().__init__(message)
        self.status_code = status_code
        self.ambiguous = ambiguous
        self.retry_after_seconds = retry_after_seconds


class _NoRedirect(urllib.request.HTTPRedirectHandler):
    def redirect_request(self, req, fp, code, msg, headers, newurl):
        return None


def _validated_timeout(value: float) -> float:
    if (isinstance(value, bool) or not isinstance(value, (int, float))
            or not 0 < value <= 300 or not math.isfinite(value)):
        raise OpenAIClientError("timeout must be finite and between zero and 300 seconds")
    return float(value)


def _transport(request: urllib.request.Request, timeout_seconds: float):
    # urllib's HTTPS handler uses certificate and hostname verification. Do not
    # let ambient proxy variables receive this separately supplied credential.
    opener = urllib.request.build_opener(
        urllib.request.ProxyHandler({}), _NoRedirect(),
    )
    return opener.open(request, timeout=timeout_seconds)


def _retry_after(headers: Any) -> float | None:
    value = headers.get("Retry-After") if headers is not None else None
    if not isinstance(value, str) or len(value) > 128:
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
                raise ValueError("duplicate key")
            result[key] = value
        return result

    def constant(_value):
        raise ValueError("nonfinite number")

    def finite_float(value):
        result = float(value)
        if not math.isfinite(result):
            raise ValueError("nonfinite number")
        return result

    value = json.loads(raw.decode("utf-8"), object_pairs_hook=pairs,
                       parse_constant=constant, parse_float=finite_float)
    if not isinstance(value, dict):
        raise ValueError("expected object")
    return value


def _id(value: Any, *, batch: bool = False) -> str:
    pattern = _BATCH_ID if batch else _FILE_ID
    if not isinstance(value, str) or pattern.fullmatch(value) is None:
        raise OpenAIClientError("invalid OpenAI batch ID" if batch else "invalid OpenAI file ID")
    return value


def _integer(value: Any, maximum: int = 2**63 - 1) -> bool:
    return type(value) is int and 0 <= value <= maximum


def _metadata(value: Any) -> dict:
    if not isinstance(value, dict) or len(value) > 16:
        raise OpenAIClientError("invalid batch metadata")
    for key, item in value.items():
        if (not isinstance(key, str) or not 1 <= len(key) <= 64
                or not isinstance(item, str) or len(item) > 512
                or any(ord(char) < 32 or 0xD800 <= ord(char) <= 0xDFFF
                       for char in key + item)):
            raise OpenAIClientError("invalid batch metadata")
    return dict(value)


def _file(value: dict, *, expected_id: str | None = None,
          uploaded_bytes: int | None = None) -> dict:
    _id(value.get("id"))
    if (value.get("object") != "file"
            or not _integer(value.get("bytes"), 512 * 1024 * 1024)
            or not _integer(value.get("created_at"))
            or not isinstance(value.get("filename"), str)
            or not 1 <= len(value["filename"]) <= 1024
            or value.get("purpose") not in {"batch", "batch_output"}
            or (expected_id is not None and value["id"] != expected_id)
            or (uploaded_bytes is not None and
                (value["bytes"] != uploaded_bytes or value["purpose"] != "batch"))
            or ("expires_at" in value and not _integer(value["expires_at"]))):
        raise OpenAIClientError("invalid OpenAI file response")
    return value


def _batch(value: dict, *, expected_id: str | None = None,
           input_file_id: str | None = None, metadata: dict | None = None) -> dict:
    _id(value.get("id"), batch=True)
    _id(value.get("input_file_id"))
    if (value.get("object") != "batch"
            or not isinstance(value.get("status"), str)
            or value["status"] not in BATCH_STATUSES
            or value.get("completion_window") != "24h"
            or not _integer(value.get("created_at"))
            or not isinstance(value.get("endpoint"), str)
            or re.fullmatch(r"/v1/[a-z]+(?:/[a-z]+)?", value["endpoint"]) is None
            or (expected_id is not None and value["id"] != expected_id)
            or (input_file_id is not None and
                (value["input_file_id"] != input_file_id
                 or value["endpoint"] != "/v1/responses"))):
        raise OpenAIClientError("invalid OpenAI batch response")
    actual_metadata = value.get("metadata")
    if actual_metadata is not None:
        _metadata(actual_metadata)
    if metadata is not None and actual_metadata != metadata:
        raise OpenAIClientError("OpenAI batch metadata does not match submission")
    for key in ("output_file_id", "error_file_id"):
        if value.get(key) is not None:
            _id(value[key])
    counts = value.get("request_counts")
    if counts is not None and (
        not isinstance(counts, dict)
        or any(not _integer(counts.get(key), MAX_BATCH_REQUESTS)
               for key in ("total", "completed", "failed"))
        or counts["completed"] + counts["failed"] > counts["total"]
    ):
        raise OpenAIClientError("invalid OpenAI batch request counts")
    return value


class OpenAIBatchClient:
    """Only construction from an explicit key; transport injection is for tests."""

    provider_name = "OpenAI"

    def __init__(self, api_key: str, timeout_seconds: float = 60,
                 transport: Callable | None = None) -> None:
        if (not isinstance(api_key, str) or not 1 <= len(api_key) <= 4096
                or any(ord(char) < 33 or ord(char) > 126 for char in api_key)):
            raise OpenAIClientError(f"{self.provider_name} API key is missing or invalid")
        timeout_seconds = _validated_timeout(timeout_seconds)
        if transport is not None and not callable(transport):
            raise OpenAIClientError("invalid HTTP transport")
        self._api_key = api_key
        self.timeout_seconds = timeout_seconds
        self._transport = transport

    def _destination(self, method: str, path: str) -> str:
        if (method not in {"GET", "POST", "DELETE"}
                or not isinstance(path, str)
                or re.fullmatch(r"/v1/(?:files|batches)(?:/[A-Za-z0-9_-]+(?:/content)?)?"
                                r"(?:\?limit=[0-9]+(?:&after=batch_[A-Za-z0-9_-]+)?)?", path) is None):
            raise OpenAIClientError("OpenAI request destination rejected")
        url = API_ORIGIN + path
        parsed = urllib.parse.urlsplit(url)
        if parsed.scheme != "https" or parsed.netloc != "api.openai.com" or parsed.fragment:
            raise OpenAIClientError("OpenAI authentication origin rejected")
        return url

    def _headers(self, raw: bool) -> dict:
        return {"Authorization": "Bearer " + self._api_key,
                "Accept": "application/octet-stream" if raw else "application/json",
                "Accept-Encoding": "identity"}

    def _response_limit(self, raw: bool) -> int:
        return MAX_RESPONSE_BYTES if raw else MAX_JSON_BYTES

    def _request(self, method: str, path: str, *, data: bytes | None = None,
                 content_type: str | None = None, raw: bool = False,
                 timeout_seconds: float | None = None) -> dict | bytes:
        # Keep overrides local: a slow create must not change concurrent GETs or
        # later calls, including when transport or response validation fails.
        request_timeout = _validated_timeout(
            self.timeout_seconds if timeout_seconds is None else timeout_seconds)
        url = self._destination(method, path)
        provider = self.provider_name
        mutation = method in {"POST", "DELETE"}
        headers = self._headers(raw)
        if content_type:
            headers["Content-Type"] = content_type
        request = urllib.request.Request(url, data=data, headers=headers, method=method)
        status = None
        maximum = self._response_limit(raw)
        deadline = time.monotonic() + request_timeout
        try:
            with (self._transport or _transport)(request, request_timeout) as response:
                status = response.status
                if type(status) is not int or status != 200:
                    raise OpenAIClientError(
                        f"{provider} request returned an unexpected HTTP status",
                        status_code=status if type(status) is int else None,
                        ambiguous=mutation and (type(status) is not int or status == 408
                                                or not 400 <= status < 500),
                        retry_after_seconds=_retry_after(response.headers),
                    )
                # Defense in depth for custom/alternative transport adapters.
                if hasattr(response, "geturl") and response.geturl() != url:
                    raise OpenAIClientError(f"{provider} response destination changed", ambiguous=mutation)
                if response.headers.get("Content-Encoding", "identity").lower() != "identity":
                    raise OpenAIClientError(f"encoded {provider} response rejected", ambiguous=mutation)
                length = response.headers.get("Content-Length")
                if length is not None and (not isinstance(length, str)
                        or re.fullmatch(r"[0-9]{1,12}", length) is None
                        or int(length) > maximum):
                    raise OpenAIClientError(f"invalid or oversized {provider} response length", ambiguous=mutation)
                parts, size = [], 0
                reader = getattr(response, "read1", response.read)
                while True:
                    if time.monotonic() >= deadline:
                        raise OpenAIClientError(f"{provider} response deadline exceeded", ambiguous=mutation)
                    part = reader(min(65536, maximum + 1 - size))
                    if not isinstance(part, bytes):
                        raise OpenAIClientError(f"invalid {provider} response bytes", ambiguous=mutation)
                    if not part:
                        break
                    size += len(part)
                    if size > maximum:
                        raise OpenAIClientError(f"{provider} response exceeds size limit", ambiguous=mutation)
                    parts.append(part)
                if length is not None and size != int(length):
                    raise OpenAIClientError(f"{provider} response length does not match", ambiguous=mutation)
                contents = b"".join(parts)
                if raw:
                    return contents
                try:
                    return _json_object(contents)
                except (ValueError, UnicodeError, RecursionError):
                    raise OpenAIClientError(f"{provider} response is not a strict JSON object",
                                            status_code=status, ambiguous=mutation) from None
        except urllib.error.HTTPError as error:
            code, retry_after = error.code, _retry_after(error.headers)
            error.close()  # Do not read or expose the error body.
            raise OpenAIClientError(f"{provider} request failed with an HTTP status",
                                    status_code=code, retry_after_seconds=retry_after,
                                    ambiguous=mutation and (code == 408 or not 400 <= code < 500)) from None
        except OpenAIClientError:
            raise
        except (OSError, ValueError, urllib.error.URLError, http.client.HTTPException):
            raise OpenAIClientError(f"{provider} request transport failed", status_code=status,
                                    ambiguous=mutation) from None

    @staticmethod
    def _validated(value: dict, validator: Callable, *, mutation: bool = False,
                   **kwargs) -> dict:
        try:
            return validator(value, **kwargs)
        except (OpenAIClientError, ValueError, TypeError, KeyError):
            raise OpenAIClientError("OpenAI response failed validation", status_code=200,
                                    ambiguous=mutation) from None

    def upload_batch(self, data: bytes) -> dict:
        if not isinstance(data, bytes) or not 0 < len(data) <= MAX_UPLOAD_BYTES:
            raise OpenAIClientError("batch upload requires bounded nonempty bytes")
        try:
            if not data.endswith(b"\n"):
                raise ValueError("missing final newline")
            rows = data.splitlines()
            if not 1 <= len(rows) <= MAX_BATCH_REQUESTS:
                raise ValueError("invalid request count")
            seen = set()
            for row in rows:
                request = _json_object(row)
                identifier = request.get("custom_id")
                if (set(request) != {"custom_id", "method", "url", "body"}
                        or not isinstance(identifier, str)
                        or _CUSTOM_ID.fullmatch(identifier) is None or identifier in seen
                        or request.get("method") != "POST" or request.get("url") != "/v1/responses"
                        or not isinstance(request.get("body"), dict)):
                    raise ValueError("invalid batch row")
                seen.add(identifier)
        except (ValueError, UnicodeError, RecursionError, TypeError):
            raise OpenAIClientError("batch upload is not valid bounded Responses JSONL") from None
        boundary = "himr-openai-" + secrets.token_hex(24)
        while boundary.encode("ascii") in data:
            boundary = "himr-openai-" + secrets.token_hex(24)
        parts = []
        for name, value in (("purpose", "batch"), ("expires_after[anchor]", "created_at"),
                            ("expires_after[seconds]", str(FILE_RETENTION_SECONDS))):
            parts.append(f'--{boundary}\r\nContent-Disposition: form-data; name="{name}"\r\n\r\n{value}\r\n'.encode())
        parts.extend([
            f'--{boundary}\r\nContent-Disposition: form-data; name="file"; filename="himr-summaries.jsonl"\r\n'
            'Content-Type: application/jsonl\r\n\r\n'.encode(),
            data, f"\r\n--{boundary}--\r\n".encode(),
        ])
        value = self._request("POST", "/v1/files", data=b"".join(parts),
                              content_type=f"multipart/form-data; boundary={boundary}")
        return self._validated(value, _file, mutation=True, uploaded_bytes=len(data))

    def create_batch(self, file_id: str, metadata: dict) -> dict:
        file_id, metadata = _id(file_id), _metadata(metadata)
        body = {"input_file_id": file_id, "endpoint": "/v1/responses",
                "completion_window": "24h", "metadata": metadata,
                "output_expires_after": {"anchor": "created_at", "seconds": FILE_RETENTION_SECONDS}}
        value = self._request("POST", "/v1/batches",
                              data=json.dumps(body, ensure_ascii=True, allow_nan=False,
                                              separators=(",", ":")).encode(),
                              content_type="application/json")
        return self._validated(value, _batch, mutation=True, input_file_id=file_id, metadata=metadata)

    def get_batch(self, batch_id: str) -> dict:
        batch_id = _id(batch_id, batch=True)
        value = self._request("GET", "/v1/batches/" + batch_id)
        return self._validated(value, _batch, expected_id=batch_id)

    def list_batches(self, after: str | None = None, limit: int = 100) -> dict:
        if type(limit) is not int or not 1 <= limit <= 100:
            raise OpenAIClientError("batch page limit must be between 1 and 100")
        query = "?limit=" + str(limit)
        if after is not None:
            query += "&after=" + _id(after, batch=True)
        value = self._request("GET", "/v1/batches" + query)
        try:
            if (value.get("object") != "list" or type(value.get("has_more")) is not bool
                    or not isinstance(value.get("data"), list) or len(value["data"]) > limit):
                raise ValueError("invalid page")
            for item in value["data"]:
                if not isinstance(item, dict):
                    raise ValueError("invalid batch")
                _batch(item)
            identifiers = [item["id"] for item in value["data"]]
            if len(set(identifiers)) != len(identifiers):
                raise ValueError("duplicate batches")
            if (value.get("first_id") != (identifiers[0] if identifiers else None)
                    or value.get("last_id") != (identifiers[-1] if identifiers else None)
                    or (value["has_more"] and not identifiers)):
                raise ValueError("invalid page cursors")
        except (OpenAIClientError, ValueError, TypeError, KeyError):
            raise OpenAIClientError("invalid OpenAI batch list response", status_code=200) from None
        return value

    def get_file(self, file_id: str) -> dict:
        file_id = _id(file_id)
        value = self._request("GET", "/v1/files/" + file_id)
        return self._validated(value, _file, expected_id=file_id)

    def download_file(self, file_id: str) -> bytes:
        return self._request("GET", "/v1/files/" + _id(file_id) + "/content", raw=True)

    def delete_file(self, file_id: str) -> dict:
        """Explicit caller-only cleanup; never invoked implicitly by this client."""
        file_id = _id(file_id)
        value = self._request("DELETE", "/v1/files/" + file_id)
        if value.get("id") != file_id or value.get("object") != "file" or value.get("deleted") is not True:
            raise OpenAIClientError("OpenAI file deletion was not confirmed", status_code=200, ambiguous=True)
        return value


# Both providers expose the same sanitized transport error contract. Existing
# OpenAI callers may retain their explicit name; supervisors should use this one.
BatchClientError = OpenAIClientError
GeminiClientError = OpenAIClientError
_GEMINI_BATCH_NAME = re.compile(r"batches/[A-Za-z0-9_-]{1,256}\Z")
_GEMINI_MODEL = re.compile(r"gemini-[a-z0-9][a-z0-9.-]{0,126}\Z")


def _gemini_name(value: Any) -> str:
    if not isinstance(value, str) or _GEMINI_BATCH_NAME.fullmatch(value) is None:
        raise BatchClientError("invalid Gemini batch name")
    return value


def _gemini_operation(value: dict, *, expected_name: str | None = None) -> dict:
    """Validate the REST Operation envelope, not SDK BatchJob lookalikes.

    The supervisor must bind model/displayName/requests and interpret the
    service-specific metadata and response, which are protobuf Any objects.
    Unknown metadata never constitutes successful work or a recoverable match.
    """
    _gemini_name(value.get("name"))
    if (set(value) - {"name", "metadata", "done", "response", "error"}
            or (expected_name is not None and value["name"] != expected_name)
            or type(value.get("done", False)) is not bool
            or any(key in value and not isinstance(value[key], dict)
                   for key in ("metadata", "response", "error"))
            or ("response" in value and "error" in value)
            or (not value.get("done", False) and ("response" in value or "error" in value))):
        raise BatchClientError("invalid Gemini operation response")
    if "error" in value and not _integer(value["error"].get("code"), 16):
        raise BatchClientError("invalid Gemini operation error code")
    return value


def _text_content(value: Any) -> bool:
    if (not isinstance(value, dict) or set(value) - {"role", "parts"}
            or not isinstance(value.get("role", "user"), str)
            or value.get("role", "user") not in {"user", "model", "system"}
            or not isinstance(value.get("parts"), list) or not value["parts"]):
        return False
    return all(isinstance(part, dict) and set(part) == {"text"}
               and isinstance(part["text"], str) for part in value["parts"])


class GeminiBatchClient(OpenAIBatchClient):
    """Inline text batches only; no file-upload or cross-provider fallback path.

    create_batch returns the raw REST Operation, not a Google SDK BatchJob.
    Input requests use {key, request}; the wire envelope supplies metadata.key.
    All inline outputs are bounded to 64 MiB. There is no remote-file fallback.
    Creation uses 180 seconds; GETs use the configured timeout (default 60).
    """

    provider_name = "Gemini"

    def _destination(self, method: str, path: str) -> str:
        if method not in {"GET", "POST"} or not isinstance(path, str):
            raise BatchClientError("Gemini request destination rejected")
        url = "https://generativelanguage.googleapis.com" + path
        parsed = urllib.parse.urlsplit(url)
        if (parsed.scheme != "https" or parsed.netloc != "generativelanguage.googleapis.com"
                or parsed.fragment or any(ord(char) < 33 or ord(char) > 126 for char in path)):
            raise BatchClientError("Gemini authentication origin rejected")
        if method == "POST":
            if (parsed.query or re.fullmatch(
                    r"/v1beta/models/gemini-[a-z0-9][a-z0-9.-]{0,126}:batchGenerateContent",
                    parsed.path) is None):
                raise BatchClientError("Gemini creation destination rejected")
        elif parsed.path == "/v1beta/batches":
            try:
                query = urllib.parse.parse_qs(parsed.query, strict_parsing=True)
                if (set(query) not in ({"pageSize"}, {"pageSize", "pageToken"})
                        or any(len(items) != 1 for items in query.values())
                        or re.fullmatch(r"[0-9]{1,3}", query["pageSize"][0]) is None
                        or not 1 <= int(query["pageSize"][0]) <= 100):
                    raise ValueError("invalid query")
            except (ValueError, KeyError):
                raise BatchClientError("Gemini listing destination rejected") from None
        elif (parsed.query or not parsed.path.startswith("/v1beta/")
              or _GEMINI_BATCH_NAME.fullmatch(parsed.path[len("/v1beta/"):]) is None):
            raise BatchClientError("Gemini polling destination rejected")
        return url

    def _headers(self, raw: bool) -> dict:
        if raw:
            raise BatchClientError("Gemini raw downloads are not supported")
        return {"x-goog-api-key": self._api_key, "Accept": "application/json",
                "Accept-Encoding": "identity"}

    def _response_limit(self, raw: bool) -> int:
        return MAX_RESPONSE_BYTES  # Completed inline batches include all outputs.

    @staticmethod
    def batch_bytes(model: str, requests: list[dict], display_name: str) -> bytes:
        """Encode validated, already-projected requests without rewriting them.

        Schema compatibility belongs in the immutable job, not the transport.
        Sorted object keys keep exact wire bytes stable after JSON persistence.
        """
        if not isinstance(model, str) or _GEMINI_MODEL.fullmatch(model) is None:
            raise BatchClientError("invalid Gemini batch model")
        if (not isinstance(display_name, str) or not 1 <= len(display_name) <= 128
                or re.fullmatch(r"[A-Za-z0-9_-]+", display_name) is None):
            raise BatchClientError("invalid Gemini batch display name")
        if not isinstance(requests, list) or not 1 <= len(requests) <= MAX_BATCH_REQUESTS:
            raise BatchClientError("invalid Gemini inline batch request count")
        seen, rows = set(), []
        for item in requests:
            if (not isinstance(item, dict) or set(item) != {"key", "request"}
                    or not isinstance(item["key"], str) or _CUSTOM_ID.fullmatch(item["key"]) is None
                    or item["key"] in seen or not isinstance(item["request"], dict)):
                raise BatchClientError("invalid Gemini inline request")
            request = item["request"]
            # These controls belong to the supervisor, and a row must not route
            # to another model, add tools, retrieve files, or enable storage.
            if (set(request) - {"model", "contents", "systemInstruction", "generationConfig", "store"}
                    or request.get("model", "models/" + model) != "models/" + model
                    or request.get("store", False) is not False
                    or not isinstance(request.get("contents"), list) or not request["contents"]
                    or any(not _text_content(content) for content in request["contents"])
                    or ("systemInstruction" in request and not _text_content(request["systemInstruction"]))
                    or ("generationConfig" in request and not isinstance(request["generationConfig"], dict))):
                raise BatchClientError("Gemini inline request controls rejected")
            seen.add(item["key"])
            rows.append({"request": request, "metadata": {"key": item["key"]}})
        body = {"batch": {"display_name": display_name,
                          "input_config": {"requests": {"requests": rows}}}}
        try:
            data = json.dumps(body, sort_keys=True, ensure_ascii=True, allow_nan=False,
                              separators=(",", ":")).encode("utf-8")
        except (ValueError, TypeError, UnicodeError, RecursionError):
            raise BatchClientError("invalid Gemini inline batch JSON") from None
        if len(data) > MAX_GEMINI_INLINE_BYTES:
            raise BatchClientError("Gemini inline batch exceeds size limit")
        return data

    def create_batch(self, model: str, requests: list[dict], display_name: str) -> dict:
        data = self.batch_bytes(model, requests, display_name)
        value = self._request("POST", "/v1beta/models/" + model + ":batchGenerateContent",
                              data=data, content_type="application/json",
                              timeout_seconds=GEMINI_CREATE_TIMEOUT_SECONDS)
        try:
            return _gemini_operation(value)
        except (BatchClientError, TypeError, ValueError):
            raise BatchClientError("Gemini creation response failed validation",
                                   status_code=200, ambiguous=True) from None

    def get_batch(self, name: str) -> dict:
        name = _gemini_name(name)
        value = self._request("GET", "/v1beta/" + name)
        try:
            return _gemini_operation(value, expected_name=name)
        except (BatchClientError, TypeError, ValueError):
            raise BatchClientError("Gemini polling response failed validation", status_code=200) from None

    def list_batches(self, page_token: str | None = None, page_size: int = 100) -> dict:
        if type(page_size) is not int or not 1 <= page_size <= 100:
            raise BatchClientError("Gemini page size must be between 1 and 100")
        query = {"pageSize": str(page_size)}
        if page_token is not None:
            if (not isinstance(page_token, str) or not 1 <= len(page_token) <= 4096
                    or any(ord(char) < 33 or ord(char) > 126 for char in page_token)):
                raise BatchClientError("invalid Gemini page token")
            query["pageToken"] = page_token
        value = self._request("GET", "/v1beta/batches?" + urllib.parse.urlencode(query))
        try:
            operations = value.get("operations", [])
            token = value.get("nextPageToken", "")
            if (set(value) - {"operations", "nextPageToken", "unreachable"}
                    or not isinstance(operations, list) or len(operations) > page_size
                    or not isinstance(token, str) or len(token) > 4096
                    or any(ord(char) < 33 or ord(char) > 126 for char in token)
                    or (token and (not operations or token == page_token))
                    or value.get("unreachable")):
                raise ValueError("invalid page")
            names = set()
            for operation in operations:
                if not isinstance(operation, dict):
                    raise ValueError("invalid operation")
                _gemini_operation(operation)
                if operation["name"] in names:
                    raise ValueError("duplicate operation")
                names.add(operation["name"])
        except (BatchClientError, ValueError, TypeError, KeyError):
            raise BatchClientError("invalid Gemini batch list response", status_code=200) from None
        return value

    def upload_batch(self, data: bytes) -> dict:
        raise BatchClientError("Gemini uses inline batches; file uploads are disabled")

    def get_file(self, file_id: str) -> dict:
        raise BatchClientError("Gemini file access is disabled")

    def download_file(self, file_id: str) -> bytes:
        raise BatchClientError("Gemini file access is disabled")

    def delete_file(self, file_id: str) -> dict:
        raise BatchClientError("Gemini file deletion is disabled")


def gemini_batch_bytes(model: str, requests: list[dict], display_name: str) -> bytes:
    """Preflight the exact Gemini POST body before writing a submission intent."""
    return GeminiBatchClient.batch_bytes(model, requests, display_name)
