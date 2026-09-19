"""Bounded stdlib Sonnet 5 Message Batches transport; imports are offline.

The caller owns consent, spend limits, durable submission intents, matching
results to a wave, and recovery of ambiguous submissions. No credentials are
discovered, requests retried, files uploaded, or remote objects deleted here.

Reviewed 2026-09-12 against the official API and model documentation:
https://platform.claude.com/docs/en/build-with-claude/batch-processing
https://platform.claude.com/docs/en/api/messages/batches/create
https://platform.claude.com/docs/en/api/messages/batches/retrieve
https://platform.claude.com/docs/en/models/sonnet-5/overview
"""

from __future__ import annotations

import datetime as dt
import json
import re
from typing import Any

from pipeline.transcript_summary_client import (
    MAX_BATCH_REQUESTS,
    OpenAIBatchClient,
    OpenAIClientError,
)


ANTHROPIC_API_ORIGIN = "https://api.anthropic.com"
ANTHROPIC_MODEL = "claude-sonnet-5"
ANTHROPIC_VERSION = "2023-06-01"
MAX_ANTHROPIC_BATCH_BYTES = 16 * 1024 * 1024
MAX_ANTHROPIC_OUTPUT_TOKENS = 128_000
ANTHROPIC_BATCH_STATUSES = frozenset({"in_progress", "canceling", "ended"})
AnthropicClientError = OpenAIClientError
_BATCH_PATH = "/v1/messages/batches"
_BATCH_ID = re.compile(r"msgbatch_[A-Za-z0-9_-]{1,128}\Z")
_CUSTOM_ID = re.compile(r"[A-Za-z0-9_-]{1,64}\Z")
_TIMESTAMP = re.compile(
    r"[0-9]{4}-[0-9]{2}-[0-9]{2}T(?:[01][0-9]|2[0-3]):[0-5][0-9]:[0-5][0-9]"
    r"(?:\.[0-9]{1,6})?(?:Z|[+-](?:[01][0-9]|2[0-3]):[0-5][0-9])\Z"
)
_COUNT_FIELDS = frozenset({"processing", "succeeded", "errored", "canceled", "expired"})
_BATCH_FIELDS = frozenset({
    "id", "type", "processing_status", "request_counts", "created_at",
    "expires_at", "ended_at", "cancel_initiated_at", "results_url",
})


def _batch_id(value: Any) -> str:
    if not isinstance(value, str) or _BATCH_ID.fullmatch(value) is None:
        raise AnthropicClientError("invalid Anthropic batch ID")
    return value


def _text(value: Any) -> bool:
    return (isinstance(value, str) and bool(value)
            and not any(0xD800 <= ord(char) <= 0xDFFF for char in value))


def _text_content(value: Any) -> bool:
    if isinstance(value, str):
        return _text(value)
    return (isinstance(value, list) and 1 <= len(value) <= MAX_BATCH_REQUESTS
            and all(isinstance(block, dict) and set(block) == {"type", "text"}
                    and block["type"] == "text" and _text(block["text"])
                    for block in value))


def _params(value: Any) -> None:
    """Reject unsupported controls before a paid request can be sent."""
    if (not isinstance(value, dict)
            or set(value) - {"model", "max_tokens", "messages", "system", "output_config", "thinking"}
            or value.get("model") != ANTHROPIC_MODEL
            or type(value.get("max_tokens")) is not int
            or not 1 <= value["max_tokens"] <= MAX_ANTHROPIC_OUTPUT_TOKENS
            or not isinstance(value.get("messages"), list)
            or not 1 <= len(value["messages"]) <= MAX_BATCH_REQUESTS):
        raise AnthropicClientError("Anthropic batch request controls rejected")
    for message in value["messages"]:
        if (not isinstance(message, dict) or set(message) != {"role", "content"}
                or not isinstance(message["role"], str)
                or message["role"] not in {"user", "assistant"}
                or not _text_content(message["content"])):
            raise AnthropicClientError("Anthropic batch requires text messages")
    if value["messages"][-1]["role"] != "user":
        raise AnthropicClientError("Anthropic batch assistant prefilling is disabled")
    if "system" in value and not _text_content(value["system"]):
        raise AnthropicClientError("invalid Anthropic batch system prompt")
    if "thinking" in value:
        thinking = value["thinking"]
        if (not isinstance(thinking, dict) or set(thinking) != {"type"}
                or not isinstance(thinking["type"], str)
                or thinking["type"] not in {"adaptive", "disabled"}):
            raise AnthropicClientError("invalid Sonnet 5 thinking controls")
    if "output_config" in value:
        config = value["output_config"]
        if (not isinstance(config, dict) or not config or set(config) - {"effort", "format"}
                or ("effort" in config and (not isinstance(config["effort"], str)
                    or config["effort"] not in {"low", "medium", "high", "xhigh", "max"}))):
            raise AnthropicClientError("invalid Anthropic output configuration")
        if "format" in config:
            output = config["format"]
            if (not isinstance(output, dict) or set(output) != {"type", "schema"}
                    or output["type"] != "json_schema"
                    or not isinstance(output["schema"], dict) or not output["schema"]):
                raise AnthropicClientError("invalid Anthropic JSON output format")


def _anthropic_batch_data(requests: list[dict]) -> bytes:
    """Shared offline validation and canonical encoding, before size admission."""
    if not isinstance(requests, list) or not 1 <= len(requests) <= MAX_BATCH_REQUESTS:
        raise AnthropicClientError("invalid Anthropic batch request count")
    seen = set()
    for item in requests:
        if (not isinstance(item, dict) or set(item) != {"custom_id", "params"}
                or not isinstance(item["custom_id"], str)
                or _CUSTOM_ID.fullmatch(item["custom_id"]) is None
                or item["custom_id"] in seen):
            raise AnthropicClientError("invalid or duplicate Anthropic batch request ID")
        seen.add(item["custom_id"])
        _params(item["params"])
    try:
        data = json.dumps({"requests": requests}, sort_keys=True, ensure_ascii=True, allow_nan=False,
                          separators=(",", ":")).encode("utf-8")
    except (ValueError, TypeError, UnicodeError, RecursionError):
        raise AnthropicClientError("invalid Anthropic batch JSON") from None
    return data


def anthropic_batch_size(requests: list[dict]) -> int:
    """Return exact wire bytes, including a candidate above the submission cap.

    Planners use this to stop adding jobs before a wave exceeds its bound. All
    request controls and IDs are validated, but this helper does not admit a
    batch for submission or perform network work.
    """
    return len(_anthropic_batch_data(requests))


def anthropic_batch_bytes(requests: list[dict]) -> bytes:
    """Validate offline and encode the exact bounded POST body for create_batch.

    This conservative subset permits text, optional adaptive/disabled thinking,
    and JSON-schema output. It excludes tools, caching, metadata, sampling
    overrides, streaming, containers, file references, and beta features.
    """
    data = _anthropic_batch_data(requests)
    if len(data) > MAX_ANTHROPIC_BATCH_BYTES:
        raise AnthropicClientError("Anthropic batch exceeds size limit")
    return data


def _timestamp(value: Any) -> dt.datetime:
    if not isinstance(value, str) or _TIMESTAMP.fullmatch(value) is None:
        raise AnthropicClientError("invalid Anthropic batch timestamp")
    try:
        return dt.datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        raise AnthropicClientError("invalid Anthropic batch timestamp") from None


def _batch(value: Any, *, expected_id: str | None = None,
           expected_count: int | None = None) -> dict:
    if (not isinstance(value, dict) or not _BATCH_FIELDS <= set(value)
            or set(value) - _BATCH_FIELDS - {"archived_at"}
            or value["type"] != "message_batch"
            or not isinstance(value["processing_status"], str)
            or value["processing_status"] not in ANTHROPIC_BATCH_STATUSES):
        raise AnthropicClientError("invalid Anthropic batch response")
    identifier = _batch_id(value["id"])
    if expected_id is not None and identifier != expected_id:
        raise AnthropicClientError("Anthropic batch response ID does not match")
    counts = value["request_counts"]
    if (not isinstance(counts, dict) or set(counts) != _COUNT_FIELDS
            or any(type(count) is not int or not 0 <= count <= MAX_BATCH_REQUESTS
                   for count in counts.values())
            or not 1 <= sum(counts.values()) <= MAX_BATCH_REQUESTS
            or (expected_count is not None and sum(counts.values()) != expected_count)):
        raise AnthropicClientError("invalid Anthropic batch request counts")
    created, expires = _timestamp(value["created_at"]), _timestamp(value["expires_at"])
    if expires <= created:
        raise AnthropicClientError("invalid Anthropic batch lifetime")
    dates = {}
    for key in ("ended_at", "cancel_initiated_at", "archived_at"):
        if value.get(key) is not None:
            dates[key] = _timestamp(value[key])
            if dates[key] < created:
                raise AnthropicClientError("invalid Anthropic batch timestamp ordering")
    ended = value["processing_status"] == "ended"
    if (ended != ("ended_at" in dates)
            or (ended and counts["processing"] != 0)
            or (not ended and value["results_url"] is not None)
            or (value["processing_status"] == "canceling" and "cancel_initiated_at" not in dates)
            or (value["processing_status"] == "in_progress" and "cancel_initiated_at" in dates)
            or ("archived_at" in dates and not ended)
            or (ended and "cancel_initiated_at" in dates
                and dates["cancel_initiated_at"] > dates["ended_at"])
            or (ended and "archived_at" in dates and dates["archived_at"] < dates["ended_at"])):
        raise AnthropicClientError("inconsistent Anthropic batch status")
    if (value["results_url"] is not None
            and value["results_url"] != ANTHROPIC_API_ORIGIN + _BATCH_PATH + "/" + identifier + "/results"):
        raise AnthropicClientError("Anthropic batch results destination rejected")
    return value


def validate_batch(value: Any, expected_id: str | None = None,
                   expected_count: int | None = None) -> dict:
    """Validate a remote or persisted receipt without credentials or HTTP."""
    if expected_id is not None:
        _batch_id(expected_id)
    if expected_count is not None and (type(expected_count) is not int
                                      or not 1 <= expected_count <= MAX_BATCH_REQUESTS):
        raise AnthropicClientError("invalid expected Anthropic batch request count")
    return _batch(value, expected_id=expected_id, expected_count=expected_count)


class AnthropicBatchClient(OpenAIBatchClient):
    """Sonnet 5 only, sharing the bounded, non-retrying stdlib HTTP transport.

    download_results returns bounded raw JSONL; the supervisor validates rows,
    IDs, schemas, and their exact match to its durable wave manifest.
    """

    provider_name = "Anthropic"

    def _destination(self, method: str, path: str) -> str:
        if not isinstance(path, str):
            raise AnthropicClientError("Anthropic request destination rejected")
        if method == "POST":
            valid = path == _BATCH_PATH
        elif method == "GET":
            valid = re.fullmatch(
                r"/v1/messages/batches/msgbatch_[A-Za-z0-9_-]{1,128}(?:/results)?", path
            ) is not None
        else:
            valid = False
        if not valid:
            raise AnthropicClientError("Anthropic request destination rejected")
        return ANTHROPIC_API_ORIGIN + path

    def _headers(self, raw: bool) -> dict:
        return {"x-api-key": self._api_key, "anthropic-version": ANTHROPIC_VERSION,
                "Accept": "application/x-jsonl" if raw else "application/json",
                "Accept-Encoding": "identity"}

    def _request(self, method: str, path: str, **kwargs) -> dict | bytes:
        try:
            return super()._request(method, path, **kwargs)
        except AnthropicClientError:
            raise
        except Exception:
            # A malformed adapter response must not expose content or obscure
            # an unknown mutation outcome. Ordinary HTTP errors use the shared
            # transport's more specific status/Retry-After handling.
            raise AnthropicClientError("Anthropic request transport failed",
                                       ambiguous=method == "POST") from None

    def create_batch(self, requests: list[dict]) -> dict:
        data = anthropic_batch_bytes(requests)
        value = self._request("POST", _BATCH_PATH, data=data, content_type="application/json")
        try:
            return validate_batch(value, expected_count=len(requests))
        except (AnthropicClientError, ValueError, TypeError, KeyError):
            raise AnthropicClientError("Anthropic creation response failed validation",
                                       status_code=200, ambiguous=True) from None

    def retrieve_batch(self, batch_id: str) -> dict:
        identifier = _batch_id(batch_id)
        value = self._request("GET", _BATCH_PATH + "/" + identifier)
        try:
            return validate_batch(value, expected_id=identifier)
        except (AnthropicClientError, ValueError, TypeError, KeyError):
            raise AnthropicClientError("Anthropic polling response failed validation", status_code=200) from None

    def download_results(self, batch_id: str) -> bytes:
        return self._request("GET", _BATCH_PATH + "/" + _batch_id(batch_id) + "/results", raw=True)

    def get_batch(self, batch_id: str) -> dict:
        return self.retrieve_batch(batch_id)

    def list_batches(self, *args, **kwargs) -> dict:
        raise AnthropicClientError("Anthropic batch listing is disabled")

    def upload_batch(self, data: bytes) -> dict:
        raise AnthropicClientError("Anthropic batches use inline requests; file uploads are disabled")

    def get_file(self, file_id: str) -> dict:
        raise AnthropicClientError("Anthropic file access is disabled")

    def download_file(self, file_id: str) -> bytes:
        raise AnthropicClientError("Anthropic file access is disabled")

    def delete_file(self, file_id: str) -> dict:
        raise AnthropicClientError("Anthropic file deletion is disabled")
