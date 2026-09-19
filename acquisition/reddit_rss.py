#!/usr/bin/env python3
"""Capture and parse public Reddit Atom feeds as unreviewed discovery evidence.

This lane deliberately does not use Reddit JSON endpoints, credentials, cookies,
comments, or media downloads.  The exact Atom response is sealed before a minimized
discovery manifest is derived from it.  Titles and links are discovery assertions,
not verified descriptions of media content.
"""

from __future__ import annotations

import argparse
import hashlib
import html
import json
import math
import re
import sys
import urllib.error
import urllib.parse
import urllib.request
import xml.etree.ElementTree as ET
from datetime import datetime, timedelta, timezone
from html.parser import HTMLParser
from pathlib import Path
from typing import Any, Iterable


SCHEMA_VERSION = 1
ATOM_NAMESPACE = "http://www.w3.org/2005/Atom"
ATOM = f"{{{ATOM_NAMESPACE}}}"
USER_AGENT = "hidinginmyroom-corpus-reddit-rss/1.0 (public metadata research)"
ACCEPT = "application/atom+xml, application/xml;q=0.9, text/xml;q=0.8"
SHA256_RE = re.compile(r"^[0-9a-f]{64}$")
SUBREDDIT_RE = re.compile(r"^[A-Za-z0-9_]{2,21}$")
POST_ID_RE = re.compile(r"^[a-z0-9]{5,16}$")
VREDDIT_ID_RE = re.compile(r"^[A-Za-z0-9_-]{5,64}$")
YOUTUBE_ID_RE = re.compile(r"^[A-Za-z0-9_-]{11}$")
UTC_RE = re.compile(r"^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(?:\.\d+)?Z$")
WHOLE_SECOND_UTC_RE = re.compile(r"^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}Z$")
URL_RE = re.compile(r"https?://[^\s<>\"']+", re.IGNORECASE)
ALLOWED_REDDIT_HOSTS = frozenset({"reddit.com", "www.reddit.com", "old.reddit.com"})
IMAGE_HOSTS = frozenset(
    {
        "i.redd.it",
        "preview.redd.it",
        "external-preview.redd.it",
        "styles.redditmedia.com",
    }
)
IMAGE_SUFFIXES = frozenset({".jpg", ".jpeg", ".png", ".webp", ".gif", ".avif"})
VIDEO_SUFFIXES = frozenset({".mp4", ".webm", ".mov", ".m4v"})
MAX_INFO_BYTES = 16 * 1024 * 1024
DEFAULT_MAX_FEED_BYTES = 8 * 1024 * 1024


class RedditRssError(RuntimeError):
    """A fail-closed capture, validation, or parsing failure."""


def canonical_bytes(value: Any) -> bytes:
    return json.dumps(
        value,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
        allow_nan=False,
    ).encode("utf-8")


def sha256_bytes(value: bytes) -> str:
    return hashlib.sha256(value).hexdigest()


def stable_id(prefix: str, value: Any) -> str:
    return f"{prefix}_{sha256_bytes(canonical_bytes(value))[:32]}"


def _now_utc_datetime() -> datetime:
    """Return the current instant; kept separate so boundary tests can fix the clock."""

    return datetime.now(timezone.utc)


def utc_now() -> str:
    return _now_utc_datetime().isoformat(timespec="seconds").replace("+00:00", "Z")


def _exact_keys(value: Any, expected: set[str], label: str) -> dict[str, Any]:
    if not isinstance(value, dict):
        raise RedditRssError(f"{label} must be an object")
    missing = sorted(expected - set(value))
    unknown = sorted(set(value) - expected)
    if missing or unknown:
        raise RedditRssError(f"{label} keys differ from the contract; missing={missing}, unknown={unknown}")
    return value


def _utc(value: Any, label: str) -> str:
    if not isinstance(value, str) or not UTC_RE.fullmatch(value):
        raise RedditRssError(f"{label} must be a UTC RFC 3339 timestamp ending in Z")
    try:
        datetime.fromisoformat(value[:-1] + "+00:00")
    except ValueError as error:
        raise RedditRssError(f"{label} is not a valid timestamp") from error
    return value


def _metadata_observation_datetime(value: Any) -> datetime:
    if not isinstance(value, str) or not WHOLE_SECOND_UTC_RE.fullmatch(value):
        raise RedditRssError(
            "metadata-observed-at must be a whole-second UTC RFC 3339 timestamp ending in Z"
        )
    try:
        return datetime.strptime(value, "%Y-%m-%dT%H:%M:%SZ").replace(tzinfo=timezone.utc)
    except ValueError as error:
        raise RedditRssError("metadata-observed-at is not a valid timestamp") from error


def _whole_seconds_since_epoch(value: datetime) -> int:
    epoch = datetime(1970, 1, 1, tzinfo=timezone.utc)
    return (value - epoch) // timedelta(seconds=1)


def _https_url(value: Any, label: str) -> str:
    if not isinstance(value, str) or len(value) > 4096:
        raise RedditRssError(f"{label} must be an HTTPS URL no longer than 4096 characters")
    try:
        parsed = urllib.parse.urlsplit(value)
    except ValueError as error:
        raise RedditRssError(f"{label} is not a valid URL") from error
    if parsed.scheme != "https" or not parsed.hostname or parsed.username or parsed.password:
        raise RedditRssError(f"{label} must be an HTTPS URL without user information")
    return value


def _reddit_feed_url(value: Any, subreddit: str, label: str) -> str:
    url = _https_url(value, label)
    parsed = urllib.parse.urlsplit(url)
    if (parsed.hostname or "").lower() not in ALLOWED_REDDIT_HOSTS:
        raise RedditRssError(f"{label} must remain on a public reddit.com host")
    expected = f"/r/{subreddit.lower()}/"
    if expected not in parsed.path.lower() or ".rss" not in parsed.path.lower():
        raise RedditRssError(f"{label} is not the requested subreddit Atom feed")
    return url


def _safe_basename(value: Any, label: str, suffix: str) -> str:
    if not isinstance(value, str) or Path(value).name != value or not value.endswith(suffix):
        raise RedditRssError(f"{label} must be a basename ending in {suffix}")
    return value


def _read_bounded(path: Path, maximum: int, label: str) -> bytes:
    try:
        size = path.stat().st_size
    except OSError as error:
        raise RedditRssError(f"cannot stat {label} {path}: {error}") from error
    if size > maximum:
        raise RedditRssError(f"{label} exceeds the {maximum}-byte limit")
    try:
        return path.read_bytes()
    except OSError as error:
        raise RedditRssError(f"cannot read {label} {path}: {error}") from error


def _load_json(path: Path, maximum: int = MAX_INFO_BYTES) -> Any:
    raw = _read_bounded(path, maximum, "JSON file")
    try:
        return json.loads(raw.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError) as error:
        raise RedditRssError(f"invalid UTF-8 JSON in {path}: {error}") from error


def _write_immutable(path: Path, payload: bytes) -> None:
    try:
        with path.open("xb") as handle:
            handle.write(payload)
    except FileExistsError:
        if path.read_bytes() != payload:
            raise RedditRssError(f"refusing to overwrite an immutable artifact: {path}")
    except OSError as error:
        raise RedditRssError(f"cannot write immutable artifact {path}: {error}") from error


def _validate_xml(payload: bytes) -> ET.Element:
    prefix = payload[:4096].upper()
    if b"<!DOCTYPE" in prefix or b"<!ENTITY" in prefix:
        raise RedditRssError("Atom payload contains a forbidden DTD or entity declaration")
    try:
        root = ET.fromstring(payload)
    except ET.ParseError as error:
        raise RedditRssError(f"response is not well-formed XML: {error}") from error
    if root.tag != f"{ATOM}feed":
        raise RedditRssError("response root is not an Atom feed")
    return root


class _SafeRedirectHandler(urllib.request.HTTPRedirectHandler):
    def __init__(self, subreddit: str):
        self.subreddit = subreddit

    def redirect_request(self, req, fp, code, msg, headers, newurl):  # noqa: N802
        _reddit_feed_url(newurl, self.subreddit, "redirect URL")
        return super().redirect_request(req, fp, code, msg, headers, newurl)


def _canonical_request_url(subreddit: str, limit: int) -> str:
    if not SUBREDDIT_RE.fullmatch(subreddit):
        raise RedditRssError("subreddit must contain 2..21 letters, digits, or underscores")
    if isinstance(limit, bool) or not isinstance(limit, int) or not 1 <= limit <= 100:
        raise RedditRssError("limit must be between 1 and 100")
    return f"https://www.reddit.com/r/{subreddit}/new/.rss?limit={limit}"


def capture_snapshot(
    *,
    subreddit: str,
    limit: int,
    out_dir: Path,
    timeout_seconds: int = 30,
    max_bytes: int = DEFAULT_MAX_FEED_BYTES,
    opener: Any | None = None,
) -> Path:
    """Capture one exact public Atom response and return its snapshot manifest."""

    request_url = _canonical_request_url(subreddit, limit)
    if not out_dir.is_absolute():
        raise RedditRssError("out-dir must be an absolute path")
    if timeout_seconds < 1 or timeout_seconds > 120:
        raise RedditRssError("timeout-seconds must be between 1 and 120")
    if max_bytes < 1024 or max_bytes > 64 * 1024 * 1024:
        raise RedditRssError("max-bytes must be between 1024 and 67108864")
    out_dir.mkdir(parents=True, exist_ok=True)

    request = urllib.request.Request(
        request_url,
        method="GET",
        headers={"Accept": ACCEPT, "User-Agent": USER_AGENT},
    )
    # Disable ambient proxy discovery so environment-provided proxy credentials can
    # never become part of this explicitly no-auth capture lane.
    client = opener or urllib.request.build_opener(
        urllib.request.ProxyHandler({}), _SafeRedirectHandler(subreddit)
    )
    started_at = utc_now()
    try:
        response = client.open(request, timeout=timeout_seconds)
        with response:
            status = int(getattr(response, "status", response.getcode()))
            final_url = response.geturl()
            _reddit_feed_url(final_url, subreddit, "final URL")
            if status != 200:
                retryable = status in {403, 408, 425, 429, 500, 502, 503, 504}
                raise RedditRssError(
                    f"Reddit Atom request failed with HTTP {status}; retryable={str(retryable).lower()}"
                )
            content_length = response.headers.get("Content-Length")
            if content_length:
                try:
                    if int(content_length) > max_bytes:
                        raise RedditRssError("Reddit Atom response exceeds max-bytes")
                except ValueError as error:
                    raise RedditRssError("invalid Content-Length in Reddit response") from error
            chunks: list[bytes] = []
            total = 0
            while True:
                chunk = response.read(min(64 * 1024, max_bytes + 1 - total))
                if not chunk:
                    break
                chunks.append(chunk)
                total += len(chunk)
                if total > max_bytes:
                    raise RedditRssError("Reddit Atom response exceeds max-bytes")
            payload = b"".join(chunks)
            content_type = str(response.headers.get("Content-Type") or "")[:200]
    except urllib.error.HTTPError as error:
        retryable = error.code in {403, 408, 425, 429, 500, 502, 503, 504}
        error.close()
        raise RedditRssError(
            f"Reddit Atom request failed with HTTP {error.code}; retryable={str(retryable).lower()}"
        ) from error
    except urllib.error.URLError as error:
        raise RedditRssError(f"Reddit Atom request failed; retryable=true; reason={error.reason}") from error

    _validate_xml(payload)
    observed_at = utc_now()
    digest = sha256_bytes(payload)
    identity = {
        "subreddit": subreddit.lower(),
        "request_url": request_url,
        "final_url": final_url,
        "observed_at": observed_at,
        "payload_sha256": digest,
    }
    snapshot_id = stable_id("rrs", identity)
    payload_filename = f"{snapshot_id}.atom.xml"
    manifest_filename = f"{snapshot_id}.snapshot.json"
    manifest = {
        "schema_version": SCHEMA_VERSION,
        "snapshot_id": snapshot_id,
        "source_kind": "reddit_atom_feed",
        "request": {
            "method": "GET",
            "url": request_url,
            "subreddit": subreddit,
            "sort": "new",
            "limit": limit,
            "accept": ACCEPT,
            "user_agent": USER_AGENT,
            "cookies_sent": False,
            "authorization_sent": False,
            "started_at": started_at,
        },
        "response": {
            "http_status": 200,
            "final_url": final_url,
            "observed_at": observed_at,
            "content_type": content_type,
            "payload_sha256": digest,
            "byte_count": len(payload),
            "payload_file": payload_filename,
        },
        "privacy": {
            "public_feed_only": True,
            "comments_collected": False,
            "authors_extracted": False,
            "publication_authority": False,
        },
    }
    _write_immutable(out_dir / payload_filename, payload)
    _write_immutable(out_dir / manifest_filename, canonical_bytes(manifest) + b"\n")
    validate_snapshot_manifest(out_dir / manifest_filename, verify_payload=True)
    return out_dir / manifest_filename


def validate_snapshot_manifest(path: Path, *, verify_payload: bool = True) -> dict[str, Any]:
    raw = _exact_keys(
        _load_json(path),
        {"schema_version", "snapshot_id", "source_kind", "request", "response", "privacy"},
        "snapshot",
    )
    if raw["schema_version"] != 1 or raw["source_kind"] != "reddit_atom_feed":
        raise RedditRssError("unsupported snapshot schema or source kind")
    request = _exact_keys(
        raw["request"],
        {
            "method",
            "url",
            "subreddit",
            "sort",
            "limit",
            "accept",
            "user_agent",
            "cookies_sent",
            "authorization_sent",
            "started_at",
        },
        "snapshot.request",
    )
    subreddit = request["subreddit"]
    if not isinstance(subreddit, str) or not SUBREDDIT_RE.fullmatch(subreddit):
        raise RedditRssError("snapshot.request.subreddit is invalid")
    if request["method"] != "GET" or request["sort"] != "new":
        raise RedditRssError("snapshot request must be a GET of the new feed")
    if request["url"] != _canonical_request_url(subreddit, request["limit"]):
        raise RedditRssError("snapshot request URL is not canonical")
    if request["accept"] != ACCEPT or request["user_agent"] != USER_AGENT:
        raise RedditRssError("snapshot request headers differ from the public no-auth contract")
    if request["cookies_sent"] is not False or request["authorization_sent"] is not False:
        raise RedditRssError("authenticated or cookie-bearing snapshots are forbidden")
    _utc(request["started_at"], "snapshot.request.started_at")

    response = _exact_keys(
        raw["response"],
        {
            "http_status",
            "final_url",
            "observed_at",
            "content_type",
            "payload_sha256",
            "byte_count",
            "payload_file",
        },
        "snapshot.response",
    )
    if response["http_status"] != 200:
        raise RedditRssError("only successful public Atom snapshots are importable")
    _reddit_feed_url(response["final_url"], subreddit, "snapshot.response.final_url")
    _utc(response["observed_at"], "snapshot.response.observed_at")
    if not isinstance(response["payload_sha256"], str) or not SHA256_RE.fullmatch(response["payload_sha256"]):
        raise RedditRssError("snapshot.response.payload_sha256 is invalid")
    if isinstance(response["byte_count"], bool) or not isinstance(response["byte_count"], int) or response["byte_count"] < 1:
        raise RedditRssError("snapshot.response.byte_count must be a positive integer")
    payload_name = _safe_basename(response["payload_file"], "snapshot.response.payload_file", ".atom.xml")
    if not isinstance(response["content_type"], str) or len(response["content_type"]) > 200:
        raise RedditRssError("snapshot.response.content_type is invalid")

    privacy = _exact_keys(
        raw["privacy"],
        {"public_feed_only", "comments_collected", "authors_extracted", "publication_authority"},
        "snapshot.privacy",
    )
    expected_privacy = {
        "public_feed_only": True,
        "comments_collected": False,
        "authors_extracted": False,
        "publication_authority": False,
    }
    if privacy != expected_privacy:
        raise RedditRssError("snapshot privacy policy differs from the fail-closed contract")

    identity = {
        "subreddit": subreddit.lower(),
        "request_url": request["url"],
        "final_url": response["final_url"],
        "observed_at": response["observed_at"],
        "payload_sha256": response["payload_sha256"],
    }
    if raw["snapshot_id"] != stable_id("rrs", identity):
        raise RedditRssError("snapshot_id does not match the sealed snapshot identity")
    if verify_payload:
        payload = _read_bounded(path.parent / payload_name, 64 * 1024 * 1024, "Atom payload")
        if len(payload) != response["byte_count"] or sha256_bytes(payload) != response["payload_sha256"]:
            raise RedditRssError("Atom payload bytes do not match the snapshot manifest")
        _validate_xml(payload)
    return raw


class _UrlCollector(HTMLParser):
    def __init__(self) -> None:
        super().__init__(convert_charrefs=True)
        self.urls: list[str] = []

    def handle_starttag(self, tag: str, attrs: list[tuple[str, str | None]]) -> None:
        for key, value in attrs:
            if key.lower() in {"href", "src", "poster"} and value:
                self.urls.append(value)

    def handle_data(self, data: str) -> None:
        self.urls.extend(match.group(0) for match in URL_RE.finditer(data))


def _normalized_url(raw: str) -> str | None:
    candidate = html.unescape(raw).strip().rstrip(".,;:!?)]]}")
    if candidate.startswith("//"):
        candidate = "https:" + candidate
    try:
        parsed = urllib.parse.urlsplit(candidate)
    except ValueError:
        return None
    if parsed.scheme not in {"http", "https"} or not parsed.hostname or parsed.username or parsed.password:
        return None
    try:
        return urllib.parse.urlunsplit(
            ("https", parsed.netloc.lower(), parsed.path or "/", parsed.query, "")
        )
    except ValueError:
        return None


def _youtube_id(parsed: urllib.parse.SplitResult) -> str | None:
    host = (parsed.hostname or "").lower()
    if host in {"youtu.be", "www.youtu.be"}:
        candidate = parsed.path.strip("/").split("/", 1)[0]
    elif host in {"youtube.com", "www.youtube.com", "m.youtube.com", "music.youtube.com"}:
        segments = [urllib.parse.unquote(part) for part in parsed.path.split("/") if part]
        if parsed.path == "/watch":
            candidate = urllib.parse.parse_qs(parsed.query).get("v", [""])[0]
        elif len(segments) >= 2 and segments[0] in {"shorts", "live", "embed"}:
            candidate = segments[1]
        else:
            return None
    else:
        return None
    return candidate if YOUTUBE_ID_RE.fullmatch(candidate) else None


def _media_locator(raw_url: str, basis: str) -> dict[str, Any] | None:
    observed = _normalized_url(raw_url)
    if not observed:
        return None
    parsed = urllib.parse.urlsplit(observed)
    host = (parsed.hostname or "").lower()
    segments = [urllib.parse.unquote(part) for part in parsed.path.split("/") if part]

    if host == "v.redd.it" and segments and VREDDIT_ID_RE.fullmatch(segments[0]):
        native_id = segments[0]
        return {
            "locator_kind": "reddit_video",
            "native_id": native_id,
            "canonical_url": f"https://v.redd.it/{native_id}",
            "observed_url": observed,
            "basis": basis,
            "assertion_state": "unreviewed",
        }
    youtube_id = _youtube_id(parsed)
    if youtube_id:
        return {
            "locator_kind": "youtube_video",
            "native_id": youtube_id,
            "canonical_url": f"https://www.youtube.com/watch?v={youtube_id}",
            "observed_url": observed,
            "basis": basis,
            "assertion_state": "unreviewed",
        }
    if host in {"archive.org", "www.archive.org"} and len(segments) >= 2:
        if segments[0] == "details" and segments[1]:
            item = segments[1]
            return {
                "locator_kind": "internet_archive_item",
                "native_id": item,
                "canonical_url": f"https://archive.org/details/{urllib.parse.quote(item, safe='')}",
                "observed_url": observed,
                "basis": basis,
                "assertion_state": "unreviewed",
            }
        if segments[0] == "download" and len(segments) >= 3 and segments[1]:
            item = segments[1]
            filename = "/".join(segments[2:])
            if filename and all(part not in {".", ".."} for part in filename.split("/")):
                native_id = f"{item}/{filename}"
                return {
                    "locator_kind": "internet_archive_file",
                    "native_id": native_id,
                    "canonical_url": (
                        f"https://archive.org/download/{urllib.parse.quote(item, safe='')}/"
                        f"{urllib.parse.quote(filename, safe='/()[],-_.')}"
                    ),
                    "observed_url": observed,
                    "basis": basis,
                    "assertion_state": "unreviewed",
                }
    if host in ALLOWED_REDDIT_HOSTS and len(segments) >= 2 and segments[0] == "gallery":
        gallery_id = segments[1].lower()
        if POST_ID_RE.fullmatch(gallery_id):
            return {
                "locator_kind": "reddit_gallery",
                "native_id": gallery_id,
                "canonical_url": f"https://www.reddit.com/gallery/{gallery_id}",
                "observed_url": observed,
                "basis": basis,
                "assertion_state": "unreviewed",
            }
    suffix = Path(parsed.path).suffix.lower()
    if host in IMAGE_HOSTS or suffix in IMAGE_SUFFIXES:
        canonical = urllib.parse.urlunsplit(("https", parsed.netloc.lower(), parsed.path, "", ""))
        return {
            "locator_kind": "image",
            "native_id": f"{host}{parsed.path}",
            "canonical_url": canonical,
            "observed_url": observed,
            "basis": basis,
            "assertion_state": "unreviewed",
        }
    if suffix in VIDEO_SUFFIXES:
        canonical = urllib.parse.urlunsplit(("https", parsed.netloc.lower(), parsed.path, "", ""))
        return {
            "locator_kind": "external_video",
            "native_id": f"{host}{parsed.path}",
            "canonical_url": canonical,
            "observed_url": observed,
            "basis": basis,
            "assertion_state": "unreviewed",
        }
    return None


def _post_id(entry: ET.Element, link_urls: Iterable[str]) -> str:
    atom_id = (entry.findtext(f"{ATOM}id") or "").strip().lower()
    match = re.search(r"(?:^|_)t3_([a-z0-9]{5,16})$", atom_id)
    if match:
        return match.group(1)
    for raw in link_urls:
        normalized = _normalized_url(raw)
        if not normalized:
            continue
        parsed = urllib.parse.urlsplit(normalized)
        segments = [part.lower() for part in parsed.path.split("/") if part]
        if "comments" in segments:
            index = segments.index("comments")
            if index + 1 < len(segments) and POST_ID_RE.fullmatch(segments[index + 1]):
                return segments[index + 1]
    raise RedditRssError("Atom entry has no valid Reddit post ID")


def _entry_timestamp(entry: ET.Element, field: str) -> str | None:
    value = (entry.findtext(f"{ATOM}{field}") or "").strip()
    if not value:
        return None
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError as error:
        raise RedditRssError(f"Atom entry has an invalid {field} timestamp") from error
    if parsed.tzinfo is None:
        raise RedditRssError(f"Atom entry {field} timestamp has no timezone")
    return parsed.astimezone(timezone.utc).isoformat(timespec="seconds").replace("+00:00", "Z")


def _parse_entries(payload: bytes, subreddit: str, limit: int) -> list[dict[str, Any]]:
    root = _validate_xml(payload)
    entries = root.findall(f"{ATOM}entry")
    if len(entries) > limit:
        raise RedditRssError("Atom response contains more entries than the requested limit")
    posts: dict[str, dict[str, Any]] = {}
    basis_rank = {"atom_media_element": 0, "atom_entry_content": 1, "atom_entry_link": 2}
    for entry in entries:
        entry_links = [
            value
            for element in entry.findall(f"{ATOM}link")
            if isinstance((value := element.attrib.get("href")), str)
        ]
        post_id = _post_id(entry, entry_links)
        if post_id in posts:
            raise RedditRssError(f"duplicate Atom entry for Reddit post {post_id}")
        permalink: str | None = None
        candidate_urls: list[tuple[str, str]] = []
        for link in entry_links:
            normalized = _normalized_url(link)
            if not normalized:
                continue
            parsed = urllib.parse.urlsplit(normalized)
            if (parsed.hostname or "").lower() in ALLOWED_REDDIT_HOSTS and "/comments/" in parsed.path:
                permalink = normalized
            else:
                candidate_urls.append((normalized, "atom_entry_link"))
        if permalink is None:
            permalink = f"https://www.reddit.com/r/{subreddit}/comments/{post_id}/"

        for content in entry.findall(f"{ATOM}content") + entry.findall(f"{ATOM}summary"):
            raw_html = content.text or ""
            collector = _UrlCollector()
            collector.feed(raw_html)
            collector.close()
            collector.urls.extend(match.group(0) for match in URL_RE.finditer(html.unescape(raw_html)))
            candidate_urls.extend((url, "atom_entry_content") for url in collector.urls)
        for element in entry.iter():
            if element.tag in {f"{ATOM}link", f"{ATOM}content", f"{ATOM}summary"}:
                continue
            for key in ("url", "src", "href"):
                value = element.attrib.get(key)
                if value:
                    candidate_urls.append((value, "atom_media_element"))

        locators: dict[tuple[str, str], dict[str, Any]] = {}
        for url, basis in candidate_urls:
            locator = _media_locator(url, basis)
            if not locator:
                continue
            key = (locator["locator_kind"], locator["native_id"])
            prior = locators.get(key)
            if prior is None or basis_rank[basis] < basis_rank[prior["basis"]]:
                locators[key] = locator

        title = " ".join(html.unescape(entry.findtext(f"{ATOM}title") or post_id).split())
        if not title or len(title) > 500:
            raise RedditRssError(f"Atom entry {post_id} has an invalid title")
        published = _entry_timestamp(entry, "published")
        updated = _entry_timestamp(entry, "updated")
        if not published and not updated:
            raise RedditRssError(f"Atom entry {post_id} has no timestamp")
        timestamp_basis = "atom_published" if published else "atom_updated_fallback"
        posts[post_id] = {
            "post_id": post_id,
            "permalink": permalink,
            "title": title,
            "published_at": published or updated,
            "updated_at": updated or published,
            "timestamp_basis": timestamp_basis,
            "title_assertion_state": "unreviewed",
            "media_locators": sorted(
                locators.values(),
                key=lambda value: (value["locator_kind"], value["native_id"], value["canonical_url"]),
            ),
        }
    return [posts[key] for key in sorted(posts)]


def _v_reddit_ids_in_info(info: dict[str, Any]) -> set[str]:
    result: set[str] = set()
    identifier = info.get("id")
    if isinstance(identifier, str) and VREDDIT_ID_RE.fullmatch(identifier):
        result.add(identifier)
    for key in ("webpage_url", "original_url", "url"):
        value = info.get(key)
        if isinstance(value, str):
            locator = _media_locator(value, "atom_entry_content")
            if locator and locator["locator_kind"] == "reddit_video":
                result.add(locator["native_id"])
    return result


def _apply_vreddit_metadata(
    posts: list[dict[str, Any]], info_paths: list[Path], observed_at: str | None
) -> None:
    if not info_paths:
        if observed_at is not None:
            raise RedditRssError("metadata-observed-at requires at least one v-reddit-info file")
        return
    if observed_at is None:
        raise RedditRssError("metadata-observed-at is required with v-reddit-info")
    observed_datetime = _metadata_observation_datetime(observed_at)
    now = _now_utc_datetime()
    if now.tzinfo is None or now.utcoffset() is None:
        raise RedditRssError("internal clock did not return a timezone-aware instant")
    if observed_datetime > now.astimezone(timezone.utc):
        raise RedditRssError("metadata-observed-at must not be in the future")

    newest_mtime_ns: int | None = None
    newest_mtime_path: Path | None = None
    for path in sorted(info_paths, key=lambda value: str(value.resolve())):
        try:
            mtime_ns = path.stat().st_mtime_ns
        except OSError as error:
            raise RedditRssError(f"cannot stat yt-dlp info JSON {path}: {error}") from error
        if newest_mtime_ns is None or mtime_ns > newest_mtime_ns:
            newest_mtime_ns = mtime_ns
            newest_mtime_path = path
    if newest_mtime_ns is None or newest_mtime_path is None:
        raise RedditRssError("cannot establish a newest v-reddit-info file mtime")
    observed_ns = _whole_seconds_since_epoch(observed_datetime) * 1_000_000_000
    if observed_ns < newest_mtime_ns:
        earliest_second = -(-newest_mtime_ns // 1_000_000_000)
        earliest = (
            datetime(1970, 1, 1, tzinfo=timezone.utc)
            + timedelta(seconds=earliest_second)
        ).strftime("%Y-%m-%dT%H:%M:%SZ")
        raise RedditRssError(
            "metadata-observed-at predates the newest v-reddit-info file mtime; "
            f"earliest whole-second value is {earliest} for {newest_mtime_path}"
        )
    by_id: dict[str, tuple[dict[str, Any], str]] = {}
    by_post: dict[str, list[dict[str, Any]]] = {}
    for post in posts:
        videos = [item for item in post["media_locators"] if item["locator_kind"] == "reddit_video"]
        by_post[post["post_id"]] = videos
        for locator in videos:
            if locator["native_id"] in by_id:
                raise RedditRssError(f"v.redd.it ID {locator['native_id']} appears in multiple posts")
            by_id[locator["native_id"]] = (locator, post["post_id"])

    enriched: set[str] = set()
    for path in sorted(info_paths, key=lambda value: str(value.resolve())):
        raw = _read_bounded(path, MAX_INFO_BYTES, "yt-dlp info JSON")
        try:
            info = json.loads(raw.decode("utf-8"))
        except (UnicodeDecodeError, json.JSONDecodeError) as error:
            raise RedditRssError(f"invalid yt-dlp info JSON {path}: {error}") from error
        if not isinstance(info, dict):
            raise RedditRssError(f"yt-dlp info JSON {path} must be an object")
        candidates = [identifier for identifier in _v_reddit_ids_in_info(info) if identifier in by_id]
        raw_id = info.get("id")
        if not candidates and isinstance(raw_id, str) and raw_id in by_post and len(by_post[raw_id]) == 1:
            candidates = [by_post[raw_id][0]["native_id"]]
        if len(set(candidates)) != 1:
            raise RedditRssError(f"yt-dlp info JSON {path} does not map to exactly one discovered v.redd.it locator")
        media_id = candidates[0]
        if media_id in enriched:
            raise RedditRssError(f"multiple yt-dlp metadata files target v.redd.it/{media_id}")
        duration = info.get("duration")
        if isinstance(duration, bool) or not isinstance(duration, (int, float)) or not math.isfinite(duration) or duration < 0:
            raise RedditRssError(f"yt-dlp info JSON {path} has no valid duration")
        duration_ms = round(duration * 1000)
        if duration_ms > 24 * 60 * 60 * 1000:
            raise RedditRssError(f"yt-dlp info JSON {path} duration exceeds 24 hours")
        availability = info.get("availability")
        access_state = "public" if availability == "public" else "unknown"
        locator = by_id[media_id][0]
        locator["metadata_observation"] = {
            "kind": "public_yt_dlp_info",
            "observed_at": observed_at,
            "payload_sha256": sha256_bytes(raw),
            "duration_ms": duration_ms,
            "access_state": access_state,
        }
        enriched.add(media_id)


def build_discovery_manifest(
    snapshot_path: Path,
    *,
    v_reddit_info_paths: list[Path] | None = None,
    metadata_observed_at: str | None = None,
) -> dict[str, Any]:
    snapshot = validate_snapshot_manifest(snapshot_path, verify_payload=True)
    response = snapshot["response"]
    payload = _read_bounded(snapshot_path.parent / response["payload_file"], 64 * 1024 * 1024, "Atom payload")
    posts = _parse_entries(payload, snapshot["request"]["subreddit"], snapshot["request"]["limit"])
    _apply_vreddit_metadata(posts, v_reddit_info_paths or [], metadata_observed_at)
    body = {
        "schema_version": 1,
        "manifest_kind": "reddit_atom_discovery",
        "generated_at": response["observed_at"],
        "subreddit": snapshot["request"]["subreddit"],
        "assertion_policy": {
            "state": "unreviewed_discovery",
            "titles_are_content_truth": False,
            "comments_retained": False,
            "authors_retained": False,
            "publication_authority": False,
        },
        "snapshot": {
            "snapshot_id": snapshot["snapshot_id"],
            "snapshot_manifest_file": snapshot_path.name,
            "payload_file": response["payload_file"],
            "request_url": snapshot["request"]["url"],
            "final_url": response["final_url"],
            "http_status": response["http_status"],
            "observed_at": response["observed_at"],
            "payload_sha256": response["payload_sha256"],
            "byte_count": response["byte_count"],
        },
        "posts": posts,
    }
    return {"discovery_id": stable_id("rdd", body), **body}


def _validate_locator(locator: Any, post_id: str, label: str) -> dict[str, Any]:
    if not isinstance(locator, dict):
        raise RedditRssError(f"{label} must be an object")
    required = {"locator_kind", "native_id", "canonical_url", "observed_url", "basis", "assertion_state"}
    allowed = required | {"metadata_observation"}
    value = _exact_keys(locator, allowed if "metadata_observation" in locator else required, label)
    if value["locator_kind"] not in {
        "reddit_video",
        "reddit_gallery",
        "youtube_video",
        "internet_archive_item",
        "internet_archive_file",
        "image",
        "external_video",
    }:
        raise RedditRssError(f"{label}.locator_kind is unsupported")
    if not isinstance(value["native_id"], str) or not value["native_id"] or len(value["native_id"]) > 2048:
        raise RedditRssError(f"{label}.native_id is invalid")
    _https_url(value["canonical_url"], f"{label}.canonical_url")
    _https_url(value["observed_url"], f"{label}.observed_url")
    if value["basis"] not in {"atom_entry_link", "atom_entry_content", "atom_media_element"}:
        raise RedditRssError(f"{label}.basis is unsupported")
    if value["assertion_state"] != "unreviewed":
        raise RedditRssError(f"{label}.assertion_state must be unreviewed")
    expected = _media_locator(value["canonical_url"], value["basis"])
    if not expected or expected["locator_kind"] != value["locator_kind"] or expected["native_id"] != value["native_id"]:
        raise RedditRssError(f"{label} canonical URL does not match its stable locator identity")
    if "metadata_observation" in value:
        observation = _exact_keys(
            value["metadata_observation"],
            {"kind", "observed_at", "payload_sha256", "duration_ms", "access_state"},
            f"{label}.metadata_observation",
        )
        if value["locator_kind"] != "reddit_video" or observation["kind"] != "public_yt_dlp_info":
            raise RedditRssError(f"{label}.metadata_observation is only valid for public v.redd.it metadata")
        _utc(observation["observed_at"], f"{label}.metadata_observation.observed_at")
        if not isinstance(observation["payload_sha256"], str) or not SHA256_RE.fullmatch(observation["payload_sha256"]):
            raise RedditRssError(f"{label}.metadata_observation.payload_sha256 is invalid")
        if isinstance(observation["duration_ms"], bool) or not isinstance(observation["duration_ms"], int) or not 0 <= observation["duration_ms"] <= 86_400_000:
            raise RedditRssError(f"{label}.metadata_observation.duration_ms is invalid")
        if observation["access_state"] not in {"public", "unknown"}:
            raise RedditRssError(f"{label}.metadata_observation.access_state is invalid")
    return value


def validate_discovery_manifest(path: Path, *, verify_artifacts: bool = True) -> dict[str, Any]:
    raw = _exact_keys(
        _load_json(path),
        {
            "discovery_id",
            "schema_version",
            "manifest_kind",
            "generated_at",
            "subreddit",
            "assertion_policy",
            "snapshot",
            "posts",
        },
        "discovery",
    )
    if raw["schema_version"] != 1 or raw["manifest_kind"] != "reddit_atom_discovery":
        raise RedditRssError("unsupported Reddit discovery manifest")
    _utc(raw["generated_at"], "discovery.generated_at")
    subreddit = raw["subreddit"]
    if not isinstance(subreddit, str) or not SUBREDDIT_RE.fullmatch(subreddit):
        raise RedditRssError("discovery.subreddit is invalid")
    policy = _exact_keys(
        raw["assertion_policy"],
        {"state", "titles_are_content_truth", "comments_retained", "authors_retained", "publication_authority"},
        "discovery.assertion_policy",
    )
    if policy != {
        "state": "unreviewed_discovery",
        "titles_are_content_truth": False,
        "comments_retained": False,
        "authors_retained": False,
        "publication_authority": False,
    }:
        raise RedditRssError("discovery assertion policy differs from the fail-closed contract")
    snapshot = _exact_keys(
        raw["snapshot"],
        {
            "snapshot_id",
            "snapshot_manifest_file",
            "payload_file",
            "request_url",
            "final_url",
            "http_status",
            "observed_at",
            "payload_sha256",
            "byte_count",
        },
        "discovery.snapshot",
    )
    _safe_basename(snapshot["snapshot_manifest_file"], "discovery.snapshot.snapshot_manifest_file", ".snapshot.json")
    _safe_basename(snapshot["payload_file"], "discovery.snapshot.payload_file", ".atom.xml")
    if snapshot["http_status"] != 200:
        raise RedditRssError("discovery snapshot must have HTTP 200")
    _reddit_feed_url(snapshot["request_url"], subreddit, "discovery.snapshot.request_url")
    _reddit_feed_url(snapshot["final_url"], subreddit, "discovery.snapshot.final_url")
    _utc(snapshot["observed_at"], "discovery.snapshot.observed_at")
    if raw["generated_at"] != snapshot["observed_at"]:
        raise RedditRssError("discovery.generated_at must equal the sealed response observation time")
    if not isinstance(snapshot["payload_sha256"], str) or not SHA256_RE.fullmatch(snapshot["payload_sha256"]):
        raise RedditRssError("discovery.snapshot.payload_sha256 is invalid")
    if isinstance(snapshot["byte_count"], bool) or not isinstance(snapshot["byte_count"], int) or snapshot["byte_count"] < 1:
        raise RedditRssError("discovery.snapshot.byte_count is invalid")

    posts = raw["posts"]
    if not isinstance(posts, list) or len(posts) > 100:
        raise RedditRssError("discovery.posts must be an array of at most 100 entries")
    post_ids: set[str] = set()
    for index, post in enumerate(posts):
        label = f"discovery.posts[{index}]"
        post = _exact_keys(
            post,
            {
                "post_id",
                "permalink",
                "title",
                "published_at",
                "updated_at",
                "timestamp_basis",
                "title_assertion_state",
                "media_locators",
            },
            label,
        )
        post_id = post["post_id"]
        if not isinstance(post_id, str) or not POST_ID_RE.fullmatch(post_id) or post_id in post_ids:
            raise RedditRssError(f"{label}.post_id is invalid or duplicated")
        post_ids.add(post_id)
        permalink = _https_url(post["permalink"], f"{label}.permalink")
        parsed_permalink = urllib.parse.urlsplit(permalink)
        if (parsed_permalink.hostname or "").lower() not in ALLOWED_REDDIT_HOSTS or f"/comments/{post_id}" not in parsed_permalink.path.lower():
            raise RedditRssError(f"{label}.permalink does not match its Reddit post ID")
        if not isinstance(post["title"], str) or not 1 <= len(post["title"]) <= 500:
            raise RedditRssError(f"{label}.title is invalid")
        _utc(post["published_at"], f"{label}.published_at")
        _utc(post["updated_at"], f"{label}.updated_at")
        if post["timestamp_basis"] not in {"atom_published", "atom_updated_fallback"}:
            raise RedditRssError(f"{label}.timestamp_basis is invalid")
        if post["title_assertion_state"] != "unreviewed":
            raise RedditRssError(f"{label}.title_assertion_state must be unreviewed")
        locators = post["media_locators"]
        if not isinstance(locators, list) or len(locators) > 100:
            raise RedditRssError(f"{label}.media_locators must be an array of at most 100 entries")
        locator_ids: set[tuple[str, str]] = set()
        for locator_index, locator in enumerate(locators):
            locator = _validate_locator(locator, post_id, f"{label}.media_locators[{locator_index}]")
            locator_key = (locator["locator_kind"], locator["native_id"])
            if locator_key in locator_ids:
                raise RedditRssError(f"{label} contains a duplicate media locator")
            locator_ids.add(locator_key)
    if [post["post_id"] for post in posts] != sorted(post_ids):
        raise RedditRssError("discovery.posts must be sorted by post_id")

    body = {key: value for key, value in raw.items() if key != "discovery_id"}
    if raw["discovery_id"] != stable_id("rdd", body):
        raise RedditRssError("discovery_id does not match the canonical minimized manifest")
    if verify_artifacts:
        snapshot_path = path.parent / snapshot["snapshot_manifest_file"]
        verified_snapshot = validate_snapshot_manifest(snapshot_path, verify_payload=True)
        response = verified_snapshot["response"]
        comparisons = {
            "snapshot_id": verified_snapshot["snapshot_id"],
            "payload_file": response["payload_file"],
            "request_url": verified_snapshot["request"]["url"],
            "final_url": response["final_url"],
            "http_status": response["http_status"],
            "observed_at": response["observed_at"],
            "payload_sha256": response["payload_sha256"],
            "byte_count": response["byte_count"],
        }
        for key, expected in comparisons.items():
            if snapshot[key] != expected:
                raise RedditRssError(f"discovery snapshot field {key} differs from the sealed snapshot")
        parsed_posts = _parse_entries(
            _read_bounded(path.parent / snapshot["payload_file"], 64 * 1024 * 1024, "Atom payload"),
            subreddit,
            verified_snapshot["request"]["limit"],
        )
        # Enrichment may add metadata_observation, but every Atom-derived field must
        # remain byte-for-byte equivalent to a fresh parse of the sealed response.
        stripped = []
        for post in posts:
            clone = json.loads(json.dumps(post))
            for locator in clone["media_locators"]:
                locator.pop("metadata_observation", None)
            stripped.append(clone)
        if stripped != parsed_posts:
            raise RedditRssError("discovery post fields do not reproduce from the sealed Atom payload")
    return raw


def write_discovery_manifest(
    snapshot_path: Path,
    *,
    out_dir: Path | None = None,
    v_reddit_info_paths: list[Path] | None = None,
    metadata_observed_at: str | None = None,
) -> Path:
    manifest = build_discovery_manifest(
        snapshot_path,
        v_reddit_info_paths=v_reddit_info_paths,
        metadata_observed_at=metadata_observed_at,
    )
    destination = out_dir or snapshot_path.parent
    if not destination.is_absolute():
        raise RedditRssError("out-dir must be an absolute path")
    if destination.resolve() != snapshot_path.parent.resolve():
        raise RedditRssError("discovery manifest must stay beside its sealed snapshot artifacts")
    path = destination / f"{manifest['discovery_id']}.discovery.json"
    _write_immutable(path, canonical_bytes(manifest) + b"\n")
    validate_discovery_manifest(path, verify_artifacts=True)
    return path


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="reddit-rss",
        description="Capture public subreddit Atom metadata without cookies, comments, or media downloads.",
    )
    subparsers = parser.add_subparsers(dest="command", required=True)
    snapshot = subparsers.add_parser("snapshot", help="capture, seal, validate, and minimize one Atom response")
    snapshot.add_argument("--subreddit", required=True)
    snapshot.add_argument("--limit", type=int, default=100)
    snapshot.add_argument("--out-dir", type=Path, required=True)
    snapshot.add_argument("--timeout-seconds", type=int, default=30)
    snapshot.add_argument("--max-bytes", type=int, default=DEFAULT_MAX_FEED_BYTES)

    parse = subparsers.add_parser("parse", help="reproduce a minimized manifest from an existing sealed snapshot")
    parse.add_argument("--snapshot", type=Path, required=True)
    parse.add_argument("--v-reddit-info", type=Path, action="append", default=[])
    parse.add_argument("--metadata-observed-at")

    validate_snapshot = subparsers.add_parser("validate-snapshot")
    validate_snapshot.add_argument("--snapshot", type=Path, required=True)
    validate_discovery = subparsers.add_parser("validate-discovery")
    validate_discovery.add_argument("--discovery", type=Path, required=True)
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    try:
        if args.command == "snapshot":
            snapshot_path = capture_snapshot(
                subreddit=args.subreddit,
                limit=args.limit,
                out_dir=args.out_dir,
                timeout_seconds=args.timeout_seconds,
                max_bytes=args.max_bytes,
            )
            discovery_path = write_discovery_manifest(snapshot_path)
            result = {
                "snapshot_manifest_path": str(snapshot_path),
                "discovery_manifest_path": str(discovery_path),
                "media_downloaded": False,
                "publication_authority": False,
            }
        elif args.command == "parse":
            discovery_path = write_discovery_manifest(
                args.snapshot,
                v_reddit_info_paths=args.v_reddit_info,
                metadata_observed_at=args.metadata_observed_at,
            )
            result = validate_discovery_manifest(discovery_path, verify_artifacts=True)
        elif args.command == "validate-snapshot":
            result = validate_snapshot_manifest(args.snapshot, verify_payload=True)
        else:
            result = validate_discovery_manifest(args.discovery, verify_artifacts=True)
    except RedditRssError as error:
        print(f"error: {error}", file=sys.stderr)
        return 2
    print(json.dumps(result, ensure_ascii=False, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
