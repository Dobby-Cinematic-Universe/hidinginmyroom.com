"""Strict importer for minimized, public Reddit Atom discovery manifests.

The importer creates private catalog candidates and review work only.  It never
downloads media, imports comments or authors, or creates publication decisions.
"""

from __future__ import annotations

import hashlib
import json
import re
import sqlite3
import urllib.parse
import xml.etree.ElementTree as ET
from collections import Counter
from pathlib import Path
from typing import Any

from .ids import recording_id, source_id, stable_id, title_from_media_filename
from .importers import (
    _add_external_id,
    _attach_recording_source,
    _begin_batch,
    _complete_batch,
    _relate_sources,
    _upsert_recording,
    _upsert_source,
    canonical_json,
    sha256_bytes,
)
from .db import transaction


SHA256_RE = re.compile(r"^[0-9a-f]{64}$")
UTC_RE = re.compile(r"^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(?:\.\d+)?Z$")
SUBREDDIT_RE = re.compile(r"^[A-Za-z0-9_]{2,21}$")
POST_ID_RE = re.compile(r"^[a-z0-9]{5,16}$")
YOUTUBE_ID_RE = re.compile(r"^[A-Za-z0-9_-]{11}$")
VREDDIT_ID_RE = re.compile(r"^[A-Za-z0-9_-]{5,64}$")
ALLOWED_REDDIT_HOSTS = frozenset({"reddit.com", "www.reddit.com", "old.reddit.com"})
MAX_MANIFEST_BYTES = 8 * 1024 * 1024
MAX_PAYLOAD_BYTES = 64 * 1024 * 1024
ACCEPT = "application/atom+xml, application/xml;q=0.9, text/xml;q=0.8"
USER_AGENT = "hidinginmyroom-corpus-reddit-rss/1.0 (public metadata research)"


class RedditDiscoveryImportError(ValueError):
    pass


def _canonical_bytes(value: Any) -> bytes:
    return json.dumps(
        value,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
        allow_nan=False,
    ).encode("utf-8")


def _derived_id(prefix: str, body: Any) -> str:
    return f"{prefix}_{hashlib.sha256(_canonical_bytes(body)).hexdigest()[:32]}"


def _exact(value: Any, expected: set[str], label: str) -> dict[str, Any]:
    if not isinstance(value, dict):
        raise RedditDiscoveryImportError(f"{label} must be an object")
    missing = sorted(expected - set(value))
    unknown = sorted(set(value) - expected)
    if missing or unknown:
        raise RedditDiscoveryImportError(
            f"{label} keys differ from the exact contract; missing={missing}, unknown={unknown}"
        )
    return value


def _read(path: Path, maximum: int, label: str) -> bytes:
    if not path.is_file():
        raise RedditDiscoveryImportError(f"{label} does not exist: {path}")
    size = path.stat().st_size
    if size > maximum:
        raise RedditDiscoveryImportError(f"{label} exceeds {maximum} bytes")
    return path.read_bytes()


def _basename(value: Any, suffix: str, label: str) -> str:
    if not isinstance(value, str) or Path(value).name != value or not value.endswith(suffix):
        raise RedditDiscoveryImportError(f"{label} must be a sibling basename ending in {suffix}")
    return value


def _https(value: Any, label: str) -> urllib.parse.SplitResult:
    if not isinstance(value, str) or len(value) > 4096:
        raise RedditDiscoveryImportError(f"{label} must be an HTTPS URL")
    try:
        parsed = urllib.parse.urlsplit(value)
    except ValueError as error:
        raise RedditDiscoveryImportError(f"{label} is invalid") from error
    if parsed.scheme != "https" or not parsed.hostname or parsed.username or parsed.password:
        raise RedditDiscoveryImportError(f"{label} must be HTTPS without user information")
    return parsed


def _reddit_url(value: Any, subreddit: str, label: str, *, feed: bool) -> str:
    parsed = _https(value, label)
    if (parsed.hostname or "").lower() not in ALLOWED_REDDIT_HOSTS:
        raise RedditDiscoveryImportError(f"{label} must remain on reddit.com")
    if feed and (f"/r/{subreddit.lower()}/" not in parsed.path.lower() or ".rss" not in parsed.path.lower()):
        raise RedditDiscoveryImportError(f"{label} is not the declared subreddit feed")
    return value


def _validate_locator(value: Any, label: str) -> dict[str, Any]:
    if not isinstance(value, dict):
        raise RedditDiscoveryImportError(f"{label} must be an object")
    required = {
        "locator_kind",
        "native_id",
        "canonical_url",
        "observed_url",
        "basis",
        "assertion_state",
    }
    expected = required | ({"metadata_observation"} if "metadata_observation" in value else set())
    value = _exact(value, expected, label)
    kind = value["locator_kind"]
    if kind not in {
        "reddit_video",
        "reddit_gallery",
        "youtube_video",
        "internet_archive_item",
        "internet_archive_file",
        "image",
        "external_video",
    }:
        raise RedditDiscoveryImportError(f"{label}.locator_kind is unsupported")
    native_id = value["native_id"]
    if not isinstance(native_id, str) or not native_id or len(native_id) > 2048:
        raise RedditDiscoveryImportError(f"{label}.native_id is invalid")
    canonical = _https(value["canonical_url"], f"{label}.canonical_url")
    _https(value["observed_url"], f"{label}.observed_url")
    if value["basis"] not in {"atom_entry_link", "atom_entry_content", "atom_media_element"}:
        raise RedditDiscoveryImportError(f"{label}.basis is invalid")
    if value["assertion_state"] != "unreviewed":
        raise RedditDiscoveryImportError(f"{label}.assertion_state must be unreviewed")

    host = (canonical.hostname or "").lower()
    segments = [urllib.parse.unquote(part) for part in canonical.path.split("/") if part]
    if kind == "reddit_video":
        if host != "v.redd.it" or not segments or segments[0] != native_id or not VREDDIT_ID_RE.fullmatch(native_id):
            raise RedditDiscoveryImportError(f"{label} v.redd.it identity does not match its canonical URL")
    elif kind == "reddit_gallery":
        if host not in ALLOWED_REDDIT_HOSTS or len(segments) < 2 or segments[0] != "gallery" or segments[1] != native_id:
            raise RedditDiscoveryImportError(f"{label} Reddit gallery identity does not match its canonical URL")
    elif kind == "youtube_video":
        query_id = urllib.parse.parse_qs(canonical.query).get("v", [None])[0]
        if host not in {"youtube.com", "www.youtube.com"} or canonical.path != "/watch" or query_id != native_id or not YOUTUBE_ID_RE.fullmatch(native_id):
            raise RedditDiscoveryImportError(f"{label} YouTube identity does not match its canonical URL")
    elif kind == "internet_archive_item":
        if host not in {"archive.org", "www.archive.org"} or len(segments) != 2 or segments[0] != "details" or segments[1] != native_id:
            raise RedditDiscoveryImportError(f"{label} Archive.org item identity does not match its canonical URL")
    elif kind == "internet_archive_file":
        if host not in {"archive.org", "www.archive.org"} or len(segments) < 3 or segments[0] != "download" or f"{segments[1]}/{'/'.join(segments[2:])}" != native_id:
            raise RedditDiscoveryImportError(f"{label} Archive.org file identity does not match its canonical URL")
    elif native_id != f"{host}{canonical.path}":
        raise RedditDiscoveryImportError(f"{label} web-media identity does not match its canonical URL")

    if "metadata_observation" in value:
        observation = _exact(
            value["metadata_observation"],
            {"kind", "observed_at", "payload_sha256", "duration_ms", "access_state"},
            f"{label}.metadata_observation",
        )
        if kind != "reddit_video" or observation["kind"] != "public_yt_dlp_info":
            raise RedditDiscoveryImportError(f"{label} metadata enrichment is only valid for v.redd.it")
        if not isinstance(observation["observed_at"], str) or not UTC_RE.fullmatch(observation["observed_at"]):
            raise RedditDiscoveryImportError(f"{label}.metadata_observation.observed_at is invalid")
        if not isinstance(observation["payload_sha256"], str) or not SHA256_RE.fullmatch(observation["payload_sha256"]):
            raise RedditDiscoveryImportError(f"{label}.metadata_observation.payload_sha256 is invalid")
        duration = observation["duration_ms"]
        if isinstance(duration, bool) or not isinstance(duration, int) or not 0 <= duration <= 86_400_000:
            raise RedditDiscoveryImportError(f"{label}.metadata_observation.duration_ms is invalid")
        if observation["access_state"] not in {"public", "unknown"}:
            raise RedditDiscoveryImportError(f"{label}.metadata_observation.access_state is invalid")
    return value


def validate_reddit_discovery_manifest(path: Path) -> dict[str, Any]:
    """Validate exact manifest shape plus the sibling sealed snapshot artifacts."""

    path = Path(path)
    raw_bytes = _read(path, MAX_MANIFEST_BYTES, "Reddit discovery manifest")
    try:
        manifest = json.loads(raw_bytes.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError) as error:
        raise RedditDiscoveryImportError(f"invalid Reddit discovery JSON: {error}") from error
    manifest = _exact(
        manifest,
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
        "manifest",
    )
    if manifest["schema_version"] != 1 or manifest["manifest_kind"] != "reddit_atom_discovery":
        raise RedditDiscoveryImportError("unsupported Reddit discovery manifest")
    if not isinstance(manifest["discovery_id"], str) or not re.fullmatch(
        r"rdd_[0-9a-f]{32}", manifest["discovery_id"]
    ):
        raise RedditDiscoveryImportError("manifest.discovery_id is invalid")
    if not isinstance(manifest["generated_at"], str) or not UTC_RE.fullmatch(manifest["generated_at"]):
        raise RedditDiscoveryImportError("manifest.generated_at is invalid")
    subreddit = manifest["subreddit"]
    if not isinstance(subreddit, str) or not SUBREDDIT_RE.fullmatch(subreddit):
        raise RedditDiscoveryImportError("manifest.subreddit is invalid")
    policy = _exact(
        manifest["assertion_policy"],
        {"state", "titles_are_content_truth", "comments_retained", "authors_retained", "publication_authority"},
        "manifest.assertion_policy",
    )
    if policy != {
        "state": "unreviewed_discovery",
        "titles_are_content_truth": False,
        "comments_retained": False,
        "authors_retained": False,
        "publication_authority": False,
    }:
        raise RedditDiscoveryImportError("manifest assertion policy is not fail closed")

    snapshot = _exact(
        manifest["snapshot"],
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
        "manifest.snapshot",
    )
    snapshot_manifest_name = _basename(snapshot["snapshot_manifest_file"], ".snapshot.json", "manifest.snapshot.snapshot_manifest_file")
    payload_name = _basename(snapshot["payload_file"], ".atom.xml", "manifest.snapshot.payload_file")
    if not isinstance(snapshot["snapshot_id"], str) or not re.fullmatch(
        r"rrs_[0-9a-f]{32}", snapshot["snapshot_id"]
    ):
        raise RedditDiscoveryImportError("manifest.snapshot.snapshot_id is invalid")
    if snapshot_manifest_name != f"{snapshot['snapshot_id']}.snapshot.json" or payload_name != f"{snapshot['snapshot_id']}.atom.xml":
        raise RedditDiscoveryImportError("manifest snapshot filenames do not match snapshot_id")
    if snapshot["http_status"] != 200:
        raise RedditDiscoveryImportError("manifest snapshot must be an HTTP 200 response")
    _reddit_url(snapshot["request_url"], subreddit, "manifest.snapshot.request_url", feed=True)
    _reddit_url(snapshot["final_url"], subreddit, "manifest.snapshot.final_url", feed=True)
    if not isinstance(snapshot["observed_at"], str) or not UTC_RE.fullmatch(snapshot["observed_at"]):
        raise RedditDiscoveryImportError("manifest.snapshot.observed_at is invalid")
    if manifest["generated_at"] != snapshot["observed_at"]:
        raise RedditDiscoveryImportError("generated_at must equal the sealed response observed_at")
    if not isinstance(snapshot["payload_sha256"], str) or not SHA256_RE.fullmatch(snapshot["payload_sha256"]):
        raise RedditDiscoveryImportError("manifest.snapshot.payload_sha256 is invalid")
    if isinstance(snapshot["byte_count"], bool) or not isinstance(snapshot["byte_count"], int) or not 1 <= snapshot["byte_count"] <= MAX_PAYLOAD_BYTES:
        raise RedditDiscoveryImportError("manifest.snapshot.byte_count is invalid")

    posts = manifest["posts"]
    if not isinstance(posts, list) or len(posts) > 100:
        raise RedditDiscoveryImportError("manifest.posts must have at most 100 entries")
    seen_posts: set[str] = set()
    for post_index, post in enumerate(posts):
        label = f"manifest.posts[{post_index}]"
        post = _exact(
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
        if not isinstance(post_id, str) or not POST_ID_RE.fullmatch(post_id) or post_id in seen_posts:
            raise RedditDiscoveryImportError(f"{label}.post_id is invalid or duplicated")
        seen_posts.add(post_id)
        permalink = _reddit_url(post["permalink"], subreddit, f"{label}.permalink", feed=False)
        if f"/comments/{post_id}" not in urllib.parse.urlsplit(permalink).path.lower():
            raise RedditDiscoveryImportError(f"{label}.permalink does not match the post ID")
        if not isinstance(post["title"], str) or not 1 <= len(post["title"]) <= 500:
            raise RedditDiscoveryImportError(f"{label}.title is invalid")
        for key in ("published_at", "updated_at"):
            if not isinstance(post[key], str) or not UTC_RE.fullmatch(post[key]):
                raise RedditDiscoveryImportError(f"{label}.{key} is invalid")
        if post["timestamp_basis"] not in {"atom_published", "atom_updated_fallback"}:
            raise RedditDiscoveryImportError(f"{label}.timestamp_basis is invalid")
        if post["title_assertion_state"] != "unreviewed":
            raise RedditDiscoveryImportError(f"{label}.title_assertion_state must be unreviewed")
        locators = post["media_locators"]
        if not isinstance(locators, list) or len(locators) > 100:
            raise RedditDiscoveryImportError(f"{label}.media_locators is invalid")
        seen_locators: set[tuple[str, str]] = set()
        for locator_index, locator in enumerate(locators):
            locator = _validate_locator(locator, f"{label}.media_locators[{locator_index}]")
            key = (locator["locator_kind"], locator["native_id"])
            if key in seen_locators:
                raise RedditDiscoveryImportError(f"{label} contains a duplicate locator")
            seen_locators.add(key)
    if [post["post_id"] for post in posts] != sorted(seen_posts):
        raise RedditDiscoveryImportError("manifest.posts must be sorted by post_id")
    body = {key: value for key, value in manifest.items() if key != "discovery_id"}
    if manifest["discovery_id"] != _derived_id("rdd", body):
        raise RedditDiscoveryImportError("manifest.discovery_id does not match its canonical contents")

    snapshot_manifest_path = path.parent / snapshot_manifest_name
    snapshot_raw = _read(snapshot_manifest_path, MAX_MANIFEST_BYTES, "Reddit snapshot manifest")
    try:
        snapshot_manifest = json.loads(snapshot_raw.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError) as error:
        raise RedditDiscoveryImportError(f"invalid Reddit snapshot JSON: {error}") from error
    snapshot_manifest = _exact(
        snapshot_manifest,
        {"schema_version", "snapshot_id", "source_kind", "request", "response", "privacy"},
        "sealed_snapshot",
    )
    request = _exact(
        snapshot_manifest["request"],
        {"method", "url", "subreddit", "sort", "limit", "accept", "user_agent", "cookies_sent", "authorization_sent", "started_at"},
        "sealed_snapshot.request",
    )
    response = _exact(
        snapshot_manifest["response"],
        {"http_status", "final_url", "observed_at", "content_type", "payload_sha256", "byte_count", "payload_file"},
        "sealed_snapshot.response",
    )
    privacy = _exact(
        snapshot_manifest["privacy"],
        {"public_feed_only", "comments_collected", "authors_extracted", "publication_authority"},
        "sealed_snapshot.privacy",
    )
    if snapshot_manifest["schema_version"] != 1 or snapshot_manifest["source_kind"] != "reddit_atom_feed":
        raise RedditDiscoveryImportError("sealed snapshot schema is unsupported")
    if request["method"] != "GET" or request["subreddit"] != subreddit or request["sort"] != "new":
        raise RedditDiscoveryImportError("sealed snapshot request differs from the discovery context")
    limit = request["limit"]
    if isinstance(limit, bool) or not isinstance(limit, int) or not 1 <= limit <= 100:
        raise RedditDiscoveryImportError("sealed snapshot request limit is invalid")
    canonical_request_url = f"https://www.reddit.com/r/{subreddit}/new/.rss?limit={limit}"
    if request["url"] != canonical_request_url:
        raise RedditDiscoveryImportError("sealed snapshot request URL is not canonical")
    if request["accept"] != ACCEPT or request["user_agent"] != USER_AGENT:
        raise RedditDiscoveryImportError("sealed snapshot headers differ from the public no-auth contract")
    if not isinstance(request["started_at"], str) or not UTC_RE.fullmatch(request["started_at"]):
        raise RedditDiscoveryImportError("sealed snapshot request started_at is invalid")
    if request["cookies_sent"] is not False or request["authorization_sent"] is not False:
        raise RedditDiscoveryImportError("authenticated Reddit snapshots are forbidden")
    if privacy != {
        "public_feed_only": True,
        "comments_collected": False,
        "authors_extracted": False,
        "publication_authority": False,
    }:
        raise RedditDiscoveryImportError("sealed snapshot privacy policy is not fail closed")
    if not isinstance(response["content_type"], str) or len(response["content_type"]) > 200:
        raise RedditDiscoveryImportError("sealed snapshot content_type is invalid")
    snapshot_identity = {
        "subreddit": subreddit.lower(),
        "request_url": request["url"],
        "final_url": response["final_url"],
        "observed_at": response["observed_at"],
        "payload_sha256": response["payload_sha256"],
    }
    if snapshot_manifest["snapshot_id"] != _derived_id("rrs", snapshot_identity):
        raise RedditDiscoveryImportError("sealed snapshot_id does not match the response identity")
    sealed_comparison = {
        "snapshot_id": snapshot_manifest["snapshot_id"],
        "payload_file": response["payload_file"],
        "request_url": request["url"],
        "final_url": response["final_url"],
        "http_status": response["http_status"],
        "observed_at": response["observed_at"],
        "payload_sha256": response["payload_sha256"],
        "byte_count": response["byte_count"],
    }
    for key, value in sealed_comparison.items():
        if snapshot[key] != value:
            raise RedditDiscoveryImportError(f"sealed snapshot field {key} differs from the discovery manifest")
    payload = _read(path.parent / payload_name, MAX_PAYLOAD_BYTES, "Reddit Atom payload")
    if len(payload) != snapshot["byte_count"] or sha256_bytes(payload) != snapshot["payload_sha256"]:
        raise RedditDiscoveryImportError("Reddit Atom payload does not match its sealed hash and byte count")
    if b"<!DOCTYPE" in payload[:4096].upper() or b"<!ENTITY" in payload[:4096].upper():
        raise RedditDiscoveryImportError("Reddit Atom payload contains a forbidden DTD or entity declaration")
    try:
        root = ET.fromstring(payload)
    except ET.ParseError as error:
        raise RedditDiscoveryImportError(f"Reddit Atom payload is invalid XML: {error}") from error
    if root.tag != "{http://www.w3.org/2005/Atom}feed":
        raise RedditDiscoveryImportError("Reddit payload is not an Atom feed")
    if len(posts) > limit:
        raise RedditDiscoveryImportError("discovery contains more posts than the sealed request limit")
    if len(root.findall("{http://www.w3.org/2005/Atom}entry")) != len(posts):
        raise RedditDiscoveryImportError("discovery post count differs from the sealed Atom entry count")
    return manifest


def _review_task(
    connection: sqlite3.Connection,
    *,
    task_kind: str,
    target_type: str,
    target_id: str,
    reason: str,
    priority: int,
    observed_at: str,
) -> None:
    task_id = stable_id("rtk", task_kind, target_type, target_id)
    connection.execute(
        """
        INSERT OR IGNORE INTO review_tasks(
            review_task_id, task_kind, target_type, target_id, reason,
            priority, status, created_at, updated_at
        ) VALUES(?, ?, ?, ?, ?, ?, 'open', ?, ?)
        """,
        (task_id, task_kind, target_type, target_id, reason, priority, observed_at, observed_at),
    )


def _locator_source(locator: dict[str, Any]) -> tuple[str, str, str, str, str | None]:
    kind = locator["locator_kind"]
    native_id = locator["native_id"]
    if kind == "reddit_video":
        return source_id("reddit", "reddit_video", native_id), "reddit", "reddit_video", native_id, None
    if kind == "reddit_gallery":
        return source_id("reddit", "reddit_gallery", native_id), "reddit", "reddit_gallery", native_id, None
    if kind == "youtube_video":
        return source_id("youtube", "youtube_video", native_id), "youtube", "youtube_video", native_id, None
    if kind == "internet_archive_item":
        return source_id("internet_archive", "archive_item", native_id), "internet_archive", "archive_item", native_id, None
    if kind == "internet_archive_file":
        filename = native_id.split("/", 1)[1]
        return (
            source_id("internet_archive", "archive_media_file", native_id),
            "internet_archive",
            "archive_media_file",
            native_id,
            title_from_media_filename(filename),
        )
    if kind == "image":
        return source_id("web", "image", native_id), "web", "image", native_id, None
    return source_id("web", "external_video", native_id), "web", "external_video", native_id, None


def import_reddit_discovery_manifest(
    connection: sqlite3.Connection, manifest_path: Path
) -> dict[str, Any]:
    """Import public-RSS leads into private metadata/review tables only."""

    manifest_path = Path(manifest_path)
    manifest = validate_reddit_discovery_manifest(manifest_path)
    digest = sha256_bytes(manifest_path.read_bytes())
    observed_at = manifest["snapshot"]["observed_at"]
    snapshot = manifest["snapshot"]
    with transaction(connection):
        batch_id, existing = _begin_batch(
            connection,
            "reddit_atom_discovery",
            digest,
            observed_at[:10],
            observed_at,
        )
        if existing is not None:
            return existing
        statistics = Counter()
        subreddit = manifest["subreddit"]
        feed_native_id = f"{subreddit}:new"
        feed_source = source_id("reddit", "subreddit_atom_feed", feed_native_id)
        _upsert_source(
            connection,
            source=feed_source,
            platform="reddit",
            source_kind="subreddit_atom_feed",
            native_id=feed_native_id,
            canonical_url=snapshot["request_url"],
            title=f"r/{subreddit} public new-post Atom feed",
            observed_at=observed_at,
            access_state="public",
            review_state="unreviewed",
            batch_id=batch_id,
            metadata={
                "discovery_state": "public_atom_snapshot",
                "snapshot_id": snapshot["snapshot_id"],
                "comments_collected": False,
                "authors_extracted": False,
                "publication_authority": False,
            },
        )
        database_snapshot_id = stable_id(
            "ssn", feed_source, observed_at, snapshot["payload_sha256"]
        )
        connection.execute(
            """
            INSERT OR IGNORE INTO source_snapshots(
                source_snapshot_id, source_id, observed_at, request_url, final_url,
                http_status, payload_sha256, artifact_path, metadata_json,
                import_batch_id
            ) VALUES(?, ?, ?, ?, ?, 200, ?, ?, ?, ?)
            """,
            (
                database_snapshot_id,
                feed_source,
                observed_at,
                snapshot["request_url"],
                snapshot["final_url"],
                snapshot["payload_sha256"],
                str((manifest_path.parent / snapshot["payload_file"]).resolve()),
                canonical_json(
                    {
                        "snapshot_id": snapshot["snapshot_id"],
                        "byte_count": snapshot["byte_count"],
                        "discovery_id": manifest["discovery_id"],
                        "unreviewed_discovery_only": True,
                    }
                ),
                batch_id,
            ),
        )
        statistics["feed_snapshots"] += 1

        for post in manifest["posts"]:
            post_id = post["post_id"]
            post_source = source_id("reddit", "post", post_id)
            _upsert_source(
                connection,
                source=post_source,
                platform="reddit",
                source_kind="post",
                native_id=post_id,
                parent_source=feed_source,
                canonical_url=post["permalink"],
                title=post["title"],
                published_at=post["published_at"],
                observed_at=observed_at,
                access_state="public",
                review_state="unreviewed",
                batch_id=batch_id,
                metadata={
                    "discovery_state": "atom_entry_unreviewed",
                    "title_assertion_state": "unreviewed",
                    "title_is_content_truth": False,
                    "timestamp_basis": post["timestamp_basis"],
                    "snapshot_id": snapshot["snapshot_id"],
                },
            )
            _add_external_id(
                connection,
                object_type="source",
                object_id=post_source,
                namespace="reddit_post_id",
                value=post_id,
                basis="Stable post ID parsed from a sealed public subreddit Atom entry",
                batch_id=batch_id,
                observed_at=observed_at,
                source=post_source,
            )
            _relate_sources(
                connection,
                from_source=post_source,
                relation_kind="listed_in",
                to_source=feed_source,
                basis="Post entry present in the sealed public subreddit Atom response",
                batch_id=batch_id,
                observed_at=observed_at,
                metadata={
                    "snapshot_id": snapshot["snapshot_id"],
                    "contextual_discovery_only": True,
                },
            )
            statistics["posts"] += 1

            for locator in post["media_locators"]:
                target, platform, source_kind, native_id, derived_title = _locator_source(locator)
                metadata_observation = locator.get("metadata_observation")
                access_state = (
                    "public"
                    if locator["locator_kind"] in {"reddit_video", "reddit_gallery"}
                    else "unknown"
                )
                if metadata_observation and metadata_observation["access_state"] == "public":
                    access_state = "public"
                _upsert_source(
                    connection,
                    source=target,
                    platform=platform,
                    source_kind=source_kind,
                    native_id=native_id,
                    canonical_url=locator["canonical_url"],
                    title=(post["title"] if locator["locator_kind"] in {"reddit_video", "reddit_gallery"} else derived_title),
                    observed_at=observed_at,
                    access_state=access_state,
                    review_state="unreviewed",
                    batch_id=batch_id,
                    metadata={
                        "discovery_state": "reddit_atom_media_locator",
                        "assertion_state": "unreviewed",
                        "context_post_id": post_id,
                        "context_snapshot_id": snapshot["snapshot_id"],
                        "locator_basis": locator["basis"],
                        "observed_url": locator["observed_url"],
                        "content_verified": False,
                        "metadata_observation": metadata_observation,
                    },
                )
                _relate_sources(
                    connection,
                    from_source=post_source,
                    relation_kind="contains_media_locator",
                    to_source=target,
                    basis=(
                        f"Public Atom entry {post_id} contained this URL via {locator['basis']}; "
                        "the relationship and title remain unreviewed contextual discovery"
                    ),
                    batch_id=batch_id,
                    observed_at=observed_at,
                    metadata={
                        "snapshot_id": snapshot["snapshot_id"],
                        "locator_kind": locator["locator_kind"],
                        "assertion_state": "unreviewed",
                        "content_verified": False,
                    },
                )
                statistics[f"locators_{locator['locator_kind']}"] += 1

                if locator["locator_kind"] == "reddit_video":
                    duration = metadata_observation["duration_ms"] if metadata_observation else None
                    recording = _upsert_recording(
                        connection,
                        canonical_key=f"reddit:video:{native_id}",
                        title=post["title"],
                        date_label=post["published_at"][:10],
                        date_basis="reddit_atom_entry_contextual",
                        duration=duration,
                        recording_type="video",
                        observed_at=observed_at,
                        batch_id=batch_id,
                        review_state="unreviewed",
                        metadata={
                            "identity_basis": "stable_v_reddit_media_id",
                            "discovery_state": "unreviewed_candidate",
                            "context_post_id": post_id,
                            "title_is_content_truth": False,
                            "duration_basis": (
                                "public_yt_dlp_info" if metadata_observation else "unknown"
                            ),
                        },
                    )
                    _attach_recording_source(
                        connection,
                        recording=recording,
                        source=target,
                        role="reddit_video_discovery_candidate",
                        method="stable_v_reddit_media_id_from_public_atom_locator",
                        confidence_state="candidate",
                        metadata={
                            "snapshot_id": snapshot["snapshot_id"],
                            "content_verified": False,
                        },
                    )
                    _add_external_id(
                        connection,
                        object_type="recording",
                        object_id=recording,
                        namespace="v_reddit_media_id",
                        value=native_id,
                        basis="Stable v.redd.it path ID in a sealed public Atom entry",
                        batch_id=batch_id,
                        observed_at=observed_at,
                        source=target,
                    )
                    _review_task(
                        connection,
                        task_kind="reddit_video_discovery_candidate",
                        target_type="recording",
                        target_id=recording,
                        reason=(
                            f"Review v.redd.it/{native_id} for relevance, completeness, rights, "
                            "privacy, and sensitivity before acquisition or publication"
                        ),
                        priority=70,
                        observed_at=observed_at,
                    )
                    statistics["recording_candidates"] += 1
                else:
                    task_kind = (
                        "reddit_external_locator_discovery"
                        if locator["locator_kind"] in {
                            "youtube_video",
                            "internet_archive_item",
                            "internet_archive_file",
                        }
                        else "reddit_media_locator_discovery"
                    )
                    _review_task(
                        connection,
                        task_kind=task_kind,
                        target_type="source",
                        target_id=target,
                        reason=(
                            f"Review the {locator['locator_kind']} locator linked by Reddit post "
                            f"{post_id}; public Atom presence is contextual discovery, not content truth"
                        ),
                        priority=85,
                        observed_at=observed_at,
                    )
        statistics["publication_decisions_created"] = 0
        statistics["media_objects_created"] = 0
        result = dict(sorted(statistics.items()))
        _complete_batch(connection, batch_id, observed_at, result)
        return result
