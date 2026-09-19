"""Stable identifiers and media-label normalization."""

from __future__ import annotations

import re
import unicodedata
import uuid


NAMESPACE = uuid.UUID("8b92c560-551c-5bc7-a8b2-cec3aa8fb730")
VIDEO_SUFFIX_RE = re.compile(r"-([A-Za-z0-9_-]{11})(?:-\d+)?$")
VIDEO_EXTENSION_RE = re.compile(r"\.(?:mp4|webm|ogv|mkv|mov|m4v)$", re.IGNORECASE)


def stable_id(prefix: str, *parts: object) -> str:
    """Return a deterministic, opaque identifier for a canonical key."""

    key = "\x1f".join(str(part) for part in parts)
    return f"{prefix}_{uuid.uuid5(NAMESPACE, key).hex}"


def source_id(platform: str, source_kind: str, native_id: str) -> str:
    return stable_id("src", platform, source_kind, native_id)


def recording_id(canonical_key: str) -> str:
    return stable_id("rec", canonical_key)


def normalize_media_base(filename: str) -> str:
    value = unicodedata.normalize("NFC", filename)
    value = re.sub(r"\.ia(?=\.[^.]+$)", "", value, flags=re.IGNORECASE)
    return VIDEO_EXTENSION_RE.sub("", value)


def platform_video_id(filename: str | None) -> str | None:
    if not filename:
        return None
    match = VIDEO_SUFFIX_RE.search(normalize_media_base(filename))
    return match.group(1) if match else None


def recording_key_for_archive(item: str, filename: str) -> str:
    video_id = platform_video_id(filename)
    if video_id:
        return f"youtube:video:{video_id}"
    return f"internet_archive:{item}:{normalize_media_base(filename)}"


def title_from_media_filename(filename: str) -> str:
    base = normalize_media_base(filename)
    base = re.sub(r"^\d{8}-", "", base)
    video_id = platform_video_id(filename)
    if video_id:
        base = re.sub(rf"-{re.escape(video_id)}(?:-\d+)?$", "", base)
    return " ".join(base.split()) or filename


def date_label_from_filename(filename: str) -> str | None:
    if not re.match(r"^\d{8}", filename):
        return None
    year, month, day = filename[:4], filename[4:6], filename[6:8]
    try:
        # Simple validation without coupling identifiers to local time zones.
        if not (1 <= int(month) <= 12 and 1 <= int(day) <= 31):
            return None
    except ValueError:
        return None
    return f"{year}-{month}-{day}"


def slugify(value: str) -> str:
    normalized = unicodedata.normalize("NFKD", value)
    ascii_value = normalized.encode("ascii", "ignore").decode("ascii").lower()
    slug = re.sub(r"[^a-z0-9]+", "-", ascii_value).strip("-")
    return slug[:80] or "recording"


def recording_slug(recording: str, title: str, date_label: str | None) -> str:
    prefix = date_label or "undated"
    return f"{prefix}-{slugify(title)}-{recording[-8:]}"

