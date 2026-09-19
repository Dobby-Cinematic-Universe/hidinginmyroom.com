"""Opt-in temp.sh WAV uploads; no network activity until explicitly called.

The homepage documents multipart POST /upload, a 4 GB limit and three-day
expiry. Its linked integration examples were unavailable on 2026-09-06, so
this adapter accepts only a plain returned URL and verifies the actual media
response before returning it to the transcription caller. Live compatibility
has not been tested. temp.sh is public file sharing, not private storage.
"""

from __future__ import annotations

import hashlib
import http.client
from html.parser import HTMLParser
import os
from pathlib import Path
import re
import secrets
import stat
import urllib.error
import urllib.parse
import urllib.request

try:
    from .salad_transcription_client import CloudClientError, _retry_after, _transport
except ImportError:
    from salad_transcription_client import CloudClientError, _retry_after, _transport


MAX_UPLOAD_BYTES = 300_000_000
MAX_URL_RESPONSE_BYTES = 8192
MAX_INSPECTION_BYTES = 64 * 1024
PREFIX_BYTES = 4096
TIMEOUT_SECONDS = 60
UPLOAD_URL = "https://temp.sh/upload"


def _temp_url(value: str) -> str:
    if (not isinstance(value, str) or not 1 <= len(value) <= MAX_URL_RESPONSE_BYTES
            or any(ord(character) < 33 or ord(character) > 126 for character in value)
            or "\\" in value):
        raise CloudClientError("invalid temp.sh media URL")
    try:
        parsed = urllib.parse.urlsplit(value)
        decoded_path = urllib.parse.unquote(parsed.path, errors="strict")
        if (parsed.scheme != "https" or parsed.netloc != "temp.sh" or parsed.fragment
                or not decoded_path.startswith("/") or decoded_path == "/"
                or "\\" in decoded_path or any(part in {".", ".."} for part in decoded_path.split("/"))
                or any(ord(character) < 32 or ord(character) == 127 for character in decoded_path)):
            raise ValueError("unexpected URL")
    except (ValueError, UnicodeError):
        raise CloudClientError("invalid temp.sh media URL") from None
    return value


def _upload_bytes(path: Path, expected_sha256: str, expected_byte_count: int) -> bytes:
    if (not isinstance(expected_sha256, str) or re.fullmatch(r"[0-9a-f]{64}", expected_sha256) is None
            or type(expected_byte_count) is not int or not 12 <= expected_byte_count <= MAX_UPLOAD_BYTES):
        raise CloudClientError("invalid temp.sh upload binding or size")
    directory = descriptor = None
    try:
        absolute = Path(path).absolute()
        if ".." in absolute.parts:
            raise ValueError("parent traversal")
        directory = os.open(absolute.anchor, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW)
        for part in absolute.parts[1:-1]:
            child = os.open(part, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW, dir_fd=directory)
            os.close(directory)
            directory = child
        descriptor = os.open(absolute.name, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK, dir_fd=directory)
        before = os.fstat(descriptor)
        if (not stat.S_ISREG(before.st_mode) or before.st_uid != os.getuid()
                or before.st_nlink != 1 or before.st_mode & 0o022 or before.st_size != expected_byte_count):
            raise ValueError("unsafe or changed file")
        with os.fdopen(descriptor, "rb") as handle:
            descriptor = None
            contents = handle.read(expected_byte_count + 1)
            after = os.fstat(handle.fileno())
        witness = lambda item: (item.st_dev, item.st_ino, item.st_size, item.st_mtime_ns,
                                item.st_ctime_ns, item.st_uid, item.st_mode, item.st_nlink)
        if (len(contents) != expected_byte_count or witness(before) != witness(after)
                or hashlib.sha256(contents).hexdigest() != expected_sha256
                or contents[:4] != b"RIFF" or contents[8:12] != b"WAVE"):
            raise ValueError("changed bytes or invalid WAV")
        return contents
    except (OSError, ValueError, TypeError):
        raise CloudClientError("temp.sh upload requires the exact bound, stable, owned WAV file") from None
    finally:
        if descriptor is not None:
            os.close(descriptor)
        if directory is not None:
            os.close(directory)


def _request(method: str, url: str, *, data=None, headers=None, limit=MAX_INSPECTION_BYTES):
    request_headers = {"Accept-Encoding": "identity", **(headers or {})}
    request = urllib.request.Request(url, method=method, data=data, headers=request_headers)
    mutating = method == "POST"
    status = None
    try:
        with _transport(request, TIMEOUT_SECONDS) as response:
            status = response.status
            allowed = {200, 201} if mutating else {200, 206}
            if status not in allowed:
                raise CloudClientError("temp.sh returned an unexpected HTTP status", status_code=status,
                                       retry_after_seconds=_retry_after(response.headers),
                                       ambiguous=mutating and (status == 408 or not 400 <= status < 500))
            return status, response.headers, response.read(limit)
    except urllib.error.HTTPError as error:
        status, retry_after = error.code, _retry_after(error.headers)
        error.close()
        raise CloudClientError("temp.sh HTTP request failed", status_code=status,
                               retry_after_seconds=retry_after,
                               ambiguous=mutating and (status == 408 or not 400 <= status < 500)) from None
    except CloudClientError:
        raise
    except (OSError, urllib.error.URLError, http.client.HTTPException, ValueError):
        raise CloudClientError("temp.sh transport failed", status_code=status, ambiguous=mutating) from None


def _content_length(headers) -> int | None:
    raw = headers.get("Content-Length")
    if raw is None:
        return None
    if not isinstance(raw, str) or re.fullmatch(r"[0-9]+", raw) is None:
        raise CloudClientError("temp.sh response has an invalid length")
    return int(raw)


def _is_html(headers, body: bytes) -> bool:
    content_type = headers.get("Content-Type", "").split(";", 1)[0].strip().lower()
    beginning = body.lstrip().lower()
    return content_type in {"text/html", "application/xhtml+xml"} or beginning.startswith((b"<!doctype html", b"<html"))


class _DownloadLinks(HTMLParser):
    def __init__(self):
        super().__init__(convert_charrefs=True)
        self.links = []
        self.anchor = None

    def handle_starttag(self, tag, attrs):
        if tag.lower() == "a":
            values = dict(attrs)
            self.anchor = {"href": values.get("href"), "download": "download" in values, "text": []}

    def handle_data(self, data):
        if self.anchor is not None:
            self.anchor["text"].append(data)

    def handle_endtag(self, tag):
        if tag.lower() == "a" and self.anchor is not None:
            anchor = self.anchor
            if anchor["download"] or "download" in "".join(anchor["text"]).lower():
                self.links.append(anchor["href"])
            self.anchor = None


def _landing_download_url(url: str) -> str:
    status, headers, body = _request("GET", url)
    length = _content_length(headers)
    if (status != 200 or len(body) >= MAX_INSPECTION_BYTES or not _is_html(headers, body)
            or (length is not None and length != len(body))):
        raise CloudClientError("temp.sh landing page is incomplete or exceeds its limit")
    parser = _DownloadLinks()
    try:
        parser.feed(body.decode("utf-8"))
        parser.close()
        candidates = {_temp_url(urllib.parse.urljoin(url, href)) for href in parser.links if isinstance(href, str)}
    except (UnicodeError, ValueError):
        raise CloudClientError("temp.sh landing page cannot be inspected") from None
    if len(candidates) != 1 or url in candidates:
        raise CloudClientError("temp.sh landing page lacks one unique observed download link")
    return candidates.pop()


def _verify_download(url: str, contents: bytes, *, allow_landing: bool = True) -> str:
    status, headers, body = _request("GET", url, headers={"Range": "bytes=0-4095"})
    if _is_html(headers, body):
        if not allow_landing:
            raise CloudClientError("temp.sh download link returned another HTML page")
        return _verify_download(_landing_download_url(url), contents, allow_landing=False)
    expected_size = len(contents)
    expected_prefix = contents[:PREFIX_BYTES]
    if body[:len(expected_prefix)] != expected_prefix or body[:4] != b"RIFF" or body[8:12] != b"WAVE":
        raise CloudClientError("temp.sh media response differs from the uploaded WAV")
    length = _content_length(headers)
    if status == 206:
        match = re.fullmatch(r"bytes 0-([0-9]+)/([0-9]+)", headers.get("Content-Range", ""))
        if (match is None or int(match.group(2)) != expected_size
                or int(match.group(1)) + 1 != len(expected_prefix) or len(body) != len(expected_prefix)
                or (length is not None and length != len(body))):
            raise CloudClientError("temp.sh ranged media response has a mismatched size")
    elif length != expected_size:
        raise CloudClientError("temp.sh media response lacks the uploaded file size")
    return url


def upload_temp_file(path: Path, *, expected_sha256: str, expected_byte_count: int) -> str:
    """Upload one explicitly authorized WAV and return verified downloadable URL.

    The local bytes are fully hashed before upload; remote verification checks
    matching size and WAV prefix, not a full remote checksum. No requests retry.
    Credentials are never accepted or forwarded. Remote files are not deleted.
    """
    contents = _upload_bytes(path, expected_sha256, expected_byte_count)
    boundary = "himr-temp-" + secrets.token_hex(24)
    while boundary.encode("ascii") in contents:
        boundary = "himr-temp-" + secrets.token_hex(24)
    prefix = (f'--{boundary}\r\nContent-Disposition: form-data; name="file"; filename="audio.wav"\r\n'
              'Content-Type: audio/wav\r\n\r\n').encode("ascii")
    suffix = f"\r\n--{boundary}--\r\n".encode("ascii")
    _, headers, body = _request("POST", UPLOAD_URL, data=b"".join((prefix, contents, suffix)),
                                headers={"Content-Type": f"multipart/form-data; boundary={boundary}", "Accept": "text/plain"},
                                limit=MAX_URL_RESPONSE_BYTES + 1)
    try:
        length = _content_length(headers)
        if len(body) > MAX_URL_RESPONSE_BYTES or (length is not None and length != len(body)):
            raise CloudClientError("temp.sh upload URL response is incomplete or exceeds its limit")
        url = _temp_url(body.decode("utf-8").strip())
        return _verify_download(url, contents)
    except (UnicodeError, ValueError):
        raise CloudClientError("temp.sh upload returned an invalid URL response", ambiguous=True) from None
    except CloudClientError as error:
        raise CloudClientError("temp.sh upload could not be verified as downloadable audio",
                               status_code=error.status_code, retry_after_seconds=error.retry_after_seconds,
                               ambiguous=True) from None
