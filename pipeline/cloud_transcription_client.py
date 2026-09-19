"""Opt-in AssemblyAI/Rev AI HTTP clients, with no implicit retries or fallback.

The caller journals a submission intent BEFORE a paid POST, then persists the
unmodified JSON response BEFORE calling ``validate_job``/``normalize_result``.
Unknown POST outcomes require reconciliation, never a second provider request.
No environment is read here: the launcher supplies an explicitly loaded key.

API contracts reviewed 2026-09-13:
https://www.assemblyai.com/docs/pre-recorded-audio/api-reference/transcripts/submit
https://www.assemblyai.com/docs/pre-recorded-audio/api-reference/files/upload
https://support.assemblyai.com/articles/9208125065-are-there-any-limits-on-file-size-or-file-duration-for-files-submitted-to-the-api
https://docs.rev.ai/api/asynchronous/get-started
https://docs.rev.ai/api/features
https://docs.rev.ai/faq
https://docs.rev.ai/changelog
"""

from __future__ import annotations

from contextlib import contextmanager
import datetime as dt
import email.utils
import hashlib
import http.client
import json
import math
import os
from pathlib import Path
import re
import secrets
import stat
import urllib.error
import urllib.parse
import urllib.request


ASSEMBLYAI_MODEL = "universal-3-5-pro"
ASSEMBLYAI_NATIVE_LANGUAGES = frozenset({
    "en", "es", "fr", "de", "it", "pt", "ar", "da", "nl", "fi", "he",
    "hi", "ja", "zh", "no", "sv", "tr", "vi", "en_us", "en_uk", "en_au",
})
ASSEMBLYAI_MAX_UPLOAD_BYTES = 2_200_000_000
ASSEMBLYAI_MAX_SECONDS = 36_000
REVAI_MAX_REQUEST_BYTES = 2_000_000_000
REVAI_MAX_UPLOAD_BYTES = REVAI_MAX_REQUEST_BYTES - 65_536
REVAI_MAX_SECONDS = 61_200
MAX_RESPONSE_BYTES = 64 * 1024 * 1024
MAX_WORDS = 1_000_000
MAX_SEGMENTS = 250_000
CHUNK_BYTES = 1024 * 1024
_ORIGINS = {"assemblyai": "https://api.assemblyai.com", "revai": "https://api.rev.ai"}
_STATES = {"assemblyai": {"queued", "processing", "completed", "error"},
           "revai": {"in_progress", "transcribed", "failed"}}
_TOKEN = re.compile(r"[A-Za-z0-9][A-Za-z0-9_-]{0,127}\Z")
_DIGEST = re.compile(r"[0-9a-f]{64}\Z")


class CloudClientError(RuntimeError):
    """Sanitized diagnostic, never includes credentials or provider text/URLs."""

    def __init__(self, message, *, status_code=None, ambiguous=False,
                 retry_after_seconds=None, response=None):
        super().__init__(message)
        self.status_code = status_code
        self.ambiguous = ambiguous
        self.retry_after_seconds = retry_after_seconds
        # A successful paid response must not be lost if a local post-upload
        # witness check fails. Private journals may retain this; never print it.
        self.response = response


class _NoRedirect(urllib.request.HTTPRedirectHandler):
    def redirect_request(self, req, fp, code, msg, headers, newurl):
        return None


def _transport(request, timeout_seconds):
    opener = urllib.request.build_opener(urllib.request.ProxyHandler({}), _NoRedirect())
    return opener.open(request, timeout=timeout_seconds)


def _json_object(raw):
    def pairs(items):
        value = {}
        for key, item in items:
            if key in value:
                raise ValueError("duplicate key")
            value[key] = item
        return value

    def constant(_):
        raise ValueError("nonfinite value")

    def finite(value):
        number = float(value)
        if not math.isfinite(number):
            raise ValueError("nonfinite value")
        return number

    value = json.loads(raw.decode("utf-8"), object_pairs_hook=pairs,
                       parse_constant=constant, parse_float=finite)
    if not isinstance(value, dict):
        raise ValueError("expected object")
    return value


def _retry_after(headers):
    value = headers.get("Retry-After") if headers else None
    if not isinstance(value, str):
        return None
    try:
        result = float(value)
    except (ValueError, OverflowError):
        try:
            deadline = email.utils.parsedate_to_datetime(value)
            if deadline.tzinfo is None:
                return None
            result = (deadline - dt.datetime.now(dt.timezone.utc)).total_seconds()
        except (ValueError, TypeError, OverflowError):
            return None
    return max(0.0, result) if math.isfinite(result) else None


def _identifier(value):
    if not isinstance(value, str) or not _TOKEN.fullmatch(value):
        raise CloudClientError("invalid cloud job identifier")
    return value


def _provider(value):
    if not isinstance(value, str) or value not in _ORIGINS:
        raise CloudClientError("unsupported transcription provider")
    return value


def _number(value, label, *, maximum, positive=False):
    if (isinstance(value, bool) or not isinstance(value, (int, float))
            or not math.isfinite(value) or value < 0 or value > maximum
            or (positive and value == 0)):
        raise CloudClientError("invalid " + label)
    return float(value)


def validate_duration(provider, seconds):
    _provider(provider)
    return _number(seconds, "media duration", maximum=(ASSEMBLYAI_MAX_SECONDS
                   if provider == "assemblyai" else REVAI_MAX_SECONDS), positive=True)


def validate_upload_url(value):
    """Only a direct AssemblyAI upload receipt is accepted, not a public host."""
    try:
        if not isinstance(value, str) or len(value) > 2048 or "\\" in value:
            raise ValueError()
        parsed = urllib.parse.urlsplit(value)
        if (parsed.scheme != "https" or parsed.netloc != "cdn.assemblyai.com"
                or parsed.query or parsed.fragment or not parsed.path.startswith("/upload/")):
            raise ValueError()
        # Provider receipts may contain an opaque project prefix followed by
        # the opaque upload id; neither segment is a user-controlled URL.
        parts = parsed.path.removeprefix("/upload/").split("/")
        if not 1 <= len(parts) <= 2:
            raise ValueError()
        for part in parts:
            _identifier(part)
        if value != "https://cdn.assemblyai.com" + parsed.path:
            raise ValueError()
    except (ValueError, TypeError, CloudClientError):
        raise CloudClientError("invalid AssemblyAI upload URL") from None
    return value


def _diarization(value):
    if type(value) is not bool:
        raise CloudClientError("diarization must be an explicit boolean")
    return value


def assemblyai_options(upload_url, *, diarization=True, language="en"):
    if not isinstance(language, str) or language not in {"en", "auto"}:
        raise CloudClientError("unsupported AssemblyAI language policy")
    return {"audio_url": validate_upload_url(upload_url),
            "speech_models": [ASSEMBLYAI_MODEL],
            **({"language_detection": True} if language == "auto" else {"language_code": "en"}),
            "speaker_labels": _diarization(diarization), "punctuate": True, "format_text": True,
            "filter_profanity": False}


def revai_options(metadata=None, *, diarization=True):
    result = {"transcriber": "machine", "language": "en", "skip_diarization": not _diarization(diarization),
              "skip_punctuation": False, "filter_profanity": False}
    if metadata is not None:
        result["metadata"] = _identifier(metadata)
    return result


def _witness(value):
    return (value.st_dev, value.st_ino, value.st_size, value.st_mtime_ns,
            value.st_ctime_ns, value.st_mode, value.st_uid, value.st_nlink)


def _path_witness(path):
    """Re-resolve every path component without following symlinks."""
    directory = descriptor = None
    try:
        absolute = Path(path).absolute()
        directory = os.open(absolute.anchor, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW)
        for part in absolute.parts[1:-1]:
            child = os.open(part, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW, dir_fd=directory)
            os.close(directory)
            directory = child
        descriptor = os.open(absolute.name, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK, dir_fd=directory)
        return _witness(os.fstat(descriptor))
    finally:
        if descriptor is not None:
            os.close(descriptor)
        if directory is not None:
            os.close(directory)


@contextmanager
def _upload_file(path, expected_sha256, maximum_bytes):
    """Pin a descriptor, prehash it, then stream it without buffering the file."""
    if not isinstance(expected_sha256, str) or not _DIGEST.fullmatch(expected_sha256):
        raise CloudClientError("upload requires an expected SHA-256 digest")
    descriptor = directory = None
    try:
        absolute = Path(path).absolute()
        if ".." in absolute.parts:
            raise ValueError()
        directory = os.open(absolute.anchor, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW)
        for part in absolute.parts[1:-1]:
            child = os.open(part, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW, dir_fd=directory)
            os.close(directory)
            directory = child
        descriptor = os.open(absolute.name, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK,
                             dir_fd=directory)
        before = os.fstat(descriptor)
        if (not stat.S_ISREG(before.st_mode) or before.st_uid != os.getuid()
                or before.st_nlink != 1 or before.st_mode & 0o022
                or not 0 < before.st_size <= maximum_bytes):
            raise ValueError()
        digest, total = hashlib.sha256(), 0
        while block := os.read(descriptor, CHUNK_BYTES):
            total += len(block)
            if total > before.st_size:
                raise ValueError()
            digest.update(block)
        if total != before.st_size or digest.hexdigest() != expected_sha256 or _witness(before) != _witness(os.fstat(descriptor)):
            raise ValueError()
        os.lseek(descriptor, 0, os.SEEK_SET)
    except (OSError, ValueError, TypeError):
        if descriptor is not None:
            os.close(descriptor)
            descriptor = None
        raise CloudClientError("upload requires a stable owned regular file matching its size limit and digest") from None
    finally:
        if directory is not None:
            os.close(directory)
    try:
        yield descriptor, before
    finally:
        if descriptor is not None:
            os.close(descriptor)


class _StreamingBody:
    def __init__(self, descriptor, before, expected_sha256, prefix=b"", suffix=b"", *, path):
        self.descriptor, self.before, self.expected_sha256 = descriptor, before, expected_sha256
        self.path = path
        self.prefix, self.suffix = prefix, suffix
        self.length = len(prefix) + before.st_size + len(suffix)
        self.consumed = False
        self.started = False

    def __iter__(self):
        if self.started:
            raise CloudClientError("upload body cannot be resent", ambiguous=True)
        self.started = True
        if _path_witness(self.path) != _witness(self.before):
            raise CloudClientError("upload media path changed during transfer", ambiguous=True)
        if self.prefix:
            yield self.prefix
        digest, total = hashlib.sha256(), 0
        while block := os.read(self.descriptor, CHUNK_BYTES):
            total += len(block)
            if total > self.before.st_size:
                raise CloudClientError("upload media changed during transfer", ambiguous=True)
            digest.update(block)
            yield block
        if (total != self.before.st_size or digest.hexdigest() != self.expected_sha256
                or _witness(self.before) != _witness(os.fstat(self.descriptor))):
            raise CloudClientError("upload media changed during transfer", ambiguous=True)
        if self.suffix:
            yield self.suffix
        self.consumed = True

    def verify(self, response):
        try:
            valid = (self.consumed and _witness(self.before) == _witness(os.fstat(self.descriptor))
                     and _path_witness(self.path) == _witness(self.before))
        except OSError:
            valid = False
        if not valid:
            raise CloudClientError("upload completion could not be verified", ambiguous=True, response=response)


class _Client:
    provider = None

    def __init__(self, api_key, timeout_seconds=120):
        if (not isinstance(api_key, str) or not 1 <= len(api_key) <= 4096
                or any(ord(char) < 33 or ord(char) > 126 for char in api_key)):
            raise CloudClientError("cloud API key is missing or invalid")
        self.timeout_seconds = _number(timeout_seconds, "request timeout", maximum=3600, positive=True)
        self._api_key = api_key

    def _request(self, method, path, *, data=None, content_type=None,
                 accept="application/json", expected_status=200):
        if method not in {"GET", "POST"} or not path.startswith("/") or any(c in path for c in "?#\\\r\n"):
            raise CloudClientError("invalid cloud request")
        provider = _provider(self.provider)
        url = _ORIGINS[provider] + path
        headers = {"Authorization": ("Bearer " if provider == "revai" else "") + self._api_key,
                   "Accept": accept, "Accept-Encoding": "identity"}
        if content_type:
            headers["Content-Type"] = content_type
        if isinstance(data, _StreamingBody):
            headers["Content-Length"] = str(data.length)
        request = urllib.request.Request(url, data=data, headers=headers, method=method)
        mutation, status = method == "POST", None
        try:
            with _transport(request, self.timeout_seconds) as response:
                status = response.status
                if status != expected_status:
                    raise CloudClientError("cloud request returned an unexpected HTTP status", status_code=status,
                                           ambiguous=mutation and (status == 408 or not 400 <= status < 500),
                                           retry_after_seconds=_retry_after(response.headers))
                encoding = response.headers.get("Content-Encoding", "identity")
                if encoding != "identity":
                    raise CloudClientError("cloud response encoding rejected", ambiguous=mutation)
                length = response.headers.get("Content-Length")
                if length is not None:
                    try:
                        length = int(length)
                    except (TypeError, ValueError):
                        raise CloudClientError("invalid cloud response length", ambiguous=mutation) from None
                    if not 0 <= length <= MAX_RESPONSE_BYTES:
                        raise CloudClientError("cloud response exceeds size limit", ambiguous=mutation)
                raw = response.read(MAX_RESPONSE_BYTES + 1)
                if len(raw) > MAX_RESPONSE_BYTES or (length is not None and len(raw) != length):
                    raise CloudClientError("cloud response length rejected", ambiguous=mutation)
                try:
                    result = _json_object(raw)
                except (ValueError, UnicodeError, RecursionError):
                    raise CloudClientError("cloud response is not a strict JSON object", ambiguous=mutation) from None
                if isinstance(data, _StreamingBody):
                    data.verify(result)
                return result
        except urllib.error.HTTPError as error:
            code, retry_after = error.code, _retry_after(error.headers)
            error.close()
            raise CloudClientError("cloud request failed with an HTTP status", status_code=code,
                                   ambiguous=mutation and (code == 408 or not 400 <= code < 500),
                                   retry_after_seconds=retry_after) from None
        except CloudClientError:
            raise
        except (OSError, ValueError, urllib.error.URLError, http.client.HTTPException):
            raise CloudClientError("cloud request transport failed", status_code=status, ambiguous=mutation) from None


class AssemblyAIClient(_Client):
    provider = "assemblyai"

    def upload(self, path, *, expected_sha256):
        with _upload_file(path, expected_sha256, ASSEMBLYAI_MAX_UPLOAD_BYTES) as (descriptor, before):
            body = _StreamingBody(descriptor, before, expected_sha256, path=path)
            return self._request("POST", "/v2/upload", data=body, content_type="application/octet-stream")

    def submit(self, upload_url, *, diarization=True, language="en"):
        body = json.dumps(assemblyai_options(upload_url, diarization=diarization, language=language), separators=(",", ":")).encode("utf-8")
        return self._request("POST", "/v2/transcript", data=body, content_type="application/json")

    def poll(self, job_id):
        return self._request("GET", "/v2/transcript/" + _identifier(job_id))


class RevAIClient(_Client):
    provider = "revai"

    def submit_file(self, path, *, expected_sha256, metadata=None, diarization=True):
        options = json.dumps(revai_options(metadata, diarization=diarization), separators=(",", ":")).encode("utf-8")
        boundary = "himr-cloud-" + secrets.token_hex(32)
        prefix = (f'--{boundary}\r\nContent-Disposition: form-data; name="options"\r\n'
                  'Content-Type: application/json\r\n\r\n').encode("ascii") + options
        # Opaque filename avoids leaking the local path/title through headers.
        suffix_name = Path(path).suffix.lower()
        if not re.fullmatch(r"\.[a-z0-9]{1,8}", suffix_name):
            suffix_name = ".bin"
        prefix += (f'\r\n--{boundary}\r\nContent-Disposition: form-data; name="media"; '
                   f'filename="audio{suffix_name}"\r\nContent-Type: application/octet-stream\r\n\r\n').encode("ascii")
        suffix = f"\r\n--{boundary}--\r\n".encode("ascii")
        with _upload_file(path, expected_sha256, REVAI_MAX_UPLOAD_BYTES) as (descriptor, before):
            body = _StreamingBody(descriptor, before, expected_sha256, prefix, suffix, path=path)
            if body.length > REVAI_MAX_REQUEST_BYTES:
                raise CloudClientError("Rev AI multipart request exceeds size limit")
            return self._request("POST", "/speechtotext/v1/jobs", data=body,
                                 content_type="multipart/form-data; boundary=" + boundary)

    def poll(self, job_id):
        return self._request("GET", "/speechtotext/v1/jobs/" + _identifier(job_id))

    def transcript(self, job_id):
        return self._request("GET", "/speechtotext/v1/jobs/" + _identifier(job_id) + "/transcript",
                             accept="application/vnd.rev.transcript.v1.0+json")


def validate_job(provider, raw, *, expected_job_id=None):
    """Validate a receipt only AFTER the caller has persisted its raw object."""
    _provider(provider)
    if not isinstance(raw, dict):
        raise CloudClientError("cloud job is not an object")
    identifier = _identifier(raw.get("id"))
    if expected_job_id is not None and identifier != _identifier(expected_job_id):
        raise CloudClientError("cloud job identity mismatch")
    if not isinstance(raw.get("status"), str) or raw["status"] not in _STATES[provider]:
        raise CloudClientError("cloud job status rejected")
    if provider == "assemblyai":
        if raw.get("speech_models") not in (None, [ASSEMBLYAI_MODEL]):
            raise CloudClientError("AssemblyAI requested model differs")
        used = raw.get("speech_model_used")
        if used is not None and used != ASSEMBLYAI_MODEL:
            raise CloudClientError("AssemblyAI actual model differs")
    else:
        if raw.get("type") not in (None, "async") or raw.get("transcriber") not in (None, "machine"):
            raise CloudClientError("Rev AI job type differs")
        if raw.get("language") not in (None, "en"):
            raise CloudClientError("Rev AI job language differs")
    return {"job_id": identifier, "status": raw["status"]}


def _text(value, *, empty=False, maximum=MAX_RESPONSE_BYTES):
    if not isinstance(value, str) or len(value) > maximum or (not empty and not value.strip()) or "\x00" in value:
        raise CloudClientError("invalid cloud transcript text")
    return value


def _speaker(value, provider):
    if provider == "revai":
        if isinstance(value, bool) or not isinstance(value, int) or not 0 <= value <= 1024:
            raise CloudClientError("invalid Rev AI speaker label")
        return "speaker_" + str(value)
    if not isinstance(value, str) or not re.fullmatch(r"[A-Z]{1,4}", value):
        raise CloudClientError("invalid AssemblyAI speaker label")
    return "speaker_" + value


def _time(value, duration, *, scale):
    number = _number(value, "word timestamp", maximum=(duration + 2) * scale)
    return int(math.floor(number / scale * 1000 + 0.5))


def _confidence(value):
    if value is None:
        return None
    return _number(value, "word confidence", maximum=1)


def _word(raw, duration, speaker, *, provider, diarization=True):
    if not isinstance(raw, dict):
        raise CloudClientError("invalid word object")
    assembly = provider == "assemblyai"
    start = _time(raw.get("start" if assembly else "ts"), duration, scale=1000 if assembly else 1)
    end = _time(raw.get("end" if assembly else "end_ts"), duration, scale=1000 if assembly else 1)
    if end < start:
        raise CloudClientError("reversed cloud word timestamp")
    label = (_speaker(raw["speaker"], provider) if assembly and raw.get("speaker") is not None else speaker) if diarization else None
    if speaker is not None and label != speaker:
        raise CloudClientError("word speaker differs from its utterance")
    return {"start_ms": start, "end_ms": end, "text": _text(raw.get("text" if assembly else "value"), maximum=65_536),
            "speaker": label, "confidence": _confidence(raw.get("confidence"))}


def _duration(provider, raw_duration, expected):
    expected = validate_duration(provider, expected)
    duration = validate_duration(provider, raw_duration)
    if abs(duration - expected) > max(2, expected * .001):
        raise CloudClientError("provider audio duration differs from local media")
    return duration


def _unattributed_segments(words, duration):
    """Group timed words for readability, never infer a single speaker."""
    if not isinstance(words, list) or not 0 < len(words) <= MAX_WORDS:
        raise CloudClientError("AssemblyAI non-diarized result lacks bounded words")
    segments, group = [], []

    def emit():
        if len(segments) >= MAX_SEGMENTS:
            raise CloudClientError("cloud transcript exceeds segment limit")
        segments.append({"start_ms": group[0]["start_ms"], "end_ms": max(word["end_ms"] for word in group),
                         "text": " ".join(word["text"] for word in group), "speaker": None, "words": group.copy()})
        group.clear()

    previous = -1
    for raw_word in words:
        word = _word(raw_word, duration, None, provider="assemblyai", diarization=False)
        if word["start_ms"] < previous:
            raise CloudClientError("cloud words are not time ordered")
        previous = word["start_ms"]
        if group and (len(group) >= 60 or word["end_ms"] - group[0]["start_ms"] > 30_000
                      or word["start_ms"] - group[-1]["end_ms"] > 2_000):
            emit()
        group.append(word)
    if group:
        emit()
    return segments


def normalize_result(provider, raw, *, expected_duration_seconds, job=None, diarization=True, language="en"):
    """Validate and normalize, never deduplicate/rewrite provider speech text.

    Only segment boundaries are retained as approximate recording-relative
    milliseconds; word timing/confidence details remain in the raw provider
    response, not the normalized transcript. Speaker labels
    apply only within this provider job and never identify a person. Overlapping
    speaker turns are allowed. Empty/silent results are held for review.
    """
    _provider(provider)
    _diarization(diarization)
    if not isinstance(language, str) or language not in {"en", "auto"} or provider != "assemblyai" and language != "en":
        raise CloudClientError("unsupported cloud language policy")
    if not isinstance(raw, dict):
        raise CloudClientError("cloud transcript is not an object")
    segments, count = [], 0
    cjk_spacing_only = False
    if provider == "assemblyai":
        validate_job(provider, raw)
        actual_diarization = raw.get("speaker_labels")
        if (raw["status"] != "completed" or raw.get("speech_model_used") != ASSEMBLYAI_MODEL
                or (diarization and actual_diarization is not True)
                or (actual_diarization is not None and actual_diarization is not diarization)):
            raise CloudClientError("AssemblyAI result is incomplete or model/diarization differs")
        allowed_languages = tuple(ASSEMBLYAI_NATIVE_LANGUAGES) if language == "auto" else (None, "en", "en_us", "en_uk", "en_au")
        if raw.get("language_code") not in allowed_languages:
            raise CloudClientError("AssemblyAI result language differs")
        duration = _duration(provider, raw.get("audio_duration"), expected_duration_seconds)
        text = _text(raw.get("text"))
        if not diarization:
            segments = _unattributed_segments(raw.get("words"), duration)
            utterances = []
        else:
            utterances = raw.get("utterances")
            if not isinstance(utterances, list) or not 0 < len(utterances) <= MAX_SEGMENTS:
                raise CloudClientError("AssemblyAI result lacks bounded utterances")
        for utterance in utterances:
            if not isinstance(utterance, dict):
                raise CloudClientError("invalid AssemblyAI utterance")
            speaker = _speaker(utterance.get("speaker"), provider)
            words = utterance.get("words")
            if not isinstance(words, list) or not words or count + len(words) > MAX_WORDS:
                raise CloudClientError("invalid AssemblyAI utterance words")
            normalized = [_word(word, duration, speaker, provider=provider) for word in words]
            start = _time(utterance.get("start"), duration, scale=1000)
            end = _time(utterance.get("end"), duration, scale=1000)
            if end < start or any(word["start_ms"] < start or word["end_ms"] > end for word in normalized):
                raise CloudClientError("utterance/word timing mismatch")
            segments.append({"start_ms": start, "end_ms": end, "speaker": speaker,
                             "text": _text(utterance.get("text")), "words": normalized})
            count += len(words)
        segment_text = " ".join(segment["text"] for segment in segments)
        if " ".join(text.split()) != " ".join(segment_text.split()):
            # Japanese/Chinese provider utterances can contain token-separating
            # spaces absent from the full text. Accept only exact non-whitespace
            # character equality in explicit auto-language mode. Do not rewrite
            # either representation, or weaken English/content validation.
            cjk_spacing_only = (language == "auto" and raw.get("language_code") in ("ja", "zh")
                                and "".join(text.split()) == "".join(segment_text.split()))
            if not cjk_spacing_only:
                raise CloudClientError("AssemblyAI full text differs from its utterances")
        model = ASSEMBLYAI_MODEL
    else:
        validate_job(provider, job)
        actual_skipped = job.get("skip_diarization")
        if job["status"] != "transcribed" or (actual_skipped is not None and actual_skipped is not (not diarization)):
            raise CloudClientError("Rev AI result is incomplete or diarization differs")
        duration = _duration(provider, job.get("duration_seconds"), expected_duration_seconds)
        monologues = raw.get("monologues")
        if not isinstance(monologues, list) or not 0 < len(monologues) <= MAX_SEGMENTS:
            raise CloudClientError("Rev AI result lacks bounded monologues")
        for monologue in monologues:
            if not isinstance(monologue, dict):
                raise CloudClientError("invalid Rev AI monologue")
            speaker = _speaker(monologue.get("speaker"), provider) if diarization else None
            elements = monologue.get("elements")
            if not isinstance(elements, list) or not elements or len(elements) > MAX_WORDS * 3:
                raise CloudClientError("invalid Rev AI elements")
            words, pieces = [], []
            for element in elements:
                if not isinstance(element, dict) or element.get("type") not in {"text", "punct"}:
                    raise CloudClientError("invalid Rev AI element type")
                pieces.append(_text(element.get("value"), empty=element["type"] == "punct", maximum=65_536))
                if element["type"] == "text":
                    words.append(_word(element, duration, speaker, provider=provider, diarization=diarization))
                    count += 1
                    if count > MAX_WORDS:
                        raise CloudClientError("cloud transcript exceeds word limit")
            if not words:
                raise CloudClientError("Rev AI monologue has no timed words")
            segments.append({"start_ms": min(word["start_ms"] for word in words),
                             "end_ms": max(word["end_ms"] for word in words),
                             "speaker": speaker, "text": _text("".join(pieces)), "words": words})
        text, model = "\n".join(segment["text"] for segment in segments), "machine"
    previous = -1
    for segment in segments:
        if segment["start_ms"] < previous:
            raise CloudClientError("cloud speaker turns are not time ordered")
        previous = segment["start_ms"]
        starts = [word["start_ms"] for word in segment["words"]]
        if starts != sorted(starts):
            raise CloudClientError("cloud words are not time ordered")
    labels = {}
    for segment in segments:
        # Raw word timestamps were needed to validate/derive segment boundaries,
        # but are deliberately not duplicated into the preferred transcript.
        del segment["words"]
        original = segment["speaker"]
        if original is None:
            continue
        if original not in labels:
            labels[original] = "SPEAKER_" + str(len(labels)).zfill(4)
        segment["speaker"] = labels[original]
    return {**({"cjk_spacing_only_difference": True} if cjk_spacing_only else {}),
            "provider": provider, "model": model, "duration_seconds": duration,
            "text": text, "segments": segments, "speaker_labels_are_identities": False,
            "diarization_requested": diarization,
            "provider_speaker_labels": {label: original.removeprefix("speaker_") for original, label in labels.items()}}
