"""Bounded read-only admission of exact seekable archive screening timelines.

Header durations are candidates, not permission to pad or truncate audio. The
first probe, measured tail-to-EOF and exact final probe use the same 16 kHz mono
PCM decoding as screening. This new archive recipe uniformly excludes the first
100 ms from screening, retaining absolute source timestamps and the full measured
EOF. It never pads codec-start shortfalls or changes the old screening decoder.
No source hash scan, normalization or download occurs.
Call in isolated spawned processes, not threads: decoder limits use preexec_fn.
"""
from __future__ import annotations

from fractions import Fraction
import errno
import hashlib
import json
import math
import os
import re
import resource
import signal
import subprocess
import tempfile
import time

from pipeline import speaker_screen as screen

ScreenError = screen.ScreenError
MAX_DURATION_MS = 86_400_000
MAX_PROBE_JSON = 1024**2
MAX_DIAGNOSTIC_BYTES = 64 * 1024
MAX_TAIL_MS = 20_000
MAX_TAIL_PCM = MAX_TAIL_MS * 32
SCREENABLE_START_MS = 100
IMPLICIT_ZERO_FORMATS = {"aac", "wav", "flac", "aiff", "mp3"}


class NeedsReview(ScreenError):
    """A source cannot be safely admitted by this bounded timeline recipe."""


def _remaining(deadline):
    left = deadline - time.monotonic()
    if left <= 0:
        raise NeedsReview("source_probe_time_budget_exhausted")
    return left


def _fraction(value, label, *, nonnegative=True):
    if (not isinstance(value, str) or not 1 <= len(value) <= 64
            or not re.fullmatch(r"[+-]?[0-9]+(?:\.[0-9]+|/[0-9]+)?", value)):
        raise NeedsReview("invalid_" + label)
    try:
        result = Fraction(value)
    except (ValueError, ZeroDivisionError) as error:
        raise NeedsReview("invalid_" + label) from error
    if nonnegative and result < 0:
        raise NeedsReview("invalid_" + label)
    return result


def _parse_json(body):
    def pairs(values):
        result = {}
        for key, value in values:
            if key in result:
                raise NeedsReview("duplicate_ffprobe_json_field")
            result[key] = value
        return result
    def constant(_value):
        raise NeedsReview("nonfinite_ffprobe_json")
    try:
        result = json.loads(body.decode("utf-8"), object_pairs_hook=pairs, parse_constant=constant)
    except (ValueError, UnicodeError, RecursionError) as error:
        raise NeedsReview("invalid_ffprobe_json") from error
    if not isinstance(result, dict):
        raise NeedsReview("invalid_ffprobe_document")
    return result


def _bounded_command(command, source_fd, tool_fd, *, maximum, deadline):
    timeout = _remaining(deadline)
    with tempfile.TemporaryFile() as output, tempfile.TemporaryFile() as errors:
        parent_pid = os.getpid()
        def limits():
            screen.die_with_parent(parent_pid)
            limit = max(maximum, MAX_DIAGNOSTIC_BYTES)
            resource.setrlimit(resource.RLIMIT_FSIZE, (limit, limit))
            seconds = max(1, math.ceil(timeout) + 2)
            _soft, hard = resource.getrlimit(resource.RLIMIT_CPU)
            if hard != resource.RLIM_INFINITY:
                seconds = min(seconds, hard)
            resource.setrlimit(resource.RLIMIT_CPU, (seconds, seconds))
            screen.child_limits(screen.DECODE_ADDRESS_SPACE_BYTES)
        process = subprocess.Popen(command, stdin=subprocess.DEVNULL, stdout=output, stderr=errors,
            pass_fds=(source_fd, tool_fd), start_new_session=True, preexec_fn=limits,
            env={"PATH": "/usr/bin:/bin", "LANG": "C", "CUDA_VISIBLE_DEVICES": "", "OMP_NUM_THREADS": "1"})
        try:
            try:
                process.wait(timeout=timeout)
            except subprocess.TimeoutExpired as error:
                raise NeedsReview("source_probe_time_budget_exhausted") from error
            errors.seek(0)
            diagnostics = errors.read(max(maximum, MAX_DIAGNOSTIC_BYTES))
            if re.search(rb"input/output error|\[errno 5\]", diagnostics, re.IGNORECASE):
                # Preserve storage failure as a stop condition for the parent
                # campaign, not an unsupported-media row to repeatedly retry.
                raise OSError(errno.EIO, "source probe reported an input/output error")
            if os.fstat(errors.fileno()).st_size > MAX_DIAGNOSTIC_BYTES:
                raise NeedsReview("excessive_media_diagnostics")
            if process.returncode != 0:
                raise NeedsReview("bounded_media_probe_or_decode_failed")
            output.seek(0)
            body = output.read(maximum + 1)
            if not body or len(body) > maximum:
                raise NeedsReview("empty_or_oversized_media_probe_output")
            return body
        finally:
            if process.poll() is None:
                os.killpg(process.pid, signal.SIGKILL)
                process.wait()


def _ffprobe(source_fd, tool_fd, deadline):
    command = [f"/proc/self/fd/{tool_fd}", "-v", "error", "-threads", "1",
        "-protocol_whitelist", "file,pipe", "-format_whitelist", screen.FORMATS,
        "-select_streams", "a:0", "-show_entries",
        "stream=index,codec_name,codec_type,sample_rate,channels,start_pts,start_time,duration_ts,time_base:stream_tags=DURATION:format=format_name,start_time,duration",
        "-of", "json", f"/proc/self/fd/{source_fd}"]
    return _parse_json(_bounded_command(command, source_fd, tool_fd, maximum=MAX_PROBE_JSON, deadline=deadline))


def _duration_tag(value):
    if not isinstance(value, str) or len(value) > 40:
        raise NeedsReview("invalid_audio_duration_tag")
    match = re.fullmatch(r"([0-9]{1,3}):([0-5][0-9]):([0-5][0-9](?:\.[0-9]{1,9})?)", value)
    if match is None:
        raise NeedsReview("invalid_audio_duration_tag")
    hours, minutes, seconds = match.groups()
    return int(hours) * 3_600 + int(minutes) * 60 + Fraction(seconds)


def _timeline(document):
    streams, container = document.get("streams"), document.get("format", {})
    if isinstance(streams, list) and not streams:
        raise NeedsReview("no_audio_stream")
    if not isinstance(streams, list) or len(streams) != 1 or not isinstance(streams[0], dict):
        raise NeedsReview("missing_or_ambiguous_first_audio_stream")
    if not isinstance(container, dict):
        raise NeedsReview("invalid_container_metadata")
    stream = streams[0]
    if stream.get("codec_type") != "audio":
        raise NeedsReview("first_selected_stream_is_not_audio")
    codec = stream.get("codec_name")
    if not isinstance(codec, str) or not re.fullmatch(r"[A-Za-z0-9_]{1,80}", codec):
        raise NeedsReview("unknown_audio_codec")
    formats = container.get("format_name")
    if not isinstance(formats, str) or len(formats) > 128:
        raise NeedsReview("unknown_container_format")
    known_starts = []
    if stream.get("start_time") is not None:
        known_starts.append(_fraction(stream["start_time"], "audio_start_time", nonnegative=False))
    time_base = None
    if stream.get("time_base") is not None:
        time_base = _fraction(stream["time_base"], "audio_time_base")
        if time_base <= 0:
            raise NeedsReview("invalid_audio_time_base")
    if stream.get("start_pts") is not None:
        if type(stream["start_pts"]) is not int or time_base is None:
            raise NeedsReview("invalid_audio_start_pts")
        known_starts.append(stream["start_pts"] * time_base)
    if any(start != 0 for start in known_starts):
        raise NeedsReview("nonzero_audio_start_requires_timeline_review")
    if not known_starts and not set(formats.split(",")) <= IMPLICIT_ZERO_FORMATS:
        raise NeedsReview("unknown_audio_start_requires_timeline_review")
    duration_ts = stream.get("duration_ts")
    if duration_ts is not None:
        if type(duration_ts) is not int or duration_ts <= 0 or time_base is None:
            raise NeedsReview("invalid_audio_duration_ticks")
        seconds, method = duration_ts * time_base, "audio_duration_ts_time_base_floor"
        duration_tag = None
    else:
        tags = stream.get("tags", {})
        if not isinstance(tags, dict) or tags.get("DURATION") is None:
            raise NeedsReview("no_exact_audio_duration_ticks_or_duration_tag")
        duration_tag = tags["DURATION"]
        seconds, method = _duration_tag(duration_tag), "audio_duration_tag_floor"
    declared = (seconds * 1_000).__floor__()
    if not 1 <= declared <= MAX_DURATION_MS:
        raise NeedsReview("declared_audio_duration_outside_screen_bounds")
    return {"method": method, "declared_duration_ms": declared,
            "declared_duration_seconds": {"numerator": seconds.numerator, "denominator": seconds.denominator},
            "duration_ts": duration_ts, "time_base": stream.get("time_base"), "duration_tag": duration_tag,
            "first_audio": {"index": stream.get("index"), "codec_name": codec,
                            "sample_rate": stream.get("sample_rate"), "channels": stream.get("channels"),
                            "start_time": stream.get("start_time"), "start_pts": stream.get("start_pts")},
            "audio_origin": "explicit_zero" if known_starts else "implicit_elementary_origin_zero",
            "container_format": formats, "container_duration_is_authoritative": False,
            "container_duration": container.get("duration"), "measured_eof_ms": None,
            "screenable_start_ms": SCREENABLE_START_MS,
            "leading_interval_not_admitted_ms": SCREENABLE_START_MS}


def _pcm_command(source_fd, tool_fd, start_ms, length_ms):
    # Exactly the old screening decode command, including input-side seek,
    # first audio selection, resampling and format/protocol restrictions.
    return [f"/proc/self/fd/{tool_fd}", "-v", "error", "-nostdin", "-threads", "1",
        "-filter_threads", "1", "-hwaccel", "none", "-protocol_whitelist", "file,pipe",
        "-format_whitelist", screen.FORMATS, "-ss", f"{start_ms / 1000:.3f}",
        "-i", f"/proc/self/fd/{source_fd}", "-t", f"{length_ms / 1000:.3f}", "-map", "0:a:0", "-vn", "-sn", "-dn",
        "-threads", "1", "-ac", "1", "-ar", "16000", "-c:a", "pcm_s16le", "-f", "s16le", "pipe:1"]


def _tail(source_fd, tool_fd, start_ms, deadline):
    command = _pcm_command(source_fd, tool_fd, start_ms, MAX_TAIL_MS)
    body = _bounded_command(command, source_fd, tool_fd, maximum=MAX_TAIL_PCM, deadline=deadline)
    if len(body) % 2:
        raise NeedsReview("tail_decode_has_incomplete_pcm_sample")
    if len(body) == MAX_TAIL_PCM:
        raise NeedsReview("tail_decode_hit_bound_before_confirmed_eof")
    samples = start_ms * 16 + len(body) // 2
    duration = samples // 16
    if not 1 <= duration <= MAX_DURATION_MS:
        raise NeedsReview("measured_audio_duration_outside_screen_bounds")
    return {"start_ms": start_ms, "decoded_pcm_bytes": len(body), "sample_rate": 16_000,
            "measured_eof_samples": samples, "duration_ms": duration,
            "fractional_millisecond_samples_discarded": samples % 16,
            "eof_observed_before_decode_bound": True}


def _exact_probe(source_fd, tool_fd, start, end, deadline):
    window = {"index": 0, "start_ms": start, "end_ms": end}
    length = end - start
    if type(start) is not int or type(end) is not int or not 0 <= start < end <= MAX_DURATION_MS or length > 10_000:
        raise NeedsReview("invalid_exact_screen_probe_coordinates")
    pcm = _bounded_command(_pcm_command(source_fd, tool_fd, start, length), source_fd, tool_fd,
                           maximum=length * 32, deadline=deadline)
    if len(pcm) != length * 32:
        raise NeedsReview("exact_screen_probe_decoded_short_or_malformed")
    return {**window, "pcm_bytes": len(pcm), "pcm_sha256": hashlib.sha256(pcm).hexdigest(),
            "exact_pcm_length": len(pcm) == (end - start) * 32}


def probe_source(recording, ffmpeg, ffprobe, *, timeout=30):
    """Read only one supplied source and return admitted or explicit needs_review.

    Optional duration_hint_ms is only a second bounded tail-seek candidate; it
    cannot supply an admitted duration. Screening is admitted only on the absolute
    source interval [100 ms, measured EOF); callers must use the matching archive
    sampling recipe, never the old plan that starts at zero. All child work shares
    one wall deadline.
    Source drift, unsafe paths and invalid bindings raise instead of admitting
    ambiguous evidence. No PCM bytes are persisted or included in the report.
    """
    if (not isinstance(recording, dict)
            or not {"media_id", "path", "sha256", "byte_count"} <= set(recording)
            or set(recording) - {"media_id", "path", "sha256", "byte_count", "duration_hint_ms"}):
        raise ScreenError("source probe requires exact media identity fields")
    if not isinstance(recording["media_id"], str) or not screen.IDENTIFIER.fullmatch(recording["media_id"]):
        raise ScreenError("source probe media_id is invalid")
    screen.file_binding({key: recording[key] for key in ("path", "sha256")})
    screen.integer(recording["byte_count"], 1, 64 * 1024**3, "source byte_count")
    hint = recording.get("duration_hint_ms")
    if hint is not None:
        screen.integer(hint, 1, MAX_DURATION_MS, "optional duration hint")
    for tool in (ffmpeg, ffprobe):
        screen.file_binding(tool)
    if type(timeout) not in (int, float) or not math.isfinite(timeout) or not 1 <= timeout <= 120:
        raise ScreenError("source probe timeout must be finite in1..120 seconds")
    started, deadline = time.monotonic(), time.monotonic() + timeout
    result = {"kind": "himr_speaker_screen_source_probe", "schema_version": 1,
        "status": "needs_review", "recording": dict(recording), "source_witness": None,
        "tools": {"ffmpeg": dict(ffmpeg), "ffprobe": dict(ffprobe)}, "duration_ms": None,
        "timeline": None, "checks": {"first_probe": None, "last_probe": None}, "tail_attempts": [],
        "error": None, "semantics": {"source_sha256_reverified": False, "full_source_hash": False,
            "normalization_performed": False, "source_modified": False, "internet_used": False,
            "missing_audio_padded": False, "source_timeline_only_not_full_integrity_verification": True,
            "intro_excluded_from_screening": True, "absolute_source_timestamps_preserved": True}}
    with screen.opened(recording["path"]) as source, screen.opened(ffmpeg["path"], executable=True) as decoder, \
            screen.opened(ffprobe["path"], executable=True) as inspector:
        source_witness = screen.witness(source)
        decoder_witness, inspector_witness = screen.witness(decoder), screen.witness(inspector)
        result["source_witness"] = source_witness
        if source_witness["st_size"] != recording["byte_count"]:
            raise ScreenError("source byte count differs from acquisition identity")
        try:
            for tool, descriptor in ((ffmpeg, decoder), (ffprobe, inspector)):
                if screen.hash_fd(descriptor, 256 * 1024**2, deadline) != tool["sha256"]:
                    raise ScreenError("source probe tool SHA-256 differs")
            result["timeline"] = timeline = _timeline(_ffprobe(source, inspector, deadline))
            declared = timeline["declared_duration_ms"]
            candidates = [max(0, declared - 15_000)]
            if hint is not None and max(0, hint - 15_000) not in candidates:
                candidates.append(max(0, hint - 15_000))
            admitted = None
            for tail_start in candidates[:2]:
                try:
                    measured = _tail(source, decoder, tail_start, deadline)
                    candidate = measured["duration_ms"]
                    if candidate <= SCREENABLE_START_MS:
                        raise NeedsReview("audio_ends_before_screenable_start")
                    first = _exact_probe(source, decoder, SCREENABLE_START_MS,
                                         min(SCREENABLE_START_MS + 10_000, candidate), deadline)
                    last = _exact_probe(source, decoder, max(SCREENABLE_START_MS, candidate - 10_000),
                                        candidate, deadline)
                    result["tail_attempts"].append({"status": "confirmed", **measured})
                    admitted = candidate, measured, first, last
                    break
                except NeedsReview as error:
                    result["tail_attempts"].append({"status": "needs_review", "start_ms": tail_start,
                                                    "error": str(error)})
            if admitted is None:
                raise NeedsReview("bounded_tail_measurement_could_not_admit_exact_eof")
            duration, measured, first, last = admitted
            timeline.update(measured_eof_ms=duration, measured_eof_samples=measured["measured_eof_samples"],
                            declared_vs_measured_ms=duration - declared,
                            final_method="bounded_tail_pcm_eof_then_exact_probes_after_explicit_100ms_start")
            result["checks"]["first_probe"] = first
            result["checks"]["last_probe"] = last
            result.update(status="admitted", duration_ms=duration)
        except NeedsReview as error:
            message = str(error)
            result["error"] = {"code": message.partition(":")[0], "message": message}
        finally:
            with screen.opened(recording["path"]) as current:
                if screen.witness(source) != source_witness or screen.witness(current) != source_witness:
                    raise ScreenError("source changed or was replaced during timeline admission")
            with screen.opened(ffmpeg["path"], executable=True) as current_decoder, \
                    screen.opened(ffprobe["path"], executable=True) as current_inspector:
                if (screen.witness(decoder) != decoder_witness or screen.witness(current_decoder) != decoder_witness
                        or screen.witness(inspector) != inspector_witness or screen.witness(current_inspector) != inspector_witness):
                    raise ScreenError("source probe tool changed during timeline admission")
    result["elapsed_seconds"] = time.monotonic() - started
    return result
