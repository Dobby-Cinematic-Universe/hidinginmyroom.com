"""Finite local decoders and anonymous CPU models for multimodal triage.

No identity recognition, network model loader, whole-video decode, or live input.
The caller supplies hash-bound files, retained descriptors and private outputs.
"""
from __future__ import annotations

import errno
from fractions import Fraction
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
from pathlib import Path

from pipeline import speaker_screen as safe
from pipeline import speaker_screen_engine as speech
from pipeline import speaker_screen_multimodal_core as core
from pipeline import yunet_face_detector as yunet

WIDTH, HEIGHT = 640, 360
YUNET_SHA = "8f2383e4dd3cfbb4553ea8718107fc0423210dc964f9f4280604804ed2552fa4"


class DecodeReview(core.TriageError):
    """Unsupported individual sample; never a negative observation."""


def seconds(ms):
    return f"{ms // 1000}.{ms % 1000:03d}"


def bounded_process(argv, *, pass_fds=(), timeout=20, maximum=2 * 1024**2):
    """Bound stdout/stderr, process group, CPU, memory, network and parent lifetime."""
    with tempfile.TemporaryFile() as out, tempfile.TemporaryFile() as err:
        parent_pid = os.getpid()

        def limits():
            safe.die_with_parent(parent_pid)
            resource.setrlimit(resource.RLIMIT_FSIZE, (maximum, maximum))
            resource.setrlimit(resource.RLIMIT_CPU, (math.ceil(timeout) + 2, math.ceil(timeout) + 2))
            safe.child_limits(1024**3)

        process = subprocess.Popen(argv, stdin=subprocess.DEVNULL, stdout=out, stderr=err,
            pass_fds=pass_fds, start_new_session=True, preexec_fn=limits,
            env={"PATH": "/usr/bin:/bin", "LANG": "C", "OMP_NUM_THREADS": "1",
                 "OPENBLAS_NUM_THREADS": "1", "CUDA_VISIBLE_DEVICES": ""})
        try:
            try:
                process.wait(timeout=timeout)
            except subprocess.TimeoutExpired:
                raise DecodeReview("sample_decode_timeout") from None
            out.seek(0); err.seek(0)
            body, errors = out.read(maximum + 1), err.read(maximum + 1)
            if len(body) > maximum or len(errors) > maximum:
                raise DecodeReview("sample_output_limit")
            if b"Input/output error" in errors:
                raise OSError(errno.EIO, "media I/O error; stop triage and inspect storage")
            if process.returncode != 0:
                raise DecodeReview("sample_decode_failed")
            return body, errors.decode("utf-8", errors="replace")
        finally:
            if process.poll() is None:
                os.killpg(process.pid, signal.SIGKILL)
                process.wait()


def _span(stream, fallback_duration_ms):
    try:
        start = Fraction(stream.get("start_time", "0"))
        if "duration_ts" in stream and "time_base" in stream:
            duration = int(stream["duration_ts"]) * Fraction(stream["time_base"])
            basis = "stream_ticks"
        elif "duration" in stream:
            duration = Fraction(stream["duration"])
            basis = "stream_duration"
        else:
            duration = Fraction(fallback_duration_ms, 1000)
            basis = "inventory_duration_hint"
        begin, end = max(0, math.ceil(start * 1000)), math.floor((start + duration) * 1000)
        core.interval(begin, end)
        return {"start_ms": begin, "end_ms": end, "basis": basis,
                "exact_eof_verified": False, "sample_failures_remain_unknown": True}
    except (TypeError, ValueError, ZeroDivisionError, core.TriageError):
        return None


def probe(source_fd, ffprobe_fd, fallback_duration_ms):
    body, _ = bounded_process([f"/proc/self/fd/{ffprobe_fd}", "-v", "error",
        "-protocol_whitelist", "file,pipe", "-format_whitelist", safe.FORMATS,
        "-probesize", "2097152", "-analyzeduration", "2000000", "-show_streams",
        "-show_format", "-of", "json", f"/proc/self/fd/{source_fd}"], pass_fds=(source_fd, ffprobe_fd))
    try:
        value = json.loads(body)
        streams = value["streams"]
        if not isinstance(streams, list) or len(streams) > 64:
            raise ValueError()
        origin = math.floor(Fraction(value.get("format", {}).get("start_time", "0")) * 1000)
        core.integer(origin, -core.MAX_DURATION_MS, core.MAX_DURATION_MS, "container time origin")
        result = {"source_start_ms": origin}
        for name in ("audio", "video"):
            selected = next((s for s in streams if s.get("codec_type") == name and
                             not s.get("disposition", {}).get("attached_pic", 0)), None)
            span = None if selected is None else _span(selected, fallback_duration_ms)
            result[name] = {"state": "absent" if selected is None else "unsupported_timing" if span is None else "available",
                            "stream_index": None if selected is None else selected["index"], "span": span}
        return result
    except (KeyError, TypeError, ValueError):
        raise DecodeReview("invalid_probe_metadata") from None


def _prefix(source_fd, ffmpeg_fd, start_ms, duration_ms, source_start_ms=0):
    # Input -ss is relative to the container start. Retain copyts and trim by
    # absolute source PTS afterward. This is tested on non-zero-start MKV: the
    # -seek_timestamp/copyts combination otherwise double-counted its offset.
    seek_ms = max(0, start_ms - source_start_ms)
    return [f"/proc/self/fd/{ffmpeg_fd}", "-hide_banner", "-nostdin", "-nostats", "-loglevel", "info",
            "-threads", "1", "-filter_threads", "1", "-hwaccel", "none",
            "-protocol_whitelist", "file,pipe", "-format_whitelist", safe.FORMATS,
            "-copyts", "-ss", seconds(seek_ms), "-t", seconds(duration_ms),
            "-i", f"/proc/self/fd/{source_fd}"]


def frame_timestamp(stderr, target_ms):
    bases = re.findall(r"showinfo@triage\s+@[^\]]+\] config in time_base: (\d+)/(\d+)", stderr)
    rows = re.findall(r"showinfo@triage\s+@[^\]]+\][^\n]*\bn:\s*(\d+)\s+pts:\s*(-?\d+)", stderr)
    if len(bases) != 1 or not rows or rows[0][0] != "0":
        raise DecodeReview("frame_timestamp_missing")
    try:
        actual = math.floor(int(rows[0][1]) * Fraction(int(bases[0][0]), int(bases[0][1])) * 1000)
    except (ValueError, ZeroDivisionError):
        raise DecodeReview("frame_timestamp_invalid") from None
    if not target_ms <= actual <= target_ms + 2000:
        raise DecodeReview("frame_timestamp_outside_seek_tolerance")
    return actual


def decode_frame(source_fd, ffmpeg_fd, stream_index, target_ms, *, source_start_ms=0):
    command = _prefix(source_fd, ffmpeg_fd, target_ms, 3000, source_start_ms) + [
        "-map", f"0:{stream_index}", "-an", "-sn", "-dn", "-map_metadata", "-1",
        # trim alone rounds the requested time to source ticks and can admit a
        # frame just before the request. Select by real source t as well.
        "-vf", f"trim=start={seconds(target_ms)},select=gte(t\\,{seconds(target_ms)}),"
               f"scale={WIDTH}:{HEIGHT}:force_original_aspect_ratio=decrease:flags=bilinear,"
               f"pad={WIDTH}:{HEIGHT}:(ow-iw)/2:(oh-ih)/2,setsar=1,format=bgr24,showinfo@triage",
        "-frames:v", "1", "-fps_mode", "passthrough", "-c:v", "rawvideo", "-threads:v", "1",
        "-pix_fmt", "bgr24", "-f", "rawvideo", "pipe:1"]
    body, stderr = bounded_process(command, pass_fds=(source_fd, ffmpeg_fd))
    if len(body) != WIDTH * HEIGHT * 3:
        raise DecodeReview("frame_missing_or_incomplete")
    return body, frame_timestamp(stderr, target_ms)


def audio_receipt(body, stderr, start_ms, end_ms):
    rows = re.findall(r"ashowinfo@triage\s+@[^\]]+\][^\n]*\bn:\s*(\d+)\s+pts:\s*(-?\d+)[^\n]*"
                      r"fmt:s16\s+channels:1\s+chlayout:mono\s+rate:16000\s+nb_samples:(\d+)", stderr)
    if not rows or not body or len(body) % 2 or len(body) > (end_ms - start_ms + 100) * 32:
        raise DecodeReview("audio_missing_or_unbounded")
    # Never repair timestamps by changing or concatenating the waveform. Split
    # at every discontinuity (even a single sample), retain only the longest
    # truly contiguous interval, and account for all discarded decoded samples.
    runs, position, byte_offset = [], None, 0
    for i, (index, pts, samples) in enumerate(rows):
        pts, samples = int(pts), int(samples)
        if int(index) != i or samples <= 0 or samples > 160000:
            raise DecodeReview("audio_frame_index_or_size_invalid")
        if position != pts:
            runs.append([pts, pts, byte_offset])
        position = pts + samples
        runs[-1][1] = position
        byte_offset += samples * 2
    if byte_offset != len(body):
        raise DecodeReview("audio_timestamp_or_byte_mismatch")
    candidates = []
    for first, end, offset in runs:
        left = max(start_ms * 16, math.ceil(first / 16) * 16)
        right = min(end_ms * 16, end // 16 * 16)
        if right > left:
            candidates.append((right - left, left, right, offset + (left - first) * 2))
    if not candidates:
        raise DecodeReview("audio_no_complete_millisecond")
    length, aligned_start, aligned_end, offset = max(candidates, key=lambda r: (r[0], -r[1]))
    if len(runs) > 1 and length < core.POLICY["min_embedding_ms"] * 16:
        raise DecodeReview("audio_no_usable_contiguous_interval")
    decoded_samples = len(body) // 2
    body = body[offset:offset + length * 2]
    return body, {"requested_start_ms": start_ms, "requested_end_ms": end_ms,
                  "start_ms": aligned_start // 16, "end_ms": aligned_end // 16,
                  "short_sample": aligned_start != start_ms * 16 or aligned_end != end_ms * 16,
                  "source_pts_verified": True, "silence_padding": False,
                  "decoded_samples": decoded_samples, "discarded_samples": decoded_samples - length,
                  "timestamp_discontinuities": len(runs) - 1, "waveform_retimed": False,
                  "pcm_sha256": hashlib.sha256(body).hexdigest()}


def decode_audio(source_fd, ffmpeg_fd, stream_index, window, *, source_start_ms=0):
    start, end = window["start_ms"], window["end_ms"]
    seek = max(0, start - 500)
    command = _prefix(source_fd, ffmpeg_fd, seek, end - seek + 100, source_start_ms) + [
        "-map", f"0:{stream_index}", "-vn", "-sn", "-dn", "-map_metadata", "-1",
        "-af", "aresample=16000:async=0,aformat=sample_fmts=s16:channel_layouts=mono,"
               f"atrim=start={seconds(start)}:end={seconds(end)},ashowinfo@triage",
        "-ac", "1", "-ar", "16000", "-c:a", "pcm_s16le", "-threads:a", "1", "-f", "s16le", "pipe:1"]
    body, stderr = bounded_process(command, pass_fds=(source_fd, ffmpeg_fd))
    return audio_receipt(body, stderr, start, end)


def face_runtime(asset):
    """Admit already acquired YuNet/OpenCV artifacts; never touch SFace."""
    def pin(value):
        return {"path": value["path"], "expected_sha256": value["sha256"], "expected_byte_count": value["byte_count"]}
    model = asset["models"]["yunet"]
    if model["sha256"] != YUNET_SHA or model["byte_count"] != 232589 or model["license_expression"] != "MIT":
        raise core.TriageError("unreviewed YuNet model")
    runtime = asset["runtime"]
    value = {"module_root": str(Path(runtime["opencv"]["binary"]["path"]).parents[1]),
             "python": {**pin(runtime["python"]), "expected_version": runtime["python"]["version"]}}
    for name in ("opencv", "numpy"):
        value[name] = {"wheel": pin(runtime[name]["wheel"]), "binary": pin(runtime[name]["binary"]),
                       "expected_version": runtime[name]["version"]}
    value["opencv"]["expected_build_information_sha256"] = runtime["opencv"]["build_information_sha256"]
    admitted = yunet.validate_runtime(value)
    speech._model_snapshot({"path": model["path"], "sha256": model["sha256"]}, 1024**2)
    cv2, np, provenance = yunet.load_runtime(admitted)
    detector = yunet.create_detector(cv2, Path(model["path"]), {
        "score_threshold": .9, "nms_threshold": .3, "top_k": 5000})
    detector.setInputSize((WIDTH, HEIGHT))
    return cv2, np, detector, provenance


def detect_faces(body, runtime):
    cv2, np, detector, _ = runtime
    frame = np.frombuffer(body, dtype=np.uint8).reshape(HEIGHT, WIDTH, 3)
    _, detected = detector.detect(frame)
    rows = [] if detected is None else detected.tolist()
    if len(rows) > 64:
        raise DecodeReview("face_density_exceeds_bound")
    faces, small = [], 0
    for row in rows:
        if len(row) != 15 or not all(math.isfinite(x) for x in row) or not .9 <= row[14] <= 1:
            raise core.TriageError("invalid YuNet detection")
        x, y, w, h = row[:4]
        left, top, right, bottom = max(0., x), max(0., y), min(float(WIDTH), x + w), min(float(HEIGHT), y + h)
        if min(right - left, bottom - top) < 12:
            small += 1
            continue
        faces.append({"box": [left, top, right - left, bottom - top], "score": row[14],
                      "clipped": left != x or top != y or right != x + w or bottom != y + h})
    return {"faces": faces, "face_count": len(faces), "small_detections_not_counted": small,
            "frame_sha256": hashlib.sha256(body).hexdigest(), "width": WIDTH, "height": HEIGHT}


class AudioEngine:
    def __init__(self, model_config):
        self.engine = speech.CpuScreenEngine(model_config, threads=1)

    def analyze(self, pcm, receipt, probe_id):
        if not any(pcm):
            return {"excerpts": [], "legacy_eligible_excerpts": 0, "vad_positive_ms": 0}
        backend = self.engine._load()
        probabilities = backend.probabilities(pcm)
        spans = core.speech_excerpts(probabilities, len(pcm) // 2)
        # Matched-input comparison with the old longest-unbroken-run selection.
        # This is eligibility/coverage, not a correctness or speaker-count metric.
        run, longest, positive = 0, 0, 0
        for i, probability in enumerate(probabilities):
            samples = min(512, len(pcm) // 2 - i * 512)
            if probability >= .5:
                run += samples; positive += samples; longest = max(longest, run)
            else:
                run = 0
        rows = []
        for i, span in enumerate(spans):
            begin, end = span["start_sample"], span["end_sample"]
            vector = backend.encode(pcm[begin * 2:end * 2])
            if not isinstance(vector, list) or len(vector) != 192 or not all(math.isfinite(x) for x in vector):
                raise core.TriageError("invalid voice embedding")
            norm = math.hypot(*vector)
            if norm <= 1e-12:
                raise core.TriageError("zero voice embedding")
            rows.append({"id": f"{probe_id}-{i}", "probe_id": probe_id,
                "start_ms": receipt["start_ms"] + begin // 16,
                "end_ms": receipt["start_ms"] + end // 16,
                "speech_ms": span["speech_samples"] // 16,
                "embedding": [float(x / norm) for x in vector]})
        return {"excerpts": core.validate_excerpts(rows), "legacy_eligible_excerpts": int(longest >= 32000),
                "vad_positive_ms": positive // 16}
