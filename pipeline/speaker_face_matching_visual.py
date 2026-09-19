#!/usr/bin/env python3
"""Bounded, anonymous audio/visual preparation for private matching pilots.

This is deliberately not the reviewed-only YuNet work-order interface.  It neither
loads models nor launches subprocesses.  The supervisor owns decoding and process
limits, and an admitted offline worker injects its NumPy/OpenCV/YuNet objects.
Only the old tracker's generic geometry helper is reused.  No recognition,
embeddings, cross-clip tracking, identity, or publication authority is provided.
"""

from __future__ import annotations

import hashlib
import json
import math
import re
from fractions import Fraction
from pathlib import Path
from typing import Any

from pipeline.shot_local_face_tracker import iou
from pipeline.speaker_screen import FORMATS


WIDTH = 640
HEIGHT = 360
FPS = 25
FRAME_MS = 40
AUDIO_RATE = 16000
SAMPLES_PER_FRAME = 640
MAX_FRAMES = 125
MIN_FRAMES = 25
MAX_SOURCE_FRAMES = 1500
MAX_SOURCE_FRAME_MS = 50
MAX_DETECTIONS_PER_FRAME = 8
MAX_TRACKS = 256
MAX_STDERR_BYTES = 2 * 1024 * 1024
FRAME_BYTES = WIDTH * HEIGHT * 3
CROP_SIZE = 112
MIN_FACE_EXTENT = 32
DETECTION_THRESHOLD = 0.9
IOU_THRESHOLD = 0.4
CROP_RECIPE = "yunet_square_1.25_resize224_center112_gray_unvalidated_v1"
TIMING_RECIPE = "absolute_pts_25fps_16k_async0_no_audio_padding_v1"
ID_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9_.:-]{0,127}$")


class VisualMatchingError(RuntimeError):
    """A bounded visual input, timeline, detector, or geometry contract failed."""


VisualError = VisualMatchingError


def shot_id(clip_id: str, shot_index: int) -> str:
    return "shot_" + hashlib.sha256(f"{clip_id}:{shot_index}".encode()).hexdigest()[:32]


def _integer(value: object, label: str, minimum: int, maximum: int) -> int:
    if type(value) is not int or not minimum <= value <= maximum:
        raise VisualMatchingError(f"{label} must be an integer in [{minimum}, {maximum}]")
    return value


def validate_clip(start_ms: object, end_ms: object) -> int:
    start = _integer(start_ms, "start_ms", 0, 14 * 86400000)
    end = _integer(end_ms, "end_ms", 0, 14 * 86400000)
    if start % FRAME_MS or end % FRAME_MS:
        raise VisualMatchingError("clip endpoints must lie on the absolute 40 ms grid")
    count = (end - start) // FRAME_MS
    return _integer(count, "clip frame count", MIN_FRAMES, MAX_FRAMES)


def _seconds(ms: int) -> str:
    return f"{ms // 1000}.{ms % 1000:03d}"


def _path(value: object, label: str) -> str:
    if not isinstance(value, (str, Path)):
        raise VisualMatchingError(f"{label} must be an absolute path")
    value = str(value)
    if not value or "\x00" in value or "\n" in value or not Path(value).is_absolute():
        raise VisualMatchingError(f"{label} must be an absolute local path")
    return value


def build_decode_commands(ffmpeg: str, source: str, start_ms: int, end_ms: int,
                          video_out: str, audio_out: str) -> list[dict[str, Any]]:
    """Describe two bounded CPU decodes; caller must enforce limits and deadlines.

    The commands preserve the source's absolute timestamps.  They never use
    STARTPTS, async audio compensation, silence padding, or video end padding.
    FPS normalization may quantize a source frame by at most one 40 ms period;
    the receipt separately checks source-frame coverage and discontinuities.
    """
    count = validate_clip(start_ms, end_ms)
    ffmpeg, source = _path(ffmpeg, "ffmpeg"), _path(source, "source")
    video_out, audio_out = _path(video_out, "video output"), _path(audio_out, "audio output")
    if len({ffmpeg, source, video_out, audio_out}) != 4:
        raise VisualMatchingError("decode executable, source, and outputs must be distinct")
    start, end = _seconds(start_ms), _seconds(end_ms)
    seek_ms = max(0, start_ms - 200)
    # Bounded preroll retains partial audio packets and resampler context.  Extra
    # source frames supply fps EOF coverage, but absolute trim bounds every emitted
    # array to the requested interval.  Parent output caps remain exact.
    prefix = [ffmpeg, "-hide_banner", "-nostdin", "-nostats", "-loglevel", "info",
              "-n", "-threads", "1", "-filter_threads", "1", "-fflags", "+bitexact",
              "-hwaccel", "none", "-protocol_whitelist", "file,pipe", "-format_whitelist", FORMATS,
              "-copyts", "-seek_timestamp", "1", "-ss", _seconds(seek_ms),
              "-t", _seconds(end_ms - seek_ms + 80), "-i", source]
    video_filter = (
        f"trim=start={start}:end={end},showinfo@match_source,"
        f"fps=fps=25:start_time={start}:round=near:eof_action=pass,"
        f"trim=start={start}:end={end},"
        "scale=640:360:force_original_aspect_ratio=decrease:flags=bilinear,"
        "pad=640:360:(ow-iw)/2:(oh-ih)/2:color=black,setsar=1,"
        "format=bgr24,showinfo@match_output"
    )
    video = prefix + ["-map", "0:v:0", "-an", "-sn", "-dn", "-map_metadata", "-1",
                      "-map_chapters", "-1", "-vf", video_filter, "-fps_mode", "passthrough",
                      "-c:v", "rawvideo", "-threads:v", "1", "-pix_fmt", "bgr24",
                      "-f", "rawvideo", video_out]
    audio_filter = (f"aresample=16000:async=0,"
                    f"aformat=sample_fmts=s16:channel_layouts=mono,"
                    f"atrim=start={start}:end={end},ashowinfo@match_audio")
    audio = prefix + ["-map", "0:a:0", "-vn", "-sn", "-dn", "-map_metadata", "-1",
                      "-map_chapters", "-1", "-af", audio_filter, "-ac", "1", "-ar", "16000",
                      "-c:a", "pcm_s16le", "-threads:a", "1", "-f", "s16le", audio_out]
    return [{"kind": kind, "argv": argv, "stdout_max": 65536,
             "stderr_max": MAX_STDERR_BYTES, "max_file_bytes": maximum, "output_path": output}
            for kind, argv, maximum, output in (
                ("video", video, count * FRAME_BYTES, video_out),
                ("audio", audio, count * SAMPLES_PER_FRAME * 2, audio_out))]


def _stderr(value: str | bytes, label: str) -> str:
    if isinstance(value, bytes):
        if len(value) > MAX_STDERR_BYTES:
            raise VisualMatchingError(f"{label} exceeds stderr byte limit")
        try:
            value = value.decode("utf-8", errors="strict")
        except UnicodeDecodeError as error:
            raise VisualMatchingError(f"{label} is not UTF-8") from error
    if not isinstance(value, str) or len(value.encode("utf-8")) > MAX_STDERR_BYTES:
        raise VisualMatchingError(f"{label} exceeds stderr byte limit")
    return value


def _video_rows(stderr: str, name: str, maximum: int) -> list[tuple[Fraction, Fraction]]:
    prefix = rf"\[showinfo@{re.escape(name)}\s+@[^\]]+\]"
    configs = re.findall(prefix + r" config in time_base: (\d+)/(\d+),", stderr)
    if len(configs) != 1 or int(configs[0][0]) <= 0 or int(configs[0][1]) <= 0:
        raise VisualMatchingError(f"{name} requires one exact positive source time base")
    base = Fraction(int(configs[0][0]), int(configs[0][1]))
    lines = re.findall(prefix + r"[^\n]*\bn:\s*[^\n]*", stderr)
    if not 1 <= len(lines) <= maximum:
        raise VisualMatchingError(f"{name} missing or excessive timestamp rows")
    rows = []
    pattern = re.compile(r"\bn:\s*(\d+)\s+pts:\s*(-?\d+)\s+pts_time:\S+\s+"
                         r"duration:\s*(-?\d+)\s+duration_time:\S+")
    for ordinal, line in enumerate(lines):
        row = pattern.search(line)
        if not row or int(row[1]) != ordinal or int(row[3]) <= 0:
            raise VisualMatchingError(f"{name} has missing, malformed, or unordered timestamp evidence")
        rows.append((int(row[2]) * base, int(row[3]) * base))
    return rows


def _audio_rows(stderr: str) -> list[tuple[int, int]]:
    lines = re.findall(r"\[ashowinfo@match_audio\s+@[^\]]+\][^\n]*\bn:\s*[^\n]*", stderr)
    if not 1 <= len(lines) <= 10000:
        raise VisualMatchingError("audio requires bounded timestamp evidence")
    pattern = re.compile(r"\bn:\s*(\d+)\s+pts:\s*(-?\d+)\s+pts_time:\S+.*?"
                         r"fmt:s16\s+channels:1\s+chlayout:mono\s+rate:16000\s+nb_samples:(\d+)")
    rows = []
    for ordinal, line in enumerate(lines):
        row = pattern.search(line)
        if not row or int(row[1]) != ordinal or int(row[3]) <= 0:
            raise VisualMatchingError("audio has malformed, non-mono, or unordered timestamp evidence")
        rows.append((int(row[2]), int(row[3])))
    return rows


def _check_bytes(video_bytes: bytes, audio_bytes: bytes, count: int) -> None:
    if not isinstance(video_bytes, bytes) or len(video_bytes) != count * FRAME_BYTES:
        raise VisualMatchingError("decoded video byte count does not match complete bounded clip")
    if not isinstance(audio_bytes, bytes) or len(audio_bytes) != count * SAMPLES_PER_FRAME * 2:
        raise VisualMatchingError("decoded audio byte count does not match complete bounded clip")


def _timeline_receipt(source: list[tuple[Fraction, Fraction]], output: list[tuple[Fraction, Fraction]],
                      audio: list[tuple[int, int]], *, start_ms: int, end_ms: int) -> dict[str, Any]:
    count = validate_clip(start_ms, end_ms)
    if not 1 <= len(source) <= MAX_SOURCE_FRAMES or not 1 <= len(output) <= MAX_FRAMES or not 1 <= len(audio) <= 10000:
        raise VisualMatchingError("timestamp proof rows are missing or exceed bounds")
    start, end, period = Fraction(start_ms, 1000), Fraction(end_ms, 1000), Fraction(1, FPS)
    if not start <= source[0][0] < start + period:
        raise VisualMatchingError("source video starts outside one-frame sampling tolerance")
    for previous, current in zip(source, source[1:]):
        if current[0] <= previous[0] or abs(current[0] - sum(previous)) > Fraction(1, 1000):
            raise VisualMatchingError("source video contains a timestamp gap or overlap")
    if any(duration > Fraction(MAX_SOURCE_FRAME_MS, 1000) for _, duration in source):
        raise VisualMatchingError("source video frame duration is too long for reliable matching")
    if source[-1][0] >= end or sum(source[-1]) < end - Fraction(1, 1000):
        raise VisualMatchingError("source video does not cover clip EOF")
    if len(output) != count or any(row != (start + index * period, period)
                                    for index, row in enumerate(output)):
        raise VisualMatchingError("normalized video timestamps are not the requested absolute 25 fps grid")
    expected_sample = start_ms * 16
    for pts, samples in audio:
        if pts != expected_sample:
            raise VisualMatchingError("audio timestamp gap, overlap, or initial A/V offset")
        expected_sample += samples
    if expected_sample != end_ms * 16:
        raise VisualMatchingError("audio timestamp coverage differs from clip EOF")
    return {"kind": "himr_speaker_face_decode_receipt", "schema_version": 1,
            "recipe": TIMING_RECIPE, "start_ms": start_ms, "end_ms": end_ms,
            "frame_count": count, "width": WIDTH, "height": HEIGHT, "fps": FPS,
            "audio_rate": AUDIO_RATE, "audio_channels": 1,
            "source_frame_count": len(source),
            "source_first_frame_offset_ms": float((source[0][0] - start) * 1000),
            "source_video_gap_tolerance_ms": 1,
            "source_max_frame_duration_ms": MAX_SOURCE_FRAME_MS,
            "decode_seek_preroll_ms": min(200, start_ms),
            "av_sync_verified": True, "av_sync_scope": "container_timestamp_alignment_only",
            "source_lip_sync_verified": False, "audio_padding": False,
            "video_bytes": count * FRAME_BYTES, "audio_bytes": count * SAMPLES_PER_FRAME * 2,
            "source_timestamps": [[pts.numerator, pts.denominator, dur.numerator, dur.denominator]
                                  for pts, dur in source],
            "output_timestamps": [[pts.numerator, pts.denominator, dur.numerator, dur.denominator]
                                  for pts, dur in output],
            "audio_timestamps": [list(row) for row in audio]}


def validate_decoded(video_bytes: bytes, audio_bytes: bytes, *, start_ms: int, end_ms: int,
                     video_stderr: str | bytes, audio_stderr: str | bytes) -> dict[str, Any]:
    """Prove timestamp alignment, not lip sync; never repair gaps or shifted origins."""
    count = validate_clip(start_ms, end_ms)
    _check_bytes(video_bytes, audio_bytes, count)
    video_log, audio_log = _stderr(video_stderr, "video log"), _stderr(audio_stderr, "audio log")
    receipt = _timeline_receipt(_video_rows(video_log, "match_source", MAX_SOURCE_FRAMES),
                                _video_rows(video_log, "match_output", MAX_FRAMES),
                                _audio_rows(audio_log), start_ms=start_ms, end_ms=end_ms)
    receipt.update({"video_sha256": hashlib.sha256(video_bytes).hexdigest(),
                    "audio_sha256": hashlib.sha256(audio_bytes).hexdigest(),
                    "video_stderr_sha256": hashlib.sha256(video_log.encode()).hexdigest(),
                    "audio_stderr_sha256": hashlib.sha256(audio_log.encode()).hexdigest()})
    return validate_receipt(receipt, start_ms=start_ms, end_ms=end_ms)


def validate_receipt(receipt: object, *, start_ms: int, end_ms: int) -> dict[str, Any]:
    """Replay a sealed timing receipt; this does not rehash deleted raw clip bytes."""
    if not isinstance(receipt, dict):
        raise VisualMatchingError("decode receipt must be an object")
    video_rows = []
    for field, maximum in (("source_timestamps", MAX_SOURCE_FRAMES), ("output_timestamps", MAX_FRAMES)):
        raw = receipt.get(field)
        if not isinstance(raw, list) or not 1 <= len(raw) <= maximum:
            raise VisualMatchingError(f"{field} missing or excessive")
        rows = []
        for row in raw:
            if not isinstance(row, list) or len(row) != 4:
                raise VisualMatchingError(f"{field} must contain four-integer rational timestamp rows")
            for index, value in enumerate(row):
                _integer(value, f"{field} value", 0 if index == 0 else 1, 10**15)
            pts, duration = Fraction(row[0], row[1]), Fraction(row[2], row[3])
            if [pts.numerator, pts.denominator, duration.numerator, duration.denominator] != row:
                raise VisualMatchingError("receipt rational timestamps must be canonical")
            rows.append((pts, duration))
        video_rows.append(rows)
    raw_audio = receipt.get("audio_timestamps")
    if not isinstance(raw_audio, list) or not 1 <= len(raw_audio) <= 10000:
        raise VisualMatchingError("receipt audio timestamps missing or excessive")
    audio = []
    for row in raw_audio:
        if not isinstance(row, list) or len(row) != 2:
            raise VisualMatchingError("receipt audio timestamps must contain two integers")
        audio.append((_integer(row[0], "audio PTS", 0, 10**15),
                      _integer(row[1], "audio sample count", 1, MAX_FRAMES * SAMPLES_PER_FRAME)))
    replay = _timeline_receipt(*video_rows, audio, start_ms=start_ms, end_ms=end_ms)
    for field in ("video_sha256", "audio_sha256", "video_stderr_sha256", "audio_stderr_sha256"):
        digest = receipt.get(field)
        if not isinstance(digest, str) or not re.fullmatch(r"[0-9a-f]{64}", digest):
            raise VisualMatchingError(f"receipt {field} must be a SHA-256")
        replay[field] = digest
    try:
        if json.dumps(receipt, sort_keys=True, allow_nan=False) != json.dumps(replay, sort_keys=True, allow_nan=False):
            raise VisualMatchingError("decode receipt differs from strict canonical timeline replay")
    except (TypeError, ValueError) as error:
        raise VisualMatchingError("decode receipt contains unsupported values") from error
    return replay


def prepare_arrays(video_bytes: bytes, audio_bytes: bytes, *, start_ms: int, end_ms: int,
                   np: Any) -> tuple[Any, Any]:
    count = validate_clip(start_ms, end_ms)
    _check_bytes(video_bytes, audio_bytes, count)
    return (np.frombuffer(video_bytes, dtype=np.uint8).reshape(count, HEIGHT, WIDTH, 3),
            np.frombuffer(audio_bytes, dtype="<i2"))


def _face_rows(raw: Any) -> list[dict[str, float]]:
    rows = [] if raw is None else list(raw)
    if len(rows) > MAX_DETECTIONS_PER_FRAME:
        raise VisualMatchingError("YuNet detection count exceeds the fixed eight-face limit")
    normalized = []
    for raw_row in rows:
        row = raw_row.tolist() if hasattr(raw_row, "tolist") else raw_row
        if not isinstance(row, list) or len(row) != 15:
            raise VisualMatchingError("YuNet must emit exactly 15 values per face")
        if any(isinstance(v, bool) or not isinstance(v, (int, float)) or not math.isfinite(v) for v in row):
            raise VisualMatchingError("YuNet face values must be finite numbers")
        x, y, width, height = map(float, row[:4])
        if x < 0 or y < 0 or width <= 0 or height <= 0 or x + width > WIDTH or y + height > HEIGHT:
            raise VisualMatchingError("YuNet face box is outside the frame")
        if not 0 <= row[14] <= 1 or any(not 0 <= row[i] <= WIDTH or not 0 <= row[i + 1] <= HEIGHT
                                       for i in range(4, 14, 2)):
            raise VisualMatchingError("YuNet confidence or landmarks are invalid")
        if row[14] >= DETECTION_THRESHOLD:
            normalized.append({"x": x, "y": y, "width": width, "height": height,
                               "detector_score": float(row[14])})
    return sorted(normalized, key=lambda box: (box["x"], box["y"], -box["detector_score"]))


def _crop(frame: Any, box: dict[str, float], cv2: Any, np: Any) -> Any | None:
    if min(box["width"], box["height"]) < MIN_FACE_EXTENT:
        return None
    extent = max(box["width"], box["height"]) * 1.25
    cx, cy = box["x"] + box["width"] / 2, box["y"] + box["height"] / 2
    left, top = math.floor(cx - extent / 2), math.floor(cy - extent / 2)
    right, bottom = math.ceil(cx + extent / 2), math.ceil(cy + extent / 2)
    if left < 0 or top < 0 or right > WIDTH or bottom > HEIGHT:
        return None  # Never synthesize border pixels or silently shift face centers.
    gray = cv2.cvtColor(frame[top:bottom, left:right], cv2.COLOR_BGR2GRAY)
    resized = cv2.resize(gray, (224, 224), interpolation=cv2.INTER_LINEAR)
    crop = np.ascontiguousarray(resized[56:168, 56:168])
    if crop.dtype != np.uint8 or crop.shape != (CROP_SIZE, CROP_SIZE):
        raise VisualMatchingError("crop adapter did not produce 112×112 uint8 grayscale")
    return crop


def detect_and_track(frames: Any, *, clip_id: str, start_ms: int, detector: Any,
                      np: Any, cv2: Any) -> dict[str, Any]:
    """Make conservative contiguous, clip-and-shot-local anonymous crop tracks.

    A missed detection, ambiguous geometry assignment, or detected shot cut ends a
    track.  No crop interpolation or reidentification is attempted.  Cuts are a
    heuristic; any reported cut makes the entire clip unscoreable in the parent.
    """
    if not isinstance(clip_id, str) or not ID_RE.fullmatch(clip_id):
        raise VisualMatchingError("clip_id must be a bounded local identifier")
    if not hasattr(frames, "shape") or len(frames.shape) != 4 or frames.shape[1:] != (HEIGHT, WIDTH, 3):
        raise VisualMatchingError("visual worker requires bounded 640×360 BGR frames")
    count = validate_clip(start_ms, start_ms + len(frames) * FRAME_MS)
    if frames.dtype != np.uint8:
        raise VisualMatchingError("visual worker requires uint8 frames")
    cv2.setNumThreads(1)
    if hasattr(cv2, "ocl") and hasattr(cv2.ocl, "setUseOpenCL"):
        cv2.ocl.setUseOpenCL(False)
    detector.setInputSize((WIDTH, HEIGHT))
    tracks: list[dict[str, Any]] = []
    active: list[dict[str, Any]] = []
    cuts: list[int] = []
    shot_index = 0
    rejected_border = 0
    rejected_small = 0
    ambiguous_frames: list[int] = []
    previous_small = None
    for index, frame in enumerate(frames):
        small = frame[::8, ::8].astype(np.int16)
        if previous_small is not None:
            difference = np.abs(small - previous_small)
            if float(difference.mean()) / 255 >= 0.18 or float((difference > 30).mean()) >= 0.65:
                cuts.append(index)
                shot_index += 1
                active = []
        previous_small = small
        try:
            _, raw_faces = detector.detect(frame)
        except Exception as error:
            raise VisualMatchingError(f"bounded YuNet inference failed: {error}") from error
        candidates = []
        for box in _face_rows(raw_faces):
            crop = _crop(frame, box, cv2, np)
            if crop is None:
                if min(box["width"], box["height"]) < MIN_FACE_EXTENT:
                    rejected_small += 1
                else:
                    rejected_border += 1
            # A detected but uncroppable face is still a possible competing
            # speaker.  Preserve its geometric observations with no model score;
            # dropping it would make another face look spuriously unambiguous.
            candidates.append((box, crop))
        # Only mutual one-to-one geometric edges are admitted; close/crossing
        # faces split tracks rather than receiving an arbitrary identity match.
        edges = {(ti, di) for ti, track in enumerate(active) for di, (box, _) in enumerate(candidates)
                 if iou(track["boxes"][-1], box) >= IOU_THRESHOLD}
        track_degree = {ti: sum(edge[0] == ti for edge in edges) for ti in range(len(active))}
        detection_degree = {di: sum(edge[1] == di for edge in edges) for di in range(len(candidates))}
        assignments = {di: active[ti] for ti, di in edges
                       if track_degree[ti] == 1 and detection_degree[di] == 1}
        if any(value > 1 for value in (*track_degree.values(), *detection_degree.values())):
            ambiguous_frames.append(index)
        next_active = []
        for di, (box, crop) in enumerate(candidates):
            track = assignments.get(di)
            if track is None:
                if len(tracks) >= MAX_TRACKS:
                    raise VisualMatchingError("clip exceeds the fixed anonymous-track limit")
                track_id = "face_track_" + hashlib.sha256(f"{clip_id}:{len(tracks)}".encode()).hexdigest()[:32]
                track = {"track_id": track_id,
                         "shot_id": shot_id(clip_id, shot_index),
                         "frame_indices": [], "boxes": [], "crops": []}
                tracks.append(track)
            track["frame_indices"].append(index)
            track["boxes"].append(dict(box))
            track["crops"].append(crop)
            next_active.append(track)
        active = next_active
    for track in tracks:
        track["crops"] = None if any(crop is None for crop in track["crops"]) else np.stack(track["crops"])
    return {"tracks": tracks, "cuts": cuts,
            "metadata": {"frame_count": count, "crop_recipe": CROP_RECIPE,
                         "preprocessing_calibrated": False, "detector_score_threshold": DETECTION_THRESHOLD,
                         "iou_threshold": IOU_THRESHOLD, "opencv_threads": 1,
                         "tracking": "mutual_unique_iou_contiguous_clip_and_shot_local",
                         "cut_detector": "subsampled_bgr_absdiff_0.18_or_changed_fraction_0.65_v1",
                         "cuts_are_heuristic": True, "cut_clip_scoreable": not cuts,
                         "rejected_border_detections": rejected_border,
                         "rejected_small_detections": rejected_small,
                         "ambiguous_geometry_frames": ambiguous_frames,
                         "identity_state": "unknown", "embeddings_present": False,
                         "cross_clip_linking": False, "publication": False}}
