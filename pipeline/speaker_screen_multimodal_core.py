"""Bounded anonymous triage: visual cues are never speaker counts or identities.

Pure policy and evidence aggregation. Thresholds are uncalibrated review-routing
heuristics, not probabilities, speaker verification, or a replacement for diarization.
"""
from __future__ import annotations

import math
import re
from statistics import median

POLICY = {
    "frame_interval_ms": 60000, "max_frames": 32,
    "confirmation_offset_ms": 1000, "max_confirmations": 8,
    "audio_window_ms": 10000, "baseline_audio_windows": 4, "max_audio_windows": 12,
    "min_embedding_ms": 2000, "max_embedding_ms": 3000, "max_excerpts_per_window": 3,
    "vad_on": 0.5, "vad_off": 0.35, "vad_silence_ms": 256, "min_speech_fraction": 0.65,
    "match_cosine_min": 0.60, "distinct_cosine_max": 0.35,
    "min_margin": 0.20, "min_support_windows": 2, "min_group_speech_ms": 4000,
    "max_excerpts": 128, "max_anchor_pairs": 64,
}
MAX_DURATION_MS = 7 * 86400000
SEMANTICS = {
    "visibility": "private", "calibrated": False, "human_reviewed": False,
    "identity_inferred": False, "speaker_count_claimed": False,
    "faces_are_speakers": False, "face_recognition": False,
    "absence_of_faces_is_negative_audio_evidence": False,
    "whole_recording_solo_claimed": False, "full_diarization": False,
    "publication_authority": False, "source_mutation": False,
    "playback_mirrors_posters_and_offscreen_speech_require_review": True,
}


class TriageError(RuntimeError):
    pass


def integer(value, low, high, label):
    if type(value) is not int or not low <= value <= high:
        raise TriageError(f"invalid {label}")
    return value


def finite(value, low, high, label):
    if type(value) not in (int, float) or not math.isfinite(value) or not low <= value <= high:
        raise TriageError(f"invalid {label}")
    return float(value)


def interval(start_ms, end_ms):
    integer(start_ms, 0, MAX_DURATION_MS - 1, "interval start")
    integer(end_ms, start_ms + 1, MAX_DURATION_MS, "interval end")
    return end_ms - start_ms


def frame_times(start_ms, end_ms):
    """Capped midpoint strata over the entire supported video interval, not a prefix."""
    duration = interval(start_ms, end_ms)
    count = min(POLICY["max_frames"], max(3, math.ceil(duration / POLICY["frame_interval_ms"])), duration)
    return [start_ms + (2 * i + 1) * duration // (2 * count) for i in range(count)]


def audio_windows(start_ms, end_ms, targets=()):
    """Keep uniform audio coverage, then add nonoverlapping cue-adjacent probes.

    Visible cues only choose where to listen. They never remove the independent
    baseline, so calls, voiceover and audio-only recordings still receive checks.
    """
    duration = interval(start_ms, end_ms)
    width = min(POLICY["audio_window_ms"], duration)
    count = min(POLICY["baseline_audio_windows"], max(1, duration // width))
    starts = [start_ms + (duration - width) // 2] if count == 1 else [
        start_ms + i * (duration - width) // (count - 1) for i in range(count)]
    rows = [{"start_ms": start, "end_ms": start + width, "reason": "uniform_audio_baseline"} for start in starts]
    if not isinstance(targets, (list, tuple)) or len(targets) > 256:
        raise TriageError("too many targeted audio cues")
    for target in targets:
        integer(target, 0, MAX_DURATION_MS, "audio target")
    # Spread the budget across cues before adding more flanks of the first cue.
    for delta in (-width // 2, width // 2, -3 * width // 2, 3 * width // 2):
        for target in targets:
            if not start_ms <= target < end_ms:
                continue
            start = max(start_ms, min(end_ms - width, target + delta))
            if any(start < row["end_ms"] and row["start_ms"] < start + width for row in rows):
                continue
            rows.append({"start_ms": start, "end_ms": start + width, "reason": "triage_cue"})
            if len(rows) == POLICY["max_audio_windows"]:
                return [{"index": i, **row} for i, row in enumerate(rows)]
    return [{"index": i, **row} for i, row in enumerate(rows)]


def speech_excerpts(probabilities, sample_count):
    """Silero-style hysteresis, real short gaps, multiple nonoverlapping excerpts.

    No disjoint waveform concatenation or synthetic audio padding. VAD-positive
    coverage is recorded separately from the enclosing excerpt's duration.
    """
    integer(sample_count, 1, 160000, "PCM sample count")
    if not isinstance(probabilities, list) or len(probabilities) != math.ceil(sample_count / 512):
        raise TriageError("VAD probabilities do not cover the probe")
    for value in probabilities:
        finite(value, 0, 1, "VAD probability")
    start, silence_start, runs = None, None, []
    for i, value in enumerate(probabilities):
        begin = i * 512
        if start is None:
            if value >= POLICY["vad_on"]:
                start = begin
        elif value < POLICY["vad_off"]:
            if silence_start is None:
                silence_start = begin
            if begin + 512 - silence_start >= POLICY["vad_silence_ms"] * 16:
                runs.append((start, silence_start))
                start, silence_start = None, None
        else:
            silence_start = None
    if start is not None:
        runs.append((start, sample_count if silence_start is None else silence_start))
    minimum, maximum = POLICY["min_embedding_ms"] * 16, POLICY["max_embedding_ms"] * 16
    candidates = []
    for begin, end in runs:
        begin, end = (begin + 15) // 16 * 16, end // 16 * 16
        count = (end - begin) // minimum
        if count < 1:
            continue
        count = min(count, math.ceil((end - begin) / maximum))
        width = min(maximum, (end - begin) // count // 16 * 16)
        for i in range(count):
            left = begin + (i * (end - begin - width) // max(1, count - 1) // 16 * 16)
            right = left + width
            positive = sum(max(0, min(right, (j + 1) * 512, sample_count) - max(left, j * 512))
                           for j, value in enumerate(probabilities) if value >= POLICY["vad_on"])
            if positive / width >= POLICY["min_speech_fraction"]:
                candidates.append({"start_sample": left, "end_sample": right, "speech_samples": positive})
    cap = POLICY["max_excerpts_per_window"]
    if len(candidates) > cap:
        candidates = [candidates[i * (len(candidates) - 1) // (cap - 1)] for i in range(cap)]
    return candidates


def validate_excerpts(rows):
    if not isinstance(rows, list) or len(rows) > POLICY["max_excerpts"]:
        raise TriageError("audio evidence exceeds excerpt bound")
    result, seen = [], set()
    for row in rows:
        if not isinstance(row, dict) or set(row) != {"id", "probe_id", "start_ms", "end_ms", "speech_ms", "embedding"}:
            raise TriageError("audio excerpt fields differ")
        for key in ("id", "probe_id"):
            if not isinstance(row[key], str) or not 1 <= len(row[key]) <= 128:
                raise TriageError("invalid audio excerpt identity")
        if row["id"] in seen:
            raise TriageError("duplicate audio excerpt")
        seen.add(row["id"])
        width = interval(row["start_ms"], row["end_ms"])
        integer(width, 2000, 5000, "embedding duration")
        integer(row["speech_ms"], 1, width, "observed speech")
        vector = row["embedding"]
        if not isinstance(vector, list) or len(vector) != 192:
            raise TriageError("expected 192-dimensional voice evidence")
        for value in vector:
            finite(value, -1, 1, "normalized embedding coordinate")
        norm = math.hypot(*vector)
        if abs(norm - 1) > 1e-5:
            raise TriageError("voice evidence is not unit normalized")
        result.append(dict(row))
    return result


def audio_diversity(excerpts):
    """Find two repeatedly supported, separated acoustic cores, never force k=2.

    Isolated outliers do not establish a second voice. Each core needs independent
    nonoverlapping probes; an old and a fresh excerpt of the same audio cannot
    supply two votes. Bounded anchor search is deliberately not exhaustive.
    """
    rows = validate_excerpts(excerpts)
    n = len(rows)
    similarities = [[1.0] * n for _ in range(n)]
    for i in range(n):
        for j in range(i):
            similarities[i][j] = similarities[j][i] = max(-1., min(1., math.fsum(
                a * b for a, b in zip(rows[i]["embedding"], rows[j]["embedding"]))))

    def core(anchor, rival):
        group = []
        for index in sorted(range(n), key=lambda i: (-similarities[anchor][i], rows[i]["id"])):
            if (similarities[anchor][index] < POLICY["match_cosine_min"] or
                    similarities[anchor][index] - similarities[rival][index] < POLICY["min_margin"]):
                continue
            if any(similarities[index][member] < POLICY["match_cosine_min"] or
                   rows[index]["probe_id"] == rows[member]["probe_id"] or
                   (rows[index]["start_ms"] < rows[member]["end_ms"] and
                    rows[member]["start_ms"] < rows[index]["end_ms"]) for member in group):
                continue
            group.append(index)
        return group

    pairs = sorted((similarities[i][j], i, j) for i in range(n) for j in range(i)
                   if similarities[i][j] <= POLICY["distinct_cosine_max"])
    support = None
    examined = 0
    for _, i, j in pairs[:POLICY["max_anchor_pairs"]]:
        examined += 1
        a, b = core(i, j), core(j, i)
        if any(len(group) < POLICY["min_support_windows"] or
               sum(rows[k]["speech_ms"] for k in group) < POLICY["min_group_speech_ms"] for group in (a, b)):
            continue
        if any(rows[x]["start_ms"] < rows[y]["end_ms"] and rows[y]["start_ms"] < rows[x]["end_ms"]
               for x in a for y in b):
            continue
        cross = max(similarities[x][y] for x in a for y in b)
        if cross > POLICY["distinct_cosine_max"]:
            continue
        support = {"groups": [[{key: rows[k][key] for key in ("id", "probe_id", "start_ms", "end_ms", "speech_ms")}
                                for k in group] for group in (a, b)], "maximum_cross_cosine": cross,
                   "minimum_within_cosine": min(similarities[x][y] for group in (a, b)
                                                for x in group for y in group)}
        break
    values = [similarities[i][j] for i in range(n) for j in range(i)]
    return {"state": "supported_audio_diversity" if support else
            "insufficient_audio" if n < 4 else "no_supported_diversity_in_samples",
            "usable_excerpts": n, "support": support, "anchor_pairs_examined": examined,
            "anchor_search_capped": len(pairs) > POLICY["max_anchor_pairs"],
            "pairwise_cosine": {"minimum": min(values), "median": median(values), "maximum": max(values)} if values else None,
            "scores_are_probabilities": False, "speaker_count": None}


def visual_summary(frames, planned_count, *, video_state="available"):
    integer(planned_count, 0, POLICY["max_frames"] + POLICY["max_confirmations"], "planned frames")
    if not isinstance(frames, list) or len(frames) > planned_count:
        raise TriageError("excess visual evidence")
    if video_state not in {"available", "no_video", "unsupported_timing"}:
        raise TriageError("invalid video state")
    good, errors, seen = [], 0, set()
    for row in frames:
        if not isinstance(row, dict) or row.get("state") not in {"decoded", "needs_review"}:
            raise TriageError("invalid frame outcome")
        target = integer(row.get("target_ms"), 0, MAX_DURATION_MS, "frame target")
        if target in seen:
            raise TriageError("duplicate frame target")
        seen.add(target)
        if row["state"] == "needs_review":
            errors += 1
            continue
        integer(row.get("actual_ms"), 0, MAX_DURATION_MS, "observed frame time")
        if not target <= row["actual_ms"] <= target + 2000:
            raise TriageError("frame timestamp outside requested seek tolerance")
        if not isinstance(row.get("frame_sha256"), str) or not re.fullmatch(r"[0-9a-f]{64}", row["frame_sha256"]):
            raise TriageError("invalid frame digest")
        count = integer(row.get("face_count"), 0, 64, "detected faces")
        if row.get("width") != 640 or row.get("height") != 360 or not isinstance(row.get("faces"), list) or len(row["faces"]) != count:
            raise TriageError("face count or image dimensions differ")
        for face in row["faces"]:
            if not isinstance(face, dict) or set(face) != {"box", "score", "clipped"} or type(face["clipped"]) is not bool:
                raise TriageError("invalid face fields")
            finite(face["score"], .9, 1, "face score")
            box = face["box"]
            if not isinstance(box, list) or len(box) != 4:
                raise TriageError("invalid face box")
            x, y, w, h = (finite(v, 0, 640, "face coordinate") for v in box)
            if min(w, h) < 12 or x + w > 640 or y + h > 360:
                raise TriageError("face box outside frame")
        if count >= 2:
            good.append(row)
    # Duplicate PTS/hashes from static covers cannot create repeat corroboration.
    unique = {(r["actual_ms"], r["frame_sha256"]) for r in good}
    hashes = {r["frame_sha256"] for r in good}
    repeat = len(unique) >= 2 and len(hashes) >= 2 and max((r["actual_ms"] for r in good), default=0) - min(
        (r["actual_ms"] for r in good), default=0) >= 500
    return {"state": "repeated_multiple_faces" if repeat else "multiple_faces_in_one_sample" if good else
            "no_multiple_faces_observed" if video_state == "available" and len(frames) > errors else
            "unusable_visual_samples" if video_state == "available" else video_state,
            "frames_planned": planned_count, "frames_decoded": len(frames) - errors, "frames_needing_review": errors,
            "multiple_face_samples": len(good), "repeated_visual_cue": repeat,
            "audio_target_ms": sorted({r["actual_ms"] for r in good}),
            "people_or_speaker_count": None, "sparse_sampling_may_miss_people": True}


def route(audio, visual):
    a = audio["state"] == "supported_audio_diversity"
    v = visual["repeated_visual_cue"]
    label, priority = (("audio_and_visual_cues", 90) if a and v else
                       ("audio_diversity_candidate", 80) if a else
                       ("visual_cue_needs_audio_review", 60) if v else
                       ("insufficient_audio", 40) if audio["state"] == "insufficient_audio" else
                       ("no_supported_multispeaker_evidence_in_samples", 20))
    return {"label": label, "review_priority": priority,
            "recommended_action": "review_or_targeted_diarization" if a else
            "targeted_audio_review" if v else "retain_uncertain_and_sample_more_if_needed",
            "automatic_diarization_authorized": False, "semantics": dict(SEMANTICS)}
