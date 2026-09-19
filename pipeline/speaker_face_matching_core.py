"""Bounded, deterministic, anonymous audio/video speaker matching contracts.

This module does no I/O or inference. A candidate links one diarization speaker
to a shot-local visual track only inside a sampled clip. It does not recognize
people, join tracks across shots, calibrate logits, or authorize publication.
"""
from __future__ import annotations

from collections import defaultdict
import hashlib
import json
import math
import re

from pipeline import screened_diarization_core as diarization


class MatchingError(RuntimeError):
    pass


DEFAULT_POLICY = {
    "min_clip_ms": 2000, "max_clip_ms": 5000, "boundary_guard_ms": 200,
    "clips_per_speaker": 3, "max_clips": 96, "min_face_px": 64,
    "max_frame_gap_ms": 80, "min_track_coverage": 0.9,
    "min_active_fraction": 0.8, "min_mean_raw_logit": 0.0,
    "min_winner_margin": 0.5,
}
MAX_DOCUMENT_BYTES = 16 * 1024**2
SEMANTICS = {
    "identity_inferred": False, "named_labels": False,
    "cross_recording_matching": False, "cross_shot_matching": False,
    "unsampled_intervals_attributed": False, "publication_authority": False,
    "human_reviewed": False, "scores_calibrated": False,
    "probabilities_produced": False, "calibration_state": "uncalibrated",
    "labels_scope": "diarization_run_and_media_sha256_and_clip_and_shot",
    "verified_sync_means": "decoded_timestamp_alignment_not_lip_sync_or_ground_truth",
    "raw_logit_semantics": "independent_active_speaker_logits_not_probabilities",
}


def _canonical(value):
    try:
        body = json.dumps(value, sort_keys=True, separators=(",", ":"),
                          allow_nan=False).encode("utf-8")
    except (TypeError, ValueError, RecursionError) as error:
        raise MatchingError("matching document must be finite JSON") from error
    if len(body) > MAX_DOCUMENT_BYTES:
        raise MatchingError("matching document exceeds 16 MiB")
    return body


def _exact(value, fields, label):
    if not isinstance(value, dict) or set(value) != set(fields):
        raise MatchingError(label + " fields differ")


def _integer(value, low, high, label):
    if type(value) is not int or not low <= value <= high:
        raise MatchingError(f"{label} must be an integer in {low}..{high}")
    return value


def _number(value, low, high, label):
    if type(value) not in (int, float) or not low <= value <= high or not math.isfinite(value):
        raise MatchingError(f"{label} must be a finite number in {low}..{high}")
    return float(value)


def _token(value, prefix, length, label):
    if not isinstance(value, str) or re.fullmatch(prefix + r"[0-9a-f]{" + str(length) + "}", value) is None:
        raise MatchingError("invalid " + label)
    return value


def _id(prefix, value):
    return prefix + hashlib.sha256(_canonical(value)).hexdigest()[:32]


def validate_policy(value=None):
    """Return a fresh exact policy; thresholds are private uncalibrated heuristics."""
    if value is None:
        value = DEFAULT_POLICY
    _exact(value, DEFAULT_POLICY, "matching policy")
    result = dict(value)
    for key, low, high in (("min_clip_ms", 2000, 5000), ("max_clip_ms", 2000, 5000),
                           ("boundary_guard_ms", 0, 1000), ("clips_per_speaker", 1, 16),
                           ("max_clips", 1, 256), ("min_face_px", 32, 512),
                           ("max_frame_gap_ms", 40, 200)):
        _integer(value[key], low, high, key)
    if value["min_clip_ms"] > value["max_clip_ms"]:
        raise MatchingError("minimum clip length exceeds maximum")
    if value["min_clip_ms"] % 40 or value["max_clip_ms"] % 40:
        raise MatchingError("clip lengths must use the 25 fps, 40 ms grid")
    for key, low, high in (("min_track_coverage", 0.8, 1.0), ("min_active_fraction", 0.5, 1.0),
                           ("min_mean_raw_logit", -20.0, 20.0), ("min_winner_margin", 0.0, 20.0)):
        result[key] = _number(value[key], low, high, key)
    return result


def validate_clip(value):
    _exact(value, {"clip_id", "run_id", "media_sha256", "speaker", "start_ms", "end_ms",
                   "source_turn_indices"}, "matching clip")
    _token(value["run_id"], "diarjob_", 32, "diarization run")
    _token(value["media_sha256"], "", 64, "media SHA-256")
    if not isinstance(value["speaker"], str) or re.fullmatch(r"SPEAKER_[0-9]{4}", value["speaker"]) is None:
        raise MatchingError("clip speaker must be an anonymous diarization label")
    _integer(value["start_ms"], 0, diarization.MAX_DURATION_MS - 1, "clip start")
    _integer(value["end_ms"], 1, diarization.MAX_DURATION_MS, "clip end")
    if not 2000 <= value["end_ms"] - value["start_ms"] <= 5000:
        raise MatchingError("clip duration must be 2..5 seconds")
    if value["start_ms"] % 40 or value["end_ms"] % 40:
        raise MatchingError("clip endpoints must use the 25 fps, 40 ms grid")
    indices = value["source_turn_indices"]
    if not isinstance(indices, list) or not 1 <= len(indices) <= diarization.MAX_TURNS:
        raise MatchingError("clip needs bounded source turn indices")
    for index in indices:
        _integer(index, 0, diarization.MAX_TURNS - 1, "source turn index")
    if indices != sorted(set(indices)):
        raise MatchingError("source turn indices must be sorted and unique")
    body = {key: value[key] for key in value if key != "clip_id"}
    if value["clip_id"] != _id("avclip_", body):
        raise MatchingError("clip ID differs from scope and interval replay")
    return json.loads(_canonical(value))


def _clean_intervals(turns):
    """Conservative integer-ms sweep: other-speaker overlap is never sampled."""
    events = defaultdict(list)
    for index, turn in enumerate(turns):
        events[turn["start_ms"]].append((1, turn["speaker"], index))
        events[turn["end_ms"]].append((-1, turn["speaker"], index))
    active, clean, previous = {}, defaultdict(list), None
    for when in sorted(events):
        if previous is not None and previous < when and len(active) == 1:
            speaker = next(iter(active))
            intervals = clean[speaker]
            if intervals and intervals[-1][1] == previous:
                intervals[-1][1] = when
            else:
                intervals.append([previous, when])
        for delta, speaker, index in events[when]:
            if delta == 1:
                active.setdefault(speaker, set()).add(index)
            else:
                active[speaker].remove(index)
                if not active[speaker]:
                    del active[speaker]
        previous = when
    return clean


def _speech_ms(turns):
    merged = []
    for turn in turns:
        start, end = turn["start_ms"], turn["end_ms"]
        if merged and start <= merged[-1][1]:
            merged[-1][1] = max(end, merged[-1][1])
        else:
            merged.append([start, end])
    return sum(end - start for start, end in merged)


def _distributed(intervals, count, length):
    """Distribute anchors over eligible speech, not across silence gaps."""
    if not intervals:
        return []
    total = sum(end - start for start, end in intervals)
    chosen = []
    for index in range(count):
        target = total // 2 if count == 1 else total * index // (count - 1)
        offset = 0
        for left, right in intervals:
            span = right - left
            if target <= offset + span:
                clip_length = min(length, span)
                center = left + target - offset
                start = max(left, min((center - clip_length // 2) // 40 * 40, right - clip_length))
                end = start + clip_length
                # Samples cannot double count already selected speech.
                if not any(start < old_end and end > old_start for old_start, old_end in chosen):
                    chosen.append((start, end))
                break
            offset += span
    return sorted(chosen)


def plan_clips(diarization_result, policy=None):
    """Plan a finite earliest/middle/latest sample from clean ordinary turns."""
    policy = validate_policy(policy)
    try:
        source = diarization.validate_result(diarization_result)
    except diarization.DiarizationError as error:
        raise MatchingError("invalid diarization evidence: " + str(error)) from error
    turns = source["representations"]["ordinary"]
    clean = _clean_intervals(turns)
    by_speaker = defaultdict(list)
    for index, turn in enumerate(turns):
        by_speaker[turn["speaker"]].append((index, turn))
    candidates, coverage = {}, []
    guard = policy["boundary_guard_ms"]
    for speaker in source["speakers"]:
        intervals = clean[speaker]
        guarded = [[(start + guard + 39) // 40 * 40, (end - guard) // 40 * 40]
                   for start, end in intervals]
        eligible = [[start, end] for start, end in guarded if end - start >= policy["min_clip_ms"]]
        candidates[speaker] = _distributed(eligible, policy["clips_per_speaker"], policy["max_clip_ms"])
        coverage.append({"speaker": speaker, "ordinary_speech_ms": _speech_ms([turn for _, turn in by_speaker[speaker]]),
                         "clean_speech_ms": sum(end - start for start, end in intervals),
                         "eligible_speech_ms": sum(end - start for start, end in eligible),
                         "sampled_ms": 0, "planned_clips": 0, "skipped_reason": None})
    clips = []
    # Round-robin allocation prevents the finite global cap favoring early speakers.
    for sample_index in range(policy["clips_per_speaker"]):
        for summary in coverage:
            speaker = summary["speaker"]
            if sample_index >= len(candidates[speaker]) or len(clips) >= policy["max_clips"]:
                continue
            start, end = candidates[speaker][sample_index]
            body = {"run_id": source["run_id"], "media_sha256": source["recording"]["media_sha256"],
                    "speaker": speaker, "start_ms": start, "end_ms": end,
                    "source_turn_indices": [index for index, turn in by_speaker[speaker]
                                            if turn["start_ms"] < end and turn["end_ms"] > start]}
            clips.append({"clip_id": _id("avclip_", body), **body})
            summary["sampled_ms"] += end - start
            summary["planned_clips"] += 1
    for summary in coverage:
        if not summary["planned_clips"]:
            summary["skipped_reason"] = ("global_clip_limit" if candidates[summary["speaker"]]
                                         else "no_clean_turn_long_enough")
        summary["unsampled_speech_ms"] = summary["ordinary_speech_ms"] - summary["sampled_ms"]
    result = {"kind": "himr_speaker_face_clip_plan", "schema_version": 1,
              "recording": source["recording"], "run_id": source["run_id"], "policy": policy,
              "clips": sorted(clips, key=lambda clip: (clip["start_ms"], clip["speaker"], clip["clip_id"])),
              "coverage": {"speakers": coverage, "sampled_ms": sum(row["sampled_ms"] for row in coverage),
                           "total_speech_ms": source["summary"]["speech_ms"],
                           "complete_recording_screened": False},
              "semantics": dict(SEMANTICS)}
    _canonical(result)
    return result


def validate_observations(value, clip):
    """Validate evidence, without asserting its model or source authenticity."""
    clip = validate_clip(clip)
    _canonical(value)
    _exact(value, {"frames", "av_sync"}, "audio/video observations")
    sync = value["av_sync"]
    _exact(sync, {"state", "offset_ms"}, "audio/video timestamp alignment")
    if sync["state"] not in ("verified", "unknown", "failed"):
        raise MatchingError("invalid audio/video timestamp alignment state")
    if sync["offset_ms"] is None:
        if sync["state"] == "verified":
            raise MatchingError("verified timestamp alignment requires an explicit offset")
    else:
        _integer(sync["offset_ms"], -60_000, 60_000, "audio/video offset")
    frames = value["frames"]
    if not isinstance(frames, list) or len(frames) > 125:
        raise MatchingError("observation frames exceed a bounded 5 second clip")
    previous, track_keys = None, set()
    for frame in frames:
        _exact(frame, {"time_ms", "shot_id", "faces"}, "observation frame")
        when = _integer(frame["time_ms"], clip["start_ms"], clip["end_ms"] - 1, "frame time")
        if when % 40 or (previous is not None and when <= previous):
            raise MatchingError("frame times must be strictly increasing on the 40 ms grid")
        previous = when
        _token(frame["shot_id"], "shot_", 32, "shot-local identifier")
        faces = frame["faces"]
        if not isinstance(faces, list) or len(faces) > 32:
            raise MatchingError("frame exceeds 32 visible track observations")
        seen = set()
        for face in faces:
            _exact(face, {"track_id", "raw_logit", "face_width_px", "face_height_px", "visible", "occluded"},
                   "face observation")
            _token(face["track_id"], "face_track_", 32, "anonymous face track")
            if face["track_id"] in seen:
                raise MatchingError("duplicate face track in the same frame")
            seen.add(face["track_id"])
            track_keys.add((frame["shot_id"], face["track_id"]))
            if len(track_keys) > 128:
                raise MatchingError("clip exceeds 128 shot-local face tracks")
            if face["raw_logit"] is not None:
                _number(face["raw_logit"], -1_000_000, 1_000_000, "raw active-speaker logit")
            _integer(face["face_width_px"], 1, 32768, "face width")
            _integer(face["face_height_px"], 1, 32768, "face height")
            if type(face["visible"]) is not bool or type(face["occluded"]) is not bool:
                raise MatchingError("face visibility/occlusion must be explicit booleans")
    return json.loads(_canonical(value))


def associate(clip, observations, policy=None, calibration=None):
    """Independently score tracks, returning a private candidate or abstention.

    The caller must verify source and model provenance. The supplied ``verified``
    flag attests decoded timestamp alignment only, never actual A/V content sync.
    No best-face argmax is used: multiple active faces always remain unknown.
    """
    if calibration is not None:
        raise MatchingError("calibrated or verified matches are not implemented in this private pilot")
    clip, policy = validate_clip(clip), validate_policy(policy)
    if not policy["min_clip_ms"] <= clip["end_ms"] - clip["start_ms"] <= policy["max_clip_ms"]:
        raise MatchingError("clip duration is outside the supplied policy")
    observations = validate_observations(observations, clip)
    frames, sync = observations["frames"], observations["av_sync"]
    expected_frames = (clip["end_ms"] - clip["start_ms"]) // 40
    reasons = []
    if sync["state"] != "verified":
        reasons.append("timestamp_alignment_not_verified")
    elif abs(sync["offset_ms"]) > 40:
        reasons.append("timestamp_offset_exceeds_one_frame")
    if not frames:
        reasons.append("no_frames")
    elif (frames[0]["time_ms"] != clip["start_ms"] or frames[-1]["time_ms"] != clip["end_ms"] - 40
          or any(right["time_ms"] - left["time_ms"] > policy["max_frame_gap_ms"]
                 for left, right in zip(frames, frames[1:]))
          or len(frames) != expected_frames):
        reasons.append("missing_frames_or_timestamp_gap")
    shots = sorted({frame["shot_id"] for frame in frames})
    if len(shots) > 1:
        reasons.append("shot_cut_inside_clip")
    tracks = defaultdict(list)
    bad_quality, simultaneous_active = False, False
    for frame in frames:
        active = 0
        for face in frame["faces"]:
            quality = (face["visible"] and not face["occluded"] and face["raw_logit"] is not None
                       and min(face["face_width_px"], face["face_height_px"]) >= policy["min_face_px"])
            tracks[(frame["shot_id"], face["track_id"])].append((frame["time_ms"], face, quality))
            bad_quality |= not quality
            active += bool(quality and face["raw_logit"] > policy["min_mean_raw_logit"])
        simultaneous_active |= active > 1
    if not tracks:
        reasons.append("no_visible_faces")
    if frames and any(not frame["faces"] for frame in frames):
        reasons.append("frames_without_visible_faces")
    if bad_quality:
        reasons.append("face_quality_or_missing_score")
    if simultaneous_active:
        reasons.append("multiple_active_faces")
    metrics, candidates = [], []
    for (shot_id, track_id), rows in sorted(tracks.items()):
        scores = [float(face["raw_logit"]) for _, face, quality in rows if quality]
        coverage = len(scores) / expected_frames
        mean = math.fsum(scores) / len(scores) if scores else None
        fraction = sum(score > policy["min_mean_raw_logit"] for score in scores) / len(scores) if scores else 0.0
        eligible = (coverage >= policy["min_track_coverage"] and mean is not None
                    and mean > policy["min_mean_raw_logit"] and fraction >= policy["min_active_fraction"])
        metric = {"shot_id": shot_id, "track_id": track_id, "observed_frames": len(rows),
                  "usable_frames": len(scores), "expected_frames": expected_frames,
                  "track_coverage": coverage, "mean_raw_logit": mean, "active_fraction": fraction,
                  "passes_independent_heuristic": eligible}
        metrics.append(metric)
        if eligible:
            candidates.append(metric)
    if len(candidates) > 1:
        reasons.append("ambiguous_active_tracks")
    elif not candidates and tracks:
        reasons.append("no_consistent_active_track")
    winner = candidates[0] if len(candidates) == 1 else None
    if winner is not None:
        competitors = [metric["mean_raw_logit"] for metric in metrics
                       if metric is not winner and metric["mean_raw_logit"] is not None]
        if competitors and winner["mean_raw_logit"] - max(competitors) < policy["min_winner_margin"]:
            reasons.append("insufficient_raw_score_margin")
    accepted = winner is not None and not reasons
    result = {"kind": "himr_speaker_face_association", "schema_version": 1,
              "clip": clip, "policy": policy, "observations": observations,
              "status": "candidate_match" if accepted else "unknown",
              "candidate_track_id": winner["track_id"] if accepted else None,
              "candidate_shot_id": winner["shot_id"] if accepted else None,
              "reasons": reasons, "track_metrics": metrics,
              "scope": {"run_id": clip["run_id"], "media_sha256": clip["media_sha256"],
                        "speaker": clip["speaker"], "start_ms": clip["start_ms"], "end_ms": clip["end_ms"]},
              "semantics": dict(SEMANTICS)}
    _canonical(result)
    return result


def validate_result(value):
    """Replay a retained association; changing any derived decision fails closed."""
    _canonical(value)
    if not isinstance(value, dict):
        raise MatchingError("invalid association document")
    try:
        expected = associate(value["clip"], value["observations"], value["policy"])
    except KeyError as error:
        raise MatchingError("association lacks replay evidence") from error
    if _canonical(expected) != _canonical(value):
        raise MatchingError("association differs from deterministic evidence replay")
    return expected
