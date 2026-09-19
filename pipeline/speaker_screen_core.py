"""Pure, bounded, anonymous speaker-diversity screening of sampled audio.

No model, media, filesystem, network, transcription, or identity operations live
here. Similarity thresholds are uncalibrated screening policy, not probabilities.
"""

from __future__ import annotations

import math
from operator import mul
from typing import Any


MAX_DURATION_MS = 7 * 24 * 60 * 60 * 1000
MAX_WINDOWS = 512
MAX_EMBEDDING_DIMENSIONS = 2048
DEFAULT_POLICY = {
    "probe_ms": 10_000,
    "stride_ms": 60_000,
    "max_windows": 512,
    "min_speech_ms": 2_000,
    "min_support": 2,
    "match_cosine_min": 0.85,
    "distinct_cosine_max": 0.65,
}


class ScreenError(RuntimeError):
    pass


def _integer(value: Any, label: str, minimum: int, maximum: int) -> int:
    if type(value) is not int or not minimum <= value <= maximum:
        raise ScreenError(f"invalid {label}")
    return value


def _finite(value: Any, label: str) -> float:
    if type(value) not in (int, float):
        raise ScreenError(f"{label} must be a finite number")
    try:
        result = float(value)
    except (OverflowError, ValueError):
        raise ScreenError(f"{label} must be a finite number") from None
    if not math.isfinite(result):
        raise ScreenError(f"{label} must be a finite number")
    return result


def validate_policy(policy: dict | None = None) -> dict:
    value = dict(DEFAULT_POLICY) if policy is None else policy
    if not isinstance(value, dict) or set(value) != set(DEFAULT_POLICY):
        raise ScreenError("screen policy fields differ")
    result = dict(value)
    _integer(result["probe_ms"], "probe duration", 1, 10_000)
    _integer(result["stride_ms"], "probe stride", result["probe_ms"], MAX_DURATION_MS)
    _integer(result["max_windows"], "maximum probes", 1, MAX_WINDOWS)
    _integer(result["min_speech_ms"], "minimum speech", 2_000, 5_000)
    _integer(result["min_support"], "minimum independent excerpts", 2, 8)
    for key in ("match_cosine_min", "distinct_cosine_max"):
        result[key] = _finite(result[key], "cosine threshold")
    if not -1 <= result["distinct_cosine_max"] < result["match_cosine_min"] <= 1:
        raise ScreenError("distinct threshold must be below matching threshold")
    return result


def plan_windows(duration_ms: int, policy: dict | None = None) -> list[dict]:
    """Spread nonoverlapping probes over the recording, never a capped prefix.

    Stride defines target sampling density. Start positions are redistributed
    uniformly between the first and last full probe. A lone probe is centered.
    Short recordings can have a probe shorter than the minimum usable speech.
    """
    duration = _integer(duration_ms, "recording duration", 1, MAX_DURATION_MS)
    selected = validate_policy(policy)
    return _planned_windows(duration, selected)


def _planned_windows(duration: int, selected: dict) -> list[dict]:
    """Plan already-validated scalar inputs without validating policy twice."""
    width = min(duration, selected["probe_ms"])
    target = (duration + selected["stride_ms"] - 1) // selected["stride_ms"]
    count = min(max(1, target), selected["max_windows"], max(1, duration // width))
    extent = duration - width
    starts = [extent // 2] if count == 1 else [index * extent // (count - 1) for index in range(count)]
    return [{"index": index, "start_ms": start, "end_ms": start + width} for index, start in enumerate(starts)]


def _embedding(value: Any) -> tuple[float, ...]:
    if not isinstance(value, list) or not 2 <= len(value) <= MAX_EMBEDDING_DIMENSIONS:
        raise ScreenError("embedding dimension must be bounded and nonempty")
    finite = [_finite(number, "embedding component") for number in value]
    scale = max(abs(number) for number in finite)
    if scale == 0:
        raise ScreenError("zero embedding has no cosine direction")
    scaled = [number / scale for number in finite]
    norm = math.sqrt(math.fsum(number * number for number in scaled))
    return tuple(number / norm for number in scaled)


def _cosine(left: tuple[float, ...], right: tuple[float, ...]) -> float:
    # Keep separately rounded float products and fsum: sumprod/BLAS can change
    # boundary decisions through fused or extended-precision multiplication.
    return max(-1.0, min(1.0, math.fsum(map(mul, left, right))))


class SummaryCache:
    """Bounded, in-memory cosine memo for repeated summaries of one recording.

    No input validation or evidence is cached. Exact normalized-vector prefixes
    may reuse pair scores; plan/policy changes, removal, insertion, or changed
    vectors reset the memo unless every cached vector position remains equal.
    Keep one instance per recording/run and discard it afterward. Never persist
    this private embedding cache or share it between concurrent callers.
    """

    __slots__ = ("_binding", "_vectors", "_rows")

    def __init__(self):
        self._binding = None
        self._vectors = ()
        self._rows: list[list[float | None]] = []

    def _prepare(self, duration: int, selected: dict, usable: list[dict]):
        binding = (duration, tuple(sorted(selected.items())))
        vectors = tuple(row["embedding"] for row in usable)
        if self._binding != binding or self._vectors != vectors[:len(self._vectors)]:
            self._rows = []
        # A lower-triangular table avoids tuple-key allocation/hash lookups and
        # cannot exceed 512*511/2 scores, even across many checkpoint summaries.
        while len(self._rows) < len(vectors):
            self._rows.append([None] * len(self._rows))
        self._binding, self._vectors = binding, vectors

    def _similarity(self, left: int, right: int) -> float:
        if left < right:
            left, right = right, left
        row = self._rows[left]
        value = row[right]
        if value is None:
            value = _cosine(self._vectors[left], self._vectors[right])
            row[right] = value
        return value


def _classify(usable: list[dict], selected: dict, similarity):
    """Complete-link decisions with shortcuts only after a result is decided."""
    groups: list[list[int]] = []
    ambiguous = []
    match_min = selected["match_cosine_min"]
    for index in range(len(usable)):
        compatible = []
        for group in groups:
            if all(similarity(index, member) >= match_min for member in group):
                compatible.append(group)
                if len(compatible) == 2:
                    # Two matches already force ambiguity; later matches cannot
                    # alter group membership, evidence order, or classification.
                    break
        if len(compatible) == 1:
            compatible[0].append(index)
        elif compatible:
            ambiguous.append(index)
        else:
            groups.append([index])
    supported = [index for index, group in enumerate(groups) if len(group) >= selected["min_support"]]
    distinct_pairs = []
    distinct_max = selected["distinct_cosine_max"]
    for offset, left in enumerate(supported):
        for right in supported[offset + 1:]:
            maximum = -1.0
            rejected = False
            for a in groups[left]:
                for b in groups[right]:
                    value = similarity(a, b)
                    if value > distinct_max:
                        rejected = True
                        break
                    maximum = max(maximum, value)
                if rejected:
                    break
            if not rejected:
                distinct_pairs.append({"group_ids": [f"screen_group_{left:04d}", f"screen_group_{right:04d}"],
                                       "maximum_cross_cosine": round(maximum, 8)})
    return groups, supported, distinct_pairs, ambiguous


def _evidence(row: dict) -> dict:
    return {key: row[key] for key in ("index", "start_ms", "end_ms", "speech_ms")}


def summarize(duration_ms: int, windows: list[dict], observations: list[dict], policy: dict | None = None,
              *, cache: SummaryCache | None = None) -> dict:
    """Summarize one deterministic plan; incomplete observations stay explicit.

    Observation start/end identify the selected contiguous excerpt when embedded,
    or a bounded inspected region otherwise. ``speech_ms`` equals the selected
    excerpt duration when embedded; a null embedding may report VAD-positive time
    within the probe. These mixed reports are not total full-probe VAD coverage.
    Groups require complete-link cosine agreement and separate probe support.
    A lone outlier cannot establish another voice or a whole-recording solo claim.
    An optional private SummaryCache reuses only exact vector-pair arithmetic;
    every call still validates the complete supplied plan and observations.
    """
    selected = validate_policy(policy)
    duration = _integer(duration_ms, "recording duration", 1, MAX_DURATION_MS)
    expected = _planned_windows(duration, selected)
    if not isinstance(windows, list) or len(windows) != len(expected):
        raise ScreenError("windows differ from the deterministic plan")
    for row, planned in zip(windows, expected):
        if not isinstance(row, dict) or set(row) != {"index", "start_ms", "end_ms"}:
            raise ScreenError("invalid planned window")
        for key in row:
            _integer(row[key], f"window {key}", 0, MAX_DURATION_MS)
        if row != planned:
            raise ScreenError("altered, reordered, or overlapping windows")
    if not isinstance(observations, list) or len(observations) > len(windows):
        raise ScreenError("observations exceed the bounded plan")
    observed = {}
    dimension = None
    for row in observations:
        if not isinstance(row, dict) or set(row) != {"index", "start_ms", "end_ms", "speech_ms", "embedding"}:
            raise ScreenError("invalid observation fields")
        index = _integer(row["index"], "observation index", 0, len(windows) - 1)
        if index in observed:
            raise ScreenError("duplicate observation index")
        window = windows[index]
        start = _integer(row["start_ms"], "excerpt start", window["start_ms"], window["end_ms"] - 1)
        end = _integer(row["end_ms"], "excerpt end", start + 1, window["end_ms"])
        speech = _integer(row["speech_ms"], "probe speech duration", 0, window["end_ms"] - window["start_ms"])
        vector = None if row["embedding"] is None else _embedding(row["embedding"])
        if vector is not None:
            if speech != end - start or not selected["min_speech_ms"] <= end - start <= 5_000:
                raise ScreenError("embedded excerpt lacks bounded contiguous speech")
            if dimension is not None and len(vector) != dimension:
                raise ScreenError("embedding dimensions differ")
            dimension = len(vector)
        observed[index] = {**row, "embedding": vector}
    ordered = [observed[index] for index in sorted(observed)]
    usable = [row for row in ordered if row["embedding"] is not None]
    if cache is None:
        cache = SummaryCache()
    elif not isinstance(cache, SummaryCache):
        raise ScreenError("invalid summary cache")
    cache._prepare(duration, selected, usable)
    groups, supported, distinct_pairs, ambiguous = _classify(usable, selected, cache._similarity)
    supported_set = set(supported)
    flags = set()
    if len(ordered) != len(windows):
        flags.add("planned_probes_uninspected")
    if not usable:
        flags.add("no_usable_speech_embeddings")
    if any(row["embedding"] is None and row["speech_ms"] >= selected["min_speech_ms"] for row in ordered):
        flags.add("speech_without_usable_embedding")
    if any(row["embedding"] is None and row["speech_ms"] < selected["min_speech_ms"] for row in ordered):
        flags.add("probes_with_insufficient_speech")
    if any(len(group) < selected["min_support"] for group in groups):
        flags.add("unsupported_or_isolated_voice_group")
    if ambiguous:
        flags.add("ambiguous_group_membership")
    if len(supported) > 1 and not distinct_pairs:
        flags.add("supported_groups_not_clearly_distinct")
    if distinct_pairs:
        status = "multiple_speaker_candidate"
        flags.add("supported_distinct_voice_groups")
    elif (len(groups) == len(supported) == 1 and not ambiguous
          and len(ordered) == len(windows) and "speech_without_usable_embedding" not in flags):
        status = "no_second_voice_detected_in_sampled_audio"
        flags.add("one_supported_sampled_voice_group")
    else:
        status = "uncertain"
    inspected_ms = sum(windows[row["index"]]["end_ms"] - windows[row["index"]]["start_ms"] for row in ordered)
    embedded_ms = sum(row["end_ms"] - row["start_ms"] for row in usable)
    return {
        "kind": "himr_speaker_diversity_screen", "schema_version": 1,
        "status": status, "duration_ms": duration_ms, "policy": selected,
        "reason_flags": sorted(flags),
        "groups": [{"group_id": f"screen_group_{index:04d}", "supported": index in supported_set,
                    "excerpt_count": len(group), "evidence": [_evidence(usable[item]) for item in group]}
                   for index, group in enumerate(groups)],
        "distinct_group_evidence": distinct_pairs,
        "ambiguous_evidence": [_evidence(usable[index]) for index in ambiguous],
        "coverage": {
            "planned_windows": len(windows), "inspected_windows": len(ordered),
            "uninspected_windows": len(windows) - len(ordered), "embedded_excerpts": len(usable),
            "planned_window_ms": sum(row["end_ms"] - row["start_ms"] for row in windows),
            "inspected_window_ms": inspected_ms, "reported_speech_ms": sum(row["speech_ms"] for row in ordered),
            "embedded_speech_ms": embedded_ms,
            "inspected_fraction": inspected_ms / duration_ms, "embedded_speech_fraction": embedded_ms / duration_ms,
            "whole_recording_inspected": inspected_ms == duration_ms,
            "sampling_may_miss_a_rare_second_voice": True,
        },
        "semantics": {
            "method": "uncalibrated_cosine_complete_link_screen",
            "score_state": "uncalibrated_similarity_not_probability",
            "labels_scope": "this_screen_only", "groups_are_person_counts": False,
            "exact_speaker_count_claimed": False, "whole_recording_solo_claimed": False,
            "identity_inferred": False, "playback_and_channel_changes_require_review": True,
            "support_unit": "separate_nonoverlapping_probe_windows",
            "inspected_window_semantics": "completed_probe_not_complete_speaker_analysis",
            "embedding_coverage": "selected_contiguous_excerpt_duration_not_all_vad_positive_time",
            "reported_speech": "selected_excerpt_when_embedded_otherwise_reported_probe_vad_positive_time",
        },
        "review": {"visibility": "private", "machine_generated": True, "human_reviewed": False,
                   "publication_authority": False, "catalogue_mutation_authority": False},
    }
