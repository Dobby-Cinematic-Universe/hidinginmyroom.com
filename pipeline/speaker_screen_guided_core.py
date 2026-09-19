"""Versioned, metadata-guided probe placement; acoustic evidence stays local.

Every uniformly dispersed v1 baseline probe is retained. Hints only add bounded
nonoverlapping probes: neither their presence nor their absence changes speaker
thresholds, provides an embedding, or establishes a speaker identity.
"""
from __future__ import annotations

import json
from typing import Any

from pipeline import speaker_screen_core as core

ScreenError = core.ScreenError
SummaryCache = core.SummaryCache
MAX_TARGETS = 32
MAX_TARGET_WINDOWS = 64
MAX_WINDOWS = core.MAX_WINDOWS + MAX_TARGET_WINDOWS
RECIPE = "baseline-preserving-context-anchor-round-robin-v1"


def _targets(value: Any, duration: int) -> list[dict]:
    if not isinstance(value, list) or len(value) > MAX_TARGETS:
        raise ScreenError("guided targets must be a list of at most 32 hints")
    result, seen = [], set()
    for row in value:
        if not isinstance(row, dict) or set(row) != {"hint_id", "start_ms", "end_ms"}:
            raise ScreenError("guided target fields differ")
        hint = row["hint_id"]
        if (not isinstance(hint, str) or not 1 <= len(hint) <= 128
                or not hint.isprintable() or hint != hint.strip()):
            raise ScreenError("invalid bounded guided hint_id")
        if hint in seen:
            raise ScreenError("duplicate guided hint_id")
        seen.add(hint)
        start = core._integer(row["start_ms"], "target start", 0, duration - 1)
        end = core._integer(row["end_ms"], "target end", start + 1, duration)
        result.append({"hint_id": hint, "start_ms": start, "end_ms": end})
    return result


def _candidates(target: dict, width: int, duration: int) -> list[dict]:
    anchors = (("start", target["start_ms"]),
               ("middle", (target["start_ms"] + target["end_ms"] - width) // 2),
               ("end", target["end_ms"] - width))
    result, seen = [], set()
    # First distribute start/middle/end requests, then their confirmation
    # neighbors. A narrow hint can still get independent adjacent probes.
    for offset, suffix in ((0, ""), (-width, "_before"), (width, "_after")):
        for label, anchor in anchors:
            start = max(0, min(duration - width, anchor + offset))
            if start not in seen:
                seen.add(start)
                result.append({"anchor": label + suffix, "desired_start_ms": start})
    return result


def _placement(target: dict, desired: int, width: int, duration: int,
               occupied: list[tuple[int, int]]) -> int | None:
    """Nearest full probe in a free gap inside hint +/- one probe of context."""
    context_start = max(0, target["start_ms"] - width)
    context_end = min(duration, target["end_ms"] + width)
    choices, previous = [], 0
    for start, end in [*sorted(occupied), (duration, duration)]:
        low, high = max(previous, context_start), min(start, context_end) - width
        if low <= high:
            chosen = max(low, min(high, desired))
            choices.append((abs(chosen - desired), chosen))
        previous = end
    return min(choices)[1] if choices else None


def _overlap(window: dict, target: dict) -> int:
    return max(0, min(window["end_ms"], target["end_ms"])
               - max(window["start_ms"], target["start_ms"]))


def build_sampling(duration_ms: int, policy: dict | None, targets: list[dict], *,
                   max_target_windows: int = 16) -> dict:
    """Seal deterministic inputs and a baseline-preserving, nonoverlapping plan.

    Input order is the explicit hint priority. Round-robin allocation prevents
    the first hint from consuming every available probe before later hints get
    an opportunity. Timeline alignment is the caller's responsibility; this
    pure module validates exact integer bounds, not a transcript's provenance.
    """
    duration = core._integer(duration_ms, "recording duration", 1, core.MAX_DURATION_MS)
    selected = core.validate_policy(policy)
    targets = _targets(targets, duration)
    cap = core._integer(max_target_windows, "maximum targeted probes", 0, MAX_TARGET_WINDOWS)
    baseline = core.plan_windows(duration, selected)
    width = min(duration, selected["probe_ms"])
    occupied = [(row["start_ms"], row["end_ms"]) for row in baseline]
    candidates = [_candidates(target, width, duration) for target in targets]
    added, skipped = [], [[] for _ in targets]
    for ordinal in range(max((len(rows) for rows in candidates), default=0)):
        for target_index, (target, rows) in enumerate(zip(targets, candidates)):
            if ordinal >= len(rows):
                continue
            candidate = rows[ordinal]
            start = _placement(target, candidate["desired_start_ms"], width, duration, occupied)
            reason = ("no_available_nonoverlapping_probe" if start is None else
                      "target_window_budget" if len(added) >= cap else None)
            if reason is not None:
                skipped[target_index].append({**candidate, "reason": reason})
                continue
            assert start is not None
            added.append({"start_ms": start, "end_ms": start + width,
                          "hint_id": target["hint_id"], **candidate})
            occupied.append((start, start + width))
    baseline_coordinates = {(row["start_ms"], row["end_ms"]) for row in baseline}
    windows = [{"index": index, "start_ms": start, "end_ms": end}
               for index, (start, end) in enumerate(sorted(occupied))]
    baseline_indices = [row["index"] for row in windows
                        if (row["start_ms"], row["end_ms"]) in baseline_coordinates]
    baseline_set = set(baseline_indices)
    target_indices = [row["index"] for row in windows if row["index"] not in baseline_set]
    indices = {(row["start_ms"], row["end_ms"]): row["index"] for row in windows}
    reasons = [{"index": indices[(row["start_ms"], row["end_ms"])],
                "hint_id": row["hint_id"], "anchor": row["anchor"],
                "desired_start_ms": row["desired_start_ms"]}
               for row in added]
    reasons.sort(key=lambda row: row["index"])
    reports = []
    for index, target in enumerate(targets):
        base_hits = [row["index"] for row in windows
                     if row["index"] in baseline_set and _overlap(row, target)]
        target_hits = [row["index"] for row in windows
                       if row["index"] not in baseline_set and _overlap(row, target)]
        assigned = [row["index"] for row in reasons if row["hint_id"] == target["hint_id"]]
        base_ms = sum(_overlap(windows[item], target) for item in base_hits)
        target_ms = sum(_overlap(windows[item], target) for item in target_hits)
        interval_ms = target["end_ms"] - target["start_ms"]
        flags = sorted({row["reason"] for row in skipped[index]}
                       | ({"hint_already_covered_by_baseline"} if base_ms == interval_ms else set()))
        reports.append({"hint_id": target["hint_id"], "interval_ms": interval_ms,
                        "baseline_indices": base_hits, "target_indices": target_hits,
                        "assigned_target_indices": assigned,
                        "baseline_overlap_ms": base_ms, "targeted_overlap_ms": target_ms,
                        "planned_overlap_ms": base_ms + target_ms,
                        "unprobed_interval_ms": interval_ms - base_ms - target_ms,
                        "reason_flags": flags, "omitted_requests": skipped[index]})
    return {"kind": "himr_guided_speaker_screen_sampling", "schema_version": 1,
            "recipe": RECIPE, "duration_ms": duration, "policy": selected,
            "targets": targets, "max_target_windows": cap, "windows": windows,
            "baseline_indices": baseline_indices, "target_indices": target_indices,
            "target_reasons": reasons, "target_coverage": reports,
            "semantics": {"baseline_preserved": True, "metadata_is_speaker_evidence": False,
                          "context_ms": width, "placement": "nearest_free_full_probe",
                          "hint_order": "caller_priority_round_robin",
                          "targeted_window_budget_used": len(added)}}


def _json(value: Any) -> str:
    try:
        return json.dumps(value, sort_keys=True, separators=(",", ":"), allow_nan=False)
    except (TypeError, ValueError, OverflowError) as error:
        raise ScreenError("guided sampling must contain finite JSON values") from error


def validate_sampling(duration_ms: int, policy: dict | None, sampling: dict) -> dict:
    if not isinstance(sampling, dict) or not {"targets", "max_target_windows", "windows"} <= set(sampling):
        raise ScreenError("invalid guided sampling document")
    if not isinstance(sampling["windows"], list) or not 1 <= len(sampling["windows"]) <= MAX_WINDOWS:
        raise ScreenError("guided sampling exceeds the 576-probe bound")
    expected = build_sampling(duration_ms, policy, sampling["targets"],
                              max_target_windows=sampling["max_target_windows"])
    if _json(sampling) != _json(expected):
        raise ScreenError("guided sampling differs from its deterministic recipe")
    return expected


def summarize(duration_ms: int, windows: list[dict], observations: list[dict], policy: dict | None = None,
              *, sampling: dict, cache: SummaryCache | None = None) -> dict:
    """Use v1 acoustic validation, arithmetic and classifications on guided probes.

    Only the plan validator and coverage accounting differ from v1. Private
    vector-pair math is delegated to the unchanged v1 helpers; no title, date,
    hint weight, or neighboring recording can influence an acoustic decision.
    """
    selected = core.validate_policy(policy)
    duration = core._integer(duration_ms, "recording duration", 1, core.MAX_DURATION_MS)
    sampling = validate_sampling(duration, selected, sampling)
    if not isinstance(windows, list) or len(windows) > MAX_WINDOWS or _json(windows) != _json(sampling["windows"]):
        raise ScreenError("windows differ from the sealed guided sampling plan")
    if not isinstance(observations, list) or len(observations) > len(windows):
        raise ScreenError("observations exceed the bounded plan")
    observed = {}
    dimension = None
    for row in observations:
        if not isinstance(row, dict) or set(row) != {"index", "start_ms", "end_ms", "speech_ms", "embedding"}:
            raise ScreenError("invalid observation fields")
        index = core._integer(row["index"], "observation index", 0, len(windows) - 1)
        if index in observed:
            raise ScreenError("duplicate observation index")
        window = windows[index]
        start = core._integer(row["start_ms"], "excerpt start", window["start_ms"], window["end_ms"] - 1)
        end = core._integer(row["end_ms"], "excerpt end", start + 1, window["end_ms"])
        speech = core._integer(row["speech_ms"], "probe speech duration", 0, window["end_ms"] - window["start_ms"])
        vector = None if row["embedding"] is None else core._embedding(row["embedding"])
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
    groups, supported, distinct_pairs, ambiguous = core._classify(usable, selected, cache._similarity)
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
    coverage = {
        "planned_windows": len(windows), "inspected_windows": len(ordered),
        "uninspected_windows": len(windows) - len(ordered), "embedded_excerpts": len(usable),
        "planned_window_ms": sum(row["end_ms"] - row["start_ms"] for row in windows),
        "inspected_window_ms": inspected_ms, "reported_speech_ms": sum(row["speech_ms"] for row in ordered),
        "embedded_speech_ms": embedded_ms,
        "inspected_fraction": inspected_ms / duration_ms, "embedded_speech_fraction": embedded_ms / duration_ms,
        "whole_recording_inspected": inspected_ms == duration_ms,
        "sampling_may_miss_a_rare_second_voice": True,
    }
    for label, key in (("baseline", "baseline_indices"), ("targeted", "target_indices")):
        indices = sampling[key]
        inspected = [index for index in indices if index in observed]
        coverage.update({f"{label}_planned_windows": len(indices),
                         f"{label}_inspected_windows": len(inspected),
                         f"{label}_uninspected_windows": len(indices) - len(inspected),
                         f"{label}_inspected_window_ms": sum(windows[i]["end_ms"] - windows[i]["start_ms"] for i in inspected)})
    return {
        "kind": "himr_guided_speaker_diversity_screen", "schema_version": 1,
        "status": status, "duration_ms": duration_ms, "policy": selected,
        "reason_flags": sorted(flags), "metadata_is_speaker_evidence": False,
        "groups": [{"group_id": f"screen_group_{index:04d}", "supported": index in supported_set,
                    "excerpt_count": len(group), "evidence": [core._evidence(usable[item]) for item in group]}
                   for index, group in enumerate(groups)],
        "distinct_group_evidence": distinct_pairs,
        "ambiguous_evidence": [core._evidence(usable[index]) for index in ambiguous],
        "coverage": coverage,
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
            "metadata_is_speaker_evidence": False, "baseline_preserved": True,
        },
        "review": {"visibility": "private", "machine_generated": True, "human_reviewed": False,
                   "publication_authority": False, "catalogue_mutation_authority": False},
    }
