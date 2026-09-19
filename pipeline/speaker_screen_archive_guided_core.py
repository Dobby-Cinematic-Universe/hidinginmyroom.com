"""Isolated archive fast-triage sampling on an explicitly admitted source interval.

The first 100 ms is not sampled: some Opus streams have a decoder-priming gap at
zero. Coordinates remain absolute source milliseconds. No padding, source edits,
or redefinition of the full measured duration is permitted. This intentionally
uses a new recipe instead of changing any existing CPU/resident/guided plan.
"""
from __future__ import annotations

import json
from typing import Any

from pipeline import speaker_screen_core as core

ScreenError = core.ScreenError
SummaryCache = core.SummaryCache
SCREENABLE_START_MS = 100
MAX_WINDOWS = 64
MAX_TARGET_WINDOWS = 64
RECIPE = "archive-fast-uniform-admitted-100ms-start-v1"


def build_sampling(duration_ms: int, policy: dict | None, targets: list[dict], *,
                   max_target_windows: int = 16) -> dict:
    duration = core._integer(duration_ms, "recording duration", SCREENABLE_START_MS + 1, core.MAX_DURATION_MS)
    selected = core.validate_policy(policy)
    if any(selected[key] != expected for key, expected in
           (("probe_ms", 10000), ("stride_ms", 300000), ("max_windows", 64))):
        raise ScreenError("archive recipe requires explicit fast-triage policy")
    if not isinstance(targets, list) or targets:
        raise ScreenError("archive fast recipe does not admit unreviewed temporal hints")
    cap = core._integer(max_target_windows, "maximum targeted probes", 0, MAX_TARGET_WINDOWS)
    baseline = core.plan_windows(duration - SCREENABLE_START_MS, selected)
    windows = [{**window, "start_ms": window["start_ms"] + SCREENABLE_START_MS,
                "end_ms": window["end_ms"] + SCREENABLE_START_MS} for window in baseline]
    return {"kind": "himr_archive_guided_speaker_screen_sampling", "schema_version": 1,
            "recipe": RECIPE, "duration_ms": duration, "policy": selected,
            "targets": [], "max_target_windows": cap, "windows": windows,
            "baseline_indices": list(range(len(windows))), "target_indices": [],
            "target_reasons": [], "target_coverage": [],
            "admitted_interval": {"start_ms": SCREENABLE_START_MS, "end_ms": duration,
                                  "leading_interval_not_screened_ms": SCREENABLE_START_MS},
            "semantics": {"baseline_preserved": False,
                          "baseline_within_admitted_interval_preserved": True,
                          "metadata_is_speaker_evidence": False,
                          "source_timestamps_rebased": False,
                          "source_duration_is_full_measured_eof": True,
                          "leading_interval_not_screened_ms": SCREENABLE_START_MS,
                          "missing_audio_padded": False,
                          "targeted_window_budget_used": 0}}


def _json(value: Any) -> str:
    try:
        return json.dumps(value, sort_keys=True, separators=(",", ":"), allow_nan=False)
    except (TypeError, ValueError, OverflowError) as error:
        raise ScreenError("guided sampling must contain finite JSON values") from error


def validate_sampling(duration_ms: int, policy: dict | None, sampling: dict) -> dict:
    if not isinstance(sampling, dict) or not {"targets", "max_target_windows", "windows"} <= set(sampling):
        raise ScreenError("invalid guided sampling document")
    if not isinstance(sampling["windows"], list) or not 1 <= len(sampling["windows"]) <= MAX_WINDOWS:
        raise ScreenError("archive fast sampling exceeds the 64-probe bound")
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
    flags = {"opening_100ms_not_screened"}
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
        "kind": "himr_archive_guided_speaker_diversity_screen", "schema_version": 1,
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
            "metadata_is_speaker_evidence": False, "baseline_preserved": False,
            "baseline_within_admitted_interval_preserved": True,
            "leading_interval_not_screened_ms": SCREENABLE_START_MS,
        },
        "review": {"visibility": "private", "machine_generated": True, "human_reviewed": False,
                   "publication_authority": False, "catalogue_mutation_authority": False},
    }
