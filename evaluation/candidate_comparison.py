"""Text-free paired ASR comparison with explicit GPU-efficiency measurements.

This module is an evaluation boundary, not an inference or promotion tool.  It
revalidates two complete system outputs against one adjudicated reference, scores
both with the same implementation, and bootstraps *paired* metric deltas by
recording family.  A small, separately sealed measurement binds the GPU accounting
needed for accuracy-per-GPU-hour decisions without making a model/runtime pin a
permanent technology choice.

The module is stdlib-only.  It performs no media, catalogue, GPU, network,
publication, or production-profile access.
"""

from __future__ import annotations

import copy
import hashlib
import math
import platform
import random
import unicodedata
from datetime import timezone
from pathlib import Path
from typing import Any, Callable, Sequence

from .scoring import (
    BOOTSTRAP_UNIT,
    IntervalStats,
    ResourceMeasurement,
    _freeze_rows,
    _interval_stats,
    _round,
    _scope_aggregate,
    score_transcript_system,
    validate_transcript_system_output,
)
from .validation import (
    _array,
    _canonical_bytes,
    _constant,
    _fail,
    _id,
    _integer,
    _object,
    _sha256,
    _stable_id,
    _string,
    _timestamp,
    _verify_manifest_digest,
    canonical_manifest_sha256,
    validate_adjudication,
    validate_interval_freeze,
)


GPU_MEASUREMENT_SCHEMA_VERSION = 1
COMPARISON_SCHEMA_VERSION = 1
IMPLEMENTATION_NAME = "himr-asr-candidate-comparator"
IMPLEMENTATION_VERSION = "0.1.0"
PAIRED_BOOTSTRAP_METHOD = "recording_family_paired_percentile_v1"
MAX_MEASUREMENT_INTEGER = 2**63 - 1
MIN_REPLICATES = 200
MAX_REPLICATES = 10_000
MIN_CONDITION_RECORDING_FAMILIES = 2
MIN_CONDITION_REFERENCE_WORDS = 100
REPORT_PUBLICATION = {"status": "withheld", "storage_policy": "private_only"}


def _implementation_sha256() -> str:
    return hashlib.sha256(Path(__file__).read_bytes()).hexdigest()


def _bounded_int(value: object, path: str, *, minimum: int = 0) -> int:
    parsed = _integer(value, path, minimum=minimum)
    if parsed > MAX_MEASUREMENT_INTEGER:
        _fail(path, f"must be <= {MAX_MEASUREMENT_INTEGER}")
    return parsed


def _measurement_identity(value: dict[str, Any]) -> str:
    unsigned = copy.deepcopy(value)
    unsigned.pop("manifest_sha256", None)
    unsigned.pop("measurement_id", None)
    digest = hashlib.sha256(_canonical_bytes(unsigned)).hexdigest()
    return f"gpu_measurement_{digest[:32]}"


def seal_gpu_evaluation_measurement(value: dict[str, Any]) -> dict[str, Any]:
    """Fill the deterministic ID and digest of an unsealed measurement."""

    value["measurement_id"] = _measurement_identity(value)
    value["manifest_sha256"] = canonical_manifest_sha256(value)
    return value


def validate_gpu_evaluation_measurement(
    value: object,
    *,
    system_output: object,
    interval_freeze: object,
    candidate_cohort: object,
) -> dict[str, Any]:
    """Validate one aggregate GPU measurement against an exact system output."""

    validated_system = validate_transcript_system_output(
        system_output, interval_freeze, candidate_cohort
    ).manifest
    item = _object(
        value,
        "$",
        {
            "schema_version",
            "manifest_kind",
            "manifest_sha256",
            "measurement_id",
            "created_at",
            "system_output_id",
            "system_output_manifest_sha256",
            "profile_identity_sha256",
            "hardware",
            "protocol",
            "metrics",
            "source_receipt_sha256s",
            "integrity",
        },
    )
    _constant(item["schema_version"], GPU_MEASUREMENT_SCHEMA_VERSION, "$.schema_version")
    _constant(item["manifest_kind"], "gpu_asr_evaluation_measurement", "$.manifest_kind")
    measurement_id = _id(item["measurement_id"], "$.measurement_id")
    created = _timestamp(item["created_at"], "$.created_at")
    if created < _timestamp(validated_system["created_at"], "$.system_output.created_at"):
        _fail("$.created_at", "cannot precede the bound system output")
    if item["system_output_id"] != validated_system["system_output_id"]:
        _fail("$.system_output_id", "does not match the exact system output")
    if item["system_output_manifest_sha256"] != validated_system["manifest_sha256"]:
        _fail("$.system_output_manifest_sha256", "does not bind the exact system output")
    _sha256(item["profile_identity_sha256"], "$.profile_identity_sha256")

    hardware = _object(
        item["hardware"],
        "$.hardware",
        {"gpu_uuid", "gpu_name", "physical_vram_bytes"},
    )
    gpu_uuid = _string(hardware["gpu_uuid"], "$.hardware.gpu_uuid")
    if not gpu_uuid.startswith("GPU-") or len(gpu_uuid) > 128:
        _fail("$.hardware.gpu_uuid", "must be a bounded NVIDIA GPU UUID")
    gpu_name = _string(hardware["gpu_name"], "$.hardware.gpu_name")
    if not gpu_name.strip() or len(gpu_name) > 256:
        _fail("$.hardware.gpu_name", "must be bounded visible text")
    _bounded_int(hardware["physical_vram_bytes"], "$.hardware.physical_vram_bytes", minimum=1)

    protocol = _object(
        item["protocol"],
        "$.protocol",
        {
            "protocol_id",
            "evaluated_audio_duration_ms",
            "includes_model_load",
            "includes_audio_preflight",
            "includes_result_publication",
            "warmup_runs",
            "measured_runs",
        },
    )
    _id(protocol["protocol_id"], "$.protocol.protocol_id")
    duration = _bounded_int(
        protocol["evaluated_audio_duration_ms"],
        "$.protocol.evaluated_audio_duration_ms",
        minimum=1,
    )
    expected_duration = sum(
        recording["evaluated_audio_duration_ms"]
        for recording in validated_system["recordings"]
    )
    if duration != expected_duration:
        _fail(
            "$.protocol.evaluated_audio_duration_ms",
            "does not equal the exact frozen audio duration in the system output",
        )
    for field in (
        "includes_model_load",
        "includes_audio_preflight",
        "includes_result_publication",
    ):
        if not isinstance(protocol[field], bool):
            _fail(f"$.protocol.{field}", "must be boolean")
    _bounded_int(protocol["warmup_runs"], "$.protocol.warmup_runs")
    _bounded_int(protocol["measured_runs"], "$.protocol.measured_runs", minimum=1)

    metrics = _object(
        item["metrics"],
        "$.metrics",
        {
            "runner_wall_time_ms",
            "gpu_sample_span_ms",
            "gpu_active_time_ms",
            "peak_process_vram_bytes",
            "estimated_energy_millijoules",
        },
    )
    runner_ms = _bounded_int(metrics["runner_wall_time_ms"], "$.metrics.runner_wall_time_ms", minimum=1)
    sample_ms = _bounded_int(metrics["gpu_sample_span_ms"], "$.metrics.gpu_sample_span_ms", minimum=1)
    active_ms = _bounded_int(metrics["gpu_active_time_ms"], "$.metrics.gpu_active_time_ms")
    peak_vram = _bounded_int(metrics["peak_process_vram_bytes"], "$.metrics.peak_process_vram_bytes", minimum=1)
    _bounded_int(metrics["estimated_energy_millijoules"], "$.metrics.estimated_energy_millijoules")
    if sample_ms > runner_ms:
        _fail("$.metrics.gpu_sample_span_ms", "cannot exceed runner wall time")
    if active_ms > sample_ms:
        _fail("$.metrics.gpu_active_time_ms", "cannot exceed GPU sample span")
    if peak_vram > hardware["physical_vram_bytes"]:
        _fail("$.metrics.peak_process_vram_bytes", "cannot exceed physical VRAM")

    receipts = _array(item["source_receipt_sha256s"], "$.source_receipt_sha256s")
    if not receipts:
        _fail("$.source_receipt_sha256s", "must bind at least one source receipt")
    for index, digest in enumerate(receipts):
        _sha256(digest, f"$.source_receipt_sha256s[{index}]")
    if receipts != sorted(set(receipts)):
        _fail("$.source_receipt_sha256s", "must be sorted and unique")

    integrity = _object(
        item["integrity"],
        "$.integrity",
        {"technology_pins_are_run_metadata", "automatic_promotion_authority"},
    )
    _constant(
        integrity["technology_pins_are_run_metadata"],
        True,
        "$.integrity.technology_pins_are_run_metadata",
    )
    _constant(
        integrity["automatic_promotion_authority"],
        "none",
        "$.integrity.automatic_promotion_authority",
    )
    _verify_manifest_digest(item)
    expected_id = _measurement_identity(item)
    if measurement_id != expected_id:
        _fail("$.measurement_id", f"must equal deterministic ID {expected_id!r}")
    return item


def _stats_for_system(
    *,
    freeze: dict[str, Any],
    reference: dict[str, Any],
    validated_system: Any,
) -> tuple[list[IntervalStats], list[ResourceMeasurement]]:
    ordered_freeze, freeze_lookup = _freeze_rows(freeze)
    reference_lookup = {row["interval_id"]: row for row in reference["intervals"]}
    system_lookup: dict[str, tuple[dict[str, Any], str]] = {}
    resources: list[ResourceMeasurement] = []
    for recording in validated_system.manifest["recordings"]:
        family_id = recording["recording_family_id"]
        measured = recording["resources"]
        resources.append(
            ResourceMeasurement(
                recording_id=recording["recording_id"],
                family_id=family_id,
                split=recording["split"],
                audio_duration_ms=recording["evaluated_audio_duration_ms"],
                wall_time_ms=measured["wall_time_ms"],
                cpu_time_ms=measured["cpu_time_ms"],
                peak_rss_bytes=measured["peak_rss_bytes"],
            )
        )
        for interval in recording["intervals"]:
            system_lookup[interval["interval_id"]] = (interval, family_id)
    stats = []
    for frozen in ordered_freeze:
        hypothesis, family_id = system_lookup[frozen["interval_id"]]
        stats.append(
            _interval_stats(
                freeze_lookup[frozen["interval_id"]],
                reference_lookup[frozen["interval_id"]],
                hypothesis,
                family_id,
                validated_system.term_tokens,
            )
        )
    return stats, resources


def _metric(aggregate: dict[str, Any], path: Sequence[str]) -> float | None:
    value: Any = aggregate
    for key in path:
        value = value[key]
    return None if value is None else float(value)


PAIR_METRICS: dict[str, tuple[tuple[str, ...], str]] = {
    "word_error_rate": (("word", "error_rate"), "lower_is_better"),
    "character_error_rate": (("character", "error_rate"), "lower_is_better"),
    "himr_term_recall": (("himr_terms", "recall"), "higher_is_better"),
    "himr_false_insertions_per_media_hour": (
        ("himr_terms", "false_insertions_per_media_hour"),
        "lower_is_better",
    ),
    "nonspeech_segments_per_hour": (
        ("nonspeech_hallucination", "segments_per_nonspeech_hour"),
        "lower_is_better",
    ),
    "p95_absolute_boundary_error_ms": (
        ("timing", "p95_absolute_boundary_error_ms"),
        "lower_is_better",
    ),
}


def _percentile(values: Sequence[float], probability: float) -> float:
    ordered = sorted(values)
    if not ordered:
        raise ValueError("percentile needs observations")
    location = (len(ordered) - 1) * probability
    low = math.floor(location)
    high = math.ceil(location)
    if low == high:
        return ordered[low]
    return ordered[low] + (ordered[high] - ordered[low]) * (location - low)


def _paired_metric(
    baseline: Sequence[IntervalStats],
    challenger: Sequence[IntervalStats],
    path: Sequence[str],
) -> tuple[float | None, float | None, float | None]:
    baseline_value = _metric(_scope_aggregate(baseline, []), path)
    challenger_value = _metric(_scope_aggregate(challenger, []), path)
    delta = (
        None
        if baseline_value is None or challenger_value is None
        else challenger_value - baseline_value
    )
    return baseline_value, challenger_value, delta


def _paired_bootstrap(
    *,
    baseline: Sequence[IntervalStats],
    challenger: Sequence[IntervalStats],
    path: Sequence[str],
    replicates: int,
    seed: int,
    label: str,
    relative: bool = False,
) -> dict[str, Any]:
    family_ids = sorted({row.family_id for row in baseline})
    if family_ids != sorted({row.family_id for row in challenger}):
        _fail("$.systems", "recording-family assignments differ between systems")
    observed_baseline, _, observed_delta = _paired_metric(baseline, challenger, path)
    observed = (
        None
        if observed_delta is None or (relative and observed_baseline in {None, 0.0})
        else observed_delta / observed_baseline
        if relative
        else observed_delta
    )
    if observed is None:
        return {"state": "not_available", "reason": "metric_denominator_is_zero"}
    if len(family_ids) < 2:
        return {"state": "not_available", "reason": "fewer_than_two_recording_families"}
    baseline_by_family = {
        family_id: [row for row in baseline if row.family_id == family_id]
        for family_id in family_ids
    }
    challenger_by_family = {
        family_id: [row for row in challenger if row.family_id == family_id]
        for family_id in family_ids
    }
    label_seed = int.from_bytes(hashlib.sha256(label.encode("utf-8")).digest()[:8], "big")
    generator = random.Random((seed + label_seed) % (2**63))
    samples: list[float] = []
    for _ in range(replicates):
        baseline_sample: list[IntervalStats] = []
        challenger_sample: list[IntervalStats] = []
        for _ in family_ids:
            selected = family_ids[generator.randrange(len(family_ids))]
            baseline_sample.extend(baseline_by_family[selected])
            challenger_sample.extend(challenger_by_family[selected])
        sample_baseline, _, sample_delta = _paired_metric(
            baseline_sample, challenger_sample, path
        )
        value = (
            None
            if sample_delta is None or (relative and sample_baseline in {None, 0.0})
            else sample_delta / sample_baseline
            if relative
            else sample_delta
        )
        if value is None:
            return {
                "state": "not_available",
                "reason": "paired_bootstrap_resample_lost_metric_denominator",
            }
        samples.append(value)
    return {
        "state": "available",
        "method": PAIRED_BOOTSTRAP_METHOD,
        "unit": BOOTSTRAP_UNIT,
        "confidence_level": 0.95,
        "replicates": replicates,
        "lower": _round(_percentile(samples, 0.025)),
        "upper": _round(_percentile(samples, 0.975)),
    }


def _comparison_row(
    *,
    name: str,
    path: Sequence[str],
    direction: str,
    baseline: Sequence[IntervalStats],
    challenger: Sequence[IntervalStats],
    replicates: int,
    seed: int,
) -> dict[str, Any]:
    baseline_value, challenger_value, delta = _paired_metric(baseline, challenger, path)
    relative_change = None
    if delta is not None and baseline_value not in {None, 0.0}:
        relative_change = delta / baseline_value
    return {
        "metric": name,
        "direction": direction,
        "baseline": _round(baseline_value),
        "challenger": _round(challenger_value),
        "challenger_minus_baseline": _round(delta),
        "relative_change": _round(relative_change),
        "paired_delta_confidence_interval": _paired_bootstrap(
            baseline=baseline,
            challenger=challenger,
            path=path,
            replicates=replicates,
            seed=seed,
            label=name,
        ),
        "paired_relative_change_confidence_interval": _paired_bootstrap(
            baseline=baseline,
            challenger=challenger,
            path=path,
            replicates=replicates,
            seed=seed,
            label=f"{name}:relative_change",
            relative=True,
        ),
    }


def _gpu_efficiency(measurement: dict[str, Any]) -> dict[str, Any]:
    audio_ms = measurement["protocol"]["evaluated_audio_duration_ms"]
    metrics = measurement["metrics"]
    wall_ms = metrics["runner_wall_time_ms"]
    active_ms = metrics["gpu_active_time_ms"]
    return {
        "evaluated_audio_duration_ms": audio_ms,
        "runner_wall_time_ms": wall_ms,
        "gpu_sample_span_ms": metrics["gpu_sample_span_ms"],
        "gpu_active_time_ms": active_ms,
        "runner_wall_real_time_factor": _round(wall_ms / audio_ms),
        "gpu_active_real_time_factor": _round(active_ms / audio_ms),
        "media_hours_per_runner_hour": _round(audio_ms / wall_ms),
        "media_hours_per_active_gpu_hour": (
            None if active_ms == 0 else _round(audio_ms / active_ms)
        ),
        "peak_process_vram_bytes": metrics["peak_process_vram_bytes"],
        "estimated_energy_millijoules": metrics["estimated_energy_millijoules"],
        # 1 Wh = 3.6e6 mJ and one media hour = 3.6e6 media ms, so
        # the conversion factors cancel exactly.
        "estimated_wh_per_media_hour": _round(
            metrics["estimated_energy_millijoules"] / audio_ms
        ),
    }


def _gate_failures(score_report: dict[str, Any]) -> list[str]:
    return sorted(
        row["gate_id"]
        for row in score_report["quality_gates"]
        if row["status"] == "fail"
    )


def _accuracy_signal(rows: Sequence[dict[str, Any]]) -> str:
    wer = next(row for row in rows if row["metric"] == "word_error_rate")
    interval = wer["paired_delta_confidence_interval"]
    if interval["state"] != "available":
        return "not_evaluable"
    if interval["upper"] < 0:
        return "challenger_improves_wer"
    if interval["lower"] > 0:
        return "challenger_regresses_wer"
    return "wer_difference_inconclusive"


def _condition_definitions(
    baseline: Sequence[IntervalStats],
) -> list[tuple[str, str, Callable[[IntervalStats], bool]]]:
    languages = sorted(
        {
            language
            for row in baseline
            for language in row.flags["language_tags"]
        }
    )
    definitions: list[tuple[str, str, Callable[[IntervalStats], bool]]] = []
    for language in languages:
        definitions.append(
            (
                "language",
                language,
                lambda row, language=language: language in row.flags["language_tags"],
            )
        )
    for dimension, key in (
        ("code_switch", "code_switch"),
        ("speaker_overlap", "speaker_overlap"),
        ("playback_speech", "playback_speech"),
    ):
        for enabled in (False, True):
            definitions.append(
                (
                    dimension,
                    "true" if enabled else "false",
                    lambda row, key=key, enabled=enabled: row.flags[key] is enabled,
                )
            )
    for noise in ("clean", "light", "moderate", "heavy", "unknown"):
        definitions.append(
            (
                "noise",
                noise,
                lambda row, noise=noise: row.flags["noise"] == noise,
            )
        )
    return definitions


def _condition_accuracy(
    *,
    baseline: Sequence[IntervalStats],
    challenger: Sequence[IntervalStats],
    replicates: int,
    seed: int,
) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for dimension, value, predicate in _condition_definitions(baseline):
        baseline_subset = [row for row in baseline if predicate(row)]
        challenger_subset = [row for row in challenger if predicate(row)]
        if [row.recording_id for row in baseline_subset] != [
            row.recording_id for row in challenger_subset
        ]:
            _fail("$.systems", f"condition membership differs for {dimension}:{value}")
        families = len({row.family_id for row in baseline_subset})
        duration_ms = sum(row.duration_ms for row in baseline_subset)
        reference_words = sum(row.reference_words for row in baseline_subset)
        covered = (
            families >= MIN_CONDITION_RECORDING_FAMILIES
            and reference_words >= MIN_CONDITION_REFERENCE_WORDS
        )
        comparison = None
        if covered:
            comparison = _comparison_row(
                name="word_error_rate",
                path=("word", "error_rate"),
                direction="lower_is_better",
                baseline=baseline_subset,
                challenger=challenger_subset,
                replicates=replicates,
                seed=seed,
            )
            if comparison["paired_delta_confidence_interval"]["state"] != "available":
                covered = False
                comparison = None
        rows.append(
            {
                "dimension": dimension,
                "value": value,
                "evaluation_state": "available" if covered else "undercovered",
                "coverage": {
                    "interval_count": len(baseline_subset),
                    "recording_family_count": families,
                    "duration_ms": duration_ms,
                    "reference_word_count": reference_words,
                    "minimum_recording_families": MIN_CONDITION_RECORDING_FAMILIES,
                    "minimum_reference_words": MIN_CONDITION_REFERENCE_WORDS,
                },
                "word_error_rate": comparison,
                "undercoverage_reason": (
                    None
                    if covered
                    else "condition_requires_two_recording_families_and_100_reference_words_with_an_available_paired_interval"
                ),
            }
        )
    return rows


def _condition_safeguard(rows: Sequence[dict[str, Any]]) -> dict[str, Any]:
    undercovered = [
        f"{row['dimension']}:{row['value']}"
        for row in rows
        if row["evaluation_state"] != "available"
    ]
    regressions = []
    for row in rows:
        comparison = row["word_error_rate"]
        if comparison is None:
            continue
        interval = comparison["paired_delta_confidence_interval"]
        if interval["state"] == "available" and interval["lower"] > 0:
            regressions.append(f"{row['dimension']}:{row['value']}")
    if regressions:
        status = "condition_regression_detected"
    elif undercovered:
        status = "not_evaluable_due_to_undercoverage"
    else:
        status = "no_condition_regression_detected"
    return {
        "status": status,
        "undercovered_conditions": undercovered,
        "conditions_with_paired_wer_regression": regressions,
        "overall_wer_can_override_condition_regression": False,
    }


def _wer_noninferiority(rows: Sequence[dict[str, Any]]) -> dict[str, Any]:
    """Apply the registered accuracy-first dual WER upper-bound safeguard."""

    wer = next(row for row in rows if row["metric"] == "word_error_rate")
    absolute = wer["paired_delta_confidence_interval"]
    relative = wer["paired_relative_change_confidence_interval"]
    thresholds = {
        "maximum_absolute_wer_increase": 0.005,
        "maximum_relative_wer_increase": 0.03,
    }
    if absolute["state"] != "available" or relative["state"] != "available":
        return {
            "status": "not_evaluable",
            "thresholds": thresholds,
            "absolute_upper": None if absolute["state"] != "available" else absolute["upper"],
            "relative_upper": None if relative["state"] != "available" else relative["upper"],
            "reason": "both_paired_absolute_and_relative_wer_intervals_are_required",
        }
    demonstrated = (
        absolute["upper"] <= thresholds["maximum_absolute_wer_increase"]
        and relative["upper"] <= thresholds["maximum_relative_wer_increase"]
    )
    return {
        "status": "demonstrated" if demonstrated else "not_demonstrated",
        "thresholds": thresholds,
        "absolute_upper": absolute["upper"],
        "relative_upper": relative["upper"],
        "reason": None if demonstrated else "one_or_both_paired_upper_bounds_exceed_the_registered_margin",
    }


def compare_transcript_systems(
    *,
    candidate_cohort: object,
    interval_freeze: object,
    pass_a: object,
    pass_b: object,
    adjudication: object,
    baseline_system_output: object,
    challenger_system_output: object,
    baseline_gpu_measurement: object,
    challenger_gpu_measurement: object,
    created_at: str,
) -> dict[str, Any]:
    """Emit one text-free paired accuracy/GPU-efficiency comparison."""

    freeze = validate_interval_freeze(interval_freeze, candidate_cohort)
    reference = validate_adjudication(
        adjudication, freeze, candidate_cohort, pass_a, pass_b
    )
    baseline = validate_transcript_system_output(
        baseline_system_output, freeze, candidate_cohort
    )
    challenger = validate_transcript_system_output(
        challenger_system_output, freeze, candidate_cohort
    )
    if baseline.manifest["manifest_sha256"] == challenger.manifest["manifest_sha256"]:
        _fail("$.systems", "baseline and challenger must be distinct system outputs")
    for field in ("revision_kind", "resource_profile"):
        if baseline.manifest["system"][field] != challenger.manifest["system"][field]:
            _fail("$.systems", f"{field} differs between baseline and challenger")
    if baseline.term_set_sha256 != challenger.term_set_sha256:
        _fail("$.systems", "normalized HIMR term sets differ")
    if baseline.manifest["scoring_profile"] != challenger.manifest["scoring_profile"]:
        _fail("$.systems", "scoring profiles differ between baseline and challenger")
    baseline_families = [
        (row["recording_id"], row["recording_family_id"], row["split"])
        for row in baseline.manifest["recordings"]
    ]
    challenger_families = [
        (row["recording_id"], row["recording_family_id"], row["split"])
        for row in challenger.manifest["recordings"]
    ]
    if baseline_families != challenger_families:
        _fail("$.systems", "recording-family assignments differ between systems")

    baseline_measurement = validate_gpu_evaluation_measurement(
        baseline_gpu_measurement,
        system_output=baseline.manifest,
        interval_freeze=freeze,
        candidate_cohort=candidate_cohort,
    )
    challenger_measurement = validate_gpu_evaluation_measurement(
        challenger_gpu_measurement,
        system_output=challenger.manifest,
        interval_freeze=freeze,
        candidate_cohort=candidate_cohort,
    )
    if baseline_measurement["hardware"] != challenger_measurement["hardware"]:
        _fail("$.gpu_measurements", "hardware differs between baseline and challenger")
    protocol_fields = (
        "protocol_id",
        "evaluated_audio_duration_ms",
        "includes_model_load",
        "includes_audio_preflight",
        "includes_result_publication",
        "warmup_runs",
        "measured_runs",
    )
    for field in protocol_fields:
        if baseline_measurement["protocol"][field] != challenger_measurement["protocol"][field]:
            _fail("$.gpu_measurements", f"measurement protocol field {field} differs")

    baseline_stats, _ = _stats_for_system(
        freeze=freeze, reference=reference, validated_system=baseline
    )
    challenger_stats, _ = _stats_for_system(
        freeze=freeze, reference=reference, validated_system=challenger
    )
    baseline_scoring = [row for row in baseline_stats if row.split == "scoring"]
    challenger_scoring = [row for row in challenger_stats if row.split == "scoring"]
    bootstrap = baseline.manifest["scoring_profile"]["bootstrap"]
    replicates = _bounded_int(
        bootstrap["replicates"], "$.scoring_profile.bootstrap.replicates", minimum=MIN_REPLICATES
    )
    if replicates > MAX_REPLICATES:
        _fail("$.scoring_profile.bootstrap.replicates", f"must be <= {MAX_REPLICATES}")
    seed = bootstrap["seed"]
    metric_rows = [
        _comparison_row(
            name=name,
            path=path,
            direction=direction,
            baseline=baseline_scoring,
            challenger=challenger_scoring,
            replicates=replicates,
            seed=seed,
        )
        for name, (path, direction) in PAIR_METRICS.items()
    ]
    condition_rows = _condition_accuracy(
        baseline=baseline_scoring,
        challenger=challenger_scoring,
        replicates=replicates,
        seed=seed,
    )

    created = _timestamp(created_at, "$.created_at")
    latest = max(
        _timestamp(reference["created_at"], "$.adjudication.created_at"),
        _timestamp(baseline_measurement["created_at"], "$.baseline_gpu_measurement.created_at"),
        _timestamp(challenger_measurement["created_at"], "$.challenger_gpu_measurement.created_at"),
    )
    if created < latest:
        _fail("$.created_at", "cannot precede adjudication or either GPU measurement")

    # Recompute ordinary reports so comparison gates and summary values are never
    # accepted from an untrusted aggregate-only document.
    baseline_report = score_transcript_system(
        candidate_cohort=candidate_cohort,
        interval_freeze=freeze,
        pass_a=pass_a,
        pass_b=pass_b,
        adjudication=reference,
        system_output=baseline.manifest,
        created_at=created_at,
    )
    challenger_report = score_transcript_system(
        candidate_cohort=candidate_cohort,
        interval_freeze=freeze,
        pass_a=pass_a,
        pass_b=pass_b,
        adjudication=reference,
        system_output=challenger.manifest,
        created_at=created_at,
    )
    baseline_efficiency = _gpu_efficiency(baseline_measurement)
    challenger_efficiency = _gpu_efficiency(challenger_measurement)
    rtf_delta = (
        challenger_efficiency["runner_wall_real_time_factor"]
        - baseline_efficiency["runner_wall_real_time_factor"]
    )
    energy_delta = (
        challenger_efficiency["estimated_wh_per_media_hour"]
        - baseline_efficiency["estimated_wh_per_media_hour"]
    )
    implementation_sha = _implementation_sha256()
    comparison_id = _stable_id(
        "transcript_system_comparison",
        freeze["manifest_sha256"],
        reference["manifest_sha256"],
        baseline.manifest["manifest_sha256"],
        challenger.manifest["manifest_sha256"],
        baseline_measurement["manifest_sha256"],
        challenger_measurement["manifest_sha256"],
        implementation_sha,
        created_at,
    )
    report = {
        "schema_version": COMPARISON_SCHEMA_VERSION,
        "manifest_kind": "transcript_system_comparison",
        "manifest_sha256": "0" * 64,
        "comparison_id": comparison_id,
        "created_at": created.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "implementation": {
            "name": IMPLEMENTATION_NAME,
            "version": IMPLEMENTATION_VERSION,
            "source_sha256": implementation_sha,
            "python_version": platform.python_version(),
            "unicode_version": unicodedata.unidata_version,
        },
        "inputs": {
            "cohort_id": freeze["cohort_id"],
            "cohort_manifest_sha256": freeze["cohort_manifest_sha256"],
            "freeze_id": freeze["freeze_id"],
            "freeze_manifest_sha256": freeze["manifest_sha256"],
            "adjudication_id": reference["adjudication_id"],
            "adjudication_manifest_sha256": reference["manifest_sha256"],
            "baseline_system_output_id": baseline.manifest["system_output_id"],
            "baseline_system_output_manifest_sha256": baseline.manifest["manifest_sha256"],
            "challenger_system_output_id": challenger.manifest["system_output_id"],
            "challenger_system_output_manifest_sha256": challenger.manifest["manifest_sha256"],
            "baseline_gpu_measurement_id": baseline_measurement["measurement_id"],
            "baseline_gpu_measurement_manifest_sha256": baseline_measurement["manifest_sha256"],
            "challenger_gpu_measurement_id": challenger_measurement["measurement_id"],
            "challenger_gpu_measurement_manifest_sha256": challenger_measurement["manifest_sha256"],
        },
        "systems": {
            "baseline": dict(baseline.manifest["system"]),
            "challenger": dict(challenger.manifest["system"]),
        },
        "comparability": {
            "same_frozen_reference": True,
            "same_recording_family_map": True,
            "same_scoring_profile": True,
            "same_gpu_hardware": True,
            "same_gpu_measurement_protocol": True,
            "technology_pins_used_for_run_integrity_only": True,
        },
        "paired_accuracy_metrics": metric_rows,
        "condition_accuracy": condition_rows,
        "gpu_efficiency": {
            "hardware": dict(baseline_measurement["hardware"]),
            "protocol": dict(baseline_measurement["protocol"]),
            "baseline": baseline_efficiency,
            "challenger": challenger_efficiency,
            "challenger_minus_baseline_runner_wall_rtf": _round(rtf_delta),
            "challenger_minus_baseline_estimated_wh_per_media_hour": _round(energy_delta),
        },
        "decision_support": {
            "accuracy_signal": _accuracy_signal(metric_rows),
            "wer_noninferiority": _wer_noninferiority(metric_rows),
            "condition_safeguard": _condition_safeguard(condition_rows),
            "repetition_review": {
                "baseline_candidate_interval_count": sum(
                    row.repetition_candidate for row in baseline_scoring
                ),
                "challenger_candidate_interval_count": sum(
                    row.repetition_candidate for row in challenger_scoring
                ),
                "review_state": (
                    "not_required"
                    if not any(row.repetition_candidate for row in challenger_scoring)
                    else "human_review_required"
                ),
                "unattended_promotion_blocked": any(
                    row.repetition_candidate for row in challenger_scoring
                ),
            },
            "efficiency_signal": (
                "challenger_faster"
                if rtf_delta < 0
                else "challenger_slower"
                if rtf_delta > 0
                else "observed_tie"
            ),
            "baseline_failed_quality_gates": _gate_failures(baseline_report),
            "challenger_failed_quality_gates": _gate_failures(challenger_report),
            "automatic_promotion_authorized": False,
            "selection_policy": "accuracy_first_then_efficiency_with_human_promotion",
        },
        "warnings": [
            "A paired confidence interval is decision support, not proof that a model generalizes beyond the frozen cohort.",
            "GPU active time and energy are sampled estimates; runner wall time is the capacity-planning denominator.",
            "No scalar accuracy-per-GPU-hour score is emitted because it would hide quality regressions behind throughput.",
            "Technology hashes identify these runs only and do not freeze the preferred model or runtime.",
        ],
        "publication": dict(REPORT_PUBLICATION),
        "contains_transcript_text": False,
        "automatic_promotion_authority": "none",
    }
    report["manifest_sha256"] = canonical_manifest_sha256(report)
    return report


def validate_transcript_system_comparison(
    report: object,
    **inputs: object,
) -> dict[str, Any]:
    """Recompute and byte-compare a comparison report."""

    if not isinstance(report, dict):
        _fail("$", "comparison report must be an object")
    _verify_manifest_digest(report)
    created_at = report.get("created_at")
    _timestamp(created_at, "$.created_at")
    expected = compare_transcript_systems(created_at=created_at, **inputs)
    if _canonical_bytes(report) != _canonical_bytes(expected):
        _fail("$", "comparison report differs from exact recomputation")
    return report


__all__ = [
    "COMPARISON_SCHEMA_VERSION",
    "GPU_MEASUREMENT_SCHEMA_VERSION",
    "compare_transcript_systems",
    "seal_gpu_evaluation_measurement",
    "validate_gpu_evaluation_measurement",
    "validate_transcript_system_comparison",
]
