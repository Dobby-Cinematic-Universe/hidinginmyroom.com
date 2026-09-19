from __future__ import annotations

import copy
import json
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path

from jsonschema import Draft202012Validator, FormatChecker

from evaluation.candidate_comparison import (
    compare_transcript_systems,
    seal_gpu_evaluation_measurement,
    validate_gpu_evaluation_measurement,
    validate_transcript_system_comparison,
)
from evaluation.scoring import seal_transcript_system_output
from evaluation.tests.test_scoring import (
    _timed_words,
    make_system_output,
    reference_fixture,
)
from evaluation.validation import ContractError
from evaluation.validation import audit_tracked_evaluation_data


ROOT = Path(__file__).resolve().parents[2]
MEASUREMENT_SCHEMA = ROOT / "evaluation/schemas/gpu-asr-evaluation-measurement.schema.json"
COMPARISON_SCHEMA = ROOT / "evaluation/schemas/transcript-system-comparison.schema.json"


def make_measurement(system_output: dict, *, wall_ms: int, active_ms: int, energy_mj: int) -> dict:
    duration = sum(row["evaluated_audio_duration_ms"] for row in system_output["recordings"])
    value = {
        "schema_version": 1,
        "manifest_kind": "gpu_asr_evaluation_measurement",
        "manifest_sha256": "0" * 64,
        "measurement_id": "gpu_measurement_" + "0" * 32,
        "created_at": "2026-08-26T22:00:00Z",
        "system_output_id": system_output["system_output_id"],
        "system_output_manifest_sha256": system_output["manifest_sha256"],
        "profile_identity_sha256": "c" * 64,
        "hardware": {
            "gpu_uuid": "GPU-0b9d7029-b3c6-1a0b-d483-6a3febc3b557",
            "gpu_name": "NVIDIA GeForce RTX 3050",
            "physical_vram_bytes": 6 * 1024**3,
        },
        "protocol": {
            "protocol_id": "gpu_eval_full_frozen_cohort_v1",
            "evaluated_audio_duration_ms": duration,
            "includes_model_load": True,
            "includes_audio_preflight": False,
            "includes_result_publication": True,
            "warmup_runs": 0,
            "measured_runs": 1,
        },
        "metrics": {
            "runner_wall_time_ms": wall_ms,
            "gpu_sample_span_ms": wall_ms - 100,
            "gpu_active_time_ms": active_ms,
            "peak_process_vram_bytes": 2 * 1024**3,
            "estimated_energy_millijoules": energy_mj,
        },
        "source_receipt_sha256s": ["d" * 64],
        "integrity": {
            "technology_pins_are_run_metadata": True,
            "automatic_promotion_authority": "none",
        },
    }
    return seal_gpu_evaluation_measurement(value)


class CandidateComparisonTest(unittest.TestCase):
    def setUp(self) -> None:
        (
            self.cohort,
            self.freeze,
            self.pass_a,
            self.pass_b,
            self.adjudication,
        ) = reference_fixture()
        self.baseline = make_system_output(self.freeze, self.adjudication)
        self.baseline["system"]["resource_profile"] = "local_gpu"
        seal_transcript_system_output(self.baseline)

        self.challenger = copy.deepcopy(self.baseline)
        self.challenger["system"].update(
            {
                "system_id": "system_whisper_challenger_v1",
                "system_label": "Synthetic challenger fixture",
                "model_id": "model_whisper_challenger_v1",
                "run_manifest_sha256": "b" * 64,
            }
        )
        references = {row["interval_id"]: row for row in self.adjudication["intervals"]}
        # Restore the baseline's deliberate substitution to the exact reference.
        interval = self.challenger["recordings"][3]["intervals"][0]
        reference = references[interval["interval_id"]]
        utterances = sorted(
            reference["utterances"],
            key=lambda row: (row["start_ms"], row["end_ms"], row["utterance_id"]),
        )
        text = " ".join(row["text"] for row in utterances)
        start = min(row["start_ms"] for row in utterances)
        end = max(row["end_ms"] for row in utterances)
        segment = interval["segments"][0]
        segment["text"] = text
        segment["words"] = _timed_words(
            text, prefix="challenger_exact_0003", start=start, end=end
        )
        # Remove the baseline's deliberate nonspeech repetition candidate.
        self.challenger["recordings"][4]["intervals"][0]["segments"] = []
        seal_transcript_system_output(self.challenger)

        self.baseline_measurement = make_measurement(
            self.baseline, wall_ms=20_000, active_ms=16_000, energy_mj=600_000
        )
        self.challenger_measurement = make_measurement(
            self.challenger, wall_ms=12_000, active_ms=9_000, energy_mj=360_000
        )
        self.inputs = {
            "candidate_cohort": self.cohort,
            "interval_freeze": self.freeze,
            "pass_a": self.pass_a,
            "pass_b": self.pass_b,
            "adjudication": self.adjudication,
            "baseline_system_output": self.baseline,
            "challenger_system_output": self.challenger,
            "baseline_gpu_measurement": self.baseline_measurement,
            "challenger_gpu_measurement": self.challenger_measurement,
        }
        self.report = compare_transcript_systems(
            **self.inputs, created_at="2026-08-26T23:30:00Z"
        )

    def test_emits_paired_accuracy_and_gpu_efficiency_without_text(self) -> None:
        rows = {row["metric"]: row for row in self.report["paired_accuracy_metrics"]}
        self.assertLess(rows["word_error_rate"]["challenger_minus_baseline"], 0)
        self.assertLess(
            rows["nonspeech_segments_per_hour"]["challenger_minus_baseline"], 0
        )
        self.assertEqual(
            rows["word_error_rate"]["paired_delta_confidence_interval"]["method"],
            "recording_family_paired_percentile_v1",
        )
        self.assertIn(
            rows["word_error_rate"]["paired_relative_change_confidence_interval"]["state"],
            {"available", "not_available"},
        )
        gpu = self.report["gpu_efficiency"]
        self.assertGreater(
            gpu["challenger"]["media_hours_per_runner_hour"],
            gpu["baseline"]["media_hours_per_runner_hour"],
        )
        self.assertLess(
            gpu["challenger"]["estimated_wh_per_media_hour"],
            gpu["baseline"]["estimated_wh_per_media_hour"],
        )
        self.assertEqual(
            self.report["decision_support"]["efficiency_signal"], "challenger_faster"
        )
        self.assertFalse(
            self.report["decision_support"]["automatic_promotion_authorized"]
        )
        noninferiority = self.report["decision_support"]["wer_noninferiority"]
        self.assertEqual(
            noninferiority["thresholds"]["maximum_absolute_wer_increase"], 0.005
        )
        self.assertEqual(
            noninferiority["thresholds"]["maximum_relative_wer_increase"], 0.03
        )
        self.assertEqual(noninferiority["status"], "not_evaluable")
        self.assertIsNone(noninferiority["relative_upper"])
        self.assertIn("both_paired", noninferiority["reason"])
        conditions = self.report["condition_accuracy"]
        self.assertTrue(
            any(
                row["dimension"] == "noise" and row["value"] == "moderate"
                for row in conditions
            )
        )
        undercovered = [
            row for row in conditions if row["evaluation_state"] == "undercovered"
        ]
        self.assertTrue(undercovered)
        self.assertTrue(all(row["word_error_rate"] is None for row in undercovered))
        safeguard = self.report["decision_support"]["condition_safeguard"]
        self.assertFalse(safeguard["overall_wer_can_override_condition_regression"])
        self.assertEqual(
            self.report["decision_support"]["repetition_review"]["review_state"],
            "not_required",
        )
        serialized = json.dumps(self.report)
        self.assertNotIn("synthetic adjudicated fixture", serialized)
        self.assertFalse(self.report["contains_transcript_text"])

    def test_measurement_and_comparison_recompute_exactly(self) -> None:
        validate_gpu_evaluation_measurement(
            self.baseline_measurement,
            system_output=self.baseline,
            interval_freeze=self.freeze,
            candidate_cohort=self.cohort,
        )
        validate_transcript_system_comparison(self.report, **self.inputs)
        checker = FormatChecker()
        for path, value in (
            (MEASUREMENT_SCHEMA, self.baseline_measurement),
            (COMPARISON_SCHEMA, self.report),
        ):
            schema = json.loads(path.read_text(encoding="utf-8"))
            errors = sorted(
                Draft202012Validator(schema, format_checker=checker).iter_errors(value),
                key=lambda error: list(error.path),
            )
            self.assertEqual(errors, [], "\n".join(error.message for error in errors))

        mutated = copy.deepcopy(self.report)
        mutated["gpu_efficiency"]["challenger"]["runner_wall_time_ms"] += 1
        with self.assertRaisesRegex(ContractError, "canonical digest mismatch"):
            validate_transcript_system_comparison(mutated, **self.inputs)

    def test_rejects_unpaired_family_or_measurement_protocol(self) -> None:
        challenger = copy.deepcopy(self.challenger)
        challenger["recordings"][0]["recording_family_id"] = "recording_family_changed"
        seal_transcript_system_output(challenger)
        measurement = make_measurement(
            challenger, wall_ms=12_000, active_ms=9_000, energy_mj=360_000
        )
        with self.assertRaisesRegex(ContractError, "recording-family assignments differ"):
            compare_transcript_systems(
                **{
                    **self.inputs,
                    "challenger_system_output": challenger,
                    "challenger_gpu_measurement": measurement,
                },
                created_at="2026-08-26T23:30:00Z",
            )

        protocol = copy.deepcopy(self.challenger_measurement)
        protocol["protocol"]["includes_model_load"] = False
        seal_gpu_evaluation_measurement(protocol)
        with self.assertRaisesRegex(ContractError, "includes_model_load differs"):
            compare_transcript_systems(
                **{**self.inputs, "challenger_gpu_measurement": protocol},
                created_at="2026-08-26T23:30:00Z",
            )

    def test_rejects_measurement_bound_to_another_system(self) -> None:
        swapped = copy.deepcopy(self.baseline_measurement)
        swapped["system_output_id"] = self.challenger["system_output_id"]
        swapped["system_output_manifest_sha256"] = self.challenger["manifest_sha256"]
        seal_gpu_evaluation_measurement(swapped)
        with self.assertRaisesRegex(ContractError, "does not match the exact system output"):
            validate_gpu_evaluation_measurement(
                swapped,
                system_output=self.baseline,
                interval_freeze=self.freeze,
                candidate_cohort=self.cohort,
            )

    def test_challenger_repetition_candidate_blocks_unattended_promotion(self) -> None:
        challenger = copy.deepcopy(self.baseline)
        challenger["system"].update(
            {
                "system_id": "system_repetition_challenger_v1",
                "system_label": "Repetition challenger fixture",
                "model_id": "model_repetition_challenger_v1",
                "run_manifest_sha256": "e" * 64,
            }
        )
        seal_transcript_system_output(challenger)
        measurement = make_measurement(
            challenger, wall_ms=12_000, active_ms=9_000, energy_mj=360_000
        )
        report = compare_transcript_systems(
            **{
                **self.inputs,
                "challenger_system_output": challenger,
                "challenger_gpu_measurement": measurement,
            },
            created_at="2026-08-26T23:30:00Z",
        )
        repetition = report["decision_support"]["repetition_review"]
        self.assertGreater(repetition["challenger_candidate_interval_count"], 0)
        self.assertEqual(repetition["review_state"], "human_review_required")
        self.assertTrue(repetition["unattended_promotion_blocked"])

    def test_cli_emits_comparison(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            values = {
                "cohort": self.cohort,
                "freeze": self.freeze,
                "pass-a": self.pass_a,
                "pass-b": self.pass_b,
                "adjudication": self.adjudication,
                "baseline": self.baseline,
                "challenger": self.challenger,
                "baseline-gpu": self.baseline_measurement,
                "challenger-gpu": self.challenger_measurement,
            }
            paths = {}
            for name, value in values.items():
                path = root / f"{name}.json"
                path.write_text(json.dumps(value), encoding="utf-8")
                paths[name] = path
            command = [
                sys.executable,
                "-m",
                "evaluation",
                "compare-transcript-systems",
                "--cohort",
                str(paths["cohort"]),
                "--freeze",
                str(paths["freeze"]),
                "--pass-a",
                str(paths["pass-a"]),
                "--pass-b",
                str(paths["pass-b"]),
                "--adjudication",
                str(paths["adjudication"]),
                "--baseline-system-output",
                str(paths["baseline"]),
                "--challenger-system-output",
                str(paths["challenger"]),
                "--baseline-gpu-measurement",
                str(paths["baseline-gpu"]),
                "--challenger-gpu-measurement",
                str(paths["challenger-gpu"]),
                "--created-at",
                self.report["created_at"],
            ]
            result = subprocess.run(
                command, cwd=ROOT, check=True, capture_output=True, text=True
            )
            self.assertEqual(json.loads(result.stdout), self.report)

    def test_private_measurement_and_comparison_are_rejected_when_tracked(self) -> None:
        for name, value, label in (
            ("gpu-measurement.json", self.baseline_measurement, "GPU evaluation measurement"),
            ("comparison.json", self.report, "transcript system comparison"),
        ):
            with self.subTest(name=name), tempfile.TemporaryDirectory() as directory:
                repository = Path(directory)
                subprocess.run(["git", "init", "-q", str(repository)], check=True)
                path = repository / name
                path.write_text(json.dumps(value), encoding="utf-8")
                subprocess.run(["git", "-C", str(repository), "add", name], check=True)
                with self.assertRaisesRegex(ContractError, label):
                    audit_tracked_evaluation_data(repository)


if __name__ == "__main__":
    unittest.main()
