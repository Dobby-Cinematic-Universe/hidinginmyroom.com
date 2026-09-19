from __future__ import annotations

import copy
import json
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path

from jsonschema import Draft202012Validator, FormatChecker

from evaluation.scoring import (
    BOOTSTRAP_METHOD,
    BOOTSTRAP_UNIT,
    NORMALIZATION_PROFILE,
    _levenshtein_distance,
    _term_occurrence_spans,
    _word_alignment,
    normalize_characters,
    normalize_words,
    score_transcript_system,
    seal_transcript_system_output,
    validate_transcript_score_report,
    validate_transcript_system_output,
)
from evaluation.tests.test_validation import (
    load_cohort,
    make_adjudication,
    make_annotation,
    make_freeze,
    seal,
)
from evaluation.validation import ContractError, audit_tracked_evaluation_data


ROOT = Path(__file__).resolve().parents[2]
SYSTEM_SCHEMA = ROOT / "evaluation/schemas/transcript-system-output.schema.json"
REPORT_SCHEMA = ROOT / "evaluation/schemas/transcript-score-report.schema.json"


def _set_non_speech(reference_pass: dict, index: int) -> None:
    interval = reference_pass["intervals"][index]
    interval["annotation_state"] = "non_speech"
    interval["utterances"] = []
    seal(reference_pass)


def reference_fixture() -> tuple[dict, dict, dict, dict, dict]:
    cohort = load_cohort()
    freeze = make_freeze(cohort)
    pass_a = make_annotation(freeze, "pass_a", "reviewer_reference_01")
    pass_b = make_annotation(freeze, "pass_b", "reviewer_reference_02")
    for index in (1, 4):
        _set_non_speech(pass_a, index)
        _set_non_speech(pass_b, index)
    adjudication = make_adjudication(freeze, pass_a, pass_b)
    for index in (1, 4):
        interval = adjudication["intervals"][index]
        interval["annotation_state"] = "non_speech"
        interval["utterances"] = []
        interval["resolution"] = "non_speech"
        interval["source_utterance_ids"] = []
        interval["decision_note"] = "Direct media review found no speech."
    seal(adjudication)
    return cohort, freeze, pass_a, pass_b, adjudication


def _timed_words(text: str, *, prefix: str, start: int, end: int) -> list[dict]:
    tokens = normalize_words(text)
    duration = end - start
    output = []
    for index, token in enumerate(tokens):
        word_start = start + duration * index // len(tokens)
        word_end = start + duration * (index + 1) // len(tokens)
        output.append(
            {
                "word_id": f"word_{prefix}_{index:04d}",
                "text": token,
                "start_ms": word_start,
                "end_ms": word_end,
                "raw_score": 0.85,
            }
        )
    return output


def make_system_output(freeze: dict, adjudication: dict) -> dict:
    references = {row["interval_id"]: row for row in adjudication["intervals"]}
    recordings = []
    for recording_index, frozen_recording in enumerate(freeze["recordings"]):
        if recording_index < 3:
            family_number = min(recording_index, 1)
        else:
            family_number = 10 + (recording_index - 3) // 2
        intervals = []
        for interval_index, frozen_interval in enumerate(frozen_recording["intervals"]):
            reference = references[frozen_interval["interval_id"]]
            global_index = recording_index + interval_index
            segments = []
            if reference["annotation_state"] == "transcribed":
                utterances = sorted(
                    reference["utterances"],
                    key=lambda item: (item["start_ms"], item["end_ms"], item["utterance_id"]),
                )
                text = " ".join(item["text"] for item in utterances)
                # One deliberate scoring-split substitution makes WER/CER nonzero.
                if global_index == 3:
                    text = text.replace("fixture", "example")
                start = min(item["start_ms"] for item in utterances)
                end = max(item["end_ms"] for item in utterances)
                segments.append(
                    {
                        "segment_id": f"segment_fixture_{global_index:04d}",
                        "start_ms": start,
                        "end_ms": end,
                        "text": text,
                        "raw_score": 0.7,
                        "words": _timed_words(
                            text,
                            prefix=f"fixture_{global_index:04d}",
                            start=start,
                            end=end,
                        ),
                    }
                )
                speech_score = 2.0
            else:
                speech_score = -2.0
                # One scoring nonspeech interval deliberately contains a repeated
                # hallucination candidate and false glossary insertions.
                if global_index == 4:
                    text = " ".join(["phantom"] * 12)
                    start = frozen_interval["start_ms"]
                    end = frozen_interval["end_ms"]
                    segments.append(
                        {
                            "segment_id": f"segment_fixture_{global_index:04d}",
                            "start_ms": start,
                            "end_ms": end,
                            "text": text,
                            "raw_score": -0.2,
                            "words": _timed_words(
                                text,
                                prefix=f"fixture_{global_index:04d}",
                                start=start,
                                end=end,
                            ),
                        }
                    )
            intervals.append(
                {
                    "interval_id": frozen_interval["interval_id"],
                    "start_ms": frozen_interval["start_ms"],
                    "end_ms": frozen_interval["end_ms"],
                    "state": "completed",
                    "speech_presence_raw_score": speech_score,
                    "segments": segments,
                }
            )
        evaluated_ms = sum(item["end_ms"] - item["start_ms"] for item in frozen_recording["intervals"])
        recordings.append(
            {
                "recording_id": frozen_recording["recording_id"],
                "recording_family_id": f"recording_family_fixture_{family_number:02d}",
                "split": frozen_recording["split"],
                "evaluated_audio_duration_ms": evaluated_ms,
                "resources": {
                    "wall_time_ms": 1000 + recording_index,
                    "cpu_time_ms": 2000 + recording_index,
                    "peak_rss_bytes": 900_000_000 + recording_index,
                },
                "intervals": intervals,
            }
        )
    value = {
        "schema_version": 1,
        "manifest_kind": "transcript_system_output",
        "manifest_sha256": "0" * 64,
        "system_output_id": "system_output_" + "0" * 32,
        "freeze_id": freeze["freeze_id"],
        "freeze_manifest_sha256": freeze["manifest_sha256"],
        "created_at": "2026-08-26T21:30:00Z",
        "system": {
            "system_id": "system_whisper_fixture_v1",
            "system_label": "Synthetic whisper fixture",
            "model_id": "model_whisper_fixture_v1",
            "run_manifest_sha256": "a" * 64,
            "revision_kind": "raw_asr",
            "resource_profile": "cpu_bronze",
        },
        "scoring_profile": {
            "normalization_profile": NORMALIZATION_PROFILE,
            "term_set": {
                "term_set_revision_id": "term_set_fixture_v1",
                "language": "en",
                "terms": ["synthetic adjudicated", "phantom"],
            },
            "bootstrap": {
                "unit": BOOTSTRAP_UNIT,
                "method": BOOTSTRAP_METHOD,
                "replicates": 200,
                "seed": 73421,
                "confidence_level": 0.95,
            },
        },
        "privacy": {
            "contains_transcript_text": True,
            "storage_policy": "private_only",
            "publication_authority": "none",
        },
        "recordings": recordings,
    }
    return seal_transcript_system_output(value)


class TranscriptScoringTest(unittest.TestCase):
    def setUp(self) -> None:
        (
            self.cohort,
            self.freeze,
            self.pass_a,
            self.pass_b,
            self.adjudication,
        ) = reference_fixture()
        self.system_output = make_system_output(self.freeze, self.adjudication)
        self.report = score_transcript_system(
            candidate_cohort=self.cohort,
            interval_freeze=self.freeze,
            pass_a=self.pass_a,
            pass_b=self.pass_b,
            adjudication=self.adjudication,
            system_output=self.system_output,
            created_at="2026-08-26T23:30:00Z",
        )

    def test_normalization_and_edit_alignment_are_deterministic(self) -> None:
        self.assertEqual(normalize_words(" Café—DON’T 42! "), ["café", "don't", "42"])
        self.assertEqual(normalize_characters("A b-c"), "abc")
        self.assertEqual(normalize_characters("DON’T"), "dont")
        alignment = _word_alignment(["one", "two", "three"], ["one", "too", "extra", "three"])
        self.assertEqual((alignment.substitutions, alignment.deletions, alignment.insertions), (1, 0, 1))
        self.assertEqual(_levenshtein_distance("kitten", "sitting"), 3)
        self.assertEqual(
            _term_occurrence_spans(
                ["himr", "himr", "term", "himr", "term"], ["himr", "term"]
            ),
            [(1, 3), (3, 5)],
        )

    def test_system_output_and_report_validate_against_runtime_and_json_schema(self) -> None:
        validate_transcript_system_output(self.system_output, self.freeze, self.cohort)
        validate_transcript_score_report(
            self.report,
            candidate_cohort=self.cohort,
            interval_freeze=self.freeze,
            pass_a=self.pass_a,
            pass_b=self.pass_b,
            adjudication=self.adjudication,
            system_output=self.system_output,
        )
        checker = FormatChecker()
        for path, instance in ((SYSTEM_SCHEMA, self.system_output), (REPORT_SCHEMA, self.report)):
            schema = json.loads(path.read_text(encoding="utf-8"))
            errors = sorted(
                Draft202012Validator(schema, format_checker=checker).iter_errors(instance),
                key=lambda error: list(error.path),
            )
            self.assertEqual(errors, [], "\n".join(error.message for error in errors))

    def test_report_covers_requested_metrics_without_leaking_text(self) -> None:
        overall = self.report["metrics"]["overall"]
        scoring = self.report["metrics"]["splits"][1]["metrics"]
        self.assertGreater(overall["word"]["error_rate"], 0)
        self.assertGreater(overall["character"]["error_rate"], 0)
        self.assertGreater(scoring["himr_terms"]["false_insertions"], 0)
        self.assertGreater(scoring["himr_terms"]["evaluated_duration_ms"], 0)
        self.assertGreater(scoring["nonspeech_hallucination"]["nonempty_segment_count"], 0)
        self.assertEqual(scoring["repetition"]["candidate_interval_count"], 1)
        self.assertEqual(overall["timing"]["word_timing_coverage"], 1.0)
        self.assertLess(overall["resources"]["wall_real_time_factor"], 1.0)
        self.assertEqual(
            overall["bootstrap_confidence_intervals"]["word_error_rate"]["state"],
            "available",
        )
        self.assertEqual(
            overall["bootstrap_confidence_intervals"]["peak_rss_bytes"]["state"],
            "available",
        )
        serialized = json.dumps(self.report, ensure_ascii=False)
        self.assertNotIn("synthetic adjudicated fixture", serialized)
        self.assertNotIn('"phantom"', serialized)
        self.assertFalse(self.report["contains_transcript_text"])

    def test_calibration_readiness_uses_only_calibration_split_and_claims_no_probability(self) -> None:
        readiness = {row["task"]: row for row in self.report["calibration_readiness"]}
        self.assertEqual(readiness["word_correctness"]["state"], "insufficient_observations")
        self.assertEqual(readiness["speech_presence"]["eligible_observation_count"], 3)
        self.assertFalse(readiness["word_correctness"]["calibration_fitted"])
        self.assertFalse(readiness["speech_presence"]["calibrated_probability_claimed"])

        scoring_scores_removed = copy.deepcopy(self.system_output)
        for recording in scoring_scores_removed["recordings"]:
            if recording["split"] != "scoring":
                continue
            for interval in recording["intervals"]:
                interval["speech_presence_raw_score"] = None
                for segment in interval["segments"]:
                    segment["raw_score"] = None
                    for word in segment["words"]:
                        word["raw_score"] = None
        seal_transcript_system_output(scoring_scores_removed)
        rescored = score_transcript_system(
            candidate_cohort=self.cohort,
            interval_freeze=self.freeze,
            pass_a=self.pass_a,
            pass_b=self.pass_b,
            adjudication=self.adjudication,
            system_output=scoring_scores_removed,
            created_at=self.report["created_at"],
        )
        self.assertEqual(
            rescored["calibration_readiness"], self.report["calibration_readiness"]
        )

    def test_bootstrap_and_full_report_replay_are_byte_deterministic(self) -> None:
        replay = score_transcript_system(
            candidate_cohort=self.cohort,
            interval_freeze=self.freeze,
            pass_a=self.pass_a,
            pass_b=self.pass_b,
            adjudication=self.adjudication,
            system_output=self.system_output,
            created_at=self.report["created_at"],
        )
        self.assertEqual(replay, self.report)

        swapped_passes = score_transcript_system(
            candidate_cohort=self.cohort,
            interval_freeze=self.freeze,
            pass_a=self.pass_b,
            pass_b=self.pass_a,
            adjudication=self.adjudication,
            system_output=self.system_output,
            created_at=self.report["created_at"],
        )
        self.assertEqual(swapped_passes, self.report)

    def test_unfinished_or_incomplete_system_output_fails_closed(self) -> None:
        unfinished = copy.deepcopy(self.system_output)
        unfinished["recordings"][0]["intervals"][0]["state"] = "running"
        seal_transcript_system_output(unfinished)
        with self.assertRaisesRegex(ContractError, "must equal 'completed'"):
            validate_transcript_system_output(unfinished, self.freeze, self.cohort)

        incomplete = copy.deepcopy(self.system_output)
        incomplete["recordings"][0]["intervals"] = []
        seal_transcript_system_output(incomplete)
        with self.assertRaisesRegex(ContractError, "cover every frozen interval"):
            validate_transcript_system_output(incomplete, self.freeze, self.cohort)

    def test_family_leakage_and_text_word_disagreement_fail_closed(self) -> None:
        leaked = copy.deepcopy(self.system_output)
        leaked["recordings"][3]["recording_family_id"] = leaked["recordings"][0]["recording_family_id"]
        seal_transcript_system_output(leaked)
        with self.assertRaisesRegex(ContractError, "cannot cross calibration/scoring"):
            validate_transcript_system_output(leaked, self.freeze, self.cohort)

        mismatch = copy.deepcopy(self.system_output)
        mismatch["recordings"][0]["intervals"][0]["segments"][0]["text"] += " extra"
        seal_transcript_system_output(mismatch)
        with self.assertRaisesRegex(ContractError, "exactly reproduce segment text"):
            validate_transcript_system_output(mismatch, self.freeze, self.cohort)

        missing_resource_profile = copy.deepcopy(self.system_output)
        del missing_resource_profile["system"]["resource_profile"]
        seal_transcript_system_output(missing_resource_profile)
        with self.assertRaisesRegex(ContractError, "missing.*resource_profile"):
            validate_transcript_system_output(
                missing_resource_profile, self.freeze, self.cohort
            )

        oversized_resource = copy.deepcopy(self.system_output)
        oversized_resource["recordings"][0]["resources"]["peak_rss_bytes"] = 2**63
        seal_transcript_system_output(oversized_resource)
        with self.assertRaisesRegex(ContractError, "must be <= 9223372036854775807"):
            validate_transcript_system_output(
                oversized_resource, self.freeze, self.cohort
            )

        oversized_term = copy.deepcopy(self.system_output)
        oversized_term["scoring_profile"]["term_set"]["terms"][0] = "x" * 257
        seal_transcript_system_output(oversized_term)
        with self.assertRaisesRegex(ContractError, "no longer than 256"):
            validate_transcript_system_output(oversized_term, self.freeze, self.cohort)

    def test_nonspeech_symbol_output_counts_as_a_nonempty_segment(self) -> None:
        output = copy.deepcopy(self.system_output)
        interval = output["recordings"][1]["intervals"][0]
        interval["segments"] = [
            {
                "segment_id": "segment_symbol_only_fixture",
                "start_ms": interval["start_ms"],
                "end_ms": interval["end_ms"],
                "text": "…",
                "raw_score": None,
                "words": [],
            }
        ]
        seal_transcript_system_output(output)
        report = score_transcript_system(
            candidate_cohort=self.cohort,
            interval_freeze=self.freeze,
            pass_a=self.pass_a,
            pass_b=self.pass_b,
            adjudication=self.adjudication,
            system_output=output,
            created_at=self.report["created_at"],
        )
        self.assertEqual(
            report["metrics"]["overall"]["nonspeech_hallucination"][
                "nonempty_segment_count"
            ],
            2,
        )

    def test_resource_profile_selects_only_registered_resource_gates(self) -> None:
        cpu_gates = {row["gate_id"]: row for row in self.report["quality_gates"]}
        self.assertEqual(cpu_gates["resource_wall_rtf"]["threshold"], 0.35)
        self.assertEqual(cpu_gates["resource_wall_rtf"]["scope"], "overall")
        self.assertNotEqual(cpu_gates["resource_wall_rtf"]["status"], "not_evaluable")
        self.assertEqual(
            cpu_gates["himr_false_term_insertions"]["status"], "not_evaluable"
        )
        self.assertEqual(
            cpu_gates["repetition_candidate_intervals"]["status"], "not_evaluable"
        )

        other = copy.deepcopy(self.system_output)
        other["system"]["resource_profile"] = "other_measured"
        seal_transcript_system_output(other)
        report = score_transcript_system(
            candidate_cohort=self.cohort,
            interval_freeze=self.freeze,
            pass_a=self.pass_a,
            pass_b=self.pass_b,
            adjudication=self.adjudication,
            system_output=other,
            created_at=self.report["created_at"],
        )
        other_gates = {row["gate_id"]: row for row in report["quality_gates"]}
        self.assertEqual(other_gates["resource_wall_rtf"]["status"], "not_evaluable")
        self.assertEqual(other_gates["cpu_bronze_peak_rss"]["status"], "not_evaluable")

        local_gpu = copy.deepcopy(self.system_output)
        local_gpu["system"]["resource_profile"] = "local_gpu"
        seal_transcript_system_output(local_gpu)
        local_gpu_report = score_transcript_system(
            candidate_cohort=self.cohort,
            interval_freeze=self.freeze,
            pass_a=self.pass_a,
            pass_b=self.pass_b,
            adjudication=self.adjudication,
            system_output=local_gpu,
            created_at=self.report["created_at"],
        )
        local_gpu_gates = {
            row["gate_id"]: row for row in local_gpu_report["quality_gates"]
        }
        self.assertEqual(local_gpu_gates["resource_wall_rtf"]["threshold"], 0.08)
        self.assertNotEqual(
            local_gpu_gates["resource_wall_rtf"]["status"], "not_evaluable"
        )
        self.assertEqual(
            local_gpu_gates["cpu_bronze_peak_rss"]["reason"],
            "gpu_gate_requires_peak_vram_not_process_rss",
        )
        report_schema = json.loads(REPORT_SCHEMA.read_text(encoding="utf-8"))
        self.assertEqual(
            list(Draft202012Validator(report_schema).iter_errors(local_gpu_report)), []
        )

        contextual = copy.deepcopy(self.system_output)
        contextual["system"]["revision_kind"] = "contextual_asr"
        contextual["scoring_profile"]["term_set"]["terms"] = ["fixture", "phantom"]
        seal_transcript_system_output(contextual)
        contextual_report = score_transcript_system(
            candidate_cohort=self.cohort,
            interval_freeze=self.freeze,
            pass_a=self.pass_a,
            pass_b=self.pass_b,
            adjudication=self.adjudication,
            system_output=contextual,
            created_at=self.report["created_at"],
        )
        contextual_gates = {
            row["gate_id"]: row for row in contextual_report["quality_gates"]
        }
        self.assertEqual(contextual_gates["himr_term_recall"]["status"], "not_evaluable")
        self.assertIn("bound_baseline", contextual_gates["himr_term_recall"]["reason"])
        self.assertEqual(
            contextual_gates["himr_false_term_insertions"]["status"], "fail"
        )

    def test_unfinished_reference_and_report_mutation_fail_closed(self) -> None:
        broken_reference = copy.deepcopy(self.adjudication)
        broken_reference["reference_state"] = "draft"
        seal(broken_reference)
        with self.assertRaisesRegex(ContractError, "adjudicated_human_reference"):
            score_transcript_system(
                candidate_cohort=self.cohort,
                interval_freeze=self.freeze,
                pass_a=self.pass_a,
                pass_b=self.pass_b,
                adjudication=broken_reference,
                system_output=self.system_output,
                created_at="2026-08-26T23:30:00Z",
            )

        mutated = copy.deepcopy(self.report)
        mutated["metrics"]["overall"]["word"]["error_count"] += 1
        with self.assertRaisesRegex(ContractError, "canonical digest mismatch"):
            validate_transcript_score_report(
                mutated,
                candidate_cohort=self.cohort,
                interval_freeze=self.freeze,
                pass_a=self.pass_a,
                pass_b=self.pass_b,
                adjudication=self.adjudication,
                system_output=self.system_output,
            )

    def test_cli_scores_and_validates_without_catalog_or_media(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            paths = {}
            for name, value in (
                ("cohort", self.cohort),
                ("freeze", self.freeze),
                ("pass-a", self.pass_a),
                ("pass-b", self.pass_b),
                ("adjudication", self.adjudication),
                ("system-output", self.system_output),
            ):
                path = root / f"{name}.json"
                path.write_text(json.dumps(value), encoding="utf-8")
                paths[name] = path
            command = [
                sys.executable,
                "-m",
                "evaluation",
                "score-transcript-system",
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
                "--system-output",
                str(paths["system-output"]),
                "--created-at",
                self.report["created_at"],
            ]
            sealed = subprocess.run(
                [
                    sys.executable,
                    "-m",
                    "evaluation",
                    "seal-transcript-system-output",
                    "--cohort",
                    str(paths["cohort"]),
                    "--freeze",
                    str(paths["freeze"]),
                    str(paths["system-output"]),
                ],
                cwd=ROOT,
                check=True,
                capture_output=True,
                text=True,
            )
            self.assertEqual(json.loads(sealed.stdout), self.system_output)
            result = subprocess.run(command, cwd=ROOT, check=True, capture_output=True, text=True)
            emitted = json.loads(result.stdout)
            self.assertEqual(emitted, self.report)
            report_path = root / "report.json"
            report_path.write_text(json.dumps(emitted), encoding="utf-8")
            validate = subprocess.run(
                [
                    sys.executable,
                    "-m",
                    "evaluation",
                    "validate-transcript-score-report",
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
                    "--system-output",
                    str(paths["system-output"]),
                    str(report_path),
                ],
                cwd=ROOT,
                check=True,
                capture_output=True,
                text=True,
            )
            self.assertTrue(json.loads(validate.stdout)["valid"])

    def test_tracked_private_system_output_is_rejected(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            repository = Path(directory)
            subprocess.run(["git", "init", "-q", str(repository)], check=True)
            output_path = repository / "system-output.json"
            output_path.write_text(json.dumps(self.system_output), encoding="utf-8")
            subprocess.run(
                ["git", "-C", str(repository), "add", output_path.name], check=True
            )
            with self.assertRaisesRegex(ContractError, "system transcript output"):
                audit_tracked_evaluation_data(repository)

            subprocess.run(
                ["git", "-C", str(repository), "rm", "--cached", output_path.name],
                check=True,
                capture_output=True,
            )
            report_path = repository / "score-report.json"
            report_path.write_text(json.dumps(self.report), encoding="utf-8")
            subprocess.run(
                ["git", "-C", str(repository), "add", report_path.name], check=True
            )
            with self.assertRaisesRegex(ContractError, "transcript score report"):
                audit_tracked_evaluation_data(repository)


if __name__ == "__main__":
    unittest.main()
