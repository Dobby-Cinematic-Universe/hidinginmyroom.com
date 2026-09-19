from __future__ import annotations

import contextlib
import copy
import io
import json
import sys
import tempfile
import unittest
from pathlib import Path


CORPUS_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(CORPUS_ROOT / "src"))

from himr_corpus.longform_asr_planner import (  # noqa: E402
    LongformPlanningError,
    MANIFEST_KIND,
    PLAN_KIND,
    POLICY_KIND,
    SAMPLE_RATE_HZ,
    SCHEMA_VERSION,
    build_longform_asr_plan,
    canonical_json,
    duration_ms_for_samples,
    load_strict_json,
    validate_longform_asr_plan,
)
from himr_corpus.longform_asr_planner_cli import main  # noqa: E402


def manifest(total_samples: int, candidates: list[dict] | None = None) -> dict:
    return {
        "boundary_candidates": candidates or [],
        "kind": MANIFEST_KIND,
        "recording": {
            "input": {
                "artifact_id": "artifact_normalized_flac_fixture",
                "byte_count": 1234,
                "channels": 1,
                "duration_ms": duration_ms_for_samples(total_samples),
                "path": "/synthetic/normalized.flac",
                "sample_rate_hz": SAMPLE_RATE_HZ,
                "sha256": "a" * 64,
                "total_samples": total_samples,
            },
            "media_id": "media_sha256_aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
            "recording_id": "recording_synthetic_longform",
        },
        "schema_version": SCHEMA_VERSION,
    }


def policy(
    *,
    direct_max: int = 480_000,
    minimum: int = 320_000,
    target: int = 400_000,
    maximum: int = 480_000,
    search: int = 80_000,
    padding: int = 32_000,
    max_spans: int = 100,
) -> dict:
    return {
        "adaptive": {
            "boundary_search_samples": search,
            "max_core_samples": maximum,
            "max_span_count": max_spans,
            "min_core_samples": minimum,
            "padding_samples": padding,
            "target_core_samples": target,
        },
        "direct_max_samples": direct_max,
        "kind": POLICY_KIND,
        "schema_version": SCHEMA_VERSION,
    }


class LongformAsrPlannerTests(unittest.TestCase):
    def test_direct_plan_reads_one_parent_without_padding_or_chunks(self) -> None:
        value = build_longform_asr_plan(manifest(320_000), policy())
        self.assertEqual(value["kind"], PLAN_KIND)
        self.assertEqual(value["strategy"], "direct")
        self.assertRegex(value["plan_id"], r"^lfplan_[0-9a-f]{32}$")
        self.assertEqual(len(value["spans"]), 1)
        span = value["spans"][0]
        self.assertEqual(span["ordinal"], 0)
        self.assertEqual(span["analysis_start_sample"], 0)
        self.assertEqual(span["analysis_end_sample"], 320_000)
        self.assertEqual(span["core_start_sample"], 0)
        self.assertEqual(span["core_end_sample"], 320_000)
        self.assertEqual(span["boundary_reason"], "recording_end")
        self.assertTrue(all(item == 0 for item in span["padding"].values()))
        self.assertEqual(
            value["execution_contract"]["audio_chunk_materialization"],
            "forbidden",
        )
        self.assertEqual(
            value["execution_contract"]["analysis_input"],
            "direct_read_from_single_parent_media",
        )
        self.assertEqual(validate_longform_asr_plan(value), value)

    def test_adaptive_cores_tile_parent_and_analysis_padding_overlaps(self) -> None:
        candidates = [
            {
                "sample": 1_200_000,
                "kind": "silence_midpoint",
                "confidence_millionths": 700_000,
            },
            {
                "sample": 384_000,
                "kind": "vad_transition",
                "confidence_millionths": 800_000,
            },
            {
                "sample": 816_000,
                "kind": "silence_midpoint",
                "confidence_millionths": 750_000,
            },
            {
                "sample": 416_000,
                "kind": "silence_midpoint",
                "confidence_millionths": 900_000,
            },
        ]
        value = build_longform_asr_plan(manifest(1_600_000, candidates), policy())
        self.assertEqual(value["strategy"], "adaptive_spans")
        self.assertEqual(
            [
                (span["core_start_sample"], span["core_end_sample"])
                for span in value["spans"]
            ],
            [
                (0, 416_000),
                (416_000, 816_000),
                (816_000, 1_200_000),
                (1_200_000, 1_600_000),
            ],
        )
        first = value["spans"][0]
        self.assertEqual(first["boundary_reason"], "candidate_silence_midpoint")
        self.assertEqual(first["boundary_confidence_millionths"], 900_000)
        self.assertEqual(first["analysis_start_sample"], 0)
        self.assertEqual(first["analysis_end_sample"], 448_000)
        self.assertEqual(first["padding"]["applied_left_samples"], 0)
        self.assertEqual(first["padding"]["clipped_left_samples"], 32_000)
        coverage = value["coverage"]
        self.assertEqual(coverage["recording_sample_count"], 1_600_000)
        self.assertEqual(coverage["core_sample_count_sum"], 1_600_000)
        self.assertEqual(coverage["core_gap_samples"], 0)
        self.assertEqual(coverage["core_overlap_samples"], 0)
        self.assertEqual(coverage["analysis_unique_sample_count"], 1_600_000)
        self.assertEqual(
            coverage["analysis_overlap_samples"],
            coverage["padding_sample_count_sum"],
        )

    def test_fixed_targets_are_clamped_to_leave_a_minimum_tail(self) -> None:
        value = build_longform_asr_plan(
            manifest(1_050),
            policy(
                direct_max=500,
                minimum=300,
                target=400,
                maximum=500,
                search=10,
                padding=20,
            ),
        )
        self.assertEqual(
            [span["core_end_sample"] for span in value["spans"]],
            [400, 750, 1_050],
        )
        self.assertEqual(
            [span["boundary_reason"] for span in value["spans"]],
            ["fixed_target", "fixed_target", "recording_end"],
        )

    def test_candidate_order_does_not_change_canonical_plan(self) -> None:
        candidates = [
            {
                "sample": 416_000,
                "kind": "silence_midpoint",
                "confidence_millionths": 900_000,
            },
            {
                "sample": 816_000,
                "kind": "silence_midpoint",
                "confidence_millionths": 800_000,
            },
        ]
        forward = build_longform_asr_plan(manifest(1_200_000, candidates), policy())
        reverse = build_longform_asr_plan(
            manifest(1_200_000, list(reversed(candidates))), policy()
        )
        self.assertEqual(canonical_json(forward), canonical_json(reverse))

    def test_tampered_plan_cannot_replay(self) -> None:
        value = build_longform_asr_plan(manifest(1_200_000), policy())
        tampered = copy.deepcopy(value)
        tampered["spans"][0]["core_end_sample"] += 1
        with self.assertRaisesRegex(LongformPlanningError, "deterministic contract"):
            validate_longform_asr_plan(tampered)

    def test_invalid_timeline_and_policy_inputs_fail_closed(self) -> None:
        bad_rate = manifest(320_000)
        bad_rate["recording"]["input"]["sample_rate_hz"] = 48_000
        with self.assertRaisesRegex(LongformPlanningError, "mono 16000 Hz"):
            build_longform_asr_plan(bad_rate, policy())

        bad_duration = manifest(320_000)
        bad_duration["recording"]["input"]["duration_ms"] += 1
        with self.assertRaisesRegex(LongformPlanningError, "inconsistent"):
            build_longform_asr_plan(bad_duration, policy())

        bad_policy = policy(direct_max=400_000, maximum=480_000)
        with self.assertRaisesRegex(LongformPlanningError, "at least"):
            build_longform_asr_plan(manifest(320_000), bad_policy)

        unknown = manifest(320_000)
        unknown["recording"]["input"]["codec"] = "flac"
        with self.assertRaisesRegex(LongformPlanningError, "unknown shape"):
            build_longform_asr_plan(unknown, policy())

    def test_max_span_count_is_an_explicit_bound(self) -> None:
        with self.assertRaisesRegex(LongformPlanningError, "max_span_count"):
            build_longform_asr_plan(
                manifest(2_000_000),
                policy(max_spans=2),
            )

        with self.assertRaisesRegex(LongformPlanningError, "between 1 and 4096"):
            build_longform_asr_plan(
                manifest(2_000_000),
                policy(max_spans=4_097),
            )

    def test_strict_loader_rejects_duplicate_keys(self) -> None:
        with tempfile.TemporaryDirectory(prefix="longform-plan-json-") as temporary:
            path = Path(temporary) / "duplicate.json"
            path.write_text('{"kind":"first","kind":"second"}\n', encoding="utf-8")
            with self.assertRaisesRegex(LongformPlanningError, "duplicate key"):
                load_strict_json(path, "fixture")

    def test_cli_emits_only_a_canonical_plan_file_and_validates_it(self) -> None:
        with tempfile.TemporaryDirectory(prefix="longform-plan-cli-") as temporary:
            root = Path(temporary).resolve()
            manifest_path = root / "manifest.json"
            policy_path = root / "policy.json"
            plan_path = root / "plan.json"
            manifest_path.write_text(
                json.dumps(manifest(320_000)), encoding="utf-8"
            )
            policy_path.write_text(json.dumps(policy()), encoding="utf-8")
            self.assertEqual(
                main(
                    [
                        "build",
                        "--manifest",
                        str(manifest_path),
                        "--policy",
                        str(policy_path),
                        "--output",
                        str(plan_path),
                    ]
                ),
                0,
            )
            plan = json.loads(plan_path.read_text(encoding="utf-8"))
            self.assertEqual(
                plan_path.read_text(encoding="utf-8"), canonical_json(plan) + "\n"
            )
            output = io.StringIO()
            with contextlib.redirect_stdout(output):
                self.assertEqual(main(["validate", "--plan", str(plan_path)]), 0)
            self.assertEqual(output.getvalue(), canonical_json(plan) + "\n")
            self.assertEqual(
                {item.name for item in root.iterdir()},
                {"manifest.json", "policy.json", "plan.json"},
            )


if __name__ == "__main__":
    unittest.main()
