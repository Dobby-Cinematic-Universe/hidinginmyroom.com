from __future__ import annotations

import copy
import hashlib
import json
import subprocess
import sys
import tempfile
import unittest
from collections import defaultdict
from pathlib import Path

from evaluation.validation import (
    ContractError,
    _expected_rendition_id,
    acquisition_selection,
    canonical_manifest_sha256,
    validate_adjudication,
    validate_annotation,
    validate_candidate_cohort,
    validate_interval_freeze,
)


ROOT = Path(__file__).resolve().parents[2]
COHORT_PATH = ROOT / "evaluation/cohorts/himr-asr-candidate-cohort-v1.json"
EXPECTED_CATALOG_SOURCE_IDS = {
    "h3ySLeBAoXs": "src_2962ce0a452759e995d5e8955efab7bd",
    "0frp1tHu7ek": "src_d221bde343e05a45abe6115ad7a3fd03",
    "s2OW-jRyFrw": "src_fafecfa0685c53bfb7bdda67adcd6e70",
    "92LgEG6NhUw": "src_c46e4b47d218535cb6254778e2017637",
    "SlTpGmCrTxE": "src_9d3fda659b0150b8887e9e496497b876",
    "8AbFGYob9SU": "src_422ea3cd176c5ddab517bce379219390",
    "UUqmpEOc5oc": "src_ca18b8b8b98b50b0bfd793bb5ba73d09",
    "Z32Y-D5kJTg": "src_d321a94f5fc351dc95983411c91c7ae8",
    "94ff_90_Dzs": "src_9c62e5f5db195220b117042802d7c00d",
    "F_G3PXJL2AM": "src_5c2a29a2ef6b5091b591cdbc00116d86",
    "tiMqrpC6ZZY": "src_e51419355b445c58942bbd02fe1088af",
    "fku-kaaUStw": "src_46e0b529015955dbb004370f6462d6ef",
}


def load_cohort() -> dict:
    return json.loads(COHORT_PATH.read_text(encoding="utf-8"))


def seal(value: dict) -> dict:
    value["manifest_sha256"] = canonical_manifest_sha256(value)
    return value


def flags(*, noise: str = "clean") -> dict:
    return {
        "language_tags": ["en"],
        "code_switch": False,
        "speaker_overlap": False,
        "playback_speech": False,
        "noise": noise,
    }


def make_freeze(cohort: dict) -> dict:
    recordings = []
    for index, candidate in enumerate(cohort["candidates"]):
        digest = hashlib.sha256(candidate["native_id"].encode()).hexdigest()
        media_id = f"media_sha256_{digest}"
        kind = "acquired_source_media"
        stratum_id = "stratum_clean_en" if index % 2 == 0 else "stratum_noisy_en"
        interval_flags = flags(noise="clean" if index % 2 == 0 else "moderate")
        recordings.append(
            {
                "candidate_id": candidate["candidate_id"],
                "recording_id": candidate["recording_id"],
                "source_id": candidate["source_id"],
                "source_native_id": candidate["native_id"],
                "source_locator": candidate["public_locator"],
                "media_id": media_id,
                "media_sha256": digest,
                "media_byte_count": 1000 + index,
                "media_duration_ms": 20000,
                "rendition_id": _expected_rendition_id(
                    candidate["recording_id"], media_id, kind
                ),
                "rendition_kind": kind,
                "timeline_coordinate_system": "rendition_media_ms",
                "split": "calibration" if index < 3 else "scoring",
                "intervals": [
                    {
                        "interval_id": f"interval_{index:02d}",
                        "start_ms": 1000,
                        "end_ms": 11000,
                        "stratum_id": stratum_id,
                        "flags": interval_flags,
                    }
                ],
            }
        )
    split_stats = {}
    stratum_stats = defaultdict(lambda: {"recordings": set(), "count": 0, "duration": 0, "flags": None})
    for recording in recordings:
        split = recording["split"]
        stat = split_stats.setdefault(split, {"recordings": 0, "count": 0, "duration": 0})
        stat["recordings"] += 1
        for interval in recording["intervals"]:
            duration = interval["end_ms"] - interval["start_ms"]
            stat["count"] += 1
            stat["duration"] += duration
            stratum = stratum_stats[interval["stratum_id"]]
            stratum["recordings"].add(recording["recording_id"])
            stratum["count"] += 1
            stratum["duration"] += duration
            stratum["flags"] = interval["flags"]
    freeze = {
        "schema_version": 1,
        "manifest_kind": "interval_freeze",
        "manifest_sha256": "0" * 64,
        "freeze_id": "freeze_fixture_v1",
        "cohort_id": cohort["cohort_id"],
        "cohort_manifest_sha256": cohort["manifest_sha256"],
        "created_at": "2026-08-26T21:00:00Z",
        "frozen_at": "2026-08-26T21:00:00Z",
        "selection_attestation": {
            "selected_without_asr_output_inspection": True,
            "attestor_id": "reviewer_selection_01",
            "attested_at": "2026-08-26T21:00:00Z",
            "selection_basis": "source_metadata_and_direct_media_only",
            "protocol_revision": "transcript_eval_protocol_v1",
        },
        "split_policy": {
            "unit": "recording",
            "no_segment_leakage": True,
            "scoring_and_calibration_disjoint": True,
            "stratification_dimensions": [
                "language",
                "code_switch",
                "speaker_overlap",
                "playback_speech",
                "noise",
            ],
        },
        "reference_state": "interval_selection_only_no_reference_text",
        "recordings": recordings,
        "accounting": {
            "recording_count": 12,
            "interval_count": 12,
            "total_duration_ms": 120000,
            "splits": [
                {
                    "split": split,
                    "recording_count": split_stats[split]["recordings"],
                    "interval_count": split_stats[split]["count"],
                    "duration_ms": split_stats[split]["duration"],
                }
                for split in ("calibration", "scoring")
            ],
            "strata": [
                {
                    "stratum_id": name,
                    "flags": stratum_stats[name]["flags"],
                    "recording_count": len(stratum_stats[name]["recordings"]),
                    "interval_count": stratum_stats[name]["count"],
                    "duration_ms": stratum_stats[name]["duration"],
                }
                for name in sorted(stratum_stats)
            ],
        },
    }
    return seal(freeze)


def freeze_lineage(freeze: dict) -> list[dict]:
    output = []
    for recording in freeze["recordings"]:
        for interval in recording["intervals"]:
            output.append(
                {
                    "interval_id": interval["interval_id"],
                    "recording_id": recording["recording_id"],
                    "source_id": recording["source_id"],
                    "media_id": recording["media_id"],
                    "media_sha256": recording["media_sha256"],
                    "rendition_id": recording["rendition_id"],
                    "split": recording["split"],
                    "start_ms": interval["start_ms"],
                    "end_ms": interval["end_ms"],
                }
            )
    return output


def make_annotation(freeze: dict, pass_name: str, annotator_id: str) -> dict:
    suffix = "a" if pass_name == "pass_a" else "b"
    intervals = []
    for index, lineage in enumerate(freeze_lineage(freeze)):
        intervals.append(
            {
                **lineage,
                "annotation_state": "transcribed",
                "utterances": [
                    {
                        "utterance_id": f"utterance_{suffix}_{index:02d}",
                        "start_ms": lineage["start_ms"],
                        "end_ms": lineage["start_ms"] + 5000,
                        "speaker_label": "speaker_local_01",
                        "text": "synthetic reference fixture",
                        "text_state": "verbatim",
                        "flags": flags(),
                    }
                ],
            }
        )
    return seal(
        {
            "schema_version": 1,
            "manifest_kind": "reference_annotation",
            "manifest_sha256": "0" * 64,
            "annotation_id": f"annotation_fixture_{suffix}",
            "freeze_id": freeze["freeze_id"],
            "freeze_manifest_sha256": freeze["manifest_sha256"],
            "pass_name": pass_name,
            "annotator_id": annotator_id,
            "created_at": "2026-08-26T22:00:00Z",
            "independence_attestation": {
                "attestor_id": annotator_id,
                "attested_at": "2026-08-26T22:00:00Z",
                "direct_media_reviewed": True,
                "asr_outputs_inspected": False,
                "other_reference_pass_inspected": False,
                "adjudication_inspected": False,
            },
            "annotation_tool": {"name": "fixture-editor", "version": "1"},
            "publication": {"status": "withheld", "storage_policy": "private_only"},
            "intervals": intervals,
        }
    )


def make_adjudication(freeze: dict, pass_a: dict, pass_b: dict) -> dict:
    intervals = []
    for index, lineage in enumerate(freeze_lineage(freeze)):
        intervals.append(
            {
                **lineage,
                "annotation_state": "transcribed",
                "utterances": [
                    {
                        "utterance_id": f"utterance_final_{index:02d}",
                        "start_ms": lineage["start_ms"],
                        "end_ms": lineage["start_ms"] + 5000,
                        "speaker_label": "speaker_local_01",
                        "text": "synthetic adjudicated fixture",
                        "text_state": "verbatim",
                        "flags": flags(),
                    }
                ],
                "resolution": "resolved_after_review",
                "source_utterance_ids": sorted(
                    [f"utterance_a_{index:02d}", f"utterance_b_{index:02d}"]
                ),
                "decision_note": "Synthetic test only.",
            }
        )
    return seal(
        {
            "schema_version": 1,
            "manifest_kind": "reference_adjudication",
            "manifest_sha256": "0" * 64,
            "adjudication_id": "adjudication_fixture_v1",
            "freeze_id": freeze["freeze_id"],
            "freeze_manifest_sha256": freeze["manifest_sha256"],
            "reference_state": "adjudicated_human_reference",
            "created_at": "2026-08-26T23:00:00Z",
            "inputs": {
                "pass_a": {
                    "annotation_id": pass_a["annotation_id"],
                    "manifest_sha256": pass_a["manifest_sha256"],
                    "annotator_id": pass_a["annotator_id"],
                },
                "pass_b": {
                    "annotation_id": pass_b["annotation_id"],
                    "manifest_sha256": pass_b["manifest_sha256"],
                    "annotator_id": pass_b["annotator_id"],
                },
            },
            "adjudicator": {
                "adjudicator_id": "reviewer_adjudicator_03",
                "adjudicated_at": "2026-08-26T23:00:00Z",
                "direct_media_reviewed": True,
                "compared_both_passes": True,
                "asr_outputs_inspected": False,
                "adjudication_tool": {"name": "fixture-editor", "version": "1"},
            },
            "publication": {"status": "withheld", "storage_policy": "private_only"},
            "intervals": intervals,
        }
    )


class EvaluationContractsTest(unittest.TestCase):
    def setUp(self) -> None:
        self.cohort = load_cohort()
        self.freeze = make_freeze(self.cohort)
        self.pass_a = make_annotation(self.freeze, "pass_a", "reviewer_reference_01")
        self.pass_b = make_annotation(self.freeze, "pass_b", "reviewer_reference_02")
        self.adjudication = make_adjudication(self.freeze, self.pass_a, self.pass_b)

    def test_candidate_cohort_is_valid_and_members_only_is_excluded(self) -> None:
        validate_candidate_cohort(self.cohort)
        self.assertEqual(
            {row["native_id"]: row["source_id"] for row in self.cohort["candidates"]},
            EXPECTED_CATALOG_SOURCE_IDS,
        )
        self.assertEqual(
            {row["source_kind"] for row in self.cohort["candidates"]},
            {"youtube_video"},
        )
        self.assertEqual(
            [row["native_id"] for row in self.cohort["exclusions"]],
            ["rKdcv4QvGig"],
        )

    def test_candidate_source_id_is_derived(self) -> None:
        broken = copy.deepcopy(self.cohort)
        broken["candidates"][0]["source_id"] = "src_" + "0" * 32
        seal(broken)
        with self.assertRaisesRegex(ContractError, "deterministic YouTube source ID"):
            validate_candidate_cohort(broken)

    def test_acquisition_selection_preserves_candidate_order_and_exact_ids(self) -> None:
        selection = acquisition_selection(self.cohort)
        self.assertEqual(
            set(selection),
            {"schema_version", "purpose", "youtube_video_ids", "source_ids", "recording_ids"},
        )
        self.assertEqual(
            selection["youtube_video_ids"],
            [row["native_id"] for row in self.cohort["candidates"]],
        )
        self.assertEqual(selection["source_ids"][0], self.cohort["candidates"][0]["source_id"])
        self.assertEqual(
            selection["recording_ids"][-1], self.cohort["candidates"][-1]["recording_id"]
        )

        broken = copy.deepcopy(self.cohort)
        broken["candidates"][0]["eligibility_state"] = "ineligible"
        seal(broken)
        with self.assertRaisesRegex(ContractError, "cannot enter an acquisition queue"):
            acquisition_selection(broken)

    def test_cli_emits_only_acquisition_selection_json(self) -> None:
        result = subprocess.run(
            [
                sys.executable,
                "-m",
                "evaluation",
                "emit-acquisition-selection",
                str(COHORT_PATH),
            ],
            cwd=ROOT,
            check=True,
            capture_output=True,
            text=True,
        )
        self.assertEqual(result.stderr, "")
        self.assertEqual(
            json.loads(result.stdout),
            acquisition_selection(self.cohort),
        )

    def test_freeze_is_media_bound_stratified_and_disjoint(self) -> None:
        validate_interval_freeze(self.freeze, self.cohort)

    def test_freeze_rejects_asr_informed_selection(self) -> None:
        broken = copy.deepcopy(self.freeze)
        broken["selection_attestation"]["selected_without_asr_output_inspection"] = False
        seal(broken)
        with self.assertRaisesRegex(ContractError, "must equal True"):
            validate_interval_freeze(broken, self.cohort)

    def test_freeze_rejects_media_lineage_mismatch(self) -> None:
        broken = copy.deepcopy(self.freeze)
        broken["recordings"][0]["media_sha256"] = "f" * 64
        seal(broken)
        with self.assertRaisesRegex(ContractError, "media_id"):
            validate_interval_freeze(broken, self.cohort)

    def test_freeze_rejects_interval_overlap(self) -> None:
        broken = copy.deepcopy(self.freeze)
        first = broken["recordings"][0]
        first["intervals"].append(
            {
                **first["intervals"][0],
                "interval_id": "interval_overlap",
                "start_ms": 10000,
                "end_ms": 12000,
            }
        )
        broken["accounting"]["interval_count"] += 1
        broken["accounting"]["total_duration_ms"] += 2000
        broken["accounting"]["splits"][0]["interval_count"] += 1
        broken["accounting"]["splits"][0]["duration_ms"] += 2000
        broken["accounting"]["strata"][0]["interval_count"] += 1
        broken["accounting"]["strata"][0]["duration_ms"] += 2000
        seal(broken)
        with self.assertRaisesRegex(ContractError, "nonoverlapping"):
            validate_interval_freeze(broken, self.cohort)

    def test_freeze_rejects_false_accounting(self) -> None:
        broken = copy.deepcopy(self.freeze)
        broken["accounting"]["total_duration_ms"] += 1
        seal(broken)
        with self.assertRaisesRegex(ContractError, "must equal 120000"):
            validate_interval_freeze(broken, self.cohort)

    def test_independent_annotations_and_adjudication_are_valid(self) -> None:
        validate_annotation(self.pass_a, self.freeze, self.cohort)
        validate_annotation(self.pass_b, self.freeze, self.cohort)
        validate_adjudication(
            self.adjudication, self.freeze, self.cohort, self.pass_a, self.pass_b
        )

    def test_annotation_rejects_inspection_of_other_pass(self) -> None:
        broken = copy.deepcopy(self.pass_a)
        broken["independence_attestation"]["other_reference_pass_inspected"] = True
        seal(broken)
        with self.assertRaisesRegex(ContractError, "must equal False"):
            validate_annotation(broken, self.freeze, self.cohort)

    def test_annotation_rejects_freeze_lineage_change(self) -> None:
        broken = copy.deepcopy(self.pass_a)
        broken["intervals"][0]["start_ms"] += 1
        broken["intervals"][0]["utterances"][0]["start_ms"] += 1
        seal(broken)
        with self.assertRaisesRegex(ContractError, "exact frozen interval lineage"):
            validate_annotation(broken, self.freeze, self.cohort)

    def test_adjudication_requires_distinct_annotators(self) -> None:
        pass_b = copy.deepcopy(self.pass_b)
        pass_b["annotator_id"] = self.pass_a["annotator_id"]
        pass_b["independence_attestation"]["attestor_id"] = self.pass_a["annotator_id"]
        seal(pass_b)
        adjudication = make_adjudication(self.freeze, self.pass_a, pass_b)
        with self.assertRaisesRegex(ContractError, "different annotators"):
            validate_adjudication(
                adjudication, self.freeze, self.cohort, self.pass_a, pass_b
            )

    def test_manifest_digest_detects_mutation(self) -> None:
        broken = copy.deepcopy(self.pass_a)
        broken["intervals"][0]["utterances"][0]["text"] = "mutated"
        with self.assertRaisesRegex(ContractError, "canonical digest mismatch"):
            validate_annotation(broken, self.freeze, self.cohort)

    def test_cli_validates_candidate(self) -> None:
        result = subprocess.run(
            [sys.executable, "-m", "evaluation", "validate-candidate", str(COHORT_PATH)],
            cwd=ROOT,
            check=True,
            capture_output=True,
            text=True,
        )
        self.assertTrue(json.loads(result.stdout)["valid"])


if __name__ == "__main__":
    unittest.main()
