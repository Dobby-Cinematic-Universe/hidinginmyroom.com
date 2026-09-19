from __future__ import annotations

import copy
import hashlib
import json
import os
import shutil
import sys
import unittest
from pathlib import Path
from unittest import mock


REPOSITORY_ROOT = Path(__file__).resolve().parents[2]
PIPELINE_ROOT = REPOSITORY_ROOT / "pipeline"
sys.path.insert(0, str(PIPELINE_ROOT))

import shot_local_face_tracker as tracker  # noqa: E402


TEST_ROOT = PIPELINE_ROOT / ".test-shot-local-face-tracker"
RESULT_SCHEMA = PIPELINE_ROOT / "schemas" / "shot-local-face-tracker-result.schema.json"


def digest(path: Path) -> str:
    value = hashlib.sha256()
    with path.open("rb") as handle:
        while chunk := handle.read(1024 * 1024):
            value.update(chunk)
    return value.hexdigest()


def detection(detection_id: str, ordinal: int, x: float, y: float = 80) -> dict:
    return {
        "detection_id": detection_id,
        "detection_ordinal": ordinal,
        "box": {"x": x, "y": y, "width": 80, "height": 100},
        "landmarks": [
            {"x": x + 20, "y": y + 30},
            {"x": x + 55, "y": y + 30},
            {"x": x + 38, "y": y + 50},
            {"x": x + 22, "y": y + 75},
            {"x": x + 53, "y": y + 75},
        ],
        "detector_score": 0.95,
        "below_64px_width": False,
    }


def artifact(
    shots: list[tuple[str, int, int]], detections_by_frame: dict[int, list[dict]]
) -> dict:
    shot_rows = []
    frames = []
    for shot_ordinal, (shot_id, start, end) in enumerate(shots):
        shot_rows.append(
            {
                "shot_id": shot_id,
                "shot_ordinal": shot_ordinal,
                "start_frame_index": start,
                "end_frame_index_exclusive": end,
                "start_timestamp_ms": start * 40,
                "end_timestamp_ms_exclusive": end * 40,
            }
        )
        for frame_index in range(start, end):
            frames.append(
                {
                    "frame_id": f"frame_{frame_index:03d}",
                    "shot_id": shot_id,
                    "frame_index": frame_index,
                    "timestamp_ms": frame_index * 40,
                    "width": 640,
                    "height": 360,
                    "detections": detections_by_frame.get(frame_index, []),
                }
            )
    return {
        "schema_version": 1,
        "artifact_type": "frame_local_face_detections",
        "recording_id": "recording_fixture_tracker_001",
        "coordinate_system": "pixel_xywh_top_left",
        "detector": {
            "name": "fixture_detector",
            "version": "fixture-only",
            "recipe_id": "fixture_recipe_v1",
        },
        "authority": {
            "embeddings_present": False,
            "identity_state": "unknown",
            "identity_label": None,
            "active_speaker_state": "unknown",
            "active_speaker_score": None,
            "publication": False,
        },
        "shots": shot_rows,
        "frames": frames,
    }


def recipe(**overrides: object) -> dict:
    value = {
        "recipe_id": "fixture_tracker_recipe_v1",
        "algorithm": "constant_velocity_hungarian_iou_v1",
        "prediction": "last_two_matched_boxes_per_frame_delta_v1",
        "assignment_objective": "max_cardinality_then_max_total_iou_stable_v1",
        "minimum_assignment_iou": 0.3,
        "max_gap_frames": 1,
        "minimum_confirmed_detections": 2,
        "iou_round_decimals": 8,
    }
    value.update(overrides)
    return value


def make_writable_tree(path: Path) -> None:
    if not path.exists():
        return
    for child in path.rglob("*"):
        if child.is_dir():
            child.chmod(0o700)
        else:
            child.chmod(0o600)
    path.chmod(0o700)


class ShotLocalFaceTrackerTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        TEST_ROOT.mkdir(mode=0o700, parents=True, exist_ok=True)

    @classmethod
    def tearDownClass(cls) -> None:
        make_writable_tree(TEST_ROOT)
        shutil.rmtree(TEST_ROOT, ignore_errors=True)

    def setUp(self) -> None:
        self.case = TEST_ROOT / self._testMethodName
        self.case.mkdir(mode=0o700)

    def tearDown(self) -> None:
        make_writable_tree(self.case)

    def normalized(self, value: dict) -> dict:
        return tracker.validate_detection_artifact(copy.deepcopy(value))

    def test_cut_forces_distinct_tracks_even_with_identical_geometry(self) -> None:
        value = self.normalized(
            artifact(
                [("shot_a", 0, 2), ("shot_b", 2, 4)],
                {
                    0: [detection("d0", 0, 100)],
                    1: [detection("d1", 0, 104)],
                    2: [detection("d2", 0, 108)],
                    3: [detection("d3", 0, 112)],
                },
            )
        )
        frames, tracks = tracker.track_detections(value, recipe(), "job_cut")
        self.assertEqual(len(tracks), 2)
        self.assertEqual([row["shot_id"] for row in tracks], ["shot_a", "shot_b"])
        self.assertNotEqual(tracks[0]["track_id"], tracks[1]["track_id"])
        self.assertEqual(frames[2]["assignments"], [])
        self.assertEqual(len(frames[2]["starts"]), 1)
        self.assertTrue(all(row["end_reason"] == "shot_end" for row in tracks))

    def test_gap_is_recorded_and_track_closes_only_after_tolerance(self) -> None:
        value = self.normalized(
            artifact(
                [("shot_a", 0, 5)],
                {
                    0: [detection("d0", 0, 100)],
                    2: [detection("d2", 0, 100)],
                },
            )
        )
        frames, tracks = tracker.track_detections(value, recipe(), "job_gap")
        self.assertEqual(len(tracks), 1)
        self.assertEqual(frames[1]["gaps"][0]["consecutive_gap_count"], 1)
        self.assertFalse(frames[1]["gaps"][0]["closed"])
        self.assertEqual(frames[2]["assignments"][0]["detection_id"], "d2")
        self.assertEqual(frames[3]["gaps"][0]["consecutive_gap_count"], 1)
        self.assertTrue(frames[4]["gaps"][0]["closed"])
        self.assertEqual(tracks[0]["end_reason"], "max_gap_exceeded")
        self.assertEqual(tracks[0]["total_gap_count"], 3)

    def test_exact_ties_are_deterministic_and_stably_ordered(self) -> None:
        value = self.normalized(
            artifact(
                [("shot_a", 0, 2)],
                {
                    0: [detection("d0a", 0, 100), detection("d0b", 1, 100)],
                    1: [detection("d1a", 0, 100), detection("d1b", 1, 100)],
                },
            )
        )
        first = tracker.track_detections(value, recipe(), "job_tie")
        second = tracker.track_detections(value, recipe(), "job_tie")
        self.assertEqual(first, second)
        rows = first[0][1]["assignments"]
        self.assertEqual([row["detection_id"] for row in rows], ["d1a", "d1b"])
        self.assertEqual([row["assignment_iou"] for row in rows], [1.0, 1.0])

    def test_constant_velocity_keeps_crossing_routes_geometry_local(self) -> None:
        value = self.normalized(
            artifact(
                [("shot_a", 0, 5)],
                {
                    0: [detection("left0", 0, 40), detection("right0", 1, 280)],
                    1: [detection("left1", 0, 80), detection("right1", 1, 240)],
                    2: [detection("left2", 0, 120), detection("right2", 1, 200)],
                    3: [detection("right3", 0, 160), detection("left3", 1, 160)],
                    4: [
                        detection("moving_left4", 0, 120),
                        detection("moving_right4", 1, 200),
                    ],
                },
            )
        )
        frames, tracks = tracker.track_detections(
            value, recipe(minimum_assignment_iou=0.2), "job_crossing"
        )
        self.assertEqual(len(tracks), 2)
        overlap = frames[3]["assignments"]
        self.assertEqual([row["detection_id"] for row in overlap], ["right3", "left3"])
        final = frames[4]["assignments"]
        self.assertEqual(len(final), 2)
        self.assertEqual({row["assignment_iou"] for row in final}, {1.0})
        self.assertEqual(
            [row["detection_id"] for row in final],
            ["moving_right4", "moving_left4"],
        )
        self.assertTrue(all(track["identity_state"] == "unknown" for track in tracks))
        self.assertTrue(all(track["active_speaker_state"] == "unknown" for track in tracks))

    def test_malformed_or_incomplete_shot_bounds_fail_closed(self) -> None:
        value = artifact(
            [("shot_a", 0, 3)],
            {0: [detection("d0", 0, 100)], 1: [detection("d1", 0, 100)]},
        )
        value["frames"].pop()
        with self.assertRaisesRegex(tracker.FaceTrackingError, "must contain every frame"):
            tracker.validate_detection_artifact(value)

        overlapping = artifact(
            [("shot_a", 0, 2), ("shot_b", 2, 4)],
            {0: [], 1: [], 2: [], 3: []},
        )
        overlapping["shots"][1]["start_frame_index"] = 1
        with self.assertRaisesRegex(tracker.FaceTrackingError, "non-overlapping"):
            tracker.validate_detection_artifact(overlapping)

    def test_dimensions_must_be_constant_within_each_shot(self) -> None:
        changed_within_shot = artifact(
            [("shot_a", 0, 2)],
            {0: [], 1: []},
        )
        changed_within_shot["frames"][1]["width"] = 320
        with self.assertRaisesRegex(tracker.FaceTrackingError, "constant within shot"):
            tracker.validate_detection_artifact(changed_within_shot)

        reset_at_cut = artifact(
            [("shot_a", 0, 1), ("shot_b", 1, 2)],
            {0: [], 1: []},
        )
        reset_at_cut["frames"][1]["width"] = 320
        reset_at_cut["frames"][1]["height"] = 240
        normalized = tracker.validate_detection_artifact(reset_at_cut)
        self.assertEqual(
            [(frame["width"], frame["height"]) for frame in normalized["frames"]],
            [(640, 360), (320, 240)],
        )

    def test_extrapolated_extent_is_clamped_and_result_schema_rejects_zero(self) -> None:
        first = detection("d0", 0, 100)
        second = detection("d1", 0, 100)
        second["box"]["width"] = 10
        second["below_64px_width"] = True
        value = self.normalized(
            artifact(
                [("shot_a", 0, 3)],
                {0: [first], 1: [second], 2: []},
            )
        )
        frames, _ = tracker.track_detections(
            value,
            recipe(minimum_assignment_iou=0.01),
            "run_extent_clamp",
        )
        prediction = frames[2]["gaps"][0]["predicted_box"]
        self.assertEqual(prediction["width"], tracker.MIN_PREDICTED_EXTENT)
        self.assertGreater(prediction["height"], 0)

        from jsonschema.validators import Draft202012Validator

        result_schema = json.loads(RESULT_SCHEMA.read_text(encoding="utf-8"))
        predicted_schema = {
            "$schema": "https://json-schema.org/draft/2020-12/schema",
            "$ref": "#/$defs/predictedBox",
            "$defs": result_schema["$defs"],
        }
        validator = Draft202012Validator(predicted_schema)
        self.assertFalse(list(validator.iter_errors(prediction)))
        invalid = {**prediction, "width": 0}
        self.assertTrue(list(validator.iter_errors(invalid)))

        observed_schema = {
            "$schema": "https://json-schema.org/draft/2020-12/schema",
            "$ref": "#/$defs/observedBox",
            "$defs": result_schema["$defs"],
        }
        observed_validator = Draft202012Validator(observed_schema)
        observed = {
            "x": 0,
            "y": 0,
            "width": 80,
            "height": 100,
        }
        self.assertFalse(list(observed_validator.iter_errors(observed)))
        self.assertTrue(
            list(observed_validator.iter_errors({**observed, "x": -0.000001}))
        )
        self.assertTrue(
            list(observed_validator.iter_errors({**observed, "height": 0}))
        )

    def test_raw_iou_controls_threshold_before_rounding(self) -> None:
        threshold = 0.33333333
        raw_target = 0.333333326
        shift = 100 * (1 - raw_target) / (1 + raw_target)
        track = {
            "matches": [
                {
                    "frame_index": 0,
                    "box": {"x": 0.0, "y": 0.0, "width": 100.0, "height": 100.0},
                }
            ]
        }
        candidate = {
            "box": {
                "x": shift,
                "y": 0.0,
                "width": 100.0,
                "height": 100.0,
            }
        }
        raw_iou = tracker.iou(track["matches"][0]["box"], candidate["box"])
        self.assertLess(raw_iou, threshold)
        self.assertEqual(tracker.round_score(raw_iou), threshold)
        assignments, unmatched_tracks, unmatched_detections = tracker.assign_tracks(
            [track], [candidate], 1, threshold
        )
        self.assertEqual(assignments, [])
        self.assertEqual(unmatched_tracks, [0])
        self.assertEqual(unmatched_detections, [0])

    def work_order(self, *, expected_sha256: str | None = None) -> dict:
        detection_path = self.case / "detections.json"
        if not detection_path.exists():
            detection_path.write_text(
                json.dumps(
                    artifact(
                        [("shot_a", 0, 2)],
                        {
                            0: [detection("d0", 0, 100)],
                            1: [detection("d1", 0, 104)],
                        },
                    ),
                    sort_keys=True,
                ),
                encoding="utf-8",
            )
            detection_path.chmod(0o400)
        output = self.case / "output"
        output.mkdir(mode=0o700, exist_ok=True)
        python = Path(sys.executable).resolve(strict=True)
        return {
            "schema_version": 1,
            "job_id": "job_integration_fixture",
            "policy": {
                "visibility": "private",
                "network_allowed": False,
                "embeddings_allowed": False,
                "cross_shot_join_allowed": False,
                "identity_decision_allowed": False,
                "active_speaker_decision_allowed": False,
                "publication_authority": "none",
                "human_review_required": True,
            },
            "runtime": {
                "python": {
                    "path": str(python),
                    "expected_sha256": digest(python),
                    "expected_byte_count": python.stat().st_size,
                    "expected_version": ".".join(str(part) for part in sys.version_info[:3]),
                }
            },
            "input": {
                "detections": {
                    "path": str(detection_path.resolve()),
                    "expected_sha256": expected_sha256 or digest(detection_path),
                    "expected_byte_count": detection_path.stat().st_size,
                }
            },
            "recipe": recipe(),
            "output": {"root": str(output.resolve())},
        }

    def test_hash_mismatch_and_writable_detection_input_fail_closed(self) -> None:
        with self.assertRaisesRegex(tracker.FaceTrackingError, "SHA-256 mismatch"):
            tracker.validate_work_order(self.work_order(expected_sha256="0" * 64))
        order = self.work_order()
        Path(order["input"]["detections"]["path"]).chmod(0o600)
        with self.assertRaisesRegex(tracker.FaceTrackingError, "sealed read-only"):
            tracker.validate_work_order(order)

    def test_atomic_input_replacement_cannot_change_verified_parse(self) -> None:
        raw_order = self.work_order()
        detection_path = Path(raw_order["input"]["detections"]["path"])
        replacement = artifact(
            [("shot_a", 0, 2)],
            {
                0: [detection("replacement_d0", 0, 200)],
                1: [detection("replacement_d1", 0, 204)],
            },
        )
        replacement["recording_id"] = "replacement_recording"
        replacement_path = self.case / "replacement.json"
        replacement_path.write_text(
            json.dumps(replacement, sort_keys=True), encoding="utf-8"
        )
        replacement_path.chmod(0o400)
        original_stable_read = tracker.stable_read
        input_reads = 0

        def adversarial_read(path: Path, label: str, **kwargs: object) -> bytes:
            nonlocal input_reads
            body = original_stable_read(path, label, **kwargs)
            if label == "input.detections":
                input_reads += 1
                if input_reads == 1:
                    os.replace(replacement_path, detection_path)
            return body

        with mock.patch.object(tracker, "stable_read", side_effect=adversarial_read):
            normalized = tracker.validate_work_order(raw_order)
        self.assertEqual(input_reads, 1)
        self.assertEqual(
            normalized["input"]["artifact"]["recording_id"],
            "recording_fixture_tracker_001",
        )
        with self.assertRaisesRegex(tracker.FaceTrackingError, "mismatch"):
            tracker.run(normalized, dry_run=False)

    def test_existing_output_fails_closed_and_result_has_no_authority(self) -> None:
        order = tracker.validate_work_order(self.work_order())
        self.assertEqual(order["runtime"]["python"]["visibility"], "host_runtime")
        self.assertEqual(order["input"]["detections"]["visibility"], "private")
        first = tracker.run(order, dry_run=False)
        result_path = Path(first["result_path"])
        self.assertTrue(result_path.is_file())
        self.assertEqual(result_path.stat().st_mode & 0o777, 0o400)
        self.assertFalse(first["authority"]["identity_decision"])
        self.assertFalse(first["authority"]["active_speaker_decision"])
        self.assertFalse(first["authority"]["publication"])
        self.assertFalse(first["authority"]["cross_shot_join"])
        self.assertEqual(first["runtime"]["python"]["visibility"], "host_runtime")
        with self.assertRaisesRegex(tracker.FaceTrackingError, "already exists"):
            tracker.run(order, dry_run=False)

    def test_symlinked_detection_input_is_rejected(self) -> None:
        order = self.work_order()
        target = Path(order["input"]["detections"]["path"])
        link = self.case / "detections-link.json"
        link.symlink_to(target)
        order["input"]["detections"]["path"] = str(link)
        with self.assertRaisesRegex(tracker.FaceTrackingError, "resolved regular file"):
            tracker.validate_work_order(order)

    def test_symlinked_output_component_is_rejected_before_tracking(self) -> None:
        order = tracker.validate_work_order(self.work_order())
        target = self.case / "outside-target"
        target.mkdir(mode=0o700)
        output_root = Path(order["output"]["root"])
        (output_root / "vision").symlink_to(target, target_is_directory=True)
        with self.assertRaisesRegex(tracker.FaceTrackingError, "resolved directory"):
            tracker.run(order, dry_run=False)
        self.assertEqual(list(target.iterdir()), [])

    def test_staging_seal_failure_leaves_no_final_result(self) -> None:
        order = tracker.validate_work_order(self.work_order())
        implementation = tracker.implementation_observation()
        result_key, final_dir = tracker.result_layout(order, implementation)
        original_chmod = Path.chmod

        def fail_staging_seal(path: Path, mode: int, *args: object, **kwargs: object):
            if (
                mode == 0o500
                and path.name.startswith(f".staging-{result_key}")
            ):
                raise OSError("injected staging seal failure")
            return original_chmod(path, mode, *args, **kwargs)

        with mock.patch.object(Path, "chmod", fail_staging_seal):
            with self.assertRaisesRegex(OSError, "injected staging seal failure"):
                tracker.run(order, dry_run=False)
        self.assertFalse(final_dir.exists())
        self.assertEqual(list(final_dir.parent.glob(".staging-*")), [])

    def test_final_directory_is_sealed_before_rename_returns(self) -> None:
        order = tracker.validate_work_order(self.work_order())
        implementation = tracker.implementation_observation()
        _, final_dir = tracker.result_layout(order, implementation)
        original_chmod = Path.chmod

        def forbid_post_rename_chmod(
            path: Path, mode: int, *args: object, **kwargs: object
        ):
            if path == final_dir:
                raise AssertionError("final directory chmod occurred after rename")
            return original_chmod(path, mode, *args, **kwargs)

        with mock.patch.object(Path, "chmod", forbid_post_rename_chmod):
            result = tracker.run(order, dry_run=False)
        self.assertEqual(Path(result["result_path"]).parent, final_dir)
        self.assertEqual(final_dir.stat().st_mode & 0o777, 0o500)


if __name__ == "__main__":
    unittest.main()
