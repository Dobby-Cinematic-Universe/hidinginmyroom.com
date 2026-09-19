from __future__ import annotations

import copy
import hashlib
import json
import os
import shutil
import struct
import sys
import unittest
import zlib
from pathlib import Path
from unittest import mock

from pipeline import shot_local_face_tracker as tracker
from pipeline import yunet_face_detector as detector


REPOSITORY_ROOT = Path(__file__).resolve().parents[2]
PIPELINE_ROOT = REPOSITORY_ROOT / "pipeline"
TEST_ROOT = PIPELINE_ROOT / ".test-yunet-face-detector"


def sha256(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def pin(path: Path) -> dict[str, object]:
    return {
        "path": str(path.resolve()),
        "expected_sha256": sha256(path),
        "expected_byte_count": path.stat().st_size,
    }


def png(width: int, height: int, color: tuple[int, int, int]) -> bytes:
    def chunk(kind: bytes, body: bytes) -> bytes:
        return (
            struct.pack(">I", len(body))
            + kind
            + body
            + struct.pack(">I", zlib.crc32(kind + body) & 0xFFFFFFFF)
        )

    row = b"\x00" + bytes(color) * width
    raw = row * height
    return (
        detector.PNG_SIGNATURE
        + chunk(b"IHDR", struct.pack(">IIBBBBB", width, height, 8, 2, 0, 0, 0))
        + chunk(b"IDAT", zlib.compress(raw, level=9))
        + chunk(b"IEND", b"")
    )


def face_row(x: float, score: float, width: float = 80.0) -> list[float]:
    y = 50.0
    height = 100.0
    return [
        x,
        y,
        width,
        height,
        x + 20,
        y + 30,
        x + 55,
        y + 30,
        x + 38,
        y + 50,
        x + 22,
        y + 75,
        x + 53,
        y + 75,
        score,
    ]


class FakeImage:
    def __init__(self, width: int, height: int):
        self.shape = (height, width, 3)


class FakeDetector:
    def __init__(self, rows: list[object]):
        self.rows = list(rows)
        self.input_sizes: list[tuple[int, int]] = []

    def setInputSize(self, size: tuple[int, int]) -> None:
        self.input_sizes.append(size)

    def detect(self, _image: FakeImage) -> tuple[None, object]:
        return None, self.rows.pop(0)


class FakeNumpy:
    uint8 = object()

    @staticmethod
    def frombuffer(body: bytes, *, dtype: object) -> bytes:
        if dtype is not FakeNumpy.uint8:
            raise AssertionError("unexpected dtype")
        return body


class FakeDnn:
    DNN_BACKEND_OPENCV = 3
    DNN_TARGET_CPU = 0


class FakeCv2:
    IMREAD_COLOR = 1
    dnn = FakeDnn()

    def __init__(self, rows: list[object]):
        self.detector = FakeDetector(rows)

    @staticmethod
    def imdecode(body: bytes, flag: int) -> FakeImage:
        if flag != FakeCv2.IMREAD_COLOR:
            raise AssertionError("unexpected decode flag")
        width, height = struct.unpack(">II", body[16:24])
        return FakeImage(width, height)

    def FaceDetectorYN_create(self, *_args: object) -> FakeDetector:
        return self.detector


def make_writable(path: Path) -> None:
    if not path.exists():
        return
    for child in path.rglob("*"):
        try:
            child.chmod(0o700 if child.is_dir() else 0o600)
        except FileNotFoundError:
            pass
    path.chmod(0o700)


class YuNetFaceDetectorTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        TEST_ROOT.mkdir(mode=0o700, parents=True, exist_ok=True)

    @classmethod
    def tearDownClass(cls) -> None:
        make_writable(TEST_ROOT)
        shutil.rmtree(TEST_ROOT, ignore_errors=True)

    def setUp(self) -> None:
        self.case = TEST_ROOT / self._testMethodName
        self.case.mkdir(mode=0o700)

    def tearDown(self) -> None:
        make_writable(self.case)

    def work_order(
        self,
        *,
        dimensions: list[tuple[int, int]] | None = None,
    ) -> dict:
        assets = self.case / "assets"
        runtime_root = assets / "runtime"
        wheels = assets / "wheels"
        cv2_dir = runtime_root / "cv2"
        numpy_dir = runtime_root / "numpy"
        models = assets / "models"
        licenses = assets / "licenses"
        frames_root = self.case / "frames"
        for directory in (
            wheels,
            cv2_dir,
            numpy_dir,
            models,
            licenses,
            frames_root,
        ):
            directory.mkdir(mode=0o700, parents=True, exist_ok=True)
        files: dict[str, Path] = {}
        for name, path in {
            "opencv_wheel": wheels / "opencv.whl",
            "opencv_binary": cv2_dir / "cv2.so",
            "numpy_wheel": wheels / "numpy.whl",
            "numpy_binary": numpy_dir / "core.so",
            "model": models / "yunet.onnx",
            "license": licenses / "yunet-LICENSE",
        }.items():
            if path.exists():
                path.chmod(0o600)
            path.write_bytes(f"synthetic {name}\n".encode())
            path.chmod(0o400)
            files[name] = path
        dimensions = dimensions or [(320, 240)] * 4
        frame_paths = []
        for index, (width, height) in enumerate(dimensions):
            path = frames_root / f"frame-{index:03d}.png"
            if path.exists():
                path.chmod(0o600)
            path.write_bytes(png(width, height, (index * 20, 30, 60)))
            path.chmod(0o400)
            frame_paths.append(path)
        runtime_root.chmod(0o500)
        output = self.case / "output"
        output.mkdir(mode=0o700, exist_ok=True)
        python = Path(sys.executable).resolve(strict=True)
        return {
            "schema_version": 1,
            "job_id": "yunet_synthetic_test",
            "policy": {
                "visibility": "private",
                "material_scope": "synthetic_fixture_only",
                "network_allowed": False,
                "embeddings_allowed": False,
                "identity_decision_allowed": False,
                "active_speaker_decision_allowed": False,
                "publication_authority": "none",
                "human_review_required": True,
            },
            "runtime": {
                "module_root": str(runtime_root.resolve()),
                "python": {
                    **pin(python),
                    "expected_version": ".".join(
                        str(part) for part in sys.version_info[:3]
                    ),
                },
                "opencv": {
                    "wheel": pin(files["opencv_wheel"]),
                    "binary": pin(files["opencv_binary"]),
                    "expected_version": "4.14.0",
                    "expected_build_information_sha256": "1" * 64,
                },
                "numpy": {
                    "wheel": pin(files["numpy_wheel"]),
                    "binary": pin(files["numpy_binary"]),
                    "expected_version": "2.4.6",
                },
            },
            "model": {
                "artifact": pin(files["model"]),
                "license": pin(files["license"]),
                "upstream_url": "https://github.com/opencv/opencv_zoo/tree/47534e27c9851bb1128ccc0102f1145e27f23f98/models/face_detection_yunet",
                "upstream_commit": "47534e27c9851bb1128ccc0102f1145e27f23f98",
                "license_expression": "MIT",
            },
            "parameters": {
                "recipe_id": detector.RECIPE_ID,
                "score_threshold": 0.9,
                "nms_threshold": 0.3,
                "top_k": 5000,
                "threads": 1,
                "backend": "opencv_cpu",
                "target": "cpu",
                "frame_period_ms": 40,
                "max_frames": 4,
                "max_detections_per_frame": 64,
            },
            "recording_id": "synthetic_recording_001",
            "shots": [
                {
                    "shot_id": "shot_a",
                    "shot_ordinal": 0,
                    "start_frame_index": 0,
                    "end_frame_index_exclusive": 2,
                    "start_timestamp_ms": 0,
                    "end_timestamp_ms_exclusive": 80,
                },
                {
                    "shot_id": "shot_b",
                    "shot_ordinal": 1,
                    "start_frame_index": 2,
                    "end_frame_index_exclusive": 4,
                    "start_timestamp_ms": 80,
                    "end_timestamp_ms_exclusive": 160,
                },
            ],
            "frames": [
                {
                    "frame_id": f"frame_{index:03d}",
                    "shot_id": "shot_a" if index < 2 else "shot_b",
                    "frame_index": index,
                    "timestamp_ms": index * 40,
                    "media_type": "image/png",
                    "input": pin(path),
                }
                for index, path in enumerate(frame_paths)
            ],
            "output": {"root": str(output.resolve())},
        }

    @staticmethod
    def runtime_observation() -> dict:
        return {
            "python_version": ".".join(str(part) for part in sys.version_info[:3]),
            "opencv_version": "4.14.0",
            "numpy_version": "2.4.6",
            "opencv_build_information_sha256": "1" * 64,
            "opencv_threads": 1,
            "opencl_enabled": False,
            "network_used": False,
        }

    def run_with_fake(self, raw_order: dict, rows: list[object]) -> dict:
        order = detector.validate_work_order(copy.deepcopy(raw_order))
        fake_cv2 = FakeCv2(rows)
        with mock.patch.object(
            detector,
            "load_runtime",
            return_value=(fake_cv2, FakeNumpy, self.runtime_observation()),
        ):
            return detector.run(order, dry_run=False)

    def test_zero_and_multiple_detections_feed_tracker_without_identity(self) -> None:
        rows = [
            [face_row(30, 0.95)],
            None,
            [face_row(170, 0.92), face_row(40, 0.99)],
            [face_row(174, 0.94), face_row(44, 0.98)],
        ]
        result = self.run_with_fake(self.work_order(), rows)
        result_path = Path(result["result_path"])
        detections_path = Path(result["detections_artifact"]["path"])
        artifact = json.loads(detections_path.read_text(encoding="utf-8"))
        self.assertEqual(
            [len(frame["detections"]) for frame in artifact["frames"]],
            [1, 0, 2, 2],
        )
        self.assertEqual(
            [row["detector_score"] for row in artifact["frames"][2]["detections"]],
            [0.99, 0.92],
        )
        self.assertEqual(
            tracker.validate_detection_artifact(artifact), artifact
        )
        frames, tracks = tracker.track_detections(
            artifact,
            {
                "recipe_id": "synthetic_detector_tracker_proof_v1",
                "algorithm": "constant_velocity_hungarian_iou_v1",
                "prediction": "last_two_matched_boxes_per_frame_delta_v1",
                "assignment_objective": "max_cardinality_then_max_total_iou_stable_v1",
                "minimum_assignment_iou": 0.3,
                "max_gap_frames": 1,
                "minimum_confirmed_detections": 1,
                "iou_round_decimals": 8,
            },
            "synthetic_detector_tracker_proof",
        )
        self.assertEqual(len(frames), 4)
        self.assertEqual([track["shot_id"] for track in tracks], ["shot_a", "shot_b", "shot_b"])
        self.assertTrue(all(track["identity_state"] == "unknown" for track in tracks))
        self.assertFalse(result["authority"]["embeddings_present"])
        self.assertFalse(result["authority"]["identity_decision"])
        self.assertFalse(result["authority"]["active_speaker_decision"])
        self.assertFalse(result["authority"]["publication"])

        from jsonschema.validators import Draft202012Validator

        for schema_name, instance in (
            ("shot-local-face-detections.schema.json", artifact),
            ("yunet-face-detector-result.schema.json", result),
        ):
            schema = json.loads(
                (PIPELINE_ROOT / "schemas" / schema_name).read_text(encoding="utf-8")
            )
            self.assertEqual(
                list(Draft202012Validator(schema).iter_errors(instance)), []
            )
        self.assertEqual(result_path.stat().st_mode & 0o777, 0o400)
        self.assertEqual(detections_path.stat().st_mode & 0o777, 0o400)
        self.assertEqual(result_path.parent.stat().st_mode & 0o777, 0o500)

    def test_public_pilot_profile_requires_one_shot_and_preserves_no_authority(
        self,
    ) -> None:
        raw = self.work_order()
        raw["job_id"] = "yunet_public_single_shot_test"
        raw["policy"]["material_scope"] = "public_single_shot_pilot_only"
        raw["shots"] = [
            {
                "shot_id": "reviewed_shot",
                "shot_ordinal": 0,
                "start_frame_index": 0,
                "end_frame_index_exclusive": 4,
                "start_timestamp_ms": 0,
                "end_timestamp_ms_exclusive": 160,
            }
        ]
        for frame in raw["frames"]:
            frame["shot_id"] = "reviewed_shot"

        normalized = detector.validate_work_order(copy.deepcopy(raw))
        self.assertEqual(
            normalized["policy"]["material_scope"],
            "public_single_shot_pilot_only",
        )
        result = self.run_with_fake(raw, [None, None, None, None])
        self.assertEqual(
            result["policy"]["material_scope"],
            "public_single_shot_pilot_only",
        )
        self.assertEqual(result["authority"]["identity_state"], "unknown")
        self.assertFalse(result["authority"]["identity_decision"])
        self.assertFalse(result["authority"]["publication"])

        from jsonschema.validators import Draft202012Validator

        work_schema = json.loads(
            (
                PIPELINE_ROOT
                / "schemas"
                / "yunet-face-detector-work-order.schema.json"
            ).read_text(encoding="utf-8")
        )
        result_schema = json.loads(
            (
                PIPELINE_ROOT
                / "schemas"
                / "yunet-face-detector-result.schema.json"
            ).read_text(encoding="utf-8")
        )
        self.assertEqual(
            list(Draft202012Validator(work_schema).iter_errors(raw)), []
        )
        self.assertEqual(
            list(Draft202012Validator(result_schema).iter_errors(result)), []
        )

        two_shots = self.work_order()
        two_shots["policy"]["material_scope"] = "public_single_shot_pilot_only"
        with self.assertRaisesRegex(
            detector.YuNetDetectorError, "exactly one reviewed shot"
        ):
            detector.validate_work_order(two_shots)
        self.assertNotEqual(
            list(Draft202012Validator(work_schema).iter_errors(two_shots)), []
        )

    def test_hash_writable_symlink_hardlink_and_unknown_key_fail_closed(self) -> None:
        base = self.work_order()
        cases: list[tuple[str, dict, str]] = []
        bad_hash = copy.deepcopy(base)
        bad_hash["frames"][0]["input"]["expected_sha256"] = "0" * 64
        cases.append(("hash", bad_hash, "SHA-256 mismatch"))
        unknown = copy.deepcopy(base)
        unknown["frames"][0]["unexpected"] = True
        cases.append(("unknown", unknown, "unknown"))
        for label, value, message in cases:
            with self.subTest(label=label), self.assertRaisesRegex(
                detector.YuNetDetectorError, message
            ):
                detector.validate_work_order(value)

        frame_path = Path(base["frames"][0]["input"]["path"])
        frame_path.chmod(0o600)
        with self.assertRaisesRegex(detector.YuNetDetectorError, "sealed read-only"):
            detector.validate_work_order(base)
        frame_path.chmod(0o400)

        link = self.case / "frame-link.png"
        link.symlink_to(frame_path)
        linked = copy.deepcopy(base)
        linked["frames"][0]["input"]["path"] = str(link)
        with self.assertRaisesRegex(detector.YuNetDetectorError, "resolved regular file"):
            detector.validate_work_order(linked)

        hardlink = self.case / "frame-hardlink.png"
        os.link(frame_path, hardlink)
        with self.assertRaisesRegex(detector.YuNetDetectorError, "one hard link"):
            detector.validate_work_order(base)

    def test_shot_assignment_density_timing_and_dimensions_fail_closed(self) -> None:
        raw = self.work_order()
        overlapping = copy.deepcopy(raw)
        overlapping["shots"][1]["start_frame_index"] = 1
        overlapping["shots"][1]["start_timestamp_ms"] = 40
        with self.assertRaisesRegex(detector.YuNetDetectorError, "non-overlapping"):
            detector.validate_work_order(overlapping)

        boolean_ordinal = copy.deepcopy(raw)
        boolean_ordinal["shots"][0]["shot_ordinal"] = False
        with self.assertRaisesRegex(detector.YuNetDetectorError, "must be an integer"):
            detector.validate_work_order(boolean_ordinal)

        ambiguous = copy.deepcopy(raw)
        ambiguous["frames"][2]["shot_id"] = "shot_a"
        with self.assertRaisesRegex(detector.YuNetDetectorError, "shot assignment"):
            detector.validate_work_order(ambiguous)

        missing = copy.deepcopy(raw)
        missing["frames"].pop(1)
        with self.assertRaisesRegex(detector.YuNetDetectorError, "every dense frame"):
            detector.validate_work_order(missing)

        drift = copy.deepcopy(raw)
        drift["frames"][1]["timestamp_ms"] = 41
        with self.assertRaisesRegex(detector.YuNetDetectorError, "dense 25 fps"):
            detector.validate_work_order(drift)

        changed = self.work_order(dimensions=[(320, 240), (321, 240), (640, 360), (640, 360)])
        with self.assertRaisesRegex(detector.YuNetDetectorError, "constant within shot"):
            detector.validate_work_order(changed)

        reset = self.work_order(dimensions=[(320, 240), (320, 240), (640, 360), (640, 360)])
        normalized = detector.validate_work_order(reset)
        self.assertEqual(
            [(frame["image"]["width"], frame["image"]["height"]) for frame in normalized["frames"]],
            [(320, 240), (320, 240), (640, 360), (640, 360)],
        )

    def test_atomic_frame_and_model_replacements_are_detected_before_inference(self) -> None:
        for target_kind in ("frame", "model"):
            with self.subTest(target=target_kind):
                raw = self.work_order()
                if target_kind == "frame":
                    target = Path(raw["frames"][0]["input"]["path"])
                    watched_label = "frames[0].input"
                    replacement_body = png(320, 240, (250, 0, 0))
                else:
                    target = Path(raw["model"]["artifact"]["path"])
                    watched_label = "model.artifact"
                    replacement_body = b"replacement synthetic model\n"
                replacement = self.case / f"{target_kind}-replacement"
                replacement.write_bytes(replacement_body)
                replacement.chmod(0o400)
                original_read = detector.stable_read
                replaced = False

                def adversarial_read(
                    path: Path, label: str, maximum: int
                ) -> bytes:
                    nonlocal replaced
                    body = original_read(path, label, maximum)
                    if label == watched_label and not replaced:
                        os.replace(replacement, target)
                        replaced = True
                    return body

                with mock.patch.object(
                    detector, "stable_read", side_effect=adversarial_read
                ):
                    normalized = detector.validate_work_order(raw)
                self.assertTrue(replaced)
                with self.assertRaisesRegex(detector.YuNetDetectorError, "mismatch"):
                    detector.run(normalized, dry_run=False)

    def test_existing_output_and_atomic_final_path_race_fail_closed(self) -> None:
        raw = self.work_order()
        rows = [None, None, None, None]
        first = self.run_with_fake(raw, rows)
        order = detector.validate_work_order(raw)
        with self.assertRaisesRegex(detector.YuNetDetectorError, "already exists"):
            detector.run(order, dry_run=False)
        self.assertTrue(Path(first["result_path"]).is_file())

        separate = self.work_order()
        separate["job_id"] = "yunet_synthetic_race"
        order = detector.validate_work_order(separate)
        fake_cv2 = FakeCv2([None, None, None, None])
        original_publish = detector.atomic_publish_directory

        def race(staging: Path, final_dir: Path) -> None:
            final_dir.mkdir(mode=0o500)
            original_publish(staging, final_dir)

        with mock.patch.object(
            detector,
            "load_runtime",
            return_value=(fake_cv2, FakeNumpy, self.runtime_observation()),
        ), mock.patch.object(detector, "atomic_publish_directory", side_effect=race):
            with self.assertRaisesRegex(detector.YuNetDetectorError, "refusing reuse"):
                detector.run(order, dry_run=False)
        final_dir = Path(detector.result_layout(order, detector.implementation_observation())[1])
        self.assertEqual(list(final_dir.iterdir()), [])
        self.assertEqual(list(final_dir.parent.glob(".staging-*")), [])

    def test_output_release_paths_and_symlinked_components_are_rejected(self) -> None:
        raw = self.work_order()
        nested = Path(raw["output"]["root"]) / "private"
        nested.mkdir(mode=0o700)
        raw["output"]["root"] = str(nested)
        with mock.patch.object(detector, "FORBIDDEN_OUTPUT_ROOTS", (self.case,)):
            with self.assertRaisesRegex(detector.YuNetDetectorError, "release or Git"):
                detector.validate_work_order(raw)

        raw = self.work_order()
        order = detector.validate_work_order(raw)
        outside = self.case / "outside"
        outside.mkdir(mode=0o700)
        output = Path(order["output"]["root"])
        (output / "vision").symlink_to(outside, target_is_directory=True)
        with self.assertRaisesRegex(detector.YuNetDetectorError, "resolved directory"):
            detector.run(order, dry_run=False)
        self.assertEqual(list(outside.iterdir()), [])

    def test_invalid_detector_rows_and_decoded_dimensions_fail_closed(self) -> None:
        raw = self.work_order()
        order = detector.validate_work_order(raw)
        invalid_rows = [
            [face_row(-1, 0.95)],
            [face_row(30, float("nan"))],
            [face_row(280, 0.95)],
        ]
        for rows, message in (
            (invalid_rows[0], "invalid face box"),
            (invalid_rows[1], "must be finite"),
            (invalid_rows[2], "exceeds frame bounds"),
        ):
            fake_cv2 = FakeCv2([rows, None, None, None])
            with self.subTest(message=message), self.assertRaisesRegex(
                detector.YuNetDetectorError, message
            ):
                detector.detect_frames(order, fake_cv2, FakeNumpy, fake_cv2.detector)

        class WrongSizeCv2(FakeCv2):
            @staticmethod
            def imdecode(_body: bytes, _flag: int) -> FakeImage:
                return FakeImage(1, 1)

        wrong = WrongSizeCv2([None, None, None, None])
        with self.assertRaisesRegex(detector.YuNetDetectorError, "dimensions differ"):
            detector.detect_frames(order, wrong, FakeNumpy, wrong.detector)

    def test_dry_run_is_deterministic_and_writes_nothing(self) -> None:
        order = detector.validate_work_order(self.work_order())
        first = detector.run(order, dry_run=True)
        second = detector.run(order, dry_run=True)
        self.assertEqual(first, second)
        self.assertFalse((Path(order["output"]["root"]) / "vision").exists())
        self.assertFalse(first["network_allowed"])
        self.assertFalse(first["embeddings_allowed"])
        self.assertEqual(first["publication_authority"], "none")


if __name__ == "__main__":
    unittest.main()
