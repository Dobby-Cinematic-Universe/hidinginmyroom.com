from __future__ import annotations

import contextlib
import hashlib
import io
import json
import os
import struct
import sys
import tempfile
import unittest
import zlib
from pathlib import Path

from pipeline import face_candidate_adapter as face


TEST_WORK_ROOT = Path(__file__).resolve().parents[1] / ".test-work"


def digest(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def file_pin(path: Path) -> dict[str, object]:
    return {
        "path": str(path),
        "expected_sha256": digest(path),
        "expected_byte_count": path.stat().st_size,
    }


def png(width: int = 2, height: int = 2) -> bytes:
    def chunk(kind: bytes, body: bytes) -> bytes:
        return (
            struct.pack(">I", len(body))
            + kind
            + body
            + struct.pack(">I", zlib.crc32(kind + body) & 0xFFFFFFFF)
        )

    raw = b"".join(b"\x00" + b"\x00\x00\x00" * width for _ in range(height))
    return (
        face.PNG_SIGNATURE
        + chunk(b"IHDR", struct.pack(">IIBBBBB", width, height, 8, 2, 0, 0, 0))
        + chunk(b"IDAT", zlib.compress(raw))
        + chunk(b"IEND", b"")
    )


class FaceCandidateAdapterTests(unittest.TestCase):
    def test_sealed_frame_path_rejects_writable_file_and_symlink(self) -> None:
        TEST_WORK_ROOT.mkdir(mode=0o700, parents=True, exist_ok=True)
        TEST_WORK_ROOT.chmod(0o700)
        with tempfile.TemporaryDirectory(dir=TEST_WORK_ROOT) as temporary:
            root = Path(temporary).resolve()
            frame = root / "frame.png"
            frame.write_bytes(png())
            frame.chmod(0o600)
            with self.assertRaisesRegex(face.FaceCandidateError, "sealed read-only"):
                face.resolved_regular_file(str(frame), "frame")

            frame.chmod(0o400)
            alias = root / "frame-alias.png"
            alias.symlink_to(frame)
            with self.assertRaisesRegex(face.FaceCandidateError, "resolved regular file"):
                face.resolved_regular_file(str(alias), "frame")

    def test_cosine_is_raw_bounded_signal(self) -> None:
        self.assertEqual(face.cosine_similarity([1.0, 0.0], [1.0, 0.0]), 1.0)
        self.assertEqual(face.cosine_similarity([1.0, 0.0], [0.0, 1.0]), 0.0)
        self.assertEqual(face.cosine_similarity([1.0, 0.0], [-1.0, 0.0]), -1.0)
        with self.assertRaisesRegex(face.FaceCandidateError, "norm"):
            face.cosine_similarity([0.0, 0.0], [1.0, 0.0])

    def test_comparison_never_forwards_source_context_label(self) -> None:
        order = {
            "comparisons": [
                {
                    "comparison_id": "pair-1",
                    "left_frame_id": "left",
                    "right_frame_id": "right",
                    "candidate_coordinate_mapping": {
                        "left_source_timestamp_ms": 1000,
                        "right_artifact_timestamp_ms": 200,
                        "basis": "separate candidate mapping",
                        "recording_relationship_decision": False,
                    },
                }
            ]
        }
        frames = [
            {
                "frame_id": "left",
                "source_context": {"source_context_label": "Context label only"},
                "detections": [{"detection_id": "d-left"}],
            },
            {
                "frame_id": "right",
                "source_context": {"source_context_label": None},
                "detections": [{"detection_id": "d-right"}],
            },
        ]
        result = face.compare_frames(
            order, frames, {"d-left": [1.0, 0.0], "d-right": [1.0, 0.0]}
        )[0]
        self.assertEqual(result["raw_cosine_similarity"], 1.0)
        self.assertIsNone(result["calibrated_probability"])
        self.assertIsNone(result["identity_label"])
        self.assertFalse(result["identity_decision"])
        self.assertFalse(result["source_context_bridge"]["label_forwarded_by_model"])

    def test_comparison_abstains_when_detection_cardinality_is_not_one(self) -> None:
        order = {
            "comparisons": [
                {
                    "comparison_id": "pair-1",
                    "left_frame_id": "left",
                    "right_frame_id": "right",
                    "candidate_coordinate_mapping": {
                        "left_source_timestamp_ms": 1000,
                        "right_artifact_timestamp_ms": 200,
                        "basis": "separate candidate mapping",
                        "recording_relationship_decision": False,
                    },
                }
            ]
        }
        frames = [
            {
                "frame_id": "left",
                "source_context": {"source_context_label": "Context label only"},
                "detections": [],
            },
            {
                "frame_id": "right",
                "source_context": {"source_context_label": None},
                "detections": [{"detection_id": "d-right"}],
            },
        ]
        result = face.compare_frames(order, frames, {})[0]
        self.assertEqual(result["state"], "abstained_detection_cardinality")
        self.assertIsNone(result["raw_cosine_similarity"])
        self.assertIsNone(result["left_detection_id"])

    def test_query_context_cannot_arrive_with_identity_label(self) -> None:
        context = {
            "context_role": "comparison_query",
            "source_native_id": "query-1",
            "source_url": "https://example.invalid/query-1",
            "source_context_label": "Name",
            "basis": "test",
            "human_identity_attestation": False,
        }
        with self.assertRaisesRegex(face.FaceCandidateError, "must be null"):
            face.validate_context(context, "context")

    def test_dry_run_is_offline_and_writes_no_biometric_artifact(self) -> None:
        TEST_WORK_ROOT.mkdir(mode=0o700, parents=True, exist_ok=True)
        TEST_WORK_ROOT.chmod(0o700)
        with tempfile.TemporaryDirectory(dir=TEST_WORK_ROOT) as temporary:
            root = Path(temporary).resolve()
            asset = root / "asset"
            runtime = asset / "runtime"
            runtime.mkdir(parents=True)
            files: dict[str, Path] = {}
            for name in ("opencv.whl", "numpy.whl", "cv2.so", "numpy.so", "yunet.onnx", "sface.onnx", "yunet.LICENSE", "sface.LICENSE"):
                path = runtime / name if name in {"cv2.so", "numpy.so"} else asset / name
                path.write_bytes((name + "\n").encode())
                path.chmod(0o444)
                files[name] = path
            runtime.chmod(0o555)
            left = root / "left.png"
            right = root / "right.png"
            left.write_bytes(png())
            right.write_bytes(png())
            left.chmod(0o444)
            right.chmod(0o444)
            output = root / "output"
            output.mkdir()
            python_path = Path(sys.executable).resolve()
            policy = {
                "visibility": "private",
                "network_allowed": False,
                "publication_authority": "none",
                "identity_decision_allowed": False,
                "human_review_required": True,
                "calibration_state": "not_calibrated",
                "calibrated_probability": None,
                "source_context_is_identity": False,
            }
            order = {
                "schema_version": 1,
                "job_id": "test-face-dry-run",
                "policy": policy,
                "runtime": {
                    "module_root": str(runtime),
                    "python": {
                        "executable": str(python_path),
                        "expected_sha256": digest(python_path),
                        "expected_byte_count": python_path.stat().st_size,
                        "expected_version": ".".join(str(v) for v in sys.version_info[:3]),
                    },
                    "opencv": {
                        "wheel": file_pin(files["opencv.whl"]),
                        "binary": file_pin(files["cv2.so"]),
                        "expected_version": "4.14.0",
                        "expected_build_information_sha256": "1" * 64,
                    },
                    "numpy": {
                        "wheel": file_pin(files["numpy.whl"]),
                        "binary": file_pin(files["numpy.so"]),
                        "expected_version": "2.4.6",
                    },
                },
                "models": {
                    "yunet": {
                        "artifact": file_pin(files["yunet.onnx"]),
                        "license": file_pin(files["yunet.LICENSE"]),
                        "upstream_url": "https://github.com/opencv/opencv_zoo/tree/47534e27c9851bb1128ccc0102f1145e27f23f98/models/face_detection_yunet",
                        "upstream_commit": "47534e27c9851bb1128ccc0102f1145e27f23f98",
                        "license_expression": "MIT",
                    },
                    "sface": {
                        "artifact": file_pin(files["sface.onnx"]),
                        "license": file_pin(files["sface.LICENSE"]),
                        "upstream_url": "https://github.com/opencv/opencv_zoo/tree/47534e27c9851bb1128ccc0102f1145e27f23f98/models/face_recognition_sface",
                        "upstream_commit": "47534e27c9851bb1128ccc0102f1145e27f23f98",
                        "license_expression": "Apache-2.0",
                    },
                },
                "parameters": {
                    "score_threshold": 0.9,
                    "nms_threshold": 0.3,
                    "top_k": 5000,
                    "threads": 1,
                    "max_frames": 2,
                    "max_detections_per_frame": 2,
                    "comparison_metric": "cosine_similarity_raw_v1",
                    "embedding_format": "float32_little_endian_v1",
                },
                "frames": [],
                "comparisons": [
                    {
                        "comparison_id": "pair-1",
                        "left_frame_id": "left",
                        "right_frame_id": "right",
                        "candidate_coordinate_mapping": {
                            "left_source_timestamp_ms": 1000,
                            "right_artifact_timestamp_ms": 200,
                            "basis": "separate candidate mapping",
                            "recording_relationship_decision": False,
                        },
                    }
                ],
                "output": {"root": str(output)},
            }
            for frame_id, path, role, label in (
                ("left", left, "source_context_anchor", "Context label only"),
                ("right", right, "comparison_query", None),
            ):
                order["frames"].append(
                    {
                        "frame_id": frame_id,
                        "path": str(path),
                        "expected_sha256": digest(path),
                        "expected_byte_count": path.stat().st_size,
                        "requested_timestamp_ms": 200,
                        "observed_timestamp_ms": 200,
                        "timestamp_drift_ms": 0,
                        "coordinate_system": "fixture-ms",
                        "source_context": {
                            "context_role": role,
                            "source_native_id": frame_id,
                            "source_url": f"https://example.invalid/{frame_id}",
                            "source_context_label": label,
                            "basis": "test context only",
                            "human_identity_attestation": False,
                        },
                    }
                )
            order_path = root / "order.json"
            order_path.write_text(json.dumps(order), encoding="utf-8")
            stdout = io.StringIO()
            try:
                with contextlib.redirect_stdout(stdout):
                    status = face.main(
                        ["run", "--work-order", str(order_path), "--dry-run"]
                    )
                self.assertEqual(status, 0, stdout.getvalue())
                result = json.loads(stdout.getvalue())
                self.assertEqual(result["status"], "planned")
                self.assertFalse(result["network_allowed"])
                self.assertFalse(result["identity_decision_allowed"])
                self.assertFalse((output / "vision").exists())
            finally:
                runtime.chmod(0o755)


if __name__ == "__main__":
    unittest.main()
