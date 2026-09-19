from __future__ import annotations

import copy
import hashlib
import importlib.util
import os
import sys
import tempfile
import unittest
from pathlib import Path
from types import ModuleType, SimpleNamespace
from unittest import mock


REPOSITORY_ROOT = Path(__file__).resolve().parents[2]


def load_module(name: str, path: Path) -> ModuleType:
    spec = importlib.util.spec_from_file_location(name, path)
    if spec is None or spec.loader is None:
        raise RuntimeError(f"could not load {path}")
    module = importlib.util.module_from_spec(spec)
    sys.modules[name] = module
    try:
        spec.loader.exec_module(module)
    except Exception:
        sys.modules.pop(name, None)
        raise
    return module


V4 = load_module(
    "himr_gpu_production_v4_test_module",
    REPOSITORY_ROOT / "pipeline/gpu/production_asr_v4.py",
)


class GPUProductionV4Tests(unittest.TestCase):
    def work_order(self, language: str) -> dict[str, object]:
        return {
            "input": {
                "expected_sha256": "0" * 64,
                "expected_duration_ms": 1_000,
                "timeline_offset_ms": 0,
            },
            "inference": {
                "language": language,
                "max_segments": 10,
                "max_words": 10,
                "max_result_bytes": 1_000_000,
            },
            "runtime": {"packages": {"faster-whisper": "1.2.1"}},
            "model": {"identity_sha256": "1" * 64, "revision": "fixture"},
            "catalog_context": None,
        }

    @staticmethod
    def info(probability: float) -> SimpleNamespace:
        return SimpleNamespace(
            language="en",
            language_probability=probability,
            all_language_probs=None,
            duration=1.0,
            duration_after_vad=1.0,
        )

    def test_dependency_pins_bind_the_exact_loaded_bytes(self) -> None:
        loader_path = REPOSITORY_ROOT / "pipeline/gpu/verified_dependency_loader.py"
        v1_path = REPOSITORY_ROOT / "pipeline/gpu/production_asr.py"
        v3_path = REPOSITORY_ROOT / "pipeline/gpu/production_asr_v3.py"
        model_admission_path = REPOSITORY_ROOT / "pipeline/gpu/admit_hf_model.py"
        runtime_admission_path = REPOSITORY_ROOT / "pipeline/gpu/admit_runtime.py"
        wrapper_path = (
            REPOSITORY_ROOT / "pipeline/bin/asr-faster-whisper-gpu-adapter-v4"
        )
        self.assertEqual(
            hashlib.sha256(loader_path.read_bytes()).hexdigest(),
            V4.VERIFIED_LOADER_SOURCE_SHA256,
        )
        self.assertEqual(
            hashlib.sha256(v1_path.read_bytes()).hexdigest(), V4.V1_SOURCE_SHA256
        )
        self.assertEqual(V4._V1_SOURCE.path, v1_path)
        self.assertEqual(V4._V1_SOURCE.sha256, V4.V1_SOURCE_SHA256)
        self.assertEqual(V4._V1_SOURCE.body, v1_path.read_bytes())
        self.assertEqual(
            hashlib.sha256(v3_path.read_bytes()).hexdigest(), V4.V3_SOURCE_SHA256
        )
        self.assertEqual(
            hashlib.sha256(model_admission_path.read_bytes()).hexdigest(),
            V4.MODEL_ADMISSION_SOURCE_SHA256,
        )
        self.assertEqual(
            hashlib.sha256(runtime_admission_path.read_bytes()).hexdigest(),
            V4.RUNTIME_ADMISSION_SOURCE_SHA256,
        )
        self.assertEqual(
            hashlib.sha256(wrapper_path.read_bytes()).hexdigest(),
            V4.V4_WRAPPER_SHA256,
        )

    def test_v4_bootstrap_and_generic_dependency_opens_are_nonblocking(self) -> None:
        retained_names = (
            "himr_gpu_verified_dependency_loader_for_v4",
            "himr_production_asr_v1_verified_for_v4",
        )
        previous = {name: sys.modules.get(name) for name in retained_names}
        test_module_name = "himr_gpu_production_v4_open_flag_test_module"
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary).resolve()
            for filename in (
                "production_asr_v4.py",
                "verified_dependency_loader.py",
                "production_asr.py",
            ):
                (root / filename).write_bytes(
                    (REPOSITORY_ROOT / "pipeline/gpu" / filename).read_bytes()
                )
                (root / filename).chmod(0o644)
            original_open = os.open
            observed: list[tuple[str, int]] = []

            def observing_open(path_value: object, flags: int, *args: object) -> int:
                name = Path(os.fspath(path_value)).name
                if name in {"verified_dependency_loader.py", "production_asr.py"}:
                    observed.append((name, flags))
                return original_open(path_value, flags, *args)

            try:
                with mock.patch.object(os, "open", side_effect=observing_open):
                    load_module(test_module_name, root / "production_asr_v4.py")
            finally:
                sys.modules.pop(test_module_name, None)
                for name, module in previous.items():
                    if module is None:
                        sys.modules.pop(name, None)
                    else:
                        sys.modules[name] = module

        self.assertEqual(
            [name for name, _flags in observed],
            ["verified_dependency_loader.py", "production_asr.py"],
        )
        self.assertTrue(all(flags & os.O_NONBLOCK for _name, flags in observed))

    def test_mutated_local_helpers_are_rejected_before_execution(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary).resolve()
            for filename, pin in V4._LOCAL_GPU_DEPENDENCY_PINS.items():
                with self.subTest(filename=filename):
                    marker = root / f"{filename}.executed"
                    candidate = root / filename
                    candidate.write_text(
                        "from pathlib import Path\n"
                        f"Path({str(marker)!r}).touch()\n",
                        encoding="utf-8",
                    )
                    candidate.chmod(0o644)
                    with (
                        mock.patch.object(
                            V4, "_LOCAL_GPU_DEPENDENCY_DIRECTORY", root
                        ),
                        self.assertRaisesRegex(
                            V4.ProductionASRError, "SHA-256 does not match"
                        ),
                    ):
                        V4._verified_local_gpu_module(
                            pin["module_name"], filename
                        )
                    self.assertFalse(marker.exists())

    def test_local_dependency_dispatcher_is_closed(self) -> None:
        with self.assertRaisesRegex(
            V4.ProductionASRError, "rejected unlisted local GPU dependency"
        ):
            V4._verified_local_gpu_module("unlisted", "unlisted.py")
        with self.assertRaisesRegex(
            V4.ProductionASRError, "rejected unlisted local GPU dependency"
        ):
            V4._verified_local_gpu_module(
                "wrong_name", "admit_hf_model.py"
            )

    def test_runtime_receipt_must_bind_all_v4_dependencies_and_wrapper(self) -> None:
        sources = [
            {
                "requested_path": str(
                    V4._LOCAL_GPU_DEPENDENCY_DIRECTORY
                    / "verified_dependency_loader.py"
                ),
                "sha256": V4.VERIFIED_LOADER_SOURCE_SHA256,
            },
            {"requested_path": str(V4._V1_PATH), "sha256": V4.V1_SOURCE_SHA256},
            {
                "requested_path": str(
                    V4._LOCAL_GPU_DEPENDENCY_DIRECTORY / "admit_hf_model.py"
                ),
                "sha256": V4.MODEL_ADMISSION_SOURCE_SHA256,
            },
            {
                "requested_path": str(
                    V4._LOCAL_GPU_DEPENDENCY_DIRECTORY / "admit_runtime.py"
                ),
                "sha256": V4.RUNTIME_ADMISSION_SOURCE_SHA256,
            },
        ]
        admission = {
            "evidence": {
                "bindings": {
                    "sources": sources,
                    "executables": [
                        {
                            "requested_path": str(V4._V4_WRAPPER_PATH),
                            "sha256": V4.V4_WRAPPER_SHA256,
                        }
                    ],
                }
            }
        }

        V4._require_v4_runtime_dependency_bindings(admission)
        for ordinal in range(len(sources)):
            with self.subTest(missing_source_ordinal=ordinal):
                changed = {
                    "evidence": {
                        "bindings": {
                            "sources": sources[:ordinal] + sources[ordinal + 1 :],
                            "executables": admission["evidence"]["bindings"][
                                "executables"
                            ],
                        }
                    }
                }
                with self.assertRaisesRegex(
                    V4.ProductionASRError, "does not bind exact v4 dependency"
                ):
                    V4._require_v4_runtime_dependency_bindings(changed)

        admission["evidence"]["bindings"]["executables"][0]["sha256"] = "0" * 64
        with self.assertRaisesRegex(
            V4.ProductionASRError, "exact v4 wrapper executable"
        ):
            V4._require_v4_runtime_dependency_bindings(admission)

    def test_v4_contract_is_media_local_language_explicit_and_distinct(self) -> None:
        contract = V4.contract_document()
        self.assertEqual(contract["schema_version"], 4)
        work_order = contract["work_order"]["descriptor"]
        self.assertEqual(
            work_order["verified_dependencies"]["load_policy"],
            "stable_retained_bytes_verified_before_compile_and_exec",
        )
        self.assertEqual(
            work_order["verified_dependencies"]["local_module_allowlist"],
            {
                filename: {
                    "module_name": pin["module_name"],
                    "sha256": pin["sha256"],
                }
                for filename, pin in sorted(V4._LOCAL_GPU_DEPENDENCY_PINS.items())
            },
        )
        self.assertEqual(
            work_order["language_contract"]["forced_language_probability"], None
        )
        self.assertEqual(work_order["language_contract"]["allowed_languages"], ["en"])
        self.assertEqual(
            work_order["coordinate_contract"]["producer_coordinate_system"],
            "media_ms",
        )
        self.assertNotEqual(
            contract["work_order"]["identity_sha256"],
            V4.V3_WORK_ORDER_CONTRACT_SHA256,
        )
        self.assertNotEqual(
            contract["result"]["identity_sha256"],
            V4.V3_RESULT_CONTRACT_SHA256,
        )

    def test_forced_language_is_configuration_with_no_detection_probability(self) -> None:
        work_order = self.work_order("en")
        raw = V4.build_raw_transcript(self.info(1.0), [], work_order)
        self.assertEqual(
            raw["language"],
            {
                "value": "en",
                "selection_basis": V4.FORCED_LANGUAGE_BASIS,
                "detection_performed": False,
                "probability_raw": None,
                "all_probabilities_raw": [],
            },
        )
        normalized = V4.normalize_raw_transcript(raw, work_order)
        self.assertEqual(
            normalized["language"],
            {
                "value": "en",
                "selection_basis": V4.FORCED_LANGUAGE_BASIS,
                "detection_performed": False,
                "raw_probability": None,
                "calibrated_probability": None,
            },
        )
        self.assertEqual(normalized["timeline"]["coordinate_system"], "media_ms")

    def test_v4_preserves_v3_word_timing_anomaly_semantics(self) -> None:
        work_order = self.work_order("en")
        raw = {
            "segments": [
                {
                    "start_seconds": 0.2,
                    "end_seconds": 0.8,
                    "text": "fixture",
                    "words": [
                        {
                            "start_seconds": 0.1,
                            "end_seconds": 0.3,
                            "text": "one",
                            "probability_raw": 0.9,
                        },
                        {
                            "start_seconds": 0.4,
                            "end_seconds": 0.7,
                            "text": "two",
                            "probability_raw": 0.8,
                        },
                        {
                            "start_seconds": 0.35,
                            "end_seconds": 0.9,
                            "text": "three",
                            "probability_raw": 0.7,
                        },
                    ],
                    "temperature_raw": 0.0,
                    "average_log_probability_raw": -0.1,
                    "compression_ratio_raw": 1.0,
                    "no_speech_probability_raw": 0.1,
                }
            ],
            "language": {
                "value": "en",
                "selection_basis": V4.FORCED_LANGUAGE_BASIS,
                "detection_performed": False,
                "probability_raw": None,
                "all_probabilities_raw": [],
            },
        }

        normalized = V4.normalize_raw_transcript(raw, work_order)
        words = normalized["segments"][0]["words"]
        self.assertEqual(
            [(item["start_ms"], item["end_ms"]) for item in words],
            [(100, 300), (400, 700), (350, 900)],
        )
        self.assertTrue(words[0]["timing_anomaly_flags"]["precedes_segment_start"])
        self.assertTrue(
            words[2]["timing_anomaly_flags"]["start_regresses_from_previous"]
        )
        self.assertTrue(words[2]["timing_anomaly_flags"]["overlaps_previous"])
        self.assertTrue(words[2]["timing_anomaly_flags"]["extends_beyond_segment_end"])

    def test_auto_language_is_rejected_until_model_capability_is_admitted(self) -> None:
        work_order = self.work_order("auto")
        with self.assertRaisesRegex(
            V4.ProductionASRError, "requires a forced inference language"
        ):
            V4.build_raw_transcript(self.info(0.73), [], work_order)

    def test_non_admitted_forced_language_is_rejected(self) -> None:
        work_order = self.work_order("fr")
        with self.assertRaisesRegex(
            V4.ProductionASRError, "exact admitted languages"
        ):
            V4.build_raw_transcript(self.info(1.0), [], work_order)

    def test_replay_rejects_forged_forced_language_probability(self) -> None:
        work_order = self.work_order("en")
        raw = V4.build_raw_transcript(self.info(1.0), [], work_order)
        raw["language"]["probability_raw"] = 1.0
        with self.assertRaisesRegex(
            V4.ProductionASRError, "forced language provenance is inconsistent"
        ):
            V4.normalize_raw_transcript(raw, work_order)

    def test_completed_result_language_lineage_is_cross_checked_at_every_layer(self) -> None:
        work_order = self.work_order("en")
        raw = V4.build_raw_transcript(self.info(1.0), [], work_order)
        normalized = V4.normalize_raw_transcript(raw, work_order)
        result = {
            "transcript": {"language": copy.deepcopy(normalized["language"])},
            "inference": {"parameters": copy.deepcopy(work_order["inference"])},
        }

        V4._require_completed_language_lineage(
            result, raw, normalized, work_order
        )
        mutations = (
            ("raw", "probability_raw", 1.0),
            ("normalized", "raw_probability", 1.0),
            ("result", "raw_probability", 1.0),
        )
        for target, key, value in mutations:
            with self.subTest(target=target, key=key):
                changed_result = copy.deepcopy(result)
                changed_raw = copy.deepcopy(raw)
                changed_normalized = copy.deepcopy(normalized)
                if target == "raw":
                    changed_raw["language"][key] = value
                elif target == "normalized":
                    changed_normalized["language"][key] = value
                else:
                    changed_result["transcript"]["language"][key] = value
                with self.assertRaises(V4.ProductionASRError):
                    V4._require_completed_language_lineage(
                        changed_result,
                        changed_raw,
                        changed_normalized,
                        work_order,
                    )

        changed_result = copy.deepcopy(result)
        changed_result["inference"]["parameters"]["language"] = "fr"
        with self.assertRaisesRegex(
            V4.ProductionASRError, "inference lineage is inconsistent"
        ):
            V4._require_completed_language_lineage(
                changed_result, raw, normalized, work_order
            )

    def test_completed_result_validator_wraps_v1_and_rechecks_artifact_bytes(self) -> None:
        work_order = self.work_order("en")
        raw = V4.build_raw_transcript(self.info(1.0), [], work_order)
        normalized = V4.normalize_raw_transcript(raw, work_order)
        raw_body = V4.canonical_bytes(raw)
        normalized_body = V4.canonical_bytes(normalized)
        result = {
            "transcript": {"language": copy.deepcopy(normalized["language"])},
            "inference": {"parameters": copy.deepcopy(work_order["inference"])},
            "artifacts": [
                {
                    "artifact_kind": "faster_whisper_raw_transcript_json",
                    "sha256": V4.sha256_bytes(raw_body),
                },
                {
                    "artifact_kind": "transcript_normalized_json",
                    "sha256": V4.sha256_bytes(normalized_body),
                },
            ],
        }
        plan = {
            "raw_transcript_path": "/not/read/raw.json",
            "normalized_transcript_path": "/not/read/normalized.json",
        }
        with (
            mock.patch.object(
                V4, "_v1_validate_completed_result", return_value=result
            ) as inherited,
            mock.patch.object(
                V4,
                "_read_completed_transcript",
                side_effect=[
                    (raw, raw_body),
                    (normalized, normalized_body),
                ],
            ) as reader,
        ):
            self.assertIs(V4.validate_completed_result(work_order, plan), result)

        inherited.assert_called_once_with(work_order, plan)
        self.assertEqual(reader.call_count, 2)
        self.assertIs(V4._V1.validate_completed_result, V4.validate_completed_result)


if __name__ == "__main__":
    unittest.main()
