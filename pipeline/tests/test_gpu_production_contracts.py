from __future__ import annotations

import hashlib
import importlib.util
import copy
import json
import os
import stat
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
    spec.loader.exec_module(module)
    return module


MODEL = load_module(
    "himr_gpu_model_admission_test_module",
    REPOSITORY_ROOT / "pipeline/gpu/admit_hf_model.py",
)
RUNTIME = load_module(
    "himr_gpu_runtime_admission_test_module",
    REPOSITORY_ROOT / "pipeline/gpu/admit_runtime.py",
)
ASR = load_module(
    "himr_gpu_production_asr_test_module",
    REPOSITORY_ROOT / "pipeline/gpu/production_asr.py",
)
ASR_V2 = load_module(
    "himr_gpu_production_asr_v2_test_module",
    REPOSITORY_ROOT / "pipeline/gpu/production_asr_v2.py",
)
ASR_V3 = load_module(
    "himr_gpu_production_asr_v3_test_module",
    REPOSITORY_ROOT / "pipeline/gpu/production_asr_v3.py",
)


class GPUProductionContractTests(unittest.TestCase):
    def test_gpu_device_binding_survives_user_namespace_id_remapping(self) -> None:
        metadata = SimpleNamespace(
            st_mode=stat.S_IFCHR | 0o666,
            st_rdev=os.makedev(195, 0),
            st_uid=65534,
            st_gid=65534,
        )
        with mock.patch.object(Path, "lstat", return_value=metadata):
            binding = RUNTIME.character_device_binding("/dev/nvidia-test")
        self.assertEqual(
            binding,
            {
                "path": "/dev/nvidia-test",
                "major": 195,
                "minor": 0,
                "mode": 0o666,
            },
        )

    def test_verified_audio_descriptor_is_passed_only_to_ffprobe(self) -> None:
        probe_output = {
            "streams": [
                {
                    "codec_name": "flac",
                    "sample_fmt": "s16",
                    "sample_rate": "16000",
                    "channels": 1,
                    "channel_layout": "mono",
                    "duration": "1.000000",
                }
            ],
            "format": {"duration": "1.000000", "format_name": "flac"},
        }
        work_order = {
            "runtime": {"ffprobe": {"path": "/usr/bin/ffprobe"}},
            "input": {
                "path": "/sealed/audio.flac",
                "expected_duration_ms": 1000,
                "media_format": {
                    "container": "flac",
                    "codec": "flac",
                    "sample_rate_hz": 16000,
                    "channels": 1,
                    "sample_format": "s16",
                },
            },
        }
        completed = SimpleNamespace(stdout=json.dumps(probe_output))
        with mock.patch.object(ASR.subprocess, "run", return_value=completed) as run:
            observed, command = ASR.probe_audio(
                work_order, "/proc/self/fd/9", pass_fds=(9,)
            )
        self.assertEqual(observed["duration_ms"], 1000)
        self.assertEqual(command[-1], "/proc/self/fd/9")
        self.assertEqual(run.call_args.kwargs["pass_fds"], (9,))

    def test_model_card_metadata_is_honest_license_evidence(self) -> None:
        MODEL.validate_model_card_license_declaration(
            b"---\nlanguage: en\nlicense: mit\n---\n# Card\n", "mit"
        )
        with self.assertRaises(MODEL.AdmissionError):
            MODEL.validate_model_card_license_declaration(
                b"---\nlicense: apache-2.0\n---\n", "mit"
            )

    def test_wheelhouse_hash_must_be_allowlisted_by_uv_lock(self) -> None:
        digest = hashlib.sha256(b"exact wheel bytes").hexdigest()
        filename = "demo-1.0-py3-none-any.whl"
        wheelhouse = {
            "wheels": [
                {
                    "path": filename,
                    "sha256": digest,
                    "normalized_name": "demo",
                    "version": "1.0",
                }
            ]
        }
        inventory = {
            "packages": [{"normalized_name": "demo", "version": "1.0"}]
        }
        lock = (
            "version = 1\nrevision = 1\nrequires-python = '==3.12.14'\n"
            "[[package]]\nname = 'demo'\nversion = '1.0'\n"
            "wheels = [{ url = 'https://files.pythonhosted.org/" + filename
            + "', hash = 'sha256:" + digest + "', size = 17 }]\n"
        ).encode()
        evidence = RUNTIME.validate_wheelhouse_packages(
            wheelhouse, inventory, lock
        )
        self.assertTrue(evidence["all_wheel_hashes_bound_to_uv_lock"])
        wheelhouse["wheels"][0]["sha256"] = "0" * 64
        with self.assertRaises(RUNTIME.RuntimeAdmissionError):
            RUNTIME.validate_wheelhouse_packages(wheelhouse, inventory, lock)

    def test_producer_native_read_only_audio_mode_is_bound(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            audio = Path(temporary).resolve() / "audio.flac"
            audio.write_bytes(b"not decoded by this contract test")
            audio.chmod(0o444)
            item = {
                "path": str(audio),
                "expected_sha256": hashlib.sha256(audio.read_bytes()).hexdigest(),
                "expected_byte_count": audio.stat().st_size,
                "expected_duration_ms": 1,
                "sealed_mode": "0444",
                "media_id": "media_test",
                "artifact_id": "artifact_test",
                "parent_processing_run_id": "run_test",
                "timeline_offset_ms": 0,
                "media_format": {
                    "container": "flac",
                    "codec": "flac",
                    "sample_rate_hz": 16000,
                    "channels": 1,
                    "sample_format": "s16",
                },
            }
            self.assertEqual(ASR.validate_input(item)["sealed_mode"], "0444")
            item["sealed_mode"] = "0400"
            with self.assertRaises(ASR.ProductionASRError):
                ASR.validate_input(item)

    def test_normative_contract_identities_recompute(self) -> None:
        contracts = ASR.contract_document()
        self.assertEqual(
            contracts["work_order"]["identity_sha256"],
            ASR.sha256_bytes(
                ASR.canonical_bytes(contracts["work_order"]["descriptor"])
            ),
        )
        self.assertEqual(
            contracts["result"]["identity_sha256"],
            ASR.sha256_bytes(
                ASR.canonical_bytes(contracts["result"]["descriptor"])
            ),
        )

    def test_v2_contract_is_media_local_and_distinct_from_v1(self) -> None:
        v1 = ASR.contract_document()
        v2 = ASR_V2.contract_document()
        self.assertEqual(v2["schema_version"], 2)
        self.assertEqual(
            v2["work_order"]["descriptor"]["coordinate_contract"]
            ["producer_coordinate_system"],
            "media_ms",
        )
        self.assertEqual(
            v2["result"]["descriptor"]["coordinate_contract"]
            ["recording_projection"],
            "not_claimed",
        )
        self.assertNotEqual(
            v1["work_order"]["identity_sha256"],
            v2["work_order"]["identity_sha256"],
        )
        self.assertNotEqual(
            v1["result"]["identity_sha256"],
            v2["result"]["identity_sha256"],
        )
        self.assertEqual(
            Path(ASR_V2._V1.__file__).resolve(),
            (REPOSITORY_ROOT / "pipeline/gpu/production_asr_v2.py").resolve(),
        )

    def test_v2_normalization_is_media_local_and_rejects_offset(self) -> None:
        raw = {
            "segments": [
                {
                    "start_seconds": 0.1,
                    "end_seconds": 0.5,
                    "text": "fixture",
                    "words": [
                        {
                            "start_seconds": 0.1,
                            "end_seconds": 0.5,
                            "text": "fixture",
                            "probability_raw": 0.9,
                        }
                    ],
                    "temperature_raw": 0.0,
                    "average_log_probability_raw": -0.1,
                    "compression_ratio_raw": 1.0,
                    "no_speech_probability_raw": 0.1,
                }
            ],
            "language": {"value": "en", "probability_raw": 1.0},
        }
        work_order = {
            "input": {"expected_duration_ms": 1_000, "timeline_offset_ms": 0},
            "inference": {"max_segments": 10, "max_words": 10},
            "catalog_context": None,
        }
        normalized = ASR_V2.normalize_raw_transcript(raw, work_order)
        self.assertEqual(normalized["timeline"]["coordinate_system"], "media_ms")
        segment = normalized["segments"][0]
        self.assertEqual(segment["start_ms"], segment["source_start_ms"])
        self.assertEqual(segment["end_ms"], segment["source_end_ms"])
        self.assertEqual(
            segment["words"][0]["start_ms"],
            segment["words"][0]["source_start_ms"],
        )
        shifted = copy.deepcopy(work_order)
        shifted["input"]["timeline_offset_ms"] = 1
        with self.assertRaises(ASR_V2.ProductionASRError):
            ASR_V2.normalize_raw_transcript(raw, shifted)

    def test_v3_contract_is_null_context_media_local_and_timing_explicit(self) -> None:
        v2 = ASR_V2.contract_document()
        v3 = ASR_V3.contract_document()
        self.assertEqual(v3["schema_version"], 3)
        self.assertEqual(
            v3["work_order"]["descriptor"]["coordinate_contract"],
            {
                "producer_coordinate_system": "media_ms",
                "origin": "normalized_input_artifact_start",
                "timeline_offset_ms": 0,
                "catalog_context": "null_required",
                "recording_projection": "separate_reviewed_operation_required",
            },
        )
        timing = v3["result"]["descriptor"]["word_timing_contract"]
        self.assertEqual(timing["raw_model_word_times"], "preserved")
        self.assertEqual(
            timing["normalized_word_anomaly_flags"],
            list(ASR_V3.WORD_TIMING_FLAG_NAMES),
        )
        self.assertNotEqual(
            v2["work_order"]["identity_sha256"],
            v3["work_order"]["identity_sha256"],
        )
        self.assertNotEqual(
            v2["result"]["identity_sha256"],
            v3["result"]["identity_sha256"],
        )

    def test_v3_preserves_and_flags_non_inverted_word_timing_anomalies(self) -> None:
        raw = {
            "segments": [
                {
                    "start_seconds": 1.0,
                    "end_seconds": 2.0,
                    "text": "fixture",
                    "words": [
                        {
                            "start_seconds": 0.9,
                            "end_seconds": 1.1,
                            "text": "one",
                            "probability_raw": 0.9,
                        },
                        {
                            "start_seconds": 1.2,
                            "end_seconds": 1.5,
                            "text": "two",
                            "probability_raw": 0.8,
                        },
                        {
                            "start_seconds": 1.1,
                            "end_seconds": 1.4,
                            "text": "three",
                            "probability_raw": 0.7,
                        },
                        {
                            "start_seconds": 1.35,
                            "end_seconds": 1.6,
                            "text": "four",
                            "probability_raw": 0.6,
                        },
                        {
                            "start_seconds": 1.9,
                            "end_seconds": 2.1,
                            "text": "five",
                            "probability_raw": 0.5,
                        },
                    ],
                    "temperature_raw": 0.0,
                    "average_log_probability_raw": -0.1,
                    "compression_ratio_raw": 1.0,
                    "no_speech_probability_raw": 0.1,
                }
            ],
            "language": {"value": "en", "probability_raw": 1.0},
        }
        work_order = {
            "input": {"expected_duration_ms": 3_000, "timeline_offset_ms": 0},
            "inference": {"max_segments": 10, "max_words": 10},
            "catalog_context": None,
        }
        normalized = ASR_V3.normalize_raw_transcript(raw, work_order)
        self.assertEqual(normalized["timeline"]["coordinate_system"], "media_ms")
        words = normalized["segments"][0]["words"]
        self.assertEqual(
            [(word["start_ms"], word["end_ms"]) for word in words],
            [(900, 1100), (1200, 1500), (1100, 1400), (1350, 1600), (1900, 2100)],
        )
        self.assertTrue(
            words[0]["timing_anomaly_flags"]["precedes_segment_start"]
        )
        self.assertTrue(
            words[2]["timing_anomaly_flags"]["start_regresses_from_previous"]
        )
        self.assertTrue(words[2]["timing_anomaly_flags"]["overlaps_previous"])
        self.assertTrue(words[3]["timing_anomaly_flags"]["overlaps_previous"])
        self.assertTrue(
            words[4]["timing_anomaly_flags"]["extends_beyond_segment_end"]
        )
        summary = normalized["word_timing_anomalies"]
        self.assertEqual(summary["anomalous_word_count"], 4)
        self.assertEqual(summary["total_flag_count"], 5)
        self.assertEqual(
            summary["flag_counts"],
            {
                "precedes_segment_start": 1,
                "extends_beyond_segment_end": 1,
                "start_regresses_from_previous": 1,
                "overlaps_previous": 2,
            },
        )
        self.assertEqual(summary["notice"], ASR_V3.WORD_TIMING_NOTICE)

    def test_v3_raw_segment_retains_model_word_times(self) -> None:
        segment = SimpleNamespace(
            id=7,
            seek=0,
            start=1.0,
            end=2.0,
            text="fixture",
            tokens=[1, 2],
            temperature=0.0,
            avg_logprob=-0.1,
            compression_ratio=1.0,
            no_speech_prob=0.1,
            words=[
                SimpleNamespace(
                    start=0.9,
                    end=1.1,
                    word="fixture",
                    probability=0.9,
                )
            ],
        )
        observed = ASR_V3.raw_segment(segment, 0)
        self.assertEqual(observed["words"][0]["start_seconds"], 0.9)
        self.assertEqual(observed["words"][0]["end_seconds"], 1.1)

    def test_v3_still_rejects_inverted_or_out_of_bounds_timing(self) -> None:
        work_order = {
            "input": {"expected_duration_ms": 1_000, "timeline_offset_ms": 0},
            "inference": {"max_segments": 10, "max_words": 10},
            "catalog_context": None,
        }
        base = {
            "segments": [
                {
                    "start_seconds": 0.1,
                    "end_seconds": 0.8,
                    "text": "fixture",
                    "words": [
                        {
                            "start_seconds": 0.2,
                            "end_seconds": 0.3,
                            "text": "fixture",
                            "probability_raw": 0.9,
                        }
                    ],
                    "temperature_raw": 0.0,
                    "average_log_probability_raw": -0.1,
                    "compression_ratio_raw": 1.0,
                    "no_speech_probability_raw": 0.1,
                }
            ],
            "language": {"value": "en", "probability_raw": 1.0},
        }
        inverted = copy.deepcopy(base)
        inverted["segments"][0]["words"][0]["start_seconds"] = 0.4
        with self.assertRaises(ASR_V3.ProductionASRError):
            ASR_V3.normalize_raw_transcript(inverted, work_order)

        out_of_bounds = copy.deepcopy(base)
        out_of_bounds["segments"][0]["words"][0]["start_seconds"] = 3.001
        out_of_bounds["segments"][0]["words"][0]["end_seconds"] = 3.002
        with self.assertRaises(ASR_V3.ProductionASRError):
            ASR_V3.normalize_raw_transcript(out_of_bounds, work_order)

        non_monotonic_segments = copy.deepcopy(base)
        non_monotonic_segments["segments"].append(
            copy.deepcopy(non_monotonic_segments["segments"][0])
        )
        non_monotonic_segments["segments"][0]["start_seconds"] = 0.4
        non_monotonic_segments["segments"][0]["end_seconds"] = 0.5
        non_monotonic_segments["segments"][1]["start_seconds"] = 0.3
        non_monotonic_segments["segments"][1]["end_seconds"] = 0.6
        with self.assertRaises(ASR_V3.ProductionASRError):
            ASR_V3.normalize_raw_transcript(non_monotonic_segments, work_order)

    def test_v3_rejects_projection_context_or_offset(self) -> None:
        raw = {
            "segments": [],
            "language": {"value": "en", "probability_raw": 1.0},
        }
        work_order = {
            "input": {"expected_duration_ms": 1_000, "timeline_offset_ms": 0},
            "inference": {"max_segments": 10, "max_words": 10},
            "catalog_context": {"recording_id": "rec_test"},
        }
        with self.assertRaises(ASR_V3.ProductionASRError):
            ASR_V3.normalize_raw_transcript(raw, work_order)
        work_order["catalog_context"] = None
        work_order["input"]["timeline_offset_ms"] = 1
        with self.assertRaises(ASR_V3.ProductionASRError):
            ASR_V3.normalize_raw_transcript(raw, work_order)

    def test_runtime_replay_requires_exact_benchmarked_profile(self) -> None:
        gpu_uuid = "GPU-0b9d7029-b3c6-1a0b-d483-6a3febc3b557"
        adapter_path = "/repo/pipeline/gpu/production_asr.py"
        adapter_sha256 = "a" * 64
        inference = {
            "language": "en",
            "beam_size": 5,
            "best_of": 5,
            "temperature": 0.0,
            "condition_on_previous_text": False,
            "word_timestamps": True,
            "vad_filter": False,
            "cpu_threads": 4,
            "num_workers": 1,
        }
        work_order = {
            "model": {"identity_sha256": "b" * 64},
            "runtime": {
                "root": "/runtime",
                "expected_device": 53,
                "python": {"path": "/python", "expected_sha256": "c" * 64},
                "pyproject": {"path": "/pyproject", "expected_sha256": "d" * 64},
                "lock": {"path": "/lock", "expected_sha256": "e" * 64},
                "runtime_manifest": {
                    "path": "/receipt",
                    "expected_sha256": "f" * 64,
                },
                "ffprobe": {"path": "/ffprobe", "expected_sha256": "1" * 64},
                "adapter": {
                    "path": adapter_path,
                    "expected_sha256": adapter_sha256,
                },
                "packages": {},
                "offline_policy": ASR.OFFLINE_POLICY,
            },
            "gpu": {
                "expected_uuid": gpu_uuid,
                "device_index": 0,
                "compute_type": "float16",
                "lock_path": f"/runtime/locks/{gpu_uuid}.lock",
                "minimum_free_vram_bytes": 2 * 1024**3,
                "maximum_process_vram_bytes": 4 * 1024**3,
            },
            "inference": inference,
        }
        contracts = ASR.contract_document()
        profile = ASR.inference_profile(work_order)
        admission = {
            "receipt_id": "gpurtadmit_test",
            "identity_sha256": "2" * 64,
            "configuration": {
                "runtime_root": "/runtime",
                "python_executable": {"path": "/python"},
                "pyproject": {"path": "/pyproject", "expected_sha256": "d" * 64},
                "lock": {"path": "/lock", "expected_sha256": "e" * 64},
            },
            "evidence": {
                "hardware": {
                    "uuid": gpu_uuid,
                    "device_index": 0,
                    "selected_compute_type": "float16",
                },
                "scheduler_lock": {
                    "lock_path": f"/runtime/locks/{gpu_uuid}.lock",
                    "accepted": True,
                },
                "bindings": {
                    "sources": [
                        {"requested_path": adapter_path, "sha256": adapter_sha256}
                    ]
                },
                "benchmark": {
                    "accepted": True,
                    "benchmark_id": "benchmark_test",
                    "profile": {
                        "name": "small_en_fp16_beam5_v1",
                        "model_identity_sha256": "b" * 64,
                        "work_order_contract_sha256": contracts["work_order"][
                            "identity_sha256"
                        ],
                        "result_contract_sha256": contracts["result"][
                            "identity_sha256"
                        ],
                        "inference": profile,
                        "inference_profile_sha256": ASR.sha256_bytes(
                            ASR.canonical_bytes(profile)
                        ),
                    },
                    "thresholds": {
                        "maximum_peak_process_vram_bytes": 4 * 1024**3,
                        "minimum_vram_reserve_bytes": 1024**3,
                    },
                },
                "wheelhouse": {"tree_sha256": "3" * 64},
                "runtime": {"tree": {"tree_sha256": "4" * 64}},
            },
        }

        def replay(candidate: dict[str, object], receipt: dict[str, object]) -> dict[str, object]:
            module = SimpleNamespace(validate_receipt=lambda *_: receipt)
            with (
                mock.patch.object(ASR, "verify_reference", return_value={}),
                mock.patch.object(ASR, "load_local_gpu_module", return_value=module),
            ):
                return ASR.replay_runtime_binding(candidate, require_current=False)

        accepted = replay(work_order, admission)
        self.assertEqual(
            accepted["admission"]["benchmark_inference_profile_sha256"],
            ASR.sha256_bytes(ASR.canonical_bytes(profile)),
        )

        wrong_profile = copy.deepcopy(admission)
        wrong_profile["evidence"]["benchmark"]["profile"][
            "model_identity_sha256"
        ] = "5" * 64
        with self.assertRaises(ASR.ProductionASRError):
            replay(work_order, wrong_profile)

        excessive_vram = copy.deepcopy(work_order)
        excessive_vram["gpu"]["maximum_process_vram_bytes"] += 1
        with self.assertRaises(ASR.ProductionASRError):
            replay(excessive_vram, admission)


if __name__ == "__main__":
    unittest.main()
