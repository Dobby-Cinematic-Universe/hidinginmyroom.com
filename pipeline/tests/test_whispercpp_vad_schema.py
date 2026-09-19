from __future__ import annotations

import copy
import json
import unittest
from pathlib import Path

import jsonschema


REPOSITORY_ROOT = Path(__file__).resolve().parents[2]
SCHEMA_PATH = REPOSITORY_ROOT / "pipeline/schemas/whispercpp-vad-result.schema.json"
PRIVATE_PILOT_RESULTS = tuple(
    REPOSITORY_ROOT
    / "research/corpus/vad-pilots/2026-08-28/vad/whispercpp/sha256/74"
    / "746f3eaf084a6155529dd8cba8a943d06da15307080a22027802ffa75c9c1857"
    / "results"
    / result_key
    / "result.json"
    for result_key in (
        "a524f8bb000ea0e27afa9fe348dfaa18d39b98b95512831c9efe3d78eaf9e52b",
        "5ada8da8fd59e3893c5f6ed5e072990b67e63a2472959e214b532c0d55f7efbd",
        "f1bcdb8c10e5ae9bd44462c5564d74997e758497d6ea46aa973ca0fd59283bbb",
    )
)
ENGINE_SHA256 = "ca7828ddc277c93daf5f356a52e853f0d4964933c2e1f01652924ec9a4e7d39e"
MODEL_SHA256 = "2aa269b785eeb53a82983a20501ddf7c1d9c48e33ab63a41391ac6c9f7fb6987"


def file_stat(byte_count: int, inode: int) -> dict:
    return {
        "device": 1,
        "inode": inode,
        "byte_count": byte_count,
        "mtime_ns": 1_787_914_721_021_214_697,
    }


def engine_build() -> dict:
    return {
        "repository": "https://github.com/ggml-org/whisper.cpp",
        "revision": "48f628a84833905ee4a0658ee6d4a5c915ce1997",
        "target": "whisper-vad-speech-segments",
        "configuration": [
            "CMAKE_BUILD_TYPE=Release",
            "CC=/usr/bin/gcc",
            "CXX=/usr/bin/g++",
            "CCACHE_DISABLE=1",
            "GGML_BLAS=OFF",
            "GGML_CUDA=OFF",
            "GGML_NATIVE=ON",
            "GGML_OPENMP=ON",
            "GGML_VULKAN=OFF",
            "WHISPER_FFMPEG=OFF",
        ],
    }


def parameters() -> dict:
    return {
        "segmentation_profile": "whispercpp-v1.8.7-silero-v6.2.0-cli-defaults-v1",
        "parameter_binding": "reviewed_source_defaults_with_broken_cli_fields_omitted_v1",
        "threads": 1,
        "threshold": 0.5,
        "min_speech_duration_ms": 250,
        "min_silence_duration_ms": 100,
        "max_speech_duration_state": "float_max_default",
        "speech_pad_ms": 30,
        "samples_overlap_seconds": 0.1,
        "use_gpu": False,
        "timeout_seconds": 900,
    }


def engine_profile() -> dict:
    return {
        "profile_id": "whispercpp-vad-speech-segments-v1.8.7-linux-amd64-cpu",
        "expected_sha256": ENGINE_SHA256,
        "byte_count": 658_912,
        "version_label": "whisper.cpp v1.8.7",
        "version_evidence": "source_revision_plus_executable_sha256",
        "build": engine_build(),
        "stdout_coordinate_unit": "centiseconds",
        "known_cli_defects": [
            "v1.8.7_min_speech_short_flag_and_min_silence_assignment_defect"
        ],
    }


def model_profile() -> dict:
    return {
        "profile_id": "silero-vad-v6.2.0-ggml",
        "expected_sha256": MODEL_SHA256,
        "byte_count": 885_098,
        "model_id": "silero_vad_v6_2_0_ggml",
        "name": "Silero VAD v6.2.0 GGML",
        "revision": "v6.2.0",
        "source": "https://huggingface.co/ggml-org/whisper-vad/blob/main/ggml-silero-v6.2.0.bin",
        "license_label": "MIT",
    }


def completed_envelope() -> dict:
    input_sha256 = "1" * 64
    logical_command = [
        "/private/tools/whisper-vad-speech-segments",
        "--threads",
        "1",
        "--vad-model",
        "/private/models/ggml-silero-v6.2.0.bin",
        "--file",
        "/private/audio/window.flac",
        "--no-prints",
    ]
    child_command = [
        "/proc/self/fd/4",
        "--threads",
        "1",
        "--vad-model",
        "/proc/self/fd/5",
        "--file",
        "/proc/self/fd/3",
        "--no-prints",
    ]
    return {
        "schema_version": 1,
        "job_id": "vad_schema_test",
        "status": "completed",
        "dry_run": False,
        "work_order_sha256": "2" * 64,
        "recipe_id": "recipe_vad_whispercpp_" + "3" * 32,
        "recipe_sha256": "3" * 64,
        "result_key": "4" * 64,
        "processing_run": {
            "processing_run_id": "run_vad_whispercpp_" + "4" * 32,
            "stage": "vad_whispercpp",
            "implementation_version": "0.3.0",
            "started_at": "2026-08-28T12:00:00Z",
            "completed_at": "2026-08-28T12:00:01Z",
            "status": "completed",
            "duration_ms": 1000,
        },
        "input": {
            "path": "/private/audio/window.flac",
            "sha256": input_sha256,
            "byte_count": 1000,
            "stat_before": file_stat(1000, 10),
            "stat_after": file_stat(1000, 10),
            "unchanged": True,
            "media_id": "media_sha256_" + input_sha256,
            "artifact_id": "artifact_normalized_audio_test",
            "parent_processing_run_id": "run_local_window_test",
            "flac_streaminfo": {
                "container": "flac",
                "sample_rate_hz": 16000,
                "channels": 1,
                "bits_per_sample": 16,
                "total_samples": 160000,
                "duration_ms": 10000,
                "duration_coordinate_basis": "flac_streaminfo_total_samples_nearest_ms",
            },
        },
        "engine": {
            "path": "/private/tools/whisper-vad-speech-segments",
            "sha256": ENGINE_SHA256,
            "byte_count": 658_912,
            "stat_before": file_stat(658_912, 11),
            "stat_after": file_stat(658_912, 11),
            "unchanged": True,
            "profile_id": "whispercpp-vad-speech-segments-v1.8.7-linux-amd64-cpu",
            "version_label": "whisper.cpp v1.8.7",
            "version_evidence": "source_revision_plus_executable_sha256",
            "build": engine_build(),
            "stdout_coordinate_unit": "centiseconds",
            "known_cli_defects": [
                "v1.8.7_min_speech_short_flag_and_min_silence_assignment_defect"
            ],
        },
        "model": {
            "path": "/private/models/ggml-silero-v6.2.0.bin",
            "sha256": MODEL_SHA256,
            "byte_count": 885_098,
            "stat_before": file_stat(885_098, 12),
            "stat_after": file_stat(885_098, 12),
            "unchanged": True,
            "profile_id": "silero-vad-v6.2.0-ggml",
            "model_id": "silero_vad_v6_2_0_ggml",
            "name": "Silero VAD v6.2.0 GGML",
            "revision": "v6.2.0",
            "source": "https://huggingface.co/ggml-org/whisper-vad/blob/main/ggml-silero-v6.2.0.bin",
            "license_label": "MIT",
        },
        "parameters": parameters(),
        "catalog_context": None,
        "timing_contract": {
            "engine_coordinate_unit": "centiseconds",
            "engine_coordinate_to_ms_multiplier": 10,
            "artifact_coordinate_system": "artifact_media_ms",
            "source_coordinate_system": None,
            "maximum_explicit_tail_overrun_ms": 64,
            "tail_policy": "retain_raw_engine_coordinate_and_clip_normalized_end",
        },
        "commands": {
            "logical": logical_command,
            "child_facing": child_command,
            "descriptor_execution_policy": "linux_sealed_memfd_retained_component_io_v3",
            "state": "executed",
        },
        "calibration": {
            "state": "not_calibrated",
            "calibrated_probability": None,
            "score_available": False,
            "threshold_is_not_confidence": True,
        },
        "safety": {
            "visibility": "private",
            "speech_presence_candidates_only": True,
            "speaker_count_authority": "none",
            "speaker_identity_authority": "none",
            "face_identity_authority": "none",
            "active_speaker_authority": "none",
            "action_detection_authority": "none",
            "publication_authority": "none",
            "catalog_write_authority": "none",
            "human_review_required": True,
            "network": "not_used_by_adapter; worker isolation still required",
        },
        "recipe": {
            "contract_version": 1,
            "implementation_version": "0.3.0",
            "stage": "vad_whispercpp",
            "descriptor_execution_policy": "linux_sealed_memfd_retained_component_io_v3",
            "engine_profile": engine_profile(),
            "model_profile": model_profile(),
            "parameters": parameters(),
            "input_audio_contract": {
                "container": "flac",
                "sample_rate_hz": 16000,
                "channels": 1,
                "bits_per_sample": 16,
            },
            "output_contract": "speech-candidate-intervals-private-v1",
        },
        "result_path": "/private/vad/result.json",
        "artifacts": [
            {
                "artifact_kind": "engine_stdout",
                "path": "/private/vad/engine.stdout.txt",
                "sha256": "6" * 64,
                "byte_count": 29,
                "visibility": "private",
            },
            {
                "artifact_kind": "engine_stderr",
                "path": "/private/vad/engine.stderr.txt",
                "sha256": "7" * 64,
                "byte_count": 0,
                "visibility": "private",
            },
        ],
        "segments": [],
        "counts": {
            "speech_segment_count": 0,
            "speech_duration_ms": 0,
            "input_duration_ms": 10000,
            "speech_coverage_ratio": 0.0,
            "tail_clipped_segment_count": 0,
        },
    }


def planned_envelope() -> dict:
    result = completed_envelope()
    result["status"] = "planned"
    result["dry_run"] = True
    result["processing_run"]["status"] = "queued"
    result["commands"]["state"] = "planned"
    result["artifacts"] = []
    result["segments"] = []
    result["counts"] = None
    return result


def failure_envelope() -> dict:
    detail = {"type": "VADError", "message": "synthetic schema failure"}
    return {
        "schema_version": 1,
        "status": "failed",
        "job_id": "vad_schema_test",
        "error": dict(detail),
        "errors": [dict(detail)],
    }


class WhisperCppVADResultSchemaTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        schema = json.loads(SCHEMA_PATH.read_text(encoding="utf-8"))
        jsonschema.Draft202012Validator.check_schema(schema)
        cls.validator = jsonschema.Draft202012Validator(
            schema, format_checker=jsonschema.FormatChecker()
        )

    def assert_rejected(self, value: dict) -> None:
        with self.assertRaises(jsonschema.ValidationError):
            self.validator.validate(value)

    def test_current_planned_completed_and_failure_envelopes_validate(self) -> None:
        for label, envelope in (
            ("planned", planned_envelope()),
            ("completed", completed_envelope()),
            ("failure", failure_envelope()),
        ):
            with self.subTest(label=label):
                self.validator.validate(envelope)

    def test_private_pilots_validate_read_only_when_present(self) -> None:
        present = [path for path in PRIVATE_PILOT_RESULTS if path.is_file()]
        if not present:
            self.skipTest("private VAD pilots are not present")
        for path in present:
            with self.subTest(result_key=path.parent.name):
                self.validator.validate(json.loads(path.read_text(encoding="utf-8")))

    def test_invented_publication_or_identity_authority_is_rejected(self) -> None:
        for key, invented in (
            ("publication_authority", "granted"),
            ("speaker_identity_authority", "Daniel"),
        ):
            with self.subTest(key=key):
                result = completed_envelope()
                result["safety"][key] = invented
                self.assert_rejected(result)

    def test_unknown_top_level_and_nested_fields_are_rejected(self) -> None:
        top_level = completed_envelope()
        top_level["unexpected"] = True
        self.assert_rejected(top_level)

        nested = completed_envelope()
        nested["safety"]["unexpected"] = True
        self.assert_rejected(nested)

    def test_arbitrary_command_is_rejected(self) -> None:
        result = completed_envelope()
        result["commands"]["logical"] = ["/bin/sh", "-c", "publish"]
        self.assert_rejected(result)

    def test_historical_and_current_execution_policies_cannot_be_mixed(self) -> None:
        result = completed_envelope()
        result["commands"][
            "descriptor_execution_policy"
        ] = "linux_proc_self_fd_retained_verified_v1"
        self.assert_rejected(result)

    def test_negative_counts_are_rejected(self) -> None:
        result = completed_envelope()
        result["counts"]["speech_segment_count"] = -1
        self.assert_rejected(result)

    def test_duplicate_artifact_kinds_are_rejected(self) -> None:
        result = completed_envelope()
        result["artifacts"][1] = copy.deepcopy(result["artifacts"][0])
        self.assert_rejected(result)


if __name__ == "__main__":
    unittest.main()
