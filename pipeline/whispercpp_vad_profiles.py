#!/usr/bin/env python3
"""Exact whisper.cpp/Silero VAD profiles admitted by the private VAD lane."""

from __future__ import annotations

from typing import Any


PROFILE_CONTRACT_VERSION = 1
PROFILE_IMPLEMENTATION_VERSION = "1.0.0"

ENGINE_PROFILE: dict[str, Any] = {
    "profile_id": "whispercpp-vad-speech-segments-v1.8.7-linux-amd64-cpu",
    "expected_sha256": "ca7828ddc277c93daf5f356a52e853f0d4964933c2e1f01652924ec9a4e7d39e",
    "byte_count": 658_912,
    "version_label": "whisper.cpp v1.8.7",
    "version_evidence": "source_revision_plus_executable_sha256",
    "build": {
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
    },
    "stdout_coordinate_unit": "centiseconds",
    "known_cli_defects": [
        "v1.8.7_min_speech_short_flag_and_min_silence_assignment_defect",
    ],
}

MODEL_PROFILE: dict[str, Any] = {
    "profile_id": "silero-vad-v6.2.0-ggml",
    "expected_sha256": "2aa269b785eeb53a82983a20501ddf7c1d9c48e33ab63a41391ac6c9f7fb6987",
    "byte_count": 885_098,
    "model_id": "silero_vad_v6_2_0_ggml",
    "name": "Silero VAD v6.2.0 GGML",
    "revision": "v6.2.0",
    "source": "https://huggingface.co/ggml-org/whisper-vad/blob/main/ggml-silero-v6.2.0.bin",
    "license_label": "MIT",
}


class VADProfileError(ValueError):
    """A requested executable/model is not the reviewed exact profile."""


def _copy(value: dict[str, Any]) -> dict[str, Any]:
    result = dict(value)
    if "build" in result:
        result["build"] = dict(result["build"])
        result["build"]["configuration"] = list(
            result["build"]["configuration"]
        )
    if "known_cli_defects" in result:
        result["known_cli_defects"] = list(result["known_cli_defects"])
    return result


def match_engine_profile(sha256: str, byte_count: int) -> dict[str, Any]:
    if (
        sha256 != ENGINE_PROFILE["expected_sha256"]
        or byte_count != ENGINE_PROFILE["byte_count"]
    ):
        raise VADProfileError(
            "VAD executable is not the exact reviewed whisper.cpp v1.8.7 profile"
        )
    return _copy(ENGINE_PROFILE)


def match_model_profile(sha256: str, byte_count: int) -> dict[str, Any]:
    if (
        sha256 != MODEL_PROFILE["expected_sha256"]
        or byte_count != MODEL_PROFILE["byte_count"]
    ):
        raise VADProfileError(
            "VAD model is not the exact reviewed Silero v6.2.0 GGML profile"
        )
    return _copy(MODEL_PROFILE)
