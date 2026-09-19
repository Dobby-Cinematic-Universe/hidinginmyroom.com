#!/usr/bin/env python3
"""Exact local whisper.cpp engine profiles shared by private ASR lanes."""

from __future__ import annotations

from typing import Any


PROFILE_CONTRACT_VERSION = 1
PROFILE_IMPLEMENTATION_VERSION = "1.0.0"

_CPU_BUILD_CONFIGURATION = (
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
)


ENGINE_PROFILES: tuple[dict[str, Any], ...] = (
    {
        "profile_id": "whispercpp-cli-v1.8.3-linux-amd64-cpu",
        "admission": "legacy_manifest_replay_only",
        "expected_sha256": "4831024debb4e60e9433d27967ba6dae033d4b0c770c4ef208c16fc5a8fe77d6",
        "byte_count": 1_010_544,
        "version_label": "whisper.cpp v1.8.3",
        "version_evidence": "source_revision_plus_executable_sha256",
        "build": {
            "repository": "https://github.com/ggml-org/whisper.cpp",
            "revision": "2eeeba56e9edd762b4b38467bab96c2517163158",
            "target": "whisper-cli",
            "configuration": list(_CPU_BUILD_CONFIGURATION),
        },
        "output_json_full_utf8_token_boundary_merge": False,
    },
    {
        "profile_id": "whispercpp-cli-v1.8.7-linux-amd64-cpu",
        "admission": "current_new_batch",
        "expected_sha256": "36be94accd60116933073e8069964c02e888e3681967184ce7fecfd5f980ae1a",
        "byte_count": 1_020_432,
        "version_label": "whisper.cpp v1.8.7",
        "version_evidence": "source_revision_plus_executable_sha256",
        "build": {
            "repository": "https://github.com/ggml-org/whisper.cpp",
            "revision": "48f628a84833905ee4a0658ee6d4a5c915ce1997",
            "target": "whisper-cli",
            "configuration": list(_CPU_BUILD_CONFIGURATION),
        },
        "output_json_full_utf8_token_boundary_merge": True,
    },
)


class EngineProfileError(ValueError):
    """An executable observation is absent from or ineligible in the allowlist."""


def match_engine_profile(
    sha256: str,
    byte_count: int,
    *,
    allow_legacy_manifest_replay: bool = False,
) -> dict[str, Any]:
    """Return a defensive copy of the one eligible exact engine profile."""

    matches = [
        profile
        for profile in ENGINE_PROFILES
        if profile["expected_sha256"] == sha256 and profile["byte_count"] == byte_count
    ]
    if len(matches) != 1:
        raise EngineProfileError(
            "whisper.cpp executable is not an exact hash-and-size allowlisted profile"
        )
    profile = matches[0]
    if (
        profile["admission"] == "legacy_manifest_replay_only"
        and not allow_legacy_manifest_replay
    ):
        raise EngineProfileError(
            "whisper.cpp v1.8.3 is retained only for sealed legacy-manifest validation; "
            "new batches require the current UTF-8-safe engine profile"
        )
    return {
        **profile,
        "build": {
            **profile["build"],
            "configuration": list(profile["build"]["configuration"]),
        },
    }


def public_engine_document(profile: dict[str, Any], executable: str) -> dict[str, Any]:
    """Project a matched profile into the existing ASR work-order engine contract."""

    return {
        "executable": executable,
        "expected_sha256": profile["expected_sha256"],
        "byte_count": profile["byte_count"],
        "version_label": profile["version_label"],
        "version_evidence": profile["version_evidence"],
        "build": {
            **profile["build"],
            "configuration": list(profile["build"]["configuration"]),
        },
    }
