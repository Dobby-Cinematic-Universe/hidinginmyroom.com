from __future__ import annotations

import hashlib
import importlib.util
import json
import math
import os
import stat
import tempfile
import unittest
from pathlib import Path
from types import ModuleType


REPOSITORY_ROOT = Path(__file__).resolve().parents[2]


def load_module(name: str, path: Path) -> ModuleType:
    spec = importlib.util.spec_from_file_location(name, path)
    if spec is None or spec.loader is None:
        raise RuntimeError(f"could not load {path}")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


SMOKE = load_module("himr_gpu_smoke_test_module", REPOSITORY_ROOT / "pipeline/gpu/smoke.py")
SEALER = load_module(
    "himr_gpu_model_sealer_test_module",
    REPOSITORY_ROOT / "pipeline/gpu/seal_model_snapshot.py",
)


class GpuSmokeProfileTests(unittest.TestCase):
    def test_canonical_json_rejects_nonfinite_numbers(self) -> None:
        for module in (SMOKE, SEALER):
            with self.assertRaises(ValueError):
                module.canonical_bytes({"unsafe": math.nan})

    def test_atomic_exclusive_writer_does_not_replace(self) -> None:
        for module in (SMOKE, SEALER):
            with self.subTest(module=module.__name__), tempfile.TemporaryDirectory() as temporary:
                root = Path(temporary).resolve()
                target = root / "receipt.json"
                module.write_exclusive(target, b"first\n")
                self.assertEqual(target.read_bytes(), b"first\n")
                self.assertEqual(stat.S_IMODE(target.stat().st_mode), 0o400)
                with self.assertRaises(FileExistsError):
                    module.write_exclusive(target, b"second\n")
                self.assertEqual(target.read_bytes(), b"first\n")
                self.assertEqual(list(root.glob(".receipt.json.tmp-*")), [])

    def test_fixture_manifest_binds_only_declared_synthetic_audio(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary).resolve()
            audio = root / "synthetic.flac"
            audio.write_bytes(b"fixture")
            audio_sha256 = hashlib.sha256(audio.read_bytes()).hexdigest()
            manifest = {
                "kind": "himr_synthetic_audio_fixture",
                "schema_version": 1,
                "created_at": "2026-08-28T00:00:00Z",
                "synthetic": True,
                "corpus_evidence": False,
                "generator": {},
                "normalization": {},
                "artifact": {
                    "path": str(audio),
                    "sha256": audio_sha256,
                    "byte_count": len(b"fixture"),
                },
                "policy": {"publication_authority": "none"},
            }
            manifest_path = root / "fixture.json"
            manifest_path.write_text(json.dumps(manifest), encoding="utf-8")
            manifest_sha256 = hashlib.sha256(manifest_path.read_bytes()).hexdigest()
            self.assertEqual(
                SMOKE.validate_fixture_manifest(
                    manifest_path,
                    manifest_sha256,
                    audio,
                    audio_sha256,
                ),
                manifest,
            )
            manifest["synthetic"] = False
            manifest_path.write_text(json.dumps(manifest), encoding="utf-8")
            with self.assertRaises(ValueError):
                SMOKE.validate_fixture_manifest(
                    manifest_path,
                    hashlib.sha256(manifest_path.read_bytes()).hexdigest(),
                    audio,
                    audio_sha256,
                )

    def test_current_network_namespace_is_not_accepted_as_parent_isolation(self) -> None:
        with self.assertRaises(RuntimeError):
            SMOKE.network_namespace_evidence(os.readlink("/proc/self/ns/net"))

    def test_probe_rejects_nonfinite_duration(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary).resolve()
            audio = root / "synthetic.flac"
            audio.write_bytes(b"fixture")
            fake = root / "ffprobe"
            fake.write_text(
                "#!/bin/sh\n"
                "printf '%s\\n' '{\"streams\":[{\"codec_type\":\"audio\","
                "\"codec_name\":\"flac\",\"sample_rate\":\"16000\","
                "\"channels\":1}],\"format\":{\"format_name\":\"flac\","
                "\"duration\":NaN}}'\n",
                encoding="utf-8",
            )
            fake.chmod(0o700)
            with self.assertRaises(ValueError):
                SMOKE.probe_duration(fake, audio)


if __name__ == "__main__":
    unittest.main()
