"""Synthetic-only tests: no Community-1 artifacts, ML imports or GPU work."""
from __future__ import annotations

from contextlib import nullcontext
import copy
import hashlib
import json
import os
from pathlib import Path
import subprocess
import sys
import tempfile
from types import SimpleNamespace
import unittest
from unittest import mock

from pipeline import screened_diarization_engine as engine


CONFIG = b"""version: 4.0.0
pipeline:
  name: pyannote.audio.pipelines.SpeakerDiarization
  params:
    segmentation: $model/segmentation
    embedding: $model/embedding
    plda: $model/plda
    clustering: VBxClustering
    embedding_exclude_overlap: true
params:
  segmentation:
    min_duration_off: 0.0
  clustering:
    threshold: 0.6
    Fa: 0.07
    Fb: 0.8
"""


def digest(body):
    return hashlib.sha256(body).hexdigest()


def request(root):
    return {"kind": "himr_screened_diarization_worker_request", "schema_version": 1,
        "model_bundle": {"path": str(root / "bundle.json"), "sha256": "a" * 64},
        "audio_pcm": {"path": str(root / "audio.pcm"), "sha256": "b" * 64, "byte_count": 32000},
        "recording_id": "test-recording", "media_sha256": "c" * 64, "duration_ms": 1000,
        "device": "cpu", "gpu_uuid": None, "threads": 1,
        "resources": {"max_duration_ms": 86400000, "max_pcm_bytes": 86400000 * 32,
            "max_waveform_bytes": 86400000 * 64, "max_turns": 100000,
            "segmentation_batch_size": 1, "embedding_batch_size": 1, "cuda_memory_fraction": .5},
        "speaker_parameters": {}}


class ConfigTests(unittest.TestCase):
    def test_reviewed_mapping_has_no_remote_runtime_references(self):
        value = engine._config(CONFIG)
        self.assertEqual(value["pipeline"]["params"]["segmentation"], "$model/segmentation")
        self.assertEqual(value["params"]["clustering"]["Fa"], .07)

    def test_safe_quoted_local_model_reference(self):
        engine._config(CONFIG.replace(b"$model/segmentation", b"'$model/segmentation'"))

    def test_dependencies_form_supported_without_executable_loader(self):
        engine._config(CONFIG.replace(b"version: 4.0.0", b"dependencies:\n  pyannote.audio: 4.0.7"))

    def test_executable_classes_remote_refs_and_paths_fail(self):
        replacements = [
            (b"pyannote.audio.pipelines.SpeakerDiarization", b"evil.module.Class"),
            (b"$model/segmentation", b"pyannote/segmentation"),
            (b"$model/embedding", b"/tmp/embedding"),
            (b"$model/plda", b"$model/../../plda"),
            (b"VBxClustering", b"OracleClustering"),
            (b"embedding_exclude_overlap: true", b"embedding_exclude_overlap: yes"),
            (b"threshold: 0.6", b"threshold: .nan"),
        ]
        for old, new in replacements:
            with self.subTest(new=new), self.assertRaises(engine.DiarizationError):
                engine._config(CONFIG.replace(old, new))

    def test_yaml_alias_tag_duplicate_and_unknown_fields_fail(self):
        variants = [CONFIG + b"device: cuda\n", CONFIG + b"preprocessors: bad\n",
            CONFIG.replace(b"threshold: 0.6", b"threshold: !!python/object:evil {}"),
            CONFIG.replace(b"threshold: 0.6", b"threshold: &x 0.6"),
            CONFIG.replace(b"threshold: 0.6", b"threshold: *x"),
            CONFIG.replace(b"threshold: 0.6", b"threshold: 0.6\n    threshold: 0.7"),
            CONFIG.replace(b"  name:", b"   name:"), CONFIG.replace(b"  name:", b"\tname:")]
        for value in variants:
            with self.subTest(value=value), self.assertRaises(engine.DiarizationError):
                engine._config(value)


class RequestTests(unittest.TestCase):
    def setUp(self):
        self.value = request(Path("/tmp/private-diarization-tests"))

    def test_exact_full_pcm_request_and_independent_copy(self):
        value = engine.validate_request(self.value)
        value["resources"]["max_turns"] = 3
        self.assertEqual(self.value["resources"]["max_turns"], 100000)

    def test_bad_durations_pcm_and_waveform_bounds_rejected(self):
        variants = []
        for duration in (0, True, 86400001, 1000.0):
            value = copy.deepcopy(self.value); value["duration_ms"] = duration; variants.append(value)
        for key, number in (("max_pcm_bytes", 31999), ("max_waveform_bytes", 63999),
                            ("max_turns", 100001), ("embedding_batch_size", 17),
                            ("segmentation_batch_size", 0), ("cuda_memory_fraction", float("nan"))):
            value = copy.deepcopy(self.value); value["resources"][key] = number; variants.append(value)
        value = copy.deepcopy(self.value); value["audio_pcm"]["byte_count"] -= 2; variants.append(value)
        for value in variants:
            with self.subTest(value=value), self.assertRaises(engine.DiarizationError):
                engine.validate_request(value)

    def test_unknown_keys_source_identity_and_device_rejected(self):
        variants = [{**self.value, "credentials": "unused"}, {**self.value, "media_sha256": "wrong"},
            {**self.value, "recording_id": "../escape"}, {**self.value, "device": "automatic"},
            {**self.value, "gpu_uuid": "GPU-" + "a" * 36}, {**self.value, "device": "cuda"}]
        for value in variants:
            with self.subTest(value=value), self.assertRaises(engine.DiarizationError):
                engine.validate_request(value)

    def test_explicit_cuda_uuid_and_bounded_count(self):
        value = {**self.value, "device": "cuda", "gpu_uuid": "GPU-12345678-1234-1234-1234-123456789abc"}
        self.assertEqual(engine.validate_request(value)["device"], "cuda")
        for params in ({"num_speakers": 256}, {"min_speakers": 1, "max_speakers": 256}):
            engine.validate_request({**self.value, "speaker_parameters": params})
        for params in ({"name": "person"}, {"num_speakers": True}, {"num_speakers": 257},
                       {"num_speakers": 2, "max_speakers": 3}, {"min_speakers": 3, "max_speakers": 2}):
            with self.subTest(params=params), self.assertRaises(engine.DiarizationError):
                engine.validate_request({**self.value, "speaker_parameters": params})


class BundleTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory(prefix="himr-diarization-engine-test-")
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        self.model_root = self.root / "models"
        self.model_root.mkdir(mode=0o700)
        files, pins, weights = [], {}, {}
        for index, (name, (_size, _blob, lfs)) in enumerate(engine.UPSTREAM_FILES.items()):
            body = CONFIG if name == "config.yaml" else f"synthetic artifact {index}".encode()
            binding = self.write(self.model_root / name, body)
            files.append({"relative_path": name, "sha256": binding["sha256"], "byte_count": len(body)})
            git = (f"version https://git-lfs.github.com/spec/v1\noid sha256:{binding['sha256']}\nsize {len(body)}\n".encode()
                   if lfs else body)
            pins[name] = (len(body), engine._git_blob(git), lfs)
            if name in engine.WEIGHT_SHA256:
                weights[name] = binding["sha256"]
        self.addCleanup(mock.patch.stopall)
        mock.patch.object(engine, "UPSTREAM_FILES", pins).start()
        mock.patch.object(engine, "WEIGHT_SHA256", weights).start()
        binary = self.write(self.root / "python", b"synthetic executable", sized=True)
        installed = self.write(self.root / "runtime.py", b"synthetic runtime", sized=True)
        packages = []
        names = ("pyannote-audio", "torch", "torchaudio", "numpy", "lightning", "pyannote-core", "pyannote-pipeline")
        for name in names:
            body = bytes(894598) if name == "pyannote-audio" else name.encode()
            wheel = self.write(self.root / (name + ".whl"), body, sized=True)
            if name == "pyannote-audio":
                mock.patch.object(engine, "PYANNOTE_WHEEL_SHA256", wheel["sha256"]).start()
            packages.append({"name": name, "version": "4.0.7" if name == "pyannote-audio" else "2.8.0", "wheel": wheel})
        self.runtime = {"kind": "himr_community1_runtime", "schema_version": 1,
            "python": {**binary, "version": ".".join(map(str, sys.version_info[:3]))},
            "packages": packages, "installed_files": [installed]}
        self.value = {"kind": "himr_community1_bundle", "schema_version": 1,
            "repository": engine.REPOSITORY, "revision": engine.REVISION, "root": str(self.model_root),
            "files": files, "runtime": self.json_file("runtime.json", self.runtime),
            "license": self.write(self.root / "LICENSE.txt", b"synthetic CC-BY review fixture"),
            "review_evidence": {}}
        self.seal()

    def write(self, path, body, sized=False):
        path.parent.mkdir(mode=0o700, parents=True, exist_ok=True)
        path.write_bytes(body)
        path.chmod(0o600)
        value = {"path": str(path), "sha256": digest(body)}
        if sized:
            value["byte_count"] = len(body)
        return value

    def json_file(self, name, value):
        return self.write(self.root / name, engine._canonical(value))

    def seal(self, **review_updates):
        review = {"kind": "himr_community1_bundle_review", "schema_version": 1,
            "repository": engine.REPOSITORY, "revision": engine.REVISION, "license": "CC-BY-4.0",
            "terms_accepted": True, "offline_runtime_reviewed": True, "reviewer": "SYNTHETIC TEST ONLY",
            "reviewed_at": "2026-09-12T00:00:00Z", "bundle_files_sha256": digest(engine._canonical(self.value["files"])),
            "runtime": self.value["runtime"], "license_snapshot": self.value["license"], **review_updates}
        self.value["review_evidence"] = self.json_file("review.json", review)
        self.binding = self.json_file("bundle.json", self.value)

    def test_complete_reviewed_local_bundle_admits_without_native_readiness_claim(self):
        with mock.patch.object(engine, "_native_inference", side_effect=AssertionError("no inference")):
            admitted = engine.admit_bundle(self.binding)
        self.assertEqual(len(admitted["files"]), 10)
        self.assertFalse(admitted["native_inference_verified"])
        self.assertEqual(admitted["runtime_binding"], self.value["runtime"])

    def test_missing_artifact_not_downloaded(self):
        (self.model_root / "embedding/pytorch_model.bin").unlink()
        with self.assertRaises(FileNotFoundError):
            engine.admit_bundle(self.binding)

    def test_missing_runtime_file_is_a_readiness_blocker(self):
        Path(self.runtime["packages"][0]["wheel"]["path"]).unlink()
        with self.assertRaises(FileNotFoundError):
            engine.admit_bundle(self.binding)

    def test_model_hash_and_upstream_git_identity_both_required(self):
        self.value["files"][0]["sha256"] = "f" * 64
        self.seal()
        with self.assertRaisesRegex(engine.DiarizationError, "SHA-256"):
            engine.admit_bundle(self.binding)
        body = b"synthetic artifact 9".replace(b"9", b"x")
        row = self.value["files"][-1]
        self.write(self.model_root / row["relative_path"], body)
        row["sha256"] = digest(body)
        self.value["files"][0]["sha256"] = digest(b"synthetic artifact 0")
        self.seal()
        with self.assertRaises(engine.DiarizationError):
            engine.admit_bundle(self.binding)

    def test_unknown_or_duplicate_relative_path_and_partial_mirror_rejected(self):
        original = copy.deepcopy(self.value)
        for path in ("../embedding/pytorch_model.bin", "/tmp/evil", "config.yaml"):
            self.value = copy.deepcopy(original)
            self.value["files"][0]["relative_path"] = path
            self.seal()
            with self.subTest(path=path), self.assertRaises(engine.DiarizationError):
                engine.admit_bundle(self.binding)
        self.value = original; self.value["files"].pop(); self.seal()
        with self.assertRaises(engine.DiarizationError):
            engine.admit_bundle(self.binding)

    def test_symlink_and_peer_writable_artifact_rejected(self):
        path = self.model_root / "README.md"
        path.chmod(0o666)
        with self.assertRaises(engine.DiarizationError):
            engine.admit_bundle(self.binding)
        path.chmod(0o600)
        replacement = self.root / "original-readme"
        path.rename(replacement); path.symlink_to(replacement)
        with self.assertRaises(OSError):
            engine.admit_bundle(self.binding)

    def test_terms_runtime_and_file_digest_review_must_match(self):
        for update in ({"terms_accepted": False}, {"offline_runtime_reviewed": False},
                       {"bundle_files_sha256": "0" * 64}, {"license": "unknown"}):
            self.seal(**update)
            with self.subTest(update=update), self.assertRaises(engine.DiarizationError):
                engine.admit_bundle(self.binding)

    def test_mutable_revision_and_unknown_runtime_rejected(self):
        self.value["revision"] = "main"; self.seal()
        with self.assertRaisesRegex(engine.DiarizationError, "revision"):
            engine.admit_bundle(self.binding)
        self.value["revision"] = engine.REVISION
        self.runtime["packages"][0]["version"] = "4.0.8"
        self.value["runtime"] = self.json_file("runtime.json", self.runtime); self.seal()
        with self.assertRaisesRegex(engine.DiarizationError, "runtime wheel"):
            engine.admit_bundle(self.binding)

    def test_duplicate_json_keys_and_nonfinite_values_rejected(self):
        for raw in (b'{"a":1,"a":2}', b'{"a":NaN}', b'[]'):
            binding = self.write(self.root / "bad.json", raw)
            with self.subTest(raw=raw), self.assertRaises(engine.DiarizationError):
                engine._json(binding)

    def fake_distributions(self):
        path = self.runtime["installed_files"][0]["path"]
        return [SimpleNamespace(metadata={"Name": row["name"]}, version=row["version"],
            files=[path], locate_file=lambda p: Path(p)) for row in self.runtime["packages"]]

    def test_runtime_checks_installed_versions_and_hashes(self):
        admitted = engine.admit_bundle(self.binding)
        with mock.patch.object(engine.sys, "executable", self.runtime["python"]["path"]), \
                mock.patch.object(engine.importlib.metadata, "distributions", return_value=self.fake_distributions()):
            result = engine._verify_runtime(admitted)
            self.assertTrue(result["all_registered_runtime_files_and_wheels_verified"])
            Path(self.runtime["installed_files"][0]["path"]).write_bytes(b"modified runtime!")
            with self.assertRaisesRegex(engine.DiarizationError, "runtime file"):
                engine._verify_runtime(admitted)

    def test_empty_installed_module_is_explicitly_hash_bound(self):
        empty = self.write(self.root / "empty_module.py", b"", sized=True)
        self.runtime["installed_files"].append(empty)
        self.value["runtime"] = self.json_file("runtime.json", self.runtime); self.seal()
        admitted = engine.admit_bundle(self.binding)
        with mock.patch.object(engine.sys, "executable", self.runtime["python"]["path"]), \
                mock.patch.object(engine.importlib.metadata, "distributions", return_value=self.fake_distributions()):
            engine._verify_runtime(admitted)

    def test_runtime_rejects_missing_record_file_and_different_interpreter(self):
        admitted = engine.admit_bundle(self.binding)
        with self.assertRaisesRegex(engine.DiarizationError, "Python"):
            engine._verify_runtime(admitted)
        distributions = self.fake_distributions()
        distributions[0].files.append(str(self.model_root / "config.yaml"))
        with mock.patch.object(engine.sys, "executable", self.runtime["python"]["path"]), \
                mock.patch.object(engine.importlib.metadata, "distributions", return_value=distributions), \
                self.assertRaisesRegex(engine.DiarizationError, "omits"):
            engine._verify_runtime(admitted)

    def test_worker_mocked_full_recording_no_embeddings_and_stable_bindings(self):
        value = request(self.root)
        value["model_bundle"] = self.binding
        value["audio_pcm"] = self.write(self.root / "audio.pcm", bytes(32000), sized=True)
        calls = []
        def inference(actual, admitted, pcm):
            self.assertEqual(os.fstat(pcm).st_size, 32000)
            self.assertEqual(actual["duration_ms"], 1000)
            calls.append(actual)
            return {"ordinary": [], "exclusive": []}, None
        with mock.patch.object(engine, "_offline_environment"), \
                mock.patch.object(engine, "_verify_runtime", return_value={"_witnesses": {}, "versions": {}}), \
                mock.patch.object(engine, "_native_inference", side_effect=inference):
            result = engine.run_worker(value)
        self.assertEqual(len(calls), 1)
        self.assertEqual(result["provenance"]["source_media_sha256"], value["media_sha256"])
        self.assertEqual(result["provenance"]["audio_pcm"], value["audio_pcm"])
        self.assertFalse(result["provenance"]["semantics"]["speaker_embeddings_exported"])
        self.assertEqual(result["provenance"]["semantics"]["score_state"], "unavailable")
        self.assertNotIn(b"confidence", engine._canonical(result))

    def test_worker_pcm_hash_mismatch_blocks_before_model_inference(self):
        value = request(self.root); value["model_bundle"] = self.binding
        self.write(self.root / "audio.pcm", bytes(32000))
        with mock.patch.object(engine, "_offline_environment"), \
                mock.patch.object(engine, "_verify_runtime", return_value={"_witnesses": {}}), \
                mock.patch.object(engine, "_native_inference") as inference, \
                self.assertRaisesRegex(engine.DiarizationError, "normalized PCM"):
            engine.run_worker(value)
        inference.assert_not_called()

    def test_parent_provenance_validation_binds_request_runtime_and_model(self):
        value = request(self.root); value["model_bundle"] = self.binding
        value["audio_pcm"] = self.write(self.root / "audio.pcm", bytes(32000), sized=True)
        admitted = engine.admit_bundle(self.binding)
        runtime = {"manifest": admitted["runtime_binding"], "python": self.runtime["python"],
            "versions": {row["name"]: row["version"] for row in self.runtime["packages"]},
            "installed_files_sha256": digest(engine._canonical(self.runtime["installed_files"])),
            "all_registered_runtime_files_and_wheels_verified": True, "_witnesses": {}}
        with mock.patch.object(engine, "_offline_environment"), \
                mock.patch.object(engine, "_verify_runtime", return_value=runtime), \
                mock.patch.object(engine, "_native_inference", return_value=({"ordinary": [], "exclusive": []}, None)):
            answer = engine.run_worker(value)
        original = answer["provenance"]
        self.assertEqual(engine.validate_provenance(original, value), original)
        for key, replacement in (("recording_id", "other"), ("source_media_sha256", "f" * 64),
                ("threads", True), ("gpu", {}), ("parameters", {}), ("runtime", {}),
                ("implementation", {"path": str(Path(engine.__file__).resolve()), "sha256": "0" * 64})):
            corrupted = {**original, key: replacement}
            with self.subTest(key=key), self.assertRaises(engine.DiarizationError):
                engine.validate_provenance(corrupted, value)

    def test_pcm_mutation_during_inference_invalidates_result(self):
        value = request(self.root); value["model_bundle"] = self.binding
        value["audio_pcm"] = self.write(self.root / "audio.pcm", bytes(32000), sized=True)
        def mutate(*_):
            path = Path(value["audio_pcm"]["path"])
            path.write_bytes(bytes([1]) * 32000)
            return {"ordinary": [], "exclusive": []}, None
        with mock.patch.object(engine, "_offline_environment"), \
                mock.patch.object(engine, "_verify_runtime", return_value={"_witnesses": {}}), \
                mock.patch.object(engine, "_native_inference", side_effect=mutate), \
                self.assertRaisesRegex(engine.DiarizationError, "PCM changed"):
            engine.run_worker(value)


class OutputTests(unittest.TestCase):
    @staticmethod
    def output(ordinary, exclusive):
        def annotation(rows):
            return SimpleNamespace(itertracks=lambda **_: ((SimpleNamespace(start=s, end=e), i, label)
                for i, (s, e, label) in enumerate(rows)))
        return SimpleNamespace(speaker_diarization=annotation(ordinary),
            exclusive_speaker_diarization=annotation(exclusive), speaker_embeddings="MUST NOT EXPORT")

    def test_ordinary_overlap_and_exclusive_preserve_raw_seconds_shared_labels(self):
        ordinary = [(0.0, .7, "SPEAKER_00"), (.3, 1.0, "SPEAKER_01")]
        exclusive = [(0.0, .351234, "SPEAKER_00"), (.351234, 1.0, "SPEAKER_01")]
        result = engine._annotations(self.output(ordinary, exclusive), 1000, 10)
        self.assertEqual(result["exclusive"][0]["end"], .351234)
        self.assertEqual(result["ordinary"][1]["speaker"], "SPEAKER_01")
        self.assertNotIn("speaker_embeddings", result)

    def test_invalid_model_output_fails_without_confidence_or_time_fabrication(self):
        invalid = [(-.01, 1., "SPEAKER_00"), (0., 1.01, "SPEAKER_00"),
            (float("nan"), 1., "SPEAKER_00"), (0., float("inf"), "SPEAKER_00"),
            (0., 1., "Daniel"), (True, 1., "SPEAKER_00"), ("0", 1., "SPEAKER_00")]
        for row in invalid:
            with self.subTest(row=row), self.assertRaises(engine.DiarizationError):
                engine._annotations(self.output([row], []), 1000, 10)

    def test_exclusive_overlap_unknown_speaker_duplicate_and_overbudget_fail(self):
        row = (0., .7, "SPEAKER_00")
        for ordinary, exclusive, maximum in [([row, row], [], 10), ([row], [row, (.5, 1., "SPEAKER_00")], 10),
                ([row], [(0., .1, "SPEAKER_01")], 10), ([row, (.8, 1., "SPEAKER_00")], [], 1)]:
            with self.subTest(ordinary=ordinary, exclusive=exclusive), self.assertRaises(engine.DiarizationError):
                engine._annotations(self.output(ordinary, exclusive), 1000, maximum)

    def test_silent_recording_can_have_two_empty_representations(self):
        self.assertEqual(engine._annotations(self.output([], []), 1000, 10), {"ordinary": [], "exclusive": []})


class RestrictedLoaderTests(unittest.TestCase):
    def fake(self, architecture=None):
        expected = engine.ARCHITECTURES["segmentation"]
        model_type = type(expected[1], (), {"__module__": expected[0], "eval": lambda self: None})
        loaded = {"pyannote.audio": {"architecture": {"module": (architecture or expected)[0],
            "class": (architecture or expected)[1]}}, "state_dict": {"tensor": "synthetic"}}
        torch = SimpleNamespace(serialization=SimpleNamespace(safe_globals=lambda values: nullcontext()),
            load=mock.Mock(return_value=loaded))
        factory = SimpleNamespace(from_pretrained=mock.Mock(return_value=model_type()))
        return torch, factory

    def test_preflight_is_weights_only_and_upstream_remains_forced_restricted(self):
        torch, factory = self.fake()
        with mock.patch.dict(os.environ, {"TORCH_FORCE_WEIGHTS_ONLY_LOAD": "1"}):
            engine._load_model("segmentation", b"synthetic", torch, factory, [])
        self.assertTrue(torch.load.call_args.kwargs["weights_only"])
        self.assertTrue(factory.from_pretrained.call_args.kwargs["strict"])
        self.assertFalse(factory.from_pretrained.call_args.kwargs["token"])

    def test_unreviewed_architecture_never_reaches_dynamic_loader(self):
        torch, factory = self.fake(("malicious.module", "Class"))
        with self.assertRaisesRegex(engine.DiarizationError, "architecture"):
            engine._load_model("segmentation", b"synthetic", torch, factory, [])
        factory.from_pretrained.assert_not_called()

    def test_restricted_load_failure_never_falls_back(self):
        torch, factory = self.fake()
        torch.load.side_effect = ValueError("unreviewed pickle global")
        with self.assertRaises(ValueError):
            engine._load_model("segmentation", b"synthetic", torch, factory, [])
        self.assertEqual(torch.load.call_count, 1)
        factory.from_pretrained.assert_not_called()

    def test_missing_force_flag_blocks_explicit_unsafe_upstream_default(self):
        torch, factory = self.fake()
        with mock.patch.dict(os.environ, {}, clear=True), self.assertRaisesRegex(engine.DiarizationError, "restricted"):
            engine._load_model("segmentation", b"synthetic", torch, factory, [])
        factory.from_pretrained.assert_not_called()


class OfflineProcessTests(unittest.TestCase):
    def test_standalone_cli_works_outside_repository_without_pythonpath(self):
        with tempfile.TemporaryDirectory(prefix="himr-diarization-cli-test-") as directory:
            result = subprocess.run([sys.executable, "-B", str(Path(engine.__file__).resolve()), "--help"],
                cwd=directory, capture_output=True, text=True, timeout=10,
                env={"PATH": "/usr/bin:/bin", "LANG": "C", "PYTHONDONTWRITEBYTECODE": "1"})
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertIn("--expected-sha256", result.stdout)

    def test_native_network_denial_and_flags_without_any_model_import(self):
        code = """import errno, os, socket, sys
from pipeline.screened_diarization_engine import _offline_environment
_offline_environment({'gpu_uuid':None,'device':'cpu','threads':1})
assert os.environ['PYANNOTE_METRICS_ENABLED']=='false'
assert os.environ['TORCH_FORCE_WEIGHTS_ONLY_LOAD']=='1'
assert 'torch' not in sys.modules
for family in (socket.AF_INET,socket.AF_INET6):
 try: socket.socket(family)
 except OSError as error: assert error.errno==errno.EPERM
 else: raise AssertionError('network not denied')
print('offline')
"""
        result = subprocess.run([sys.executable, "-c", code], capture_output=True, text=True, timeout=10,
                                env={**os.environ, "PYTHONDONTWRITEBYTECODE": "1"})
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual(result.stdout.strip(), "offline")


if __name__ == "__main__":
    unittest.main()
