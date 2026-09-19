"""Synthetic-only offline adapter checks; no models, installs, or GPU work."""
from __future__ import annotations

import copy
from fractions import Fraction
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

import numpy as np

from pipeline import speaker_face_matching_core as core
from pipeline import speaker_face_matching_engine as engine
from pipeline import speaker_face_matching_visual as visual


def digest(body):
    return hashlib.sha256(body).hexdigest()


def clip():
    value = {"run_id": "diarjob_" + "a" * 32, "media_sha256": "b" * 64,
        "speaker": "SPEAKER_0000", "start_ms": 0, "end_ms": 2000, "source_turn_indices": [0]}
    return {"clip_id": "avclip_" + digest(engine._canonical(value))[:32], **value}


def request(root):
    return {"kind": "himr_speaker_face_matching_worker_request", "schema_version": 1,
        "model_bundle": {"path": str(root / "bundle.json"), "sha256": "a" * 64},
        "audio_pcm": {"path": str(root / "audio.pcm"), "sha256": "b" * 64, "byte_count": 64000},
        "video_rgb": {"path": str(root / "video.bgr"), "sha256": "c" * 64, "byte_count": 50 * 640 * 360 * 3},
        "decode_receipt": {"path": str(root / "decode.json"), "sha256": "d" * 64},
        "clip": clip(), "device": "cpu", "gpu_uuid": None, "threads": 1,
        "resources": {"max_frames": 125, "max_tracks": 16, "cuda_memory_fraction": .5}}


def timing(value):
    item = value["clip"]
    rows = [(Fraction(i, 25), Fraction(1, 25)) for i in range(50)]
    result = visual._timeline_receipt(rows, rows, [(0, 32000)], start_ms=item["start_ms"], end_ms=item["end_ms"])
    result.update({"video_sha256": value["video_rgb"]["sha256"], "audio_sha256": value["audio_pcm"]["sha256"],
        "video_stderr_sha256": "e" * 64, "audio_stderr_sha256": "f" * 64})
    return result


class RequestTests(unittest.TestCase):
    def setUp(self):
        self.value = request(Path("/tmp/speaker-face-engine-test"))

    def test_exact_bounded_clip_request_copies(self):
        value = engine.validate_request(self.value)
        value["resources"]["max_tracks"] = 1
        self.assertEqual(self.value["resources"]["max_tracks"], 16)

    def test_no_names_credentials_or_arbitrary_entrypoints(self):
        for field in ("person_name", "token", "model_class", "entrypoint", "face_embeddings"):
            with self.subTest(field=field), self.assertRaises(engine.MatchingError):
                engine.validate_request({**self.value, field: "unused"})

    def test_wrong_clip_scope_rejected(self):
        for key, bad in (("speaker", "Alice"), ("run_id", "another-run"), ("end_ms", 6000), ("start_ms", 1)):
            value = copy.deepcopy(self.value)
            value["clip"][key] = bad
            with self.subTest(key=key), self.assertRaises((engine.MatchingError, core.MatchingError)):
                engine.validate_request(value)

    def test_sizes_duplicates_and_limits_fail(self):
        variants = []
        for key in ("audio_pcm", "video_rgb"):
            value = copy.deepcopy(self.value); value[key]["byte_count"] -= 1; variants.append(value)
        for key, bad in (("max_frames", 126), ("max_frames", True), ("max_tracks", 17),
                         ("max_tracks", 0), ("cuda_memory_fraction", float("nan"))):
            value = copy.deepcopy(self.value); value["resources"][key] = bad; variants.append(value)
        value = copy.deepcopy(self.value); value["video_rgb"]["path"] = value["audio_pcm"]["path"]; variants.append(value)
        for value in variants:
            with self.subTest(value=value), self.assertRaises(engine.MatchingError):
                engine.validate_request(value)

    def test_only_explicit_device_selection(self):
        for changes in ({"device": "auto"}, {"device": "cuda"}, {"gpu_uuid": "GPU-bad"}):
            with self.subTest(changes=changes), self.assertRaises(engine.MatchingError):
                engine.validate_request({**self.value, **changes})
        engine.validate_request({**self.value, "device": "cuda", "gpu_uuid": "GPU-12345678-1234-1234-1234-123456789abc"})

    def test_decode_timeline_and_input_hashes_replay(self):
        receipt = timing(self.value)
        self.assertEqual(engine._decode_sync(receipt, self.value), {"state": "verified", "offset_ms": 0})
        for key, bad in (("video_sha256", "0" * 64), ("audio_sha256", "0" * 64),
                         ("source_lip_sync_verified", True), ("audio_bytes", 63999)):
            with self.subTest(key=key), self.assertRaises((engine.MatchingError, visual.VisualMatchingError)):
                engine._decode_sync({**receipt, key: bad}, self.value)

    def test_no_ml_imports_for_cli_help_outside_repo(self):
        code = "import sys; from pipeline import speaker_face_matching_engine; assert not {'torch','cv2','python_speech_features'} & set(sys.modules)"
        subprocess.run([sys.executable, "-B", "-c", code], cwd=engine.ROOT, check=True, capture_output=True, timeout=15)
        result = subprocess.run([sys.executable, "-B", str(Path(engine.__file__).resolve()), "--help"],
            cwd="/tmp", capture_output=True, timeout=15)
        self.assertEqual(result.returncode, 0, result.stderr)


class BundleTests(unittest.TestCase):
    def setUp(self):
        temporary = tempfile.TemporaryDirectory(prefix="himr-face-engine-test-")
        self.addCleanup(temporary.cleanup)
        self.root = Path(temporary.name)
        model_root = self.root / "models"
        model_root.mkdir(mode=0o700)
        pins, files = {}, []
        for ordinal, name in enumerate(engine.UPSTREAM_FILES):
            body = f"synthetic artifact {ordinal}".encode()
            ref = self.write(model_root / name, body)
            files.append({"relative_path": name, "sha256": ref["sha256"], "byte_count": len(body)})
            pins[name] = (len(body), engine.common._git_blob(body))
        self.patch(engine, "UPSTREAM_FILES", pins)
        yunet = self.write(self.root / "yunet.onnx", b"synthetic yunet", sized=True)
        self.patch(engine, "YUNET_SHA256", yunet["sha256"])
        self.patch(engine, "YUNET_BYTES", yunet["byte_count"])
        python = self.write(self.root / "python", b"synthetic executable", sized=True)
        installed = self.write(self.root / "runtime.py", b"synthetic installed file", sized=True)
        packages = [{"name": name, "version": version,
            "wheel": self.write(self.root / (name + ".whl"), name.encode(), sized=True)} for name, version in (
                ("torch", "2.8.0"), ("numpy", "2.4.6"), ("scipy", "1.17.0"),
                ("python-speech-features", "0.6"), ("opencv-python-headless", "4.14.0.94"))]
        self.runtime = {"kind": "himr_lrasd_runtime", "schema_version": 1,
            "python": {**python, "version": ".".join(map(str, sys.version_info[:3]))},
            "packages": packages, "installed_files": [installed]}
        self.bundle = {"kind": "himr_lrasd_bundle", "schema_version": 1,
            "repository": engine.REPOSITORY, "revision": engine.REVISION, "root": str(model_root),
            "files": files, "runtime": self.json_file("runtime.json", self.runtime), "yunet": yunet,
            "license": {"path": str(model_root / "LICENSE"), "sha256": next(row["sha256"] for row in files if row["relative_path"] == "LICENSE")},
            "review_evidence": {}}
        self.seal()
        self.value = request(self.root)
        self.value["model_bundle"] = self.binding
        self.value["decode_receipt"] = self.json_file("decode.json", timing(self.value))

    def patch(self, owner, name, value):
        patch = mock.patch.object(owner, name, value)
        patch.start()
        self.addCleanup(patch.stop)

    def write(self, path, body, sized=False):
        path.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
        path.write_bytes(body)
        path.chmod(0o600)
        return {"path": str(path), "sha256": digest(body), **({"byte_count": len(body)} if sized else {})}

    def json_file(self, name, value):
        return self.write(self.root / name, engine._canonical(value))

    def seal(self, review_changes=None):
        review = {"kind": "himr_lrasd_bundle_review", "schema_version": 1,
            "repository": engine.REPOSITORY, "revision": engine.REVISION, "license": "MIT",
            "license_reviewed": True, "offline_runtime_reviewed": True,
            "reviewer": "synthetic test", "reviewed_at": "synthetic timestamp",
            "bundle_files_sha256": digest(engine._canonical(self.bundle["files"])),
            "runtime": self.bundle["runtime"], "license_snapshot": self.bundle["license"], "yunet": self.bundle["yunet"]}
        review.update(review_changes or {})
        self.bundle["review_evidence"] = self.json_file("review.json", review)
        self.binding = self.json_file("bundle.json", self.bundle)

    def output(self):
        admitted = engine.admit_bundle(self.binding)
        detected = {"tracks": [], "cuts": []}
        observations = engine._observations(self.value, detected, {}, {"state": "verified", "offset_ms": 0})
        implementation = {"path": str(Path(engine.__file__).resolve()), "sha256": engine._self_hash()}
        return {"kind": "himr_speaker_face_matching_engine_output", "schema_version": 1,
            "clip_id": self.value["clip"]["clip_id"], "observations": observations,
            "provenance": engine._provenance(self.value, admitted, engine._runtime_proof(admitted), None, implementation)}

    def test_admission_is_readonly_and_not_native_readiness(self):
        before = {path: path.stat().st_mtime_ns for path in self.root.rglob("*") if path.is_file()}
        value = engine.admit_bundle(self.binding)
        self.assertFalse(value["native_inference_verified"])
        self.assertFalse(value["runtime_files_sha256_reverified"])
        self.assertEqual(before, {path: path.stat().st_mtime_ns for path in before})

    def test_unknown_revision_and_source_inventory_fail(self):
        for change in ({"revision": "0" * 40}, {"files": self.bundle["files"][:-1]},
                       {"files": [self.bundle["files"][0]] * len(self.bundle["files"])}, {"entrypoint": "evil.py"}):
            value = {**self.bundle, **change}
            with self.subTest(change=change), self.assertRaises(engine.MatchingError):
                engine.admit_bundle(self.json_file("bad-bundle.json", value))

    def test_resealed_changed_source_fails_upstream_git_pin(self):
        row = self.bundle["files"][0]
        body = b"x" * row["byte_count"]
        self.write(Path(self.bundle["root"]) / row["relative_path"], body)
        row["sha256"] = digest(body)
        self.seal()
        with self.assertRaises(engine.MatchingError):
            engine.admit_bundle(self.binding)

    def test_yunet_bytes_and_explicit_review_are_bound(self):
        self.bundle["yunet"]["sha256"] = "0" * 64
        self.seal()
        with self.assertRaises(engine.MatchingError):
            engine.admit_bundle(self.binding)

    def test_bundle_review_cannot_be_claimed_implicitly(self):
        self.seal({"offline_runtime_reviewed": False})
        with self.assertRaises(engine.MatchingError):
            engine.admit_bundle(self.binding)

    def test_separate_registered_runtime_contract(self):
        variants = []
        value = copy.deepcopy(self.runtime); value["kind"] = "himr_community1_runtime"; variants.append(value)
        value = copy.deepcopy(self.runtime); value["packages"][0]["version"] = "2.5.1"; variants.append(value)
        value = copy.deepcopy(self.runtime); value["packages"][3]["version"] = "0.7"; variants.append(value)
        value = copy.deepcopy(self.runtime); value["packages"] = value["packages"][:-1]; variants.append(value)
        value = copy.deepcopy(self.runtime); value["installed_files"] *= 2; variants.append(value)
        for value in variants:
            with self.subTest(value=value), self.assertRaises(engine.MatchingError):
                engine._runtime_schema(value)

    def test_runtime_presence_checks_sizes(self):
        Path(self.runtime["installed_files"][0]["path"]).write_bytes(b"wrong")
        with self.assertRaises(engine.MatchingError):
            engine.admit_bundle(self.binding)

    def test_output_scope_provenance_and_implementation_replayed(self):
        output = self.output()
        engine.validate_output(output, self.value)
        variants = []
        value = copy.deepcopy(output); value["provenance"]["semantics"]["person_identity_claimed"] = True; variants.append(value)
        value = copy.deepcopy(output); value["provenance"]["preprocessing"]["mfcc"]["numcep"] = 20; variants.append(value)
        value = copy.deepcopy(output); value["provenance"]["runtime"]["versions"]["torch"] = "2.9.0"; variants.append(value)
        value = copy.deepcopy(output); value["provenance"]["visual_implementation"]["sha256"] = "0" * 64; variants.append(value)
        value = copy.deepcopy(output); value["observations"]["av_sync"]["state"] = "unknown"; variants.append(value)
        value = copy.deepcopy(output); value["observations"]["frames"].pop(); variants.append(value)
        value = copy.deepcopy(output); value["face_embeddings"] = []; variants.append(value)
        for value in variants:
            with self.subTest(value=value), self.assertRaises(engine.MatchingError):
                engine.validate_output(value, self.value)

    def test_scored_short_small_discontinuous_or_cut_tracks_rejected(self):
        output = self.output()
        face = {"track_id": "face_track_" + "b" * 32, "raw_logit": 2.,
            "face_width_px": 80, "face_height_px": 80, "visible": True, "occluded": False}
        for frame in output["observations"]["frames"]:
            frame["faces"] = [copy.deepcopy(face)]
        engine.validate_output(output, self.value)
        variants = []
        value = copy.deepcopy(output)
        for frame in value["observations"]["frames"][20:]:
            frame["faces"] = []
        variants.append(value)
        value = copy.deepcopy(output); value["observations"]["frames"][20]["faces"] = []; variants.append(value)
        value = copy.deepcopy(output); value["observations"]["frames"][20]["faces"][0]["face_width_px"] = 63; variants.append(value)
        value = copy.deepcopy(output); value["observations"]["frames"][20]["faces"][0]["raw_logit"] = None; variants.append(value)
        value = copy.deepcopy(output); value["observations"]["frames"][20]["shot_id"] = "shot_" + "b" * 32; variants.append(value)
        for value in variants:
            with self.subTest(value=value), self.assertRaises(engine.MatchingError):
                engine.validate_output(value, self.value)

    def test_output_remains_replayable_without_raw_clip_files(self):
        output = self.output()
        self.assertFalse(Path(self.value["audio_pcm"]["path"]).exists())
        self.assertFalse(Path(self.value["video_rgb"]["path"]).exists())
        engine.validate_request(self.value)
        engine.validate_output(output, self.value)

    def test_fixed_imports_ignore_unregistered_package_initializers(self):
        bodies = {"model/Classifier.py": b"class Fusion: pass\nclass Detector: pass\n",
            "model/Encoder.py": b"class visual_encoder: pass\nclass audio_encoder: pass\n",
            "model/Model.py": b"from model.Classifier import Fusion\nfrom model.Encoder import audio_encoder\nclass ASD_Model: pass\n",
            "loss.py": b"class lossAV: pass\nclass lossV: pass\n"}
        admitted = {"files": {}, "witnesses": {}}
        for relative, body in bodies.items():
            ref = self.write(self.root / "isolated" / relative, body, sized=True)
            admitted["files"][relative] = ref
            with engine.safe.opened(ref["path"]) as fd:
                admitted["witnesses"][relative] = engine.safe.witness(fd)
        self.write(self.root / "isolated/model/__init__.py", b"raise RuntimeError('unregistered initializer ran')")
        original_path = list(sys.path)
        with engine._fixed_modules(admitted) as classes:
            self.assertEqual([cls.__name__ for cls in classes], ["ASD_Model", "lossAV", "lossV"])
            self.assertEqual(sys.path, original_path)
        self.assertFalse(set(engine.MODULES) & set(sys.modules))
        self.assertNotIn("model", sys.modules)

    def test_worker_checks_inputs_before_any_inference(self):
        self.write(Path(self.value["audio_pcm"]["path"]), b"short audio")
        admitted = engine.admit_bundle(self.binding)
        runtime = {**engine._runtime_proof(admitted), "_witnesses": {}}
        with mock.patch.object(engine.common, "_offline_environment"), mock.patch.object(engine.common, "_verify_runtime", return_value=runtime), \
                mock.patch.object(engine, "_native_inference") as inference, self.assertRaises(engine.MatchingError):
            engine.run_worker(self.value)
        inference.assert_not_called()

    def test_mocked_worker_verifies_complete_output_without_real_models(self):
        self.value["audio_pcm"] = self.write(self.root / "audio.pcm", bytes(64000), sized=True)
        self.value["video_rgb"] = self.write(self.root / "video.bgr", bytes(50 * 640 * 360 * 3), sized=True)
        self.value["decode_receipt"] = self.json_file("decode.json", timing(self.value))
        admitted = engine.admit_bundle(self.binding)
        runtime = {**engine._runtime_proof(admitted), "_witnesses": {}}
        observations = self.output()["observations"]
        with mock.patch.object(engine.common, "_offline_environment"), mock.patch.object(engine.common, "_verify_runtime", return_value=runtime), \
                mock.patch.object(engine, "_native_inference", return_value=(observations, None)) as inference:
            value = engine.run_worker(self.value)
        inference.assert_called_once()
        self.assertEqual(value["observations"], observations)
        self.assertFalse(value["provenance"]["semantics"]["quality_gate_passed"])

    def test_worker_detects_input_mutation_during_mocked_inference(self):
        self.value["audio_pcm"] = self.write(self.root / "audio.pcm", bytes(64000), sized=True)
        self.value["video_rgb"] = self.write(self.root / "video.bgr", bytes(50 * 640 * 360 * 3), sized=True)
        self.value["decode_receipt"] = self.json_file("decode.json", timing(self.value))
        admitted = engine.admit_bundle(self.binding)
        runtime = {**engine._runtime_proof(admitted), "_witnesses": {}}
        observations = self.output()["observations"]
        def infer(*args):
            self.write(self.root / "audio.pcm", b"x" * 64000)
            return observations, None
        with mock.patch.object(engine.common, "_offline_environment"), mock.patch.object(engine.common, "_verify_runtime", return_value=runtime), \
                mock.patch.object(engine, "_native_inference", side_effect=infer), self.assertRaises(engine.MatchingError):
            engine.run_worker(self.value)


class ModelContractTests(unittest.TestCase):
    def test_mfcc_preserves_int16_scale_and_wraps_only_final_feature_row(self):
        samples = np.arange(16000, dtype=np.int16)
        features = np.arange(99 * 13, dtype=np.float64).reshape(99, 13)
        mfcc = mock.Mock(return_value=features)
        result = engine._features(np, mfcc, samples, 25)
        self.assertEqual(result.shape, (100, 13))
        self.assertEqual(result.dtype, np.float32)
        np.testing.assert_array_equal(result[-1], features[0])
        self.assertIs(mfcc.call_args.args[0], samples)
        self.assertEqual(mfcc.call_args.kwargs["winlen"], .025)
        self.assertEqual(mfcc.call_args.kwargs["winstep"], .010)

    def test_mfcc_rejects_bad_shape_nonfinite_or_wrong_alignment(self):
        for features in (np.zeros((98, 13)), np.zeros((101, 13)), np.zeros((100, 14)), np.full((99, 13), np.nan)):
            with self.subTest(shape=features.shape), self.assertRaises(engine.MatchingError):
                engine._features(np, mock.Mock(return_value=features), np.zeros(16000, dtype=np.int16), 25)

    def test_weight_loader_forces_restricted_deserialization_and_strict_state(self):
        tensor = SimpleNamespace(shape=(2,), dtype="float32", is_sparse=False)
        torch = SimpleNamespace(load=mock.Mock(return_value={"model.weight": tensor}), Tensor=SimpleNamespace,
            isfinite=mock.Mock(return_value=SimpleNamespace(all=lambda: SimpleNamespace(item=lambda: True))))
        wrapper = mock.Mock()
        wrapper.state_dict.return_value = {"model.weight": tensor}
        with mock.patch.dict(os.environ, {"TORCH_FORCE_WEIGHTS_ONLY_LOAD": "1"}):
            engine._load_weights(torch, wrapper, b"synthetic pickle bytes not actually read")
        self.assertEqual(torch.load.call_args.kwargs, {"weights_only": True, "map_location": "cpu"})
        wrapper.load_state_dict.assert_called_once_with({"model.weight": tensor}, strict=True)
        wrapper.eval.assert_called_once()

    def test_loader_has_no_unsafe_fallback_or_partial_key_acceptance(self):
        wrapper = mock.Mock()
        wrapper.state_dict.return_value = {"model.weight": object()}
        for state in ({}, {"module.model.weight": object()}, {"model.weight": object(), "extra": object()}):
            torch = SimpleNamespace(load=mock.Mock(return_value=state))
            with mock.patch.dict(os.environ, {"TORCH_FORCE_WEIGHTS_ONLY_LOAD": "1"}), self.assertRaises(engine.MatchingError):
                engine._load_weights(torch, wrapper, b"unused")
            self.assertEqual(torch.load.call_count, 1)
        with mock.patch.dict(os.environ, {"TORCH_FORCE_WEIGHTS_ONLY_LOAD": "0"}), self.assertRaises(engine.MatchingError):
            engine._load_weights(torch, wrapper, b"unused")

    def test_preimported_model_namespaces_rejected_without_overwrite(self):
        existing = object()
        with mock.patch.dict(sys.modules, {"model": existing}), self.assertRaises(engine.MatchingError):
            with engine._fixed_modules({}):
                self.fail("must not enter")
        self.assertNotIn("model", sys.modules)

    def test_no_face_and_cut_observations_preserve_all_frames(self):
        value = request(Path("/tmp/speaker-face-engine-test"))
        observations = engine._observations(value, {"tracks": [], "cuts": [25]}, {}, {"state": "verified", "offset_ms": 0})
        self.assertEqual(len(observations["frames"]), 50)
        self.assertEqual(len({row["shot_id"] for row in observations["frames"]}), 2)
        with self.assertRaises(engine.MatchingError):
            engine._observations(value, {"tracks": [], "cuts": [25]}, {"some-track": [1.]}, {"state": "verified", "offset_ms": 0})

    def test_anonymous_track_logits_serve_only_corresponding_frames(self):
        value = request(Path("/tmp/speaker-face-engine-test"))
        track_id = "face_track_" + "a" * 32
        track = {"track_id": track_id, "shot_id": visual.shot_id(value["clip"]["clip_id"], 0),
            "frame_indices": list(range(10, 40)), "boxes": [{"width": 80., "height": 75.}] * 30}
        result = engine._observations(value, {"tracks": [track], "cuts": []}, {track_id: [2.5] * 30},
            {"state": "verified", "offset_ms": 0})
        self.assertEqual(result["frames"][9]["faces"], [])
        self.assertEqual(result["frames"][10]["faces"][0]["raw_logit"], 2.5)
        self.assertEqual(result["frames"][40]["faces"], [])

    def test_visible_border_face_is_unscored_competitor_not_silently_omitted(self):
        value = request(Path("/tmp/speaker-face-engine-test"))
        normal_id, border_id = "face_track_" + "a" * 32, "face_track_" + "b" * 32
        shot = visual.shot_id(value["clip"]["clip_id"], 0)
        normal = {"track_id": normal_id, "shot_id": shot, "frame_indices": list(range(50)),
            "boxes": [{"width": 80., "height": 75.}] * 50,
            "crops": np.zeros((50, 112, 112), dtype=np.uint8)}
        border = {"track_id": border_id, "shot_id": shot, "frame_indices": list(range(50)),
            "boxes": [{"width": 80., "height": 75.}] * 50, "crops": None}
        detected = {"tracks": [normal, border], "cuts": []}
        self.assertEqual([track["track_id"] for track in engine._usable_tracks(detected)], [normal_id])
        observations = engine._observations(value, detected, {normal_id: [2.5] * 50},
            {"state": "verified", "offset_ms": 0})
        self.assertEqual(len(observations["frames"][0]["faces"]), 2)
        self.assertIsNone(observations["frames"][0]["faces"][1]["raw_logit"])
        result = core.associate(value["clip"], observations)
        self.assertEqual(result["status"], "unknown")
        self.assertIn("face_quality_or_missing_score", result["reasons"])
        self.assertEqual(engine._usable_tracks({**detected, "cuts": [25]}), [])


if __name__ == "__main__":
    unittest.main()
