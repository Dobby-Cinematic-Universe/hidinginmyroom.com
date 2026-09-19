"""Offline resident-engine contracts; no downloaded models or CUDA required."""

from __future__ import annotations

from contextlib import nullcontext
import copy
import hashlib
import json
import math
from pathlib import Path
import random
import struct
import tempfile
from types import SimpleNamespace
import unittest
from unittest import mock

from pipeline import speaker_screen_accelerated_engine as engine
from pipeline import speaker_screen_engine as cpu
from pipeline.tests import test_speaker_screen_engine as cpu_tests

try:
    import numpy as np
except ImportError:
    np = None


class ResidentEngineTests(unittest.TestCase):
    def setUp(self):
        temporary = tempfile.TemporaryDirectory(prefix="resident-engine-test-")
        self.addCleanup(temporary.cleanup)
        self.root = Path(temporary.name)
        self.config = {"kind": "himr_speaker_screen_models", "schema_version": 1}
        for name in ("silero_vad", "ecapa_embedding"):
            path = self.root / name
            path.write_bytes(("not a real model: " + name).encode())
            path.chmod(0o400)
            self.config[name] = {"path": str(path), "sha256": hashlib.sha256(path.read_bytes()).hexdigest()}

    @staticmethod
    def item(duration=2000, *, index=0, offset=0, value=1234):
        return {"pcm": struct.pack("<h", value) * (duration * 16),
                "window": {"index": index, "start_ms": offset, "end_ms": offset + duration}}

    @staticmethod
    def backend(probabilities=None):
        backend = mock.Mock()
        if probabilities is None:
            backend.probabilities.side_effect = lambda pcm: [0.9] * ((len(pcm) // 2 + 511) // 512)
        else:
            backend.probabilities.return_value = probabilities
        backend.encode_batch.side_effect = lambda items: [[1.0] * 192 for _ in items]
        return backend

    def test_constructor_is_lazy_and_rejects_invalid_bounds(self):
        with mock.patch.object(engine, "_ResidentBackend", side_effect=AssertionError("eager model load")):
            subject = engine.ResidentScreenEngine(self.config)
        self.assertIsNone(subject._backend)
        for keyword, values in {
            "device": (None, [], {}, True, "auto", "cuda:0"),
            "threads": (True, 0, 5, 1.5), "batch_size": (True, 0, 17, 1.5),
            "cuda_memory_fraction": (True, None, "0.5", 0, 0.09, 0.76, float("inf"), float("nan")),
        }.items():
            for value in values:
                with self.subTest(keyword=keyword, value=value), self.assertRaises(cpu.ScreenEngineError):
                    engine.ResidentScreenEngine(self.config, **{keyword: value})
        for device in ([], {}, False):
            with self.assertRaises(cpu.ScreenEngineError):
                engine.model_recipe(device)

    def test_complete_batch_validation_precedes_all_inference(self):
        subject = engine.ResidentScreenEngine(self.config, batch_size=2)
        invalid = [None, {}, {**self.item(), "extra": True},
                   {**self.item(), "pcm": bytearray(self.item()["pcm"])},
                   {**self.item(), "window": {"index": 0, "start_ms": 0, "end_ms": 2000, "extra": 0}}]
        for field, values in {"index": (True, -1, 2**53), "start_ms": (True, -1, 2000),
                              "end_ms": (True, 0, 10001, 2**53)}.items():
            for value in values:
                row = self.item()
                row["window"][field] = value
                invalid.append(row)
        with mock.patch.object(subject, "_load", side_effect=AssertionError("invalid batch reached model")):
            for item in invalid:
                with self.subTest(item=str(item)[:80]), self.assertRaises(cpu.ScreenEngineError):
                    subject.analyze_batch([self.item(), item])
            for items in (None, (), [self.item()] * 3):
                with self.assertRaises(cpu.ScreenEngineError):
                    subject.analyze_batch(items)
        # Invalid caller arguments have not poisoned an untouched model.
        self.assertEqual(subject.analyze_batch([]), [])

    def test_empty_and_zero_batches_are_model_free(self):
        subject = engine.ResidentScreenEngine(self.config)
        with mock.patch.object(subject, "_load", side_effect=AssertionError("silence loaded a model")):
            self.assertEqual(subject.analyze_batch([]), [])
            observed = subject.analyze_batch([self.item(value=0), self.item(index=1, value=0)])
        self.assertEqual([row["speech_ms"] for row in observed], [0, 0])
        self.assertEqual([row["embedding"] for row in observed], [None, None])

    def test_weights_are_hash_admitted_once_and_reused_across_recording_indices(self):
        subject = engine.ResidentScreenEngine(self.config, threads=2, batch_size=4)
        backend = self.backend()
        with mock.patch.object(engine, "_ResidentBackend", return_value=backend) as constructor, \
                mock.patch.object(cpu, "_model_snapshot", wraps=cpu._model_snapshot) as reader:
            before = subject.initialize()
            subject.analyze_batch([self.item(index=0)])
            subject.analyze_batch([self.item(index=0, value=4321), self.item(index=1, value=0)])
            after = subject.initialize()
        self.assertEqual(before, after)
        self.assertEqual(reader.call_count, 2)
        self.assertEqual(constructor.call_count, 1)
        self.assertEqual(constructor.call_args.kwargs,
            {"threads": 2, "device": "cpu", "batch_size": 4, "cuda_memory_fraction": 0.5})
        self.assertEqual(backend.probabilities.call_count, 2)
        self.assertEqual(backend.encode_batch.call_count, 2)

    def test_mismatched_models_fail_before_backend_and_poison_initialization(self):
        config = copy.deepcopy(self.config)
        config["silero_vad"]["sha256"] = "0" * 64
        subject = engine.ResidentScreenEngine(config)
        with mock.patch.object(engine, "_ResidentBackend", side_effect=AssertionError("unverified deserializer")):
            with self.assertRaises(cpu.ScreenEngineError):
                subject.initialize()
            with self.assertRaisesRegex(cpu.ScreenEngineError, "failed previously"):
                subject.analyze_batch([])

    def test_mixed_batch_preserves_order_duplicate_indices_and_input_immutability(self):
        subject = engine.ResidentScreenEngine(self.config)
        items = [self.item(index=7), self.item(index=7, value=0), self.item(index=2, offset=5000, value=987)]
        before = copy.deepcopy(items)
        backend = self.backend()
        backend.encode_batch.side_effect = lambda excerpts: [[1.0, 0.0] + [0.0] * 190,
                                                             [0.0, 1.0] + [0.0] * 190]
        with mock.patch.object(subject, "_load", return_value=backend):
            observed = subject.analyze_batch(items)
        self.assertEqual(items, before)
        self.assertEqual([row["index"] for row in observed], [7, 7, 2])
        self.assertEqual(observed[0]["embedding"][:2], [1.0, 0.0])
        self.assertIsNone(observed[1]["embedding"])
        self.assertEqual(observed[2]["embedding"][:2], [0.0, 1.0])
        self.assertEqual(observed[2]["start_ms"], 5000)

    def test_selection_and_normalization_match_cpu_singleton_exactly(self):
        generator = random.Random(81425)
        for trial in range(100):
            duration = generator.randint(1, 10000)
            count = (duration * 16 + 511) // 512
            # Runs deliberately include both eligible excerpts and short/disjoint speech.
            probabilities = []
            while len(probabilities) < count:
                probabilities.extend([generator.choice((0.0, 0.49, 0.5, 0.9))] * generator.randint(1, 200))
            probabilities = probabilities[:count]
            vector = [generator.uniform(-100, 100) for _ in range(192)]
            item = self.item(duration, index=trial, offset=trial * 20000)
            old_backend = mock.Mock()
            old_backend.probabilities.return_value = probabilities
            old_backend.encode.return_value = vector
            new_backend = self.backend(probabilities)
            new_backend.encode_batch.side_effect = lambda excerpts: [vector for _ in excerpts]
            original = cpu.CpuScreenEngine(self.config)
            resident = engine.ResidentScreenEngine(self.config)
            with mock.patch.object(original, "_load", return_value=old_backend), \
                    mock.patch.object(resident, "_load", return_value=new_backend):
                expected = original.analyze(item["pcm"], **item["window"])
                actual = resident.analyze_batch([item])[0]
            self.assertEqual(actual, expected, f"trial {trial}")
            if expected["embedding"] is not None:
                self.assertEqual(new_backend.encode_batch.call_args.args[0], [old_backend.encode.call_args.args[0]])

    def test_probability_embedding_and_backend_failures_never_return_partial_batches(self):
        scenarios = [([], None), ([0.9] * 62 + [True], None), ([0.9] * 62 + [float("nan")], None),
                     ([0.9] * 63, []), ([0.9] * 63, [[1.0] * 191]),
                     ([0.9] * 63, [[0.0] * 192]), ([0.9] * 63, [[1.0] * 191 + [float("inf")]])]
        for probabilities, vectors in scenarios:
            subject = engine.ResidentScreenEngine(self.config)
            backend = self.backend(probabilities)
            if vectors is not None:
                backend.encode_batch.side_effect = None
                backend.encode_batch.return_value = vectors
            with mock.patch.object(subject, "_load", return_value=backend):
                with self.assertRaises(cpu.ScreenEngineError):
                    subject.analyze_batch([self.item()])
                with self.assertRaisesRegex(cpu.ScreenEngineError, "failed previously"):
                    subject.analyze_batch([self.item(value=0)])
        subject = engine.ResidentScreenEngine(self.config, device="cuda")
        with mock.patch.object(subject, "_load", side_effect=RuntimeError("CUDA out of memory")) as load:
            with self.assertRaisesRegex(cpu.ScreenEngineError, "no observations"):
                subject.analyze_batch([self.item()])
            self.assertEqual(load.call_count, 1)  # No implicit CPU fallback.

    def test_provenance_is_finite_private_recipe_distinct_and_not_a_mutable_alias(self):
        subject = engine.ResidentScreenEngine(self.config)
        provenance = subject.provenance()
        json.dumps(provenance, allow_nan=False)
        self.assertNotIn("models_loaded", provenance)
        self.assertNotIn("failed", provenance)
        self.assertEqual(provenance["recipe"]["identity_authority"], "none")
        self.assertFalse(provenance["recipe"]["screening_calibrated"])
        self.assertFalse(provenance["recipe"]["runtime_benchmarked"])
        self.assertNotEqual(provenance["recipe"]["recipe_id"], cpu.MODEL_RECIPE["recipe_id"])
        self.assertNotEqual(provenance["recipe"]["recipe_id"], engine.model_recipe("cuda")["recipe_id"])
        provenance["recipe"]["embedding"]["channels"][0] = -1
        self.assertEqual(subject.provenance()["recipe"]["embedding"]["channels"][0], 1024)

    def test_cpu_branch_calls_original_guard_without_changing_it(self):
        original = cpu._runtime_imports
        with mock.patch.object(cpu._LocalBackend, "__init__", return_value=None) as constructor:
            engine._ResidentBackend(b"vad", b"embedding", threads=1, device="cpu", batch_size=8, cuda_memory_fraction=0.5)
        constructor.assert_called_once_with(b"vad", b"embedding", threads=1)
        self.assertIs(cpu._runtime_imports, original)

    def test_cuda_dependency_mismatch_rejected_before_imports(self):
        with mock.patch.object(cpu, "runtime_versions", return_value=cpu.RUNTIME_PINS), \
                self.assertRaisesRegex(cpu.ScreenEngineError, "reviewed isolated"):
            engine._cuda_runtime_imports(1, 0.5)


class CudaControlTests(unittest.TestCase):
    def runtime(self):
        return SimpleNamespace(version=SimpleNamespace(cuda="12.8", hip=None),
            cuda=SimpleNamespace(is_available=lambda: True, set_device=mock.Mock(),
                get_device_properties=lambda index: SimpleNamespace(total_memory=6 * 1024**3, name="Fake GPU", major=8, minor=6),
                set_per_process_memory_fraction=mock.Mock(), mem_get_info=lambda index: (5 * 1024**3, 6 * 1024**3)),
            set_float32_matmul_precision=mock.Mock(), use_deterministic_algorithms=mock.Mock(),
            backends=SimpleNamespace(cuda=SimpleNamespace(matmul=SimpleNamespace()), cudnn=SimpleNamespace()))

    def test_allocator_controls_and_float32_are_explicit_before_models(self):
        runtime = self.runtime()
        info = engine._configure_cuda(runtime, 0.5)
        runtime.cuda.set_device.assert_called_once_with(0)
        runtime.cuda.set_per_process_memory_fraction.assert_called_once_with(0.5, device=0)
        self.assertEqual(info["torch_allocator_limit_bytes"], 3 * 1024**3)
        self.assertFalse(runtime.backends.cuda.matmul.allow_tf32)
        self.assertFalse(runtime.backends.cudnn.allow_tf32)
        self.assertFalse(runtime.backends.cudnn.benchmark)
        self.assertTrue(runtime.backends.cudnn.deterministic)
        runtime.set_float32_matmul_precision.assert_called_once_with("highest")
        runtime.use_deterministic_algorithms.assert_called_once_with(True)

    def test_unavailable_wrong_runtime_and_insufficient_memory_fail_closed(self):
        for edit in (
            lambda runtime: setattr(runtime.version, "cuda", "12.6"),
            lambda runtime: setattr(runtime.version, "hip", "anything"),
            lambda runtime: setattr(runtime.cuda, "is_available", lambda: False),
            lambda runtime: setattr(runtime.cuda, "mem_get_info", lambda index: (100 * 1024**2, 6 * 1024**3)),
            lambda runtime: setattr(runtime.cuda, "mem_get_info", lambda index: (7 * 1024**3, 6 * 1024**3)),
            lambda runtime: setattr(runtime.cuda, "get_device_properties", lambda index: SimpleNamespace(total_memory=True)),
        ):
            runtime = self.runtime()
            edit(runtime)
            with self.assertRaises(cpu.ScreenEngineError):
                engine._configure_cuda(runtime, 0.5)


@unittest.skipUnless(np is not None, "CUDA constructor boundary tests use local NumPy and fake Torch")
class CudaConstructorTests(unittest.TestCase):
    def setUp(self):
        self.fixture = cpu_tests.LocalBackendBoundaryTests()
        self.fixture.setUp()
        self.runtime = self.fixture.runtime
        self.embedding = mock.Mock()
        self.embedding.to.return_value = self.embedding
        self.runtime.ECAPA_TDNN = mock.Mock(return_value=self.embedding)

    def load(self):
        with mock.patch.object(engine, "_cuda_runtime_imports", return_value=self.runtime):
            return engine._ResidentBackend(b"exact verified ONNX", b"exact verified ECAPA", threads=2,
                                          device="cuda", batch_size=8, cuda_memory_fraction=0.5)

    def test_cuda_load_is_weights_only_cpu_strict_and_only_embedding_moves_to_gpu(self):
        self.load()
        load_call = self.runtime.torch.load.call_args
        self.assertEqual(load_call.args[0].getvalue(), b"exact verified ECAPA")
        self.assertEqual(load_call.kwargs, {"weights_only": True, "map_location": "cpu"})
        self.assertEqual(self.runtime.ort.InferenceSession.call_args.args, (b"exact verified ONNX",))
        self.assertEqual(self.runtime.ort.InferenceSession.call_args.kwargs["providers"], ["CPUExecutionProvider"])
        self.embedding.load_state_dict.assert_called_once_with(self.runtime.torch.load.return_value, strict=True)
        self.embedding.requires_grad_.assert_called_once_with(False)
        self.assertEqual(self.embedding.to.call_args_list,
                         [mock.call("cpu"), mock.call(device="cuda:0", dtype=np.float32)])
        self.assertEqual(self.runtime.ECAPA_TDNN.call_args.kwargs["channels"], [1024, 1024, 1024, 1024, 3072])

    def test_cuda_backend_reuses_exact_resetting_cpu_vad_and_never_counts_frame_padding(self):
        backend = self.load()
        pcm16 = struct.pack("<h", 1234) * 520
        self.assertEqual(backend.probabilities(pcm16), [float(np.float32(0.8))] * 2)
        self.assertEqual(backend.probabilities(pcm16), [float(np.float32(0.8))] * 2)
        frames = self.fixture.frames
        np.testing.assert_array_equal(frames[0]["state"], 0)
        np.testing.assert_array_equal(frames[2]["state"], 0)
        np.testing.assert_array_equal(frames[2]["input"][:, :64], 0)
        np.testing.assert_array_equal(frames[1]["input"][:, 64 + 8:], 0)

    def test_unsafe_state_or_non_cpu_vad_does_not_move_any_embedding_to_cuda(self):
        for state in ({}, {"bad": "not a tensor"}, {"bad": self.fixture.Tensor([float("nan")])}):
            self.runtime.torch.load.return_value = state
            self.embedding.to.reset_mock()
            with self.assertRaises(cpu.ScreenEngineError):
                self.load()
            self.assertNotIn(mock.call(device="cuda:0", dtype=np.float32), self.embedding.to.call_args_list)
        self.fixture.session.get_providers.return_value = ["CUDAExecutionProvider", "CPUExecutionProvider"]
        with self.assertRaisesRegex(cpu.ScreenEngineError, "non-CPU"):
            self.load()


@unittest.skipUnless(np is not None, "batch tensor tests need only local NumPy")
class BatchFeatureTests(unittest.TestCase):
    def setUp(self):
        class Tensor:
            def __init__(self, value, device="cpu"):
                self.value = np.asarray(value)
                self.device = SimpleNamespace(type=device.split(":")[0])
            @property
            def shape(self): return self.value.shape
            @property
            def dtype(self): return self.value.dtype
            def to(self, *, device, dtype): return Tensor(self.value.astype(dtype), device)
            def unsqueeze(self, dimension): return Tensor(np.expand_dims(self.value, dimension))
            def all(self): return Tensor(self.value.all())
            def item(self): return self.value.item()
            def detach(self): return self
            def reshape(self, *shape): return Tensor(self.value.reshape(*shape), self.device.type)
            def tolist(self): return self.value.tolist()
        self.Tensor = Tensor
        self.calls, self.normalizations = [], []
        def features(audio):
            count = audio.shape[1] // 160 + 1
            return Tensor(np.full((1, count, 80), audio.value[0, 0], dtype=np.float32))
        def normalize(value, lengths):
            self.normalizations.append(value.shape)
            return value
        def embedding(value, lengths):
            self.calls.append(value.shape)
            result = np.zeros((value.shape[0], 1, 192), dtype=np.float32)
            result[:, 0, 0] = value.value[:, 0, 0]
            result[:, 0, 1] = 1
            return Tensor(result, value.device.type)
        self.backend = object.__new__(engine._ResidentBackend)
        self.backend.device, self.backend.batch_size = "cpu", 8
        self.backend.features, self.backend.normalization, self.backend.embedding = features, normalize, embedding
        self.backend.runtime = SimpleNamespace(np=np, torch=SimpleNamespace(
            inference_mode=nullcontext, autocast=lambda **kwargs: nullcontext(), float32=np.float32, from_numpy=Tensor,
            ones=lambda count, *, device, dtype: Tensor(np.ones(count, dtype=dtype), device),
            cat=lambda tensors, *, dim: Tensor(np.concatenate([item.value for item in tensors], axis=dim)),
            isfinite=lambda tensor: Tensor(np.isfinite(tensor.value))))

    def test_equal_feature_groups_are_batched_without_padding_and_order_is_restored(self):
        items = [ResidentEngineTests.item(duration, value=value)["pcm"]
                 for duration, value in ((2000, 1000), (5000, 2000), (2000, 3000), (3000, 4000))]
        result = self.backend.encode_batch(items)
        self.assertEqual(self.calls, [(2, 201, 80), (1, 501, 80), (1, 301, 80)])
        self.assertEqual(self.normalizations, [(1, 201, 80), (1, 501, 80), (1, 201, 80), (1, 301, 80)])
        self.assertEqual([row[0] for row in result], [1000 / 32768, 2000 / 32768, 3000 / 32768, 4000 / 32768])
        # Fresh call cannot retain previous feature/embedding rows.
        self.assertEqual(len(self.backend.encode_batch([items[0]])), 1)
        self.assertEqual(self.calls[-1], (1, 201, 80))

    def test_singleton_features_and_result_match_unbatched_cpu_path(self):
        pcm16 = ResidentEngineTests.item()["pcm"]
        expected = cpu._LocalBackend.encode(self.backend, pcm16)
        actual = self.backend.encode_batch([pcm16])[0]
        self.assertEqual(actual, expected)

    def test_feature_bounds_and_batch_bounds_are_checked(self):
        for excerpts in ([], (), [b"x"], [b"x" * (5001 * 32)], [b"x" * (2000 * 32)] * 9):
            with self.assertRaises(cpu.ScreenEngineError):
                self.backend.encode_batch(excerpts)
        self.backend.features = lambda audio: self.Tensor(np.zeros((1, 503, 80), dtype=np.float32))
        with self.assertRaisesRegex(cpu.ScreenEngineError, "feature extraction"):
            self.backend.encode_batch([ResidentEngineTests.item()["pcm"]])


if __name__ == "__main__":
    unittest.main()
