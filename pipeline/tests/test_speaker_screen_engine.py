"""Synthetic/offline model-boundary tests; never load real speaker weights."""

from __future__ import annotations

from contextlib import nullcontext
import copy
import hashlib
import io
import math
import os
from pathlib import Path
import struct
import tempfile
from types import SimpleNamespace
import unittest
from unittest import mock

from pipeline import speaker_screen_engine as engine

try:
    import numpy as np
except ImportError:
    np = None


class SpeakerScreenEngineTests(unittest.TestCase):
    def setUp(self):
        temporary = tempfile.TemporaryDirectory(prefix="speaker-engine-test-")
        self.addCleanup(temporary.cleanup)
        self.root = Path(temporary.name)
        self.config = {"kind": "himr_speaker_screen_models", "schema_version": 1}
        for name in ("silero_vad", "ecapa_embedding"):
            path = self.root / (name + ".fixture")
            path.write_bytes(("synthetic non-model " + name).encode())
            path.chmod(0o400)
            self.config[name] = {"path": str(path), "sha256": hashlib.sha256(path.read_bytes()).hexdigest()}

    @staticmethod
    def pcm(milliseconds, value=1000):
        return struct.pack("<h", value) * (milliseconds * 16)

    def fake(self, probabilities, vector=None):
        backend = mock.Mock()
        backend.probabilities.return_value = probabilities
        backend.encode.return_value = [1.0] * 192 if vector is None else vector
        return backend

    def analyze(self, milliseconds, probabilities, *, offset=0, vector=None):
        subject = engine.CpuScreenEngine(self.config)
        backend = self.fake(probabilities, vector)
        with mock.patch.object(subject, "_load", return_value=backend):
            result = subject.analyze(self.pcm(milliseconds), start_ms=offset, end_ms=offset + milliseconds, index=3)
        return result, backend

    def test_configuration_and_constructor_are_metadata_only_and_return_independent_copy(self):
        with mock.patch.object(engine.os, "open", side_effect=AssertionError("no model opens")), \
                mock.patch.object(engine, "_runtime_imports", side_effect=AssertionError("no ML imports")):
            validated = engine.validate_model_config(self.config)
            subject = engine.CpuScreenEngine(self.config, threads=2)
        validated["silero_vad"]["path"] = "/changed"
        self.assertEqual(subject.model_config, self.config)
        self.assertIsNone(subject._backend)

    def test_config_paths_exact_fields_hashes_and_thread_types_fail_closed(self):
        edits = (
            lambda value: value.update(schema_version=True),
            lambda value: value.update(extra="not admitted"),
            lambda value: value["silero_vad"].update(url="https://example.invalid/model"),
            lambda value: value["silero_vad"].update(sha256="A" * 64),
            lambda value: value["silero_vad"].update(path="https://example.invalid/model"),
            lambda value: value["silero_vad"].update(path="/private/../model"),
            lambda value: value["silero_vad"].update(path="/private//model"),
            lambda value: value["silero_vad"].update(path="/private/model\n"),
            lambda value: value["ecapa_embedding"].update(path=value["silero_vad"]["path"]),
        )
        for edit in edits:
            value = copy.deepcopy(self.config)
            edit(value)
            with self.subTest(config=value), self.assertRaises(engine.ScreenEngineError):
                engine.validate_model_config(value)
        for threads in (True, 0, 5, 1.5):
            with self.subTest(threads=threads), self.assertRaises(engine.ScreenEngineError):
                engine.CpuScreenEngine(self.config, threads=threads)

    def test_digital_silence_is_model_free_not_a_fabricated_embedding(self):
        subject = engine.CpuScreenEngine(self.config)
        with mock.patch.object(subject, "_load", side_effect=AssertionError("digital zero needs no model")):
            result = subject.analyze(self.pcm(10000, 0), start_ms=5000, end_ms=15000, index=0)
        self.assertEqual(result, {"index": 0, "start_ms": 5000, "end_ms": 15000, "speech_ms": 0, "embedding": None})

    def test_pcm_byte_count_duration_and_coordinate_bounds(self):
        subject = engine.CpuScreenEngine(self.config)
        rows = (
            (b"", 0, 1000, 0), (self.pcm(1000)[:-1], 0, 1000, 0),
            (self.pcm(10001), 0, 10001, 0), (self.pcm(1000), -1, 999, 0),
            (self.pcm(1000), True, 1001, 0), (self.pcm(1000), 0, 1000, True),
            (self.pcm(1000), 0, 1000, -1), (bytearray(self.pcm(1000)), 0, 1000, 0),
        )
        with mock.patch.object(subject, "_load", side_effect=AssertionError("invalid PCM reached inference")):
            for pcm, begin, end, index in rows:
                with self.subTest(length=len(pcm), begin=begin, end=end, index=index), self.assertRaises(engine.ScreenEngineError):
                    subject.analyze(pcm, start_ms=begin, end_ms=end, index=index)

    def test_longest_contiguous_excerpt_has_actual_bounds_and_five_second_cap(self):
        # 64 ms quiet, 2.016 s speech, 32 ms quiet, then 7.888 s speech.
        probabilities = [0.1] * 2 + [0.9] * 63 + [0.1] + [0.9] * (313 - 66)
        result, backend = self.analyze(10000, probabilities, offset=100000)
        self.assertEqual(result["start_ms"], 102112)
        self.assertEqual(result["end_ms"], 107112)
        self.assertEqual(result["speech_ms"], 5000)
        self.assertEqual(len(backend.encode.call_args.args[0]), 5000 * 32)
        self.assertEqual(len(result["embedding"]), 192)
        self.assertAlmostEqual(math.hypot(*result["embedding"]), 1.0, places=12)

    def test_disjoint_short_speech_is_not_concatenated_to_make_an_embedding(self):
        probabilities = [0.9] * 32 + [0.1] + [0.9] * 32 + [0.1] * (125 - 65)
        result, backend = self.analyze(4000, probabilities, offset=40000)
        self.assertEqual(result["speech_ms"], 2048)
        self.assertEqual((result["start_ms"], result["end_ms"]), (40000, 44000))
        self.assertIsNone(result["embedding"])
        backend.encode.assert_not_called()

    def test_equal_length_runs_choose_earliest_and_two_second_boundary_is_exact(self):
        result, _ = self.analyze(5000, [0.9] * 63 + [0.1] + [0.9] * 63 + [0.1] * (157 - 127))
        self.assertEqual((result["start_ms"], result["end_ms"]), (0, 2016))
        for milliseconds, eligible in ((1999, False), (2000, True)):
            frames = (milliseconds * 16 + 511) // 512
            result, backend = self.analyze(milliseconds, [0.9] * frames)
            self.assertEqual(result["speech_ms"], milliseconds)
            self.assertEqual(result["embedding"] is not None, eligible)
            self.assertEqual(backend.encode.call_count, int(eligible))

    def test_no_speech_and_partial_frame_never_count_zero_padding(self):
        result, backend = self.analyze(10, [0.8])
        self.assertEqual(result["speech_ms"], 10)
        self.assertIsNone(result["embedding"])
        backend.encode.assert_not_called()
        result, _ = self.analyze(1000, [0.49] * 32)
        self.assertEqual(result["speech_ms"], 0)

    def test_invalid_probabilities_and_embeddings_fail_without_observation(self):
        for probabilities in ([], [0.9] * 62, [0.9] * 62 + [True], [0.9] * 62 + [float("nan")],
                              [0.9] * 62 + [float("inf")], [0.9] * 62 + [1.01]):
            with self.subTest(probabilities=probabilities[-2:]), self.assertRaises(engine.ScreenEngineError):
                self.analyze(2000, probabilities)
        for vector in ([0.0] * 192, [1.0] * 191, [1.0] * 191 + [float("nan")],
                       [1.0] * 191 + [True], [float("inf")] * 192):
            with self.subTest(vector=vector[-2:]), self.assertRaises(engine.ScreenEngineError):
                self.analyze(2000, [0.9] * 63, vector=vector)

    def test_models_are_hash_verified_once_and_backend_receives_exact_snapshots(self):
        backend = self.fake([0.9] * 63)
        subject = engine.CpuScreenEngine(self.config, threads=2)
        original_reader = engine._model_snapshot
        with mock.patch.object(engine, "_model_snapshot", wraps=original_reader) as reader, \
                mock.patch.object(engine, "_LocalBackend", return_value=backend) as constructor:
            first = subject.analyze(self.pcm(2000), start_ms=0, end_ms=2000, index=0)
            second = subject.analyze(self.pcm(2000), start_ms=2000, end_ms=4000, index=1)
        self.assertIsNotNone(first["embedding"])
        self.assertIsNotNone(second["embedding"])
        self.assertEqual(reader.call_count, 2)
        constructor.assert_called_once()
        self.assertEqual(constructor.call_args.args, tuple(Path(self.config[name]["path"]).read_bytes()
                                                         for name in ("silero_vad", "ecapa_embedding")))
        self.assertEqual(constructor.call_args.kwargs, {"threads": 2})

    def test_mismatched_or_unsafe_model_never_reaches_deserializer(self):
        wrong = copy.deepcopy(self.config)
        wrong["silero_vad"]["sha256"] = "0" * 64
        with mock.patch.object(engine, "_LocalBackend", side_effect=AssertionError("must not deserialize")), \
                self.assertRaises(engine.ScreenEngineError):
            engine.CpuScreenEngine(wrong).analyze(self.pcm(2000), start_ms=0, end_ms=2000, index=0)
        path = Path(self.config["silero_vad"]["path"])
        with self.assertRaises(engine.ScreenEngineError):
            engine._model_snapshot(self.config["silero_vad"], 1)
        path.chmod(0o666)
        with self.assertRaises(engine.ScreenEngineError):
            engine._model_snapshot(self.config["silero_vad"], 1024)
        path.chmod(0o400)
        alias = self.root / "aliased-model"
        os.link(path, alias)
        with self.assertRaises(engine.ScreenEngineError):
            engine._model_snapshot(self.config["silero_vad"], 1024)
        alias.unlink()
        path.rename(alias)
        path.symlink_to(alias)
        with self.assertRaises(engine.ScreenEngineError):
            engine._model_snapshot(self.config["silero_vad"], 1024)

    def test_model_ancestor_symlinks_and_mutation_while_reading_are_rejected(self):
        directory = self.root / "models"
        directory.mkdir(mode=0o700)
        path = directory / "model"
        path.write_bytes(b"synthetic model")
        path.chmod(0o400)
        link = self.root / "symlinked-models"
        link.symlink_to(directory, target_is_directory=True)
        binding = {"path": str(link / "model"), "sha256": hashlib.sha256(path.read_bytes()).hexdigest()}
        with self.assertRaises(engine.ScreenEngineError):
            engine._model_snapshot(binding, 1024)
        binding["path"] = str(path)
        original_read = os.read
        def mutate(descriptor, size):
            value = original_read(descriptor, size)
            if value:
                path.chmod(0o600)
                path.write_bytes(b"different model")
            return value
        with mock.patch.object(engine.os, "read", side_effect=mutate), self.assertRaises(engine.ScreenEngineError):
            engine._model_snapshot(binding, 1024)

    def test_runtime_metadata_is_lazy_and_unreviewed_runtime_fails_before_framework_import(self):
        def version(name):
            if name == "torch":
                raise engine.metadata.PackageNotFoundError(name)
            return engine.RUNTIME_PINS[name]
        with mock.patch.object(engine.metadata, "version", side_effect=version):
            versions = engine.runtime_versions()
        self.assertIsNone(versions["torch"])
        with mock.patch.dict(os.environ, {}, clear=False), \
                mock.patch.object(engine, "runtime_versions", return_value=versions), \
                self.assertRaisesRegex(engine.ScreenEngineError, "isolated runtime"):
            engine._runtime_imports(1)

    def test_provenance_is_not_a_live_benchmark_or_mutable_recipe_alias(self):
        subject = engine.CpuScreenEngine(self.config)
        result = subject.provenance()
        self.assertFalse(result["models_loaded"])
        self.assertFalse(result["recipe"]["runtime_benchmarked"])
        self.assertEqual(result["recipe"]["identity_authority"], "none")
        result["recipe"]["embedding"]["channels"][0] = -1
        self.assertEqual(engine.MODEL_RECIPE["embedding"]["channels"][0], 1024)


@unittest.skipUnless(np is not None, "numeric boundary tests use local NumPy; no models or Torch are needed")
class LocalBackendBoundaryTests(unittest.TestCase):
    def setUp(self):
        class Tensor:
            def __init__(self, value):
                self.value = np.asarray(value)
                self.device = SimpleNamespace(type="cpu")
                self.layout = "strided"
            @property
            def shape(self): return self.value.shape
            def numel(self): return self.value.size
            def all(self): return Tensor(self.value.all())
            def item(self): return self.value.item()
            def to(self, *args, **kwargs): return self
            def unsqueeze(self, dimension): return Tensor(np.expand_dims(self.value, dimension))
            def detach(self): return self
            def reshape(self, *shape): return Tensor(self.value.reshape(*shape))
            def tolist(self): return self.value.tolist()
        self.Tensor = Tensor
        self.layers = []
        class Layer:
            def __init__(inner, kind):
                inner.kind = kind
                inner.loaded = None
                self.layers.append(inner)
            def to(inner, device):
                self.assertEqual(device, "cpu")
                return inner
            def eval(inner): return inner
            def requires_grad_(inner, value): self.assertFalse(value)
            def load_state_dict(inner, value, *, strict):
                self.assertTrue(strict)
                inner.loaded = value
            def __call__(inner, *args):
                if inner.kind == "features": return Tensor(np.ones((1, 5, 80), dtype=np.float32))
                if inner.kind == "normalize": return args[0]
                return Tensor(np.ones((1, 1, 192), dtype=np.float32))
        self.options = SimpleNamespace(add_session_config_entry=mock.Mock())
        self.session = mock.Mock()
        self.session.get_providers.return_value = ["CPUExecutionProvider"]
        self.session.get_inputs.return_value = [SimpleNamespace(name=name, type=dtype, shape=shape)
            for name, dtype, shape in (("input", "tensor(float)", [None, None]),
                                      ("state", "tensor(float)", [2, None, 128]), ("sr", "tensor(int64)", []))]
        self.session.get_outputs.return_value = [SimpleNamespace(name=name, type="tensor(float)") for name in ("output", "stateN")]
        self.frames = []
        def vad(_names, inputs):
            self.frames.append({key: value.copy() for key, value in inputs.items()})
            return np.array([[0.8]], dtype=np.float32), inputs["state"] + np.float32(1)
        self.session.run.side_effect = vad
        self.runtime = SimpleNamespace(
            np=np,
            ort=SimpleNamespace(SessionOptions=mock.Mock(return_value=self.options),
                ExecutionMode=SimpleNamespace(ORT_SEQUENTIAL="sequential"),
                InferenceSession=mock.Mock(return_value=self.session)),
            torch=SimpleNamespace(load=mock.Mock(return_value={"synthetic.weight": Tensor([1.0])}),
                is_tensor=lambda value: isinstance(value, Tensor), strided="strided", float32=np.float32,
                isfinite=lambda value: Tensor(np.isfinite(value.value)), inference_mode=nullcontext,
                from_numpy=Tensor, ones=lambda *args, **kwargs: Tensor(np.ones(args))),
            Fbank=mock.Mock(side_effect=lambda **_kwargs: Layer("features")),
            InputNormalization=mock.Mock(side_effect=lambda **_kwargs: Layer("normalize")),
            ECAPA_TDNN=mock.Mock(side_effect=lambda **_kwargs: Layer("embedding")),
            versions=dict(engine.RUNTIME_PINS),
        )

    def load(self):
        with mock.patch.object(engine, "_runtime_imports", return_value=self.runtime):
            return engine._LocalBackend(b"verified ONNX snapshot", b"verified ECAPA snapshot", threads=2)

    def test_cpu_only_bytes_load_weights_only_and_exact_architecture(self):
        backend = self.load()
        call = self.runtime.ort.InferenceSession.call_args
        self.assertEqual(call.args, (b"verified ONNX snapshot",))
        self.assertEqual(call.kwargs["providers"], ["CPUExecutionProvider"])
        self.assertEqual(self.options.intra_op_num_threads, 2)
        self.assertEqual(self.options.inter_op_num_threads, 1)
        self.assertEqual(self.options.execution_mode, "sequential")
        call = self.runtime.torch.load.call_args
        self.assertIsInstance(call.args[0], io.BytesIO)
        self.assertEqual(call.args[0].getvalue(), b"verified ECAPA snapshot")
        self.assertEqual(call.kwargs, {"weights_only": True, "map_location": "cpu"})
        self.assertEqual(self.runtime.Fbank.call_args.kwargs["n_mels"], 80)
        self.assertEqual(self.runtime.InputNormalization.call_args.kwargs, {"norm_type": "sentence", "std_norm": False})
        architecture = self.runtime.ECAPA_TDNN.call_args.kwargs
        self.assertEqual(architecture["channels"], [1024, 1024, 1024, 1024, 3072])
        self.assertEqual(architecture["lin_neurons"], 192)
        self.assertEqual(len(backend.encode(struct.pack("<h", 1234) * 32000)), 192)

    def test_silero_512_frame_64_context_state_reset_and_final_zero_padding(self):
        backend = self.load()
        pcm = struct.pack("<h", 32767) * 1032
        self.assertEqual(len(backend.probabilities(pcm)), 3)
        self.assertEqual(self.frames[0]["input"].shape, (1, 576))
        self.assertEqual(self.frames[0]["input"].dtype, np.float32)
        self.assertEqual(self.frames[0]["state"].shape, (2, 1, 128))
        self.assertEqual(self.frames[0]["sr"].dtype, np.int64)
        self.assertEqual(self.frames[0]["sr"].shape, ())
        np.testing.assert_array_equal(self.frames[0]["input"][:, :64], 0)
        np.testing.assert_array_equal(self.frames[1]["input"][:, :64], self.frames[0]["input"][:, -64:])
        np.testing.assert_array_equal(self.frames[1]["state"], 1)
        np.testing.assert_array_equal(self.frames[2]["input"][:, 64 + 8:], 0)
        self.assertGreater(float(self.frames[0]["input"][0, 64]), 0.99)
        backend.probabilities(pcm)
        np.testing.assert_array_equal(self.frames[3]["state"], 0)
        np.testing.assert_array_equal(self.frames[3]["input"][:, :64], 0)

    def test_non_cpu_provider_or_wrong_onnx_schema_is_rejected(self):
        self.session.get_providers.return_value = ["CUDAExecutionProvider", "CPUExecutionProvider"]
        with self.assertRaises(engine.ScreenEngineError):
            self.load()
        self.session.get_providers.return_value = ["CPUExecutionProvider"]
        self.session.get_inputs.return_value[0].name = "unreviewed_input"
        with self.assertRaises(engine.ScreenEngineError):
            self.load()

    def test_non_tensor_or_nonfinite_checkpoint_and_invalid_vad_outputs_rejected(self):
        for state in ({"bad": "arbitrary object"}, {"weight": self.Tensor([float("nan")])}, {}):
            self.runtime.torch.load.return_value = state
            with self.subTest(state=state), self.assertRaises(engine.ScreenEngineError):
                self.load()
        self.runtime.torch.load.return_value = {"weight": self.Tensor([1.0])}
        backend = self.load()
        for output, state in ((np.array([[float("nan")]], dtype=np.float32), np.zeros((2, 1, 128), dtype=np.float32)),
                              (np.array([[0.9]], dtype=np.float32), np.zeros((1, 128), dtype=np.float32)),
                              (np.array([[0.9]], dtype=np.float64), np.zeros((2, 1, 128), dtype=np.float32))):
            self.session.run.side_effect = lambda *_args: (output, state)
            with self.subTest(output=output, state_shape=state.shape), self.assertRaises(engine.ScreenEngineError):
                backend.probabilities(struct.pack("<h", 1) * 512)


if __name__ == "__main__":
    unittest.main()
