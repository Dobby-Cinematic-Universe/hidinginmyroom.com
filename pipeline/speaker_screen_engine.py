"""Local CPU Silero-VAD/ECAPA screening; never a person-identity decision.

Import and configuration validation use only the standard library. Run inference
only in the caller's isolated, network-denied, time/memory-bounded worker. No
remote model loader, YAML interpreter, audio decoder, or CUDA path is used here.
The caller owns source admission, decoding, checkpoints, and process deadlines.

Architecture references (inspected 2026-09-07):
https://huggingface.co/speechbrain/spkrec-ecapa-voxceleb/raw/main/hyperparams.yaml
https://github.com/speechbrain/speechbrain/blob/v1.0.3/speechbrain/inference/classifiers.py
https://github.com/snakers4/silero-vad/blob/master/src/silero_vad/utils_vad.py
The supplied artifact SHA-256, not a mutable upstream URL/name, binds execution.
"""

from __future__ import annotations

import copy
import hashlib
from importlib import metadata
import io
import math
import os
from pathlib import Path
import re
import stat
from types import SimpleNamespace


SAMPLE_RATE = 16000
FRAME_SAMPLES = 512
CONTEXT_SAMPLES = 64
MAX_PROBE_MS = 10000
MIN_EMBEDDING_MS = 2000
MAX_EMBEDDING_MS = 5000
EMBEDDING_DIMENSIONS = 192
MAX_MODEL_BYTES = {"silero_vad": 32 * 1024**2, "ecapa_embedding": 128 * 1024**2}
RUNTIME_PINS = {"numpy": "2.2.6", "torch": "2.8.0+cpu", "torchaudio": "2.8.0+cpu",
                "speechbrain": "1.0.3", "onnxruntime": "1.22.1"}
MODEL_RECIPE = {
    "recipe_id": "silero_onnx_ecapa_voxceleb_cpu_v1", "sample_rate_hz": SAMPLE_RATE,
    "pcm_format": "signed_16_bit_little_endian_mono", "max_probe_ms": MAX_PROBE_MS,
    "vad": {"family": "silero_onnx_stateful_16k", "frame_samples": FRAME_SAMPLES,
            "context_samples": CONTEXT_SAMPLES, "state_shape": [2, 1, 128],
            "threshold": 0.5, "state_reset": "each_probe", "gap_bridging": False,
            "padding": "zero_pad_final_frame_but_never_count_padding_as_speech"},
    "embedding": {"family": "speechbrain/spkrec-ecapa-voxceleb", "checkpoint": "embedding_model.ckpt",
                  "n_mels": 80, "channels": [1024, 1024, 1024, 1024, 3072],
                  "kernel_sizes": [5, 3, 3, 3, 1], "dilations": [1, 2, 3, 4, 1],
                  "attention_channels": 128, "lin_neurons": EMBEDDING_DIMENSIONS,
                  "feature_normalization": "sentence_mean_only", "global_embedding_normalization": False,
                  "output_normalization": "unit_l2", "checkpoint_load": "weights_only_cpu_strict"},
    "excerpt": {"selection": "longest_contiguous_positive_run_earliest_tie_prefix_cap",
                "minimum_ms": MIN_EMBEDDING_MS, "maximum_ms": MAX_EMBEDDING_MS,
                "concatenate_disjoint_speech": False, "embedding_timestamps": "selected_excerpt_only"},
    "execution": {"device": "cpu", "network": "caller_process_denied",
                  "remote_model_loading": False, "yaml_execution": False,
                  "minimum_threads": 1, "maximum_threads": 4},
    "runtime_candidate_pins": dict(RUNTIME_PINS),
    "identity_authority": "none", "speaker_overlap_detection": False,
    "screening_calibrated": False, "runtime_benchmarked": False,
}


class ScreenEngineError(RuntimeError):
    """A local model, runtime, PCM input, or observation failed admission."""


def _path(value):
    if (not isinstance(value, str) or not value.startswith("/") or len(value) > 4096
            or any(ord(char) < 32 for char in value) or "\\" in value or "//" in value
            or any(part in {".", ".."} for part in value.split("/"))
            or str(Path(value)) != value or value == "/"):
        raise ScreenEngineError("model path must be a normalized absolute local file path")
    return value


def validate_model_config(config: dict) -> dict:
    """Validate metadata only; never open a model or import an ML framework."""
    if (not isinstance(config, dict) or set(config) != {"kind", "schema_version", "silero_vad", "ecapa_embedding"}
            or config["kind"] != "himr_speaker_screen_models"
            or type(config["schema_version"]) is not int or config["schema_version"] != 1):
        raise ScreenEngineError("speaker model configuration has an invalid exact schema")
    result = {"kind": config["kind"], "schema_version": 1}
    for name in MAX_MODEL_BYTES:
        binding = config[name]
        if (not isinstance(binding, dict) or set(binding) != {"path", "sha256"}
                or not isinstance(binding["sha256"], str)
                or re.fullmatch(r"[0-9a-f]{64}", binding["sha256"]) is None):
            raise ScreenEngineError("each model needs an exact local path and SHA-256 binding")
        result[name] = {"path": _path(binding["path"]), "sha256": binding["sha256"]}
    if result["silero_vad"]["path"] == result["ecapa_embedding"]["path"]:
        raise ScreenEngineError("VAD and embedding artifacts must be distinct local files")
    return result


def runtime_versions() -> dict[str, str | None]:
    """Installed distribution metadata, not proof of a successful model pilot."""
    result = {}
    for name in RUNTIME_PINS:
        try:
            result[name] = metadata.version(name)
        except metadata.PackageNotFoundError:
            result[name] = None
    return result


def _witness(info):
    return (info.st_dev, info.st_ino, info.st_mode, info.st_uid, info.st_nlink,
            info.st_size, info.st_mtime_ns, info.st_ctime_ns)


def _model_snapshot(binding: dict, maximum: int) -> bytes:
    """Hash the exact bounded bytes later handed to a model deserializer."""
    path = Path(binding["path"])
    parent = descriptor = None
    try:
        parent = os.open("/", os.O_RDONLY | os.O_DIRECTORY | os.O_CLOEXEC | os.O_NOFOLLOW)
        for part in path.parts[1:-1]:
            child = os.open(part, os.O_RDONLY | os.O_DIRECTORY | os.O_CLOEXEC | os.O_NOFOLLOW, dir_fd=parent)
            os.close(parent)
            parent = child
        descriptor = os.open(path.name, os.O_RDONLY | os.O_CLOEXEC | os.O_NOFOLLOW | os.O_NONBLOCK, dir_fd=parent)
        before = os.fstat(descriptor)
        if (not stat.S_ISREG(before.st_mode) or before.st_uid not in {0, os.geteuid()}
                or before.st_nlink != 1 or before.st_mode & 0o022 or not 0 < before.st_size <= maximum):
            raise ScreenEngineError("model artifact is not a bounded safe regular file")
        chunks, count, digest = [], 0, hashlib.sha256()
        while count <= maximum:
            block = os.read(descriptor, min(1024**2, maximum + 1 - count))
            if not block:
                break
            chunks.append(block)
            count += len(block)
            digest.update(block)
        if (count != before.st_size or count > maximum or digest.hexdigest() != binding["sha256"]
                or _witness(before) != _witness(os.fstat(descriptor))
                or _witness(before) != _witness(os.stat(path.name, dir_fd=parent, follow_symlinks=False))):
            raise ScreenEngineError("model artifact differs from its stable SHA-256 binding")
        return b"".join(chunks)
    except OSError as error:
        raise ScreenEngineError("cannot safely read a configured local model artifact") from error
    finally:
        if descriptor is not None:
            os.close(descriptor)
        if parent is not None:
            os.close(parent)


def _runtime_imports(threads):
    # This function is only called in an explicitly selected isolated worker.
    # Offline flags are defense in depth; the caller must deny network syscalls.
    for key, value in {"HF_HUB_OFFLINE": "1", "HF_DATASETS_OFFLINE": "1", "HF_HUB_DISABLE_TELEMETRY": "1",
                       "DO_NOT_TRACK": "1", "CUDA_VISIBLE_DEVICES": "", "OMP_NUM_THREADS": str(threads),
                       "MKL_NUM_THREADS": str(threads), "OPENBLAS_NUM_THREADS": str(threads)}.items():
        os.environ[key] = value
    versions = runtime_versions()
    if versions != RUNTIME_PINS:
        raise ScreenEngineError("speaker runtime differs from the reviewed CPU dependency candidate; use the isolated runtime")
    try:
        import numpy as np
        import onnxruntime as ort
        import torch
        from speechbrain.lobes.features import Fbank
        from speechbrain.lobes.models.ECAPA_TDNN import ECAPA_TDNN
        from speechbrain.processing.features import InputNormalization
    except (ImportError, OSError, RuntimeError, AttributeError) as error:
        raise ScreenEngineError("isolated CPU speaker runtime cannot be imported") from error
    if getattr(torch.version, "cuda", None) is not None or getattr(torch.version, "hip", None) is not None:
        raise ScreenEngineError("speaker runtime must use the CPU-only Torch wheel")
    torch.set_num_threads(threads)
    if torch.get_num_interop_threads() != 1:
        try:
            torch.set_num_interop_threads(1)
        except RuntimeError as error:
            raise ScreenEngineError("speaker runtime thread pool was already initialized incompatibly") from error
    return SimpleNamespace(np=np, ort=ort, torch=torch, Fbank=Fbank, ECAPA_TDNN=ECAPA_TDNN,
                           InputNormalization=InputNormalization, versions=versions)


class _LocalBackend:
    def __init__(self, vad_bytes, embedding_bytes, *, threads):
        self.runtime = runtime = _runtime_imports(threads)
        np, ort, torch = runtime.np, runtime.ort, runtime.torch
        options = ort.SessionOptions()
        options.inter_op_num_threads = 1
        options.intra_op_num_threads = threads
        options.execution_mode = ort.ExecutionMode.ORT_SEQUENTIAL
        options.enable_cpu_mem_arena = False
        options.add_session_config_entry("session.intra_op.allow_spinning", "0")
        options.add_session_config_entry("session.inter_op.allow_spinning", "0")
        # Bytes, not a pathname: no mutable reread or external ONNX data files.
        self.vad = ort.InferenceSession(vad_bytes, sess_options=options, providers=["CPUExecutionProvider"])
        if self.vad.get_providers() != ["CPUExecutionProvider"]:
            raise ScreenEngineError("VAD session admitted a non-CPU execution provider")
        inputs = {item.name: item for item in self.vad.get_inputs()}
        outputs = {item.name: item for item in self.vad.get_outputs()}
        if (set(inputs) != {"input", "state", "sr"} or set(outputs) != {"output", "stateN"}
                or inputs["input"].type != "tensor(float)" or inputs["state"].type != "tensor(float)"
                or inputs["sr"].type != "tensor(int64)" or inputs["sr"].shape != []
                or outputs["output"].type != "tensor(float)" or outputs["stateN"].type != "tensor(float)"):
            raise ScreenEngineError("local VAD artifact is not the reviewed Silero ONNX interface")
        self.features = runtime.Fbank(n_mels=80, sample_rate=SAMPLE_RATE, n_fft=400,
                                      win_length=25, hop_length=10, deltas=False, context=False, requires_grad=False).to("cpu").eval()
        self.normalization = runtime.InputNormalization(norm_type="sentence", std_norm=False).to("cpu").eval()
        recipe = MODEL_RECIPE["embedding"]
        self.embedding = runtime.ECAPA_TDNN(input_size=80, device="cpu", channels=recipe["channels"],
            kernel_sizes=recipe["kernel_sizes"], dilations=recipe["dilations"], attention_channels=128,
            lin_neurons=EMBEDDING_DIMENSIONS, res2net_scale=8, se_channels=128,
            global_context=True, groups=[1, 1, 1, 1, 1], dropout=0.0).to("cpu")
        state = torch.load(io.BytesIO(embedding_bytes), weights_only=True, map_location="cpu")
        if not isinstance(state, dict) or not 0 < len(state) <= 4096:
            raise ScreenEngineError("embedding checkpoint is not a bounded tensor state dictionary")
        parameters = 0
        for key, tensor in state.items():
            if (not isinstance(key, str) or not 0 < len(key) <= 256 or not torch.is_tensor(tensor)
                    or tensor.device.type != "cpu" or tensor.layout != torch.strided):
                raise ScreenEngineError("embedding checkpoint contains an unsupported state value")
            parameters += tensor.numel()
            if parameters > 50_000_000 or not torch.isfinite(tensor).all().item():
                raise ScreenEngineError("embedding checkpoint contains nonfinite or excessive parameters")
        self.embedding.load_state_dict(state, strict=True)
        self.embedding.requires_grad_(False)
        self.embedding.eval()
        self._sample_rate = np.array(SAMPLE_RATE, dtype=np.int64)

    def probabilities(self, pcm16):
        np = self.runtime.np
        waveform = np.frombuffer(pcm16, dtype="<i2").astype(np.float32) / np.float32(32768.0)
        state = np.zeros((2, 1, 128), dtype=np.float32)
        context = np.zeros((1, CONTEXT_SAMPLES), dtype=np.float32)
        result = []
        for start in range(0, waveform.size, FRAME_SAMPLES):
            frame = waveform[start:start + FRAME_SAMPLES]
            if frame.size < FRAME_SAMPLES:
                frame = np.pad(frame, (0, FRAME_SAMPLES - frame.size))
            inputs = np.concatenate((context, frame.reshape(1, FRAME_SAMPLES)), axis=1)
            output, next_state = self.vad.run(["output", "stateN"],
                                            {"input": inputs, "state": state, "sr": self._sample_rate})
            if (not isinstance(output, np.ndarray) or output.shape != (1, 1) or output.dtype != np.float32
                    or not np.isfinite(output).all() or not 0 <= float(output[0, 0]) <= 1
                    or not isinstance(next_state, np.ndarray) or next_state.shape != (2, 1, 128)
                    or next_state.dtype != np.float32 or not np.isfinite(next_state).all()):
                raise ScreenEngineError("VAD returned an invalid probability or recurrent state")
            state = next_state
            context = inputs[:, -CONTEXT_SAMPLES:].copy()
            result.append(float(output[0, 0]))
        return result

    def encode(self, pcm16):
        np, torch = self.runtime.np, self.runtime.torch
        waveform = np.frombuffer(pcm16, dtype="<i2").astype(np.float32) / np.float32(32768.0)
        with torch.inference_mode():
            audio = torch.from_numpy(waveform).unsqueeze(0).to(device="cpu", dtype=torch.float32)
            lengths = torch.ones(1, device="cpu", dtype=torch.float32)
            features = self.features(audio)
            features = self.normalization(features, lengths)
            embedding = self.embedding(features, lengths)
            if (tuple(embedding.shape) != (1, 1, EMBEDDING_DIMENSIONS)
                    or embedding.device.type != "cpu" or not torch.isfinite(embedding).all().item()):
                raise ScreenEngineError("ECAPA returned an invalid CPU embedding")
            return embedding.detach().reshape(EMBEDDING_DIMENSIONS).tolist()


class CpuScreenEngine:
    """One lazy resident model pair for bounded independent PCM probes.

    VAD-positive means an individual 32 ms frame meets the fixed threshold;
    this conservative screen does not bridge gaps or guarantee a single voice.
    Embedding-bearing bounds describe only the selected contiguous excerpt.
    Without an eligible excerpt, bounds describe the whole probe and speech_ms
    is the total positive-frame duration. Digital zero is a model-free negative.
    """

    def __init__(self, model_config: dict, threads: int = 1):
        self.model_config = validate_model_config(model_config)
        if type(threads) is not int or not 1 <= threads <= 4:
            raise ScreenEngineError("speaker CPU threads must be an integer from one through four")
        self.threads = threads
        self._backend = None

    def _load(self):
        if self._backend is None:
            vad = _model_snapshot(self.model_config["silero_vad"], MAX_MODEL_BYTES["silero_vad"])
            embedding = _model_snapshot(self.model_config["ecapa_embedding"], MAX_MODEL_BYTES["ecapa_embedding"])
            self._backend = _LocalBackend(vad, embedding, threads=self.threads)
        return self._backend

    def provenance(self):
        return {"models": copy.deepcopy(self.model_config), "recipe": copy.deepcopy(MODEL_RECIPE),
                "threads": self.threads, "models_loaded": self._backend is not None,
                "runtime_versions": runtime_versions()}

    def analyze(self, pcm16: bytes, *, start_ms: int, end_ms: int, index: int) -> dict:
        if (type(index) is not int or not 0 <= index <= 2**53 - 1
                or type(start_ms) is not int or type(end_ms) is not int
                or not 0 <= start_ms < end_ms <= 2**53 - 1 or end_ms - start_ms > MAX_PROBE_MS
                or not isinstance(pcm16, bytes) or len(pcm16) != (end_ms - start_ms) * 32):
            raise ScreenEngineError("probe must contain exact bounded 16 kHz mono PCM16LE bytes and integer coordinates")
        observation = {"index": index, "start_ms": start_ms, "end_ms": end_ms, "speech_ms": 0, "embedding": None}
        if not any(pcm16):
            return observation
        try:
            backend = self._load()
            probabilities = backend.probabilities(pcm16)
            samples = len(pcm16) // 2
            if not isinstance(probabilities, list) or len(probabilities) != (samples + FRAME_SAMPLES - 1) // FRAME_SAMPLES:
                raise ScreenEngineError("VAD probability count does not cover the bounded probe")
            runs, run_start, speech_samples = [], None, 0
            for ordinal, probability in enumerate(probabilities):
                if (isinstance(probability, bool) or not isinstance(probability, (int, float))
                        or not math.isfinite(probability) or not 0 <= probability <= 1):
                    raise ScreenEngineError("VAD probability is not finite and bounded")
                begin, end = ordinal * FRAME_SAMPLES, min(samples, (ordinal + 1) * FRAME_SAMPLES)
                if probability >= 0.5:
                    speech_samples += end - begin
                    if run_start is None:
                        run_start = begin
                elif run_start is not None:
                    runs.append((run_start, begin))
                    run_start = None
            if run_start is not None:
                runs.append((run_start, samples))
            observation["speech_ms"] = speech_samples // 16
            if not runs:
                return observation
            begin, end = max(runs, key=lambda span: (span[1] - span[0], -span[0]))
            if end - begin < MIN_EMBEDDING_MS * 16:
                return observation
            end = min(end, begin + MAX_EMBEDDING_MS * 16)
            vector = backend.encode(pcm16[begin * 2:end * 2])
            if (not isinstance(vector, list) or len(vector) != EMBEDDING_DIMENSIONS
                    or any(isinstance(value, bool) or not isinstance(value, (int, float))
                           or not math.isfinite(value) for value in vector)):
                raise ScreenEngineError("speaker vector is not a finite 192-dimensional embedding")
            norm = math.hypot(*vector)
            if not math.isfinite(norm) or norm <= 1e-12:
                raise ScreenEngineError("speaker embedding has an invalid magnitude")
            observation.update(start_ms=start_ms + begin // 16, end_ms=start_ms + end // 16,
                               speech_ms=(end - begin) // 16, embedding=[float(value / norm) for value in vector])
            return observation
        except ScreenEngineError:
            raise
        except Exception as error:
            raise ScreenEngineError("local CPU speaker analysis failed; no observation was admitted") from error
