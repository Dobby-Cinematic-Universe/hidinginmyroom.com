"""Resident CPU/CUDA speaker-screen inference, separate from the CPU v1 runner.

The caller owns process isolation, networking denial, decoding, deadlines and
recording-local checkpoints. Only weights survive calls: VAD state, PCM,
features and embeddings do not become cross-recording caches. CUDA is explicit,
float32-only and fail-closed; it is never a fallback for the CPU recipe.

Reviewed runtime behavior:
https://docs.pytorch.org/docs/2.8/notes/cuda.html
https://docs.pytorch.org/docs/2.8/generated/torch.cuda.memory.set_per_process_memory_fraction.html
"""

from __future__ import annotations

import copy
import io
import math
import os
from types import SimpleNamespace

from pipeline import speaker_screen_engine as cpu


ScreenEngineError = cpu.ScreenEngineError
CPU_RUNTIME_PINS = dict(cpu.RUNTIME_PINS)
CUDA_RUNTIME_PINS = {**CPU_RUNTIME_PINS, "torch": "2.8.0+cu128", "torchaudio": "2.8.0+cu128"}
MAX_BATCH_SIZE = 16


def model_recipe(device):
    if not isinstance(device, str) or device not in {"cpu", "cuda"}:
        raise ScreenEngineError("resident speaker device must be cpu or cuda")
    recipe = copy.deepcopy(cpu.MODEL_RECIPE)
    recipe["recipe_id"] = f"silero_onnx_ecapa_voxceleb_resident_{device}_v1"
    recipe["embedding"]["execution_dtype"] = "float32"
    recipe["embedding"]["batching"] = "equal_feature_lengths_only_no_padding"
    recipe["embedding"]["feature_extraction_device"] = "cpu"
    recipe["execution"].update(device=device, model_residency="across_calls_no_evidence_cache",
                               maximum_batch_size=MAX_BATCH_SIZE, tf32=False, mixed_precision=False)
    recipe["runtime_candidate_pins"] = dict(CPU_RUNTIME_PINS if device == "cpu" else CUDA_RUNTIME_PINS)
    if device == "cuda":
        recipe["execution"].update(cuda_logical_device=0, deterministic_algorithms=True,
                                   cuda_allocation_limit="torch_allocator_fraction_not_all_driver_memory")
    return recipe


def _configure_cuda(torch, fraction):
    """Set controls before model allocations; the cap covers Torch's allocator."""
    if (getattr(torch.version, "cuda", None) != "12.8"
            or getattr(torch.version, "hip", None) is not None or not torch.cuda.is_available()):
        raise ScreenEngineError("reviewed CUDA 12.8 runtime and an available NVIDIA GPU are required")
    torch.cuda.set_device(0)
    properties = torch.cuda.get_device_properties(0)
    total = properties.total_memory
    if type(total) is not int or not 1024**3 <= total <= 1024**4:
        raise ScreenEngineError("CUDA device has an unsupported memory capacity")
    torch.cuda.set_per_process_memory_fraction(fraction, device=0)
    free, available_total = torch.cuda.mem_get_info(0)
    if (type(free) is not int or type(available_total) is not int
            or not 0 <= free <= available_total <= total or free < 512 * 1024**2
            or int(total * fraction) < 256 * 1024**2):
        raise ScreenEngineError("CUDA screening requires 512 MiB free and a 256 MiB allocator budget")
    torch.set_float32_matmul_precision("highest")
    torch.backends.cuda.matmul.allow_tf32 = False
    torch.backends.cudnn.allow_tf32 = False
    torch.backends.cuda.matmul.allow_fp16_reduced_precision_reduction = False
    torch.backends.cuda.matmul.allow_bf16_reduced_precision_reduction = False
    torch.backends.cudnn.benchmark = False
    torch.backends.cudnn.deterministic = True
    torch.use_deterministic_algorithms(True)
    return {"logical_device": 0, "name": str(properties.name), "total_memory_bytes": total,
            "capability": [int(properties.major), int(properties.minor)],
            "cuda_runtime": str(torch.version.cuda), "memory_fraction": fraction,
            "torch_allocator_limit_bytes": int(total * fraction)}


def _cuda_runtime_imports(threads, fraction):
    for key, value in {"HF_HUB_OFFLINE": "1", "HF_DATASETS_OFFLINE": "1",
                       "HF_HUB_DISABLE_TELEMETRY": "1", "DO_NOT_TRACK": "1",
                       "OMP_NUM_THREADS": str(threads), "MKL_NUM_THREADS": str(threads),
                       "OPENBLAS_NUM_THREADS": str(threads), "NVIDIA_TF32_OVERRIDE": "0",
                       "CUBLAS_WORKSPACE_CONFIG": ":4096:8"}.items():
        os.environ[key] = value
    versions = cpu.runtime_versions()
    if versions != CUDA_RUNTIME_PINS:
        raise ScreenEngineError("resident CUDA runtime differs from the reviewed isolated dependency pins")
    try:
        import numpy as np
        import onnxruntime as ort
        import torch
        from speechbrain.lobes.features import Fbank
        from speechbrain.lobes.models.ECAPA_TDNN import ECAPA_TDNN
        from speechbrain.processing.features import InputNormalization
        torch.set_num_threads(threads)
        if torch.get_num_interop_threads() != 1:
            torch.set_num_interop_threads(1)
        gpu = _configure_cuda(torch, fraction)
    except ScreenEngineError:
        raise
    except (ImportError, OSError, RuntimeError, AttributeError, ValueError) as error:
        raise ScreenEngineError("isolated CUDA speaker runtime cannot be initialized") from error
    return SimpleNamespace(np=np, ort=ort, torch=torch, Fbank=Fbank, ECAPA_TDNN=ECAPA_TDNN,
                           InputNormalization=InputNormalization, versions=versions, gpu=gpu)


class _ResidentBackend(cpu._LocalBackend):
    """Reuse the exact CPU VAD implementation; batch only the ECAPA forward pass."""

    def __init__(self, vad_bytes, embedding_bytes, *, threads, device, batch_size, cuda_memory_fraction):
        self.device = device
        self.batch_size = batch_size
        if device == "cpu":
            # This preserves (and does not monkeypatch) the old CPU-wheel guard.
            super().__init__(vad_bytes, embedding_bytes, threads=threads)
            return
        self.runtime = runtime = _cuda_runtime_imports(threads, cuda_memory_fraction)
        np, ort, torch = runtime.np, runtime.ort, runtime.torch
        options = ort.SessionOptions()
        options.inter_op_num_threads = 1
        options.intra_op_num_threads = threads
        options.execution_mode = ort.ExecutionMode.ORT_SEQUENTIAL
        options.enable_cpu_mem_arena = False
        options.add_session_config_entry("session.intra_op.allow_spinning", "0")
        options.add_session_config_entry("session.inter_op.allow_spinning", "0")
        self.vad = ort.InferenceSession(vad_bytes, sess_options=options, providers=["CPUExecutionProvider"])
        if self.vad.get_providers() != ["CPUExecutionProvider"]:
            raise ScreenEngineError("resident VAD admitted a non-CPU execution provider")
        inputs = {item.name: item for item in self.vad.get_inputs()}
        outputs = {item.name: item for item in self.vad.get_outputs()}
        if (set(inputs) != {"input", "state", "sr"} or set(outputs) != {"output", "stateN"}
                or inputs["input"].type != "tensor(float)" or inputs["state"].type != "tensor(float)"
                or inputs["sr"].type != "tensor(int64)" or inputs["sr"].shape != []
                or outputs["output"].type != "tensor(float)" or outputs["stateN"].type != "tensor(float)"):
            raise ScreenEngineError("resident VAD is not the reviewed Silero ONNX interface")
        self.features = runtime.Fbank(n_mels=80, sample_rate=cpu.SAMPLE_RATE, n_fft=400,
            win_length=25, hop_length=10, deltas=False, context=False, requires_grad=False).to("cpu").eval()
        self.normalization = runtime.InputNormalization(norm_type="sentence", std_norm=False).to("cpu").eval()
        recipe = cpu.MODEL_RECIPE["embedding"]
        self.embedding = runtime.ECAPA_TDNN(input_size=80, device="cpu", channels=recipe["channels"],
            kernel_sizes=recipe["kernel_sizes"], dilations=recipe["dilations"], attention_channels=128,
            lin_neurons=cpu.EMBEDDING_DIMENSIONS, res2net_scale=8, se_channels=128,
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
        self.embedding.to(device="cuda:0", dtype=torch.float32)
        self._sample_rate = np.array(cpu.SAMPLE_RATE, dtype=np.int64)

    def encode_batch(self, excerpts):
        if not isinstance(excerpts, list) or not 0 < len(excerpts) <= self.batch_size:
            raise ScreenEngineError("ECAPA excerpt batch exceeds its admitted bound")
        np, torch = self.runtime.np, self.runtime.torch
        if self.device == "cuda" and (torch.backends.cuda.matmul.allow_tf32
                or torch.backends.cudnn.allow_tf32 or torch.backends.cudnn.benchmark
                or not torch.backends.cudnn.deterministic or not torch.are_deterministic_algorithms_enabled()
                or torch.cuda.current_device() != 0):
            raise ScreenEngineError("CUDA inference controls changed after admission")
        groups, result = {}, [None] * len(excerpts)
        with torch.inference_mode(), torch.autocast(device_type="cpu", enabled=False), \
                torch.autocast(device_type=self.device, enabled=False):
            for ordinal, pcm16 in enumerate(excerpts):
                if (not isinstance(pcm16, bytes) or len(pcm16) % 2
                        or not cpu.MIN_EMBEDDING_MS * 32 <= len(pcm16) <= cpu.MAX_EMBEDDING_MS * 32):
                    raise ScreenEngineError("ECAPA excerpt is not bounded mono PCM16LE")
                waveform = np.frombuffer(pcm16, dtype="<i2").astype(np.float32) / np.float32(32768.0)
                audio = torch.from_numpy(waveform).unsqueeze(0).to(device="cpu", dtype=torch.float32)
                features = self.normalization(self.features(audio), torch.ones(1, device="cpu", dtype=torch.float32))
                if (len(features.shape) != 3 or features.shape[0] != 1 or not 1 <= features.shape[1] <= 502
                        or features.shape[2] != 80 or features.device.type != "cpu" or features.dtype != torch.float32
                        or not torch.isfinite(features).all().item()):
                    raise ScreenEngineError("ECAPA feature extraction returned an invalid tensor")
                groups.setdefault(int(features.shape[1]), []).append((ordinal, features))
            target = "cuda:0" if self.device == "cuda" else "cpu"
            for group in groups.values():
                # No padding and no shared feature normalization: every excerpt
                # has exactly the features it would have in singleton inference.
                features = torch.cat([item[1] for item in group], dim=0).to(device=target, dtype=torch.float32)
                lengths = torch.ones(len(group), device=target, dtype=torch.float32)
                encoded = self.embedding(features, lengths)
                if (tuple(encoded.shape) != (len(group), 1, cpu.EMBEDDING_DIMENSIONS)
                        or encoded.device.type != self.device or encoded.dtype != torch.float32
                        or not torch.isfinite(encoded).all().item()):
                    raise ScreenEngineError("ECAPA returned an invalid resident embedding batch")
                # This blocking transfer includes completion of the GPU work.
                vectors = encoded.detach().to(device="cpu", dtype=torch.float32).reshape(len(group), cpu.EMBEDDING_DIMENSIONS).tolist()
                for (ordinal, _), vector in zip(group, vectors):
                    result[ordinal] = vector
        return result


def _validate_item(item):
    if (not isinstance(item, dict) or set(item) != {"pcm", "window"}
            or not isinstance(item["window"], dict) or set(item["window"]) != {"index", "start_ms", "end_ms"}):
        raise ScreenEngineError("each resident batch item requires exact PCM and window fields")
    pcm16, window = item["pcm"], item["window"]
    index, start_ms, end_ms = (window[key] for key in ("index", "start_ms", "end_ms"))
    if (type(index) is not int or not 0 <= index <= 2**53 - 1
            or type(start_ms) is not int or type(end_ms) is not int
            or not 0 <= start_ms < end_ms <= 2**53 - 1 or end_ms - start_ms > cpu.MAX_PROBE_MS
            or not isinstance(pcm16, bytes) or len(pcm16) != (end_ms - start_ms) * 32):
        raise ScreenEngineError("probe must contain exact bounded 16 kHz mono PCM16LE bytes and integer coordinates")
    return {"index": index, "start_ms": start_ms, "end_ms": end_ms, "speech_ms": 0, "embedding": None}


def _select_excerpt(pcm16, probabilities, observation):
    """The CPU v1 contiguous-run selection, before model-dependent encoding."""
    samples = len(pcm16) // 2
    if not isinstance(probabilities, list) or len(probabilities) != (samples + cpu.FRAME_SAMPLES - 1) // cpu.FRAME_SAMPLES:
        raise ScreenEngineError("VAD probability count does not cover the bounded probe")
    runs, run_start, speech_samples = [], None, 0
    for ordinal, probability in enumerate(probabilities):
        if (isinstance(probability, bool) or not isinstance(probability, (int, float))
                or not math.isfinite(probability) or not 0 <= probability <= 1):
            raise ScreenEngineError("VAD probability is not finite and bounded")
        begin, end = ordinal * cpu.FRAME_SAMPLES, min(samples, (ordinal + 1) * cpu.FRAME_SAMPLES)
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
        return None
    begin, end = max(runs, key=lambda span: (span[1] - span[0], -span[0]))
    if end - begin < cpu.MIN_EMBEDDING_MS * 16:
        return None
    end = min(end, begin + cpu.MAX_EMBEDDING_MS * 16)
    start_ms = observation["start_ms"]
    observation.update(start_ms=start_ms + begin // 16, end_ms=start_ms + end // 16,
                       speech_ms=(end - begin) // 16)
    return pcm16[begin * 2:end * 2]


class ResidentScreenEngine:
    """Lazy resident weights with independently reset probes and bounded batches.

    Outputs preserve input order (including repeated window indices from
    different recordings). Batched CPU/CUDA floating-point results need not be
    bit-identical to CPU singleton results, so callers must use new plan IDs.
    A failed analysis poisons this instance: no partial observations or implicit
    CPU fallback may be admitted after an inference error.
    """

    def __init__(self, model_config, *, device="cpu", threads=1, batch_size=8, cuda_memory_fraction=0.5):
        self.model_config = cpu.validate_model_config(model_config)
        if not isinstance(device, str) or device not in {"cpu", "cuda"}:
            raise ScreenEngineError("resident speaker device must be cpu or cuda")
        if type(threads) is not int or not 1 <= threads <= 4:
            raise ScreenEngineError("resident speaker threads must be an integer from one through four")
        if type(batch_size) is not int or not 1 <= batch_size <= MAX_BATCH_SIZE:
            raise ScreenEngineError("resident speaker batch size must be an integer from one through sixteen")
        if (isinstance(cuda_memory_fraction, bool) or not isinstance(cuda_memory_fraction, (int, float))
                or not math.isfinite(cuda_memory_fraction) or not 0.1 <= cuda_memory_fraction <= 0.75):
            raise ScreenEngineError("CUDA memory fraction must be finite and between 0.1 and 0.75")
        self.device, self.threads, self.batch_size = device, threads, batch_size
        self.cuda_memory_fraction = float(cuda_memory_fraction)
        self._backend = None
        self._failed = False

    def _load(self):
        if self._backend is None:
            vad = cpu._model_snapshot(self.model_config["silero_vad"], cpu.MAX_MODEL_BYTES["silero_vad"])
            embedding = cpu._model_snapshot(self.model_config["ecapa_embedding"], cpu.MAX_MODEL_BYTES["ecapa_embedding"])
            self._backend = _ResidentBackend(vad, embedding, threads=self.threads, device=self.device,
                                            batch_size=self.batch_size, cuda_memory_fraction=self.cuda_memory_fraction)
        return self._backend

    def provenance(self):
        return {"models": copy.deepcopy(self.model_config), "recipe": model_recipe(self.device),
                "threads": self.threads, "device": self.device, "batch_size": self.batch_size,
                "cuda_memory_fraction": self.cuda_memory_fraction if self.device == "cuda" else None,
                "runtime_versions": cpu.runtime_versions(),
                "cuda": copy.deepcopy(self._backend.runtime.gpu) if self._backend is not None and self.device == "cuda" else None}

    def initialize(self):
        """Explicitly admit/load the resident pair before sealing provenance."""
        if self._failed:
            raise ScreenEngineError("resident speaker engine failed previously; use a fresh isolated worker")
        try:
            self._load()
            return self.provenance()
        except ScreenEngineError:
            self._failed = True
            raise
        except Exception as error:
            self._failed = True
            raise ScreenEngineError("resident speaker models could not be initialized") from error

    def analyze_batch(self, items):
        if self._failed:
            raise ScreenEngineError("resident speaker engine failed previously; use a fresh isolated worker")
        if not isinstance(items, list) or len(items) > self.batch_size:
            raise ScreenEngineError("resident probe batch exceeds its admitted bound")
        # Validate the entire batch before any model load or inference.
        observations = [_validate_item(item) for item in items]
        try:
            excerpts, ordinals = [], []
            backend = None
            for ordinal, (item, observation) in enumerate(zip(items, observations)):
                if not any(item["pcm"]):
                    continue
                if backend is None:
                    backend = self._load()
                excerpt = _select_excerpt(item["pcm"], backend.probabilities(item["pcm"]), observation)
                if excerpt is not None:
                    excerpts.append(excerpt)
                    ordinals.append(ordinal)
            if excerpts:
                vectors = backend.encode_batch(excerpts)
                if not isinstance(vectors, list) or len(vectors) != len(excerpts):
                    raise ScreenEngineError("ECAPA embedding batch does not match its input count")
                for ordinal, vector in zip(ordinals, vectors):
                    if (not isinstance(vector, list) or len(vector) != cpu.EMBEDDING_DIMENSIONS
                            or any(isinstance(value, bool) or not isinstance(value, (int, float))
                                   or not math.isfinite(value) for value in vector)):
                        raise ScreenEngineError("speaker vector is not a finite 192-dimensional embedding")
                    norm = math.hypot(*vector)
                    if not math.isfinite(norm) or norm <= 1e-12:
                        raise ScreenEngineError("speaker embedding has an invalid magnitude")
                    observations[ordinal]["embedding"] = [float(value / norm) for value in vector]
            return observations
        except ScreenEngineError:
            self._failed = True
            raise
        except Exception as error:
            self._failed = True
            raise ScreenEngineError("resident speaker analysis failed; no observations were admitted") from error
