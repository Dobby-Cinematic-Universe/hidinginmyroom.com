#!/usr/bin/env python3
"""Run a bounded, offline faster-whisper CUDA smoke test and seal its result."""

from __future__ import annotations

import argparse
import gc
import hashlib
import importlib.util
import json
import math
import os
import platform
import re
import secrets
import stat
import subprocess
import sys
import sysconfig
import threading
import time
from datetime import datetime, timezone
from importlib.metadata import distributions, version
from pathlib import Path
from typing import Any


SHA256_RE = re.compile(r"^[0-9a-f]{64}$")


def canonical_bytes(value: Any) -> bytes:
    return (
        json.dumps(
            value,
            sort_keys=True,
            separators=(",", ":"),
            ensure_ascii=False,
            allow_nan=False,
        )
        + "\n"
    ).encode()


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def utc_now() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def ensure_cuda_wheel_libraries() -> None:
    os.environ["HF_HUB_OFFLINE"] = "1"
    os.environ["HF_HUB_DISABLE_TELEMETRY"] = "1"
    os.environ["DO_NOT_TRACK"] = "1"
    purelib = Path(sysconfig.get_paths()["purelib"])
    required = [purelib / "nvidia" / "cublas" / "lib", purelib / "nvidia" / "cudnn" / "lib"]
    missing = [str(path) for path in required if not path.is_dir()]
    if missing:
        raise RuntimeError(f"missing pinned CUDA wheel library directories: {missing}")
    current = [part for part in os.environ.get("LD_LIBRARY_PATH", "").split(":") if part]
    if all(str(path) in current for path in required):
        return
    environment = os.environ.copy()
    environment.pop("PYTHONHOME", None)
    environment.pop("PYTHONPATH", None)
    environment["LD_LIBRARY_PATH"] = ":".join(str(path) for path in required + [Path(part) for part in current])
    source = str(Path(__file__).resolve())
    os.execve(sys.executable, [sys.executable, "-I", source, *sys.argv[1:]], environment)


def expected_sha256(value: str, label: str) -> str:
    if not SHA256_RE.fullmatch(value):
        raise ValueError(f"{label} must be a lowercase SHA-256 digest")
    return value


def file_snapshot(path: Path) -> dict[str, int]:
    metadata = path.stat()
    return {
        "device": metadata.st_dev,
        "inode": metadata.st_ino,
        "byte_count": metadata.st_size,
        "mtime_ns": metadata.st_mtime_ns,
        "mode": stat.S_IMODE(metadata.st_mode),
        "link_count": metadata.st_nlink,
        "uid": metadata.st_uid,
    }


def verify_hash(path: Path, expected: str, label: str) -> str:
    expected_sha256(expected, f"expected {label} digest")
    actual = sha256_file(path)
    if actual != expected:
        raise ValueError(f"{label} SHA-256 does not match")
    return actual


def load_json(path: Path) -> Any:
    return json.loads(path.read_text(encoding="utf-8"), parse_constant=lambda value: (_ for _ in ()).throw(ValueError(value)))


def validate_fixture_manifest(
    path: Path,
    expected_manifest_sha256: str,
    audio: Path,
    expected_audio_sha256: str,
) -> dict[str, Any]:
    verify_hash(path, expected_manifest_sha256, "synthetic fixture manifest")
    fixture = load_json(path)
    required = {
        "kind",
        "schema_version",
        "created_at",
        "synthetic",
        "corpus_evidence",
        "generator",
        "normalization",
        "artifact",
        "policy",
    }
    if not isinstance(fixture, dict) or set(fixture) != required:
        raise ValueError("synthetic fixture manifest has an unexpected shape")
    if fixture["kind"] != "himr_synthetic_audio_fixture" or fixture["schema_version"] != 1:
        raise ValueError("unsupported synthetic fixture manifest")
    if fixture["synthetic"] is not True or fixture["corpus_evidence"] is not False:
        raise ValueError("smoke profile accepts only an explicit non-corpus synthetic fixture")
    artifact = fixture["artifact"]
    if not isinstance(artifact, dict) or set(artifact) != {"path", "sha256", "byte_count"}:
        raise ValueError("synthetic fixture artifact binding is invalid")
    if Path(artifact["path"]) != audio:
        raise ValueError("synthetic fixture path does not bind the requested audio")
    if artifact["sha256"] != expected_audio_sha256 or artifact["byte_count"] != audio.stat().st_size:
        raise ValueError("synthetic fixture bytes do not bind the requested audio")
    policy = fixture["policy"]
    if not isinstance(policy, dict) or policy.get("publication_authority") != "none":
        raise ValueError("synthetic fixture policy is invalid")
    return fixture


def network_namespace_evidence(expected_parent_namespace: str) -> dict[str, Any]:
    namespace = os.readlink("/proc/self/ns/net")
    if namespace == expected_parent_namespace:
        raise RuntimeError("process did not enter a distinct network namespace")
    interfaces: list[str] = []
    for line in Path("/proc/net/dev").read_text(encoding="utf-8").splitlines()[2:]:
        if ":" in line:
            interfaces.append(line.split(":", 1)[0].strip())
    interfaces = sorted(set(interfaces))
    non_loopback = [name for name in interfaces if name != "lo"]
    if non_loopback:
        raise RuntimeError(f"isolated smoke namespace exposes non-loopback interfaces: {non_loopback}")
    return {
        "method": "bubblewrap_unshare_net",
        "expected_parent_namespace": expected_parent_namespace,
        "process_namespace": namespace,
        "interfaces_from_proc_net_dev": interfaces,
        "non_loopback_interfaces": non_loopback,
        "verified": True,
    }


class HardDeadline:
    def __init__(self, seconds: float) -> None:
        self.seconds = seconds
        self.cancel = threading.Event()
        self.thread = threading.Thread(target=self._watch, name="himr-hard-deadline", daemon=True)

    def _watch(self) -> None:
        if not self.cancel.wait(self.seconds):
            os._exit(124)

    def __enter__(self) -> HardDeadline:
        self.thread.start()
        return self

    def __exit__(self, *_: Any) -> None:
        self.cancel.set()
        self.thread.join(timeout=2)


def walk_regular_files(root: Path) -> list[Path]:
    files: list[Path] = []
    for directory, directory_names, file_names in os.walk(root, followlinks=False):
        directory_path = Path(directory)
        if stat.S_IMODE(directory_path.lstat().st_mode) != 0o500:
            raise ValueError(f"model directory is not mode 0500: {directory_path}")
        for name in directory_names:
            child = directory_path / name
            if stat.S_ISLNK(child.lstat().st_mode):
                raise ValueError(f"model snapshot contains a symlinked directory: {child}")
        for name in file_names:
            path = directory_path / name
            metadata = path.lstat()
            if not stat.S_ISREG(metadata.st_mode) or metadata.st_nlink != 1:
                raise ValueError(f"model snapshot contains an unsafe file: {path}")
            if stat.S_IMODE(metadata.st_mode) != 0o400:
                raise ValueError(f"model file is not mode 0400: {path}")
            files.append(path)
    return sorted(files)


def validate_model_manifest(path: Path) -> tuple[dict[str, Any], Path]:
    manifest = load_json(path)
    if isinstance(manifest, dict) and manifest.get("kind") == "himr_hf_model_snapshot_manifest":
        module_path = Path(__file__).resolve().with_name("admit_hf_model.py")
        spec = importlib.util.spec_from_file_location(
            "himr_smoke_model_admission", module_path
        )
        if spec is None or spec.loader is None:
            raise ValueError("production model admission validator is unavailable")
        production_model_admission = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(production_model_admission)
        if path.name != production_model_admission.MANIFEST_NAME:
            raise ValueError("production model manifest has a non-canonical bundle path")
        summary = production_model_admission.validate_bundle(path.parent)
        root = path.parent / production_model_admission.SNAPSHOT_DIRECTORY
        normalized = {
            "kind": manifest["kind"],
            "schema_version": manifest["schema_version"],
            "repository": summary["repository"],
            "revision": summary["revision"],
            "license": manifest["expected_license"],
            "license_evidence": manifest["license_evidence"],
            "files": manifest["files"],
            "identity_sha256": summary["manifest_identity_sha256"],
            "snapshot_root": str(root),
            "admission_receipt_sha256": summary["receipt_sha256"],
            "admission_receipt_identity_sha256": summary[
                "receipt_identity_sha256"
            ],
        }
        return normalized, root
    required = {
        "kind",
        "schema_version",
        "repository",
        "revision",
        "license",
        "files",
        "identity_sha256",
        "sealed_at",
        "snapshot_root",
        "acquisition",
        "policy",
    }
    if not isinstance(manifest, dict) or set(manifest) != required:
        raise ValueError("model manifest has an unexpected shape")
    if manifest["kind"] != "himr_private_model_snapshot" or manifest["schema_version"] != 1:
        raise ValueError("unsupported model manifest")
    root = Path(manifest["snapshot_root"])
    if not root.is_absolute() or not root.is_dir() or root.resolve() != root:
        raise ValueError("model snapshot root is invalid")
    expected_files = manifest["files"]
    actual_paths = walk_regular_files(root)
    if [item["path"] for item in expected_files] != [path.relative_to(root).as_posix() for path in actual_paths]:
        raise ValueError("model snapshot file set differs from its manifest")
    for item, file_path in zip(expected_files, actual_paths, strict=True):
        metadata = file_path.stat()
        if metadata.st_size != item["byte_count"] or sha256_file(file_path) != item["sha256"]:
            raise ValueError(f"model snapshot file failed replay: {file_path}")
    identity_basis = {
        key: manifest[key]
        for key in ("kind", "schema_version", "repository", "revision", "license", "files")
    }
    if hashlib.sha256(canonical_bytes(identity_basis)).hexdigest() != manifest["identity_sha256"]:
        raise ValueError("model manifest identity digest is invalid")
    if manifest["acquisition"].get("credential_used") is not False:
        raise ValueError("credential-bearing model snapshots are outside this smoke profile")
    return manifest, root


def probe_duration(ffprobe: Path, audio: Path) -> tuple[float, dict[str, Any], str]:
    command = [
        str(ffprobe),
        "-v",
        "error",
        "-show_streams",
        "-show_format",
        "-of",
        "json",
        str(audio),
    ]
    completed = subprocess.run(command, check=True, capture_output=True, text=True, timeout=30)
    payload = json.loads(completed.stdout)
    audio_streams = [stream for stream in payload.get("streams", []) if stream.get("codec_type") == "audio"]
    if len(audio_streams) != 1 or len(payload.get("streams", [])) != 1:
        raise ValueError("smoke input must contain exactly one stream and it must be audio")
    stream = audio_streams[0]
    if (
        stream.get("codec_name") != "flac"
        or int(stream.get("sample_rate", 0)) != 16_000
        or int(stream.get("channels", 0)) != 1
        or payload.get("format", {}).get("format_name") != "flac"
    ):
        raise ValueError("smoke input must be 16 kHz mono FLAC")
    duration = float(payload.get("format", {}).get("duration", audio_streams[0].get("duration", 0)))
    if not math.isfinite(duration) or duration <= 0:
        raise ValueError("smoke input duration is unavailable")
    version_output = subprocess.run(
        [str(ffprobe), "-version"], check=True, capture_output=True, text=True, timeout=10
    ).stdout
    return duration, payload, version_output


class NvmlSampler:
    def __init__(self, pynvml: Any, handle: Any) -> None:
        self.pynvml = pynvml
        self.handle = handle
        self.stop_event = threading.Event()
        self.global_peak_bytes = 0
        self.process_peak_bytes = 0
        self.thread = threading.Thread(target=self._sample, name="himr-nvml-sampler", daemon=True)

    def _sample(self) -> None:
        while not self.stop_event.is_set():
            try:
                memory = self.pynvml.nvmlDeviceGetMemoryInfo(self.handle)
                self.global_peak_bytes = max(self.global_peak_bytes, int(memory.used))
                for process in self.pynvml.nvmlDeviceGetComputeRunningProcesses(self.handle):
                    if process.pid == os.getpid() and process.usedGpuMemory is not None:
                        self.process_peak_bytes = max(self.process_peak_bytes, int(process.usedGpuMemory))
            except self.pynvml.NVMLError:
                pass
            self.stop_event.wait(0.02)

    def start(self) -> None:
        self.thread.start()

    def stop(self) -> None:
        self.stop_event.set()
        self.thread.join(timeout=2)


def nvml_memory(pynvml: Any, handle: Any) -> dict[str, int]:
    memory = pynvml.nvmlDeviceGetMemoryInfo(handle)
    return {"total_bytes": int(memory.total), "free_bytes": int(memory.free), "used_bytes": int(memory.used)}


def cuda_library_evidence() -> list[dict[str, Any]]:
    purelib = Path(sysconfig.get_paths()["purelib"])
    patterns = (
        purelib / "nvidia" / "cublas" / "lib" / "libcublas.so.12",
        purelib / "nvidia" / "cudnn" / "lib" / "libcudnn.so.9",
    )
    records: list[dict[str, Any]] = []
    for path in patterns:
        if not path.is_file() or path.resolve() != path:
            raise RuntimeError(f"required CUDA shared library is missing or unsafe: {path}")
        records.append(
            {
                "path": str(path),
                "sha256": sha256_file(path),
                "byte_count": path.stat().st_size,
            }
        )
    return records


def write_exclusive(path: Path, payload: bytes) -> None:
    if not path.is_absolute():
        raise ValueError("result output must be absolute")
    parent = path.parent
    if not parent.is_dir() or parent.resolve() != parent:
        raise ValueError("result parent must be an existing non-symlinked directory")
    parent_descriptor = os.open(parent, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW)
    temporary_name = f".{path.name}.tmp-{os.getpid()}-{secrets.token_hex(16)}"
    descriptor: int | None = None
    try:
        descriptor = os.open(
            temporary_name,
            os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW,
            0o400,
            dir_fd=parent_descriptor,
        )
        with os.fdopen(descriptor, "wb", closefd=False) as handle:
            handle.write(payload)
            handle.flush()
            os.fsync(handle.fileno())
        os.fchmod(descriptor, 0o400)
        os.link(
            temporary_name,
            path.name,
            src_dir_fd=parent_descriptor,
            dst_dir_fd=parent_descriptor,
            follow_symlinks=False,
        )
        os.unlink(temporary_name, dir_fd=parent_descriptor)
        os.fsync(parent_descriptor)
    finally:
        if descriptor is not None:
            os.close(descriptor)
        try:
            os.unlink(temporary_name, dir_fd=parent_descriptor)
        except FileNotFoundError:
            pass
        os.close(parent_descriptor)


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--audio", required=True, type=Path)
    parser.add_argument("--expected-audio-sha256", required=True)
    parser.add_argument("--fixture-manifest", required=True, type=Path)
    parser.add_argument("--expected-fixture-manifest-sha256", required=True)
    parser.add_argument("--model-manifest", required=True, type=Path)
    parser.add_argument("--expected-model-manifest-sha256", required=True)
    parser.add_argument("--pyproject", required=True, type=Path)
    parser.add_argument("--expected-pyproject-sha256", required=True)
    parser.add_argument("--lock", required=True, type=Path)
    parser.add_argument("--expected-lock-sha256", required=True)
    parser.add_argument("--ffprobe", required=True, type=Path)
    parser.add_argument("--expected-ffprobe-sha256", required=True)
    parser.add_argument("--expected-smoke-source-sha256", required=True)
    parser.add_argument("--expected-python-executable-sha256", required=True)
    parser.add_argument("--sandbox-executable", required=True, type=Path)
    parser.add_argument("--expected-sandbox-executable-sha256", required=True)
    parser.add_argument("--expected-parent-network-namespace", required=True)
    parser.add_argument("--runtime-root", required=True, type=Path)
    parser.add_argument("--expected-runtime-device", required=True, type=int)
    parser.add_argument("--expected-gpu-uuid", required=True)
    parser.add_argument("--min-free-vram-bytes", default=1_073_741_824, type=int)
    parser.add_argument("--beam-size", default=1, type=int)
    parser.add_argument("--best-of", default=1, type=int)
    parser.add_argument("--cpu-threads", default=1, type=int)
    parser.add_argument("--num-workers", default=1, type=int)
    parser.add_argument("--max-audio-seconds", default=30.0, type=float)
    parser.add_argument("--max-audio-bytes", default=4_194_304, type=int)
    parser.add_argument("--max-wall-seconds", default=60.0, type=float)
    parser.add_argument("--output", required=True, type=Path)
    args = parser.parse_args()

    ensure_cuda_wheel_libraries()
    if not sys.flags.isolated:
        raise RuntimeError("smoke must execute with Python isolated mode (-I)")
    if sys.version_info[:3] != (3, 12, 14):
        raise RuntimeError("smoke profile requires managed CPython 3.12.14")
    if os.environ.get("CUDA_VISIBLE_DEVICES") not in (None, "", "0"):
        raise RuntimeError("smoke profile requires physical/logical CUDA device zero alignment")
    started_at = utc_now()
    wall_start = time.monotonic()

    source_path = Path(__file__).resolve()
    python_path = Path(sys.executable).resolve()
    runtime_root = args.runtime_root
    if not runtime_root.is_absolute() or not runtime_root.is_dir() or runtime_root.resolve() != runtime_root:
        raise ValueError("runtime root must be an existing absolute non-symlinked directory")
    runtime_metadata = runtime_root.stat()
    if (
        runtime_metadata.st_uid != os.getuid()
        or stat.S_IMODE(runtime_metadata.st_mode) != 0o700
        or runtime_metadata.st_dev != args.expected_runtime_device
    ):
        raise ValueError("runtime root ownership, mode, or expected main-drive device is invalid")
    paths = (
        (args.audio, "audio"),
        (args.fixture_manifest, "synthetic fixture manifest"),
        (args.model_manifest, "model manifest"),
        (args.pyproject, "pyproject"),
        (args.lock, "lock"),
        (args.ffprobe, "ffprobe"),
        (args.sandbox_executable, "sandbox executable"),
        (source_path, "smoke source"),
        (python_path, "Python executable"),
    )
    for path, label in paths:
        if not path.is_absolute() or not path.is_file() or path.resolve() != path:
            raise ValueError(f"{label} must be an existing absolute non-symlinked file")
    for private_path in (args.audio, args.fixture_manifest, args.model_manifest, args.output.parent):
        if runtime_root not in private_path.parents:
            raise ValueError(f"private smoke path lies outside the declared runtime root: {private_path}")
        if private_path.parent.stat().st_dev != args.expected_runtime_device:
            raise ValueError(f"private smoke path is not on the expected main-drive device: {private_path}")
    output_parent = args.output.parent
    output_parent_metadata = output_parent.stat()
    if output_parent_metadata.st_uid != os.getuid() or stat.S_IMODE(output_parent_metadata.st_mode) != 0o700:
        raise ValueError("result parent must be owner-private mode 0700")
    if args.output.exists() or args.output.is_symlink():
        raise FileExistsError(f"refusing to replace smoke result: {args.output}")
    if args.max_audio_bytes <= 0 or args.max_audio_bytes > 4_194_304:
        raise ValueError("max audio bytes must be within (0, 4194304]")
    if args.audio.stat().st_size > args.max_audio_bytes:
        raise ValueError("audio exceeds the bounded smoke byte limit")
    if not math.isfinite(args.max_audio_seconds) or not 0 < args.max_audio_seconds <= 30:
        raise ValueError("max audio duration must be finite and within (0, 30] seconds")
    if not math.isfinite(args.max_wall_seconds) or not 1 <= args.max_wall_seconds <= 120:
        raise ValueError("hard wall deadline must be finite and within [1, 120] seconds")
    if args.min_free_vram_bytes < 0:
        raise ValueError("minimum free VRAM must not be negative")
    if not 1 <= args.beam_size <= 20 or not 1 <= args.best_of <= 20:
        raise ValueError("beam size and best-of must each be within [1, 20]")
    if not 1 <= args.cpu_threads <= 32 or not 1 <= args.num_workers <= 4:
        raise ValueError("CPU threads must be [1, 32] and workers [1, 4]")

    expected_hashes = {
        args.audio: (args.expected_audio_sha256, "audio"),
        args.model_manifest: (args.expected_model_manifest_sha256, "model manifest"),
        args.pyproject: (args.expected_pyproject_sha256, "pyproject"),
        args.lock: (args.expected_lock_sha256, "lock"),
        args.ffprobe: (args.expected_ffprobe_sha256, "ffprobe"),
        source_path: (args.expected_smoke_source_sha256, "smoke source"),
        python_path: (args.expected_python_executable_sha256, "Python executable"),
        args.sandbox_executable: (args.expected_sandbox_executable_sha256, "sandbox executable"),
    }
    snapshots_before = {str(path): file_snapshot(path) for path, _ in paths}
    for path, (digest, label) in expected_hashes.items():
        verify_hash(path, digest, label)
    fixture = validate_fixture_manifest(
        args.fixture_manifest,
        args.expected_fixture_manifest_sha256,
        args.audio,
        args.expected_audio_sha256,
    )
    model_manifest_sha256 = args.expected_model_manifest_sha256
    network_isolation = network_namespace_evidence(args.expected_parent_network_namespace)
    duration_seconds, probe, ffprobe_version = probe_duration(args.ffprobe, args.audio)
    if duration_seconds > args.max_audio_seconds:
        raise ValueError("audio exceeds the bounded smoke duration")
    model_manifest, model_root = validate_model_manifest(args.model_manifest)
    if runtime_root not in model_root.parents or model_root.stat().st_dev != args.expected_runtime_device:
        raise ValueError("sealed model root must remain on the declared main-drive runtime")

    deadline = HardDeadline(args.max_wall_seconds)
    deadline.thread.start()

    import ctranslate2
    import pynvml
    from faster_whisper import WhisperModel

    pynvml.nvmlInit()
    try:
        device_count = ctranslate2.get_cuda_device_count()
        if device_count < 1:
            raise RuntimeError("CTranslate2 reports no CUDA device")
        handle = pynvml.nvmlDeviceGetHandleByIndex(0)
        gpu_uuid = pynvml.nvmlDeviceGetUUID(handle)
        if gpu_uuid != args.expected_gpu_uuid:
            raise RuntimeError("NVML physical device zero does not match the expected GPU UUID")
        memory_before = nvml_memory(pynvml, handle)
        if memory_before["free_bytes"] < args.min_free_vram_bytes:
            raise RuntimeError("available VRAM is below the bounded smoke floor")
        sampler = NvmlSampler(pynvml, handle)
        sampler.start()
        try:
            model_start = time.monotonic()
            model = WhisperModel(
                str(model_root),
                device="cuda",
                device_index=0,
                compute_type="float16",
                local_files_only=True,
                cpu_threads=args.cpu_threads,
                num_workers=args.num_workers,
            )
            model_load_seconds = time.monotonic() - model_start
            inference_start = time.monotonic()
            segment_iterator, info = model.transcribe(
                str(args.audio),
                language="en",
                beam_size=args.beam_size,
                best_of=args.best_of,
                word_timestamps=True,
                vad_filter=False,
                condition_on_previous_text=False,
                temperature=0.0,
            )
            segments = []
            for segment in segment_iterator:
                segments.append(
                    {
                        "start_ms": round(segment.start * 1000),
                        "end_ms": round(segment.end * 1000),
                        "text": segment.text,
                        "words": [
                            {
                                "start_ms": round(word.start * 1000),
                                "end_ms": round(word.end * 1000),
                                "text": word.word,
                                "raw_probability": word.probability,
                                "calibrated_probability": None,
                            }
                            for word in (segment.words or [])
                        ],
                    }
                )
            inference_seconds = time.monotonic() - inference_start
            del model
            gc.collect()
        finally:
            sampler.stop()
        memory_after = nvml_memory(pynvml, handle)
        hardware = {
            "device_index": 0,
            "name": pynvml.nvmlDeviceGetName(handle),
            "uuid": gpu_uuid,
            "driver_version": pynvml.nvmlSystemGetDriverVersion(),
            "cuda_driver_version": pynvml.nvmlSystemGetCudaDriverVersion_v2(),
            "compute_capability": list(pynvml.nvmlDeviceGetCudaComputeCapability(handle)),
            "ctranslate2_cuda_device_count": device_count,
            "ctranslate2_supported_compute_types": sorted(ctranslate2.get_supported_compute_types("cuda", 0)),
            "memory_before": memory_before,
            "memory_after": memory_after,
            "global_peak_used_bytes": sampler.global_peak_bytes,
            "process_peak_used_bytes": sampler.process_peak_bytes,
        }
    finally:
        pynvml.nvmlShutdown()
        deadline.cancel.set()
        deadline.thread.join(timeout=2)

    replayed_model_manifest, replayed_model_root = validate_model_manifest(args.model_manifest)
    if replayed_model_manifest != model_manifest or replayed_model_root != model_root:
        raise RuntimeError("model manifest changed during smoke inference")
    for path, (digest, label) in expected_hashes.items():
        verify_hash(path, digest, f"post-run {label}")
    verify_hash(
        args.fixture_manifest,
        args.expected_fixture_manifest_sha256,
        "post-run synthetic fixture manifest",
    )
    snapshots_after = {str(path): file_snapshot(path) for path, _ in paths}
    if snapshots_before != snapshots_after:
        raise RuntimeError("a pinned smoke input changed during execution")

    result_core = {
        "kind": "himr_gpu_smoke_result",
        "schema_version": 2,
        "status": "completed",
        "started_at": started_at,
        "completed_at": utc_now(),
        "input": {
            "path": str(args.audio),
            "sha256": args.expected_audio_sha256,
            "byte_count": args.audio.stat().st_size,
            "duration_seconds": duration_seconds,
            "synthetic": True,
            "corpus_evidence": False,
            "fixture_manifest_path": str(args.fixture_manifest),
            "fixture_manifest_sha256": args.expected_fixture_manifest_sha256,
            "fixture_created_at": fixture["created_at"],
            "stat_before": snapshots_before[str(args.audio)],
            "stat_after": snapshots_after[str(args.audio)],
            "ffprobe": probe,
        },
        "model": {
            "manifest_path": str(args.model_manifest),
            "manifest_sha256": model_manifest_sha256,
            "identity_sha256": model_manifest["identity_sha256"],
            "repository": model_manifest["repository"],
            "revision": model_manifest["revision"],
            "license": model_manifest["license"],
        },
        "runtime": {
            "python": sys.version,
            "python_executable": sys.executable,
            "python_executable_resolved": str(python_path),
            "python_executable_sha256": args.expected_python_executable_sha256,
            "python_isolated_mode": bool(sys.flags.isolated),
            "platform": platform.platform(),
            "packages": {
                name: version(name)
                for name in (
                    "av",
                    "ctranslate2",
                    "faster-whisper",
                    "nvidia-cublas-cu12",
                    "nvidia-cudnn-cu12",
                    "nvidia-ml-py",
                )
            },
            "installed_packages": sorted(
                (
                    {
                        "name": distribution.metadata["Name"],
                        "version": distribution.version,
                    }
                    for distribution in distributions()
                    if distribution.metadata["Name"]
                ),
                key=lambda item: item["name"].lower(),
            ),
            "cuda_shared_libraries": cuda_library_evidence(),
            "pyproject_path": str(args.pyproject),
            "pyproject_sha256": args.expected_pyproject_sha256,
            "lock_path": str(args.lock),
            "lock_sha256": args.expected_lock_sha256,
            "smoke_source_path": str(source_path),
            "smoke_source_sha256": args.expected_smoke_source_sha256,
            "ffprobe_path": str(args.ffprobe),
            "ffprobe_sha256": args.expected_ffprobe_sha256,
            "ffprobe_version": ffprobe_version,
            "sandbox": {
                "executable": str(args.sandbox_executable),
                "executable_sha256": args.expected_sandbox_executable_sha256,
                "version": subprocess.run(
                    [str(args.sandbox_executable), "--version"],
                    check=True,
                    capture_output=True,
                    text=True,
                    timeout=10,
                ).stdout.strip(),
                "network_namespace": network_isolation,
                "root_mounted_read_only_by_launcher": "operator_command_attestation",
                "explicit_result_parent_mounted_read_write": "operator_command_attestation",
            },
            "huggingface_offline_requested": all(
                os.environ.get(name) == "1"
                for name in ("HF_HUB_OFFLINE", "HF_HUB_DISABLE_TELEMETRY", "DO_NOT_TRACK")
            ),
        },
        "hardware": hardware,
        "parameters": {
            "device": "cuda",
            "device_index": 0,
            "compute_type": "float16",
            "language": "en",
            "beam_size": args.beam_size,
            "best_of": args.best_of,
            "temperature": 0.0,
            "word_timestamps": True,
            "vad_filter": False,
            "condition_on_previous_text": False,
            "cpu_threads": args.cpu_threads,
            "num_workers": args.num_workers,
            "max_audio_seconds": args.max_audio_seconds,
            "max_audio_bytes": args.max_audio_bytes,
            "max_wall_seconds": args.max_wall_seconds,
            "minimum_free_vram_bytes": args.min_free_vram_bytes,
        },
        "output": {
            "machine_generated": True,
            "human_reviewed": False,
            "verified_quotation": False,
            "raw_scores_calibrated": False,
            "language": info.language,
            "language_probability_raw": info.language_probability,
            "segments": segments,
        },
        "metrics": {
            "model_load_seconds": model_load_seconds,
            "inference_seconds": inference_seconds,
            "wall_seconds": time.monotonic() - wall_start,
            "real_time_factor_inference": inference_seconds / duration_seconds,
        },
        "policy": {
            "catalogue_mutated": False,
            "identity_authority": "none",
            "publication_authority": "none",
            "wiki_authority": "none",
            "corpus_media_processed": False,
            "existing_asr_rerun": False,
        },
    }
    result = {
        **result_core,
        "identity_sha256": hashlib.sha256(canonical_bytes(result_core)).hexdigest(),
    }
    payload = canonical_bytes(result)
    write_exclusive(args.output, payload)
    print(
        json.dumps(
            {
                "status": result["status"],
                "output": str(args.output),
                "identity_sha256": result["identity_sha256"],
                "result_sha256": hashlib.sha256(payload).hexdigest(),
            },
            sort_keys=True,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
