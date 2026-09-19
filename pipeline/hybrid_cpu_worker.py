"""Bounded, resumable CPU transcription for an isolated hybrid workspace.

The existing whisper.cpp adapter is reused without editing its source or sealed
work. New windows, results, and receipts belong only to the caller's private output
root. CPU inference sees only an exact-sample, bounded physical FLAC window.
This is recorded-file ASR, not streaming captions, cloud
diarization, summarization, publication, or catalogue admission.
"""

from __future__ import annotations

import argparse
import copy
from contextlib import contextmanager
import fcntl
import hashlib
import json
import os
from pathlib import Path
import signal
import shutil
import stat
import subprocess
import sys
import tempfile
from typing import Any
import urllib.parse

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from pipeline import asr_whispercpp as adapter
from pipeline import whispercpp_engine_profiles as engine_profiles
from pipeline import longform_asr_input as recording_inputs
from pipeline.salad_transcription_contract import CloudContractError, canonical_bytes, load_recording_input


MAX_JSON_BYTES = 64 * 1024 * 1024
MAX_RECORDING_SAMPLES = 24 * 3600 * 16000
POLICY = {
    "visibility": "private", "machine_generated": True, "human_review_required": True,
    "verified_quotation": False, "source_artifact_mutation": False,
    "source_controller_mutation": False, "catalogue_mutation_authority": "none",
    "publication_authority": "none", "deletion_authority": "none",
    "cpu_only": True, "cloud_access": False, "diarization": False, "summary": False,
}


class CpuWorkerError(RuntimeError):
    pass


def _hash(value: Any) -> str:
    return hashlib.sha256(canonical_bytes(value)).hexdigest()


def _path(value: str | Path) -> Path:
    path = Path(value)
    if not path.is_absolute() or str(path) != str(value) or any(part in {".", ".."} for part in str(value).split("/")) or path == Path("/"):
        raise CpuWorkerError("CPU worker paths must be normalized absolute paths")
    return path


@contextmanager
def _open(path: Path):
    path = _path(path)
    directory = os.open("/", os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW)
    descriptor = None
    try:
        for part in path.parts[1:-1]:
            child = os.open(part, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW, dir_fd=directory)
            os.close(directory)
            directory = child
        descriptor = os.open(path.name, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK, dir_fd=directory)
        info = os.fstat(descriptor)
        if not stat.S_ISREG(info.st_mode) or info.st_mode & 0o022:
            raise CpuWorkerError("CPU input must be a regular file without peer write access")
        yield descriptor
    finally:
        if descriptor is not None:
            os.close(descriptor)
        os.close(directory)


def _witness(value):
    return (value.st_dev, value.st_ino, value.st_size, value.st_mtime_ns, value.st_ctime_ns)


def _read_bytes(path: Path) -> bytes:
    with _open(path) as descriptor:
        before = os.fstat(descriptor)
        if not 0 < before.st_size <= MAX_JSON_BYTES:
            raise CpuWorkerError("CPU JSON or implementation file exceeds its size bound")
        with os.fdopen(os.dup(descriptor), "rb") as handle:
            body = handle.read(MAX_JSON_BYTES + 1)
        if len(body) != before.st_size or _witness(before) != _witness(os.fstat(descriptor)):
            raise CpuWorkerError("CPU evidence changed while being read")
    return body


def _read_json(path: Path, digest: str | None = None) -> dict:
    body = _read_bytes(path)
    if digest is not None and hashlib.sha256(body).hexdigest() != digest:
        raise CpuWorkerError("CPU evidence SHA-256 differs")

    def pairs(items):
        result = {}
        for key, value in items:
            if key in result:
                raise CpuWorkerError("CPU evidence repeats a JSON field")
            result[key] = value
        return result

    try:
        value = json.loads(body, object_pairs_hook=pairs,
                           parse_constant=lambda _value: (_ for _ in ()).throw(CpuWorkerError("nonfinite CPU evidence")))
        canonical_bytes(value)  # Also rejects overflowing JSON floats.
    except (ValueError, UnicodeError, RecursionError) as error:
        raise CpuWorkerError("invalid CPU JSON evidence") from error
    if not isinstance(value, dict):
        raise CpuWorkerError("CPU evidence must be a JSON object")
    return value


def _private(path: Path):
    current = Path("/")
    for part in _path(path).parts[1:]:
        current /= part
        if not current.exists() and not current.is_symlink():
            current.mkdir(mode=0o700)
        if not stat.S_ISDIR(current.lstat().st_mode):
            raise CpuWorkerError("CPU workspace contains a symlink or non-directory")
    info = path.stat()
    if info.st_uid != os.getuid() or info.st_mode & 0o077:
        raise CpuWorkerError("CPU workspace must be private and owned")


def _sync(path: Path):
    descriptor = os.open(path, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW)
    try:
        os.fsync(descriptor)
    finally:
        os.close(descriptor)


def _write(path: Path, value: dict):
    body = canonical_bytes(value)
    if len(body) > MAX_JSON_BYTES:
        raise CpuWorkerError("CPU output exceeds its JSON size bound")
    descriptor, temporary = tempfile.mkstemp(prefix=".hybrid-cpu-", dir=path.parent)
    staging = Path(temporary)
    try:
        with os.fdopen(descriptor, "wb") as handle:
            handle.write(body)
            handle.flush()
            os.fsync(handle.fileno())
        try:
            os.link(staging, path, follow_symlinks=False)
        except FileExistsError:
            if canonical_bytes(_read_json(path)) != body:
                raise CpuWorkerError("immutable CPU evidence differs from existing output") from None
        _sync(path.parent)
    finally:
        staging.unlink(missing_ok=True)


def _file_binding(path: Path) -> dict:
    body = _read_bytes(path)
    return {"path": str(path), "sha256": hashlib.sha256(body).hexdigest(), "byte_count": len(body)}


def _implementation() -> dict:
    return {name: _file_binding(Path(module_path).resolve()) for name, module_path in (
        ("worker", __file__), ("adapter", adapter.__file__), ("engine_profiles", engine_profiles.__file__),
        ("recording_input", recording_inputs.__file__),
        ("recording_validator", load_recording_input.__code__.co_filename),
    )}


def admit_cpu_config(engine: dict, model: dict, ffprobe: dict, ffmpeg: dict) -> dict:
    """Validate rich adapter metadata and paths, without hashing model/media."""
    try:
        normalized_engine = adapter.validate_engine(engine)
        normalized_model = adapter.validate_model(model)
        if normalized_engine != engine or normalized_model != model:
            raise CpuWorkerError("CPU engine/model metadata must already be normalized")
        profiles = [profile for profile in engine_profiles.ENGINE_PROFILES
                    if profile["expected_sha256"] == engine["expected_sha256"]
                    and profile["admission"] == "current_new_batch"]
        if len(profiles) != 1 or any(engine[key] != profiles[0][key] for key in ("version_label", "version_evidence", "build")):
            raise CpuWorkerError("CPU execution requires the exact current reviewed whisper.cpp profile")
        for name, tool in (("ffprobe", ffprobe), ("ffmpeg", ffmpeg)):
            if not isinstance(tool, dict) or set(tool) != {"path", "sha256"}:
                raise CpuWorkerError(f"{name} needs exact path and SHA-256 fields")
            adapter.sha256_value(tool["sha256"], name + " SHA-256")
            if not os.access(tool["path"], os.X_OK):
                raise CpuWorkerError(name + " must be executable")
        for path in (engine["executable"], model["path"], ffprobe["path"], ffmpeg["path"]):
            with _open(_path(path)):
                pass
    except (adapter.ASRError, OSError) as error:
        raise CpuWorkerError("CPU engine/model metadata fails the adapter contract") from error
    return {"engine": copy.deepcopy(engine), "model": copy.deepcopy(model),
            "ffprobe": copy.deepcopy(ffprobe), "ffmpeg": copy.deepcopy(ffmpeg)}


def _output_root(path: Path, inputs: list[str]) -> Path:
    root = _path(path)
    if root in {Path.home(), ROOT} or len(root.parts) < 4 or any(base == root or base in root.parents for base in (Path("/tmp"), Path("/var/tmp"))):
        raise CpuWorkerError("CPU output needs a dedicated durable directory outside temporary roots")
    for value in inputs:
        source = _path(value)
        if root == source.parent or root in source.parents or source.parent in root.parents:
            raise CpuWorkerError("CPU output must be separate from source and tool directories")
    return root


def build_cpu_plan(recording_input: Path, expected_sha256: str, *, output_root: Path,
                   engine: dict, model: dict, ffprobe: dict, ffmpeg: dict, threads: int = 6,
                   timeout_seconds: int = 7200, window_seconds: int = 1800) -> dict:
    try:
        threads = adapter.integer(threads, "CPU threads", 1, 8)
        timeout_seconds = adapter.integer(timeout_seconds, "CPU timeout", 1, 86400)
        window_seconds = adapter.integer(window_seconds, "CPU window seconds", 1, 1800)
    except adapter.ASRError as error:
        raise CpuWorkerError("CPU limits must be bounded integers") from error
    try:
        source = load_recording_input(_path(recording_input), expected_sha256)
        raw_manifest = _read_json(recording_input, expected_sha256)
    except CloudContractError as error:
        raise CpuWorkerError("CPU recording input fails its hash-bound manifest contract") from error
    samples = source["audio"]["total_samples"]
    if not 16 <= samples <= MAX_RECORDING_SAMPLES:
        raise CpuWorkerError("CPU recording must be at least one millisecond and at most 24 hours")
    config = admit_cpu_config(engine, model, ffprobe, ffmpeg)
    root = _output_root(output_root, [str(recording_input), source["audio"]["path"], engine["executable"], model["path"], ffprobe["path"], ffmpeg["path"]])
    # The historical CPU adapter speaks milliseconds, not exact PCM samples.
    # Flooring avoids asking it to decode beyond its rounded FFprobe extent.
    duration_ms = samples // 16
    windows = []
    for start in range(0, samples, window_seconds * 16000):
        end = min(samples, start + window_seconds * 16000)
        if end - start < 16 and windows:
            windows[-1]["end_sample"] = end
            continue
        windows.append({"ordinal": len(windows), "start_sample": start, "end_sample": end,
                        "offset_ms": start // 16, "duration_ms": (end - start) // 16})
    core = {
        "kind": "himr_hybrid_cpu_plan", "schema_version": 1,
        "recording": source, "artifact_id": raw_manifest["recording"]["input"]["artifact_id"],
        "admission": {"processing_run_id": "run_hybrid_cpu_input_" + expected_sha256[:32],
                      "basis": "new_hash_bound_recording_input_admission_not_existing_catalogue_producer"},
        "output_root": str(root), **config,
        "threads": threads, "timeout_seconds": timeout_seconds, "window_seconds": window_seconds,
        "windows": windows, "implementation": _implementation(),
        "timing": {"coordinate_system": "normalized_recording_relative_milliseconds",
                   "submillisecond_tail_samples": samples - duration_ms * 16,
                   "exact_sample_coverage_claimed": False, "segment_boundary_overruns_preserved": True},
        "resource_policy": {"max_windows_per_call": 100, "physical_window_flac": True,
                            "max_inference_input_samples": window_seconds * 16000 + 15,
                            "full_recording_asr_input": False,
                            "network_isolation_required_at_service_boundary": True},
        "policy": copy.deepcopy(POLICY),
    }
    identity = _hash(core)
    return {**core, "identity_sha256": identity, "plan_id": "hybridcpu_" + identity[:32]}


def _validate_plan(value: dict) -> dict:
    try:
        replay = build_cpu_plan(Path(value["recording"]["manifest"]["path"]), value["recording"]["manifest"]["sha256"],
                                output_root=Path(value["output_root"]), engine=value["engine"], model=value["model"],
                                ffprobe=value["ffprobe"], ffmpeg=value["ffmpeg"], threads=value["threads"], timeout_seconds=value["timeout_seconds"],
                                window_seconds=value["window_seconds"])
    except (KeyError, TypeError) as error:
        raise CpuWorkerError("CPU plan is incomplete") from error
    if canonical_bytes(value) != canonical_bytes(replay):
        raise CpuWorkerError("CPU plan differs from its inputs or pinned implementation")
    return replay


def _derived_path(plan: dict, window: dict) -> Path:
    return Path(plan["output_root"]) / "derived" / f"{window['ordinal']:05d}"


def _derived_receipt(plan: dict, window: dict) -> dict:
    directory = _derived_path(plan, window)
    receipt = _read_json(directory / "receipt.json")
    if (set(receipt) != {"kind", "schema_version", "plan_id", "window", "source_audio", "audio", "manifest"}
            or receipt["kind"] != "himr_hybrid_cpu_window_audio" or receipt["schema_version"] != 1
            or receipt["plan_id"] != plan["plan_id"] or receipt["window"] != window
            or receipt["source_audio"] != plan["recording"]["audio"]
            or receipt["audio"].get("path") != str(directory / "audio.flac")
            or receipt["manifest"].get("path") != str(directory / "recording-input.json")):
        raise CpuWorkerError("CPU derived window receipt differs from its source plan")
    manifest = _read_json(Path(receipt["manifest"]["path"]), receipt["manifest"]["sha256"])
    expected_manifest = _window_manifest(plan, window, receipt["audio"])
    if manifest != expected_manifest:
        raise CpuWorkerError("CPU window manifest differs from its exact sample coordinates")
    return receipt


def _window_manifest(plan: dict, window: dict, audio: dict) -> dict:
    samples = window["end_sample"] - window["start_sample"]
    digest = adapter.sha256_value(audio.get("sha256"), "derived audio digest")
    adapter.integer(audio.get("byte_count"), "derived audio bytes", 1, MAX_JSON_BYTES)
    return {"kind": "himr_longform_recording_input_manifest", "schema_version": 1, "boundary_candidates": [],
            "recording": {"recording_id": "hybridcpuwindow_" + _hash({"plan": plan["plan_id"], "window": window})[:32],
                          "media_id": "media_sha256_" + digest,
                          "input": {**copy.deepcopy(audio), "artifact_id": "artifact_hybrid_cpu_window_" + digest[:32],
                                    "sample_rate_hz": 16000, "channels": 1, "total_samples": samples,
                                    "duration_ms": (samples * 1000 + 8000) // 16000}}}


def _worker_environment() -> dict[str, str]:
    return {"PATH": "/usr/bin:/bin", "LANG": "C.UTF-8", "LC_ALL": "C.UTF-8", "TZ": "UTC"}


def _verify_derived_audio(plan: dict, window: dict, path: Path, expected: dict | None = None, *, lease_fds: tuple[int, ...] = ()) -> dict:
    with _open(path), _open(Path(plan["ffprobe"]["path"])):
        with recording_inputs._retain_file(path, "CPU window FLAC") as audio:
            if expected is not None and (audio.sha256, audio.identity.byte_count) != (expected["sha256"], expected["byte_count"]):
                raise CpuWorkerError("CPU derived audio bytes changed")
            with recording_inputs._retain_file(Path(plan["ffprobe"]["path"]), "CPU ffprobe", executable=True) as probe:
                if probe.sha256 != plan["ffprobe"]["sha256"]:
                    raise CpuWorkerError("CPU ffprobe changed")
                command = [probe.proc_path, "-v", "error", "-select_streams", "a:0", "-show_entries",
                           "stream=codec_name,sample_rate,channels,time_base,duration_ts", "-of", "json", audio.proc_path]
                try:
                    observed = subprocess.run(command, stdin=subprocess.DEVNULL, stdout=subprocess.PIPE, stderr=subprocess.PIPE,
                                              timeout=120, check=False, env=_worker_environment(),
                                              pass_fds=tuple(dict.fromkeys((probe.descriptor, audio.descriptor, *lease_fds))))
                finally:
                    recording_inputs._verify_after_probe(probe)
                    recording_inputs._verify_after_probe(audio)
                if observed.returncode != 0 or len(observed.stdout) > 65536:
                    raise CpuWorkerError("CPU window sample probe failed")
                value = json.loads(observed.stdout)
                streams = value.get("streams", []) if isinstance(value, dict) else []
                if len(streams) != 1 or not isinstance(streams[0], dict):
                    raise CpuWorkerError("CPU window must have one normalized audio stream")
                stream = streams[0]
                if any(stream.get(key) != expected_value for key, expected_value in {
                    "codec_name": "flac", "sample_rate": "16000", "channels": 1, "time_base": "1/16000",
                }.items()):
                    raise CpuWorkerError("CPU window must be normalized mono 16 kHz FLAC")
                samples = stream.get("duration_ts")
                if isinstance(samples, bool) or not isinstance(samples, int) or samples < 1:
                    raise CpuWorkerError("CPU window probe lacks an exact integer sample count")
                if samples != window["end_sample"] - window["start_sample"]:
                    raise CpuWorkerError("CPU extracted window sample count differs")
            return {"path": str(path), "sha256": audio.sha256, "byte_count": audio.identity.byte_count}


def _prepare_window(plan: dict, window: dict, *, lease_fds: tuple[int, ...] = ()) -> dict:
    directory = _derived_path(plan, window)
    _private(directory)
    if (directory / "receipt.json").exists() or (directory / "receipt.json").is_symlink():
        receipt = _derived_receipt(plan, window)
        _verify_derived_audio(plan, window, Path(receipt["audio"]["path"]), receipt["audio"], lease_fds=lease_fds)
        return receipt
    estimated_staging = (window["end_sample"] - window["start_sample"]) * 2 + 1024 * 1024
    if shutil.disk_usage(directory).free < estimated_staging + 128 * 1024 * 1024:
        raise CpuWorkerError("insufficient free space for bounded CPU window staging")
    descriptor, temporary = tempfile.mkstemp(prefix=".hybrid-cpu-flac-", dir=directory)
    staging = Path(temporary)
    source_path = Path(plan["recording"]["audio"]["path"])
    ffmpeg_path = Path(plan["ffmpeg"]["path"])
    try:
        with _open(source_path), _open(ffmpeg_path):
            with recording_inputs._retain_file(source_path, "CPU source recording") as source:
                if (source.sha256, source.identity.byte_count) != (plan["recording"]["audio"]["sha256"], plan["recording"]["audio"]["byte_count"]):
                    raise CpuWorkerError("CPU source recording changed")
                with recording_inputs._retain_file(ffmpeg_path, "CPU ffmpeg", executable=True) as ffmpeg:
                    if ffmpeg.sha256 != plan["ffmpeg"]["sha256"]:
                        raise CpuWorkerError("CPU ffmpeg changed")
                    command = [ffmpeg.proc_path, "-hide_banner", "-loglevel", "error", "-nostdin", "-y",
                               "-protocol_whitelist", "file,pipe", "-f", "flac", "-i", source.proc_path,
                               "-map", "0:a:0", "-vn", "-af",
                               f"atrim=start_sample={window['start_sample']}:end_sample={window['end_sample']},asetpts=PTS-STARTPTS",
                               "-ar", "16000", "-ac", "1", "-c:a", "flac", "-sample_fmt", "s16",
                               "-map_metadata", "-1", "-f", "flac", f"/proc/self/fd/{descriptor}"]
                    completed = subprocess.run(command, stdin=subprocess.DEVNULL, stdout=subprocess.DEVNULL,
                                               stderr=subprocess.PIPE, timeout=3600, check=False,
                                               env=_worker_environment(), pass_fds=tuple(dict.fromkeys((ffmpeg.descriptor, source.descriptor, descriptor, *lease_fds))))
                    recording_inputs._verify_after_probe(ffmpeg)
                    recording_inputs._verify_after_probe(source)
                    if completed.returncode != 0:
                        raise CpuWorkerError("CPU window extraction failed")
        os.fsync(descriptor)
        audio = _verify_derived_audio(plan, window, staging, lease_fds=lease_fds)
        destination = directory / "audio.flac"
        try:
            os.link(staging, destination, follow_symlinks=False)
        except FileExistsError:
            _verify_derived_audio(plan, window, destination, audio, lease_fds=lease_fds)
        audio["path"] = str(destination)
        _sync(directory)
        manifest_path = directory / "recording-input.json"
        _write(manifest_path, _window_manifest(plan, window, audio))
        receipt = {"kind": "himr_hybrid_cpu_window_audio", "schema_version": 1,
                   "plan_id": plan["plan_id"], "window": copy.deepcopy(window),
                   "source_audio": copy.deepcopy(plan["recording"]["audio"]),
                   "audio": audio, "manifest": _file_binding(manifest_path)}
        _write(directory / "receipt.json", receipt)
        return receipt
    finally:
        os.close(descriptor)
        staging.unlink(missing_ok=True)


def build_work_order(plan: dict, window: dict) -> dict:
    if window not in plan["windows"]:
        raise CpuWorkerError("CPU window is not in the plan")
    derived = _derived_receipt(plan, window)
    manifest = _window_manifest(plan, window, derived["audio"])
    identity = _hash({"plan_id": plan["plan_id"], "window": window})
    return {
        "schema_version": 1, "job_id": "hybridcpuwindow_" + identity[:32],
        "input": {"path": derived["audio"]["path"], "expected_sha256": derived["audio"]["sha256"],
                  "media_id": manifest["recording"]["media_id"], "artifact_id": manifest["recording"]["input"]["artifact_id"],
                  "parent_processing_run_id": "run_hybrid_cpu_window_" + identity[:32]},
        "engine": copy.deepcopy(plan["engine"]), "model": copy.deepcopy(plan["model"]),
        "window": {"offset_ms": 0, "duration_ms": window["duration_ms"]},
        "inference": {"language": "en", "threads": plan["threads"], "translate": False,
                      "split_on_word": True, "best_of": 5, "beam_size": 5,
                      "max_segment_characters": 0, "word_threshold": 0.01,
                      "entropy_threshold": 2.4, "logprob_threshold": -1.0, "no_speech_threshold": 0.6,
                      "temperature": 0.0, "temperature_increment": 0.2, "no_fallback": False,
                      "timeout_seconds": plan["timeout_seconds"]},
        "glossary": None, "catalog_context": None,
        "output": {"root": str(Path(plan["output_root"]) / "adapter-results")},
    }


def _result_binding(order: dict) -> dict:
    window = {**order["window"], "end_ms": order["window"]["offset_ms"] + order["window"]["duration_ms"]}
    recipe = {
        "contract_version": adapter.CONTRACT_VERSION, "implementation_version": adapter.IMPLEMENTATION_VERSION,
        "stage": adapter.STAGE, "descriptor_execution_policy": adapter.DESCRIPTOR_EXECUTION_POLICY,
        "engine": {"sha256": order["engine"]["expected_sha256"], "version": order["engine"]["version_label"],
                   "version_evidence": order["engine"]["version_evidence"], "build": order["engine"]["build"]},
        "model": {key: order["model"][key] for key in ("model_id", "name", "revision", "source", "license_label")}
                 | {"sha256": order["model"]["expected_sha256"]},
        "window": window, "inference": order["inference"], "glossary": None,
        "output_contract": "whisper.cpp-output-json-full-normalized-v1",
    }
    recipe_sha = hashlib.sha256(adapter.canonical_bytes(recipe)).hexdigest()
    recipe_id = "recipe_asr_whispercpp_" + recipe_sha[:32]
    order_sha = hashlib.sha256(adapter.canonical_bytes(order)).hexdigest()
    core = {"work_order_sha256": order_sha, "input_sha256": order["input"]["expected_sha256"],
            "input_media_id": order["input"]["media_id"], "input_artifact_id": order["input"]["artifact_id"],
            "parent_processing_run_id": order["input"]["parent_processing_run_id"],
            "recipe_id": recipe_id, "catalog_context": None}
    result_key = hashlib.sha256(adapter.canonical_bytes(core)).hexdigest()
    digest = order["input"]["expected_sha256"]
    result_path = Path(order["output"]["root"]) / "asr" / "whispercpp" / "sha256" / digest[:2] / digest / "results" / result_key / "result.json"
    return {"path": str(result_path), "result_key": result_key, "recipe_id": recipe_id,
            "work_order_sha256": order_sha, "window": window}


def _validate_result(plan: dict, window: dict, expected_file_sha256: str | None = None) -> tuple[dict, dict]:
    order = build_work_order(plan, window)
    binding = _result_binding(order)
    path = Path(binding["path"])
    result = _read_json(path, expected_file_sha256)
    if (result.get("status") != "completed" or result.get("dry_run") is not False
            or result.get("job_id") != order["job_id"] or result.get("work_order_sha256") != binding["work_order_sha256"]
            or result.get("window") != binding["window"] or result.get("catalog_context") is not None):
        raise CpuWorkerError("CPU result does not match its planned work order")
    for key, expected in (("input", order["input"]["expected_sha256"]),
                          ("engine", order["engine"]["expected_sha256"]), ("model", order["model"]["expected_sha256"])):
        if not isinstance(result.get(key), dict) or result[key].get("sha256") != expected:
            raise CpuWorkerError("CPU result has another source, engine, or model")
    try:
        reused = adapter.validate_completed_reuse(path, result_key=binding["result_key"], recipe_id=binding["recipe_id"], run_dir=path.parent)
    except adapter.ASRError as error:
        raise CpuWorkerError("CPU result artifacts fail immutable adapter replay") from error
    if result != reused:
        raise CpuWorkerError("CPU result changed during replay")
    raw = None
    for artifact in result["artifacts"]:
        artifact_path = Path(urllib.parse.unquote(urllib.parse.urlsplit(artifact["storage_uri"]).path))
        if artifact_path.parent != path.parent:
            raise CpuWorkerError("CPU artifact escapes its exact result directory")
        value = _read_json(artifact_path, artifact["sha256"])
        if artifact["artifact_kind"] == "whispercpp_output_json_full":
            raw = value
    normalized = adapter.normalize_engine_output(raw, requested_language="en", window=binding["window"])
    if result["transcript"] != normalized:
        raise CpuWorkerError("CPU transcript differs from its raw engine output")
    receipt = {"ordinal": window["ordinal"], "job_id": order["job_id"], "window": copy.deepcopy(window),
               "result": _file_binding(path)}
    return receipt, normalized


def _invoke_window(plan_path: Path, plan_sha256: str, window: dict, timeout_seconds: int, lock_descriptor: int, *, lease_fds: tuple[int, ...] = ()):
    command = [sys.executable, "-I", str(Path(__file__).resolve()), "--execute-window", str(window["ordinal"]),
               "--plan", str(plan_path), "--plan-sha256", plan_sha256, "--lock-fd", str(lock_descriptor)]
    for descriptor in lease_fds:
        command.extend(["--lease-fd", str(descriptor)])
    process = subprocess.Popen(command, stdin=subprocess.DEVNULL, stdout=subprocess.PIPE, stderr=subprocess.PIPE,
                               start_new_session=True, env=_worker_environment(), pass_fds=tuple(dict.fromkeys((lock_descriptor, *lease_fds))))
    try:
        stdout, _stderr = process.communicate(timeout=timeout_seconds + 600)
    except BaseException:
        # The child handles TERM by unwinding the native-command finally block,
        # which terminates its separate-session whisper.cpp process as well.
        adapter.terminate_group(process)
        raise
    if process.returncode != 0:
        raise CpuWorkerError("bounded CPU window failed; completed windows remain reusable")
    if len(stdout) > 65536:
        raise CpuWorkerError("CPU window returned oversized diagnostic output")


def _child_execute(plan: dict, ordinal: int, lock_descriptor: int, *, lease_fds: tuple[int, ...] = ()):
    plan = _validate_plan(plan)
    if isinstance(ordinal, bool) or not isinstance(ordinal, int) or not 0 <= ordinal < len(plan["windows"]):
        raise CpuWorkerError("CPU window ordinal is invalid")
    lock = Path(plan["output_root"]) / ".lock"
    if _witness(os.fstat(lock_descriptor)) != _witness(lock.stat(follow_symlinks=False)):
        raise CpuWorkerError("CPU child did not inherit its workspace lock")
    order = adapter.validate_work_order(build_work_order(plan, plan["windows"][ordinal]))
    with adapter.retained_verified_file(Path(plan["ffprobe"]["path"]), plan["ffprobe"]["sha256"], "CPU ffprobe", executable=True) as probe:
        original_require, original_run = adapter.require_ffprobe, adapter.run_command

        def pinned_run(command, **kwargs):
            if command[0] == str(probe.path):
                command[0] = probe.proc_path
                kwargs["pass_fds"] = tuple(dict.fromkeys((*kwargs.get("pass_fds", ()), probe.descriptor)))
            descriptors = tuple(dict.fromkeys((*kwargs.get("pass_fds", ()), lock_descriptor, *lease_fds)))
            process = subprocess.Popen(command, stdin=subprocess.DEVNULL, stdout=subprocess.PIPE, stderr=subprocess.PIPE,
                                       text=True, encoding="utf-8", errors="replace", env=kwargs["environment"],
                                       start_new_session=True, pass_fds=descriptors)
            try:
                stdout, stderr = process.communicate(timeout=kwargs["timeout_seconds"])
                if process.returncode != 0:
                    raise CpuWorkerError("CPU native command failed")
                return subprocess.CompletedProcess(command, process.returncode, adapter.bounded_output(stdout), adapter.bounded_output(stderr))
            finally:
                if process.poll() is None:
                    adapter.terminate_group(process)

        try:
            # Only this isolated child module instance is adapted. The original
            # adapter source and every other running Python process stay intact.
            adapter.require_ffprobe = lambda: probe.path
            adapter.run_command = pinned_run
            adapter.run_asr(order, dry_run=False)
        finally:
            adapter.require_ffprobe, adapter.run_command = original_require, original_run
            adapter.verify_retained_file(probe, "CPU ffprobe")


def run_cpu(recording_input: Path, expected_sha256: str, *, output_root: Path,
            engine: dict, model: dict, ffprobe: dict, ffmpeg: dict, threads: int = 6,
            timeout_seconds: int = 7200, window_seconds: int = 1800,
            max_windows: int = 1, lease_fds: tuple[int, ...] = ()) -> dict:
    """Complete at most max_windows new CPU windows; safely reuse saved results."""
    if isinstance(max_windows, bool) or not isinstance(max_windows, int) or not 1 <= max_windows <= 100:
        raise CpuWorkerError("max_windows must be an integer from one to 100")
    if not isinstance(lease_fds, tuple) or any(isinstance(descriptor, bool) or not isinstance(descriptor, int)
                                              or not stat.S_ISREG(os.fstat(descriptor).st_mode) for descriptor in lease_fds):
        raise CpuWorkerError("CPU inherited leases must be retained regular-file descriptors")
    plan = build_cpu_plan(recording_input, expected_sha256, output_root=output_root, engine=engine, model=model,
                          ffprobe=ffprobe, ffmpeg=ffmpeg, threads=threads, timeout_seconds=timeout_seconds, window_seconds=window_seconds)
    root = Path(plan["output_root"])
    _private(root)
    descriptor = os.open(root / ".lock", os.O_CREAT | os.O_RDWR | os.O_NOFOLLOW | os.O_NONBLOCK, 0o600)
    try:
        info = os.fstat(descriptor)
        if not stat.S_ISREG(info.st_mode) or info.st_uid != os.getuid() or info.st_nlink != 1 or info.st_mode & 0o077:
            raise CpuWorkerError("invalid CPU workspace lock")
        try:
            fcntl.flock(descriptor, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError:
            raise CpuWorkerError("another CPU worker owns this recording") from None
        _write(root / "plan.json", plan)
        _private(root / "windows")
        _private(root / "work-orders")
        receipts = []
        transcripts = []
        executed = 0
        for window in plan["windows"]:
            checkpoint_path = root / "windows" / f"{window['ordinal']:05d}.json"
            existing = _read_json(checkpoint_path) if checkpoint_path.exists() or checkpoint_path.is_symlink() else None
            if existing is None:
                if executed >= max_windows:
                    break
                _prepare_window(plan, window, lease_fds=tuple(dict.fromkeys((descriptor, *lease_fds))))
            order = build_work_order(plan, window)
            _write(root / "work-orders" / f"{window['ordinal']:05d}.json", order)
            result_path = Path(_result_binding(order)["path"])
            if existing is None:
                if not result_path.exists():
                    _invoke_window(root / "plan.json", _hash(plan), window, timeout_seconds, descriptor, lease_fds=lease_fds)
                    executed += 1
            expected_result_sha = existing["result"]["sha256"] if existing is not None else None
            receipt, transcript = _validate_result(plan, window, expected_result_sha)
            _write(checkpoint_path, receipt)
            receipts.append(receipt)
            transcripts.append(transcript)
        status = "completed" if len(receipts) == len(plan["windows"]) else "progress"
        outcome = {"status": status, "plan_id": plan["plan_id"], "recording_id": plan["recording"]["recording_id"],
                   "completed_windows": len(receipts), "total_windows": len(plan["windows"]),
                   "executed_windows": executed, "cpu_only": True, "source_controller_mutated": False}
        if status == "completed":
            segments = []
            for receipt, transcript in zip(receipts, transcripts):
                offset = receipt["window"]["offset_ms"]
                for segment in transcript["segments"]:
                    projected = copy.deepcopy(segment)
                    projected.update(source_segment_ordinal=segment["ordinal"], ordinal=len(segments), window_ordinal=receipt["ordinal"],
                                     start_ms=segment["start_ms"] + offset, end_ms=segment["end_ms"] + offset)
                    for token in projected["tokens"]:
                        for field in ("start_ms", "end_ms"):
                            if token[field] is not None:
                                token[field] += offset
                    segments.append(projected)
            recording_transcript = {
                "kind": "himr_hybrid_cpu_recording_transcript", "schema_version": 1,
                "plan_id": plan["plan_id"], "recording": copy.deepcopy(plan["recording"]),
                "sources": receipts, "segments": segments,
                "text": " ".join(segment["text"].strip() for segment in segments if segment["text"].strip()),
                "timing": copy.deepcopy(plan["timing"]),
                "overlap": {"boundary_overruns_may_repeat_text": True, "semantic_deduplication_claimed": False},
                "policy": copy.deepcopy(POLICY),
            }
            transcript_path = root / "recording-transcript.json"
            _write(transcript_path, recording_transcript)
            completion = {"kind": "himr_hybrid_cpu_completion", "schema_version": 1,
                          "plan_id": plan["plan_id"], "recording_id": plan["recording"]["recording_id"],
                          "media_id": plan["recording"]["media_id"], "windows": receipts,
                          "transcript": _file_binding(transcript_path), "policy": copy.deepcopy(POLICY)}
            _write(root / "completion.json", completion)
            outcome["completion"] = _file_binding(root / "completion.json")
            outcome["transcript"] = completion["transcript"]
        return outcome
    except (adapter.ASRError, recording_inputs.LongformInputError, OSError, KeyError, TypeError, ValueError, subprocess.SubprocessError) as error:
        raise CpuWorkerError("CPU worker evidence or bounded execution failed; no completed outputs were removed") from error
    finally:
        os.close(descriptor)


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--execute-window", type=int, required=True)
    parser.add_argument("--plan", type=Path, required=True)
    parser.add_argument("--plan-sha256", required=True)
    parser.add_argument("--lock-fd", type=int, required=True)
    parser.add_argument("--lease-fd", type=int, action="append", default=[])
    args = parser.parse_args(argv)
    try:
        def terminate(_signum, _frame):
            raise KeyboardInterrupt("CPU worker termination")
        signal.signal(signal.SIGTERM, terminate)
        _child_execute(_read_json(_path(args.plan), args.plan_sha256), args.execute_window, args.lock_fd, lease_fds=tuple(args.lease_fd))
    except Exception:
        print("HybridCpuError: window failed; private adapter evidence is retained", file=sys.stderr)
        return 2
    print(json.dumps({"status": "completed", "window_ordinal": args.execute_window}))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
