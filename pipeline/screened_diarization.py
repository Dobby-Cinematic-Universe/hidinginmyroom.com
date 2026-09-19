"""Finite, private diarization of verified completed archive-screen candidates.

No discovery, background watcher, acquisition/ASR mutation, model download, or
publication. Planning snapshots completed screen evidence; execution is explicit
and requires an offline admitted Community-1 bundle and a memory-limited process
tree. Each recording runs whole: speaker labels are never stitched across jobs.
"""
from __future__ import annotations

import argparse
from contextlib import contextmanager
import ctypes
import errno
import fcntl
import hashlib
import json
import math
import os
from pathlib import Path
import re
import resource
import signal
import stat
import subprocess
import sys
import tempfile
import time
import uuid

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))
from pipeline import speaker_screen as screen
from pipeline import speaker_screen_paths as paths
from pipeline import speaker_screen_batch as old_batch
from pipeline import speaker_screen_archive_guided as archive
from pipeline import screened_diarization_core as core
from pipeline import screened_diarization_engine as engine
from pipeline import screened_diarization_selection as selection

Error = screen.ScreenError
MARKER = {"kind": "himr_private_screened_diarization_workspace", "schema_version": 1}
IMPLEMENTATION_NAMES = archive.IMPLEMENTATION_NAMES + (
    "speaker_screen_campaign.py", "screened_diarization.py", "screened_diarization_core.py",
    "screened_diarization_engine.py", "screened_diarization_selection.py")
DEFAULT_EXECUTION = {"device": "cuda", "gpu_uuid": None, "threads": 1,
    "segmentation_batch_size": 1, "embedding_batch_size": 1, "cuda_memory_fraction": 0.5}
DEFAULT_LIMITS = {"max_recordings": 128, "max_jobs_per_run": 8, "max_run_seconds": 28800,
    "job_timeout_seconds": 14400, "decode_timeout_seconds": 7200, "max_duration_ms": 86400000,
    "max_pcm_bytes": 86400000 * 32, "max_waveform_bytes": 86400000 * 64,
    "host_memory_max_bytes": 12 * 1024**3, "min_free_bytes": 8 * 1024**3, "max_turns": 20000}
NORMALIZATION_FILTER = "aresample=16000:async=1:first_pts=0:min_comp=0.0000625:min_hard_comp=0.0000625:max_soft_comp=0"
NORMALIZATION_TIMELINE = {"source_origin_ms": 0, "initial_encoder_priming_padding_or_trim": True,
    "timestamp_gap_silence_insertion_or_overlap_trim": True, "hard_compensation_threshold_output_samples": 1,
    "resample_async": 1, "soft_time_stretching": False, "ffmpeg_audio_filter": NORMALIZATION_FILTER,
    "tail_padding": False, "duration_bound": "screened_audio_eof_integer_ms",
    "source_modified": False, "whole_recording": True, "chunk_stitching": False}
PLAN_SEMANTICS = {"private": True, "production_quality_validated": False, "source_mutation": False,
    "screening_is_selection_not_human_review": True, "automatic_speaker_count_by_default": True,
    "named_identity_inferred": False, "cross_recording_speaker_matching": False,
    "publication_authority": False, "whole_recording_jobs": True, "chunk_stitching": False,
    "new_screen_completions_require_new_plan": True, "model_downloads": False}


def implementation():
    return {name: hashlib.sha256((Path(__file__).parent / name).read_bytes()).hexdigest()
            for name in IMPLEMENTATION_NAMES}


def binding(path, maximum=screen.MAX_JSON, deadline=None):
    path = screen.path_value(path)
    with screen.opened(path) as fd:
        return {"path": str(path), "sha256": screen.hash_fd(fd, maximum, deadline or time.monotonic() + 60)}


def _check_reference(ref, maximum=screen.MAX_JSON, *, executable=False):
    screen.file_binding(ref)
    with screen.opened(ref["path"], executable=executable) as fd:
        if screen.hash_fd(fd, maximum, time.monotonic() + 60) != ref["sha256"]:
            raise Error("diarization input SHA-256 differs")


def _mkdir(path):
    with paths.retained_directory(path.parent) as fd:
        try:
            os.mkdir(path.name, 0o700, dir_fd=fd)
            os.fsync(fd)
        except FileExistsError:
            pass
    with paths.retained_directory(path):
        pass


def _workspace(root):
    _mkdir(root)
    with paths.retained_directory(root) as fd:
        if not screen.exists(root / "workspace.json"):
            if list(os.scandir(fd)):
                raise Error("refusing nonempty unmarked diarization workspace")
            screen.write_immutable(root / "workspace.json", MARKER)
        if screen.read_json(root / "workspace.json") != MARKER:
            raise Error("diarization workspace marker differs")


@contextmanager
def _locked(root):
    with paths.retained_directory(root) as directory:
        fd = os.open("execution.lock", os.O_CREAT | os.O_RDWR | os.O_NOFOLLOW | os.O_CLOEXEC, 0o600, dir_fd=directory)
        try:
            info = os.fstat(fd)
            if (not stat.S_ISREG(info.st_mode) or info.st_nlink != 1 or info.st_uid != os.getuid()
                    or stat.S_IMODE(info.st_mode) != 0o600 or info.st_size):
                raise Error("unsafe diarization execution lock")
            try:
                fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
            except BlockingIOError:
                raise Error("another diarization parent or child owns this workspace") from None
            yield fd
        finally:
            os.close(fd)


def snapshot(root, value):
    body = screen.canonical({"kind": "himr_screened_diarization_status", "schema_version": 1,
                             "updated_unix": time.time(), **value})
    if len(body) > screen.MAX_JSON:
        raise Error("diarization status exceeds JSON bound")
    with paths.retained_directory(root) as fd:
        name = ".status-" + uuid.uuid4().hex
        target = os.open(name, os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW, 0o600, dir_fd=fd)
        try:
            with os.fdopen(target, "wb") as stream:
                stream.write(body)
                stream.flush()
                os.fsync(stream.fileno())
            os.replace(name, "status.json", src_dir_fd=fd, dst_dir_fd=fd)
            os.fsync(fd)
        finally:
            try:
                os.unlink(name, dir_fd=fd)
            except FileNotFoundError:
                pass


def validate_request(value):
    screen.exact(value, {"kind", "schema_version", "source", "include_uncertain", "media_ids",
        "state_root", "model_bundle", "python", "ffmpeg", "speaker_bounds", "execution", "limits",
        "blocking_units", "retain_normalized_audio", "purpose"}, "screened diarization request")
    if (value["kind"] != "himr_screened_diarization_request" or type(value["schema_version"]) is not int
            or value["schema_version"] != 1 or value["purpose"] != "private_unvalidated_pilot"):
        raise Error("unsupported private diarization request")
    screen.exact(value["source"], {"kind", "binding"}, "screening source")
    if value["source"]["kind"] not in ("campaign", "batch"):
        raise Error("screening source must be an explicit campaign or batch")
    screen.file_binding(value["source"]["binding"])
    screen.file_binding(value["ffmpeg"])
    for key in ("include_uncertain", "retain_normalized_audio"):
        if type(value[key]) is not bool:
            raise Error(key + " must be boolean")
    if value["media_ids"] is not None and (not isinstance(value["media_ids"], list)
            or not 1 <= len(value["media_ids"]) <= 16384
            or any(not isinstance(item, str) or not screen.IDENTIFIER.fullmatch(item) for item in value["media_ids"])
            or len(set(value["media_ids"])) != len(value["media_ids"])):
        raise Error("invalid explicit recording filter")
    for key in ("model_bundle", "python"):
        if value[key] is not None:
            screen.file_binding(value[key])
    root = screen.path_value(value["state_root"])
    protected = [value["source"]["binding"], value["ffmpeg"]]
    protected += [value[key] for key in ("model_bundle", "python") if value[key] is not None]
    if any(old_batch._overlap(root, Path(ref["path"])) for ref in protected):
        raise Error("diarization workspace overlaps an input")
    execution = value["execution"]
    screen.exact(execution, DEFAULT_EXECUTION, "diarization execution")
    if execution["device"] not in ("cpu", "cuda"):
        raise Error("diarization device must be cpu or cuda")
    if execution["device"] == "cpu":
        if execution["gpu_uuid"] is not None:
            raise Error("CPU request must not select a GPU")
    elif not isinstance(execution["gpu_uuid"], str) or not re.fullmatch(r"GPU-[0-9a-fA-F-]{36}", execution["gpu_uuid"]):
        raise Error("CUDA requires an explicit GPU UUID")
    for key, high in (("threads", 4), ("segmentation_batch_size", 16), ("embedding_batch_size", 16)):
        screen.integer(execution[key], 1, high, key)
    fraction = execution["cuda_memory_fraction"]
    if type(fraction) not in (int, float) or not math.isfinite(fraction) or not 0.1 <= fraction <= 0.75:
        raise Error("CUDA allocator fraction must be finite in 0.1..0.75")
    limits = value["limits"]
    screen.exact(limits, DEFAULT_LIMITS, "diarization limits")
    for key, low, high in (("max_recordings", 1, 128), ("max_jobs_per_run", 1, 128),
            ("max_run_seconds", 30, 604800), ("job_timeout_seconds", 10, 86400),
            ("decode_timeout_seconds", 10, 86400), ("max_duration_ms", 1, 86400000),
            ("max_pcm_bytes", 32000, 86400000 * 32), ("max_waveform_bytes", 64000, 86400000 * 64),
            ("host_memory_max_bytes", 2 * 1024**3, 32 * 1024**3),
            ("min_free_bytes", 1024**3, 64 * 1024**3), ("max_turns", 1, 100000)):
        screen.integer(limits[key], low, high, key)
    if (not isinstance(value["blocking_units"], list) or len(value["blocking_units"]) > 16
            or any(not isinstance(unit, str) or not re.fullmatch(r"[a-zA-Z0-9_.@-]{1,160}\.service", unit)
                   for unit in value["blocking_units"])):
        raise Error("invalid blocking service names")
    if not isinstance(value["speaker_bounds"], list) or len(value["speaker_bounds"]) > 128:
        raise Error("speaker bounds require a finite reviewed list")
    seen = set()
    for row in value["speaker_bounds"]:
        screen.exact(row, {"media_sha256", "bounds"}, "reviewed speaker bound")
        if row["media_sha256"] in seen:
            raise Error("duplicate reviewed speaker bounds")
        seen.add(row["media_sha256"])
        core.validate_speaker_bounds(row["bounds"], media_sha256=row["media_sha256"])
    return value


def _select(request):
    function = selection.select_campaign if request["source"]["kind"] == "campaign" else selection.select_batch
    return function(request["source"]["binding"], include_uncertain=request["include_uncertain"], media_ids=request["media_ids"])


def _bounds(request, media_sha256):
    chosen = next((row["bounds"] for row in request["speaker_bounds"] if row["media_sha256"] == media_sha256), None)
    result = core.validate_speaker_bounds(chosen, media_sha256=media_sha256)
    if result["review"] is not None:
        _check_reference(result["review"]["evidence"])
    return result


def resource_deficits(record, limits):
    duration = record["recording"]["duration_ms"]
    pcm = duration * 32
    # A conservative admission estimate, not a measured memory guarantee. The
    # hard cgroup remains authoritative; a long file is never secretly chunked.
    estimated = 2 * 1024**3 + 5 * pcm
    result = []
    for failed, reason in ((duration > limits["max_duration_ms"], "recording_duration_limit"),
                          (pcm > limits["max_pcm_bytes"], "normalized_pcm_limit"),
                          (2 * pcm > limits["max_waveform_bytes"], "float_waveform_limit"),
                          (estimated > limits["host_memory_max_bytes"], "estimated_host_memory_limit")):
        if failed:
            result.append(reason)
    return {"blockers": result, "pcm_bytes": pcm, "estimated_host_memory_bytes": estimated}


def _planned_jobs(request, selected, ref):
    root = Path(request["state_root"])
    selection.validate_selection_snapshot(selected, source=request["source"],
        include_uncertain=request["include_uncertain"], media_ids=request["media_ids"])
    jobs = []
    for record in selected["records"][:request["limits"]["max_recordings"]]:
        protected = [Path(record["recording"]["path"])]
        protected += [Path(record[key]["path"]) for key in
            ("screening_result", "screening_plan", "screening_source_binding", "screening_batch")]
        protected.append(Path(record["screening_batch"]["path"]).parent)
        if any(old_batch._overlap(root, path) for path in protected):
            raise Error("diarization workspace overlaps source media or screening evidence")
        reviewed = _bounds(request, record["recording"]["sha256"])
        if reviewed["review"] and old_batch._overlap(root, Path(reviewed["review"]["evidence"]["path"])):
            raise Error("diarization output overlaps reviewed evidence")
        payload = {"screened_record": record, "speaker_bounds": reviewed,
                   "resource_admission": resource_deficits(record, request["limits"])}
        jobs.append({"job_id": "diarjob_" + screen.digest({"request": ref, **payload})[:32], **payload})
    return jobs


def _check_bundle_paths(request):
    if request["model_bundle"] is None:
        return
    admitted = engine.admit_bundle(request["model_bundle"])
    root = Path(request["state_root"])
    protected = [Path(row["path"]) for row in admitted["files"].values()]
    protected += [Path(admitted[key]["path"]) for key in ("runtime_binding", "review_evidence", "license")]
    runtime = admitted["runtime"]
    protected += [Path(row["path"]) for row in runtime["installed_files"]]
    protected += [Path(row["wheel"]["path"]) for row in runtime["packages"]]
    protected.append(Path(runtime["python"]["path"]))
    if any(old_batch._overlap(root, path) for path in protected):
        raise Error("diarization workspace overlaps a model or runtime artifact")
    if request["python"] is not None and request["python"] != {
            key: runtime["python"][key] for key in ("path", "sha256")}:
        raise Error("requested Python differs from the model bundle runtime")


def create_plan(request_path, expected_sha256):
    ref = {"path": str(screen.path_value(request_path)), "sha256": expected_sha256}
    request = validate_request(screen.read_json(Path(ref["path"]), ref["sha256"]))
    root = Path(request["state_root"])
    if old_batch._overlap(root, Path(ref["path"])):
        raise Error("request must be outside diarization workspace")
    if any(old_batch._overlap(root, Path(__file__).parent / name) for name in IMPLEMENTATION_NAMES):
        raise Error("workspace overlaps pipeline implementation")
    _check_reference(request["ffmpeg"], 256 * 1024**2, executable=True)
    selected = _select(request)
    jobs = _planned_jobs(request, selected, ref)
    blockers = []
    if request["model_bundle"] is None:
        blockers.append("approved_community_1_model_bundle_not_configured")
    else:
        _check_bundle_paths(request)
    if request["python"] is None:
        blockers.append("isolated_diarization_python_not_configured")
    else:
        _check_reference(request["python"], 256 * 1024**2, executable=True)
    value = {"kind": "himr_screened_diarization_plan", "schema_version": 1,
        "request": ref, "request_value": request, "implementation": implementation(),
        "selection": selected, "jobs": jobs, "readiness_blockers": blockers,
        "deferred_selected_count": max(0, len(selected["records"]) - len(jobs)),
        "semantics": dict(PLAN_SEMANTICS)}
    plan = {**value, "plan_id": "screeneddiar_" + screen.digest(value)[:32]}
    if len(screen.canonical(plan)) > screen.MAX_JSON:
        raise Error("diarization plan exceeds metadata bound; use a smaller explicit media filter")
    _workspace(root)
    with _locked(root):
        screen.write_immutable(root / "plan.json", plan)
        snapshot(root, {"state": "blocked_setup" if blockers else "planned", "plan_id": plan["plan_id"],
            "planned_recordings": len(jobs), "blockers": blockers, "completed_recordings": 0})
    return plan


def load_plan(path, expected):
    value = screen.read_json(screen.path_value(path), expected)
    screen.exact(value, {"kind", "schema_version", "request", "request_value", "implementation", "selection",
        "jobs", "readiness_blockers", "deferred_selected_count", "semantics", "plan_id"}, "diarization plan")
    if value["kind"] != "himr_screened_diarization_plan" or type(value["schema_version"]) is not int or value["schema_version"] != 1:
        raise Error("unsupported diarization plan")
    payload = {key: item for key, item in value.items() if key != "plan_id"}
    if value["plan_id"] != "screeneddiar_" + screen.digest(payload)[:32] or value["implementation"] != implementation():
        raise Error("diarization plan or implementation changed")
    request = validate_request(value["request_value"])
    blockers = (["approved_community_1_model_bundle_not_configured"] if request["model_bundle"] is None else [])
    blockers += ["isolated_diarization_python_not_configured"] if request["python"] is None else []
    if value["readiness_blockers"] != blockers or screen.canonical(value["semantics"]) != screen.canonical(PLAN_SEMANTICS):
        raise Error("diarization readiness or private policy claims differ")
    if screen.read_json(Path(value["request"]["path"]), value["request"]["sha256"]) != request:
        raise Error("diarization request changed")
    root = Path(request["state_root"])
    if Path(path) != root / "plan.json" or screen.read_json(root / "workspace.json") != MARKER:
        raise Error("diarization plan is outside marked workspace")
    if not isinstance(value["jobs"], list) or len(value["jobs"]) > request["limits"]["max_recordings"]:
        raise Error("invalid bounded diarization job list")
    expected_jobs = _planned_jobs(request, value["selection"], value["request"])
    if (screen.canonical(value["jobs"]) != screen.canonical(expected_jobs)
            or type(value["deferred_selected_count"]) is not int
            or value["deferred_selected_count"] != max(0, len(value["selection"]["records"]) - len(expected_jobs))):
        raise Error("diarization jobs differ from the exact request and screened selection")
    _check_bundle_paths(request)
    return value


def _service_blockers(units):
    blocked = []
    for unit in units:
        result = subprocess.run(["/usr/bin/systemctl", "--user", "show", unit, "-p", "ActiveState", "--value"],
            stdin=subprocess.DEVNULL, capture_output=True, timeout=5, check=False)
        state = result.stdout.decode("ascii", errors="strict").strip()
        if result.returncode != 0 or state not in ("inactive", "failed"):
            blocked.append(unit + ":" + (state or "cannot_inspect"))
    return blocked


def verify_memory_limit(maximum):
    # Inspect the entire unified hierarchy, including inherited ceilings. Never
    # use RLIMIT_AS for CUDA's much larger virtual address reservations.
    before = Path("/proc/self/cgroup").read_text()
    rows = [line[3:] for line in before.splitlines() if line.startswith("0::")]
    if len(rows) != 1 or not rows[0].startswith("/") or ".." in rows[0].split("/"):
        raise Error("a verifiable unified memory cgroup is required")
    mounts = []
    for line in Path("/proc/self/mountinfo").read_text().splitlines():
        fields = line.split()
        if "-" in fields and fields[fields.index("-") + 1] == "cgroup2":
            if fields[3] != "/" or "\\" in fields[4]:
                raise Error("cannot inspect hidden cgroup ancestors")
            mounts.append(Path(fields[4]))
    if len(mounts) != 1:
        raise Error("one unified cgroup mount is required")
    mount = mounts[0]
    current, memory, swap = mount / rows[0].lstrip("/"), [], []
    for _ in range(128):
        for name, values in (("memory.max", memory), ("memory.swap.max", swap)):
            try:
                raw = (current / name).read_text().strip()
            except FileNotFoundError:
                if current == mount:
                    continue
                raise Error("memory cgroup controls unavailable") from None
            if raw != "max":
                if not re.fullmatch(r"0|[1-9][0-9]{0,19}", raw):
                    raise Error("invalid memory cgroup limit")
                values.append(int(raw))
        if current == mount:
            break
        current = current.parent
    else:
        raise Error("memory cgroup hierarchy exceeds bound")
    if (not memory or not 0 < min(memory) <= maximum or not swap or min(swap) != 0
            or Path("/proc/self/cgroup").read_text() != before):
        raise Error("run in a dedicated memory-limited cgroup with no swap")
    return {"memory_max_bytes": min(memory), "memory_swap_max_bytes": min(swap)}


def _command(command, *, inherited_fds=(), lock_fd, timeout, maximum, environment, output=None):
    parent = os.getpid()
    process = None
    with tempfile.TemporaryFile() as capture, tempfile.TemporaryFile() as errors:
        try:
            inherited_mask = signal.pthread_sigmask(signal.SIG_BLOCK, set())
            def setup():
                screen.die_with_parent(parent)
                signal.pthread_sigmask(signal.SIG_SETMASK, inherited_mask)
                resource.setrlimit(resource.RLIMIT_CORE, (0, 0))
                resource.setrlimit(resource.RLIMIT_FSIZE, (maximum + 1, maximum + 1))
                screen.deny_internet()
            with old_batch._deferred_launch_signals():
                process = subprocess.Popen(command, stdin=subprocess.DEVNULL, stdout=capture if output is None else output,
                    stderr=errors, close_fds=True, pass_fds=tuple(inherited_fds) + (lock_fd,),
                    start_new_session=True, preexec_fn=setup, env=environment)
            process.wait(timeout=timeout)
            errors.seek(0)
            diagnostics = errors.read(min(maximum, 1024**2))
            if re.search(rb"input/output error|\[errno 5\]", diagnostics, re.IGNORECASE):
                raise OSError(errno.EIO, "diarization subprocess reported storage I/O failure")
            if os.fstat(errors.fileno()).st_size > 1024**2:
                raise Error("diarization subprocess diagnostics exceeded bound")
            if process.returncode:
                raise Error("bounded diarization subprocess failed: " + diagnostics[-3000:].decode(errors="replace"))
            if output is not None:
                if os.fstat(output).st_size > maximum:
                    raise Error("diarization subprocess output exceeded bound")
                return None
            capture.seek(0)
            result = capture.read(maximum + 1)
            if len(result) > maximum:
                raise Error("diarization subprocess output exceeded bound")
            return result
        finally:
            with old_batch._deferred_launch_signals():
                if process is not None:
                    # Terminate the owned group even when the immediate child
                    # exited, so an inherited-lock grandchild cannot survive.
                    try:
                        os.killpg(process.pid, signal.SIGKILL)
                    except ProcessLookupError:
                        pass
                    process.wait(timeout=10)


def _environment(execution, work):
    return {"PATH": "/usr/bin:/bin", "LANG": "C.UTF-8", "PYTHONNOUSERSITE": "1",
        "PYTHONDONTWRITEBYTECODE": "1", "PYANNOTE_METRICS_ENABLED": "0", "HF_HUB_OFFLINE": "1",
        "HF_HUB_DISABLE_TELEMETRY": "1", "TRANSFORMERS_OFFLINE": "1", "HF_HUB_DISABLE_IMPLICIT_TOKEN": "1",
        "HF_HOME": str(work / "offline-cache"), "TORCH_HOME": str(work / "offline-cache"),
        "TORCH_FORCE_WEIGHTS_ONLY_LOAD": "1", "OMP_NUM_THREADS": str(execution["threads"]),
        "MKL_NUM_THREADS": str(execution["threads"]), "OPENBLAS_NUM_THREADS": str(execution["threads"]),
        "CUDA_VISIBLE_DEVICES": execution["gpu_uuid"] if execution["device"] == "cuda" else ""}


def normalize_audio(record, request, work, *, lock_fd, deadline):
    """One whole-source FLAC normalization, then exact PCM, never ASR or stitching."""
    source_ref = record["recording"]
    duration = source_ref["duration_ms"]
    expected_bytes = duration * 32
    limits = request["limits"]
    if resource_deficits(record, limits)["blockers"]:
        raise Error("whole-recording resource limit; no automatic chunking")
    environment = _environment({**request["execution"], "device": "cpu", "gpu_uuid": None}, work)
    flac_path, pcm_path = work / "normalized.flac", work / "normalized.pcm"
    with paths.retained_directory(work) as directory, screen.opened(source_ref["path"]) as source, \
            screen.opened(request["ffmpeg"]["path"], executable=True) as tool:
        if screen.witness(source) != record["source_witness"]:
            raise Error("source changed after screening")
        tool_before = screen.witness(tool)
        if screen.hash_fd(tool, 256 * 1024**2, deadline) != request["ffmpeg"]["sha256"]:
            raise Error("normalization decoder changed")
        for name, input_fd, output_format in ((flac_path.name, source, "flac"), (pcm_path.name, None, "s16le")):
            output_fd = os.open(name, os.O_RDWR | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW, 0o600, dir_fd=directory)
            audio_fd = os.open(flac_path.name, os.O_RDONLY | os.O_NOFOLLOW, dir_fd=directory) if input_fd is None else input_fd
            try:
                command = [f"/proc/self/fd/{tool}", "-v", "error", "-nostdin", "-threads", "1", "-filter_threads", "1",
                    "-hwaccel", "none", "-protocol_whitelist", "file,pipe", "-format_whitelist", screen.FORMATS,
                    "-i", f"/proc/self/fd/{audio_fd}", "-map", "0:a:0", "-vn", "-sn", "-dn", "-ac", "1", "-ar", "16000"]
                if output_format == "flac":
                    # Preserve PTS, including startup/packet gaps. Corrections
                    # insert silence or trim timestamp overlaps; soft stretching
                    # and unconstrained tail padding remain disabled.
                    command += ["-af", NORMALIZATION_FILTER, "-t", f"{duration / 1000:.3f}",
                                "-sample_fmt", "s16", "-c:a", "flac", "-f", "flac", "pipe:1"]
                else:
                    command += ["-c:a", "pcm_s16le", "-f", "s16le", "pipe:1"]
                _command(command, inherited_fds=(source, tool, audio_fd) if audio_fd != source else (source, tool),
                    lock_fd=lock_fd, timeout=max(0.01, min(limits["decode_timeout_seconds"], deadline - time.monotonic())),
                    maximum=min(limits["max_pcm_bytes"] + 1024**2, expected_bytes + 1024**2),
                    environment=environment, output=output_fd)
                os.fsync(output_fd)
                count = os.fstat(output_fd).st_size
                if output_format == "s16le" and count != expected_bytes:
                    raise Error("normalized audio does not match full screened timeline; review required")
                if not 0 < count <= limits["max_pcm_bytes"] + 1024**2:
                    raise Error("normalized audio exceeds bounded size")
                os.fchmod(output_fd, 0o400)
            finally:
                os.close(output_fd)
                if audio_fd != source:
                    os.close(audio_fd)
        with screen.opened(source_ref["path"]) as current:
            if screen.witness(source) != record["source_witness"] or screen.witness(current) != record["source_witness"]:
                raise Error("source changed during normalization")
        if screen.witness(tool) != tool_before:
            raise Error("normalization decoder changed during decode")
    return {"flac": {**binding(flac_path, limits["max_pcm_bytes"] + 1024**2, deadline), "byte_count": flac_path.stat().st_size},
        "pcm": {**binding(pcm_path, limits["max_pcm_bytes"], deadline), "byte_count": expected_bytes},
        "duration_ms": duration, "sample_rate": 16000, "channels": 1, "pcm_format": "s16le",
        "source_witness": record["source_witness"], "source_sha256_reverified": False,
        "timeline": dict(NORMALIZATION_TIMELINE)}


@contextmanager
def _attempt(folder, retain):
    attempt = Path(tempfile.mkdtemp(prefix="attempt-", dir=folder))
    try:
        yield attempt
    finally:
        if not retain:
            with paths.retained_directory(attempt) as fd:
                for name in ("normalized.pcm", "normalized.flac"):
                    try:
                        info = os.stat(name, dir_fd=fd, follow_symlinks=False)
                    except FileNotFoundError:
                        continue
                    if not stat.S_ISREG(info.st_mode) or info.st_nlink != 1 or info.st_uid != os.getuid():
                        raise Error("unsafe generated audio cleanup target")
                    os.unlink(name, dir_fd=fd)
                os.fsync(fd)


def _validate_audio_receipt(audio, job, folder, worker_ref, engine_ref):
    screen.exact(audio, {"flac", "pcm", "duration_ms", "sample_rate", "channels", "pcm_format", "source_witness",
        "source_sha256_reverified", "timeline"}, "normalized audio receipt")
    screen.file_binding(worker_ref)
    screen.file_binding(engine_ref)
    attempt = Path(worker_ref["path"]).parent
    if (attempt.parent != folder or not re.fullmatch(r"attempt-[a-z0-9_]{8}", attempt.name)
            or Path(worker_ref["path"]) != attempt / "worker-request.json"
            or Path(engine_ref["path"]) != attempt / "engine-output.json"):
        raise Error("diarization proof paths are outside the owned job attempt")
    duration = job["screened_record"]["recording"]["duration_ms"]
    for key, name in (("pcm", "normalized.pcm"), ("flac", "normalized.flac")):
        screen.exact(audio[key], {"path", "sha256", "byte_count"}, "normalized audio binding")
        screen.file_binding({field: audio[key][field] for field in ("path", "sha256")})
        screen.integer(audio[key]["byte_count"], 1, duration * 32 + 1024**2, "normalized audio byte count")
        if Path(audio[key]["path"]) != attempt / name:
            raise Error("normalized audio is outside the owned job attempt")
    if (any(type(audio[key]) is not int for key in ("duration_ms", "sample_rate", "channels"))
            or audio["duration_ms"] != duration or audio["sample_rate"] != 16000 or audio["channels"] != 1
            or audio["pcm_format"] != "s16le" or audio["source_sha256_reverified"] is not False
            or audio["pcm"]["byte_count"] != duration * 32 or screen.canonical(audio["timeline"]) != screen.canonical(NORMALIZATION_TIMELINE)
            or audio["source_witness"] != job["screened_record"]["source_witness"]):
        raise Error("normalized audio provenance differs from the source timeline")


def _read_completed(job, folder, plan):
    path = folder / "result.json"
    if not screen.exists(path):
        return None
    value = screen.read_json(path)
    screen.exact(value, {"kind", "schema_version", "state", "plan_id", "job_id", "screened_record", "implementation",
        "normalization", "worker_request", "engine_output", "model_provenance", "memory_envelope", "diarization",
        "normalized_audio_retained", "production_quality_validated", "publication_authority"}, "diarization job result")
    if (value.get("kind") != "himr_screened_diarization_job_result" or value.get("plan_id") != plan["plan_id"]
            or value.get("job_id") != job["job_id"] or value.get("screened_record") != job["screened_record"]
            or type(value["schema_version"]) is not int or value["schema_version"] != 1
            or value.get("state") != "completed" or value.get("implementation") != plan["implementation"]
            or value["production_quality_validated"] is not False or value["publication_authority"] is not False):
        raise Error("saved diarization result does not match the exact job")
    core.validate_result(value["diarization"])
    if (value["diarization"].get("run_id") != job["job_id"]
            or value["diarization"].get("recording") != {
                "media_sha256": job["screened_record"]["recording"]["sha256"],
                "duration_ms": job["screened_record"]["recording"]["duration_ms"]}
            or value["diarization"].get("speaker_bounds") != job["speaker_bounds"]):
        raise Error("saved diarization speaker scope differs")
    request = plan["request_value"]
    if value["normalized_audio_retained"] is not request["retain_normalized_audio"]:
        raise Error("normalized audio retention claim differs")
    _validate_audio_receipt(value["normalization"], job, folder, value["worker_request"], value["engine_output"])
    worker = screen.read_json(Path(value["worker_request"]["path"]), value["worker_request"]["sha256"])
    if worker != make_worker_request(request, job, value["normalization"]):
        raise Error("saved worker request differs from diarization job")
    raw = screen.read_json(Path(value["engine_output"]["path"]), value["engine_output"]["sha256"])
    _validate_engine_output(raw, worker, plan)
    rebuilt = core.normalize_output(worker["duration_ms"], raw["ordinary"], raw["exclusive"],
        media_sha256=worker["media_sha256"], run_id=job["job_id"], bounds=job["speaker_bounds"])
    if rebuilt != value["diarization"] or raw["provenance"] != value["model_provenance"]:
        raise Error("diarization result differs from bound engine output")
    return value


def make_worker_request(request, job, audio):
    recording = job["screened_record"]["recording"]
    value = {"kind": "himr_screened_diarization_worker_request", "schema_version": 1,
        "model_bundle": request["model_bundle"], "audio_pcm": audio["pcm"], "recording_id": recording["media_id"],
        "media_sha256": recording["sha256"], "duration_ms": recording["duration_ms"],
        "device": request["execution"]["device"], "gpu_uuid": request["execution"]["gpu_uuid"],
        "threads": request["execution"]["threads"], "speaker_parameters": job["speaker_bounds"]["parameters"],
        "resources": {**{key: request["limits"][key] for key in
            ("max_duration_ms", "max_pcm_bytes", "max_waveform_bytes", "max_turns")},
            **{key: request["execution"][key] for key in
            ("segmentation_batch_size", "embedding_batch_size", "cuda_memory_fraction")}}}
    engine.validate_request(value)
    return value


def _validate_engine_output(answer, worker_request, plan):
    screen.exact(answer, {"kind", "schema_version", "ordinary", "exclusive", "provenance"}, "diarization engine output")
    if (answer["kind"] != "himr_screened_diarization_engine_output" or type(answer["schema_version"]) is not int
            or answer["schema_version"] != 1):
        raise Error("unsupported diarization engine output")
    if any(not isinstance(answer[key], list) or len(answer[key]) > worker_request["resources"]["max_turns"]
           for key in ("ordinary", "exclusive")):
        raise Error("diarization result exceeds turn limit")
    engine.validate_provenance(answer["provenance"], worker_request, expected_implementation={
        "path": str(Path(engine.__file__).resolve()), "sha256": plan["implementation"]["screened_diarization_engine.py"]})


def _parse_worker_output(body):
    def pairs(items):
        result = {}
        for key, value in items:
            if key in result:
                raise Error("duplicate model JSON field")
            result[key] = value
        return result
    return json.loads(body, object_pairs_hook=pairs,
        parse_constant=lambda _: (_ for _ in ()).throw(Error("nonfinite model JSON")))


def status_plan(path, expected):
    plan = load_plan(path, expected)
    root = Path(plan["request_value"]["state_root"])
    completed = sum(_read_completed(job, root / job["job_id"], plan) is not None for job in plan["jobs"])
    return {"state": "completed" if plan["jobs"] and completed == len(plan["jobs"]) else "incomplete",
        "plan_id": plan["plan_id"], "planned_recordings": len(plan["jobs"]), "completed_recordings": completed,
        "remaining_recordings": len(plan["jobs"]) - completed, "setup_blockers": plan["readiness_blockers"],
        "deferred_selected_count": plan["deferred_selected_count"]}


def run_plan(path, expected):
    plan = load_plan(path, expected)
    request, root = plan["request_value"], Path(plan["request_value"]["state_root"])
    with old_batch._cancellable(), _locked(root) as lock_fd:
        if plan["readiness_blockers"]:
            outcome = {"state": "blocked_setup", "blockers": plan["readiness_blockers"]}
            snapshot(root, outcome)
            return outcome
        blockers = _service_blockers(request["blocking_units"])
        if blockers:
            outcome = {"state": "blocked_existing_work", "blockers": blockers}
            snapshot(root, outcome)
            return outcome
        memory = verify_memory_limit(request["limits"]["host_memory_max_bytes"])
        _check_reference(request["python"], 256 * 1024**2, executable=True)
        engine.admit_bundle(request["model_bundle"])
        deadline = time.monotonic() + request["limits"]["max_run_seconds"]
        processed = 0
        try:
            for job in plan["jobs"]:
                folder = root / job["job_id"]
                if _read_completed(job, folder, plan) is not None:
                    continue
                if time.monotonic() >= deadline or processed >= request["limits"]["max_jobs_per_run"]:
                    break
                if job["resource_admission"]["blockers"]:
                    raise Error("job needs a larger reviewed resource envelope: " + job["job_id"])
                if _service_blockers(request["blocking_units"]):
                    raise Error("blocking work became active; stopping before another recording")
                if implementation() != plan["implementation"]:
                    raise Error("diarization implementation changed")
                selection.verify_selected_record(job["screened_record"])
                reviewed = _bounds(request, job["screened_record"]["recording"]["sha256"])
                if reviewed != job["speaker_bounds"]:
                    raise Error("reviewed speaker bounds changed")
                free = os.statvfs(root)
                required = request["limits"]["min_free_bytes"] + job["resource_admission"]["pcm_bytes"] * 2 + 2 * 1024**2
                if free.f_bavail * free.f_frsize < required:
                    raise Error("insufficient free space for bounded normalized audio")
                _mkdir(folder)
                with _attempt(folder, request["retain_normalized_audio"]) as attempt:
                    job_deadline = min(deadline, time.monotonic() + request["limits"]["job_timeout_seconds"])
                    snapshot(root, {"state": "normalizing", "plan_id": plan["plan_id"], "active_job": job["job_id"]})
                    audio = normalize_audio(job["screened_record"], request, attempt, lock_fd=lock_fd, deadline=job_deadline)
                    recording = job["screened_record"]["recording"]
                    worker_request = make_worker_request(request, job, audio)
                    screen.write_immutable(attempt / "worker-request.json", worker_request)
                    worker_ref = binding(attempt / "worker-request.json")
                    snapshot(root, {"state": "diarizing", "plan_id": plan["plan_id"], "active_job": job["job_id"]})
                    answer_bytes = _command([request["python"]["path"], "-B", str(Path(engine.__file__).resolve()),
                        "--request", worker_ref["path"], "--expected-sha256", worker_ref["sha256"]], lock_fd=lock_fd,
                        timeout=max(0.01, job_deadline - time.monotonic()), maximum=screen.MAX_JSON,
                        environment=_environment(request["execution"], attempt))
                    # Retain exact worker output for independent result validation.
                    answer = _parse_worker_output(answer_bytes)
                    _validate_engine_output(answer, worker_request, plan)
                    screen.write_immutable(attempt / "engine-output.json", answer)
                    engine_output_ref = binding(attempt / "engine-output.json")
                    normalized = core.normalize_output(recording["duration_ms"], answer["ordinary"], answer["exclusive"],
                        media_sha256=recording["sha256"], run_id=job["job_id"], bounds=reviewed)
                    selection.verify_selected_record(job["screened_record"])
                    if implementation() != plan["implementation"] or _bounds(request, recording["sha256"]) != reviewed:
                        raise Error("diarization implementation or reviewed bounds changed before publication")
                    result = {"kind": "himr_screened_diarization_job_result", "schema_version": 1,
                        "state": "completed", "plan_id": plan["plan_id"], "job_id": job["job_id"],
                        "screened_record": job["screened_record"], "implementation": plan["implementation"],
                        "normalization": audio, "worker_request": worker_ref, "engine_output": engine_output_ref,
                        "model_provenance": answer["provenance"],
                        "memory_envelope": memory, "diarization": normalized,
                        "normalized_audio_retained": request["retain_normalized_audio"],
                        "production_quality_validated": False, "publication_authority": False}
                    screen.write_immutable(folder / "result.json", result)
                    processed += 1
            outcome = status_plan(path, expected)
            snapshot(root, outcome)
            return outcome
        except BaseException as error:
            snapshot(root, {"state": "interrupted" if isinstance(error, KeyboardInterrupt) else "failed",
                "plan_id": plan["plan_id"], "error": f"{type(error).__name__}: {str(error)[:1200]}",
                "completed_results_preserved": True, "unfinished_attempts_retained": True})
            raise


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    commands = parser.add_subparsers(dest="command", required=True)
    planner = commands.add_parser("plan")
    planner.add_argument("--request", required=True)
    planner.add_argument("--expected-sha256", required=True)
    for name in ("run", "status"):
        child = commands.add_parser(name)
        child.add_argument("--manifest", required=True)
        child.add_argument("--expected-sha256", required=True)
    args = parser.parse_args(argv)
    try:
        if args.command == "plan":
            result = create_plan(args.request, args.expected_sha256)
        else:
            result = (run_plan if args.command == "run" else status_plan)(args.manifest, args.expected_sha256)
        print(json.dumps(result, indent=2, sort_keys=True, allow_nan=False))
        return 2 if result.get("state", "").startswith("blocked") else 0
    except KeyboardInterrupt:
        print("Diarization interrupted; committed results preserved.", file=sys.stderr)
        return 130
    except (Error, OSError, ValueError, RuntimeError, subprocess.SubprocessError) as error:
        print(f"ScreenedDiarizationError: {error}", file=sys.stderr)
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
