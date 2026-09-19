"""Finite private speaker-to-face matching from completed diarization jobs.

Only anonymous, clip-local candidate associations are produced. This stage never
recognizes people, links faces across shots, changes upstream artifacts, publishes
labels, or downloads models. Missing/ambiguous video remains explicitly unknown.
"""
from __future__ import annotations

import argparse
from contextlib import contextmanager
import errno
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
from pipeline import screened_diarization as diar
from pipeline import speaker_screen as safe
from pipeline import speaker_screen_paths as paths
from pipeline import speaker_screen_batch as batch
from pipeline import screened_diarization_engine as strict
from pipeline import speaker_face_matching_core as core
from pipeline import speaker_face_matching_visual as visual
from pipeline import speaker_face_matching_engine as engine

Error = safe.ScreenError
MAX_JSON = 16 * 1024**2
MAX_VIDEO = 125 * 640 * 360 * 3
MAX_PCM = 5000 * 32
DEFAULT_LIMITS = {"max_clips": 256, "max_jobs_per_run": 32,
    "max_run_seconds": 3600, "clip_timeout_seconds": 120,
    "host_memory_max_bytes": 4 * 1024**3, "min_free_bytes": 1024**3}
DEFAULT_EXECUTION = {"device": "cpu", "gpu_uuid": None, "threads": 1,
    "cuda_memory_fraction": 0.5}
MARKER = {"kind": "himr_private_speaker_face_matching_workspace", "schema_version": 1}
SEMANTICS = {"visibility": "private", "production_quality_validated": False,
    "person_identity_inferred": False, "cross_recording_matching": False,
    "cross_shot_face_matching": False, "publication_authority": False,
    "upstream_mutation": False, "sampled_clips_only": True,
    "unsampled_intervals_remain_unknown": True, "model_downloads": False,
    "new_diarization_completions_require_new_plan": True}
NAMES = tuple(dict.fromkeys((*diar.IMPLEMENTATION_NAMES,
    "shot_local_face_tracker.py", "yunet_face_detector.py",
    "speaker_face_matching.py", "speaker_face_matching_core.py",
    "speaker_face_matching_visual.py", "speaker_face_matching_engine.py")))


class ChildError(Error):
    pass


def implementation():
    return {name: hashlib.sha256((Path(__file__).parent / name).read_bytes()).hexdigest() for name in NAMES}


def _json(ref):
    return strict._json(ref)[0]


def _same(a, b):
    return safe.canonical(a) == safe.canonical(b)


def _seal(path, value):
    if len(safe.canonical(value)) > MAX_JSON:
        raise Error("matching JSON exceeds its finite bound")
    safe.write_immutable(path, value)
    return diar.binding(path, MAX_JSON)


def snapshot(root, value):
    body = safe.canonical({"kind": "himr_speaker_face_matching_status", "schema_version": 1,
                           "updated_unix": time.time(), **value})
    if len(body) > MAX_JSON:
        raise Error("matching status exceeds its finite bound")
    with paths.retained_directory(root) as fd:
        name = ".matching-status-" + uuid.uuid4().hex
        out = os.open(name, os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW, 0o600, dir_fd=fd)
        try:
            with os.fdopen(out, "wb") as stream:
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
    safe.exact(value, {"kind", "schema_version", "purpose", "diarization_plan", "job_ids",
        "state_root", "ffmpeg", "python", "model_bundle", "policy", "execution", "limits",
        "blocking_units", "retain_clip_media"}, "speaker-face matching request")
    if (value["kind"] != "himr_speaker_face_matching_request" or type(value["schema_version"]) is not int
            or value["schema_version"] != 1 or value["purpose"] != "private_unvalidated_pilot"):
        raise Error("unsupported matching request")
    for key in ("diarization_plan", "ffmpeg"):
        safe.file_binding(value[key])
    for key in ("python", "model_bundle"):
        if value[key] is not None:
            safe.file_binding(value[key])
    safe.path_value(value["state_root"])
    ids = value["job_ids"]
    if ids is not None and (not isinstance(ids, list) or not 1 <= len(ids) <= 128
            or any(not isinstance(x, str) or not re.fullmatch(r"diarjob_[0-9a-f]{32}", x) for x in ids)
            or len(set(ids)) != len(ids)):
        raise Error("invalid explicit diarization job filter")
    core.validate_policy(value["policy"])
    ex = value["execution"]
    safe.exact(ex, DEFAULT_EXECUTION, "matching execution")
    if ex["device"] not in ("cpu", "cuda"):
        raise Error("matching device must be cpu or cuda")
    if ex["device"] == "cpu":
        if ex["gpu_uuid"] is not None:
            raise Error("CPU execution cannot select a GPU")
    elif not isinstance(ex["gpu_uuid"], str) or not re.fullmatch(r"GPU-[0-9a-fA-F]{8}(?:-[0-9a-fA-F]{4}){3}-[0-9a-fA-F]{12}", ex["gpu_uuid"]):
        raise Error("CUDA requires an explicit GPU UUID")
    safe.integer(ex["threads"], 1, 4, "matching threads")
    if type(ex["cuda_memory_fraction"]) not in (int, float) or not math.isfinite(ex["cuda_memory_fraction"]) or not 0.1 <= ex["cuda_memory_fraction"] <= 0.75:
        raise Error("invalid matching CUDA memory fraction")
    limits = value["limits"]
    safe.exact(limits, DEFAULT_LIMITS, "matching limits")
    for key, lo, hi in (("max_clips", 1, 4096), ("max_jobs_per_run", 1, 4096),
            ("max_run_seconds", 30, 86400), ("clip_timeout_seconds", 5, 3600),
            ("host_memory_max_bytes", 2 * 1024**3, 16 * 1024**3),
            ("min_free_bytes", 256 * 1024**2, 64 * 1024**3)):
        safe.integer(limits[key], lo, hi, key)
    if type(value["retain_clip_media"]) is not bool:
        raise Error("clip retention must be boolean")
    units = value["blocking_units"]
    if (not isinstance(units, list) or len(units) > 16
            or any(not isinstance(x, str) or not re.fullmatch(r"[a-zA-Z0-9_.@-]{1,160}\.service", x) for x in units)
            or len(set(units)) != len(units)):
        raise Error("invalid blocking units")
    return value


def _paths(value):
    if isinstance(value, str) and value.startswith("/"):
        yield Path(value)
    elif isinstance(value, dict):
        for item in value.values():
            yield from _paths(item)
    elif isinstance(value, list):
        for item in value:
            yield from _paths(item)


def _protect(request, upstream, records):
    root = Path(request["state_root"])
    # Output cannot be an ancestor/descendant of any source workspace or model.
    protected = list(_paths({"upstream": upstream, "records": records}))
    protected += [Path(request["diarization_plan"]["path"]).parent, Path(__file__).parent,
                  ROOT / "src", ROOT / "public", ROOT / "dist", ROOT / ".git"]
    protected += [Path(request[k]["path"]) for k in ("ffmpeg", "python", "model_bundle") if request[k] is not None]
    if request["model_bundle"] is not None:
        admitted = engine.admit_bundle(request["model_bundle"])
        protected.extend(_paths(admitted))
        python = admitted["runtime"]["python"]
        if request["python"] != {key: python[key] for key in ("path", "sha256")}:
            raise Error("matching Python differs from registered isolated runtime")
    if any(batch._overlap(root, path) for path in protected):
        raise Error("matching workspace overlaps a protected source, runtime, or publication path")


def _upstream(request):
    ref = request["diarization_plan"]
    return diar.load_plan(ref["path"], ref["sha256"])


def _read_source(upstream, job):
    folder = Path(upstream["request_value"]["state_root"]) / job["job_id"]
    value = diar._read_completed(job, folder, upstream)
    return value, folder / "result.json"


def _capture(request, upstream):
    valid = {job["job_id"] for job in upstream["jobs"]}
    if request["job_ids"] is not None and not set(request["job_ids"]) <= valid:
        raise Error("explicit job filter contains a job outside the diarization plan")
    records, pending = [], 0
    for job in upstream["jobs"]:
        if request["job_ids"] is not None and job["job_id"] not in request["job_ids"]:
            continue
        result, path = _read_source(upstream, job)
        if result is None:
            pending += 1
            continue
        records.append({"job_id": job["job_id"], "diarization_result": diar.binding(path, MAX_JSON),
                        "clip_plan": core.plan_clips(result["diarization"], request["policy"])})
    return records, pending


def _jobs(records, request):
    jobs = []
    for record in records:
        for clip in record["clip_plan"]["clips"]:
            body = {"source_job_id": record["job_id"], "diarization_result": record["diarization_result"], "clip": clip}
            jobs.append({"job_id": "avmatch_" + safe.digest(body)[:32], **body})
    return jobs[:request["limits"]["max_clips"]], max(0, len(jobs) - request["limits"]["max_clips"])


def _blockers(request):
    return [reason for key, reason in (("model_bundle", "approved_lrasd_yunet_bundle_not_configured"),
            ("python", "isolated_matching_python_not_configured")) if request[key] is None]


def create_plan(request_path, expected_sha256):
    ref = {"path": str(safe.path_value(request_path)), "sha256": expected_sha256}
    request = validate_request(_json(ref))
    upstream = _upstream(request)
    records, pending = _capture(request, upstream)
    _protect(request, upstream, records)
    diar._check_reference(request["ffmpeg"], 256 * 1024**2, executable=True)
    jobs, deferred = _jobs(records, request)
    body = {"kind": "himr_speaker_face_matching_plan", "schema_version": 1,
        "request": ref, "request_value": request, "records": records, "jobs": jobs,
        "pending_diarization_jobs_at_snapshot": pending, "deferred_clips": deferred,
        "readiness_blockers": _blockers(request), "implementation": implementation(), "semantics": SEMANTICS}
    plan = {**body, "plan_id": "avmatchplan_" + safe.digest(body)[:32]}
    if len(safe.canonical(plan)) > MAX_JSON:
        raise Error("matching plan exceeds bound; select fewer recording jobs")
    root = Path(request["state_root"])
    diar._mkdir(root)
    with paths.retained_directory(root) as fd, diar._locked(root):
        if safe.exists(root / "workspace.json"):
            if _json(diar.binding(root / "workspace.json")) != MARKER:
                raise Error("matching workspace marker differs")
        elif any(entry.name != "execution.lock" for entry in os.scandir(fd)):
            raise Error("refusing a nonempty unmarked matching workspace")
        else:
            _seal(root / "workspace.json", MARKER)
        _seal(root / "plan.json", plan)
        snapshot(root, {"state": "blocked_setup" if plan["readiness_blockers"] else "planned",
            "plan_id": plan["plan_id"], "planned_clips": len(jobs), "blockers": plan["readiness_blockers"]})
    return plan


def load_plan(path, expected_sha256):
    path = safe.path_value(path)
    plan = _json({"path": str(path), "sha256": expected_sha256})
    safe.exact(plan, {"kind", "schema_version", "request", "request_value", "records", "jobs",
        "pending_diarization_jobs_at_snapshot", "deferred_clips", "readiness_blockers",
        "implementation", "semantics", "plan_id"}, "matching plan")
    body = {key: value for key, value in plan.items() if key != "plan_id"}
    if (plan["kind"] != "himr_speaker_face_matching_plan" or type(plan["schema_version"]) is not int
            or plan["schema_version"] != 1 or plan["plan_id"] != "avmatchplan_" + safe.digest(body)[:32]
            or not _same(plan["implementation"], implementation()) or not _same(plan["semantics"], SEMANTICS)):
        raise Error("matching plan or implementation drift")
    request = validate_request(plan["request_value"])
    if not _same(_json(plan["request"]), request) or path != Path(request["state_root"]) / "plan.json":
        raise Error("matching request or plan location differs")
    if _json(diar.binding(path.parent / "workspace.json")) != MARKER:
        raise Error("matching workspace marker differs")
    upstream = _upstream(request)
    jobs_by_id = {job["job_id"]: job for job in upstream["jobs"]}
    if not isinstance(plan["records"], list) or len(plan["records"]) > len(jobs_by_id):
        raise Error("matching selection exceeds source job set")
    selected_ids = []
    for record in plan["records"]:
        safe.exact(record, {"job_id", "diarization_result", "clip_plan"}, "matching source record")
        if not isinstance(record["job_id"], str) or not re.fullmatch(r"diarjob_[0-9a-f]{32}", record["job_id"]):
            raise Error("invalid selected diarization job ID")
        job = jobs_by_id.get(record["job_id"])
        if job is None or record["job_id"] in selected_ids or (request["job_ids"] is not None and record["job_id"] not in request["job_ids"]):
            raise Error("matching selected job not in requested source scope")
        selected_ids.append(record["job_id"])
        result, result_path = _read_source(upstream, job)
        if result is None or not _same(record["diarization_result"], diar.binding(result_path, MAX_JSON)):
            raise Error("completed diarization result disappeared or changed")
        if not _same(core.plan_clips(result["diarization"], request["policy"]), record["clip_plan"]):
            raise Error("matching clips differ from completed diarization")
    if selected_ids != [job["job_id"] for job in upstream["jobs"] if job["job_id"] in selected_ids]:
        raise Error("matching source order differs")
    safe.integer(plan["pending_diarization_jobs_at_snapshot"], 0, len(jobs_by_id), "pending source jobs")
    expected_total = len(request["job_ids"]) if request["job_ids"] is not None else len(jobs_by_id)
    if len(selected_ids) + plan["pending_diarization_jobs_at_snapshot"] != expected_total:
        raise Error("matching selection historical coverage differs")
    rebuilt, deferred = _jobs(plan["records"], request)
    if not _same(plan["jobs"], rebuilt) or type(plan["deferred_clips"]) is not int or plan["deferred_clips"] != deferred or plan["readiness_blockers"] != _blockers(request):
        raise Error("matching jobs or readiness differ from sealed request")
    _protect(request, upstream, plan["records"])
    return plan, upstream


def _child(command, *, lock_fd, inherited_fds=(), environment, timeout,
           maximum, stdout_max=65536, stderr_max=2 * 1024**2):
    """Own/reap the process group; bound files and both captured output streams."""
    parent, process = os.getpid(), None
    with tempfile.TemporaryFile() as output, tempfile.TemporaryFile() as errors:
        try:
            inherited_mask = signal.pthread_sigmask(signal.SIG_BLOCK, set())
            def setup():
                safe.die_with_parent(parent)
                signal.pthread_sigmask(signal.SIG_SETMASK, inherited_mask)
                resource.setrlimit(resource.RLIMIT_CORE, (0, 0))
                cap = max(maximum, stdout_max, stderr_max) + 1
                resource.setrlimit(resource.RLIMIT_FSIZE, (cap, cap))
                safe.deny_internet()
            with batch._deferred_launch_signals():
                process = subprocess.Popen(command, stdin=subprocess.DEVNULL, stdout=output, stderr=errors,
                    close_fds=True, pass_fds=tuple(inherited_fds) + (lock_fd,), start_new_session=True,
                    preexec_fn=setup, env=environment)
            process.wait(timeout=max(0.01, timeout))
            errors.seek(0)
            diagnostics = errors.read(stderr_max + 1)
            if re.search(rb"input/output error|\[errno 5\]", diagnostics, re.I):
                raise OSError(errno.EIO, "matching subprocess reported storage I/O failure")
            if len(diagnostics) > stderr_max:
                raise ChildError("matching subprocess diagnostics exceeded bound")
            if process.returncode:
                raise ChildError("matching subprocess failed: " + diagnostics[-2000:].decode(errors="replace"))
            output.seek(0)
            body = output.read(stdout_max + 1)
            if len(body) > stdout_max:
                raise ChildError("matching subprocess stdout exceeded bound")
            return body, diagnostics
        finally:
            with batch._deferred_launch_signals():
                if process is not None:
                    try:
                        os.killpg(process.pid, signal.SIGKILL)
                    except ProcessLookupError:
                        pass
                    process.wait(timeout=10)


@contextmanager
def _attempt(folder, retain):
    work = Path(tempfile.mkdtemp(prefix="attempt-", dir=folder))
    try:
        yield work
    finally:
        if not retain:
            with paths.retained_directory(work) as fd:
                for name in ("video.rgb", "audio.pcm"):
                    try:
                        info = os.stat(name, dir_fd=fd, follow_symlinks=False)
                    except FileNotFoundError:
                        continue
                    if not stat.S_ISREG(info.st_mode) or info.st_nlink != 1 or info.st_uid != os.getuid():
                        raise Error("unsafe generated matching media cleanup target")
                    os.unlink(name, dir_fd=fd)
                os.fsync(fd)


def decode_clip(record, clip, request, work, *, lock_fd, deadline):
    source = record["screened_record"]
    diagnostics = {}
    with safe.opened(source["recording"]["path"]) as media, safe.opened(request["ffmpeg"]["path"], executable=True) as tool:
        if safe.witness(media) != source["source_witness"]:
            raise Error("source changed after diarization")
        tool_witness = safe.witness(tool)
        if safe.hash_fd(tool, 256 * 1024**2, deadline) != request["ffmpeg"]["sha256"]:
            raise Error("matching decoder changed")
        commands = visual.build_decode_commands(f"/proc/self/fd/{tool}", f"/proc/self/fd/{media}",
            clip["start_ms"], clip["end_ms"], str(work / "video.rgb"), str(work / "audio.pcm"))
        for command in commands:
            _, log = _child(command["argv"], lock_fd=lock_fd, inherited_fds=(media, tool),
                environment=diar._environment({**request["execution"], "device": "cpu", "gpu_uuid": None}, work),
                timeout=deadline-time.monotonic(), maximum=command["max_file_bytes"],
                stdout_max=command["stdout_max"], stderr_max=command["stderr_max"])
            diagnostics[command["kind"]] = log
            with safe.opened(command["output_path"]) as fd:
                if not 0 < os.fstat(fd).st_size <= command["max_file_bytes"]:
                    raise ChildError("decoded clip file exceeds bound")
                os.fchmod(fd, 0o400)
        with safe.opened(source["recording"]["path"]) as current:
            if safe.witness(media) != source["source_witness"] or safe.witness(current) != source["source_witness"] or safe.witness(tool) != tool_witness:
                raise Error("source or decoder changed during clip decode")
    video_ref, audio_ref = diar.binding(work / "video.rgb", MAX_VIDEO), diar.binding(work / "audio.pcm", MAX_PCM)
    video, _ = strict._read(video_ref, MAX_VIDEO)
    audio, _ = strict._read(audio_ref, MAX_PCM)
    receipt = visual.validate_decoded(video, audio, start_ms=clip["start_ms"], end_ms=clip["end_ms"],
        video_stderr=diagnostics["video"], audio_stderr=diagnostics["audio"])
    receipt_ref = _seal(work / "decode-receipt.json", receipt)
    _seal(work / "decode-diagnostics.json", {key: value.decode(errors="replace") for key, value in diagnostics.items()})
    return {"video_rgb": {**video_ref, "byte_count": len(video)},
            "audio_pcm": {**audio_ref, "byte_count": len(audio)}, "decode_receipt": receipt_ref}


def make_worker_request(request, job, decoded):
    worker = {"kind": "himr_speaker_face_matching_worker_request", "schema_version": 1,
        "model_bundle": request["model_bundle"], "clip": job["clip"], **decoded,
        **{key: request["execution"][key] for key in ("device", "gpu_uuid", "threads")},
        "resources": {"max_frames": 125, "max_tracks": 16,
                      "cuda_memory_fraction": request["execution"]["cuda_memory_fraction"]}}
    engine.validate_request(worker)
    return worker


def _answer(answer, worker, plan):
    engine.validate_output(answer, worker, expected_implementation={
        "path": str(Path(engine.__file__).resolve()), "sha256": plan["implementation"]["speaker_face_matching_engine.py"]})
    return core.associate(worker["clip"], answer["observations"], plan["request_value"]["policy"])


def _base_result(plan, job):
    return {"kind": "himr_speaker_face_matching_job_result", "schema_version": 1,
        "plan_id": plan["plan_id"], "job": job, "implementation": plan["implementation"], "semantics": SEMANTICS}


def _read_completed(plan, job):
    folder = Path(plan["request_value"]["state_root"]) / job["job_id"]
    path = folder / "result.json"
    if not safe.exists(path):
        return None
    result = _json(diar.binding(path, MAX_JSON))
    base = _base_result(plan, job)
    safe.exact(result, {*base, "state", "review_reason", "worker_request", "engine_output", "association", "clip_media_retained"}, "matching job result")
    if any(not _same(result[key], value) for key, value in base.items()) or result["clip_media_retained"] is not plan["request_value"]["retain_clip_media"]:
        raise Error("matching result differs from sealed job")
    if result["state"] == "needs_review":
        if (not isinstance(result["review_reason"], str) or not 1 <= len(result["review_reason"]) <= 2400
                or any(result[key] is not None for key in ("worker_request", "engine_output", "association"))):
            raise Error("invalid unprocessed matching review result")
        return result
    if result["state"] != "completed" or result["review_reason"] is not None:
        raise Error("invalid matching result state")
    worker_ref, raw_ref = result["worker_request"], result["engine_output"]
    safe.file_binding(worker_ref)
    safe.file_binding(raw_ref)
    attempt = Path(worker_ref["path"]).parent
    if (attempt.parent != folder or not re.fullmatch(r"attempt-[a-z0-9_]{8}", attempt.name)
            or Path(worker_ref["path"]) != attempt / "worker-request.json" or Path(raw_ref["path"]) != attempt / "engine-output.json"):
        raise Error("matching proof paths escape owned attempt")
    worker = _json(worker_ref)
    decoded = {key: worker[key] for key in ("video_rgb", "audio_pcm", "decode_receipt")}
    if not _same(worker, make_worker_request(plan["request_value"], job, decoded)):
        raise Error("matching worker request differs from job")
    for key, name in (("video_rgb", "video.rgb"), ("audio_pcm", "audio.pcm"), ("decode_receipt", "decode-receipt.json")):
        if Path(decoded[key]["path"]) != attempt / name:
            raise Error("matching decoded proof escapes owned attempt")
    receipt = _json(worker["decode_receipt"])
    visual.validate_receipt(receipt, start_ms=job["clip"]["start_ms"], end_ms=job["clip"]["end_ms"])
    for prefix, binding_key in (("video", "video_rgb"), ("audio", "audio_pcm")):
        if (receipt[prefix + "_sha256"] != worker[binding_key]["sha256"]
                or receipt[prefix + "_bytes"] != worker[binding_key]["byte_count"]):
            raise Error("decode receipt differs from prepared audio/video binding")
    if not _same(result["association"], _answer(_json(raw_ref), worker, plan)):
        raise Error("matching result differs from replayed model receipt")
    return result


def _status(plan):
    counts = {"planned_clips": len(plan["jobs"]), "processed_clips": 0,
              "candidate_matches": 0, "unknown_associations": 0, "needs_review_clips": 0}
    for job in plan["jobs"]:
        result = _read_completed(plan, job)
        if result is None:
            continue
        counts["processed_clips"] += 1
        if result["state"] == "needs_review":
            counts["needs_review_clips"] += 1
        elif result["association"]["status"] == "candidate_match":
            counts["candidate_matches"] += 1
        else:
            counts["unknown_associations"] += 1
    counts["remaining_clips"] = counts["planned_clips"] - counts["processed_clips"]
    state = "empty_selection" if not plan["jobs"] else "incomplete" if counts["remaining_clips"] else "completed_with_reviews" if counts["needs_review_clips"] else "completed"
    return {"state": state, "plan_id": plan["plan_id"], **counts,
        "selected_recordings": len(plan["records"]), "deferred_clips": plan["deferred_clips"],
        "pending_diarization_jobs_at_snapshot": plan["pending_diarization_jobs_at_snapshot"],
        "setup_blockers": plan["readiness_blockers"], "semantics": SEMANTICS}


def status_plan(path, expected_sha256):
    plan, _ = load_plan(path, expected_sha256)
    return _status(plan)


def run_plan(path, expected_sha256):
    plan, upstream = load_plan(path, expected_sha256)
    request, root = plan["request_value"], Path(plan["request_value"]["state_root"])
    with batch._cancellable(), diar._locked(root) as lock_fd:
        outcome = _status(plan)
        if not outcome["remaining_clips"]:
            snapshot(root, outcome)
            return outcome
        if plan["readiness_blockers"]:
            outcome = {**outcome, "state": "blocked_setup", "blockers": plan["readiness_blockers"]}
            snapshot(root, outcome)
            return outcome
        blockers = diar._service_blockers(request["blocking_units"])
        if blockers:
            outcome = {**outcome, "state": "blocked_existing_work", "blockers": blockers}
            snapshot(root, outcome)
            return outcome
        diar.verify_memory_limit(request["limits"]["host_memory_max_bytes"])
        diar._check_reference(request["python"], 256 * 1024**2, executable=True)
        deadline = time.monotonic() + request["limits"]["max_run_seconds"]
        by_id, processed = {job["job_id"]: job for job in upstream["jobs"]}, 0
        try:
            for job in plan["jobs"]:
                if _read_completed(plan, job) is not None:
                    continue
                if processed >= request["limits"]["max_jobs_per_run"] or time.monotonic() >= deadline:
                    break
                if diar._service_blockers(request["blocking_units"]):
                    raise Error("blocking work became active; no new matching clip launched")
                if implementation() != plan["implementation"]:
                    raise Error("matching implementation drift")
                source, source_path = _read_source(upstream, by_id[job["source_job_id"]])
                if source is None or diar.binding(source_path, MAX_JSON) != job["diarization_result"]:
                    raise Error("diarization source changed before matching")
                diar.selection.verify_selected_record(source["screened_record"])
                free = os.statvfs(root)
                if free.f_bavail * free.f_frsize < request["limits"]["min_free_bytes"] + MAX_VIDEO + MAX_PCM:
                    raise Error("insufficient private scratch space for matching clip")
                folder = root / job["job_id"]
                diar._mkdir(folder)
                result = {**_base_result(plan, job), "state": "completed", "review_reason": None,
                    "worker_request": None, "engine_output": None, "association": None,
                    "clip_media_retained": request["retain_clip_media"]}
                with _attempt(folder, request["retain_clip_media"]) as work:
                    clip_deadline = min(deadline, time.monotonic() + request["limits"]["clip_timeout_seconds"])
                    snapshot(root, {"state": "decoding", "plan_id": plan["plan_id"], "job_id": job["job_id"]})
                    try:
                        decoded = decode_clip(source, job["clip"], request, work, lock_fd=lock_fd, deadline=clip_deadline)
                    except (visual.VisualError, ChildError, subprocess.TimeoutExpired) as error:
                        result.update(state="needs_review", review_reason=(type(error).__name__ + ": " + str(error))[:2400])
                    else:
                        worker = make_worker_request(request, job, decoded)
                        ref = _seal(work / "worker-request.json", worker)
                        snapshot(root, {"state": "matching", "plan_id": plan["plan_id"], "job_id": job["job_id"]})
                        body, _ = _child([request["python"]["path"], "-B", str(Path(engine.__file__).resolve()),
                            "--request", ref["path"], "--expected-sha256", ref["sha256"]], lock_fd=lock_fd,
                            environment=diar._environment(request["execution"], work), timeout=clip_deadline-time.monotonic(),
                            maximum=MAX_JSON, stdout_max=MAX_JSON)
                        answer = diar._parse_worker_output(body)
                        result.update(worker_request=ref, engine_output=_seal(work / "engine-output.json", answer),
                                      association=_answer(answer, worker, plan))
                    diar.selection.verify_selected_record(source["screened_record"])
                    if implementation() != plan["implementation"]:
                        raise Error("matching implementation changed before result commit")
                    _seal(folder / "result.json", result)
                processed += 1
            outcome = _status(plan)
            snapshot(root, outcome)
            return outcome
        except BaseException as error:
            snapshot(root, {"state": "interrupted" if isinstance(error, KeyboardInterrupt) else "failed",
                "plan_id": plan["plan_id"], "error": (type(error).__name__ + ": " + str(error))[:2400],
                "completed_results_preserved": True})
            raise


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    commands = parser.add_subparsers(dest="command", required=True)
    planner = commands.add_parser("plan")
    planner.add_argument("--request", required=True)
    planner.add_argument("--expected-sha256", required=True)
    for name in ("status", "run"):
        command = commands.add_parser(name)
        command.add_argument("--manifest", required=True)
        command.add_argument("--expected-sha256", required=True)
    args = parser.parse_args(argv)
    try:
        value = create_plan(args.request, args.expected_sha256) if args.command == "plan" else (
            run_plan if args.command == "run" else status_plan)(args.manifest, args.expected_sha256)
        print(json.dumps(value, indent=2, sort_keys=True, allow_nan=False))
        return 2 if value.get("state", "").startswith("blocked") else 0
    except KeyboardInterrupt:
        print("Matching interrupted; committed clip results preserved.", file=sys.stderr)
        return 130
    except (Error, OSError, ValueError, RuntimeError, KeyError, TypeError, subprocess.SubprocessError) as error:
        print("SpeakerFaceMatchingError: " + str(error), file=sys.stderr)
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
