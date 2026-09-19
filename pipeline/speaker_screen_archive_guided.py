"""Separate finite archive screening over the admitted [100 ms, EOF) interval.

Metadata affects queue order only, never speaker labels. The leading 100 ms is
explicitly omitted while all remaining timestamps keep the original source origin.
All evidence belongs to a new private versioned workspace; existing CPU/resident
work orders are read-only templates and their checkpoint formats stay untouched.
"""
from __future__ import annotations

import argparse
from contextlib import contextmanager
import copy
import fcntl
import hashlib
import json
import os
from pathlib import Path
import stat
import sys
import time

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from pipeline import speaker_screen as screen
from pipeline import speaker_screen_accelerated as resident
from pipeline import speaker_screen_batch as old_batch
from pipeline import speaker_screen_core as core
from pipeline import speaker_screen_guidance as guidance_api
from pipeline import speaker_screen_archive_guided_core as guided_core
from pipeline import speaker_screen_paths as paths

ScreenError = screen.ScreenError
InvocationLimit = resident.InvocationLimit
DEFAULT_EXECUTION = resident.DEFAULT_EXECUTION
validate_execution = resident.validate_execution
_runtime_binding = resident._runtime_binding
_validate_runtime = resident._validate_runtime
_validate_observation = resident._validate_observation
DecodePool = resident.DecodePool
ResidentModelProcess = resident.worker_api.ResidentModelProcess
MAX_RUNS = resident.MAX_RUNS
MAX_MANIFEST_BYTES = screen.MAX_JSON
MARKER = {"kind": "himr_private_archive_guided_speaker_screen_workspace", "schema_version": 1}
IMPLEMENTATION_NAMES = resident.IMPLEMENTATION_NAMES + (
    "speaker_screen_archive_guided.py", "speaker_screen_archive_guided_core.py", "speaker_screen_guidance.py")


def _implementation():
    return {name: hashlib.sha256((Path(__file__).parent / name).read_bytes()).hexdigest()
            for name in IMPLEMENTATION_NAMES}


def _verify_implementation(binding):
    if not isinstance(binding, dict) or binding != _implementation():
        raise ScreenError("guided implementation differs from the sealed plan")


def _worker_implementation(binding):
    # The unchanged workers accept only their eight reviewed source files. The
    # parent independently checks the complete guided source set before/after
    # inference and immediately before every immutable evidence publication.
    _verify_implementation(binding)
    return {name: binding[name] for name in resident.IMPLEMENTATION_NAMES}


def _write(path, value, implementation, *, guidance_witnesses=None):
    _verify_implementation(implementation)
    if guidance_witnesses is not None:
        guidance_api.check_sources(guidance_witnesses)
    screen.write_immutable(path, value)


def build_manifest(request, *, request_binding=None):
    screen.exact(request, {"kind", "schema_version", "work_orders", "guidance", "state_root",
                           "execution", "max_target_windows"}, "guided request")
    if (request["kind"] != "himr_archive_guided_speaker_screen_request"
            or type(request["schema_version"]) is not int or request["schema_version"] != 1):
        raise ScreenError("unsupported guided request")
    cap = request["max_target_windows"]
    screen.integer(cap, 0, 64, "maximum targeted windows")
    execution = validate_execution(request["execution"])
    runtime = _runtime_binding(execution)
    originals = old_batch._load_orders(request["work_orders"])
    guidance = guidance_api.load_guidance(request["guidance"], originals)
    records = guidance["recordings"]
    if len(records) != len(originals):
        raise ScreenError("guidance does not cover the exact original input order")
    root = screen.path_value(request["state_root"])
    if request_binding is not None:
        screen.file_binding(request_binding)
    old_batch._check_paths(root, request["work_orders"], originals,
        request_path=None if request_binding is None else request_binding["path"], python=runtime["python"])
    output_roots = [root] + [screen.path_value(plan["order"]["output_root"]) for plan in originals]
    protected = [Path(__file__).parent / name for name in IMPLEMENTATION_NAMES]
    protected += [screen.path_value(ref["path"]) for ref in guidance["source_bindings"]]
    if any(old_batch._overlap(output, source) for output in output_roots for source in protected):
        raise ScreenError("guided or original workspace overlaps guidance or implementation")
    models = originals[0]["order"]["models"]
    if any(plan["order"]["models"] != models for plan in originals):
        raise ScreenError("guided batch requires one identical hash-bound model pair")
    implementation = _implementation()
    indices = sorted(range(len(originals)), key=lambda index: (-records[index]["priority"]["score"], index))
    plans = []
    for index, original_index in enumerate(indices):
        original = originals[original_index]
        record = records[original_index]
        order = copy.deepcopy(original["order"])
        if (record["media_id"] != order["recording"]["media_id"]
                or record["media_sha256"] != order["recording"]["sha256"]):
            raise ScreenError("guidance is bound to a different recording")
        old_root, order["output_root"] = order["output_root"], str(root)
        sampling = guided_core.build_sampling(order["recording"]["duration_ms"], order["policy"],
                                               record["targets"], max_target_windows=cap)
        windows = sampling["windows"]
        width = min(execution["batch_size"], order["resources"]["max_windows_per_run"])
        value = {"kind": "himr_archive_guided_speaker_screen_plan", "schema_version": 1,
                 "index": index, "original_index": original_index,
                 "work_order": dict(request["work_orders"][original_index]),
                 "original_output_root": old_root, "order": order, "guidance": record,
                 "sampling": sampling, "execution": execution, "runtime_binding": runtime,
                 "implementation": implementation, "windows": windows,
                 "batches": [windows[start:start + width] for start in range(0, len(windows), width)]}
        plans.append({**value, "plan_id": "archivescreen_" + screen.digest(value)[:32]})
    value = {"kind": "himr_archive_guided_speaker_screen_manifest", "schema_version": 1,
             "request": request_binding, "work_orders": copy.deepcopy(request["work_orders"]),
             "guidance": copy.deepcopy(request["guidance"]), "guidance_sources": guidance["source_bindings"],
             "max_target_windows": cap, "state_root": str(root), "execution": execution,
             "runtime_binding": runtime, "implementation": implementation, "models": models, "plans": plans,
             "semantics": {"visibility": "private", "discovery": False, "controller_mutation": False,
                 "source_mutation": False, "publication_authority": False, "person_identity_claimed": False,
                 "cross_recording_evidence_cache": False, "models_resident_across_recordings": True,
                 "fixed_atomic_probe_batches": True, "one_bounded_invocation_per_recording_per_run": True,
                 "metadata_is_not_speaker_evidence": True, "admitted_interval_baseline_preserved": True, "source_time_origin_unchanged": True,
                 "leading_source_interval_omitted_ms": 100,
                 "positive_early_stop_requires_complete_admitted_interval_baseline": True}}
    manifest = {**value, "batch_id": "archivebatch_" + screen.digest(value)[:32]}
    if len(screen.canonical(manifest)) > MAX_MANIFEST_BYTES:
        raise ScreenError("guided manifest exceeds bounded JSON size; submit fewer work orders")
    return manifest


def validate_manifest(value):
    screen.exact(value, {"kind", "schema_version", "request", "work_orders", "guidance", "guidance_sources",
        "max_target_windows", "state_root", "execution", "runtime_binding", "implementation", "models",
        "plans", "semantics", "batch_id"}, "guided manifest")
    if not isinstance(value["plans"], list) or not 1 <= len(value["plans"]) <= old_batch.MAX_JOBS:
        raise ScreenError("guided manifest requires 1..128 exact plans")
    request = {"kind": "himr_archive_guided_speaker_screen_request", "schema_version": 1,
               "work_orders": value["work_orders"], "guidance": value["guidance"],
               "state_root": value["state_root"], "execution": value["execution"],
               "max_target_windows": value["max_target_windows"]}
    if build_manifest(request, request_binding=value["request"]) != value:
        raise ScreenError("guided manifest, inputs, implementation, or runtime differs")
    if value["request"] is not None:
        original = screen.read_json(Path(value["request"]["path"]), value["request"]["sha256"])
        if build_manifest(original, request_binding=value["request"]) != value:
            raise ScreenError("guided request differs from sealed manifest")
    return value


def _workspace(root, *, create=False):
    root = screen.path_value(root)
    if create:
        with paths.retained_directory(root.parent) as parent:
            try:
                os.mkdir(root.name, 0o700, dir_fd=parent)
                os.fsync(parent)
            except FileExistsError:
                pass
    with paths.retained_directory(root) as directory:
        if not screen.exists(root / "workspace.json") and create:
            with os.scandir(directory) as entries:
                if next(entries, None) is not None:
                    raise ScreenError("refusing an unmarked nonempty guided workspace")
            _write(root / "workspace.json", MARKER, _implementation())
        if screen.read_json(root / "workspace.json") != MARKER:
            raise ScreenError("guided workspace marker differs")
    return root


def seal_manifest(request_path, expected_sha256, output):
    binding = {"path": str(screen.path_value(request_path)), "sha256": expected_sha256}
    screen.file_binding(binding)
    value = build_manifest(screen.read_json(Path(binding["path"]), expected_sha256), request_binding=binding)
    root = Path(value["state_root"])
    if screen.path_value(output) != root / "manifest.json":
        raise ScreenError("manifest must be state_root/manifest.json")
    _workspace(root, create=True)
    _write(root / "manifest.json", value, value["implementation"])
    return value


def _load_manifest(path, expected):
    binding = {"path": str(screen.path_value(path)), "sha256": expected}
    screen.file_binding(binding)
    value = validate_manifest(screen.read_json(Path(path), expected))
    if Path(path) != Path(value["state_root"]) / "manifest.json":
        raise ScreenError("guided manifest is outside its workspace")
    _workspace(Path(value["state_root"]))
    return value


def _check_lock(fd):
    info = os.fstat(fd)
    if (not stat.S_ISREG(info.st_mode) or info.st_uid != os.getuid() or info.st_nlink != 1
            or stat.S_IMODE(info.st_mode) != 0o600 or info.st_size):
        raise ScreenError("unsafe guided execution lock")


@contextmanager
def _locked(root):
    with paths.retained_directory(root) as directory:
        if screen.read_json(root / "workspace.json") != MARKER:
            raise ScreenError("guided workspace marker differs")
        fd = os.open("archive-guided.lock", os.O_RDWR | os.O_CREAT | os.O_NOFOLLOW | os.O_CLOEXEC, 0o600, dir_fd=directory)
        try:
            _check_lock(fd)
            try:
                fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
            except BlockingIOError:
                raise ScreenError("another guided screen or surviving child holds this workspace") from None
            yield fd
        finally:
            os.close(fd)


def _checkpoint_path(job, index):
    return job / f"batch-{index:04d}.json"


def _observations_for(plan, job, binding, *, cache=None):
    observations, proofs, runtime, completed = [], [], None, set()
    missing = False
    for index, windows in enumerate(plan["batches"]):
        path = _checkpoint_path(job, index)
        if not screen.exists(path):
            missing = True
            continue
        if missing:
            raise ScreenError("guided checkpoint sequence has a gap")
        value = screen.read_json(path)
        screen.exact(value, {"kind", "schema_version", "plan_id", "binding_sha256", "batch_index",
                             "window_results", "runtime"}, "guided batch checkpoint")
        if (value["kind"] != "himr_archive_guided_speaker_screen_checkpoint"
                or type(value["schema_version"]) is not int or value["schema_version"] != 1
                or type(value["batch_index"]) is not int or value["batch_index"] != index
                or value["plan_id"] != plan["plan_id"] or value["binding_sha256"] != screen.digest(binding)
                or not isinstance(value["window_results"], list) or len(value["window_results"]) != len(windows)
                or not isinstance(value["runtime"], dict) or not value["runtime"]):
            raise ScreenError("guided checkpoint differs from plan/source/batch")
        _validate_runtime(plan, value["runtime"])
        if runtime is not None and runtime != value["runtime"]:
            raise ScreenError("guided checkpoints mix incompatible runtimes")
        runtime = value["runtime"]
        for item, window in zip(value["window_results"], windows):
            screen.exact(item, {"window", "pcm_sha256", "observation"}, "guided window evidence")
            if (screen.canonical(item["window"]) != screen.canonical(window)
                    or not isinstance(item["pcm_sha256"], str) or not screen.SHA.fullmatch(item["pcm_sha256"])
                    or not isinstance(item["observation"], dict)
                    or item["observation"].get("index") != window["index"]):
                raise ScreenError("guided evidence window differs")
            _validate_observation(item["observation"])
            observations.append(item["observation"])
        completed.add(index)
        proofs.append({"batch_index": index, "sha256": screen.digest(value)})
    summary = guided_core.summarize(plan["order"]["recording"]["duration_ms"], plan["windows"], observations,
                                    plan["order"]["policy"], sampling=plan["sampling"], cache=cache)
    return observations, proofs, runtime, summary, completed


def _sampling_counts(plan, observations):
    inspected = {item["index"] for item in observations}
    result = {}
    for name, key in (("baseline", "baseline_indices"), ("targeted", "target_indices")):
        indices = set(plan["sampling"][key])
        result[name + "_planned_windows"] = len(indices)
        result[name + "_inspected_windows"] = len(indices & inspected)
        result[name + "_remaining_windows"] = len(indices - inspected)
    return result


def _positive_complete(plan, observations, summary):
    return bool(plan["order"]["resources"].get("early_stop_on_positive", False)
        and summary["status"] == "multiple_speaker_candidate"
        and not _sampling_counts(plan, observations)["baseline_remaining_windows"])


def _result_document(plan, binding, observations, proofs, runtime, summary):
    complete = len(observations) == len(plan["windows"])
    positive = _positive_complete(plan, observations, summary)
    return {"kind": "himr_archive_guided_speaker_screen_result", "schema_version": 1,
        "plan_id": plan["plan_id"], "recording": plan["order"]["recording"],
        "priority": plan["guidance"]["priority"], "original_index": plan["original_index"],
        "state": "completed" if complete else "paused", "completed_windows": len(observations),
        "planned_windows": len(plan["windows"]), "remaining_windows": len(plan["windows"]) - len(observations),
        **_sampling_counts(plan, observations), "screening_decision_complete": complete or positive,
        "stop_reason": "sampling_plan_completed" if complete else "supported_multiple_speakers" if positive else "invocation_limit",
        "source_binding": binding, "runtime": runtime, "checkpoint_hashes": proofs, "summary": summary,
        "policy": {"visibility": "private", "cpu_only": plan["execution"]["device"] == "cpu",
            "device": plan["execution"]["device"], "scores_calibrated": False, "human_review_required": True,
            "person_identity_claimed": False, "full_diarization": False, "whole_recording_solo_claim": False,
            "catalogue_mutation": False, "source_mutation": False, "publication_authority": "none",
            "metadata_is_not_speaker_evidence": True, "admitted_interval_baseline_preserved": True, "source_time_origin_unchanged": True,
                 "leading_source_interval_omitted_ms": 100}}


def _read_job(plan, root):
    job = root / plan["plan_id"]
    initial = {"plan_id": plan["plan_id"], "state": "not_started", "planned_windows": len(plan["windows"]),
               **_sampling_counts(plan, [])}
    if not screen.exists(job):
        return initial
    with paths.retained_directory(job) as directory:
        if not screen.exists(job / "source-binding.json"):
            with os.scandir(directory) as entries:
                names = {entry.name for entry in entries}
            if not names <= {"plan.json"}:
                raise ScreenError("incomplete guided initialization contains unexpected evidence")
            if "plan.json" in names and screen.read_json(job / "plan.json") != plan:
                raise ScreenError("saved guided plan differs")
            return initial
        if screen.read_json(job / "plan.json") != plan:
            raise ScreenError("saved guided plan differs")
        binding = screen.read_json(job / "source-binding.json")
        screen.validate_binding(binding, plan)
        observation, proofs, runtime, summary, _ = _observations_for(plan, job, binding)
        result = _result_document(plan, binding, observation, proofs, runtime, summary)
        if screen.exists(job / "result.json") and screen.read_json(job / "result.json") != result:
            raise ScreenError("sealed guided result differs from checkpoint replay")
        return result


def _projection(plan, value):
    counts = {key: value[key] for key in _sampling_counts(plan, [])}
    return {"index": plan["index"], "original_index": plan["original_index"], "plan_id": plan["plan_id"],
        "media_id": plan["order"]["recording"]["media_id"], "title": plan["guidance"]["title"],
        "date": plan["guidance"]["date"], "priority": plan["guidance"]["priority"],
        "sampling_state": value["state"], "screening_decision_complete": value.get("screening_decision_complete", False),
        "classification": value.get("summary", {}).get("status"),
        "completed_windows": value.get("completed_windows", 0), "planned_windows": len(plan["windows"]),
        "remaining_windows": value.get("remaining_windows", len(plan["windows"])), **counts,
        "coverage": value.get("summary", {}).get("coverage"),
        "reason_flags": value.get("summary", {}).get("reason_flags", []),
        "stop_reason": value.get("stop_reason"), "source_currently_rechecked": False}


def _summary(manifest, rows, *, invocation_state=None, errors=None, metrics=None):
    counts = {"recordings": len(rows), "screening_decisions_complete": sum(row["screening_decision_complete"] for row in rows),
        "sampling_plans_completed": sum(row["sampling_state"] == "completed" for row in rows),
        "completed_windows": sum(row["completed_windows"] for row in rows),
        "planned_windows": sum(row["planned_windows"] for row in rows)}
    counts.update({key: sum(row[key] for row in rows) for key in _sampling_counts(manifest["plans"][0], [])})
    counts.update({name: sum(row["classification"] == name for row in rows) for name in sorted(old_batch.KINDS)})
    return {"kind": "himr_archive_guided_speaker_screen_status", "schema_version": 1,
        "batch_id": manifest["batch_id"], "device": manifest["execution"]["device"],
        "state": "screening_complete" if counts["screening_decisions_complete"] == len(rows) else "paused",
        "invocation_state": invocation_state, "counts": counts, "recordings": rows, "errors": errors or [],
        "metrics": metrics, "semantics": {**manifest["semantics"], "source_currently_rechecked": False,
            "completed_sampling_is_not_complete_diarization": True, "whole_recording_solo_claimed": False}}


def status_batch(path, expected):
    manifest = _load_manifest(path, expected)
    root = Path(manifest["state_root"])
    with paths.retained_directory(root) as directory:
        descriptor = None
        try:
            try:
                descriptor = os.open("archive-guided.lock", os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK, dir_fd=directory)
            except FileNotFoundError:
                pass
            if descriptor is not None:
                _check_lock(descriptor)
                try:
                    fcntl.flock(descriptor, fcntl.LOCK_SH | fcntl.LOCK_NB)
                except BlockingIOError:
                    return {"kind": "himr_archive_guided_speaker_screen_status", "schema_version": 1,
                        "batch_id": manifest["batch_id"], "device": manifest["execution"]["device"],
                        "state": "running", "counts": None, "snapshot_deferred": True,
                        "reason": "an active writer owns the checkpoint sequence", "errors": []}
            rows = [_projection(plan, _read_job(plan, root)) for plan in manifest["plans"]]
        finally:
            if descriptor is not None:
                os.close(descriptor)
    _verify_implementation(manifest["implementation"])
    return _summary(manifest, rows)


def _run_number(root, manifest):
    with os.scandir(paths.anchor(root)) as entries:
        names = [entry.name for entry in entries]
    if len(names) > 2 * MAX_RUNS + 140:
        raise ScreenError("guided journal exceeds bounded size")
    starts = sorted(name for name in names if name.startswith("run-") and name.endswith(".start.json"))
    if len(starts) >= MAX_RUNS or starts != [f"run-{number:06d}.start.json" for number in range(1, len(starts) + 1)]:
        raise ScreenError("guided run sequence differs or is exhausted")
    for number, name in enumerate(starts, 1):
        start = screen.read_json(root / name)
        if start != {"kind": "himr_archive_guided_speaker_screen_run", "schema_version": 1,
                     "batch_id": manifest["batch_id"], "run": number}:
            raise ScreenError("guided run journal differs from manifest")
        finish = root / f"run-{number:06d}.finish.json"
        if screen.exists(finish):
            value = screen.read_json(finish)
            if (value.get("run") != number or value.get("start_sha256") != screen.digest(start)
                    or value.get("batch_id") != manifest["batch_id"]):
                raise ScreenError("guided run result differs from its start")
    for name in names:
        if name.startswith("run-") and name.endswith(".finish.json") and name.replace(".finish.json", ".start.json") not in starts:
            raise ScreenError("guided run result lacks its start")
    return len(starts) + 1


def _run_recording(plan, root, model, decoders, deadline, metrics, guidance_witnesses):
    order, implementation = plan["order"], plan["implementation"]
    deadline = min(deadline, time.monotonic() + order["resources"]["max_run_seconds"])
    _verify_implementation(implementation)
    with screen.opened(order["recording"]["path"]) as source, screen.opened(order["ffmpeg"]["path"], executable=True) as ffmpeg:
        source_witness, ffmpeg_witness = screen.witness(source), screen.witness(ffmpeg)
        if source_witness["st_size"] != order["recording"]["byte_count"]:
            raise ScreenError("guided source byte count differs")
        if screen.hash_fd(ffmpeg, 256 * 1024**2, deadline) != order["ffmpeg"]["sha256"]:
            raise ScreenError("guided FFmpeg SHA-256 differs")
        if order["source_verification"] == "sha256" and screen.hash_fd(source, 64 * 1024**3, deadline) != order["recording"]["sha256"]:
            raise ScreenError("guided source SHA-256 differs")
        binding = {"kind": "himr_speaker_screen_source_binding", "schema_version": 1,
            "plan_id": plan["plan_id"], "source_witness": source_witness,
            "source_sha256_reverified": order["source_verification"] == "sha256"}

        def publish(path, value):
            with screen.opened(order["recording"]["path"]) as current:
                if (screen.witness(current) != source_witness or screen.witness(source) != source_witness
                        or screen.witness(ffmpeg) != ffmpeg_witness):
                    raise ScreenError("guided source changed or was replaced before publication")
            _write(path, value, implementation, guidance_witnesses=guidance_witnesses)

        job = root / plan["plan_id"]
        try:
            os.mkdir(job.name, 0o700, dir_fd=paths.anchor(root))
            screen.sync_directory(root)
        except FileExistsError:
            pass
        with paths.retained_directory(job):
            publish(job / "plan.json", plan)
            publish(job / "source-binding.json", binding)
            cache = core.SummaryCache()
            observations, proofs, runtime, summary, completed = _observations_for(plan, job, binding, cache=cache)
            initial = _result_document(plan, binding, observations, proofs, runtime, summary)
            if screen.exists(job / "result.json"):
                if screen.read_json(job / "result.json") != initial:
                    raise ScreenError("guided final result differs from checkpoint replay")
                return initial
            if initial["screening_decision_complete"]:
                publish(job / "result.json", initial)
                return initial
            pending, added = [], 0
            for index, windows in enumerate(plan["batches"]):
                if index in completed:
                    continue
                if added + len(windows) > order["resources"]["max_windows_per_run"]:
                    break
                pending.append((index, windows))
                added += len(windows)
            timeout = order["resources"]["window_timeout_seconds"]
            if pending and time.monotonic() < deadline:
                decoders.submit(order, source_witness, ffmpeg_witness, pending[0][1], timeout, deadline)
            for ordinal, (index, windows) in enumerate(pending):
                if time.monotonic() >= deadline:
                    break
                _verify_implementation(implementation)
                decoded = decoders.collect(deadline)
                if ordinal + 1 < len(pending) and time.monotonic() < deadline:
                    decoders.submit(order, source_witness, ffmpeg_witness, pending[ordinal + 1][1], timeout, deadline)
                # The unchanged resident protocol admits indices 0..511. A
                # guided plan can have 512 baseline + 64 additive probes, so
                # use fixed-batch-local transport indices. PCM and exact
                # timeline stay unchanged; durable evidence always uses the
                # original global index restored after response validation.
                model_items = [{"pcm": item["pcm"], "window": {**item["window"], "index": local_index}}
                               for local_index, item in enumerate(decoded)]
                started = time.monotonic()
                answer = model.analyze_batch(model_items, min(timeout * len(windows), screen.remaining(deadline)))
                metrics["model_roundtrip_seconds"] += time.monotonic() - started
                _verify_implementation(implementation)
                if not isinstance(answer, dict) or not isinstance(answer.get("runtime"), dict) or not answer["runtime"]:
                    raise ScreenError("guided model response lacks runtime provenance")
                _validate_runtime(plan, answer["runtime"])
                if runtime is not None and answer["runtime"] != runtime:
                    raise ScreenError("guided runtime changed; do not mix embeddings")
                incoming = answer.get("observations")
                if not isinstance(incoming, list) or len(incoming) != len(windows):
                    raise ScreenError("guided observation batch count differs")
                results = []
                for local_index, (item, observation, window) in enumerate(zip(decoded, incoming, windows)):
                    if (not isinstance(observation, dict) or type(observation.get("index")) is not int
                            or observation["index"] != local_index):
                        raise ScreenError("guided observation belongs to another probe")
                    observation = {**observation, "index": window["index"]}
                    _validate_observation(observation)
                    if (observation.get("embedding") is not None and type(observation.get("speech_ms")) is int
                            and observation["speech_ms"] < order["policy"]["min_speech_ms"]):
                        observation = {**observation, "embedding": None}
                    results.append({"window": window, "pcm_sha256": hashlib.sha256(item["pcm"]).hexdigest(), "observation": observation})
                candidate = observations + [item["observation"] for item in results]
                candidate_summary = guided_core.summarize(order["recording"]["duration_ms"], plan["windows"], candidate,
                    order["policy"], sampling=plan["sampling"], cache=cache)
                with screen.opened(order["recording"]["path"]) as current:
                    if (screen.witness(current) != source_witness or screen.witness(source) != source_witness
                            or screen.witness(ffmpeg) != ffmpeg_witness):
                        raise ScreenError("guided source changed or was replaced before checkpoint")
                checkpoint = {"kind": "himr_archive_guided_speaker_screen_checkpoint", "schema_version": 1,
                    "plan_id": plan["plan_id"], "binding_sha256": screen.digest(binding),
                    "batch_index": index, "window_results": results, "runtime": answer["runtime"]}
                started = time.monotonic()
                publish(_checkpoint_path(job, index), checkpoint)
                metrics["checkpoint_write_seconds"] += time.monotonic() - started
                metrics["new_windows"] += len(windows)
                metrics["new_embedding_windows"] += sum(item["observation"]["embedding"] is not None for item in results)
                metrics["model_statistics"] = answer.get("statistics", {})
                observations, summary, runtime = candidate, candidate_summary, answer["runtime"]
                if _positive_complete(plan, observations, summary):
                    break
            # Prefetch is speculative, never evidence. Do not consume its results
            # after this recording ends or let them enter another recording.
            if decoders.active is not None:
                decoders.close()
            observations, proofs, runtime, summary, _ = _observations_for(plan, job, binding, cache=cache)
            result = _result_document(plan, binding, observations, proofs, runtime, summary)
            if result["screening_decision_complete"]:
                publish(job / "result.json", result)
            _verify_implementation(implementation)
            return result


def run_batch(path, expected):
    manifest = _load_manifest(path, expected)
    execution, root = manifest["execution"], Path(manifest["state_root"])
    started = time.monotonic()
    deadline = started + execution["max_run_seconds"]
    model = decoders = None
    metrics = {"elapsed_seconds": 0.0, "model_workers_started": 0, "decoder_workers_started": 0,
        "model_roundtrip_seconds": 0.0, "decode_cpu_wall_seconds_sum": 0.0,
        "checkpoint_write_seconds": 0.0, "new_windows": 0, "new_embedding_windows": 0, "model_statistics": {}}
    errors, invocation = [], "finished"
    with old_batch._cancellable(), _locked(root) as lock_fd:
        rows = [_projection(plan, _read_job(plan, root)) for plan in manifest["plans"]]
        pending = [plan for plan, row in zip(manifest["plans"], rows) if not row["screening_decision_complete"]]
        if not pending:
            metrics["elapsed_seconds"] = time.monotonic() - started
            _verify_implementation(manifest["implementation"])
            return _summary(manifest, rows, invocation_state="finished", metrics=metrics)
        guidance_witnesses = guidance_api.witness_sources(manifest["guidance_sources"])
        number = _run_number(root, manifest)
        start = {"kind": "himr_archive_guided_speaker_screen_run", "schema_version": 1,
                 "batch_id": manifest["batch_id"], "run": number}
        _write(root / f"run-{number:06d}.start.json", start, manifest["implementation"],
               guidance_witnesses=guidance_witnesses)
        current = None
        try:
            child_implementation = _worker_implementation(manifest["implementation"])
            guidance_api.check_sources(guidance_witnesses)
            if time.monotonic() >= deadline:
                raise InvocationLimit("metadata verification exhausted the bounded invocation")
            model = ResidentModelProcess(manifest["models"], execution, child_implementation,
                max_run_seconds=execution["max_run_seconds"], lock_fd=lock_fd)
            metrics["model_workers_started"] = 1
            decoders = DecodePool(execution["decode_prefetch"], child_implementation,
                max_run_seconds=execution["max_run_seconds"], lock_fd=lock_fd)
            metrics["decoder_workers_started"] = execution["decode_prefetch"]
            for plan in pending:
                current = plan
                if time.monotonic() >= deadline:
                    invocation = "time_limit"
                    break
                recording_deadline = min(deadline, time.monotonic() + plan["order"]["resources"]["max_run_seconds"])
                try:
                    result = _run_recording(plan, root, model, decoders, recording_deadline, metrics, guidance_witnesses)
                except (ScreenError, OSError, RuntimeError, EOFError, TimeoutError):
                    if time.monotonic() >= recording_deadline:
                        raise InvocationLimit("recording reached its bounded invocation limit") from None
                    raise
                rows[plan["index"]] = _projection(plan, result)
        except InvocationLimit:
            invocation = "time_limit"
        except KeyboardInterrupt:
            invocation = "cancelled"
        except (ScreenError, OSError, ValueError, RuntimeError, EOFError, TimeoutError) as error:
            invocation = "time_limit" if time.monotonic() >= deadline else "failed"
            errors.append({"index": None if current is None else current["index"],
                           "reason": f"{type(error).__name__}: {str(error)[:1200]}"})
        finally:
            try:
                with old_batch._deferred_launch_signals():
                    if decoders is not None:
                        metrics["decode_cpu_wall_seconds_sum"] = decoders.decoded_seconds
                        metrics["decoder_workers_started"] = decoders.worker_starts
                        try:
                            decoders.close()
                        except (OSError, RuntimeError) as error:
                            errors.append({"index": None, "reason": "decoder cleanup failed: " + str(error)[:1200]})
                            invocation = "failed"
                    if model is not None:
                        try:
                            model.close()
                        except (OSError, RuntimeError) as error:
                            errors.append({"index": None, "reason": "model cleanup failed: " + str(error)[:1200]})
                            invocation = "failed"
            except KeyboardInterrupt:
                invocation = "cancelled"
        for plan in manifest["plans"]:
            try:
                rows[plan["index"]] = _projection(plan, _read_job(plan, root))
            except (ScreenError, OSError, ValueError, RuntimeError) as error:
                errors.append({"index": plan["index"], "reason": "checkpoint replay failed: " + str(error)[:1200]})
                invocation = "failed"
        _verify_implementation(manifest["implementation"])
        metrics["elapsed_seconds"] = time.monotonic() - started
        result = _summary(manifest, rows, invocation_state=invocation, errors=errors, metrics=metrics)
        _write(root / f"run-{number:06d}.finish.json", {
            "kind": "himr_archive_guided_speaker_screen_run_result", "schema_version": 1,
            "batch_id": manifest["batch_id"], "run": number, "start_sha256": screen.digest(start), "result": result},
            manifest["implementation"], guidance_witnesses=guidance_witnesses)
        return result


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    commands = parser.add_subparsers(dest="command", required=True)
    planner = commands.add_parser("plan")
    planner.add_argument("--request", required=True)
    planner.add_argument("--expected-sha256", required=True)
    planner.add_argument("--output", required=True)
    for name in ("run", "status"):
        selected = commands.add_parser(name)
        selected.add_argument("--manifest", required=True)
        selected.add_argument("--expected-sha256", required=True)
    args = parser.parse_args(argv)
    try:
        if args.command == "plan":
            result = seal_manifest(args.request, args.expected_sha256, args.output)
        elif args.command == "status":
            result = status_batch(args.manifest, args.expected_sha256)
        else:
            result = run_batch(args.manifest, args.expected_sha256)
        print(json.dumps(result, sort_keys=True, indent=2, allow_nan=False))
        return 130 if result.get("invocation_state") == "cancelled" else 2 if result.get("errors") else 0
    except KeyboardInterrupt:
        print("Guided screen interrupted; committed probe batches are preserved.", file=sys.stderr)
        return 130
    except (ScreenError, OSError, ValueError, RuntimeError, EOFError, TimeoutError) as error:
        print(f"GuidedSpeakerScreenError: {error}", file=sys.stderr)
        return 2


if __name__ == "__main__":
    raise SystemExit(main())

