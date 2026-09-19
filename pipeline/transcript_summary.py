"""Private, finite, resumable transcript/timeline summaries through paid Batch APIs.

Planning, preparation, status, retry planning and export are offline. Only explicit
submit/poll/reconcile commands use a separately supplied API key. No service,
source, corpus database, identity label or public website is changed.
"""
from __future__ import annotations

import argparse
from contextlib import contextmanager
import ctypes
import datetime as dt
import errno
import fcntl
import hashlib
import json
import os
from pathlib import Path
import re
import stat
import sys
import time
import uuid

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))
from pipeline import speaker_screen as safe
from pipeline import speaker_screen_paths as paths
from pipeline import transcript_summary_sources as sources_module
from pipeline import transcript_summary_core as core
from pipeline import transcript_summary_client as client_module
from pipeline import transcript_summary_anthropic as anthropic_module
from pipeline import transcript_summary_env as env_module

Error = safe.ScreenError
MAX_JSON = 64 * 1024**2
MAX_WAVE_BYTES = 16 * 1024**2
DEFAULT_LIMITS = {"max_sources": 256, "max_total_source_bytes": 128 * 1024**2,
    "max_jobs": 8192, "max_waves": 1024, "max_jobs_per_wave": 64,
    "max_wave_bytes": MAX_WAVE_BYTES}
DEFAULT_BUDGET = {"max_reserved_microusd": 25_000_000, "max_attempts_per_job": 2}
SEMANTICS = {"visibility": "private", "machine_generated": True, "human_reviewed": False,
    "citation_links_are_not_factual_verification": True, "publication_authority": False,
    "identity_authority": False, "source_mutation": False, "raw_media_upload": False,
    "summaries_are_not_verified_quotations": True, "automatic_paid_retries": False}
MARKER = {"kind": "himr_private_transcript_summary_workspace", "schema_version": 1}
NAMES = ("transcript_summary.py", "transcript_summary_core.py", "transcript_summary_sources.py",
         "transcript_summary_client.py", "transcript_summary_anthropic.py", "transcript_summary_env.py", "transcript_summary_recovery.py", "transcript_summary_classification.py", "speaker_screen.py", "speaker_screen_core.py",
         "speaker_screen_engine.py", "speaker_screen_paths.py")
TERMINAL = {"completed", "failed", "cancelled", "expired"}
ANTHROPIC_REJECTED_HTTP = frozenset({400, 401, 402, 403, 404, 413, 422, 429})
PHASE_STAGES = {
    "transcripts": frozenset({"chunk", "transcript"}),
    "synthesis": frozenset({"timeline", "yearly", "archive", "topic"}),
}
PHASE_STAGES["all"] = PHASE_STAGES["transcripts"] | PHASE_STAGES["synthesis"]


def phase_stages(phase):
    if not isinstance(phase, str) or phase not in PHASE_STAGES:
        raise Error("summary phase must be all, transcripts or synthesis")
    return PHASE_STAGES[phase]


def phase_progress(sources, state, config):
    def scope_key(value):
        scope = value.get("scope", value)
        return value["stage"], scope["period"], tuple(sorted(scope["source_ids"]))

    completed = {scope_key(result) for result in state["results"] if result["scope"]["final"]}
    expected = core.planned_scopes(sources, config)
    remaining = {phase: sum(scope_key(scope) not in completed for scope in expected
                            if scope["stage"] in stages)
                 for phase, stages in PHASE_STAGES.items()}
    return {"transcript_phase_complete": remaining["transcripts"] == 0,
            "transcript_summaries_remaining": remaining["transcripts"],
            "synthesis_phase_complete": remaining["synthesis"] == 0}


def synthesis_gate(phase, progress):
    if phase == "synthesis" and not progress["transcript_phase_complete"]:
        return {"state": "waiting_for_transcripts", "wave_id": None,
                "phase": phase, **progress}
    return None


def implementation():
    return {name: hashlib.sha256((Path(__file__).parent / name).read_bytes()).hexdigest() for name in NAMES}


def canonical(value):
    return safe.canonical(value)


def digest(value):
    return hashlib.sha256(canonical(value)).hexdigest()


def parse(raw):
    def pairs(rows):
        value = {}
        for key, item in rows:
            if key in value:
                raise Error("duplicate summary JSON key")
            value[key] = item
        return value
    try:
        value = json.loads(raw, object_pairs_hook=pairs,
            parse_constant=lambda _: (_ for _ in ()).throw(Error("nonfinite summary JSON")))
        canonical(value)
        return value
    except (ValueError, UnicodeError, RecursionError) as error:
        raise Error("invalid bounded summary JSON") from error


def binding(path):
    path = safe.path_value(path)
    with safe.opened(path) as fd:
        return {"path": str(path), "sha256": safe.hash_fd(fd, MAX_JSON, time.monotonic() + 60)}


def read_bytes(ref):
    safe.file_binding(ref)
    with safe.opened(ref["path"]) as fd:
        before = safe.witness(fd)
        if not 0 < before["st_size"] <= MAX_JSON:
            raise Error("summary artifact exceeds finite size bound")
        raw = os.pread(fd, MAX_JSON + 1, 0)
        if len(raw) != before["st_size"] or safe.witness(fd) != before or hashlib.sha256(raw).hexdigest() != ref["sha256"]:
            raise Error("summary artifact changed or digest differs")
        return raw


def read(ref):
    return parse(read_bytes(ref))


def put_bytes(path, raw):
    """Atomic no-replace Linux publication; no crash-time hard-link artifacts."""
    path = Path(path)
    if not isinstance(raw, bytes) or not 0 < len(raw) <= MAX_JSON:
        raise Error("summary output exceeds finite byte bound")
    with paths.retained_directory(path.parent) as directory:
        temporary = ".summary-" + uuid.uuid4().hex
        fd = os.open(temporary, os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW, 0o600, dir_fd=directory)
        try:
            with os.fdopen(fd, "wb") as stream:
                stream.write(raw)
                stream.flush()
                os.fchmod(stream.fileno(), 0o400)
                os.fsync(stream.fileno())
            library = ctypes.CDLL(None, use_errno=True)
            rename = library.renameat2
            rename.argtypes = [ctypes.c_int, ctypes.c_char_p, ctypes.c_int, ctypes.c_char_p, ctypes.c_uint]
            rename.restype = ctypes.c_int
            if rename(directory, os.fsencode(temporary), directory, os.fsencode(path.name), 1):
                code = ctypes.get_errno()
                if code != errno.EEXIST:
                    raise OSError(code, os.strerror(code))
                if read_bytes(binding(path)) != raw:
                    raise Error("existing immutable summary artifact differs")
            os.fsync(directory)
        finally:
            try:
                os.unlink(temporary, dir_fd=directory)
            except FileNotFoundError:
                pass
    return binding(path)


def put(path, value):
    return put_bytes(path, canonical(value))


def mkdir(path):
    path = Path(path)
    with paths.retained_directory(path.parent) as directory:
        try:
            os.mkdir(path.name, 0o700, dir_fd=directory)
            os.fsync(directory)
        except FileExistsError:
            pass
    with paths.retained_directory(path):
        pass


@contextmanager
def locked(root):
    with paths.retained_directory(root) as directory:
        fd = os.open("execution.lock", os.O_RDWR | os.O_CREAT | os.O_NOFOLLOW, 0o600, dir_fd=directory)
        try:
            info = os.fstat(fd)
            if not stat.S_ISREG(info.st_mode) or info.st_nlink != 1 or info.st_uid != os.getuid() or info.st_mode & 0o077:
                raise Error("unsafe summary workspace lock")
            try:
                fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
            except BlockingIOError:
                raise Error("another summary command owns this workspace") from None
            yield
        finally:
            os.close(fd)


def all_paths(value):
    if isinstance(value, str) and value.startswith("/"):
        yield Path(value)
    elif isinstance(value, dict):
        for item in value.values():
            yield from all_paths(item)
    elif isinstance(value, list):
        for item in value:
            yield from all_paths(item)


def protect(root, value):
    protected = [ROOT / name for name in ("pipeline", "src", "public", "dist", ".git")]
    protected += list(all_paths(value))
    if any(root == item or root in item.parents or item in root.parents for item in protected):
        raise Error("summary workspace overlaps a source, request or protected publication/code path")


def validate_request(value):
    fields = {"kind", "schema_version", "state_root", "sources", "config", "limits", "budget", "cloud"}
    if isinstance(value, dict) and "recovery" in value:
        fields.add("recovery")
        safe.file_binding(value["recovery"])
    if isinstance(value, dict) and "classification_policy" in value:
        from pipeline import transcript_summary_classification as classification
        fields.add("classification_policy")
        if (value["classification_policy"] != classification.POLICY or
                value["config"]["transcript_profile"] != "gemini_flash_batch" or "broader_synthesis" in value["config"]):
            raise Error("unsupported classification inheritance policy")
    safe.exact(value, fields, "summary request")
    if value["kind"] != "himr_transcript_summary_request" or type(value["schema_version"]) is not int or value["schema_version"] != 1:
        raise Error("unsupported summary request")
    safe.path_value(value["state_root"])
    core.normalize_config(value["config"])
    safe.exact(value["limits"], DEFAULT_LIMITS, "summary limits")
    for key, low, high in (("max_sources", 1, 4096), ("max_total_source_bytes", 1024**2, 1024**3),
            ("max_jobs", 1, 50000), ("max_waves", 1, 4096), ("max_jobs_per_wave", 1, 128),
            ("max_wave_bytes", 16384, MAX_WAVE_BYTES)):
        safe.integer(value["limits"][key], low, high, key)
    if not isinstance(value["sources"], list) or not len(value["sources"]) <= value["limits"]["max_sources"]:
        raise Error("source selection exceeds finite limit")
    for spec in value["sources"]:
        sources_module.validate_spec(spec)
    safe.exact(value["budget"], DEFAULT_BUDGET, "summary budget")
    safe.integer(value["budget"]["max_reserved_microusd"], 1, 10_000_000_000, "budget ceiling")
    safe.integer(value["budget"]["max_attempts_per_job"], 1, 5, "maximum explicit attempts")
    safe.exact(value["cloud"], {"processing_approved", "paid_tier_confirmed"}, "cloud approval")
    if any(type(flag) is not bool for flag in value["cloud"].values()):
        raise Error("cloud approvals must be explicit booleans")
    return value


def recovery_input(request, sources):
    if "recovery" not in request:
        return None
    from pipeline import transcript_summary_recovery as recovery
    return recovery.load(request["recovery"], sources, request["config"])


def plan_initial(request, sources):
    imported = recovery_input(request, sources)
    return core.initial_jobs(sources, request["config"]) if imported is None else imported["initial_jobs"]


def create_plan(request_path, expected_sha256):
    ref = {"path": str(safe.path_value(request_path)), "sha256": expected_sha256}
    request = validate_request(read(ref))
    root = Path(request["state_root"])
    protect(root, {"request": ref, "sources": request["sources"]})
    normalized, total, ids, recordings = [], 0, set(), set()
    for spec in request["sources"]:
        source = sources_module.normalize_source(spec)
        total += len(canonical(source))
        if total > request["limits"]["max_total_source_bytes"]:
            raise Error("selected normalized transcript bytes exceed plan limit")
        if source["source_id"] in ids or source["recording_id"] in recordings:
            raise Error("select exactly one transcript revision per recording in a summary plan")
        ids.add(source["source_id"])
        recordings.add(source["recording_id"])
        normalized.append(source)
    initial = plan_initial(request, normalized)
    core.planned_scopes(normalized, request["config"])
    if len(initial) > request["limits"]["max_jobs"]:
        raise Error("initial jobs exceed finite plan limit")
    body = {"kind": "himr_transcript_summary_plan", "schema_version": 1, "request": ref,
        "request_value": request, "source_ids": [source["source_id"] for source in normalized],
        "source_bytes": total, "initial_job_ids": [job["job_id"] for job in initial],
        "implementation": implementation(), "semantics": SEMANTICS}
    plan = {**body, "plan_id": "summaryplan_" + digest(body)[:32]}
    mkdir(root)
    with locked(root), paths.retained_directory(root) as fd:
        if safe.exists(root / "workspace.json"):
            if read(binding(root / "workspace.json")) != MARKER:
                raise Error("summary workspace marker differs")
        else:
            with os.scandir(fd) as entries:
                if any(entry.name != "execution.lock" for entry in entries):
                    raise Error("refusing a nonempty unmarked summary workspace")
            put(root / "workspace.json", MARKER)
        mkdir(root / "sources")
        mkdir(root / "waves")
        for source in normalized:
            put(root / "sources" / (source["source_id"] + ".json"), source)
        plan_ref = put(root / "plan.json", plan)
    return {"plan": plan_ref, "plan_id": plan["plan_id"], "selected_transcripts": len(normalized),
        "initial_jobs": len(initial), "initial_reserved_microusd": sum(job["budget"]["maximum_cost_microusd"] for job in initial),
        "cloud_enabled": all(request["cloud"].values()), "network_calls": 0}


def load_plan(path, expected_sha256):
    path = safe.path_value(path)
    plan = read({"path": str(path), "sha256": expected_sha256})
    safe.exact(plan, {"kind", "schema_version", "request", "request_value", "source_ids", "source_bytes",
        "initial_job_ids", "implementation", "semantics", "plan_id"}, "summary plan")
    body = {key: value for key, value in plan.items() if key != "plan_id"}
    if (plan["kind"] != "himr_transcript_summary_plan" or type(plan["schema_version"]) is not int or plan["schema_version"] != 1
            or plan["plan_id"] != "summaryplan_" + digest(body)[:32] or plan["implementation"] != implementation()
            or plan["semantics"] != SEMANTICS):
        raise Error("summary plan or implementation changed")
    request = validate_request(plan["request_value"])
    if request != read(plan["request"]) or path != Path(request["state_root"]) / "plan.json":
        raise Error("summary request or plan location changed")
    root = path.parent
    protect(root, {"request": plan["request"], "sources": request["sources"]})
    if read(binding(root / "workspace.json")) != MARKER:
        raise Error("summary workspace marker differs")
    normalized = []
    for spec in request["sources"]:
        source = sources_module.normalize_source(spec)
        stored = read(binding(root / "sources" / (source["source_id"] + ".json")))
        if stored != source:
            raise Error("normalized source differs from current bound transcript")
        normalized.append(source)
    if [source["source_id"] for source in normalized] != plan["source_ids"] or sum(len(canonical(s)) for s in normalized) != plan["source_bytes"]:
        raise Error("summary source selection changed")
    if [job["job_id"] for job in plan_initial(request, normalized)] != plan["initial_job_ids"]:
        raise Error("initial summary job replay differs")
    return plan, normalized


def wire(jobs):
    if not jobs:
        raise Error("empty API batch is not permitted")
    if jobs[0]["provider"] == "openai":
        return b"".join(canonical({"custom_id": job["job_id"], "method": "POST", "url": "/v1/responses", "body": job["request"]["body"]}) for job in jobs)
    if jobs[0]["provider"] == "anthropic":
        # Stable job template. The separately sealed provider-requests.bin binds
        # each wire ID to this particular wave, including explicit retry attempts.
        return canonical([{"custom_id": job["job_id"], "params": job["request"]["body"]} for job in jobs])
    return canonical([{"key": job["job_id"], "request": job["request"]["body"]} for job in jobs])


def anthropic_custom_id(wave, job):
    return "summaryreq_" + digest({"wave_id": wave["wave_id"], "job_id": job["job_id"]})[:32]


def anthropic_requests(wave):
    if wave["provider"] != "anthropic":
        raise Error("Anthropic request requires an Anthropic wave")
    return [{"custom_id": anthropic_custom_id(wave, job), "params": job["request"]["body"]}
            for job in wave["jobs"]]


def submission_size(jobs):
    if jobs[0]["provider"] == "gemini":
        return len(client_module.gemini_batch_bytes(jobs[0]["model"], parse(wire(jobs)), "summarywave_" + "0" * 32))
    if jobs[0]["provider"] == "anthropic":
        dummy = {"provider": "anthropic", "wave_id": "summarywave_" + "0" * 32, "jobs": jobs}
        return anthropic_module.anthropic_batch_size(anthropic_requests(dummy))
    return len(wire(jobs))


def ready_jobs(sources, jobs, results, config, *, stages=None, initial_jobs_override=None):
    existing = {job["job_id"] for job in jobs}
    initial = core.initial_jobs(sources, config) if initial_jobs_override is None else initial_jobs_override
    missing = [job for job in initial if job["job_id"] not in existing]
    # Full initial coverage is part of the finite plan, even when transport waves
    # declare only a subset at a time. An unsubmitted chunk has no result.
    declared = jobs + missing
    return ([job for job in missing if stages is None or job["stage"] in stages]
            + core.next_jobs(sources, declared, {r["job_id"]: r for r in results}, config,
                             stages=stages, initial_jobs_override=initial_jobs_override))


def wave_folder(plan, wave_id):
    if not isinstance(wave_id, str) or not re.fullmatch(r"summarywave_[0-9a-f]{32}", wave_id):
        raise Error("invalid summary wave ID")
    return Path(plan["request_value"]["state_root"]) / "waves" / wave_id


def _read_optional(path):
    return read(binding(path)) if safe.exists(path) else None


def load_state(plan, sources):
    root = Path(plan["request_value"]["state_root"])
    with paths.retained_directory(root / "waves") as directory:
        with os.scandir(directory) as entries:
            names = [entry.name for entry in entries if entry.name.startswith("summarywave_")]
    if len(names) > plan["request_value"]["limits"]["max_waves"]:
        raise Error("summary wave inventory exceeds plan bound")
    waves = []
    for name in names:
        folder = wave_folder(plan, name)
        # A crash before manifest publication leaves an inert, unsubmitted folder.
        value = _read_optional(folder / "wave.json")
        if value is not None:
            waves.append(value)
    waves.sort(key=lambda w: w["ordinal"])
    imported = recovery_input(plan["request_value"], sources)
    initial_override = None if imported is None else imported["initial_jobs"]
    imported_rows = [] if imported is None else imported["retained"] + imported["recovered"]
    imported_results = {row["job_id"]: row["result"] for row in imported_rows}
    if imported is not None and "classification_policy" in imported and imported["classification_policy"] != plan["request_value"].get("classification_policy"):
        raise Error("imported classification policy differs from the plan")
    imported_jobs = [] if imported is None else imported.get("retained_jobs", initial_override)
    jobs = [job for job in imported_jobs if job["job_id"] in imported_results]
    if len({job["job_id"] for job in jobs}) != len(jobs) or {job["job_id"] for job in jobs} != set(imported_results):
        raise Error("imported result graph differs")
    results = [imported_results[job["job_id"]] for job in jobs]
    known, attempts, reserved = {job["job_id"]: job for job in jobs}, {}, 0
    collections, rejections = {}, {}
    for ordinal, wave in enumerate(waves):
        fields = {"kind", "schema_version", "plan_id", "ordinal", "provider", "model", "jobs", "retry_of",
            "input_sha256", "input_bytes", "maximum_cost_microusd", "wave_id"}
        if "classification_policy" in wave:
            fields.add("classification_policy")
        safe.exact(wave, fields, "summary wave")
        expected_policy = plan["request_value"].get("classification_policy") if wave["provider"] == "gemini" else None
        if wave.get("classification_policy") != expected_policy:
            raise Error("wave classification policy differs from the plan")
        body = {key: value for key, value in wave.items() if key != "wave_id"}
        if (wave["wave_id"] != "summarywave_" + digest(body)[:32] or wave["plan_id"] != plan["plan_id"]
                or wave["kind"] != "himr_transcript_summary_wave" or type(wave["schema_version"]) is not int or wave["schema_version"] != 1
                or type(wave["ordinal"]) is not int or wave["ordinal"] != ordinal):
            raise Error("summary wave identity or order differs")
        folder = wave_folder(plan, wave["wave_id"])
        if not isinstance(wave["jobs"], list) or not 1 <= len(wave["jobs"]) <= plan["request_value"]["limits"]["max_jobs_per_wave"]:
            raise Error("invalid bounded wave job list")
        for job in wave["jobs"]:
            core.validate_job(job)
        # Replay the complete declared graph, but do not construct unrelated
        # future reducers simply to inspect or collect an earlier-phase wave.
        candidates = {job["job_id"]: job for job in ready_jobs(
            sources, jobs, results, plan["request_value"]["config"],
            stages={job["stage"] for job in wave["jobs"]}, initial_jobs_override=initial_override)}
        if wave["retry_of"] is not None:
            if wave["retry_of"] in rejections:
                failed = set(rejections[wave["retry_of"]]["job_ids"])
            elif wave["retry_of"] in collections:
                failed = {row["job_id"] for row in collections[wave["retry_of"]]["outcomes"] if row["state"] != "completed"}
            else:
                raise Error("retry does not refer to a collected or definitively rejected earlier wave")
            active = {j["job_id"] for prior in waves[:ordinal]
                      if prior["wave_id"] not in collections and prior["wave_id"] not in rejections for j in prior["jobs"]}
            candidates = {key: known[key] for key in failed - active if key not in {r["job_id"] for r in results}}
        seen = set()
        for job in wave["jobs"]:
            core.validate_job(job)
            key = job["job_id"]
            if key in seen or key not in candidates or job != candidates[key] or job["provider"] != wave["provider"] or job["model"] != wave["model"]:
                raise Error("wave job differs from ready dependency frontier")
            seen.add(key)
            attempts[key] = attempts.get(key, 0) + 1
            if attempts[key] > plan["request_value"]["budget"]["max_attempts_per_job"]:
                raise Error("job exceeds explicit attempt limit")
            if key not in known:
                jobs.append(job)
                known[key] = job
        data = wire(wave["jobs"])
        if (wave["input_sha256"] != hashlib.sha256(data).hexdigest() or wave["input_bytes"] != len(data)
                or submission_size(wave["jobs"]) > plan["request_value"]["limits"]["max_wave_bytes"]
                or read_bytes({"path": str(folder / "requests.bin"), "sha256": wave["input_sha256"]}) != data
                or wave["maximum_cost_microusd"] != sum(j["budget"]["maximum_cost_microusd"] for j in wave["jobs"])):
            raise Error("sealed API batch input or reservation differs")
        if wave["provider"] == "anthropic":
            provider_data = anthropic_module.anthropic_batch_bytes(anthropic_requests(wave))
            if read_bytes(binding(folder / "provider-requests.bin")) != provider_data:
                raise Error("sealed Anthropic wave-specific input differs")
        if safe.exists(folder / "submit-intent.json"):
            intent = read(binding(folder / "submit-intent.json"))
            if intent != {"wave_id": wave["wave_id"], "maximum_cost_microusd": wave["maximum_cost_microusd"], "input_sha256": wave["input_sha256"]}:
                raise Error("submission intent differs")
            reserved += wave["maximum_cost_microusd"]
        rejection = _read_optional(folder / "submission-rejected.json")
        if rejection is not None:
            safe.exact(rejection, {"wave_id", "input_sha256", "status_code", "state", "automatic_resubmission"}, "submission rejection")
            if (wave["provider"] != "anthropic" or rejection["wave_id"] != wave["wave_id"]
                    or rejection["input_sha256"] != wave["input_sha256"]
                    or type(rejection["status_code"]) is not int or rejection["status_code"] not in ANTHROPIC_REJECTED_HTTP
                    or rejection["state"] != "definitive_http_rejection" or rejection["automatic_resubmission"] is not False
                    or not safe.exists(folder / "submit-intent.json")
                    or any(safe.exists(folder / name) for name in (
                        "submitted.json", "submission-response.json", "reconciliation-response.json",
                        "reconciliation-results.json", "capture.json", "collection.json"))):
                raise Error("definitive submission rejection proof differs")
            rejections[wave["wave_id"]] = {**rejection, "job_ids": [job["job_id"] for job in wave["jobs"]]}
        submitted = _read_optional(folder / "submitted.json")
        if submitted is not None:
            safe.exact(submitted, {"wave_id", "remote_id", "response", "reconciled"}, "submission receipt")
            if (submitted["wave_id"] != wave["wave_id"] or type(submitted["reconciled"]) is not bool
                    or not safe.exists(folder / "submit-intent.json")):
                raise Error("submission receipt lacks matching durable intent")
            safe.file_binding(submitted["response"])
            expected_name = "reconciliation-response.json" if submitted["reconciled"] else "submission-response.json"
            if Path(submitted["response"]["path"]) != folder / expected_name:
                raise Error("submission proof escapes owned wave")
            remote = read(submitted["response"])
            check_remote(wave, remote, submitted["remote_id"])
            if wave["provider"] == "anthropic" and submitted["reconciled"]:
                proof = read(binding(folder / "reconciliation-results.json"))
                if proof.get("batch") != remote:
                    raise Error("Anthropic reconciliation batch proof differs")
                check_anthropic_reconciliation(wave, proof)
            if wave["provider"] == "openai":
                uploaded = read(binding(folder / "uploaded-input.json"))
                if (uploaded.get("purpose") != "batch" or uploaded.get("bytes") != wave["input_bytes"]
                        or remote.get("input_file_id") != uploaded.get("id")):
                    raise Error("submitted OpenAI input differs from sealed upload")
        collection = _read_optional(folder / "collection.json")
        if collection is not None:
            replay = collect_result(wave, read(collection["capture"]))
            if collection != {**replay, "capture": collection["capture"]} or Path(collection["capture"]["path"]) != folder / "capture.json":
                raise Error("summary collection differs from provider capture replay")
            submitted = _read_optional(folder / "submitted.json")
            if submitted is None or not safe.exists(folder / "submit-intent.json"):
                raise Error("collection has no durable paid submission receipt")
            check_remote(wave, read(collection["capture"])["batch"], submitted["remote_id"])
            collections[wave["wave_id"]] = collection
            for row in collection["outcomes"]:
                if row["state"] == "completed":
                    if any(result["job_id"] == row["job_id"] for result in results):
                        raise Error("completed job was paid or collected twice")
                    results.append(row["result"])
    if len(jobs) > plan["request_value"]["limits"]["max_jobs"] or reserved > plan["request_value"]["budget"]["max_reserved_microusd"]:
        raise Error("summary state exceeds declared job or reservation ceiling")
    return {"waves": waves, "jobs": jobs, "results": results, "collections": collections, "rejections": rejections,
            "attempts": attempts, "reserved_microusd": reserved,
            "initial_jobs_override": initial_override, "imported_chunk_jobs": sum(row["result"]["stage"] == "chunk" for row in imported_rows),
            "imported_transcript_jobs": sum(row["result"]["stage"] == "transcript" for row in imported_rows),
            "recovered_chunk_jobs": 0 if imported is None else sum(row["result"]["stage"] == "chunk" for row in imported["recovered"]),
            "import_retry_jobs": [] if imported is None else [row["job_id"] for row in imported["retry"]]}


def create_wave(plan, state, candidates, retry_of=None):
    request = plan["request_value"]
    if not candidates:
        return {"state": "no_ready_jobs", "wave_id": None}
    if len(state["waves"]) >= request["limits"]["max_waves"]:
        raise Error("wave limit reached; no automatic expansion")
    first, selected = candidates[0], []
    for job in candidates:
        if (job["provider"], job["model"]) != (first["provider"], first["model"]):
            continue
        if state["attempts"].get(job["job_id"], 0) >= request["budget"]["max_attempts_per_job"]:
            continue
        proposed = selected + [job]
        if (len(proposed) > request["limits"]["max_jobs_per_wave"]
                or submission_size(proposed) > request["limits"]["max_wave_bytes"]
                or len(canonical(proposed)) + 8192 > MAX_JSON):
            break
        selected = proposed
    if not selected:
        raise Error("no job fits wave byte/attempt limits")
    if len({job["job_id"] for job in state["jobs"] + selected}) > request["limits"]["max_jobs"]:
        raise Error("hierarchy exceeds finite job budget")
    data = wire(selected)
    body = {"kind": "himr_transcript_summary_wave", "schema_version": 1, "plan_id": plan["plan_id"],
        "ordinal": len(state["waves"]), "provider": first["provider"], "model": first["model"],
        "jobs": selected, "retry_of": retry_of, "input_sha256": hashlib.sha256(data).hexdigest(),
        "input_bytes": len(data), "maximum_cost_microusd": sum(j["budget"]["maximum_cost_microusd"] for j in selected)}
    if "classification_policy" in request and first["provider"] == "gemini":
        body["classification_policy"] = request["classification_policy"]
    wave = {**body, "wave_id": "summarywave_" + digest(body)[:32]}
    folder = wave_folder(plan, wave["wave_id"])
    mkdir(folder)
    put_bytes(folder / "requests.bin", data)
    if wave["provider"] == "anthropic":
        put_bytes(folder / "provider-requests.bin",
                  anthropic_module.anthropic_batch_bytes(anthropic_requests(wave)))
    ref = put(folder / "wave.json", wave)
    return {"state": "prepared", "wave_id": wave["wave_id"], "manifest": ref, "jobs": len(selected),
        "provider": wave["provider"], "model": wave["model"], "maximum_cost_microusd": wave["maximum_cost_microusd"]}


def prepare_plan(path, expected_sha256, retry_wave=None, *, phase="all"):
    stages = phase_stages(phase)
    plan, sources = load_plan(path, expected_sha256)
    with locked(Path(plan["request_value"]["state_root"])):
        state = load_state(plan, sources)
        progress = phase_progress(sources, state, plan["request_value"]["config"])
        blocked = synthesis_gate(phase, progress)
        if blocked is not None:
            return blocked
        if retry_wave is not None:
            if retry_wave in state["rejections"]:
                failed = set(state["rejections"][retry_wave]["job_ids"])
            elif retry_wave in state["collections"]:
                failed = {r["job_id"] for r in state["collections"][retry_wave]["outcomes"] if r["state"] != "completed"}
            else:
                raise Error("retry requires a terminal collected or definitively rejected wave")
            active = {j["job_id"] for w in state["waves"]
                      if w["wave_id"] not in state["collections"] and w["wave_id"] not in state["rejections"] for j in w["jobs"]}
            complete = {r["job_id"] for r in state["results"]}
            return create_wave(plan, state, [j for j in state["jobs"]
                if j["job_id"] in failed - active - complete and j["stage"] in stages], retry_wave)
        for wave in state["waves"]:
            folder = wave_folder(plan, wave["wave_id"])
            if not safe.exists(folder / "submit-intent.json"):
                matching = [job for job in wave["jobs"] if job["stage"] in stages]
                if not matching:
                    continue
                if len(matching) != len(wave["jobs"]):
                    raise Error("prepared wave mixes summary phases; inspect it before choosing --phase all")
                return {"state": "already_prepared", "wave_id": wave["wave_id"], "jobs": len(wave["jobs"]),
                        "phase": phase, "provider": wave["provider"], "model": wave["model"],
                        "maximum_cost_microusd": wave["maximum_cost_microusd"]}
        candidates = ready_jobs(sources, state["jobs"], state["results"],
                                plan["request_value"]["config"], stages=stages,
                                initial_jobs_override=state.get("initial_jobs_override"))
        if not candidates and phase != "all" and progress[
                "transcript_phase_complete" if phase == "transcripts" else "synthesis_phase_complete"]:
            return {"state": "phase_complete", "wave_id": None, "phase": phase, **progress}
        return {**create_wave(plan, state, candidates), "phase": phase}


def api_client(provider, *, env_file=None):
    providers = {"gemini": ("GEMINI_API_KEY", client_module.GeminiBatchClient),
                 "openai": ("OPENAI_API_KEY", client_module.OpenAIBatchClient),
                 "anthropic": ("ANTHROPIC_API_KEY", anthropic_module.AnthropicBatchClient)}
    if provider not in providers:
        raise Error("unsupported summary API provider")
    name, factory = providers[provider]
    key = env_module.api_key(name, default_path=ROOT / ".env", env_file=env_file)
    if not key:
        raise Error(name + " is required only for explicit API commands")
    return factory(key)


def remote_view(provider, value):
    if not isinstance(value, dict):
        raise Error("invalid provider batch receipt")
    if provider == "openai":
        if not isinstance(value.get("metadata", {}), dict) or value.get("status") not in client_module.BATCH_STATUSES:
            raise Error("invalid OpenAI batch metadata/state")
        return {"id": value.get("id"), "status": value.get("status"),
            "tag": value.get("metadata", {}).get("himr_wave"), "model": None}
    if provider == "anthropic":
        anthropic_module.validate_batch(value)
        return {"id": value["id"],
                "status": "completed" if value["processing_status"] == "ended" else "in_progress",
                "tag": None, "model": None}
    if provider != "gemini":
        raise Error("unsupported summary API provider")
    metadata = value.get("metadata", {})
    if not isinstance(metadata, dict):
        raise Error("invalid Gemini operation metadata")
    if type(value.get("done", False)) is not bool or ("response" in value and "error" in value):
        raise Error("invalid Gemini operation terminal envelope")
    status = metadata.get("state", "BATCH_STATE_PENDING")
    names = {"SUCCEEDED": "completed", "FAILED": "failed", "CANCELLED": "cancelled", "EXPIRED": "expired",
             "PENDING": "in_progress", "RUNNING": "in_progress", "UNSPECIFIED": "in_progress"}
    match = re.fullmatch(r"(?:BATCH|JOB)_STATE_(.*)", status) if isinstance(status, str) else None
    if not match or match[1] not in names:
        raise Error("unknown Gemini batch state")
    normalized = names[match[1]]
    done = value.get("done", False)
    if "error" in value:
        if (not done or not isinstance(value["error"], dict)
                or type(value["error"].get("code")) is not int or not 0 <= value["error"]["code"] <= 16):
            raise Error("invalid Gemini terminal error")
        normalized = "cancelled" if value["error"].get("code") == 1 else "failed"
    elif (normalized in TERMINAL) != done or ("response" in value and not done):
        raise Error("Gemini state and operation completion disagree")
    return {"id": value.get("name"), "status": normalized,
        "tag": metadata.get("displayName", metadata.get("display_name")), "model": metadata.get("model")}


def check_remote(wave, value, remote_id=None):
    view = remote_view(wave["provider"], value)
    if wave["provider"] == "anthropic":
        # Anthropic has no batch-level application metadata. A normal POST is
        # bound by our durable local receipt. Lost POSTs additionally require
        # complete terminal, wave-specific result IDs before adoption.
        anthropic_module.validate_batch(value, expected_id=remote_id,
                                        expected_count=len(wave["jobs"]))
        return view
    pattern = r"batches/[A-Za-z0-9_-]{1,128}" if wave["provider"] == "gemini" else r"batch_[A-Za-z0-9_-]{1,128}"
    if not isinstance(view["id"], str) or not re.fullmatch(pattern, view["id"]) or (remote_id is not None and view["id"] != remote_id):
        raise Error("remote batch identity differs")
    if view["tag"] != wave["wave_id"]:
        raise Error("remote batch does not carry this exact wave identity")
    if wave["provider"] == "gemini":
        if view["model"] not in (wave["model"], "models/" + wave["model"]):
            raise Error("remote Gemini model differs")
    elif value.get("endpoint") != "/v1/responses" or value.get("completion_window") != "24h":
        raise Error("remote OpenAI batch endpoint/window differs")
    return view


def _find_wave(state, wave_id):
    found = [w for w in state["waves"] if w["wave_id"] == wave_id]
    if len(found) != 1:
        raise Error("wave is not in this plan")
    return found[0]


def submit_wave(path, expected_sha256, wave_id, *, allow_paid_api=False, client=None, phase="all", env_file=None):
    stages = phase_stages(phase)
    plan, sources = load_plan(path, expected_sha256)
    root = Path(plan["request_value"]["state_root"])
    with locked(root):
        state = load_state(plan, sources)
        wave = _find_wave(state, wave_id)
        if any(job["stage"] not in stages for job in wave["jobs"]):
            raise Error("wave contains jobs outside the requested summary phase")
        folder = wave_folder(plan, wave_id)
        submitted = _read_optional(folder / "submitted.json")
        if submitted is not None:
            return {"state": "already_submitted", "wave_id": wave_id, "remote_id": submitted["remote_id"]}
        if wave_id in state["rejections"]:
            return {"state": "submission_rejected", "wave_id": wave_id,
                    "status_code": state["rejections"][wave_id]["status_code"], "automatic_resubmission": False}
        if safe.exists(folder / "submit-intent.json"):
            return {"state": "needs_reconciliation", "wave_id": wave_id, "automatic_resubmission": False}
        if phase == "synthesis":
            blocked = synthesis_gate(phase, phase_progress(sources, state, plan["request_value"]["config"]))
            if blocked is not None:
                return blocked
        if not allow_paid_api or not all(plan["request_value"]["cloud"].values()):
            raise Error("paid submission requires approved cloud processing, paid-tier confirmation and --allow-paid-api")
        today = dt.datetime.now(dt.timezone.utc).date().isoformat()
        if any(today > job["budget"]["pricing_valid_until"] for job in wave["jobs"]):
            raise Error("pricing review expired; update rates and create a new reviewed plan")
        if state["reserved_microusd"] + wave["maximum_cost_microusd"] > plan["request_value"]["budget"]["max_reserved_microusd"]:
            raise Error("paid wave exceeds remaining local reservation budget")
        # Pure local admission must succeed before a durable paid intent exists.
        if submission_size(wave["jobs"]) > plan["request_value"]["limits"]["max_wave_bytes"]:
            raise Error("provider submission exceeds wave byte limit")
        client = client or api_client(wave["provider"], env_file=env_file)
        put(folder / "submit-intent.json", {"wave_id": wave_id, "maximum_cost_microusd": wave["maximum_cost_microusd"], "input_sha256": wave["input_sha256"]})
        # A crash or uncertain POST after this point never causes automatic replay.
        if wave["provider"] == "gemini":
            remote = client.create_batch(wave["model"], parse(wire(wave["jobs"])), wave_id)
            # Operation metadata may initially omit binding fields. Preserve the
            # returned batch name before semantic checks; never repeat its POST.
            put(folder / "submission-response.json", remote)
        elif wave["provider"] == "anthropic":
            data = anthropic_module.anthropic_batch_bytes(anthropic_requests(wave))
            if read_bytes(binding(folder / "provider-requests.bin")) != data:
                raise Error("Anthropic submission differs from sealed request")
            try:
                remote = client.create_batch(anthropic_requests(wave))
            except client_module.BatchClientError as error:
                if (error.ambiguous or type(error.status_code) is not int
                        or error.status_code not in ANTHROPIC_REJECTED_HTTP):
                    raise
                # A confirmed rejected POST created no batch. Preserve an audit
                # proof and allow only a separately prepared, paid retry attempt.
                put(folder / "submission-rejected.json", {"wave_id": wave_id,
                    "input_sha256": wave["input_sha256"], "status_code": error.status_code,
                    "state": "definitive_http_rejection", "automatic_resubmission": False})
                return {"state": "submission_rejected", "wave_id": wave_id,
                        "status_code": error.status_code, "automatic_resubmission": False}
        else:
            uploaded = client.upload_batch(wire(wave["jobs"]))
            put(folder / "uploaded-input.json", uploaded)
            remote = client.create_batch(uploaded["id"], {"himr_wave": wave_id, "himr_plan": plan["plan_id"]})
        view = check_remote(wave, remote)
        capture = put(folder / "submission-response.json", remote)
        put(folder / "submitted.json", {"wave_id": wave_id, "remote_id": view["id"], "response": capture, "reconciled": False})
        return {"state": "submitted", "wave_id": wave_id, "remote_id": view["id"], "reserved_microusd": wave["maximum_cost_microusd"]}


def reconcile_wave(path, expected_sha256, wave_id, remote_id, *, client=None, env_file=None):
    """Explicitly adopt one provider batch after a lost submission response."""
    plan, sources = load_plan(path, expected_sha256)
    with locked(Path(plan["request_value"]["state_root"])):
        state = load_state(plan, sources)
        wave = _find_wave(state, wave_id)
        folder = wave_folder(plan, wave_id)
        if wave_id in state["rejections"]:
            raise Error("definitively rejected submission has no remote batch; prepare an explicit retry")
        if not safe.exists(folder / "submit-intent.json"):
            raise Error("reconciliation requires a durable submission intent")
        previous = _read_optional(folder / "submitted.json")
        if previous is not None:
            if previous["remote_id"] != remote_id:
                raise Error("wave already refers to a different remote batch")
            return {"state": "already_submitted", "remote_id": remote_id}
        proof = (_read_optional(folder / "reconciliation-results.json")
                 if wave["provider"] == "anthropic" else None)
        if proof is not None:
            # Reuse the complete local result proof after a crash, even when the
            # provider's retention window has elapsed. No additional HTTP/key.
            check_anthropic_reconciliation(wave, proof)
            remote = proof["batch"]
        else:
            client = client or api_client(wave["provider"], env_file=env_file)
            remote = client.get_batch(remote_id)
        view = check_remote(wave, remote, remote_id)
        if wave["provider"] == "anthropic":
            if view["status"] not in TERMINAL:
                return {"state": "needs_reconciliation", "wave_id": wave_id,
                        "remote_state": "in_progress", "automatic_resubmission": False,
                        "reason": "Anthropic requires terminal wave-specific result IDs before adoption"}
            if proof is None:
                proof = {"batch": remote, "items": remote_items("anthropic", remote, client)}
            check_anthropic_reconciliation(wave, proof)
            put(folder / "reconciliation-results.json", proof)
        if wave["provider"] == "openai":
            uploaded = _read_optional(folder / "uploaded-input.json")
            if uploaded is None or remote.get("input_file_id") != uploaded["id"]:
                raise Error("remote batch does not reference the known uploaded input")
        ref = put(folder / "reconciliation-response.json", remote)
        put(folder / "submitted.json", {"wave_id": wave_id, "remote_id": remote_id, "response": ref, "reconciled": True})
        return {"state": "reconciled", "wave_id": wave_id, "remote_id": remote_id}


def check_anthropic_reconciliation(wave, capture):
    """Counts alone cannot prove an untagged Anthropic batch belongs to a wave."""
    safe.exact(capture, {"batch", "items"}, "Anthropic reconciliation capture")
    check_remote(wave, capture["batch"])
    if capture["batch"]["processing_status"] != "ended" or not isinstance(capture["items"], list):
        raise Error("Anthropic reconciliation requires complete terminal results")
    expected = {anthropic_custom_id(wave, job): job for job in wave["jobs"]}
    seen = set()
    for row in capture["items"]:
        safe.exact(row, {"custom_id", "response", "error"}, "Anthropic reconciliation item")
        key = row["custom_id"]
        if not isinstance(key, str) or key not in expected or key in seen:
            raise Error("Anthropic reconciliation has foreign or duplicate wave request IDs")
        seen.add(key)
        if row["response"] is not None:
            if not isinstance(row["response"], dict) or row["response"].get("model") != expected[key]["model"]:
                raise Error("Anthropic reconciliation model differs")
    if seen != set(expected):
        raise Error("Anthropic reconciliation requires every expected wave request ID")
    collect_result(wave, capture)


def response_payload(provider, response):
    """Only complete structured text; never use thought text, refusals or tools."""
    if not isinstance(response, dict):
        raise Error("missing provider response")
    if provider == "openai":
        if response.get("status") != "completed" or response.get("error") is not None or response.get("incomplete_details") is not None:
            raise Error("incomplete or failed OpenAI response")
        output = response.get("output", [])
        if not isinstance(output, list) or any(not isinstance(item, dict) or item.get("type") not in ("message", "reasoning") for item in output):
            raise Error("unsupported OpenAI response output")
        messages = [item for item in output if item.get("type") == "message"]
        if len(messages) != 1 or messages[0].get("status") != "completed":
            raise Error("missing complete assistant message")
        parts = messages[0].get("content", [])
        if not isinstance(parts, list) or len(parts) != 1 or not isinstance(parts[0], dict) or parts[0].get("type") != "output_text":
            raise Error("refused or non-text summary output")
        text = parts[0].get("text")
    elif provider == "anthropic":
        if (response.get("type") != "message" or response.get("role") != "assistant"
                or response.get("stop_reason") != "end_turn"
                or response.get("stop_sequence") is not None):
            raise Error("incomplete, refused or non-assistant Anthropic response")
        content = response.get("content")
        if not isinstance(content, list) or any(not isinstance(part, dict)
                or part.get("type") not in {"text", "thinking", "redacted_thinking"} for part in content):
            raise Error("unsupported Anthropic response content")
        # Adaptive thinking may be hidden, summarized or redacted; never treat it
        # as summary text or source evidence, and never depend on block position.
        parts = [part for part in content if part["type"] == "text"]
        if (len(parts) != 1 or set(parts[0]) - {"type", "text", "citations"}
                or parts[0].get("citations") not in (None, [])):
            raise Error("missing complete Anthropic structured text")
        text = parts[0].get("text")
    elif provider == "gemini":
        candidates = response.get("candidates", [])
        feedback = response.get("promptFeedback", {})
        if (not isinstance(feedback, dict) or feedback.get("blockReason") or not isinstance(candidates, list)
                or len(candidates) != 1 or not isinstance(candidates[0], dict) or candidates[0].get("finishReason") != "STOP"):
            raise Error("blocked or incomplete Gemini response")
        content = candidates[0].get("content", {})
        if not isinstance(content, dict) or not isinstance(content.get("parts"), list) or any(not isinstance(part, dict) for part in content["parts"]):
            raise Error("invalid Gemini message parts")
        if any(set(part) - {"text", "thought", "thoughtSignature"} for part in content["parts"]):
            raise Error("tool-bearing Gemini output is not a transcript summary")
        parts = [part for part in content["parts"] if part.get("thought") is not True]
        if (len(parts) != 1 or not isinstance(parts[0].get("text"), str)
                or set(parts[0]) - {"text", "thought", "thoughtSignature"}):
            raise Error("missing complete Gemini structured text")
        text = parts[0]["text"]
    else:
        raise Error("unsupported summary API provider")
    if not isinstance(text, str) or len(text.encode()) > 512 * 1024:
        raise Error("summary text exceeds output bound")
    return parse(text)


def collect_result(wave, capture):
    safe.exact(capture, {"batch", "items"}, "provider capture")
    view = check_remote(wave, capture["batch"])
    if view["status"] not in TERMINAL or not isinstance(capture["items"], list):
        raise Error("only terminal batch results can be collected")
    expected = {(anthropic_custom_id(wave, job) if wave["provider"] == "anthropic" else job["job_id"]): job["job_id"]
                for job in wave["jobs"]}
    by_id = {}
    observed = {kind: 0 for kind in ("succeeded", "errored", "canceled", "expired")}
    for row in capture["items"]:
        safe.exact(row, {"custom_id", "response", "error"}, "batch result item")
        key = row["custom_id"]
        if not isinstance(key, str) or key not in expected or expected[key] in by_id:
            raise Error("provider output contains unknown or duplicate job IDs")
        by_id[expected[key]] = row
        if wave["provider"] == "anthropic":
            if row["response"] is not None and row["error"] is None:
                kind = "succeeded"
            elif row["response"] is None and isinstance(row["error"], dict):
                kind = row["error"].get("code")
                if not isinstance(kind, str) or kind not in observed or kind == "succeeded":
                    raise Error("invalid retained Anthropic failure kind")
            else:
                raise Error("inconsistent Anthropic result envelope")
            observed[kind] += 1
    if wave["provider"] == "anthropic" and any(
            count > capture["batch"]["request_counts"][kind] for kind, count in observed.items()):
        # Missing rows become review-required, but returned rows cannot claim
        # more successes/failures of any kind than the provider's terminal batch.
        raise Error("Anthropic result outcomes contradict terminal request counts")
    outcomes = []
    for job in wave["jobs"]:
        row = by_id.get(job["job_id"])
        result, failure, detail, adjustments = None, None, None, []
        if row is None:
            failure = "missing_terminal_result"
        elif row["error"] is not None or row["response"] is None:
            failure = "provider_request_failed"
        else:
            try:
                if not isinstance(row["response"], dict):
                    raise Error("invalid provider response object")
                reported = row["response"].get("modelVersion" if wave["provider"] == "gemini" else "model")
                if wave["provider"] == "anthropic" and reported != job["model"]:
                    raise Error("Anthropic response must identify the exact summary model")
                if reported is not None and (not isinstance(reported, str) or not (
                        reported == job["model"] or (wave["provider"] == "gemini" and reported.startswith(job["model"] + "-")))):
                    raise Error("provider reported a different summary model")
                payload = response_payload(wave["provider"], row["response"])
                if "classification_policy" in wave:
                    from pipeline import transcript_summary_classification as classification
                    if wave["classification_policy"] != classification.POLICY or wave["provider"] != "gemini":
                        raise Error("unsupported wave classification policy")
                    result, adjustments = classification.normalize(job, payload)
                else:
                    result = core.normalize_api_result(job, payload)
            except (Error, ValueError, RuntimeError, KeyError, TypeError) as error:
                failure = "output_needs_review"
                # New contracts retain a local validation diagnosis. Do not
                # change the sealed collection format of legacy paid batches.
                if "max_evidence_refs_per_item" in job["config"]:
                    detail = str(error)[:300]
        outcome = {"job_id": job["job_id"], "state": "completed" if result is not None else "needs_review",
                   "result": result, "failure": failure}
        if "max_evidence_refs_per_item" in job["config"]:
            outcome["validation_error"] = detail
        if job["config"].get("gemini_schema_policy") == "local_array_bounds_v2":
            # Persist bounded codes, never echo arbitrary provider error text
            # (which can contain request content) into controller diagnostics.
            code = row.get("error", {}).get("code") if row and isinstance(row.get("error"), dict) else None
            outcome["provider_error_code"] = code if wave["provider"] == "gemini" and type(code) is int and 0 <= code <= 16 else None
        if "classification_policy" in wave:
            outcome["classification_adjustments"] = adjustments
        outcomes.append(outcome)
    return {"kind": "himr_transcript_summary_collection", "schema_version": 1,
        "wave_id": wave["wave_id"], "remote_id": view["id"], "remote_state": view["status"], "outcomes": outcomes}


def remote_items(provider, remote, client):
    if provider == "anthropic":
        anthropic_module.validate_batch(remote)
        if remote["processing_status"] != "ended":
            raise Error("Anthropic results require an ended batch")
        if remote["results_url"] is None:
            raise Error("Anthropic terminal result file is unavailable; no automatic resubmission")
        raw = client.download_results(remote["id"])
        if not isinstance(raw, bytes) or len(raw) > client_module.MAX_RESPONSE_BYTES:
            raise Error("Anthropic result file exceeds byte limit")
        rows = []
        for line in raw.splitlines():
            if not line.strip() or len(rows) >= client_module.MAX_BATCH_REQUESTS:
                raise Error("invalid or oversized Anthropic results file")
            item = parse(line)
            safe.exact(item, {"custom_id", "result"}, "Anthropic result row")
            result = item["result"]
            if not isinstance(result, dict):
                raise Error("invalid Anthropic result envelope")
            kind = result.get("type")
            if kind == "succeeded":
                safe.exact(result, {"type", "message"}, "Anthropic successful result")
                if not isinstance(result["message"], dict):
                    raise Error("invalid Anthropic successful message")
                response, error = result["message"], None
            elif kind == "errored":
                safe.exact(result, {"type", "error"}, "Anthropic failed result")
                if not isinstance(result["error"], dict):
                    raise Error("invalid Anthropic request error")
                response, error = None, {"code": "errored"}
            elif kind in {"canceled", "expired"}:
                safe.exact(result, {"type"}, "Anthropic terminal result")
                response, error = None, {"code": kind}
            else:
                raise Error("unknown Anthropic request outcome")
            rows.append({"custom_id": item["custom_id"], "response": response, "error": error})
        return rows
    if provider == "gemini":
        metadata = remote.get("metadata", {})
        primary = metadata.get("output")
        alternate = remote.get("response")
        if primary is not None and not isinstance(primary, dict):
            raise Error("invalid Gemini metadata output")
        if alternate is not None and not isinstance(alternate, dict):
            raise Error("invalid Gemini response output")
        # Operation.response may be an output payload, a resource with output,
        # or merely resource metadata. Compare actual output containers only.
        def payload(value):
            if value is None:
                return None
            nested = value.get("output", value)
            if not isinstance(nested, dict):
                raise Error("invalid Gemini resource output")
            keys = {"inlinedResponses", "responsesFile"} & set(nested)
            return {key: nested[key] for key in keys} if keys else None
        first, second = payload(primary), payload(alternate)
        if first is not None and second is not None and first != second:
            raise Error("conflicting Gemini output containers")
        output = first if first is not None else (second or {})
        rows = output.get("inlinedResponses", {})
        if isinstance(rows, dict):
            rows = rows.get("inlinedResponses", [])
        if not isinstance(rows, list):
            raise Error("unsupported Gemini inline response envelope")
        if output.get("responsesFile"):
            raise Error("unexpected file output for an inline Gemini batch")
        if any(not isinstance(row, dict) or not isinstance(row.get("metadata", {}), dict) for row in rows):
            raise Error("invalid Gemini result rows")
        return [{"custom_id": row.get("metadata", {}).get("key"), "response": row.get("response"), "error": row.get("error")} for row in rows]
    rows = []
    for key in ("output_file_id", "error_file_id"):
        if remote.get(key) is None:
            continue
        raw = client.download_file(remote[key])
        for line in raw.splitlines():
            if not line.strip():
                raise Error("empty line in provider output file")
            item = parse(line)
            response = item.get("response")
            success = isinstance(response, dict) and response.get("status_code") == 200
            rows.append({"custom_id": item.get("custom_id"), "response": response.get("body") if success else None,
                         "error": item.get("error") if success else {"code": "request_failed"}})
    return rows


def poll_wave(path, expected_sha256, wave_id, *, client=None, env_file=None):
    plan, sources = load_plan(path, expected_sha256)
    with locked(Path(plan["request_value"]["state_root"])):
        state = load_state(plan, sources)
        wave = _find_wave(state, wave_id)
        folder = wave_folder(plan, wave_id)
        if wave_id in state["collections"]:
            return {"state": "already_collected", "wave_id": wave_id}
        submitted = _read_optional(folder / "submitted.json")
        if submitted is None:
            raise Error("batch is not durably submitted; reconcile an ambiguous submission first")
        capture = _read_optional(folder / "capture.json")
        if capture is None and wave["provider"] == "anthropic" and submitted["reconciled"]:
            capture = read(binding(folder / "reconciliation-results.json"))
            check_anthropic_reconciliation(wave, capture)
            put(folder / "capture.json", capture)
        if capture is None:
            client = client or api_client(wave["provider"], env_file=env_file)
            remote = client.get_batch(submitted["remote_id"])
            view = check_remote(wave, remote, submitted["remote_id"])
            if wave["provider"] == "openai":
                uploaded = read(binding(folder / "uploaded-input.json"))
                if remote.get("input_file_id") != uploaded["id"]:
                    raise Error("provider batch input file differs")
            if view["status"] not in TERMINAL:
                return {"state": "remote_pending", "remote_state": view["status"], "wave_id": wave_id}
            capture = {"batch": remote, "items": remote_items(wave["provider"], remote, client)}
            collect_result(wave, capture)  # Validate whole set before committing.
            put(folder / "capture.json", capture)
        result = collect_result(wave, capture)
        put(folder / "collection.json", {**result, "capture": binding(folder / "capture.json")})
        return {"state": "collected", "wave_id": wave_id,
            "completed": sum(row["state"] == "completed" for row in result["outcomes"]),
            "needs_review": sum(row["state"] != "completed" for row in result["outcomes"])}


def status_plan(path, expected_sha256, *, phase="all"):
    plan, sources = load_plan(path, expected_sha256)
    state = load_state(plan, sources)
    return status_from_state(plan, sources, state, phase=phase)


def status_from_state(plan, sources, state, *, phase="all"):
    """Reuse one validated snapshot for progress and accounting, avoiding replay twice."""
    stages = phase_stages(phase)
    progress = phase_progress(sources, state, plan["request_value"]["config"])
    blocked = synthesis_gate(phase, progress)
    ready = ready_jobs(sources, state["jobs"], state["results"], plan["request_value"]["config"],
                       stages=frozenset() if blocked else stages,
                       initial_jobs_override=state.get("initial_jobs_override"))
    completed = {row["job_id"] for row in state["results"]}
    failures = {row["job_id"] for c in state["collections"].values() for row in c["outcomes"] if row["state"] != "completed"} - completed
    failures |= {job_id for rejection in state["rejections"].values() for job_id in rejection["job_ids"]} - completed
    failures &= {job["job_id"] for job in state["jobs"] if job["stage"] in stages}
    failure_reasons = {}
    for collection in state["collections"].values():
        for row in collection["outcomes"]:
            if row["job_id"] in failures:
                reason = row.get("validation_error") or row["failure"]
                if row.get("provider_error_code") is not None:
                    code = row["provider_error_code"]
                    name = {3: "INVALID_ARGUMENT", 8: "RESOURCE_EXHAUSTED", 13: "INTERNAL", 14: "UNAVAILABLE"}.get(code, "PROVIDER_ERROR")
                    reason += f": {name} ({code})"
                failure_reasons[reason] = failure_reasons.get(reason, 0) + 1
    pending, ambiguous, prepared = [], [], []
    relevant_waves = {wave["wave_id"] for wave in state["waves"]
                      if any(job["stage"] in stages for job in wave["jobs"])}
    for wave in state["waves"]:
        folder = wave_folder(plan, wave["wave_id"])
        if (wave["wave_id"] not in relevant_waves or wave["wave_id"] in state["collections"]
                or wave["wave_id"] in state["rejections"]):
            continue
        if safe.exists(folder / "submitted.json"):
            pending.append(wave["wave_id"])
        elif safe.exists(folder / "submit-intent.json"):
            ambiguous.append(wave["wave_id"])
        else:
            prepared.append(wave["wave_id"])
    finals = [result for result in state["results"] if result["scope"]["final"]]
    scopes = core.planned_scopes(sources, plan["request_value"]["config"])
    outcome = ("needs_reconciliation" if ambiguous else "waiting_remote" if pending
               else "waiting_for_transcripts" if blocked else "prepared" if prepared
               else "ready" if ready else "needs_review" if failures else "completed")
    return {"kind": "himr_transcript_summary_status", "schema_version": 1, "state": outcome,
        "phase": phase, **progress,
        "plan_id": plan["plan_id"], "selected_transcripts": len(sources), "declared_jobs": len(state["jobs"]),
        "completed_jobs": len(completed), "failed_jobs": len(failures), "ready_jobs": len(ready),
        "failure_reasons": failure_reasons,
        "imported_chunk_jobs": state.get("imported_chunk_jobs", 0),
        "recovered_chunk_jobs": state.get("recovered_chunk_jobs", 0),
        "imported_transcript_jobs": state.get("imported_transcript_jobs", 0),
        "transcript_summaries_complete": sum(r["stage"] == "transcript" for r in finals),
        "timeline_summaries_complete": sum(r["stage"] == "timeline" for r in finals),
        "yearly_summaries_complete": sum(r["stage"] == "yearly" for r in finals),
        "archive_summaries_complete": sum(r["stage"] == "archive" for r in finals),
        "topic_summaries_complete": sum(r["stage"] == "topic" for r in finals),
        "planned_summary_counts": {stage: sum(scope["stage"] == stage for scope in scopes)
                                   for stage in ("transcript", "timeline", "yearly", "archive", "topic")},
        "empty_transcripts": sum(not any(row["text"].strip() for row in s["segments"]) for s in sources),
        "prepared_waves": prepared, "pending_waves": pending, "ambiguous_waves": ambiguous,
        "rejected_waves": sorted(set(state["rejections"]) & relevant_waves),
        "reserved_microusd": state["reserved_microusd"],
        "budget_microusd": plan["request_value"]["budget"]["max_reserved_microusd"],
        "cloud_enabled": all(plan["request_value"]["cloud"].values()), "semantics": SEMANTICS}


def export_plan(path, expected_sha256, *, phase="all"):
    stages = phase_stages(phase)
    plan, sources = load_plan(path, expected_sha256)
    root = Path(plan["request_value"]["state_root"])
    with locked(root):
        state = load_state(plan, sources)
        finals = [r for r in state["results"] if r["scope"]["final"] and r["stage"] in stages]
        progress = phase_progress(sources, state, plan["request_value"]["config"])
        complete = progress["transcript_phase_complete"] and progress["synthesis_phase_complete"]
        phase_complete = (complete if phase == "all" else progress[
            "transcript_phase_complete" if phase == "transcripts" else "synthesis_phase_complete"])
        body = {"kind": "himr_private_summary_export", "schema_version": 1, "plan_id": plan["plan_id"],
            "results": finals, "selected_source_ids": plan["source_ids"], "semantics": SEMANTICS,
            "phase": phase, "phase_complete": phase_complete, "complete": complete}
        mkdir(root / "exports")
        target = root / "exports" / ("summaries-" + digest(body)[:32] + ".json")
        return {"state": "exported_private", "artifact": put(target, body), "final_summaries": len(finals),
                "phase": phase, "phase_complete": phase_complete, "complete": complete}


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    commands = parser.add_subparsers(dest="command", required=True)
    plan = commands.add_parser("plan")
    plan.add_argument("--request", required=True)
    plan.add_argument("--expected-sha256", required=True)
    for name in ("prepare", "status", "submit", "poll", "retry", "reconcile", "export"):
        sub = commands.add_parser(name)
        sub.add_argument("--manifest", required=True)
        sub.add_argument("--expected-sha256", required=True)
        if name in ("prepare", "status", "submit", "retry", "export"):
            sub.add_argument("--phase", choices=("all", "transcripts", "synthesis"), default="all",
                             help="limit this command to a phase; synthesis waits for all transcript summaries")
        if name in ("submit", "poll", "retry", "reconcile"):
            sub.add_argument("--wave", required=True)
        if name in ("submit", "poll", "reconcile"):
            sub.add_argument("--env-file", help="private API-key dotenv file; defaults to the repository .env")
        if name == "submit":
            sub.add_argument("--allow-paid-api", action="store_true")
        if name == "reconcile":
            sub.add_argument("--remote-id", required=True)
    args = parser.parse_args(argv)
    try:
        if args.command == "plan":
            value = create_plan(args.request, args.expected_sha256)
        else:
            common = (args.manifest, args.expected_sha256)
            if args.command == "prepare":
                value = prepare_plan(*common, phase=args.phase)
            elif args.command == "retry":
                value = prepare_plan(*common, retry_wave=args.wave, phase=args.phase)
            elif args.command == "submit":
                value = submit_wave(*common, args.wave, allow_paid_api=args.allow_paid_api, phase=args.phase,
                                    env_file=args.env_file)
            elif args.command == "poll":
                value = poll_wave(*common, args.wave, env_file=args.env_file)
            elif args.command == "reconcile":
                value = reconcile_wave(*common, args.wave, args.remote_id, env_file=args.env_file)
            elif args.command == "export":
                value = export_plan(*common, phase=args.phase)
            else:
                value = status_plan(*common, phase=args.phase)
        print(json.dumps(value, indent=2, sort_keys=True, allow_nan=False))
        return 2 if value.get("state") in {"needs_reconciliation", "submission_rejected"} else 0
    except KeyboardInterrupt:
        print("Summary command interrupted; durable submission intents and results preserved.", file=sys.stderr)
        return 130
    except (RuntimeError, OSError, ValueError, KeyError, TypeError) as error:
        # These admission/client errors contain controlled diagnostics, not raw
        # HTTP exception bodies. Unexpected errors and OS paths stay redacted.
        if isinstance(error, (Error, core.SummaryError, sources_module.SourceError,
                              client_module.BatchClientError, env_module.EnvFileError)):
            detail = str(error).replace("\n", " ")[:800]
        elif isinstance(error, OSError):
            detail = "local I/O failure (errno " + str(error.errno) + "); inspect private input paths and storage"
        else:
            detail = type(error).__name__ + "; inspect private plan/receipts before retrying"
        print("TranscriptSummaryError: " + detail, file=sys.stderr)
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
