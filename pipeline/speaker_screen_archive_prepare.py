"""Prepare and launch one finite, explicit full-archive fast-triage campaign.

Reads admitted acquisition metadata, deduplicates exact content, checks bounded
audio endpoints, and creates separate guided plans. No source deletion, complete
media hashing, transcript trust changes, ASR/controller writes or discovery.
"""
from __future__ import annotations

import argparse
from concurrent.futures import ProcessPoolExecutor, wait, FIRST_COMPLETED
from contextlib import contextmanager
import ctypes
from datetime import datetime, timezone
import errno
import fcntl
import hashlib
import json
import math
import multiprocessing
import os
from pathlib import Path
import signal
import stat
import sys
import time
import uuid

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))
from pipeline import speaker_screen as screen
from pipeline import speaker_screen_archive_guided as guided
from pipeline import speaker_screen_archive_inventory as inventory_api
from pipeline import speaker_screen_source_probe as source_api
from pipeline import speaker_screen_campaign as campaign
from pipeline import speaker_screen_paths as paths

ScreenError = screen.ScreenError
MARKER = {"kind": "himr_private_archive_screen_preparation", "schema_version": 1}
IMPLEMENTATION_NAMES = campaign.IMPLEMENTATION_NAMES + (
    "speaker_screen_archive_inventory.py", "speaker_screen_source_probe.py", "speaker_screen_archive_prepare.py")
MAX_INVENTORY_BYTES = 128 * 1024**2
FAST_POLICY = {"probe_ms": 10000, "stride_ms": 300000, "max_windows": 64}
SOURCE_TRUST = {"source_sha256_reverified": False, "full_source_hash": False,
    "admission_witness_checked_before_batch_sealing": True,
    "all_inventory_source_witnesses_checked_before_initial_launch": True,
    "initial_launch_check_is_not_continuous_monitoring": True,
    "immutable_cas_required_after_initial_launch": True,
    "runtime_retains_its_own_source_witness": True,
    "leading_interval_not_screened_ms": 100,
    "timestamps_remain_absolute_source_ms": True,
    "full_measured_eof_retained": True}


def _implementation():
    return {name: hashlib.sha256((Path(__file__).parent / name).read_bytes()).hexdigest()
            for name in IMPLEMENTATION_NAMES}


def _verify(expected):
    if expected != _implementation():
        raise ScreenError("archive preparation implementation changed")


def _binding(path):
    path = screen.path_value(path)
    with screen.opened(path) as descriptor:
        return {"path": str(path), "sha256": screen.hash_fd(descriptor, MAX_INVENTORY_BYTES, time.monotonic() + 30)}


def _mkdir(path):
    with paths.retained_directory(path.parent) as parent:
        try:
            os.mkdir(path.name, 0o700, dir_fd=parent)
            os.fsync(parent)
        except FileExistsError:
            pass
    with paths.retained_directory(path):
        pass


def _write_large_immutable(path, value):
    body = screen.canonical(value)
    if len(body) > MAX_INVENTORY_BYTES:
        raise ScreenError("archive inventory exceeds bounded metadata size")
    with paths.retained_directory(path.parent) as parent:
        if screen.exists(path):
            with screen.opened(path) as existing:
                if screen.hash_fd(existing, MAX_INVENTORY_BYTES, time.monotonic() + 30) != hashlib.sha256(body).hexdigest():
                    raise ScreenError("existing archive preparation document differs")
            return
        name = ".archive-screen-" + uuid.uuid4().hex
        descriptor = os.open(name, os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW, 0o600, dir_fd=parent)
        try:
            with os.fdopen(descriptor, "wb") as stream:
                os.fchmod(stream.fileno(), 0o400)
                stream.write(body)
                stream.flush()
                os.fsync(stream.fileno())
            # Match the reviewed immutable writer: no temporary hardlink to
            # leave st_nlink=2 if the process dies between publication/cleanup.
            library = ctypes.CDLL(None, use_errno=True)
            rename = getattr(library, "renameat2", None)
            if rename is None:
                raise ScreenError("Linux renameat2 is required for restart-safe inventory publication")
            rename.argtypes = [ctypes.c_int, ctypes.c_char_p, ctypes.c_int, ctypes.c_char_p, ctypes.c_uint]
            if rename(parent, os.fsencode(name), parent, os.fsencode(path.name), 1) != 0:
                error = ctypes.get_errno()
                if error != errno.EEXIST:
                    raise OSError(error, os.strerror(error))
                with screen.opened(path) as existing:
                    if screen.hash_fd(existing, MAX_INVENTORY_BYTES, time.monotonic() + 30) != hashlib.sha256(body).hexdigest():
                        raise ScreenError("existing archive preparation document differs")
            os.fsync(parent)
        finally:
            try:
                os.unlink(name, dir_fd=parent)
            except FileNotFoundError:
                pass


def _snapshot(root, value):
    body = screen.canonical({"kind": "himr_archive_screen_preparation_status", "schema_version": 1,
        "updated_at": datetime.now(timezone.utc).isoformat(), **value})
    if len(body) > screen.MAX_JSON:
        raise ScreenError("archive preparation status exceeds bound")
    with paths.retained_directory(root) as parent:
        name = ".progress-" + uuid.uuid4().hex
        fd = os.open(name, os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW, 0o600, dir_fd=parent)
        try:
            with os.fdopen(fd, "wb") as stream:
                stream.write(body)
                stream.flush()
                os.fsync(stream.fileno())
            os.replace(name, "preparation-status.json", src_dir_fd=parent, dst_dir_fd=parent)
            os.fsync(parent)
        finally:
            try:
                os.unlink(name, dir_fd=parent)
            except FileNotFoundError:
                pass


@contextmanager
def _locked(root):
    with paths.retained_directory(root) as directory:
        fd = os.open("preparation.lock", os.O_RDWR | os.O_CREAT | os.O_NOFOLLOW | os.O_CLOEXEC, 0o600, dir_fd=directory)
        try:
            info = os.fstat(fd)
            if not stat.S_ISREG(info.st_mode) or info.st_nlink != 1 or stat.S_IMODE(info.st_mode) != 0o600:
                raise ScreenError("unsafe archive preparation lock")
            try:
                fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
            except BlockingIOError:
                raise ScreenError("archive preparation is already running") from None
            yield
        finally:
            os.close(fd)


def validate_request(value):
    screen.exact(value, {"kind", "schema_version", "state_root", "checkpoints", "admissions", "ffmpeg", "ffprobe",
                         "models", "execution", "sampling", "recordings_per_batch", "probe_workers",
                         "probe_timeout_seconds", "max_prepare_seconds", "free_space_floor_bytes"}, "archive prepare request")
    if (value["kind"] != "himr_archive_speaker_screen_prepare_request"
            or type(value["schema_version"]) is not int or value["schema_version"] != 1):
        raise ScreenError("unsupported archive preparation request")
    if value["sampling"] != "fast-triage":
        raise ScreenError("this explicitly authorized archive campaign uses fast-triage only")
    root = screen.path_value(value["state_root"])
    for key in ("checkpoints", "admissions"):
        if not isinstance(value[key], list) or not 1 <= len(value[key]) <= 8:
            raise ScreenError("archive preparation requires 1..8 explicit " + key)
        for ref in value[key]:
            screen.file_binding(ref)
            if guided.old_batch._overlap(root, Path(ref["path"])):
                raise ScreenError("preparation workspace overlaps its inventory input")
    for key in ("ffmpeg", "ffprobe"):
        screen.file_binding(value[key])
    screen.engine.validate_model_config(value["models"])
    for ref in (value["ffmpeg"], value["ffprobe"], value["models"]["silero_vad"], value["models"]["ecapa_embedding"]):
        if guided.old_batch._overlap(root, Path(ref["path"])):
            raise ScreenError("preparation workspace overlaps a model or tool")
    for key, low, high in (("recordings_per_batch", 32, 128), ("probe_workers", 1, 2),
                           ("probe_timeout_seconds", 10, 120), ("max_prepare_seconds", 60, 28800),
                           ("free_space_floor_bytes", 1024**3, 64 * 1024**3)):
        screen.integer(value[key], low, high, key)
    execution = guided.validate_execution(value["execution"])
    if execution["device"] != "cuda":
        raise ScreenError("full-archive launch requires the explicitly selected CUDA device")
    return {**value, "execution": execution}


def _admitted_timeline(probe):
    duration = probe.get("duration_ms")
    screen.integer(duration, 101, 86400000, "admitted source duration")
    timeline = probe.get("timeline")
    if (not isinstance(timeline, dict)
            or type(timeline.get("screenable_start_ms")) is not int or timeline["screenable_start_ms"] != 100
            or type(timeline.get("leading_interval_not_admitted_ms")) is not int
            or timeline["leading_interval_not_admitted_ms"] != 100
            or type(timeline.get("measured_eof_ms")) is not int or timeline["measured_eof_ms"] != duration):
        raise ScreenError("source admission does not match the explicit 100 ms archive recipe")
    checks = probe.get("checks")
    if not isinstance(checks, dict):
        raise ScreenError("source admission lacks exact archive endpoint checks")
    for key, start, end in (("first_probe", 100, min(duration, 10100)),
                            ("last_probe", max(100, duration - 10000), duration)):
        row = checks.get(key)
        if (not isinstance(row, dict) or type(row.get("start_ms")) is not int or row["start_ms"] != start
                or type(row.get("end_ms")) is not int or row["end_ms"] != end
                or type(row.get("pcm_bytes")) is not int or row["pcm_bytes"] != (end - start) * 32
                or row.get("exact_pcm_length") is not True):
            raise ScreenError("source admission endpoint check differs from the archive recipe")
    return duration


def make_order(record, probe, request, root):
    if probe["status"] != "admitted" or probe["recording"] != record["recording"]:
        raise ScreenError("source probe did not admit this exact recording")
    duration = _admitted_timeline(probe)
    identity = {key: record["recording"][key] for key in ("media_id", "path", "sha256", "byte_count")}
    order = {"kind": "himr_cpu_speaker_screen_work_order", "schema_version": 1,
        "recording": {**identity, "duration_ms": duration}, "ffmpeg": request["ffmpeg"],
        "models": request["models"], "policy": dict(FAST_POLICY),
        "resources": {"threads": 1, "window_timeout_seconds": 30, "max_run_seconds": 3600,
                      "max_windows_per_run": 64, "early_stop_on_positive": False},
        "source_verification": "metadata_witness", "output_root": str(root / "unused-order-outputs" / identity["media_id"])}
    return screen.validate_order(order)


def _probe_worker(recording, ffmpeg, ffprobe, timeout):
    # Process isolation (not threads) keeps FFmpeg's preexec resource sandbox safe.
    return source_api.probe_source(recording, ffmpeg, ffprobe, timeout=timeout)


def _cached_probe(path, record, implementation, *, expected_sha256=None):
    if not screen.exists(path):
        return None
    value = screen.read_json(path, expected_sha256)
    screen.exact(value, {"kind", "schema_version", "recording", "implementation", "probe"}, "source admission receipt")
    if (value["kind"] != "himr_archive_screen_source_admission"
            or type(value["schema_version"]) is not int or value["schema_version"] != 1):
        raise ScreenError("invalid source admission receipt kind/version")
    if value.get("implementation") != implementation or value.get("recording") != record["recording"]:
        raise ScreenError("saved source admission does not match this preparation")
    probe = value.get("probe")
    if not isinstance(probe, dict) or probe.get("recording") != record["recording"]:
        raise ScreenError("malformed source admission receipt")
    if (probe.get("kind") != "himr_speaker_screen_source_probe" or type(probe.get("schema_version")) is not int
            or probe["schema_version"] != 1 or probe.get("status") not in ("admitted", "needs_review")):
        raise ScreenError("invalid source probe kind/version/status")
    if probe["status"] == "admitted":
        _admitted_timeline(probe)
        if probe.get("error") is not None:
            raise ScreenError("admitted source probe cannot contain an error")
    elif probe.get("duration_ms") is not None or not isinstance(probe.get("error"), dict):
        raise ScreenError("review-required source probe must have a reason and no admitted duration")
    else:
        screen.exact(probe["error"], {"code", "message"}, "source probe error")
        if any(not isinstance(probe["error"][key], str) or not probe["error"][key] for key in ("code", "message")):
            raise ScreenError("invalid source probe review reason")
    if not isinstance(probe.get("source_witness"), dict):
        raise ScreenError("source probe has no retained source witness")
    with screen.opened(record["recording"]["path"]) as source:
        if screen.witness(source) != probe["source_witness"]:
            raise ScreenError("source changed after endpoint admission")
    return probe


def _source_guard(root, inventory, records, implementation):
    """Seal receipt hashes as well as inventory membership; never read media bytes."""
    receipts = []
    for record in records:
        media_id = record["recording"]["media_id"]
        path = root / "source-probes" / (media_id + ".json")
        binding = _binding(path)
        probe = _cached_probe(path, record, implementation, expected_sha256=binding["sha256"])
        if probe is None:
            raise ScreenError("source guard requires a completed endpoint receipt for every input")
        receipts.append({"media_id": media_id, "status": probe["status"], "receipt": binding})
    inventory_binding = _binding(root / "inventory.json")
    # Bind exactly the inventory consumed during this invocation, not another
    # well-formed inventory substituted at its pathname before publication.
    if inventory_binding["sha256"] != screen.digest(inventory):
        raise ScreenError("archive inventory changed before source guard publication")
    value = {"kind": "himr_archive_screen_source_guard", "schema_version": 1,
             "inventory": inventory_binding, "implementation": implementation,
             "receipts": receipts, "semantics": dict(SOURCE_TRUST)}
    screen.write_immutable(root / "source-guard.json", value)
    return _binding(root / "source-guard.json")


def _verify_source_guard(root, binding, implementation):
    """Recheck a ready campaign's exact inventory and endpoint witnesses."""
    screen.file_binding(binding)
    if binding["path"] != str(root / "source-guard.json"):
        raise ScreenError("archive source guard is outside its preparation workspace")
    value = screen.read_json(root / "source-guard.json", binding["sha256"])
    screen.exact(value, {"kind", "schema_version", "inventory", "implementation", "receipts", "semantics"},
                 "archive source guard")
    if (value["kind"] != "himr_archive_screen_source_guard" or type(value["schema_version"]) is not int
            or value["schema_version"] != 1 or value["implementation"] != implementation
            or value["semantics"] != SOURCE_TRUST):
        raise ScreenError("archive source guard kind, implementation, or trust contract differs")
    screen.file_binding(value["inventory"])
    if value["inventory"]["path"] != str(root / "inventory.json"):
        raise ScreenError("source guard inventory is outside its preparation workspace")
    inventory, _reference = inventory_api._Reader().read(value["inventory"], maximum=MAX_INVENTORY_BYTES)
    if (inventory.get("kind") != "himr_private_speaker_screen_archive_inventory"
            or type(inventory.get("schema_version")) is not int or inventory["schema_version"] != 1
            or not isinstance(inventory.get("records"), list) or not 1 <= len(inventory["records"]) <= 128 * 128):
        raise ScreenError("source guard inventory has an invalid kind, version, or recording count")
    records = inventory["records"]
    if not isinstance(value["receipts"], list) or len(value["receipts"]) != len(records):
        raise ScreenError("source guard does not cover the exact inventory")
    seen, admitted = set(), 0
    for record, row in zip(records, value["receipts"]):
        if not isinstance(record, dict) or not isinstance(record.get("recording"), dict):
            raise ScreenError("source guard inventory record is malformed")
        media_id = record["recording"].get("media_id")
        if not isinstance(media_id, str) or not screen.IDENTIFIER.fullmatch(media_id) or media_id in seen:
            raise ScreenError("source guard inventory repeats or has an invalid recording identity")
        seen.add(media_id)
        screen.exact(row, {"media_id", "status", "receipt"}, "source guard receipt")
        screen.file_binding(row["receipt"])
        path = root / "source-probes" / (media_id + ".json")
        if row["media_id"] != media_id or row["receipt"]["path"] != str(path):
            raise ScreenError("source guard receipt belongs to another input")
        probe = _cached_probe(path, record, implementation, expected_sha256=row["receipt"]["sha256"])
        if probe is None or row["status"] != probe["status"]:
            raise ScreenError("source guard receipt disposition differs")
        admitted += probe["status"] == "admitted"
    return {"unique_files": len(records), "admitted_unique_files": admitted,
            "excluded_unique_files": len(records) - admitted}


def _verify_batch_admissions(root, selected, expected_probes, implementation):
    for record, _order_reference in selected:
        media_id = record["recording"]["media_id"]
        probe = _cached_probe(root / "source-probes" / (media_id + ".json"), record, implementation)
        if probe is None or probe["status"] != "admitted" or probe != expected_probes[media_id]:
            raise ScreenError("source admission changed before guided batch sealing")


def prepare(request_path, expected_sha256):
    binding = {"path": str(screen.path_value(request_path)), "sha256": expected_sha256}
    screen.file_binding(binding)
    request = validate_request(screen.read_json(Path(binding["path"]), expected_sha256))
    root = Path(request["state_root"])
    if guided.old_batch._overlap(root, Path(binding["path"])):
        raise ScreenError("preparation request must be outside its new workspace")
    implementation = _implementation()
    for name in IMPLEMENTATION_NAMES:
        if guided.old_batch._overlap(root, Path(__file__).parent / name):
            raise ScreenError("preparation workspace overlaps implementation")
    runtime = guided._runtime_binding(request["execution"])
    _mkdir(root)
    with paths.retained_directory(root) as directory:
        if not screen.exists(root / "workspace.json"):
            if list(os.scandir(directory)):
                raise ScreenError("refusing unmarked nonempty archive preparation workspace")
            screen.write_immutable(root / "workspace.json", MARKER)
        if screen.read_json(root / "workspace.json") != MARKER:
            raise ScreenError("archive preparation marker differs")
    with _locked(root):
        started = time.monotonic()
        deadline = started + request["max_prepare_seconds"]
        screen.write_immutable(root / "preparation-plan.json", {"kind": "himr_archive_screen_preparation_plan",
            "schema_version": 1, "request": binding, "request_value": request, "implementation": implementation,
            "runtime_binding": runtime})
        if screen.exists(root / "preparation-result.json"):
            try:
                previous = screen.read_json(root / "preparation-result.json")
                if (previous.get("kind") != "himr_archive_screen_preparation_result"
                        or type(previous.get("schema_version")) is not int or previous["schema_version"] != 1
                        or previous.get("state") != "ready" or previous.get("sampling") != "fast-triage"
                        or not isinstance(previous.get("campaign"), dict)
                        or previous["campaign"].get("path") != str(root / "campaign" / "manifest.json")
                        or not isinstance(previous.get("source_guard"), dict)
                        or previous.get("source_trust") != SOURCE_TRUST):
                    raise ScreenError("saved archive preparation result differs")
                screen.file_binding(previous["campaign"])
                campaign._load_manifest(previous["campaign"]["path"], previous["campaign"]["sha256"])
                guarded = _verify_source_guard(root, previous["source_guard"], implementation)
                if any(previous.get(key) != guarded[key] for key in ("admitted_unique_files", "excluded_unique_files")):
                    raise ScreenError("saved archive preparation result differs from guarded source counts")
                _verify(implementation)
                outcome = {key: item for key, item in previous.items() if key not in ("kind", "schema_version")}
                _snapshot(root, outcome)
                return outcome
            except BaseException as error:
                _snapshot(root, {"state": "interrupted" if isinstance(error, KeyboardInterrupt) else "failed",
                                 "sampling": "fast-triage", "error": f"{type(error).__name__}: {str(error)[:1000]}"})
                raise
        _snapshot(root, {"state": "inventory", "sampling": "fast-triage"})
        try:
            inventory = inventory_api.inventory(request["checkpoints"], request["admissions"], verify_results=True)
            records = inventory["records"]
            for record in records:
                if guided.old_batch._overlap(root, Path(record["recording"]["path"])):
                    raise ScreenError("archive preparation overlaps a media input")
            if len(records) > 128 * request["recordings_per_batch"]:
                raise ScreenError("inventory exceeds the finite 128-batch campaign bound")
            _verify(implementation)
            _write_large_immutable(root / "inventory.json", inventory)
            for name in ("source-probes", "orders", "batches"):
                _mkdir(root / name)
            probes = {}
            for index, record in enumerate(records):
                saved = _cached_probe(root / "source-probes" / (record["recording"]["media_id"] + ".json"), record, implementation)
                if saved is not None:
                    probes[index] = saved
            pending = iter(index for index in range(len(records)) if index not in probes)
            active = {}
            executor = ProcessPoolExecutor(max_workers=request["probe_workers"], mp_context=multiprocessing.get_context("spawn"))
            last_snapshot = 0.0
            try:
                while True:
                    if time.monotonic() > deadline:
                        raise ScreenError("bounded archive preparation time limit reached; endpoint receipts preserved")
                    if os.statvfs(root).f_bavail * os.statvfs(root).f_frsize < request["free_space_floor_bytes"]:
                        raise ScreenError("hot storage free-space floor reached")
                    while len(active) < request["probe_workers"]:
                        index = next(pending, None)
                        if index is None:
                            break
                        future = executor.submit(_probe_worker, records[index]["recording"], request["ffmpeg"],
                                                 request["ffprobe"], request["probe_timeout_seconds"])
                        active[future] = index
                    if time.monotonic() - last_snapshot >= 10 or not active:
                        _snapshot(root, {"state": "checking_audio_endpoints", "sampling": "fast-triage",
                            "inventory_counts": inventory["counts"], "unique_files": len(records),
                            "checked_files": len(probes), "admitted_files": sum(p["status"] == "admitted" for p in probes.values()),
                            "needs_review_files": sum(p["status"] != "admitted" for p in probes.values()),
                            "active_files": len(active), "elapsed_seconds": time.monotonic() - started})
                        last_snapshot = time.monotonic()
                    if not active:
                        break
                    done, _ = wait(active, timeout=1, return_when=FIRST_COMPLETED)
                    for future in done:
                        index = active.pop(future)
                        probe = future.result()
                        _verify(implementation)
                        record = records[index]
                        screen.write_immutable(root / "source-probes" / (record["recording"]["media_id"] + ".json"), {
                            "kind": "himr_archive_screen_source_admission", "schema_version": 1,
                            "recording": record["recording"], "implementation": implementation, "probe": probe})
                        probes[index] = probe
            finally:
                for future in active:
                    future.cancel()
                executor.shutdown(wait=True, cancel_futures=True)
            admitted, excluded = [], []
            for index, record in enumerate(records):
                probe = probes[index]
                if probe["status"] == "admitted":
                    order = make_order(record, probe, request, root)
                    order_path = root / "orders" / (record["recording"]["media_id"] + ".json")
                    screen.write_immutable(order_path, order)
                    admitted.append((record, _binding(order_path)))
                else:
                    error = probe.get("error") or {"code": "unknown_probe_failure"}
                    excluded.append({"media_id": record["recording"]["media_id"], "aliases": record["aliases"],
                                     "reason": error, "disposition": "no_audio" if error["code"] == "no_audio_stream" else "needs_review"})
            screen.write_immutable(root / "excluded-inputs.json", {"kind": "himr_archive_screen_exclusions",
                "schema_version": 1, "records": excluded, "excluded_is_not_single_speaker": True})
            if not admitted:
                raise ScreenError("no exact audio timelines were admitted")
            source_guard = _source_guard(root, inventory, records, implementation)
            expected_probes = {records[index]["recording"]["media_id"]: probe for index, probe in probes.items()}
            batches = []
            for offset in range(0, len(admitted), request["recordings_per_batch"]):
                number = len(batches) + 1
                selected = admitted[offset:offset + request["recordings_per_batch"]]
                folder = root / "batches" / f"batch-{number:04d}"
                _mkdir(folder)
                metadata = {"kind": "himr_speaker_screen_guidance", "schema_version": 1, "events": [], "records": [
                    {"media_id": row["recording"]["media_id"], "media_sha256": row["recording"]["sha256"],
                     "acquisition_result": row["acquisition_result"], "timed_cues": None} for row, _ref in selected]}
                screen.write_immutable(folder / "guidance.json", metadata)
                batch_request = {"kind": "himr_archive_guided_speaker_screen_request", "schema_version": 1,
                    "work_orders": [ref for _row, ref in selected], "guidance": _binding(folder / "guidance.json"),
                    "state_root": str(folder / "state"), "execution": request["execution"], "max_target_windows": 0}
                screen.write_immutable(folder / "request.json", batch_request)
                request_ref = _binding(folder / "request.json")
                _verify(implementation)
                _verify_batch_admissions(root, selected, expected_probes, implementation)
                guided.seal_manifest(request_ref["path"], request_ref["sha256"], str(folder / "state" / "manifest.json"))
                batches.append(_binding(folder / "state" / "manifest.json"))
                _snapshot(root, {"state": "sealing_batches", "sampling": "fast-triage", "batches_prepared": len(batches),
                    "admitted_unique_files": len(admitted), "excluded_unique_files": len(excluded), "inventory_counts": inventory["counts"]})
            campaign_request = {"kind": "himr_guided_speaker_screen_campaign_request", "schema_version": 1,
                "campaign_root": str(root / "campaign"), "python": runtime["python"], "batches": batches,
                "max_passes_per_batch": 8, "max_run_seconds": 604800}
            screen.write_immutable(root / "campaign-request.json", campaign_request)
            campaign_request_ref = _binding(root / "campaign-request.json")
            _verify(implementation)
            manifest = campaign.create_campaign(campaign_request_ref["path"], campaign_request_ref["sha256"],
                                                 str(root / "campaign" / "manifest.json"))
            campaign_ref = _binding(root / "campaign" / "manifest.json")
            _verify_source_guard(root, source_guard, implementation)
            outcome = {"state": "ready", "sampling": "fast-triage", "campaign": campaign_ref,
                "source_guard": source_guard, "source_trust": dict(SOURCE_TRUST),
                "leading_interval_not_screened_per_admitted_file_ms": 100,
                "total_leading_interval_not_screened_ms": 100 * len(admitted),
                "campaign_id": manifest["campaign_id"], "batches": len(batches), "inventory_counts": inventory["counts"],
                "admitted_unique_files": len(admitted), "admitted_source_aliases": sum(len(row["aliases"]) for row, _ref in admitted),
                "excluded_unique_files": len(excluded), "no_audio_unique_files": sum(r["disposition"] == "no_audio" for r in excluded),
                "needs_review_unique_files": sum(r["disposition"] == "needs_review" for r in excluded),
                "elapsed_seconds": time.monotonic() - started}
            screen.write_immutable(root / "preparation-result.json", {"kind": "himr_archive_screen_preparation_result", "schema_version": 1, **outcome})
            _snapshot(root, outcome)
            return outcome
        except BaseException as error:
            _snapshot(root, {"state": "interrupted" if isinstance(error, KeyboardInterrupt) else "failed",
                             "sampling": "fast-triage", "error": f"{type(error).__name__}: {str(error)[:1000]}"})
            raise


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("command", choices=("prepare", "launch"))
    parser.add_argument("--request", required=True)
    parser.add_argument("--expected-sha256", required=True)
    args = parser.parse_args(argv)
    def stop(_signum, _frame):
        raise KeyboardInterrupt()
    signal.signal(signal.SIGTERM, stop)
    try:
        result = prepare(args.request, args.expected_sha256)
        print(json.dumps(result, indent=2, sort_keys=True), flush=True)
        if args.command == "launch":
            ref = result["campaign"]
            os.execv(sys.executable, [sys.executable, "-B", str(Path(campaign.__file__).resolve()), "run",
                                    "--manifest", ref["path"], "--expected-sha256", ref["sha256"]])
        return 0
    except KeyboardInterrupt:
        print("Archive screen preparation interrupted; completed endpoint receipts preserved.", file=sys.stderr)
        return 130
    except (ScreenError, OSError, ValueError, RuntimeError) as error:
        print(f"ArchiveSpeakerScreenError: {error}", file=sys.stderr)
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
