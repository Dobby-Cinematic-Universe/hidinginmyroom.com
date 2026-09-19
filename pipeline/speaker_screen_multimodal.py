#!/usr/bin/env python3
"""Separate, resumable, private audio/visual speaker-screening pilot.

prepare is metadata-only. run uses two sequential, network-denied CPU workers:
one resident YuNet detector, then one resident Silero/ECAPA pair. No old workspaces,
transcripts, services, identities, or publication decisions are modified.
"""
from __future__ import annotations

import argparse
from contextlib import contextmanager
import fcntl
import hashlib
import json
import os
from pathlib import Path
import signal
import subprocess
import sys
import time

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from pipeline import speaker_screen as safe
from pipeline import speaker_screen_engine as speech
from pipeline import speaker_screen_archive_guided_core as legacy_core
from pipeline import speaker_screen_multimodal_core as core
from pipeline import speaker_screen_multimodal_engine as engine

KIND = "himr_multimodal_speaker_triage"
FILES = ("speaker_screen_multimodal.py", "speaker_screen_multimodal_core.py",
         "speaker_screen_multimodal_engine.py", "speaker_screen.py", "speaker_screen_core.py",
         "speaker_screen_engine.py", "speaker_screen_paths.py", "speaker_screen_guided_core.py",
         "speaker_screen_archive_guided_core.py", "yunet_face_detector.py")


def implementation():
    return {name: hashlib.sha256((ROOT / "pipeline" / name).read_bytes()).hexdigest() for name in FILES}


def bind(path, maximum=128 * 1024**2):
    path = safe.path_value(path)
    with safe.opened(path) as fd:
        before = safe.witness(fd)
        if not 0 < before["st_size"] <= maximum:
            raise core.TriageError("binding file exceeds bound")
        digest = hashlib.sha256()
        while block := os.read(fd, 1024**2):
            digest.update(block)
        if safe.witness(fd) != before:
            raise core.TriageError("binding file changed")
    return {"path": str(path), "sha256": digest.hexdigest()}


def read(ref):
    safe.file_binding(ref)
    return safe.read_json(Path(ref["path"]), ref["sha256"])


def mkdir(path):
    with safe.paths.retained_directory(path.parent) as fd:
        try:
            os.mkdir(path.name, 0o700, dir_fd=fd)
        except FileExistsError:
            pass
    with safe.paths.retained_directory(path):
        pass


def write(path, value):
    safe.write_immutable(path, value)
    return bind(path)


def source_witness(recording):
    with safe.opened(recording["path"]) as fd:
        value = safe.witness(fd)
    if value["st_size"] != recording["byte_count"]:
        raise core.TriageError("media size differs from inventory")
    return value


def cached_evidence(path, recording, witness, models):
    """Recheck the old result against its pinned plan and checkpoint hashes.

    No legacy label is changed. Retain only a bounded spread of its valid vectors,
    with source identity and proof bindings; never associate voices across videos.
    """
    if path is None:
        return {"excerpts": [], "proofs": [], "previous_status": None, "original_usable_excerpts": 0,
                "speech_resample_target_ms": []}
    result_ref = bind(path); value = read(result_ref)
    if value.get("kind") != "himr_archive_guided_speaker_screen_result" or value.get("state") != "completed":
        raise core.TriageError("only completed archive screen results can be reused")
    if any(value["recording"][key] != recording[key] for key in ("path", "media_id", "sha256", "byte_count")):
        raise core.TriageError("cached audio belongs to different media")
    if value["source_binding"]["source_witness"] != witness:
        raise core.TriageError("cached audio source changed; use a fresh selection")
    if value["runtime"]["models"] != models:
        raise core.TriageError("cached voice model differs from fresh voice model")
    folder = path.parent
    plan_ref = bind(folder / "plan.json"); plan = read(plan_ref)
    plan_body = {key: val for key, val in plan.items() if key != "plan_id"}
    if (plan["plan_id"] != value["plan_id"] or
            plan["plan_id"] != "archivescreen_" + safe.digest(plan_body)[:32] or
            plan["order"]["recording"] != value["recording"]):
        raise core.TriageError("cached plan identity differs")
    observations, proofs = [], [result_ref, plan_ref]
    if len(value["checkpoint_hashes"]) != len(plan["batches"]):
        raise core.TriageError("cached sampling is incomplete")
    for i, proof in enumerate(value["checkpoint_hashes"]):
        checkpoint_ref = bind(folder / f"batch-{i:04d}.json")
        checkpoint = read(checkpoint_ref)
        if (proof != {"batch_index": i, "sha256": safe.digest(checkpoint)} or
                checkpoint["plan_id"] != plan["plan_id"] or checkpoint["batch_index"] != i or
                checkpoint["binding_sha256"] != safe.digest(value["source_binding"]) or
                checkpoint["runtime"] != value["runtime"] or
                [row["window"] for row in checkpoint["window_results"]] != plan["batches"][i]):
            raise core.TriageError("cached checkpoint integrity differs")
        observations += [row["observation"] for row in checkpoint["window_results"]]
        proofs.append(checkpoint_ref)
    replay = legacy_core.summarize(value["recording"]["duration_ms"], plan["windows"], observations,
                                   plan["order"]["policy"], sampling=plan["sampling"])
    if replay != value["summary"]:
        raise core.TriageError("cached audio summary does not replay")
    rows = [{"id": f"cached-{row['index']}", "probe_id": f"cached-{row['index']}",
             **{key: row[key] for key in ("start_ms", "end_ms", "speech_ms", "embedding")}}
            for row in observations if row["embedding"] is not None]
    count = len(rows)
    if count > 64:
        rows = [rows[i * (count - 1) // 63] for i in range(64)]
    core.validate_excerpts(rows)
    # Old VAD-positive probes that failed its strict excerpt selection are useful
    # places to listen again. Spread a bounded four cues over the recording.
    recovery = [(r["start_ms"] + r["end_ms"]) // 2 for r in observations
                if r["embedding"] is None and r["speech_ms"] >= 2000]
    if len(recovery) > 4:
        recovery = [recovery[i * (len(recovery) - 1) // 3] for i in range(4)]
    return {"excerpts": rows, "proofs": proofs, "previous_status": replay["status"],
            "original_usable_excerpts": count, "coverage": replay["coverage"],
            "speech_resample_target_ms": recovery}


def _overlap(left, right):
    return left == right or left in right.parents or right in left.parents


def prepare(inventory_path, expected, old_root, asset_path, models_path, audio_python, root,
            *, media_ids=(), limit=16, max_seconds=1800, audio_refresh="priority"):
    core.integer(limit, 1, 4096, "recording limit")
    core.integer(max_seconds, 60, 86400, "runtime limit")
    if audio_refresh not in {"priority", "all", "none"}:
        raise core.TriageError("invalid audio refresh mode")
    inventory_ref = {"path": str(safe.path_value(inventory_path)), "sha256": expected}
    inventory = read(inventory_ref)
    if inventory.get("kind") != "himr_private_speaker_screen_archive_inventory" or inventory.get("schema_version") != 1:
        raise core.TriageError("unsupported archive inventory")
    records = inventory["records"]
    if len(records) > 8192:
        raise core.TriageError("inventory exceeds bound")
    if media_ids:
        if len(set(media_ids)) != len(media_ids):
            raise core.TriageError("duplicate selected media identity")
        by_id = {r["recording"]["media_id"]: r for r in records}
        if any(identity not in by_id for identity in media_ids):
            raise core.TriageError("unknown selected media identity")
        records = [by_id[identity] for identity in media_ids]
        if len(records) > limit:
            raise core.TriageError("explicit selection exceeds limit")
    else:
        records = records[:limit]
    if not records:
        raise core.TriageError("empty recording selection")
    root, old_root = safe.path_value(root), safe.path_value(old_root)
    assets, models = bind(asset_path), bind(models_path)
    asset = read(assets); model_config = speech.validate_model_config(read(models))
    launch = safe.path_value(audio_python)
    python_ref = bind(launch.resolve(strict=True))
    pyconfig = bind(launch.parent.parent / "pyvenv.cfg")
    protected = [Path(inventory_path), old_root, Path(asset_path).parent, Path(models_path).parent,
                 launch.parent.parent, ROOT / "pipeline"] + [Path(r["recording"]["path"]) for r in records]
    if any(_overlap(root, item) for item in protected) or safe.exists(root):
        raise core.TriageError("new workspace must be disjoint and not already exist")
    # Only metadata of the existing completed-result tree is inventoried. No raw
    # media enumeration, rehash or acquisition discovery is performed here.
    wanted = {r["recording"]["media_id"] for r in records}
    old = {}
    for path in sorted(old_root.glob("batches/batch-*/state/*/result.json")):
        value = safe.read_json(path)
        identity = value.get("recording", {}).get("media_id")
        if identity in wanted:
            if identity in old:
                raise core.TriageError("duplicate cached recording")
            old[identity] = path
    mkdir(root); mkdir(root / "inputs"); mkdir(root / "jobs")
    jobs = []
    for record in records:
        recording = dict(record["recording"])
        identity = recording["media_id"]
        if identity != "media_sha256_" + recording["sha256"] or not safe.SHA.fullmatch(recording["sha256"]):
            raise core.TriageError("inventory content identity differs")
        core.integer(recording["duration_hint_ms"], 1, core.MAX_DURATION_MS, "duration hint")
        acquisition = read(record["acquisition_result"])
        if acquisition.get("status") != "completed" or acquisition.get("errors"):
            raise core.TriageError("inventory acquisition is not completed")
        if any(acquisition.get("admission", {}).get(key) != recording[key]
               for key in ("media_id", "path", "sha256", "byte_count")):
            raise core.TriageError("inventory media differs from acquisition receipt")
        witness = source_witness(recording)
        cached = cached_evidence(old.get(identity), recording, witness, model_config)
        job = {"recording": recording, "source_witness": witness,
               "title": record["aliases"][0]["title"], "acquisition": record["acquisition_result"],
               "cached_audio": cached, "cached_diversity": core.audio_diversity(cached["excerpts"])}
        job_id = "avtriage_" + safe.digest(job)[:32]
        ref = write(root / "inputs" / f"{job_id}.json", job)
        jobs.append({"job_id": job_id, "input": ref})
    body = {"kind": KIND + "_manifest", "schema_version": 1, "state_root": str(root),
            "inventory": inventory_ref, "jobs": jobs, "policy": core.POLICY,
            "implementation": implementation(), "face_assets": assets, "audio_models": models,
            "audio_python": {"launcher": str(launch), "executable": python_ref, "configuration": pyconfig},
            "ffmpeg": bind(Path("/usr/bin/ffmpeg").resolve()), "ffprobe": bind(Path("/usr/bin/ffprobe").resolve()),
            "max_runtime_seconds": max_seconds, "audio_refresh": audio_refresh, "semantics": core.SEMANTICS}
    manifest = {**body, "plan_id": "avcampaign_" + safe.digest(body)[:32]}
    ref = write(root / "manifest.json", manifest)
    return {"state": "prepared_offline", "manifest": ref, "recordings": len(jobs),
            "reused_audio_recordings": sum(bool(read(j["input"])["cached_audio"]["proofs"]) for j in jobs),
            "paid_requests": 0, "media_decodes": 0}


def load_manifest(path, expected):
    manifest = read({"path": str(safe.path_value(path)), "sha256": expected})
    if (manifest.get("kind") != KIND + "_manifest" or manifest.get("schema_version") != 1 or
            manifest.get("implementation") != implementation() or manifest.get("policy") != core.POLICY or
            manifest.get("semantics") != core.SEMANTICS):
        raise core.TriageError("triage implementation or policy changed; new plan required")
    body = {k: v for k, v in manifest.items() if k != "plan_id"}
    if manifest["plan_id"] != "avcampaign_" + safe.digest(body)[:32]:
        raise core.TriageError("triage manifest identity differs")
    if Path(path) != Path(manifest["state_root"]) / "manifest.json":
        raise core.TriageError("triage manifest location differs")
    if not isinstance(manifest["jobs"], list) or not 1 <= len(manifest["jobs"]) <= 4096:
        raise core.TriageError("triage selection exceeds bound")
    return manifest


def load_job(row):
    job = read(row["input"])
    if row["job_id"] != "avtriage_" + safe.digest(job)[:32]:
        raise core.TriageError("triage job identity differs")
    return job


@contextmanager
def locked(root):
    with safe.paths.retained_directory(root) as directory:
        fd = os.open("triage.lock", os.O_RDWR | os.O_CREAT | os.O_NOFOLLOW, 0o600, dir_fd=directory)
        try:
            fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
            yield
        finally:
            os.close(fd)


def outcome(path, manifest, row, phase):
    if not safe.exists(path):
        return None
    value = safe.read_json(path)
    if value.get("plan_id") != manifest["plan_id"] or value.get("job_id") != row["job_id"] or value.get("phase") != phase:
        raise core.TriageError("triage checkpoint belongs to another plan/job/phase")
    return value


def commit(path, body, manifest, row, phase):
    if implementation() != manifest["implementation"]:
        raise core.TriageError("triage implementation changed while running")
    job = load_job(row)
    if source_witness(job["recording"]) != job["source_witness"]:
        raise core.TriageError("source changed during triage")
    write(path, {"plan_id": manifest["plan_id"], "job_id": row["job_id"], "phase": phase, **body})


def validate_audio_checkpoint(value, window):
    if value.get("window") != window or value.get("state") not in {"analyzed", "needs_review"}:
        raise core.TriageError("targeted audio plan differs from checkpoint")
    core.validate_excerpts(value.get("excerpts"))
    if value["state"] == "needs_review":
        if value["excerpts"]:
            raise core.TriageError("failed audio sample contains voice evidence")
        return
    receipt = value["receipt"]
    if (receipt.get("requested_start_ms") != window["start_ms"] or receipt.get("requested_end_ms") != window["end_ms"] or
            receipt.get("source_pts_verified") is not True or receipt.get("silence_padding") is not False or
            not isinstance(receipt.get("pcm_sha256"), str) or not safe.SHA.fullmatch(receipt["pcm_sha256"])):
        raise core.TriageError("audio receipt differs from requested source interval")
    start, end = receipt["start_ms"], receipt["end_ms"]
    core.interval(start, end)
    if not window["start_ms"] <= start < end <= window["end_ms"] or receipt["short_sample"] != (start != window["start_ms"] or end != window["end_ms"]):
        raise core.TriageError("audio receipt source timing differs")
    decoded = core.integer(receipt.get("decoded_samples"), 1, (window["end_ms"]-window["start_ms"]+100)*16, "decoded samples")
    discarded = core.integer(receipt.get("discarded_samples"), 0, decoded, "discarded samples")
    core.integer(receipt.get("timestamp_discontinuities"), 0, 4096, "timestamp discontinuities")
    if decoded-discarded != (end-start)*16 or receipt.get("waveform_retimed") is not False:
        raise core.TriageError("audio receipt waveform accounting differs")
    previous = None
    for i, excerpt in enumerate(value["excerpts"]):
        if (excerpt["probe_id"] != f"fresh-{window['index']}" or excerpt["id"] != f"fresh-{window['index']}-{i}" or
                not start <= excerpt["start_ms"] < excerpt["end_ms"] <= end or
                previous is not None and excerpt["start_ms"] < previous):
            raise core.TriageError("voice excerpts escape their decoded source interval")
        previous = excerpt["end_ms"]
    if len(value["excerpts"]) > core.POLICY["max_excerpts_per_window"]:
        raise core.TriageError("too many voice excerpts per window")
    core.integer(value.get("legacy_eligible_excerpts"), 0, 1, "legacy matched-probe eligibility")
    core.integer(value.get("vad_positive_ms"), 0, end-start, "VAD coverage")


def visual_job(manifest, row, runtime, deadline, source_fd, ffmpeg_fd, ffprobe_fd):
    job = load_job(row); folder = Path(manifest["state_root"]) / "jobs" / row["job_id"]
    if outcome(folder / "visual.json", manifest, row, "visual"):
        return True
    metadata = outcome(folder / "probe.json", manifest, row, "probe")
    if metadata is None:
        try:
            media = engine.probe(source_fd, ffprobe_fd, job["recording"]["duration_hint_ms"])
        except engine.DecodeReview as error:
            media = {name: {"state": "unsupported_timing", "span": None, "stream_index": None}
                     for name in ("audio", "video")}
            media["error"] = str(error)
        commit(folder / "probe.json", {"media": media}, manifest, row, "probe")
        metadata = outcome(folder / "probe.json", manifest, row, "probe")
    video = metadata["media"]["video"]
    state = "no_video" if video["state"] == "absent" else video["state"]
    frames = []
    if state == "available":
        span = video["span"]
        targets = engine.core.frame_times(span["start_ms"], span["end_ms"])
        baseline = len(targets)
        index = 0
        while index < len(targets):
            if time.monotonic() >= deadline:
                return False
            target = targets[index]; path = folder / f"frame-{target:012d}.json"
            frame = outcome(path, manifest, row, "frame")
            if frame is None:
                started = time.monotonic()
                try:
                    body, actual = engine.decode_frame(source_fd, ffmpeg_fd, video["stream_index"], target,
                                                       source_start_ms=metadata["media"].get("source_start_ms", 0))
                    decoded = time.monotonic()
                    detected = engine.detect_faces(body, runtime())
                    result = {"state": "decoded", "target_ms": target, "actual_ms": actual, **detected,
                              "decode_seconds": decoded - started, "detect_seconds": time.monotonic() - decoded}
                except engine.DecodeReview as error:
                    result = {"state": "needs_review", "target_ms": target, "reason": str(error)}
                commit(path, result, manifest, row, "frame")
                frame = outcome(path, manifest, row, "frame")
            frames.append(frame)
            if (index < baseline and frame["state"] == "decoded" and frame["face_count"] >= 2 and
                    len(targets) < baseline + core.POLICY["max_confirmations"]):
                confirm = target + core.POLICY["confirmation_offset_ms"]
                if confirm < span["end_ms"] and confirm not in targets:
                    targets.append(confirm)
            index += 1
    summary = core.visual_summary(frames, len(frames), video_state=state)
    commit(folder / "visual.json", {"summary": summary, "probe": bind(folder / "probe.json"), "frames": [bind(folder / f"frame-{f['target_ms']:012d}.json")
                                                                 for f in frames]}, manifest, row, "visual")
    return True


def audio_job(manifest, row, model, deadline, source_fd, ffmpeg_fd):
    job = load_job(row); folder = Path(manifest["state_root"]) / "jobs" / row["job_id"]
    if outcome(folder / "result.json", manifest, row, "result"):
        return True
    visual = outcome(folder / "visual.json", manifest, row, "visual")
    if visual is None:
        return False
    frames = [read(ref) for ref in visual["frames"]]
    metadata = read(visual["probe"])["media"]
    vstate = "no_video" if metadata["video"]["state"] == "absent" else metadata["video"]["state"]
    if core.visual_summary(frames, len(frames), video_state=vstate) != visual["summary"]:
        raise core.TriageError("visual summary failed evidence replay")
    cached = job["cached_audio"]["excerpts"]
    a = core.audio_diversity(cached)
    if a != job["cached_diversity"]:
        raise core.TriageError("cached acoustic ranking does not replay")
    refresh = manifest["audio_refresh"] == "all" or (manifest["audio_refresh"] == "priority" and
        (visual["summary"]["multiple_face_samples"] or a["state"] != "no_supported_diversity_in_samples"))
    targets = list(visual["summary"]["audio_target_ms"])
    if a["support"]:
        targets += [(r["start_ms"] + r["end_ms"]) // 2 for group in a["support"]["groups"] for r in group]
    targets += job["cached_audio"]["speech_resample_target_ms"]
    windows, fresh, proofs, failures = [], [], [], 0
    if refresh and metadata["audio"]["state"] == "available":
        span = metadata["audio"]["span"]
        # Endpoint guard is explicitly unsampled; receipt still records the real
        # absolute PTS and permits a valid shorter sample without silence padding.
        begin, end = span["start_ms"] + min(100, (span["end_ms"] - span["start_ms"]) // 4), span["end_ms"]
        windows = core.audio_windows(begin, end, targets[:256])
        for window in windows:
            if time.monotonic() >= deadline:
                return False
            path = folder / f"audio-{window['index']:04d}.json"
            result = outcome(path, manifest, row, "audio")
            if result is None:
                started = time.monotonic()
                try:
                    pcm, receipt = engine.decode_audio(source_fd, ffmpeg_fd, metadata["audio"]["stream_index"], window,
                                                       source_start_ms=metadata.get("source_start_ms", 0))
                    decoded = time.monotonic()
                    analyzed = model().analyze(pcm, receipt, f"fresh-{window['index']}")
                    result = {"state": "analyzed", "window": window, "receipt": receipt, **analyzed,
                              "decode_seconds": decoded - started, "model_seconds": time.monotonic() - decoded}
                except engine.DecodeReview as error:
                    result = {"state": "needs_review", "window": window, "reason": str(error), "excerpts": []}
                commit(path, result, manifest, row, "audio")
                result = outcome(path, manifest, row, "audio")
            validate_audio_checkpoint(result, window)
            fresh += result["excerpts"]; failures += result["state"] == "needs_review"; proofs.append(bind(path))
    audio = core.audio_diversity(cached + fresh)
    summary = {"recording": job["recording"], "title": job["title"],
        "cached_audio": job["cached_diversity"], "audio": audio, "visual": visual["summary"],
        "route": core.route(audio, visual["summary"]), "fresh_windows_planned": len(windows),
        "fresh_windows_analyzed": len(windows) - failures, "fresh_windows_needing_review": failures,
        "fresh_excerpts": len(fresh), "audio_refresh_requested": bool(refresh),
        "audio_availability": metadata["audio"]["state"], "audio_proofs": proofs, "visual_proof": bind(folder / "visual.json")}
    commit(folder / "result.json", summary, manifest, row, "result")
    return True


def worker(manifest, phase, deadline):
    # Enforced before importing any ML runtime. No credentials or network clients
    # are read; an unavailable model/runtime fails closed without a fallback.
    safe.child_limits(4 * 1024**3)
    runtime_cache, audio_cache = [], []

    def runtime():
        if not runtime_cache:
            runtime_cache.append(engine.face_runtime(read(manifest["face_assets"])))
        return runtime_cache[0]

    def model():
        if not audio_cache:
            audio_cache.append(engine.AudioEngine(read(manifest["audio_models"])))
        return audio_cache[0]

    for tool in ("ffmpeg", "ffprobe"):
        if bind(Path(manifest[tool]["path"])) != manifest[tool]:
            raise core.TriageError("decoder executable changed")
    for row in manifest["jobs"]:
        if time.monotonic() >= deadline:
            return
        job = load_job(row); folder = Path(manifest["state_root"]) / "jobs" / row["job_id"]
        mkdir(folder)
        final = folder / ("visual.json" if phase == "visual" else "result.json")
        if outcome(final, manifest, row, phase if phase == "visual" else "result"):
            continue
        if source_witness(job["recording"]) != job["source_witness"]:
            raise core.TriageError("source changed before inference")
        with safe.opened(job["recording"]["path"]) as source_fd, safe.opened(manifest["ffmpeg"]["path"]) as ffmpeg_fd:
            if phase == "visual":
                with safe.opened(manifest["ffprobe"]["path"]) as ffprobe_fd:
                    visual_job(manifest, row, runtime, deadline, source_fd, ffmpeg_fd, ffprobe_fd)
            else:
                audio_job(manifest, row, model, deadline, source_fd, ffmpeg_fd)


def status(manifest, *, replay=True):
    counts = {"selected": len(manifest["jobs"]), "visual_complete": 0, "complete": 0,
              "audio_diversity_candidates": 0, "repeated_visual_cues": 0, "fresh_audio_windows": 0,
              "sample_failures": 0, "cached_excerpts_reused": 0, "fresh_excerpts": 0,
              "audio_unavailable": 0, "visual_unavailable": 0, "legacy_matched_probe_eligible_excerpts": 0,
              "fresh_audio_ms": 0, "fresh_vad_positive_ms": 0, "trimmed_audio_probes": 0,
              "audio_probes_with_timestamp_discontinuities": 0}
    records = []
    root = Path(manifest["state_root"])
    for row in manifest["jobs"]:
        folder = root / "jobs" / row["job_id"]
        visual = outcome(folder / "visual.json", manifest, row, "visual")
        result = outcome(folder / "result.json", manifest, row, "result")
        counts["visual_complete"] += visual is not None
        if result:
            job = load_job(row)
            if replay:
                audio_receipts = [read(ref) for ref in result["audio_proofs"]]
                for receipt in audio_receipts:
                    validate_audio_checkpoint(receipt, receipt["window"])
                    if receipt.get("plan_id") != manifest["plan_id"] or receipt.get("job_id") != row["job_id"] or receipt.get("phase") != "audio":
                        raise core.TriageError("replayed audio belongs to another job")
                fresh = [item for receipt in audio_receipts for item in receipt["excerpts"]]
                if core.audio_diversity(job["cached_audio"]["excerpts"] + fresh) != result["audio"]:
                    raise core.TriageError("final audio evidence failed replay")
                v = read(result["visual_proof"])
                frames = [read(ref) for ref in v["frames"]]
                if Path(v["probe"]["path"]) != folder / "probe.json":
                    raise core.TriageError("probe receipt escaped job workspace")
                metadata = read(v["probe"])["media"]
                vs = "no_video" if metadata["video"]["state"] == "absent" else metadata["video"]["state"]
                targets = core.frame_times(metadata["video"]["span"]["start_ms"], metadata["video"]["span"]["end_ms"]) if vs == "available" else []
                baseline = len(targets)
                for i, frame in enumerate(frames):
                    if (i >= len(targets) or frame.get("target_ms") != targets[i] or
                            frame.get("plan_id") != manifest["plan_id"] or frame.get("job_id") != row["job_id"] or
                            frame.get("phase") != "frame" or Path(v["frames"][i]["path"]) != folder / f"frame-{targets[i]:012d}.json"):
                        raise core.TriageError("visual sample escaped deterministic plan")
                    if (i < baseline and frame["state"] == "decoded" and frame["face_count"] >= 2 and
                            len(targets) < baseline + core.POLICY["max_confirmations"]):
                        confirmation = targets[i] + core.POLICY["confirmation_offset_ms"]
                        if confirmation < metadata["video"]["span"]["end_ms"] and confirmation not in targets:
                            targets.append(confirmation)
                if len(frames) != len(targets):
                    raise core.TriageError("visual snapshot drops planned samples")
                if core.visual_summary(frames, len(frames), video_state=vs) != result["visual"] or v["summary"] != result["visual"]:
                    raise core.TriageError("final visual evidence failed replay")
                if core.route(result["audio"], result["visual"]) != result["route"]:
                    raise core.TriageError("final routing failed replay")
                a = core.audio_diversity(job["cached_audio"]["excerpts"])
                refresh = manifest["audio_refresh"] == "all" or (manifest["audio_refresh"] == "priority" and
                    (v["summary"]["multiple_face_samples"] or a["state"] != "no_supported_diversity_in_samples"))
                cues = list(v["summary"]["audio_target_ms"])
                if a["support"]:
                    cues += [(r["start_ms"] + r["end_ms"]) // 2 for group in a["support"]["groups"] for r in group]
                cues += job["cached_audio"]["speech_resample_target_ms"]
                windows = []
                if refresh and metadata["audio"]["state"] == "available":
                    span = metadata["audio"]["span"]
                    windows = core.audio_windows(span["start_ms"] + min(100, (span["end_ms"]-span["start_ms"])//4), span["end_ms"], cues[:256])
                if [r["window"] for r in audio_receipts] != windows or any(
                    Path(ref["path"]) != folder / f"audio-{i:04d}.json" for i, ref in enumerate(result["audio_proofs"])):
                    raise core.TriageError("audio snapshot drops or changes scheduled windows")
                failures = sum(r["state"] == "needs_review" for r in audio_receipts)
                expected = {"recording": job["recording"], "title": job["title"], "cached_audio": a,
                    "fresh_windows_planned": len(windows), "fresh_windows_analyzed": len(windows)-failures,
                    "fresh_windows_needing_review": failures, "fresh_excerpts": len(fresh),
                    "audio_refresh_requested": bool(refresh), "audio_availability": metadata["audio"]["state"]}
                if any(result.get(k) != value for k, value in expected.items()):
                    raise core.TriageError("final counts or source selection do not replay")
            counts["complete"] += 1
            counts["audio_diversity_candidates"] += result["audio"]["state"] == "supported_audio_diversity"
            counts["repeated_visual_cues"] += result["visual"]["repeated_visual_cue"]
            counts["fresh_audio_windows"] += result["fresh_windows_analyzed"]
            counts["sample_failures"] += result["fresh_windows_needing_review"] + result["visual"]["frames_needing_review"]
            counts["cached_excerpts_reused"] += len(job["cached_audio"]["excerpts"])
            counts["fresh_excerpts"] += result["fresh_excerpts"]
            counts["audio_unavailable"] += result["audio_availability"] != "available"
            counts["visual_unavailable"] += result["visual"]["state"] in {"no_video", "unsupported_timing", "unusable_visual_samples"}
            counts["legacy_matched_probe_eligible_excerpts"] += sum(read(ref).get("legacy_eligible_excerpts", 0) for ref in result["audio_proofs"])
            for ref in result["audio_proofs"]:
                sample = read(ref)
                if sample["state"] == "analyzed":
                    receipt = sample["receipt"]
                    counts["fresh_audio_ms"] += receipt["end_ms"]-receipt["start_ms"]
                    counts["fresh_vad_positive_ms"] += sample["vad_positive_ms"]
                    counts["trimmed_audio_probes"] += receipt["short_sample"]
                    counts["audio_probes_with_timestamp_discontinuities"] += receipt["timestamp_discontinuities"] > 0
            records.append({"job_id": row["job_id"], "title": result["title"], "route": result["route"]["label"],
                            "priority": result["route"]["review_priority"], "result": bind(folder / "result.json")})
    return {"kind": KIND + "_status", "plan_id": manifest["plan_id"],
            "state": "completed_with_sample_reviews" if counts["complete"] == counts["selected"] and counts["sample_failures"] else
                     "completed" if counts["complete"] == counts["selected"] else "incomplete",
            "counts": counts, "records": sorted(records, key=lambda r: (-r["priority"], r["job_id"])),
            "semantics": core.SEMANTICS}


def run(path, expected):
    manifest = load_manifest(path, expected); root = Path(manifest["state_root"])
    with locked(root):
        for row in manifest["jobs"]:
            job = load_job(row)
            if source_witness(job["recording"]) != job["source_witness"]:
                raise core.TriageError("selected source changed; existing evidence is historical only")
        existing = status(manifest)
        if existing["counts"]["complete"] == len(manifest["jobs"]):
            return existing
        deadline = time.monotonic() + manifest["max_runtime_seconds"]
        for phase in ("visual", "audio"):
            if time.monotonic() >= deadline:
                break
            python = read(manifest["face_assets"])["runtime"]["python"]["path"] if phase == "visual" else manifest["audio_python"]["launcher"]
            if phase == "audio":
                if (bind(Path(python).resolve(strict=True)) != manifest["audio_python"]["executable"] or
                        bind(Path(manifest["audio_python"]["configuration"]["path"])) != manifest["audio_python"]["configuration"]):
                    raise core.TriageError("audio interpreter configuration changed")
            command = [python, "-B", str(Path(__file__).resolve()), "worker", "--manifest", str(path),
                       "--expected-sha256", expected, "--phase", phase,
                       "--deadline", str(deadline), "--parent-pid", str(os.getpid())]
            process = subprocess.Popen(command, stdin=subprocess.DEVNULL, start_new_session=True,
                env={"PATH": "/usr/bin:/bin", "LANG": "C.UTF-8", "PYTHONDONTWRITEBYTECODE": "1",
                     "PYTHONNOUSERSITE": "1", "OMP_NUM_THREADS": "1", "OPENBLAS_NUM_THREADS": "1",
                     "MKL_NUM_THREADS": "1", "CUDA_VISIBLE_DEVICES": "", "HF_HUB_OFFLINE": "1"})
            try:
                process.wait(timeout=max(1, deadline - time.monotonic() + 25))
                if process.returncode != 0:
                    raise core.TriageError(f"{phase} worker failed; committed samples preserved")
            except subprocess.TimeoutExpired:
                raise core.TriageError("finite worker deadline exceeded; checkpoints preserved") from None
            finally:
                if process.poll() is None:
                    os.killpg(process.pid, signal.SIGKILL); process.wait()
        value = status(manifest)
        # Immutable snapshots rather than a mutable status pointer; status command
        # always replays committed evidence and does not need a running worker.
        write(root / ("status-" + safe.digest(value)[:32] + ".json"), value)
        return value


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    commands = parser.add_subparsers(dest="command", required=True)
    prepare_parser = commands.add_parser("prepare")
    for name in ("inventory", "expected-sha256", "audio-results-root", "face-assets", "audio-models", "audio-python", "state-root"):
        prepare_parser.add_argument("--" + name, required=True)
    prepare_parser.add_argument("--media-id", action="append", default=[])
    prepare_parser.add_argument("--limit", type=int, default=16)
    prepare_parser.add_argument("--max-seconds", type=int, default=1800)
    prepare_parser.add_argument("--audio-refresh", choices=("priority", "all", "none"), default="priority")
    for name in ("run", "status", "worker"):
        sub = commands.add_parser(name)
        sub.add_argument("--manifest", required=True); sub.add_argument("--expected-sha256", required=True)
        if name == "worker":
            sub.add_argument("--phase", choices=("visual", "audio"), required=True)
            sub.add_argument("--deadline", type=float, required=True); sub.add_argument("--parent-pid", type=int, required=True)
    args = parser.parse_args(argv)
    try:
        if args.command == "prepare":
            value = prepare(args.inventory, args.expected_sha256, args.audio_results_root,
                args.face_assets, args.audio_models, args.audio_python, args.state_root,
                media_ids=args.media_id, limit=args.limit, max_seconds=args.max_seconds, audio_refresh=args.audio_refresh)
        elif args.command == "worker":
            safe.die_with_parent(args.parent_pid)
            manifest = load_manifest(args.manifest, args.expected_sha256)
            if not time.monotonic() < args.deadline <= time.monotonic() + manifest["max_runtime_seconds"] + 1:
                raise core.TriageError("worker deadline invalid")
            worker(manifest, args.phase, args.deadline)
            return 0
        elif args.command == "run":
            value = run(args.manifest, args.expected_sha256)
        else:
            value = status(load_manifest(args.manifest, args.expected_sha256))
        print(json.dumps(value, sort_keys=True, allow_nan=False, indent=2))
        return 0
    except KeyboardInterrupt:
        print("Multimodal triage interrupted; committed evidence retained.", file=sys.stderr)
        return 130
    except (RuntimeError, OSError, ValueError, KeyError, TypeError) as error:
        detail = str(error) if isinstance(error, (core.TriageError, safe.ScreenError, speech.ScreenEngineError)) else type(error).__name__
        print("MultimodalTriageError: " + detail, file=sys.stderr)
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
