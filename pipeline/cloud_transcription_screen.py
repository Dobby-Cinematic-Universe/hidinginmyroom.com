"""Separate CPU screening adapter for the opt-in cloud transcription lane.

Completed evidence is replayed from explicitly pinned multimodal campaigns.
Otherwise a one-record campaign is created beneath the caller's new job folder;
the independent archive campaign is never mutated or waited on. Only a complete,
adequately sampled negative can disable provider diarization. Sampling never
establishes a whole-recording speaker count or identity.
"""
from __future__ import annotations

import argparse
from contextlib import contextmanager
import fcntl
import hashlib
import json
import os
from pathlib import Path
import sys

from pipeline import cloud_transcription_archive as archive
from pipeline import speaker_screen_multimodal as mm

core = mm.core
safe = mm.safe
KIND = "himr_cloud_speaker_screen"
POLICY = {
    "version": "conservative_completed_sample_v1",
    "negative_min_independent_fresh_probes": 4,
    "negative_requires_all_scheduled_audio": True,
    "negative_requires_zero_sample_failures": True,
    "negative_requires_zero_multiple_face_samples": True,
    "uncertain_requires_diarization": True,
    "positive_requires_supported_audio_diversity": True,
    "fresh_audio_refresh": "all",
    "max_isolated_workers_per_configuration": 1,
}
SEMANTICS = {
    "sample_screen_not_full_diarization": True,
    "whole_recording_single_speaker_proven": False,
    "speaker_identity_inferred": False,
    "faces_are_speakers": False,
    "calibrated_accuracy_claimed": False,
    "existing_archive_campaign_mutated": False,
    "cloud_api_calls": False,
}


class ScreenError(RuntimeError):
    pass


class ScreenBusy(ScreenError):
    """Another isolated screen owns this configuration's bounded CPU slot."""


def implementation():
    return {"adapter": hashlib.sha256(Path(__file__).read_bytes()).hexdigest(),
            "archive_io": hashlib.sha256(Path(archive.__file__).read_bytes()).hexdigest(),
            "multimodal": mm.implementation()}


def _raw_recording(recording):
    media = recording.get("media", {})
    for key in ("path", "sha256", "byte_count"):
        if key not in media:
            raise ScreenError("cloud recording lacks a media binding")
    safe.path_value(media["path"])
    if not isinstance(media["sha256"], str) or not safe.SHA.fullmatch(media["sha256"]):
        raise ScreenError("cloud recording lacks a content identity")
    safe.integer(media["byte_count"], 1, 2**63-1, "media bytes")
    identity = "media_sha256_" + media["sha256"]
    if recording.get("recording_id") != identity:
        raise ScreenError("cloud recording identity differs from media")
    core.integer(recording.get("duration_ms"), 1, core.MAX_DURATION_MS, "recording duration")
    return {**media, "media_id": identity, "duration_hint_ms": recording["duration_ms"]}


def prepare_config(template_manifest_ref, reuse_manifest_refs=(), *, max_runtime_seconds=300):
    """Index old inputs once, without opening media or starting any model/API."""
    core.integer(max_runtime_seconds, 60, 900, "per-record screening runtime")
    references = [template_manifest_ref, *reuse_manifest_refs]
    if len(references) > 16:
        raise ScreenError("too many screening provenance campaigns")
    manifests, catalog, seen = [], [], set()
    for reference in references:
        safe.file_binding(reference)
        key = (reference["path"], reference["sha256"])
        if key in seen:
            continue
        seen.add(key)
        manifest = mm.load_manifest(reference["path"], reference["sha256"])
        ordinal = len(manifests)
        manifests.append(reference)
        for row in manifest["jobs"]:
            job = mm.load_job(row)
            raw = job["recording"]
            if raw["media_id"] != "media_sha256_" + raw["sha256"]:
                raise ScreenError("screen input content identity differs")
            catalog.append({"recording_id": raw["media_id"], "manifest_index": ordinal, "job": row})
            if len(catalog) > 16384:
                raise ScreenError("screen provenance index exceeds its bound")
    return {"kind": KIND + "_config", "schema_version": 1,
            "template_manifest": template_manifest_ref, "reuse_manifests": manifests,
            "catalog": sorted(catalog, key=lambda row: (row["recording_id"], row["manifest_index"])),
            "max_runtime_seconds": max_runtime_seconds,
            "implementation": implementation(), "policy": POLICY, "semantics": SEMANTICS}


def load_config(reference):
    return validate_config(archive.read_bound(reference))


def validate_config(config):
    """Validate an already hash-bound configuration object for offline planning."""
    if not isinstance(config, dict):
        raise ScreenError("screen configuration must be an object")
    if (config.get("kind") != KIND + "_config" or type(config.get("schema_version")) is not int
            or config["schema_version"] != 1 or config.get("implementation") != implementation()
            or config.get("policy") != POLICY or config.get("semantics") != SEMANTICS):
        raise ScreenError("screen configuration implementation or policy differs")
    core.integer(config.get("max_runtime_seconds"), 60, 900, "screen runtime")
    if (not isinstance(config.get("catalog"), list) or len(config["catalog"]) > 16384
            or not isinstance(config.get("reuse_manifests"), list)
            or not 1 <= len(config["reuse_manifests"]) <= 16
            or config.get("template_manifest") != config["reuse_manifests"][0]):
        raise ScreenError("screen provenance index is invalid")
    for reference in config["reuse_manifests"]:
        safe.file_binding(reference)
    return config


def _manifest(config, index):
    core.integer(index, 0, len(config["reuse_manifests"]) - 1, "manifest index")
    reference = config["reuse_manifests"][index]
    return reference, mm.load_manifest(reference["path"], reference["sha256"])


def _cached_replay(job, manifest):
    cached = job["cached_audio"]
    if not cached["proofs"]:
        expected = {"excerpts": [], "proofs": [], "previous_status": None,
                    "original_usable_excerpts": 0, "speech_resample_target_ms": []}
    else:
        expected = mm.cached_evidence(Path(cached["proofs"][0]["path"]), job["recording"],
                                      job["source_witness"], mm.read(manifest["audio_models"]))
    if cached != expected or core.audio_diversity(cached["excerpts"]) != job["cached_diversity"]:
        raise ScreenError("cached acoustic evidence does not replay")


def _validate_job(recording, manifest, row):
    if row not in manifest["jobs"]:
        raise ScreenError("indexed screen job is absent from its pinned manifest")
    job = mm.load_job(row)
    if job["recording"] != _raw_recording(recording):
        raise ScreenError("screen job belongs to different media or duration")
    if mm.source_witness(job["recording"]) != job["source_witness"]:
        raise ScreenError("screen evidence source witness changed")
    acquisition = mm.read(job["acquisition"])
    if (acquisition.get("status") != "completed" or acquisition.get("errors")
            or any(acquisition.get("admission", {}).get(key) != job["recording"][key]
                   for key in ("path", "sha256", "byte_count", "media_id"))):
        raise ScreenError("screen source acquisition no longer replays")
    _cached_replay(job, manifest)
    return job


def classify_result(result, audio_receipts):
    """Interpret only a fully replayed result; never call this a speaker count."""
    audio, visual = result["audio"], result["visual"]
    if audio["state"] == "supported_audio_diversity":
        return "screen_positive", ["repeated_supported_acoustic_diversity"]
    reasons = []
    if audio["state"] != "no_supported_diversity_in_samples":
        reasons.append("insufficient_usable_audio")
    if audio.get("anchor_search_capped"):
        reasons.append("acoustic_search_budget_exhausted")
    if result.get("audio_availability") != "available":
        reasons.append("audio_unavailable_or_unsupported")
    if not result.get("audio_refresh_requested"):
        reasons.append("no_fresh_audio_baseline")
    if (result.get("fresh_windows_needing_review") or visual.get("frames_needing_review")
            or result.get("fresh_windows_analyzed") != result.get("fresh_windows_planned")):
        reasons.append("sample_failure_or_incomplete_coverage")
    if visual.get("multiple_face_samples"):
        reasons.append("visual_people_cue_requires_audio_review")
    if visual.get("state") not in {"no_multiple_faces_observed", "no_video"}:
        reasons.append("visual_evidence_uncertain")
    useful_probes = {receipt["window"]["index"] for receipt in audio_receipts
                     if receipt.get("state") == "analyzed" and receipt.get("excerpts")
                     and receipt["window"].get("reason") == "uniform_audio_baseline"}
    if len(useful_probes) < POLICY["negative_min_independent_fresh_probes"]:
        reasons.append("fewer_than_four_usable_independent_baseline_probes")
    return ("screen_uncertain", sorted(set(reasons))) if reasons else (
        "screen_negative", ["complete_samples_without_supported_multispeaker_evidence"])


def _decision(recording, config_ref, manifest_ref, result_ref, job, result, receipts, *, reused,
              diagnostic=None):
    state, reasons = classify_result(result, receipts) if result else (
        "screen_uncertain", ["screen_incomplete_or_worker_failed"])
    return {"kind": KIND + "_decision", "schema_version": 1,
            "recording_id": recording["recording_id"], "media": recording["media"],
            "state": state, "diarization": state != "screen_negative", "reasons": reasons,
            "configuration": config_ref, "manifest": manifest_ref, "result": result_ref,
            "diagnostic": diagnostic, "reused_existing_screen": reused,
            "source_witness": job["source_witness"],
            "evidence_summary": {
                "audio_state": result["audio"]["state"] if result else None,
                "visual_state": result["visual"]["state"] if result else None,
                "fresh_audio_windows": result["fresh_windows_analyzed"] if result else 0,
                "usable_excerpts": result["audio"]["usable_excerpts"] if result else 0,
                "multiple_face_samples": result["visual"]["multiple_face_samples"] if result else 0,
                "completed_evidence_replayed": result is not None,
            }, "method": {"sampling_policy": core.POLICY, "decision_policy": POLICY,
                           "implementation": implementation()}, "semantics": SEMANTICS}


def import_completed(recording, config_ref, manifest_ref, row, *, reused=True):
    """Replay one job against its original whole-manifest hash; no old writes."""
    manifest = mm.load_manifest(manifest_ref["path"], manifest_ref["sha256"])
    job = _validate_job(recording, manifest, row)
    folder = Path(manifest["state_root"]) / "jobs" / row["job_id"]
    result_path = folder / "result.json"
    if not safe.exists(result_path):
        return None
    reference = mm.bind(result_path)
    result = mm.read(reference)
    # status() is a pure proof replay. Scope its loop to this proven member of the
    # original manifest; do not write or claim a new valid campaign manifest.
    replay = mm.status({**manifest, "jobs": [row]}, replay=True)
    if replay["counts"]["complete"] != 1 or mm.bind(result_path) != reference:
        raise ScreenError("completed screening result changed or did not replay")
    receipts = [mm.read(ref) for ref in result["audio_proofs"]]
    return _decision(recording, config_ref, manifest_ref, reference, job, result, receipts, reused=reused)


def validate_decision(decision, recording, config_ref):
    """Replay the persisted paid-gate decision; never trust a diarization bool.

    Call before provider submission and when admitting transcript provenance.
    Immutable proof hashes, implementation, current source witness, and the full
    classification policy are checked again without model/API execution.
    """
    config = load_config(config_ref)
    _raw_recording(recording)
    if (not isinstance(decision, dict) or decision.get("configuration") != config_ref
            or decision.get("recording_id") != recording["recording_id"]
            or decision.get("media") != recording["media"]
            or type(decision.get("reused_existing_screen")) is not bool):
        raise ScreenError("screen decision source or configuration differs")
    reference = decision.get("manifest")
    safe.file_binding(reference)
    manifest = mm.load_manifest(reference["path"], reference["sha256"])
    reused = decision["reused_existing_screen"]
    if reused:
        if reference not in config["reuse_manifests"]:
            raise ScreenError("reused decision comes from an unadmitted campaign")
        index = config["reuse_manifests"].index(reference)
        candidates = [entry["job"] for entry in config["catalog"]
                      if entry["recording_id"] == recording["recording_id"]
                      and entry["manifest_index"] == index]
    else:
        _, template = _manifest(config, 0)
        if (len(manifest["jobs"]) != 1 or manifest["audio_refresh"] != "all"
                or manifest["max_runtime_seconds"] != config["max_runtime_seconds"]
                or any(manifest[key] != template[key] for key in (
                    "face_assets", "audio_models", "audio_python", "ffmpeg", "ffprobe", "inventory"))):
            raise ScreenError("isolated decision implementation or configuration differs")
        root = Path(manifest["state_root"])
        if any(mm._overlap(root, Path(ref["path"]).parent) for ref in config["reuse_manifests"]):
            raise ScreenError("isolated decision overlaps an existing campaign")
        candidates = manifest["jobs"]
    if decision.get("result") is not None:
        safe.file_binding(decision["result"])
        candidates = [row for row in candidates if decision["result"]["path"] == str(
            Path(manifest["state_root"]) / "jobs" / row["job_id"] / "result.json")]
        if len(candidates) != 1:
            raise ScreenError("screen decision result is not an admitted job")
        # Hash-check the caller's exact snapshot before the regular replay binds
        # the currently visible completed file.
        mm.read(decision["result"])
        expected = import_completed(recording, config_ref, reference, candidates[0], reused=reused)
    else:
        if reused or len(candidates) != 1 or decision.get("diagnostic") is None:
            raise ScreenError("uncertain decision lacks an isolated execution diagnostic")
        diagnostic = decision["diagnostic"]
        safe.file_binding(diagnostic)
        if diagnostic["path"] != str(Path(manifest["state_root"]) / "screen-failure.json"):
            raise ScreenError("screen diagnostic escaped its isolated workspace")
        saved = mm.read(diagnostic)
        if (saved.get("kind") != KIND + "_worker_diagnostic" or saved.get("schema_version") != 1
                or saved.get("configuration") != config_ref or saved.get("manifest") != reference
                or saved.get("recording_id") != recording["recording_id"]
                or saved.get("diarization_required") is not True
                or saved.get("reason") not in {"bounded_screen_incomplete", "bounded_screen_worker_failed_or_timed_out"}):
            raise ScreenError("uncertain screen diagnostic provenance differs")
        job = _validate_job(recording, manifest, candidates[0])
        mm.status({**manifest, "jobs": candidates}, replay=True)
        expected = _decision(recording, config_ref, reference, None, job, None, [], reused=False,
                             diagnostic=diagnostic)
    if expected is None or decision != expected:
        raise ScreenError("persisted screen decision does not replay")
    return decision


def lookup_completed(recording, config_ref, config=None):
    config = config or load_config(config_ref)
    for entry in config["catalog"]:
        if entry.get("recording_id") != recording["recording_id"]:
            continue
        reference, manifest = _manifest(config, entry["manifest_index"])
        row = entry["job"]
        if row not in manifest["jobs"]:
            raise ScreenError("screen index job was not admitted by its manifest")
        path = Path(manifest["state_root"]) / "jobs" / row["job_id"] / "result.json"
        if not safe.exists(path):
            continue
        result = import_completed(recording, config_ref, reference, row)
        if result:
            return result
    return None


@contextmanager
def _slot(config_ref):
    directory_path = Path(config_ref["path"]).parent
    with safe.paths.retained_directory(directory_path) as directory:
        fd = os.open("cloud-screen.lock", os.O_CREAT | os.O_RDWR | os.O_NOFOLLOW, 0o600, dir_fd=directory)
        try:
            info = os.fstat(fd)
            if info.st_uid != os.geteuid() or info.st_nlink != 1 or info.st_mode & 0o077:
                raise ScreenError("unsafe screen CPU-slot lock")
            try:
                fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
            except BlockingIOError:
                raise ScreenBusy("another isolated cloud screen is already running") from None
            yield
        finally:
            os.close(fd)


def _isolated_manifest(recording, folder, config):
    template_ref, template = _manifest(config, 0)
    root = folder / "multimodal"
    raw = _raw_recording(recording)
    protected = [Path(reference["path"]).parent for reference in config["reuse_manifests"]]
    protected += [Path(raw["path"]), mm.ROOT / "pipeline"]
    protected += [Path(template[key]["path"]).parent for key in ("face_assets", "audio_models")]
    if any(mm._overlap(root, path) for path in protected):
        raise ScreenError("isolated screen workspace overlaps protected inputs or campaigns")
    if safe.exists(root / "manifest.json"):
        reference = mm.bind(root / "manifest.json")
        manifest = mm.load_manifest(reference["path"], reference["sha256"])
        if (len(manifest["jobs"]) != 1 or manifest["audio_refresh"] != "all"
                or manifest["max_runtime_seconds"] != config["max_runtime_seconds"]
                or any(manifest[key] != template[key] for key in (
                    "face_assets", "audio_models", "audio_python", "ffmpeg", "ffprobe", "inventory"))):
            raise ScreenError("isolated screen resume configuration differs")
        _validate_job(recording, manifest, manifest["jobs"][0])
        return reference, manifest
    if safe.exists(root):
        raise ScreenError("uncommitted isolated screen workspace requires review")
    aliases = recording.get("aliases", [])
    if not aliases:
        raise ScreenError("screen requires a verified acquisition provenance alias")
    acquisition = aliases[0]["acquisition_result"]
    proof = mm.read(acquisition)
    if (proof.get("status") != "completed" or proof.get("errors")
            or any(proof.get("admission", {}).get(key) != raw[key]
                   for key in ("path", "sha256", "byte_count", "media_id"))):
        raise ScreenError("isolated screen acquisition binding differs")
    cached = {"excerpts": [], "proofs": [], "previous_status": None,
              "original_usable_excerpts": 0, "speech_resample_target_ms": []}
    job = {"recording": raw, "source_witness": mm.source_witness(raw), "title": recording.get("title"),
           "acquisition": acquisition, "cached_audio": cached, "cached_diversity": core.audio_diversity([])}
    mm.mkdir(root); mm.mkdir(root / "inputs"); mm.mkdir(root / "jobs")
    job_id = "avtriage_" + safe.digest(job)[:32]
    input_ref = mm.write(root / "inputs" / (job_id + ".json"), job)
    body = {key: value for key, value in template.items() if key != "plan_id"}
    body.update(state_root=str(root), jobs=[{"job_id": job_id, "input": input_ref}],
                max_runtime_seconds=config["max_runtime_seconds"], audio_refresh="all")
    manifest = {**body, "plan_id": "avcampaign_" + safe.digest(body)[:32]}
    return mm.write(root / "manifest.json", manifest), manifest


def screen_one(recording, folder, config_ref):
    """Reuse a completed proof, or run one bounded isolated CPU-only screen.

    Caller owns queue ordering and only calls this for cloud-missing recordings.
    The caller publishes returned decisions as its own immutable screen.json.
    No network or paid provider is used by this adapter or its worker children.
    """
    _raw_recording(recording)
    config = load_config(config_ref)
    completed = lookup_completed(recording, config_ref, config)
    if completed:
        return completed
    folder = safe.path_value(folder)
    with safe.paths.retained_directory(folder), _slot(config_ref):
        # Completion may have appeared while the separate archive job progressed.
        completed = lookup_completed(recording, config_ref, config)
        if completed:
            return completed
        manifest_ref, manifest = _isolated_manifest(recording, folder, config)
        row = manifest["jobs"][0]
        failed_path = Path(manifest["state_root"]) / "screen-failure.json"
        if not safe.exists(failed_path):
            try:
                mm.run(manifest_ref["path"], manifest_ref["sha256"])
            except (core.TriageError, OSError, RuntimeError) as error:
                # Check source/config integrity again before allowing the bounded
                # execution failure to become uncertainty (never a negative).
                load_config(config_ref)
                current = mm.load_manifest(manifest_ref["path"], manifest_ref["sha256"])
                _validate_job(recording, current, row)
                mm.status({**current, "jobs": [row]}, replay=True)
                mm.write(failed_path, {"kind": KIND + "_worker_diagnostic", "schema_version": 1,
                    "configuration": config_ref, "manifest": manifest_ref,
                    "recording_id": recording["recording_id"], "error_type": type(error).__name__,
                    "reason": "bounded_screen_worker_failed_or_timed_out", "diarization_required": True})
        completed = import_completed(recording, config_ref, manifest_ref, row, reused=False)
        if completed:
            return completed
        job = _validate_job(recording, manifest, row)
        if not safe.exists(failed_path):
            mm.write(failed_path, {"kind": KIND + "_worker_diagnostic", "schema_version": 1,
                "configuration": config_ref, "manifest": manifest_ref,
                "recording_id": recording["recording_id"], "error_type": None,
                "reason": "bounded_screen_incomplete", "diarization_required": True})
        diagnostic = mm.bind(failed_path)
        saved = mm.read(diagnostic)
        if (saved.get("configuration") != config_ref or saved.get("manifest") != manifest_ref
                or saved.get("recording_id") != recording["recording_id"]
                or saved.get("diarization_required") is not True):
            raise ScreenError("screen diagnostic belongs to different inputs")
        return _decision(recording, config_ref, manifest_ref, None, job, None, [], reused=False,
                         diagnostic=diagnostic)


def write_config(output, config):
    """Publish a fresh configuration inside an existing owned private parent."""
    from pipeline import transcript_summary as io

    output = safe.path_value(output)
    config = validate_config(config)
    if safe.exists(output):
        raise ScreenError("screen configuration output must be a fresh file")
    # Include actual media locations as well as model/runtime/old-workspace paths.
    # Opening indexed input JSON is metadata-only; raw media remains unopened.
    manifests = [mm.load_manifest(ref["path"], ref["sha256"]) for ref in config["reuse_manifests"]]
    media_paths = [mm.load_job(entry["job"])["recording"]["path"] for entry in config["catalog"]]
    io.protect(output.parent, {"configuration": config, "manifests": manifests, "media": media_paths})
    with safe.paths.retained_directory(output.parent):
        pass
    return archive.write_inventory(output, config)


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    commands = parser.add_subparsers(dest="command", required=True)
    prepare = commands.add_parser("prepare-config", help="Index pinned screening inputs without any model/API work")
    prepare.add_argument("--template-manifest", required=True)
    prepare.add_argument("--expected-sha256", required=True)
    prepare.add_argument("--reuse-manifest", action="append", default=[])
    prepare.add_argument("--reuse-sha256", action="append", default=[])
    prepare.add_argument("--max-runtime-seconds", type=int, default=300)
    prepare.add_argument("--output", required=True, help="Fresh file in an existing owned 0700 directory")
    args = parser.parse_args(argv)
    try:
        if len(args.reuse_manifest) != len(args.reuse_sha256):
            raise ScreenError("each reuse manifest needs its matching SHA-256")
        reference = {"path": str(safe.path_value(args.template_manifest)), "sha256": args.expected_sha256}
        reuse = [{"path": str(safe.path_value(path)), "sha256": sha}
                 for path, sha in zip(args.reuse_manifest, args.reuse_sha256, strict=True)]
        config = prepare_config(reference, reuse, max_runtime_seconds=args.max_runtime_seconds)
        result = write_config(args.output, config)
        print(json.dumps({"configuration": result, "indexed_screen_jobs": len(config["catalog"]),
                          "source_campaigns": len(config["reuse_manifests"]),
                          "max_runtime_seconds": config["max_runtime_seconds"],
                          "model_executions": 0, "api_requests": 0}, sort_keys=True))
        return 0
    except (RuntimeError, OSError, ValueError, KeyError, TypeError) as error:
        detail = str(error) if isinstance(error, (ScreenError, core.TriageError, safe.ScreenError,
                                                archive.ArchiveInventoryError)) else type(error).__name__
        print("CloudScreenError: " + detail, file=sys.stderr)
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
