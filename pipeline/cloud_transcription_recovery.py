"""Offline admission of already-paid terminal cloud results into a new plan.

Producer code snapshots authenticate an explicit old contract; no current-plan
implementation check is bypassed and no producer code is executed. Every paid
reservation must have successful terminal evidence. Re-normalization creates
only new artifacts, preserves original provider/intent references, and carries
the entire old reservation forward without pretending a new submission occurred.
"""
from __future__ import annotations

import argparse
import hashlib
import json
import math
import os
from pathlib import Path
import stat
import sys
import time

from pipeline import transcript_summary as io
from pipeline import cloud_transcription_client as clients
from pipeline import cloud_transcription_media as media
from pipeline import cloud_transcription_screen as screen

KIND = "himr_cloud_paid_recovery"
PRODUCER_KIND = "himr_third_party_first_cloud_transcription_plan"
RATES = {"assemblyai": 230000, "revai": 200000}
POLICY = {"whole_recordings": True, "automatic_chunking": False, "automatic_paid_retries": False,
          "fallback_on_ambiguous_submission": False, "global_glossary_prompt": False,
          "speaker_labels_are_identities": False, "publication_authority": False,
          "source_mutation": False, "existing_campaign_mutation": False,
          "third_party_model_attestation": "user_confirmed_author_report_not_independently_verified"}
SEMANTICS = {"new_paid_requests": 0, "old_artifact_writes": False, "original_intents_preserved": True,
             "all_producer_paid_jobs_accounted": True, "unknown_usage_hold_released": False,
             "producer_must_remain_stopped": True, "publication_authority": False}
MAX_PAID_JOBS = 64
PRODUCER_FILES = {
    "cloud_transcription.py", "cloud_transcription_archive.py", "cloud_transcription_client.py",
    "cloud_transcription_env.py", "cloud_transcription_import.py", "cloud_transcription_media.py",
    "cloud_transcription_screen.py", "speaker_screen.py", "speaker_screen_core.py", "speaker_screen_engine.py",
    "speaker_screen_paths.py", "transcript_summary.py", "transcript_summary_anthropic.py",
    "transcript_summary_classification.py", "transcript_summary_client.py", "transcript_summary_core.py",
    "transcript_summary_env.py", "transcript_summary_recovery.py", "transcript_summary_sources.py"}


class RecoveryError(RuntimeError):
    pass


def implementation():
    return {name: hashlib.sha256((Path(__file__).parent / name).read_bytes()).hexdigest()
            for name in ("cloud_transcription_recovery.py", "cloud_transcription_client.py",
                         "cloud_transcription_media.py", "cloud_transcription_screen.py")}


def _ref(path):
    return io.binding(Path(path))


def _read(path, proofs):
    reference = _ref(path)
    proofs[str(path)] = reference
    return io.read(reference)


def _job_id(recording):
    return "cloudjob_" + io.digest({"media": recording["media"], "recording_id": recording["recording_id"],
                                    "duration_ms": recording["duration_ms"]})[:32]


def _cost(provider, duration_ms, diarization):
    seconds = max(15, math.ceil((duration_ms + media.tolerance_ms(duration_ms)) / 1000))
    rate = RATES[provider] - (20000 if provider == "assemblyai" and not diarization else 0)
    return (seconds * rate + 3599) // 3600


def producer_plan(plan_ref, code_ref):
    """Authenticate the explicitly supported producer, never call load_plan."""
    plan, code = io.read(plan_ref), io.read(code_ref)
    io.safe.exact(plan, {"kind", "schema_version", "state_root", "inventory", "third_party_inventory",
        "matching", "implementation", "ffmpeg", "screen_config", "rates_microusd_hour", "policy", "recordings"}, "recovery producer plan")
    if (plan["kind"] != PRODUCER_KIND or type(plan["schema_version"]) is not int or plan["schema_version"] != 1
            or plan["policy"] != POLICY or plan["rates_microusd_hour"] != RATES
            or set(plan["implementation"]) != PRODUCER_FILES
            or code.get("kind") != "himr_paid_cloud_v3_code_snapshot" or code.get("schema_version") != 1
            or code.get("old_plan") != plan_ref or code.get("implementation") != plan["implementation"]
            or code.get("snapshot_before_english_locale_compatibility_fix") is not True
            or code.get("source_mutation") is not False or code.get("new_paid_requests") != 0
            or not isinstance(code.get("files"), dict)
            or not set(plan["implementation"]) <= set(code["files"]) or len(code["files"]) > 64):
        raise RecoveryError("producer contract or exact code proof differs")
    for name, reference in code["files"].items():
        io.safe.file_binding(reference)
        if Path(reference["path"]).name != name or not name.endswith(".py"):
            raise RecoveryError("producer code snapshot filename differs")
        if name in plan["implementation"] and reference["sha256"] != plan["implementation"][name]:
            raise RecoveryError("producer code snapshot hash differs")
        io.read_bytes(reference)
    root = io.safe.path_value(plan["state_root"])
    if Path(plan_ref["path"]) != root / "plan.json":
        raise RecoveryError("producer plan escaped its original workspace")
    archive = io.read(plan["inventory"])
    source = archive.get("recordings")
    if (archive.get("kind") != "himr_cloud_transcription_archive_inventory" or archive.get("schema_version") != 1
            or not isinstance(source, list) or not 1 <= len(source) <= 10000
            or len(plan["recordings"]) != len(source)):
        raise RecoveryError("producer source inventory or partition differs")
    seen = set()
    for row, recording in zip(plan["recordings"], source, strict=True):
        io.safe.exact(row, {"job_id", "recording", "provider", "disposition", "reason", "import",
                           "maximum_cost_microusd", "match_status"}, "producer recording")
        if row["recording"] != recording or row["job_id"] != _job_id(recording) or row["job_id"] in seen:
            raise RecoveryError("producer recording identity or source binding differs")
        seen.add(row["job_id"])
        if row["disposition"] == "cloud":
            duration = recording.get("duration_ms")
            io.safe.integer(duration, 1, 17*3600000, "producer duration")
            provider = "assemblyai" if duration <= 10*3600000 else "revai"
            reason = "within_assemblyai_whole_recording_limit" if provider == "assemblyai" else "exceeds_assemblyai_duration_within_revai_limit"
            if (recording.get("state") != "ready" or row["provider"] != provider or row["reason"] != reason
                    or row["match_status"] != "missing" or row["import"] is not None
                    or row["maximum_cost_microusd"] != _cost(provider, duration, True)):
                raise RecoveryError("producer paid route or allowance differs")
        elif (row["disposition"] not in {"third_party", "review", "no_audio"}
              or row["provider"] is not None or row["maximum_cost_microusd"] != 0):
            raise RecoveryError("unsupported producer source disposition")
    return plan, code


def _listing(path):
    with io.paths.retained_directory(path) as directory:
        with os.scandir(directory) as entries:
            result = []
            for entry in entries:
                info = entry.stat(follow_symlinks=False)
                kind = "directory" if stat.S_ISDIR(info.st_mode) else "file" if stat.S_ISREG(info.st_mode) else None
                if kind is None or info.st_uid != os.geteuid() or (kind == "file" and info.st_nlink != 1):
                    raise RecoveryError("unsafe producer workspace entry")
                result.append([entry.name, kind])
        return sorted(result)


def _inventory(root):
    result = [{"path": str(root / "reservations"), "entries": _listing(root / "reservations")},
              {"path": str(root / "jobs"), "entries": _listing(root / "jobs")}]
    if len(result[1]["entries"]) > 10000:
        raise RecoveryError("producer job directory inventory exceeds bound")
    for name, kind in result[1]["entries"]:
        if kind != "directory":
            raise RecoveryError("unexpected file in producer jobs root")
        result.append({"path": str(root / "jobs" / name), "entries": _listing(root / "jobs" / name)})
    return result


def _verify_media(recording):
    with io.safe.opened(recording["media"]["path"]) as descriptor:
        observed = io.safe.witness(descriptor)
        if observed["st_size"] != recording["media"]["byte_count"]:
            raise RecoveryError("paid source size differs")
        if io.safe.hash_fd(descriptor, observed["st_size"], time.monotonic()+900) != recording["media"]["sha256"]:
            raise RecoveryError("paid source content differs from acquisition")


def _request_echo(provider, submission, terminal, upload, intent):
    if provider == "assemblyai":
        options = clients.assemblyai_options(upload["upload_url"], diarization=intent["diarization"])
        for raw in (submission, terminal):
            for name, expected in options.items():
                if name == "language_code":
                    if raw.get(name) not in {"en", "en_us", "en_uk", "en_au"}:
                        raise RecoveryError("paid AssemblyAI language echo differs")
                elif raw.get(name) != expected or isinstance(expected, bool) and raw.get(name) is not expected:
                    raise RecoveryError("paid AssemblyAI uploaded audio or option echo differs")
            if raw.get("language_detection") is not False:
                raise RecoveryError("paid AssemblyAI language-detection echo differs")
        return options
    options = clients.revai_options(intent["request_metadata"], diarization=intent["diarization"])
    for raw in (submission, terminal):
        for name, expected in options.items():
            if raw.get(name) != expected or isinstance(expected, bool) and raw.get(name) is not expected:
                raise RecoveryError("paid Rev AI intent fingerprint or option echo differs")
    return options


def _carryover(folder, plan_ref, prepared, decision, upload, proofs):
    path = folder / "unsubmitted-upload-carryover.json"
    if not io.safe.exists(path):
        return
    carried = _read(path, proofs)
    if (carried.get("kind") != "himr_unsubmitted_canary_carryover" or carried.get("schema_version") != 1
            or carried.get("new_plan") != plan_ref or carried.get("new_paid_requests") != 0
            or carried.get("old_paid_intent_exists") is not False or carried.get("old_paid_reservation_exists") is not False
            or carried.get("source_artifacts_changed") is not False):
        raise RecoveryError("unsubmitted upload carryover provenance differs")
    for reference in (carried["old_client"], carried["old_plan"], carried["original_prepared_audio"],
                      carried["original_screen"], carried["original_upload"]):
        io.read_bytes(reference)
        proofs[reference["path"]] = reference
    old_plan = io.read(carried["old_plan"])
    old_folder = Path(old_plan["state_root"]) / "jobs" / folder.name
    if (io.safe.exists(old_folder / "intent.json")
            or io.safe.exists(Path(old_plan["state_root"]) / "reservations" / (folder.name + ".json"))):
        raise RecoveryError("carryover predecessor has unaccounted paid work")
    old_audio = io.read(carried["original_prepared_audio"])
    expected_audio = {**old_audio, "audio": {**old_audio["audio"], "path": prepared["audio"]["path"]}}
    retained = {name: proofs[str(folder / (name + ".json"))] for name in ("audio", "screen", "upload")}
    if (carried.get("retained") != retained or prepared != expected_audio
            or io.read(carried["original_screen"]) != decision or io.read(carried["original_upload"]) != upload):
        raise RecoveryError("carried upload/audio/screen does not match original unsubmitted evidence")


def _transcript(row, audio, terminal, raw, bindings, decision):
    normalized = clients.normalize_result(row["provider"], raw, expected_duration_seconds=audio["duration_ms"]/1000,
                                           job=terminal, diarization=decision["diarization"])
    return {"kind": "himr_cloud_recording_transcript", "schema_version": 1, "job_id": row["job_id"],
        "recording_id": row["recording"]["recording_id"], "source_media": row["recording"]["media"],
        "status": "completed", "provider_job_id": terminal["id"], "raw_result": bindings["raw_result"],
        "provider_job": bindings["terminal"], "screen_decision": bindings["screen"], "audio": audio,
        "whole_recording_submitted": True,
        "normalizer_implementation_sha256": implementation()["cloud_transcription_client.py"],
        "machine_generated": True, "full_media_coverage_verified": False, "human_reviewed": False,
        "verified_quotation": False, "speaker_identity_inferred": False, "publication_authority": False,
        **normalized}


def inspect_producer(plan_ref, code_ref):
    """Validate all paid work before any admission can be published."""
    plan, code = producer_plan(plan_ref, code_ref)
    root = Path(plan["state_root"])
    before = _inventory(root)
    proofs = {ref["path"]: ref for ref in [plan_ref, code_ref, plan["inventory"], plan["screen_config"],
        plan["third_party_inventory"], plan["matching"], *code["files"].values()]}
    for reference in proofs.values():
        io.read_bytes(reference)
    marker = _read(root / "workspace.json", proofs)
    if (marker.get("kind") != PRODUCER_KIND + "_workspace" or marker.get("schema_version") != 1
            or marker.get("inventory") != plan["inventory"] or marker.get("screen_config") != plan["screen_config"]
            or marker.get("implementation") != plan["implementation"]):
        raise RecoveryError("producer workspace marker differs")
    matches = io.read(plan["matching"])
    if (matches.get("recording_ids") != [row["recording"]["recording_id"] for row in plan["recordings"]]
            or len(matches.get("matches", [])) != len(plan["recordings"])):
        raise RecoveryError("producer third-party selection coverage differs")
    rows = {row["job_id"]: row for row in plan["recordings"] if row["disposition"] == "cloud"}
    for name, kind in before[0]["entries"]:
        if kind != "file" or name.removesuffix(".json") not in rows or not name.endswith(".json"):
            raise RecoveryError("unaccounted producer reservation")
    paid, total = [], 0
    allowed = {"job.json", "screen.json", "screen-import.json", "screen-work", "audio.json", "audio.wav", "upload.json",
               "unsubmitted-upload-carryover.json", "intent.json", "submission.json", "reconciled.json",
               "terminal-job.json", "provider-transcript.json", "transcript.json", "completion.json"}
    for entry in before[2:]:
        folder = Path(entry["path"])
        identity = folder.name
        if identity not in rows or any(name not in allowed for name, _ in entry["entries"]):
            raise RecoveryError("unaccounted producer job or artifact")
        row = rows[identity]
        if _read(folder / "job.json", proofs) != {"plan": plan_ref, "recording": row}:
            raise RecoveryError("producer job marker differs")
        for name, kind in entry["entries"]:
            if kind == "file" and name.endswith(".json"):
                _read(folder / name, proofs)
        reservation_path = root / "reservations" / (identity + ".json")
        has_intent, reserved = io.safe.exists(folder / "intent.json"), io.safe.exists(reservation_path)
        if not has_intent and not reserved:
            if any(io.safe.exists(folder / name) for name in ("submission.json", "reconciled.json", "terminal-job.json", "completion.json", "provider-transcript.json")):
                raise RecoveryError("paid producer result lacks original intent")
            continue
        if not has_intent or not reserved:
            raise RecoveryError("producer has an incomplete paid reservation/intent")
        if len(paid) >= MAX_PAID_JOBS:
            raise RecoveryError("paid recovery exceeds job bound")
        intent = _read(folder / "intent.json", proofs)
        reservation = _read(reservation_path, proofs)
        prepared = _read(folder / "audio.json", proofs)
        decision = _read(folder / "screen.json", proofs)
        screen_ref = proofs[str(folder / "screen.json")]
        screen.validate_decision(decision, row["recording"], plan["screen_config"])
        audio = prepared["audio"]
        expected = {"kind": "himr_cloud_paid_intent", "schema_version": 1, "plan": plan_ref,
            "job_id": identity, "recording_id": row["recording"]["recording_id"], "provider": row["provider"],
            "audio": audio, "screen_decision": screen_ref, "diarization": decision["diarization"],
            "request_metadata": identity + "_" + io.digest({"audio": audio, "screen": screen_ref})[:24],
            "maximum_cost_microusd": _cost(row["provider"], row["recording"]["duration_ms"], decision["diarization"])}
        if intent != expected or reservation != expected:
            raise RecoveryError("original paid intent or reservation differs")
        if (prepared.get("kind") != "himr_cloud_prepared_audio" or prepared.get("schema_version") != 1
                or prepared.get("source") != row["recording"]["media"] or prepared.get("ffmpeg") != plan["ffmpeg"]
                or prepared.get("whole_recording") is not True or prepared.get("speech_filter") is not False
                or prepared.get("cuts") is not False or Path(audio["path"]) != folder / "audio.wav"):
            raise RecoveryError("producer upload audio provenance differs")
        _verify_media(row["recording"])
        if media.inspect_wav(folder / "audio.wav", row["recording"]["duration_ms"]) != audio:
            raise RecoveryError("producer uploaded audio content differs")
        proofs[audio["path"]] = {"path": audio["path"], "sha256": audio["sha256"]}
        submitted_name = "submission.json" if io.safe.exists(folder / "submission.json") else "reconciled.json"
        if not io.safe.exists(folder / submitted_name) or not io.safe.exists(folder / "terminal-job.json"):
            raise RecoveryError("producer paid job is pending or unreconciled")
        submission = _read(folder / submitted_name, proofs)
        terminal = _read(folder / "terminal-job.json", proofs)
        clients.validate_job(row["provider"], submission)
        clients.validate_job(row["provider"], terminal, expected_job_id=submission["id"])
        if terminal["status"] != ("completed" if row["provider"] == "assemblyai" else "transcribed"):
            raise RecoveryError("producer paid job did not succeed")
        upload = _read(folder / "upload.json", proofs) if row["provider"] == "assemblyai" else None
        if upload is not None:
            _carryover(folder, plan_ref, prepared, decision, upload, proofs)
        options = _request_echo(row["provider"], submission, terminal, upload, intent)
        raw_path = folder / ("terminal-job.json" if row["provider"] == "assemblyai" else "provider-transcript.json")
        raw = _read(raw_path, proofs)
        bindings = {"job": proofs[str(folder / "job.json")], "intent": proofs[str(folder / "intent.json")],
                    "reservation": proofs[str(reservation_path)], "audio": proofs[str(folder / "audio.json")],
                    "screen": screen_ref, "submission": proofs[str(folder / submitted_name)],
                    "terminal": proofs[str(folder / "terminal-job.json")], "raw_result": proofs[str(raw_path)]}
        if upload is not None:
            bindings["upload"] = proofs[str(folder / "upload.json")]
        transcript = _transcript(row, audio, terminal, raw, bindings, decision)
        amount = expected["maximum_cost_microusd"]
        total += amount
        paid.append({"recording": row["recording"], "producer_job_id": identity, "bindings": bindings,
                     "prior_reserved_microusd": amount, "request_options": options, "transcript": transcript})
    if {Path(item["bindings"]["reservation"]["path"]).name for item in paid} != {name for name, _ in before[0]["entries"]}:
        raise RecoveryError("not every producer paid reservation was recovered")
    if not paid:
        raise RecoveryError("producer has no successful paid jobs to recover")
    limit = _read(root / "spending-limit.json", proofs)
    io.safe.exact(limit, {"kind", "plan", "maximum_microusd", "scope", "automatic_hold_release"}, "old spending limit")
    if (limit["kind"] != "himr_cloud_spending_limit" or limit["plan"] != plan_ref
            or limit["scope"] != "this_workspace_not_provider_account" or limit["automatic_hold_release"] is not False
            or type(limit["maximum_microusd"]) is not int or not 0 < total <= limit["maximum_microusd"]):
        raise RecoveryError("old spending allowance does not cover all paid jobs")
    if _inventory(root) != before:
        raise RecoveryError("producer job or reservation inventory changed during recovery")
    return {"kind": KIND + "_inspection", "schema_version": 1, "producer_plan": plan_ref,
            "producer_code": code_ref, "paid_jobs": paid, "prior_reserved_microusd": total,
            "producer_inventory": before, "proofs": sorted(proofs.values(), key=lambda ref: ref["path"]),
            "implementation": implementation(), "semantics": SEMANTICS}


def _completion(item, transcript_ref):
    value, bindings = item["transcript"], item["bindings"]
    return {"kind": "himr_cloud_transcription_completion", "schema_version": 1,
            "job_id": item["producer_job_id"], "audio": value["audio"], "raw_result": bindings["raw_result"],
            "provider_job": bindings["terminal"], "transcript": transcript_ref, "screen_decision": bindings["screen"]}


def recover_completed(plan_ref, code_ref, out_root):
    out_root = io.safe.path_value(out_root)
    if io.safe.exists(out_root):
        raise RecoveryError("paid recovery requires a fresh private output root")
    inspection = inspect_producer(plan_ref, code_ref)
    io.protect(out_root, inspection)
    io.mkdir(out_root)
    provenance = io.put(out_root / "provenance.json", inspection)
    admissions = []
    for item in inspection["paid_jobs"]:
        folder = out_root / item["producer_job_id"]
        io.mkdir(folder)
        transcript = io.put(folder / "transcript.json", item["transcript"])
        completion = io.put(folder / "completion.json", _completion(item, transcript))
        admission = {"kind": KIND + "_admission", "schema_version": 1, "recording": item["recording"],
            "producer_plan": plan_ref, "producer_code": code_ref, "producer_job_id": item["producer_job_id"],
            "transcript": transcript, "completion": completion, "provenance": provenance,
            "prior_reserved_microusd": item["prior_reserved_microusd"], "implementation": implementation(),
            "semantics": SEMANTICS}
        reference = io.put(folder / "admission.json", admission)
        admissions.append({"recording_id": item["recording"]["recording_id"], "admission": reference})
    if _inventory(Path(io.read(plan_ref)["state_root"])) != inspection["producer_inventory"]:
        raise RecoveryError("producer changed before recovery catalog publication")
    return io.put(out_root / "catalog.json", {"kind": KIND + "_catalog", "schema_version": 1,
        "producer_plan": plan_ref, "producer_code": code_ref, "provenance": provenance,
        "admissions": admissions, "prior_reserved_microusd": inspection["prior_reserved_microusd"],
        "implementation": implementation(), "semantics": SEMANTICS})


def _replay_provenance(reference):
    saved = io.read(reference)
    if saved.get("implementation") != implementation() or saved.get("semantics") != SEMANTICS:
        raise RecoveryError("paid admission implementation or policy differs")
    expected = inspect_producer(saved["producer_plan"], saved["producer_code"])
    if saved != expected:
        raise RecoveryError("paid admission producer provenance does not replay")
    return expected


def _check_admission(value, reference, item, inspection, provenance):
    io.safe.exact(value, {"kind", "schema_version", "recording", "producer_plan", "producer_code",
        "producer_job_id", "transcript", "completion", "provenance", "prior_reserved_microusd",
        "implementation", "semantics"}, "paid admission")
    folder = Path(provenance["path"]).parent / item["producer_job_id"]
    if (value["kind"] != KIND + "_admission" or type(value["schema_version"]) is not int or value["schema_version"] != 1
            or value["implementation"] != implementation() or value["semantics"] != SEMANTICS
            or value["provenance"] != provenance or value["recording"] != item["recording"]
            or value["producer_plan"] != inspection["producer_plan"] or value["producer_code"] != inspection["producer_code"]
            or value["producer_job_id"] != item["producer_job_id"]
            or value["prior_reserved_microusd"] != item["prior_reserved_microusd"]
            or Path(reference["path"]) != folder / "admission.json"
            or Path(value["transcript"]["path"]) != folder / "transcript.json"
            or Path(value["completion"]["path"]) != folder / "completion.json"
            or io.read(value["transcript"]) != item["transcript"]
            or io.read(value["completion"]) != _completion(item, value["transcript"])):
        raise RecoveryError("paid admission normalized output, provenance, or reservation differs")


def load_admission(reference, recording):
    value = io.read(reference)
    if (value.get("kind") != KIND + "_admission" or value.get("schema_version") != 1
            or value.get("recording") != recording or value.get("implementation") != implementation()
            or value.get("semantics") != SEMANTICS):
        raise RecoveryError("paid admission recording, implementation, or policy differs")
    inspection = _replay_provenance(value["provenance"])
    candidates = [item for item in inspection["paid_jobs"] if item["recording"] == recording]
    if len(candidates) != 1:
        raise RecoveryError("paid admission does not select exactly one producer recording")
    item = candidates[0]
    _check_admission(value, reference, item, inspection, value["provenance"])
    return {key: value[key] for key in ("transcript", "completion", "prior_reserved_microusd", "provenance")}


def load_catalog(reference):
    value = io.read(reference)
    io.safe.exact(value, {"kind", "schema_version", "producer_plan", "producer_code", "provenance",
                         "admissions", "prior_reserved_microusd", "implementation", "semantics"}, "paid recovery catalog")
    if (value.get("kind") != KIND + "_catalog" or value.get("schema_version") != 1
            or value.get("implementation") != implementation() or value.get("semantics") != SEMANTICS):
        raise RecoveryError("paid recovery catalog implementation or policy differs")
    inspection = _replay_provenance(value["provenance"])
    expected = {item["recording"]["recording_id"]: item for item in inspection["paid_jobs"]}
    entries = value.get("admissions")
    if (not isinstance(entries, list) or len(entries) != len(expected)
            or {entry["recording_id"] for entry in entries} != set(expected)
            or value["producer_plan"] != inspection["producer_plan"] or value["producer_code"] != inspection["producer_code"]
            or value["prior_reserved_microusd"] != inspection["prior_reserved_microusd"]
            or Path(reference["path"]) != Path(value["provenance"]["path"]).parent / "catalog.json"):
        raise RecoveryError("paid catalog omits producer jobs or loses reservations")
    for entry in entries:
        admission = io.read(entry["admission"])
        item = expected[entry["recording_id"]]
        _check_admission(admission, entry["admission"], item, inspection, value["provenance"])
    return value


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--producer-plan", required=True)
    parser.add_argument("--expected-sha256", required=True)
    parser.add_argument("--producer-code", required=True)
    parser.add_argument("--producer-code-sha256", required=True)
    parser.add_argument("--output-root", required=True)
    args = parser.parse_args(argv)
    try:
        io.safe.deny_internet()
        reference = recover_completed({"path": args.producer_plan, "sha256": args.expected_sha256},
            {"path": args.producer_code, "sha256": args.producer_code_sha256}, args.output_root)
        value = load_catalog(reference)
        print(json.dumps({"catalog": reference, "recovered_paid_jobs": len(value["admissions"]),
                          "prior_reserved_microusd": value["prior_reserved_microusd"], "new_paid_requests": 0}, sort_keys=True))
        return 0
    except (RuntimeError, ValueError, OSError, KeyError, TypeError) as error:
        print("CloudRecoveryError: " + str(error), file=sys.stderr)
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
