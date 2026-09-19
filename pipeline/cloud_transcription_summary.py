"""Separate finite Gemini transcript-summary worker for preferred cloud sources.

Preparation/status/export are offline. A dedicated root lock serializes global
pre-POST reservations across independent one-recording summary plans without
locking acquisition, transcription or screening. Existing summary receipt,
evidence, classification and usage validators remain authoritative. Unknown paid
submissions are held, never retried or silently replaced by another provider.
"""
from __future__ import annotations

import argparse
from collections import Counter
from copy import deepcopy
import hashlib
from pathlib import Path
import signal
import time

from pipeline import transcript_summary as r
from pipeline import transcript_summary_campaign as accounting
from pipeline import cloud_transcription as cloud


class SummaryWorkerError(RuntimeError):
    pass


KIND = "himr_cloud_transcript_summary_worker"
CONFIG = {**r.core.DEFAULT_CONFIG, "timeline_profile": "gemini_flash_batch",
          "max_chunk_input_bytes": 26000, "max_evidence_refs_per_item": 256,
          "gemini_schema_policy": "local_array_bounds_v2",
          "transcript_input_policy": "text_and_speaker_evidence_v1"}
MAX_RECORDS = 10000
MAX_RESERVATIONS = 50000
POLICY = {"phase": "transcripts", "automatic_paid_retries": False,
          "source_mutation": False, "publication_authority": False,
          "old_summary_campaign_mutation": False, "max_attempts_per_job": 1,
          "usage_accounting": "validated_terminal_usage_plus_all_unknown_holds",
          "model_input": "text_and_speaker_evidence_v1",
          "anonymous_label_handling": "hold_for_speaker_identity"}


def implementation():
    return {**cloud.implementation(), "cloud_transcription_summary.py":
            hashlib.sha256(Path(__file__).read_bytes()).hexdigest(),
            "transcript_summary_campaign.py": hashlib.sha256(Path(accounting.__file__).read_bytes()).hexdigest()}


def prepare(cloud_plan_ref, state_root, *, max_total_budget_microusd, config=None):
    """Create a distinct immutable worker; neither submit nor restart anything."""
    cloud.load_plan(cloud_plan_ref)
    config = r.core.normalize_config(CONFIG if config is None else config)
    if (config["transcript_profile"] != "gemini_flash_batch" or "broader_synthesis" in config
            or config.get("transcript_input_policy") != POLICY["model_input"]
            or config.get("gemini_schema_policy") != "local_array_bounds_v2"):
        raise SummaryWorkerError("worker requires transcript-only Gemini with stripped-input and repaired schema policies")
    r.safe.integer(max_total_budget_microusd, 1, 10_000_000_000, "global Gemini budget")
    root = r.safe.path_value(state_root)
    r.protect(root, {"cloud_plan": cloud_plan_ref})
    cloud_root = Path(cloud_plan_ref["path"]).parent
    if root == cloud_root or root in cloud_root.parents or cloud_root in root.parents:
        raise SummaryWorkerError("summary stage requires a separate nonoverlapping workspace")
    cloud.private_root(root)
    manifest = {"kind": KIND, "schema_version": 1, "cloud_plan": cloud_plan_ref,
                "state_root": str(root), "config": config,
                "max_total_budget_microusd": max_total_budget_microusd,
                "implementation": implementation(), "policy": POLICY}
    with r.locked(root):
        if not r.safe.exists(root / "workspace.json") and any(p.name != "execution.lock" for p in root.iterdir()):
            raise SummaryWorkerError("refusing a nonempty unmarked summary-worker workspace")
        r.put(root / "workspace.json", manifest)
        for name in ("entries", "requests", "records", "reservations", "exports"):
            r.mkdir(root / name)
        ref = r.put(root / "manifest.json", manifest)
    return {"state": "prepared_offline", "manifest": ref, "new_paid_requests": 0,
            "max_total_budget_microusd": max_total_budget_microusd, "phase": "transcripts"}


def load_manifest(ref):
    value = r.read(ref)
    r.safe.exact(value, {"kind", "schema_version", "cloud_plan", "state_root", "config",
                         "max_total_budget_microusd", "implementation", "policy"}, "summary worker")
    if (value["kind"] != KIND or type(value["schema_version"]) is not int or value["schema_version"] != 1
            or value["implementation"] != implementation() or value["policy"] != POLICY):
        raise SummaryWorkerError("summary worker implementation or policy changed")
    root = r.safe.path_value(value["state_root"])
    if str(root / "manifest.json") != ref["path"] or r.read(r.binding(root / "workspace.json")) != value:
        raise SummaryWorkerError("summary worker location or marker differs")
    r.safe.integer(value["max_total_budget_microusd"], 1, 10_000_000_000, "global Gemini budget")
    if r.core.normalize_config(value["config"]) != value["config"]:
        raise SummaryWorkerError("summary config changed")
    if (value["config"]["transcript_profile"] != "gemini_flash_batch"
            or "broader_synthesis" in value["config"]
            or value["config"].get("transcript_input_policy") != POLICY["model_input"]
            or value["config"].get("gemini_schema_policy") != "local_array_bounds_v2"):
        raise SummaryWorkerError("worker config does not enforce transcript-only stripped Gemini inputs")
    cloud.load_plan(value["cloud_plan"])
    return value


def _available(manifest):
    exported = cloud.export(manifest["cloud_plan"])
    if exported.get("kind") != "himr_preferred_transcript_selection" or exported.get("plan") != manifest["cloud_plan"]:
        raise SummaryWorkerError("preferred transcript selection differs")
    records = exported.get("records")
    if not isinstance(records, list) or len(records) > MAX_RECORDS:
        raise SummaryWorkerError("preferred source count exceeds its bound")
    result = {}
    for row in records:
        if not isinstance(row, dict) or row.get("format") not in {"third_party", "cloud"}:
            raise SummaryWorkerError("summary worker accepts only explicit preferred transcript formats")
        if row["recording_id"] in result:
            raise SummaryWorkerError("preferred selection repeats a recording")
        result[row["recording_id"]] = row
    return result


def _identity_holds(available):
    """Inspect hash-bound canonical labels without inventing named identities.

    The producer export has already replayed completed cloud/import artifacts.
    This gate reads their exact bytes and typed label fields, not a caller's
    diarization flag. Full source/evidence normalization still runs when a source
    is admitted to an actual summary plan; we need not build evidence IDs for
    every subtitle in the archive merely to decide whether it must wait.
    """
    held = {}
    kinds = {"third_party": "himr_third_party_transcript_import", "cloud": "himr_cloud_recording_transcript"}
    for recording, source in available.items():
        doc = r.read(source["transcript"])
        if (doc.get("kind") != kinds[source["format"]] or type(doc.get("schema_version")) is not int
                or doc["schema_version"] != 1 or doc.get("recording_id") != recording
                or doc.get("status") != "completed"):
            raise SummaryWorkerError("identity gate requires a typed complete preferred transcript")
        if source["format"] == "cloud" and (type(doc.get("diarization_requested")) is not bool
                                             or doc.get("machine_generated") is not True):
            raise SummaryWorkerError("cloud identity gate lacks explicit generation/diarization semantics")
        segments = doc.get("segments")
        if not isinstance(segments, list) or len(segments) > r.sources_module.MAX_SEGMENTS:
            raise SummaryWorkerError("identity gate segment list exceeds its bound")
        labels, count = set(), 0
        for segment in segments:
            if not isinstance(segment, dict) or "speaker" not in segment:
                raise SummaryWorkerError("identity gate segment lacks an explicit speaker field")
            label = segment["speaker"]
            if label is None:
                continue
            if not isinstance(label, str) or not r.sources_module.SPEAKER.fullmatch(label):
                raise SummaryWorkerError("unsupported speaker identity mapping; no implicit naming bypass")
            labels.add(label)
            count += 1
        if count:
            held[recording] = {"recording_id": recording, "reason": "speaker_identity_pending",
                "format": source["format"], "transcript": source["transcript"],
                "anonymous_segment_count": count, "anonymous_label_count": len(labels)}
    return held


def _record_key(recording_id):
    return "summaryrecord_" + r.digest({"recording_id": recording_id})[:32]


def _request(manifest, source):
    # Dates, titles and provider/acquisition details remain in the entry's source
    # receipt for later synthesis, never in transcript-level model evidence.
    spec = {"recording_id": source["recording_id"], "format": source["format"],
            "transcript": source["transcript"], "completion": source.get("completion"),
            "title": None, "date": None}
    return {"kind": "himr_transcript_summary_request", "schema_version": 1,
        "state_root": str(Path(manifest["state_root"]) / "records" / _record_key(source["recording_id"])),
        "sources": [spec], "config": manifest["config"],
        "classification_policy": "conservative_evidence_inheritance_v1",
        "limits": {**r.DEFAULT_LIMITS, "max_sources": 1, "max_waves": 256},
        "budget": {"max_reserved_microusd": manifest["max_total_budget_microusd"], "max_attempts_per_job": 1},
        "cloud": {"processing_approved": True, "paid_tier_confirmed": True}}


def _ensure_record(manifest, source):
    root = Path(manifest["state_root"])
    key = _record_key(source["recording_id"])
    request = _request(manifest, source)
    request_ref = r.put(root / "requests" / (key + ".json"), request)
    path = Path(request["state_root"]) / "plan.json"
    if r.safe.exists(path):
        plan_ref = r.binding(path)
        plan, _ = r.load_plan(plan_ref["path"], plan_ref["sha256"])
        if plan["request_value"] != request:
            raise SummaryWorkerError("recording summary request changed")
    else:
        plan_ref = r.create_plan(request_ref["path"], request_ref["sha256"])["plan"]
    entry = {"kind": KIND + "_record", "schema_version": 1, "source": source,
             "request": request_ref, "plan": plan_ref}
    r.put(root / "entries" / (key + ".json"), entry)
    return entry


def _json_names(folder, maximum):
    with r.paths.retained_directory(folder) as descriptor:
        import os
        with os.scandir(descriptor) as entries:
            names = [row.name for row in entries if row.name.endswith(".json")]
    if len(names) > maximum:
        raise SummaryWorkerError("summary worker artifact inventory exceeds its bound")
    return sorted(names)


def _reservation(worker_ref, entry, wave):
    return {"kind": KIND + "_reservation", "schema_version": 1,
        "worker": worker_ref, "record_plan": entry["plan"], "wave_id": wave["wave_id"],
        "input_sha256": wave["input_sha256"], "maximum_cost_microusd": wave["maximum_cost_microusd"]}


def _snapshot(manifest, worker_ref):
    root, available = Path(manifest["state_root"]), _available(manifest)
    identity_holds = _identity_holds(available)
    records, waves = {}, {}
    settled = held = 0
    for name in _json_names(root / "entries", MAX_RECORDS):
        entry = r.read(r.binding(root / "entries" / name))
        r.safe.exact(entry, {"kind", "schema_version", "source", "request", "plan"}, "summary record entry")
        source = entry["source"]
        recording = source["recording_id"]
        if (entry["kind"] != KIND + "_record" or type(entry["schema_version"]) is not int
                or entry["schema_version"] != 1 or name != _record_key(recording) + ".json"
                or available.get(recording) != source):
            raise SummaryWorkerError("record entry differs from its preferred complete transcript")
        expected = _request(manifest, source)
        if r.read(entry["request"]) != expected:
            raise SummaryWorkerError("record summary request differs from the global worker policy")
        plan, sources = r.load_plan(entry["plan"]["path"], entry["plan"]["sha256"])
        if plan["request"] != entry["request"] or plan["request_value"] != expected:
            raise SummaryWorkerError("record summary plan differs")
        state = r.load_state(plan, sources)
        progress = r.status_from_state(plan, sources, state, phase="transcripts")
        accounted = accounting.accounted_state(plan, state)
        settled += accounted["usage_estimate_microusd"]
        held += accounted["unsettled_hold_microusd"]
        # Retain only compact progress/receipt metadata between recordings.
        # Keeping every normalized source and dependency graph here would grow
        # memory with the archive's millions of subtitle cues.
        records[recording] = {"entry": entry, "status": progress}
        for wave in state["waves"]:
            if wave["wave_id"] in waves:
                raise SummaryWorkerError("global summary wave identity is duplicated")
            if wave["retry_of"] is not None or wave["provider"] != "gemini" or any(
                    job["stage"] not in {"chunk", "transcript"} for job in wave["jobs"]):
                raise SummaryWorkerError("worker contains an unauthorized retry/provider/summary stage")
            waves[wave["wave_id"]] = (entry, r.wave_folder(plan, wave["wave_id"]),
                {key: wave[key] for key in ("wave_id", "input_sha256", "maximum_cost_microusd")})
    reservations, orphan_reservations = {}, []
    for name in _json_names(root / "reservations", MAX_RESERVATIONS):
        saved = r.read(r.binding(root / "reservations" / name))
        wave_id = saved.get("wave_id")
        if wave_id not in waves or name != wave_id + ".json":
            raise SummaryWorkerError("global reservation has no exact retained summary wave")
        entry, folder, wave = waves[wave_id]
        if saved != _reservation(worker_ref, entry, wave):
            raise SummaryWorkerError("global reservation differs from its source/wave/cost")
        reservations[wave_id] = saved
        if not r.safe.exists(folder / "submit-intent.json"):
            held += wave["maximum_cost_microusd"]
            orphan_reservations.append({"recording_id": entry["source"]["recording_id"],
                "plan": entry["plan"], "state": "orphan_global_reservation",
                "ambiguous_waves": [wave_id], "failed_jobs": 0})
            # Even if the interruption occurred before POST, missing local
            # evidence cannot establish that after a disk failure. Never turn
            # a previously durable paid reservation into an automatic retry.
            records[entry["source"]["recording_id"]]["status"]["state"] = "needs_reconciliation"
    for wave_id, (_, folder, wave) in waves.items():
        if r.safe.exists(folder / "submit-intent.json") and wave_id not in reservations:
            raise SummaryWorkerError("paid child wave bypassed the worker's global reservation")
    if settled + held > manifest["max_total_budget_microusd"]:
        raise SummaryWorkerError("existing validated Gemini usage and holds exceed the worker budget")
    return {"available": available, "records": records, "waves": waves, "reservations": reservations,
            "orphan_reservations": orphan_reservations,
            "identity_holds": identity_holds,
            "usage_estimate_microusd": settled, "unsettled_hold_microusd": held,
            "accounted_microusd": settled + held}


def _public(manifest, snapshot):
    counts = Counter(row["status"]["state"] for row in snapshot["records"].values())
    retained_complete = sum(row["status"]["transcript_phase_complete"] for row in snapshot["records"].values())
    complete = sum(row["status"]["transcript_phase_complete"]
                   for recording, row in snapshot["records"].items() if recording not in snapshot["identity_holds"])
    pending = sum(len(row["status"]["pending_waves"]) for row in snapshot["records"].values())
    unknown = sum(len(row["status"]["ambiguous_waves"]) for row in snapshot["records"].values())
    unknown += len(snapshot["orphan_reservations"])
    holds = [{"recording_id": key, "plan": row["entry"]["plan"],
              "state": row["status"]["state"], "ambiguous_waves": row["status"]["ambiguous_waves"],
              "failed_jobs": row["status"]["failed_jobs"]}
             for key, row in snapshot["records"].items()
             if row["status"]["ambiguous_waves"] or row["status"]["failed_jobs"]]
    holds.extend(snapshot["orphan_reservations"])
    return {"kind": KIND + "_status", "phase": "transcripts", "counts": dict(counts),
        "preferred_transcripts_available": len(snapshot["available"]),
        "recording_plans": len(snapshot["records"]), "transcript_summaries_complete": complete,
        "retained_transcript_summaries_complete": retained_complete,
        "waiting_for_summary_admission": len(set(snapshot["available"]) - set(snapshot["records"]) - set(snapshot["identity_holds"])),
        "speaker_identity_pending": len(snapshot["identity_holds"]),
        "speaker_identity_pending_recording_ids": sorted(snapshot["identity_holds"]),
        "speaker_identity_holds": [snapshot["identity_holds"][key] for key in sorted(snapshot["identity_holds"])],
        "pending_waves": pending, "potential_active_waves": pending + unknown, "holds": holds,
        **{key: snapshot[key] for key in ("usage_estimate_microusd", "unsettled_hold_microusd", "accounted_microusd")},
        "max_total_budget_microusd": manifest["max_total_budget_microusd"], "automatic_paid_retries": False}


def status(worker_ref):
    manifest = load_manifest(worker_ref)
    with r.locked(Path(manifest["state_root"])):
        return _public(manifest, _snapshot(manifest, worker_ref))


def cycle(worker_ref, *, allow_paid_api=False, max_active=4, max_new_waves=4,
          env_file=None, client=None, stopping=lambda: False):
    if not allow_paid_api:
        raise SummaryWorkerError("summary run requires explicit --allow-paid-api")
    r.safe.integer(max_active, 1, 8, "active Gemini wave limit")
    r.safe.integer(max_new_waves, 0, 8, "new Gemini wave limit")
    manifest = load_manifest(worker_ref)
    root = Path(manifest["state_root"])
    submitted = attempted = 0
    errors = []
    with r.locked(root):
        snapshot = _snapshot(manifest, worker_ref)
        for row in snapshot["records"].values():
            ref = row["entry"]["plan"]
            for wave in row["status"]["pending_waves"]:
                if stopping():
                    break
                try:
                    r.poll_wave(ref["path"], ref["sha256"], wave, client=client, env_file=env_file)
                except r.client_module.BatchClientError:
                    errors.append({"operation": "poll", "wave_id": wave, "state": "transport_error_retained"})
        snapshot = _snapshot(manifest, worker_ref)
        active_records = sum(not row["status"]["transcript_phase_complete"]
                             and row["status"]["state"] != "needs_review"
                             and (recording not in snapshot["identity_holds"] or bool(row["status"]["pending_waves"])
                                  or bool(row["status"]["ambiguous_waves"]))
                             for recording, row in snapshot["records"].items())
        if max_new_waves and not stopping():
            for recording, source in snapshot["available"].items():
                if active_records >= max_active or stopping():
                    break
                if recording not in snapshot["records"] and recording not in snapshot["identity_holds"]:
                    _ensure_record(manifest, source)
                    active_records += 1
        snapshot = _snapshot(manifest, worker_ref)
        pending = _public(manifest, snapshot)["potential_active_waves"]
        accounted = snapshot["accounted_microusd"]
        budget_paused = False
        for recording, row in snapshot["records"].items():
            ref = row["entry"]["plan"]
            progress = row["status"]
            # Previously submitted batches were polled above to retain paid
            # output. Anonymous sources never receive a new chunk/reducer wave
            # or a preferred summary export under this worker policy.
            if recording in snapshot["identity_holds"]:
                continue
            if progress["transcript_phase_complete"]:
                r.export_plan(ref["path"], ref["sha256"], phase="transcripts")
                continue
            if (stopping() or attempted >= max_new_waves or pending >= max_active
                    or progress["state"] in {"needs_review", "needs_reconciliation"}):
                continue
            prepared = r.prepare_plan(ref["path"], ref["sha256"], phase="transcripts")
            if not prepared.get("wave_id"):
                continue
            wave_id = prepared["wave_id"]
            current_plan, sources = r.load_plan(ref["path"], ref["sha256"])
            state = r.load_state(current_plan, sources)
            wave = next(value for value in state["waves"] if value["wave_id"] == wave_id)
            reservation = _reservation(worker_ref, row["entry"], wave)
            ledger_path = root / "reservations" / (wave_id + ".json")
            if r.safe.exists(ledger_path):
                if r.read(r.binding(ledger_path)) != reservation:
                    raise SummaryWorkerError("existing global wave reservation differs")
            else:
                if accounted + wave["maximum_cost_microusd"] > manifest["max_total_budget_microusd"]:
                    budget_paused = True
                    continue
                r.put(ledger_path, reservation)
                accounted += wave["maximum_cost_microusd"]
            if stopping():
                break
            try:
                result = r.submit_wave(ref["path"], ref["sha256"], wave_id, allow_paid_api=True,
                                       client=client, env_file=env_file, phase="transcripts")
                if result["state"] == "submitted":
                    submitted += 1
                    attempted += 1
                    pending += 1
            except r.client_module.BatchClientError:
                # The ledger plus the original runner's durable intent remain
                # authoritative. Neither this nor a later cycle repeats POST.
                attempted += 1
                pending += 1
                errors.append({"operation": "submit", "wave_id": wave_id, "state": "submission_evidence_retained"})
        final = _snapshot(manifest, worker_ref)
        public = _public(manifest, final)
        state = ("paused" if stopping() else "budget_paused" if budget_paused else
                 "waiting_remote" if public["pending_waves"] else
                 "needs_review" if public["holds"] and public["potential_active_waves"] >= max_active else
                 "ready" if public["waiting_for_summary_admission"] else
                 "needs_review" if public["holds"] else
                 "waiting_for_speaker_identity" if public["speaker_identity_pending"] else
                 "waiting_for_preferred_transcripts")
        return {**public, "state": state, "new_paid_requests": attempted,
                "confirmed_new_submissions": submitted, "transport_events": errors}


def export(worker_ref):
    manifest = load_manifest(worker_ref)
    root = Path(manifest["state_root"])
    with r.locked(root):
        snapshot = _snapshot(manifest, worker_ref)
        records = []
        for recording, row in snapshot["records"].items():
            if not row["status"]["transcript_phase_complete"] or recording in snapshot["identity_holds"]:
                continue
            ref = row["entry"]["plan"]
            artifact = r.export_plan(ref["path"], ref["sha256"], phase="transcripts")
            records.append({"recording_id": recording, "source": row["entry"]["source"],
                            "summary_plan": ref, "summary_export": artifact["artifact"]})
        document = {"kind": KIND + "_export", "schema_version": 1, "worker": worker_ref,
                    "phase": "transcripts", "records": records,
                    "speaker_identity_holds": list(snapshot["identity_holds"].values()),
                    "publication_authority": False}
        output = r.put(root / "exports" / ("summaries-" + r.digest(document)[:32] + ".json"), document)
        return {"state": "exported_private", "artifact": output, "transcript_summaries": len(records),
                "speaker_identity_pending": len(snapshot["identity_holds"])}


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    commands = parser.add_subparsers(dest="command", required=True)
    create = commands.add_parser("prepare")
    create.add_argument("--cloud-plan", required=True)
    create.add_argument("--expected-sha256", required=True)
    create.add_argument("--state-root", required=True)
    create.add_argument("--budget-usd", type=cloud.usd, required=True)
    for name in ("cycle", "run", "status", "export"):
        command = commands.add_parser(name)
        command.add_argument("--manifest", required=True)
        command.add_argument("--expected-sha256", required=True)
        if name in {"cycle", "run"}:
            command.add_argument("--allow-paid-api", action="store_true")
            command.add_argument("--max-active", type=int, default=4)
            command.add_argument("--max-new-waves", type=int, default=4)
            command.add_argument("--env-file")
        if name == "run":
            command.add_argument("--max-cycles", type=int, default=1440)
            command.add_argument("--poll-seconds", type=int, default=60)
    args = parser.parse_args(argv)
    if args.command == "prepare":
        result = prepare({"path": args.cloud_plan, "sha256": args.expected_sha256}, args.state_root,
                         max_total_budget_microusd=args.budget_usd)
    else:
        ref = {"path": args.manifest, "sha256": args.expected_sha256}
        if args.command == "status":
            result = status(ref)
        elif args.command == "export":
            result = export(ref)
        else:
            paused = [False]
            def pause(_signum, _frame):
                paused[0] = True
            for sig in (signal.SIGTERM, signal.SIGINT):
                signal.signal(sig, pause)
            count = 1 if args.command == "cycle" else args.max_cycles
            r.safe.integer(count, 1, 10080, "finite summary cycle limit")
            if args.command == "run":
                r.safe.integer(args.poll_seconds, 10, 300, "summary polling interval")
            for ordinal in range(count):
                result = cycle(ref, allow_paid_api=args.allow_paid_api, max_active=args.max_active,
                               max_new_waves=args.max_new_waves, env_file=args.env_file, stopping=lambda: paused[0])
                print(r.canonical(result).decode().strip(), flush=True)
                if paused[0] or result["state"] in {"budget_paused", "needs_review"}:
                    break
                if ordinal + 1 < count:
                    deadline = time.monotonic() + args.poll_seconds
                    while not paused[0] and time.monotonic() < deadline:
                        time.sleep(min(1, max(0, deadline - time.monotonic())))
            return 2 if result["state"] == "needs_review" else 0
    print(r.canonical(result).decode().strip(), flush=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
