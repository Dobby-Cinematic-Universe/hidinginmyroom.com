"""Finite Gemini-only campaign over explicitly selected immutable transcript shards.

No discovery, paid retries, Sonnet, media uploads, or publication. The existing
runner owns per-wave intents and reservations. This controller adds a campaign
limit: terminal, validated usage plus conservative holds for every other intent.
Usage is a token-based estimate, not an invoice or an account-wide billing cap.
"""
from __future__ import annotations

import argparse
from contextlib import contextmanager
import datetime as dt
import hashlib
import os
from pathlib import Path
import signal
import sys
import time
import uuid

from pipeline import transcript_summary as runner
from pipeline import transcript_summary_core as core
from pipeline import transcript_summary_reader as reader

KIND = "himr_gemini_transcript_campaign"
MARKER = {"kind": KIND + "_workspace", "schema_version": 1}


def implementation():
    return {**runner.implementation(), **{
        name: hashlib.sha256((Path(__file__).parent / name).read_bytes()).hexdigest()
        for name in ("transcript_summary_campaign.py", "transcript_summary_reader.py")}}


def validate_manifest(value):
    fields = {"kind", "schema_version", "state_root", "shards", "config",
        "budget_microusd", "max_active_shards", "poll_seconds", "max_runtime_seconds",
        "cloud", "implementation"}
    if isinstance(value, dict) and "selection" in value:
        fields.add("selection")
        runner.safe.file_binding(value["selection"])
    if isinstance(value, dict) and "recovery" in value:
        fields.add("recovery")
    if isinstance(value, dict) and "classification_policy" in value:
        from pipeline import transcript_summary_classification as classification
        fields.add("classification_policy")
        if value["classification_policy"] != classification.POLICY:
            raise runner.Error("unsupported campaign classification policy")
    runner.safe.exact(value, fields, "Gemini campaign")
    if value["kind"] != KIND or type(value["schema_version"]) is not int or value["schema_version"] != 1:
        raise runner.Error("unsupported Gemini campaign")
    runner.safe.path_value(value["state_root"])
    config = core.normalize_config(value["config"])
    if config["transcript_profile"] != "gemini_flash_batch" or "broader_synthesis" in config:
        raise runner.Error("campaign admits Gemini transcript summaries only")
    if config["timeline_profile"] != "gemini_flash_batch":
        raise runner.Error("unused synthesis profile must also be Gemini; synthesis is never run")
    for name, low, high in (("budget_microusd", 1, 120_000_000),
                            ("max_active_shards", 1, 4), ("poll_seconds", 15, 300),
                            ("max_runtime_seconds", 60, 14 * 86400)):
        runner.safe.integer(value[name], low, high, name)
    if (value["cloud"] != {"processing_approved": True, "paid_tier_confirmed": True} or
            any(type(flag) is not bool for flag in value["cloud"].values())):
        raise runner.Error("campaign requires explicit paid cloud approval")
    if value["implementation"] != implementation():
        raise runner.Error("campaign implementation changed")
    shards = value["shards"]
    if not isinstance(shards, list) or not 1 <= len(shards) <= 512:
        raise runner.Error("campaign needs a finite shard selection")
    seen = set()
    for shard in shards:
        if not isinstance(shard, list) or not 1 <= len(shard) <= 32:
            raise runner.Error("campaign shard requires 1..32 transcripts")
        for spec in shard:
            runner.sources_module.validate_spec(spec)
            if spec["recording_id"] in seen:
                raise runner.Error("campaign contains duplicate recording revisions")
            seen.add(spec["recording_id"])
    if len(seen) > 4096:
        raise runner.Error("campaign source limit exceeded")
    if "recovery" in value:
        recovery = value["recovery"]
        recovery_fields = {"producer_manifest", "producer_code", "imports", "previous_completed",
            "prior_usage_estimate_microusd", "prior_unsettled_hold_microusd", "producer_budget_microusd",
            "maximum_additional_paid_attempts_per_failed_chunk"}
        if isinstance(recovery, dict) and "schema_repair" in recovery:
            recovery_fields.add("schema_repair")
            repair = recovery["schema_repair"]
            runner.safe.exact(repair, {"producer_manifest", "producer_code", "diagnostic"}, "schema repair")
            for key in ("producer_manifest", "producer_code"):
                runner.safe.file_binding(repair[key])
            if repair["diagnostic"] is not None:
                runner.safe.file_binding(repair["diagnostic"])
            if config.get("gemini_schema_policy") != "local_array_bounds_v2":
                raise runner.Error("schema repair requires local array bounds")
        if isinstance(recovery, dict) and "continuation" in recovery:
            from pipeline import transcript_summary_classification as classification
            recovery_fields.add("continuation")
            continuation = recovery["continuation"]
            runner.safe.exact(continuation, {"producer_manifest", "producer_code"}, "classification continuation")
            for key in continuation:
                runner.safe.file_binding(continuation[key])
            if value.get("classification_policy") != classification.POLICY:
                raise runner.Error("classification continuation requires conservative inheritance")
        runner.safe.exact(recovery, recovery_fields, "campaign recovery")
        for key in ("producer_manifest", "producer_code"):
            runner.safe.file_binding(recovery[key])
        if not isinstance(recovery["imports"], dict) or len(recovery["imports"]) > len(shards):
            raise runner.Error("invalid recovery shard selection")
        for index, ref in recovery["imports"].items():
            if not isinstance(index, str) or not index.isdecimal() or str(int(index)) != index or not 0 <= int(index) < len(shards):
                raise runner.Error("recovery shard index differs")
            runner.safe.file_binding(ref)
        for key in ("prior_usage_estimate_microusd", "prior_unsettled_hold_microusd", "producer_budget_microusd"):
            runner.safe.integer(recovery[key], 0, 120_000_000, key)
        if (recovery["maximum_additional_paid_attempts_per_failed_chunk"] != 1 or
                type(recovery["maximum_additional_paid_attempts_per_failed_chunk"]) is not int or
                value["budget_microusd"] + recovery["prior_usage_estimate_microusd"] + recovery["prior_unsettled_hold_microusd"] != recovery["producer_budget_microusd"] or
                not isinstance(recovery["previous_completed"], list)):
            raise runner.Error("recovery budget or retry authority differs")
    return value


def prepare_manifest(selection_path, expected_sha256, output, state_root, *, budget_microusd=120_000_000):
    """Seal an explicit offline archive selection without credentials or network."""
    selection_ref = {"path": str(runner.safe.path_value(selection_path)), "sha256": expected_sha256}
    selection = runner.read(selection_ref)
    if (not isinstance(selection, dict) or
            selection.get("kind") != "himr_private_transcript_archive_selection" or
            type(selection.get("schema_version")) is not int or selection["schema_version"] != 1 or
            not isinstance(selection.get("sources"), list) or
            not isinstance(selection.get("shards"), list) or
            any(not isinstance(shard, list) for shard in selection["shards"]) or
            selection["sources"] != [spec for shard in selection["shards"] for spec in shard]):
        raise runner.Error("unsupported or inconsistent archive selection")
    if (not isinstance(selection.get("semantics"), dict) or
            selection["semantics"].get("third_party_transcripts_included") is not False or
            any(not isinstance(spec, dict) or spec.get("format") == "third_party" for spec in selection["sources"])):
        raise runner.Error("archive selection must explicitly exclude third-party transcripts")
    output, root = runner.safe.path_value(output), runner.safe.path_value(state_root)
    value = validate_manifest({"kind": KIND, "schema_version": 1, "selection": selection_ref,
        "state_root": str(root), "shards": selection["shards"],
        "config": {**core.DEFAULT_CONFIG, "timeline_profile": "gemini_flash_batch",
                   "max_chunk_input_bytes": 24000, "max_evidence_refs_per_item": 256,
                   "gemini_schema_policy": "local_array_bounds_v2"},
        "budget_microusd": budget_microusd, "max_active_shards": 4,
        "poll_seconds": 60, "max_runtime_seconds": 14 * 86400,
        "cloud": {"processing_approved": True, "paid_tier_confirmed": True},
        "classification_policy": "conservative_evidence_inheritance_v1",
        "implementation": implementation()})
    runner.protect(root, {"manifest": str(output), "selection": selection_ref, "sources": value["shards"]})
    return {"state": "prepared_offline_campaign", "manifest": runner.put(output, value),
            "state_root": str(root), "selected_transcripts": len(selection["sources"]),
            "shards": len(value["shards"]), "budget_microusd": value["budget_microusd"],
            "paid_requests_started": 0}


def usage_cost(job, response):
    """Return conservative reported text usage, or retain the entire job hold.

    totalTokenCount includes prompt, candidates and thinking according to the
    GenerateContent API. Unknown extra tokens are charged at the output rate.
    Missing/inconsistent usage, tools, caches or another model release no hold.
    https://ai.google.dev/api/generate-content#UsageMetadata
    """
    maximum = job["budget"]["maximum_cost_microusd"]
    if job["provider"] != "gemini" or job["model"] != core.PROFILES["gemini_flash_batch"]["model"]:
        raise runner.Error("campaign encountered a non-Gemini job")
    if not isinstance(response, dict):
        return maximum, False
    model = response.get("modelVersion")
    if not isinstance(model, str) or not (model == job["model"] or model.startswith(job["model"] + "-")):
        return maximum, False
    usage = response.get("usageMetadata")
    if not isinstance(usage, dict):
        return maximum, False
    values = [usage.get("promptTokenCount"), usage.get("candidatesTokenCount"),
              usage.get("thoughtsTokenCount", 0), usage.get("totalTokenCount")]
    if any(type(n) is not int or not 0 <= n <= 10**8 for n in values):
        return maximum, False
    prompt, candidates, thoughts, total = values
    if (prompt <= 0 or total < prompt + candidates + thoughts or
            any(usage.get(key, 0) != 0 for key in ("toolUsePromptTokenCount", "cachedContentTokenCount"))):
        return maximum, False
    profile = core.PROFILES["gemini_flash_batch"]
    charge = ((prompt * profile["input_rate_eighths_microusd"] + 7) // 8 +
              ((total - prompt) * profile["output_rate_eighths_microusd"] + 7) // 8)
    if (prompt > job["budget"]["input_token_allowance"] or
            total - prompt > job["budget"]["output_token_allowance"] or charge > maximum):
        raise runner.Error("reported usage exceeds conservative reservation; stop for review")
    return charge, True


def accounted_state(plan, state):
    settled, held = 0, 0
    for wave in state["waves"]:
        folder = runner.wave_folder(plan, wave["wave_id"])
        if not runner.safe.exists(folder / "submit-intent.json"):
            continue
        collection = state["collections"].get(wave["wave_id"])
        if collection is None:
            held += wave["maximum_cost_microusd"]
            continue
        capture = runner.read(collection["capture"])
        # load_state already replayed capture, wave identity and complete outcomes.
        rows = {row["custom_id"]: row for row in capture["items"]}
        for job in wave["jobs"]:
            row = rows.get(job["job_id"], {})
            charge, measured = usage_cost(job, row.get("response"))
            if measured:
                settled += charge
            else:
                held += charge
    return {"usage_estimate_microusd": settled, "unsettled_hold_microusd": held,
            "accounted_microusd": settled + held}


def request_for(manifest, index):
    root = Path(manifest["state_root"])
    result = {"kind": "himr_transcript_summary_request", "schema_version": 1,
        "state_root": str(root / f"shard-{index:04d}"), "sources": manifest["shards"][index],
        "config": manifest["config"],
        "limits": {**runner.DEFAULT_LIMITS, "max_sources": 32, "max_jobs_per_wave": 64},
        "budget": {"max_reserved_microusd": manifest["budget_microusd"], "max_attempts_per_job": 1},
        "cloud": manifest["cloud"]}
    imported = manifest.get("recovery", {}).get("imports", {}).get(str(index))
    if imported is not None:
        result["recovery"] = imported
    if "classification_policy" in manifest:
        result["classification_policy"] = manifest["classification_policy"]
    return result


def ensure_plan(manifest, index):
    root = Path(manifest["state_root"])
    request = request_for(manifest, index)
    ref = runner.put(root / "requests" / f"shard-{index:04d}.json", request)
    path = Path(request["state_root"]) / "plan.json"
    if not runner.safe.exists(path):
        return runner.create_plan(ref["path"], ref["sha256"])["plan"]
    result = runner.binding(path)
    plan, _ = runner.load_plan(result["path"], result["sha256"])
    if plan["request_value"] != request:
        raise runner.Error("existing shard differs from campaign selection")
    return result


def inspect_shard(ref):
    args = ref["path"], ref["sha256"]
    plan, sources = runner.load_plan(*args)
    state = runner.load_state(plan, sources)
    status = runner.status_from_state(plan, sources, state, phase="transcripts")
    return {"plan": ref, "status": status, **accounted_state(plan, state)}


def export_shard(row):
    if "reader_export" not in row:
        args = row["plan"]["path"], row["plan"]["sha256"]
        row["internal_export"] = runner.export_plan(*args, phase="transcripts")
        row["reader_export"] = reader.export_reader(*args, phase="transcripts")


def write_status(root, value):
    """Replace only this controller's small private status pointer, atomically."""
    path = root / "status.json"
    temporary = root / (".status-" + uuid.uuid4().hex)
    runner.put(temporary, value)
    os.replace(temporary, path)
    with runner.paths.retained_directory(root) as directory:
        os.fsync(directory)
    # The full shard inventory stays in status.json; avoid repeatedly copying it
    # into the service journal at every small progress update.
    print(runner.canonical({k: v for k, v in value.items() if k != "shards"}).decode().strip(), flush=True)


def runnable(row):
    status = row["status"]
    return not status["transcript_phase_complete"] and (
        bool(status["pending_waves"]) or status.get("ready_jobs", 0) > 0 or
        bool(status.get("prepared_waves")) or not status["failed_jobs"])


def progress_status(manifest, ref, shards, *, state="running", canary_only=False, phase="processing", active_shard=None):
    previous = manifest.get("recovery", {}).get("previous_completed", [])
    completed_before = sum(row["transcript_summaries"] for row in previous)
    value = {"kind": KIND + "_status", "schema_version": 1,
        "updated_at": dt.datetime.now(dt.timezone.utc).isoformat(), "state": state,
        "manifest": ref, "selected_transcripts": sum(map(len, manifest["shards"])),
        "total_shards": len(manifest["shards"]), "started_shards": len(shards),
        "completed_shards": sum(row["status"]["transcript_phase_complete"] for row in shards.values()),
        "pending_waves": sum(len(row["status"]["pending_waves"]) for row in shards.values()),
        "failed_jobs": sum(row["status"]["failed_jobs"] for row in shards.values()),
        "completed_jobs": sum(row["status"].get("completed_jobs", 0) for row in shards.values()),
        "transcript_summaries_complete": sum(row["status"]["transcript_summaries_complete"] for row in shards.values()),
        "imported_chunk_jobs": sum(row["status"].get("imported_chunk_jobs", 0) for row in shards.values()),
        "recovered_chunk_jobs": sum(row["status"].get("recovered_chunk_jobs", 0) for row in shards.values()),
        "imported_transcript_jobs": sum(row["status"].get("imported_transcript_jobs", 0) for row in shards.values()),
        "budget_microusd": manifest["budget_microusd"],
        "usage_estimate_microusd": sum(row["usage_estimate_microusd"] for row in shards.values()),
        "unsettled_hold_microusd": sum(row["unsettled_hold_microusd"] for row in shards.values()),
        "canary_only": canary_only, "synthesis_started": False, "automatic_paid_retries": False,
        "phase": phase, "active_shard": active_shard, "previous_completed_summaries": completed_before,
        "shards": [{"index": index, **row} for index, row in sorted(shards.items())]}
    value["total_transcript_summaries_complete"] = completed_before + value["transcript_summaries_complete"]
    value["total_selected_transcripts"] = value["selected_transcripts"] + sum(
        len(row["recording_ids"]) for row in previous)
    prior = manifest.get("recovery", {})
    value["prior_usage_estimate_microusd"] = prior.get("prior_usage_estimate_microusd", 0)
    value["prior_unsettled_hold_microusd"] = prior.get("prior_unsettled_hold_microusd", 0)
    return value


@contextmanager
def pause_signal():
    """Finish an in-flight request; never abort a POST merely to pause expansion."""
    stopping = [False]
    old = signal.signal(signal.SIGTERM, lambda *_: stopping.__setitem__(0, True))
    try:
        yield stopping
    finally:
        signal.signal(signal.SIGTERM, old)


def run(path, expected_sha256, *, allow_paid_api=False, canary_only=False):
    if type(canary_only) is not bool:
        raise runner.Error("canary_only must be a boolean")
    ref = {"path": str(runner.safe.path_value(path)), "sha256": expected_sha256}
    manifest = validate_manifest(runner.read(ref))
    if "selection" in manifest:
        selection = runner.read(manifest["selection"])
        if not isinstance(selection, dict) or selection.get("shards") != manifest["shards"]:
            raise runner.Error("campaign selection differs from its sealed metadata inventory")
    if not allow_paid_api:
        raise runner.Error("campaign start requires --allow-paid-api")
    root = Path(manifest["state_root"])
    runner.protect(root, {"manifest": ref, "sources": manifest["shards"]})
    runner.mkdir(root)
    with runner.locked(root), pause_signal() as stopping:
        if not runner.safe.exists(root / "workspace.json"):
            if any(p.name != "execution.lock" for p in root.iterdir()):
                raise runner.Error("refusing nonempty unmarked campaign workspace")
        runner.put(root / "workspace.json", MARKER)
        runner.put(root / "manifest-binding.json", ref)
        runner.mkdir(root / "requests")
        if canary_only and any(runner.safe.exists(root / f"shard-{index:04d}" / "plan.json")
                               for index in range(1, len(manifest["shards"]))):
            raise runner.Error("canary-only mode requires no previously prepared later shards")
        started = time.monotonic()
        shards = {}
        def publish(**kwargs):
            value = progress_status(manifest, ref, shards, canary_only=canary_only, **kwargs)
            write_status(root, value)
            return value
        publish(phase="loading")
        if "recovery" in manifest:
            from pipeline import transcript_summary_recovery as recovery
            recovery.validate_campaign(manifest, progress=lambda index: publish(
                phase="validating_recovery", active_shard=index))
        selected_indices = range(1 if canary_only else len(manifest["shards"]))
        for index in selected_indices:
            if stopping[0]:
                return publish(state="paused", phase="paused_after_request")
            plan_path = root / f"shard-{index:04d}" / "plan.json"
            if runner.safe.exists(plan_path):
                shards[index] = inspect_shard(ensure_plan(manifest, index))
                publish(phase="loading", active_shard=index)
        while time.monotonic() - started < manifest["max_runtime_seconds"]:
            if stopping[0]:
                return publish(state="paused", phase="paused_after_request")
            if implementation() != manifest["implementation"]:
                raise runner.Error("campaign implementation changed while running")
            if sum(row["accounted_microusd"] for row in shards.values()) > manifest["budget_microusd"]:
                raise runner.Error("existing campaign usage and holds exceed its allowance")
            budget_paused = False
            for index in selected_indices:
                if stopping[0]:
                    return publish(state="paused", phase="paused_after_request")
                active = sum(runnable(row) for row in shards.values())
                if index not in shards:
                    # First shard is a production canary; do not fan out until
                    # its whole-transcript output has passed local validation.
                    if index and (0 not in shards or not shards[0]["status"]["transcript_phase_complete"]):
                        break
                    if active >= manifest["max_active_shards"]:
                        continue
                    publish(phase="preparing_shard", active_shard=index)
                    shards[index] = inspect_shard(ensure_plan(manifest, index))
                row = shards[index]
                status, args = row["status"], (row["plan"]["path"], row["plan"]["sha256"])
                if status["ambiguous_waves"] or status["rejected_waves"]:
                    raise runner.Error("campaign needs submission reconciliation; no automatic retry")
                if status["transcript_phase_complete"]:
                    export_shard(row)
                    continue
                publish(phase="polling" if status["pending_waves"] else "checking_dependencies", active_shard=index)
                for wave_id in status["pending_waves"]:
                    runner.poll_wave(*args, wave_id)
                row = shards[index] = inspect_shard(row["plan"])
                status = row["status"]
                publish(phase="checked_dependencies", active_shard=index)
                if status["transcript_phase_complete"]:
                    export_shard(row)
                    continue
                if status["pending_waves"] or (status["failed_jobs"] and not (
                        status.get("ready_jobs", 0) or status.get("prepared_waves"))):
                    continue
                prepared = runner.prepare_plan(*args, phase="transcripts")
                if prepared["state"] not in {"prepared", "already_prepared"}:
                    raise runner.Error("unfinished shard has no schedulable transcript wave")
                wave_path = Path(args[0]).parent / "waves" / prepared["wave_id"] / "wave.json"
                wave = runner.read(runner.binding(wave_path))
                if wave["provider"] != "gemini" or any(j["stage"] not in {"chunk", "transcript"} for j in wave["jobs"]):
                    raise runner.Error("campaign wave escaped Gemini transcript phase")
                total = sum(s["accounted_microusd"] for s in shards.values())
                if total + wave["maximum_cost_microusd"] > manifest["budget_microusd"]:
                    budget_paused = True
                    continue
                if stopping[0]:
                    return publish(state="paused", phase="paused_before_submission")
                publish(phase="submitting", active_shard=index)
                outcome = runner.submit_wave(*args, prepared["wave_id"], allow_paid_api=True, phase="transcripts")
                if outcome["state"] not in {"submitted", "already_submitted"}:
                    raise runner.Error("submission did not produce a durable receipt; review required")
                shards[index] = inspect_shard(row["plan"])
                publish(phase="submitted", active_shard=index)
            complete = sum(row["status"]["transcript_phase_complete"] for row in shards.values())
            failed = sum(row["status"]["failed_jobs"] for row in shards.values())
            pending = sum(len(row["status"]["pending_waves"]) for row in shards.values())
            finished = len(shards) == len(manifest["shards"]) and complete + sum(
                bool(row["status"]["failed_jobs"]) and not runnable(row) for row in shards.values()) == len(shards)
            state = ("awaiting_canary_review" if canary_only and complete == 1 else
                     "completed" if finished and not failed else "needs_review" if finished else
                     "budget_paused" if budget_paused and not pending else
                     "needs_review" if 0 in shards and shards[0]["status"]["failed_jobs"] and not runnable(shards[0]) else "running")
            status = publish(state=state, phase="waiting" if state == "running" else "finished")
            if state != "running":
                return status
            time.sleep(manifest["poll_seconds"])
        raise runner.Error("finite campaign time limit reached; existing batches remain recoverable")


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    mode = parser.add_mutually_exclusive_group(required=True)
    mode.add_argument("--manifest")
    mode.add_argument("--selection", help="prepare a manifest offline from a reviewed selection")
    parser.add_argument("--expected-sha256", required=True)
    parser.add_argument("--output", help="new private manifest path for offline preparation")
    parser.add_argument("--state-root", help="new private campaign workspace for offline preparation")
    parser.add_argument("--allow-paid-api", action="store_true")
    parser.add_argument("--canary-only", action="store_true",
                        help="run only the first shard, export it, then pause for quality review")
    parser.add_argument("--budget-microusd", type=int,
                        help="offline preparation allowance, 1..120000000 (default: 120000000)")
    args = parser.parse_args(argv)
    try:
        if args.selection:
            if not args.output or not args.state_root or args.allow_paid_api or args.canary_only:
                raise runner.Error("offline preparation requires --output and --state-root, without --allow-paid-api")
            print(runner.canonical(prepare_manifest(args.selection, args.expected_sha256,
                args.output, args.state_root, budget_microusd=(120_000_000 if args.budget_microusd is None
                                                              else args.budget_microusd))).decode().strip())
            return 0
        if args.output or args.state_root or args.budget_microusd is not None:
            raise runner.Error("run mode uses only the sealed manifest's paths")
        result = run(args.manifest, args.expected_sha256, allow_paid_api=args.allow_paid_api,
                     canary_only=args.canary_only)
        return 0 if result["state"] in {"completed", "awaiting_canary_review", "paused"} else 2
    except (runner.Error, core.SummaryError, runner.sources_module.SourceError,
            runner.client_module.BatchClientError, runner.env_module.EnvFileError, OSError) as error:
        # A failed POST is never retried here. Keep the last counts but mark the
        # status stale/failed so an unattended service cannot look healthy.
        try:
            ref = {"path": str(runner.safe.path_value(args.manifest)), "sha256": args.expected_sha256}
            manifest = runner.read(ref)
            root = Path(manifest["state_root"])
            if (runner.read(runner.binding(root / "workspace.json")) == MARKER and
                    runner.read(runner.binding(root / "manifest-binding.json")) == ref):
                with runner.locked(root):
                    status = runner._read_optional(root / "status.json") or {"kind": KIND + "_status", "manifest": ref}
                    write_status(root, {**status, "state": "stopped_for_review", "error": str(error),
                        "updated_at": dt.datetime.now(dt.timezone.utc).isoformat(),
                        "counts_may_be_stale": True, "automatic_paid_retries": False})
        except (OSError, RuntimeError, KeyError, TypeError):
            pass
        print("Gemini campaign stopped: " + str(error), file=sys.stderr, flush=True)
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
