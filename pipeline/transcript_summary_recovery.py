"""Explicit, offline adoption of terminal Gemini chunks into a new contract.

Producer workspaces, requests, receipts and outputs are read-only. Exact source
coverage and complete producer capture replay are required before importing a
result. A new manifest records both the transition and all prior spending/holds.
Only failed or never-submitted chunks may become new paid work. This is not a
generic implementation-hash override or an automatic retry loop.
"""
from __future__ import annotations

import argparse
from collections import OrderedDict
from copy import deepcopy
from pathlib import Path
import sys

from pipeline import transcript_summary as r
from pipeline import transcript_summary_core as core

KIND = "himr_gemini_terminal_chunk_recovery"
_CACHE = OrderedDict()
_CACHE_BYTES = 0
MAX_CACHE_BYTES = 256 * 1024**2


def _witness(ref):
    with r.safe.opened(ref["path"]) as fd:
        return r.safe.witness(fd)


def producer_manifest(ref, code_ref):
    value = r.read(ref)
    code = r.read(code_ref)
    if (value.get("kind") != "himr_gemini_transcript_campaign" or value.get("schema_version") != 1 or
            code.get("producer_manifest") != ref or set(code.get("files", {})) != set(value.get("implementation", {}))):
        raise r.Error("recovery producer manifest or code proof differs")
    for name, proof in code["files"].items():
        if proof["sha256"] != value["implementation"][name] or Path(proof["path"]).name != name:
            raise r.Error("producer source-code snapshot differs")
        r.read_bytes(proof)
    config = core.normalize_config(value["config"])
    if (config["transcript_profile"] != "gemini_flash_batch" or "broader_synthesis" in config or
            value["cloud"] != {"processing_approved": True, "paid_tier_confirmed": True} or
            not isinstance(value["shards"], list) or not 1 <= len(value["shards"]) <= 512):
        raise r.Error("recovery producer must be an explicitly selected Gemini transcript campaign")
    r.safe.integer(value["budget_microusd"], 1, 120_000_000, "producer budget")
    return value


def inspect_producer(manifest_ref, code_ref, index, *, expected_sources=None):
    """Replay the old protocol, not the corrected result policy, on old receipts."""
    manifest = producer_manifest(manifest_ref, code_ref)
    r.safe.integer(index, 0, len(manifest["shards"])-1, "producer shard index")
    folder = Path(manifest["state_root"]) / f"shard-{index:04d}"
    ref = r.binding(folder / "plan.json")
    plan = r.read(ref)
    body = {k: v for k, v in plan.items() if k != "plan_id"}
    expected_implementation = {k: v for k, v in manifest["implementation"].items()
                               if k not in {"transcript_summary_campaign.py", "transcript_summary_reader.py"}}
    request = r.validate_request(plan["request_value"])
    if (plan.get("kind") != "himr_transcript_summary_plan" or plan.get("schema_version") != 1 or
            plan["plan_id"] != "summaryplan_" + r.digest(body)[:32] or
            plan["implementation"] != expected_implementation or plan["semantics"] != r.SEMANTICS or
            request != r.read(plan["request"]) or Path(request["state_root"]) != folder or
            request["sources"] != manifest["shards"][index] or request["config"] != manifest["config"] or
            "recovery" in request or r.read(r.binding(folder / "workspace.json")) != r.MARKER):
        raise r.Error("producer plan is not the bound original Gemini shard")
    sources = expected_sources if expected_sources is not None else [
        r.sources_module.normalize_source(spec) for spec in request["sources"]]
    if len(sources) != len(request["sources"]):
        raise r.Error("recovery source selection differs")
    for source, spec in zip(sources, request["sources"]):
        if (source["transcript"] != spec["transcript"] or source["recording_id"] != spec["recording_id"] or
                source != r.read(r.binding(folder / "sources" / (source["source_id"] + ".json")))):
            raise r.Error("recovery source differs from producer transcript")
    initial = core.initial_jobs(sources, request["config"])
    if ([s["source_id"] for s in sources] != plan["source_ids"] or
            sum(len(r.canonical(s)) for s in sources) != plan["source_bytes"] or
            [j["job_id"] for j in initial] != plan["initial_job_ids"]):
        raise r.Error("producer initial source coverage differs")
    core.source_coverage(sources, initial)
    # The producer implementation identity was checked against its separately
    # bound byte-for-byte snapshot above. Job construction and v1 capture rules
    # remain backward compatible; never pretend this is a current load_plan.
    state = r.load_state(plan, sources)
    for wave in state["waves"]:
        if wave["provider"] != "gemini" or any(j["stage"] not in {"chunk", "transcript"} for j in wave["jobs"]):
            raise r.Error("producer wave escaped transcript recovery")
        paid = r.safe.exists(r.wave_folder(plan, wave["wave_id"]) / "submit-intent.json")
        if paid and wave["wave_id"] not in state["collections"]:
            raise r.Error("collect/reconcile every producer paid wave before migration")
    return manifest, ref, plan, sources, initial, state


def _build(manifest_ref, code_ref, index, config, *, expected_sources=None, producer_snapshot=None):
    from pipeline import transcript_summary_campaign as campaign
    manifest, plan_ref, plan, sources, initial, state = (producer_snapshot if producer_snapshot is not None else
        inspect_producer(manifest_ref, code_ref, index, expected_sources=expected_sources))
    config = core.normalize_config(config)
    if (config.get("max_evidence_refs_per_item") != 256 or config["transcript_profile"] != "gemini_flash_batch" or
            config["timeline_profile"] != "gemini_flash_batch" or "broader_synthesis" in config):
        raise r.Error("recovery requires the bounded corrected Gemini-only contract")
    if any(j["stage"] != "chunk" for j in state["jobs"]):
        raise r.Error("completed/reduced transcripts must be retained separately, not resubmitted")
    original_by_id = {j["job_id"]: j for j in initial}
    replacements = [core.make_job("chunk", j["scope"], j["evidence"], [], config) for j in initial]
    core.source_coverage(sources, replacements)
    new_by_old = dict(zip(original_by_id, replacements))
    outcomes = {}
    proofs = [manifest_ref, code_ref, plan_ref, plan["request"], r.binding(Path(plan_ref["path"]).parent / "workspace.json")]
    proofs += [r.binding(Path(plan_ref["path"]).parent / "sources" / (s["source_id"] + ".json")) for s in sources]
    recovered, retained, retry, pending = [], [], [], []
    for wave in state["waves"]:
        folder = r.wave_folder(plan, wave["wave_id"])
        for name in ("wave.json", "requests.bin", "submit-intent.json", "submitted.json", "submission-response.json",
                     "reconciliation-response.json", "capture.json", "collection.json"):
            if r.safe.exists(folder / name):
                proofs.append(r.binding(folder / name))
        collection = state["collections"].get(wave["wave_id"])
        if collection is None:
            continue
        capture = r.read(collection["capture"])
        rows = {row["custom_id"]: row for row in capture["items"]}
        for outcome in collection["outcomes"]:
            identity = outcome["job_id"]
            if identity in outcomes or identity not in original_by_id:
                raise r.Error("duplicate/out-of-scope producer chunk attempt")
            new_job = new_by_old[identity]
            raw = rows.get(identity, {})
            normalized, failure = None, None
            try:
                if raw.get("error") is not None or not isinstance(raw.get("response"), dict):
                    raise r.Error("producer request failed or lacks a terminal response")
                reported = raw["response"].get("modelVersion")
                if not isinstance(reported, str) or not (reported == new_job["model"] or reported.startswith(new_job["model"] + "-")):
                    raise r.Error("producer response model differs")
                normalized = core.normalize_api_result(new_job, r.response_payload("gemini", raw["response"]))
            except (RuntimeError, ValueError, KeyError, TypeError) as error:
                failure = str(error)
            if outcome["state"] == "completed" and normalized is None:
                raise r.Error("migration would discard an already validated successful chunk")
            row = {"producer_job_id": identity, "job_id": new_job["job_id"],
                   "producer_collection": r.binding(folder / "collection.json")}
            if normalized is not None:
                row["result"] = normalized
                (retained if outcome["state"] == "completed" else recovered).append(row)
            else:
                retry.append({**row, "reason": failure, "prior_paid_attempts": 1, "maximum_additional_paid_attempts": 1})
            outcomes[identity] = row
    for identity, new_job in new_by_old.items():
        if identity not in outcomes:
            pending.append(new_job["job_id"])
    # Pin all original artifacts. The byte/metadata witnesses make the admission
    # cache cheap without turning recovery into an unchecked copied result.
    proofs += list(r.read(code_ref)["files"].values())
    unique = {ref["path"]: ref for ref in proofs}
    return {"kind": KIND, "schema_version": 1, "producer_manifest": manifest_ref, "producer_code": code_ref,
            "producer_index": index, "producer_plan": plan_ref, "config": config,
            "source_ids": [s["source_id"] for s in sources], "initial_jobs": replacements,
            "retained": retained, "recovered": recovered, "retry": retry, "unsubmitted_jobs": pending,
            "proofs": sorted(unique.values(), key=lambda x: x["path"]),
            "prior_accounting": campaign.accounted_state(plan, state),
            "policy": {"preserve_all_evidence_links": True, "rewrite_source_or_producer": False,
                       "source_coverage_complete": True, "retry_only_failed_chunks": True,
                       "maximum_additional_paid_attempts": 1, "human_reviewed": False}}


def load(ref, sources, config):
    global _CACHE_BYTES
    value = r.read(ref)
    if value.get("kind") == "himr_gemini_classification_continuation":
        from pipeline import transcript_summary_classification as classification
        return classification.load(ref, sources, config)
    if value.get("kind") != KIND or value.get("schema_version") != 1 or value.get("config") != config:
        raise r.Error("unsupported or differently configured recovery admission")
    if len(value.get("proofs", [])) > 20000:
        raise r.Error("recovery proof list exceeds bound")
    key = ref["sha256"], r.digest(sources), r.digest(config)
    witnesses = [_witness(proof) for proof in value["proofs"]]
    cached = _CACHE.get(key)
    if cached is not None and cached[0] == witnesses:
        _CACHE.move_to_end(key)
        return deepcopy(cached[1])
    for proof in value["proofs"]:
        r.read_bytes(proof)
    expected = _build(value["producer_manifest"], value["producer_code"], value["producer_index"], config,
                      expected_sources=sources)
    if value != expected or witnesses != [_witness(proof) for proof in value["proofs"]]:
        raise r.Error("recovery admission does not replay or producer changed")
    size = len(r.canonical(expected))
    if size <= MAX_CACHE_BYTES:
        previous = _CACHE.pop(key, None)
        if previous:
            _CACHE_BYTES -= previous[2]
        while _CACHE and (_CACHE_BYTES + size > MAX_CACHE_BYTES or len(_CACHE) >= 64):
            _, entry = _CACHE.popitem(last=False)
            _CACHE_BYTES -= entry[2]
        _CACHE[key] = witnesses, deepcopy(expected), size
        _CACHE_BYTES += size
    return expected


def completed_record(plan_ref, plan, sources, state, accounting):
    return {"producer_plan": plan_ref, "source_ids": plan["source_ids"],
            "recording_ids": [source["recording_id"] for source in sources],
            "transcript_summaries": sum(result["stage"] == "transcript" and result["scope"]["final"]
                                        for result in state["results"]),
            "accounting": accounting}


def schema_failure_accounting(repair, manifest_ref, code_ref, shards=None):
    """Only the terminal, all-invalid-argument reducer canary may be superseded.

    Replay its bound producer code contract and receipts; no successful reducer,
    pending request, paid chunk attempt or later shard may disappear. This is a
    one-time schema transition, not generic failed-campaign retry authority.
    """
    from pipeline import transcript_summary_campaign as campaign
    parent = producer_manifest(manifest_ref, code_ref)
    failed = producer_manifest(repair["producer_manifest"], repair["producer_code"])
    prior = failed.get("recovery", {})
    if (prior.get("producer_manifest") != manifest_ref or prior.get("producer_code") != code_ref or
            "schema_repair" in prior or failed["config"] != {
                **parent["config"], "max_evidence_refs_per_item": 256, "max_chunk_input_bytes": 26000} or
            (shards is not None and failed["shards"] != shards)):
        raise r.Error("schema failure is not the bound original recovery canary")
    root = Path(failed["state_root"])
    r.protect(root, {"original_root": parent["state_root"]})
    if (r.read(r.binding(root / "workspace.json")) != campaign.MARKER or
            r.read(r.binding(root / "manifest-binding.json")) != repair["producer_manifest"] or
            sorted(p.name for p in root.iterdir() if p.name.startswith("shard-")) != ["shard-0000"]):
        raise r.Error("schema repair requires an isolated first-shard canary")
    folder = root / "shard-0000"
    plan = r.read(r.binding(folder / "plan.json"))
    request = r.validate_request(plan["request_value"])
    expected_code = {k: v for k, v in failed["implementation"].items()
                     if k not in {"transcript_summary_campaign.py", "transcript_summary_reader.py"}}
    if (plan.get("kind") != "himr_transcript_summary_plan" or plan.get("schema_version") != 1 or
            plan["plan_id"] != "summaryplan_" + r.digest({k: v for k, v in plan.items() if k != "plan_id"})[:32] or
            plan["implementation"] != expected_code or plan["semantics"] != r.SEMANTICS or
            request != campaign.request_for(failed, 0) or request != r.read(plan["request"]) or
            r.read(r.binding(folder / "workspace.json")) != r.MARKER):
        raise r.Error("failed canary plan identity differs")
    sources = [r.sources_module.normalize_source(spec) for spec in request["sources"]]
    for source in sources:
        if source != r.read(r.binding(folder / "sources" / (source["source_id"] + ".json"))):
            raise r.Error("failed canary transcript changed")
    initial = r.plan_initial(request, sources)
    if (plan["source_ids"] != [s["source_id"] for s in sources] or
            plan["source_bytes"] != sum(len(r.canonical(s)) for s in sources) or
            plan["initial_job_ids"] != [j["job_id"] for j in initial]):
        raise r.Error("failed canary source coverage differs")
    state = r.load_state(plan, sources)
    if len(state["waves"]) != 1:
        raise r.Error("schema repair admits exactly one failed canary wave")
    wave = state["waves"][0]
    collection = state["collections"].get(wave["wave_id"])
    if (not collection or not r.safe.exists(r.wave_folder(plan, wave["wave_id"]) / "submit-intent.json") or
            any(j["stage"] != "transcript" for j in wave["jobs"])):
        raise r.Error("schema canary must have terminal reducers only; collect/reconcile first")
    capture = r.read(collection["capture"])
    rows = {row["custom_id"]: row for row in capture["items"]}
    if any(outcome["state"] != "needs_review" or outcome.get("failure") != "provider_request_failed" or
           rows[outcome["job_id"]].get("response") is not None or
           rows[outcome["job_id"]].get("error") != {"code": 3, "message": "Request contains an invalid argument."}
           for outcome in collection["outcomes"]):
        raise r.Error("schema repair cannot discard successful or differently failed outputs")
    accounting = campaign.accounted_state(plan, state)
    return failed, accounting


def diagnostic_hold(ref):
    """Retain the entire standard-rate allowance for the two bounded A/B probes."""
    if ref is None:
        return 0
    report = r.read(ref)
    root = Path(ref["path"]).parent
    if (report.get("kind") != "himr_gemini_schema_diagnostic" or report.get("schema_version") != 1 or
            report.get("automatic_retry") is not False or len(report.get("rows", [])) != 2):
        raise r.Error("unsupported schema diagnostic accounting")
    r.read_bytes(report["script"])
    total = 0
    for name, row in zip(("bounded-arrays", "local-array-bounds"), report["rows"]):
        terminal = r.read(r.binding(root / (name + "-terminal.json")))
        intent = r.read(r.binding(root / (name + "-intent.json")))
        wire = r.read(intent["wire"])
        reserve = ((len(core.canonical(wire)) + 4096) * 6 + 7) // 8 + (65536 * 30 + 7) // 8
        if (row != terminal or row["name"] != name or row["state"] not in {"rejected", "succeeded"} or
                intent.get("automatic_retry") is not False or
                intent["model"] != core.PROFILES["gemini_flash_batch"]["model"] or
                row["reserved_microusd"] != reserve or intent["reserved_microusd"] != reserve):
            raise r.Error("schema diagnostic reservation differs or remains ambiguous")
        if row["state"] == "succeeded":
            r.read(row["capture"])
        total += reserve
    if total != report["reserved_microusd"] or not 0 < total <= 1_000_000:
        raise r.Error("schema diagnostic total differs")
    return total


def _repair_accounting(repair, manifest_ref, code_ref, shards, settled, held):
    failed, accounting = schema_failure_accounting(repair, manifest_ref, code_ref, shards)
    prior = failed["recovery"]
    parent = producer_manifest(manifest_ref, code_ref)
    if (prior["prior_usage_estimate_microusd"] != settled or prior["prior_unsettled_hold_microusd"] != held or
            prior["producer_budget_microusd"] != parent["budget_microusd"] or
            failed["budget_microusd"] != parent["budget_microusd"] - settled - held):
        raise r.Error("failed canary original budget accounting differs")
    return (settled + accounting["usage_estimate_microusd"],
            held + accounting["unsettled_hold_microusd"] + diagnostic_hold(repair["diagnostic"]))


def validate_campaign(value, *, progress=None):
    """Before new paid work, replay the entire parent partition and budget.

    No selected recording, paid intent, unknown-usage hold or completed final
    may disappear merely because the new manifest has self-consistent totals.
    """
    from pipeline import transcript_summary_campaign as campaign
    recovery = value["recovery"]
    parent = producer_manifest(recovery["producer_manifest"], recovery["producer_code"])
    expected_config = {**parent["config"], "max_evidence_refs_per_item": 256, "max_chunk_input_bytes": 26000}
    if "gemini_schema_policy" in value["config"]:
        expected_config["gemini_schema_policy"] = "local_array_bounds_v2"
    if value["config"] != expected_config:
        raise r.Error("recovery changed configuration outside the corrected contract")
    r.protect(Path(value["state_root"]), {"producer_root": parent["state_root"]})
    shards, previous, imported = [], [], set()
    settled = held = 0
    for index, specs in enumerate(parent["shards"]):
        path = Path(parent["state_root"]) / f"shard-{index:04d}" / "plan.json"
        if r.safe.exists(path):
            if progress:
                progress(index)
            key = str(len(shards))
            ref = recovery["imports"].get(key)
            admission = r.read(ref) if ref else None
            if admission is not None and admission.get("producer_index") == index:
                if (admission.get("producer_manifest") != recovery["producer_manifest"] or
                        admission.get("producer_code") != recovery["producer_code"]):
                    raise r.Error("campaign import belongs to another producer")
                sources = [r.sources_module.normalize_source(spec) for spec in specs]
                admission = load(ref, sources, value["config"])
                accounting = admission["prior_accounting"]
                imported.add(key)
            else:
                _, plan_ref, plan, sources, _, state = inspect_producer(
                    recovery["producer_manifest"], recovery["producer_code"], index)
                if not r.phase_progress(sources, state, parent["config"])["transcript_phase_complete"]:
                    raise r.Error("started producer shard is missing its recovery admission")
                accounting = campaign.accounted_state(plan, state)
                previous.append(completed_record(plan_ref, plan, sources, state, accounting))
                settled += accounting["usage_estimate_microusd"]
                held += accounting["unsettled_hold_microusd"]
                continue
            settled += accounting["usage_estimate_microusd"]
            held += accounting["unsettled_hold_microusd"]
        shards.append(specs)
    if "schema_repair" in recovery:
        settled, held = _repair_accounting(recovery["schema_repair"], recovery["producer_manifest"],
            recovery["producer_code"], shards, settled, held)
    if "continuation" in recovery:
        from pipeline import transcript_summary_classification as classification
        settled, held = classification.accounting(value, settled, held)
    if (shards != value["shards"] or imported != set(recovery["imports"]) or
            previous != recovery["previous_completed"] or
            settled != recovery["prior_usage_estimate_microusd"] or
            held != recovery["prior_unsettled_hold_microusd"] or
            parent["budget_microusd"] != recovery["producer_budget_microusd"] or
            value["budget_microusd"] != parent["budget_microusd"] - settled - held):
        raise r.Error("recovery source partition or prior paid accounting differs")
    return {"validated_imports": len(imported), "prior_usage_estimate_microusd": settled,
            "prior_unsettled_hold_microusd": held}


def _new_config(config):
    return {**config, "max_evidence_refs_per_item": 256, "max_chunk_input_bytes": 26000,
            "gemini_schema_policy": "local_array_bounds_v2"}


def prepare_campaign(manifest_ref, code_ref, audit_root, output, state_root, *, schema_repair=None):
    """Collect-only must finish first. This function never calls any API."""
    from pipeline import transcript_summary_campaign as campaign
    manifest = producer_manifest(manifest_ref, code_ref)
    config = _new_config(manifest["config"])
    root, output, audit = map(r.safe.path_value, (state_root, output, audit_root))
    r.protect(root, {"producer": manifest_ref, "code": code_ref, "audit": str(audit), "output": str(output)})
    if r.safe.exists(root) or r.safe.exists(output):
        raise r.Error("recovery requires a fresh manifest and workspace")
    if schema_repair is not None:
        schema_failure_accounting(schema_repair, manifest_ref, code_ref)
        diagnostic_hold(schema_repair["diagnostic"])
    r.mkdir(audit)
    imports, shards, previous = {}, [], []
    settled = held = 0
    for index, specs in enumerate(manifest["shards"]):
        plan_path = Path(manifest["state_root"]) / f"shard-{index:04d}" / "plan.json"
        if r.safe.exists(plan_path):
            snapshot = inspect_producer(manifest_ref, code_ref, index)
            _, plan_ref, plan, sources, _, state = snapshot
            accounting = campaign.accounted_state(plan, state)
            settled += accounting["usage_estimate_microusd"]; held += accounting["unsettled_hold_microusd"]
            progress = r.phase_progress(sources, state, manifest["config"])
            if progress["transcript_phase_complete"]:
                previous.append(completed_record(plan_ref, plan, sources, state, accounting))
                continue
            value = _build(manifest_ref, code_ref, index, config, expected_sources=sources,
                           producer_snapshot=snapshot)
            imports[str(len(shards))] = r.put(audit / f"shard-{index:04d}-admission.json", value)
            print(r.canonical({"state": "recovery_admitted_offline", "producer_shard": index,
                              "retained": len(value["retained"]), "recovered": len(value["recovered"]),
                              "retry": len(value["retry"])}).decode().strip(), flush=True)
        shards.append(specs)
    if schema_repair is not None:
        settled, held = _repair_accounting(schema_repair, manifest_ref, code_ref, shards, settled, held)
    remaining = manifest["budget_microusd"]-settled-held
    if remaining <= 0 or not shards:
        raise r.Error("no remaining recovery budget or selected work")
    selection = r.read(manifest["selection"])
    selection_ref = r.put(audit / "selection.json", {"kind": selection["kind"], "schema_version": 1,
        "sources": [s for group in shards for s in group], "shards": shards,
        "semantics": selection["semantics"], "recovery_parent_selection": manifest["selection"]})
    recovery = {"producer_manifest": manifest_ref, "producer_code": code_ref, "imports": imports,
                "previous_completed": previous, "prior_usage_estimate_microusd": settled,
                "prior_unsettled_hold_microusd": held, "producer_budget_microusd": manifest["budget_microusd"],
                "maximum_additional_paid_attempts_per_failed_chunk": 1}
    if schema_repair is not None:
        recovery["schema_repair"] = schema_repair
    value = campaign.validate_manifest({**manifest, "implementation": campaign.implementation(),
        "selection": selection_ref, "config": config, "state_root": str(root), "shards": shards,
        "budget_microusd": remaining, "recovery": recovery})
    ref = r.put(output, value)
    return {"state": "recovery_prepared_offline", "manifest": ref, "selected_transcripts": sum(map(len, shards)),
            "previous_completed_summaries": sum(row["transcript_summaries"] for row in previous),
            "prior_usage_estimate_microusd": settled, "prior_unsettled_hold_microusd": held,
            "remaining_budget_microusd": remaining, "new_paid_requests": 0}


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--producer-manifest", required=True)
    parser.add_argument("--producer-sha256", required=True)
    parser.add_argument("--producer-code", required=True)
    parser.add_argument("--producer-code-sha256", required=True)
    parser.add_argument("--audit-root", required=True)
    parser.add_argument("--output", required=True)
    parser.add_argument("--state-root", required=True)
    parser.add_argument("--schema-repair", help="Hash-bound schema-failure transition JSON (offline)")
    parser.add_argument("--schema-repair-sha256")
    args = parser.parse_args(argv)
    try:
        if bool(args.schema_repair) != bool(args.schema_repair_sha256):
            raise r.Error("schema repair requires its exact digest")
        repair = r.read({"path": args.schema_repair, "sha256": args.schema_repair_sha256}) if args.schema_repair else None
        result = prepare_campaign(
            {"path": args.producer_manifest, "sha256": args.producer_sha256},
            {"path": args.producer_code, "sha256": args.producer_code_sha256},
            args.audit_root, args.output, args.state_root, schema_repair=repair)
        print(r.canonical(result).decode().strip(), flush=True)
        return 0
    except (RuntimeError, ValueError, OSError, KeyError, TypeError) as error:
        print("Gemini recovery stopped: " + str(error), file=sys.stderr)
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
