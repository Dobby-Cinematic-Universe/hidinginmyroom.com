"""Conservative evidence-tag inheritance and explicit paid-result continuation.

Only classification metadata may become more cautious. Text, evidence links,
provider captures and producer workspaces never change. This is not an entailment
check or authority to relabel a summary as fact-checked.
"""
from copy import deepcopy
from pathlib import Path
import argparse
import sys

from pipeline import transcript_summary as r
from pipeline import transcript_summary_core as core

POLICY = "conservative_evidence_inheritance_v1"
KIND = "himr_gemini_classification_continuation"
_CACHE = {}


def normalize(job, payload):
    """Derive a tag no more certain than either model or any cited evidence."""
    core._exact(payload, core.SECTIONS, "summary response")
    value = deepcopy(payload)
    levels = {"reported_statement": 0, "reported_allegation": 1, "uncertainty": 2}
    names = tuple(levels)
    evidence = {"e" + str(i): item["classification"] for i, item in enumerate(job["evidence"], 1)}
    changes = []
    for section, items in value.items():
        if not isinstance(items, list):
            raise r.Error("summary sections must be arrays")
        for index, item in enumerate(items):
            core._exact(item, ("text", "classification", "evidence_ids"), "summary item")
            if not isinstance(item["classification"], str) or item["classification"] not in levels:
                raise r.Error("invalid summary classification")
            refs = item["evidence_ids"]
            if not isinstance(refs, list) or not refs or any(not isinstance(ref, str) or ref not in evidence for ref in refs):
                raise r.Error("summary item cites missing or foreign evidence")
            # Raw transcript excerpts have no inherited classification. They
            # impose no floor; the model still has to supply a valid tag.
            inherited = [levels[evidence[ref]] for ref in refs if evidence[ref] is not None]
            tag = names[max([levels[item["classification"]]] + inherited)]
            if item["classification"] != tag:
                changes.append({"section": section, "item_index": index,
                    "model_classification": item["classification"], "inherited_classification": tag})
                item["classification"] = tag
    # The existing strict validator still enforces every field, length, count,
    # reference, duplicate, classification and scope constraint on the result.
    return core.normalize_api_result(job, value), changes


def inspect(ref, code_ref):
    """Replay the stopped, terminal, first-shard producer under its old policy."""
    from pipeline import transcript_summary_campaign as campaign
    from pipeline import transcript_summary_recovery as recovery
    value = recovery.producer_manifest(ref, code_ref)
    if ("classification_policy" in value or "recovery" not in value or
            "continuation" in value["recovery"]):
        raise r.Error("classification continuation requires the original recovered canary")
    root = Path(value["state_root"])
    if (r.read(r.binding(root / "workspace.json")) != campaign.MARKER or
            r.read(r.binding(root / "manifest-binding.json")) != ref or
            sorted(p.name for p in root.iterdir() if p.name.startswith("shard-")) != ["shard-0000"]):
        raise r.Error("classification continuation requires an isolated first-shard canary")
    folder = root / "shard-0000"
    plan = r.read(r.binding(folder / "plan.json"))
    request = r.validate_request(plan["request_value"])
    expected = {k: v for k, v in value["implementation"].items()
                if k not in {"transcript_summary_campaign.py", "transcript_summary_reader.py"}}
    if (plan.get("kind") != "himr_transcript_summary_plan" or plan.get("schema_version") != 1 or
            plan["plan_id"] != "summaryplan_" + r.digest({k: v for k, v in plan.items() if k != "plan_id"})[:32] or
            plan["implementation"] != expected or plan["semantics"] != r.SEMANTICS or
            request != campaign.request_for(value, 0) or request != r.read(plan["request"]) or
            r.read(r.binding(folder / "workspace.json")) != r.MARKER):
        raise r.Error("classification producer plan identity differs")
    sources = [r.sources_module.normalize_source(spec) for spec in request["sources"]]
    if any(source != r.read(r.binding(folder / "sources" / (source["source_id"] + ".json"))) for source in sources):
        raise r.Error("classification producer source changed")
    initial = r.plan_initial(request, sources)
    if (plan["source_ids"] != [s["source_id"] for s in sources] or
            plan["source_bytes"] != sum(len(r.canonical(s)) for s in sources) or
            plan["initial_job_ids"] != [j["job_id"] for j in initial]):
        raise r.Error("classification producer source coverage differs")
    state = r.load_state(plan, sources)
    if len(state["waves"]) != 1:
        raise r.Error("classification continuation admits one terminal reducer wave")
    wave = state["waves"][0]
    if (wave["wave_id"] not in state["collections"] or
            any(j["stage"] != "transcript" or not j["scope"]["final"] for j in wave["jobs"])):
        raise r.Error("collect/reconcile the final reducer canary before continuation")
    return value, plan, sources, initial, state


def build(base_ref, continuation, *, sources=None, config=None):
    from pipeline import transcript_summary_campaign as campaign
    from pipeline import transcript_summary_recovery as recovery
    producer, plan, selected, initial, state = inspect(continuation["producer_manifest"], continuation["producer_code"])
    if ((sources is not None and sources != selected) or (config is not None and config != producer["config"]) or
            base_ref != producer["recovery"]["imports"].get("0")):
        raise r.Error("classification import source, config or base differs")
    base = recovery.load(base_ref, selected, producer["config"])
    if base["kind"] != recovery.KIND:
        raise r.Error("classification continuation cannot form a recovery loop")
    wave = state["waves"][0]
    collection = state["collections"][wave["wave_id"]]
    capture = r.read(collection["capture"])
    rows = {row["custom_id"]: row for row in capture["items"]}
    outcomes = {row["job_id"]: row for row in collection["outcomes"]}
    result = deepcopy(base)
    result.update(kind=KIND, base_admission=base_ref, continuation=continuation,
                  classification_policy=POLICY, retained_jobs=state["jobs"], classification_adjustments=[])
    for job in wave["jobs"]:
        raw = rows.get(job["job_id"], {})
        if raw.get("error") is not None or not isinstance(raw.get("response"), dict):
            raise r.Error("classification recovery cannot replace provider failures or missing outputs")
        version = raw["response"].get("modelVersion")
        if not isinstance(version, str) or not (version == job["model"] or version.startswith(job["model"] + "-")):
            raise r.Error("classification recovery model differs")
        normalized, adjustments = normalize(job, r.response_payload("gemini", raw["response"]))
        old = outcomes[job["job_id"]]
        if old["state"] == "completed" and normalized != old["result"]:
            raise r.Error("classification continuation would change an already valid result")
        imported = {"producer_job_id": job["job_id"], "job_id": job["job_id"],
                    "producer_collection": r.binding(r.wave_folder(plan, wave["wave_id"]) / "collection.json"),
                    "result": normalized}
        result["retained" if old["state"] == "completed" else "recovered"].append(imported)
        result["classification_adjustments"] += [{"job_id": job["job_id"], **change} for change in adjustments]
    results = [row["result"] for row in result["retained"] + result["recovered"]]
    if not r.phase_progress(selected, {**state, "results": results}, producer["config"])["transcript_phase_complete"]:
        raise r.Error("classification continuation must preserve and finish the entire canary")
    if core.next_jobs(selected, state["jobs"], {v["job_id"]: v for v in results}, producer["config"],
                      stages={"chunk", "transcript"}, initial_jobs_override=initial):
        raise r.Error("classification continuation left unaccounted dependencies")
    proofs = result["proofs"] + [base_ref, continuation["producer_manifest"], continuation["producer_code"]]
    proofs += list(r.read(continuation["producer_code"])["files"].values())
    root = Path(producer["state_root"])
    paths = [root / name for name in ("workspace.json", "manifest-binding.json")]
    paths += [root / "shard-0000" / name for name in ("workspace.json", "plan.json")]
    paths += [Path(plan["request"]["path"])]
    paths += sorted((root / "shard-0000" / "sources").glob("*.json"))
    paths += sorted(r.wave_folder(plan, wave["wave_id"]).iterdir())
    proofs += [r.binding(path) for path in paths if path.is_file() and not path.name.endswith(".lock")]
    result["proofs"] = sorted({ref["path"]: ref for ref in proofs}.values(), key=lambda ref: ref["path"])
    result["continuation_accounting"] = campaign.accounted_state(plan, state)
    return result


def load(ref, sources, config):
    from pipeline import transcript_summary_recovery as recovery
    value = r.read(ref)
    if value.get("kind") != KIND or value.get("config") != config or len(value.get("proofs", [])) > 20000:
        raise r.Error("invalid classification continuation admission")
    key = (ref["sha256"], r.digest(sources), r.digest(config))
    witnesses = [recovery._witness(proof) for proof in value["proofs"]]
    cached = _CACHE.get(key)
    if cached is not None and cached[0] == witnesses:
        return deepcopy(cached[1])
    for proof in value["proofs"]:
        r.read_bytes(proof)
    expected = build(value["base_admission"], value["continuation"], sources=sources, config=config)
    if expected != value or witnesses != [recovery._witness(proof) for proof in value["proofs"]]:
        raise r.Error("classification admission does not replay or producer changed")
    if len(r.canonical(value)) <= 128 * 1024**2:
        _CACHE.clear()  # Exactly one canary, bounded cache, no unbounded lineage.
        _CACHE[key] = witnesses, deepcopy(expected)
    return expected


def accounting(manifest, settled, held):
    """Charge the continuation's producer exactly once, in addition to its lineage."""
    from pipeline import transcript_summary_campaign as campaign
    recovery = manifest["recovery"]
    continuation = recovery["continuation"]
    parent, plan, sources, _, state = inspect(continuation["producer_manifest"], continuation["producer_code"])
    if (manifest.get("classification_policy") != POLICY or manifest["shards"] != parent["shards"] or
            manifest["config"] != parent["config"] or manifest.get("selection") != parent.get("selection") or
            parent["recovery"]["prior_usage_estimate_microusd"] != settled or
            parent["recovery"]["prior_unsettled_hold_microusd"] != held):
        raise r.Error("classification continuation partition or prior accounting differs")
    expected = deepcopy(parent["recovery"])
    imported = load(recovery["imports"]["0"], sources, manifest["config"])
    if imported["continuation"] != continuation or imported["base_admission"] != expected["imports"]["0"]:
        raise r.Error("classification continuation refers to another canary")
    extra = campaign.accounted_state(plan, state)
    expected["imports"]["0"] = recovery["imports"]["0"]
    expected["continuation"] = continuation
    expected["prior_usage_estimate_microusd"] += extra["usage_estimate_microusd"]
    expected["prior_unsettled_hold_microusd"] += extra["unsettled_hold_microusd"]
    if (expected != recovery or manifest["budget_microusd"] != parent["budget_microusd"] - extra["accounted_microusd"]):
        raise r.Error("classification continuation loses prior spending or completed work")
    return expected["prior_usage_estimate_microusd"], expected["prior_unsettled_hold_microusd"]


def prepare(manifest_ref, code_ref, audit_root, output, state_root):
    from pipeline import transcript_summary_campaign as campaign
    from pipeline import transcript_summary_recovery as recovery
    parent = recovery.producer_manifest(manifest_ref, code_ref)
    root, output, audit = map(r.safe.path_value, (state_root, output, audit_root))
    r.protect(root, {"producer_root": parent["state_root"], "audit": str(audit), "output": str(output)})
    if r.safe.exists(root) or r.safe.exists(output):
        raise r.Error("classification continuation requires a fresh workspace and manifest")
    r.mkdir(audit)
    continuation = {"producer_manifest": manifest_ref, "producer_code": code_ref}
    admission = build(parent["recovery"]["imports"]["0"], continuation)
    bound = r.put(audit / "canary-admission.json", admission)
    extra = admission["continuation_accounting"]
    value = deepcopy(parent)
    value.update(state_root=str(root), implementation=campaign.implementation(), classification_policy=POLICY,
                 budget_microusd=parent["budget_microusd"] - extra["accounted_microusd"])
    value["recovery"]["imports"]["0"] = bound
    value["recovery"]["continuation"] = continuation
    value["recovery"]["prior_usage_estimate_microusd"] += extra["usage_estimate_microusd"]
    value["recovery"]["prior_unsettled_hold_microusd"] += extra["unsettled_hold_microusd"]
    campaign.validate_manifest(value)
    return {"state": "classification_continuation_prepared_offline", "manifest": r.put(output, value),
            "imported_final_summaries": sum(row["result"]["stage"] == "transcript" for row in admission["retained"] + admission["recovered"]),
            "classification_adjustments": len(admission["classification_adjustments"]), "new_paid_requests": 0}


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    for name in ("producer-manifest", "producer-sha256", "producer-code", "producer-code-sha256", "audit-root", "output", "state-root"):
        parser.add_argument("--" + name, required=True)
    args = parser.parse_args(argv)
    result = prepare({"path": args.producer_manifest, "sha256": args.producer_sha256},
        {"path": args.producer_code, "sha256": args.producer_code_sha256}, args.audit_root, args.output, args.state_root)
    print(r.canonical(result).decode().strip(), flush=True)


if __name__ == "__main__":
    try:
        main()
    except (RuntimeError, ValueError, OSError, KeyError, TypeError) as error:
        print("Classification continuation stopped: " + str(error), file=sys.stderr)
        raise SystemExit(2)
