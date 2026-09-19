"""Read-only, finite diarization selection from completed archive screens.

The screening model is not executed. Only already-published complete result
files are considered, their checkpoint evidence is replayed, and source inode
metadata is checked. Screen labels are fallible routing hints, not identities,
human review, exact speaker counts, or evidence that a recording is solo.
"""
from __future__ import annotations

import argparse
from contextlib import contextmanager
import copy
import hashlib
import json
import os
from pathlib import Path
import stat
import sys

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))
from pipeline import speaker_screen as screen
from pipeline import speaker_screen_archive_guided as archive
from pipeline import speaker_screen_campaign as campaign
from pipeline import speaker_screen_paths as paths

SelectionError = screen.ScreenError
MAX_RECORDINGS = campaign.MAX_BATCHES * archive.old_batch.MAX_JOBS
LABELS = {"multiple_speaker_candidate", "uncertain", "no_second_voice_detected_in_sampled_audio"}
RECORD_FIELDS = {"recording", "source_witness", "screening_result", "screening_plan",
    "screening_source_binding", "screening_batch", "selected_label", "selection_reason", "screening_evidence", "metadata"}
BATCH_FIELDS = {"kind", "schema_version", "request", "work_orders", "guidance", "guidance_sources",
    "max_target_windows", "state_root", "execution", "runtime_binding", "implementation", "models",
    "plans", "semantics", "batch_id"}
CAMPAIGN_FIELDS = {"kind", "schema_version", "request", "campaign_root", "python", "implementation",
    "batches", "max_passes_per_batch", "max_run_seconds", "device", "gpu_uuid", "semantics", "campaign_id"}
SELECTION_FIELDS = {"kind", "schema_version", "source", "options", "selection_complete", "records",
    "pending_media_ids", "counts", "implementation", "semantics", "selection_id"}
SEMANTICS = {"read_only": True, "finite_snapshot": True, "future_results_automatically_appended": False,
    "only_complete_immutable_results_replayed": True, "models_executed": False,
    "screen_is_human_review": False, "screen_labels_are_ground_truth": False,
    "identity_inferred": False, "exact_two_speakers_inferred": False, "solo_inferred": False,
    "media_ids_are_filter_not_label_override": True, "source_witness_currently_verified": True,
    "source_media_fully_rehashed_by_selector": False, "source_mutation": False,
    "controller_mutation": False, "publication_authority": False}


def _implementation():
    return {"screened_diarization_selection.py": hashlib.sha256(Path(__file__).read_bytes()).hexdigest(),
            **archive._implementation()}


def _version(value, kind):
    if value.get("kind") != kind or type(value.get("schema_version")) is not int or value["schema_version"] != 1:
        raise SelectionError("unsupported screening evidence kind/version")


def _identifier(value, key, prefix):
    original = {name: item for name, item in value.items() if name != key}
    if value.get(key) != prefix + screen.digest(original)[:32]:
        raise SelectionError("screening document canonical identity differs")


def _json(body):
    def pairs(values):
        result = {}
        for key, value in values:
            if key in result:
                raise SelectionError("duplicate evidence JSON field")
            result[key] = value
        return result
    try:
        value = json.loads(body, object_pairs_hook=pairs,
            parse_constant=lambda _: (_ for _ in ()).throw(SelectionError("nonfinite evidence JSON")))
        if not isinstance(value, dict):
            raise SelectionError("evidence document must be an object")
        screen.canonical(value)
        return value
    except (ValueError, UnicodeError, RecursionError) as error:
        raise SelectionError("invalid screening evidence JSON") from error


@contextmanager
def _stable_document(path, expected=None, *, private=True):
    """Keep a no-symlink descriptor through validation and detect replacement.

    The existing writer's immutable-publication protocol uses mode 0600, not a
    read-only mode bit. Both exact owner-private 0600 and 0400 are supported;
    this reader never changes permissions or takes the live writer's lock.
    """
    path = screen.path_value(path)
    with screen.opened(path) as descriptor:
        before = screen.witness(descriptor)
        if private and (before["st_uid"] != os.getuid() or stat.S_IMODE(before["st_mode"]) not in (0o400, 0o600)):
            raise SelectionError("screening evidence must remain owner-private 0400/0600")
        if not 0 < before["st_size"] <= screen.MAX_JSON:
            raise SelectionError("screening evidence exceeds bounded JSON size")
        body = os.pread(descriptor, screen.MAX_JSON + 1, 0)
        if len(body) != before["st_size"] or screen.witness(descriptor) != before:
            raise SelectionError("screening evidence changed during read")
        binding = {"path": str(path), "sha256": hashlib.sha256(body).hexdigest()}
        if expected is not None and binding["sha256"] != expected:
            raise SelectionError("screening evidence SHA-256 differs")
        value = _json(body)
        yield value, binding, before
        if screen.witness(descriptor) != before:
            raise SelectionError("screening evidence changed during verification")
        with screen.opened(path) as current:
            if screen.witness(current) != before:
                raise SelectionError("screening evidence path was replaced during verification")


def _bound_document(binding):
    screen.file_binding(binding)
    with _stable_document(binding["path"], binding["sha256"]) as (value, _ref, _witness):
        return value


def _runtime(value, execution):
    """Validate historical pinned provenance without importing model packages."""
    screen.exact(value, {"python", "versions", "recipe", "nvidia_driver_version"}, "historical screen runtime")
    engine = archive.resident.engine
    versions = engine.CPU_RUNTIME_PINS if execution["device"] == "cpu" else engine.CUDA_RUNTIME_PINS
    if value["versions"] != versions or value["recipe"] != engine.model_recipe(execution["device"]):
        raise SelectionError("screen runtime recipe or pinned package provenance differs")
    python = value["python"]
    screen.exact(python, {"path", "resolved_path", "sha256", "python_version", "venv_configuration"}, "historical Python")
    for key in ("path", "resolved_path"):
        screen.path_value(python[key])
    screen.file_binding({"path": python["resolved_path"], "sha256": python["sha256"]})
    if not isinstance(python["python_version"], str) or not 1 <= len(python["python_version"]) <= 1024:
        raise SelectionError("invalid historical Python version")
    if python["venv_configuration"] is not None:
        screen.file_binding(python["venv_configuration"])
    if execution["device"] == "cpu":
        if value["nvidia_driver_version"] is not None:
            raise SelectionError("CPU screen cannot bind a CUDA driver")
    elif not isinstance(value["nvidia_driver_version"], str) or not 1 <= len(value["nvidia_driver_version"]) <= 64:
        raise SelectionError("CUDA screen lacks bounded historical driver provenance")


def _load_batch(binding, *, rebuild=True):
    value = _bound_document(binding)
    screen.exact(value, BATCH_FIELDS, "archive screening manifest")
    _version(value, "himr_archive_guided_speaker_screen_manifest")
    _identifier(value, "batch_id", "archivebatch_")
    root = screen.path_value(value["state_root"])
    if Path(binding["path"]) != root / "manifest.json":
        raise SelectionError("archive manifest is outside its sealed workspace")
    archive._workspace(root)
    if value["implementation"] != archive._implementation():
        raise SelectionError("frozen screening implementation differs")
    execution = archive.validate_execution(value["execution"])
    if screen.canonical(execution) != screen.canonical(value["execution"]):
        raise SelectionError("archive execution configuration is not normalized")
    _runtime(value["runtime_binding"], execution)
    screen.integer(value["max_target_windows"], 0, 64, "archive target budget")
    plans = value["plans"]
    if not isinstance(plans, list) or not 1 <= len(plans) <= archive.old_batch.MAX_JOBS:
        raise SelectionError("archive manifest plan count exceeds bound")
    originals = archive.old_batch._load_orders(value["work_orders"]) if rebuild else None
    if rebuild:
        normalized = archive.guidance_api.load_guidance(value["guidance"], originals)
        if normalized["source_bindings"] != value["guidance_sources"]:
            raise SelectionError("archive guidance source bindings differ")
        records = normalized["recordings"]
        indices = sorted(range(len(originals)), key=lambda index: (-records[index]["priority"]["score"], index))
        if len(plans) != len(indices):
            raise SelectionError("archive original-order count differs")
    seen_media = set()
    for index, plan in enumerate(plans):
        screen.exact(plan, {"kind", "schema_version", "index", "original_index", "work_order", "original_output_root",
            "order", "guidance", "sampling", "execution", "runtime_binding", "implementation", "windows", "batches", "plan_id"},
            "archive recording plan")
        _version(plan, "himr_archive_guided_speaker_screen_plan")
        _identifier(plan, "plan_id", "archivescreen_")
        if (type(plan["index"]) is not int or plan["index"] != index or plan["implementation"] != value["implementation"]
                or plan["execution"] != execution or plan["runtime_binding"] != value["runtime_binding"]
                or plan["order"]["output_root"] != str(root) or plan["order"]["models"] != value["models"]):
            raise SelectionError("archive recording plan does not match its batch")
        identity = plan["order"]["recording"]["media_id"]
        if identity in seen_media:
            raise SelectionError("archive batch repeats a recording")
        seen_media.add(identity)
        archive.guided_core.validate_sampling(plan["order"]["recording"]["duration_ms"], plan["order"]["policy"], plan["sampling"])
        if plan["windows"] != plan["sampling"]["windows"]:
            raise SelectionError("archive plan windows differ from admitted interval sampling")
        width = min(execution["batch_size"], plan["order"]["resources"]["max_windows_per_run"])
        if plan["batches"] != [plan["windows"][start:start + width] for start in range(0, len(plan["windows"]), width)]:
            raise SelectionError("archive plan checkpoint partition differs")
        if rebuild:
            original_index = indices[index]
            order = copy.deepcopy(originals[original_index]["order"])
            original_output, order["output_root"] = order["output_root"], str(root)
            expected = {**plan, "original_index": original_index, "order": order,
                "original_output_root": original_output, "work_order": value["work_orders"][original_index],
                "guidance": records[original_index], "sampling": archive.guided_core.build_sampling(
                    order["recording"]["duration_ms"], order["policy"], records[original_index]["targets"],
                    max_target_windows=value["max_target_windows"])}
            if screen.canonical(expected) != screen.canonical(plan):
                raise SelectionError("archive plan differs from original orders or verified guidance")
    if value["request"] is not None:
        request = _bound_document(value["request"])
        screen.exact(request, {"kind", "schema_version", "work_orders", "guidance", "state_root", "execution", "max_target_windows"},
                     "archive screening request")
        _version(request, "himr_archive_guided_speaker_screen_request")
        expected = {"kind": request["kind"], "schema_version": 1,
            **{key: value[key] for key in ("work_orders", "guidance", "state_root", "max_target_windows")},
            "execution": execution}
        if screen.canonical({**request, "execution": archive.validate_execution(request["execution"])}) != screen.canonical(expected):
            raise SelectionError("archive request differs from sealed batch")
    return value


def _source_witness(recording, expected):
    with screen.opened(recording["path"]) as descriptor:
        current = screen.witness(descriptor)
        if current != expected or current["st_size"] != recording["byte_count"]:
            raise SelectionError("screened source metadata changed; fresh admission is required")


def _completed_record(batch_binding, batch, plan):
    root, job = Path(batch["state_root"]), Path(batch["state_root"]) / plan["plan_id"]
    result_path = job / "result.json"
    if not screen.exists(result_path):
        return None
    # Holding the completed result descriptor across replay prevents a file
    # replacement from turning an in-progress checkpoint prefix into evidence.
    with _stable_document(result_path) as (saved, result_ref, _result_witness):
        _version(saved, "himr_archive_guided_speaker_screen_result")
        if (saved.get("plan_id") != plan["plan_id"] or saved.get("recording") != plan["order"]["recording"]
                or saved.get("state") != "completed" or saved.get("screening_decision_complete") is not True
                or saved.get("completed_windows") != len(plan["windows"])
                or saved.get("baseline_inspected_windows") != len(plan["sampling"]["baseline_indices"])):
            raise SelectionError("published screen result does not establish complete admitted-interval sampling")
        evidence_witnesses = []
        with _stable_document(job / "plan.json", screen.digest(plan)) as (saved_plan, plan_ref, witness):
            if saved_plan != plan:
                raise SelectionError("saved recording plan differs")
            evidence_witnesses.append((plan_ref["path"], witness))
        with _stable_document(job / "source-binding.json") as (source_binding, source_ref, witness):
            screen.validate_binding(source_binding, plan)
            evidence_witnesses.append((source_ref["path"], witness))
        proofs = saved.get("checkpoint_hashes")
        if not isinstance(proofs, list) or len(proofs) != len(plan["batches"]):
            raise SelectionError("completed result lacks every fixed checkpoint batch")
        for index, proof in enumerate(proofs):
            screen.exact(proof, {"batch_index", "sha256"}, "screen checkpoint proof")
            if type(proof["batch_index"]) is not int or proof["batch_index"] != index:
                raise SelectionError("completed result checkpoint sequence differs")
            with _stable_document(job / f"batch-{index:04d}.json", proof["sha256"]) as (_value, ref, witness):
                evidence_witnesses.append((ref["path"], witness))
        _source_witness(plan["order"]["recording"], source_binding["source_witness"])
        replay = archive._read_job(plan, root)
        if screen.canonical(replay) != screen.canonical(saved):
            raise SelectionError("completed screening result differs from full checkpoint replay")
        for path, witness in evidence_witnesses:
            with screen.opened(path) as descriptor:
                if screen.witness(descriptor) != witness:
                    raise SelectionError("screening evidence changed during checkpoint replay")
        _source_witness(plan["order"]["recording"], source_binding["source_witness"])
        label = replay["summary"]["status"]
        if label not in LABELS:
            raise SelectionError("unrecognized screening label")
        record = {"recording": copy.deepcopy(plan["order"]["recording"]),
            "source_witness": source_binding["source_witness"], "screening_result": result_ref,
            "screening_plan": plan_ref, "screening_source_binding": source_ref,
            "screening_batch": dict(batch_binding), "selected_label": label,
            "selection_reason": "uncertain_opt_in" if label == "uncertain" else label,
            "screening_evidence": {"plan_id": plan["plan_id"], "completed_windows": replay["completed_windows"],
                "planned_windows": replay["planned_windows"], "baseline_inspected_windows": replay["baseline_inspected_windows"],
                "baseline_planned_windows": replay["baseline_planned_windows"], "checkpoint_replay_verified": True,
                "source_sha256_reverified": source_binding["source_sha256_reverified"], "human_reviewed": False},
            "metadata": {"title": plan["guidance"]["title"], "date": plan["guidance"]["date"]}}
        return record


def _options(include_uncertain, media_ids):
    if type(include_uncertain) is not bool:
        raise SelectionError("include_uncertain must be boolean")
    if media_ids is None:
        return None
    if not isinstance(media_ids, list) or len(media_ids) > MAX_RECORDINGS:
        raise SelectionError("media_ids must be a finite explicit list")
    for value in media_ids:
        if not isinstance(value, str) or not screen.IDENTIFIER.fullmatch(value):
            raise SelectionError("invalid selected media identity")
    if len(set(media_ids)) != len(media_ids):
        raise SelectionError("media_ids repeats an identity")
    return set(media_ids)


def _select(source_kind, source_binding, batches, *, include_uncertain, media_ids):
    requested = _options(include_uncertain, media_ids)
    known, selected, pending = set(), [], []
    labels = {label: 0 for label in sorted(LABELS)}
    completed = 0
    for binding, batch in batches:
        for plan in batch["plans"]:
            identity = plan["order"]["recording"]["media_id"]
            if identity in known:
                raise SelectionError("selection source repeats a recording across batches")
            known.add(identity)
            record = _completed_record(binding, batch, plan)
            if record is None:
                pending.append(identity)
                continue
            completed += 1
            labels[record["selected_label"]] += 1
            if requested is not None and identity not in requested:
                continue
            if record["selected_label"] == "multiple_speaker_candidate" or (
                    include_uncertain and record["selected_label"] == "uncertain"):
                selected.append(record)
    if requested is not None and requested - known:
        raise SelectionError("explicit media_ids are absent from the sealed selection source")
    value = {"kind": "himr_screened_diarization_selection", "schema_version": 1,
        "source": {"kind": source_kind, "binding": dict(source_binding)},
        "options": {"include_uncertain": include_uncertain, "media_ids": None if requested is None else sorted(requested)},
        "selection_complete": not pending, "records": selected, "pending_media_ids": pending,
        "counts": {"source_recordings": len(known), "completed_screening_results": completed,
            "pending_screening_results": len(pending), "selected_recordings": len(selected), **labels},
        "implementation": _implementation(), "semantics": dict(SEMANTICS)}
    result = {**value, "selection_id": "screenseldiar_" + screen.digest(value)[:32]}
    if len(screen.canonical(result)) > screen.MAX_JSON:
        raise SelectionError("selection exceeds bounded JSON size; use a smaller explicit media_ids filter")
    return result


def select_batch(batch_binding, *, include_uncertain=False, media_ids=None):
    """Snapshot one explicitly bound archive batch without acquiring its lock."""
    batch = _load_batch(batch_binding)
    return _select("batch", batch_binding, [(batch_binding, batch)], include_uncertain=include_uncertain, media_ids=media_ids)


def _load_campaign(campaign_binding, *, rebuild=True):
    """Load only the sealed source catalog, never recording results or status."""
    value = _bound_document(campaign_binding)
    screen.exact(value, CAMPAIGN_FIELDS, "archive screening campaign")
    _version(value, "himr_guided_speaker_screen_campaign")
    _identifier(value, "campaign_id", "guidedscreencampaign_")
    root = screen.path_value(value["campaign_root"])
    if Path(campaign_binding["path"]) != root / "manifest.json":
        raise SelectionError("screening campaign manifest is outside its workspace")
    campaign._workspace(root)
    if value["implementation"] != campaign._implementation():
        raise SelectionError("frozen screening campaign implementation differs")
    rows = value["batches"]
    if not isinstance(rows, list) or not 1 <= len(rows) <= campaign.MAX_BATCHES:
        raise SelectionError("campaign batch count exceeds finite bound")
    screen.integer(value["max_passes_per_batch"], 1, campaign.MAX_PASSES, "campaign pass limit")
    screen.integer(value["max_run_seconds"], 10, campaign.MAX_SECONDS, "campaign time limit")
    if value["request"] is not None:
        request = _bound_document(value["request"])
        expected = {"kind": "himr_guided_speaker_screen_campaign_request", "schema_version": 1,
            "campaign_root": value["campaign_root"], "python": value["python"],
            "batches": [row["manifest"] for row in rows], "max_passes_per_batch": value["max_passes_per_batch"],
            "max_run_seconds": value["max_run_seconds"]}
        if screen.canonical(request) != screen.canonical(expected):
            raise SelectionError("campaign request differs from sealed manifest")
    batches = []
    for index, row in enumerate(rows):
        screen.exact(row, {"index", "manifest", "batch_id", "planned_counts", "batch_max_run_seconds"}, "campaign batch")
        if type(row["index"]) is not int or row["index"] != index:
            raise SelectionError("campaign batch order differs")
        batch = _load_batch(row["manifest"], rebuild=rebuild)
        if (row["batch_id"] != batch["batch_id"] or row["batch_max_run_seconds"] != batch["execution"]["max_run_seconds"]
                or value["python"] != batch["runtime_binding"]["python"] or value["device"] != batch["execution"]["device"]
                or value["gpu_uuid"] != batch["execution"]["gpu_uuid"]):
            raise SelectionError("campaign and batch execution identities differ")
        counts = {key: 0 for key in campaign.COUNT_KEYS}
        for plan in batch["plans"]:
            counts["recordings"] += 1
            counts["planned_windows"] += len(plan["windows"])
            counts["baseline_planned_windows"] += len(plan["sampling"]["baseline_indices"])
            counts["targeted_planned_windows"] += len(plan["sampling"]["target_indices"])
        counts["baseline_remaining_windows"] = counts["baseline_planned_windows"]
        counts["targeted_remaining_windows"] = counts["targeted_planned_windows"]
        if screen.canonical(counts) != screen.canonical(row["planned_counts"]):
            raise SelectionError("campaign planned counts differ from exact sealed windows")
        batches.append((row["manifest"], batch))
    return batches


def select_campaign(campaign_binding, *, include_uncertain=False, media_ids=None):
    """Snapshot explicit campaign batches; absent future results remain pending."""
    return _select("campaign", campaign_binding, _load_campaign(campaign_binding),
                   include_uncertain=include_uncertain, media_ids=media_ids)


def verify_selected_record(record):
    """Reverify one sealed selected record, independent of new campaign results."""
    screen.exact(record, RECORD_FIELDS, "selected diarization recording")
    if record["selected_label"] not in ("multiple_speaker_candidate", "uncertain"):
        raise SelectionError("record label is not eligible for diarization selection")
    batch = _load_batch(record["screening_batch"], rebuild=False)
    matches = [plan for plan in batch["plans"] if plan["order"]["recording"]["media_id"] == record["recording"]["media_id"]]
    if len(matches) != 1:
        raise SelectionError("selected recording is absent or ambiguous in sealed batch")
    actual = _completed_record(record["screening_batch"], batch, matches[0])
    if actual is None or screen.canonical(actual) != screen.canonical(record):
        raise SelectionError("selected screening evidence or source no longer matches sealed selection")
    return actual


def validate_selection_snapshot(document, *, source, include_uncertain=False, media_ids=None):
    """Verify one finite historical snapshot without discovering new results.

    Selected receipts and their current source witnesses are replayed. Historical
    aggregate counts for unselected recordings are checked for consistency, not
    refreshed from a campaign that may have completed more work since selection.
    """
    screen.exact(document, SELECTION_FIELDS, "diarization selection snapshot")
    _version(document, "himr_screened_diarization_selection")
    _identifier(document, "selection_id", "screenseldiar_")
    if len(screen.canonical(document)) > screen.MAX_JSON:
        raise SelectionError("selection snapshot exceeds bounded JSON size")
    screen.exact(source, {"kind", "binding"}, "selection source")
    if source["kind"] not in ("batch", "campaign"):
        raise SelectionError("unsupported selection source")
    screen.file_binding(source["binding"])
    requested = _options(include_uncertain, media_ids)
    expected_options = {"include_uncertain": include_uncertain,
                        "media_ids": None if requested is None else sorted(requested)}
    if (screen.canonical(document["source"]) != screen.canonical(source)
            or screen.canonical(document["options"]) != screen.canonical(expected_options)
            or screen.canonical(document["semantics"]) != screen.canonical(SEMANTICS)
            or document["implementation"] != _implementation()):
        raise SelectionError("selection source, options, semantics or implementation differs")
    batches = (_load_campaign(source["binding"], rebuild=False) if source["kind"] == "campaign" else
               [(source["binding"], _load_batch(source["binding"], rebuild=False))])
    catalog = {}
    for binding, batch in batches:
        for plan in batch["plans"]:
            identity = plan["order"]["recording"]["media_id"]
            if identity in catalog:
                raise SelectionError("selection source repeats a recording")
            catalog[identity] = {"ordinal": len(catalog), "binding": binding, "plan": plan}
    if requested is not None and requested - catalog.keys():
        raise SelectionError("selection filter is outside its sealed source")
    records, pending = document["records"], document["pending_media_ids"]
    if not isinstance(records, list) or len(records) > len(catalog):
        raise SelectionError("selection record count exceeds its finite source")
    if (not isinstance(pending, list) or any(not isinstance(identity, str) for identity in pending)
            or len(set(pending)) != len(pending) or set(pending) - catalog.keys()
            or pending != sorted(pending, key=lambda identity: catalog[identity]["ordinal"])):
        raise SelectionError("historical pending recording list differs from source order")
    if type(document["selection_complete"]) is not bool or document["selection_complete"] != (not pending):
        raise SelectionError("historical selection completion differs from pending count")
    counts = document["counts"]
    screen.exact(counts, {"source_recordings", "completed_screening_results", "pending_screening_results",
                         "selected_recordings", *LABELS}, "selection counts")
    for key, value in counts.items():
        screen.integer(value, 0, len(catalog), "selection count " + key)
    if (counts["source_recordings"] != len(catalog) or counts["pending_screening_results"] != len(pending)
            or counts["completed_screening_results"] != len(catalog) - len(pending)
            or sum(counts[label] for label in LABELS) != counts["completed_screening_results"]
            or counts["selected_recordings"] != len(records)):
        raise SelectionError("historical selection counts are inconsistent")
    seen, last_ordinal = set(), -1
    selected_labels = {label: 0 for label in LABELS}
    for record in records:
        screen.exact(record, RECORD_FIELDS, "selected diarization recording")
        if not isinstance(record["recording"], dict) or not isinstance(record["screening_evidence"], dict):
            raise SelectionError("selected recording proof must be an object")
        identity = record["recording"].get("media_id")
        if not isinstance(identity, str) or identity not in catalog or identity in seen or identity in pending:
            raise SelectionError("selected recording is outside completed snapshot source")
        entry = catalog[identity]
        if (entry["ordinal"] <= last_ordinal or record["screening_batch"] != entry["binding"]
                or record["recording"] != entry["plan"]["order"]["recording"]
                or record["screening_evidence"].get("plan_id") != entry["plan"]["plan_id"]
                or (requested is not None and identity not in requested)):
            raise SelectionError("selected recording order, membership or filter differs")
        label = record["selected_label"]
        if label != "multiple_speaker_candidate" and not (include_uncertain and label == "uncertain"):
            raise SelectionError("selected label is not eligible under snapshot options")
        verify_selected_record(record)
        selected_labels[label] += 1
        seen.add(identity)
        last_ordinal = entry["ordinal"]
    if any(selected_labels[label] > counts[label] for label in LABELS):
        raise SelectionError("selected labels exceed historical completed label counts")
    if requested is None and (selected_labels["multiple_speaker_candidate"] != counts["multiple_speaker_candidate"]
            or (include_uncertain and selected_labels["uncertain"] != counts["uncertain"])):
        raise SelectionError("unfiltered snapshot omits eligible historical selections")
    return copy.deepcopy(document)


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--source-kind", required=True, choices=("campaign", "batch"))
    parser.add_argument("--manifest", required=True)
    parser.add_argument("--expected-sha256", required=True)
    parser.add_argument("--include-uncertain", action="store_true")
    parser.add_argument("--media-id", action="append", dest="media_ids")
    args = parser.parse_args(argv)
    try:
        function = select_campaign if args.source_kind == "campaign" else select_batch
        result = function({"path": args.manifest, "sha256": args.expected_sha256},
                          include_uncertain=args.include_uncertain, media_ids=args.media_ids)
        print(json.dumps(result, sort_keys=True, indent=2, allow_nan=False))
        return 0
    except (SelectionError, OSError, ValueError, KeyError, TypeError) as error:
        print(f"ScreenedDiarizationSelectionError: {error}", file=sys.stderr)
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
