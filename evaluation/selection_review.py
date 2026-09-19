"""ASR-blind human interval-selection review and deterministic freeze compilation.

The immutable proposal documents remain machine-generated and unreviewed.  This
module creates a separate private review manifest, validates a completed direct-
parent-media review, and compiles the accepted parent-coordinate intervals into a
schema-v2 freeze.  It never reads ASR or reference text and never writes files or
the catalogue.
"""

from __future__ import annotations

import hashlib
from collections import defaultdict
from pathlib import Path
from typing import Any, Sequence

from .interval_proposal import validate_interval_proposal
from .validation import (
    SPLITS,
    MINIMUM_SELECTION_DURATION_MS,
    ContractError,
    _array,
    _canonical_bytes,
    _choice,
    _constant,
    _id,
    _integer,
    _object,
    _stable_id,
    _string,
    _timestamp,
    _validate_flags,
    _verify_manifest_digest,
    canonical_manifest_sha256,
    validate_candidate_cohort,
    validate_interval_freeze,
)


MINIMUM_ACCEPTED_DURATION_MS = MINIMUM_SELECTION_DURATION_MS
PROTOCOL_REVISION_DEFAULT = "transcript_eval_protocol_v1"
REJECTION_REASONS = {
    "boundary_requires_reproposal",
    "duplicate_or_redundant",
    "no_usable_speech",
    "out_of_scope",
    "playback_only",
    "privacy_or_sensitivity",
    "technical_quality",
    "other_reviewed_reason",
}
TRIM_REASONS = {
    "remove_non_speech_edge",
    "remove_sensitive_edge",
    "speech_boundary_refinement",
    "other_reviewed_trim",
}
REVIEW_STATES = {"incomplete_template", "completed_private"}


def _fail(path: str, message: str) -> None:
    raise ContractError(f"{path}: {message}")


def _proposal_input_value(
    ordinal: int, request: dict[str, Any], proposal: dict[str, Any]
) -> dict[str, Any]:
    return {
        "ordinal": ordinal,
        "request_schema_version": request["schema_version"],
        "request_id": request["request_id"],
        "request_manifest_sha256": request["manifest_sha256"],
        "proposal_schema_version": proposal["schema_version"],
        "proposal_id": proposal["proposal_id"],
        "proposal_manifest_sha256": proposal["manifest_sha256"],
    }


def _validated_bundles(
    candidate_cohort: object,
    requests: Sequence[object],
    proposals: Sequence[object],
    catalog_path: str | Path,
) -> tuple[dict[str, Any], list[dict[str, Any]]]:
    cohort = validate_candidate_cohort(candidate_cohort)
    if len(requests) != 2 or len(proposals) != 2:
        _fail("$.proposal_inputs", "requires exactly two matching request/proposal pairs")

    cohort_order = {
        row["candidate_id"]: index for index, row in enumerate(cohort["candidates"])
    }
    bundles: list[dict[str, Any]] = []
    seen_proposal_ids: set[str] = set()
    seen_request_ids: set[str] = set()
    for index, (request_value, proposal_value) in enumerate(zip(requests, proposals)):
        if not isinstance(request_value, dict) or not isinstance(proposal_value, dict):
            _fail(f"$.proposal_inputs[{index}]", "request and proposal must be objects")
        proposal = validate_interval_proposal(
            proposal_value, request_value, cohort, catalog_path
        )
        request = request_value
        if request["schema_version"] != proposal["schema_version"]:
            _fail(f"$.proposal_inputs[{index}]", "request/proposal schema versions differ")
        if proposal["proposal_id"] in seen_proposal_ids:
            _fail(f"$.proposal_inputs[{index}].proposal_id", "must be unique")
        if request["request_id"] in seen_request_ids:
            _fail(f"$.proposal_inputs[{index}].request_id", "must be unique")
        seen_proposal_ids.add(proposal["proposal_id"])
        seen_request_ids.add(request["request_id"])
        positions: list[int] = []
        for recording_index, recording in enumerate(proposal["recordings"]):
            candidate_id = recording["candidate_id"]
            if candidate_id not in cohort_order:
                _fail(
                    f"$.proposal_inputs[{index}].recordings[{recording_index}].candidate_id",
                    "is not in the candidate cohort",
                )
            positions.append(cohort_order[candidate_id])
        if positions != sorted(positions):
            _fail(f"$.proposal_inputs[{index}].recordings", "must follow cohort order")
        bundles.append(
            {
                "request": request,
                "proposal": proposal,
                "positions": positions,
            }
        )

    bundles.sort(key=lambda item: tuple(item["positions"]))
    seen_candidates: set[str] = set()
    seen_recordings: set[str] = set()
    seen_sources: set[str] = set()
    seen_intervals: set[str] = set()
    for bundle_index, bundle in enumerate(bundles):
        proposal = bundle["proposal"]
        for recording_index, recording in enumerate(proposal["recordings"]):
            path = f"$.proposal_inputs[{bundle_index}].recordings[{recording_index}]"
            candidate_id = recording["candidate_id"]
            candidate = cohort["candidates"][cohort_order[candidate_id]]
            for field in ("recording_id", "source_id"):
                if recording[field] != candidate[field]:
                    _fail(f"{path}.{field}", "does not match the candidate cohort")
            if recording["source_native_id"] != candidate["native_id"]:
                _fail(f"{path}.source_native_id", "does not match the candidate cohort")
            for seen, value, field in (
                (seen_candidates, candidate_id, "candidate_id"),
                (seen_recordings, recording["recording_id"], "recording_id"),
                (seen_sources, recording["source_id"], "source_id"),
            ):
                if value in seen:
                    _fail(f"{path}.{field}", "appears in more than one proposal")
                seen.add(value)
            for interval_index, interval in enumerate(recording["intervals"]):
                interval_id = interval["interval_id"]
                if interval_id in seen_intervals:
                    _fail(
                        f"{path}.intervals[{interval_index}].interval_id",
                        "must be globally unique across proposals",
                    )
                seen_intervals.add(interval_id)
                if proposal["schema_version"] == 2:
                    if (
                        interval["start_ms"] != interval["parent_start_ms"]
                        or interval["end_ms"] != interval["parent_end_ms"]
                    ):
                        _fail(
                            f"{path}.intervals[{interval_index}]",
                            "schema-v2 interval must expose exact parent coordinates",
                        )

    expected_candidates = set(cohort_order)
    if seen_candidates != expected_candidates:
        missing = sorted(expected_candidates - seen_candidates)
        extra = sorted(seen_candidates - expected_candidates)
        _fail(
            "$.proposal_inputs",
            f"proposal union must cover the exact cohort; missing={missing}, extra={extra}",
        )
    return cohort, bundles


def _ordered_proposal_inputs(bundles: Sequence[dict[str, Any]]) -> list[dict[str, Any]]:
    return [
        _proposal_input_value(index, bundle["request"], bundle["proposal"])
        for index, bundle in enumerate(bundles, start=1)
    ]


def _ordered_recording_pairs(
    cohort: dict[str, Any], bundles: Sequence[dict[str, Any]]
) -> list[tuple[dict[str, Any], dict[str, Any]]]:
    """Return proposal/recording pairs in global cohort order."""

    cohort_order = {
        row["candidate_id"]: index for index, row in enumerate(cohort["candidates"])
    }
    pairs = [
        (bundle["proposal"], recording)
        for bundle in bundles
        for recording in bundle["proposal"]["recordings"]
    ]
    return sorted(pairs, key=lambda pair: cohort_order[pair[1]["candidate_id"]])


def _expected_review_id(
    cohort: dict[str, Any],
    proposal_inputs: Sequence[dict[str, Any]],
    created_at: str,
    protocol_revision: str,
) -> str:
    """Return the stable plan ID; the digest identifies an exact review revision."""

    return _stable_id(
        "selection_review",
        cohort["cohort_id"],
        cohort["manifest_sha256"],
        created_at,
        protocol_revision,
        hashlib.sha256(_canonical_bytes(list(proposal_inputs))).hexdigest(),
    )


def _null_flags() -> dict[str, None]:
    return {
        "language_tags": None,
        "code_switch": None,
        "speaker_overlap": None,
        "playback_speech": None,
        "noise": None,
    }


def emit_selection_review_template(
    candidate_cohort: object,
    requests: Sequence[object],
    proposals: Sequence[object],
    catalog_path: str | Path,
    created_at: str,
    protocol_revision: str = PROTOCOL_REVISION_DEFAULT,
) -> dict[str, Any]:
    """Emit one valid, incomplete, private review template to the caller."""

    _timestamp(created_at, "$.created_at")
    _id(protocol_revision, "$.protocol.protocol_revision")
    cohort, bundles = _validated_bundles(
        candidate_cohort, requests, proposals, catalog_path
    )
    proposal_inputs = _ordered_proposal_inputs(bundles)
    review_id = _expected_review_id(
        cohort, proposal_inputs, created_at, protocol_revision
    )
    recordings: list[dict[str, Any]] = []
    interval_count = 0
    total_duration_ms = 0
    for proposal, recording in _ordered_recording_pairs(cohort, bundles):
        decisions: list[dict[str, Any]] = []
        for interval in recording["intervals"]:
            interval_count += 1
            total_duration_ms += interval["end_ms"] - interval["start_ms"]
            decisions.append(
                {
                    "selection_decision_id": _stable_id(
                        "selection_decision",
                        review_id,
                        proposal["proposal_id"],
                        interval["interval_id"],
                    ),
                    "proposal_interval_id": interval["interval_id"],
                    "proposal_start_ms": interval["start_ms"],
                    "proposal_end_ms": interval["end_ms"],
                    "decision": None,
                    "accepted_start_ms": None,
                    "accepted_end_ms": None,
                    "adjustment_reason": None,
                    "rejection_reason": None,
                    "flags": _null_flags(),
                }
            )
        recordings.append(
            {
                "candidate_id": recording["candidate_id"],
                "recording_id": recording["recording_id"],
                "source_id": recording["source_id"],
                "proposal_id": proposal["proposal_id"],
                "proposal_schema_version": proposal["schema_version"],
                "split": None,
                "intervals": decisions,
            }
        )
    review = {
        "schema_version": 1,
        "manifest_kind": "interval_selection_review",
        "manifest_sha256": "0" * 64,
        "review_id": review_id,
        "review_state": "incomplete_template",
        "created_at": created_at,
        "cohort_id": cohort["cohort_id"],
        "cohort_manifest_sha256": cohort["manifest_sha256"],
        "protocol": {
            "protocol_revision": protocol_revision,
            "minimum_accepted_duration_ms": MINIMUM_ACCEPTED_DURATION_MS,
            "split_unit": "recording",
            "interval_adjustment": "trim_only_within_proposed_parent_range",
        },
        "proposal_inputs": proposal_inputs,
        "proposal_accounting": {
            "recording_count": len(recordings),
            "interval_count": interval_count,
            "total_duration_ms": total_duration_ms,
        },
        "reviewer": {
            "reviewer_id": None,
            "review_tool": None,
            "reviewed_at": None,
            "attested_at": None,
            "direct_parent_media_reviewed": None,
            "asr_outputs_inspected": None,
            "reference_text_inspected": None,
            "selection_basis": None,
        },
        "privacy": {
            "storage_policy": "private_only",
            "publication_authority": "none",
            "reference_text_present": False,
        },
        "recordings": recordings,
    }
    review["manifest_sha256"] = canonical_manifest_sha256(review)
    _validate_review_against_bundles(review, cohort, bundles)
    return review


def _validate_review_flags(value: object, path: str, *, complete: bool) -> dict[str, Any]:
    flags = _object(
        value,
        path,
        {"language_tags", "code_switch", "speaker_overlap", "playback_speech", "noise"},
    )
    if not complete:
        if any(item is not None for item in flags.values()):
            _fail(path, "must contain only null fields until an interval is included")
        return flags
    _validate_flags(flags, path)
    return flags


def _validate_review_against_bundles(
    value: object,
    cohort: dict[str, Any],
    bundles: Sequence[dict[str, Any]],
    *,
    require_completed: bool = False,
) -> dict[str, Any]:
    review = _object(
        value,
        "$",
        {
            "schema_version",
            "manifest_kind",
            "manifest_sha256",
            "review_id",
            "review_state",
            "created_at",
            "cohort_id",
            "cohort_manifest_sha256",
            "protocol",
            "proposal_inputs",
            "proposal_accounting",
            "reviewer",
            "privacy",
            "recordings",
        },
    )
    _constant(review["schema_version"], 1, "$.schema_version")
    _constant(review["manifest_kind"], "interval_selection_review", "$.manifest_kind")
    state = _choice(review["review_state"], REVIEW_STATES, "$.review_state")
    if require_completed and state != "completed_private":
        _fail("$.review_state", "must be completed_private before freeze compilation")
    created_at = _timestamp(review["created_at"], "$.created_at")
    if review["cohort_id"] != cohort["cohort_id"]:
        _fail("$.cohort_id", "does not match the candidate cohort")
    if review["cohort_manifest_sha256"] != cohort["manifest_sha256"]:
        _fail("$.cohort_manifest_sha256", "does not bind the exact candidate cohort")

    protocol = _object(
        review["protocol"],
        "$.protocol",
        {
            "protocol_revision",
            "minimum_accepted_duration_ms",
            "split_unit",
            "interval_adjustment",
        },
    )
    protocol_revision = _id(protocol["protocol_revision"], "$.protocol.protocol_revision")
    _constant(
        protocol["minimum_accepted_duration_ms"],
        MINIMUM_ACCEPTED_DURATION_MS,
        "$.protocol.minimum_accepted_duration_ms",
    )
    _constant(protocol["split_unit"], "recording", "$.protocol.split_unit")
    _constant(
        protocol["interval_adjustment"],
        "trim_only_within_proposed_parent_range",
        "$.protocol.interval_adjustment",
    )
    expected_inputs = _ordered_proposal_inputs(bundles)
    if review["proposal_inputs"] != expected_inputs:
        _fail("$.proposal_inputs", "does not bind the exact validated request/proposal pairs")
    expected_review_id = _expected_review_id(
        cohort, expected_inputs, review["created_at"], protocol_revision
    )
    if review["review_id"] != expected_review_id:
        _fail("$.review_id", f"must equal deterministic ID {expected_review_id!r}")
    latest_proposal_time = max(
        _timestamp(bundle["proposal"]["created_at"], "$.proposal.created_at")
        for bundle in bundles
    )
    if created_at < latest_proposal_time:
        _fail("$.created_at", "cannot precede a bound proposal")

    privacy = _object(
        review["privacy"],
        "$.privacy",
        {"storage_policy", "publication_authority", "reference_text_present"},
    )
    _constant(privacy["storage_policy"], "private_only", "$.privacy.storage_policy")
    _constant(privacy["publication_authority"], "none", "$.privacy.publication_authority")
    _constant(privacy["reference_text_present"], False, "$.privacy.reference_text_present")

    reviewer = _object(
        review["reviewer"],
        "$.reviewer",
        {
            "reviewer_id",
            "review_tool",
            "reviewed_at",
            "attested_at",
            "direct_parent_media_reviewed",
            "asr_outputs_inspected",
            "reference_text_inspected",
            "selection_basis",
        },
    )
    if state == "incomplete_template":
        if any(item is not None for item in reviewer.values()):
            _fail("$.reviewer", "incomplete template reviewer fields must all be null")
    else:
        reviewer_id = _id(reviewer["reviewer_id"], "$.reviewer.reviewer_id")
        review_tool = _object(
            reviewer["review_tool"],
            "$.reviewer.review_tool",
            {"name", "version"},
        )
        _string(review_tool["name"], "$.reviewer.review_tool.name")
        _string(review_tool["version"], "$.reviewer.review_tool.version")
        reviewed_at = _timestamp(reviewer["reviewed_at"], "$.reviewer.reviewed_at")
        if reviewed_at < created_at:
            _fail("$.reviewer.reviewed_at", "cannot precede template creation")
        attested_at = _timestamp(reviewer["attested_at"], "$.reviewer.attested_at")
        if attested_at < reviewed_at:
            _fail("$.reviewer.attested_at", "cannot precede review completion")
        _constant(
            reviewer["direct_parent_media_reviewed"],
            True,
            "$.reviewer.direct_parent_media_reviewed",
        )
        _constant(reviewer["asr_outputs_inspected"], False, "$.reviewer.asr_outputs_inspected")
        _constant(
            reviewer["reference_text_inspected"],
            False,
            "$.reviewer.reference_text_inspected",
        )
        _constant(
            reviewer["selection_basis"],
            "source_metadata_and_direct_parent_media_only",
            "$.reviewer.selection_basis",
        )
        if reviewer_id == protocol_revision:
            _fail("$.reviewer.reviewer_id", "must identify a reviewer, not the protocol")

    expected_recordings = _ordered_recording_pairs(cohort, bundles)
    recordings = _array(review["recordings"], "$.recordings")
    if len(recordings) != len(expected_recordings):
        _fail("$.recordings", "must cover every proposed recording exactly once")

    proposed_count = 0
    proposed_duration = 0
    accepted_duration = 0
    accepted_count = 0
    split_recordings: defaultdict[str, int] = defaultdict(int)
    seen_decisions: set[str] = set()
    for recording_index, (raw, expected_pair) in enumerate(
        zip(recordings, expected_recordings)
    ):
        proposal, proposed_recording = expected_pair
        path = f"$.recordings[{recording_index}]"
        recording = _object(
            raw,
            path,
            {
                "candidate_id",
                "recording_id",
                "source_id",
                "proposal_id",
                "proposal_schema_version",
                "split",
                "intervals",
            },
        )
        for field in ("candidate_id", "recording_id", "source_id"):
            if recording[field] != proposed_recording[field]:
                _fail(f"{path}.{field}", "does not match proposal recording order/lineage")
        if recording["proposal_id"] != proposal["proposal_id"]:
            _fail(f"{path}.proposal_id", "does not match its proposal")
        if recording["proposal_schema_version"] != proposal["schema_version"]:
            _fail(f"{path}.proposal_schema_version", "does not match its proposal")
        if state == "incomplete_template":
            _constant(recording["split"], None, f"{path}.split")
            split = None
        else:
            split = _choice(recording["split"], set(SPLITS), f"{path}.split")
            split_recordings[split] += 1
        decisions = _array(recording["intervals"], f"{path}.intervals")
        proposed_intervals = proposed_recording["intervals"]
        if len(decisions) != len(proposed_intervals):
            _fail(f"{path}.intervals", "must decide every proposed interval exactly once")
        recording_accepted = 0
        for decision_index, (raw_decision, proposed_interval) in enumerate(
            zip(decisions, proposed_intervals)
        ):
            decision_path = f"{path}.intervals[{decision_index}]"
            decision = _object(
                raw_decision,
                decision_path,
                {
                    "selection_decision_id",
                    "proposal_interval_id",
                    "proposal_start_ms",
                    "proposal_end_ms",
                    "decision",
                    "accepted_start_ms",
                    "accepted_end_ms",
                    "adjustment_reason",
                    "rejection_reason",
                    "flags",
                },
            )
            expected_decision_id = _stable_id(
                "selection_decision",
                review["review_id"],
                proposal["proposal_id"],
                proposed_interval["interval_id"],
            )
            if decision["selection_decision_id"] != expected_decision_id:
                _fail(
                    f"{decision_path}.selection_decision_id",
                    f"must equal deterministic ID {expected_decision_id!r}",
                )
            if expected_decision_id in seen_decisions:
                _fail(f"{decision_path}.selection_decision_id", "must be globally unique")
            seen_decisions.add(expected_decision_id)
            if decision["proposal_interval_id"] != proposed_interval["interval_id"]:
                _fail(f"{decision_path}.proposal_interval_id", "does not match proposal order")
            for field in ("start_ms", "end_ms"):
                if decision[f"proposal_{field}"] != proposed_interval[field]:
                    _fail(f"{decision_path}.proposal_{field}", "does not match proposal")
            proposed_count += 1
            proposed_duration += proposed_interval["end_ms"] - proposed_interval["start_ms"]
            if state == "incomplete_template":
                _constant(decision["decision"], None, f"{decision_path}.decision")
                _constant(decision["accepted_start_ms"], None, f"{decision_path}.accepted_start_ms")
                _constant(decision["accepted_end_ms"], None, f"{decision_path}.accepted_end_ms")
                _constant(decision["adjustment_reason"], None, f"{decision_path}.adjustment_reason")
                _constant(decision["rejection_reason"], None, f"{decision_path}.rejection_reason")
                _validate_review_flags(decision["flags"], f"{decision_path}.flags", complete=False)
                continue
            selected = _choice(decision["decision"], {"include", "exclude"}, f"{decision_path}.decision")
            if selected == "include":
                start = _integer(decision["accepted_start_ms"], f"{decision_path}.accepted_start_ms")
                end = _integer(decision["accepted_end_ms"], f"{decision_path}.accepted_end_ms", minimum=1)
                if start < proposed_interval["start_ms"] or end > proposed_interval["end_ms"]:
                    _fail(decision_path, "accepted interval may trim but never shift or expand the proposed parent range")
                if end <= start:
                    _fail(decision_path, "accepted interval must be nonempty")
                trimmed = (
                    start != proposed_interval["start_ms"]
                    or end != proposed_interval["end_ms"]
                )
                if trimmed:
                    _choice(
                        decision["adjustment_reason"],
                        TRIM_REASONS,
                        f"{decision_path}.adjustment_reason",
                    )
                else:
                    _constant(
                        decision["adjustment_reason"],
                        None,
                        f"{decision_path}.adjustment_reason",
                    )
                _constant(decision["rejection_reason"], None, f"{decision_path}.rejection_reason")
                _validate_review_flags(decision["flags"], f"{decision_path}.flags", complete=True)
                accepted_duration += end - start
                accepted_count += 1
                recording_accepted += 1
            else:
                _constant(decision["accepted_start_ms"], None, f"{decision_path}.accepted_start_ms")
                _constant(decision["accepted_end_ms"], None, f"{decision_path}.accepted_end_ms")
                _constant(decision["adjustment_reason"], None, f"{decision_path}.adjustment_reason")
                _choice(decision["rejection_reason"], REJECTION_REASONS, f"{decision_path}.rejection_reason")
                _validate_review_flags(decision["flags"], f"{decision_path}.flags", complete=False)
        if state == "completed_private" and recording_accepted == 0:
            _fail(f"{path}.intervals", "must accept at least one interval for every recording")

    accounting = _object(
        review["proposal_accounting"],
        "$.proposal_accounting",
        {"recording_count", "interval_count", "total_duration_ms"},
    )
    _constant(accounting["recording_count"], len(expected_recordings), "$.proposal_accounting.recording_count")
    _constant(accounting["interval_count"], proposed_count, "$.proposal_accounting.interval_count")
    _constant(accounting["total_duration_ms"], proposed_duration, "$.proposal_accounting.total_duration_ms")
    if state == "completed_private":
        for split in SPLITS:
            if split_recordings[split] == 0:
                _fail("$.recordings", f"split {split!r} must contain at least one recording")
        if accepted_duration < MINIMUM_ACCEPTED_DURATION_MS:
            shortfall = MINIMUM_ACCEPTED_DURATION_MS - accepted_duration
            _fail(
                "$.recordings",
                f"accepted duration {accepted_duration} ms is below minimum "
                f"{MINIMUM_ACCEPTED_DURATION_MS} ms; shortfall {shortfall} ms",
            )
        if accepted_count == 0:
            _fail("$.recordings", "completed review must accept intervals")
    _verify_manifest_digest(review)
    return review


def validate_interval_selection_review(
    value: object,
    candidate_cohort: object,
    requests: Sequence[object],
    proposals: Sequence[object],
    catalog_path: str | Path,
    *,
    require_completed: bool = False,
) -> dict[str, Any]:
    """Validate an incomplete template or a completed private selection review."""

    cohort, bundles = _validated_bundles(
        candidate_cohort, requests, proposals, catalog_path
    )
    return _validate_review_against_bundles(
        value, cohort, bundles, require_completed=require_completed
    )


def _stratum_id(flags: dict[str, Any]) -> str:
    return _stable_id(
        "stratum", hashlib.sha256(_canonical_bytes(flags)).hexdigest()
    )


def compile_interval_freeze(
    value: object,
    candidate_cohort: object,
    requests: Sequence[object],
    proposals: Sequence[object],
    catalog_path: str | Path,
    frozen_at: str,
) -> dict[str, Any]:
    """Compile one completed private review into a deterministic schema-v2 freeze."""

    cohort, bundles = _validated_bundles(
        candidate_cohort, requests, proposals, catalog_path
    )
    review = _validate_review_against_bundles(
        value, cohort, bundles, require_completed=True
    )
    freeze_time = _timestamp(frozen_at, "$.frozen_at")
    review_attested_at = _timestamp(
        review["reviewer"]["attested_at"], "$.reviewer.attested_at"
    )
    if freeze_time < review_attested_at:
        _fail("$.frozen_at", "cannot precede the review attestation")
    proposal_by_id = {bundle["proposal"]["proposal_id"]: bundle["proposal"] for bundle in bundles}
    recording_by_id: dict[tuple[str, str], dict[str, Any]] = {}
    for bundle in bundles:
        proposal = bundle["proposal"]
        for recording in proposal["recordings"]:
            recording_by_id[(proposal["proposal_id"], recording["recording_id"])] = recording

    review_sha = review["manifest_sha256"]
    freeze_id = _stable_id(
        "freeze",
        cohort["manifest_sha256"],
        review_sha,
        review["protocol"]["protocol_revision"],
        frozen_at,
    )
    recordings: list[dict[str, Any]] = []
    split_recordings: defaultdict[str, set[str]] = defaultdict(set)
    split_intervals: defaultdict[str, int] = defaultdict(int)
    split_duration: defaultdict[str, int] = defaultdict(int)
    stratum_recordings: defaultdict[str, set[str]] = defaultdict(set)
    stratum_intervals: defaultdict[str, int] = defaultdict(int)
    stratum_duration: defaultdict[str, int] = defaultdict(int)
    stratum_flags: dict[str, dict[str, Any]] = {}
    total_intervals = 0
    total_duration = 0
    cohort_by_id = {row["candidate_id"]: row for row in cohort["candidates"]}
    for review_recording in review["recordings"]:
        proposal = proposal_by_id[review_recording["proposal_id"]]
        proposed = recording_by_id[(proposal["proposal_id"], review_recording["recording_id"])]
        candidate = cohort_by_id[review_recording["candidate_id"]]
        split = review_recording["split"]
        frozen_intervals: list[dict[str, Any]] = []
        proposed_by_id = {row["interval_id"]: row for row in proposed["intervals"]}
        for decision in review_recording["intervals"]:
            if decision["decision"] != "include":
                continue
            source = proposed_by_id[decision["proposal_interval_id"]]
            start = decision["accepted_start_ms"]
            end = decision["accepted_end_ms"]
            flags = decision["flags"]
            stratum = _stratum_id(flags)
            stratum_flags.setdefault(stratum, flags)
            frozen_intervals.append(
                {
                    "interval_id": _stable_id(
                        "interval",
                        review_sha,
                        proposal["proposal_id"],
                        source["interval_id"],
                        start,
                        end,
                    ),
                    "proposal_id": proposal["proposal_id"],
                    "proposal_interval_id": source["interval_id"],
                    "selection_decision_id": decision["selection_decision_id"],
                    "start_ms": start,
                    "end_ms": end,
                    "stratum_id": stratum,
                    "flags": flags,
                }
            )
            duration = end - start
            total_intervals += 1
            total_duration += duration
            split_intervals[split] += 1
            split_duration[split] += duration
            stratum_recordings[stratum].add(proposed["recording_id"])
            stratum_intervals[stratum] += 1
            stratum_duration[stratum] += duration
        split_recordings[split].add(proposed["recording_id"])
        if proposal["schema_version"] == 2:
            media_id = proposed["parent_media_id"]
            media_sha = proposed["parent_media_sha256"]
            media_bytes = proposed["parent_media_byte_count"]
            media_duration = proposed["parent_media_duration_ms"]
            rendition_id = proposed["parent_rendition_id"]
            rendition_kind = proposed["parent_rendition_kind"]
        else:
            media_id = proposed["parent_media_id"]
            media_sha = proposed["parent_media_sha256"]
            media_bytes = proposed["parent_media_byte_count"]
            media_duration = proposed["parent_media_duration_ms"]
            rendition_id = proposed["rendition_id"]
            rendition_kind = proposed["rendition_kind"]
        recordings.append(
            {
                "candidate_id": proposed["candidate_id"],
                "recording_id": proposed["recording_id"],
                "source_id": proposed["source_id"],
                "source_native_id": proposed["source_native_id"],
                "source_locator": candidate["public_locator"],
                "media_id": media_id,
                "media_sha256": media_sha,
                "media_byte_count": media_bytes,
                "media_duration_ms": media_duration,
                "rendition_id": rendition_id,
                "rendition_kind": rendition_kind,
                "timeline_coordinate_system": "rendition_media_ms",
                "split": split,
                "intervals": frozen_intervals,
            }
        )

    proposal_inputs = review["proposal_inputs"]
    freeze = {
        "schema_version": 2,
        "manifest_kind": "interval_freeze",
        "manifest_sha256": "0" * 64,
        "freeze_id": freeze_id,
        "cohort_id": cohort["cohort_id"],
        "cohort_manifest_sha256": cohort["manifest_sha256"],
        "created_at": frozen_at,
        "frozen_at": frozen_at,
        "selection_attestation": {
            "selected_without_asr_output_inspection": True,
            "attestor_id": review["reviewer"]["reviewer_id"],
            "attested_at": review["reviewer"]["attested_at"],
            "selection_basis": "source_metadata_and_direct_media_only",
            "protocol_revision": review["protocol"]["protocol_revision"],
        },
        "selection_provenance": {
            "review_id": review["review_id"],
            "review_manifest_sha256": review_sha,
            "proposal_inputs": proposal_inputs,
        },
        "split_policy": {
            "unit": "recording",
            "no_segment_leakage": True,
            "scoring_and_calibration_disjoint": True,
            "stratification_dimensions": [
                "language",
                "code_switch",
                "speaker_overlap",
                "playback_speech",
                "noise",
            ],
        },
        "reference_state": "interval_selection_only_no_reference_text",
        "recordings": recordings,
        "accounting": {
            "recording_count": len(recordings),
            "interval_count": total_intervals,
            "total_duration_ms": total_duration,
            "splits": [
                {
                    "split": split,
                    "recording_count": len(split_recordings[split]),
                    "interval_count": split_intervals[split],
                    "duration_ms": split_duration[split],
                }
                for split in SPLITS
            ],
            "strata": [
                {
                    "stratum_id": stratum,
                    "flags": stratum_flags[stratum],
                    "recording_count": len(stratum_recordings[stratum]),
                    "interval_count": stratum_intervals[stratum],
                    "duration_ms": stratum_duration[stratum],
                }
                for stratum in sorted(stratum_flags)
            ],
        },
    }
    freeze["manifest_sha256"] = canonical_manifest_sha256(freeze)
    validate_interval_freeze(freeze, cohort)
    return freeze


def validate_compiled_interval_freeze(
    value: object,
    review: object,
    candidate_cohort: object,
    requests: Sequence[object],
    proposals: Sequence[object],
    catalog_path: str | Path,
) -> dict[str, Any]:
    """Validate a v2 freeze by deterministic recompilation from private inputs."""

    freeze = validate_interval_freeze(value, candidate_cohort)
    if freeze["schema_version"] != 2:
        _fail("$.schema_version", "compiled selection validation requires version 2")
    expected = compile_interval_freeze(
        review,
        candidate_cohort,
        requests,
        proposals,
        catalog_path,
        freeze["frozen_at"],
    )
    if freeze != expected:
        _fail("$", "does not equal deterministic compilation of the bound review")
    return freeze
