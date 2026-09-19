"""Private, deterministic state for direct-parent interval-selection review.

This module is deliberately independent of the HTTP review surface.  It binds a
mutable draft to one exact incomplete selection-review template, applies a small
typed operation language with optimistic revisions, and materializes (but does not
publish or freeze) a completed private review.  Playback coverage is operational
state only and is never interpreted as a decision or human attestation.
"""

from __future__ import annotations

import copy
import hashlib
import json
import os
import secrets
import stat
from pathlib import Path
from typing import Any

from .selection_review import (
    MINIMUM_ACCEPTED_DURATION_MS,
    REJECTION_REASONS,
    TRIM_REASONS,
)
from .validation import (
    SPLITS,
    ContractError,
    _array,
    _canonical_bytes,
    _choice,
    _constant,
    _id,
    _integer,
    _object,
    _sha256,
    _stable_id,
    _timestamp,
    _validate_flags,
    _verify_manifest_digest,
    canonical_manifest_sha256,
)


DRAFT_SCHEMA_VERSION = 1
DRAFT_KIND = "interval_selection_draft"
DRAFT_LIFECYCLES = {"draft", "finalizing", "finalized", "invalidated"}
REVIEW_TOOL_NAME = "himr_selection_review_workspace"
REVIEW_TOOL_VERSION = "0.1.0"
MAX_DRAFT_BYTES = 4 * 1024 * 1024
MAX_COVERAGE_RANGES_PER_INTERVAL = 512
REQUIRED_COVERAGE_TOLERANCE_MS = 1_000


class IndeterminateDraftCommit(ContractError):
    """The draft replacement succeeded but its directory fsync did not."""


def _fail(path: str, message: str) -> None:
    raise ContractError(f"{path}: {message}")


def _null_flags() -> dict[str, None]:
    return {
        "language_tags": None,
        "code_switch": None,
        "speaker_overlap": None,
        "playback_speech": None,
        "noise": None,
    }


def _validate_null_flags(value: object, path: str) -> dict[str, Any]:
    flags = _object(
        value,
        path,
        {
            "language_tags",
            "code_switch",
            "speaker_overlap",
            "playback_speech",
            "noise",
        },
    )
    for key, item in flags.items():
        _constant(item, None, f"{path}.{key}")
    return flags


def _validate_finalization_intent(
    value: object,
    template: dict[str, Any],
    path: str = "$.finalization_intent",
) -> dict[str, Any]:
    intent = _object(
        value,
        path,
        {"reviewer_id", "reviewed_at", "attested_at", "begun_at"},
    )
    reviewer_id = _id(intent["reviewer_id"], f"{path}.reviewer_id")
    reviewed_at = _timestamp(intent["reviewed_at"], f"{path}.reviewed_at")
    attested_at = _timestamp(intent["attested_at"], f"{path}.attested_at")
    begun_at = _timestamp(intent["begun_at"], f"{path}.begun_at")
    if reviewer_id == template["protocol"]["protocol_revision"]:
        _fail(f"{path}.reviewer_id", "must identify a reviewer, not the protocol")
    if reviewed_at < _timestamp(template["created_at"], "$.template.created_at"):
        _fail(f"{path}.reviewed_at", "cannot precede template creation")
    if attested_at < reviewed_at:
        _fail(f"{path}.attested_at", "cannot precede review completion")
    if begun_at < attested_at:
        _fail(f"{path}.begun_at", "cannot precede the attestation")
    return intent


def _validate_proposal_inputs(value: object, path: str) -> list[dict[str, Any]]:
    inputs = _array(value, path)
    if len(inputs) != 2:
        _fail(path, "must contain exactly two proposal inputs")
    request_ids: set[str] = set()
    proposal_ids: set[str] = set()
    for index, raw in enumerate(inputs):
        item_path = f"{path}[{index}]"
        row = _object(
            raw,
            item_path,
            {
                "ordinal",
                "request_schema_version",
                "request_id",
                "request_manifest_sha256",
                "proposal_schema_version",
                "proposal_id",
                "proposal_manifest_sha256",
            },
        )
        _constant(row["ordinal"], index + 1, f"{item_path}.ordinal")
        request_version = _integer(
            row["request_schema_version"],
            f"{item_path}.request_schema_version",
            minimum=1,
        )
        proposal_version = _integer(
            row["proposal_schema_version"],
            f"{item_path}.proposal_schema_version",
            minimum=1,
        )
        if request_version not in {1, 2} or proposal_version != request_version:
            _fail(item_path, "request/proposal schema versions must match and be 1 or 2")
        request_id = _id(row["request_id"], f"{item_path}.request_id")
        proposal_id = _id(row["proposal_id"], f"{item_path}.proposal_id")
        _sha256(
            row["request_manifest_sha256"],
            f"{item_path}.request_manifest_sha256",
        )
        _sha256(
            row["proposal_manifest_sha256"],
            f"{item_path}.proposal_manifest_sha256",
        )
        if request_id in request_ids or proposal_id in proposal_ids:
            _fail(item_path, "request_id and proposal_id must be unique")
        request_ids.add(request_id)
        proposal_ids.add(proposal_id)
    return inputs


def _validate_incomplete_template(value: object) -> dict[str, Any]:
    """Validate the exact structural subset required to bind a workspace draft."""

    template = _object(
        value,
        "$.template",
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
    _constant(template["schema_version"], 1, "$.template.schema_version")
    _constant(
        template["manifest_kind"],
        "interval_selection_review",
        "$.template.manifest_kind",
    )
    _constant(
        template["review_state"],
        "incomplete_template",
        "$.template.review_state",
    )
    _id(template["review_id"], "$.template.review_id")
    _timestamp(template["created_at"], "$.template.created_at")
    _id(template["cohort_id"], "$.template.cohort_id")
    _sha256(
        template["cohort_manifest_sha256"],
        "$.template.cohort_manifest_sha256",
    )

    protocol = _object(
        template["protocol"],
        "$.template.protocol",
        {
            "protocol_revision",
            "minimum_accepted_duration_ms",
            "split_unit",
            "interval_adjustment",
        },
    )
    _id(protocol["protocol_revision"], "$.template.protocol.protocol_revision")
    _constant(
        protocol["minimum_accepted_duration_ms"],
        MINIMUM_ACCEPTED_DURATION_MS,
        "$.template.protocol.minimum_accepted_duration_ms",
    )
    _constant(protocol["split_unit"], "recording", "$.template.protocol.split_unit")
    _constant(
        protocol["interval_adjustment"],
        "trim_only_within_proposed_parent_range",
        "$.template.protocol.interval_adjustment",
    )
    _validate_proposal_inputs(template["proposal_inputs"], "$.template.proposal_inputs")

    reviewer = _object(
        template["reviewer"],
        "$.template.reviewer",
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
    for key, item in reviewer.items():
        _constant(item, None, f"$.template.reviewer.{key}")

    privacy = _object(
        template["privacy"],
        "$.template.privacy",
        {"storage_policy", "publication_authority", "reference_text_present"},
    )
    _constant(
        privacy["storage_policy"], "private_only", "$.template.privacy.storage_policy"
    )
    _constant(
        privacy["publication_authority"],
        "none",
        "$.template.privacy.publication_authority",
    )
    _constant(
        privacy["reference_text_present"],
        False,
        "$.template.privacy.reference_text_present",
    )

    recordings = _array(template["recordings"], "$.template.recordings")
    if len(recordings) != 12:
        _fail("$.template.recordings", "must contain exactly 12 recordings")
    candidate_ids: set[str] = set()
    recording_ids: set[str] = set()
    source_ids: set[str] = set()
    decision_ids: set[str] = set()
    proposal_interval_ids: set[str] = set()
    interval_count = 0
    total_duration_ms = 0
    for recording_index, raw in enumerate(recordings):
        recording_path = f"$.template.recordings[{recording_index}]"
        recording = _object(
            raw,
            recording_path,
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
        for field, seen in (
            ("candidate_id", candidate_ids),
            ("recording_id", recording_ids),
            ("source_id", source_ids),
        ):
            item = _id(recording[field], f"{recording_path}.{field}")
            if item in seen:
                _fail(f"{recording_path}.{field}", "must be globally unique")
            seen.add(item)
        _id(recording["proposal_id"], f"{recording_path}.proposal_id")
        version = _integer(
            recording["proposal_schema_version"],
            f"{recording_path}.proposal_schema_version",
            minimum=1,
        )
        if version not in {1, 2}:
            _fail(f"{recording_path}.proposal_schema_version", "must be 1 or 2")
        _constant(recording["split"], None, f"{recording_path}.split")
        intervals = _array(recording["intervals"], f"{recording_path}.intervals")
        if not intervals:
            _fail(f"{recording_path}.intervals", "must not be empty")
        previous_end = -1
        for decision_index, raw_decision in enumerate(intervals):
            decision_path = f"{recording_path}.intervals[{decision_index}]"
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
            selection_id = _id(
                decision["selection_decision_id"],
                f"{decision_path}.selection_decision_id",
            )
            proposal_interval_id = _id(
                decision["proposal_interval_id"],
                f"{decision_path}.proposal_interval_id",
            )
            if selection_id in decision_ids or proposal_interval_id in proposal_interval_ids:
                _fail(decision_path, "decision and proposal interval IDs must be globally unique")
            decision_ids.add(selection_id)
            proposal_interval_ids.add(proposal_interval_id)
            start = _integer(
                decision["proposal_start_ms"], f"{decision_path}.proposal_start_ms"
            )
            end = _integer(
                decision["proposal_end_ms"],
                f"{decision_path}.proposal_end_ms",
                minimum=1,
            )
            if end <= start:
                _fail(decision_path, "proposal interval must be nonempty")
            if start < previous_end:
                _fail(decision_path, "proposal intervals must be sorted and nonoverlapping")
            previous_end = end
            for field in (
                "decision",
                "accepted_start_ms",
                "accepted_end_ms",
                "adjustment_reason",
                "rejection_reason",
            ):
                _constant(decision[field], None, f"{decision_path}.{field}")
            _validate_null_flags(decision["flags"], f"{decision_path}.flags")
            interval_count += 1
            total_duration_ms += end - start

    accounting = _object(
        template["proposal_accounting"],
        "$.template.proposal_accounting",
        {"recording_count", "interval_count", "total_duration_ms"},
    )
    _constant(
        accounting["recording_count"],
        len(recordings),
        "$.template.proposal_accounting.recording_count",
    )
    _constant(
        accounting["interval_count"],
        interval_count,
        "$.template.proposal_accounting.interval_count",
    )
    _constant(
        accounting["total_duration_ms"],
        total_duration_ms,
        "$.template.proposal_accounting.total_duration_ms",
    )
    _verify_manifest_digest(template, "$.template")
    return template


def _expected_draft_id(
    template: dict[str, Any], workspace_id: str, workspace_manifest_sha256: str
) -> str:
    return _stable_id(
        "selection_draft",
        workspace_id,
        workspace_manifest_sha256,
        template["review_id"],
        template["manifest_sha256"],
    )


def _default_workspace_id(template: dict[str, Any]) -> str:
    return _stable_id(
        "selection_workspace",
        template["review_id"],
        template["manifest_sha256"],
        "selection_draft_v1",
    )


def _template_binding(template: dict[str, Any]) -> dict[str, Any]:
    return {
        "review_id": template["review_id"],
        "template_manifest_sha256": template["manifest_sha256"],
        "cohort_id": template["cohort_id"],
        "cohort_manifest_sha256": template["cohort_manifest_sha256"],
        "proposal_inputs_sha256": hashlib.sha256(
            _canonical_bytes(template["proposal_inputs"])
        ).hexdigest(),
    }


def _empty_draft_decision(template_decision: dict[str, Any]) -> dict[str, Any]:
    return {
        "selection_decision_id": template_decision["selection_decision_id"],
        "proposal_interval_id": template_decision["proposal_interval_id"],
        "proposal_start_ms": template_decision["proposal_start_ms"],
        "proposal_end_ms": template_decision["proposal_end_ms"],
        "decision": None,
        "accepted_start_ms": None,
        "accepted_end_ms": None,
        "adjustment_reason": None,
        "rejection_reason": None,
        "flags": _null_flags(),
        "coverage_ranges": [],
    }


def create_selection_draft(
    template_value: object,
    workspace_id: str,
    workspace_manifest_sha256: str,
    now: str,
) -> dict[str, Any]:
    """Create a revision-zero draft bound to an exact workspace manifest."""

    template = _validate_incomplete_template(template_value)
    workspace = _id(workspace_id, "$.workspace_id")
    workspace_digest = _sha256(
        workspace_manifest_sha256, "$.workspace_manifest_sha256"
    )
    created = _timestamp(now, "$.now")
    if created < _timestamp(template["created_at"], "$.template.created_at"):
        _fail("$.now", "cannot precede template creation")
    draft_id = _expected_draft_id(template, workspace, workspace_digest)
    draft = {
        "schema_version": DRAFT_SCHEMA_VERSION,
        "manifest_kind": DRAFT_KIND,
        "manifest_sha256": "0" * 64,
        "draft_id": draft_id,
        "workspace_id": workspace,
        "workspace_manifest_sha256": workspace_digest,
        "template_binding": _template_binding(template),
        "revision": 0,
        "lifecycle": "draft",
        "finalization_intent": None,
        "finalization_manifest_sha256": None,
        "created_at": now,
        "updated_at": now,
        "recordings": [
            {
                "candidate_id": recording["candidate_id"],
                "recording_id": recording["recording_id"],
                "source_id": recording["source_id"],
                "proposal_id": recording["proposal_id"],
                "proposal_schema_version": recording["proposal_schema_version"],
                "split": None,
                "intervals": [
                    _empty_draft_decision(decision)
                    for decision in recording["intervals"]
                ],
            }
            for recording in template["recordings"]
        ],
    }
    draft["manifest_sha256"] = canonical_manifest_sha256(draft)
    return validate_selection_draft(
        draft, template, workspace, workspace_digest
    )


def create_draft(template_value: object) -> dict[str, Any]:
    """Create a deterministic draft with a template-derived local workspace binding.

    New server code should call :func:`create_selection_draft` with the exact
    independently verified workspace manifest identity.
    """

    template = _validate_incomplete_template(template_value)
    return create_selection_draft(
        template,
        _default_workspace_id(template),
        template["manifest_sha256"],
        template["created_at"],
    )


def _validate_coverage(
    value: object,
    path: str,
    *,
    proposal_start_ms: int,
    proposal_end_ms: int,
) -> list[dict[str, int]]:
    rows = _array(value, path)
    if len(rows) > MAX_COVERAGE_RANGES_PER_INTERVAL:
        _fail(
            path,
            f"must contain at most {MAX_COVERAGE_RANGES_PER_INTERVAL} ranges",
        )
    previous_end: int | None = None
    for index, raw in enumerate(rows):
        item_path = f"{path}[{index}]"
        row = _object(raw, item_path, {"start_ms", "end_ms"})
        start = _integer(row["start_ms"], f"{item_path}.start_ms")
        end = _integer(row["end_ms"], f"{item_path}.end_ms", minimum=1)
        if end <= start:
            _fail(item_path, "coverage range must be nonempty")
        if start < proposal_start_ms or end > proposal_end_ms:
            _fail(item_path, "coverage range must stay within the proposal interval")
        if previous_end is not None and start <= previous_end:
            _fail(path, "coverage ranges must be sorted, disjoint, and adjacency-merged")
        previous_end = end
    return rows


def _validate_draft_decision(
    value: object,
    path: str,
    template_decision: dict[str, Any],
) -> dict[str, Any]:
    decision = _object(
        value,
        path,
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
            "coverage_ranges",
        },
    )
    for field in (
        "selection_decision_id",
        "proposal_interval_id",
        "proposal_start_ms",
        "proposal_end_ms",
    ):
        if decision[field] != template_decision[field]:
            _fail(f"{path}.{field}", "does not match the immutable template")
    proposal_start = template_decision["proposal_start_ms"]
    proposal_end = template_decision["proposal_end_ms"]
    _validate_coverage(
        decision["coverage_ranges"],
        f"{path}.coverage_ranges",
        proposal_start_ms=proposal_start,
        proposal_end_ms=proposal_end,
    )

    selected = decision["decision"]
    if selected is None:
        for field in (
            "accepted_start_ms",
            "accepted_end_ms",
            "adjustment_reason",
            "rejection_reason",
        ):
            _constant(decision[field], None, f"{path}.{field}")
        _validate_null_flags(decision["flags"], f"{path}.flags")
    elif selected == "include":
        start = _integer(decision["accepted_start_ms"], f"{path}.accepted_start_ms")
        end = _integer(
            decision["accepted_end_ms"], f"{path}.accepted_end_ms", minimum=1
        )
        if start < proposal_start or end > proposal_end or end <= start:
            _fail(path, "included bounds must be a nonempty trim within the proposal")
        trimmed = start != proposal_start or end != proposal_end
        if trimmed:
            _choice(
                decision["adjustment_reason"],
                TRIM_REASONS,
                f"{path}.adjustment_reason",
            )
        else:
            _constant(decision["adjustment_reason"], None, f"{path}.adjustment_reason")
        _constant(decision["rejection_reason"], None, f"{path}.rejection_reason")
        _validate_flags(decision["flags"], f"{path}.flags")
    elif selected == "exclude":
        for field in ("accepted_start_ms", "accepted_end_ms", "adjustment_reason"):
            _constant(decision[field], None, f"{path}.{field}")
        _choice(
            decision["rejection_reason"],
            REJECTION_REASONS,
            f"{path}.rejection_reason",
        )
        _validate_null_flags(decision["flags"], f"{path}.flags")
    else:
        _fail(f"{path}.decision", "must be null, include, or exclude")
    return decision


def validate_selection_draft(
    value: object,
    template_value: object,
    workspace_id: str,
    workspace_manifest_sha256: str,
) -> dict[str, Any]:
    """Validate a draft against one exact immutable incomplete template."""

    template = _validate_incomplete_template(template_value)
    expected_workspace = _id(workspace_id, "$.expected_workspace_id")
    expected_workspace_digest = _sha256(
        workspace_manifest_sha256, "$.expected_workspace_manifest_sha256"
    )
    draft = _object(
        value,
        "$",
        {
            "schema_version",
            "manifest_kind",
            "manifest_sha256",
            "draft_id",
            "workspace_id",
            "workspace_manifest_sha256",
            "template_binding",
            "revision",
            "lifecycle",
            "finalization_intent",
            "finalization_manifest_sha256",
            "created_at",
            "updated_at",
            "recordings",
        },
    )
    _constant(draft["schema_version"], DRAFT_SCHEMA_VERSION, "$.schema_version")
    _constant(draft["manifest_kind"], DRAFT_KIND, "$.manifest_kind")
    _constant(draft["workspace_id"], expected_workspace, "$.workspace_id")
    _constant(
        draft["workspace_manifest_sha256"],
        expected_workspace_digest,
        "$.workspace_manifest_sha256",
    )
    expected_draft_id = _expected_draft_id(
        template, expected_workspace, expected_workspace_digest
    )
    _constant(draft["draft_id"], expected_draft_id, "$.draft_id")
    binding = _object(
        draft["template_binding"],
        "$.template_binding",
        {
            "review_id",
            "template_manifest_sha256",
            "cohort_id",
            "cohort_manifest_sha256",
            "proposal_inputs_sha256",
        },
    )
    if binding != _template_binding(template):
        _fail("$.template_binding", "does not bind the exact incomplete template")
    _integer(draft["revision"], "$.revision")
    created_at = _timestamp(draft["created_at"], "$.created_at")
    updated_at = _timestamp(draft["updated_at"], "$.updated_at")
    if created_at < _timestamp(template["created_at"], "$.template.created_at"):
        _fail("$.created_at", "cannot precede template creation")
    if updated_at < created_at:
        _fail("$.updated_at", "cannot precede draft creation")
    lifecycle = _choice(draft["lifecycle"], DRAFT_LIFECYCLES, "$.lifecycle")
    intent = draft["finalization_intent"]
    if lifecycle in {"finalizing", "finalized"}:
        intent_row = _validate_finalization_intent(intent, template)
    elif lifecycle == "draft":
        _constant(intent, None, "$.finalization_intent")
    elif intent is not None:
        # An invalidated in-flight finalization retains its exact durable intent.
        intent_row = _validate_finalization_intent(intent, template)
    if intent is not None:
        begun_at = _timestamp(intent_row["begun_at"], "$.finalization_intent.begun_at")
        if updated_at < begun_at:
            _fail("$.updated_at", "cannot precede finalization intent")
    if lifecycle == "finalized":
        _sha256(
            draft["finalization_manifest_sha256"],
            "$.finalization_manifest_sha256",
        )
    else:
        _constant(
            draft["finalization_manifest_sha256"],
            None,
            "$.finalization_manifest_sha256",
        )

    recordings = _array(draft["recordings"], "$.recordings")
    if len(recordings) != len(template["recordings"]):
        _fail("$.recordings", "must cover the exact template recording set")
    for recording_index, (raw, template_recording) in enumerate(
        zip(recordings, template["recordings"])
    ):
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
        for field in (
            "candidate_id",
            "recording_id",
            "source_id",
            "proposal_id",
            "proposal_schema_version",
        ):
            if recording[field] != template_recording[field]:
                _fail(f"{path}.{field}", "does not match immutable template order/lineage")
        if recording["split"] is not None:
            _choice(recording["split"], set(SPLITS), f"{path}.split")
        decisions = _array(recording["intervals"], f"{path}.intervals")
        if len(decisions) != len(template_recording["intervals"]):
            _fail(f"{path}.intervals", "must cover the exact template decision set")
        for decision_index, (decision, template_decision) in enumerate(
            zip(decisions, template_recording["intervals"])
        ):
            _validate_draft_decision(
                decision,
                f"{path}.intervals[{decision_index}]",
                template_decision,
            )
    _verify_manifest_digest(draft)
    return draft


def validate_draft(value: object, template_value: object) -> dict[str, Any]:
    """Validate using the workspace binding declared by the draft itself."""

    if not isinstance(value, dict):
        _fail("$", "must be an object")
    return validate_selection_draft(
        value,
        template_value,
        value.get("workspace_id"),
        value.get("workspace_manifest_sha256"),
    )


def _recording_by_id(draft: dict[str, Any], recording_id: str) -> dict[str, Any]:
    matches = [row for row in draft["recordings"] if row["recording_id"] == recording_id]
    if len(matches) != 1:
        _fail("$.operation.recording_id", "does not identify one draft recording")
    return matches[0]


def _decision_by_id(draft: dict[str, Any], selection_id: str) -> dict[str, Any]:
    matches = [
        decision
        for recording in draft["recordings"]
        for decision in recording["intervals"]
        if decision["selection_decision_id"] == selection_id
    ]
    if len(matches) != 1:
        _fail(
            "$.operation.selection_decision_id",
            "does not identify one draft decision",
        )
    return matches[0]


def _operation_header(
    operation: object, keys: set[str], operation_name: str
) -> dict[str, Any]:
    row = _object(operation, "$.operation", keys | {"operation", "expected_revision"})
    _constant(row["operation"], operation_name, "$.operation.operation")
    _integer(row["expected_revision"], "$.operation.expected_revision")
    return row


def _merge_ranges(
    rows: list[dict[str, int]], start: int, end: int
) -> list[dict[str, int]]:
    pairs = sorted(
        [(row["start_ms"], row["end_ms"]) for row in rows] + [(start, end)]
    )
    merged: list[list[int]] = []
    for left, right in pairs:
        if not merged or left > merged[-1][1]:
            merged.append([left, right])
        else:
            merged[-1][1] = max(merged[-1][1], right)
    return [{"start_ms": left, "end_ms": right} for left, right in merged]


def apply_operation(
    draft_value: object, template_value: object, operation: object
) -> dict[str, Any]:
    """Apply one exact typed mutation and return a newly sealed draft revision."""

    template = _validate_incomplete_template(template_value)
    current = validate_draft(draft_value, template)
    if current["lifecycle"] != "draft":
        _fail("$.lifecycle", "only a draft lifecycle accepts mutations")
    if not isinstance(operation, dict):
        _fail("$.operation", "must be an object")
    name = operation.get("operation")
    if not isinstance(name, str):
        _fail("$.operation.operation", "must be a string")

    updated = copy.deepcopy(current)
    if name == "set_split":
        row = _operation_header(operation, {"recording_id", "split"}, name)
        recording_id = _id(row["recording_id"], "$.operation.recording_id")
        split = _choice(row["split"], set(SPLITS), "$.operation.split")
        _recording_by_id(updated, recording_id)["split"] = split
    elif name == "clear_split":
        row = _operation_header(operation, {"recording_id"}, name)
        recording_id = _id(row["recording_id"], "$.operation.recording_id")
        _recording_by_id(updated, recording_id)["split"] = None
    elif name == "set_include":
        row = _operation_header(
            operation,
            {
                "selection_decision_id",
                "accepted_start_ms",
                "accepted_end_ms",
                "adjustment_reason",
                "flags",
            },
            name,
        )
        selection_id = _id(
            row["selection_decision_id"], "$.operation.selection_decision_id"
        )
        decision = _decision_by_id(updated, selection_id)
        decision.update(
            decision="include",
            accepted_start_ms=row["accepted_start_ms"],
            accepted_end_ms=row["accepted_end_ms"],
            adjustment_reason=row["adjustment_reason"],
            rejection_reason=None,
            flags=copy.deepcopy(row["flags"]),
        )
    elif name == "set_exclude":
        row = _operation_header(
            operation,
            {"selection_decision_id", "rejection_reason"},
            name,
        )
        selection_id = _id(
            row["selection_decision_id"], "$.operation.selection_decision_id"
        )
        decision = _decision_by_id(updated, selection_id)
        decision.update(
            decision="exclude",
            accepted_start_ms=None,
            accepted_end_ms=None,
            adjustment_reason=None,
            rejection_reason=row["rejection_reason"],
            flags=_null_flags(),
        )
    elif name == "clear_decision":
        row = _operation_header(operation, {"selection_decision_id"}, name)
        selection_id = _id(
            row["selection_decision_id"], "$.operation.selection_decision_id"
        )
        decision = _decision_by_id(updated, selection_id)
        decision.update(
            decision=None,
            accepted_start_ms=None,
            accepted_end_ms=None,
            adjustment_reason=None,
            rejection_reason=None,
            flags=_null_flags(),
        )
    elif name == "merge_coverage":
        row = _operation_header(
            operation,
            {"selection_decision_id", "start_ms", "end_ms"},
            name,
        )
        selection_id = _id(
            row["selection_decision_id"], "$.operation.selection_decision_id"
        )
        start = _integer(row["start_ms"], "$.operation.start_ms")
        end = _integer(row["end_ms"], "$.operation.end_ms", minimum=1)
        decision = _decision_by_id(updated, selection_id)
        if (
            end <= start
            or start < decision["proposal_start_ms"]
            or end > decision["proposal_end_ms"]
        ):
            _fail(
                "$.operation",
                "coverage must be a nonempty range within the proposal interval",
            )
        decision["coverage_ranges"] = _merge_ranges(
            decision["coverage_ranges"], start, end
        )
    else:
        _fail(
            "$.operation.operation",
            "must be set_split, clear_split, set_include, set_exclude, "
            "clear_decision, or merge_coverage",
        )

    if operation["expected_revision"] != current["revision"]:
        _fail(
            "$.operation.expected_revision",
            f"stale revision; expected {current['revision']}",
        )
    updated["revision"] += 1
    updated["manifest_sha256"] = canonical_manifest_sha256(updated)
    return validate_draft(updated, template)


def apply_draft_operation(
    draft_value: object,
    template_value: object,
    expected_revision: int,
    operation: object,
    now: str,
) -> dict[str, Any]:
    """Server-facing operation API with revision and clock supplied out of band."""

    draft = validate_draft(draft_value, template_value)
    _integer(expected_revision, "$.expected_revision")
    updated_time = _timestamp(now, "$.now")
    if updated_time < _timestamp(draft["updated_at"], "$.updated_at"):
        _fail("$.now", "cannot precede the prior draft update")
    if not isinstance(operation, dict):
        _fail("$.operation", "must be an object")
    if "expected_revision" in operation:
        _fail(
            "$.operation.expected_revision",
            "is supplied separately and must not appear in the operation",
        )
    internal_operation = dict(operation)
    internal_operation["expected_revision"] = expected_revision
    updated = apply_operation(draft, template_value, internal_operation)
    updated["updated_at"] = now
    updated["manifest_sha256"] = canonical_manifest_sha256(updated)
    return validate_draft(updated, template_value)


def draft_readiness(
    draft_value: object, template_value: object
) -> dict[str, Any]:
    """Return deterministic completion/accounting facts without attesting anything."""

    draft = validate_draft(draft_value, template_value)
    missing_split_recording_ids: list[str] = []
    pending_selection_decision_ids: list[str] = []
    missing_coverage_selection_decision_ids: list[str] = []
    accepted_interval_count = 0
    excluded_interval_count = 0
    accepted_duration_ms = 0
    coverage_duration_ms = 0
    proposal_duration_ms = 0
    pending_full_duration_ms = 0
    splits = {split: {"recording_count": 0, "accepted_duration_ms": 0} for split in SPLITS}
    for recording in draft["recordings"]:
        split = recording["split"]
        if split is None:
            missing_split_recording_ids.append(recording["recording_id"])
        else:
            splits[split]["recording_count"] += 1
        recording_accepted_duration = 0
        for decision in recording["intervals"]:
            proposal_duration = (
                decision["proposal_end_ms"] - decision["proposal_start_ms"]
            )
            proposal_duration_ms += proposal_duration
            decision_coverage = sum(
                row["end_ms"] - row["start_ms"]
                for row in decision["coverage_ranges"]
            )
            coverage_duration_ms += decision_coverage
            required_coverage = max(
                1, proposal_duration - REQUIRED_COVERAGE_TOLERANCE_MS
            )
            if decision_coverage < required_coverage:
                missing_coverage_selection_decision_ids.append(
                    decision["selection_decision_id"]
                )
            if decision["decision"] is None:
                pending_selection_decision_ids.append(
                    decision["selection_decision_id"]
                )
                pending_full_duration_ms += proposal_duration
            elif decision["decision"] == "include":
                accepted_interval_count += 1
                duration = decision["accepted_end_ms"] - decision["accepted_start_ms"]
                accepted_duration_ms += duration
                recording_accepted_duration += duration
            else:
                excluded_interval_count += 1
        if split is not None:
            splits[split]["accepted_duration_ms"] += recording_accepted_duration
    duration_shortfall_ms = max(
        0, MINIMUM_ACCEPTED_DURATION_MS - accepted_duration_ms
    )
    every_recording_has_acceptance = all(
        any(decision["decision"] == "include" for decision in recording["intervals"])
        for recording in draft["recordings"]
    )
    both_splits_present = all(splits[split]["recording_count"] > 0 for split in SPLITS)
    complete = (
        not missing_split_recording_ids
        and not pending_selection_decision_ids
        and not missing_coverage_selection_decision_ids
        and accepted_duration_ms >= MINIMUM_ACCEPTED_DURATION_MS
        and every_recording_has_acceptance
        and both_splits_present
    )
    ready_to_begin = draft["lifecycle"] == "draft" and complete
    ready = draft["lifecycle"] == "finalizing" and complete
    maximum_possible_duration_ms = accepted_duration_ms + pending_full_duration_ms
    removed_duration_ms = (
        proposal_duration_ms - accepted_duration_ms - pending_full_duration_ms
    )
    remaining_removal_budget_ms = max(
        0,
        proposal_duration_ms
        - MINIMUM_ACCEPTED_DURATION_MS
        - removed_duration_ms,
    )
    return {
        "ready_to_begin_finalization": ready_to_begin,
        "ready_to_materialize": ready,
        "missing_split_recording_ids": missing_split_recording_ids,
        "pending_selection_decision_ids": pending_selection_decision_ids,
        "missing_coverage_selection_decision_ids": (
            missing_coverage_selection_decision_ids
        ),
        "accepted_interval_count": accepted_interval_count,
        "excluded_interval_count": excluded_interval_count,
        "accepted_duration_ms": accepted_duration_ms,
        "minimum_accepted_duration_ms": MINIMUM_ACCEPTED_DURATION_MS,
        "duration_shortfall_ms": duration_shortfall_ms,
        "proposal_duration_ms": proposal_duration_ms,
        "maximum_possible_duration_ms": maximum_possible_duration_ms,
        "completion_impossible": (
            maximum_possible_duration_ms < MINIMUM_ACCEPTED_DURATION_MS
        ),
        "removed_duration_ms": removed_duration_ms,
        "remaining_removal_budget_ms": remaining_removal_budget_ms,
        "coverage_duration_ms": coverage_duration_ms,
        "required_coverage_tolerance_ms": REQUIRED_COVERAGE_TOLERANCE_MS,
        "every_recording_has_acceptance": every_recording_has_acceptance,
        "both_splits_present": both_splits_present,
        "splits": [
            {"split": split, **splits[split]}
            for split in SPLITS
        ],
    }


def begin_draft_finalization(
    draft_value: object,
    template_value: object,
    expected_revision: int,
    reviewer_id: str,
    reviewed_at: str,
    attested_at: str,
    now: str,
) -> dict[str, Any]:
    """Durably express reviewer intent before media/catalog verification and output."""

    template = _validate_incomplete_template(template_value)
    draft = validate_draft(draft_value, template)
    if draft["lifecycle"] != "draft":
        _fail("$.lifecycle", "only a draft can begin finalization")
    _integer(expected_revision, "$.expected_revision")
    if expected_revision != draft["revision"]:
        _fail("$.expected_revision", f"stale revision; expected {draft['revision']}")
    readiness = draft_readiness(draft, template)
    if not readiness["ready_to_begin_finalization"]:
        _fail("$", "draft is not ready to begin finalization")
    reviewer = _id(reviewer_id, "$.reviewer_id")
    reviewed_time = _timestamp(reviewed_at, "$.reviewed_at")
    attested_time = _timestamp(attested_at, "$.attested_at")
    begun_time = _timestamp(now, "$.now")
    if reviewer == template["protocol"]["protocol_revision"]:
        _fail("$.reviewer_id", "must identify a reviewer, not the protocol")
    if reviewed_time < _timestamp(template["created_at"], "$.template.created_at"):
        _fail("$.reviewed_at", "cannot precede template creation")
    if attested_time < reviewed_time:
        _fail("$.attested_at", "cannot precede review completion")
    if begun_time < attested_time:
        _fail("$.now", "cannot precede the attestation")
    if begun_time < _timestamp(draft["updated_at"], "$.updated_at"):
        _fail("$.now", "cannot precede the prior draft update")
    updated = copy.deepcopy(draft)
    updated["revision"] += 1
    updated["lifecycle"] = "finalizing"
    updated["finalization_intent"] = {
        "reviewer_id": reviewer,
        "reviewed_at": reviewed_at,
        "attested_at": attested_at,
        "begun_at": now,
    }
    updated["updated_at"] = now
    updated["manifest_sha256"] = canonical_manifest_sha256(updated)
    return validate_draft(updated, template)


def materialize_completed_review(
    template_value: object,
    draft_value: object,
    reviewer_id: str,
    reviewed_at: str,
    attested_at: str,
) -> dict[str, Any]:
    """Materialize a completed private review after an explicit attestation action.

    Calling this function is the explicit finalization boundary.  Coverage is not
    inspected and can never infer direct-media review or blinding attestations.
    """

    template = _validate_incomplete_template(template_value)
    draft = validate_draft(draft_value, template)
    if draft["lifecycle"] != "finalizing":
        _fail("$.lifecycle", "only a persisted finalizing draft can materialize a review")
    reviewer = _id(reviewer_id, "$.reviewer_id")
    reviewed_time = _timestamp(reviewed_at, "$.reviewed_at")
    attested_time = _timestamp(attested_at, "$.attested_at")
    created_time = _timestamp(template["created_at"], "$.template.created_at")
    if reviewed_time < created_time:
        _fail("$.reviewed_at", "cannot precede template creation")
    if attested_time < reviewed_time:
        _fail("$.attested_at", "cannot precede review completion")
    if reviewer == template["protocol"]["protocol_revision"]:
        _fail("$.reviewer_id", "must identify a reviewer, not the protocol")
    intent = draft["finalization_intent"]
    if (
        reviewer_id != intent["reviewer_id"]
        or reviewed_at != intent["reviewed_at"]
        or attested_at != intent["attested_at"]
    ):
        _fail(
            "$.finalization_intent",
            "materialization arguments must equal the persisted reviewer intent",
        )

    completed = copy.deepcopy(template)
    completed["review_state"] = "completed_private"
    completed["reviewer"] = {
        "reviewer_id": reviewer,
        "review_tool": {"name": REVIEW_TOOL_NAME, "version": REVIEW_TOOL_VERSION},
        "reviewed_at": reviewed_at,
        "attested_at": attested_at,
        "direct_parent_media_reviewed": True,
        "asr_outputs_inspected": False,
        "reference_text_inspected": False,
        "selection_basis": "source_metadata_and_direct_parent_media_only",
    }
    for recording_index, (target_recording, draft_recording) in enumerate(
        zip(completed["recordings"], draft["recordings"])
    ):
        if draft_recording["split"] is None:
            _fail(
                f"$.recordings[{recording_index}].split",
                "must be decided before materialization",
            )
        target_recording["split"] = draft_recording["split"]
        for decision_index, (target, source) in enumerate(
            zip(target_recording["intervals"], draft_recording["intervals"])
        ):
            if source["decision"] is None:
                _fail(
                    f"$.recordings[{recording_index}].intervals[{decision_index}].decision",
                    "must be decided before materialization",
                )
            for field in (
                "decision",
                "accepted_start_ms",
                "accepted_end_ms",
                "adjustment_reason",
                "rejection_reason",
                "flags",
            ):
                target[field] = copy.deepcopy(source[field])
    completed["manifest_sha256"] = canonical_manifest_sha256(completed)
    return completed


def mark_finalized(
    draft_value: object,
    template_value: object,
    completed_review_manifest_sha256: str,
    *,
    expected_revision: int,
) -> dict[str, Any]:
    """Return a final immutable state marker after the review file is durable."""

    template = _validate_incomplete_template(template_value)
    draft = validate_draft(draft_value, template)
    if draft["lifecycle"] != "finalizing":
        _fail("$.lifecycle", "only a finalizing draft can be marked finalized")
    _integer(expected_revision, "$.expected_revision")
    if expected_revision != draft["revision"]:
        _fail("$.expected_revision", f"stale revision; expected {draft['revision']}")
    digest = _sha256(
        completed_review_manifest_sha256, "$.completed_review_manifest_sha256"
    )
    updated = copy.deepcopy(draft)
    updated["revision"] += 1
    updated["lifecycle"] = "finalized"
    updated["finalization_manifest_sha256"] = digest
    updated["manifest_sha256"] = canonical_manifest_sha256(updated)
    return validate_draft(updated, template)


def mark_draft_finalized(
    draft_value: object,
    template_value: object,
    expected_revision: int,
    completed_manifest_sha256: str,
    now: str,
) -> dict[str, Any]:
    """Server-facing durable-finalization marker with an explicit update time."""

    draft = validate_draft(draft_value, template_value)
    updated_time = _timestamp(now, "$.now")
    if updated_time < _timestamp(draft["updated_at"], "$.updated_at"):
        _fail("$.now", "cannot precede the prior draft update")
    updated = mark_finalized(
        draft,
        template_value,
        completed_manifest_sha256,
        expected_revision=expected_revision,
    )
    updated["updated_at"] = now
    updated["manifest_sha256"] = canonical_manifest_sha256(updated)
    return validate_draft(updated, template_value)


def mark_invalidated(
    draft_value: object,
    template_value: object,
    *,
    expected_revision: int,
) -> dict[str, Any]:
    """Return a fail-closed invalidated state while preserving review work."""

    template = _validate_incomplete_template(template_value)
    draft = validate_draft(draft_value, template)
    if draft["lifecycle"] not in {"draft", "finalizing"}:
        _fail("$.lifecycle", "only a draft or finalizing state can be invalidated")
    _integer(expected_revision, "$.expected_revision")
    if expected_revision != draft["revision"]:
        _fail("$.expected_revision", f"stale revision; expected {draft['revision']}")
    updated = copy.deepcopy(draft)
    updated["revision"] += 1
    updated["lifecycle"] = "invalidated"
    updated["manifest_sha256"] = canonical_manifest_sha256(updated)
    return validate_draft(updated, template)


def mark_draft_invalidated(
    draft_value: object,
    template_value: object,
    expected_revision: int,
    now: str,
) -> dict[str, Any]:
    """Server-facing fail-closed marker with an explicit update time."""

    draft = validate_draft(draft_value, template_value)
    updated_time = _timestamp(now, "$.now")
    if updated_time < _timestamp(draft["updated_at"], "$.updated_at"):
        _fail("$.now", "cannot precede the prior draft update")
    updated = mark_invalidated(
        draft,
        template_value,
        expected_revision=expected_revision,
    )
    updated["updated_at"] = now
    updated["manifest_sha256"] = canonical_manifest_sha256(updated)
    return validate_draft(updated, template_value)


def _strict_parent(path: Path) -> None:
    parent = path.parent
    parent.mkdir(parents=True, exist_ok=True, mode=0o700)
    metadata = parent.lstat()
    if stat.S_ISLNK(metadata.st_mode) or not stat.S_ISDIR(metadata.st_mode):
        _fail("$.path", "draft parent must be a non-symlink directory")
    if metadata.st_uid != os.geteuid() or stat.S_IMODE(metadata.st_mode) != 0o700:
        _fail("$.path", "draft parent must be current-user-owned with mode 0700")


def _persist_validated_draft(path_value: str | Path, draft: dict[str, Any]) -> None:
    path = Path(path_value)
    if not path.is_absolute():
        path = path.resolve()
    _strict_parent(path)
    try:
        existing = path.lstat()
    except FileNotFoundError:
        existing = None
    except OSError as error:
        raise ContractError(f"$.path: cannot inspect draft path: {error}") from error
    if existing is not None:
        if stat.S_ISLNK(existing.st_mode) or not stat.S_ISREG(existing.st_mode):
            _fail("$.path", "existing draft must be a non-symlink regular file")
        if (
            existing.st_uid != os.geteuid()
            or existing.st_nlink != 1
            or stat.S_IMODE(existing.st_mode) != 0o600
        ):
            _fail(
                "$.path",
                "existing draft must be current-user-owned, single-link, and mode 0600",
            )

    body = (
        json.dumps(draft, ensure_ascii=False, sort_keys=True, indent=2, allow_nan=False)
        + "\n"
    ).encode("utf-8")
    if len(body) > MAX_DRAFT_BYTES:
        _fail("$.path", f"serialized draft exceeds {MAX_DRAFT_BYTES} bytes")
    temporary = path.with_name(f".{path.name}.tmp-{os.getpid()}-{secrets.token_hex(8)}")
    flags = (
        os.O_WRONLY
        | os.O_CREAT
        | os.O_EXCL
        | getattr(os, "O_CLOEXEC", 0)
        | getattr(os, "O_NOFOLLOW", 0)
    )
    descriptor: int | None = None
    replaced = False
    try:
        descriptor = os.open(temporary, flags, 0o600)
        with os.fdopen(descriptor, "wb") as handle:
            descriptor = None
            handle.write(body)
            handle.flush()
            os.fsync(handle.fileno())
        os.chmod(temporary, 0o600)
        os.replace(temporary, path)
        replaced = True
        directory_fd = os.open(path.parent, os.O_RDONLY | getattr(os, "O_CLOEXEC", 0))
        try:
            os.fsync(directory_fd)
        finally:
            os.close(directory_fd)
    except OSError as error:
        if replaced:
            raise IndeterminateDraftCommit(
                "$.path: draft replacement reached its pathname but its directory "
                f"fsync failed; persisted state must be reconciled: {error}"
            ) from error
        raise ContractError(f"$.path: cannot persist draft atomically: {error}") from error
    finally:
        if descriptor is not None:
            os.close(descriptor)
        try:
            temporary.unlink()
        except FileNotFoundError:
            pass


def save_draft_atomic(
    path_value: str | Path, draft_value: object, template_value: object
) -> None:
    """Atomically persist one fully validated draft as owner-only JSON."""

    draft = validate_draft(draft_value, template_value)
    _persist_validated_draft(path_value, draft)


def save_selection_draft(path_value: str | Path, draft_value: object) -> None:
    """Atomically persist a caller-validated draft.

    The server must call :func:`validate_selection_draft` immediately before this
    persistence-only helper because the immutable template is intentionally not an
    argument here.
    """

    draft = _object(
        draft_value,
        "$",
        {
            "schema_version",
            "manifest_kind",
            "manifest_sha256",
            "draft_id",
            "workspace_id",
            "workspace_manifest_sha256",
            "template_binding",
            "revision",
            "lifecycle",
            "finalization_intent",
            "finalization_manifest_sha256",
            "created_at",
            "updated_at",
            "recordings",
        },
    )
    _constant(draft["schema_version"], DRAFT_SCHEMA_VERSION, "$.schema_version")
    _constant(draft["manifest_kind"], DRAFT_KIND, "$.manifest_kind")
    _verify_manifest_digest(draft)
    _persist_validated_draft(path_value, draft)


def _draft_file_identity(value: os.stat_result) -> tuple[int, ...]:
    return (
        value.st_dev,
        value.st_ino,
        value.st_size,
        value.st_mtime_ns,
        value.st_ctime_ns,
        value.st_mode,
        value.st_uid,
        value.st_nlink,
    )


def load_draft(path_value: str | Path, template_value: object) -> dict[str, Any]:
    """Load one bounded, duplicate-key-free, owner-only draft JSON file."""

    path = Path(path_value)
    if not path.is_absolute():
        path = path.resolve()
    try:
        before = path.lstat()
    except OSError as error:
        raise ContractError(f"$.path: cannot inspect draft: {error}") from error
    if stat.S_ISLNK(before.st_mode) or not stat.S_ISREG(before.st_mode):
        _fail("$.path", "draft must be a non-symlink regular file")
    if (
        before.st_uid != os.geteuid()
        or before.st_nlink != 1
        or stat.S_IMODE(before.st_mode) != 0o600
    ):
        _fail(
            "$.path",
            "draft must be current-user-owned, single-link, and mode 0600",
        )
    if before.st_size > MAX_DRAFT_BYTES:
        _fail("$.path", f"draft exceeds {MAX_DRAFT_BYTES} bytes")
    flags = os.O_RDONLY | getattr(os, "O_CLOEXEC", 0) | getattr(os, "O_NOFOLLOW", 0)
    descriptor: int | None = None
    try:
        descriptor = os.open(path, flags)
        opened = os.fstat(descriptor)
        if (
            not stat.S_ISREG(opened.st_mode)
            or _draft_file_identity(opened) != _draft_file_identity(before)
        ):
            _fail("$.path", "draft changed while opening")
        body = os.pread(descriptor, min(opened.st_size + 1, MAX_DRAFT_BYTES + 1), 0)
        after = os.fstat(descriptor)
        path_after = path.lstat()
    except OSError as error:
        raise ContractError(f"$.path: cannot read draft: {error}") from error
    finally:
        if descriptor is not None:
            os.close(descriptor)
    if len(body) > MAX_DRAFT_BYTES:
        _fail("$.path", f"draft exceeds {MAX_DRAFT_BYTES} bytes")
    if (
        len(body) != opened.st_size
        or _draft_file_identity(after) != _draft_file_identity(opened)
        or _draft_file_identity(path_after) != _draft_file_identity(opened)
    ):
        _fail("$.path", "draft changed while reading")

    def unique_object(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
        output: dict[str, Any] = {}
        for key, item in pairs:
            if key in output:
                raise ContractError(f"$.path: duplicate JSON object key {key!r}")
            output[key] = item
        return output

    def reject_constant(value: str) -> None:
        raise ContractError(f"$.path: non-finite JSON number {value!r} is forbidden")

    try:
        parsed = json.loads(
            body.decode("utf-8"),
            object_pairs_hook=unique_object,
            parse_constant=reject_constant,
        )
    except ContractError:
        raise
    except (UnicodeDecodeError, ValueError) as error:
        raise ContractError(f"$.path: invalid UTF-8 JSON draft: {error}") from error
    return validate_draft(parsed, template_value)


def load_selection_draft(
    path_value: str | Path,
    template_value: object,
    workspace_id: str,
    workspace_manifest_sha256: str,
) -> dict[str, Any]:
    """Load a strict draft and verify its external workspace binding."""

    draft = load_draft(path_value, template_value)
    return validate_selection_draft(
        draft,
        template_value,
        workspace_id,
        workspace_manifest_sha256,
    )
