from __future__ import annotations

import copy
import hashlib
import json
import os
import subprocess
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from jsonschema import Draft202012Validator, FormatChecker

from evaluation.review_workspace_state import (
    REVIEW_TOOL_NAME,
    REVIEW_TOOL_VERSION,
    apply_draft_operation,
    begin_draft_finalization,
    create_selection_draft,
    draft_readiness,
    load_selection_draft,
    mark_draft_finalized,
    mark_draft_invalidated,
    materialize_completed_review,
    save_selection_draft,
    validate_selection_draft,
)
from evaluation.tests.test_selection_review import SelectionFixture
from evaluation.validation import (
    ContractError,
    audit_tracked_evaluation_data,
    canonical_manifest_sha256,
    load_json,
)
from evaluation.selection_review import validate_interval_selection_review


ROOT = Path(__file__).resolve().parents[2]
SCHEMA_PATH = ROOT / "evaluation/schemas/interval-selection-draft.schema.json"
WORKSPACE_ID = "selection_workspace_fixture"
WORKSPACE_MANIFEST_SHA256 = hashlib.sha256(b"workspace-manifest").hexdigest()
CREATED_AT = "2026-08-26T21:30:00Z"
UPDATED_AT = "2026-08-26T21:45:00Z"


def seal(value: dict) -> dict:
    value["manifest_sha256"] = canonical_manifest_sha256(value)
    return value


def clean_flags() -> dict:
    return {
        "language_tags": ["en"],
        "code_switch": False,
        "speaker_overlap": False,
        "playback_speech": False,
        "noise": "clean",
    }


class ReviewWorkspaceStateTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temporary = tempfile.TemporaryDirectory()
        self.root = Path(self.temporary.name)
        self.fixture = SelectionFixture(self.root)
        self.validator_patch = patch(
            "evaluation.selection_review.validate_interval_proposal",
            side_effect=lambda proposal, request, cohort, catalog: proposal,
        )
        self.validator_patch.start()
        self.template = self.fixture.template()
        self.draft = create_selection_draft(
            self.template,
            WORKSPACE_ID,
            WORKSPACE_MANIFEST_SHA256,
            CREATED_AT,
        )
        self.schema = load_json(SCHEMA_PATH)
        self.schema_validator = Draft202012Validator(
            self.schema, format_checker=FormatChecker()
        )

    def tearDown(self) -> None:
        self.validator_patch.stop()
        self.temporary.cleanup()

    def assert_schema_valid(self, value: dict) -> None:
        errors = sorted(
            self.schema_validator.iter_errors(value), key=lambda error: list(error.path)
        )
        self.assertEqual(errors, [], [error.message for error in errors])

    def operation(self, draft: dict, body: dict, now: str = UPDATED_AT) -> dict:
        return apply_draft_operation(
            draft,
            self.template,
            draft["revision"],
            body,
            now,
        )

    def complete(self) -> dict:
        draft = self.draft
        for index, recording in enumerate(draft["recordings"]):
            draft = self.operation(
                draft,
                {
                    "operation": "set_split",
                    "recording_id": recording["recording_id"],
                    "split": "calibration" if index < 3 else "scoring",
                },
            )
        for recording in list(draft["recordings"]):
            for decision in list(recording["intervals"]):
                draft = self.operation(
                    draft,
                    {
                        "operation": "merge_coverage",
                        "selection_decision_id": decision[
                            "selection_decision_id"
                        ],
                        "start_ms": decision["proposal_start_ms"],
                        "end_ms": decision["proposal_end_ms"],
                    },
                )
                draft = self.operation(
                    draft,
                    {
                        "operation": "set_include",
                        "selection_decision_id": decision["selection_decision_id"],
                        "accepted_start_ms": decision["proposal_start_ms"],
                        "accepted_end_ms": decision["proposal_end_ms"],
                        "adjustment_reason": None,
                        "flags": clean_flags(),
                    },
                )
        return draft

    def test_create_is_deterministic_schema_valid_and_exactly_bound(self) -> None:
        replay = create_selection_draft(
            self.template,
            WORKSPACE_ID,
            WORKSPACE_MANIFEST_SHA256,
            CREATED_AT,
        )
        self.assertEqual(self.draft, replay)
        self.assertEqual(self.draft["revision"], 0)
        self.assertEqual(self.draft["lifecycle"], "draft")
        self.assertEqual(self.draft["workspace_id"], WORKSPACE_ID)
        self.assertEqual(
            self.draft["workspace_manifest_sha256"], WORKSPACE_MANIFEST_SHA256
        )
        self.assertEqual(len(self.draft["recordings"]), 12)
        self.assertEqual(
            sum(len(row["intervals"]) for row in self.draft["recordings"]), 123
        )
        self.assert_schema_valid(self.draft)
        self.assertIs(
            validate_selection_draft(
                self.draft,
                self.template,
                WORKSPACE_ID,
                WORKSPACE_MANIFEST_SHA256,
            ),
            self.draft,
        )

        for expected_workspace, expected_digest, message in (
            ("selection_workspace_other", WORKSPACE_MANIFEST_SHA256, "workspace_id"),
            (WORKSPACE_ID, hashlib.sha256(b"other").hexdigest(), "workspace_manifest"),
        ):
            with self.subTest(message=message), self.assertRaisesRegex(
                ContractError, message
            ):
                validate_selection_draft(
                    self.draft,
                    self.template,
                    expected_workspace,
                    expected_digest,
                )

    def test_unknown_partial_and_immutable_lineage_tampering_fail(self) -> None:
        unknown = copy.deepcopy(self.draft)
        unknown["surprise"] = True
        seal(unknown)
        self.assertTrue(list(self.schema_validator.iter_errors(unknown)))
        with self.assertRaisesRegex(ContractError, "unexpected"):
            validate_selection_draft(
                unknown, self.template, WORKSPACE_ID, WORKSPACE_MANIFEST_SHA256
            )

        partial = copy.deepcopy(self.draft)
        partial_decision = partial["recordings"][0]["intervals"][0]
        partial_decision["decision"] = "include"
        seal(partial)
        self.assertTrue(list(self.schema_validator.iter_errors(partial)))
        with self.assertRaisesRegex(ContractError, "accepted_start_ms"):
            validate_selection_draft(
                partial, self.template, WORKSPACE_ID, WORKSPACE_MANIFEST_SHA256
            )

        mutations = (
            ("recording", lambda value: value["recordings"][0].__setitem__("recording_id", value["recordings"][1]["recording_id"])),
            ("decision", lambda value: value["recordings"][0]["intervals"][0].__setitem__("selection_decision_id", value["recordings"][0]["intervals"][1]["selection_decision_id"])),
            ("bounds", lambda value: value["recordings"][0]["intervals"][0].__setitem__("proposal_start_ms", value["recordings"][0]["intervals"][0]["proposal_start_ms"] + 1)),
            ("order", lambda value: value["recordings"].reverse()),
        )
        for name, mutate in mutations:
            changed = copy.deepcopy(self.draft)
            mutate(changed)
            seal(changed)
            with self.subTest(name=name), self.assertRaisesRegex(
                ContractError, "immutable template|order/lineage"
            ):
                validate_selection_draft(
                    changed,
                    self.template,
                    WORKSPACE_ID,
                    WORKSPACE_MANIFEST_SHA256,
                )

    def test_typed_operations_enforce_revision_and_complete_branches(self) -> None:
        recording = self.draft["recordings"][0]
        decision = recording["intervals"][0]
        split = self.operation(
            self.draft,
            {
                "operation": "set_split",
                "recording_id": recording["recording_id"],
                "split": "calibration",
            },
        )
        self.assertEqual(split["revision"], 1)
        self.assertEqual(split["updated_at"], UPDATED_AT)
        with self.assertRaisesRegex(ContractError, "stale revision"):
            apply_draft_operation(
                split,
                self.template,
                0,
                {
                    "operation": "clear_split",
                    "recording_id": recording["recording_id"],
                },
                UPDATED_AT,
            )
        with self.assertRaisesRegex(ContractError, "unexpected"):
            apply_draft_operation(
                split,
                self.template,
                split["revision"],
                {
                    "operation": "clear_split",
                    "recording_id": recording["recording_id"],
                    "extra": True,
                },
                UPDATED_AT,
            )

        included = self.operation(
            split,
            {
                "operation": "set_include",
                "selection_decision_id": decision["selection_decision_id"],
                "accepted_start_ms": decision["proposal_start_ms"] + 10,
                "accepted_end_ms": decision["proposal_end_ms"],
                "adjustment_reason": "speech_boundary_refinement",
                "flags": clean_flags(),
            },
        )
        included_decision = included["recordings"][0]["intervals"][0]
        self.assertEqual(included_decision["decision"], "include")
        self.assert_schema_valid(included)

        with self.assertRaisesRegex(ContractError, "within the proposal"):
            self.operation(
                included,
                {
                    "operation": "set_include",
                    "selection_decision_id": decision["selection_decision_id"],
                    "accepted_start_ms": decision["proposal_start_ms"] - 1,
                    "accepted_end_ms": decision["proposal_end_ms"],
                    "adjustment_reason": "speech_boundary_refinement",
                    "flags": clean_flags(),
                },
            )
        with self.assertRaisesRegex(ContractError, "adjustment_reason"):
            self.operation(
                included,
                {
                    "operation": "set_include",
                    "selection_decision_id": decision["selection_decision_id"],
                    "accepted_start_ms": decision["proposal_start_ms"] + 1,
                    "accepted_end_ms": decision["proposal_end_ms"],
                    "adjustment_reason": None,
                    "flags": clean_flags(),
                },
            )

        excluded = self.operation(
            included,
            {
                "operation": "set_exclude",
                "selection_decision_id": decision["selection_decision_id"],
                "rejection_reason": "technical_quality",
            },
        )
        self.assertEqual(excluded["recordings"][0]["intervals"][0]["decision"], "exclude")
        cleared = self.operation(
            excluded,
            {
                "operation": "clear_decision",
                "selection_decision_id": decision["selection_decision_id"],
            },
        )
        self.assertIsNone(cleared["recordings"][0]["intervals"][0]["decision"])
        split_cleared = self.operation(
            cleared,
            {
                "operation": "clear_split",
                "recording_id": recording["recording_id"],
            },
        )
        self.assertIsNone(split_cleared["recordings"][0]["split"])

    def test_coverage_is_normalized_bounded_and_never_infers_attestation(self) -> None:
        decision = self.draft["recordings"][0]["intervals"][0]
        start = decision["proposal_start_ms"]
        draft = self.operation(
            self.draft,
            {
                "operation": "merge_coverage",
                "selection_decision_id": decision["selection_decision_id"],
                "start_ms": start + 100,
                "end_ms": start + 200,
            },
        )
        draft = self.operation(
            draft,
            {
                "operation": "merge_coverage",
                "selection_decision_id": decision["selection_decision_id"],
                "start_ms": start + 200,
                "end_ms": start + 300,
            },
        )
        draft = self.operation(
            draft,
            {
                "operation": "merge_coverage",
                "selection_decision_id": decision["selection_decision_id"],
                "start_ms": start + 150,
                "end_ms": start + 250,
            },
        )
        current = draft["recordings"][0]["intervals"][0]
        self.assertEqual(
            current["coverage_ranges"],
            [{"start_ms": start + 100, "end_ms": start + 300}],
        )
        self.assertIsNone(current["decision"])
        readiness = draft_readiness(draft, self.template)
        self.assertFalse(readiness["ready_to_materialize"])
        self.assertGreater(readiness["coverage_duration_ms"], 0)
        self.assertNotIn("reviewer", draft)
        with self.assertRaisesRegex(ContractError, "persisted finalizing"):
            materialize_completed_review(
                self.template,
                draft,
                "reviewer_fixture",
                "2026-08-26T22:00:00Z",
                "2026-08-26T22:05:00Z",
            )

        with self.assertRaisesRegex(ContractError, "coverage must be"):
            self.operation(
                draft,
                {
                    "operation": "merge_coverage",
                    "selection_decision_id": decision["selection_decision_id"],
                    "start_ms": start - 1,
                    "end_ms": start + 1,
                },
            )

        nonnormalized = copy.deepcopy(draft)
        nonnormalized["recordings"][0]["intervals"][0]["coverage_ranges"] = [
            {"start_ms": start + 100, "end_ms": start + 200},
            {"start_ms": start + 200, "end_ms": start + 250},
        ]
        seal(nonnormalized)
        with self.assertRaisesRegex(ContractError, "adjacency-merged"):
            validate_selection_draft(
                nonnormalized,
                self.template,
                WORKSPACE_ID,
                WORKSPACE_MANIFEST_SHA256,
            )

        too_many = copy.deepcopy(draft)
        too_many["recordings"][0]["intervals"][0]["coverage_ranges"] = [
            {"start_ms": start + index * 2, "end_ms": start + index * 2 + 1}
            for index in range(513)
        ]
        seal(too_many)
        self.assertTrue(list(self.schema_validator.iter_errors(too_many)))
        with self.assertRaisesRegex(ContractError, "at most 512"):
            validate_selection_draft(
                too_many,
                self.template,
                WORKSPACE_ID,
                WORKSPACE_MANIFEST_SHA256,
            )

    def test_persistence_never_writes_a_draft_its_loader_must_reject(self) -> None:
        directory = self.root / "bounded-draft"
        directory.mkdir(mode=0o700)
        path = directory / "draft.json"
        save_selection_draft(path, self.draft)
        before = path.read_bytes()
        with (
            patch("evaluation.review_workspace_state.MAX_DRAFT_BYTES", 10),
            self.assertRaisesRegex(ContractError, "serialized draft exceeds"),
        ):
            save_selection_draft(path, self.draft)
        self.assertEqual(path.read_bytes(), before)

    def test_materialized_review_has_exact_shape_and_can_mark_finalized(self) -> None:
        draft = self.complete()
        readiness = draft_readiness(draft, self.template)
        self.assertTrue(readiness["ready_to_begin_finalization"])
        self.assertFalse(readiness["ready_to_materialize"])
        self.assertEqual(readiness["accepted_interval_count"], 123)
        self.assertEqual(readiness["accepted_duration_ms"], 3_690_000)

        with self.assertRaisesRegex(ContractError, "persisted finalizing"):
            materialize_completed_review(
                self.template,
                draft,
                "reviewer_fixture",
                "2026-08-26T22:00:00Z",
                "2026-08-26T22:05:00Z",
            )

        draft = begin_draft_finalization(
            draft,
            self.template,
            draft["revision"],
            "reviewer_fixture",
            "2026-08-26T22:00:00Z",
            "2026-08-26T22:05:00Z",
            "2026-08-26T22:05:00Z",
        )
        self.assertEqual(draft["lifecycle"], "finalizing")
        self.assertEqual(
            draft["finalization_intent"],
            {
                "reviewer_id": "reviewer_fixture",
                "reviewed_at": "2026-08-26T22:00:00Z",
                "attested_at": "2026-08-26T22:05:00Z",
                "begun_at": "2026-08-26T22:05:00Z",
            },
        )
        self.assert_schema_valid(draft)
        readiness = draft_readiness(draft, self.template)
        self.assertFalse(readiness["ready_to_begin_finalization"])
        self.assertTrue(readiness["ready_to_materialize"])

        malformed = copy.deepcopy(draft)
        malformed["finalization_intent"] = None
        seal(malformed)
        self.assertTrue(list(self.schema_validator.iter_errors(malformed)))
        with self.assertRaisesRegex(ContractError, "finalization_intent"):
            validate_selection_draft(
                malformed,
                self.template,
                WORKSPACE_ID,
                WORKSPACE_MANIFEST_SHA256,
            )

        path = self.root / "crash-recovery" / "selection-draft.json"
        save_selection_draft(path, draft)
        draft = load_selection_draft(
            path,
            self.template,
            WORKSPACE_ID,
            WORKSPACE_MANIFEST_SHA256,
        )
        with self.assertRaisesRegex(ContractError, "persisted reviewer intent"):
            materialize_completed_review(
                self.template,
                draft,
                "different_reviewer",
                "2026-08-26T22:00:00Z",
                "2026-08-26T22:05:00Z",
            )
        completed = materialize_completed_review(
            self.template,
            draft,
            "reviewer_fixture",
            "2026-08-26T22:00:00Z",
            "2026-08-26T22:05:00Z",
        )
        self.assertEqual(completed["review_state"], "completed_private")
        self.assertEqual(
            completed["reviewer"]["review_tool"],
            {"name": REVIEW_TOOL_NAME, "version": REVIEW_TOOL_VERSION},
        )
        self.assertTrue(completed["reviewer"]["direct_parent_media_reviewed"])
        self.assertFalse(completed["reviewer"]["asr_outputs_inspected"])
        self.assertFalse(completed["reviewer"]["reference_text_inspected"])
        self.assertNotIn("coverage_ranges", completed["recordings"][0]["intervals"][0])
        validate_interval_selection_review(
            completed,
            self.fixture.cohort,
            self.fixture.requests,
            self.fixture.proposals,
            self.fixture.catalog,
            require_completed=True,
        )

        finalized = mark_draft_finalized(
            draft,
            self.template,
            draft["revision"],
            completed["manifest_sha256"],
            "2026-08-26T22:06:00Z",
        )
        self.assertEqual(finalized["lifecycle"], "finalized")
        self.assertEqual(
            finalized["finalization_manifest_sha256"], completed["manifest_sha256"]
        )
        self.assert_schema_valid(finalized)
        with self.assertRaisesRegex(ContractError, "only a draft"):
            apply_draft_operation(
                finalized,
                self.template,
                finalized["revision"],
                {
                    "operation": "clear_split",
                    "recording_id": finalized["recordings"][0]["recording_id"],
                },
                "2026-08-26T22:07:00Z",
            )

    def test_invalidation_and_owner_only_atomic_persistence(self) -> None:
        invalidated = mark_draft_invalidated(
            self.draft,
            self.template,
            self.draft["revision"],
            "2026-08-26T21:46:00Z",
        )
        self.assertEqual(invalidated["lifecycle"], "invalidated")
        self.assertIsNone(invalidated["finalization_manifest_sha256"])
        self.assertEqual(invalidated["updated_at"], "2026-08-26T21:46:00Z")
        self.assert_schema_valid(invalidated)

        path = self.root / "private-workspace" / "selection-draft.json"
        validated = validate_selection_draft(
            self.draft,
            self.template,
            WORKSPACE_ID,
            WORKSPACE_MANIFEST_SHA256,
        )
        save_selection_draft(path, validated)
        self.assertEqual(path.parent.stat().st_mode & 0o777, 0o700)
        self.assertEqual(path.stat().st_mode & 0o777, 0o600)
        loaded = load_selection_draft(
            path,
            self.template,
            WORKSPACE_ID,
            WORKSPACE_MANIFEST_SHA256,
        )
        self.assertEqual(loaded, self.draft)
        os.chmod(path, 0o644)
        with self.assertRaisesRegex(ContractError, "mode 0600"):
            load_selection_draft(
                path,
                self.template,
                WORKSPACE_ID,
                WORKSPACE_MANIFEST_SHA256,
            )

    def test_loader_rejects_path_swap_hardlinks_and_huge_integers(self) -> None:
        live = self.root / "live"
        replacement = self.root / "replacement"
        live.mkdir(mode=0o700)
        replacement.mkdir(mode=0o700)
        live_path = live / "draft.json"
        replacement_path = replacement / "draft.json"
        save_selection_draft(live_path, self.draft)
        changed = self.operation(
            self.draft,
            {
                "operation": "set_split",
                "recording_id": self.draft["recordings"][0]["recording_id"],
                "split": "calibration",
            },
        )
        save_selection_draft(replacement_path, changed)

        real_pread = os.pread
        swapped = False

        def swap_after_descriptor_read(
            descriptor: int, byte_count: int, offset: int
        ) -> bytes:
            nonlocal swapped
            body = real_pread(descriptor, byte_count, offset)
            if not swapped:
                swapped = True
                live.rename(self.root / "old-live")
                replacement.rename(live)
            return body

        with (
            patch(
                "evaluation.review_workspace_state.os.pread",
                side_effect=swap_after_descriptor_read,
            ),
            self.assertRaisesRegex(ContractError, "changed while reading"),
        ):
            load_selection_draft(
                live_path,
                self.template,
                WORKSPACE_ID,
                WORKSPACE_MANIFEST_SHA256,
            )

        hardlink = live / "draft-hardlink.json"
        os.link(live_path, hardlink)
        with self.assertRaisesRegex(ContractError, "single-link"):
            load_selection_draft(
                live_path,
                self.template,
                WORKSPACE_ID,
                WORKSPACE_MANIFEST_SHA256,
            )

        huge_directory = self.root / "huge"
        huge_directory.mkdir(mode=0o700)
        huge_path = huge_directory / "draft.json"
        huge_path.write_bytes(b'{"revision":' + b"9" * 5000 + b"}\n")
        os.chmod(huge_path, 0o600)
        with self.assertRaisesRegex(ContractError, "invalid UTF-8 JSON draft"):
            load_selection_draft(
                huge_path,
                self.template,
                WORKSPACE_ID,
                WORKSPACE_MANIFEST_SHA256,
            )

    def test_audit_must_reject_tracked_interval_selection_draft(self) -> None:
        repository = self.root / "repository"
        repository.mkdir()
        subprocess.run(["git", "init", "-q", str(repository)], check=True)
        draft_path = repository / "selection-draft.json"
        draft_path.write_text(
            json.dumps({"manifest_kind": "interval_selection_draft"}),
            encoding="utf-8",
        )
        subprocess.run(
            ["git", "-C", str(repository), "add", "selection-draft.json"],
            check=True,
        )
        with self.assertRaisesRegex(ContractError, "private selection workspace data"):
            audit_tracked_evaluation_data(repository)


if __name__ == "__main__":
    unittest.main()
