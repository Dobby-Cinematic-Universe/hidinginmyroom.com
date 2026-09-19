from __future__ import annotations

import copy
import hashlib
import json
import os
import tempfile
import threading
import unittest
from pathlib import Path
from unittest.mock import patch

from evaluation.review_workspace import (
    MediaBinding,
    PinnedInput,
    ReviewWorkspace,
    StaleRevision,
    WorkspacePaths,
    _atomic_write_bytes,
    _fingerprint,
    _hash_path,
    _pin_ui_assets,
    _sha256_fd,
)
from evaluation.review_workspace_state import (
    IndeterminateDraftCommit,
    apply_draft_operation,
    begin_draft_finalization,
    create_selection_draft,
    load_selection_draft,
    save_selection_draft,
)
from evaluation.tests.test_selection_review import SelectionFixture
from evaluation.validation import ContractError, canonical_manifest_sha256


WORKSPACE_ID = "selection_workspace_integration_fixture"
WORKSPACE_MANIFEST_SHA256 = hashlib.sha256(b"integration-workspace").hexdigest()
CREATED_AT = "2026-08-26T21:30:00Z"
UPDATED_AT = "2026-08-26T21:45:00Z"
FINALIZED_AT = "2026-08-27T12:00:00Z"


def clean_flags() -> dict[str, object]:
    return {
        "language_tags": ["en"],
        "code_switch": False,
        "speaker_overlap": False,
        "playback_speech": False,
        "noise": "clean",
    }


def finalization_payload(revision: int, reviewer_id: str = "reviewer_fixture") -> dict:
    return {
        "expected_revision": revision,
        "reviewer_id": reviewer_id,
        "direct_parent_media_reviewed": True,
        "asr_outputs_inspected": False,
        "reference_text_inspected": False,
        "selection_basis": "source_metadata_and_direct_parent_media_only",
    }


class PinnedInputStub:
    def __init__(self, value: dict, label: str):
        self.value = value
        self.label = label
        self.reverify_count = 0
        self.identity_check_count = 0
        self.closed = False

    def reverify(self) -> None:
        self.reverify_count += 1

    def assert_identity(self) -> None:
        self.identity_check_count += 1

    def close(self) -> None:
        self.closed = True


class ReviewWorkspaceIntegrationTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.fixture_temporary = tempfile.TemporaryDirectory()
        fixture_root = Path(cls.fixture_temporary.name)
        cls.fixture = SelectionFixture(fixture_root)
        with patch(
            "evaluation.selection_review.validate_interval_proposal",
            side_effect=lambda proposal, request, cohort, catalog: proposal,
        ):
            cls.template = cls.fixture.template()
        draft = create_selection_draft(
            cls.template,
            WORKSPACE_ID,
            WORKSPACE_MANIFEST_SHA256,
            CREATED_AT,
        )
        for index, recording in enumerate(list(draft["recordings"])):
            draft = apply_draft_operation(
                draft,
                cls.template,
                draft["revision"],
                {
                    "operation": "set_split",
                    "recording_id": recording["recording_id"],
                    "split": "calibration" if index < 3 else "scoring",
                },
                UPDATED_AT,
            )
        for recording in list(draft["recordings"]):
            for decision in list(recording["intervals"]):
                decision_id = decision["selection_decision_id"]
                draft = apply_draft_operation(
                    draft,
                    cls.template,
                    draft["revision"],
                    {
                        "operation": "set_include",
                        "selection_decision_id": decision_id,
                        "accepted_start_ms": decision["proposal_start_ms"],
                        "accepted_end_ms": decision["proposal_end_ms"],
                        "adjustment_reason": None,
                        "flags": clean_flags(),
                    },
                    UPDATED_AT,
                )
                draft = apply_draft_operation(
                    draft,
                    cls.template,
                    draft["revision"],
                    {
                        "operation": "merge_coverage",
                        "selection_decision_id": decision_id,
                        "start_ms": decision["proposal_start_ms"],
                        "end_ms": decision["proposal_end_ms"],
                    },
                    UPDATED_AT,
                )
        cls.completed_draft = draft

    @classmethod
    def tearDownClass(cls) -> None:
        cls.fixture_temporary.cleanup()

    def setUp(self) -> None:
        self.temporary = tempfile.TemporaryDirectory()
        self.root = Path(self.temporary.name)
        self.workspace = self.make_workspace()

    def tearDown(self) -> None:
        self.workspace.close()
        self.temporary.cleanup()

    def make_workspace(self, draft: dict | None = None) -> ReviewWorkspace:
        private_parent = self.root / "private"
        private_parent.mkdir(mode=0o700)
        workspace_root = private_parent / "workspace"
        workspace_root.mkdir(mode=0o700)
        paths = WorkspacePaths.under(workspace_root)
        paths.catalog_snapshot.write_bytes(b"tiny-catalogue-snapshot")
        os.chmod(paths.catalog_snapshot, 0o400)

        media_root = self.root / "media"
        media_root.mkdir(mode=0o700)
        native_ids = {
            candidate["recording_id"]: candidate["native_id"]
            for candidate in self.fixture.cohort["candidates"]
        }
        media: list[MediaBinding] = []
        for ordinal, recording in enumerate(self.template["recordings"], start=1):
            body = f"tiny-parent-media-{ordinal}".encode("ascii")
            digest = hashlib.sha256(body).hexdigest()
            path = media_root / f"parent-{ordinal}.mp4"
            path.write_bytes(body)
            os.chmod(path, 0o400)
            descriptor = os.open(
                path,
                os.O_RDONLY | getattr(os, "O_CLOEXEC", 0),
            )
            info = os.fstat(descriptor)
            media.append(
                MediaBinding(
                    ordinal=ordinal,
                    candidate_id=recording["candidate_id"],
                    recording_id=recording["recording_id"],
                    source_id=recording["source_id"],
                    source_native_id=native_ids[recording["recording_id"]],
                    title=f"Integration parent {ordinal}",
                    proposal_id=recording["proposal_id"],
                    proposal_schema_version=recording["proposal_schema_version"],
                    media_id=f"media_sha256_{digest}",
                    sha256=digest,
                    byte_count=len(body),
                    duration_ms=500_000,
                    rendition_id=f"rendition_integration_{ordinal}",
                    rendition_kind="acquired_source_media",
                    media_location_id=f"media_location_integration_{ordinal}",
                    storage_uri=path.as_uri(),
                    storage_class="local_hot_cache",
                    path=path,
                    mime_type="video/mp4",
                    codecs=("h264", "aac"),
                    fd=descriptor,
                    startup_fingerprint=_fingerprint(info),
                    startup_mode=0o400,
                    mutability_policy="read_only_exact_bytes",
                    opaque_id=f"opaque-parent-{ordinal}",
                )
            )

        cohort_pin = PinnedInputStub(copy.deepcopy(self.fixture.cohort), "cohort")
        pins = [
            cohort_pin,
            *[
                PinnedInputStub(copy.deepcopy(value), f"request-{index}")
                for index, value in enumerate(self.fixture.requests, start=1)
            ],
            *[
                PinnedInputStub(copy.deepcopy(value), f"proposal-{index}")
                for index, value in enumerate(self.fixture.proposals, start=1)
            ],
        ]
        workspace = ReviewWorkspace.__new__(ReviewWorkspace)
        workspace.paths = paths
        workspace._mutex = threading.RLock()
        workspace._draft_state_unavailable = False
        workspace.manifest = {
            "workspace_id": WORKSPACE_ID,
            "manifest_sha256": WORKSPACE_MANIFEST_SHA256,
        }
        workspace.template = copy.deepcopy(self.template)
        workspace.draft = copy.deepcopy(draft or self.completed_draft)
        workspace.cohort_pin = cohort_pin
        workspace.requests = copy.deepcopy(self.fixture.requests)
        workspace.proposals = copy.deepcopy(self.fixture.proposals)
        workspace.pins = pins
        workspace.media = media
        workspace.catalog_snapshot_sha256 = hashlib.sha256(
            paths.catalog_snapshot.read_bytes()
        ).hexdigest()
        workspace.catalog_snapshot_fingerprint = _fingerprint(
            paths.catalog_snapshot.lstat()
        )
        workspace.catalog_projection_sha256 = hashlib.sha256(
            b"integration-catalogue-projection"
        ).hexdigest()
        workspace.closed = False
        save_selection_draft(paths.draft, workspace.draft)
        return workspace

    def mutate_first_media(self, body: bytes = b"mutated-parent-media") -> None:
        binding = self.workspace.media[0]
        os.chmod(binding.path, 0o600)
        binding.path.write_bytes(body)
        os.chmod(binding.path, 0o400)

    def finalizing_draft(self, draft: dict | None = None) -> dict:
        source = copy.deepcopy(draft or self.completed_draft)
        return begin_draft_finalization(
            source,
            self.template,
            source["revision"],
            "reviewer_fixture",
            FINALIZED_AT,
            FINALIZED_AT,
            FINALIZED_AT,
        )

    def completed_validator(self):
        return patch(
            "evaluation.review_workspace.validate_interval_selection_review",
            side_effect=lambda review, *args, **kwargs: review,
        )

    def test_client_kind_translation_persists_and_stale_revision_is_typed(self) -> None:
        recording = self.workspace.draft["recordings"][0]
        revision = self.workspace.draft["revision"]
        with patch("evaluation.review_workspace._utc_now", return_value=FINALIZED_AT):
            self.workspace.apply_operation(
                revision,
                {
                    "kind": "set_split",
                    "recording_id": recording["recording_id"],
                    "split": "scoring",
                },
            )
        self.assertEqual(self.workspace.draft["revision"], revision + 1)
        self.assertEqual(self.workspace.draft["recordings"][0]["split"], "scoring")
        persisted = load_selection_draft(
            self.workspace.paths.draft,
            self.template,
            WORKSPACE_ID,
            WORKSPACE_MANIFEST_SHA256,
        )
        self.assertEqual(persisted, self.workspace.draft)
        with self.assertRaises(StaleRevision) as raised:
            self.workspace.apply_operation(
                revision,
                {
                    "kind": "clear_split",
                    "recording_id": recording["recording_id"],
                },
            )
        self.assertEqual(raised.exception.current_revision, revision + 1)

    def test_failed_persistence_never_publishes_rejected_memory_state(self) -> None:
        recording = self.workspace.draft["recordings"][0]
        revision = self.workspace.draft["revision"]
        memory_before = copy.deepcopy(self.workspace.draft)
        disk_before = self.workspace.paths.draft.read_bytes()
        with (
            patch("evaluation.review_workspace._utc_now", return_value=FINALIZED_AT),
            patch(
                "evaluation.review_workspace_state.save_selection_draft",
                side_effect=ContractError("$.path: injected pre-replace failure"),
            ),
            self.assertRaisesRegex(ContractError, "injected pre-replace failure"),
        ):
            self.workspace.apply_operation(
                revision,
                {
                    "kind": "set_split",
                    "recording_id": recording["recording_id"],
                    "split": "scoring",
                },
            )
        self.assertEqual(self.workspace.draft, memory_before)
        self.assertEqual(self.workspace.paths.draft.read_bytes(), disk_before)

    def test_post_replace_fsync_failure_reconciles_then_requires_restart(self) -> None:
        recording = self.workspace.draft["recordings"][0]
        revision = self.workspace.draft["revision"]
        real_fsync = os.fsync
        calls = 0

        def fail_directory_fsync(descriptor: int) -> None:
            nonlocal calls
            calls += 1
            if calls == 2:
                raise OSError("injected directory fsync failure")
            real_fsync(descriptor)

        with (
            patch("evaluation.review_workspace._utc_now", return_value=FINALIZED_AT),
            patch(
                "evaluation.review_workspace_state.os.fsync",
                side_effect=fail_directory_fsync,
            ),
            self.assertRaises(IndeterminateDraftCommit),
        ):
            self.workspace.apply_operation(
                revision,
                {
                    "kind": "set_split",
                    "recording_id": recording["recording_id"],
                    "split": "scoring",
                },
            )

        persisted = load_selection_draft(
            self.workspace.paths.draft,
            self.template,
            WORKSPACE_ID,
            WORKSPACE_MANIFEST_SHA256,
        )
        self.assertEqual(self.workspace.draft, persisted)
        self.assertEqual(persisted["revision"], revision + 1)
        self.assertEqual(persisted["recordings"][0]["split"], "scoring")
        with self.assertRaisesRegex(ContractError, "restart the workspace"):
            self.workspace.bootstrap(prefix="/private/", csrf_token="csrf")
        with self.assertRaisesRegex(ContractError, "restart the workspace"):
            self.workspace.apply_operation(
                persisted["revision"],
                {
                    "kind": "clear_split",
                    "recording_id": recording["recording_id"],
                },
            )

    def test_failed_intent_persistence_never_begins_external_finalization(self) -> None:
        revision = self.workspace.draft["revision"]
        memory_before = copy.deepcopy(self.workspace.draft)
        disk_before = self.workspace.paths.draft.read_bytes()
        with (
            patch("evaluation.review_workspace._utc_now", return_value=FINALIZED_AT),
            patch(
                "evaluation.review_workspace_state.save_selection_draft",
                side_effect=ContractError("$.path: injected intent save failure"),
            ),
            patch.object(self.workspace, "reverify_exact_inputs") as reverify,
            self.assertRaisesRegex(ContractError, "intent save failure"),
        ):
            self.workspace.finalize(revision, finalization_payload(revision))
        reverify.assert_not_called()
        self.assertEqual(self.workspace.draft, memory_before)
        self.assertEqual(self.workspace.paths.draft.read_bytes(), disk_before)
        self.assertFalse(self.workspace.paths.completed_review.exists())

    def test_bootstrap_maps_full_playback_coverage_and_readiness(self) -> None:
        bootstrap = self.workspace.bootstrap(prefix="/private/", csrf_token="csrf")
        self.assertEqual(bootstrap["state"], "ready")
        self.assertTrue(bootstrap["progress"]["ready_to_begin_finalization"])
        self.assertFalse(bootstrap["progress"]["ready_to_materialize"])
        self.assertEqual(bootstrap["progress"]["coverage_duration_ms"], 3_690_000)
        self.assertEqual(bootstrap["progress"]["accepted_duration_ms"], 3_690_000)
        first = bootstrap["recordings"][0]["intervals"][0]
        self.assertNotIn("coverage_ranges", first)
        self.assertEqual(
            first["playback_coverage_ranges"],
            [
                {
                    "start_ms": first["proposal_start_ms"],
                    "end_ms": first["proposal_end_ms"],
                }
            ],
        )
        self.assertEqual(
            first["playback_coverage_ms"],
            first["proposal_end_ms"] - first["proposal_start_ms"],
        )
        self.assertIsNone(bootstrap["completed_manifest_sha256"])

    def test_finalize_persists_intent_before_verification_and_marks_digest(self) -> None:
        events: list[str] = []
        real_save = save_selection_draft
        real_write = _atomic_write_bytes

        def save_hook(path: Path, value: dict) -> None:
            events.append(f"save:{value['lifecycle']}")
            real_save(path, value)

        def write_hook(
            path: Path,
            body: bytes,
            *,
            mode: int,
            replace: bool,
        ) -> None:
            if path == self.workspace.paths.completed_review:
                label = "publish-review"
            elif path == self.workspace.paths.receipt:
                label = "publish-receipt"
            elif "-review-" in path.name:
                label = "stage-review"
            else:
                label = "stage-receipt"
            events.append(f"write:{label}")
            real_write(path, body, mode=mode, replace=replace)

        def validate_hook(review: dict, *args, **kwargs) -> dict:
            events.append("validate")
            return review

        revision = self.workspace.draft["revision"]
        with (
            patch("evaluation.review_workspace._utc_now", return_value=FINALIZED_AT),
            patch(
                "evaluation.review_workspace_state.save_selection_draft",
                side_effect=save_hook,
            ),
            patch(
                "evaluation.review_workspace._atomic_write_bytes",
                side_effect=write_hook,
            ),
            patch(
                "evaluation.review_workspace.validate_interval_selection_review",
                side_effect=validate_hook,
            ),
            patch.object(
                self.workspace,
                "reverify_exact_inputs",
                side_effect=lambda: events.append("reverify"),
            ),
        ):
            self.workspace.finalize(revision, finalization_payload(revision))

        self.assertLess(events.index("save:finalizing"), events.index("reverify"))
        self.assertLess(events.index("reverify"), events.index("validate"))
        self.assertLess(events.index("validate"), events.index("write:stage-review"))
        self.assertLess(
            events.index("write:stage-review"), events.index("write:stage-receipt")
        )
        self.assertLess(
            events.index("write:stage-receipt"), events.index("write:publish-receipt")
        )
        self.assertLess(
            events.index("write:publish-receipt"), events.index("write:publish-review")
        )
        self.assertLess(
            events.index("write:publish-review"), events.index("save:finalized")
        )
        self.assertEqual(self.workspace.draft["lifecycle"], "finalized")
        completed = json.loads(self.workspace.paths.completed_review.read_bytes())
        self.assertEqual(
            self.workspace.draft["finalization_manifest_sha256"],
            completed["manifest_sha256"],
        )
        persisted = load_selection_draft(
            self.workspace.paths.draft,
            self.template,
            WORKSPACE_ID,
            WORKSPACE_MANIFEST_SHA256,
        )
        self.assertEqual(persisted, self.workspace.draft)

    def test_crash_resume_accepts_identical_output_and_receipt_deterministically(self) -> None:
        finalizing = self.finalizing_draft()
        self.workspace.draft = copy.deepcopy(finalizing)
        save_selection_draft(self.workspace.paths.draft, self.workspace.draft)
        with self.completed_validator():
            self.workspace._complete_finalization()
        expected_draft = copy.deepcopy(self.workspace.draft)
        expected_review = self.workspace.paths.completed_review.read_bytes()
        expected_receipt = self.workspace.paths.receipt.read_bytes()

        self.workspace.draft = copy.deepcopy(finalizing)
        save_selection_draft(self.workspace.paths.draft, self.workspace.draft)
        with self.completed_validator():
            self.workspace._complete_finalization()
        self.assertEqual(self.workspace.draft, expected_draft)
        self.assertEqual(self.workspace.paths.completed_review.read_bytes(), expected_review)
        self.assertEqual(self.workspace.paths.receipt.read_bytes(), expected_receipt)

    def test_crash_resume_rejects_noncanonical_equivalent_receipt(self) -> None:
        finalizing = self.finalizing_draft()
        self.workspace.draft = copy.deepcopy(finalizing)
        save_selection_draft(self.workspace.paths.draft, self.workspace.draft)
        with self.completed_validator():
            self.workspace._complete_finalization()
        receipt = json.loads(self.workspace.paths.receipt.read_bytes())
        os.chmod(self.workspace.paths.receipt, 0o600)
        self.workspace.paths.receipt.write_text(
            json.dumps(receipt, indent=2) + "\n", encoding="utf-8"
        )
        os.chmod(self.workspace.paths.receipt, 0o400)
        self.workspace.draft = copy.deepcopy(finalizing)
        save_selection_draft(self.workspace.paths.draft, self.workspace.draft)
        with (
            self.completed_validator(),
            self.assertRaisesRegex(ContractError, "conflicting finalization receipt"),
        ):
            self.workspace._complete_finalization()
        self.assertEqual(self.workspace.draft["lifecycle"], "finalizing")

    def test_conflicting_preexisting_output_fails_without_receipt_or_final_mark(self) -> None:
        self.workspace.draft = self.finalizing_draft()
        save_selection_draft(self.workspace.paths.draft, self.workspace.draft)
        self.workspace.paths.completed_review.write_bytes(b"{}\n")
        os.chmod(self.workspace.paths.completed_review, 0o400)
        with self.completed_validator(), self.assertRaisesRegex(
            ContractError, "conflicting completed review"
        ):
            self.workspace._complete_finalization()
        self.assertEqual(self.workspace.draft["lifecycle"], "finalizing")
        self.assertFalse(self.workspace.paths.receipt.exists())
        persisted = load_selection_draft(
            self.workspace.paths.draft,
            self.template,
            WORKSPACE_ID,
            WORKSPACE_MANIFEST_SHA256,
        )
        self.assertEqual(persisted["lifecycle"], "finalizing")

    def test_exact_input_failure_invalidates_persists_and_creates_no_output(self) -> None:
        events: list[str] = []
        real_save = save_selection_draft

        def save_hook(path: Path, value: dict) -> None:
            events.append(f"save:{value['lifecycle']}")
            real_save(path, value)

        revision = self.workspace.draft["revision"]
        with (
            patch("evaluation.review_workspace._utc_now", return_value=FINALIZED_AT),
            patch(
                "evaluation.review_workspace_state.save_selection_draft",
                side_effect=save_hook,
            ),
            patch.object(
                self.workspace,
                "reverify_exact_inputs",
                side_effect=ContractError("$.media: changed"),
            ),
            self.assertRaisesRegex(ContractError, "media: changed"),
        ):
            self.workspace.finalize(revision, finalization_payload(revision))
        self.assertEqual(events, ["save:finalizing", "save:invalidated"])
        self.assertEqual(self.workspace.draft["lifecycle"], "invalidated")
        self.assertIsNotNone(self.workspace.draft["finalization_intent"])
        self.assertFalse(self.workspace.paths.completed_review.exists())
        self.assertFalse(self.workspace.paths.receipt.exists())
        persisted = load_selection_draft(
            self.workspace.paths.draft,
            self.template,
            WORKSPACE_ID,
            WORKSPACE_MANIFEST_SHA256,
        )
        self.assertEqual(persisted, self.workspace.draft)

    def test_validator_regression_preserves_recoverable_finalizing_state(self) -> None:
        revision = self.workspace.draft["revision"]
        with (
            patch("evaluation.review_workspace._utc_now", return_value=FINALIZED_AT),
            patch(
                "evaluation.review_workspace.validate_interval_selection_review",
                side_effect=ContractError("$.validator: injected regression"),
            ),
            self.assertRaisesRegex(ContractError, "injected regression"),
        ):
            self.workspace.finalize(revision, finalization_payload(revision))
        self.assertEqual(self.workspace.draft["lifecycle"], "finalizing")
        self.assertIsNotNone(self.workspace.draft["finalization_intent"])
        self.assertFalse(self.workspace.paths.completed_review.exists())

    def test_input_mutation_after_validation_invalidates_before_output(self) -> None:
        revision = self.workspace.draft["revision"]

        def mutate_after_validation(review: dict, *args, **kwargs) -> dict:
            self.mutate_first_media()
            return review

        with (
            patch("evaluation.review_workspace._utc_now", return_value=FINALIZED_AT),
            patch(
                "evaluation.review_workspace.validate_interval_selection_review",
                side_effect=mutate_after_validation,
            ),
            self.assertRaisesRegex(ContractError, "parent media changed"),
        ):
            self.workspace.finalize(revision, finalization_payload(revision))
        self.assertEqual(self.workspace.draft["lifecycle"], "invalidated")
        self.assertFalse(self.workspace.paths.completed_review.exists())
        self.assertFalse(self.workspace.paths.receipt.exists())

    def test_input_mutation_during_staging_never_publishes_canonical_artifacts(self) -> None:
        revision = self.workspace.draft["revision"]
        real_write = _atomic_write_bytes
        mutated = False

        def mutate_after_review_write(
            path: Path,
            body: bytes,
            *,
            mode: int,
            replace: bool,
        ) -> None:
            nonlocal mutated
            real_write(path, body, mode=mode, replace=replace)
            if (
                path.parent == self.workspace.paths.root
                and "noncanonical-finalization-review" in path.name
                and not mutated
            ):
                mutated = True
                self.mutate_first_media()

        with (
            patch("evaluation.review_workspace._utc_now", return_value=FINALIZED_AT),
            self.completed_validator(),
            patch(
                "evaluation.review_workspace._atomic_write_bytes",
                side_effect=mutate_after_review_write,
            ),
            self.assertRaisesRegex(ContractError, "parent media changed"),
        ):
            self.workspace.finalize(revision, finalization_payload(revision))
        self.assertTrue(mutated)
        self.assertEqual(self.workspace.draft["lifecycle"], "invalidated")
        self.assertFalse(self.workspace.paths.completed_review.exists())
        self.assertFalse(self.workspace.paths.receipt.exists())
        staged = list(
            self.workspace.paths.root.glob(".noncanonical-finalization-*.json")
        )
        self.assertEqual(len(staged), 2)
        self.assertTrue(all(path.stat().st_mode & 0o777 == 0o400 for path in staged))
        bootstrap = self.workspace.bootstrap(prefix="/private/", csrf_token="csrf")
        self.assertEqual(bootstrap["state"], "invalid")

    def test_pinned_input_path_replacement_during_hash_is_rejected(self) -> None:
        path = self.root / "pinned-input.json"
        path.write_bytes(b'{"value":"original"}\n')
        pin = PinnedInput.open_json("request", 1, path)
        real_hash = _sha256_fd
        replaced = False

        def replace_after_hash(descriptor: int) -> str:
            nonlocal replaced
            digest = real_hash(descriptor)
            if descriptor == pin.fd and not replaced:
                replacement = path.with_name("pinned-input-replacement.json")
                replacement.write_bytes(b'{"value":"replaced"}\n')
                os.replace(replacement, path)
                replaced = True
            return digest

        try:
            with (
                patch(
                    "evaluation.review_workspace._sha256_fd",
                    side_effect=replace_after_hash,
                ),
                self.assertRaisesRegex(ContractError, "changed"),
            ):
                pin.reverify()
        finally:
            pin.close()
        self.assertTrue(replaced)

    def test_catalog_path_replacement_during_hash_is_rejected(self) -> None:
        path = self.root / "catalogue-snapshot.bin"
        path.write_bytes(b"exact-catalogue-snapshot")
        os.chmod(path, 0o400)
        expected = _fingerprint(path.lstat())
        real_hash = _sha256_fd
        replaced = False

        def replace_after_hash(descriptor: int) -> str:
            nonlocal replaced
            digest = real_hash(descriptor)
            if not replaced:
                replacement = path.with_name("catalogue-snapshot-replacement.bin")
                replacement.write_bytes(b"changed-catalogue-snapshot")
                os.chmod(replacement, 0o400)
                os.replace(replacement, path)
                replaced = True
            return digest

        with (
            patch(
                "evaluation.review_workspace._sha256_fd",
                side_effect=replace_after_hash,
            ),
            self.assertRaisesRegex(ContractError, "changed while being hashed"),
        ):
            _hash_path(path, expected_fingerprint=expected)
        self.assertTrue(replaced)

    def test_media_path_replacement_during_final_hash_blocks_review_publish(self) -> None:
        revision = self.workspace.draft["revision"]
        target = self.workspace.media[0]
        real_hash = _sha256_fd
        target_hash_calls = 0
        replaced = False

        def replace_during_final_hash(descriptor: int) -> str:
            nonlocal target_hash_calls, replaced
            digest = real_hash(descriptor)
            if descriptor == target.fd:
                target_hash_calls += 1
                if target_hash_calls == 4:
                    replacement = target.path.with_name("replacement-payload")
                    replacement.write_bytes(b"X" * target.byte_count)
                    os.chmod(replacement, 0o400)
                    os.replace(replacement, target.path)
                    replaced = True
            return digest

        with (
            patch("evaluation.review_workspace._utc_now", return_value=FINALIZED_AT),
            self.completed_validator(),
            patch(
                "evaluation.review_workspace._sha256_fd",
                side_effect=replace_during_final_hash,
            ),
            self.assertRaisesRegex(ContractError, "changed while being hashed"),
        ):
            self.workspace.finalize(revision, finalization_payload(revision))
        self.assertEqual(target_hash_calls, 4)
        self.assertTrue(replaced)
        self.assertEqual(self.workspace.draft["lifecycle"], "invalidated")
        self.assertFalse(self.workspace.paths.completed_review.exists())

    def test_workspace_closing_identity_sweep_catches_early_media_replacement(self) -> None:
        revision = self.workspace.draft["revision"]
        target = self.workspace.media[0]
        real_assert = target.assert_unchanged
        target_rehash_calls = 0
        replaced = False

        def replace_after_item_check(*, rehash: bool) -> None:
            nonlocal target_rehash_calls, replaced
            real_assert(rehash=rehash)
            if rehash:
                target_rehash_calls += 1
                if target_rehash_calls == 4:
                    replacement = target.path.with_name("closing-sweep-replacement")
                    replacement.write_bytes(b"Y" * target.byte_count)
                    os.chmod(replacement, 0o400)
                    os.replace(replacement, target.path)
                    replaced = True

        with (
            patch("evaluation.review_workspace._utc_now", return_value=FINALIZED_AT),
            self.completed_validator(),
            patch.object(
                target,
                "assert_unchanged",
                side_effect=replace_after_item_check,
            ),
            self.assertRaisesRegex(ContractError, "changed after initialization"),
        ):
            self.workspace.finalize(revision, finalization_payload(revision))
        self.assertEqual(target_rehash_calls, 4)
        self.assertTrue(replaced)
        self.assertEqual(self.workspace.draft["lifecycle"], "invalidated")
        self.assertFalse(self.workspace.paths.completed_review.exists())

    def test_same_uid_change_after_final_publish_is_a_documented_guard_limit(self) -> None:
        revision = self.workspace.draft["revision"]
        real_write = _atomic_write_bytes
        mutated = False

        def mutate_after_canonical_review_publish(
            path: Path,
            body: bytes,
            *,
            mode: int,
            replace: bool,
        ) -> None:
            nonlocal mutated
            real_write(path, body, mode=mode, replace=replace)
            if path == self.workspace.paths.completed_review and not mutated:
                mutated = True
                self.mutate_first_media()

        with (
            patch("evaluation.review_workspace._utc_now", return_value=FINALIZED_AT),
            self.completed_validator(),
            patch(
                "evaluation.review_workspace._atomic_write_bytes",
                side_effect=mutate_after_canonical_review_publish,
            ),
        ):
            self.workspace.finalize(revision, finalization_payload(revision))
        self.assertTrue(mutated)
        self.assertEqual(self.workspace.draft["lifecycle"], "finalized")
        # POSIX mode 0400 cannot stop the owning UID from chmod+write after the
        # last check.  The workspace makes no immutability claim and fails the
        # next finalized access instead of returning a completed bootstrap.
        with self.assertRaisesRegex(ContractError, "parent media changed"):
            self.workspace.bootstrap(prefix="/private/", csrf_token="csrf")

    def test_canonical_replacement_before_final_mark_is_rejected(self) -> None:
        revision = self.workspace.draft["revision"]
        real_write = _atomic_write_bytes
        replaced = False

        def replace_review_after_publish(
            path: Path,
            body: bytes,
            *,
            mode: int,
            replace: bool,
        ) -> None:
            nonlocal replaced
            real_write(path, body, mode=mode, replace=replace)
            if path == self.workspace.paths.completed_review and not replaced:
                replaced = True
                os.chmod(path, 0o600)
                path.write_bytes(b"{}\n")
                os.chmod(path, 0o400)

        with (
            patch("evaluation.review_workspace._utc_now", return_value=FINALIZED_AT),
            self.completed_validator(),
            patch(
                "evaluation.review_workspace._atomic_write_bytes",
                side_effect=replace_review_after_publish,
            ),
            self.assertRaisesRegex(ContractError, "changed during finalization publish"),
        ):
            self.workspace.finalize(revision, finalization_payload(revision))
        self.assertTrue(replaced)
        self.assertEqual(self.workspace.draft["lifecycle"], "finalizing")
        persisted = load_selection_draft(
            self.workspace.paths.draft,
            self.template,
            WORKSPACE_ID,
            WORKSPACE_MANIFEST_SHA256,
        )
        self.assertEqual(persisted["lifecycle"], "finalizing")

    def test_finalized_finalize_is_idempotent_for_the_persisted_reviewer(self) -> None:
        revision = self.workspace.draft["revision"]
        with (
            patch("evaluation.review_workspace._utc_now", return_value=FINALIZED_AT),
            self.completed_validator(),
        ):
            self.workspace.finalize(revision, finalization_payload(revision))
        finalized = copy.deepcopy(self.workspace.draft)
        review_bytes = self.workspace.paths.completed_review.read_bytes()
        receipt_bytes = self.workspace.paths.receipt.read_bytes()
        final_revision = finalized["revision"]
        with self.completed_validator():
            self.workspace.finalize(
                final_revision,
                finalization_payload(final_revision),
            )
        self.assertEqual(self.workspace.draft, finalized)
        self.assertEqual(self.workspace.paths.completed_review.read_bytes(), review_bytes)
        self.assertEqual(self.workspace.paths.receipt.read_bytes(), receipt_bytes)

    def test_finalized_verification_requires_exact_receipt(self) -> None:
        revision = self.workspace.draft["revision"]
        with (
            patch("evaluation.review_workspace._utc_now", return_value=FINALIZED_AT),
            self.completed_validator(),
        ):
            self.workspace.finalize(revision, finalization_payload(revision))
        os.chmod(self.workspace.paths.receipt, 0o600)
        self.workspace.paths.receipt.write_text("{}\n", encoding="utf-8")
        os.chmod(self.workspace.paths.receipt, 0o400)
        with (
            self.completed_validator(),
            self.assertRaisesRegex(ContractError, "receipt"),
        ):
            self.workspace._verify_finalized_output()

    def test_finalized_verification_binds_reviewer_intent(self) -> None:
        revision = self.workspace.draft["revision"]
        with (
            patch("evaluation.review_workspace._utc_now", return_value=FINALIZED_AT),
            self.completed_validator(),
        ):
            self.workspace.finalize(revision, finalization_payload(revision))
        self.workspace.draft["finalization_intent"]["reviewer_id"] = "reviewer_tampered"
        with (
            self.completed_validator(),
            self.assertRaisesRegex(ContractError, "durable finalization intent"),
        ):
            self.workspace._verify_finalized_output()

    def test_finalized_verification_binds_exact_decisions_and_splits(self) -> None:
        revision = self.workspace.draft["revision"]
        with (
            patch("evaluation.review_workspace._utc_now", return_value=FINALIZED_AT),
            self.completed_validator(),
        ):
            self.workspace.finalize(revision, finalization_payload(revision))
        self.workspace.draft["recordings"][0]["split"] = "scoring"
        self.workspace.draft["manifest_sha256"] = canonical_manifest_sha256(
            self.workspace.draft
        )
        save_selection_draft(self.workspace.paths.draft, self.workspace.draft)
        with (
            self.completed_validator(),
            self.assertRaisesRegex(ContractError, "finalized draft decisions and splits"),
        ):
            self.workspace._verify_finalized_output()

    def test_finalized_bootstrap_revalidates_review_and_receipt(self) -> None:
        revision = self.workspace.draft["revision"]
        with (
            patch("evaluation.review_workspace._utc_now", return_value=FINALIZED_AT),
            self.completed_validator(),
        ):
            self.workspace.finalize(revision, finalization_payload(revision))
        review_body = self.workspace.paths.completed_review.read_bytes()
        self.workspace.paths.completed_review.unlink()
        with (
            self.completed_validator(),
            self.assertRaisesRegex(ContractError, "completed_review"),
        ):
            self.workspace.bootstrap(prefix="/private/", csrf_token="csrf")
        _atomic_write_bytes(
            self.workspace.paths.completed_review,
            review_body,
            mode=0o400,
            replace=False,
        )
        self.workspace.paths.receipt.unlink()
        with (
            self.completed_validator(),
            self.assertRaisesRegex(ContractError, "receipt"),
        ):
            self.workspace.bootstrap(prefix="/private/", csrf_token="csrf")

    def test_finalized_bootstrap_closes_the_input_validation_race(self) -> None:
        revision = self.workspace.draft["revision"]
        with (
            patch("evaluation.review_workspace._utc_now", return_value=FINALIZED_AT),
            self.completed_validator(),
        ):
            self.workspace.finalize(revision, finalization_payload(revision))

        def mutate_inside_validator(review: dict, *args, **kwargs) -> dict:
            self.mutate_first_media()
            return review

        with (
            patch(
                "evaluation.review_workspace.validate_interval_selection_review",
                side_effect=mutate_inside_validator,
            ),
            self.assertRaisesRegex(ContractError, "parent media changed"),
        ):
            self.workspace.bootstrap(prefix="/private/", csrf_token="csrf")

    def test_finalized_bootstrap_reloads_artifacts_after_closing_media_rehash(self) -> None:
        revision = self.workspace.draft["revision"]
        with (
            patch("evaluation.review_workspace._utc_now", return_value=FINALIZED_AT),
            self.completed_validator(),
        ):
            self.workspace.finalize(revision, finalization_payload(revision))

        real_reverify = self.workspace.reverify_exact_inputs
        calls = 0

        def replace_review_during_closing_rehash() -> None:
            nonlocal calls
            calls += 1
            real_reverify()
            if calls == 2:
                os.chmod(self.workspace.paths.completed_review, 0o600)
                self.workspace.paths.completed_review.write_bytes(b"{}\n")
                os.chmod(self.workspace.paths.completed_review, 0o400)

        with (
            self.completed_validator(),
            patch.object(
                self.workspace,
                "reverify_exact_inputs",
                side_effect=replace_review_during_closing_rehash,
            ),
            self.assertRaisesRegex(
                ContractError, "changed during finalized verification"
            ),
        ):
            self.workspace.bootstrap(prefix="/private/", csrf_token="csrf")
        self.assertEqual(calls, 2)

    def test_pinned_ui_assets_survive_mutation_and_reject_symlinks(self) -> None:
        ui_root = self.root / "review-ui"
        ui_root.mkdir(mode=0o700)
        original: dict[str, bytes] = {}
        for name in ("index.html", "app.js", "styles.css"):
            body = f"pinned-{name}".encode("ascii")
            original[name] = body
            path = ui_root / name
            path.write_bytes(body)
            os.chmod(path, 0o644)
        pinned = _pin_ui_assets(ui_root)
        (ui_root / "app.js").write_bytes(b"mutated-script")
        self.assertEqual(pinned["app.js"]["body"], original["app.js"])
        self.assertEqual(
            pinned["app.js"]["sha256"],
            hashlib.sha256(original["app.js"]).hexdigest(),
        )

        (ui_root / "styles.css").unlink()
        (ui_root / "styles.css").symlink_to(ui_root / "app.js")
        with self.assertRaisesRegex(ContractError, "non-symlink regular file"):
            _pin_ui_assets(ui_root)


if __name__ == "__main__":
    unittest.main()
