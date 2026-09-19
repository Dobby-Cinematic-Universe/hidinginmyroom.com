from __future__ import annotations

import hashlib
import json
import os
import tempfile
import unittest
from pathlib import Path
from unittest import mock

from himr_corpus.db import connect, migrate
from himr_corpus.ids import stable_id
from himr_corpus.machine_transcript_publication import DISCLAIMER_CODE, DISCLAIMER_TEXT
from himr_corpus.reviewed_release_staging import (
    ReviewedReleaseStagingError,
    stage_reviewed_release,
)
from himr_corpus.sharded_release import validate_sharded_release


def _sha256(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


class ReviewedReleaseStagingTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temporary = tempfile.TemporaryDirectory(
            prefix="reviewed-release-staging-test-"
        )
        self.addCleanup(self.temporary.cleanup)
        self.root = Path(self.temporary.name)
        self.source_id = "src_" + "1" * 32
        self.recording_id = "rec_" + "2" * 32
        self.revision_id = "transcript_revision_" + "3" * 32
        self.base = self.root / "sealed-base.sqlite3"
        self._create_base()
        self.evidence = self.root / "evidence.json"
        self.reviewer = self.root / "reviewer.json"
        self.publication = self.root / "publication.json"
        self._write_inputs()
        self.work = self.root / "private-work"
        self.work.mkdir(mode=0o700)

    def _create_base(self) -> None:
        connection = connect(self.base)
        try:
            migrate(connection)
            connection.execute(
                """
                INSERT INTO sources(
                    source_id, platform, source_kind, native_id, canonical_url,
                    title, observed_at, access_state, review_state, metadata_json,
                    created_at, updated_at
                ) VALUES(?, 'reddit', 'reddit_video', 'fixture123',
                         'https://v.redd.it/fixture123', 'Community title',
                         '2026-01-01T00:00:00Z', 'public', 'unreviewed', '{}',
                         '2026-01-01T00:00:00Z', '2026-01-01T00:00:00Z')
                """,
                (self.source_id,),
            )
            connection.execute(
                """
                INSERT INTO recordings(
                    recording_id, canonical_key, slug, title, date_label,
                    date_year, date_basis, duration_ms, recording_type,
                    review_state, metadata_json, created_at, updated_at
                ) VALUES(?, 'reddit:video:fixture123', 'reddit-fixture123',
                         'Community title', '2026-01-01', 2026,
                         'reddit_atom_entry_contextual', NULL, 'video',
                         'unreviewed', '{}', '2026-01-01T00:00:00Z',
                         '2026-01-01T00:00:00Z')
                """,
                (self.recording_id,),
            )
            connection.execute(
                """
                INSERT INTO recording_sources(
                    recording_source_id, recording_id, source_id, mapping_role,
                    mapping_method, confidence_state, metadata_json
                ) VALUES(?, ?, ?, 'reddit_video_discovery_candidate',
                         'stable_v_reddit_media_id_from_public_atom_locator',
                         'candidate', '{}')
                """,
                (
                    stable_id("rso", self.recording_id, self.source_id),
                    self.recording_id,
                    self.source_id,
                ),
            )
            connection.execute(
                """
                INSERT INTO transcript_revisions(
                    revision_id, recording_id, revision_kind, origin, language,
                    review_state, created_at, metadata_json
                ) VALUES(?, ?, 'raw_asr', 'machine fixture', 'en', 'machine',
                         '2026-01-01T00:00:01Z', '{}')
                """,
                (self.revision_id, self.recording_id),
            )
            for ordinal in range(3):
                connection.execute(
                    """
                    INSERT INTO transcript_segments(
                        segment_id, revision_id, ordinal, start_ms, end_ms,
                        text, normalized_text, speaker_label, language,
                        confidence_band, calibrated_probability, metadata_json
                    ) VALUES(?, ?, ?, ?, ?, ?, ?, NULL, 'en', NULL, NULL, '{}')
                    """,
                    (
                        stable_id("seg", self.revision_id, ordinal),
                        self.revision_id,
                        ordinal,
                        ordinal * 1000,
                        (ordinal + 1) * 1000,
                        f"machine text {ordinal}",
                        f"machine text {ordinal}",
                    ),
                )
            connection.execute("PRAGMA wal_checkpoint(TRUNCATE)")
            connection.execute("PRAGMA journal_mode = DELETE")
        finally:
            connection.close()
        self.base.chmod(0o400)

    def _write_json(self, path: Path, value: object) -> None:
        path.write_text(
            json.dumps(value, ensure_ascii=False, indent=2) + "\n", encoding="utf-8"
        )
        path.chmod(0o600)

    def _publication_manifest(self) -> dict[str, object]:
        reviewer_id = "reviewer_release_fixture_human"
        publications = [
            {
                "publication_decision_id": f"publish-fixture-{object_type}",
                "object_type": object_type,
                "object_id": object_id,
                "decision": "publish",
                "reviewer_id": reviewer_id,
                "decided_at": "2026-01-02T00:00:00Z",
                "basis": f"Human {object_type} publication decision.",
                "note": f"Private {object_type} review note.",
            }
            for object_type, object_id in (
                ("source", self.source_id),
                ("recording", self.recording_id),
            )
        ]
        gates = [
            {
                "publication_gate_decision_id": (
                    f"clear-fixture-{object_type}-{gate_kind}"
                ),
                "object_type": object_type,
                "object_id": object_id,
                "gate_kind": gate_kind,
                "decision": "clear",
                "reviewer_id": reviewer_id,
                "decided_at": "2026-01-02T00:00:00Z",
                "basis": f"Human {gate_kind} finding for {object_type}.",
                "note": f"Private {gate_kind} note for {object_type}.",
            }
            for object_type, object_id in (
                ("source", self.source_id),
                ("recording", self.recording_id),
                ("transcript_revision", self.revision_id),
            )
            for gate_kind in ("rights", "privacy", "sensitivity")
        ]
        return {
            "schema_version": 1,
            "manifest_id": "publication-reviewed-release-fixture",
            "publication_decisions": publications,
            "gate_decisions": gates,
        }

    def _write_inputs(self) -> None:
        evidence = {
            "schema_version": 1,
            "packet_kind": "first_public_machine_transcript_review_evidence",
            "source": {"source_id": self.source_id},
            "recording": {"recording_id": self.recording_id},
            "transcript_revision": {
                "revision_id": self.revision_id,
                "speaker_labels": {"named_segment_count": 0},
                "segments": [
                    {"ordinal": ordinal, "start_ms": ordinal * 1000,
                     "end_ms": (ordinal + 1) * 1000, "text": f"machine text {ordinal}"}
                    for ordinal in range(3)
                ],
                "required_public_warning": DISCLAIMER_TEXT,
            },
            "release_expectation_after_valid_decisions": {
                "sources": 1,
                "recordings": 1,
                "transcript_revisions": 1,
                "segments": 3,
                "machine_generated": True,
                "unreviewed": True,
                "verified_quotation": False,
                "disclaimer_code": DISCLAIMER_CODE,
            },
        }
        reviewer = {
            "schema_version": 1,
            "manifest_id": "reviewer-admin-reviewed-release-fixture",
            "created_at": "2026-01-01T00:00:03Z",
            "authorized_by": "test_fixture_operator",
            "basis": "Authorize the one disposable human reviewer.",
            "registrations": [
                {
                    "reviewer_registration_id": "register-release-fixture-human",
                    "reviewer_id": "reviewer_release_fixture_human",
                    "display_label": "Release Fixture Human",
                    "reviewer_kind": "human",
                    "registered_at": "2026-01-01T00:00:01Z",
                    "basis": "Register the disposable human reviewer inactive.",
                }
            ],
            "state_changes": [
                {
                    "reviewer_state_change_id": "activate-release-fixture-human",
                    "reviewer_id": "reviewer_release_fixture_human",
                    "active": True,
                    "changed_at": "2026-01-01T00:00:02Z",
                    "basis": "Explicitly activate the disposable human reviewer.",
                }
            ],
        }
        self._write_json(self.evidence, evidence)
        self._write_json(self.reviewer, reviewer)
        self._write_json(self.publication, self._publication_manifest())

    def _plan(self) -> dict[str, object]:
        return stage_reviewed_release(
            mode="plan",
            base_catalog=self.base,
            expected_base_sha256=_sha256(self.base),
            evidence_path=self.evidence,
            expected_evidence_sha256=_sha256(self.evidence),
            reviewer_manifest_path=self.reviewer,
            publication_manifest_path=self.publication,
            work_root=self.work,
        )

    def test_plan_and_build_never_open_base_and_emit_private_candidate(self) -> None:
        base_sha = _sha256(self.base)
        real_connect = connect

        def guarded_connect(path: str | Path):
            self.assertNotEqual(Path(path).resolve(), self.base.resolve())
            return real_connect(path)

        with mock.patch(
            "himr_corpus.reviewed_release_staging.connect", side_effect=guarded_connect
        ):
            plan = self._plan()
            build = stage_reviewed_release(
                mode="build",
                base_catalog=self.base,
                expected_base_sha256=base_sha,
                evidence_path=self.evidence,
                expected_evidence_sha256=_sha256(self.evidence),
                reviewer_manifest_path=self.reviewer,
                publication_manifest_path=self.publication,
                work_root=self.work,
                expected_machine_plan_sha256=plan["machine_plan_sha256"],
                expected_reviewer_manifest_sha256=plan[
                    "reviewer_manifest_sha256"
                ],
                expected_publication_manifest_sha256=plan[
                    "publication_manifest_sha256"
                ],
            )

        self.assertEqual(_sha256(self.base), base_sha)
        self.assertFalse(Path(f"{self.base}-wal").exists())
        self.assertFalse(Path(f"{self.base}-shm").exists())
        self.assertEqual(plan["state"], "plan_only")
        self.assertIsNone(plan["candidate_release_manifest"])
        self.assertEqual(build["state"], "candidate_not_published")
        manifest = Path(build["candidate_release_manifest"])
        validated = validate_sharded_release(manifest)
        self.assertEqual(validated["recordings"], 1)
        self.assertEqual(validated["transcript_revisions"], 1)
        self.assertEqual(validated["segments"], 3)
        receipt = json.loads(Path(build["receipt_path"]).read_text(encoding="utf-8"))
        self.assertFalse(receipt["live_catalog_opened"])
        self.assertFalse(receipt["live_catalog_mutated"])
        self.assertFalse(receipt["public_tree_written"])
        self.assertFalse(receipt["wording_review_performed"])
        self.assertFalse(receipt["identity_claimed"])
        self.assertEqual(receipt["mandatory_disclaimer"], DISCLAIMER_TEXT)
        self.assertEqual(receipt["release_result"]["recordings"], 1)
        self.assertEqual(os.stat(manifest).st_mode & 0o777, 0o400)

    def test_template_placeholder_fails_before_staging(self) -> None:
        raw = json.loads(self.reviewer.read_text(encoding="utf-8"))
        raw["basis"] = "REPLACE_WITH_ACTUAL_AUTHORITY"
        self._write_json(self.reviewer, raw)
        with self.assertRaisesRegex(ReviewedReleaseStagingError, "template marker"):
            self._plan()
        self.assertEqual(list(self.work.iterdir()), [])

    def test_missing_one_independent_gate_fails_before_staging(self) -> None:
        raw = json.loads(self.publication.read_text(encoding="utf-8"))
        raw["gate_decisions"] = raw["gate_decisions"][:-1]
        self._write_json(self.publication, raw)
        with self.assertRaisesRegex(ReviewedReleaseStagingError, "exactly nine"):
            self._plan()
        self.assertEqual(list(self.work.iterdir()), [])

    def test_build_requires_the_reviewed_manifest_digests(self) -> None:
        plan = self._plan()
        with self.assertRaisesRegex(
            ReviewedReleaseStagingError, "reviewer manifest changed"
        ):
            stage_reviewed_release(
                mode="build",
                base_catalog=self.base,
                expected_base_sha256=_sha256(self.base),
                evidence_path=self.evidence,
                expected_evidence_sha256=_sha256(self.evidence),
                reviewer_manifest_path=self.reviewer,
                publication_manifest_path=self.publication,
                work_root=self.work,
                expected_machine_plan_sha256=plan["machine_plan_sha256"],
                expected_reviewer_manifest_sha256="0" * 64,
                expected_publication_manifest_sha256=plan[
                    "publication_manifest_sha256"
                ],
            )

    def test_writable_or_sidecar_base_is_rejected(self) -> None:
        self.base.chmod(0o600)
        with self.assertRaisesRegex(ReviewedReleaseStagingError, "sealed read-only"):
            self._plan()
        self.base.chmod(0o400)
        sidecar = Path(f"{self.base}-wal")
        sidecar.touch()
        with self.assertRaisesRegex(ReviewedReleaseStagingError, "no SQLite sidecars"):
            self._plan()


if __name__ == "__main__":
    unittest.main()
