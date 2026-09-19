from __future__ import annotations

import hashlib
import io
import json
import shutil
import sqlite3
import tempfile
import unittest
from contextlib import redirect_stdout
from datetime import datetime, timezone
from pathlib import Path
from unittest import mock


CORPUS_ROOT = Path(__file__).resolve().parents[1]

from himr_corpus import cli  # noqa: E402
from himr_corpus.db import connect, migrate, transaction, utc_now  # noqa: E402
from himr_corpus.exporter import build_release  # noqa: E402
from himr_corpus.ids import source_id, stable_id  # noqa: E402
from himr_corpus.machine_transcript_publication import (  # noqa: E402
    DISCLAIMER_CODE,
    DISCLAIMER_TEXT,
    MACHINE_TRANSCRIPT_POLICY_REVIEWER_ID,
    MachineTranscriptPublicationPolicyError,
    POLICY_BASIS,
    POLICY_ID,
    PUBLIC_LABEL,
    apply_machine_transcript_publication_plan,
    build_machine_transcript_publication_plan,
    validate_machine_transcript_publication_policy,
)
from himr_corpus.publication_admin import (  # noqa: E402
    PublicationManifestError,
    load_publication_manifest,
)
from himr_corpus.reviewer_admin import (  # noqa: E402
    MACHINE_TRANSCRIPT_POLICY_DISPLAY_LABEL,
    ReviewerAdminManifestError,
    ensure_machine_transcript_policy_reviewer,
)
from himr_corpus.validation import validate_database  # noqa: E402
from corpus.tests.reviewer_fixtures import register_reviewer_fixture  # noqa: E402


class MachineTranscriptPublicationPolicyTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temporary = tempfile.TemporaryDirectory(
            prefix="machine-transcript-policy-test-"
        )
        self.addCleanup(self.temporary.cleanup)
        self.database = Path(self.temporary.name) / "corpus.sqlite3"
        self.connection = connect(self.database)
        self.addCleanup(self.connection.close)
        migrate(self.connection)
        self.human_reviewer = "reviewer_test_human_transcript_policy"
        register_reviewer_fixture(
            self.connection,
            self.human_reviewer,
            "Test Human Transcript Policy Reviewer",
            "human",
        )

    def _add_revision(
        self,
        label: str,
        *,
        segment_count: int = 2,
        speaker_label: str | None = None,
        revision_kind: str = "raw_asr",
        review_state: str = "machine",
    ) -> tuple[str, str]:
        recording_id = stable_id("rec", "machine-transcript-policy", label)
        revision_id = stable_id("trv", "machine-transcript-policy", label)
        self.connection.execute(
            """
            INSERT INTO recordings(
                recording_id, canonical_key, slug, title, date_label, date_year,
                date_basis, duration_ms, recording_type, review_state,
                metadata_json, created_at, updated_at
            ) VALUES(?, ?, ?, ?, '2026-01-01', 2026, 'platform_timestamp',
                     60000, 'video', 'unreviewed', '{}',
                     '2026-01-01T00:00:00Z', '2026-01-01T00:00:00Z')
            """,
            (
                recording_id,
                f"test:machine-transcript-policy:{label}",
                f"machine-transcript-policy-{label}",
                f"Machine transcript policy {label}",
            ),
        )
        self.connection.execute(
            """
            INSERT INTO transcript_revisions(
                revision_id, recording_id, revision_kind, origin, language,
                review_state, created_at, metadata_json
            ) VALUES(?, ?, ?, 'test machine ASR', 'en', ?,
                     '2026-01-01T00:00:01Z', '{}')
            """,
            (revision_id, recording_id, revision_kind, review_state),
        )
        self.connection.executemany(
            """
            INSERT INTO transcript_segments(
                segment_id, revision_id, ordinal, start_ms, end_ms, text,
                normalized_text, speaker_label, language, confidence_band,
                metadata_json
            ) VALUES(?, ?, ?, ?, ?, ?, ?, ?, 'en', 'medium', '{}')
            """,
            [
                (
                    stable_id("seg", revision_id, ordinal),
                    revision_id,
                    ordinal,
                    ordinal * 1000,
                    (ordinal + 1) * 1000,
                    f"PRIVATE WORDING SENTINEL {label} {ordinal}",
                    f"private wording sentinel {label} {ordinal}",
                    speaker_label,
                )
                for ordinal in range(segment_count)
            ],
        )
        return recording_id, revision_id

    def _add_gates(
        self,
        revision_id: str,
        *,
        gate_kinds: tuple[str, ...] = ("rights", "privacy", "sensitivity"),
        decision: str = "clear",
        decided_at: str = "2026-01-02T00:00:00Z",
    ) -> None:
        for gate_kind in gate_kinds:
            self.connection.execute(
                """
                INSERT INTO publication_gate_decisions(
                    publication_gate_decision_id, object_type, object_id,
                    gate_kind, decision, reviewer_id, review_decision_id,
                    decided_at, basis, notes, manifest_id
                ) VALUES(?, 'transcript_revision', ?, ?, ?, ?, NULL, ?,
                         'Independent human gate decision.', NULL, NULL)
                """,
                (
                    stable_id(
                        "pgt", revision_id, gate_kind, decision, decided_at
                    ),
                    revision_id,
                    gate_kind,
                    decision,
                    self.human_reviewer,
                    decided_at,
                ),
            )

    def _human_publication(
        self,
        revision_id: str,
        decision: str,
        *,
        decided_at: str = "2026-01-06T00:00:00Z",
    ) -> None:
        self.connection.execute(
            """
            INSERT INTO publication_decisions(
                publication_decision_id, object_type, object_id, decision,
                reviewer_id, review_decision_id, decided_at, basis, notes,
                public_label, manifest_id
            ) VALUES(?, 'transcript_revision', ?, ?, ?, NULL, ?,
                     'Explicit human publication state.', NULL, NULL, NULL)
            """,
            (
                stable_id("pub", revision_id, decision, decided_at),
                revision_id,
                decision,
                self.human_reviewer,
                decided_at,
            ),
        )

    def _human_retraction(self, revision_id: str) -> None:
        task_id = stable_id("rtask", revision_id, "retract")
        review_id = stable_id("rdec", revision_id, "retract")
        self.connection.execute(
            """
            INSERT INTO review_tasks(
                review_task_id, task_kind, target_type, target_id, reason,
                priority, status, created_at, updated_at
            ) VALUES(?, 'transcript_lifecycle', 'transcript_revision', ?,
                     'Test human retraction.', 100, 'completed',
                     '2026-01-03T00:00:00Z', '2026-01-03T00:00:00Z')
            """,
            (task_id, revision_id),
        )
        self.connection.execute(
            """
            INSERT INTO review_decisions(
                review_decision_id, review_task_id, target_type, target_id,
                reviewer_id, decision, decided_at, reviewed_complete_item, basis
            ) VALUES(?, ?, 'transcript_revision', ?, ?, 'reject',
                     '2026-01-03T00:00:01Z', 1, 'Human retraction review.')
            """,
            (review_id, task_id, revision_id, self.human_reviewer),
        )
        self.connection.execute(
            """
            INSERT INTO transcript_lifecycle_decisions(
                transcript_lifecycle_decision_id, revision_id, lifecycle_state,
                reason_code, reviewer_id, review_decision_id, decided_at, basis,
                public_explanation, notes
            ) VALUES(?, ?, 'retracted', 'editorial_decision', ?, ?,
                     '2026-01-04T00:00:00Z', 'Human retraction decision.',
                     'Retracted by a human reviewer.', NULL)
            """,
            (
                stable_id("tlc", revision_id, "retracted"),
                revision_id,
                self.human_reviewer,
                review_id,
            ),
        )

    def _apply_one_baseline(self) -> tuple[dict, str]:
        _, revision_id = self._add_revision("baseline")
        self._add_gates(revision_id)
        plan = build_machine_transcript_publication_plan(self.connection)
        result = apply_machine_transcript_publication_plan(
            self.connection, expected_plan_sha256=plan["plan_sha256"]
        )
        return result, revision_id

    def test_plan_is_text_free_exact_and_counts_distinct_segments(self) -> None:
        _, revision_id = self._add_revision("multi-segment", segment_count=4)
        self._add_gates(revision_id)
        before = {
            table: self.connection.execute(f"SELECT count(*) FROM {table}").fetchone()[0]
            for table in (
                "reviewers",
                "publication_decisions",
                "publication_gate_decisions",
                "publication_manifest_imports",
                "machine_transcript_publication_policy_runs",
            )
        }

        plan = build_machine_transcript_publication_plan(self.connection)

        self.assertEqual(plan["eligible_revision_count"], 1)
        self.assertEqual(plan["revisions"][0]["revision_id"], revision_id)
        self.assertEqual(plan["revisions"][0]["segment_count"], 4)
        self.assertEqual(
            set(plan["revisions"][0]["gates"]),
            {"rights", "privacy", "sensitivity"},
        )
        self.assertEqual(plan["disclaimer_code"], DISCLAIMER_CODE)
        self.assertEqual(plan["disclaimer"], DISCLAIMER_TEXT)
        encoded = json.dumps(plan, sort_keys=True)
        self.assertNotIn("PRIVATE WORDING SENTINEL", encoded)
        self.assertNotIn("normalized_text", encoded)
        after = {
            table: self.connection.execute(f"SELECT count(*) FROM {table}").fetchone()[0]
            for table in before
        }
        self.assertEqual(after, before)

    def test_apply_is_exact_initial_publish_and_replay_is_noop(self) -> None:
        _, revision_id = self._add_revision("apply-replay", segment_count=3)
        self._add_gates(revision_id)
        _, second_revision_id = self._add_revision(
            "apply-replay-second", segment_count=2, revision_kind="contextual_asr"
        )
        self._add_gates(second_revision_id)
        plan = build_machine_transcript_publication_plan(self.connection)

        result = apply_machine_transcript_publication_plan(
            self.connection, expected_plan_sha256=plan["plan_sha256"]
        )

        self.assertEqual(result["decisions_inserted"], 2)
        self.assertFalse(result["already_applied"])
        self.assertEqual(result["gate_decisions_inserted"], 0)
        self.assertEqual(result["wording_reviews_inserted"], 0)
        self.assertEqual(result["lifecycle_decisions_inserted"], 0)
        decision = self.connection.execute(
            """
            SELECT decision.*, run.applied_at, manifest.imported_at
            FROM publication_decisions AS decision
            JOIN machine_transcript_publication_policy_runs AS run
              ON run.manifest_id = decision.manifest_id
            JOIN publication_manifest_imports AS manifest
              ON manifest.manifest_id = run.manifest_id
            WHERE decision.object_id = ?
            """,
            (revision_id,),
        ).fetchone()
        self.assertEqual(decision["decision"], "publish")
        self.assertIsNone(decision["review_decision_id"])
        self.assertEqual(decision["notes"], DISCLAIMER_TEXT)
        self.assertEqual(decision["decided_at"], decision["applied_at"])
        self.assertEqual(decision["decided_at"], decision["imported_at"])
        self.assertEqual(
            self.connection.execute(
                "SELECT count(*) FROM machine_transcript_policy_publish_scope"
            ).fetchone()[0],
            0,
        )
        before_counts = {
            table: self.connection.execute(f"SELECT count(*) FROM {table}").fetchone()[0]
            for table in (
                "reviewers",
                "reviewer_admin_events",
                "publication_manifest_imports",
                "publication_decisions",
                "publication_gate_decisions",
                "review_decisions",
                "transcript_lifecycle_decisions",
                "machine_transcript_publication_policy_runs",
            )
        }

        replay = apply_machine_transcript_publication_plan(
            self.connection, expected_plan_sha256=plan["plan_sha256"]
        )

        self.assertTrue(replay["already_applied"])
        self.assertEqual(replay["decisions_inserted"], 0)
        self.assertEqual(replay["existing_policy_decisions"], 2)
        self.assertEqual(
            {
                table: self.connection.execute(
                    f"SELECT count(*) FROM {table}"
                ).fetchone()[0]
                for table in before_counts
            },
            before_counts,
        )
        validate_machine_transcript_publication_policy(self.connection)
        validate_database(self.connection)

    def test_digest_mismatch_is_atomic_and_creates_no_policy_authority(self) -> None:
        _, revision_id = self._add_revision("digest-mismatch")
        self._add_gates(revision_id)
        with self.assertRaisesRegex(
            MachineTranscriptPublicationPolicyError, "plan changed"
        ):
            apply_machine_transcript_publication_plan(
                self.connection, expected_plan_sha256="0" * 64
            )
        self.assertIsNone(
            self.connection.execute(
                "SELECT 1 FROM reviewers WHERE reviewer_id = ?",
                (MACHINE_TRANSCRIPT_POLICY_REVIEWER_ID,),
            ).fetchone()
        )
        for table in (
            "publication_decisions",
            "publication_manifest_imports",
            "machine_transcript_publication_policy_runs",
        ):
            self.assertEqual(
                self.connection.execute(f"SELECT count(*) FROM {table}").fetchone()[0],
                0,
            )

    def test_scope_requires_all_current_human_clears_and_no_lifecycle(self) -> None:
        _, missing = self._add_revision("missing-gate")
        self._add_gates(missing, gate_kinds=("rights", "privacy"))
        _, withheld = self._add_revision("withheld-gate")
        self._add_gates(withheld)
        self._add_gates(
            withheld,
            gate_kinds=("privacy",),
            decision="withhold",
            decided_at="2026-01-03T00:00:00Z",
        )
        _, lifecycle = self._add_revision("lifecycle-history")
        self._add_gates(lifecycle)
        self._human_retraction(lifecycle)
        _, premature = self._add_revision("gate-before-revision")
        self._add_gates(premature, decided_at="2025-12-31T00:00:00Z")

        plan = build_machine_transcript_publication_plan(self.connection)

        self.assertEqual(plan["eligible_revision_count"], 0)
        self.assertEqual(plan["revisions"], [])

    def test_database_rejects_a_canonical_backdated_run_atomically(self) -> None:
        # A reserved reviewer active since the fixture epoch isolates the temporal
        # run guard from the reviewer-activation guard.
        with mock.patch("himr_corpus.reviewer_admin.datetime") as mocked_datetime:
            mocked_datetime.now.return_value = datetime(
                2025, 1, 1, tzinfo=timezone.utc
            )
            with transaction(self.connection):
                ensure_machine_transcript_policy_reviewer(self.connection)
        _, revision_id = self._add_revision("backdated-run")
        self._add_gates(revision_id)
        plan = build_machine_transcript_publication_plan(self.connection)
        payload = {key: value for key, value in plan.items() if key != "plan_sha256"}
        plan_json = json.dumps(
            payload, ensure_ascii=False, separators=(",", ":"), sort_keys=True
        )
        self.assertEqual(
            hashlib.sha256(plan_json.encode("utf-8")).hexdigest(),
            plan["plan_sha256"],
        )
        manifest_id = f"machine-transcript-default-policy-v1:{plan['plan_sha256']}"
        with self.assertRaisesRegex(
            sqlite3.IntegrityError, "outside its exact initial-publication scope"
        ):
            with transaction(self.connection):
                self.connection.execute(
                    """
                    INSERT INTO machine_transcript_publication_policy_runs(
                        plan_sha256, policy_id, manifest_id, reviewer_id, plan_json,
                        eligible_revision_count, new_decision_count,
                        existing_policy_count, protected_existing_count, applied_at
                    ) VALUES(?, ?, ?, ?, ?, 1, 1, 0, 0,
                             '2025-12-31T00:00:00Z')
                    """,
                    (
                        plan["plan_sha256"],
                        POLICY_ID,
                        manifest_id,
                        MACHINE_TRANSCRIPT_POLICY_REVIEWER_ID,
                        plan_json,
                    ),
                )
                self.connection.execute(
                    """
                    INSERT INTO publication_manifest_imports(
                        manifest_id, input_sha256, schema_version,
                        publication_decision_count, gate_decision_count, imported_at
                    ) VALUES(?, ?, 1, 1, 0, '2025-12-31T00:00:00Z')
                    """,
                    (manifest_id, plan["plan_sha256"]),
                )
                self.connection.execute(
                    """
                    INSERT INTO publication_decisions(
                        publication_decision_id, object_type, object_id, decision,
                        reviewer_id, decided_at, basis, notes, public_label, manifest_id
                    ) VALUES('machine-transcript-default-v1:' || ?,
                             'transcript_revision', ?, 'publish', ?,
                             '2025-12-31T00:00:00Z', ?, ?, ?, ?)
                    """,
                    (
                        revision_id,
                        revision_id,
                        MACHINE_TRANSCRIPT_POLICY_REVIEWER_ID,
                        POLICY_BASIS,
                        DISCLAIMER_TEXT,
                        PUBLIC_LABEL,
                        manifest_id,
                    ),
                )
        self.assertEqual(
            self.connection.execute(
                "SELECT count(*) FROM publication_manifest_imports"
            ).fetchone()[0],
            0,
        )
        self.assertEqual(
            self.connection.execute(
                "SELECT count(*) FROM machine_transcript_publication_policy_runs"
            ).fetchone()[0],
            0,
        )
        self.assertEqual(
            self.connection.execute(
                "SELECT count(*) FROM publication_decisions"
            ).fetchone()[0],
            0,
        )

    def test_database_rejects_subset_and_text_bearing_policy_plans(self) -> None:
        with transaction(self.connection):
            ensure_machine_transcript_policy_reviewer(self.connection)
        for label in ("complete-scope-a", "complete-scope-b"):
            _, revision_id = self._add_revision(label)
            self._add_gates(revision_id)
        complete = build_machine_transcript_publication_plan(self.connection)
        self.assertEqual(complete["eligible_revision_count"], 2)
        base_payload = {
            key: value for key, value in complete.items() if key != "plan_sha256"
        }
        subset = json.loads(json.dumps(base_payload))
        subset["eligible_revision_count"] = 1
        subset["revisions"] = subset["revisions"][:1]
        text_bearing = json.loads(json.dumps(base_payload))
        text_bearing["revisions"][0]["text"] = "PRIVATE_SENTINEL"

        for label, payload in (("subset", subset), ("text-bearing", text_bearing)):
            plan_json = json.dumps(
                payload, ensure_ascii=False, separators=(",", ":"), sort_keys=True
            )
            digest = hashlib.sha256(plan_json.encode("utf-8")).hexdigest()
            applied_at = utc_now()
            manifest_id = f"machine-transcript-default-policy-v1:{digest}"
            with self.subTest(plan=label), self.assertRaisesRegex(
                sqlite3.IntegrityError,
                "outside its exact initial-publication scope",
            ):
                with transaction(self.connection):
                    self.connection.execute(
                        """
                        INSERT INTO machine_transcript_publication_policy_runs(
                            plan_sha256, policy_id, manifest_id, reviewer_id,
                            plan_json, eligible_revision_count, new_decision_count,
                            existing_policy_count, protected_existing_count,
                            applied_at
                        ) VALUES(?, ?, ?, ?, ?, ?, ?, 0, 0, ?)
                        """,
                        (
                            digest,
                            POLICY_ID,
                            manifest_id,
                            MACHINE_TRANSCRIPT_POLICY_REVIEWER_ID,
                            plan_json,
                            payload["eligible_revision_count"],
                            payload["eligible_revision_count"],
                            applied_at,
                        ),
                    )
                    self.connection.execute(
                        """
                        INSERT INTO publication_manifest_imports(
                            manifest_id, input_sha256, schema_version,
                            publication_decision_count, gate_decision_count,
                            imported_at
                        ) VALUES(?, ?, 1, ?, 0, ?)
                        """,
                        (
                            manifest_id,
                            digest,
                            payload["eligible_revision_count"],
                            applied_at,
                        ),
                    )
            self.assertEqual(
                self.connection.execute(
                    "SELECT count(*) FROM machine_transcript_publication_policy_runs"
                ).fetchone()[0],
                0,
            )
            self.assertEqual(
                self.connection.execute(
                    "SELECT count(*) FROM publication_manifest_imports"
                ).fetchone()[0],
                0,
            )
        self.assertEqual(
            self.connection.execute(
                """
                SELECT count(*) FROM machine_transcript_publication_policy_runs
                WHERE instr(plan_json, 'PRIVATE_SENTINEL') > 0
                """
            ).fetchone()[0],
            0,
        )

    def test_committed_run_cannot_publish_after_segment_or_gate_drift(self) -> None:
        revisions: list[str] = []
        for label in ("segment-drift", "gate-drift"):
            _, revision_id = self._add_revision(label)
            self._add_gates(revision_id)
            revisions.append(revision_id)
        plan = build_machine_transcript_publication_plan(self.connection)
        self.assertEqual(plan["eligible_revision_count"], 2)
        plan_json = json.dumps(
            {key: value for key, value in plan.items() if key != "plan_sha256"},
            ensure_ascii=False,
            separators=(",", ":"),
            sort_keys=True,
        )
        manifest_id = f"machine-transcript-default-policy-v1:{plan['plan_sha256']}"
        applied_at = utc_now()
        with transaction(self.connection):
            ensure_machine_transcript_policy_reviewer(self.connection)
            self.connection.execute(
                """
                INSERT INTO machine_transcript_publication_policy_runs(
                    plan_sha256, policy_id, manifest_id, reviewer_id, plan_json,
                    eligible_revision_count, new_decision_count,
                    existing_policy_count, protected_existing_count, applied_at
                ) VALUES(?, ?, ?, ?, ?, 2, 2, 0, 0, ?)
                """,
                (
                    plan["plan_sha256"],
                    POLICY_ID,
                    manifest_id,
                    MACHINE_TRANSCRIPT_POLICY_REVIEWER_ID,
                    plan_json,
                    applied_at,
                ),
            )
            self.connection.execute(
                """
                INSERT INTO publication_manifest_imports(
                    manifest_id, input_sha256, schema_version,
                    publication_decision_count, gate_decision_count, imported_at
                ) VALUES(?, ?, 1, 2, 0, ?)
                """,
                (manifest_id, plan["plan_sha256"], applied_at),
            )

        self.connection.execute(
            """
            INSERT INTO transcript_segments(
                segment_id, revision_id, ordinal, start_ms, end_ms, text,
                language, metadata_json
            ) VALUES(?, ?, 2, 2000, 3000, 'drifted text', 'en', '{}')
            """,
            (stable_id("seg", revisions[0], "drift"), revisions[0]),
        )
        self._add_gates(
            revisions[1],
            gate_kinds=("privacy",),
            decision="clear",
            decided_at="2026-02-01T00:00:00Z",
        )

        for revision_id in revisions:
            with self.subTest(revision_id=revision_id), self.assertRaisesRegex(
                sqlite3.IntegrityError, "not authorized"
            ):
                self.connection.execute(
                    """
                    INSERT INTO publication_decisions(
                        publication_decision_id, object_type, object_id, decision,
                        reviewer_id, review_decision_id, decided_at, basis, notes,
                        public_label, manifest_id
                    ) VALUES('machine-transcript-default-v1:' || ?,
                             'transcript_revision', ?, 'publish', ?, NULL, ?, ?, ?, ?, ?)
                    """,
                    (
                        revision_id,
                        revision_id,
                        MACHINE_TRANSCRIPT_POLICY_REVIEWER_ID,
                        applied_at,
                        POLICY_BASIS,
                        DISCLAIMER_TEXT,
                        PUBLIC_LABEL,
                        manifest_id,
                    ),
                )
        self.assertEqual(
            self.connection.execute(
                "SELECT count(*) FROM publication_decisions WHERE reviewer_id = ?",
                (MACHINE_TRANSCRIPT_POLICY_REVIEWER_ID,),
            ).fetchone()[0],
            0,
        )
        self.assertEqual(
            self.connection.execute(
                "SELECT count(*) FROM public_transcript_revisions"
            ).fetchone()[0],
            0,
        )

    def test_supported_admin_paths_reject_reserved_policy_namespaces(self) -> None:
        with self.assertRaisesRegex(
            ReviewerAdminManifestError, "reserved for the dedicated closed policy"
        ):
            register_reviewer_fixture(
                self.connection,
                MACHINE_TRANSCRIPT_POLICY_REVIEWER_ID,
                MACHINE_TRANSCRIPT_POLICY_DISPLAY_LABEL,
                "automated_policy",
            )
        self.assertIsNone(
            self.connection.execute(
                "SELECT 1 FROM reviewers WHERE reviewer_id = ?",
                (MACHINE_TRANSCRIPT_POLICY_REVIEWER_ID,),
            ).fetchone()
        )

        base = {
            "schema_version": 1,
            "manifest_id": "ordinary-publication-manifest",
            "publication_decisions": [
                {
                    "publication_decision_id": "ordinary-publication-decision",
                    "object_type": "source",
                    "object_id": "ordinary-source",
                    "decision": "withhold",
                    "reviewer_id": self.human_reviewer,
                    "decided_at": "2026-01-01T00:00:00Z",
                    "basis": "Test reserved namespace rejection.",
                    "note": "No decision may squat a built-in prefix.",
                }
            ],
            "gate_decisions": [],
        }
        variants = []
        reserved_manifest = json.loads(json.dumps(base))
        reserved_manifest["manifest_id"] = (
            "machine-transcript-default-policy-v1:" + "0" * 64
        )
        variants.append(reserved_manifest)
        reserved_decision = json.loads(json.dumps(base))
        reserved_decision["publication_decisions"][0]["publication_decision_id"] = (
            "machine-transcript-default-v1:ordinary-source"
        )
        variants.append(reserved_decision)
        reserved_gate = json.loads(json.dumps(base))
        reserved_gate["publication_decisions"] = []
        reserved_gate["gate_decisions"] = [
            {
                "publication_gate_decision_id":
                    "machine-transcript-default-v1:ordinary-source",
                "object_type": "source",
                "object_id": "ordinary-source",
                "gate_kind": "rights",
                "decision": "withhold",
                "reviewer_id": self.human_reviewer,
                "decided_at": "2026-01-01T00:00:00Z",
                "basis": "Test reserved namespace rejection.",
                "note": "No gate may squat a built-in decision prefix.",
            }
        ]
        variants.append(reserved_gate)
        for index, variant in enumerate(variants):
            path = Path(self.temporary.name) / f"reserved-publication-{index}.json"
            path.write_text(json.dumps(variant), encoding="utf-8")
            with self.subTest(index=index), self.assertRaisesRegex(
                PublicationManifestError, "prefixes are reserved"
            ):
                load_publication_manifest(path)

        _, revision_id = self._add_revision("reserved-direct-sql")
        with self.assertRaisesRegex(sqlite3.IntegrityError, "namespace is dedicated"):
            self.connection.execute(
                """
                INSERT INTO publication_decisions(
                    publication_decision_id, object_type, object_id, decision,
                    reviewer_id, decided_at, basis
                ) VALUES('machine-transcript-default-v1:' || ?,
                         'transcript_revision', ?, 'withhold', ?,
                         '2026-01-01T00:00:00Z', 'Forbidden reserved ID.')
                """,
                (revision_id, revision_id, self.human_reviewer),
            )
        with self.assertRaisesRegex(sqlite3.IntegrityError, "namespace is dedicated"):
            self.connection.execute(
                """
                INSERT INTO publication_manifest_imports(
                    manifest_id, input_sha256, schema_version,
                    publication_decision_count, gate_decision_count, imported_at
                ) VALUES(?, ?, 1, 1, 0, '2026-01-01T00:00:00Z')
                """,
                (
                    "machine-transcript-default-policy-v1:" + "1" * 64,
                    "1" * 64,
                ),
            )
        self.connection.execute("DROP TRIGGER machine_transcript_policy_decision_namespace")
        self.connection.execute(
            """
            INSERT INTO publication_decisions(
                publication_decision_id, object_type, object_id, decision,
                reviewer_id, decided_at, basis
            ) VALUES('machine-transcript-default-v1:' || ?,
                     'transcript_revision', ?, 'withhold', ?,
                     '2026-01-01T00:00:00Z', 'Simulated catalog tamper.')
            """,
            (revision_id, revision_id, self.human_reviewer),
        )
        with self.assertRaisesRegex(RuntimeError, "namespace was used outside policy"):
            validate_machine_transcript_publication_policy(self.connection)

    def test_policy_apply_rejects_exact_identity_with_nonbuiltin_provenance(self) -> None:
        # Simulate unsupported direct/catalog tampering that produces the right
        # visible ID, label, kind, and active state under unrelated audit IDs.
        with mock.patch(
            "himr_corpus.reviewer_admin.MACHINE_TRANSCRIPT_POLICY_REVIEWER_ID",
            "temporarily-not-reserved-for-squatter-fixture",
        ):
            register_reviewer_fixture(
                self.connection,
                MACHINE_TRANSCRIPT_POLICY_REVIEWER_ID,
                MACHINE_TRANSCRIPT_POLICY_DISPLAY_LABEL,
                "automated_policy",
            )
        _, revision_id = self._add_revision("nonbuiltin-policy-provenance")
        self._add_gates(revision_id)
        plan = build_machine_transcript_publication_plan(self.connection)

        with self.assertRaisesRegex(
            ReviewerAdminManifestError, "exact built-in provenance"
        ):
            apply_machine_transcript_publication_plan(
                self.connection, expected_plan_sha256=plan["plan_sha256"]
            )
        self.assertEqual(
            self.connection.execute(
                "SELECT count(*) FROM machine_transcript_publication_policy_runs"
            ).fetchone()[0],
            0,
        )
        self.assertEqual(
            self.connection.execute(
                "SELECT count(*) FROM publication_decisions"
            ).fetchone()[0],
            0,
        )

    def test_named_speakers_are_excluded_and_validator_catches_direct_tamper(self) -> None:
        _, named = self._add_revision("named-speaker", speaker_label="Daniel")
        self._add_gates(named)
        _, ordinary = self._add_revision("unknown-speaker")
        self._add_gates(ordinary)
        plan = build_machine_transcript_publication_plan(self.connection)
        self.assertEqual(
            [item["revision_id"] for item in plan["revisions"]], [ordinary]
        )
        apply_machine_transcript_publication_plan(
            self.connection, expected_plan_sha256=plan["plan_sha256"]
        )

        # Simulate corruption outside the supported append-only path. Semantic
        # validation must independently catch identity data added after admission.
        self.connection.execute("DROP TRIGGER transcript_segments_no_update")
        self.connection.execute(
            "UPDATE transcript_segments SET speaker_label = 'Daniel' WHERE revision_id = ?",
            (ordinary,),
        )
        with self.assertRaisesRegex(RuntimeError, "invalid revision"):
            validate_machine_transcript_publication_policy(self.connection)

    def test_direct_sql_cannot_override_human_publish_withhold_or_remove(self) -> None:
        baseline_result, _ = self._apply_one_baseline()
        manifest_id = baseline_result["manifest_id"]
        run_time = self.connection.execute(
            "SELECT applied_at FROM machine_transcript_publication_policy_runs"
        ).fetchone()[0]
        targets: list[tuple[str, str]] = []
        for decision in ("publish", "withhold", "remove"):
            _, revision_id = self._add_revision(f"human-{decision}")
            self._add_gates(revision_id)
            if decision == "remove":
                self._human_retraction(revision_id)
            self._human_publication(revision_id, decision)
            targets.append((decision, revision_id))

        for decision, revision_id in targets:
            self.assertIsNone(
                self.connection.execute(
                    """
                    SELECT 1 FROM machine_transcript_policy_publish_scope
                    WHERE revision_id = ?
                    """,
                    (revision_id,),
                ).fetchone(),
                decision,
            )
            with self.assertRaisesRegex(
                sqlite3.IntegrityError,
                "not authorized",
                msg=decision,
            ):
                self.connection.execute(
                    """
                    INSERT INTO publication_decisions(
                        publication_decision_id, object_type, object_id, decision,
                        reviewer_id, review_decision_id, decided_at, basis, notes,
                        public_label, manifest_id
                    ) VALUES('machine-transcript-default-v1:' || ?,
                             'transcript_revision', ?, 'publish', ?, NULL, ?,
                             'Closed automated policy v1: the ordinary recording-coordinate machine transcript has current human clear decisions for each independent rights, privacy, and sensitivity gate; no wording review was performed.',
                             ?, 'machine transcript (unreviewed)', ?)
                    """,
                    (
                        revision_id,
                        revision_id,
                        MACHINE_TRANSCRIPT_POLICY_REVIEWER_ID,
                        run_time,
                        DISCLAIMER_TEXT,
                        manifest_id,
                    ),
                )

    def test_policy_reviewer_has_no_gate_review_correction_or_lifecycle_authority(self) -> None:
        baseline_result, baseline = self._apply_one_baseline()
        run_time = self.connection.execute(
            "SELECT applied_at FROM machine_transcript_publication_policy_runs"
        ).fetchone()[0]
        for gate_decision in ("clear", "withhold"):
            with self.assertRaisesRegex(sqlite3.IntegrityError, "not authorized"):
                self.connection.execute(
                    """
                    INSERT INTO publication_gate_decisions(
                        publication_gate_decision_id, object_type, object_id,
                        gate_kind, decision, reviewer_id, decided_at, basis
                    ) VALUES(?, 'transcript_revision', ?, 'rights', ?, ?, ?, 'forbidden')
                    """,
                    (
                        stable_id("badgate", gate_decision),
                        baseline,
                        gate_decision,
                        MACHINE_TRANSCRIPT_POLICY_REVIEWER_ID,
                        run_time,
                    ),
                )
        with self.assertRaisesRegex(sqlite3.IntegrityError, "cannot review wording"):
            self.connection.execute(
                """
                INSERT INTO review_decisions(
                    review_decision_id, target_type, target_id, reviewer_id,
                    decision, decided_at, basis
                ) VALUES('bad-policy-review', 'transcript_revision', ?, ?,
                         'dispute', ?, 'forbidden')
                """,
                (baseline, MACHINE_TRANSCRIPT_POLICY_REVIEWER_ID, run_time),
            )
        with self.assertRaisesRegex(sqlite3.IntegrityError, "cannot create corrections"):
            self.connection.execute(
                """
                INSERT INTO corrections(
                    correction_id, target_type, target_id, reviewer_id, reason, created_at
                ) VALUES('bad-policy-correction', 'transcript_revision', ?, ?,
                         'forbidden', ?)
                """,
                (baseline, MACHINE_TRANSCRIPT_POLICY_REVIEWER_ID, run_time),
            )
        human_review_id = stable_id("rdec", baseline, "human-dispute")
        self.connection.execute(
            """
            INSERT INTO review_decisions(
                review_decision_id, target_type, target_id, reviewer_id,
                decision, decided_at, basis
            ) VALUES(?, 'transcript_revision', ?, ?, 'dispute',
                     '2026-01-03T00:00:00Z', 'Human review for negative test.')
            """,
            (human_review_id, baseline, self.human_reviewer),
        )
        for state in ("disputed", "retracted", "reinstated"):
            with self.assertRaisesRegex(sqlite3.IntegrityError, "cannot dispute"):
                self.connection.execute(
                    """
                    INSERT INTO transcript_lifecycle_decisions(
                        transcript_lifecycle_decision_id, revision_id,
                        lifecycle_state, reason_code, reviewer_id,
                        review_decision_id, decided_at, basis,
                        public_explanation
                    ) VALUES(?, ?, ?, 'editorial_decision', ?, ?, ?,
                             'forbidden', 'forbidden')
                    """,
                    (
                        stable_id("badtlc", baseline, state),
                        baseline,
                        state,
                        MACHINE_TRANSCRIPT_POLICY_REVIEWER_ID,
                        human_review_id,
                        run_time,
                    ),
                )
        self.assertEqual(baseline_result["lifecycle_decisions_inserted"], 0)

    def test_existing_exporter_emits_machine_warning_code(self) -> None:
        recording_id, revision_id = self._add_revision("public-export")
        self._add_gates(revision_id)
        first_segment_id = stable_id("seg", revision_id, 0)
        first_word_id = stable_id("wrd", first_segment_id, 0)
        self.connection.execute(
            """
            INSERT INTO transcript_words(
                word_id, segment_id, ordinal, start_ms, end_ms, token,
                normalized_token
            ) VALUES(?, ?, 0, 0, 500, 'PRIVATE', 'private')
            """,
            (first_word_id, first_segment_id),
        )
        source = source_id("youtube", "youtube_video", "policy-test-video")
        self.connection.execute(
            """
            INSERT INTO sources(
                source_id, platform, source_kind, native_id, canonical_url, title,
                observed_at, access_state, review_state, metadata_json, created_at,
                updated_at
            ) VALUES(?, 'youtube', 'youtube_video', 'policy-test-video',
                     'https://www.youtube.com/watch?v=policy-test-video',
                     'Policy test video', '2026-01-01T00:00:00Z', 'public',
                     'reviewed', '{}', '2026-01-01T00:00:00Z',
                     '2026-01-01T00:00:00Z')
            """,
            (source,),
        )
        self.connection.execute(
            """
            INSERT INTO recording_sources(
                recording_source_id, recording_id, source_id, mapping_role,
                mapping_method, confidence_state, metadata_json
            ) VALUES(?, ?, ?, 'full', 'human_review', 'reviewed', '{}')
            """,
            (stable_id("rs", recording_id, source), recording_id, source),
        )
        for object_type, object_id in (("source", source), ("recording", recording_id)):
            for gate_kind in ("rights", "privacy", "sensitivity"):
                self.connection.execute(
                    """
                    INSERT INTO publication_gate_decisions(
                        publication_gate_decision_id, object_type, object_id,
                        gate_kind, decision, reviewer_id, decided_at, basis
                    ) VALUES(?, ?, ?, ?, 'clear', ?,
                             '2026-01-02T00:00:00Z', 'Human parent gate clear.')
                    """,
                    (
                        stable_id("pgt", object_type, object_id, gate_kind),
                        object_type,
                        object_id,
                        gate_kind,
                        self.human_reviewer,
                    ),
                )
            self.connection.execute(
                """
                INSERT INTO publication_decisions(
                    publication_decision_id, object_type, object_id, decision,
                    reviewer_id, decided_at, basis, public_label
                ) VALUES(?, ?, ?, 'publish', ?, '2026-01-03T00:00:00Z',
                         'Human parent publication.', 'test parent')
                """,
                (
                    stable_id("pub", object_type, object_id),
                    object_type,
                    object_id,
                    self.human_reviewer,
                ),
            )
        plan = build_machine_transcript_publication_plan(self.connection)
        apply_machine_transcript_publication_plan(
            self.connection, expected_plan_sha256=plan["plan_sha256"]
        )

        release = build_release(self.connection)
        exported = release["recordings"][0]["transcript_revisions"][0]
        self.assertEqual(exported["revision_id"], revision_id)
        self.assertTrue(exported["machine_generated"])
        self.assertTrue(exported["unreviewed"])
        self.assertFalse(exported["verified_quotation"])
        self.assertEqual(exported["disclaimer_code"], DISCLAIMER_CODE)
        self.assertEqual(
            self.connection.execute(
                """
                SELECT notes FROM current_publication_decisions
                WHERE object_type = 'transcript_revision' AND object_id = ?
                """,
                (revision_id,),
            ).fetchone()[0],
            DISCLAIMER_TEXT,
        )
        public_before = json.dumps(release, ensure_ascii=False, sort_keys=True)
        segment_rows_before = [
            tuple(row)
            for row in self.connection.execute(
                """
                SELECT segment_id, ordinal, start_ms, end_ms, text, speaker_label
                FROM public_transcript_segments
                WHERE revision_id = ? ORDER BY ordinal
                """,
                (revision_id,),
            )
        ]
        self.assertEqual(len(segment_rows_before), 2)

        # BEFORE INSERT guards remain effective when callers disable recursive
        # triggers, including INSERT OR REPLACE's implicit-delete route.
        self.connection.execute("PRAGMA recursive_triggers = OFF")
        try:
            for ordinal, speaker in ((2, None), (3, "Daniel")):
                with self.subTest(speaker=speaker), self.assertRaisesRegex(
                    sqlite3.IntegrityError, "seals its exact segment set"
                ):
                    self.connection.execute(
                        """
                        INSERT INTO transcript_segments(
                            segment_id, revision_id, ordinal, start_ms, end_ms,
                            text, speaker_label, language, metadata_json
                        ) VALUES(?, ?, ?, ?, ?, 'post-policy text', ?, 'en', '{}')
                        """,
                        (
                            stable_id("seg", revision_id, ordinal),
                            revision_id,
                            ordinal,
                            ordinal * 1000,
                            (ordinal + 1) * 1000,
                            speaker,
                        ),
                    )
            with self.assertRaisesRegex(
                sqlite3.IntegrityError, "append-only|seals its exact segment set"
            ):
                self.connection.execute(
                    """
                    INSERT OR REPLACE INTO transcript_segments(
                        segment_id, revision_id, ordinal, start_ms, end_ms,
                        text, normalized_text, speaker_label, language,
                        confidence_band, metadata_json
                    ) VALUES(?, ?, 0, 0, 1000, 'replacement text',
                             'replacement text', NULL, 'en', 'medium', '{}')
                    """,
                    (first_segment_id, revision_id),
                )
            with self.assertRaisesRegex(sqlite3.IntegrityError, "append-only"):
                self.connection.execute(
                    """
                    INSERT OR REPLACE INTO transcript_revisions(
                        revision_id, recording_id, revision_kind, origin, language,
                        review_state, created_at, metadata_json
                    ) VALUES(?, ?, 'raw_asr', 'replacement', 'fr', 'machine',
                             '2026-01-01T00:00:01Z', '{}')
                    """,
                    (revision_id, recording_id),
                )
            with self.assertRaisesRegex(
                sqlite3.IntegrityError, "append-only|seals transcript words"
            ):
                self.connection.execute(
                    """
                    INSERT OR REPLACE INTO transcript_words(
                        word_id, segment_id, ordinal, start_ms, end_ms, token,
                        normalized_token
                    ) VALUES(?, ?, 0, 0, 500, 'REPLACED', 'replaced')
                    """,
                    (first_word_id, first_segment_id),
                )
            with self.assertRaisesRegex(sqlite3.IntegrityError, "seals transcript words"):
                self.connection.execute(
                    """
                    INSERT INTO transcript_words(
                        word_id, segment_id, ordinal, start_ms, end_ms, token,
                        normalized_token
                    ) VALUES(?, ?, 1, 500, 900, 'LATER', 'later')
                    """,
                    (stable_id("wrd", first_segment_id, 1), first_segment_id),
                )
        finally:
            self.connection.execute("PRAGMA recursive_triggers = ON")

        self.assertEqual(
            json.dumps(build_release(self.connection), ensure_ascii=False, sort_keys=True),
            public_before,
        )
        self.assertEqual(
            [
                tuple(row)
                for row in self.connection.execute(
                    """
                    SELECT segment_id, ordinal, start_ms, end_ms, text, speaker_label
                    FROM public_transcript_segments
                    WHERE revision_id = ? ORDER BY ordinal
                    """,
                    (revision_id,),
                )
            ],
            segment_rows_before,
        )

    def test_human_lifecycle_and_evidence_rows_reject_replace_with_recursion_off(self) -> None:
        _, revision_id = self._add_revision("no-replace-lifecycle")
        self._human_retraction(revision_id)
        lifecycle = self.connection.execute(
            """
            SELECT * FROM transcript_lifecycle_decisions WHERE revision_id = ?
            """,
            (revision_id,),
        ).fetchone()
        review = self.connection.execute(
            """
            SELECT * FROM review_decisions WHERE review_decision_id = ?
            """,
            (lifecycle["review_decision_id"],),
        ).fetchone()
        correction_id = stable_id("cor", revision_id, "test")
        self.connection.execute(
            """
            INSERT INTO corrections(
                correction_id, target_type, target_id, reviewer_id, reason, created_at
            ) VALUES(?, 'transcript_revision', ?, ?, 'Original correction.',
                     '2026-01-05T00:00:00Z')
            """,
            (correction_id, revision_id, self.human_reviewer),
        )
        _, parent_revision = self._add_revision("no-replace-parent")
        self.connection.execute(
            """
            INSERT INTO transcript_revision_parents(
                revision_id, parent_revision_id, relation_kind
            ) VALUES(?, ?, 'derived_from')
            """,
            (revision_id, parent_revision),
        )

        self.connection.execute("PRAGMA recursive_triggers = OFF")
        try:
            with self.assertRaisesRegex(sqlite3.IntegrityError, "append-only"):
                self.connection.execute(
                    """
                    INSERT OR REPLACE INTO transcript_lifecycle_decisions(
                        transcript_lifecycle_decision_id, revision_id,
                        lifecycle_state, reason_code, reviewer_id,
                        review_decision_id, decided_at, basis,
                        public_explanation, notes
                    ) VALUES(?, ?, 'retracted', 'editorial_decision', ?, ?, ?, ?,
                             'Silently changed explanation.', NULL)
                    """,
                    (
                        lifecycle["transcript_lifecycle_decision_id"],
                        revision_id,
                        self.human_reviewer,
                        lifecycle["review_decision_id"],
                        lifecycle["decided_at"],
                        lifecycle["basis"],
                    ),
                )
            with self.assertRaisesRegex(sqlite3.IntegrityError, "append-only"):
                self.connection.execute(
                    """
                    INSERT OR REPLACE INTO review_decisions(
                        review_decision_id, review_task_id, target_type, target_id,
                        reviewer_id, decision, decided_at, reviewed_complete_item,
                        basis
                    ) VALUES(?, ?, 'transcript_revision', ?, ?, 'reject', ?, 1,
                             'Silently changed review basis.')
                    """,
                    (
                        review["review_decision_id"],
                        review["review_task_id"],
                        revision_id,
                        self.human_reviewer,
                        review["decided_at"],
                    ),
                )
            with self.assertRaisesRegex(sqlite3.IntegrityError, "append-only"):
                self.connection.execute(
                    """
                    INSERT OR REPLACE INTO corrections(
                        correction_id, target_type, target_id, reviewer_id,
                        reason, created_at
                    ) VALUES(?, 'transcript_revision', ?, ?,
                             'Silently changed correction.', '2026-01-05T00:00:00Z')
                    """,
                    (correction_id, revision_id, self.human_reviewer),
                )
            with self.assertRaisesRegex(sqlite3.IntegrityError, "append-only"):
                self.connection.execute(
                    """
                    INSERT OR REPLACE INTO transcript_revision_parents(
                        revision_id, parent_revision_id, relation_kind
                    ) VALUES(?, ?, 'supersedes')
                    """,
                    (revision_id, parent_revision),
                )
        finally:
            self.connection.execute("PRAGMA recursive_triggers = ON")

        self.assertEqual(
            self.connection.execute(
                """
                SELECT public_explanation FROM transcript_lifecycle_decisions
                WHERE transcript_lifecycle_decision_id = ?
                """,
                (lifecycle["transcript_lifecycle_decision_id"],),
            ).fetchone()[0],
            "Retracted by a human reviewer.",
        )

    def test_fresh_migration_creates_no_authority_or_public_state(self) -> None:
        empty = tempfile.TemporaryDirectory(prefix="machine-policy-empty-")
        self.addCleanup(empty.cleanup)
        connection = connect(Path(empty.name) / "empty.sqlite3")
        self.addCleanup(connection.close)
        migrate(connection)
        for table in (
            "reviewers",
            "reviewer_admin_events",
            "publication_decisions",
            "publication_gate_decisions",
            "review_decisions",
            "transcript_lifecycle_decisions",
            "publication_manifest_imports",
            "machine_transcript_publication_policy_runs",
            "public_transcript_revisions",
        ):
            self.assertEqual(
                connection.execute(f"SELECT count(*) FROM {table}").fetchone()[0],
                0,
                table,
            )
        validate_database(connection)

    def test_plan_and_apply_refuse_pending_migration_without_sidecars(self) -> None:
        pending_dir = Path(self.temporary.name) / "pending-migrations"
        pending_dir.mkdir()
        for path in sorted((CORPUS_ROOT / "migrations").glob("[0-9][0-9][0-9][0-9]_*.sql")):
            if int(path.name[:4]) <= 27:
                shutil.copy2(path, pending_dir / path.name)
        pending_database = Path(self.temporary.name) / "pending-v27.sqlite3"
        pending_connection = connect(pending_database)
        with mock.patch("himr_corpus.db.MIGRATIONS_DIR", pending_dir):
            migrate(pending_connection)
        pending_connection.close()
        before = (
            hashlib.sha256(pending_database.read_bytes()).hexdigest(),
            pending_database.stat().st_size,
            pending_database.stat().st_mtime_ns,
            sorted(path.name for path in pending_database.parent.iterdir()),
        )

        for argv in (
            ["plan-machine-transcript-publication", "--db", str(pending_database)],
            [
                "apply-machine-transcript-publication",
                "--db",
                str(pending_database),
                "--expected-plan-sha256",
                "0" * 64,
            ],
        ):
            with self.subTest(command=argv[0]), self.assertRaisesRegex(
                RuntimeError, "Pending migrations"
            ), redirect_stdout(io.StringIO()):
                cli.main(argv)
            self.assertEqual(
                (
                    hashlib.sha256(pending_database.read_bytes()).hexdigest(),
                    pending_database.stat().st_size,
                    pending_database.stat().st_mtime_ns,
                    sorted(path.name for path in pending_database.parent.iterdir()),
                ),
                before,
            )

    def test_migration_refuses_preexisting_reserved_policy_provenance(self) -> None:
        migration_dir = Path(self.temporary.name) / "reserved-preflight-migrations"
        migration_dir.mkdir()
        source_migrations = CORPUS_ROOT / "migrations"
        for path in sorted(source_migrations.glob("[0-9][0-9][0-9][0-9]_*.sql")):
            if int(path.name[:4]) <= 27:
                shutil.copy2(path, migration_dir / path.name)
        database = Path(self.temporary.name) / "reserved-preflight-v27.sqlite3"
        connection = connect(database)
        self.addCleanup(connection.close)
        with mock.patch("himr_corpus.db.MIGRATIONS_DIR", migration_dir):
            migrate(connection)
        # Simulate the pre-0028 administrator, which did not yet reserve this ID.
        with mock.patch(
            "himr_corpus.reviewer_admin.MACHINE_TRANSCRIPT_POLICY_REVIEWER_ID",
            "not-yet-reserved-in-v27",
        ):
            register_reviewer_fixture(
                connection,
                MACHINE_TRANSCRIPT_POLICY_REVIEWER_ID,
                MACHINE_TRANSCRIPT_POLICY_DISPLAY_LABEL,
                "automated_policy",
            )
        connection.execute(
            """
            INSERT INTO publication_decisions(
                publication_decision_id, object_type, object_id, decision,
                reviewer_id, decided_at, basis
            ) VALUES('preexisting-reserved-withhold', 'source',
                     'preexisting-source', 'withhold', ?,
                     '2026-01-01T00:00:00Z', 'Preexisting restrictive use.')
            """,
            (MACHINE_TRANSCRIPT_POLICY_REVIEWER_ID,),
        )
        shutil.copy2(
            source_migrations / "0028_machine_transcript_default_policy.sql",
            migration_dir / "0028_machine_transcript_default_policy.sql",
        )

        with mock.patch(
            "himr_corpus.db.MIGRATIONS_DIR", migration_dir
        ), self.assertRaisesRegex(
            sqlite3.IntegrityError,
            "reserved machine transcript policy provenance already exists",
        ):
            migrate(connection)

        self.assertEqual(
            connection.execute("SELECT max(version) FROM schema_migrations").fetchone()[0],
            27,
        )
        self.assertIsNone(
            connection.execute(
                """
                SELECT 1 FROM sqlite_master
                WHERE type = 'table'
                  AND name = 'machine_transcript_publication_policy_runs'
                """
            ).fetchone()
        )
        self.assertEqual(
            connection.execute(
                """
                SELECT decision FROM publication_decisions
                WHERE publication_decision_id = 'preexisting-reserved-withhold'
                """
            ).fetchone()[0],
            "withhold",
        )

    def test_migration_refuses_preexisting_reserved_gate_id_prefix(self) -> None:
        migration_dir = Path(self.temporary.name) / "reserved-gate-migrations"
        migration_dir.mkdir()
        source_migrations = CORPUS_ROOT / "migrations"
        for path in sorted(source_migrations.glob("[0-9][0-9][0-9][0-9]_*.sql")):
            if int(path.name[:4]) <= 27:
                shutil.copy2(path, migration_dir / path.name)
        database = Path(self.temporary.name) / "reserved-gate-v27.sqlite3"
        connection = connect(database)
        self.addCleanup(connection.close)
        with mock.patch("himr_corpus.db.MIGRATIONS_DIR", migration_dir):
            migrate(connection)
        register_reviewer_fixture(
            connection,
            "reviewer_v27_gate_prefix",
            "V27 Gate Prefix Reviewer",
            "human",
        )
        connection.execute(
            """
            INSERT INTO publication_gate_decisions(
                publication_gate_decision_id, object_type, object_id, gate_kind,
                decision, reviewer_id, decided_at, basis
            ) VALUES('machine-transcript-default-v1:squatted-gate',
                     'source', 'preexisting-source', 'rights', 'withhold',
                     'reviewer_v27_gate_prefix', '2026-01-01T00:00:00Z',
                     'Preexisting reserved gate ID prefix.')
            """
        )
        shutil.copy2(
            source_migrations / "0028_machine_transcript_default_policy.sql",
            migration_dir / "0028_machine_transcript_default_policy.sql",
        )

        with mock.patch(
            "himr_corpus.db.MIGRATIONS_DIR", migration_dir
        ), self.assertRaisesRegex(
            sqlite3.IntegrityError,
            "reserved machine transcript policy provenance already exists",
        ):
            migrate(connection)
        self.assertEqual(
            connection.execute("SELECT max(version) FROM schema_migrations").fetchone()[0],
            27,
        )
        self.assertIsNone(
            connection.execute(
                """
                SELECT 1 FROM sqlite_master
                WHERE type = 'table'
                  AND name = 'machine_transcript_publication_policy_runs'
                """
            ).fetchone()
        )
        self.assertEqual(
            connection.execute(
                """
                SELECT decision FROM publication_gate_decisions
                WHERE publication_gate_decision_id =
                      'machine-transcript-default-v1:squatted-gate'
                """
            ).fetchone()[0],
            "withhold",
        )


if __name__ == "__main__":  # pragma: no cover
    unittest.main()
