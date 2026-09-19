from __future__ import annotations

import copy
import hashlib
import json
import sys
import tempfile
import unittest
from pathlib import Path


CORPUS_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(CORPUS_ROOT / "src"))

from himr_corpus.cli import build_parser  # noqa: E402
from himr_corpus.db import connect, migrate  # noqa: E402
from himr_corpus.entity_event_map_admin import (  # noqa: E402
    EntityEventMapManifestError,
    import_entity_event_map_manifest,
    validate_entity_event_map_manifest,
)


class EntityEventMapAdminTests(unittest.TestCase):
    def setUp(self) -> None:
        work_root = CORPUS_ROOT / "work"
        work_root.mkdir(exist_ok=True)
        self.temporary = tempfile.TemporaryDirectory(prefix="entity-event-map-", dir=work_root)
        self.addCleanup(self.temporary.cleanup)
        self.root = Path(self.temporary.name).resolve()
        self.connection = connect(self.root / "corpus.sqlite3")
        self.addCleanup(self.connection.close)
        migrate(self.connection)
        self._seed_catalog()
        self.local_evidence = self.root / "fictional-evidence.txt"
        self.local_evidence.write_bytes(b"fictional local evidence\n")
        self.manifest = self._manifest()

    def _seed_catalog(self) -> None:
        timestamp = "2026-08-26T20:00:00Z"
        self.connection.execute(
            """
            INSERT INTO sources(
                source_id, platform, source_kind, native_id, canonical_url,
                observed_at, access_state, review_state, created_at, updated_at
            ) VALUES(
                'source_fixture', 'fixture', 'public_video', 'fixture-video',
                'https://example.invalid/fictional-video', ?, 'public', 'reviewed', ?, ?
            )
            """,
            (timestamp, timestamp, timestamp),
        )
        self.connection.execute(
            """
            INSERT INTO recordings(
                recording_id, canonical_key, slug, title, date_basis, duration_ms,
                recording_type, review_state, metadata_json, created_at, updated_at
            ) VALUES(
                'recording_fixture', 'fixture:recording', 'fictional-recording',
                'Fictional recording', 'fixture', 10000, 'video', 'reviewed', '{}', ?, ?
            )
            """,
            (timestamp, timestamp),
        )
        self.connection.execute(
            """
            INSERT INTO recording_sources(
                recording_source_id, recording_id, source_id, mapping_role,
                mapping_method, confidence_state, metadata_json
            ) VALUES(
                'recording_source_fixture', 'recording_fixture', 'source_fixture',
                'canonical', 'fixture', 'reviewed', '{}'
            )
            """
        )
        self.connection.execute(
            """
            INSERT INTO media_objects(
                media_id, sha256, byte_count, media_kind, mime_type, container,
                duration_ms, first_cataloged_at, integrity_state
            ) VALUES(
                'media_fixture', ?, 100, 'video', 'video/mp4', 'mp4', 10000, ?, 'verified'
            )
            """,
            ("a" * 64, timestamp),
        )
        self.connection.execute(
            """
            INSERT INTO renditions(
                rendition_id, recording_id, media_id, rendition_kind,
                label, review_state, metadata_json
            ) VALUES(
                'rendition_fixture', 'recording_fixture', 'media_fixture',
                'source', 'Fictional source rendition', 'reviewed', '{}'
            )
            """
        )
        for revision_id, state in (
            ("revision_machine", "machine"),
            ("revision_reviewed", "media_checked"),
        ):
            self.connection.execute(
                """
                INSERT INTO transcript_revisions(
                    revision_id, recording_id, rendition_id, revision_kind,
                    origin, language, review_state, created_at, metadata_json
                ) VALUES(?, 'recording_fixture', 'rendition_fixture', 'human_verbatim',
                         'fixture', 'en', ?, ?, '{}')
                """,
                (revision_id, state, timestamp),
            )

    def _manifest(self) -> dict[str, object]:
        digest = hashlib.sha256(self.local_evidence.read_bytes()).hexdigest()
        return {
            "schema_version": 1,
            "manifest_id": "fixture-map-001",
            "created_at": "2026-08-26T21:00:00Z",
            "created_by": "fixture-maintainer",
            "basis": "All records are fictional test fixtures.",
            "entities": [
                {
                    "entity_id": "entity_fixture_aster",
                    "entity_type": "person",
                    "canonical_label": "Fictional Aster",
                    "slug": "fictional-aster",
                    "privacy_classification": "living_private_person",
                    "metadata": {"fictional": True},
                }
            ],
            "aliases": [
                {
                    "entity_alias_id": "alias_fixture_aster_handle",
                    "entity_id": "entity_fixture_aster",
                    "alias": "aster_fixture",
                    "alias_kind": "handle",
                    "source_id": "source_fixture",
                    "valid_from": None,
                    "valid_to": None,
                    "sensitive": False,
                    "privacy_review_required": True,
                }
            ],
            "appearances": [
                {
                    "appearance_id": "appearance_fixture_aster",
                    "entity_id": "entity_fixture_aster",
                    "source_id": "source_fixture",
                    "recording_id": "recording_fixture",
                    "rendition_id": "rendition_fixture",
                    "start_ms": 1000,
                    "end_ms": 2500,
                    "appearance_role": "fictional on-screen participant",
                    "evidence_basis_kind": "direct_media_observation",
                    "transcript_revision_id": None,
                    "local_evidence": None,
                    "basis": "Visible in this fictional fixture interval.",
                }
            ],
            "events": [
                {
                    "event_id": "event_fixture_claim",
                    "canonical_label": "Fictional claim",
                    "slug": "fictional-claim",
                    "event_kind": "fixture_claim",
                    "description": "An unverified fictional statement.",
                    "metadata": {"fictional": True},
                }
            ],
            "event_dates": [
                {
                    "event_date_id": "event_date_fixture_claim",
                    "event_id": "event_fixture_claim",
                    "date_kind": "statement_date",
                    "value_start": "2026-01-02",
                    "value_end": None,
                    "precision": "day",
                    "basis": "Fixture-only date.",
                    "certainty": "certain",
                }
            ],
            "event_participants": [
                {
                    "event_id": "event_fixture_claim",
                    "entity_id": "entity_fixture_aster",
                    "participant_role": "fictional claimant",
                }
            ],
            "event_relations": [],
            "event_evidence": [
                {
                    "event_evidence_id": "event_evidence_fixture_claim",
                    "event_id": "event_fixture_claim",
                    "source_id": "source_fixture",
                    "recording_id": "recording_fixture",
                    "rendition_id": "rendition_fixture",
                    "start_ms": 3000,
                    "end_ms": 4500,
                    "support_kind": "contradicting",
                    "evidence_basis_kind": "subject_unverified_claim",
                    "transcript_revision_id": None,
                    "local_evidence": {
                        "artifact_id": "artifact_fixture_claim_evidence",
                        "path": str(self.local_evidence),
                        "sha256": digest,
                        "byte_count": self.local_evidence.stat().st_size,
                    },
                    "basis": "The fictional subject says this; the occurrence is not verified.",
                }
            ],
        }

    def _write(self, value: dict[str, object], name: str = "manifest.json") -> Path:
        path = self.root / name
        path.write_text(json.dumps(value, indent=2), encoding="utf-8")
        return path.resolve()

    def test_validates_imports_privately_and_replays_idempotently(self) -> None:
        path = self._write(self.manifest)
        validated = validate_entity_event_map_manifest(self.connection, path)
        self.assertTrue(validated["validated"])
        self.assertEqual(validated["counts"]["event_evidence"], 1)
        self.assertEqual(self.connection.execute("SELECT count(*) FROM entities").fetchone()[0], 0)

        first = import_entity_event_map_manifest(self.connection, path)
        second = import_entity_event_map_manifest(self.connection, path)
        self.assertFalse(first["idempotent_replay"])
        self.assertTrue(second["idempotent_replay"])
        self.assertEqual(first["input_sha256"], second["input_sha256"])
        self.assertEqual(self.connection.execute("SELECT count(*) FROM entities").fetchone()[0], 1)
        self.assertEqual(self.connection.execute("SELECT count(*) FROM events").fetchone()[0], 1)
        self.assertEqual(self.connection.execute("SELECT count(*) FROM appearances").fetchone()[0], 1)
        self.assertEqual(self.connection.execute("SELECT count(*) FROM event_evidence").fetchone()[0], 1)
        self.assertEqual(
            self.connection.execute(
                "SELECT count(*) FROM event_participant_publication_subjects"
            ).fetchone()[0],
            1,
        )
        self.assertEqual(self.connection.execute("SELECT count(*) FROM claim_catalog_links").fetchone()[0], 2)
        self.assertEqual(self.connection.execute("SELECT count(*) FROM observations").fetchone()[0], 2)
        self.assertEqual(
            self.connection.execute("SELECT support_kind FROM event_evidence").fetchone()[0],
            "contradicting",
        )
        metadata = [
            json.loads(row[0])["entity_event_map"]
            for row in self.connection.execute(
                "SELECT metadata_json FROM observations ORDER BY observation_kind"
            )
        ]
        self.assertEqual(
            {item["evidence_basis_kind"] for item in metadata},
            {"direct_media_observation", "subject_unverified_claim"},
        )
        self.assertEqual(
            {item["time_coordinate_system"] for item in metadata},
            {"rendition_media_ms"},
        )
        anchor = self.connection.execute(
            """
            SELECT source_id, recording_id, rendition_id, link_state
            FROM claim_catalog_links
            WHERE claim_id = 'entity_event_map:event_evidence:event_evidence_fixture_claim'
            """
        ).fetchone()
        self.assertEqual(tuple(anchor), ("source_fixture", "recording_fixture", "rendition_fixture", "candidate"))
        self.assertEqual(
            self.connection.execute(
                "SELECT count(*) FROM review_tasks WHERE task_kind = 'entity_alias_privacy_review'"
            ).fetchone()[0],
            1,
        )
        self.assertEqual(
            self.connection.execute("SELECT visibility FROM artifacts").fetchone()[0],
            "private",
        )
        self.assertEqual(self.connection.execute("SELECT count(*) FROM public_entities").fetchone()[0], 0)
        self.assertEqual(self.connection.execute("SELECT count(*) FROM public_events").fetchone()[0], 0)
        for table in (
            "identity_assertions",
            "identity_assertion_subjects",
            "identity_assertion_decisions",
            "publication_decisions",
            "publication_gate_decisions",
        ):
            self.assertEqual(self.connection.execute(f"SELECT count(*) FROM {table}").fetchone()[0], 0)

    def test_rejects_unknown_mismatched_and_out_of_bounds_anchors_atomically(self) -> None:
        for name, mutate, message in (
            (
                "unknown-source",
                lambda value: value["appearances"][0].__setitem__("source_id", "source_missing"),
                "does not resolve|unknown",
            ),
            (
                "unknown-rendition",
                lambda value: value["event_evidence"][0].__setitem__(
                    "rendition_id", "rendition_missing"
                ),
                "does not resolve",
            ),
            (
                "out-of-bounds",
                lambda value: value["event_evidence"][0].__setitem__("end_ms", 10001),
                "exceeds rendition duration",
            ),
        ):
            with self.subTest(name=name):
                value = copy.deepcopy(self.manifest)
                value["manifest_id"] = f"fixture-map-{name}"
                mutate(value)
                with self.assertRaisesRegex(EntityEventMapManifestError, message):
                    import_entity_event_map_manifest(
                        self.connection, self._write(value, f"{name}.json")
                    )
                self.assertEqual(
                    self.connection.execute(
                        "SELECT count(*) FROM import_batches WHERE importer_name = 'entity_event_map_manifest_v1'"
                    ).fetchone()[0],
                    0,
                )
                self.assertEqual(self.connection.execute("SELECT count(*) FROM entities").fetchone()[0], 0)

    def test_transcript_derived_edge_requires_reviewed_matching_revision(self) -> None:
        machine = copy.deepcopy(self.manifest)
        machine["event_evidence"][0]["transcript_revision_id"] = "revision_machine"
        with self.assertRaisesRegex(EntityEventMapManifestError, "human-reviewed revision"):
            import_entity_event_map_manifest(
                self.connection, self._write(machine, "machine-transcript.json")
            )
        reviewed = copy.deepcopy(self.manifest)
        reviewed["manifest_id"] = "fixture-map-reviewed-transcript"
        reviewed["event_evidence"][0]["transcript_revision_id"] = "revision_reviewed"
        imported = import_entity_event_map_manifest(
            self.connection, self._write(reviewed, "reviewed-transcript.json")
        )
        self.assertFalse(imported["idempotent_replay"])
        self.assertEqual(
            self.connection.execute("SELECT transcript_revision_id FROM event_evidence").fetchone()[0],
            "revision_reviewed",
        )

    def test_sensitive_private_handle_requires_explicit_privacy_review(self) -> None:
        unsafe = copy.deepcopy(self.manifest)
        unsafe["aliases"][0]["privacy_review_required"] = False
        with self.assertRaisesRegex(EntityEventMapManifestError, "privacy review"):
            import_entity_event_map_manifest(
                self.connection, self._write(unsafe, "unsafe-private-handle.json")
            )
        self.assertEqual(self.connection.execute("SELECT count(*) FROM entity_aliases").fetchone()[0], 0)

    def test_whole_item_event_evidence_keeps_explicit_rendition_anchor(self) -> None:
        whole = copy.deepcopy(self.manifest)
        whole["manifest_id"] = "fixture-map-whole-item-evidence"
        whole["event_evidence"][0]["start_ms"] = None
        whole["event_evidence"][0]["end_ms"] = None
        whole["event_evidence"][0]["local_evidence"] = None
        import_entity_event_map_manifest(
            self.connection, self._write(whole, "whole-item-evidence.json")
        )
        evidence = self.connection.execute(
            "SELECT start_ms, end_ms, observation_id FROM event_evidence"
        ).fetchone()
        self.assertEqual(tuple(evidence), (None, None, None))
        anchor = self.connection.execute(
            """
            SELECT rendition_id, start_ms, end_ms, observation_id
            FROM claim_catalog_links
            WHERE claim_id = 'entity_event_map:event_evidence:event_evidence_fixture_claim'
            """
        ).fetchone()
        self.assertEqual(tuple(anchor), ("rendition_fixture", None, None, None))

    def test_local_evidence_may_not_point_into_public_build_paths(self) -> None:
        unsafe = copy.deepcopy(self.manifest)
        public_file = (CORPUS_ROOT.parent / "public" / "favicon.svg").resolve()
        unsafe["event_evidence"][0]["local_evidence"] = {
            "artifact_id": "artifact_unsafe_public_path",
            "path": str(public_file),
            "sha256": hashlib.sha256(public_file.read_bytes()).hexdigest(),
            "byte_count": public_file.stat().st_size,
        }
        with self.assertRaisesRegex(EntityEventMapManifestError, "public/static"):
            import_entity_event_map_manifest(
                self.connection, self._write(unsafe, "unsafe-public-path.json")
            )

    def test_orphan_event_and_relation_cycle_are_rejected(self) -> None:
        orphan = copy.deepcopy(self.manifest)
        orphan["event_participants"] = []
        with self.assertRaisesRegex(EntityEventMapManifestError, "orphan event.*participant"):
            import_entity_event_map_manifest(
                self.connection, self._write(orphan, "orphan.json")
            )

        cycle = copy.deepcopy(self.manifest)
        cycle["manifest_id"] = "fixture-map-cycle"
        cycle["events"].append(
            {
                "event_id": "event_fixture_second",
                "canonical_label": "Second fictional event",
                "slug": "second-fictional-event",
                "event_kind": "fixture",
                "description": None,
                "metadata": {"fictional": True},
            }
        )
        cycle["event_participants"].append(
            {
                "event_id": "event_fixture_second",
                "entity_id": "entity_fixture_aster",
                "participant_role": "fictional participant",
            }
        )
        second_evidence = copy.deepcopy(cycle["event_evidence"][0])
        second_evidence["event_evidence_id"] = "event_evidence_fixture_second"
        second_evidence["event_id"] = "event_fixture_second"
        second_evidence["start_ms"] = None
        second_evidence["end_ms"] = None
        second_evidence["local_evidence"] = None
        cycle["event_evidence"].append(second_evidence)
        cycle["event_relations"] = [
            {
                "event_relation_id": "relation_fixture_forward",
                "from_event_id": "event_fixture_claim",
                "relation_kind": "precedes",
                "to_event_id": "event_fixture_second",
                "basis": "Fictional fixture.",
            },
            {
                "event_relation_id": "relation_fixture_backward",
                "from_event_id": "event_fixture_second",
                "relation_kind": "follows",
                "to_event_id": "event_fixture_claim",
                "basis": "Fictional fixture.",
            },
        ]
        with self.assertRaisesRegex(EntityEventMapManifestError, "create a cycle"):
            import_entity_event_map_manifest(
                self.connection, self._write(cycle, "cycle.json")
            )
        self.assertEqual(self.connection.execute("SELECT count(*) FROM events").fetchone()[0], 0)

    def test_exact_manifest_and_local_file_tampering_are_rejected(self) -> None:
        path = self._write(self.manifest, "exact-replay.json")
        import_entity_event_map_manifest(self.connection, path)
        changed = copy.deepcopy(self.manifest)
        changed["basis"] = "Different bytes under the same manifest ID."
        changed_path = self._write(changed, "changed-replay.json")
        with self.assertRaisesRegex(EntityEventMapManifestError, "different exact bytes"):
            import_entity_event_map_manifest(self.connection, changed_path)
        self.assertEqual(
            self.connection.execute(
                "SELECT count(*) FROM import_batches WHERE importer_name = 'entity_event_map_manifest_v1'"
            ).fetchone()[0],
            1,
        )

        fresh = copy.deepcopy(self.manifest)
        fresh["manifest_id"] = "fixture-map-local-tamper"
        fresh_path = self._write(fresh, "local-tamper.json")
        self.local_evidence.write_bytes(b"changed bytes\n")
        with self.assertRaisesRegex(EntityEventMapManifestError, "byte_count|SHA-256"):
            import_entity_event_map_manifest(self.connection, fresh_path)

    def test_completed_replay_refuses_to_repair_deleted_rows_silently(self) -> None:
        path = self._write(self.manifest, "deleted-row-replay.json")
        import_entity_event_map_manifest(self.connection, path)
        task_id = self.connection.execute(
            "SELECT review_task_id FROM review_tasks WHERE task_kind = 'event_date_review'"
        ).fetchone()[0]
        self.connection.execute(
            "DELETE FROM review_tasks WHERE review_task_id = ?", (task_id,)
        )
        remaining = self.connection.execute("SELECT count(*) FROM review_tasks").fetchone()[0]
        with self.assertRaisesRegex(EntityEventMapManifestError, "tampered ledger"):
            import_entity_event_map_manifest(self.connection, path)
        self.assertEqual(
            self.connection.execute("SELECT count(*) FROM review_tasks").fetchone()[0],
            remaining,
        )

    def test_deleted_manifest_ledger_is_not_silently_recreated(self) -> None:
        path = self._write(self.manifest, "deleted-ledger-replay.json")
        imported = import_entity_event_map_manifest(self.connection, path)
        self.connection.execute(
            "DELETE FROM import_batches WHERE import_batch_id = ?",
            (imported["import_batch_id"],),
        )
        with self.assertRaisesRegex(EntityEventMapManifestError, "ledger is missing"):
            import_entity_event_map_manifest(self.connection, path)
        self.assertEqual(
            self.connection.execute(
                "SELECT count(*) FROM import_batches WHERE importer_name = 'entity_event_map_manifest_v1'"
            ).fetchone()[0],
            0,
        )

    def test_cli_registers_validation_and_import_commands(self) -> None:
        parser = build_parser()
        validate_args = parser.parse_args(
            [
                "validate-entity-event-map-manifest",
                "--db",
                "fixture.sqlite3",
                "--manifest",
                "fixture.json",
            ]
        )
        import_args = parser.parse_args(
            [
                "import-entity-event-map-manifest",
                "--db",
                "fixture.sqlite3",
                "--manifest",
                "fixture.json",
            ]
        )
        self.assertEqual(validate_args.command, "validate-entity-event-map-manifest")
        self.assertEqual(import_args.command, "import-entity-event-map-manifest")


if __name__ == "__main__":
    unittest.main()
