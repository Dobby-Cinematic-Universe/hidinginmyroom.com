from __future__ import annotations

import copy
import json
import sqlite3
import sys
import tempfile
import unittest
from pathlib import Path


CORPUS_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(CORPUS_ROOT / "src"))

from himr_corpus.db import connect, migrate  # noqa: E402
from himr_corpus.graph_release import (  # noqa: E402
    build_graph_release,
    export_graph_release_from_graph,
    validate_graph_release,
    validate_graph_release_shape,
)
from himr_corpus.ids import stable_id  # noqa: E402
from himr_corpus.importers import canonical_json  # noqa: E402
from corpus.tests.reviewer_fixtures import register_reviewer_fixture  # noqa: E402


class PublicGraphReleaseTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temporary = tempfile.TemporaryDirectory(prefix="public-graph-", dir="/tmp")
        self.addCleanup(self.temporary.cleanup)
        self.root = Path(self.temporary.name)
        self.connection = connect(self.root / "catalog.sqlite3")
        self.addCleanup(self.connection.close)
        migrate(self.connection)
        register_reviewer_fixture(
            self.connection, "reviewer_graph_human", "Synthetic graph reviewer", "human"
        )
        self._seed_catalog()

    def _seed_catalog(self) -> None:
        created = "2026-08-27T20:00:00Z"
        self.connection.execute(
            """
            INSERT INTO sources(
                source_id, platform, source_kind, native_id, canonical_url,
                observed_at, access_state, review_state, metadata_json,
                created_at, updated_at
            ) VALUES(
                'source_graph_fixture', 'fixture', 'public_video', 'graph-fixture',
                'https://example.test/public-graph-fixture', ?, 'public', 'reviewed',
                '{"private_note":"private:source-note"}', ?, ?
            )
            """,
            (created, created, created),
        )
        self.connection.execute(
            """
            INSERT INTO recordings(
                recording_id, canonical_key, slug, title, date_label, date_year,
                date_basis, duration_ms, recording_type, review_state, metadata_json,
                created_at, updated_at
            ) VALUES(
                'recording_graph_fixture', 'fixture:public-graph',
                'fictional-graph-recording', 'Fictional graph recording',
                '2026-01-02', 2026, 'synthetic fixture', 10000, 'video', 'reviewed',
                '{"private_note":"private:recording-note"}', ?, ?
            )
            """,
            (created, created),
        )
        self.connection.execute(
            """
            INSERT INTO recording_sources(
                recording_source_id, recording_id, source_id, mapping_role,
                mapping_method, confidence_state, metadata_json
            ) VALUES(
                'recording_source_graph_fixture', 'recording_graph_fixture',
                'source_graph_fixture', 'canonical', 'human_fixture', 'reviewed', '{}'
            )
            """
        )
        self.connection.execute(
            """
            INSERT INTO media_objects(
                media_id, sha256, byte_count, media_kind, mime_type, container,
                duration_ms, first_cataloged_at, integrity_state
            ) VALUES(
                'media_graph_fixture', ?, 100, 'video', 'video/mp4', 'mp4',
                10000, ?, 'verified'
            )
            """,
            ("a" * 64, created),
        )
        self.connection.execute(
            """
            INSERT INTO renditions(
                rendition_id, recording_id, media_id, rendition_kind,
                label, review_state, metadata_json
            ) VALUES(
                'rendition_graph_fixture', 'recording_graph_fixture',
                'media_graph_fixture', 'source', 'Synthetic source rendition',
                'reviewed', '{}'
            )
            """
        )
        self.connection.execute(
            """
            INSERT INTO entities(
                entity_id, entity_type, canonical_label, slug, visibility,
                review_state, metadata_json, created_at
            ) VALUES(
                'entity_graph_aster', 'person', 'Private working Aster label',
                'fictional-aster-public', 'public_candidate', 'reviewed',
                '{"private_note":"private:entity-note"}', ?
            )
            """,
            (created,),
        )
        self.connection.execute(
            """
            INSERT INTO entity_aliases(
                entity_alias_id, entity_id, alias, alias_kind, visibility,
                source_id, valid_from, valid_to
            ) VALUES(
                'alias_graph_private', 'entity_graph_aster',
                'secret-fixture-handle', 'handle', 'private',
                'source_graph_fixture', NULL, NULL
            )
            """
        )
        for event_id, slug in (
            ("event_graph_arrival", "fictional-arrival-public"),
            ("event_graph_departure", "fictional-departure-public"),
        ):
            self.connection.execute(
                """
                INSERT INTO events(
                    event_id, canonical_label, slug, event_kind, description,
                    visibility, review_state, metadata_json, created_at
                ) VALUES(?, 'Private working event label', ?, 'fixture_event',
                         'private:working-description', 'public_candidate', 'reviewed',
                         '{"private_note":"private:event-note"}', ?)
                """,
                (event_id, slug, created),
            )
        self.connection.execute(
            """
            INSERT INTO appearances(
                appearance_id, entity_id, recording_id, start_ms, end_ms,
                appearance_role, observation_id, review_state
            ) VALUES(
                'appearance_graph_aster', 'entity_graph_aster',
                'recording_graph_fixture', 1000, 2500,
                'private working appearance role', NULL, 'reviewed'
            )
            """
        )
        self.connection.execute(
            """
            INSERT INTO event_dates(
                event_date_id, event_id, date_kind, value_start, value_end,
                precision, basis, certainty
            ) VALUES(
                'date_graph_arrival', 'event_graph_arrival', 'private date kind',
                '2026-01-02', NULL, 'day', 'private:date-basis', 'certain'
            )
            """
        )
        for event_id in ("event_graph_arrival", "event_graph_departure"):
            role = f"private role for {event_id}"
            participant_id = stable_id(
                "participant", event_id, "entity_graph_aster", role
            )
            self.connection.execute(
                """
                INSERT INTO event_participants(
                    event_id, entity_id, participant_role, review_state
                ) VALUES(?, 'entity_graph_aster', ?, 'reviewed')
                """,
                (event_id, role),
            )
            self.connection.execute(
                """
                INSERT INTO event_participant_publication_subjects(
                    event_participant_id, event_id, entity_id, participant_role, created_at
                ) VALUES(?, ?, 'entity_graph_aster', ?, ?)
                """,
                (participant_id, event_id, role, created),
            )
        self.connection.execute(
            """
            INSERT INTO event_relations(
                event_relation_id, from_event_id, relation_kind, to_event_id, basis
            ) VALUES(
                'relation_graph_arrival_departure', 'event_graph_arrival',
                'private_before_label', 'event_graph_departure', 'private:relation-basis'
            )
            """
        )
        for ordinal, (evidence_id, event_id, start_ms, end_ms, support) in enumerate(
            (
                ("evidence_graph_arrival", "event_graph_arrival", 1000, 2500, "direct"),
                (
                    "evidence_graph_departure",
                    "event_graph_departure",
                    3000,
                    4500,
                    "contradicting",
                ),
            )
        ):
            self.connection.execute(
                """
                INSERT INTO event_evidence(
                    event_evidence_id, event_id, recording_id, source_id,
                    transcript_revision_id, observation_id, start_ms, end_ms, support_kind
                ) VALUES(?, ?, 'recording_graph_fixture', 'source_graph_fixture',
                         NULL, NULL, ?, ?, ?)
                """,
                (evidence_id, event_id, start_ms, end_ms, support),
            )
            self._insert_anchor(
                f"anchor_evidence_{ordinal}",
                f"entity_event_map:event_evidence:{evidence_id}",
                start_ms,
                end_ms,
            )
        self._insert_anchor(
            "anchor_appearance",
            "entity_event_map:appearance:appearance_graph_aster",
            1000,
            2500,
        )

        self._publish_base("source", "source_graph_fixture", "20:00:01")
        self._publish_base("recording", "recording_graph_fixture", "20:00:02")
        graph_objects = [
            ("entity", "entity_graph_aster", "Fictional Aster", False),
            ("event", "event_graph_arrival", "Fictional arrival", False),
            ("event", "event_graph_departure", "Fictional departure", False),
            ("appearance", "appearance_graph_aster", "Visible participant", True),
            ("event_date", "date_graph_arrival", "Occurrence date", False),
            (
                "event_participant",
                stable_id(
                    "participant",
                    "event_graph_arrival",
                    "entity_graph_aster",
                    "private role for event_graph_arrival",
                ),
                "Fictional attendee",
                False,
            ),
            (
                "event_participant",
                stable_id(
                    "participant",
                    "event_graph_departure",
                    "entity_graph_aster",
                    "private role for event_graph_departure",
                ),
                "Fictional attendee",
                False,
            ),
            (
                "event_relation",
                "relation_graph_arrival_departure",
                "precedes",
                False,
            ),
            (
                "event_evidence",
                "evidence_graph_arrival",
                "Visible arrival evidence",
                True,
            ),
            (
                "event_evidence",
                "evidence_graph_departure",
                "Contradicting departure evidence",
                True,
            ),
        ]
        for ordinal, item in enumerate(graph_objects, start=10):
            self._publish_graph(*item, second=ordinal)

    def _insert_anchor(
        self, link_id: str, claim_id: str, start_ms: int | None, end_ms: int | None
    ) -> None:
        self.connection.execute(
            """
            INSERT INTO claim_catalog_links(
                claim_catalog_link_id, claim_id, evidence_index, source_id,
                recording_id, rendition_id, transcript_revision_id, observation_id,
                start_ms, end_ms, link_state, basis
            ) VALUES(?, ?, 0, 'source_graph_fixture', 'recording_graph_fixture',
                     'rendition_graph_fixture', NULL, NULL, ?, ?, 'reviewed',
                     'private:anchor-basis')
            """,
            (link_id, claim_id, start_ms, end_ms),
        )

    def _publish_base(self, object_type: str, object_id: str, clock: str) -> None:
        self.connection.execute(
            """
            INSERT INTO publication_decisions(
                publication_decision_id, object_type, object_id, decision,
                reviewer_id, review_decision_id, decided_at, basis, public_label
            ) VALUES(?, ?, ?, 'publish', 'reviewer_graph_human', NULL, ?,
                     'Synthetic public anchor publication.', 'Synthetic public anchor')
            """,
            (f"publish_{object_type}_{object_id}", object_type, object_id, f"2026-08-27T{clock}Z"),
        )
        for index, gate in enumerate(("rights", "privacy", "sensitivity"), start=1):
            self.connection.execute(
                """
                INSERT INTO publication_gate_decisions(
                    publication_gate_decision_id, object_type, object_id, gate_kind,
                    decision, reviewer_id, review_decision_id, decided_at, basis
                ) VALUES(?, ?, ?, ?, 'clear', 'reviewer_graph_human', NULL, ?,
                         'Synthetic public anchor gate.')
                """,
                (
                    f"gate_{object_type}_{object_id}_{gate}",
                    object_type,
                    object_id,
                    gate,
                    f"2026-08-27T20:00:0{index + 2}Z",
                ),
            )

    def _publish_graph(
        self,
        object_type: str,
        object_id: str,
        public_label: str,
        directly_perceived: bool,
        *,
        second: int,
        gates: tuple[str, ...] = ("rights", "privacy", "sensitivity"),
    ) -> str:
        review_id = f"review_graph_{second:02d}"
        self.connection.execute(
            """
            INSERT INTO review_decisions(
                review_decision_id, review_task_id, target_type, target_id,
                reviewer_id, decision, decided_at, audio_directly_perceived,
                video_directly_perceived, reviewed_complete_item, basis
            ) VALUES(?, NULL, ?, ?, 'reviewer_graph_human', 'accept', ?, 0, ?, 1,
                     'Complete human review of a synthetic graph fixture.')
            """,
            (
                review_id,
                object_type,
                object_id,
                f"2026-08-27T20:01:{second:02d}Z",
                int(directly_perceived),
            ),
        )
        self.connection.execute(
            """
            INSERT INTO publication_decisions(
                publication_decision_id, object_type, object_id, decision,
                reviewer_id, review_decision_id, decided_at, basis, public_label
            ) VALUES(?, ?, ?, 'publish', 'reviewer_graph_human', ?, ?,
                     'Publish only the reviewed synthetic graph label.', ?)
            """,
            (
                f"publish_graph_{second:02d}",
                object_type,
                object_id,
                review_id,
                f"2026-08-27T20:02:{second:02d}Z",
                public_label,
            ),
        )
        for gate_index, gate in enumerate(gates):
            self.connection.execute(
                """
                INSERT INTO publication_gate_decisions(
                    publication_gate_decision_id, object_type, object_id, gate_kind,
                    decision, reviewer_id, review_decision_id, decided_at, basis
                ) VALUES(?, ?, ?, ?, 'clear', 'reviewer_graph_human', ?, ?,
                         'Synthetic independently recorded graph gate.')
                """,
                (
                    f"gate_graph_{second:02d}_{gate}",
                    object_type,
                    object_id,
                    gate,
                    review_id,
                    f"2026-08-27T20:03:{second + gate_index:02d}Z",
                ),
            )
        return review_id

    def test_reviewed_views_and_hash_tree_emit_only_whitelisted_graph(self) -> None:
        graph = build_graph_release(self.connection)
        self.assertEqual(
            graph["counts"],
            {
                "entities": 1,
                "events": 2,
                "appearances": 1,
                "event_participants": 2,
                "event_dates": 1,
                "event_relations": 1,
                "event_evidence": 2,
            },
        )
        self.assertEqual(graph["entities"][0]["label"], "Fictional Aster")
        self.assertEqual(
            graph["appearances"][0]["anchor"]["rendition"],
            {
                "rendition_id": "rendition_graph_fixture",
                "time_basis": "rendition_media_ms",
            },
        )
        encoded = canonical_json(graph)
        for forbidden in (
            "secret-fixture-handle",
            "Private working",
            "private:",
            "observation_id",
            "transcript_revision_id",
            "identity",
            "biometric",
        ):
            self.assertNotIn(forbidden, encoded)

        output = self.root / "static-graph"
        first = export_graph_release_from_graph(graph, output)
        manifest_bytes = (output / "manifest.json").read_bytes()
        second = export_graph_release_from_graph(graph, output)
        self.assertEqual(first["release_id"], second["release_id"])
        self.assertEqual(manifest_bytes, (output / "manifest.json").read_bytes())
        validated = validate_graph_release(output / "manifest.json")
        self.assertEqual(validated["events"], 2)

    def test_graph_gate_and_anchor_fail_closed(self) -> None:
        appearance_id = "appearance_graph_aster"
        self.connection.execute(
            """
            INSERT INTO publication_gate_decisions(
                publication_gate_decision_id, object_type, object_id, gate_kind,
                decision, reviewer_id, review_decision_id, decided_at, basis
            ) VALUES(
                'gate_graph_appearance_privacy_withhold', 'appearance', ?, 'privacy',
                'withhold', 'reviewer_graph_human', NULL,
                '2026-08-27T20:10:00Z', 'Synthetic privacy withdrawal.'
            )
            """,
            (appearance_id,),
        )
        graph = build_graph_release(self.connection)
        self.assertEqual(graph["appearances"], [])
        self.assertEqual(graph["counts"]["entities"], 1)

        # The base table permits this legacy-shaped interval, but migration 0031's
        # public anchor view requires a known duration and refuses overflow.
        self.connection.execute(
            """
            INSERT INTO appearances(
                appearance_id, entity_id, recording_id, start_ms, end_ms,
                appearance_role, observation_id, review_state
            ) VALUES(
                'appearance_graph_overflow', 'entity_graph_aster',
                'recording_graph_fixture', 9000, 12000, 'private overflow', NULL,
                'reviewed'
            )
            """
        )
        self._insert_anchor(
            "anchor_appearance_overflow",
            "entity_event_map:appearance:appearance_graph_overflow",
            9000,
            12000,
        )
        self._publish_graph(
            "appearance",
            "appearance_graph_overflow",
            "Overflow edge",
            True,
            second=30,
        )
        self.assertIsNone(
            self.connection.execute(
                "SELECT 1 FROM public_graph_appearances WHERE appearance_id = ?",
                ("appearance_graph_overflow",),
            ).fetchone()
        )

    def test_graph_decision_requires_complete_human_review(self) -> None:
        with self.assertRaisesRegex(sqlite3.IntegrityError, "complete human review"):
            self.connection.execute(
                """
                INSERT INTO publication_decisions(
                    publication_decision_id, object_type, object_id, decision,
                    reviewer_id, review_decision_id, decided_at, basis, public_label
                ) VALUES(
                    'unsafe_graph_publish', 'event_date', 'date_graph_arrival',
                    'publish', 'reviewer_graph_human', NULL,
                    '2026-08-27T20:20:00Z', 'Unsafe synthetic attempt.', 'Unsafe label'
                )
                """
            )

    def test_validator_rejects_invalid_calendar_and_relation_cycle(self) -> None:
        graph = build_graph_release(self.connection)
        invalid_date = copy.deepcopy(graph)
        invalid_date["event_dates"][0]["value_start"] = "2026-02-30"
        with self.assertRaisesRegex(ValueError, "calendar"):
            validate_graph_release_shape(invalid_date)

        cyclic = copy.deepcopy(graph)
        cyclic["event_relations"].append(
            {
                "event_relation_id": "z_relation_graph_departure_arrival",
                "from_event_id": "event_graph_departure",
                "to_event_id": "event_graph_arrival",
                "label": "returns to",
            }
        )
        cyclic["counts"]["event_relations"] += 1
        with self.assertRaisesRegex(ValueError, "directed cycle"):
            validate_graph_release_shape(cyclic)

    def test_transcript_backed_event_evidence_is_not_a_v1_graph_dependency(self) -> None:
        created = "2026-08-27T20:00:00Z"
        self.connection.execute(
            """
            INSERT INTO transcript_revisions(
                revision_id, recording_id, rendition_id, revision_kind, origin,
                language, review_state, created_at, metadata_json
            ) VALUES(
                'revision_graph_reviewed', 'recording_graph_fixture',
                'rendition_graph_fixture', 'human_verbatim', 'synthetic', 'en',
                'media_checked', ?, '{}'
            )
            """,
            (created,),
        )
        self.connection.execute(
            """
            INSERT INTO event_evidence(
                event_evidence_id, event_id, recording_id, source_id,
                transcript_revision_id, observation_id, start_ms, end_ms, support_kind
            ) VALUES(
                'evidence_graph_transcript', 'event_graph_arrival',
                'recording_graph_fixture', 'source_graph_fixture',
                'revision_graph_reviewed', NULL, 5000, 6000, 'contextual'
            )
            """
        )
        self.connection.execute(
            """
            INSERT INTO claim_catalog_links(
                claim_catalog_link_id, claim_id, evidence_index, source_id,
                recording_id, rendition_id, transcript_revision_id, observation_id,
                start_ms, end_ms, link_state, basis
            ) VALUES(
                'anchor_evidence_transcript',
                'entity_event_map:event_evidence:evidence_graph_transcript', 0,
                'source_graph_fixture', 'recording_graph_fixture',
                'rendition_graph_fixture', 'revision_graph_reviewed', NULL,
                5000, 6000, 'reviewed', 'private:transcript-anchor')
            """
        )
        self._publish_graph(
            "event_evidence",
            "evidence_graph_transcript",
            "Transcript-backed edge",
            True,
            second=31,
        )
        self.assertIsNone(
            self.connection.execute(
                "SELECT 1 FROM public_graph_event_evidence WHERE event_evidence_id = ?",
                ("evidence_graph_transcript",),
            ).fetchone()
        )

    def test_hash_tampering_is_rejected(self) -> None:
        output = self.root / "tamper"
        result = export_graph_release_from_graph(build_graph_release(self.connection), output)
        manifest = json.loads((output / "manifest.json").read_text(encoding="utf-8"))
        shard = Path(result["release_directory"]) / manifest["graph_shard"]["path"]
        shard.write_bytes(shard.read_bytes() + b" ")
        with self.assertRaisesRegex(ValueError, "byte count or SHA-256"):
            validate_graph_release(output / "manifest.json")


if __name__ == "__main__":
    unittest.main()
