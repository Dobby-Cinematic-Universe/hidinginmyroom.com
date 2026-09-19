from __future__ import annotations

import json
import sqlite3
import sys
import tempfile
import unittest
from pathlib import Path


CORPUS_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(CORPUS_ROOT / "src"))

from himr_corpus.db import connect, migrate  # noqa: E402
from himr_corpus.exporter import build_release  # noqa: E402
from himr_corpus.ids import recording_id, source_id, stable_id  # noqa: E402
from himr_corpus.validation import validate_database  # noqa: E402
from corpus.tests.reviewer_fixtures import (  # noqa: E402
    register_reviewer_fixture,
    set_reviewer_active_fixture,
)


T0 = "2026-08-26T10:00:00Z"
T1 = "2026-08-26T11:00:00Z"
T2 = "2026-08-26T12:00:00Z"


class IdentityClusterSafetyTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temporary = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.connection = connect(Path(self.temporary.name) / "corpus.sqlite3")
        self.addCleanup(self.connection.close)
        migrate(self.connection)

        self.human = "reviewer_identity_human"
        self.robot = "reviewer_identity_policy"
        register_reviewer_fixture(
            self.connection, self.human, "Identity reviewer", "human"
        )
        register_reviewer_fixture(
            self.connection, self.robot, "Identity policy", "automated_policy"
        )

        self.source = source_id("youtube", "youtube_video", "identitytest")
        self.recording = recording_id("youtube:video:identitytest")
        self.connection.execute(
            """
            INSERT INTO sources(
                source_id, platform, source_kind, native_id, canonical_url, title,
                observed_at, access_state, review_state, created_at, updated_at
            ) VALUES(?, 'youtube', 'youtube_video', 'identitytest',
                     'https://www.youtube.com/watch?v=identitytest', 'Identity fixture',
                     ?, 'public', 'reviewed', ?, ?)
            """,
            (self.source, T0, T0, T0),
        )
        self.connection.execute(
            """
            INSERT INTO recordings(
                recording_id, canonical_key, slug, title, date_label, date_year,
                date_basis, duration_ms, recording_type, review_state, created_at, updated_at
            ) VALUES(?, 'youtube:video:identitytest', 'identity-fixture',
                     'Identity fixture', '2026-08-26', 2026, 'test', 10000,
                     'video', 'reviewed', ?, ?)
            """,
            (self.recording, T0, T0),
        )
        self.connection.execute(
            """
            INSERT INTO recording_sources(
                recording_source_id, recording_id, source_id, mapping_role,
                mapping_method, confidence_state
            ) VALUES(?, ?, ?, 'primary', 'test', 'reviewed')
            """,
            (stable_id("rso", self.recording, self.source), self.recording, self.source),
        )

        self.face_1 = self._observation("face_1", "face_track", 0, 1000)
        self.face_2 = self._observation("face_2", "face_track", 1000, 2000)
        self.voice_1 = self._observation("voice_1", "speaker_turn", 0, 1000)
        self.active_1 = self._observation("active_1", "active_speaker", 0, 1000)
        self.connection.executemany(
            "INSERT INTO face_track_observations(observation_id) VALUES(?)",
            ((self.face_1,), (self.face_2,)),
        )
        self.connection.execute(
            """
            INSERT INTO speaker_turn_observations(
                observation_id, speaker_cluster_id, overlap_detected
            ) VALUES(?, 'private-speaker-candidate', 0)
            """,
            (self.voice_1,),
        )
        self.connection.execute(
            """
            INSERT INTO active_speaker_observations(
                observation_id, speaker_turn_observation_id,
                face_track_observation_id, offscreen_or_unknown
            ) VALUES(?, ?, ?, 0)
            """,
            (self.active_1, self.voice_1, self.face_1),
        )

        self.face_model, self.face_run = self._model_and_run("face")
        self.face_cluster = stable_id("icl", "face", "fixture")
        self.connection.execute(
            """
            INSERT INTO identity_clusters(
                identity_cluster_id, modality, implementation_version,
                visibility, created_at
            ) VALUES(?, 'face', 'cluster-contract-v1', 'private', ?)
            """,
            (self.face_cluster, T0),
        )
        self.face_v1 = self._cluster_version(
            self.face_cluster, 1, None, self.face_model, self.face_run
        )
        self.face_m1 = self._membership(self.face_v1, self.face_1, "member")
        self.face_m2 = self._membership(self.face_v1, self.face_2, "member")

    def _observation(
        self, label: str, kind: str, start_ms: int, end_ms: int
    ) -> str:
        observation = stable_id("obs", label)
        self.connection.execute(
            """
            INSERT INTO observations(
                observation_id, observation_kind, recording_id, start_ms, end_ms,
                visibility, review_state, created_at
            ) VALUES(?, ?, ?, ?, ?, 'private', 'machine', ?)
            """,
            (observation, kind, self.recording, start_ms, end_ms, T0),
        )
        return observation

    def _model_and_run(self, modality: str) -> tuple[str, str]:
        model = stable_id("mdl", modality, "identity-test")
        run = stable_id("run", modality, "identity-test")
        self.connection.execute(
            """
            INSERT INTO models(
                model_id, task, name, version, weights_sha256, configuration_json
            ) VALUES(?, ?, ?, '1.0', ?, '{}')
            """,
            (model, f"{modality}_identity_embedding", f"{modality}-fixture", modality[0] * 64),
        )
        self.connection.execute(
            """
            INSERT INTO processing_runs(
                processing_run_id, stage, implementation_version, model_id,
                parameters_json, environment_json, started_at, completed_at, status
            ) VALUES(?, 'identity_cluster', '1.0', ?, '{}', '{}', ?, ?, 'completed')
            """,
            (run, model, T0, T0),
        )
        return model, run

    def _cluster_version(
        self,
        cluster: str,
        version_number: int,
        parent: str | None,
        model: str,
        run: str,
    ) -> str:
        version = stable_id("icv", cluster, version_number)
        self.connection.execute(
            """
            INSERT INTO identity_cluster_versions(
                identity_cluster_version_id, identity_cluster_id, version_number,
                parent_identity_cluster_version_id, primary_model_id,
                primary_processing_run_id, model_snapshot_sha256,
                run_snapshot_sha256, clustering_method, created_at
            ) VALUES(?, ?, ?, ?, ?, ?, ?, ?, 'agglomerative-test', ?)
            """,
            (version, cluster, version_number, parent, model, run, "a" * 64, "b" * 64, T0),
        )
        return version

    def _membership(self, version: str, observation: str, state: str) -> str:
        membership = stable_id("icm", version, observation)
        self.connection.execute(
            """
            INSERT INTO identity_cluster_memberships(
                identity_cluster_membership_id, identity_cluster_version_id,
                observation_id, membership_state, basis, created_at
            ) VALUES(?, ?, ?, ?, 'synthetic identity fixture', ?)
            """,
            (membership, version, observation, state, T0),
        )
        return membership

    def _review(
        self,
        review_id: str,
        target_type: str,
        target_id: str,
        *,
        reviewer: str | None = None,
        decision: str = "accept",
        audio: int = 0,
        video: int = 1,
        complete: int = 1,
        decided_at: str = T1,
    ) -> str:
        reviewer = reviewer or self.human
        self.connection.execute(
            """
            INSERT INTO review_decisions(
                review_decision_id, target_type, target_id, reviewer_id, decision,
                decided_at, audio_directly_perceived, video_directly_perceived,
                reviewed_complete_item, basis
            ) VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, 'synthetic direct-media review')
            """,
            (
                review_id,
                target_type,
                target_id,
                reviewer,
                decision,
                decided_at,
                audio,
                video,
                complete,
            ),
        )
        return review_id

    def _accept_cluster(self, version: str, label: str = "face-v1") -> None:
        review = self._review(
            stable_id("rvd", "cluster", label), "identity_cluster_version", version
        )
        self.connection.execute(
            """
            INSERT INTO identity_cluster_version_review_decisions(
                identity_cluster_version_review_decision_id,
                identity_cluster_version_id, decision, reviewer_id,
                review_decision_id, decided_at, basis
            ) VALUES(?, ?, 'accept', ?, ?, ?, 'human checked all cluster members')
            """,
            (stable_id("icr", label), version, self.human, review, T1),
        )

    def _publish(self, object_type: str, object_id: str, timestamp: str = T2) -> None:
        self.connection.execute(
            """
            INSERT INTO publication_decisions(
                publication_decision_id, object_type, object_id, decision,
                reviewer_id, decided_at, basis
            ) VALUES(?, ?, ?, 'publish', ?, ?, 'synthetic publication review')
            """,
            (stable_id("pub", object_type, object_id), object_type, object_id, self.human, timestamp),
        )
        for gate in ("rights", "privacy", "sensitivity"):
            self.connection.execute(
                """
                INSERT INTO publication_gate_decisions(
                    publication_gate_decision_id, object_type, object_id,
                    gate_kind, decision, reviewer_id, decided_at, basis
                ) VALUES(?, ?, ?, ?, 'clear', ?, ?, 'synthetic completed gate')
                """,
                (
                    stable_id("pgt", object_type, object_id, gate),
                    object_type,
                    object_id,
                    gate,
                    self.human,
                    timestamp,
                ),
            )

    @staticmethod
    def _ordered_pair(left: str, right: str) -> tuple[str, str]:
        return tuple(sorted((left, right)))  # type: ignore[return-value]

    def _machine_cannot_link(
        self, decision_id: str, decided_at: str = T1
    ) -> None:
        left, right = self._ordered_pair(self.face_1, self.face_2)
        self.connection.execute(
            """
            INSERT INTO identity_cannot_link_decisions(
                identity_cannot_link_decision_id, left_observation_id,
                right_observation_id, decision, decision_origin, model_id,
                processing_run_id, decided_at, basis
            ) VALUES(?, ?, ?, 'cannot_link', 'machine', ?, ?, ?,
                     'simultaneous distinct face tracks')
            """,
            (decision_id, left, right, self.face_model, self.face_run, decided_at),
        )

    def test_all_modalities_are_typed_and_versioned(self) -> None:
        with self.assertRaisesRegex(sqlite3.IntegrityError, "does not match modality"):
            self._membership(self.face_v1, self.voice_1, "candidate")

        for modality, observation in (
            ("voice", self.voice_1),
            ("audiovisual", self.active_1),
        ):
            model, run = self._model_and_run(modality)
            cluster = stable_id("icl", modality, "fixture")
            self.connection.execute(
                """
                INSERT INTO identity_clusters(
                    identity_cluster_id, modality, implementation_version,
                    visibility, created_at
                ) VALUES(?, ?, 'cluster-contract-v1', 'private', ?)
                """,
                (cluster, modality, T0),
            )
            version = self._cluster_version(cluster, 1, None, model, run)
            self._membership(version, observation, "member")

        with self.assertRaisesRegex(sqlite3.IntegrityError, "gapless parent chain"):
            self._cluster_version(
                self.face_cluster, 3, self.face_v1, self.face_model, self.face_run
            )

    def test_cannot_link_conflicts_fail_closed_but_later_human_clear_works(self) -> None:
        with self.assertRaisesRegex(sqlite3.IntegrityError, "conflicts with current"):
            self._machine_cannot_link(stable_id("iclx", "blocked-by-v1"))

        face_v2 = self._cluster_version(
            self.face_cluster, 2, self.face_v1, self.face_model, self.face_run
        )
        self._membership(face_v2, self.face_1, "member")
        cannot_link = stable_id("iclx", "face-pair-machine")
        self._machine_cannot_link(cannot_link)
        with self.assertRaisesRegex(sqlite3.IntegrityError, "violates a current cannot-link"):
            self._membership(face_v2, self.face_2, "member")

        left, right = self._ordered_pair(self.face_1, self.face_2)
        same_time_clear = stable_id("iclx", "same-time-clear")
        review = self._review(
            stable_id("rvd", "same-time-clear"),
            "identity_cannot_link",
            same_time_clear,
            decision="reject",
            decided_at=T1,
        )
        with self.assertRaisesRegex(sqlite3.IntegrityError, "genuinely later"):
            self.connection.execute(
                """
                INSERT INTO identity_cannot_link_decisions(
                    identity_cannot_link_decision_id, left_observation_id,
                    right_observation_id, decision, decision_origin, reviewer_id,
                    review_decision_id, decided_at, basis
                ) VALUES(?, ?, ?, 'clear', 'human', ?, ?, ?, 'same-time clear')
                """,
                (same_time_clear, left, right, self.human, review, T1),
            )

        later_clear = stable_id("iclx", "later-clear")
        later_review = self._review(
            stable_id("rvd", "later-clear"),
            "identity_cannot_link",
            later_clear,
            decision="reject",
            decided_at=T2,
        )
        self.connection.execute(
            """
            INSERT INTO identity_cannot_link_decisions(
                identity_cannot_link_decision_id, left_observation_id,
                right_observation_id, decision, decision_origin, reviewer_id,
                review_decision_id, decided_at, basis
            ) VALUES(?, ?, ?, 'clear', 'human', ?, ?, ?, 'later reviewed correction')
            """,
            (later_clear, left, right, self.human, later_review, T2),
        )
        self._membership(face_v2, self.face_2, "member")

    def test_lineage_membership_and_biometric_artifacts_are_immutable_private(self) -> None:
        artifact = stable_id("art", "private-biometric-sentinel")
        self.connection.execute(
            """
            INSERT INTO artifacts(
                artifact_id, processing_run_id, artifact_kind, storage_uri,
                sha256, byte_count, visibility, metadata_json
            ) VALUES(?, ?, 'face_embedding',
                     'research/private/BIOMETRIC_SENTINEL.facevec', ?, 128,
                     'private', '{"sensitivity":"BIOMETRIC_SENTINEL"}')
            """,
            (artifact, self.face_run, "c" * 64),
        )
        biometric = stable_id("bar", artifact)
        self.connection.execute(
            """
            INSERT INTO biometric_artifacts(
                biometric_artifact_id, artifact_id, modality, biometric_kind, created_at
            ) VALUES(?, ?, 'face', 'face_embedding', ?)
            """,
            (biometric, artifact, T0),
        )
        self.connection.execute(
            """
            INSERT INTO identity_cluster_version_artifacts(
                identity_cluster_version_id, biometric_artifact_id, artifact_role
            ) VALUES(?, ?, 'member_embeddings')
            """,
            (self.face_v1, biometric),
        )

        guarded_operations = (
            ("membership", "UPDATE identity_cluster_memberships SET basis='tamper' WHERE identity_cluster_membership_id=?", self.face_m1),
            ("membership", "DELETE FROM identity_cluster_memberships WHERE identity_cluster_membership_id=?", self.face_m1),
            ("version", "UPDATE identity_cluster_versions SET clustering_method='tamper' WHERE identity_cluster_version_id=?", self.face_v1),
            ("cluster", "DELETE FROM identity_clusters WHERE identity_cluster_id=?", self.face_cluster),
            ("model", "UPDATE models SET version='tamper' WHERE model_id=?", self.face_model),
            ("run", "UPDATE processing_runs SET parameters_json='{}' WHERE processing_run_id=?", self.face_run),
            ("artifact", "UPDATE artifacts SET visibility='public' WHERE artifact_id=?", artifact),
            ("artifact", "DELETE FROM artifacts WHERE artifact_id=?", artifact),
        )
        for label, statement, identifier in guarded_operations:
            with self.subTest(label=label), self.assertRaises(sqlite3.IntegrityError):
                self.connection.execute(statement, (identifier,))

        public_artifact = stable_id("art", "bad-public-biometric")
        self.connection.execute(
            """
            INSERT INTO artifacts(
                artifact_id, artifact_kind, storage_uri, sha256,
                byte_count, visibility
            ) VALUES(?, 'face_embedding', 'public/bad.facevec', ?, 10, 'public')
            """,
            (public_artifact, "d" * 64),
        )
        with self.assertRaisesRegex(sqlite3.IntegrityError, "private non-web storage"):
            self.connection.execute(
                """
                INSERT INTO biometric_artifacts(
                    biometric_artifact_id, artifact_id, modality, biometric_kind, created_at
                ) VALUES(?, ?, 'face', 'face_embedding', ?)
                """,
                (stable_id("bar", public_artifact), public_artifact, T0),
            )

        with self.assertRaisesRegex(sqlite3.IntegrityError, "cannot be published"):
            self.connection.execute(
                """
                INSERT INTO publication_decisions(
                    publication_decision_id, object_type, object_id, decision,
                    reviewer_id, decided_at, basis
                ) VALUES(?, 'artifact', ?, 'publish', ?, ?, 'should fail')
                """,
                (stable_id("pub", artifact), artifact, self.human, T2),
            )
        with self.assertRaisesRegex(sqlite3.IntegrityError, "cannot clear"):
            self.connection.execute(
                """
                INSERT INTO publication_gate_decisions(
                    publication_gate_decision_id, object_type, object_id, gate_kind,
                    decision, reviewer_id, decided_at, basis
                ) VALUES(?, 'artifact', ?, 'privacy', 'clear', ?, ?, 'should fail')
                """,
                (stable_id("pgt", artifact), artifact, self.human, T2),
            )

    def test_only_human_reviewed_identity_projection_is_public_and_export_stays_clean(self) -> None:
        robot_review = self._review(
            stable_id("rvd", "robot-cluster"),
            "identity_cluster_version",
            self.face_v1,
            reviewer=self.robot,
        )
        with self.assertRaisesRegex(sqlite3.IntegrityError, "active human"):
            self.connection.execute(
                """
                INSERT INTO identity_cluster_version_review_decisions(
                    identity_cluster_version_review_decision_id,
                    identity_cluster_version_id, decision, reviewer_id,
                    review_decision_id, decided_at, basis
                ) VALUES(?, ?, 'accept', ?, ?, ?, 'automated acceptance must fail')
                """,
                (stable_id("icr", "robot"), self.face_v1, self.robot, robot_review, T1),
            )

        self._accept_cluster(self.face_v1)
        entity = stable_id("ent", "identity-sentinel-person")
        self.connection.execute(
            """
            INSERT INTO entities(
                entity_id, entity_type, canonical_label, slug, visibility,
                review_state, metadata_json, created_at
            ) VALUES(?, 'person', 'Reviewed public label', 'reviewed-public-label',
                     'public_candidate', 'reviewed', '{}', ?)
            """,
            (entity, T0),
        )
        assertion = stable_id("ias", self.face_v1, entity)
        self.connection.execute(
            """
            INSERT INTO identity_assertion_subjects(
                identity_assertion_id, identity_cluster_version_id,
                entity_id, created_at
            ) VALUES(?, ?, ?, ?)
            """,
            (assertion, self.face_v1, entity, T0),
        )

        indirect_review = self._review(
            stable_id("rvd", "identity-indirect"),
            "identity_assertion",
            assertion,
            audio=1,
            video=0,
        )
        with self.assertRaisesRegex(sqlite3.IntegrityError, "direct-media"):
            self.connection.execute(
                """
                INSERT INTO identity_assertion_decisions(
                    identity_assertion_decision_id, identity_assertion_id,
                    decision, reviewer_id, review_decision_id, decided_at, basis
                ) VALUES(?, ?, 'assert', ?, ?, ?, 'audio-only review of face')
                """,
                (
                    stable_id("iad", "identity-indirect"),
                    assertion,
                    self.human,
                    indirect_review,
                    T1,
                ),
            )

        direct_review = self._review(
            stable_id("rvd", "identity-direct"),
            "identity_assertion",
            assertion,
            video=1,
        )
        self.connection.execute(
            """
            INSERT INTO identity_assertion_decisions(
                identity_assertion_decision_id, identity_assertion_id,
                decision, reviewer_id, review_decision_id, decided_at, basis
            ) VALUES(?, ?, 'assert', ?, ?, ?, 'face checked against source media')
            """,
            (stable_id("iad", "identity-direct"), assertion, self.human, direct_review, T1),
        )
        with self.assertRaisesRegex(
            sqlite3.IntegrityError,
            "review decisions are append-only|immutable after identity citation",
        ):
            self.connection.execute(
                "UPDATE review_decisions SET video_directly_perceived=0 WHERE review_decision_id=?",
                (direct_review,),
            )

        artifact = stable_id("art", "export-leak-sentinel")
        self.connection.execute(
            """
            INSERT INTO artifacts(
                artifact_id, processing_run_id, artifact_kind, storage_uri,
                sha256, byte_count, visibility, metadata_json
            ) VALUES(?, ?, 'face_embedding',
                     'research/private/BIOMETRIC_EXPORT_SENTINEL.facevec', ?, 128,
                     'private', '{"secret":"BIOMETRIC_EXPORT_SENTINEL"}')
            """,
            (artifact, self.face_run, "e" * 64),
        )
        biometric = stable_id("bar", artifact)
        self.connection.execute(
            """
            INSERT INTO biometric_artifacts(
                biometric_artifact_id, artifact_id, modality, biometric_kind, created_at
            ) VALUES(?, ?, 'face', 'face_embedding', ?)
            """,
            (biometric, artifact, T0),
        )
        self.connection.execute(
            """
            INSERT INTO identity_cluster_version_artifacts(
                identity_cluster_version_id, biometric_artifact_id, artifact_role
            ) VALUES(?, ?, 'private-centroid')
            """,
            (self.face_v1, biometric),
        )

        for object_type, object_id in (
            ("source", self.source),
            ("recording", self.recording),
            ("entity", entity),
            ("identity_assertion", assertion),
        ):
            self._publish(object_type, object_id)

        public_mapping = self.connection.execute(
            "SELECT * FROM public_identity_assertions"
        ).fetchone()
        self.assertIsNotNone(public_mapping)
        self.assertEqual(set(public_mapping.keys()), {
            "identity_assertion_id", "entity_id", "modality", "decided_at"
        })
        set_reviewer_active_fixture(
            self.connection,
            self.human,
            False,
            changed_at="2026-08-26T12:30:00Z",
            sequence_label="identity-view-deactivate",
        )
        self.assertEqual(
            self.connection.execute(
                "SELECT count(*) FROM public_identity_assertions"
            ).fetchone()[0],
            0,
        )
        set_reviewer_active_fixture(
            self.connection,
            self.human,
            True,
            changed_at="2026-08-26T12:31:00Z",
            sequence_label="identity-view-reactivate",
        )

        release = build_release(self.connection)
        rendered = json.dumps(release, sort_keys=True)
        self.assertEqual(release["counts"]["recordings"], 1)
        for private_value in (
            "BIOMETRIC_EXPORT_SENTINEL",
            artifact,
            biometric,
            self.face_cluster,
            self.face_v1,
            assertion,
            self.face_1,
        ):
            self.assertNotIn(private_value, rendered)
        status = validate_database(self.connection)
        self.assertEqual(status["biometric_artifacts"], 1)
        self.assertEqual(status["public_identity_assertions"], 1)


if __name__ == "__main__":
    unittest.main()
