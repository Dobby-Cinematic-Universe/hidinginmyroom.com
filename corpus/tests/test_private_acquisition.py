from __future__ import annotations

import hashlib
import json
import os
import sqlite3
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path


CORPUS_ROOT = Path(__file__).resolve().parents[1]
REPOSITORY_ROOT = CORPUS_ROOT.parent
sys.path.insert(0, str(CORPUS_ROOT / "src"))

from himr_corpus.db import connect, migrate  # noqa: E402
from himr_corpus.exporter import build_release  # noqa: E402
from himr_corpus.ids import source_id, stable_id  # noqa: E402
from himr_corpus.importers import canonical_json  # noqa: E402
from himr_corpus.private_acquisition import (  # noqa: E402
    PrivateAcquisitionError,
    build_private_acquisition_seal_plan,
    validate_private_acquisition_seal_plan,
    validate_private_acquisition_seal_receipt,
)
from himr_corpus.result_importers import (  # noqa: E402
    ResultImportError,
    import_acquisition_result,
)
from himr_corpus.validation import validate_database  # noqa: E402
from corpus.tests.reviewer_fixtures import register_reviewer_fixture  # noqa: E402


POLICY = {
    "storage_scope": "private_canonical_cache",
    "publication_disposition": "never_publish",
    "publication_authority": "none",
    "basis": "Explicit local-custody instruction; these bytes must not be published.",
}
OBSERVED_AT = "2026-08-27T12:00:01Z"


def producer_id(prefix: str, *parts: object) -> str:
    digest = hashlib.sha256(canonical_json(list(parts)).encode("utf-8")).hexdigest()
    return f"{prefix}_{digest[:32]}"


class PrivateAcquisitionPolicyTests(unittest.TestCase):
    def setUp(self) -> None:
        work_root = CORPUS_ROOT / "work"
        work_root.mkdir(exist_ok=True)
        self.temporary = tempfile.TemporaryDirectory(
            prefix="private-acquisition-test-", dir=work_root
        )
        self.root = Path(self.temporary.name)
        self.artifacts = self.root / "portable-private-tree"
        self.artifacts.mkdir()
        self.work_order_path = self.artifacts / "work-order.json"
        self.result_path = self.artifacts / "result.json"
        self.media_path = self.artifacts / "media.bin"
        media_bytes = b"private local media fixture bytes\x00\x01"
        self.media_path.write_bytes(media_bytes)
        digest = hashlib.sha256(media_bytes).hexdigest()
        media_id = f"media_sha256_{digest}"
        self.producer_source_id = producer_id(
            "source", "local_test", "video", "private-local-001"
        )
        source = {
            "platform": "local_test",
            "source_kind": "video",
            "native_id": "private-local-001",
            "canonical_url": None,
            "title": "Private local fixture",
            "published_at": None,
            "access_state": "unknown",
        }
        self.work_order = {
            "schema_version": 1,
            "job_id": "private-local-policy-001",
            "adapter": "local_file",
            "source": source,
            "adapter_config": {
                "path": str((self.root / "source-outside-cache.bin").resolve()),
                "expected_sha256": digest,
                "expected_byte_count": len(media_bytes),
            },
            "output": {"root": str(self.artifacts.resolve())},
            "limits": {
                "max_job_bytes": 1024,
                "global_cache_cap_bytes": 4096,
                "free_space_floor_bytes": 0,
            },
            "handling_policy": POLICY,
        }
        self.work_order_path.write_text(
            json.dumps(self.work_order, sort_keys=True), encoding="utf-8"
        )
        work_order_sha256 = hashlib.sha256(
            canonical_json(self.work_order).encode("utf-8")
        ).hexdigest()
        probe = {
            "media": {
                "media_id": media_id,
                "sha256": digest,
                "byte_count": len(media_bytes),
            },
            "format": {"duration_ms": 1000},
        }
        media_uri = self.media_path.resolve().as_uri()
        self.result = {
            "schema_version": 1,
            "job_id": "private-local-policy-001",
            "adapter": "local_file",
            "status": "completed",
            "dry_run": False,
            "reused": False,
            "work_order_sha256": work_order_sha256,
            "handling_policy": POLICY,
            "started_at": "2026-08-27T12:00:00Z",
            "completed_at": OBSERVED_AT,
            "duration_ms": 1000,
            "source": source,
            "limits": self.work_order["limits"],
            "capacity_before": {
                "filesystem_path": str(self.artifacts),
                "managed_bytes": 0,
                "free_bytes": 4096,
                "reserve_bytes": len(media_bytes),
                "projected_managed_bytes": len(media_bytes),
                "projected_free_bytes": 4096 - len(media_bytes),
                "global_cache_cap_bytes": 4096,
                "free_space_floor_bytes": 0,
            },
            "capacity_after": {
                "filesystem_path": str(self.artifacts),
                "managed_bytes": len(media_bytes),
                "free_bytes": 4096 - len(media_bytes),
                "reserve_bytes": 0,
                "projected_managed_bytes": len(media_bytes),
                "projected_free_bytes": 4096 - len(media_bytes),
                "global_cache_cap_bytes": 4096,
                "free_space_floor_bytes": 0,
            },
            "commands": [["local-copy", "source", "stage"]],
            "source_observation": {},
            "selected_remote_metadata": {},
            "admission": {
                "media_id": media_id,
                "sha256": digest,
                "byte_count": len(media_bytes),
                "path": str(self.media_path.resolve()),
                "storage_uri": media_uri,
                "normalized_probe": probe,
            },
            "catalog_records": {
                "sources": [
                    {
                        "source_id": self.producer_source_id,
                        **source,
                        "parent_source_id": None,
                        "historical_url": None,
                        "observed_at": OBSERVED_AT,
                        "review_state": "metadata_only",
                        "metadata_json": {
                            "acquisition_adapter": "local_file",
                            "selected_remote_metadata": {},
                            "handling_policy": POLICY,
                        },
                        "created_at": OBSERVED_AT,
                        "updated_at": OBSERVED_AT,
                    }
                ],
                "media_objects": [
                    {
                        "media_id": media_id,
                        "sha256": digest,
                        "byte_count": len(media_bytes),
                        "media_kind": "video",
                        "mime_type": "video/mp4",
                        "container": "mp4",
                        "duration_ms": 1000,
                        "ffprobe_json": probe,
                        "first_cataloged_at": OBSERVED_AT,
                        "integrity_state": "verified",
                    }
                ],
                "media_locations": [
                    {
                        "media_location_id": producer_id(
                            "media_location", media_id, media_uri
                        ),
                        "media_id": media_id,
                        "storage_uri": media_uri,
                        "storage_class": "local_hot_cache",
                        "verified_at": OBSERVED_AT,
                        "is_primary": 1,
                    }
                ],
                "media_sources": [
                    {
                        "media_source_id": producer_id(
                            "media_source", media_id, self.producer_source_id
                        ),
                        "media_id": media_id,
                        "source_id": self.producer_source_id,
                        "retrieved_at": OBSERVED_AT,
                        "retrieval_tool": "private-fixture",
                        "retrieval_tool_version": "1",
                        "source_snapshot_id": None,
                    }
                ],
            },
            "result_path": str(self.result_path.resolve()),
            "errors": [],
        }
        self.result_path.write_text(
            json.dumps(self.result, sort_keys=True), encoding="utf-8"
        )
        self.connection = connect(self.root / "corpus.sqlite3")
        migrate(self.connection)

    def tearDown(self) -> None:
        self.connection.close()
        self.temporary.cleanup()

    def _plan_and_receipt(self) -> tuple[dict, dict, Path]:
        plan = build_private_acquisition_seal_plan(
            self.artifacts,
            work_order_path="work-order.json",
            result_path="result.json",
            media_path="media.bin",
        )
        self.assertFalse(plan["source_byte_identity_claimed"])
        self.assertNotIn(str(self.artifacts), json.dumps(plan))
        with self.assertRaisesRegex(
            PrivateAcquisitionError, "not owner-only"
        ):
            validate_private_acquisition_seal_plan(self.artifacts, plan)
        os.chmod(self.artifacts, 0o700)
        for path in (self.work_order_path, self.result_path, self.media_path):
            os.chmod(path, 0o600)
        receipt = validate_private_acquisition_seal_plan(self.artifacts, plan)
        validate_private_acquisition_seal_receipt(self.artifacts, receipt)
        receipt_path = self.root / "seal-receipt.json"
        receipt_path.write_text(json.dumps(receipt, sort_keys=True), encoding="utf-8")
        os.chmod(receipt_path, 0o600)
        return plan, receipt, receipt_path

    def test_review_only_seal_and_durable_policy_import(self) -> None:
        plan, receipt, receipt_path = self._plan_and_receipt()
        for value, filename in ((plan, "plan.json"), (receipt, "receipt.json")):
            path = self.root / filename
            path.write_text(json.dumps(value), encoding="utf-8")
            completed = subprocess.run(
                [
                    sys.executable,
                    str(REPOSITORY_ROOT / "scripts/validate-json-contracts.py"),
                    "--validate",
                    "corpus/schemas/private-acquisition-seal.schema.json",
                    str(path),
                ],
                cwd=REPOSITORY_ROOT,
                check=False,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                text=True,
            )
            self.assertEqual(completed.returncode, 0, completed.stderr)

        first = import_acquisition_result(
            self.connection,
            self.result_path,
            private_artifact_root=self.artifacts,
            private_seal_receipt_path=receipt_path,
        )
        self.assertEqual(first["acquisition_handling_restrictions"], 1)
        restriction = self.connection.execute(
            "SELECT * FROM acquisition_handling_restrictions"
        ).fetchone()
        self.assertEqual(restriction["publication_disposition"], "never_publish")
        self.assertEqual(restriction["publication_authority"], "none")
        self.assertEqual(restriction["seal_plan_sha256"], receipt["plan_sha256"])
        effective = {
            (row["object_type"], row["object_id"])
            for row in self.connection.execute(
                "SELECT * FROM effective_acquisition_handling_restrictions"
            )
        }
        self.assertIn(("source", first["source_id"]), effective)
        self.assertIn(("media", first["media_id"]), effective)
        self.assertEqual(
            first,
            import_acquisition_result(
                self.connection,
                self.result_path,
                private_artifact_root=self.artifacts,
                private_seal_receipt_path=receipt_path,
            ),
        )
        validate_database(self.connection)

    def test_policy_cannot_be_dropped_and_publication_propagates_closed(self) -> None:
        _, _, receipt_path = self._plan_and_receipt()
        canonical_source = source_id("local_test", "video", "private-local-001")
        reviewer = "reviewer_private_policy_fixture"
        register_reviewer_fixture(
            self.connection, reviewer, "Private policy fixture"
        )
        self.connection.execute(
            """
            INSERT INTO publication_decisions(
                publication_decision_id, object_type, object_id, decision,
                reviewer_id, decided_at, basis
            ) VALUES('preexisting-private-source-publish', 'source', ?, 'publish',
                     ?, '2026-08-27T11:00:00Z', 'synthetic pre-policy conflict')
            """,
            (canonical_source, reviewer),
        )
        imported = import_acquisition_result(
            self.connection,
            self.result_path,
            private_artifact_root=self.artifacts,
            private_seal_receipt_path=receipt_path,
        )
        with self.assertRaisesRegex(
            PrivateAcquisitionError, "conflicts with private acquisition restriction"
        ):
            build_release(self.connection)
        with self.assertRaisesRegex(
            sqlite3.IntegrityError, "forbids publication"
        ):
            self.connection.execute(
                """
                INSERT INTO publication_decisions(
                    publication_decision_id, object_type, object_id, decision,
                    reviewer_id, decided_at, basis
                ) VALUES('later-private-source-publish', 'source', ?, 'publish',
                         ?, '2026-08-27T13:00:00Z', 'must be rejected')
                """,
                (imported["source_id"], reviewer),
            )
        with self.assertRaisesRegex(
            sqlite3.IntegrityError, "forbids gate clearance"
        ):
            self.connection.execute(
                """
                INSERT INTO publication_gate_decisions(
                    publication_gate_decision_id, object_type, object_id,
                    gate_kind, decision, reviewer_id, decided_at, basis
                ) VALUES('later-private-source-rights-clear', 'source', ?,
                         'rights', 'clear', ?, '2026-08-27T13:00:01Z',
                         'must be rejected')
                """,
                (imported["source_id"], reviewer),
            )

        recording = "rec_" + "1" * 32
        self.connection.execute(
            """
            INSERT INTO recordings(
                recording_id, canonical_key, slug, title, date_basis,
                recording_type, review_state, metadata_json, created_at, updated_at
            ) VALUES(?, 'private-policy-recording', 'private-policy-recording',
                     'Private policy recording', 'test', 'video', 'reviewed', '{}',
                     ?, ?)
            """,
            (recording, OBSERVED_AT, OBSERVED_AT),
        )
        self.connection.execute(
            """
            INSERT INTO recording_sources(
                recording_source_id, recording_id, source_id, mapping_role,
                mapping_method, confidence_state, metadata_json
            ) VALUES(?, ?, ?, 'complete_source', 'test', 'reviewed', '{}')
            """,
            (stable_id("rso", recording, imported["source_id"]), recording, imported["source_id"]),
        )
        revision = "private-policy-revision"
        self.connection.execute(
            """
            INSERT INTO transcript_revisions(
                revision_id, recording_id, revision_kind, origin, language,
                review_state, created_at, metadata_json
            ) VALUES(?, ?, 'human_verbatim', 'test', 'en', 'media_checked', ?, '{}')
            """,
            (revision, recording, OBSERVED_AT),
        )
        propagated = {
            (row["object_type"], row["object_id"])
            for row in self.connection.execute(
                "SELECT * FROM effective_acquisition_handling_restrictions"
            )
        }
        self.assertIn(("recording", recording), propagated)
        self.assertIn(("transcript_revision", revision), propagated)

        dropped = json.loads(json.dumps(self.result))
        dropped.pop("handling_policy")
        dropped["catalog_records"]["sources"][0]["metadata_json"].pop(
            "handling_policy"
        )
        dropped_path = self.root / "policy-dropped-result.json"
        dropped["result_path"] = str(dropped_path)
        dropped_path.write_text(json.dumps(dropped, sort_keys=True), encoding="utf-8")
        with self.assertRaisesRegex(
            ResultImportError, "omits an effective source/media handling_policy"
        ):
            import_acquisition_result(self.connection, dropped_path)


if __name__ == "__main__":
    unittest.main()
