from __future__ import annotations

import copy
import hashlib
import json
import sqlite3
import sys
import tempfile
import unittest
from pathlib import Path


CORPUS_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(CORPUS_ROOT / "src"))

from himr_corpus.db import connect, migrate  # noqa: E402
from himr_corpus.model_registry_admin import (  # noqa: E402
    import_model_registry_manifest,
    validate_model_registry_manifest,
)
from himr_corpus.result_importers import ResultImportError  # noqa: E402


class ModelRegistryAdminTests(unittest.TestCase):
    def setUp(self) -> None:
        work_root = CORPUS_ROOT / "work"
        work_root.mkdir(exist_ok=True)
        self.temporary = tempfile.TemporaryDirectory(prefix="model-registry-", dir=work_root)
        self.root = Path(self.temporary.name)
        self.connection = connect(self.root / "corpus.sqlite3")
        migrate(self.connection)
        self.weights = (self.root / "weights.bin").resolve()
        self.weights.write_bytes(b"exact model weights fixture\n")
        self.manifest = {
            "schema_version": 1,
            "manifest_id": "model-registry-fixture-001",
            "created_at": "2026-08-26T22:00:00Z",
            "registered_by": "test-maintainer",
            "basis": "Local fixture hash and reviewed source/license metadata.",
            "models": [
                {
                    "model_id": "model_fixture_v1",
                    "task": "asr",
                    "name": "Fixture model",
                    "version": "fixture-revision-1",
                    "weights_path": str(self.weights),
                    "weights_sha256": hashlib.sha256(self.weights.read_bytes()).hexdigest(),
                    "weights_byte_count": self.weights.stat().st_size,
                    "license_label": "test-only",
                    "configuration_json": {
                        "source": "local test fixture",
                        "runtime": "whisper.cpp",
                    },
                }
            ],
        }
        self.manifest_path = self._write(self.manifest, "manifest.json")

    def tearDown(self) -> None:
        self.connection.close()
        self.temporary.cleanup()

    def _write(self, value: dict, name: str) -> Path:
        path = self.root / name
        path.write_text(json.dumps(value, indent=2) + "\n", encoding="utf-8")
        return path

    def test_validates_imports_idempotently_and_freezes_model(self) -> None:
        validated = validate_model_registry_manifest(self.manifest_path)
        self.assertEqual(validated["model_count"], 1)
        first = import_model_registry_manifest(self.connection, self.manifest_path)
        second = import_model_registry_manifest(self.connection, self.manifest_path)
        self.assertEqual(first, second)
        self.assertEqual(first["model_ids"], ["model_fixture_v1"])
        snapshot = self.connection.execute(
            "SELECT model_snapshot_json FROM model_registry_manifest_models"
        ).fetchone()[0]
        self.assertEqual(json.loads(snapshot)["weights_byte_count"], self.weights.stat().st_size)
        with self.assertRaisesRegex(sqlite3.IntegrityError, "append-only"):
            self.connection.execute(
                "UPDATE models SET version = 'changed' WHERE model_id = 'model_fixture_v1'"
            )
        with self.assertRaisesRegex(sqlite3.IntegrityError, "append-only"):
            self.connection.execute(
                "DELETE FROM model_registry_manifest_imports WHERE manifest_id = ?",
                (self.manifest["manifest_id"],),
            )

    def test_unknown_tampered_and_conflicting_manifests_leave_no_partial_rows(self) -> None:
        unknown = copy.deepcopy(self.manifest)
        unknown["models"][0]["surprise"] = True
        with self.assertRaisesRegex(ResultImportError, "unknown"):
            import_model_registry_manifest(self.connection, self._write(unknown, "unknown.json"))

        original = self.weights.read_bytes()
        self.weights.write_bytes(b"tampered\n")
        with self.assertRaisesRegex(ResultImportError, "byte_count|SHA-256"):
            import_model_registry_manifest(self.connection, self.manifest_path)
        self.weights.write_bytes(original)
        self.assertEqual(
            self.connection.execute("SELECT count(*) FROM model_registry_manifest_imports").fetchone()[0],
            0,
        )

        conflicting_model = copy.deepcopy(self.manifest)
        conflicting_model["models"].append(
            {
                **conflicting_model["models"][0],
                "model_id": "model_fixture_v2",
                "name": "Different ID, same natural provenance tuple",
            }
        )
        # Seed a natural-key collision before the transaction; the first manifest
        # model is inserted and then the second fails, so rollback is observable.
        conflicting_model["models"][1]["name"] = conflicting_model["models"][0]["name"]
        path = self._write(conflicting_model, "natural-collision.json")
        with self.assertRaisesRegex(ResultImportError, "provenance tuple"):
            import_model_registry_manifest(self.connection, path)
        self.assertEqual(self.connection.execute("SELECT count(*) FROM models").fetchone()[0], 0)
        self.assertEqual(
            self.connection.execute("SELECT count(*) FROM model_registry_manifest_imports").fetchone()[0],
            0,
        )

    def test_manifest_id_reuse_with_different_body_is_rejected(self) -> None:
        import_model_registry_manifest(self.connection, self.manifest_path)
        changed = copy.deepcopy(self.manifest)
        changed["basis"] = "A different claimed basis."
        with self.assertRaisesRegex(ResultImportError, "manifest_id"):
            import_model_registry_manifest(self.connection, self._write(changed, "changed.json"))
        self.assertEqual(
            self.connection.execute("SELECT count(*) FROM model_registry_manifest_imports").fetchone()[0],
            1,
        )


if __name__ == "__main__":
    unittest.main()
