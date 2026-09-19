from __future__ import annotations

import copy
import hashlib
import json
import os
import shutil
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path


CORPUS_ROOT = Path(__file__).resolve().parents[1]
REPOSITORY_ROOT = CORPUS_ROOT.parent
sys.path.insert(0, str(CORPUS_ROOT / "src"))

from himr_corpus.db import connect, migrate  # noqa: E402
from himr_corpus.cli import build_parser  # noqa: E402
from himr_corpus.ids import recording_id, source_id, stable_id  # noqa: E402
from himr_corpus.result_importers import (  # noqa: E402
    ResultImportError,
    import_acquisition_result,
    import_preprocess_result,
)
from himr_corpus.sparse_frame_result_importer import (  # noqa: E402
    import_sparse_frame_result,
    validate_sparse_frame_result_file,
)


OBSERVED_AT = "2026-08-26T20:00:00Z"
NATIVE_ID = "sparse-frame-import-001"


def run_json(command: list[str]) -> dict:
    completed = subprocess.run(
        command,
        cwd=REPOSITORY_ROOT,
        stdin=subprocess.DEVNULL,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
        encoding="utf-8",
        errors="replace",
        check=False,
    )
    if completed.returncode:
        raise AssertionError(
            f"producer failed with exit {completed.returncode}:\n{completed.stderr}"
        )
    return json.loads(completed.stdout)


def write_json(path: Path, value: object) -> None:
    path.write_text(json.dumps(value, sort_keys=True, indent=2) + "\n", encoding="utf-8")


def make_writable_and_remove(path: Path) -> None:
    if not path.exists():
        return
    for child in sorted(path.rglob("*"), key=lambda item: len(item.parts), reverse=True):
        try:
            child.chmod(0o755 if child.is_dir() else 0o644)
        except FileNotFoundError:
            pass
    path.chmod(0o755)
    shutil.rmtree(path)


@unittest.skipUnless(
    shutil.which("ffmpeg") and shutil.which("ffprobe"),
    "FFmpeg and FFprobe are required for sparse-frame admission fixtures",
)
class SparseFrameResultImporterTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        work = CORPUS_ROOT / "work"
        work.mkdir(exist_ok=True)
        cls.temporary = tempfile.TemporaryDirectory(prefix="sparse-frame-import-", dir=work)
        cls.root = Path(cls.temporary.name)
        cls.source_path = cls.root / "source.mp4"
        completed = subprocess.run(
            [
                shutil.which("ffmpeg") or "ffmpeg",
                "-hide_banner", "-nostdin", "-loglevel", "error",
                "-f", "lavfi", "-i", "color=c=black:size=320x180:rate=25:duration=4",
                "-an", "-c:v", "libx264", "-preset", "ultrafast",
                "-pix_fmt", "yuv420p", str(cls.source_path),
            ],
            stdin=subprocess.DEVNULL,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.PIPE,
            check=False,
        )
        if completed.returncode:
            raise unittest.SkipTest("host FFmpeg cannot create the admission fixture")

        acquisition_order = {
            "schema_version": 1,
            "job_id": "sparse-frame-import-acquisition",
            "adapter": "local_file",
            "source": {
                "platform": "local_test",
                "source_kind": "video",
                "native_id": NATIVE_ID,
                "canonical_url": None,
                "title": "Sparse-frame importer fixture",
                "published_at": None,
                "access_state": "unknown",
            },
            "adapter_config": {
                "path": str(cls.source_path),
                "expected_sha256": None,
                "expected_byte_count": None,
            },
            "output": {"root": str(cls.root / "acquired")},
            "limits": {
                "max_job_bytes": 32 * 1024 * 1024,
                "global_cache_cap_bytes": 128 * 1024 * 1024,
                "free_space_floor_bytes": 0,
            },
        }
        acquisition_order_path = cls.root / "acquisition-order.json"
        write_json(acquisition_order_path, acquisition_order)
        cls.acquisition = run_json([
            sys.executable,
            str(REPOSITORY_ROOT / "acquisition/acquire.py"),
            "run", "--work-order", str(acquisition_order_path),
        ])
        cls.acquisition_path = Path(cls.acquisition["result_path"])

        profile = json.loads(
            (REPOSITORY_ROOT / "pipeline/profiles/cpu-balanced-v1.json").read_text(encoding="utf-8")
        )
        preprocess_order = {
            "schema_version": 1,
            "job_id": "sparse-frame-import-preprocess",
            "source": {
                "path": cls.acquisition["admission"]["path"],
                "expected_sha256": cls.acquisition["admission"]["sha256"],
                "first_cataloged_at": cls.acquisition["completed_at"],
            },
            "output": {"root": str(cls.root / "processed")},
            "operations": {"probe": True, "audio_flac": True, "proxy": True, "routing": True},
            "profile": profile,
        }
        preprocess_order_path = cls.root / "preprocess-order.json"
        write_json(preprocess_order_path, preprocess_order)
        cls.preprocess = run_json([
            sys.executable,
            str(REPOSITORY_ROOT / "pipeline/media_preprocess.py"),
            "run", "--work-order", str(preprocess_order_path),
        ])
        cls.preprocess_path = Path(cls.preprocess["result_path"])

        sparse_order = run_json([
            sys.executable,
            str(REPOSITORY_ROOT / "pipeline/sparse_frame_router.py"),
            "create-work-order",
            "--job-id", "sparse-frame-import-routing",
            "--preprocess-result", str(cls.preprocess_path),
            "--output-root", str(cls.root / "vision"),
        ])
        # Keep this catalog-boundary suite small. Scene-selection behavior is covered
        # by the producer suite; start plus periodic frames are sufficient to test
        # catalog admission without selecting a transition on the terminal frame.
        sparse_order["sampling"] = {
            "include_recording_start": True,
            "scene_changes": {"enabled": False, "max_frames": 0, "offset_ms": 0},
            "periodic": {"enabled": True, "interval_ms": 1_000, "max_frames": 2},
            "min_separation_ms": 100,
        }
        sparse_order["limits"]["max_frames"] = 3
        sparse_order_path = cls.root / "sparse-order.json"
        write_json(sparse_order_path, sparse_order)
        cls.sparse = run_json([
            sys.executable,
            str(REPOSITORY_ROOT / "pipeline/sparse_frame_router.py"),
            "run", "--work-order", str(sparse_order_path),
        ])
        cls.sparse_path = Path(cls.sparse["result_path"])

    @classmethod
    def tearDownClass(cls) -> None:
        root = getattr(cls, "root", None)
        if root is not None:
            make_writable_and_remove(root)
        temporary = getattr(cls, "temporary", None)
        if temporary is not None:
            temporary.cleanup()

    def setUp(self) -> None:
        self.database_root = self.root / f"db-{self._testMethodName}"
        self.database_root.mkdir()
        self.connection = connect(self.database_root / "corpus.sqlite3")
        migrate(self.connection)
        self.source_id = source_id("local_test", "video", NATIVE_ID)
        self.recording_id = recording_id(f"synthetic:local_test:video:{NATIVE_ID}")
        batch_id = stable_id("imp", "sparse-frame-import-test-seed", NATIVE_ID)
        input_sha = hashlib.sha256(NATIVE_ID.encode("utf-8")).hexdigest()
        self.connection.execute(
            "INSERT INTO import_batches(import_batch_id, importer_name, importer_version, input_sha256, started_at, completed_at, status, statistics_json) VALUES(?, 'sparse-frame-test-seed', '1', ?, ?, ?, 'completed', '{}')",
            (batch_id, input_sha, OBSERVED_AT, OBSERVED_AT),
        )
        self.connection.execute(
            "INSERT INTO sources(source_id, platform, source_kind, native_id, title, observed_at, access_state, review_state, metadata_json, created_by_import_batch_id, created_at, updated_at) VALUES(?, 'local_test', 'video', ?, 'Fixture', ?, 'public', 'metadata_only', '{}', ?, ?, ?)",
            (self.source_id, NATIVE_ID, OBSERVED_AT, batch_id, OBSERVED_AT, OBSERVED_AT),
        )
        self.connection.execute(
            "INSERT INTO recordings(recording_id, canonical_key, slug, title, date_basis, recording_type, review_state, metadata_json, created_at, updated_at) VALUES(?, ?, ?, 'Fixture', 'test', 'video', 'metadata_only', '{}', ?, ?)",
            (
                self.recording_id,
                f"synthetic:{NATIVE_ID}",
                f"synthetic-{NATIVE_ID}",
                OBSERVED_AT,
                OBSERVED_AT,
            ),
        )
        self.connection.execute(
            "INSERT INTO recording_sources(recording_source_id, recording_id, source_id, mapping_role, mapping_method, confidence_state, metadata_json) VALUES(?, ?, ?, 'complete_source', 'test_fixture', 'reviewed', '{}')",
            (stable_id("rso", self.recording_id, self.source_id, "complete_source"), self.recording_id, self.source_id),
        )
        import_acquisition_result(self.connection, self.acquisition_path)
        import_preprocess_result(self.connection, self.preprocess_path)

    def tearDown(self) -> None:
        self.connection.close()
        shutil.rmtree(self.database_root)

    def _rewrite_result(self, value: dict) -> bytes:
        original = self.sparse_path.read_bytes()
        self.sparse_path.chmod(0o644)
        write_json(self.sparse_path, value)
        self.sparse_path.chmod(0o444)
        return original

    def _restore_result(self, body: bytes) -> None:
        self.sparse_path.chmod(0o644)
        self.sparse_path.write_bytes(body)
        self.sparse_path.chmod(0o444)

    def test_validation_and_import_are_private_transactional_and_idempotent(self) -> None:
        validated = validate_sparse_frame_result_file(self.sparse_path)
        self.assertEqual(validated["frame_count"], len(self.sparse["frames"]))
        first = import_sparse_frame_result(self.connection, self.sparse_path)
        second = import_sparse_frame_result(self.connection, self.sparse_path)
        self.assertEqual(first, second)
        self.assertEqual(first["artifacts"], len(self.sparse["frames"]))
        self.assertEqual(first["observations"], len(self.sparse["frames"]))
        self.assertEqual(
            self.connection.execute(
                "SELECT count(*) FROM artifacts WHERE artifact_kind = 'sparse_frame_png' AND visibility = 'private'"
            ).fetchone()[0],
            len(self.sparse["frames"]),
        )
        self.assertEqual(
            self.connection.execute(
                "SELECT count(*) FROM observations WHERE observation_kind = 'sparse_frame_routing_candidate' AND visibility = 'private' AND review_state = 'machine'"
            ).fetchone()[0],
            len(self.sparse["frames"]),
        )
        self.assertEqual(self.connection.execute("SELECT count(*) FROM ocr_observations").fetchone()[0], 0)
        self.assertEqual(self.connection.execute("SELECT count(*) FROM face_track_observations").fetchone()[0], 0)
        self.assertEqual(self.connection.execute("SELECT count(*) FROM identity_assertions").fetchone()[0], 0)
        self.assertEqual(self.connection.execute("SELECT count(*) FROM publication_decisions").fetchone()[0], 0)
        for row in self.connection.execute(
            "SELECT metadata_json FROM observations WHERE observation_kind = 'sparse_frame_routing_candidate'"
        ):
            metadata = json.loads(row[0])
            self.assertTrue(metadata["routing_only"])
            self.assertEqual(metadata["ocr_routing"]["evaluation_state"], "not_evaluated")
            self.assertEqual(metadata["ocr_routing"]["text_presence"], "unknown")
            self.assertNotIn("raw_text", metadata)
            self.assertNotIn("person", metadata)
            self.assertNotIn("identity", metadata)

    def test_cli_registers_read_only_validation_and_catalog_import(self) -> None:
        parser = build_parser()
        validate_args = parser.parse_args(
            ["validate-sparse-frame-result", "--result", str(self.sparse_path)]
        )
        import_args = parser.parse_args(
            [
                "import-sparse-frame-result",
                "--db",
                str(self.database_root / "cli.sqlite3"),
                "--result",
                str(self.sparse_path),
            ]
        )
        self.assertEqual(validate_args.command, "validate-sparse-frame-result")
        self.assertFalse(hasattr(validate_args, "db"))
        self.assertEqual(import_args.command, "import-sparse-frame-result")

    def test_tampered_png_fails_before_any_sparse_catalog_write(self) -> None:
        artifact_path = Path(self.sparse["artifacts"][0]["path"])
        original = artifact_path.read_bytes()
        try:
            artifact_path.chmod(0o644)
            artifact_path.write_bytes(original + b"tamper")
            artifact_path.chmod(0o444)
            with self.assertRaisesRegex(ResultImportError, "byte_count|SHA-256|PNG"):
                import_sparse_frame_result(self.connection, self.sparse_path)
            self.assertEqual(
                self.connection.execute(
                    "SELECT count(*) FROM processing_runs WHERE stage = 'sparse_frame_router'"
                ).fetchone()[0],
                0,
            )
        finally:
            artifact_path.chmod(0o644)
            artifact_path.write_bytes(original)
            artifact_path.chmod(0o444)

    def test_unknown_field_and_forged_timestamp_are_rejected(self) -> None:
        unknown = copy.deepcopy(self.sparse)
        unknown["unexpected"] = True
        original = self._rewrite_result(unknown)
        try:
            with self.assertRaisesRegex(ResultImportError, "unknown"):
                validate_sparse_frame_result_file(self.sparse_path)
        finally:
            self._restore_result(original)

    def test_duplicate_keys_in_sparse_result_are_rejected(self) -> None:
        original = self.sparse_path.read_bytes()
        self.sparse_path.chmod(0o644)
        self.sparse_path.write_bytes(
            original.replace(b"{\n", b'{\n  "schema_version": 1,\n', 1)
        )
        self.sparse_path.chmod(0o444)
        try:
            with self.assertRaisesRegex(ResultImportError, "duplicate JSON key"):
                validate_sparse_frame_result_file(self.sparse_path)
        finally:
            self._restore_result(original)

        forged = copy.deepcopy(self.sparse)
        forged["frames"][0]["timestamp"]["timestamp_ms"] += 1
        original = self._rewrite_result(forged)
        try:
            with self.assertRaisesRegex(ResultImportError, "timestamp"):
                validate_sparse_frame_result_file(self.sparse_path)
        finally:
            self._restore_result(original)

    def test_missing_proxy_rendition_rolls_back_the_whole_import(self) -> None:
        proxy_media_id = self.sparse["input_proxy"]["media_id"]
        self.connection.execute(
            "DELETE FROM renditions WHERE media_id = ? AND rendition_kind = 'low_resolution_cfr_proxy'",
            (proxy_media_id,),
        )
        with self.assertRaisesRegex(ResultImportError, "no eligible catalog rendition"):
            import_sparse_frame_result(self.connection, self.sparse_path)
        self.assertEqual(
            self.connection.execute(
                "SELECT count(*) FROM processing_runs WHERE stage = 'sparse_frame_router'"
            ).fetchone()[0],
            0,
        )
        self.assertEqual(
            self.connection.execute(
                "SELECT count(*) FROM artifacts WHERE artifact_kind = 'sparse_frame_png'"
            ).fetchone()[0],
            0,
        )

    def test_nonprivate_proxy_catalog_lineage_is_rejected(self) -> None:
        self.connection.execute(
            "UPDATE artifacts SET visibility = 'review' WHERE artifact_id = ?",
            (self.sparse["input_proxy"]["artifact_id"],),
        )
        with self.assertRaisesRegex(ResultImportError, "proxy artifact"):
            import_sparse_frame_result(self.connection, self.sparse_path)
        self.assertEqual(
            self.connection.execute(
                "SELECT count(*) FROM processing_runs WHERE stage = 'sparse_frame_router'"
            ).fetchone()[0],
            0,
        )


if __name__ == "__main__":
    unittest.main()
