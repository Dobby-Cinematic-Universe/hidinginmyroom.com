from __future__ import annotations

import copy
import contextlib
import hashlib
import io
import json
import os
import shutil
import sqlite3
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path
from unittest import mock


CORPUS_ROOT = Path(__file__).resolve().parents[1]
REPOSITORY_ROOT = CORPUS_ROOT.parent
sys.path.insert(0, str(CORPUS_ROOT / "src"))

from himr_corpus.cli import build_parser, main  # noqa: E402
import himr_corpus.db as db_module  # noqa: E402
from himr_corpus.db import connect, migrate  # noqa: E402
from himr_corpus.exporter import export_release  # noqa: E402
from himr_corpus.ids import recording_id, source_id, stable_id  # noqa: E402
from himr_corpus.ocr_tesseract_result_importer import (  # noqa: E402
    _exact_source_anchors,
    _frame_rows,
    _insert_processing_run,
    _read_ocr_result,
    _word_bundle_rows,
    import_ocr_tesseract_result,
    search_private_ocr,
    validate_ocr_tesseract_result_file,
)
from himr_corpus.result_importers import (  # noqa: E402
    ResultImportError,
    import_acquisition_result,
    import_preprocess_result,
)
from himr_corpus.sparse_frame_result_importer import import_sparse_frame_result  # noqa: E402
from himr_corpus.validation import validate_database  # noqa: E402
from corpus.tests.reviewer_fixtures import register_reviewer_fixture  # noqa: E402


OBSERVED_AT = "2026-08-28T12:00:00Z"
NATIVE_ID = "private-ocr-import-001"
TSV_HEADER = (
    "level\tpage_num\tblock_num\tpar_num\tline_num\tword_num\t"
    "left\ttop\twidth\theight\tconf\ttext"
)
VALID_TSV = (
    TSV_HEADER
    + "\n1\t1\t0\t0\t0\t0\t0\t0\t320\t180\t-1\t\n"
    + "5\t1\t1\t1\t1\t1\t10\t20\t60\t15\t87.123456\tHIMRverse\n"
    + "5\t1\t1\t1\t1\t2\t75\t20\t45\t15\t96\tDaniel\n"
).encode("utf-8")


def digest(path: Path) -> str:
    value = hashlib.sha256()
    with path.open("rb") as handle:
        while chunk := handle.read(1024 * 1024):
            value.update(chunk)
    return value.hexdigest()


def write_json(path: Path, value: object) -> None:
    path.write_text(json.dumps(value, sort_keys=True, indent=2) + "\n", encoding="utf-8")


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
        raise AssertionError(f"fixture producer failed ({completed.returncode}):\n{completed.stderr}")
    return json.loads(completed.stdout)


def write_fake_tesseract(path: Path) -> None:
    program = f"""#!/usr/bin/python3
import pathlib
import sys
if sys.argv[1:] == ["--version"]:
    print("tesseract 5.5.3-fixture")
    print("fixture-runtime offline")
    raise SystemExit(0)
arguments = sys.argv[1:]
if not pathlib.Path(arguments[0]).is_file() or "tessedit_create_tsv=1" not in arguments:
    raise SystemExit(7)
sys.stdout.buffer.write({VALID_TSV!r})
"""
    path.write_text(program, encoding="utf-8")
    path.chmod(0o500)


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
    "FFmpeg and FFprobe are required for the OCR admission fixture",
)
class OCRTesseractResultImporterTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        work = CORPUS_ROOT / "work"
        work.mkdir(exist_ok=True)
        cls.temporary = tempfile.TemporaryDirectory(prefix="ocr-import-", dir=work)
        cls.root = Path(cls.temporary.name)
        cls.source_path = cls.root / "source.mp4"
        completed = subprocess.run(
            [
                shutil.which("ffmpeg") or "ffmpeg",
                "-hide_banner", "-nostdin", "-loglevel", "error",
                "-f", "lavfi", "-i", "color=c=black:size=320x180:rate=25:duration=2",
                "-an", "-c:v", "libx264", "-preset", "ultrafast",
                "-pix_fmt", "yuv420p", str(cls.source_path),
            ],
            stdin=subprocess.DEVNULL,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.PIPE,
            check=False,
        )
        if completed.returncode:
            raise unittest.SkipTest("host FFmpeg cannot create the OCR fixture")

        acquisition_order = {
            "schema_version": 1,
            "job_id": "private-ocr-import-acquisition",
            "adapter": "local_file",
            "source": {
                "platform": "local_test", "source_kind": "video", "native_id": NATIVE_ID,
                "canonical_url": None, "title": "Private OCR importer fixture",
                "published_at": None, "access_state": "unknown",
            },
            "adapter_config": {
                "path": str(cls.source_path), "expected_sha256": None,
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
        cls.acquisition = run_json(
            [sys.executable, str(REPOSITORY_ROOT / "acquisition/acquire.py"), "run", "--work-order", str(acquisition_order_path)]
        )
        cls.acquisition_path = Path(cls.acquisition["result_path"])

        profile = json.loads(
            (REPOSITORY_ROOT / "pipeline/profiles/cpu-balanced-v1.json").read_text(encoding="utf-8")
        )
        preprocess_order = {
            "schema_version": 1,
            "job_id": "private-ocr-import-preprocess",
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
        cls.preprocess = run_json(
            [sys.executable, str(REPOSITORY_ROOT / "pipeline/media_preprocess.py"), "run", "--work-order", str(preprocess_order_path)]
        )
        cls.preprocess_path = Path(cls.preprocess["result_path"])

        sparse_order = run_json(
            [
                sys.executable, str(REPOSITORY_ROOT / "pipeline/sparse_frame_router.py"),
                "create-work-order", "--job-id", "private-ocr-import-routing",
                "--preprocess-result", str(cls.preprocess_path),
                "--output-root", str(cls.root / "vision"),
            ]
        )
        sparse_order["sampling"] = {
            "include_recording_start": True,
            "scene_changes": {"enabled": False, "max_frames": 0, "offset_ms": 0},
            "periodic": {"enabled": False, "interval_ms": 1_000, "max_frames": 0},
            "min_separation_ms": 0,
        }
        sparse_order["limits"]["max_frames"] = 1
        sparse_order_path = cls.root / "sparse-order.json"
        write_json(sparse_order_path, sparse_order)
        cls.sparse = run_json(
            [sys.executable, str(REPOSITORY_ROOT / "pipeline/sparse_frame_router.py"), "run", "--work-order", str(sparse_order_path)]
        )
        cls.sparse_path = Path(cls.sparse["result_path"])

        cls.tessdata = cls.root / "tessdata"
        cls.tessdata.mkdir(mode=0o700)
        cls.model = cls.tessdata / "eng.traineddata"
        cls.model.write_bytes(b"fixture-eng-traineddata-v1\n")
        cls.model.chmod(0o400)
        cls.engine = cls.root / "tesseract-fixture"
        write_fake_tesseract(cls.engine)
        version = subprocess.run(
            [str(cls.engine), "--version"], capture_output=True, text=True, check=True
        ).stdout.strip()
        ocr_order = {
            "schema_version": 1,
            "job_id": "private-ocr-catalog-fixture",
            "sparse_frame_result": {"path": str(cls.sparse_path), "expected_sha256": digest(cls.sparse_path)},
            "execution_selection": {
                "mode": "explicit_frame_ids", "basis": "reviewer_selected",
                "frame_ids": [cls.sparse["frames"][0]["frame_id"]],
            },
            "tesseract": {
                "executable": str(cls.engine), "expected_sha256": digest(cls.engine),
                "expected_byte_count": cls.engine.stat().st_size,
                "expected_version_output_sha256": hashlib.sha256(version.encode()).hexdigest(),
                "expected_version_label": version.splitlines()[0],
                "tessdata_dir": str(cls.tessdata),
                "models": [{
                    "language": "eng", "path": str(cls.model),
                    "expected_sha256": digest(cls.model),
                    "expected_byte_count": cls.model.stat().st_size,
                }],
            },
            "parameters": {
                "languages": ["eng"], "oem": 1, "psm": 6, "dpi": 300,
                "preserve_interword_spaces": True, "thread_limit": 1,
                "tsv_creation": "explicit_tessedit_create_tsv_1",
            },
            "limits": {
                "max_frames": 1, "max_tsv_bytes_per_frame": 4096,
                "max_words_per_frame": 10, "timeout_seconds_per_frame": 10,
            },
            "output": {"root": str(cls.root / "ocr-output")},
        }
        ocr_order_path = cls.root / "ocr-order.json"
        write_json(ocr_order_path, ocr_order)
        cls.ocr = run_json(
            [sys.executable, str(REPOSITORY_ROOT / "pipeline/ocr_tesseract_adapter.py"), "run", "--work-order", str(ocr_order_path)]
        )
        cls.ocr_path = Path(cls.ocr["result_path"])
        cls.ocr_body = cls.ocr_path.read_bytes()
        cls.ocr_digest = hashlib.sha256(cls.ocr_body).hexdigest()

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
        self.database_path = self.database_root / "corpus.sqlite3"
        self.connection = connect(self.database_path)
        migrate(self.connection)
        self.source_id = source_id("local_test", "video", NATIVE_ID)
        self.recording_id = recording_id(f"synthetic:local_test:video:{NATIVE_ID}")
        batch_id = stable_id("imp", "private-ocr-test-seed", NATIVE_ID)
        input_sha = hashlib.sha256(NATIVE_ID.encode()).hexdigest()
        self.connection.execute(
            "INSERT INTO import_batches(import_batch_id, importer_name, importer_version, input_sha256, started_at, completed_at, status, statistics_json) VALUES(?, 'private-ocr-test-seed', '1', ?, ?, ?, 'completed', '{}')",
            (batch_id, input_sha, OBSERVED_AT, OBSERVED_AT),
        )
        self.connection.execute(
            "INSERT INTO sources(source_id, platform, source_kind, native_id, title, observed_at, access_state, review_state, metadata_json, created_by_import_batch_id, created_at, updated_at) VALUES(?, 'local_test', 'video', ?, 'Fixture', ?, 'public', 'metadata_only', '{}', ?, ?, ?)",
            (self.source_id, NATIVE_ID, OBSERVED_AT, batch_id, OBSERVED_AT, OBSERVED_AT),
        )
        self.connection.execute(
            "INSERT INTO recordings(recording_id, canonical_key, slug, title, date_basis, recording_type, review_state, metadata_json, created_at, updated_at) VALUES(?, ?, ?, 'Fixture', 'test', 'video', 'metadata_only', '{}', ?, ?)",
            (self.recording_id, f"synthetic:{NATIVE_ID}", f"synthetic-{NATIVE_ID}", OBSERVED_AT, OBSERVED_AT),
        )
        self.connection.execute(
            "INSERT INTO recording_sources(recording_source_id, recording_id, source_id, mapping_role, mapping_method, confidence_state, metadata_json) VALUES(?, ?, ?, 'complete_source', 'test_fixture', 'reviewed', '{}')",
            (stable_id("rso", self.recording_id, self.source_id, "complete_source"), self.recording_id, self.source_id),
        )
        import_acquisition_result(self.connection, self.acquisition_path)
        import_preprocess_result(self.connection, self.preprocess_path)

    def tearDown(self) -> None:
        self._restore_ocr()
        sparse = json.loads(self.sparse_path.read_text(encoding="utf-8"))
        png = Path(sparse["artifacts"][0]["path"])
        png.chmod(0o400)
        if self.connection is not None:
            self.connection.close()
        shutil.rmtree(self.database_root)

    def _admit_sparse(self) -> None:
        import_sparse_frame_result(self.connection, self.sparse_path)

    def _restore_ocr(self) -> None:
        if not self.ocr_path.exists():
            return
        self.ocr_path.parent.chmod(0o700)
        self.ocr_path.chmod(0o600)
        self.ocr_path.write_bytes(self.ocr_body)
        self.ocr_path.chmod(0o400)
        self.ocr_path.parent.chmod(0o500)

    def _mutate_ocr(self, mutate) -> None:
        value = json.loads(self.ocr_body)
        mutate(value)
        self.ocr_path.parent.chmod(0o700)
        self.ocr_path.chmod(0o600)
        write_json(self.ocr_path, value)
        self.ocr_path.chmod(0o400)
        self.ocr_path.parent.chmod(0o500)

    def _run_cli(self, arguments: list[str]) -> dict:
        output = io.StringIO()
        with contextlib.redirect_stdout(output):
            main(arguments)
        return json.loads(output.getvalue())

    def _expected_rows(self):
        result = _read_ocr_result(self.ocr_path)
        anchors = _exact_source_anchors(self.connection, result["_sparse"])
        receipt_id = stable_id(
            "ocrimp", "private_ocr_tesseract_result_v1", result["_raw_sha256"]
        )
        frames, words = _frame_rows(
            self.connection, result, anchors, receipt_id
        )
        return result, frames, words

    def _add_human_reviewer(self) -> str:
        reviewer_id = "reviewer_ocr_human_test"
        register_reviewer_fixture(
            self.connection, reviewer_id, "OCR Human Test"
        )
        return reviewer_id

    def test_validation_import_search_and_exact_replay(self) -> None:
        validation = validate_ocr_tesseract_result_file(self.ocr_path)
        self.assertEqual(validation["word_count"], 2)
        self.assertIsNone(validation["calibrated_probability"])
        self._admit_sparse()
        first = import_ocr_tesseract_result(
            self.connection, self.ocr_path, expected_result_sha256=self.ocr_digest
        )
        second = import_ocr_tesseract_result(
            self.connection, self.ocr_path, expected_result_sha256=self.ocr_digest
        )
        self.assertEqual(first["status"], "admitted")
        self.assertEqual(second["status"], "exact_replay")
        self.assertEqual(first["private_ocr_import_receipt_id"], second["private_ocr_import_receipt_id"])
        self.assertEqual(self.connection.execute("SELECT count(*) FROM private_ocr_word_fts").fetchone()[0], 2)
        found = search_private_ocr(self.connection, "HIMRverse")
        self.assertEqual(found["result_count"], 1)
        self.assertEqual(found["results"][0]["raw_text"], "HIMRverse")
        self.assertIsNone(found["results"][0]["calibrated_probability"])
        self.assertEqual(found["coordinate_system"], "rendition_media_ms")
        self.assertEqual(
            found["time_coordinate_scope"], "proxy_rendition_local_media_time"
        )
        self.assertEqual(found["source_time_mapping"], "not_asserted")
        self.assertEqual(found["recording_time_mapping"], "not_asserted")
        frame = self.connection.execute(
            "SELECT * FROM private_ocr_frame_admissions"
        ).fetchone()
        sparse = self.connection.execute(
            "SELECT * FROM observations WHERE observation_id = ?",
            (frame["sparse_frame_observation_id"],),
        ).fetchone()
        self.assertEqual(
            frame["media_id"], self.ocr["source_lineage"]["source_media_id"]
        )
        self.assertNotEqual(frame["source_media_id"], frame["media_id"])
        self.assertEqual(
            (frame["start_ms"], frame["end_ms"]),
            (sparse["start_ms"], sparse["end_ms"]),
        )
        self.assertEqual(
            found["results"][0]["proxy_rendition_start_ms"], frame["start_ms"]
        )
        self.assertEqual(
            found["results"][0]["proxy_rendition_end_ms"], frame["end_ms"]
        )
        self.assertIsNone(found["results"][0]["source_start_ms"])
        self.assertIsNone(found["results"][0]["recording_start_ms"])
        self.assertEqual(found["redaction_state"], "pending")
        self.assertEqual(self.connection.execute("SELECT count(*) FROM publication_decisions").fetchone()[0], 0)
        self.assertEqual(self.connection.execute("SELECT count(*) FROM appearances").fetchone()[0], 0)
        self.assertEqual(self.connection.execute("SELECT count(*) FROM event_evidence").fetchone()[0], 0)
        status = validate_database(self.connection)
        self.assertEqual(status["private_ocr_import_receipts"], 1)
        self.assertEqual(status["private_ocr_word_fts"], 2)
        self.connection.execute("PRAGMA wal_checkpoint(TRUNCATE)")
        self.connection.close()
        self.connection = None
        replay_cli = self._run_cli([
            "import-ocr-tesseract-result",
            "--db", str(self.database_path),
            "--result", str(self.ocr_path),
            "--expected-result-sha256", self.ocr_digest,
        ])
        self.assertEqual(replay_cli["status"], "exact_replay")
        self.assertEqual(replay_cli["fts_rows_added"], 0)
        search_cli = self._run_cli([
            "search-private-ocr", "--db", str(self.database_path),
            "--query", "HIMRverse",
        ])
        self.assertEqual(search_cli["result_count"], 1)
        validation_cli = self._run_cli([
            "validate-ocr-tesseract-result", "--result", str(self.ocr_path)
        ])
        self.assertEqual(validation_cli["result_raw_sha256"], self.ocr_digest)

    def test_append_only_duplicates_and_fts_tampering_fail_closed(self) -> None:
        self._admit_sparse()
        import_ocr_tesseract_result(
            self.connection, self.ocr_path, expected_result_sha256=self.ocr_digest
        )
        receipt = self.connection.execute(
            "SELECT * FROM private_ocr_import_receipts"
        ).fetchone()
        word = self.connection.execute(
            "SELECT * FROM private_ocr_word_admissions ORDER BY word_admission_sequence LIMIT 1"
        ).fetchone()
        with self.assertRaises(sqlite3.IntegrityError):
            self.connection.execute(
                "UPDATE private_ocr_import_receipts SET human_review = 'required'"
            )
        with self.assertRaises(sqlite3.IntegrityError):
            self.connection.execute(
                "DELETE FROM private_ocr_word_admissions WHERE private_ocr_word_admission_id = ?",
                (word["private_ocr_word_admission_id"],),
            )
        duplicate = dict(word)
        duplicate.pop("word_admission_sequence")
        duplicate["private_ocr_word_admission_id"] = "ocrword_duplicate_test"
        columns = ", ".join(duplicate)
        placeholders = ", ".join("?" for _ in duplicate)
        with self.assertRaises(sqlite3.IntegrityError):
            self.connection.execute(
                f"INSERT INTO private_ocr_word_admissions({columns}) VALUES({placeholders})",
                tuple(duplicate.values()),
            )
        base_fts = self.connection.execute(
            "SELECT rowid, private_ocr_word_admission_id, raw_text "
            "FROM private_ocr_word_fts ORDER BY rowid LIMIT 1"
        ).fetchone()
        self.connection.execute(
            "INSERT INTO private_ocr_word_fts(private_ocr_word_admission_id, raw_text) VALUES(?, ?)",
            (base_fts["private_ocr_word_admission_id"], base_fts["raw_text"]),
        )
        with self.assertRaisesRegex(ResultImportError, "FTS differs"):
            search_private_ocr(self.connection, "HIMRverse")
        with self.assertRaisesRegex(ResultImportError, "FTS differs"):
            import_ocr_tesseract_result(
                self.connection,
                self.ocr_path,
                expected_result_sha256=self.ocr_digest,
            )
        with self.assertRaisesRegex(RuntimeError, "private redaction-pending semantics"):
            validate_database(self.connection)
        self.connection.execute(
            "DELETE FROM private_ocr_word_fts WHERE rowid = "
            "(SELECT max(rowid) FROM private_ocr_word_fts)"
        )

        self.connection.execute(
            "UPDATE private_ocr_word_fts SET raw_text = 'direct-update-tamper' "
            "WHERE rowid = ?",
            (base_fts["rowid"],),
        )
        with self.assertRaisesRegex(ResultImportError, "FTS differs"):
            search_private_ocr(self.connection, "HIMRverse")
        self.connection.execute(
            "UPDATE private_ocr_word_fts SET raw_text = ? WHERE rowid = ?",
            (base_fts["raw_text"], base_fts["rowid"]),
        )

        self.connection.execute(
            "DELETE FROM private_ocr_word_fts WHERE rowid = ?", (base_fts["rowid"],)
        )
        with self.assertRaisesRegex(ResultImportError, "FTS differs"):
            search_private_ocr(self.connection, "HIMRverse")
        self.connection.execute(
            "INSERT INTO private_ocr_word_fts(private_ocr_word_admission_id, raw_text) VALUES(?, ?)",
            (base_fts["private_ocr_word_admission_id"], base_fts["raw_text"]),
        )
        replay = import_ocr_tesseract_result(
            self.connection, self.ocr_path, expected_result_sha256=self.ocr_digest
        )
        self.assertEqual(replay["status"], "exact_replay")
        self.assertEqual(receipt["publication_authority"], "none")

    def test_insert_or_replace_is_blocked_with_recursive_triggers_on_or_off(self) -> None:
        self._admit_sparse()
        import_ocr_tesseract_result(
            self.connection, self.ocr_path, expected_result_sha256=self.ocr_digest
        )
        tables = (
            "private_ocr_import_receipts",
            "private_ocr_frame_admissions",
            "private_ocr_word_admissions",
            "import_batches",
            "processing_runs",
            "run_inputs",
            "artifacts",
            "observations",
            "ocr_observations",
            "observation_scores",
        )
        rows = {
            table: dict(
                self.connection.execute(
                    f"SELECT * FROM {table} "
                    + (
                        "WHERE importer_name = 'private_ocr_tesseract_result_v1'"
                        if table == "import_batches"
                        else "WHERE stage = 'ocr_tesseract_tsv'"
                        if table == "processing_runs"
                        else "WHERE processing_run_id IN (SELECT processing_run_id FROM private_ocr_import_receipts)"
                        if table in {"run_inputs", "artifacts", "observations"}
                        else "WHERE observation_id IN (SELECT observation_id FROM private_ocr_word_admissions)"
                        if table in {"ocr_observations", "observation_scores"}
                        else ""
                    )
                    + " LIMIT 1"
                ).fetchone()
            )
            for table in tables
        }
        for recursive in (1, 0):
            self.connection.execute(f"PRAGMA recursive_triggers = {recursive}")
            self.assertEqual(
                self.connection.execute("PRAGMA recursive_triggers").fetchone()[0],
                recursive,
            )
            for table, row in rows.items():
                columns = ", ".join(row)
                placeholders = ", ".join("?" for _ in row)
                with self.subTest(recursive_triggers=recursive, table=table):
                    with self.assertRaisesRegex(
                        sqlite3.IntegrityError,
                        "replacement is forbidden|are sealed|policy is fixed",
                    ):
                        self.connection.execute(
                            f"INSERT OR REPLACE INTO {table}({columns}) "
                            f"VALUES({placeholders})",
                            tuple(row.values()),
                        )
        self.connection.execute("PRAGMA recursive_triggers = ON")
        self.assertEqual(search_private_ocr(self.connection, "HIMRverse")["result_count"], 1)

    def test_post_receipt_append_is_blocked_and_search_replays_sealed_result(self) -> None:
        self._admit_sparse()
        import_ocr_tesseract_result(
            self.connection, self.ocr_path, expected_result_sha256=self.ocr_digest
        )
        original_word = dict(
            self.connection.execute(
                "SELECT * FROM private_ocr_word_admissions "
                "ORDER BY word_admission_sequence LIMIT 1"
            ).fetchone()
        )
        original_observation = dict(
            self.connection.execute(
                "SELECT * FROM observations WHERE observation_id = ?",
                (original_word["observation_id"],),
            ).fetchone()
        )
        forged_observation = dict(original_observation)
        forged_observation["observation_id"] = "obs_forged_append"
        metadata = json.loads(forged_observation["metadata_json"])
        metadata["region_id"] = "region_forged_append"
        forged_observation["metadata_json"] = json.dumps(
            metadata, ensure_ascii=False, sort_keys=True, separators=(",", ":")
        )
        columns = ", ".join(forged_observation)
        placeholders = ", ".join("?" for _ in forged_observation)
        with self.assertRaisesRegex(
            sqlite3.IntegrityError, "private OCR observation policy is fixed"
        ):
            self.connection.execute(
                f"INSERT INTO observations({columns}) VALUES({placeholders})",
                tuple(forged_observation.values()),
            )

        # Simulate a legacy or administratively weakened catalog to prove that the
        # search boundary itself still refuses a coherent ledger+FTS append.
        self.connection.execute("DROP TRIGGER private_ocr_observation_insert_policy")
        self.connection.execute("DROP TRIGGER private_ocr_word_observation_is_private")
        self.connection.execute(
            f"INSERT INTO observations({columns}) VALUES({placeholders})",
            tuple(forged_observation.values()),
        )
        detail = dict(
            self.connection.execute(
                "SELECT * FROM ocr_observations WHERE observation_id = ?",
                (original_word["observation_id"],),
            ).fetchone()
        )
        detail["observation_id"] = forged_observation["observation_id"]
        detail["raw_text"] = "FORGEDAPPEND"
        score = dict(
            self.connection.execute(
                "SELECT * FROM observation_scores WHERE observation_id = ?",
                (original_word["observation_id"],),
            ).fetchone()
        )
        score["observation_score_id"] = "score_forged_append"
        score["observation_id"] = forged_observation["observation_id"]
        forged_word = dict(original_word)
        forged_word.pop("word_admission_sequence")
        forged_word["private_ocr_word_admission_id"] = "ocrword_forged_append"
        forged_word["observation_id"] = forged_observation["observation_id"]
        forged_word["region_id"] = "region_forged_append"
        forged_word["word_ordinal"] = 2
        forged_word["raw_text"] = "FORGEDAPPEND"
        for table, row in (
            ("ocr_observations", detail),
            ("observation_scores", score),
            ("private_ocr_word_admissions", forged_word),
        ):
            row_columns = ", ".join(row)
            row_placeholders = ", ".join("?" for _ in row)
            self.connection.execute(
                f"INSERT INTO {table}({row_columns}) VALUES({row_placeholders})",
                tuple(row.values()),
            )
        self.assertEqual(
            self.connection.execute(
                "SELECT count(*) FROM private_ocr_word_admissions"
            ).fetchone()[0],
            3,
        )
        self.assertEqual(
            self.connection.execute(
                "SELECT count(*) FROM private_ocr_word_fts"
            ).fetchone()[0],
            3,
        )
        with self.assertRaisesRegex(ResultImportError, "word admission row set differs"):
            search_private_ocr(self.connection, "FORGEDAPPEND")

    def test_completed_envelope_duplicate_keys_and_coordinate_drift_fail(self) -> None:
        self.ocr_path.parent.chmod(0o700)
        self.ocr_path.chmod(0o600)
        self.ocr_path.write_bytes(self.ocr_body.replace(b"{\n", b'{\n  "schema_version": 1,\n', 1))
        self.ocr_path.chmod(0o400)
        self.ocr_path.parent.chmod(0o500)
        with self.assertRaisesRegex(ResultImportError, "duplicate JSON key"):
            validate_ocr_tesseract_result_file(self.ocr_path)
        self._restore_ocr()
        self._mutate_ocr(
            lambda value: value["ocr_frames"][0]["frame_locator"]["timestamp"].__setitem__("timestamp_ms", 81)
        )
        with self.assertRaisesRegex(ResultImportError, "exact sparse-frame locator"):
            validate_ocr_tesseract_result_file(self.ocr_path)

    def test_raw_score_is_replayed_not_used_as_probability(self) -> None:
        self._mutate_ocr(
            lambda value: value["ocr_frames"][0]["words"][0].__setitem__("raw_score", 0.87123456)
        )
        with self.assertRaisesRegex(ResultImportError, "raw TSV replay"):
            validate_ocr_tesseract_result_file(self.ocr_path)
        self._restore_ocr()
        self._admit_sparse()
        import_ocr_tesseract_result(
            self.connection, self.ocr_path, expected_result_sha256=self.ocr_digest
        )
        score = self.connection.execute(
            "SELECT raw_score, calibrated_probability, calibration_set_id FROM observation_scores WHERE score_name = 'tesseract_raw_0_100_not_probability' ORDER BY observation_score_id LIMIT 1"
        ).fetchone()
        self.assertGreater(score["raw_score"], 1)
        self.assertIsNone(score["calibrated_probability"])
        self.assertIsNone(score["calibration_set_id"])
        with self.assertRaises(sqlite3.IntegrityError):
            self.connection.execute(
                "UPDATE private_ocr_word_admissions SET calibrated_probability = 0.9"
            )

    def test_tsv_png_writable_symlink_and_tampering_fail(self) -> None:
        tsv = Path(self.ocr["ocr_frames"][0]["tsv_artifact"]["path"])
        tsv.chmod(0o600)
        with self.assertRaisesRegex(ResultImportError, "sealed read-only"):
            validate_ocr_tesseract_result_file(self.ocr_path)
        tsv.chmod(0o400)
        sparse = json.loads(self.sparse_path.read_text(encoding="utf-8"))
        png = Path(sparse["artifacts"][0]["path"])
        png.chmod(0o600)
        with self.assertRaisesRegex(ResultImportError, "sealed"):
            validate_ocr_tesseract_result_file(self.ocr_path)
        png.chmod(0o400)
        alias = self.root / "result-alias.json"
        alias.symlink_to(self.ocr_path)
        try:
            with self.assertRaisesRegex(ResultImportError, "without traversal or symlinks"):
                validate_ocr_tesseract_result_file(alias)
        finally:
            alias.unlink()
        tsv.parent.chmod(0o700)
        tsv.chmod(0o600)
        original = tsv.read_bytes()
        tsv.write_bytes(original.replace(b"HIMRverse", b"HIMRversf"))
        tsv.chmod(0o400)
        tsv.parent.chmod(0o500)
        try:
            with self.assertRaisesRegex(ResultImportError, "SHA-256 differs"):
                validate_ocr_tesseract_result_file(self.ocr_path)
        finally:
            tsv.parent.chmod(0o700)
            tsv.chmod(0o600)
            tsv.write_bytes(original)
            tsv.chmod(0o400)
            tsv.parent.chmod(0o500)

    def test_missing_upstream_catalog_and_file_fail_without_partial_receipt(self) -> None:
        with self.assertRaisesRegex(ResultImportError, "sparse observation"):
            import_ocr_tesseract_result(
                self.connection, self.ocr_path, expected_result_sha256=self.ocr_digest
            )
        self.assertEqual(self.connection.execute("SELECT count(*) FROM private_ocr_import_receipts").fetchone()[0], 0)
        self.assertEqual(self.connection.execute("SELECT count(*) FROM private_ocr_word_fts").fetchone()[0], 0)
        self._mutate_ocr(
            lambda value: value["sparse_frame_result"].__setitem__("path", str(self.root / "missing-result.json"))
        )
        with self.assertRaisesRegex(ResultImportError, "not a readable current file"):
            validate_ocr_tesseract_result_file(self.ocr_path)

    def test_transaction_rolls_back_on_observation_collision(self) -> None:
        self._admit_sparse()
        proxy_rendition = self.connection.execute(
            "SELECT rendition_id, recording_id FROM renditions WHERE rendition_kind = 'low_resolution_cfr_proxy'"
        ).fetchone()
        word = self.ocr["ocr_frames"][0]["words"][0]
        run_id = self.ocr["processing_run"]["processing_run_id"]
        observation_id = stable_id(
            "obs", run_id, proxy_rendition["rendition_id"], word["region_id"],
            "ocr_tesseract_word_candidate",
        )
        self.connection.execute(
            "INSERT INTO observations(observation_id, observation_kind, recording_id, rendition_id, start_ms, end_ms, visibility, review_state, created_at) VALUES(?, 'unrelated_collision', ?, ?, 0, 1, 'private', 'machine', ?)",
            (observation_id, proxy_rendition["recording_id"], proxy_rendition["rendition_id"], OBSERVED_AT),
        )
        with self.assertRaisesRegex(ResultImportError, "observations ID already has different data"):
            import_ocr_tesseract_result(
                self.connection, self.ocr_path, expected_result_sha256=self.ocr_digest
            )
        self.assertEqual(self.connection.execute("SELECT count(*) FROM private_ocr_import_receipts").fetchone()[0], 0)
        self.assertEqual(self.connection.execute("SELECT count(*) FROM private_ocr_frame_admissions").fetchone()[0], 0)
        self.assertEqual(self.connection.execute("SELECT count(*) FROM private_ocr_word_fts").fetchone()[0], 0)
        self.assertEqual(self.connection.execute("SELECT count(*) FROM processing_runs WHERE stage = 'ocr_tesseract_tsv'").fetchone()[0], 0)

    def test_fts_is_private_and_has_no_export_identity_event_or_publication_lane(self) -> None:
        self._admit_sparse()
        imported = import_ocr_tesseract_result(
            self.connection, self.ocr_path, expected_result_sha256=self.ocr_digest
        )
        word = self.connection.execute("SELECT * FROM private_ocr_word_admissions LIMIT 1").fetchone()
        frame = self.connection.execute(
            "SELECT * FROM private_ocr_frame_admissions WHERE private_ocr_frame_admission_id = ?",
            (word["private_ocr_frame_admission_id"],),
        ).fetchone()
        # The private-OCR BEFORE trigger fires before any generic reviewer FK can
        # turn this into a publication path.
        reviewer_id = "reviewer_nonexistent_ocr_test"
        with self.assertRaises(sqlite3.IntegrityError):
            self.connection.execute(
                "INSERT INTO publication_decisions(publication_decision_id, object_type, object_id, decision, reviewer_id, decided_at, basis) VALUES('pub_ocr', 'private_ocr_word_admission', ?, 'publish', ?, ?, 'forbidden')",
                (word["private_ocr_word_admission_id"], reviewer_id, OBSERVED_AT),
            )
        human_reviewer_id = self._add_human_reviewer()
        tsv_artifact_id = frame["tsv_artifact_id"]
        for ordinal, (object_type, object_id) in enumerate(
            (
                ("observation", word["observation_id"]),
                ("artifact", tsv_artifact_id),
            )
        ):
            with self.assertRaises(sqlite3.IntegrityError):
                self.connection.execute(
                    "INSERT INTO publication_decisions(publication_decision_id, object_type, object_id, decision, reviewer_id, decided_at, basis) VALUES(?, ?, ?, 'publish', ?, ?, 'generic OCR bypass forbidden')",
                    (
                        f"pub_ocr_generic_{ordinal}",
                        object_type,
                        object_id,
                        human_reviewer_id,
                        OBSERVED_AT,
                    ),
                )
            with self.assertRaises(sqlite3.IntegrityError):
                self.connection.execute(
                    "INSERT INTO publication_gate_decisions(publication_gate_decision_id, object_type, object_id, gate_kind, decision, reviewer_id, decided_at, basis) VALUES(?, ?, ?, 'rights', 'clear', ?, ?, 'generic OCR bypass forbidden')",
                    (
                        f"gate_ocr_generic_{ordinal}",
                        object_type,
                        object_id,
                        human_reviewer_id,
                        OBSERVED_AT,
                    ),
                )
        entity_id = "entity_ocr_test"
        self.connection.execute(
            "INSERT INTO entities(entity_id, entity_type, canonical_label, slug, visibility, review_state, created_at) VALUES(?, 'person', 'OCR Test', 'ocr-test', 'private', 'unreviewed', ?)",
            (entity_id, OBSERVED_AT),
        )
        with self.assertRaises(sqlite3.IntegrityError):
            self.connection.execute(
                "INSERT INTO appearances(appearance_id, entity_id, recording_id, start_ms, end_ms, appearance_role, observation_id) VALUES('appearance_ocr', ?, ?, ?, ?, 'machine_ocr', ?)",
                (entity_id, frame["recording_id"], frame["start_ms"], frame["end_ms"], word["observation_id"]),
            )
        event_id = "event_ocr_test"
        self.connection.execute(
            "INSERT INTO events(event_id, canonical_label, slug, event_kind, created_at) VALUES(?, 'OCR Event', 'ocr-event', 'test', ?)",
            (event_id, OBSERVED_AT),
        )
        with self.assertRaises(sqlite3.IntegrityError):
            self.connection.execute(
                "INSERT INTO event_evidence(event_evidence_id, event_id, observation_id, support_kind) VALUES('evidence_ocr', ?, ?, 'direct')",
                (event_id, word["observation_id"]),
            )
        release_path = self.database_root / "release.json"
        export_release(self.connection, release_path)
        self.assertNotIn("HIMRverse", release_path.read_text(encoding="utf-8"))
        self.assertEqual(imported["publication_decisions_added"], 0)
        self.assertEqual(imported["identity_rows_added"], 0)
        self.assertEqual(imported["event_rows_added"], 0)

    def test_count_equal_forged_word_ledger_replay_and_validation_fail(self) -> None:
        self._admit_sparse()
        import_ocr_tesseract_result(
            self.connection, self.ocr_path, expected_result_sha256=self.ocr_digest
        )
        word = self.connection.execute(
            "SELECT * FROM private_ocr_word_admissions ORDER BY word_admission_sequence LIMIT 1"
        ).fetchone()
        self.connection.execute("DROP TRIGGER private_ocr_words_no_update")
        self.connection.execute("DROP TRIGGER private_ocr_details_no_update")
        self.connection.execute(
            "UPDATE private_ocr_word_admissions SET raw_text = 'FORGED0' "
            "WHERE private_ocr_word_admission_id = ?",
            (word["private_ocr_word_admission_id"],),
        )
        self.connection.execute(
            "UPDATE ocr_observations SET raw_text = 'FORGED0' WHERE observation_id = ?",
            (word["observation_id"],),
        )
        self.connection.execute(
            "UPDATE private_ocr_word_fts SET raw_text = 'FORGED0' "
            "WHERE private_ocr_word_admission_id = ?",
            (word["private_ocr_word_admission_id"],),
        )
        with self.assertRaisesRegex(ResultImportError, "word admission differs"):
            import_ocr_tesseract_result(
                self.connection, self.ocr_path, expected_result_sha256=self.ocr_digest
            )
        with self.assertRaisesRegex(RuntimeError, "current-file validation failed"):
            validate_database(self.connection)

    def test_hidden_fts_shadow_token_tamper_fails_search_replay_and_validation(self) -> None:
        self._admit_sparse()
        import_ocr_tesseract_result(
            self.connection, self.ocr_path, expected_result_sha256=self.ocr_digest
        )
        changed = 0
        for row in self.connection.execute(
            "SELECT id, block FROM private_ocr_word_fts_data WHERE id > 10"
        ).fetchall():
            block = row["block"]
            if block is not None and b"himrverse" in block:
                self.connection.execute(
                    "UPDATE private_ocr_word_fts_data SET block = ? WHERE id = ?",
                    (block.replace(b"himrverse", b"xxxxxxxxx"), row["id"]),
                )
                changed += 1
        self.assertEqual(changed, 1)
        with self.assertRaisesRegex(ResultImportError, "FTS integrity check failed"):
            search_private_ocr(self.connection, "HIMRverse")
        with self.assertRaisesRegex(ResultImportError, "FTS integrity check failed"):
            import_ocr_tesseract_result(
                self.connection, self.ocr_path, expected_result_sha256=self.ocr_digest
            )
        with self.assertRaisesRegex(RuntimeError, "SQLite integrity check failed"):
            validate_database(self.connection)

    def test_processing_run_and_input_provenance_are_sealed_and_replayed(self) -> None:
        self._admit_sparse()
        imported = import_ocr_tesseract_result(
            self.connection, self.ocr_path, expected_result_sha256=self.ocr_digest
        )
        run_id = imported["processing_run_id"]
        with self.assertRaises(sqlite3.IntegrityError):
            self.connection.execute(
                "UPDATE processing_runs SET implementation_version = 'tampered' "
                "WHERE processing_run_id = ?",
                (run_id,),
            )
        with self.assertRaises(sqlite3.IntegrityError):
            self.connection.execute(
                "DELETE FROM run_inputs WHERE processing_run_id = ?", (run_id,)
            )
        with self.assertRaises(sqlite3.IntegrityError):
            self.connection.execute(
                "INSERT INTO run_inputs(run_input_id, processing_run_id, object_type, object_id, input_role, input_sha256) VALUES('rin_extra_ocr_test', ?, 'artifact', 'artifact_extra_ocr_test', 'extra', ?)",
                (run_id, "0" * 64),
            )
        with self.assertRaises(sqlite3.IntegrityError):
            self.connection.execute(
                "INSERT INTO artifacts(artifact_id, processing_run_id, artifact_kind, storage_uri, sha256, byte_count, visibility) VALUES('artifact_extra_ocr_test', ?, 'other', 'file:///tmp/extra-ocr-test', ?, 1, 'private')",
                (run_id, "0" * 64),
            )
        with self.assertRaises(sqlite3.IntegrityError):
            self.connection.execute(
                "UPDATE import_batches SET statistics_json = '{}' "
                "WHERE import_batch_id = ?",
                (imported["import_batch_id"],),
            )

        self.connection.execute("DROP TRIGGER private_ocr_processing_runs_no_update")
        self.connection.execute(
            "UPDATE processing_runs SET implementation_version = 'tampered' "
            "WHERE processing_run_id = ?",
            (run_id,),
        )
        with self.assertRaisesRegex(ResultImportError, "processing run differs"):
            import_ocr_tesseract_result(
                self.connection, self.ocr_path, expected_result_sha256=self.ocr_digest
            )
        with self.assertRaisesRegex(RuntimeError, "current-file validation failed"):
            validate_database(self.connection)

    def test_deleted_processing_input_is_not_an_exact_replay(self) -> None:
        self._admit_sparse()
        imported = import_ocr_tesseract_result(
            self.connection, self.ocr_path, expected_result_sha256=self.ocr_digest
        )
        self.connection.execute("DROP TRIGGER private_ocr_run_inputs_no_delete")
        self.connection.execute(
            "DELETE FROM run_inputs WHERE processing_run_id = ?",
            (imported["processing_run_id"],),
        )
        with self.assertRaisesRegex(ResultImportError, "processing input row set"):
            import_ocr_tesseract_result(
                self.connection, self.ocr_path, expected_result_sha256=self.ocr_digest
            )
        with self.assertRaisesRegex(RuntimeError, "current-file validation failed"):
            validate_database(self.connection)

    def test_preexisting_generic_publication_state_blocks_future_ocr_object(self) -> None:
        self._admit_sparse()
        result, _frames, words = self._expected_rows()
        reviewer_id = self._add_human_reviewer()
        self.connection.execute(
            "INSERT INTO publication_decisions(publication_decision_id, object_type, object_id, decision, reviewer_id, decided_at, basis) VALUES('pub_future_ocr_observation', 'observation', ?, 'publish', ?, ?, 'preexisting generic state')",
            (words[0]["observation_id"], reviewer_id, OBSERVED_AT),
        )
        with self.assertRaises(sqlite3.IntegrityError):
            import_ocr_tesseract_result(
                self.connection,
                self.ocr_path,
                expected_result_sha256=result["_raw_sha256"],
            )
        self.assertEqual(
            self.connection.execute(
                "SELECT count(*) FROM private_ocr_import_receipts"
            ).fetchone()[0],
            0,
        )

    def test_preexisting_generic_publication_state_blocks_future_tsv_artifact(self) -> None:
        self._admit_sparse()
        reviewer_id = self._add_human_reviewer()
        artifact_id = self.ocr["ocr_frames"][0]["tsv_artifact"]["artifact_id"]
        self.connection.execute(
            "INSERT INTO publication_decisions(publication_decision_id, object_type, object_id, decision, reviewer_id, decided_at, basis) VALUES('pub_future_ocr_artifact', 'artifact', ?, 'publish', ?, ?, 'preexisting generic state')",
            (artifact_id, reviewer_id, OBSERVED_AT),
        )
        with self.assertRaises(sqlite3.IntegrityError):
            import_ocr_tesseract_result(
                self.connection,
                self.ocr_path,
                expected_result_sha256=self.ocr_digest,
            )
        self.assertEqual(
            self.connection.execute(
                "SELECT count(*) FROM private_ocr_import_receipts"
            ).fetchone()[0],
            0,
        )

    def test_pre_admission_event_link_is_blocked_and_cannot_commit(self) -> None:
        self._admit_sparse()
        result, _frames, words = self._expected_rows()
        _insert_processing_run(self.connection, result)
        _word, observation, _detail, _score = _word_bundle_rows(result, words[0])
        columns = ", ".join(observation)
        with self.assertRaisesRegex(
            sqlite3.IntegrityError, "private OCR observation policy is fixed"
        ):
            self.connection.execute(
                f"INSERT INTO observations({columns}) VALUES({', '.join('?' for _ in observation)})",
                tuple(observation.values()),
            )
        self.connection.execute("DROP TRIGGER private_ocr_observation_insert_policy")
        self.connection.execute(
            f"INSERT INTO observations({columns}) VALUES({', '.join('?' for _ in observation)})",
            tuple(observation.values()),
        )
        event_id = "event_pre_admission_ocr_test"
        self.connection.execute(
            "INSERT INTO events(event_id, canonical_label, slug, event_kind, created_at) "
            "VALUES(?, 'Pre-admission OCR Event', 'pre-admission-ocr-event', 'test', ?)",
            (event_id, OBSERVED_AT),
        )
        with self.assertRaises(sqlite3.IntegrityError):
            self.connection.execute(
                "INSERT INTO event_evidence(event_evidence_id, event_id, observation_id, support_kind) "
                "VALUES('evidence_pre_admission_blocked', ?, ?, 'direct')",
                (event_id, observation["observation_id"]),
            )
        self.connection.execute("DROP TRIGGER private_ocr_event_evidence_forbidden")
        self.connection.execute(
            "INSERT INTO event_evidence(event_evidence_id, event_id, observation_id, support_kind) "
            "VALUES('evidence_pre_admission_bypass_simulation', ?, ?, 'direct')",
            (event_id, observation["observation_id"]),
        )
        with self.assertRaises((sqlite3.IntegrityError, ResultImportError)):
            import_ocr_tesseract_result(
                self.connection,
                self.ocr_path,
                expected_result_sha256=result["_raw_sha256"],
            )
        self.assertEqual(
            self.connection.execute(
                "SELECT count(*) FROM private_ocr_import_receipts"
            ).fetchone()[0],
            0,
        )

    def test_missing_observation_policy_key_is_rejected_null_safely(self) -> None:
        self._admit_sparse()
        result, _frames, words = self._expected_rows()
        _insert_processing_run(self.connection, result)
        _word, observation, _detail, _score = _word_bundle_rows(result, words[0])
        metadata = json.loads(observation["metadata_json"])
        metadata.pop("human_review")
        observation["metadata_json"] = json.dumps(metadata, sort_keys=True, separators=(",", ":"))
        columns = ", ".join(observation)
        with self.assertRaises(sqlite3.IntegrityError):
            self.connection.execute(
                f"INSERT INTO observations({columns}) VALUES({', '.join('?' for _ in observation)})",
                tuple(observation.values()),
            )

    def test_removed_proxy_and_sparse_lineage_keys_fail_full_validation(self) -> None:
        self._admit_sparse()
        import_ocr_tesseract_result(
            self.connection, self.ocr_path, expected_result_sha256=self.ocr_digest
        )
        proxy = self.connection.execute(
            "SELECT rendition_id, metadata_json FROM renditions "
            "WHERE rendition_kind = 'low_resolution_cfr_proxy'"
        ).fetchone()
        self.connection.execute(
            "UPDATE renditions SET metadata_json = '{}' WHERE rendition_id = ?",
            (proxy["rendition_id"],),
        )
        with self.assertRaisesRegex(RuntimeError, "private redaction-pending semantics"):
            validate_database(self.connection)
        self.connection.execute(
            "UPDATE renditions SET metadata_json = ? WHERE rendition_id = ?",
            (proxy["metadata_json"], proxy["rendition_id"]),
        )
        sparse = self.connection.execute(
            "SELECT observation_id, metadata_json FROM observations "
            "WHERE observation_kind = 'sparse_frame_routing_candidate' LIMIT 1"
        ).fetchone()
        self.connection.execute(
            "UPDATE observations SET metadata_json = '{}' WHERE observation_id = ?",
            (sparse["observation_id"],),
        )
        with self.assertRaisesRegex(RuntimeError, "private redaction-pending semantics"):
            validate_database(self.connection)
        self.connection.execute(
            "UPDATE observations SET metadata_json = ? WHERE observation_id = ?",
            (sparse["metadata_json"], sparse["observation_id"]),
        )
        validate_database(self.connection)

    def test_migration_preflight_abort_is_atomic(self) -> None:
        migration_source = db_module.MIGRATIONS_DIR
        isolated_root = self.database_root / "migration-preflight"
        isolated_migrations = isolated_root / "migrations"
        isolated_migrations.mkdir(parents=True)
        for source in sorted(migration_source.glob("*.sql")):
            if int(source.name[:4]) <= 32:
                shutil.copy2(source, isolated_migrations / source.name)
        isolated_connection = connect(isolated_root / "catalog.sqlite3")
        try:
            with mock.patch.object(db_module, "MIGRATIONS_DIR", isolated_migrations):
                self.assertEqual(len(migrate(isolated_connection)), 32)
                register_reviewer_fixture(
                    isolated_connection,
                    "reviewer_ocr_preflight",
                    "OCR Preflight Reviewer",
                )
                isolated_connection.execute(
                    "INSERT INTO publication_decisions(publication_decision_id, object_type, object_id, decision, reviewer_id, decided_at, basis) VALUES('pub_reserved_ocr_preflight', 'private_ocr_word_admission', 'future_private_ocr_word', 'withhold', 'reviewer_ocr_preflight', ?, 'preflight fixture')",
                    (OBSERVED_AT,),
                )
                shutil.copy2(
                    migration_source / "0033_private_ocr_tesseract.sql",
                    isolated_migrations / "0033_private_ocr_tesseract.sql",
                )
                with self.assertRaisesRegex(
                    sqlite3.IntegrityError, "reserved private OCR state"
                ):
                    migrate(isolated_connection)
                self.assertEqual(
                    isolated_connection.execute(
                        "SELECT max(version) FROM schema_migrations"
                    ).fetchone()[0],
                    32,
                )
                self.assertIsNone(
                    isolated_connection.execute(
                        "SELECT 1 FROM sqlite_master "
                        "WHERE name = 'private_ocr_import_receipts'"
                    ).fetchone()
                )
                self.assertEqual(
                    isolated_connection.execute("PRAGMA integrity_check").fetchone()[0],
                    "ok",
                )
        finally:
            isolated_connection.close()

    def test_sqlite_before_344_cannot_install_or_use_private_ocr_fts(self) -> None:
        migration_source = db_module.MIGRATIONS_DIR
        isolated_root = self.database_root / "sqlite-version-floor"
        isolated_migrations = isolated_root / "migrations"
        isolated_migrations.mkdir(parents=True)
        for source in sorted(migration_source.glob("*.sql")):
            if int(source.name[:4]) <= 32:
                shutil.copy2(source, isolated_migrations / source.name)
        isolated_connection = connect(isolated_root / "catalog.sqlite3")
        try:
            with mock.patch.object(db_module, "MIGRATIONS_DIR", isolated_migrations):
                self.assertEqual(len(migrate(isolated_connection)), 32)
                shutil.copy2(
                    migration_source / "0033_private_ocr_tesseract.sql",
                    isolated_migrations / "0033_private_ocr_tesseract.sql",
                )
                isolated_connection.create_function(
                    "sqlite_version", 0, lambda: "3.43.2"
                )
                with self.assertRaisesRegex(
                    RuntimeError, "migration 0033 requires SQLite 3.44.0 or newer"
                ):
                    migrate(isolated_connection)
                self.assertEqual(
                    isolated_connection.execute(
                        "SELECT max(version) FROM schema_migrations"
                    ).fetchone()[0],
                    32,
                )
                self.assertIsNone(
                    isolated_connection.execute(
                        "SELECT 1 FROM sqlite_master "
                        "WHERE name = 'private_ocr_import_receipts'"
                    ).fetchone()
                )
        finally:
            isolated_connection.close()

        self._admit_sparse()
        import_ocr_tesseract_result(
            self.connection, self.ocr_path, expected_result_sha256=self.ocr_digest
        )
        self.connection.create_function("sqlite_version", 0, lambda: "3.43.2")
        with self.assertRaisesRegex(
            RuntimeError, "migration 0033 requires SQLite 3.44.0 or newer"
        ):
            migrate(self.connection)
        with self.assertRaisesRegex(
            ResultImportError, "requires SQLite 3.44.0 or newer"
        ):
            search_private_ocr(self.connection, "HIMRverse")
        with self.assertRaisesRegex(
            ResultImportError, "requires SQLite 3.44.0 or newer"
        ):
            import_ocr_tesseract_result(
                self.connection,
                self.ocr_path,
                expected_result_sha256=self.ocr_digest,
            )

    def test_cli_registers_validate_import_and_search_without_auto_migration(self) -> None:
        parser = build_parser()
        self.assertEqual(
            parser.parse_args(["validate-ocr-tesseract-result", "--result", str(self.ocr_path)]).command,
            "validate-ocr-tesseract-result",
        )
        self.assertEqual(
            parser.parse_args([
                "import-ocr-tesseract-result", "--db", "x.sqlite3", "--result", str(self.ocr_path),
                "--expected-result-sha256", self.ocr_digest,
            ]).command,
            "import-ocr-tesseract-result",
        )
        self.assertEqual(
            parser.parse_args(["search-private-ocr", "--db", "x.sqlite3", "--query", "HIMRverse"]).command,
            "search-private-ocr",
        )
        self.connection.execute("DELETE FROM schema_migrations WHERE version = 33")
        self.connection.execute("PRAGMA wal_checkpoint(TRUNCATE)")
        self.connection.close()
        self.connection = None
        before = self.database_path.read_bytes()
        with self.assertRaisesRegex(RuntimeError, "Pending migrations"):
            self._run_cli([
                "import-ocr-tesseract-result",
                "--db", str(self.database_path),
                "--result", str(self.ocr_path),
                "--expected-result-sha256", self.ocr_digest,
            ])
        self.assertEqual(self.database_path.read_bytes(), before)


if __name__ == "__main__":
    unittest.main()
