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
from datetime import datetime, timezone
from pathlib import Path


CORPUS_ROOT = Path(__file__).resolve().parents[1]
REPOSITORY_ROOT = CORPUS_ROOT.parent
sys.path.insert(0, str(CORPUS_ROOT / "src"))

from himr_corpus.db import connect, migrate  # noqa: E402
from himr_corpus.ids import recording_id, source_id, stable_id  # noqa: E402
from himr_corpus.local_window_result_importer import (  # noqa: E402
    import_local_window_result,
    validate_local_window_result_file,
)
from himr_corpus.result_importers import (  # noqa: E402
    ResultImportError,
    import_acquisition_result,
)


def digest(path: Path) -> str:
    value = hashlib.sha256()
    with path.open("rb") as handle:
        while chunk := handle.read(1024 * 1024):
            value.update(chunk)
    return value.hexdigest()


def run_json(command: list[str]) -> dict:
    completed = subprocess.run(
        command,
        cwd=REPOSITORY_ROOT,
        stdin=subprocess.DEVNULL,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
        check=False,
    )
    if completed.returncode:
        raise AssertionError(
            f"command failed with {completed.returncode}: {command!r}\n{completed.stderr}"
        )
    return json.loads(completed.stdout)


class LocalWindowResultImporterCLITests(unittest.TestCase):
    def test_cli_exposes_read_only_validation_and_private_import(self) -> None:
        completed = subprocess.run(
            [sys.executable, "-m", "himr_corpus", "--help"],
            cwd=REPOSITORY_ROOT,
            env={**os.environ, "PYTHONPATH": str(CORPUS_ROOT / "src")},
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            check=False,
        )
        self.assertEqual(completed.returncode, 0, completed.stderr)
        self.assertIn("validate-local-window-result", completed.stdout)
        self.assertIn("import-local-window-result", completed.stdout)

    def test_cli_validation_cannot_create_or_migrate_a_database(self) -> None:
        with tempfile.TemporaryDirectory(prefix="local-window-cli-readonly-") as temporary:
            missing_database = Path(temporary) / "must-not-exist.sqlite3"
            completed = subprocess.run(
                [
                    sys.executable,
                    "-m",
                    "himr_corpus",
                    "validate-local-window-result",
                    "--db",
                    str(missing_database),
                    "--result",
                    str(Path(temporary) / "missing-result.json"),
                    "--observed-at",
                    "2026-08-26T23:40:15Z",
                ],
                cwd=REPOSITORY_ROOT,
                env={**os.environ, "PYTHONPATH": str(CORPUS_ROOT / "src")},
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                text=True,
                check=False,
            )
            self.assertNotEqual(completed.returncode, 0)
            self.assertFalse(missing_database.exists())


@unittest.skipUnless(
    shutil.which("ffmpeg") and shutil.which("ffprobe"),
    "FFmpeg and FFprobe are required for local-window admission integration",
)
class LocalWindowResultImporterTests(unittest.TestCase):
    def setUp(self) -> None:
        work_root = CORPUS_ROOT / "work"
        work_root.mkdir(exist_ok=True)
        self.temporary = tempfile.TemporaryDirectory(
            prefix="local-window-import-", dir=work_root
        )
        self.root = Path(self.temporary.name).resolve()
        self.connection = connect(self.root / "corpus.sqlite3")
        migrate(self.connection)
        self.ffmpeg = Path(shutil.which("ffmpeg") or "").resolve()
        self.ffprobe = Path(shutil.which("ffprobe") or "").resolve()
        source = self.root / "source.mp4"
        subprocess.run(
            [
                str(self.ffmpeg), "-hide_banner", "-nostdin", "-loglevel", "error",
                "-f", "lavfi", "-i", "testsrc2=size=160x90:rate=10:duration=2.2",
                "-f", "lavfi", "-i", "sine=frequency=440:sample_rate=48000:duration=2.2",
                "-c:v", "libx264", "-preset", "ultrafast", "-pix_fmt", "yuv420p",
                "-c:a", "aac", "-shortest", str(source),
            ],
            check=True,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.PIPE,
        )
        self.platform = "local_test"
        self.source_kind = "video"
        self.native_id = "local-window-admission-001"
        self.canonical_source_id = source_id(
            self.platform, self.source_kind, self.native_id
        )
        self.recording_id = recording_id("synthetic:local-window-admission-001")
        seed_time = "2026-08-26T20:00:00Z"
        seed_batch = stable_id("imp", "local-window-admission-test-seed")
        self.connection.execute(
            """
            INSERT INTO import_batches(
                import_batch_id, importer_name, importer_version, input_sha256,
                source_snapshot_date, started_at, completed_at, status, statistics_json
            ) VALUES(?, 'local-window-test-seed', '1', ?, NULL, ?, ?, 'completed', '{}')
            """,
            (seed_batch, "a" * 64, seed_time, seed_time),
        )
        self.connection.execute(
            """
            INSERT INTO sources(
                source_id, platform, source_kind, native_id, canonical_url, title,
                observed_at, access_state, review_state, metadata_json,
                created_by_import_batch_id, created_at, updated_at
            ) VALUES(?, ?, ?, ?, NULL, 'Local window fixture', ?, 'public',
                     'metadata_only', '{}', ?, ?, ?)
            """,
            (
                self.canonical_source_id, self.platform, self.source_kind,
                self.native_id, seed_time, seed_batch, seed_time, seed_time,
            ),
        )
        self.connection.execute(
            """
            INSERT INTO recordings(
                recording_id, canonical_key, slug, title, date_basis,
                recording_type, review_state, metadata_json, created_at, updated_at
            ) VALUES(?, 'synthetic:local-window-admission-001',
                     'synthetic-local-window-admission-001', 'Fixture recording',
                     'test', 'video', 'metadata_only', '{}', ?, ?)
            """,
            (self.recording_id, seed_time, seed_time),
        )
        self.connection.execute(
            """
            INSERT INTO recording_sources(
                recording_source_id, recording_id, source_id, mapping_role,
                mapping_method, confidence_state, metadata_json
            ) VALUES(?, ?, ?, 'complete_source', 'test_fixture', 'reviewed', '{}')
            """,
            (
                stable_id("rso", self.recording_id, self.canonical_source_id),
                self.recording_id,
                self.canonical_source_id,
            ),
        )

        acquired_root = self.root / "acquired"
        acquisition_order = {
            "schema_version": 1,
            "job_id": "local-window-admission-acquisition",
            "adapter": "local_file",
            "source": {
                "platform": self.platform,
                "source_kind": self.source_kind,
                "native_id": self.native_id,
                "canonical_url": None,
                "title": "Local window fixture",
                "published_at": None,
                "access_state": "unknown",
            },
            "adapter_config": {
                "path": str(source),
                "expected_sha256": None,
                "expected_byte_count": None,
            },
            "output": {"root": str(acquired_root)},
            "limits": {
                "max_job_bytes": 16 * 1024 * 1024,
                "global_cache_cap_bytes": 64 * 1024 * 1024,
                "free_space_floor_bytes": 0,
            },
        }
        acquisition_order_path = self.root / "acquisition-order.json"
        acquisition_order_path.write_text(json.dumps(acquisition_order), encoding="utf-8")
        acquisition = run_json(
            [
                sys.executable,
                str(REPOSITORY_ROOT / "acquisition/acquire.py"),
                "run",
                "--work-order",
                str(acquisition_order_path),
            ]
        )
        self.acquisition_result_path = Path(acquisition["result_path"])
        import_acquisition_result(self.connection, self.acquisition_result_path)

        bundle_root = self.root / "window-bundles"
        output_root = self.root / "window-artifacts"
        manifest = run_json(
            [
                sys.executable,
                str(REPOSITORY_ROOT / "pipeline/local_window.py"),
                "materialize",
                "--acquisition-result",
                str(self.acquisition_result_path),
                "--bundle-root",
                str(bundle_root),
                "--window-output-root",
                str(output_root),
                "--ffmpeg",
                str(self.ffmpeg),
                "--ffmpeg-sha256",
                digest(self.ffmpeg),
                "--ffprobe",
                str(self.ffprobe),
                "--ffprobe-sha256",
                digest(self.ffprobe),
                "--chunk-duration-ms",
                "1000",
                "--max-windows",
                "8",
                "--max-window-output-bytes",
                str(64 * 1024 * 1024),
                "--free-space-floor-bytes",
                "0",
                "--timeout-seconds",
                "120",
            ]
        )
        self.order_paths = [
            bundle_root / manifest["bundle_relative_path"] / row["path"]
            for row in manifest["work_orders"]
        ]
        order_path = self.order_paths[1]
        completed = run_json(
            [
                sys.executable,
                str(REPOSITORY_ROOT / "pipeline/local_window.py"),
                "run",
                "--work-order",
                str(order_path),
            ]
        )
        self.result_path = Path(completed["result_path"])
        self.observed_at = datetime.now(timezone.utc).isoformat(
            timespec="seconds"
        ).replace("+00:00", "Z")

    def _rewrite_result(self, value: dict) -> None:
        directory = self.result_path.parent
        directory.chmod(0o700)
        self.result_path.chmod(0o600)
        self.result_path.write_text(
            json.dumps(value, ensure_ascii=False, indent=2, sort_keys=True) + "\n",
            encoding="utf-8",
        )
        self.result_path.chmod(0o400)
        directory.chmod(0o500)

    def _protected_rows(self) -> dict[str, list[tuple]]:
        tables = (
            "sources",
            "source_hashes",
            "source_metadata_observations",
            "source_relations",
            "media_sources",
            "recordings",
            "recording_sources",
            "recording_relations",
            "recording_metadata_observations",
            "publication_decisions",
            "publication_gate_decisions",
            "publication_manifest_imports",
            "identity_assertions",
            "identity_assertion_decisions",
            "identity_cannot_link_decisions",
            "identity_cluster_memberships",
            "identity_cluster_versions",
            "identity_clusters",
            "claim_catalog_links",
            "claim_import_issues",
        )
        return {
            table: [tuple(row) for row in self.connection.execute(f"SELECT * FROM {table}")]
            for table in tables
        }

    def tearDown(self) -> None:
        self.connection.close()
        for child in sorted(self.root.rglob("*"), reverse=True):
            try:
                child.chmod(0o700 if child.is_dir() else 0o600)
            except OSError:
                pass
        self.temporary.cleanup()

    def _assert_contract(self, value: dict, filename: str) -> None:
        path = self.root / filename
        path.write_text(json.dumps(value, indent=2) + "\n", encoding="utf-8")
        completed = subprocess.run(
            [
                sys.executable,
                str(REPOSITORY_ROOT / "scripts/validate-json-contracts.py"),
                "--validate",
                "corpus/schemas/local-window-catalog-admission.schema.json",
                str(path),
            ],
            cwd=REPOSITORY_ROOT,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            check=False,
        )
        self.assertEqual(completed.returncode, 0, completed.stdout + completed.stderr)

    def test_private_admission_is_idempotent_and_yields_truthful_asr_input(self) -> None:
        protected_before = self._protected_rows()
        before_publication = self.connection.execute(
            "SELECT count(*) FROM publication_decisions"
        ).fetchone()[0]
        validated = validate_local_window_result_file(
            self.connection, self.result_path, observed_at=self.observed_at
        )
        self.assertEqual(validated["status"], "validated")
        self._assert_contract(validated, "validated-admission.json")

        admitted = import_local_window_result(
            self.connection, self.result_path, observed_at=self.observed_at
        )
        replay = import_local_window_result(
            self.connection, self.result_path, observed_at=self.observed_at
        )
        self.assertEqual(admitted, replay)
        self.assertEqual(admitted["status"], "admitted")
        self._assert_contract(admitted, "admitted-window.json")
        self.assertEqual(admitted["statistics"]["publication_decisions"], 0)
        self.assertEqual(admitted["statistics"]["identity_claims"], 0)
        self.assertEqual(
            self.connection.execute("SELECT count(*) FROM publication_decisions").fetchone()[0],
            before_publication,
        )

        run = self.connection.execute(
            """
            SELECT stage, started_at, completed_at, parameters_json, environment_json
            FROM processing_runs WHERE processing_run_id = ?
            """,
            (admitted["admission_processing_run_id"],),
        ).fetchone()
        self.assertEqual(run["stage"], "local_window_result_admission")
        self.assertEqual(run["started_at"], self.observed_at)
        self.assertEqual(run["completed_at"], self.observed_at)
        self.assertEqual(
            json.loads(run["parameters_json"])["run_semantics"],
            "catalog_admission_verification_not_extraction_execution",
        )
        self.assertEqual(
            json.loads(run["environment_json"])["producer_extraction_time_state"],
            "not_present_in_local_window_result_v1",
        )
        self.assertEqual(
            self.connection.execute(
                "SELECT count(*) FROM artifacts WHERE processing_run_id = ? AND visibility = 'private'",
                (admitted["admission_processing_run_id"],),
            ).fetchone()[0],
            2,
        )
        self.assertEqual(
            self.connection.execute("SELECT count(*) FROM pragma_foreign_key_check").fetchone()[0],
            0,
        )
        self.assertEqual(self._protected_rows(), protected_before)

        asr_reference = admitted["asr_work_order_inputs"][0]
        input_row = self.connection.execute(
            """
            SELECT processing_run_id, storage_uri, sha256, byte_count, visibility
            FROM artifacts WHERE artifact_id = ?
            """,
            (asr_reference["input"]["artifact_id"],),
        ).fetchone()
        self.assertEqual(
            input_row["processing_run_id"],
            asr_reference["input"]["parent_processing_run_id"],
        )
        self.assertEqual(input_row["sha256"], asr_reference["input"]["expected_sha256"])
        self.assertEqual(input_row["visibility"], "private")
        rendition = self.connection.execute(
            "SELECT recording_id, media_id FROM renditions WHERE rendition_id = ?",
            (asr_reference["catalog_context"]["rendition_id"],),
        ).fetchone()
        self.assertEqual(rendition["recording_id"], self.recording_id)
        self.assertEqual(rendition["media_id"], asr_reference["input"]["media_id"])
        self.assertEqual(asr_reference["window"], {"offset_ms": 0, "duration_ms": None})
        self.assertEqual(
            asr_reference["source_time_mapping"]["artifact_zero_maps_to_source_ms"],
            1000,
        )

        asr_order = json.loads(
            (REPOSITORY_ROOT / "pipeline/examples/asr-whispercpp-work-order.example.json")
            .read_text(encoding="utf-8")
        )
        asr_order["input"] = asr_reference["input"]
        asr_order["catalog_context"] = asr_reference["catalog_context"]
        asr_order["window"] = asr_reference["window"]
        asr_path = self.root / "truthful-asr-work-order.json"
        asr_path.write_text(json.dumps(asr_order), encoding="utf-8")
        contract = subprocess.run(
            [
                sys.executable,
                str(REPOSITORY_ROOT / "scripts/validate-json-contracts.py"),
                "--validate",
                "pipeline/schemas/asr-whispercpp-work-order.schema.json",
                str(asr_path),
            ],
            cwd=REPOSITORY_ROOT,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            check=False,
        )
        self.assertEqual(contract.returncode, 0, contract.stdout + contract.stderr)

        with self.assertRaises(ResultImportError):
            import_local_window_result(
                self.connection,
                self.result_path,
                observed_at="2026-08-27T23:30:00Z",
            )

    def test_modes_tree_paths_hashes_and_catalog_lineage_fail_closed(self) -> None:
        result_body = self.result_path.read_bytes()
        self.result_path.chmod(0o600)
        with self.assertRaises(ResultImportError):
            validate_local_window_result_file(
                self.connection, self.result_path, observed_at=self.observed_at
            )
        self.result_path.chmod(0o400)

        result_dir = self.result_path.parent
        result_dir.chmod(0o700)
        extra = result_dir / "unexpected.txt"
        extra.write_text("not admitted\n", encoding="utf-8")
        extra.chmod(0o400)
        result_dir.chmod(0o500)
        with self.assertRaises(ResultImportError):
            validate_local_window_result_file(
                self.connection, self.result_path, observed_at=self.observed_at
            )
        result_dir.chmod(0o700)
        extra.unlink()
        result_dir.chmod(0o500)

        result = json.loads(result_body)
        audio_path = Path(
            next(
                row["path"]
                for row in result["artifacts"]
                if row["artifact_kind"] == "window_audio_16khz_mono_flac"
            )
        )
        audio_body = audio_path.read_bytes()
        audio_path.chmod(0o600)
        with self.assertRaises(ResultImportError):
            validate_local_window_result_file(
                self.connection, self.result_path, observed_at=self.observed_at
            )
        audio_path.chmod(0o400)
        audio_path.chmod(0o600)
        audio_path.write_bytes(audio_body + b"tamper")
        audio_path.chmod(0o400)
        with self.assertRaises(ResultImportError):
            validate_local_window_result_file(
                self.connection, self.result_path, observed_at=self.observed_at
            )
        audio_path.chmod(0o600)
        audio_path.write_bytes(audio_body)
        audio_path.chmod(0o400)

        tampered = copy.deepcopy(result)
        tampered["result_path"] = str(self.root / "wrong" / "result.json")
        self._rewrite_result(tampered)
        with self.assertRaises(ResultImportError):
            validate_local_window_result_file(
                self.connection, self.result_path, observed_at=self.observed_at
            )
        self.result_path.parent.chmod(0o700)
        self.result_path.chmod(0o600)
        self.result_path.write_bytes(result_body)
        self.result_path.chmod(0o400)
        self.result_path.parent.chmod(0o500)

        acquisition_digest = hashlib.sha256(
            json.dumps(
                json.loads(self.acquisition_result_path.read_text(encoding="utf-8")),
                ensure_ascii=False,
                sort_keys=True,
                separators=(",", ":"),
            ).encode("utf-8")
        ).hexdigest()
        batch = self.connection.execute(
            """
            SELECT import_batch_id FROM import_batches
            WHERE importer_name = 'acquisition_result_v1' AND input_sha256 = ?
            """,
            (acquisition_digest,),
        ).fetchone()
        self.connection.execute(
            "UPDATE import_batches SET status = 'failed' WHERE import_batch_id = ?",
            (batch["import_batch_id"],),
        )
        with self.assertRaises(ResultImportError):
            validate_local_window_result_file(
                self.connection, self.result_path, observed_at=self.observed_at
            )

    def test_forged_probe_work_order_commands_tools_and_profile_fail_closed(self) -> None:
        original_result_body = self.result_path.read_bytes()
        original = json.loads(original_result_body)
        audio = next(
            row
            for row in original["artifacts"]
            if row["artifact_kind"] == "window_audio_16khz_mono_flac"
        )
        audio_path = Path(audio["path"])
        original_audio = audio_path.read_bytes()
        forged_audio = b"not a flac file\n"
        audio_path.chmod(0o600)
        audio_path.write_bytes(forged_audio)
        audio_path.chmod(0o400)
        forged = copy.deepcopy(original)
        forged_artifact = next(
            row
            for row in forged["artifacts"]
            if row["artifact_kind"] == "window_audio_16khz_mono_flac"
        )
        forged_digest = hashlib.sha256(forged_audio).hexdigest()
        forged_artifact["sha256"] = forged_digest
        forged_artifact["byte_count"] = len(forged_audio)
        key = "\x1f".join(
            (
                forged["bundle_id"],
                forged["window"]["window_id"],
                forged_artifact["artifact_kind"],
                forged_digest,
            )
        ).encode("utf-8")
        forged_artifact["artifact_id"] = (
            "artifact_" + hashlib.sha256(key).hexdigest()[:32]
        )
        self._rewrite_result(forged)
        with self.assertRaises(ResultImportError):
            validate_local_window_result_file(
                self.connection, self.result_path, observed_at=self.observed_at
            )
        audio_path.chmod(0o600)
        audio_path.write_bytes(original_audio)
        audio_path.chmod(0o400)

        proxy = next(
            row
            for row in original["artifacts"]
            if row["artifact_kind"] == "window_low_resolution_cfr_proxy"
        )
        proxy_path = Path(proxy["path"])
        original_proxy = proxy_path.read_bytes()
        extra_stream_proxy = self.root / "proxy-with-extra-audio.mp4"
        subprocess.run(
            [
                str(self.ffmpeg),
                "-hide_banner",
                "-nostdin",
                "-loglevel",
                "error",
                "-i",
                str(proxy_path),
                "-map",
                "0:v:0",
                "-map",
                "0:a:0",
                "-map",
                "0:a:0",
                "-c",
                "copy",
                str(extra_stream_proxy),
            ],
            check=True,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.PIPE,
        )
        extra_stream_body = extra_stream_proxy.read_bytes()
        proxy_path.chmod(0o600)
        proxy_path.write_bytes(extra_stream_body)
        proxy_path.chmod(0o400)
        forged = copy.deepcopy(original)
        forged_proxy = next(
            row
            for row in forged["artifacts"]
            if row["artifact_kind"] == "window_low_resolution_cfr_proxy"
        )
        forged_proxy["sha256"] = hashlib.sha256(extra_stream_body).hexdigest()
        forged_proxy["byte_count"] = len(extra_stream_body)
        key = "\x1f".join(
            (
                forged["bundle_id"],
                forged["window"]["window_id"],
                forged_proxy["artifact_kind"],
                forged_proxy["sha256"],
            )
        ).encode("utf-8")
        forged_proxy["artifact_id"] = (
            "artifact_" + hashlib.sha256(key).hexdigest()[:32]
        )
        self._rewrite_result(forged)
        with self.assertRaises(ResultImportError):
            validate_local_window_result_file(
                self.connection, self.result_path, observed_at=self.observed_at
            )
        proxy_path.chmod(0o600)
        proxy_path.write_bytes(original_proxy)
        proxy_path.chmod(0o400)

        for mutate in (
            lambda value: value.__setitem__("work_order_sha256", "f" * 64),
            lambda value: value["commands"].__setitem__(
                0, [value["tools"]["ffmpeg"]["path"], "-version"]
            ),
            lambda value: value["tools"]["ffprobe"].__setitem__(
                "path", "/does/not/exist/ffprobe"
            ),
            lambda value: value["profile"].__setitem__("ffmpeg_threads", 0),
        ):
            tampered = copy.deepcopy(original)
            mutate(tampered)
            self._rewrite_result(tampered)
            with self.assertRaises(ResultImportError):
                validate_local_window_result_file(
                    self.connection, self.result_path, observed_at=self.observed_at
                )

        capped = copy.deepcopy(original)
        capped["limits"]["max_window_output_bytes"] = sum(
            artifact["byte_count"] for artifact in capped["artifacts"]
        )
        work_order = {
            "schema_version": 1,
            "job_id": capped["job_id"],
            "bundle_id": capped["bundle_id"],
            "source": {
                key: capped["source"][key]
                for key in (
                    "path",
                    "expected_sha256",
                    "byte_count",
                    "media_id",
                    "duration_ms",
                    "acquisition_result_path",
                    "acquisition_result_sha256",
                )
            },
            "window": capped["window"],
            "tools": capped["tools"],
            "profile": capped["profile"],
            "limits": capped["limits"],
            "output": {"root": str(self.result_path.parents[5])},
            "safety": capped["safety"],
        }
        old_prefix = capped["work_order_sha256"][:12]
        capped["work_order_sha256"] = hashlib.sha256(
            json.dumps(
                work_order,
                ensure_ascii=False,
                sort_keys=True,
                separators=(",", ":"),
            ).encode("utf-8")
        ).hexdigest()
        new_prefix = capped["work_order_sha256"][:12]
        capped["commands"] = [
            [argument.replace(old_prefix, new_prefix) for argument in command_row]
            for command_row in capped["commands"]
        ]
        self._rewrite_result(capped)
        with self.assertRaises(ResultImportError):
            validate_local_window_result_file(
                self.connection, self.result_path, observed_at=self.observed_at
            )

        self.result_path.parent.chmod(0o700)
        self.result_path.chmod(0o600)
        self.result_path.write_bytes(original_result_body)
        self.result_path.chmod(0o400)
        self.result_path.parent.chmod(0o500)

    def test_partial_related_candidate_mapping_cannot_create_asr_context(self) -> None:
        mapping_id = stable_id("rso", self.recording_id, self.canonical_source_id)
        self.connection.execute(
            """
            UPDATE recording_sources
            SET mapping_role = 'related_context', source_start_ms = 0,
                source_end_ms = 500, recording_start_ms = 10000,
                recording_end_ms = 10500, confidence_state = 'metadata_only'
            WHERE recording_source_id = ?
            """,
            (mapping_id,),
        )
        with self.assertRaises(ResultImportError):
            validate_local_window_result_file(
                self.connection, self.result_path, observed_at=self.observed_at
            )
        self.connection.execute(
            """
            UPDATE recording_sources
            SET mapping_role = 'complete_source', source_start_ms = NULL,
                source_end_ms = NULL, recording_start_ms = NULL,
                recording_end_ms = NULL, confidence_state = 'candidate'
            WHERE recording_source_id = ?
            """,
            (mapping_id,),
        )
        with self.assertRaises(ResultImportError):
            validate_local_window_result_file(
                self.connection, self.result_path, observed_at=self.observed_at
            )

    def test_admitted_window_path_cannot_be_rebound_to_new_valid_media(self) -> None:
        import_local_window_result(
            self.connection, self.result_path, observed_at=self.observed_at
        )
        replacement = run_json(
            [
                sys.executable,
                str(REPOSITORY_ROOT / "pipeline/local_window.py"),
                "run",
                "--work-order",
                str(self.order_paths[0]),
            ]
        )
        replacement_result = json.loads(
            Path(replacement["result_path"]).read_text(encoding="utf-8")
        )
        replacement_by_kind = {
            row["artifact_kind"]: row for row in replacement_result["artifacts"]
        }
        forged = json.loads(self.result_path.read_text(encoding="utf-8"))
        for artifact in forged["artifacts"]:
            source_artifact = replacement_by_kind[artifact["artifact_kind"]]
            target_path = Path(artifact["path"])
            target_path.chmod(0o600)
            target_path.write_bytes(Path(source_artifact["path"]).read_bytes())
            target_path.chmod(0o400)
            artifact["sha256"] = source_artifact["sha256"]
            artifact["byte_count"] = source_artifact["byte_count"]
            artifact["normalized_probe"] = source_artifact["normalized_probe"]
            key = "\x1f".join(
                (
                    forged["bundle_id"],
                    forged["window"]["window_id"],
                    artifact["artifact_kind"],
                    artifact["sha256"],
                )
            ).encode("utf-8")
            artifact["artifact_id"] = (
                "artifact_" + hashlib.sha256(key).hexdigest()[:32]
            )
        self._rewrite_result(forged)
        validated = validate_local_window_result_file(
            self.connection, self.result_path, observed_at=self.observed_at
        )
        self.assertEqual(validated["status"], "validated")
        with self.assertRaises(ResultImportError):
            import_local_window_result(
                self.connection, self.result_path, observed_at=self.observed_at
            )

    def test_disputed_catalog_objects_cannot_supply_asr_context(self) -> None:
        cases = (
            (
                "UPDATE sources SET review_state = 'disputed' WHERE source_id = ?",
                (self.canonical_source_id,),
                "UPDATE sources SET review_state = 'metadata_only' WHERE source_id = ?",
            ),
            (
                "UPDATE recordings SET review_state = 'disputed' WHERE recording_id = ?",
                (self.recording_id,),
                "UPDATE recordings SET review_state = 'metadata_only' WHERE recording_id = ?",
            ),
            (
                """
                UPDATE renditions SET review_state = 'disputed'
                WHERE recording_id = ? AND rendition_kind = 'acquired_source_media'
                """,
                (self.recording_id,),
                """
                UPDATE renditions SET review_state = 'unreviewed'
                WHERE recording_id = ? AND rendition_kind = 'acquired_source_media'
                """,
            ),
        )
        for dispute_sql, parameters, restore_sql in cases:
            self.connection.execute(dispute_sql, parameters)
            with self.assertRaises(ResultImportError):
                validate_local_window_result_file(
                    self.connection, self.result_path, observed_at=self.observed_at
                )
            self.connection.execute(restore_sql, parameters)

    def test_gap_parent_span_cannot_become_recording_time(self) -> None:
        result = json.loads(self.result_path.read_text(encoding="utf-8"))
        parent_media_id = result["source"]["media_id"]
        parent_rendition_id = stable_id(
            "rnd", self.recording_id, parent_media_id, "acquired_source_media"
        )
        self.connection.execute(
            """
            INSERT INTO timeline_map_spans(
                timeline_map_span_id, rendition_id, ordinal, media_start_ms,
                media_end_ms, recording_start_ms, recording_end_ms,
                mapping_kind, confidence_state
            ) VALUES(?, ?, 0, 0, 2200, 10000, 12200, 'gap', 'reviewed')
            """,
            (stable_id("tms", parent_rendition_id, "gap-test"), parent_rendition_id),
        )
        admitted = import_local_window_result(
            self.connection, self.result_path, observed_at=self.observed_at
        )
        for artifact in admitted["artifacts"]:
            rendition_id = artifact["rendition_contexts"][0]["rendition_id"]
            span = self.connection.execute(
                """
                SELECT recording_start_ms, recording_end_ms, mapping_kind
                FROM timeline_map_spans WHERE rendition_id = ?
                """,
                (rendition_id,),
            ).fetchone()
            self.assertIsNone(span["recording_start_ms"])
            self.assertIsNone(span["recording_end_ms"])
            self.assertEqual(span["mapping_kind"], "unknown")


if __name__ == "__main__":
    unittest.main()
