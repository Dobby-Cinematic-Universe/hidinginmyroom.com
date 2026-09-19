from __future__ import annotations

import copy
import hashlib
import json
import shutil
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path


CORPUS_ROOT = Path(__file__).resolve().parents[1]
REPOSITORY_ROOT = CORPUS_ROOT.parent
sys.path.insert(0, str(CORPUS_ROOT / "src"))

from himr_corpus import __version__  # noqa: E402
from himr_corpus.db import connect, migrate  # noqa: E402
from himr_corpus.exporter import build_release  # noqa: E402
from himr_corpus.ids import recording_id, source_id, stable_id  # noqa: E402
from himr_corpus.importers import import_youtube_discovery_candidates  # noqa: E402
from himr_corpus.result_importers import (  # noqa: E402
    ResultImportError,
    import_acquisition_result,
    import_preprocess_result,
    validate_preprocess_result,
)
from himr_corpus.validation import validate_database  # noqa: E402


OBSERVED_AT = "2026-08-26T20:00:00Z"


def producer_id(prefix: str, *parts: object) -> str:
    body = json.dumps(
        list(parts), ensure_ascii=False, sort_keys=True, separators=(",", ":")
    ).encode("utf-8")
    return f"{prefix}_{hashlib.sha256(body).hexdigest()[:32]}"


def artifact_id(run_id: str, kind: str, digest: str) -> str:
    body = json.dumps(
        {"processing_run_id": run_id, "kind": kind, "sha256": digest},
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
    ).encode("utf-8")
    return f"artifact_{hashlib.sha256(body).hexdigest()[:32]}"


class ResultImporterTests(unittest.TestCase):
    def setUp(self):
        work_root = CORPUS_ROOT / "work"
        work_root.mkdir(exist_ok=True)
        self.temporary = tempfile.TemporaryDirectory(prefix="result-import-", dir=work_root)
        self.root = Path(self.temporary.name)
        self.connection = connect(self.root / "corpus.sqlite3")
        migrate(self.connection)

    def tearDown(self):
        self.connection.close()
        self.temporary.cleanup()

    def _run_json(self, command: list[str]) -> dict:
        completed = subprocess.run(
            command,
            cwd=REPOSITORY_ROOT,
            check=False,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
        )
        if completed.returncode:
            self.fail(
                f"Producer failed with exit {completed.returncode}: {completed.stderr}"
            )
        return json.loads(completed.stdout)

    def _validate_contract(self, schema: str, instance: Path) -> None:
        completed = subprocess.run(
            [
                sys.executable,
                str(REPOSITORY_ROOT / "scripts/validate-json-contracts.py"),
                "--validate",
                schema,
                str(instance),
            ],
            cwd=REPOSITORY_ROOT,
            check=False,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
        )
        if completed.returncode:
            self.fail(
                "Generated producer result failed its JSON contract:\n"
                f"{completed.stdout}{completed.stderr}"
            )

    def _validate_contract_value(
        self, schema: str, instance: dict, filename: str
    ) -> None:
        instance_path = self.root / filename
        instance_path.write_text(json.dumps(instance), encoding="utf-8")
        self._validate_contract(schema, instance_path)

    def _seed_recording_mapping(self, *, platform: str, source_kind: str, native_id: str):
        source = source_id(platform, source_kind, native_id)
        recording = recording_id(f"synthetic:{platform}:{source_kind}:{native_id}")
        batch = stable_id("imp", "result-import-test-seed", native_id)
        self.connection.execute(
            """
            INSERT INTO import_batches(
                import_batch_id, importer_name, importer_version, input_sha256,
                source_snapshot_date, started_at, completed_at, status, statistics_json
            ) VALUES(?, 'result-import-test-seed', '1', ?, NULL, ?, ?, 'completed', '{}')
            """,
            (batch, (native_id.encode("utf-8").hex() + "0" * 64)[:64], OBSERVED_AT, OBSERVED_AT),
        )
        self.connection.execute(
            """
            INSERT INTO sources(
                source_id, platform, source_kind, native_id, canonical_url, title,
                observed_at, access_state, review_state, metadata_json,
                created_by_import_batch_id, created_at, updated_at
            ) VALUES(?, ?, ?, ?, NULL, 'Synthetic source', ?, 'public',
                     'metadata_only', '{}', ?, ?, ?)
            """,
            (source, platform, source_kind, native_id, OBSERVED_AT, batch, OBSERVED_AT, OBSERVED_AT),
        )
        self.connection.execute(
            """
            INSERT INTO recordings(
                recording_id, canonical_key, slug, title, date_basis,
                recording_type, review_state, metadata_json, created_at, updated_at
            ) VALUES(?, ?, ?, 'Synthetic recording', 'test', 'video',
                     'metadata_only', '{}', ?, ?)
            """,
            (
                recording,
                f"synthetic:{native_id}",
                f"synthetic-{native_id}",
                OBSERVED_AT,
                OBSERVED_AT,
            ),
        )
        self.connection.execute(
            """
            INSERT INTO recording_sources(
                recording_source_id, recording_id, source_id, mapping_role,
                mapping_method, confidence_state, metadata_json
            ) VALUES(?, ?, ?, 'complete_source', 'test_fixture', 'reviewed', '{}')
            """,
            (stable_id("rso", recording, source, "complete_source"), recording, source),
        )
        return source, recording

    def _write_acquisition_fixture(
        self,
        *,
        platform: str,
        source_kind: str,
        native_id: str,
        title: str,
        access_state: str,
    ) -> tuple[Path, dict]:
        payload_path = self.root / f"{native_id}-payload.bin"
        payload_bytes = f"verified acquisition payload for {native_id}".encode()
        payload_path.write_bytes(payload_bytes)
        payload_uri = payload_path.resolve().as_uri()
        digest = hashlib.sha256(payload_bytes).hexdigest()
        media_id = f"media_sha256_{digest}"
        producer_source_id = producer_id(
            "source", platform, source_kind, native_id
        )
        completed_at = "2026-08-26T20:00:01Z"
        probe = {
            "media": {
                "media_id": media_id,
                "sha256": digest,
                "byte_count": len(payload_bytes),
            },
            "format": {"duration_ms": 1000},
        }
        result = {
            "schema_version": 1,
            "job_id": f"synthetic-acquisition-{native_id}",
            "adapter": "local_file",
            "status": "completed",
            "dry_run": False,
            "reused": False,
            "work_order_sha256": hashlib.sha256(native_id.encode()).hexdigest(),
            "started_at": "2026-08-26T20:00:00Z",
            "completed_at": completed_at,
            "duration_ms": 1000,
            "source": {
                "platform": platform,
                "source_kind": source_kind,
                "native_id": native_id,
                "canonical_url": f"https://www.youtube.com/watch?v={native_id}",
                "title": title,
                "published_at": None,
                "access_state": access_state,
            },
            "limits": {
                "max_job_bytes": 1000,
                "global_cache_cap_bytes": 10000,
                "free_space_floor_bytes": 0,
            },
            "capacity_before": {
                "filesystem_path": str(self.root),
                "managed_bytes": 0,
                "free_bytes": 10000,
                "reserve_bytes": len(payload_bytes),
                "projected_managed_bytes": len(payload_bytes),
                "projected_free_bytes": 10000 - len(payload_bytes),
                "global_cache_cap_bytes": 10000,
                "free_space_floor_bytes": 0,
            },
            "capacity_after": {
                "filesystem_path": str(self.root),
                "managed_bytes": len(payload_bytes),
                "free_bytes": 10000 - len(payload_bytes),
                "reserve_bytes": 0,
                "projected_managed_bytes": len(payload_bytes),
                "projected_free_bytes": 10000 - len(payload_bytes),
                "global_cache_cap_bytes": 10000,
                "free_space_floor_bytes": 0,
            },
            "commands": [["local-copy", str(payload_path)]],
            "source_observation": {},
            "selected_remote_metadata": {},
            "admission": {
                "media_id": media_id,
                "sha256": digest,
                "byte_count": len(payload_bytes),
                "path": str(payload_path.resolve()),
                "storage_uri": payload_uri,
                "normalized_probe": probe,
            },
            "catalog_records": {
                "sources": [
                    {
                        "source_id": producer_source_id,
                        "platform": platform,
                        "source_kind": source_kind,
                        "native_id": native_id,
                        "parent_source_id": None,
                        "canonical_url": f"https://www.youtube.com/watch?v={native_id}",
                        "historical_url": None,
                        "title": title,
                        "published_at": None,
                        "observed_at": completed_at,
                        "access_state": access_state,
                        "review_state": "metadata_only",
                        "metadata_json": {"fixture": "verified_acquisition"},
                        "created_at": completed_at,
                        "updated_at": completed_at,
                    }
                ],
                "media_objects": [
                    {
                        "media_id": media_id,
                        "sha256": digest,
                        "byte_count": len(payload_bytes),
                        "media_kind": "video",
                        "mime_type": "video/mp4",
                        "container": "mp4",
                        "duration_ms": 1000,
                        "ffprobe_json": probe,
                        "first_cataloged_at": completed_at,
                        "integrity_state": "verified",
                    }
                ],
                "media_locations": [
                    {
                        "media_location_id": producer_id(
                            "media_location", media_id, payload_uri
                        ),
                        "media_id": media_id,
                        "storage_uri": payload_uri,
                        "storage_class": "local_hot_cache",
                        "verified_at": completed_at,
                        "is_primary": 1,
                    }
                ],
                "media_sources": [
                    {
                        "media_source_id": producer_id(
                            "media_source", media_id, producer_source_id
                        ),
                        "media_id": media_id,
                        "source_id": producer_source_id,
                        "retrieved_at": completed_at,
                        "retrieval_tool": "synthetic",
                        "retrieval_tool_version": "1",
                        "source_snapshot_id": None,
                    }
                ],
            },
            "result_path": str(self.root / f"{native_id}-result.json"),
            "errors": [],
        }
        result_path = Path(result["result_path"])
        result_path.write_text(json.dumps(result, sort_keys=True), encoding="utf-8")
        return result_path, result

    def test_acquisition_appends_ranked_metadata_for_existing_discovery_candidate(self):
        native_id = "acquirefix1"
        candidate_path = self.root / "candidate.jsonl"
        candidate_path.write_text(
            json.dumps(
                {
                    "id": native_id,
                    "title": "Unverified discovery title",
                    "channel": "Search result channel",
                    "url": f"https://www.youtube.com/watch?v={native_id}",
                },
                sort_keys=True,
            )
            + "\n",
            encoding="utf-8",
        )
        import_youtube_discovery_candidates(
            self.connection,
            candidate_path,
            observed_at="2026-08-27T00:00:00Z",
            query_label="acquisition precedence regression",
        )
        canonical_source_id = source_id("youtube", "youtube_video", native_id)
        candidate = self.connection.execute(
            """
            SELECT source.current_metadata_observation_id, source.access_state,
                   observation.quality_rank
            FROM sources AS source
            JOIN source_metadata_observations AS observation
              ON observation.source_metadata_observation_id =
                 source.current_metadata_observation_id
            WHERE source.source_id = ?
            """,
            (canonical_source_id,),
        ).fetchone()
        self.assertEqual(candidate["quality_rank"], 100)
        self.assertEqual(candidate["access_state"], "unknown")

        result_path, result = self._write_acquisition_fixture(
            platform="youtube",
            source_kind="youtube_video",
            native_id=native_id,
            title="Locally verified acquisition title",
            access_state="unknown",
        )
        first = import_acquisition_result(self.connection, result_path)
        source = self.connection.execute(
            "SELECT * FROM sources WHERE source_id = ?", (canonical_source_id,)
        ).fetchone()
        observations = [
            dict(row)
            for row in self.connection.execute(
                """
                SELECT observation.*, batch.importer_name
                FROM source_metadata_observations AS observation
                JOIN import_batches AS batch
                  ON batch.import_batch_id = observation.import_batch_id
                WHERE observation.source_id = ?
                ORDER BY observation.quality_rank
                """,
                (canonical_source_id,),
            )
        ]
        self.assertEqual(
            [(row["importer_name"], row["quality_rank"]) for row in observations],
            [
                ("youtube_discovery_candidates", 100),
                ("acquisition_result_v1", 700),
            ],
        )
        acquisition_observation = observations[-1]
        self.assertEqual(
            source["current_metadata_observation_id"],
            acquisition_observation["source_metadata_observation_id"],
        )
        self.assertEqual(source["access_state"], "unknown")
        self.assertEqual(source["title"], "Locally verified acquisition title")
        self.assertEqual(source["review_state"], "metadata_only")
        self.assertEqual(
            json.loads(source["metadata_json"]), {"fixture": "verified_acquisition"}
        )
        self.assertEqual(source["observed_at"], "2026-08-27T00:00:00Z")
        self.assertEqual(
            acquisition_observation["quality_basis"],
            "acquisition_result_v1: locally verified acquisition result",
        )
        self.assertIsNotNone(acquisition_observation["import_observation_id"])
        receipt = self.connection.execute(
            "SELECT * FROM import_observations WHERE import_observation_id = ?",
            (acquisition_observation["import_observation_id"],),
        ).fetchone()
        self.assertEqual(receipt["observed_at"], result["completed_at"])
        self.assertEqual(receipt["completed_at"], result["completed_at"])
        self.assertEqual(receipt["status"], "completed")
        validate_database(self.connection)

        replay_snapshot = {
            "source": dict(source),
            "observations": observations,
            "import_observations": [
                dict(row)
                for row in self.connection.execute(
                    "SELECT * FROM import_observations ORDER BY import_observation_id"
                )
            ],
        }
        self.assertEqual(first, import_acquisition_result(self.connection, result_path))
        self.assertEqual(
            replay_snapshot,
            {
                "source": dict(
                    self.connection.execute(
                        "SELECT * FROM sources WHERE source_id = ?",
                        (canonical_source_id,),
                    ).fetchone()
                ),
                "observations": [
                    dict(row)
                    for row in self.connection.execute(
                        """
                        SELECT observation.*, batch.importer_name
                        FROM source_metadata_observations AS observation
                        JOIN import_batches AS batch
                          ON batch.import_batch_id = observation.import_batch_id
                        WHERE observation.source_id = ?
                        ORDER BY observation.quality_rank
                        """,
                        (canonical_source_id,),
                    )
                ],
                "import_observations": [
                    dict(row)
                    for row in self.connection.execute(
                        "SELECT * FROM import_observations ORDER BY import_observation_id"
                    )
                ],
            },
        )
        validate_database(self.connection)

    def test_acquisition_replay_upgrades_its_pre_observation_projection(self):
        native_id = "oldacquire1"
        result_path, result = self._write_acquisition_fixture(
            platform="youtube",
            source_kind="youtube_video",
            native_id=native_id,
            title="Pre-observation acquisition title",
            access_state="unknown",
        )
        result_digest = hashlib.sha256(
            json.dumps(
                result,
                ensure_ascii=False,
                sort_keys=True,
                separators=(",", ":"),
            ).encode("utf-8")
        ).hexdigest()
        batch_id = stable_id("imp", "acquisition_result_v1", result_digest)
        source_row = result["catalog_records"]["sources"][0]
        canonical_source_id = source_id("youtube", "youtube_video", native_id)
        self.connection.execute(
            """
            INSERT INTO import_batches(
                import_batch_id, importer_name, importer_version, input_sha256,
                source_snapshot_date, started_at, completed_at, status,
                statistics_json
            ) VALUES(?, 'acquisition_result_v1', ?, ?, NULL, ?, ?, 'completed', '{}')
            """,
            (
                batch_id,
                __version__,
                result_digest,
                result["started_at"],
                result["completed_at"],
            ),
        )
        self.connection.execute(
            """
            INSERT INTO sources(
                source_id, platform, source_kind, native_id, parent_source_id,
                canonical_url, historical_url, title, published_at, observed_at,
                access_state, review_state, metadata_json,
                created_by_import_batch_id, created_at, updated_at
            ) VALUES(?, ?, ?, ?, NULL, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                canonical_source_id,
                source_row["platform"],
                source_row["source_kind"],
                source_row["native_id"],
                source_row["canonical_url"],
                source_row["historical_url"],
                source_row["title"],
                source_row["published_at"],
                source_row["observed_at"],
                source_row["access_state"],
                source_row["review_state"],
                json.dumps(
                    source_row["metadata_json"],
                    sort_keys=True,
                    separators=(",", ":"),
                ),
                batch_id,
                source_row["created_at"],
                source_row["updated_at"],
            ),
        )

        first = import_acquisition_result(self.connection, result_path)
        observations = self.connection.execute(
            """
            SELECT * FROM source_metadata_observations
            WHERE source_id = ?
            """,
            (canonical_source_id,),
        ).fetchall()
        self.assertEqual(len(observations), 1)
        self.assertEqual(observations[0]["quality_rank"], 700)
        self.assertEqual(
            observations[0]["quality_basis"],
            "acquisition_result_v1: locally verified acquisition result",
        )
        self.assertNotIn("pre-observation projection", observations[0]["quality_basis"])
        self.assertIsNotNone(observations[0]["import_observation_id"])
        self.assertEqual(
            self.connection.execute(
                "SELECT current_metadata_observation_id FROM sources WHERE source_id = ?",
                (canonical_source_id,),
            ).fetchone()[0],
            observations[0]["source_metadata_observation_id"],
        )
        validate_database(self.connection)
        self.assertEqual(first, import_acquisition_result(self.connection, result_path))
        self.assertEqual(
            self.connection.execute(
                "SELECT count(*) FROM source_metadata_observations WHERE source_id = ?",
                (canonical_source_id,),
            ).fetchone()[0],
            1,
        )
        validate_database(self.connection)

    @unittest.skipUnless(
        shutil.which("ffmpeg") and shutil.which("ffprobe"),
        "FFmpeg and FFprobe are required for producer-to-catalog integration",
    )
    def test_producer_results_import_end_to_end_idempotently_and_stay_private(self):
        source_path = self.root / "source.mp4"
        subprocess.run(
            [
                shutil.which("ffmpeg") or "ffmpeg",
                "-hide_banner",
                "-loglevel",
                "error",
                "-f",
                "lavfi",
                "-i",
                "testsrc=size=160x90:rate=10",
                "-f",
                "lavfi",
                "-i",
                "sine=frequency=600:sample_rate=16000",
                "-t",
                "1",
                "-c:v",
                "libx264",
                "-pix_fmt",
                "yuv420p",
                "-c:a",
                "aac",
                str(source_path),
            ],
            check=True,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.PIPE,
        )

        canonical_source, recording = self._seed_recording_mapping(
            platform="local_test", source_kind="video", native_id="result-e2e-001"
        )
        acquired_root = self.root / "acquired"
        acquisition_order = {
            "schema_version": 1,
            "job_id": "result-e2e-acquisition",
            "adapter": "local_file",
            "source": {
                "platform": "local_test",
                "source_kind": "video",
                "native_id": "result-e2e-001",
                "canonical_url": None,
                "title": "Result importer fixture",
                "published_at": None,
                "access_state": "unknown",
            },
            "adapter_config": {
                "path": str(source_path),
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
        acquisition_plan = self._run_json(
            [
                sys.executable,
                str(REPOSITORY_ROOT / "acquisition/acquire.py"),
                "run",
                "--work-order",
                str(acquisition_order_path),
                "--dry-run",
            ]
        )
        self._validate_contract_value(
            "acquisition/schemas/result.schema.json",
            acquisition_plan,
            "acquisition-plan.json",
        )
        acquisition_result = self._run_json(
            [
                sys.executable,
                str(REPOSITORY_ROOT / "acquisition/acquire.py"),
                "run",
                "--work-order",
                str(acquisition_order_path),
            ]
        )
        acquisition_result_path = Path(acquisition_result["result_path"])
        self._validate_contract(
            "acquisition/schemas/result.schema.json", acquisition_result_path
        )
        first_acquisition_import = import_acquisition_result(
            self.connection, acquisition_result_path
        )
        self.assertEqual(
            first_acquisition_import,
            import_acquisition_result(self.connection, acquisition_result_path),
        )
        self.assertEqual(first_acquisition_import["source_id"], canonical_source)
        self.assertNotEqual(
            acquisition_result["catalog_records"]["sources"][0]["source_id"],
            canonical_source,
        )
        media_id = first_acquisition_import["media_id"]
        media_row = self.connection.execute(
            "SELECT first_cataloged_at FROM media_objects WHERE media_id = ?", (media_id,)
        ).fetchone()
        media_source_row = self.connection.execute(
            "SELECT retrieved_at FROM media_sources WHERE media_id = ?", (media_id,)
        ).fetchone()
        self.assertEqual(media_row["first_cataloged_at"], acquisition_result["completed_at"])
        self.assertEqual(media_source_row["retrieved_at"], acquisition_result["completed_at"])
        self.assertEqual(
            self.connection.execute(
                "SELECT count(*) FROM renditions WHERE recording_id = ? AND media_id = ?",
                (recording, media_id),
            ).fetchone()[0],
            1,
        )

        preprocess_root = self.root / "processed"
        profile = json.loads(
            (REPOSITORY_ROOT / "pipeline/profiles/cpu-balanced-v1.json").read_text(
                encoding="utf-8"
            )
        )
        preprocess_order = {
            "schema_version": 1,
            "job_id": "result-e2e-preprocess",
            "source": {
                "path": acquisition_result["admission"]["path"],
                "expected_sha256": acquisition_result["admission"]["sha256"],
                "first_cataloged_at": acquisition_result["completed_at"],
            },
            "output": {"root": str(preprocess_root)},
            "operations": {
                "probe": True,
                "audio_flac": True,
                "proxy": True,
                # Keep the producer-to-catalog test portable across FFmpeg builds;
                # routing observation ingestion is exercised by contract fixtures.
                "routing": False,
            },
            "profile": profile,
        }
        preprocess_order_path = self.root / "preprocess-order.json"
        preprocess_order_path.write_text(json.dumps(preprocess_order), encoding="utf-8")
        preprocess_plan = self._run_json(
            [
                sys.executable,
                str(REPOSITORY_ROOT / "pipeline/media_preprocess.py"),
                "run",
                "--work-order",
                str(preprocess_order_path),
                "--dry-run",
            ]
        )
        self._validate_contract_value(
            "pipeline/schemas/result.schema.json",
            preprocess_plan,
            "preprocess-plan.json",
        )
        preprocess_result = self._run_json(
            [
                sys.executable,
                str(REPOSITORY_ROOT / "pipeline/media_preprocess.py"),
                "run",
                "--work-order",
                str(preprocess_order_path),
            ]
        )
        preprocess_result_path = Path(preprocess_result["result_path"])
        self._validate_contract(
            "pipeline/schemas/result.schema.json", preprocess_result_path
        )
        first_preprocess_import = import_preprocess_result(
            self.connection, preprocess_result_path
        )
        self.assertEqual(
            first_preprocess_import,
            import_preprocess_result(self.connection, preprocess_result_path),
        )
        self.assertEqual(first_preprocess_import["source_renditions"], 1)
        self.assertEqual(first_preprocess_import["derived_renditions"], 2)
        self.assertGreaterEqual(first_preprocess_import["observations"], 0)
        first_derivation = preprocess_result["catalog_records"]["media_derivations"][0]
        first_child_media_id = first_derivation["child_media_id"]
        protected_output_tampers = [
            (
                "missing derivation",
                "DELETE FROM media_derivations WHERE child_media_id = ? AND parent_media_id = ? AND derivation_kind = ?",
                (
                    first_child_media_id,
                    first_derivation["parent_media_id"],
                    first_derivation["derivation_kind"],
                ),
            ),
            (
                "extra derivation",
                "INSERT INTO media_derivations(child_media_id, parent_media_id, derivation_kind, processing_run_id, metadata_json) VALUES(?, ?, 'unexpected_test_derivation', ?, '{}')",
                (
                    first_child_media_id,
                    first_derivation["parent_media_id"],
                    first_derivation["processing_run_id"],
                ),
            ),
            (
                "different derived media",
                "UPDATE media_objects SET duration_ms = duration_ms + 1 WHERE media_id = ?",
                (first_child_media_id,),
            ),
            (
                "different derived location",
                "UPDATE media_locations SET storage_class = 'tampered' WHERE media_id = ?",
                (first_child_media_id,),
            ),
            (
                "different derived rendition",
                "UPDATE renditions SET label = 'Tampered' WHERE media_id = ?",
                (first_child_media_id,),
            ),
        ]
        for label, statement, parameters in protected_output_tampers:
            with self.subTest(replay_output_tamper=label):
                self.connection.execute("SAVEPOINT replay_output_tamper")
                try:
                    cursor = self.connection.execute(statement, parameters)
                    self.assertGreater(cursor.rowcount, 0)
                    changes_after_tamper = self.connection.total_changes
                    with self.assertRaisesRegex(
                        ResultImportError, "completed preprocess replay"
                    ):
                        import_preprocess_result(
                            self.connection, preprocess_result_path
                        )
                    self.assertEqual(
                        self.connection.total_changes,
                        changes_after_tamper,
                        "a failed replay must not repair protected output rows",
                    )
                finally:
                    self.connection.execute("ROLLBACK TO replay_output_tamper")
                    self.connection.execute("RELEASE replay_output_tamper")

        replay_result = self._run_json(
            [
                sys.executable,
                str(REPOSITORY_ROOT / "pipeline/media_preprocess.py"),
                "run",
                "--work-order",
                str(preprocess_order_path),
            ]
        )
        replay_result_path = Path(replay_result["result_path"])
        self.assertEqual(replay_result["reuse"]["mode"], "verified_prior_result")
        self.assertNotEqual(
            replay_result["processing_run"]["processing_run_id"],
            preprocess_result["processing_run"]["processing_run_id"],
        )
        replay_import = import_preprocess_result(self.connection, replay_result_path)
        self.assertEqual(
            replay_import,
            import_preprocess_result(self.connection, replay_result_path),
        )
        changes_before_older_replay = self.connection.total_changes
        self.assertEqual(
            first_preprocess_import,
            import_preprocess_result(self.connection, preprocess_result_path),
        )
        self.assertEqual(
            self.connection.total_changes,
            changes_before_older_replay,
            "a completed older attempt must remain a read-only exact replay",
        )
        self.assertEqual(
            self.connection.execute(
                "SELECT count(*) FROM artifacts WHERE visibility <> 'private'"
            ).fetchone()[0],
            0,
        )
        self.assertEqual(
            self.connection.execute("SELECT count(*) FROM publication_decisions").fetchone()[0],
            0,
        )
        release_text = json.dumps(build_release(self.connection), sort_keys=True)
        self.assertNotIn(str(self.root), release_text)
        self.assertNotIn("file://", release_text)
        original_prior_bytes = preprocess_result_path.read_bytes()
        preprocess_result_path.chmod(0o644)
        preprocess_result_path.write_bytes(original_prior_bytes + b"\n")
        preprocess_result_path.chmod(0o444)
        with self.assertRaisesRegex(ResultImportError, "prior result SHA-256"):
            import_preprocess_result(self.connection, replay_result_path)

    def test_unknown_fields_and_cross_field_mismatch_are_rejected_without_writes(self):
        # A deliberately small completed acquisition envelope exercises the strict
        # boundary without invoking either producer.
        payload_path = self.root / "strict-payload.bin"
        payload_bytes = b"0123456789"
        payload_path.write_bytes(payload_bytes)
        payload_uri = payload_path.resolve().as_uri()
        digest = hashlib.sha256(payload_bytes).hexdigest()
        media_id = f"media_sha256_{digest}"
        producer_source_id = producer_id(
            "source", "local_test", "video", "strict-001"
        )
        probe = {
            "media": {"media_id": media_id, "sha256": digest, "byte_count": 10},
            "format": {"duration_ms": 1000},
        }
        result = {
            "schema_version": 1,
            "job_id": "synthetic-acquisition",
            "adapter": "local_file",
            "status": "completed",
            "dry_run": False,
            "reused": False,
            "work_order_sha256": "b" * 64,
            "started_at": "2026-08-26T20:00:00Z",
            "completed_at": "2026-08-26T20:00:01Z",
            "duration_ms": 1000,
            "source": {
                "platform": "local_test",
                "source_kind": "video",
                "native_id": "strict-001",
                "canonical_url": None,
                "title": "Strict fixture",
                "published_at": None,
                "access_state": "unknown",
            },
            "limits": {
                "max_job_bytes": 100,
                "global_cache_cap_bytes": 1000,
                "free_space_floor_bytes": 0,
            },
            "capacity_before": {
                "filesystem_path": "/private/cache",
                "managed_bytes": 0,
                "free_bytes": 1000,
                "reserve_bytes": 10,
                "projected_managed_bytes": 10,
                "projected_free_bytes": 990,
                "global_cache_cap_bytes": 1000,
                "free_space_floor_bytes": 0,
            },
            "capacity_after": {
                "filesystem_path": "/private/cache",
                "managed_bytes": 10,
                "free_bytes": 990,
                "reserve_bytes": 0,
                "projected_managed_bytes": 10,
                "projected_free_bytes": 990,
                "global_cache_cap_bytes": 1000,
                "free_space_floor_bytes": 0,
            },
            "commands": [["local-copy", "/private/source", "/private/stage"]],
            "source_observation": {},
            "selected_remote_metadata": {},
            "admission": {
                "media_id": media_id,
                "sha256": digest,
                "byte_count": 10,
                "path": str(payload_path.resolve()),
                "storage_uri": payload_uri,
                "normalized_probe": probe,
            },
            "catalog_records": {
                "sources": [
                    {
                        "source_id": producer_source_id,
                        "platform": "local_test",
                        "source_kind": "video",
                        "native_id": "strict-001",
                        "parent_source_id": None,
                        "canonical_url": None,
                        "historical_url": None,
                        "title": "Strict fixture",
                        "published_at": None,
                        "observed_at": "2026-08-26T20:00:01Z",
                        "access_state": "unknown",
                        "review_state": "metadata_only",
                        "metadata_json": {},
                        "created_at": "2026-08-26T20:00:01Z",
                        "updated_at": "2026-08-26T20:00:01Z",
                    }
                ],
                "media_objects": [
                    {
                        "media_id": media_id,
                        "sha256": digest,
                        "byte_count": 10,
                        "media_kind": "video",
                        "mime_type": "video/mp4",
                        "container": "mp4",
                        "duration_ms": 1000,
                        "ffprobe_json": probe,
                        "first_cataloged_at": "2026-08-26T20:00:01Z",
                        "integrity_state": "verified",
                    }
                ],
                "media_locations": [
                    {
                        "media_location_id": producer_id(
                            "media_location", media_id, payload_uri
                        ),
                        "media_id": media_id,
                        "storage_uri": payload_uri,
                        "storage_class": "local_hot_cache",
                        "verified_at": "2026-08-26T20:00:01Z",
                        "is_primary": 1,
                    }
                ],
                "media_sources": [
                    {
                        "media_source_id": producer_id(
                            "media_source", media_id, producer_source_id
                        ),
                        "media_id": media_id,
                        "source_id": producer_source_id,
                        "retrieved_at": "2026-08-26T20:00:01Z",
                        "retrieval_tool": "synthetic",
                        "retrieval_tool_version": "1",
                        "source_snapshot_id": None,
                    }
                ],
            },
            "result_path": "/private/result.json",
            "errors": [],
        }
        unknown = copy.deepcopy(result)
        unknown["unexpected"] = True
        unknown_path = self.root / "unknown.json"
        unknown_path.write_text(json.dumps(unknown), encoding="utf-8")
        with self.assertRaisesRegex(ResultImportError, "unknown"):
            import_acquisition_result(self.connection, unknown_path)

        mismatch = copy.deepcopy(result)
        mismatch["catalog_records"]["media_objects"][0]["byte_count"] = 11
        mismatch_path = self.root / "mismatch.json"
        mismatch_path.write_text(json.dumps(mismatch), encoding="utf-8")
        with self.assertRaisesRegex(ResultImportError, "byte_count"):
            import_acquisition_result(self.connection, mismatch_path)
        self.assertEqual(self.connection.execute("SELECT count(*) FROM import_batches").fetchone()[0], 0)
        self.assertEqual(self.connection.execute("SELECT count(*) FROM media_objects").fetchone()[0], 0)

        missing = copy.deepcopy(result)
        missing_path_value = (self.root / "missing-payload.bin").resolve()
        missing_uri = missing_path_value.as_uri()
        missing["admission"]["path"] = str(missing_path_value)
        missing["admission"]["storage_uri"] = missing_uri
        missing_location = missing["catalog_records"]["media_locations"][0]
        missing_location["storage_uri"] = missing_uri
        missing_location["media_location_id"] = producer_id(
            "media_location", media_id, missing_uri
        )
        missing_result_path = self.root / "missing-file-result.json"
        missing_result_path.write_text(json.dumps(missing), encoding="utf-8")
        with self.assertRaisesRegex(ResultImportError, "not a readable current file"):
            import_acquisition_result(self.connection, missing_result_path)

        payload_path.write_bytes(b"abcdefghij")
        tampered_result_path = self.root / "tampered-file-result.json"
        tampered_result_path.write_text(json.dumps(result), encoding="utf-8")
        with self.assertRaisesRegex(ResultImportError, "SHA-256"):
            import_acquisition_result(self.connection, tampered_result_path)
        payload_path.write_bytes(payload_bytes)
        self.assertEqual(self.connection.execute("SELECT count(*) FROM import_batches").fetchone()[0], 0)

        # This conflict is discovered only after the transaction has begun and the
        # source row has been attempted. The entire result import still rolls back.
        self.connection.execute(
            """
            INSERT INTO media_objects(
                media_id, sha256, byte_count, media_kind, first_cataloged_at,
                integrity_state
            ) VALUES(?, ?, 999, 'video', ?, 'verified')
            """,
            (media_id, digest, OBSERVED_AT),
        )
        valid_path = self.root / "valid-but-conflicting.json"
        valid_path.write_text(json.dumps(result), encoding="utf-8")
        with self.assertRaisesRegex(ResultImportError, "existing media identity"):
            import_acquisition_result(self.connection, valid_path)
        self.assertEqual(self.connection.execute("SELECT count(*) FROM import_batches").fetchone()[0], 0)
        self.assertEqual(
            self.connection.execute(
                "SELECT count(*) FROM import_observations"
            ).fetchone()[0],
            0,
        )
        self.assertEqual(self.connection.execute("SELECT count(*) FROM sources").fetchone()[0], 0)
        self.assertEqual(
            self.connection.execute(
                "SELECT count(*) FROM source_metadata_observations"
            ).fetchone()[0],
            0,
        )
        self.assertEqual(self.connection.execute("SELECT count(*) FROM external_ids").fetchone()[0], 0)

    def test_routing_candidates_become_private_observations_only_with_recording_context(self):
        _, recording = self._seed_recording_mapping(
            platform="local_test", source_kind="video", native_id="routing-001"
        )
        source_file = self.root / "routing-source.mp4"
        source_bytes = b"s" * 100
        source_file.write_bytes(source_bytes)
        source_uri = source_file.resolve().as_uri()
        digest = hashlib.sha256(source_bytes).hexdigest()
        media_id = f"media_sha256_{digest}"
        probe = {
            "media": {"media_id": media_id, "sha256": digest, "byte_count": 100},
            "format": {"duration_ms": 1000},
            "primary_streams": {"video_index": 0, "audio_index": 1},
        }
        self.connection.execute(
            """
            INSERT INTO media_objects(
                media_id, sha256, byte_count, media_kind, mime_type, container,
                duration_ms, ffprobe_json, first_cataloged_at, integrity_state
            ) VALUES(?, ?, 100, 'video', 'video/mp4', 'mp4', 1000, ?, ?, 'verified')
            """,
            (media_id, digest, json.dumps(probe), OBSERVED_AT),
        )
        rendition = stable_id("rnd", recording, media_id, "acquired_source_media")
        self.connection.execute(
            """
            INSERT INTO renditions(
                rendition_id, recording_id, media_id, rendition_kind,
                label, review_state, metadata_json
            ) VALUES(?, ?, ?, 'acquired_source_media', 'Fixture', 'unreviewed', '{}')
            """,
            (rendition, recording, media_id),
        )

        profile = json.loads(
            (REPOSITORY_ROOT / "pipeline/profiles/cpu-balanced-v1.json").read_text(
                encoding="utf-8"
            )
        )
        def tool_provenance(name: str) -> dict[str, object]:
            version_output = f"{name} test build"
            return {
                "name": name,
                "executable_sha256": hashlib.sha256(f"{name}-binary".encode()).hexdigest(),
                "executable_byte_count": 100,
                "version": version_output,
                "version_output": version_output,
                "version_output_sha256": hashlib.sha256(version_output.encode()).hexdigest(),
                "build_configuration": "--test-fixture",
            }
        parameters_json: dict[str, object] = {
            "contract_version": 1,
            "implementation_version": "test-v1",
            "operations": {
                "probe": True,
                "audio_flac": False,
                "proxy": False,
                "routing": True,
            },
            "profile": profile,
            "tools": {
                "ffmpeg": tool_provenance("ffmpeg"),
                "ffprobe": tool_provenance("ffprobe"),
            },
        }
        recipe_sha256 = hashlib.sha256(
            json.dumps(parameters_json, sort_keys=True, separators=(",", ":")).encode()
        ).hexdigest()
        execution_nonce = "1" * 32
        run_id = producer_id(
            "run_preprocess", digest, recipe_sha256, execution_nonce
        )
        output_root = self.root / "processed"
        object_dir = output_root / "media" / "sha256" / digest[:2] / digest
        recipe_dir = object_dir / "recipes" / recipe_sha256
        run_dir = recipe_dir / "executions" / run_id
        run_dir.mkdir(parents=True)
        run = {
            "processing_run_id": run_id,
            "stage": "media_preprocess",
            "implementation_version": "test-v1",
            "parameters_json": parameters_json,
            "environment_json": {
                "platform": "test",
                "python": "3.12.0",
                "cpu_only": True,
                "execution_nonce": execution_nonce,
                "tool_paths": {"ffmpeg": "/test/ffmpeg", "ffprobe": "/test/ffprobe"},
            },
            "started_at": "2026-08-26T20:01:00Z",
            "completed_at": "2026-08-26T20:01:01Z",
            "status": "completed",
        }
        routing = {
            "schema_version": 1,
            "parameters": {
                "scene_threshold_percent": profile["scene_threshold_percent"],
                "silence_noise_db": profile["silence_noise_db"],
                "silence_min_duration_ms": profile["silence_min_duration_ms"],
                "near_silent_fraction": profile["near_silent_fraction"],
            },
            "coverage": {"duration_ms": 1000, "has_video": True, "has_audio": True},
            "scene_changes": [{"timestamp_ms": 100, "score_percent": 25}],
            "silence_intervals": [{"start_ms": 200, "end_ms": 400, "duration_ms": 200}],
            "summary": {
                "scene_change_count": 1,
                "silence_interval_count": 1,
                "silent_duration_ms": 200,
                "silent_fraction": 0.2,
            },
            "routing_candidates": {
                "asr": "process",
                "ocr": "scene_keyframes",
                "visual": "scene_and_speech_windows",
                "diarization": "router_pending",
                "active_speaker": "router_pending",
            },
            "warning": "Routing only",
            "source_media_id": media_id,
        }
        probe_artifact_path = run_dir / "probe.normalized.json"
        probe_artifact_bytes = (json.dumps(probe, sort_keys=True) + "\n").encode()
        probe_artifact_path.write_bytes(probe_artifact_bytes)
        probe_artifact_digest = hashlib.sha256(probe_artifact_bytes).hexdigest()
        routing_artifact_path = run_dir / "routing.json"
        routing_artifact_bytes = (json.dumps(routing, sort_keys=True) + "\n").encode()
        routing_artifact_path.write_bytes(routing_artifact_bytes)
        routing_artifact_digest = hashlib.sha256(routing_artifact_bytes).hexdigest()
        artifacts = [
            {
                "artifact_id": artifact_id(
                    run["processing_run_id"],
                    "ffprobe_normalized_json",
                    probe_artifact_digest,
                ),
                "processing_run_id": run["processing_run_id"],
                "artifact_kind": "ffprobe_normalized_json",
                "storage_uri": probe_artifact_path.resolve().as_uri(),
                "path": str(probe_artifact_path.resolve()),
                "sha256": probe_artifact_digest,
                "byte_count": len(probe_artifact_bytes),
                "schema_version": 1,
                "visibility": "private",
                "media_kind": "document",
                "mime_type": "application/json",
                "normalized_probe": None,
            },
            {
                "artifact_id": artifact_id(
                    run["processing_run_id"],
                    "scene_silence_routing_json",
                    routing_artifact_digest,
                ),
                "processing_run_id": run["processing_run_id"],
                "artifact_kind": "scene_silence_routing_json",
                "storage_uri": routing_artifact_path.resolve().as_uri(),
                "path": str(routing_artifact_path.resolve()),
                "sha256": routing_artifact_digest,
                "byte_count": len(routing_artifact_bytes),
                "schema_version": 1,
                "visibility": "private",
                "media_kind": "document",
                "mime_type": "application/json",
                "normalized_probe": None,
            },
        ]
        core_artifacts = [
            {
                key: artifact[key]
                for key in (
                    "artifact_id",
                    "processing_run_id",
                    "artifact_kind",
                    "storage_uri",
                    "sha256",
                    "byte_count",
                    "schema_version",
                    "visibility",
                )
            }
            for artifact in artifacts
        ]
        result = {
            "schema_version": 1,
            "job_id": "routing-fixture",
            "status": "completed",
            "dry_run": False,
            "duration_ms": 1000,
            "processing_run": run,
            "input": {
                "path": str(source_file.resolve()),
                "storage_uri": source_uri,
                "media_id": media_id,
                "sha256": digest,
                "byte_count": 100,
                "stat_before": {"device": 1, "inode": 2, "byte_count": 100, "mtime_ns": 3},
                "stat_after": {"device": 1, "inode": 2, "byte_count": 100, "mtime_ns": 3},
                "unchanged": True,
                "catalog_observation": {
                    "first_cataloged_at": OBSERVED_AT,
                    "basis": "upstream_work_order",
                    "acquisition_timestamp_state": "not_claimed_by_preprocessing",
                },
            },
            "layout": {
                "output_root": str(output_root),
                "object_dir": str(object_dir),
                "recipe_dir": str(recipe_dir),
                "run_dir": str(run_dir),
                "recipe_id": f"recipe_preprocess_{recipe_sha256[:32]}",
                "recipe_sha256": recipe_sha256,
            },
            "steps": [
                {"name": "probe", "status": "completed", "command": ["ffprobe"], "output_path": str(probe_artifact_path.resolve())},
                {"name": "audio_flac", "status": "disabled", "command": None, "output_path": None},
                {"name": "proxy", "status": "disabled", "command": None, "output_path": None},
                {"name": "routing", "status": "completed", "command": ["ffmpeg"], "output_path": str(routing_artifact_path.resolve())},
            ],
            "artifacts": artifacts,
            "routing": routing,
            "reuse": {
                "mode": "none",
                "prior_processing_run_id": None,
                "prior_result_path": None,
                "prior_result_sha256": None,
                "verified_at": None,
            },
            "catalog_records": {
                "processing_runs": [run],
                "run_inputs": [
                    {
                        "run_input_id": producer_id(
                            "run_input",
                            run["processing_run_id"],
                            media_id,
                            "source_media",
                        ),
                        "processing_run_id": run["processing_run_id"],
                        "object_type": "media",
                        "object_id": media_id,
                        "input_role": "source_media",
                        "input_sha256": digest,
                    }
                ],
                "media_objects": [
                    {
                        "media_id": media_id,
                        "sha256": digest,
                        "byte_count": 100,
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
                            "media_location", media_id, source_uri
                        ),
                        "media_id": media_id,
                        "storage_uri": source_uri,
                        "storage_class": "local",
                        "verified_at": "2026-08-26T20:01:01Z",
                        "is_primary": 1,
                    }
                ],
                "media_derivations": [],
                "artifacts": core_artifacts,
            },
            "errors": [],
            "result_path": str(run_dir / "result.json"),
        }
        out_of_range = copy.deepcopy(result)
        out_of_range["routing"]["silence_intervals"][-1] = {
            "start_ms": 200,
            "end_ms": 1_010,
            "duration_ms": 810,
        }
        with self.assertRaisesRegex(
            ResultImportError,
            "preprocess silence interval is inconsistent",
        ):
            validate_preprocess_result(out_of_range)

        summary_tamper = copy.deepcopy(result)
        summary_tamper["routing"]["summary"]["silent_duration_ms"] = 201
        with self.assertRaisesRegex(
            ResultImportError,
            "summary disagrees with exact interval arithmetic",
        ):
            validate_preprocess_result(summary_tamper)

        fraction_tamper = copy.deepcopy(result)
        fraction_tamper["routing"]["summary"]["silent_fraction"] = 0.200001
        with self.assertRaisesRegex(
            ResultImportError,
            "summary disagrees with exact interval arithmetic",
        ):
            validate_preprocess_result(fraction_tamper)

        overlapping = copy.deepcopy(result)
        overlapping["routing"]["silence_intervals"].append(
            {"start_ms": 300, "end_ms": 500, "duration_ms": 200}
        )
        overlapping["routing"]["summary"].update(
            {
                "silence_interval_count": 2,
                "silent_duration_ms": 400,
                "silent_fraction": 0.4,
            }
        )
        with self.assertRaisesRegex(
            ResultImportError,
            "sorted, unique, and nonoverlapping",
        ):
            validate_preprocess_result(overlapping)

        duplicate_scene = copy.deepcopy(result)
        duplicate_scene["routing"]["scene_changes"].append(
            {"timestamp_ms": 100, "score_percent": 30.0}
        )
        duplicate_scene["routing"]["summary"]["scene_change_count"] = 2
        with self.assertRaisesRegex(
            ResultImportError,
            "strictly sorted and unique",
        ):
            validate_preprocess_result(duplicate_scene)

        scene_out_of_range = copy.deepcopy(result)
        scene_out_of_range["routing"]["scene_changes"][0]["timestamp_ms"] = 1_001
        with self.assertRaisesRegex(
            ResultImportError,
            "scene-change candidate is out of range",
        ):
            validate_preprocess_result(scene_out_of_range)

        scene_at_endpoint = copy.deepcopy(result)
        scene_at_endpoint["routing"]["scene_changes"][0]["timestamp_ms"] = 1_000
        validate_preprocess_result(scene_at_endpoint)

        coverage_tamper = copy.deepcopy(result)
        coverage_tamper["routing"]["coverage"]["duration_ms"] = 999
        with self.assertRaisesRegex(
            ResultImportError,
            "coverage duration disagrees with source media",
        ):
            validate_preprocess_result(coverage_tamper)

        coherent_duration_tamper = copy.deepcopy(result)
        coherent_duration_tamper["routing"]["coverage"]["duration_ms"] = 999
        coherent_duration_tamper["catalog_records"]["media_objects"][0][
            "duration_ms"
        ] = 999
        with self.assertRaisesRegex(
            ResultImportError,
            "source media duration disagrees with normalized probe",
        ):
            validate_preprocess_result(coherent_duration_tamper)

        result_path = run_dir / "result.json"
        result_path.write_text(json.dumps(result), encoding="utf-8")
        probe_artifact_path.chmod(0o444)
        routing_artifact_path.chmod(0o444)
        result_path.chmod(0o444)
        imported = import_preprocess_result(self.connection, result_path)
        self.assertEqual(imported["observations"], 2)
        self.assertEqual(imported["routing_observations_deferred"], 0)
        database_path = self.root / "corpus.sqlite3"
        self.connection.execute("PRAGMA wal_checkpoint(TRUNCATE)")
        database_before = database_path.read_bytes()
        wal_path = Path(f"{database_path}-wal")
        wal_before = wal_path.read_bytes() if wal_path.exists() else None
        changes_before = self.connection.total_changes
        self.assertEqual(imported, import_preprocess_result(self.connection, result_path))
        self.assertEqual(self.connection.total_changes, changes_before)
        self.assertEqual(database_path.read_bytes(), database_before)
        self.assertEqual(
            wal_path.read_bytes() if wal_path.exists() else None,
            wal_before,
        )
        self.assertEqual(
            self.connection.execute(
                "SELECT count(*) FROM observations WHERE visibility = 'private' AND review_state = 'machine'"
            ).fetchone()[0],
            2,
        )
        self.assertEqual(self.connection.execute("SELECT count(*) FROM observation_scores").fetchone()[0], 1)

        job_id = imported["job_id"]
        tamper_cases = [
            (
                "different processing run",
                "UPDATE processing_runs SET environment_json = '{}' WHERE processing_run_id = ?",
                (run_id,),
            ),
            (
                "missing run input",
                "DELETE FROM run_inputs WHERE processing_run_id = ?",
                (run_id,),
            ),
            (
                "different artifact metadata",
                "UPDATE artifacts SET metadata_json = '{\"tampered\":true}' WHERE processing_run_id = ? AND artifact_kind = 'ffprobe_normalized_json'",
                (run_id,),
            ),
            (
                "missing routing score",
                "DELETE FROM observation_scores WHERE observation_id IN (SELECT observation_id FROM observations WHERE processing_run_id = ?)",
                (run_id,),
            ),
            (
                "different routing observation",
                "UPDATE observations SET metadata_json = '{\"tampered\":true}' WHERE processing_run_id = ? AND observation_kind = 'scene_change_candidate'",
                (run_id,),
            ),
            (
                "different job",
                "UPDATE jobs SET priority = 99 WHERE job_id = ?",
                (job_id,),
            ),
            (
                "different job attempt",
                "UPDATE job_attempts SET status = 'failed' WHERE job_id = ? AND processing_run_id = ?",
                (job_id, run_id),
            ),
            (
                "different job attempt ordinal",
                "UPDATE job_attempts SET attempt_number = 2 WHERE job_id = ? AND processing_run_id = ?",
                (job_id, run_id),
            ),
            (
                "different producer external ID",
                "UPDATE external_ids SET basis = 'tampered' WHERE object_type = 'processing_run' AND object_id = ? AND namespace = 'media_preprocess_producer_job_id'",
                (run_id,),
            ),
            (
                "different source media integrity",
                "UPDATE media_objects SET integrity_state = 'unverified' WHERE media_id = ?",
                (media_id,),
            ),
            (
                "different source media duration",
                "UPDATE media_objects SET duration_ms = duration_ms + 1 WHERE media_id = ?",
                (media_id,),
            ),
            (
                "different source probe identity",
                "UPDATE media_objects SET ffprobe_json = json_set(ffprobe_json, '$.media.sha256', ?) WHERE media_id = ?",
                ("0" * 64, media_id),
            ),
            (
                "different source probe shape",
                "UPDATE media_objects SET ffprobe_json = '{\"tampered\":true}' WHERE media_id = ?",
                (media_id,),
            ),
            (
                "different source location",
                "UPDATE media_locations SET is_primary = 0 WHERE media_id = ? AND storage_uri = ?",
                (media_id, source_uri),
            ),
            (
                "different source location storage class",
                "UPDATE media_locations SET storage_class = 'tampered' WHERE media_id = ? AND storage_uri = ?",
                (media_id, source_uri),
            ),
            (
                "different import statistics",
                "UPDATE import_batches SET statistics_json = '{}' WHERE import_batch_id = ?",
                (imported["import_batch_id"],),
            ),
            (
                "missing import batch",
                "DELETE FROM import_batches WHERE import_batch_id = ?",
                (imported["import_batch_id"],),
            ),
        ]
        for label, statement, parameters in tamper_cases:
            with self.subTest(replay_tamper=label):
                self.connection.execute("SAVEPOINT replay_tamper")
                try:
                    cursor = self.connection.execute(statement, parameters)
                    self.assertGreater(cursor.rowcount, 0)
                    changes_after_tamper = self.connection.total_changes
                    with self.assertRaisesRegex(
                        ResultImportError,
                        "completed preprocess replay|run exists without",
                    ):
                        import_preprocess_result(self.connection, result_path)
                    self.assertEqual(
                        self.connection.total_changes,
                        changes_after_tamper,
                        "a failed replay must not repair or otherwise write catalog rows",
                    )
                finally:
                    self.connection.execute("ROLLBACK TO replay_tamper")
                    self.connection.execute("RELEASE replay_tamper")

        batch_count = self.connection.execute("SELECT count(*) FROM import_batches").fetchone()[0]
        routing_artifact_path.chmod(0o644)
        routing_artifact_path.write_bytes(b"x" * len(routing_artifact_bytes))
        routing_artifact_path.chmod(0o444)
        with self.assertRaisesRegex(ResultImportError, "SHA-256"):
            import_preprocess_result(self.connection, result_path)
        routing_artifact_path.chmod(0o644)
        routing_artifact_path.write_bytes(routing_artifact_bytes)
        routing_artifact_path.chmod(0o444)
        probe_artifact_path.unlink()
        with self.assertRaisesRegex(ResultImportError, "not a readable current file"):
            import_preprocess_result(self.connection, result_path)
        self.assertEqual(
            self.connection.execute("SELECT count(*) FROM import_batches").fetchone()[0],
            batch_count,
        )


if __name__ == "__main__":
    unittest.main()
