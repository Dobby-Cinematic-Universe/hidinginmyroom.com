from __future__ import annotations

import copy
import hashlib
import json
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
PIPELINE_ROOT = REPOSITORY_ROOT / "pipeline"
sys.path.insert(0, str(CORPUS_ROOT / "src"))

from himr_corpus.asr_result_importer import (  # noqa: E402
    _validate_transcript,
    import_asr_whispercpp_result,
    validate_asr_whispercpp_result,
)
from himr_corpus.rendition_local_asr_bridge import (  # noqa: E402
    build_rendition_local_asr_admission_plan,
    import_rendition_local_asr_result,
    search_rendition_local_transcripts,
)
from himr_corpus.media_local_asr_bridge import (  # noqa: E402
    build_media_local_asr_admission_plan,
    import_media_local_asr_result,
    search_media_local_transcripts,
)
import himr_corpus.media_local_asr_bridge as media_bridge  # noqa: E402
from himr_corpus.db import connect, migrate  # noqa: E402
from himr_corpus.model_registry_admin import import_model_registry_manifest  # noqa: E402
from himr_corpus.result_importers import ResultImportError  # noqa: E402
from himr_corpus.validation import validate_database  # noqa: E402
from corpus.tests.reviewer_fixtures import register_reviewer_fixture  # noqa: E402


OBSERVED_AT = "2026-08-26T20:00:00Z"


def digest(path: Path) -> str:
    value = hashlib.sha256()
    with path.open("rb") as handle:
        while chunk := handle.read(1024 * 1024):
            value.update(chunk)
    return value.hexdigest()


def write_json(path: Path, value: object) -> None:
    path.write_text(json.dumps(value, indent=2) + "\n", encoding="utf-8")


def generate_audio(path: Path, *, duration_seconds: float = 2.5) -> None:
    subprocess.run(
        [
            shutil.which("ffmpeg") or "ffmpeg",
            "-hide_banner",
            "-nostdin",
            "-loglevel",
            "error",
            "-f",
            "lavfi",
            "-i",
            f"sine=frequency=440:sample_rate=16000:duration={duration_seconds}",
            "-ac",
            "1",
            "-ar",
            "16000",
            "-sample_fmt",
            "s16",
            "-c:a",
            "flac",
            str(path),
        ],
        check=True,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.PIPE,
    )


def make_fake_whisper(path: Path) -> None:
    path.write_text(
        "#!/usr/bin/env python3\n"
        "import json, sys\n"
        "from pathlib import Path\n"
        "def arg(name): return sys.argv[sys.argv.index(name) + 1]\n"
        "offset = int(arg('--offset-t')); duration = int(arg('--duration'))\n"
        "end = min(offset + duration + 100, offset + 800)\n"
        "middle = offset + (end - offset) // 2\n"
        "def stamp(ms):\n"
        " h, rem = divmod(ms, 3600000); m, rem = divmod(rem, 60000); s, rem = divmod(rem, 1000)\n"
        " return f'{h:02d}:{m:02d}:{s:02d},{rem:03d}'\n"
        "def timing(a, b): return {'timestamps': {'from': stamp(a), 'to': stamp(b)}, 'offsets': {'from': a, 'to': b}}\n"
        f"mode = {path.name!r}\n"
        "second_timing = timing(offset, 0) if 'inverted' in mode else timing(offset, middle)\n"
        "tokens = [\n"
        " {'text': '[_BEG_]', **timing(offset, offset), 'id': 50363, 'p': 1.0, 't_dtw': -1.0},\n"
        " {'text': ' HIMR', **second_timing, 'id': 101, 'p': 0.875, 't_dtw': -1.0},\n"
        " {'text': ' test', **timing(middle, end), 'id': 102, 'p': 0.625, 't_dtw': 12.5}]\n"
        "payload = {'systeminfo': 'fake cpu build', 'model': {'type': 'fake'},\n"
        " 'params': {'model': arg('--model'), 'language': arg('--language'), 'translate': False},\n"
        " 'result': {'language': 'en' if arg('--language') == 'auto' else arg('--language')},\n"
        " 'transcription': [{**timing(offset, end), 'text': ' HIMR test', 'tokens': tokens}]}\n"
        "Path(arg('--output-file') + '.json').write_text(json.dumps(payload), encoding='utf-8')\n",
        encoding="utf-8",
    )
    path.chmod(0o755)


@unittest.skipUnless(
    shutil.which("ffmpeg") and shutil.which("ffprobe"),
    "FFmpeg and FFprobe are required for ASR result-import integration",
)
class ASRResultImporterTests(unittest.TestCase):
    def setUp(self) -> None:
        work_root = CORPUS_ROOT / "work"
        work_root.mkdir(exist_ok=True)
        self.temporary = tempfile.TemporaryDirectory(prefix="asr-import-", dir=work_root)
        self.root = Path(self.temporary.name)
        self.connection = connect(self.root / "corpus.sqlite3")
        migrate(self.connection)
        self.audio = (self.root / "audio.flac").resolve()
        generate_audio(self.audio)
        self.model = (self.root / "model.bin").resolve()
        self.model.write_bytes(b"fake pinned model\n")
        self.engine = (self.root / "whisper-cli-fake").resolve()
        make_fake_whisper(self.engine)
        self.glossary = (self.root / "glossary.json").resolve()
        write_json(
            self.glossary,
            {
                "schema_version": 1,
                "glossary_revision_id": "glossary_himr_neutral_v1",
                "revision": "2026-08-26.1",
                "language": "en",
                "terms": ["HIMR", "HIMRFAM", "Dobby"],
            },
        )
        self.result = self._produce(with_context=True, with_glossary=True)
        self.result_path = Path(self.result["result_path"])
        self._seed_dependencies(self.result)

    def tearDown(self) -> None:
        self.connection.close()
        for path in sorted(self.root.rglob("*"), reverse=True):
            if path.is_dir():
                path.chmod(0o700)
            elif not path.is_symlink():
                path.chmod(0o600)
        self.temporary.cleanup()

    def _produce(
        self,
        *,
        with_context: bool,
        with_glossary: bool,
        duration_ms: int = 1_250,
        output_name: str = "output",
        model_id: str = "model_whisper_fake_v1",
        inverted_token: bool = False,
        offset_ms: int = 250,
    ) -> dict:
        if inverted_token:
            self.engine = (self.root / "whisper-cli-fake-inverted").resolve()
            make_fake_whisper(self.engine)
        audio_sha = digest(self.audio)
        order = {
            "schema_version": 1,
            "job_id": f"asr-import-{output_name}",
            "input": {
                "path": str(self.audio),
                "expected_sha256": audio_sha,
                "media_id": f"media_sha256_{audio_sha}",
                "artifact_id": "artifact_normalized_audio_fixture",
                "parent_processing_run_id": "run_preprocess_fixture",
            },
            "engine": {
                "executable": str(self.engine),
                "expected_sha256": digest(self.engine),
                "version_label": "fake-pinned-v1",
                "version_evidence": "source_revision_plus_executable_sha256",
                "build": {
                    "repository": "https://github.com/ggml-org/whisper.cpp",
                    "revision": "fake-commit-for-test",
                    "target": "whisper-cli",
                    "configuration": ["GGML_NATIVE=OFF", "GGML_CUDA=OFF"],
                },
            },
            "model": {
                "path": str(self.model),
                "expected_sha256": digest(self.model),
                "model_id": model_id,
                "name": "fake whisper fixture",
                "revision": "fake-v1",
                "source": "local test fixture",
                "license_label": "test-only",
            },
            "window": {"offset_ms": offset_ms, "duration_ms": duration_ms},
            "inference": {
                "language": "en",
                "threads": 2,
                "translate": False,
                "split_on_word": True,
                "best_of": 5,
                "beam_size": 5,
                "max_segment_characters": 0,
                "word_threshold": 0.01,
                "entropy_threshold": 2.4,
                "logprob_threshold": -1.0,
                "no_speech_threshold": 0.6,
                "temperature": 0.0,
                "temperature_increment": 0.2,
                "no_fallback": False,
                "timeout_seconds": 10,
            },
            "glossary": {
                "path": str(self.glossary),
                "expected_sha256": digest(self.glossary),
            }
            if with_glossary
            else None,
            "catalog_context": {
                "recording_id": "recording_fixture",
                "rendition_id": "rendition_fixture",
            }
            if with_context
            else None,
            "output": {"root": str((self.root / output_name).resolve())},
        }
        order_path = self.root / f"{output_name}-work-order.json"
        write_json(order_path, order)
        completed = subprocess.run(
            [
                sys.executable,
                str(PIPELINE_ROOT / "asr_whispercpp.py"),
                "run",
                "--work-order",
                str(order_path),
            ],
            cwd=REPOSITORY_ROOT,
            check=False,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
        )
        self.assertEqual(completed.returncode, 0, completed.stderr)
        return json.loads(completed.stdout)

    def _mark_as_local_window(
        self, result: dict, *, recording_start_ms: int | None
    ) -> None:
        """Apply the catalog metadata shape emitted by local-window admission."""

        source_start_ms = 1_800_000
        duration_ms = result["input"]["probe"]["duration_ms"]
        source_mapping = {
            "artifact_zero_maps_to_source_ms": source_start_ms,
            "boundary": "half_open",
            "byte_exact_source_fragment": False,
            "coordinate_precision": "integer_millisecond_contract",
            "extraction_method": "ffmpeg_accurate_seek_transcode",
            "source_end_ms": source_start_ms + duration_ms,
            "source_start_ms": source_start_ms,
        }
        window = {
            "boundary": "half_open",
            "end_ms": source_start_ms + duration_ms,
            "is_partial_tail": False,
            "ordinal": 1,
            "start_ms": source_start_ms,
            "window_id": "window_000002",
        }
        evidence = {
            "acquisition_import_batch_id": "imp_acquisition_fixture",
            "acquisition_result_sha256": "a" * 64,
            "boundary_calibration_state": "not_calibrated",
            "contract_version": 1,
            "identity_authority": "none",
            "local_window_result_sha256": "b" * 64,
            "local_window_result_uri": (self.root / "local-window" / "result.json").as_uri(),
            "normalized_probe": result["input"]["probe"],
            "publication_state": "withheld_by_default",
            "representation_is_original_source": False,
            "run_semantics": "catalog_admission_verification_not_extraction_execution",
            "source_media_id": f"media_sha256_{'c' * 64}",
            "source_time_mapping": source_mapping,
            "window": window,
        }
        artifact_metadata = {**evidence, "media_id": result["input"]["media_id"]}
        if recording_start_ms is None:
            coordinate_mapping = {
                "basis_ids": [],
                "recording_end_ms": None,
                "recording_start_ms": None,
                "state": "unasserted_no_unique_catalog_transform",
                "timeline_mapping_kind": "unknown",
            }
            recording_end_ms = None
            mapping_kind = "unknown"
        else:
            coordinate_mapping = {
                "basis_ids": ["future-reviewed-exact-offset"],
                "recording_end_ms": recording_start_ms + duration_ms,
                "recording_start_ms": recording_start_ms,
                "state": "future_reviewed_exact_offset",
                "timeline_mapping_kind": "exact",
            }
            recording_end_ms = recording_start_ms + duration_ms
            mapping_kind = "exact"
        rendition_metadata = {
            **evidence,
            "parent_rendition_id": "rendition_parent_fixture",
            "recording_coordinate_mapping": coordinate_mapping,
        }
        self.connection.execute(
            """
            UPDATE artifacts
            SET artifact_kind = 'window_audio_16khz_mono_flac', metadata_json = ?
            WHERE artifact_id = ?
            """,
            (json.dumps(artifact_metadata, sort_keys=True), result["input"]["artifact_id"]),
        )
        self.connection.execute(
            """
            UPDATE renditions
            SET rendition_kind = 'local_window:window_audio_16khz_mono_flac:window_000002',
                metadata_json = ?
            WHERE rendition_id = 'rendition_fixture'
            """,
            (json.dumps(rendition_metadata, sort_keys=True),),
        )
        self.connection.execute(
            """
            INSERT INTO timeline_map_spans(
                timeline_map_span_id, rendition_id, ordinal, media_start_ms,
                media_end_ms, recording_start_ms, recording_end_ms,
                mapping_kind, confidence_state
            ) VALUES('timeline_local_window_fixture', 'rendition_fixture', 0, 0, ?, ?, ?, ?, 'metadata_only')
            """,
            (duration_ms, recording_start_ms, recording_end_ms, mapping_kind),
        )

    def _mark_as_rendition_local_bridge_fixture(
        self,
        result: dict,
        *,
        source_contract_extra_ms: int = 8,
        recording_duration_delta_ms: int = 19,
    ) -> None:
        """Seed the exact admitted-lineage shape required by the private bridge."""

        self._mark_as_local_window(result, recording_start_ms=None)
        input_duration = result["input"]["probe"]["duration_ms"]
        source_start = 1_800_000
        source_end = source_start + input_duration + source_contract_extra_ms
        source_mapping = {
            "artifact_zero_maps_to_source_ms": source_start,
            "boundary": "half_open",
            "byte_exact_source_fragment": False,
            "coordinate_precision": "integer_millisecond_contract",
            "extraction_method": "ffmpeg_accurate_seek_transcode",
            "source_end_ms": source_end,
            "source_start_ms": source_start,
        }
        local_dir = (self.root / f"local-{result['result_key'][:12]}").resolve()
        local_dir.mkdir(parents=True, exist_ok=True)
        local_result_path = local_dir / "result.json"
        local_result = {
            "schema_version": 1,
            "time_mapping": source_mapping,
            "artifacts": [
                {
                    "artifact_kind": "window_audio_16khz_mono_flac",
                    "path": result["input"]["path"],
                    "sha256": result["input"]["sha256"],
                    "byte_count": result["input"]["byte_count"],
                }
            ],
        }
        write_json(local_result_path, local_result)
        local_result_sha = digest(local_result_path)
        source_media_sha = "c" * 64
        source_media_id = f"media_sha256_{source_media_sha}"
        source_id = "source_bridge_fixture"
        parent_rendition_id = "rendition_parent_fixture"
        evidence = {
            "acquisition_import_batch_id": "imp_acquisition_fixture",
            "acquisition_result_sha256": "a" * 64,
            "boundary_calibration_state": "not_calibrated",
            "contract_version": 1,
            "identity_authority": "none",
            "local_window_result_sha256": local_result_sha,
            "local_window_result_uri": local_result_path.as_uri(),
            "normalized_probe": result["input"]["probe"],
            "publication_state": "withheld_by_default",
            "representation_is_original_source": False,
            "run_semantics": "catalog_admission_verification_not_extraction_execution",
            "source_media_id": source_media_id,
            "source_time_mapping": source_mapping,
            "window": {
                "boundary": "half_open",
                "end_ms": source_end,
                "is_partial_tail": True,
                "ordinal": 2,
                "start_ms": source_start,
                "window_id": "window_000002",
            },
        }
        artifact_metadata = {
            **evidence,
            "media_id": result["input"]["media_id"],
        }
        rendition_metadata = {
            **evidence,
            "parent_rendition_id": parent_rendition_id,
            "recording_coordinate_mapping": {
                "basis_ids": ["recording_source_bridge_fixture"],
                "recording_end_ms": None,
                "recording_start_ms": None,
                "state": "unasserted_no_unique_catalog_transform",
                "timeline_mapping_kind": "unknown",
            },
        }
        self.connection.execute(
            "UPDATE artifacts SET artifact_kind = ?, metadata_json = ? WHERE artifact_id = ?",
            (
                "window_audio_16khz_mono_flac",
                json.dumps(artifact_metadata, sort_keys=True, separators=(",", ":")),
                result["input"]["artifact_id"],
            ),
        )
        self.connection.execute(
            """
            UPDATE renditions
            SET rendition_kind = ?, metadata_json = ?
            WHERE rendition_id = 'rendition_fixture'
            """,
            (
                "local_window:window_audio_16khz_mono_flac:fixture:window_000002",
                json.dumps(rendition_metadata, sort_keys=True, separators=(",", ":")),
            ),
        )
        parent_parameters = {
            "contract_version": 1,
            "local_window_result_sha256": local_result_sha,
            "local_window_result_uri": local_result_path.as_uri(),
            "run_semantics": "catalog_admission_verification_not_extraction_execution",
            "time_mapping": source_mapping,
        }
        parent_environment = {
            "credentials_used": False,
            "identity_claims_allowed": False,
            "network_access_performed": False,
            "publication_authority": "none",
        }
        self.connection.execute(
            """
            UPDATE processing_runs
            SET stage = 'local_window_result_admission',
                implementation_version = 'local-window-catalog-bridge/1',
                parameters_json = ?, environment_json = ?, status = 'completed',
                error_text = NULL
            WHERE processing_run_id = 'run_preprocess_fixture'
            """,
            (
                json.dumps(parent_parameters, sort_keys=True, separators=(",", ":")),
                json.dumps(parent_environment, sort_keys=True, separators=(",", ":")),
            ),
        )
        self.connection.execute(
            """
            INSERT INTO import_batches(
                import_batch_id, importer_name, importer_version, input_sha256,
                started_at, completed_at, status, statistics_json
            ) VALUES('imp_local_window_bridge_fixture',
                     'local_window_result_admission_v1',
                     'local-window-catalog-bridge/1', ?, ?, ?, 'completed', '{}')
            """,
            (local_result_sha, OBSERVED_AT, OBSERVED_AT),
        )
        self.connection.execute(
            """
            INSERT INTO sources(
                source_id, platform, source_kind, native_id, canonical_url,
                observed_at, access_state, review_state, metadata_json,
                created_at, updated_at
            ) VALUES(?, 'youtube', 'video', 'bridge-fixture',
                     'https://www.youtube.com/watch?v=bridge-fixture', ?, 'public',
                     'metadata_only', '{}', ?, ?)
            """,
            (source_id, OBSERVED_AT, OBSERVED_AT, OBSERVED_AT),
        )
        self.connection.execute(
            """
            INSERT INTO media_objects(
                media_id, sha256, byte_count, media_kind, mime_type, container,
                duration_ms, first_cataloged_at, integrity_state
            ) VALUES(?, ?, 1, 'video', 'video/mp4', 'mp4', ?, ?, 'verified')
            """,
            (source_media_id, source_media_sha, source_end, OBSERVED_AT),
        )
        self.connection.execute(
            """
            INSERT INTO media_sources(
                media_source_id, media_id, source_id, retrieved_at,
                retrieval_tool, retrieval_tool_version
            ) VALUES('media_source_bridge_fixture', ?, ?, ?, 'fixture', '1')
            """,
            (source_media_id, source_id, OBSERVED_AT),
        )
        self.connection.execute(
            """
            INSERT INTO renditions(
                rendition_id, recording_id, media_id, rendition_kind,
                label, review_state, metadata_json
            ) VALUES(?, 'recording_fixture', ?, 'acquired_source_media',
                     'Fixture acquired source', 'unreviewed', '{}')
            """,
            (parent_rendition_id, source_media_id),
        )
        self.connection.execute(
            """
            INSERT INTO recording_sources(
                recording_source_id, recording_id, source_id, mapping_role,
                mapping_method, confidence_state, metadata_json
            ) VALUES('recording_source_bridge_fixture', 'recording_fixture', ?,
                     'current_platform_listing', 'fixture', 'metadata_only', '{}')
            """,
            (source_id,),
        )
        self.connection.execute(
            "UPDATE recordings SET duration_ms = ? WHERE recording_id = 'recording_fixture'",
            (source_end - recording_duration_delta_ms,),
        )

    def _seed_dependencies(
        self, result: dict, *, with_context: bool = True, register_model: bool = True
    ) -> None:
        media_id = result["input"]["media_id"]
        self.connection.execute(
            """
            INSERT OR IGNORE INTO media_objects(
                media_id, sha256, byte_count, media_kind, mime_type, container,
                duration_ms, first_cataloged_at, integrity_state
            ) VALUES(?, ?, ?, 'audio', 'audio/flac', 'flac', ?, ?, 'verified')
            """,
            (
                media_id,
                result["input"]["sha256"],
                result["input"]["byte_count"],
                result["input"]["probe"]["duration_ms"],
                OBSERVED_AT,
            ),
        )
        self.connection.execute(
            """
            INSERT OR IGNORE INTO processing_runs(
                processing_run_id, stage, implementation_version, parameters_json,
                environment_json, started_at, completed_at, status
            ) VALUES('run_preprocess_fixture', 'media_preprocess', 'fixture', '{}', '{}',
                     ?, ?, 'completed')
            """,
            (OBSERVED_AT, OBSERVED_AT),
        )
        self.connection.execute(
            """
            INSERT OR IGNORE INTO artifacts(
                artifact_id, processing_run_id, artifact_kind, storage_uri, sha256,
                byte_count, schema_version, visibility, metadata_json
            ) VALUES('artifact_normalized_audio_fixture', 'run_preprocess_fixture',
                     'normalized_audio_flac', ?, ?, ?, 1, 'private', '{}')
            """,
            (
                self.audio.as_uri(),
                result["input"]["sha256"],
                result["input"]["byte_count"],
            ),
        )
        if register_model:
            model_manifest = {
                "schema_version": 1,
                "manifest_id": f"registry-{result['model']['model_id']}",
                "created_at": OBSERVED_AT,
                "registered_by": "ASR importer test fixture",
                "basis": "Exact local test weights and fixture provenance.",
                "models": [
                    {
                        "model_id": result["model"]["model_id"],
                        "task": "asr",
                        "name": result["model"]["name"],
                        "version": result["model"]["revision"],
                        "weights_path": result["model"]["path"],
                        "weights_sha256": result["model"]["sha256"],
                        "weights_byte_count": result["model"]["byte_count"],
                        "license_label": result["model"]["license_label"],
                        "configuration_json": {"source": result["model"]["source"]},
                    }
                ],
            }
            manifest_path = self.root / f"{result['model']['model_id']}-registry.json"
            write_json(manifest_path, model_manifest)
            import_model_registry_manifest(self.connection, manifest_path)
        if result["glossary"] is not None:
            self.connection.execute(
                """
                INSERT OR IGNORE INTO glossary_revisions(
                    glossary_revision_id, sha256, created_at, description, artifact_uri
                ) VALUES(?, ?, ?, 'test fixture', ?)
                """,
                (
                    result["glossary"]["glossary_revision_id"],
                    result["glossary"]["sha256"],
                    OBSERVED_AT,
                    self.glossary.as_uri(),
                ),
            )
        if with_context and result["catalog_context"] is not None:
            self.connection.execute(
                """
                INSERT OR IGNORE INTO recordings(
                    recording_id, canonical_key, slug, title, date_basis,
                    recording_type, review_state, metadata_json, created_at, updated_at
                ) VALUES('recording_fixture', 'fixture:recording', 'fixture-recording',
                         'Fixture recording', 'test', 'video', 'metadata_only', '{}', ?, ?)
                """,
                (OBSERVED_AT, OBSERVED_AT),
            )
            self.connection.execute(
                """
                INSERT OR IGNORE INTO renditions(
                    rendition_id, recording_id, media_id, rendition_kind,
                    label, review_state, metadata_json
                ) VALUES('rendition_fixture', 'recording_fixture', ?,
                         'normalized_audio', 'Fixture audio', 'unreviewed', '{}')
                """,
                (media_id,),
            )

    def _variant(self, value: dict, name: str, *, relocate_artifacts: bool = True) -> Path:
        variant_dir = (self.root / "variants" / name).resolve()
        variant_dir.mkdir(parents=True, exist_ok=True)
        if relocate_artifacts:
            names = {
                "whispercpp_output_json_full": "whisper.raw.json",
                "transcript_normalized_json": "transcript.normalized.json",
            }
            for artifact in value["artifacts"]:
                old_path = Path(artifact["storage_uri"].removeprefix("file://"))
                new_path = variant_dir / names[artifact["artifact_kind"]]
                shutil.copyfile(old_path, new_path)
                artifact["storage_uri"] = new_path.as_uri()
            value["catalog_records"]["artifacts"] = copy.deepcopy(value["artifacts"])
        path = variant_dir / "result.json"
        value["result_path"] = str(path)
        write_json(path, value)
        return path

    def test_imports_current_030_shape_idempotently_with_zero_length_token(self) -> None:
        first = import_asr_whispercpp_result(self.connection, self.result_path)
        second = import_asr_whispercpp_result(self.connection, self.result_path)
        self.assertEqual(first, second)
        self.assertEqual(first["transcript_revisions"], 1)
        self.assertEqual(self.result["processing_run"]["implementation_version"], "0.3.0")
        self.assertEqual(first["transcript_segments"], 1)
        self.assertEqual(first["transcript_words"], 3)
        self.assertEqual(first["processing_run_provenance_ids"], 6)
        self.assertEqual(
            self.connection.execute(
                "SELECT count(*) FROM external_ids WHERE object_id = ?",
                (first["processing_run_id"],),
            ).fetchone()[0],
            6,
        )
        word = self.connection.execute(
            "SELECT start_ms, end_ms, calibrated_probability FROM transcript_words ORDER BY ordinal LIMIT 1"
        ).fetchone()
        self.assertEqual(word["start_ms"], word["end_ms"])
        self.assertIsNone(word["calibrated_probability"])
        revision = self.connection.execute(
            "SELECT review_state, revision_kind FROM transcript_revisions"
        ).fetchone()
        self.assertEqual(dict(revision), {"review_state": "machine", "revision_kind": "contextual_asr"})
        self.assertEqual(
            self.connection.execute("SELECT count(*) FROM publication_decisions").fetchone()[0],
            0,
        )
        self.assertEqual(
            self.connection.execute("SELECT count(*) FROM publication_gate_decisions").fetchone()[0],
            0,
        )

    def test_current_descriptor_command_provenance_is_tamper_evident(self) -> None:
        mutations: list[tuple[str, dict]] = []

        path_backed = copy.deepcopy(self.result)
        command = path_backed["commands"][1]
        command[command.index("--file") + 1] = path_backed["input"]["path"]
        mutations.append(("path-backed executed input", path_backed))

        logical_tamper = copy.deepcopy(self.result)
        environment = json.loads(
            logical_tamper["processing_run"]["environment_json"]
        )
        logical = environment["command_provenance"]["logical_commands"][1]
        logical[logical.index("--file") + 1] = logical_tamper["model"]["path"]
        environment_text = json.dumps(
            environment,
            ensure_ascii=False,
            sort_keys=True,
            separators=(",", ":"),
        )
        logical_tamper["processing_run"]["environment_json"] = environment_text
        logical_tamper["catalog_records"]["processing_runs"][0][
            "environment_json"
        ] = environment_text
        mutations.append(("logical input", logical_tamper))

        state_tamper = copy.deepcopy(self.result)
        environment = json.loads(state_tamper["processing_run"]["environment_json"])
        environment["command_provenance"]["result_command_states"][-1] = "planned"
        environment_text = json.dumps(
            environment,
            ensure_ascii=False,
            sort_keys=True,
            separators=(",", ":"),
        )
        state_tamper["processing_run"]["environment_json"] = environment_text
        state_tamper["catalog_records"]["processing_runs"][0][
            "environment_json"
        ] = environment_text
        mutations.append(("execution state", state_tamper))

        for name, value in mutations:
            with self.subTest(name=name):
                with self.assertRaisesRegex(
                    ResultImportError, "command|descriptor"
                ):
                    validate_asr_whispercpp_result(value)

    def test_zero_offset_full_media_context_remains_importable(self) -> None:
        result = self._produce(
            with_context=True,
            with_glossary=False,
            duration_ms=2_500,
            offset_ms=0,
            output_name="full-media-zero-offset",
        )
        self._seed_dependencies(result)

        imported = import_asr_whispercpp_result(self.connection, result["result_path"])
        self.assertEqual(imported["transcript_revisions"], 1)
        segment = self.connection.execute(
            """
            SELECT s.start_ms, s.end_ms
            FROM transcript_segments AS s
            JOIN transcript_revisions AS r ON r.revision_id = s.revision_id
            WHERE r.processing_run_id = ?
            """,
            (result["processing_run"]["processing_run_id"],),
        ).fetchone()
        self.assertEqual((segment["start_ms"], segment["end_ms"]), (0, 800))

    def test_contextual_local_window_with_real_unknown_timeline_shape_fails_without_writes(self) -> None:
        result = self._produce(
            with_context=True,
            with_glossary=False,
            duration_ms=2_500,
            offset_ms=0,
            output_name="local-window-unknown",
        )
        self._seed_dependencies(result)
        self._mark_as_local_window(result, recording_start_ms=None)

        with self.assertRaisesRegex(ResultImportError, "no full-coverage exact catalog timeline"):
            import_asr_whispercpp_result(self.connection, result["result_path"])
        self.assertEqual(
            self.connection.execute(
                "SELECT count(*) FROM processing_runs WHERE stage = 'asr_whispercpp'"
            ).fetchone()[0],
            0,
        )
        self.assertEqual(
            self.connection.execute(
                "SELECT count(*) FROM import_batches WHERE importer_name = 'asr_whispercpp_result_v1'"
            ).fetchone()[0],
            0,
        )

    def test_contextual_local_window_with_future_exact_nonidentity_span_still_fails_without_writes(self) -> None:
        result = self._produce(
            with_context=True,
            with_glossary=False,
            duration_ms=2_500,
            offset_ms=0,
            output_name="local-window-future-exact",
        )
        self._seed_dependencies(result)
        self._mark_as_local_window(result, recording_start_ms=1_800_000)

        with self.assertRaisesRegex(ResultImportError, "separately sealed.*translation bridge"):
            import_asr_whispercpp_result(self.connection, result["result_path"])
        self.assertEqual(
            self.connection.execute(
                "SELECT count(*) FROM processing_runs WHERE stage = 'asr_whispercpp'"
            ).fetchone()[0],
            0,
        )
        self.assertEqual(
            self.connection.execute(
                "SELECT count(*) FROM transcript_revisions"
            ).fetchone()[0],
            0,
        )

    def test_rendition_local_bridge_imports_searchable_private_coordinates_idempotently(self) -> None:
        result = self._produce(
            with_context=True,
            with_glossary=False,
            duration_ms=2_500,
            offset_ms=0,
            output_name="rendition-local-bridge",
        )
        self._seed_dependencies(result, register_model=False)
        self._mark_as_rendition_local_bridge_fixture(result)

        plan = build_rendition_local_asr_admission_plan(
            self.connection, result["result_path"]
        )
        self.assertEqual(plan["coordinate_contract"]["coordinate_system"], "rendition_media_ms")
        self.assertEqual(
            plan["coordinate_contract"]["recording_transform_state"], "unresolved"
        )
        self.assertFalse(plan["coordinate_contract"]["recording_coordinates_asserted"])
        self.assertEqual(plan["coordinate_contract"]["artifact_source_duration_delta_ms"], -8)
        self.assertEqual(plan["statistics"]["recording_scoped_transcript_revisions"], 0)
        self.assertEqual(plan["statistics"]["transform_candidates"], 2)
        self.assertNotIn("HIMR test", json.dumps(plan))

        with self.assertRaisesRegex(ResultImportError, "separately reviewed plan"):
            import_rendition_local_asr_result(
                self.connection,
                result["result_path"],
                expected_plan_sha256="0" * 64,
            )
        self.assertEqual(
            self.connection.execute(
                "SELECT count(*) FROM rendition_local_transcript_revisions"
            ).fetchone()[0],
            0,
        )

        first = import_rendition_local_asr_result(
            self.connection,
            result["result_path"],
            expected_plan_sha256=plan["plan_sha256"],
        )
        second = import_rendition_local_asr_result(
            self.connection,
            result["result_path"],
            expected_plan_sha256=plan["plan_sha256"],
        )
        self.assertEqual(first, second)
        self.assertEqual(first["status"], "admitted")
        self.assertEqual(
            self.connection.execute(
                "SELECT count(*) FROM rendition_local_transcript_revisions"
            ).fetchone()[0],
            1,
        )
        self.assertEqual(
            self.connection.execute(
                "SELECT count(*) FROM rendition_local_transcript_fts"
            ).fetchone()[0],
            first["statistics"]["rendition_local_segments"],
        )
        self.assertEqual(
            self.connection.execute("SELECT count(*) FROM transcript_revisions").fetchone()[0],
            0,
        )
        span = self.connection.execute(
            """
            SELECT recording_start_ms, recording_end_ms, mapping_kind
            FROM timeline_map_spans WHERE rendition_id = 'rendition_fixture'
            """
        ).fetchone()
        self.assertEqual(dict(span), {
            "recording_start_ms": None,
            "recording_end_ms": None,
            "mapping_kind": "unknown",
        })
        searched = search_rendition_local_transcripts(
            self.connection, "HIMR", recording_id="recording_fixture"
        )
        self.assertEqual(searched["result_count"], 1)
        self.assertEqual(searched["coordinate_system"], "rendition_media_ms")
        self.assertIsNone(searched["results"][0]["recording_start_ms"])
        self.assertIsNone(searched["results"][0]["recording_end_ms"])

        register_reviewer_fixture(
            self.connection, "reviewer_bridge_fixture", "Fixture reviewer"
        )
        with self.assertRaisesRegex(sqlite3.IntegrityError, "no publication lane"):
            self.connection.execute(
                """
                INSERT INTO publication_decisions(
                    publication_decision_id, object_type, object_id, decision,
                    reviewer_id, decided_at, basis
                ) VALUES('publication_bridge_fixture',
                         'rendition_local_transcript_revision', ?, 'publish',
                         'reviewer_bridge_fixture', ?, 'must remain private')
                """,
                (plan["result"]["local_revision_id"], OBSERVED_AT),
            )

    def test_rendition_local_bridge_preserves_tail_overrun_without_clipping(self) -> None:
        self.connection.execute("DELETE FROM renditions WHERE rendition_id = 'rendition_fixture'")
        self.connection.execute(
            "DELETE FROM artifacts WHERE artifact_id = 'artifact_normalized_audio_fixture'"
        )
        tail_audio = (self.root / "tail-audio.flac").resolve()
        generate_audio(tail_audio, duration_seconds=0.5)
        self.audio = tail_audio
        result = self._produce(
            with_context=True,
            with_glossary=False,
            duration_ms=500,
            offset_ms=0,
            output_name="rendition-local-tail-overrun",
        )
        self._seed_dependencies(result, register_model=False)
        self._mark_as_rendition_local_bridge_fixture(result)

        plan = build_rendition_local_asr_admission_plan(
            self.connection, result["result_path"]
        )
        coordinates = plan["coordinate_contract"]
        self.assertEqual(coordinates["input_duration_ms"], 500)
        self.assertEqual(coordinates["max_segment_end_ms"], 600)
        self.assertEqual(coordinates["input_boundary_overrun_ms"], 100)
        self.assertEqual(coordinates["artifact_source_duration_delta_ms"], -8)
        self.assertEqual(coordinates["source_boundary_overrun_ms"], 92)

        import_rendition_local_asr_result(
            self.connection,
            result["result_path"],
            expected_plan_sha256=plan["plan_sha256"],
        )
        segment = self.connection.execute(
            """
            SELECT end_ms, source_end_ms, source_boundary_overrun_ms
            FROM rendition_local_transcript_segments
            """
        ).fetchone()
        self.assertEqual(segment["end_ms"], 600)
        self.assertEqual(segment["source_boundary_overrun_ms"], 92)
        self.assertEqual(
            segment["source_end_ms"],
            coordinates["max_segment_source_end_ms"],
        )

    def test_rendition_local_bridge_refuses_recording_timeline_reinterpretation(self) -> None:
        result = self._produce(
            with_context=True,
            with_glossary=False,
            duration_ms=2_500,
            offset_ms=0,
            output_name="rendition-local-refuse-timeline",
        )
        self._seed_dependencies(result, register_model=False)
        self._mark_as_rendition_local_bridge_fixture(result)
        self.connection.execute(
            """
            UPDATE timeline_map_spans
            SET recording_start_ms = 1800000, recording_end_ms = 1802500,
                mapping_kind = 'exact'
            WHERE rendition_id = 'rendition_fixture'
            """
        )
        with self.assertRaisesRegex(ResultImportError, "unresolved full-artifact timeline"):
            build_rendition_local_asr_admission_plan(
                self.connection, result["result_path"]
            )
        self.assertEqual(
            self.connection.execute(
                "SELECT count(*) FROM rendition_local_asr_imports"
            ).fetchone()[0],
            0,
        )

    def test_media_local_bridge_is_text_free_idempotent_private_and_coordinate_null(self) -> None:
        result = self._produce(
            with_context=False,
            with_glossary=False,
            duration_ms=2_500,
            offset_ms=0,
            output_name="media-local-bridge",
        )
        self._seed_dependencies(result, with_context=False, register_model=False)
        self.connection.execute(
            """
            INSERT INTO import_batches(
                import_batch_id, importer_name, importer_version, input_sha256,
                started_at, completed_at, status, statistics_json
            ) VALUES('imp_media_local_preprocess_fixture',
                     'media_preprocess_result_v1', '0.2.0', ?, ?, ?,
                     'completed', '{}')
            """,
            ("e" * 64, OBSERVED_AT, OBSERVED_AT),
        )
        lineage = {
            "queue_manifest_uri": (self.root / "sealed-queue.json").as_uri(),
            "queue_manifest_raw_sha256":
                "100d142cf459a663dd0f899db74b9666fbe49942d7ff0960280424899d39b5a0",
            "queue_identity_sha256":
                "66349d9b85c74f2376830edf2a7d4f0ccf9d4e093f9c55b8127429259f2948d1",
            "queue_id": "asrppqueue_66349d9b85c74f2376830edf2a7d4f0c",
            "queue_ordinal": 2,
            "routing_hint": "process",
            "work_order_uri": (self.root / "sealed-work-order.json").as_uri(),
            "work_order_raw_sha256": "a" * 64,
            "work_order_canonical_sha256": "b" * 64,
            "work_order_byte_count": 1024,
            "preprocess_result_uri": (self.root / "preprocess-result.json").as_uri(),
            "preprocess_result_raw_sha256": "c" * 64,
            "preprocess_result_canonical_sha256": "d" * 64,
            "preprocess_result_byte_count": 2048,
            "preprocess_import_batch_id": "imp_media_local_preprocess_fixture",
            "preprocess_processing_run_id": "run_preprocess_fixture",
            "preprocess_source_media_id": result["input"]["media_id"],
            "audio_artifact": {
                "artifact_id": result["input"]["artifact_id"],
                "media_id": result["input"]["media_id"],
            },
        }
        seal = {
            "seal_receipt_uri": (self.root / "seal-receipt.json").as_uri(),
            "seal_receipt_raw_sha256":
                "806439f2736dae9b95ffdffd19c3464c9efc39e79a5fbc21f7223b9f2c717b7b",
            "seal_receipt_id": "asrsealreceipt_c0694c5c36586eb433a766490f3dbc01",
            "seal_receipt_identity_sha256":
                "9f29212c38a78ff91faaea5dc7d8eb10f3d0405c0075ce8e365d4b33598df524",
            "seal_receipt_ordinal": 9,
            "sealed_result_directory_uri": Path(result["result_path"]).parent.as_uri(),
        }
        target = "himr_corpus.media_local_asr_bridge._require_exact_media_lineage"
        seal_target = "himr_corpus.media_local_asr_bridge._require_result_seal"
        with mock.patch(target, return_value=lineage), mock.patch(
            seal_target, return_value=seal
        ):
            plan = build_media_local_asr_admission_plan(
                self.connection, result["result_path"], self.root / "sealed-queue.json"
            )
            self.assertNotIn("HIMR test", json.dumps(plan))
            self.assertIsNone(plan["catalog_context"]["recording_id"])
            self.assertEqual(plan["coordinate_contract"]["coordinate_system"], "media_ms")
            self.assertFalse(
                plan["coordinate_contract"]["recording_coordinates_asserted"]
            )
            with self.assertRaisesRegex(ResultImportError, "separately reviewed plan"):
                import_media_local_asr_result(
                    self.connection,
                    result["result_path"],
                    self.root / "sealed-queue.json",
                    expected_plan_sha256="0" * 64,
                )
            first = import_media_local_asr_result(
                self.connection,
                result["result_path"],
                self.root / "sealed-queue.json",
                expected_plan_sha256=plan["plan_sha256"],
            )
            second = import_media_local_asr_result(
                self.connection,
                result["result_path"],
                self.root / "sealed-queue.json",
                expected_plan_sha256=plan["plan_sha256"],
            )
        self.assertEqual(first, second)
        self.assertEqual(first["status"], "admitted")
        self.assertEqual(
            self.connection.execute(
                "SELECT count(*) FROM media_local_transcript_fts"
            ).fetchone()[0],
            first["statistics"]["media_local_segments"],
        )
        self.assertEqual(
            self.connection.execute("SELECT count(*) FROM transcript_revisions").fetchone()[0],
            0,
        )
        searched = search_media_local_transcripts(self.connection, "HIMR")
        self.assertEqual(searched["result_count"], 1)
        hit = searched["results"][0]
        self.assertIsNone(hit["recording_id"])
        self.assertIsNone(hit["rendition_id"])
        self.assertIsNone(hit["source_start_ms"])
        self.assertIsNone(hit["source_end_ms"])
        self.assertIsNone(hit["recording_start_ms"])
        self.assertIsNone(hit["recording_end_ms"])
        register_reviewer_fixture(
            self.connection, "reviewer_media_local_fixture", "Fixture reviewer"
        )
        with self.assertRaisesRegex(sqlite3.IntegrityError, "no publication lane"):
            self.connection.execute(
                """
                INSERT INTO publication_decisions(
                    publication_decision_id, object_type, object_id, decision,
                    reviewer_id, decided_at, basis
                ) VALUES('publication_media_local_fixture',
                         'media_local_transcript_revision', ?, 'publish',
                         'reviewer_media_local_fixture', ?, 'must remain private')
                """,
                (plan["result"]["media_local_revision_id"], OBSERVED_AT),
            )
        with self.assertRaisesRegex(sqlite3.IntegrityError, "append-only"):
            self.connection.execute(
                "UPDATE media_local_transcript_revisions SET origin = 'changed'"
            )
        validate_database(self.connection)

    def test_media_local_bridge_preserves_overrun_without_clipping(self) -> None:
        self.connection.execute(
            "DELETE FROM artifacts WHERE artifact_id = 'artifact_normalized_audio_fixture'"
        )
        tail_audio = (self.root / "media-local-tail.flac").resolve()
        generate_audio(tail_audio, duration_seconds=0.5)
        self.audio = tail_audio
        result = self._produce(
            with_context=False,
            with_glossary=False,
            duration_ms=500,
            offset_ms=0,
            output_name="media-local-tail-overrun",
            inverted_token=True,
        )
        self._seed_dependencies(result, with_context=False, register_model=False)
        lineage = {
            "queue_manifest_uri": (self.root / "sealed-queue.json").as_uri(),
            "queue_manifest_raw_sha256":
                "100d142cf459a663dd0f899db74b9666fbe49942d7ff0960280424899d39b5a0",
            "queue_identity_sha256":
                "66349d9b85c74f2376830edf2a7d4f0ccf9d4e093f9c55b8127429259f2948d1",
            "queue_id": "asrppqueue_66349d9b85c74f2376830edf2a7d4f0c",
            "queue_ordinal": 3,
            "routing_hint": "process",
            "work_order_uri": (self.root / "sealed-work-order.json").as_uri(),
            "work_order_raw_sha256": "a" * 64,
            "work_order_canonical_sha256": "b" * 64,
            "work_order_byte_count": 1024,
            "preprocess_result_uri": (self.root / "preprocess-result.json").as_uri(),
            "preprocess_result_raw_sha256": "c" * 64,
            "preprocess_result_canonical_sha256": "d" * 64,
            "preprocess_result_byte_count": 2048,
            "preprocess_import_batch_id": "unused_in_plan_only",
            "preprocess_processing_run_id": "run_preprocess_fixture",
            "preprocess_source_media_id": result["input"]["media_id"],
            "audio_artifact": {
                "artifact_id": result["input"]["artifact_id"],
                "media_id": result["input"]["media_id"],
            },
        }
        seal = {
            "seal_receipt_uri": (self.root / "seal-receipt.json").as_uri(),
            "seal_receipt_raw_sha256":
                "806439f2736dae9b95ffdffd19c3464c9efc39e79a5fbc21f7223b9f2c717b7b",
            "seal_receipt_id": "asrsealreceipt_c0694c5c36586eb433a766490f3dbc01",
            "seal_receipt_identity_sha256":
                "9f29212c38a78ff91faaea5dc7d8eb10f3d0405c0075ce8e365d4b33598df524",
            "seal_receipt_ordinal": 10,
            "sealed_result_directory_uri": Path(result["result_path"]).parent.as_uri(),
        }
        target = "himr_corpus.media_local_asr_bridge._require_exact_media_lineage"
        seal_target = "himr_corpus.media_local_asr_bridge._require_result_seal"
        with mock.patch(target, return_value=lineage), mock.patch(
            seal_target, return_value=seal
        ):
            plan = build_media_local_asr_admission_plan(
                self.connection, result["result_path"], self.root / "sealed-queue.json"
            )
        coordinates = plan["coordinate_contract"]
        self.assertEqual(coordinates["input_duration_ms"], 500)
        self.assertEqual(coordinates["max_segment_end_ms"], 600)
        self.assertEqual(coordinates["input_boundary_overrun_ms"], 100)
        self.assertEqual(coordinates["null_timed_word_count"], 0)

    def test_media_local_bridge_rejects_unsealed_result_state(self) -> None:
        result = self._produce(
            with_context=False,
            with_glossary=False,
            duration_ms=2_500,
            offset_ms=0,
            output_name="media-local-seal-guard",
        )
        result_path = Path(result["result_path"])
        result_dir = result_path.parent
        result_names = (
            "result.json", "transcript.normalized.json", "whisper.raw.json"
        )
        for name in result_names:
            (result_dir / name).chmod(0o400)
        result_dir.chmod(0o500)
        raw_body = result_path.read_bytes()
        raw = json.loads(raw_body)

        def receipt_fixture():
            directory_stat = result_dir.stat()
            files = []
            for name in result_names:
                path = result_dir / name
                file_stat = path.stat()
                files.append(
                    {
                        "byte_count": file_stat.st_size,
                        "content_unchanged": True,
                        "ctime_ns_after": file_stat.st_ctime_ns,
                        "device": file_stat.st_dev,
                        "inode": file_stat.st_ino,
                        "mode_after": 0o400,
                        "mtime_ns": file_stat.st_mtime_ns,
                        "mtime_unchanged": True,
                        "name": name,
                        "nlink": file_stat.st_nlink,
                        "path": str(path),
                        "sha256": digest(path),
                    }
                )
            member = {
                "directory": {
                    "content_entries_unchanged": True,
                    "ctime_ns_after": directory_stat.st_ctime_ns,
                    "device": directory_stat.st_dev,
                    "inode": directory_stat.st_ino,
                    "mode_after": 0o500,
                    "mtime_ns": directory_stat.st_mtime_ns,
                    "mtime_unchanged": True,
                    "nlink": directory_stat.st_nlink,
                    "path": str(result_dir),
                },
                "files": files,
                "input_sha256": raw["input"]["sha256"],
                "job_id": raw["job_id"],
                "ordinal": 9,
                "result_key": raw["result_key"],
                "result_path": str(result_path),
                "source_id": media_bridge.QUEUE_ID,
                "source_ordinal": 2,
                "work_order_sha256": raw["work_order_sha256"],
            }
            rows = [
                {"result_path": f"/sealed/not-selected/{ordinal}/result.json"}
                for ordinal in range(1, 26)
            ]
            rows[8] = member
            receipt = {
                "applied_at": OBSERVED_AT,
                "authority": {},
                "kind": "asr_whispercpp_completed_result_seal_receipt",
                "plan": {},
                "policy": {
                    "allowed_entries": list(result_names),
                    "directory_mode_after": 0o500,
                    "file_mode_after": 0o400,
                    "hardlink_policy": "all_three_files_must_have_nlink_1",
                },
                "result_count": 25,
                "results": rows,
                "schema_version": 1,
                "state": "applied_content_and_mtime_preserved",
            }
            identity = hashlib.sha256(
                json.dumps(
                    receipt, ensure_ascii=False, sort_keys=True, separators=(",", ":")
                ).encode()
            ).hexdigest()
            receipt["identity_sha256"] = identity
            receipt["receipt_id"] = "asrsealreceipt_test"
            body = (
                json.dumps(
                    receipt, ensure_ascii=False, sort_keys=True, separators=(",", ":")
                )
                + "\n"
            ).encode()
            return receipt, body, identity

        def assert_rejected(receipt, receipt_body, identity, pattern):
            sealed_json = (
                self.root / "receipt.json",
                receipt_body,
                receipt,
                hashlib.sha256(receipt_body).hexdigest(),
                hashlib.sha256(receipt_body.rstrip(b"\n")).hexdigest(),
            )
            with mock.patch.object(media_bridge, "_sealed_json", return_value=sealed_json), \
                 mock.patch.object(media_bridge, "SEAL_RECEIPT_ID", "asrsealreceipt_test"), \
                 mock.patch.object(
                     media_bridge, "SEAL_RECEIPT_IDENTITY_SHA256", identity
                 ):
                with self.assertRaisesRegex(ResultImportError, pattern):
                    media_bridge._require_result_seal(result_path, raw_body, raw)

        receipt, receipt_body, identity = receipt_fixture()
        media_bridge._resolved_file(result_path, "sealed fixture")
        (result_dir / "transcript.normalized.json").chmod(0o600)
        assert_rejected(receipt, receipt_body, identity, "differs from its exact seal")
        (result_dir / "transcript.normalized.json").chmod(0o400)

        receipt, receipt_body, identity = receipt_fixture()
        hardlink = self.root / "unexpected-hardlink.json"
        hardlink.hardlink_to(result_dir / "whisper.raw.json")
        receipt, receipt_body, identity = receipt_fixture()
        assert_rejected(receipt, receipt_body, identity, "differs from its exact seal")
        hardlink.unlink()

        result_dir.chmod(0o700)
        extra = result_dir / "unexpected.json"
        extra.write_text("{}\n", encoding="utf-8")
        result_dir.chmod(0o500)
        receipt, receipt_body, identity = receipt_fixture()
        assert_rejected(receipt, receipt_body, identity, "directory differs")

    def test_imports_inverted_token_with_null_word_time_and_exact_raw_provenance(self) -> None:
        result = self._produce(
            with_context=True,
            with_glossary=False,
            output_name="inverted-token",
            inverted_token=True,
        )
        self._seed_dependencies(result)
        first = import_asr_whispercpp_result(self.connection, result["result_path"])
        second = import_asr_whispercpp_result(self.connection, result["result_path"])
        self.assertEqual(first, second)

        word = self.connection.execute(
            """
            SELECT w.start_ms, w.end_ms
            FROM transcript_words w
            JOIN transcript_segments s ON s.segment_id = w.segment_id
            JOIN transcript_revisions r ON r.revision_id = s.revision_id
            WHERE r.processing_run_id = ? AND w.ordinal = 1
            """,
            (result["processing_run"]["processing_run_id"],),
        ).fetchone()
        self.assertIsNone(word["start_ms"])
        self.assertIsNone(word["end_ms"])

        segment_row = self.connection.execute(
            """
            SELECT s.start_ms, s.end_ms, s.metadata_json
            FROM transcript_segments s
            JOIN transcript_revisions r ON r.revision_id = s.revision_id
            WHERE r.processing_run_id = ?
            """,
            (result["processing_run"]["processing_run_id"],),
        ).fetchone()
        self.assertEqual((segment_row["start_ms"], segment_row["end_ms"]), (250, 1_050))
        segment_metadata = json.loads(segment_row["metadata_json"])
        self.assertEqual(
            segment_metadata["token_timing_anomalies"],
            [
                {
                    "ordinal": 1,
                    "timing_state": "unavailable",
                    "timing_quality_flags": ["invalid_upstream_inverted"],
                    "original_offsets": {"from": 250, "to": 0},
                }
            ],
        )
        self.assertEqual(
            segment_metadata["engine_segment"]["tokens"][1]["offsets"],
            {"from": 250, "to": 0},
        )
        revision_metadata = json.loads(
            self.connection.execute(
                "SELECT metadata_json FROM transcript_revisions WHERE processing_run_id = ?",
                (result["processing_run"]["processing_run_id"],),
            ).fetchone()["metadata_json"]
        )
        self.assertEqual(
            revision_metadata["quality_flags"], ["token_timing_unavailable"]
        )

        tampered = copy.deepcopy(result)
        tampered["transcript"]["segments"][0]["tokens"][1]["original_offsets"]["to"] = 1
        with self.assertRaisesRegex(ResultImportError, "preserved raw offsets"):
            import_asr_whispercpp_result(
                self.connection,
                self._variant(tampered, "inverted-token-tampered"),
            )

    def test_legacy_022_normalized_token_shape_remains_valid(self) -> None:
        legacy = copy.deepcopy(self.result["transcript"])
        for segment in legacy["segments"]:
            for token in segment["tokens"]:
                token.pop("timing_state", None)
                token.pop("timing_quality_flags", None)
                token.pop("original_offsets", None)
        recipe = json.loads(self.result["processing_run"]["parameters_json"])
        recipe["implementation_version"] = "0.2.2"
        validated = _validate_transcript(
            legacy,
            window=self.result["window"],
            recipe=recipe,
        )
        self.assertEqual(validated, legacy)

    def test_023_inverted_token_contract_remains_compatible(self) -> None:
        result = self._produce(
            with_context=True,
            with_glossary=False,
            output_name="inverted-token-023-compatibility",
            inverted_token=True,
        )
        recipe = json.loads(result["processing_run"]["parameters_json"])
        recipe["implementation_version"] = "0.2.3"
        validated = _validate_transcript(
            result["transcript"],
            window=result["window"],
            recipe=recipe,
        )
        self.assertEqual(validated, result["transcript"])

    def test_null_context_imports_provenance_only_and_invents_no_recording(self) -> None:
        before_recordings = self.connection.execute("SELECT count(*) FROM recordings").fetchone()[0]
        result = self._produce(
            with_context=False,
            with_glossary=False,
            duration_ms=2_500,
            offset_ms=0,
            output_name="local-window-null-context",
        )
        self._seed_dependencies(result, with_context=False)
        self._mark_as_local_window(result, recording_start_ms=None)
        imported = import_asr_whispercpp_result(self.connection, result["result_path"])
        self.assertIsNone(imported["recording_id"])
        self.assertEqual(imported["transcript_revisions"], 0)
        self.assertEqual(
            self.connection.execute("SELECT count(*) FROM recordings").fetchone()[0],
            before_recordings,
        )
        self.assertEqual(
            self.connection.execute(
                "SELECT count(*) FROM processing_runs WHERE processing_run_id = ?",
                (result["processing_run"]["processing_run_id"],),
            ).fetchone()[0],
            1,
        )

    def test_overrun_flags_and_zero_length_times_survive_exactly(self) -> None:
        result = self._produce(
            with_context=True,
            with_glossary=False,
            duration_ms=500,
            output_name="overrun",
        )
        self._seed_dependencies(result)
        imported = import_asr_whispercpp_result(self.connection, result["result_path"])
        self.assertEqual(imported["transcript_segments"], 1)
        row = self.connection.execute(
            """
            SELECT s.end_ms, s.metadata_json
            FROM transcript_segments s
            JOIN transcript_revisions r ON r.revision_id = s.revision_id
            WHERE r.processing_run_id = ?
            """,
            (result["processing_run"]["processing_run_id"],),
        ).fetchone()
        self.assertEqual(row["end_ms"], 850)
        self.assertEqual(json.loads(row["metadata_json"])["window_overrun_ms"], 100)
        self.assertEqual(
            json.loads(row["metadata_json"])["quality_flags"],
            ["end_after_requested_window"],
        )

    def test_rejects_unknown_dry_run_cross_array_and_bad_registry_without_writes(self) -> None:
        mutations: list[tuple[str, dict]] = []
        unknown = copy.deepcopy(self.result)
        unknown["surprise"] = True
        mutations.append(("unknown", unknown))
        dry = copy.deepcopy(self.result)
        dry["status"] = "planned"
        dry["dry_run"] = True
        mutations.append(("dry-run", dry))
        cross = copy.deepcopy(self.result)
        cross["catalog_records"]["transcript_segments"][0]["text"] = "changed"
        mutations.append(("cross-array", cross))
        for name, value in mutations:
            with self.subTest(name=name):
                with self.assertRaises(ResultImportError):
                    import_asr_whispercpp_result(self.connection, self._variant(value, name))
        failure_path = self.root / "failed-result.json"
        write_json(
            failure_path,
            {
                "schema_version": 1,
                "status": "failed",
                "job_id": "failed-fixture",
                "error": {"type": "ASRError", "message": "fixture failure"},
                "errors": [{"type": "ASRError", "message": "fixture failure"}],
            },
        )
        with self.assertRaises(ResultImportError):
            import_asr_whispercpp_result(self.connection, failure_path)
        self.assertEqual(
            self.connection.execute(
                "SELECT count(*) FROM processing_runs WHERE stage = 'asr_whispercpp'"
            ).fetchone()[0],
            0,
        )
        wrong_model_result = self._produce(
            with_context=True,
            with_glossary=False,
            output_name="wrong-registry",
            model_id="model_whisper_fake_wrong_registry",
        )
        self._seed_dependencies(wrong_model_result, register_model=False)
        self.connection.execute(
            """
            INSERT INTO models(
                model_id, task, name, version, weights_sha256, license_label,
                configuration_json
            ) VALUES(?, 'asr', ?, ?, ?, ?, ?)
            """,
            (
                wrong_model_result["model"]["model_id"],
                wrong_model_result["model"]["name"],
                wrong_model_result["model"]["revision"],
                "0" * 64,
                wrong_model_result["model"]["license_label"],
                json.dumps({"source": wrong_model_result["model"]["source"]}),
            ),
        )
        with self.assertRaisesRegex(ResultImportError, "model"):
            import_asr_whispercpp_result(self.connection, wrong_model_result["result_path"])
        self.assertEqual(
            self.connection.execute("SELECT count(*) FROM import_batches").fetchone()[0],
            0,
        )

    def test_late_run_conflict_rolls_back_batch_artifacts_and_transcript(self) -> None:
        run_id = self.result["processing_run"]["processing_run_id"]
        self.connection.execute(
            """
            INSERT INTO processing_runs(
                processing_run_id, stage, implementation_version, parameters_json,
                environment_json, started_at, completed_at, status
            ) VALUES(?, 'conflicting_stage', 'fixture', '{}', '{}', ?, ?, 'completed')
            """,
            (run_id, OBSERVED_AT, OBSERVED_AT),
        )
        with self.assertRaisesRegex(ResultImportError, "processing_run_id"):
            import_asr_whispercpp_result(self.connection, self.result_path)
        self.assertEqual(
            self.connection.execute("SELECT count(*) FROM import_batches").fetchone()[0],
            0,
        )
        self.assertEqual(
            self.connection.execute(
                "SELECT count(*) FROM artifacts WHERE processing_run_id = ?", (run_id,)
            ).fetchone()[0],
            0,
        )
        self.assertEqual(
            self.connection.execute(
                "SELECT count(*) FROM transcript_revisions WHERE processing_run_id = ?",
                (run_id,),
            ).fetchone()[0],
            0,
        )

    def test_rejects_tampered_artifact_and_unsafe_uri_before_transaction(self) -> None:
        raw = next(
            artifact
            for artifact in self.result["artifacts"]
            if artifact["artifact_kind"] == "whispercpp_output_json_full"
        )
        raw_path = Path(raw["storage_uri"].removeprefix("file://"))
        original = raw_path.read_bytes()
        raw_path.write_bytes(b"{}\n")
        with self.assertRaisesRegex(ResultImportError, "byte_count|SHA-256"):
            import_asr_whispercpp_result(self.connection, self.result_path)
        raw_path.write_bytes(original)

        unsafe = copy.deepcopy(self.result)
        unsafe["artifacts"][0]["storage_uri"] = "file://remote-host/private/a.json"
        unsafe["catalog_records"]["artifacts"] = copy.deepcopy(unsafe["artifacts"])
        with self.assertRaisesRegex(ResultImportError, "local file URI"):
            import_asr_whispercpp_result(
                self.connection,
                self._variant(unsafe, "unsafe-uri", relocate_artifacts=False),
            )
        self.assertEqual(
            self.connection.execute("SELECT count(*) FROM import_batches").fetchone()[0],
            0,
        )


if __name__ == "__main__":
    unittest.main()
