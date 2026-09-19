from __future__ import annotations

import copy
import hashlib
import json
import sqlite3
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path

from evaluation.interval_proposal import (
    DEFAULT_POLICY,
    _Catalog,
    _apply_global_caps,
    _apply_global_caps_v2,
    _catalog_binding,
    _read_pinned_json,
    _validate_preprocess_result,
    _window_request_adapter,
    emit_full_rendition_request,
    emit_local_window_proposal_request,
    prepare_interval_proposal,
    validate_interval_proposal_request,
    validate_interval_proposal,
)
from evaluation.validation import (
    ContractError,
    _expected_rendition_id,
    _stable_id,
    canonical_manifest_sha256,
    load_json,
)


ROOT = Path(__file__).resolve().parents[2]
COHORT_PATH = ROOT / "evaluation/cohorts/himr-asr-candidate-cohort-v1.json"
REQUEST_SCHEMA = ROOT / "evaluation/schemas/interval-proposal-request.schema.json"
PROPOSAL_SCHEMA = ROOT / "evaluation/schemas/interval-proposal.schema.json"
REQUEST_V2_SCHEMA = ROOT / "evaluation/schemas/interval-proposal-request-v2.schema.json"
PROPOSAL_V2_SCHEMA = ROOT / "evaluation/schemas/interval-proposal-v2.schema.json"
LOCAL_WINDOW_RESULT_SCHEMA = ROOT / "pipeline/schemas/local-window-result.schema.json"


def canonical_bytes(value: object) -> bytes:
    return json.dumps(
        value, ensure_ascii=False, sort_keys=True, separators=(",", ":"), allow_nan=False
    ).encode()


def write_sealed_bytes(path: Path, body: bytes) -> None:
    """Explicitly unseal a test input, replace it, and reseal it read-only."""

    if path.exists():
        path.chmod(0o600)
    try:
        path.write_bytes(body)
    finally:
        if path.exists():
            path.chmod(0o400)


def write_sealed_json(path: Path, value: object) -> None:
    write_sealed_bytes(path, json.dumps(value).encode("utf-8"))


def reseal_request(request: dict) -> None:
    parts = [
        request["cohort_id"],
        request["created_at"],
        hashlib.sha256(canonical_bytes(request["policy"])).hexdigest(),
        hashlib.sha256(canonical_bytes(request["recordings"])).hexdigest(),
    ]
    if request.get("schema_version") == 2:
        parts.insert(0, 2)
    request["request_id"] = _stable_id("proposal_request", *parts)
    request["manifest_sha256"] = canonical_manifest_sha256(request)


class Fixture:
    def __init__(
        self,
        root: Path,
        *,
        local_window: bool = False,
        analysis_digit: str = "b",
        offset: int = 50_000,
        window_ordinal: int = 1,
        suffix: str = "",
    ):
        self.root = root
        self.cohort = load_json(COHORT_PATH)
        self.candidate = self.cohort["candidates"][0]
        self.parent_sha = "a" * 64
        self.parent_media_id = f"media_sha256_{self.parent_sha}"
        self.parent_duration = 120_000
        self.parent_bytes = 123_456
        self.rendition_kind = "acquired_source_media"
        self.rendition_id = _expected_rendition_id(
            self.candidate["recording_id"], self.parent_media_id, self.rendition_kind
        )
        self.analysis_sha = analysis_digit * 64 if local_window else self.parent_sha
        self.analysis_media_id = f"media_sha256_{self.analysis_sha}"
        self.analysis_duration = 30_000 if local_window else self.parent_duration
        self.analysis_bytes = 45_678 if local_window else self.parent_bytes
        self.offset = offset if local_window else 0
        self.source_end = self.offset + self.analysis_duration
        self.window_ordinal = window_ordinal
        run_digit = str((window_ordinal % 8) + 1) if local_window else "1"
        self.run_id = "run_preprocess_" + run_digit * 32
        self.run_parameters = {"contract_version": 1, "fixture": "media_preprocess"}
        self.run_environment = {"cpu_only": True, "fixture": "offline"}
        self.recipe_sha = hashlib.sha256(canonical_bytes(self.run_parameters)).hexdigest()
        self.audio_sha = "e" * 64
        self.suffix = suffix
        self.result_path = self.root / f"media-preprocess-result{suffix}.json"
        self.routing_path = self.root / f"routing{suffix}.json"
        self.probe_path = self.root / f"probe{suffix}.normalized.json"
        self.audio_path = self.root / f"audio-16khz-mono{suffix}.flac"
        self.local_path = self.root / f"local-window-result{suffix}.json"
        self.probe_artifact_id = f"artifact_probe_fixture{suffix}"
        self.routing_artifact_id = f"artifact_routing_fixture{suffix}"
        self.audio_artifact_id = f"artifact_audio_fixture{suffix}"
        self.db_path = self.root / "catalog.sqlite3"
        self._write_routing_artifact()
        write_sealed_bytes(self.probe_path, canonical_bytes({}))
        self.probe_sha = hashlib.sha256(self.probe_path.read_bytes()).hexdigest()
        self.probe_bytes = self.probe_path.stat().st_size
        self._create_database()
        self._write_preprocess_result()
        if local_window:
            self._write_local_window_result()

    def _create_database(self) -> None:
        connection = sqlite3.connect(self.db_path)
        connection.executescript(
            """
            CREATE TABLE schema_migrations(version INTEGER PRIMARY KEY,name TEXT,sha256 TEXT,applied_at TEXT);
            CREATE TABLE sources(source_id TEXT PRIMARY KEY,platform TEXT,source_kind TEXT,native_id TEXT,canonical_url TEXT,access_state TEXT);
            CREATE TABLE recordings(recording_id TEXT PRIMARY KEY);
            CREATE TABLE recording_sources(recording_source_id TEXT PRIMARY KEY,recording_id TEXT,source_id TEXT,mapping_role TEXT,source_start_ms INTEGER,source_end_ms INTEGER,recording_start_ms INTEGER,recording_end_ms INTEGER,mapping_method TEXT,confidence_state TEXT);
            CREATE TABLE media_objects(media_id TEXT PRIMARY KEY,sha256 TEXT,byte_count INTEGER,media_kind TEXT,mime_type TEXT,container TEXT,duration_ms INTEGER,ffprobe_json TEXT,first_cataloged_at TEXT,integrity_state TEXT);
            CREATE TABLE media_locations(media_location_id TEXT PRIMARY KEY,media_id TEXT,storage_uri TEXT,storage_class TEXT,verified_at TEXT,is_primary INTEGER);
            CREATE TABLE media_derivations(child_media_id TEXT,parent_media_id TEXT,derivation_kind TEXT,processing_run_id TEXT,metadata_json TEXT,PRIMARY KEY(child_media_id,parent_media_id,derivation_kind));
            CREATE TABLE renditions(rendition_id TEXT PRIMARY KEY,recording_id TEXT,media_id TEXT,rendition_kind TEXT,review_state TEXT);
            CREATE TABLE processing_runs(processing_run_id TEXT PRIMARY KEY,stage TEXT,implementation_version TEXT,model_id TEXT,glossary_revision_id TEXT,parameters_json TEXT,environment_json TEXT,random_seed INTEGER,started_at TEXT,completed_at TEXT,status TEXT,error_text TEXT);
            CREATE TABLE run_inputs(run_input_id TEXT PRIMARY KEY,processing_run_id TEXT,object_type TEXT,object_id TEXT,input_role TEXT,input_sha256 TEXT);
            CREATE TABLE artifacts(artifact_id TEXT PRIMARY KEY,processing_run_id TEXT,artifact_kind TEXT,storage_uri TEXT,sha256 TEXT,byte_count INTEGER,schema_version INTEGER,visibility TEXT);
            """
        )
        connection.execute(
            "INSERT INTO schema_migrations VALUES(1,'fixture',?,?)",
            ("f" * 64, "2026-08-26T00:00:00Z"),
        )
        connection.execute(
            "INSERT INTO sources VALUES(?,?,?,?,?,?)",
            (
                self.candidate["source_id"],
                "youtube",
                "youtube_video",
                self.candidate["native_id"],
                self.candidate["public_locator"],
                "public",
            ),
        )
        connection.execute(
            "INSERT INTO recordings VALUES(?)", (self.candidate["recording_id"],)
        )
        connection.execute(
            "INSERT INTO recording_sources VALUES(?,?,?,?,?,?,?,?,?,?)",
            (
                "recording_source_fixture",
                self.candidate["recording_id"],
                self.candidate["source_id"],
                "primary",
                None,
                None,
                None,
                None,
                "exact",
                "metadata_only",
            ),
        )
        connection.execute(
            "INSERT INTO media_objects VALUES(?,?,?,?,?,?,?,?,?,?)",
            (
                self.parent_media_id,
                self.parent_sha,
                self.parent_bytes,
                "video",
                "video/mp4",
                "mp4",
                self.parent_duration,
                "{}",
                "2026-08-26T20:00:00Z",
                "verified",
            ),
        )
        if self.analysis_media_id != self.parent_media_id:
            connection.execute(
                "INSERT INTO media_objects VALUES(?,?,?,?,?,?,?,?,?,?)",
                (
                    self.analysis_media_id,
                    self.analysis_sha,
                    self.analysis_bytes,
                    "video",
                    "video/mp4",
                    "mp4",
                    self.analysis_duration,
                    "{}",
                    "2026-08-26T20:00:00Z",
                    "verified",
                ),
            )
        connection.execute(
            "INSERT INTO renditions VALUES(?,?,?,?,?)",
            (
                self.rendition_id,
                self.candidate["recording_id"],
                self.parent_media_id,
                self.rendition_kind,
                "unreviewed",
            ),
        )
        connection.execute(
            "INSERT INTO processing_runs VALUES(?,?,?,?,?,?,?,?,?,?,?,?)",
            (
                self.run_id,
                "media_preprocess",
                "test-1",
                None,
                None,
                canonical_bytes(self.run_parameters).decode(),
                canonical_bytes(self.run_environment).decode(),
                None,
                "2026-08-26T21:00:00Z",
                "2026-08-26T21:01:00Z",
                "completed",
                None,
            ),
        )
        connection.execute(
            "INSERT INTO run_inputs VALUES(?,?,?,?,?,?)",
            (
                f"run_input_fixture{self.suffix}",
                self.run_id,
                "media",
                self.analysis_media_id,
                "source_media",
                self.analysis_sha,
            ),
        )
        for artifact_id, kind, digest, size in (
            (
                self.probe_artifact_id,
                "ffprobe_normalized_json",
                self.probe_sha,
                self.probe_bytes,
            ),
            (
                self.routing_artifact_id,
                "scene_silence_routing_json",
                self.routing_sha,
                self.routing_bytes,
            ),
            (self.audio_artifact_id, "audio_16khz_mono_flac", self.audio_sha, 500),
        ):
            connection.execute(
                "INSERT INTO artifacts VALUES(?,?,?,?,?,?,?,?)",
                (
                    artifact_id,
                    self.run_id,
                    kind,
                    {
                        "ffprobe_normalized_json": self.probe_path,
                        "scene_silence_routing_json": self.routing_path,
                        "audio_16khz_mono_flac": self.audio_path,
                    }[kind].as_uri(),
                    digest,
                    size,
                    1,
                    "private",
                ),
            )
        connection.commit()
        connection.close()

    def _routing_value(self) -> dict:
        scenes = [
            {"timestamp_ms": 15_000, "score_percent": 60.0},
            {"timestamp_ms": min(75_000, self.analysis_duration), "score_percent": 50.0},
        ]
        silences = [
            {"start_ms": 0, "end_ms": 2_000, "duration_ms": 2_000},
            {
                "start_ms": self.analysis_duration - 3_000,
                "end_ms": self.analysis_duration,
                "duration_ms": 3_000,
            },
        ]
        silent_duration = sum(item["duration_ms"] for item in silences)
        return {
            "schema_version": 1,
            "source_media_id": self.analysis_media_id,
            "coverage": {
                "duration_ms": self.analysis_duration,
                "has_audio": True,
                "has_video": True,
            },
            "parameters": {
                "scene_threshold_percent": 10.0,
                "silence_noise_db": -35.0,
                "silence_min_duration_ms": 750,
                "near_silent_fraction": 0.98,
            },
            "routing_candidates": {
                "asr": "process",
                "ocr": "scene_keyframes",
                "visual": "scene_and_speech_windows",
                "diarization": "router_pending",
                "active_speaker": "router_pending",
            },
            "scene_changes": scenes,
            "silence_intervals": silences,
            "summary": {
                "scene_change_count": len(scenes),
                "silence_interval_count": len(silences),
                "silent_duration_ms": silent_duration,
                "silent_fraction": round(silent_duration / self.analysis_duration, 6),
            },
            "warning": (
                "Routing values are machine-generated workload suggestions, not content "
                "findings. They do not identify a speaker or establish that a video is "
                "single-speaker."
            ),
        }

    def _write_routing_artifact(self) -> None:
        body = canonical_bytes(self._routing_value())
        write_sealed_bytes(self.routing_path, body)
        self.routing_sha = hashlib.sha256(body).hexdigest()
        self.routing_bytes = len(body)

    def _preprocess_value(self) -> dict:
        return {
            "schema_version": 1,
            "status": "completed",
            "dry_run": False,
            "job_id": "job_fixture",
            "input": {
                "media_id": self.analysis_media_id,
                "sha256": self.analysis_sha,
                "byte_count": self.analysis_bytes,
                "path": str(self.root / "source-media.mp4"),
                "storage_uri": (self.root / "source-media.mp4").as_uri(),
                "stat_before": {
                    "device": 1,
                    "inode": 2,
                    "byte_count": self.analysis_bytes,
                    "mtime_ns": 3,
                },
                "stat_after": {
                    "device": 1,
                    "inode": 2,
                    "byte_count": self.analysis_bytes,
                    "mtime_ns": 3,
                },
                "unchanged": True,
            },
            "layout": {
                "object_dir": str(self.root),
                "output_root": str(self.root),
                "recipe_sha256": self.recipe_sha,
                "run_dir": str(self.root),
            },
            "processing_run": {
                "processing_run_id": self.run_id,
                "stage": "media_preprocess",
                "implementation_version": "test-1",
                "parameters_json": self.run_parameters,
                "environment_json": self.run_environment,
                "started_at": "2026-08-26T21:00:00Z",
                "completed_at": "2026-08-26T21:01:00Z",
                "status": "completed",
            },
            "steps": [
                {
                    "name": name,
                    "status": "completed",
                    "command": ["/usr/bin/fixture", name],
                    "output_path": str(self.root / f"{name}.out"),
                }
                for name in ("probe", "audio_flac", "proxy", "routing")
            ],
            "artifacts": [
                {
                    "schema_version": 1,
                    "artifact_id": self.probe_artifact_id,
                    "artifact_kind": "ffprobe_normalized_json",
                    "sha256": self.probe_sha,
                    "byte_count": self.probe_bytes,
                    "visibility": "private",
                    "processing_run_id": self.run_id,
                    "path": str(self.probe_path),
                    "storage_uri": self.probe_path.as_uri(),
                    "media_kind": "document",
                    "mime_type": "application/json",
                    "normalized_probe": None,
                },
                {
                    "schema_version": 1,
                    "artifact_id": self.routing_artifact_id,
                    "artifact_kind": "scene_silence_routing_json",
                    "sha256": self.routing_sha,
                    "byte_count": self.routing_bytes,
                    "visibility": "private",
                    "processing_run_id": self.run_id,
                    "path": str(self.routing_path),
                    "storage_uri": self.routing_path.as_uri(),
                    "media_kind": "document",
                    "mime_type": "application/json",
                    "normalized_probe": None,
                },
                {
                    "schema_version": 1,
                    "artifact_id": self.audio_artifact_id,
                    "artifact_kind": "audio_16khz_mono_flac",
                    "sha256": self.audio_sha,
                    "byte_count": 500,
                    "visibility": "private",
                    "processing_run_id": self.run_id,
                    "path": str(self.audio_path),
                    "storage_uri": self.audio_path.as_uri(),
                    "media_kind": "audio",
                    "mime_type": "audio/flac",
                    "normalized_probe": {},
                },
            ],
            "routing": self._routing_value(),
            "duration_ms": self.analysis_duration,
            "errors": [],
            "catalog_records": {
                "media_objects": [],
                "media_locations": [],
                "processing_runs": [],
                "run_inputs": [],
                "artifacts": [],
                "media_derivations": [],
            },
            "result_path": str(self.result_path),
        }

    def _write_preprocess_result(self) -> None:
        write_sealed_json(self.result_path, self._preprocess_value())

    def _write_local_window_result(self) -> None:
        source_stat = {
            "device": 1,
            "inode": 2,
            "byte_count": self.parent_bytes,
            "mtime_ns": 3,
        }
        tool = {
            "path": "/usr/bin/fixture",
            "sha256": "3" * 64,
            "byte_count": 1,
            "version_output_sha256": "4" * 64,
            "version_first_line": "fixture 1",
        }
        value = {
            "schema_version": 1,
            "implementation_version": "test-1",
            "status": "completed",
            "dry_run": False,
            "job_id": "window_job_fixture",
            "bundle_id": "windowbundle_" + "1" * 32,
            "work_order_sha256": "2" * 64,
            "source": {
                "path": str(self.root / "parent-media.mp4"),
                "media_id": self.parent_media_id,
                "expected_sha256": self.parent_sha,
                "byte_count": self.parent_bytes,
                "duration_ms": self.parent_duration,
                "acquisition_result_path": str(self.root / "acquisition-result.json"),
                "acquisition_result_sha256": "5" * 64,
                "stat_before": source_stat,
                "stat_after": source_stat,
                "unchanged": True,
            },
            "window": {
                "window_id": f"window_{self.window_ordinal:06d}",
                "ordinal": self.window_ordinal,
                "start_ms": self.offset,
                "end_ms": self.source_end,
                "boundary": "half_open",
                "is_partial_tail": False,
            },
            "tools": {"ffmpeg": tool, "ffprobe": tool},
            "profile": {
                "profile_id": "long-window-cpu-v1",
                "ffmpeg_threads": 1,
                "audio_sample_rate_hz": 16000,
                "audio_channels": 1,
                "audio_sample_format": "s16",
                "flac_compression_level": 8,
                "proxy_width": 640,
                "proxy_height": 360,
                "proxy_fps": 25,
                "proxy_video_codec": "libx264",
                "proxy_preset": "veryfast",
                "proxy_crf": 28,
                "proxy_audio_codec": "aac",
                "proxy_audio_bitrate": "96k",
                "video_stream_index": None,
                "audio_stream_index": 0,
            },
            "limits": {
                "max_window_output_bytes": 1_000_000,
                "free_space_floor_bytes": 0,
                "timeout_seconds": 60,
            },
            "commands": [["/usr/bin/fixture", "probe"], ["/usr/bin/fixture", "audio"]],
            "artifacts": [
                {
                    "artifact_id": "window_artifact_fixture",
                    "artifact_kind": "window_audio_16khz_mono_flac",
                    "path": str(self.root / "window-audio.flac"),
                    "sha256": self.analysis_sha,
                    "byte_count": self.analysis_bytes,
                    "visibility": "private",
                    "normalized_probe": {
                        "duration_ms": self.analysis_duration,
                        "video_stream_index": None,
                        "audio_stream_index": 0,
                        "video": None,
                        "audio": {
                            "codec_name": "flac",
                            "sample_rate_hz": 16000,
                            "channels": 1,
                            "sample_format": "s16"
                        },
                    }
                }
            ],
            "time_mapping": {
                "boundary": "half_open",
                "source_start_ms": self.offset,
                "source_end_ms": self.source_end,
                "artifact_zero_maps_to_source_ms": self.offset,
                "coordinate_precision": "integer_millisecond_contract",
                "extraction_method": "ffmpeg_accurate_seek_transcode",
                "byte_exact_source_fragment": False,
            },
            "safety": {
                "network_allowed": False,
                "credentials_allowed": False,
                "identity_claims_allowed": False,
                "publication_authority": "none",
                "source_bytes_preserved": True,
                "remote_section_download": False,
            },
            "result_path": str(self.local_path),
        }
        write_sealed_json(self.local_path, value)

    def request(self) -> dict:
        if self.analysis_media_id == self.parent_media_id:
            return emit_full_rendition_request(
                self.cohort,
                self.db_path,
                [self.result_path],
                "2026-08-26T22:00:00Z",
                dict(DEFAULT_POLICY),
            )
        result_digest = hashlib.sha256(self.result_path.read_bytes()).hexdigest()
        local_digest = hashlib.sha256(self.local_path.read_bytes()).hexdigest()
        request = {
            "schema_version": 1,
            "manifest_kind": "interval_proposal_request",
            "manifest_sha256": "0" * 64,
            "request_id": "placeholder",
            "created_at": "2026-08-26T22:00:00Z",
            "cohort_id": self.cohort["cohort_id"],
            "cohort_manifest_sha256": self.cohort["manifest_sha256"],
            "policy": dict(DEFAULT_POLICY),
            "safety": {
                "selection_basis": "sealed_media_metadata_scene_silence_routing_only",
                "asr_output_inputs_allowed": False,
                "transcript_inputs_allowed": False,
                "direct_media_content_inspection_allowed": False,
                "output_path_allowed": False,
                "catalog_write_allowed": False,
                "freeze_authority": "none",
                "reference_quality_authority": "none",
            },
            "recordings": [
                {
                    "candidate_id": self.candidate["candidate_id"],
                    "recording_id": self.candidate["recording_id"],
                    "source_id": self.candidate["source_id"],
                    "source_native_id": self.candidate["native_id"],
                    "source_locator": self.candidate["public_locator"],
                    "rendition_id": self.rendition_id,
                    "rendition_kind": self.rendition_kind,
                    "parent_media_id": self.parent_media_id,
                    "parent_media_sha256": self.parent_sha,
                    "parent_media_byte_count": self.parent_bytes,
                    "parent_media_duration_ms": self.parent_duration,
                    "analysis_media_id": self.analysis_media_id,
                    "analysis_media_sha256": self.analysis_sha,
                    "analysis_media_byte_count": self.analysis_bytes,
                    "analysis_media_duration_ms": self.analysis_duration,
                    "preprocess_result_path": str(self.result_path),
                    "preprocess_result_sha256": result_digest,
                    "timeline": {
                        "binding_kind": "local_window",
                        "source_offset_ms": self.offset,
                        "source_end_ms": self.source_end,
                        "local_window_result_path": str(self.local_path),
                        "local_window_result_sha256": local_digest,
                    },
                }
            ],
        }
        reseal_request(request)
        return request


class AdmittedMultiWindowFixture:
    """Two sealed routing results with catalogue-admitted local derivatives."""

    def __init__(self, root: Path, *, second_offset: int = 40_000):
        first_root = root / "first"
        second_root = root / "second"
        first_root.mkdir()
        second_root.mkdir()
        self.first = Fixture(
            first_root,
            local_window=True,
            analysis_digit="b",
            offset=0,
            window_ordinal=1,
            suffix="_1",
        )
        self.second = Fixture(
            second_root,
            local_window=True,
            analysis_digit="c",
            offset=second_offset,
            window_ordinal=2,
            suffix="_2",
        )
        self.cohort = self.first.cohort
        self.db_path = self.first.db_path
        self.windows = [self.first, self.second]
        self._extend_catalog_schema()
        self._copy_second_preprocess_lineage()
        for fixture in self.windows:
            self._admit_preprocess_result(fixture)
        for index, fixture in enumerate(self.windows):
            self._seal_admitted_result(fixture, proxy_digit="d" if index == 0 else "f")
            self._admit(fixture)

    def _extend_catalog_schema(self) -> None:
        connection = sqlite3.connect(self.db_path)
        connection.executescript(
            """
            ALTER TABLE artifacts ADD COLUMN metadata_json TEXT NOT NULL DEFAULT '{}';
            ALTER TABLE renditions ADD COLUMN label TEXT;
            ALTER TABLE renditions ADD COLUMN metadata_json TEXT NOT NULL DEFAULT '{}';
            ALTER TABLE recordings ADD COLUMN review_state TEXT NOT NULL DEFAULT 'unreviewed';
            ALTER TABLE recordings ADD COLUMN merged_into_recording_id TEXT;
            CREATE TABLE timeline_map_spans(
                timeline_map_span_id TEXT PRIMARY KEY,
                rendition_id TEXT,
                ordinal INTEGER,
                media_start_ms INTEGER,
                media_end_ms INTEGER,
                recording_start_ms INTEGER,
                recording_end_ms INTEGER,
                mapping_kind TEXT,
                confidence_state TEXT
            );
            CREATE TABLE import_batches(
                import_batch_id TEXT PRIMARY KEY,
                importer_name TEXT NOT NULL,
                importer_version TEXT NOT NULL,
                input_sha256 TEXT NOT NULL,
                source_snapshot_date TEXT,
                started_at TEXT NOT NULL,
                completed_at TEXT,
                status TEXT NOT NULL,
                statistics_json TEXT NOT NULL
            );
            """
        )
        connection.execute(
            "UPDATE recording_sources SET mapping_role='current_platform_listing'"
        )
        connection.commit()
        connection.close()

    def _copy_second_preprocess_lineage(self) -> None:
        source = sqlite3.connect(self.second.db_path)
        source.row_factory = sqlite3.Row
        target = sqlite3.connect(self.db_path)
        analysis = source.execute(
            "SELECT * FROM media_objects WHERE media_id=?",
            (self.second.analysis_media_id,),
        ).fetchone()
        target.execute(
            "INSERT INTO media_objects VALUES(?,?,?,?,?,?,?,?,?,?)", tuple(analysis)
        )
        run = source.execute(
            "SELECT * FROM processing_runs WHERE processing_run_id=?",
            (self.second.run_id,),
        ).fetchone()
        target.execute(
            "INSERT INTO processing_runs VALUES(?,?,?,?,?,?,?,?,?,?,?,?)", tuple(run)
        )
        run_input = source.execute(
            "SELECT * FROM run_inputs WHERE processing_run_id=?",
            (self.second.run_id,),
        ).fetchone()
        target.execute("INSERT INTO run_inputs VALUES(?,?,?,?,?,?)", tuple(run_input))
        for artifact in source.execute(
            "SELECT artifact_id,processing_run_id,artifact_kind,storage_uri,sha256,"
            "byte_count,schema_version,visibility FROM artifacts WHERE processing_run_id=?",
            (self.second.run_id,),
        ):
            target.execute(
                "INSERT INTO artifacts(artifact_id,processing_run_id,artifact_kind,"
                "storage_uri,sha256,byte_count,schema_version,visibility) "
                "VALUES(?,?,?,?,?,?,?,?)",
                tuple(artifact),
            )
        target.commit()
        target.close()
        source.close()

    def _admit_preprocess_result(self, fixture: Fixture) -> None:
        digest = hashlib.sha256(
            canonical_bytes(load_json(fixture.result_path))
        ).hexdigest()
        connection = sqlite3.connect(self.db_path)
        connection.execute(
            "INSERT INTO import_batches VALUES(?,?,?,?,?,?,?,?,?)",
            (
                _stable_id("imp", "media_preprocess_result_v1", digest),
                "media_preprocess_result_v1",
                "fixture-1",
                digest,
                None,
                "2026-08-26T21:00:00Z",
                "2026-08-26T21:01:00Z",
                "completed",
                "{}",
            ),
        )
        connection.commit()
        connection.close()

    def _replace_admitted_preprocess_result(
        self, fixture: Fixture, value: dict
    ) -> None:
        old_digest = hashlib.sha256(
            canonical_bytes(load_json(fixture.result_path))
        ).hexdigest()
        write_sealed_json(fixture.result_path, value)
        connection = sqlite3.connect(self.db_path)
        connection.execute(
            "DELETE FROM import_batches WHERE importer_name=? AND input_sha256=?",
            ("media_preprocess_result_v1", old_digest),
        )
        connection.commit()
        connection.close()
        self._admit_preprocess_result(fixture)

    def _seal_admitted_result(self, fixture: Fixture, *, proxy_digit: str) -> None:
        value = load_json(fixture.local_path)
        ordinal = fixture.window_ordinal
        window_id = f"window_{ordinal:06d}"
        value["job_id"] = f"local-window-{ordinal:06d}"
        value["window"]["window_id"] = window_id
        value["window"]["ordinal"] = ordinal
        value["commands"] = [
            ["/usr/bin/fixture", "audio"],
            ["/usr/bin/fixture", "probe-audio"],
            ["/usr/bin/fixture", "proxy"],
            ["/usr/bin/fixture", "probe-proxy"],
        ]
        audio = value["artifacts"][0]
        audio["artifact_id"] = self._producer_artifact_id(
            value["bundle_id"], window_id, audio["artifact_kind"], audio["sha256"]
        )
        proxy_sha = proxy_digit * 64
        proxy = {
            "artifact_id": self._producer_artifact_id(
                value["bundle_id"],
                window_id,
                "window_low_resolution_cfr_proxy",
                proxy_sha,
            ),
            "artifact_kind": "window_low_resolution_cfr_proxy",
            "path": str(fixture.root / f"window-proxy{fixture.suffix}.mp4"),
            "sha256": proxy_sha,
            "byte_count": 67_890 + ordinal,
            "visibility": "private",
            "normalized_probe": {
                "duration_ms": fixture.analysis_duration,
                "video_stream_index": 0,
                "audio_stream_index": 1,
                "video": {
                    "codec_name": "h264",
                    "width": 640,
                    "height": 360,
                    "pixel_format": "yuv420p",
                    "average_frame_rate": 25.0,
                },
                "audio": {
                    "codec_name": "aac",
                    "sample_rate_hz": 48000,
                    "channels": 2,
                    "sample_format": "fltp",
                },
            },
        }
        value["artifacts"] = [audio, proxy]
        write_sealed_json(fixture.local_path, value)

    @staticmethod
    def _producer_artifact_id(
        bundle_id: str, window_id: str, artifact_kind: str, digest: str
    ) -> str:
        return "artifact_" + hashlib.sha256(
            "\x1f".join((bundle_id, window_id, artifact_kind, digest)).encode()
        ).hexdigest()[:32]

    def _admit(self, fixture: Fixture) -> None:
        result = load_json(fixture.local_path)
        result_sha = hashlib.sha256(fixture.local_path.read_bytes()).hexdigest()
        acquisition_batch = _stable_id("imp", "fixture-acquisition", fixture.parent_media_id)
        acquisition_run = _stable_id("run", "fixture-acquisition", fixture.parent_media_id)
        run_id = _stable_id(
            "run",
            "local_window_result_admission",
            result_sha,
            fixture.parent_media_id,
            acquisition_batch,
        )
        source_mapping = {
            "recording_source_id": "recording_source_fixture",
            "mapping_role": "current_platform_listing",
            "source_start_ms": None,
            "source_end_ms": None,
            "recording_start_ms": None,
            "recording_end_ms": None,
            "mapping_method": "exact",
            "confidence_state": "metadata_only",
        }
        parameters = {
            "contract_version": 1,
            "run_semantics": "catalog_admission_verification_not_extraction_execution",
            "local_window_result_sha256": result_sha,
            "local_window_result_uri": fixture.local_path.as_uri(),
            "local_window_implementation_version": result["implementation_version"],
            "work_order_sha256": result["work_order_sha256"],
            "bundle_id": result["bundle_id"],
            "window": result["window"],
            "time_mapping": result["time_mapping"],
            "acquisition_result_sha256": result["source"]["acquisition_result_sha256"],
            "acquisition_import_sha256": "8" * 64,
            "acquisition_import_batch_id": acquisition_batch,
            "acquisition_processing_run_id": acquisition_run,
            "catalog_context_basis": [
                {
                    "recording_id": fixture.candidate["recording_id"],
                    "parent_rendition_id": fixture.rendition_id,
                    "parent_rendition_review_state": "unreviewed",
                    "source_mappings": [source_mapping],
                }
            ],
        }
        environment = {
            "observation_timestamp_basis": "operator_supplied_catalog_admission_time",
            "producer_extraction_time_state": "not_present_in_local_window_result_v1",
            "network_access_performed": False,
            "credentials_used": False,
            "publication_authority": "none",
            "identity_claims_allowed": False,
        }
        connection = sqlite3.connect(self.db_path)
        connection.execute(
            "INSERT INTO processing_runs VALUES(?,?,?,?,?,?,?,?,?,?,?,?)",
            (
                run_id,
                "local_window_result_admission",
                "local-window-catalog-bridge/1",
                None,
                None,
                json.dumps(parameters, sort_keys=True, separators=(",", ":")),
                json.dumps(environment, sort_keys=True, separators=(",", ":")),
                None,
                "2026-08-26T21:30:00Z",
                "2026-08-26T21:30:00Z",
                "completed",
                None,
            ),
        )
        for object_type, object_id, role, digest in (
            (
                "media",
                fixture.parent_media_id,
                "verified_acquired_parent_media",
                fixture.parent_sha,
            ),
            (
                "import_batch",
                acquisition_batch,
                "verified_acquisition_catalog_admission",
                result["source"]["acquisition_result_sha256"],
            ),
        ):
            connection.execute(
                "INSERT INTO run_inputs VALUES(?,?,?,?,?,?)",
                (
                    _stable_id("rin", run_id, object_type, object_id, role),
                    run_id,
                    object_type,
                    object_id,
                    role,
                    digest,
                ),
            )
        coordinate_mapping = {
            "recording_start_ms": None,
            "recording_end_ms": None,
            "state": "unasserted_no_unique_catalog_transform",
            "basis_ids": ["recording_source_fixture"],
            "timeline_mapping_kind": "unknown",
        }
        for artifact in result["artifacts"]:
            media_id = f"media_sha256_{artifact['sha256']}"
            if media_id != fixture.analysis_media_id:
                connection.execute(
                    "INSERT INTO media_objects VALUES(?,?,?,?,?,?,?,?,?,?)",
                    (
                        media_id,
                        artifact["sha256"],
                        artifact["byte_count"],
                        "video",
                        "video/mp4",
                        "mp4",
                        artifact["normalized_probe"]["duration_ms"],
                        "{}",
                        "2026-08-26T21:30:00Z",
                        "verified",
                    ),
                )
            evidence = {
                "contract_version": 1,
                "run_semantics": "catalog_admission_verification_not_extraction_execution",
                "local_window_result_sha256": result_sha,
                "local_window_result_uri": fixture.local_path.as_uri(),
                "acquisition_result_sha256": result["source"]["acquisition_result_sha256"],
                "acquisition_import_batch_id": acquisition_batch,
                "source_media_id": fixture.parent_media_id,
                "source_time_mapping": result["time_mapping"],
                "window": result["window"],
                "normalized_probe": artifact["normalized_probe"],
                "boundary_calibration_state": "not_calibrated",
                "representation_is_original_source": False,
                "publication_state": "withheld_by_default",
                "identity_authority": "none",
            }
            kind = (
                f"local_window:{artifact['artifact_kind']}:"
                f"{result['bundle_id']}:{result['window']['window_id']}"
            )
            connection.execute(
                "INSERT INTO media_derivations VALUES(?,?,?,?,?)",
                (
                    media_id,
                    fixture.parent_media_id,
                    kind,
                    run_id,
                    json.dumps(evidence, sort_keys=True, separators=(",", ":")),
                ),
            )
            connection.execute(
                "INSERT INTO artifacts VALUES(?,?,?,?,?,?,?,?,?)",
                (
                    artifact["artifact_id"],
                    run_id,
                    artifact["artifact_kind"],
                    Path(artifact["path"]).as_uri(),
                    artifact["sha256"],
                    artifact["byte_count"],
                    1,
                    "private",
                    json.dumps(
                        {**evidence, "media_id": media_id},
                        sort_keys=True,
                        separators=(",", ":"),
                    ),
                ),
            )
            rendition_id = _expected_rendition_id(
                fixture.candidate["recording_id"], media_id, kind
            )
            rendition_metadata = {
                **evidence,
                "parent_rendition_id": fixture.rendition_id,
                "recording_coordinate_mapping": coordinate_mapping,
            }
            connection.execute(
                "INSERT INTO renditions VALUES(?,?,?,?,?,?,?)",
                (
                    rendition_id,
                    fixture.candidate["recording_id"],
                    media_id,
                    kind,
                    "unreviewed",
                    "fixture local window",
                    json.dumps(
                        rendition_metadata, sort_keys=True, separators=(",", ":")
                    ),
                ),
            )
            mapped_duration = min(
                artifact["normalized_probe"]["duration_ms"],
                fixture.source_end - fixture.offset,
            )
            connection.execute(
                "INSERT INTO timeline_map_spans VALUES(?,?,?,?,?,?,?,?,?)",
                (
                    _stable_id("tms", rendition_id, 0),
                    rendition_id,
                    0,
                    0,
                    mapped_duration,
                    None,
                    None,
                    "unknown",
                    "metadata_only",
                ),
            )
            if media_id == fixture.analysis_media_id:
                fixture.analysis_rendition_id = rendition_id
                fixture.analysis_rendition_kind = kind
                fixture.admission_run_id = run_id
        connection.commit()
        connection.close()

    def emit(self, *, policy: dict[str, int] | None = None) -> dict:
        return emit_local_window_proposal_request(
            self.cohort,
            self.db_path,
            [fixture.local_path for fixture in self.windows],
            [fixture.result_path for fixture in self.windows],
            "2026-08-26T22:00:00Z",
            dict(DEFAULT_POLICY if policy is None else policy),
        )


class IntervalProposalTests(unittest.TestCase):
    def test_catalog_audit_is_filesystem_nonmutating(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            fixture = Fixture(Path(directory))
            catalog = fixture.db_path
            parent = catalog.parent

            def snapshot() -> tuple[object, ...]:
                catalog_stat = catalog.lstat()
                parent_stat = parent.lstat()
                return (
                    hashlib.sha256(catalog.read_bytes()).hexdigest(),
                    catalog_stat.st_size,
                    catalog_stat.st_mode,
                    catalog_stat.st_mtime_ns,
                    catalog_stat.st_ctime_ns,
                    parent_stat.st_mtime_ns,
                    parent_stat.st_ctime_ns,
                    tuple(sorted(path.name for path in parent.iterdir())),
                )

            before = snapshot()
            with _Catalog(catalog) as audit:
                self.assertEqual(
                    audit.one(
                        "SELECT count(*) AS count FROM sources",
                        (),
                        "fixture.sources",
                    )["count"],
                    1,
                )
            self.assertEqual(snapshot(), before)
            self.assertFalse(Path(str(catalog) + "-wal").exists())
            self.assertFalse(Path(str(catalog) + "-shm").exists())
            self.assertFalse(Path(str(catalog) + "-journal").exists())

    def test_catalog_audit_rejects_any_existing_sqlite_sidecar(self) -> None:
        for suffix in ("-wal", "-shm", "-journal"):
            with (
                self.subTest(suffix=suffix),
                tempfile.TemporaryDirectory() as directory,
            ):
                fixture = Fixture(Path(directory))
                sidecar = Path(str(fixture.db_path) + suffix)
                sidecar.write_bytes(b"")
                with self.assertRaisesRegex(
                    ContractError,
                    "closed, checkpointed SQLite file with no sidecars",
                ):
                    _Catalog(fixture.db_path)

    def test_global_cap_is_round_robin_across_recordings(self) -> None:
        prepared = [
            {
                "recording_id": f"recording_{recording}",
                "intervals": [
                    {"interval_id": f"interval_{recording}_{index}", "duration_ms": 30_000}
                    for index in range(3)
                ],
            }
            for recording in range(3)
        ]
        policy = dict(DEFAULT_POLICY)
        policy["max_total_intervals"] = 3
        policy["max_total_duration_ms"] = 90_000
        count, duration = _apply_global_caps(prepared, policy)
        self.assertEqual((count, duration), (3, 90_000))
        self.assertEqual([len(row["intervals"]) for row in prepared], [1, 1, 1])

    def test_v2_global_duration_cap_scans_past_an_oversized_head(self) -> None:
        policy = dict(DEFAULT_POLICY)
        policy["max_total_intervals"] = 2
        policy["max_total_duration_ms"] = 15_000
        intervals = [
            {"interval_id": "too_long", "duration_ms": 30_000},
            {"interval_id": "fits", "duration_ms": 10_000},
        ]
        legacy = [{"recording_id": "recording_1", "intervals": copy.deepcopy(intervals)}]
        grouped = [{"recording_id": "recording_1", "intervals": copy.deepcopy(intervals)}]
        self.assertEqual(_apply_global_caps(legacy, policy), (0, 0))
        self.assertEqual(_apply_global_caps_v2(grouped, policy), (1, 10_000))
        self.assertEqual(grouped[0]["intervals"][0]["interval_id"], "fits")

    def test_exact_catalog_rows_allow_only_registered_producer_lineage(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            fixture = Fixture(Path(directory))
            value = fixture._preprocess_value()
            source_uri = value["input"]["storage_uri"]
            verified_at = "2026-08-26T21:01:00Z"
            connection = sqlite3.connect(fixture.db_path)
            connection.execute(
                "INSERT INTO media_locations VALUES(?,?,?,?,?,?)",
                (
                    "mlc_catalog_local_fixture",
                    fixture.analysis_media_id,
                    source_uri,
                    "local_hot_cache",
                    verified_at,
                    1,
                ),
            )
            connection.commit()
            connection.close()

            producer_run_input_id = "run_input_" + hashlib.sha256(
                canonical_bytes(
                    [fixture.run_id, fixture.analysis_media_id, "source_media"]
                )
            ).hexdigest()[:32]
            producer_location_id = "media_location_" + hashlib.sha256(
                canonical_bytes([fixture.analysis_media_id, source_uri])
            ).hexdigest()[:32]
            value["catalog_records"] = {
                "processing_runs": [copy.deepcopy(value["processing_run"])],
                "run_inputs": [
                    {
                        "run_input_id": producer_run_input_id,
                        "processing_run_id": fixture.run_id,
                        "object_type": "media",
                        "object_id": fixture.analysis_media_id,
                        "input_role": "source_media",
                        "input_sha256": fixture.analysis_sha,
                    }
                ],
                "media_objects": [
                    {
                        "media_id": fixture.analysis_media_id,
                        "sha256": fixture.analysis_sha,
                        "byte_count": fixture.analysis_bytes,
                        "media_kind": "video",
                        "mime_type": "video/mp4",
                        "container": "mp4",
                        "duration_ms": fixture.analysis_duration,
                        "ffprobe_json": {},
                        "first_cataloged_at": "2026-08-26T20:00:00Z",
                        "integrity_state": "verified",
                    }
                ],
                "media_locations": [
                    {
                        "media_location_id": producer_location_id,
                        "media_id": fixture.analysis_media_id,
                        "storage_uri": source_uri,
                        "storage_class": "local",
                        "verified_at": verified_at,
                        "is_primary": 1,
                    }
                ],
                "media_derivations": [],
                "artifacts": [
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
                    for artifact in value["artifacts"]
                ],
            }
            write_sealed_json(fixture.result_path, value)
            request = emit_full_rendition_request(
                fixture.cohort,
                fixture.db_path,
                [fixture.result_path],
                "2026-08-26T22:00:00Z",
            )
            self.assertEqual(len(request["recordings"]), 1)

    def test_v1_relevant_rows_golden_omits_v2_import_batch_extension(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            fixture = Fixture(Path(directory))
            request = fixture.request()
            request_row = request["recordings"][0]
            value, _, _ = _read_pinned_json(
                request_row["preprocess_result_path"],
                request_row["preprocess_result_sha256"],
                "golden_v1.preprocess_result",
            )
            preprocess = _validate_preprocess_result(
                value, request_row, "golden_v1.preprocess_result"
            )
            with _Catalog(fixture.db_path) as catalog:
                binding = _catalog_binding(
                    catalog, request_row, preprocess, "golden_v1"
                )
            self.assertNotIn("preprocess_import_batch", binding["relevant_rows"])
            self.assertEqual(
                binding["relevant_rows_sha256"],
                "fcbf372b3a0581d1eef60ac7ecb0eb197d5e99a7c56b9470d9d75129f9ec8c2b",
            )
            proposal = prepare_interval_proposal(
                request, fixture.cohort, fixture.db_path
            )
            self.assertEqual(
                proposal["catalog_basis"]["relevant_rows_sha256"],
                "6ee62bcde6e42b93513001d0495b6167ccefb26d6109a69edcee79db6200d154",
            )
            self.assertEqual(
                proposal["recordings"][0]["preprocess_binding"][
                    "catalog_rows_sha256"
                ],
                binding["relevant_rows_sha256"],
            )

        with tempfile.TemporaryDirectory() as directory:
            fixture = AdmittedMultiWindowFixture(Path(directory))
            request = fixture.emit()
            parent = request["recordings"][0]
            window = parent["windows"][0]
            request_row = {
                **_window_request_adapter(parent, window),
            }
            value, _, _ = _read_pinned_json(
                window["preprocess_result_path"],
                window["preprocess_result_raw_sha256"],
                "v2_batch.preprocess_result",
            )
            preprocess = _validate_preprocess_result(
                value, request_row, "v2_batch.preprocess_result"
            )
            with _Catalog(fixture.db_path) as catalog:
                binding = _catalog_binding(
                    catalog,
                    request_row,
                    preprocess,
                    "v2_batch",
                    import_envelope_sha256=window[
                        "preprocess_import_envelope_sha256"
                    ],
                    expected_import_batch_id=window["preprocess_import_batch_id"],
                )
            self.assertEqual(
                binding["relevant_rows"]["preprocess_import_batch"][
                    "import_batch_id"
                ],
                window["preprocess_import_batch_id"],
            )

    def test_deterministic_unreviewed_nonoverlapping_and_schema_valid(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            fixture = Fixture(Path(directory))
            before = hashlib.sha256(fixture.db_path.read_bytes()).hexdigest()
            request = fixture.request()
            request["policy"]["max_intervals_per_recording"] = 2
            request["policy"]["max_total_intervals"] = 2
            request["policy"]["max_total_duration_ms"] = 60_000
            reseal_request(request)
            first = prepare_interval_proposal(request, fixture.cohort, fixture.db_path)
            second = prepare_interval_proposal(request, fixture.cohort, fixture.db_path)
            after = hashlib.sha256(fixture.db_path.read_bytes()).hexdigest()
            self.assertEqual(first, second)
            self.assertEqual(before, after)
            self.assertEqual(first["proposal_state"], "proposal_unreviewed")
            self.assertFalse(first["safety"]["asr_output_inspected"])
            self.assertFalse(first["safety"]["reference_quality_claimed"])
            self.assertFalse(first["safety"]["freeze_created"])
            intervals = first["recordings"][0]["intervals"]
            self.assertEqual(len(intervals), 2)
            self.assertEqual(first["accounting"]["interval_count"], 2)
            self.assertEqual(first["accounting"]["total_duration_ms"], 60_000)
            for left, right in zip(intervals, intervals[1:]):
                self.assertLessEqual(
                    left["end_ms"] + request["policy"]["minimum_gap_ms"],
                    right["start_ms"],
                )
            for interval in intervals:
                reviewer = interval["reviewer_completion"]
                self.assertIsNone(reviewer["include"])
                self.assertIsNone(reviewer["split"])
                self.assertIsNone(reviewer["stratum_id"])
                self.assertTrue(all(value is None for value in reviewer["flags"].values()))
            self._schema_validate(REQUEST_SCHEMA, request, Path(directory) / "request.json")
            self._schema_validate(PROPOSAL_SCHEMA, first, Path(directory) / "proposal.json")

    def test_refuses_transcript_path_and_asr_stage(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            fixture = Fixture(root)
            forbidden = root / "transcript-result.json"
            write_sealed_bytes(forbidden, fixture.result_path.read_bytes())
            with self.assertRaisesRegex(ContractError, "ASR/transcript/reference-shaped"):
                emit_full_rendition_request(
                    fixture.cohort,
                    fixture.db_path,
                    [forbidden],
                    "2026-08-26T22:00:00Z",
                )
            transcription = root / "transcription-output.json"
            write_sealed_bytes(transcription, fixture.result_path.read_bytes())
            with self.assertRaisesRegex(ContractError, "ASR/transcript/reference-shaped"):
                emit_full_rendition_request(
                    fixture.cohort,
                    fixture.db_path,
                    [transcription],
                    "2026-08-26T22:00:00Z",
                )
            value = fixture._preprocess_value()
            value["processing_run"]["stage"] = "asr_whispercpp"
            neutral = root / "machine-result.json"
            write_sealed_json(neutral, value)
            with self.assertRaisesRegex(ContractError, "never ASR/transcript"):
                emit_full_rendition_request(
                    fixture.cohort,
                    fixture.db_path,
                    [neutral],
                    "2026-08-26T22:00:00Z",
                )
            symlink = root / "preprocess-link.json"
            symlink.symlink_to(fixture.result_path)
            with self.assertRaisesRegex(ContractError, "symbolic link"):
                emit_full_rendition_request(
                    fixture.cohort,
                    fixture.db_path,
                    [symlink],
                    "2026-08-26T22:00:00Z",
                )

    def test_writable_preprocess_and_local_json_inputs_fail_closed(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            fixture = Fixture(Path(directory))
            request = fixture.request()
            result_sha256 = hashlib.sha256(fixture.result_path.read_bytes()).hexdigest()
            fixture.result_path.chmod(0o600)
            with self.assertRaisesRegex(ContractError, "sealed read-only"):
                prepare_interval_proposal(request, fixture.cohort, fixture.db_path)
            fixture.result_path.chmod(0o444)
            self.assertEqual(
                hashlib.sha256(fixture.result_path.read_bytes()).hexdigest(),
                result_sha256,
            )
            proposal = prepare_interval_proposal(
                request, fixture.cohort, fixture.db_path
            )
            self.assertEqual(proposal["schema_version"], 1)

        with tempfile.TemporaryDirectory() as directory:
            fixture = AdmittedMultiWindowFixture(Path(directory))
            fixture.first.local_path.chmod(0o600)
            with self.assertRaisesRegex(ContractError, "sealed read-only"):
                fixture.emit()

        with tempfile.TemporaryDirectory() as directory:
            fixture = AdmittedMultiWindowFixture(Path(directory))
            fixture.first.result_path.chmod(0o600)
            with self.assertRaisesRegex(ContractError, "sealed read-only"):
                fixture.emit()

    def test_hash_tamper_and_quality_claim_fail_closed(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            fixture = Fixture(Path(directory))
            request = fixture.request()
            request["recordings"][0]["preprocess_result_sha256"] = "0" * 64
            reseal_request(request)
            with self.assertRaisesRegex(ContractError, "file digest mismatch"):
                prepare_interval_proposal(request, fixture.cohort, fixture.db_path)

            request = fixture.request()
            proposal = prepare_interval_proposal(request, fixture.cohort, fixture.db_path)
            tampered = copy.deepcopy(proposal)
            tampered["safety"]["reference_quality_claimed"] = True
            tampered["manifest_sha256"] = canonical_manifest_sha256(tampered)
            with self.assertRaisesRegex(ContractError, "deterministic regeneration"):
                validate_interval_proposal(
                    tampered, request, fixture.cohort, fixture.db_path
                )

    def test_routing_must_equal_registered_hash_pinned_artifact(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            fixture = Fixture(Path(directory))
            value = fixture._preprocess_value()
            value["routing"]["scene_changes"] = []
            value["routing"]["summary"]["scene_change_count"] = 0
            write_sealed_json(fixture.result_path, value)
            with self.assertRaisesRegex(ContractError, "exactly equal.*routing artifact"):
                emit_full_rendition_request(
                    fixture.cohort,
                    fixture.db_path,
                    [fixture.result_path],
                    "2026-08-26T22:00:00Z",
                )

            value = fixture._preprocess_value()
            value["catalog_records"]["transcript_segments"] = [
                {"text": "must never enter proposal routing"}
            ]
            write_sealed_json(fixture.result_path, value)
            with self.assertRaisesRegex(ContractError, "forbidden transcript/ASR field"):
                emit_full_rendition_request(
                    fixture.cohort,
                    fixture.db_path,
                    [fixture.result_path],
                    "2026-08-26T22:00:00Z",
                )

            value = fixture._preprocess_value()
            value["catalog_records"]["media_objects"].append(
                {"description": "verbatim transcript content under a neutral key"}
            )
            write_sealed_json(fixture.result_path, value)
            with self.assertRaisesRegex(
                ContractError, r"catalog_records\.media_objects\[0\].*unexpected"
            ):
                emit_full_rendition_request(
                    fixture.cohort,
                    fixture.db_path,
                    [fixture.result_path],
                    "2026-08-26T22:00:00Z",
                )

            value = fixture._preprocess_value()
            value["catalog_records"]["media_objects"].append(
                {
                    "media_id": fixture.analysis_media_id,
                    "sha256": fixture.analysis_sha,
                    "byte_count": fixture.analysis_bytes,
                    "media_kind": "video",
                    "mime_type": "video/mp4",
                    "container": "mp4",
                    "duration_ms": fixture.analysis_duration,
                    "ffprobe_json": {
                        "note": "verbatim transcript content under an allowed neutral key"
                    },
                    "first_cataloged_at": "2026-08-26T20:00:00Z",
                    "integrity_state": "verified",
                }
            )
            write_sealed_json(fixture.result_path, value)
            with self.assertRaisesRegex(
                ContractError, "must exactly equal the hash-pinned probe artifact"
            ):
                emit_full_rendition_request(
                    fixture.cohort,
                    fixture.db_path,
                    [fixture.result_path],
                    "2026-08-26T22:00:00Z",
                )

    def test_recipe_run_and_input_catalog_lineage_fail_closed(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            fixture = Fixture(Path(directory))
            value = fixture._preprocess_value()
            value["layout"]["recipe_sha256"] = "0" * 64
            write_sealed_json(fixture.result_path, value)
            with self.assertRaisesRegex(ContractError, "parameters_json SHA-256"):
                emit_full_rendition_request(
                    fixture.cohort,
                    fixture.db_path,
                    [fixture.result_path],
                    "2026-08-26T22:00:00Z",
                )

            fixture._write_preprocess_result()
            connection = sqlite3.connect(fixture.db_path)
            connection.execute(
                "INSERT INTO run_inputs VALUES(?,?,?,?,?,?)",
                (
                    "run_input_forbidden_extra",
                    fixture.run_id,
                    "transcript_revision",
                    "revision_forbidden",
                    "asr_output",
                    "9" * 64,
                ),
            )
            connection.commit()
            connection.close()
            with self.assertRaisesRegex(ContractError, "exactly one input"):
                emit_full_rendition_request(
                    fixture.cohort,
                    fixture.db_path,
                    [fixture.result_path],
                    "2026-08-26T22:00:00Z",
                )

    def test_request_emitter_refuses_unregistered_preprocess_run(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            fixture = Fixture(Path(directory))
            value = fixture._preprocess_value()
            missing_run = "run_preprocess_" + "9" * 32
            value["processing_run"]["processing_run_id"] = missing_run
            for artifact in value["artifacts"]:
                artifact["processing_run_id"] = missing_run
            write_sealed_json(fixture.result_path, value)
            with self.assertRaisesRegex(ContractError, "expected exactly one row, found 0"):
                emit_full_rendition_request(
                    fixture.cohort,
                    fixture.db_path,
                    [fixture.result_path],
                    "2026-08-26T22:00:00Z",
                )

    def test_local_window_maps_exact_parent_offsets(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            fixture = Fixture(Path(directory), local_window=True)
            request = fixture.request()
            proposal = prepare_interval_proposal(request, fixture.cohort, fixture.db_path)
            self._schema_validate(
                LOCAL_WINDOW_RESULT_SCHEMA,
                load_json(fixture.local_path),
                Path(directory) / "local-window-schema-copy.json",
            )
            self._schema_validate(
                REQUEST_SCHEMA, request, Path(directory) / "local-request.json"
            )
            self._schema_validate(
                PROPOSAL_SCHEMA, proposal, Path(directory) / "local-proposal.json"
            )
            recording = proposal["recordings"][0]
            self.assertEqual(recording["timeline"]["source_offset_ms"], fixture.offset)
            self.assertIsNotNone(recording["timeline"]["local_window_binding"])
            for interval in recording["intervals"]:
                self.assertEqual(
                    interval["start_ms"], fixture.offset + interval["local_start_ms"]
                )
                self.assertEqual(
                    interval["end_ms"], fixture.offset + interval["local_end_ms"]
                )
                self.assertLessEqual(interval["end_ms"], fixture.source_end)

    def test_multi_local_window_request_and_proposal_are_grouped_deterministic_and_read_only(
        self,
    ) -> None:
        with tempfile.TemporaryDirectory() as directory:
            fixture = AdmittedMultiWindowFixture(Path(directory))
            policy = dict(DEFAULT_POLICY)
            policy["max_intervals_per_recording"] = 2
            policy["max_total_intervals"] = 2
            policy["max_total_duration_ms"] = 60_000
            before = hashlib.sha256(fixture.db_path.read_bytes()).hexdigest()
            first_request = fixture.emit(policy=policy)
            second_request = fixture.emit(policy=policy)
            first = prepare_interval_proposal(
                first_request, fixture.cohort, fixture.db_path
            )
            second = prepare_interval_proposal(
                first_request, fixture.cohort, fixture.db_path
            )
            after = hashlib.sha256(fixture.db_path.read_bytes()).hexdigest()
            self.assertEqual(before, after)
            self.assertEqual(first_request, second_request)
            self.assertEqual(first, second)
            self.assertEqual(first_request["schema_version"], 2)
            self.assertEqual(len(first_request["recordings"]), 1)
            self.assertEqual(len(first_request["recordings"][0]["windows"]), 2)
            self.assertEqual(first["accounting"]["window_count"], 2)
            self.assertEqual(first["accounting"]["interval_count"], 2)
            recording = first["recordings"][0]
            self.assertEqual(len(recording["windows"]), 2)
            self.assertEqual(len(recording["intervals"]), 2)
            request_windows = {
                row["window_id"]: row
                for row in first_request["recordings"][0]["windows"]
            }
            for interval in recording["intervals"]:
                window = request_windows[interval["window_id"]]
                self.assertEqual(
                    interval["parent_start_ms"],
                    window["source_offset_ms"] + interval["local_start_ms"],
                )
                self.assertEqual(
                    interval["parent_end_ms"],
                    window["source_offset_ms"] + interval["local_end_ms"],
                )
                self.assertEqual(interval["start_ms"], interval["parent_start_ms"])
                self.assertEqual(interval["end_ms"], interval["parent_end_ms"])
                self.assertEqual(interval["status"], "proposal_unreviewed")
            for left, right in zip(recording["intervals"], recording["intervals"][1:]):
                self.assertLessEqual(
                    left["parent_end_ms"] + policy["minimum_gap_ms"],
                    right["parent_start_ms"],
                )
            self._schema_validate(
                REQUEST_V2_SCHEMA,
                first_request,
                Path(directory) / "multi-request.json",
            )
            self._schema_validate(
                PROPOSAL_V2_SCHEMA,
                first,
                Path(directory) / "multi-proposal.json",
            )
            self.assertEqual(
                validate_interval_proposal(
                    first, first_request, fixture.cohort, fixture.db_path
                ),
                first,
            )

            capped_policy = dict(policy)
            capped_policy["max_intervals_per_recording"] = 1
            capped_request = fixture.emit(policy=capped_policy)
            capped = prepare_interval_proposal(
                capped_request, fixture.cohort, fixture.db_path
            )
            self.assertEqual(capped["accounting"]["interval_count"], 1)
            self.assertEqual(len(capped["recordings"][0]["intervals"]), 1)

    def test_multi_local_window_cli_emits_v2_without_output_or_catalog_mutation(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            fixture = AdmittedMultiWindowFixture(Path(directory))
            before = hashlib.sha256(fixture.db_path.read_bytes()).hexdigest()
            command = [
                sys.executable,
                "-m",
                "evaluation",
                "emit-local-window-proposal-request",
                "--cohort",
                str(COHORT_PATH),
                "--catalog",
                str(fixture.db_path),
                "--created-at",
                "2026-08-26T22:00:00Z",
            ]
            for window in fixture.windows:
                command.extend(["--local-window-result", str(window.local_path)])
            for window in fixture.windows:
                command.extend(["--preprocess-result", str(window.result_path)])
            completed = subprocess.run(
                command,
                cwd=ROOT,
                text=True,
                capture_output=True,
                check=False,
            )
            after = hashlib.sha256(fixture.db_path.read_bytes()).hexdigest()
            self.assertEqual(completed.returncode, 0, completed.stderr)
            self.assertEqual(before, after)
            emitted = json.loads(completed.stdout)
            self.assertEqual(emitted["schema_version"], 2)
            self.assertEqual(len(emitted["recordings"][0]["windows"]), 2)

            rejected = subprocess.run(
                [*command, "--output", str(Path(directory) / "forbidden.json")],
                cwd=ROOT,
                text=True,
                capture_output=True,
                check=False,
            )
            self.assertNotEqual(rejected.returncode, 0)
            self.assertIn("unrecognized arguments", rejected.stderr)

    def test_multi_local_window_accepts_percent_encoded_file_uris(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory) / "directory with spaces"
            root.mkdir()
            fixture = AdmittedMultiWindowFixture(root)
            request = fixture.emit()
            proposal = prepare_interval_proposal(
                request, fixture.cohort, fixture.db_path
            )
            self.assertEqual(request["schema_version"], 2)
            self.assertGreater(proposal["accounting"]["interval_count"], 0)
            preprocess = load_json(fixture.first.result_path)
            self.assertIn("%20", preprocess["input"]["storage_uri"])

    def test_multi_local_window_uses_actual_importer_canonical_digest_semantics(
        self,
    ) -> None:
        corpus_src = ROOT / "corpus/src"
        sys.path.insert(0, str(corpus_src))
        try:
            from himr_corpus.result_importers import _result_file
        finally:
            sys.path.remove(str(corpus_src))

        with tempfile.TemporaryDirectory() as directory:
            fixture = AdmittedMultiWindowFixture(Path(directory))
            raw_sha = hashlib.sha256(
                fixture.first.result_path.read_bytes()
            ).hexdigest()
            parsed, importer_sha, importer_raw_sha = _result_file(
                fixture.first.result_path
            )
            expected_import_sha = hashlib.sha256(canonical_bytes(parsed)).hexdigest()
            self.assertEqual(importer_sha, expected_import_sha)
            self.assertEqual(importer_raw_sha, raw_sha)
            self.assertNotEqual(raw_sha, importer_sha)

            request = fixture.emit()
            window = request["recordings"][0]["windows"][0]
            self.assertNotIn("preprocess_result_sha256", window)
            self.assertEqual(window["preprocess_result_raw_sha256"], raw_sha)
            self.assertEqual(
                window["preprocess_import_envelope_sha256"], importer_sha
            )
            self.assertEqual(
                window["preprocess_import_batch_id"],
                _stable_id("imp", "media_preprocess_result_v1", importer_sha),
            )
            proposal = prepare_interval_proposal(
                request, fixture.cohort, fixture.db_path
            )
            binding = proposal["recordings"][0]["windows"][0][
                "preprocess_binding"
            ]
            self.assertNotIn("result_sha256", binding)
            self.assertEqual(binding["result_raw_sha256"], raw_sha)
            self.assertEqual(binding["import_envelope_sha256"], importer_sha)
            self.assertEqual(
                binding["import_batch_id"], window["preprocess_import_batch_id"]
            )

            write_sealed_bytes(
                fixture.first.result_path,
                fixture.first.result_path.read_bytes() + b"\n"
            )
            with self.assertRaisesRegex(ContractError, "file digest mismatch"):
                prepare_interval_proposal(request, fixture.cohort, fixture.db_path)
            reformatted_request = fixture.emit()
            reformatted_window = reformatted_request["recordings"][0]["windows"][0]
            self.assertNotEqual(
                reformatted_window["preprocess_result_raw_sha256"], raw_sha
            )
            self.assertEqual(
                reformatted_window["preprocess_import_envelope_sha256"], importer_sha
            )
            self.assertEqual(
                reformatted_window["preprocess_import_batch_id"],
                window["preprocess_import_batch_id"],
            )

    def test_multi_local_window_rejects_ambiguous_window_identity(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            fixture = AdmittedMultiWindowFixture(Path(directory))
            request = fixture.emit()
            first, second = request["recordings"][0]["windows"]
            second["window_id"] = first["window_id"]
            second["window_ordinal"] = first["window_ordinal"]
            second["analysis_rendition_kind"] = ":".join(
                [
                    *second["analysis_rendition_kind"].split(":")[:-1],
                    first["window_id"],
                ]
            )
            second["analysis_rendition_id"] = _expected_rendition_id(
                request["recordings"][0]["recording_id"],
                second["analysis_media_id"],
                second["analysis_rendition_kind"],
            )
            reseal_request(request)
            with self.assertRaisesRegex(ContractError, r"window_id.*unique"):
                validate_interval_proposal_request(request, fixture.cohort)

    def test_multi_local_window_binds_exact_preprocess_import_and_rejects_hidden_paths(
        self,
    ) -> None:
        with tempfile.TemporaryDirectory() as directory:
            fixture = AdmittedMultiWindowFixture(Path(directory))
            request = fixture.emit()
            preprocess = load_json(fixture.first.result_path)
            preprocess["steps"][0]["command"] = [
                "/usr/bin/fixture",
                "--input",
                "/private/transcript.json",
            ]
            write_sealed_json(fixture.first.result_path, preprocess)
            with self.assertRaisesRegex(ContractError, "ASR/transcript/reference-shaped"):
                fixture.emit()
            request["recordings"][0]["windows"][0][
                "preprocess_result_raw_sha256"
            ] = hashlib.sha256(fixture.first.result_path.read_bytes()).hexdigest()
            import_sha = hashlib.sha256(canonical_bytes(preprocess)).hexdigest()
            request["recordings"][0]["windows"][0][
                "preprocess_import_envelope_sha256"
            ] = import_sha
            request["recordings"][0]["windows"][0][
                "preprocess_import_batch_id"
            ] = _stable_id("imp", "media_preprocess_result_v1", import_sha)
            reseal_request(request)
            with self.assertRaisesRegex(ContractError, "ASR/transcript/reference-shaped"):
                prepare_interval_proposal(request, fixture.cohort, fixture.db_path)

        with tempfile.TemporaryDirectory() as directory:
            fixture = AdmittedMultiWindowFixture(Path(directory))
            request = fixture.emit()
            preprocess = load_json(fixture.first.result_path)
            preprocess["steps"][0]["command"][1] = "probe-read-only-v2"
            write_sealed_json(fixture.first.result_path, preprocess)
            request["recordings"][0]["windows"][0][
                "preprocess_result_raw_sha256"
            ] = hashlib.sha256(fixture.first.result_path.read_bytes()).hexdigest()
            import_sha = hashlib.sha256(canonical_bytes(preprocess)).hexdigest()
            request["recordings"][0]["windows"][0][
                "preprocess_import_envelope_sha256"
            ] = import_sha
            request["recordings"][0]["windows"][0][
                "preprocess_import_batch_id"
            ] = _stable_id("imp", "media_preprocess_result_v1", import_sha)
            reseal_request(request)
            with self.assertRaisesRegex(ContractError, r"import_batch.*found 0"):
                prepare_interval_proposal(request, fixture.cohort, fixture.db_path)

        with tempfile.TemporaryDirectory() as directory:
            fixture = AdmittedMultiWindowFixture(Path(directory))
            local = load_json(fixture.first.local_path)
            local["commands"][0].extend(
                ["--reference-input", "/private/reference-transcript.json"]
            )
            write_sealed_json(fixture.first.local_path, local)
            with self.assertRaisesRegex(ContractError, "ASR/transcript/reference-shaped"):
                fixture.emit()

    def test_multi_local_window_requires_current_unmerged_recording(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            fixture = AdmittedMultiWindowFixture(Path(directory))
            connection = sqlite3.connect(fixture.db_path)
            connection.execute(
                "UPDATE recordings SET review_state='rejected' WHERE recording_id=?",
                (fixture.first.candidate["recording_id"],),
            )
            connection.commit()
            connection.close()
            with self.assertRaisesRegex(ContractError, "no longer an admissible unmerged"):
                fixture.emit()

    def test_multi_local_window_admission_context_may_use_archive_source_mapping(
        self,
    ) -> None:
        with tempfile.TemporaryDirectory() as directory:
            fixture = AdmittedMultiWindowFixture(Path(directory))
            archive_source_id = "src_" + "9" * 32
            archive_mapping_id = "rso_" + "9" * 32
            archive_mapping = {
                "recording_source_id": archive_mapping_id,
                "mapping_role": "archive_original_file",
                "source_start_ms": None,
                "source_end_ms": None,
                "recording_start_ms": None,
                "recording_end_ms": None,
                "mapping_method": "exact",
                "confidence_state": "metadata_only",
            }
            connection = sqlite3.connect(fixture.db_path)
            connection.execute(
                "INSERT INTO sources VALUES(?,?,?,?,?,?)",
                (
                    archive_source_id,
                    "internet_archive",
                    "archive_file",
                    "archive-fixture",
                    "https://archive.org/download/archive-fixture/source.mp4",
                    "public",
                ),
            )
            connection.execute(
                "INSERT INTO recording_sources VALUES(?,?,?,?,?,?,?,?,?,?)",
                (
                    archive_mapping_id,
                    fixture.first.candidate["recording_id"],
                    archive_source_id,
                    archive_mapping["mapping_role"],
                    None,
                    None,
                    None,
                    None,
                    archive_mapping["mapping_method"],
                    archive_mapping["confidence_state"],
                ),
            )
            row = connection.execute(
                "SELECT parameters_json FROM processing_runs WHERE processing_run_id=?",
                (fixture.first.admission_run_id,),
            ).fetchone()
            parameters = json.loads(row[0])
            parameters["catalog_context_basis"][0]["source_mappings"] = [
                archive_mapping
            ]
            connection.execute(
                "UPDATE processing_runs SET parameters_json=? WHERE processing_run_id=?",
                (
                    json.dumps(parameters, sort_keys=True, separators=(",", ":")),
                    fixture.first.admission_run_id,
                ),
            )
            connection.commit()
            connection.close()

            request = fixture.emit()
            proposal = prepare_interval_proposal(
                request, fixture.cohort, fixture.db_path
            )
            self.assertEqual(request["schema_version"], 2)
            self.assertGreater(proposal["accounting"]["interval_count"], 0)

    def test_multi_local_proxy_preprocess_accepts_admission_preserved_catalog_values(
        self,
    ) -> None:
        with tempfile.TemporaryDirectory() as directory:
            fixture = AdmittedMultiWindowFixture(Path(directory))
            local = load_json(fixture.first.local_path)
            proxy = next(
                artifact
                for artifact in local["artifacts"]
                if artifact["artifact_kind"]
                == "window_low_resolution_cfr_proxy"
            )
            proxy_media_id = f"media_sha256_{proxy['sha256']}"
            proxy_path = Path(proxy["path"])
            proxy_uri = proxy_path.as_uri()
            preprocess = load_json(fixture.first.result_path)
            preprocess["input"].update(
                {
                    "media_id": proxy_media_id,
                    "sha256": proxy["sha256"],
                    "byte_count": proxy["byte_count"],
                    "path": str(proxy_path),
                    "storage_uri": proxy_uri,
                }
            )
            for stat_name in ("stat_before", "stat_after"):
                preprocess["input"][stat_name]["byte_count"] = proxy["byte_count"]
            preprocess["routing"]["source_media_id"] = proxy_media_id
            routing_body = canonical_bytes(preprocess["routing"])
            write_sealed_bytes(fixture.first.routing_path, routing_body)
            routing_sha = hashlib.sha256(routing_body).hexdigest()
            routing_descriptor = next(
                artifact
                for artifact in preprocess["artifacts"]
                if artifact["artifact_kind"] == "scene_silence_routing_json"
            )
            routing_descriptor["sha256"] = routing_sha
            routing_descriptor["byte_count"] = len(routing_body)
            connection = sqlite3.connect(fixture.db_path)
            media = connection.execute(
                "SELECT media_kind,mime_type,duration_ms,first_cataloged_at "
                "FROM media_objects WHERE media_id=?",
                (proxy_media_id,),
            ).fetchone()
            location_id = "media_location_" + hashlib.sha256(
                canonical_bytes([proxy_media_id, proxy_uri])
            ).hexdigest()[:32]
            preprocess["catalog_records"]["media_objects"] = [
                {
                    "media_id": proxy_media_id,
                    "sha256": proxy["sha256"],
                    "byte_count": proxy["byte_count"],
                    "media_kind": media[0],
                    "mime_type": media[1],
                    "container": "mov,mp4,m4a,3gp,3g2,mj2",
                    "duration_ms": media[2],
                    "ffprobe_json": {},
                    "first_cataloged_at": media[3],
                    "integrity_state": "verified",
                }
            ]
            preprocess["catalog_records"]["media_locations"] = [
                {
                    "media_location_id": location_id,
                    "media_id": proxy_media_id,
                    "storage_uri": proxy_uri,
                    "storage_class": "local",
                    "verified_at": "2026-08-26T21:30:00Z",
                    "is_primary": 1,
                }
            ]
            connection.execute(
                "UPDATE run_inputs SET object_id=?,input_sha256=? "
                "WHERE processing_run_id=?",
                (proxy_media_id, proxy["sha256"], fixture.first.run_id),
            )
            connection.execute(
                "UPDATE artifacts SET sha256=?,byte_count=? WHERE artifact_id=?",
                (routing_sha, len(routing_body), fixture.first.routing_artifact_id),
            )
            connection.execute(
                "INSERT INTO media_locations VALUES(?,?,?,?,?,?)",
                (
                    "media_location_admission_fixture",
                    proxy_media_id,
                    proxy_uri,
                    "private_local",
                    "2026-08-26T21:30:00Z",
                    1,
                ),
            )
            connection.commit()
            connection.close()
            fixture._replace_admitted_preprocess_result(fixture.first, preprocess)

            request = fixture.emit()
            first_window = request["recordings"][0]["windows"][0]
            self.assertEqual(first_window["analysis_media_id"], proxy_media_id)
            self.assertIn(
                "window_low_resolution_cfr_proxy",
                first_window["analysis_rendition_kind"],
            )
            proposal = prepare_interval_proposal(
                request, fixture.cohort, fixture.db_path
            )
            self.assertGreater(proposal["accounting"]["interval_count"], 0)

    def test_multi_local_window_duplicates_overlaps_and_pair_mismatches_fail_closed(
        self,
    ) -> None:
        with tempfile.TemporaryDirectory() as directory:
            fixture = AdmittedMultiWindowFixture(Path(directory))
            with self.assertRaisesRegex(ContractError, "duplicates a parent source window"):
                emit_local_window_proposal_request(
                    fixture.cohort,
                    fixture.db_path,
                    [fixture.first.local_path, fixture.first.local_path],
                    [fixture.first.result_path, fixture.first.result_path],
                    "2026-08-26T22:00:00Z",
                )
            with self.assertRaisesRegex(ContractError, "exactly one sealed local-window artifact"):
                emit_local_window_proposal_request(
                    fixture.cohort,
                    fixture.db_path,
                    [window.local_path for window in fixture.windows],
                    [fixture.second.result_path, fixture.first.result_path],
                    "2026-08-26T22:00:00Z",
                )

        with tempfile.TemporaryDirectory() as directory:
            overlapping = AdmittedMultiWindowFixture(
                Path(directory), second_offset=20_000
            )
            with self.assertRaisesRegex(ContractError, "sorted and nonoverlapping"):
                overlapping.emit()

        with tempfile.TemporaryDirectory() as directory:
            close_windows = AdmittedMultiWindowFixture(
                Path(directory), second_offset=32_000
            )
            request = close_windows.emit()
            proposal = prepare_interval_proposal(
                request, close_windows.cohort, close_windows.db_path
            )
            self.assertEqual(
                proposal["accounting"]["interval_count"],
                1,
                "minimum_gap_ms must apply across derivative windows of one parent",
            )

    def test_multi_local_window_boundary_routing_shapes_and_catalog_lineage_fail_closed(
        self,
    ) -> None:
        with tempfile.TemporaryDirectory() as directory:
            fixture = AdmittedMultiWindowFixture(Path(directory))
            local = load_json(fixture.first.local_path)
            local["time_mapping"]["artifact_zero_maps_to_source_ms"] += 1
            write_sealed_json(fixture.first.local_path, local)
            with self.assertRaisesRegex(ContractError, "does not preserve exact source offset"):
                fixture.emit()

        with tempfile.TemporaryDirectory() as directory:
            fixture = AdmittedMultiWindowFixture(Path(directory))
            local = load_json(fixture.first.local_path)
            local["unexpected_notes"] = "neutral-shaped extra data"
            write_sealed_json(fixture.first.local_path, local)
            with self.assertRaisesRegex(ContractError, "unexpected"):
                fixture.emit()

        with tempfile.TemporaryDirectory() as directory:
            fixture = AdmittedMultiWindowFixture(Path(directory))
            local = load_json(fixture.first.local_path)
            local["artifacts"][0]["unexpected_payload"] = "not admitted"
            write_sealed_json(fixture.first.local_path, local)
            with self.assertRaisesRegex(ContractError, "unexpected"):
                fixture.emit()

        with tempfile.TemporaryDirectory() as directory:
            fixture = AdmittedMultiWindowFixture(Path(directory))
            preprocess = load_json(fixture.first.result_path)
            preprocess["routing"]["scene_changes"] = []
            preprocess["routing"]["summary"]["scene_change_count"] = 0
            write_sealed_json(fixture.first.result_path, preprocess)
            with self.assertRaisesRegex(ContractError, "exactly equal.*routing artifact"):
                fixture.emit()

        with tempfile.TemporaryDirectory() as directory:
            fixture = AdmittedMultiWindowFixture(Path(directory))
            preprocess = load_json(fixture.first.result_path)
            preprocess["asr_output"] = {"segments": []}
            write_sealed_json(fixture.first.result_path, preprocess)
            with self.assertRaisesRegex(ContractError, "forbidden.*ASR field"):
                fixture.emit()

        with tempfile.TemporaryDirectory() as directory:
            fixture = AdmittedMultiWindowFixture(Path(directory))
            connection = sqlite3.connect(fixture.db_path)
            row = connection.execute(
                "SELECT metadata_json FROM media_derivations WHERE child_media_id=?",
                (fixture.first.analysis_media_id,),
            ).fetchone()
            metadata = json.loads(row[0])
            metadata["source_media_id"] = "media_sha256_" + "9" * 64
            connection.execute(
                "UPDATE media_derivations SET metadata_json=? WHERE child_media_id=?",
                (
                    json.dumps(metadata, sort_keys=True, separators=(",", ":")),
                    fixture.first.analysis_media_id,
                ),
            )
            connection.commit()
            connection.close()
            with self.assertRaisesRegex(ContractError, "does not exactly match sealed"):
                fixture.emit()

        with tempfile.TemporaryDirectory() as directory:
            fixture = AdmittedMultiWindowFixture(Path(directory))
            connection = sqlite3.connect(fixture.db_path)
            connection.execute(
                "INSERT INTO artifacts VALUES(?,?,?,?,?,?,?,?,?)",
                (
                    "artifact_forbidden_extra",
                    fixture.first.admission_run_id,
                    "asr_output",
                    "file:///private/forbidden.json",
                    "7" * 64,
                    1,
                    1,
                    "private",
                    "{}",
                ),
            )
            connection.commit()
            connection.close()
            with self.assertRaisesRegex(ContractError, "artifact set differs"):
                fixture.emit()

    def test_multi_local_window_request_source_offset_skew_fails_on_sealed_replay(
        self,
    ) -> None:
        with tempfile.TemporaryDirectory() as directory:
            fixture = AdmittedMultiWindowFixture(Path(directory))
            request = fixture.emit()
            request["recordings"][0]["windows"][0]["source_offset_ms"] += 1
            reseal_request(request)
            validate_interval_proposal_request(request, fixture.cohort)
            with self.assertRaisesRegex(ContractError, "requested local source window"):
                prepare_interval_proposal(request, fixture.cohort, fixture.db_path)

    def test_cli_has_no_output_option(self) -> None:
        completed = subprocess.run(
            [
                sys.executable,
                "-m",
                "evaluation",
                "propose-intervals",
                "--cohort",
                str(COHORT_PATH),
                "--catalog",
                "/tmp/not-opened.sqlite3",
                "/tmp/not-opened-request.json",
                "--output",
                "/tmp/forbidden.json",
            ],
            cwd=ROOT,
            text=True,
            capture_output=True,
            check=False,
        )
        self.assertNotEqual(completed.returncode, 0)
        self.assertIn("unrecognized arguments", completed.stderr)

    def _schema_validate(self, schema: Path, value: dict, path: Path) -> None:
        path.write_text(json.dumps(value), encoding="utf-8")
        completed = subprocess.run(
            [
                sys.executable,
                "scripts/validate-json-contracts.py",
                "--validate",
                str(schema),
                str(path),
            ],
            cwd=ROOT,
            text=True,
            capture_output=True,
            check=False,
        )
        self.assertEqual(completed.returncode, 0, completed.stderr)


if __name__ == "__main__":
    unittest.main()
