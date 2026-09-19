from __future__ import annotations

import hashlib
import json
import os
import shutil
import subprocess
import unittest
from pathlib import Path


REPOSITORY_ROOT = Path(__file__).resolve().parents[2]
PIPELINE_ROOT = REPOSITORY_ROOT / "pipeline"
PROGRAM = PIPELINE_ROOT / "speaker_activity_router.py"
WORK_SCHEMA = PIPELINE_ROOT / "schemas" / "speaker-activity-routing-work-order.schema.json"
RESULT_SCHEMA = PIPELINE_ROOT / "schemas" / "speaker-activity-routing-result.schema.json"
HINT_SCHEMA = PIPELINE_ROOT / "schemas" / "speaker-reviewed-hints.schema.json"
VALIDATOR = REPOSITORY_ROOT / "scripts" / "validate-json-contracts.py"
TEST_ROOT = PIPELINE_ROOT / ".test-speaker-routing"


def canonical_bytes(value: object) -> bytes:
    return json.dumps(
        value, ensure_ascii=False, sort_keys=True, separators=(",", ":")
    ).encode("utf-8")


def write_json(path: Path, value: object) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(value, sort_keys=True, indent=2) + "\n", encoding="utf-8")
    return path


def digest(path: Path) -> str:
    value = hashlib.sha256()
    with path.open("rb") as handle:
        while chunk := handle.read(1024 * 1024):
            value.update(chunk)
    return value.hexdigest()


def run(command: list[str]) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        command,
        stdin=subprocess.DEVNULL,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
        encoding="utf-8",
        errors="replace",
        check=False,
    )


class SpeakerActivityRouterTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        TEST_ROOT.mkdir(parents=True, exist_ok=True)

    @classmethod
    def tearDownClass(cls) -> None:
        shutil.rmtree(TEST_ROOT, ignore_errors=True)

    def setUp(self) -> None:
        self.case = TEST_ROOT / self._testMethodName
        self.case.mkdir(parents=True)
        self.duration_ms = 12_000
        self.recording_id = "rec_fixture_speaker_routing_001"
        self.audio_sha = "a" * 64
        self.proxy_sha = "b" * 64
        self.glossary_sha = "c" * 64
        self.preprocess_run_id = "run_preprocess_0123456789abcdef0123456789abcdef"
        self.asr_run_id = "run_asr_whispercpp_0123456789abcdef0123456789abcdef"

    def preprocess(self, *, video: bool = True) -> Path:
        artifacts = [
            {
                "artifact_kind": "audio_16khz_mono_flac",
                "sha256": self.audio_sha,
            }
        ]
        if video:
            artifacts.append(
                {
                    "artifact_kind": "low_resolution_cfr_proxy",
                    "sha256": self.proxy_sha,
                }
            )
        return write_json(
            self.case / "preprocess-result.json",
            {
                "schema_version": 1,
                "status": "completed",
                "dry_run": False,
                "processing_run": {
                    "processing_run_id": self.preprocess_run_id,
                    "stage": "media_preprocess",
                },
                "routing": {
                    "coverage": {
                        "duration_ms": self.duration_ms,
                        "has_video": video,
                    }
                },
                "artifacts": artifacts,
            },
        )

    def asr(self, *, segments: list[tuple[int, int]] | None = None, glossary: bool = True) -> Path:
        segment_values = segments if segments is not None else [(0, self.duration_ms)]
        return write_json(
            self.case / "asr-result.json",
            {
                "schema_version": 1,
                "status": "completed",
                "dry_run": False,
                "processing_run": {
                    "processing_run_id": self.asr_run_id,
                    "stage": "asr_whispercpp",
                },
                "input": {
                    "parent_processing_run_id": self.preprocess_run_id,
                    "sha256": self.audio_sha,
                },
                "glossary": {"sha256": self.glossary_sha} if glossary else None,
                "transcript": {
                    "window": {
                        "offset_ms": 0,
                        "duration_ms": self.duration_ms,
                        "end_ms": self.duration_ms,
                    },
                    "segments": [
                        {
                            "ordinal": ordinal,
                            "start_ms": start,
                            "end_ms": end,
                            "text": "fixture",
                        }
                        for ordinal, (start, end) in enumerate(segment_values)
                    ],
                },
            },
        )

    def hint(
        self,
        hint_id: str,
        start: int,
        end: int,
        *,
        presence: str = "present",
        multiplicity: str = "unknown",
        origin: str = "live_voice",
        face: str = "unknown",
        relation: str = "unknown",
    ) -> dict:
        return {
            "hint_id": hint_id,
            "start_ms": start,
            "end_ms": end,
            "speech_presence": presence,
            "speech_multiplicity": multiplicity,
            "audio_origin": origin,
            "face_visibility": face,
            "speaker_visual_relation": relation,
            "review_confidence": "high",
            "comment": None,
        }

    def hints(self, intervals: list[dict] | None = None) -> Path:
        return write_json(
            self.case / "reviewed-hints.json",
            {
                "schema_version": 1,
                "review_batch_id": "speaker_review_fixture_001",
                "recording_id": self.recording_id,
                "duration_ms": self.duration_ms,
                "review_state": "human_reviewed",
                "reviewer_id": "reviewer_fixture_a",
                "reviewed_at": "2026-08-26T12:00:00Z",
                "source_identity_attestation": None,
                "intervals": intervals or [],
            },
        )

    def source_identity_attestation(
        self,
        *,
        live_speaker_count: int = 1,
        title_context: str = "no_contradiction_found",
        reviewer_id: str = "reviewer_fixture_a",
        reviewer_kind: str = "human",
        reviewed_at: str = "2026-08-26T12:00:00Z",
    ) -> dict:
        return {
            "attestation_id": "source_identity_attestation_fixture_001",
            "source_confirmation": "confirmed_daniel_owned_source",
            "source_confirmation_evidence": (
                "Human-confirmed fixture provenance for a Daniel-owned source."
            ),
            "public_label": "Daniel",
            "basis": "confirmed_daniel_source_solo_presumption",
            "review_scope": "complete_recording",
            "reviewed_live_speaker_count": live_speaker_count,
            "title_context_assessment": title_context,
            "reviewer_id": reviewer_id,
            "reviewer_kind": reviewer_kind,
            "reviewed_at": reviewed_at,
            "comment": "Fixture-only human source and complete-speaker review.",
        }

    def set_source_identity_attestation(self, path: Path, attestation: dict) -> Path:
        raw = json.loads(path.read_text(encoding="utf-8"))
        raw["source_identity_attestation"] = attestation
        return write_json(path, raw)

    def unconfigured(self, name: str) -> dict:
        tasks = {
            "diarization": "overlap_aware_diarization",
            "face_tracking": "face_tracking",
            "active_speaker": "active_speaker_association",
        }
        return {
            "status": "unconfigured",
            "task": tasks[name],
            "reason": "No pinned fixture capability.",
        }

    def pinned(self, name: str, *, gpu: bool = False, memory_mb: int = 512) -> dict:
        root = self.case / f"pinned-{name}"
        root.mkdir(parents=True, exist_ok=True)
        paths: dict[str, Path] = {}
        for filename, body in (
            ("tool.bin", b"fake engine that must never execute\n"),
            ("model-manifest.json", b'{"fixture":true}\n'),
            ("weights.bin", b"fake weights\n"),
            ("calibration.json", b'{"method":"fixture"}\n'),
        ):
            paths[filename] = root / filename
            paths[filename].write_bytes(body)

        def reference(filename: str) -> dict:
            return {"path": str(paths[filename]), "expected_sha256": digest(paths[filename])}

        tasks = {
            "diarization": "overlap_aware_diarization",
            "face_tracking": "face_tracking",
            "active_speaker": "active_speaker_association",
        }
        return {
            "status": "pinned",
            "task": tasks[name],
            "tool": {
                "name": "fixture-engine",
                "version": "0.0.0-fixture",
                "file": reference("tool.bin"),
            },
            "model": {
                "model_id": f"model_fixture_{name}",
                "revision": "fixture-revision",
                "manifest": reference("model-manifest.json"),
                "weights": reference("weights.bin"),
            },
            "calibration": {
                "method": "fixture-only-no-inference",
                "artifact": reference("calibration.json"),
            },
            "requirements": {
                "cpu_threads": 2,
                "memory_mb": memory_mb,
                "gpu_required": gpu,
                "gpu_memory_mb": 2048 if gpu else 0,
            },
        }

    def work_order(
        self,
        preprocess: Path,
        asr: Path,
        hints: Path,
        *,
        capabilities: dict | None = None,
        video: bool = True,
        glossary: bool = True,
    ) -> dict:
        return {
            "schema_version": 1,
            "job_id": "speaker-routing-test-001",
            "recording": {
                "recording_id": self.recording_id,
                "duration_ms": self.duration_ms,
            },
            "inputs": {
                "preprocess_result": {
                    "path": str(preprocess),
                    "expected_sha256": digest(preprocess),
                },
                "asr_result": {"path": str(asr), "expected_sha256": digest(asr)},
                "normalized_audio_sha256": self.audio_sha,
                "proxy_video_sha256": self.proxy_sha if video else None,
                "glossary_sha256": self.glossary_sha if glossary else None,
            },
            "reviewed_hints": {"path": str(hints), "expected_sha256": digest(hints)},
            "capabilities": capabilities
            or {
                name: self.unconfigured(name)
                for name in ("diarization", "face_tracking", "active_speaker")
            },
            "resources": {
                "cpu_threads": 4,
                "memory_mb": 4096,
                "gpu_available": False,
                "gpu_memory_mb": 0,
                "max_parallel_tasks": 1,
            },
            "policy": {
                "diarization_chunk_ms": 3000,
                "face_chunk_ms": 3000,
                "active_speaker_chunk_ms": 2000,
                "route_visual_when_face_unknown": True,
            },
            "output": {"result_path": str(self.case / "route-result.json")},
        }

    def execute(self, order: dict, *, name: str = "work-order.json") -> subprocess.CompletedProcess[str]:
        path = write_json(self.case / name, order)
        return run(["python3", str(PROGRAM), "run", "--work-order", str(path)])

    def full_route(self) -> tuple[dict, dict]:
        preprocess = self.preprocess()
        asr = self.asr()
        hints = self.hints(
            [
                self.hint(
                    "hint_solo", 0, 2000, multiplicity="single", face="single_face", relation="onscreen"
                ),
                self.hint(
                    "hint_overlap", 2000, 4000, multiplicity="overlap", face="multiple_faces", relation="mixed"
                ),
                self.hint(
                    "hint_playback", 4000, 6000, origin="playback_voice", face="none", relation="offscreen"
                ),
                self.hint(
                    "hint_tts", 6000, 8000, origin="tts_voice", face="none", relation="offscreen"
                ),
                self.hint(
                    "hint_synthetic", 8000, 10000, origin="synthetic_voice", face="unknown"
                ),
                self.hint(
                    "hint_reaction", 10000, 12000, origin="reaction_insert_voice", face="unknown"
                ),
            ]
        )
        order = self.work_order(preprocess, asr, hints)
        completed = self.execute(order)
        self.assertEqual(completed.returncode, 0, completed.stderr)
        return order, json.loads(completed.stdout)

    def test_routes_distinct_origins_overlap_solo_and_unknown_associations(self) -> None:
        _, result = self.full_route()
        task_types = {task["task_type"] for task in result["tasks"]}
        self.assertIn("solo_fast_path", task_types)
        self.assertIn("overlap_aware_diarization", task_types)
        self.assertIn("playback_voice_review", task_types)
        self.assertIn("reaction_insert_review", task_types)
        self.assertIn("tts_voice_review", task_types)
        self.assertIn("synthetic_voice_review", task_types)
        self.assertIn("offscreen_speech_review", task_types)
        solo = result["intervals"][0]["speech_observation"]
        self.assertEqual(solo["provisional_speaker_label"], "unknown_single")
        self.assertTrue(
            all(
                interval["speech_observation"]["provisional_speaker_label"] is None
                for interval in result["intervals"]
                if interval["audio_origin_observation"]["category"] != "live_voice"
            )
        )
        self.assertTrue(
            all(
                interval["speech_observation"]["public_speaker_label"] is None
                for interval in result["intervals"]
            )
        )
        overlap = next(
            item for item in result["intervals"] if item["speech_observation"]["multiplicity"] == "overlap"
        )
        self.assertEqual(overlap["start_ms"], 2000)
        self.assertEqual(overlap["end_ms"], 4000)
        self.assertTrue(result["identity_safety"]["overlapping_speech_representable"])
        self.assertTrue(result["active_speaker_association_placeholders"])
        for association in result["active_speaker_association_placeholders"]:
            self.assertEqual(association["association_state"], "unknown")
            self.assertIsNone(association["raw_confidence"])
            self.assertIsNone(association["calibrated_confidence"])
            self.assertIsNone(association["face_track_id"])
        for interval in result["intervals"]:
            self.assertEqual(interval["timestamp_semantics"], "half_open")
            self.assertLess(interval["start_ms"], interval["end_ms"])
            self.assertFalse(
                interval["face_visibility_observation"]["speaking_face_claimed"]
            )

    def test_result_is_byte_deterministic_and_schema_valid(self) -> None:
        order, result = self.full_route()
        first = Path(order["output"]["result_path"]).read_bytes()
        completed = self.execute(order, name="work-order-second.json")
        self.assertEqual(completed.returncode, 0, completed.stderr)
        self.assertEqual(first, completed.stdout.encode())
        schema_check = run(
            [
                "python3",
                str(VALIDATOR),
                "--validate",
                str(RESULT_SCHEMA),
                order["output"]["result_path"],
            ]
        )
        self.assertEqual(schema_check.returncode, 0, schema_check.stderr)
        self.assertEqual(result["status"], "completed")

    def test_confirmed_daniel_solo_source_carries_review_bound_public_label(self) -> None:
        preprocess = self.preprocess()
        asr = self.asr()
        hints = self.hints(
            [
                self.hint(
                    "hint_daniel_solo",
                    0,
                    self.duration_ms,
                    multiplicity="single",
                    face="none",
                    relation="offscreen",
                )
            ]
        )
        self.set_source_identity_attestation(
            hints, self.source_identity_attestation()
        )
        completed = self.execute(self.work_order(preprocess, asr, hints))
        self.assertEqual(completed.returncode, 0, completed.stderr)
        result = json.loads(completed.stdout)
        speech = result["intervals"][0]["speech_observation"]
        self.assertEqual(speech["provisional_speaker_label"], "unknown_single")
        self.assertEqual(speech["public_speaker_label"], "Daniel")
        self.assertFalse(
            result["identity_safety"][
                "named_public_label_requires_visual_confirmation"
            ]
        )
        self.assertFalse(
            result["intervals"][0]["face_visibility_observation"][
                "speaking_face_claimed"
            ]
        )
        self.assertEqual(
            speech["public_identity_attribution"],
            {
                "public_label": "Daniel",
                "basis": "confirmed_daniel_source_solo_presumption",
                "source_confirmation": "confirmed_daniel_owned_source",
                "source_confirmation_attestation_id": (
                    "source_identity_attestation_fixture_001"
                ),
                "reviewer_id": "reviewer_fixture_a",
                "reviewer_kind": "human",
                "reviewed_at": "2026-08-26T12:00:00Z",
            },
        )
        self.assertEqual(
            result["inputs"]["source_identity_attestation"]["reviewer_id"],
            "reviewer_fixture_a",
        )
        solo = next(task for task in result["tasks"] if task["task_type"] == "solo_fast_path")
        self.assertIn("confirmed_daniel_source_solo_presumption", solo["reason_codes"])
        schema_check = run(
            [
                "python3",
                str(VALIDATOR),
                "--validate",
                str(RESULT_SCHEMA),
                result["result_path"],
            ]
        )
        self.assertEqual(schema_check.returncode, 0, schema_check.stderr)

    def test_title_context_contradiction_or_multiple_speakers_keeps_labels_unknown(self) -> None:
        for index, attestation in enumerate(
            (
                self.source_identity_attestation(
                    title_context="contradicts_solo_daniel_presumption"
                ),
                self.source_identity_attestation(live_speaker_count=2),
            )
        ):
            with self.subTest(index=index):
                shutil.rmtree(self.case, ignore_errors=True)
                self.case.mkdir(parents=True)
                preprocess = self.preprocess()
                asr = self.asr()
                hints = self.hints(
                    [
                        self.hint(
                            "hint_solo",
                            0,
                            self.duration_ms,
                            multiplicity="single",
                            face="single_face",
                            relation="onscreen",
                        )
                    ]
                )
                self.set_source_identity_attestation(hints, attestation)
                completed = self.execute(self.work_order(preprocess, asr, hints))
                self.assertEqual(completed.returncode, 0, completed.stderr)
                speech = json.loads(completed.stdout)["intervals"][0][
                    "speech_observation"
                ]
                self.assertEqual(speech["provisional_speaker_label"], "unknown_single")
                self.assertIsNone(speech["public_speaker_label"])
                self.assertIsNone(speech["public_identity_attribution"])

    def test_reviewed_live_overlap_remains_unnamed(self) -> None:
        preprocess = self.preprocess()
        asr = self.asr()
        hints = self.hints(
            [
                self.hint(
                    "hint_reviewed_overlap",
                    0,
                    self.duration_ms,
                    multiplicity="overlap",
                    face="multiple_faces",
                    relation="mixed",
                )
            ]
        )
        self.set_source_identity_attestation(
            hints, self.source_identity_attestation(live_speaker_count=2)
        )
        completed = self.execute(self.work_order(preprocess, asr, hints))
        self.assertEqual(completed.returncode, 0, completed.stderr)
        speech = json.loads(completed.stdout)["intervals"][0]["speech_observation"]
        self.assertEqual(speech["multiplicity"], "overlap")
        self.assertIsNone(speech["provisional_speaker_label"])
        self.assertIsNone(speech["public_speaker_label"])
        self.assertIsNone(speech["public_identity_attribution"])

    def test_named_label_is_visual_independent_but_never_crosses_origin_guards(self) -> None:
        preprocess = self.preprocess()
        asr = self.asr()
        hints = self.hints(
            [
                self.hint(
                    "hint_onscreen",
                    0,
                    1000,
                    multiplicity="single",
                    face="single_face",
                    relation="onscreen",
                ),
                self.hint(
                    "hint_offscreen",
                    1000,
                    2000,
                    multiplicity="single",
                    face="none",
                    relation="offscreen",
                ),
                self.hint(
                    "hint_visual_unknown",
                    2000,
                    3000,
                    multiplicity="single",
                    face="unknown",
                    relation="unknown",
                ),
                self.hint(
                    "hint_visual_multiple_mixed",
                    3000,
                    4000,
                    multiplicity="single",
                    face="multiple_faces",
                    relation="mixed",
                ),
                self.hint(
                    "hint_playback",
                    4000,
                    5600,
                    origin="playback_voice",
                    face="none",
                    relation="offscreen",
                ),
                self.hint(
                    "hint_reaction",
                    5600,
                    7200,
                    origin="reaction_insert_voice",
                    face="single_face",
                    relation="onscreen",
                ),
                self.hint(
                    "hint_tts",
                    7200,
                    8800,
                    origin="tts_voice",
                    face="none",
                    relation="offscreen",
                ),
                self.hint(
                    "hint_synthetic",
                    8800,
                    10400,
                    origin="synthetic_voice",
                    face="single_face",
                    relation="onscreen",
                ),
                self.hint(
                    "hint_unknown_origin",
                    10400,
                    12000,
                    origin="unknown",
                    face="multiple_faces",
                    relation="unknown",
                ),
            ]
        )
        self.set_source_identity_attestation(
            hints, self.source_identity_attestation()
        )
        completed = self.execute(self.work_order(preprocess, asr, hints))
        self.assertEqual(completed.returncode, 0, completed.stderr)
        result = json.loads(completed.stdout)
        named = [
            interval
            for interval in result["intervals"]
            if interval["speech_observation"]["public_speaker_label"] is not None
        ]
        self.assertEqual(
            [(item["start_ms"], item["end_ms"]) for item in named],
            [(0, 1000), (1000, 2000), (2000, 3000), (3000, 4000)],
        )
        self.assertTrue(
            all(
                item["speech_observation"]["public_speaker_label"] is None
                for item in result["intervals"]
                if item["start_ms"] >= 4000
            )
        )
        self.assertTrue(
            all(
                not item["face_visibility_observation"]["speaking_face_claimed"]
                for item in named
            )
        )

    def test_solo_presumption_rejects_incomplete_review_overlap_and_reviewer_mismatch(self) -> None:
        cases = (
            (
                [
                    self.hint(
                        "incomplete",
                        0,
                        6000,
                        multiplicity="single",
                        face="single_face",
                        relation="onscreen",
                    )
                ],
                self.source_identity_attestation(),
                "full-duration hints",
            ),
            (
                [
                    self.hint(
                        "overlap",
                        0,
                        self.duration_ms,
                        multiplicity="overlap",
                        face="multiple_faces",
                        relation="mixed",
                    )
                ],
                self.source_identity_attestation(),
                "live overlap",
            ),
            (
                [
                    self.hint(
                        "reviewer",
                        0,
                        self.duration_ms,
                        multiplicity="single",
                        face="single_face",
                        relation="onscreen",
                    )
                ],
                self.source_identity_attestation(reviewer_id="reviewer_other"),
                "human reviewer",
            ),
            (
                [
                    self.hint(
                        "automated-reviewer",
                        0,
                        self.duration_ms,
                        multiplicity="single",
                        face="single_face",
                        relation="onscreen",
                    )
                ],
                self.source_identity_attestation(
                    reviewer_kind="automated_policy"
                ),
                "reviewer_kind must be human",
            ),
        )
        for index, (intervals, attestation, expected) in enumerate(cases):
            with self.subTest(index=index):
                shutil.rmtree(self.case, ignore_errors=True)
                self.case.mkdir(parents=True)
                preprocess = self.preprocess()
                asr = self.asr()
                hints = self.hints(intervals)
                self.set_source_identity_attestation(hints, attestation)
                order = self.work_order(preprocess, asr, hints)
                order["output"]["result_path"] = str(self.case / f"rejected-{index}.json")
                completed = self.execute(order, name=f"rejected-order-{index}.json")
                self.assertNotEqual(completed.returncode, 0)
                self.assertIn(expected, completed.stderr)

    def test_asr_alone_never_uses_solo_fast_path(self) -> None:
        preprocess = self.preprocess()
        asr = self.asr(segments=[(0, 4000)])
        hints = self.hints([])
        completed = self.execute(self.work_order(preprocess, asr, hints))
        self.assertEqual(completed.returncode, 0, completed.stderr)
        result = json.loads(completed.stdout)
        self.assertNotIn("solo_fast_path", {task["task_type"] for task in result["tasks"]})
        self.assertTrue(
            all(
                interval["speech_observation"]["provisional_speaker_label"] is None
                for interval in result["intervals"]
            )
        )

    def test_asr_overlap_and_touching_boundaries_use_half_open_sweep(self) -> None:
        preprocess = self.preprocess()
        asr = self.asr(segments=[(0, 2000), (1500, 3000), (3000, 4000)])
        hints = self.hints([])
        completed = self.execute(self.work_order(preprocess, asr, hints))
        self.assertEqual(completed.returncode, 0, completed.stderr)
        intervals = json.loads(completed.stdout)["intervals"]
        evidence = {
            (item["start_ms"], item["end_ms"]): item["evidence"]["asr_segment_ordinals"]
            for item in intervals
        }
        self.assertEqual(evidence[(0, 1500)], [0])
        self.assertEqual(evidence[(1500, 2000)], [0, 1])
        self.assertEqual(evidence[(2000, 3000)], [1])
        self.assertEqual(evidence[(3000, 4000)], [2])

    def test_audio_only_never_routes_face_or_active_speaker(self) -> None:
        preprocess = self.preprocess(video=False)
        asr = self.asr(glossary=False)
        hints = self.hints([self.hint("hint_unknown", 0, self.duration_ms)])
        order = self.work_order(preprocess, asr, hints, video=False, glossary=False)
        completed = self.execute(order)
        self.assertEqual(completed.returncode, 0, completed.stderr)
        task_types = {task["task_type"] for task in json.loads(completed.stdout)["tasks"]}
        self.assertNotIn("face_tracking", task_types)
        self.assertNotIn("active_speaker_association", task_types)

    def test_pinned_files_are_hashed_but_never_executed_and_resources_gate(self) -> None:
        preprocess = self.preprocess()
        asr = self.asr()
        hints = self.hints([self.hint("hint_overlap", 0, self.duration_ms, multiplicity="overlap")])
        capabilities = {
            "diarization": self.pinned("diarization", gpu=True),
            "face_tracking": self.pinned("face_tracking"),
            "active_speaker": self.unconfigured("active_speaker"),
        }
        order = self.work_order(preprocess, asr, hints, capabilities=capabilities)
        completed = self.execute(order)
        self.assertEqual(completed.returncode, 0, completed.stderr)
        result = json.loads(completed.stdout)
        diar_tasks = [task for task in result["tasks"] if task["task_type"] == "overlap_aware_diarization"]
        self.assertTrue(diar_tasks)
        self.assertTrue(all(task["execution_state"] == "blocked_resources" for task in diar_tasks))
        self.assertTrue(all("gpu" in task["resource_deficits"] for task in diar_tasks))
        face_tasks = [task for task in result["tasks"] if task["task_type"] == "face_tracking"]
        self.assertTrue(face_tasks)
        self.assertTrue(all(task["execution_state"] == "ready_pinned" for task in face_tasks))
        fake_tool = Path(capabilities["face_tracking"]["tool"]["file"]["path"])
        self.assertFalse(os.access(fake_tool, os.X_OK), "fixture tool must not be executable")

    def test_tampered_pinned_weight_fails_closed(self) -> None:
        preprocess = self.preprocess()
        asr = self.asr()
        hints = self.hints([self.hint("hint_overlap", 0, self.duration_ms, multiplicity="overlap")])
        pinned = self.pinned("diarization")
        order = self.work_order(
            preprocess,
            asr,
            hints,
            capabilities={
                "diarization": pinned,
                "face_tracking": self.unconfigured("face_tracking"),
                "active_speaker": self.unconfigured("active_speaker"),
            },
        )
        Path(pinned["model"]["weights"]["path"]).write_bytes(b"tampered\n")
        completed = self.execute(order)
        self.assertNotEqual(completed.returncode, 0)
        self.assertIn("SHA-256 mismatch", completed.stderr)

    def test_parent_run_audio_proxy_and_glossary_mismatches_fail_closed(self) -> None:
        for field in ("parent", "audio", "proxy", "glossary"):
            with self.subTest(field=field):
                shutil.rmtree(self.case, ignore_errors=True)
                self.case.mkdir(parents=True)
                preprocess = self.preprocess()
                asr = self.asr()
                hints = self.hints([])
                order = self.work_order(preprocess, asr, hints)
                if field == "parent":
                    raw = json.loads(asr.read_text())
                    raw["input"]["parent_processing_run_id"] = "run_preprocess_wrong"
                    write_json(asr, raw)
                    order["inputs"]["asr_result"]["expected_sha256"] = digest(asr)
                elif field == "audio":
                    order["inputs"]["normalized_audio_sha256"] = "d" * 64
                elif field == "proxy":
                    order["inputs"]["proxy_video_sha256"] = "d" * 64
                else:
                    order["inputs"]["glossary_sha256"] = "d" * 64
                completed = self.execute(order)
                self.assertNotEqual(completed.returncode, 0)

    def test_overlapping_hints_and_invalid_absent_multiplicity_fail(self) -> None:
        preprocess = self.preprocess()
        asr = self.asr()
        cases = [
            [self.hint("a", 0, 7000), self.hint("b", 6000, 12000)],
            [self.hint("a", 0, 12000, presence="absent", multiplicity="single")],
        ]
        for index, values in enumerate(cases):
            with self.subTest(index=index):
                hints = self.hints(values)
                order = self.work_order(preprocess, asr, hints)
                order["output"]["result_path"] = str(self.case / f"result-{index}.json")
                completed = self.execute(order, name=f"work-order-{index}.json")
                self.assertNotEqual(completed.returncode, 0)

    def test_duplicate_hint_ids_and_unzoned_review_time_fail(self) -> None:
        preprocess = self.preprocess()
        asr = self.asr()
        cases = [
            {
                "intervals": [
                    self.hint("duplicate", 0, 6000),
                    self.hint("duplicate", 6000, 12000),
                ]
            },
            {
                "intervals": [self.hint("valid", 0, 12000)],
                "reviewed_at": "2026-08-26T12:00:00",
            },
        ]
        for index, changes in enumerate(cases):
            with self.subTest(index=index):
                hints = self.hints(changes["intervals"])
                if "reviewed_at" in changes:
                    raw = json.loads(hints.read_text())
                    raw["reviewed_at"] = changes["reviewed_at"]
                    write_json(hints, raw)
                order = self.work_order(preprocess, asr, hints)
                order["output"]["result_path"] = str(self.case / f"bad-hints-{index}.json")
                completed = self.execute(order, name=f"bad-hints-order-{index}.json")
                self.assertNotEqual(completed.returncode, 0)

    def test_unknown_work_order_key_and_symlink_input_fail(self) -> None:
        preprocess = self.preprocess()
        asr = self.asr()
        hints = self.hints([])
        order = self.work_order(preprocess, asr, hints)
        order["identity"] = "Daniel"
        completed = self.execute(order)
        self.assertNotEqual(completed.returncode, 0)
        del order["identity"]
        symlink = self.case / "asr-link.json"
        symlink.symlink_to(asr)
        order["inputs"]["asr_result"] = {
            "path": str(symlink),
            "expected_sha256": digest(asr),
        }
        completed = self.execute(order, name="work-order-symlink.json")
        self.assertNotEqual(completed.returncode, 0)

    def test_schema_rejects_identity_label_and_non_half_open_semantics(self) -> None:
        order, result = self.full_route()
        result["intervals"][0]["speech_observation"]["provisional_speaker_label"] = "Daniel"
        invalid_label = write_json(self.case / "invalid-label.json", result)
        checked = run(
            ["python3", str(VALIDATOR), "--validate", str(RESULT_SCHEMA), str(invalid_label)]
        )
        self.assertNotEqual(checked.returncode, 0)

        result["intervals"][0]["speech_observation"]["provisional_speaker_label"] = "unknown_single"
        result["intervals"][0]["timestamp_semantics"] = "closed"
        invalid_time = write_json(self.case / "invalid-time.json", result)
        checked = run(
            ["python3", str(VALIDATOR), "--validate", str(RESULT_SCHEMA), str(invalid_time)]
        )
        self.assertNotEqual(checked.returncode, 0)

        preprocess = self.preprocess()
        asr = self.asr()
        hints = self.hints(
            [
                self.hint(
                    "hint_playback_single",
                    0,
                    self.duration_ms,
                    multiplicity="single",
                    origin="playback_voice",
                )
            ]
        )
        non_live_order = self.work_order(preprocess, asr, hints)
        non_live_order["output"]["result_path"] = str(
            self.case / "non-live-result.json"
        )
        completed = self.execute(non_live_order, name="non-live-work-order.json")
        self.assertEqual(completed.returncode, 0, completed.stderr)
        result = json.loads(completed.stdout)
        non_live = result["intervals"][0]
        self.assertIsNone(
            non_live["speech_observation"]["provisional_speaker_label"]
        )
        non_live["speech_observation"]["provisional_speaker_label"] = "unknown_single"
        invalid_non_live_solo = write_json(
            self.case / "invalid-non-live-solo.json", result
        )
        checked = run(
            [
                "python3",
                str(VALIDATOR),
                "--validate",
                str(RESULT_SCHEMA),
                str(invalid_non_live_solo),
            ]
        )
        self.assertNotEqual(checked.returncode, 0)

        _, result = self.full_route()
        result["intervals"][0]["speech_observation"][
            "provisional_speaker_label"
        ] = None
        invalid_missing_solo = write_json(self.case / "invalid-missing-solo.json", result)
        checked = run(
            [
                "python3",
                str(VALIDATOR),
                "--validate",
                str(RESULT_SCHEMA),
                str(invalid_missing_solo),
            ]
        )
        self.assertNotEqual(checked.returncode, 0)

    def test_schema_rejects_unattested_or_unsafe_public_daniel_label(self) -> None:
        preprocess = self.preprocess()
        asr = self.asr()
        hints = self.hints(
            [
                self.hint(
                    "hint_daniel_solo",
                    0,
                    self.duration_ms,
                    multiplicity="single",
                    face="single_face",
                    relation="onscreen",
                )
            ]
        )
        self.set_source_identity_attestation(
            hints, self.source_identity_attestation()
        )
        completed = self.execute(self.work_order(preprocess, asr, hints))
        self.assertEqual(completed.returncode, 0, completed.stderr)
        valid = json.loads(completed.stdout)

        def rejected(name: str, mutate) -> None:
            candidate = json.loads(json.dumps(valid))
            mutate(candidate)
            path = write_json(self.case / f"invalid-public-{name}.json", candidate)
            checked = run(
                [
                    "python3",
                    str(VALIDATOR),
                    "--validate",
                    str(RESULT_SCHEMA),
                    str(path),
                ]
            )
            self.assertNotEqual(checked.returncode, 0, name)

        rejected(
            "missing-source-attestation",
            lambda value: value["inputs"].__setitem__(
                "source_identity_attestation", None
            ),
        )
        rejected(
            "title-contradiction",
            lambda value: value["inputs"]["source_identity_attestation"].__setitem__(
                "title_context_assessment",
                "contradicts_solo_daniel_presumption",
            ),
        )
        rejected(
            "missing-reviewer-binding",
            lambda value: value["intervals"][0]["speech_observation"][
                "public_identity_attribution"
            ].pop("reviewer_id"),
        )
        rejected(
            "playback",
            lambda value: value["intervals"][0][
                "audio_origin_observation"
            ].__setitem__("category", "playback_voice"),
        )

    def test_tracked_work_order_and_hint_examples_validate(self) -> None:
        examples = [
            (
                WORK_SCHEMA,
                PIPELINE_ROOT / "examples" / "speaker-activity-routing-work-order.example.json",
            ),
            (
                HINT_SCHEMA,
                PIPELINE_ROOT / "examples" / "speaker-reviewed-hints.example.json",
            ),
            (
                HINT_SCHEMA,
                PIPELINE_ROOT
                / "examples"
                / "speaker-reviewed-hints.daniel-solo.example.json",
            ),
        ]
        for schema, example in examples:
            with self.subTest(example=example.name):
                checked = run(
                    ["python3", str(VALIDATOR), "--validate", str(schema), str(example)]
                )
                self.assertEqual(checked.returncode, 0, checked.stderr)


if __name__ == "__main__":
    unittest.main()
