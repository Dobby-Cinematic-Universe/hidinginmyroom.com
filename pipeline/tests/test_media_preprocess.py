from __future__ import annotations

import fcntl
import hashlib
import json
import os
import shutil
import subprocess
import unittest
from pathlib import Path

from pipeline.media_preprocess import PipelineError, parse_routing_log
from pipeline import media_preprocess_v034 as successor


REPOSITORY_ROOT = Path(__file__).resolve().parents[2]
PIPELINE_ROOT = REPOSITORY_ROOT / "pipeline"
PROGRAM = PIPELINE_ROOT / "media_preprocess.py"
PROFILE = PIPELINE_ROOT / "profiles" / "cpu-balanced-v1.json"
CONTRACT_VALIDATOR = REPOSITORY_ROOT / "scripts" / "validate-json-contracts.py"
RESULT_SCHEMA = PIPELINE_ROOT / "schemas" / "result.schema.json"
TEST_ROOT = PIPELINE_ROOT / ".test-work"


def run(command: list[str], *, check: bool = True) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        command,
        stdin=subprocess.DEVNULL,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
        encoding="utf-8",
        errors="replace",
        check=check,
    )


def digest(path: Path) -> str:
    value = hashlib.sha256()
    with path.open("rb") as handle:
        while chunk := handle.read(1024 * 1024):
            value.update(chunk)
    return value.hexdigest()


def generate_fixture(path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    command = [
        "ffmpeg",
        "-hide_banner",
        "-nostdin",
        "-loglevel",
        "error",
        "-f",
        "lavfi",
        "-i",
        "color=c=black:s=320x240:r=30:d=1.5",
        "-f",
        "lavfi",
        "-i",
        "color=c=white:s=320x240:r=30:d=1.5",
        "-f",
        "lavfi",
        "-i",
        "color=c=blue:s=320x240:r=30:d=1.5",
        "-f",
        "lavfi",
        "-i",
        "sine=frequency=440:sample_rate=48000:duration=1.5",
        "-f",
        "lavfi",
        "-i",
        "anullsrc=r=48000:cl=mono:d=1.5",
        "-f",
        "lavfi",
        "-i",
        "sine=frequency=660:sample_rate=48000:duration=1.5",
        "-filter_complex",
        "[0:v][1:v][2:v]concat=n=3:v=1:a=0[v];"
        "[3:a][4:a][5:a]concat=n=3:v=0:a=1[a]",
        "-map",
        "[v]",
        "-map",
        "[a]",
        "-c:v",
        "libx264",
        "-preset",
        "ultrafast",
        "-pix_fmt",
        "yuv420p",
        "-c:a",
        "aac",
        "-shortest",
        str(path),
    ]
    run(command)


def generate_video_only_fixture(path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    run(
        [
            "ffmpeg",
            "-hide_banner",
            "-nostdin",
            "-loglevel",
            "error",
            "-f",
            "lavfi",
            "-i",
            "color=c=blue:s=320x240:r=30:d=1.0",
            "-an",
            "-c:v",
            "libx264",
            "-preset",
            "ultrafast",
            "-pix_fmt",
            "yuv420p",
            str(path),
        ]
    )


def work_order(source: Path, output_root: Path, expected_sha256: str | None = None) -> dict:
    return {
        "schema_version": 1,
        "job_id": "lavfi-fixture-001",
        "source": {
            "path": str(source.resolve()),
            "expected_sha256": expected_sha256,
            "first_cataloged_at": None,
        },
        "output": {"root": str(output_root.resolve())},
        "operations": {
            "probe": True,
            "audio_flac": True,
            "proxy": True,
            "routing": True,
        },
        "profile": json.loads(PROFILE.read_text(encoding="utf-8")),
    }


class MediaPreprocessTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        if not shutil.which("ffmpeg") or not shutil.which("ffprobe"):
            raise unittest.SkipTest("ffmpeg and ffprobe are required")
        TEST_ROOT.mkdir(parents=True, exist_ok=True)

    @classmethod
    def tearDownClass(cls) -> None:
        shutil.rmtree(TEST_ROOT, ignore_errors=True)

    def setUp(self) -> None:
        self.case = TEST_ROOT / self._testMethodName
        self.case.mkdir(parents=True, exist_ok=True)
        self.source = self.case / "fixture.mp4"
        generate_fixture(self.source)
        self.source_digest = digest(self.source)
        self.source_stat = self.source.stat()

    def write_work_order(self, value: dict) -> Path:
        path = self.case / "work-order.json"
        path.write_text(json.dumps(value, indent=2) + "\n", encoding="utf-8")
        return path

    def execute(self, value: dict, *arguments: str) -> subprocess.CompletedProcess[str]:
        work_order_path = self.write_work_order(value)
        return run(
            [
                "python3",
                str(PROGRAM),
                "run",
                "--work-order",
                str(work_order_path),
                *arguments,
            ],
            check=False,
        )

    def assert_result_contract(self, result: dict, filename: str) -> None:
        result_path = self.case / filename
        result_path.write_text(json.dumps(result), encoding="utf-8")
        completed = run(
            [
                "python3",
                str(CONTRACT_VALIDATOR),
                "--validate",
                str(RESULT_SCHEMA),
                str(result_path),
            ],
            check=False,
        )
        self.assertEqual(
            completed.returncode,
            0,
            f"Generated preprocessing result failed its JSON contract:\n"
            f"{completed.stdout}{completed.stderr}",
        )

    def reseal_routing_and_result(self, result: dict) -> None:
        routing_path = Path(
            next(
                artifact["path"]
                for artifact in result["artifacts"]
                if artifact["artifact_kind"] == "scene_silence_routing_json"
            )
        )
        routing_body = (
            json.dumps(
                result["routing"], ensure_ascii=False, sort_keys=True, indent=2
            )
            + "\n"
        ).encode("utf-8")
        routing_path.chmod(0o644)
        routing_path.write_bytes(routing_body)
        routing_path.chmod(0o444)
        routing_sha256 = hashlib.sha256(routing_body).hexdigest()
        descriptor = next(
            artifact
            for artifact in result["artifacts"]
            if artifact["artifact_kind"] == "scene_silence_routing_json"
        )
        old_artifact_id = descriptor["artifact_id"]
        artifact_identity = {
            "processing_run_id": result["processing_run"]["processing_run_id"],
            "kind": "scene_silence_routing_json",
            "sha256": routing_sha256,
        }
        new_artifact_id = "artifact_" + hashlib.sha256(
            json.dumps(
                artifact_identity,
                ensure_ascii=False,
                sort_keys=True,
                separators=(",", ":"),
            ).encode("utf-8")
        ).hexdigest()[:32]
        descriptor.update(
            {
                "artifact_id": new_artifact_id,
                "sha256": routing_sha256,
                "byte_count": len(routing_body),
            }
        )
        catalog_descriptor = next(
            artifact
            for artifact in result["catalog_records"]["artifacts"]
            if artifact["artifact_id"] == old_artifact_id
        )
        catalog_descriptor.update(
            {
                "artifact_id": new_artifact_id,
                "sha256": routing_sha256,
                "byte_count": len(routing_body),
            }
        )
        result_path = Path(result["result_path"])
        result_path.chmod(0o644)
        result_path.write_text(
            json.dumps(result, ensure_ascii=False, sort_keys=True, indent=2) + "\n",
            encoding="utf-8",
        )
        result_path.chmod(0o444)

    def test_routing_clamps_bounded_silence_tail_to_declared_coverage(self) -> None:
        profile = json.loads(PROFILE.read_text(encoding="utf-8"))
        routing = parse_routing_log(
            "\n".join(
                (
                    "[silencedetect] silence_start: 834.398",
                    "[silencedetect] silence_end: 838.912 | silence_duration: 4.514",
                )
            ),
            duration_ms=838_902,
            has_video=True,
            has_audio=True,
            profile=profile,
        )

        self.assertEqual(
            routing["silence_intervals"],
            [{"start_ms": 834_398, "end_ms": 838_902, "duration_ms": 4_504}],
        )
        self.assertEqual(routing["summary"]["silent_duration_ms"], 4_504)
        self.assertEqual(routing["summary"]["silent_fraction"], 0.005369)

    def test_routing_rejects_large_silence_tail_overrun(self) -> None:
        profile = json.loads(PROFILE.read_text(encoding="utf-8"))
        with self.assertRaisesRegex(
            PipelineError,
            "silence end exceeds normalized media duration by 251 ms",
        ):
            parse_routing_log(
                "\n".join(
                    (
                        "[silencedetect] silence_start: 838.000",
                        "[silencedetect] silence_end: 839.153 | silence_duration: 1.153",
                    )
                ),
                duration_ms=838_902,
                has_video=True,
                has_audio=True,
                profile=profile,
            )

    def test_routing_tail_boundaries_negative_zero_and_fallback_start(self) -> None:
        profile = json.loads(PROFILE.read_text(encoding="utf-8"))
        exact_bound = parse_routing_log(
            "silence_start: 0.500\nsilence_end: 1.250 | silence_duration: 0.750",
            duration_ms=1_000,
            has_video=True,
            has_audio=True,
            profile=profile,
        )
        self.assertEqual(
            exact_bound["silence_intervals"],
            [{"start_ms": 500, "end_ms": 1_000, "duration_ms": 500}],
        )
        self.assertEqual(
            exact_bound["summary"],
            {
                "scene_change_count": 0,
                "silence_interval_count": 1,
                "silent_duration_ms": 500,
                "silent_fraction": 0.5,
            },
        )

        fallback_start = parse_routing_log(
            "silence_end: 1.250 | silence_duration: 0.750",
            duration_ms=1_000,
            has_video=True,
            has_audio=True,
            profile=profile,
        )
        self.assertEqual(fallback_start["silence_intervals"], exact_bound["silence_intervals"])

        for end_text in ("-0.001", "0.000"):
            with self.subTest(end_text=end_text):
                empty = parse_routing_log(
                    f"silence_start: -0.100\nsilence_end: {end_text}",
                    duration_ms=1_000,
                    has_video=True,
                    has_audio=True,
                    profile=profile,
                )
                self.assertEqual(empty["silence_intervals"], [])
                self.assertEqual(empty["summary"]["silent_duration_ms"], 0)
                self.assertEqual(empty["summary"]["silent_fraction"], 0.0)

        zero_coverage = parse_routing_log(
            "silence_start: 0.000\nsilence_end: 0.250",
            duration_ms=0,
            has_video=True,
            has_audio=True,
            profile=profile,
        )
        self.assertEqual(zero_coverage["silence_intervals"], [])
        self.assertIsNone(zero_coverage["summary"]["silent_fraction"])
        with self.assertRaisesRegex(PipelineError, "by 251 ms"):
            parse_routing_log(
                "silence_start: 0.000\nsilence_end: 0.251",
                duration_ms=0,
                has_video=True,
                has_audio=True,
                profile=profile,
            )

    def test_routing_clamps_deduplicates_and_bounds_scene_tail(self) -> None:
        profile = json.loads(PROFILE.read_text(encoding="utf-8"))
        routing = parse_routing_log(
            "\n".join(
                (
                    "lavfi.scd.score: 20.0, lavfi.scd.time: 1.100",
                    "lavfi.scd.score: 40.0, lavfi.scd.time: 1.250",
                    "lavfi.scd.score: 30.0, lavfi.scd.time: 1.250",
                )
            ),
            duration_ms=1_000,
            has_video=True,
            has_audio=True,
            profile=profile,
        )
        self.assertEqual(
            routing["scene_changes"],
            [{"timestamp_ms": 1_000, "score_percent": 40.0}],
        )
        self.assertEqual(routing["summary"]["scene_change_count"], 1)
        with self.assertRaisesRegex(
            PipelineError, "scene timestamp exceeds normalized media duration by 251 ms"
        ):
            parse_routing_log(
                "lavfi.scd.score: 20.0, lavfi.scd.time: 1.251",
                duration_ms=1_000,
                has_video=True,
                has_audio=True,
                profile=profile,
            )

    def test_dry_run_hashes_and_probes_without_writing_outputs(self) -> None:
        output_root = self.case / "derived"
        completed = self.execute(work_order(self.source, output_root), "--dry-run")
        self.assertEqual(completed.returncode, 0, completed.stderr)
        result = json.loads(completed.stdout)
        self.assert_result_contract(result, "dry-run-result.contract.json")
        self.assertEqual(result["status"], "planned")
        self.assertTrue(result["dry_run"])
        self.assertEqual(result["input"]["sha256"], self.source_digest)
        self.assertFalse(output_root.exists())
        ffmpeg_steps = [step for step in result["steps"] if step["command"]]
        for step in ffmpeg_steps[1:]:
            self.assertIn("-threads", step["command"])
            thread_index = step["command"].index("-threads")
            self.assertEqual(step["command"][thread_index + 1], "4")

    def test_full_pipeline_emits_valid_derivatives_and_catalog_hints(self) -> None:
        output_root = self.case / "derived"
        completed = self.execute(work_order(self.source, output_root))
        self.assertEqual(completed.returncode, 0, completed.stderr)
        result = json.loads(completed.stdout)
        self.assert_result_contract(result, "full-result.contract.json")
        self.assertEqual(result["status"], "completed")
        self.assertFalse(result["dry_run"])
        self.assertTrue(result["input"]["unchanged"])
        self.assertEqual(digest(self.source), self.source_digest)
        self.assertEqual(self.source.stat().st_size, self.source_stat.st_size)
        self.assertEqual(self.source.stat().st_mtime_ns, self.source_stat.st_mtime_ns)

        run_dir = Path(result["layout"]["run_dir"])
        self.assertTrue((run_dir / "probe.normalized.json").is_file())
        self.assertTrue((run_dir / "routing.json").is_file())
        self.assertTrue((run_dir / "result.json").is_file())
        self.assertEqual(
            json.loads((run_dir / "result.json").read_text(encoding="utf-8")),
            result,
        )
        audio = run_dir / "artifacts" / "audio-16khz-mono.flac"
        proxy = run_dir / "artifacts" / "proxy-640x360-25fps.mp4"
        self.assertTrue(audio.is_file())
        self.assertTrue(proxy.is_file())

        audio_probe = json.loads(
            run(
                [
                    "ffprobe",
                    "-v",
                    "error",
                    "-select_streams",
                    "a:0",
                    "-show_entries",
                    "stream=codec_name,sample_rate,channels,sample_fmt",
                    "-of",
                    "json",
                    str(audio),
                ]
            ).stdout
        )["streams"][0]
        self.assertEqual(audio_probe["codec_name"], "flac")
        self.assertEqual(audio_probe["sample_rate"], "16000")
        self.assertEqual(audio_probe["channels"], 1)
        self.assertEqual(audio_probe["sample_fmt"], "s16")
        audio_step = next(
            step for step in result["steps"] if step["name"] == "audio_flac"
        )
        self.assertIn("-sample_fmt", audio_step["command"])
        sample_format_index = audio_step["command"].index("-sample_fmt")
        self.assertEqual(audio_step["command"][sample_format_index + 1], "s16")

        audio_artifact = next(
            artifact
            for artifact in result["artifacts"]
            if artifact["artifact_kind"] == "audio_16khz_mono_flac"
        )
        artifact_identity = {
            "processing_run_id": result["processing_run"]["processing_run_id"],
            "kind": audio_artifact["artifact_kind"],
            "sha256": audio_artifact["sha256"],
        }
        expected_artifact_id = "artifact_" + hashlib.sha256(
            json.dumps(
                artifact_identity,
                ensure_ascii=False,
                sort_keys=True,
                separators=(",", ":"),
            ).encode("utf-8")
        ).hexdigest()[:32]
        self.assertEqual(audio_artifact["artifact_id"], expected_artifact_id)

        proxy_probe = json.loads(
            run(
                [
                    "ffprobe",
                    "-v",
                    "error",
                    "-select_streams",
                    "v:0",
                    "-show_entries",
                    "stream=width,height,avg_frame_rate",
                    "-of",
                    "json",
                    str(proxy),
                ]
            ).stdout
        )["streams"][0]
        self.assertEqual(proxy_probe["width"], 640)
        self.assertEqual(proxy_probe["height"], 360)
        self.assertEqual(proxy_probe["avg_frame_rate"], "25/1")

        routing = result["routing"]
        self.assertGreaterEqual(routing["summary"]["scene_change_count"], 2)
        self.assertGreaterEqual(routing["summary"]["silence_interval_count"], 1)
        self.assertEqual(routing["routing_candidates"]["asr"], "process")
        self.assertIn("media_objects", result["catalog_records"])
        self.assertEqual(len(result["catalog_records"]["media_derivations"]), 2)
        source_media = next(
            media
            for media in result["catalog_records"]["media_objects"]
            if media["media_id"] == result["input"]["media_id"]
        )
        self.assertNotIn("acquired_at", source_media)
        self.assertEqual(
            source_media["first_cataloged_at"],
            result["input"]["catalog_observation"]["first_cataloged_at"],
        )
        self.assertEqual(
            result["input"]["catalog_observation"]["acquisition_timestamp_state"],
            "not_claimed_by_preprocessing",
        )
        for artifact in result["artifacts"]:
            self.assertTrue(Path(artifact["path"]).is_relative_to(output_root))
            self.assertRegex(artifact["sha256"], r"^[0-9a-f]{64}$")
            self.assertEqual(Path(artifact["path"]).stat().st_mode & 0o222, 0)
        self.assertEqual(Path(result["result_path"]).stat().st_mode & 0o222, 0)
        tools = result["processing_run"]["parameters_json"]["tools"]
        tool_paths = result["processing_run"]["environment_json"]["tool_paths"]
        for name in ("ffmpeg", "ffprobe"):
            provenance = tools[name]
            self.assertEqual(provenance["name"], name)
            self.assertEqual(
                provenance["executable_sha256"], digest(Path(tool_paths[name]))
            )
            self.assertEqual(
                provenance["version_output_sha256"],
                hashlib.sha256(provenance["version_output"].encode()).hexdigest(),
            )
            self.assertEqual(
                provenance["version"], provenance["version_output"].splitlines()[0]
            )
        recipe_body = json.dumps(
            result["processing_run"]["parameters_json"],
            ensure_ascii=False,
            sort_keys=True,
            separators=(",", ":"),
        ).encode()
        self.assertEqual(
            result["layout"]["recipe_sha256"], hashlib.sha256(recipe_body).hexdigest()
        )
        self.assertEqual(
            result["layout"]["recipe_id"],
            f"recipe_preprocess_{result['layout']['recipe_sha256'][:32]}",
        )

    def test_asr_ready_operations_emit_only_probe_and_normalized_audio(self) -> None:
        output_root = self.case / "asr-ready-derived"
        value = work_order(self.source, output_root)
        value["operations"] = {
            "probe": True,
            "audio_flac": True,
            "proxy": False,
            "routing": False,
        }
        completed = self.execute(value)
        self.assertEqual(completed.returncode, 0, completed.stderr)
        result = json.loads(completed.stdout)
        self.assertEqual(result["status"], "completed")
        self.assertIsNone(result["routing"])
        self.assertEqual(
            ["ffprobe_normalized_json", "audio_16khz_mono_flac"],
            [artifact["artifact_kind"] for artifact in result["artifacts"]],
        )
        self.assertEqual(
            {"probe": "completed", "audio_flac": "completed", "proxy": "disabled", "routing": "disabled"},
            {step["name"]: step["status"] for step in result["steps"]},
        )
        run_dir = Path(result["layout"]["run_dir"])
        self.assertTrue((run_dir / "probe.normalized.json").is_file())
        self.assertTrue((run_dir / "artifacts" / "audio-16khz-mono.flac").is_file())
        self.assertFalse(any(run_dir.rglob("proxy-*.mp4")))
        self.assertFalse((run_dir / "routing.json").exists())

    def test_enrichment_only_emits_visual_routing_without_regenerating_audio(self) -> None:
        output_root = self.case / "enrichment-only-derived"
        value = work_order(self.source, output_root)
        value["operations"] = {
            "probe": True,
            "audio_flac": False,
            "proxy": True,
            "routing": True,
        }
        completed = self.execute(value)
        self.assertEqual(completed.returncode, 0, completed.stderr)
        result = json.loads(completed.stdout)
        self.assertEqual(
            [
                "ffprobe_normalized_json",
                "low_resolution_cfr_proxy",
                "scene_silence_routing_json",
            ],
            [artifact["artifact_kind"] for artifact in result["artifacts"]],
        )
        self.assertEqual("disabled", result["steps"][1]["status"])
        self.assertFalse(
            any(Path(result["layout"]["run_dir"]).rglob("audio-*.flac"))
        )

        video_only = self.case / "video-only.mp4"
        generate_video_only_fixture(video_only)
        video_value = work_order(video_only, self.case / "video-only-enrichment")
        video_value["job_id"] = "video-only-enrichment-001"
        video_value["operations"] = dict(value["operations"])
        video_completed = self.execute(video_value)
        self.assertEqual(video_completed.returncode, 0, video_completed.stderr)
        video_result = json.loads(video_completed.stdout)
        self.assertEqual("not_applicable", video_result["steps"][1]["status"])
        self.assertEqual("completed", video_result["steps"][2]["status"])
        self.assertEqual("completed", video_result["steps"][3]["status"])
        self.assertNotIn(
            "audio_16khz_mono_flac",
            {artifact["artifact_kind"] for artifact in video_result["artifacts"]},
        )

    def test_existing_outputs_are_reused_without_changing_source(self) -> None:
        output_root = self.case / "derived"
        value = work_order(self.source, output_root)
        first = self.execute(value)
        self.assertEqual(first.returncode, 0, first.stderr)
        first_result = json.loads(first.stdout)
        second = self.execute(value)
        self.assertEqual(second.returncode, 0, second.stderr)
        result = json.loads(second.stdout)
        statuses = {step["name"]: step["status"] for step in result["steps"]}
        self.assertEqual(statuses["audio_flac"], "reused")
        self.assertEqual(statuses["proxy"], "reused")
        self.assertTrue(result["input"]["unchanged"])
        self.assertNotEqual(
            first_result["processing_run"]["processing_run_id"],
            result["processing_run"]["processing_run_id"],
        )
        self.assertEqual(
            first_result["layout"]["recipe_id"], result["layout"]["recipe_id"]
        )
        self.assertEqual(result["reuse"]["mode"], "verified_prior_result")
        self.assertEqual(
            result["reuse"]["prior_processing_run_id"],
            first_result["processing_run"]["processing_run_id"],
        )
        prior_result_path = Path(first_result["result_path"])
        self.assertEqual(
            result["reuse"]["prior_result_sha256"], digest(prior_result_path)
        )
        first_artifacts = {
            item["artifact_kind"]: item for item in first_result["artifacts"]
        }
        for artifact in result["artifacts"]:
            prior_path = Path(first_artifacts[artifact["artifact_kind"]]["path"])
            current_path = Path(artifact["path"])
            self.assertEqual(prior_path.stat().st_ino, current_path.stat().st_ino)
            self.assertNotEqual(
                first_artifacts[artifact["artifact_kind"]]["artifact_id"],
                artifact["artifact_id"],
            )

    def test_successor_duplicate_reuse_has_distinct_single_link_artifacts(self) -> None:
        output_root = self.case / "successor-derived"
        value = successor.validate_work_order(work_order(self.source, output_root))
        first = successor.run_work_order(value, dry_run=False)
        second = successor.run_work_order(value, dry_run=False)
        self.assertEqual("none", first["reuse"]["mode"])
        self.assertEqual("verified_prior_result", second["reuse"]["mode"])
        first_by_kind = {
            row["artifact_kind"]: row for row in first["artifacts"]
        }
        self.assertEqual(set(first_by_kind), {
            row["artifact_kind"] for row in second["artifacts"]
        })
        for row in second["artifacts"]:
            prior = Path(first_by_kind[row["artifact_kind"]]["path"])
            reused = Path(row["path"])
            self.assertEqual(first_by_kind[row["artifact_kind"]]["sha256"], row["sha256"])
            self.assertNotEqual(prior.stat().st_ino, reused.stat().st_ino)
            self.assertEqual(1, prior.stat().st_nlink)
            self.assertEqual(1, reused.stat().st_nlink)

    def test_replay_rejects_coherently_resealed_routing_summary_tamper(self) -> None:
        output_root = self.case / "derived"
        value = work_order(self.source, output_root)
        first = self.execute(value)
        self.assertEqual(first.returncode, 0, first.stderr)
        result = json.loads(first.stdout)
        result["routing"]["summary"]["silent_duration_ms"] += 1
        self.reseal_routing_and_result(result)

        replay = self.execute(value)
        self.assertEqual(replay.returncode, 2)
        self.assertIn(
            "summary disagrees with exact routing arithmetic",
            json.loads(replay.stderr)["error"]["message"],
        )
        self.assertEqual(len(list(output_root.rglob("result.json"))), 1)

    def test_implementation_version_separates_old_recipe_tree(self) -> None:
        output_root = self.case / "derived"
        value = work_order(self.source, output_root)
        planned = self.execute(value, "--dry-run")
        self.assertEqual(planned.returncode, 0, planned.stderr)
        plan = json.loads(planned.stdout)
        self.assertEqual(plan["processing_run"]["implementation_version"], "0.3.3")
        self.assertEqual(
            plan["processing_run"]["parameters_json"]["implementation_version"],
            "0.3.3",
        )

        old_recipe = json.loads(
            json.dumps(plan["processing_run"]["parameters_json"])
        )
        old_recipe["implementation_version"] = "0.3.2"
        old_recipe_sha256 = hashlib.sha256(
            json.dumps(
                old_recipe,
                ensure_ascii=False,
                sort_keys=True,
                separators=(",", ":"),
            ).encode("utf-8")
        ).hexdigest()
        self.assertNotEqual(old_recipe_sha256, plan["layout"]["recipe_sha256"])
        old_recipe_dir = (
            Path(plan["layout"]["object_dir"])
            / "recipes"
            / old_recipe_sha256
        )
        stale_result = (
            old_recipe_dir
            / "executions"
            / ("run_preprocess_" + "0" * 32)
            / "result.json"
        )
        stale_result.parent.mkdir(parents=True)
        stale_result.write_text("{}\n", encoding="utf-8")
        stale_result.chmod(0o444)

        completed = self.execute(value)
        self.assertEqual(completed.returncode, 0, completed.stderr)
        result = json.loads(completed.stdout)
        self.assertEqual(result["reuse"]["mode"], "none")
        self.assertEqual(result["layout"]["recipe_sha256"], plan["layout"]["recipe_sha256"])
        self.assertTrue(stale_result.is_file())

    def test_tampered_prior_artifact_is_rejected_instead_of_reused(self) -> None:
        output_root = self.case / "derived"
        value = work_order(self.source, output_root)
        first = self.execute(value)
        self.assertEqual(first.returncode, 0, first.stderr)
        result = json.loads(first.stdout)
        audio = Path(
            next(
                artifact["path"]
                for artifact in result["artifacts"]
                if artifact["artifact_kind"] == "audio_16khz_mono_flac"
            )
        )
        audio.chmod(0o644)
        with audio.open("r+b") as handle:
            handle.seek(-1, os.SEEK_END)
            original = handle.read(1)
            handle.seek(-1, os.SEEK_END)
            handle.write(bytes([original[0] ^ 0x01]))
        audio.chmod(0o444)

        second = self.execute(value)
        self.assertEqual(second.returncode, 2)
        self.assertIn("SHA-256", json.loads(second.stderr)["error"]["message"])
        self.assertEqual(len(list(output_root.rglob("result.json"))), 1)

    def test_sealed_prior_envelope_with_tampered_relationship_is_rejected(self) -> None:
        output_root = self.case / "derived"
        value = work_order(self.source, output_root)
        first = self.execute(value)
        self.assertEqual(first.returncode, 0, first.stderr)
        result = json.loads(first.stdout)
        result_path = Path(result["result_path"])
        result["artifacts"][0]["artifact_id"] = "artifact_" + "0" * 32
        result_path.chmod(0o644)
        result_path.unlink()
        result_path.write_text(json.dumps(result, sort_keys=True, indent=2) + "\n", encoding="utf-8")
        result_path.chmod(0o444)

        second = self.execute(value)
        self.assertEqual(second.returncode, 2)
        self.assertIn(
            "artifact identity", json.loads(second.stderr)["error"]["message"]
        )

    def test_orphaned_partial_execution_is_never_reuse_authority(self) -> None:
        output_root = self.case / "derived"
        value = work_order(self.source, output_root)
        plan = self.execute(value, "--dry-run")
        self.assertEqual(plan.returncode, 0, plan.stderr)
        planned = json.loads(plan.stdout)
        orphan_run = Path(planned["layout"]["run_dir"])
        orphan_proxy = orphan_run / "artifacts" / "proxy-640x360-25fps.mp4"
        orphan_proxy.parent.mkdir(parents=True)
        run(
            [
                "ffmpeg",
                "-hide_banner",
                "-nostdin",
                "-loglevel",
                "error",
                "-i",
                str(self.source),
                "-vf",
                "scale=640:360,fps=25",
                "-an",
                "-c:v",
                "libx264",
                "-pix_fmt",
                "yuv420p",
                str(orphan_proxy),
            ]
        )
        orphan_proxy.chmod(0o444)

        completed = self.execute(value)
        self.assertEqual(completed.returncode, 0, completed.stderr)
        result = json.loads(completed.stdout)
        self.assertEqual(result["reuse"]["mode"], "none")
        self.assertNotEqual(result["layout"]["run_dir"], str(orphan_run))
        self.assertEqual(
            {step["status"] for step in result["steps"]}, {"completed"}
        )
        self.assertTrue(orphan_proxy.is_file())

    def test_recipe_writer_lock_rejects_concurrent_admission(self) -> None:
        output_root = self.case / "derived"
        value = work_order(self.source, output_root)
        plan = self.execute(value, "--dry-run")
        self.assertEqual(plan.returncode, 0, plan.stderr)
        recipe_dir = Path(json.loads(plan.stdout)["layout"]["recipe_dir"])
        recipe_dir.mkdir(parents=True)
        lock_path = recipe_dir / ".preprocess.lock"
        with lock_path.open("a+b") as handle:
            fcntl.flock(handle.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
            blocked = self.execute(value)
            fcntl.flock(handle.fileno(), fcntl.LOCK_UN)
        self.assertEqual(blocked.returncode, 2)
        self.assertIn(
            "holds the recipe lock", json.loads(blocked.stderr)["error"]["message"]
        )
        self.assertFalse((recipe_dir / "executions").exists())

    def test_stale_result_copy_fails_closed_and_replay_chain_stays_unique(self) -> None:
        output_root = self.case / "derived"
        value = work_order(self.source, output_root)
        executions: list[dict] = []
        for _ in range(3):
            completed = self.execute(value)
            self.assertEqual(completed.returncode, 0, completed.stderr)
            executions.append(json.loads(completed.stdout))
        run_ids = {
            result["processing_run"]["processing_run_id"] for result in executions
        }
        self.assertEqual(len(run_ids), 3)
        self.assertEqual(executions[0]["reuse"]["mode"], "none")
        self.assertTrue(
            all(
                result["reuse"]["mode"] == "verified_prior_result"
                for result in executions[1:]
            )
        )
        audio_inodes = {
            Path(
                next(
                    artifact["path"]
                    for artifact in result["artifacts"]
                    if artifact["artifact_kind"] == "audio_16khz_mono_flac"
                )
            ).stat().st_ino
            for result in executions
        }
        self.assertEqual(len(audio_inodes), 1)

        recipe_dir = Path(executions[0]["layout"]["recipe_dir"])
        stale_dir = recipe_dir / "executions" / ("run_preprocess_" + "f" * 32)
        stale_dir.mkdir()
        stale_result = stale_dir / "result.json"
        shutil.copyfile(executions[0]["result_path"], stale_result)
        stale_result.chmod(0o444)
        blocked = self.execute(value)
        self.assertEqual(blocked.returncode, 2)
        self.assertIn("layout or path", json.loads(blocked.stderr)["error"]["message"])

    def test_audio_only_input_skips_proxy_but_routes_silence(self) -> None:
        source = self.case / "audio-only.flac"
        run(
            [
                "ffmpeg",
                "-hide_banner",
                "-nostdin",
                "-loglevel",
                "error",
                "-f",
                "lavfi",
                "-i",
                "anullsrc=r=48000:cl=stereo:d=1.5",
                "-c:a",
                "flac",
                str(source),
            ]
        )
        output_root = self.case / "audio-derived"
        completed = self.execute(work_order(source, output_root))
        self.assertEqual(completed.returncode, 0, completed.stderr)
        result = json.loads(completed.stdout)
        self.assert_result_contract(result, "audio-only-result.contract.json")
        statuses = {step["name"]: step["status"] for step in result["steps"]}
        self.assertEqual(statuses["audio_flac"], "completed")
        self.assertEqual(statuses["proxy"], "not_applicable")
        self.assertEqual(statuses["routing"], "completed")
        self.assertEqual(
            result["routing"]["routing_candidates"]["asr"],
            "review_near_silent_candidate",
        )

    def test_video_only_input_skips_audio_and_still_makes_proxy(self) -> None:
        source = self.case / "video-only.mp4"
        run(
            [
                "ffmpeg",
                "-hide_banner",
                "-nostdin",
                "-loglevel",
                "error",
                "-f",
                "lavfi",
                "-i",
                "testsrc2=s=320x240:r=30:d=1.5",
                "-an",
                "-c:v",
                "libx264",
                "-preset",
                "ultrafast",
                "-pix_fmt",
                "yuv420p",
                str(source),
            ]
        )
        output_root = self.case / "video-derived"
        completed = self.execute(work_order(source, output_root))
        self.assertEqual(completed.returncode, 0, completed.stderr)
        result = json.loads(completed.stdout)
        self.assert_result_contract(result, "video-only-result.contract.json")
        statuses = {step["name"]: step["status"] for step in result["steps"]}
        self.assertEqual(statuses["audio_flac"], "not_applicable")
        self.assertEqual(statuses["proxy"], "completed")
        self.assertEqual(statuses["routing"], "completed")
        self.assertEqual(result["routing"]["routing_candidates"]["asr"], "skip_no_audio")

    def test_static_60fps_video_routes_silence_without_scene_packets(self) -> None:
        source = self.case / "static-silent.mkv"
        run(
            [
                "ffmpeg",
                "-hide_banner",
                "-nostdin",
                "-loglevel",
                "error",
                "-f",
                "lavfi",
                "-i",
                "color=c=black:s=320x240:r=60:d=2",
                "-f",
                "lavfi",
                "-i",
                "anullsrc=r=48000:cl=mono:d=2",
                "-c:v",
                "libx264",
                "-preset",
                "ultrafast",
                "-pix_fmt",
                "yuv420p",
                "-c:a",
                "aac",
                "-shortest",
                str(source),
            ]
        )
        completed = self.execute(work_order(source, self.case / "static-derived"))
        self.assertEqual(completed.returncode, 0, completed.stderr)
        result = json.loads(completed.stdout)
        self.assert_result_contract(result, "static-result.contract.json")
        self.assertEqual(result["routing"]["summary"]["scene_change_count"], 0)
        self.assertGreaterEqual(
            result["routing"]["summary"]["silence_interval_count"], 1
        )
        routing_step = next(
            step for step in result["steps"] if step["name"] == "routing"
        )
        filter_text = " ".join(routing_step["command"])
        self.assertIn("sc_pass=0", filter_text)
        self.assertNotIn("sc_pass=1", filter_text)

    def test_expected_hash_mismatch_writes_no_output(self) -> None:
        output_root = self.case / "derived"
        value = work_order(self.source, output_root, expected_sha256="0" * 64)
        completed = self.execute(value)
        self.assertEqual(completed.returncode, 2)
        failure = json.loads(completed.stderr)
        self.assertEqual(failure["status"], "failed")
        self.assertIn("SHA-256 mismatch", failure["error"]["message"])
        self.assertFalse(output_root.exists())

    def test_tmp_output_root_is_rejected(self) -> None:
        value = work_order(self.source, Path("/tmp/himr-pipeline-test-output"))
        completed = self.execute(value, "--dry-run")
        self.assertEqual(completed.returncode, 2)
        failure = json.loads(completed.stderr)
        self.assertIn("may not be under /tmp", failure["error"]["message"])

    def test_filesystem_root_and_existing_file_output_are_rejected(self) -> None:
        root_completed = self.execute(work_order(self.source, Path("/")), "--dry-run")
        self.assertEqual(root_completed.returncode, 2)
        self.assertIn(
            "may not be the filesystem root",
            json.loads(root_completed.stderr)["error"]["message"],
        )

        output_file = self.case / "not-a-directory"
        output_file.write_text("sentinel\n", encoding="utf-8")
        file_completed = self.execute(work_order(self.source, output_file), "--dry-run")
        self.assertEqual(file_completed.returncode, 2)
        self.assertIn(
            "must identify a directory or a new path",
            json.loads(file_completed.stderr)["error"]["message"],
        )

    def test_non_s16_audio_profile_is_rejected_before_output(self) -> None:
        output_root = self.case / "derived"
        value = work_order(self.source, output_root)
        value["profile"]["audio_sample_format"] = "s32"
        completed = self.execute(value)
        self.assertEqual(completed.returncode, 2)
        self.assertIn(
            "audio_sample_format to be s16",
            json.loads(completed.stderr)["error"]["message"],
        )
        self.assertFalse(output_root.exists())

    def test_profile_extras_and_invalid_catalog_time_are_rejected(self) -> None:
        output_root = self.case / "derived"
        extra_work_order = work_order(self.source, output_root)
        extra_work_order["unexpected"] = True
        completed = self.execute(extra_work_order, "--dry-run")
        self.assertEqual(completed.returncode, 2)
        self.assertIn("unknown keys: unexpected", json.loads(completed.stderr)["error"]["message"])

        extra_profile = work_order(self.source, output_root)
        extra_profile["profile"]["unexpected"] = True
        completed = self.execute(extra_profile, "--dry-run")
        self.assertEqual(completed.returncode, 2)
        self.assertIn("unknown keys: unexpected", json.loads(completed.stderr)["error"]["message"])

        invalid_time = work_order(self.source, output_root)
        invalid_time["source"]["first_cataloged_at"] = "yesterday"
        completed = self.execute(invalid_time, "--dry-run")
        self.assertEqual(completed.returncode, 2)
        self.assertIn("RFC 3339", json.loads(completed.stderr)["error"]["message"])
        self.assertFalse(output_root.exists())


if __name__ == "__main__":
    unittest.main()
