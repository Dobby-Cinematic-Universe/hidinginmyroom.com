from __future__ import annotations

import copy
import hashlib
import json
import shutil
import subprocess
import unittest
from pathlib import Path


REPOSITORY_ROOT = Path(__file__).resolve().parents[2]
PIPELINE_ROOT = REPOSITORY_ROOT / "pipeline"
PROGRAM = PIPELINE_ROOT / "audio_fingerprint.py"
TEST_ROOT = PIPELINE_ROOT / ".test-work" / "audio-fingerprint"


def run(command: list[str], *, check: bool = True) -> subprocess.CompletedProcess[str]:
    return subprocess.run(command, stdin=subprocess.DEVNULL, stdout=subprocess.PIPE,
                          stderr=subprocess.PIPE, text=True, encoding="utf-8",
                          errors="replace", check=check)


def digest(path: Path) -> str:
    value = hashlib.sha256()
    with path.open("rb") as handle:
        while chunk := handle.read(1024 * 1024):
            value.update(chunk)
    return value.hexdigest()


def write_json(path: Path, value: object) -> None:
    path.write_text(json.dumps(value, indent=2) + "\n", encoding="utf-8")


def validate_schema(instance: object, schema_name: str) -> None:
    try:
        import jsonschema
    except ImportError:
        return
    schema = json.loads((PIPELINE_ROOT / "schemas" / schema_name).read_text(encoding="utf-8"))
    jsonschema.Draft202012Validator.check_schema(schema)
    jsonschema.validate(instance, schema)


@unittest.skipUnless(shutil.which("ffmpeg"), "FFmpeg is required")
class AudioFingerprintTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        TEST_ROOT.mkdir(parents=True, exist_ok=True)
        cls.engine_pin = json.loads(run(["python3", str(PROGRAM), "inspect-engine"]).stdout)

    @classmethod
    def tearDownClass(cls) -> None:
        shutil.rmtree(TEST_ROOT, ignore_errors=True)

    def setUp(self) -> None:
        self.case = TEST_ROOT / self._testMethodName
        self.case.mkdir(parents=True, exist_ok=True)
        self.audio = (self.case / "normalized.flac").resolve()
        run([self.engine_pin["executable"], "-hide_banner", "-nostdin", "-loglevel", "error",
             "-f", "lavfi", "-i", "sine=frequency=440:sample_rate=16000:duration=35",
             "-ac", "1", "-ar", "16000", "-sample_fmt", "s16", "-c:a", "flac",
             str(self.audio)])
        self.audio.chmod(0o444)

    def order(self, *, selection: dict | None = None, output_name: str = "output") -> dict:
        audio_sha = digest(self.audio)
        return {
            "schema_version": 1,
            "job_id": f"fingerprint-{output_name}",
            "input": {
                "path": str(self.audio), "expected_sha256": audio_sha,
                "expected_byte_count": self.audio.stat().st_size,
                "media_id": f"media_sha256_{audio_sha}",
                "artifact_id": "artifact_normalized_audio_fixture",
                "parent_processing_run_id": "run_preprocess_fixture",
                "duration_ms": 35_000, "sample_rate_hz": 16_000,
                "channels": 1, "sample_format": "s16",
            },
            "engine": self.engine_pin,
            "fingerprint": {
                "algorithm": 1, "raw_format": "ffmpeg_chromaprint_fp_format_raw",
                "threads": 1, "timeout_seconds": 30,
                "selection": selection or {"mode": "full_track"},
            },
            "catalog_context": {"recording_id": "recording_fixture", "rendition_id": "rendition_fixture"},
            "output": {"root": str((self.case / output_name).resolve())},
        }

    def execute(self, order: dict, *arguments: str) -> subprocess.CompletedProcess[str]:
        path = self.case / f"order-{len(list(self.case.glob('order-*.json')))}.json"
        write_json(path, order)
        return run(["python3", str(PROGRAM), "run", "--work-order", str(path), *arguments], check=False)

    def order_for_audio(
        self,
        audio: Path,
        *,
        output_name: str,
        recording_id: str,
        rendition_id: str,
        engine_pin: dict | None = None,
    ) -> dict:
        order = self.order(output_name=output_name)
        audio_sha = digest(audio)
        order["job_id"] = f"fingerprint-{output_name}"
        order["input"].update({
            "path": str(audio),
            "expected_sha256": audio_sha,
            "expected_byte_count": audio.stat().st_size,
            "media_id": f"media_sha256_{audio_sha}",
            "artifact_id": f"artifact_normalized_audio_{output_name}",
            "parent_processing_run_id": f"run_preprocess_{output_name}",
        })
        order["engine"] = engine_pin or self.engine_pin
        order["catalog_context"] = {
            "recording_id": recording_id,
            "rendition_id": rendition_id,
        }
        return order

    def compare_v2_order(self, query: dict, candidate: dict, output_name: str = "comparisons-v2") -> dict:
        return {
            "schema_version": 2,
            "job_id": "compare-v2-fixture",
            "method": "exact_raw_bytes_v2",
            "query": {
                "role": "query",
                "result_path": query["result_path"],
                "expected_result_sha256": digest(Path(query["result_path"])),
                "fingerprint_id": query["fingerprints"][0]["fingerprint_id"],
            },
            "candidate": {
                "role": "candidate",
                "result_path": candidate["result_path"],
                "expected_result_sha256": digest(Path(candidate["result_path"])),
                "fingerprint_id": candidate["fingerprints"][0]["fingerprint_id"],
            },
            "output": {"root": str((self.case / output_name).resolve())},
        }

    def run_compare_v2(self, order: dict, *arguments: str) -> subprocess.CompletedProcess[str]:
        path = self.case / f"compare-v2-{len(list(self.case.glob('compare-v2-*.json')))}.json"
        write_json(path, order)
        return run(
            ["python3", str(PROGRAM), "compare-v2", "--work-order", str(path), *arguments],
            check=False,
        )

    def test_full_track_is_sealed_raw_and_recipe_is_distinct_from_execution(self) -> None:
        completed = self.execute(self.order())
        self.assertEqual(completed.returncode, 0, completed.stderr)
        result = json.loads(completed.stdout)
        validate_schema(result, "audio-fingerprint-result.schema.json")
        self.assertEqual(result["status"], "completed")
        self.assertNotEqual(result["recipe_id"], result["processing_run"]["processing_run_id"])
        self.assertEqual(result["expanded_windows"], [{"window_id": "full_track", "window_kind": "full_track", "start_ms": 0, "end_ms": 35_000}])
        item = result["fingerprints"][0]
        self.assertGreater(item["fingerprint_word_count"], 0)
        self.assertEqual(item["artifact"]["byte_count"] % 4, 0)
        artifact = Path(item["artifact"]["path"])
        self.assertEqual(digest(artifact), item["artifact"]["sha256"])
        self.assertEqual(artifact.stat().st_mode & 0o222, 0)
        self.assertEqual(Path(result["result_path"]).stat().st_mode & 0o222, 0)

    def test_recipe_is_deterministic_but_execution_is_unique(self) -> None:
        first = json.loads(self.execute(self.order(output_name="one")).stdout)
        second = json.loads(self.execute(self.order(output_name="two")).stdout)
        self.assertEqual(first["recipe_id"], second["recipe_id"])
        self.assertNotEqual(first["processing_run"]["processing_run_id"], second["processing_run"]["processing_run_id"])
        self.assertEqual(first["fingerprints"][0]["artifact"]["sha256"], second["fingerprints"][0]["artifact"]["sha256"])

    def test_fixed_chunks_use_half_open_boundaries_and_flag_partial_short_tail(self) -> None:
        order = self.order(selection={"mode": "fixed_chunks", "chunk_duration_ms": 15_000,
                                      "hop_ms": 15_000, "include_partial_tail": True,
                                      "minimum_tail_ms": 1_000})
        result = json.loads(self.execute(order).stdout)
        self.assertEqual([(row["start_ms"], row["end_ms"], row["window_kind"]) for row in result["expanded_windows"]],
                         [(0, 15_000, "fixed_chunk"), (15_000, 30_000, "fixed_chunk"),
                          (30_000, 35_000, "partial_tail_chunk")])
        self.assertIn("partial_tail_chunk", result["fingerprints"][2]["quality_flags"])
        self.assertIn("short_window_under_10s", result["fingerprints"][2]["quality_flags"])

    def test_unknown_field_window_boundary_hash_and_unsafe_path_fail_closed(self) -> None:
        unknown = self.order(output_name="unknown"); unknown["surprise"] = True
        self.assertIn("unknown", self.execute(unknown).stderr)
        boundary = self.order(output_name="boundary")
        boundary["fingerprint"]["selection"] = {"mode": "explicit_windows", "windows": [{"window_id": "bad", "start_ms": 34_000, "end_ms": 35_001}]}
        self.assertIn("start < end <= duration", self.execute(boundary).stderr)
        tampered = self.order(output_name="tampered")
        tampered["input"]["expected_sha256"] = "0" * 64
        tampered["input"]["media_id"] = f"media_sha256_{'0' * 64}"
        self.assertIn("bytes differ", self.execute(tampered).stderr)
        unsafe = self.order(output_name="unsafe"); unsafe["output"]["root"] = "/tmp/himr-fingerprint-unsafe"
        self.assertIn("must not be under /tmp", self.execute(unsafe).stderr)
        alias = self.case / "normalized-alias.flac"
        alias.symlink_to(self.audio)
        symlinked = self.order(output_name="symlinked")
        symlinked["input"]["path"] = str(alias)
        self.assertIn("without symlinks", self.execute(symlinked).stderr)

    def test_dry_run_creates_nothing(self) -> None:
        order = self.order(output_name="dry")
        result = json.loads(self.execute(order, "--dry-run").stdout)
        validate_schema(result, "audio-fingerprint-result.schema.json")
        self.assertEqual(result["status"], "planned")
        self.assertFalse(Path(order["output"]["root"]).exists())

    def test_conservative_compare_emits_review_candidate_not_probability(self) -> None:
        extraction = json.loads(self.execute(self.order(output_name="extract", selection={
            "mode": "explicit_windows", "windows": [
                {"window_id": "left", "start_ms": 0, "end_ms": 15_000},
                {"window_id": "right", "start_ms": 15_000, "end_ms": 30_000},
            ]})).stdout)
        sides = []
        for role, item in zip(("query", "candidate"), extraction["fingerprints"], strict=True):
            sides.append({
                "role": role, "path": item["artifact"]["path"],
                "expected_sha256": item["artifact"]["sha256"],
                "expected_byte_count": item["artifact"]["byte_count"],
                "artifact_id": item["artifact"]["artifact_id"],
                "fingerprint_id": item["fingerprint_id"], "media_id": extraction["input"]["media_id"],
                "implementation_version": item["implementation_version"], "algorithm": item["algorithm"],
                "raw_format": item["raw_format"], "sample_rate_hz": item["sample_rate_hz"],
                "channels": item["channels"], "window_kind": item["window_kind"],
                "start_ms": item["start_ms"], "end_ms": item["end_ms"],
                "fingerprint_word_count": item["fingerprint_word_count"], "quality_flags": item["quality_flags"],
            })
        order = {
            "schema_version": 1, "job_id": "compare-fixture", "method": "exact_raw_bytes_v1",
            "query": sides[0], "candidate": sides[1],
            "catalog_context": {
                "query": {"recording_id": "recording_fixture", "rendition_id": "rendition_fixture"},
                "candidate": {"recording_id": "recording_fixture", "rendition_id": "rendition_fixture"},
            },
            "output": {"root": str((self.case / "comparisons").resolve())},
        }
        path = self.case / "compare.json"; write_json(path, order)
        planned = run(
            ["python3", str(PROGRAM), "compare", "--work-order", str(path), "--dry-run"],
            check=False,
        )
        self.assertEqual(planned.returncode, 0, planned.stderr)
        planned_result = json.loads(planned.stdout)
        validate_schema(planned_result, "audio-fingerprint-compare-result.schema.json")
        self.assertEqual(planned_result["status"], "planned")
        self.assertIsNone(planned_result["comparison"])
        completed = run(["python3", str(PROGRAM), "compare", "--work-order", str(path)], check=False)
        self.assertEqual(completed.returncode, 0, completed.stderr)
        result = json.loads(completed.stdout)
        validate_schema(result, "audio-fingerprint-compare-result.schema.json")
        self.assertIsNone(result["comparison"]["calibrated_probability"])
        self.assertTrue(result["comparison"]["requires_human_review"])
        self.assertFalse(result["comparison"]["relationship_asserted"])
        self.assertEqual(result["comparison"]["decision_state"], "candidate")
        bad = copy.deepcopy(order); bad["candidate"]["unexpected"] = 1; write_json(path, bad)
        rejected = run(["python3", str(PROGRAM), "compare", "--work-order", str(path)], check=False)
        self.assertNotEqual(rejected.returncode, 0)
        self.assertIn("unknown", rejected.stderr)

    def test_v2_compares_cross_recording_equal_raw_bytes_across_distinct_recipes(self) -> None:
        candidate_audio = (self.case / "candidate-with-metadata.flac").resolve()
        run([
            self.engine_pin["executable"], "-hide_banner", "-nostdin", "-loglevel", "error",
            "-f", "lavfi", "-i", "sine=frequency=440:sample_rate=16000:duration=35",
            "-metadata", "title=candidate-recording", "-ac", "1", "-ar", "16000",
            "-sample_fmt", "s16", "-c:a", "flac", str(candidate_audio),
        ])
        candidate_audio.chmod(0o444)
        self.assertNotEqual(digest(self.audio), digest(candidate_audio))
        query = json.loads(self.execute(self.order_for_audio(
            self.audio, output_name="v2-query", recording_id="recording_query",
            rendition_id="rendition_query",
        )).stdout)
        candidate = json.loads(self.execute(self.order_for_audio(
            candidate_audio, output_name="v2-candidate", recording_id="recording_candidate",
            rendition_id="rendition_candidate",
        )).stdout)
        self.assertNotEqual(query["recipe_id"], candidate["recipe_id"])
        self.assertNotEqual(
            query["fingerprints"][0]["implementation_version"],
            candidate["fingerprints"][0]["implementation_version"],
        )
        self.assertEqual(
            query["fingerprints"][0]["artifact"]["sha256"],
            candidate["fingerprints"][0]["artifact"]["sha256"],
        )

        order = self.compare_v2_order(query, candidate)
        try:
            import jsonschema
        except ImportError:
            jsonschema = None
        if jsonschema is not None:
            work_schema = json.loads(
                (PIPELINE_ROOT / "schemas" / "audio-fingerprint-compare-work-order-v2.schema.json").read_text(encoding="utf-8")
            )
            swapped_roles = copy.deepcopy(order)
            swapped_roles["query"]["role"] = "candidate"
            swapped_roles["candidate"]["role"] = "query"
            self.assertFalse(jsonschema.Draft202012Validator(work_schema).is_valid(swapped_roles))
        first_plan = self.run_compare_v2(order, "--dry-run")
        second_plan = self.run_compare_v2(order, "--dry-run")
        self.assertEqual(first_plan.returncode, 0, first_plan.stderr)
        self.assertEqual(second_plan.returncode, 0, second_plan.stderr)
        first_planned = json.loads(first_plan.stdout)
        second_planned = json.loads(second_plan.stdout)
        validate_schema(first_planned, "audio-fingerprint-compare-result-v2.schema.json")
        self.assertEqual(first_planned["recipe_sha256"], second_planned["recipe_sha256"])
        self.assertFalse(Path(order["output"]["root"]).exists())
        if jsonschema is not None:
            result_schema = json.loads(
                (PIPELINE_ROOT / "schemas" / "audio-fingerprint-compare-result-v2.schema.json").read_text(encoding="utf-8")
            )
            invalid_plan = copy.deepcopy(first_planned)
            invalid_plan["dry_run"] = False
            self.assertFalse(jsonschema.Draft202012Validator(result_schema).is_valid(invalid_plan))

        completed = self.run_compare_v2(order)
        self.assertEqual(completed.returncode, 0, completed.stderr)
        result = json.loads(completed.stdout)
        validate_schema(result, "audio-fingerprint-compare-result-v2.schema.json")
        if jsonschema is not None:
            invalid_completed = copy.deepcopy(result)
            invalid_completed["comparison"] = None
            self.assertFalse(jsonschema.Draft202012Validator(result_schema).is_valid(invalid_completed))
            invalid_roles = copy.deepcopy(result)
            invalid_roles["query"]["role"] = "candidate"
            self.assertFalse(jsonschema.Draft202012Validator(result_schema).is_valid(invalid_roles))
        self.assertTrue(result["comparison"]["exact_raw_equal"])
        self.assertIn("cross_recording", result["comparison"]["quality_flags"])
        self.assertIn("cross_input_media", result["comparison"]["quality_flags"])
        self.assertIn("cross_extraction_recipe", result["comparison"]["quality_flags"])
        self.assertEqual(result["comparison"]["calibration_state"], "not_calibrated")
        self.assertIsNone(result["comparison"]["calibrated_probability"])
        self.assertEqual(result["comparison"]["decision_state"], "candidate")
        self.assertTrue(result["comparison"]["requires_human_review"])
        self.assertFalse(result["comparison"]["relationship_asserted"])
        self.assertEqual(result["visibility"], "private")
        self.assertEqual(result["publication_authority"], "none")

        # Historical v1 remains intentionally recipe-qualified and rejects this pair.
        v1_sides = []
        for role, extraction in (("query", query), ("candidate", candidate)):
            item = extraction["fingerprints"][0]
            v1_sides.append({
                "role": role, "path": item["artifact"]["path"],
                "expected_sha256": item["artifact"]["sha256"],
                "expected_byte_count": item["artifact"]["byte_count"],
                "artifact_id": item["artifact"]["artifact_id"],
                "fingerprint_id": item["fingerprint_id"],
                "media_id": extraction["input"]["media_id"],
                "implementation_version": item["implementation_version"],
                "algorithm": item["algorithm"], "raw_format": item["raw_format"],
                "sample_rate_hz": item["sample_rate_hz"], "channels": item["channels"],
                "window_kind": item["window_kind"], "start_ms": item["start_ms"],
                "end_ms": item["end_ms"],
                "fingerprint_word_count": item["fingerprint_word_count"],
                "quality_flags": item["quality_flags"],
            })
        v1_order = {
            "schema_version": 1, "job_id": "v1-history", "method": "exact_raw_bytes_v1",
            "query": v1_sides[0], "candidate": v1_sides[1], "catalog_context": None,
            "output": {"root": str((self.case / "v1-history-output").resolve())},
        }
        v1_path = self.case / "v1-history.json"
        write_json(v1_path, v1_order)
        v1_rejected = run(
            ["python3", str(PROGRAM), "compare", "--work-order", str(v1_path)],
            check=False,
        )
        self.assertNotEqual(v1_rejected.returncode, 0)
        self.assertIn("identical implementation_version", v1_rejected.stderr)

    def test_v2_revalidates_envelope_selection_and_current_artifact_bytes(self) -> None:
        query = json.loads(self.execute(self.order_for_audio(
            self.audio, output_name="tamper-query", recording_id="recording_query",
            rendition_id="rendition_query",
        )).stdout)
        candidate = json.loads(self.execute(self.order_for_audio(
            self.audio, output_name="tamper-candidate", recording_id="recording_candidate",
            rendition_id="rendition_candidate",
        )).stdout)
        order = self.compare_v2_order(query, candidate)
        bad_digest = copy.deepcopy(order)
        bad_digest["query"]["expected_result_sha256"] = "0" * 64
        rejected = self.run_compare_v2(bad_digest)
        self.assertNotEqual(rejected.returncode, 0)
        self.assertIn("expected SHA-256", rejected.stderr)
        missing = copy.deepcopy(order)
        missing["candidate"]["fingerprint_id"] = "fingerprint_missing"
        rejected = self.run_compare_v2(missing)
        self.assertNotEqual(rejected.returncode, 0)
        self.assertIn("select exactly one", rejected.stderr)

        artifact_path = Path(candidate["fingerprints"][0]["artifact"]["path"])
        artifact_path.chmod(0o644)
        artifact_path.write_bytes(artifact_path.read_bytes() + b"tamper")
        artifact_path.chmod(0o444)
        rejected = self.run_compare_v2(order)
        self.assertNotEqual(rejected.returncode, 0)
        self.assertIn("current bytes differ", rejected.stderr)

    def test_v2_rejects_mismatched_pinned_engine_builds(self) -> None:
        wrapper = (self.case / "ffmpeg-wrapper.py").resolve()
        wrapper.write_text(
            "#!/usr/bin/env python3\n"
            "import os, sys\n"
            f"target = {self.engine_pin['executable']!r}\n"
            "os.execv(target, [target, *sys.argv[1:]])\n",
            encoding="utf-8",
        )
        wrapper.chmod(0o755)
        wrapper_pin = json.loads(run([
            "python3", str(PROGRAM), "inspect-engine", "--executable", str(wrapper)
        ]).stdout)
        self.assertNotEqual(self.engine_pin["expected_sha256"], wrapper_pin["expected_sha256"])
        query = json.loads(self.execute(self.order_for_audio(
            self.audio, output_name="engine-query", recording_id="recording_query",
            rendition_id="rendition_query",
        )).stdout)
        candidate = json.loads(self.execute(self.order_for_audio(
            self.audio, output_name="engine-candidate", recording_id="recording_candidate",
            rendition_id="rendition_candidate", engine_pin=wrapper_pin,
        )).stdout)
        rejected = self.run_compare_v2(self.compare_v2_order(query, candidate))
        self.assertNotEqual(rejected.returncode, 0)
        self.assertIn("identical pinned engine/build identity", rejected.stderr)


if __name__ == "__main__":
    unittest.main()
