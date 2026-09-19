from __future__ import annotations

import copy
import hashlib
import json
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path

from jsonschema import Draft202012Validator


REPOSITORY_ROOT = Path(__file__).resolve().parents[2]
PIPELINE_ROOT = REPOSITORY_ROOT / "pipeline"
CORPUS_SOURCE_ROOT = REPOSITORY_ROOT / "corpus" / "src"
for path in (str(PIPELINE_ROOT), str(CORPUS_SOURCE_ROOT)):
    if path not in sys.path:
        sys.path.insert(0, path)

import longform_transcript_assembler as assembler  # noqa: E402
from himr_corpus.longform_asr_planner import (  # noqa: E402
    MANIFEST_KIND,
    build_longform_asr_plan,
    duration_ms_for_samples,
)
from pipeline.gpu import longform_asr_execution_v1 as executor  # noqa: E402


FIXTURE = (
    PIPELINE_ROOT
    / "tests"
    / "fixtures"
    / "longform_assembler"
    / "two-span-overlap.json"
)
BINDINGS_SCHEMA = PIPELINE_ROOT / "schemas" / "longform-span-transcript-bindings.schema.json"
OUTPUT_SCHEMA = PIPELINE_ROOT / "schemas" / "longform-recording-transcript.schema.json"


def digest(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def execution_lineage() -> dict:
    return {
        "longform_scope_status": "candidate_unsoaked",
        "production_profile": {
            "profile_id": "gpuprofile_assembler_fixture",
            "identity_sha256": "a" * 64,
            "physical_sha256": "b" * 64,
        },
        "runtime_admission": {
            "receipt_id": "gpurtv2_assembler_fixture",
            "identity_sha256": "c" * 64,
            "physical_sha256": "d" * 64,
            "status": "candidate",
        },
        "model": {
            "repository": "fixture/faster-whisper",
            "revision": "fixture-model",
            "identity_sha256": "f" * 64,
        },
    }


class FakeEngine:
    def __init__(self, segments: list[dict]) -> None:
        self.segments = copy.deepcopy(segments)

    def transcribe(self, request: executor.SpanEngineRequest) -> dict:
        return {
            "engine": {
                "library": "fixture-engine",
                "library_version": "1",
                "model_revision": "fixture-model",
                "model_identity_sha256": "f" * 64,
                "input_decoder": {
                    "kind": "fixture_in_memory",
                    "executable_path": None,
                    "executable_sha256": None,
                    "execution_mode": "fixture_logical_view",
                    "persistent_audio_chunks": False,
                },
            },
            "language": {
                "value": "en",
                "selection_basis": "forced_by_execution_work_order",
                "detection_performed": False,
                "probability_raw": None,
            },
            "duration_samples": request.source.analysis_sample_count,
            "segments": copy.deepcopy(self.segments),
        }


class CoreOwnershipIndexTests(unittest.TestCase):
    def test_respects_half_open_boundaries_and_rejects_edges(self) -> None:
        def span(ordinal: int, start: int, end: int) -> assembler.Span:
            return assembler.Span(
                span_id=f"lfspan_{ordinal:032x}",
                ordinal=ordinal,
                analysis_start=start,
                analysis_end=end,
                core_start=start,
                core_end=end,
                boundary_reason="fixture",
                boundary_confidence_millionths=None,
            )

        spans = (span(0, 0, 100), span(1, 100, 200), span(2, 200, 300))
        index = assembler.CoreOwnershipIndex.build(spans)

        self.assertEqual(
            [0, 0, 1, 1, 2, 2],
            [
                index.owner_for_anchor(anchor).ordinal
                for anchor in (0, 99, 100, 199, 200, 299)
            ],
        )
        for anchor in (-1, 300):
            with self.subTest(anchor=anchor):
                with self.assertRaisesRegex(
                    assembler.AssemblyError, "outside the parent timeline"
                ):
                    index.owner_for_anchor(anchor)

        for malformed in (
            (span(0, 0, 100), span(1, 101, 200)),
            (span(0, 0, 101), span(1, 100, 200)),
        ):
            with self.subTest(malformed=malformed):
                with self.assertRaisesRegex(assembler.AssemblyError, "exactly tile"):
                    assembler.CoreOwnershipIndex.build(malformed)
        with self.assertRaisesRegex(assembler.AssemblyError, "at least one span"):
            assembler.CoreOwnershipIndex.build(())

    def test_one_sample_plan_with_zero_rounded_milliseconds_is_accepted(self) -> None:
        manifest = {
            "boundary_candidates": [],
            "kind": MANIFEST_KIND,
            "recording": {
                "input": {
                    "artifact_id": "artifact_one_sample",
                    "byte_count": 1,
                    "channels": 1,
                    "duration_ms": 0,
                    "path": "/fixture/one-sample.flac",
                    "sample_rate_hz": 16_000,
                    "sha256": "0" * 64,
                    "total_samples": 1,
                },
                "media_id": "media_one_sample",
                "recording_id": "recording_one_sample",
            },
            "schema_version": 1,
        }
        policy = {
            "adaptive": {
                "boundary_search_samples": 0,
                "max_core_samples": 1,
                "max_span_count": 1,
                "min_core_samples": 1,
                "padding_samples": 0,
                "target_core_samples": 1,
            },
            "direct_max_samples": 1,
            "kind": "himr_longform_asr_planning_policy",
            "schema_version": 1,
        }
        plan = build_longform_asr_plan(manifest, policy)

        replayed, spans = assembler._validate_plan(plan)

        self.assertEqual(replayed, plan)
        self.assertEqual(spans[0].core_samples, 1)


class CompactWordOrderingTests(unittest.TestCase):
    def test_source_word_order_survives_regressing_model_timestamps(self) -> None:
        span = assembler.Span(
            span_id="lfspan_" + "1" * 32,
            ordinal=0,
            analysis_start=0,
            analysis_end=300,
            core_start=0,
            core_end=300,
            boundary_reason="fixture",
            boundary_confidence_millionths=None,
        )

        def word(
            ordinal: int,
            start: int,
            end: int,
            text: str,
            anomaly_flags: tuple[str, ...] = (),
        ) -> assembler.Unit:
            return assembler.Unit(
                unit_id=assembler.stable_id("lfunit", ordinal, start, end, text),
                unit_kind="word",
                span=span,
                segment_ordinal=0,
                word_ordinal=ordinal,
                text=text,
                normalized_text=assembler.normalize_piece(text),
                start_sample=start,
                end_sample=end,
                anchor_sample=start + (end - start) // 2,
                owner_span_id=span.span_id,
                core_owned=True,
                core_margin_samples=start,
                raw_probability=None,
                anomaly_flags=anomaly_flags,
                retained=True,
            )

        candidate = assembler.SegmentCandidate(
            span=span,
            local_ordinal=0,
            projected_start_sample=50,
            projected_end_sample=200,
            original_text=" first second",
            units=[
                word(0, 100, 200, " first"),
                word(1, 50, 90, " second", ("start_regresses_from_previous",)),
            ],
        )
        loaded = assembler.LoadedSpan(
            span=span,
            status="completed",
            failure_code=None,
            result_path=None,
            result_sha256=None,
            result_identity_sha256=None,
            transcript_path=None,
            transcript_sha256=None,
            transcript_identity_sha256=None,
            segments=[candidate],
        )

        rows = assembler._output_segments([loaded], [])

        self.assertEqual(rows[0]["text"], "first second")
        self.assertEqual(
            [word_row["text"] for word_row in rows[0]["words"]],
            [" first", " second"],
        )
        self.assertEqual(
            [
                (word_row["start_sample"], word_row["end_sample"])
                for word_row in rows[0]["words"]
            ],
            [(100, 200), (50, 90)],
        )
        self.assertEqual(
            rows[0]["words"][1]["anomaly_flags"],
            ["start_regresses_from_previous"],
        )


class LongformTranscriptAssemblerTests(unittest.TestCase):
    def setUp(self) -> None:
        fixture = json.loads(FIXTURE.read_text(encoding="utf-8"))
        test_root = PIPELINE_ROOT / ".test-work"
        test_root.mkdir(parents=True, exist_ok=True)
        self.temporary = tempfile.TemporaryDirectory(
            prefix="longform-assembler-", dir=test_root
        )
        self.root = Path(self.temporary.name)
        self.source = self.root / "parent.flac"
        self.source.write_bytes(b"tiny-parent-fixture")
        total_samples = fixture["total_samples"]
        manifest = {
            "boundary_candidates": [],
            "kind": MANIFEST_KIND,
            "recording": {
                "input": {
                    "artifact_id": "artifact_fixture_parent",
                    "byte_count": self.source.stat().st_size,
                    "channels": 1,
                    "duration_ms": duration_ms_for_samples(total_samples),
                    "path": str(self.source),
                    "sample_rate_hz": 16_000,
                    "sha256": digest(self.source),
                    "total_samples": total_samples,
                },
                "media_id": "media_fixture_parent",
                "recording_id": "recording_fixture_parent",
            },
            "schema_version": 1,
        }
        self.plan = build_longform_asr_plan(manifest, fixture["planning_policy"])
        self.assertEqual(2, len(self.plan["spans"]))
        self.plan_path = self.root / "plan.json"
        self.plan_path.write_bytes(assembler.pretty_bytes(self.plan))
        self.bindings_path = self.root / "bindings.json"
        self.rows: list[dict] = []
        self.source_artifacts: list[Path] = []
        for span, segments in zip(
            self.plan["spans"], fixture["span_segments"], strict=True
        ):
            work_order = executor.make_work_order_from_plan(
                self.plan,
                span_ordinal=span["ordinal"],
                output_root=self.root / "results",
                execution_lineage=execution_lineage(),
            )
            bundle = executor.execute_work_order(
                work_order,
                FakeEngine(segments),
                utc_clock=lambda: "2026-08-30T00:00:00Z",
                monotonic_clock=iter((1.0, 2.0)).__next__,
                attempt_id=f"attempt_fixture_{span['ordinal']}",
            )
            materialized = executor.materialize_result_bundle(bundle)
            result_path = Path(materialized["result_path"])
            transcript_path = Path(materialized["transcript_path"])
            self.source_artifacts.extend((result_path, transcript_path))
            self.rows.append(
                {
                    "failure_code": None,
                    "result": {"path": str(result_path), "sha256": digest(result_path)},
                    "span_id": span["span_id"],
                    "status": "completed",
                    "transcript": {
                        "path": str(transcript_path),
                        "sha256": digest(transcript_path),
                    },
                }
            )
        self.write_bindings(self.rows)

    def tearDown(self) -> None:
        self.temporary.cleanup()

    def write_bindings(self, rows: list[dict]) -> None:
        bindings = {
            "kind": assembler.BINDINGS_KIND,
            "schema_version": 1,
            "parent_manifest_sha256": digest(self.plan_path),
            "spans": rows,
            "policy": dict(assembler.BINDINGS_POLICY),
        }
        self.bindings_path.write_bytes(assembler.pretty_bytes(bindings))

    def assemble(self) -> dict:
        return assembler.load_and_assemble(self.plan_path, self.bindings_path)

    def test_projects_exact_samples_and_reconciles_overlap_deterministically(self) -> None:
        before = {path: digest(path) for path in self.source_artifacts}
        first = self.assemble()
        second = self.assemble()
        self.assertEqual(first, second)
        self.assertTrue(first["coverage"]["complete"])
        self.assertEqual(2, first["counts"]["duplicate_group_count"])
        self.assertEqual("consistent", first["boundaries"][0]["status"])
        words = [word for segment in first["segments"] for word in segment["words"]]
        self.assertEqual([" hello", " same", " right"], [word["text"] for word in words])
        self.assertEqual(
            [(340, 360), (390, 420), (430, 450)],
            [(word["start_sample"], word["end_sample"]) for word in words],
        )
        self.assertTrue(all("start_ms" not in word for word in words))
        self.assertEqual(
            "machine_generated_unreviewed_not_verified_quotation_v1",
            first["disclaimer"]["code"],
        )
        self.assertEqual(before, {path: digest(path) for path in self.source_artifacts})

    def test_bindings_and_output_match_their_public_json_schemas(self) -> None:
        bindings_schema = json.loads(BINDINGS_SCHEMA.read_text(encoding="utf-8"))
        output_schema = json.loads(OUTPUT_SCHEMA.read_text(encoding="utf-8"))
        Draft202012Validator.check_schema(bindings_schema)
        Draft202012Validator.check_schema(output_schema)
        Draft202012Validator(bindings_schema).validate(
            json.loads(self.bindings_path.read_text(encoding="utf-8"))
        )
        Draft202012Validator(output_schema).validate(self.assemble())

    def test_pending_span_is_explicit_partial_coverage_not_false_no_speech(self) -> None:
        rows = copy.deepcopy(self.rows)
        rows[1] = {
            "failure_code": None,
            "result": None,
            "span_id": rows[1]["span_id"],
            "status": "pending",
            "transcript": None,
        }
        self.write_bindings(rows)
        value = self.assemble()
        self.assertFalse(value["coverage"]["complete"])
        self.assertEqual(400, value["coverage"]["status_sample_counts"]["pending"])
        self.assertEqual("unassessed", value["boundaries"][0]["status"])
        self.assertIn(
            "missing_right_completed_transcript",
            value["boundaries"][0]["conflict_flags"],
        )

    def test_any_untimed_word_falls_back_to_one_segment_unit(self) -> None:
        total_samples = 800
        source_body = self.source.read_bytes()
        manifest = {
            "boundary_candidates": [],
            "kind": MANIFEST_KIND,
            "recording": {
                "input": {
                    "artifact_id": "artifact_untimed_parent",
                    "byte_count": len(source_body),
                    "channels": 1,
                    "duration_ms": duration_ms_for_samples(total_samples),
                    "path": str(self.source),
                    "sample_rate_hz": 16_000,
                    "sha256": hashlib.sha256(source_body).hexdigest(),
                    "total_samples": total_samples,
                },
                "media_id": "media_untimed_parent",
                "recording_id": "recording_untimed_parent",
            },
            "schema_version": 1,
        }
        policy = {
            "adaptive": {
                "boundary_search_samples": 50,
                "max_core_samples": 800,
                "max_span_count": 1,
                "min_core_samples": 200,
                "padding_samples": 0,
                "target_core_samples": 400,
            },
            "direct_max_samples": 800,
            "kind": "himr_longform_asr_planning_policy",
            "schema_version": 1,
        }
        plan = build_longform_asr_plan(manifest, policy)
        plan_path = self.root / "untimed-plan.json"
        plan_path.write_bytes(assembler.pretty_bytes(plan))
        work_order = executor.make_work_order_from_plan(
            plan,
            span_ordinal=0,
            output_root=self.root / "untimed-results",
            execution_lineage=execution_lineage(),
        )
        segment = {
            "end_sample": 300,
            "ordinal": 0,
            "scores": {
                "average_log_probability_raw": -0.2,
                "compression_ratio_raw": 1.1,
                "no_speech_probability_raw": 0.1,
                "temperature_raw": 0.0,
            },
            "start_sample": 100,
            "text": " timed untimed",
            "timing_clipped": False,
            "words": [
                {
                    "end_sample": 160,
                    "ordinal": 0,
                    "probability_raw": 0.9,
                    "start_sample": 120,
                    "text": " timed",
                    "timing_clipped": False,
                },
                {
                    "end_sample": None,
                    "ordinal": 1,
                    "probability_raw": 0.8,
                    "start_sample": None,
                    "text": " untimed",
                    "timing_clipped": False,
                },
            ],
        }
        bundle = executor.execute_work_order(
            work_order,
            FakeEngine([segment]),
            utc_clock=lambda: "2026-08-30T00:00:00Z",
            monotonic_clock=iter((1.0, 2.0)).__next__,
            attempt_id="attempt_untimed_fixture",
        )
        materialized = executor.materialize_result_bundle(bundle)
        result_path = Path(materialized["result_path"])
        transcript_path = Path(materialized["transcript_path"])
        bindings = {
            "kind": assembler.BINDINGS_KIND,
            "schema_version": 1,
            "parent_manifest_sha256": digest(plan_path),
            "spans": [
                {
                    "failure_code": None,
                    "result": {"path": str(result_path), "sha256": digest(result_path)},
                    "span_id": plan["spans"][0]["span_id"],
                    "status": "completed",
                    "transcript": {
                        "path": str(transcript_path),
                        "sha256": digest(transcript_path),
                    },
                }
            ],
            "policy": dict(assembler.BINDINGS_POLICY),
        }
        bindings_path = self.root / "untimed-bindings.json"
        bindings_path.write_bytes(assembler.pretty_bytes(bindings))

        value = assembler.load_and_assemble(plan_path, bindings_path)

        self.assertEqual(len(value["segments"]), 1)
        self.assertEqual(value["segments"][0]["text"], "timed untimed")
        self.assertEqual(value["segments"][0]["words"], [])
        self.assertEqual(
            value["segments"][0]["ownership"]["basis"],
            "segment_anchor_in_source_core_then_duplicate_reconciliation",
        )
        native = json.loads(transcript_path.read_text(encoding="utf-8"))
        self.assertIsNone(native["segments"][0]["words"][1]["start_sample"])
        self.assertIsNone(native["segments"][0]["words"][1]["end_sample"])

    def test_hash_valid_result_with_false_coverage_is_rejected(self) -> None:
        result_path = Path(self.rows[0]["result"]["path"])
        result = json.loads(result_path.read_text(encoding="utf-8"))
        result["coverage"]["engine_reported_analysis_complete"] = False
        core = {
            key: value
            for key, value in result.items()
            if key not in {"identity_sha256", "result_id"}
        }
        identity = assembler.sha256_bytes(
            assembler.canonical_bytes(core, trailing_newline=True)
        )
        result["identity_sha256"] = identity
        result["result_id"] = f"longasrresult1_{identity[:32]}"
        result_path.chmod(0o600)
        result_path.write_bytes(assembler.canonical_bytes(result, trailing_newline=True))
        self.rows[0]["result"]["sha256"] = digest(result_path)
        self.write_bindings(self.rows)
        with self.assertRaisesRegex(assembler.AssemblyError, "complete span coverage"):
            self.assemble()

    def test_hash_valid_result_with_non_candidate_longform_lineage_is_rejected(self) -> None:
        result_path = Path(self.rows[0]["result"]["path"])
        result = json.loads(result_path.read_text(encoding="utf-8"))
        result["execution_lineage"]["longform_scope_status"] = "admitted"
        core = {
            key: value
            for key, value in result.items()
            if key not in {"identity_sha256", "result_id"}
        }
        identity = assembler.sha256_bytes(
            assembler.canonical_bytes(core, trailing_newline=True)
        )
        result["identity_sha256"] = identity
        result["result_id"] = f"longasrresult1_{identity[:32]}"
        result_path.chmod(0o600)
        result_path.write_bytes(assembler.canonical_bytes(result, trailing_newline=True))
        self.rows[0]["result"]["sha256"] = digest(result_path)
        self.write_bindings(self.rows)

        with self.assertRaisesRegex(assembler.AssemblyError, "scope status differs"):
            self.assemble()

    def test_output_is_create_only(self) -> None:
        output = self.root / "assembled.json"
        value = self.assemble()
        assembler.write_new_output(output, value)
        with self.assertRaisesRegex(assembler.AssemblyError, "never overwritten"):
            assembler.write_new_output(output, value)

    def test_cli_emits_one_strict_json_summary(self) -> None:
        completed = subprocess.run(
            [
                str(PIPELINE_ROOT / "bin" / "longform-transcript-assembler"),
                "--parent-manifest",
                str(self.plan_path),
                "--span-results",
                str(self.bindings_path),
                "--validate-only",
            ],
            stdin=subprocess.DEVNULL,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            check=False,
        )
        self.assertEqual(0, completed.returncode, completed.stderr.decode())
        response = json.loads(completed.stdout.decode("utf-8"))
        self.assertEqual("valid", response["status"])
        self.assertTrue(response["coverage_complete"])
        self.assertEqual(b"", completed.stderr)


if __name__ == "__main__":
    unittest.main()
