from __future__ import annotations

import hashlib
import importlib.util
import json
import math
import shutil
import subprocess
import sys
import tempfile
import unittest
import wave
from jsonschema import Draft202012Validator, FormatChecker
from pathlib import Path
from types import ModuleType, SimpleNamespace
from unittest import mock


ROOT = Path(__file__).resolve().parents[2]


def load_module(name: str, path: Path) -> ModuleType:
    spec = importlib.util.spec_from_file_location(name, path)
    if spec is None or spec.loader is None:
        raise RuntimeError(f"cannot load {path}")
    module = importlib.util.module_from_spec(spec)
    sys.modules[name] = module
    spec.loader.exec_module(module)
    return module


PLANNER = load_module(
    "himr_longform_planner_executor_test",
    ROOT / "corpus/src/himr_corpus/longform_asr_planner.py",
)
EXECUTOR = load_module(
    "himr_longform_executor_test",
    ROOT / "pipeline/gpu/longform_asr_execution_v1.py",
)


class FakeEngine:
    def __init__(self, segments: list[dict[str, object]] | None = None) -> None:
        self.calls: list[object] = []
        self.segments = segments or []

    def transcribe(self, request: object) -> dict[str, object]:
        self.calls.append(request)
        return {
            "engine": {
                "library": "fake-cpu-engine",
                "library_version": "1.0",
                "model_revision": "fixture-revision",
                "model_identity_sha256": "9" * 64,
                "input_decoder": {
                    "kind": "fake_ephemeral_decoder",
                    "executable_path": None,
                    "executable_sha256": None,
                    "execution_mode": "in_memory_fixture",
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
            "segments": self.segments,
        }


def scores() -> dict[str, float | None]:
    return {
        "temperature_raw": 0.0,
        "average_log_probability_raw": -0.2,
        "compression_ratio_raw": 1.1,
        "no_speech_probability_raw": 0.01,
    }


def segment(
    ordinal: int,
    start: int,
    end: int,
    text: str,
    *,
    word_probability: float | None = None,
    clipped: bool = False,
) -> dict[str, object]:
    return {
        "ordinal": ordinal,
        "start_sample": start,
        "end_sample": end,
        "text": text,
        "words": [
            {
                "ordinal": 0,
                "start_sample": start,
                "end_sample": end,
                "text": text,
                "probability_raw": word_probability,
                "timing_clipped": clipped,
            }
        ],
        "scores": scores(),
        "timing_clipped": clipped,
    }


class LongFormExecutionV1Tests(unittest.TestCase):
    def setUp(self) -> None:
        self.temporary = tempfile.TemporaryDirectory()
        self.root = Path(self.temporary.name)
        self.audio = self.root / "parent.flac"
        self.audio.write_bytes(b"fixture-normalized-parent-audio")

    def tearDown(self) -> None:
        self.temporary.cleanup()

    def plan(self, total_samples: int, *, direct_max: int) -> dict[str, object]:
        body = self.audio.read_bytes()
        manifest = {
            "kind": PLANNER.MANIFEST_KIND,
            "schema_version": 1,
            "recording": {
                "recording_id": "rec_fixture",
                "media_id": "media_fixture",
                "input": {
                    "artifact_id": "artifact_fixture",
                    "path": str(self.audio),
                    "sha256": hashlib.sha256(body).hexdigest(),
                    "byte_count": len(body),
                    "sample_rate_hz": 16_000,
                    "channels": 1,
                    "total_samples": total_samples,
                    "duration_ms": PLANNER.duration_ms_for_samples(total_samples),
                },
            },
            "boundary_candidates": [],
        }
        policy = {
            "kind": PLANNER.POLICY_KIND,
            "schema_version": 1,
            "direct_max_samples": direct_max,
            "adaptive": {
                "min_core_samples": 16_000,
                "target_core_samples": 32_000,
                "max_core_samples": 40_000,
                "boundary_search_samples": 4_000,
                "padding_samples": 8_000,
                "max_span_count": 100,
            },
        }
        return PLANNER.build_longform_asr_plan(manifest, policy)

    def order(self, plan: dict[str, object], ordinal: int) -> dict[str, object]:
        return EXECUTOR.make_work_order_from_plan(
            plan,
            span_ordinal=ordinal,
            output_root=self.root / "results",
            execution_lineage={
                "longform_scope_status": "candidate_unsoaked",
                "production_profile": {
                    "profile_id": "gpuprofile_fixture",
                    "identity_sha256": "1" * 64,
                    "physical_sha256": "2" * 64,
                },
                "runtime_admission": {
                    "receipt_id": "gpurtv2_fixture",
                    "identity_sha256": "3" * 64,
                    "physical_sha256": "4" * 64,
                    "status": "candidate",
                },
                "model": {
                    "repository": "fixture/faster-whisper",
                    "revision": "fixture-revision",
                    "identity_sha256": "9" * 64,
                },
            },
        )

    @staticmethod
    def clocks() -> tuple[object, object]:
        utc_values = iter(("2026-08-30T12:00:00.000000Z", "2026-08-30T12:00:01.000000Z"))
        mono_values = iter((100.0, 101.0))
        return lambda: next(utc_values), lambda: next(mono_values)

    def assert_schema_valid(self, name: str, value: object) -> None:
        schema_path = ROOT / "pipeline" / "schemas" / name
        schema = json.loads(schema_path.read_text(encoding="utf-8"))
        Draft202012Validator.check_schema(schema)
        Draft202012Validator(
            schema, format_checker=FormatChecker()
        ).validate(value)

    def test_direct_executes_parent_once_and_materializes_compact_transcript(self) -> None:
        plan = self.plan(32_000, direct_max=40_000)
        order = self.order(plan, 0)
        engine = FakeEngine([segment(0, 1_600, 8_000, " hello")])
        utc, monotonic = self.clocks()
        bundle = EXECUTOR.execute_work_order(
            order,
            engine,
            utc_clock=utc,
            monotonic_clock=monotonic,
            attempt_id="longasrattempt_fixture",
        )
        self.assertEqual(len(engine.calls), 1)
        request = engine.calls[0]
        self.assertTrue(request.source.is_whole_recording)
        self.assertTrue(request.source.retained_path.startswith("/proc/self/fd/"))
        transcript = bundle["transcript"]
        self.assertEqual(transcript["kind"], "himr_longform_asr_span_transcript")
        word = transcript["segments"][0]["words"][0]
        self.assertEqual(set(word), {"text", "start_sample", "end_sample"})
        self.assertNotIn("policy", transcript)
        self.assertNotIn("engine", transcript)
        self.assertEqual(bundle["result"]["plan"], order["plan"])
        self.assert_schema_valid("longform-asr-span-work-order.schema.json", order)
        self.assert_schema_valid(
            "longform-asr-span-transcript.schema.json", transcript
        )
        self.assert_schema_valid(
            "longform-asr-span-result.schema.json", bundle["result"]
        )
        receipt = EXECUTOR.materialize_result_bundle(bundle)
        self.assertFalse(receipt["persistent_audio_chunks_created"])
        replayed = EXECUTOR.replay_materialized_result(order)
        self.assertEqual(replayed, bundle)
        leaves = sorted(path.name for path in Path(receipt["result_directory"]).iterdir())
        self.assertEqual(leaves, ["result.json", "transcript.json"])
        with self.assertRaises(EXECUTOR.LongFormExecutionError):
            EXECUTOR.materialize_result_bundle(bundle)

    def test_adaptive_span_has_exact_parent_and_local_coordinates(self) -> None:
        plan = self.plan(100_000, direct_max=40_000)
        self.assertEqual(plan["strategy"], "adaptive_spans")
        order = self.order(plan, 1)
        analysis_count = order["span"]["analysis"]["sample_count"]
        engine = FakeEngine(
            [
                segment(0, 0, 4_000, " left"),
                segment(1, 7_000, 9_000, " boundary"),
                segment(2, 10_000, 20_000, " core", word_probability=0.8),
                segment(3, 39_000, 42_000, " other boundary", clipped=True),
                segment(4, 42_000, min(45_000, analysis_count), " right"),
            ]
        )
        utc, monotonic = self.clocks()
        bundle = EXECUTOR.execute_work_order(
            order, engine, utc_clock=utc, monotonic_clock=monotonic
        )
        result = bundle["result"]
        self.assertEqual(result["span"]["analysis"], order["span"]["analysis"])
        self.assertEqual(result["coverage"]["analysis_sample_count"], analysis_count)
        counts = result["dispositions"]["segment_counts"]
        self.assertEqual(counts["left_context_only"], 1)
        self.assertEqual(counts["crosses_left_core_boundary"], 1)
        self.assertEqual(counts["core_owned"], 1)
        self.assertEqual(counts["crosses_right_core_boundary"], 1)
        self.assertEqual(counts["right_context_only"], 1)
        compact_word = bundle["transcript"]["segments"][2]["words"][0]
        self.assertEqual(compact_word["probability_raw"], 0.8)
        self.assertNotIn("ownership_disposition", compact_word)
        clipped_word = bundle["transcript"]["segments"][3]["words"][0]
        self.assertIs(clipped_word["timing_clipped"], True)

    def test_batch_work_order_projection_replays_plan_once_and_is_equivalent(self) -> None:
        plan = self.plan(100_000, direct_max=40_000)
        expected = [self.order(plan, row["ordinal"]) for row in plan["spans"]]
        original = EXECUTOR._validated_planner_plan
        with mock.patch.object(
            EXECUTOR, "_validated_planner_plan", wraps=original
        ) as replay:
            observed = EXECUTOR.make_work_orders_from_plan(
                plan,
                output_root=self.root / "results",
                execution_lineage=expected[0]["execution_lineage"],
            )
        self.assertEqual(replay.call_count, 1)
        self.assertEqual(observed, expected)

    def test_source_hash_mismatch_fails_before_engine(self) -> None:
        plan = self.plan(32_000, direct_max=40_000)
        order = self.order(plan, 0)
        self.audio.write_bytes(b"changed")
        engine = FakeEngine()
        with self.assertRaisesRegex(EXECUTOR.LongFormExecutionError, "byte count differs|SHA-256 differs"):
            EXECUTOR.execute_work_order(order, engine)
        self.assertEqual(engine.calls, [])

    def test_self_rehashed_malformed_nested_documents_are_rejected(self) -> None:
        plan = self.plan(32_000, direct_max=40_000)
        order = self.order(plan, 0)
        utc, monotonic = self.clocks()
        bundle = EXECUTOR.execute_work_order(
            order,
            FakeEngine([segment(0, 1_600, 8_000, " hello")]),
            utc_clock=utc,
            monotonic_clock=monotonic,
        )

        malformed_transcript = json.loads(
            json.dumps(bundle, ensure_ascii=False)
        )
        transcript = malformed_transcript["transcript"]
        transcript["segments"][0]["words"][0]["ordinal"] = 0
        transcript_core = {
            key: value
            for key, value in transcript.items()
            if key not in {"identity_sha256", "document_id"}
        }
        transcript_identity = EXECUTOR.sha256_bytes(
            EXECUTOR.canonical_bytes(transcript_core)
        )
        transcript["identity_sha256"] = transcript_identity
        transcript["document_id"] = f"longasrtranscript1_{transcript_identity[:32]}"
        result = malformed_transcript["result"]
        plan_paths = EXECUTOR.result_plan(order)
        result["artifacts"] = [
            EXECUTOR._artifact(
                transcript,
                plan_paths["transcript_path"],
                "longform_span_transcript_json",
            )
        ]
        result_core = {
            key: value
            for key, value in result.items()
            if key not in {"identity_sha256", "result_id"}
        }
        result_identity = EXECUTOR.sha256_bytes(EXECUTOR.canonical_bytes(result_core))
        result["identity_sha256"] = result_identity
        result["result_id"] = f"longasrresult1_{result_identity[:32]}"
        with self.assertRaisesRegex(
            EXECUTOR.LongFormExecutionError, "word 0.0 fields differ"
        ):
            EXECUTOR.validate_result_bundle(malformed_transcript)

        malformed_result = json.loads(json.dumps(bundle, ensure_ascii=False))
        malformed_result["result"]["coverage"]["analysis_sample_count"] -= 1
        result_core = {
            key: value
            for key, value in malformed_result["result"].items()
            if key not in {"identity_sha256", "result_id"}
        }
        result_identity = EXECUTOR.sha256_bytes(EXECUTOR.canonical_bytes(result_core))
        malformed_result["result"]["identity_sha256"] = result_identity
        malformed_result["result"]["result_id"] = (
            f"longasrresult1_{result_identity[:32]}"
        )
        with self.assertRaisesRegex(EXECUTOR.LongFormExecutionError, "coverage differs"):
            EXECUTOR.validate_result_bundle(malformed_result)

    def test_failed_publish_does_not_poison_final_result_directory(self) -> None:
        plan = self.plan(32_000, direct_max=40_000)
        order = self.order(plan, 0)
        utc, monotonic = self.clocks()
        bundle = EXECUTOR.execute_work_order(
            order, FakeEngine(), utc_clock=utc, monotonic_clock=monotonic
        )
        final = Path(EXECUTOR.result_plan(order)["result_directory"])
        with mock.patch.object(EXECUTOR, "_rename_noreplace", side_effect=OSError("fixture")):
            with self.assertRaises(OSError):
                EXECUTOR.materialize_result_bundle(bundle)
        self.assertFalse(final.exists())
        self.assertEqual(list(final.parent.glob(f".{final.name}.stage-*")), [])
        EXECUTOR.materialize_result_bundle(bundle)
        self.assertTrue(final.is_dir())

    def test_replay_rejects_result_leaf_entry_swap_during_retained_read(self) -> None:
        plan = self.plan(32_000, direct_max=40_000)
        order = self.order(plan, 0)
        utc, monotonic = self.clocks()
        bundle = EXECUTOR.execute_work_order(
            order, FakeEngine(), utc_clock=utc, monotonic_clock=monotonic
        )
        EXECUTOR.materialize_result_bundle(bundle)
        transcript_path = Path(EXECUTOR.result_plan(order)["transcript_path"])
        transcript_body = transcript_path.read_bytes()
        backup = transcript_path.with_name("transcript-swapped.json")
        original_pread = EXECUTOR.os.pread
        swapped = False

        def swap_entry(descriptor: int, count: int, offset: int) -> bytes:
            nonlocal swapped
            body = original_pread(descriptor, count, offset)
            if not swapped:
                swapped = True
                transcript_path.rename(backup)
                transcript_path.write_bytes(transcript_body)
            return body

        with mock.patch.object(EXECUTOR.os, "pread", side_effect=swap_entry):
            with self.assertRaisesRegex(
                EXECUTOR.LongFormExecutionError,
                "changed during retained read|directory entry differs",
            ):
                EXECUTOR.replay_materialized_result(order)

    def test_faster_whisper_adapter_uses_retained_descriptor_and_checks_duration(self) -> None:
        plan = self.plan(32_000, direct_max=40_000)
        order = self.order(plan, 0)

        class Model:
            def __init__(self) -> None:
                self.audio = None

            def transcribe(self, audio: object, **_: object) -> tuple[object, object]:
                self.audio = audio
                row = SimpleNamespace(
                    start=0.1,
                    end=0.5,
                    text=" hello",
                    words=[SimpleNamespace(start=0.1, end=0.5, word=" hello", probability=0.9)],
                    temperature=0.0,
                    avg_logprob=-0.1,
                    compression_ratio=1.0,
                    no_speech_prob=0.0,
                )
                return iter((row,)), SimpleNamespace(duration=2.0)

        model = Model()
        adapter = EXECUTOR.FasterWhisperModelEngine(
            model,
            library_version="1.2.0",
            model_revision="fixture-revision",
            model_identity_sha256="9" * 64,
        )
        utc, monotonic = self.clocks()
        EXECUTOR.execute_work_order(
            order, adapter, utc_clock=utc, monotonic_clock=monotonic
        )
        self.assertRegex(model.audio, r"^/proc/self/fd/[0-9]+$")

        class WrongDurationModel(Model):
            def transcribe(self, audio: object, **kwargs: object) -> tuple[object, object]:
                rows, _ = super().transcribe(audio, **kwargs)
                return rows, SimpleNamespace(duration=1.0)

        wrong = EXECUTOR.FasterWhisperModelEngine(
            WrongDurationModel(),
            library_version="1.2.0",
            model_revision="fixture-revision",
            model_identity_sha256="9" * 64,
        )
        with self.assertRaisesRegex(EXECUTOR.LongFormExecutionError, "decoded duration differs"):
            EXECUTOR.execute_work_order(order, wrong)

    def test_faster_whisper_121_empty_alignment_is_a_segment_without_words(self) -> None:
        class AlignmentBackend:
            @staticmethod
            def align(*_: object, **__: object) -> list[object]:
                return [
                    SimpleNamespace(
                        text_token_probs=[0.9],
                        alignments=[],
                    )
                ]

        class Model:
            def __init__(self) -> None:
                self.model = AlignmentBackend()
                self.tokens_per_second = 50

            @staticmethod
            def find_alignment(*_: object, **__: object) -> list[object]:
                raise AssertionError("unguarded faster-whisper method was called")

        class Tokenizer:
            sot_sequence = [1]
            eot = 2

            @staticmethod
            def split_to_word_tokens(*_: object) -> object:
                raise AssertionError("empty alignment should skip token projection")

        model = Model()
        EXECUTOR.FasterWhisperModelEngine(
            model,
            library_version="1.2.1",
            model_revision="fixture-revision",
            model_identity_sha256="9" * 64,
        )

        self.assertEqual(
            model.find_alignment(Tokenizer(), [[10]], object(), 1),
            [[]],
        )

    @unittest.skipUnless(
        importlib.util.find_spec("numpy"),
        "NumPy is required for the non-empty Faster-Whisper alignment test",
    )
    def test_faster_whisper_121_guard_preserves_nonempty_alignment(self) -> None:
        class AlignmentBackend:
            @staticmethod
            def align(*_: object, **__: object) -> list[object]:
                return [
                    SimpleNamespace(
                        text_token_probs=[0.8, 0.6],
                        alignments=[(0, 0), (0, 1), (1, 2), (1, 3), (2, 4)],
                    )
                ]

        class Model:
            def __init__(self) -> None:
                self.model = AlignmentBackend()
                self.tokens_per_second = 20

            @staticmethod
            def find_alignment(*_: object, **__: object) -> list[object]:
                raise AssertionError("unguarded faster-whisper method was called")

        class Tokenizer:
            sot_sequence = [1]
            eot = 99

            @staticmethod
            def split_to_word_tokens(_: object) -> tuple[list[str], list[list[int]]]:
                return [" hello", "<eot>"], [[10, 11], [99]]

        model = Model()
        EXECUTOR.FasterWhisperModelEngine(
            model,
            library_version="1.2.1",
            model_revision="fixture-revision",
            model_identity_sha256="9" * 64,
        )

        aligned = model.find_alignment(Tokenizer(), [[10, 11]], object(), 100)
        self.assertEqual(len(aligned), 1)
        self.assertEqual(len(aligned[0]), 1)
        self.assertEqual(aligned[0][0]["word"], " hello")
        self.assertEqual(aligned[0][0]["tokens"], [10, 11])
        self.assertEqual(aligned[0][0]["start"], 0.0)
        self.assertEqual(aligned[0][0]["end"], 0.2)
        self.assertAlmostEqual(aligned[0][0]["probability"], 0.7)

    @unittest.skipUnless(
        shutil.which("ffmpeg") and importlib.util.find_spec("numpy"),
        "FFmpeg and NumPy are required for the light sequential decoder test",
    )
    def test_sequential_decoder_reuses_overlap_in_one_forward_process(self) -> None:
        pcm = self.root / "tone.wav"
        flac = self.root / "tone.flac"
        total_samples = 48_000
        with wave.open(str(pcm), "wb") as handle:
            handle.setnchannels(1)
            handle.setsampwidth(2)
            handle.setframerate(16_000)
            values = bytearray()
            for index in range(total_samples):
                sample_value = round(8_000 * math.sin(index * 2 * math.pi * 220 / 16_000))
                values.extend(int(sample_value).to_bytes(2, "little", signed=True))
            handle.writeframes(values)
        ffmpeg = Path(shutil.which("ffmpeg") or "/usr/bin/ffmpeg").resolve()
        subprocess.run(
            [str(ffmpeg), "-hide_banner", "-loglevel", "error", "-nostdin", "-i", str(pcm), "-c:a", "flac", str(flac)],
            check=True,
        )
        body = flac.read_bytes()
        binding = {
            "path": str(flac),
            "sha256": hashlib.sha256(body).hexdigest(),
            "byte_count": len(body),
        }
        with EXECUTOR.RetainedSource(binding) as source:
            with EXECUTOR.SequentialFFmpegSpanDecoder(
                source,
                total_samples=total_samples,
                executable=ffmpeg,
                executable_sha256=hashlib.sha256(ffmpeg.read_bytes()).hexdigest(),
            ) as decoder:
                first = EXECUTOR.SpanSourceView(
                    str(flac), source.proc_path, source.descriptor, binding["sha256"], len(body), total_samples, 0, 32_000
                )
                second = EXECUTOR.SpanSourceView(
                    str(flac), source.proc_path, source.descriptor, binding["sha256"], len(body), total_samples, 24_000, 48_000
                )
                first_audio = decoder(first)
                second_audio = decoder(second)
                self.assertEqual(len(first_audio), 32_000)
                self.assertEqual(len(second_audio), 24_000)
                self.assertTrue((first_audio[24_000:32_000] == second_audio[:8_000]).all())
                self.assertEqual(decoder.process.poll(), None)
                decoder.finish()
                self.assertEqual(decoder.process.returncode, 0)


if __name__ == "__main__":
    unittest.main()
