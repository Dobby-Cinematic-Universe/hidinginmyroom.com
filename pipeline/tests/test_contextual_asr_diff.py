from __future__ import annotations

import hashlib
import json
import shutil
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path

from jsonschema import Draft202012Validator


PIPELINE_ROOT = Path(__file__).resolve().parents[1]
if str(PIPELINE_ROOT) not in sys.path:
    sys.path.insert(0, str(PIPELINE_ROOT))

import asr_whispercpp  # noqa: E402
import contextual_asr_diff as diff  # noqa: E402


def canonical_text(value: object) -> str:
    return json.dumps(
        value,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
        allow_nan=False,
    )


def digest(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


class ContextualASRDiffTests(unittest.TestCase):
    def setUp(self) -> None:
        if not shutil.which("ffmpeg") or not shutil.which("ffprobe"):
            self.skipTest("ffmpeg and ffprobe are required")
        test_root = PIPELINE_ROOT / ".test-work"
        test_root.mkdir(parents=True, exist_ok=True)
        self.temp = tempfile.TemporaryDirectory(
            prefix="himr-contextual-diff-", dir=test_root
        )
        self.root = Path(self.temp.name)
        self.input = self.root / "audio.flac"
        self.engine = self.root / "whisper-cli"
        self.engine_payload = self.root / "engine-payload.json"
        self.model = self.root / "model.bin"
        self.glossary = self.root / "glossary.json"
        generated = subprocess.run(
            [
                "ffmpeg",
                "-hide_banner",
                "-nostdin",
                "-loglevel",
                "error",
                "-f",
                "lavfi",
                "-i",
                "anullsrc=r=16000:cl=mono",
                "-t",
                "60",
                "-ac",
                "1",
                "-ar",
                "16000",
                "-sample_fmt",
                "s16",
                "-c:a",
                "flac",
                str(self.input),
            ],
            stdin=subprocess.DEVNULL,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            check=False,
        )
        if generated.returncode != 0:
            self.fail(generated.stderr.decode("utf-8", errors="replace"))
        self.engine.write_text(
            "#!/usr/bin/env python3\n"
            "import sys\n"
            "from pathlib import Path\n"
            "args = sys.argv[1:]\n"
            "if '--version' in args:\n"
            "    print('whisper.cpp version: contextual-diff-fixture-v1')\n"
            "    raise SystemExit(0)\n"
            "def arg(name): return args[args.index(name) + 1]\n"
            f"payload = Path({str(self.engine_payload)!r}).read_bytes()\n"
            "Path(arg('--output-file') + '.json').write_bytes(payload)\n",
            encoding="utf-8",
        )
        self.engine.chmod(0o700)
        self.model.write_bytes(b"test-model")
        self.glossary_document = {
            "schema_version": 1,
            "glossary_revision_id": "glossary_himr_neutral_en_v1",
            "revision": "2026-08-27.1-machine-candidate",
            "language": "en",
            "terms": ["HIMRFAM", "Dobby"],
        }
        self.glossary.write_text(
            json.dumps(self.glossary_document, indent=2, sort_keys=True) + "\n",
            encoding="utf-8",
        )
        self.validated_glossary = asr_whispercpp.validate_glossary_document(
            self.glossary_document, "en"
        )

    def tearDown(self) -> None:
        self.temp.cleanup()

    def token(
        self,
        ordinal: int,
        text: str,
        start_ms: int | None,
        end_ms: int | None,
        probability: float,
        *,
        token_id: int = 100,
    ) -> dict[str, object]:
        return {
            "ordinal": ordinal,
            "token_id": token_id,
            "text": text,
            "start_ms": start_ms,
            "end_ms": end_ms,
            "raw_probability": probability,
            "raw_dtw_timestamp": -1.0,
            "metadata_json": "{}",
        }

    def segment(
        self,
        ordinal: int,
        start_ms: int,
        end_ms: int,
        tokens: list[dict[str, object]],
    ) -> dict[str, object]:
        return {
            "ordinal": ordinal,
            "start_ms": start_ms,
            "end_ms": end_ms,
            "text": "fixture text retained only inside private result",
            "tokens": tokens,
            "token_count": len(tokens),
            "quality_flags": [],
            "window_overrun_ms": 0,
        }

    def make_result(
        self,
        name: str,
        *,
        contextual: bool,
        segments: list[dict[str, object]],
        inference_override: dict[str, object] | None = None,
        model_path: Path | None = None,
    ) -> Path:
        active_model = model_path or self.model
        inference = {
            "language": "en",
            "threads": 1,
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
            "timeout_seconds": 7200,
        }
        if inference_override:
            inference.update(inference_override)

        def stamp(milliseconds: int) -> str:
            hours, remainder = divmod(milliseconds, 3_600_000)
            minutes, remainder = divmod(remainder, 60_000)
            seconds, milliseconds = divmod(remainder, 1_000)
            return f"{hours:02d}:{minutes:02d}:{seconds:02d},{milliseconds:03d}"

        def timing(start_ms: int, end_ms: int) -> dict[str, object]:
            return {
                "timestamps": {"from": stamp(start_ms), "to": stamp(end_ms)},
                "offsets": {"from": start_ms, "to": end_ms},
            }

        raw_segments: list[dict[str, object]] = []
        for segment in segments:
            raw_tokens: list[dict[str, object]] = []
            for token in segment["tokens"]:
                token_start = token["start_ms"]
                token_end = token["end_ms"]
                if token_start is None or token_end is None:
                    token_start = int(segment["start_ms"]) + 1
                    token_end = int(segment["start_ms"])
                raw_tokens.append(
                    {
                        "text": token["text"],
                        **timing(int(token_start), int(token_end)),
                        "id": token["token_id"],
                        "p": token["raw_probability"],
                        "t_dtw": token["raw_dtw_timestamp"],
                    }
                )
            raw_segments.append(
                {
                    **timing(int(segment["start_ms"]), int(segment["end_ms"])),
                    "text": segment["text"],
                    "tokens": raw_tokens,
                }
            )
        self.engine_payload.write_text(
            json.dumps(
                {
                    "systeminfo": "contextual diff fixture",
                    "params": {"translate": False},
                    "result": {"language": "en"},
                    "transcription": raw_segments,
                },
                sort_keys=True,
            ),
            encoding="utf-8",
        )
        input_sha = digest(self.input)
        order = {
            "schema_version": 1,
            "job_id": f"job-{name}",
            "input": {
                "path": str(self.input.resolve()),
                "expected_sha256": input_sha,
                "media_id": f"media_sha256_{input_sha}",
                "artifact_id": "artifact_normalized_audio_fixture",
                "parent_processing_run_id": "run_preprocess_fixture",
            },
            "engine": {
                "executable": str(self.engine.resolve()),
                "expected_sha256": digest(self.engine),
                "version_label": "contextual-diff-fixture-v1",
                "version_evidence": "source_revision_plus_executable_sha256",
                "build": {
                    "repository": "https://example.invalid/whisper.cpp",
                    "revision": "fixture-revision",
                    "target": "whisper-cli",
                    "configuration": ["GGML_CUDA=OFF"],
                },
            },
            "model": {
                "path": str(active_model.resolve()),
                "expected_sha256": digest(active_model),
                "model_id": "model_fixture",
                "name": "fixture model",
                "revision": "fixture-revision",
                "source": "https://example.invalid/model",
                "license_label": "fixture-only",
            },
            "window": {"offset_ms": 0, "duration_ms": 60_000},
            "inference": inference,
            "glossary": {
                "path": str(self.glossary.resolve()),
                "expected_sha256": digest(self.glossary),
            }
            if contextual
            else None,
            "catalog_context": None,
            "output": {"root": str((self.root / name).resolve())},
        }
        validated_order = asr_whispercpp.validate_work_order(order)
        result = asr_whispercpp.run_asr(validated_order, dry_run=False)
        return Path(result["result_path"])

    def pair(self, suffix: str = "") -> tuple[Path, Path]:
        raw = self.make_result(
            f"raw{suffix}",
            contextual=False,
            segments=[
                self.segment(
                    0,
                    0,
                    20_000,
                    [
                        self.token(0, " Him", 1_000, 2_000, 0.6),
                        self.token(1, " fam", 2_000, 3_000, 0.4),
                    ],
                )
            ],
        )
        contextual = self.make_result(
            f"contextual{suffix}",
            contextual=True,
            segments=[
                self.segment(
                    0,
                    0,
                    20_000,
                    [
                        self.token(0, " HIMRFAM", 1_100, 3_100, 0.8),
                        self.token(1, " Dobby", 4_000, 5_000, 0.7),
                    ],
                )
            ],
        )
        return raw, contextual

    def test_builds_deterministic_text_private_change_metrics(self) -> None:
        raw, contextual = self.pair()
        first = diff.build_diff(
            baseline_result=str(raw), contextual_result=str(contextual)
        )
        second = diff.build_diff(
            baseline_result=str(raw), contextual_result=str(contextual)
        )
        self.assertEqual(first, second)
        self.assertEqual(first["summary"]["changed_blocks"], 1)
        self.assertGreater(first["summary"]["total_character_edit_distance"], 0)
        block = first["changed_block_metrics"][0]
        self.assertEqual(block["first_timed_token_start_drift_ms"], 100)
        self.assertEqual(
            {item["term"]: item["delta"] for item in block["glossary_term_counts"]},
            {"HIMRFAM": 1, "Dobby": 1},
        )
        serialized = canonical_text(first)
        self.assertNotIn(" Him fam", serialized)
        self.assertNotIn("fixture text retained", serialized)
        for forbidden in (
            "preferred_text",
            "corrected_text",
            "word_error_rate",
            "accuracy_score",
            "accepted_revision",
        ):
            self.assertNotIn(forbidden, serialized)
        schema = json.loads(
            (PIPELINE_ROOT / "schemas" / "contextual-asr-diff.schema.json").read_text(
                encoding="utf-8"
            )
        )
        Draft202012Validator(schema).validate(first)

    def test_segmentation_change_with_same_tokens_is_unchanged(self) -> None:
        tokens = [
            self.token(0, " same", 1_000, 2_000, 0.8),
            self.token(1, " words", 31_000, 32_000, 0.7),
        ]
        raw = self.make_result(
            "raw-segments",
            contextual=False,
            segments=[self.segment(0, 0, 60_000, tokens)],
        )
        contextual = self.make_result(
            "contextual-segments",
            contextual=True,
            segments=[
                self.segment(0, 0, 30_000, [tokens[0]]),
                self.segment(1, 30_000, 60_000, [tokens[1]]),
            ],
        )
        result = diff.build_diff(
            baseline_result=str(raw), contextual_result=str(contextual)
        )
        self.assertEqual(result["summary"]["changed_blocks"], 0)
        self.assertEqual(result["summary"]["unchanged_blocks"], 2)

    def test_untimed_token_uses_segment_midpoint_and_is_counted(self) -> None:
        raw = self.make_result(
            "raw-untimed",
            contextual=False,
            segments=[
                self.segment(
                    0, 30_000, 40_000, [self.token(0, " raw", None, None, 0.5)]
                )
            ],
        )
        contextual = self.make_result(
            "contextual-untimed",
            contextual=True,
            segments=[],
        )
        result = diff.build_diff(
            baseline_result=str(raw), contextual_result=str(contextual)
        )
        changed = result["changed_block_metrics"][0]
        self.assertEqual(changed["block_index"], 1)
        self.assertEqual(changed["baseline_untimed_lexical_tokens"], 1)
        self.assertEqual(
            changed["empty_transition"], "baseline_nonempty_contextual_empty"
        )

    def test_rejects_recipe_drift_beyond_glossary(self) -> None:
        raw = self.make_result("raw-drift", contextual=False, segments=[])
        contextual = self.make_result(
            "contextual-drift",
            contextual=True,
            segments=[],
            inference_override={"beam_size": 4},
        )
        with self.assertRaisesRegex(diff.DiffError, "beyond the glossary"):
            diff.build_diff(
                baseline_result=str(raw), contextual_result=str(contextual)
            )

    def test_rejects_model_and_artifact_tampering(self) -> None:
        raw, contextual = self.pair()
        self.model.write_bytes(b"changed-model")
        with self.assertRaisesRegex(
            diff.DiffError, "strict catalog-free validation.*model"
        ):
            diff.build_diff(
                baseline_result=str(raw), contextual_result=str(contextual)
            )
        self.model.write_bytes(b"test-model")
        normalized = raw.parent / "transcript.normalized.json"
        normalized.write_bytes(normalized.read_bytes() + b" ")
        with self.assertRaisesRegex(
            diff.DiffError, "strict catalog-free validation.*transcript_normalized_json"
        ):
            diff.build_diff(
                baseline_result=str(raw), contextual_result=str(contextual)
            )

    def test_rejects_malformed_nested_result_before_comparison(self) -> None:
        raw, contextual = self.pair()
        malformed = json.loads(raw.read_text(encoding="utf-8"))
        malformed["catalog_records"]["unreviewed_extra_lane"] = []
        raw.write_text(
            json.dumps(malformed, indent=2, sort_keys=True) + "\n",
            encoding="utf-8",
        )
        with self.assertRaisesRegex(
            diff.DiffError, "strict catalog-free validation.*catalog_records"
        ):
            diff.build_diff(
                baseline_result=str(raw), contextual_result=str(contextual)
            )

    def test_rejects_malformed_run_input_and_commands(self) -> None:
        for suffix, mutate, expected in (
            (
                "-bad-run-input",
                lambda value: value["run_input"].__setitem__("unexpected", True),
                "run_input",
            ),
            (
                "-bad-commands",
                lambda value: value["commands"].append([]),
                "commands",
            ),
            (
                "-bad-processing-run",
                lambda value: value["processing_run"].__setitem__(
                    "unreviewed_extra", None
                ),
                "processing_run",
            ),
        ):
            with self.subTest(suffix=suffix):
                raw, contextual = self.pair(suffix)
                malformed = json.loads(raw.read_text(encoding="utf-8"))
                mutate(malformed)
                raw.write_text(
                    json.dumps(malformed, indent=2, sort_keys=True) + "\n",
                    encoding="utf-8",
                )
                with self.assertRaisesRegex(
                    diff.DiffError,
                    rf"strict catalog-free validation.*{expected}",
                ):
                    diff.build_diff(
                        baseline_result=str(raw), contextual_result=str(contextual)
                    )

    def test_exact_output_reuse_and_refuses_different_bytes(self) -> None:
        raw, contextual = self.pair()
        output = self.root / "durable" / "diff.json"
        argv = [
            sys.executable,
            str(PIPELINE_ROOT / "contextual_asr_diff.py"),
            "--baseline-result",
            str(raw),
            "--contextual-result",
            str(contextual),
            "--output",
            str(output),
        ]
        first = subprocess.run(argv, check=False, capture_output=True)
        self.assertEqual(first.returncode, 0, first.stderr.decode())
        second = subprocess.run(argv, check=False, capture_output=True)
        self.assertEqual(second.returncode, 0, second.stderr.decode())
        self.assertEqual(first.stdout, second.stdout)
        self.assertEqual(output.read_bytes(), first.stdout)
        output.write_text("{}\n", encoding="utf-8")
        failed = subprocess.run(argv, check=False, capture_output=True)
        self.assertEqual(failed.returncode, 2)
        self.assertIn(b"existing diff output bytes differ", failed.stderr)

    def test_rejects_invalid_block_size_and_same_result(self) -> None:
        raw, contextual = self.pair()
        with self.assertRaisesRegex(diff.DiffError, "block_ms"):
            diff.build_diff(
                baseline_result=str(raw),
                contextual_result=str(contextual),
                block_ms=1000,
            )
        with self.assertRaisesRegex(diff.DiffError, "paths must differ"):
            diff.build_diff(
                baseline_result=str(raw), contextual_result=str(raw)
            )


if __name__ == "__main__":
    unittest.main()
