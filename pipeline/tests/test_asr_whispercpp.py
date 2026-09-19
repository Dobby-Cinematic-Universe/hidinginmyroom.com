from __future__ import annotations

import hashlib
import json
import os
import shutil
import subprocess
import unittest
from pathlib import Path
from unittest import mock

from pipeline.asr_whispercpp import (
    ASRError,
    DESCRIPTOR_EXECUTION_POLICY,
    normalize_engine_output,
    reject_execution_ineligible_engine,
    retained_verified_file,
    run_asr,
    stable_file_bytes,
    verify_retained_file,
)


REPOSITORY_ROOT = Path(__file__).resolve().parents[2]
PIPELINE_ROOT = REPOSITORY_ROOT / "pipeline"
PROGRAM = PIPELINE_ROOT / "asr_whispercpp.py"
TEST_ROOT = PIPELINE_ROOT / ".test-work" / "asr-whispercpp"
INVALID_UTF8_OUTPUT = (
    b'{"result":{"language":"en"},"transcription":[{"text":"before '
    + bytes.fromhex("f0288c28")
    + b' after"}]}'
)


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


def generate_audio(path: Path, *, sample_format: str = "s16") -> None:
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
            "sine=frequency=440:sample_rate=16000:duration=2.5",
            "-ac",
            "1",
            "-ar",
            "16000",
            "-sample_fmt",
            sample_format,
            "-c:a",
            "flac",
            str(path),
        ]
    )


def make_fake_whisper(path: Path) -> None:
    path.write_text(
        "#!/usr/bin/env python3\n"
        "import json, sys\n"
        "from pathlib import Path\n"
        "if '--version' in sys.argv:\n"
        "    print('whisper.cpp version: fake-pinned-v1')\n"
        "    raise SystemExit(0)\n"
        "def arg(name): return sys.argv[sys.argv.index(name) + 1]\n"
        "offset = int(arg('--offset-t'))\n"
        "duration = int(arg('--duration'))\n"
        "end = min(offset + duration + 100, offset + 800)\n"
        "middle = offset + (end - offset) // 2\n"
        "def stamp(ms):\n"
        "    h, rem = divmod(ms, 3600000); m, rem = divmod(rem, 60000); s, rem = divmod(rem, 1000)\n"
        "    return f'{h:02d}:{m:02d}:{s:02d},{rem:03d}'\n"
        "def timing(a, b): return {'timestamps': {'from': stamp(a), 'to': stamp(b)}, 'offsets': {'from': a, 'to': b}}\n"
        f"mode = {path.name!r}\n"
        "if 'invalid-utf8' in mode:\n"
        "    raw = b'{\"result\":{\"language\":\"en\"},\"transcription\":[{\"text\":\"before ' + bytes.fromhex('f0288c28') + b' after\"}]}'\n"
        "    Path(arg('--output-file') + '.json').write_bytes(raw)\n"
        "    raise SystemExit(0)\n"
        "if 'noninverted-outside' in mode: second_timing = timing(offset, end + 1)\n"
        "elif 'negative' in mode: second_timing = timing(middle, -1)\n"
        "elif 'inverted' in mode: second_timing = timing(offset, 0)\n"
        "else: second_timing = timing(offset, middle)\n"
        "tokens = [\n"
        "    {'text': '[_BEG_]', **timing(offset, offset), 'id': 50363, 'p': 1.0, 't_dtw': -1.0},\n"
        "    {'text': ' HIMR', **second_timing, 'id': 101, 'p': 0.875, 't_dtw': -1.0},\n"
        "    {'text': ' test', **timing(middle, end), 'id': 102, 'p': 0.625, 't_dtw': 12.5},\n"
        "]\n"
        "payload = {\n"
        " 'systeminfo': 'fake cpu build',\n"
        " 'model': {'type': 'fake', 'multilingual': True, 'vocab': 1, 'audio': {'ctx': 1, 'state': 1, 'head': 1, 'layer': 1}, 'text': {'ctx': 1, 'state': 1, 'head': 1, 'layer': 1}, 'mels': 80, 'ftype': 1},\n"
        " 'params': {'model': arg('--model'), 'language': arg('--language'), 'translate': False},\n"
        " 'result': {'language': 'en' if arg('--language') == 'auto' else arg('--language')},\n"
        " 'transcription': [{**timing(offset, end), 'text': ' HIMR test', 'tokens': tokens}],\n"
        "}\n"
        "Path(arg('--output-file') + '.json').write_text(json.dumps(payload), encoding='utf-8')\n",
        encoding="utf-8",
    )
    path.chmod(0o755)


def write_json(path: Path, value: object) -> None:
    path.write_text(json.dumps(value, indent=2) + "\n", encoding="utf-8")


def validate_schema(instance: object, schema_name: str) -> None:
    try:
        import jsonschema
    except ImportError:
        return
    schema = json.loads(
        (PIPELINE_ROOT / "schemas" / schema_name).read_text(encoding="utf-8")
    )
    jsonschema.Draft202012Validator.check_schema(schema)
    jsonschema.validate(instance, schema)


class WhisperCppASRTests(unittest.TestCase):
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
        self.audio = self.case / "audio-16khz-mono-s16.flac"
        generate_audio(self.audio)
        self.model = self.case / "ggml-fake-model.bin"
        self.model.write_bytes(b"fake pinned whisper model\n")
        self.executable = self.case / "whisper-cli-fake"
        make_fake_whisper(self.executable)
        self.glossary = self.case / "neutral-glossary.json"
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

    def order(
        self,
        *,
        output_root: Path | None = None,
        audio: Path | None = None,
        with_glossary: bool = True,
        with_context: bool = True,
    ) -> dict:
        audio = audio or self.audio
        audio_sha = digest(audio)
        return {
            "schema_version": 1,
            "job_id": "asr-fixture-001",
            "input": {
                "path": str(audio.resolve()),
                "expected_sha256": audio_sha,
                "media_id": f"media_sha256_{audio_sha}",
                "artifact_id": "artifact_normalized_audio_fixture",
                "parent_processing_run_id": "run_preprocess_fixture",
            },
            "engine": {
                "executable": str(self.executable.resolve()),
                "expected_sha256": digest(self.executable),
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
                "path": str(self.model.resolve()),
                "expected_sha256": digest(self.model),
                "model_id": "model_whisper_fake_v1",
                "name": "fake whisper fixture",
                "revision": "fake-v1",
                "source": "local test fixture",
                "license_label": "test-only",
            },
            "window": {"offset_ms": 250, "duration_ms": 1_250},
            "inference": {
                "language": "en",
                "threads": 4,
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
                "path": str(self.glossary.resolve()),
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
            "output": {
                "root": str((output_root or (self.case / "output")).resolve())
            },
        }

    def execute(self, value: dict, *arguments: str) -> subprocess.CompletedProcess[str]:
        work_order = self.case / "work-order.json"
        write_json(work_order, value)
        return run(
            [
                "python3",
                str(PROGRAM),
                "run",
                "--work-order",
                str(work_order),
                *arguments,
            ],
            check=False,
        )

    def test_full_fake_run_preserves_provenance_scores_and_catalog_rows(self) -> None:
        order = self.order()
        before = self.audio.stat()
        completed = self.execute(order)
        self.assertEqual(completed.returncode, 0, completed.stderr)
        result = json.loads(completed.stdout)
        validate_schema(result, "asr-whispercpp-result.schema.json")
        self.assertEqual(result["status"], "completed")
        self.assertFalse(result["dry_run"])
        self.assertNotEqual(
            result["recipe_id"], result["processing_run"]["processing_run_id"]
        )
        self.assertTrue(result["recipe_id"].startswith("recipe_asr_whispercpp_"))
        self.assertTrue(
            result["processing_run"]["processing_run_id"].startswith(
                "run_asr_whispercpp_"
            )
        )
        self.assertRegex(result["processing_run"]["started_at"], r"Z$")
        self.assertRegex(result["processing_run"]["completed_at"], r"Z$")
        self.assertEqual(result["input"]["probe"]["sample_format"], "s16")
        self.assertTrue(result["input"]["unchanged"])
        self.assertEqual(self.audio.stat().st_size, before.st_size)
        self.assertEqual(self.audio.stat().st_mtime_ns, before.st_mtime_ns)

        transcript = result["transcript"]
        self.assertEqual(transcript["segment_count"], 1)
        self.assertEqual(transcript["token_count"], 3)
        self.assertEqual(transcript["segments"][0]["start_ms"], 250)
        self.assertEqual(transcript["segments"][0]["end_ms"], 1050)
        self.assertEqual(transcript["quality_flags"], [])
        self.assertEqual(transcript["segments"][0]["quality_flags"], [])
        self.assertEqual(transcript["segments"][0]["window_overrun_ms"], 0)
        first_token = transcript["segments"][0]["tokens"][0]
        self.assertEqual(first_token["text"], "[_BEG_]")
        self.assertEqual(first_token["start_ms"], first_token["end_ms"])
        self.assertNotIn("timing_state", first_token)
        self.assertNotIn("timing_quality_flags", first_token)
        self.assertNotIn("original_offsets", first_token)
        lexical_token = transcript["segments"][0]["tokens"][1]
        self.assertEqual(lexical_token["raw_probability"], 0.875)
        self.assertEqual(lexical_token["raw_dtw_timestamp"], -1.0)
        self.assertIn('"p":0.875', lexical_token["metadata_json"])

        command = result["commands"][-1]
        probe_command = result["commands"][0]
        self.assertRegex(probe_command[-1], r"^/proc/self/fd/[0-9]+$")
        self.assertRegex(command[0], r"^/proc/self/fd/[0-9]+$")
        self.assertRegex(
            command[command.index("--model") + 1], r"^/proc/self/fd/[0-9]+$"
        )
        self.assertRegex(
            command[command.index("--file") + 1], r"^/proc/self/fd/[0-9]+$"
        )
        self.assertIn("--output-json-full", command)
        self.assertIn("--no-gpu", command)
        self.assertIn("--prompt", command)
        prompt = command[command.index("--prompt") + 1]
        self.assertEqual(
            prompt, "Vocabulary terms, spelling only: HIMR, HIMRFAM, Dobby"
        )
        recipe = json.loads(result["processing_run"]["parameters_json"])
        self.assertEqual(
            recipe["descriptor_execution_policy"], DESCRIPTOR_EXECUTION_POLICY
        )
        environment = json.loads(result["processing_run"]["environment_json"])
        provenance = environment["command_provenance"]
        self.assertEqual(
            provenance["descriptor_execution_policy"],
            DESCRIPTOR_EXECUTION_POLICY,
        )
        self.assertEqual(
            provenance["result_commands_definition"],
            "exact_child_facing_argv_v1",
        )
        self.assertEqual(
            provenance["result_command_states"], ["executed", "executed"]
        )
        logical_probe, logical_whisper = provenance["logical_commands"]
        self.assertEqual(logical_probe[-1], str(self.audio.resolve()))
        self.assertEqual(logical_whisper[0], str(self.executable.resolve()))
        self.assertEqual(
            logical_whisper[logical_whisper.index("--model") + 1],
            str(self.model.resolve()),
        )
        self.assertEqual(
            logical_whisper[logical_whisper.index("--file") + 1],
            str(self.audio.resolve()),
        )
        self.assertEqual(
            logical_whisper[logical_whisper.index("--output-file") + 1],
            str(Path(result["result_path"]).parent / "whisper-output"),
        )

        self.assertEqual(
            result["catalog_records"]["processing_runs"][0],
            result["processing_run"],
        )
        self.assertEqual(
            result["catalog_records"]["run_inputs"][0], result["run_input"]
        )
        self.assertEqual(result["catalog_records"]["artifacts"], result["artifacts"])
        revision = result["catalog_records"]["transcript_revisions"][0]
        self.assertEqual(revision["review_state"], "machine")
        self.assertEqual(revision["revision_kind"], "contextual_asr")
        self.assertNotIn("publication", revision)
        for artifact in result["artifacts"]:
            self.assertEqual(artifact["visibility"], "private")
            expected = "artifact_" + hashlib.sha256(
                json.dumps(
                    [
                        result["processing_run"]["processing_run_id"],
                        artifact["artifact_kind"],
                        artifact["sha256"],
                    ],
                    ensure_ascii=False,
                    sort_keys=True,
                    separators=(",", ":"),
                ).encode("utf-8")
            ).hexdigest()[:32]
            self.assertEqual(artifact["artifact_id"], expected)
        result_path = Path(result["result_path"])
        self.assertEqual(json.loads(result_path.read_text(encoding="utf-8")), result)

        reused = self.execute(order)
        self.assertEqual(reused.returncode, 0, reused.stderr)
        self.assertEqual(reused.stdout, completed.stdout)

    def test_inverted_token_offsets_are_unavailable_without_invented_precision(self) -> None:
        self.executable = self.case / "whisper-cli-fake-inverted"
        make_fake_whisper(self.executable)
        completed = self.execute(self.order(with_glossary=False))
        self.assertEqual(completed.returncode, 0, completed.stderr)
        result = json.loads(completed.stdout)
        validate_schema(result, "asr-whispercpp-result.schema.json")

        transcript = result["transcript"]
        self.assertEqual(transcript["quality_flags"], ["token_timing_unavailable"])
        segment = transcript["segments"][0]
        self.assertEqual((segment["start_ms"], segment["end_ms"]), (250, 1_050))
        self.assertEqual(segment["quality_flags"], ["token_timing_unavailable"])
        token = segment["tokens"][1]
        self.assertIsNone(token["start_ms"])
        self.assertIsNone(token["end_ms"])
        self.assertEqual(token["timing_state"], "unavailable")
        self.assertEqual(
            token["timing_quality_flags"], ["invalid_upstream_inverted"]
        )
        self.assertEqual(token["original_offsets"], {"from": 250, "to": 0})
        self.assertEqual(
            json.loads(token["metadata_json"])["offsets"],
            {"from": 250, "to": 0},
        )

        catalog_segment = result["catalog_records"]["transcript_segments"][0]
        catalog_metadata = json.loads(catalog_segment["metadata_json"])
        self.assertEqual(
            catalog_metadata["token_timing_anomalies"],
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
            catalog_metadata["engine_segment"]["tokens"][1]["offsets"],
            {"from": 250, "to": 0},
        )
        revision_metadata = json.loads(
            result["catalog_records"]["transcript_revisions"][0]["metadata_json"]
        )
        self.assertEqual(
            revision_metadata["quality_flags"], ["token_timing_unavailable"]
        )

    def test_exact_real_inversion_shape_preserves_original_offsets(self) -> None:
        raw_token = {
            "text": " fictional",
            "offsets": {"from": 31_000, "to": 30_000},
            "id": 1867,
            "p": 0.0289736,
            "t_dtw": -1.0,
        }
        transcript = normalize_engine_output(
            {
                "params": {"translate": False},
                "result": {"language": "en"},
                "transcription": [
                    {
                        "offsets": {"from": 31_000, "to": 33_000},
                        "text": " fictional",
                        "tokens": [raw_token],
                    }
                ],
            },
            requested_language="en",
            window={"offset_ms": 0, "duration_ms": 60_000, "end_ms": 60_000},
        )
        segment = transcript["segments"][0]
        token = segment["tokens"][0]
        self.assertEqual((segment["start_ms"], segment["end_ms"]), (31_000, 33_000))
        self.assertEqual((token["start_ms"], token["end_ms"]), (None, None))
        self.assertEqual(token["original_offsets"], {"from": 31_000, "to": 30_000})
        self.assertEqual(token["timing_state"], "unavailable")
        self.assertEqual(token["timing_quality_flags"], ["invalid_upstream_inverted"])
        self.assertEqual(json.loads(token["metadata_json"]), raw_token)

    def test_malformed_or_observed_out_of_bounds_offsets_still_fail(self) -> None:
        for mode, expected_message in (
            ("negative", "offsets.to must be between 0"),
            ("noninverted-outside", "token timestamp lies outside its segment"),
        ):
            with self.subTest(mode=mode):
                self.executable = self.case / f"whisper-cli-fake-{mode}"
                make_fake_whisper(self.executable)
                failed = self.execute(self.order(with_glossary=False))
                self.assertEqual(failed.returncode, 2)
                self.assertIn(
                    expected_message,
                    json.loads(failed.stderr)["error"]["message"],
                )

    def test_dry_run_is_no_write_with_stable_recipe(self) -> None:
        output_root = self.case / "output"
        order = self.order(output_root=output_root)
        first = self.execute(order, "--dry-run")
        second = self.execute(order, "--dry-run")
        self.assertEqual(first.returncode, 0, first.stderr)
        self.assertEqual(second.returncode, 0, second.stderr)
        first_result = json.loads(first.stdout)
        second_result = json.loads(second.stdout)
        validate_schema(first_result, "asr-whispercpp-result.schema.json")
        self.assertEqual(first_result["status"], "planned")
        self.assertEqual(first_result["recipe_id"], second_result["recipe_id"])
        self.assertNotEqual(
            first_result["processing_run"]["processing_run_id"],
            second_result["processing_run"]["processing_run_id"],
        )
        self.assertFalse(output_root.exists())

    def test_tampered_completed_artifact_is_never_overwritten_or_reused(self) -> None:
        order = self.order()
        first = self.execute(order)
        self.assertEqual(first.returncode, 0, first.stderr)
        result = json.loads(first.stdout)
        raw_artifact = next(
            artifact
            for artifact in result["artifacts"]
            if artifact["artifact_kind"] == "whispercpp_output_json_full"
        )
        raw_path = Path(raw_artifact["storage_uri"].removeprefix("file://"))
        original_result = Path(result["result_path"]).read_bytes()
        raw_path.write_text("{}\n", encoding="utf-8")

        second = self.execute(order)
        self.assertEqual(second.returncode, 2)
        failure = json.loads(second.stderr)
        self.assertIn("artifact hash mismatch", failure["error"]["message"])
        self.assertEqual(Path(result["result_path"]).read_bytes(), original_result)
        self.assertEqual(raw_path.read_text(encoding="utf-8"), "{}\n")

    def test_invalid_utf8_is_exactly_quarantined_replayed_and_tamper_evident(self) -> None:
        self.executable = self.case / "whisper-cli-fake-invalid-utf8"
        make_fake_whisper(self.executable)
        order = self.order(with_glossary=False)
        output_root = Path(order["output"]["root"])

        first = self.execute(order)
        self.assertEqual(first.returncode, 2)
        self.assertEqual(first.stdout, "")
        self.assertNotIn("Traceback", first.stderr)
        failure = json.loads(first.stderr)
        validate_schema(failure, "asr-whispercpp-result.schema.json")
        self.assertEqual(failure["status"], "failed")
        self.assertEqual(failure["error"]["type"], "InvalidUTF8OutputError")
        self.assertEqual(failure["error"], failure["errors"][0])

        quarantine = failure["quarantine"]
        raw_path = Path(quarantine["raw_artifact"]["path"])
        receipt_path = Path(quarantine["receipt_path"])
        self.assertEqual(raw_path.read_bytes(), INVALID_UTF8_OUTPUT)
        self.assertEqual(quarantine["raw_artifact"]["byte_count"], len(INVALID_UTF8_OUTPUT))
        self.assertEqual(quarantine["raw_artifact"]["sha256"], hashlib.sha256(INVALID_UTF8_OUTPUT).hexdigest())
        self.assertEqual(raw_path.stat().st_mode & 0o777, 0o400)
        self.assertEqual(receipt_path.stat().st_mode & 0o777, 0o400)
        self.assertEqual(raw_path.parent.stat().st_mode & 0o777, 0o500)
        self.assertEqual(raw_path.stat().st_nlink, 1)
        self.assertEqual(receipt_path.stat().st_nlink, 1)

        receipt_body = receipt_path.read_bytes()
        receipt = json.loads(receipt_body)
        validate_schema(receipt, "asr-whispercpp-quarantine-receipt.schema.json")
        self.assertEqual(quarantine["receipt_sha256"], hashlib.sha256(receipt_body).hexdigest())
        self.assertEqual(receipt["failure_key"], quarantine["failure_key"])
        self.assertEqual(receipt["raw_artifact"], quarantine["raw_artifact"])
        self.assertFalse(receipt["safety"]["invalid_bytes_ignored_or_replaced"])
        self.assertFalse(receipt["safety"]["decoded_text_emitted"])
        self.assertEqual(
            receipt["invalid_utf8"]["start_byte"],
            INVALID_UTF8_OUTPUT.index(bytes.fromhex("f0")),
        )

        result_dir = (
            output_root
            / "asr"
            / "whispercpp"
            / "sha256"
            / digest(self.audio)[:2]
            / digest(self.audio)
            / "results"
            / receipt["lineage"]["result_key"]
        )
        self.assertFalse(result_dir.exists())
        self.assertEqual(list(output_root.rglob(".*.tmp-*")), [])

        raw_stat = raw_path.stat()
        receipt_stat = receipt_path.stat()
        second = self.execute(order)
        self.assertEqual(second.returncode, 2)
        self.assertNotIn("Traceback", second.stderr)
        second_failure = json.loads(second.stderr)
        validate_schema(second_failure, "asr-whispercpp-result.schema.json")
        self.assertEqual(second_failure["quarantine"], quarantine)
        self.assertEqual(raw_path.read_bytes(), INVALID_UTF8_OUTPUT)
        self.assertEqual(receipt_path.read_bytes(), receipt_body)
        self.assertEqual(raw_path.stat().st_mtime_ns, raw_stat.st_mtime_ns)
        self.assertEqual(receipt_path.stat().st_mtime_ns, receipt_stat.st_mtime_ns)
        self.assertFalse(result_dir.exists())

        try:
            raw_path.chmod(0o600)
            raw_path.write_bytes(b"tampered quarantine bytes")
            raw_path.chmod(0o400)
            tampered = self.execute(order)
            self.assertEqual(tampered.returncode, 2)
            self.assertNotIn("Traceback", tampered.stderr)
            tamper_failure = json.loads(tampered.stderr)
            validate_schema(tamper_failure, "asr-whispercpp-result.schema.json")
            self.assertIn(
                "failed exact immutable replay",
                tamper_failure["error"]["message"],
            )
            self.assertEqual(raw_path.read_bytes(), b"tampered quarantine bytes")
            self.assertFalse(result_dir.exists())
        finally:
            raw_path.parent.chmod(0o700)
            raw_path.chmod(0o600)
            receipt_path.chmod(0o600)

    def test_stable_file_read_detects_same_length_mtime_restored_overwrite(self) -> None:
        path = self.case / "race-output.json"
        original = b'{"value":"aaaa"}'
        replacement = b'{"value":"bbbb"}'
        self.assertEqual(len(original), len(replacement))
        path.write_bytes(original)
        before = path.stat()
        real_read = os.read
        changed = False

        def overwrite_after_read(descriptor: int, count: int) -> bytes:
            nonlocal changed
            chunk = real_read(descriptor, count)
            if chunk and not changed:
                changed = True
                path.write_bytes(replacement)
                os.utime(
                    path,
                    ns=(before.st_atime_ns, before.st_mtime_ns),
                )
            return chunk

        with mock.patch(
            "pipeline.asr_whispercpp.os.read",
            side_effect=overwrite_after_read,
        ):
            with self.assertRaisesRegex(ASRError, "changed while being read"):
                stable_file_bytes(
                    path,
                    maximum_bytes=len(original),
                    label="race fixture",
                )
        self.assertTrue(changed)
        self.assertEqual(path.read_bytes(), replacement)
        self.assertEqual(path.stat().st_mtime_ns, before.st_mtime_ns)

    def test_retained_descriptor_survives_path_swap_and_restore_is_rejected(self) -> None:
        path = self.case / "retained-fixture.bin"
        backup = self.case / "retained-fixture.original"
        original = b"verified original bytes\n"
        replacement = b"unverified replacement!\n"
        self.assertEqual(len(original), len(replacement))
        path.write_bytes(original)

        with retained_verified_file(
            path,
            hashlib.sha256(original).hexdigest(),
            "retained fixture",
        ) as retained:
            path.rename(backup)
            path.write_bytes(replacement)
            self.assertEqual(Path(retained.proc_path).read_bytes(), original)
            path.unlink()
            backup.rename(path)
            self.assertEqual(path.read_bytes(), original)
            with self.assertRaisesRegex(
                ASRError, "changed during ASR execution"
            ):
                verify_retained_file(retained, "retained fixture")

    def test_retained_descriptor_transport_fails_closed_without_procfs(self) -> None:
        path = self.case / "retained-fixture.bin"
        body = b"verified bytes\n"
        path.write_bytes(body)
        with mock.patch(
            "pipeline.asr_whispercpp.PROC_SELF_FD_ROOT",
            self.case / "missing-proc-self-fd",
        ):
            with self.assertRaisesRegex(
                ASRError, "mounted Linux /proc/self/fd descriptor transport"
            ):
                with retained_verified_file(
                    path,
                    hashlib.sha256(body).hexdigest(),
                    "retained fixture",
                ):
                    self.fail("descriptor transport unexpectedly opened")

    def test_run_wrapper_final_check_rejects_swap_restored_before_return(self) -> None:
        backup = self.audio.with_name("audio.original.flac")
        original = self.audio.read_bytes()

        def swap_and_restore(*args: object, **kwargs: object) -> dict:
            self.audio.rename(backup)
            self.audio.write_bytes(b"replacement audio bytes")
            self.audio.unlink()
            backup.rename(self.audio)
            return {"status": "planned"}

        with mock.patch(
            "pipeline.asr_whispercpp._run_asr_retained",
            side_effect=swap_and_restore,
        ):
            with self.assertRaisesRegex(ASRError, "changed during ASR execution"):
                run_asr(self.order(), dry_run=True)
        self.assertEqual(self.audio.read_bytes(), original)
        self.assertFalse(backup.exists())

    def test_execution_boundary_rejects_only_the_exact_legacy_engine_profile(self) -> None:
        legacy = {
            "sha256": "4831024debb4e60e9433d27967ba6dae033d4b0c770c4ef208c16fc5a8fe77d6",
            "byte_count": 1_010_544,
        }
        with self.assertRaisesRegex(ASRError, "validation-only.*cannot be executed"):
            reject_execution_ineligible_engine(legacy)

        reject_execution_ineligible_engine(
            {
                "sha256": "36be94accd60116933073e8069964c02e888e3681967184ce7fecfd5f980ae1a",
                "byte_count": 1_020_432,
            }
        )
        reject_execution_ineligible_engine(
            {"sha256": "d" * 64, "byte_count": 123}
        )

    def test_non_normalized_audio_and_out_of_bounds_window_fail(self) -> None:
        s32 = self.case / "audio-s32.flac"
        generate_audio(s32, sample_format="s32")
        invalid_audio = self.execute(self.order(audio=s32))
        self.assertEqual(invalid_audio.returncode, 2)
        self.assertIn(
            "not normalized 16 kHz mono s16 FLAC",
            json.loads(invalid_audio.stderr)["error"]["message"],
        )

        order = self.order()
        order["window"] = {"offset_ms": 2_000, "duration_ms": 1_000}
        invalid_window = self.execute(order)
        self.assertEqual(invalid_window.returncode, 2)
        self.assertIn(
            "exceeds the input audio duration",
            json.loads(invalid_window.stderr)["error"]["message"],
        )

    def test_bounded_engine_window_overrun_is_preserved_and_flagged(self) -> None:
        order = self.order(with_glossary=False)
        order["window"] = {"offset_ms": 250, "duration_ms": 500}
        completed = self.execute(order)
        self.assertEqual(completed.returncode, 0, completed.stderr)
        result = json.loads(completed.stdout)
        validate_schema(result, "asr-whispercpp-result.schema.json")
        transcript = result["transcript"]
        self.assertEqual(
            transcript["quality_flags"], ["segment_end_after_requested_window"]
        )
        segment = transcript["segments"][0]
        self.assertEqual(segment["end_ms"], 850)
        self.assertEqual(segment["window_overrun_ms"], 100)
        self.assertEqual(segment["quality_flags"], ["end_after_requested_window"])
        catalog_segment = result["catalog_records"]["transcript_segments"][0]
        catalog_metadata = json.loads(catalog_segment["metadata_json"])
        self.assertEqual(catalog_metadata["window_overrun_ms"], 100)
        self.assertEqual(
            catalog_metadata["quality_flags"], ["end_after_requested_window"]
        )

    def test_hash_unknown_key_and_root_policy_fail_with_envelopes(self) -> None:
        wrong_hash = self.order()
        wrong_hash["model"]["expected_sha256"] = "0" * 64
        failed = self.execute(wrong_hash)
        self.assertEqual(failed.returncode, 2)
        envelope = json.loads(failed.stderr)
        validate_schema(envelope, "asr-whispercpp-result.schema.json")
        self.assertEqual(envelope["status"], "failed")
        self.assertEqual(envelope["job_id"], "asr-fixture-001")
        self.assertIn("model SHA-256 mismatch", envelope["error"]["message"])

        unknown = self.order()
        unknown["inference"]["mystery"] = True
        failed = self.execute(unknown)
        self.assertEqual(failed.returncode, 2)
        self.assertIn("unsupported keys", json.loads(failed.stderr)["error"]["message"])

        root = self.order(output_root=Path("/"))
        failed = self.execute(root)
        self.assertEqual(failed.returncode, 2)
        self.assertIn(
            "may not be the filesystem root",
            json.loads(failed.stderr)["error"]["message"],
        )

    def test_null_catalog_context_omits_transcript_catalog_rows(self) -> None:
        completed = self.execute(self.order(with_glossary=False, with_context=False))
        self.assertEqual(completed.returncode, 0, completed.stderr)
        result = json.loads(completed.stdout)
        self.assertIsNone(result["catalog_context"])
        self.assertIsNone(result["glossary"])
        self.assertEqual(
            set(result["catalog_records"]),
            {"processing_runs", "run_inputs", "artifacts"},
        )

    def test_tracked_examples_conform_to_strict_schemas(self) -> None:
        order_example = json.loads(
            (PIPELINE_ROOT / "examples" / "asr-whispercpp-work-order.example.json").read_text(
                encoding="utf-8"
            )
        )
        glossary_example = json.loads(
            (PIPELINE_ROOT / "examples" / "neutral-glossary.example.json").read_text(
                encoding="utf-8"
            )
        )
        validate_schema(order_example, "asr-whispercpp-work-order.schema.json")
        validate_schema(glossary_example, "neutral-glossary.schema.json")


if __name__ == "__main__":
    unittest.main()
