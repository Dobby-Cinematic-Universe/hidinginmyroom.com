from __future__ import annotations

import ast
import copy
import hashlib
import json
import os
import shutil
import stat
import subprocess
import sys
import unittest
from pathlib import Path
from unittest import mock


REPOSITORY_ROOT = Path(__file__).resolve().parents[2]
PIPELINE_ROOT = REPOSITORY_ROOT / "pipeline"
TEST_ROOT = (
    PIPELINE_ROOT / ".test-work" / f"contextual-asr-batch-{os.getpid()}"
)
sys.path.insert(0, str(PIPELINE_ROOT))

import contextual_asr_batch as batch  # noqa: E402


TRANSCRIPT_SENTINEL = "SECRETBASELINETEXT"
GLOSSARY_SENTINELS = ("ZORBGLOSSARYTERM", "HIMRFAM")


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
        check=True,
    )


def generate_audio(path: Path, frequency: int) -> None:
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
            f"sine=frequency={frequency}:sample_rate=16000:duration=1",
            "-ac",
            "1",
            "-ar",
            "16000",
            "-sample_fmt",
            "s16",
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
        "    print('whisper.cpp fixture current')\n"
        "    raise SystemExit(0)\n"
        "def arg(name): return sys.argv[sys.argv.index(name) + 1]\n"
        "offset = int(arg('--offset-t'))\n"
        "duration = int(arg('--duration'))\n"
        "end = min(offset + duration, offset + 800)\n"
        "middle = offset + (end - offset) // 2\n"
        "def stamp(ms):\n"
        "    h, rem = divmod(ms, 3600000)\n"
        "    m, rem = divmod(rem, 60000)\n"
        "    s, rem = divmod(rem, 1000)\n"
        "    return f'{h:02d}:{m:02d}:{s:02d},{rem:03d}'\n"
        "def timing(a, b):\n"
        "    return {'timestamps': {'from': stamp(a), 'to': stamp(b)}, "
        "'offsets': {'from': a, 'to': b}}\n"
        "tokens = [\n"
        " {'text': '[_BEG_]', **timing(offset, offset), 'id': 50363, "
        "'p': 1.0, 't_dtw': -1.0},\n"
        f" {{'text': ' {TRANSCRIPT_SENTINEL}', **timing(offset, middle), "
        "'id': 101, 'p': 0.875, 't_dtw': -1.0},\n"
        " {'text': ' fixture', **timing(middle, end), 'id': 102, "
        "'p': 0.625, 't_dtw': 12.5},\n"
        "]\n"
        "payload = {\n"
        " 'systeminfo': 'fixture cpu build',\n"
        " 'model': {'type': 'fixture', 'multilingual': True, 'vocab': 1, "
        "'audio': {'ctx': 1, 'state': 1, 'head': 1, 'layer': 1}, "
        "'text': {'ctx': 1, 'state': 1, 'head': 1, 'layer': 1}, "
        "'mels': 80, 'ftype': 1},\n"
        " 'params': {'model': arg('--model'), 'language': arg('--language'), "
        "'translate': False},\n"
        " 'result': {'language': arg('--language')},\n"
        f" 'transcription': [{{**timing(offset, end), "
        f"'text': ' {TRANSCRIPT_SENTINEL} fixture', 'tokens': tokens}}],\n"
        "}\n"
        "Path(arg('--output-file') + '.json').write_text(" 
        "json.dumps(payload), encoding='utf-8')\n",
        encoding="utf-8",
    )
    path.chmod(0o700)


def remove_test_tree() -> None:
    if not TEST_ROOT.exists():
        return
    for current, directories, files in os.walk(TEST_ROOT):
        Path(current).chmod(0o700)
        for name in directories:
            path = Path(current) / name
            if not path.is_symlink():
                path.chmod(0o700)
        for name in files:
            path = Path(current) / name
            if not path.is_symlink():
                path.chmod(0o600)
    shutil.rmtree(TEST_ROOT)


def validate_schema(instance: object, name: str) -> None:
    import jsonschema

    schema = json.loads(
        (PIPELINE_ROOT / "schemas" / name).read_text(encoding="utf-8")
    )
    jsonschema.Draft202012Validator.check_schema(schema)
    jsonschema.validate(instance, schema)


class ContextualASRBatchTests(unittest.TestCase):
    def setUp(self) -> None:
        if not shutil.which("ffmpeg") or not shutil.which("ffprobe"):
            self.skipTest("ffmpeg and ffprobe are required")
        remove_test_tree()
        TEST_ROOT.mkdir(parents=True, mode=0o700)
        os.chmod(TEST_ROOT, 0o700)
        self.case = TEST_ROOT / self._testMethodName
        self.case.mkdir(mode=0o700)
        self.engine = self.case / "whisper-cli-fixture"
        make_fake_whisper(self.engine)
        self.model = self.case / "ggml-fixture-model.bin"
        self.model.write_bytes(b"fixture model bytes\n")
        current = copy.deepcopy(
            next(
                profile
                for profile in batch.whispercpp_engine_profiles.ENGINE_PROFILES
                if profile["admission"] == "current_new_batch"
            )
        )
        current.update(
            {
                "profile_id": "whispercpp-fixture-current",
                "expected_sha256": digest(self.engine),
                "byte_count": self.engine.stat().st_size,
                "version_label": "whisper.cpp fixture current",
            }
        )
        self.current_profile = current
        self.profile_patcher = mock.patch.object(
            batch.whispercpp_engine_profiles,
            "ENGINE_PROFILES",
            (current,),
        )
        self.profile_patcher.start()
        self.glossary = self.case / "neutral-glossary.json"
        self.glossary.write_bytes(
            batch.pretty_bytes(
                {
                    "schema_version": 1,
                    "glossary_revision_id": "glossary_fixture_neutral_v1",
                    "revision": "2026-08-27.1",
                    "language": "en",
                    "terms": list(GLOSSARY_SENTINELS),
                }
            )
        )
        self.glossary.chmod(0o400)
        self.batch_root = self.case / "private-contextual-batches"
        self.contextual_output = self.case / "private-contextual-results"
        self.raw_results: list[Path] = []

    def tearDown(self) -> None:
        self.profile_patcher.stop()
        remove_test_tree()

    def inference(self, *, beam_size: int = 5, language: str = "en") -> dict:
        return {
            "language": language,
            "threads": 2,
            "translate": False,
            "split_on_word": True,
            "best_of": 5,
            "beam_size": beam_size,
            "max_segment_characters": 0,
            "word_threshold": 0.01,
            "entropy_threshold": 2.4,
            "logprob_threshold": -1.0,
            "no_speech_threshold": 0.6,
            "temperature": 0.0,
            "temperature_increment": 0.2,
            "no_fallback": False,
            "timeout_seconds": 10,
        }

    def raw_order(
        self,
        ordinal: int,
        audio: Path,
        *,
        duration_ms: int = 1_000,
        beam_size: int = 5,
        with_glossary: bool = False,
    ) -> dict:
        audio_sha = digest(audio)
        engine = batch.whispercpp_engine_profiles.public_engine_document(
            self.current_profile, str(self.engine.resolve())
        )
        engine.pop("byte_count")
        return {
            "schema_version": 1,
            "job_id": f"asr-raw-fixture-{ordinal:03d}",
            "input": {
                "path": str(audio.resolve()),
                "expected_sha256": audio_sha,
                "media_id": f"media_sha256_{audio_sha}",
                "artifact_id": f"artifact_normalized_fixture_{ordinal:03d}",
                "parent_processing_run_id": f"run_preprocess_fixture_{ordinal:03d}",
            },
            "engine": engine,
            "model": {
                "path": str(self.model.resolve()),
                "expected_sha256": digest(self.model),
                "model_id": "model_whisper_fixture_v1",
                "name": "fixture whisper model",
                "revision": "fixture-v1",
                "source": "local test fixture",
                "license_label": "test-only",
            },
            "window": {"offset_ms": 0, "duration_ms": duration_ms},
            "inference": self.inference(beam_size=beam_size),
            "glossary": {
                "path": str(self.glossary.resolve()),
                "expected_sha256": digest(self.glossary),
            }
            if with_glossary
            else None,
            "catalog_context": None,
            "output": {"root": str((self.case / "raw-results").resolve())},
        }

    def make_raw_result(
        self,
        ordinal: int,
        frequency: int,
        *,
        duration_ms: int = 1_000,
        beam_size: int = 5,
        with_glossary: bool = False,
    ) -> Path:
        audio = self.case / "inputs" / f"audio-{ordinal:03d}.flac"
        generate_audio(audio, frequency)
        order = batch.asr_whispercpp.validate_work_order(
            self.raw_order(
                ordinal,
                audio,
                duration_ms=duration_ms,
                beam_size=beam_size,
                with_glossary=with_glossary,
            )
        )
        result = batch.asr_whispercpp.run_asr(order, dry_run=False)
        path = Path(result["result_path"])
        self.assertEqual(
            {item.name for item in path.parent.iterdir()},
            {"result.json", "transcript.normalized.json", "whisper.raw.json"},
        )
        for child in path.parent.iterdir():
            child.chmod(0o400)
        path.parent.chmod(0o500)
        self.raw_results.append(path)
        return path

    def materialize(self, baselines: list[Path]) -> tuple[dict, Path]:
        return batch.materialize_batch(
            baseline_paths=baselines,
            glossary_path=self.glossary.resolve(),
            batch_root=self.batch_root.resolve(),
            asr_output_root=self.contextual_output.resolve(),
        )

    def assert_mode(self, path: Path, expected: int) -> None:
        self.assertEqual(stat.S_IMODE(path.lstat().st_mode), expected)

    def test_materialize_is_order_independent_text_free_and_exactly_replayable(
        self,
    ) -> None:
        first = self.make_raw_result(1, 440)
        second = self.make_raw_result(2, 550)
        manifest, manifest_path = self.materialize([second, first])
        validate_schema(manifest, "contextual-asr-batch-manifest.schema.json")
        self.assertEqual(manifest["work_order_count"], 2)
        self.assertEqual(manifest["profile"], batch.PROFILE)
        self.assertEqual(manifest["safety"], batch.SAFETY)
        self.assertFalse(manifest["safety"]["manifest_contains_transcript_text"])
        self.assertFalse(manifest["safety"]["manifest_contains_glossary_terms"])
        manifest_bytes = manifest_path.read_bytes()
        for value in (TRANSCRIPT_SENTINEL, *GLOSSARY_SENTINELS):
            self.assertNotIn(value.encode("utf-8"), manifest_bytes)
        self.assert_mode(manifest_path.parent, 0o500)
        self.assert_mode(manifest_path, 0o400)
        self.assert_mode(manifest_path.parent / "work-orders", 0o500)
        for entry in manifest["work_orders"]:
            order_path = manifest_path.parent / entry["path"]
            self.assert_mode(order_path, 0o400)
            order_body = order_path.read_bytes()
            for value in (TRANSCRIPT_SENTINEL, *GLOSSARY_SENTINELS):
                self.assertNotIn(value.encode("utf-8"), order_body)
            order = json.loads(order_body)
            validate_schema(order, "asr-whispercpp-work-order.schema.json")
            self.assertEqual(order["glossary"]["path"], str(self.glossary.resolve()))
            self.assertEqual(order["window"], {"offset_ms": 0, "duration_ms": 1_000})
            raw = json.loads(
                Path(entry["baseline"]["result_path"]).read_text(encoding="utf-8")
            )
            recipe = json.loads(raw["processing_run"]["parameters_json"])
            self.assertEqual(order["inference"], recipe["inference"])
            self.assertEqual(order["catalog_context"], raw["catalog_context"])
            self.assertEqual(order["input"]["path"], raw["input"]["path"])
            self.assertEqual(order["engine"]["expected_sha256"], raw["engine"]["sha256"])
            self.assertEqual(order["model"]["expected_sha256"], raw["model"]["sha256"])

        before = {
            str(path.relative_to(manifest_path.parent)): digest(path)
            for path in [
                manifest_path,
                *(manifest_path.parent / "work-orders").iterdir(),
            ]
        }
        replay, replay_path = self.materialize([first, second])
        self.assertEqual(replay_path, manifest_path)
        self.assertEqual(replay, manifest)
        after = {
            str(path.relative_to(manifest_path.parent)): digest(path)
            for path in [
                manifest_path,
                *(manifest_path.parent / "work-orders").iterdir(),
            ]
        }
        self.assertEqual(after, before)
        validated, orders = batch.validate_batch(manifest_path)
        self.assertEqual(validated, manifest)
        self.assertEqual(len(orders), 2)

    def test_dry_run_and_completed_run_preserve_raw_baseline(self) -> None:
        baseline = self.make_raw_result(1, 440)
        manifest, manifest_path = self.materialize([baseline])
        baseline_files = sorted(baseline.parent.iterdir())
        before = {
            path.name: (digest(path), path.stat().st_size, path.stat().st_mtime_ns)
            for path in baseline_files
        }
        planned = batch.run_batch(manifest_path, dry_run=True)
        validate_schema(planned, "contextual-asr-batch-run.schema.json")
        self.assertEqual(planned["status"], "planned")
        self.assertTrue(planned["dry_run"])
        self.assertEqual(planned["results"][0]["status"], "planned")
        self.assertFalse(self.contextual_output.exists())
        completed = batch.run_batch(manifest_path, dry_run=False)
        validate_schema(completed, "contextual-asr-batch-run.schema.json")
        self.assertEqual(completed["status"], "completed")
        self.assertFalse(completed["dry_run"])
        self.assertEqual(completed["results"][0]["status"], "completed")
        contextual_result = json.loads(
            Path(completed["results"][0]["result_path"]).read_text(encoding="utf-8")
        )
        self.assertEqual(
            contextual_result["glossary"]["glossary_revision_id"],
            manifest["glossary"]["glossary_revision_id"],
        )
        self.assertEqual(
            contextual_result["processing_run"]["glossary_revision_id"],
            manifest["glossary"]["glossary_revision_id"],
        )
        recipe = json.loads(contextual_result["processing_run"]["parameters_json"])
        self.assertIsNotNone(recipe["glossary"])
        after = {
            path.name: (digest(path), path.stat().st_size, path.stat().st_mtime_ns)
            for path in baseline_files
        }
        self.assertEqual(after, before)
        for path in baseline_files:
            self.assert_mode(path, 0o400)
        self.assert_mode(baseline.parent, 0o500)
        batch.validate_batch(manifest_path)

    def test_rejects_contextual_partial_legacy_and_mixed_decode_baselines(self) -> None:
        raw = self.make_raw_result(1, 440)
        contextual = self.make_raw_result(2, 550, with_glossary=True)
        partial = self.make_raw_result(3, 660, duration_ms=500)
        mixed = self.make_raw_result(4, 770, beam_size=4)
        with self.assertRaisesRegex(batch.ContextualBatchError, "current completed"):
            self.materialize([contextual])
        with self.assertRaisesRegex(batch.ContextualBatchError, "full normalized input"):
            self.materialize([partial])
        with self.assertRaisesRegex(batch.ContextualBatchError, "decoding policy"):
            self.materialize([raw, mixed])
        legacy = copy.deepcopy(self.current_profile)
        legacy["admission"] = "legacy_manifest_replay_only"
        with mock.patch.object(
            batch.whispercpp_engine_profiles, "ENGINE_PROFILES", (legacy,)
        ):
            with self.assertRaisesRegex(batch.ContextualBatchError, "not eligible"):
                self.materialize([raw])

    def test_rejects_permissions_hardlinks_symlinks_and_unknown_entries(self) -> None:
        baseline = self.make_raw_result(1, 440)
        baseline.chmod(0o600)
        with self.assertRaisesRegex(batch.ContextualBatchError, "mode must be exactly"):
            self.materialize([baseline])
        baseline.chmod(0o400)

        glossary_link = self.case / "neutral-glossary-hardlink.json"
        os.link(self.glossary, glossary_link)
        with self.assertRaisesRegex(batch.ContextualBatchError, "exactly one hard link"):
            self.materialize([baseline])
        glossary_link.unlink()

        baseline.parent.chmod(0o700)
        unknown = baseline.parent / "unexpected.txt"
        unknown.write_text("not part of the envelope\n", encoding="utf-8")
        unknown.chmod(0o400)
        baseline.parent.chmod(0o500)
        with self.assertRaisesRegex(batch.ContextualBatchError, "exactly the three"):
            self.materialize([baseline])
        baseline.parent.chmod(0o700)
        unknown.unlink()
        baseline.parent.chmod(0o500)

        symlink = self.case / "baseline-result-link.json"
        symlink.symlink_to(baseline)
        with self.assertRaisesRegex(batch.ContextualBatchError, "already be resolved"):
            self.materialize([symlink])

        manifest, manifest_path = self.materialize([baseline])
        manifest_path.parent.chmod(0o700)
        extra = manifest_path.parent / "unexpected.txt"
        extra.write_text("immutable-layout-tamper\n", encoding="utf-8")
        extra.chmod(0o400)
        manifest_path.parent.chmod(0o500)
        with self.assertRaisesRegex(batch.ContextualBatchError, "extra entries"):
            batch.validate_batch(manifest_path)
        manifest_path.parent.chmod(0o700)
        extra.unlink()
        manifest_path.parent.chmod(0o500)
        self.assertEqual(batch.validate_batch(manifest_path)[0], manifest)

    def test_rejects_duplicate_targets_root_overlap_and_glossary_change(self) -> None:
        baseline = self.make_raw_result(1, 440)
        with self.assertRaisesRegex(batch.ContextualBatchError, "must be unique"):
            self.materialize([baseline, baseline])
        with self.assertRaisesRegex(batch.ContextualBatchError, "must be disjoint"):
            batch.build_batch(
                baseline_paths=[baseline],
                glossary_path=self.glossary.resolve(),
                batch_root=self.batch_root.resolve(),
                asr_output_root=(self.batch_root / "nested").resolve(),
            )
        _, manifest_path = self.materialize([baseline])
        self.glossary.chmod(0o600)
        old = self.glossary.read_bytes()
        changed = json.loads(old)
        changed["revision"] = "2026-08-27.2"
        self.glossary.write_bytes(batch.pretty_bytes(changed))
        self.glossary.chmod(0o400)
        with self.assertRaisesRegex(batch.ContextualBatchError, "deterministic replay"):
            batch.validate_batch(manifest_path)
        self.glossary.chmod(0o600)
        self.glossary.write_bytes(old)
        self.glossary.chmod(0o400)
        batch.validate_batch(manifest_path)

    def test_rejects_changed_input_engine_model_and_raw_artifact_bytes(self) -> None:
        baseline = self.make_raw_result(1, 440)
        envelope = json.loads(baseline.read_text(encoding="utf-8"))
        targets = [
            (Path(envelope["input"]["path"]), b"changed normalized input\n"),
            (Path(envelope["model"]["path"]), b"changed model\n"),
            (Path(envelope["engine"]["path"]), b"#!/bin/sh\nexit 1\n"),
        ]
        for target, replacement in targets:
            with self.subTest(target=target.name):
                original = target.read_bytes()
                original_mode = stat.S_IMODE(target.stat().st_mode)
                target.chmod(0o700 if os.access(target, os.X_OK) else 0o600)
                target.write_bytes(replacement)
                if original_mode & 0o100:
                    target.chmod(0o700)
                with self.assertRaises(batch.ContextualBatchError):
                    self.materialize([baseline])
                target.write_bytes(original)
                target.chmod(original_mode)

        normalized = baseline.parent / "transcript.normalized.json"
        original = normalized.read_bytes()
        normalized.chmod(0o600)
        normalized.write_bytes(b"{}\n")
        normalized.chmod(0o400)
        with self.assertRaises(batch.ContextualBatchError):
            self.materialize([baseline])
        normalized.chmod(0o600)
        normalized.write_bytes(original)
        normalized.chmod(0o400)
        self.materialize([baseline])

    def test_serial_failure_is_fail_fast_and_schema_valid(self) -> None:
        first = self.make_raw_result(1, 440)
        second = self.make_raw_result(2, 550)
        _, manifest_path = self.materialize([first, second])
        real_run = batch.asr_whispercpp.run_asr
        calls: list[str] = []

        def fail_second(order: dict, *, dry_run: bool) -> dict:
            calls.append(order["job_id"])
            if len(calls) == 2:
                raise batch.asr_whispercpp.ASRError("deliberate fixture failure")
            return real_run(order, dry_run=dry_run)

        with mock.patch.object(batch.asr_whispercpp, "run_asr", side_effect=fail_second):
            with self.assertRaises(batch.ContextualBatchRunFailure) as raised:
                batch.run_batch(manifest_path, dry_run=True)
        failure = raised.exception.result
        validate_schema(failure, "contextual-asr-batch-run.schema.json")
        self.assertEqual(failure["status"], "failed")
        self.assertTrue(failure["dry_run"])
        self.assertEqual(len(failure["results"]), 1)
        self.assertEqual(failure["failed_job"]["ordinal"], 2)
        self.assertEqual(len(calls), 2)
        self.assertIn("deliberate fixture failure", failure["failed_job"]["error"]["message"])

    def test_controller_imports_no_database_or_network_client(self) -> None:
        source = Path(batch.__file__).read_text(encoding="utf-8")
        tree = ast.parse(source)
        imported_roots: set[str] = set()
        for node in ast.walk(tree):
            if isinstance(node, ast.Import):
                imported_roots.update(alias.name.split(".")[0] for alias in node.names)
            elif isinstance(node, ast.ImportFrom) and node.module:
                imported_roots.add(node.module.split(".")[0])
        self.assertTrue(
            imported_roots.isdisjoint(
                {"sqlite3", "socket", "requests", "httpx", "aiohttp"}
            )
        )
        self.assertNotIn("urllib.request", source)
        self.assertEqual(batch.SAFETY["database_access"], "none")
        self.assertEqual(batch.SAFETY["catalog_writes"], False)
        self.assertEqual(batch.SAFETY["publication_authority"], "none")
        self.assertEqual(batch.SAFETY["review_authority"], "none")

        baseline = self.make_raw_result(1, 440)
        with mock.patch(
            "sqlite3.connect", side_effect=AssertionError("database access is forbidden")
        ):
            self.materialize([baseline])


if __name__ == "__main__":
    unittest.main()
