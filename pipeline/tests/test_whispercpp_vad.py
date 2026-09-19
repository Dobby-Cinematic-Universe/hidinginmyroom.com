from __future__ import annotations

import copy
import fcntl
import hashlib
import json
import os
import shutil
import stat
import struct
import subprocess
import unittest
from pathlib import Path
from unittest import mock

import jsonschema

from pipeline import whispercpp_vad as vad


REPOSITORY_ROOT = Path(__file__).resolve().parents[2]
PIPELINE_ROOT = REPOSITORY_ROOT / "pipeline"
TEST_ROOT = PIPELINE_ROOT / ".test-whispercpp-vad"
WORK_SCHEMA = PIPELINE_ROOT / "schemas" / "whispercpp-vad-work-order.schema.json"
RESULT_SCHEMA = PIPELINE_ROOT / "schemas" / "whispercpp-vad-result.schema.json"
PROGRAM = PIPELINE_ROOT / "whispercpp_vad.py"


def digest(path: Path) -> str:
    value = hashlib.sha256()
    with path.open("rb") as handle:
        while chunk := handle.read(1024 * 1024):
            value.update(chunk)
    return value.hexdigest()


def make_writable(path: Path) -> None:
    if not path.exists():
        return
    for child in path.rglob("*"):
        try:
            child.chmod(0o700 if child.is_dir() else 0o600)
        except FileNotFoundError:
            pass
    path.chmod(0o700)


def flac_streaminfo(
    *, total_samples: int = 160_000, sample_rate: int = 16_000,
    channels: int = 1, bits_per_sample: int = 16,
) -> bytes:
    streaminfo = bytearray(34)
    streaminfo[0:2] = (4096).to_bytes(2, "big")
    streaminfo[2:4] = (4096).to_bytes(2, "big")
    packed = (
        (sample_rate << 44)
        | ((channels - 1) << 41)
        | ((bits_per_sample - 1) << 36)
        | total_samples
    )
    streaminfo[10:18] = packed.to_bytes(8, "big")
    return b"fLaC" + bytes([0x80]) + (34).to_bytes(3, "big") + bytes(streaminfo)


class WhisperCppVADTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        TEST_ROOT.mkdir(mode=0o700, parents=True, exist_ok=True)
        cls.work_schema = json.loads(WORK_SCHEMA.read_text(encoding="utf-8"))
        cls.result_schema = json.loads(RESULT_SCHEMA.read_text(encoding="utf-8"))

    @classmethod
    def tearDownClass(cls) -> None:
        make_writable(TEST_ROOT)
        shutil.rmtree(TEST_ROOT, ignore_errors=True)

    def setUp(self) -> None:
        self.case = TEST_ROOT / self._testMethodName
        self.case.mkdir(mode=0o700)

    def tearDown(self) -> None:
        make_writable(self.case)

    def fixture(
        self,
        stdout: str = (
            "\nDetected 2 speech segments:\n"
            "Speech segment 0: start = 1.00, end = 20.00\n"
            "Speech segment 1: start = 900.00, end = 1002.00\n\n"
        ),
        *,
        total_samples: int = 160_000,
    ) -> tuple[dict, Path, Path, Path]:
        assets = self.case / "assets"
        assets.mkdir(mode=0o700)
        input_path = assets / "audio.flac"
        input_path.write_bytes(flac_streaminfo(total_samples=total_samples))
        input_path.chmod(0o400)
        model_path = assets / "model.ggml"
        model_path.write_text(stdout, encoding="utf-8")
        model_path.chmod(0o400)
        engine_path = assets / "fake-vad-engine"
        engine_path.write_text(
            "#!/usr/bin/python3\n"
            "import pathlib,sys,time\n"
            "args=sys.argv[1:]\n"
            "model=args[args.index('--vad-model')+1]\n"
            "body=pathlib.Path(model).read_text(encoding='utf-8')\n"
            "if body.startswith('EXIT:'):\n"
            "    sys.exit(int(body.split(':',1)[1]))\n"
            "if body.startswith('SLEEP:'):\n"
            "    time.sleep(float(body.split(':',1)[1]))\n"
            "if body.startswith('STDOUT_BURST:'):\n"
            "    sys.stdout.write('x'*int(body.split(':',1)[1]))\n"
            "    sys.exit(0)\n"
            "if body.startswith('STDERR_BURST:'):\n"
            "    sys.stderr.write('x'*int(body.split(':',1)[1]))\n"
            "    sys.exit(0)\n"
            "sys.stdout.write(body)\n"
            "sys.stderr.write('fake engine diagnostic\\n')\n",
            encoding="utf-8",
        )
        engine_path.chmod(0o500)
        output = self.case / "private-output"
        output.mkdir(mode=0o700)
        profile_engine = copy.deepcopy(vad.profiles.ENGINE_PROFILE)
        profile_model = copy.deepcopy(vad.profiles.MODEL_PROFILE)
        work = {
            "schema_version": 1,
            "job_id": "vad_synthetic_test",
            "input": {
                "path": str(input_path.resolve()),
                "expected_sha256": digest(input_path),
                "expected_byte_count": input_path.stat().st_size,
                "media_id": f"media_sha256_{digest(input_path)}",
                "artifact_id": "artifact_normalized_audio_test",
                "parent_processing_run_id": "run_local_window_test",
            },
            "engine": {
                "executable": str(engine_path.resolve()),
                "expected_sha256": digest(engine_path),
                "expected_byte_count": engine_path.stat().st_size,
                "profile_id": profile_engine["profile_id"],
                "version_label": profile_engine["version_label"],
                "version_evidence": profile_engine["version_evidence"],
                "build": profile_engine["build"],
            },
            "model": {
                "path": str(model_path.resolve()),
                "expected_sha256": digest(model_path),
                "expected_byte_count": model_path.stat().st_size,
                **{
                    key: profile_model[key]
                    for key in (
                        "profile_id", "model_id", "name", "revision",
                        "source", "license_label",
                    )
                },
            },
            "parameters": {
                "segmentation_profile": vad.SEGMENTATION_PROFILE,
                "parameter_binding": vad.PARAMETER_BINDING,
                "threads": 1,
                "threshold": 0.5,
                "min_speech_duration_ms": 250,
                "min_silence_duration_ms": 100,
                "max_speech_duration_state": "float_max_default",
                "speech_pad_ms": 30,
                "samples_overlap_seconds": 0.1,
                "use_gpu": False,
                "timeout_seconds": 10,
            },
            "catalog_context": {
                "recording_id": "recording_test",
                "rendition_id": "rendition_test",
                "coordinate_system": "rendition_media_ms",
                "source_start_ms": 1_800_000,
            },
            "output": {"root": str(output.resolve())},
        }
        return work, input_path, engine_path, model_path

    def profile_patches(self):
        return (
            mock.patch.object(
                vad.profiles,
                "match_engine_profile",
                side_effect=lambda _sha, _size: copy.deepcopy(vad.profiles.ENGINE_PROFILE),
            ),
            mock.patch.object(
                vad.profiles,
                "match_model_profile",
                side_effect=lambda _sha, _size: copy.deepcopy(vad.profiles.MODEL_PROFILE),
            ),
        )

    def run_fixture(self, work: dict, *, dry_run: bool = False) -> dict:
        validated = vad.validate_work_order(work)
        engine_patch, model_patch = self.profile_patches()
        with engine_patch, model_patch:
            return vad.run_vad(validated, dry_run=dry_run)

    def test_work_order_and_completed_result_validate(self) -> None:
        work, *_ = self.fixture()
        jsonschema.Draft202012Validator(self.work_schema).validate(work)
        result = self.run_fixture(work)
        self.assertEqual(result["status"], "completed")
        self.assertEqual(result["counts"]["speech_segment_count"], 2)
        self.assertEqual(result["counts"]["speech_duration_ms"], 1190)
        self.assertEqual(result["counts"]["tail_clipped_segment_count"], 1)
        self.assertEqual(result["segments"][1]["artifact_end_ms"], 10_000)
        self.assertEqual(result["segments"][1]["tail_overrun_ms"], 20)
        self.assertEqual(result["segments"][0]["source_start_ms"], 1_800_010)
        self.assertIsNone(result["segments"][0]["calibrated_probability"])
        run_dir = Path(result["result_path"]).parent
        self.assertEqual(stat.S_IMODE(run_dir.stat().st_mode), 0o500)
        for path in run_dir.iterdir():
            self.assertEqual(stat.S_IMODE(path.stat().st_mode), 0o400)

    def test_exact_replay_returns_original_envelope(self) -> None:
        work, *_ = self.fixture()
        first = self.run_fixture(work)
        mtimes = {path.name: path.stat().st_mtime_ns for path in Path(first["result_path"]).parent.iterdir()}
        second = self.run_fixture(work)
        self.assertEqual(first, second)
        self.assertEqual(
            mtimes,
            {path.name: path.stat().st_mtime_ns for path in Path(first["result_path"]).parent.iterdir()},
        )

    def test_dry_run_writes_nothing(self) -> None:
        work, *_ = self.fixture()
        output = Path(work["output"]["root"])
        result = self.run_fixture(work, dry_run=True)
        self.assertEqual(result["status"], "planned")
        self.assertEqual(list(output.iterdir()), [])

    def test_zero_segments_is_valid(self) -> None:
        work, *_ = self.fixture("\nDetected 0 speech segments:\n\n")
        result = self.run_fixture(work)
        self.assertEqual(result["segments"], [])
        self.assertEqual(result["counts"]["speech_coverage_ratio"], 0.0)

    def test_malformed_or_mismatched_stdout_fails(self) -> None:
        for index, stdout in enumerate((
            "Detected 1 speech segments:\n",
            "Detected 1 speech segments:\nSpeech segment 1: start = 1.00, end = 2.00\n",
            "Detected 1 speech segments:\nSpeech segment 0: start = 2.00, end = 1.00\n",
            "Detected 2 speech segments:\nSpeech segment 0: start = 1.00, end = 5.00\nSpeech segment 1: start = 4.00, end = 8.00\n",
            "unexpected\n",
        )):
            with self.subTest(index=index):
                subcase = self.case / f"bad-{index}"
                subcase.mkdir(mode=0o700)
                original = self.case
                self.case = subcase
                try:
                    work, *_ = self.fixture(stdout)
                    with self.assertRaises(vad.VADError):
                        self.run_fixture(work)
                finally:
                    self.case = original

    def test_tail_overrun_above_bound_fails(self) -> None:
        work, *_ = self.fixture(
            "Detected 1 speech segments:\nSpeech segment 0: start = 900.00, end = 1007.00\n"
        )
        with self.assertRaisesRegex(vad.VADError, "exceeds the input"):
            self.run_fixture(work)

    def test_nonzero_exit_and_timeout_fail_without_result(self) -> None:
        for index, body in enumerate(("EXIT:7", "SLEEP:2")):
            with self.subTest(body=body):
                subcase = self.case / f"child-{index}"
                subcase.mkdir(mode=0o700)
                original = self.case
                self.case = subcase
                try:
                    work, *_ = self.fixture(body)
                    if body.startswith("SLEEP"):
                        work["parameters"]["timeout_seconds"] = 1
                    with self.assertRaises(vad.VADError):
                        self.run_fixture(work)
                    output = Path(work["output"]["root"])
                    self.assertEqual(list(output.rglob("result.json")), [])
                    self.assertEqual(
                        [path for path in output.rglob("*") if ".tmp-" in path.name],
                        [],
                    )
                finally:
                    self.case = original

    def test_child_pipe_caps_fail_closed_without_partial_result(self) -> None:
        for index, body in enumerate(
            (
                f"STDOUT_BURST:{vad.MAX_STDOUT_BYTES + 1}",
                f"STDERR_BURST:{vad.MAX_STDERR_BYTES + 1}",
            )
        ):
            with self.subTest(body=body.split(":", 1)[0]):
                subcase = self.case / f"pipe-cap-{index}"
                subcase.mkdir(mode=0o700)
                original = self.case
                self.case = subcase
                try:
                    work, *_ = self.fixture(body)
                    with self.assertRaisesRegex(vad.VADError, "bounded output limit"):
                        self.run_fixture(work)
                    self.assertEqual(
                        list(Path(work["output"]["root"]).rglob("result.json")), []
                    )
                finally:
                    self.case = original

    def test_hash_size_and_profile_mismatch_fail(self) -> None:
        work, *_ = self.fixture()
        wrong_hash = copy.deepcopy(work)
        wrong_hash["input"]["expected_sha256"] = "f" * 64
        wrong_hash["input"]["media_id"] = "media_sha256_" + "f" * 64
        with self.assertRaises(vad.ASRError):
            self.run_fixture(wrong_hash)
        wrong_size = copy.deepcopy(work)
        wrong_size["model"]["expected_byte_count"] += 1
        with self.assertRaisesRegex(vad.VADError, "byte count mismatch"):
            self.run_fixture(wrong_size)
        validated = vad.validate_work_order(work)
        with mock.patch.object(
            vad.profiles,
            "match_engine_profile",
            side_effect=vad.profiles.VADProfileError("not admitted"),
        ):
            with self.assertRaisesRegex(vad.VADError, "not admitted"):
                vad.run_vad(validated, dry_run=True)

    def test_input_must_be_sealed_single_link(self) -> None:
        work, input_path, *_ = self.fixture()
        input_path.chmod(0o600)
        with self.assertRaisesRegex(vad.VADError, "sealed mode-0400"):
            self.run_fixture(work)
        input_path.chmod(0o400)
        linked = input_path.with_name("linked.flac")
        os.link(input_path, linked)
        with self.assertRaisesRegex(vad.VADError, "exactly one hard link"):
            self.run_fixture(work)

    def test_engine_and_model_must_be_sealed_owned_single_link_assets(self) -> None:
        work, _input_path, engine_path, model_path = self.fixture()
        engine_path.chmod(0o700)
        with self.assertRaisesRegex(vad.VADError, "mode-0500"):
            self.run_fixture(work)
        engine_path.chmod(0o500)
        model_path.chmod(0o600)
        with self.assertRaisesRegex(vad.VADError, "mode-0400"):
            self.run_fixture(work)
        model_path.chmod(0o400)
        linked_model = model_path.with_name("linked-model.ggml")
        os.link(model_path, linked_model)
        with self.assertRaisesRegex(vad.VADError, "exactly one hard link"):
            self.run_fixture(work)

    def test_execution_copy_has_all_required_seals_and_rejects_mutation(self) -> None:
        _work, _input_path, engine_path, _model_path = self.fixture()
        with vad.retained_verified_file(
            engine_path,
            digest(engine_path),
            "VAD executable",
            executable=True,
        ) as retained:
            vad.require_sealed_asset(retained, "VAD executable", executable=True)
            with vad.sealed_execution_copy(
                retained, "engine-test", executable=True
            ) as (descriptor, _proc_path):
                required = (
                    fcntl.F_SEAL_SEAL
                    | fcntl.F_SEAL_WRITE
                    | fcntl.F_SEAL_GROW
                    | fcntl.F_SEAL_SHRINK
                )
                self.assertEqual(
                    fcntl.fcntl(descriptor, fcntl.F_GET_SEALS), required
                )
                with self.assertRaises(OSError):
                    os.write(descriptor, b"mutation")
                with self.assertRaises(OSError):
                    os.ftruncate(descriptor, 0)

    def test_flac_contract_is_enforced(self) -> None:
        work, input_path, *_ = self.fixture()
        input_path.chmod(0o600)
        input_path.write_bytes(flac_streaminfo(sample_rate=48_000))
        input_path.chmod(0o400)
        work["input"]["expected_sha256"] = digest(input_path)
        work["input"]["expected_byte_count"] = input_path.stat().st_size
        work["input"]["media_id"] = "media_sha256_" + digest(input_path)
        with self.assertRaisesRegex(vad.VADError, "normalized 16 kHz"):
            self.run_fixture(work)

    def test_submillisecond_flac_and_unbounded_numeric_text_fail_closed(self) -> None:
        work, *_ = self.fixture(total_samples=1)
        with self.assertRaisesRegex(vad.VADError, "at least one millisecond"):
            self.run_fixture(work)
        subcase = self.case / "numeric-bound"
        subcase.mkdir(mode=0o700)
        original = self.case
        self.case = subcase
        try:
            work, *_ = self.fixture(
                "Detected 1 speech segments:\n"
                f"Speech segment 0: start = {'9' * 5000}.00, end = {'9' * 5000}.00\n"
            )
            with self.assertRaisesRegex(vad.VADError, "unexpected syntax"):
                self.run_fixture(work)
        finally:
            self.case = original

    def test_symlinks_public_outputs_and_nested_inputs_are_rejected(self) -> None:
        work, input_path, *_ = self.fixture()
        link = input_path.with_name("input-link.flac")
        link.symlink_to(input_path)
        work["input"]["path"] = str(link)
        with self.assertRaisesRegex(vad.VADError, "without symlinks"):
            vad.validate_work_order(work)
        work["input"]["path"] = str(input_path)
        work["output"]["root"] = str(REPOSITORY_ROOT / "public" / "vad")
        with self.assertRaisesRegex(vad.VADError, "may not be under"):
            vad.validate_work_order(work)

    def test_output_root_direct_parent_must_exist_in_dry_run_and_apply(self) -> None:
        work, *_ = self.fixture()
        work["output"]["root"] = str(
            self.case / "missing-parent" / "private-output"
        )
        with self.assertRaisesRegex(vad.VADError, "direct parent must already exist"):
            vad.validate_work_order(work)

    def test_runtime_rejects_unknown_fields_and_nondefault_parameters(self) -> None:
        work, *_ = self.fixture()
        work["unexpected"] = True
        with self.assertRaisesRegex(vad.VADError, "unknown"):
            vad.validate_work_order(work)
        work.pop("unexpected")
        work["parameters"]["min_silence_duration_ms"] = 101
        with self.assertRaisesRegex(vad.VADError, "must equal 100"):
            vad.validate_work_order(work)
        work["parameters"]["min_silence_duration_ms"] = 100
        work["parameters"]["use_gpu"] = 0
        with self.assertRaisesRegex(vad.VADError, "must be a boolean"):
            vad.validate_work_order(work)
        work["parameters"]["use_gpu"] = False
        work["schema_version"] = True
        with self.assertRaisesRegex(vad.VADError, "schema_version"):
            vad.validate_work_order(work)

    def test_strict_json_rejects_duplicate_keys(self) -> None:
        with self.assertRaisesRegex(vad.VADError, "duplicate object key"):
            vad.strict_json_bytes(b'{"schema_version":1,"schema_version":1}', "test")

    def test_reuse_rejects_artifact_tampering(self) -> None:
        work, *_ = self.fixture()
        result = self.run_fixture(work)
        stdout = Path(result["result_path"]).with_name("engine.stdout.txt")
        stdout.chmod(0o600)
        stdout.write_text("tampered\n", encoding="utf-8")
        stdout.chmod(0o400)
        with self.assertRaises(vad.VADError):
            self.run_fixture(work)

    def test_reuse_rejects_result_semantic_tampering(self) -> None:
        work, *_ = self.fixture()
        result = self.run_fixture(work)
        result_path = Path(result["result_path"])
        result_path.chmod(0o600)
        altered = json.loads(result_path.read_text(encoding="utf-8"))
        altered["unexpected"] = True
        result_path.write_text(
            json.dumps(altered, sort_keys=True, indent=2) + "\n", encoding="utf-8"
        )
        result_path.chmod(0o400)
        with self.assertRaisesRegex(vad.VADError, "missing or extra"):
            self.run_fixture(work)

    def test_reuse_requires_each_artifact_kind_exactly_once(self) -> None:
        work, *_ = self.fixture()
        result = self.run_fixture(work)
        result_path = Path(result["result_path"])
        stderr_path = result_path.with_name("engine.stderr.txt")
        stderr_path.chmod(0o600)
        stderr_path.write_text("changed but still sealed\n", encoding="utf-8")
        stderr_path.chmod(0o400)
        result_path.chmod(0o600)
        altered = json.loads(result_path.read_text(encoding="utf-8"))
        altered["artifacts"][1] = copy.deepcopy(altered["artifacts"][0])
        result_path.write_text(
            json.dumps(altered, sort_keys=True, indent=2) + "\n", encoding="utf-8"
        )
        result_path.chmod(0o400)
        with self.assertRaisesRegex(
            vad.VADError, "invalid artifact order|duplicates an artifact kind"
        ):
            self.run_fixture(work)

    def test_dry_run_rejects_corrupt_replay_and_unsafe_existing_root(self) -> None:
        work, *_ = self.fixture()
        result = self.run_fixture(work)
        result_path = Path(result["result_path"])
        result_path.chmod(0o600)
        altered = json.loads(result_path.read_text(encoding="utf-8"))
        altered["safety"]["publication_authority"] = "granted"
        result_path.write_text(
            json.dumps(altered, sort_keys=True, indent=2) + "\n", encoding="utf-8"
        )
        result_path.chmod(0o400)
        with self.assertRaisesRegex(vad.VADError, "safety boundary"):
            self.run_fixture(work, dry_run=True)

        subcase = self.case / "unsafe-root"
        subcase.mkdir(mode=0o700)
        original = self.case
        self.case = subcase
        try:
            work, *_ = self.fixture()
            Path(work["output"]["root"]).chmod(0o755)
            with self.assertRaisesRegex(vad.VADError, "owner-private"):
                self.run_fixture(work, dry_run=True)
        finally:
            self.case = original

    def test_retained_creation_cannot_be_redirected_by_ancestor_replacement(self) -> None:
        output_root = self.case / "retained-root"
        moved_root = self.case / "moved-root"
        redirect_target = self.case / "redirect-target"
        redirect_target.mkdir(mode=0o700)
        observations = vad.preflight_output_tree(
            output_root, output_root / "nested" / "result"
        )
        try:
            with self.assertRaisesRegex(vad.VADError, "path changed|was replaced"):
                with vad.retained_private_path(
                    output_root, ("nested",), observations
                ) as (_path, directory_fd):
                    output_root.rename(moved_root)
                    output_root.symlink_to(
                        redirect_target, target_is_directory=True
                    )
                    vad.immutable_write_at(directory_fd, "probe", b"retained\n")
        finally:
            if output_root.is_symlink():
                output_root.unlink()
            if moved_root.exists():
                moved_root.rename(output_root)
        self.assertTrue((output_root / "nested" / "probe").is_file())
        self.assertFalse((redirect_target / "nested" / "probe").exists())

    def test_retained_component_check_rejects_symlink_back_to_moved_tree(self) -> None:
        output_root = self.case / "same-tree-root"
        moved_root = self.case / "same-tree-moved"
        observations = vad.preflight_output_tree(
            output_root, output_root / "nested" / "result"
        )
        try:
            with self.assertRaisesRegex(vad.VADError, "component was replaced"):
                with vad.retained_private_path(
                    output_root, ("nested",), observations
                ) as (_path, directory_fd):
                    output_root.rename(moved_root)
                    output_root.symlink_to(moved_root, target_is_directory=True)
                    vad.immutable_write_at(directory_fd, "probe", b"retained\n")
        finally:
            if output_root.is_symlink():
                output_root.unlink()
            if moved_root.exists():
                moved_root.rename(output_root)
        self.assertTrue((output_root / "nested" / "probe").is_file())

    def test_no_replace_publication_preserves_the_first_directory(self) -> None:
        parent = self.case / "publish-parent"
        parent.mkdir(mode=0o700)
        flags = os.O_RDONLY | getattr(os, "O_DIRECTORY", 0)
        parent_fd = os.open(parent, flags)
        try:
            os.mkdir("first", mode=0o700, dir_fd=parent_fd)
            os.mkdir("second", mode=0o700, dir_fd=parent_fd)
            vad.atomic_publish_directory_at(parent_fd, "first", "result")
            first_identity = (parent / "result").stat().st_ino
            with self.assertRaises(FileExistsError):
                vad.atomic_publish_directory_at(parent_fd, "second", "result")
            self.assertEqual((parent / "result").stat().st_ino, first_identity)
            self.assertTrue((parent / "second").is_dir())
        finally:
            os.close(parent_fd)

    def test_reuse_requires_schema_ordered_artifacts(self) -> None:
        work, *_ = self.fixture()
        result = self.run_fixture(work)
        result_path = Path(result["result_path"])
        result_path.chmod(0o600)
        altered = json.loads(result_path.read_text(encoding="utf-8"))
        altered["artifacts"].reverse()
        result_path.write_text(
            json.dumps(altered, sort_keys=True, indent=2) + "\n",
            encoding="utf-8",
        )
        result_path.chmod(0o400)
        with self.assertRaisesRegex(vad.VADError, "invalid artifact order"):
            self.run_fixture(work)

    def test_reuse_rejects_noncanonical_or_inverted_processing_time(self) -> None:
        for index, timestamps in enumerate(
            (
                ("2026-01-01Z", "2026-01-01T00:00:01Z"),
                ("2026-01-01T00:00:02Z", "2026-01-01T00:00:01Z"),
            )
        ):
            with self.subTest(index=index):
                subcase = self.case / f"time-{index}"
                subcase.mkdir(mode=0o700)
                original = self.case
                self.case = subcase
                try:
                    work, *_ = self.fixture()
                    result = self.run_fixture(work)
                    result_path = Path(result["result_path"])
                    result_path.chmod(0o600)
                    altered = json.loads(result_path.read_text(encoding="utf-8"))
                    altered["processing_run"]["started_at"] = timestamps[0]
                    altered["processing_run"]["completed_at"] = timestamps[1]
                    result_path.write_text(
                        json.dumps(altered, sort_keys=True, indent=2) + "\n",
                        encoding="utf-8",
                    )
                    result_path.chmod(0o400)
                    with self.assertRaises(vad.VADError):
                        self.run_fixture(work)
                finally:
                    self.case = original


if __name__ == "__main__":
    unittest.main()
