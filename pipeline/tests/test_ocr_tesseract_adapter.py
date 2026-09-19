from __future__ import annotations

import copy
import hashlib
import json
import os
import shutil
import subprocess
import sys
import unittest
from pathlib import Path


REPOSITORY_ROOT = Path(__file__).resolve().parents[2]
PIPELINE_ROOT = REPOSITORY_ROOT / "pipeline"
PROGRAM = PIPELINE_ROOT / "ocr_tesseract_adapter.py"
SPARSE_PROGRAM = PIPELINE_ROOT / "sparse_frame_router.py"
TESTS_ROOT = PIPELINE_ROOT / "tests"
sys.path.insert(0, str(TESTS_ROOT))
from test_sparse_frame_router import (  # noqa: E402
    create_preprocess_fixture,
    remove_test_tree,
    work_order as sparse_work_order,
)


TEST_ROOT = PIPELINE_ROOT / f".test-ocr-adapter-{os.getpid()}"
TSV_HEADER = (
    "level\tpage_num\tblock_num\tpar_num\tline_num\tword_num\t"
    "left\ttop\twidth\theight\tconf\ttext"
)
VALID_TSV = (
    TSV_HEADER
    + "\n1\t1\t0\t0\t0\t0\t0\t0\t320\t180\t-1\t\n"
    + "5\t1\t1\t1\t1\t1\t10\t20\t60\t15\t87.123456\tHIMRverse\n"
    + "5\t1\t1\t1\t1\t2\t75\t20\t45\t15\t96\tDaniel\n"
).encode("utf-8")


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


def validate_schema(instance: object, name: str) -> None:
    try:
        import jsonschema
    except ImportError:
        return
    schema = json.loads((PIPELINE_ROOT / "schemas" / name).read_text(encoding="utf-8"))
    jsonschema.Draft202012Validator.check_schema(schema)
    jsonschema.validate(instance, schema)


def write_fake_tesseract(path: Path, tsv: bytes = VALID_TSV) -> None:
    program = f"""#!/usr/bin/python3
import pathlib
import sys

if sys.argv[1:] == ["--version"]:
    print("tesseract 5.5.3-fixture")
    print("fixture-runtime offline")
    raise SystemExit(0)

arguments = sys.argv[1:]
required = ["stdout", "--tessdata-dir", "-l", "--oem", "--psm", "--dpi"]
if any(value not in arguments for value in required):
    print("missing fixed argument", file=sys.stderr)
    raise SystemExit(7)
if "tessedit_create_tsv=1" not in arguments:
    print("missing explicit TSV variable", file=sys.stderr)
    raise SystemExit(8)
if arguments[-1] == "tsv":
    print("named configs/tsv must not be used", file=sys.stderr)
    raise SystemExit(9)
if not pathlib.Path(arguments[0]).is_file():
    print("missing frame", file=sys.stderr)
    raise SystemExit(10)
sys.stdout.buffer.write({tsv!r})
"""
    path.write_text(program, encoding="utf-8")
    path.chmod(0o500)


@unittest.skipUnless(shutil.which("ffmpeg"), "FFmpeg is required for synthetic sparse frames")
class OCRTesseractAdapterTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        remove_test_tree(TEST_ROOT)
        TEST_ROOT.mkdir(parents=True, mode=0o700)

    @classmethod
    def tearDownClass(cls) -> None:
        remove_test_tree(TEST_ROOT)

    def setUp(self) -> None:
        self.case = TEST_ROOT / self._testMethodName
        self.case.mkdir(parents=True, mode=0o700)
        self.sparse_result = self.create_sparse_result()
        self.tessdata = self.case / "tessdata"
        self.tessdata.mkdir(mode=0o700)
        self.model = self.tessdata / "eng.traineddata"
        self.model.write_bytes(b"fixture-eng-traineddata-v1\n")
        self.model.chmod(0o400)
        self.engine = self.case / "tesseract-fixture"
        write_fake_tesseract(self.engine)
        self.value = self.order()

    def create_sparse_result(self) -> Path:
        preprocess = create_preprocess_fixture(self.case, duration_seconds=1)
        value = sparse_work_order(preprocess, self.case / "sparse-derived")
        value["sampling"] = {
            "include_recording_start": True,
            "scene_changes": {"enabled": False, "max_frames": 0, "offset_ms": 0},
            "periodic": {"enabled": False, "interval_ms": 1000, "max_frames": 0},
            "min_separation_ms": 0,
        }
        value["limits"]["max_frames"] = 1
        work = self.case / "sparse-work-order.json"
        work.write_text(json.dumps(value, indent=2) + "\n", encoding="utf-8")
        completed = run(
            ["python3", str(SPARSE_PROGRAM), "run", "--work-order", str(work)],
            check=False,
        )
        self.assertEqual(completed.returncode, 0, completed.stderr)
        return Path(json.loads(completed.stdout)["result_path"])

    def engine_version(self) -> tuple[str, str]:
        completed = run([str(self.engine), "--version"])
        output = completed.stdout.strip()
        return output.splitlines()[0], hashlib.sha256(output.encode()).hexdigest()

    def order(self, output_name: str = "ocr-derived") -> dict:
        version_label, version_sha = self.engine_version()
        sparse = json.loads(self.sparse_result.read_text(encoding="utf-8"))
        return {
            "schema_version": 1,
            "job_id": f"ocr-fixture-{output_name}",
            "sparse_frame_result": {
                "path": str(self.sparse_result),
                "expected_sha256": digest(self.sparse_result),
            },
            "execution_selection": {
                "mode": "explicit_frame_ids",
                "basis": "reviewer_selected",
                "frame_ids": [sparse["frames"][0]["frame_id"]],
            },
            "tesseract": {
                "executable": str(self.engine),
                "expected_sha256": digest(self.engine),
                "expected_byte_count": self.engine.stat().st_size,
                "expected_version_output_sha256": version_sha,
                "expected_version_label": version_label,
                "tessdata_dir": str(self.tessdata),
                "models": [
                    {
                        "language": "eng",
                        "path": str(self.model),
                        "expected_sha256": digest(self.model),
                        "expected_byte_count": self.model.stat().st_size,
                    }
                ],
            },
            "parameters": {
                "languages": ["eng"],
                "oem": 1,
                "psm": 6,
                "dpi": 300,
                "preserve_interword_spaces": True,
                "thread_limit": 1,
                "tsv_creation": "explicit_tessedit_create_tsv_1",
            },
            "limits": {
                "max_frames": 1,
                "max_tsv_bytes_per_frame": 4096,
                "max_words_per_frame": 10,
                "timeout_seconds_per_frame": 10,
            },
            "output": {"root": str((self.case / output_name).resolve())},
        }

    def execute(self, value: dict, *arguments: str) -> subprocess.CompletedProcess[str]:
        path = self.case / f"ocr-work-{len(list(self.case.glob('ocr-work-*.json')))}.json"
        path.write_text(json.dumps(value, indent=2) + "\n", encoding="utf-8")
        return run(
            [
                "python3",
                str(PROGRAM),
                "run",
                "--work-order",
                str(path),
                *arguments,
            ],
            check=False,
        )

    def test_completed_result_preserves_lineage_geometry_and_uncalibrated_scores(self) -> None:
        completed = self.execute(self.value)
        self.assertEqual(completed.returncode, 0, completed.stderr)
        result = json.loads(completed.stdout)
        validate_schema(result, "ocr-tesseract-result.schema.json")
        self.assertEqual(result["status"], "completed")
        self.assertEqual(result["policy"]["publication_authority"], "none")
        self.assertEqual(result["processing_run"]["environment_json"]["network"], "not_used")
        self.assertIn("tessedit_create_tsv=1", result["commands"][0])
        self.assertNotEqual(result["commands"][0][-1], "tsv")
        frame = result["ocr_frames"][0]
        self.assertEqual(frame["word_count"], 2)
        word = frame["words"][0]
        self.assertEqual(word["raw_text"], "HIMRverse")
        self.assertEqual(word["raw_score_text"], "87.123456")
        self.assertEqual(word["score_scale"]["calibration_state"], "not_calibrated")
        self.assertEqual(word["score_scale"]["probability_interpretation"], "not_a_probability")
        self.assertEqual(word["rectangle"]["coordinate_space"], "source_frame_pixels")
        self.assertEqual(word["frame_locator"], frame["frame_locator"])
        sparse = json.loads(self.sparse_result.read_text(encoding="utf-8"))
        self.assertEqual(frame["frame_locator"]["timestamp"], sparse["frames"][0]["timestamp"])
        self.assertEqual(
            frame["frame_locator"]["ocr_routing_reason_codes"],
            sparse["frames"][0]["ocr_routing"]["reason_codes"],
        )
        tsv = Path(frame["tsv_artifact"]["path"])
        self.assertEqual(tsv.read_bytes(), VALID_TSV)
        self.assertEqual(tsv.stat().st_mode & 0o777, 0o400)
        self.assertEqual(Path(result["result_path"]).stat().st_mode & 0o777, 0o400)
        self.assertEqual(Path(result["result_path"]).parent.stat().st_mode & 0o777, 0o500)

    def test_exact_replay_is_byte_stable_and_does_not_reexecute(self) -> None:
        first = self.execute(self.value)
        self.assertEqual(first.returncode, 0, first.stderr)
        result = json.loads(first.stdout)
        result_path = Path(result["result_path"])
        before = (digest(result_path), result_path.stat().st_mtime_ns)
        second = self.execute(self.value)
        self.assertEqual(second.returncode, 0, second.stderr)
        self.assertEqual(first.stdout, second.stdout)
        self.assertEqual(before, (digest(result_path), result_path.stat().st_mtime_ns))

        tsv = Path(result["ocr_frames"][0]["tsv_artifact"]["path"])
        tsv.parent.chmod(0o700)
        tsv.chmod(0o600)
        body = tsv.read_bytes()
        tsv.write_bytes(body.replace(b"HIMRverse", b"HIMRversf"))
        tsv.chmod(0o400)
        tsv.parent.chmod(0o500)
        blocked = self.execute(self.value)
        self.assertEqual(blocked.returncode, 2)
        self.assertIn("SHA-256 mismatch", blocked.stderr)

    def test_dry_run_rehashes_inputs_but_creates_no_output(self) -> None:
        completed = self.execute(self.value, "--dry-run")
        self.assertEqual(completed.returncode, 0, completed.stderr)
        result = json.loads(completed.stdout)
        validate_schema(result, "ocr-tesseract-result.schema.json")
        self.assertEqual(result["status"], "planned")
        self.assertEqual(result["ocr_frames"], [])
        self.assertFalse(Path(self.value["output"]["root"]).exists())

    def test_execution_is_explicitly_subset_routed_not_blanket_sparse_ocr(self) -> None:
        preprocess = create_preprocess_fixture(
            self.case / "multi-source", duration_seconds=3
        )
        sparse_value = sparse_work_order(
            preprocess, self.case / "multi-sparse-derived"
        )
        sparse_value["sampling"] = {
            "include_recording_start": True,
            "scene_changes": {"enabled": False, "max_frames": 0, "offset_ms": 0},
            "periodic": {"enabled": True, "interval_ms": 1000, "max_frames": 2},
            "min_separation_ms": 0,
        }
        sparse_value["limits"]["max_frames"] = 3
        sparse_work = self.case / "multi-sparse-work-order.json"
        sparse_work.write_text(json.dumps(sparse_value, indent=2) + "\n", encoding="utf-8")
        sparse_completed = run(
            ["python3", str(SPARSE_PROGRAM), "run", "--work-order", str(sparse_work)],
            check=False,
        )
        self.assertEqual(sparse_completed.returncode, 0, sparse_completed.stderr)
        sparse_result = json.loads(sparse_completed.stdout)
        self.assertEqual(len(sparse_result["frames"]), 3)

        value = copy.deepcopy(self.value)
        sparse_path = Path(sparse_result["result_path"])
        value["job_id"] = "ocr-explicit-subset"
        value["sparse_frame_result"] = {
            "path": str(sparse_path),
            "expected_sha256": digest(sparse_path),
        }
        value["execution_selection"] = {
            "mode": "explicit_frame_ids",
            "basis": "external_text_presence_candidate",
            "frame_ids": [sparse_result["frames"][1]["frame_id"]],
        }
        value["output"]["root"] = str((self.case / "subset-output").resolve())
        completed = self.execute(value)
        self.assertEqual(completed.returncode, 0, completed.stderr)
        result = json.loads(completed.stdout)
        self.assertEqual(len(result["commands"]), 1)
        self.assertEqual(len(result["ocr_frames"]), 1)
        self.assertEqual(
            result["ocr_frames"][0]["frame_locator"]["frame_id"],
            sparse_result["frames"][1]["frame_id"],
        )

        unknown = copy.deepcopy(value)
        unknown["job_id"] = "ocr-unknown-selection"
        unknown["execution_selection"]["frame_ids"] = ["frame_" + "0" * 32]
        unknown["output"]["root"] = str((self.case / "unknown-output").resolve())
        blocked = self.execute(unknown, "--dry-run")
        self.assertEqual(blocked.returncode, 2)
        self.assertIn("must exist", blocked.stderr)
        self.assertFalse(Path(unknown["output"]["root"]).exists())

    def test_plain_text_wrong_header_zero_area_and_bad_score_fail_closed(self) -> None:
        cases = {
            "plain": b"HIMRverse Daniel\n",
            "zero": (
                TSV_HEADER
                + "\n5\t1\t1\t1\t1\t1\t10\t20\t0\t15\t90\tbad\n"
            ).encode(),
            "score": (
                TSV_HEADER
                + "\n5\t1\t1\t1\t1\t1\t10\t20\t20\t15\t-1\tbad\n"
            ).encode(),
        }
        for label, tsv in cases.items():
            with self.subTest(label=label):
                engine = self.case / f"tesseract-{label}"
                write_fake_tesseract(engine, tsv)
                value = copy.deepcopy(self.value)
                value["job_id"] = f"ocr-invalid-{label}"
                value["output"]["root"] = str((self.case / f"output-{label}").resolve())
                value["tesseract"]["executable"] = str(engine)
                value["tesseract"]["expected_sha256"] = digest(engine)
                value["tesseract"]["expected_byte_count"] = engine.stat().st_size
                completed = run([str(engine), "--version"])
                version = completed.stdout.strip()
                value["tesseract"]["expected_version_label"] = version.splitlines()[0]
                value["tesseract"]["expected_version_output_sha256"] = hashlib.sha256(version.encode()).hexdigest()
                blocked = self.execute(value)
                self.assertEqual(blocked.returncode, 2)
                self.assertRegex(blocked.stderr, "TSV header|zero-area|raw 0\\.\\.100")
                self.assertEqual(list(Path(value["output"]["root"]).rglob("result.json")), [])

    def test_source_engine_model_and_symlink_tampering_fail_before_result(self) -> None:
        bad_source = copy.deepcopy(self.value)
        bad_source["sparse_frame_result"]["expected_sha256"] = "0" * 64
        self.assertIn("SHA-256 mismatch", self.execute(bad_source).stderr)

        bad_engine = copy.deepcopy(self.value)
        bad_engine["tesseract"]["expected_sha256"] = "0" * 64
        self.assertIn("Tesseract executable SHA-256 mismatch", self.execute(bad_engine).stderr)

        bad_version = copy.deepcopy(self.value)
        bad_version["tesseract"]["expected_version_output_sha256"] = "0" * 64
        self.assertIn("version output differs", self.execute(bad_version).stderr)

        bad_model = copy.deepcopy(self.value)
        bad_model["tesseract"]["models"][0]["expected_sha256"] = "0" * 64
        self.assertIn("model SHA-256 mismatch", self.execute(bad_model).stderr)

        alias = self.case / "eng-alias.traineddata"
        alias.symlink_to(self.model)
        symlinked = copy.deepcopy(self.value)
        symlinked["tesseract"]["models"][0]["path"] = str(alias)
        self.assertIn("without symlinks", self.execute(symlinked).stderr)

        sparse = json.loads(self.sparse_result.read_text(encoding="utf-8"))
        png = Path(sparse["artifacts"][0]["path"])
        png.parent.chmod(0o755)
        png.chmod(0o600)
        body = png.read_bytes()
        png.write_bytes(body[:-1] + bytes([body[-1] ^ 1]))
        png.chmod(0o400)
        png.parent.chmod(0o555)
        self.assertIn("sparse frame PNG SHA-256 mismatch", self.execute(self.value).stderr)
        self.assertFalse(Path(self.value["output"]["root"]).exists())

    def test_work_order_generator_pins_all_assets_without_running_frames(self) -> None:
        output = self.case / "generated-output"
        generated = run(
            [
                "python3",
                str(PROGRAM),
                "create-work-order",
                "--job-id",
                "generated-ocr-fixture",
                "--sparse-frame-result",
                str(self.sparse_result),
                "--selection-basis",
                "reviewer_selected",
                "--frame-id",
                json.loads(self.sparse_result.read_text(encoding="utf-8"))["frames"][0]["frame_id"],
                "--tesseract",
                str(self.engine),
                "--tessdata-dir",
                str(self.tessdata),
                "--model",
                f"eng={self.model}",
                "--language",
                "eng",
                "--output-root",
                str(output),
            ],
            check=False,
        )
        self.assertEqual(generated.returncode, 0, generated.stderr)
        value = json.loads(generated.stdout)
        validate_schema(value, "ocr-tesseract-work-order.schema.json")
        self.assertEqual(value["sparse_frame_result"]["expected_sha256"], digest(self.sparse_result))
        self.assertEqual(value["tesseract"]["expected_sha256"], digest(self.engine))
        self.assertEqual(value["tesseract"]["models"][0]["expected_sha256"], digest(self.model))
        self.assertFalse(output.exists())


if __name__ == "__main__":
    unittest.main()
