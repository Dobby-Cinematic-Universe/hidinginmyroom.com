from __future__ import annotations

import hashlib
import importlib.util
import json
import os
import subprocess
import sys
import unittest
from pathlib import Path


REPOSITORY_ROOT = Path(__file__).resolve().parents[2]
PIPELINE_ROOT = REPOSITORY_ROOT / "pipeline"
PROGRAM = PIPELINE_ROOT / "ocr_preflight.py"
DEFAULT_REQUIREMENTS = PIPELINE_ROOT / "ocr-preflight-requirements-v1.json"
TEST_ROOT = PIPELINE_ROOT / ".test-work" / "ocr-preflight"

SPEC = importlib.util.spec_from_file_location("ocr_preflight", PROGRAM)
assert SPEC is not None and SPEC.loader is not None
ocr_preflight = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(ocr_preflight)


def digest_bytes(body: bytes) -> str:
    return hashlib.sha256(body).hexdigest()


def digest(path: Path) -> str:
    return digest_bytes(path.read_bytes())


def write(path: Path, body: str, *, executable: bool = False) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(body, encoding="utf-8")
    path.chmod(0o755 if executable else 0o600)


def write_json(path: Path, value: object) -> None:
    write(path, json.dumps(value, ensure_ascii=False, sort_keys=True, indent=2) + "\n")


class OCRPreflightTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        TEST_ROOT.mkdir(parents=True, exist_ok=True)

    @classmethod
    def tearDownClass(cls) -> None:
        import shutil

        shutil.rmtree(TEST_ROOT, ignore_errors=True)

    def setUp(self) -> None:
        self.case = TEST_ROOT / self._testMethodName
        self.case.mkdir(parents=True, exist_ok=True)

    def fake_manifest(self) -> Path:
        python = sys.executable
        ffmpeg = self.case / "fake-ffmpeg"
        tesseract = self.case / "fake-tesseract"
        ffmpeg_version = b"ffmpeg fake 1.0\n"
        ffmpeg_help = b"Optical Character Recognition\ndatapath\nlanguage\n"
        tesseract_version = b"tesseract fake 1.0\n"
        write(
            ffmpeg,
            f"#!{python}\n"
            "import pathlib, sys\n"
            "if '-version' in sys.argv:\n"
            f"    sys.stdout.buffer.write({ffmpeg_version!r}); raise SystemExit(0)\n"
            "if 'filter=ocr' in sys.argv:\n"
            f"    sys.stdout.buffer.write({ffmpeg_help!r}); raise SystemExit(0)\n"
            "if 'lavfi' in sys.argv:\n"
            "    pathlib.Path(sys.argv[-1]).write_bytes(b'fixed synthetic frame')\n"
            "    raise SystemExit(0)\n"
            "sys.stdout.write('frame:0 pts:0 pts_time:0\\n'\n"
            "                 'lavfi.ocr.text=HIMRverse Daniel 2026\\n'\n"
            "                 'lavfi.ocr.confidence=90 91 92 \\n')\n",
            executable=True,
        )
        tsv = (
            "level\tpage_num\tblock_num\tpar_num\tline_num\tword_num\tleft\ttop\t"
            "width\theight\tconf\ttext\n"
            "5\t1\t1\t1\t1\t1\t10\t20\t100\t30\t91.5\tHIMRverse\n"
        ).encode()
        write(
            tesseract,
            f"#!{python}\n"
            "import sys\n"
            "if '--version' in sys.argv:\n"
            f"    sys.stdout.buffer.write({tesseract_version!r})\n"
            "else:\n"
            f"    sys.stdout.buffer.write({tsv!r})\n",
            executable=True,
        )
        library_avfilter = self.case / "libavfilter.so"
        library_tesseract = self.case / "libtesseract.so"
        library_leptonica = self.case / "libleptonica.so"
        license_path = self.case / "LICENSE"
        tessdata = self.case / "tessdata"
        font = self.case / "font.ttf"
        write(library_avfilter, "fake avfilter library\n")
        write(library_tesseract, "fake tesseract library\n")
        write(library_leptonica, "fake leptonica library\n")
        write(license_path, "Apache-2.0 fixture\n")
        write(font, "fake font\n")
        tessdata.mkdir()
        language = tessdata / "eng.traineddata"
        write(language, "fake traineddata\n")
        manifest = {
            "schema_version": 1,
            "purpose": "offline_ocr_adapter_feasibility_gate",
            "policy": {
                "automatic_publication": False,
                "calibration_state": "not_calibrated",
                "identity_inference": False,
                "network_allowed": False,
                "require_deterministic_tsv": True,
                "require_raw_word_confidence": True,
                "require_word_boxes": True,
            },
            "ffmpeg": {
                "path": str(ffmpeg),
                "expected_sha256": digest(ffmpeg),
                "expected_version_output_sha256": digest_bytes(ffmpeg_version),
                "expected_ocr_help_output_sha256": digest_bytes(ffmpeg_help),
            },
            "tesseract_cli": {
                "path": str(tesseract),
                "expected_sha256": digest(tesseract),
                "expected_version_output_sha256": digest_bytes(tesseract_version),
            },
            "linked_libraries": [
                {
                    "name": "libavfilter",
                    "path": str(library_avfilter),
                    "expected_sha256": digest(library_avfilter),
                },
                {
                    "name": "libtesseract",
                    "path": str(library_tesseract),
                    "expected_sha256": digest(library_tesseract),
                },
                {
                    "name": "libleptonica",
                    "path": str(library_leptonica),
                    "expected_sha256": digest(library_leptonica),
                },
            ],
            "tessdata": {
                "path": str(tessdata),
                "license": {"path": str(license_path), "expected_sha256": digest(license_path)},
                "languages": [
                    {
                        "code": "eng",
                        "filename": "eng.traineddata",
                        "expected_sha256": digest(language),
                    }
                ],
            },
            "fixtures": [
                {
                    "fixture_id": "english_fixture",
                    "language": "eng",
                    "text": "HIMRverse Daniel 2026",
                    "font": {"path": str(font), "expected_sha256": digest(font)},
                }
            ],
        }
        path = self.case / "requirements.json"
        write_json(path, manifest)
        return path

    def run_program(self, manifest: Path) -> subprocess.CompletedProcess[str]:
        return subprocess.run(
            [sys.executable, str(PROGRAM), "--requirements", str(manifest)],
            stdin=subprocess.DEVNULL,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            encoding="utf-8",
            errors="replace",
            check=False,
        )

    def test_fake_fully_pinned_tsv_path_is_ready_but_never_calibrated_or_public(self) -> None:
        completed = self.run_program(self.fake_manifest())
        self.assertEqual(completed.returncode, 0, completed.stderr)
        report = json.loads(completed.stdout)
        self.assertEqual(report["status"], "ready_for_adapter_implementation")
        self.assertEqual(report["blocking_requirements"], [])
        self.assertEqual(report["calibration_state"], "not_calibrated")
        self.assertFalse(report["automatic_publication"])
        self.assertFalse(report["identity_inference"])
        self.assertFalse(report["network_used"])
        self.assertFalse(report["consumed_sparse_frame_results"])
        self.assertFalse(report["emitted_ocr_observations"])
        statuses = {row["check_id"]: row["status"] for row in report["checks"]}
        self.assertEqual(statuses["OCR_FIXTURE_ENGLISH_FIXTURE_FFMPEG_GEOMETRY"], "unsupported")
        self.assertEqual(statuses["OCR_FIXTURE_ENGLISH_FIXTURE_TSV"], "pass")

    def test_hash_change_fails_closed_before_execution(self) -> None:
        path = self.fake_manifest()
        manifest = json.loads(path.read_text(encoding="utf-8"))
        manifest["ffmpeg"]["expected_sha256"] = "0" * 64
        write_json(path, manifest)
        completed = self.run_program(path)
        self.assertEqual(completed.returncode, 1)
        report = json.loads(completed.stdout)
        self.assertEqual(report["status"], "blocked")
        row = next(row for row in report["checks"] if row["check_id"] == "OCR_TOOL_FFMPEG_PIN")
        self.assertEqual(row["status"], "fail")
        self.assertIn("hash mismatch", row["message"])

    def test_missing_pinned_language_files_are_explicit_blockers(self) -> None:
        manifest, manifest_sha = ocr_preflight.load_manifest(DEFAULT_REQUIREMENTS)
        report = ocr_preflight.run_preflight(manifest, manifest_sha)
        self.assertEqual(report["status"], "blocked")
        blockers = {row["check_id"] for row in report["blocking_requirements"]}
        self.assertNotIn("OCR_TOOL_TESSERACT_CLI_PIN", blockers)
        self.assertNotIn("OCR_TOOL_TESSERACT_VERSION_PIN", blockers)
        self.assertIn("OCR_LANGUAGE_JPN_PIN", blockers)
        self.assertIn("OCR_LANGUAGE_KOR_PIN", blockers)
        self.assertFalse(report["automatic_publication"])
        self.assertEqual(report["calibration_state"], "not_calibrated")

    def test_manifest_unknown_field_and_policy_weakening_are_rejected(self) -> None:
        path = self.fake_manifest()
        manifest = json.loads(path.read_text(encoding="utf-8"))
        manifest["unknown"] = True
        with self.assertRaisesRegex(ocr_preflight.PreflightError, "unknown keys"):
            ocr_preflight.validate_manifest(manifest)
        manifest.pop("unknown")
        manifest["policy"]["automatic_publication"] = True
        with self.assertRaisesRegex(ocr_preflight.PreflightError, "must be false"):
            ocr_preflight.validate_manifest(manifest)

    def test_symlink_manifest_and_symlink_tool_fail_closed(self) -> None:
        path = self.fake_manifest()
        alias = self.case / "requirements-alias.json"
        alias.symlink_to(path)
        completed = self.run_program(alias)
        self.assertEqual(completed.returncode, 1)
        self.assertEqual(json.loads(completed.stdout)["status"], "invalid_preflight")

        manifest = json.loads(path.read_text(encoding="utf-8"))
        tool_alias = self.case / "ffmpeg-alias"
        tool_alias.symlink_to(Path(manifest["ffmpeg"]["path"]))
        manifest["ffmpeg"]["path"] = str(tool_alias)
        manifest["ffmpeg"]["expected_sha256"] = digest(Path(tool_alias).resolve())
        write_json(path, manifest)
        blocked = json.loads(self.run_program(path).stdout)
        row = next(row for row in blocked["checks"] if row["check_id"] == "OCR_TOOL_FFMPEG_PIN")
        self.assertEqual(row["status"], "fail")
        self.assertIn("without symlink", row["message"])

    def test_tsv_requires_real_positive_geometry_finite_confidence_and_text(self) -> None:
        header = "\t".join(ocr_preflight.TSV_FIELDS) + "\n"
        good = header + "5\t1\t1\t1\t1\t1\t10\t20\t30\t40\t88.5\tDaniel\n"
        parsed = ocr_preflight.validate_tsv(good.encode())
        self.assertEqual(parsed["word_count"], 1)
        for bad in (
            header + "5\t1\t1\t1\t1\t1\t10\t20\t0\t40\t88.5\tDaniel\n",
            header + "5\t1\t1\t1\t1\t1\t10\t20\t30\t40\tnan\tDaniel\n",
            header + "5\t1\t1\t1\t1\t1\t1270\t20\t30\t40\t88.5\tDaniel\n",
            header + "5\t1\t1\t1\t1\t1\t10\t20\t30\t40\t1000\tDaniel\n",
            header + "5\t1\t1\t1\t1\t1\t10\t20\t30\t40\t88.5\t\n",
        ):
            with self.assertRaises(ocr_preflight.PreflightError):
                ocr_preflight.validate_tsv(bad.encode())

    def test_command_environment_does_not_inherit_path_or_secrets(self) -> None:
        os.environ["HIMR_TEST_SECRET"] = "must-not-propagate"
        environment = ocr_preflight.command_environment(self.case)
        self.assertNotIn("HIMR_TEST_SECRET", environment)
        self.assertNotIn("PATH", environment)
        self.assertNotIn("HOME", environment)
        self.assertEqual(environment["OMP_NUM_THREADS"], "1")


if __name__ == "__main__":
    unittest.main()
