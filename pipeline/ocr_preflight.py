#!/usr/bin/env python3
"""Fail-closed, offline feasibility gate for a future sparse-frame OCR adapter.

This program does not consume corpus media, emit OCR observations, identify anyone,
or publish anything.  It verifies pinned local assets and probes whether an OCR CLI
can return deterministic TSV word geometry and raw confidence values.  FFmpeg's OCR
filter is inspected as a diagnostic, but its frame-level metadata is not accepted as
a substitute for word geometry.
"""

from __future__ import annotations

import argparse
import csv
import hashlib
import io
import json
import math
import os
import re
import stat
import subprocess
import sys
import tempfile
from pathlib import Path
from typing import Any


IMPLEMENTATION_VERSION = "0.2.0"
MAX_MANIFEST_BYTES = 1024 * 1024
MAX_COMMAND_OUTPUT_BYTES = 4 * 1024 * 1024
SHA256_RE = re.compile(r"^[0-9a-f]{64}$")
LANGUAGE_RE = re.compile(r"^[a-z][a-z0-9_]{1,15}$")
FIXTURE_ID_RE = re.compile(r"^[a-z][a-z0-9_]{1,63}$")
TSV_FIELDS = (
    "level",
    "page_num",
    "block_num",
    "par_num",
    "line_num",
    "word_num",
    "left",
    "top",
    "width",
    "height",
    "conf",
    "text",
)


class PreflightError(RuntimeError):
    """The preflight manifest or local execution boundary is invalid."""


def sha256_bytes(value: bytes) -> str:
    return hashlib.sha256(value).hexdigest()


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        while chunk := handle.read(1024 * 1024):
            digest.update(chunk)
    return digest.hexdigest()


def exact_keys(value: dict[str, Any], label: str, expected: set[str]) -> None:
    missing = sorted(expected - set(value))
    unknown = sorted(set(value) - expected)
    if missing:
        raise PreflightError(f"{label} is missing keys: {', '.join(missing)}")
    if unknown:
        raise PreflightError(f"{label} has unknown keys: {', '.join(unknown)}")


def require_object(value: Any, label: str) -> dict[str, Any]:
    if not isinstance(value, dict):
        raise PreflightError(f"{label} must be an object")
    return value


def require_text(value: Any, label: str, *, maximum: int = 4096) -> str:
    if (
        not isinstance(value, str)
        or not value
        or len(value) > maximum
        or "\x00" in value
    ):
        raise PreflightError(f"{label} must be non-empty bounded text")
    return value


def require_bool(value: Any, label: str, expected: bool) -> bool:
    if value is not expected:
        raise PreflightError(f"{label} must be {str(expected).lower()}")
    return expected


def require_pin(value: Any, label: str, *, allow_null: bool = False) -> str | None:
    if value is None and allow_null:
        return None
    if not isinstance(value, str) or not SHA256_RE.fullmatch(value):
        raise PreflightError(f"{label} must be a lowercase SHA-256")
    return value


def require_absolute_path(value: Any, label: str) -> str:
    text = require_text(value, label)
    if "://" in text or not Path(text).is_absolute():
        raise PreflightError(f"{label} must be an absolute local path")
    return text


def validate_pin_object(raw: Any, label: str, *, nullable_pin: bool = False) -> dict[str, Any]:
    value = require_object(raw, label)
    exact_keys(value, label, {"path", "expected_sha256"})
    return {
        "path": require_absolute_path(value["path"], f"{label}.path"),
        "expected_sha256": require_pin(
            value["expected_sha256"],
            f"{label}.expected_sha256",
            allow_null=nullable_pin,
        ),
    }


def validate_manifest(raw: Any) -> dict[str, Any]:
    root = require_object(raw, "manifest")
    exact_keys(
        root,
        "manifest",
        {
            "schema_version",
            "purpose",
            "policy",
            "ffmpeg",
            "tesseract_cli",
            "linked_libraries",
            "tessdata",
            "fixtures",
        },
    )
    if root["schema_version"] != 1:
        raise PreflightError("manifest.schema_version must equal 1")
    if root["purpose"] != "offline_ocr_adapter_feasibility_gate":
        raise PreflightError("manifest.purpose is unsupported")

    policy = require_object(root["policy"], "manifest.policy")
    exact_keys(
        policy,
        "manifest.policy",
        {
            "automatic_publication",
            "calibration_state",
            "identity_inference",
            "network_allowed",
            "require_deterministic_tsv",
            "require_raw_word_confidence",
            "require_word_boxes",
        },
    )
    require_bool(policy["automatic_publication"], "policy.automatic_publication", False)
    require_bool(policy["identity_inference"], "policy.identity_inference", False)
    require_bool(policy["network_allowed"], "policy.network_allowed", False)
    require_bool(policy["require_deterministic_tsv"], "policy.require_deterministic_tsv", True)
    require_bool(policy["require_raw_word_confidence"], "policy.require_raw_word_confidence", True)
    require_bool(policy["require_word_boxes"], "policy.require_word_boxes", True)
    if policy["calibration_state"] != "not_calibrated":
        raise PreflightError("policy.calibration_state must be not_calibrated")

    ffmpeg = require_object(root["ffmpeg"], "manifest.ffmpeg")
    exact_keys(
        ffmpeg,
        "manifest.ffmpeg",
        {
            "path",
            "expected_sha256",
            "expected_version_output_sha256",
            "expected_ocr_help_output_sha256",
        },
    )
    ffmpeg = {
        "path": require_absolute_path(ffmpeg["path"], "manifest.ffmpeg.path"),
        "expected_sha256": require_pin(ffmpeg["expected_sha256"], "manifest.ffmpeg.expected_sha256"),
        "expected_version_output_sha256": require_pin(
            ffmpeg["expected_version_output_sha256"],
            "manifest.ffmpeg.expected_version_output_sha256",
        ),
        "expected_ocr_help_output_sha256": require_pin(
            ffmpeg["expected_ocr_help_output_sha256"],
            "manifest.ffmpeg.expected_ocr_help_output_sha256",
        ),
    }

    tesseract = require_object(root["tesseract_cli"], "manifest.tesseract_cli")
    exact_keys(
        tesseract,
        "manifest.tesseract_cli",
        {"path", "expected_sha256", "expected_version_output_sha256"},
    )
    tesseract = {
        "path": require_absolute_path(tesseract["path"], "manifest.tesseract_cli.path"),
        "expected_sha256": require_pin(
            tesseract["expected_sha256"],
            "manifest.tesseract_cli.expected_sha256",
            allow_null=True,
        ),
        "expected_version_output_sha256": require_pin(
            tesseract["expected_version_output_sha256"],
            "manifest.tesseract_cli.expected_version_output_sha256",
            allow_null=True,
        ),
    }

    libraries_raw = root["linked_libraries"]
    if not isinstance(libraries_raw, list) or not libraries_raw:
        raise PreflightError("manifest.linked_libraries must be a non-empty array")
    libraries: list[dict[str, Any]] = []
    library_names: set[str] = set()
    for index, raw_library in enumerate(libraries_raw):
        label = f"manifest.linked_libraries[{index}]"
        library = require_object(raw_library, label)
        exact_keys(library, label, {"name", "path", "expected_sha256"})
        name = require_text(library["name"], f"{label}.name", maximum=64)
        if name in library_names:
            raise PreflightError(f"duplicate linked library name: {name}")
        library_names.add(name)
        libraries.append(
            {
                "name": name,
                "path": require_absolute_path(library["path"], f"{label}.path"),
                "expected_sha256": require_pin(
                    library["expected_sha256"], f"{label}.expected_sha256"
                ),
            }
        )
    required_libraries = {"libavfilter", "libtesseract", "libleptonica"}
    if set(library_names) != required_libraries:
        raise PreflightError(
            "linked libraries must be exactly libavfilter, libtesseract, and libleptonica"
        )

    tessdata = require_object(root["tessdata"], "manifest.tessdata")
    exact_keys(tessdata, "manifest.tessdata", {"path", "license", "languages"})
    tessdata_path = require_absolute_path(tessdata["path"], "manifest.tessdata.path")
    license_pin = validate_pin_object(tessdata["license"], "manifest.tessdata.license")
    languages_raw = tessdata["languages"]
    if not isinstance(languages_raw, list) or not languages_raw:
        raise PreflightError("manifest.tessdata.languages must be a non-empty array")
    languages: list[dict[str, Any]] = []
    language_codes: set[str] = set()
    for index, raw_language in enumerate(languages_raw):
        label = f"manifest.tessdata.languages[{index}]"
        language = require_object(raw_language, label)
        exact_keys(language, label, {"code", "filename", "expected_sha256"})
        code = require_text(language["code"], f"{label}.code", maximum=16)
        if not LANGUAGE_RE.fullmatch(code):
            raise PreflightError(f"{label}.code is invalid")
        if code in language_codes:
            raise PreflightError(f"duplicate language code: {code}")
        language_codes.add(code)
        filename = require_text(language["filename"], f"{label}.filename", maximum=64)
        if filename != f"{code}.traineddata":
            raise PreflightError(f"{label}.filename must equal {code}.traineddata")
        languages.append(
            {
                "code": code,
                "filename": filename,
                "expected_sha256": require_pin(
                    language["expected_sha256"],
                    f"{label}.expected_sha256",
                    allow_null=True,
                ),
            }
        )

    fixtures_raw = root["fixtures"]
    if not isinstance(fixtures_raw, list) or not fixtures_raw:
        raise PreflightError("manifest.fixtures must be a non-empty array")
    fixtures: list[dict[str, Any]] = []
    fixture_ids: set[str] = set()
    fixture_languages: set[str] = set()
    for index, raw_fixture in enumerate(fixtures_raw):
        label = f"manifest.fixtures[{index}]"
        fixture = require_object(raw_fixture, label)
        exact_keys(fixture, label, {"fixture_id", "language", "text", "font"})
        fixture_id = require_text(fixture["fixture_id"], f"{label}.fixture_id", maximum=64)
        if not FIXTURE_ID_RE.fullmatch(fixture_id):
            raise PreflightError(f"{label}.fixture_id is invalid")
        if fixture_id in fixture_ids:
            raise PreflightError(f"duplicate fixture_id: {fixture_id}")
        fixture_ids.add(fixture_id)
        language = require_text(fixture["language"], f"{label}.language", maximum=16)
        if language not in language_codes:
            raise PreflightError(f"{label}.language is not a required language")
        fixture_languages.add(language)
        fixtures.append(
            {
                "fixture_id": fixture_id,
                "language": language,
                "text": require_text(fixture["text"], f"{label}.text", maximum=256),
                "font": validate_pin_object(fixture["font"], f"{label}.font"),
            }
        )
    missing_fixtures = sorted(language_codes - fixture_languages)
    if missing_fixtures:
        raise PreflightError(
            "every required language needs a fixture; missing: " + ", ".join(missing_fixtures)
        )

    return {
        "schema_version": 1,
        "purpose": root["purpose"],
        "policy": policy,
        "ffmpeg": ffmpeg,
        "tesseract_cli": tesseract,
        "linked_libraries": libraries,
        "tessdata": {
            "path": tessdata_path,
            "license": license_pin,
            "languages": languages,
        },
        "fixtures": fixtures,
    }


def load_manifest(path: Path) -> tuple[dict[str, Any], str]:
    if not path.is_absolute():
        raise PreflightError("--requirements must be an absolute local path")
    try:
        info = path.lstat()
    except FileNotFoundError as error:
        raise PreflightError(f"requirements manifest does not exist: {path}") from error
    if stat.S_ISLNK(info.st_mode) or not stat.S_ISREG(info.st_mode):
        raise PreflightError("requirements manifest must be a regular file, not a symlink")
    if info.st_size > MAX_MANIFEST_BYTES:
        raise PreflightError("requirements manifest is too large")
    body = path.read_bytes()
    try:
        raw = json.loads(body.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError) as error:
        raise PreflightError(f"requirements manifest is invalid UTF-8 JSON: {error}") from error
    return validate_manifest(raw), sha256_bytes(body)


def check(
    checks: list[dict[str, Any]],
    check_id: str,
    *,
    required: bool,
    status: str,
    message: str,
    evidence: dict[str, Any] | None = None,
) -> bool:
    if status not in {"pass", "fail", "not_run", "unsupported"}:
        raise AssertionError(status)
    checks.append(
        {
            "check_id": check_id,
            "required": required,
            "status": status,
            "message": message,
            "evidence": evidence or {},
        }
    )
    return status == "pass"


def inspect_pinned_file(
    checks: list[dict[str, Any]],
    check_id: str,
    pin: dict[str, Any],
    *,
    executable: bool = False,
) -> Path | None:
    path = Path(pin["path"])
    expected = pin["expected_sha256"]
    if expected is None:
        exists = path.exists()
        check(
            checks,
            check_id,
            required=True,
            status="fail",
            message=(
                f"Required file is missing and has no reviewed SHA-256 pin: {path}"
                if not exists
                else f"No reviewed SHA-256 pin exists for {path}"
            ),
            evidence={"path": str(path), "exists": exists},
        )
        return None
    try:
        info = path.lstat()
    except FileNotFoundError:
        check(
            checks,
            check_id,
            required=True,
            status="fail",
            message=f"Required pinned file is missing: {path}",
            evidence={"path": str(path), "expected_sha256": expected},
        )
        return None
    if stat.S_ISLNK(info.st_mode) or not stat.S_ISREG(info.st_mode):
        check(
            checks,
            check_id,
            required=True,
            status="fail",
            message=f"Pinned path must be a regular file without symlink indirection: {path}",
            evidence={"path": str(path)},
        )
        return None
    if executable and not os.access(path, os.X_OK):
        check(
            checks,
            check_id,
            required=True,
            status="fail",
            message=f"Pinned executable is not executable: {path}",
            evidence={"path": str(path)},
        )
        return None
    observed = sha256_file(path)
    if observed != expected:
        check(
            checks,
            check_id,
            required=True,
            status="fail",
            message=f"Pinned file hash mismatch: {path}",
            evidence={
                "path": str(path),
                "expected_sha256": expected,
                "observed_sha256": observed,
                "byte_count": info.st_size,
            },
        )
        return None
    check(
        checks,
        check_id,
        required=True,
        status="pass",
        message=f"Pinned file is present and hash-matched: {path}",
        evidence={"path": str(path), "sha256": observed, "byte_count": info.st_size},
    )
    return path


def inspect_directory(
    checks: list[dict[str, Any]], check_id: str, path: Path
) -> bool:
    try:
        info = path.lstat()
    except FileNotFoundError:
        check(
            checks,
            check_id,
            required=True,
            status="fail",
            message=f"Required directory is missing: {path}",
            evidence={"path": str(path)},
        )
        return False
    valid = stat.S_ISDIR(info.st_mode) and not stat.S_ISLNK(info.st_mode)
    check(
        checks,
        check_id,
        required=True,
        status="pass" if valid else "fail",
        message=(
            f"Required directory is present without symlink indirection: {path}"
            if valid
            else f"Required directory must be a directory without symlink indirection: {path}"
        ),
        evidence={"path": str(path)},
    )
    return valid


def command_environment(tessdata: Path) -> dict[str, str]:
    return {
        "LANG": "C.UTF-8",
        "LC_ALL": "C.UTF-8",
        "TZ": "UTC",
        "OMP_NUM_THREADS": "1",
        "OMP_THREAD_LIMIT": "1",
        "TESSDATA_PREFIX": str(tessdata),
        "AV_LOG_FORCE_NOCOLOR": "1",
    }


def run_command(
    command: list[str], *, environment: dict[str, str], timeout_seconds: int = 30
) -> subprocess.CompletedProcess[bytes]:
    try:
        completed = subprocess.run(
            command,
            stdin=subprocess.DEVNULL,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            env=environment,
            timeout=timeout_seconds,
            check=False,
        )
    except (OSError, subprocess.TimeoutExpired) as error:
        raise PreflightError(f"cannot execute {command[0]}: {error}") from error
    if len(completed.stdout) + len(completed.stderr) > MAX_COMMAND_OUTPUT_BYTES:
        raise PreflightError(f"command output exceeds safety limit: {command[0]}")
    return completed


def combined_output(completed: subprocess.CompletedProcess[bytes]) -> bytes:
    return completed.stdout + completed.stderr


def ffmpeg_escape_path(path: Path) -> str:
    text = str(path)
    for source, replacement in (("\\", "\\\\"), (":", "\\:"), ("'", "\\'"), (",", "\\,")):
        text = text.replace(source, replacement)
    return text


def render_fixture(
    ffmpeg: Path,
    fixture: dict[str, Any],
    destination: Path,
    *,
    environment: dict[str, str],
) -> subprocess.CompletedProcess[bytes]:
    text_path = destination.with_suffix(".txt")
    text_path.write_text(fixture["text"], encoding="utf-8")
    font_path = Path(fixture["font"]["path"])
    video_filter = (
        f"drawtext=fontfile='{ffmpeg_escape_path(font_path)}':"
        f"textfile='{ffmpeg_escape_path(text_path)}':"
        "fontcolor=black:fontsize=64:x=40:y=100"
    )
    return run_command(
        [
            str(ffmpeg),
            "-hide_banner",
            "-nostdin",
            "-loglevel",
            "error",
            "-f",
            "lavfi",
            "-i",
            "color=c=white:s=1280x360:r=1:d=1",
            "-vf",
            video_filter,
            "-frames:v",
            "1",
            "-pix_fmt",
            "rgb24",
            str(destination),
        ],
        environment=environment,
    )


def ffmpeg_ocr_metadata(
    ffmpeg: Path,
    frame: Path,
    tessdata: Path,
    language: str,
    *,
    environment: dict[str, str],
) -> subprocess.CompletedProcess[bytes]:
    ocr_filter = (
        f"ocr=datapath='{ffmpeg_escape_path(tessdata)}':language={language}:whitelist=,"
        "metadata=print:file=-"
    )
    return run_command(
        [
            str(ffmpeg),
            "-hide_banner",
            "-nostdin",
            "-loglevel",
            "error",
            "-i",
            str(frame),
            "-vf",
            ocr_filter,
            "-frames:v",
            "1",
            "-f",
            "null",
            "-",
        ],
        environment=environment,
    )


def metadata_keys(output: bytes) -> list[str]:
    keys: set[str] = set()
    for line in output.decode("utf-8", errors="replace").splitlines():
        if line.startswith("lavfi.ocr.") and "=" in line:
            keys.add(line.split("=", 1)[0])
    return sorted(keys)


def metadata_values(output: bytes) -> dict[str, str]:
    values: dict[str, str] = {}
    for line in output.decode("utf-8", errors="replace").splitlines():
        if line.startswith("lavfi.ocr.") and "=" in line:
            key, value = line.split("=", 1)
            values[key] = value[:4096]
    return values


def tesseract_tsv(
    executable: Path,
    frame: Path,
    language: str,
    *,
    environment: dict[str, str],
) -> subprocess.CompletedProcess[bytes]:
    return run_command(
        [
            str(executable),
            str(frame),
            "stdout",
            "-l",
            language,
            "--psm",
            "6",
            "-c",
            "tessedit_create_tsv=1",
        ],
        environment=environment,
    )


def validate_tsv(
    body: bytes, *, frame_width: int = 1280, frame_height: int = 360
) -> dict[str, Any]:
    try:
        text = body.decode("utf-8")
    except UnicodeDecodeError as error:
        raise PreflightError(f"Tesseract TSV is not UTF-8: {error}") from error
    reader = csv.DictReader(io.StringIO(text), delimiter="\t")
    if tuple(reader.fieldnames or ()) != TSV_FIELDS:
        raise PreflightError("Tesseract TSV header does not expose the required fields")
    words = []
    for index, row in enumerate(reader, start=2):
        if row.get("level") != "5" or not (row.get("text") or "").strip():
            continue
        try:
            left = int(row["left"])
            top = int(row["top"])
            width = int(row["width"])
            height = int(row["height"])
            confidence = float(row["conf"])
        except (KeyError, TypeError, ValueError) as error:
            raise PreflightError(f"invalid word geometry/confidence on TSV row {index}") from error
        if (
            left < 0
            or top < 0
            or width <= 0
            or height <= 0
            or left + width > frame_width
            or top + height > frame_height
        ):
            raise PreflightError(f"invalid word box on TSV row {index}")
        if not math.isfinite(confidence) or not 0 <= confidence <= 100:
            raise PreflightError(f"invalid word confidence on TSV row {index}")
        words.append(
            {
                "left": left,
                "top": top,
                "width": width,
                "height": height,
                "raw_confidence": confidence,
                "text": row["text"],
            }
        )
    if not words:
        raise PreflightError("Tesseract TSV contains no word rows with text")
    return {"word_count": len(words), "words": words}


def run_preflight(manifest: dict[str, Any], manifest_sha256: str) -> dict[str, Any]:
    checks: list[dict[str, Any]] = []
    tessdata = Path(manifest["tessdata"]["path"])
    environment = command_environment(tessdata)

    ffmpeg = inspect_pinned_file(checks, "OCR_TOOL_FFMPEG_PIN", manifest["ffmpeg"], executable=True)
    tesseract = inspect_pinned_file(
        checks, "OCR_TOOL_TESSERACT_CLI_PIN", manifest["tesseract_cli"], executable=True
    )
    libraries_ok = True
    for library in manifest["linked_libraries"]:
        libraries_ok = (
            inspect_pinned_file(
                checks, f"OCR_LIBRARY_{library['name'].upper()}_PIN", library
            )
            is not None
            and libraries_ok
        )
    tessdata_directory_ok = inspect_directory(checks, "OCR_TESSDATA_DIRECTORY", tessdata)
    license_ok = inspect_pinned_file(
        checks, "OCR_TESSDATA_LICENSE_PIN", manifest["tessdata"]["license"]
    ) is not None

    language_paths: dict[str, Path] = {}
    for language in manifest["tessdata"]["languages"]:
        pin = {
            "path": str(tessdata / language["filename"]),
            "expected_sha256": language["expected_sha256"],
        }
        path = inspect_pinned_file(
            checks, f"OCR_LANGUAGE_{language['code'].upper()}_PIN", pin
        )
        if path is not None:
            language_paths[language["code"]] = path

    font_paths: dict[str, Path] = {}
    for fixture in manifest["fixtures"]:
        path = inspect_pinned_file(
            checks,
            f"OCR_FIXTURE_{fixture['fixture_id'].upper()}_FONT_PIN",
            fixture["font"],
        )
        if path is not None:
            font_paths[fixture["fixture_id"]] = path

    ffmpeg_ready = ffmpeg is not None and libraries_ok and tessdata_directory_ok and license_ok
    if ffmpeg is not None:
        version = run_command(
            [str(ffmpeg), "-hide_banner", "-version"], environment=environment
        )
        observed = sha256_bytes(combined_output(version))
        expected = manifest["ffmpeg"]["expected_version_output_sha256"]
        check(
            checks,
            "OCR_TOOL_FFMPEG_VERSION_PIN",
            required=True,
            status="pass" if version.returncode == 0 and observed == expected else "fail",
            message=(
                "FFmpeg version output is hash-matched"
                if version.returncode == 0 and observed == expected
                else "FFmpeg version output differs from the reviewed pin"
            ),
            evidence={
                "expected_sha256": expected,
                "observed_sha256": observed,
                "returncode": version.returncode,
            },
        )
        help_result = run_command(
            [str(ffmpeg), "-hide_banner", "-h", "filter=ocr"], environment=environment
        )
        help_body = combined_output(help_result)
        help_sha = sha256_bytes(help_body)
        expected_help = manifest["ffmpeg"]["expected_ocr_help_output_sha256"]
        help_ok = (
            help_result.returncode == 0
            and help_sha == expected_help
            and b"Optical Character Recognition" in help_body
            and b"datapath" in help_body
            and b"language" in help_body
        )
        check(
            checks,
            "OCR_TOOL_FFMPEG_FILTER_PIN",
            required=True,
            status="pass" if help_ok else "fail",
            message=(
                "FFmpeg OCR filter help is hash-matched"
                if help_ok
                else "FFmpeg OCR filter is missing or differs from the reviewed pin"
            ),
            evidence={
                "expected_sha256": expected_help,
                "observed_sha256": help_sha,
                "returncode": help_result.returncode,
            },
        )
        ffmpeg_ready = ffmpeg_ready and help_ok
    else:
        check(
            checks,
            "OCR_TOOL_FFMPEG_VERSION_PIN",
            required=True,
            status="not_run",
            message="FFmpeg version pin cannot be checked until its executable pin passes",
        )
        check(
            checks,
            "OCR_TOOL_FFMPEG_FILTER_PIN",
            required=True,
            status="not_run",
            message="FFmpeg OCR filter cannot be checked until its executable pin passes",
        )

    tesseract_version_ok = False
    if tesseract is not None:
        version = run_command([str(tesseract), "--version"], environment=environment)
        observed = sha256_bytes(combined_output(version))
        expected = manifest["tesseract_cli"]["expected_version_output_sha256"]
        tesseract_version_ok = (
            expected is not None and version.returncode == 0 and observed == expected
        )
        check(
            checks,
            "OCR_TOOL_TESSERACT_VERSION_PIN",
            required=True,
            status="pass" if tesseract_version_ok else "fail",
            message=(
                "Tesseract version output is hash-matched"
                if tesseract_version_ok
                else "Tesseract version output has no reviewed matching pin"
            ),
            evidence={
                "expected_sha256": expected,
                "observed_sha256": observed,
                "returncode": version.returncode,
            },
        )
    else:
        check(
            checks,
            "OCR_TOOL_TESSERACT_VERSION_PIN",
            required=True,
            status="not_run",
            message="Tesseract version cannot be checked until its executable pin passes",
        )

    with tempfile.TemporaryDirectory(prefix="himr-ocr-preflight-") as temporary:
        temporary_root = Path(temporary)
        for fixture in manifest["fixtures"]:
            fixture_id = fixture["fixture_id"]
            language = fixture["language"]
            frame = temporary_root / f"{fixture_id}.png"
            prerequisites = ffmpeg_ready and fixture_id in font_paths
            if not prerequisites:
                check(
                    checks,
                    f"OCR_FIXTURE_{fixture_id.upper()}_RENDER",
                    required=True,
                    status="not_run",
                    message="Synthetic fixture rendering prerequisites did not pass",
                )
                check(
                    checks,
                    f"OCR_FIXTURE_{fixture_id.upper()}_FFMPEG_DETERMINISM",
                    required=True,
                    status="not_run",
                    message="FFmpeg OCR determinism probe could not run",
                )
                check(
                    checks,
                    f"OCR_FIXTURE_{fixture_id.upper()}_TSV",
                    required=True,
                    status="not_run",
                    message="TSV geometry/confidence probe could not run",
                )
                continue
            rendered = render_fixture(
                ffmpeg, fixture, frame, environment=environment  # type: ignore[arg-type]
            )
            render_ok = rendered.returncode == 0 and frame.is_file() and frame.stat().st_size > 0
            check(
                checks,
                f"OCR_FIXTURE_{fixture_id.upper()}_RENDER",
                required=True,
                status="pass" if render_ok else "fail",
                message=("Synthetic fixture rendered" if render_ok else "Synthetic fixture rendering failed"),
                evidence={
                    "returncode": rendered.returncode,
                    "frame_sha256": sha256_file(frame) if render_ok else None,
                    "language": language,
                },
            )
            if not render_ok or language not in language_paths:
                check(
                    checks,
                    f"OCR_FIXTURE_{fixture_id.upper()}_FFMPEG_DETERMINISM",
                    required=True,
                    status="not_run",
                    message=f"Pinned {language} traineddata is unavailable",
                )
                check(
                    checks,
                    f"OCR_FIXTURE_{fixture_id.upper()}_TSV",
                    required=True,
                    status="not_run",
                    message=f"Pinned {language} traineddata is unavailable",
                )
                continue

            ffmpeg_runs = [
                ffmpeg_ocr_metadata(
                    ffmpeg, frame, tessdata, language, environment=environment  # type: ignore[arg-type]
                )
                for _ in range(3)
            ]
            ffmpeg_outputs = [completed.stdout for completed in ffmpeg_runs]
            keys = metadata_keys(ffmpeg_outputs[0]) if ffmpeg_outputs else []
            values = metadata_values(ffmpeg_outputs[0]) if ffmpeg_outputs else {}
            deterministic = (
                all(completed.returncode == 0 for completed in ffmpeg_runs)
                and len(set(ffmpeg_outputs)) == 1
                and "lavfi.ocr.text" in keys
                and "lavfi.ocr.confidence" in keys
            )
            check(
                checks,
                f"OCR_FIXTURE_{fixture_id.upper()}_FFMPEG_DETERMINISM",
                required=True,
                status="pass" if deterministic else "fail",
                message=(
                    "Three FFmpeg OCR metadata runs are byte-identical"
                    if deterministic
                    else "FFmpeg OCR metadata is missing or non-deterministic"
                ),
                evidence={
                    "language": language,
                    "metadata_keys": keys,
                    "recognized_text": values.get("lavfi.ocr.text"),
                    "raw_confidence_text": values.get("lavfi.ocr.confidence"),
                    "output_sha256": sha256_bytes(ffmpeg_outputs[0]) if ffmpeg_outputs else None,
                    "returncodes": [completed.returncode for completed in ffmpeg_runs],
                    "repeat_count": 3,
                },
            )
            geometry_keys = [
                key
                for key in keys
                if any(token in key.lower() for token in ("left", "top", "width", "height", "box", "bbox"))
            ]
            check(
                checks,
                f"OCR_FIXTURE_{fixture_id.upper()}_FFMPEG_GEOMETRY",
                required=False,
                status="pass" if geometry_keys else "unsupported",
                message=(
                    "FFmpeg OCR metadata exposes geometry"
                    if geometry_keys
                    else "FFmpeg OCR metadata exposes no word/line geometry and cannot be the adapter"
                ),
                evidence={"metadata_keys": keys, "geometry_keys": geometry_keys},
            )

            if tesseract is None or not tesseract_version_ok:
                check(
                    checks,
                    f"OCR_FIXTURE_{fixture_id.upper()}_TSV",
                    required=True,
                    status="not_run",
                    message="Pinned Tesseract CLI with reviewed version output is unavailable",
                )
                continue
            tsv_runs = [
                tesseract_tsv(tesseract, frame, language, environment=environment)
                for _ in range(3)
            ]
            try:
                parsed = validate_tsv(tsv_runs[0].stdout)
                tsv_ok = (
                    all(completed.returncode == 0 for completed in tsv_runs)
                    and len({completed.stdout for completed in tsv_runs}) == 1
                )
                tsv_error = None
            except PreflightError as error:
                parsed = None
                tsv_ok = False
                tsv_error = str(error)
            check(
                checks,
                f"OCR_FIXTURE_{fixture_id.upper()}_TSV",
                required=True,
                status="pass" if tsv_ok else "fail",
                message=(
                    "Three Tesseract TSV runs provide byte-identical word boxes and raw confidence"
                    if tsv_ok
                    else "Tesseract TSV geometry/confidence requirement failed"
                ),
                evidence={
                    "language": language,
                    "output_sha256": sha256_bytes(tsv_runs[0].stdout),
                    "repeat_count": 3,
                    "returncodes": [completed.returncode for completed in tsv_runs],
                    "word_count": parsed["word_count"] if parsed else 0,
                    "validation_error": tsv_error,
                },
            )

    blocking = [
        row
        for row in checks
        if row["required"] and row["status"] != "pass"
    ]
    return {
        "schema_version": 1,
        "implementation_version": IMPLEMENTATION_VERSION,
        "status": "blocked" if blocking else "ready_for_adapter_implementation",
        "manifest_sha256": manifest_sha256,
        "network_used": False,
        "consumed_sparse_frame_results": False,
        "emitted_ocr_observations": False,
        "automatic_publication": False,
        "identity_inference": False,
        "calibration_state": "not_calibrated",
        "checks": checks,
        "blocking_requirements": [
            {"check_id": row["check_id"], "message": row["message"]}
            for row in blocking
        ],
    }


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--requirements",
        type=Path,
        default=Path(__file__).with_name("ocr-preflight-requirements-v1.json"),
        help="absolute path to the reviewed feasibility/pin manifest",
    )
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    requirements = args.requirements
    if not requirements.is_absolute():
        requirements = Path.cwd() / requirements
    try:
        manifest, manifest_sha256 = load_manifest(requirements)
        report = run_preflight(manifest, manifest_sha256)
    except PreflightError as error:
        report = {
            "schema_version": 1,
            "implementation_version": IMPLEMENTATION_VERSION,
            "status": "invalid_preflight",
            "network_used": False,
            "consumed_sparse_frame_results": False,
            "emitted_ocr_observations": False,
            "automatic_publication": False,
            "identity_inference": False,
            "calibration_state": "not_calibrated",
            "error": {"type": type(error).__name__, "message": str(error)},
        }
    print(json.dumps(report, ensure_ascii=False, sort_keys=True, indent=2))
    return 0 if report["status"] == "ready_for_adapter_implementation" else 1


if __name__ == "__main__":
    raise SystemExit(main())
