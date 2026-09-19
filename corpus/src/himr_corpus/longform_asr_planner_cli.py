"""Standalone CLI for the metadata-only long-form ASR planner."""

from __future__ import annotations

import argparse
import os
import sys
from pathlib import Path
from typing import Sequence

from .longform_asr_planner import (
    LongformPlanningError,
    build_longform_asr_plan,
    canonical_json,
    load_strict_json,
    validate_longform_asr_plan,
)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="himr-longform-asr-plan")
    subparsers = parser.add_subparsers(dest="command", required=True)

    build = subparsers.add_parser(
        "build", help="emit a direct or adaptive logical-span plan"
    )
    build.add_argument("--manifest", required=True)
    build.add_argument("--policy", required=True)
    build.add_argument(
        "--output",
        default="-",
        help="new plan JSON path, or '-' for canonical JSON on stdout",
    )

    validate = subparsers.add_parser(
        "validate", help="rebuild and validate a previously emitted plan"
    )
    validate.add_argument("--plan", required=True)
    return parser


def _write_new_json(path_value: str, body: bytes) -> None:
    path = Path(path_value)
    if not path.is_absolute():
        raise LongformPlanningError("output path must be absolute")
    if not path.parent.is_dir():
        raise LongformPlanningError("output parent directory does not exist")
    temporary = path.with_name(f".{path.name}.tmp-{os.getpid()}")
    descriptor: int | None = None
    try:
        descriptor = os.open(temporary, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
        with os.fdopen(descriptor, "wb") as handle:
            descriptor = None
            handle.write(body)
            handle.flush()
            os.fsync(handle.fileno())
        if path.exists():
            raise LongformPlanningError("output path already exists")
        os.link(temporary, path)
        path.chmod(0o400)
    except OSError as error:
        raise LongformPlanningError(
            f"plan output cannot be created: {error}"
        ) from error
    finally:
        if descriptor is not None:
            os.close(descriptor)
        try:
            temporary.unlink()
        except FileNotFoundError:
            pass


def main(argv: Sequence[str] | None = None) -> int:
    parser = build_parser()
    arguments = parser.parse_args(argv)
    try:
        if arguments.command == "build":
            manifest = load_strict_json(arguments.manifest, "recording manifest")
            policy = load_strict_json(arguments.policy, "planning policy")
            value = build_longform_asr_plan(manifest, policy)
            body = (canonical_json(value) + "\n").encode("utf-8")
            if arguments.output == "-":
                sys.stdout.buffer.write(body)
            else:
                _write_new_json(arguments.output, body)
            return 0
        if arguments.command == "validate":
            value = load_strict_json(arguments.plan, "long-form ASR plan")
            validated = validate_longform_asr_plan(value)
            sys.stdout.write(canonical_json(validated) + "\n")
            return 0
    except LongformPlanningError as error:
        parser.exit(2, f"himr-longform-asr-plan: error: {error}\n")
    raise AssertionError("argparse accepted an unknown command")


if __name__ == "__main__":
    raise SystemExit(main())
