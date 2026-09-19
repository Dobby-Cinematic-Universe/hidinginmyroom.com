"""Command-line entry point for the private HIMR operator console."""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Any

from .server import OperatorHTTP
from .materialize_successor_profiles import (
    SuccessorProfileError,
    materialize_successor_profile_set,
)
from .service import (
    DEFAULT_SERVICE_STATE_DIRECTORY,
    OperatorService,
    ServiceError,
    initialize_workspace,
    validate_profiles,
)


def _repository_root() -> Path:
    return Path(__file__).resolve().parent.parent


def _absolute(value: str | Path) -> Path:
    path = Path(value)
    return path if path.is_absolute() else (Path.cwd() / path).absolute()


def _emit(value: dict[str, Any], *, error: bool = False) -> None:
    stream = sys.stderr if error else sys.stdout
    stream.write(
        json.dumps(
            value,
            ensure_ascii=False,
            sort_keys=True,
            separators=(",", ":"),
            allow_nan=False,
        )
        + "\n"
    )
    stream.flush()


def build_parser() -> argparse.ArgumentParser:
    repository = _repository_root()
    default_workspace = repository / "research" / "operator-console"
    parser = argparse.ArgumentParser(
        prog="himr-operator",
        description="Run the private loopback-only HIMR pipeline operator console.",
    )
    commands = parser.add_subparsers(dest="command", required=True)

    initialize = commands.add_parser(
        "init", help="create an empty owner-only operator workspace without replacement"
    )
    initialize.add_argument("--workspace", default=str(default_workspace))

    validate = commands.add_parser(
        "validate-profiles", help="strictly validate every closed profile without execution"
    )
    validate.add_argument("--workspace", default=str(default_workspace))
    validate.add_argument("--profiles")

    successor = commands.add_parser(
        "stage-autonomy-successor",
        help=(
            "seal a stopped successor Start/Stop profile set without changing the "
            "active console"
        ),
    )
    successor.add_argument("--workspace", default=str(default_workspace))
    successor.add_argument("--profiles")
    successor.add_argument("--expected-profile-set-sha256", required=True)
    successor.add_argument("--config", type=Path, required=True)
    successor.add_argument("--expected-config-sha256", required=True)
    successor.add_argument("--output", type=Path, required=True)

    serve = commands.add_parser("serve", help="serve the authenticated loopback console")
    serve.add_argument("--workspace", default=str(default_workspace))
    serve.add_argument("--profiles")
    serve.add_argument(
        "--state-root",
        help=(
            "owner-only console job state "
            f"(default: WORKSPACE/{DEFAULT_SERVICE_STATE_DIRECTORY})"
        ),
    )
    serve.add_argument("--port", type=int, default=0)
    serve.add_argument("--open-browser", action="store_true")
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    repo_root = _repository_root().resolve(strict=True)
    workspace = _absolute(args.workspace)
    try:
        if args.command == "init":
            _emit(initialize_workspace(repo_root=repo_root, workspace=workspace))
            return 0

        profiles = _absolute(args.profiles) if args.profiles else workspace / "profiles.json"
        if args.command == "stage-autonomy-successor":
            _emit(
                materialize_successor_profile_set(
                    repo_root=repo_root,
                    current_profiles=profiles,
                    expected_current_profiles_sha256=args.expected_profile_set_sha256,
                    successor_config=_absolute(args.config),
                    expected_successor_config_sha256=args.expected_config_sha256,
                    output=_absolute(args.output),
                )
            )
            return 0
        if args.command == "validate-profiles":
            _emit(validate_profiles(repo_root=repo_root, profile_path=profiles))
            return 0

        state_root = (
            _absolute(args.state_root)
            if args.state_root
            else workspace / DEFAULT_SERVICE_STATE_DIRECTORY
        )
        service = OperatorService(
            repo_root=repo_root,
            profile_path=profiles,
            state_root=state_root,
        )
        server: OperatorHTTP | None = None
        try:
            server = OperatorHTTP(service, port=args.port)
            _emit(
                {
                    "schema_version": 1,
                    "status": "serving",
                    "origin": server.origin,
                    "bootstrap_url": server.bootstrap_url,
                    "profile_set_sha256": service.profile_set.raw_sha256,
                    "profile_count": len(service.profile_set.profiles),
                    "advisory_only": True,
                    "cancellation_supported": service.cancellation_supported,
                }
            )
            server.serve_forever(open_browser=args.open_browser)
        except KeyboardInterrupt:
            pass
        finally:
            if server is not None:
                server.close()
            service.close()
        return 0
    except (ServiceError, SuccessorProfileError, OSError, ValueError) as error:
        _emit(
            {
                "schema_version": 1,
                "status": "failed",
                "error": {
                    "type": type(error).__name__,
                    "message": str(error),
                },
            },
            error=True,
        )
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
