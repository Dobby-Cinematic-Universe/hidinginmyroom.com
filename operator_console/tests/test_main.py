from __future__ import annotations

import io
import json
import unittest
from pathlib import Path
from unittest import mock

from operator_console.__main__ import main
from operator_console.service import DEFAULT_SERVICE_STATE_DIRECTORY


class OperatorMainTests(unittest.TestCase):
    def test_stage_successor_routes_exact_paths_without_serving(self) -> None:
        workspace = Path("/srv/himr/research/operator-console")
        config = Path("/srv/himr/research/corpus/new/config.json")
        output = workspace / "profiles.successor.json"
        expected = {
            "status": "successor_profile_set_staged",
            "processing_started": False,
        }
        with (
            mock.patch(
                "operator_console.__main__.materialize_successor_profile_set",
                return_value=expected,
            ) as materialize,
            mock.patch("sys.stdout", new_callable=io.StringIO) as stdout,
        ):
            status = main(
                [
                    "stage-autonomy-successor",
                    "--workspace",
                    str(workspace),
                    "--expected-profile-set-sha256",
                    "1" * 64,
                    "--config",
                    str(config),
                    "--expected-config-sha256",
                    "2" * 64,
                    "--output",
                    str(output),
                ]
            )

        self.assertEqual(0, status)
        self.assertEqual(expected, json.loads(stdout.getvalue()))
        self.assertEqual(
            {
                "repo_root": Path(__file__).resolve().parents[2],
                "current_profiles": workspace / "profiles.json",
                "expected_current_profiles_sha256": "1" * 64,
                "successor_config": config,
                "expected_successor_config_sha256": "2" * 64,
                "output": output,
            },
            materialize.call_args.kwargs,
        )

    def test_serve_defaults_to_console_owned_service_state(self) -> None:
        workspace = Path("/srv/himr/research/operator-console")
        service = mock.Mock()
        service.profile_set.raw_sha256 = "a" * 64
        service.profile_set.profiles = []
        service.cancellation_supported = False
        server = mock.Mock()
        server.origin = "http://127.0.0.1:12345"
        server.bootstrap_url = f"{server.origin}/bootstrap/token"

        with (
            mock.patch(
                "operator_console.__main__.OperatorService", return_value=service
            ) as ctor,
            mock.patch("operator_console.__main__.OperatorHTTP", return_value=server),
            mock.patch("sys.stdout", new_callable=io.StringIO),
        ):
            status = main(["serve", "--workspace", str(workspace), "--port", "0"])

        self.assertEqual(status, 0)
        self.assertEqual(
            ctor.call_args.kwargs["state_root"],
            workspace / DEFAULT_SERVICE_STATE_DIRECTORY,
        )
        server.serve_forever.assert_called_once_with(open_browser=False)
        server.close.assert_called_once_with()
        service.close.assert_called_once_with()


if __name__ == "__main__":
    unittest.main()
