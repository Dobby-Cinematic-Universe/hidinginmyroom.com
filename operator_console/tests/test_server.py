from __future__ import annotations

import http.client
import json
import tempfile
import threading
import unittest
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable
from unittest import mock

from operator_console import registry
from operator_console.server import OperatorHTTP
from operator_console.service import OperatorService


@dataclass(frozen=True)
class HTTPResult:
    status: int
    headers: dict[str, tuple[str, ...]]
    body: bytes

    def header(self, name: str) -> str | None:
        values = self.headers.get(name.lower(), ())
        if not values:
            return None
        if len(values) != 1:
            raise AssertionError(f"header {name!r} is not singular: {values!r}")
        return values[0]


class OperatorHTTPTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temporary = tempfile.TemporaryDirectory(prefix="himr-operator-http-")
        self.repo = Path(self.temporary.name).resolve()
        research = self.repo / "research"
        research.mkdir()
        workspace = research / "console"
        workspace.mkdir(mode=0o700)
        self.profile_path = workspace / "profiles.json"
        self.state_root = workspace / "state"
        fake = research / "fake-tool"
        fake.write_text(
            "#!/usr/bin/python3\nimport json\nprint(json.dumps({'status':'fixture_ok'}))\n",
            encoding="utf-8",
        )
        fake.chmod(0o700)
        action = registry.ActionSpec(
            action_id="preprocess.status",
            stage="Fixture",
            label="Finite fixture",
            description="Fake finite executable only.",
            effect="inspect",
            resource="preprocess",
            launcher="repo",
            entrypoint=str(fake.relative_to(self.repo)),
            prefix=(),
            fields=(),
            confirmation=None,
            timeout_seconds=5,
        )
        self.action_patch = mock.patch.dict(
            registry.ACTIONS, {"preprocess.status": action}, clear=False
        )
        self.action_patch.start()
        document = {
            "schema_version": 1,
            "profiles": [
                {
                    "id": "fixture.profile",
                    "label": "Fixture profile",
                    "description": "Fake finite command.",
                    "action": "preprocess.status",
                    "parameters": {},
                }
            ],
        }
        self.profile_path.write_text(
            json.dumps(document, sort_keys=True, separators=(",", ":")) + "\n",
            encoding="utf-8",
        )
        self.profile_path.chmod(0o400)
        self.ui_root = workspace / "ui"
        self.ui_root.mkdir(mode=0o700)
        (self.ui_root / "index.html").write_text("<!doctype html><title>fixture</title>")
        (self.ui_root / "app.js").write_text('"use strict";')
        (self.ui_root / "styles.css").write_text("body { color: white; }")
        self.service = OperatorService(
            repo_root=self.repo,
            profile_path=self.profile_path,
            state_root=self.state_root,
        )
        self.app = OperatorHTTP(self.service, port=0, ui_root=self.ui_root)
        self.thread = threading.Thread(
            target=self.app.server.serve_forever,
            kwargs={"poll_interval": 0.01},
            daemon=True,
        )
        self.thread.start()

    def tearDown(self) -> None:
        self.app.server.shutdown()
        self.thread.join(timeout=3)
        self.app.close()
        self.service.wait_for_jobs(timeout=3)
        self.service.close()
        self.action_patch.stop()
        self.temporary.cleanup()

    def request(
        self,
        method: str,
        path: str,
        *,
        headers: Iterable[tuple[str, str]] = (),
        body: bytes | str | None = None,
        host_values: tuple[str, ...] | None = None,
    ) -> HTTPResult:
        if isinstance(body, str):
            body = body.encode("utf-8")
        supplied = list(headers)
        lower = [name.lower() for name, _value in supplied]
        if body is not None and "content-length" not in lower:
            supplied.append(("Content-Length", str(len(body))))
        if host_values is None:
            host_values = (self.app.host_header,)
        connection = http.client.HTTPConnection("127.0.0.1", self.app.port, timeout=5)
        try:
            connection.putrequest(method, path, skip_host=True, skip_accept_encoding=True)
            for value in host_values:
                connection.putheader("Host", value)
            for name, value in supplied:
                connection.putheader(name, value)
            connection.endheaders(body)
            response = connection.getresponse()
            payload = response.read()
            output: dict[str, list[str]] = {}
            for name, value in response.getheaders():
                output.setdefault(name.lower(), []).append(value)
            return HTTPResult(
                response.status,
                {key: tuple(values) for key, values in output.items()},
                payload,
            )
        finally:
            connection.close()

    @staticmethod
    def json(response: HTTPResult) -> dict[str, object]:
        value = json.loads(response.body.decode("utf-8"))
        if not isinstance(value, dict):
            raise AssertionError("response JSON is not an object")
        return value

    def assert_security_headers(self, response: HTTPResult) -> None:
        self.assertEqual(response.header("Cache-Control"), "no-store, private, max-age=0")
        self.assertEqual(response.header("Referrer-Policy"), "no-referrer")
        self.assertEqual(response.header("X-Content-Type-Options"), "nosniff")
        self.assertEqual(response.header("X-Frame-Options"), "DENY")
        self.assertEqual(response.header("Cross-Origin-Opener-Policy"), "same-origin")
        self.assertEqual(response.header("Cross-Origin-Resource-Policy"), "same-origin")
        self.assertIn("default-src 'none'", response.header("Content-Security-Policy") or "")

    def bootstrap(self) -> tuple[str, dict[str, object]]:
        response = self.request("GET", f"/bootstrap/{self.app.bootstrap_token}")
        self.assertEqual(response.status, 303)
        cookie = response.header("Set-Cookie")
        self.assertIsNotNone(cookie)
        assert cookie is not None
        session = cookie.split(";", 1)[0]
        state = self.request(
            "GET", self.app.prefix + "api/state", headers=(("Cookie", session),)
        )
        return session, self.json(state)

    def mutation_headers(self, cookie: str, csrf: str) -> list[tuple[str, str]]:
        return [
            ("Cookie", cookie),
            ("Origin", self.app.origin),
            ("Sec-Fetch-Site", "same-origin"),
            ("X-HIMR-CSRF", csrf),
            ("Content-Type", "application/json"),
        ]

    def test_exact_loopback_bootstrap_authentication_and_security_headers(self) -> None:
        address = self.app.server.server_address
        self.assertEqual(address[0], "127.0.0.1")
        self.assertNotIn("localhost", self.app.bootstrap_url)
        route = f"/bootstrap/{self.app.bootstrap_token}"
        head = self.request("HEAD", route)
        self.assertEqual(head.status, 405)
        self.assertTrue(self.app.bootstrap_available)

        unauthenticated = self.request("GET", self.app.prefix + "api/state")
        self.assertEqual(unauthenticated.status, 403)
        response = self.request("GET", route)
        self.assertEqual(response.status, 303)
        self.assertEqual(response.header("Location"), self.app.prefix)
        cookie = response.header("Set-Cookie") or ""
        self.assertIn("HttpOnly", cookie)
        self.assertIn("SameSite=Strict", cookie)
        self.assertIn(f"Path={self.app.prefix}", cookie)
        self.assert_security_headers(response)
        expired = self.request("GET", route)
        self.assertEqual(expired.status, 410)

        session = cookie.split(";", 1)[0]
        for path in (self.app.prefix, self.app.prefix + "app.js", self.app.prefix + "styles.css"):
            asset = self.request("GET", path, headers=(("Cookie", session),))
            self.assertEqual(asset.status, 200)
            self.assert_security_headers(asset)
            self.assertTrue((asset.header("ETag") or "").startswith('"sha256-'))

    def test_host_path_body_framing_and_methods_fail_closed(self) -> None:
        cookie, _state = self.bootstrap()
        route = self.app.prefix + "api/state"
        for hosts in ((), ("localhost",), ("127.0.0.1:1",), (self.app.host_header,) * 2):
            with self.subTest(hosts=hosts):
                response = self.request(
                    "GET", route, headers=(("Cookie", cookie),), host_values=hosts
                )
                self.assertEqual(response.status, 400)
        for invalid in (route + "?x=1", route + "%3Fx", self.app.prefix + "api\\state"):
            with self.subTest(path=invalid):
                response = self.request("GET", invalid, headers=(("Cookie", cookie),))
                self.assertEqual(response.status, 400)
        body_get = self.request(
            "GET",
            route,
            headers=(("Cookie", cookie), ("Content-Length", "1")),
            body=b"x",
        )
        self.assertEqual(body_get.status, 400)
        for method in ("PUT", "PATCH", "DELETE", "OPTIONS", "TRACE", "CONNECT"):
            with self.subTest(method=method):
                response = self.request(method, route)
                self.assertEqual(response.status, 405)

    def test_mutations_require_cookie_origin_csrf_and_exact_shapes(self) -> None:
        cookie, state = self.bootstrap()
        csrf = str(state["csrf_token"])
        body = json.dumps(
            {"profile_id": "fixture.profile", "expected_revision": state["revision"]}
        )
        headers = self.mutation_headers(cookie, csrf)
        variants = {
            "no_cookie": [row for row in headers if row[0] != "Cookie"],
            "no_origin": [row for row in headers if row[0] != "Origin"],
            "wrong_origin": [
                (name, "http://localhost") if name == "Origin" else (name, value)
                for name, value in headers
            ],
            "no_csrf": [row for row in headers if row[0] != "X-HIMR-CSRF"],
            "cross_site": [
                (name, "cross-site") if name == "Sec-Fetch-Site" else (name, value)
                for name, value in headers
            ],
            "duplicate_origin": [*headers, ("Origin", self.app.origin)],
        }
        for name, variant in variants.items():
            with self.subTest(name=name):
                response = self.request(
                    "POST", self.app.prefix + "api/prepare", headers=variant, body=body
                )
                self.assertEqual(response.status, 403)

        wrong_shape = self.request(
            "POST",
            self.app.prefix + "api/prepare",
            headers=headers,
            body=json.dumps({"profile_id": "fixture.profile", "revision": state["revision"]}),
        )
        self.assertEqual(wrong_shape.status, 422)
        duplicate = self.request(
            "POST",
            self.app.prefix + "api/prepare",
            headers=headers,
            body=b'{"profile_id":"fixture.profile","expected_revision":0,"expected_revision":0}',
        )
        self.assertEqual(duplicate.status, 400)

    def test_prepare_execute_state_and_offset_log_route(self) -> None:
        cookie, state = self.bootstrap()
        headers = self.mutation_headers(cookie, str(state["csrf_token"]))
        prepare = self.request(
            "POST",
            self.app.prefix + "api/prepare",
            headers=headers,
            body=json.dumps(
                {"profile_id": "fixture.profile", "expected_revision": state["revision"]}
            ),
        )
        self.assertEqual(prepare.status, 200)
        prepared_response = self.json(prepare)
        prepared = prepared_response["prepared"]
        current = prepared_response["state"]
        assert isinstance(prepared, dict) and isinstance(current, dict)
        self.assertNotIn("argv", prepared)
        execute = self.request(
            "POST",
            self.app.prefix + "api/execute",
            headers=headers,
            body=json.dumps(
                {
                    "preparation_token": prepared["preparation_token"],
                    "expected_revision": current["revision"],
                    "confirmation": None,
                }
            ),
        )
        self.assertEqual(execute.status, 202)
        job_id = self.json(execute)["job"]["job_id"]
        self.assertTrue(self.service.wait_for_jobs(timeout=3))
        refreshed = self.request(
            "GET", self.app.prefix + "api/state", headers=(("Cookie", cookie),)
        )
        jobs = self.json(refreshed)["jobs"]
        job = next(row for row in jobs if row["job_id"] == job_id)
        self.assertEqual(job["state"], "succeeded")
        base = job["logs"]["stdout"]["base_url"]
        chunk = self.request("GET", base + "0", headers=(("Cookie", cookie),))
        self.assertEqual(chunk.status, 200)
        value = self.json(chunk)
        self.assertEqual(value["offset"], 0)
        self.assertTrue(value["eof"])
        self.assertIn("fixture_ok", value["text"])
        beyond = self.request(
            "GET",
            base + str(value["next_offset"] + 1),
            headers=(("Cookie", cookie),),
        )
        self.assertEqual(beyond.status, 416)
        head_beyond = self.request(
            "HEAD",
            base + str(value["next_offset"] + 1),
            headers=(("Cookie", cookie),),
        )
        self.assertEqual(head_beyond.status, 416)
        self.assertEqual(head_beyond.body, b"")
        self.assertGreater(int(head_beyond.header("Content-Length") or "0"), 0)
        unauthenticated = self.request("GET", base + "0")
        self.assertEqual(unauthenticated.status, 403)

    def test_cancel_endpoint_is_present_but_never_signals(self) -> None:
        cookie, state = self.bootstrap()
        headers = self.mutation_headers(cookie, str(state["csrf_token"]))
        response = self.request(
            "POST",
            self.app.prefix + "api/cancel",
            headers=headers,
            body=json.dumps(
                {
                    "job_id": "job_" + "0" * 32,
                    "expected_revision": state["revision"],
                    "confirmation": None,
                }
            ),
        )
        self.assertEqual(response.status, 404)
        self.assertEqual(self.json(response)["error"], "unknown_job")


if __name__ == "__main__":
    unittest.main()
