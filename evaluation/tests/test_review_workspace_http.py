from __future__ import annotations

import concurrent.futures
import hashlib
import http.client
import json
import os
import socket
import tempfile
import threading
import unittest
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Iterable
from unittest.mock import patch

from evaluation.review_workspace import (
    ReviewWorkspaceHTTP,
    StaleRevision,
    _parse_single_range,
)
from evaluation.validation import ContractError


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
            raise AssertionError(f"response header {name!r} is not singular: {values!r}")
        return values[0]


class FakeMedia:
    def __init__(self, path: Path, body: bytes):
        self.path = path
        self.fd = os.open(path, os.O_RDONLY | getattr(os, "O_CLOEXEC", 0))
        self.opaque_id = "parent_media_fixture"
        self.byte_count = len(body)
        self.mime_type = "video/mp4"
        self.sha256 = hashlib.sha256(body).hexdigest()
        self.unchanged_checks: list[bool] = []

    def assert_unchanged(self, *, rehash: bool) -> None:
        self.unchanged_checks.append(rehash)
        info = os.fstat(self.fd)
        if info.st_size != self.byte_count:
            raise ContractError("$.media: changed")

    def close(self) -> None:
        os.close(self.fd)


class FakeWorkspace:
    """Small deterministic surface consumed by ReviewWorkspaceHTTP."""

    def __init__(self, media: FakeMedia):
        self.media = [media]
        self.revision = 7
        self.completed = False
        self.bootstrap_calls: list[tuple[str, str]] = []
        self.operation_calls: list[tuple[int, object]] = []
        self.finalize_calls: list[tuple[int, object]] = []
        self._lock = threading.Lock()

    def expected_bootstrap(self, *, prefix: str, csrf_token: str) -> dict[str, Any]:
        state = "completed" if self.completed else "editing"
        return {
            "schema_version": 1,
            "workspace_id": "selection_workspace_http_fixture",
            "review_id": "selection_review_http_fixture",
            "state": state,
            "revision": self.revision,
            "csrf_token": csrf_token,
            "privacy": {
                "storage_policy": "private_only",
                "publication_authority": "none",
                "asr_or_reference_data_available": False,
            },
            "minimum_accepted_duration_ms": 3_600_000,
            "progress": {
                "ready": False,
                "decision_count": 1,
                "decided_count": self.revision - 7,
            },
            "recordings": [
                {
                    "recording_id": "rec_http_fixture",
                    "title": "HTTP fixture media",
                    "media": {
                        "url": f"{prefix}media/{self.media[0].opaque_id}",
                        "byte_count": self.media[0].byte_count,
                        "sha256": self.media[0].sha256,
                    },
                    "intervals": [],
                }
            ],
            "completed_manifest_sha256": (
                hashlib.sha256(b"completed-http-fixture").hexdigest()
                if self.completed
                else None
            ),
        }

    def bootstrap(self, *, prefix: str, csrf_token: str) -> dict[str, Any]:
        with self._lock:
            self.bootstrap_calls.append((prefix, csrf_token))
            return self.expected_bootstrap(prefix=prefix, csrf_token=csrf_token)

    def apply_operation(self, expected_revision: int, operation: object) -> None:
        with self._lock:
            if expected_revision != self.revision:
                raise StaleRevision(self.revision)
            self.operation_calls.append((expected_revision, operation))
            self.revision += 1

    def finalize(self, expected_revision: int, payload: object) -> None:
        with self._lock:
            if expected_revision != self.revision:
                raise StaleRevision(self.revision)
            self.finalize_calls.append((expected_revision, payload))
            self.revision += 1
            self.completed = True


class RangeParserTests(unittest.TestCase):
    def test_closed_open_and_suffix_ranges(self) -> None:
        cases = {
            "bytes=0-0": (0, 0),
            "bytes=2-5": (2, 5),
            "bytes=5-": (5, 9),
            "bytes=-3": (7, 9),
            "bytes=-99": (0, 9),
            "bytes=8-99": (8, 9),
        }
        for value, expected in cases.items():
            with self.subTest(value=value):
                self.assertEqual(_parse_single_range(value, 10), expected)

    def test_malformed_multi_and_out_of_range_values_fail_closed(self) -> None:
        values = [
            "",
            "Bytes=0-1",
            "items=0-1",
            "bytes=",
            "bytes=-",
            "bytes=-0",
            "bytes=0-1,3-4",
            "bytes=0 -1",
            "bytes= 0-1",
            "bytes=0- 1",
            "bytes=0--1",
            "bytes=-1-2",
            "bytes=+1-2",
            "bytes=10-",
            "bytes=9-8",
            "bytes=٠-١",
        ]
        for value in values:
            with self.subTest(value=value), self.assertRaises(ValueError):
                _parse_single_range(value, 10)

    def test_empty_representation_has_no_satisfiable_suffix(self) -> None:
        with self.assertRaises(ValueError):
            _parse_single_range("bytes=-1", 0)


class ReviewWorkspaceHTTPTests(unittest.TestCase):
    media_body = b"\x00HIMR-range-fixture\xff"

    def setUp(self) -> None:
        self.temporary = tempfile.TemporaryDirectory()
        self.root = Path(self.temporary.name)
        self.media_path = self.root / "payload"
        self.media_path.write_bytes(self.media_body)
        self.media = FakeMedia(self.media_path, self.media_body)
        self.workspace = FakeWorkspace(self.media)
        self.app = ReviewWorkspaceHTTP(self.workspace, port=0)
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
        self.media.close()
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
        lower_names = [name.lower() for name, _value in supplied]
        if body is not None and "content-length" not in lower_names:
            supplied.append(("Content-Length", str(len(body))))
        if host_values is None:
            host_values = (self.app.host_header,)

        connection = http.client.HTTPConnection("127.0.0.1", self.app.port, timeout=5)
        try:
            connection.putrequest(
                method,
                path,
                skip_host=True,
                skip_accept_encoding=True,
            )
            for value in host_values:
                connection.putheader("Host", value)
            for name, value in supplied:
                connection.putheader(name, value)
            connection.endheaders(body)
            response = connection.getresponse()
            response_body = response.read()
            output_headers: dict[str, list[str]] = {}
            for name, value in response.getheaders():
                output_headers.setdefault(name.lower(), []).append(value)
            return HTTPResult(
                status=response.status,
                headers={key: tuple(values) for key, values in output_headers.items()},
                body=response_body,
            )
        finally:
            connection.close()

    def raw_request(self, request: bytes) -> bytes:
        """Send an exact request target that http.client would normalize."""

        with socket.create_connection(("127.0.0.1", self.app.port), timeout=5) as stream:
            stream.sendall(request)
            stream.shutdown(socket.SHUT_WR)
            chunks: list[bytes] = []
            while True:
                block = stream.recv(64 * 1024)
                if not block:
                    break
                chunks.append(block)
        return b"".join(chunks)

    def bootstrap_session(self) -> str:
        response = self.request(
            "GET", f"/bootstrap/{self.app.bootstrap_token}"
        )
        self.assertEqual(response.status, 303)
        cookie = response.header("Set-Cookie")
        self.assertIsNotNone(cookie)
        assert cookie is not None
        return cookie.split(";", 1)[0]

    def authenticated_headers(self, cookie: str) -> list[tuple[str, str]]:
        return [("Cookie", cookie)]

    def mutation_headers(self, cookie: str) -> list[tuple[str, str]]:
        return [
            ("Cookie", cookie),
            ("Origin", self.app.origin),
            ("X-HIMR-CSRF", self.app.csrf_token),
            ("Sec-Fetch-Site", "same-origin"),
            ("Content-Type", "application/json"),
        ]

    def assert_common_security_headers(self, response: HTTPResult) -> None:
        self.assertEqual(
            response.header("Cache-Control"), "no-store, private, max-age=0"
        )
        self.assertEqual(response.header("Pragma"), "no-cache")
        self.assertEqual(response.header("Expires"), "0")
        self.assertEqual(response.header("Referrer-Policy"), "no-referrer")
        self.assertEqual(response.header("X-Content-Type-Options"), "nosniff")
        self.assertEqual(response.header("X-Frame-Options"), "DENY")
        self.assertEqual(
            response.header("Cross-Origin-Opener-Policy"), "same-origin"
        )
        self.assertEqual(
            response.header("Cross-Origin-Resource-Policy"), "same-origin"
        )
        csp = response.header("Content-Security-Policy")
        self.assertIsNotNone(csp)
        assert csp is not None
        for directive in (
            "default-src 'none'",
            "media-src 'self'",
            "connect-src 'self'",
            "frame-ancestors 'none'",
            "object-src 'none'",
        ):
            self.assertIn(directive, csp)
        self.assertNotIn("access-control-allow-origin", response.headers)
        self.assertNotIn("access-control-allow-credentials", response.headers)

    def assert_json(self, response: HTTPResult) -> dict[str, Any]:
        self.assertEqual(
            response.header("Content-Type"), "application/json; charset=utf-8"
        )
        self.assertEqual(int(response.header("Content-Length") or -1), len(response.body))
        return json.loads(response.body)

    def test_binds_only_exact_ipv4_loopback(self) -> None:
        address = self.app.server.server_address
        self.assertEqual(address[0], "127.0.0.1")
        self.assertEqual(self.app.server.address_family, socket.AF_INET)
        self.assertEqual(self.app.origin, f"http://127.0.0.1:{address[1]}")
        self.assertEqual(self.app.host_header, f"127.0.0.1:{address[1]}")
        self.assertNotIn("localhost", self.app.bootstrap_url)

    def test_one_time_bootstrap_and_protected_static_and_api_routes(self) -> None:
        bootstrap_path = f"/bootstrap/{self.app.bootstrap_token}"

        head = self.request("HEAD", bootstrap_path)
        self.assertEqual(head.status, 405)
        self.assertEqual(head.body, b"")
        self.assertTrue(self.app.bootstrap_available)

        for path in (
            self.app.prefix,
            self.app.prefix + "app.js",
            self.app.prefix + "styles.css",
            self.app.prefix + "api/bootstrap",
            self.app.prefix + f"media/{self.media.opaque_id}",
        ):
            with self.subTest(path=path):
                response = self.request("GET", path)
                self.assertEqual(response.status, 403)

        bootstrap = self.request("GET", bootstrap_path)
        self.assertEqual(bootstrap.status, 303)
        self.assertEqual(bootstrap.body, b"")
        self.assertEqual(bootstrap.header("Location"), self.app.prefix)
        self.assertEqual(bootstrap.header("Content-Length"), "0")
        self.assert_common_security_headers(bootstrap)
        set_cookie = bootstrap.header("Set-Cookie")
        self.assertIsNotNone(set_cookie)
        assert set_cookie is not None
        self.assertIn(f"Path={self.app.prefix}", set_cookie)
        self.assertIn("HttpOnly", set_cookie)
        self.assertIn("SameSite=Strict", set_cookie)
        cookie = set_cookie.split(";", 1)[0]

        expired = self.request("GET", bootstrap_path)
        self.assertEqual(expired.status, 410)
        self.assertEqual(self.assert_json(expired)["message"], "bootstrap URL has expired")

        api = self.request(
            "GET",
            self.app.prefix + "api/bootstrap",
            headers=self.authenticated_headers(cookie),
        )
        self.assertEqual(api.status, 200)
        self.assert_common_security_headers(api)
        expected = self.workspace.expected_bootstrap(
            prefix=self.app.prefix,
            csrf_token=self.app.csrf_token,
        )
        self.assertEqual(self.assert_json(api), expected)

        api_head = self.request(
            "HEAD",
            self.app.prefix + "api/bootstrap",
            headers=self.authenticated_headers(cookie),
        )
        self.assertEqual(api_head.status, 200)
        self.assertEqual(api_head.body, b"")
        self.assertEqual(api_head.header("Content-Length"), api.header("Content-Length"))

        assets = {
            self.app.prefix: ("index.html", "text/html; charset=utf-8"),
            self.app.prefix + "app.js": (
                "app.js",
                "text/javascript; charset=utf-8",
            ),
            self.app.prefix + "styles.css": (
                "styles.css",
                "text/css; charset=utf-8",
            ),
        }
        for path, (name, content_type) in assets.items():
            with self.subTest(path=path):
                asset = self.app.ui_assets[name]
                response = self.request(
                    "GET", path, headers=self.authenticated_headers(cookie)
                )
                self.assertEqual(response.status, 200)
                self.assertEqual(response.body, asset["body"])
                self.assertEqual(response.header("Content-Type"), content_type)
                self.assertEqual(
                    response.header("ETag"),
                    f'"sha256-{asset["sha256"]}"',
                )
                self.assert_common_security_headers(response)

    def test_host_path_and_query_validation_fail_closed(self) -> None:
        path = self.app.prefix + "api/bootstrap"
        host_values = [
            (f"localhost:{self.app.port}",),
            ("127.0.0.1:1",),
            (self.app.host_header, self.app.host_header),
            (),
        ]
        for values in host_values:
            with self.subTest(host_values=values):
                response = self.request("GET", path, host_values=values)
                self.assertEqual(response.status, 400)
                self.assert_common_security_headers(response)

        for invalid_path in (
            path + "?debug=1",
            path + "%3Fdebug=1",
            self.app.prefix + "%2e%2e/api/bootstrap",
            self.app.prefix + "api\\bootstrap",
        ):
            with self.subTest(path=invalid_path):
                response = self.request("GET", invalid_path)
                self.assertEqual(response.status, 400)

        double_slash = self.raw_request(
            b"GET //127.0.0.1/anything HTTP/1.1\r\n"
            + f"Host: {self.app.host_header}\r\n".encode("ascii")
            + b"Connection: close\r\n\r\n"
        )
        # Some Python versions normalize an absolute-path beginning with `//`
        # to a single slash in BaseHTTPRequestHandler before our handler sees
        # it.  Either way it must fail closed rather than reach a workspace route.
        status_line = double_slash.split(b"\r\n", 1)[0]
        self.assertIn(
            status_line.split(b" ", 2)[1],
            {b"400", b"404"},
            status_line,
        )

        not_a_route = self.request("GET", self.app.prefix + "../api/bootstrap")
        self.assertEqual(not_a_route.status, 404)

    def test_get_body_cannot_desynchronize_a_second_request(self) -> None:
        cookie = self.bootstrap_session()
        route = self.app.prefix + "api/bootstrap"
        second = (
            f"GET {route} HTTP/1.1\r\n"
            f"Host: {self.app.host_header}\r\n"
            f"Cookie: {cookie}\r\n\r\n"
        ).encode("ascii")
        request = (
            f"GET {route} HTTP/1.1\r\n"
            f"Host: {self.app.host_header}\r\n"
            f"Cookie: {cookie}\r\n"
            f"Content-Length: {len(second)}\r\n\r\n"
        ).encode("ascii") + second
        before = len(self.workspace.bootstrap_calls)
        response = self.raw_request(request)
        self.assertEqual(response.count(b"HTTP/1.1"), 1)
        self.assertTrue(response.startswith(b"HTTP/1.1 400"), response[:100])
        self.assertEqual(len(self.workspace.bootstrap_calls), before)

    def test_mutations_require_session_origin_csrf_and_same_site(self) -> None:
        cookie = self.bootstrap_session()
        route = self.app.prefix + "api/mutate"
        body = json.dumps(
            {
                "expected_revision": self.workspace.revision,
                "operation": {"operation_kind": "fixture_operation", "value": "typed"},
            }
        )
        baseline = self.mutation_headers(cookie)
        forbidden_headers = {
            "no_cookie": [row for row in baseline if row[0] != "Cookie"],
            "wrong_cookie": [
                (name, "himr_review_session=wrong") if name == "Cookie" else (name, value)
                for name, value in baseline
            ],
            "no_origin": [row for row in baseline if row[0] != "Origin"],
            "wrong_origin": [
                (name, "http://localhost") if name == "Origin" else (name, value)
                for name, value in baseline
            ],
            "no_csrf": [row for row in baseline if row[0] != "X-HIMR-CSRF"],
            "wrong_csrf": [
                (name, "wrong") if name == "X-HIMR-CSRF" else (name, value)
                for name, value in baseline
            ],
            "cross_site": [
                (name, "cross-site") if name == "Sec-Fetch-Site" else (name, value)
                for name, value in baseline
            ],
            "duplicate_origin": [*baseline, ("Origin", self.app.origin)],
            "duplicate_csrf": [*baseline, ("X-HIMR-CSRF", self.app.csrf_token)],
        }
        for name, headers in forbidden_headers.items():
            with self.subTest(name=name):
                response = self.request("POST", route, headers=headers, body=body)
                self.assertEqual(response.status, 403)
                self.assert_common_security_headers(response)
        self.assertEqual(self.workspace.operation_calls, [])

        wrong_host = self.request(
            "POST",
            route,
            headers=baseline,
            body=body,
            host_values=(f"localhost:{self.app.port}",),
        )
        self.assertEqual(wrong_host.status, 403)
        self.assertEqual(self.workspace.operation_calls, [])

    def test_typed_mutate_finalize_and_stale_revision_responses(self) -> None:
        cookie = self.bootstrap_session()
        headers = self.mutation_headers(cookie)
        operation = {"operation_kind": "fixture_operation", "value": "typed"}
        initial_revision = self.workspace.revision
        mutate_body = {
            "expected_revision": initial_revision,
            "operation": operation,
        }
        mutate = self.request(
            "POST",
            self.app.prefix + "api/mutate",
            headers=headers,
            body=json.dumps(mutate_body),
        )
        self.assertEqual(mutate.status, 200)
        self.assertEqual(self.workspace.operation_calls, [(initial_revision, operation)])
        self.assertEqual(
            self.assert_json(mutate),
            self.workspace.expected_bootstrap(
                prefix=self.app.prefix,
                csrf_token=self.app.csrf_token,
            ),
        )

        stale = self.request(
            "POST",
            self.app.prefix + "api/mutate",
            headers=headers,
            body=json.dumps(mutate_body),
        )
        self.assertEqual(stale.status, 409)
        self.assertEqual(
            self.assert_json(stale),
            {"error": "stale_revision", "current_revision": initial_revision + 1},
        )
        self.assertEqual(len(self.workspace.operation_calls), 1)

        current_revision = self.workspace.revision
        finalize_payload = {
            "expected_revision": current_revision,
            "reviewer_id": "reviewer_http_fixture",
            "direct_parent_media_reviewed": True,
            "asr_outputs_inspected": False,
            "reference_text_inspected": False,
            "selection_basis": "source_metadata_and_direct_parent_media_only",
        }
        finalize = self.request(
            "POST",
            self.app.prefix + "api/finalize",
            headers=headers,
            body=json.dumps(finalize_payload),
        )
        self.assertEqual(finalize.status, 200)
        self.assertEqual(
            self.workspace.finalize_calls,
            [(current_revision, finalize_payload)],
        )
        final_bootstrap = self.assert_json(finalize)
        self.assertEqual(final_bootstrap["state"], "completed")
        self.assertEqual(final_bootstrap["revision"], current_revision + 1)
        self.assertIsNotNone(final_bootstrap["completed_manifest_sha256"])

    def test_post_types_shape_and_content_type_fail_before_delegation(self) -> None:
        cookie = self.bootstrap_session()
        route = self.app.prefix + "api/mutate"
        headers = self.mutation_headers(cookie)
        for revision in (True, -1, "7"):
            value = {"expected_revision": revision, "operation": {}}
            with self.subTest(revision=revision):
                response = self.request(
                    "POST", route, headers=headers, body=json.dumps(value)
                )
                self.assertEqual(response.status, 400)

        duplicate = b'{"expected_revision":7,"expected_revision":7,"operation":{}}'
        response = self.request("POST", route, headers=headers, body=duplicate)
        self.assertEqual(response.status, 400)

        malformed = self.request("POST", route, headers=headers, body=b"{")
        self.assertEqual(malformed.status, 400)

        huge_integer = (
            b'{"expected_revision":'
            + b"9" * 5_000
            + b',"operation":{}}'
        )
        huge_integer_response = self.request(
            "POST", route, headers=headers, body=huge_integer
        )
        self.assertEqual(huge_integer_response.status, 400)

        extra = self.request(
            "POST",
            route,
            headers=headers,
            body=json.dumps(
                {"expected_revision": 7, "operation": {}, "unexpected": True}
            ),
        )
        self.assertEqual(extra.status, 422)

        for content_type in (None, "application/json; charset=utf-8", "text/plain"):
            filtered = [row for row in headers if row[0] != "Content-Type"]
            if content_type is not None:
                filtered.append(("Content-Type", content_type))
            with self.subTest(content_type=content_type):
                response = self.request(
                    "POST",
                    route,
                    headers=filtered,
                    body=json.dumps({"expected_revision": 7, "operation": {}}),
                )
                self.assertEqual(response.status, 415)
        self.assertEqual(self.workspace.operation_calls, [])

    def test_unsupported_methods_and_unknown_post_route(self) -> None:
        for method in ("PUT", "PATCH", "DELETE", "OPTIONS", "TRACE", "CONNECT"):
            with self.subTest(method=method):
                response = self.request(method, self.app.prefix + "api/bootstrap")
                self.assertEqual(response.status, 405)
                self.assert_common_security_headers(response)
                self.assertNotIn("allow", response.headers)

        cookie = self.bootstrap_session()
        response = self.request(
            "POST",
            self.app.prefix + "api/unknown",
            headers=self.mutation_headers(cookie),
            body=json.dumps({"expected_revision": self.workspace.revision}),
        )
        self.assertEqual(response.status, 404)
        self.assertEqual(self.workspace.operation_calls, [])
        self.assertEqual(self.workspace.finalize_calls, [])

    def assert_media_headers(
        self,
        response: HTTPResult,
        *,
        content_length: int,
        content_range: str | None,
    ) -> None:
        self.assert_common_security_headers(response)
        self.assertEqual(response.header("Accept-Ranges"), "bytes")
        self.assertEqual(response.header("Content-Type"), self.media.mime_type)
        self.assertEqual(response.header("Content-Length"), str(content_length))
        self.assertEqual(response.header("Content-Range"), content_range)
        self.assertEqual(
            response.header("ETag"), f'"sha256-{self.media.sha256}"'
        )

    def test_media_full_get_and_head_have_exact_parity(self) -> None:
        cookie = self.bootstrap_session()
        route = self.app.prefix + f"media/{self.media.opaque_id}"
        headers = self.authenticated_headers(cookie)
        get = self.request("GET", route, headers=headers)
        self.assertEqual(get.status, 200)
        self.assertEqual(get.body, self.media_body)
        self.assert_media_headers(
            get, content_length=len(self.media_body), content_range=None
        )

        head = self.request("HEAD", route, headers=headers)
        self.assertEqual(head.status, get.status)
        self.assertEqual(head.body, b"")
        self.assert_media_headers(
            head, content_length=len(self.media_body), content_range=None
        )

        unauthenticated = self.request("GET", route)
        self.assertEqual(unauthenticated.status, 403)
        missing = self.request(
            "GET",
            self.app.prefix + "media/not_the_bound_media",
            headers=headers,
        )
        self.assertEqual(missing.status, 404)

    def test_media_closed_open_suffix_and_clamped_ranges(self) -> None:
        cookie = self.bootstrap_session()
        route = self.app.prefix + f"media/{self.media.opaque_id}"
        length = len(self.media_body)
        cases = {
            "bytes=0-0": (0, 0),
            "bytes=2-5": (2, 5),
            "bytes=5-": (5, length - 1),
            "bytes=-3": (length - 3, length - 1),
            "bytes=-999": (0, length - 1),
            "bytes=0-999": (0, length - 1),
        }
        for range_value, (start, end) in cases.items():
            headers = [
                *self.authenticated_headers(cookie),
                ("Range", range_value),
            ]
            with self.subTest(range=range_value):
                get = self.request("GET", route, headers=headers)
                self.assertEqual(get.status, 206)
                self.assertEqual(get.body, self.media_body[start : end + 1])
                self.assert_media_headers(
                    get,
                    content_length=end - start + 1,
                    content_range=f"bytes {start}-{end}/{length}",
                )
                head = self.request("HEAD", route, headers=headers)
                self.assertEqual(head.status, get.status)
                self.assertEqual(head.body, b"")
                self.assert_media_headers(
                    head,
                    content_length=end - start + 1,
                    content_range=f"bytes {start}-{end}/{length}",
                )

    def test_invalid_multi_and_out_of_range_media_requests_are_416(self) -> None:
        cookie = self.bootstrap_session()
        route = self.app.prefix + f"media/{self.media.opaque_id}"
        size = len(self.media_body)
        values = (
            "bytes=",
            "items=0-1",
            "bytes=0-1,3-4",
            f"bytes={size}-",
            "bytes=8-7",
            "bytes=-0",
        )
        for value in values:
            headers = [*self.authenticated_headers(cookie), ("Range", value)]
            with self.subTest(value=value):
                get = self.request("GET", route, headers=headers)
                self.assertEqual(get.status, 416)
                self.assertEqual(get.header("Content-Range"), f"bytes */{size}")
                self.assertEqual(get.header("Accept-Ranges"), "bytes")
                body = self.assert_json(get)
                self.assertEqual(body["message"], "range not satisfiable")

                head = self.request("HEAD", route, headers=headers)
                self.assertEqual(head.status, get.status)
                self.assertEqual(head.body, b"")
                self.assertEqual(head.header("Content-Range"), get.header("Content-Range"))
                self.assertEqual(head.header("Content-Length"), get.header("Content-Length"))

        duplicate = self.request(
            "GET",
            route,
            headers=[
                *self.authenticated_headers(cookie),
                ("Range", "bytes=0-0"),
                ("Range", "bytes=1-1"),
            ],
        )
        self.assertEqual(duplicate.status, 416)
        self.assertEqual(duplicate.header("Content-Range"), f"bytes */{size}")

    def test_concurrent_byte_ranges_use_independent_exact_offsets(self) -> None:
        cookie = self.bootstrap_session()
        route = self.app.prefix + f"media/{self.media.opaque_id}"
        offsets = [index % len(self.media_body) for index in range(32)]

        def fetch(offset: int) -> tuple[int, bytes, str | None]:
            response = self.request(
                "GET",
                route,
                headers=[
                    *self.authenticated_headers(cookie),
                    ("Range", f"bytes={offset}-{offset}"),
                ],
            )
            return response.status, response.body, response.header("Content-Range")

        with concurrent.futures.ThreadPoolExecutor(max_workers=8) as executor:
            observed = list(executor.map(fetch, offsets))
        for offset, (status, body, content_range) in zip(offsets, observed):
            self.assertEqual(status, 206)
            self.assertEqual(body, self.media_body[offset : offset + 1])
            self.assertEqual(
                content_range,
                f"bytes {offset}-{offset}/{len(self.media_body)}",
            )


if __name__ == "__main__":
    unittest.main()
