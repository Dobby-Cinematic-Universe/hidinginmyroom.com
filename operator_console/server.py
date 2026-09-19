"""Hardened same-origin loopback HTTP surface for the operator service."""

from __future__ import annotations

import hashlib
import hmac
import json
import os
import re
import secrets
import socket
import stat
import threading
import webbrowser
from http import HTTPStatus
from http.cookies import SimpleCookie
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Any, Iterable

from .service import (
    JOB_ID_RE,
    OperatorService,
    ServiceError,
    StaleRevision,
)


MAX_POST_BYTES = 64 * 1024
MAX_UI_ASSET_BYTES = 2 * 1024 * 1024
MAX_REQUEST_PATH_BYTES = 4096
SOCKET_TIMEOUT_SECONDS = 10
MAX_REQUEST_THREADS = 16
UI_ASSET_TYPES = {
    "index.html": "text/html; charset=utf-8",
    "app.js": "text/javascript; charset=utf-8",
    "styles.css": "text/css; charset=utf-8",
}
SESSION_COOKIE_NAME = "himr_operator_session"
SESSION_PREFIX_RE = re.compile(r"/o/[A-Za-z0-9_-]{32,192}/\Z")
LOG_ROUTE_RE = re.compile(
    r"(?P<prefix>/o/[A-Za-z0-9_-]{32,192}/)api/jobs/"
    r"(?P<job>job_[0-9a-f]{32})/logs/(?P<stream>stdout|stderr)/(?P<offset>0|[1-9][0-9]*)\Z"
)


def _canonical_bytes(value: Any) -> bytes:
    return json.dumps(
        value,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
        allow_nan=False,
    ).encode("utf-8")


def _strict_json_bytes(body: bytes) -> dict[str, Any]:
    def unique(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
        result: dict[str, Any] = {}
        for key, value in pairs:
            if key in result:
                raise ValueError(f"duplicate key {key!r}")
            result[key] = value
        return result

    def reject_constant(value: str) -> None:
        raise ValueError(f"non-finite number {value!r}")

    try:
        value = json.loads(
            body.decode("utf-8"),
            object_pairs_hook=unique,
            parse_constant=reject_constant,
        )
    except (UnicodeDecodeError, ValueError, RecursionError) as error:
        raise ServiceError("invalid_request", f"request body is not strict JSON: {error}", status=400) from error
    if not isinstance(value, dict):
        raise ServiceError("invalid_request", "request top level must be an object", status=400)
    return value


def _exact_object(value: dict[str, Any], keys: set[str]) -> dict[str, Any]:
    if set(value) != keys:
        raise ServiceError("invalid_request", "request has missing or unexpected fields", status=422)
    return value


def _pin_ui_assets(root: Path) -> dict[str, dict[str, Any]]:
    absolute = root.absolute()
    try:
        info = absolute.lstat()
    except OSError as error:
        raise ServiceError("ui_unavailable", f"cannot inspect UI asset root: {error}") from error
    if stat.S_ISLNK(info.st_mode) or not stat.S_ISDIR(info.st_mode):
        raise ServiceError("ui_unavailable", "UI asset root must be a non-symlink directory")
    result: dict[str, dict[str, Any]] = {}
    for name, content_type in UI_ASSET_TYPES.items():
        path = absolute / name
        flags = os.O_RDONLY | getattr(os, "O_CLOEXEC", 0) | getattr(os, "O_NONBLOCK", 0)
        flags |= getattr(os, "O_NOFOLLOW", 0)
        try:
            descriptor = os.open(path, flags)
        except OSError as error:
            raise ServiceError("ui_unavailable", f"cannot open UI asset {name}: {error}") from error
        try:
            before = os.fstat(descriptor)
            if (
                not stat.S_ISREG(before.st_mode)
                or before.st_size < 1
                or before.st_size > MAX_UI_ASSET_BYTES
                or before.st_nlink != 1
            ):
                raise ServiceError("ui_unavailable", f"UI asset {name} is not a bounded regular file")
            body = os.pread(descriptor, before.st_size + 1, 0)
            after = os.fstat(descriptor)
            named = path.lstat()
        finally:
            os.close(descriptor)
        fingerprint = lambda value: (
            value.st_dev,
            value.st_ino,
            value.st_mode,
            value.st_nlink,
            value.st_size,
            value.st_mtime_ns,
            value.st_ctime_ns,
        )
        if (
            len(body) != before.st_size
            or fingerprint(before) != fingerprint(after)
            or (named.st_dev, named.st_ino) != (before.st_dev, before.st_ino)
        ):
            raise ServiceError("ui_unavailable", f"UI asset {name} changed while being read")
        result[name] = {
            "body": body,
            "content_type": content_type,
            "sha256": hashlib.sha256(body).hexdigest(),
        }
    return result


class _BoundedLoopbackHTTPServer(ThreadingHTTPServer):
    address_family = socket.AF_INET
    allow_reuse_address = False
    daemon_threads = True

    def __init__(self, server_address: tuple[str, int], handler: type[BaseHTTPRequestHandler]):
        self._request_slots = threading.BoundedSemaphore(MAX_REQUEST_THREADS)
        super().__init__(server_address, handler)

    def get_request(self) -> tuple[socket.socket, Any]:
        request, address = super().get_request()
        request.settimeout(SOCKET_TIMEOUT_SECONDS)
        return request, address

    def process_request(self, request: socket.socket, client_address: Any) -> None:
        if not self._request_slots.acquire(blocking=False):
            request.close()
            return
        try:
            super().process_request(request, client_address)
        except Exception:
            self._request_slots.release()
            raise

    def process_request_thread(self, request: socket.socket, client_address: Any) -> None:
        try:
            super().process_request_thread(request, client_address)
        finally:
            self._request_slots.release()


class OperatorHTTP:
    """Private loopback facade with one-use bootstrap and same-origin mutations."""

    def __init__(
        self,
        service: OperatorService,
        *,
        port: int = 0,
        ui_root: Path | None = None,
    ):
        if isinstance(port, bool) or not isinstance(port, int) or not 0 <= port <= 65535:
            raise ServiceError("invalid_port", "port must be an integer from 0 through 65535")
        self.service = service
        self.bootstrap_token = secrets.token_urlsafe(32)
        self.bootstrap_available = True
        self.session_cookie = secrets.token_urlsafe(32)
        self.csrf_token = secrets.token_urlsafe(32)
        self.route_token = secrets.token_urlsafe(32)
        self.prefix = f"/o/{self.route_token}/"
        self._secret_lock = threading.Lock()
        if ui_root is None:
            ui_root = Path(__file__).resolve().parent / "ui"
        self.ui_assets = _pin_ui_assets(ui_root)
        self.server = _BoundedLoopbackHTTPServer(("127.0.0.1", port), self._handler_type())
        self.port = int(self.server.server_address[1])
        self.origin = f"http://127.0.0.1:{self.port}"
        self.host_header = f"127.0.0.1:{self.port}"
        self.bootstrap_url = f"{self.origin}/bootstrap/{self.bootstrap_token}"

    def _handler_type(self) -> type[BaseHTTPRequestHandler]:
        app = self

        class Handler(BaseHTTPRequestHandler):
            protocol_version = "HTTP/1.1"
            server_version = ""
            sys_version = ""

            def version_string(self) -> str:
                return ""

            def log_message(self, _format: str, *_args: object) -> None:
                return

            def _common_headers(self) -> None:
                self.send_header("Connection", "close")
                self.send_header("Cache-Control", "no-store, private, max-age=0")
                self.send_header("Pragma", "no-cache")
                self.send_header("Expires", "0")
                self.send_header("Referrer-Policy", "no-referrer")
                self.send_header("X-Content-Type-Options", "nosniff")
                self.send_header("X-Frame-Options", "DENY")
                self.send_header("Cross-Origin-Opener-Policy", "same-origin")
                self.send_header("Cross-Origin-Resource-Policy", "same-origin")
                self.send_header(
                    "Content-Security-Policy",
                    "default-src 'none'; script-src 'self'; style-src 'self'; "
                    "connect-src 'self'; img-src 'self'; base-uri 'none'; "
                    "form-action 'self'; frame-ancestors 'none'; object-src 'none'",
                )

            def _send_body(
                self,
                status: int,
                body: bytes,
                content_type: str,
                *,
                head: bool = False,
                extra_headers: Iterable[tuple[str, str]] = (),
            ) -> None:
                self.close_connection = True
                self.send_response(status)
                self._common_headers()
                self.send_header("Content-Type", content_type)
                self.send_header("Content-Length", str(len(body)))
                for name, value in extra_headers:
                    self.send_header(name, value)
                self.end_headers()
                if not head and body:
                    self.wfile.write(body)

            def _json(self, status: int, value: dict[str, Any], *, head: bool = False) -> None:
                self._send_body(
                    status,
                    _canonical_bytes(value),
                    "application/json; charset=utf-8",
                    head=head,
                )

            def _safe_error(
                self,
                status: int,
                *,
                code: str = "request_rejected",
                message: str = "request rejected",
            ) -> None:
                if "/" in message or "\\" in message:
                    message = "request failed a private-path or command binding"
                self._json(
                    status,
                    {"error": code, "message": message[:300]},
                    head=self.command == "HEAD",
                )

            def send_error(
                self,
                code: int,
                message: str | None = None,
                explain: str | None = None,
            ) -> None:
                del message, explain
                self._safe_error(code)

            def _valid_host(self) -> bool:
                values = self.headers.get_all("Host", failobj=[])
                return len(values) == 1 and hmac.compare_digest(values[0], app.host_header)

            def _cookie_authenticated(self) -> bool:
                values = self.headers.get_all("Cookie", failobj=[])
                if len(values) != 1:
                    return False
                raw = values[0]
                names = [part.split("=", 1)[0].strip() for part in raw.split(";") if "=" in part]
                if names.count(SESSION_COOKIE_NAME) != 1:
                    return False
                try:
                    cookie = SimpleCookie()
                    cookie.load(raw)
                except Exception:
                    return False
                morsel = cookie.get(SESSION_COOKIE_NAME)
                return morsel is not None and hmac.compare_digest(
                    morsel.value, app.session_cookie
                )

            def _authenticated(self) -> bool:
                return self._valid_host() and self._cookie_authenticated()

            def _valid_mutation_headers(self) -> bool:
                if not self._authenticated():
                    return False
                origins = self.headers.get_all("Origin", failobj=[])
                csrf = self.headers.get_all("X-HIMR-CSRF", failobj=[])
                fetch_sites = self.headers.get_all("Sec-Fetch-Site", failobj=[])
                if len(origins) != 1 or not hmac.compare_digest(origins[0], app.origin):
                    return False
                if len(csrf) != 1 or not hmac.compare_digest(csrf[0], app.csrf_token):
                    return False
                if fetch_sites and (
                    len(fetch_sites) != 1 or fetch_sites[0] != "same-origin"
                ):
                    return False
                return True

            def _request_path(self) -> str | None:
                try:
                    encoded_length = len(self.path.encode("utf-8"))
                except UnicodeError:
                    return None
                if (
                    encoded_length > MAX_REQUEST_PATH_BYTES
                    or not self.path.startswith("/")
                    or self.path.startswith("//")
                    or any(character in self.path for character in ("?", "#", "%", "\\"))
                ):
                    return None
                return self.path

            def _valid_bodyless_framing(self) -> bool:
                if self.headers.get_all("Transfer-Encoding", failobj=[]):
                    return False
                lengths = self.headers.get_all("Content-Length", failobj=[])
                if not lengths:
                    return True
                if (
                    len(lengths) != 1
                    or len(lengths[0]) > 20
                    or not lengths[0].isascii()
                    or not lengths[0].isdigit()
                ):
                    return False
                return int(lengths[0]) == 0

            def _read_json_body(self) -> dict[str, Any] | None:
                if self.headers.get_all("Transfer-Encoding", failobj=[]):
                    self._safe_error(HTTPStatus.BAD_REQUEST)
                    return None
                content_types = self.headers.get_all("Content-Type", failobj=[])
                lengths = self.headers.get_all("Content-Length", failobj=[])
                if content_types != ["application/json"] or len(lengths) != 1:
                    self._safe_error(HTTPStatus.UNSUPPORTED_MEDIA_TYPE)
                    return None
                value = lengths[0]
                if (
                    len(value) > 20
                    or not value.isascii()
                    or not value.isdigit()
                ):
                    self._safe_error(HTTPStatus.BAD_REQUEST)
                    return None
                length = int(value)
                if length <= 0 or length > MAX_POST_BYTES:
                    self._safe_error(HTTPStatus.REQUEST_ENTITY_TOO_LARGE)
                    return None
                body = self.rfile.read(length)
                if len(body) != length:
                    self._safe_error(HTTPStatus.BAD_REQUEST)
                    return None
                try:
                    return _strict_json_bytes(body)
                except ServiceError as error:
                    self._service_error(error)
                    return None

            def _service_error(self, error: ServiceError) -> None:
                extra: dict[str, Any] = {}
                if isinstance(error, StaleRevision):
                    extra["current_revision"] = error.current_revision
                message = str(error)
                if "/" in message or "\\" in message:
                    message = "request failed a private-path or command binding"
                self._json(
                    error.status,
                    {"error": error.code, "message": message[:300], **extra},
                    head=self.command == "HEAD",
                )

            def _bootstrap(self, path: str, *, head: bool) -> bool:
                expected = f"/bootstrap/{app.bootstrap_token}"
                if path != expected:
                    return False
                if head:
                    self._safe_error(HTTPStatus.METHOD_NOT_ALLOWED)
                    return True
                with app._secret_lock:
                    if not app.bootstrap_available:
                        self._safe_error(
                            HTTPStatus.GONE,
                            code="bootstrap_expired",
                            message="bootstrap URL has expired",
                        )
                        return True
                    app.bootstrap_available = False
                self.send_response(HTTPStatus.SEE_OTHER)
                self._common_headers()
                self.send_header("Content-Length", "0")
                self.send_header("Location", app.prefix)
                self.send_header(
                    "Set-Cookie",
                    f"{SESSION_COOKIE_NAME}={app.session_cookie}; Path={app.prefix}; "
                    "HttpOnly; SameSite=Strict",
                )
                self.end_headers()
                return True

            def _static(self, path: str, *, head: bool) -> bool:
                routes = {
                    app.prefix: "index.html",
                    app.prefix + "app.js": "app.js",
                    app.prefix + "styles.css": "styles.css",
                }
                name = routes.get(path)
                if name is None:
                    return False
                if not self._authenticated():
                    self._safe_error(HTTPStatus.FORBIDDEN)
                    return True
                asset = app.ui_assets[name]
                self._send_body(
                    HTTPStatus.OK,
                    asset["body"],
                    asset["content_type"],
                    head=head,
                    extra_headers=(("ETag", f'"sha256-{asset["sha256"]}"'),),
                )
                return True

            def _api_get(self, path: str, *, head: bool) -> bool:
                if path == app.prefix + "api/state":
                    if not self._authenticated():
                        self._safe_error(HTTPStatus.FORBIDDEN)
                        return True
                    self._json(
                        HTTPStatus.OK,
                        app.service.public_state(
                            csrf_token=app.csrf_token, prefix=app.prefix
                        ),
                        head=head,
                    )
                    return True
                match = LOG_ROUTE_RE.fullmatch(path)
                if match is None or match.group("prefix") != app.prefix:
                    return False
                if not self._authenticated():
                    self._safe_error(HTTPStatus.FORBIDDEN)
                    return True
                try:
                    offset = int(match.group("offset"))
                    value = app.service.read_log_chunk(
                        match.group("job"), match.group("stream"), offset
                    )
                except ServiceError as error:
                    self._service_error(error)
                    return True
                self._json(HTTPStatus.OK, value, head=head)
                return True

            def do_GET(self) -> None:
                self._handle_get(head=False)

            def do_HEAD(self) -> None:
                self._handle_get(head=True)

            def _handle_get(self, *, head: bool) -> None:
                path = self._request_path()
                if (
                    path is None
                    or not self._valid_host()
                    or not self._valid_bodyless_framing()
                ):
                    self._safe_error(HTTPStatus.BAD_REQUEST)
                    return
                if self._bootstrap(path, head=head):
                    return
                if self._static(path, head=head):
                    return
                if self._api_get(path, head=head):
                    return
                self._safe_error(HTTPStatus.NOT_FOUND)

            def do_POST(self) -> None:
                path = self._request_path()
                if path is None or not self._valid_mutation_headers():
                    self._safe_error(HTTPStatus.FORBIDDEN)
                    return
                routes = {
                    app.prefix + "api/prepare": "prepare",
                    app.prefix + "api/execute": "execute",
                    app.prefix + "api/cancel": "cancel",
                }
                operation = routes.get(path)
                if operation is None:
                    self._safe_error(HTTPStatus.NOT_FOUND)
                    return
                body = self._read_json_body()
                if body is None:
                    return
                try:
                    if operation == "prepare":
                        row = _exact_object(body, {"profile_id", "expected_revision"})
                        prepared = app.service.prepare(
                            profile_id=row["profile_id"],
                            expected_revision=row["expected_revision"],
                        )
                        self._json(
                            HTTPStatus.OK,
                            {
                                "state": app.service.public_state(
                                    csrf_token=app.csrf_token, prefix=app.prefix
                                ),
                                "prepared": prepared,
                            },
                        )
                    elif operation == "execute":
                        row = _exact_object(
                            body,
                            {"preparation_token", "expected_revision", "confirmation"},
                        )
                        job = app.service.execute(
                            preparation_token=row["preparation_token"],
                            expected_revision=row["expected_revision"],
                            confirmation=row["confirmation"],
                        )
                        current = app.service.public_job(job["job_id"], prefix=app.prefix)
                        self._json(
                            HTTPStatus.ACCEPTED,
                            {
                                "state": app.service.public_state(
                                    csrf_token=app.csrf_token, prefix=app.prefix
                                ),
                                "job": current,
                            },
                        )
                    else:
                        row = _exact_object(
                            body, {"job_id", "expected_revision", "confirmation"}
                        )
                        job = app.service.cancel(
                            job_id=row["job_id"],
                            expected_revision=row["expected_revision"],
                            confirmation=row["confirmation"],
                        )
                        current = app.service.public_job(
                            job["job_id"], prefix=app.prefix
                        )
                        self._json(
                            HTTPStatus.ACCEPTED,
                            {
                                "state": app.service.public_state(
                                    csrf_token=app.csrf_token, prefix=app.prefix
                                ),
                                "job": current,
                            },
                        )
                except ServiceError as error:
                    self._service_error(error)

            def _method_not_allowed(self) -> None:
                self._safe_error(HTTPStatus.METHOD_NOT_ALLOWED)

            do_PUT = _method_not_allowed
            do_PATCH = _method_not_allowed
            do_DELETE = _method_not_allowed
            do_OPTIONS = _method_not_allowed
            do_TRACE = _method_not_allowed
            do_CONNECT = _method_not_allowed

        return Handler

    def serve_forever(self, *, open_browser: bool) -> None:
        if open_browser:
            webbrowser.open(self.bootstrap_url, new=1, autoraise=True)
        try:
            self.server.serve_forever(poll_interval=0.25)
        finally:
            self.server.server_close()

    def close(self) -> None:
        self.server.server_close()
