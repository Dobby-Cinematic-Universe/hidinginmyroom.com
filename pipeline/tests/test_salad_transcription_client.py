from __future__ import annotations

import io
import http.client
import hashlib
import json
import os
import tempfile
import unittest
import urllib.error
import urllib.request
from pathlib import Path
from types import SimpleNamespace
from unittest import mock

from pipeline import salad_transcription_client as client


class Response(io.BytesIO):
    def __init__(self, body=b"{}", *, status=200, headers=None):
        super().__init__(body)
        self.status = status
        self.headers = headers or {}


class SaladClientTests(unittest.TestCase):
    def setUp(self):
        self.api = client.SaladClient("my-org", "secret-api-key")
        self.job_id = "123e4567-e89b-12d3-a456-426614174000"
        # Construct a synthetic query separately: this is not a retained access URL.
        self.signed_url = "https://storage-api.salad.com/organizations/my-org/files/himr/plan/chunk.wav" + "?token=private-token"

    def test_credentials_from_environment_and_configuration_validation(self):
        with mock.patch.dict(os.environ, {"SALAD_API_KEY": "environment-key"}):
            self.assertEqual(client.SaladClient("my-org")._api_key, "environment-key")
        for organization in ["x", "UPPER", "../oops", "my-org/secret", None]:
            with self.subTest(organization=organization), self.assertRaises(client.CloudClientError):
                client.SaladClient(organization, "key")
        for key in ["", "secret\r\nHeader: injected", "secret key", "secret\x00", "é"]:
            with self.subTest(key=key), self.assertRaises(client.CloudClientError) as raised:
                client.SaladClient("my-org", key)
            self.assertNotIn("injected", str(raised.exception))
        for timeout in [0, -1, 301, float("nan"), float("inf"), True, "60"]:
            with self.subTest(timeout=timeout), self.assertRaises(client.CloudClientError):
                client.SaladClient("my-org", "key", timeout)

    def test_endpoint_get_and_submit_exact_paths_auth_and_no_retries(self):
        with mock.patch.object(client, "_transport", return_value=Response()) as transport:
            self.api.endpoint("transcribe")
        request, timeout = transport.call_args.args
        self.assertEqual(request.full_url, "https://api.salad.com/api/public/organizations/my-org/inference-endpoints/transcribe")
        self.assertEqual(request.get_header("Salad-api-key"), "secret-api-key")
        self.assertEqual(request.get_method(), "GET")
        self.assertEqual(timeout, 60)
        payload = {"input": {"url": "https://media.example/source.wav"}}
        with mock.patch.object(client, "_transport", return_value=Response(status=201)) as transport:
            self.api.submit("transcription-lite", payload)
        request = transport.call_args.args[0]
        self.assertTrue(request.full_url.endswith("/transcription-lite/jobs"))
        self.assertEqual(request.get_method(), "POST")
        self.assertEqual(json.loads(request.data), payload)
        transport.assert_called_once()

    def test_job_id_validation_poll_and_cancel(self):
        for job_id in ["../other", "not-a-uuid", None, self.job_id.upper()]:
            with self.subTest(job_id=job_id), self.assertRaises(client.CloudClientError):
                self.api.get_job("transcribe", job_id)
        with mock.patch.object(client, "_transport", return_value=Response()) as transport:
            self.api.get_job("transcribe", self.job_id)
        self.assertTrue(transport.call_args.args[0].full_url.endswith("/jobs/" + self.job_id))
        with mock.patch.object(client, "_transport", return_value=Response(status=202)) as transport:
            self.assertIsNone(self.api.cancel_job("transcribe", self.job_id))
        self.assertEqual(transport.call_args.args[0].get_method(), "DELETE")

    def test_unsupported_engine_and_invalid_json_never_call_transport(self):
        with mock.patch.object(client, "_transport") as transport:
            for engine in ["other", "transcribe/../jobs", None, []]:
                with self.subTest(engine=engine), self.assertRaises(client.CloudClientError):
                    self.api.endpoint(engine)
            for payload in [[], {"number": float("nan")}, {"number": float("inf")}, {"data": object()}]:
                with self.subTest(payload=payload), self.assertRaises(client.CloudClientError):
                    self.api.submit("transcribe", payload)
        transport.assert_not_called()

    def test_transport_errors_are_sanitized_and_submission_is_ambiguous(self):
        for method in ["GET", "POST"]:
            error = urllib.error.URLError("secret-api-key https://source.test?token=private")
            with mock.patch.object(client, "_transport", side_effect=error) as transport:
                with self.assertRaises(client.CloudClientError) as raised:
                    if method == "POST":
                        self.api.submit("transcribe", {"input": {}})
                    else:
                        self.api.endpoint("transcribe")
            self.assertEqual(raised.exception.ambiguous, method == "POST")
            self.assertNotIn("secret-api-key", str(raised.exception))
            self.assertNotIn("private", str(raised.exception))
            transport.assert_called_once()

    def test_http_error_status_retry_after_and_ambiguity(self):
        for code, ambiguous in [(400, False), (401, False), (408, True), (429, False), (500, True), (302, True)]:
            error = urllib.error.HTTPError("https://private.test?token=secret", code, "sensitive body", {"Retry-After": "12"}, io.BytesIO(b"secret-api-key"))
            with mock.patch.object(client, "_transport", side_effect=error) as transport:
                with self.assertRaises(client.CloudClientError) as raised:
                    self.api.submit("transcribe", {})
            self.assertEqual(raised.exception.status_code, code)
            self.assertEqual(raised.exception.retry_after_seconds, 12)
            self.assertEqual(raised.exception.ambiguous, ambiguous)
            self.assertNotIn("sensitive", str(raised.exception))
            transport.assert_called_once()

    def test_real_transport_disables_redirects_and_environment_proxies(self):
        request = urllib.request.Request("https://api.salad.com/api/public/test")
        with mock.patch.object(client.urllib.request, "build_opener") as build:
            client._transport(request, 7)
        handlers = build.call_args.args
        self.assertEqual(handlers[0].proxies, {})
        self.assertIsInstance(handlers[1], client._NoRedirect)
        self.assertIsNone(handlers[1].redirect_request(request, None, 302, "Found", {}, "https://evil.example"))
        build.return_value.open.assert_called_once_with(request, timeout=7)

    def test_strict_json_rejects_nonfinite_duplicates_arrays_and_invalid_utf8(self):
        bodies = [b'{"x":1,"x":2}', b'{"nested":{"x":1,"x":2}}', b'{"x":NaN}', b'{"x":Infinity}', b'{"x":1e999}', b'[]', b'null', b'{"x":"\xff"}']
        for body in bodies:
            with self.subTest(body=body), mock.patch.object(client, "_transport", return_value=Response(body, status=201)):
                with self.assertRaises(client.CloudClientError) as raised:
                    self.api.submit("transcribe", {})
                self.assertTrue(raised.exception.ambiguous)

    def test_response_is_bounded_with_and_without_content_length(self):
        for headers, body in [({}, b" " * 33), ({"Content-Length": "33"}, b"{}"), ({"Content-Length": "invalid"}, b"{}"), ({"Content-Length": "12"}, b"{}")]:
            with mock.patch.object(client, "MAX_RESPONSE_BYTES", 32), mock.patch.object(client, "_transport", return_value=Response(body, headers=headers)):
                with self.assertRaises(client.CloudClientError):
                    self.api.endpoint("transcribe")

    def test_incomplete_http_transport_is_sanitized(self):
        with mock.patch.object(client, "_transport", side_effect=http.client.IncompleteRead(b"private data")):
            with self.assertRaises(client.CloudClientError) as raised:
                self.api.submit("transcribe", {})
        self.assertTrue(raised.exception.ambiguous)
        self.assertNotIn("private", str(raised.exception))

    def test_download_output_is_auth_free_and_fixed_to_signed_https_storage(self):
        output = {"text": "transcript", "duration": 0.01, "processing_time": 2}
        with mock.patch.object(client, "_transport", return_value=Response(json.dumps(output).encode())) as transport:
            self.assertEqual(self.api.download_output(self.signed_url), output)
        request = transport.call_args.args[0]
        self.assertEqual(request.full_url, self.signed_url)
        self.assertIsNone(request.get_header("Salad-api-key"))
        self.assertIsNone(request.get_header("Authorization"))
        invalid = [
            self.signed_url.replace("https://", "http://"),
            self.signed_url.replace("storage-api.salad.com", "evil.example"),
            self.signed_url.replace("storage-api.salad.com", "storage-api.salad.com:443"),
            self.signed_url.replace("storage-api.salad.com", "user@storage-api.salad.com"),
            self.signed_url + "#fragment", self.signed_url + "&token=other",
            self.signed_url.replace("?token=private-token", ""),
            self.signed_url.replace("chunk.wav", "%2e%2e/secret"),
            self.signed_url.replace("chunk.wav", "../secret"),
            self.signed_url.replace("chunk.wav", "chunk.wav\n"),
            self.signed_url.replace("private-token", "%0Asecret"),
        ]
        with mock.patch.object(client, "_transport") as transport:
            for url in invalid:
                with self.subTest(url=url), self.assertRaises(client.CloudClientError):
                    self.api.download_output(url)
        transport.assert_not_called()

    def test_storage_upload_multipart_and_signed_url_binding(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "chunk.wav"
            path.write_bytes(b"RIFF audio data")
            path.chmod(0o600)
            body = json.dumps({"url": self.signed_url}).encode()
            with mock.patch.object(client, "_transport", return_value=Response(body)) as transport:
                self.assertEqual(self.api.upload_file(path, "himr/plan/chunk.wav", expires_seconds=86400), self.signed_url)
            request = transport.call_args.args[0]
            self.assertEqual(request.get_method(), "PUT")
            self.assertEqual(request.full_url, self.signed_url.split("?")[0])
            self.assertEqual(request.get_header("Salad-api-key"), "secret-api-key")
            self.assertIn("multipart/form-data; boundary=himr-salad-", request.get_header("Content-type"))
            for field, value in [("mimeType", "audio/wav"), ("sign", "true"), ("signatureExp", "86400")]:
                self.assertIn(f'name="{field}"\r\n\r\n{value}\r\n'.encode(), request.data)
            self.assertIn(b'name="file"; filename="chunk.wav"', request.data)
            self.assertIn(b"RIFF audio data", request.data)
            for returned in [self.signed_url.replace("my-org", "other-org"), self.signed_url.replace("chunk.wav", "other.wav"), self.signed_url.replace("storage-api.salad.com", "evil.example"), None]:
                with mock.patch.object(client, "_transport", return_value=Response(json.dumps({"url": returned}).encode())):
                    with self.assertRaises(client.CloudClientError) as raised:
                        self.api.upload_file(path, "himr/plan/chunk.wav")
                self.assertTrue(raised.exception.ambiguous)

    def test_upload_refuses_unsafe_paths_without_network(self):
        with tempfile.TemporaryDirectory() as directory, mock.patch.object(client, "_transport") as transport:
            root = Path(directory)
            path = root / "chunk.wav"
            path.write_bytes(b"audio")
            path.chmod(0o600)
            symlink = root / "link.wav"
            symlink.symlink_to(path)
            for name in ["../file.wav", "/file.wav", "dir//file.wav", "file\r\nInjected", "dir/%2f.wav"]:
                with self.subTest(name=name), self.assertRaises(client.CloudClientError):
                    self.api.upload_file(path, name)
            with self.assertRaises(client.CloudClientError):
                self.api.upload_file(symlink, "file.wav")
            parent_link = root / "linked"
            parent_link.symlink_to(root, target_is_directory=True)
            with self.assertRaises(client.CloudClientError):
                self.api.upload_file(parent_link / "chunk.wav", "file.wav")
            hardlink = root / "hardlink.wav"
            os.link(path, hardlink)
            with self.assertRaises(client.CloudClientError):
                self.api.upload_file(path, "file.wav")
            hardlink.unlink()
            path.chmod(0o666)
            with self.assertRaises(client.CloudClientError):
                self.api.upload_file(path, "file.wav")
            path.chmod(0o600)
            with mock.patch.object(client, "MAX_UPLOAD_BYTES", 3), self.assertRaises(client.CloudClientError):
                self.api.upload_file(path, "file.wav")
            for expires in [0, -1, True, 30 * 86400 + 1]:
                with self.subTest(expires=expires), self.assertRaises(client.CloudClientError):
                    self.api.upload_file(path, "file.wav", expires_seconds=expires)
            transport.assert_not_called()

    def test_retry_after_invalid_and_past_date(self):
        for value in ["NaN", "inf", "invalid", "Wed, 01 Jan 2020 00:00:00"]:
            self.assertIsNone(client._retry_after({"Retry-After": value}))
        self.assertEqual(client._retry_after({"Retry-After": "Wed, 01 Jan 2020 00:00:00 GMT"}), 0)

    def test_upload_rejects_foreign_owner_empty_nonregular_and_changed_file(self):
        with tempfile.TemporaryDirectory() as directory, mock.patch.object(client, "_transport") as transport:
            path = Path(directory) / "chunk.wav"
            path.write_bytes(b"audio")
            path.chmod(0o600)
            with mock.patch.object(client.os, "getuid", return_value=os.getuid() + 1):
                with self.assertRaises(client.CloudClientError):
                    self.api.upload_file(path, "file.wav")
            before = path.stat()
            values = {key: getattr(before, key) for key in ["st_dev", "st_ino", "st_size", "st_mtime_ns", "st_ctime_ns", "st_mode", "st_uid", "st_nlink"]}
            values["st_mtime_ns"] += 1
            with mock.patch.object(client.os, "fstat", side_effect=[before, SimpleNamespace(**values)]):
                with self.assertRaises(client.CloudClientError):
                    self.api.upload_file(path, "file.wav")
            path.write_bytes(b"")
            with self.assertRaises(client.CloudClientError):
                self.api.upload_file(path, "file.wav")
            with self.assertRaises(client.CloudClientError):
                self.api.upload_file(Path(directory), "file.wav")
            transport.assert_not_called()

    def test_upload_checks_expected_digest_and_size_of_bytes_actually_sent(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "chunk.wav"
            original = b"original audio bytes"
            path.write_bytes(original)
            path.chmod(0o600)
            digest = hashlib.sha256(original).hexdigest()
            with mock.patch.object(client, "_transport", return_value=Response(json.dumps({"url": self.signed_url}).encode())) as transport:
                self.api.upload_file(path, "himr/plan/chunk.wav", expected_sha256=digest, expected_byte_count=len(original))
                self.assertIn(original, transport.call_args.args[0].data)
            with mock.patch.object(client, "_transport") as transport:
                for arguments in [
                    {"expected_sha256": "0" * 64}, {"expected_byte_count": len(original) + 1},
                    {"expected_sha256": "invalid"}, {"expected_byte_count": True},
                    {"expected_byte_count": -1}, {"expected_byte_count": 2.5},
                ]:
                    with self.subTest(arguments=arguments), self.assertRaises(client.CloudClientError):
                        self.api.upload_file(path, "himr/plan/chunk.wav", **arguments)
                # Equal-size replacement after the caller's receipt check must
                # still fail before any HTTP PUT can occur.
                path.write_bytes(b"x" * len(original))
                with self.assertRaises(client.CloudClientError):
                    self.api.upload_file(path, "himr/plan/chunk.wav", expected_sha256=digest, expected_byte_count=len(original))
                transport.assert_not_called()


if __name__ == "__main__":
    unittest.main()
