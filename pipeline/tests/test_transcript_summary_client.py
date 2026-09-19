from __future__ import annotations

from copy import deepcopy
import http.client
import io
import json
import os
import unittest
import urllib.error
import urllib.parse
import urllib.request
from unittest import mock

from pipeline import transcript_summary_client as client


def encoded(value):
    return json.dumps(value, separators=(",", ":")).encode()


class Response(io.BytesIO):
    def __init__(self, value=None, *, raw=None, status=200, headers=None):
        super().__init__(encoded({} if value is None else value) if raw is None else raw)
        self.status = status
        self.headers = headers or {}


def batch(**updates):
    result = {"id": "batch_abc123", "object": "batch", "endpoint": "/v1/responses",
              "input_file_id": "file-abc123", "completion_window": "24h",
              "created_at": 1770000000, "status": "validating",
              "output_file_id": None, "error_file_id": None, "metadata": {"plan": "plan_1"}}
    result.update(updates)
    return result


def file_object(**updates):
    result = {"id": "file-abc123", "object": "file", "bytes": 123,
              "created_at": 1770000000, "expires_at": 1770604800,
              "filename": "himr-summaries.jsonl", "purpose": "batch"}
    result.update(updates)
    return result


def operation(**updates):
    result = {"name": "batches/abc123", "metadata": {
        "@type": "type.googleapis.com/google.ai.generativelanguage.v1beta.GenerateContentBatch",
        "name": "batches/abc123", "model": "models/gemini-3.8-flash",
        "displayName": "wave_1", "state": "BATCH_STATE_PENDING",
    }}
    result.update(updates)
    return result


class SharedTransportTests(unittest.TestCase):
    def test_key_validation_without_environment_discovery(self):
        with mock.patch.dict(os.environ, {"OPENAI_API_KEY": "environment-secret", "GEMINI_API_KEY": "environment-secret"}):
            for kind in (client.OpenAIBatchClient, client.GeminiBatchClient):
                for key in (None, "", "a b", "a\rb", "a\nb", "a\x00b", "é", "a" * 4097):
                    with self.subTest(kind=kind, key=key), self.assertRaises(client.BatchClientError) as error:
                        kind(key)
                    self.assertNotIn("environment-secret", str(error.exception))
                self.assertEqual(kind("test-key")._api_key, "test-key")

    def test_timeout_and_transport_validation(self):
        for kind in (client.OpenAIBatchClient, client.GeminiBatchClient):
            for timeout in (0, -1, 301, 10**1000, True, "1", float("nan"), float("inf")):
                with self.subTest(kind=kind, timeout=timeout), self.assertRaises(client.BatchClientError):
                    kind("key", timeout)
            with self.assertRaises(client.BatchClientError):
                kind("key", transport=123)

    def test_per_request_timeout_override_is_validated_before_transport(self):
        transport = mock.Mock()
        api = client.OpenAIBatchClient("key", transport=transport)
        for timeout in (0, -1, 301, 10**1000, True, "private-value", float("nan"), float("inf")):
            with self.subTest(timeout=timeout), self.assertRaises(client.BatchClientError) as error:
                api._request("POST", "/v1/batches", timeout_seconds=timeout)
            self.assertFalse(error.exception.ambiguous)
            self.assertNotIn("private-value", str(error.exception))
            self.assertEqual(api.timeout_seconds, 60)
        transport.assert_not_called()

    def test_per_request_timeout_override_controls_deadline_without_mutation(self):
        transport = mock.Mock(return_value=Response())
        api = client.OpenAIBatchClient("key", transport=transport)
        with mock.patch.object(client.time, "monotonic", side_effect=[0, 8]):
            with self.assertRaisesRegex(client.BatchClientError, "deadline exceeded") as error:
                api._request("POST", "/v1/batches", timeout_seconds=7)
        self.assertTrue(error.exception.ambiguous)
        self.assertEqual(transport.call_args.args[1], 7)
        self.assertEqual(api.timeout_seconds, 60)
        transport.assert_called_once()

    def test_builtin_transport_disables_proxy_and_redirects(self):
        request = urllib.request.Request("https://api.openai.com/v1/batches")
        with mock.patch.object(client.urllib.request, "build_opener") as build:
            client._transport(request, 7)
        handlers = build.call_args.args
        self.assertEqual(handlers[0].proxies, {})
        self.assertIsInstance(handlers[1], client._NoRedirect)
        self.assertIsNone(handlers[1].redirect_request(request, None, 302, "Found", {}, "https://elsewhere.invalid"))
        build.return_value.open.assert_called_once_with(request, timeout=7)

    def calls(self, api):
        if isinstance(api, client.GeminiBatchClient):
            return [lambda: api.get_batch("batches/abc123"),
                    lambda: api.create_batch("gemini-3.8-flash", [{"key": "job_1", "request": {"contents": [{"parts": [{"text": "content"}]}]}}], "wave_1")]
        return [lambda: api.get_batch("batch_abc123"),
                lambda: api.create_batch("file-abc123", {"plan": "plan_1"})]

    def test_transport_failure_is_sanitized_ambiguous_only_for_mutations(self):
        for kind in (client.OpenAIBatchClient, client.GeminiBatchClient):
            for index in (0, 1):
                for failure in (urllib.error.URLError("secret-token"), OSError("secret-token"),
                                http.client.IncompleteRead(b"private text")):
                    transport = mock.Mock(side_effect=failure)
                    api = kind("secret-token", transport=transport)
                    with self.assertRaises(client.BatchClientError) as error:
                        self.calls(api)[index]()
                    self.assertEqual(error.exception.ambiguous, bool(index))
                    self.assertNotIn("secret-token", str(error.exception))
                    self.assertNotIn("private text", str(error.exception))
                    transport.assert_called_once()

    def test_http_errors_never_expose_body_or_retry_automatically(self):
        for kind in (client.OpenAIBatchClient, client.GeminiBatchClient):
            for status, ambiguous in ((400, False), (401, False), (408, True), (429, False), (500, True), (302, True)):
                body = io.BytesIO(b"private content secret-token")
                failure = urllib.error.HTTPError("https://secret-token.invalid", status, "private reason", {"Retry-After": "12"}, body)
                transport = mock.Mock(side_effect=failure)
                api = kind("secret-token", transport=transport)
                with self.assertRaises(client.BatchClientError) as error:
                    self.calls(api)[1]()
                self.assertEqual(error.exception.status_code, status)
                self.assertEqual(error.exception.ambiguous, ambiguous)
                self.assertEqual(error.exception.retry_after_seconds, 12)
                self.assertNotIn("private", str(error.exception))
                self.assertTrue(body.closed)
                transport.assert_called_once()

    def test_retry_after_dates_and_nonfinite(self):
        for value in (None, "nonsense", "NaN", "Infinity", "-Infinity", "x" * 129):
            self.assertIsNone(client._retry_after({"Retry-After": value}))
        self.assertEqual(client._retry_after({"Retry-After": "-1"}), 0)
        self.assertGreater(client._retry_after({"Retry-After": "Thu, 01 Jan 2099 00:00:00 GMT"}), 0)

    def test_success_with_wrong_status_remains_ambiguous(self):
        for kind in (client.OpenAIBatchClient, client.GeminiBatchClient):
            for status in (201, 204, 301, 500, True):
                api = kind("key", transport=mock.Mock(return_value=Response(status=status)))
                with self.assertRaises(client.BatchClientError) as error:
                    self.calls(api)[1]()
                self.assertTrue(error.exception.ambiguous)

    def test_json_parse_rejects_duplicate_nonfinite_and_nonobject(self):
        bodies = (b'{"a":1,"a":2}', b'{"x":{"a":1,"a":2}}', b'{"x":NaN}',
                  b'{"x":Infinity}', b'{"x":1e999}', b'[]', b'null', b'{"x":"\xff"}')
        for kind in (client.OpenAIBatchClient, client.GeminiBatchClient):
            for body in bodies:
                api = kind("key", transport=mock.Mock(return_value=Response(raw=body)))
                with self.assertRaises(client.BatchClientError) as error:
                    self.calls(api)[1]()
                self.assertTrue(error.exception.ambiguous)

    def test_response_bounds_and_declared_lengths(self):
        fixtures = (({}, b" " * 33), ({"Content-Length": "33"}, b"{}"),
                    ({"Content-Length": "invalid"}, b"{}"), ({"Content-Length": "-2"}, b"{}"),
                    ({"Content-Length": "4"}, b"{}"), ({"Content-Length": "0"}, b"{}"),
                    ({"Content-Encoding": "gzip"}, b"{}"))
        for kind in (client.OpenAIBatchClient, client.GeminiBatchClient):
            for headers, body in fixtures:
                api = kind("key", transport=mock.Mock(return_value=Response(raw=body, headers=headers)))
                with mock.patch.object(client, "MAX_JSON_BYTES", 32), mock.patch.object(client, "MAX_RESPONSE_BYTES", 32):
                    with self.assertRaises(client.BatchClientError) as error:
                        self.calls(api)[1]()
                    self.assertTrue(error.exception.ambiguous)

    def test_destination_defense_in_depth(self):
        for kind in (client.OpenAIBatchClient, client.GeminiBatchClient):
            transport = mock.Mock()
            api = kind("key", transport=transport)
            for path in ("https://evil.invalid", "/v1/files/../secret", "/v1/files/file-id?key=secret",
                         "//evil.invalid/v1/files", "/v1/files\r\nX:y", "/v1beta/batches?key=secret"):
                with self.subTest(kind=kind, path=path), self.assertRaises(client.BatchClientError):
                    api._request("GET", path)
            transport.assert_not_called()

    def test_changed_final_url_is_rejected(self):
        for kind in (client.OpenAIBatchClient, client.GeminiBatchClient):
            response = Response()
            response.geturl = lambda: "https://evil.invalid/"
            api = kind("key", transport=mock.Mock(return_value=response))
            with self.assertRaises(client.BatchClientError) as error:
                self.calls(api)[1]()
            self.assertTrue(error.exception.ambiguous)

    def test_absolute_read_deadline(self):
        api = client.OpenAIBatchClient("key", transport=mock.Mock(return_value=Response(batch())))
        with mock.patch.object(client.time, "monotonic", side_effect=[0, 61]):
            with self.assertRaises(client.BatchClientError) as error:
                api.create_batch("file-abc123", {"plan": "plan_1"})
        self.assertTrue(error.exception.ambiguous)


class OpenAIClientTests(unittest.TestCase):
    def setUp(self):
        self.data = encoded({"custom_id": "job_1", "method": "POST", "url": "/v1/responses", "body": {"model": "gpt-5.4-mini-2026-03-17", "input": "source"}}) + b"\n"

    def test_upload_has_exact_fields_private_filename_and_expiration(self):
        transport = mock.Mock(return_value=Response(file_object(bytes=len(self.data))))
        api = client.OpenAIBatchClient("key", transport=transport)
        self.assertEqual(api.upload_batch(self.data)["id"], "file-abc123")
        request, timeout = transport.call_args.args
        self.assertEqual(request.full_url, "https://api.openai.com/v1/files")
        self.assertEqual(request.get_method(), "POST")
        self.assertEqual(request.get_header("Authorization"), "Bearer key")
        self.assertEqual(timeout, 60)
        self.assertIn('boundary=himr-openai-', request.get_header("Content-type"))
        self.assertIn(b'name="purpose"\r\n\r\nbatch\r\n', request.data)
        self.assertIn(b'name="expires_after[anchor]"\r\n\r\ncreated_at\r\n', request.data)
        self.assertIn(b'name="expires_after[seconds]"\r\n\r\n604800\r\n', request.data)
        self.assertIn(b'filename="himr-summaries.jsonl"', request.data)
        self.assertIn(self.data, request.data)
        self.assertIsInstance(request.data, bytes)
        transport.assert_called_once()

    def test_upload_jsonl_validation_is_before_network(self):
        transport = mock.Mock()
        api = client.OpenAIBatchClient("key", transport=transport)
        invalid = [None, "text", b"", self.data[:-1], b"\n", self.data + b"\n", self.data * 2,
                   b'[]\n', b'{"custom_id":"one","custom_id":"two"}\n',
                   self.data.replace(b"/v1/responses", b"/v1/chat/completions"),
                   self.data.replace(b'"POST"', b'"GET"'), self.data.replace(b'"job_1"', b'"../job"')]
        for value in invalid:
            with self.subTest(value=value), self.assertRaises(client.BatchClientError) as error:
                api.upload_batch(value)
            self.assertFalse(error.exception.ambiguous)
        with mock.patch.object(client, "MAX_UPLOAD_BYTES", 1), self.assertRaises(client.BatchClientError):
            api.upload_batch(self.data)
        with mock.patch.object(client, "MAX_BATCH_REQUESTS", 0), self.assertRaises(client.BatchClientError):
            api.upload_batch(self.data)
        transport.assert_not_called()

    def test_upload_rejects_response_file_purpose_or_size_mismatch(self):
        for value in (file_object(bytes=0), file_object(bytes=len(self.data), purpose="assistants"),
                      file_object(bytes=len(self.data), id="../file"), {"id": "file-abc123"}):
            api = client.OpenAIBatchClient("key", transport=mock.Mock(return_value=Response(value)))
            with self.assertRaises(client.BatchClientError) as error:
                api.upload_batch(self.data)
            self.assertTrue(error.exception.ambiguous)

    def test_create_batch_request_and_receipt_binding(self):
        transport = mock.Mock(return_value=Response(batch()))
        api = client.OpenAIBatchClient("key", transport=transport)
        self.assertEqual(api.create_batch("file-abc123", {"plan": "plan_1"}), batch())
        request = transport.call_args.args[0]
        self.assertEqual(request.full_url, "https://api.openai.com/v1/batches")
        self.assertEqual(json.loads(request.data), {
            "input_file_id": "file-abc123", "endpoint": "/v1/responses", "completion_window": "24h",
            "metadata": {"plan": "plan_1"}, "output_expires_after": {"anchor": "created_at", "seconds": 604800}})
        self.assertIsNone(request.get_header("Idempotency-key"))

    def test_create_mismatched_response_is_ambiguous(self):
        for value in (batch(metadata={}), batch(input_file_id="file-other"),
                      batch(endpoint="/v1/chat/completions"), batch(status="surprise"), batch(id="bad")):
            api = client.OpenAIBatchClient("key", transport=mock.Mock(return_value=Response(value)))
            with self.assertRaises(client.BatchClientError) as error:
                api.create_batch("file-abc123", {"plan": "plan_1"})
            self.assertTrue(error.exception.ambiguous)

    def test_file_and_batch_ids_cannot_inject_paths(self):
        transport = mock.Mock()
        api = client.OpenAIBatchClient("key", transport=transport)
        for value in (None, [], "", "../evil", "file-abc/../foo", "file-abc?key=secret", "batch_abc\n", "é"):
            for method in (api.get_batch, api.get_file, api.download_file, api.delete_file):
                with self.subTest(method=method, value=value), self.assertRaises(client.BatchClientError):
                    method(value)
        transport.assert_not_called()

    def test_metadata_bounds_before_create(self):
        transport = mock.Mock()
        api = client.OpenAIBatchClient("key", transport=transport)
        for value in (None, [], {"": "value"}, {"key": 1}, {"key": "x" * 513}, {"x" * 65: "v"},
                      {str(index): "v" for index in range(17)}, {"key": "\n"}, {"key": "\ud800"}):
            with self.subTest(value=value), self.assertRaises(client.BatchClientError):
                api.create_batch("file-abc123", value)
        transport.assert_not_called()

    def test_poll_get_file_download_and_explicit_delete(self):
        payload = b'{"custom_id":"job_1"}\n'
        transport = mock.Mock(side_effect=[Response(batch(status="completed")), Response(file_object()),
                                         Response(raw=payload), Response({"id": "file-abc123", "object": "file", "deleted": True})])
        api = client.OpenAIBatchClient("key", transport=transport)
        self.assertEqual(api.get_batch("batch_abc123")["status"], "completed")
        self.assertEqual(api.get_file("file-abc123"), file_object())
        self.assertEqual(api.download_file("file-abc123"), payload)
        self.assertTrue(api.delete_file("file-abc123")["deleted"])
        self.assertEqual([call.args[0].get_method() for call in transport.call_args_list], ["GET", "GET", "GET", "DELETE"])
        self.assertEqual(transport.call_args_list[2].args[0].full_url, "https://api.openai.com/v1/files/file-abc123/content")

    def test_get_and_delete_response_ids_must_match(self):
        for method, response in (("get_batch", batch(id="batch_other")), ("get_file", file_object(id="file-other")),
                                 ("delete_file", {"id": "file-other", "object": "file", "deleted": True})):
            api = client.OpenAIBatchClient("key", transport=mock.Mock(return_value=Response(response)))
            with self.assertRaises(client.BatchClientError) as error:
                getattr(api, method)("batch_abc123" if method == "get_batch" else "file-abc123")
            self.assertEqual(error.exception.ambiguous, method == "delete_file")

    def test_delete_does_not_accept_false_or_truthy_nonboolean(self):
        for deleted in (False, 1, "true", None):
            api = client.OpenAIBatchClient("key", transport=mock.Mock(return_value=Response({"id": "file-abc123", "object": "file", "deleted": deleted})))
            with self.assertRaises(client.BatchClientError) as error:
                api.delete_file("file-abc123")
            self.assertTrue(error.exception.ambiguous)

    def test_paginated_list_keeps_unrelated_batches_for_caller_filter(self):
        value = {"object": "list", "data": [batch(endpoint="/v1/chat/completions")], "has_more": True,
                 "first_id": "batch_abc123", "last_id": "batch_abc123"}
        transport = mock.Mock(return_value=Response(value))
        api = client.OpenAIBatchClient("key", transport=transport)
        self.assertEqual(api.list_batches(after="batch_previous", limit=20), value)
        self.assertEqual(transport.call_args.args[0].full_url, "https://api.openai.com/v1/batches?limit=20&after=batch_previous")
        transport.assert_called_once()

    def test_empty_batch_list(self):
        value = {"object": "list", "data": [], "has_more": False, "first_id": None, "last_id": None}
        api = client.OpenAIBatchClient("key", transport=mock.Mock(return_value=Response(value)))
        self.assertEqual(api.list_batches(), value)

    def test_bad_page_counts_cursors_and_duplicates(self):
        value = {"object": "list", "data": [batch()], "has_more": True, "first_id": "batch_abc123", "last_id": "batch_abc123"}
        cases = [{**value, "has_more": 1}, {**value, "last_id": "batch_wrong"},
                 {**value, "data": []}, {**value, "data": [batch(), batch()]}, {**value, "data": [None]}]
        for item in cases:
            api = client.OpenAIBatchClient("key", transport=mock.Mock(return_value=Response(item)))
            with self.assertRaises(client.BatchClientError):
                api.list_batches()
        for counts in ({"total": 1, "completed": 2, "failed": 0}, {"total": True, "completed": 0, "failed": 0}, []):
            api = client.OpenAIBatchClient("key", transport=mock.Mock(return_value=Response(batch(request_counts=counts))))
            with self.assertRaises(client.BatchClientError):
                api.get_batch("batch_abc123")


class GeminiClientTests(unittest.TestCase):
    def setUp(self):
        self.request = {"contents": [{"role": "user", "parts": [{"text": "transcript"}]}],
                        "systemInstruction": {"parts": [{"text": "summarize source only"}]},
                        "generationConfig": {"responseMimeType": "application/json", "maxOutputTokens": 2000},
                        "store": False}
        self.rows = [{"key": "job_1", "request": self.request}]

    def test_create_only_timeout_leaves_reentrant_get_timeout_unchanged(self):
        for configured_timeout in (60, 7):
            calls = []
            def transport(request, timeout):
                calls.append((request.get_method(), timeout))
                self.assertEqual(api.timeout_seconds, configured_timeout)
                if request.get_method() == "POST":
                    api.get_batch("batches/abc123")
                return Response(operation())
            api = client.GeminiBatchClient("key", configured_timeout, transport=transport)
            with self.subTest(configured_timeout=configured_timeout):
                api.create_batch("gemini-3.8-flash", self.rows, "wave_1")
                self.assertEqual(calls, [("POST", 180), ("GET", configured_timeout)])
                self.assertEqual(api.timeout_seconds, configured_timeout)

    def test_create_failure_does_not_change_get_timeout_or_retry(self):
        for failure in (TimeoutError("private timeout details"), Response(raw=b"not JSON"),
                        Response({"name": "invalid"})):
            transport = mock.Mock(side_effect=[failure, Response(operation())])
            api = client.GeminiBatchClient("key", transport=transport)
            with self.subTest(failure_type=type(failure).__name__):
                with self.assertRaises(client.BatchClientError) as error:
                    api.create_batch("gemini-3.8-flash", self.rows, "wave_1")
                self.assertTrue(error.exception.ambiguous)
                self.assertNotIn("private timeout details", str(error.exception))
                transport.assert_called_once()
                self.assertEqual(api.timeout_seconds, 60)
                api.get_batch("batches/abc123")
                self.assertEqual([call.args[1] for call in transport.call_args_list], [180, 60])

    def test_create_deadline_is_extended_but_still_bounded(self):
        transport = mock.Mock(side_effect=[Response(operation()), Response(operation())])
        api = client.GeminiBatchClient("key", transport=transport)
        with mock.patch.object(client.time, "monotonic", side_effect=[0, 61, 62]):
            self.assertEqual(api.create_batch("gemini-3.8-flash", self.rows, "wave_1"), operation())
        with mock.patch.object(client.time, "monotonic", side_effect=[0, 181]):
            with self.assertRaisesRegex(client.BatchClientError, "deadline exceeded") as error:
                api.create_batch("gemini-3.8-flash", self.rows, "wave_1")
        self.assertTrue(error.exception.ambiguous)
        self.assertEqual(transport.call_count, 2)
        self.assertEqual(api.timeout_seconds, 60)

    def test_inline_creation_exact_envelope_header_and_no_bearer(self):
        transport = mock.Mock(return_value=Response(operation()))
        api = client.GeminiBatchClient("gemini-key", transport=transport)
        self.assertEqual(api.create_batch("gemini-3.8-flash", self.rows, "wave_1"), operation())
        request = transport.call_args.args[0]
        self.assertEqual(request.full_url, "https://generativelanguage.googleapis.com/v1beta/models/gemini-3.8-flash:batchGenerateContent")
        self.assertEqual(request.get_header("X-goog-api-key"), "gemini-key")
        self.assertIsNone(request.get_header("Authorization"))
        self.assertNotIn("gemini-key", request.full_url)
        self.assertEqual(json.loads(request.data), {"batch": {"display_name": "wave_1", "input_config": {"requests": {"requests": [{"request": self.request, "metadata": {"key": "job_1"}}]}}}})
        self.assertEqual(request.data, client.gemini_batch_bytes("gemini-3.8-flash", self.rows, "wave_1"))
        transport.assert_called_once()

    def test_offline_preflight_counts_the_complete_post_envelope(self):
        with mock.patch.object(client, "_transport") as transport:
            raw_rows = encoded(self.rows)
            complete = client.gemini_batch_bytes("gemini-3.8-flash", self.rows, "wave_1")
            self.assertGreater(len(complete), len(raw_rows))
            with mock.patch.object(client, "MAX_GEMINI_INLINE_BYTES", len(raw_rows)):
                with self.assertRaises(client.BatchClientError) as error:
                    client.gemini_batch_bytes("gemini-3.8-flash", self.rows, "wave_1")
                self.assertFalse(error.exception.ambiguous)
        transport.assert_not_called()

    def test_inline_wire_bytes_are_stable_after_sorted_json_roundtrip(self):
        self.request["generationConfig"]["responseSchema"] = {
            "type": "OBJECT", "required": ["summary"], "propertyOrdering": ["summary"],
            "properties": {"summary": {"type": "ARRAY", "minItems": "1",
                                       "items": {"type": "STRING"}}}}
        before = deepcopy(self.rows)
        restored = json.loads(json.dumps(self.rows, sort_keys=True))
        first = client.gemini_batch_bytes("gemini-3.8-flash", self.rows, "wave_1")
        second = client.gemini_batch_bytes("gemini-3.8-flash", restored, "wave_1")
        self.assertEqual(first, second)
        self.assertEqual(self.rows, before)

    def test_transport_never_silently_rewrites_a_prepared_schema(self):
        self.request["generationConfig"]["responseSchema"] = {
            "type": "ARRAY", "minItems": "1", "maxItems": "24",
            "items": {"type": "STRING", "minLength": "1", "maxLength": "1200"}}
        before = deepcopy(self.rows)
        wire = json.loads(client.gemini_batch_bytes("gemini-3.8-flash", self.rows, "wave_1"))
        sent = wire["batch"]["input_config"]["requests"]["requests"][0]["request"]
        self.assertEqual(sent, self.request)
        self.assertEqual(self.rows, before)

    def test_create_rejects_model_display_or_job_key_injection(self):
        transport = mock.Mock()
        api = client.GeminiBatchClient("key", transport=transport)
        for model in (None, [], "models/gemini-3.8-flash", "gemini-x:generateContent", "../model", "gemini-x?key=secret"):
            with self.subTest(model=model), self.assertRaises(client.BatchClientError):
                api.create_batch(model, self.rows, "wave_1")
        for display in (None, [], "", "wave\nsecret", "x" * 129):
            with self.subTest(display=display), self.assertRaises(client.BatchClientError):
                api.create_batch("gemini-3.8-flash", self.rows, display)
        for rows in ([], None, self.rows * 2, [{"key": "../bad", "request": self.request}], [{"key": "job_1"}], [None]):
            with self.subTest(rows=rows), self.assertRaises(client.BatchClientError):
                api.create_batch("gemini-3.8-flash", rows, "wave_1")
        transport.assert_not_called()

    def test_inline_requests_cannot_enable_tools_other_models_storage_or_files(self):
        transport = mock.Mock()
        api = client.GeminiBatchClient("key", transport=transport)
        changes = ({"tools": []}, {"cachedContent": "cachedContents/123"}, {"store": True},
                   {"model": "models/gemini-other"}, {"generationConfig": []}, {"contents": []},
                   {"contents": [{"parts": [{"fileData": {"fileUri": "https://private.invalid"}}]}]},
                   {"contents": [{"parts": [{"text": "okay", "inlineData": {}}]}]},
                   {"contents": [{"role": [], "parts": [{"text": "okay"}]}]},
                   {"systemInstruction": {"parts": [{"functionCall": {}}]}})
        for change in changes:
            with self.subTest(change=change), self.assertRaises(client.BatchClientError):
                api.create_batch("gemini-3.8-flash", [{"key": "job_1", "request": {**self.request, **change}}], "wave_1")
        transport.assert_not_called()

    def test_nonfinite_json_and_oversized_inline_rejected_before_network(self):
        transport = mock.Mock()
        api = client.GeminiBatchClient("key", transport=transport)
        request = {**self.request, "generationConfig": {"temperature": float("nan")}}
        with self.assertRaises(client.BatchClientError):
            api.create_batch("gemini-3.8-flash", [{"key": "job_1", "request": request}], "wave_1")
        with mock.patch.object(client, "MAX_GEMINI_INLINE_BYTES", 10), self.assertRaises(client.BatchClientError):
            api.create_batch("gemini-3.8-flash", self.rows, "wave_1")
        transport.assert_not_called()

    def test_pending_completed_and_error_operations(self):
        outputs = [operation(), operation(done=False), operation(done=True, response={"inlinedResponses": {"inlinedResponses": [{"metadata": {"key": "job_1"}, "response": {"candidates": []}}]}}),
                   operation(done=True, error={"code": 1, "message": "Cancelled"})]
        transport = mock.Mock(side_effect=[Response(item) for item in outputs])
        api = client.GeminiBatchClient("key", transport=transport)
        for expected in outputs:
            self.assertEqual(api.get_batch("batches/abc123"), expected)
        self.assertTrue(all(call.args[0].get_method() == "GET" for call in transport.call_args_list))

    def test_mutation_malformed_operation_is_ambiguous(self):
        invalid = [operation(name="batches/../escape"), operation(done="true"),
                   operation(done=False, response={}), operation(done=True, response={}, error={"code": 1}),
                   operation(done=True, error={"code": "1"}), operation(metadata=[]),
                   {"name": "batches/abc123", "state": "JOB_STATE_SUCCEEDED", "dest": {}}]
        for value in invalid:
            api = client.GeminiBatchClient("key", transport=mock.Mock(return_value=Response(value)))
            with self.assertRaises(client.BatchClientError) as error:
                api.create_batch("gemini-3.8-flash", self.rows, "wave_1")
            self.assertTrue(error.exception.ambiguous)

    def test_poll_name_binding(self):
        api = client.GeminiBatchClient("key", transport=mock.Mock(return_value=Response(operation(name="batches/other"))))
        with self.assertRaises(client.BatchClientError) as error:
            api.get_batch("batches/abc123")
        self.assertFalse(error.exception.ambiguous)

    def test_list_pagination_uses_opaque_escaped_tokens(self):
        value = {"operations": [operation()], "nextPageToken": "next+=/opaque"}
        transport = mock.Mock(return_value=Response(value))
        api = client.GeminiBatchClient("key", transport=transport)
        self.assertEqual(api.list_batches("previous+=/opaque", page_size=20), value)
        request = transport.call_args.args[0]
        self.assertEqual(urllib.parse.parse_qs(urllib.parse.urlsplit(request.full_url).query), {"pageSize": ["20"], "pageToken": ["previous+=/opaque"]})
        self.assertIsNone(request.get_header("Authorization"))
        transport.assert_called_once()

    def test_empty_listing_uses_proto_defaults(self):
        api = client.GeminiBatchClient("key", transport=mock.Mock(return_value=Response({})))
        self.assertEqual(api.list_batches(), {})

    def test_malformed_or_incomplete_list_never_means_empty_success(self):
        cases = ({"operations": [operation(), operation()]}, {"operations": None},
                 {"batches": []},
                 {"operations": [], "nextPageToken": "next"}, {"unreachable": ["region"]},
                 {"operations": [None]}, {"operations": [operation()], "nextPageToken": "same"})
        for value in cases:
            api = client.GeminiBatchClient("key", transport=mock.Mock(return_value=Response(value)))
            with self.assertRaises(client.BatchClientError):
                api.list_batches("same")

    def test_bad_page_size_token_or_batch_name_never_calls_transport(self):
        transport = mock.Mock()
        api = client.GeminiBatchClient("key", transport=transport)
        for value in (None, [], "", "../escape", "batches/a/b", "batches/a?key=x", "batches/a\n"):
            with self.subTest(value=value), self.assertRaises(client.BatchClientError):
                api.get_batch(value)
        for size in (0, 101, True, "1"):
            with self.assertRaises(client.BatchClientError):
                api.list_batches(page_size=size)
        for token in ("", [], "line\nbreak", "é", "x" * 4097):
            with self.assertRaises(client.BatchClientError):
                api.list_batches(page_token=token)
        transport.assert_not_called()

    def test_no_file_operations_or_provider_fallback(self):
        transport = mock.Mock()
        api = client.GeminiBatchClient("key", transport=transport)
        for method in (api.upload_batch, api.get_file, api.download_file, api.delete_file):
            with self.assertRaises(client.BatchClientError):
                method("file-abc123")
        transport.assert_not_called()


if __name__ == "__main__":
    unittest.main()
