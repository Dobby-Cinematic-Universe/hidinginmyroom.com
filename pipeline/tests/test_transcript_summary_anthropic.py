from __future__ import annotations

import copy
import http.client
import io
import json
import os
import unittest
import urllib.error
import urllib.request
from unittest import mock

from pipeline import transcript_summary_anthropic as client
from pipeline import transcript_summary_client as shared


class Response(io.BytesIO):
    def __init__(self, value=None, *, raw=None, status=200, headers=None):
        super().__init__(json.dumps({} if value is None else value).encode() if raw is None else raw)
        self.status = status
        self.headers = {} if headers is None else headers


def request(custom_id="wave1_job1", **params):
    value = {"model": "claude-sonnet-5", "max_tokens": 1024,
             "messages": [{"role": "user", "content": "private source text"}],
             "system": "Summarize the source.", "thinking": {"type": "disabled"},
             "output_config": {"format": {"type": "json_schema", "schema": {
                 "type": "object", "properties": {"summary": {"type": "string"}},
                 "required": ["summary"], "additionalProperties": False}}}}
    value.update(params)
    return {"custom_id": custom_id, "params": value}


def batch(*, ended=False, **updates):
    value = {"id": "msgbatch_abc123", "type": "message_batch",
             "processing_status": "ended" if ended else "in_progress",
             "request_counts": {"processing": 0 if ended else 1,
                                "succeeded": 1 if ended else 0,
                                "errored": 0, "canceled": 0, "expired": 0},
             "created_at": "2026-09-12T15:00:00.123456Z",
             "expires_at": "2026-09-13T15:00:00.123456Z",
             "ended_at": "2026-09-12T15:01:00.123456Z" if ended else None,
             "cancel_initiated_at": None,
             "results_url": ("https://api.anthropic.com/v1/messages/batches/msgbatch_abc123/results"
                             if ended else None)}
    value.update(updates)
    return value


class AnthropicRequestTests(unittest.TestCase):
    def test_exact_body_and_authentication(self):
        rows = [request(messages=[{"role": "user", "content": "Transcript: café 🎵"}])]
        before = copy.deepcopy(rows)
        data = client.anthropic_batch_bytes(rows)
        transport = mock.Mock(return_value=Response(batch()))
        api = client.AnthropicBatchClient("explicit-key", 7, transport)
        self.assertEqual(api.create_batch(rows), batch())
        sent, timeout = transport.call_args.args
        self.assertEqual(sent.full_url, "https://api.anthropic.com/v1/messages/batches")
        self.assertEqual(sent.get_method(), "POST")
        self.assertEqual(sent.data, data)
        self.assertEqual(data, json.dumps({"requests": rows}, sort_keys=True, ensure_ascii=True,
                                          allow_nan=False, separators=(",", ":")).encode())
        self.assertEqual(json.loads(data), {"requests": rows})
        self.assertEqual(sent.get_header("X-api-key"), "explicit-key")
        self.assertEqual(sent.get_header("Anthropic-version"), "2023-06-01")
        self.assertEqual(sent.get_header("Content-type"), "application/json")
        self.assertEqual(sent.get_header("Accept-encoding"), "identity")
        self.assertIsNone(sent.get_header("Authorization"))
        self.assertEqual(timeout, 7)
        self.assertEqual(rows, before)
        transport.assert_called_once()

    def test_body_is_stable_across_canonical_manifest_roundtrips(self):
        rows = [request()]
        recovered = json.loads(json.dumps(rows, sort_keys=True))
        self.assertEqual(client.anthropic_batch_bytes(rows), client.anthropic_batch_bytes(recovered))

    def test_minimal_request_and_text_blocks(self):
        rows = [{"custom_id": "job", "params": {"model": "claude-sonnet-5", "max_tokens": 1,
                  "messages": [{"role": "user", "content": [{"type": "text", "text": "a"}]}]}}]
        self.assertEqual(json.loads(client.anthropic_batch_bytes(rows)), {"requests": rows})
        for thinking in ("adaptive", "disabled"):
            row = request(thinking={"type": thinking}, system=[{"type": "text", "text": "a"}])
            client.anthropic_batch_bytes([row])

    def test_batch_limits_and_ids_checked_before_network(self):
        transport = mock.Mock()
        api = client.AnthropicBatchClient("key", transport=transport)
        for rows in (None, {}, [], (), [None], [request(), request()],
                     [{**request(), "metadata": {}}], [{"custom_id": "one"}]):
            with self.subTest(rows=rows), self.assertRaises(client.AnthropicClientError) as error:
                api.create_batch(rows)
            self.assertFalse(error.exception.ambiguous)
        for identifier in (None, 12, "", "x" * 65, "a/b", "..", "a b", "a\n", "café", "a?b", "a%2fb"):
            with self.subTest(identifier=identifier), self.assertRaises(client.AnthropicClientError):
                api.create_batch([request(identifier)])
        client.anthropic_batch_bytes([request("a_-Z09"), request("z" * 64)])
        with mock.patch.object(client, "MAX_BATCH_REQUESTS", 1):
            with self.assertRaises(client.AnthropicClientError):
                api.create_batch([request("one"), request("two")])
        transport.assert_not_called()

    def test_exact_byte_limit_inclusive(self):
        rows = [request()]
        data = client.anthropic_batch_bytes(rows)
        self.assertEqual(client.MAX_ANTHROPIC_BATCH_BYTES, 16 * 1024 * 1024)
        self.assertEqual(client.MAX_BATCH_REQUESTS, 50_000)
        with mock.patch.object(client, "MAX_ANTHROPIC_BATCH_BYTES", len(data)):
            self.assertEqual(client.anthropic_batch_bytes(rows), data)
        transport = mock.Mock()
        with mock.patch.object(client, "MAX_ANTHROPIC_BATCH_BYTES", len(data) - 1):
            with self.assertRaises(client.AnthropicClientError):
                client.AnthropicBatchClient("key", transport=transport).create_batch(rows)
        transport.assert_not_called()

    def test_size_reports_oversized_candidates_without_admitting_them(self):
        rows = [request("one"), request("two")]
        expected = len(client.anthropic_batch_bytes(rows))
        self.assertEqual(client.anthropic_batch_size(rows), expected)
        with mock.patch.object(client, "MAX_ANTHROPIC_BATCH_BYTES", expected - 1):
            self.assertEqual(client.anthropic_batch_size(rows), expected)
            with self.assertRaises(client.AnthropicClientError):
                client.anthropic_batch_bytes(rows)
        for invalid in ([], [request("duplicate"), request("duplicate")], [request(model="other")]):
            with self.assertRaises(client.AnthropicClientError):
                client.anthropic_batch_size(invalid)

    def test_model_and_output_limit_validation(self):
        bad_params = [{"model": value} for value in (None, "claude-sonnet-4-6", "claude-sonnet-5-latest", [], {})]
        bad_params += [{"max_tokens": value} for value in (None, True, 0, -1, 128001, 1.5, "1024")]
        for params in bad_params:
            with self.subTest(params=params), self.assertRaises(client.AnthropicClientError):
                client.anthropic_batch_bytes([request(**params)])
        for tokens in (1, 128_000):
            client.anthropic_batch_bytes([request(max_tokens=tokens)])

    def test_forbidden_request_fields_and_metadata(self):
        fields = {"tools": [], "tool_choice": {"type": "auto"}, "metadata": {},
                  "stream": False, "speed": "fast", "temperature": 0,
                  "top_p": 0.9, "top_k": 10, "container": "private-container",
                  "cache_control": {"type": "ephemeral"}, "service_tier": "auto",
                  "store": False, "response_format": {"type": "json_object"}}
        transport = mock.Mock()
        api = client.AnthropicBatchClient("key", transport=transport)
        for key, value in fields.items():
            with self.subTest(key=key), self.assertRaises(client.AnthropicClientError):
                api.create_batch([request(**{key: value})])
        with self.assertRaises(TypeError):
            api.create_batch([request()], metadata={"wave": "one"})
        transport.assert_not_called()

    def test_text_validation_rejects_nontext_content(self):
        values = (None, [], "", "\ud800", [{"type": "image", "source": {"url": "https://private.invalid"}}],
                  [{"type": "text", "text": "a", "cache_control": {"type": "ephemeral"}}],
                  [{"type": "text", "text": ""}], [{"type": "tool_result", "content": "a"}],
                  [{"type": "text", "text": 1}])
        for content in values:
            for params in ({"messages": [{"role": "user", "content": content}]}, {"system": content}):
                with self.subTest(params=params), self.assertRaises(client.AnthropicClientError):
                    client.anthropic_batch_bytes([request(**params)])
        for messages in ([], {}, "source", [{"role": "system", "content": "source"}],
                         [{"role": [], "content": "source"}], [{"role": "user"}],
                         [{"role": "user", "content": "source", "name": "person"}],
                         [{"role": "assistant", "content": "prefill"}]):
            with self.subTest(messages=messages), self.assertRaises(client.AnthropicClientError):
                client.anthropic_batch_bytes([request(messages=messages)])

    def test_thinking_and_schema_configuration_validation(self):
        values = [{"thinking": value} for value in (None, "disabled", {"type": "enabled", "budget_tokens": 1024},
                  {"type": "adaptive", "budget_tokens": 1024}, {"type": []})]
        values += [{"output_config": value} for value in (None, {}, [], {"effort": []}, {"effort": "extreme"},
                   {"format": {"type": "text"}}, {"format": {"type": "json_schema", "schema": []}},
                   {"format": {"type": "json_schema", "schema": {}}}, {"extra": True})]
        for params in values:
            with self.subTest(params=params), self.assertRaises(client.AnthropicClientError):
                client.anthropic_batch_bytes([request(**params)])
        for bad_schema in ({"enum": [float("nan")]}, {"enum": [float("inf")]}, {"enum": [object()]}):
            with self.assertRaises(client.AnthropicClientError) as error:
                client.anthropic_batch_bytes([request(output_config={"format": {"type": "json_schema", "schema": bad_schema}})])
            self.assertNotIn("object at", str(error.exception))


class AnthropicResponseTests(unittest.TestCase):
    def test_public_offline_validator_and_get_alias(self):
        original = batch()
        self.assertIs(client.validate_batch(original, "msgbatch_abc123", 1), original)
        for expected_count in (0, True, "1", -1, 50001):
            with self.assertRaises(client.AnthropicClientError):
                client.validate_batch(original, expected_count=expected_count)
        for value in (None, [], "batch", 1):
            with self.assertRaises(client.AnthropicClientError):
                client.validate_batch(value)
        transport = mock.Mock(return_value=Response(original))
        api = client.AnthropicBatchClient("key", transport=transport)
        self.assertEqual(api.get_batch("msgbatch_abc123"), original)
        transport.assert_called_once()

    def test_retrieve_ended_and_download_official_path(self):
        results = b'{"custom_id":"wave1_job1","result":{"type":"expired"}}\n'
        transport = mock.Mock(side_effect=[Response(batch(ended=True)), Response(raw=results)])
        api = client.AnthropicBatchClient("key", transport=transport)
        self.assertEqual(api.retrieve_batch("msgbatch_abc123"), batch(ended=True))
        self.assertEqual(api.download_results("msgbatch_abc123"), results)
        first = transport.call_args_list[0].args[0]
        second = transport.call_args_list[1].args[0]
        self.assertEqual(first.full_url, "https://api.anthropic.com/v1/messages/batches/msgbatch_abc123")
        self.assertEqual(second.full_url, first.full_url + "/results")
        for sent in (first, second):
            self.assertEqual(sent.get_method(), "GET")
            self.assertIsNone(sent.data)
            self.assertEqual(sent.get_header("X-api-key"), "key")
        self.assertEqual(second.get_header("Accept"), "application/x-jsonl")
        self.assertEqual(transport.call_count, 2)

    def test_invalid_response_is_ambiguous_only_after_creation(self):
        invalid = [batch(id="batch_other"), batch(type="batch"), batch(processing_status=[]),
                   batch(processing_status="completed"), batch(private_extra="private-value"),
                   batch(request_counts=None), batch(request_counts={}),
                   batch(request_counts={**batch()["request_counts"], "processing": True}),
                   batch(request_counts={**batch()["request_counts"], "processing": -1}),
                   batch(request_counts={**batch()["request_counts"], "processing": 0}),
                   batch(request_counts={**batch()["request_counts"], "processing": 50001}),
                   batch(request_counts={**batch()["request_counts"], "succeeded": 50000}),
                   batch(request_counts={**batch()["request_counts"], "unknown": 0})]
        for field in batch():
            value = batch()
            del value[field]
            invalid.append(value)
        for value in invalid:
            for mutation in (False, True):
                transport = mock.Mock(return_value=Response(value))
                api = client.AnthropicBatchClient("key", transport=transport)
                with self.subTest(value=value, mutation=mutation), self.assertRaises(client.AnthropicClientError) as error:
                    api.create_batch([request()]) if mutation else api.retrieve_batch("msgbatch_abc123")
                self.assertEqual(error.exception.ambiguous, mutation)
                self.assertEqual(error.exception.status_code, 200)
                self.assertNotIn("private-value", str(error.exception))
                transport.assert_called_once()

    def test_creation_binds_request_count_and_retrieval_binds_id(self):
        value = batch(request_counts={**batch()["request_counts"], "processing": 2})
        api = client.AnthropicBatchClient("key", transport=mock.Mock(return_value=Response(value)))
        with self.assertRaises(client.AnthropicClientError) as error:
            api.create_batch([request()])
        self.assertTrue(error.exception.ambiguous)
        api = client.AnthropicBatchClient("key", transport=mock.Mock(return_value=Response(batch(id="msgbatch_other"))))
        with self.assertRaises(client.AnthropicClientError) as error:
            api.retrieve_batch("msgbatch_abc123")
        self.assertFalse(error.exception.ambiguous)

    def test_ended_counts_and_timestamp_consistency(self):
        values = [batch(ended=True, request_counts=batch()["request_counts"]), batch(ended=True, ended_at=None),
                  batch(ended_at="2026-09-12T15:01:00Z"), batch(processing_status="canceling"),
                  batch(cancel_initiated_at="2026-09-12T15:01:00Z"),
                  batch(ended=True, cancel_initiated_at="2026-09-12T15:02:00Z"),
                  batch(archived_at="2026-09-13T15:02:00Z"),
                  batch(ended=True, archived_at="2026-09-12T15:00:01Z"),
                  batch(expires_at="2026-09-12T15:00:00.123456Z"),
                  batch(ended=True, ended_at="2026-09-11T15:00:00Z")]
        for value in values:
            with self.subTest(value=value), self.assertRaises(client.AnthropicClientError):
                client._batch(value)
        client._batch(batch(processing_status="canceling", cancel_initiated_at="2026-09-12T15:00:01Z"))
        for outcome in ("succeeded", "errored", "canceled", "expired"):
            counts = {key: int(key == outcome) for key in batch()["request_counts"]}
            client._batch(batch(ended=True, request_counts=counts))
        client._batch(batch(ended=True, archived_at="2026-09-13T15:02:00Z", results_url=None))

    def test_timestamps_are_strict_aware_calendar_values(self):
        invalid = (None, 123, True, "2026-09-12", "2026-09-12T15:00:00", "2026-09-12 15:00:00Z",
                   "2026-02-30T15:00:00Z", "2026-09-12T24:00:00Z", "2026-09-12T15:00:00+25:00",
                   "2026-09-12T15:00:00+00:99", "2026-09-12T15:00:00.1234567Z", "2026-09-12T15:00:00Z\n")
        for field in ("created_at", "expires_at", "ended_at", "cancel_initiated_at", "archived_at"):
            for timestamp in invalid:
                if timestamp is None and field not in {"created_at", "expires_at", "ended_at"}:
                    continue
                with self.subTest(field=field, timestamp=timestamp), self.assertRaises(client.AnthropicClientError):
                    client._batch(batch(ended=True, **{field: timestamp}))
        client._batch(batch(created_at="2026-09-12T11:00:00.123456-04:00"))

    def test_results_url_cannot_change_destination(self):
        official = batch(ended=True)["results_url"]
        invalid = ("https://attacker.invalid/results", official.replace("https:", "http:"),
                   official.replace("api.anthropic.com", "api.anthropic.com.attacker.invalid"),
                   official.replace("api.anthropic.com", "api.anthropic.com:443"),
                   official.replace("api.anthropic.com", "secret@api.anthropic.com"),
                   official.replace("msgbatch_abc123", "msgbatch_other"), official + "?key=private",
                   official + "#fragment", official + "/", official.replace("abc123", "abc%31%32%33"),
                   "//api.anthropic.com/v1/messages/batches/msgbatch_abc123/results", {}, [])
        for url in invalid:
            transport = mock.Mock(return_value=Response(batch(ended=True, results_url=url)))
            api = client.AnthropicBatchClient("key", transport=transport)
            with self.subTest(url=url), self.assertRaises(client.AnthropicClientError):
                api.retrieve_batch("msgbatch_abc123")
            transport.assert_called_once()


class AnthropicTransportSafetyTests(unittest.TestCase):
    def test_explicit_key_timeout_and_transport_validation(self):
        with mock.patch.dict(os.environ, {"ANTHROPIC_API_KEY": "ambient-private-key"}):
            for key in (None, "", "a b", "a\rb", "a\nb", "é", "a" * 4097):
                with self.assertRaises(client.AnthropicClientError) as error:
                    client.AnthropicBatchClient(key)
                self.assertNotIn("ambient-private-key", str(error.exception))
        for timeout in (0, -1, 301, True, "1", float("nan"), float("inf")):
            with self.assertRaises(client.AnthropicClientError):
                client.AnthropicBatchClient("key", timeout)
        with self.assertRaises(client.AnthropicClientError):
            client.AnthropicBatchClient("key", transport=123)
        self.assertIs(client.AnthropicClientError, shared.OpenAIClientError)

    def test_builtin_transport_reuses_no_proxy_or_redirect_handlers(self):
        with mock.patch.object(shared.urllib.request, "build_opener") as build:
            build.return_value.open.return_value = Response(batch())
            api = client.AnthropicBatchClient("key")
            self.assertEqual(api.retrieve_batch("msgbatch_abc123"), batch())
        proxy_handler, redirect_handler = build.call_args.args
        self.assertEqual(proxy_handler.proxies, {})
        self.assertIsInstance(redirect_handler, shared._NoRedirect)
        self.assertIsNone(redirect_handler.redirect_request(None, None, 302, "Found", {}, "https://evil.invalid"))
        self.assertEqual(build.return_value.open.call_args.kwargs, {"timeout": 60})

    def test_invalid_ids_paths_and_inherited_mutations_never_send(self):
        transport = mock.Mock()
        api = client.AnthropicBatchClient("key", transport=transport)
        for identifier in (None, "", "msgbatch_", "batch_one", "msgbatch_../a", "msgbatch_a/b", "msgbatch_a\n",
                           "msgbatch_a?x=1", "msgbatch_é", "msgbatch_" + "a" * 129, "https://evil.invalid"):
            for operation in (api.retrieve_batch, api.download_results):
                with self.subTest(identifier=identifier), self.assertRaises(client.AnthropicClientError):
                    operation(identifier)
        for method, path in (("POST", "/v1/messages"), ("GET", "/v1/messages/batches"),
                             ("DELETE", "/v1/messages/batches/msgbatch_a"),
                             ("POST", "/v1/messages/batches/msgbatch_a/cancel"),
                             ("GET", "/v1/files/file-a"), ("GET", "//evil.invalid"),
                             ("GET", "/v1/messages/batches/msgbatch_a/results?key=private"),
                             ("GET", "/v1/messages/batches/msgbatch_a/../results"),
                             ("GET", "https://api.anthropic.com/v1/messages/batches/msgbatch_a"),
                             ("GET", "/v1/messages/batches/msgbatch_a%2fresults")):
            with self.subTest(method=method, path=path), self.assertRaises(client.AnthropicClientError):
                api._request(method, path)
        for operation, argument in ((api.upload_batch, b"data"), (api.get_file, "file-a"),
                                    (api.download_file, "file-a"), (api.delete_file, "file-a")):
            with self.assertRaises(client.AnthropicClientError):
                operation(argument)
        with self.assertRaises(client.AnthropicClientError):
            api.list_batches()
        transport.assert_not_called()

    def test_http_errors_redacted_closed_and_not_retried(self):
        for code, ambiguous in ((400, False), (401, False), (403, False), (408, True),
                                (429, False), (500, True), (503, True), (302, True)):
            body = io.BytesIO(b"private source private-key")
            failure = urllib.error.HTTPError("https://private-key.invalid", code,
                                             "private reason", {"Retry-After": "12"}, body)
            transport = mock.Mock(side_effect=failure)
            with self.assertRaises(client.AnthropicClientError) as error:
                client.AnthropicBatchClient("private-key", transport=transport).create_batch([request()])
            self.assertEqual(error.exception.status_code, code)
            self.assertEqual(error.exception.ambiguous, ambiguous)
            self.assertEqual(error.exception.retry_after_seconds, 12)
            self.assertNotIn("private", str(error.exception))
            self.assertTrue(body.closed)
            transport.assert_called_once()

    def test_transport_errors_redacted_and_not_retried(self):
        for failure in (urllib.error.URLError("private-key"), OSError("private-key"),
                        http.client.IncompleteRead(b"private source"), RuntimeError("private-key")):
            for mutation in (False, True):
                transport = mock.Mock(side_effect=failure)
                api = client.AnthropicBatchClient("private-key", transport=transport)
                with self.assertRaises(client.AnthropicClientError) as error:
                    api.create_batch([request()]) if mutation else api.retrieve_batch("msgbatch_abc123")
                self.assertEqual(error.exception.ambiguous, mutation)
                self.assertNotIn("private", str(error.exception))
                transport.assert_called_once()

    def test_malformed_adapter_responses_keep_creation_ambiguous(self):
        for headers in (None, {"Content-Encoding": None}, {"Content-Encoding": []}):
            response = Response(batch())
            response.headers = headers
            transport = mock.Mock(return_value=response)
            with self.assertRaises(client.AnthropicClientError) as error:
                client.AnthropicBatchClient("private-key", transport=transport).create_batch([request()])
            self.assertTrue(error.exception.ambiguous)
            self.assertNotIn("private", str(error.exception))
            transport.assert_called_once()

    def test_wrong_status_json_and_changed_final_origin(self):
        fixtures = [Response(status=status) for status in (201, 204, 301, 500, True)]
        fixtures += [Response(raw=raw) for raw in (b'[]', b'null', b'{"a":1,"a":2}', b'{"a":NaN}',
                                                 b'{"a":Infinity}', b'{"a":1e999}', b'{"a":"\xff"}')]
        changed = Response(batch())
        changed.geturl = lambda: "https://evil.invalid/private-key"
        fixtures.append(changed)
        for response in fixtures:
            transport = mock.Mock(return_value=response)
            with self.assertRaises(client.AnthropicClientError) as error:
                client.AnthropicBatchClient("key", transport=transport).create_batch([request()])
            self.assertTrue(error.exception.ambiguous)
            self.assertNotIn("private", str(error.exception))
            transport.assert_called_once()

    def test_response_and_download_byte_limits(self):
        fixtures = (({}, b"x" * 33), ({"Content-Length": "33"}, b"{}"),
                    ({"Content-Length": "-1"}, b"{}"), ({"Content-Length": "x"}, b"{}"),
                    ({"Content-Length": "4"}, b"{}"), ({"Content-Encoding": "gzip"}, b"{}"))
        for headers, raw in fixtures:
            for mutation in (False, True):
                transport = mock.Mock(return_value=Response(raw=raw, headers=headers))
                api = client.AnthropicBatchClient("key", transport=transport)
                with mock.patch.object(shared, "MAX_JSON_BYTES", 32), mock.patch.object(shared, "MAX_RESPONSE_BYTES", 32):
                    with self.assertRaises(client.AnthropicClientError) as error:
                        api.create_batch([request()]) if mutation else api.download_results("msgbatch_abc123")
                self.assertEqual(error.exception.ambiguous, mutation)
                transport.assert_called_once()

    def test_absolute_deadline(self):
        api = client.AnthropicBatchClient("key", transport=mock.Mock(return_value=Response(batch())))
        with mock.patch.object(shared.time, "monotonic", side_effect=[0, 61]):
            with self.assertRaises(client.AnthropicClientError) as error:
                api.create_batch([request()])
        self.assertTrue(error.exception.ambiguous)


if __name__ == "__main__":
    unittest.main()
