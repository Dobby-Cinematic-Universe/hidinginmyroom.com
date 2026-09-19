from __future__ import annotations

import copy
import hashlib
import http.client
import io
import json
import os
from pathlib import Path
import tempfile
import unittest
import urllib.error
import urllib.request
from unittest import mock

from pipeline import cloud_transcription_client as c


class Response(io.BytesIO):
    def __init__(self, value=None, *, raw=None, status=200, headers=None):
        super().__init__(raw if raw is not None else json.dumps(value or {}).encode())
        self.status = status
        self.headers = headers or {}


def assembly_result():
    word = {"text": "Hello.", "start": 100, "end": 700, "speaker": "A", "confidence": .99}
    return {"id": "123e4567-e89b-12d3-a456-426614174000", "status": "completed",
            "audio_duration": 10, "speech_models": [c.ASSEMBLYAI_MODEL],
            "speech_model_used": c.ASSEMBLYAI_MODEL, "speaker_labels": True,
            "language_code": "en", "text": "Hello.", "words": [word],
            "utterances": [{"speaker": "A", "start": 100, "end": 700,
                            "text": "Hello.", "words": [word]}]}


def rev_job():
    return {"id": "0ABCDEFG12345678", "status": "transcribed", "type": "async",
            "transcriber": "machine", "language": "en", "duration_seconds": 10}


def rev_result():
    return {"monologues": [{"speaker": 0, "elements": [
        {"type": "text", "value": "Hello", "ts": .1, "end_ts": .7, "confidence": .8},
        {"type": "punct", "value": ","}, {"type": "punct", "value": " "},
        {"type": "text", "value": "world", "ts": .8, "end_ts": 1.2, "confidence": 1},
        {"type": "punct", "value": "."}]}]}


class CloudTranscriptionClientTests(unittest.TestCase):
    def setUp(self):
        self.aai = c.AssemblyAIClient("secret-key")
        self.rev = c.RevAIClient("secret-key")
        self.url = "https://cdn.assemblyai.com/upload/f756988d-47e2-4ca3-96ce-04bb168f8f2a"
        self.folder = tempfile.TemporaryDirectory()
        self.addCleanup(self.folder.cleanup)
        self.path = Path(self.folder.name) / "private-title.wav"
        self.contents = b"RIFF some audio data"
        self.path.write_bytes(self.contents)
        self.path.chmod(0o600)
        self.digest = hashlib.sha256(self.contents).hexdigest()

    def streaming_transport(self, value, capture):
        def transport(request, timeout):
            capture.append((request, b"".join(request.data), timeout))
            return Response(value)
        return transport

    def test_constructor_requires_explicit_safe_key_and_bounded_timeout(self):
        for key in [None, "", "secret\nAuthorization:other", "token with spaces", "é", "x" * 4097]:
            with self.subTest(key=key), self.assertRaises(c.CloudClientError):
                c.AssemblyAIClient(key)
        for timeout in [None, True, 0, -1, 3601, float("nan"), float("inf")]:
            with self.subTest(timeout=timeout), self.assertRaises(c.CloudClientError):
                c.RevAIClient("key", timeout)
        with mock.patch.dict(os.environ, {"ASSEMBLYAI_API_KEY": "environment-secret"}):
            with self.assertRaises(TypeError):
                c.AssemblyAIClient()

    def test_assembly_exact_model_request_without_glossary(self):
        raw = {"id": "job", "status": "queued"}
        with mock.patch.object(c, "_transport", return_value=Response(raw)) as transport:
            self.assertEqual(self.aai.submit(self.url), raw)
        request = transport.call_args.args[0]
        self.assertEqual(request.full_url, "https://api.assemblyai.com/v2/transcript")
        self.assertEqual(request.get_header("Authorization"), "secret-key")
        self.assertEqual(request.get_method(), "POST")
        self.assertEqual(json.loads(request.data), {"audio_url": self.url,
            "speech_models": ["universal-3-5-pro"], "language_code": "en", "speaker_labels": True,
            "punctuate": True, "format_text": True, "filter_profanity": False})
        transport.assert_called_once()

    def test_assembly_upload_streams_raw_bytes_with_length_and_no_multipart(self):
        captured = []
        with mock.patch.object(c, "_transport", side_effect=self.streaming_transport({"upload_url": self.url}, captured)):
            self.assertEqual(self.aai.upload(self.path, expected_sha256=self.digest), {"upload_url": self.url})
        request, body, timeout = captured[0]
        self.assertEqual(request.full_url, "https://api.assemblyai.com/v2/upload")
        self.assertEqual(body, self.contents)
        self.assertNotIsInstance(request.data, bytes)
        self.assertEqual(request.get_header("Content-length"), str(len(body)))
        self.assertEqual(request.get_header("Content-type"), "application/octet-stream")

    def test_rev_multipart_streams_machine_job_and_opaque_filename(self):
        captured = []
        with mock.patch.object(c, "_transport", side_effect=self.streaming_transport(rev_job(), captured)):
            self.rev.submit_file(self.path, expected_sha256=self.digest, metadata="request_fingerprint_abc")
        request, body, _ = captured[0]
        self.assertEqual(request.full_url, "https://api.rev.ai/speechtotext/v1/jobs")
        self.assertEqual(request.get_header("Authorization"), "Bearer secret-key")
        self.assertEqual(request.get_header("Content-length"), str(len(body)))
        self.assertIn(b'name="options"', body)
        self.assertIn(b'name="media"; filename="audio.wav"', body)
        self.assertIn(b'"transcriber":"machine"', body)
        self.assertIn(b'"skip_diarization":false', body)
        self.assertIn(b'"metadata":"request_fingerprint_abc"', body)
        self.assertIn(self.contents, body)
        self.assertNotIn(b"private-title", body)
        for unwanted in [b"custom_vocab", b"speaker_names", b"human", b"fusion", b"low_cost"]:
            self.assertNotIn(unwanted, body)

    def test_explicit_diarization_off_requests_preserve_rev_metadata(self):
        with mock.patch.object(c, "_transport", return_value=Response()) as transport:
            self.aai.submit(self.url, diarization=False)
        options = json.loads(transport.call_args.args[0].data)
        self.assertIs(options["speaker_labels"], False)
        self.assertNotIn("speakers_expected", options)
        self.assertNotIn("speaker_options", options)
        captured = []
        with mock.patch.object(c, "_transport", side_effect=self.streaming_transport(rev_job(), captured)):
            self.rev.submit_file(self.path, expected_sha256=self.digest, metadata="fingerprint", diarization=False)
        self.assertIn(b'"skip_diarization":true', captured[0][1])
        self.assertIn(b'"metadata":"fingerprint"', captured[0][1])
        self.assertNotIn(b"speakers_count", captured[0][1])

    def test_diarization_switch_rejects_truthy_nonbooleans_without_network(self):
        with mock.patch.object(c, "_transport") as transport:
            for value in [None, 0, 1, "false", [], {}]:
                with self.subTest(value=value):
                    with self.assertRaises(c.CloudClientError):
                        self.aai.submit(self.url, diarization=value)
                    with self.assertRaises(c.CloudClientError):
                        self.rev.submit_file(self.path, expected_sha256=self.digest, diarization=value)
                    with self.assertRaises(c.CloudClientError):
                        c.normalize_result("assemblyai", assembly_result(), expected_duration_seconds=10, diarization=value)
        transport.assert_not_called()

    def test_poll_paths_and_rev_transcript_accept(self):
        for api, expected in [(self.aai, "https://api.assemblyai.com/v2/transcript/safe-id"),
                              (self.rev, "https://api.rev.ai/speechtotext/v1/jobs/safe-id")]:
            with mock.patch.object(c, "_transport", return_value=Response()) as transport:
                api.poll("safe-id")
            self.assertEqual(transport.call_args.args[0].full_url, expected)
            self.assertEqual(transport.call_args.args[0].get_method(), "GET")
        with mock.patch.object(c, "_transport", return_value=Response()) as transport:
            self.rev.transcript("safe-id")
        request = transport.call_args.args[0]
        self.assertEqual(request.full_url, "https://api.rev.ai/speechtotext/v1/jobs/safe-id/transcript")
        self.assertEqual(request.get_header("Accept"), "application/vnd.rev.transcript.v1.0+json")

    def test_invalid_job_ids_urls_or_metadata_never_reach_network(self):
        invalid_urls = [None, "http://cdn.assemblyai.com/upload/abc", self.url + "?secret=key",
                        self.url + "#x", self.url.replace("cdn.assemblyai.com", "evil.example"),
                        self.url.replace("cdn.assemblyai.com", "cdn.assemblyai.com:443"),
                        self.url.replace("cdn.assemblyai.com", "user@cdn.assemblyai.com"),
                        "https://cdn.assemblyai.com/upload/../abc", self.url + "\n"]
        with mock.patch.object(c, "_transport") as transport:
            for value in invalid_urls:
                with self.subTest(url=value), self.assertRaises(c.CloudClientError):
                    self.aai.submit(value)
            for value in [None, "", "../secret", "id?key=other", "id\n", "x" * 129]:
                with self.subTest(identifier=value), self.assertRaises(c.CloudClientError):
                    self.rev.poll(value)
            with self.assertRaises(c.CloudClientError):
                self.rev.submit_file(self.path, expected_sha256=self.digest, metadata="secret\n")
        transport.assert_not_called()

    def test_assembly_project_qualified_upload_receipt_is_accepted_without_relaxing_origin(self):
        # Synthetic opaque identifiers, never a retained private capability URL.
        qualified = 'https://cdn.assemblyai.com/upload/' + 'a' * 64 + '/123e4567-e89b-12d3-a456-426614174000'
        self.assertEqual(c.validate_upload_url(qualified), qualified)
        self.assertEqual(c.assemblyai_options(qualified)['audio_url'], qualified)
        invalid = [qualified + '/extra', qualified + '/', qualified.replace('/upload/', '/upload//'),
                   qualified.replace('/' + 'a' * 64 + '/', '/%2F/'), qualified.replace('/upload/', '/upload/../'),
                   qualified + '?token=private', qualified + '#fragment',
                   qualified.replace('cdn.assemblyai.com', 'cdn.assemblyai.com:443'),
                   qualified.replace('cdn.assemblyai.com', 'user@cdn.assemblyai.com'),
                   qualified.replace('cdn.assemblyai.com', 'evil.example'),
                   qualified.replace('https://', 'http://'),
                   'https://cdn.assemblyai.com/upload/' + 'x' * 129 + '/safe']
        for value in invalid:
            with self.subTest(value=value), self.assertRaises(c.CloudClientError):
                c.validate_upload_url(value)

    def test_upload_rejects_wrong_digest_empty_large_symlink_and_unsafe_mode(self):
        with mock.patch.object(c, "_transport") as transport:
            for digest in [None, "invalid", "0" * 64]:
                with self.subTest(digest=digest), self.assertRaises(c.CloudClientError):
                    self.aai.upload(self.path, expected_sha256=digest)
            with mock.patch.object(c, "ASSEMBLYAI_MAX_UPLOAD_BYTES", 1), self.assertRaises(c.CloudClientError):
                self.aai.upload(self.path, expected_sha256=self.digest)
            with mock.patch.object(c, "REVAI_MAX_UPLOAD_BYTES", 1), self.assertRaises(c.CloudClientError):
                self.rev.submit_file(self.path, expected_sha256=self.digest)
            linked = self.path.with_name("symlink.wav")
            linked.symlink_to(self.path)
            with self.assertRaises(c.CloudClientError):
                self.aai.upload(linked, expected_sha256=self.digest)
            self.path.chmod(0o666)
            with self.assertRaises(c.CloudClientError):
                self.aai.upload(self.path, expected_sha256=self.digest)
            self.path.chmod(0o600)
            self.path.write_bytes(b"")
            with self.assertRaises(c.CloudClientError):
                self.aai.upload(self.path, expected_sha256=hashlib.sha256(b"").hexdigest())
        transport.assert_not_called()

    def test_symlinked_parent_and_hardlinked_upload_rejected(self):
        alias = self.path.parent / "alias"
        alias.symlink_to(self.path.parent, target_is_directory=True)
        with mock.patch.object(c, "_transport") as transport:
            with self.assertRaises(c.CloudClientError):
                self.aai.upload(alias / self.path.name, expected_sha256=self.digest)
            os.link(self.path, self.path.with_name("hardlink.wav"))
            with self.assertRaises(c.CloudClientError):
                self.aai.upload(self.path, expected_sha256=self.digest)
        transport.assert_not_called()

    def test_streaming_uses_bounded_chunks_and_is_not_replayable(self):
        captured = []
        def transport(request, timeout):
            blocks = list(request.data)
            self.assertTrue(all(len(block) <= 4 for block in blocks))
            self.assertEqual(b"".join(blocks), self.contents)
            with self.assertRaises(c.CloudClientError):
                list(request.data)
            captured.append(request)
            return Response({"upload_url": self.url})
        with mock.patch.object(c, "CHUNK_BYTES", 4), mock.patch.object(c, "_transport", side_effect=transport):
            self.aai.upload(self.path, expected_sha256=self.digest)
        self.assertEqual(len(captured), 1)

    def test_upload_path_replaced_during_transfer_is_ambiguous_and_keeps_receipt(self):
        receipt = rev_job()
        def transport(request, timeout):
            list(request.data)
            self.path.rename(self.path.with_name("old.wav"))
            self.path.write_bytes(self.contents)
            return Response(receipt)
        with mock.patch.object(c, "_transport", side_effect=transport), self.assertRaises(c.CloudClientError) as raised:
            self.rev.submit_file(self.path, expected_sha256=self.digest)
        self.assertTrue(raised.exception.ambiguous)
        self.assertEqual(raised.exception.response, receipt)

    def test_upload_modified_midstream_is_ambiguous(self):
        def transport(request, timeout):
            stream = iter(request.data)
            next(stream)
            self.path.write_bytes(b"changed")
            list(stream)
            self.fail("changed upload must not finish")
        with mock.patch.object(c, "CHUNK_BYTES", 4), mock.patch.object(c, "_transport", side_effect=transport), self.assertRaises(c.CloudClientError) as raised:
            self.aai.upload(self.path, expected_sha256=self.digest)
        self.assertTrue(raised.exception.ambiguous)

    def test_raw_success_response_not_semantically_validated_before_journaling(self):
        # No valid id/status: return to orchestrator first, don't lose evidence.
        raw = {"unexpected": "private provider information"}
        with mock.patch.object(c, "_transport", return_value=Response(raw)):
            self.assertEqual(self.aai.submit(self.url), raw)
        with self.assertRaises(c.CloudClientError):
            c.validate_job("assemblyai", raw)

    def test_network_errors_sanitized_and_never_retried(self):
        for method in ["submit", "poll"]:
            for error in [urllib.error.URLError("secret-key private-url"), OSError("private"),
                          http.client.IncompleteRead(b"secret-key")]:
                with mock.patch.object(c, "_transport", side_effect=error) as transport:
                    with self.assertRaises(c.CloudClientError) as raised:
                        getattr(self.aai, method)(self.url if method == "submit" else "id")
                self.assertEqual(raised.exception.ambiguous, method == "submit")
                self.assertNotIn("secret-key", str(raised.exception))
                self.assertNotIn("private", str(raised.exception))
                transport.assert_called_once()

    def test_http_error_ambiguity_and_retry_after(self):
        for status, ambiguous in [(400, False), (401, False), (408, True), (429, False), (500, True), (302, True)]:
            error = urllib.error.HTTPError("https://secret-url", status, "secret-key", {"Retry-After": "12"}, io.BytesIO(b"private"))
            with mock.patch.object(c, "_transport", side_effect=error) as transport, self.assertRaises(c.CloudClientError) as raised:
                self.aai.submit(self.url)
            self.assertEqual(raised.exception.status_code, status)
            self.assertEqual(raised.exception.ambiguous, ambiguous)
            self.assertEqual(raised.exception.retry_after_seconds, 12)
            self.assertNotIn("secret", str(raised.exception))
            transport.assert_called_once()

    def test_no_redirects_and_no_environment_proxies(self):
        request = urllib.request.Request("https://api.rev.ai/")
        with mock.patch.object(c.urllib.request, "build_opener") as build:
            c._transport(request, 3)
        self.assertEqual(build.call_args.args[0].proxies, {})
        self.assertIsInstance(build.call_args.args[1], c._NoRedirect)
        self.assertIsNone(build.call_args.args[1].redirect_request(request, None, 302, "Found", {}, "https://evil.example"))
        build.return_value.open.assert_called_once_with(request, timeout=3)

    def test_json_rejects_duplicates_nonfinite_invalid_utf8_and_nonobjects(self):
        for body in [b'{"a":1,"a":2}', b'{"x":{"a":1,"a":2}}', b'{"a":NaN}',
                     b'{"a":Infinity}', b'{"a":1e999}', b'[]', b'null', b'{"x":"\xff"}']:
            with self.subTest(body=body), mock.patch.object(c, "_transport", return_value=Response(raw=body)), self.assertRaises(c.CloudClientError) as raised:
                self.aai.submit(self.url)
            self.assertTrue(raised.exception.ambiguous)

    def test_response_limits_lengths_encodings_and_unexpected_status(self):
        for body, headers, status in [(b" " * 33, {}, 200), (b"{}", {"Content-Length": "33"}, 200),
            (b"{}", {"Content-Length": "3"}, 200), (b"{}", {"Content-Length": "invalid"}, 200),
            (b"{}", {"Content-Encoding": "gzip"}, 200), (b"{}", {}, 201)]:
            with mock.patch.object(c, "MAX_RESPONSE_BYTES", 32), mock.patch.object(c, "_transport", return_value=Response(raw=body, headers=headers, status=status)), self.assertRaises(c.CloudClientError):
                self.rev.poll("job")

    def test_validate_job_rejects_identity_state_model_and_human_transcription(self):
        self.assertEqual(c.validate_job("revai", rev_job())["status"], "transcribed")
        for provider, raw, field, value in [("assemblyai", assembly_result(), "status", "unknown"),
            ("assemblyai", assembly_result(), "speech_model_used", "universal-2"),
            ("assemblyai", assembly_result(), "speech_models", ["universal-2"]),
            ("revai", rev_job(), "type", "streaming"), ("revai", rev_job(), "transcriber", "human"),
            ("revai", rev_job(), "language", "de")]:
            raw[field] = value
            with self.subTest(field=field), self.assertRaises(c.CloudClientError):
                c.validate_job(provider, raw)
        with self.assertRaises(c.CloudClientError):
            c.validate_job("revai", rev_job(), expected_job_id="another-job")

    def test_duration_limits_are_strict_not_split_or_fallback(self):
        for provider, maximum in [("assemblyai", 36_000), ("revai", 61_200)]:
            self.assertEqual(c.validate_duration(provider, maximum), maximum)
            for value in [0, -1, maximum + .001, True, float("nan"), "10"]:
                with self.subTest(provider=provider, value=value), self.assertRaises(c.CloudClientError):
                    c.validate_duration(provider, value)

    def test_assembly_normalization_has_generic_labels_segment_timing_and_no_words(self):
        raw = assembly_result()
        before = copy.deepcopy(raw)
        result = c.normalize_result("assemblyai", raw, expected_duration_seconds=10)
        self.assertEqual(raw, before)
        self.assertEqual(result["text"], "Hello.")
        self.assertEqual(result["segments"][0]["start_ms"], 100)
        self.assertEqual(set(result["segments"][0]), {"start_ms", "end_ms", "text", "speaker"})
        self.assertEqual(raw["words"][0]["start"], 100)
        self.assertEqual(result["segments"][0]["speaker"], "SPEAKER_0000")
        self.assertEqual(result["provider_speaker_labels"], {"SPEAKER_0000": "A"})
        self.assertFalse(result["speaker_labels_are_identities"])

    def test_assembly_completed_english_locale_aliases_preserve_raw_language(self):
        baseline = c.normalize_result('assemblyai', assembly_result(), expected_duration_seconds=10)
        for locale in (None, 'en', 'en_us', 'en_uk', 'en_au'):
            raw = assembly_result()
            raw['language_code'] = locale
            before = copy.deepcopy(raw)
            with self.subTest(locale=locale):
                self.assertEqual(c.normalize_result('assemblyai', raw, expected_duration_seconds=10), baseline)
                self.assertEqual(raw, before)
                self.assertEqual(raw['language_code'], locale)
        self.assertEqual(c.assemblyai_options(self.url)['language_code'], 'en')
        for locale in ('de', 'fr', 'ja', 'en_US', 'en-US', 'english', 'en_ca', '', True, 1, []):
            raw = assembly_result()
            raw['language_code'] = locale
            with self.subTest(locale=locale), self.assertRaises(c.CloudClientError):
                c.normalize_result('assemblyai', raw, expected_duration_seconds=10)

    def test_rev_normalization_preserves_punctuation_and_converts_seconds(self):
        raw = rev_result()
        before = copy.deepcopy(raw)
        result = c.normalize_result("revai", raw, expected_duration_seconds=10, job=rev_job())
        self.assertEqual(raw, before)
        self.assertEqual(result["text"], "Hello, world.")
        self.assertEqual(result["segments"][0]["start_ms"], 100)
        self.assertEqual(result["segments"][0]["end_ms"], 1200)
        self.assertEqual(set(result["segments"][0]), {"start_ms", "end_ms", "text", "speaker"})
        self.assertEqual(raw["monologues"][0]["elements"][3]["ts"], .8)
        self.assertEqual(result["provider_speaker_labels"], {"SPEAKER_0000": "0"})
        self.assertEqual(result["model"], "machine")

    def test_assembly_nondiarized_uses_words_and_fabricates_no_speaker(self):
        raw = assembly_result()
        raw["speaker_labels"] = False
        raw.pop("utterances")
        raw["words"][0].pop("speaker")
        result = c.normalize_result("assemblyai", raw, expected_duration_seconds=10, diarization=False)
        self.assertEqual(result["text"], "Hello.")
        self.assertIsNone(result["segments"][0]["speaker"])
        self.assertNotIn("words", result["segments"][0])
        self.assertEqual(result["segments"][0]["start_ms"], 100)
        self.assertEqual(raw["words"][0]["start"], 100)
        self.assertEqual(result["provider_speaker_labels"], {})
        self.assertIs(result["diarization_requested"], False)
        raw.pop("speaker_labels")
        self.assertEqual(c.normalize_result("assemblyai", raw, expected_duration_seconds=10, diarization=False), result)

    def test_assembly_nondiarized_grouping_keeps_segment_bounds_not_word_timestamps(self):
        raw = assembly_result()
        raw["speaker_labels"] = False
        raw["utterances"] = None
        raw["audio_duration"] = 70
        raw["words"] = [{"start": index * 1000, "end": index * 1000 + 400, "text": "word", "confidence": 1}
                        for index in range(70)]
        raw["text"] = " ".join("word" for _ in range(70))
        before = copy.deepcopy(raw)
        result = c.normalize_result("assemblyai", raw, expected_duration_seconds=70, diarization=False)
        self.assertEqual(raw, before)
        self.assertEqual([segment["start_ms"] for segment in result["segments"]], [0, 30_000, 60_000])
        self.assertEqual([segment["end_ms"] for segment in result["segments"]], [29_400, 59_400, 69_400])
        self.assertTrue(all(set(segment) == {"start_ms", "end_ms", "text", "speaker"} for segment in result["segments"]))
        self.assertTrue(all(segment["end_ms"] - segment["start_ms"] <= 30_000 for segment in result["segments"]))
        self.assertTrue(all(segment["speaker"] is None for segment in result["segments"]))
        self.assertEqual(" ".join(segment["text"] for segment in result["segments"]), raw["text"])

    def test_nondiarized_results_reject_option_echo_mismatch(self):
        with self.assertRaises(c.CloudClientError):
            c.normalize_result("assemblyai", assembly_result(), expected_duration_seconds=10, diarization=False)
        for flag, requested in [(False, False), (True, True), (1, False), (0, True)]:
            job = {**rev_job(), "skip_diarization": flag}
            with self.subTest(flag=flag, requested=requested), self.assertRaises(c.CloudClientError):
                c.normalize_result("revai", rev_result(), expected_duration_seconds=10, job=job, diarization=requested)

    def test_rev_nondiarized_preserves_text_but_does_not_turn_placeholder_into_identity(self):
        raw = rev_result()
        job = {**rev_job(), "skip_diarization": True}
        result = c.normalize_result("revai", raw, expected_duration_seconds=10, job=job, diarization=False)
        self.assertEqual(result["text"], "Hello, world.")
        self.assertIsNone(result["segments"][0]["speaker"])
        self.assertEqual(set(result["segments"][0]), {"start_ms", "end_ms", "text", "speaker"})
        self.assertEqual(raw["monologues"][0]["elements"][0]["ts"], .1)
        self.assertEqual(result["provider_speaker_labels"], {})
        self.assertIs(result["diarization_requested"], False)
        raw["monologues"][0].pop("speaker")
        self.assertEqual(c.normalize_result("revai", raw, expected_duration_seconds=10, job=job, diarization=False), result)

    def test_nondiarized_missing_timing_or_reversed_word_order_is_not_fabricated(self):
        raw = assembly_result()
        raw["speaker_labels"] = False
        raw["utterances"] = None
        for words in [None, [], [{"text": "Hello."}],
                      [{"text": "Hello.", "start": 500, "end": 600}, {"text": "Again.", "start": 100, "end": 200}]]:
            raw["words"] = words
            with self.subTest(words=words), self.assertRaises(c.CloudClientError):
                c.normalize_result("assemblyai", raw, expected_duration_seconds=10, diarization=False)

    def test_normalization_rejects_silence_missing_model_diarization_and_duration(self):
        for field, value in [("status", "processing"), ("speech_model_used", None),
                ("speaker_labels", False), ("audio_duration", 20), ("text", ""),
                ("utterances", []), ("language_code", "de")]:
            raw = assembly_result()
            raw[field] = value
            with self.subTest(field=field), self.assertRaises(c.CloudClientError):
                c.normalize_result("assemblyai", raw, expected_duration_seconds=10)
        with self.assertRaises(c.CloudClientError):
            c.normalize_result("revai", {"monologues": []}, expected_duration_seconds=10, job=rev_job())
        with self.assertRaises(c.CloudClientError):
            c.normalize_result("revai", rev_result(), expected_duration_seconds=10, job=None)

    def test_normalization_rejects_invalid_words_timestamps_speakers_confidences(self):
        for field, value in [("start", -1), ("end", 99), ("end", 100_000),
                ("start", float("nan")), ("confidence", 1.1), ("confidence", True),
                ("speaker", "Daniel"), ("speaker", "B"), ("text", "")]:
            raw = assembly_result()
            raw["utterances"][0]["words"][0][field] = value
            with self.subTest(field=field, value=value), self.assertRaises(c.CloudClientError):
                c.normalize_result("assemblyai", raw, expected_duration_seconds=10)

    def test_normalization_rejects_text_utterance_disagreement_and_word_limits(self):
        raw = assembly_result()
        raw["text"] = "Unrelated glossary insertion."
        with self.assertRaises(c.CloudClientError):
            c.normalize_result("assemblyai", raw, expected_duration_seconds=10)
        with mock.patch.object(c, "MAX_WORDS", 0), self.assertRaises(c.CloudClientError):
            c.normalize_result("assemblyai", assembly_result(), expected_duration_seconds=10)
        with mock.patch.object(c, "MAX_WORDS", 1), self.assertRaises(c.CloudClientError):
            c.normalize_result("revai", rev_result(), expected_duration_seconds=10, job=rev_job())

    def test_rev_rejects_unknown_element_and_punctuation_only_monologue(self):
        raw = rev_result()
        raw["monologues"][0]["elements"][0]["type"] = "unknown"
        with self.assertRaises(c.CloudClientError):
            c.normalize_result("revai", raw, expected_duration_seconds=10, job=rev_job())
        raw["monologues"][0]["elements"] = [{"type": "punct", "value": "."}]
        with self.assertRaises(c.CloudClientError):
            c.normalize_result("revai", raw, expected_duration_seconds=10, job=rev_job())


if __name__ == "__main__":
    unittest.main()
