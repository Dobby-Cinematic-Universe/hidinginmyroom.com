from __future__ import annotations

import hashlib
import io
import os
from pathlib import Path
import tempfile
import unittest
import urllib.error
import wave
from unittest import mock

from pipeline import salad_transcription_temp_upload as temp_upload
from pipeline.salad_transcription_client import CloudClientError


class Response(io.BytesIO):
    def __init__(self, body, *, status=200, headers=None):
        super().__init__(body)
        self.status = status
        self.headers = headers or {}
        self.read_sizes = []

    def read(self, count=-1):
        self.read_sizes.append(count)
        return super().read(count)


class TempUploadTests(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.root = Path(self.temporary.name)
        self.path = self.root / "private-recording-name.wav"
        with wave.open(str(self.path), "wb") as handle:
            handle.setparams((1, 2, 16000, 16000, "NONE", "not compressed"))
            handle.writeframes(b"\0\0" * 16000)
        self.path.chmod(0o600)
        self.body = self.path.read_bytes()
        self.digest = hashlib.sha256(self.body).hexdigest()
        self.url = "https://temp.sh/AbCdE/audio.wav"

    def upload(self, path=None, **kwargs):
        return temp_upload.upload_temp_file(path or self.path,
            expected_sha256=kwargs.get("expected_sha256", self.digest),
            expected_byte_count=kwargs.get("expected_byte_count", len(self.body)))

    def response_url(self, url=None):
        body = ((url or self.url) + "\n").encode()
        return Response(body, headers={"Content-Type": "text/plain", "Content-Length": str(len(body))})

    def response_range(self):
        prefix = self.body[:4096]
        return Response(prefix, status=206, headers={"Content-Type": "audio/wav",
            "Content-Range": f"bytes 0-{len(prefix)-1}/{len(self.body)}", "Content-Length": str(len(prefix))})

    def test_plain_url_range_verification_multipart_and_no_credentials(self):
        with mock.patch.dict(os.environ, {"SALAD_API_KEY": "do-not-forward"}):
            with mock.patch.object(temp_upload, "_transport", side_effect=[self.response_url(), self.response_range()]) as transport:
                self.assertEqual(self.upload(), self.url)
        self.assertEqual(transport.call_count, 2)
        post, timeout = transport.call_args_list[0].args
        self.assertEqual(post.full_url, "https://temp.sh/upload")
        self.assertEqual(post.get_method(), "POST")
        self.assertEqual(timeout, 60)
        self.assertIn(b'name="file"; filename="audio.wav"', post.data)
        self.assertIn(b"Content-Type: audio/wav", post.data)
        self.assertIn(self.body, post.data)
        self.assertNotIn(b"private-recording-name", post.data)
        for call in transport.call_args_list:
            request = call.args[0]
            self.assertIsNone(request.get_header("Authorization"))
            self.assertIsNone(request.get_header("Salad-api-key"))
            self.assertNotIn("do-not-forward", str(request.headers))
        self.assertEqual(transport.call_args_list[1].args[0].get_header("Range"), "bytes=0-4095")

    def test_range_ignored_with_matching_full_size_is_supported_and_read_is_bounded(self):
        response = Response(self.body, headers={"Content-Type": "audio/wav", "Content-Length": str(len(self.body))})
        with mock.patch.object(temp_upload, "_transport", side_effect=[self.response_url(), response]):
            self.assertEqual(self.upload(), self.url)
        self.assertEqual(response.read_sizes, [64 * 1024])

    def test_ignored_range_allows_large_declared_wav_without_reading_entire_file(self):
        buffer = io.BytesIO()
        with wave.open(buffer, "wb") as handle:
            handle.setparams((1, 2, 16000, 100000, "NONE", "not compressed"))
            handle.writeframes(b"\0\0" * 100000)
        large = buffer.getvalue()
        response = Response(large, headers={"Content-Type": "audio/wav", "Content-Length": str(len(large))})
        with mock.patch.object(temp_upload, "_transport", return_value=response):
            self.assertEqual(temp_upload._verify_download(self.url, large), self.url)
        self.assertGreater(len(large), temp_upload.MAX_INSPECTION_BYTES)
        self.assertEqual(response.read_sizes, [temp_upload.MAX_INSPECTION_BYTES])

    def test_one_html_landing_hop_follows_only_observed_download_link(self):
        page = b'<html><a href="/">Home</a><a href="/files/AbCdE/audio.wav?download=1">Click here to download</a></html>'
        def html():
            return Response(page, headers={"Content-Type": "text/html", "Content-Length": str(len(page))})
        direct = "https://temp.sh/files/AbCdE/audio.wav?download=1"
        with mock.patch.object(temp_upload, "_transport", side_effect=[self.response_url(), html(), html(), self.response_range()]) as transport:
            self.assertEqual(self.upload(), direct)
        self.assertEqual([call.args[0].full_url for call in transport.call_args_list],
                         [temp_upload.UPLOAD_URL, self.url, self.url, direct])
        self.assertIsNone(transport.call_args_list[2].args[0].get_header("Range"))

    def test_download_attribute_and_repeated_same_url_are_supported(self):
        page = b'<html><a href="/files/audio.wav" download>WAV</a><a href="/files/audio.wav">Download</a></html>'
        def html():
            return Response(page, headers={"Content-Type": "text/html"})
        with mock.patch.object(temp_upload, "_transport", side_effect=[self.response_url(), html(), html(), self.response_range()]):
            self.assertEqual(self.upload(), "https://temp.sh/files/audio.wav")

    def test_upload_return_url_must_be_one_plain_https_same_origin_url(self):
        invalid = ["http://temp.sh/a/file.wav", "https://www.temp.sh/a/file.wav", "https://evil.example/file.wav",
                   "https://user@temp.sh/a/file.wav", "https://temp.sh:443/a/file.wav", self.url + "#fragment",
                   "https://temp.sh/../private", "https://temp.sh/%2e%2e/private", "https://temp.sh/%0d%0aheader",
                   '{"url":"' + self.url + '"}', "<html>upload completed</html>", self.url + "\n" + self.url]
        for url in invalid:
            with self.subTest(url=url), mock.patch.object(temp_upload, "_transport", return_value=self.response_url(url)) as transport:
                with self.assertRaises(CloudClientError) as raised:
                    self.upload()
                self.assertTrue(raised.exception.ambiguous)
                self.assertNotIn("private", str(raised.exception))
                transport.assert_called_once()

    def test_html_wrong_host_ambiguous_missing_links_and_second_hop_are_rejected(self):
        pages = [b'<html><a href="https://evil.example/a.wav">Download</a></html>',
                 b'<html><a href="/first.wav">Download</a><a href="/second.wav" download>Audio</a></html>',
                 b'<html><script>location="/audio.wav"</script></html>',
                 b'<html><a href="javascript:alert(1)">Download</a></html>']
        for page in pages:
            with self.subTest(page=page):
                def html():
                    return Response(page, headers={"Content-Type": "text/html"})
                with mock.patch.object(temp_upload, "_transport", side_effect=[self.response_url(), html(), html()]) as transport:
                    with self.assertRaises(CloudClientError):
                        self.upload()
                self.assertEqual(transport.call_count, 3)
        page = b'<html><a href="/files/audio.wav">Download</a></html>'
        def html():
            return Response(page, headers={"Content-Type": "text/html"})
        with mock.patch.object(temp_upload, "_transport", side_effect=[self.response_url(), html(), html(), html()]) as transport:
            with self.assertRaises(CloudClientError):
                self.upload()
        self.assertEqual(transport.call_count, 4)

    def test_remote_wav_magic_prefix_and_reported_sizes_must_match(self):
        responses = [
            Response(b"not a WAV", headers={"Content-Length": str(len(self.body))}),
            Response(b"RIFF" + b"x" * 4092, status=206, headers={"Content-Range": f"bytes 0-4095/{len(self.body)}"}),
            Response(self.body[:4096], status=206, headers={"Content-Range": f"bytes 0-4095/{len(self.body)+1}"}),
            Response(self.body[:4096], status=206, headers={}),
            Response(self.body[:4096], status=206, headers={"Content-Range": f"bytes 1-4096/{len(self.body)}"}),
            Response(self.body[:4096], headers={}),
            Response(self.body[:4096], headers={"Content-Length": str(len(self.body)+1)}),
            Response(self.body[:4096], headers={"Content-Length": "invalid"}),
        ]
        for response in responses:
            with self.subTest(response=response.headers), mock.patch.object(temp_upload, "_transport", side_effect=[self.response_url(), response]):
                with self.assertRaises(CloudClientError):
                    self.upload()

    def test_http_and_transport_errors_are_sanitized_not_retried(self):
        for error in [urllib.error.URLError("private-recording-name secret-token"),
                      urllib.error.HTTPError(self.url, 503, "secret-token", {"Retry-After": "10"}, io.BytesIO()),
                      urllib.error.HTTPError(self.url, 302, "redirect", {"Location": "https://evil.example"}, io.BytesIO())]:
            with mock.patch.object(temp_upload, "_transport", side_effect=error) as transport:
                with self.assertRaises(CloudClientError) as raised:
                    self.upload()
            self.assertNotIn("secret-token", str(raised.exception))
            self.assertNotIn("private-recording-name", str(raised.exception))
            self.assertTrue(raised.exception.ambiguous)
            transport.assert_called_once()

    def test_plain_url_and_html_response_limits_are_enforced(self):
        with mock.patch.object(temp_upload, "_transport", return_value=Response(b"x" * (temp_upload.MAX_URL_RESPONSE_BYTES + 1))) as transport:
            with self.assertRaises(CloudClientError):
                self.upload()
        transport.assert_called_once()
        page = b"<html>" + b"x" * temp_upload.MAX_INSPECTION_BYTES
        def html():
            return Response(page, headers={"Content-Type": "text/html"})
        with mock.patch.object(temp_upload, "_transport", side_effect=[self.response_url(), html(), html()]) as transport:
            with self.assertRaises(CloudClientError):
                self.upload()
        self.assertEqual(transport.call_count, 3)

    def test_changed_size_hash_and_unsafe_files_stop_before_upload(self):
        with mock.patch.object(temp_upload, "_transport") as transport:
            for kwargs in [{"expected_sha256": "0" * 64}, {"expected_sha256": "invalid"},
                           {"expected_byte_count": len(self.body)+1}, {"expected_byte_count": True},
                           {"expected_byte_count": 300_000_001}]:
                with self.subTest(kwargs=kwargs), self.assertRaises(CloudClientError):
                    self.upload(**kwargs)
            symlink = self.root / "linked.wav"
            symlink.symlink_to(self.path)
            with self.assertRaises(CloudClientError):
                self.upload(symlink)
            parent_link = self.root / "linked-directory"
            parent_link.symlink_to(self.root, target_is_directory=True)
            with self.assertRaises(CloudClientError):
                self.upload(parent_link / self.path.name)
            link = self.root / "hardlinked.wav"
            os.link(self.path, link)
            with self.assertRaises(CloudClientError):
                self.upload()
            link.unlink()
            self.path.chmod(0o666)
            with self.assertRaises(CloudClientError):
                self.upload()
            self.path.chmod(0o600)
            with mock.patch.object(temp_upload.os, "getuid", return_value=os.getuid()+1):
                with self.assertRaises(CloudClientError):
                    self.upload()
            self.path.write_bytes(b"x" * len(self.body))
            with self.assertRaises(CloudClientError):
                self.upload()
        transport.assert_not_called()

    def test_inherited_transport_disables_redirects_and_environment_proxies(self):
        # This adapter deliberately shares only the auth-free transport, not a
        # SaladClient object, so provider API keys cannot reach temp.sh.
        from pipeline import salad_transcription_client as salad_client
        self.assertIs(temp_upload._transport, salad_client._transport)
        with mock.patch.object(salad_client.urllib.request, "build_opener") as build:
            temp_upload._transport(urllib.request.Request("https://temp.sh/upload"), 60)
        handlers = build.call_args.args
        self.assertEqual(handlers[0].proxies, {})
        self.assertIsInstance(handlers[1], salad_client._NoRedirect)


if __name__ == "__main__":
    unittest.main()
