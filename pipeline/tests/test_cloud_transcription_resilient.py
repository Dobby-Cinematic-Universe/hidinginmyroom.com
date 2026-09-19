"""Safe cloud retries, deadlines and durable paid-state integration; no network."""
from contextlib import nullcontext
from pathlib import Path
from types import SimpleNamespace
import unittest
from unittest.mock import Mock, patch

from pipeline import cloud_transcription_resilient as resilient
from pipeline.tests import test_cloud_transcription_runtime as fixtures

cloud = resilient.cloud


class Clock:
    def __init__(self):
        self.now = 0.0
        self.sleeps = []
        self.on_sleep = lambda: None

    def monotonic(self):
        return self.now

    def sleep(self, seconds):
        self.sleeps.append(seconds)
        self.now += seconds
        self.on_sleep()


def http_error(code=503, **kwargs):
    return cloud.clients.CloudClientError("cloud request failed with an HTTP status", status_code=code, **kwargs)


class RetryTests(unittest.TestCase):
    def setUp(self):
        self.clock, self.events, self.stopped = Clock(), [], False
        self.raw = SimpleNamespace(**{name: Mock(return_value={"id": "success"})
                                    for name in ("upload", "poll", "transcript", "submit", "submit_file")})

    def proxy(self, provider="assemblyai", **kwargs):
        return resilient.RetryingClient(provider, self.raw, deadline=kwargs.pop("deadline", 10000),
            stopping=lambda: self.stopped, on_event=self.events.append,
            clock=self.clock.monotonic, sleep=self.clock.sleep, jitter=lambda: 0, **kwargs)

    def test_upload_retries_transient_failures_without_touching_paid_methods(self):
        self.raw.upload.side_effect = [http_error(), cloud.clients.CloudClientError(
            "cloud request transport failed", ambiguous=True), {"upload_url": "private"}]
        result = self.proxy().upload("PRIVATE PATH", expected_sha256="a" * 64)
        self.assertEqual(result, {"upload_url": "private"})
        self.assertEqual(self.raw.upload.call_count, 3)
        self.raw.submit.assert_not_called()
        self.raw.submit_file.assert_not_called()
        self.assertEqual([event["delay_seconds"] for event in self.events], [5, 10])
        self.assertEqual(self.clock.now, 15)
        self.assertNotIn("PRIVATE", repr(self.events))

    def test_get_poll_and_rev_transcript_honor_retry_after(self):
        for operation, provider in (("poll", "assemblyai"), ("transcript", "revai")):
            with self.subTest(operation=operation):
                self.clock.now = 0
                getattr(self.raw, operation).side_effect = [http_error(429, retry_after_seconds=12), {"id": "done"}]
                result = getattr(self.proxy(provider), operation)("PRIVATE JOB ID")
                self.assertEqual(result, {"id": "done"})
                self.assertEqual(self.clock.now, 12)
                self.assertNotIn("PRIVATE", repr(self.events))

    def test_auth_invalid_request_and_normalization_errors_never_retry(self):
        errors = [http_error(code) for code in (400, 401, 403, 404, 409, 413, 422)]
        errors += [cloud.clients.CloudClientError(message, ambiguous=True) for message in (
            "cloud response is not a strict JSON object", "upload media changed during transfer",
            "upload requires a stable owned regular file matching its size limit and digest")]
        errors.append(cloud.clients.CloudClientError("cloud request transport failed", status_code=403))
        for error in errors:
            with self.subTest(error=str(error), status=error.status_code):
                self.raw.upload.reset_mock()
                self.raw.upload.side_effect = error
                with self.assertRaises(cloud.clients.CloudClientError) as caught:
                    self.proxy().upload("private", expected_sha256="a" * 64)
                self.assertIs(caught.exception, error)
                self.assertEqual(self.raw.upload.call_count, 1)
        self.assertEqual(self.events, [])
        self.assertEqual(self.clock.sleeps, [])

    def test_untrusted_response_is_preserved_and_never_retried(self):
        response = {"secret": "PRIVATE PROVIDER BODY"}
        error = cloud.clients.CloudClientError("cloud request transport failed", ambiguous=True, response=response)
        self.raw.upload.side_effect = error
        with self.assertRaises(cloud.clients.CloudClientError) as caught:
            self.proxy().upload("private", expected_sha256="a" * 64)
        self.assertIs(caught.exception.response, response)
        self.assertEqual(self.raw.upload.call_count, 1)
        self.assertEqual(self.events, [])

    def test_both_paid_methods_delegate_exactly_once_even_for_transient_errors(self):
        for name, provider in (("submit", "assemblyai"), ("submit_file", "revai")):
            with self.subTest(name=name):
                error = http_error(503, ambiguous=True)
                getattr(self.raw, name).side_effect = error
                with self.assertRaises(cloud.clients.CloudClientError) as caught:
                    getattr(self.proxy(provider), name)("PRIVATE INPUT", diarization=True)
                self.assertIs(caught.exception, error)
                self.assertEqual(getattr(self.raw, name).call_count, 1)
        self.assertEqual(self.events, [])
        self.assertEqual(self.clock.sleeps, [])

    def test_rev_upload_is_not_misclassified_as_unbillable(self):
        with self.assertRaises(resilient.ResilientError):
            self.proxy("revai").upload("private", expected_sha256="a" * 64)
        self.raw.upload.assert_not_called()

    def test_safe_retry_window_has_four_attempts_then_dedicated_exhaustion(self):
        self.raw.poll.side_effect = http_error(408)
        with self.assertRaises(resilient.SafeOperationExhausted) as caught:
            self.proxy().poll("private")
        self.assertEqual(caught.exception.operation, "poll")
        self.assertEqual(caught.exception.attempts, 4)
        self.assertEqual(self.raw.poll.call_count, 4)
        self.assertEqual(self.clock.now, 35)

    def test_long_retry_after_goes_to_cooldown_without_early_retry(self):
        self.raw.poll.side_effect = http_error(429, retry_after_seconds=600)
        with self.assertRaises(resilient.SafeOperationExhausted) as caught:
            self.proxy().poll("private")
        self.assertEqual(caught.exception.attempts, 1)
        self.assertEqual(self.raw.poll.call_count, 1)
        self.assertEqual(self.clock.now, 0)

    def test_pause_and_deadline_interrupt_backoff_before_another_attempt(self):
        self.raw.upload.side_effect = http_error()
        self.clock.on_sleep = lambda: setattr(self, "stopped", True)
        with self.assertRaises(resilient.RetryInterrupted) as caught:
            self.proxy().upload("private", expected_sha256="a" * 64)
        self.assertEqual(caught.exception.reason, "paused")
        self.assertEqual(self.raw.upload.call_count, 1)
        self.stopped = False
        self.clock.on_sleep = lambda: None
        with self.assertRaises(resilient.RetryInterrupted) as caught:
            self.proxy(deadline=self.clock.now + 2).upload("private", expected_sha256="a" * 64)
        self.assertEqual(caught.exception.reason, "runtime_limit")
        self.assertEqual(self.raw.upload.call_count, 2)

    def test_error_metadata_cannot_expose_message_response_or_untyped_values(self):
        error = cloud.clients.CloudClientError("PRIVATE MESSAGE", status_code="PRIVATE STATUS",
            ambiguous="PRIVATE FLAG", retry_after_seconds=float("inf"), response={"key": "PRIVATE KEY"})
        self.assertEqual(resilient.error_metadata(error),
                         {"status_code": None, "ambiguous": None, "retry_after_seconds": None})


class RunnerTests(unittest.TestCase):
    def setUp(self):
        self.case = fixtures.CloudRuntimeTests()
        self.case.setUp()
        self.addCleanup(self.case.doCleanups)
        self.clock, self.events = Clock(), []
        socket_guard = patch("socket.socket", side_effect=AssertionError("network forbidden"))
        socket_guard.start()
        self.addCleanup(socket_guard.stop)

    def run_lane(self, **kwargs):
        values = dict(allow_paid_api=True, budget_microusd=10_000_000, max_runtime_seconds=600,
            env_file=str(self.case.root / ".env"), cooldown_seconds=15, poll_seconds=15,
            stopping=lambda: self.case.stopped, on_event=self.events.append,
            client_factory=lambda provider: self.case.clients[provider],
            clock=self.clock.monotonic, sleep=self.clock.sleep, jitter=lambda: 0)
        values.update(kwargs)
        with patch.object(cloud.media, "prepare", side_effect=self.case.audio_builder):
            return resilient.run(self.case.ref, **values)

    def test_outer_upload_cooldown_preserves_previous_paid_receipt_and_never_repeats_post(self):
        case = self.case
        case.add_recording()
        case.add_recording()
        case.prepare()
        raw = case.clients["assemblyai"]
        original_upload = raw.upload
        failures = [0]
        def upload(*args, **kwargs):
            if len(raw.jobs) == 1 and failures[0] < 4:
                failures[0] += 1
                raise http_error()
            return original_upload(*args, **kwargs)
        raw.after_post = lambda: setattr(case, "stopped", len(raw.jobs) == 2)
        with patch.object(raw, "upload", side_effect=upload):
            result = self.run_lane()
        self.assertEqual(result["state"], "paused")
        self.assertEqual(result["exhausted_safe_retry_windows"], 1)
        self.assertEqual(len(case.submits()), 2)
        for index in (0, 1):
            self.assertTrue((case.folder(index) / "intent.json").is_file())
            self.assertTrue((case.folder(index) / "submission.json").is_file())
        self.assertEqual(failures[0], 4)
        self.assertTrue(any(event.get("event") == "safe_operation_cooldown" for event in self.events))

    def test_outer_get_cooldown_collects_existing_job_without_new_paid_submission(self):
        case = self.case
        case.prepare()
        case.cycle()
        raw = case.clients["assemblyai"]
        raw.poll_status = "completed"
        original_poll = raw.poll
        failures = [0]
        def poll(identifier):
            if failures[0] < 4:
                failures[0] += 1
                raise http_error(429)
            return original_poll(identifier)
        with patch.object(raw, "poll", side_effect=poll):
            result = self.run_lane()
        self.assertEqual(result["state"], "completed")
        self.assertEqual(len(case.submits()), 1)
        self.assertTrue((case.folder() / "completion.json").is_file())

    def test_ambiguous_paid_post_escapes_runner_and_resume_requires_reconciliation(self):
        case = self.case
        case.prepare()
        raw = case.clients["assemblyai"]
        error = cloud.clients.CloudClientError("cloud request transport failed", ambiguous=True)
        raw.post_error = error
        with self.assertRaises(cloud.clients.CloudClientError) as caught:
            self.run_lane()
        self.assertIs(caught.exception, error)
        self.assertEqual(len(case.submits()), 1)
        self.assertTrue((case.folder() / "intent.json").is_file())
        self.assertFalse((case.folder() / "submission.json").exists())
        raw.post_error = None
        result = self.run_lane()
        self.assertEqual(result["state"], "reconciliation_required")
        self.assertEqual(len(case.submits()), 1)

    def test_cooldown_honors_long_retry_after_until_finite_deadline(self):
        case = self.case
        case.prepare()
        raw = case.clients["assemblyai"]
        with patch.object(raw, "upload", side_effect=http_error(429, retry_after_seconds=600)) as upload:
            result = self.run_lane(max_runtime_seconds=100)
        self.assertEqual(result["state"], "runtime_limit")
        self.assertEqual(upload.call_count, 1)
        self.assertEqual(self.clock.now, 100)
        self.assertEqual(case.submits(), [])

    def test_permission_budget_runtime_and_concurrency_reject_before_cycle(self):
        self.case.prepare()
        for options in ({"allow_paid_api": False}, {"budget_microusd": 150_000_001},
                        {"max_runtime_seconds": 86401}, {"max_active": 5}):
            with self.subTest(options=options), patch.object(cloud, "cycle") as cycle:
                with self.assertRaises(RuntimeError):
                    self.run_lane(**options)
                cycle.assert_not_called()

    def test_wrong_runner_hash_prevents_release_activation_and_client_construction(self):
        runtime = Path(cloud.__file__).parent.parent
        if Path.cwd() != runtime:
            self.skipTest("CLI runtime check requires matching runtime cwd")
        argv = ["--plan", "/private/plan.json", "--expected-sha256", "a" * 64,
                "--execution-release", "/private/release.json", "--execution-release-sha256", "b" * 64,
                "--runtime-path", str(runtime), "--runner-sha256", "0" * 64, "--env-file", "/private/.env"]
        with patch.object(resilient.release, "activate") as activate, \
                patch.object(resilient, "run") as run, patch.object(resilient, "_emit"):
            self.assertEqual(resilient.main(argv), 2)
        activate.assert_not_called()
        run.assert_not_called()


if __name__ == "__main__":
    unittest.main()
