"""Synthetic-only dotenv and lazy credential tests; never read the real .env."""

from __future__ import annotations

import io
import os
from pathlib import Path
import tempfile
from types import SimpleNamespace
import unittest
from unittest import mock

from pipeline import transcript_summary as runner
from pipeline import transcript_summary_env as env
from pipeline import transcript_summary_core as core
from pipeline.tests import test_transcript_summary as existing
from pipeline.tests.test_transcript_summary_sonnet import AnthropicService


class EnvParserTests(unittest.TestCase):
    def test_common_assignments_quotes_comments_and_crlf(self):
        raw = (b"# header\r\n\r\n GEMINI_API_KEY = bare-key # comment\r\n"
               b"export ANTHROPIC_API_KEY='single-key'\r\n"
               b'\t export OPENAI_API_KEY = "double-key" \t# comment\r\n')
        self.assertEqual(env.parse_api_keys(raw), {
            "GEMINI_API_KEY": "bare-key", "ANTHROPIC_API_KEY": "single-key", "OPENAI_API_KEY": "double-key"})

    def test_only_supported_names_are_returned(self):
        self.assertEqual(env.parse_api_keys(
            b"UNRELATED='unterminated\nUNRELATED=second\nnot an assignment\n"
            b"PATH=$(no-execution)\nGEMINI_API_KEY_SUFFIX=other\nOPENAI_API_KEY=needed\n"),
            {"OPENAI_API_KEY": "needed"})

    def test_empty_values_hashes_and_literal_shell_syntax(self):
        self.assertEqual(env.parse_api_keys(b"GEMINI_API_KEY=\nANTHROPIC_API_KEY=''\nOPENAI_API_KEY= # empty\n"),
                         {name: "" for name in env.API_KEY_NAMES})
        self.assertEqual(env.parse_api_keys(b'GEMINI_API_KEY="a#b"\nOPENAI_API_KEY=token#hash\n'),
                         {"GEMINI_API_KEY": "a#b", "OPENAI_API_KEY": "token#hash"})
        literal = '$(never-run)${OTHER_KEY}`also-never-run`'
        before = dict(os.environ)
        self.assertEqual(env.parse_api_keys(('GEMINI_API_KEY="' + literal + '"\n').encode()),
                         {"GEMINI_API_KEY": literal})
        self.assertEqual(dict(os.environ), before)

    def test_quoted_escapes_are_data(self):
        self.assertEqual(env.parse_api_keys(b'GEMINI_API_KEY="a\\\"b\\\\c\\n\\t"\n'),
                         {"GEMINI_API_KEY": 'a"b\\c\n\t'})
        self.assertEqual(env.parse_api_keys(b"GEMINI_API_KEY='a\\'b\\\\c'\n"),
                         {"GEMINI_API_KEY": "a'b\\c"})
        self.assertEqual(env.parse_api_keys(b'GEMINI_API_KEY="a\\$OTHER"\n'),
                         {"GEMINI_API_KEY": "a\\$OTHER"})

    def test_supported_malformed_or_duplicate_entries_are_redacted(self):
        for line in (b"GEMINI_API_KEY private-secret", b'GEMINI_API_KEY="private-secret',
                     b"GEMINI_API_KEY='private-secret' trailing", b"GEMINI_API_KEY='private-secret'#comment",
                     b"GEMINI_API_KEY=private\x00secret", b"GEMINI_API_KEY=private\rsecret",
                     b"GEMINI_API_KEY=one\nGEMINI_API_KEY=private-secret",
                     b"export GEMINI_API_KEY=one\nexport GEMINI_API_KEY=private-secret"):
            with self.subTest(line=line), self.assertRaises(env.EnvFileError) as error:
                env.parse_api_keys(b"# first line\n" + line)
            self.assertIn("line ", str(error.exception))
            self.assertNotIn("private", str(error.exception))
            self.assertNotIn("one", str(error.exception))

    def test_utf8_and_size_are_bounded(self):
        self.assertEqual(env.parse_api_keys(b"#" + b"x" * (env.MAX_ENV_BYTES - 1)), {})
        for raw in (b"\xff", b"x" * (env.MAX_ENV_BYTES + 1), "not-bytes", None):
            with self.assertRaises(env.EnvFileError):
                env.parse_api_keys(raw)


class EnvFileTests(unittest.TestCase):
    def setUp(self):
        temporary = tempfile.TemporaryDirectory()
        self.addCleanup(temporary.cleanup)
        self.root = Path(temporary.name)
        self.default = self.root / ".env"
        patcher = mock.patch.dict(os.environ, {}, clear=True)
        patcher.start()
        self.addCleanup(patcher.stop)

    def file(self, raw=b"GEMINI_API_KEY=synthetic-file-key\n", *, path=None, mode=0o600):
        path = self.default if path is None else path
        if path.exists():
            path.chmod(0o600)
        path.write_bytes(raw)
        path.chmod(mode)
        return path

    def key(self, name="GEMINI_API_KEY", **kwargs):
        return env.api_key(name, default_path=self.default, **kwargs)

    def test_default_and_explicit_file_keys_do_not_mutate_environment(self):
        self.file()
        self.assertEqual(self.key(), "synthetic-file-key")
        explicit = self.file(b"GEMINI_API_KEY=explicit-key\n", path=self.root / "other.env")
        self.assertEqual(self.key(env_file=explicit), "explicit-key")
        self.assertEqual(dict(os.environ), {})
        self.assertIsNone(self.key("ANTHROPIC_API_KEY"))

    def test_exported_variable_including_empty_precedes_any_file_access(self):
        for value in ("exported-key", ""):
            with mock.patch.dict(os.environ, {"GEMINI_API_KEY": value}), mock.patch.object(
                    env, "_read_env", side_effect=AssertionError("No credential file access")) as read:
                self.assertEqual(self.key(env_file=self.root / "does-not-exist"), value)
                read.assert_not_called()

    def test_only_relevant_exported_key_skips_file(self):
        self.file()
        with mock.patch.dict(os.environ, {"ANTHROPIC_API_KEY": "other-provider-key"}):
            self.assertEqual(self.key(), "synthetic-file-key")

    def test_missing_default_is_optional_but_explicit_missing_fails(self):
        self.assertIsNone(self.key())
        with self.assertRaisesRegex(env.EnvFileError, "explicit.*does not exist"):
            self.key(env_file=self.default)

    def test_private_current_user_owned_regular_file_required(self):
        for mode in (0o400, 0o600):
            self.file(mode=mode)
            self.assertEqual(self.key(), "synthetic-file-key")
        for mode in (0o644, 0o640, 0o604, 0o660, 0o700):
            self.file(mode=mode)
            with self.assertRaisesRegex(env.EnvFileError, "chmod 600"):
                self.key()
        self.file()
        with mock.patch.object(env.os, "getuid", return_value=os.getuid() + 1):
            with self.assertRaisesRegex(env.EnvFileError, "current user"):
                self.key()

    def test_symlink_leaf_and_ancestor_rejected_without_read(self):
        real = self.file(path=self.root / "real.env")
        self.default.symlink_to(real)
        directory = self.root / "directory"
        directory.mkdir()
        inside = self.file(path=directory / "keys.env")
        link = self.root / "directory-link"
        link.symlink_to(directory, target_is_directory=True)
        with mock.patch.object(env.os, "pread", side_effect=AssertionError("No unsafe read")) as read:
            for path in (self.default, link / inside.name):
                with self.assertRaisesRegex(env.EnvFileError, "symlinks"):
                    self.key(env_file=path)
            read.assert_not_called()

    def test_fifo_directory_and_hardlink_rejected_before_read(self):
        fifo = self.root / "pipe"
        os.mkfifo(fifo, 0o600)
        directory = self.root / "directory"
        directory.mkdir()
        original = self.file(path=self.root / "keys")
        alias = self.root / "hardlink"
        os.link(original, alias)
        with mock.patch.object(env.os, "pread", side_effect=AssertionError("No unsafe read")) as read:
            for path in (fifo, directory, alias):
                with self.assertRaisesRegex(env.EnvFileError, "regular file"):
                    self.key(env_file=path)
            read.assert_not_called()

    def test_size_change_and_short_reads_rejected(self):
        self.file(b"x" * (env.MAX_ENV_BYTES + 1))
        with mock.patch.object(env.os, "pread", side_effect=AssertionError("No oversized read")):
            with self.assertRaisesRegex(env.EnvFileError, "size limit"):
                self.key()
        self.file()
        with mock.patch.object(env.os, "pread", return_value=b"short"):
            with self.assertRaisesRegex(env.EnvFileError, "changed"):
                self.key()
        before = self.default.stat()
        names = ("st_dev", "st_ino", "st_mode", "st_uid", "st_nlink", "st_size", "st_mtime_ns", "st_ctime_ns")
        changed = SimpleNamespace(**{name: getattr(before, name) for name in names})
        changed.st_mtime_ns += 1
        with mock.patch.object(env.os, "fstat", side_effect=[before, changed]):
            with self.assertRaisesRegex(env.EnvFileError, "changed"):
                self.key()

    def test_relative_paths_and_no_shell_or_user_expansion(self):
        self.file()
        relative = os.path.relpath(self.default)
        self.assertEqual(self.key(env_file=relative), "synthetic-file-key")
        with self.assertRaises(env.EnvFileError) as error:
            self.key(env_file=self.root / "$(private-command).env")
        self.assertNotIn("private-command", str(error.exception))

    def test_api_client_default_explicit_precedence_and_redacted_missing_key(self):
        with mock.patch.object(runner, "ROOT", self.root):
            with self.assertRaisesRegex(runner.Error, "GEMINI_API_KEY is required"):
                runner.api_client("gemini")
            self.file()
            self.assertEqual(runner.api_client("gemini")._api_key, "synthetic-file-key")
            with mock.patch.dict(os.environ, {"GEMINI_API_KEY": "exported-key"}):
                self.assertEqual(runner.api_client("gemini", env_file=self.root / "missing")._api_key, "exported-key")
            self.file(b'GEMINI_API_KEY="private bad key"\n')
            with self.assertRaises(runner.client_module.BatchClientError) as error:
                runner.api_client("gemini")
            self.assertNotIn("private", str(error.exception))


class LazyEnvIntegrationTests(unittest.TestCase):
    file = existing.SummaryRunnerTests.file
    source = existing.SummaryRunnerTests.source
    plan = existing.SummaryRunnerTests.plan
    prepare = existing.SummaryRunnerTests.prepare
    submit = existing.SummaryRunnerTests.submit
    finish = existing.SummaryRunnerTests.finish

    def setUp(self):
        existing.SummaryRunnerTests.setUp(self)
        for patcher in (mock.patch.object(runner, "ROOT", self.root),
                        mock.patch.dict(os.environ, {}, clear=True)):
            patcher.start()
            self.addCleanup(patcher.stop)

    def test_offline_commands_injected_clients_and_admission_failures_never_read_env(self):
        with mock.patch.object(env, "_read_env", side_effect=AssertionError("No secret file read")) as read:
            self.plan(approved=False)
            wave, service, _ = self.prepare()
            runner.status_plan(*self.args, phase="transcripts")
            runner.export_plan(*self.args, phase="transcripts")
            with self.assertRaises(runner.Error):
                runner.submit_wave(*self.args, wave["wave_id"], allow_paid_api=True,
                                   env_file=self.root / "missing.env")
            self.plan()
            wave, service, _ = self.prepare()
            self.finish(wave, service)
            runner.prepare_plan(*self.args, retry_wave=wave["wave_id"])
            self.assertEqual(runner.poll_wave(*self.args, wave["wave_id"],
                             env_file=self.root / "missing.env")["state"], "already_collected")
            read.assert_not_called()

    def test_actual_client_need_loads_only_then_and_keeps_credentials_out_of_artifacts(self):
        self.plan()
        wave, service, folder = self.prepare()
        path = self.root / "synthetic-api-keys.env"
        path.write_text("GEMINI_API_KEY=synthetic-file-secret\n", encoding="utf-8")
        path.chmod(0o600)
        with mock.patch.object(runner.client_module, "GeminiBatchClient", return_value=service.api) as factory:
            result = runner.submit_wave(*self.args, wave["wave_id"], allow_paid_api=True, env_file=path)
            self.assertEqual(result["state"], "submitted")
            factory.assert_called_once_with("synthetic-file-secret")
        for artifact in folder.iterdir():
            if artifact.is_file():
                raw = artifact.read_bytes()
                self.assertNotIn(b"synthetic-file-secret", raw)
                self.assertNotIn(b"synthetic-api-keys.env", raw)
        self.assertEqual(dict(os.environ), {})

    def test_cached_reconciliation_and_collection_ignore_missing_explicit_file(self):
        self.plan(config={**core.DEFAULT_CONFIG, "transcript_profile": "anthropic_sonnet_batch"})
        prepared = runner.prepare_plan(*self.args, phase="transcripts")
        folder = Path(self.request["state_root"]) / "waves" / prepared["wave_id"]
        wave = runner.read(runner.binding(folder / "wave.json"))
        service = AnthropicService(wave)
        service.fail_post = True
        with self.assertRaises(runner.client_module.BatchClientError):
            self.submit(wave, service)
        service.fail_post = False
        service.state = "completed"
        original_put = runner.put

        def crash_before_receipt(path, value):
            if Path(path).name == "submitted.json":
                raise RuntimeError("Synthetic crash")
            return original_put(path, value)

        with mock.patch.object(runner, "put", side_effect=crash_before_receipt):
            with self.assertRaisesRegex(RuntimeError, "Synthetic crash"):
                runner.reconcile_wave(*self.args, wave["wave_id"], service.name, client=service.api)
        self.assertTrue((folder / "reconciliation-results.json").exists())
        with mock.patch.object(env, "_read_env", side_effect=AssertionError("No secret file read")) as read:
            self.assertEqual(runner.reconcile_wave(*self.args, wave["wave_id"], service.name,
                             env_file=self.root / "missing.env")["state"], "reconciled")
            self.assertEqual(runner.poll_wave(*self.args, wave["wave_id"],
                             env_file=self.root / "missing.env")["completed"], 1)
            read.assert_not_called()


class EnvCliTests(unittest.TestCase):
    def test_env_file_is_passed_only_to_api_commands(self):
        for command, function in (("submit", "submit_wave"), ("poll", "poll_wave"), ("reconcile", "reconcile_wave")):
            args = [command, "--manifest", "synthetic-plan", "--expected-sha256", "a" * 64,
                    "--wave", "synthetic-wave", "--env-file", "synthetic-keys.env"]
            if command == "reconcile":
                args += ["--remote-id", "msgbatch_synthetic"]
            with mock.patch.object(runner, function, return_value={"state": "synthetic"}) as action:
                with mock.patch("sys.stdout", new_callable=io.StringIO):
                    self.assertEqual(runner.main(args), 0)
                self.assertEqual(action.call_args.kwargs["env_file"], "synthetic-keys.env")
        for command in ("plan", "prepare", "status", "retry", "export"):
            args = [command, "--request" if command == "plan" else "--manifest", "synthetic-plan",
                    "--expected-sha256", "a" * 64, "--env-file", "synthetic-keys.env"]
            if command == "retry":
                args += ["--wave", "synthetic-wave"]
            with mock.patch("sys.stderr", new_callable=io.StringIO), self.assertRaises(SystemExit) as error:
                runner.main(args)
            self.assertEqual(error.exception.code, 2)

    def test_env_errors_are_controlled_cli_diagnostics(self):
        args = ["poll", "--manifest", "synthetic-plan", "--expected-sha256", "a" * 64,
                "--wave", "synthetic-wave", "--env-file", "private-path.env"]
        with mock.patch.object(runner, "poll_wave", side_effect=env.EnvFileError("API key env file requires chmod 600")):
            with mock.patch("sys.stderr", new_callable=io.StringIO) as stderr:
                self.assertEqual(runner.main(args), 2)
            self.assertIn("chmod 600", stderr.getvalue())
            self.assertNotIn("private-path", stderr.getvalue())


if __name__ == "__main__":
    unittest.main()
