"""Supervisor checks use fake processes/clocks; no provider or systemd calls."""
from __future__ import annotations

import contextlib
import io
import os
from pathlib import Path
import signal
import subprocess
import unittest
from unittest import mock

from pipeline import cloud_transcription_parallel as parallel
from pipeline.tests import test_cloud_transcription_runtime as fixtures


class FakeClock:
    def __init__(self):
        self.now = 0.0
        self.sleeps = []

    def monotonic(self):
        return self.now

    def sleep(self, duration):
        if duration < 0:
            raise AssertionError('negative sleep')
        self.sleeps.append(duration)
        self.now += duration


class Process:
    def __init__(self, clock, pid, *, finish=None, code=0, ignore_term=False, shutdown_delay=0):
        self.clock, self.pid, self.finish, self.code = clock, pid, finish, code
        self.ignore_term, self.shutdown_delay = ignore_term, shutdown_delay
        self.terminated = False

    def poll(self):
        return self.code if self.finish is not None and self.clock.now >= self.finish else None

    def terminate_group(self):
        self.terminated = True
        if not self.ignore_term:
            self.finish = self.clock.now + self.shutdown_delay
            self.code = 0


class ParallelTests(unittest.TestCase):
    def setUp(self):
        self.plan = {'path': '/private/cloud/plan.json', 'sha256': 'a' * 64}
        self.summary = {'path': '/private/summary/manifest.json', 'sha256': 'b' * 64}
        self.clock = FakeClock()
        self.events = []
        self.processes = []
        self.started = []
        self.signals = []
        self.patches = contextlib.ExitStack()
        self.addCleanup(self.patches.close)
        self.patches.enter_context(mock.patch.object(parallel.cloud, 'load_plan', return_value={'screen_config': {'path': '/private/screen.json', 'sha256': 'c' * 64}}))
        self.patches.enter_context(mock.patch.object(parallel.summaries, 'load_manifest', return_value={'cloud_plan': self.plan}))
        self.patches.enter_context(mock.patch.object(parallel.cloud, 'status', return_value={'spending_limit_microusd': None}))
        self.patches.enter_context(mock.patch.object(parallel.time, 'monotonic', side_effect=self.clock.monotonic))
        self.patches.enter_context(mock.patch.object(parallel.time, 'sleep', side_effect=self.clock.sleep))
        self.spawn = self.patches.enter_context(mock.patch.object(parallel.subprocess, 'Popen', side_effect=self.popen))
        self.signal = self.patches.enter_context(mock.patch.object(parallel.os, 'killpg', side_effect=self.killpg))

    def popen(self, command, **kwargs):
        process = self.processes[len(self.started)]
        self.started.append((command, kwargs, process))
        return process

    def killpg(self, pid, signum):
        self.signals.append((pid, signum))
        next(process for process in self.processes if process.pid == pid).terminate_group()

    def children(self, screen=0, transcription=2, summaries=4, *, failed=None, ignore=None, delay=0):
        for ordinal, (name, finish) in enumerate([('screen', screen), ('transcription', transcription), ('summaries', summaries)], 100):
            self.processes.append(Process(self.clock, ordinal, finish=finish,
                code=2 if name == failed else 0, ignore_term=name == ignore, shutdown_delay=delay))

    def run_pipeline(self, **kwargs):
        options = {'allow_paid_api': True, 'transcription_budget_microusd': 1234567,
                   'max_runtime_seconds': 120, 'shutdown_grace_seconds': 4,
                   'on_event': self.events.append}
        options.update(kwargs)
        return parallel.run(self.plan, self.summary, **options)

    def test_no_paid_permission_prevents_even_preflight_or_spawn(self):
        with self.assertRaises(parallel.ParallelError):
            self.run_pipeline(allow_paid_api=False)
        parallel.cloud.load_plan.assert_not_called()
        self.spawn.assert_not_called()

    def test_invalid_budgets_durations_and_grace_never_spawn(self):
        for kwargs in [{'transcription_budget_microusd': None}, {'transcription_budget_microusd': 0},
                       {'max_runtime_seconds': True}, {'max_runtime_seconds': 0},
                       {'max_runtime_seconds': parallel.MAX_RUNTIME_SECONDS + 1}, {'shutdown_grace_seconds': 0}]:
            with self.subTest(kwargs=kwargs), self.assertRaises(RuntimeError):
                self.run_pipeline(**kwargs)
        self.spawn.assert_not_called()

    def test_mismatched_summary_cloud_plan_fails_before_spawn(self):
        parallel.summaries.load_manifest.return_value = {'cloud_plan': {**self.plan, 'sha256': 'd' * 64}}
        with self.assertRaises(parallel.ParallelError):
            self.run_pipeline()
        self.spawn.assert_not_called()

    def test_missing_screen_configuration_and_changed_budget_fail_before_spawn(self):
        parallel.cloud.load_plan.return_value = {'screen_config': None}
        with self.assertRaises(parallel.ParallelError):
            self.run_pipeline()
        parallel.cloud.load_plan.return_value = {'screen_config': self.plan}
        parallel.cloud.status.return_value = {'spending_limit_microusd': 12}
        with self.assertRaises(parallel.ParallelError):
            self.run_pipeline()
        self.spawn.assert_not_called()

    def test_commands_are_three_independent_stages_with_exact_bindings(self):
        self.children()
        result = self.run_pipeline(env_file='/private/config/.env')
        self.assertEqual(result['state'], 'workers_exited')
        self.assertEqual(result['exit_code'], 0)
        self.assertFalse(result['worker_exit_is_full_archive_completion'])
        self.assertEqual(len(self.started), 3)
        commands = [row[0] for row in self.started]
        self.assertEqual(commands[0][3:5], ['pipeline.cloud_transcription', 'screen'])
        self.assertEqual(commands[1][3:5], ['pipeline.cloud_transcription', 'run'])
        self.assertEqual(commands[2][3:5], ['pipeline.cloud_transcription_summary', 'run'])
        for command in commands:
            self.assertIn('--expected-sha256', command)
            self.assertEqual(command[1], '-B')
        self.assertIn('1.234567', commands[1])
        self.assertEqual(commands[1][commands[1].index('--max-active') + 1], '4')
        self.assertEqual(commands[2][commands[2].index('--max-active') + 1], '8')
        self.assertEqual(commands[2][commands[2].index('--max-cycles') + 1], '2')
        self.assertNotIn('--env-file', commands[0])
        self.assertEqual(commands[1][-2:], ['--env-file', '/private/config/.env'])
        self.assertEqual(commands[2][-2:], ['--env-file', '/private/config/.env'])

    def test_concurrency_is_configurable_and_bounded(self):
        args = dict(transcription_budget_microusd=1_000_000, max_runtime_seconds=60)
        built = parallel.commands(self.plan, self.summary, **args,
                                  transcription_concurrency=3, summary_concurrency=6)
        for stage, value in (('transcription', '3'), ('summaries', '6')):
            command = built[stage]
            self.assertEqual(command[command.index('--max-active') + 1], value)
        for bad in (0, 9, True, 1.5):
            with self.assertRaises(RuntimeError):
                parallel.commands(self.plan, self.summary, **args, transcription_concurrency=bad)
            with self.assertRaises(RuntimeError):
                parallel.commands(self.plan, self.summary, **args, summary_concurrency=bad)

    def test_only_provider_specific_explicit_overrides_enter_child_environment(self):
        self.children()
        secrets = {'ASSEMBLYAI_API_KEY': 'private-aai-key', 'REVAI_ACCESS_TOKEN': 'private-rev-key',
                   'REVAI_API_KEY': 'private-rev-key', 'GEMINI_API_KEY': 'private-gemini-key',
                   'ANTHROPIC_API_KEY': 'private-anthropic-key', 'OPENAI_API_KEY': 'private-openai-key',
                   'HTTPS_PROXY': 'https://proxy-secret'}
        with mock.patch.dict(os.environ, secrets):
            report = self.run_pipeline()
        for stage, (command, kwargs, process) in zip(('screen', 'transcription', 'summaries'), self.started):
            expected = {**parallel.CHILD_ENV, **{name: secrets[name] for name in parallel.CHILD_KEY_NAMES[stage]}}
            self.assertEqual(kwargs['env'], expected)
            self.assertEqual(kwargs['stdin'], subprocess.DEVNULL)
            self.assertIsNone(kwargs['stdout'])
            self.assertIsNone(kwargs['stderr'])
            self.assertTrue(kwargs['start_new_session'])
            self.assertTrue(kwargs['close_fds'])
            self.assertEqual(kwargs['cwd'], str(Path(parallel.__file__).resolve().parents[1]))
            for secret in secrets.values():
                self.assertNotIn(secret, repr(command))
                self.assertNotIn(secret, repr(report))
                self.assertNotIn(secret, repr(self.events))

    def test_screen_normal_exit_does_not_stop_or_restart_other_stages(self):
        self.children(screen=0, transcription=3, summaries=5)
        result = self.run_pipeline()
        self.assertEqual(result['state'], 'workers_exited')
        self.assertGreaterEqual(self.clock.now, 5)
        self.signal.assert_not_called()
        self.assertEqual(len(self.started), 3)

    def test_nonzero_child_exit_stops_other_sessions_gracefully_without_restart(self):
        self.children(screen=1, transcription=None, summaries=None, failed='screen', delay=2)
        result = self.run_pipeline()
        self.assertEqual(result['state'], 'child_failed')
        self.assertEqual(result['exit_code'], 2)
        self.assertEqual(set(self.signals), {(101, signal.SIGTERM), (102, signal.SIGTERM)})
        self.assertEqual(result['remaining_worker_pids'], {})
        self.assertGreaterEqual(self.clock.now, 3)
        self.assertEqual(len(self.started), 3)

    def test_immediate_first_child_failure_never_starts_paid_stages(self):
        self.children(screen=0, transcription=None, summaries=None, failed='screen')
        result = self.run_pipeline()
        self.assertEqual(result['exit_code'], 2)
        self.assertEqual(len(self.started), 1)
        self.signal.assert_not_called()

    def test_deadline_requests_sigterm_and_returns_meaningful_runtime_code(self):
        self.children(screen=0, transcription=None, summaries=None)
        result = self.run_pipeline(max_runtime_seconds=3)
        self.assertEqual(result['state'], 'runtime_limit')
        self.assertEqual(result['exit_code'], 124)
        self.assertEqual(set(self.signals), {(101, signal.SIGTERM), (102, signal.SIGTERM)})
        self.assertTrue(all(value <= 1 for value in self.clock.sleeps))

    def test_unresponsive_paid_worker_is_reported_not_force_killed(self):
        self.children(screen=0, transcription=None, summaries=None, ignore='transcription')
        result = self.run_pipeline(max_runtime_seconds=2, shutdown_grace_seconds=4)
        self.assertEqual(result['state'], 'shutdown_incomplete')
        self.assertEqual(result['exit_code'], 2)
        self.assertEqual(result['remaining_worker_pids'], {'transcription': 101})
        self.assertFalse(result['force_kill_used'])
        self.assertEqual(self.clock.now, 6)
        self.assertTrue(all(signum == signal.SIGTERM for _, signum in self.signals))
        self.assertEqual(len(self.started), 3)

    def test_operator_pause_before_launch_spawns_nothing(self):
        result = self.run_pipeline(stopping=lambda: signal.SIGTERM)
        self.assertEqual(result['state'], 'paused')
        self.assertEqual(result['exit_code'], 143)
        self.spawn.assert_not_called()

    def test_operator_pause_during_monitor_waits_for_graceful_children(self):
        self.children(screen=0, transcription=None, summaries=None)
        result = self.run_pipeline(stopping=lambda: signal.SIGINT if self.clock.now >= 1 else None)
        self.assertEqual(result['exit_code'], 130)
        self.assertEqual(result['state'], 'paused')
        self.assertEqual(result['remaining_worker_pids'], {})

    def test_partial_launch_failure_stops_already_started_child(self):
        self.children(screen=None, transcription=None, summaries=None)
        original = self.popen
        def spawn(command, **kwargs):
            if len(self.started) == 1:
                raise OSError('private executable or environment data')
            return original(command, **kwargs)
        self.spawn.side_effect = spawn
        result = self.run_pipeline()
        self.assertEqual(result['state'], 'launch_failed')
        self.assertEqual(result['exit_code'], 2)
        self.assertEqual(self.signals, [(100, signal.SIGTERM)])
        self.assertNotIn('private executable', repr(result))
        self.assertEqual(len(self.started), 1)

    def test_missing_process_group_during_shutdown_is_safe(self):
        self.children(screen=0, transcription=None, summaries=None)
        def gone(pid, signum):
            process = next(process for process in self.processes if process.pid == pid)
            process.finish = self.clock.now
            raise ProcessLookupError('already exited')
        self.signal.side_effect = gone
        result = self.run_pipeline(max_runtime_seconds=1)
        self.assertEqual(result['state'], 'runtime_limit')
        self.assertEqual(result['remaining_worker_pids'], {})

    def test_signal_handlers_are_restored_and_first_pause_signal_retained(self):
        before = {kind: signal.getsignal(kind) for kind in (signal.SIGINT, signal.SIGTERM)}
        with parallel.pause_signals() as requested:
            self.assertIsNone(requested())
            signal.getsignal(signal.SIGTERM)(signal.SIGTERM, None)
            signal.getsignal(signal.SIGINT)(signal.SIGINT, None)
            self.assertEqual(requested(), signal.SIGTERM)
        self.assertEqual({kind: signal.getsignal(kind) for kind in before}, before)

    def test_help_does_not_validate_manifests_or_launch_workers(self):
        with contextlib.redirect_stdout(io.StringIO()), self.assertRaises(SystemExit) as raised:
            parallel.main(['--help'])
        self.assertEqual(raised.exception.code, 0)
        parallel.cloud.load_plan.assert_not_called()
        parallel.summaries.load_manifest.assert_not_called()
        self.spawn.assert_not_called()


class RealPreflightTests(unittest.TestCase):
    def test_actual_bound_cloud_and_summary_manifest_preflight_is_offline(self):
        helper = fixtures.CloudRuntimeTests()
        helper.setUp()
        self.addCleanup(helper.doCleanups)
        helper.prepare()
        prepared = parallel.summaries.prepare(helper.ref, helper.root / 'summary-worker',
                                             max_total_budget_microusd=1_000_000)
        with mock.patch.object(parallel.subprocess, 'Popen') as spawn:
            commands = parallel._validated_commands(helper.ref, prepared['manifest'],
                transcription_budget_microusd=1_000_000, max_runtime_seconds=3600, env_file=None)
        self.assertEqual(set(commands), {'screen', 'transcription', 'summaries'})
        spawn.assert_not_called()
        broken = {**prepared['manifest'], 'sha256': '0' * 64}
        with self.assertRaises(RuntimeError), mock.patch.object(parallel.subprocess, 'Popen') as spawn:
            parallel.run(helper.ref, broken, allow_paid_api=True, transcription_budget_microusd=1_000_000)
        spawn.assert_not_called()


if __name__ == '__main__':
    unittest.main()
