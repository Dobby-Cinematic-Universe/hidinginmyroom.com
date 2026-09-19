"""Explicit hash-pinned activation of queue accounting and scheduler reuse."""
import argparse
import hashlib
from pathlib import Path
import sys


def main():
    parser = argparse.ArgumentParser(add_help=False)
    flags = ('runtime-path', 'worker-sha256', 'launcher-sha256', 'recovery-sha256',
        'admission-sha256', 'reviewed-sha256', 'feed-sha256', 'reviewed-feed',
        'queue-sha256', 'efficiency-sha256', 'scheduler-state')
    for flag in flags:
        parser.add_argument('--' + flag, required=True)
    parser.add_argument('--queue-policy', choices=('legacy', 'tier1-95-percent', 'tier2-95-percent'), default='legacy')
    parser.add_argument('--retry-manifest')
    parser.add_argument('--retry-manifest-sha256')
    parser.add_argument('--retry-sha256')
    parser.add_argument('--spend-policy', choices=('local', 'google-dashboard'), default='local')
    parser.add_argument('--spend-policy-sha256')
    args, rest = parser.parse_known_args()
    retry_options = (args.retry_manifest, args.retry_manifest_sha256, args.retry_sha256)
    if any(retry_options) and not all(retry_options):
        parser.error('targeted retry requires a hash-pinned module and authority manifest')
    if (args.spend_policy == 'google-dashboard') != bool(args.spend_policy_sha256):
        parser.error('dashboard-managed spending requires the exact policy module hash')
    runtime, source = Path(args.runtime_path).resolve(), Path(__file__).resolve().parent
    paths = [(Path(__file__), args.launcher_sha256),
        (runtime / 'pipeline/cloud_transcription_summary.py', args.worker_sha256),
        (source / 'gemini_recovery_extension.py', args.recovery_sha256),
        (source / 'short_summary_admission_runner.py', args.admission_sha256),
        (source / 'reviewed_summary_adapter.py', args.reviewed_sha256),
        (source / 'reviewed_transcript_feed.py', args.feed_sha256),
        (source / 'gemini_queue_tokens.py', args.queue_sha256),
        (source / 'gemini_scheduler_efficiency.py', args.efficiency_sha256)]
    if args.retry_manifest:
        paths.extend([(source / 'gemini_targeted_retry.py', args.retry_sha256),
            (Path(args.retry_manifest), args.retry_manifest_sha256)])
    if args.spend_policy_sha256:
        paths.append((source / 'gemini_dashboard_spend.py', args.spend_policy_sha256))
    for path, expected in paths:
        if hashlib.sha256(path.read_bytes()).hexdigest() != expected:
            raise RuntimeError('efficient Gemini launcher implementation changed')
    sys.path.insert(0, str(runtime))
    from pipeline import cloud_transcription_summary as worker
    from pipeline import short_summary_admission_runner as admission
    from pipeline import gemini_recovery_extension as recovery
    from pipeline import reviewed_summary_adapter as reviewed
    from pipeline import gemini_scheduler_efficiency as efficiency
    from pipeline import gemini_queue_tokens as queue
    if Path(worker.__file__).resolve() != paths[1][0]:
        raise RuntimeError('unexpected retained summary runtime')
    spend_reference = None
    if args.spend_policy == 'google-dashboard':
        from pipeline import gemini_dashboard_spend as spend
        if '--manifest' not in rest or '--expected-sha256' not in rest:
            parser.error('dashboard-managed spending requires a bound existing worker')
        spend_reference = dict(path=rest[rest.index('--manifest') + 1],
            sha256=rest[rest.index('--expected-sha256') + 1])
        spend.install(worker, spend_reference)
    admission.install(worker)
    recovery.install(worker)
    reviewed.install(worker, args.reviewed_feed)
    if args.retry_manifest:
        from pipeline import gemini_targeted_retry as retry
        retry.install(worker, dict(path=str(Path(args.retry_manifest).resolve()), sha256=args.retry_manifest_sha256))
    if spend_reference is not None:
        spend.install_collectors(worker, spend_reference)
    env_file = rest[rest.index('--env-file') + 1] if '--env-file' in rest else None
    counted = args.queue_policy != 'legacy'
    tier = 'tier2' if args.queue_policy == 'tier2-95-percent' else 'tier1'
    if counted and rest and rest[0] in {'run', 'cycle'} and not any(
            value == '--max-enqueued-tokens' or value.startswith('--max-enqueued-tokens=') for value in rest):
        rest.extend(['--max-enqueued-tokens', str(queue.tier_limits(tier)[1])])
    efficiency.install(worker, args.scheduler_state, env_file=env_file,
        queue_policy=queue.COUNTED_POLICY if counted else queue.POLICY, queue_tier=tier,
        allow_count_api=bool(rest and rest[0] in {'run', 'cycle'} and '--allow-paid-api' in rest))
    return worker.main(rest)


if __name__ == '__main__':
    raise SystemExit(main())
