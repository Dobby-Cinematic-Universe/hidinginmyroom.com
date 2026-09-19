"""Hash-pinned recovery launcher with explicit manual-review source admission."""
import argparse
import hashlib
from pathlib import Path
import sys


def main():
    parser = argparse.ArgumentParser(add_help=False)
    for flag in ('runtime-path', 'worker-sha256', 'launcher-sha256', 'recovery-sha256',
                 'admission-sha256', 'reviewed-sha256', 'feed-sha256', 'reviewed-feed'):
        parser.add_argument('--' + flag, required=True)
    args, rest = parser.parse_known_args()
    runtime = Path(args.runtime_path).resolve()
    source = Path(__file__).resolve().parent
    paths = [(Path(__file__), args.launcher_sha256),
        (runtime / 'pipeline/cloud_transcription_summary.py', args.worker_sha256),
        (source / 'gemini_recovery_extension.py', args.recovery_sha256),
        (source / 'short_summary_admission_runner.py', args.admission_sha256),
        (source / 'reviewed_summary_adapter.py', args.reviewed_sha256),
        (source / 'reviewed_transcript_feed.py', args.feed_sha256)]
    for path, expected in paths:
        if hashlib.sha256(path.read_bytes()).hexdigest() != expected:
            raise RuntimeError('reviewed recovery launcher code hash mismatch')
    sys.path.insert(0, str(runtime))
    from pipeline import cloud_transcription_summary as worker
    from pipeline import short_summary_admission_runner as admission
    from pipeline import gemini_recovery_extension as recovery
    from pipeline import reviewed_summary_adapter as reviewed
    if Path(worker.__file__).resolve() != paths[1][0]:
        raise RuntimeError('unexpected summary runtime import')
    admission.install(worker)
    recovery.install(worker)
    reviewed.install(worker, args.reviewed_feed)
    return worker.main(rest)


if __name__ == '__main__':
    raise SystemExit(main())
