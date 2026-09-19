"""Pinned recovery launcher; leaves sealed runtime and paid artifacts unchanged."""
import argparse
import hashlib
from pathlib import Path
import sys


def main():
    parser = argparse.ArgumentParser(add_help=False)
    parser.add_argument('--runtime-path', required=True)
    parser.add_argument('--worker-sha256', required=True)
    parser.add_argument('--launcher-sha256', required=True)
    parser.add_argument('--recovery-sha256', required=True)
    parser.add_argument('--admission-sha256', required=True)
    args, rest = parser.parse_known_args()
    runtime = Path(args.runtime_path).resolve()
    source = Path(__file__).resolve().parent
    paths = [(Path(__file__), args.launcher_sha256),
        (runtime / 'pipeline/cloud_transcription_summary.py', args.worker_sha256),
        (source / 'gemini_recovery_extension.py', args.recovery_sha256),
        (source / 'short_summary_admission_runner.py', args.admission_sha256)]
    for path, expected in paths:
        if hashlib.sha256(path.read_bytes()).hexdigest() != expected:
            raise RuntimeError('recovery launcher code hash mismatch')
    sys.path.insert(0, str(runtime))
    from pipeline import cloud_transcription_summary as worker
    from pipeline import short_summary_admission_runner as admission
    from pipeline import gemini_recovery_extension as recovery
    if Path(worker.__file__).resolve() != paths[1][0]:
        raise RuntimeError('unexpected summary runtime import')
    admission.install(worker)
    recovery.install(worker)
    return worker.main(rest)


if __name__ == '__main__':
    raise SystemExit(main())
