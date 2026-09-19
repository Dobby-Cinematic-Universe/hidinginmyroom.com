"""Hash-pinned process-local admission extension for the retained summary worker."""
import argparse
import hashlib
from pathlib import Path
import sys

REASON = 'source_transcript_under_50_words'


def install(worker):
    original_gate, original_public = worker._identity_holds, worker._public

    def gate(available):
        held = original_gate(available)
        for recording, source in available.items():
            doc = worker.r.read(source['transcript'])
            words = sum(len(segment['text'].split()) for segment in doc['segments'])
            if words < 50:
                held[recording] = dict(recording_id=recording, reason=REASON,
                    transcript=source['transcript'], word_count=words)
        return held

    def public(manifest, snapshot):
        result = original_public(manifest, snapshot)
        short = [row for row in result['speaker_identity_holds'] if row['reason'] == REASON]
        identity = [row for row in result['speaker_identity_holds'] if row['reason'] != REASON]
        result.update(short_transcripts_withheld=len(short), short_transcript_holds=short,
            minimum_transcript_words=50, speaker_identity_pending=len(identity),
            speaker_identity_holds=identity,
            speaker_identity_pending_recording_ids=sorted(row['recording_id'] for row in identity))
        return result

    worker._identity_holds, worker._public = gate, public


def main():
    parser = argparse.ArgumentParser(add_help=False)
    parser.add_argument('--runtime-path', required=True)
    parser.add_argument('--worker-sha256', required=True)
    parser.add_argument('--extension-sha256', required=True)
    args, rest = parser.parse_known_args()
    runtime = Path(args.runtime_path).resolve()
    worker_path = runtime / 'pipeline/cloud_transcription_summary.py'
    for path, expected in [(Path(__file__), args.extension_sha256), (worker_path, args.worker_sha256)]:
        if hashlib.sha256(path.read_bytes()).hexdigest() != expected:
            raise RuntimeError('summary admission extension code hash mismatch')
    sys.path.insert(0, str(runtime))
    from pipeline import cloud_transcription_summary as worker
    if Path(worker.__file__).resolve() != worker_path:
        raise RuntimeError('unexpected summary worker import')
    install(worker)
    return worker.main(rest)


if __name__ == '__main__':
    raise SystemExit(main())
