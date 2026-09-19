"""Finite local-only refresh of completed summary exports; never submits jobs."""
import argparse
import hashlib
import json
from pathlib import Path
import subprocess
import time


def signature(reader, targeted):
    paths = [reader] + sorted(targeted.glob('shard-*/reader-exports/reader-*/index.json'))
    tail=Path('research/private-summaries/gemini-standard-tail-20260917/completed.json')
    if tail.exists():paths.append(tail)
    delta=Path('research/private-summaries/sonnet-summary-delta-20260917-v3')
    if (delta/'status.json').exists() and json.loads((delta/'status.json').read_text()).get('complete'):
        paths.append(delta/'reader/index.json')
    digest = hashlib.sha256()
    for path in paths:
        digest.update(str(path).encode()); digest.update(path.read_bytes())
    return digest.hexdigest()


def main():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument('--directory', required=True)
    p.add_argument('--reader', type=Path, required=True)
    p.add_argument('--targeted-run', type=Path, required=True)
    p.add_argument('--site-unit', required=True)
    a = p.parse_args(); previous = None; deadline = time.monotonic() + 7 * 86400
    while time.monotonic() < deadline:
        pointer = json.loads(Path('research/corpus/site-previews/current.json').read_text())
        if pointer['directory'] != a.directory:
            print('Active preview changed; leaving the new preview untouched.', flush=True)
            return
        current = signature(a.reader, a.targeted_run)
        if current != previous:
            subprocess.run(['node', 'scripts/refresh-preview-summaries.mjs', a.directory,
                            str(a.reader), str(a.targeted_run)], check=True)
            # Reuse cached local embeddings; no network/API or media decoding.
            subprocess.run(['node', 'scripts/group-corpus-events.mjs'], check=True)
            subprocess.run(['systemctl', '--user', 'restart', a.site_unit], check=True)
            previous = current
            print('Updated local summaries and event groups; restarted only local site.', flush=True)
        time.sleep(60)


if __name__ == '__main__': main()
