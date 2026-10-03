"""Deterministic approved scoring bundles, independent of the corpus bundle."""
import argparse
import gzip
import hashlib
import json
import math
import os
from pathlib import Path, PurePosixPath
import re
import shutil
import subprocess
import tarfile
import tempfile
import urllib.request

PREFIX = 'src/data/analysis/'
SUPPLEMENTS = ('extended-robustness.json', 'joint-trends.json', 'map-diagnostics.json',
               'recording-discovery.json', 'recording-umap.json', 'passage-insights.json',
               'factor-diagnostics.json', 'temporal-comparisons.json')
MAX_ARCHIVE = 150_000_000
MAX_UNPACKED = 600_000_000
MAX_FILES = 10009
MANIFEST_KEYS = {'version', 'type', 'sha256', 'bytes', 'unpacked_bytes', 'files',
                 'corpus_release', 'questionnaire_version', 'model', 'source_generated_at',
                 'scored_recordings', 'scored_passages', 'loadings_sha256'}
COMPATIBILITY_KEYS = {'compatible_corpus_release', 'compatibility_basis'}


def compatible_corpus(m, corpus):
    extra = set(m) & COMPATIBILITY_KEYS
    if extra and (extra != COMPATIBILITY_KEYS
                  or m['compatibility_basis'] != 'reviewed_transcript_update_historical_scores'
                  or not re.fullmatch(r'release_[a-f0-9]{24}', str(m['compatible_corpus_release']))
                  or m['compatible_corpus_release'] == m['corpus_release']):
        raise ValueError('Invalid historical analysis compatibility')
    return corpus == m['corpus_release'] or (extra and corpus == m['compatible_corpus_release'])


def load(file):
    return json.loads(file.read_text())


def digest(file):
    with file.open('rb') as stream:
        return hashlib.file_digest(stream, 'sha256').hexdigest()


def source_count(value):
    """Scientific JSON may serialize a count as 4034.0; pins use integers."""
    if (type(value) not in (int, float) or not math.isfinite(value)
            or value <= 0 or value != int(value)):
        raise ValueError('Invalid source coverage count')
    return int(value)


def allowed(name):
    p = PurePosixPath(name)
    if p.is_absolute() or '..' in p.parts or '\\' in name or str(p) != name:
        return False
    return name in {PREFIX+'release.json', *(PREFIX+s for s in SUPPLEMENTS)} or bool(
        re.fullmatch(re.escape(PREFIX)+r'passages/rec_[a-f0-9]{32}\.json', name))


def corpus_identity(manifest):
    value = load(manifest)
    identity = value.get('corpus_release')
    if not isinstance(identity, str) or not identity:
        raise ValueError('Invalid corpus pin')
    return identity


def validate_manifest(m, corpus):
    if set(m) not in (MANIFEST_KEYS, MANIFEST_KEYS | {'url'}, MANIFEST_KEYS | COMPATIBILITY_KEYS, MANIFEST_KEYS | COMPATIBILITY_KEYS | {'url'}):
        raise ValueError('Unexpected analysis manifest fields')
    if type(m['version']) is not int or m['version'] != 1 or m['type'] != 'transcript-analysis' or not compatible_corpus(m, corpus):
        raise ValueError('Analysis/corpus manifest identity mismatch')
    for key, limit in [('bytes', MAX_ARCHIVE), ('unpacked_bytes', MAX_UNPACKED), ('files', MAX_FILES),
                       ('scored_recordings', 10000), ('scored_passages', 1_000_000)]:
        if type(m[key]) is not int or not 0 < m[key] <= limit:
            raise ValueError('Invalid analysis manifest bound: '+key)
    if m['files'] != m['scored_recordings'] + 9:
        raise ValueError('Manifest inventory count mismatch')
    for key in ('sha256', 'loadings_sha256'):
        if not isinstance(m[key], str) or not re.fullmatch('[a-f0-9]{64}', m[key]):
            raise ValueError('Invalid manifest hash')
    for key in ('questionnaire_version', 'model', 'source_generated_at'):
        if not isinstance(m[key], str) or not 0 < len(m[key]) <= 160:
            raise ValueError('Invalid manifest identity')


def verify(directory, m, full=True):
    release = load(directory/'release.json')
    identities = {'corpus_release_id': m['corpus_release'], 'questionnaire_version': m['questionnaire_version'],
                  'model': m['model'], 'generated_at': m['source_generated_at']}
    if any(release.get(key) != value for key, value in identities.items()):
        raise ValueError('Analysis release identity mismatch')
    videos = release['videos']
    ids = [v['recording_id'] for v in videos]
    if len(ids) != len(set(ids)) or len(ids) != m['scored_recordings']:
        raise ValueError('Recording inventory mismatch')
    if any(not re.fullmatch('rec_[a-f0-9]{32}', id) for id in ids):
        raise ValueError('Invalid recording identifier')
    if release['coverage']['scored_chunks'] != m['scored_passages']:
        raise ValueError('Scored passage count mismatch')
    expected = {'release.json', *SUPPLEMENTS, *('passages/'+id+'.json' for id in ids)}
    actual = {p.relative_to(directory).as_posix() for p in directory.rglob('*') if p.is_file()}
    if actual != expected:
        raise ValueError('Analysis file inventory mismatch')
    total = 0
    for video in videos:
        value = load(directory/'passages'/(video['recording_id']+'.json'))
        if value.get('recording') != {'recording_id': video['recording_id'], 'revision_id': video['revision_id']}:
            raise ValueError('Passage recording revision mismatch')
        if any(value.get(k) != v for k, v in identities.items() if k != 'generated_at') or value.get('source_generated_at') != m['source_generated_at']:
            raise ValueError('Passage release identity mismatch')
        total += len(value['passages'])
    if total != m['scored_passages']:
        raise ValueError('Passage inventory count mismatch')
    if full:
        checker = Path(__file__).resolve().with_name('check-analysis-release.mjs')
        result = subprocess.run(['node', str(checker), str(directory)], check=True,
                                capture_output=True, text=True,
                                env={**os.environ, 'NODE_OPTIONS': '--max-old-space-size=512'})
        checked = json.loads(result.stdout)
        if checked['loadings_sha256'] != m['loadings_sha256']:
            raise ValueError('Factor solution hash mismatch')


def pack(source, output, corpus_pin):
    corpus = corpus_identity(corpus_pin)
    release = load(source/'public-release.json')
    if release.get('corpus_release_id') != corpus:
        raise ValueError('Source differs from corpus pin')
    if output.exists():
        raise ValueError('Pack output must be a fresh directory')
    output.mkdir(parents=True, mode=0o700)
    with tempfile.TemporaryDirectory(prefix='analysis-pack-', dir=output) as folder:
        directory = Path(folder)
        paths = [('public-release.json', 'release.json')] + [(s, s) for s in SUPPLEMENTS]
        paths += [('passages/'+v['recording_id']+'.json', 'passages/'+v['recording_id']+'.json') for v in release['videos']]
        size = 0
        for src, target in paths:
            file = source/src
            if any(p.is_symlink() for p in [file, *file.parents]) or not file.is_file() or not allowed(PREFIX+target):
                raise ValueError('Unsafe source file')
            limit = 80*1024*1024 if target == 'release.json' else 8*1024*1024
            if file.stat().st_size > limit:
                raise ValueError('Source file exceeds bound')
            size += file.stat().st_size
            if size > MAX_UNPACKED or len(paths) > MAX_FILES:
                raise ValueError('Source bundle exceeds bounds')
            dest = directory/target
            dest.parent.mkdir(parents=True, exist_ok=True)
            shutil.copyfile(file, dest)
        m = dict(version=1, type='transcript-analysis', sha256='0'*64, bytes=1,
                 unpacked_bytes=size, files=len(paths), corpus_release=corpus,
                 questionnaire_version=release['questionnaire_version'], model=release['model'],
                 source_generated_at=release['generated_at'], scored_recordings=source_count(release['coverage']['scored_recordings']),
                 scored_passages=source_count(release['coverage']['scored_chunks']),
                 loadings_sha256=load(directory/'recording-discovery.json')['loadings_sha256'])
        validate_manifest(m, corpus)
        verify(directory, m)
        archive = output/'analysis.tar.gz'
        with archive.open('wb') as raw, gzip.GzipFile(filename='', fileobj=raw, mode='wb', mtime=0, compresslevel=6) as compressed, tarfile.open(fileobj=compressed, mode='w|') as tar:
            for _, target in sorted(paths, key=lambda pair: pair[1]):
                file = directory/target
                info = tarfile.TarInfo(PREFIX+target)
                info.size = file.stat().st_size
                info.mode = 0o644
                info.mtime = 0
                with file.open('rb') as stream:
                    tar.addfile(info, stream)
        m.update(sha256=digest(archive), bytes=archive.stat().st_size)
        validate_manifest(m, corpus)
        archive.chmod(0o600)
        (output/'manifest.json').write_text(json.dumps(m, indent=2)+'\n')
        (output/'manifest.json').chmod(0o600)
        print(json.dumps(m))


def restore(manifest, root, corpus_pin, local=None):
    m = load(manifest)
    validate_manifest(m, corpus_identity(corpus_pin))
    active = root/'src/data/corpus/manifest.json'
    if active.exists() and not compatible_corpus(m, load(active).get('release_id')):
        raise ValueError('Restored corpus differs from analysis pin')
    with tempfile.TemporaryDirectory(prefix='himr-analysis-') as folder:
        temp = Path(folder)
        archive = temp/'bundle.tar.gz'
        if local:
            if local.is_symlink() or not local.is_file():
                raise ValueError('Invalid local archive')
            shutil.copyfile(local, archive)
        else:
            url = m.get('url', '')
            expected = 'https://himr-corpus-release-assets.a-xauiw.workers.dev/releases/'+m['sha256']+'.tar.gz'
            if url != expected:
                raise ValueError('Invalid analysis bundle URL')
            request = urllib.request.Request(url, headers={'User-Agent': 'HIMR-Pages-Analysis/1.0'})
            with urllib.request.urlopen(request, timeout=120) as response, archive.open('wb') as out:
                if response.geturl() != url:
                    raise ValueError('Bundle redirects forbidden')
                size = 0
                while chunk := response.read(1024*1024):
                    size += len(chunk)
                    if size > m['bytes']:
                        raise ValueError('Download exceeds expected size')
                    out.write(chunk)
        if archive.stat().st_size != m['bytes'] or digest(archive) != m['sha256']:
            raise ValueError('Analysis bundle checksum mismatch')
        staging = temp/'staging'
        staging.mkdir()
        seen = set()
        size = 0
        with tarfile.open(archive, 'r:gz') as tar:
            for member in tar:
                if not member.isfile() or not allowed(member.name) or member.name in seen or member.size < 0:
                    raise ValueError('Unsafe or duplicate archive member')
                seen.add(member.name)
                size += member.size
                if len(seen) > m['files'] or size > m['unpacked_bytes']:
                    raise ValueError('Archive exceeds unpacked bounds')
                limit = 80*1024*1024 if member.name == PREFIX+'release.json' else 8*1024*1024
                if member.size > limit:
                    raise ValueError('Archive member exceeds bound')
                file = staging/member.name
                file.parent.mkdir(parents=True, exist_ok=True)
                with tar.extractfile(member) as source, file.open('wb') as out:
                    shutil.copyfileobj(source, out)
        if len(seen) != m['files'] or size != m['unpacked_bytes']:
            raise ValueError('Archive inventory differs')
        verify(staging/PREFIX, m)
        destination = root/PREFIX
        if destination.exists():
            for file in destination.rglob('*'):
                if file.is_symlink():
                    raise ValueError('Symlinked analysis destination')
                if file.is_file():
                    name = PREFIX+file.relative_to(destination).as_posix()
                    if name not in seen and name not in {PREFIX+'questions.json', PREFIX+'factor-descriptions.json'}:
                        raise ValueError('Unrecognized or stale generated destination; preserve it before restoring')
        for name in seen:
            target = root/name
            if any(p.is_symlink() for p in [target, *target.parents]):
                raise ValueError('Symlinked destination')
            temporary = target.with_name(target.name+'.analysis-restore.tmp')
            if temporary.exists() or temporary.is_symlink():
                raise ValueError('Unexpected restore temporary file')
        for name in sorted(seen):
            target = root/name
            target.parent.mkdir(parents=True, exist_ok=True)
            temporary = target.with_name(target.name+'.analysis-restore.tmp')
            with temporary.open('xb') as out, (staging/name).open('rb') as source:
                shutil.copyfileobj(source, out)
            temporary.chmod(0o644)
            temporary.replace(target)
        print(json.dumps(dict(restored=True, recordings=m['scored_recordings'], passages=m['scored_passages'], sha256=m['sha256'])))


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    sub = parser.add_subparsers(dest='command', required=True)
    pack_args = sub.add_parser('pack')
    pack_args.add_argument('--source', type=Path, required=True)
    pack_args.add_argument('--output', type=Path, required=True)
    pack_args.add_argument('--corpus-manifest', type=Path, default=Path('corpus-release.json'))
    restore_args = sub.add_parser('restore')
    restore_args.add_argument('--manifest', type=Path, default=Path('analysis-release.json'))
    restore_args.add_argument('--root', type=Path, default=Path.cwd())
    restore_args.add_argument('--corpus-manifest', type=Path, default=Path('corpus-release.json'))
    restore_args.add_argument('--local', type=Path)
    args = parser.parse_args()
    if args.command == 'pack':
        pack(args.source, args.output, args.corpus_manifest)
    else:
        restore(args.manifest, args.root, args.corpus_manifest, args.local)
