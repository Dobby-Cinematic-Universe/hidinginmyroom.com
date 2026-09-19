"""Package/restore approved static data; never fetch private research in Pages."""
import argparse
import gzip
import hashlib
import json
from pathlib import Path, PurePosixPath
import shutil
import tarfile
import tempfile
import urllib.request

MAX_BYTES=2_000_000_000
MAX_FILES=20000


def digest(path):
    with path.open('rb') as stream:return hashlib.file_digest(stream,'sha256').hexdigest()


def allowed(name):
    p=PurePosixPath(name)
    return (not p.is_absolute() and '..' not in p.parts and '\\' not in name
        and str(p)==name and any(name.startswith(prefix) for prefix in
            ('src/data/corpus/','src/data/summaries/','.release-data/search/')))


def verify(root,manifest):
    corpus=json.loads((root/'src/data/corpus/manifest.json').read_text())
    summaries=json.loads((root/'src/data/summaries/release.json').read_text())
    search=json.loads((root/'.release-data/search/search-manifest.json').read_text())
    if corpus['release_id']!=manifest['corpus_release'] or summaries['release_id']!=manifest['summary_release']:
        raise ValueError('Bundle release identity mismatch')
    if corpus['counts']['recordings']!=manifest['recordings'] or len(summaries['summaries'])!=manifest['summaries']:
        raise ValueError('Bundle count mismatch')
    if search['release_id']!=corpus['release_id'] or search['recording_count']!=manifest['transcripts']:
        raise ValueError('Cached search release mismatch')
    if not manifest['recordings'] or not manifest['transcripts'] or not manifest['summaries']:
        raise ValueError('Empty release forbidden')


def pack(candidate,search,output):
    report=json.loads((candidate/'candidate-report.json').read_text())
    if report['build']!='passed' or report['held_summaries']:raise ValueError('Unvalidated candidate')
    output.mkdir(parents=True,exist_ok=True)
    archive=output/'corpus.tar.gz'
    roots=[(candidate/'src/data/corpus','src/data/corpus'),(candidate/'src/data/summaries','src/data/summaries'),(search,'.release-data/search')]
    count=0;size=0
    with archive.open('wb') as raw,gzip.GzipFile(filename='',fileobj=raw,mode='wb',mtime=0,compresslevel=6) as compressed,tarfile.open(fileobj=compressed,mode='w|') as tar:
        for folder,prefix in roots:
            for file in sorted(folder.rglob('*')):
                if file.is_symlink():raise ValueError('Symlink forbidden')
                if not file.is_file():continue
                name=prefix+'/'+file.relative_to(folder).as_posix()
                if not allowed(name):raise ValueError('Unsafe archive path')
                size+=file.stat().st_size;count+=1
                if size>MAX_BYTES or count>MAX_FILES:raise ValueError('Bundle exceeds bounds')
                info=tarfile.TarInfo(name);info.size=file.stat().st_size;info.mode=0o644;info.mtime=0
                with file.open('rb') as source:tar.addfile(info,source)
    summaries=json.loads((candidate/'src/data/summaries/release.json').read_text())
    manifest=dict(version=1,sha256=digest(archive),bytes=archive.stat().st_size,unpacked_bytes=size,files=count,
        corpus_release=report['corpus_release'],summary_release=summaries['release_id'],recordings=report['recordings'],transcripts=report['transcripts'],summaries=report['summaries'])
    (output/'manifest.json').write_text(json.dumps(manifest,indent=2)+'\n')
    print(json.dumps(manifest))


def restore(manifest_path,root,local=None):
    m=json.loads(manifest_path.read_text())
    if m.get('version')!=1 or not 0<m['bytes']<=300_000_000 or not 0<m['unpacked_bytes']<=MAX_BYTES:
        raise ValueError('Invalid bundle bounds')
    with tempfile.TemporaryDirectory(prefix='himr-release-') as folder:
        temp=Path(folder);archive=temp/'bundle.tar.gz'
        if local:shutil.copyfile(local,archive)
        else:
            url=m['url']
            if not url.startswith('https://') or not url.endswith('/'+m['sha256']+'.tar.gz'):raise ValueError('Invalid bundle URL')
            request=urllib.request.Request(url,headers={'User-Agent':'HIMR-Pages-Release/1.0','Accept':'application/gzip'})
            with urllib.request.urlopen(request,timeout=120) as response,archive.open('wb') as out:
                if response.geturl()!=url:raise ValueError('Bundle redirects forbidden')
                size=0
                while chunk:=response.read(1024*1024):
                    size+=len(chunk)
                    if size>m['bytes']:raise ValueError('Bundle download exceeds expected size')
                    out.write(chunk)
        if archive.stat().st_size!=m['bytes'] or digest(archive)!=m['sha256']:raise ValueError('Bundle checksum mismatch')
        staging=temp/'staging';staging.mkdir();seen=set();size=0
        with tarfile.open(archive,'r:gz') as tar:
            for member in tar:
                if not member.isfile() or not allowed(member.name) or member.name in seen:raise ValueError('Unsafe or duplicate archive member')
                seen.add(member.name);size+=member.size
                if len(seen)>MAX_FILES or size>m['unpacked_bytes']:raise ValueError('Archive exceeds unpacked bounds')
                target=staging/member.name;target.parent.mkdir(parents=True,exist_ok=True)
                with tar.extractfile(member) as source,target.open('wb') as out:shutil.copyfileobj(source,out)
        if len(seen)!=m['files'] or size!=m['unpacked_bytes']:raise ValueError('Archive inventory differs')
        verify(staging,m)
        # Existing public placeholders may be replaced; reject symlinked destinations.
        for name in seen:
            target=root/name
            if any(p.is_symlink() for p in [target,*target.parents]):raise ValueError('Symlinked destination')
        for name in sorted(seen):
            target=root/name;target.parent.mkdir(parents=True,exist_ok=True);shutil.copyfile(staging/name,target)
        print(json.dumps(dict(restored=True,recordings=m['recordings'],summaries=m['summaries'])))


if __name__=='__main__':
    p=argparse.ArgumentParser();sub=p.add_subparsers(dest='command',required=True)
    a=sub.add_parser('pack');a.add_argument('--candidate',type=Path,required=True);a.add_argument('--search',type=Path,required=True);a.add_argument('--output',type=Path,required=True)
    a=sub.add_parser('restore');a.add_argument('--manifest',type=Path,default=Path('corpus-release.json'));a.add_argument('--root',type=Path,default=Path.cwd());a.add_argument('--local',type=Path)
    a=p.parse_args()
    if a.command=='pack':pack(a.candidate,a.search,a.output)
    else:restore(a.manifest,a.root,a.local)
