import test from 'node:test';
import assert from 'node:assert/strict';
import {spawnSync} from 'node:child_process';

test('analysis bundle inventory, identities, deterministic packaging and hostile archives',()=>{
  const code=String.raw`
import importlib.util,json,tempfile,tarfile,io,hashlib
from pathlib import Path
from unittest.mock import patch
spec=importlib.util.spec_from_file_location('bundle','scripts/analysis-release-bundle.py')
b=importlib.util.module_from_spec(spec);spec.loader.exec_module(b)
assert b.source_count(4034.0)==4034
for invalid in (True,False,1.5,float('nan'),float('inf'),'4034',0,-1):
    try:b.source_count(invalid);raise AssertionError('invalid source count accepted')
    except ValueError:pass
assert b.allowed('src/data/analysis/release.json')
for path in ['../src/data/analysis/release.json','/src/data/analysis/release.json','src/data/analysis/../release.json','src/data/analysis/questions.json','src/data/analysis/passages/manifest.json','research/raw.json']:
    assert not b.allowed(path),path
with tempfile.TemporaryDirectory() as folder:
    root=Path(folder);source=root/'source';source.mkdir();(source/'passages').mkdir()
    rec='rec_'+'a'*32;rev='rev_'+'b'*32
    r=dict(corpus_release_id='corpus-a',questionnaire_version='q',model='m',generated_at='now',coverage=dict(scored_recordings=1.0,scored_chunks=1.0),videos=[dict(recording_id=rec,revision_id=rev)])
    (source/'public-release.json').write_text(json.dumps(r))
    for name in b.SUPPLEMENTS:(source/name).write_text(json.dumps(dict(loadings_sha256='c'*64)))
    p=dict(corpus_release_id='corpus-a',questionnaire_version='q',model='m',source_generated_at='now',recording=dict(recording_id=rec,revision_id=rev),passages=[{}])
    (source/'passages'/(rec+'.json')).write_text(json.dumps(p))
    # Private receipts and passage bookkeeping must never enter the archive.
    (source/'receipts.json').write_text('private');(source/'passages/manifest.json').write_text('private')
    pin=root/'corpus.json';pin.write_text(json.dumps(dict(corpus_release='corpus-a')))
    check=type('Result',(),{'stdout':json.dumps(dict(loadings_sha256='c'*64))})()
    with patch.object(b.subprocess,'run',return_value=check):
        b.pack(source,root/'one',pin);b.pack(source,root/'two',pin)
        assert (root/'one/analysis.tar.gz').read_bytes()==(root/'two/analysis.tar.gz').read_bytes()
        m=b.load(root/'one/manifest.json');assert m['files']==10
        assert type(m['scored_recordings']) is int and type(m['scored_passages']) is int
        try:b.validate_manifest(dict(m,scored_recordings=1.0),'corpus-a');raise AssertionError('float pin count accepted')
        except ValueError:pass
        dest=root/'dest';(dest/'src/data/analysis').mkdir(parents=True)
        authored=dest/'src/data/analysis/questions.json';authored.write_text('authored')
        b.restore(root/'one/manifest.json',dest,pin,root/'one/analysis.tar.gz')
        assert authored.read_text()=='authored'
        assert (dest/'src/data/analysis/release.json').exists()
        stale=dest/'src/data/analysis/old.json';stale.write_text('recover me')
        try:b.restore(root/'one/manifest.json',dest,pin,root/'one/analysis.tar.gz');raise AssertionError('stale file accepted')
        except ValueError as error:assert 'stale' in str(error)
        assert stale.read_text()=='recover me'
        wrong=root/'wrong.json';wrong.write_text(json.dumps(dict(corpus_release='other')))
        try:b.restore(root/'one/manifest.json',root/'new',wrong);raise AssertionError('wrong corpus accepted')
        except ValueError as error:assert 'identity mismatch' in str(error)
        compatible='release_'+'d'*24
        historical=dict(m,compatible_corpus_release=compatible,compatibility_basis='reviewed_transcript_update_historical_scores')
        b.validate_manifest(historical,compatible)
        assert historical['corpus_release']=='corpus-a'
        for bad in (dict(historical,compatibility_basis='anything'),dict(historical,compatible_corpus_release='other')):
            try:b.validate_manifest(bad,compatible);raise AssertionError('unbound compatibility accepted')
            except ValueError:pass
        historical_manifest=root/'historical.json';historical_manifest.write_text(json.dumps(historical))
        new_pin=root/'new-pin.json';new_pin.write_text(json.dumps(dict(corpus_release=compatible)))
        b.restore(historical_manifest,root/'historical-dest',new_pin,root/'one/analysis.tar.gz')
        assert b.load(root/'historical-dest/src/data/analysis/release.json')['corpus_release_id']=='corpus-a'
        corrupt=root/'corrupt.json';corrupt.write_text(json.dumps(dict(m,sha256='f'*64)))
        try:b.restore(corrupt,root/'corrupt-dest',pin,root/'one/analysis.tar.gz');raise AssertionError('bad checksum accepted')
        except ValueError as error:assert 'checksum mismatch' in str(error)
        for mode in ('traversal','duplicate','symlink','unexpected','oversized'):
            archive=root/(mode+'.tar.gz')
            with tarfile.open(archive,'w:gz') as tar:
                info=tarfile.TarInfo('../bad' if mode=='traversal' else b.PREFIX+('private.json' if mode=='unexpected' else 'release.json'))
                if mode=='symlink':info.type=tarfile.SYMTYPE;info.linkname='/tmp/unsafe';tar.addfile(info)
                else:
                    payload=b'{}';info.size=len(payload);tar.addfile(info,io.BytesIO(payload))
                    if mode=='duplicate':tar.addfile(info,io.BytesIO(payload))
            bad=dict(m,sha256=b.digest(archive),bytes=archive.stat().st_size)
            if mode=='oversized':bad['unpacked_bytes']=1
            manifest=root/(mode+'.json');manifest.write_text(json.dumps(bad))
            try:b.restore(manifest,root/('dest-'+mode),pin,archive);raise AssertionError(mode+' accepted')
            except ValueError:pass
        changed=json.loads((source/'passages'/(rec+'.json')).read_text());changed['recording']['revision_id']='wrong'
        (source/'passages'/(rec+'.json')).write_text(json.dumps(changed))
        try:b.pack(source,root/'bad-revision',pin);raise AssertionError('wrong revision accepted')
        except ValueError as error:assert 'revision mismatch' in str(error)
print('bundle contracts passed')
`;
  const result=spawnSync('python3',['-c',code],{encoding:'utf8',timeout:30000});
  assert.equal(result.status,0,result.stderr);
  assert.match(result.stdout,/bundle contracts passed/);
});
