import test from 'node:test';
import assert from 'node:assert/strict';
import {spawnSync} from 'node:child_process';

test('bundle restore rejects unsafe paths and corrupt archives before touching data',()=>{
  const result=spawnSync('python3',['-c',`
import importlib.util,json,tempfile
from pathlib import Path
s=importlib.util.spec_from_file_location('bundle','scripts/release-bundle.py')
m=importlib.util.module_from_spec(s);s.loader.exec_module(m)
for p in ['../secret','src/data/corpus/../secret','/src/data/corpus/a','src/data/corpus//a','src/data/corpus/./a','private/key']:
    assert not m.allowed(p),p
assert m.allowed('src/data/corpus/manifest.json')
with tempfile.TemporaryDirectory() as d:
    root=Path(d); archive=root/'bad.gz';archive.write_bytes(b'bad')
    manifest=root/'manifest.json'
    manifest.write_text(json.dumps(dict(version=1,bytes=3,unpacked_bytes=1,sha256='0'*64)))
    try:m.restore(manifest,root/'output',archive)
    except ValueError as e:assert 'checksum' in str(e)
    else:raise AssertionError('Corrupt bundle accepted')
    assert not (root/'output').exists()
`],{encoding:'utf8'});
  assert.equal(result.status,0,result.stderr);
});
