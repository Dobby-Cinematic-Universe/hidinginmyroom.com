import test from 'node:test';
import assert from 'node:assert/strict';
import {mkdtemp,mkdir,writeFile,symlink,rm} from 'node:fs/promises';
import os from 'node:os';
import path from 'node:path';
import {previewArgument,selectedPreviewDirectory,restorePinnedCandidateAnalysis} from '../candidate-inputs.mjs';

test('explicit preview overrides the live pointer for candidate inputs',()=>{
  const pointer={directory:'release-live'};
  assert.equal(selectedPreviewDirectory(pointer,'release-new'),'release-new');
  assert.equal(selectedPreviewDirectory(pointer),'release-live');
  assert.equal(pointer.directory,'release-live');
  assert.throws(()=>selectedPreviewDirectory(pointer,'../release-new'),/Unsafe preview/);
});

test('grouping preview arguments support explicit flag and positional forms',()=>{
  assert.equal(previewArgument([]),undefined);
  assert.equal(previewArgument(['--preview=release-new']),'release-new');
  assert.equal(previewArgument(['release-new']),'release-new');
  for(const args of [['--preview='],['../release-new'],['--other'],['release-new','release-old']]){
    assert.throws(()=>previewArgument(args));
  }
});

test('candidate analysis uses the verified pinned restore and propagates failure',async()=>{
  const project=await mkdtemp(path.join(os.tmpdir(),'himr-candidate-inputs-'));
  const root=path.join(project,'candidate');
  try{
    await mkdir(root);
    let calls=0;
    const run=async(command,args)=>{
      calls++;
      assert.equal(command,'python3');
      assert.deepEqual(args,[path.join(project,'scripts/analysis-release-bundle.py'),'restore',
        '--manifest',path.join(project,'analysis-release.json'),'--root',root,
        '--corpus-manifest',path.join(project,'corpus-release.json')]);
    };
    assert.equal(await restorePinnedCandidateAnalysis({project,root,run}),false);
    assert.equal(calls,0);
    await writeFile(path.join(project,'analysis-release.json'),'{}');
    assert.equal(await restorePinnedCandidateAnalysis({project,root,run}),true);
    assert.equal(calls,1);
    await assert.rejects(restorePinnedCandidateAnalysis({project,root,run:async()=>{throw Error('Checksum mismatch');}}),/Checksum mismatch/);
    await rm(path.join(project,'analysis-release.json'));
    await symlink('missing.json',path.join(project,'analysis-release.json'));
    await assert.rejects(restorePinnedCandidateAnalysis({project,root,run}),/Invalid analysis release manifest/);
    assert.equal(calls,1);
  }finally{await rm(project,{recursive:true,force:true});}
});
