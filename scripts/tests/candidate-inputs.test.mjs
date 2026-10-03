import test from 'node:test';
import assert from 'node:assert/strict';
import {mkdtemp,mkdir,readFile,writeFile,symlink,rm} from 'node:fs/promises';
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
        '--manifest',path.join(root,'analysis-release.json'),'--root',root,
        '--corpus-manifest',path.join(project,'corpus-release.json')]);
    };
    assert.equal(await restorePinnedCandidateAnalysis({project,root,run}),false);
    assert.equal(calls,0);
    await writeFile(path.join(project,'analysis-release.json'),'{}');
    assert.equal(await restorePinnedCandidateAnalysis({project,root,run}),true);
    assert.equal(await readFile(path.join(root,'analysis-release.json'),'utf8'),'{}');
    assert.equal(calls,1);
    await assert.rejects(restorePinnedCandidateAnalysis({project,root,run:async()=>{throw Error('Checksum mismatch');}}),/Checksum mismatch/);
    await rm(path.join(project,'analysis-release.json'));
    await symlink('missing.json',path.join(project,'analysis-release.json'));
    await assert.rejects(restorePinnedCandidateAnalysis({project,root,run}),/Invalid analysis release manifest/);
    assert.equal(calls,1);
  }finally{await rm(project,{recursive:true,force:true});}
});

test('candidate pin retains historical compatibility bytes and rejects a symlink destination',async()=>{
  const project=await mkdtemp(path.join(os.tmpdir(),'himr-candidate-pin-'));
  const root=path.join(project,'candidate');
  try {
    await mkdir(root);
    const pin=JSON.stringify({corpus_release:'old',compatible_corpus_release:'release_'+'b'.repeat(24),compatibility_basis:'reviewed_transcript_update_historical_scores'},null,2)+'\n';
    await writeFile(path.join(project,'analysis-release.json'),pin);
    await restorePinnedCandidateAnalysis({project,root,run:async()=>{}});
    assert.equal(await readFile(path.join(root,'analysis-release.json'),'utf8'),pin);
    await rm(path.join(root,'analysis-release.json'));
    await symlink(path.join(project,'analysis-release.json'),path.join(root,'analysis-release.json'));
    await assert.rejects(restorePinnedCandidateAnalysis({project,root,run:async()=>{throw Error('must not run');}}),/Invalid candidate analysis release manifest/);
  } finally {await rm(project,{recursive:true,force:true});}
});
