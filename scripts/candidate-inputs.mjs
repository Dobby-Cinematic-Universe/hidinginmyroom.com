import {copyFile,lstat} from 'node:fs/promises';
import {spawn} from 'node:child_process';
import path from 'node:path';

export function selectedPreviewDirectory(pointer, explicitPreview) {
  const directory=explicitPreview || pointer.directory;
  if(!/^release-[a-z0-9-]+$/.test(directory || ''))throw Error('Unsafe preview');
  return directory;
}

export function previewArgument(args) {
  if(!args.length)return undefined;
  if(args.length!==1)throw Error('Pass one preview directory');
  const value=args[0].startsWith('--preview=')?args[0].slice('--preview='.length):args[0];
  return selectedPreviewDirectory({},value);
}

const runRestore=(command,args)=>new Promise((resolve,reject)=>{
  const child=spawn(command,args,{stdio:'inherit'});
  child.on('error',reject);
  child.on('exit',code=>code===0?resolve():reject(Error('Analysis restore exited '+code)));
});

export async function restorePinnedCandidateAnalysis({project,root,run=runRestore}) {
  const manifest=path.join(project,'analysis-release.json');
  let stat;
  try{stat=await lstat(manifest);}catch(error){if(error.code==='ENOENT')return false;throw error;}
  if(!stat.isFile() || stat.isSymbolicLink())throw Error('Invalid analysis release manifest');
  const candidateManifest=path.join(root,'analysis-release.json');
  try {
    const target=await lstat(candidateManifest);
    if(!target.isFile() || target.isSymbolicLink())throw Error('Invalid candidate analysis release manifest');
  } catch(error) { if(error.code!=='ENOENT')throw error; }
  // Restore and the subsequent Astro build consume the same exact approved pin.
  await copyFile(manifest,candidateManifest);
  await run('python3',[
    path.join(project,'scripts/analysis-release-bundle.py'),'restore',
    '--manifest',candidateManifest,'--root',root,
    '--corpus-manifest',path.join(project,'corpus-release.json'),
  ]);
  return true;
}
