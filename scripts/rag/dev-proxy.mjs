import {readFile} from 'node:fs/promises';
import path from 'node:path';
export function privateRagProxy(){return {name:'private-rag-loopback',apply:'serve',configureServer(server){
  const root=path.resolve('research/cloudflare-rag/pilot-v1');
  server.middlewares.use('/api/private-rag',async(req,res)=>{
    const reply=(status,body)=>{res.writeHead(status,{'Content-Type':'application/json','Cache-Control':'no-store'});res.end(JSON.stringify(body));};
    const host=req.headers.host||'';
    if(!/^(localhost|127\.0\.0\.1|\[::1\])(:\d+)?$/.test(host)||!['127.0.0.1','::1','::ffff:127.0.0.1'].includes(req.socket.remoteAddress))return reply(403,{error:'Local pilot only.'});
    if(req.headers.origin!==`http://${host}`)return reply(403,{error:'Same-origin requests only.'});
    if(req.method!=='POST'||!req.headers['content-type']?.startsWith('application/json'))return reply(405,{error:'JSON POST required.'});
    try{
      let bytes=0,parts=[];for await(const p of req){bytes+=p.length;if(bytes>4096)return reply(413,{error:'Request too large.'});parts.push(p);}
      const body=JSON.parse(Buffer.concat(parts).toString());
      const connection=JSON.parse(await readFile(path.join(root,'connection.json'),'utf8'));
      const manifest=JSON.parse(await readFile(path.join(root,'manifest.json'),'utf8'));
      const remote=await fetch(connection.url+'/query',{method:'POST',headers:{Authorization:`Bearer ${connection.token}`,'Content-Type':'application/json'},body:JSON.stringify(body),signal:AbortSignal.timeout(60000)});
      const result=await remote.json();
      const known=new Map(manifest.documents.map(d=>[d.key,d]));
      if(result.chunks)result.chunks=result.chunks.map(c=>{const d=known.get(c.key);return {id:c.id,text:c.text,title:d?.title||'Source passage',href:d?.href||null,kind:d?.kind||'source'};});
      reply(remote.status,result);
    }catch{reply(503,{error:'Private pilot unavailable. Indexing may still be running.'});}
  });
}};}
