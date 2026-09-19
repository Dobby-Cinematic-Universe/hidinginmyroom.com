// Read-only public access to explicitly named approved, content-addressed bundles.
export default {async fetch(request,env){
  const key=new URL(request.url).pathname.slice(1);
  if(!['GET','HEAD'].includes(request.method)||!/^releases\/[a-f0-9]{64}\.tar\.gz$/.test(key))return new Response('Not found',{status:404});
  const object=await env.RELEASES.get(key);
  if(!object)return new Response('Not found',{status:404});
  return new Response(request.method==='HEAD'?null:object.body,{headers:{'Content-Type':'application/gzip','Content-Length':String(object.size),'ETag':object.httpEtag,'Cache-Control':'public, max-age=31536000, immutable','X-Content-Type-Options':'nosniff'}});
}};
