"""Offline MiniLM embeddings and bounded complete-link related-account groups.

Only model downloads happen during setup. This command never sends corpus text
to an API. Embeddings are cached by exact text, tokenizer, model and pooling recipe.
"""
import argparse
from collections import Counter
import hashlib
import json
import os
from pathlib import Path
import re
import sqlite3
import time

REVISION='1110a243fdf4706b3f48f1d95db1a4f5529b4d41'
MODEL='sentence-transformers/all-MiniLM-L6-v2'


def sha(data):return hashlib.sha256(data).hexdigest()


def explicit_years(row):
    return set(re.findall(r'\b(?:19|20)\d{2}\b',' '.join(row.get('dates',[]))))


def required_similarity(a,b,informative):
    # Keep distinct relationship transitions apart even when the same two names
    # dominate the embedding. Negated claims of the same transition can still
    # appear together as conflicting accounts, without resolving either claim.
    def transitions(row):
        text=row.get('text','').lower()
        return {name for name,pattern in (
            ('marriage',r'\b(?:married|marriage|wedding)\b'),
            ('divorce',r'\b(?:divorced?|divorcing|separated)\b')) if re.search(pattern,text)}
    ta,tb=transitions(a),transitions(b)
    if ta and tb and ta.isdisjoint(tb):return 1.01
    ya,yb=explicit_years(a),explicit_years(b)
    # A recording date is deliberately never used here. Explicit years in the
    # descriptions are a conservative incompatibility signal, not assigned dates.
    if ya and yb and ya.isdisjoint(yb):return 1.01
    shared=set(a['entities'])&set(b['entities'])
    if shared&informative:return .82
    if ya&yb:return .86
    return .90


def cluster(rows,vectors,entities,max_group=24):
    import numpy as np
    n=len(rows);df=Counter(e for r in rows for e in set(r['entities']))
    informative={e['id'] for e in entities if e['label'].casefold()!='daniel'
        and e['type'] in {'person','unknown'} and df[e['id']]<=max(5,n*.1)}
    edges=[];neighbors=min(32,n)
    for start in range(0,n,256):
        scores=vectors[start:start+256]@vectors.T
        candidates=np.argpartition(-scores,neighbors-1,axis=1)[:,:neighbors]
        for local,js in enumerate(candidates):
            i=start+local
            for j in js.tolist():
                if j<=i:continue
                score=float(scores[local,j])
                if score>=required_similarity(rows[i],rows[j],informative):edges.append((score,i,j))
    edges.sort(key=lambda e:(-e[0],rows[e[1]]['id'],rows[e[2]]['id']))
    parent=list(range(n));members={i:[i] for i in range(n)}
    def root(i):
        while parent[i]!=i:parent[i]=parent[parent[i]];i=parent[i]
        return i
    for _,i,j in edges:
        a,b=root(i),root(j)
        if a==b or len(members[a])+len(members[b])>max_group:continue
        left,right=members[a],members[b]
        cross=vectors[left]@vectors[right].T
        if any(float(cross[x,y])<required_similarity(rows[u],rows[v],informative)
               for x,u in enumerate(left) for y,v in enumerate(right)):continue
        parent[b]=a;members[a]=left+right;del members[b]
    groups=[]
    for indices in members.values():
        if len(indices)<2:continue
        ids=sorted(rows[i]['id'] for i in indices)
        matrix=vectors[indices]@vectors[indices].T
        representative=indices[int(np.argmax(matrix.mean(axis=1)))]
        groups.append(dict(id='accounts_'+sha(json.dumps(ids,separators=(',',':')).encode())[:24],
            members=ids,representative=rows[representative]['id'],minimum_similarity=round(float(matrix.min()),5),
            recording_count=len({rid for i in indices for rid in rows[i]['recordings']})))
    return sorted(groups,key=lambda g:(-g['recording_count'],-len(g['members']),g['id']))


def embeddings(rows,runtime):
    import numpy as np
    import onnxruntime as ort
    from tokenizers import Tokenizer
    model=(runtime/'model.onnx').read_bytes();tokenizer_bytes=(runtime/'tokenizer.json').read_bytes()
    recipe=sha(model)+':'+sha(tokenizer_bytes)+':attention-masked-mean-pool-normalize-256-overflow-v2'
    db=sqlite3.connect(runtime/'embeddings.sqlite3')
    db.execute('CREATE TABLE IF NOT EXISTS embeddings (key TEXT PRIMARY KEY, vector BLOB NOT NULL)')
    keys=[sha((recipe+'\n'+r['text']).encode()) for r in rows]
    vectors=np.zeros((len(rows),384),dtype=np.float32);missing=[]
    for i,k in enumerate(keys):
        cached=db.execute('SELECT vector FROM embeddings WHERE key=?',(k,)).fetchone()
        if cached:
            v=np.frombuffer(cached[0],dtype=np.float32)
            if v.shape!=(384,) or not np.isfinite(v).all() or not .99<float(np.linalg.norm(v))<1.01:raise ValueError('invalid embedding cache')
            vectors[i]=v
        else:missing.append(i)
    model_info=dict(name=MODEL,revision=REVISION,onnx_variant='model_quint8_avx2.onnx',
        model_sha256=sha(model),tokenizer_sha256=sha(tokenizer_bytes),recipe=recipe,dimensions=384)
    print(json.dumps(dict(stage='embedding',events=len(rows),cached=len(rows)-len(missing),new=len(missing))),flush=True)
    if missing:
        tokenizer=Tokenizer.from_str(tokenizer_bytes.decode());tokenizer.no_padding();tokenizer.enable_truncation(max_length=256,stride=32)
        options=ort.SessionOptions();options.intra_op_num_threads=4;options.inter_op_num_threads=1
        session=ort.InferenceSession(model,sess_options=options,providers=['CPUExecutionProvider'])
        inputs={i.name for i in session.get_inputs()}
        for offset in range(0,len(missing),32):
            batch=missing[offset:offset+32];encoded=tokenizer.encode_batch([rows[i]['text'] for i in batch]);chunks=[];owners=[]
            for owner,e in enumerate(encoded):
                for chunk in [e,*e.overflowing]:chunks.append(chunk);owners.append(owner)
            totals=np.zeros((len(batch),384),dtype=np.float32);weights=np.zeros(len(batch),dtype=np.float32)
            for pos in range(0,len(chunks),32):
                part=chunks[pos:pos+32];width=max(len(e.ids) for e in part)
                ids=np.zeros((len(part),width),dtype=np.int64);mask=np.zeros_like(ids);types=np.zeros_like(ids)
                for j,e in enumerate(part):ids[j,:len(e.ids)]=e.ids;mask[j,:len(e.ids)]=e.attention_mask;types[j,:len(e.ids)]=e.type_ids
                values={'input_ids':ids,'attention_mask':mask,'token_type_ids':types}
                hidden=session.run(None,{k:v for k,v in values.items() if k in inputs})[0]
                pooled=(hidden*mask[:,:,None]).sum(axis=1)/mask.sum(axis=1)[:,None]
                for j,v in enumerate(pooled):
                    owner=owners[pos+j];weight=int(mask[j].sum());totals[owner]+=v*weight;weights[owner]+=weight
            totals/=weights[:,None];norm=np.linalg.norm(totals,axis=1,keepdims=True)
            if not np.isfinite(totals).all() or (norm<=0).any():raise ValueError('invalid model embeddings')
            totals/=norm
            for i,v in zip(batch,totals):vectors[i]=v;db.execute('INSERT OR REPLACE INTO embeddings VALUES (?,?)',(keys[i],v.tobytes()))
            db.commit()
            if offset%512==0:print(json.dumps(dict(stage='embedding',done=min(offset+32,len(missing)),total=len(missing))),flush=True)
    db.close();return vectors,model_info


def main():
    p=argparse.ArgumentParser(description=__doc__);p.add_argument('--input',type=Path,required=True);p.add_argument('--output',type=Path,required=True);p.add_argument('--runtime',type=Path,required=True)
    a=p.parse_args();os.umask(0o077);started=time.monotonic();data=json.loads(a.input.read_text())
    if data['schema_version']!=1 or not data['events']:raise ValueError('empty/invalid event input')
    if sha(json.dumps(data['events'],ensure_ascii=False,separators=(',',':')).encode())!=data['input_sha256']:
        raise ValueError('event input digest mismatch')
    rows=data['events'];vectors,model=embeddings(rows,a.runtime)
    print(json.dumps(dict(stage='clustering')),flush=True);groups=cluster(rows,vectors,data['entities'])
    result=dict(schema_version=1,kind='himr_local_related_accounts',input_sha256=data['input_sha256'],model=model,
        policy=dict(kind='bounded_complete_link',max_group_size=24,informative_entity_threshold=.82,
            shared_explicit_year_threshold=.86,otherwise_threshold=.90,recording_date_constraint=False,
            distinct_relationship_transitions_separated=True,
            no_identity_inference=True,no_single_event_assertion=True),groups=groups,
        stats=dict(events=len(rows),groups=len(groups),grouped_descriptions=sum(len(g['members']) for g in groups),
            multi_recording_groups=sum(g['recording_count']>1 for g in groups),elapsed_seconds=round(time.monotonic()-started,2)))
    temp=a.output.with_suffix('.tmp');temp.write_text(json.dumps(result,ensure_ascii=False,separators=(',',':'))+'\n');os.replace(temp,a.output)
    print(json.dumps(result['stats']),flush=True)


if __name__=='__main__':main()
