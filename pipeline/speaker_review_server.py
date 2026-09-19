"""Loopback-only video/transcript review; append-only manual decisions."""
import argparse
import json
import mimetypes
import os
from pathlib import Path
import re
import secrets
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import urlsplit
from datetime import datetime, timezone
from pipeline.transcript_audio_review import binding, read_bound, write_json

ASSETS = Path(__file__).parent/'speaker_review_ui'


def ranked_names(records, reviews):
    """Count current manual assignments, not superseded save revisions."""
    assignments={};spellings={}
    def remember(name):
        name=' '.join(name.split())
        if name:spellings.setdefault(name.casefold(),name)
        return name.casefold()
    for job,r in records.items():
        for m in r['confirmed'].get('confirmed_mappings',[]):
            assignments[(job,'label',m['label'])]=remember(m['name'])
        for m in r['confirmed'].get('additional_confirmed_participants_without_label',[]):
            remember(m['name'])
    for entry in reviews:
        d=entry['decision'];job=d['job_id']
        if job not in records or entry['transcript']!=records[job]['transcript']:continue
        key=(job,d['scope'],d['label'] if d['scope']=='label' else d['segment_index'])
        assignments[key]=remember(d['name']) if d['source']=='participant' else ''
    counts={key:0 for key in spellings}
    for key in assignments.values():
        if key:counts[key]+=1
    return sorted([dict(name=spellings[k],uses=n) for k,n in counts.items()],
                  key=lambda x:(-x['uses'],x['name'].casefold()))


def review_progress(record, reviews):
    """Completion means a decision was saved, not that attribution is certain."""
    whole={m['label']:'participant' for m in record['confirmed'].get('confirmed_mappings',[])}
    individual={}
    for entry in reviews:
        if entry['transcript']!=record['transcript']:continue
        d=entry['decision']
        if d['scope']=='label':whole[d['label']]=d['source']
        else:individual[(d['segment_index'],d['label'])]=d['source']
    unresolved=0;uncertain=0;labels=set()
    for i,s in enumerate(record['doc']['segments']):
        label=s.get('speaker')
        source=individual.get((i,label),whole.get(label))
        if source == 'uncertain':uncertain+=1
        if source not in {'participant','playback','tts','uncertain'}:
            unresolved+=1;labels.add(label)
    return dict(labeling_complete=unresolved==0,unresolved_turns=unresolved,
                unresolved_labels=len(labels),reviewed_uncertain_turns=uncertain)


def byte_range(value, size):
    if value is None:return 0,size-1,False
    m=re.fullmatch(r'bytes=(\d*)-(\d*)',value)
    if not m or not any(m.groups()):raise ValueError('invalid range')
    a,b=m.groups()
    if not a:
        n=int(b)
        if n<=0:raise ValueError('invalid suffix')
        start,end=max(0,size-n),size-1
    else:start,end=int(a),min(int(b),size-1) if b else size-1
    if start>end or start>=size:raise ValueError('unsatisfiable range')
    return start,end,True


class ReviewServer(ThreadingHTTPServer):
    daemon_threads=True

    def __init__(self, address, report_path, confirmations_path, decisions_path):
        self.records={}
        self.report_path=Path(report_path)
        self.confirmations_path=Path(confirmations_path)
        self.report_witness=None
        self.refresh_records()
        self.token=secrets.token_urlsafe(32)
        self.decisions=Path(decisions_path).resolve()
        self.decisions.mkdir(mode=0o700,parents=True,exist_ok=True)
        super().__init__(address,Handler)

    def refresh_records(self):
        stat=self.report_path.stat()
        witness=(stat.st_ino,stat.st_size,stat.st_mtime_ns,stat.st_ctime_ns)
        if witness==self.report_witness:return
        report=read_bound(binding(self.report_path))
        confirmed=read_bound(binding(self.confirmations_path))
        by_job={r['job_id']:r for r in confirmed['records']}
        records={}
        for r in report['reports']:
            old=self.records.get(r['job_id'])
            doc=old['doc'] if old and old['transcript']==r['transcript'] else read_bound(r['transcript'])
            records[r['job_id']]={'title':r['title'],'transcript':r['transcript'],
                'doc':doc,'confirmed':by_job.get(r['job_id'],{}),
                'flags':{c['segment_index']:c['flags'] for c in r['candidates']}}
        self.records=records
        self.report_witness=witness


class Handler(BaseHTTPRequestHandler):
    def log_message(self,*args):pass

    def allowed(self):
        port=self.server.server_port
        if self.headers.get('Host') not in {f'127.0.0.1:{port}',f'localhost:{port}'}:
            self.send_error(403);return False
        origin=self.headers.get('Origin')
        if origin and origin not in {f'http://127.0.0.1:{port}',f'http://localhost:{port}'}:
            self.send_error(403);return False
        return True

    def respond(self,body,ctype='application/json',status=200):
        if not isinstance(body,bytes):body=json.dumps(body).encode()
        self.send_response(status)
        self.send_header('Content-Type',ctype)
        self.send_header('Content-Length',str(len(body)))
        self.send_header('Cache-Control','no-store')
        self.send_header('X-Content-Type-Options','nosniff')
        self.send_header('Content-Security-Policy',"default-src 'self'; media-src 'self'; frame-ancestors 'none'")
        self.end_headers()
        if self.command!='HEAD':self.wfile.write(body)

    def do_HEAD(self):self.do_GET()

    def do_GET(self):
        if not self.allowed():return
        self.server.refresh_records()
        path=urlsplit(self.path).path
        if path in {'/','/app.js','/style.css'}:
            name='index.html' if path=='/' else path[1:]
            return self.respond((ASSETS/name).read_bytes(),mimetypes.guess_type(name)[0] or 'text/plain')
        if path=='/api/records':
            grouped={k:[] for k in self.server.records}
            for f in sorted(self.server.decisions.glob('*.json')):
                d=json.loads(f.read_text());job=d['decision']['job_id']
                if job in grouped:grouped[job].append(d)
            return self.respond(dict(token=self.server.token,records=[dict(id=k,title=v['title'],
                labels=len({s['speaker'] for s in v['doc']['segments'] if s.get('speaker')}),
                **review_progress(v,grouped[k]))
                for k,v in self.server.records.items()]))
        if path=='/api/names':
            reviews=[json.loads(f.read_text()) for f in sorted(self.server.decisions.glob('*.json'))]
            return self.respond(dict(names=ranked_names(self.server.records,reviews)))
        match=re.fullmatch(r'/(api/record|media)/([a-z0-9_]+)',path)
        if not match or match[2] not in self.server.records:return self.send_error(404)
        kind,job=match.groups();r=self.server.records[job]
        if kind=='media':return self.media(r)
        decisions=[]
        for f in sorted(self.server.decisions.glob(job+'-*.json')):
            d=json.loads(f.read_text())
            if d['transcript']==r['transcript']:decisions.append(d)
        return self.respond(dict(id=job,title=r['title'],segments=r['doc']['segments'],
            duration=r['doc']['duration_seconds'],confirmed=r['confirmed'],
            flags=r['flags'],decisions=decisions,media_url='/media/'+job))

    def media(self,r):
        source=r['doc']['source_media'];p=Path(source['path'])
        try:
            with p.open('rb') as f:
                size=os.fstat(f.fileno()).st_size
                if size!=source['byte_count']:return self.send_error(409,'Media size changed')
                try:start,end,partial=byte_range(self.headers.get('Range'),size)
                except ValueError:
                    self.send_response(416);self.send_header('Content-Range',f'bytes */{size}');self.end_headers();return
                self.send_response(206 if partial else 200)
                # Archive sources are opaque paths; sniff container header.
                header=f.read(16)
                ctype='video/mp4' if header[4:8]==b'ftyp' else 'video/webm' if header[:4]==b'\x1aE\xdf\xa3' else 'application/octet-stream'
                self.send_header('Content-Type',ctype);self.send_header('Accept-Ranges','bytes')
                self.send_header('Content-Length',str(end-start+1));self.send_header('Cache-Control','private, no-store')
                if partial:self.send_header('Content-Range',f'bytes {start}-{end}/{size}')
                self.end_headers()
                if self.command=='HEAD':return
                f.seek(start);remaining=end-start+1
                while remaining:
                    block=f.read(min(262144,remaining))
                    if not block:break
                    self.wfile.write(block);remaining-=len(block)
        except (BrokenPipeError,ConnectionResetError):pass
        except OSError:self.close_connection=True

    def do_POST(self):
        if not self.allowed():return
        self.server.refresh_records()
        if self.path!='/api/decision':return self.send_error(404)
        if self.headers.get('X-Review-Token')!=self.server.token:return self.send_error(403)
        try:
            size=int(self.headers.get('Content-Length','0'))
            if not 0<size<=16384:raise ValueError()
            data=json.loads(self.rfile.read(size))
            if set(data)!={'job_id','scope','segment_index','label','name','source','notes'}:raise ValueError()
            r=self.server.records[data['job_id']]
            if data['scope'] not in {'segment','label'}:raise ValueError()
            labels={s.get('speaker') for s in r['doc']['segments']}
            if data['label'] not in labels or data['label'] is None:raise ValueError()
            i=data['segment_index']
            if type(i) is not int or not 0<=i<len(r['doc']['segments']):raise ValueError()
            if r['doc']['segments'][i]['speaker']!=data['label']:raise ValueError()
            if data['source'] not in {'participant','playback','tts','uncertain'}:raise ValueError()
            if not isinstance(data['name'],str) or len(data['name'])>100:raise ValueError()
            if not isinstance(data['notes'],str) or len(data['notes'])>4000:raise ValueError()
            if data['source']!='participant' and data['name'].strip():raise ValueError()
            value=dict(kind='himr_manual_video_review_decision',transcript=r['transcript'],
                authority='local_user_review',created_at=datetime.now(timezone.utc).isoformat(),
                decision=data,production_applied=False)
            identifier=data['job_id']+'-'+datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%S%f')+'-'+secrets.token_hex(4)
            write_json(self.server.decisions/(identifier+'.json'),value)
            self.respond(dict(saved=True,id=identifier))
        except (ValueError,KeyError,TypeError):self.send_error(400,'Invalid review decision')


if __name__=='__main__':
    p=argparse.ArgumentParser(description=__doc__)
    p.add_argument('--report',required=True);p.add_argument('--confirmations',required=True)
    p.add_argument('--decisions',required=True);p.add_argument('--port',type=int,default=8766)
    a=p.parse_args();os.umask(0o077)
    server=ReviewServer(('127.0.0.1',a.port),a.report,a.confirmations,a.decisions)
    print(f'Review UI: http://127.0.0.1:{server.server_port}',flush=True)
    server.serve_forever()
