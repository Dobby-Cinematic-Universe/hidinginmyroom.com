"""Reversible summary consumption index; no mutation of running paid workers."""
import argparse
import hashlib
import json
import os
from pathlib import Path
import signal
import time

MIN_WORDS=50


def transcript_words(doc):
    return sum(len(s['text'].split()) for s in doc['segments'])


def summary_eligible(doc):
    """Shared predicate for future submitter integration; not a live monkeypatch."""
    return transcript_words(doc)>=MIN_WORDS


class Filter:
    def __init__(self,records,output):
        self.records=Path(records).resolve();self.output=Path(output).resolve()
        self.output.mkdir(mode=0o700,parents=True,exist_ok=True)
        self.cache={}

    def read(self,path,expected=None):
        path=Path(path);stat=path.stat();witness=(stat.st_ino,stat.st_size,stat.st_mtime_ns,stat.st_ctime_ns)
        key=str(path)
        if key not in self.cache or self.cache[key][0]!=witness:
            raw=path.read_bytes();after=path.stat()
            if (after.st_ino,after.st_size,after.st_mtime_ns,after.st_ctime_ns)!=witness:
                raise ValueError('input changed during read')
            ref={'path':str(path.resolve()),'sha256':hashlib.sha256(raw).hexdigest()}
            self.cache[key]=(witness,json.loads(raw),ref)
        _,doc,ref=self.cache[key]
        if expected and ref!=expected:raise ValueError('transcript reference differs')
        return doc,ref

    def scan(self):
        eligible=[];withheld=[]
        for path in sorted(self.records.glob('*/plan.json')):
            plan,_=self.read(path)
            sources=plan['request_value']['sources']
            if len(sources)!=1:raise ValueError('expected transcript-level single source')
            source=sources[0];doc,source_ref=self.read(source['transcript']['path'],source['transcript'])
            words=transcript_words(doc)
            exports=[]
            for file in sorted((path.parent/'exports').glob('summaries-*.json')):
                export,ref=self.read(file)
                if export['phase']!='transcripts' or export['plan_id']!=plan['plan_id']:
                    raise ValueError('summary export source mismatch')
                if export['phase_complete']:exports.append(ref)
            entry=dict(recording_id=source['recording_id'],summary_record=path.parent.name,
                transcript=source_ref,word_count=words,summary_exports=exports)
            if words<MIN_WORDS:
                withheld.append({**entry,'reason':'source_transcript_under_50_words',
                    'show_summary':False,'display_instead':'original_transcript',
                    'existing_summaries_preserved':True})
            elif exports:eligible.append(entry)
        result=dict(kind='himr_length_filtered_summary_index',minimum_transcript_words=MIN_WORDS,
            word_count_basis='whitespace_separated_words_in_segment_text_only',
            eligible_summaries=eligible,withheld_sources=withheld,
            original_summaries_modified=False,original_transcripts_modified=False,
            live_submission_gate_installed=False,
            scope='Downstream consumers must use this index; original export directories remain unfiltered.',
            new_paid_requests=0)
        raw=(json.dumps(result,ensure_ascii=False,indent=2)+'\n').encode()
        target=self.output/'index.json'
        if not target.exists() or target.read_bytes()!=raw:
            temp=self.output/('index-'+str(os.getpid())+'.tmp')
            with temp.open('xb') as f:f.write(raw);f.flush();os.fsync(f.fileno())
            os.replace(temp,target)
        return dict(eligible_exported_records=len(eligible),short_sources_withheld=len(withheld),
            short_exported_records_withheld=sum(bool(r['summary_exports']) for r in withheld),
            index=str(target),new_paid_requests=0)


if __name__=='__main__':
    p=argparse.ArgumentParser(description=__doc__)
    p.add_argument('--records',required=True);p.add_argument('--output',required=True)
    p.add_argument('--watch',action='store_true');p.add_argument('--interval',type=int,default=60)
    a=p.parse_args();os.umask(0o077)
    if a.interval<15:raise ValueError('scan interval must be at least 15 seconds')
    running=True
    def stop(*_):
        global running
        running=False
    signal.signal(signal.SIGTERM,stop);signal.signal(signal.SIGINT,stop)
    worker=Filter(a.records,a.output)
    while running:
        print(json.dumps(worker.scan()),flush=True)
        if not a.watch:break
        deadline=time.monotonic()+a.interval
        while running and time.monotonic()<deadline:time.sleep(1)
