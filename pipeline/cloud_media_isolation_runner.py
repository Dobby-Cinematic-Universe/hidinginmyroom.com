"""Explicit, hash-pinned process-local extension of the existing cloud runtime.

Only known pre-submission media failures are isolated. Original disk/runtime code,
paid inspection, release checks, budgets and clients remain unchanged.
"""
import argparse
from contextlib import contextmanager
import hashlib
import json
from pathlib import Path
import sys

LOCAL_FAILURES=frozenset({
    'decoded audio duration differs from the whole recording; review required',
    'prepared WAV is truncated',
    'unreceipted audio exists after interrupted preparation; review required',
    'whole-recording audio preparation failed; source preserved',
})
PAID_FILES=('intent.json','submission.json','reconciled.json','completion.json',
            'terminal-job.json','collection-review.json','submission-untrusted-response.json',
            'provider-transcript.json')


class RecordHeld(RuntimeError):pass
class MediaCircuitOpen(RuntimeError):pass


def classify(error, media_error_class):
    return isinstance(error,media_error_class) and str(error) in LOCAL_FAILURES


@contextmanager
def isolate(cloud, plan_ref, holds_root, extension_ref, emit):
    holds_root=Path(holds_root)
    cloud.io.mkdir(holds_root)
    original_prepare=cloud.media.prepare
    original_inspect=cloud.inspect_job
    original_cycle=cloud.cycle
    streak=0

    def no_paid(plan,row):
        folder=cloud._folder(plan,row)
        paths=[Path(plan['state_root'])/'reservations'/(row['job_id']+'.json')]
        paths.extend(folder/name for name in PAID_FILES)
        if any(cloud.io.safe.exists(p) for p in paths):
            raise cloud.CloudError('media isolation refuses to mask paid evidence')

    def read_hold(plan,ref,row):
        path=holds_root/(row['job_id']+'.json')
        if not cloud.io.safe.exists(path):return None
        hold=cloud.io.read(cloud.io.binding(path))
        if (hold.get('kind')!='himr_pre_submission_media_hold' or hold.get('plan')!=ref
            or hold.get('recording')!=row['recording'] or hold.get('job_id')!=row['job_id']
            or hold.get('extension')!=extension_ref or hold.get('reason') not in LOCAL_FAILURES):
            raise cloud.CloudError('media hold binding differs')
        no_paid(plan,row)
        return hold

    def inspect(plan,ref,row,**kwargs):
        # Original inspection always runs first; a hold never hides damaged proof.
        result=original_inspect(plan,ref,row,**kwargs)
        hold=read_hold(plan,ref,row)
        if hold:
            if result['state'] not in {'ready','waiting_screen'}:
                raise cloud.CloudError('media hold conflicts with original job state')
            return {**result,'state':'needs_review','media_review_reason':hold['reason']}
        return result

    def prepare(recording,folder,ffmpeg):
        nonlocal streak
        try:
            result=original_prepare(recording,folder,ffmpeg)
        except cloud.media.MediaError as error:
            if not classify(error,cloud.media.MediaError):raise
            plan=cloud.load_plan(plan_ref)
            row=next(r for r in plan['recordings'] if cloud._folder(plan,r)==Path(folder))
            if row['recording']!=recording or plan['ffmpeg']!=ffmpeg:
                raise cloud.CloudError('media hold target differs')
            no_paid(plan,row)
            hold={'kind':'himr_pre_submission_media_hold','plan':plan_ref,
                'job_id':row['job_id'],'recording':recording,'extension':extension_ref,
                'reason':str(error),'automatic_retry':False,'new_paid_requests':0,
                'files_preserved':True,'resolution':'manual_repair_and_explicit_hold_clearance'}
            cloud.io.put(holds_root/(row['job_id']+'.json'),hold)
            streak+=1
            emit({'event':'record_media_held','job_id':row['job_id'],'reason':str(error),
                  'consecutive_media_failures':streak,'paid_post_retried':False})
            if streak>=3:
                raise MediaCircuitOpen('three consecutive media preparations failed; inspect shared resources') from None
            raise RecordHeld(row['job_id']) from None
        else:
            streak=0
            return result

    def cycle(ref,**kwargs):
        if ref!=plan_ref:raise cloud.CloudError('media isolation plan differs')
        if kwargs.get('prepare_audio') is not None:
            raise cloud.CloudError('media isolation does not accept another audio builder')
        try:return original_cycle(ref,**kwargs)
        except RecordHeld:
            # The original cycle has released its lock. Paid commits before the
            # failing row remain durable; next cycle inspects them normally.
            return {**cloud.status(ref),'state':'running','media_hold_added':True,
                    'automatic_paid_retries':False}

    cloud.inspect_job=inspect;cloud.media.prepare=prepare;cloud.cycle=cycle
    try:yield
    finally:
        cloud.inspect_job=original_inspect;cloud.media.prepare=original_prepare;cloud.cycle=original_cycle


def main():
    p=argparse.ArgumentParser(description=__doc__)
    for name in ('plan','expected-sha256','execution-release','execution-release-sha256',
                 'runtime-path','runner-sha256','extension-sha256','holds-root','env-file'):
        p.add_argument('--'+name,required=True)
    p.add_argument('--allow-paid-api',action='store_true')
    p.add_argument('--budget-usd',default='150')
    p.add_argument('--max-active',type=int,default=4)
    p.add_argument('--max-runtime-seconds',type=int,default=86400)
    p.add_argument('--poll-seconds',type=int,default=60)
    p.add_argument('--cooldown-seconds',type=int,default=300)
    a=p.parse_args()
    source=Path(__file__).resolve();runtime=Path(a.runtime_path).resolve()
    if hashlib.sha256(source.read_bytes()).hexdigest()!=a.extension_sha256:
        raise RuntimeError('media isolation extension changed')
    if Path.cwd()!=runtime:raise RuntimeError('run from exact approved runtime')
    sys.path.insert(0,str(runtime))
    from pipeline import cloud_transcription_resilient as base
    cloud=base.cloud
    if Path(cloud.__file__).resolve().parent!=runtime/'pipeline':raise RuntimeError('wrong runtime imports')
    cloud.io.read_bytes({'path':str(Path(base.__file__).resolve()),'sha256':a.runner_sha256})
    ref={'path':a.plan,'sha256':a.expected_sha256}
    extension_ref={'path':str(source),'sha256':a.extension_sha256}
    release_ref={'path':a.execution_release,'sha256':a.execution_release_sha256}
    try:
        with base.release.activate(release_ref),cloud.pause_signal() as stopping:
            cloud.load_plan(ref)  # Original release/plan validation before extension.
            with isolate(cloud,ref,a.holds_root,extension_ref,base._emit):
                result=base.run(ref,allow_paid_api=a.allow_paid_api,budget_microusd=cloud.usd(a.budget_usd),
                    max_active=a.max_active,max_runtime_seconds=a.max_runtime_seconds,env_file=a.env_file,
                    poll_seconds=a.poll_seconds,cooldown_seconds=a.cooldown_seconds,stopping=stopping,on_event=base._emit)
        base._emit(result)
        return 2 if result['state'] in {'needs_review','reconciliation_required'} else 0
    except (RuntimeError,OSError,ValueError,KeyError,TypeError) as error:
        base._emit({'state':'stopped','error_type':type(error).__name__,
                    'durable_paid_evidence_preserved':True,'automatic_paid_retries':False})
        return 2


if __name__=='__main__':raise SystemExit(main())
