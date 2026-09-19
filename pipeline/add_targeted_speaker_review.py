"""Add one completed targeted transcript to a live, reloadable review index."""
import argparse
from copy import deepcopy
from pathlib import Path
import os
from pipeline import transcript_summary as r
from pipeline.reviewed_transcript_feed import atomic


def admit(report_path, plan_path, job_id, audit_root):
    report_ref = r.binding(report_path)
    report = r.read(report_ref)
    plan_ref = r.binding(plan_path)
    plan = r.read(plan_ref)
    row, = [x for x in plan['recordings'] if x['job_id'] == job_id]
    folder = Path(plan['state_root']) / 'jobs' / job_id
    completion = r.read(r.binding(folder / 'completion.json'))
    ref = completion['transcript']; doc = r.read(ref)
    if (doc['status'] != 'completed' or doc['job_id'] != job_id
            or doc['recording_id'] != row['recording']['recording_id']):
        raise r.Error('completed targeted transcript identity differs')
    labels = sorted({s['speaker'] for s in doc['segments'] if s.get('speaker')})
    if len(labels) < 2: raise r.Error('expected multiple anonymous speaker labels')
    previous = [x for x in report['reports'] if x['job_id'] == job_id]
    if previous:
        if previous[0]['transcript'] != ref: raise r.Error('existing review uses a different transcript')
        return dict(state='already_present', job_id=job_id)
    r.mkdir(audit_root)
    backup = r.put(audit_root / 'previous-review-index.json', report)
    updated = deepcopy(report)
    updated['reports'].append(dict(job_id=job_id, title=row['recording']['title'], transcript=ref,
        candidates=[], recovered_copy=True, targeted_review_plan=plan_ref))
    if r.binding(report_path) != report_ref: raise r.Error('review index changed during preparation')
    atomic(report_path, updated)
    result = dict(state='added', job_id=job_id, title=row['recording']['title'],
        labels=labels, turns=len(doc['segments']), duration_seconds=doc['duration_seconds'],
        previous_report=backup, report=r.binding(report_path), transcript=ref,
        existing_reviews_modified=False, paid_requests=0)
    r.put(audit_root / 'admission.json', result)
    return result


if __name__ == '__main__':
    os.umask(0o077)
    p = argparse.ArgumentParser(description=__doc__)
    for name in ('report','plan','audit-root'): p.add_argument('--'+name, type=Path, required=True)
    p.add_argument('--job', required=True)
    a = p.parse_args()
    print(r.canonical(admit(a.report.resolve(), a.plan.resolve(), a.job, a.audit_root.resolve())).decode())
