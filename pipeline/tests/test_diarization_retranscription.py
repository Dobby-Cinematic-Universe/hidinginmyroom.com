from copy import deepcopy
import unittest
from pipeline.diarization_retranscription import select, SELECTED, TOTAL_CAP, BUDGET


class SelectionTests(unittest.TestCase):
    def fixture(self):
        return dict(prior_reserved_microusd=0, recordings=[dict(job_id=job,
            disposition='third_party', import_={'transcript': {}}, maximum_cost_microusd=0,
            recording=dict(state='ready', reasons=[], recording_id=job)) for job in SELECTED])

    def base(self):
        value = self.fixture()
        for row in value['recordings']:
            row['import'] = row.pop('import_')
        return value

    def test_exact_selected_physical_files_and_originals_preserved(self):
        base = self.base()
        base['recordings'][0]['recording'].update(state='review', reasons=['source_id_maps_to_multiple_physical_recordings'])
        before = deepcopy(base)
        rows, _ = select(base)
        self.assertEqual(len(rows), 6)
        self.assertEqual(rows[0][1]['state'], 'ready')
        self.assertEqual(base, before)

    def test_other_admission_failures_and_already_cloud_sources_reject(self):
        for field, value in [('disposition', 'cloud'), ('recording', dict(state='review', reasons=['damaged']))]:
            base = self.base()
            base['recordings'][0][field] = value
            with self.assertRaises(ValueError):
                select(base)

    def test_combined_budget_cap_is_enforced(self):
        base = self.base()
        base['prior_reserved_microusd'] = TOTAL_CAP - BUDGET + 1
        with self.assertRaises(ValueError):
            select(base)
