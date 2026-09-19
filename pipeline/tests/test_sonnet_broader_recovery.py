from copy import deepcopy
from unittest.mock import patch
from pipeline import sonnet_broader_recovery as recovery


def test_retry_preserves_input_and_original():
    job = {'params': {'system': 'original', 'max_tokens': 8192,
                     'messages': [{'role': 'user', 'content': 'evidence'}]}, 'evidence': [1, 2]}
    before = deepcopy(job)
    params = recovery.retry_params(job)
    assert job == before
    assert params['messages'] == job['params']['messages']
    assert 'e1 through e2' in params['system']
    assert params['max_tokens'] == 16384
    assert recovery.retry_cost(params) > recovery.retry_cost(job['params'])


def test_ambiguous_intent_is_not_resubmitted(tmp_path):
    from unittest.mock import Mock
    folder = tmp_path / 'batch'; folder.mkdir(mode=0o700)
    recovery.r.put(folder / 'submit-intent.json', {'already': 'sealed'})
    state = {'reserved': 1, 'pending': [], 'held': [], 'jobs': {}}
    value = (state, set(), [(folder, {}, [], 1)], [], 0)
    api = Mock()
    with patch.object(recovery, 'overlay', return_value=value):
        result = recovery.tick(tmp_path, tmp_path, api, 100)
    assert result['pending_replacements'] == 1
    api.create_batch.assert_not_called()
    api.get_batch.assert_not_called()


def test_shared_budget_blocks_new_submission(tmp_path):
    from unittest.mock import Mock
    folder = tmp_path / 'batch'; folder.mkdir(mode=0o700)
    state = {'reserved': 99, 'pending': [], 'held': [], 'jobs': {}}
    value = (state, set(), [(folder, {}, [], 2)], [], 0)
    api = Mock()
    with patch.object(recovery, 'overlay', return_value=value):
        recovery.tick(tmp_path, tmp_path, api, 100)
    api.create_batch.assert_not_called()
    assert not (folder / 'submit-intent.json').exists()


def test_failed_replacement_is_not_queued_again(tmp_path):
    from unittest.mock import Mock
    state = {'reserved': 1, 'pending': [], 'held': [{'job_id': 'already_attempted'}], 'jobs': {}}
    value = (state, {'already_attempted'}, [], [], 0)
    with patch.object(recovery, 'overlay', return_value=value):
        result = recovery.tick(tmp_path, tmp_path, Mock(), 100)
    assert result['pending_replacements'] == 0
    assert not (tmp_path / 'batches').exists()
