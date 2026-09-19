-- A sealed v2 comparison run has exactly two extraction-result inputs and two raw
-- fingerprint inputs.  The required rows are checked by the 0015 admission trigger;
-- this migration rejects pre-admission extras and any post-admission insertion.

CREATE TRIGGER audio_fingerprint_match_candidates_v2_exact_input_count
BEFORE INSERT ON audio_fingerprint_match_candidates_v2
BEGIN
    SELECT CASE WHEN (
        SELECT count(*) FROM run_inputs AS input
        WHERE input.processing_run_id = NEW.processing_run_id
    ) <> 4 THEN RAISE(ABORT, 'audio fingerprint v2 comparison requires exactly four inputs') END;
END;

CREATE TRIGGER audio_fingerprint_match_candidates_v2_input_no_insert
BEFORE INSERT ON run_inputs
WHEN EXISTS (
    SELECT 1 FROM audio_fingerprint_match_candidates_v2 AS typed
    WHERE typed.processing_run_id = NEW.processing_run_id
)
BEGIN
    SELECT RAISE(ABORT, 'audio fingerprint v2 run inputs are append-only');
END;
