-- Biometric clustering is a private analytical aid, never a publication source by
-- itself.  These tables supersede the unversioned identity_assertions workflow in
-- 0004 without rewriting that historical migration.

CREATE TABLE identity_cluster_versions (
    identity_cluster_version_id TEXT PRIMARY KEY,
    identity_cluster_id TEXT NOT NULL
        REFERENCES identity_clusters(identity_cluster_id) ON DELETE RESTRICT,
    version_number INTEGER NOT NULL CHECK(version_number >= 1),
    parent_identity_cluster_version_id TEXT
        REFERENCES identity_cluster_versions(identity_cluster_version_id) ON DELETE RESTRICT,
    primary_model_id TEXT NOT NULL REFERENCES models(model_id) ON DELETE RESTRICT,
    primary_processing_run_id TEXT NOT NULL
        REFERENCES processing_runs(processing_run_id) ON DELETE RESTRICT,
    model_snapshot_sha256 TEXT NOT NULL CHECK(length(model_snapshot_sha256) = 64),
    run_snapshot_sha256 TEXT NOT NULL CHECK(length(run_snapshot_sha256) = 64),
    clustering_method TEXT NOT NULL,
    visibility TEXT NOT NULL DEFAULT 'private' CHECK(visibility = 'private'),
    metadata_json TEXT NOT NULL DEFAULT '{}' CHECK(json_valid(metadata_json)),
    created_at TEXT NOT NULL CHECK(julianday(created_at) IS NOT NULL),
    UNIQUE(identity_cluster_id, version_number)
);

CREATE INDEX identity_cluster_versions_cluster_idx
    ON identity_cluster_versions(identity_cluster_id, version_number);

CREATE TRIGGER identity_cluster_versions_lineage_required
BEFORE INSERT ON identity_cluster_versions
WHEN NOT EXISTS (
    SELECT 1
    FROM processing_runs AS run
    WHERE run.processing_run_id = NEW.primary_processing_run_id
      AND run.status = 'completed'
      AND run.completed_at IS NOT NULL
      AND run.model_id = NEW.primary_model_id
)
BEGIN
    SELECT RAISE(ABORT, 'identity cluster version requires a completed matching model/run');
END;

CREATE TRIGGER identity_cluster_versions_first_parent
BEFORE INSERT ON identity_cluster_versions
WHEN NEW.version_number = 1
 AND (
     NEW.parent_identity_cluster_version_id IS NOT NULL
     OR EXISTS (
         SELECT 1 FROM identity_cluster_versions
         WHERE identity_cluster_id = NEW.identity_cluster_id
     )
 )
BEGIN
    SELECT RAISE(ABORT, 'first identity cluster version must be the parentless version 1');
END;

CREATE TRIGGER identity_cluster_versions_next_parent
BEFORE INSERT ON identity_cluster_versions
WHEN NEW.version_number > 1
 AND NOT EXISTS (
     SELECT 1
     FROM identity_cluster_versions AS parent
     WHERE parent.identity_cluster_version_id = NEW.parent_identity_cluster_version_id
       AND parent.identity_cluster_id = NEW.identity_cluster_id
       AND parent.version_number = NEW.version_number - 1
       AND NOT EXISTS (
           SELECT 1
           FROM identity_cluster_versions AS later
           WHERE later.identity_cluster_id = NEW.identity_cluster_id
             AND later.version_number >= NEW.version_number
       )
 )
BEGIN
    SELECT RAISE(ABORT, 'identity cluster versions must form one gapless parent chain');
END;

CREATE TRIGGER identity_cluster_versions_no_update
BEFORE UPDATE ON identity_cluster_versions
BEGIN
    SELECT RAISE(ABORT, 'identity cluster versions are append-only');
END;

CREATE TRIGGER identity_cluster_versions_no_delete
BEFORE DELETE ON identity_cluster_versions
BEGIN
    SELECT RAISE(ABORT, 'identity cluster versions are append-only');
END;

-- Once a model or completed run is cited by a cluster version its database row is
-- frozen.  The two snapshot hashes additionally bind the canonical model and run
-- manifests used outside SQLite.
CREATE TRIGGER identity_cluster_models_no_update
BEFORE UPDATE ON models
WHEN EXISTS (
    SELECT 1 FROM identity_cluster_versions
    WHERE primary_model_id = OLD.model_id
)
BEGIN
    SELECT RAISE(ABORT, 'model is immutable after identity lineage citation');
END;

CREATE TRIGGER identity_cluster_models_no_delete
BEFORE DELETE ON models
WHEN EXISTS (
    SELECT 1 FROM identity_cluster_versions
    WHERE primary_model_id = OLD.model_id
)
BEGIN
    SELECT RAISE(ABORT, 'model is immutable after identity lineage citation');
END;

CREATE TRIGGER identity_cluster_runs_no_update
BEFORE UPDATE ON processing_runs
WHEN EXISTS (
    SELECT 1 FROM identity_cluster_versions
    WHERE primary_processing_run_id = OLD.processing_run_id
)
BEGIN
    SELECT RAISE(ABORT, 'processing run is immutable after identity lineage citation');
END;

CREATE TRIGGER identity_cluster_runs_no_delete
BEFORE DELETE ON processing_runs
WHEN EXISTS (
    SELECT 1 FROM identity_cluster_versions
    WHERE primary_processing_run_id = OLD.processing_run_id
)
BEGIN
    SELECT RAISE(ABORT, 'processing run is immutable after identity lineage citation');
END;

CREATE TRIGGER identity_clusters_no_update
BEFORE UPDATE ON identity_clusters
BEGIN
    SELECT RAISE(ABORT, 'identity clusters are append-only');
END;

CREATE TRIGGER identity_clusters_private_artifact_insert
BEFORE INSERT ON identity_clusters
WHEN NEW.artifact_id IS NOT NULL
 AND NOT EXISTS (
     SELECT 1
     FROM artifacts AS artifact
     WHERE artifact.artifact_id = NEW.artifact_id
       AND artifact.visibility = 'private'
       AND lower(artifact.storage_uri) NOT LIKE 'http://%'
       AND lower(artifact.storage_uri) NOT LIKE 'https://%'
       AND lower(replace(artifact.storage_uri, char(92), '/')) NOT GLOB 'src/*'
       AND lower(replace(artifact.storage_uri, char(92), '/')) NOT GLOB 'public/*'
       AND lower(replace(artifact.storage_uri, char(92), '/')) NOT GLOB 'dist/*'
       AND instr(lower(replace(artifact.storage_uri, char(92), '/')), '/src/data/corpus/') = 0
       AND instr(lower(replace(artifact.storage_uri, char(92), '/')), '/public/') = 0
       AND instr(lower(replace(artifact.storage_uri, char(92), '/')), '/dist/') = 0
       AND (
           lower(replace(artifact.storage_uri, char(92), '/')) GLOB 'research/*'
           OR instr(lower(replace(artifact.storage_uri, char(92), '/')), '/research/') > 0
           OR lower(artifact.storage_uri) LIKE 'private:%'
       )
 )
BEGIN
    SELECT RAISE(ABORT, 'identity cluster artifact must use private storage');
END;

CREATE TRIGGER identity_clusters_no_delete
BEFORE DELETE ON identity_clusters
BEGIN
    SELECT RAISE(ABORT, 'identity clusters are append-only');
END;

CREATE VIEW current_identity_cluster_versions AS
SELECT version.*
FROM identity_cluster_versions AS version
JOIN (
    SELECT identity_cluster_id, MAX(version_number) AS version_number
    FROM identity_cluster_versions
    GROUP BY identity_cluster_id
) AS current
  ON current.identity_cluster_id = version.identity_cluster_id
 AND current.version_number = version.version_number;

-- Raw embeddings, centroids, enrollment crops, and voiceprints must be registered
-- through this private wrapper.  storage_policy is intentionally a constant: an
-- artifact path beneath the public site or repository is never an acceptable state.
CREATE TABLE biometric_artifacts (
    biometric_artifact_id TEXT PRIMARY KEY,
    artifact_id TEXT NOT NULL UNIQUE REFERENCES artifacts(artifact_id) ON DELETE RESTRICT,
    modality TEXT NOT NULL CHECK(modality IN ('face', 'voice', 'audiovisual')),
    biometric_kind TEXT NOT NULL
        CHECK(biometric_kind IN (
            'face_embedding', 'voice_embedding', 'audiovisual_embedding',
            'cluster_centroid', 'enrollment_sample', 'similarity_index', 'other_biometric'
        )),
    storage_policy TEXT NOT NULL DEFAULT 'private_outside_repository'
        CHECK(storage_policy = 'private_outside_repository'),
    created_at TEXT NOT NULL CHECK(julianday(created_at) IS NOT NULL)
);

CREATE TABLE identity_cluster_version_artifacts (
    identity_cluster_version_id TEXT NOT NULL
        REFERENCES identity_cluster_versions(identity_cluster_version_id) ON DELETE RESTRICT,
    biometric_artifact_id TEXT NOT NULL
        REFERENCES biometric_artifacts(biometric_artifact_id) ON DELETE RESTRICT,
    artifact_role TEXT NOT NULL,
    PRIMARY KEY(identity_cluster_version_id, biometric_artifact_id, artifact_role)
);

CREATE TRIGGER biometric_artifacts_private_insert
BEFORE INSERT ON biometric_artifacts
WHEN NOT EXISTS (
    SELECT 1
    FROM artifacts AS artifact
    JOIN processing_runs AS run
      ON run.processing_run_id = artifact.processing_run_id
     AND run.status = 'completed'
     AND run.completed_at IS NOT NULL
    WHERE artifact.artifact_id = NEW.artifact_id
      AND artifact.visibility = 'private'
      AND lower(artifact.storage_uri) NOT LIKE 'http://%'
      AND lower(artifact.storage_uri) NOT LIKE 'https://%'
      AND lower(replace(artifact.storage_uri, char(92), '/')) NOT GLOB 'src/*'
      AND lower(replace(artifact.storage_uri, char(92), '/')) NOT GLOB 'public/*'
      AND lower(replace(artifact.storage_uri, char(92), '/')) NOT GLOB 'dist/*'
      AND instr(lower(replace(artifact.storage_uri, char(92), '/')), '/src/data/corpus/') = 0
      AND instr(lower(replace(artifact.storage_uri, char(92), '/')), '/public/') = 0
      AND instr(lower(replace(artifact.storage_uri, char(92), '/')), '/dist/') = 0
      AND (
          lower(replace(artifact.storage_uri, char(92), '/')) GLOB 'research/*'
          OR instr(lower(replace(artifact.storage_uri, char(92), '/')), '/research/') > 0
          OR lower(artifact.storage_uri) LIKE 'private:%'
      )
)
BEGIN
    SELECT RAISE(ABORT, 'biometric artifact must use private non-web storage outside public paths');
END;

CREATE TRIGGER biometric_artifacts_no_update
BEFORE UPDATE ON biometric_artifacts
BEGIN
    SELECT RAISE(ABORT, 'biometric artifacts are append-only');
END;

CREATE TRIGGER biometric_artifacts_no_delete
BEFORE DELETE ON biometric_artifacts
BEGIN
    SELECT RAISE(ABORT, 'biometric artifacts are append-only');
END;

CREATE TRIGGER identity_cluster_version_artifacts_no_update
BEFORE UPDATE ON identity_cluster_version_artifacts
BEGIN
    SELECT RAISE(ABORT, 'identity cluster artifact links are append-only');
END;

CREATE TRIGGER identity_cluster_version_artifacts_modality
BEFORE INSERT ON identity_cluster_version_artifacts
WHEN NOT EXISTS (
    SELECT 1
    FROM identity_cluster_versions AS version
    JOIN identity_clusters AS cluster
      ON cluster.identity_cluster_id = version.identity_cluster_id
    JOIN biometric_artifacts AS biometric
      ON biometric.biometric_artifact_id = NEW.biometric_artifact_id
    WHERE version.identity_cluster_version_id = NEW.identity_cluster_version_id
      AND biometric.modality = cluster.modality
)
BEGIN
    SELECT RAISE(ABORT, 'biometric artifact modality does not match identity cluster');
END;

CREATE TRIGGER identity_cluster_version_artifacts_no_delete
BEFORE DELETE ON identity_cluster_version_artifacts
BEGIN
    SELECT RAISE(ABORT, 'identity cluster artifact links are append-only');
END;

CREATE TRIGGER cited_biometric_artifacts_no_update
BEFORE UPDATE ON artifacts
WHEN EXISTS (
        SELECT 1 FROM biometric_artifacts
        WHERE artifact_id = OLD.artifact_id
    )
 OR EXISTS (
        SELECT 1 FROM identity_clusters
        WHERE artifact_id = OLD.artifact_id
    )
BEGIN
    SELECT RAISE(ABORT, 'cited biometric artifact is immutable and private');
END;

CREATE TRIGGER cited_biometric_artifacts_no_delete
BEFORE DELETE ON artifacts
WHEN EXISTS (
        SELECT 1 FROM biometric_artifacts
        WHERE artifact_id = OLD.artifact_id
    )
 OR EXISTS (
        SELECT 1 FROM identity_clusters
        WHERE artifact_id = OLD.artifact_id
    )
BEGIN
    SELECT RAISE(ABORT, 'cited biometric artifact is immutable and private');
END;

CREATE TABLE identity_cluster_memberships (
    identity_cluster_membership_id TEXT PRIMARY KEY,
    identity_cluster_version_id TEXT NOT NULL
        REFERENCES identity_cluster_versions(identity_cluster_version_id) ON DELETE RESTRICT,
    observation_id TEXT NOT NULL REFERENCES observations(observation_id) ON DELETE RESTRICT,
    membership_state TEXT NOT NULL
        CHECK(membership_state IN ('member', 'candidate', 'excluded')),
    membership_role TEXT NOT NULL DEFAULT 'ordinary'
        CHECK(membership_role IN ('ordinary', 'exemplar', 'anchor', 'outlier')),
    raw_score REAL,
    calibrated_probability REAL
        CHECK(calibrated_probability IS NULL OR
              (calibrated_probability >= 0 AND calibrated_probability <= 1)),
    calibration_set_id TEXT REFERENCES calibration_sets(calibration_set_id) ON DELETE RESTRICT,
    basis TEXT NOT NULL,
    visibility TEXT NOT NULL DEFAULT 'private' CHECK(visibility = 'private'),
    created_at TEXT NOT NULL CHECK(julianday(created_at) IS NOT NULL),
    CHECK(calibrated_probability IS NULL OR calibration_set_id IS NOT NULL),
    UNIQUE(identity_cluster_version_id, observation_id)
);

CREATE INDEX identity_cluster_memberships_observation_idx
    ON identity_cluster_memberships(observation_id, identity_cluster_version_id);

CREATE TRIGGER identity_cluster_memberships_modality
BEFORE INSERT ON identity_cluster_memberships
WHEN NOT EXISTS (
    SELECT 1
    FROM identity_cluster_versions AS version
    JOIN identity_clusters AS cluster
      ON cluster.identity_cluster_id = version.identity_cluster_id
    WHERE version.identity_cluster_version_id = NEW.identity_cluster_version_id
      AND (
          (cluster.modality = 'face' AND EXISTS (
              SELECT 1 FROM face_track_observations
              WHERE observation_id = NEW.observation_id
          ))
          OR (cluster.modality = 'voice' AND EXISTS (
              SELECT 1 FROM speaker_turn_observations
              WHERE observation_id = NEW.observation_id
          ))
          OR (cluster.modality = 'audiovisual' AND EXISTS (
              SELECT 1 FROM active_speaker_observations
              WHERE observation_id = NEW.observation_id
          ))
      )
)
BEGIN
    SELECT RAISE(ABORT, 'identity cluster membership observation does not match modality');
END;

CREATE TRIGGER identity_cluster_memberships_no_update
BEFORE UPDATE ON identity_cluster_memberships
BEGIN
    SELECT RAISE(ABORT, 'identity cluster memberships are append-only');
END;

CREATE TRIGGER identity_cluster_memberships_no_delete
BEFORE DELETE ON identity_cluster_memberships
BEGIN
    SELECT RAISE(ABORT, 'identity cluster memberships are append-only');
END;

-- Cannot-link decisions are a versioned pair stream.  A current cannot_link or
-- dispute blocks co-membership; a later human clear can supersede it.  Pair ordering
-- makes the constraint symmetric without storing two rows.
CREATE TABLE identity_cannot_link_decisions (
    cannot_link_sequence INTEGER PRIMARY KEY AUTOINCREMENT,
    identity_cannot_link_decision_id TEXT NOT NULL UNIQUE,
    left_observation_id TEXT NOT NULL REFERENCES observations(observation_id) ON DELETE RESTRICT,
    right_observation_id TEXT NOT NULL REFERENCES observations(observation_id) ON DELETE RESTRICT,
    decision TEXT NOT NULL CHECK(decision IN ('cannot_link', 'clear', 'dispute')),
    decision_origin TEXT NOT NULL CHECK(decision_origin IN ('machine', 'human')),
    reviewer_id TEXT REFERENCES reviewers(reviewer_id) ON DELETE RESTRICT,
    review_decision_id TEXT REFERENCES review_decisions(review_decision_id) ON DELETE RESTRICT,
    model_id TEXT REFERENCES models(model_id) ON DELETE RESTRICT,
    processing_run_id TEXT REFERENCES processing_runs(processing_run_id) ON DELETE RESTRICT,
    decided_at TEXT NOT NULL CHECK(julianday(decided_at) IS NOT NULL),
    basis TEXT NOT NULL,
    CHECK(left_observation_id < right_observation_id),
    CHECK(
        (decision_origin = 'machine' AND decision = 'cannot_link'
         AND reviewer_id IS NULL AND review_decision_id IS NULL
         AND model_id IS NOT NULL AND processing_run_id IS NOT NULL)
        OR
        (decision_origin = 'human' AND reviewer_id IS NOT NULL
         AND review_decision_id IS NOT NULL)
    ),
    UNIQUE(left_observation_id, right_observation_id, decided_at, decision)
);

CREATE INDEX identity_cannot_link_pair_idx
    ON identity_cannot_link_decisions(
        left_observation_id, right_observation_id, cannot_link_sequence
    );

CREATE VIEW current_identity_cannot_link_decisions AS
SELECT decision.*
FROM identity_cannot_link_decisions AS decision
JOIN (
    SELECT left_observation_id, right_observation_id,
           MAX(cannot_link_sequence) AS cannot_link_sequence
    FROM identity_cannot_link_decisions
    GROUP BY left_observation_id, right_observation_id
) AS current
  ON current.left_observation_id = decision.left_observation_id
 AND current.right_observation_id = decision.right_observation_id
 AND current.cannot_link_sequence = decision.cannot_link_sequence;

CREATE TRIGGER identity_cannot_link_machine_lineage
BEFORE INSERT ON identity_cannot_link_decisions
WHEN NEW.decision_origin = 'machine'
 AND NOT EXISTS (
     SELECT 1
     FROM processing_runs AS run
     WHERE run.processing_run_id = NEW.processing_run_id
       AND run.status = 'completed'
       AND run.completed_at IS NOT NULL
       AND run.model_id = NEW.model_id
 )
BEGIN
    SELECT RAISE(ABORT, 'machine cannot-link requires a completed matching model/run');
END;

CREATE TRIGGER identity_cannot_link_human_review
BEFORE INSERT ON identity_cannot_link_decisions
WHEN NEW.decision_origin = 'human'
 AND NOT EXISTS (
     SELECT 1
     FROM reviewers AS reviewer
     JOIN review_decisions AS review
       ON review.review_decision_id = NEW.review_decision_id
     WHERE reviewer.reviewer_id = NEW.reviewer_id
       AND reviewer.reviewer_kind = 'human'
       AND reviewer.active = 1
       AND review.reviewer_id = reviewer.reviewer_id
       AND review.target_type = 'identity_cannot_link'
       AND review.target_id = NEW.identity_cannot_link_decision_id
       AND review.reviewed_complete_item = 1
       AND (review.audio_directly_perceived = 1 OR review.video_directly_perceived = 1)
       AND (
           (NEW.decision = 'cannot_link' AND review.decision IN ('accept', 'correct', 'split'))
           OR (NEW.decision = 'clear' AND review.decision IN ('reject', 'correct', 'merge'))
           OR (NEW.decision = 'dispute' AND review.decision IN ('dispute', 'defer'))
       )
 )
BEGIN
    SELECT RAISE(ABORT, 'human cannot-link decision requires matching direct-media review');
END;

CREATE TRIGGER identity_cannot_link_no_current_comembership
BEFORE INSERT ON identity_cannot_link_decisions
WHEN NEW.decision IN ('cannot_link', 'dispute')
 AND EXISTS (
     SELECT 1
     FROM current_identity_cluster_versions AS version
     JOIN identity_cluster_memberships AS left_member
       ON left_member.identity_cluster_version_id = version.identity_cluster_version_id
      AND left_member.observation_id = NEW.left_observation_id
      AND left_member.membership_state = 'member'
     JOIN identity_cluster_memberships AS right_member
       ON right_member.identity_cluster_version_id = version.identity_cluster_version_id
      AND right_member.observation_id = NEW.right_observation_id
      AND right_member.membership_state = 'member'
 )
BEGIN
    SELECT RAISE(ABORT, 'cannot-link conflicts with current identity cluster membership');
END;

CREATE TRIGGER identity_cannot_link_clear_fail_closed
BEFORE INSERT ON identity_cannot_link_decisions
WHEN NEW.decision = 'clear'
 AND EXISTS (
     SELECT 1
     FROM identity_cannot_link_decisions AS existing
     WHERE existing.left_observation_id = NEW.left_observation_id
       AND existing.right_observation_id = NEW.right_observation_id
       AND existing.decision IN ('cannot_link', 'dispute')
       AND julianday(NEW.decided_at) <= julianday(existing.decided_at)
 )
BEGIN
    SELECT RAISE(ABORT, 'cannot-link clear must be a genuinely later human decision');
END;

CREATE TRIGGER identity_cannot_link_no_update
BEFORE UPDATE ON identity_cannot_link_decisions
BEGIN
    SELECT RAISE(ABORT, 'cannot-link decisions are append-only');
END;

CREATE TRIGGER identity_cannot_link_no_delete
BEFORE DELETE ON identity_cannot_link_decisions
BEGIN
    SELECT RAISE(ABORT, 'cannot-link decisions are append-only');
END;

CREATE TRIGGER identity_cannot_link_models_no_update
BEFORE UPDATE ON models
WHEN EXISTS (
        SELECT 1 FROM identity_cannot_link_decisions
        WHERE model_id = OLD.model_id
    )
 OR EXISTS (
        SELECT 1
        FROM biometric_artifacts AS biometric
        JOIN artifacts AS artifact ON artifact.artifact_id = biometric.artifact_id
        JOIN processing_runs AS run ON run.processing_run_id = artifact.processing_run_id
        WHERE run.model_id = OLD.model_id
    )
BEGIN
    SELECT RAISE(ABORT, 'model is immutable after biometric lineage citation');
END;

CREATE TRIGGER identity_cannot_link_models_no_delete
BEFORE DELETE ON models
WHEN EXISTS (
        SELECT 1 FROM identity_cannot_link_decisions
        WHERE model_id = OLD.model_id
    )
 OR EXISTS (
        SELECT 1
        FROM biometric_artifacts AS biometric
        JOIN artifacts AS artifact ON artifact.artifact_id = biometric.artifact_id
        JOIN processing_runs AS run ON run.processing_run_id = artifact.processing_run_id
        WHERE run.model_id = OLD.model_id
    )
BEGIN
    SELECT RAISE(ABORT, 'model is immutable after biometric lineage citation');
END;

CREATE TRIGGER identity_cannot_link_runs_no_update
BEFORE UPDATE ON processing_runs
WHEN EXISTS (
        SELECT 1 FROM identity_cannot_link_decisions
        WHERE processing_run_id = OLD.processing_run_id
    )
 OR EXISTS (
        SELECT 1
        FROM biometric_artifacts AS biometric
        JOIN artifacts AS artifact ON artifact.artifact_id = biometric.artifact_id
        WHERE artifact.processing_run_id = OLD.processing_run_id
    )
BEGIN
    SELECT RAISE(ABORT, 'processing run is immutable after biometric lineage citation');
END;

CREATE TRIGGER identity_cannot_link_runs_no_delete
BEFORE DELETE ON processing_runs
WHEN EXISTS (
        SELECT 1 FROM identity_cannot_link_decisions
        WHERE processing_run_id = OLD.processing_run_id
    )
 OR EXISTS (
        SELECT 1
        FROM biometric_artifacts AS biometric
        JOIN artifacts AS artifact ON artifact.artifact_id = biometric.artifact_id
        WHERE artifact.processing_run_id = OLD.processing_run_id
    )
BEGIN
    SELECT RAISE(ABORT, 'processing run is immutable after biometric lineage citation');
END;

CREATE TRIGGER identity_cluster_memberships_respect_cannot_link
BEFORE INSERT ON identity_cluster_memberships
WHEN NEW.membership_state = 'member'
 AND EXISTS (
     SELECT 1
     FROM identity_cluster_memberships AS member
     JOIN current_identity_cannot_link_decisions AS cannot_link
       ON cannot_link.decision IN ('cannot_link', 'dispute')
      AND cannot_link.left_observation_id =
          CASE
              WHEN member.observation_id < NEW.observation_id
              THEN member.observation_id ELSE NEW.observation_id
          END
      AND cannot_link.right_observation_id =
          CASE
              WHEN member.observation_id < NEW.observation_id
              THEN NEW.observation_id ELSE member.observation_id
          END
     WHERE member.identity_cluster_version_id = NEW.identity_cluster_version_id
       AND member.membership_state = 'member'
 )
BEGIN
    SELECT RAISE(ABORT, 'identity cluster membership violates a current cannot-link');
END;

CREATE TABLE identity_cluster_version_review_decisions (
    cluster_review_sequence INTEGER PRIMARY KEY AUTOINCREMENT,
    identity_cluster_version_review_decision_id TEXT NOT NULL UNIQUE,
    identity_cluster_version_id TEXT NOT NULL
        REFERENCES identity_cluster_versions(identity_cluster_version_id) ON DELETE RESTRICT,
    decision TEXT NOT NULL CHECK(decision IN ('accept', 'reject', 'dispute')),
    reviewer_id TEXT NOT NULL REFERENCES reviewers(reviewer_id) ON DELETE RESTRICT,
    review_decision_id TEXT NOT NULL UNIQUE
        REFERENCES review_decisions(review_decision_id) ON DELETE RESTRICT,
    decided_at TEXT NOT NULL CHECK(julianday(decided_at) IS NOT NULL),
    basis TEXT NOT NULL,
    UNIQUE(identity_cluster_version_id, decided_at, decision)
);

CREATE VIEW current_identity_cluster_version_review_decisions AS
SELECT decision.*
FROM identity_cluster_version_review_decisions AS decision
JOIN (
    SELECT identity_cluster_version_id,
           MAX(cluster_review_sequence) AS cluster_review_sequence
    FROM identity_cluster_version_review_decisions
    GROUP BY identity_cluster_version_id
) AS current
  ON current.identity_cluster_version_id = decision.identity_cluster_version_id
 AND current.cluster_review_sequence = decision.cluster_review_sequence;

CREATE TRIGGER identity_cluster_version_review_human
BEFORE INSERT ON identity_cluster_version_review_decisions
WHEN NOT EXISTS (
    SELECT 1
    FROM identity_cluster_versions AS version
    JOIN identity_clusters AS cluster
      ON cluster.identity_cluster_id = version.identity_cluster_id
    JOIN reviewers AS reviewer
      ON reviewer.reviewer_id = NEW.reviewer_id
    JOIN review_decisions AS review
      ON review.review_decision_id = NEW.review_decision_id
    WHERE version.identity_cluster_version_id = NEW.identity_cluster_version_id
      AND reviewer.reviewer_kind = 'human'
      AND reviewer.active = 1
      AND review.reviewer_id = reviewer.reviewer_id
      AND review.target_type = 'identity_cluster_version'
      AND review.target_id = NEW.identity_cluster_version_id
      AND (
          NEW.decision <> 'accept'
          OR (
              review.decision IN ('accept', 'correct')
              AND review.reviewed_complete_item = 1
              AND (
                  (cluster.modality = 'face' AND review.video_directly_perceived = 1)
                  OR (cluster.modality = 'voice' AND review.audio_directly_perceived = 1)
                  OR (cluster.modality = 'audiovisual'
                      AND review.audio_directly_perceived = 1
                      AND review.video_directly_perceived = 1)
              )
          )
      )
)
BEGIN
    SELECT RAISE(ABORT, 'identity cluster acceptance requires active human direct-media review');
END;

CREATE TRIGGER identity_cluster_version_review_consistent
BEFORE INSERT ON identity_cluster_version_review_decisions
WHEN NEW.decision = 'accept'
 AND (
     NOT EXISTS (
         SELECT 1 FROM identity_cluster_memberships
         WHERE identity_cluster_version_id = NEW.identity_cluster_version_id
           AND membership_state = 'member'
     )
     OR EXISTS (
         SELECT 1
         FROM identity_cluster_memberships AS left_member
         JOIN identity_cluster_memberships AS right_member
           ON right_member.identity_cluster_version_id = left_member.identity_cluster_version_id
          AND left_member.observation_id < right_member.observation_id
          AND right_member.membership_state = 'member'
         JOIN current_identity_cannot_link_decisions AS cannot_link
           ON cannot_link.left_observation_id = left_member.observation_id
          AND cannot_link.right_observation_id = right_member.observation_id
          AND cannot_link.decision IN ('cannot_link', 'dispute')
         WHERE left_member.identity_cluster_version_id = NEW.identity_cluster_version_id
           AND left_member.membership_state = 'member'
     )
 )
BEGIN
    SELECT RAISE(ABORT, 'identity cluster acceptance requires members with no cannot-link conflict');
END;

CREATE TRIGGER identity_cluster_version_review_accept_fail_closed
BEFORE INSERT ON identity_cluster_version_review_decisions
WHEN NEW.decision = 'accept'
 AND EXISTS (
     SELECT 1
     FROM identity_cluster_version_review_decisions AS existing
     WHERE existing.identity_cluster_version_id = NEW.identity_cluster_version_id
       AND existing.decision IN ('reject', 'dispute')
       AND julianday(NEW.decided_at) <= julianday(existing.decided_at)
 )
BEGIN
    SELECT RAISE(ABORT, 'cluster acceptance must postdate a reject or dispute');
END;

CREATE TRIGGER identity_cluster_version_review_no_update
BEFORE UPDATE ON identity_cluster_version_review_decisions
BEGIN
    SELECT RAISE(ABORT, 'identity cluster review decisions are append-only');
END;

CREATE TRIGGER identity_cluster_version_review_no_delete
BEFORE DELETE ON identity_cluster_version_review_decisions
BEGIN
    SELECT RAISE(ABORT, 'identity cluster review decisions are append-only');
END;

-- The stable subject and its decision stream replace mutable legacy mappings.  The
-- subject stays private; only the narrow public_identity_assertions projection can
-- become public, after human media review plus all normal publication gates.
CREATE TABLE identity_assertion_subjects (
    identity_assertion_id TEXT PRIMARY KEY,
    identity_cluster_version_id TEXT NOT NULL
        REFERENCES identity_cluster_versions(identity_cluster_version_id) ON DELETE RESTRICT,
    entity_id TEXT NOT NULL REFERENCES entities(entity_id) ON DELETE RESTRICT,
    visibility TEXT NOT NULL DEFAULT 'private' CHECK(visibility = 'private'),
    created_at TEXT NOT NULL CHECK(julianday(created_at) IS NOT NULL),
    UNIQUE(identity_cluster_version_id, entity_id)
);

CREATE TABLE identity_assertion_decisions (
    assertion_decision_sequence INTEGER PRIMARY KEY AUTOINCREMENT,
    identity_assertion_decision_id TEXT NOT NULL UNIQUE,
    identity_assertion_id TEXT NOT NULL
        REFERENCES identity_assertion_subjects(identity_assertion_id) ON DELETE RESTRICT,
    decision TEXT NOT NULL CHECK(decision IN ('assert', 'withdraw', 'reject', 'dispute')),
    reviewer_id TEXT NOT NULL REFERENCES reviewers(reviewer_id) ON DELETE RESTRICT,
    review_decision_id TEXT NOT NULL UNIQUE
        REFERENCES review_decisions(review_decision_id) ON DELETE RESTRICT,
    decided_at TEXT NOT NULL CHECK(julianday(decided_at) IS NOT NULL),
    basis TEXT NOT NULL,
    UNIQUE(identity_assertion_id, decided_at, decision)
);

CREATE VIEW current_identity_assertion_decisions AS
SELECT decision.*
FROM identity_assertion_decisions AS decision
JOIN (
    SELECT identity_assertion_id,
           MAX(assertion_decision_sequence) AS assertion_decision_sequence
    FROM identity_assertion_decisions
    GROUP BY identity_assertion_id
) AS current
  ON current.identity_assertion_id = decision.identity_assertion_id
 AND current.assertion_decision_sequence = decision.assertion_decision_sequence;

CREATE TRIGGER identity_assertion_decisions_human
BEFORE INSERT ON identity_assertion_decisions
WHEN NOT EXISTS (
    SELECT 1
    FROM identity_assertion_subjects AS assertion
    JOIN identity_cluster_versions AS version
      ON version.identity_cluster_version_id = assertion.identity_cluster_version_id
    JOIN identity_clusters AS cluster
      ON cluster.identity_cluster_id = version.identity_cluster_id
    JOIN reviewers AS reviewer
      ON reviewer.reviewer_id = NEW.reviewer_id
    JOIN review_decisions AS review
      ON review.review_decision_id = NEW.review_decision_id
    WHERE assertion.identity_assertion_id = NEW.identity_assertion_id
      AND reviewer.reviewer_kind = 'human'
      AND reviewer.active = 1
      AND review.reviewer_id = reviewer.reviewer_id
      AND review.target_type = 'identity_assertion'
      AND review.target_id = NEW.identity_assertion_id
      AND (
          NEW.decision <> 'assert'
          OR (
              review.decision IN ('accept', 'correct')
              AND review.reviewed_complete_item = 1
              AND (
                  (cluster.modality = 'face' AND review.video_directly_perceived = 1)
                  OR (cluster.modality = 'voice' AND review.audio_directly_perceived = 1)
                  OR (cluster.modality = 'audiovisual'
                      AND review.audio_directly_perceived = 1
                      AND review.video_directly_perceived = 1)
              )
          )
      )
)
BEGIN
    SELECT RAISE(ABORT, 'identity assertion requires active human direct-media review');
END;

CREATE TRIGGER identity_assertion_requires_accepted_current_cluster
BEFORE INSERT ON identity_assertion_decisions
WHEN NEW.decision = 'assert'
 AND NOT EXISTS (
     SELECT 1
     FROM identity_assertion_subjects AS assertion
     JOIN current_identity_cluster_versions AS current_version
       ON current_version.identity_cluster_version_id = assertion.identity_cluster_version_id
     JOIN current_identity_cluster_version_review_decisions AS cluster_review
       ON cluster_review.identity_cluster_version_id = current_version.identity_cluster_version_id
      AND cluster_review.decision = 'accept'
     WHERE assertion.identity_assertion_id = NEW.identity_assertion_id
 )
BEGIN
    SELECT RAISE(ABORT, 'identity assertion requires the accepted current cluster version');
END;

CREATE TRIGGER identity_assertion_assert_fail_closed
BEFORE INSERT ON identity_assertion_decisions
WHEN NEW.decision = 'assert'
 AND EXISTS (
     SELECT 1
     FROM identity_assertion_decisions AS existing
     WHERE existing.identity_assertion_id = NEW.identity_assertion_id
       AND existing.decision IN ('withdraw', 'reject', 'dispute')
       AND julianday(NEW.decided_at) <= julianday(existing.decided_at)
 )
BEGIN
    SELECT RAISE(ABORT, 'identity assertion must postdate a withdrawal, reject, or dispute');
END;

CREATE TRIGGER identity_assertion_subjects_no_update
BEFORE UPDATE ON identity_assertion_subjects
BEGIN
    SELECT RAISE(ABORT, 'identity assertion subjects are append-only');
END;

CREATE TRIGGER identity_assertion_subjects_no_delete
BEFORE DELETE ON identity_assertion_subjects
BEGIN
    SELECT RAISE(ABORT, 'identity assertion subjects are append-only');
END;

CREATE TRIGGER identity_assertion_decisions_no_update
BEFORE UPDATE ON identity_assertion_decisions
BEGIN
    SELECT RAISE(ABORT, 'identity assertion decisions are append-only');
END;

CREATE TRIGGER identity_assertion_decisions_no_delete
BEFORE DELETE ON identity_assertion_decisions
BEGIN
    SELECT RAISE(ABORT, 'identity assertion decisions are append-only');
END;

CREATE TRIGGER cited_identity_review_decisions_no_update
BEFORE UPDATE ON review_decisions
WHEN EXISTS (
        SELECT 1 FROM identity_cannot_link_decisions
        WHERE review_decision_id = OLD.review_decision_id
    )
 OR EXISTS (
        SELECT 1 FROM identity_cluster_version_review_decisions
        WHERE review_decision_id = OLD.review_decision_id
    )
 OR EXISTS (
        SELECT 1 FROM identity_assertion_decisions
        WHERE review_decision_id = OLD.review_decision_id
    )
BEGIN
    SELECT RAISE(ABORT, 'review decision is immutable after identity citation');
END;

CREATE TRIGGER cited_identity_review_decisions_no_delete
BEFORE DELETE ON review_decisions
WHEN EXISTS (
        SELECT 1 FROM identity_cannot_link_decisions
        WHERE review_decision_id = OLD.review_decision_id
    )
 OR EXISTS (
        SELECT 1 FROM identity_cluster_version_review_decisions
        WHERE review_decision_id = OLD.review_decision_id
    )
 OR EXISTS (
        SELECT 1 FROM identity_assertion_decisions
        WHERE review_decision_id = OLD.review_decision_id
    )
BEGIN
    SELECT RAISE(ABORT, 'review decision is immutable after identity citation');
END;

CREATE VIEW public_identity_assertions AS
SELECT assertion.identity_assertion_id,
       assertion.entity_id,
       cluster.modality,
       decision.decided_at
FROM identity_assertion_subjects AS assertion
JOIN current_identity_assertion_decisions AS decision
  ON decision.identity_assertion_id = assertion.identity_assertion_id
 AND decision.decision = 'assert'
JOIN reviewers AS assertion_reviewer
  ON assertion_reviewer.reviewer_id = decision.reviewer_id
 AND assertion_reviewer.reviewer_kind = 'human'
 AND assertion_reviewer.active = 1
JOIN review_decisions AS assertion_review_record
  ON assertion_review_record.review_decision_id = decision.review_decision_id
 AND assertion_review_record.reviewer_id = assertion_reviewer.reviewer_id
 AND assertion_review_record.reviewed_complete_item = 1
JOIN current_identity_cluster_versions AS version
  ON version.identity_cluster_version_id = assertion.identity_cluster_version_id
JOIN identity_clusters AS cluster
  ON cluster.identity_cluster_id = version.identity_cluster_id
JOIN current_identity_cluster_version_review_decisions AS cluster_review
  ON cluster_review.identity_cluster_version_id = version.identity_cluster_version_id
 AND cluster_review.decision = 'accept'
JOIN reviewers AS cluster_reviewer
  ON cluster_reviewer.reviewer_id = cluster_review.reviewer_id
 AND cluster_reviewer.reviewer_kind = 'human'
 AND cluster_reviewer.active = 1
JOIN review_decisions AS cluster_review_record
  ON cluster_review_record.review_decision_id = cluster_review.review_decision_id
 AND cluster_review_record.reviewer_id = cluster_reviewer.reviewer_id
 AND cluster_review_record.reviewed_complete_item = 1
JOIN public_entities AS entity
  ON entity.entity_id = assertion.entity_id
JOIN publication_eligible_objects AS eligible
  ON eligible.object_type = 'identity_assertion'
 AND eligible.object_id = assertion.identity_assertion_id
JOIN current_publication_decisions AS publication
  ON publication.object_type = 'identity_assertion'
 AND publication.object_id = assertion.identity_assertion_id
 AND publication.decision = 'publish'
JOIN reviewers AS publication_reviewer
  ON publication_reviewer.reviewer_id = publication.reviewer_id
 AND publication_reviewer.reviewer_kind = 'human'
 AND publication_reviewer.active = 1
WHERE (
        (cluster.modality = 'face'
         AND assertion_review_record.video_directly_perceived = 1
         AND cluster_review_record.video_directly_perceived = 1)
        OR (cluster.modality = 'voice'
            AND assertion_review_record.audio_directly_perceived = 1
            AND cluster_review_record.audio_directly_perceived = 1)
        OR (cluster.modality = 'audiovisual'
            AND assertion_review_record.audio_directly_perceived = 1
            AND assertion_review_record.video_directly_perceived = 1
            AND cluster_review_record.audio_directly_perceived = 1
            AND cluster_review_record.video_directly_perceived = 1)
      )
  AND NOT EXISTS (
      SELECT 1
      FROM current_publication_gate_decisions AS gate
      JOIN reviewers AS gate_reviewer ON gate_reviewer.reviewer_id = gate.reviewer_id
      WHERE gate.object_type = 'identity_assertion'
        AND gate.object_id = assertion.identity_assertion_id
        AND (gate_reviewer.reviewer_kind <> 'human' OR gate_reviewer.active <> 1)
  );

-- Generic publication streams intentionally have open object-type text fields.  Stop
-- them from ever allowlisting raw biometric internals, and require an active human
-- reviewed assertion before the narrow identity projection can be allowlisted.
CREATE TRIGGER publication_decisions_block_private_identity_objects
BEFORE INSERT ON publication_decisions
WHEN NEW.decision = 'publish'
 AND (
     NEW.object_type IN (
         'identity_cluster', 'identity_cluster_version',
         'identity_cluster_membership', 'identity_cannot_link',
         'biometric_artifact', 'legacy_identity_assertion'
     )
     OR (NEW.object_type = 'artifact' AND EXISTS (
         SELECT 1 FROM biometric_artifacts WHERE artifact_id = NEW.object_id
     ))
     OR (NEW.object_type = 'artifact' AND EXISTS (
         SELECT 1 FROM identity_clusters WHERE artifact_id = NEW.object_id
     ))
     OR (NEW.object_type = 'observation' AND EXISTS (
         SELECT 1 FROM identity_cluster_memberships WHERE observation_id = NEW.object_id
     ))
 )
BEGIN
    SELECT RAISE(ABORT, 'private biometric identity objects cannot be published');
END;

CREATE TRIGGER publication_gate_decisions_block_private_identity_objects
BEFORE INSERT ON publication_gate_decisions
WHEN NEW.decision = 'clear'
 AND (
     NEW.object_type IN (
         'identity_cluster', 'identity_cluster_version',
         'identity_cluster_membership', 'identity_cannot_link',
         'biometric_artifact', 'legacy_identity_assertion'
     )
     OR (NEW.object_type = 'artifact' AND EXISTS (
         SELECT 1 FROM biometric_artifacts WHERE artifact_id = NEW.object_id
     ))
     OR (NEW.object_type = 'artifact' AND EXISTS (
         SELECT 1 FROM identity_clusters WHERE artifact_id = NEW.object_id
     ))
     OR (NEW.object_type = 'observation' AND EXISTS (
         SELECT 1 FROM identity_cluster_memberships WHERE observation_id = NEW.object_id
     ))
 )
BEGIN
    SELECT RAISE(ABORT, 'private biometric identity objects cannot clear publication gates');
END;

CREATE TRIGGER publication_decisions_identity_assertion_human
BEFORE INSERT ON publication_decisions
WHEN NEW.object_type = 'identity_assertion'
 AND NEW.decision = 'publish'
 AND (
     NOT EXISTS (
         SELECT 1 FROM reviewers
         WHERE reviewer_id = NEW.reviewer_id
           AND reviewer_kind = 'human'
           AND active = 1
     )
     OR NOT EXISTS (
         SELECT 1
         FROM current_identity_assertion_decisions
         WHERE identity_assertion_id = NEW.object_id
           AND decision = 'assert'
     )
 )
BEGIN
    SELECT RAISE(ABORT, 'identity assertion publication requires active human-reviewed assertion');
END;

CREATE TRIGGER publication_gate_decisions_identity_assertion_human
BEFORE INSERT ON publication_gate_decisions
WHEN NEW.object_type = 'identity_assertion'
 AND NEW.decision = 'clear'
 AND NOT EXISTS (
     SELECT 1 FROM reviewers
     WHERE reviewer_id = NEW.reviewer_id
       AND reviewer_kind = 'human'
       AND active = 1
 )
BEGIN
    SELECT RAISE(ABORT, 'identity assertion gates require an active human reviewer');
END;
