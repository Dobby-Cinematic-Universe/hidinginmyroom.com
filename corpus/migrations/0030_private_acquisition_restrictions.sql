-- Private local acquisition policy is durable catalog state, not source metadata.
-- Each row binds one exact result/work-order pair to both the admitted media bytes
-- and the producer source.  Rows are append-only; a later metadata winner cannot
-- erase or weaken the restriction.

CREATE TABLE acquisition_handling_restrictions (
    restriction_sequence INTEGER PRIMARY KEY AUTOINCREMENT,
    acquisition_handling_restriction_id TEXT NOT NULL UNIQUE,
    import_batch_id TEXT NOT NULL UNIQUE
        REFERENCES import_batches(import_batch_id) ON DELETE RESTRICT,
    source_id TEXT NOT NULL REFERENCES sources(source_id) ON DELETE RESTRICT,
    media_id TEXT NOT NULL REFERENCES media_objects(media_id) ON DELETE RESTRICT,
    work_order_sha256 TEXT NOT NULL CHECK(
        length(work_order_sha256) = 64
        AND work_order_sha256 NOT GLOB '*[^0-9a-f]*'
    ),
    result_canonical_sha256 TEXT NOT NULL UNIQUE CHECK(
        length(result_canonical_sha256) = 64
        AND result_canonical_sha256 NOT GLOB '*[^0-9a-f]*'
    ),
    source_metadata_sha256 TEXT NOT NULL CHECK(
        length(source_metadata_sha256) = 64
        AND source_metadata_sha256 NOT GLOB '*[^0-9a-f]*'
    ),
    policy_sha256 TEXT NOT NULL CHECK(
        length(policy_sha256) = 64
        AND policy_sha256 NOT GLOB '*[^0-9a-f]*'
    ),
    seal_plan_sha256 TEXT NOT NULL CHECK(
        length(seal_plan_sha256) = 64
        AND seal_plan_sha256 NOT GLOB '*[^0-9a-f]*'
    ),
    storage_scope TEXT NOT NULL CHECK(storage_scope = 'private_canonical_cache'),
    publication_disposition TEXT NOT NULL CHECK(
        publication_disposition IN ('no_publication_authority', 'never_publish')
    ),
    publication_authority TEXT NOT NULL CHECK(publication_authority = 'none'),
    basis TEXT NOT NULL CHECK(length(trim(basis)) BETWEEN 1 AND 1000),
    policy_json TEXT NOT NULL CHECK(
        json_valid(policy_json)
        AND json_type(policy_json, '$') = 'object'
        AND json_extract(policy_json, '$.storage_scope') = storage_scope
        AND json_extract(policy_json, '$.publication_disposition') =
            publication_disposition
        AND json_extract(policy_json, '$.publication_authority') =
            publication_authority
        AND json_extract(policy_json, '$.basis') = basis
    ),
    seal_validated_at TEXT NOT NULL CHECK(julianday(seal_validated_at) IS NOT NULL),
    recorded_at TEXT NOT NULL CHECK(julianday(recorded_at) IS NOT NULL),
    UNIQUE(source_id, media_id, result_canonical_sha256)
);

CREATE INDEX acquisition_handling_restrictions_source_idx
    ON acquisition_handling_restrictions(source_id, restriction_sequence);
CREATE INDEX acquisition_handling_restrictions_media_idx
    ON acquisition_handling_restrictions(media_id, restriction_sequence);

CREATE TRIGGER acquisition_handling_restrictions_no_update
BEFORE UPDATE ON acquisition_handling_restrictions
BEGIN
    SELECT RAISE(ABORT, 'acquisition handling restrictions are append-only');
END;

CREATE TRIGGER acquisition_handling_restrictions_no_delete
BEFORE DELETE ON acquisition_handling_restrictions
BEGIN
    SELECT RAISE(ABORT, 'acquisition handling restrictions are append-only');
END;

CREATE TRIGGER acquisition_handling_restrictions_exact_provenance
BEFORE INSERT ON acquisition_handling_restrictions
WHEN (SELECT count(*) FROM json_each(NEW.policy_json)) <> 4
 OR EXISTS (
        SELECT 1 FROM json_each(NEW.policy_json) AS field
        WHERE field.key NOT IN (
            'storage_scope', 'publication_disposition',
            'publication_authority', 'basis'
        )
    )
 OR NOT EXISTS (
        SELECT 1
        FROM import_batches AS batch
        WHERE batch.import_batch_id = NEW.import_batch_id
          AND batch.importer_name = 'acquisition_result_v1'
          AND batch.input_sha256 = NEW.result_canonical_sha256
    )
 OR NOT EXISTS (
        SELECT 1
        FROM media_sources AS link
        WHERE link.media_id = NEW.media_id
          AND link.source_id = NEW.source_id
    )
 OR NOT EXISTS (
        SELECT 1
        FROM source_metadata_observations AS observation
        WHERE observation.import_batch_id = NEW.import_batch_id
          AND observation.source_id = NEW.source_id
          AND observation.access_state = 'unknown'
          AND json_extract(
                  observation.metadata_json, '$.acquisition_adapter'
              ) = 'local_file'
          AND json_extract(
                  observation.metadata_json,
                  '$.handling_policy.storage_scope'
              ) = NEW.storage_scope
          AND json_extract(
                  observation.metadata_json,
                  '$.handling_policy.publication_disposition'
              ) = NEW.publication_disposition
          AND json_extract(
                  observation.metadata_json,
                  '$.handling_policy.publication_authority'
              ) = NEW.publication_authority
          AND json_extract(
                  observation.metadata_json, '$.handling_policy.basis'
              ) = NEW.basis
    )
BEGIN
    SELECT RAISE(ABORT, 'acquisition handling restriction lacks exact import provenance');
END;

-- Restrictions propagate conservatively through the source/media graph, derived
-- media, recording mappings, and transcript ownership.  The recursive UNION is
-- cycle-safe and retains the strongest disposition observed on every reachable
-- object.
CREATE VIEW effective_acquisition_handling_restrictions AS
WITH RECURSIVE restriction_graph(object_type, object_id, disposition) AS (
    SELECT 'source', source_id, publication_disposition
    FROM acquisition_handling_restrictions
    UNION
    SELECT 'media', media_id, publication_disposition
    FROM acquisition_handling_restrictions
    UNION
    SELECT 'media', link.media_id, graph.disposition
    FROM restriction_graph AS graph
    JOIN media_sources AS link
      ON graph.object_type = 'source'
     AND link.source_id = graph.object_id
    UNION
    SELECT 'source', link.source_id, graph.disposition
    FROM restriction_graph AS graph
    JOIN media_sources AS link
      ON graph.object_type = 'media'
     AND link.media_id = graph.object_id
    UNION
    SELECT 'media', derivation.child_media_id, graph.disposition
    FROM restriction_graph AS graph
    JOIN media_derivations AS derivation
      ON graph.object_type = 'media'
     AND derivation.parent_media_id = graph.object_id
    UNION
    SELECT 'recording', link.recording_id, graph.disposition
    FROM restriction_graph AS graph
    JOIN recording_sources AS link
      ON graph.object_type = 'source'
     AND link.source_id = graph.object_id
     AND link.confidence_state <> 'rejected'
    UNION
    SELECT 'recording', rendition.recording_id, graph.disposition
    FROM restriction_graph AS graph
    JOIN renditions AS rendition
      ON graph.object_type = 'media'
     AND rendition.media_id = graph.object_id
     AND rendition.review_state <> 'rejected'
    UNION
    SELECT 'transcript_revision', revision.revision_id, graph.disposition
    FROM restriction_graph AS graph
    JOIN transcript_revisions AS revision
      ON graph.object_type = 'recording'
     AND revision.recording_id = graph.object_id
)
SELECT object_type,
       object_id,
       CASE max(CASE disposition WHEN 'never_publish' THEN 2 ELSE 1 END)
           WHEN 2 THEN 'never_publish'
           ELSE 'no_publication_authority'
       END AS publication_disposition,
       'none' AS publication_authority
FROM restriction_graph
GROUP BY object_type, object_id;

-- New publication authority cannot be asserted while the effective restriction is
-- present.  Restrictive publication decisions remain available for remediation.
CREATE TRIGGER publication_decisions_private_acquisition_fail_closed
BEFORE INSERT ON publication_decisions
WHEN NEW.decision = 'publish'
 AND EXISTS (
     SELECT 1
     FROM effective_acquisition_handling_restrictions AS restriction
     WHERE restriction.object_type = NEW.object_type
       AND restriction.object_id = NEW.object_id
 )
BEGIN
    SELECT RAISE(ABORT, 'private acquisition restriction forbids publication');
END;

CREATE TRIGGER publication_gate_decisions_private_acquisition_fail_closed
BEFORE INSERT ON publication_gate_decisions
WHEN NEW.decision = 'clear'
 AND EXISTS (
     SELECT 1
     FROM effective_acquisition_handling_restrictions AS restriction
     WHERE restriction.object_type = NEW.object_type
       AND restriction.object_id = NEW.object_id
 )
BEGIN
    SELECT RAISE(ABORT, 'private acquisition restriction forbids gate clearance');
END;

-- All existing public views flow through this view.  Replacing it makes a newly
-- admitted or newly propagated restriction immediately fail closed without
-- rewriting prior publication history.
DROP VIEW publication_eligible_objects;

CREATE VIEW publication_eligible_objects AS
SELECT decision.object_type, decision.object_id
FROM current_publication_decisions AS decision
JOIN (
    SELECT object_type, object_id
    FROM current_publication_gate_decisions
    WHERE decision = 'clear'
    GROUP BY object_type, object_id
    HAVING count(DISTINCT gate_kind) = 3
) AS cleared
  ON cleared.object_type = decision.object_type
 AND cleared.object_id = decision.object_id
WHERE decision.decision = 'publish'
  AND NOT EXISTS (
      SELECT 1
      FROM effective_acquisition_handling_restrictions AS restriction
      WHERE restriction.object_type = decision.object_type
        AND restriction.object_id = decision.object_id
  );
