BEGIN;

DELETE FROM source_observations
WHERE source_kind IN ('structural', 'runtime_health', 'load');

ALTER TABLE source_observations DROP CONSTRAINT source_observations_source_kind_check;
ALTER TABLE source_observations ADD CONSTRAINT source_observations_source_kind_check
    CHECK (source_kind IN (
        'kubernetes_pod', 'kubernetes_workload', 'engine_health', 'engine_membership'
    ));

ALTER TABLE source_observations ADD CONSTRAINT source_observations_capacity_fkey
    FOREIGN KEY (cluster_id, namespace, logical_engine, pod_uid, endpoint_epoch, recovery_epoch)
    REFERENCES instance_capacity (cluster_id, namespace, logical_engine, pod_uid, endpoint_epoch, recovery_epoch);

COMMIT;
