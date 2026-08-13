BEGIN;

ALTER TABLE workload_operations
    ADD COLUMN workload_namespace text,
    ADD COLUMN workload_name text,
    ADD COLUMN workload_kind text,
    ADD COLUMN workload_uid_observed text,
    ADD COLUMN workload_replicas integer,
    ADD CONSTRAINT workload_operations_scope_kind_check
        CHECK (workload_kind IS NULL OR workload_kind IN ('deployment','statefulset')),
    ADD CONSTRAINT workload_operations_scope_replicas_check
        CHECK (workload_replicas IS NULL OR workload_replicas >= 0);

-- Existing incomplete operations cannot be reconstructed safely from a UID
-- alone. They deliberately remain NULL and keep controller readiness closed
-- until an operator supplies their observed scope or retires them with proof.
ALTER TABLE workload_operations
    ADD CONSTRAINT workload_operations_current_scope_check
    CHECK (phase IN ('completed','failed') OR
        (workload_namespace IS NOT NULL AND workload_name IS NOT NULL AND
         workload_kind IS NOT NULL AND workload_uid_observed = workload_uid AND
         workload_replicas IS NOT NULL)) NOT VALID;

CREATE INDEX workload_operations_current_scope_idx
    ON workload_operations (cluster_id, workload_namespace, workload_name)
    WHERE is_current;

COMMIT;
