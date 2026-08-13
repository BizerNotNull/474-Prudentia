BEGIN;

DROP INDEX IF EXISTS workload_operations_current_scope_idx;
ALTER TABLE workload_operations
    DROP CONSTRAINT IF EXISTS workload_operations_current_scope_check,
    DROP CONSTRAINT IF EXISTS workload_operations_scope_replicas_check,
    DROP CONSTRAINT IF EXISTS workload_operations_scope_kind_check,
    DROP COLUMN IF EXISTS workload_replicas,
    DROP COLUMN IF EXISTS workload_uid_observed,
    DROP COLUMN IF EXISTS workload_kind,
    DROP COLUMN IF EXISTS workload_name,
    DROP COLUMN IF EXISTS workload_namespace;

COMMIT;
