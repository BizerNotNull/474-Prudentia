BEGIN;

-- Observations precede projection/capacity creation. Requiring an existing
-- capacity row inverted the controller protocol and made first observation
-- acceptance impossible.
DO $$
DECLARE constraint_name text;
BEGIN
    SELECT c.conname INTO constraint_name
    FROM pg_constraint c
    WHERE c.conrelid = 'source_observations'::regclass
      AND c.confrelid = 'instance_capacity'::regclass
      AND c.contype = 'f';
    IF constraint_name IS NOT NULL THEN
        EXECUTE format('ALTER TABLE source_observations DROP CONSTRAINT %I', constraint_name);
    END IF;
END
$$;

ALTER TABLE source_observations DROP CONSTRAINT source_observations_source_kind_check;
ALTER TABLE source_observations ADD CONSTRAINT source_observations_source_kind_check
    CHECK (source_kind IN (
        'structural', 'runtime_health', 'load',
        'kubernetes_pod', 'kubernetes_workload', 'engine_health', 'engine_membership'
    ));

COMMIT;
