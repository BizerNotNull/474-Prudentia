DROP INDEX IF EXISTS scheduler_backends_source_idx;

ALTER TABLE scheduler_backends
    DROP COLUMN IF EXISTS source_name,
    DROP COLUMN IF EXISTS source_namespace;

DROP TABLE IF EXISTS controller_writer_generations;
