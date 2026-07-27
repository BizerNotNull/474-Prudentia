CREATE TABLE controller_writer_generations (
    cluster text PRIMARY KEY,
    current_generation bigint NOT NULL CHECK (current_generation > 0),
    holder text NOT NULL,
    acquired_at timestamptz NOT NULL
);

ALTER TABLE scheduler_backends
    ADD COLUMN source_namespace text NOT NULL DEFAULT '',
    ADD COLUMN source_name text NOT NULL DEFAULT '';

CREATE INDEX scheduler_backends_source_idx
    ON scheduler_backends (cluster, source_namespace, source_name);
