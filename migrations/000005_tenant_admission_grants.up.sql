CREATE TABLE tenant_counters (
    tenant_hash bytea PRIMARY KEY CHECK (octet_length(tenant_hash) = 32),
    grant_limit integer NOT NULL CHECK (grant_limit >= 0),
    active_grants integer NOT NULL DEFAULT 0 CHECK (active_grants >= 0),
    orphaned_grants integer NOT NULL DEFAULT 0 CHECK (orphaned_grants >= 0),
    version bigint NOT NULL DEFAULT 1 CHECK (version > 0),
    CHECK (active_grants + orphaned_grants <= grant_limit)
);

CREATE TABLE admission_grants (
    grant_id text PRIMARY KEY,
    request_id text NOT NULL UNIQUE,
    reservation_id text NOT NULL UNIQUE REFERENCES scheduler_reservations (reservation_id),
    tenant_hash bytea NOT NULL REFERENCES tenant_counters (tenant_hash),
    slot_cost integer NOT NULL CHECK (slot_cost > 0),
    state text NOT NULL CHECK (state IN ('active_reserved', 'retained_rerank', 'orphaned', 'released')),
    active_contribution integer GENERATED ALWAYS AS (
        CASE WHEN state IN ('active_reserved', 'retained_rerank') THEN slot_cost ELSE 0 END
    ) STORED,
    orphaned_contribution integer GENERATED ALWAYS AS (
        CASE WHEN state = 'orphaned' THEN slot_cost ELSE 0 END
    ) STORED,
    execution_deadline timestamptz NOT NULL,
    classification_after timestamptz NOT NULL,
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    updated_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    CHECK (
        (state = 'released' AND active_contribution = 0 AND orphaned_contribution = 0)
        OR
        (state <> 'released' AND active_contribution + orphaned_contribution = slot_cost)
    )
);

CREATE INDEX admission_grants_classification_idx
    ON admission_grants (classification_after, grant_id)
    WHERE state <> 'released';
